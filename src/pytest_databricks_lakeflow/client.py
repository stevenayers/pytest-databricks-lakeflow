from __future__ import annotations

import copy
import logging
import secrets
import warnings
from collections.abc import Callable, Collection, Generator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import timedelta
from functools import partial
from pathlib import Path
from typing import Any, Literal, get_args

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

from pytest_databricks_lakeflow import bundle, helpers, session
from pytest_databricks_lakeflow.ephemeral import EphemeralJob
from pytest_databricks_lakeflow.errors import LakeflowBundleError, LakeflowError
from pytest_databricks_lakeflow.helpers import LifecycleStep
from pytest_databricks_lakeflow.results import NOTEBOOK_EXPORTS, JobRun, NotebookExport
from pytest_databricks_lakeflow.settings import (
    SIDE_EFFECTS,
    Helpers,
    RunAs,
    SideEffects,
    Stub,
    Upstream,
    add_dependencies,
    apply_task_params,
    check_lifecycle_params,
    check_params,
    check_task_params,
    disable_tasks,
    inject_lifecycle,
    inject_probes,
    inject_test_marker,
    merge_environments,
    override_tasks,
    require_task,
    run_job_tasks,
    sanitise,
    stub_upstream,
    upstream_values_from,
    wrap_task,
)
from pytest_databricks_lakeflow.settings import force_conditions as force_condition_outcomes

log = logging.getLogger("pytest_databricks_lakeflow")

Sync = Literal["deploy", "sync", "none"]
SYNC_STRATEGIES: tuple[str, ...] = get_args(Sync)
Children = Literal["warn", "refuse"]
CHILDREN: tuple[str, ...] = get_args(Children)
# A setup or teardown notebook's source, and the job parameters its statements read.
Notebook = tuple[str, frozenset[str]]
UNVERSIONED_ARTIFACTS = (
    "Artifacts {names} have no dynamic_version: true. Serverless caches wheels by file name, so a rebuilt wheel with "
    "the same version may not be picked up. Set artifacts.<name>.dynamic_version: true in databricks.yml."
)
LOCAL_PREFIXES = ("./", "../")
PLUGIN_DEPLOY_FLAGS = {
    "-t": "Pass target= to Lakeflow instead",
    "--target": "Pass target= to Lakeflow instead",
    "-e": "Pass target= to Lakeflow instead",
    "--environment": "Pass target= to Lakeflow instead",
    "-p": "Pass profile= to Lakeflow instead",
    "--profile": "Pass profile= to Lakeflow instead",
    "--var": "Pass variables= to Lakeflow instead",
    "--auto-approve": "The plugin always passes --auto-approve",
}


@dataclass(kw_only=True, eq=False)
class Lakeflow:
    """Makes throwaway copies of a bundle's jobs for tests.

    Tests get one from the `lakeflow` fixture. Override the fixture to configure it:

        @pytest.fixture(scope="session")
        def lakeflow() -> Lakeflow:
            return Lakeflow(target="test", catalog="dev_sandbox")

    Creating one does nothing. The first test that uses it sets up the bundle, which happens once per session for
    each bundle target, however many `Lakeflow` instances use it and however many xdist workers there are.

    Attributes:
        bundle_root: The bundle's directory, relative to the pytest rootdir. Defaults to the nearest directory with
            a databricks.yml, starting at the rootdir.
        target: The bundle target to deploy and read job templates from.
        profile: The Databricks config profile for the CLI and the SDK. Defaults to their standard resolution, which
            reads `DATABRICKS_CONFIG_PROFILE` and the other `DATABRICKS_*` variables.
        workspace_client: A client to use instead of making one from `profile`. The CLI still uses `profile`, or
            else the client's profile.
        sync: How to prepare the bundle: `deploy`, `sync` or `none`. Before `deploy`, `bundle validate` reads the
            run_as of the target and of each job, and the setup fails with `LakeflowBundleError` if one names another
            user, or a service principal on which you do not have the Service Principal User role.
        variables: Bundle variables, passed as `--var name=value` to every `bundle` command, `bundle destroy`
            included. Instances with different variables set the target up separately.
        deploy_flags: Extra flags for `databricks bundle deploy`, added at the end of the command, such as
            `["--fail-on-active-runs"]`. Instances with different flags deploy separately. Used only when `sync`
            is `deploy`. `-t`, `--target`, `-e`, `--environment`, `-p`, `--profile`, `--var` and `--auto-approve` are
            not allowed.
        timeout: The default timeout for one job run. Each job is also created with `timeout_seconds` set to this
            plus 60 seconds, unless the template has a smaller one, so the platform stops runs the plugin cannot.
            A longer `timeout` passed to `run()` raises the job's `timeout_seconds` before that run starts.
        poll_interval: How long to wait between two polls of a run that has not finished. Defaults to two seconds.
            Zero or less raises `ValueError`.
        catalog: The catalog in which `schema()` creates temporary schemas.
        helpers_path: The workspace folder for the helper notebooks. Defaults to
            `/Users/<you>/.pytest-databricks-lakeflow`.
        helpers_compute: A `job_cluster_key` from the job for stub, probe, setup and teardown tasks, for workspaces
            without serverless.
        helpers_environment_version: The serverless environment version for stub, probe, setup and teardown tasks.
        disable_auto_optimization: Stop serverless from retrying failed tasks, so that a failure fails once and fast.
        performance_optimized: Run every ephemeral job as `PERFORMANCE_OPTIMIZED`, whatever the template says.
        side_effects: `neutralise` pauses dashboard subscriptions and SQL alert subscriptions in every ephemeral
            job, so runs do not email snapshots or notify subscribers. `keep` leaves them as the template has them.
        environment_version: The serverless environment version of every environment copied from the template,
            except those whose spec sets `base_environment`. The helper environment keeps
            `helpers_environment_version`.
        budget_policy_id: The budget policy of every ephemeral job.
        existing_cluster_id: A running cluster for every task that the template puts on a job cluster or a new
            cluster, stub, probe, setup and teardown tasks and tasks nested in a `for_each_task` included, so that
            tests skip cluster start-up. The job clusters are dropped. Serverless and warehouse tasks keep their
            compute.
        single_node: Make every job cluster and every task's `new_cluster` in each ephemeral job a single node
            cluster, so that runs start faster and cost one node: no workers, no autoscaling, and the single node
            Spark configuration and `ResourceClass` tag. The job fails to build if one of those clusters uses
            standard access mode, because single node compute needs dedicated access mode. Serverless tasks and
            `existing_cluster_id` are left alone.
        allow_undeclared_params: Allow parameters that the job does not declare.
        test_marker: Declare a `__pytest_test_id` job parameter on every ephemeral job, with the test node id as its
            default, so job code can tell that it runs under test. The node id is used even when `name` replaces it
            in the job name. Tests cannot pass the parameter themselves.
        notebook_export: Which notebook tasks to read with `jobs.export_run` after a run, into `TaskRun.notebook`:
            `failed` for those that ran and did not succeed, `always` for every one that ran, `never` for none.
        sweep_hours: The age after which `--lakeflow-sweep` deletes leftover jobs.
        keep: Keep every ephemeral job and temporary schema. `--lakeflow-keep` also turns this on.
        keep_failed: Keep the ephemeral job when its test fails. `--lakeflow-keep-failed` and
            `--no-lakeflow-keep-failed` override it.
        strict_artifacts: Raise `LakeflowError` from the first template lookup when the bundle has a wheel artifact
            without `dynamic_version: true`, or, with `sync="none"`, a local library built after the copy that the
            last deploy uploaded, instead of the `UserWarning` emitted once per session for each. Serverless is
            reported to cache wheels by file name (databricks/cli#1621 and #2958), so a rebuilt wheel with an
            unchanged version may not be picked up.
        permissions: The `access_control_list` applied to every ephemeral job when it is created, so that people
            other than its creator can open it. Each entry is a `jobs.JobAccessControlRequest` or a dict such as
            `{"group_name": "data-eng", "permission_level": "CAN_VIEW"}`. `IS_OWNER` raises `ValueError`, because
            the creator owns every ephemeral job; use `CAN_MANAGE`.
        modify_settings: Called with each ephemeral job's settings and the test, just before the job is created.
            Change the settings in place.
        run_finished: Called with the outcome of each run and the test, after the run finishes.
        active_runs: Ids of the runs started by this `Lakeflow` that have not finished.
    """

    bundle_root: str | Path | None = None
    target: str = "test"
    profile: str | None = None
    workspace_client: WorkspaceClient | None = field(default=None, repr=False)
    sync: Sync = "deploy"
    variables: Mapping[str, str] | None = None
    deploy_flags: Sequence[str] = ()
    timeout: timedelta = timedelta(minutes=30)
    poll_interval: timedelta | None = None
    catalog: str | None = None
    helpers_path: str | None = None
    helpers_compute: str | None = None
    helpers_environment_version: str = "4"
    disable_auto_optimization: bool = True
    performance_optimized: bool = False
    side_effects: SideEffects = "neutralise"
    environment_version: str | None = None
    budget_policy_id: str | None = None
    existing_cluster_id: str | None = None
    single_node: bool = False
    allow_undeclared_params: bool = False
    test_marker: bool = False
    notebook_export: NotebookExport = "failed"
    sweep_hours: float = 24
    keep: bool = False
    keep_failed: bool = True
    strict_artifacts: bool = False
    permissions: Sequence[Mapping[str, str] | jobs.JobAccessControlRequest] | None = None
    modify_settings: Callable[[jobs.JobSettings, pytest.Item], None] | None = field(default=None, repr=False)
    run_finished: Callable[[JobRun, pytest.Item], None] | None = field(default=None, repr=False)
    active_runs: set[int] = field(default_factory=set, init=False, repr=False)
    _ws: WorkspaceClient | None = field(default=None, init=False, repr=False)
    _state_cache: tuple[session.SessionState, dict[str, Any]] | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.sync not in SYNC_STRATEGIES:
            raise ValueError(f"sync must be one of {', '.join(SYNC_STRATEGIES)}, got {self.sync!r}")
        if self.side_effects not in SIDE_EFFECTS:
            raise ValueError(f"side_effects must be one of {', '.join(SIDE_EFFECTS)}, got {self.side_effects!r}")
        if self.notebook_export not in NOTEBOOK_EXPORTS:
            raise ValueError(
                f"notebook_export must be one of {', '.join(NOTEBOOK_EXPORTS)}, got {self.notebook_export!r}"
            )
        if self.poll_interval is not None and self.poll_interval <= timedelta(0):
            raise ValueError(f"poll_interval must be longer than zero, got {self.poll_interval!r}")
        _check_deploy_flags(self.deploy_flags)
        _check_permissions(self.access_control_list())

    @property
    def ws(self) -> WorkspaceClient:
        """The workspace client: `workspace_client`, or one made from `profile` on first use."""
        if self.workspace_client is not None:
            return self.workspace_client
        if self._ws is None:
            self._ws = session.make_workspace_client(self.profile)
        return self._ws

    def access_control_list(self) -> list[jobs.JobAccessControlRequest] | None:
        """Get `permissions` as the `access_control_list` that `jobs.create` takes, or `None` if there are none."""
        if self.permissions is None:
            return None
        requests = []
        for entry in self.permissions:
            if isinstance(entry, jobs.JobAccessControlRequest):
                requests.append(entry)
            else:
                requests.append(jobs.JobAccessControlRequest.from_dict(dict(entry)))
        return requests

    @property
    def run_id(self) -> str:
        """The id of this test session, which tags ephemeral jobs and names temporary schemas."""
        return session.current().run_id

    @property
    def templates(self) -> dict[str, dict[str, Any]]:
        """The deployed settings of each usable bundle job, by resource key, as dicts."""
        return self._state()["templates"]

    def template(self, key: str) -> jobs.JobSettings:
        """Get a fresh copy of the deployed settings of a bundle job.

        Args:
            key: The bundle resource key of the job.

        Returns:
            The job's settings, safe to change.

        Raises:
            KeyError: If the bundle has no job `key`.
            LakeflowBundleError: If the job cannot be used as a template, for example because it is not deployed.
        """
        state = self._state()
        if key in state["template_errors"]:
            raise LakeflowBundleError(state["template_errors"][key])
        if key not in state["templates"]:
            raise KeyError(f"No job {key!r} in the bundle. Available: {sorted(state['templates'])}")
        return jobs.JobSettings.from_dict(copy.deepcopy(state["templates"][key]))

    def job(
        self,
        key: str,
        params: Mapping[str, str] | None = None,
        capture_task_values: Mapping[str, Sequence[str]] | None = None,
        name: str | None = None,
        run_as: RunAs = None,
        disable: Sequence[str] | None = None,
        *,
        task_params: Mapping[str, Mapping[str, str] | Sequence[str]] | None = None,
        force_conditions: Mapping[str, bool] | None = None,
        children: Children = "warn",
        setup: LifecycleStep | None = None,
        teardown: LifecycleStep | None = None,
        extra_dependencies: Sequence[str] | None = None,
        environments: Mapping[str, Mapping[str, Any]] | None = None,
        task_overrides: Mapping[str, Mapping[str, object]] | None = None,
        upstream_task_values: Upstream | None = None,
    ) -> EphemeralJob:
        """Make a throwaway copy of a bundle job.

        `setup` and `teardown` run on the job's compute, as notebook tasks of the copy. The setup task runs before
        every other task on every run, and the teardown task after every other task, whether they passed or failed.
        Each is one of three forms:

        - A plain function taking any of `spark`, `dbutils` and `params`, a dict of the job's parameters with their
          values for the run, as strings: those it declares, those `modify_settings` declares and those passed
          under `allow_undeclared_params=True`. It must be self-contained: it is shipped as source, so imports go
          inside it and it cannot use variables of an enclosing function. A value it returns becomes the task's
          `output` as JSON.
        - A list of SQL statements, each rendered with `string.Template` from the same parameters (`${name}`, and
          `$$` for a literal dollar) and run in order through `spark.sql`. The output is `{"statements": <count>}`.
          The list must not be empty. Each `${name}` is checked here, when `job()` is called, so it must be a
          parameter the bundle job declares or a key of `params`; one that only `run(params=...)` or
          `modify_settings` would supply is refused.
        - A `pathlib.Path` to a `.py` file, relative to the pytest rootdir, uploaded as the notebook. It reads the
          parameters from its widgets and calls `dbutils.notebook.exit` itself for an output.

        Args:
            key: The bundle resource key of the job, as `databricks bundle run` takes it.
            params: Job parameters for every run of the copy.
            capture_task_values: The task value keys to capture, by task key.
            name: Replaces the test node id in the job name.
            run_as: Who the copy runs as. `None` removes the template's `run_as`, so it runs as you. `"keep"` keeps
                the template's. A string with `@` names a user, any other string names a service principal by its
                application id, and a `jobs.JobRunAs` is used as given. A service principal needs you to have the
                Service Principal User role on it, which is checked before the copy is created when the workspace
                client knows its account id.
            disable: Task keys to disable in the copy. Runs skip them, and the platform disables every task downstream
                of them too. Both appear in `JobRun.disabled_tasks`.
            task_params: Parameters for single tasks of the copy, by task key, in the shape the task type takes. A
                mapping is merged into a notebook task's `base_parameters`, a Python wheel task's `named_parameters` or
                a SQL task's `parameters`, so the template's `{{tasks.<key>.values.<name>}}` references survive. A list
                replaces a Python script, JAR or Python wheel task's `parameters` or a dbt task's `commands`. A Python
                wheel task takes one or the other, so the one not given is cleared. Values are passed unchanged. A name
                that is also a job parameter emits a `UserWarning`, because the job value wins.
            force_conditions: The outcome each if/else condition task takes in every run of the copy, by task key:
                `True` runs the `true` branch, `False` the `false` branch. The task's operands are replaced by
                constants, so the branch no longer depends on the data upstream of it. The upstream tasks still run.
            children: What to do about Run Job tasks, which the copy keeps as deployed, so they run the deployed
                child job and not a copy. `"warn"` emits a `UserWarning` for each one that is not disabled nor
                inside a disabled `for_each_task`. `"refuse"` raises `LakeflowError` instead, before anything is
                created.
            setup: A function, a list of SQL statements or a `.py` file to run first, as the task `__pytest_setup`,
                which every task without `depends_on` comes to depend on. Its outcome is `JobRun.setup`.
            teardown: A function, a list of SQL statements or a `.py` file to run last, as the task
                `__pytest_teardown` with `run_if` `ALL_DONE`, after every task that no other task depends on. Its
                outcome is `JobRun.teardown`, and a failure is logged as a warning.
            extra_dependencies: Test-only dependencies for the copy, as PyPI requirements, workspace or Volumes
                paths of wheels, or local wheels as paths relative to the bundle root that start with `./` or `../`,
                such as `./dist/*.whl[test]`. A local path is pointed at the copy of each matching file that the last
                `bundle deploy` uploaded, its extras kept. They are appended to every environment taken from the
                template and, as `pypi` or `whl` libraries, to every notebook, Python script and Python wheel task on
                a cluster. A wheel with extras reaches only the environments, because `libraries[].whl` takes no
                extras. The helper tasks do not get them. The notebook tasks they do not reach, after `environments`
                and `modify_settings`, such as a serverless notebook task without an `environment_key`, are named in
                a `UserWarning` when the job is created.
            environments: Fields of the copy's serverless environment specs, by `environment_key`, such as
                `{"default": {"environment_version": "5"}}`. A field replaces the template's field, after
                `environment_version` and `extra_dependencies` are applied, so `dependencies` replaces the whole list.
                A key the job does not have adds an environment holding only the fields passed, which a task can
                use through `modify_settings`.
            task_overrides: Timeout and retry fields of the copy's tasks, by task key, such as
                `{"load": {"timeout_seconds": 60, "max_retries": 1, "retry_on_timeout": True}}`. The fields are
                `timeout_seconds`, which applies to each attempt, `max_retries`, `min_retry_interval_millis` and
                `retry_on_timeout`. They are applied before the probes, setup and teardown tasks are added, so those
                cannot be named.
            upstream_task_values: Tasks of the copy to replace with stubs, by task key, with the task values each stub
                sets, such as `{"extract": {"row_count": 10}}`, or a `Stub` with `values` and `outputs`. A stub keeps
                the task's key and place, so the tasks downstream of it still depend on it and their
                `{{tasks.<key>.values.<name>}}` references resolve against it, and `run(only=...)` adds it whenever a
                selected task depends on it. Their `{{tasks.<key>.output.<path>}}` references are replaced by the
                stub's `outputs`. It drops the task's
                `depends_on`, so the tasks upstream of it still run on a full run but no longer hold it back. It
                is applied after `task_params`, `disable`, `force_conditions`, `extra_dependencies` and
                `task_overrides`, so what they set on that task is lost. It appears in `JobRun.injected_tasks`.

        Returns:
            The job, created when entered with `with` or on its first run.

        Raises:
            KeyError: If the bundle has no job `key`.
            LakeflowError: If called outside a test, `capture_task_values`, `disable` or `task_overrides` names a task
                that is not in the job, `task_overrides` sets a field it does not take, `force_conditions` names a
                task that is not a condition task, is disabled, or has an outcome that is not a bool, a parameter
                name is reserved, an entry of `extra_dependencies` is a local path that does not start with `./` or
                `../`, matches no file, has no uploaded copy or is used with `sync="sync"`, or is a wheel with extras
                while the copy has a Python task on a cluster, `children` is `"refuse"` and the copy has a Run Job task
                that is not disabled nor inside a disabled `for_each_task`, `environments` names a key starting with
                `__pytest_`, holds an entry that is not a mapping or a field that a
                serverless environment does not take, sets both `environment_version` and `base_environment` on one
                environment, or brings the job past 10 environments, or `setup` or `teardown` is not a plain function
                with its source in a file, uses variables of an enclosing function, takes a parameter other than
                `spark`, `dbutils` and `params`, or has a decorator. Also if `setup` or `teardown` is a string or an
                empty list, a statement has a `$` that is neither a placeholder nor `$$` or a `${name}` that is neither
                declared nor in `params`, or a path is missing or not a `.py` file.
                Also if `task_params` names a task that is not in the job or is not a notebook, Python script, JAR,
                Python wheel, SQL or dbt task, gives a task a value that does not fit its type, or holds a parameter
                name starting with `__pytest_`, or `upstream_task_values` names a task that is not in the job or holds
                a value that is too large or not JSON-serialisable, an output that contains `{{` or is not
                JSON-serialisable, or no output for a path that a task of the copy refers to.
            ValueError: If `children` is not `"warn"` or `"refuse"`.
        """
        _check_children(children)
        notebooks = _lifecycle_notebooks(setup, teardown)
        item = session.current_item()
        settings = self.template(key)
        check_params(key, settings, params or {}, allow_undeclared=True)
        _check_lifecycle_params(key, settings, params or {}, notebooks)
        # Applied first, so a task key or value it rejects gives no collision warning before the error.
        apply_task_params(key, settings, task_params or {})
        check_task_params(key, settings, task_params or {}, params or {})
        disable_tasks(settings, disable or [])
        force_condition_outcomes(settings, force_conditions or {})
        dependencies = self._resolve_local_wheels(extra_dependencies or [])
        add_dependencies(settings, dependencies)
        override_tasks(settings, task_overrides or {})
        stub_upstream(settings, upstream_task_values or {}, self._helpers())
        inject_probes(settings, capture_task_values or {}, self._helpers())
        self._inject_lifecycle(settings, notebooks, params or {})
        return self._ephemeral(
            item,
            key,
            settings,
            params=params,
            capture_task_values=capture_task_values,
            stubs=list(upstream_task_values or {}),
            name=name,
            run_as=run_as,
            children=children,
            method="job",
            environments=environments,
            extra_dependencies=dependencies,
        )

    def task(
        self,
        ref: str,
        params: Mapping[str, str] | None = None,
        upstream_task_values: Upstream | None = None,
        capture_task_values: Sequence[str] | None = None,
        run_as: RunAs = None,
        *,
        task_params: Mapping[str, Mapping[str, str] | Sequence[str]] | None = None,
        upstream_from: JobRun | None = None,
        children: Children = "warn",
        setup: LifecycleStep | None = None,
        teardown: LifecycleStep | None = None,
        extra_dependencies: Sequence[str] | None = None,
        environments: Mapping[str, Mapping[str, Any]] | None = None,
        task_overrides: Mapping[str, Mapping[str, object]] | None = None,
    ) -> EphemeralJob:
        """Make a throwaway job that holds one task of a bundle job.

        Each upstream task named in `upstream_task_values` is replaced by a stub task with the same key, so the task's
        `{{tasks.<key>.values.<name>}}` references and `taskValues.get` calls resolve against the stub. A stub is a
        notebook task, so it has no SQL, alert or other task output: the task's `{{tasks.<key>.output.<path>}}`
        references are replaced by the literals that a `Stub` gives in `outputs`.

        `upstream_from` replays the task with the values a previous run of the job captured: each task that the task
        depends on is stubbed with its `task_values` in that run. The platform returns no task values with a run, so
        that run must have captured them, with `capture_task_values={"<upstream key>": [...]}` on `job()`.

        Args:
            ref: The job and task as `"job_key.task_key"`.
            params: Job parameters for every run.
            upstream_task_values: By upstream task key, a mapping of value keys to values, or a `Stub` with `values`
                and `outputs`. An entry replaces the values `upstream_from` holds for that task.
            capture_task_values: The task value keys to capture from the task.
            run_as: Who the job runs as, as for `job()`.
            task_params: Parameters for the task, as for `job()`. Only the task under test can be named.
            upstream_from: A run of the same job whose captured task values stub the task's upstream tasks.
            children: What to do when the task is a Run Job task, as for `job()`.
            setup: Runs first, before the stubs, as for `job()`.
            teardown: Runs last, after the task and its probe, as for `job()`.
            extra_dependencies: Test-only dependencies for the job, as for `job()`.
            environments: Fields of the job's serverless environment specs, by `environment_key`, as for `job()`.
            task_overrides: Timeout and retry fields of the task, as for `job()`. Only the task under test can be
                named, as `{"<task_key>": {...}}`.

        Returns:
            The job, created when entered with `with` or on its first run.

        Raises:
            KeyError: If the bundle has no such job.
            LakeflowError: If called outside a test, the job has no such task, the task refers to task values of a
                task that is not stubbed or to an output that its stub does not give, `upstream_from` is a run of
                another job or lacks the captured values of an upstream task, a value is too large or not
                JSON-serialisable, an output contains `{{` or is not JSON-serialisable, an entry of
                `extra_dependencies` cannot be used as for `job()`, `children` is `"refuse"` and the task is a Run Job
                task, or `task_params` or `task_overrides` names another task. Also in the cases where `job()` raises
                it because `task_params`, `environments` or `task_overrides` cannot be applied or `setup` or
                `teardown` cannot run.
            ValueError: If `children` is not `"warn"` or `"refuse"`.
        """
        _check_children(children)
        notebooks = _lifecycle_notebooks(setup, teardown)
        item = session.current_item()
        job_key, _, task_key = ref.partition(".")
        if not job_key or not task_key:
            raise LakeflowError(f"Expected 'job_key.task_key', got {ref!r}")
        settings = self.template(job_key)
        # Before task_params, so a mistyped ref lists the job's tasks and warns about no parameter collisions.
        require_task(settings, task_key)
        check_params(job_key, settings, params or {}, allow_undeclared=True)
        _check_lifecycle_params(job_key, settings, params or {}, notebooks)
        # Applied first, as in job().
        apply_task_params(job_key, settings, task_params or {}, allowed=[task_key])
        check_task_params(job_key, settings, task_params or {}, params or {})
        others = sorted(set(task_overrides or {}) - {task_key})
        if others:
            raise LakeflowError(f"task_overrides can only name {task_key!r}, the task under test, not {others}")
        dependencies = self._resolve_local_wheels(extra_dependencies or [])
        add_dependencies(settings, dependencies)
        explicit = dict(upstream_task_values or {})
        upstream: dict[str, Mapping[str, object] | Stub] = {}
        if upstream_from is not None:
            upstream.update(upstream_values_from(job_key, settings, task_key, upstream_from, explicit))
        upstream.update(explicit)
        wrap_task(settings, task_key, upstream, list(capture_task_values or []), self._helpers())
        override_tasks(settings, task_overrides or {})
        self._inject_lifecycle(settings, notebooks, params or {})
        capture: dict[str, list[str]] = {}
        if capture_task_values:
            capture = {task_key: list(capture_task_values)}
        return self._ephemeral(
            item,
            job_key,
            settings,
            params=params,
            capture_task_values=capture,
            stubs=upstream,
            name=None,
            run_as=run_as,
            children=children,
            method="task",
            environments=environments,
            extra_dependencies=dependencies,
        )

    @contextmanager
    def schema(self, catalog: str | None = None) -> Generator[str, None, None]:
        """Create a temporary schema, and drop it on leaving the `with` block.

        The schema is named `<catalog>.pytest_<run id>_<random>`. Pass it to jobs as a job parameter, because
        bundle variables cannot change per test. It is dropped even when the block fails, unless `keep` or
        `--lakeflow-keep` is set.

        Args:
            catalog: The catalog to create it in. Defaults to `catalog`.

        Yields:
            The schema's full name.

        Raises:
            LakeflowError: If there is no catalog.
        """
        catalog = catalog or self.catalog
        if not catalog:
            raise LakeflowError("Lakeflow.schema() needs a catalog. Pass catalog to Lakeflow or to schema().")
        state = session.current()
        name = f"{session.schema_prefix(state.run_id)}{secrets.token_hex(4)}"
        full_name = f"{catalog}.{name}"
        self.ws.schemas.create(name=name, catalog_name=catalog)
        log.info("Created schema %s", full_name)
        try:
            yield full_name
        finally:
            if state.keeps(self.keep, keep_failed=False, failed=False):
                log.warning("Keeping schema %s", full_name)
                state.kept_schemas.append(full_name)
            else:
                self.ws.schemas.delete(full_name, force=True)
                log.info("Dropped schema %s", full_name)

    def _state(self) -> dict[str, Any]:
        current = session.current()
        if self._state_cache is not None and self._state_cache[0] is current:
            return self._state_cache[1]
        profile = self.profile or None
        if profile is None and self.workspace_client is not None:
            profile = self.workspace_client.config.profile
        deploy_flags: tuple[str, ...] = ()
        if self.sync == "deploy":
            deploy_flags = tuple(self.deploy_flags)
        key = session.BundleKey(
            root=session.resolve_root(self.bundle_root, current.rootpath),
            target=self.target,
            profile=profile,
            sync=self.sync,
            helpers_path=self.helpers_path,
            variables=tuple(sorted((self.variables or {}).items())),
            deploy_flags=deploy_flags,
        )
        state = current.bundle_state(key, self.ws, self.sweep_hours)
        self._check_artifacts(current, key, state)
        current.lakeflows.append(self)
        self._state_cache = (current, state)
        return state

    def _check_artifacts(self, current: session.SessionState, key: session.BundleKey, state: dict[str, Any]) -> None:
        names = state.get("unversioned_artifacts") or []
        if names:
            self._warn_or_raise(UNVERSIONED_ARTIFACTS.format(names=names), key, current.warned_artifacts)
        stale = state.get("stale_uploads") or []
        if stale:
            self._warn_or_raise(_stale_message(stale, key.target), key, current.warned_uploads)

    def _warn_or_raise(self, message: str, key: session.BundleKey, warned: set[session.BundleKey]) -> None:
        if self.strict_artifacts:
            raise LakeflowError(message)
        if key in warned:
            return
        warned.add(key)
        warnings.warn(message, stacklevel=3)

    def _resolve_local_wheels(self, dependencies: Sequence[str]) -> list[str]:
        """Point each `./` and `../` entry of `extra_dependencies` at the uploaded copies of the files it matches."""
        local = [dep for dep in dependencies if dep.startswith(LOCAL_PREFIXES)]
        if not local:
            return list(dependencies)
        if self.sync == "sync":
            raise LakeflowError(
                f"extra_dependencies holds local paths ({', '.join(local)}), which `bundle sync` does not upload. "
                'Use Lakeflow(sync="deploy").'
            )
        state = self._state()
        root = Path(state["root"])
        resolved: list[str] = []
        for dep in dependencies:
            if not dep.startswith(LOCAL_PREFIXES):
                resolved.append(dep)
                continue
            try:
                resolved.extend(bundle.uploaded(dep, root, state["artifact_path"], partial(session.exists, self.ws)))
            except LakeflowBundleError as e:
                raise LakeflowBundleError(f"extra_dependencies: {e}") from e
        return resolved

    def _job_keys(self) -> dict[int, str]:
        return {job_id: key for key, job_id in self._state().get("job_ids", {}).items()}

    def _helpers(self) -> Helpers:
        paths = self._state()["helpers"]
        return Helpers(
            set_values=paths.get("set_values", ""),
            get_values=paths.get("get_values", ""),
            environment_version=self.helpers_environment_version,
            job_cluster_key=self.helpers_compute,
        )

    def _inject_lifecycle(
        self, settings: jobs.JobSettings, notebooks: Mapping[str, Notebook], params: Mapping[str, str]
    ) -> None:
        if not notebooks:
            return
        folder = session.folder_of_helpers(self._state())
        uploaded = session.current().uploaded_notebooks
        paths: dict[str, str] = {}
        for role, (source, _) in notebooks.items():
            paths[role] = helpers.upload_lifecycle(self.ws, folder, role, source, uploaded)
        inject_lifecycle(settings, paths.get("setup"), paths.get("teardown"), self._helpers(), params)

    def _ephemeral(
        self,
        item: pytest.Item,
        key: str,
        settings: jobs.JobSettings,
        *,
        params: Mapping[str, str] | None,
        capture_task_values: Mapping[str, Sequence[str]] | None,
        stubs: Collection[str],
        name: str | None,
        run_as: RunAs,
        children: Children,
        method: str,
        environments: Mapping[str, Mapping[str, Any]] | None,
        extra_dependencies: Sequence[str],
    ) -> EphemeralJob:
        if settings.performance_target == jobs.PerformanceTarget.STANDARD and not self.performance_optimized:
            warnings.warn(
                f"Job {key!r} has performance_target STANDARD, which adds 3 to 4 minutes to every run. "
                "Pass performance_optimized=True to Lakeflow to run tests with PERFORMANCE_OPTIMIZED.",
                stacklevel=3,
            )
        for task in run_job_tasks(settings):
            message = _child_message(key, task)
            if children == "refuse":
                raise LakeflowError(message)
            warnings.warn(f'{message} Pass children="refuse" to Lakeflow.{method}() to fail instead.', stacklevel=3)
        current = session.current()
        attribution = dict(current.attribution)
        if "pytest_user" not in attribution:
            user = self._state().get("user")
            if user:
                attribution["pytest_user"] = user
        sanitise(
            settings,
            name=name or item.nodeid,
            tags={
                "pytest": "",
                "pytest_run_id": self.run_id,
                "pytest_nodeid": item.nodeid,
                **current.expiry_tag(),
                **attribution,
            },
            description=f"{item.nodeid} {attribution.get('pytest_ci_url', '')}".strip(),
            disable_auto_optimization=self.disable_auto_optimization,
            performance_optimized=self.performance_optimized,
            run_as=run_as,
            timeout=self.timeout,
            side_effects=self.side_effects,
            environment_version=self.environment_version,
            budget_policy_id=self.budget_policy_id,
            existing_cluster_id=self.existing_cluster_id,
            single_node=self.single_node,
        )
        merge_environments(settings, environments or {})
        if self.test_marker:
            inject_test_marker(settings, item.nodeid)
        job = EphemeralJob(
            self, key, settings, params, capture_task_values, stubs, extra_dependencies=extra_dependencies
        )
        item.addfinalizer(partial(job._teardown, item))
        return job


def _check_deploy_flags(deploy_flags: Sequence[str]) -> None:
    if isinstance(deploy_flags, str):
        raise ValueError(f"deploy_flags must be a list of flags, not a string: [{deploy_flags!r}]")
    for flag in deploy_flags:
        if not isinstance(flag, str):
            raise ValueError(f"deploy_flags must contain only strings, got {flag!r}")
        name = _flag_name(flag)
        if name in PLUGIN_DEPLOY_FLAGS:
            raise ValueError(f"deploy_flags cannot contain {name}. {PLUGIN_DEPLOY_FLAGS[name]}.")


def _check_permissions(requests: Sequence[jobs.JobAccessControlRequest] | None) -> None:
    for request in requests or []:
        if request.permission_level == jobs.JobPermissionLevel.IS_OWNER:
            raise ValueError(
                "permissions cannot grant IS_OWNER: the creator owns every ephemeral job. Use CAN_MANAGE instead."
            )


def _flag_name(flag: str) -> str:
    """Get the option a CLI flag sets: `--target` for `--target=x`, `-t` for `-t` and for the joined form `-tx`."""
    name = flag.split("=", 1)[0]
    if name.startswith("--"):
        return name
    return name[:2]


def _lifecycle_notebooks(setup: LifecycleStep | None, teardown: LifecycleStep | None) -> dict[str, Notebook]:
    """Build the setup and teardown notebooks that were asked for, by role, checking both before anything else."""
    notebooks: dict[str, Notebook] = {}
    if setup is None and teardown is None:
        return notebooks
    rootpath = session.current().rootpath
    if setup is not None:
        notebooks["setup"] = helpers.lifecycle_source(setup, rootpath, "setup")
    if teardown is not None:
        notebooks["teardown"] = helpers.lifecycle_source(teardown, rootpath, "teardown")
    return notebooks


def _stale_message(stale: Sequence[str], target: str) -> str:
    deploy = f"Run `databricks bundle deploy -t {target}`."
    if len(stale) == 1:
        return f"{stale[0]} was built after its uploaded copy. {deploy}"
    return f"{', '.join(stale)} were built after their uploaded copies. {deploy}"


def _check_lifecycle_params(
    job_key: str, settings: jobs.JobSettings, params: Mapping[str, str], notebooks: Mapping[str, Notebook]
) -> None:
    for role, (_, names) in notebooks.items():
        check_lifecycle_params(job_key, settings, params, role, names)


def _check_children(children: str) -> None:
    if children not in CHILDREN:
        raise ValueError(f"children must be one of {', '.join(CHILDREN)}, got {children!r}")


def _child_message(job_key: str, task: jobs.Task) -> str:
    child = task.run_job_task.job_id if task.run_job_task else None
    return (
        f"Task {task.task_key!r} of job {job_key!r} runs the deployed job {child}, not a copy. Its run keeps that "
        "job's run_as and notifications and is not cancelled or deleted with the ephemeral job."
    )
