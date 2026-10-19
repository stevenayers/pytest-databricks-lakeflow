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

from pytest_databricks_lakeflow import helpers, session
from pytest_databricks_lakeflow.ephemeral import EphemeralJob
from pytest_databricks_lakeflow.errors import LakeflowBundleError, LakeflowError
from pytest_databricks_lakeflow.results import NOTEBOOK_EXPORTS, JobRun, NotebookExport
from pytest_databricks_lakeflow.settings import (
    SIDE_EFFECTS,
    Helpers,
    RunAs,
    SideEffects,
    add_dependencies,
    apply_task_params,
    check_params,
    check_task_params,
    disable_tasks,
    inject_lifecycle,
    inject_probes,
    inject_test_marker,
    merge_environments,
    run_job_tasks,
    sanitise,
    upstream_values_from,
    wrap_task,
)
from pytest_databricks_lakeflow.settings import force_conditions as force_condition_outcomes

log = logging.getLogger("pytest_databricks_lakeflow")

Sync = Literal["deploy", "sync", "none"]
SYNC_STRATEGIES: tuple[str, ...] = get_args(Sync)
Children = Literal["warn", "refuse"]
CHILDREN: tuple[str, ...] = get_args(Children)
UNVERSIONED_ARTIFACTS = (
    "Artifacts {names} have no dynamic_version: true. Serverless caches wheels by file name, so a rebuilt wheel with "
    "the same version may not be picked up. Set artifacts.<name>.dynamic_version: true in databricks.yml."
)
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
        sync: How to prepare the bundle: `deploy`, `sync` or `none`.
        variables: Bundle variables, passed as `--var name=value` to every `bundle` command, `bundle destroy`
            included. Instances with different variables set the target up separately.
        deploy_flags: Extra flags for `databricks bundle deploy`, added at the end of the command, such as
            `["--fail-on-active-runs"]`. Instances with different flags deploy separately. Used only when `sync`
            is `deploy`. `-t`, `--target`, `-e`, `--environment`, `-p`, `--profile`, `--var` and `--auto-approve` are
            not allowed.
        timeout: The default timeout for one job run. Each job is also created with `timeout_seconds` set to this
            plus 60 seconds, unless the template has a smaller one, so the platform stops runs the plugin cannot.
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
            without `dynamic_version: true`, instead of the `UserWarning` emitted once per session. Serverless caches
            wheels by file name, so a rebuilt wheel with an unchanged version may not be picked up.
        permissions: The `access_control_list` applied to every ephemeral job when it is created, so that people
            other than its creator can open it. Each entry is a `jobs.JobAccessControlRequest` or a dict such as
            `{"group_name": "data-eng", "permission_level": "CAN_VIEW"}`.
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
        setup: Callable[..., object] | None = None,
        teardown: Callable[..., object] | None = None,
        extra_dependencies: Sequence[str] | None = None,
        environments: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> EphemeralJob:
        """Make a throwaway copy of a bundle job.

        `setup` and `teardown` are plain functions that run on the job's compute, as notebook tasks of the copy. Each
        takes any of `spark`, `dbutils` and `params`, a dict of the parameters the job declares with their values for
        the run, as strings. Only declared parameters reach it, because the names are fixed when `job()` is called:
        one passed under `allow_undeclared_params=True`, or declared by `modify_settings`, is not in `params`. The
        setup task runs before every other task on every run, and the teardown task after every other task, whether
        they passed or failed.
        A function must be self-contained: it is shipped as source, so imports go inside it and it cannot use
        variables of an enclosing function. A value it returns becomes the task's `output` as JSON.

        Args:
            key: The bundle resource key of the job, as `databricks bundle run` takes it.
            params: Job parameters for every run of the copy.
            capture_task_values: The task value keys to capture, by task key.
            name: Replaces the test node id in the job name.
            run_as: Who the copy runs as. `None` removes the template's `run_as`, so it runs as you. `"keep"` keeps
                the template's. A string with `@` names a user, any other string names a service principal by its
                application id, and a `jobs.JobRunAs` is used as given.
            disable: Task keys to disable in the copy. Runs skip them, and they appear in `JobRun.disabled_tasks`.
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
            setup: Runs first, as the task `__pytest_setup`, which every task without `depends_on` comes to depend
                on. Its outcome is `JobRun.setup`.
            teardown: Runs last, as the task `__pytest_teardown` with `run_if` `ALL_DONE`, after every task that no
                other task depends on. Its outcome is `JobRun.teardown`, and a failure is logged as a warning.
            extra_dependencies: Test-only dependencies for the copy, as PyPI requirements or workspace or Volumes
                paths of wheels. They are appended to every environment taken from the template and, as `pypi` or
                `whl` libraries, to every notebook, Python script and Python wheel task on a cluster. The helper tasks
                do not get them.
            environments: Fields of the copy's serverless environment specs, by `environment_key`, such as
                `{"default": {"environment_version": "5"}}`. A field replaces the template's field, after
                `environment_version` and `extra_dependencies` are applied, so `dependencies` replaces the whole list.
                A key the job does not have adds an environment holding only the fields passed, which a task can
                use through `modify_settings`.

        Returns:
            The job, created when entered with `with` or on its first run.

        Raises:
            KeyError: If the bundle has no job `key`.
            LakeflowError: If called outside a test, `capture_task_values` or `disable` names a task that is not in
                the job, `force_conditions` names a task that is not a condition task, is disabled, or has an outcome
                that is not a bool, a parameter name is reserved, an entry of `extra_dependencies` is a local path,
                `children` is `"refuse"` and the copy has a Run Job task that is not disabled nor inside a disabled
                `for_each_task`, `environments` names a key starting with `__pytest_`, holds an entry that is not a
                mapping or a field that a serverless environment does not take, sets both `environment_version` and
                `base_environment` on one environment, or brings the job past 10 environments, or `setup` or
                `teardown` is not a plain function with its source in a file, uses variables of an enclosing
                function, takes a parameter other than `spark`, `dbutils` and `params`, or has a decorator.
                Also if `task_params` names a task that is not in the job or is not a notebook, Python script, JAR,
                Python wheel, SQL or dbt task, gives a task a value that does not fit its type, or holds a parameter
                name starting with `__pytest_`.
            ValueError: If `children` is not `"warn"` or `"refuse"`.
        """
        _check_children(children)
        notebooks = _lifecycle_notebooks(setup, teardown)
        item = session.current_item()
        settings = self.template(key)
        check_params(key, settings, params or {}, allow_undeclared=True)
        check_task_params(key, settings, task_params or {}, params or {})
        apply_task_params(key, settings, task_params or {})
        disable_tasks(settings, disable or [])
        force_condition_outcomes(settings, force_conditions or {})
        add_dependencies(settings, extra_dependencies or [])
        inject_probes(settings, capture_task_values or {}, self._helpers())
        self._inject_lifecycle(settings, notebooks)
        return self._ephemeral(
            item,
            key,
            settings,
            params=params,
            capture_task_values=capture_task_values,
            stubs=(),
            name=name,
            run_as=run_as,
            children=children,
            method="job",
            environments=environments,
        )

    def task(
        self,
        ref: str,
        params: Mapping[str, str] | None = None,
        upstream_task_values: Mapping[str, Mapping[str, object]] | None = None,
        capture_task_values: Sequence[str] | None = None,
        run_as: RunAs = None,
        *,
        task_params: Mapping[str, Mapping[str, str] | Sequence[str]] | None = None,
        upstream_from: JobRun | None = None,
        children: Children = "warn",
        setup: Callable[..., object] | None = None,
        teardown: Callable[..., object] | None = None,
        extra_dependencies: Sequence[str] | None = None,
        environments: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> EphemeralJob:
        """Make a throwaway job that holds one task of a bundle job.

        Each upstream task named in `upstream_task_values` is replaced by a stub task with the same key, so the task's
        `{{tasks.<key>.values.<name>}}` references and `taskValues.get` calls resolve against the stub.

        `upstream_from` replays the task with the values a previous run of the job captured: each task that the task
        depends on is stubbed with its `task_values` in that run. The platform returns no task values with a run, so
        that run must have captured them, with `capture_task_values={"<upstream key>": [...]}` on `job()`.

        Args:
            ref: The job and task as `"job_key.task_key"`.
            params: Job parameters for every run.
            upstream_task_values: The task values each stub sets, by upstream task key. An entry replaces the values
                `upstream_from` holds for that task.
            capture_task_values: The task value keys to capture from the task.
            run_as: Who the job runs as, as for `job()`.
            task_params: Parameters for the task, as for `job()`. Only the task under test can be named.
            upstream_from: A run of the same job whose captured task values stub the task's upstream tasks.
            children: What to do when the task is a Run Job task, as for `job()`.
            setup: Runs first, before the stubs, as for `job()`.
            teardown: Runs last, after the task and its probe, as for `job()`.
            extra_dependencies: Test-only dependencies for the job, as for `job()`.
            environments: Fields of the job's serverless environment specs, by `environment_key`, as for `job()`.

        Returns:
            The job, created when entered with `with` or on its first run.

        Raises:
            KeyError: If the bundle has no such job.
            LakeflowError: If called outside a test, the job has no such task, the task refers to task values of a
                task that is not stubbed, `upstream_from` is a run of another job or lacks the captured values of an
                upstream task, a value is too large or not JSON-serialisable, an entry of `extra_dependencies` is a
                local path, `children` is `"refuse"` and the task is a Run Job task, or `task_params` names another
                task. Also in the cases where `job()` raises it because `task_params` or `environments` cannot be
                applied or `setup` or `teardown` cannot run.
            ValueError: If `children` is not `"warn"` or `"refuse"`.
        """
        _check_children(children)
        notebooks = _lifecycle_notebooks(setup, teardown)
        item = session.current_item()
        job_key, _, task_key = ref.partition(".")
        if not job_key or not task_key:
            raise LakeflowError(f"Expected 'job_key.task_key', got {ref!r}")
        settings = self.template(job_key)
        check_params(job_key, settings, params or {}, allow_undeclared=True)
        check_task_params(job_key, settings, task_params or {}, params or {})
        apply_task_params(job_key, settings, task_params or {}, allowed=[task_key])
        add_dependencies(settings, extra_dependencies or [])
        explicit = dict(upstream_task_values or {})
        upstream: dict[str, Mapping[str, object]] = {}
        if upstream_from is not None:
            upstream.update(upstream_values_from(job_key, settings, task_key, upstream_from, explicit))
        upstream.update(explicit)
        wrap_task(settings, task_key, upstream, list(capture_task_values or []), self._helpers())
        self._inject_lifecycle(settings, notebooks)
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
        if not names:
            return
        message = UNVERSIONED_ARTIFACTS.format(names=names)
        if self.strict_artifacts:
            raise LakeflowError(message)
        if key in current.warned_artifacts:
            return
        current.warned_artifacts.add(key)
        warnings.warn(message, stacklevel=2)

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

    def _inject_lifecycle(self, settings: jobs.JobSettings, notebooks: Mapping[str, str]) -> None:
        if not notebooks:
            return
        folder = session.folder_of_helpers(self._state())
        uploaded = session.current().uploaded_notebooks
        paths: dict[str, str] = {}
        for role, source in notebooks.items():
            paths[role] = helpers.upload_lifecycle(self.ws, folder, role, source, uploaded)
        inject_lifecycle(settings, paths.get("setup"), paths.get("teardown"), self._helpers())

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
        job = EphemeralJob(self, key, settings, params, capture_task_values, stubs)
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


def _flag_name(flag: str) -> str:
    """Get the option a CLI flag sets: `--target` for `--target=x`, `-t` for `-t` and for the joined form `-tx`."""
    name = flag.split("=", 1)[0]
    if name.startswith("--"):
        return name
    return name[:2]


def _lifecycle_notebooks(setup: Callable[..., object] | None, teardown: Callable[..., object] | None) -> dict[str, str]:
    """Build the setup and teardown notebooks that were asked for, by role, checking both before anything else."""
    notebooks: dict[str, str] = {}
    if setup is not None:
        notebooks["setup"] = helpers.lifecycle_notebook(setup, "setup")
    if teardown is not None:
        notebooks["teardown"] = helpers.lifecycle_notebook(teardown, "teardown")
    return notebooks


def _check_children(children: str) -> None:
    if children not in CHILDREN:
        raise ValueError(f"children must be one of {', '.join(CHILDREN)}, got {children!r}")


def _child_message(job_key: str, task: jobs.Task) -> str:
    child = task.run_job_task.job_id if task.run_job_task else None
    return (
        f"Task {task.task_key!r} of job {job_key!r} runs the deployed job {child}, not a copy. Its run keeps that "
        "job's run_as and notifications and is not cancelled or deleted with the ephemeral job."
    )
