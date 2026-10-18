from __future__ import annotations

import copy
import logging
import re
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

from pytest_databricks_lakeflow import session
from pytest_databricks_lakeflow.ephemeral import EphemeralJob
from pytest_databricks_lakeflow.errors import LakeflowBundleError, LakeflowError
from pytest_databricks_lakeflow.results import JobRun
from pytest_databricks_lakeflow.settings import Helpers, RunAs, check_params, inject_probes, sanitise, wrap_task

log = logging.getLogger("pytest_databricks_lakeflow")

Sync = Literal["deploy", "sync", "none"]
SYNC_STRATEGIES: tuple[str, ...] = get_args(Sync)


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
        timeout: The default timeout for one job run.
        catalog: The catalog in which `schema()` creates temporary schemas.
        helpers_path: The workspace folder for the helper notebooks. Defaults to
            `/Users/<you>/.pytest-databricks-lakeflow`.
        helpers_compute: A `job_cluster_key` from the job for stub and probe tasks, for workspaces without
            serverless.
        helpers_environment_version: The serverless environment version for stub and probe tasks.
        disable_auto_optimization: Stop serverless from retrying failed tasks, so that a failure fails once and fast.
        performance_optimized: Run every ephemeral job as `PERFORMANCE_OPTIMIZED`, whatever the template says.
        allow_undeclared_params: Allow parameters that the job does not declare.
        sweep_hours: The age after which `--lakeflow-sweep` deletes leftover jobs.
        keep: Keep every ephemeral job and temporary schema. `--lakeflow-keep` also turns this on.
        keep_failed: Keep the ephemeral job when its test fails. `--lakeflow-keep-failed` and
            `--no-lakeflow-keep-failed` override it.
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
    timeout: timedelta = timedelta(minutes=30)
    catalog: str | None = None
    helpers_path: str | None = None
    helpers_compute: str | None = None
    helpers_environment_version: str = "4"
    disable_auto_optimization: bool = True
    performance_optimized: bool = False
    allow_undeclared_params: bool = False
    sweep_hours: float = 24
    keep: bool = False
    keep_failed: bool = True
    permissions: Sequence[Mapping[str, str] | jobs.JobAccessControlRequest] | None = None
    modify_settings: Callable[[jobs.JobSettings, pytest.Item], None] | None = field(default=None, repr=False)
    run_finished: Callable[[JobRun, pytest.Item], None] | None = field(default=None, repr=False)
    active_runs: set[int] = field(default_factory=set, init=False, repr=False)
    _ws: WorkspaceClient | None = field(default=None, init=False, repr=False)
    _state_cache: tuple[session.SessionState, dict[str, Any]] | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.sync not in SYNC_STRATEGIES:
            raise ValueError(f"sync must be one of {', '.join(SYNC_STRATEGIES)}, got {self.sync!r}")

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
        return [
            entry
            if isinstance(entry, jobs.JobAccessControlRequest)
            else jobs.JobAccessControlRequest.from_dict(dict(entry))
            for entry in self.permissions
        ]

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
    ) -> EphemeralJob:
        """Make a throwaway copy of a bundle job.

        Args:
            key: The bundle resource key of the job, as `databricks bundle run` takes it.
            params: Job parameters for every run of the copy.
            capture_task_values: The task value keys to capture, by task key.
            name: Replaces the test node id in the job name.
            run_as: Who the copy runs as. `None` removes the template's `run_as`, so it runs as you. `"keep"` keeps
                the template's. A string with `@` names a user, any other string names a service principal by its
                application id, and a `jobs.JobRunAs` is used as given.

        Returns:
            The job, created when entered with `with` or on its first run.

        Raises:
            KeyError: If the bundle has no job `key`.
            LakeflowError: If called outside a test, `capture_task_values` names a task that is not in the job, or a
                parameter name is reserved.
        """
        item = session.current_item()
        settings = self.template(key)
        check_params(key, settings, params or {}, allow_undeclared=True)
        inject_probes(settings, capture_task_values or {}, self._helpers())
        return self._ephemeral(item, key, settings, params, capture_task_values, (), name, run_as)

    def task(
        self,
        ref: str,
        params: Mapping[str, str] | None = None,
        upstream_task_values: Mapping[str, Mapping[str, object]] | None = None,
        capture_task_values: Sequence[str] | None = None,
        run_as: RunAs = None,
    ) -> EphemeralJob:
        """Make a throwaway job that holds one task of a bundle job.

        Each upstream task named in `upstream_task_values` is replaced by a stub task with the same key, so the task's
        `{{tasks.<key>.values.<name>}}` references and `taskValues.get` calls resolve against the stub.

        Args:
            ref: The job and task as `"job_key.task_key"`.
            params: Job parameters for every run.
            upstream_task_values: The task values each stub sets, by upstream task key.
            capture_task_values: The task value keys to capture from the task.
            run_as: Who the job runs as, as for `job()`.

        Returns:
            The job, created when entered with `with` or on its first run.

        Raises:
            KeyError: If the bundle has no such job.
            LakeflowError: If called outside a test, the job has no such task, the task refers to task values of a
                task that is not stubbed, or a value is too large or not JSON-serialisable.
        """
        item = session.current_item()
        job_key, _, task_key = ref.partition(".")
        if not job_key or not task_key:
            raise LakeflowError(f"Expected 'job_key.task_key', got {ref!r}")
        settings = self.template(job_key)
        check_params(job_key, settings, params or {}, allow_undeclared=True)
        upstream = dict(upstream_task_values or {})
        wrap_task(settings, task_key, upstream, list(capture_task_values or []), self._helpers())
        capture = {task_key: list(capture_task_values)} if capture_task_values else {}
        return self._ephemeral(item, job_key, settings, params, capture, upstream, None, run_as)

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
        name = f"pytest_{re.sub(r'[^a-z0-9_]', '_', state.run_id.lower())}_{secrets.token_hex(4)}"
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
        key = session.BundleKey(
            root=session.resolve_root(self.bundle_root, current.rootpath),
            target=self.target,
            profile=self.profile or (self.workspace_client.config.profile if self.workspace_client else None),
            sync=self.sync,
            helpers_path=self.helpers_path,
        )
        state = current.bundle_state(key, self.ws, self.sweep_hours)
        current.lakeflows.append(self)
        self._state_cache = (current, state)
        return state

    def _helpers(self) -> Helpers:
        paths = self._state()["helpers"]
        return Helpers(
            set_values=paths.get("set_values", ""),
            get_values=paths.get("get_values", ""),
            environment_version=self.helpers_environment_version,
            job_cluster_key=self.helpers_compute,
        )

    def _ephemeral(
        self,
        item: pytest.Item,
        key: str,
        settings: jobs.JobSettings,
        params: Mapping[str, str] | None,
        capture_task_values: Mapping[str, Sequence[str]] | None,
        stubs: Collection[str],
        name: str | None,
        run_as: RunAs,
    ) -> EphemeralJob:
        if settings.performance_target == jobs.PerformanceTarget.STANDARD and not self.performance_optimized:
            warnings.warn(
                f"Job {key!r} has performance_target STANDARD, which adds 3 to 4 minutes to every run. "
                "Pass performance_optimized=True to Lakeflow to run tests with PERFORMANCE_OPTIMIZED.",
                stacklevel=3,
            )
        sanitise(
            settings,
            name=name or item.nodeid,
            tags={"pytest": "", "pytest_run_id": self.run_id, "pytest_nodeid": item.nodeid},
            disable_auto_optimization=self.disable_auto_optimization,
            performance_optimized=self.performance_optimized,
            run_as=run_as,
        )
        job = EphemeralJob(self, key, settings, params, capture_task_values, stubs)
        item.addfinalizer(partial(job._teardown, item))
        return job
