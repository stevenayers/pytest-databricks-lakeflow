from __future__ import annotations

import copy
import logging
import time
import warnings
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import Any

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors.base import DatabricksError
from databricks.sdk.service import jobs

from pytest_databricks_lakeflow.ephemeral import EphemeralJob, cancel_run, item_failed
from pytest_databricks_lakeflow.errors import LakeflowBundleError, LakeflowError
from pytest_databricks_lakeflow.options import Options
from pytest_databricks_lakeflow.settings import Helpers, check_params, inject_probes, sanitise, wrap_task

log = logging.getLogger("pytest_databricks_lakeflow")


@dataclass
class Lakeflow:
    """Creates throwaway copies of the bundle's jobs. Use the `lakeflow` fixture to get one.

    Attributes:
        ws: The workspace client.
        options: The plugin's options.
        run_id: The id of this test session, used to tag ephemeral jobs.
        templates: The deployed settings of each bundle job, by resource key, as dicts.
        template_errors: Why each bundle job that cannot be used has no template, by resource key.
        helpers: Workspace paths of the helper notebooks, by name.
        request: The test this copy is bound to, or `None` for the session's copy.
        active_runs: Ids of runs started in this session that have not finished.
    """

    ws: WorkspaceClient
    options: Options
    run_id: str
    templates: dict[str, dict[str, Any]]
    template_errors: dict[str, str] = field(default_factory=dict)
    helpers: dict[str, str] = field(default_factory=dict)
    request: pytest.FixtureRequest | None = None
    active_runs: set[int] = field(default_factory=set)

    @classmethod
    def from_state(cls, ws: WorkspaceClient, options: Options, state: dict[str, Any]) -> Lakeflow:
        return cls(
            ws=ws,
            options=options,
            run_id=state["run_id"],
            templates=state["templates"],
            template_errors=state["template_errors"],
            helpers=state["helpers"],
        )

    def for_test(self, request: pytest.FixtureRequest) -> Lakeflow:
        """Bind a copy to one test.

        Args:
            request: The test's fixture request.

        Returns:
            A copy that shares this one's templates and active runs.
        """
        return replace(self, request=request)

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
        if key in self.template_errors:
            raise LakeflowBundleError(self.template_errors[key])
        if key not in self.templates:
            raise KeyError(f"No job {key!r} in the bundle. Available: {sorted(self.templates)}")
        return jobs.JobSettings.from_dict(copy.deepcopy(self.templates[key]))

    def current_request(self) -> pytest.FixtureRequest:
        if self.request is None:
            raise LakeflowError("Use the lakeflow fixture inside a test to create ephemeral jobs")
        return self.request

    def job(
        self,
        key: str,
        params: Mapping[str, str] | None = None,
        capture_values: Mapping[str, Sequence[str]] | None = None,
        name: str | None = None,
    ) -> EphemeralJob:
        """Make a throwaway copy of a bundle job.

        Args:
            key: The bundle resource key of the job, as `databricks bundle run` takes it.
            params: Job parameters for every run of the copy.
            capture_values: The task value keys to capture, by task key.
            name: Replaces the test node id in the job name.

        Returns:
            The job, created when entered with `with` or on its first run.

        Raises:
            KeyError: If the bundle has no job `key`.
            LakeflowError: If `capture_values` names a task that is not in the job, or a parameter name is
                reserved.
        """
        settings = self.template(key)
        check_params(key, settings, params or {}, allow_undeclared=True)
        inject_probes(settings, capture_values or {}, self._helpers())
        return self._ephemeral(key, settings, params, capture_values, (), name)

    def task(
        self,
        ref: str,
        params: Mapping[str, str] | None = None,
        upstream_values: Mapping[str, Mapping[str, object]] | None = None,
        capture_values: Sequence[str] | None = None,
    ) -> EphemeralJob:
        """Make a throwaway job that holds one task of a bundle job.

        Each upstream task named in `upstream_values` is replaced by a stub task with the same key, so the task's
        `{{tasks.<key>.values.<name>}}` references and `taskValues.get` calls resolve against the stub.

        Args:
            ref: The job and task as `"job_key.task_key"`.
            params: Job parameters for every run.
            upstream_values: The task values each stub sets, by upstream task key.
            capture_values: The task value keys to capture from the task.

        Returns:
            The job, created when entered with `with` or on its first run.

        Raises:
            KeyError: If the bundle has no such job.
            LakeflowError: If the job has no such task, the task refers to task values of a task that is not
                stubbed, or a value is too large or not JSON-serialisable.
        """
        job_key, _, task_key = ref.partition(".")
        if not job_key or not task_key:
            raise LakeflowError(f"Expected 'job_key.task_key', got {ref!r}")
        settings = self.template(job_key)
        check_params(job_key, settings, params or {}, allow_undeclared=True)
        upstream = dict(upstream_values or {})
        wrap_task(settings, task_key, upstream, list(capture_values or []), self._helpers())
        capture = {task_key: list(capture_values)} if capture_values else {}
        return self._ephemeral(job_key, settings, params, capture, upstream, None)

    def _helpers(self) -> Helpers:
        return Helpers(
            set_values=self.helpers.get("set_values", ""),
            get_values=self.helpers.get("get_values", ""),
            environment_version=self.options.helpers_environment_version,
            job_cluster_key=self.options.helpers_compute,
        )

    def _ephemeral(
        self,
        key: str,
        settings: jobs.JobSettings,
        params: Mapping[str, str] | None,
        capture_values: Mapping[str, Sequence[str]] | None,
        stubs: Collection[str],
        name: str | None,
    ) -> EphemeralJob:
        request = self.current_request()
        if settings.performance_target == jobs.PerformanceTarget.STANDARD and not self.options.performance_optimized:
            warnings.warn(
                f"Job {key!r} has performance_target STANDARD, which adds 3 to 4 minutes to every run. "
                "Set lakeflow_performance_optimized = true to run tests with PERFORMANCE_OPTIMIZED.",
                stacklevel=3,
            )
        nodeid = request.node.nodeid
        sanitise(
            settings,
            name=name or nodeid,
            tags={"pytest": "", "pytest_run_id": self.run_id, "pytest_nodeid": nodeid},
            disable_auto_optimization=self.options.disable_auto_optimization,
            performance_optimized=self.options.performance_optimized,
        )
        job = EphemeralJob(self, key, settings, params, capture_values, stubs)
        request.addfinalizer(lambda: job.close(failed=item_failed(request.node.stash)))
        return job

    def sweep(self) -> list[int]:
        """Delete this user's ephemeral jobs from other runs that are older than `lakeflow_sweep_hours`.

        Returns:
            The ids of the deleted jobs.
        """
        me = self.ws.current_user.me().user_name
        cutoff = (time.time() - self.options.sweep_hours * 3600) * 1000
        stale = [
            job.job_id
            for job in self.ws.jobs.list(limit=100)
            if job.job_id is not None and job.creator_user_name == me and _stale(job, self.run_id, cutoff)
        ]
        deleted = []
        for job_id in stale:
            try:
                self.ws.jobs.delete(job_id)
            except DatabricksError as e:
                log.warning("Could not delete job %s: %s", job_id, e)
            else:
                deleted.append(job_id)
        log.info("Swept %d leftover ephemeral jobs", len(deleted))
        return deleted

    def cancel_active_runs(self) -> None:
        """Cancel every run started in this session that has not finished."""
        for run_id in sorted(self.active_runs):
            cancel_run(self.ws, run_id)
        self.active_runs.clear()


def _stale(job: jobs.BaseJob, run_id: str, cutoff_ms: float) -> bool:
    tags = (job.settings.tags if job.settings else None) or {}
    other_run = tags.get("pytest_run_id", run_id) != run_id
    return "pytest" in tags and other_run and (job.created_time or 0) < cutoff_ms
