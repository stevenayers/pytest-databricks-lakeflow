from __future__ import annotations

import logging
import time
from collections.abc import Collection, Mapping, Sequence
from datetime import timedelta
from types import TracebackType
from typing import TYPE_CHECKING, cast

import pytest
from databricks.sdk.errors.base import DatabricksError
from databricks.sdk.service import jobs

from pytest_databricks_lakeflow import session
from pytest_databricks_lakeflow.errors import LakeflowError, LakeflowTimeout
from pytest_databricks_lakeflow.results import JobRun, build_job_run
from pytest_databricks_lakeflow.settings import check_params, create_kwargs, select

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

    from pytest_databricks_lakeflow.client import Lakeflow

log = logging.getLogger("pytest_databricks_lakeflow")

POLL_SECONDS = 2.0
REPORTS = pytest.StashKey[dict[str, pytest.TestReport]]()


class EphemeralJob:
    """A throwaway copy of a bundle job.

    The job is created on entering the `with` block, or by the first `run()` when used without `with`. It is
    deleted on leaving the block, or at the end of the test that made it, unless `keep` is set or the test failed
    and `keep_failed` is on.

    Attributes:
        lakeflow: The `Lakeflow` that made the job.
        job_key: The bundle resource key of the job it copies.
        settings: The settings the job is created with.
        params: Job parameters for every run.
        capture_values: The task value keys captured by probes, by task key.
        stubs: Keys of the stub tasks.
        job_id: The job's id, once created.
        runs: The outcome of each finished run, in order.
    """

    def __init__(
        self,
        lakeflow: Lakeflow,
        job_key: str,
        settings: jobs.JobSettings,
        params: Mapping[str, str] | None = None,
        capture_values: Mapping[str, Sequence[str]] | None = None,
        stubs: Collection[str] = (),
    ) -> None:
        self.lakeflow = lakeflow
        self.job_key = job_key
        self.settings = settings
        self.params = dict(params or {})
        self.capture_values = {key: list(keys) for key, keys in (capture_values or {}).items()}
        self.stubs = frozenset(stubs)
        self.job_id: int | None = None
        self.runs: list[JobRun] = []
        self._run_urls: dict[int, str] = {}
        self._active: set[int] = set()
        self._entered = False
        self._closed = False

    @property
    def url(self) -> str | None:
        """The job's page in the workspace, or `None` before it is created."""
        return None if self.job_id is None else f"{self.lakeflow.ws.config.host}/jobs/{self.job_id}"

    def __enter__(self) -> EphemeralJob:
        self.create()
        self._entered = True
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        skipped = exc_type is not None and issubclass(exc_type, (pytest.skip.Exception, pytest.xfail.Exception))
        self.close(failed=exc_type is not None and not skipped)

    def create(self) -> None:
        """Create the job if it does not exist yet.

        Raises:
            LakeflowError: If the job has been closed, or `params` names a parameter that the job does not
                declare after `modify_settings` has run.
        """
        if self._closed:
            raise LakeflowError(f"The ephemeral job for {self.job_key!r} has already been closed")
        if self.job_id is not None:
            return
        item = session.current_item()
        if self.lakeflow.modify_settings is not None:
            self.lakeflow.modify_settings(self.settings, item)
        check_params(self.job_key, self.settings, self.params, self.lakeflow.allow_undeclared_params)
        self.job_id = self.lakeflow.ws.jobs.create(**create_kwargs(self.settings)).job_id
        log.info("Created job %s from %s: %s", self.job_id, self.job_key, self.url)
        item.user_properties.append(("lakeflow_job_url", self.url))

    def run(
        self,
        params: Mapping[str, str] | None = None,
        only: Sequence[str] | None = None,
        timeout: timedelta | None = None,
    ) -> JobRun:
        """Run the job and wait for it to finish, creating it first if needed.

        Args:
            params: Job parameters for this run only, merged over the job's params.
            only: Task keys to run. `+key`, `key+` and `+key+` add upstream and downstream tasks.
            timeout: How long to wait. Defaults to `Lakeflow`'s `timeout`.

        Returns:
            The run's outcome.

        Raises:
            LakeflowError: If the job has been closed, a parameter is not declared, or `only` names an unknown
                task.
            LakeflowTimeout: If the run did not finish in time. The run is cancelled first.
        """
        self.create()
        assert self.job_id is not None
        item = session.current_item()
        check_params(self.job_key, self.settings, params or {}, self.lakeflow.allow_undeclared_params)
        merged = {**self.params, **(params or {})}
        selected = select(self.settings, only, self.stubs) if only else None
        ws = self.lakeflow.ws
        waiter = ws.jobs.run_now(job_id=self.job_id, job_parameters=merged or None, only=selected)
        run_id = cast(jobs.RunNowResponse, waiter.response).run_id
        assert run_id is not None
        self._active.add(run_id)
        self.lakeflow.active_runs.add(run_id)
        log.info("Started run %s of job %s", run_id, self.job_id)
        run = self._wait(run_id, self.lakeflow.timeout if timeout is None else timeout, item)
        self._forget(run_id)
        result = build_job_run(
            ws, run, job_key=self.job_key, job_id=self.job_id, stubs=self.stubs, capture_values=self.capture_values
        )
        self.runs.append(result)
        log.info("Run %s finished %s in %s: %s", run_id, result.result_state, result.duration, result.run_url)
        if self.lakeflow.run_finished is not None:
            self.lakeflow.run_finished(result, item)
        return result

    def _wait(self, run_id: int, timeout: timedelta, item: pytest.Item) -> jobs.Run:
        ws = self.lakeflow.ws
        deadline = time.monotonic() + timeout.total_seconds()
        while True:
            run = ws.jobs.get_run(run_id)
            if run_id not in self._run_urls and run.run_page_url:
                self._run_urls[run_id] = run.run_page_url
                item.user_properties.append(("lakeflow_run_url", run.run_page_url))
            if _terminated(run):
                return run
            if time.monotonic() >= deadline:
                self._cancel(run_id)
                raise LakeflowTimeout(
                    f"Run {run_id} of {self.job_key!r} did not finish within {timeout} and was cancelled: "
                    f"{run.run_page_url}"
                )
            time.sleep(POLL_SECONDS)

    def close(self, failed: bool = False) -> None:
        """Cancel active runs, then delete the job unless the keep rules say to keep it.

        Args:
            failed: Whether the test failed, which keeps the job when `keep_failed` is on.
        """
        if self._closed:
            return
        self._closed = True
        for run_id in sorted(self._active):
            self._cancel(run_id)
        if self.job_id is None:
            return
        if session.current().keeps(self.lakeflow.keep, self.lakeflow.keep_failed, failed):
            log.warning("Keeping job %s from %s: %s", self.job_id, self.job_key, self.url)
            for url in self._run_urls.values():
                log.warning("Kept run: %s", url)
            return
        self.lakeflow.ws.jobs.delete(self.job_id)
        log.info("Deleted job %s", self.job_id)

    def _teardown(self, item: pytest.Item) -> None:
        """Close the job at the end of the test that made it, unless a `with` block closes it."""
        if not self._entered:
            self.close(failed=item_failed(item.stash))

    def _cancel(self, run_id: int) -> None:
        cancel_run(self.lakeflow.ws, run_id)
        self._forget(run_id)

    def _forget(self, run_id: int) -> None:
        self._active.discard(run_id)
        self.lakeflow.active_runs.discard(run_id)


def cancel_run(ws: WorkspaceClient, run_id: int) -> None:
    try:
        ws.jobs.cancel_run(run_id)
        log.info("Cancelled run %s", run_id)
    except DatabricksError as e:
        log.warning("Could not cancel run %s: %s", run_id, e)


def _terminated(run: jobs.Run) -> bool:
    if run.status and run.status.state:
        return run.status.state == jobs.RunLifecycleStateV2State.TERMINATED
    state = run.state.life_cycle_state if run.state else None
    return state in (
        jobs.RunLifeCycleState.TERMINATED,
        jobs.RunLifeCycleState.SKIPPED,
        jobs.RunLifeCycleState.INTERNAL_ERROR,
    )


def item_failed(stash: pytest.Stash) -> bool:
    return any(report.failed for report in stash.get(REPORTS, {}).values())
