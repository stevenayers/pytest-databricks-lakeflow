from __future__ import annotations

import json
import logging
import time
import uuid
from collections.abc import Collection, Mapping, Sequence
from datetime import timedelta
from types import TracebackType
from typing import TYPE_CHECKING, cast

import pytest
from databricks.sdk.errors.base import DatabricksError
from databricks.sdk.service import jobs

from pytest_databricks_lakeflow import session
from pytest_databricks_lakeflow.errors import LakeflowError, LakeflowTimeout
from pytest_databricks_lakeflow.results import (
    JobRun,
    build_job_run,
    describe_failing,
    duration,
    error_line,
    failing_parameters,
    fetch_outputs,
    format_parameters,
    has_output,
)
from pytest_databricks_lakeflow.settings import check_compute, check_params, create_kwargs, pipeline_params, select

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

    from pytest_databricks_lakeflow.client import Lakeflow

log = logging.getLogger("pytest_databricks_lakeflow")

POLL_SECONDS = 2.0
REPORTS = pytest.StashKey[dict[str, pytest.TestReport]]()
PROGRESS = pytest.StashKey[list[str]]()
TERMINATED = jobs.RunLifecycleStateV2State.TERMINATED
TERMINAL_LIFE_CYCLE = (
    jobs.RunLifeCycleState.TERMINATED,
    jobs.RunLifeCycleState.SKIPPED,
    jobs.RunLifeCycleState.INTERNAL_ERROR,
)

# The last state logged for each task attempt, keyed by task key and attempt number, and whether it had terminated.
LoggedStates = dict[tuple[str, int], tuple[str, bool]]


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
        capture_task_values: The task value keys captured by probes, by task key.
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
        capture_task_values: Mapping[str, Sequence[str]] | None = None,
        stubs: Collection[str] = (),
    ) -> None:
        self.lakeflow = lakeflow
        self.job_key = job_key
        self.settings = settings
        self.params = dict(params or {})
        self.capture_task_values = {key: list(keys) for key, keys in (capture_task_values or {}).items()}
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
        if self.job_id is None:
            return None
        return f"{self.lakeflow.ws.config.host}/jobs/{self.job_id}"

    def __enter__(self) -> EphemeralJob:
        self.create()
        self._entered = True
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        failed = exc_type is not None and not issubclass(exc_type, (pytest.skip.Exception, pytest.xfail.Exception))
        self.close(failed=failed)

    def create(self) -> None:
        """Create the job if it does not exist yet.

        Raises:
            LakeflowError: If the job has been closed, `params` names a parameter that the job does not
                declare after `modify_settings` has run, or a serverless task that needs an environment has none.
        """
        if self._closed:
            raise LakeflowError(f"The ephemeral job for {self.job_key!r} has already been closed")
        if self.job_id is not None:
            return
        item = session.current_item()
        if self.lakeflow.modify_settings is not None:
            self.lakeflow.modify_settings(self.settings, item)
        check_params(self.job_key, self.settings, self.params, self.lakeflow.allow_undeclared_params)
        check_compute(self.settings)
        acl = self.lakeflow.access_control_list()
        self.job_id = self.lakeflow.ws.jobs.create(**create_kwargs(self.settings), access_control_list=acl).job_id
        log.info("Created job %s from %s: %s", self.job_id, self.job_key, self.url)
        item.user_properties.append(("lakeflow_job_url", self.url))
        item.user_properties.append(("lakeflow_job_id", self.job_id))

    def run(
        self,
        params: Mapping[str, str] | None = None,
        only: Sequence[str] | None = None,
        timeout: timedelta | None = None,
        *,
        performance_target: jobs.PerformanceTarget | str | None = None,
        queue: bool | None = None,
        full_refresh: bool | None = None,
        full_refresh_selection: Sequence[str] | None = None,
        refresh_selection: Sequence[str] | None = None,
    ) -> JobRun:
        """Run the job and wait for it to finish, creating it first if needed.

        Every call sends a fresh idempotency token with the run request, so a request the SDK retries cannot
        start a second run.

        Args:
            params: Job parameters for this run only, merged over the job's params.
            only: Task keys to run. `+key`, `key+` and `+key+` add upstream and downstream tasks.
            timeout: How long to wait. Defaults to `Lakeflow`'s `timeout`. It must not be longer than the job's
                `timeout_seconds`, which `Lakeflow`'s `timeout` sets.
            performance_target: The performance target of this run only, such as `"PERFORMANCE_OPTIMIZED"`. It
                applies to serverless tasks and leaves the job's settings unchanged. Defaults to the job's own.
            queue: Whether this run may queue behind other runs. Defaults to the job's own queue setting.
            full_refresh: Whether the job's pipeline tasks run a full refresh in this run. Defaults to what each
                `pipeline_task` sets. The pipeline is the deployed one, not a copy.
            full_refresh_selection: Tables that the job's pipeline tasks fully refresh in this run. Needs
                databricks-sdk 0.110.0 or later.
            refresh_selection: Tables that the job's pipeline tasks refresh in this run. It applies to every
                pipeline task in the run. Needs databricks-sdk 0.110.0 or later.

        Returns:
            The run's outcome.

        Raises:
            LakeflowError: If the job has been closed, a parameter is not declared, `only` names an unknown
                task, `timeout` is longer than the job's `timeout_seconds`, a pipeline refresh option is set and
                the job has no pipeline task, or a selection is set and the installed databricks-sdk cannot send it.
            LakeflowTimeout: If the run did not finish in time. The run is cancelled first.
            ValueError: If `performance_target` is not a known performance target.
        """
        target = None if performance_target is None else jobs.PerformanceTarget(performance_target)
        queue_settings = None if queue is None else jobs.QueueSettings(enabled=queue)
        self.create()
        assert self.job_id is not None
        item = session.current_item()
        run_params = dict(params or {})
        check_params(self.job_key, self.settings, run_params, self.lakeflow.allow_undeclared_params)
        refresh = pipeline_params(
            self.settings,
            full_refresh=full_refresh,
            full_refresh_selection=full_refresh_selection,
            refresh_selection=refresh_selection,
        )
        if timeout is None:
            timeout = self.lakeflow.timeout
        else:
            self._check_timeout(timeout)
        merged = {**self.params, **run_params}
        selected = select(self.settings, only, self.stubs) if only else None
        ws = self.lakeflow.ws
        token = uuid.uuid4().hex
        waiter = ws.jobs.run_now(
            job_id=self.job_id,
            idempotency_token=token,
            job_parameters=merged or None,
            only=selected,
            performance_target=target,
            queue=queue_settings,
            pipeline_params=refresh,
        )
        run_id = cast(jobs.RunNowResponse, waiter.response).run_id
        assert run_id is not None
        self._active.add(run_id)
        self.lakeflow.active_runs.add(run_id)
        log.info("Started run %s of job %s", run_id, self.job_id)
        log.debug("Run %s was started with idempotency token %s", run_id, token)
        run, outputs = self._wait(run_id, timeout, item)
        self._forget(run_id)
        disabled = {task.task_key for task in self.settings.tasks or [] if task.disabled and task.task_key}
        result = build_job_run(
            ws,
            run,
            job_key=self.job_key,
            job_id=self.job_id,
            stubs=self.stubs,
            capture_task_values=self.capture_task_values,
            disabled=disabled,
            outputs=outputs,
            job_keys=self.lakeflow._job_keys(),
        )
        self.runs.append(result)
        _record_outcome(item, result)
        log.info("Run %s finished %s in %s: %s", run_id, result.result_state, result.duration, result.run_url)
        if self.lakeflow.run_finished is not None:
            self.lakeflow.run_finished(result, item)
        return result

    def _check_timeout(self, timeout: timedelta) -> None:
        limit = self.settings.timeout_seconds
        if limit and timeout.total_seconds() > limit:
            raise LakeflowError(
                f"run(timeout={timeout}) of {self.job_key!r} is longer than the job's timeout_seconds of {limit} "
                f"({timedelta(seconds=limit)}), so the platform would stop the run first. Pass a longer "
                "Lakeflow(timeout=...), which sets the job's timeout, or a shorter run timeout."
            )

    def _wait(self, run_id: int, timeout: timedelta, item: pytest.Item) -> tuple[jobs.Run, dict[int, jobs.RunOutput]]:
        ws = self.lakeflow.ws
        deadline = time.monotonic() + timeout.total_seconds()
        logged: LoggedStates = {}
        outputs: dict[int, jobs.RunOutput] = {}
        progress = item.stash.setdefault(PROGRESS, [])
        while True:
            run = ws.jobs.get_run(run_id, include_resolved_values=True)
            if run_id not in self._run_urls and run.run_page_url:
                self._run_urls[run_id] = run.run_page_url
                item.user_properties.append(("lakeflow_run_url", run.run_page_url))
            self._log_progress(run, logged, outputs, progress)
            if _terminated(run):
                return run, outputs
            if time.monotonic() >= deadline:
                self._cancel(run_id)
                raise LakeflowTimeout(
                    f"Run {run_id} of {self.job_key!r} did not finish within {timeout} and was cancelled: "
                    f"{run.run_page_url}"
                )
            time.sleep(POLL_SECONDS)

    def _log_progress(
        self, run: jobs.Run, logged: LoggedStates, outputs: dict[int, jobs.RunOutput], progress: list[str]
    ) -> None:
        """Log each task run whose state changed since the last poll, reading the output of those that finished.

        A task attempt that was logged as terminated is not logged again, so a result state that appears on a later
        poll does not log a second line.
        """
        states: list[tuple[jobs.RunTask, str | None, bool]] = []
        finished: list[jobs.RunTask] = []
        for task in run.tasks or []:
            state, done = _task_state(task)
            states.append((task, state, done))
            if done and task.run_id is not None and task.run_id not in outputs and has_output(task):
                finished.append(task)
        outputs.update(fetch_outputs(self.lakeflow.ws, finished))
        for task, state, done in states:
            key = (task.task_key or "", task.attempt_number or 0)
            previous, was_done = logged.get(key, (None, False))
            if state is None or was_done or previous == state:
                continue
            line = _transition(task, previous, state, outputs.get(task.run_id or 0), done)
            logged[key] = (state, done)
            log.info("%s", line)
            progress.append(line)

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
        state = session.current()
        if state.keeps(self.lakeflow.keep, self.lakeflow.keep_failed, failed):
            log.warning("Keeping job %s from %s: %s", self.job_id, self.job_key, self.url)
            state.kept_jobs.append(str(self.url))
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


def _record_outcome(item: pytest.Item, result: JobRun) -> None:
    """Record the run's outcome in the test's user properties, with its resolved parameters when it failed."""
    item.user_properties.extend(
        [
            ("lakeflow_run_id", result.run_id),
            ("lakeflow_result_state", result.result_state),
            ("lakeflow_duration", f"{result.duration.total_seconds():.3f}"),
        ]
    )
    if not result.failed:
        return
    item.user_properties.append(("lakeflow_failing", "; ".join(describe_failing(result))))
    run_params = format_parameters(result.parameters)
    if run_params:
        item.user_properties.append(("lakeflow_params", run_params))
    task_params = failing_parameters(result)
    if task_params:
        shown = {key: json.loads(text) for key, text in task_params.items()}
        item.user_properties.append(("lakeflow_task_params", json.dumps(shown, sort_keys=True)))


def _terminated(run: jobs.Run) -> bool:
    if run.status and run.status.state:
        return run.status.state == TERMINATED
    state = run.state.life_cycle_state if run.state else None
    return state in TERMINAL_LIFE_CYCLE


def _task_state(task: jobs.RunTask) -> tuple[str | None, bool]:
    """The state to report for a task run, and whether it has terminated.

    The V2 `status.state` while running and the result state once terminated, falling back to the legacy life cycle
    state when the run has no V2 status. Any field may be missing.
    """
    result = task.state.result_state.value if task.state and task.state.result_state else None
    if task.status is not None and task.status.state is not None:
        status = task.status.state
        if status != TERMINATED:
            return status.value, False
        details = task.status.termination_details
        code = details.code.value if details and details.code else None
        return result or code or status.value, True
    life_cycle = task.state.life_cycle_state if task.state else None
    if life_cycle in TERMINAL_LIFE_CYCLE:
        return result or life_cycle.value, True
    if life_cycle is None:
        return None, False
    return life_cycle.value, False


def _transition(task: jobs.RunTask, previous: str | None, state: str, output: jobs.RunOutput | None, done: bool) -> str:
    """Describe a task run's change of state, such as `transform: RUNNING -> FAILED in 0:00:42 ValueError: x`."""
    label = task.task_key or "?"
    if task.attempt_number:
        label = f"{label} (attempt {task.attempt_number})"
    line = f"{label}: {state}"
    if previous:
        line = f"{label}: {previous} -> {state}"
    if not done:
        return line
    elapsed = duration(task.start_time, task.end_time)
    if elapsed:
        line = f"{line} in {timedelta(seconds=int(elapsed.total_seconds()))}"
    error = error_line(task, output)
    if error:
        line = f"{line} {error}"
    return line


def item_failed(stash: pytest.Stash) -> bool:
    return any(report.failed for report in stash.get(REPORTS, {}).values())
