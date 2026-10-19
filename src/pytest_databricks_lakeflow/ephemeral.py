from __future__ import annotations

import json
import logging
import time
import uuid
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from types import TracebackType
from typing import TYPE_CHECKING, cast

import pytest
from databricks.sdk.errors import PermissionDenied
from databricks.sdk.errors.base import DatabricksError
from databricks.sdk.service import jobs

from pytest_databricks_lakeflow import helpers, session
from pytest_databricks_lakeflow.errors import LakeflowError, LakeflowTimeout
from pytest_databricks_lakeflow.results import (
    PREFIX,
    DebugNotebook,
    JobRun,
    build_job_run,
    describe_failing,
    describe_notebooks,
    duration,
    error_line,
    failing_parameters,
    fetch_outputs,
    format_parameters,
    has_output,
    task_outcome,
)
from pytest_databricks_lakeflow.settings import (
    TIMEOUT_GRACE_SECONDS,
    check_compute,
    check_params,
    check_references,
    create_kwargs,
    disabled_task_keys,
    job_timeout_seconds,
    name_lifecycle_params,
    pipeline_params,
    require_task,
    rewire_teardown,
    select,
    selected_tasks,
    stub_upstream,
    upstream_values_from,
)

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

    from pytest_databricks_lakeflow.client import Lakeflow

log = logging.getLogger("pytest_databricks_lakeflow")

POLL_SECONDS = 2.0
ROLE_FIX = (
    "Ask a manager of that service principal to grant you the role, or leave run_as out to run the copy as yourself."
)
REPORTS = pytest.StashKey[dict[str, pytest.TestReport]]()
PROGRESS = pytest.StashKey[list[str]]()
NOTEBOOKS = pytest.StashKey[list[str]]()
RUNS = pytest.StashKey[list[tuple["EphemeralJob", JobRun]]]()
TERMINATED = jobs.RunLifecycleStateV2State.TERMINATED
TERMINAL_LIFE_CYCLE = (
    jobs.RunLifeCycleState.TERMINATED,
    jobs.RunLifeCycleState.SKIPPED,
    jobs.RunLifeCycleState.INTERNAL_ERROR,
)

# The last state logged for each task attempt, keyed by task key and attempt number, and whether it had terminated.
LoggedStates = dict[tuple[str, int], tuple[str, bool]]


@dataclass
class _Polling:
    """What polling one run has seen: the task states logged, the outputs read, and whether the plugin cancelled it."""

    run_id: int
    item: pytest.Item
    logged: LoggedStates = field(default_factory=dict)
    outputs: dict[int, jobs.RunOutput] = field(default_factory=dict)
    cancelled: bool = False


class EphemeralJob:
    """A throwaway copy of a bundle job.

    The job is created on entering the `with` block, or by the first `run()` or `start()` when used without `with`.
    It is deleted on leaving the block, or at the end of the test that made it, unless `keep` is set or the test
    failed and `keep_failed` is on. Runs that are still active then are cancelled first.

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
        self._active: dict[int, pytest.Item] = {}
        self._entered = False
        self._closed = False
        self._timeout_raised = False

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
                Also if the job runs as a service principal and you do not have the Service Principal User role on
                it, or `jobs.create` refuses the job with `PERMISSION_DENIED`, with the platform's error as its
                cause. Without a service principal in `run_as`, errors from `jobs.create` are raised unchanged.
        """
        if self._closed:
            raise LakeflowError(f"The ephemeral job for {self.job_key!r} has already been closed")
        if self.job_id is not None:
            return
        item = session.current_item()
        if self.lakeflow.modify_settings is not None:
            self.lakeflow.modify_settings(self.settings, item)
            name_lifecycle_params(self.settings, self.params)
        check_params(self.job_key, self.settings, self.params, self.lakeflow.allow_undeclared_params)
        check_compute(self.settings)
        self.job_id = self._create_job()
        log.info("Created job %s from %s: %s", self.job_id, self.job_key, self.url)
        item.user_properties.append(("lakeflow_job_url", self.url))
        item.user_properties.append(("lakeflow_job_id", self.job_id))

    def _create_job(self) -> int | None:
        """Create the job, checking first that its creator may make it run as the service principal it names."""
        ws = self.lakeflow.ws
        acl = self.lakeflow.access_control_list()
        application_id = None
        if self.settings.run_as is not None:
            application_id = self.settings.run_as.service_principal_name
        if application_id and session.check_sp_user_role(ws, application_id) is False:
            raise LakeflowError(
                f"{self._creator()} does not have the Service Principal User role on {application_id}, so the copy "
                f"of {self.job_key!r} cannot run as it. {ROLE_FIX}"
            )
        try:
            return ws.jobs.create(**create_kwargs(self.settings), access_control_list=acl).job_id
        except PermissionDenied as e:
            if not application_id:
                raise
            raise LakeflowError(
                f"Creating the copy of {self.job_key!r} with run_as {application_id} was refused: {e} To create a "
                f"job that runs as a service principal, {self._creator()} needs the Service Principal User role on "
                f"it. {ROLE_FIX}"
            ) from e

    def _creator(self) -> str:
        return self.lakeflow._state()["user"]

    def run(
        self,
        params: Mapping[str, str] | None = None,
        only: Sequence[str] | None = None,
        timeout: timedelta | None = None,
        *,
        stop_after: timedelta | None = None,
        performance_target: jobs.PerformanceTarget | str | None = None,
        queue: bool | None = None,
        full_refresh: bool | None = None,
        full_refresh_selection: Sequence[str] | None = None,
        refresh_selection: Sequence[str] | None = None,
        refresh_flow_selection: Sequence[str] | None = None,
        reset_checkpoint_selection: Sequence[str] | None = None,
        replay: JobRun | None = None,
    ) -> JobRun:
        """Run the job and wait for it to finish, creating it first if needed.

        Every call sends a fresh idempotency token with the run request, so a request the SDK retries cannot
        start a second run. It is `start(params, only, timeout=timeout, ...).wait(stop_after=stop_after)`.

        Args:
            params: Job parameters for this run only, merged over the job's params.
            only: Task keys to run. `+key`, `key+` and `+key+` add upstream and downstream tasks.
            timeout: How long to wait. Defaults to `Lakeflow`'s `timeout`. When this plus 60 seconds is longer than
                the job's `timeout_seconds`, which `Lakeflow`'s `timeout` sets, the job's `timeout_seconds` is
                raised to it before the run starts, with a warning when the template or `modify_settings` set the
                shorter value.
            stop_after: How long the run may go on before it is cancelled, for a job that does not end on its own,
                such as a streaming job. The run is then polled until it has ended and returned as a `JobRun` whose
                `cancelled` is true, instead of raising. It counts from the start of the run and must not be longer
                than `timeout`.
            performance_target: The performance target of this run only, such as `"PERFORMANCE_OPTIMIZED"`. It
                applies to serverless tasks and leaves the job's settings unchanged. Defaults to the job's own.
            queue: Whether this run may queue behind other runs. Defaults to the job's own queue setting.
            full_refresh: Whether the job's pipeline tasks run a full refresh in this run. Defaults to what each
                `pipeline_task` sets. The pipeline is the deployed one, not a copy.
            full_refresh_selection: Tables that the job's pipeline tasks fully refresh in this run. Needs
                databricks-sdk 0.110.0 or later.
            refresh_selection: Tables that the job's pipeline tasks refresh in this run. It applies to every
                pipeline task in the run. Needs databricks-sdk 0.110.0 or later.
            refresh_flow_selection: Flows that the job's pipeline tasks refresh in this run, by flow name. Needs
                databricks-sdk 0.110.0 or later.
            reset_checkpoint_selection: Streaming flows whose checkpoints the job's pipeline tasks reset in this run,
                so that they process their source again from the start. Needs databricks-sdk 0.110.0 or later.
            replay: A run of the same job whose captured task values stub the tasks that `only` leaves out and a
                selected task depends on, as `upstream_task_values` on `job()` does. The platform returns no task
                values with a run, so that run must have captured them, with
                `capture_task_values={"<upstream key>": [...]}` on `job()`. The stubs are part of the job's settings,
                so `replay` needs `only` and a job that has not been created yet: pass it to the first run of a new
                `lakeflow.job(...)`.

        Returns:
            The run's outcome.

        Raises:
            LakeflowError: If the job has been closed, a parameter is not declared, `only` names an unknown
                task, a selected task refers to the task values of a task that `only` leaves out and no stub
                replaces, a pipeline refresh option is set and the job has no pipeline task, a selection is set and
                the installed databricks-sdk cannot send it, or `stop_after` is longer than `timeout`. Also if
                `replay` is passed without `only` or to a job that has been created, is a run of another job, or
                lacks the captured values of a task it would stub. Each is raised before the run starts.
            LakeflowTimeout: If the run did not finish in time. The run is cancelled first.
            ValueError: If `performance_target` is not a known performance target.
        """
        if stop_after is not None:
            limit = timeout
            if limit is None:
                limit = self.lakeflow.timeout
            _check_stop_after(self.job_key, stop_after, limit)
        if replay is not None:
            self._replay(replay, only)
        handle = self.start(
            params,
            only,
            timeout=timeout,
            performance_target=performance_target,
            queue=queue,
            full_refresh=full_refresh,
            full_refresh_selection=full_refresh_selection,
            refresh_selection=refresh_selection,
            refresh_flow_selection=refresh_flow_selection,
            reset_checkpoint_selection=reset_checkpoint_selection,
        )
        return handle.wait(stop_after=stop_after)

    def start(
        self,
        params: Mapping[str, str] | None = None,
        only: Sequence[str] | None = None,
        *,
        timeout: timedelta | None = None,
        performance_target: jobs.PerformanceTarget | str | None = None,
        queue: bool | None = None,
        full_refresh: bool | None = None,
        full_refresh_selection: Sequence[str] | None = None,
        refresh_selection: Sequence[str] | None = None,
        refresh_flow_selection: Sequence[str] | None = None,
        reset_checkpoint_selection: Sequence[str] | None = None,
    ) -> RunHandle:
        """Start a run of the job without waiting for it, creating the job first if needed.

        The run is cancelled when the job closes, unless `RunHandle.wait()` has returned by then. Its page is
        recorded in the test's report straight away, so a run that is never waited for still appears there, and
        with `--lakeflow-report-dir` the cancelled run is listed in report.json. The job allows one run at a time,
        so start runs of different jobs to run them side by side.

        Args:
            params: Job parameters for this run only, as for `run()`.
            only: Task keys to run, as for `run()`.
            timeout: How long `RunHandle.wait()` waits when it is given no timeout of its own. Defaults to
                `Lakeflow`'s `timeout`. As for `run()`, when this plus 60 seconds is longer than the job's
                `timeout_seconds`, the job's `timeout_seconds` is raised to it before the run starts.
            performance_target: The performance target of this run only, as for `run()`.
            queue: Whether this run may queue behind other runs, as for `run()`.
            full_refresh: Whether the job's pipeline tasks run a full refresh in this run, as for `run()`.
            full_refresh_selection: Tables that the job's pipeline tasks fully refresh in this run, as for `run()`.
            refresh_selection: Tables that the job's pipeline tasks refresh in this run, as for `run()`.
            refresh_flow_selection: Flows that the job's pipeline tasks refresh in this run, as for `run()`.
            reset_checkpoint_selection: Streaming flows whose checkpoints the job's pipeline tasks reset in this run,
                as for `run()`.

        Returns:
            A handle to wait for the run or cancel it.

        Raises:
            LakeflowError: If the job has been closed, a parameter is not declared, `only` names an unknown
                task, a selected task refers to the task values of a task that `only` leaves out and no stub
                replaces, a pipeline refresh option is set and the job has no pipeline task, or a selection is set
                and the installed databricks-sdk cannot send it.
            ValueError: If `performance_target` is not a known performance target.
        """
        target = _performance_target(performance_target)
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
            refresh_flow_selection=refresh_flow_selection,
            reset_checkpoint_selection=reset_checkpoint_selection,
        )
        merged = {**self.params, **run_params}
        selected = None
        if only:
            check_references(self.settings, selected_tasks(self.settings, only, self.stubs), self.stubs)
            selected = select(self.settings, only, self.stubs)
        if timeout is not None:
            self._raise_timeout(timeout)
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
        self._active[run_id] = item
        self.lakeflow.active_runs.add(run_id)
        log.info("Started run %s of job %s", run_id, self.job_id)
        log.debug("Run %s was started with idempotency token %s", run_id, token)
        first = ws.jobs.get_run(run_id)
        self._record_url(run_id, first, item)
        return RunHandle(self, run_id, item, first, timeout)

    def _replay(self, run: JobRun, only: Sequence[str] | None) -> None:
        """Stub the tasks that `only` leaves out and a selected task depends on, with their values in `run`."""
        if self.job_id is not None:
            raise LakeflowError(
                "replay stubs tasks in the job's settings, so it must be passed before the job is created, and job "
                f"{self.job_id} of {self.job_key!r} already exists. Pass replay to the first run of a new "
                "lakeflow.job(...)."
            )
        if not only:
            raise LakeflowError("replay stubs the tasks that only leaves out, so it needs only")
        selected = selected_tasks(self.settings, only, self.stubs)
        explicit = selected | self.stubs
        for task in self.settings.tasks or []:
            if task.task_key.startswith(PREFIX):
                explicit.add(task.task_key)
        upstream: dict[str, dict[str, object]] = {}
        for task_key in sorted(selected - self.stubs):
            # `key+` selects the teardown task, which depends on every leaf; injected tasks read no task values.
            if task_key.startswith(PREFIX):
                continue
            values = upstream_values_from(self.job_key, self.settings, task_key, run, explicit, argument="replay")
            upstream.update(values)
        if not upstream:
            return
        helpers = self.lakeflow._helpers()
        if helpers.job_cluster_key and self.lakeflow.existing_cluster_id:
            # sanitise has already moved the job clusters to the existing cluster, so the helper cluster is gone.
            raise LakeflowError(
                "replay cannot add stubs when helpers_compute and existing_cluster_id are both set. Pass the "
                "values in upstream_task_values on job() instead."
            )
        stub_upstream(self.settings, upstream, helpers)
        # The stubs drop their depends_on after inject_lifecycle chose the teardown's, so it is chosen again.
        rewire_teardown(self.settings)
        for key in upstream:
            stub = require_task(self.settings, key)
            # sanitise has already run, so the stubs get what it gives serverless helper tasks.
            if self.lakeflow.disable_auto_optimization and not stub.job_cluster_key:
                stub.disable_auto_optimization = True
        self.stubs = self.stubs | upstream.keys()

    def _raise_timeout(self, timeout: timedelta) -> None:
        """Raise the job's `timeout_seconds` to `timeout` plus the grace period when it is shorter."""
        limit = self.settings.timeout_seconds
        needed = job_timeout_seconds(timeout)
        if not limit or needed <= limit:
            return
        assert self.job_id is not None
        # Without tasks in the body, jobs/update reads a one-task job in the legacy single-task format and sets
        # timeout_seconds on the task, leaving the job's own unchanged. Sending the tasks the job was created with
        # changes only the job's.
        changes = jobs.JobSettings(timeout_seconds=needed, tasks=self.settings.tasks)
        self.lakeflow.ws.jobs.update(self.job_id, new_settings=changes)
        level = logging.INFO
        source = ""
        # Any other value than the one Lakeflow's timeout sets came from the template or modify_settings.
        if limit != job_timeout_seconds(self.lakeflow.timeout) and not self._timeout_raised:
            level = logging.WARNING
            source = ", which the template or modify_settings set,"
        log.log(
            level,
            "Raised timeout_seconds of job %s from %s%s to %s for a run timeout of %s",
            self.job_id,
            limit,
            source,
            needed,
            timeout,
        )
        self.settings.timeout_seconds = needed
        self._timeout_raised = True

    def _check_timeout(self, timeout: timedelta) -> None:
        limit = self.settings.timeout_seconds
        if limit and timeout.total_seconds() > limit:
            raise LakeflowError(
                f"wait(timeout={timeout}) of {self.job_key!r} is longer than the job's timeout_seconds of {limit} "
                f"({timedelta(seconds=limit)}), so the platform would stop the run first. Pass the timeout to "
                "run(timeout=...) or start(timeout=...), which raise the job's timeout before the run starts, or a "
                "shorter wait timeout."
            )

    def _wait(
        self,
        run_id: int,
        timeout: timedelta,
        item: pytest.Item,
        first: jobs.Run | None = None,
        first_at: float = 0.0,
        stop_after: timedelta | None = None,
    ) -> tuple[jobs.Run, dict[int, jobs.RunOutput]]:
        """Poll the run until it terminates, starting from `first`, the poll that `start()` made at `first_at`.

        With `stop_after`, the run is cancelled once `stop_after` has passed since `first_at` and polled until it
        terminates. When `timeout` expires first, the run is cancelled, unless `stop_after` already did that, reported
        with `--lakeflow-report-dir`, and `LakeflowTimeout` is raised.
        """
        deadline = time.monotonic() + timeout.total_seconds()
        stop_at = None
        if stop_after is not None:
            stop_at = first_at + stop_after.total_seconds()
        # A snapshot older than one poll interval is polled again at once, without a sleep or the deadline check, so
        # a wait() long after start() neither sleeps on it nor times out a run that has finished since.
        stale = first is not None and time.monotonic() - first_at >= self._poll_interval()
        polling = _Polling(run_id, item)
        run = self._poll(polling, deadline, first, stale, stop_at)
        if _terminated(run):
            return run, polling.outputs
        if not polling.cancelled:
            cancel_run(self.lakeflow.ws, run_id)
        self._forget(run_id)
        message = (
            f"Run {run_id} of {self.job_key!r} did not finish within {timeout} and was cancelled: "
            f"{run.run_page_url}\nTasks when cancelled: {_task_states(run)}"
        )
        self._report_cancelled(polling)
        raise LakeflowTimeout(message)

    def _poll(
        self,
        polling: _Polling,
        deadline: float,
        first: jobs.Run | None = None,
        stale: bool = False,
        stop_at: float | None = None,
    ) -> jobs.Run:
        """Poll the run until it terminates or `deadline` passes, and return the last snapshot.

        The run is cancelled once `stop_at` passes, and polled on until it terminates. Like the deadline, `stop_at` is
        checked only after a fresh poll, so a `stale` snapshot is polled again first.
        """
        ws = self.lakeflow.ws
        interval = self._poll_interval()
        progress = polling.item.stash.setdefault(PROGRESS, [])
        run = first
        if run is None:
            run = ws.jobs.get_run(polling.run_id)
        while True:
            self._record_url(polling.run_id, run, polling.item)
            self._log_progress(run, polling.logged, polling.outputs, progress)
            if _terminated(run):
                return run
            if not stale:
                now = time.monotonic()
                if stop_at is not None and now >= stop_at and not polling.cancelled:
                    # The run stays active until it has terminated, so a job that closes meanwhile still cancels it.
                    cancel_run(ws, polling.run_id)
                    polling.cancelled = True
                if now >= deadline:
                    return run
                time.sleep(interval)
            stale = False
            run = ws.jobs.get_run(polling.run_id)

    def _poll_interval(self) -> float:
        if self.lakeflow.poll_interval is not None:
            return self.lakeflow.poll_interval.total_seconds()
        return POLL_SECONDS

    def _report_cancelled(self, polling: _Polling) -> None:
        """List a run the plugin cancelled in the report.json of the test that started it, with `--lakeflow-report-dir`.

        The run is neither appended to `runs` nor passed to `run_finished`.
        """
        if session.current().report_dir is None:
            return
        try:
            result = self._read_cancelled(polling)
        # The report never stops the job from being deleted, nor hides the LakeflowTimeout about to be raised.
        except Exception as e:
            log.warning("Could not read cancelled run %s of %r for the report: %s", polling.run_id, self.job_key, e)
            return
        if result is not None:
            polling.item.stash.setdefault(RUNS, []).append((self, result))

    def _read_cancelled(self, polling: _Polling) -> JobRun | None:
        """Poll a cancelled run until it terminates, for at most the grace period, and build its `JobRun`."""
        run = self._poll(polling, time.monotonic() + TIMEOUT_GRACE_SECONDS)
        if _terminated(run):
            return self._job_run(run, polling.outputs)
        log.warning(
            "Run %s of %r had not ended %s seconds after it was cancelled, so report.json does not list it: %s",
            polling.run_id,
            self.job_key,
            TIMEOUT_GRACE_SECONDS,
            run.run_page_url,
        )
        return None

    def _job_run(self, run: jobs.Run, outputs: Mapping[int, jobs.RunOutput]) -> JobRun:
        """Build the `JobRun` of a terminated run of this job, from the task outputs read while polling."""
        assert self.job_id is not None
        return build_job_run(
            self.lakeflow.ws,
            run,
            job_key=self.job_key,
            job_id=self.job_id,
            stubs=self.stubs,
            capture_task_values=self.capture_task_values,
            disabled=disabled_task_keys(self.settings),
            outputs=outputs,
            job_keys=self.lakeflow._job_keys(),
            notebook_export=self.lakeflow.notebook_export,
        )

    def _record_url(self, run_id: int, run: jobs.Run, item: pytest.Item) -> None:
        if run_id in self._run_urls or not run.run_page_url:
            return
        self._run_urls[run_id] = run.run_page_url
        item.user_properties.append(("lakeflow_run_url", run.run_page_url))

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

    def debug_notebook(self, task_key: str, run: JobRun | None = None) -> DebugNotebook:
        """Write a notebook that reproduces a notebook task of a run, to debug it interactively.

        The notebook sets the task's resolved parameters as widgets and runs the task's notebook with `%run`. It is
        written to `<helpers_path>/debug/<job_key>__<task_key>__<run_id>`, replacing any notebook of the same run,
        and its URL is recorded in the test's report as `lakeflow_debug_notebook`. It replays the parameters only:
        a notebook that reads upstream task values needs `debugValue=` in `dbutils.jobs.taskValues.get()`.

        Args:
            task_key: The notebook task to reproduce.
            run: The run whose parameters to replay. Defaults to the job's last run.

        Returns:
            The notebook written.

        Raises:
            LakeflowError: If the job has no task `task_key`, the task is not a notebook task or runs its notebook
                from Git, the job has not run, or the task has no resolved parameters in the run, as when it did not
                run.
            KeyError: If the task was not part of the run, as `JobRun.task()` raises.
        """
        notebook = self._notebook_task(task_key)
        if run is None:
            if not self.runs:
                raise LakeflowError(
                    f"Job {self.job_key!r} has not run yet, so there are no resolved parameters to replay"
                )
            run = self.runs[-1]
        task = run.task(task_key)
        if not task.ran:
            raise LakeflowError(
                f"Task {task_key!r} did not run in run {run.run_id} ({task.result_state}), so it has no resolved "
                "parameters"
            )
        parameters = task.parameters
        if not isinstance(parameters, dict):
            raise LakeflowError(
                f"Task {task_key!r} has no resolved parameters in run {run.run_id}: the platform returned none, or "
                "the task run could not be read"
            )
        heading = (
            f"Parameters of task {task_key!r} in run {run.run_id} of job {self.job_key!r} ({task.run_url}), as the "
            "platform resolved them"
        )
        source = helpers.debug_notebook_source(notebook.notebook_path, parameters, heading=heading)
        folder = session.folder_of_helpers(self.lakeflow._state())
        path = f"{folder}/{helpers.DEBUG_FOLDER}/{self.job_key}__{task_key}__{run.run_id}"
        url = helpers.upload_notebook(self.lakeflow.ws, path, source)
        log.info("Wrote a debug notebook for task %r of run %s: %s", task_key, run.run_id, url)
        state = session.current()
        state.debug_notebooks.append(url)
        if state.item is not None:
            state.item.user_properties.append(("lakeflow_debug_notebook", url))
        return DebugNotebook(task_key=task_key, run_id=run.run_id, path=path, url=url)

    def _notebook_task(self, task_key: str) -> jobs.NotebookTask:
        """The notebook task `task_key` of the job, which `%run` can reach."""
        tasks = {}
        for task in self.settings.tasks or []:
            if not task.task_key.startswith(PREFIX):
                tasks[task.task_key] = task
        if task_key not in tasks:
            raise LakeflowError(f"Job {self.job_key!r} has no task {task_key!r}. Available: {sorted(tasks)}")
        notebook = tasks[task_key].notebook_task
        if notebook is None or task_key in self.stubs:
            notebook_tasks = []
            for key, task in tasks.items():
                if task.notebook_task is not None and key not in self.stubs:
                    notebook_tasks.append(key)
            raise LakeflowError(
                f"Task {task_key!r} of {self.job_key!r} is not a notebook task, so there is no notebook to run. "
                f"Notebook tasks: {sorted(notebook_tasks)}"
            )
        # A notebook task without a source reads from Git when the job has a git_source.
        from_git = notebook.source == jobs.Source.GIT
        if notebook.source is None and self.settings.git_source is not None:
            from_git = True
        if from_git:
            raise LakeflowError(
                f"Task {task_key!r} of {self.job_key!r} runs its notebook from Git ({notebook.notebook_path}), which "
                "%run cannot reach"
            )
        return notebook

    def close(self, failed: bool = False) -> None:
        """Cancel active runs, then delete the job unless the keep rules say to keep it.

        With `--lakeflow-report-dir`, each cancelled run is polled until it has ended, for at most 60 seconds, and
        listed in the report.json of the test that is running, whose report is still to be written: for a job that a
        module- or session-scoped fixture closes, the test being torn down. Outside a test the run is not polled.
        The job is deleted even when the polling is interrupted.

        Args:
            failed: Whether the test failed, which keeps the job when `keep_failed` is on.
        """
        if self._closed:
            return
        self._closed = True
        cancelled = sorted(self._active)
        for run_id in cancelled:
            self._cancel(run_id)
        item = session.current().item
        try:
            if item is not None:
                for run_id in cancelled:
                    self._report_cancelled(_Polling(run_id, item))
            if failed and session.current().debug_notebook:
                self._write_debug_notebooks()
        finally:
            self._delete_unless_kept(failed)

    def _write_debug_notebooks(self) -> None:
        """Write a debug notebook for each notebook task that failed in the last run. Errors are only logged."""
        if not self.runs:
            return
        run = self.runs[-1]
        for key, task in run.tasks.items():
            if task.succeeded or not task.ran:
                continue
            if require_task(self.settings, key).notebook_task is None:
                continue
            # Teardown never fails because of a debugging aid.
            try:
                self.debug_notebook(key, run)
            except (LakeflowError, DatabricksError) as e:
                log.warning("Could not write a debug notebook for task %r of run %s: %s", key, run.run_id, e)

    def _delete_unless_kept(self, failed: bool) -> None:
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
        self._active.pop(run_id, None)
        self.lakeflow.active_runs.discard(run_id)


class RunHandle:
    """A run started by `EphemeralJob.start()`, to wait for or cancel later.

    Attributes:
        job: The job the run belongs to.
        run_id: The run's id.
    """

    def __init__(
        self, job: EphemeralJob, run_id: int, item: pytest.Item, first: jobs.Run, timeout: timedelta | None = None
    ) -> None:
        self.job = job
        self.run_id = run_id
        self._item = item
        self._first = first
        self._first_at = time.monotonic()
        self._timeout = timeout
        self._result: JobRun | None = None

    @property
    def url(self) -> str | None:
        """The run's page in the workspace, or `None` if the workspace has not reported it yet."""
        return self.job._run_urls.get(self.run_id)

    def wait(self, timeout: timedelta | None = None, *, stop_after: timedelta | None = None) -> JobRun:
        """Wait for the run to finish.

        Waiting again returns the same `JobRun`, and `run_finished` is called only the first time.

        Args:
            timeout: How long to wait from this call. Defaults to the `timeout` passed to `start()`, else
                `Lakeflow`'s `timeout`. It must not be longer than the job's `timeout_seconds`, which `Lakeflow`'s
                `timeout` sets. The run has already started, so pass a longer timeout to `start()` or `run()`, which
                raise `timeout_seconds` first.
            stop_after: How long the run may go on before it is cancelled, counted from `start()`, for a job that does
                not end on its own. The run is then polled until it has ended and returned as a `JobRun` whose
                `cancelled` is true, instead of raising. It must not be longer than `timeout`.

        Returns:
            The run's outcome, which is also appended to the job's `runs`.

        Raises:
            LakeflowError: If the job closed before the run finished, which cancelled the run, `timeout` is longer
                than the job's `timeout_seconds`, or `stop_after` is longer than `timeout`. In the last two cases the
                run goes on.
            LakeflowTimeout: If the run did not finish in time. The run is cancelled first.
        """
        if self._result is not None:
            return self._result
        job = self.job
        if job._closed:
            raise LakeflowError(f"The ephemeral job for {job.job_key!r} has already been closed")
        if timeout is not None:
            job._check_timeout(timeout)
        elif self._timeout is not None:
            timeout = self._timeout
        else:
            timeout = job.lakeflow.timeout
        if stop_after is not None:
            _check_stop_after(job.job_key, stop_after, timeout)
        first = self._first
        self._first = None
        run, outputs = job._wait(self.run_id, timeout, self._item, first, self._first_at, stop_after)
        job._forget(self.run_id)
        result = job._job_run(run, outputs)
        self._result = result
        job.runs.append(result)
        # Held on the test only for --lakeflow-report-dir, because a run can carry megabytes of logs and notebooks.
        if session.current().report_dir is not None:
            self._item.stash.setdefault(RUNS, []).append((job, result))
        _record_outcome(self._item, result)
        log.info("Run %s finished %s in %s: %s", self.run_id, result.result_state, result.duration, result.run_url)
        _warn_on_failed_teardown(result)
        if job.lakeflow.run_finished is not None:
            job.lakeflow.run_finished(result, self._item)
        return result

    def cancel(self) -> None:
        """Cancel the run. It ends `CANCELED` shortly after, which a later `wait()` returns."""
        self.job._cancel(self.run_id)


def cancel_run(ws: WorkspaceClient, run_id: int) -> None:
    try:
        ws.jobs.cancel_run(run_id)
        log.info("Cancelled run %s", run_id)
    except DatabricksError as e:
        log.warning("Could not cancel run %s: %s", run_id, e)


def _check_stop_after(job_key: str, stop_after: timedelta, timeout: timedelta) -> None:
    if stop_after > timeout:
        raise LakeflowError(
            f"stop_after={stop_after} of {job_key!r} is longer than its timeout of {timeout}, so the run would time "
            "out before it is stopped. Pass a longer timeout or a shorter stop_after."
        )


def _performance_target(value: jobs.PerformanceTarget | str | None) -> jobs.PerformanceTarget | None:
    if value is None:
        return None
    return jobs.PerformanceTarget(value)


def _record_outcome(item: pytest.Item, result: JobRun) -> None:
    """Record the run's outcome in the test's user properties, with its resolved parameters when it failed.

    The notebook lines of the failing tasks go to the stash instead, for the report section of a failed test.
    """
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
    item.stash.setdefault(NOTEBOOKS, []).extend(describe_notebooks(result))
    run_params = format_parameters(result.parameters)
    if run_params:
        item.user_properties.append(("lakeflow_params", run_params))
    task_params = failing_parameters(result)
    if task_params:
        shown = {key: json.loads(text) for key, text in task_params.items()}
        item.user_properties.append(("lakeflow_task_params", json.dumps(shown, sort_keys=True)))


def _warn_on_failed_teardown(result: JobRun) -> None:
    """Log a failed teardown task, which leaves `succeeded` as it was, so that it does not pass unnoticed."""
    teardown = result.teardown
    if teardown is None or teardown.succeeded:
        return
    log.warning("The teardown task of run %s failed: %s %s", result.run_id, task_outcome(teardown), teardown.run_url)


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


def _task_states(run: jobs.Run) -> str:
    """List each task run's state, such as `extract SUCCESS, transform RUNNING, load PENDING`."""
    states = []
    for task in run.tasks or []:
        state, _ = _task_state(task)
        states.append(f"{_label(task)} {state or '?'}")
    return ", ".join(states) or "none"


def _label(task: jobs.RunTask) -> str:
    """Name a task run by its key, adding the attempt number of a retry."""
    label = task.task_key or "?"
    if task.attempt_number:
        label = f"{label} (attempt {task.attempt_number})"
    return label


def _transition(task: jobs.RunTask, previous: str | None, state: str, output: jobs.RunOutput | None, done: bool) -> str:
    """Describe a task run's change of state, such as `transform: RUNNING -> FAILED in 0:00:42 ValueError: x`."""
    label = _label(task)
    line = f"{label}: {previous} -> {state}" if previous else f"{label}: {state}"
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
