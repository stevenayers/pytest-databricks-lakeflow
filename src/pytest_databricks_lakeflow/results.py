from __future__ import annotations

import json
import logging
import re
import warnings
from collections.abc import Collection, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from datetime import timedelta
from enum import Enum
from typing import TYPE_CHECKING

from databricks.sdk.errors.base import DatabricksError
from databricks.sdk.service import jobs

from pytest_databricks_lakeflow.errors import LakeflowRunFailed

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

log = logging.getLogger("pytest_databricks_lakeflow")

PREFIX = "__pytest_"
PROBE_PREFIX = f"{PREFIX}probe__"
NOT_RUN = frozenset({"UPSTREAM_FAILED", "UPSTREAM_CANCELED", "DISABLED", "EXCLUDED", "CANCELED"})

ANSI = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
EXC_LINE = re.compile(r"^(?P<name>[A-Za-z_]\w*(?:\.(?:<locals>|[A-Za-z_]\w*))*)(?:: ?(?P<msg>.*))?$")
JAVA_FRAME = re.compile(r"^\s+at \S")
CAUSED_BY = re.compile(r"^\s*Caused by:\s?(?P<cause>.*)$")
ERROR_CLASS = re.compile(r"^\[([A-Z][A-Z0-9_]*(?:\.[A-Z0-9_]+)*)\]")


def _first_line(text: str, limit: int = 200) -> str:
    lines = text.strip().splitlines()
    line = lines[0] if lines else ""
    return line if len(line) <= limit else f"{line[: limit - 3]}..."


@dataclass(frozen=True)
class TaskError:
    """An error reported by a task run.

    Attributes:
        type: The exception class name as Python printed it, possibly dotted, such as
            `json.decoder.JSONDecodeError`. `None` when the error is not an exception, for example when the
            platform skipped the task.
        message: The exception message.
        trace: The error trace with ANSI colour codes removed.
    """

    type: str | None
    message: str
    trace: str

    @classmethod
    def from_output(cls, error: str | None, trace: str | None) -> TaskError | None:
        """Parse the error fields of a task's `get_run_output` response.

        The `error` field is preferred, because `error_trace` is ANSI-coloured IPython output. The last
        `Name: message` line of the trace is the fallback.

        Args:
            error: The `error` field.
            trace: The `error_trace` field.

        Returns:
            The parsed error, or `None` when both fields are empty.
        """
        if not error and not trace:
            return None
        err = ANSI.sub("", error or "").strip("\n")
        tr = ANSI.sub("", trace or "")
        if err:
            first, *rest = err.splitlines()
            match = EXC_LINE.match(first)
            if match:
                lines = [match["msg"] or "", *(line for line in rest if not JAVA_FRAME.match(line))]
                return cls(type=match["name"], message="\n".join(lines).strip(), trace=tr or err)
        for line in reversed(tr.splitlines()):
            match = EXC_LINE.match(line)
            if match and match["msg"] is not None:
                return cls(type=match["name"], message=match["msg"].strip(), trace=tr)
        return cls(type=None, message=err or tr.strip(), trace=tr)

    @property
    def root_cause(self) -> str | None:
        """The text after the last `Caused by:` line of `trace`, or `None` when the trace has no such line."""
        for line in reversed(self.trace.splitlines()):
            match = CAUSED_BY.match(line)
            if match:
                return match["cause"].strip()
        return None

    @property
    def code(self) -> str | None:
        """The bracketed error class, such as `TABLE_OR_VIEW_NOT_FOUND`, at the start of `message` or `root_cause`.

        `None` when neither starts with one. Lowercase tags such as `[my_tag]` are not error classes.
        """
        for text in (self.message, self.root_cause):
            match = ERROR_CLASS.match(text or "")
            if match:
                return match[1]
        return None

    def __str__(self) -> str:
        return f"{self.type}: {self.message}" if self.type else self.message


@dataclass(frozen=True)
class Timing:
    """Where the time of a run or task run went, as `runs/get` reports it.

    Attributes:
        queue: Time spent waiting in the queue.
        setup: Time spent setting up compute and the environment.
        execution: Time spent running the code.
        cleanup: Time spent cleaning up afterwards.
    """

    queue: timedelta = timedelta(0)
    setup: timedelta = timedelta(0)
    execution: timedelta = timedelta(0)
    cleanup: timedelta = timedelta(0)

    @property
    def total(self) -> timedelta:
        """The sum of `queue`, `setup`, `execution` and `cleanup`.

        It differs from the `duration` of `JobRun` and `TaskRun`, which is wall clock from start to end, for example
        when a run waited in the queue.
        """
        return self.queue + self.setup + self.execution + self.cleanup

    @classmethod
    def from_run(cls, run: jobs.Run | jobs.RunTask) -> Timing:
        """Read the timing of a run or task run.

        Args:
            run: The run or task run from `get_run`. Its durations are in milliseconds.

        Returns:
            The timing, with `timedelta(0)` for every duration the run does not report.
        """
        return cls(
            queue=_millis(run.queue_duration),
            setup=_millis(run.setup_duration),
            execution=_millis(run.execution_duration),
            cleanup=_millis(run.cleanup_duration),
        )


class TaskValues(dict[str, object]):
    """Task values captured by a probe task.

    A dict whose missing keys raise a `KeyError` that says why the value is missing: the task never set it, the
    task did not run, or the probe failed.

    Attributes:
        reasons: The explanation for each missing key.
    """

    def __init__(self, values: Mapping[str, object] | None = None, reasons: Mapping[str, str] | None = None) -> None:
        super().__init__(values or {})
        self.reasons = dict(reasons or {})

    def __missing__(self, key: str) -> object:
        if key in self.reasons:
            raise KeyError(self.reasons[key])
        raise KeyError(key)


@dataclass(frozen=True)
class TaskRun:
    """The outcome of one task in a job run.

    When the platform retried the task, this describes the last attempt and `attempts` holds every attempt.

    Attributes:
        task_key: The task's key.
        run_id: The task run's id.
        result_state: The task's result state, such as `SUCCESS`, `FAILED` or `UPSTREAM_FAILED`.
        termination_code: The termination code, such as `RUN_EXECUTION_ERROR`.
        termination_message: The termination message.
        run_url: The task run's page in the workspace.
        duration: The time from start to end.
        output: The value the notebook passed to `dbutils.notebook.exit()`.
        output_truncated: Whether `output` was cut at 5 MB.
        logs: stdout and stderr of a Python script, wheel or JAR task. Empty for notebook tasks.
        logs_truncated: Whether `logs` holds only the last 5 MB.
        error: The task's error, or `None` if it succeeded or did not run.
        task_values: Task values captured by a probe.
        missing_task_values: Keys that were requested from the probe but not captured.
        attempt: The attempt number, 0 for the first attempt.
        parameters: The task's parameters after job parameters and `{{...}}` references were resolved. A dict for
            notebook (`base_parameters`), SQL, run-job (`job_parameters`) and Python wheel tasks with named
            parameters; a list for Python wheel tasks with positional parameters, Python script, JAR and
            spark-submit tasks, and dbt tasks (`commands`); `None` for other task types or when the platform
            reported no resolved values.
        attempts: One `TaskRun` per attempt, in attempt order, each with empty `attempts`. The last equals this
            task apart from `attempts`.
        disabled: Whether the task was disabled, with `disable=` on `Lakeflow.job()` or in the bundle, read from the
            job's settings because the run does not say. Tasks left out by `only` are not flagged.
        timing: The queue, setup, execution and cleanup durations of the task run. Zero where the platform reported
            none.
        effective_performance_target: The serverless performance mode the task ran with, `STANDARD` or
            `PERFORMANCE_OPTIMIZED`, or `None` when the platform did not say.
    """

    task_key: str
    run_id: int
    result_state: str | None
    termination_code: str | None
    termination_message: str | None
    run_url: str | None
    duration: timedelta
    output: str | None = None
    output_truncated: bool = False
    logs: str = ""
    logs_truncated: bool = False
    error: TaskError | None = None
    task_values: TaskValues = field(default_factory=TaskValues)
    missing_task_values: list[str] = field(default_factory=list)
    attempt: int = 0
    parameters: dict[str, str] | list[str] | None = None
    attempts: list[TaskRun] = field(default_factory=list)
    disabled: bool = False
    timing: Timing = field(default_factory=Timing)
    effective_performance_target: str | None = None

    @property
    def succeeded(self) -> bool:
        """Whether the task ended `SUCCESS`."""
        return self.result_state == "SUCCESS"

    @property
    def timed_out(self) -> bool:
        """Whether the task ended `TIMEDOUT`, stopped by the platform at its `timeout_seconds`."""
        return self.result_state == "TIMEDOUT"

    @property
    def ran(self) -> bool:
        """Whether the task ran. False if it was skipped, excluded, disabled or cancelled."""
        return self.result_state not in NOT_RUN

    @property
    def excluded(self) -> bool:
        """Whether the task ended `EXCLUDED` because its `run_if` or if/else condition was not met."""
        return self.result_state == "EXCLUDED"

    @property
    def output_json(self) -> object:
        """`output` parsed as JSON.

        Raises:
            ValueError: If the notebook did not call `dbutils.notebook.exit()`, if `output` was cut at 5 MB, or if
                `output` is not JSON. The message names the task.
        """
        name = self.task_key
        if self.output is None:
            raise ValueError(f"Task {name!r} has no output: dbutils.notebook.exit() was not called")
        if self.output_truncated:
            raise ValueError(f"Output of task {name!r} was cut at 5 MB, so it cannot be parsed as JSON")
        try:
            return json.loads(self.output)
        except ValueError as e:
            raise ValueError(f"Output of task {name!r} is not JSON: {self.output[:200]!r}") from e

    @property
    def log_lines(self) -> list[str]:
        """The non-blank lines of `logs`."""
        return [line for line in self.logs.splitlines() if line.strip()]

    def raised(self, exc_type: type[BaseException]) -> bool:
        """Check whether the task failed with an exception of the given class.

        Args:
            exc_type: The exception class. Only its name is compared, because the task ran in another process.

        Returns:
            True if the task's error has the class name of `exc_type`.
        """
        if self.error is None or self.error.type is None:
            return False
        return self.error.type.rsplit(".", 1)[-1] == exc_type.__name__

    def __repr__(self) -> str:
        error = f", error={_first_line(str(self.error))!r}" if self.error else ""
        params = f", params={self.parameters!r}" if self.error and self.parameters is not None else ""
        return f"TaskRun({self.task_key!r}, {self.result_state}{error}{params}, {self.run_url})"


@dataclass(frozen=True)
class JobRun:
    """The outcome of one run of an ephemeral job.

    Attributes:
        job_key: The bundle resource key of the job.
        job_id: The ephemeral job's id.
        run_id: The run's id.
        run_url: The run's page in the workspace.
        result_state: The parent run's result state. Not a verdict: a run whose probe ran after a failed task
            ends `SUCCESS_WITH_FAILURES`. Use `succeeded` instead.
        termination_code: The parent run's termination code.
        termination_message: The parent run's termination message.
        duration: The time from start to end of the parent run.
        tasks: The user's tasks that were part of the run, by task key.
        injected_tasks: The stub and probe tasks added by the plugin, by task key.
        skipped_tasks: Tasks left out by `only` or disabled, by task key. A disabled task has `disabled` set.
        parameters: The run's job parameters by name: the value passed for the run, or the default when none was.
            Parameters the plugin injects are left out.
        timing: The queue, setup, execution and cleanup durations of the parent run. Zero where the platform
            reported none; a run with several tasks reports its time on the tasks instead.
        effective_performance_target: The serverless performance mode the run used, `STANDARD` or
            `PERFORMANCE_OPTIMIZED`, or `None` when the platform did not say.
    """

    job_key: str
    job_id: int
    run_id: int
    run_url: str
    result_state: str | None
    termination_code: str | None
    termination_message: str | None
    duration: timedelta
    tasks: dict[str, TaskRun]
    injected_tasks: dict[str, TaskRun]
    skipped_tasks: dict[str, TaskRun] = field(default_factory=dict)
    parameters: dict[str, str] = field(default_factory=dict)
    timing: Timing = field(default_factory=Timing)
    effective_performance_target: str | None = None

    @property
    def succeeded(self) -> bool:
        """Whether every task in `tasks` ended `SUCCESS`.

        Injected and skipped tasks do not count, nor do tasks `EXCLUDED` because their `run_if` or if/else
        condition was not met.
        """
        return bool(self.tasks) and not self._failing()

    @property
    def failed(self) -> bool:
        """Whether the run did not succeed."""
        return not self.succeeded

    @property
    def timed_out(self) -> bool:
        """Whether the parent run ended `TIMEDOUT` or any task in `tasks` timed out."""
        return self.result_state == "TIMEDOUT" or any(t.timed_out for t in self.tasks.values())

    def task(self, key: str) -> TaskRun:
        """Get one of the user's tasks.

        Args:
            key: The task key.

        Returns:
            The task's outcome.

        Raises:
            KeyError: If the task was not part of the run. The message lists the available keys and says whether
                the task was disabled or skipped by `only`.
        """
        if key not in self.tasks:
            skipped = ""
            if key in self.skipped_tasks:
                if self.skipped_tasks[key].disabled:
                    skipped = f" Task {key!r} was disabled (in the job or with disable=)."
                else:
                    skipped = f" Skipped by only: {sorted(k for k, t in self.skipped_tasks.items() if not t.disabled)}."
            raise KeyError(f"No task {key!r} in run. Available: {sorted(self.tasks)}.{skipped}")
        return self.tasks[key]

    def raise_for_failure(self) -> JobRun:
        """Raise if the run did not succeed.

        Returns:
            The run itself, so `run = job.run().raise_for_failure()` works.

        Raises:
            LakeflowRunFailed: If `succeeded` is false. The message is `repr(run)` followed by one line per failing
                task with its error, the error's root cause when there is one, and the task's run page.
        """
        if self.succeeded:
            return self
        lines = [repr(self)]
        for t in self._failing():
            cause = f" (root cause: {t.error.root_cause})" if t.error and t.error.root_cause else ""
            lines.append(f"{t.task_key}: {t.error if t.error else t.result_state}{cause} {t.run_url}")
        raise LakeflowRunFailed("\n".join(lines), self)

    def _failing(self) -> list[TaskRun]:
        return [t for t in self.tasks.values() if not t.succeeded and t.result_state != "EXCLUDED"]

    def __repr__(self) -> str:
        failing = describe_failing(self)
        params = f"params={self.parameters!r}, " if failing and self.parameters else ""
        return f"JobRun({self.job_key!r}, {self.result_state}, {self.run_url}, {params}failing={failing})"


def describe_failing(run: JobRun) -> list[str]:
    """Describe each failing task of `run` as `key: error`, with the error's first line or the result state."""
    return [f"{t.task_key}: {_first_line(str(t.error)) if t.error else t.result_state}" for t in run._failing()]


def build_job_run(
    ws: WorkspaceClient,
    run: jobs.Run,
    *,
    job_key: str,
    job_id: int,
    stubs: Collection[str] = (),
    capture_task_values: Mapping[str, Sequence[str]] | None = None,
    disabled: Collection[str] = (),
    outputs: Mapping[int, jobs.RunOutput] | None = None,
) -> JobRun:
    """Build a `JobRun` from a terminated run, reading each task's output.

    When the platform retried a task, the `TaskRun` describes its last attempt and its `attempts` hold every
    attempt. Probe outputs become the `values` of the tasks they watched.

    Args:
        ws: The workspace client.
        run: The terminated parent run from `get_run`.
        job_key: The bundle resource key of the job.
        job_id: The ephemeral job's id.
        stubs: Keys of stub tasks, which count as injected tasks.
        capture_task_values: The value keys requested from each probed task, by task key.
        disabled: Keys of the tasks disabled in the job's settings. The Jobs API does not flag them on the run, where
            a disabled task and one left out by `only` both end `DISABLED`.
        outputs: Task outputs already read, by task run id. Only the outputs missing from it are fetched.

    Returns:
        The run's outcome.
    """
    by_key: dict[str, list[jobs.RunTask]] = {}
    for task in run.tasks or []:
        by_key.setdefault(task.task_key or "", []).append(task)
    for tasks in by_key.values():
        tasks.sort(key=lambda t: t.attempt_number or 0)
    ran = [task for tasks in by_key.values() for task in tasks if has_output(task)]
    known = dict(outputs or {})
    known.update(fetch_outputs(ws, [task for task in ran if (task.run_id or 0) not in known]))
    attempts = {key: [_task_run(task, known.get(task.run_id or 0)) for task in tasks] for key, tasks in by_key.items()}
    task_runs = {
        key: replace(runs[-1], disabled=key in disabled and runs[-1].result_state == "DISABLED")
        for key, runs in attempts.items()
    }
    injected = {key: t for key, t in task_runs.items() if key.startswith(PREFIX) or key in stubs}
    user = {key: t for key, t in task_runs.items() if key not in injected}
    for target, keys in (capture_task_values or {}).items():
        if target in user:
            user[target] = _with_values(user[target], injected.get(f"{PROBE_PREFIX}{target}"), keys)
    injected = {key: _with_attempts(t, attempts[key]) for key, t in injected.items()}
    user = {key: _with_attempts(t, attempts[key]) for key, t in user.items()}
    details = run.status.termination_details if run.status else None
    return JobRun(
        job_key=job_key,
        job_id=job_id,
        run_id=run.run_id or 0,
        run_url=run.run_page_url or "",
        result_state=_enum(run.state.result_state if run.state else None),
        termination_code=_enum(details.code if details else None),
        termination_message=details.message if details else None,
        duration=duration(run.start_time, run.end_time),
        tasks={key: t for key, t in user.items() if t.result_state != "DISABLED"},
        injected_tasks=injected,
        skipped_tasks={key: t for key, t in user.items() if t.result_state == "DISABLED"},
        parameters=_job_parameters(run.job_parameters),
        timing=Timing.from_run(run),
        effective_performance_target=_enum(run.effective_performance_target),
    )


def _with_attempts(last: TaskRun, attempts: list[TaskRun]) -> TaskRun:
    return replace(last, attempts=[*attempts[:-1], last])


def _job_parameters(parameters: Sequence[jobs.JobParameter] | None) -> dict[str, str]:
    return {
        p.name: p.value if p.value is not None else p.default or ""
        for p in parameters or []
        if p.name and not p.name.startswith(PREFIX)
    }


def has_output(task: jobs.RunTask) -> bool:
    """Whether a terminated task run has an output to read: every task run but a disabled one has."""
    return _enum(_result_state(task)) != "DISABLED"


def fetch_outputs(ws: WorkspaceClient, tasks: Sequence[jobs.RunTask]) -> dict[int, jobs.RunOutput]:
    """Read the output of each task run in parallel, by task run id. An output that cannot be read is empty."""
    if not tasks:
        return {}

    def fetch(task: jobs.RunTask) -> jobs.RunOutput:
        try:
            return ws.jobs.get_run_output(task.run_id or 0)
        except DatabricksError as e:
            log.warning("Could not read the output of task %s (run %s): %s", task.task_key, task.run_id, e)
            return jobs.RunOutput()

    with ThreadPoolExecutor(max_workers=8) as pool:
        return {task.run_id or 0: output for task, output in zip(tasks, pool.map(fetch, tasks), strict=True)}


def error_line(task: jobs.RunTask, output: jobs.RunOutput | None) -> str | None:
    """The first line of the error a terminated task run reported, or `None` when it reported none or did not run."""
    if output is None or _enum(_result_state(task)) in NOT_RUN:
        return None
    error = TaskError.from_output(output.error, output.error_trace)
    return _first_line(str(error)) if error else None


def _task_run(task: jobs.RunTask, out: jobs.RunOutput | None) -> TaskRun:
    out = out or jobs.RunOutput()
    state = _enum(_result_state(task))
    details = task.status.termination_details if task.status else None
    notebook = out.notebook_output
    key = task.task_key or ""
    truncated = [
        what for what, cut in (("output", notebook and notebook.truncated), ("logs", out.logs_truncated)) if cut
    ]
    if truncated:
        verb = "were" if len(truncated) > 1 else "was"
        warnings.warn(f"The {' and '.join(truncated)} of task {key!r} {verb} cut at the 5 MB limit", stacklevel=2)
    return TaskRun(
        task_key=key,
        run_id=task.run_id or 0,
        result_state=state,
        termination_code=_enum(details.code if details else None),
        termination_message=details.message if details else None,
        run_url=task.run_page_url,
        duration=duration(task.start_time, task.end_time),
        output=notebook.result if notebook else None,
        output_truncated=bool(notebook and notebook.truncated),
        logs=out.logs or "",
        logs_truncated=bool(out.logs_truncated),
        error=None if state in NOT_RUN else TaskError.from_output(out.error, out.error_trace),
        attempt=task.attempt_number or 0,
        parameters=_parameters(task.resolved_values),
        timing=Timing.from_run(task),
        effective_performance_target=_enum(task.effective_performance_target),
    )


def _parameters(resolved: jobs.ResolvedValues | None) -> dict[str, str] | list[str] | None:
    if resolved is None:
        return None
    if resolved.notebook_task is not None:
        return resolved.notebook_task.base_parameters
    if resolved.sql_task is not None:
        return resolved.sql_task.parameters
    if resolved.python_wheel_task is not None:
        wheel = resolved.python_wheel_task
        return wheel.named_parameters if wheel.named_parameters is not None else wheel.parameters
    if resolved.run_job_task is not None:
        return resolved.run_job_task.job_parameters
    for strings in (resolved.spark_jar_task, resolved.spark_python_task, resolved.spark_submit_task):
        if strings is not None:
            return strings.parameters
    if resolved.dbt_task is not None:
        return resolved.dbt_task.commands
    return None


def _with_values(target: TaskRun, probe: TaskRun | None, keys: Sequence[str]) -> TaskRun:
    name = target.task_key
    if not target.ran:
        why = {k: f"Task {name!r} did not run ({target.result_state}), so value {k!r} was not captured" for k in keys}
        return replace(target, task_values=TaskValues({}, why), missing_task_values=list(keys))
    captured = _probe_output(probe)
    if isinstance(captured, str):
        why = {k: f"Value {k!r} of task {name!r} was not captured: {captured}" for k in keys}
        return replace(target, task_values=TaskValues({}, why), missing_task_values=list(keys))
    values, missing = captured
    why = {k: f"Task {name!r} never set value {k!r}" for k in missing}
    return replace(target, task_values=TaskValues(values, why), missing_task_values=missing)


def _probe_output(probe: TaskRun | None) -> tuple[dict[str, object], list[str]] | str:
    if probe is None:
        return "its probe task was not part of the run"
    if not probe.succeeded:
        error = f": {probe.error}" if probe.error else ""
        return f"probe task {probe.task_key} ended {probe.result_state}{error}"
    try:
        data = json.loads(probe.output or "")
        return dict(data["values"]), list(data["missing"])
    except (ValueError, KeyError, TypeError):
        truncated = " (truncated)" if probe.output_truncated else ""
        return f"probe task {probe.task_key} returned unreadable output{truncated}"


def _result_state(task: jobs.RunTask) -> jobs.RunResultState | None:
    return task.state.result_state if task.state else None


def _enum(value: Enum | str | None) -> str | None:
    return value.value if isinstance(value, Enum) else value


def duration(start: int | None, end: int | None) -> timedelta:
    return timedelta(milliseconds=end - start) if start and end and end >= start else timedelta(0)


def _millis(value: int | None) -> timedelta:
    return timedelta(milliseconds=value) if value else timedelta(0)
