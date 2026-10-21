"""The outcome of a run and of each of its tasks, as tests read it."""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

from databricks.sdk.service import jobs, pipelines

from pytest_databricks_lakeflow.errors import LakeflowError, LakeflowRunFailed
from pytest_databricks_lakeflow.naming import PREFIX
from pytest_databricks_lakeflow.results.serialise import serialise_with_files

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

    from pytest_databricks_lakeflow.dbt import DbtArtifacts


NOT_RUN = frozenset({"UPSTREAM_FAILED", "UPSTREAM_CANCELED", "DISABLED", "EXCLUDED", "CANCELED"})
ANSI = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
EXC_LINE = re.compile(r"^(?P<name>[A-Za-z_]\w*(?:\.(?:<locals>|[A-Za-z_]\w*))*)(?:: ?(?P<msg>.*))?$")
JAVA_FRAME = re.compile(r"^\s+at \S")
CAUSED_BY = re.compile(r"^\s*Caused by:\s?(?P<cause>.*)$")
CHAIN_MARKERS = re.compile(
    r"^(?:The above exception was the direct cause of the following exception:"
    r"|During handling of the above exception, another exception occurred:)[ \t\r]*$",
    re.MULTILINE,
)
ERROR_CLASS = re.compile(r"^\[([A-Z][A-Z0-9_]*(?:\.[A-Z0-9_]+)*)\]")
# An update's "started an update" event is logged a few milliseconds before the update's creation time (7 ms on the
# workspace), so the events of an update are read from a margin before it.
EVENT_LEAD_MS = 60_000


def first_line(text: str, limit: int = 200) -> str:
    lines = text.strip().splitlines()
    line = lines[0] if lines else ""
    return line if len(line) <= limit else f"{line[: limit - 3]}..."


def last_exception_line(lines: Sequence[str]) -> tuple[str, str] | None:
    # The last `Name: message` line of a trace names the exception that ended it.
    for line in reversed(lines):
        match = EXC_LINE.match(line)
        if match and match["msg"] is not None:
            return match["name"], match["msg"].strip()
    return None


def join_lines(parts: Iterable[str]) -> str:
    joined = ""
    for part in parts:
        if part and not part.endswith("\n"):
            part += "\n"
        joined += part
    return joined


def format_parameters(parameters: Mapping[str, str] | Sequence[str] | None) -> str:
    """Format resolved parameters as one line of JSON with sorted keys, or `""` when there are none.

    Each value is cut to its first line of at most 200 characters. Keys the plugin injects are left out.
    """
    shown: dict[str, str] | list[str]
    if isinstance(parameters, Mapping):
        shown = {}
        for key, value in parameters.items():
            if not key.startswith(PREFIX):
                shown[key] = first_line(str(value))
    else:
        shown = [first_line(str(value)) for value in parameters or []]
    if not shown:
        return ""
    return json.dumps(shown, sort_keys=True)


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
        error_text = ANSI.sub("", error or "").strip("\n")
        trace_text = ANSI.sub("", trace or "")
        if error_text:
            first, *rest = error_text.splitlines()
            match = EXC_LINE.match(first)
            if match:
                message_lines = [match["msg"] or ""]
                message_lines += [line for line in rest if not JAVA_FRAME.match(line)]
                return cls(type=match["name"], message="\n".join(message_lines).strip(), trace=trace_text or error_text)
        last = last_exception_line(trace_text.splitlines())
        if last:
            name, message = last
            return cls(type=name, message=message, trace=trace_text)
        return cls(type=None, message=error_text or trace_text.strip(), trace=trace_text)

    @property
    def root_cause(self) -> str | None:
        """The first thing that went wrong, read from `trace`.

        A Python chained traceback, from `raise ... from e` or an exception raised while handling another, prints
        the innermost exception first, so only the part of `trace` before the first chain marker is read. In that
        part, the text after the last `Caused by:` line wins, as in a Py4J or Spark trace. Otherwise, when the
        trace is chained, it is the innermost exception as `Name: message`, such as
        `ZeroDivisionError: division by zero`, while `type` and `message` still describe the outer exception.
        `None` when the trace has neither a `Caused by:` line nor a chain.
        """
        innermost, *outer = CHAIN_MARKERS.split(self.trace, maxsplit=1)
        lines = innermost.splitlines()
        for line in reversed(lines):
            match = CAUSED_BY.match(line)
            if match:
                return match["cause"].strip()
        if not outer:
            return None
        last = last_exception_line(lines)
        if last is None:
            return None
        name, message = last
        if message:
            return f"{name}: {message}"
        return name

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
        if self.type:
            return f"{self.type}: {self.message}"
        return self.message


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
class NotebookCell:
    """One cell of a notebook exported from a task run.

    Attributes:
        index: The cell's position in `NotebookView.cells`, counting from 0.
        code: The cell's source.
        output: The cell's text output, such as what it printed, one result per line, with HTML tags and ANSI colour
            codes removed and HTML entities unescaped. Empty when the cell had no output.
        output_html: The cell's results as exported, unchanged: HTML for an HTML result, otherwise the text the
            notebook wrote.
        error: The exception the cell raised, as `Name: message`, or `None` when it raised nothing. A cell skipped
            because an earlier cell failed has no error.
        stdout: What the cell wrote to standard output, such as what it printed, with ANSI colour codes removed.
            Unlike `output`, it leaves out standard error, displayed results and the `dbutils.notebook.exit()`
            value.
    """

    index: int
    code: str
    output: str = ""
    output_html: str = field(default="", repr=False)
    error: str | None = None
    stdout: str = field(default="", repr=False)


@dataclass(frozen=True)
class NotebookView:
    """A notebook task run as `jobs.export_run` renders it, with every cell's code and output.

    The output comes from the rendered notebook, so it is complete only once the task has terminated.

    Attributes:
        html: The exported HTML, unchanged.
        cells: The notebook's cells in order. Empty when the export could not be read.
    """

    html: str
    cells: list[NotebookCell] = field(default_factory=list)

    @property
    def stdout(self) -> str:
        """What every cell wrote to standard output, in order, which is what `print()` produced."""
        return join_lines(cell.stdout for cell in self.cells)

    @property
    def failed_cell(self) -> NotebookCell | None:
        """The first cell that raised an exception, or `None`."""
        for cell in self.cells:
            if cell.error is not None:
                return cell
        return None

    def save(self, path: str | Path) -> Path:
        """Write the exported HTML to a file, which opens in a browser like the run page.

        Args:
            path: The file to write. Missing parent directories are created.

        Returns:
            The path written.
        """
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(self.html, encoding="utf-8")
        return target

    def __repr__(self) -> str:
        failed = self.failed_cell
        index = failed.index if failed else None
        return f"NotebookView(cells={len(self.cells)}, failed_cell={index})"


@dataclass(frozen=True)
class DebugNotebook:
    """A notebook in the workspace that reproduces a notebook task of a run, written by `EphemeralJob.debug_notebook()`.

    It sets the task's resolved parameters as widgets and runs the task's notebook with `%run`.

    Attributes:
        task_key: The task's key.
        run_id: The id of the job run whose parameters it replays.
        path: The notebook's workspace path.
        url: The notebook's page in the workspace.
    """

    task_key: str
    run_id: int
    path: str
    url: str

    def __str__(self) -> str:
        return self.url


@dataclass(frozen=True)
class SqlResult:
    """The rows a SQL file task returned, as `runs/get-output` reports them under `sql_output.file_output`.

    The Jobs API reference does not document `file_output` and databricks-sdk does not model it, so the plugin reads
    the raw response. The platform keeps at most 1,000 rows, for 7 days.

    Attributes:
        task_key: The task's key.
        columns: The column names in order, such as `["n"]`.
        types: The platform's type name of each column, such as `["int"]`.
        rows: One dict per row, by column name. Values are strings as the platform reports them, such as `"1"`,
            `"1.5"`, `"true"` or `"2026-01-01"`, and a NULL is `None`. When two columns share a name, the later
            one wins.
        truncated: Whether the platform cut the rows, which it does after 1,000.
        warehouse_id: The SQL warehouse the task ran on.
        query_text: The SQL file as the task ran it. Named parameter markers such as `:n` are kept; the values
            bound to them are in `TaskRun.parameters`.
    """

    task_key: str
    columns: list[str]
    types: list[str]
    rows: list[dict[str, str | None]]
    truncated: bool = False
    warehouse_id: str | None = None
    query_text: str | None = None

    @property
    def first_row(self) -> dict[str, str | None]:
        """The first of `rows`.

        Raises:
            LakeflowError: If the query returned no rows.
        """
        if not self.rows:
            raise LakeflowError(f"Task {self.task_key!r} returned no rows")
        return self.rows[0]

    @classmethod
    def from_output(cls, task_key: str, output: Mapping[str, Any]) -> SqlResult | None:
        """Read the query result of a SQL file task from its raw `runs/get-output` response.

        Args:
            task_key: The task's key.
            output: The response as JSON.

        Returns:
            The result, or `None` when the response has no `sql_output.file_output.query_result`, as for a saved
            query, alert or dashboard task.
        """
        file_output = _mapping(_mapping(output.get("sql_output")).get("file_output"))
        query_result = file_output.get("query_result")
        if not isinstance(query_result, Mapping):
            return None
        columns: list[str] = []
        types: list[str] = []
        for column in _list(query_result.get("columns")):
            columns.append(str(_mapping(column).get("name") or ""))
            types.append(str(_mapping(column).get("type") or ""))
        rows: list[dict[str, str | None]] = []
        for row in _list(query_result.get("rows")):
            values = _list(_mapping(row).get("values"))
            rows.append(_sql_row(columns, values))
        return cls(
            task_key=task_key,
            columns=columns,
            types=types,
            rows=rows,
            truncated=bool(query_result.get("truncated")),
            warehouse_id=file_output.get("warehouse_id"),
            query_text=file_output.get("query_text"),
        )


def _sql_row(columns: Sequence[str], values: Sequence[object]) -> dict[str, str | None]:
    # A value is `{"string_value": "1"}`; the platform reports a NULL as `{}`.
    row: dict[str, str | None] = {}
    for name, value in zip(columns, values, strict=False):
        string_value = _mapping(value).get("string_value")
        if isinstance(string_value, str):
            row[name] = string_value
        else:
            row[name] = None
    return row


def _mapping(value: object) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}


def _list(value: object) -> list[Any]:
    if isinstance(value, list):
        return value
    return []


@dataclass(frozen=True)
class PipelineUpdate:
    """An update of a pipeline: the one a pipeline task started, or one that `EphemeralPipeline.run()` started.

    The Jobs API does not say which update a pipeline task started, so the plugin lists the pipeline's updates once
    the run finished and takes the one created while the task ran, preferring one whose `cause` is `JOB_TASK`. The
    pipeline is the one the task points at: the deployed one, or a copy with `pipelines="copy"`.

    Attributes:
        pipeline_id: The pipeline's id.
        update_id: The update's id.
        state: The update's state, such as `COMPLETED`, `FAILED` or `CANCELED`.
        cause: What started the update: `JOB_TASK` for an update that a pipeline task started, `API_CALL` for one
            that `EphemeralPipeline.run()` started.
        full_refresh: Whether the update was a full refresh of every table.
    """

    pipeline_id: str
    update_id: str
    state: str | None
    cause: str | None
    full_refresh: bool
    _ws: WorkspaceClient = field(repr=False, compare=False)
    _since: int = field(repr=False, compare=False)

    @classmethod
    def from_info(cls, ws: WorkspaceClient, info: pipelines.UpdateInfo, *, since: int | None = None) -> PipelineUpdate:
        """Build the update from what `pipelines.get_update` or `pipelines.list_updates` reports.

        Args:
            ws: The workspace client, which `events()` reads the events with.
            info: The update as the Pipelines API reports it.
            since: The time in epoch milliseconds before which the update logged no event, so `events()` stops
                paging there. Defaults to a minute before the update was created.

        Returns:
            The update.
        """
        if since is None:
            since = (info.creation_time or 0) - EVENT_LEAD_MS
        return cls(
            pipeline_id=info.pipeline_id or "",
            update_id=info.update_id or "",
            state=enum_value(info.state),
            cause=enum_value(info.cause),
            full_refresh=bool(info.full_refresh),
            _ws=ws,
            _since=since,
        )

    def events(self, level: str | None = None) -> list[pipelines.PipelineEvent]:
        """Read the events the update logged, newest first, with `pipelines.list_pipeline_events`.

        The platform lists the events of every update of the pipeline, newest first, so the events of other updates
        are dropped and the listing stops at the first event logged before the task started.

        Args:
            level: Only events of this level, such as `ERROR`, `WARN` or `INFO`. Every level when `None`.

        Returns:
            The events, each a `databricks.sdk.service.pipelines.PipelineEvent` with `level`, `event_type`,
            `message` and, for an error, `error`.
        """
        query = None
        if level is not None:
            # The platform refuses `level="ERROR"` as an invalid filter expression; the value needs single quotes.
            query = f"level='{level}'"
        found: list[pipelines.PipelineEvent] = []
        for event in self._ws.pipelines.list_pipeline_events(self.pipeline_id, filter=query):
            logged = _event_millis(event.timestamp)
            if logged is not None and logged < self._since:
                break
            if event.origin is not None and event.origin.update_id == self.update_id:
                found.append(event)
        return found


def _event_millis(timestamp: str | None) -> int | None:
    # A pipeline event's timestamp is ISO 8601 text such as `2026-10-19T19:00:28.187Z`.
    if not timestamp:
        return None
    try:
        when = datetime.fromisoformat(timestamp)
    except ValueError:
        return None
    return int(when.timestamp() * 1000)


@dataclass(frozen=True)
class ForEachError:
    """One error that iterations of a for-each task failed with, as the platform counts it.

    Attributes:
        message: The error message. The platform reports the same generic message, such as `Workload failed, see
            run output for details`, for every iteration whose code raised; each iteration's own error is in
            `TaskRun.iterations`.
        count: How many iterations failed with it.
        termination_category: The platform's category of the error, such as `RunExecutionError`, or `None`.
    """

    message: str
    count: int
    termination_category: str | None = None


@dataclass(frozen=True)
class ForEachStats:
    """The iteration counts of a for-each task, as the platform reports them on the task run.

    Each iteration counts once, however often it was retried.

    Attributes:
        total_iterations: How many iterations the task had, one per input.
        succeeded_iterations: How many of them succeeded.
        failed_iterations: How many of them failed.
        errors: The errors they failed with, each with the number of iterations that failed with it.
    """

    total_iterations: int
    succeeded_iterations: int
    failed_iterations: int
    errors: list[ForEachError] = field(default_factory=list)

    @classmethod
    def from_stats(cls, stats: jobs.ForEachStats) -> ForEachStats:
        """Read the counts of a `RunForEachTask.stats`.

        Args:
            stats: The stats from `get_run`.

        Returns:
            The counts, with 0 for each one the platform does not report.
        """
        counts = stats.task_run_stats or jobs.ForEachTaskTaskRunStats()
        errors: list[ForEachError] = []
        for error in stats.error_message_stats or []:
            errors.append(
                ForEachError(
                    message=error.error_message or "",
                    count=error.count or 0,
                    termination_category=error.termination_category,
                )
            )
        return cls(
            total_iterations=counts.total_iterations or 0,
            succeeded_iterations=counts.succeeded_iterations or 0,
            failed_iterations=counts.failed_iterations or 0,
            errors=errors,
        )


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
        output: The value the notebook passed to `dbutils.notebook.exit()`. For a clean room notebook task, the
            value from `clean_rooms_notebook_output` when the top-level notebook output is missing or has no result.
        output_truncated: Whether `output` was cut at 5 MB.
        logs: stdout and stderr of a Python script, wheel or JAR task. Empty for notebook tasks, whose printed text
            is in `notebook`.
        logs_truncated: Whether `logs` holds only the last 5 MB.
        error: The task's error, or `None` if it succeeded or did not run. For a failed alert task, which the Jobs
            API reports no error for, the task's termination message with `type` `None`.
        task_values: Task values captured by a probe.
        missing_task_values: Keys that were requested from the probe but not captured.
        attempt: The attempt number, 0 for the first attempt.
        parameters: The task's parameters after job parameters and `{{...}}` references were resolved. A dict for
            notebook (`base_parameters`), SQL, run-job (`job_parameters`) and Python wheel tasks with named
            parameters; a list for Python wheel tasks with positional parameters, Python script, JAR and
            spark-submit tasks, and dbt tasks (`commands`); `None` for other task types, for a task that did not
            run, and when the task run could not be read.
        attempts: One `TaskRun` per attempt, in attempt order, each with empty `attempts`. The last equals this
            task apart from `attempts`.
        timing: The queue, setup, execution and cleanup durations of the task run. Zero where the platform reported
            none.
        effective_performance_target: The serverless performance mode the task ran with, `STANDARD` or
            `PERFORMANCE_OPTIMIZED`, or `None` when the platform did not say.
        condition_outcome: The outcome of an if/else condition task: `True` when the condition held, `False` when it
            did not, `None` for any other task or when the platform reported no outcome.
        sql_output: The output of a SQL task as the platform reported it: `query_output` (with `warehouse_id`,
            `output_link`, `query_text` and `sql_statements`), `alert_output` or `dashboard_output`. Holds no query
            rows: a SQL file task's rows are in `sql_result`. `None` for any other task or when the platform reported
            none.
        alert_output: The output of an alert task, with its `alert_state`. `None` for any other task or when the
            platform reported none.
        dbt_output: The output of a dbt task, with `artifacts_link` and `artifacts_headers`, or `None`. The link is a
            pre-signed URL to a tarball of the dbt artefacts that the platform signs for 30 minutes; fetch it with
            the headers before then. report.json keeps the link and leaves out the headers.
        dbt: The artefacts of a dbt task, downloaded from `dbt_output.artifacts_link` and parsed once the run finished,
            with `Lakeflow(dbt_artifacts=True)`. `None` by default, for any other task, when the platform reported no
            link, and when the artefacts could not be read, such as a tarball over 50 MB, which logs a warning.
        run_job_output: The output of a run-job task, with the triggered run's `run_id`, or `None`.
        child_run_id: The id of the job run that a run-job task triggered, from `run_job_output.run_id`. The run is
            of the deployed child job, not a copy, so the plugin does not cancel or delete it. `None` for any other
            task or when the platform reported no run id.
        child_run: The run that a run-job task triggered, as a `JobRun` with the child's own tasks, read with
            `get_run` once the parent run finished. Its `job_key` is the child job's bundle resource key, or its job
            id as a string when the job is not in the bundle. A child with its own run-job tasks has nested
            `child_run`s, and a task disabled in the child job is in its `disabled_tasks`. Each of `attempts` carries
            the child run that attempt triggered. `None` for any other task, when the task did not run, when the
            platform reported no run id, or when the child run could not be read, which logs a warning.
        dashboard_output: The output of a dashboard task, with its `page_snapshots`, or `None`.
        clean_rooms_notebook_output: The output of a clean room notebook task, with `notebook_output` and
            `clean_room_job_run_state`, or `None`.
        notebook: The rendered notebook of a notebook task, with each cell's code, output and error, read with
            `jobs.export_run` once the run finished. Exported for the tasks that `Lakeflow(notebook_export=...)`
            selects: by default, a notebook task that ran and did not succeed. `None` for any other task, and when
            `export_run` failed, which logs a warning.
        sql_result: The columns and rows a SQL file task returned, read from `runs/get-output` once the run
            finished. `None` for any other task, including a saved query task, for a task that did not run, and when
            the output could not be read, which logs a warning.
        pipeline_update: The pipeline update that a pipeline task started, found once the run finished among the
            pipeline's updates by its creation time, which lies between the task's start and end. `None` for any
            other task, for a task that did not run, and when no update was created in that time or the updates
            could not be read, which logs a warning.
        iterations: The iterations of a for-each task, one `TaskRun` per input in iteration order, read once the run
            finished from the for-each task run fetched by its own id. Each is built like a task, with one
            `get_run_output` and one `get_run` for every attempt of it: `task_key` is the nested task's key, `output`,
            `error` and the rest are its own, `parameters` are the nested task's resolved parameters, where the
            iteration's input appears wherever the nested task passes `{{input}}`, and `attempts` holds every attempt
            when the platform retried it. Iterations have no `task_values`, because the platform refuses
            `taskValues.set` in an iteration, and no `notebook`. Empty for any other task, for a for-each task that
            did not start, such as one that ended `UPSTREAM_FAILED`, and when the iterations could not be read, which
            logs a warning.
        for_each_stats: The iteration counts of a for-each task, as the platform reports them, or `None` for any
            other task and for a for-each task run that reports no stats.
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
    timing: Timing = field(default_factory=Timing)
    effective_performance_target: str | None = None
    condition_outcome: bool | None = None
    sql_output: jobs.SqlOutput | None = None
    alert_output: jobs.AlertTaskOutput | None = None
    dbt_output: jobs.DbtOutput | None = None
    dbt: DbtArtifacts | None = None
    run_job_output: jobs.RunJobOutput | None = None
    child_run_id: int | None = None
    child_run: JobRun | None = None
    dashboard_output: jobs.DashboardTaskOutput | None = None
    clean_rooms_notebook_output: jobs.CleanRoomsNotebookTaskCleanRoomsNotebookTaskOutput | None = None
    notebook: NotebookView | None = None
    sql_result: SqlResult | None = None
    pipeline_update: PipelineUpdate | None = None
    iterations: list[TaskRun] = field(default_factory=list)
    for_each_stats: ForEachStats | None = None

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
        """Whether the task ran. False if it was skipped, excluded, disabled or cancelled, or has no result state.

        The platform reports no result state for the tasks of a run it skipped because every task is disabled.
        """
        return ran(self.result_state)

    @property
    def excluded(self) -> bool:
        """Whether the task ended `EXCLUDED` because its `run_if` or if/else condition was not met."""
        return self.result_state == "EXCLUDED"

    @property
    def upstream_failed(self) -> bool:
        """Whether the task ended `UPSTREAM_FAILED` because a task it depends on failed."""
        return self.result_state == "UPSTREAM_FAILED"

    @property
    def disabled(self) -> bool:
        """Whether the task ended `DISABLED`: disabled in the job, downstream of a disabled task, or left out by `only`.

        `JobRun.disabled_tasks` and `JobRun.skipped_tasks` tell them apart.
        """
        return self.result_state == "DISABLED"

    @property
    def cancelled(self) -> bool:
        """Whether the task ended `CANCELED`."""
        return self.result_state == "CANCELED"

    @property
    def alert_state(self) -> str | None:
        """The state an alert evaluated to: `TRIGGERED`, `OK`, `ERROR` or `UNKNOWN`.

        Read from `alert_output` for an alert task, else from `sql_output.alert_output` for a legacy SQL alert task.
        `None` for any other task or when the platform reported no state. An alert task succeeds whether or not the
        alert triggered, so assert on this rather than on `succeeded`. `ERROR` means the alert could not be
        evaluated, for example because its query failed; the task then fails too.
        """
        # Observed on an alert task run: runs/get-output carries `alert_output.alert_state`, which databricks-sdk
        # models, with `TRIGGERED`, `OK`, or `ERROR` when the alert's query failed.
        if self.alert_output is not None and self.alert_output.alert_state is not None:
            return enum_value(self.alert_output.alert_state)
        if self.sql_output is not None and self.sql_output.alert_output is not None:
            return enum_value(self.sql_output.alert_output.alert_state)
        return None

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
        detail = ""
        if self.error:
            detail = f", error={first_line(str(self.error))!r}"
            if self.parameters is not None:
                detail += f", params={self.parameters!r}"
        return f"TaskRun({self.task_key!r}, {self.result_state}{detail}, {self.run_url})"


@dataclass(frozen=True)
class StepResult:
    """The outcome of a `setup=` or `teardown=` step, however it ran.

    `JobRun.setup` and `JobRun.teardown` hold one, so do `EphemeralJob.teardown` for a teardown that ran when the job
    closed and the return value of `Lakeflow.once()`.

    Attributes:
        role: `"setup"` or `"teardown"`.
        ran_as: `"connect"` when the step ran from the test process over Databricks Connect, `"task"` when it ran as a
            notebook task of the job.
        succeeded: Whether the step finished without an error.
        output: What the step gave back, as JSON text: a function's return value, or `{"statements": <count>}` for a
            list of SQL statements. `None` when a function returned `None`, or a notebook file did not call
            `dbutils.notebook.exit()`.
        error: What the step raised, or `None`. Over Connect, `type` is the exception's class, dotted with its module
            unless it is a built-in, and `trace` the Python traceback.
        duration: How long the step ran.
        task: The step's task run, when it ran as a task, else `None`.
    """

    role: str
    ran_as: str
    succeeded: bool
    output: str | None = None
    error: TaskError | None = None
    duration: timedelta = timedelta(0)
    task: TaskRun | None = field(default=None, repr=False)

    @property
    def output_json(self) -> object:
        """`output` parsed as JSON.

        Raises:
            ValueError: If the step gave nothing back, or its output is not JSON. The message names the step.
        """
        if self.output is None:
            raise ValueError(f"The {self.role} step has no output: it returned None")
        try:
            return json.loads(self.output)
        except ValueError as e:
            raise ValueError(f"The {self.role} step's output is not JSON: {self.output[:200]!r}") from e

    @classmethod
    def from_task(cls, role: str, task: TaskRun) -> StepResult:
        """Describe a step that ran as a task by that task's run.

        Args:
            role: `"setup"` or `"teardown"`.
            task: The task run of `__pytest_setup` or `__pytest_teardown`.

        Returns:
            The step's outcome, with `task` set.
        """
        return cls(
            role=role,
            ran_as="task",
            succeeded=task.succeeded,
            output=task.output,
            error=task.error,
            duration=task.duration,
            task=task,
        )


@dataclass(frozen=True)
class JobRun:
    """The outcome of one run of an ephemeral job, or of the child run that a run-job task triggered.

    Attributes:
        job_key: The bundle resource key of the job. For a child run, the child job's key when the job is in the
            bundle, otherwise its job id as a string.
        job_id: The ephemeral job's id. For a child run, the id of the deployed child job.
        run_id: The run's id.
        run_url: The run's page in the workspace.
        result_state: The parent run's result state. Not a verdict: a run whose probe ran after a failed task
            ends `SUCCESS_WITH_FAILURES`. Use `succeeded` instead.
        termination_code: The parent run's termination code.
        termination_message: The parent run's termination message.
        duration: The time from start to end of the parent run.
        tasks: The user's tasks that were part of the run, by task key.
        injected_tasks: The stub, probe, setup and teardown tasks added by the plugin, by task key.
        skipped_tasks: Tasks left out by `only`, by task key.
        disabled_tasks: Tasks disabled in the job, in the bundle or with `disable=`, and the tasks the platform
            disabled because they are downstream of one, by task key. When every task is disabled, the platform skips
            the run: its tasks have no result state and are all here.
        parameters: The run's job parameters by name: the value passed for the run, or the default when none was.
            Parameters the plugin injects are left out.
        timing: The queue, setup, execution and cleanup durations of the parent run. Zero where the platform
            reported none; a run with several tasks reports its time on the tasks instead.
        effective_performance_target: The serverless performance mode the run used, `STANDARD` or
            `PERFORMANCE_OPTIMIZED`, or `None` when the platform did not say.
        trigger_type: What started the run, as the Jobs API reports it: `ONE_TIME` for a run of an ephemeral job,
            which is started with run-now. A trigger simulated with `trigger=` on `Lakeflow.job` or `Lakeflow.task`
            changes only what the task code sees, not this. `None` when the platform did not say.
        setup: The outcome of the job's `setup=` step for this run, or `None` when the job has none. As a task, a
            failure leaves the user's tasks `UPSTREAM_FAILED`, so `succeeded` is false and `repr()` names it first;
            over Connect, a failure raises before the run starts.
        teardown: The outcome of the job's `teardown=` step for this run, or `None` when the job has none or runs it
            when the job closes, with `teardown_at="close"`, into `EphemeralJob.teardown`. A failed teardown does
            not change `succeeded`; the plugin logs a warning instead. Assert `run.teardown.succeeded` to fail the
            test on it.
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
    disabled_tasks: dict[str, TaskRun] = field(default_factory=dict)
    parameters: dict[str, str] = field(default_factory=dict)
    timing: Timing = field(default_factory=Timing)
    effective_performance_target: str | None = None
    trigger_type: str | None = None
    setup: StepResult | None = None
    teardown: StepResult | None = None

    @property
    def succeeded(self) -> bool:
        """Whether every task in `tasks` ended `SUCCESS`.

        Injected and skipped tasks do not count, nor do tasks `EXCLUDED` because their `run_if` or if/else
        condition was not met.
        """
        return bool(self.tasks) and not self._failing_tasks()

    @property
    def failed(self) -> bool:
        """Whether the run did not succeed."""
        return not self.succeeded

    @property
    def timed_out(self) -> bool:
        """Whether the parent run ended `TIMEDOUT` or any task in `tasks` timed out."""
        return self.result_state == "TIMEDOUT" or any(task.timed_out for task in self.tasks.values())

    @property
    def cancelled(self) -> bool:
        """Whether the parent run ended `CANCELED`, as a run stopped by `stop_after` does. Such a run also `failed`."""
        return self.result_state == "CANCELED"

    @property
    def executed_tasks(self) -> list[str]:
        """The sorted keys of the tasks in `tasks` whose `ran` is true."""
        return sorted(key for key, task in self.tasks.items() if task.ran)

    def task(self, key: str) -> TaskRun:
        """Get one of the user's tasks.

        Args:
            key: The task key.

        Returns:
            The task's outcome.

        Raises:
            KeyError: If the task was not part of the run. The message lists the available keys and says whether
                the task was disabled in the job or skipped by `only`.
        """
        if key in self.tasks:
            return self.tasks[key]
        reason = ""
        if key in self.disabled_tasks:
            reason = f" Disabled in the job: {sorted(self.disabled_tasks)}."
        elif key in self.skipped_tasks:
            reason = f" Skipped by only: {sorted(self.skipped_tasks)}."
        raise KeyError(f"No task {key!r} in run. Available: {sorted(self.tasks)}.{reason}")

    def branch_taken(self, key: str) -> bool:
        """Get the outcome of an if/else condition task.

        Args:
            key: The condition task's key.

        Returns:
            The task's `condition_outcome`: `True` when the condition held, `False` when it did not.

        Raises:
            KeyError: If the task was not part of the run.
            LakeflowError: If the task is not a condition task, or the platform reported no outcome for it.
        """
        task = self.task(key)
        if task.condition_outcome is None:
            raise LakeflowError(
                f"Task {key!r} is not a condition task or has no outcome; check its result_state ({task.result_state})"
            )
        return task.condition_outcome

    def raise_for_failure(self) -> JobRun:
        """Raise if the run did not succeed.

        Returns:
            The run itself, so `run = job.run().raise_for_failure()` works.

        Raises:
            LakeflowRunFailed: If `succeeded` is false. The message is `repr(run)`, with the run's job parameters
                on a `params:` line of their own when it has any, followed by one line per failing task with its
                error, the error's root cause when there is one, and the task's run page. A failing task whose
                resolved parameters are known gets an indented `params:` line after its own.
        """
        if self.succeeded:
            return self
        run_parameters = format_parameters(self.parameters)
        # The job parameters go on a line of their own, so the first line leaves them out.
        lines = [self._describe(show_parameters=not run_parameters)]
        if run_parameters:
            lines.append(f"params: {run_parameters}")
        task_parameters = failing_parameters(self)
        for task in self._failing_tasks():
            outcome = str(task.error) if task.error else task.result_state
            cause = ""
            if task.error and task.error.root_cause:
                cause = f" (root cause: {task.error.root_cause})"
            lines.append(f"{task.task_key}: {outcome}{cause} {task.run_url}")
            if task.task_key in task_parameters:
                lines.append(f"  params: {task_parameters[task.task_key]}")
        raise LakeflowRunFailed("\n".join(lines), self)

    def as_dict(self) -> dict[str, Any]:
        """Get the run as a dict that `json.dumps` accepts, as `--lakeflow-report-dir` writes it to `report.json`.

        Every field of the run and of each `TaskRun` is kept, including `attempts` and a nested `child_run`, with these
        changes: `duration` becomes `duration_seconds` and each `timing` a dict of `queue_seconds`, `setup_seconds`,
        `execution_seconds` and `cleanup_seconds`; `error` is a dict of `type`, `message`, `trace`, `root_cause` and
        `code`, or `None`; `task_values` is a plain dict and `missing_task_value_reasons` holds the reason for each of
        `missing_task_values`; the outputs of SQL, alert, dbt, run-job, dashboard and clean room tasks are the dicts of
        the Jobs API, except that a dbt output leaves out `artifacts_headers`; `dbt` is a dict of `artifacts_dir`,
        `results` (each a dict of the `DbtResult` fields, by unique id), `failed_models` and `all_passed`, leaving out
        `run_results` and `manifest`, or `None`; `pipeline_update` is a dict of
        `pipeline_id`, `update_id`, `state`, `cause` and `full_refresh`, or `None`; `iterations` is a list of tasks
        serialised the same way, and `for_each_stats` a dict of its fields, with `errors` a list of dicts, or `None`.
        `setup` and `teardown` are each a dict of `role`, `ran_as`, `succeeded`, `output`, `error`, `duration_seconds`
        and `task_key`, the last `None` for a step that ran over Connect, or `None`. The run also gets `succeeded`.

        `logs` and the exported `notebook` are left out, because logs can reach 5 MB per task. Instead `logs_file` and
        `notebook_file` name where `--lakeflow-report-dir` saves them, relative to the test's report directory:
        `<run_id>/<task_key>.log` and `<run_id>/<task_key>.html`, with `.attempt-<n>` before the suffix for an attempt
        that was retried and `.iteration-<n>`, counting from 0, for an iteration of a for-each task. Each is `None`
        when the task has no logs or no exported notebook.

        Returns:
            The run as a JSON-serialisable dict.
        """
        data, _ = serialise_with_files(self)
        return data

    def _failing_tasks(self) -> list[TaskRun]:
        return [task for task in self.tasks.values() if not task.succeeded and not task.excluded]

    def __repr__(self) -> str:
        return self._describe(show_parameters=True)

    def _describe(self, *, show_parameters: bool) -> str:
        failing = describe_failing(self)
        parameters = ""
        if show_parameters and failing and self.parameters:
            parameters = f"params={self.parameters!r}, "
        return f"JobRun({self.job_key!r}, {self.result_state}, {self.run_url}, {parameters}failing={failing})"


def describe_failing(run: JobRun) -> list[str]:
    """Describe each failing task of `run` as `key: error`, with the error's first line or the result state.

    A failed setup task comes first, as `setup: error`, because it is why the tasks after it did not run. When the
    run did not succeed and no task in `tasks` is failing, the run's termination message follows as `run: message`,
    as for a run the platform skipped because every task is disabled.
    """
    described: list[str] = []
    if run.setup is not None and not run.setup.succeeded:
        described.append(f"setup: {step_outcome(run.setup)}")
    failing = run._failing_tasks()
    for task in failing:
        described.append(f"{task.task_key}: {task_outcome(task)}")
    if run.failed and not failing and run.termination_code != "SUCCESS" and run.termination_message:
        described.append(f"run: {run.termination_message}")
    return described


def step_outcome(step: StepResult) -> str:
    """The first line of the step's error, or its task's result state, or `failed` when it has neither."""
    if step.task is not None:
        return task_outcome(step.task)
    if step.error is not None:
        return first_line(str(step.error))
    return "failed"


def task_outcome(task: TaskRun) -> str:
    """The first line of the task's error, or its result state when it has none."""
    if task.error:
        return first_line(str(task.error))
    return str(task.result_state)


def describe_notebooks(run: JobRun, tail: int = 50) -> list[str]:
    """Describe the exported notebook of each failing task of `run`.

    Each gets the first code line of the cell that failed and the last `tail` lines of what the notebook printed.
    """
    described: list[str] = []
    for task in run._failing_tasks():
        notebook = task.notebook
        if notebook is None:
            continue
        key = task.task_key
        failed = notebook.failed_cell
        if failed is not None:
            described.append(f"{key}: notebook.cells[{failed.index}] failed: {first_line(failed.code)}")
        lines = notebook.stdout.splitlines()
        if not lines:
            continue
        if len(lines) > tail:
            described.append(f"{key}: notebook stdout, last {tail} of {len(lines)} lines:")
        else:
            described.append(f"{key}: notebook stdout:")
        for line in lines[-tail:]:
            described.append(f"  {line}")
    return described


def failing_parameters(run: JobRun) -> dict[str, str]:
    """Format the resolved parameters of each failing task of `run` that has any, by task key."""
    formatted: dict[str, str] = {}
    for task in run._failing_tasks():
        text = format_parameters(task.parameters)
        if text:
            formatted[task.task_key] = text
    return formatted


def ran(result_state: str | None) -> bool:
    # A task with no result state did not run: the platform reports none for the tasks of a skipped run.
    return result_state is not None and result_state not in NOT_RUN


def enum_value(value: Enum | str | None) -> str | None:
    if isinstance(value, Enum):
        return value.value
    return value


def duration(start: int | None, end: int | None) -> timedelta:
    if start and end and end >= start:
        return timedelta(milliseconds=end - start)
    return timedelta(0)


def _millis(value: int | None) -> timedelta:
    if value:
        return timedelta(milliseconds=value)
    return timedelta(0)
