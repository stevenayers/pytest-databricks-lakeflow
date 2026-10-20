from __future__ import annotations

import base64
import json
import logging
import re
import urllib.parse
import warnings
from collections.abc import Collection, Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta
from enum import Enum
from html import unescape
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, get_args

from databricks.sdk.errors.base import DatabricksError
from databricks.sdk.service import jobs, pipelines

from pytest_databricks_lakeflow.errors import LakeflowError, LakeflowRunFailed

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

log = logging.getLogger("pytest_databricks_lakeflow")

PREFIX = "__pytest_"
PROBE_PREFIX = f"{PREFIX}probe__"
SETUP_KEY = f"{PREFIX}setup"
TEARDOWN_KEY = f"{PREFIX}teardown"
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

NotebookExport = Literal["never", "failed", "always"]
NOTEBOOK_EXPORTS: tuple[str, ...] = get_args(NotebookExport)
NOTEBOOK_MODEL = "__DATABRICKS_NOTEBOOK_MODEL"
NOTEBOOK_MODEL_LINE = re.compile(NOTEBOOK_MODEL + r"\s*=\s*'([^']*)'")
HTML_TAG = re.compile(r"<[^>]*>")
HTML_RESULTS = frozenset({"html", "htmlSandbox"})
SKIPPED_SUMMARY = "Command skipped"
RUN_OUTPUT_PATH = "/api/2.2/jobs/runs/get-output"
# An update's "started an update" event is logged a few milliseconds before the update's creation time (7 ms on the
# workspace), so the events of an update are read from a margin before it.
EVENT_LEAD_MS = 60_000


def _first_line(text: str, limit: int = 200) -> str:
    lines = text.strip().splitlines()
    line = lines[0] if lines else ""
    return line if len(line) <= limit else f"{line[: limit - 3]}..."


def _last_exception_line(lines: Sequence[str]) -> tuple[str, str] | None:
    # The last `Name: message` line of a trace names the exception that ended it.
    for line in reversed(lines):
        match = EXC_LINE.match(line)
        if match and match["msg"] is not None:
            return match["name"], match["msg"].strip()
    return None


def format_parameters(parameters: Mapping[str, str] | Sequence[str] | None) -> str:
    """Format resolved parameters as one line of JSON with sorted keys, or `""` when there are none.

    Each value is cut to its first line of at most 200 characters. Keys the plugin injects are left out.
    """
    shown: dict[str, str] | list[str]
    if isinstance(parameters, Mapping):
        shown = {}
        for key, value in parameters.items():
            if not key.startswith(PREFIX):
                shown[key] = _first_line(str(value))
    else:
        shown = [_first_line(str(value)) for value in parameters or []]
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
        last = _last_exception_line(trace_text.splitlines())
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
        last = _last_exception_line(lines)
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
        return _join_lines(cell.stdout for cell in self.cells)

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


def parse_notebook_export(html: str) -> NotebookView:
    """Read the cells of a notebook view exported by `jobs.export_run`.

    The export embeds the notebook as a `__DATABRICKS_NOTEBOOK_MODEL` line holding base64 of URL-encoded JSON. The
    format is not documented, so parsing is best effort: when the model is missing or unreadable, the view has no
    cells, keeps the HTML, and a `UserWarning` names the marker.

    Args:
        html: The `content` of a `NOTEBOOK` view.

    Returns:
        The notebook's cells and the HTML.
    """
    model = _notebook_model(html)
    if model is None:
        warnings.warn(
            f"The exported notebook has no readable {NOTEBOOK_MODEL} model, so its cells are empty; "
            "NotebookView.html keeps the export",
            stacklevel=2,
        )
        return NotebookView(html=html)
    commands: list[dict[str, Any]] = []
    for command in model.get("commands") or []:
        if isinstance(command, dict):
            commands.append(command)
    commands.sort(key=_position)
    cells = [_notebook_cell(index, command) for index, command in enumerate(commands)]
    return NotebookView(html=html, cells=cells)


def _notebook_model(html: str) -> dict[str, Any] | None:
    match = NOTEBOOK_MODEL_LINE.search(html)
    if match is None:
        return None
    try:
        encoded = base64.b64decode(match[1]).decode()
        model = json.loads(urllib.parse.unquote(encoded))
    except ValueError:
        return None
    if not isinstance(model, dict):
        return None
    return model


def _position(command: dict[str, Any]) -> float:
    position = command.get("position")
    if isinstance(position, int | float):
        return float(position)
    return 0.0


def _notebook_cell(index: int, command: dict[str, Any]) -> NotebookCell:
    results = command.get("results")
    if not isinstance(results, dict):
        results = {}
    is_error = results.get("type") == "error"
    texts: list[str] = []
    raw: list[str] = []
    printed: list[str] = []
    if not is_error:
        for kind, name, data in _result_items(results):
            raw.append(data)
            if kind in HTML_RESULTS:
                texts.append(_html_text(data))
            else:
                texts.append(ANSI.sub("", data))
            # Printed text is an item named "stdout"; stderr is a sibling item, the exit value a "raw" result.
            if name == "stdout":
                printed.append(ANSI.sub("", data))
    trace = command.get("error")
    if not trace and is_error:
        trace = "".join(data for _, _, data in _result_items(results))
    code = command.get("command")
    if not isinstance(code, str):
        code = ""
    return NotebookCell(
        index=index,
        code=code,
        output=_join_lines(texts),
        output_html="".join(raw),
        error=_cell_error(command.get("errorSummary"), trace),
        stdout=_join_lines(printed),
    )


def _result_items(results: dict[str, Any]) -> list[tuple[str | None, str | None, str]]:
    # `results.data` is a string, or a list of items with their own `type`, `name` and `data`, or rows of a table.
    data = results.get("data")
    if isinstance(data, str):
        return [(results.get("type"), None, data)]
    if not isinstance(data, list):
        return []
    items: list[tuple[str | None, str | None, str]] = []
    for item in data:
        if isinstance(item, dict) and isinstance(item.get("data"), str):
            items.append((item.get("type"), item.get("name"), item["data"]))
    return items


def _cell_error(summary: object, trace: object) -> str | None:
    # The platform marks a cell skipped after a failure with the summary "Command skipped" and a null `error`.
    if isinstance(summary, str) and summary.strip() == SKIPPED_SUMMARY:
        return None
    if isinstance(summary, str) and summary.strip():
        return _error_text(summary).strip()
    if not isinstance(trace, str) or not trace.strip():
        return None
    text = _error_text(trace)
    last = _last_exception_line(text.splitlines())
    if last is None:
        return _first_line(text)
    name, message = last
    if message:
        return f"{name}: {message}"
    return name


def _html_text(text: str) -> str:
    return unescape(HTML_TAG.sub("", ANSI.sub("", text)))


def _error_text(text: str) -> str:
    # Older exports render errors as HTML; a plain trace keeps names such as `<locals>` and `<command-1>`.
    if "</" in text:
        return _html_text(text)
    return ANSI.sub("", text)


def _join_lines(parts: Iterable[str]) -> str:
    joined = ""
    for part in parts:
        if part and not part.endswith("\n"):
            part += "\n"
        joined += part
    return joined


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
            state=_enum(info.state),
            cause=_enum(info.cause),
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
        dbt_output: The output of a dbt task, with `artifacts_link` and `artifacts_headers`, or `None`.
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
    run_job_output: jobs.RunJobOutput | None = None
    child_run_id: int | None = None
    child_run: JobRun | None = None
    dashboard_output: jobs.DashboardTaskOutput | None = None
    clean_rooms_notebook_output: jobs.CleanRoomsNotebookTaskCleanRoomsNotebookTaskOutput | None = None
    notebook: NotebookView | None = None
    sql_result: SqlResult | None = None
    pipeline_update: PipelineUpdate | None = None

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
        return _ran(self.result_state)

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
            return _enum(self.alert_output.alert_state)
        if self.sql_output is not None and self.sql_output.alert_output is not None:
            return _enum(self.sql_output.alert_output.alert_state)
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
            detail = f", error={_first_line(str(self.error))!r}"
            if self.parameters is not None:
                detail += f", params={self.parameters!r}"
        return f"TaskRun({self.task_key!r}, {self.result_state}{detail}, {self.run_url})"


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

    @property
    def setup(self) -> TaskRun | None:
        """The task that ran the job's `setup=` function, or `None` when the job has none.

        When it fails, the user's tasks end `UPSTREAM_FAILED`, so `succeeded` is false, and `repr()` names the setup
        failure first.
        """
        return self.injected_tasks.get(SETUP_KEY)

    @property
    def teardown(self) -> TaskRun | None:
        """The task that ran the job's `teardown=` function, or `None` when the job has none or the run left it out.

        A run leaves it out when the job was made with `teardown_at="close"`: the task then ends `DISABLED` and runs
        alone when the job closes, in `EphemeralJob.teardown_run`. A failed teardown does not change `succeeded`; the
        plugin logs a warning instead. Assert `run.teardown.succeeded` to fail the test on it.
        """
        teardown = self.injected_tasks.get(TEARDOWN_KEY)
        if teardown is None or teardown.disabled:
            return None
        return teardown

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
        described.append(f"setup: {task_outcome(run.setup)}")
    failing = run._failing_tasks()
    for task in failing:
        described.append(f"{task.task_key}: {task_outcome(task)}")
    if run.failed and not failing and run.termination_code != "SUCCESS" and run.termination_message:
        described.append(f"run: {run.termination_message}")
    return described


def task_outcome(task: TaskRun) -> str:
    """The first line of the task's error, or its result state when it has none."""
    if task.error:
        return _first_line(str(task.error))
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
            described.append(f"{key}: notebook.cells[{failed.index}] failed: {_first_line(failed.code)}")
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


def serialise(run: JobRun) -> dict[str, Any]:
    """Turn a run into a dict that `json.dumps` accepts, as `--lakeflow-report-dir` writes it to `report.json`.

    Every field of the run and of each `TaskRun` is kept, including `attempts` and a nested `child_run`, with these
    changes: `duration` becomes `duration_seconds` and each `timing` a dict of `queue_seconds`, `setup_seconds`,
    `execution_seconds` and `cleanup_seconds`; `error` is a dict of `type`, `message`, `trace`, `root_cause` and
    `code`, or `None`; `task_values` is a plain dict and `missing_task_value_reasons` holds the reason for each of
    `missing_task_values`; the outputs of SQL, alert, dbt, run-job, dashboard and clean room tasks are the dicts of
    the Jobs API; `pipeline_update` is a dict of `pipeline_id`, `update_id`, `state`, `cause` and `full_refresh`, or
    `None`. The run also gets `succeeded`.

    `logs` and the exported `notebook` are left out, because logs can reach 5 MB per task. Instead `logs_file` and
    `notebook_file` name where `--lakeflow-report-dir` saves them, relative to the test's report directory:
    `<run_id>/<task_key>.log` and `<run_id>/<task_key>.html`, with `.attempt-<n>` before the suffix for an attempt
    that was retried. Each is `None` when the task has no logs or no exported notebook.

    Args:
        run: The run.

    Returns:
        The run as a JSON-serialisable dict.
    """
    data, _ = serialise_with_files(run)
    return data


def serialise_with_files(run: JobRun) -> tuple[dict[str, Any], dict[str, str]]:
    """Serialise a run as `serialise` does, and collect the logs and notebook HTML it names, by file name."""
    files: dict[str, str] = {}
    return _serialise_run(run, files), files


def _serialise_run(run: JobRun, files: dict[str, str]) -> dict[str, Any]:
    def tasks(group: Mapping[str, TaskRun]) -> dict[str, Any]:
        return {key: _serialise_task(task, run.run_id, key, files) for key, task in group.items()}

    return {
        "job_key": run.job_key,
        "job_id": run.job_id,
        "run_id": run.run_id,
        "run_url": run.run_url,
        "result_state": run.result_state,
        "succeeded": run.succeeded,
        "termination_code": run.termination_code,
        "termination_message": run.termination_message,
        "duration_seconds": run.duration.total_seconds(),
        "tasks": tasks(run.tasks),
        "injected_tasks": tasks(run.injected_tasks),
        "skipped_tasks": tasks(run.skipped_tasks),
        "disabled_tasks": tasks(run.disabled_tasks),
        "parameters": run.parameters,
        "timing": _serialise_timing(run.timing),
        "effective_performance_target": run.effective_performance_target,
        "trigger_type": run.trigger_type,
    }


def _serialise_task(task: TaskRun, run_id: int, stem: str, files: dict[str, str]) -> dict[str, Any]:
    # `stem` names the side files: the task key, plus the attempt number for an attempt that was retried.
    logs_file = None
    if task.logs:
        logs_file = f"{run_id}/{stem}.log"
        files[logs_file] = task.logs
    notebook_file = None
    if task.notebook is not None:
        notebook_file = f"{run_id}/{stem}.html"
        files[notebook_file] = task.notebook.html
    attempts: list[dict[str, Any]] = []
    for index, attempt in enumerate(task.attempts):
        attempt_stem = stem
        if index < len(task.attempts) - 1:
            attempt_stem = f"{stem}.attempt-{attempt.attempt}"
        attempts.append(_serialise_task(attempt, run_id, attempt_stem, files))
    child_run = None
    if task.child_run is not None:
        child_run = _serialise_run(task.child_run, files)
    sql_result = None
    if task.sql_result is not None:
        sql_result = asdict(task.sql_result)
    pipeline_update = None
    if task.pipeline_update is not None:
        update = task.pipeline_update
        pipeline_update = {
            "pipeline_id": update.pipeline_id,
            "update_id": update.update_id,
            "state": update.state,
            "cause": update.cause,
            "full_refresh": update.full_refresh,
        }
    return {
        "task_key": task.task_key,
        "run_id": task.run_id,
        "result_state": task.result_state,
        "termination_code": task.termination_code,
        "termination_message": task.termination_message,
        "run_url": task.run_url,
        "duration_seconds": task.duration.total_seconds(),
        "output": task.output,
        "output_truncated": task.output_truncated,
        "logs_file": logs_file,
        "logs_truncated": task.logs_truncated,
        "error": _serialise_error(task.error),
        "task_values": dict(task.task_values),
        "missing_task_values": list(task.missing_task_values),
        "missing_task_value_reasons": dict(task.task_values.reasons),
        "attempt": task.attempt,
        "parameters": task.parameters,
        "attempts": attempts,
        "timing": _serialise_timing(task.timing),
        "effective_performance_target": task.effective_performance_target,
        "condition_outcome": task.condition_outcome,
        "sql_output": _as_dict(task.sql_output),
        "alert_output": _as_dict(task.alert_output),
        "dbt_output": _as_dict(task.dbt_output),
        "run_job_output": _as_dict(task.run_job_output),
        "child_run_id": task.child_run_id,
        "child_run": child_run,
        "dashboard_output": _as_dict(task.dashboard_output),
        "clean_rooms_notebook_output": _as_dict(task.clean_rooms_notebook_output),
        "notebook_file": notebook_file,
        "sql_result": sql_result,
        "pipeline_update": pipeline_update,
    }


def _serialise_error(error: TaskError | None) -> dict[str, Any] | None:
    if error is None:
        return None
    return {
        "type": error.type,
        "message": error.message,
        "trace": error.trace,
        "root_cause": error.root_cause,
        "code": error.code,
    }


def _serialise_timing(timing: Timing) -> dict[str, float]:
    return {
        "queue_seconds": timing.queue.total_seconds(),
        "setup_seconds": timing.setup.total_seconds(),
        "execution_seconds": timing.execution.total_seconds(),
        "cleanup_seconds": timing.cleanup.total_seconds(),
    }


def _as_dict(output: Any) -> dict[str, Any] | None:
    # The task outputs are databricks-sdk dataclasses, which turn themselves into the API's JSON.
    if output is None:
        return None
    return output.as_dict()


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
    job_keys: Mapping[int, str] | None = None,
    notebook_export: NotebookExport = "failed",
) -> JobRun:
    """Build a `JobRun` from a terminated run, reading each task's output and resolved parameters.

    When the platform retried a task, the `TaskRun` describes its last attempt and its `attempts` hold every
    attempt. Probe outputs become the `values` of the tasks they watched.

    Args:
        ws: The workspace client.
        run: The terminated parent run from `get_run`.
        job_key: The bundle resource key of the job.
        job_id: The ephemeral job's id.
        stubs: Keys of stub tasks, which count as injected tasks.
        capture_task_values: The value keys requested from each probed task, by task key.
        disabled: Keys of the tasks disabled in the job's settings and of the tasks downstream of them, as
            `disabled_task_keys` returns them. The Jobs API does not flag them on the run, where a disabled task, a
            task downstream of one and a task left out by `only` all end `DISABLED`. A task in `disabled` that ended
            `DISABLED` or has no result state is filed under `disabled_tasks`; any other task that ended `DISABLED`
            under `skipped_tasks`.
        outputs: Task outputs already read, by task run id. Only the outputs missing from it are fetched.
        job_keys: Bundle resource keys by job id, which name the child runs of run-job tasks.
        notebook_export: Which notebook tasks to export with `jobs.export_run`: `failed` for those that ran and did
            not succeed, `always` for every one that ran, `never` for none. Only the last attempt of a user task is
            exported.

    Returns:
        The run's outcome.
    """
    task_runs_by_key = _task_runs_by_key(run)
    known_outputs = dict(outputs or {})
    unread: list[jobs.RunTask] = []
    for task_runs in task_runs_by_key.values():
        for task_run in task_runs:
            if has_output(task_run) and (task_run.run_id or 0) not in known_outputs:
                unread.append(task_run)
    known_outputs.update(fetch_outputs(ws, unread))
    ran: list[jobs.RunTask] = []
    for task_runs in task_runs_by_key.values():
        for task_run in task_runs:
            if _ran(_result_state(task_run)):
                ran.append(task_run)
    resolved = fetch_resolved_values(ws, ran)
    sql_results = fetch_sql_results(ws, ran)
    pipeline_updates = fetch_pipeline_updates(ws, ran)

    attempts: dict[str, list[TaskRun]] = {}
    for key, task_runs in task_runs_by_key.items():
        attempts[key] = []
        for task_run in task_runs:
            run_id = task_run.run_id or 0
            attempt = _task_run(
                task_run,
                known_outputs.get(run_id),
                resolved.get(run_id),
                sql_results.get(run_id),
                pipeline_updates.get(run_id),
            )
            attempts[key].append(_with_child_run(ws, attempt, job_keys or {}, notebook_export))

    injected: dict[str, TaskRun] = {}
    user: dict[str, TaskRun] = {}
    for key, key_attempts in attempts.items():
        if key.startswith(PREFIX) or key in stubs:
            injected[key] = key_attempts[-1]
        else:
            user[key] = key_attempts[-1]
    for target, keys in (capture_task_values or {}).items():
        if target in user:
            probe = injected.get(f"{PROBE_PREFIX}{target}")
            user[target] = _with_values(user[target], probe, keys)
    user = _with_notebooks(ws, user, task_runs_by_key, notebook_export)
    injected = {key: _with_attempts(task, attempts[key]) for key, task in injected.items()}
    user = {key: _with_attempts(task, attempts[key]) for key, task in user.items()}

    tasks: dict[str, TaskRun] = {}
    skipped_tasks: dict[str, TaskRun] = {}
    disabled_tasks: dict[str, TaskRun] = {}
    for key, task in user.items():
        if key in disabled and (task.disabled or task.result_state is None):
            disabled_tasks[key] = task
        elif task.disabled:
            skipped_tasks[key] = task
        else:
            tasks[key] = task

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
        tasks=tasks,
        injected_tasks=injected,
        skipped_tasks=skipped_tasks,
        disabled_tasks=disabled_tasks,
        parameters=_job_parameters(run.job_parameters),
        timing=Timing.from_run(run),
        effective_performance_target=_enum(run.effective_performance_target),
        trigger_type=_enum(run.trigger),
    )


def _task_runs_by_key(run: jobs.Run) -> dict[str, list[jobs.RunTask]]:
    # Every attempt of a task, in attempt order, keyed by task key in the order the run lists the tasks.
    by_key: dict[str, list[jobs.RunTask]] = {}
    for task_run in run.tasks or []:
        by_key.setdefault(task_run.task_key or "", []).append(task_run)
    for task_runs in by_key.values():
        task_runs.sort(key=lambda t: t.attempt_number or 0)
    return by_key


def _with_child_run(
    ws: WorkspaceClient, task: TaskRun, job_keys: Mapping[int, str], notebook_export: NotebookExport
) -> TaskRun:
    if task.child_run_id is None or not task.ran:
        return task
    try:
        child = ws.jobs.get_run(task.child_run_id)
    except DatabricksError as e:
        log.warning("Could not read run %s triggered by task %s: %s", task.child_run_id, task.task_key, e)
        return task
    job_id = child.job_id or 0
    key = job_keys.get(job_id) or str(job_id)
    # `only` never applies to a child run, so every task of it that ended DISABLED is disabled in the child job.
    disabled = {t.task_key for t in child.tasks or [] if t.task_key}
    child_run = build_job_run(
        ws, child, job_key=key, job_id=job_id, disabled=disabled, job_keys=job_keys, notebook_export=notebook_export
    )
    return replace(task, child_run=child_run)


def _with_notebooks(
    ws: WorkspaceClient,
    tasks: dict[str, TaskRun],
    task_runs_by_key: Mapping[str, Sequence[jobs.RunTask]],
    notebook_export: NotebookExport,
) -> dict[str, TaskRun]:
    wanted: list[TaskRun] = []
    for key, task in tasks.items():
        is_notebook = task_runs_by_key[key][-1].notebook_task is not None
        if is_notebook and _should_export(notebook_export, task):
            wanted.append(task)
    exports = fetch_notebook_exports(ws, wanted)
    # Parsed here rather than in the fetching threads, so that a parse warning reaches the test's warning capture.
    with_notebooks = dict(tasks)
    for key, html in exports.items():
        with_notebooks[key] = replace(tasks[key], notebook=parse_notebook_export(html))
    return with_notebooks


def _should_export(notebook_export: NotebookExport, task: TaskRun) -> bool:
    if not task.ran:
        return False
    if notebook_export == "always":
        return True
    if notebook_export == "failed":
        return not task.succeeded
    return False


def _with_attempts(last: TaskRun, attempts: list[TaskRun]) -> TaskRun:
    return replace(last, attempts=[*attempts[:-1], last])


def _job_parameters(parameters: Sequence[jobs.JobParameter] | None) -> dict[str, str]:
    resolved: dict[str, str] = {}
    for parameter in parameters or []:
        if not parameter.name or parameter.name.startswith(PREFIX):
            continue
        if parameter.value is not None:
            resolved[parameter.name] = parameter.value
        else:
            resolved[parameter.name] = parameter.default or ""
    return resolved


def has_output(task: jobs.RunTask) -> bool:
    """Whether a terminated task run has an output to read.

    Every one has but a disabled one and one with no result state, as each task of a skipped run has.
    """
    state = _result_state(task)
    return state is not None and state != "DISABLED"


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


def fetch_notebook_exports(ws: WorkspaceClient, tasks: Sequence[TaskRun]) -> dict[str, str]:
    """Export the notebook of each task run in parallel, as HTML by task key.

    `jobs.export_run` works only on a notebook task run; on a job run or another task type it fails with
    `InvalidParameterValue`. A task run that cannot be exported, or whose export has no notebook view, is left out.
    """
    if not tasks:
        return {}

    def fetch(task: TaskRun) -> str | None:
        try:
            export = ws.jobs.export_run(task.run_id, views_to_export=jobs.ViewsToExport.CODE)
        except DatabricksError as e:
            log.warning("Could not export the notebook of task %s (%s): %s", task.task_key, task.run_url, e)
            return None
        for view in export.views or []:
            if view.type == jobs.ViewType.NOTEBOOK and view.content is not None:
                return view.content
        return None

    with ThreadPoolExecutor(max_workers=8) as pool:
        exports: dict[str, str] = {}
        for task, html in zip(tasks, pool.map(fetch, tasks), strict=True):
            if html is not None:
                exports[task.task_key] = html
        return exports


def fetch_resolved_values(ws: WorkspaceClient, tasks: Sequence[jobs.RunTask]) -> dict[int, jobs.ResolvedValues]:
    """Read the resolved parameters of each task run in parallel, by task run id.

    The Jobs API fills `resolved_values` only when a task run is fetched by its own run id with
    `include_resolved_values`; on the job run every task's `resolved_values` is null, whatever the compute or the API
    version. A task run that cannot be read, or that reports no resolved values, is left out.
    """
    if not tasks:
        return {}

    def fetch(task: jobs.RunTask) -> jobs.ResolvedValues | None:
        try:
            run = ws.jobs.get_run(task.run_id or 0, include_resolved_values=True)
        except DatabricksError as e:
            log.warning("Could not read the parameters of task %s (run %s): %s", task.task_key, task.run_id, e)
            return None
        return run.tasks[0].resolved_values if run.tasks else None

    with ThreadPoolExecutor(max_workers=8) as pool:
        resolved: dict[int, jobs.ResolvedValues] = {}
        for task, values in zip(tasks, pool.map(fetch, tasks), strict=True):
            if values is not None:
                resolved[task.run_id or 0] = values
        return resolved


def fetch_sql_results(ws: WorkspaceClient, tasks: Sequence[jobs.RunTask]) -> dict[int, SqlResult]:
    """Read the query result of each SQL file task run in parallel, by task run id.

    Only task runs with `sql_task.file` are read. The platform reports their rows under `sql_output.file_output`,
    which databricks-sdk does not model, so the raw `runs/get-output` response is read with `ws.api_client`. A task
    run that cannot be read, or that reports no query result, is left out.
    """
    file_tasks: list[jobs.RunTask] = []
    for task in tasks:
        if task.sql_task is not None and task.sql_task.file is not None:
            file_tasks.append(task)
    if not file_tasks:
        return {}

    def fetch(task: jobs.RunTask) -> SqlResult | None:
        try:
            output = ws.api_client.do("GET", RUN_OUTPUT_PATH, query={"run_id": task.run_id or 0})
        except DatabricksError as e:
            log.warning("Could not read the SQL result of task %s (run %s): %s", task.task_key, task.run_id, e)
            return None
        if not isinstance(output, Mapping):
            return None
        return SqlResult.from_output(task.task_key or "", output)

    with ThreadPoolExecutor(max_workers=8) as pool:
        results: dict[int, SqlResult] = {}
        for task, result in zip(file_tasks, pool.map(fetch, file_tasks), strict=True):
            if result is not None:
                results[task.run_id or 0] = result
        return results


def fetch_pipeline_updates(ws: WorkspaceClient, tasks: Sequence[jobs.RunTask]) -> dict[int, PipelineUpdate]:
    """Find the pipeline update that each pipeline task run started, by task run id.

    The Jobs API does not report it: a task run's `pipeline_task` is the task's settings, and `runs/get-output` of a
    pipeline task holds only metadata and the error "Please refer to the logs for this pipeline in the pipelines
    page.", even when the task succeeded. So the pipeline's updates are listed, and the one created between the task
    run's start and end is taken, one whose `cause` is `JOB_TASK` first. The task runs are read in parallel. A task
    run whose update is not found is left out, with a warning.
    """
    pipeline_tasks: list[tuple[jobs.RunTask, str]] = []
    for task in tasks:
        if task.pipeline_task is not None and task.pipeline_task.pipeline_id:
            pipeline_tasks.append((task, task.pipeline_task.pipeline_id))
    if not pipeline_tasks:
        return {}

    def fetch(pair: tuple[jobs.RunTask, str]) -> PipelineUpdate | None:
        return _pipeline_update(ws, *pair)

    with ThreadPoolExecutor(max_workers=8) as pool:
        updates: dict[int, PipelineUpdate] = {}
        for (task, _), update in zip(pipeline_tasks, pool.map(fetch, pipeline_tasks), strict=True):
            if update is not None:
                updates[task.run_id or 0] = update
        return updates


def _pipeline_update(ws: WorkspaceClient, task: jobs.RunTask, pipeline_id: str) -> PipelineUpdate | None:
    start = task.start_time or 0
    end = task.end_time or 0
    try:
        listed = _updates_since(ws, pipeline_id, start)
    except DatabricksError as e:
        log.warning(
            "Could not read the updates of pipeline %s for task %s (run %s): %s",
            pipeline_id,
            task.task_key,
            task.run_id,
            e,
        )
        return None
    in_window: list[pipelines.UpdateInfo] = []
    for info in listed:
        if info.creation_time is not None and start <= info.creation_time <= end:
            in_window.append(info)
    if not in_window:
        log.warning(
            "Found no update of pipeline %s started in the time window of task %s (run %s)",
            pipeline_id,
            task.task_key,
            task.run_id,
        )
        return None
    chosen = in_window[0]
    for info in in_window:
        if info.cause == pipelines.UpdateInfoCause.JOB_TASK:
            chosen = info
            break
    return PipelineUpdate.from_info(ws, chosen, since=start)


def _updates_since(ws: WorkspaceClient, pipeline_id: str, start: int) -> list[pipelines.UpdateInfo]:
    """List the pipeline's updates, newest first as the platform pages them, up to the first created before `start`."""
    updates: list[pipelines.UpdateInfo] = []
    page_token = None
    while True:
        response = ws.pipelines.list_updates(pipeline_id, page_token=page_token)
        page = response.updates or []
        updates.extend(page)
        page_token = response.next_page_token
        if not page_token:
            return updates
        for info in page:
            if info.creation_time is not None and info.creation_time < start:
                return updates


def error_line(task: jobs.RunTask, output: jobs.RunOutput | None) -> str | None:
    """The first line of the error a terminated task run reported, or `None` when it reported none or did not run."""
    if output is None:
        return None
    error = _task_error(task, output)
    if error is None:
        return None
    return _first_line(str(error))


def _task_error(task: jobs.RunTask, out: jobs.RunOutput) -> TaskError | None:
    state = _result_state(task)
    if not _ran(state):
        return None
    # runs/get-output of every pipeline task run carries the error "Please refer to the logs for this pipeline in the
    # pipelines page.", also when the task ended SUCCESS.
    if task.pipeline_task is not None and state == "SUCCESS":
        return None
    error = TaskError.from_output(out.error, out.error_trace)
    if error is None and task.alert_task is not None and state != "SUCCESS":
        return _alert_error(task)
    return error


def _alert_error(task: jobs.RunTask) -> TaskError | None:
    # runs/get-output of an alert task whose query failed holds only the alert state, with no error: the reason is
    # in the task run's termination message, which the legacy state message repeats.
    details = task.status.termination_details if task.status else None
    message = details.message if details else None
    if not message and task.state:
        message = task.state.state_message
    if not message:
        return None
    return TaskError(type=None, message=message, trace="")


def _task_run(
    task: jobs.RunTask,
    out: jobs.RunOutput | None,
    resolved: jobs.ResolvedValues | None,
    sql_result: SqlResult | None,
    pipeline_update: PipelineUpdate | None,
) -> TaskRun:
    out = out or jobs.RunOutput()
    key = task.task_key or ""
    state = _result_state(task)
    details = task.status.termination_details if task.status else None
    notebook = _notebook_output(out)
    output_truncated = bool(notebook and notebook.truncated)
    logs_truncated = bool(out.logs_truncated)
    _warn_when_truncated(key, output_truncated=output_truncated, logs_truncated=logs_truncated)
    return TaskRun(
        task_key=key,
        run_id=task.run_id or 0,
        result_state=state,
        termination_code=_enum(details.code if details else None),
        termination_message=details.message if details else None,
        run_url=task.run_page_url,
        duration=duration(task.start_time, task.end_time),
        output=notebook.result if notebook else None,
        output_truncated=output_truncated,
        logs=out.logs or "",
        logs_truncated=logs_truncated,
        error=_task_error(task, out),
        attempt=task.attempt_number or 0,
        parameters=_parameters(resolved),
        timing=Timing.from_run(task),
        effective_performance_target=_enum(task.effective_performance_target),
        condition_outcome=_condition_outcome(task.condition_task),
        sql_output=out.sql_output,
        alert_output=out.alert_output,
        dbt_output=out.dbt_output,
        run_job_output=out.run_job_output,
        child_run_id=out.run_job_output.run_id if out.run_job_output else None,
        dashboard_output=out.dashboard_output,
        clean_rooms_notebook_output=out.clean_rooms_notebook_output,
        sql_result=sql_result,
        pipeline_update=pipeline_update,
    )


def _notebook_output(out: jobs.RunOutput) -> jobs.NotebookOutput | None:
    # A clean room notebook task reports its exit value under `clean_rooms_notebook_output` instead.
    notebook = out.notebook_output
    if notebook is not None and notebook.result is not None:
        return notebook
    clean_room = out.clean_rooms_notebook_output
    if clean_room and clean_room.notebook_output:
        return clean_room.notebook_output
    return notebook


def _warn_when_truncated(key: str, *, output_truncated: bool, logs_truncated: bool) -> None:
    cut: list[str] = []
    if output_truncated:
        cut.append("output")
    if logs_truncated:
        cut.append("logs")
    if not cut:
        return
    verb = "were" if len(cut) > 1 else "was"
    # stacklevel 3 attributes the warning to the caller of `_task_run`, which builds the whole run.
    warnings.warn(f"The {' and '.join(cut)} of task {key!r} {verb} cut at the 5 MB limit", stacklevel=3)


def _condition_outcome(condition: jobs.RunConditionTask | None) -> bool | None:
    if condition is None:
        return None
    if condition.outcome == "true":
        return True
    if condition.outcome == "false":
        return False
    return None


def _parameters(resolved: jobs.ResolvedValues | None) -> dict[str, str] | list[str] | None:
    if resolved is None:
        return None
    if resolved.notebook_task is not None:
        return resolved.notebook_task.base_parameters
    if resolved.sql_task is not None:
        # The platform adds every job parameter here on each SQL file task, also on one that declares no parameters.
        return resolved.sql_task.parameters
    if resolved.python_wheel_task is not None:
        wheel = resolved.python_wheel_task
        if wheel.named_parameters is not None:
            return wheel.named_parameters
        return wheel.parameters
    if resolved.run_job_task is not None:
        return resolved.run_job_task.job_parameters
    for positional in (resolved.spark_jar_task, resolved.spark_python_task, resolved.spark_submit_task):
        if positional is not None:
            return positional.parameters
    if resolved.dbt_task is not None:
        return resolved.dbt_task.commands
    return None


def _with_values(target: TaskRun, probe: TaskRun | None, keys: Sequence[str]) -> TaskRun:
    name = target.task_key
    if not target.ran:
        reasons = {
            key: f"Task {name!r} did not run ({target.result_state}), so value {key!r} was not captured" for key in keys
        }
        return replace(target, task_values=TaskValues({}, reasons), missing_task_values=list(keys))
    captured = _probe_output(probe)
    if isinstance(captured, str):
        reasons = {key: f"Value {key!r} of task {name!r} was not captured: {captured}" for key in keys}
        return replace(target, task_values=TaskValues({}, reasons), missing_task_values=list(keys))
    values, missing = captured
    reasons = {key: f"Task {name!r} never set value {key!r}" for key in missing}
    return replace(target, task_values=TaskValues(values, reasons), missing_task_values=missing)


def _probe_output(probe: TaskRun | None) -> tuple[dict[str, object], list[str]] | str:
    # The captured values and the keys the task never set, or a string saying why nothing was captured.
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


def _result_state(task: jobs.RunTask) -> str | None:
    return _enum(task.state.result_state if task.state else None)


def _ran(result_state: str | None) -> bool:
    # A task with no result state did not run: the platform reports none for the tasks of a skipped run.
    return result_state is not None and result_state not in NOT_RUN


def _enum(value: Enum | str | None) -> str | None:
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
