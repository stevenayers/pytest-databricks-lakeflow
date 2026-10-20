from __future__ import annotations

import json
import warnings
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from typing import Any, cast

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, pipelines

from pytest_databricks_lakeflow.errors import LakeflowError, LakeflowRunFailed
from pytest_databricks_lakeflow.results import (
    JobRun,
    NotebookView,
    PipelineUpdate,
    SqlResult,
    TaskError,
    TaskRun,
    TaskValues,
    Timing,
    format_parameters,
    parse_notebook_export,
)

from .fakes import DATA, FakeWorkspaceClient, notebook_export

SAMPLES: list[dict[str, Any]] = json.loads((DATA / "task_errors.json").read_text())
FAILED_EXPORT = (DATA / "notebook_export_failed.html").read_text()
UNLINKED = [s for s in SAMPLES if not s["name"].endswith("_linked")]
LINKED = [s for s in SAMPLES if s["name"].endswith("_linked")]
LOWERCASE_TAGGED = [
    s for s in SAMPLES if s["name"].split(".")[0] in {"script_permission", "whl_custom", "whl_permission"}
]

PY4J_TRACE = """Py4JJavaError: An error occurred while calling o123.save.
: org.apache.spark.SparkException: Job aborted.
\tat org.apache.spark.sql.execution.datasources.FileFormatWriter$.write(FileFormatWriter.scala:251)
Caused by: org.apache.spark.SparkException: Task failed while writing rows.
\tat org.apache.spark.sql.execution.datasources.FileFormatWriter$.executeTask(FileFormatWriter.scala:310)
\t... 12 more
  Caused by: java.io.FileNotFoundException: /mnt/data/part-0001 (No such file or directory)
\tat java.io.FileInputStream.open0(Native Method)
"""
PY4J_ROOT_CAUSE = "java.io.FileNotFoundException: /mnt/data/part-0001 (No such file or directory)"

IMPLICIT_CHAIN_TRACE = """KeyError                                  Traceback (most recent call last)
File <command-1>, line 3
      2 try:
----> 3     {}["k"]
      4 except KeyError:

KeyError: 'k'

During handling of the above exception, another exception occurred:

ValueError                                Traceback (most recent call last)
File <command-1>, line 5
      4 except KeyError:
----> 5     raise ValueError("outer")

ValueError: outer
"""


# The `file_output` of the `sql` bundle's `types` task, as the workspace returned it: a NULL value is `{}`.
TYPES_FILE_OUTPUT = {
    "query_result": {
        "columns": [
            {"name": "n", "type": "int"},
            {"name": "m", "type": "null"},
            {"name": "d", "type": "decimal"},
            {"name": "day", "type": "date"},
            {"name": "b", "type": "boolean"},
        ],
        "rows": [
            {
                "values": [
                    {"string_value": "1"},
                    {},
                    {"string_value": "1.5"},
                    {"string_value": "2026-01-01"},
                    {"string_value": "true"},
                ]
            }
        ],
        "truncated": False,
    },
    "query_text": "SELECT 1 AS n, NULL AS m, 1.5 AS d, DATE '2026-01-01' AS day, true AS b\n",
    "sql_statements": [{"lookup_key": "CiQwMWYx"}],
    "warehouse_id": "w1",
}


HTML_OUTPUT = '<div class="ansiout">rows &gt; 0\nsecond\n</div>'


def task(key: str, state: str | None = "SUCCESS", **kwargs: Any) -> TaskRun:
    return TaskRun(
        task_key=key,
        run_id=1,
        result_state=state,
        termination_code=None,
        termination_message=None,
        run_url=f"https://example.com/{key}",
        duration=timedelta(seconds=1),
        **kwargs,
    )


def job_run(tasks: list[TaskRun], skipped: list[TaskRun] | None = None, **kwargs: Any) -> JobRun:
    return JobRun(
        job_key="my_job",
        job_id=1,
        run_id=2,
        run_url="https://example.com/run",
        result_state="SUCCESS",
        termination_code="SUCCESS",
        termination_message=None,
        duration=timedelta(seconds=3),
        tasks={t.task_key: t for t in tasks},
        injected_tasks={},
        skipped_tasks={t.task_key: t for t in skipped or []},
        **kwargs,
    )


def parse(sample: dict[str, Any]) -> TaskError:
    error = TaskError.from_output(sample["error"], sample["error_trace"])
    assert error is not None
    return error


class TestTaskErrorParsing:
    """TaskError.from_output reads the error and error_trace fields of a task output."""

    @pytest.mark.parametrize("sample", SAMPLES, ids=[s["name"] for s in SAMPLES])
    def test_task_error_parses_recorded_outputs(self, sample: dict[str, Any]) -> None:
        error = parse(sample)
        assert error.type == sample["type"]
        assert error.message.startswith(sample["message"])
        assert "\x1b[" not in error.trace

    def test_task_error_none_without_output(self) -> None:
        assert TaskError.from_output(None, None) is None
        assert TaskError.from_output("", "") is None

    def test_task_error_without_exception_type(self) -> None:
        message = "Run result unavailable: The task was skipped because not all its dependencies were executed."
        error = TaskError.from_output(message, None)
        assert error == TaskError(type=None, message=message, trace="")

    def test_task_error_falls_back_to_trace(self) -> None:
        trace = "Traceback (most recent call last):\n  File 'x.py', line 1\n\x1b[0;31mKeyError\x1b[0m: 'k'\n"
        error = TaskError.from_output(None, trace)
        assert error is not None
        assert (error.type, error.message) == ("KeyError", "'k'")

    def test_task_error_drops_java_frames_from_message(self) -> None:
        error = TaskError.from_output("Py4JJavaError: boom\n: java.lang.X: y\n\tat a.b.C(C.java:1)\nmore", None)
        assert error is not None
        assert error.message == "boom\n: java.lang.X: y\nmore"


class TestRootCause:
    """TaskError.root_cause reads the innermost failure from the trace."""

    def test_root_cause_is_the_last_caused_by_line(self) -> None:
        error = TaskError(type="Py4JJavaError", message="An error occurred while calling o123.save.", trace=PY4J_TRACE)
        assert error.root_cause == PY4J_ROOT_CAUSE

    @pytest.mark.parametrize("sample", UNLINKED, ids=[s["name"] for s in UNLINKED])
    def test_root_cause_is_none_without_caused_by(self, sample: dict[str, Any]) -> None:
        assert parse(sample).root_cause is None

    @pytest.mark.parametrize("sample", LINKED, ids=[s["name"] for s in LINKED])
    def test_root_cause_of_a_chained_python_exception(self, sample: dict[str, Any]) -> None:
        error = parse(sample)
        assert (error.type, error.message) == ("RuntimeError", "wrapped outer")
        assert error.root_cause == "ZeroDivisionError: division by zero"

    @pytest.mark.parametrize(
        ("trace", "expected"),
        [
            (IMPLICIT_CHAIN_TRACE, "KeyError: 'k'"),
            (IMPLICIT_CHAIN_TRACE.replace("\n", "\r\n"), "KeyError: 'k'"),
            (IMPLICIT_CHAIN_TRACE.replace("KeyError: 'k'", "KeyError: "), "KeyError"),
        ],
        ids=["lf", "crlf", "empty_message"],
    )
    def test_root_cause_of_an_implicit_chain(self, trace: str, expected: str) -> None:
        assert TaskError(type="ValueError", message="outer", trace=trace).root_cause == expected

    def test_root_cause_prefers_caused_by_inside_the_innermost_segment(self) -> None:
        trace = (
            f"{PY4J_TRACE}\nThe above exception was the direct cause of the following exception:\n\n"
            "RuntimeError                              Traceback (most recent call last)\n"
            "Caused by: java.lang.IllegalStateException: not the root\n"
            "RuntimeError: save failed\n"
        )
        error = TaskError(type="RuntimeError", message="save failed", trace=trace)
        assert error.root_cause == PY4J_ROOT_CAUSE

    def test_root_cause_is_none_for_an_empty_trace(self) -> None:
        assert TaskError(type="RuntimeError", message="boom", trace="").root_cause is None


class TestErrorCode:
    """TaskError.code reads the bracketed Spark error class."""

    @pytest.mark.parametrize(
        ("message", "trace", "code"),
        [
            ("[TABLE_OR_VIEW_NOT_FOUND] The table x cannot be found. SQLSTATE: 42P01", "", "TABLE_OR_VIEW_NOT_FOUND"),
            ("[DELTA_INVALID_FORMAT.X] Incompatible format detected.", "", "DELTA_INVALID_FORMAT.X"),
            (
                "An error occurred while calling o1.sql.",
                "Caused by: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column cannot be resolved.",
                "UNRESOLVED_COLUMN.WITH_SUGGESTION",
            ),
            ("The table x cannot be found.", "", None),
        ],
        ids=["message", "sub_class", "root_cause", "none"],
    )
    def test_code_reads_the_spark_error_class(self, message: str, trace: str, code: str | None) -> None:
        assert TaskError(type="AnalysisException", message=message, trace=trace).code == code

    @pytest.mark.parametrize("sample", LOWERCASE_TAGGED, ids=[s["name"] for s in LOWERCASE_TAGGED])
    def test_code_ignores_lowercase_tags(self, sample: dict[str, Any]) -> None:
        error = parse(sample)
        assert error.message.startswith("[")
        assert error.code is None


STATE_PROPERTIES = ("succeeded", "timed_out", "excluded", "upstream_failed", "disabled", "cancelled")


class TestTaskRunStates:
    """The result state of a TaskRun drives its boolean properties."""

    @pytest.mark.parametrize(
        ("state", "flag", "ran"),
        [
            ("SUCCESS", "succeeded", True),
            ("FAILED", None, True),
            ("TIMEDOUT", "timed_out", True),
            ("EXCLUDED", "excluded", False),
            ("UPSTREAM_FAILED", "upstream_failed", False),
            ("DISABLED", "disabled", False),
            ("CANCELED", "cancelled", False),
            ("UPSTREAM_CANCELED", None, False),
            (None, None, False),
        ],
    )
    def test_state_properties(self, state: str | None, flag: str | None, ran: bool) -> None:
        run = task("t", state)
        assert {name: getattr(run, name) for name in STATE_PROPERTIES} == {
            name: name == flag for name in STATE_PROPERTIES
        }
        assert run.ran is ran

    def test_excluded_property(self) -> None:
        excluded = task("branch", "EXCLUDED")
        assert excluded.excluded
        assert not excluded.disabled
        assert not task("other", "DISABLED").excluded
        assert not task("load").excluded

    @pytest.mark.parametrize("state", ["FAILED", "SUCCESS", "CANCELED", None])
    def test_timed_out_is_false_for_other_states(self, state: str | None) -> None:
        assert not task("a", state).timed_out
        assert not job_run([task("a", state)]).timed_out

    def test_job_run_times_out_with_the_run_or_a_task(self) -> None:
        assert job_run([task("a", "TIMEDOUT")]).timed_out
        assert replace(job_run([task("a")]), result_state="TIMEDOUT").timed_out

    def test_raised_compares_class_name(self) -> None:
        run = task("t", "FAILED", error=TaskError(type="json.decoder.JSONDecodeError", message="bad", trace=""))
        assert run.raised(json.JSONDecodeError)
        assert not run.raised(ValueError)
        assert not task("t").raised(Exception)
        assert not task("t", "FAILED", error=TaskError(type=None, message="x", trace="")).raised(Exception)

    def test_log_lines_drop_blank_lines(self) -> None:
        assert task("t", logs="one\n\n\ntwo\n  \n").log_lines == ["one", "two"]

    def test_alert_state_is_none_without_alert_output(self) -> None:
        assert task("a").alert_state is None
        query = jobs.SqlOutput(query_output=jobs.SqlQueryOutput(warehouse_id="w1"))
        assert task("a", sql_output=query).alert_state is None
        assert task("a", sql_output=jobs.SqlOutput(alert_output=jobs.SqlAlertOutput())).alert_state is None

    def test_new_output_fields_default_to_none(self) -> None:
        t = task("a")
        outputs = (t.sql_output, t.alert_output, t.dbt_output, t.run_job_output, t.dashboard_output)
        assert outputs == (None,) * 5
        assert t.clean_rooms_notebook_output is None
        assert t.sql_result is None


class TestTaskValues:
    """TaskValues explains why a requested value is missing."""

    def test_task_values_explain_missing_keys(self) -> None:
        values = TaskValues({"a": 1}, {"b": "Task 't' never set value 'b'"})
        assert values["a"] == 1
        with pytest.raises(KeyError, match="never set value 'b'"):
            values["b"]
        with pytest.raises(KeyError) as excinfo:
            values["c"]
        assert excinfo.value.args == ("c",)
        assert values == {"a": 1}


class TestOutputJson:
    """TaskRun.output_json parses the notebook exit value and names the task when it cannot."""

    def test_output_json_parses_exit_value(self) -> None:
        assert task("t", output='{"rows": 7, "ok": true}').output_json == {"rows": 7, "ok": True}

    def test_output_json_without_exit_call(self) -> None:
        with pytest.raises(ValueError, match=r"Task 't' has no output: dbutils\.notebook\.exit\(\) was not called"):
            _ = task("t").output_json

    def test_output_json_when_not_json(self) -> None:
        with pytest.raises(ValueError, match="Output of task 't' is not JSON: 'not json'"):
            _ = task("t", output="not json").output_json
        with pytest.raises(ValueError, match="Output of task 't' is not JSON") as excinfo:
            _ = task("t", output="x" * 1000).output_json
        assert "x" * 201 not in str(excinfo.value)

    def test_output_json_when_truncated(self) -> None:
        with pytest.raises(ValueError, match="Output of task 't' was cut at 5 MB"):
            _ = task("t", output='{"a": 1}', output_truncated=True).output_json


class TestSqlResult:
    """SqlResult reads the query result of a SQL file task from the raw runs/get-output response."""

    def test_from_output_maps_missing_string_values_to_none(self) -> None:
        result = SqlResult.from_output("types", {"sql_output": {"file_output": TYPES_FILE_OUTPUT}})
        assert result is not None
        assert result.columns == ["n", "m", "d", "day", "b"]
        assert result.types == ["int", "null", "decimal", "date", "boolean"]
        assert result.rows == [{"n": "1", "m": None, "d": "1.5", "day": "2026-01-01", "b": "true"}]
        assert result.first_row["m"] is None
        assert result.warehouse_id == "w1"
        assert result.query_text == TYPES_FILE_OUTPUT["query_text"]

    @pytest.mark.parametrize(
        "output",
        [
            pytest.param({}, id="no-sql-output"),
            pytest.param({"sql_output": {"query_output": {}}}, id="saved-query"),
            pytest.param({"sql_output": {"file_output": {"warehouse_id": "w1"}}}, id="no-query-result"),
        ],
    )
    def test_from_output_is_none_without_a_query_result(self, output: dict[str, Any]) -> None:
        assert SqlResult.from_output("query", output) is None

    def test_first_row_raises_when_there_are_no_rows(self) -> None:
        empty = {"query_result": {"columns": [{"name": "n", "type": "int"}], "rows": [], "truncated": False}}
        result = SqlResult.from_output("query", {"sql_output": {"file_output": empty}})
        assert result is not None
        assert result.rows == []
        with pytest.raises(LakeflowError, match="Task 'query' returned no rows"):
            _ = result.first_row


class TestJobRun:
    """JobRun judges the run by the user's tasks and looks them up by key."""

    def test_job_run_succeeds_only_when_every_user_task_succeeds(self) -> None:
        assert job_run([task("a"), task("b")]).succeeded
        assert job_run([task("a"), task("b", "FAILED")]).failed
        assert job_run([task("a")], skipped=[task("b", "DISABLED")]).succeeded
        assert job_run([]).failed

    def test_job_run_task_lists_available_keys(self) -> None:
        run = job_run([task("a")], skipped=[task("b", "DISABLED")])
        assert run.task("a").task_key == "a"
        with pytest.raises(KeyError, match=r"No task 'b' in run. Available: \['a'\]. Skipped by only: \['b'\]"):
            run.task("b")
        with pytest.raises(KeyError, match=r"No task 'c' in run. Available: \['a'\]") as excinfo:
            run.task("c")
        assert "Skipped" not in str(excinfo.value)

    def test_branch_taken_rejects_non_condition_tasks(self) -> None:
        run = job_run([task("check", condition_outcome=True), task("notebook")])
        assert run.branch_taken("check") is True
        with pytest.raises(LakeflowError, match=r"Task 'notebook' is not a condition task.*result_state"):
            run.branch_taken("notebook")
        with pytest.raises(KeyError, match="No task 'nope'"):
            run.branch_taken("nope")

    def test_executed_tasks_are_the_sorted_tasks_that_ran(self) -> None:
        run = job_run([task("z"), task("a", "FAILED"), task("m", "EXCLUDED"), task("b", "UPSTREAM_FAILED")])
        assert run.executed_tasks == ["a", "z"]


class TestReprs:
    """The reprs of JobRun and TaskRun are one line and name the failures."""

    def test_reprs_are_short_and_name_failures(self) -> None:
        error = TaskError(type="PermissionError", message="denied\nsecond line", trace="long trace")
        run = job_run([task("a"), task("b", "FAILED", error=error, logs="x" * 10_000), task("c", "UPSTREAM_FAILED")])
        failing = "failing=['b: PermissionError: denied', 'c: UPSTREAM_FAILED']"
        assert repr(run) == f"JobRun('my_job', SUCCESS, https://example.com/run, {failing})"
        assert repr(run.task("b")) == "TaskRun('b', FAILED, error='PermissionError: denied', https://example.com/b)"

    def test_failed_reprs_show_parameters(self) -> None:
        error = TaskError(type="ValueError", message="bad date", trace="")
        failed = task("b", "FAILED", error=error, parameters={"date": "2026-10-01"})
        run = job_run([task("a"), failed], parameters={"date": "2026-10-01"})
        assert repr(run) == (
            "JobRun('my_job', SUCCESS, https://example.com/run, params={'date': '2026-10-01'}, "
            "failing=['b: ValueError: bad date'])"
        )
        assert repr(failed) == (
            "TaskRun('b', FAILED, error='ValueError: bad date', params={'date': '2026-10-01'}, https://example.com/b)"
        )
        jar = task("c", "FAILED", error=error, parameters=["--date", "2026-10-01"])
        assert "params=['--date', '2026-10-01']" in repr(jar)

    def test_successful_reprs_hide_parameters(self) -> None:
        ok = task("a", parameters={"date": "2026-10-01"})
        run = job_run([ok], parameters={"date": "2026-10-01"})
        assert repr(run) == "JobRun('my_job', SUCCESS, https://example.com/run, failing=[])"
        assert repr(ok) == "TaskRun('a', SUCCESS, https://example.com/a)"
        assert "params" not in repr(task("b", "UPSTREAM_FAILED", parameters={"date": "x"}))
        assert "params" not in repr(job_run([task("c", "FAILED", error=TaskError(None, "boom", ""))]))

    def test_outputs_are_left_out_of_the_repr(self) -> None:
        notebook = parse_notebook_export(FAILED_EXPORT)
        t = task("a", run_job_output=jobs.RunJobOutput(run_id=42), dbt_output=jobs.DbtOutput(artifacts_link="l"))
        t = replace(t, notebook=notebook)
        assert repr(t) == "TaskRun('a', SUCCESS, https://example.com/a)"
        assert repr(job_run([t])) == "JobRun('my_job', SUCCESS, https://example.com/run, failing=[])"
        assert repr(notebook) == "NotebookView(cells=4, failed_cell=2)"


class TestRaiseForFailure:
    """JobRun.raise_for_failure raises LakeflowRunFailed with one line per failing task."""

    def test_raise_for_failure_returns_the_run_when_it_succeeded(self) -> None:
        run = job_run([task("a"), task("b")])
        assert run.raise_for_failure() is run

    def test_raise_for_failure_raises_with_every_failing_task(self) -> None:
        error = TaskError(type="Py4JJavaError", message="An error occurred while calling o123.save.", trace=PY4J_TRACE)
        run = job_run([task("a"), task("b", "FAILED", error=error), task("c", "UPSTREAM_FAILED")])
        with pytest.raises(LakeflowRunFailed) as excinfo:
            run.raise_for_failure()
        assert isinstance(excinfo.value, LakeflowError)
        assert excinfo.value.run is run
        message = str(excinfo.value)
        assert message.startswith(repr(run))
        detail = message.removeprefix(repr(run))
        assert detail.count("\nb: ") == 1
        assert detail.count("\nc: ") == 1
        assert "\na: " not in detail
        assert f"b: {error}" in detail
        assert "root cause: java.io.FileNotFoundException: /mnt/data/part-0001" in detail
        assert "https://example.com/b" in detail
        assert "c: UPSTREAM_FAILED" in detail
        assert "https://example.com/c" in detail

    def test_raise_for_failure_prints_the_chained_root_cause(self) -> None:
        run = job_run([task("transform", "FAILED", error=parse(LINKED[0]))])
        with pytest.raises(LakeflowRunFailed, match=r"root cause: ZeroDivisionError: division by zero"):
            run.raise_for_failure()

    def test_raise_for_failure_lists_parameters(self) -> None:
        error = TaskError(type="ValueError", message="bad limit", trace="")
        failed = task("extract", "FAILED", error=error, parameters={"date": "2026-10-01", "limit": "10"})
        unknown = task("load", "FAILED", error=error, parameters=None)
        empty = task("publish", "FAILED", error=error, parameters={})
        injected = task("notify", "FAILED", error=error, parameters={"__pytest_task_key": "notify"})
        tasks = [failed, unknown, empty, injected, task("ok", parameters={"date": "2026-10-01"})]
        run = job_run(tasks, parameters={"date": "2026-10-01"})
        with pytest.raises(LakeflowRunFailed) as excinfo:
            run.raise_for_failure()
        lines = str(excinfo.value).splitlines()
        assert lines[0].startswith("JobRun('my_job', SUCCESS, https://example.com/run, failing=")
        assert lines[1] == 'params: {"date": "2026-10-01"}'
        assert lines[2].startswith("extract: ValueError: bad limit")
        assert lines[3] == '  params: {"date": "2026-10-01", "limit": "10"}'
        assert lines[4].startswith("load: ValueError: bad limit")
        assert lines[5].startswith("publish: ValueError: bad limit")
        assert lines[6].startswith("notify: ValueError: bad limit")
        assert len(lines) == 7


class TestFormatParameters:
    """format_parameters renders resolved parameters as one line of JSON."""

    def test_format_parameters_sorts_cuts_and_hides_reserved_keys(self) -> None:
        text = format_parameters({"b": "2", "a": "x" * 500})
        assert "\n" not in text
        assert text.index('"a"') < text.index('"b"')
        value = json.loads(text)["a"]
        assert len(value) == 200
        assert value.endswith("...")
        assert json.loads(format_parameters(["--rows", "10"])) == ["--rows", "10"]
        assert format_parameters(None) == ""
        assert format_parameters({}) == ""
        assert format_parameters([]) == ""
        assert format_parameters({"__pytest_task_key": "t", "limit": "10"}) == '{"limit": "10"}'


class TestTiming:
    """Timing sums its parts and reads millisecond durations from a run."""

    def test_timing_total(self) -> None:
        timing = Timing(timedelta(seconds=1), timedelta(seconds=2), timedelta(seconds=3), timedelta(seconds=4))
        assert timing.total == timedelta(seconds=10)

    def test_timing_from_a_task_converts_milliseconds(self) -> None:
        run_task = jobs.RunTask(
            task_key="t", queue_duration=500, setup_duration=1_000, execution_duration=2_500, cleanup_duration=None
        )
        timing = Timing.from_run(run_task)
        assert timing == Timing(timedelta(milliseconds=500), timedelta(seconds=1), timedelta(seconds=2.5), timedelta(0))
        assert timing.total == timedelta(seconds=4)


class TestNotebookExport:
    """parse_notebook_export reads the cells, printed text and failing cell of an exported notebook."""

    def test_parse_notebook_export_cells_and_stdout(self) -> None:
        # Recorded from the example bundle's transform task run with param1="forbidden".
        notebook = parse_notebook_export(FAILED_EXPORT)
        assert notebook.html == FAILED_EXPORT
        assert [cell.index for cell in notebook.cells] == [0, 1, 2, 3]
        assert notebook.cells[0].code.startswith("import time\n")
        assert notebook.cells[1].code.startswith("# The printed lines reach the test")
        printed = "transform: checking forbidden against /Volumes/example/raw/landing\ntransform: 7 rows expected\n"
        assert notebook.cells[1].output == printed
        assert [cell.output for cell in notebook.cells] == ["", printed, "", ""]
        assert notebook.stdout == printed

    def test_failed_cell_names_the_exception(self) -> None:
        notebook = parse_notebook_export(FAILED_EXPORT)
        failed = notebook.failed_cell
        assert failed is notebook.cells[2]
        assert failed.code.startswith('dbutils.jobs.taskValues.set(key="stage", value="validation")')
        assert failed.error == "PermissionError: forbidden may not read /Volumes/example/raw/landing"
        # The platform marks the cell after the failure as an error with the summary "Command skipped".
        assert notebook.cells[3].error is None
        assert [cell.error is None for cell in notebook.cells] == [True, True, False, True]

    def test_parse_handles_list_results_and_missing_results(self) -> None:
        commands = [
            {
                "position": 2.0,
                "command": "display(df)",
                "results": {
                    "type": "listResults",
                    "data": [
                        {"type": "html", "data": "<b>bold</b> &amp; more"},
                        {"type": "ansi", "name": "stdout", "data": "\x1b[31mred\x1b[39m <kept>\n"},
                    ],
                },
            },
            {"position": 1.0, "command": "print('rows')", "results": {"type": "html", "data": HTML_OUTPUT}},
            {"position": 3.0, "command": "x = 1"},
            {"position": 4.0, "command": "df.collect()", "results": {"type": "table", "data": [[1, "a"]]}},
            {
                "position": 5.0,
                "command": "raise KeyError('k')\nprint('never')",
                "results": {"type": "error", "data": '<span class="ansi-red-fg">KeyError</span>: &#39;k&#39;'},
            },
        ]
        notebook = parse_notebook_export(notebook_export(commands))
        assert [cell.code for cell in notebook.cells] == [
            "print('rows')",
            "display(df)",
            "x = 1",
            "df.collect()",
            "raise KeyError('k')\nprint('never')",
        ]
        outputs = ["rows > 0\nsecond\n", "bold & more\nred <kept>\n", "", "", ""]
        assert [cell.output for cell in notebook.cells] == outputs
        assert notebook.cells[0].output_html == HTML_OUTPUT
        assert notebook.stdout == "red <kept>\n"
        assert notebook.failed_cell is notebook.cells[4]
        assert notebook.failed_cell.error == "KeyError: 'k'"

    def test_stdout_keeps_only_the_stdout_stream(self) -> None:
        commands = [
            {
                "position": 1.0,
                "command": "print('a'); print('b', file=sys.stderr)",
                "results": {
                    "type": "listResults",
                    "data": [
                        {"type": "ansi", "name": "stdout", "data": "\x1b[32ma\x1b[39m\n"},
                        {"type": "ansi", "name": "stderr", "data": "b\n"},
                    ],
                },
            },
            {
                "position": 2.0,
                "command": "print('c')",
                "results": {
                    "type": "listResults",
                    "data": [
                        {"type": "ansi", "name": "stdout", "data": "c"},
                    ],
                },
            },
            {"position": 3.0, "command": "dbutils.notebook.exit('done')", "results": {"type": "raw", "data": "done"}},
        ]
        notebook = parse_notebook_export(notebook_export(commands))
        assert [cell.output for cell in notebook.cells] == ["a\nb\n", "c\n", "done\n"]
        assert [cell.stdout for cell in notebook.cells] == ["a\n", "c\n", ""]
        assert notebook.stdout == "a\nc\n"

    @pytest.mark.parametrize(
        ("summary", "trace", "expected"),
        [
            pytest.param("Command skipped", None, None, id="skipped"),
            pytest.param("Command skipped", "", None, id="skipped-empty-trace"),
            pytest.param("ValueError: bad input", None, "ValueError: bad input", id="summary-without-trace"),
            pytest.param(None, "Traceback\nKeyError: 'k'\n", "KeyError: 'k'", id="trace-without-summary"),
            pytest.param(None, None, None, id="no-error"),
        ],
    )
    def test_cell_error_decides_skipped_on_the_summary(
        self, summary: str | None, trace: str | None, expected: str | None
    ) -> None:
        command = {"position": 1.0, "command": "x", "results": None, "error": trace, "errorSummary": summary}
        notebook = parse_notebook_export(notebook_export([command]))
        assert notebook.cells[0].error == expected

    @pytest.mark.parametrize(
        "html",
        [
            pytest.param("<html><body>no model here</body></html>", id="missing"),
            pytest.param("<script>var __DATABRICKS_NOTEBOOK_MODEL = '!!not base64';</script>", id="unreadable"),
        ],
    )
    def test_parse_without_marker_warns_and_keeps_html(self, html: str) -> None:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            notebook = parse_notebook_export(html)
        assert (notebook.cells, notebook.html, notebook.stdout, notebook.failed_cell) == ([], html, "", None)
        assert len(caught) == 1
        assert caught[0].category is UserWarning
        assert "__DATABRICKS_NOTEBOOK_MODEL" in str(caught[0].message)

    def test_save_writes_the_exported_html(self, tmp_path: Path) -> None:
        notebook = NotebookView(html="<html>exported</html>")
        saved = notebook.save(tmp_path / "build" / "lakeflow" / "transform.html")
        assert saved == tmp_path / "build" / "lakeflow" / "transform.html"
        assert saved.read_text() == "<html>exported</html>"


def event(event_id: str, update_id: str | None, timestamp: str, level: str = "INFO") -> pipelines.PipelineEvent:
    origin = pipelines.Origin(pipeline_id="abc", update_id=update_id)
    return pipelines.PipelineEvent(id=event_id, origin=origin, timestamp=timestamp, level=pipelines.EventLevel(level))


def pipeline_update(ws: FakeWorkspaceClient) -> PipelineUpdate:
    # The task run started at 2026-10-19T19:00:27Z, a second before the platform created the update.
    return PipelineUpdate(
        pipeline_id="abc",
        update_id="mine",
        state="FAILED",
        cause="JOB_TASK",
        full_refresh=False,
        _ws=cast(WorkspaceClient, ws),
        _since=1_792_436_427_000,
    )


def fake_with_events() -> FakeWorkspaceClient:
    ws = FakeWorkspaceClient()
    # Newest first, as the platform returns them. "started" is logged a few milliseconds before the update's
    # creation time, which is why the lookup stops at the task run's start rather than at the update's creation.
    ws.pipelines.events["abc"] = [
        event("next", "later", "2026-10-19T19:05:00.000Z", "ERROR"),
        event("failed", "mine", "2026-10-19T19:00:54.285Z", "ERROR"),
        event("progress", "mine", "2026-10-19T19:00:50.019Z"),
        event("started", "mine", "2026-10-19T19:00:28.187Z"),
        event("created", None, "2026-10-19T18:00:00.000Z"),
        event("old", "earlier", "2026-10-19T17:00:00.000Z", "ERROR"),
    ]
    return ws


class TestPipelineUpdate:
    """PipelineUpdate reads the events its update logged, and only those."""

    def test_events_filter_by_level(self) -> None:
        ws = fake_with_events()
        errors = pipeline_update(ws).events(level="ERROR")
        assert [e.id for e in errors] == ["failed"]
        assert ws.pipelines.calls == [
            {"method": "list_pipeline_events", "pipeline_id": "abc", "filter": "level='ERROR'"}
        ]

    def test_events_without_a_level_pass_no_filter(self) -> None:
        ws = fake_with_events()
        assert [e.id for e in pipeline_update(ws).events()] == ["failed", "progress", "started"]
        assert ws.pipelines.calls == [{"method": "list_pipeline_events", "pipeline_id": "abc", "filter": None}]

    def test_events_stop_paging_before_the_task_started(self) -> None:
        ws = fake_with_events()
        pipeline_update(ws).events()
        assert ws.pipelines.read_events == ["next", "failed", "progress", "started", "created"]

    def test_from_info_pages_events_from_a_minute_before_the_update(self) -> None:
        ws = fake_with_events()
        # Created at 2026-10-19T19:00:28.194Z, 7 ms after its "started" event was logged.
        info = pipelines.UpdateInfo(
            pipeline_id="abc",
            update_id="mine",
            state=pipelines.UpdateInfoState.FAILED,
            cause=pipelines.UpdateInfoCause.API_CALL,
            full_refresh=True,
            creation_time=1_792_436_428_194,
        )
        update = PipelineUpdate.from_info(cast(WorkspaceClient, ws), info)
        assert repr(update) == (
            "PipelineUpdate(pipeline_id='abc', update_id='mine', state='FAILED', cause='API_CALL', full_refresh=True)"
        )
        assert [e.id for e in update.events()] == ["failed", "progress", "started"]
        assert ws.pipelines.read_events == ["next", "failed", "progress", "started", "created"]

    def test_workspace_client_stays_out_of_the_repr_and_equality(self) -> None:
        first = pipeline_update(FakeWorkspaceClient())
        assert first == pipeline_update(FakeWorkspaceClient())
        assert repr(first) == (
            "PipelineUpdate(pipeline_id='abc', update_id='mine', state='FAILED', cause='JOB_TASK', full_refresh=False)"
        )
