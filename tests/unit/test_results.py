from __future__ import annotations

import json
from dataclasses import replace
from datetime import timedelta
from typing import Any

import pytest
from databricks.sdk.service import jobs

from pytest_databricks_lakeflow.errors import LakeflowError, LakeflowRunFailed
from pytest_databricks_lakeflow.results import JobRun, TaskError, TaskRun, TaskValues, Timing

from .fakes import DATA

SAMPLES: list[dict[str, Any]] = json.loads((DATA / "task_errors.json").read_text())


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


@pytest.mark.parametrize("sample", SAMPLES, ids=[s["name"] for s in SAMPLES])
def test_task_error_parses_recorded_outputs(sample: dict[str, Any]) -> None:
    error = TaskError.from_output(sample["error"], sample["error_trace"])
    assert error is not None
    assert error.type == sample["type"]
    assert error.message.startswith(sample["message"])
    assert "\x1b[" not in error.trace


def test_task_error_none_without_output() -> None:
    assert TaskError.from_output(None, None) is None
    assert TaskError.from_output("", "") is None


def test_task_error_without_exception_type() -> None:
    message = "Run result unavailable: The task was skipped because not all its dependencies were executed."
    error = TaskError.from_output(message, None)
    assert error == TaskError(type=None, message=message, trace="")


def test_task_error_falls_back_to_trace() -> None:
    trace = "Traceback (most recent call last):\n  File 'x.py', line 1\n\x1b[0;31mKeyError\x1b[0m: 'k'\n"
    error = TaskError.from_output(None, trace)
    assert error is not None
    assert (error.type, error.message) == ("KeyError", "'k'")


def test_task_error_drops_java_frames_from_message() -> None:
    error = TaskError.from_output("Py4JJavaError: boom\n: java.lang.X: y\n\tat a.b.C(C.java:1)\nmore", None)
    assert error is not None
    assert error.message == "boom\n: java.lang.X: y\nmore"


def test_raised_compares_class_name() -> None:
    run = task("t", "FAILED", error=TaskError(type="json.decoder.JSONDecodeError", message="bad", trace=""))
    assert run.raised(json.JSONDecodeError)
    assert not run.raised(ValueError)
    assert not task("t").raised(Exception)
    assert not task("t", "FAILED", error=TaskError(type=None, message="x", trace="")).raised(Exception)


def test_task_values_explain_missing_keys() -> None:
    values = TaskValues({"a": 1}, {"b": "Task 't' never set value 'b'"})
    assert values["a"] == 1
    with pytest.raises(KeyError, match="never set value 'b'"):
        values["b"]
    with pytest.raises(KeyError) as excinfo:
        values["c"]
    assert excinfo.value.args == ("c",)
    assert values == {"a": 1}


def test_log_lines_drop_blank_lines() -> None:
    assert task("t", logs="one\n\n\ntwo\n  \n").log_lines == ["one", "two"]


def test_job_run_succeeds_only_when_every_user_task_succeeds() -> None:
    assert job_run([task("a"), task("b")]).succeeded
    assert job_run([task("a"), task("b", "FAILED")]).failed
    assert job_run([task("a")], skipped=[task("b", "DISABLED")]).succeeded
    assert job_run([]).failed


def test_job_run_task_lists_available_keys() -> None:
    run = job_run([task("a")], skipped=[task("b", "DISABLED")])
    assert run.task("a").task_key == "a"
    with pytest.raises(KeyError, match=r"No task 'b' in run. Available: \['a'\]. Skipped by only: \['b'\]"):
        run.task("b")
    with pytest.raises(KeyError, match=r"No task 'c' in run. Available: \['a'\]") as excinfo:
        run.task("c")
    assert "Skipped" not in str(excinfo.value)


def test_reprs_are_short_and_name_failures() -> None:
    error = TaskError(type="PermissionError", message="denied\nsecond line", trace="long trace")
    run = job_run([task("a"), task("b", "FAILED", error=error, logs="x" * 10_000), task("c", "UPSTREAM_FAILED")])
    failing = "failing=['b: PermissionError: denied', 'c: UPSTREAM_FAILED']"
    assert repr(run) == f"JobRun('my_job', SUCCESS, https://example.com/run, {failing})"
    assert repr(run.task("b")) == "TaskRun('b', FAILED, error='PermissionError: denied', https://example.com/b)"


def test_failed_reprs_show_parameters() -> None:
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


def test_successful_reprs_hide_parameters() -> None:
    ok = task("a", parameters={"date": "2026-10-01"})
    run = job_run([ok], parameters={"date": "2026-10-01"})
    assert repr(run) == "JobRun('my_job', SUCCESS, https://example.com/run, failing=[])"
    assert repr(ok) == "TaskRun('a', SUCCESS, https://example.com/a)"
    assert "params" not in repr(task("b", "UPSTREAM_FAILED", parameters={"date": "x"}))
    assert "params" not in repr(job_run([task("c", "FAILED", error=TaskError(None, "boom", ""))]))


def test_did_not_run_states() -> None:
    assert task("a", "SUCCESS").ran
    assert task("a", "FAILED").ran
    assert not task("a", "UPSTREAM_FAILED").ran
    assert not task("a", "DISABLED").ran


PY4J_TRACE = """Py4JJavaError: An error occurred while calling o123.save.
: org.apache.spark.SparkException: Job aborted.
\tat org.apache.spark.sql.execution.datasources.FileFormatWriter$.write(FileFormatWriter.scala:251)
Caused by: org.apache.spark.SparkException: Task failed while writing rows.
\tat org.apache.spark.sql.execution.datasources.FileFormatWriter$.executeTask(FileFormatWriter.scala:310)
\t... 12 more
  Caused by: java.io.FileNotFoundException: /mnt/data/part-0001 (No such file or directory)
\tat java.io.FileInputStream.open0(Native Method)
"""


def test_excluded_property() -> None:
    excluded = task("branch", "EXCLUDED")
    assert excluded.excluded
    assert not excluded.disabled
    assert not task("other", "DISABLED").excluded
    assert not task("load").excluded


def test_root_cause_is_the_last_caused_by_line() -> None:
    error = TaskError(type="Py4JJavaError", message="An error occurred while calling o123.save.", trace=PY4J_TRACE)
    assert error.root_cause == "java.io.FileNotFoundException: /mnt/data/part-0001 (No such file or directory)"


@pytest.mark.parametrize("sample", SAMPLES, ids=[s["name"] for s in SAMPLES])
def test_root_cause_is_none_without_caused_by(sample: dict[str, Any]) -> None:
    error = TaskError.from_output(sample["error"], sample["error_trace"])
    assert error is not None
    assert error.root_cause is None


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
def test_code_reads_the_spark_error_class(message: str, trace: str, code: str | None) -> None:
    assert TaskError(type="AnalysisException", message=message, trace=trace).code == code


@pytest.mark.parametrize(
    "sample",
    [s for s in SAMPLES if s["name"].split(".")[0] in {"script_permission", "whl_custom", "whl_permission"}],
    ids=lambda s: s["name"],
)
def test_code_ignores_lowercase_tags(sample: dict[str, Any]) -> None:
    error = TaskError.from_output(sample["error"], sample["error_trace"])
    assert error is not None
    assert error.message.startswith("[")
    assert error.code is None


def test_raise_for_failure_returns_the_run_when_it_succeeded() -> None:
    run = job_run([task("a"), task("b")])
    assert run.raise_for_failure() is run


def test_raise_for_failure_raises_with_every_failing_task() -> None:
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


def test_output_json_parses_exit_value() -> None:
    assert task("t", output='{"rows": 7, "ok": true}').output_json == {"rows": 7, "ok": True}


def test_output_json_without_exit_call() -> None:
    with pytest.raises(ValueError, match=r"Task 't' has no output: dbutils\.notebook\.exit\(\) was not called"):
        _ = task("t").output_json


def test_output_json_when_not_json() -> None:
    with pytest.raises(ValueError, match="Output of task 't' is not JSON: 'not json'"):
        _ = task("t", output="not json").output_json
    with pytest.raises(ValueError, match="Output of task 't' is not JSON") as excinfo:
        _ = task("t", output="x" * 1000).output_json
    assert "x" * 201 not in str(excinfo.value)


def test_output_json_when_truncated() -> None:
    with pytest.raises(ValueError, match="Output of task 't' was cut at 5 MB"):
        _ = task("t", output='{"a": 1}', output_truncated=True).output_json


@pytest.mark.parametrize("state", ["FAILED", "SUCCESS", "CANCELED", None])
def test_timed_out_is_false_for_other_states(state: str | None) -> None:
    assert not task("a", state).timed_out
    assert not job_run([task("a", state)]).timed_out


def test_timed_out_task() -> None:
    timed_out = task("a", "TIMEDOUT")
    assert timed_out.timed_out
    assert timed_out.ran
    assert job_run([timed_out]).timed_out
    assert replace(job_run([task("a")]), result_state="TIMEDOUT").timed_out


def test_attempts_default_to_empty() -> None:
    assert task("a").attempts == []


def test_timing_total() -> None:
    timing = Timing(timedelta(seconds=1), timedelta(seconds=2), timedelta(seconds=3), timedelta(seconds=4))
    assert timing.total == timedelta(seconds=10)


def test_timing_from_a_task_converts_milliseconds() -> None:
    task = jobs.RunTask(
        task_key="t", queue_duration=500, setup_duration=1_000, execution_duration=2_500, cleanup_duration=None
    )
    timing = Timing.from_run(task)
    assert timing == Timing(timedelta(milliseconds=500), timedelta(seconds=1), timedelta(seconds=2.5), timedelta(0))
    assert timing.total == timedelta(seconds=4)
