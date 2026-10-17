from __future__ import annotations

import json
from datetime import timedelta
from typing import Any

import pytest

from pytest_databricks_lakeflow.results import JobRun, TaskError, TaskRun, TaskValues

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


def job_run(tasks: list[TaskRun], skipped: list[TaskRun] | None = None) -> JobRun:
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


def test_did_not_run_states() -> None:
    assert task("a", "SUCCESS").ran
    assert task("a", "FAILED").ran
    assert not task("a", "UPSTREAM_FAILED").ran
    assert not task("a", "DISABLED").ran
