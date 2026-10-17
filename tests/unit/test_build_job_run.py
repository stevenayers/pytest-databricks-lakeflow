from __future__ import annotations

from datetime import timedelta
from typing import Any, cast

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service import jobs

from pytest_databricks_lakeflow.results import JobRun, build_job_run

from .fakes import FakeWorkspaceClient, make_run, recorded

CAPTURE = {"transform": ["stage", "rows_written"]}


def build_from(ws: FakeWorkspaceClient, run: jobs.Run, **kwargs: Any) -> JobRun:
    return build_job_run(cast(WorkspaceClient, ws), run, job_key="my_job", job_id=1, **kwargs)


def build(name: str, **kwargs: Any) -> tuple[JobRun, FakeWorkspaceClient]:
    ws = FakeWorkspaceClient()
    run, outputs = recorded(name)
    ws.jobs.outputs.update(outputs)
    return build_from(ws, run, **kwargs), ws


def test_failed_task_with_probe() -> None:
    run, _ = build("probe_failure", capture_values=CAPTURE)
    assert run.failed
    assert (run.result_state, run.termination_code) == ("SUCCESS_WITH_FAILURES", "SUCCESS_WITH_FAILURES")
    assert run.duration == timedelta(milliseconds=60_355)
    assert sorted(run.tasks) == ["extract", "transform"]
    assert sorted(run.injected_tasks) == ["__pytest_probe__transform"]
    transform = run.task("transform")
    assert (transform.attempt, transform.run_id) == (1, 1054812578975269)
    assert transform.termination_code == "RUN_EXECUTION_ERROR"
    assert transform.raised(ValueError)
    assert transform.error is not None
    assert transform.error.message == "validation failed for explode"
    assert transform.values == {"stage": "validation"}
    assert transform.values["stage"] == "validation"
    assert transform.missing_values == ["rows_written"]
    with pytest.raises(KeyError, match="Task 'transform' never set value 'rows_written'"):
        transform.values["rows_written"]
    assert run.task("extract").output == '{"task": "extract", "marker": "extract"}'
    assert run.task("extract").values == {}


def test_successful_run_with_probe() -> None:
    run, _ = build("probe_success", capture_values=CAPTURE)
    assert run.succeeded
    transform = run.task("transform")
    assert transform.values == {"stage": "done", "rows_written": 7}
    assert transform.missing_values == []
    assert transform.error is None
    assert transform.output is not None and '"greeting": "hello world"' in transform.output


def test_values_of_a_task_that_did_not_run() -> None:
    run, _ = build("probe_upstream_failed", capture_values={**CAPTURE, "load": ["rows_written"]})
    load = run.task("load")
    assert (load.result_state, load.termination_code, load.ran) == ("UPSTREAM_FAILED", "SKIPPED", False)
    assert load.error is None
    assert load.missing_values == ["rows_written"]
    with pytest.raises(KeyError, match=r"Task 'load' did not run \(UPSTREAM_FAILED\), so value 'rows_written'"):
        load.values["rows_written"]
    assert run.task("transform").values["stage"] == "validation"


def test_failed_parent() -> None:
    run, _ = build("failed_no_probe_leaf")
    assert run.failed
    assert (run.result_state, run.termination_code) == ("FAILED", "RUN_EXECUTION_ERROR")
    assert "transform: ValueError: validation failed for explode" in repr(run)
    assert "load: UPSTREAM_FAILED" in repr(run)


def test_tasks_left_out_by_only_are_skipped() -> None:
    run, ws = build("only_plus_transform")
    assert run.succeeded
    assert "load" not in run.tasks
    assert sorted(run.skipped_tasks) == ["load"]
    assert run.skipped_tasks["load"].result_state == "DISABLED"
    assert 378690881497327 not in ws.jobs.output_calls
    with pytest.raises(KeyError, match="Skipped by only"):
        run.task("load")


def test_logs_of_wheel_and_script_tasks() -> None:
    run, _ = build("wheel_logs")
    assert "scr_a" in run.task("scr_a").logs
    assert run.task("scr_a").log_lines
    assert all(line.strip() for line in run.task("same_a").log_lines)
    assert run.task("extract").logs == ""


def test_cancelled_run() -> None:
    run, _ = build("cancelled")
    task = run.task("my_wheel_task")
    assert (run.result_state, run.termination_code) == ("CANCELED", "USER_CANCELED")
    assert (task.result_state, task.ran, task.error) == ("CANCELED", False, None)
    assert run.failed


def test_stubs_count_as_injected() -> None:
    run, _ = build("probe_success", stubs={"extract"})
    assert sorted(run.tasks) == ["transform"]
    assert sorted(run.injected_tasks) == ["__pytest_probe__transform", "extract"]


def test_values_when_the_probe_is_missing_or_fails() -> None:
    ws = FakeWorkspaceClient()
    run = make_run(1, {"transform": "SUCCESS", "__pytest_probe__transform": "FAILED", "other": "SUCCESS"})
    assert run.tasks is not None
    ws.jobs.outputs[run.tasks[1].run_id or 0] = jobs.RunOutput(error="PermissionError: cannot read {values}")
    result = build_from(ws, run, capture_values={"transform": ["k"], "other": ["k"]})
    with pytest.raises(KeyError, match=r"probe task __pytest_probe__transform ended FAILED: PermissionError"):
        result.task("transform").values["k"]
    with pytest.raises(KeyError, match="its probe task was not part of the run"):
        result.task("other").values["k"]


def test_unreadable_probe_output() -> None:
    ws = FakeWorkspaceClient()
    run = make_run(1, {"transform": "SUCCESS", "__pytest_probe__transform": "SUCCESS"})
    assert run.tasks is not None
    notebook = jobs.NotebookOutput(result='{"values": {"k": "', truncated=True)
    ws.jobs.outputs[run.tasks[1].run_id or 0] = jobs.RunOutput(notebook_output=notebook)
    result = build_from(ws, run, capture_values={"transform": ["k"]})
    with pytest.raises(KeyError, match=r"returned unreadable output \(truncated\)"):
        result.task("transform").values["k"]


def test_output_errors_do_not_hide_the_run(caplog: pytest.LogCaptureFixture) -> None:
    ws = FakeWorkspaceClient()
    run = make_run(1, {"a": "SUCCESS"})
    assert run.tasks is not None
    ws.jobs.outputs[run.tasks[0].run_id or 0] = NotFound("gone")
    result = build_from(ws, run)
    assert result.succeeded
    assert result.task("a").output is None
    assert "Could not read the output of task a" in caplog.text


def test_untaken_branches_do_not_fail_the_run() -> None:
    ws = FakeWorkspaceClient()
    run = make_run(1, {"check": "SUCCESS", "on_true": "SUCCESS", "on_false": "EXCLUDED"})
    result = build_from(ws, run)
    assert result.succeeded
    assert result.task("on_false").result_state == "EXCLUDED"
    assert repr(result).endswith("failing=[])")
