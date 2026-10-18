from __future__ import annotations

import warnings
from dataclasses import replace
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
    run, _ = build("probe_failure", capture_task_values=CAPTURE)
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
    assert transform.task_values == {"stage": "validation"}
    assert transform.task_values["stage"] == "validation"
    assert transform.missing_task_values == ["rows_written"]
    with pytest.raises(KeyError, match="Task 'transform' never set value 'rows_written'"):
        transform.task_values["rows_written"]
    assert run.task("extract").output == '{"task": "extract", "marker": "extract"}'
    assert run.task("extract").task_values == {}


def test_successful_run_with_probe() -> None:
    run, _ = build("probe_success", capture_task_values=CAPTURE)
    assert run.succeeded
    transform = run.task("transform")
    assert transform.task_values == {"stage": "done", "rows_written": 7}
    assert transform.missing_task_values == []
    assert transform.error is None
    assert transform.output is not None and '"greeting": "hello world"' in transform.output


def test_values_of_a_task_that_did_not_run() -> None:
    run, _ = build("probe_upstream_failed", capture_task_values={**CAPTURE, "load": ["rows_written"]})
    load = run.task("load")
    assert (load.result_state, load.termination_code, load.ran) == ("UPSTREAM_FAILED", "SKIPPED", False)
    assert load.error is None
    assert load.missing_task_values == ["rows_written"]
    with pytest.raises(KeyError, match=r"Task 'load' did not run \(UPSTREAM_FAILED\), so value 'rows_written'"):
        load.task_values["rows_written"]
    assert run.task("transform").task_values["stage"] == "validation"


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
    result = build_from(ws, run, capture_task_values={"transform": ["k"], "other": ["k"]})
    with pytest.raises(KeyError, match=r"probe task __pytest_probe__transform ended FAILED: PermissionError"):
        result.task("transform").task_values["k"]
    with pytest.raises(KeyError, match="its probe task was not part of the run"):
        result.task("other").task_values["k"]


def test_unreadable_probe_output() -> None:
    ws = FakeWorkspaceClient()
    run = make_run(1, {"transform": "SUCCESS", "__pytest_probe__transform": "SUCCESS"})
    assert run.tasks is not None
    notebook = jobs.NotebookOutput(result='{"values": {"k": "', truncated=True)
    ws.jobs.outputs[run.tasks[1].run_id or 0] = jobs.RunOutput(notebook_output=notebook)
    with pytest.warns(UserWarning, match="__pytest_probe__transform"):
        result = build_from(ws, run, capture_task_values={"transform": ["k"]})
    with pytest.raises(KeyError, match=r"returned unreadable output \(truncated\)"):
        result.task("transform").task_values["k"]


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


def test_output_json_of_a_recorded_run() -> None:
    run, _ = build("probe_failure")
    assert run.task("extract").output_json == {"task": "extract", "marker": "extract"}


def test_truncated_output_warns() -> None:
    ws = FakeWorkspaceClient()
    run = make_run(1, {"extract": "SUCCESS", "transform": "SUCCESS"})
    assert run.tasks is not None
    notebook = jobs.NotebookOutput(result="x", truncated=True)
    ws.jobs.outputs[run.tasks[1].run_id or 0] = jobs.RunOutput(notebook_output=notebook)
    with pytest.warns(UserWarning, match="transform") as record:
        result = build_from(ws, run)
    assert len(record) == 1
    assert "5 MB" in str(record[0].message)
    assert "extract" not in str(record[0].message)
    assert result.task("transform").output_truncated


def test_truncated_logs_warn() -> None:
    ws = FakeWorkspaceClient()
    run = make_run(1, {"my_wheel_task": "SUCCESS"})
    assert run.tasks is not None
    ws.jobs.outputs[run.tasks[0].run_id or 0] = jobs.RunOutput(logs="tail", logs_truncated=True)
    with pytest.warns(UserWarning, match="my_wheel_task.*5 MB") as record:
        build_from(ws, run)
    assert len(record) == 1


def test_truncated_output_and_logs_warn_once_per_task() -> None:
    ws = FakeWorkspaceClient()
    run = make_run(1, {"t": "SUCCESS"})
    assert run.tasks is not None
    notebook = jobs.NotebookOutput(result="x", truncated=True)
    ws.jobs.outputs[run.tasks[0].run_id or 0] = jobs.RunOutput(notebook_output=notebook, logs_truncated=True)
    with pytest.warns(UserWarning, match="The output and logs of task 't' were cut at the 5 MB limit") as record:
        build_from(ws, run)
    assert len(record) == 1


def test_no_warning_without_truncation() -> None:
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        build("probe_success", capture_task_values=CAPTURE)
    assert record == []


def test_job_parameters_are_resolved() -> None:
    run, _ = build("probe_failure")
    assert run.parameters == {"param1": "explode", "param2": "world", "mode": "full", "schema": ""}


def test_job_parameters_default_to_empty() -> None:
    ws = FakeWorkspaceClient()
    run = make_run(1, {"a": "SUCCESS"})
    assert run.job_parameters is None
    assert build_from(ws, run).parameters == {}


def test_task_parameters_from_resolved_values() -> None:
    ws = FakeWorkspaceClient()
    run = make_run(1, {"transform": "SUCCESS", "jar": "SUCCESS", "plain": "SUCCESS"})
    assert run.tasks is not None
    notebook = jobs.ResolvedNotebookTaskValues(base_parameters={"row_count": "7"})
    run.tasks[0].resolved_values = jobs.ResolvedValues(notebook_task=notebook)
    jar = jobs.ResolvedStringParamsValues(parameters=["--date", "2026-10-01"])
    run.tasks[1].resolved_values = jobs.ResolvedValues(spark_jar_task=jar)
    result = build_from(ws, run)
    assert result.task("transform").parameters == {"row_count": "7"}
    assert result.task("jar").parameters == ["--date", "2026-10-01"]
    assert result.task("plain").parameters is None


@pytest.mark.parametrize(
    ("resolved", "expected"),
    [
        (jobs.ResolvedValues(sql_task=jobs.ResolvedParamPairValues(parameters={"a": "1"})), {"a": "1"}),
        (
            jobs.ResolvedValues(python_wheel_task=jobs.ResolvedPythonWheelTaskValues(named_parameters={"b": "2"})),
            {"b": "2"},
        ),
        (jobs.ResolvedValues(python_wheel_task=jobs.ResolvedPythonWheelTaskValues(parameters=["x"])), ["x"]),
        (jobs.ResolvedValues(run_job_task=jobs.ResolvedRunJobTaskValues(job_parameters={"c": "3"})), {"c": "3"}),
        (jobs.ResolvedValues(spark_python_task=jobs.ResolvedStringParamsValues(parameters=["p"])), ["p"]),
        (jobs.ResolvedValues(spark_submit_task=jobs.ResolvedStringParamsValues(parameters=["s"])), ["s"]),
        (jobs.ResolvedValues(dbt_task=jobs.ResolvedDbtTaskValues(commands=["dbt run"])), ["dbt run"]),
        (jobs.ResolvedValues(condition_task=jobs.ResolvedConditionTaskValues(left="1", right="2")), None),
        (jobs.ResolvedValues(), None),
    ],
)
def test_task_parameters_of_each_task_type(resolved: jobs.ResolvedValues, expected: object) -> None:
    ws = FakeWorkspaceClient()
    run = make_run(1, {"t": "SUCCESS"})
    assert run.tasks is not None
    run.tasks[0].resolved_values = resolved
    assert build_from(ws, run).task("t").parameters == expected


def test_reserved_job_parameters_are_hidden() -> None:
    ws = FakeWorkspaceClient()
    run = make_run(1, {"a": "SUCCESS"})
    run.job_parameters = [
        jobs.JobParameter(name="__pytest_values", default="{}", value='{"k": 1}'),
        jobs.JobParameter(name="date", default="2026-01-01"),
        jobs.JobParameter(default="nameless"),
    ]
    parameters = build_from(ws, run).parameters
    assert parameters == {"date": "2026-01-01"}
    assert not any(key.startswith("__pytest_") for key in parameters)


def test_every_attempt_is_kept() -> None:
    run, ws = build("probe_failure", capture_task_values=CAPTURE)
    transform = run.task("transform")
    assert transform.attempt == 1
    assert [a.attempt for a in transform.attempts] == [0, 1]
    assert [a.run_id for a in transform.attempts] == [1103977916948693, 1054812578975269]
    assert all(a.attempts == [] for a in transform.attempts)
    assert transform.attempts[-1] == replace(transform, attempts=[])
    assert {1103977916948693, 1054812578975269} <= set(ws.jobs.output_calls)


def test_a_single_attempt_equals_the_task() -> None:
    run, _ = build("probe_failure", capture_task_values=CAPTURE)
    extract = run.task("extract")
    assert extract.attempts == [replace(extract, attempts=[])]


def test_attempts_from_make_run() -> None:
    ws = FakeWorkspaceClient()
    ws.jobs.outputs[902] = jobs.RunOutput(error="ValueError: flaky")
    run = build_from(ws, make_run(9, {"s": "SUCCESS"}, attempts={"t": ["FAILED", "FAILED", "SUCCESS"]}))
    t = run.task("t")
    assert t.succeeded
    assert (t.attempt, t.run_id) == (2, 903)
    assert [(a.attempt, a.result_state) for a in t.attempts] == [(0, "FAILED"), (1, "FAILED"), (2, "SUCCESS")]
    assert t.attempts[0].error is None
    assert t.attempts[1].raised(ValueError)
    assert sorted(ws.jobs.output_calls) == [900, 901, 902, 903]
    assert run.task("s").attempts == [replace(run.task("s"), attempts=[])]


def test_timed_out_task_and_run() -> None:
    ws = FakeWorkspaceClient()
    ws.jobs.outputs[900] = jobs.RunOutput(error="TimeoutError: too slow")
    run = build_from(ws, make_run(9, {"load": "TIMEDOUT"}, result_state="TIMEDOUT", code="RUN_EXECUTION_ERROR"))
    load = run.task("load")
    assert run.timed_out
    assert load.timed_out
    assert load.ran
    assert load.raised(TimeoutError)
    assert run.failed


def test_timed_out_task_without_error_in_repr() -> None:
    ws = FakeWorkspaceClient()
    run = build_from(ws, make_run(9, {"load": "TIMEDOUT"}, result_state="TIMEDOUT", code="RUN_EXECUTION_ERROR"))
    assert run.timed_out
    assert run.task("load").timed_out
    assert run.failed
    assert "load: TIMEDOUT" in repr(run)


def test_a_timed_out_task_times_out_the_run() -> None:
    ws = FakeWorkspaceClient()
    run = build_from(ws, make_run(9, {"load": "TIMEDOUT"}, result_state="FAILED", code="RUN_EXECUTION_ERROR"))
    assert run.timed_out


def test_a_failed_run_did_not_time_out() -> None:
    run, _ = build("probe_failure", capture_task_values=CAPTURE)
    assert not run.timed_out


def test_task_keys_keep_the_order_the_run_lists_them() -> None:
    run = make_run(9, {"a": "SUCCESS"}, attempts={"b": ["FAILED", "SUCCESS"]})
    a, b0, b1 = run.tasks or []
    run.tasks = [b1, a, b0]
    result = build_from(FakeWorkspaceClient(), run)
    assert list(result.tasks) == ["b", "a"]
    assert [t.attempt for t in result.task("b").attempts] == [0, 1]
