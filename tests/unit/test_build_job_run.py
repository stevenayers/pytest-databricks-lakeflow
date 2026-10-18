from __future__ import annotations

import json
import warnings
from collections.abc import Callable
from dataclasses import replace
from datetime import timedelta
from typing import Any, cast

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service import jobs

from pytest_databricks_lakeflow.results import JobRun, TaskRun, Timing, build_job_run

from .fakes import DATA, FakeWorkspaceClient, make_run, recorded

CAPTURE = {"transform": ["stage", "rows_written"]}


def build_from(ws: FakeWorkspaceClient, run: jobs.Run, **kwargs: Any) -> JobRun:
    return build_job_run(cast(WorkspaceClient, ws), run, job_key="my_job", job_id=1, **kwargs)


def build(name: str, **kwargs: Any) -> tuple[JobRun, FakeWorkspaceClient]:
    ws = FakeWorkspaceClient()
    run, outputs = recorded(name)
    ws.jobs.outputs.update(outputs)
    return build_from(ws, run, **kwargs), ws


NEW_ALERT_TASK = pytest.mark.skipif(
    not hasattr(jobs, "AlertTaskOutput"), reason="this databricks-sdk has no alert task output"
)


def _built_task(output: jobs.RunOutput, key: str = "t") -> TaskRun:
    ws = FakeWorkspaceClient()
    run = make_run(1, {key: "SUCCESS"})
    ws.jobs.outputs[100] = output
    return build_from(ws, run).task(key)


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


def test_disabled_in_the_template_is_not_skipped_by_only() -> None:
    ws = FakeWorkspaceClient()
    run = build_from(ws, make_run(1, {"a": "SUCCESS", "b": "DISABLED", "c": "DISABLED"}), disabled={"c"})
    assert run.succeeded
    assert sorted(run.tasks) == ["a"]
    assert sorted(run.skipped_tasks) == ["b"]
    assert sorted(run.disabled_tasks) == ["c"]
    assert run.disabled_tasks["c"].disabled
    with pytest.raises(KeyError, match=r"Disabled in the job: \['c'\]") as raised:
        run.task("c")
    assert "Skipped by only" not in str(raised.value)
    with pytest.raises(KeyError, match=r"Skipped by only: \['b'\]") as raised:
        run.task("b")
    assert "Disabled in the job" not in str(raised.value)


def test_only_skipped_task_is_still_skipped_by_only() -> None:
    run, _ = build("only_plus_transform")
    assert sorted(run.skipped_tasks) == ["load"]
    assert run.disabled_tasks == {}
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
    assert result.task("on_false").excluded
    assert not result.task("on_true").excluded
    assert repr(result).endswith("failing=[])")


def test_condition_outcome_and_branch_helpers() -> None:
    ws = FakeWorkspaceClient()
    states = {"check": "SUCCESS", "on_true": "SUCCESS", "on_false": "EXCLUDED"}
    run = build_from(ws, make_run(1, states, condition_outcomes={"check": "true"}))
    assert run.task("check").condition_outcome is True
    assert run.task("on_true").condition_outcome is None
    assert run.branch_taken("check") is True
    assert run.task("on_false").excluded
    assert run.executed_tasks == ["check", "on_true"]
    assert run.succeeded
    false = build_from(ws, make_run(2, {"check": "SUCCESS"}, condition_outcomes={"check": "false"}))
    assert false.task("check").condition_outcome is False
    assert false.branch_taken("check") is False


def test_condition_without_a_recognised_outcome() -> None:
    ws = FakeWorkspaceClient()
    run = build_from(ws, make_run(1, {"check": "SUCCESS"}, condition_outcomes={"check": ""}))
    assert run.task("check").condition_outcome is None


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


def _recorded_task(data: dict[str, Any], key: str) -> dict[str, Any]:
    return next(t for t in data["tasks"] if t["task_key"] == key)


def test_timing_breakdown_from_a_recorded_run() -> None:
    data = json.loads((DATA / "runs" / "probe_success.json").read_text())["run"]
    run, _ = build("probe_success", capture_task_values=CAPTURE)
    assert "queue_duration" not in data
    # probe_success has several tasks, so its parent run reports zero for setup, execution and cleanup. The
    # comparisons below check those zeros; test_timing_on_a_single_task_parent_run covers non-zero parent values.
    assert run.timing.queue == timedelta(0)
    assert run.timing.setup == timedelta(milliseconds=data["setup_duration"])
    assert run.timing.execution == timedelta(milliseconds=data["execution_duration"])
    assert run.timing.cleanup == timedelta(milliseconds=data["cleanup_duration"])
    assert run.effective_performance_target == data["effective_performance_target"]
    transform = _recorded_task(data, "transform")
    assert run.task("transform").timing.execution == timedelta(milliseconds=transform["execution_duration"])
    assert run.task("transform").timing.setup == timedelta(milliseconds=transform["setup_duration"])
    assert run.task("transform").effective_performance_target == transform["effective_performance_target"]
    assert run.task("transform").timing.total == sum(
        (timedelta(milliseconds=transform[f"{k}_duration"]) for k in ("setup", "execution", "cleanup")), timedelta(0)
    )


def test_timing_on_a_single_task_parent_run() -> None:
    data = json.loads((DATA / "runs" / "cancelled.json").read_text())["run"]
    assert data["setup_duration"] and data["execution_duration"]
    run, _ = build("cancelled")
    assert run.timing.setup == timedelta(milliseconds=data["setup_duration"])
    assert run.timing.execution == timedelta(milliseconds=data["execution_duration"])
    assert run.timing.cleanup == timedelta(milliseconds=data["cleanup_duration"])
    assert run.timing.total == timedelta(milliseconds=data["setup_duration"] + data["execution_duration"])
    assert run.effective_performance_target == data["effective_performance_target"]


def test_timing_defaults_to_zero_without_fields() -> None:
    run = build_from(FakeWorkspaceClient(), make_run(1, {"t": "SUCCESS"}))
    zero = Timing(timedelta(0), timedelta(0), timedelta(0), timedelta(0))
    assert run.timing == zero
    assert run.effective_performance_target is None
    assert run.task("t").timing == zero
    assert run.task("t").effective_performance_target is None


def test_effective_performance_target_on_run_and_task() -> None:
    data = json.loads((DATA / "runs" / "wheel_logs.json").read_text())["run"]
    run, _ = build("wheel_logs")
    assert run.effective_performance_target == data["effective_performance_target"]
    for key, task in run.tasks.items():
        assert task.effective_performance_target == _recorded_task(data, key)["effective_performance_target"]


def test_timing_leaves_duration_and_repr_alone() -> None:
    run, _ = build("probe_failure", capture_task_values=CAPTURE)
    assert run.duration == timedelta(milliseconds=60_355)
    assert "timing" not in repr(run)
    assert "timing" not in repr(run.task("transform"))
    assert "performance" not in repr(run) + repr(run.task("transform"))


def test_build_job_run_reuses_given_outputs() -> None:
    ws = FakeWorkspaceClient()
    run, outputs = recorded("probe_success")
    ws.jobs.outputs.update(outputs)
    extract = next(t for t in run.tasks or [] if t.task_key == "extract")
    given = jobs.RunOutput(notebook_output=jobs.NotebookOutput(result="fetched while polling"))
    result = build_from(ws, run, outputs={extract.run_id or 0: given}, capture_task_values=CAPTURE)
    assert result.task("extract").output == "fetched while polling"
    assert result.task("transform").task_values == {"stage": "done", "rows_written": 7}
    assert sorted(ws.jobs.output_calls) == sorted(set(outputs) - {extract.run_id})


def test_sql_query_output_is_exposed() -> None:
    query = jobs.SqlQueryOutput(warehouse_id="w1", output_link="https://x/q")
    task = _built_task(jobs.RunOutput(sql_output=jobs.SqlOutput(query_output=query)), "daily_query")
    assert task.sql_output is not None and task.sql_output.query_output is not None
    assert task.sql_output.query_output.warehouse_id == "w1"
    assert task.sql_output.query_output.output_link == "https://x/q"
    assert task.alert_state is None


@pytest.mark.parametrize(
    "output",
    [
        pytest.param(
            lambda: jobs.RunOutput(alert_output=jobs.AlertTaskOutput(alert_state=jobs.AlertEvaluationState.TRIGGERED)),
            marks=NEW_ALERT_TASK,
            id="alert-task",
        ),
        pytest.param(
            lambda: jobs.RunOutput(
                sql_output=jobs.SqlOutput(alert_output=jobs.SqlAlertOutput(alert_state=jobs.SqlAlertState.TRIGGERED))
            ),
            id="legacy-sql-alert",
        ),
    ],
)
def test_alert_state_from_new_and_legacy_alert_outputs(output: Callable[[], jobs.RunOutput]) -> None:
    assert _built_task(output(), "check").alert_state == "TRIGGERED"


def test_legacy_sql_alert_state_ok() -> None:
    alert = jobs.SqlAlertOutput(alert_state=jobs.SqlAlertState.OK)
    assert _built_task(jobs.RunOutput(sql_output=jobs.SqlOutput(alert_output=alert))).alert_state == "OK"


@NEW_ALERT_TASK
def test_alert_task_output_is_exposed() -> None:
    alert = jobs.AlertTaskOutput(alert_state=jobs.AlertEvaluationState.OK)
    task = _built_task(jobs.RunOutput(alert_output=alert), "check")
    assert task.alert_output == alert
    assert task.alert_state == "OK"


def test_run_job_and_dbt_outputs() -> None:
    trigger = _built_task(jobs.RunOutput(run_job_output=jobs.RunJobOutput(run_id=42)), "trigger")
    assert trigger.run_job_output is not None and trigger.run_job_output.run_id == 42
    dbt = jobs.DbtOutput(artifacts_link="https://x/artifacts", artifacts_headers={"h": "v"})
    models = _built_task(jobs.RunOutput(dbt_output=dbt), "models")
    assert models.dbt_output is not None and models.dbt_output.artifacts_link == "https://x/artifacts"


def test_dashboard_output_is_exposed() -> None:
    dashboard = jobs.DashboardTaskOutput(page_snapshots=[])
    assert _built_task(jobs.RunOutput(dashboard_output=dashboard)).dashboard_output == dashboard


def test_clean_room_notebook_output_fills_output() -> None:
    clean = jobs.CleanRoomsNotebookTaskCleanRoomsNotebookTaskOutput(
        notebook_output=jobs.NotebookOutput(result='{"n": 3}', truncated=False)
    )
    task = _built_task(jobs.RunOutput(clean_rooms_notebook_output=clean), "clean")
    assert task.clean_rooms_notebook_output == clean
    assert task.output == '{"n": 3}'
    assert task.output_json == {"n": 3}
    assert task.output_truncated is False


def test_top_level_notebook_output_wins_over_clean_room_output() -> None:
    clean = jobs.CleanRoomsNotebookTaskCleanRoomsNotebookTaskOutput(notebook_output=jobs.NotebookOutput(result="b"))
    output = jobs.RunOutput(notebook_output=jobs.NotebookOutput(result="a"), clean_rooms_notebook_output=clean)
    assert _built_task(output).output == "a"


def test_empty_top_level_notebook_output_falls_back_to_clean_room_output() -> None:
    output = jobs.RunOutput.from_dict(
        {"notebook_output": {}, "clean_rooms_notebook_output": {"notebook_output": {"result": "b", "truncated": True}}}
    )
    with pytest.warns(UserWarning, match="The output of task 't' was cut"):
        task = _built_task(output)
    assert task.output == "b"
    assert task.output_truncated is True


def test_truncated_clean_room_output() -> None:
    clean = jobs.CleanRoomsNotebookTaskCleanRoomsNotebookTaskOutput(
        notebook_output=jobs.NotebookOutput(result="x", truncated=True)
    )
    with pytest.warns(UserWarning, match="The output of task 'clean' was cut"):
        task = _built_task(jobs.RunOutput(clean_rooms_notebook_output=clean), "clean")
    assert task.output_truncated is True


def test_task_outputs_are_none_for_an_empty_run_output() -> None:
    task = _built_task(jobs.RunOutput())
    assert task.sql_output is None
    assert task.alert_output is None
    assert task.dbt_output is None
    assert task.run_job_output is None
    assert task.dashboard_output is None
    assert task.clean_rooms_notebook_output is None
    assert task.alert_state is None
    assert task.output is None


@pytest.mark.parametrize("name", sorted(p.stem for p in (DATA / "runs").glob("*.json")))
def test_recorded_runs_have_no_task_outputs(name: str) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        run, _ = build(name)
    for task in run.tasks.values():
        fields = (task.sql_output, task.alert_output, task.dbt_output, task.run_job_output, task.dashboard_output)
        assert fields == (None,) * 5
        assert task.clean_rooms_notebook_output is None
        assert task.alert_state is None


def test_child_run_id_of_a_run_job_task() -> None:
    ws = FakeWorkspaceClient()
    ws.jobs.outputs[100] = jobs.RunOutput(run_job_output=jobs.RunJobOutput(run_id=777))
    run = build_from(ws, make_run(1, {"trigger_child": "SUCCESS"}))
    assert run.task("trigger_child").child_run_id == 777


def test_child_run_id_is_none_for_other_tasks() -> None:
    run, _ = build("probe_success", capture_task_values=CAPTURE)
    assert all(task.child_run_id is None for task in run.tasks.values())


def test_child_run_id_is_none_when_the_platform_reports_no_run_id() -> None:
    assert _built_task(jobs.RunOutput(run_job_output=jobs.RunJobOutput())).child_run_id is None


def _with_child(job_id: int = 123, states: dict[str, str] | None = None) -> tuple[FakeWorkspaceClient, jobs.Run]:
    ws = FakeWorkspaceClient()
    ws.jobs.outputs[100] = jobs.RunOutput(run_job_output=jobs.RunJobOutput(run_id=777))
    child = make_run(777, states or {"load": "SUCCESS"})
    child.job_id = job_id
    ws.jobs.add_run(child)
    return ws, make_run(1, {"trigger_child": "SUCCESS"})


def test_child_run_of_a_run_job_task() -> None:
    ws, run = _with_child()
    child = build_from(ws, run, job_keys={123: "child"}).task("trigger_child").child_run
    assert child is not None
    assert (child.job_key, child.job_id, child.run_id) == ("child", 123, 777)
    assert sorted(child.tasks) == ["load"]
    assert child.task("load").succeeded
    assert child.succeeded
    assert [call["run_id"] for call in ws.jobs.get_run_calls] == [777]


def test_child_run_key_falls_back_to_the_job_id() -> None:
    ws, run = _with_child()
    child = build_from(ws, run).task("trigger_child").child_run
    assert child is not None
    assert (child.job_key, child.job_id) == ("123", 123)


def test_child_run_follows_the_parent_rules_for_failure() -> None:
    ws, run = _with_child(states={"load": "FAILED"})
    ws.jobs.outputs[77700] = jobs.RunOutput(error="boom")
    child = build_from(ws, run).task("trigger_child").child_run
    assert child is not None
    assert child.failed
    assert child.task("load").error is not None


def test_child_run_is_none_for_a_run_job_task_that_did_not_run() -> None:
    ws = FakeWorkspaceClient()
    ws.jobs.outputs[100] = jobs.RunOutput(run_job_output=jobs.RunJobOutput(run_id=777))
    run = build_from(ws, make_run(1, {"trigger_child": "UPSTREAM_FAILED"}))
    assert run.task("trigger_child").child_run is None
    assert ws.jobs.get_run_calls == []


def test_child_run_is_none_when_the_child_run_cannot_be_read(caplog: pytest.LogCaptureFixture) -> None:
    ws = FakeWorkspaceClient()
    ws.jobs.outputs[100] = jobs.RunOutput(run_job_output=jobs.RunJobOutput(run_id=777))
    task = build_from(ws, make_run(1, {"trigger_child": "SUCCESS"})).task("trigger_child")
    assert task.child_run_id == 777
    assert task.child_run is None
    assert "Could not read run 777 triggered by task trigger_child" in caplog.text


def test_child_run_is_none_for_other_tasks() -> None:
    run, ws = build("probe_success", capture_task_values=CAPTURE)
    assert all(task.child_run is None for task in run.tasks.values())
    assert all(task.child_run is None for task in run.injected_tasks.values())
    assert ws.jobs.get_run_calls == []


def test_child_run_of_a_child_is_nested() -> None:
    ws = FakeWorkspaceClient()
    ws.jobs.outputs[100] = jobs.RunOutput(run_job_output=jobs.RunJobOutput(run_id=777))
    ws.jobs.outputs[77700] = jobs.RunOutput(run_job_output=jobs.RunJobOutput(run_id=888))
    child = make_run(777, {"trigger_grandchild": "SUCCESS"})
    child.job_id = 123
    grandchild = make_run(888, {"load": "SUCCESS"})
    grandchild.job_id = 456
    ws.jobs.add_run(child)
    ws.jobs.add_run(grandchild)
    run = build_from(ws, make_run(1, {"trigger_child": "SUCCESS"}), job_keys={123: "child", 456: "grandchild"})
    first = run.task("trigger_child").child_run
    assert first is not None
    second = first.task("trigger_grandchild").child_run
    assert second is not None
    assert (second.job_key, second.job_id, second.run_id) == ("grandchild", 456, 888)
    assert second.task("load").succeeded


def test_attempts_of_a_run_job_task_end_with_the_child_run() -> None:
    ws, run = _with_child()
    task = build_from(ws, run).task("trigger_child")
    assert task.child_run is not None
    assert task.attempts[-1].child_run == task.child_run


def test_child_run_puts_disabled_child_tasks_in_disabled_tasks() -> None:
    ws, run = _with_child(states={"load": "SUCCESS", "backfill": "DISABLED"})
    child = build_from(ws, run).task("trigger_child").child_run
    assert child is not None
    assert sorted(child.disabled_tasks) == ["backfill"]
    assert child.skipped_tasks == {}
    with pytest.raises(KeyError, match="Disabled in the job"):
        child.task("backfill")


def test_every_attempt_of_a_run_job_task_has_its_child_run() -> None:
    ws = FakeWorkspaceClient()
    ws.jobs.outputs[100] = jobs.RunOutput(run_job_output=jobs.RunJobOutput(run_id=777))
    ws.jobs.outputs[101] = jobs.RunOutput(run_job_output=jobs.RunJobOutput(run_id=778))
    for child_id, state in ((777, "FAILED"), (778, "SUCCESS")):
        child = make_run(child_id, {"load": state})
        child.job_id = 123
        ws.jobs.add_run(child)
    run = make_run(1, {}, attempts={"trigger_child": ["FAILED", "SUCCESS"]})
    task = build_from(ws, run).task("trigger_child")
    first, last = task.attempts
    assert first.child_run is not None
    assert (first.child_run.run_id, first.child_run.failed) == (777, True)
    assert last.child_run is not None
    assert last.child_run.run_id == 778
    assert task.child_run == last.child_run
