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

NEW_ALERT_TASK = pytest.mark.skipif(
    not hasattr(jobs, "AlertTaskOutput"), reason="this databricks-sdk has no alert task output"
)


def build_from(ws: FakeWorkspaceClient, run: jobs.Run, **kwargs: Any) -> JobRun:
    # Register the run so that the fake serves its task runs by their own ids, as the Jobs API does.
    ws.jobs.add_run(run)
    return build_job_run(cast(WorkspaceClient, ws), run, job_key="my_job", job_id=1, **kwargs)


def build(name: str, **kwargs: Any) -> tuple[JobRun, FakeWorkspaceClient]:
    """Build the recorded run `name` with its recorded task outputs."""
    ws = FakeWorkspaceClient()
    run, outputs = recorded(name)
    ws.jobs.outputs.update(outputs)
    return build_from(ws, run, **kwargs), ws


def run_id_of(run: jobs.Run, key: str, attempt: int = 0) -> int:
    """Return the task run id of attempt `attempt` of task `key` in `run`, so an output can be keyed by it."""
    for task in run.tasks or []:
        if task.task_key == key and task.attempt_number == attempt:
            assert task.run_id is not None
            return task.run_id
    raise AssertionError(f"run {run.run_id} has no attempt {attempt} of task {key}")


def triggers(ws: FakeWorkspaceClient, run: jobs.Run, key: str, child_run_id: int, attempt: int = 0) -> None:
    """Make the run job task `key` of `run` report that it triggered run `child_run_id`."""
    output = jobs.RunOutput(run_job_output=jobs.RunJobOutput(run_id=child_run_id))
    ws.jobs.outputs[run_id_of(run, key, attempt)] = output


def built_task(output: jobs.RunOutput, key: str = "t") -> TaskRun:
    """Build a one-task run whose task ended SUCCESS with `output`, and return that task."""
    ws = FakeWorkspaceClient()
    run = make_run(1, {key: "SUCCESS"})
    ws.jobs.outputs[run_id_of(run, key)] = output
    return build_from(ws, run).task(key)


def recorded_data(name: str) -> dict[str, Any]:
    return json.loads((DATA / "runs" / f"{name}.json").read_text())["run"]


def recorded_task(data: dict[str, Any], key: str) -> dict[str, Any]:
    return next(t for t in data["tasks"] if t["task_key"] == key)


def parent_with_child(
    job_id: int = 123, states: dict[str, str] | None = None
) -> tuple[FakeWorkspaceClient, jobs.Run, jobs.Run]:
    """Build a parent run whose `trigger_child` task triggered run 777 of job `job_id`, with tasks in `states`.

    Returns the fake client, the parent run and the child run.
    """
    ws = FakeWorkspaceClient()
    parent = make_run(1, {"trigger_child": "SUCCESS"})
    triggers(ws, parent, "trigger_child", 777)
    child = make_run(777, states or {"load": "SUCCESS"})
    child.job_id = job_id
    ws.jobs.add_run(child)
    return ws, parent, child


class TestRecordedRuns:
    """Runs recorded from a real workspace build into the expected JobRun."""

    def test_failed_task_with_probe(self) -> None:
        run, _ = build("probe_failure", capture_task_values=CAPTURE)
        assert run.failed
        assert not run.timed_out
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
        assert run.task("extract").output_json == {"task": "extract", "marker": "extract"}
        assert run.task("extract").task_values == {}

    def test_successful_run_with_probe(self) -> None:
        run, _ = build("probe_success", capture_task_values=CAPTURE)
        assert run.succeeded
        transform = run.task("transform")
        assert transform.task_values == {"stage": "done", "rows_written": 7}
        assert transform.missing_task_values == []
        assert transform.error is None
        assert transform.output is not None and '"greeting": "hello world"' in transform.output

    def test_failed_parent(self) -> None:
        run, _ = build("failed_no_probe_leaf")
        assert run.failed
        assert (run.result_state, run.termination_code) == ("FAILED", "RUN_EXECUTION_ERROR")
        assert "transform: ValueError: validation failed for explode" in repr(run)
        assert "load: UPSTREAM_FAILED" in repr(run)

    def test_cancelled_run(self) -> None:
        run, _ = build("cancelled")
        task = run.task("my_wheel_task")
        assert (run.result_state, run.termination_code) == ("CANCELED", "USER_CANCELED")
        assert (task.result_state, task.ran, task.error) == ("CANCELED", False, None)
        assert run.failed

    def test_logs_of_wheel_and_script_tasks(self) -> None:
        run, _ = build("wheel_logs")
        assert "scr_a" in run.task("scr_a").logs
        assert run.task("scr_a").log_lines
        assert all(line.strip() for line in run.task("same_a").log_lines)
        assert run.task("extract").logs == ""


class TestTaskValues:
    """Probe outputs become task values, with a reason for every value that is missing."""

    def test_values_of_a_task_that_did_not_run(self) -> None:
        run, _ = build("probe_upstream_failed", capture_task_values={**CAPTURE, "load": ["rows_written"]})
        load = run.task("load")
        assert (load.result_state, load.termination_code, load.ran) == ("UPSTREAM_FAILED", "SKIPPED", False)
        assert load.error is None
        assert load.missing_task_values == ["rows_written"]
        with pytest.raises(KeyError, match=r"Task 'load' did not run \(UPSTREAM_FAILED\), so value 'rows_written'"):
            load.task_values["rows_written"]
        assert run.task("transform").task_values["stage"] == "validation"

    def test_values_when_the_probe_is_missing_or_fails(self) -> None:
        ws = FakeWorkspaceClient()
        run = make_run(1, {"transform": "SUCCESS", "__pytest_probe__transform": "FAILED", "other": "SUCCESS"})
        ws.jobs.outputs[run_id_of(run, "__pytest_probe__transform")] = jobs.RunOutput(
            error="PermissionError: cannot read {values}"
        )
        result = build_from(ws, run, capture_task_values={"transform": ["k"], "other": ["k"]})
        with pytest.raises(KeyError, match=r"probe task __pytest_probe__transform ended FAILED: PermissionError"):
            result.task("transform").task_values["k"]
        with pytest.raises(KeyError, match="its probe task was not part of the run"):
            result.task("other").task_values["k"]

    def test_unreadable_probe_output(self) -> None:
        ws = FakeWorkspaceClient()
        run = make_run(1, {"transform": "SUCCESS", "__pytest_probe__transform": "SUCCESS"})
        notebook = jobs.NotebookOutput(result='{"values": {"k": "', truncated=True)
        ws.jobs.outputs[run_id_of(run, "__pytest_probe__transform")] = jobs.RunOutput(notebook_output=notebook)
        with pytest.warns(UserWarning, match="__pytest_probe__transform"):
            result = build_from(ws, run, capture_task_values={"transform": ["k"]})
        with pytest.raises(KeyError, match=r"returned unreadable output \(truncated\)"):
            result.task("transform").task_values["k"]


class TestSkippedAndDisabledTasks:
    """DISABLED task runs are told apart by the job's settings, and stubs count as injected."""

    def test_tasks_left_out_by_only_are_skipped(self) -> None:
        run, ws = build("only_plus_transform")
        assert run.succeeded
        assert "load" not in run.tasks
        assert sorted(run.skipped_tasks) == ["load"]
        assert run.skipped_tasks["load"].result_state == "DISABLED"
        assert 378690881497327 not in ws.jobs.output_calls
        with pytest.raises(KeyError, match="Skipped by only"):
            run.task("load")

    def test_only_skipped_task_is_still_skipped_by_only(self) -> None:
        run, _ = build("only_plus_transform")
        assert sorted(run.skipped_tasks) == ["load"]
        assert run.disabled_tasks == {}
        with pytest.raises(KeyError, match="Skipped by only"):
            run.task("load")

    def test_disabled_in_the_template_is_not_skipped_by_only(self) -> None:
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

    def test_stubs_count_as_injected(self) -> None:
        run, _ = build("probe_success", stubs={"extract"})
        assert sorted(run.tasks) == ["transform"]
        assert sorted(run.injected_tasks) == ["__pytest_probe__transform", "extract"]


class TestOutputs:
    """Task outputs are read once, failures to read them are logged, and truncation warns."""

    def test_output_errors_do_not_hide_the_run(self, caplog: pytest.LogCaptureFixture) -> None:
        ws = FakeWorkspaceClient()
        run = make_run(1, {"a": "SUCCESS"})
        ws.jobs.outputs[run_id_of(run, "a")] = NotFound("gone")
        result = build_from(ws, run)
        assert result.succeeded
        assert result.task("a").output is None
        assert "Could not read the output of task a" in caplog.text

    def test_build_job_run_reuses_given_outputs(self) -> None:
        ws = FakeWorkspaceClient()
        run, outputs = recorded("probe_success")
        ws.jobs.outputs.update(outputs)
        extract = next(t for t in run.tasks or [] if t.task_key == "extract")
        given = jobs.RunOutput(notebook_output=jobs.NotebookOutput(result="fetched while polling"))
        result = build_from(ws, run, outputs={extract.run_id or 0: given}, capture_task_values=CAPTURE)
        assert result.task("extract").output == "fetched while polling"
        assert result.task("transform").task_values == {"stage": "done", "rows_written": 7}
        assert sorted(ws.jobs.output_calls) == sorted(set(outputs) - {extract.run_id})

    @pytest.mark.parametrize(
        ("output", "message", "output_truncated", "logs_truncated"),
        [
            (
                jobs.RunOutput(notebook_output=jobs.NotebookOutput(result="x", truncated=True)),
                "The output of task 'transform' was cut at the 5 MB limit",
                True,
                False,
            ),
            (
                jobs.RunOutput(logs="tail", logs_truncated=True),
                "The logs of task 'transform' was cut at the 5 MB limit",
                False,
                True,
            ),
            (
                jobs.RunOutput(notebook_output=jobs.NotebookOutput(result="x", truncated=True), logs_truncated=True),
                "The output and logs of task 'transform' were cut at the 5 MB limit",
                True,
                True,
            ),
            (
                jobs.RunOutput(
                    clean_rooms_notebook_output=jobs.CleanRoomsNotebookTaskCleanRoomsNotebookTaskOutput(
                        notebook_output=jobs.NotebookOutput(result="x", truncated=True)
                    )
                ),
                "The output of task 'transform' was cut at the 5 MB limit",
                True,
                False,
            ),
        ],
        ids=["output", "logs", "output_and_logs", "clean_room_output"],
    )
    def test_truncated_output_warns(
        self, output: jobs.RunOutput, message: str, output_truncated: bool, logs_truncated: bool
    ) -> None:
        ws = FakeWorkspaceClient()
        run = make_run(1, {"extract": "SUCCESS", "transform": "SUCCESS"})
        ws.jobs.outputs[run_id_of(run, "transform")] = output
        with pytest.warns(UserWarning) as record:
            result = build_from(ws, run)
        assert [str(w.message) for w in record] == [message]
        transform = result.task("transform")
        assert transform.output_truncated is output_truncated
        assert transform.logs_truncated is logs_truncated

    def test_no_warning_without_truncation(self) -> None:
        with warnings.catch_warnings(record=True) as record:
            warnings.simplefilter("always")
            build("probe_success", capture_task_values=CAPTURE)
        assert record == []


class TestConditions:
    """Condition task outcomes and the branches they exclude."""

    def test_untaken_branches_do_not_fail_the_run(self) -> None:
        ws = FakeWorkspaceClient()
        run = make_run(1, {"check": "SUCCESS", "on_true": "SUCCESS", "on_false": "EXCLUDED"})
        result = build_from(ws, run)
        assert result.succeeded
        assert result.task("on_false").result_state == "EXCLUDED"
        assert result.task("on_false").excluded
        assert not result.task("on_true").excluded
        assert repr(result).endswith("failing=[])")

    def test_condition_outcome_and_branch_helpers(self) -> None:
        ws = FakeWorkspaceClient()
        states = {"check": "SUCCESS", "on_true": "SUCCESS", "on_false": "EXCLUDED"}
        run = build_from(ws, make_run(1, states, condition_outcomes={"check": "true"}))
        assert run.task("check").condition_outcome is True
        assert run.task("on_true").condition_outcome is None
        assert run.branch_taken("check") is True
        assert run.task("on_false").excluded
        assert run.executed_tasks == ["check", "on_true"]
        assert run.succeeded

    @pytest.mark.parametrize(("outcome", "expected"), [("true", True), ("false", False), ("", None)])
    def test_condition_outcome_reads_the_reported_outcome(self, outcome: str, expected: bool | None) -> None:
        ws = FakeWorkspaceClient()
        run = build_from(ws, make_run(1, {"check": "SUCCESS"}, condition_outcomes={"check": outcome}))
        assert run.task("check").condition_outcome is expected
        if expected is not None:
            assert run.branch_taken("check") is expected


class TestParameters:
    """Job parameters come from the run, task parameters from each task's resolved values."""

    def test_job_parameters_are_resolved(self) -> None:
        run, _ = build("probe_failure")
        assert run.parameters == {"param1": "explode", "param2": "world", "mode": "full", "schema": ""}

    def test_job_parameters_default_to_empty(self) -> None:
        ws = FakeWorkspaceClient()
        run = make_run(1, {"a": "SUCCESS"})
        assert run.job_parameters is None
        assert build_from(ws, run).parameters == {}

    def test_reserved_job_parameters_are_hidden(self) -> None:
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

    def test_task_parameters_are_read_from_each_task_run(self) -> None:
        # The API fills resolved_values only on a task run fetched by its own id, never on the job run.
        ws = FakeWorkspaceClient()
        run = make_run(1, {"transform": "SUCCESS", "jar": "SUCCESS", "plain": "SUCCESS", "load": "UPSTREAM_FAILED"})
        notebook = jobs.ResolvedNotebookTaskValues(base_parameters={"row_count": "7"})
        ws.jobs.resolved[100] = jobs.ResolvedValues(notebook_task=notebook)
        jar = jobs.ResolvedStringParamsValues(parameters=["--date", "2026-10-01"])
        ws.jobs.resolved[101] = jobs.ResolvedValues(spark_jar_task=jar)
        result = build_from(ws, run)
        assert result.task("transform").parameters == {"row_count": "7"}
        assert result.task("jar").parameters == ["--date", "2026-10-01"]
        assert result.task("plain").parameters is None
        fetched = [call["run_id"] for call in ws.jobs.get_run_calls if call.get("include_resolved_values")]
        assert sorted(fetched) == [100, 101, 102], "one fetch per task run that ran, none for load"

    def test_task_parameters_survive_an_unreadable_task_run(self) -> None:
        ws = FakeWorkspaceClient()
        run = make_run(1, {"a": "SUCCESS", "b": "SUCCESS"})
        ws.jobs.resolved[100] = NotFound("gone")
        ws.jobs.resolved[101] = jobs.ResolvedValues(sql_task=jobs.ResolvedParamPairValues(parameters={"k": "v"}))
        result = build_from(ws, run)
        assert result.succeeded
        assert result.task("a").parameters is None
        assert result.task("b").parameters == {"k": "v"}

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
    def test_task_parameters_of_each_task_type(self, resolved: jobs.ResolvedValues, expected: object) -> None:
        ws = FakeWorkspaceClient()
        ws.jobs.resolved[100] = resolved
        assert build_from(ws, make_run(1, {"t": "SUCCESS"})).task("t").parameters == expected


class TestAttempts:
    """A retried task describes its last attempt and keeps every attempt in order."""

    def test_every_attempt_is_kept(self) -> None:
        run, ws = build("probe_failure", capture_task_values=CAPTURE)
        transform = run.task("transform")
        assert transform.attempt == 1
        assert [a.attempt for a in transform.attempts] == [0, 1]
        assert [a.run_id for a in transform.attempts] == [1103977916948693, 1054812578975269]
        assert all(a.attempts == [] for a in transform.attempts)
        assert transform.attempts[-1] == replace(transform, attempts=[])
        assert {1103977916948693, 1054812578975269} <= set(ws.jobs.output_calls)
        extract = run.task("extract")
        assert extract.attempts == [replace(extract, attempts=[])]

    def test_attempts_from_make_run(self) -> None:
        ws = FakeWorkspaceClient()
        raw = make_run(9, {"s": "SUCCESS"}, attempts={"t": ["FAILED", "FAILED", "SUCCESS"]})
        ws.jobs.outputs[run_id_of(raw, "t", attempt=1)] = jobs.RunOutput(error="ValueError: flaky")
        run = build_from(ws, raw)
        t = run.task("t")
        assert t.succeeded
        assert (t.attempt, t.run_id) == (2, run_id_of(raw, "t", attempt=2))
        assert [(a.attempt, a.result_state) for a in t.attempts] == [(0, "FAILED"), (1, "FAILED"), (2, "SUCCESS")]
        assert t.attempts[0].error is None
        assert t.attempts[1].raised(ValueError)
        every_task_run = [run_id_of(raw, "s"), *(run_id_of(raw, "t", attempt) for attempt in range(3))]
        assert sorted(ws.jobs.output_calls) == sorted(every_task_run)
        assert run.task("s").attempts == [replace(run.task("s"), attempts=[])]

    def test_task_keys_keep_the_order_the_run_lists_them(self) -> None:
        run = make_run(9, {"a": "SUCCESS"}, attempts={"b": ["FAILED", "SUCCESS"]})
        a, b0, b1 = run.tasks or []
        run.tasks = [b1, a, b0]
        result = build_from(FakeWorkspaceClient(), run)
        assert list(result.tasks) == ["b", "a"]
        assert [t.attempt for t in result.task("b").attempts] == [0, 1]


class TestTimeouts:
    """A TIMEDOUT task or run marks the run as timed out."""

    def test_timed_out_task_and_run(self) -> None:
        ws = FakeWorkspaceClient()
        raw = make_run(9, {"load": "TIMEDOUT"}, result_state="TIMEDOUT", code="RUN_EXECUTION_ERROR")
        ws.jobs.outputs[run_id_of(raw, "load")] = jobs.RunOutput(error="TimeoutError: too slow")
        run = build_from(ws, raw)
        load = run.task("load")
        assert run.timed_out
        assert load.timed_out
        assert load.ran
        assert load.raised(TimeoutError)
        assert run.failed

    def test_a_timed_out_task_times_out_a_failed_run(self) -> None:
        ws = FakeWorkspaceClient()
        run = build_from(ws, make_run(9, {"load": "TIMEDOUT"}, result_state="FAILED", code="RUN_EXECUTION_ERROR"))
        assert run.timed_out
        assert run.task("load").timed_out
        assert run.failed
        assert "load: TIMEDOUT" in repr(run)


class TestTiming:
    """Timing and the effective performance target are read from the run and each task run."""

    def test_timing_breakdown_from_a_recorded_run(self) -> None:
        data = recorded_data("probe_success")
        run, _ = build("probe_success", capture_task_values=CAPTURE)
        assert "queue_duration" not in data
        # probe_success has several tasks, so its parent run reports zero for setup, execution and cleanup. The
        # comparisons below check those zeros; test_timing_on_a_single_task_parent_run covers non-zero parent values.
        assert run.timing.queue == timedelta(0)
        assert run.timing.setup == timedelta(milliseconds=data["setup_duration"])
        assert run.timing.execution == timedelta(milliseconds=data["execution_duration"])
        assert run.timing.cleanup == timedelta(milliseconds=data["cleanup_duration"])
        assert run.effective_performance_target == data["effective_performance_target"]
        transform = recorded_task(data, "transform")
        assert run.task("transform").timing.execution == timedelta(milliseconds=transform["execution_duration"])
        assert run.task("transform").timing.setup == timedelta(milliseconds=transform["setup_duration"])
        assert run.task("transform").effective_performance_target == transform["effective_performance_target"]
        assert run.task("transform").timing.total == sum(
            (timedelta(milliseconds=transform[f"{k}_duration"]) for k in ("setup", "execution", "cleanup")),
            timedelta(0),
        )
        assert "timing" not in repr(run) + repr(run.task("transform"))
        assert "performance" not in repr(run) + repr(run.task("transform"))

    def test_timing_on_a_single_task_parent_run(self) -> None:
        data = recorded_data("cancelled")
        assert data["setup_duration"] and data["execution_duration"]
        run, _ = build("cancelled")
        assert run.timing.setup == timedelta(milliseconds=data["setup_duration"])
        assert run.timing.execution == timedelta(milliseconds=data["execution_duration"])
        assert run.timing.cleanup == timedelta(milliseconds=data["cleanup_duration"])
        assert run.timing.total == timedelta(milliseconds=data["setup_duration"] + data["execution_duration"])
        assert run.effective_performance_target == data["effective_performance_target"]

    def test_timing_defaults_to_zero_without_fields(self) -> None:
        run = build_from(FakeWorkspaceClient(), make_run(1, {"t": "SUCCESS"}))
        zero = Timing(timedelta(0), timedelta(0), timedelta(0), timedelta(0))
        assert run.timing == zero
        assert run.effective_performance_target is None
        assert run.task("t").timing == zero
        assert run.task("t").effective_performance_target is None

    def test_effective_performance_target_on_run_and_task(self) -> None:
        data = recorded_data("wheel_logs")
        run, _ = build("wheel_logs")
        assert run.effective_performance_target == data["effective_performance_target"]
        for key, task in run.tasks.items():
            assert task.effective_performance_target == recorded_task(data, key)["effective_performance_target"]


class TestTaskOutputs:
    """The typed outputs of SQL, alert, dbt, run-job, dashboard and clean room tasks are passed through."""

    def test_sql_query_output_is_exposed(self) -> None:
        query = jobs.SqlQueryOutput(warehouse_id="w1", output_link="https://x/q")
        task = built_task(jobs.RunOutput(sql_output=jobs.SqlOutput(query_output=query)), "daily_query")
        assert task.sql_output is not None and task.sql_output.query_output is not None
        assert task.sql_output.query_output.warehouse_id == "w1"
        assert task.sql_output.query_output.output_link == "https://x/q"
        assert task.alert_state is None

    @pytest.mark.parametrize(
        ("output", "expected"),
        [
            pytest.param(
                lambda: jobs.RunOutput(
                    alert_output=jobs.AlertTaskOutput(alert_state=jobs.AlertEvaluationState.TRIGGERED)
                ),
                "TRIGGERED",
                marks=NEW_ALERT_TASK,
                id="alert-task",
            ),
            pytest.param(
                lambda: jobs.RunOutput(
                    sql_output=jobs.SqlOutput(
                        alert_output=jobs.SqlAlertOutput(alert_state=jobs.SqlAlertState.TRIGGERED)
                    )
                ),
                "TRIGGERED",
                id="legacy-sql-alert",
            ),
            pytest.param(
                lambda: jobs.RunOutput(
                    sql_output=jobs.SqlOutput(alert_output=jobs.SqlAlertOutput(alert_state=jobs.SqlAlertState.OK))
                ),
                "OK",
                id="legacy-sql-alert-ok",
            ),
        ],
    )
    def test_alert_state_from_new_and_legacy_alert_outputs(
        self, output: Callable[[], jobs.RunOutput], expected: str
    ) -> None:
        assert built_task(output(), "check").alert_state == expected

    @NEW_ALERT_TASK
    def test_alert_task_output_is_exposed(self) -> None:
        alert = jobs.AlertTaskOutput(alert_state=jobs.AlertEvaluationState.OK)
        task = built_task(jobs.RunOutput(alert_output=alert), "check")
        assert task.alert_output == alert
        assert task.alert_state == "OK"

    def test_run_job_and_dbt_outputs(self) -> None:
        trigger = built_task(jobs.RunOutput(run_job_output=jobs.RunJobOutput(run_id=42)), "trigger")
        assert trigger.run_job_output is not None and trigger.run_job_output.run_id == 42
        dbt = jobs.DbtOutput(artifacts_link="https://x/artifacts", artifacts_headers={"h": "v"})
        models = built_task(jobs.RunOutput(dbt_output=dbt), "models")
        assert models.dbt_output is not None and models.dbt_output.artifacts_link == "https://x/artifacts"

    def test_dashboard_output_is_exposed(self) -> None:
        dashboard = jobs.DashboardTaskOutput(page_snapshots=[])
        assert built_task(jobs.RunOutput(dashboard_output=dashboard)).dashboard_output == dashboard

    def test_clean_room_notebook_output_fills_output(self) -> None:
        clean = jobs.CleanRoomsNotebookTaskCleanRoomsNotebookTaskOutput(
            notebook_output=jobs.NotebookOutput(result='{"n": 3}', truncated=False)
        )
        task = built_task(jobs.RunOutput(clean_rooms_notebook_output=clean), "clean")
        assert task.clean_rooms_notebook_output == clean
        assert task.output == '{"n": 3}'
        assert task.output_json == {"n": 3}
        assert task.output_truncated is False

    def test_top_level_notebook_output_wins_over_clean_room_output(self) -> None:
        clean = jobs.CleanRoomsNotebookTaskCleanRoomsNotebookTaskOutput(notebook_output=jobs.NotebookOutput(result="b"))
        output = jobs.RunOutput(notebook_output=jobs.NotebookOutput(result="a"), clean_rooms_notebook_output=clean)
        assert built_task(output).output == "a"

    def test_empty_top_level_notebook_output_falls_back_to_clean_room_output(self) -> None:
        output = jobs.RunOutput.from_dict(
            {
                "notebook_output": {},
                "clean_rooms_notebook_output": {"notebook_output": {"result": "b", "truncated": True}},
            }
        )
        with pytest.warns(UserWarning, match="The output of task 't' was cut"):
            task = built_task(output)
        assert task.output == "b"
        assert task.output_truncated is True

    def test_task_outputs_are_none_for_an_empty_run_output(self) -> None:
        task = built_task(jobs.RunOutput())
        assert task.sql_output is None
        assert task.alert_output is None
        assert task.dbt_output is None
        assert task.run_job_output is None
        assert task.dashboard_output is None
        assert task.clean_rooms_notebook_output is None
        assert task.alert_state is None
        assert task.output is None


class TestChildRuns:
    """A run-job task carries the id of the run it triggered and that run, read once the parent finished."""

    def test_child_run_id_comes_from_the_run_job_output(self) -> None:
        with_id = built_task(jobs.RunOutput(run_job_output=jobs.RunJobOutput(run_id=777)), "trigger_child")
        assert with_id.child_run_id == 777
        assert built_task(jobs.RunOutput(run_job_output=jobs.RunJobOutput())).child_run_id is None

    def test_child_run_of_a_run_job_task(self) -> None:
        ws, run, _ = parent_with_child()
        child = build_from(ws, run, job_keys={123: "child"}).task("trigger_child").child_run
        assert child is not None
        assert (child.job_key, child.job_id, child.run_id) == ("child", 123, 777)
        assert sorted(child.tasks) == ["load"]
        assert child.task("load").succeeded
        assert child.succeeded
        # The child run is read once; the other calls read each task run's resolved parameters.
        assert [call["run_id"] for call in ws.jobs.get_run_calls if not call.get("include_resolved_values")] == [777]

    def test_child_run_key_falls_back_to_the_job_id(self) -> None:
        ws, run, _ = parent_with_child()
        child = build_from(ws, run).task("trigger_child").child_run
        assert child is not None
        assert (child.job_key, child.job_id) == ("123", 123)

    def test_child_run_follows_the_parent_rules_for_failure(self) -> None:
        ws, run, child_run = parent_with_child(states={"load": "FAILED"})
        ws.jobs.outputs[run_id_of(child_run, "load")] = jobs.RunOutput(error="boom")
        child = build_from(ws, run).task("trigger_child").child_run
        assert child is not None
        assert child.failed
        assert child.task("load").error is not None

    def test_child_run_is_none_for_a_run_job_task_that_did_not_run(self) -> None:
        ws = FakeWorkspaceClient()
        raw = make_run(1, {"trigger_child": "UPSTREAM_FAILED"})
        triggers(ws, raw, "trigger_child", 777)
        run = build_from(ws, raw)
        assert run.task("trigger_child").child_run is None
        assert ws.jobs.get_run_calls == []

    def test_child_run_is_none_when_the_child_run_cannot_be_read(self, caplog: pytest.LogCaptureFixture) -> None:
        ws = FakeWorkspaceClient()
        raw = make_run(1, {"trigger_child": "SUCCESS"})
        triggers(ws, raw, "trigger_child", 777)
        task = build_from(ws, raw).task("trigger_child")
        assert task.child_run_id == 777
        assert task.child_run is None
        assert "Could not read run 777 triggered by task trigger_child" in caplog.text

    def test_child_run_is_none_for_other_tasks(self) -> None:
        run, ws = build("probe_success", capture_task_values=CAPTURE)
        assert all(task.child_run_id is None for task in run.tasks.values())
        assert all(task.child_run is None for task in run.tasks.values())
        assert all(task.child_run is None for task in run.injected_tasks.values())
        assert not [call for call in ws.jobs.get_run_calls if not call.get("include_resolved_values")]

    def test_child_run_of_a_child_is_nested(self) -> None:
        ws = FakeWorkspaceClient()
        parent = make_run(1, {"trigger_child": "SUCCESS"})
        child = make_run(777, {"trigger_grandchild": "SUCCESS"})
        child.job_id = 123
        grandchild = make_run(888, {"load": "SUCCESS"})
        grandchild.job_id = 456
        triggers(ws, parent, "trigger_child", 777)
        triggers(ws, child, "trigger_grandchild", 888)
        ws.jobs.add_run(child)
        ws.jobs.add_run(grandchild)
        run = build_from(ws, parent, job_keys={123: "child", 456: "grandchild"})
        first = run.task("trigger_child").child_run
        assert first is not None
        second = first.task("trigger_grandchild").child_run
        assert second is not None
        assert (second.job_key, second.job_id, second.run_id) == ("grandchild", 456, 888)
        assert second.task("load").succeeded

    def test_child_run_puts_disabled_child_tasks_in_disabled_tasks(self) -> None:
        ws, run, _ = parent_with_child(states={"load": "SUCCESS", "backfill": "DISABLED"})
        child = build_from(ws, run).task("trigger_child").child_run
        assert child is not None
        assert sorted(child.disabled_tasks) == ["backfill"]
        assert child.skipped_tasks == {}
        with pytest.raises(KeyError, match="Disabled in the job"):
            child.task("backfill")

    def test_every_attempt_of_a_run_job_task_has_its_child_run(self) -> None:
        ws = FakeWorkspaceClient()
        run = make_run(1, {}, attempts={"trigger_child": ["FAILED", "SUCCESS"]})
        triggers(ws, run, "trigger_child", 777, attempt=0)
        triggers(ws, run, "trigger_child", 778, attempt=1)
        for child_id, state in ((777, "FAILED"), (778, "SUCCESS")):
            child = make_run(child_id, {"load": state})
            child.job_id = 123
            ws.jobs.add_run(child)
        task = build_from(ws, run).task("trigger_child")
        first, last = task.attempts
        assert first.child_run is not None
        assert (first.child_run.run_id, first.child_run.failed) == (777, True)
        assert last.child_run is not None
        assert last.child_run.run_id == 778
        assert task.child_run == last.child_run
        assert task.attempts[-1].child_run == task.child_run
