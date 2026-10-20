"""Job runs as the Jobs API reports them: recorded runs, built runs and the raw outputs the tests replay."""

from __future__ import annotations

import base64
import json
import time
import urllib.error
import urllib.parse
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, field, fields, replace
from pathlib import Path
from typing import TYPE_CHECKING, Any

from databricks.sdk.service import jobs

if TYPE_CHECKING:
    from .workspace import FakeWorkspaceClient


DATA = Path(__file__).parent.parent / "data"
HOST = "https://example.cloud.databricks.com"
HOUR_MS = 3_600_000
TERMINATED = jobs.RunLifecycleStateV2State.TERMINATED
SKIPPED = jobs.RunLifeCycleState.SKIPPED
# What the platform reports, on the run and on each of its task runs, when every task of the job is disabled.
ALL_DISABLED = "Skipped because every task is disabled. Re-enable at least one task to be able to run the job."
# The error `runs/get-output` reports for every pipeline task run, also one that ended SUCCESS.
PIPELINE_LOGS = "Please refer to the logs for this pipeline in the pipelines page."
# The termination message of an alert task whose query failed, as recorded on the alerts bundle.
ALERT_FAILED = (
    "An error occurred during execution of task Alert task: Query execution failed. Please check the alert query "
    "text and evaluation conditions first."
)
# What the platform reports for a for-each task whose iterations failed: one entry per distinct error, counted.
ITERATION_FAILED = "Workload failed, see run output for details"


@dataclass
class RunWithIterations(jobs.Run):
    """A job run that also carries the iterations of its for-each tasks, which `FakeJobs` serves.

    The Jobs API lists no iterations on a job run, only on a for-each task run fetched by its own id, so `make_run`
    and `recorded` hand them to `FakeJobs.add_run` and `FakeJobs.run_now` here.

    Attributes:
        for_each_iterations: The iteration runs of each for-each task, by the for-each task run's id.
    """

    for_each_iterations: dict[int, list[jobs.RunTask]] = field(default_factory=dict)


def listed_job(
    job_id: int,
    age_hours: float,
    tags: dict[str, str],
    creator: str = "someone@example.com",
    notebooks: Sequence[str] = (),
    tasks: Sequence[jobs.Task] = (),
) -> jobs.BaseJob:
    """Build a job as `jobs.list` returns it, created `age_hours` ago by `creator`.

    It has `tasks` and one notebook task for each of `notebooks`, which `jobs.list` returns only with `expand_tasks`.
    """
    created = int(time.time() * 1000 - age_hours * HOUR_MS)
    tasks = list(tasks)
    for i, path in enumerate(notebooks):
        tasks.append(jobs.Task(task_key=f"task_{i}", notebook_task=jobs.NotebookTask(notebook_path=path)))
    settings = jobs.JobSettings(tags=tags, tasks=tasks or None)
    return jobs.BaseJob(job_id=job_id, created_time=created, creator_user_name=creator, settings=settings)


def sweep_candidates() -> list[jobs.BaseJob]:
    """Jobs for `--lakeflow-sweep` to choose from. With a 24 hour cutoff and run id `this-run`, it deletes 1 and 7."""
    old = {"pytest": "", "pytest_run_id": "old-run"}
    return [
        listed_job(1, 25, old),
        listed_job(2, 25, {"pytest": "", "pytest_run_id": "this-run"}),
        listed_job(3, 1, old),
        listed_job(4, 25, old, creator="colleague@example.com"),
        listed_job(5, 25, {"pytest_run_id": "old-run"}),
        listed_job(6, 25, {"pytest": ""}),
        listed_job(7, 30, old),
        jobs.BaseJob(job_id=8, created_time=0),
    ]


def recorded(name: str) -> tuple[jobs.Run, dict[int, jobs.RunOutput]]:
    """Load a run recorded during the spikes, with its task outputs by task run id.

    A recording of a run with for-each tasks has an `iterations` key: the iterations that each for-each task run
    lists, by its run id, each with the `resolved_values` its own `get_run` reported. The run then carries them, as
    a `RunWithIterations`.
    """
    data = json.loads((DATA / "runs" / f"{name}.json").read_text())
    outputs = {int(run_id): jobs.RunOutput.from_dict(output) for run_id, output in data["outputs"].items()}
    run = jobs.Run.from_dict(data["run"])
    iterations: dict[int, list[jobs.RunTask]] = {}
    for loop_run_id, listed in data.get("iterations", {}).items():
        iterations[int(loop_run_id)] = [jobs.RunTask.from_dict(iteration) for iteration in listed]
    return with_iterations(run, iterations), outputs


def with_iterations(run: jobs.Run, iterations: Mapping[int, list[jobs.RunTask]]) -> jobs.Run:
    """Return `run` carrying `iterations`, by for-each task run id, or `run` itself when there are none."""
    if not iterations:
        return run
    values = {f.name: getattr(run, f.name) for f in fields(jobs.Run)}
    return RunWithIterations(**values, for_each_iterations=dict(iterations))


def make_run(
    run_id: int,
    states: Mapping[str, str | None],
    result_state: str = "SUCCESS",
    code: str = "SUCCESS",
    attempts: Mapping[str, Sequence[str | None]] | None = None,
    condition_outcomes: Mapping[str, str] | None = None,
    notebooks: Collection[str] = (),
    sql_files: Mapping[str, str] | None = None,
    pipeline_tasks: Mapping[str, str] | None = None,
    trigger: jobs.TriggerType = jobs.TriggerType.ONE_TIME,
    iterations: Mapping[str, Sequence[str]] | None = None,
    effective_performance_target: str | None = None,
) -> jobs.Run:
    """Build a terminated run whose tasks ended in `states`, keyed by task key.

    Like the Jobs API, a `DISABLED` task run never carries `disabled`, whether the job disabled it or `only` left it
    out: only the job's settings say which tasks are disabled.

    A state of `None` gives a task run with no result state and legacy life cycle `SKIPPED`, as the platform reports
    every task of a run it skipped.

    `attempts` gives the state of every attempt of the tasks it names, in order, numbered from attempt 0. Task run
    ids are `run_id * 100` plus the position of the attempt among all task runs.

    `condition_outcomes` makes the tasks it names condition tasks, with the given `outcome`, such as `"true"`.

    `notebooks` makes the tasks it names notebook tasks, which the run reports with their `notebook_task`.

    `sql_files` makes the tasks it names SQL file tasks on warehouse `w1`, running the file at the given path, which
    the run reports with their `sql_task`.

    `pipeline_tasks` makes the tasks it names pipeline tasks that update the pipeline with the given id. Every task run
    starts at 1,000 ms and ends at 3,000 ms, so an update created in between is the one the task started.

    `trigger` is what started the run, `ONE_TIME` for a run-now run, as the platform reports it.

    `iterations` makes the tasks it names for-each tasks over the inputs `"0"`, `"1"` and so on, repeating the task
    `each`, whose iterations ended in the given states, in order, each in one attempt. The task run reports its
    `for_each_task` with the platform's `stats`, and the run is a `RunWithIterations`, so `FakeJobs` lists the
    iterations on the task run fetched by its own id. An iteration's run id is the for-each task run's id times 1,000
    plus its position.

    `effective_performance_target` is the serverless performance mode the run and every task run report, such as
    `"STANDARD"`. `None`, the default, is what a run on classic compute reports.
    """
    states_by_key = {key: [state] for key, state in states.items()}
    for key, key_states in (attempts or {}).items():
        states_by_key[key] = list(key_states)
    tasks = []
    for key, key_states in states_by_key.items():
        for attempt, state in enumerate(key_states):
            task_run_id = run_id * 100 + len(tasks)
            tasks.append(
                jobs.RunTask(
                    task_key=key,
                    run_id=task_run_id,
                    attempt_number=attempt,
                    state=_state(state),
                    status=jobs.RunStatus(state=TERMINATED, termination_details=_details(state)),
                    run_page_url=f"{HOST}/jobs/runs/{task_run_id}",
                    start_time=1_000,
                    end_time=3_000,
                    condition_task=_condition(condition_outcomes, key),
                    notebook_task=_notebook(notebooks, key),
                    sql_task=_sql_file(sql_files, key),
                    pipeline_task=_pipeline(pipeline_tasks, key),
                    for_each_task=_for_each(iterations, key),
                    effective_performance_target=_performance_target(effective_performance_target),
                )
            )
    loops: dict[int, list[jobs.RunTask]] = {}
    for task in tasks:
        if iterations is not None and task.task_key in iterations and task.run_id is not None:
            loops[task.run_id] = _iteration_runs(task.run_id, iterations[task.task_key])
    run = jobs.Run(
        run_id=run_id,
        job_run_id=run_id,
        state=jobs.RunState(result_state=jobs.RunResultState(result_state)),
        status=jobs.RunStatus(state=TERMINATED, termination_details=_details(code)),
        tasks=tasks,
        run_page_url=f"{HOST}/jobs/runs/{run_id}",
        start_time=1_000,
        end_time=6_000,
        trigger=trigger,
        effective_performance_target=_performance_target(effective_performance_target),
    )
    return with_iterations(run, loops)


def skipped_run(run_id: int, keys: Sequence[str], notebooks: Collection[str] = ()) -> jobs.Run:
    """Build the run the platform reports when every task of the job is disabled, with one task run per key.

    As recorded on a real run: neither the run nor its task runs have a result state, their legacy life cycle is
    `SKIPPED`, and each carries termination code `RUN_EXECUTION_ERROR` with the message in `ALL_DISABLED`. The
    platform ends the run at once, so it starts and ends at the same time. `notebooks` makes the tasks it names
    notebook tasks, as in `make_run`.
    """
    run = make_run(run_id, dict.fromkeys(keys), notebooks=notebooks)
    details = jobs.TerminationDetails(
        code=jobs.TerminationCodeCode.RUN_EXECUTION_ERROR,
        message=ALL_DISABLED,
        type=jobs.TerminationTypeType.CLIENT_ERROR,
    )
    status = jobs.RunStatus(state=TERMINATED, termination_details=details)
    state = jobs.RunState(life_cycle_state=SKIPPED, state_message=ALL_DISABLED)
    tasks = [replace(task, state=state, status=status) for task in run.tasks or []]
    return replace(run, state=state, status=status, tasks=tasks, end_time=run.start_time)


def queue_failed_run(ws: FakeWorkspaceClient, states: Mapping[str, str | None] | None = None) -> None:
    """Queue failed run 77, whose tasks end in `states`, by default transform `FAILED`.

    Its first task run, 7700, reports resolved notebook parameters, so a debug notebook can replay them.
    """
    ws.jobs.resolved[7700] = jobs.ResolvedValues(
        notebook_task=jobs.ResolvedNotebookTaskValues(base_parameters={"row_count": "7", "param1": "hello"})
    )
    if states is None:
        states = {"transform": "FAILED"}
    ws.jobs.queue(make_run(77, states, "FAILED", "RUN_EXECUTION_ERROR"))


def failed_alert_run(run_id: int, key: str = "broken") -> jobs.Run:
    """Build the run of one alert task, `key`, whose query failed, as recorded on the alerts bundle.

    The task ends FAILED with termination code `RUN_EXECUTION_ERROR`, type `CLIENT_ERROR` and the message in
    `ALERT_FAILED`, which is also its state message, and the run's life cycle is `INTERNAL_ERROR`. The task's
    `runs/get-output` holds only `alert_output.alert_state` `ERROR`, with no error: see `failed_alert_output`.
    """
    run = make_run(run_id, {key: "FAILED"}, "FAILED", "RUN_EXECUTION_ERROR")
    details = jobs.TerminationDetails(
        code=jobs.TerminationCodeCode.RUN_EXECUTION_ERROR,
        message=ALERT_FAILED,
        type=jobs.TerminationTypeType.CLIENT_ERROR,
    )
    status = jobs.RunStatus(state=TERMINATED, termination_details=details)
    state = jobs.RunState(
        life_cycle_state=jobs.RunLifeCycleState.TERMINATED,
        result_state=jobs.RunResultState.FAILED,
        state_message=ALERT_FAILED,
    )
    alert = jobs.AlertTask(alert_id="3926452559074312", warehouse_id="0123456789abcdef")
    tasks = [replace(task, state=state, status=status, alert_task=alert) for task in run.tasks or []]
    run_state = jobs.RunState(
        life_cycle_state=jobs.RunLifeCycleState.INTERNAL_ERROR, result_state=jobs.RunResultState.FAILED
    )
    return replace(run, state=run_state, status=status, tasks=tasks)


def failed_alert_output() -> jobs.RunOutput:
    """The `runs/get-output` of the alert task of `failed_alert_run`: an alert state and no error."""
    return jobs.RunOutput(alert_output=jobs.AlertTaskOutput(alert_state=jobs.AlertEvaluationState.ERROR))


def running_run(run_id: int, states: dict[str, str]) -> jobs.Run:
    """Build a run that has not terminated, with one task run per key of `states`.

    A value that is a V2 lifecycle state, such as `PENDING` or `RUNNING`, gives a task run still in that state. Any
    other value is a result state, such as `SUCCESS`, of a task run that has terminated. Task run ids are numbered as
    in `make_run`, so outputs keyed by them also match the terminated run.
    """
    lifecycle = {state.value for state in jobs.RunLifecycleStateV2State} - {TERMINATED.value}
    tasks = []
    for position, (key, state) in enumerate(states.items()):
        task_run_id = run_id * 100 + position
        url = f"{HOST}/jobs/runs/{task_run_id}"
        if state in lifecycle:
            status = jobs.RunStatus(state=jobs.RunLifecycleStateV2State(state))
            tasks.append(jobs.RunTask(task_key=key, run_id=task_run_id, status=status, run_page_url=url))
            continue
        tasks.append(
            jobs.RunTask(
                task_key=key,
                run_id=task_run_id,
                state=jobs.RunState(result_state=jobs.RunResultState(state)),
                status=jobs.RunStatus(state=TERMINATED, termination_details=_details(state)),
                run_page_url=url,
                start_time=1_000,
                end_time=3_000,
            )
        )
    status = jobs.RunStatus(state=jobs.RunLifecycleStateV2State.RUNNING)
    return jobs.Run(
        run_id=run_id, job_run_id=run_id, status=status, tasks=tasks, run_page_url=f"{HOST}/jobs/runs/{run_id}"
    )


def _condition(outcomes: Mapping[str, str] | None, key: str) -> jobs.RunConditionTask | None:
    if outcomes is None or key not in outcomes:
        return None
    return jobs.RunConditionTask(op=jobs.ConditionTaskOp.EQUAL_TO, left="", right="", outcome=outcomes[key])


def _notebook(notebooks: Collection[str], key: str) -> jobs.NotebookTask | None:
    if key not in notebooks:
        return None
    return jobs.NotebookTask(notebook_path=f"/Workspace/src/{key}")


def _sql_file(sql_files: Mapping[str, str] | None, key: str) -> jobs.SqlTask | None:
    if sql_files is None or key not in sql_files:
        return None
    return jobs.SqlTask(warehouse_id="w1", file=jobs.SqlTaskFile(path=sql_files[key]))


def _pipeline(pipeline_ids: Mapping[str, str] | None, key: str) -> jobs.PipelineTask | None:
    if pipeline_ids is None or key not in pipeline_ids:
        return None
    return jobs.PipelineTask(pipeline_id=pipeline_ids[key])


def _performance_target(target: str | None) -> jobs.PerformanceTarget | None:
    if target is None:
        return None
    return jobs.PerformanceTarget(target)


def _for_each(iterations: Mapping[str, Sequence[str]] | None, key: str) -> jobs.RunForEachTask | None:
    if iterations is None or key not in iterations:
        return None
    states = iterations[key]
    succeeded = states.count("SUCCESS")
    failed = len(states) - succeeded
    # As recorded, the platform counts each iteration once, however often it was retried, and groups the errors by
    # message, with the same generic message for every iteration that failed.
    errors = []
    if failed:
        errors.append(
            jobs.ForEachTaskErrorMessageStats(
                count=failed, error_message=ITERATION_FAILED, termination_category="RunExecutionError"
            )
        )
    task_run_stats = jobs.ForEachTaskTaskRunStats(
        total_iterations=len(states),
        scheduled_iterations=len(states),
        active_iterations=0,
        completed_iterations=len(states),
        succeeded_iterations=succeeded,
        failed_iterations=failed,
    )
    stats = jobs.ForEachStats(task_run_stats=task_run_stats, error_message_stats=errors)
    inputs = json.dumps([str(position) for position in range(len(states))])
    return jobs.RunForEachTask(inputs=inputs, task=jobs.Task(task_key="each"), stats=stats)


def _iteration_runs(loop_run_id: int, states: Sequence[str]) -> list[jobs.RunTask]:
    runs: list[jobs.RunTask] = []
    for position, state in enumerate(states):
        iteration_run_id = loop_run_id * 1000 + position
        runs.append(
            jobs.RunTask(
                task_key="each",
                run_id=iteration_run_id,
                attempt_number=0,
                state=_state(state),
                status=jobs.RunStatus(state=TERMINATED, termination_details=_details(state)),
                run_page_url=f"{HOST}/jobs/runs/{iteration_run_id}",
                start_time=1_000,
                end_time=2_000,
            )
        )
    return runs


def _state(state: str | None) -> jobs.RunState:
    if state is None:
        return jobs.RunState(life_cycle_state=SKIPPED)
    return jobs.RunState(result_state=jobs.RunResultState(state))


def _details(state: str | None) -> jobs.TerminationDetails:
    if state is None:
        return jobs.TerminationDetails(code=jobs.TerminationCodeCode.RUN_EXECUTION_ERROR)
    codes = {
        "FAILED": "RUN_EXECUTION_ERROR",
        "UPSTREAM_FAILED": "SKIPPED",
        "CANCELED": "USER_CANCELED",
        "EXCLUDED": "SKIPPED",
        "TIMEDOUT": "RUN_EXECUTION_ERROR",
    }
    return jobs.TerminationDetails(code=jobs.TerminationCodeCode(codes.get(state, state)))


def notebook_export(commands: Sequence[dict[str, Any]]) -> str:
    """Build the HTML of a notebook view as `jobs.export_run` returns it, with a notebook model of `commands`.

    Like the real export, the model is JSON, URL-encoded as `encodeURIComponent` does, then base64-encoded.
    """
    model = json.dumps({"version": "NotebookV1", "name": "notebook", "language": "python", "commands": commands})
    encoded = base64.b64encode(urllib.parse.quote(model, safe="!~*'()").encode()).decode()
    return (
        "<!DOCTYPE html>\n<html>\n<head>\n<title>notebook - Databricks</title>\n"
        f"<script>var __DATABRICKS_NOTEBOOK_MODEL = '{encoded}';</script>\n</head>\n<body></body>\n</html>"
    )


QUERY_FILE = "/Workspace/Users/someone@example.com/.bundle/lakeflow_sql/test/files/src/queries/count.sql"
# The raw `runs/get-output` of the `sql` bundle's file task, as the workspace returned it. databricks-sdk does not
# model `sql_output.file_output`, so `jobs.RunOutput.from_dict` drops it.
SQL_FILE_OUTPUT = {
    "sql_output": {
        "file_output": {
            "query_result": {
                "columns": [{"name": "n", "type": "int"}],
                "rows": [{"values": [{"string_value": "1"}]}],
                "truncated": False,
            },
            "query_text": "SELECT 1 AS n\n",
            "sql_statements": [{"lookup_key": "CiQwMWYx"}],
            "warehouse_id": "0123456789abcdef",
        }
    }
}
