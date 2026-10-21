"""Build a `JobRun` from a terminated run, reading each task's output, parameters and the rest from the Jobs API."""

from __future__ import annotations

import json
import logging
import os
import warnings
from collections.abc import Callable, Collection, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Literal, TypeVar, get_args

from databricks.sdk.errors.base import DatabricksError
from databricks.sdk.service import jobs, pipelines

from pytest_databricks_lakeflow.dbt import DbtArtifacts, read_artifacts
from pytest_databricks_lakeflow.naming import PREFIX, PROBE_PREFIX
from pytest_databricks_lakeflow.results.models import (
    ForEachStats,
    JobRun,
    PipelineUpdate,
    SqlResult,
    TaskError,
    TaskRun,
    TaskValues,
    Timing,
    duration,
    enum_value,
    first_line,
    ran,
)
from pytest_databricks_lakeflow.results.notebook import parse_notebook_export

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

log = logging.getLogger("pytest_databricks_lakeflow")
T = TypeVar("T")
R = TypeVar("R")
# How many task runs one process reads from the Jobs API at once, at most. The workspace allows 20 `runs/get-output`
# requests per second in all, so the budget below is shared between the xdist workers, which finish runs at the same
# time, and each gets `FETCH_BUDGET // workers` threads, at least one.
FETCH_THREADS = 8
FETCH_BUDGET = 16
NotebookExport = Literal["never", "failed", "always"]
NOTEBOOK_EXPORTS: tuple[str, ...] = get_args(NotebookExport)
RUN_OUTPUT_PATH = "/api/2.2/jobs/runs/get-output"


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
    dbt_artifacts_dir: Path | None = None,
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
        dbt_artifacts_dir: Where to download the artefacts of each dbt task run that reported a link, into a
            folder named after the task run's id, while the link is still valid. `None` downloads nothing.

    Returns:
        The run's outcome.
    """
    task_runs_by_key = _task_runs_by_key(run)
    every_attempt: list[jobs.RunTask] = []
    for task_runs in task_runs_by_key.values():
        every_attempt.extend(task_runs)
    dbt_commands = None
    if run.overriding_parameters is not None:
        dbt_commands = run.overriding_parameters.dbt_commands
    reading = _Reading(job_keys or {}, notebook_export, dbt_artifacts_dir, dbt_commands)
    built = _read_task_runs(ws, every_attempt, outputs or {}, reading)

    attempts: dict[str, list[TaskRun]] = {}
    for key, task_runs in task_runs_by_key.items():
        attempts[key] = [built[task_run.run_id or 0] for task_run in task_runs]
        latest = task_runs[-1]
        if latest.for_each_task is not None:
            attempts[key][-1] = _with_iterations(ws, attempts[key][-1], latest, reading)

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
        result_state=enum_value(run.state.result_state if run.state else None),
        termination_code=enum_value(details.code if details else None),
        termination_message=details.message if details else None,
        duration=duration(run.start_time, run.end_time),
        tasks=tasks,
        injected_tasks=injected,
        skipped_tasks=skipped_tasks,
        disabled_tasks=disabled_tasks,
        parameters=_job_parameters(run.job_parameters),
        timing=Timing.from_run(run),
        effective_performance_target=enum_value(run.effective_performance_target),
        trigger_type=enum_value(run.trigger),
    )


def _task_runs_by_key(run: jobs.Run) -> dict[str, list[jobs.RunTask]]:
    # Every attempt of a task, in attempt order, keyed by task key in the order the run lists the tasks.
    by_key: dict[str, list[jobs.RunTask]] = {}
    for task_run in run.tasks or []:
        by_key.setdefault(task_run.task_key or "", []).append(task_run)
    for task_runs in by_key.values():
        task_runs.sort(key=lambda t: t.attempt_number or 0)
    return by_key


@dataclass(frozen=True)
class _Reading:
    """How to read the task runs of a run, its iterations and the child runs of its run-job tasks alike.

    Attributes:
        job_keys: Bundle resource keys by job id, which name the child runs.
        notebook_export: Which notebook tasks to export.
        dbt_artifacts_dir: Where to download the artefacts of dbt tasks, or `None` for nowhere.
        dbt_commands: The `dbt_commands` the run was started with, which every dbt task in it ran, or `None`.
    """

    job_keys: Mapping[int, str]
    notebook_export: NotebookExport
    dbt_artifacts_dir: Path | None
    dbt_commands: Sequence[str] | None = None


def _read_task_runs(
    ws: WorkspaceClient,
    task_runs: Sequence[jobs.RunTask],
    outputs: Mapping[int, jobs.RunOutput],
    reading: _Reading,
) -> dict[int, TaskRun]:
    """Build a `TaskRun` for each task run, by run id, reading its output, resolved parameters and the rest.

    Only the outputs missing from `outputs` are fetched. A run-job task gets the child run it triggered, and a dbt task
    its artefacts when `reading` names a directory for them.
    """
    known_outputs = dict(outputs)
    unread: list[jobs.RunTask] = []
    ran_tasks: list[jobs.RunTask] = []
    for task_run in task_runs:
        if has_output(task_run) and (task_run.run_id or 0) not in known_outputs:
            unread.append(task_run)
        if ran(_result_state(task_run)):
            ran_tasks.append(task_run)
    known_outputs.update(fetch_outputs(ws, unread))
    resolved = fetch_resolved_values(ws, ran_tasks)
    sql_results = fetch_sql_results(ws, ran_tasks)
    pipeline_updates = fetch_pipeline_updates(ws, ran_tasks)
    dbt_artifacts: dict[int, DbtArtifacts] = {}
    if reading.dbt_artifacts_dir is not None:
        dbt_artifacts = fetch_dbt_artifacts(task_runs, known_outputs, reading.dbt_artifacts_dir)

    built: dict[int, TaskRun] = {}
    for task_run in task_runs:
        run_id = task_run.run_id or 0
        task = _task_run(
            task_run,
            known_outputs.get(run_id),
            resolved.get(run_id),
            sql_results.get(run_id),
            pipeline_updates.get(run_id),
            reading.dbt_commands,
        )
        if run_id in dbt_artifacts:
            task = replace(task, dbt=dbt_artifacts[run_id])
        built[run_id] = _with_child_run(ws, task, reading)
    return built


def _with_iterations(ws: WorkspaceClient, task: TaskRun, task_run: jobs.RunTask, reading: _Reading) -> TaskRun:
    loop = task_run.for_each_task
    stats = None
    if loop is not None and loop.stats is not None:
        stats = ForEachStats.from_stats(loop.stats)
    # A cancelled for-each task may have started iterations before it stopped.
    if not task.ran and not task.cancelled:
        return replace(task, for_each_stats=stats)
    iteration_runs = _iterations(ws, task_run)
    log.info(f"Read {len(iteration_runs)} iterations of task {task.task_key} (run {task.run_id})")
    built = _read_task_runs(ws, iteration_runs, {}, reading)
    grouped: list[list[TaskRun]] = []
    for iteration_run in iteration_runs:
        attempt = built[iteration_run.run_id or 0]
        # The platform lists each retry of an iteration right after the attempt before it, as an iteration run of its
        # own with a higher `attempt_number`. Only a retry's own `get_run` names its first attempt.
        if grouped and attempt.attempt > 0:
            grouped[-1].append(attempt)
        else:
            grouped.append([attempt])
    iterations = [_with_attempts(attempts[-1], attempts) for attempts in grouped]
    return replace(task, iterations=iterations, for_each_stats=stats)


def _iterations(ws: WorkspaceClient, task_run: jobs.RunTask) -> list[jobs.RunTask]:
    """List the iteration runs of a for-each task run, every attempt of each, in iteration order.

    The job run does not list them: only the for-each task run fetched by its own id does. runs/get pages them 100 at
    a time, and databricks-sdk's `jobs.get_run` follows `next_page_token` until it has them all.
    """
    try:
        loop = ws.jobs.get_run(task_run.run_id or 0)
    except DatabricksError as e:
        log.warning(f"Could not read the iterations of task {task_run.task_key} (run {task_run.run_id}): {e}")
        return []
    return list(loop.iterations or [])


def _with_child_run(ws: WorkspaceClient, task: TaskRun, reading: _Reading) -> TaskRun:
    if task.child_run_id is None or not task.ran:
        return task
    try:
        child = ws.jobs.get_run(task.child_run_id)
    except DatabricksError as e:
        log.warning(f"Could not read run {task.child_run_id} triggered by task {task.task_key}: {e}")
        return task
    job_id = child.job_id or 0
    key = reading.job_keys.get(job_id) or str(job_id)
    # `only` never applies to a child run, so every task of it that ended DISABLED is disabled in the child job.
    disabled = {t.task_key for t in child.tasks or [] if t.task_key}
    child_run = build_job_run(
        ws,
        child,
        job_key=key,
        job_id=job_id,
        disabled=disabled,
        job_keys=reading.job_keys,
        notebook_export=reading.notebook_export,
        dbt_artifacts_dir=reading.dbt_artifacts_dir,
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


def fetch_each(items: Sequence[T], fetch: Callable[[T], R | None]) -> list[tuple[T, R]]:
    """Call `fetch` on each of `items` in parallel, and pair each item with what it returned, leaving out `None`.

    Args:
        items: What to fetch for, such as task runs.
        fetch: Reads one item from the workspace. It returns `None` for an item that has nothing to read.

    Returns:
        The items that have something, each with its result, in the order of `items`.
    """
    if not items:
        return []
    with ThreadPoolExecutor(max_workers=fetch_threads()) as pool:
        results = list(pool.map(fetch, items))
    found: list[tuple[T, R]] = []
    for item, result in zip(items, results, strict=True):
        if result is not None:
            found.append((item, result))
    return found


def fetch_threads() -> int:
    """How many task runs this process reads at once: `FETCH_THREADS`, or its share of `FETCH_BUDGET` under xdist.

    pytest-xdist sets `PYTEST_XDIST_WORKER_COUNT` in every worker; a value that is not a number counts as one worker.
    """
    try:
        workers = int(os.environ.get("PYTEST_XDIST_WORKER_COUNT", "1"))
    except ValueError:
        workers = 1
    return max(1, min(FETCH_THREADS, FETCH_BUDGET // max(1, workers)))


def fetch_outputs(ws: WorkspaceClient, tasks: Sequence[jobs.RunTask]) -> dict[int, jobs.RunOutput]:
    """Read the output of each task run in parallel, by task run id. An output that cannot be read is empty."""

    def fetch(task: jobs.RunTask) -> jobs.RunOutput:
        try:
            return ws.jobs.get_run_output(task.run_id or 0)
        except DatabricksError as e:
            log.warning(f"Could not read the output of task {task.task_key} (run {task.run_id}): {e}")
            return jobs.RunOutput()

    return {task.run_id or 0: output for task, output in fetch_each(tasks, fetch)}


def fetch_dbt_artifacts(
    tasks: Sequence[jobs.RunTask], outputs: Mapping[int, jobs.RunOutput], directory: Path
) -> dict[int, DbtArtifacts]:
    """Download and parse the artefacts of each dbt task run in parallel, by task run id, into `directory`.

    Each task run whose output has a dbt artefacts link gets a folder named after its run id. The platform signs the
    link for 30 minutes, so this runs as soon as the run has finished. A task run whose artefacts cannot be read is
    left out, with a warning naming the task.
    """
    linked: list[tuple[jobs.RunTask, jobs.DbtOutput]] = []
    for task in tasks:
        output = outputs.get(task.run_id or 0)
        if output is not None and output.dbt_output is not None and output.dbt_output.artifacts_link:
            linked.append((task, output.dbt_output))

    def fetch(pair: tuple[jobs.RunTask, jobs.DbtOutput]) -> DbtArtifacts | None:
        task, output = pair
        return read_artifacts(task.task_key or "", output, directory / str(task.run_id or 0))

    return {task.run_id or 0: artifacts for (task, _), artifacts in fetch_each(linked, fetch)}


def fetch_notebook_exports(ws: WorkspaceClient, tasks: Sequence[TaskRun]) -> dict[str, str]:
    """Export the notebook of each task run in parallel, as HTML by task key.

    `jobs.export_run` works only on a notebook task run; on a job run or another task type it fails with
    `InvalidParameterValue`. A task run that cannot be exported, or whose export has no notebook view, is left out.
    """

    def fetch(task: TaskRun) -> str | None:
        try:
            export = ws.jobs.export_run(task.run_id, views_to_export=jobs.ViewsToExport.CODE)
        except DatabricksError as e:
            log.warning(f"Could not export the notebook of task {task.task_key} ({task.run_url}): {e}")
            return None
        for view in export.views or []:
            if view.type == jobs.ViewType.NOTEBOOK and view.content is not None:
                return view.content
        return None

    return {task.task_key: html for task, html in fetch_each(tasks, fetch)}


def fetch_resolved_values(ws: WorkspaceClient, tasks: Sequence[jobs.RunTask]) -> dict[int, jobs.ResolvedValues]:
    """Read the resolved parameters of each task run in parallel, by task run id.

    The Jobs API fills `resolved_values` only when a task run is fetched by its own run id with
    `include_resolved_values`; on the job run every task's `resolved_values` is null, whatever the compute or the API
    version. A task run that cannot be read, or that reports no resolved values, is left out.
    """

    def fetch(task: jobs.RunTask) -> jobs.ResolvedValues | None:
        try:
            run = ws.jobs.get_run(task.run_id or 0, include_resolved_values=True)
        except DatabricksError as e:
            log.warning(f"Could not read the parameters of task {task.task_key} (run {task.run_id}): {e}")
            return None
        return run.tasks[0].resolved_values if run.tasks else None

    return {task.run_id or 0: values for task, values in fetch_each(tasks, fetch)}


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

    def fetch(task: jobs.RunTask) -> SqlResult | None:
        try:
            output = ws.api_client.do("GET", RUN_OUTPUT_PATH, query={"run_id": task.run_id or 0})
        except DatabricksError as e:
            log.warning(f"Could not read the SQL result of task {task.task_key} (run {task.run_id}): {e}")
            return None
        if not isinstance(output, Mapping):
            return None
        return SqlResult.from_output(task.task_key or "", output)

    return {task.run_id or 0: result for task, result in fetch_each(file_tasks, fetch)}


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

    def fetch(pair: tuple[jobs.RunTask, str]) -> PipelineUpdate | None:
        return _pipeline_update(ws, *pair)

    return {task.run_id or 0: update for (task, _), update in fetch_each(pipeline_tasks, fetch)}


def _pipeline_update(ws: WorkspaceClient, task: jobs.RunTask, pipeline_id: str) -> PipelineUpdate | None:
    start = task.start_time or 0
    end = task.end_time or 0
    try:
        listed = _updates_since(ws, pipeline_id, start)
    except DatabricksError as e:
        log.warning(
            f"Could not read the updates of pipeline {pipeline_id} for task {task.task_key} (run {task.run_id}): {e}"
        )
        return None
    in_window: list[pipelines.UpdateInfo] = []
    for info in listed:
        if info.creation_time is not None and start <= info.creation_time <= end:
            in_window.append(info)
    if not in_window:
        log.warning(
            f"Found no update of pipeline {pipeline_id} started in the time window of task {task.task_key} (run "
            f"{task.run_id})"
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
    return first_line(str(error))


def _task_error(task: jobs.RunTask, out: jobs.RunOutput) -> TaskError | None:
    state = _result_state(task)
    if not ran(state):
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
    dbt_commands: Sequence[str] | None = None,
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
        termination_code=enum_value(details.code if details else None),
        termination_message=details.message if details else None,
        run_url=task.run_page_url,
        duration=duration(task.start_time, task.end_time),
        output=notebook.result if notebook else None,
        output_truncated=output_truncated,
        logs=out.logs or "",
        logs_truncated=logs_truncated,
        error=_task_error(task, out),
        attempt=task.attempt_number or 0,
        parameters=_parameters(resolved, dbt_commands),
        timing=Timing.from_run(task),
        effective_performance_target=enum_value(task.effective_performance_target),
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


def _parameters(
    resolved: jobs.ResolvedValues | None, dbt_commands: Sequence[str] | None = None
) -> dict[str, str] | list[str] | None:
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
        # A dbt task run's resolved_values hold the task's own commands, also in a run that run_now's dbt_commands
        # overrode and that ran those instead. Only the run's overriding_parameters name them.
        if dbt_commands is not None:
            return list(dbt_commands)
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
    return enum_value(task.state.result_state if task.state else None)
