"""Turn a run into the JSON that `--lakeflow-report-dir` writes to `report.json`."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict
from typing import TYPE_CHECKING, Any

from databricks.sdk.service import jobs

if TYPE_CHECKING:
    from pytest_databricks_lakeflow.dbt import DbtArtifacts
    from pytest_databricks_lakeflow.results.models import JobRun, StepResult, TaskError, TaskRun, Timing


def serialise_with_files(run: JobRun) -> tuple[dict[str, Any], dict[str, str]]:
    """Serialise a run as `JobRun.as_dict` does, and collect the logs and notebook HTML it names, by file name."""
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
        "setup": _serialise_step(run.setup),
        "teardown": _serialise_step(run.teardown),
    }


def _serialise_step(step: StepResult | None) -> dict[str, Any] | None:
    if step is None:
        return None
    return {
        "role": step.role,
        "ran_as": step.ran_as,
        "succeeded": step.succeeded,
        "output": step.output,
        "error": _serialise_error(step.error),
        "duration_seconds": step.duration.total_seconds(),
        "task_key": step.task.task_key if step.task is not None else None,
    }


def _serialise_task(task: TaskRun, run_id: int, stem: str, files: dict[str, str]) -> dict[str, Any]:
    # `stem` names the side files: the task key, plus the attempt number for an attempt that was retried and the
    # index for an iteration of a for-each task.
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
    iterations: list[dict[str, Any]] = []
    for index, iteration in enumerate(task.iterations):
        iterations.append(_serialise_task(iteration, run_id, f"{stem}.iteration-{index}", files))
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
        "dbt_output": _serialise_dbt_output(task.dbt_output),
        "dbt": _serialise_dbt(task.dbt),
        "run_job_output": _as_dict(task.run_job_output),
        "child_run_id": task.child_run_id,
        "child_run": child_run,
        "dashboard_output": _as_dict(task.dashboard_output),
        "clean_rooms_notebook_output": _as_dict(task.clean_rooms_notebook_output),
        "notebook_file": notebook_file,
        "sql_result": sql_result,
        "pipeline_update": pipeline_update,
        "iterations": iterations,
        "for_each_stats": asdict(task.for_each_stats) if task.for_each_stats is not None else None,
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


def _serialise_dbt_output(output: jobs.DbtOutput | None) -> dict[str, Any] | None:
    # The artifacts headers can carry a signature for the pre-signed link, valid for 30 minutes, so report.json keeps
    # only the link.
    data = _as_dict(output)
    if data is not None:
        data.pop("artifacts_headers", None)
    return data


def _serialise_dbt(artifacts: DbtArtifacts | None) -> dict[str, Any] | None:
    # run_results.json and manifest.json stay in artifacts_dir: a manifest can run to megabytes.
    if artifacts is None:
        return None
    results: dict[str, Any] = {}
    for unique_id, result in artifacts.results.items():
        results[unique_id] = asdict(result)
    return {
        "artifacts_dir": str(artifacts.artifacts_dir),
        "results": results,
        "failed_models": artifacts.failed_models,
        "all_passed": artifacts.all_passed,
    }


def _as_dict(output: Any) -> dict[str, Any] | None:
    # The task outputs are databricks-sdk dataclasses, which turn themselves into the API's JSON.
    if output is None:
        return None
    return output.as_dict()
