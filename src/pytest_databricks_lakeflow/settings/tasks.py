"""Find and edit the tasks of a job's settings, the task nested in a `for_each_task` included."""

from __future__ import annotations

import logging
from collections.abc import Collection, Mapping, Sequence

from databricks.sdk.service import jobs

from pytest_databricks_lakeflow.errors import LakeflowError
from pytest_databricks_lakeflow.naming import PREFIX

log = logging.getLogger("pytest_databricks_lakeflow")
TASK_OVERRIDES = frozenset({"timeout_seconds", "max_retries", "min_retry_interval_millis", "retry_on_timeout"})


def with_nested(tasks: Sequence[jobs.Task] | None) -> list[jobs.Task]:
    result = list(tasks or [])
    for task in list(result):
        if task.for_each_task and task.for_each_task.task:
            result.append(task.for_each_task.task)
    return result


def enabled_tasks(settings: jobs.JobSettings, *, skip: Collection[str] = ()) -> list[jobs.Task]:
    """Get the tasks that are not disabled, nested tasks included unless they or their `for_each_task` are.

    The top-level tasks in `skip` are left out too, with the task nested in each.
    """
    enabled = [task for task in settings.tasks or [] if not task.disabled and task.task_key not in skip]
    return [task for task in with_nested(enabled) if not task.disabled]


def reachable(start: str, edges: Mapping[str, set[str]]) -> set[str]:
    seen: set[str] = set()
    pending = [start]
    while pending:
        for key in edges.get(pending.pop(), set()) - seen:
            seen.add(key)
            pending.append(key)
    return seen


def tasks_by_key(settings: jobs.JobSettings) -> dict[str, jobs.Task]:
    """Get the job's top-level tasks by key, in order."""
    return {task.task_key: task for task in settings.tasks or []}


def user_task_keys(settings: jobs.JobSettings, *, exclude: Collection[str] = ()) -> list[str]:
    """Get the sorted keys of the job's top-level tasks that the plugin did not inject, less `exclude`."""
    keys: list[str] = []
    for task in settings.tasks or []:
        if not task.task_key.startswith(PREFIX) and task.task_key not in exclude:
            keys.append(task.task_key)
    return sorted(keys)


def require_task(settings: jobs.JobSettings, task_key: str) -> jobs.Task:
    """Get the job's top-level task `task_key`, raising `LakeflowError` with the available keys if there is none."""
    target = find_task(settings, task_key)
    if target is None:
        available = sorted(t.task_key for t in settings.tasks or [])
        raise LakeflowError(f"No task {task_key!r} in the job. Available: {available}")
    return target


def find_task(settings: jobs.JobSettings, task_key: str) -> jobs.Task | None:
    for task in settings.tasks or []:
        if task.task_key == task_key:
            return task
    return None


def disable_tasks(settings: jobs.JobSettings, keys: Sequence[str]) -> jobs.JobSettings:
    """Set `disabled` on the named tasks, so the job skips them.

    Args:
        settings: The job's settings. They are changed in place.
        keys: The task keys to disable.

    Returns:
        `settings`.

    Raises:
        LakeflowError: If a key is not a task in the job.
    """
    tasks = tasks_by_key(settings)
    for key in keys:
        if key not in tasks:
            raise LakeflowError(f"Unknown task {key!r} in disable. Available: {sorted(tasks)}")
    for key in keys:
        tasks[key].disabled = True
    return settings


def disabled_task_keys(settings: jobs.JobSettings) -> set[str]:
    """Get the keys of the disabled tasks and of every task downstream of one, whatever its `run_if`.

    The platform disables a task downstream of a disabled task ("This task was disabled because it is downstream of
    a disabled task"), and the run does not say which task was disabled in the job: `RunTask.disabled` is always
    null. A task listed here that the platform ran anyway still ends in a result state other than `DISABLED`.

    Args:
        settings: The job's settings.

    Returns:
        The keys of the top-level tasks whose `disabled` is true, and of the top-level tasks that depend on one of
        them, directly or through other tasks.
    """
    tasks = settings.tasks or []
    downstream: dict[str, set[str]] = {task.task_key: set() for task in tasks}
    for task in tasks:
        for dependency in task.depends_on or []:
            downstream.setdefault(dependency.task_key, set()).add(task.task_key)
    disabled: set[str] = set()
    for task in tasks:
        if task.disabled:
            disabled.add(task.task_key)
            disabled |= reachable(task.task_key, downstream)
    return disabled


def override_tasks(settings: jobs.JobSettings, overrides: Mapping[str, Mapping[str, object]]) -> jobs.JobSettings:
    """Set the timeout and retry fields in `TASK_OVERRIDES` on the named tasks.

    Every entry is checked before any task is changed.

    Args:
        settings: The job's settings. They are changed in place.
        overrides: The fields to set and their values, by task key.

    Returns:
        `settings`.

    Raises:
        ValueError: If a field is not in `TASK_OVERRIDES`.
        LakeflowError: If a key is not a task in the job.
    """
    tasks = tasks_by_key(settings)
    allowed = sorted(TASK_OVERRIDES)
    for key, values in overrides.items():
        if key not in tasks:
            raise LakeflowError(f"Unknown task {key!r} in task_overrides. Available: {sorted(tasks)}")
        for name in values:
            if name not in TASK_OVERRIDES:
                raise ValueError(f"Unknown field {name!r} in task_overrides[{key!r}]. Allowed: {allowed}")
    for key, values in overrides.items():
        for name, value in values.items():
            setattr(tasks[key], name, value)
    return settings


def force_conditions(settings: jobs.JobSettings, outcomes: Mapping[str, bool]) -> jobs.JobSettings:
    """Rewrite the named condition tasks so each one evaluates to the given outcome.

    Each task's operands become `true == true` or `true == false`, which `EQUAL_TO` compares as strings. Its
    `depends_on` and `run_if` are kept, so the tasks upstream of it still run. The original operands are logged.

    Args:
        settings: The job's settings. They are changed in place.
        outcomes: The outcome each condition task must produce, by task key.

    Returns:
        `settings`.

    Raises:
        ValueError: If an outcome is not a bool.
        LakeflowError: If a key is not a task in the job, its task has no `condition_task`, or the task is disabled
            or downstream of a disabled task.
    """
    if not outcomes:
        return settings
    tasks = tasks_by_key(settings)
    conditions = sorted(key for key, task in tasks.items() if task.condition_task)
    disabled = disabled_task_keys(settings)
    for key, outcome in outcomes.items():
        if key not in tasks:
            raise LakeflowError(f"Unknown task {key!r} in force_conditions. Available: {sorted(tasks)}")
        if tasks[key].condition_task is None:
            raise LakeflowError(
                f"Task {key!r} in force_conditions has no condition_task. Condition tasks: {conditions}"
            )
        if key in disabled:
            # A disabled condition task, or one downstream of a disabled task, which the platform disables too, is
            # never evaluated, so forcing its outcome would do nothing.
            raise LakeflowError(f"Task {key!r} in force_conditions is disabled, so its condition is never evaluated")
        if not isinstance(outcome, bool):
            # bool("false") is True, so a string would silently force the true branch.
            raise ValueError(f"force_conditions[{key!r}] must be True or False, got {outcome!r}")
    for key, outcome in outcomes.items():
        _force_condition(tasks[key], outcome)
    return settings


def _force_condition(task: jobs.Task, outcome: bool) -> None:
    condition = task.condition_task
    assert condition is not None
    expected = str(outcome).lower()
    # The SDK reads an operator it does not know as None, although `op` is typed as always set.
    op = getattr(condition.op, "value", None)
    log.info(
        f"Forcing condition task {task.task_key!r} to {expected}; it compared {condition.left!r} {op} "
        f"{condition.right!r}"
    )
    task.condition_task = jobs.ConditionTask(op=jobs.ConditionTaskOp.EQUAL_TO, left="true", right=expected)


def run_job_tasks(settings: jobs.JobSettings) -> list[jobs.Task]:
    """Get the enabled tasks that run another job, tasks nested in a `for_each_task` included.

    `sanitise` keeps their `run_job_task` as deployed, so they trigger the deployed job, not a copy. A disabled
    task, a task downstream of one and a task nested in a disabled `for_each_task` does not run, so it is left out.

    Args:
        settings: The job's settings.

    Returns:
        Every enabled task with a `run_job_task`, in the order of `settings.tasks`, nested tasks last.
    """
    return [task for task in enabled_tasks(settings, skip=disabled_task_keys(settings)) if task.run_job_task]


def pipeline_task_ids(settings: jobs.JobSettings) -> list[str]:
    """Get the pipelines that the enabled pipeline tasks update, tasks nested in a `for_each_task` included.

    A disabled task, a task downstream of one and a task nested in a disabled `for_each_task` does not run, so its
    pipeline is left out, as `run_job_tasks` leaves such tasks out.

    Args:
        settings: The job's settings.

    Returns:
        Each pipeline id once, in the order of `settings.tasks`, nested tasks last.
    """
    ids: list[str] = []
    for task in enabled_tasks(settings, skip=disabled_task_keys(settings)):
        if task.pipeline_task is None or not task.pipeline_task.pipeline_id:
            continue
        if task.pipeline_task.pipeline_id not in ids:
            ids.append(task.pipeline_task.pipeline_id)
    return ids


def rewire_pipelines(settings: jobs.JobSettings, copies: Mapping[str, str]) -> jobs.JobSettings:
    """Point every pipeline task at the copy of its pipeline, tasks nested in a `for_each_task` included.

    Args:
        settings: The job's settings. They are changed in place.
        copies: The id of each copy, by the id of the pipeline it copies. A task on another pipeline is left alone.

    Returns:
        `settings`.
    """
    for task in with_nested(settings.tasks):
        if task.pipeline_task is not None and task.pipeline_task.pipeline_id in copies:
            task.pipeline_task.pipeline_id = copies[task.pipeline_task.pipeline_id]
    return settings
