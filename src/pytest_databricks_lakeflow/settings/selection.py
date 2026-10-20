"""What `run(only=...)` selects, and the references the copy cannot resolve."""

from __future__ import annotations

import json
from collections.abc import Collection, Sequence
from typing import Any

from databricks.sdk.service import jobs

from pytest_databricks_lakeflow.errors import LakeflowError
from pytest_databricks_lakeflow.naming import PROBE_PREFIX, SETUP_KEY, TEARDOWN_KEY
from pytest_databricks_lakeflow.settings.references import (
    OUTPUT_REFERENCE,
    TASK_VALUES_REFERENCE,
    TRIGGER_KINDS,
    TRIGGER_REFERENCE,
    own_fields,
)
from pytest_databricks_lakeflow.settings.tasks import disabled_task_keys, reachable, user_task_keys, with_nested


def select(
    settings: jobs.JobSettings, only: Sequence[str], stubs: Collection[str] = (), exclude: Collection[str] = ()
) -> list[str]:
    """Add the injected tasks that the selected tasks need to `only`.

    Those are the stubs that selected tasks depend on, the probes on selected tasks, and the setup and teardown tasks
    whenever the job has them.

    Args:
        settings: The job's settings.
        only: Task keys, where `+key`, `key+` and `+key+` select upstream and downstream tasks, as `run_now` does.
        stubs: Keys of the stub tasks.
        exclude: Keys of tasks to leave out of the run, such as the teardown task when it runs as the job closes.

    Returns:
        `only` followed by the injected tasks it does not already select. When `only` selects a task in `exclude`,
        every selected task by its key instead, in sorted order, without the disabled tasks and the tasks downstream
        of them.

    Raises:
        LakeflowError: If `only` names a task that is not in the job.
    """
    selected = selected_tasks(settings, only, stubs)
    needed = running_stubs(settings, selected, stubs)
    for task in settings.tasks or []:
        key = task.task_key
        if key.startswith(PROBE_PREFIX) and key.removeprefix(PROBE_PREFIX) in selected:
            needed.add(key)
        if key in (SETUP_KEY, TEARDOWN_KEY) and key not in exclude:
            needed.add(key)
    if selected.isdisjoint(exclude):
        return [*only, *sorted(needed - selected)]
    # `key+` reaches the teardown task, which depends on every leaf, and run_now has no way to leave one task out of a
    # `+` selection, so the selection is spelt out. The platform does not run a disabled task that a `+` reaches, nor
    # one downstream of it, so those stay out. `check_disabled` has already refused an `only` that names one.
    left_out = set(exclude) | disabled_task_keys(settings)
    return sorted((selected | needed) - left_out)


def selected_tasks(settings: jobs.JobSettings, only: Sequence[str], stubs: Collection[str] = ()) -> set[str]:
    """Get the keys of the tasks that `only` selects, with `+key`, `key+` and `+key+` expanded.

    Args:
        settings: The job's settings.
        only: Task keys, as `select` takes them.
        stubs: Keys of the stub tasks, which the error message leaves out of the available tasks.

    Returns:
        The keys of the selected tasks, without the injected tasks that `select` adds.

    Raises:
        LakeflowError: If `only` names a task that is not in the job.
    """
    upstream = {t.task_key: {d.task_key for d in t.depends_on or []} for t in settings.tasks or []}
    downstream = {key: {other for other, deps in upstream.items() if key in deps} for key in upstream}
    selected: set[str] = set()
    for entry in only:
        key = entry.strip("+")
        if key not in upstream:
            available = user_task_keys(settings, exclude=stubs)
            raise LakeflowError(f"Unknown task {key!r} in only. Available: {available}")
        selected.add(key)
        if entry.startswith("+"):
            selected |= reachable(key, upstream)
        if entry.endswith("+"):
            selected |= reachable(key, downstream)
    return selected


def running_stubs(settings: jobs.JobSettings, selected: Collection[str], stubs: Collection[str]) -> set[str]:
    """The stubs that run with the selected tasks: those a selected task depends on."""
    running: set[str] = set()
    for task in settings.tasks or []:
        if task.task_key not in selected:
            continue
        for dependency in task.depends_on or []:
            if dependency.task_key in stubs:
                running.add(dependency.task_key)
    return running


def check_disabled(settings: jobs.JobSettings, only: Sequence[str]) -> None:
    """Fail early when `only` names a task that is disabled or downstream of a disabled task.

    The platform runs a disabled task that `only` names, and with `key+` the tasks downstream of it, though jobs/get
    still shows it disabled. A `+` that only reaches a disabled task does not run it, so that is allowed.

    Args:
        settings: The job's settings.
        only: Task keys, as `select` takes them.

    Raises:
        LakeflowError: If a task that `only` names, with or without `+`, is one that `disabled_task_keys` returns.
    """
    disabled = disabled_task_keys(settings)
    named: set[str] = set()
    for entry in only:
        key = entry.strip("+")
        if key in disabled:
            named.add(key)
    if named:
        raise LakeflowError(
            f"only names {sorted(named)}, which are disabled in the copy or downstream of a disabled task, and the "
            "platform runs a disabled task that only names. Leave them out of only, or do not disable them."
        )


def check_references(settings: jobs.JobSettings, selected: Collection[str], stubs: Collection[str]) -> None:
    """Fail early when a selected task refers to the task values or the output of a task that the run leaves out.

    The platform does not check this when the run starts. It ends the run with life cycle state `INTERNAL_ERROR` and
    "Failed to resolve references: Task value referenced by 'tasks.<key>.values.<name>' was not found.", or, for an
    output, with termination code `INVALID_RUN_CONFIGURATION` and "Failed to resolve references: Cannot access output
    from task '<key>' because the task was skipped."

    Args:
        settings: The job's settings.
        selected: Keys of the tasks that `only` selects, as `selected_tasks` returns them.
        stubs: Keys of the stub tasks. `select` adds only those that a selected task depends on, so only they run.

    Raises:
        LakeflowError: If a selected task that is not a stub refers to the task values or the output of a task
            that is neither selected nor a stub that runs.
    """
    running = {*selected, *running_stubs(settings, selected, stubs)}
    for task in settings.tasks or []:
        if task.task_key not in selected or task.task_key in stubs:
            continue
        text = json.dumps(task.as_dict())
        missing = sorted(set(TASK_VALUES_REFERENCE.findall(text)) - running)
        if missing:
            raise LakeflowError(
                f"Task {task.task_key!r} refers to task values of {missing}, which only leaves out, so the platform "
                "could not resolve them. Select them in only, stub them with upstream_task_values on job(), or pass "
                "replay to run()."
            )
        missing = sorted({key for key, _ in OUTPUT_REFERENCE.findall(text)} - running)
        if missing:
            raise LakeflowError(
                f"Task {task.task_key!r} refers to the output of {missing}, which only leaves out, so the platform "
                "could not resolve it. Select them in only, or stub them with Stub(outputs=...) in "
                "upstream_task_values on job()."
            )


def check_output_references(job_key: str, settings: jobs.JobSettings) -> None:
    """Fail early when a task refers to the output of a notebook task, which Databricks refuses when the job is created.

    `jobs/create` refuses `{{tasks.<key>.output.<path>}}` to a notebook task with InvalidParameterValue "Unsupported
    task type for the 'output' namespace." A stub is a notebook task too, but its outputs have been replaced by then.

    Args:
        job_key: The bundle resource key of the job, for the message.
        settings: The job's settings, as they will be created.

    Raises:
        LakeflowError: If a task, nested tasks included, refers to the output of a notebook task.
    """
    tasks = with_nested(settings.tasks)
    notebooks = {task.task_key for task in tasks if task.notebook_task is not None}
    for task in tasks:
        for match in OUTPUT_REFERENCE.finditer(json.dumps(task.as_dict())):
            target = match[1]
            if target not in notebooks:
                continue
            raise LakeflowError(
                f"Task {task.task_key!r} of {job_key!r} refers to {match[0]}, but {target!r} is a notebook task, and "
                "Databricks refuses output references to notebook tasks (\"Unsupported task type for the 'output' "
                f'namespace."). Set a task value in the notebook and refer to it with '
                f"{{{{tasks.{target}.values.<name>}}}} instead."
            )


def check_trigger_references(job_key: str, settings: jobs.JobSettings) -> None:
    """Fail early when the copy keeps a table update or file arrival trigger reference that it cannot resolve.

    `sanitise` strips the template's trigger, so jobs/create refuses `{{job.trigger.table_update.*}}` and
    `{{job.trigger.file_arrival.*}}` in the copy unless `trigger=` rewrote them or `modify_settings` set that trigger.

    Args:
        job_key: The bundle resource key of the job, for the message.
        settings: The job's settings, as they will be created.

    Raises:
        LakeflowError: If a task, nested tasks included, or a job parameter's default refers to a trigger of a kind
            the settings do not have.
    """
    configured = _configured_triggers(settings.trigger)
    for task in with_nested(settings.tasks):
        _check_trigger_kinds(f"Task {task.task_key!r} of {job_key!r}", own_fields(task), configured)
    # jobs/create was seen refusing these references in tasks; a job parameter default is assumed to be checked alike.
    for parameter in settings.parameters or []:
        _check_trigger_kinds(f"Job parameter {parameter.name!r} of {job_key!r}", parameter.default, configured)


def _check_trigger_kinds(owner: str, fields: Any, configured: set[str]) -> None:
    for match in TRIGGER_REFERENCE.finditer(json.dumps(fields)):
        path = match[1]
        kind = path.split(".")[0]
        if kind not in TRIGGER_KINDS or kind in configured:
            continue
        raise LakeflowError(
            f"{owner} refers to {match[0]}, but the copy has no {TRIGGER_KINDS[kind]} trigger, and jobs/create "
            f"refuses the reference without one. Set its value with trigger={{{path!r}: ...}} on job() or task()."
        )


def _configured_triggers(trigger: jobs.TriggerSettings | None) -> set[str]:
    configured: set[str] = set()
    if trigger is None:
        return configured
    if trigger.table_update is not None:
        configured.add("table_update")
    if trigger.file_arrival is not None:
        configured.add("file_arrival")
    return configured
