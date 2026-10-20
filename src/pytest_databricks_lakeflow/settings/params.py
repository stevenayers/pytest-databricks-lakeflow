"""Job and task parameters of the copy, the parameters the plugin declares, and the options of one run."""

from __future__ import annotations

import json
import warnings
from collections.abc import Collection, Mapping, Sequence
from dataclasses import fields
from typing import Any

from databricks.sdk.service import jobs

from pytest_databricks_lakeflow.errors import LakeflowError
from pytest_databricks_lakeflow.limits import MAX_PARAMETER_DEFAULT_BYTES
from pytest_databricks_lakeflow.naming import PREFIX, TEST_ID_PARAMETER, TRIGGER_PARAM_PREFIX, trim
from pytest_databricks_lakeflow.settings.references import TRIGGER_REFERENCE, point_at_parameters
from pytest_databricks_lakeflow.settings.tasks import with_nested

# The field that holds each task type's parameters, for task_params given as a mapping, which is merged, and as a
# list, which replaces it. A Python wheel task takes either.
MAPPING_PARAMS = {"notebook_task": "base_parameters", "python_wheel_task": "named_parameters", "sql_task": "parameters"}
LIST_PARAMS = {
    "spark_python_task": "parameters",
    "spark_jar_task": "parameters",
    "python_wheel_task": "parameters",
    "dbt_task": "commands",
}
TASK_PARAMS_KINDS = tuple(sorted(MAPPING_PARAMS.keys() | LIST_PARAMS.keys()))
PIPELINE_PARAMS_FIELDS = frozenset(f.name for f in fields(jobs.PipelineParams))


def declared_params(settings: jobs.JobSettings) -> list[str]:
    """The names of the parameters the job declares, in order."""
    names: list[str] = []
    for parameter in settings.parameters or []:
        if parameter.name:
            names.append(parameter.name)
    return names


def check_params(job_key: str, settings: jobs.JobSettings, params: Mapping[str, str], allow_undeclared: bool) -> None:
    reserved = sorted(key for key in params if key.startswith(PREFIX))
    if reserved:
        raise ValueError(f"Parameter names starting with {PREFIX!r} are reserved for the plugin: {reserved}")
    declared = {p.name for p in settings.parameters or []}
    undeclared = sorted(set(params) - declared)
    if undeclared and not allow_undeclared:
        raise LakeflowError(
            f"Job {job_key!r} does not declare the parameters {undeclared}. Declared: {sorted(declared)}. "
            "Pass allow_undeclared_params=True to Lakeflow to pass them anyway."
        )


def check_task_params(
    job_key: str,
    settings: jobs.JobSettings,
    task_params: Mapping[str, Mapping[str, str] | Sequence[str]],
    params: Mapping[str, str],
) -> None:
    """Check the parameter names of `task_params`, and warn about each one that is also a job parameter.

    A job parameter is pushed to every task that takes named parameters and wins over a task parameter of the same
    name, so such a task parameter has no effect. It may be declared on purpose, so this warns rather than raises.
    Lists have no names to check.

    Args:
        job_key: The bundle resource key of the job, for the messages.
        settings: The job's settings.
        task_params: The task parameters, by task key.
        params: The job parameters passed for every run.

    Raises:
        ValueError: If a parameter name starts with `__pytest_`.
    """
    named: list[tuple[str, str]] = []
    for task_key, value in task_params.items():
        if not isinstance(value, Mapping):
            continue
        for name in value:
            # apply_task_params rejects a name that is not a string, naming the shape the task takes.
            if isinstance(name, str):
                named.append((task_key, name))
    reserved = sorted(name for _, name in named if name.startswith(PREFIX))
    if reserved:
        raise ValueError(f"Parameter names starting with {PREFIX!r} are reserved for the plugin: {reserved}")
    job_parameters = set(params) | {p.name for p in settings.parameters or []}
    for task_key, name in named:
        if name in job_parameters:
            warnings.warn(
                f"task_params for {task_key!r} sets {name!r}, also a job parameter of {job_key!r}. The job value wins.",
                stacklevel=3,
            )


def apply_task_params(
    job_key: str,
    settings: jobs.JobSettings,
    task_params: Mapping[str, Mapping[str, str] | Sequence[str]],
    *,
    allowed: Collection[str] | None = None,
) -> jobs.JobSettings:
    """Set the parameters of the named tasks, in the shape each task type takes.

    A mapping is merged into the task's named parameters, so the template's `{{tasks.<key>.values.<name>}}`
    references survive. A list replaces the task's positional parameters, which have no names to merge on. A mapping
    for a Python wheel task clears its positional parameters and a list clears its named parameters, because the
    Jobs API takes one or the other. Values are passed through unchanged. A task nested in a `for_each_task` is named
    by its own key, and every iteration gets its parameters.

    Args:
        job_key: The bundle resource key of the job, for the messages.
        settings: The job's settings. They are changed in place.
        task_params: A mapping of names to strings or a list of strings, by task key.
        allowed: The task keys that `task_params` may name. Defaults to every task of the job, tasks nested in a
            `for_each_task` included.

    Returns:
        `settings`.

    Raises:
        ValueError: If a value does not fit its task's type, or gives a dbt task an empty list of commands.
        LakeflowError: If a key is not an allowed task of the job, or names a task that is not a notebook, Python
            script, JAR, Python wheel, SQL or dbt task. Nothing is changed in either case. For a `for_each_task` whose
            nested task may be named, the message names the nested task.
    """
    if not task_params:
        return settings
    tasks: dict[str, jobs.Task] = {}
    for task in with_nested(settings.tasks):
        if task.task_key.startswith(PREFIX):
            continue
        if allowed is not None and task.task_key not in allowed:
            continue
        tasks[task.task_key] = task
    for key in task_params:
        if key not in tasks:
            raise LakeflowError(f"Unknown task {key!r} in task_params of job {job_key!r}. Available: {sorted(tasks)}")
    kinds: dict[str, str] = {}
    for key, value in task_params.items():
        kinds[key] = _task_params_kind(tasks[key], value, tasks)
    for key, value in task_params.items():
        _set_task_params(getattr(tasks[key], kinds[key]), kinds[key], value)
    return settings


def _task_params_kind(task: jobs.Task, value: object, nameable: Collection[str]) -> str:
    """Get the field of the task type that `value` edits, checking that `value` fits it."""
    for kind in TASK_PARAMS_KINDS:
        if getattr(task, kind) is not None:
            _check_task_params_shape(task, kind, value)
            return kind
    advice = "Change it with modify_settings instead."
    nested = task.for_each_task.task if task.for_each_task else None
    if nested is not None and nested.task_key in nameable:
        advice = f"Name its nested task, {nested.task_key!r}, instead."
    raise LakeflowError(
        f"task_params cannot set the parameters of task {task.task_key!r}: it is not a notebook, Python script, "
        f"JAR, Python wheel, SQL or dbt task. {advice}"
    )


def _check_task_params_shape(task: jobs.Task, kind: str, value: object) -> None:
    fits = False
    if isinstance(value, Mapping):
        fits = kind in MAPPING_PARAMS and all(isinstance(k, str) and isinstance(v, str) for k, v in value.items())
    elif isinstance(value, Sequence) and not isinstance(value, str):
        fits = kind in LIST_PARAMS and all(isinstance(item, str) for item in value)
    if fits and kind == "dbt_task" and not value:
        raise ValueError(
            f"task_params[{task.task_key!r}]: dbt commands must not be empty. Give task {task.task_key!r} at least "
            "one command, such as ['dbt build']."
        )
    if fits:
        return
    shapes: list[str] = []
    if kind in MAPPING_PARAMS:
        shapes.append(f"a mapping of names to strings, merged into {kind}.{MAPPING_PARAMS[kind]}")
    if kind in LIST_PARAMS:
        shapes.append(f"a list of strings, which replaces {kind}.{LIST_PARAMS[kind]}")
    raise ValueError(
        f"task_params[{task.task_key!r}] must be {', or '.join(shapes)}, for task {task.task_key!r}; got {value!r}"
    )


def _set_task_params(code: Any, kind: str, value: Mapping[str, str] | Sequence[str]) -> None:
    if isinstance(value, Mapping):
        field = MAPPING_PARAMS[kind]
        setattr(code, field, {**(getattr(code, field) or {}), **value})
        if kind == "python_wheel_task":
            code.parameters = None
        return
    setattr(code, LIST_PARAMS[kind], list(value))
    if kind == "python_wheel_task":
        code.named_parameters = None


def inject_test_marker(settings: jobs.JobSettings, nodeid: str) -> jobs.JobSettings:
    """Declare the `__pytest_test_id` job parameter with the test's node id as its default.

    The default is the value of every run, so job code can read it to tell that it runs under test. A definition of
    the same name in the template is replaced.

    Args:
        settings: The job's settings. They are changed in place.
        nodeid: The node id of the test, trimmed to 4096 UTF-8 bytes.

    Returns:
        `settings`.
    """
    kept = [p for p in settings.parameters or [] if p.name != TEST_ID_PARAMETER]
    marker = jobs.JobParameterDefinition(name=TEST_ID_PARAMETER, default=trim(nodeid, MAX_PARAMETER_DEFAULT_BYTES))
    settings.parameters = [*kept, marker]
    return settings


def inject_trigger(settings: jobs.JobSettings, trigger: Mapping[str, str]) -> jobs.JobSettings:
    """Point `{{job.trigger.<path>}}` references at job parameters that hold simulated values.

    No API starts a run as if a trigger fired, so each reference whose path is a key of `trigger` becomes
    `{{job.parameters.__pytest_trigger_<path>}}`, with each `.` of the path as `_`, and that parameter is declared with
    the value as its default. A reference whose path is not a key is left as it is and named in a `UserWarning`.

    Args:
        settings: The job's settings. They are not changed.
        trigger: The value of each reference, by its path after `job.trigger.`, such as `"type"` or
            `"table_update.updated_tables"`.

    Returns:
        The rewritten settings, with the parameters appended after the job's own, or `settings` when `trigger` is
        empty.

    Raises:
        ValueError: If a value is not a string.
        LakeflowError: If a key is not the path of a reference in the settings.
    """
    if not trigger:
        return settings
    text = json.dumps(settings.as_dict())
    used = sorted(set(TRIGGER_REFERENCE.findall(text)))
    for path, value in trigger.items():
        if path not in used:
            references = [f"{{{{job.trigger.{name}}}}}" for name in used]
            raise LakeflowError(
                f"trigger names {path!r}, but the job has no {{{{job.trigger.{path}}}}} reference. Trigger references "
                f"in the job: {references}"
            )
        if not isinstance(value, str):
            raise ValueError(f"trigger[{path!r}] must be a string, the value a job parameter holds, got {value!r}")
    left = [f"job.trigger.{path}" for path in used if path not in trigger]
    if left:
        warnings.warn(
            f"The job still refers to {left}, which trigger does not set. A run-now run resolves job.trigger.type to "
            "one_time and job.trigger.time.* to the time of the run, and jobs/create refuses table_update and "
            "file_arrival references in a job without that trigger. Add them to trigger to pin them.",
            stacklevel=3,
        )

    def name_for(path: str) -> str | None:
        if path not in trigger:
            return None
        return _trigger_parameter(path)

    rewritten = jobs.JobSettings.from_dict(json.loads(point_at_parameters(text, TRIGGER_REFERENCE, name_for)))
    definitions: list[jobs.JobParameterDefinition] = []
    for path, value in trigger.items():
        definitions.append(jobs.JobParameterDefinition(name=_trigger_parameter(path), default=value))
    rewritten.parameters = [*(rewritten.parameters or []), *definitions]
    return rewritten


def _trigger_parameter(path: str) -> str:
    return f"{TRIGGER_PARAM_PREFIX}{path.replace('.', '_')}"


def pipeline_params(
    settings: jobs.JobSettings,
    *,
    full_refresh: bool | None,
    full_refresh_selection: Sequence[str] | None,
    refresh_selection: Sequence[str] | None,
    refresh_flow_selection: Sequence[str] | None,
    reset_checkpoint_selection: Sequence[str] | None,
) -> jobs.PipelineParams | None:
    """Build the pipeline refresh options of one run.

    Args:
        settings: The job's settings.
        full_refresh: Whether pipeline tasks run a full refresh.
        full_refresh_selection: The tables that pipeline tasks fully refresh.
        refresh_selection: The tables that pipeline tasks refresh.
        refresh_flow_selection: The flows that pipeline tasks refresh.
        reset_checkpoint_selection: The streaming flows whose checkpoints pipeline tasks reset.

    Returns:
        The options for `run_now`, or `None` when every argument is `None`.

    Raises:
        ValueError: If a selection is a string rather than a list of names.
        LakeflowError: If an argument is set and no task, nested `for_each_task` tasks included, is a pipeline task,
            or a selection is set and the installed databricks-sdk cannot send it.
    """
    selections = {
        "full_refresh_selection": full_refresh_selection,
        "refresh_selection": refresh_selection,
        "refresh_flow_selection": refresh_flow_selection,
        "reset_checkpoint_selection": reset_checkpoint_selection,
    }
    if full_refresh is None and all(value is None for value in selections.values()):
        return None
    check_selections(selections)
    unsupported = sorted(k for k, v in selections.items() if v is not None and k not in PIPELINE_PARAMS_FIELDS)
    if unsupported:
        raise LakeflowError(
            "full_refresh_selection, refresh_selection, refresh_flow_selection and reset_checkpoint_selection need "
            f"databricks-sdk 0.110.0 or later; got {unsupported}. Upgrade databricks-sdk to pass them."
        )
    if not any(task.pipeline_task for task in with_nested(settings.tasks)):
        keys = sorted(task.task_key for task in settings.tasks or [] if task.task_key)
        raise LakeflowError(
            "full_refresh, full_refresh_selection, refresh_selection, refresh_flow_selection and "
            f"reset_checkpoint_selection apply to pipeline tasks, but the job has no pipeline task. Tasks: {keys}"
        )
    chosen = {key: list(value) for key, value in selections.items() if value is not None}
    return jobs.PipelineParams(full_refresh=full_refresh, **chosen)


def check_selections(selections: Mapping[str, Sequence[str] | None]) -> None:
    """Fail on a table or flow selection given as a string, which would be sent as a list of its characters.

    Raises:
        ValueError: If a selection is a string.
    """
    for key, value in selections.items():
        if isinstance(value, str):
            raise ValueError(f"{key} must be a list of names, not a string: {value!r}")
