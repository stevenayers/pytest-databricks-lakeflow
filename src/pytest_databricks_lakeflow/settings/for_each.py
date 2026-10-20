"""Run a for-each body on its own, and replace the inputs a for-each task iterates over."""

from __future__ import annotations

import json
import math
from collections.abc import Collection, Mapping, Sequence
from decimal import Decimal

from databricks.sdk.service import jobs

from pytest_databricks_lakeflow.errors import LakeflowError
from pytest_databricks_lakeflow.limits import MAX_FOR_EACH_INPUTS_CHARS, MAX_PARAMETERS_JSON_CHARS
from pytest_databricks_lakeflow.naming import INPUT_PARAMETER
from pytest_databricks_lakeflow.settings.references import INPUT_REFERENCE, escape, point_at_parameters
from pytest_databricks_lakeflow.settings.tasks import require_task, tasks_by_key


def for_each_bodies(settings: jobs.JobSettings) -> dict[str, str]:
    """Get the key of the task nested in each top-level `for_each_task`, by the key of its for-each task."""
    bodies: dict[str, str] = {}
    for task in settings.tasks or []:
        if task.for_each_task and task.for_each_task.task:
            bodies[task.task_key] = task.for_each_task.task.task_key
    return bodies


def lift_nested_task(settings: jobs.JobSettings, loop_key: str, nested_key: str) -> jobs.Task:
    """Replace the for-each task `loop_key` with the task nested in it, in the same place, so it runs once.

    The nested task keeps all of its own settings and takes the for-each task's `depends_on`, which `upstream_from`
    reads to choose the stubs. The for-each task's `inputs`, `concurrency` and other fields are dropped.

    Args:
        settings: The job's settings. They are changed in place.
        loop_key: The key of the top-level task with the `for_each_task`.
        nested_key: The key of the task nested in it.

    Returns:
        The nested task, now a top-level task of `settings`.

    Raises:
        LakeflowError: If `loop_key` is not a task of the job, has no `for_each_task`, or nests a task with another
            key. Nothing is changed then.
    """
    loop = require_task(settings, loop_key)
    bodies = [f"{key}.{body}" for key, body in for_each_bodies(settings).items()]
    body = loop.for_each_task.task if loop.for_each_task else None
    if body is None:
        raise LakeflowError(
            f"Task {loop_key!r} has no for_each_task, so {f'{loop_key}.{nested_key}'!r} names no nested task. "
            f"For-each bodies: {bodies}"
        )
    if body.task_key != nested_key:
        raise LakeflowError(
            f"for_each_task {loop_key!r} nests {body.task_key!r}, not {nested_key!r}. For-each bodies: {bodies}"
        )
    # The body runs where its loop sits in the graph, so it takes the loop's `depends_on`: `upstream_values_from`
    # finds the tasks to stub from it, and `wrap_task` replaces them with the stubs.
    body.depends_on = loop.depends_on
    settings.tasks = [body if task is loop else task for task in settings.tasks or []]
    return body


def override_for_each_inputs(
    settings: jobs.JobSettings, inputs: Mapping[str, Sequence[object]], *, stubs: Collection[str] = ()
) -> jobs.JobSettings:
    """Replace the `inputs` of the named for-each tasks with the JSON of the given lists.

    Each list is encoded with `escape`, as the stub widgets are, so a `{{` in a value is not read as a reference.
    Every entry is checked before any task is changed.

    Args:
        settings: The job's settings. They are changed in place.
        inputs: The list each for-each task iterates over, by task key.
        stubs: Keys of the tasks `upstream_task_values` replaced with stubs, which have no `for_each_task`.

    Returns:
        `settings`.

    Raises:
        ValueError: If a list is not a list, is not JSON-serialisable, is longer than 5,000 characters as JSON, holds
            `None` or a nested list, or mixes objects with primitives.
        LakeflowError: If a key is not a task in the job, is stubbed, or its task has no `for_each_task`.
    """
    tasks = tasks_by_key(settings)
    loops = sorted(key for key, task in tasks.items() if task.for_each_task)
    encoded: dict[str, str] = {}
    for key, values in inputs.items():
        if key not in tasks:
            raise LakeflowError(f"Unknown task {key!r} in for_each_inputs. For-each tasks: {loops}")
        if key in stubs:
            raise LakeflowError(
                f"Task {key!r} in for_each_inputs is stubbed by upstream_task_values, so it has no for_each_task. "
                f"For-each tasks: {loops}"
            )
        if key not in loops:
            raise LakeflowError(f"Task {key!r} in for_each_inputs has no for_each_task. For-each tasks: {loops}")
        encoded[key] = _for_each_inputs(key, values)
    for key, text in encoded.items():
        loop = tasks[key].for_each_task
        assert loop is not None
        loop.inputs = text
    return settings


def _for_each_inputs(key: str, values: Sequence[object]) -> str:
    if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
        # list("ab") would iterate over the characters.
        raise ValueError(f"for_each_inputs[{key!r}] must be a list, got {values!r}")
    try:
        text = escape(list(values))
    except (TypeError, ValueError) as e:
        raise ValueError(f"Inputs for task {key!r} in for_each_inputs are not JSON-serialisable: {e}") from None
    if len(text) > MAX_FOR_EACH_INPUTS_CHARS:
        raise ValueError(
            f"Inputs for task {key!r} in for_each_inputs are {len(text)} characters as JSON. A for-each task allows "
            f"{MAX_FOR_EACH_INPUTS_CHARS}."
        )
    _check_for_each_items(key, values)
    return text


def _check_for_each_items(key: str, values: Sequence[object]) -> None:
    """Fail on the items that jobs/create refuses in a for-each task's inputs, given values that are JSON-serialisable.

    jobs/create refuses `[null]`, `[[1, 2]]`, `[1, {"k": 1}]` and `[{"k": 1}, "a"]` with "The JSON string in the inputs
    field should be an array of primitives or objects.", and accepts `[1, "a"]` and objects holding lists, objects or
    null.
    """
    first = None
    for index, value in enumerate(values):
        kind = _item_kind(value)
        found = f"{kind} at index {index}"
        if kind in ("None", "a nested list"):
            raise ValueError(_for_each_items_message(key, found))
        if first is None:
            first = kind
        elif kind != first:
            raise ValueError(_for_each_items_message(key, f"{found} after {first} at index 0"))


def _item_kind(value: object) -> str:
    if value is None:
        return "None"
    if isinstance(value, Mapping):
        return "an object"
    if isinstance(value, (str, int, float)):
        return "a primitive"
    # Anything else that serialises to JSON is a list or a tuple.
    return "a nested list"


def _for_each_items_message(key: str, found: str) -> str:
    return (
        f"Inputs for task {key!r} in for_each_inputs hold {found}. jobs/create takes a list of all primitives or all "
        "objects, without null or nested lists."
    )


def inject_input(settings: jobs.JobSettings, task_key: str, value: object | None) -> jobs.JobSettings:
    """Point the `{{input}}` references of one task at job parameters whose defaults hold `value`.

    `{{input}}` becomes `{{job.parameters.__pytest_input}}` and `{{input.<key>}}` becomes
    `{{job.parameters.__pytest_input_<key>}}`. Each parameter is declared with the text a for-each iteration would
    give the reference: a string as it is, an integer item as written, and anything else as the platform writes JSON.
    A definition of the same name in the template is replaced, and the rest are appended after the job's own, in the
    order of their keys.

    Args:
        settings: The job's settings, holding `task_key` as a top-level task. They are changed in place.
        task_key: The task, a for-each body that `lift_nested_task` lifted.
        value: The iteration input, a string, number, boolean or mapping. `None` means none was given.

    Returns:
        `settings`.

    Raises:
        ValueError: If `value` is not a string, number, boolean or mapping, or a rendered value contains `{{` or is
            not JSON-serialisable.
        LakeflowError: If the task refers to `{{input}}` and `value` is `None`, `value` is given and the task does
            not refer to it, or a key is not in `value`. Nothing is changed in either case.
    """
    target = require_task(settings, task_key)
    text = json.dumps(target.as_dict())
    keys = sorted(set(INPUT_REFERENCE.findall(text)))
    if not keys:
        if value is not None:
            raise LakeflowError(f"Task {task_key!r} does not refer to {{{{input}}}}, so input= has no effect")
        return settings
    if value is None:
        references = " and ".join(_input_reference(key) for key in keys)
        raise LakeflowError(f"Task {task_key!r} refers to {references} in its settings. Pass input= to lakeflow.task()")
    if not isinstance(value, (str, int, float, Mapping)):
        raise ValueError(
            f"input for task {task_key!r} is {value!r}. A for-each item is a string, number, boolean or object."
        )
    definitions: list[jobs.JobParameterDefinition] = []
    for key in keys:
        default = _render_input(task_key, key, value)
        definitions.append(jobs.JobParameterDefinition(name=_input_parameter(key), default=default))
    names = {definition.name for definition in definitions}
    kept: list[jobs.JobParameterDefinition] = []
    for parameter in settings.parameters or []:
        if parameter.name not in names:
            kept.append(parameter)
    rewritten = point_at_parameters(text, INPUT_REFERENCE, _input_parameter)
    lifted = jobs.Task.from_dict(json.loads(rewritten))
    settings.tasks = [lifted if task is target else task for task in settings.tasks or []]
    settings.parameters = [*kept, *definitions]
    return settings


def check_input_size(settings: jobs.JobSettings, task_key: str) -> None:
    """Fail early when the job parameters, the input that `inject_input` declared among them, are too long.

    jobs/create limits the job parameters as a whole, so this runs once every parameter of the copy is declared: the
    trigger parameters and the test marker too.

    Args:
        settings: The job's settings, after `inject_input`.
        task_key: The task `inject_input` gave the input to, for the message.

    Raises:
        LakeflowError: If the job parameters are longer than 10,000 characters as JSON.
    """
    size = _parameters_json_chars(settings.parameters or [])
    if size > MAX_PARAMETERS_JSON_CHARS:
        raise LakeflowError(
            f"input for task {task_key!r} makes the job parameters {size} characters as JSON. jobs/create allows "
            f"{MAX_PARAMETERS_JSON_CHARS}."
        )


def _parameters_json_chars(parameters: Sequence[jobs.JobParameterDefinition]) -> int:
    size = 2 + max(len(parameters) - 1, 0)
    for parameter in parameters:
        size += 29 + len(_json_escaped(parameter.name or "")) + len(_json_escaped(parameter.default or ""))
    return size


def _json_escaped(text: str) -> str:
    return json.dumps(text, ensure_ascii=False)[1:-1]


def _input_reference(key: str) -> str:
    if not key:
        return "{{input}}"
    return f"{{{{input.{key}}}}}"


def _input_parameter(key: str) -> str:
    if not key:
        return INPUT_PARAMETER
    return f"{INPUT_PARAMETER}_{key}"


def _render_input(task_key: str, key: str, value: object) -> str:
    """Get the text a for-each iteration over the item `value` gives `{{input}}`, or `{{input.<key>}}` with a key."""
    reference = _input_reference(key)
    if not key:
        part = value
    elif not isinstance(value, Mapping):
        raise LakeflowError(f"{reference} in task {task_key!r} needs an object as input, got {value!r}")
    elif key not in value:
        raise LakeflowError(f"{reference} in task {task_key!r} needs key {key!r}, but input has {list(value)}")
    else:
        part = value[key]
    try:
        text = _rendered_item(part, whole=not key)
    except (TypeError, ValueError, OverflowError) as e:
        raise ValueError(f"input for task {task_key!r} is not JSON-serialisable: {e}") from None
    if "{{" in text:
        raise ValueError(f"input for task {task_key!r} contains '{{{{', which the platform would substitute")
    return text


def _rendered_item(value: object, *, whole: bool) -> str:
    # A for-each iteration gives a string as it is and an integer item as written, but turns every number in an
    # object into a Java double: {"id": 7} gives {{input}} '{"id":7.0}' and {{input.id}} '7.0'. The lifted body gets
    # the same text, so it parses the input as it would in its loop.
    if isinstance(value, str):
        return value
    if whole and isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    return _platform_json(value)


def _platform_json(value: object) -> str:
    """Write `value` as JSON the way a for-each iteration does: compact, non-ASCII as it is, numbers as Java doubles."""
    if value is None or isinstance(value, (bool, str)):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, (int, float)):
        return _java_double(float(value))
    if isinstance(value, Mapping):
        members: list[str] = []
        for key, member in value.items():
            if not isinstance(key, str):
                raise TypeError(f"keys must be str, not {type(key).__name__}")
            members.append(f"{json.dumps(key, ensure_ascii=False)}:{_platform_json(member)}")
        return "{" + ",".join(members) + "}"
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(_platform_json(member) for member in value) + "]"
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _java_double(number: float) -> str:
    """Write a number as Java's `Double.toString` does: `7.0`, `0.1`, `1.0E20`, `1.2345678E7`, `1.0E-5`."""
    if not math.isfinite(number):
        raise ValueError(f"Out of range float values are not JSON compliant: {number!r}")
    if number == 0 or 1e-3 <= abs(number) < 1e7:
        # repr writes these without an exponent, with at least one digit after the point, as Java does.
        return repr(number)
    sign, digits, exponent = Decimal(repr(number)).normalize().as_tuple()
    assert isinstance(exponent, int)
    figures = "".join(str(digit) for digit in digits)
    mantissa = f"{figures[0]}.{figures[1:] or '0'}"
    return f"{'-' if sign else ''}{mantissa}E{exponent + len(figures) - 1}"
