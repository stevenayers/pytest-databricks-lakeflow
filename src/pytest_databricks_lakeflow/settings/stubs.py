"""Stub tasks that stand in for upstream tasks, and probe tasks that capture the task values a task sets."""

from __future__ import annotations

import dataclasses
import json
import re
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from databricks.sdk.service import jobs

from pytest_databricks_lakeflow.errors import LakeflowError
from pytest_databricks_lakeflow.limits import MAX_KEY_CHARS, MAX_VALUE_CHARS
from pytest_databricks_lakeflow.naming import (
    KEYS_WIDGET,
    PROBE_PREFIX,
    SETUP_KEY,
    TASK_KEY_WIDGET,
    VALUES_WIDGET,
)
from pytest_databricks_lakeflow.settings.compute import Helpers, add_helper_compute, on_helper_compute
from pytest_databricks_lakeflow.settings.for_each import for_each_bodies
from pytest_databricks_lakeflow.settings.references import (
    OUTPUT_REFERENCE,
    TASK_REFERENCE,
    TASK_VALUES_REFERENCE,
    escape,
)
from pytest_databricks_lakeflow.settings.tasks import find_task, require_task, tasks_by_key, user_task_keys

if TYPE_CHECKING:
    from pytest_databricks_lakeflow.results.models import JobRun


@dataclass(frozen=True)
class Stub:
    """What a stub task stands in for: the task values it sets and the output references it answers.

    A plain mapping in `upstream_task_values` is the same as `Stub(values=mapping)`.

    Attributes:
        values: The task values the stub sets, by key, as a plain mapping gives them.
        outputs: Literals for the `{{tasks.<key>.output.<path>}}` references to the stubbed task, by the path after
            `output.` exactly as the reference spells it, such as `"first_row.n"`, `"alert_state"` or `"rows"`. A
            string is substituted verbatim and any other value as compact JSON, so `{"rows": [{"n": "3"}]}` gives
            `[{"n":"3"}]`, the platform's own spelling.
    """

    values: Mapping[str, object] = dataclasses.field(default_factory=dict)
    outputs: Mapping[str, object] = dataclasses.field(default_factory=dict)


Upstream = Mapping[str, Mapping[str, object] | Stub]


def stub_upstream(settings: jobs.JobSettings, upstream_task_values: Upstream, helpers: Helpers) -> jobs.JobSettings:
    """Replace the named tasks with stub tasks that set the given task values, each in its task's place.

    A stub keeps the key of the task it replaces, so `depends_on` entries and `{{tasks.<key>.values.<name>}}`
    references to it resolve against it. Every other task's `{{tasks.<key>.output.<path>}}` references to it are
    replaced by the stub's outputs. It drops the task's `depends_on`, so the tasks upstream of it no longer hold it
    back, but depends on the setup task when the job has one, as every task without `depends_on` does. Every entry is
    checked before any task is replaced.

    Args:
        settings: The job's settings. They are changed in place.
        upstream_task_values: The task values each stub sets, or a `Stub` with its values and outputs, by the key of
            the task it replaces.
        helpers: Where the helper notebooks are and what they run on.

    Returns:
        `settings`.

    Raises:
        ValueError: If a value is too large or not JSON-serialisable, or an output contains `{{` or is not
            JSON-serialisable.
        LakeflowError: If a key is not a task in the job, or a task refers to an output its stub does not give.
    """
    if not upstream_task_values:
        return settings
    tasks = tasks_by_key(settings)
    for key in upstream_task_values:
        if key not in tasks:
            available = user_task_keys(settings)
            raise LakeflowError(f"Unknown task {key!r} in upstream_task_values. Available: {available}")
    stubs = as_stubs(upstream_task_values)
    replaced: list[jobs.Task] = []
    for task in settings.tasks or []:
        if task.task_key not in stubs:
            replaced.append(_rewrite_outputs(task, stubs))
            continue
        stub = _stub_task(task.task_key, stubs[task.task_key].values, helpers)
        if SETUP_KEY in tasks:
            stub.depends_on = [jobs.TaskDependency(task_key=SETUP_KEY)]
        replaced.append(stub)
    settings.tasks = replaced
    add_helper_compute(settings, helpers)
    return settings


def as_stubs(upstream_task_values: Upstream) -> dict[str, Stub]:
    """Turn each plain mapping of task values into a `Stub`, and check every stub's values and outputs.

    Raises:
        ValueError: If a value is too large or not JSON-serialisable, or an output contains `{{` or is not
            JSON-serialisable.
    """
    stubs: dict[str, Stub] = {}
    for key, stub in upstream_task_values.items():
        if not isinstance(stub, Stub):
            stub = Stub(values=stub)
        check_values(key, stub.values)
        for path in stub.outputs:
            _render_output(key, stub, path)
        stubs[key] = stub
    return stubs


def _rewrite_outputs(task: jobs.Task, stubs: Mapping[str, Stub]) -> jobs.Task:
    """Get the task with its output references to the stubbed tasks replaced by the stubs' outputs.

    The task is not changed. It is returned as it is when it has no such reference.

    Raises:
        LakeflowError: If the task refers to an output that its stub does not give.
    """

    def replacement(match: re.Match[str]) -> str:
        key, path = match[1], match[2]
        if key not in stubs:
            return match[0]
        if path not in stubs[key].outputs:
            raise LakeflowError(
                f"Task {task.task_key!r} refers to {match[0]}, but the stub for {key!r} has no output {path!r}. "
                f"Pass Stub(outputs={{{path!r}: ...}}) for {key!r} in upstream_task_values."
            )
        # The text is JSON, so the literal is JSON-string-escaped to keep its quotes and backslashes.
        return json.dumps(_render_output(key, stubs[key], path))[1:-1]

    text = json.dumps(task.as_dict())
    rewritten = OUTPUT_REFERENCE.sub(replacement, text)
    if rewritten == text:
        return task
    return jobs.Task.from_dict(json.loads(rewritten))


def _render_output(key: str, stub: Stub, path: str) -> str:
    value = stub.outputs[path]
    if isinstance(value, str):
        rendered = value
    else:
        try:
            # Compact, as the platform spells rows and first_row: {"n":"1","m":""}, with non-ASCII text unescaped.
            rendered = json.dumps(value, separators=(",", ":"), ensure_ascii=False)
        except (TypeError, ValueError) as e:
            raise ValueError(f"Output {path!r} of stub {key!r} is not JSON-serialisable: {e}") from None
    # The platform substitutes the literal and would then resolve a reference inside it.
    if "{{" in rendered:
        raise ValueError(
            f"Output {path!r} of stub {key!r} contains '{{{{', which the platform would resolve as a reference: "
            f"{rendered!r}. Stubbed outputs are literals."
        )
    return rendered


def inject_probes(
    settings: jobs.JobSettings, capture_task_values: Mapping[str, Sequence[str]], helpers: Helpers
) -> jobs.JobSettings:
    """Add a probe task for each task in `capture_task_values`.

    Args:
        settings: The job's settings. They are changed in place.
        capture_task_values: The task value keys to capture, by task key.
        helpers: Where the helper notebooks are and what they run on.

    Returns:
        `settings`.

    Raises:
        ValueError: If a key is longer than 512 characters.
        LakeflowError: If a task is not in the job, is a for-each task or is nested in one.
    """
    if not capture_task_values:
        return settings
    tasks = tasks_by_key(settings)
    _check_not_nested(settings, capture_task_values)
    unknown = sorted(set(capture_task_values) - set(tasks))
    if unknown:
        raise LakeflowError(
            f"capture_task_values names tasks that are not in the job: {unknown}. Available: {sorted(tasks)}"
        )
    for target, value_keys in capture_task_values.items():
        _check_capturable(tasks[target])
        _check_keys(target, value_keys)
    probes: list[jobs.Task] = []
    for target, value_keys in capture_task_values.items():
        probes.append(_probe_task(target, value_keys, helpers))
    settings.tasks = [*(settings.tasks or []), *probes]
    add_helper_compute(settings, helpers)
    return settings


def wrap_task(
    settings: jobs.JobSettings,
    task_key: str,
    upstream_task_values: Upstream,
    capture_task_values: Sequence[str],
    helpers: Helpers,
) -> jobs.JobSettings:
    """Reduce the job to one task, a stub task for each upstream task in `upstream_task_values`, and a probe.

    A stub has the key of the task it replaces, so the target's `{{tasks.<key>.values.<name>}}` references and
    `taskValues.get` calls resolve against it. The target's `{{tasks.<key>.output.<path>}}` references are replaced by
    the stub's outputs.

    Args:
        settings: The job's settings. They are changed in place.
        task_key: The task to keep.
        upstream_task_values: The task values each stub sets, or a `Stub` with its values and outputs, by upstream
            task key.
        capture_task_values: The task value keys to capture from the task. No probe is added if empty.
        helpers: Where the helper notebooks are and what they run on.

    Returns:
        `settings`.

    Raises:
        ValueError: If `upstream_task_values` names the task under test, a value is too large or not
            JSON-serialisable, or an output contains `{{` or is not JSON-serialisable.
        LakeflowError: If the task is not in the job, the task refers to a task that is not stubbed (its task
            values or any other `{{tasks.<key>.` reference) or to an output its stub does not give, or
            `capture_task_values` is given for a for-each task.
    """
    target = require_task(settings, task_key)
    if task_key in upstream_task_values:
        raise ValueError(f"upstream_task_values cannot stub {task_key!r}, the task under test")
    text = json.dumps(target.as_dict())
    missing_values = sorted(set(TASK_VALUES_REFERENCE.findall(text)) - set(upstream_task_values))
    if missing_values:
        raise LakeflowError(
            f"Task {task_key!r} refers to task values of {missing_values} in its settings. Pass them in "
            "upstream_task_values."
        )
    # Unlike run(only=...), which leaves a task in the job and lets these references resolve against its DISABLED
    # run, task() drops the task from the job, so jobs/create would refuse any reference to it.
    missing = sorted(set(TASK_REFERENCE.findall(text)) - set(upstream_task_values))
    if missing:
        raise LakeflowError(
            f"Task {task_key!r} refers to {missing} in its settings, which task() leaves out of the job, and "
            "jobs/create refuses a reference to a task that is not in the job. Pass them in upstream_task_values."
        )
    stubs = as_stubs(upstream_task_values)
    target = _rewrite_outputs(target, stubs)
    if capture_task_values:
        _check_capturable(target)
    _check_keys(task_key, capture_task_values)
    target.depends_on = [jobs.TaskDependency(task_key=key) for key in stubs] or None
    stub_tasks = [_stub_task(key, stub.values, helpers) for key, stub in stubs.items()]
    probes = []
    if capture_task_values:
        probes.append(_probe_task(task_key, capture_task_values, helpers))
    settings.tasks = [*stub_tasks, target, *probes]
    if stub_tasks or probes:
        add_helper_compute(settings, helpers)
    return settings


def recover_injected(settings: jobs.JobSettings) -> tuple[frozenset[str], dict[str, list[str]]]:
    """Read back the stubs and probes that `stub_upstream`, `inject_probes` or `wrap_task` added to a created copy.

    `json.loads` undoes `escape`, so a value key containing `{{` comes back as it was written.

    Args:
        settings: The settings of a created copy, as `jobs/get` returns them.

    Returns:
        The keys of the stub tasks, and the value keys each probe captures, by the key of the task it watches.
    """
    stubs: set[str] = set()
    capture: dict[str, list[str]] = {}
    for task in settings.tasks or []:
        params = task.notebook_task.base_parameters if task.notebook_task else None
        if not params or not task.task_key:
            continue
        if VALUES_WIDGET in params:
            stubs.add(task.task_key)
        elif task.task_key.startswith(PROBE_PREFIX) and TASK_KEY_WIDGET in params and KEYS_WIDGET in params:
            capture[params[TASK_KEY_WIDGET]] = list(json.loads(params[KEYS_WIDGET]))
    return frozenset(stubs), capture


def upstream_values_from(
    job_key: str,
    settings: jobs.JobSettings,
    task_key: str,
    run: JobRun,
    explicit: Collection[str] = (),
    *,
    argument: str = "upstream_from",
) -> dict[str, dict[str, object]]:
    """Take the stub values for a task's upstream tasks from the task values a previous run captured.

    The upstream tasks are those the task depends on in `settings`. The platform returns no task values with a run,
    so only values that a probe captured, with `capture_task_values`, are in `run`.

    Args:
        job_key: The bundle resource key of the job the task belongs to.
        settings: The job's settings, before the task is wrapped.
        task_key: The task to replay.
        run: A run of the job that captured the upstream tasks' values.
        explicit: Upstream task keys whose values are given some other way, which are not taken from `run`.
        argument: The name of the argument that passed `run`, for the messages.

    Returns:
        The captured values of each upstream task, by task key. Empty if the job has no task `task_key`.

    Raises:
        ValueError: If `run` is a run of another job.
        LakeflowError: If an upstream task was not part of `run` or has no captured values.
    """
    if run.job_key != job_key:
        raise ValueError(f"{argument} is a run of job {run.job_key!r}, not of {job_key!r}")
    target = find_task(settings, task_key)
    if target is None:
        return {}
    upstream: dict[str, dict[str, object]] = {}
    for dependency in target.depends_on or []:
        key = dependency.task_key
        if key in explicit:
            continue
        if key not in run.tasks:
            raise LakeflowError(_no_upstream_values(argument, key, task_key, "was not part of the run"))
        values = run.tasks[key].task_values
        if not values:
            raise LakeflowError(_no_upstream_values(argument, key, task_key, "has no captured task values"))
        upstream[key] = dict(values)
    return upstream


def _no_upstream_values(argument: str, key: str, task_key: str, reason: str) -> str:
    return (
        f"{argument} has no task values for {key!r}, upstream of {task_key!r}: it {reason}. Run the job with "
        f"capture_task_values={{{key!r}: [...]}} first, or pass its values in upstream_task_values."
    )


def _probe_task(target_key: str, keys: Sequence[str], helpers: Helpers) -> jobs.Task:
    notebook = jobs.NotebookTask(
        notebook_path=helpers.get_values,
        base_parameters={TASK_KEY_WIDGET: target_key, KEYS_WIDGET: escape(list(keys))},
    )
    task = jobs.Task(
        task_key=f"{PROBE_PREFIX}{target_key}",
        depends_on=[jobs.TaskDependency(task_key=target_key)],
        run_if=jobs.RunIf.ALL_DONE,
        notebook_task=notebook,
    )
    return on_helper_compute(task, helpers)


def _stub_task(task_key: str, values: Mapping[str, object], helpers: Helpers) -> jobs.Task:
    notebook = jobs.NotebookTask(notebook_path=helpers.set_values, base_parameters={VALUES_WIDGET: escape(values)})
    return on_helper_compute(jobs.Task(task_key=task_key, notebook_task=notebook), helpers)


def check_values(task_key: str, values: Mapping[str, object]) -> None:
    """Fail early on stub values that `taskValues.set` would reject at run time.

    Args:
        task_key: The stub's task key, for error messages.
        values: The values the stub will set.

    Raises:
        ValueError: If a key is longer than 512 characters, or a value is not JSON-serialisable or is longer than
            49,152 characters as JSON.
    """
    _check_keys(task_key, values)
    for key, value in values.items():
        try:
            size = len(json.dumps(value))
        except (TypeError, ValueError) as e:
            raise ValueError(f"Value {key!r} for task {task_key!r} is not JSON-serialisable: {e}") from None
        if size > MAX_VALUE_CHARS:
            raise ValueError(
                f"Value {key!r} for task {task_key!r} is {size} characters as JSON. A task value allows "
                f"{MAX_VALUE_CHARS}."
            )


def _check_capturable(task: jobs.Task) -> None:
    # The platform refuses taskValues.set in an iteration, so a probe on a for-each task finds every key missing.
    if task.for_each_task is not None:
        raise LakeflowError(
            f"capture_task_values names the for-each task {task.task_key!r}, whose iterations cannot set task values. "
            "Return the values with dbutils.notebook.exit() and read each iteration's output from "
            f"run.task({task.task_key!r}).iterations instead."
        )


def _check_not_nested(settings: jobs.JobSettings, capture_task_values: Mapping[str, Sequence[str]]) -> None:
    # A nested task runs only as the loop's iterations, which cannot set task values either.
    for loop_key, body_key in for_each_bodies(settings).items():
        if body_key in capture_task_values:
            raise LakeflowError(
                f"capture_task_values names the task {body_key!r}, nested in the for-each task {loop_key!r}, whose "
                "iterations cannot set task values. Return the values with dbutils.notebook.exit() and read each "
                f"iteration's output from run.task({loop_key!r}).iterations instead."
            )


def _check_keys(task_key: str, keys: Collection[str]) -> None:
    for key in keys:
        if len(key) > MAX_KEY_CHARS:
            raise ValueError(f"Task value key {key[:40]!r}... of task {task_key!r} is longer than {MAX_KEY_CHARS}")
