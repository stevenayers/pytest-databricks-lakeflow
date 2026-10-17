from __future__ import annotations

import inspect
import json
import re
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, fields
from typing import Any

from databricks.sdk.service import compute, jobs

from pytest_databricks_lakeflow.errors import LakeflowError
from pytest_databricks_lakeflow.helpers import KEYS_WIDGET, TASK_KEY_WIDGET, VALUES_WIDGET
from pytest_databricks_lakeflow.results import PREFIX, PROBE_PREFIX

STRIPPED = (
    "deployment",
    "edit_mode",
    "schedule",
    "trigger",
    "triggers",
    "continuous",
    "email_notifications",
    "webhook_notifications",
    "notification_settings",
    "run_as",
    "parent_path",
)
TASK_STRIPPED = ("email_notifications", "webhook_notifications", "notification_settings")
CREATE_PARAMETERS = frozenset(inspect.signature(jobs.JobsAPI.create).parameters) - {"self"}
NAME_PREFIX = "[pytest] "
MAX_NAME_BYTES = 4096
MAX_TAG_BYTES = 256
MAX_TAGS = 25
NEWLINES = re.compile(r"[\r\n]+")
ENV_KEY = f"{PREFIX}serverless"
MAX_VALUE_CHARS = 49152
MAX_KEY_CHARS = 512
TASK_REFERENCE = re.compile(r"\{\{\s*tasks\.([\w-]+)\.")


@dataclass(frozen=True)
class Helpers:
    """Where the helper notebooks are, and the compute that stub and probe tasks run on.

    Attributes:
        set_values: Workspace path of the notebook that sets task values.
        get_values: Workspace path of the notebook that reports task values.
        environment_version: Serverless environment version for helper tasks.
        job_cluster_key: A job cluster for helper tasks instead of serverless.
    """

    set_values: str
    get_values: str
    environment_version: str = "4"
    job_cluster_key: str | None = None


def sanitise(
    settings: jobs.JobSettings,
    name: str,
    tags: Mapping[str, str],
    *,
    disable_auto_optimization: bool,
    performance_optimized: bool,
) -> jobs.JobSettings:
    """Make a copied template safe to create as a throwaway job.

    Removes what would make the copy run on its own, notify people, run as someone else, or count as
    bundle-managed. Names the job, adds `tags` to the template's tags and allows one run at a time.

    Args:
        settings: A copy of a template. It is changed in place.
        name: The job name, without the `[pytest] ` prefix.
        tags: Tags to add. Values are made acceptable as tag values.
        disable_auto_optimization: Stop serverless from retrying failed code tasks.
        performance_optimized: Override the performance target with `PERFORMANCE_OPTIMIZED`.

    Returns:
        `settings`.

    Raises:
        LakeflowError: If the job would have more than 25 tags.
    """
    for attr in STRIPPED:
        setattr(settings, attr, None)
    for task in settings.tasks or []:
        for attr in TASK_STRIPPED:
            setattr(task, attr, None)
        if disable_auto_optimization and _serverless(task):
            task.disable_auto_optimization = True
    if performance_optimized:
        settings.performance_target = jobs.PerformanceTarget.PERFORMANCE_OPTIMIZED
    settings.max_concurrent_runs = 1
    settings.name = _trim(NAME_PREFIX + NEWLINES.sub(" ", name), MAX_NAME_BYTES)
    settings.tags = {**(settings.tags or {}), **{key: tag_value(value) for key, value in tags.items()}}
    if len(settings.tags) > MAX_TAGS:
        raise LakeflowError(
            f"The job would have {len(settings.tags)} tags, more than the {MAX_TAGS} allowed. "
            f"Remove {len(settings.tags) - MAX_TAGS} from the template: {sorted(settings.tags)}"
        )
    return settings


def tag_value(value: str) -> str:
    """Make a string acceptable as a job tag value: Latin-1, one line, at most 256 UTF-8 bytes."""
    latin1 = "".join(c if ord(c) < 256 else "?" for c in NEWLINES.sub(" ", value))
    return _trim(latin1, MAX_TAG_BYTES)


def _trim(text: str, max_bytes: int) -> str:
    return text.encode()[:max_bytes].decode(errors="ignore")


def _serverless(task: jobs.Task) -> bool:
    runs_code = task.notebook_task or task.spark_python_task or task.python_wheel_task
    on_cluster = task.job_cluster_key or task.new_cluster or task.existing_cluster_id
    on_warehouse = task.notebook_task and task.notebook_task.warehouse_id
    return bool(runs_code) and not on_cluster and not on_warehouse


def create_kwargs(settings: jobs.JobSettings) -> dict[str, Any]:
    """Get the settings fields that `jobs.create` accepts, leaving out `None` and empty lists."""
    return {
        f.name: value
        for f in fields(settings)
        if f.name in CREATE_PARAMETERS and (value := getattr(settings, f.name)) is not None and value != []
    }


def check_params(job_key: str, settings: jobs.JobSettings, params: Mapping[str, str], allow_undeclared: bool) -> None:
    reserved = sorted(key for key in params if key.startswith(PREFIX))
    if reserved:
        raise LakeflowError(f"Parameter names starting with {PREFIX!r} are reserved for the plugin: {reserved}")
    declared = {p.name for p in settings.parameters or []}
    undeclared = sorted(set(params) - declared)
    if undeclared and not allow_undeclared:
        raise LakeflowError(
            f"Job {job_key!r} does not declare the parameters {undeclared}. Declared: {sorted(declared)}. "
            "Set lakeflow_allow_undeclared_params = true to pass them anyway."
        )


def select(settings: jobs.JobSettings, only: Sequence[str], stubs: Collection[str] = ()) -> list[str]:
    """Add the injected tasks that the selected tasks need to `only`.

    Those are the stubs that selected tasks depend on and the probes on selected tasks.

    Args:
        settings: The job's settings.
        only: Task keys, where `+key`, `key+` and `+key+` select upstream and downstream tasks, as `run_now` does.
        stubs: Keys of the stub tasks.

    Returns:
        `only` followed by the injected tasks it does not already select.

    Raises:
        LakeflowError: If `only` names a task that is not in the job.
    """
    upstream = {t.task_key: {d.task_key for d in t.depends_on or []} for t in settings.tasks or []}
    downstream = {key: {other for other, deps in upstream.items() if key in deps} for key in upstream}
    selected: set[str] = set()
    for entry in only:
        key = entry.strip("+")
        if key not in upstream:
            available = sorted(k for k in upstream if not k.startswith(PREFIX) and k not in stubs)
            raise LakeflowError(f"Unknown task {key!r} in only. Available: {available}")
        selected.add(key)
        if entry.startswith("+"):
            selected |= _reachable(key, upstream)
        if entry.endswith("+"):
            selected |= _reachable(key, downstream)
    needed = {dep for key in selected for dep in upstream[key] if dep in stubs}
    needed |= {key for key in upstream if key.startswith(PROBE_PREFIX) and key.removeprefix(PROBE_PREFIX) in selected}
    return [*only, *sorted(needed - selected)]


def _reachable(start: str, edges: Mapping[str, set[str]]) -> set[str]:
    seen: set[str] = set()
    pending = [start]
    while pending:
        for key in edges[pending.pop()] - seen:
            seen.add(key)
            pending.append(key)
    return seen


def inject_probes(
    settings: jobs.JobSettings, capture_values: Mapping[str, Sequence[str]], helpers: Helpers
) -> jobs.JobSettings:
    """Add a probe task for each task in `capture_values`.

    Args:
        settings: The job's settings. They are changed in place.
        capture_values: The task value keys to capture, by task key.
        helpers: Where the helper notebooks are and what they run on.

    Returns:
        `settings`.

    Raises:
        LakeflowError: If a task is not in the job, or a key is longer than 512 characters.
    """
    if not capture_values:
        return settings
    keys = {t.task_key for t in settings.tasks or []}
    unknown = sorted(set(capture_values) - keys)
    if unknown:
        raise LakeflowError(f"capture_values names tasks that are not in the job: {unknown}. Available: {sorted(keys)}")
    for target, value_keys in capture_values.items():
        _check_keys(target, value_keys)
    probes = [_probe_task(target, value_keys, helpers) for target, value_keys in capture_values.items()]
    settings.tasks = [*(settings.tasks or []), *probes]
    _add_helper_compute(settings, helpers)
    return settings


def wrap_task(
    settings: jobs.JobSettings,
    task_key: str,
    upstream_values: Mapping[str, Mapping[str, object]],
    capture_values: Sequence[str],
    helpers: Helpers,
) -> jobs.JobSettings:
    """Reduce the job to one task, a stub task for each upstream task in `upstream_values`, and a probe.

    A stub has the key of the task it replaces, so the target's `{{tasks.<key>.values.<name>}}` references and
    `taskValues.get` calls resolve against it.

    Args:
        settings: The job's settings. They are changed in place.
        task_key: The task to keep.
        upstream_values: The task values each stub sets, by upstream task key.
        capture_values: The task value keys to capture from the task. No probe is added if empty.
        helpers: Where the helper notebooks are and what they run on.

    Returns:
        `settings`.

    Raises:
        LakeflowError: If the task is not in the job, the task refers to task values of a task that is not
            stubbed, or a value is too large or not JSON-serialisable.
    """
    target = next((t for t in settings.tasks or [] if t.task_key == task_key), None)
    if target is None:
        available = sorted(t.task_key for t in settings.tasks or [])
        raise LakeflowError(f"No task {task_key!r} in the job. Available: {available}")
    if task_key in upstream_values:
        raise LakeflowError(f"upstream_values cannot stub {task_key!r}, the task under test")
    referenced = set(TASK_REFERENCE.findall(json.dumps(target.as_dict())))
    missing = sorted(referenced - set(upstream_values))
    if missing:
        raise LakeflowError(
            f"Task {task_key!r} refers to task values of {missing} in its settings. Pass them in upstream_values."
        )
    for stub_key, values in upstream_values.items():
        check_values(stub_key, values)
    _check_keys(task_key, capture_values)
    target.depends_on = [jobs.TaskDependency(task_key=key) for key in upstream_values] or None
    stubs = [_stub_task(key, values, helpers) for key, values in upstream_values.items()]
    probes = [_probe_task(task_key, capture_values, helpers)] if capture_values else []
    settings.tasks = [*stubs, target, *probes]
    if stubs or probes:
        _add_helper_compute(settings, helpers)
    return settings


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
    return _on_helper_compute(task, helpers)


def _stub_task(task_key: str, values: Mapping[str, object], helpers: Helpers) -> jobs.Task:
    notebook = jobs.NotebookTask(notebook_path=helpers.set_values, base_parameters={VALUES_WIDGET: escape(values)})
    return _on_helper_compute(jobs.Task(task_key=task_key, notebook_task=notebook), helpers)


def escape(value: object) -> str:
    """Encode a value as JSON for a helper widget, escaping `{{` so the platform does not treat it as a reference."""
    return json.dumps(value).replace("{{", "{\\u007b")


def check_values(task_key: str, values: Mapping[str, object]) -> None:
    """Fail early on stub values that `taskValues.set` would reject at run time.

    Args:
        task_key: The stub's task key, for error messages.
        values: The values the stub will set.

    Raises:
        LakeflowError: If a key is longer than 512 characters, or a value is not JSON-serialisable or is longer
            than 49,152 characters as JSON.
    """
    _check_keys(task_key, values)
    for key, value in values.items():
        try:
            size = len(json.dumps(value))
        except (TypeError, ValueError) as e:
            raise LakeflowError(f"Value {key!r} for task {task_key!r} is not JSON-serialisable: {e}") from None
        if size > MAX_VALUE_CHARS:
            raise LakeflowError(
                f"Value {key!r} for task {task_key!r} is {size} characters as JSON. A task value allows "
                f"{MAX_VALUE_CHARS}."
            )


def _check_keys(task_key: str, keys: Collection[str]) -> None:
    for key in keys:
        if len(key) > MAX_KEY_CHARS:
            raise LakeflowError(f"Task value key {key[:40]!r}... of task {task_key!r} is longer than {MAX_KEY_CHARS}")


def _on_helper_compute(task: jobs.Task, helpers: Helpers) -> jobs.Task:
    if helpers.job_cluster_key:
        task.job_cluster_key = helpers.job_cluster_key
    else:
        task.environment_key = ENV_KEY
    return task


def _add_helper_compute(settings: jobs.JobSettings, helpers: Helpers) -> None:
    if helpers.job_cluster_key:
        clusters = sorted(c.job_cluster_key for c in settings.job_clusters or [])
        if helpers.job_cluster_key not in clusters:
            raise LakeflowError(
                f"lakeflow_helpers_compute is {helpers.job_cluster_key!r}, but the job has no job cluster with "
                f"that key. Available: {clusters}"
            )
    elif not any(env.environment_key == ENV_KEY for env in settings.environments or []):
        spec = compute.Environment.from_dict({"environment_version": helpers.environment_version})
        environment = jobs.JobEnvironment(environment_key=ENV_KEY, spec=spec)
        settings.environments = [*(settings.environments or []), environment]
