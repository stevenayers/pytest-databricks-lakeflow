from __future__ import annotations

import inspect
import json
import logging
import math
import re
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, fields
from datetime import timedelta
from typing import Any, Literal, get_args

from databricks.sdk.service import compute, jobs

from pytest_databricks_lakeflow.errors import LakeflowError
from pytest_databricks_lakeflow.helpers import KEYS_WIDGET, TASK_KEY_WIDGET, VALUES_WIDGET
from pytest_databricks_lakeflow.results import PREFIX, PROBE_PREFIX

log = logging.getLogger("pytest_databricks_lakeflow")

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
    "parent_path",
)
TASK_STRIPPED = ("email_notifications", "webhook_notifications", "notification_settings")
CREATE_PARAMETERS = frozenset(inspect.signature(jobs.JobsAPI.create).parameters) - {"self"}
NAME_PREFIX = "[pytest] "
RunAs = Literal["keep"] | str | jobs.JobRunAs | None
SideEffects = Literal["neutralise", "keep"]
SIDE_EFFECTS: tuple[str, ...] = get_args(SideEffects)
MAX_NAME_BYTES = 4096
MAX_TAG_BYTES = 256
MAX_TAGS = 25
MAX_DESCRIPTION_BYTES = 27700
ATTRIBUTION_TAGS = ("pytest_ci_url", "pytest_user", "pytest_git_sha", "pytest_git_branch")
NEWLINES = re.compile(r"[\r\n]+")
ENV_KEY = f"{PREFIX}serverless"
MAX_ENVIRONMENTS = 10
MAX_VALUE_CHARS = 49152
MAX_KEY_CHARS = 512
TASK_REFERENCE = re.compile(r"\{\{\s*tasks\.([\w-]+)\.")
TIMEOUT_GRACE_SECONDS = 60
PIPELINE_PARAMS_FIELDS = frozenset(f.name for f in fields(jobs.PipelineParams))


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
    run_as: RunAs = None,
    description: str | None = None,
    timeout: timedelta | None = None,
    side_effects: str = "neutralise",
    environment_version: str | None = None,
    budget_policy_id: str | None = None,
    existing_cluster_id: str | None = None,
) -> jobs.JobSettings:
    """Make a copied template safe to create as a throwaway job.

    Removes what would make the copy run on its own, notify people, or count as bundle-managed, and removes
    `run_as` unless `run_as` says otherwise. Pauses dashboard and SQL alert subscriptions unless `side_effects` is
    `"keep"`. Names the job, adds `tags` to the template's tags and allows one run at a time. Applies the compute
    overrides that are given.

    Args:
        settings: A copy of a template. It is changed in place.
        name: The job name, without the `[pytest] ` prefix.
        tags: Tags to add. Values are made acceptable as tag values. The attribution tags in `ATTRIBUTION_TAGS`
            are added only while the job has fewer than 25 tags, in that order; the others must fit.
        disable_auto_optimization: Stop serverless from retrying failed code tasks.
        performance_optimized: Override the performance target with `PERFORMANCE_OPTIMIZED`.
        run_as: `None` removes the template's `run_as`, so the job runs as its creator. `"keep"` keeps it. A
            `jobs.JobRunAs` replaces it. Any other string names a user when it contains `@`, and otherwise a
            service principal by its application id.
        description: Put before the template's description, separated by a blank line.
        timeout: The plugin's timeout for one run. The job's `timeout_seconds` becomes this plus
            `TIMEOUT_GRACE_SECONDS`, so the platform stops a run that the plugin can no longer cancel. A template
            with a smaller positive `timeout_seconds` keeps it.
        side_effects: `"neutralise"` sets `dashboard_task.subscription.paused` on dashboard tasks with a
            subscription and `sql_task.alert.pause_subscriptions` on SQL alert tasks. `"keep"` leaves them as the
            template has them.
        environment_version: The serverless environment version of every environment except the helper
            environment and those whose spec sets `base_environment`.
        budget_policy_id: The budget policy of the job.
        existing_cluster_id: A running cluster for every task that the template puts on a job cluster or a new
            cluster, helper tasks and tasks nested in a `for_each_task` included. The job clusters are removed.
            Serverless and warehouse tasks are left alone.

    Returns:
        `settings`.

    Raises:
        LakeflowError: If the job would have more than 25 tags without the attribution tags.
    """
    for attr in STRIPPED:
        setattr(settings, attr, None)
    if run_as != "keep":
        settings.run_as = _run_as(run_as)
    for task in settings.tasks or []:
        for attr in TASK_STRIPPED:
            setattr(task, attr, None)
        if disable_auto_optimization and _serverless(task):
            task.disable_auto_optimization = True
        if side_effects == "neutralise":
            _pause_subscriptions(task)
    if environment_version is not None:
        _set_environment_version(settings, environment_version)
    if budget_policy_id is not None:
        settings.budget_policy_id = budget_policy_id
    if existing_cluster_id is not None:
        _use_existing_cluster(settings, existing_cluster_id)
    if performance_optimized:
        settings.performance_target = jobs.PerformanceTarget.PERFORMANCE_OPTIMIZED
    settings.max_concurrent_runs = 1
    if timeout is not None:
        mirrored = math.ceil(timeout.total_seconds()) + TIMEOUT_GRACE_SECONDS
        if not settings.timeout_seconds or settings.timeout_seconds > mirrored:
            settings.timeout_seconds = mirrored
    settings.name = _trim(NAME_PREFIX + NEWLINES.sub(" ", name), MAX_NAME_BYTES)
    if description is not None:
        parts = [description, settings.description] if settings.description else [description]
        settings.description = _trim("\n\n".join(parts), MAX_DESCRIPTION_BYTES)
    required = {key: tag_value(value) for key, value in tags.items() if key not in ATTRIBUTION_TAGS}
    settings.tags = {**(settings.tags or {}), **required}
    if len(settings.tags) > MAX_TAGS:
        raise LakeflowError(
            f"The job would have {len(settings.tags)} tags, more than the {MAX_TAGS} allowed. "
            f"Remove {len(settings.tags) - MAX_TAGS} from the template: {sorted(settings.tags)}"
        )
    dropped = []
    for key in ATTRIBUTION_TAGS:
        if key not in tags:
            continue
        if key in settings.tags or len(settings.tags) < MAX_TAGS:
            settings.tags[key] = tag_value(tags[key])
        else:
            dropped.append(key)
    if dropped:
        log.debug("Left out the tags %s, because the job already has %d tags", dropped, MAX_TAGS)
    return settings


def _pause_subscriptions(task: jobs.Task) -> None:
    if task.dashboard_task and task.dashboard_task.subscription:
        task.dashboard_task.subscription.paused = True
    if task.sql_task and task.sql_task.alert:
        task.sql_task.alert.pause_subscriptions = True


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
    tasks = {t.task_key: t for t in settings.tasks or []}
    for key in keys:
        if key not in tasks:
            raise LakeflowError(f"Unknown task {key!r} in disable. Available: {sorted(tasks)}")
    for key in keys:
        tasks[key].disabled = True
    return settings


def pipeline_params(
    settings: jobs.JobSettings,
    *,
    full_refresh: bool | None,
    full_refresh_selection: Sequence[str] | None,
    refresh_selection: Sequence[str] | None,
) -> jobs.PipelineParams | None:
    """Build the pipeline refresh options of one run.

    Args:
        settings: The job's settings.
        full_refresh: Whether pipeline tasks run a full refresh.
        full_refresh_selection: The tables that pipeline tasks fully refresh.
        refresh_selection: The tables that pipeline tasks refresh.

    Returns:
        The options for `run_now`, or `None` when every argument is `None`.

    Raises:
        LakeflowError: If an argument is set and no task, nested `for_each_task` tasks included, is a pipeline task,
            a selection is a string rather than a list of table names, or a selection is set and the installed
            databricks-sdk cannot send it.
    """
    selections = {"full_refresh_selection": full_refresh_selection, "refresh_selection": refresh_selection}
    if full_refresh is None and all(value is None for value in selections.values()):
        return None
    for key, value in selections.items():
        if isinstance(value, str):
            raise LakeflowError(f"{key} must be a list of table names, not a string: {value!r}")
    unsupported = sorted(k for k, v in selections.items() if v is not None and k not in PIPELINE_PARAMS_FIELDS)
    if unsupported:
        raise LakeflowError(
            f"full_refresh_selection and refresh_selection need databricks-sdk 0.110.0 or later; got {unsupported}. "
            "Upgrade databricks-sdk to pass them."
        )
    if not any(task.pipeline_task for task in _with_nested(settings.tasks)):
        keys = sorted(task.task_key for task in settings.tasks or [] if task.task_key)
        raise LakeflowError(
            f"full_refresh, full_refresh_selection and refresh_selection apply to pipeline tasks, but the job has "
            f"no pipeline task. Tasks: {keys}"
        )
    chosen = {key: list(value) for key, value in selections.items() if value is not None}
    return jobs.PipelineParams(full_refresh=full_refresh, **chosen)


def _set_environment_version(settings: jobs.JobSettings, version: str) -> None:
    for env in settings.environments or []:
        if env.environment_key == ENV_KEY or (env.spec and getattr(env.spec, "base_environment", None)):
            continue
        if env.spec is None:
            env.spec = compute.Environment.from_dict({})
        env.spec.environment_version = version


def _with_nested(tasks: Sequence[jobs.Task] | None) -> list[jobs.Task]:
    tasks = list(tasks or [])
    return [*tasks, *(t.for_each_task.task for t in tasks if t.for_each_task and t.for_each_task.task)]


def run_job_tasks(settings: jobs.JobSettings) -> list[jobs.Task]:
    """Get the enabled tasks that run another job, tasks nested in a `for_each_task` included.

    `sanitise` keeps their `run_job_task` as deployed, so they trigger the deployed job, not a copy. A disabled
    task, and a task nested in a disabled `for_each_task`, does not run, so it is left out.

    Args:
        settings: The job's settings.

    Returns:
        Every enabled task with a `run_job_task`, in the order of `settings.tasks`, nested tasks last.
    """
    enabled = [task for task in settings.tasks or [] if not task.disabled]
    return [task for task in _with_nested(enabled) if task.run_job_task and not task.disabled]


def _use_existing_cluster(settings: jobs.JobSettings, cluster_id: str) -> None:
    for task in _with_nested(settings.tasks):
        if task.job_cluster_key or task.new_cluster:
            task.existing_cluster_id = cluster_id
            task.job_cluster_key = None
            task.new_cluster = None
    settings.job_clusters = None


def _run_as(run_as: str | jobs.JobRunAs | None) -> jobs.JobRunAs | None:
    if run_as is None or isinstance(run_as, jobs.JobRunAs):
        return run_as
    if "@" in run_as:
        return jobs.JobRunAs(user_name=run_as)
    return jobs.JobRunAs(service_principal_name=run_as)


def tag_value(value: str) -> str:
    """Make a string acceptable as a job tag value: Latin-1, one line, at most 256 UTF-8 bytes."""
    latin1 = "".join(c if ord(c) < 256 else "?" for c in NEWLINES.sub(" ", value))
    return _trim(latin1, MAX_TAG_BYTES)


def _trim(text: str, max_bytes: int) -> str:
    return text.encode()[:max_bytes].decode(errors="ignore")


def _serverless(task: jobs.Task) -> bool:
    runs_code = task.notebook_task or task.spark_python_task or task.python_wheel_task or task.spark_jar_task
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
            "Pass allow_undeclared_params=True to Lakeflow to pass them anyway."
        )


def check_compute(settings: jobs.JobSettings) -> None:
    """Fail early on tasks that the Jobs API rejects on serverless because they have no environment.

    Args:
        settings: The job's settings.

    Raises:
        LakeflowError: If a Python script, Python wheel, JAR or dbt task, nested `for_each_task` tasks included, has no
            cluster and no `environment_key`.
    """
    missing = [
        task.task_key
        for task in _with_nested(settings.tasks)
        if (task.spark_python_task or task.python_wheel_task or task.spark_jar_task or task.dbt_task)
        and not (task.job_cluster_key or task.new_cluster or task.existing_cluster_id or task.environment_key)
    ]
    if missing:
        raise LakeflowError(
            f"Tasks {missing} have no cluster and no environment_key. Serverless Python script, Python wheel, JAR "
            "and dbt tasks need an environment_key that names one of the job's environments."
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
        LakeflowError: If a task is not in the job, or a key is longer than 512 characters.
    """
    if not capture_task_values:
        return settings
    keys = {t.task_key for t in settings.tasks or []}
    unknown = sorted(set(capture_task_values) - keys)
    if unknown:
        raise LakeflowError(
            f"capture_task_values names tasks that are not in the job: {unknown}. Available: {sorted(keys)}"
        )
    for target, value_keys in capture_task_values.items():
        _check_keys(target, value_keys)
    probes = [_probe_task(target, value_keys, helpers) for target, value_keys in capture_task_values.items()]
    settings.tasks = [*(settings.tasks or []), *probes]
    _add_helper_compute(settings, helpers)
    return settings


def wrap_task(
    settings: jobs.JobSettings,
    task_key: str,
    upstream_task_values: Mapping[str, Mapping[str, object]],
    capture_task_values: Sequence[str],
    helpers: Helpers,
) -> jobs.JobSettings:
    """Reduce the job to one task, a stub task for each upstream task in `upstream_task_values`, and a probe.

    A stub has the key of the task it replaces, so the target's `{{tasks.<key>.values.<name>}}` references and
    `taskValues.get` calls resolve against it.

    Args:
        settings: The job's settings. They are changed in place.
        task_key: The task to keep.
        upstream_task_values: The task values each stub sets, by upstream task key.
        capture_task_values: The task value keys to capture from the task. No probe is added if empty.
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
    if task_key in upstream_task_values:
        raise LakeflowError(f"upstream_task_values cannot stub {task_key!r}, the task under test")
    referenced = set(TASK_REFERENCE.findall(json.dumps(target.as_dict())))
    missing = sorted(referenced - set(upstream_task_values))
    if missing:
        raise LakeflowError(
            f"Task {task_key!r} refers to task values of {missing} in its settings. Pass them in upstream_task_values."
        )
    for stub_key, values in upstream_task_values.items():
        check_values(stub_key, values)
    _check_keys(task_key, capture_task_values)
    target.depends_on = [jobs.TaskDependency(task_key=key) for key in upstream_task_values] or None
    stubs = [_stub_task(key, values, helpers) for key, values in upstream_task_values.items()]
    probes = [_probe_task(task_key, capture_task_values, helpers)] if capture_task_values else []
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
                f"helpers_compute is {helpers.job_cluster_key!r}, but the job has no job cluster with "
                f"that key. Available: {clusters}"
            )
    elif not any(env.environment_key == ENV_KEY for env in settings.environments or []):
        existing = [env.environment_key for env in settings.environments or []]
        if len(existing) >= MAX_ENVIRONMENTS:
            raise LakeflowError(
                f"The job already has {len(existing)} environments, the most a job allows is {MAX_ENVIRONMENTS}: "
                f"{existing}. One must go from the bundle job to make room for the helper "
                f"environment {ENV_KEY!r}, or pass helpers_compute to run helper tasks on a job cluster."
            )
        spec = compute.Environment.from_dict({"environment_version": helpers.environment_version})
        environment = jobs.JobEnvironment(environment_key=ENV_KEY, spec=spec)
        settings.environments = [*(settings.environments or []), environment]
