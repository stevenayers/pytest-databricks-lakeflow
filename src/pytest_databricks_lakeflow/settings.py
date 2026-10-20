from __future__ import annotations

import dataclasses
import inspect
import json
import logging
import math
import re
import warnings
from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import dataclass, fields
from datetime import timedelta
from decimal import Decimal
from typing import Any, Literal, get_args

from databricks.sdk.service import compute, jobs, pipelines

from pytest_databricks_lakeflow.bundle import split_extras
from pytest_databricks_lakeflow.errors import LakeflowError
from pytest_databricks_lakeflow.helpers import KEYS_WIDGET, PARAMS_WIDGET, TASK_KEY_WIDGET, VALUES_WIDGET
from pytest_databricks_lakeflow.results import PREFIX, PROBE_PREFIX, SETUP_KEY, TEARDOWN_KEY, JobRun

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
PIPELINE_CREATE_PARAMETERS = frozenset(inspect.signature(pipelines.PipelinesAPI.create).parameters) - {"self"}
# What would make a pipeline copy run on its own, notify people or count as bundle-managed, and its event log, which
# would write the copy's events into the deployed pipeline's event log table.
PIPELINE_STRIPPED = ("id", "deployment", "continuous", "trigger", "notifications", "event_log")
NAME_PREFIX = "[pytest] "
RunAs = Literal["keep"] | str | jobs.JobRunAs | None
SideEffects = Literal["neutralise", "keep"]
SIDE_EFFECTS: tuple[str, ...] = get_args(SideEffects)
MAX_NAME_BYTES = 4096
# pipelines/create refuses a longer name with "Name cannot be longer than 255 characters.", counting characters, not
# bytes: 255 characters of 479 bytes are accepted.
MAX_PIPELINE_NAME_CHARS = 255
MAX_TAG_BYTES = 256
MAX_TAGS = 25
MAX_DESCRIPTION_BYTES = 27700
ATTRIBUTION_TAGS = ("pytest_ci_url", "pytest_user", "pytest_git_sha", "pytest_git_branch")
NEWLINES = re.compile(r"[\r\n]+")
ENV_KEY = f"{PREFIX}serverless"
TEST_ID_PARAMETER = f"{PREFIX}test_id"
MAX_PARAMETER_DEFAULT_BYTES = 4096
MAX_ENVIRONMENTS = 10
MAX_VALUE_CHARS = 49152
MAX_KEY_CHARS = 512
# jobs/create refuses a literal for_each_task.inputs longer than this ("Invalid inputs: cannot exceed 5000
# characters"), although the docs give 48 KB for an array that comes from a task value reference.
MAX_FOR_EACH_INPUTS_CHARS = 5000
# jobs/create refuses any reference to a task that is not in the job: "Invalid reference:
# 'tasks.ghost.result_state'. Task 'ghost' does not exist."
TASK_REFERENCE = re.compile(r"\{\{\s*tasks\.([\w-]+)\.")
# jobs/create refuses a reference to an undeclared parameter: "Reference 'job.parameters.nope' in task 't' refers to
# an undefined job parameter 'nope'."
JOB_PARAMETER_REFERENCE = re.compile(r"\{\{\s*job\.parameters\.([\w-]+)\s*\}\}")
# Of the references to a task that run_now(only=...) leaves out, task values fail the run, and so do outputs
# ("Cannot access output from task '<key>' because the task was skipped."). The others resolve against the task's
# DISABLED run: result_state to "disabled", error_code to "Disabled", run_id to its run id.
TASK_VALUES_REFERENCE = re.compile(r"\{\{\s*tasks\.([\w-]+)\.values\.")
# A stub is a notebook task, which has no SQL, alert or clean room output, so these references are replaced by the
# literals its outputs give. The platform's paths after `output.` include rows, first_row, first_row.<column>,
# alert_state, catalog_name, schema_name and response.
OUTPUT_REFERENCE = re.compile(r"\{\{\s*tasks\.([\w-]+)\.output\.([\w.-]+)\s*\}\}")
# A run-now run resolves {{job.trigger.type}} to one_time and {{job.trigger.time.*}} to the time of the run, and
# jobs/create refuses a table_update or file_arrival reference in a job without that trigger ("Table update trigger
# references can not be used unless the job has a table update trigger configured.", "Job does not have a file
# arrival trigger configured.") and any other path ("Invalid reference: 'job.trigger.nope'. 'trigger' is unknown.").
TRIGGER_REFERENCE = re.compile(r"\{\{\s*job\.trigger\.([\w.]+)\s*\}\}")
TRIGGER_PARAM_PREFIX = f"{PREFIX}trigger_"
# The trigger references that jobs/create refuses in a job without that trigger, by the first part of their path.
TRIGGER_KINDS = {"table_update": "table update", "file_arrival": "file arrival"}
# jobs/create refuses a deeper reference: "Invalid reference: 'input.e.f'. Input reference can only access top-level
# keys." So a deployed template has none, and the pattern matches `{{input}}` and `{{input.<key>}}` only.
INPUT_REFERENCE = re.compile(r"\{\{\s*input(?:\.([^.\s}]+))?\s*\}\}")
INPUT_PARAMETER = f"{PREFIX}input"
# jobs/create refuses job parameters longer than this as JSON: "The provided parameters (in json representation) are
# too long (10032 bytes); it cannot exceed 10000 bytes." for one parameter `p` with a default of 10,000 characters.
# It counts 29 characters for each definition plus its name and its JSON-escaped default, a comma between two
# definitions and 2 for the brackets, and a non-ASCII character as one. The limit is on the parameter list as a whole,
# not on each value as the For each docs say, so the input's room depends on every other parameter of the copy.
MAX_PARAMETERS_JSON_CHARS = 10_000
TIMEOUT_GRACE_SECONDS = 60
TASK_OVERRIDES = frozenset({"timeout_seconds", "max_retries", "min_retry_interval_millis", "retry_on_timeout"})
PIPELINE_PARAMS_FIELDS = frozenset(f.name for f in fields(jobs.PipelineParams))
SINGLE_NODE_CONF = {"spark.master": "local[*, 4]", "spark.databricks.cluster.profile": "singleNode"}
SINGLE_NODE_TAGS = {"ResourceClass": "SingleNode"}
SHARED_ACCESS_MODES = frozenset(
    {compute.DataSecurityMode.USER_ISOLATION, compute.DataSecurityMode.DATA_SECURITY_MODE_STANDARD}
)
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


@dataclass(frozen=True)
class Helpers:
    """Where the helper notebooks are, and the compute that stub, probe, setup and teardown tasks run on.

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
    single_node: bool = False,
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
            are added only while the job has fewer than 25 tags, in that order; the others must fit. The plugin's
            `pytest`, `pytest_run_id`, `pytest_nodeid` and `pytest_job_key` leave room for 21 tags of the
            template's own, or 20 with `--lakeflow-keep-ttl`, which adds `pytest_expires`.
        disable_auto_optimization: Stop serverless from retrying failed code tasks, tasks nested in a
            `for_each_task` included.
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
            template has them. Alert tasks and Power BI tasks are left as the template has them in both modes.
        environment_version: The serverless environment version of every environment except the helper
            environment and those whose spec sets `base_environment`.
        budget_policy_id: The budget policy of the job.
        existing_cluster_id: A running cluster for every task that the template puts on a job cluster or a new
            cluster, helper tasks and tasks nested in a `for_each_task` included. The job clusters are removed.
            Serverless and warehouse tasks are left alone.
        single_node: Make every job cluster and every task's `new_cluster` a single node cluster, tasks nested in a
            `for_each_task` included: no workers, no autoscaling, and the `SINGLE_NODE_CONF` Spark configuration
            and `SINGLE_NODE_TAGS` tags merged over the cluster's own. Applied after `existing_cluster_id`, which
            leaves no cluster to change.

    Returns:
        `settings`.

    Raises:
        LakeflowError: If the job would have more than 25 tags without the attribution tags, or `single_node` is
            set and a cluster uses standard access mode, which single node compute does not support.
    """
    for attr in STRIPPED:
        setattr(settings, attr, None)
    if run_as != "keep":
        settings.run_as = _run_as(run_as)
    for task in settings.tasks or []:
        for attr in TASK_STRIPPED:
            setattr(task, attr, None)
        if side_effects == "neutralise":
            _pause_subscriptions(task)
    if disable_auto_optimization:
        # A nested task needs it too, or serverless retries each failing iteration once.
        for task in _with_nested(settings.tasks):
            if _serverless(task):
                task.disable_auto_optimization = True
    if environment_version is not None:
        _set_environment_version(settings, environment_version)
    if budget_policy_id is not None:
        settings.budget_policy_id = budget_policy_id
    if existing_cluster_id is not None:
        _use_existing_cluster(settings, existing_cluster_id)
    if single_node:
        _single_node_clusters(settings)
    if performance_optimized:
        settings.performance_target = jobs.PerformanceTarget.PERFORMANCE_OPTIMIZED
    settings.max_concurrent_runs = 1
    if timeout is not None:
        _mirror_timeout(settings, timeout)
    settings.name = _trim(NAME_PREFIX + NEWLINES.sub(" ", name), MAX_NAME_BYTES)
    if description is not None:
        parts = [description, settings.description] if settings.description else [description]
        settings.description = _trim("\n\n".join(parts), MAX_DESCRIPTION_BYTES)
    settings.tags = _merged_tags(settings.tags, tags, "job")
    return settings


def _run_as(run_as: str | jobs.JobRunAs | None) -> jobs.JobRunAs | None:
    if run_as is None or isinstance(run_as, jobs.JobRunAs):
        return run_as
    if "@" in run_as:
        return jobs.JobRunAs(user_name=run_as)
    return jobs.JobRunAs(service_principal_name=run_as)


def _serverless(task: jobs.Task) -> bool:
    runs_code = task.notebook_task or task.spark_python_task or task.python_wheel_task or task.spark_jar_task
    on_warehouse = task.notebook_task and task.notebook_task.warehouse_id
    return bool(runs_code) and not _on_cluster(task) and not on_warehouse


def _on_cluster(task: jobs.Task) -> bool:
    return bool(task.job_cluster_key or task.new_cluster or task.existing_cluster_id)


def _pause_subscriptions(task: jobs.Task) -> None:
    if task.dashboard_task and task.dashboard_task.subscription:
        task.dashboard_task.subscription.paused = True
    if task.sql_task and task.sql_task.alert:
        task.sql_task.alert.pause_subscriptions = True


def _set_environment_version(settings: jobs.JobSettings, version: str) -> None:
    for env in settings.environments or []:
        if env.environment_key == ENV_KEY:
            continue
        if env.spec and env.spec.base_environment:
            continue
        if env.spec is None:
            env.spec = compute.Environment.from_dict({})
        env.spec.environment_version = version


def _use_existing_cluster(settings: jobs.JobSettings, cluster_id: str) -> None:
    for task in _with_nested(settings.tasks):
        if task.job_cluster_key or task.new_cluster:
            task.existing_cluster_id = cluster_id
            task.job_cluster_key = None
            task.new_cluster = None
    settings.job_clusters = None


def _single_node_clusters(settings: jobs.JobSettings) -> None:
    clusters: list[tuple[str | None, compute.ClusterSpec]] = []
    for job_cluster in settings.job_clusters or []:
        if job_cluster.new_cluster:
            clusters.append((job_cluster.job_cluster_key, job_cluster.new_cluster))
    for task in _with_nested(settings.tasks):
        if task.new_cluster:
            clusters.append((task.task_key, task.new_cluster))
    for key, cluster in clusters:
        if cluster.data_security_mode in SHARED_ACCESS_MODES:
            raise LakeflowError(
                f"single_node cannot change cluster {key!r}: its data_security_mode is "
                f"{cluster.data_security_mode.value}, and single node compute needs dedicated access mode. Set "
                "data_security_mode to SINGLE_USER or DATA_SECURITY_MODE_DEDICATED, or leave single_node off."
            )
    for _, cluster in clusters:
        cluster.num_workers = 0
        cluster.autoscale = None
        cluster.spark_conf = {**(cluster.spark_conf or {}), **SINGLE_NODE_CONF}
        cluster.custom_tags = {**(cluster.custom_tags or {}), **SINGLE_NODE_TAGS}


def job_timeout_seconds(timeout: timedelta) -> int:
    """The job `timeout_seconds` that covers a run timeout: the timeout in whole seconds plus the grace period."""
    return math.ceil(timeout.total_seconds()) + TIMEOUT_GRACE_SECONDS


def _mirror_timeout(settings: jobs.JobSettings, timeout: timedelta) -> None:
    mirrored = job_timeout_seconds(timeout)
    if not settings.timeout_seconds or settings.timeout_seconds > mirrored:
        settings.timeout_seconds = mirrored


def _merged_tags(current: Mapping[str, str] | None, tags: Mapping[str, str], what: str) -> dict[str, str]:
    """Add `tags` to the tags a job or pipeline has, the attribution tags only while there is room for them."""
    required = {key: tag_value(value) for key, value in tags.items() if key not in ATTRIBUTION_TAGS}
    merged = {**(current or {}), **required}
    if len(merged) > MAX_TAGS:
        raise LakeflowError(
            f"The {what} would have {len(merged)} tags, more than the {MAX_TAGS} allowed. "
            f"Remove {len(merged) - MAX_TAGS} from the template: {sorted(merged)}"
        )
    dropped = []
    for key in ATTRIBUTION_TAGS:
        if key not in tags:
            continue
        if key in merged or len(merged) < MAX_TAGS:
            merged[key] = tag_value(tags[key])
        else:
            dropped.append(key)
    if dropped:
        log.debug("Left out the tags %s, because the %s already has %d tags", dropped, what, MAX_TAGS)
    return merged


def sanitise_pipeline(
    spec: pipelines.PipelineSpec,
    name: str,
    tags: Mapping[str, str],
    *,
    schema: str,
    configuration: Mapping[str, str] | None = None,
    budget_policy_id: str | None = None,
) -> dict[str, Any]:
    """Turn the spec of a deployed pipeline into the arguments of `pipelines.create` for a throwaway copy.

    The copy keeps the spec's code, compute and channel, so it runs the files that the last `bundle deploy` uploaded.
    It drops the fields in `PIPELINE_STRIPPED`, publishes to `schema` in the publishing mode the spec uses (`target`
    for the legacy mode, `schema` otherwise), runs in development mode, and is named and tagged as an ephemeral job
    is. `run_as` is not part of a spec, so the copy runs as its creator.

    Args:
        spec: The deployed pipeline's spec. It is not changed.
        name: The pipeline name, without the `[pytest] ` prefix. With the prefix it is cut to 255 characters.
        tags: Tags to add, as for `sanitise`.
        schema: The full name of the schema to publish to, `catalog.schema`, as `Lakeflow.schema()` yields it.
        configuration: Keys to set in the copy's configuration, over the spec's.
        budget_policy_id: The budget policy of the copy. Defaults to the spec's.

    Returns:
        The keyword arguments for `pipelines.create`.

    Raises:
        LakeflowError: If `schema` is not a full name, the spec publishes to the Hive metastore, or the copy would
            have more than 25 tags without the attribution tags.
    """
    catalog, _, schema_name = schema.partition(".")
    if not schema_name:
        raise LakeflowError(f"schema must be a full name, catalog.schema, as lakeflow.schema() yields: {schema!r}")
    if spec.catalog is None:
        raise LakeflowError(
            f"Pipeline {spec.name or name!r} publishes to the Hive metastore (no catalog), so it cannot be copied into "
            "a Unity Catalog schema"
        )
    kwargs: dict[str, Any] = {}
    for field in fields(spec):
        if field.name not in PIPELINE_CREATE_PARAMETERS or field.name in PIPELINE_STRIPPED:
            continue
        value = getattr(spec, field.name)
        if value is None or value == []:
            continue
        kwargs[field.name] = value
    kwargs["catalog"] = catalog
    if spec.target:
        kwargs["target"] = schema_name
    else:
        kwargs["schema"] = schema_name
    kwargs["name"] = (NAME_PREFIX + NEWLINES.sub(" ", name))[:MAX_PIPELINE_NAME_CHARS]
    kwargs["development"] = True
    kwargs["allow_duplicate_names"] = True
    merged = {**(spec.configuration or {}), **(configuration or {})}
    if merged:
        kwargs["configuration"] = merged
    if budget_policy_id is not None:
        kwargs["budget_policy_id"] = budget_policy_id
    kwargs["tags"] = _merged_tags(spec.tags, tags, "pipeline")
    return kwargs


def tag_value(value: str) -> str:
    """Make a string acceptable as a job tag value: Latin-1, one line, at most 256 UTF-8 bytes."""
    latin1 = "".join(c if ord(c) < 256 else "?" for c in NEWLINES.sub(" ", value))
    return _trim(latin1, MAX_TAG_BYTES)


def _trim(text: str, max_bytes: int) -> str:
    return text.encode()[:max_bytes].decode(errors="ignore")


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
    marker = jobs.JobParameterDefinition(name=TEST_ID_PARAMETER, default=_trim(nodeid, MAX_PARAMETER_DEFAULT_BYTES))
    settings.parameters = [*kept, marker]
    return settings


def add_dependencies(settings: jobs.JobSettings, dependencies: Sequence[str]) -> jobs.JobSettings:
    """Append test-only dependencies to every environment and to every Python task on a cluster.

    Each environment's `spec.dependencies` gains every dependency. Each notebook, Python script and Python wheel
    task that runs on a job cluster, a new cluster or an existing cluster, tasks nested in a `for_each_task`
    included, gains a `whl` library for a wheel path ending in `.whl` and a `pypi` library for any other requirement,
    a direct reference such as `pkg @ https://host/pkg-1.0-py3-none-any.whl` included. A wheel path with extras, such
    as `/Workspace/.../pkg-0.1-py3-none-any.whl[test]`, reaches the environments as it is. Serverless tasks without an
    `environment_key` cannot take dependencies from the job settings, so they are left alone; the notebook tasks it
    does not reach, after `environments` and `modify_settings`, are named in a warning when the job is created (see
    `unreached_notebook_tasks`). Call it before the helper environment is added, so that helper tasks do not install
    them.

    Args:
        settings: The job's settings. They are changed in place.
        dependencies: PyPI requirements, or workspace or Volumes paths of wheels, with or without extras.

    Returns:
        `settings`.

    Raises:
        LakeflowError: If a dependency is a local path, or a wheel path with extras would reach a task on a cluster,
            whose `libraries[].whl` takes no extras. Nothing is changed then.
    """
    if not dependencies:
        return settings
    local = [dep for dep in dependencies if _is_local_path(split_extras(dep)[0])]
    if local:
        raise LakeflowError(
            f"Local paths in extra_dependencies must start with ./ or ../, relative to the bundle root: "
            f"{', '.join(map(repr, local))}. Otherwise pass a PyPI requirement or a /Workspace or /Volumes path."
        )
    cluster_tasks = []
    for task in _with_nested(settings.tasks):
        runs_python = task.notebook_task or task.spark_python_task or task.python_wheel_task
        if runs_python and _on_cluster(task):
            cluster_tasks.append(task)
    _check_cluster_extras(cluster_tasks, dependencies)
    for env in settings.environments or []:
        if env.spec is None:
            env.spec = compute.Environment.from_dict({})
        env.spec.dependencies = [*(env.spec.dependencies or []), *dependencies]
    for task in cluster_tasks:
        task.libraries = [*(task.libraries or []), *(_library(dep) for dep in dependencies)]
    return settings


def _is_local_path(dependency: str) -> bool:
    if dependency.startswith(("./", "../")):
        return True
    return dependency.endswith(".whl") and not dependency.startswith("/") and ":" not in dependency


def _check_cluster_extras(cluster_tasks: Sequence[jobs.Task], dependencies: Sequence[str]) -> None:
    with_extras = [dep for dep in dependencies if split_extras(dep)[1]]
    if not cluster_tasks or not with_extras:
        return
    keys = sorted(task.task_key for task in cluster_tasks)
    raise LakeflowError(
        f"Tasks {keys} run on a cluster, whose libraries take a wheel without extras, so "
        f"{', '.join(map(repr, with_extras))} cannot be installed there with them. Pass the packages the extras "
        "add as PyPI requirements instead."
    )


# A PEP 508 direct reference starts with a project name, optional extras and "@", with or without spaces. A wheel path
# can hold "@" too, as in /Workspace/Users/<email>/, so only the start of the string tells them apart.
_DIRECT_REFERENCE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\s*(\[[^\]]*\])?\s*@")


def _library(dependency: str) -> compute.Library:
    # A direct reference such as "pkg @ https://host/pkg.whl" is a requirement that pip installs, not a path.
    if dependency.endswith(".whl") and not _DIRECT_REFERENCE.match(dependency):
        return compute.Library(whl=dependency)
    return compute.Library(pypi=compute.PythonPyPiLibrary(package=dependency))


def unreached_notebook_tasks(
    settings: jobs.JobSettings, dependencies: Sequence[str], stubs: Collection[str] = ()
) -> list[str]:
    """Get the notebook tasks whose compute does not hold every one of `dependencies`.

    It reads the settings as they are created, after `environments` and `modify_settings`. A notebook task on a
    cluster needs each dependency in its `libraries`, and a serverless one in the `spec.dependencies` of the
    environment its `environment_key` names. A serverless notebook task without an `environment_key` has neither.
    Disabled tasks, tasks downstream of one, tasks nested in a disabled `for_each_task`, notebook tasks on a SQL
    warehouse, which take no Python packages, and the setup, teardown, probe and stub tasks are left out.

    Args:
        settings: The job's settings.
        dependencies: The test-only dependencies, as `add_dependencies` took them.
        stubs: Keys of the stub tasks.

    Returns:
        The keys of those tasks, in the order of `settings.tasks`, nested tasks last. Empty when `dependencies` is.
    """
    if not dependencies:
        return []
    environments: dict[str, list[str]] = {}
    for env in settings.environments or []:
        listed: list[str] = []
        if env.spec is not None:
            listed = env.spec.dependencies or []
        environments[env.environment_key] = listed
    unreached: list[str] = []
    for task in _enabled_tasks(settings, skip=disabled_task_keys(settings)):
        if task.task_key.startswith(PREFIX) or task.task_key in stubs:
            continue
        if task.notebook_task is None or task.notebook_task.warehouse_id:
            continue
        if not _holds_dependencies(task, environments, dependencies):
            unreached.append(task.task_key)
    return unreached


def _holds_dependencies(task: jobs.Task, environments: Mapping[str, list[str]], dependencies: Sequence[str]) -> bool:
    if _on_cluster(task):
        libraries = task.libraries or []
        return all(_library(dep) in libraries for dep in dependencies)
    if task.environment_key:
        listed = environments.get(task.environment_key, [])
        return all(dep in listed for dep in dependencies)
    return False


def merge_environments(settings: jobs.JobSettings, environments: Mapping[str, Mapping[str, Any]]) -> jobs.JobSettings:
    """Merge per-job overrides into the job's serverless environments.

    A field in the override of an existing environment replaces the template's field, so `dependencies` replaces
    the list rather than appending to it. A key the job does not have is appended as a new environment. Call it after
    `sanitise`, so that an `environment_version` here wins over the session-wide one, and after the helper
    environment is added, so that it counts towards the cap.

    Args:
        settings: The job's settings. They are changed in place.
        environments: The fields of each environment's `spec` to set, by `environment_key`.

    Returns:
        `settings`.

    Raises:
        LakeflowError: If a key starts with `__pytest_`, an override is not a mapping or holds a field that
            `compute.Environment` does not have, a spec would set both `environment_version` and
            `base_environment`, or the job would have more than 10 environments. Nothing is changed then.
    """
    if not environments:
        return settings
    check_environments(environments)
    merged = list(settings.environments or [])
    positions = {env.environment_key: index for index, env in enumerate(merged)}
    for key, override in environments.items():
        if key in positions:
            index = positions[key]
            merged[index] = _merged_environment(key, merged[index].spec, override)
        else:
            merged.append(_merged_environment(key, None, override))
    if len(merged) > MAX_ENVIRONMENTS:
        keys = [env.environment_key for env in merged]
        raise LakeflowError(
            f"The job would have {len(merged)} environments, more than the {MAX_ENVIRONMENTS} a job allows: {keys}. "
            "Add fewer environments in environments=, or remove some from the bundle job."
        )
    settings.environments = merged
    return settings


def check_environments(environments: Mapping[str, Mapping[str, Any]]) -> None:
    """Check the per-job environment overrides that need no job: their keys, and the fields of each override.

    Raises:
        LakeflowError: If a key starts with `__pytest_`, an override is not a mapping or holds a field that
            `compute.Environment` does not have.
    """
    reserved = sorted(key for key in environments if key.startswith(PREFIX))
    if reserved:
        raise LakeflowError(f"Environment keys starting with {PREFIX!r} are reserved for the plugin: {reserved}")
    known = sorted(f.name for f in fields(compute.Environment))
    for key, override in environments.items():
        if not isinstance(override, Mapping):
            raise LakeflowError(
                f"environments[{key!r}] must be a mapping of spec fields to values, not {type(override).__name__}"
            )
        # Environment.from_dict drops a field it does not know without a word.
        unknown = sorted(set(override) - set(known))
        if unknown:
            raise LakeflowError(
                f"environments[{key!r}] has fields that a serverless environment does not take: {unknown}. "
                f"Known fields: {known}"
            )


def _merged_environment(
    key: str, template: compute.Environment | None, override: Mapping[str, Any]
) -> jobs.JobEnvironment:
    current: dict[str, Any] = {}
    if template is not None:
        current = template.as_dict()
    spec = compute.Environment.from_dict({**current, **override})
    # jobs/create refuses the pair with InvalidParameterValue "Both base environment and version are provided for
    # environment 'default'. Only one of them must be provided.", so this only reports it before anything is created.
    if spec.environment_version and spec.base_environment:
        raise LakeflowError(
            f"environments[{key!r}] would set both environment_version and base_environment, which a job does not "
            "accept. Set the one you do not want to None in the override."
        )
    return jobs.JobEnvironment(environment_key=key, spec=spec)


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
            disabled |= _reachable(task.task_key, downstream)
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
        LakeflowError: If a key is not a task in the job, or a field is not in `TASK_OVERRIDES`.
    """
    tasks = {t.task_key: t for t in settings.tasks or []}
    allowed = sorted(TASK_OVERRIDES)
    for key, values in overrides.items():
        if key not in tasks:
            raise LakeflowError(f"Unknown task {key!r} in task_overrides. Available: {sorted(tasks)}")
        for name in values:
            if name not in TASK_OVERRIDES:
                raise LakeflowError(f"Unknown field {name!r} in task_overrides[{key!r}]. Allowed: {allowed}")
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
        LakeflowError: If a key is not a task in the job, its task has no `condition_task`, the task is disabled
            or downstream of a disabled task, or an outcome is not a bool.
    """
    if not outcomes:
        return settings
    tasks = {t.task_key: t for t in settings.tasks or []}
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
            raise LakeflowError(f"force_conditions[{key!r}] must be True or False, got {outcome!r}")
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
        "Forcing condition task %r to %s; it compared %r %s %r",
        task.task_key,
        expected,
        condition.left,
        op,
        condition.right,
    )
    task.condition_task = jobs.ConditionTask(op=jobs.ConditionTaskOp.EQUAL_TO, left="true", right=expected)


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
        LakeflowError: If a key is not a task in the job or its task has no `for_each_task`, or a list is not a list,
            is not JSON-serialisable or is longer than 5,000 characters as JSON.
    """
    tasks = {t.task_key: t for t in settings.tasks or []}
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
        raise LakeflowError(f"for_each_inputs[{key!r}] must be a list, got {values!r}")
    try:
        text = escape(list(values))
    except (TypeError, ValueError) as e:
        raise LakeflowError(f"Inputs for task {key!r} in for_each_inputs are not JSON-serialisable: {e}") from None
    if len(text) > MAX_FOR_EACH_INPUTS_CHARS:
        raise LakeflowError(
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
            raise LakeflowError(_for_each_items_message(key, found))
        if first is None:
            first = kind
        elif kind != first:
            raise LakeflowError(_for_each_items_message(key, f"{found} after {first} at index 0"))


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
        LakeflowError: If a key is not the path of a reference in the settings, or a value is not a string.
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
            raise LakeflowError(f"trigger[{path!r}] must be a string, the value a job parameter holds, got {value!r}")
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

    rewritten = jobs.JobSettings.from_dict(json.loads(_point_at_parameters(text, TRIGGER_REFERENCE, name_for)))
    definitions: list[jobs.JobParameterDefinition] = []
    for path, value in trigger.items():
        definitions.append(jobs.JobParameterDefinition(name=_trigger_parameter(path), default=value))
    rewritten.parameters = [*(rewritten.parameters or []), *definitions]
    return rewritten


def _trigger_parameter(path: str) -> str:
    return f"{TRIGGER_PARAM_PREFIX}{path.replace('.', '_')}"


def _point_at_parameters(text: str, pattern: re.Pattern[str], name_for: Callable[[str], str | None]) -> str:
    """Replace each match of `pattern` in `text` with a reference to the job parameter `name_for` gives its group.

    A group that did not take part in the match is passed as `""`, and a match `name_for` gives no name for is left as
    it is.
    """

    def replacement(match: re.Match[str]) -> str:
        name = name_for(match[1] or "")
        if name is None:
            return match[0]
        return f"{{{{job.parameters.{name}}}}}"

    return pattern.sub(replacement, text)


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
        LakeflowError: If the task refers to `{{input}}` and `value` is `None`, or `value` is given and the task does
            not refer to it, `value` is not a string, number, boolean or mapping, a key is not in `value`, or a
            rendered value contains `{{` or is not JSON-serialisable. Nothing is changed then.
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
        raise LakeflowError(
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
    rewritten = _point_at_parameters(text, INPUT_REFERENCE, _input_parameter)
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
        raise LakeflowError(f"input for task {task_key!r} is not JSON-serialisable: {e}") from None
    if "{{" in text:
        raise LakeflowError(f"input for task {task_key!r} contains '{{{{', which the platform would substitute")
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
        LakeflowError: If an argument is set and no task, nested `for_each_task` tasks included, is a pipeline task,
            a selection is a string rather than a list of names, or a selection is set and the installed
            databricks-sdk cannot send it.
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
    if not any(task.pipeline_task for task in _with_nested(settings.tasks)):
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
        LakeflowError: If a selection is a string.
    """
    for key, value in selections.items():
        if isinstance(value, str):
            raise LakeflowError(f"{key} must be a list of names, not a string: {value!r}")


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
    for task in _enabled_tasks(settings, skip=disabled_task_keys(settings)):
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
    for task in _with_nested(settings.tasks):
        if task.pipeline_task is not None and task.pipeline_task.pipeline_id in copies:
            task.pipeline_task.pipeline_id = copies[task.pipeline_task.pipeline_id]
    return settings


def _with_nested(tasks: Sequence[jobs.Task] | None) -> list[jobs.Task]:
    result = list(tasks or [])
    for task in list(result):
        if task.for_each_task and task.for_each_task.task:
            result.append(task.for_each_task.task)
    return result


def run_job_tasks(settings: jobs.JobSettings) -> list[jobs.Task]:
    """Get the enabled tasks that run another job, tasks nested in a `for_each_task` included.

    `sanitise` keeps their `run_job_task` as deployed, so they trigger the deployed job, not a copy. A disabled
    task, a task downstream of one and a task nested in a disabled `for_each_task` does not run, so it is left out.

    Args:
        settings: The job's settings.

    Returns:
        Every enabled task with a `run_job_task`, in the order of `settings.tasks`, nested tasks last.
    """
    return [task for task in _enabled_tasks(settings, skip=disabled_task_keys(settings)) if task.run_job_task]


def _enabled_tasks(settings: jobs.JobSettings, *, skip: Collection[str] = ()) -> list[jobs.Task]:
    """Get the tasks that are not disabled, nested tasks included unless they or their `for_each_task` are.

    The top-level tasks in `skip` are left out too, with the task nested in each.
    """
    enabled = [task for task in settings.tasks or [] if not task.disabled and task.task_key not in skip]
    return [task for task in _with_nested(enabled) if not task.disabled]


def create_kwargs(settings: jobs.JobSettings) -> dict[str, Any]:
    """Get the settings fields that `jobs.create` accepts, leaving out `None` and empty lists."""
    kwargs: dict[str, Any] = {}
    for field in fields(settings):
        if field.name not in CREATE_PARAMETERS:
            continue
        value = getattr(settings, field.name)
        if value is None or value == []:
            continue
        kwargs[field.name] = value
    return kwargs


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
        LakeflowError: If a parameter name starts with `__pytest_`.
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
        raise LakeflowError(f"Parameter names starting with {PREFIX!r} are reserved for the plugin: {reserved}")
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
        LakeflowError: If a key is not an allowed task of the job, starts with `__pytest_`, names a task that is not a
            notebook, Python script, JAR, Python wheel, SQL or dbt task, has a value that does not fit the task's
            type, or gives a dbt task an empty list of commands. Nothing is changed then. For a `for_each_task` whose
            nested task may be named, the message names the nested task.
    """
    if not task_params:
        return settings
    tasks: dict[str, jobs.Task] = {}
    for task in _with_nested(settings.tasks):
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
        raise LakeflowError(
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
    raise LakeflowError(
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


def check_compute(settings: jobs.JobSettings) -> None:
    """Fail early on tasks that the Jobs API rejects on serverless because they have no environment.

    Args:
        settings: The job's settings.

    Raises:
        LakeflowError: If a Python script, Python wheel, JAR or dbt task, nested `for_each_task` tasks included, has no
            cluster and no `environment_key`.
    """
    missing: list[str] = []
    for task in _with_nested(settings.tasks):
        needs_environment = task.spark_python_task or task.python_wheel_task or task.spark_jar_task or task.dbt_task
        if needs_environment and not _on_cluster(task) and not task.environment_key:
            missing.append(task.task_key)
    if missing:
        raise LakeflowError(
            f"Tasks {missing} have no cluster and no environment_key. Serverless Python script, Python wheel, JAR "
            "and dbt tasks need an environment_key that names one of the job's environments."
        )


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
    needed = _running_stubs(settings, selected, stubs)
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
            available = sorted(k for k in upstream if not k.startswith(PREFIX) and k not in stubs)
            raise LakeflowError(f"Unknown task {key!r} in only. Available: {available}")
        selected.add(key)
        if entry.startswith("+"):
            selected |= _reachable(key, upstream)
        if entry.endswith("+"):
            selected |= _reachable(key, downstream)
    return selected


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
    running = {*selected, *_running_stubs(settings, selected, stubs)}
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
    for task in _with_nested(settings.tasks):
        _check_trigger_kinds(f"Task {task.task_key!r} of {job_key!r}", _own_fields(task), configured)
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


def _own_fields(task: jobs.Task) -> dict[str, Any]:
    """The task as a dict, without the task nested in its `for_each_task`, which `_with_nested` lists on its own."""
    own = task.as_dict()
    loop = own.get("for_each_task")
    if loop is not None:
        own["for_each_task"] = {key: value for key, value in loop.items() if key != "task"}
    return own


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
    tasks = _with_nested(settings.tasks)
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


def lint(settings: jobs.JobSettings) -> list[str]:
    """Find the wiring mistakes in a job's settings that `bundle validate` passes, without changing them.

    jobs/create refuses most of them, one at a time; this finds them all at once, before anything reaches the
    workspace.

    Args:
        settings: The job's settings.

    Returns:
        One message per problem, each starting with the key of its task, in the order of `settings.tasks` with each
        nested task after its `for_each_task`: a `{{job.parameters.<name>}}` the job does not declare, a
        `{{tasks.<key>.values.<name>}}` whose task is not upstream, a `depends_on` task that is not in the job, and
        retry settings on a `for_each_task`.
    """
    declared = sorted(_declared_params(settings))
    keys = [task.task_key for task in settings.tasks or []]
    graph = {task.task_key: {dep.task_key for dep in task.depends_on or []} for task in settings.tasks or []}
    problems: list[str] = []
    for task in settings.tasks or []:
        upstream = _reachable(task.task_key, graph) & graph.keys()
        problems += _task_problems(task, upstream, declared, keys)
        if task.for_each_task and task.for_each_task.task:
            # A nested task may refer to the task values of any task upstream of its for_each task, and jobs/create
            # refuses a reference to the for_each task itself: "Task 'loop' must be a dependency of task 'inner'."
            problems += _task_problems(task.for_each_task.task, upstream, declared, keys)
    return problems


def _task_problems(task: jobs.Task, upstream: set[str], declared: list[str], keys: list[str]) -> list[str]:
    body = task.as_dict()
    if "for_each_task" in body:
        body["for_each_task"].pop("task", None)
    text = json.dumps(body)
    prefix = f"Task {task.task_key!r}:"
    problems: list[str] = []
    for name in sorted(set(JOB_PARAMETER_REFERENCE.findall(text)) - set(declared)):
        problems.append(f"{prefix} {{{{job.parameters.{name}}}}} is not declared. Declared: {declared}")
    for key in sorted(set(TASK_VALUES_REFERENCE.findall(text)) - upstream):
        problems.append(
            f"{prefix} refers to the task values of {key!r}, which is not upstream of it. Upstream: {sorted(upstream)}"
        )
    for dependency in task.depends_on or []:
        if dependency.task_key not in keys:
            problems.append(
                f"{prefix} depends on {dependency.task_key!r}, which is not a task in the job. Tasks: {sorted(keys)}"
            )
    retries = _retries(task)
    if task.for_each_task and retries:
        # jobs/create refuses max_retries above 0 on a for_each task ("For each task cannot be retried but has
        # retries configured") and accepts min_retry_interval_millis and retry_on_timeout there, to no effect. jobs/get
        # returns max_retries 0 and retry_on_timeout false beside either, so those defaults are not reported.
        nested = "its nested task"
        if task.for_each_task.task:
            nested += f" {task.for_each_task.task.task_key!r}"
        problems.append(
            f"{prefix} a for_each task cannot be retried, but it sets {retries}. Set retries on {nested} instead."
        )
    return problems


def _retries(task: jobs.Task) -> str:
    """The retry settings a task sets to other than their defaults, as `name=value` pairs."""
    pairs: list[str] = []
    for name in ("max_retries", "min_retry_interval_millis", "retry_on_timeout"):
        value = getattr(task, name)
        if value:
            pairs.append(f"{name}={value}")
    return ", ".join(pairs)


def _running_stubs(settings: jobs.JobSettings, selected: Collection[str], stubs: Collection[str]) -> set[str]:
    """The stubs that run with the selected tasks: those a selected task depends on."""
    running: set[str] = set()
    for task in settings.tasks or []:
        if task.task_key not in selected:
            continue
        for dependency in task.depends_on or []:
            if dependency.task_key in stubs:
                running.add(dependency.task_key)
    return running


def _reachable(start: str, edges: Mapping[str, set[str]]) -> set[str]:
    seen: set[str] = set()
    pending = [start]
    while pending:
        for key in edges.get(pending.pop(), set()) - seen:
            seen.add(key)
            pending.append(key)
    return seen


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
        LakeflowError: If a key is not a task in the job, a value is too large or not JSON-serialisable, an output
            contains `{{` or is not JSON-serialisable, or a task refers to an output its stub does not give.
    """
    if not upstream_task_values:
        return settings
    tasks = {t.task_key: t for t in settings.tasks or []}
    for key in upstream_task_values:
        if key not in tasks:
            available = sorted(k for k in tasks if not k.startswith(PREFIX))
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
    _add_helper_compute(settings, helpers)
    return settings


def as_stubs(upstream_task_values: Upstream) -> dict[str, Stub]:
    """Turn each plain mapping of task values into a `Stub`, and check every stub's values and outputs.

    Raises:
        LakeflowError: If a value is too large or not JSON-serialisable, or an output contains `{{` or is not
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
            raise LakeflowError(f"Output {path!r} of stub {key!r} is not JSON-serialisable: {e}") from None
    # The platform substitutes the literal and would then resolve a reference inside it.
    if "{{" in rendered:
        raise LakeflowError(
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
        LakeflowError: If a task is not in the job or is a for-each task, or a key is longer than 512 characters.
    """
    if not capture_task_values:
        return settings
    tasks: dict[str, jobs.Task] = {}
    for task in settings.tasks or []:
        tasks[task.task_key] = task
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
    _add_helper_compute(settings, helpers)
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
        LakeflowError: If the task is not in the job, the task refers to a task that is not stubbed (its task
            values or any other `{{tasks.<key>.` reference) or to an output its stub does not give, a value is too
            large or not JSON-serialisable, an output contains `{{` or is not JSON-serialisable, or
            `capture_task_values` is given for a for-each task.
    """
    target = require_task(settings, task_key)
    if task_key in upstream_task_values:
        raise LakeflowError(f"upstream_task_values cannot stub {task_key!r}, the task under test")
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
        _add_helper_compute(settings, helpers)
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


def require_task(settings: jobs.JobSettings, task_key: str) -> jobs.Task:
    """Get the job's top-level task `task_key`, raising `LakeflowError` with the available keys if there is none."""
    target = _find_task(settings, task_key)
    if target is None:
        available = sorted(t.task_key for t in settings.tasks or [])
        raise LakeflowError(f"No task {task_key!r} in the job. Available: {available}")
    return target


def _find_task(settings: jobs.JobSettings, task_key: str) -> jobs.Task | None:
    for task in settings.tasks or []:
        if task.task_key == task_key:
            return task
    return None


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
        LakeflowError: If `run` is a run of another job, or an upstream task was not part of it or has no captured
            values.
    """
    if run.job_key != job_key:
        raise LakeflowError(f"{argument} is a run of job {run.job_key!r}, not of {job_key!r}")
    target = _find_task(settings, task_key)
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


def inject_lifecycle(
    settings: jobs.JobSettings,
    setup: str | None,
    teardown: str | None,
    helpers: Helpers,
    params: Collection[str] = (),
) -> jobs.JobSettings:
    """Add a setup task that runs before everything else and a teardown task that runs after everything else.

    Every task without `depends_on`, stubs included, comes to depend on the setup task. The teardown task depends
    on every task that no other task depends on, probes included, with `run_if` `ALL_DONE`, so it runs whether the
    tasks before it passed or failed. Both are notebook tasks on the helper compute, with a `__pytest_params` widget
    as `name_lifecycle_params` writes it.

    Args:
        settings: The job's settings. They are changed in place.
        setup: Workspace path of the setup notebook, or `None` for no setup task.
        teardown: Workspace path of the teardown notebook, or `None` for no teardown task.
        helpers: Where the helper notebooks are and what they run on.
        params: The names of the job parameters the test passes to every run.

    Returns:
        `settings`.
    """
    if setup is None and teardown is None:
        return settings
    tasks = list(settings.tasks or [])
    if setup is not None:
        for task in tasks:
            if not task.depends_on:
                task.depends_on = [jobs.TaskDependency(task_key=SETUP_KEY)]
        tasks.insert(0, _lifecycle_task(SETUP_KEY, setup, helpers))
    if teardown is not None:
        last = _lifecycle_task(TEARDOWN_KEY, teardown, helpers)
        last.depends_on = _leaves(tasks)
        last.run_if = jobs.RunIf.ALL_DONE
        tasks.append(last)
    settings.tasks = tasks
    name_lifecycle_params(settings, params)
    _add_helper_compute(settings, helpers)
    return settings


def _leaves(tasks: Sequence[jobs.Task]) -> list[jobs.TaskDependency]:
    """A dependency on each of `tasks` that no other of them depends on, in order."""
    upstream = {d.task_key for t in tasks for d in t.depends_on or []}
    leaves: list[jobs.TaskDependency] = []
    for task in tasks:
        if task.task_key not in upstream:
            leaves.append(jobs.TaskDependency(task_key=task.task_key))
    return leaves


def rewire_teardown(settings: jobs.JobSettings) -> jobs.JobSettings:
    """Make the teardown task depend again on every task that no other task depends on.

    For tasks replaced after `inject_lifecycle`, such as the stubs of `run(replay=...)`, which drop their
    `depends_on` and so can leave the tasks upstream of them with no task after them.

    Args:
        settings: The job's settings. They are changed in place.

    Returns:
        `settings`.
    """
    others: list[jobs.Task] = []
    teardown = None
    for task in settings.tasks or []:
        if task.task_key == TEARDOWN_KEY:
            teardown = task
        else:
            others.append(task)
    if teardown is not None:
        teardown.depends_on = _leaves(others)
    return settings


def _lifecycle_task(task_key: str, notebook_path: str, helpers: Helpers) -> jobs.Task:
    notebook = jobs.NotebookTask(notebook_path=notebook_path)
    return _on_helper_compute(jobs.Task(task_key=task_key, notebook_task=notebook), helpers)


def name_lifecycle_params(settings: jobs.JobSettings, params: Collection[str]) -> jobs.JobSettings:
    """Write the names of the job's parameters into the `__pytest_params` widget of the setup and teardown tasks.

    The names are the parameters the job declares and then those in `params`, which `allow_undeclared_params` may
    let through undeclared. The notebook reads the parameters from the widgets that the platform fills with them,
    and falls back to these names where `dbutils.widgets.getAll` is missing. `EphemeralJob.create` calls this again
    after `modify_settings`, which may declare more.

    Args:
        settings: The job's settings. They are changed in place.
        params: The names of the job parameters the test passes to every run.

    Returns:
        `settings`.
    """
    widget = escape(lifecycle_params(settings, params))
    for task in settings.tasks or []:
        if task.task_key in (SETUP_KEY, TEARDOWN_KEY) and task.notebook_task is not None:
            task.notebook_task.base_parameters = {**(task.notebook_task.base_parameters or {}), PARAMS_WIDGET: widget}
    return settings


def lifecycle_params(settings: jobs.JobSettings, params: Collection[str]) -> list[str]:
    """The job parameters a setup or teardown step can read: those the job declares, then the others in `params`."""
    names: list[str] = []
    for name in [*_declared_params(settings), *params]:
        if name not in names and not name.startswith(PREFIX):
            names.append(name)
    return names


def check_lifecycle_params(
    job_key: str, settings: jobs.JobSettings, params: Collection[str], argument: str, names: Collection[str]
) -> None:
    """Fail early on a `${name}` in `setup=` or `teardown=` statements that names no parameter of the job.

    Args:
        job_key: The bundle resource key of the job, for error messages.
        settings: The job's settings.
        params: The names of the job parameters the test passes to every run.
        argument: `"setup"` or `"teardown"`, for error messages.
        names: The placeholders the statements use.

    Raises:
        LakeflowError: If a name is neither declared by the job nor in `params`.
    """
    unknown = sorted(set(names) - set(lifecycle_params(settings, params)))
    if unknown:
        declared = sorted(_declared_params(settings))
        raise LakeflowError(
            f"{argument}= reads {unknown}, which job {job_key!r} does not declare and params does not hold. "
            f"Declared: {declared}. Pass them in params, or write $$ for a literal dollar."
        )


def _declared_params(settings: jobs.JobSettings) -> list[str]:
    """The names of the parameters the job declares, in order."""
    names: list[str] = []
    for parameter in settings.parameters or []:
        if parameter.name:
            names.append(parameter.name)
    return names


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


def _check_capturable(task: jobs.Task) -> None:
    # The platform refuses taskValues.set in an iteration, so a probe on a for-each task finds every key missing.
    if task.for_each_task is not None:
        raise LakeflowError(
            f"capture_task_values names the for-each task {task.task_key!r}, whose iterations cannot set task values. "
            "Return the values with dbutils.notebook.exit() and read each iteration's output from "
            f"run.task({task.task_key!r}).iterations instead."
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
        return
    existing = [env.environment_key for env in settings.environments or []]
    if ENV_KEY in existing:
        return
    if len(existing) >= MAX_ENVIRONMENTS:
        raise LakeflowError(
            f"The job already has {len(existing)} environments, the most a job allows is {MAX_ENVIRONMENTS}: "
            f"{existing}. One must go from the bundle job to make room for the helper "
            f"environment {ENV_KEY!r}, or pass helpers_compute to run helper tasks on a job cluster."
        )
    spec = compute.Environment.from_dict({"environment_version": helpers.environment_version})
    environment = jobs.JobEnvironment(environment_key=ENV_KEY, spec=spec)
    settings.environments = [*(settings.environments or []), environment]
