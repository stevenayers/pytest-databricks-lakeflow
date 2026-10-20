"""Where the copy's tasks run: clusters, serverless environments, dependencies and the helper compute."""

from __future__ import annotations

import re
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, fields
from typing import Any

from databricks.sdk.service import compute, jobs

from pytest_databricks_lakeflow.bundle import split_extras
from pytest_databricks_lakeflow.errors import LakeflowError
from pytest_databricks_lakeflow.limits import MAX_ENVIRONMENTS
from pytest_databricks_lakeflow.naming import ENV_KEY, PREFIX
from pytest_databricks_lakeflow.settings.tasks import disabled_task_keys, enabled_tasks, with_nested

SINGLE_NODE_CONF = {"spark.master": "local[*, 4]", "spark.databricks.cluster.profile": "singleNode"}
SINGLE_NODE_TAGS = {"ResourceClass": "SingleNode"}
SHARED_ACCESS_MODES = frozenset(
    {compute.DataSecurityMode.USER_ISOLATION, compute.DataSecurityMode.DATA_SECURITY_MODE_STANDARD}
)


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


def serverless(task: jobs.Task) -> bool:
    runs_code = task.notebook_task or task.spark_python_task or task.python_wheel_task or task.spark_jar_task
    on_warehouse = task.notebook_task and task.notebook_task.warehouse_id
    return bool(runs_code) and not on_cluster(task) and not on_warehouse


def on_cluster(task: jobs.Task) -> bool:
    return bool(task.job_cluster_key or task.new_cluster or task.existing_cluster_id)


def set_environment_version(settings: jobs.JobSettings, version: str) -> None:
    for env in settings.environments or []:
        if env.environment_key == ENV_KEY:
            continue
        if env.spec and env.spec.base_environment:
            continue
        if env.spec is None:
            env.spec = compute.Environment.from_dict({})
        env.spec.environment_version = version


def use_existing_cluster(settings: jobs.JobSettings, cluster_id: str) -> None:
    for task in with_nested(settings.tasks):
        if task.job_cluster_key or task.new_cluster:
            task.existing_cluster_id = cluster_id
            task.job_cluster_key = None
            task.new_cluster = None
    settings.job_clusters = None


def single_node_clusters(settings: jobs.JobSettings) -> None:
    clusters: list[tuple[str | None, compute.ClusterSpec]] = []
    for job_cluster in settings.job_clusters or []:
        if job_cluster.new_cluster:
            clusters.append((job_cluster.job_cluster_key, job_cluster.new_cluster))
    for task in with_nested(settings.tasks):
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


def check_compute(settings: jobs.JobSettings) -> None:
    """Fail early on tasks that the Jobs API rejects on serverless because they have no environment.

    Args:
        settings: The job's settings.

    Raises:
        LakeflowError: If a Python script, Python wheel, JAR or dbt task, nested `for_each_task` tasks included, has no
            cluster and no `environment_key`.
    """
    missing: list[str] = []
    for task in with_nested(settings.tasks):
        needs_environment = task.spark_python_task or task.python_wheel_task or task.spark_jar_task or task.dbt_task
        if needs_environment and not on_cluster(task) and not task.environment_key:
            missing.append(task.task_key)
    if missing:
        raise LakeflowError(
            f"Tasks {missing} have no cluster and no environment_key. Serverless Python script, Python wheel, JAR "
            "and dbt tasks need an environment_key that names one of the job's environments."
        )


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
        ValueError: If a dependency is a local path that does not start with `./` or `../`.
        LakeflowError: If a wheel path with extras would reach a task on a cluster, whose `libraries[].whl` takes no
            extras. Nothing is changed in either case.
    """
    if not dependencies:
        return settings
    local = [dep for dep in dependencies if _is_local_path(split_extras(dep)[0])]
    if local:
        raise ValueError(
            f"Local paths in extra_dependencies must start with ./ or ../, relative to the bundle root: "
            f"{', '.join(map(repr, local))}. Otherwise pass a PyPI requirement or a /Workspace or /Volumes path."
        )
    cluster_tasks = []
    for task in with_nested(settings.tasks):
        runs_python = task.notebook_task or task.spark_python_task or task.python_wheel_task
        if runs_python and on_cluster(task):
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
    for task in enabled_tasks(settings, skip=disabled_task_keys(settings)):
        if task.task_key.startswith(PREFIX) or task.task_key in stubs:
            continue
        if task.notebook_task is None or task.notebook_task.warehouse_id:
            continue
        if not _holds_dependencies(task, environments, dependencies):
            unreached.append(task.task_key)
    return unreached


def _holds_dependencies(task: jobs.Task, environments: Mapping[str, list[str]], dependencies: Sequence[str]) -> bool:
    if on_cluster(task):
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
        ValueError: If a key starts with `__pytest_`, or an override is not a mapping or holds a field that
            `compute.Environment` does not have.
        LakeflowError: If a spec would set both `environment_version` and `base_environment`, or the job would have
            more than 10 environments. Nothing is changed in either case.
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
        ValueError: If a key starts with `__pytest_`, or an override is not a mapping or holds a field that
            `compute.Environment` does not have.
    """
    reserved = sorted(key for key in environments if key.startswith(PREFIX))
    if reserved:
        raise ValueError(f"Environment keys starting with {PREFIX!r} are reserved for the plugin: {reserved}")
    known = sorted(f.name for f in fields(compute.Environment))
    for key, override in environments.items():
        if not isinstance(override, Mapping):
            raise ValueError(
                f"environments[{key!r}] must be a mapping of spec fields to values, not {type(override).__name__}"
            )
        # Environment.from_dict drops a field it does not know without a word.
        unknown = sorted(set(override) - set(known))
        if unknown:
            raise ValueError(
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


def on_helper_compute(task: jobs.Task, helpers: Helpers) -> jobs.Task:
    if helpers.job_cluster_key:
        task.job_cluster_key = helpers.job_cluster_key
    else:
        task.environment_key = ENV_KEY
    return task


def add_helper_compute(settings: jobs.JobSettings, helpers: Helpers) -> None:
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
