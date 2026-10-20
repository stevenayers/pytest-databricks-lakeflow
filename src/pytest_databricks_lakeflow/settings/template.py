"""Make a copied template safe to create as a throwaway job or pipeline: strip, rename, tag and time it."""

from __future__ import annotations

import inspect
import logging
import math
from collections.abc import Mapping
from dataclasses import fields
from datetime import timedelta
from typing import Any, Literal, get_args

from databricks.sdk.service import jobs, pipelines

from pytest_databricks_lakeflow.errors import LakeflowError
from pytest_databricks_lakeflow.limits import (
    MAX_DESCRIPTION_BYTES,
    MAX_NAME_BYTES,
    MAX_PIPELINE_NAME_CHARS,
    MAX_TAGS,
    TIMEOUT_GRACE_SECONDS,
)
from pytest_databricks_lakeflow.naming import ATTRIBUTION_TAGS, NAME_PREFIX, NEWLINES, tag_value, trim
from pytest_databricks_lakeflow.settings.compute import (
    serverless,
    set_environment_version,
    single_node_clusters,
    use_existing_cluster,
)
from pytest_databricks_lakeflow.settings.tasks import with_nested

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
RunAs = Literal["keep"] | str | jobs.JobRunAs | None
SideEffects = Literal["neutralise", "keep"]
SIDE_EFFECTS: tuple[str, ...] = get_args(SideEffects)


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
        for task in with_nested(settings.tasks):
            if serverless(task):
                task.disable_auto_optimization = True
    if environment_version is not None:
        set_environment_version(settings, environment_version)
    if budget_policy_id is not None:
        settings.budget_policy_id = budget_policy_id
    if existing_cluster_id is not None:
        use_existing_cluster(settings, existing_cluster_id)
    if single_node:
        single_node_clusters(settings)
    if performance_optimized:
        settings.performance_target = jobs.PerformanceTarget.PERFORMANCE_OPTIMIZED
    settings.max_concurrent_runs = 1
    if timeout is not None:
        _mirror_timeout(settings, timeout)
    settings.name = trim(NAME_PREFIX + NEWLINES.sub(" ", name), MAX_NAME_BYTES)
    if description is not None:
        parts = [description, settings.description] if settings.description else [description]
        settings.description = trim("\n\n".join(parts), MAX_DESCRIPTION_BYTES)
    settings.tags = _merged_tags(settings.tags, tags, "job")
    return settings


def _run_as(run_as: str | jobs.JobRunAs | None) -> jobs.JobRunAs | None:
    if run_as is None or isinstance(run_as, jobs.JobRunAs):
        return run_as
    if "@" in run_as:
        return jobs.JobRunAs(user_name=run_as)
    return jobs.JobRunAs(service_principal_name=run_as)


def _pause_subscriptions(task: jobs.Task) -> None:
    if task.dashboard_task and task.dashboard_task.subscription:
        task.dashboard_task.subscription.paused = True
    if task.sql_task and task.sql_task.alert:
        task.sql_task.alert.pause_subscriptions = True


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
        log.debug(f"Left out the tags {dropped}, because the {what} already has {MAX_TAGS} tags")
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
        ValueError: If `schema` is not a full name.
        LakeflowError: If the spec publishes to the Hive metastore, or the copy would have more than 25 tags without
            the attribution tags.
    """
    catalog, _, schema_name = schema.partition(".")
    if not schema_name:
        raise ValueError(f"schema must be a full name, catalog.schema, as lakeflow.schema() yields: {schema!r}")
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
