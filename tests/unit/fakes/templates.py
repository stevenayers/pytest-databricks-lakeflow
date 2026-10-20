"""Deployed bundle jobs, pipelines, saved queries and wheels, as the tests find them in the fake workspace."""

from __future__ import annotations

import os
import re
import zipfile
from pathlib import Path

from databricks.sdk.service import jobs, pipelines, sql


def example_settings() -> jobs.JobSettings:
    """Build a deployed bundle job: extract, transform and load, with notifications, a schedule and parameters."""
    nb = "/Workspace/Users/someone@example.com/.bundle/example/test/files/src"
    return jobs.JobSettings.from_dict(
        {
            "name": "[dev someone] my_job",
            "deployment": {"kind": "BUNDLE", "metadata_file_path": "/Workspace/x/state/metadata.json"},
            "edit_mode": "UI_LOCKED",
            "schedule": {"quartz_cron_expression": "0 0 * * * ?", "timezone_id": "UTC", "pause_status": "PAUSED"},
            "email_notifications": {"on_failure": ["someone@example.com"]},
            "run_as": {"user_name": "someone@example.com"},
            "max_concurrent_runs": 4,
            "tags": {"dev": "someone", "purpose": "pytest"},
            "parameters": [{"name": "param1", "default": "hello"}, {"name": "schema", "default": ""}],
            "environments": [{"environment_key": "default", "spec": {"environment_version": "4"}}],
            "job_clusters": [{"job_cluster_key": "classic", "new_cluster": {"spark_version": "17.3.x-scala2.13"}}],
            "tasks": [
                {"task_key": "extract", "notebook_task": {"notebook_path": f"{nb}/extract"}},
                {
                    "task_key": "transform",
                    "depends_on": [{"task_key": "extract"}],
                    "notebook_task": {
                        "notebook_path": f"{nb}/transform",
                        "base_parameters": {"row_count": "{{tasks.extract.values.row_count}}"},
                    },
                    "email_notifications": {"on_failure": ["someone@example.com"]},
                },
                {
                    "task_key": "load",
                    "depends_on": [{"task_key": "transform"}],
                    "environment_key": "default",
                    "python_wheel_task": {"package_name": "pkg", "entry_point": "load"},
                },
                {
                    "task_key": "classic",
                    "job_cluster_key": "classic",
                    "notebook_task": {"notebook_path": f"{nb}/classic"},
                },
            ],
        }
    )


def pipeline_settings() -> jobs.JobSettings:
    """Build a deployed bundle job with a pipeline task and a notebook task that depends on it."""
    nb = "/Workspace/Users/someone@example.com/.bundle/example/test/files/src"
    return jobs.JobSettings.from_dict(
        {
            "name": "[dev someone] pipeline_job",
            "tasks": [
                {"task_key": "refresh", "pipeline_task": {"pipeline_id": "abc"}},
                {
                    "task_key": "check",
                    "depends_on": [{"task_key": "refresh"}],
                    "notebook_task": {"notebook_path": f"{nb}/check"},
                },
            ],
        }
    )


def pipeline_spec() -> pipelines.PipelineSpec:
    """Build the spec of a deployed bundle pipeline, `abc`: what a copy must drop, and a glob library to keep.

    `pipeline_settings()` points its pipeline task at it, so `fake_ws.pipelines.specs["abc"] = pipeline_spec()` wires
    the two together.
    """
    files = "/Workspace/Users/someone@example.com/.bundle/example/test/files"
    return pipelines.PipelineSpec.from_dict(
        {
            "id": "abc",
            "name": "[dev someone] my_pipeline",
            "catalog": "prod_catalog",
            "schema": "prod",
            "serverless": True,
            "development": False,
            "continuous": True,
            "channel": "CURRENT",
            "deployment": {"kind": "BUNDLE", "metadata_file_path": "/Workspace/x/state/metadata.json"},
            "notifications": [{"email_recipients": ["someone@example.com"], "alerts": ["on-update-failure"]}],
            "event_log": {"catalog": "prod_catalog", "schema": "ops", "name": "event_log"},
            "trigger": {"cron": {"quartz_cron_schedule": "0 0 * * * ?", "timezone_id": "UTC"}},
            "libraries": [{"glob": {"include": f"{files}/src/pipeline/**"}}],
            "root_path": f"{files}/src/pipeline",
            "configuration": {"schema": "prod"},
            "tags": {"dev": "someone"},
        }
    )


def reporting_settings() -> jobs.JobSettings:
    """Build a reporting job: subscribed dashboard and SQL alert tasks, alert and Power BI tasks, and a notify task."""
    return jobs.JobSettings.from_dict(
        {
            "name": "[dev someone] reporting",
            "tasks": [
                {
                    "task_key": "dashboard",
                    "dashboard_task": {
                        "dashboard_id": "d1",
                        "warehouse_id": "w1",
                        "subscription": {
                            "custom_subject": "Daily report",
                            "subscribers": [{"user_name": "someone@example.com"}, {"destination_id": "slack"}],
                        },
                    },
                },
                {
                    "task_key": "alert",
                    "sql_task": {
                        "warehouse_id": "w1",
                        "alert": {"alert_id": "a1", "subscriptions": [{"user_name": "someone@example.com"}]},
                    },
                },
                {
                    "task_key": "check",
                    "alert_task": {
                        "alert_id": "a2",
                        "warehouse_id": "w1",
                        "subscribers": [{"user_name": "someone@example.com"}],
                    },
                },
                {
                    "task_key": "refresh_powerbi",
                    "power_bi_task": {
                        "connection_resource_name": "pbi",
                        "warehouse_id": "w1",
                        "refresh_after_update": True,
                        "power_bi_model": {
                            "workspace_name": "Reports",
                            "model_name": "Sales",
                            "storage_mode": "IMPORT",
                            "authentication_method": "OAUTH",
                            "overwrite_existing": False,
                        },
                        "tables": [{"catalog": "main", "schema": "gold", "name": "sales", "storage_mode": "IMPORT"}],
                    },
                },
                {
                    "task_key": "notify",
                    "depends_on": [
                        {"task_key": "dashboard"},
                        {"task_key": "alert"},
                        {"task_key": "check"},
                        {"task_key": "refresh_powerbi"},
                    ],
                    "notebook_task": {"notebook_path": "/Workspace/src/notify"},
                },
            ],
        }
    )


def parent_settings() -> jobs.JobSettings:
    """Build a parent job: a notebook task, then a Run Job task that triggers the deployed job 123."""
    return jobs.JobSettings.from_dict(
        {
            "name": "[dev someone] parent",
            "deployment": {"kind": "BUNDLE", "metadata_file_path": "/Workspace/x/state/metadata.json"},
            "parameters": [{"name": "schema", "default": ""}],
            "tasks": [
                {"task_key": "prepare", "notebook_task": {"notebook_path": "/Workspace/src/prepare"}},
                {
                    "task_key": "trigger_child",
                    "depends_on": [{"task_key": "prepare"}],
                    "run_job_task": {"job_id": 123, "job_parameters": {"schema": "{{job.parameters.schema}}"}},
                },
            ],
        }
    )


def branching_settings() -> jobs.JobSettings:
    """Build an if/else job like the `branches` bundle: `check` compares a value `check_input` sets, two branches."""
    nb = "/Workspace/Users/someone@example.com/.bundle/lakeflow_branches/test/files/src/notebooks"
    return jobs.JobSettings.from_dict(
        {
            "name": "[dev someone] branching_job",
            "parameters": [{"name": "rows", "default": "0"}],
            "tasks": [
                {"task_key": "check_input", "notebook_task": {"notebook_path": f"{nb}/check_input"}},
                {
                    "task_key": "check",
                    "depends_on": [{"task_key": "check_input"}],
                    "condition_task": {
                        "op": "GREATER_THAN",
                        "left": "{{tasks.check_input.values.rows}}",
                        "right": "0",
                    },
                },
                {
                    "task_key": "on_true",
                    "depends_on": [{"task_key": "check", "outcome": "true"}],
                    "notebook_task": {"notebook_path": f"{nb}/on_true"},
                },
                {
                    "task_key": "on_false",
                    "depends_on": [{"task_key": "check", "outcome": "false"}],
                    "notebook_task": {"notebook_path": f"{nb}/on_false"},
                },
            ],
        }
    )


def for_each_settings() -> jobs.JobSettings:
    """Build a fan-out job: `loop` repeats the notebook task `each` over three inputs, then `after` runs."""
    nb = "/Workspace/Users/someone@example.com/.bundle/lakeflow_for_each/test/files/src/notebooks"
    return jobs.JobSettings.from_dict(
        {
            "name": "[dev someone] fanout",
            "tasks": [
                {
                    "task_key": "loop",
                    "for_each_task": {
                        "inputs": '["a", "b", "c"]',
                        "task": {
                            "task_key": "each",
                            "notebook_task": {"notebook_path": f"{nb}/each", "base_parameters": {"item": "{{input}}"}},
                        },
                    },
                },
                {
                    "task_key": "after",
                    "depends_on": [{"task_key": "loop"}],
                    "notebook_task": {"notebook_path": f"{nb}/after"},
                },
            ],
        }
    )


def fanout_objects_settings() -> jobs.JobSettings:
    """Build the `for_each` bundle's `fanout_objects`: `extract`, then `loop` over objects with the body `inner`.

    `inner` reads the whole item, its `id` and a task value of `extract`, and `after` runs once `loop` is done.
    """
    nb = "/Workspace/Users/someone@example.com/.bundle/lakeflow_for_each/test/files/src/notebooks"
    return jobs.JobSettings.from_dict(
        {
            "name": "[dev someone] fanout_objects",
            "tasks": [
                {"task_key": "extract", "notebook_task": {"notebook_path": f"{nb}/extract"}},
                {
                    "task_key": "loop",
                    "depends_on": [{"task_key": "extract"}],
                    "for_each_task": {
                        "inputs": '[{"id": 1, "path": "/a"}, {"id": 2, "path": "/b"}]',
                        "concurrency": 2,
                        "task": {
                            "task_key": "inner",
                            "timeout_seconds": 600,
                            "notebook_task": {
                                "notebook_path": f"{nb}/inner",
                                "base_parameters": {
                                    "item": "{{input}}",
                                    "item_id": "{{input.id}}",
                                    "rows": "{{tasks.extract.values.rows}}",
                                },
                            },
                        },
                    },
                },
                {
                    "task_key": "after",
                    "depends_on": [{"task_key": "loop"}],
                    "notebook_task": {"notebook_path": f"{nb}/after"},
                },
            ],
        }
    )


def dbt_settings() -> jobs.JobSettings:
    """Build a deployed bundle job with a serverless dbt task, `dbt_build`, and a notebook task downstream of it."""
    files = "/Workspace/Users/someone@example.com/.bundle/analytics/test/files"
    return jobs.JobSettings.from_dict(
        {
            "name": "[dev someone] analytics",
            "environments": [{"environment_key": "dbt", "spec": {"environment_version": "4"}}],
            "tasks": [
                {
                    "task_key": "dbt_build",
                    "environment_key": "dbt",
                    "dbt_task": {
                        "commands": ["dbt deps", "dbt build"],
                        "warehouse_id": "w1",
                        "catalog": "prod_catalog",
                        "schema": "analytics",
                        "project_directory": f"{files}/dbt",
                    },
                },
                {
                    "task_key": "check",
                    "depends_on": [{"task_key": "dbt_build"}],
                    "notebook_task": {"notebook_path": f"{files}/src/check"},
                },
            ],
        }
    )


def saved_query_settings() -> jobs.JobSettings:
    """Build a reporting job: `totals` runs the saved query q1 after `extract`, and `loop` runs q2 for each input."""
    return jobs.JobSettings.from_dict(
        {
            "name": "[dev someone] saved_queries",
            "parameters": [{"name": "run_date", "default": "2026-10-20"}],
            "tasks": [
                {"task_key": "extract", "notebook_task": {"notebook_path": "/Workspace/src/extract"}},
                {
                    "task_key": "totals",
                    "depends_on": [{"task_key": "extract"}],
                    "sql_task": {"warehouse_id": "w1", "query": {"query_id": "q1"}, "parameters": {"limit": "5"}},
                },
                {
                    "task_key": "loop",
                    "for_each_task": {
                        "inputs": '["a", "b"]',
                        "task": {"task_key": "each", "sql_task": {"warehouse_id": "w1", "query": {"query_id": "q2"}}},
                    },
                },
            ],
        }
    )


QUERY_TEXT = "SELECT count(*) AS n FROM orders WHERE region = :region AND day = :run_date LIMIT :limit"


def saved_queries() -> dict[str, sql.Query]:
    """The saved queries of `saved_query_settings`, as `queries.get` returns them, by id.

    q1 sets a catalog and schema and has a text, a numeric and a task-set parameter, and q2 a legacy `{{ }}` marker
    with a single-value enum default and neither catalog nor schema.
    """
    return {
        "q1": sql.Query.from_dict(
            {
                "id": "q1",
                "query_text": QUERY_TEXT,
                "catalog": "main",
                "schema": "sales",
                "parameters": [
                    {"name": "region", "text_value": {"value": "emea"}},
                    {"name": "limit", "numeric_value": {"value": 10}},
                    {"name": "ratio", "numeric_value": {"value": 2.5}},
                    {"name": "run_date", "date_value": {"date_value": "2026-01-01"}},
                ],
            }
        ),
        "q2": sql.Query.from_dict(
            {
                "id": "q2",
                "query_text": "SELECT {{ size }} AS size",
                "parameters": [{"name": "size", "enum_value": {"values": ["small"], "enum_options": "small\nlarge"}}],
            }
        ),
    }


def triggered_settings() -> jobs.JobSettings:
    """Build `example_settings` with `transform` reading the trigger type and the tables a table update trigger saw."""
    settings = example_settings()
    transform = next(task for task in settings.tasks or [] if task.task_key == "transform")
    assert transform.notebook_task is not None
    transform.notebook_task.base_parameters = {
        **(transform.notebook_task.base_parameters or {}),
        "kind": "{{job.trigger.type}}",
        "tables": "{{job.trigger.table_update.updated_tables}}",
    }
    return settings


def build_wheel(
    dist: Path,
    name: str = "pkg",
    *,
    version: str = "0.1.0",
    entry_points: str = "[console_scripts]\nmain = pkg.main:main\n",
    init: str = "",
    modified: float | None = None,
) -> Path:
    """Write a wheel of project `name` to `dist`, with `entry_points` as its entry_points.txt and `init` in its package.

    The package is `name` with underscores, as `uv build` names it, and `modified` is the wheel's modification time.
    """
    package = re.sub(r"[-.]+", "_", name).lower()
    stem = f"{package}-{version}"
    dist.mkdir(parents=True, exist_ok=True)
    path = dist / f"{stem}-py3-none-any.whl"
    with zipfile.ZipFile(path, "w") as wheel:
        wheel.writestr(f"{package}/__init__.py", init)
        wheel.writestr(f"{package}/main.py", "def main():\n    pass\n")
        wheel.writestr(f"{stem}.dist-info/METADATA", f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n")
        if entry_points:
            wheel.writestr(f"{stem}.dist-info/entry_points.txt", entry_points)
    if modified is not None:
        os.utime(path, (modified, modified))
    return path
