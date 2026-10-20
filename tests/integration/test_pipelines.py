r"""Integration tests for the ``pipelines`` bundle (tests/integration/bundles/pipelines), run in a real workspace.

The bundle deploys a schema in the catalog from ``LAKEFLOW_CATALOG``, passed as the bundle variable ``catalog``, and
a serverless pipeline, ``numbers``, whose one materialised view over three literal rows lives in that schema. The
job ``pipeline_job`` has one pipeline task, ``refresh``, that updates it. The deployed pipeline's table and schema
belong to the deployment, which ``--lakeflow-destroy`` removes. Every test that runs the deployed pipeline shares one
run, marked with the ``xdist_group`` ``refresh_run``, because a pipeline runs one update at a time. The tests that copy
the pipeline publish the copy to a ``lakeflow.schema()`` schema of their own and check that no copy is left behind.
Run them with:

    LAKEFLOW_INTEGRATION=1 DATABRICKS_CONFIG_PROFILE=<profile> LAKEFLOW_CATALOG=<catalog> \
        uv run pytest tests/integration/test_pipelines.py -n 8 --dist loadgroup --lakeflow-sweep
"""

from __future__ import annotations

import dataclasses
import os
from collections.abc import Iterator

import pytest
from databricks.sdk.errors import NotFound
from databricks.sdk.service import pipelines

from pytest_databricks_lakeflow import JobRun, Lakeflow

from .conftest import lakeflow_for

CATALOG = os.environ.get("LAKEFLOW_CATALOG")

# The spec fields that a pipeline copy changes; every other field is the deployed pipeline's.
CHANGED_FIELDS = {"id", "name", "schema", "development", "tags", "deployment"}

pytestmark = pytest.mark.skipif(not CATALOG, reason="set LAKEFLOW_CATALOG to the catalog the pipeline publishes to")


@pytest.fixture(scope="module")
def lakeflow() -> Lakeflow:
    assert CATALOG is not None
    return dataclasses.replace(lakeflow_for("pipelines"), variables={"catalog": CATALOG})


@pytest.fixture(scope="module")
def refresh_run(lakeflow: Lakeflow) -> Iterator[tuple[JobRun, str]]:
    with lakeflow.job("pipeline_job") as job:
        pipeline_task = job.settings.tasks[0].pipeline_task if job.settings.tasks else None
        assert pipeline_task is not None
        yield job.run(full_refresh=True), pipeline_task.pipeline_id


@pytest.mark.xdist_group("refresh_run")
def test_pipeline_full_refresh(refresh_run: tuple[JobRun, str]) -> None:
    run, pipeline_id = refresh_run
    assert run.succeeded, run
    # runs/get-output reports an error for every pipeline task run, which the plugin drops when the task succeeded.
    assert run.task("refresh").error is None
    update = run.task("refresh").pipeline_update
    assert update is not None
    assert update.pipeline_id == pipeline_id
    assert update.update_id
    assert update.state == "COMPLETED"
    assert update.cause == "JOB_TASK"
    assert update.full_refresh is True
    assert update.events(level="ERROR") == []


@pytest.mark.xdist_group("refresh_run")
def test_pipeline_update_events_belong_to_the_update(refresh_run: tuple[JobRun, str]) -> None:
    run, _ = refresh_run
    update = run.task("refresh").pipeline_update
    assert update is not None
    events = update.events()
    assert events
    assert {event.origin.update_id if event.origin else None for event in events} == {update.update_id}
    assert {event.level for event in update.events(level="INFO")} == {pipelines.EventLevel.INFO}


def deployed_pipeline_id(lakeflow: Lakeflow) -> str:
    [refresh] = lakeflow.template("pipeline_job").tasks or []
    pipeline_task = refresh.pipeline_task
    assert pipeline_task is not None
    return pipeline_task.pipeline_id


def tables_in(lakeflow: Lakeflow, schema: str) -> set[str]:
    catalog, _, name = schema.partition(".")
    return {table.name or "" for table in lakeflow.ws.tables.list(catalog, name)}


def test_pipeline_copy_writes_to_the_test_schema(lakeflow: Lakeflow) -> None:
    ws = lakeflow.ws
    deployed = ws.pipelines.get(deployed_pipeline_id(lakeflow)).spec
    assert deployed is not None
    with lakeflow.schema() as schema, lakeflow.pipeline("numbers", schema=schema) as pipeline:
        update = pipeline.run(full_refresh=True)
        assert update.state == "COMPLETED", update
        assert (update.cause, update.full_refresh) == ("API_CALL", True)
        assert update.events(level="ERROR") == []
        assert "numbers" in tables_in(lakeflow, schema)
        assert pipeline.pipeline_id is not None
        copy_id = pipeline.pipeline_id
        copied = ws.pipelines.get(copy_id).spec
        assert copied is not None
        assert copied.schema == schema.partition(".")[2] != deployed.schema
        assert copied.development is True
        assert copied.deployment is None
        assert {**(deployed.tags or {}), "pytest": ""}.items() <= (copied.tags or {}).items()
        unchanged = {key: value for key, value in deployed.as_dict().items() if key not in CHANGED_FIELDS}
        assert {key: value for key, value in copied.as_dict().items() if key not in CHANGED_FIELDS} == unchanged
    with pytest.raises(NotFound):
        ws.pipelines.get(copy_id)


def test_job_copy_points_at_the_pipeline_copy(lakeflow: Lakeflow) -> None:
    deployed_id = deployed_pipeline_id(lakeflow)
    with lakeflow.schema() as schema, lakeflow.job("pipeline_job", pipelines="copy", schema=schema) as job:
        [copy] = job.pipelines
        assert copy.pipeline_key == "numbers"
        run = job.run(full_refresh=True)
        assert run.succeeded, run
        update = run.task("refresh").pipeline_update
        assert update is not None
        assert update.pipeline_id == copy.pipeline_id != deployed_id
        assert (update.state, update.cause) == ("COMPLETED", "JOB_TASK")
        assert lakeflow.ws.tables.exists(f"{schema}.numbers").table_exists
    with pytest.raises(NotFound):
        lakeflow.ws.pipelines.get(str(copy.pipeline_id))
