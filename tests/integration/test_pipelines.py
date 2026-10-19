r"""Integration tests for the ``pipelines`` bundle (tests/integration/bundles/pipelines), run in a real workspace.

The bundle deploys a schema in the catalog from ``LAKEFLOW_CATALOG``, passed as the bundle variable ``catalog``, and
a serverless pipeline, ``numbers``, whose one materialised view over three literal rows lives in that schema. The
job ``pipeline_job`` has one pipeline task, ``refresh``, that updates it. The tests create no tables or schemas of
their own: the pipeline's table and schema belong to the deployment, which ``--lakeflow-destroy`` removes. Every
test that runs the pipeline shares one run, marked with the ``xdist_group`` ``refresh_run``, because the pipeline is
the deployed one and runs one update at a time. Run them with:

    LAKEFLOW_INTEGRATION=1 DATABRICKS_CONFIG_PROFILE=<profile> LAKEFLOW_CATALOG=<catalog> \
        uv run pytest tests/integration/test_pipelines.py -n 8 --dist loadgroup --lakeflow-sweep
"""

from __future__ import annotations

import dataclasses
import os
from collections.abc import Iterator

import pytest
from databricks.sdk.service import pipelines

from pytest_databricks_lakeflow import JobRun, Lakeflow

from .conftest import lakeflow_for

CATALOG = os.environ.get("LAKEFLOW_CATALOG")

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
