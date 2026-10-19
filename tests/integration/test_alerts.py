r"""Integration tests for the ``alerts`` bundle (tests/integration/bundles/alerts), run in a real workspace.

The bundle deploys three alerts on the warehouse named by the ``warehouse_id`` bundle variable, which these tests read
from ``LAKEFLOW_WAREHOUSE_ID``: ``fires`` (``SELECT 1 AS n``, n > 0), ``quiet`` (the same query, n > 5) and
``broken`` (a query that raises an error). Its ``alert_job`` evaluates each alert in an alert task of the same name.
Run them with:

    LAKEFLOW_INTEGRATION=1 DATABRICKS_CONFIG_PROFILE=<profile> LAKEFLOW_CATALOG=<catalog> \
        LAKEFLOW_WAREHOUSE_ID=<warehouse id> \
        uv run pytest tests/integration/test_alerts.py -n 8 --dist loadgroup --lakeflow-sweep
"""

from __future__ import annotations

import dataclasses
import os

import pytest

from pytest_databricks_lakeflow import Lakeflow

from .conftest import lakeflow_for

WAREHOUSE_ID = os.environ.get("LAKEFLOW_WAREHOUSE_ID")

pytestmark = pytest.mark.skipif(
    not WAREHOUSE_ID, reason="set LAKEFLOW_WAREHOUSE_ID to the id of a SQL warehouse to run the alerts bundle"
)


@pytest.fixture(scope="module")
def lakeflow() -> Lakeflow:
    assert WAREHOUSE_ID is not None
    return dataclasses.replace(lakeflow_for("alerts"), variables={"warehouse_id": WAREHOUSE_ID})


@pytest.mark.parametrize(("key", "state"), [("fires", "TRIGGERED"), ("quiet", "OK")])
def test_alert_task_reports_its_state(lakeflow: Lakeflow, key: str, state: str) -> None:
    with lakeflow.job("alert_job") as job:
        assert job.job_id is not None
        # databricks-sdk parses a missing `subscribers` as [], so read the raw settings to see what the copy holds.
        raw = lakeflow.ws.api_client.do("GET", "/api/2.2/jobs/get", query={"job_id": job.job_id})
        assert isinstance(raw, dict)
        alert_tasks = {task["task_key"]: task["alert_task"] for task in raw["settings"]["tasks"]}
        assert sorted(alert_tasks) == ["broken", "fires", "quiet"]
        for alert_task in alert_tasks.values():
            assert "subscribers" not in alert_task
        run = job.run(only=[key])
        # An alert task succeeds once the alert was evaluated, whether or not it fired.
        assert run.succeeded, run
        assert run.task(key).alert_state == state


def test_alert_task_whose_query_fails_reports_error(lakeflow: Lakeflow) -> None:
    with lakeflow.job("alert_job") as job:
        run = job.run(only=["broken"])
        assert run.failed
        task = run.task("broken")
        assert (task.result_state, task.termination_code) == ("FAILED", "RUN_EXECUTION_ERROR")
        assert task.termination_message is not None
        assert "Query execution failed" in task.termination_message
        assert task.alert_state == "ERROR"
