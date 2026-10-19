r"""Integration tests for the ``sql`` bundle (tests/integration/bundles/sql), run in a real workspace.

The bundle's ``sql_job`` runs a SQL file on the warehouse named by the ``warehouse_id`` bundle variable, which
these tests read from ``LAKEFLOW_WAREHOUSE_ID``. Run them with:

    LAKEFLOW_INTEGRATION=1 DATABRICKS_CONFIG_PROFILE=<profile> LAKEFLOW_CATALOG=<catalog> \
        LAKEFLOW_WAREHOUSE_ID=<warehouse id> uv run pytest tests/integration/test_sql.py -n 4 --lakeflow-sweep
"""

from __future__ import annotations

import dataclasses
import os
from typing import Any

import pytest

from pytest_databricks_lakeflow import Lakeflow

from .conftest import lakeflow_for

WAREHOUSE_ID = os.environ.get("LAKEFLOW_WAREHOUSE_ID")

pytestmark = pytest.mark.skipif(
    not WAREHOUSE_ID, reason="set LAKEFLOW_WAREHOUSE_ID to the id of a SQL warehouse to run the sql bundle"
)


@pytest.fixture(scope="module")
def lakeflow() -> Lakeflow:
    assert WAREHOUSE_ID is not None
    return dataclasses.replace(lakeflow_for("sql"), variables={"warehouse_id": WAREHOUSE_ID})


def raw_run_output(lakeflow: Lakeflow, run_id: int) -> dict[str, Any]:
    """Fetch a task run's output as the REST API returns it, including fields the SDK does not model."""
    response = lakeflow.ws.api_client.do("GET", "/api/2.2/jobs/runs/get-output", query={"run_id": run_id})
    assert isinstance(response, dict)
    return response


def test_sql_file_task_runs_on_the_warehouse(lakeflow: Lakeflow) -> None:
    with lakeflow.job("sql_job") as job:
        run = job.run()
        assert run.succeeded, run
        task = run.task("query")
        # The platform reports a SQL file task under sql_output.file_output, which databricks-sdk's SqlOutput does
        # not model, so the plugin sees an empty SqlOutput. query_output and output_link belong to saved-query
        # tasks only. Loosen these two assertions once the SDK or the plugin surfaces file_output.
        assert task.sql_output is not None
        assert task.sql_output.query_output is None
        assert task.alert_output is None
        assert task.alert_state is None
        file_output = raw_run_output(lakeflow, task.run_id)["sql_output"]["file_output"]
        assert file_output["warehouse_id"] == WAREHOUSE_ID
        assert file_output["query_text"].strip() == "SELECT 1 AS n"
        assert file_output["query_result"]["rows"] == [{"values": [{"string_value": "1"}]}]
