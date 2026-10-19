r"""Integration tests for the ``sql`` bundle (tests/integration/bundles/sql), run in a real workspace.

The bundle's ``sql_job`` runs SQL files on the warehouse named by the ``warehouse_id`` bundle variable, which
these tests read from ``LAKEFLOW_WAREHOUSE_ID``. Run them with:

    LAKEFLOW_INTEGRATION=1 DATABRICKS_CONFIG_PROFILE=<profile> LAKEFLOW_CATALOG=<catalog> \
        LAKEFLOW_WAREHOUSE_ID=<warehouse id> uv run pytest tests/integration/test_sql.py -n 4 --lakeflow-sweep
"""

from __future__ import annotations

import dataclasses
import os

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


def test_sql_file_task_runs_on_the_warehouse(lakeflow: Lakeflow) -> None:
    with lakeflow.job("sql_job") as job:
        run = job.run(only=["query"])
        assert run.succeeded, run
        task = run.task("query")
        # The query declares no parameters, but the platform adds every job parameter to each SQL file task.
        assert task.parameters == {"n": "3"}
        assert task.sql_output is not None
        result = task.sql_result
        assert result is not None
        assert result.warehouse_id == WAREHOUSE_ID
        assert result.query_text is not None and result.query_text.strip() == "SELECT 1 AS n"
        assert (result.columns, result.types) == (["n"], ["int"])
        assert result.rows == [{"n": "1"}]
        assert result.first_row["n"] == "1"
        assert result.truncated is False


def test_sql_result_values_are_strings_and_null_is_none(lakeflow: Lakeflow) -> None:
    with lakeflow.job("sql_job") as job:
        run = job.run(only=["types"])
        assert run.succeeded, run
        result = run.task("types").sql_result
        assert result is not None
        assert result.types == ["int", "null", "decimal", "date", "boolean"]
        assert result.first_row == {"n": "1", "m": None, "d": "1.5", "day": "2026-01-01", "b": "true"}


def test_sql_result_is_cut_at_1000_rows(lakeflow: Lakeflow) -> None:
    with lakeflow.job("sql_job") as job:
        run = job.run(only=["wide"])
        assert run.succeeded, run
        result = run.task("wide").sql_result
        assert result is not None
        assert result.truncated is True
        assert len(result.rows) == 1000
        assert (result.rows[0], result.rows[-1]) == ({"id": "0"}, {"id": "999"})


def test_sql_result_keeps_parameter_markers(lakeflow: Lakeflow) -> None:
    with lakeflow.job("sql_job", params={"n": "7"}) as job:
        run = job.run(only=["parameterised"])
        assert run.succeeded, run
        task = run.task("parameterised")
        assert task.parameters == {"n": "7"}
        result = task.sql_result
        assert result is not None
        assert result.query_text is not None and result.query_text.strip() == "SELECT :n AS n"
        assert result.first_row == {"n": "7"}


def test_task_params_merge_into_sql_parameters(lakeflow: Lakeflow) -> None:
    task_params = {"labelled": {"label": "from-task-params"}, "parameterised": {"n": "9"}}
    with pytest.warns(UserWarning, match="sets 'n', also a job parameter of 'sql_job'. The job value wins"):
        job = lakeflow.job("sql_job", task_params=task_params)
    with job:
        run = job.run(only=["labelled", "parameterised"])
        assert run.succeeded, run
        labelled = run.task("labelled")
        assert labelled.parameters == {"label": "from-task-params", "n": "3"}
        assert labelled.sql_result is not None
        assert labelled.sql_result.first_row == {"label": "from-task-params"}
        parameterised = run.task("parameterised")
        assert parameterised.parameters == {"n": "3"}, "the job parameter wins over the task parameter of the same name"
        assert parameterised.sql_result is not None
        assert parameterised.sql_result.first_row == {"n": "3"}
