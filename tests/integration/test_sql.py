r"""Integration tests for the ``sql`` bundle (tests/integration/bundles/sql), run in a real workspace.

The bundle's ``sql_job`` runs SQL files on the warehouse named by the ``warehouse_id`` bundle variable, which
these tests read from ``LAKEFLOW_WAREHOUSE_ID``, and its ``saved_query_job`` runs a saved query that the module
creates and passes as the ``query_id`` bundle variable. The whole module is in the ``xdist_group`` ``sql_run``, so
that ``--dist loadgroup`` sends it to one worker: each worker would create its own saved query, and so deploy the
target with its own variables at the same time as the others. The tests that only read a task's result share one
run of the tasks they read.
Run them with:

    LAKEFLOW_INTEGRATION=1 DATABRICKS_CONFIG_PROFILE=<profile> LAKEFLOW_CATALOG=<catalog> \
        LAKEFLOW_WAREHOUSE_ID=<warehouse id> \
        uv run pytest tests/integration/test_sql.py -n 8 --dist loadgroup --lakeflow-sweep
"""

from __future__ import annotations

import dataclasses
import json
import os
import secrets
from collections.abc import Iterator

import pytest
from databricks.sdk.errors import NotFound
from databricks.sdk.service import sql

from pytest_databricks_lakeflow import JobRun, Lakeflow, LakeflowError, Stub

from .conftest import lakeflow_for

WAREHOUSE_ID = os.environ.get("LAKEFLOW_WAREHOUSE_ID")
CATALOG = os.environ.get("LAKEFLOW_CATALOG")

pytestmark = [
    pytest.mark.skipif(
        not WAREHOUSE_ID, reason="set LAKEFLOW_WAREHOUSE_ID to the id of a SQL warehouse to run the sql bundle"
    ),
    pytest.mark.xdist_group("sql_run"),
]


@pytest.fixture(scope="module")
def saved_query_id() -> Iterator[str]:
    """Create a saved query with a parameter default for ``saved_query_job``, and move it to the trash afterwards."""
    ws = lakeflow_for("sql").ws
    query = ws.queries.create(
        query=sql.CreateQueryRequestQuery(
            display_name=f"pytest_{secrets.token_hex(4)}",
            warehouse_id=WAREHOUSE_ID,
            query_text="SELECT :region AS region, current_catalog() AS catalog",
            catalog=CATALOG,
            parameters=[sql.QueryParameter(name="region", text_value=sql.TextValue(value="emea"))],
        )
    )
    assert query.id is not None
    try:
        yield query.id
    finally:
        ws.queries.delete(query.id)


@pytest.fixture(scope="module")
def lakeflow(saved_query_id: str) -> Lakeflow:
    assert WAREHOUSE_ID is not None
    variables = {"warehouse_id": WAREHOUSE_ID, "query_id": saved_query_id}
    return dataclasses.replace(lakeflow_for("sql"), variables=variables)


@pytest.fixture(scope="module")
def sql_run(lakeflow: Lakeflow) -> Iterator[JobRun]:
    with lakeflow.job("sql_job") as job:
        yield job.run(only=["query", "types", "wide", "use_results"])


def test_sql_file_task_runs_on_the_warehouse(sql_run: JobRun) -> None:
    assert sql_run.succeeded, sql_run
    task = sql_run.task("query")
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


def test_sql_result_values_are_strings_and_null_is_none(sql_run: JobRun) -> None:
    assert sql_run.succeeded, sql_run
    result = sql_run.task("types").sql_result
    assert result is not None
    assert result.types == ["int", "null", "decimal", "date", "boolean"]
    assert result.first_row == {"n": "1", "m": None, "d": "1.5", "day": "2026-01-01", "b": "true"}


def test_sql_result_is_cut_at_1000_rows(sql_run: JobRun) -> None:
    assert sql_run.succeeded, sql_run
    result = sql_run.task("wide").sql_result
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


def test_output_references_resolve_to_the_query_results(sql_run: JobRun) -> None:
    assert sql_run.succeeded, sql_run
    parameters = sql_run.task("use_results").parameters
    assert isinstance(parameters, dict)
    assert parameters["count"] == "1"
    # Compact JSON with every value a string and NULL as "", unlike SqlResult, which gives None. Keys are not in
    # column order.
    row = json.loads(parameters["row"])
    assert row == {"n": "1", "m": "", "d": "1.5", "day": "2026-01-01", "b": "true"}
    assert parameters["row"] == json.dumps(row, separators=(",", ":"))
    assert json.loads(parameters["rows"]) == [row]


def test_output_references_to_left_out_tasks_are_refused(lakeflow: Lakeflow) -> None:
    # The platform fails such a run with INVALID_RUN_CONFIGURATION "Failed to resolve references: Cannot access output
    # from task 'query' because the task was skipped.", so the plugin refuses it before the run starts.
    with lakeflow.job("sql_job") as job:
        with pytest.raises(LakeflowError, match=r"Task 'use_results' refers to the output of \['query', 'types'\]"):
            job.run(only=["use_results"])
        assert job.runs == []


def test_stubbed_outputs_replace_the_references(lakeflow: Lakeflow) -> None:
    rows = [{"n": "7", "m": "", "d": "2.5", "day": "2026-02-01", "b": "false"}]
    upstream = {
        "query": Stub(outputs={"first_row.n": "7"}),
        "types": Stub(outputs={"first_row": rows[0], "rows": rows}),
    }
    with lakeflow.task("sql_job.use_results", upstream_task_values=upstream) as task:
        run = task.run()
        assert run.succeeded, run
        assert sorted(run.injected_tasks) == ["query", "types"]
        parameters = run.task("use_results").parameters
        assert isinstance(parameters, dict)
        assert parameters["count"] == "7"
        assert parameters["row"] == '{"n":"7","m":"","d":"2.5","day":"2026-02-01","b":"false"}'
        assert json.loads(parameters["rows"]) == rows
        assert run.task("use_results").output_json == {
            "count": "7",
            "row": parameters["row"],
            "rows": parameters["rows"],
        }


def test_inlined_saved_query_returns_its_rows(lakeflow: Lakeflow) -> None:
    with lakeflow.job("saved_query_job", saved_queries="inline") as job:
        run = job.run()
        assert run.succeeded, run
        task = run.task("saved")
        # The task sets no parameter, so the query's default is carried into the SQL file task.
        assert task.parameters == {"region": "emea"}
        result = task.sql_result
        assert result is not None
        assert result.columns == ["region", "catalog"]
        assert result.first_row["region"] == "emea"
        if CATALOG:
            assert result.first_row["catalog"] == CATALOG, "the USE CATALOG line applies the query's catalog"
        sql_task = (job.settings.tasks or [])[0].sql_task
        assert sql_task is not None and sql_task.file is not None
        path = sql_task.file.path
    with pytest.raises(NotFound):
        lakeflow.ws.workspace.get_status(path)
