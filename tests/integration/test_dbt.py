r"""Integration tests for the ``dbt`` bundle (tests/integration/bundles/dbt), run in a real workspace.

The bundle's ``dbt_job`` has one serverless dbt task, ``dbt_build``, that runs the dbt project under ``dbt/`` on the
warehouse named by the ``warehouse_id`` bundle variable, in the catalog named by ``catalog``. These tests read them
from ``LAKEFLOW_WAREHOUSE_ID`` and ``LAKEFLOW_CATALOG``. The project's ``generate_schema_name`` macro puts every model
in the schema of the ``target_schema`` dbt variable, so each run builds into a ``lakeflow.schema()`` that is dropped
afterwards. Run them with:

    LAKEFLOW_INTEGRATION=1 DATABRICKS_CONFIG_PROFILE=<profile> LAKEFLOW_CATALOG=<catalog> \
        LAKEFLOW_WAREHOUSE_ID=<warehouse id> \
        uv run pytest tests/integration/test_dbt.py -n 8 --dist loadgroup --lakeflow-sweep
"""

from __future__ import annotations

import dataclasses
import os
from collections.abc import Iterator

import pytest

from pytest_databricks_lakeflow import JobRun, Lakeflow

from .conftest import lakeflow_for

CATALOG = os.environ.get("LAKEFLOW_CATALOG")
WAREHOUSE_ID = os.environ.get("LAKEFLOW_WAREHOUSE_ID")

pytestmark = pytest.mark.skipif(
    not (WAREHOUSE_ID and CATALOG),
    reason="set LAKEFLOW_WAREHOUSE_ID and LAKEFLOW_CATALOG to run the dbt bundle on a SQL warehouse",
)


@pytest.fixture(scope="module")
def lakeflow() -> Lakeflow:
    assert WAREHOUSE_ID is not None and CATALOG is not None
    variables = {"warehouse_id": WAREHOUSE_ID, "catalog": CATALOG}
    return dataclasses.replace(lakeflow_for("dbt"), variables=variables, dbt_artifacts=True)


@pytest.fixture(scope="module")
def schema(lakeflow: Lakeflow) -> Iterator[str]:
    with lakeflow.schema() as name:
        yield name


@pytest.fixture(scope="module")
def commands(schema: str) -> list[str]:
    target_schema = schema.split(".")[1]
    return [f'dbt build --select tag:ci --vars "{{target_schema: {target_schema}}}"']


@pytest.fixture(scope="module")
def dbt_run(lakeflow: Lakeflow, commands: list[str]) -> Iterator[JobRun]:
    with lakeflow.job("dbt_job") as job:
        yield job.run(dbt_commands=commands)


@pytest.mark.xdist_group("dbt_run")
def test_dbt_task(dbt_run: JobRun) -> None:
    assert dbt_run.succeeded, dbt_run
    dbt = dbt_run.task("dbt_build").dbt
    assert dbt is not None
    assert dbt.all_passed, dbt
    assert dbt.failed_models == []
    assert dbt.results["model.lakeflow_dbt.orders"].status == "success"
    tests = {uid: result.status for uid, result in dbt.results.items() if uid.startswith("test.")}
    assert tests and set(tests.values()) == {"pass"}, tests
    assert dbt.manifest is not None and "model.lakeflow_dbt.orders" in dbt.manifest["nodes"]
    assert (dbt.artifacts_dir / "target" / "run_results.json").is_file()


@pytest.mark.xdist_group("dbt_run")
def test_dbt_commands_replace_the_commands_of_the_run(
    lakeflow: Lakeflow, dbt_run: JobRun, schema: str, commands: list[str]
) -> None:
    task = dbt_run.task("dbt_build")
    assert task.parameters == commands
    dbt = task.dbt
    assert dbt is not None
    # The deployed commands build every model; --select tag:ci leaves out the untagged one.
    assert "model.lakeflow_dbt.order_counts" not in dbt.results
    assert lakeflow.ws.tables.exists(f"{schema}.orders").table_exists
