r"""The acceptance tests from the spec, run against examples/bundle in a real workspace.

Run them with:

    LAKEFLOW_INTEGRATION=1 DATABRICKS_CONFIG_PROFILE=<profile> LAKEFLOW_CATALOG=<catalog> \
        uv run pytest tests/integration -n 4
"""

from __future__ import annotations

import os

import pytest

from pytest_databricks_lakeflow import Lakeflow

pytestmark = pytest.mark.skipif(
    os.environ.get("LAKEFLOW_INTEGRATION") != "1", reason="set LAKEFLOW_INTEGRATION=1 to run against a workspace"
)


def test_job_succeeds(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job", params={"param1": "hello", "param2": "world"}) as job:
        run = job.run()
        assert run.succeeded
        assert run.task("transform").output == "hello world"
        assert "Success! hello" in run.task("my_wheel_task").logs


def test_job_surfaces_permission_error(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job", params={"param1": "forbidden", "param2": "world"}) as job:
        run = job.run()
        assert run.failed
        assert run.termination_code == "RUN_EXECUTION_ERROR"
        assert run.task("transform").raised(PermissionError)
        assert run.task("load").result_state == "UPSTREAM_FAILED"
        assert run.task("transform").attempt == 0


def test_subset_of_job(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job") as job:
        run = job.run(only=["+transform"])
        assert run.succeeded
        assert "load" not in run.tasks
        assert sorted(run.skipped_tasks) == ["load", "my_wheel_task"]


@pytest.mark.parametrize(
    ("greeting", "expected"),
    [("hello", "hello world"), ("goodbye", "goodbye world")],
)
def test_single_task(lakeflow: Lakeflow, greeting: str, expected: str) -> None:
    with lakeflow.schema() as schema:
        with lakeflow.task(
            "my_example_job.transform",
            params={"param1": greeting, "param2": "world", "schema": schema, "mode": "incremental"},
            upstream_values={"extract": {"row_count": 42, "source_path": "/Volumes/x/y/z"}},
            capture_values=["rows_written"],
        ) as task:
            run = task.run()
            assert run.succeeded
            assert run.task("transform").output == expected
            assert run.task("transform").values["rows_written"] == 42
        assert lakeflow.ws.tables.exists(f"{schema}.example_rows").table_exists


def test_run_twice_in_one_job(lakeflow: Lakeflow) -> None:
    with lakeflow.schema() as schema, lakeflow.job("my_example_job", params={"schema": schema}) as job:
        first = job.run()
        second = job.run(params={"mode": "incremental"})
        assert first.succeeded and second.succeeded
        assert second.duration < first.duration


def test_values_captured_even_on_failure(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job", params={"param1": "explode"}, capture_values={"transform": ["stage"]}) as job:
        run = job.run()
        assert run.failed
        assert run.task("transform").values["stage"] == "validation"


def test_missing_values_explain_themselves(lakeflow: Lakeflow) -> None:
    job = lakeflow.job("my_example_job", capture_values={"transform": ["never_set"], "load": ["anything"]})
    run = job.run(params={"param1": "explode"})
    assert run.task("transform").missing_values == ["never_set"]
    with pytest.raises(KeyError, match="Task 'transform' never set value 'never_set'"):
        run.task("transform").values["never_set"]
    with pytest.raises(KeyError, match=r"Task 'load' did not run \(UPSTREAM_FAILED\)"):
        run.task("load").values["anything"]


def test_stub_values_are_not_templated(lakeflow: Lakeflow) -> None:
    source_path = "/Volumes/{{job.id}}/{{tasks.other.values.x}}"
    with lakeflow.task(
        "my_example_job.transform",
        upstream_values={"extract": {"row_count": 1, "source_path": source_path}},
        capture_values=["source_path"],
    ) as task:
        run = task.run()
        assert run.succeeded, run
        assert run.task("transform").values["source_path"] == source_path
