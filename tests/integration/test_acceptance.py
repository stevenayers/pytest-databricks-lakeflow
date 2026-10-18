r"""The acceptance tests from the spec, run against examples/bundle in a real workspace.

Run them with:

    LAKEFLOW_INTEGRATION=1 DATABRICKS_CONFIG_PROFILE=<profile> LAKEFLOW_CATALOG=<catalog> \
        uv run pytest tests/integration -n 4
"""

from __future__ import annotations

import dataclasses
import os
import re
import warnings
from pathlib import Path

import pytest
from databricks.sdk.service import jobs

from pytest_databricks_lakeflow import Lakeflow, LakeflowRunFailed

pytestmark = pytest.mark.skipif(
    os.environ.get("LAKEFLOW_INTEGRATION") != "1", reason="set LAKEFLOW_INTEGRATION=1 to run against a workspace"
)


def test_job_succeeds(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job", params={"param1": "hello", "param2": "world"}) as job:
        run = job.run()
        assert run.succeeded
        assert run.task("transform").output == "hello world"
        assert "Success! hello" in run.task("my_wheel_task").logs
        assert run.parameters["param1"] == "hello"
        # Some workspaces return no resolved values even with include_resolved_values.
        transform = run.task("transform").parameters
        if transform is not None:
            assert isinstance(transform, dict)
            assert transform["row_count"] == "7"
        else:
            warnings.warn("The workspace returned no resolved values for transform", stacklevel=1)
        assert job.job_id is not None
        settings = lakeflow.ws.jobs.get(job.job_id).settings
        tags = (settings.tags if settings else None) or {}
        assert tags["pytest_user"]
        if (Path(__file__).parents[2] / ".git").exists():
            assert re.fullmatch(r"[0-9a-f]{40}", tags["pytest_git_sha"])


def test_job_surfaces_permission_error(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job", params={"param1": "forbidden", "param2": "world"}) as job:
        run = job.run()
        assert run.failed
        assert run.termination_code == "RUN_EXECUTION_ERROR"
        assert run.task("transform").raised(PermissionError)
        assert run.task("load").result_state == "UPSTREAM_FAILED"
        assert run.task("transform").attempt == 0


def test_raise_for_failure_names_the_permission_error(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job", params={"param1": "forbidden", "param2": "world"}) as job:
        run = job.run()
        with pytest.raises(LakeflowRunFailed) as excinfo:
            run.raise_for_failure()
        assert excinfo.value.run is run
        assert "PermissionError" in str(excinfo.value)
        assert "transform" in str(excinfo.value)


def test_output_json_refuses_a_plain_exit_value(lakeflow: Lakeflow) -> None:
    # extract.py exits with the plain string "extracted 7 rows", so there is no JSON output to parse.
    with lakeflow.job("my_example_job", params={"param1": "hello", "param2": "world"}) as job:
        run = job.run().raise_for_failure()
        with pytest.raises(ValueError, match="Output of task 'extract' is not JSON"):
            _ = run.task("extract").output_json


def test_subset_of_job(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job") as job:
        run = job.run(only=["+transform"])
        assert run.succeeded
        assert "load" not in run.tasks
        assert sorted(run.skipped_tasks) == ["load", "my_wheel_task"]


def test_run_with_performance_target_override(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job") as job:
        run = job.run(performance_target="PERFORMANCE_OPTIMIZED")
        assert run.succeeded


@pytest.mark.parametrize(
    ("greeting", "expected"),
    [("hello", "hello world"), ("goodbye", "goodbye world")],
)
def test_single_task(lakeflow: Lakeflow, greeting: str, expected: str) -> None:
    with lakeflow.schema() as schema:
        with lakeflow.task(
            "my_example_job.transform",
            params={"param1": greeting, "param2": "world", "schema": schema, "mode": "incremental"},
            upstream_task_values={"extract": {"row_count": 42, "source_path": "/Volumes/x/y/z"}},
            capture_task_values=["rows_written"],
        ) as task:
            run = task.run()
            assert run.succeeded
            assert run.task("transform").output == expected
            assert run.task("transform").task_values["rows_written"] == 42
        assert lakeflow.ws.tables.exists(f"{schema}.example_rows").table_exists


def test_run_twice_in_one_job(lakeflow: Lakeflow) -> None:
    with lakeflow.schema() as schema, lakeflow.job("my_example_job", params={"schema": schema}) as job:
        first = job.run()
        second = job.run(params={"mode": "incremental"})
        assert first.succeeded and second.succeeded
        assert second.duration < first.duration


def test_values_captured_even_on_failure(lakeflow: Lakeflow) -> None:
    with lakeflow.job(
        "my_example_job", params={"param1": "explode"}, capture_task_values={"transform": ["stage"]}
    ) as job:
        run = job.run()
        assert run.failed
        assert run.task("transform").task_values["stage"] == "validation"


def test_missing_task_values_explain_themselves(lakeflow: Lakeflow) -> None:
    job = lakeflow.job("my_example_job", capture_task_values={"transform": ["never_set"], "load": ["anything"]})
    run = job.run(params={"param1": "explode"})
    assert run.task("transform").missing_task_values == ["never_set"]
    with pytest.raises(KeyError, match="Task 'transform' never set value 'never_set'"):
        run.task("transform").task_values["never_set"]
    with pytest.raises(KeyError, match=r"Task 'load' did not run \(UPSTREAM_FAILED\)"):
        run.task("load").task_values["anything"]


def test_stub_values_are_not_templated(lakeflow: Lakeflow) -> None:
    source_path = "/Volumes/{{job.id}}/{{tasks.other.values.x}}"
    with lakeflow.task(
        "my_example_job.transform",
        upstream_task_values={"extract": {"row_count": 1, "source_path": source_path}},
        capture_task_values=["source_path"],
    ) as task:
        run = task.run()
        assert run.succeeded, run
        assert run.task("transform").task_values["source_path"] == source_path


def test_permissions_are_applied(lakeflow: Lakeflow) -> None:
    # The creator already owns the job, so grant to a group rather than to the current user.
    shared = dataclasses.replace(lakeflow, permissions=[{"group_name": "users", "permission_level": "CAN_MANAGE_RUN"}])
    with shared.job("my_example_job") as job:
        assert job.job_id is not None
        acl = shared.ws.jobs.get_permissions(str(job.job_id)).access_control_list or []
        levels = [
            permission.permission_level
            for entry in acl
            if entry.group_name == "users"
            for permission in entry.all_permissions or []
        ]
        assert jobs.JobPermissionLevel.CAN_MANAGE_RUN in levels


def test_run_as_keep_runs_the_example_job(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job", params={"param1": "hello", "param2": "world"}, run_as="keep") as job:
        assert job.run().succeeded
