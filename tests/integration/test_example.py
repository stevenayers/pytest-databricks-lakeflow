r"""Integration tests for the ``example`` bundle (tests/integration/bundles/example), run in a real workspace.

Run them with:

    LAKEFLOW_INTEGRATION=1 DATABRICKS_CONFIG_PROFILE=<profile> LAKEFLOW_CATALOG=<catalog> \
        uv run pytest tests/integration/test_example.py -n 4 --lakeflow-sweep
"""

from __future__ import annotations

import dataclasses
import re
import warnings
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from databricks.sdk.errors import NotFound
from databricks.sdk.service import jobs

from pytest_databricks_lakeflow import Lakeflow, LakeflowError, LakeflowRunFailed

from .conftest import lakeflow_for

if TYPE_CHECKING:
    from databricks.sdk.dbutils import RemoteDbUtils


@pytest.fixture(scope="module")
def lakeflow() -> Lakeflow:
    return lakeflow_for("example")


def test_job_succeeds(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job", params={"param1": "hello", "param2": "world"}) as job:
        run = job.run()
        assert run.succeeded
        assert run.task("transform").output == "hello world"
        assert run.task("transform").notebook is None, "only failed notebook tasks are exported by default"
        assert "Success! hello" in run.task("my_wheel_task").logs
        assert run.parameters["param1"] == "hello"
        transform = run.task("transform").parameters
        assert isinstance(transform, dict)
        assert transform["row_count"] == "7"
        assert transform["param1"] == "hello", "job parameters reach notebook tasks too"
        # A run with several tasks reports its durations on the tasks and usually zero on the run itself.
        assert run.timing.execution >= timedelta(0)
        assert run.task("transform").timing.execution > timedelta(0)
        if run.effective_performance_target is not None:
            assert run.effective_performance_target in {"STANDARD", "PERFORMANCE_OPTIMIZED"}
        else:
            warnings.warn("The workspace returned no effective performance target", stacklevel=1)
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
        assert run.task("transform").attempts[-1].attempt == 0
        assert not run.timed_out
        # transform.py prints two lines in its second cell and raises in its third.
        notebook = run.task("transform").notebook
        assert notebook is not None
        assert "transform: checking forbidden against /Volumes/example/raw/landing" in notebook.stdout
        assert notebook.failed_cell is not None
        assert notebook.failed_cell.index == 2
        assert notebook.failed_cell.error == "PermissionError: forbidden may not read /Volumes/example/raw/landing"
        assert run.task("extract").notebook is None
        with pytest.raises(LakeflowRunFailed) as excinfo:
            run.raise_for_failure()
        assert "forbidden" in str(excinfo.value)


def test_raise_for_failure_names_the_permission_error(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job", params={"param1": "forbidden", "param2": "world"}) as job:
        run = job.run()
        with pytest.raises(LakeflowRunFailed) as excinfo:
            run.raise_for_failure()
        assert excinfo.value.run is run
        assert "PermissionError" in str(excinfo.value)
        assert "transform" in str(excinfo.value)


def test_root_cause_follows_the_exception_chain(lakeflow: Lakeflow) -> None:
    # transform.py raises RuntimeError("wrapper failed") from KeyError("root cause") for param1 == "chained".
    with lakeflow.job("my_example_job", params={"param1": "chained", "param2": "world"}) as job:
        run = job.run()
        assert run.failed
        error = run.task("transform").error
        assert error is not None
        assert error.type == "RuntimeError"
        assert error.message == "wrapper failed"
        assert error.root_cause == "KeyError: 'root cause'"
        with pytest.raises(LakeflowRunFailed) as excinfo:
            run.raise_for_failure()
        assert "root cause: KeyError: 'root cause'" in str(excinfo.value)


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


def test_disabled_task_does_not_run(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job", disable=["load"]) as job:
        run = job.run()
        assert run.succeeded, run
        assert sorted(run.disabled_tasks) == ["load"]
        assert "load" not in run.skipped_tasks
        assert run.disabled_tasks["load"].disabled
        with pytest.raises(KeyError, match="Disabled in the job"):
            run.task("load")


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


def test_start_two_jobs_and_wait(lakeflow: Lakeflow) -> None:
    hello = lakeflow.job("my_example_job", params={"param1": "hello", "param2": "world"})
    hi = lakeflow.job("my_example_job", params={"param1": "hi", "param2": "there"})
    with hello, hi:
        first = hello.start()
        second = hi.start()
        assert first.url is not None and second.url is not None
        first_run = first.wait()
        second_run = second.wait()
        assert first_run.succeeded and second_run.succeeded
        assert first_run.task("transform").output == "hello world"
        assert second_run.task("transform").output == "hi there"
        assert (hello.runs, hi.runs) == ([first_run], [second_run])
        # The second run started before the first ended, so it did not wait for it.
        second_started = lakeflow.ws.jobs.get_run(second_run.run_id).start_time
        first_ended = lakeflow.ws.jobs.get_run(first_run.run_id).end_time
        assert second_started is not None and first_ended is not None
        assert second_started < first_ended


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


def test_replay_transform_from_a_full_run(lakeflow: Lakeflow) -> None:
    first = lakeflow.job("my_example_job", capture_task_values={"extract": ["row_count", "source_path"]}).run()
    assert first.succeeded, first
    assert first.task("extract").task_values == {"row_count": 7, "source_path": "/Volumes/example/raw/landing"}
    with lakeflow.task("my_example_job.transform", upstream_from=first, capture_task_values=["source_path"]) as task:
        run = task.run()
        assert run.succeeded, run
        assert run.task("transform").output == "hello world"
        assert run.task("transform").task_values["source_path"] == "/Volumes/example/raw/landing"
        transform = run.task("transform").parameters
        assert isinstance(transform, dict)
        assert transform["row_count"] == "7", "the stub sets the row count the full run's extract set"


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


def test_test_marker_is_accepted_by_the_api(lakeflow: Lakeflow, request: pytest.FixtureRequest) -> None:
    marked = dataclasses.replace(lakeflow, test_marker=True)
    with marked.job("my_example_job", params={"param1": "hello", "param2": "world"}) as job:
        assert job.job_id is not None
        settings = marked.ws.jobs.get(job.job_id).settings
        parameters = {p.name: p.default for p in (settings.parameters if settings else None) or []}
        assert parameters["__pytest_test_id"] == request.node.nodeid
        run = job.run()
        assert run.succeeded, run
        assert "__pytest_test_id" not in run.parameters
        values = {p.name: p.value or p.default for p in marked.ws.jobs.get_run(run.run_id).job_parameters or []}
        assert values["__pytest_test_id"] == request.node.nodeid


def test_run_as_keep_runs_the_example_job(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job", params={"param1": "hello", "param2": "world"}, run_as="keep") as job:
        assert job.run().succeeded


@pytest.fixture(scope="module")
def lakeflow_on_version_4(lakeflow: Lakeflow) -> Lakeflow:
    return dataclasses.replace(lakeflow, environment_version="4")


def test_environment_version_override(lakeflow_on_version_4: Lakeflow) -> None:
    with lakeflow_on_version_4.job("my_example_job", params={"param1": "hello", "param2": "world"}) as job:
        assert job.run().succeeded
        assert job.job_id is not None
        settings = lakeflow_on_version_4.ws.jobs.get(job.job_id).settings
        environments = (settings.environments if settings else None) or []
        spec = environments[0].spec
        assert spec is not None
        assert spec.environment_version == "4"


def seed_orders(spark, dbutils, params):
    schema = params["schema"]
    spark.sql(f"CREATE OR REPLACE TABLE {schema}.seed_orders AS SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, sku)")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {schema}.landing")
    dbutils.fs.put(f"/Volumes/{schema.replace('.', '/')}/landing/orders.csv", "id,sku\n1,a\n2,b\n", True)
    return {"schema": schema, "param1": params["param1"]}


# Annotated with a name the notebook does not import, to show the cluster does not evaluate annotations.
def remove_landing_file(dbutils: RemoteDbUtils, params: dict[str, str]) -> bool:
    return dbutils.fs.rm(f"/Volumes/{params['schema'].replace('.', '/')}/landing/orders.csv")


def check_compute(spark):
    return spark.sql("SELECT 1 AS ready").collect()[0]["ready"]


def report_params(params):
    return params


def broken_setup():
    raise ValueError("no seed data")


def test_setup_seeds_a_table_and_teardown_removes_a_file(lakeflow: Lakeflow) -> None:
    with lakeflow.schema() as schema:
        with lakeflow.job(
            "my_example_job",
            params={"schema": schema, "mode": "incremental"},
            setup=seed_orders,
            teardown=remove_landing_file,
        ) as job:
            run = job.run(params={"param1": "seeded"})
            assert run.succeeded, run
            assert run.setup is not None and run.setup.succeeded
            assert run.setup.output_json == {"schema": schema, "param1": "seeded"}
            assert run.task("load").output == "loaded 2 seeded orders"
            assert run.teardown is not None and run.teardown.succeeded
            assert run.teardown.output_json is True
        landing_file = f"/Volumes/{schema.replace('.', '/')}/landing/orders.csv"
        with pytest.raises(NotFound):
            lakeflow.ws.files.get_metadata(landing_file)


def test_teardown_runs_after_a_failed_task(lakeflow: Lakeflow) -> None:
    with lakeflow.job(
        "my_example_job", params={"param1": "forbidden"}, setup=check_compute, teardown=report_params
    ) as job:
        run = job.run()
        assert run.failed
        assert run.task("transform").raised(PermissionError)
        assert run.setup is not None and run.setup.output_json == 1
        assert run.teardown is not None and run.teardown.succeeded, run.teardown
        assert run.teardown.output_json == {"param1": "forbidden", "param2": "world", "schema": "", "mode": "full"}


def test_only_still_runs_setup_and_teardown(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job", setup=check_compute, teardown=report_params) as job:
        # load depends on tasks that only leaves out; the platform still holds it until the setup has finished.
        run = job.run(only=["load"])
        assert run.succeeded, run
        assert sorted(run.injected_tasks) == ["__pytest_setup", "__pytest_teardown"]
        assert run.setup is not None and run.setup.succeeded
        assert run.teardown is not None and run.teardown.succeeded, run.teardown
        assert sorted(run.skipped_tasks) == ["extract", "my_wheel_task", "transform"]
        timings = {t.task_key: t for t in lakeflow.ws.jobs.get_run(run.run_id).tasks or []}
        assert (timings["load"].start_time or 0) >= (timings["__pytest_setup"].end_time or 0)


def test_failed_setup_is_named_first(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job", setup=broken_setup, teardown=report_params) as job:
        run = job.run()
        assert run.failed
        assert "failing=['setup: ValueError: no seed data'" in repr(run), run
        assert run.task("extract").upstream_failed
        assert run.teardown is not None and run.teardown.succeeded, run.teardown


def test_task_setup_runs_before_the_stubs(lakeflow: Lakeflow) -> None:
    with lakeflow.task(
        "my_example_job.transform",
        upstream_task_values={"extract": {"row_count": 1, "source_path": "/Volumes/x/y/z"}},
        setup=check_compute,
        teardown=report_params,
    ) as task:
        run = task.run()
        assert run.succeeded, run
        setup, stub = run.injected_tasks["__pytest_setup"], run.injected_tasks["extract"]
        assert setup.succeeded and stub.succeeded
        assert run.teardown is not None and run.teardown.succeeded, run.teardown


def test_extra_dependencies_are_importable(lakeflow: Lakeflow) -> None:
    with lakeflow.task(
        "my_example_job.my_wheel_task",
        params={"param1": "version inflection"},
        extra_dependencies=["inflection==0.5.1"],
    ) as task:
        run = task.run()
        wheel_task = run.task("my_wheel_task")
        assert run.succeeded, wheel_task.error
        assert "inflection 0.5.1" in wheel_task.logs


def test_strict_artifacts_refuses_the_unversioned_example_wheel(lakeflow: Lakeflow) -> None:
    # The example's wheel artifact has no dynamic_version, so a strict Lakeflow refuses its templates.
    strict = dataclasses.replace(lakeflow, strict_artifacts=True)
    with pytest.raises(LakeflowError, match=r"^Artifacts \['lakeflow_example'\] have no dynamic_version: true\."):
        strict.template("my_example_job")
