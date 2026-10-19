r"""Integration tests for the ``example`` bundle (tests/integration/bundles/example), run in a real workspace.

Tests that only read the same run share it through a module-scoped fixture, and are marked with an ``xdist_group``
named after that fixture, so that ``--dist loadgroup`` sends them to one worker and the run happens once. Run them with:

    LAKEFLOW_INTEGRATION=1 DATABRICKS_CONFIG_PROFILE=<profile> LAKEFLOW_CATALOG=<catalog> \
        uv run pytest tests/integration/test_example.py -n 4 --dist loadgroup --lakeflow-sweep
"""

from __future__ import annotations

import dataclasses
import json
import os
import posixpath
import re
import time
import warnings
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from databricks.sdk.errors import NotFound
from databricks.sdk.service import jobs
from databricks.sdk.service.workspace import ExportFormat, ObjectType

from pytest_databricks_lakeflow import JobRun, Lakeflow, LakeflowError, LakeflowRunFailed, session
from pytest_databricks_lakeflow.results import serialise

from .conftest import BUNDLES, lakeflow_for

if TYPE_CHECKING:
    from databricks.sdk.dbutils import RemoteDbUtils


@pytest.fixture(scope="module")
def lakeflow() -> Lakeflow:
    return lakeflow_for("example")


@pytest.fixture(scope="module")
def hello_run(lakeflow: Lakeflow) -> Iterator[JobRun]:
    with lakeflow.job("my_example_job", params={"param1": "hello", "param2": "world"}) as job:
        yield job.run()


@pytest.mark.xdist_group("hello_run")
def test_job_succeeds(lakeflow: Lakeflow, hello_run: JobRun) -> None:
    assert hello_run.succeeded
    assert hello_run.task("transform").output == "hello world"
    assert hello_run.task("transform").notebook is None, "only failed notebook tasks are exported by default"
    assert "Success! hello" in hello_run.task("my_wheel_task").logs
    assert hello_run.parameters["param1"] == "hello"
    transform = hello_run.task("transform").parameters
    assert isinstance(transform, dict)
    assert transform["row_count"] == "7"
    assert transform["param1"] == "hello", "job parameters reach notebook tasks too"
    # A run with several tasks reports its durations on the tasks and usually zero on the run itself.
    assert hello_run.timing.execution >= timedelta(0)
    assert hello_run.task("transform").timing.execution > timedelta(0)
    if hello_run.effective_performance_target is not None:
        assert hello_run.effective_performance_target in {"STANDARD", "PERFORMANCE_OPTIMIZED"}
    else:
        warnings.warn("The workspace returned no effective performance target", stacklevel=1)
    # The fixture's job is deleted after the module, so it still exists here.
    settings = lakeflow.ws.jobs.get(hello_run.job_id).settings
    tags = (settings.tags if settings else None) or {}
    assert tags["pytest_user"]
    if (Path(__file__).parents[2] / ".git").exists():
        assert re.fullmatch(r"[0-9a-f]{40}", tags["pytest_git_sha"])


# The run's report.json goes to the first test that uses it, test_job_surfaces_permission_error, where the CI report
# check looks for the exported transform notebook.
@pytest.fixture(scope="module")
def forbidden_run(lakeflow: Lakeflow) -> Iterator[JobRun]:
    with lakeflow.job("my_example_job", params={"param1": "forbidden", "param2": "world"}) as job:
        yield job.run()


@pytest.mark.xdist_group("forbidden_run")
def test_job_surfaces_permission_error(forbidden_run: JobRun) -> None:
    assert forbidden_run.failed
    assert forbidden_run.termination_code == "RUN_EXECUTION_ERROR"
    assert forbidden_run.task("transform").raised(PermissionError)
    assert forbidden_run.task("load").result_state == "UPSTREAM_FAILED"
    assert forbidden_run.task("transform").attempt == 0
    assert forbidden_run.task("transform").attempts[-1].attempt == 0
    assert not forbidden_run.timed_out
    # transform.py prints two lines in its second cell and raises in its third.
    notebook = forbidden_run.task("transform").notebook
    assert notebook is not None
    assert "transform: checking forbidden against /Volumes/example/raw/landing" in notebook.stdout
    assert notebook.failed_cell is not None
    assert notebook.failed_cell.index == 2
    assert notebook.failed_cell.error == "PermissionError: forbidden may not read /Volumes/example/raw/landing"
    assert forbidden_run.task("extract").notebook is None
    # --lakeflow-report-dir writes this as report.json, with the notebook saved as <run_id>/transform.html.
    data = json.loads(json.dumps(serialise(forbidden_run)))
    transform = data["tasks"]["transform"]
    assert transform["notebook_file"] == f"{forbidden_run.run_id}/transform.html"
    assert transform["error"]["type"] == "PermissionError"
    assert data["tasks"]["load"]["result_state"] == "UPSTREAM_FAILED"
    with pytest.raises(LakeflowRunFailed) as excinfo:
        forbidden_run.raise_for_failure()
    assert "forbidden" in str(excinfo.value)


def test_task_overrides_retry_a_failing_task(lakeflow: Lakeflow) -> None:
    overrides = {"transform": {"max_retries": 1, "min_retry_interval_millis": 0}}
    params = {"param1": "forbidden", "param2": "world"}
    with lakeflow.job("my_example_job", params=params, task_overrides=overrides) as job:
        run = job.run()
        assert run.failed
        transform = run.task("transform")
        assert [a.attempt for a in transform.attempts] == [0, 1]
        assert all(a.raised(PermissionError) for a in transform.attempts)
        assert run.task("load").result_state == "UPSTREAM_FAILED"


@pytest.mark.xdist_group("forbidden_run")
def test_raise_for_failure_names_the_permission_error(forbidden_run: JobRun) -> None:
    with pytest.raises(LakeflowRunFailed) as excinfo:
        forbidden_run.raise_for_failure()
    assert excinfo.value.run is forbidden_run
    assert "PermissionError" in str(excinfo.value)
    assert "transform" in str(excinfo.value)


@pytest.mark.xdist_group("forbidden_run")
def test_debug_notebook_is_uploaded(lakeflow: Lakeflow, forbidden_run: JobRun) -> None:
    # A job that is never run is never created, so this replays the shared run without a run of its own.
    job = lakeflow.job("my_example_job")
    notebook = job.debug_notebook("transform", forbidden_run)
    try:
        status = lakeflow.ws.workspace.get_status(notebook.path)
        assert status.object_type == ObjectType.NOTEBOOK
        assert notebook.url == f"{lakeflow.ws.config.host}/#notebook/{status.object_id}"
        assert notebook.path.endswith(
            f"/.pytest-databricks-lakeflow/debug/my_example_job__transform__{forbidden_run.run_id}"
        )
        source = lakeflow.ws.workspace.download(notebook.path, format=ExportFormat.SOURCE).read().decode()
        assert 'dbutils.widgets.text("param1", "forbidden")' in source
        assert 'dbutils.widgets.text("row_count", "7")' in source
        assert "# MAGIC %run /Workspace/" in source
        assert source.rstrip().endswith("/src/notebooks/transform")
    finally:
        lakeflow.ws.workspace.delete(notebook.path)


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


@pytest.mark.xdist_group("hello_run")
def test_output_json_refuses_a_plain_exit_value(hello_run: JobRun) -> None:
    # extract.py exits with the plain string "extracted 7 rows", so there is no JSON output to parse.
    run = hello_run.raise_for_failure()
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


@pytest.fixture(scope="module")
def explode_run(lakeflow: Lakeflow) -> Iterator[JobRun]:
    # transform sets stage before it raises, never sets never_set, and load does not run.
    capture = {"transform": ["stage", "never_set"], "load": ["anything"]}
    with lakeflow.job("my_example_job", params={"param1": "explode"}, capture_task_values=capture) as job:
        yield job.run()


@pytest.mark.xdist_group("explode_run")
def test_values_captured_even_on_failure(explode_run: JobRun) -> None:
    assert explode_run.failed
    assert explode_run.task("transform").task_values["stage"] == "validation"


@pytest.mark.xdist_group("explode_run")
def test_missing_task_values_explain_themselves(explode_run: JobRun) -> None:
    assert explode_run.task("transform").missing_task_values == ["never_set"]
    with pytest.raises(KeyError, match="Task 'transform' never set value 'never_set'"):
        explode_run.task("transform").task_values["never_set"]
    with pytest.raises(KeyError, match=r"Task 'load' did not run \(UPSTREAM_FAILED\)"):
        explode_run.task("load").task_values["anything"]


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


@pytest.fixture(scope="module")
def captured_run(lakeflow: Lakeflow) -> Iterator[JobRun]:
    with lakeflow.job("my_example_job", capture_task_values={"extract": ["row_count", "source_path"]}) as job:
        yield job.run()


@pytest.mark.xdist_group("captured_run")
def test_replay_transform_from_a_full_run(lakeflow: Lakeflow, captured_run: JobRun) -> None:
    assert captured_run.succeeded, captured_run
    extract = captured_run.task("extract")
    assert extract.task_values == {"row_count": 7, "source_path": "/Volumes/example/raw/landing"}
    with lakeflow.task(
        "my_example_job.transform", upstream_from=captured_run, capture_task_values=["source_path"]
    ) as task:
        run = task.run()
        assert run.succeeded, run
        assert run.task("transform").output == "hello world"
        assert run.task("transform").task_values["source_path"] == "/Volumes/example/raw/landing"
        transform = run.task("transform").parameters
        assert isinstance(transform, dict)
        assert transform["row_count"] == "7", "the stub sets the row count the full run's extract set"


def test_downstream_half_with_stubbed_extract(lakeflow: Lakeflow) -> None:
    stubbed = {"extract": {"row_count": 3, "source_path": "/Volumes/x/y/z"}}
    with lakeflow.job("my_example_job", upstream_task_values=stubbed) as job:
        run = job.run(only=["transform+"])
        assert run.succeeded, run
        assert "extract" in run.injected_tasks
        assert sorted(run.tasks) == ["load", "transform"]
        transform = run.task("transform").parameters
        assert isinstance(transform, dict)
        assert transform["row_count"] == "3"
        # Without the stub, only=["transform"] ends INTERNAL_ERROR: "Failed to resolve references".
        run = job.run(only=["transform"])
        assert run.succeeded, run
        assert sorted(run.tasks) == ["transform"]


@pytest.mark.xdist_group("captured_run")
def test_replay_downstream_half_from_a_full_run(lakeflow: Lakeflow, captured_run: JobRun) -> None:
    assert captured_run.succeeded, captured_run
    run = lakeflow.job("my_example_job").run(only=["transform+"], replay=captured_run)
    assert run.succeeded, run
    assert sorted(run.injected_tasks) == ["extract"]
    transform = run.task("transform").parameters
    assert isinstance(transform, dict)
    assert transform["row_count"] == "7", "the stub sets the row count the full run's extract set"
    with pytest.raises(LakeflowError, match=r"refers to task values of \['extract'\]"):
        lakeflow.job("my_example_job").run(only=["transform"])


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


def test_run_as_self_passes_the_pre_deploy_check(lakeflow: Lakeflow) -> None:
    # The test target sets run_as to ${workspace.current_user.userName}, which setup checks before `bundle deploy`.
    me = lakeflow.ws.current_user.me().user_name
    assert lakeflow.template("my_example_job").run_as == jobs.JobRunAs(user_name=me)
    with lakeflow.job("my_example_job", run_as=me) as job:
        assert job.job_id is not None
        created = lakeflow.ws.jobs.get(job.job_id)
        assert created.run_as_user_name == me
        assert created.settings is not None
        assert created.settings.run_as == jobs.JobRunAs(user_name=me)


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


def test_per_job_environment_version(lakeflow: Lakeflow) -> None:
    # A key the job does not have adds an environment that no task uses; the Jobs API accepts it and the run succeeds.
    overrides = {
        "default": {"environment_version": "5"},
        "spare": {"environment_version": "4", "dependencies": ["inflection==0.5.1"]},
    }
    with lakeflow.job("my_example_job", environments=overrides) as job:
        run = job.run()
        assert run.succeeded, run
        assert job.job_id is not None
        settings = lakeflow.ws.jobs.get(job.job_id).settings
        environments = (settings.environments if settings else None) or []
        specs = {env.environment_key: env.spec for env in environments}
        assert list(specs) == ["default", "spare"]
        default = specs["default"]
        assert default is not None
        assert default.environment_version == "5"
        assert default.dependencies and default.dependencies[0].endswith(".whl"), "the bundle's wheel is kept"
        spare = specs["spare"]
        assert spare is not None
        assert spare.environment_version == "4"
        assert spare.dependencies == ["inflection==0.5.1"], "a new environment holds only the fields passed"


def test_task_params_override_one_task(lakeflow: Lakeflow) -> None:
    task_params = {"my_wheel_task": ["from-task-params"], "extract": {"limit": "10", "param2": "from-task-params"}}
    with pytest.warns(UserWarning, match="sets 'param2', also a job parameter of 'my_example_job'. The job value wins"):
        job = lakeflow.job("my_example_job", task_params=task_params)
    with job:
        run = job.run(only=["extract", "my_wheel_task"])
        assert run.succeeded, run
        assert run.task("my_wheel_task").parameters == ["from-task-params"]
        assert "Success! from-task-params" in run.task("my_wheel_task").logs
        extract = run.task("extract").parameters
        assert isinstance(extract, dict)
        assert extract["limit"] == "10"
        assert extract["param2"] == "world", "the job parameter wins over the task parameter of the same name"


def test_task_params_set_named_wheel_parameters(lakeflow: Lakeflow) -> None:
    task_params = {"my_wheel_task": {"greeting": "from-task-params", "param1": "from-task-params"}}
    with pytest.warns(UserWarning, match="sets 'param1', also a job parameter of 'my_example_job'. The job value wins"):
        job = lakeflow.job("my_example_job", task_params=task_params)
    with job:
        run = job.run(only=["my_wheel_task"])
        assert run.succeeded, run
        wheel = run.task("my_wheel_task")
        # The platform passes every job parameter to a wheel task with named parameters, and the job value wins.
        received = {"greeting": "from-task-params", "param1": "hello", "param2": "world", "schema": "", "mode": "full"}
        assert wheel.parameters == received
        assert "Named: --greeting=from-task-params --mode=full --param1=hello --param2=world --schema=" in wheel.logs


def test_only_resolves_the_result_state_of_a_task_left_out(lakeflow: Lakeflow) -> None:
    def refer_to_extract(settings: jobs.JobSettings, item: pytest.Item) -> None:
        for task in settings.tasks or []:
            if task.task_key == "load" and task.notebook_task is not None:
                task.notebook_task.base_parameters = {"upstream": "{{tasks.extract.result_state}}"}

    with dataclasses.replace(lakeflow, modify_settings=refer_to_extract).job("my_example_job") as job:
        run = job.run(only=["load"])
        assert run.succeeded, run
        load = run.task("load").parameters
        assert isinstance(load, dict)
        assert load["upstream"] == "disabled", "only checks task values; the platform resolves other references"


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


def declare_run_date(settings: jobs.JobSettings, item: pytest.Item) -> None:
    run_date = jobs.JobParameterDefinition(name="run_date", default="2026-01-01")
    settings.parameters = [*(settings.parameters or []), run_date]


def test_sql_setup_and_teardown(lakeflow: Lakeflow) -> None:
    with lakeflow.schema() as schema:
        with lakeflow.job(
            "my_example_job",
            params={"schema": schema},
            setup=[
                "CREATE OR REPLACE TABLE ${schema}.seed AS SELECT 1 AS id",
                "CREATE OR REPLACE TABLE ${schema}.scratch AS SELECT 2 AS id",
            ],
            teardown=["DROP TABLE ${schema}.scratch"],
        ) as job:
            run = job.run(only=["extract"])
            assert run.succeeded, run
            assert run.setup is not None and run.setup.output_json == {"statements": 2}, run.setup
            assert run.teardown is not None and run.teardown.output_json == {"statements": 1}, run.teardown
        assert lakeflow.ws.tables.exists(f"{schema}.seed").table_exists
        assert not lakeflow.ws.tables.exists(f"{schema}.scratch").table_exists


def test_sql_setup_reads_a_parameter_declared_by_modify_settings(lakeflow: Lakeflow) -> None:
    declaring = dataclasses.replace(lakeflow, modify_settings=declare_run_date)
    with (
        lakeflow.schema() as schema,
        declaring.job(
            "my_example_job",
            params={"schema": schema, "run_date": "2026-10-19"},
            setup=["CREATE OR REPLACE TABLE ${schema}.seed AS SELECT DATE'${run_date}' AS day, '$$1' AS price"],
            teardown=Path("tests/integration/notebooks/read_seed.py"),
        ) as job,
    ):
        run = job.run(only=["extract"])
        assert run.succeeded, run
        assert run.setup is not None and run.setup.succeeded, run.setup
        assert run.teardown is not None and run.teardown.succeeded, run.teardown
        assert run.teardown.output_json == [{"day": "2026-10-19", "price": "$1"}]


def test_undeclared_run_params_reach_the_setup_function(lakeflow: Lakeflow) -> None:
    loose = dataclasses.replace(lakeflow, allow_undeclared_params=True)
    with loose.job("my_example_job", setup=report_params) as job:
        run = job.run(params={"run_date": "2026-10-19"}, only=["extract"])
        assert run.succeeded, run
        assert run.setup is not None and run.setup.succeeded, run.setup
        params = run.setup.output_json
        assert isinstance(params, dict) and params["run_date"] == "2026-10-19", params


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


def test_local_wheel_extras_are_installed(lakeflow: Lakeflow) -> None:
    # The example wheel's `test` extra pulls in inflection, which serverless environment 4 does not ship.
    with lakeflow.task(
        "my_example_job.my_wheel_task",
        params={"param1": "version inflection"},
        extra_dependencies=["./dist/*.whl[test]"],
    ) as task:
        specs = {env.environment_key: env.spec for env in task.settings.environments or []}
        default = specs["default"]
        assert default is not None and default.dependencies
        uploaded = default.dependencies[-1]
        assert uploaded.endswith("/.internal/lakeflow_example-0.1.0-py3-none-any.whl[test]"), uploaded
        run = task.run()
        wheel_task = run.task("my_wheel_task")
        assert run.succeeded, wheel_task.error
        assert "inflection 0.5.1" in wheel_task.logs


def test_none_warns_about_a_wheel_built_after_its_upload(lakeflow: Lakeflow) -> None:
    lakeflow.template("my_example_job")  # The session's deploy has built and uploaded the wheel.
    [wheel] = (BUNDLES / "example" / "dist").glob("*.whl")
    built = wheel.stat()
    later = time.time() + 600
    os.utime(wheel, (later, later))
    try:
        stale = r"dist/lakeflow_example-0\.1\.0-py3-none-any\.whl was built after its uploaded copy\. Run `databricks"
        with pytest.warns(UserWarning, match=stale):
            dataclasses.replace(lakeflow, sync="none").template("my_example_job")
    finally:
        os.utime(wheel, (built.st_atime, built.st_mtime))


def test_strict_artifacts_refuses_the_unversioned_example_wheel(lakeflow: Lakeflow) -> None:
    # The example's wheel artifact has no dynamic_version, so a strict Lakeflow refuses its templates.
    strict = dataclasses.replace(lakeflow, strict_artifacts=True)
    with pytest.raises(LakeflowError, match=r"^Artifacts \['lakeflow_example'\] have no dynamic_version: true\."):
        strict.template("my_example_job")


def test_sweep_dry_run_lists_without_deleting(lakeflow: Lakeflow) -> None:
    assert lakeflow.catalog, "set LAKEFLOW_CATALOG"
    with (
        lakeflow.schema() as schema,
        lakeflow.job("my_example_job", setup=check_compute, teardown=report_params) as job,
    ):
        assert job.job_id is not None
        notebooks: dict[str, str] = {}
        for task in job.settings.tasks or []:
            if task.notebook_task is not None:
                notebooks[task.task_key] = task.notebook_task.notebook_path
        setup, teardown = notebooks["__pytest_setup"], notebooks["__pytest_teardown"]
        time.sleep(1)
        # Swept as another run with no age limit would sweep, so this test's job, schema and notebooks are listed.
        options = session.SweepOptions(dry_run=True, schemas_catalog=lakeflow.catalog, notebooks=True)
        report = session.sweep(lakeflow.ws, "another_run", 0, posixpath.dirname(setup), options)
        assert report.dry_run
        assert report.errors == ()
        assert job.job_id in report.jobs
        assert schema in report.schemas
        assert {setup, teardown} <= set(report.notebooks), report.notebooks
        assert lakeflow.ws.jobs.get(job.job_id).job_id == job.job_id
        assert lakeflow.ws.schemas.get(schema).full_name == schema
        for path in (setup, teardown):
            lakeflow.ws.workspace.get_status(path)


def test_keep_ttl_tag_protects_a_job_from_the_sweep(lakeflow: Lakeflow, monkeypatch: pytest.MonkeyPatch) -> None:
    # As --lakeflow-keep-ttl=48h sets it, for this test only.
    monkeypatch.setattr(session.current(), "keep_ttl", timedelta(hours=48))
    with lakeflow.job("my_example_job") as job:
        assert job.job_id is not None
        stored = lakeflow.ws.jobs.get(job.job_id).settings
        assert stored is not None and stored.tags is not None
        assert stored.tags["pytest_expires"] == (job.settings.tags or {})["pytest_expires"]
        assert re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ", stored.tags["pytest_expires"])
        time.sleep(1)
        options = session.SweepOptions(dry_run=True)
        report = session.sweep(lakeflow.ws, "another_run", 0, "/unused", options)
        assert job.job_id not in report.jobs
