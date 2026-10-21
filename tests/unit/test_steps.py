"""Setup and teardown steps that run from the test process over Databricks Connect, and the choice of how they run."""

from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace
from typing import Any, cast

import pytest

from pytest_databricks_lakeflow import JobRun, StepResult, TaskError
from pytest_databricks_lakeflow import steps as steps_module

from .conftest import configure_lakeflow, run_tests
from .fakes.runs import make_run
from .fakes.workspace import FakeSdkConfig, FakeWorkspaceClient

pytestmark = pytest.mark.usefixtures("fake_cli", "bundle_dir")
SETUP_TASK = "__pytest_setup"
TEARDOWN_TASK = "__pytest_teardown"


class ConnectSpark:
    """Stands in for a Databricks Connect session, recording each statement and how many runs had started by then."""

    def __init__(self, ws: FakeWorkspaceClient) -> None:
        self.ws = ws
        self.statements: list[str] = []
        self.runs_started: list[int] = []
        self.sessions: list[str | None] = []
        self.stopped_after: list[int] = []

    def sql(self, statement: str) -> None:
        self.statements.append(statement)
        self.runs_started.append(len(self.ws.jobs.run_now_calls))

    def stop(self) -> None:
        self.stopped_after.append(len(self.statements))


@pytest.fixture
def connect(monkeypatch: pytest.MonkeyPatch, fake_ws: FakeWorkspaceClient) -> ConnectSpark:
    """Make databricks-connect available, with every session the fake one."""
    spark = ConnectSpark(fake_ws)

    def connect_session(ws: Any, cluster_id: str | None) -> ConnectSpark:
        spark.sessions.append(cluster_id)
        return spark

    monkeypatch.setattr(steps_module, "connect_available", lambda: True)
    monkeypatch.setattr(steps_module, "connect_session", connect_session)
    return spark


@pytest.fixture
def no_connect(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(steps_module, "connect_available", lambda: False)


def task_keys(ws: FakeWorkspaceClient, index: int = 0) -> list[str]:
    return [task.task_key for task in ws.jobs.created[index]["tasks"]]


class TestStepResult:
    """What a step reports, however it ran."""

    def test_output_json_reads_the_output(self) -> None:
        step = StepResult(role="setup", ran_as="connect", succeeded=True, output='{"rows": 2}')
        assert step.output_json == {"rows": 2}

    @pytest.mark.parametrize(
        ("output", "message"),
        [
            pytest.param(None, "The setup step has no output: it returned None", id="none"),
            pytest.param("rows", "The setup step's output is not JSON: 'rows'", id="not json"),
        ],
    )
    def test_output_json_names_the_step_when_it_cannot(self, output: str | None, message: str) -> None:
        step = StepResult(role="setup", ran_as="connect", succeeded=True, output=output)
        with pytest.raises(ValueError, match=f"^{message}"):
            _ = step.output_json


class TestConnectSteps:
    """With databricks-connect, `setup=` and `teardown=` run from the test process and add no task to the copy."""

    def test_sql_setup_runs_before_the_run_and_adds_no_task(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, connect: ConnectSpark
    ) -> None:
        source = """
def test_job(lakeflow):
    run = lakeflow.job("my_job", params={"schema": "c.s"}, setup=["CREATE TABLE ${schema}.t"]).run()
    assert (run.setup.ran_as, run.setup.succeeded, run.setup.output_json) == ("connect", True, {"statements": 1})
    assert run.setup.task is None
"""
        run_tests(pytester, source).assert_outcomes(passed=1)
        assert SETUP_TASK not in task_keys(fake_ws)
        assert connect.statements == ["CREATE TABLE c.s.t"]
        assert connect.runs_started == [0]
        assert connect.sessions == [None]

    def test_function_setup_gets_spark_dbutils_and_the_runs_parameters(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, connect: ConnectSpark
    ) -> None:
        source = """
seen = {}


def seed(spark, dbutils, params):
    seen.update(spark=spark, dbutils=dbutils, params=params)
    return {"rows": 2, "when": params["schema"]}


def test_job(lakeflow):
    job = lakeflow.job("my_job", params={"schema": "c.s"}, setup=seed)
    run = job.run(params={"param1": "bye"})
    assert run.setup.output_json == {"rows": 2, "when": "c.s"}
    assert seen["params"] == {"param1": "bye", "schema": "c.s"}
    assert seen["dbutils"] is lakeflow.ws.dbutils
    assert seen["spark"].ws is lakeflow.ws
"""
        run_tests(pytester, source).assert_outcomes(passed=1)

    def test_parameters_fall_back_to_the_declared_defaults(
        self, pytester: pytest.Pytester, connect: ConnectSpark
    ) -> None:
        source = """
def test_job(lakeflow):
    lakeflow.job("my_job", setup=["SELECT '${param1}'"]).run()
"""
        run_tests(pytester, source).assert_outcomes(passed=1)
        assert connect.statements == ["SELECT 'hello'"]

    def test_a_failed_setup_raises_before_the_run_starts(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, connect: ConnectSpark
    ) -> None:
        source = """
def broken(spark):
    raise ValueError("no seed")


def test_job(lakeflow):
    lakeflow.job("my_job", setup=broken).run()
"""
        result = run_tests(pytester, source)
        result.assert_outcomes(failed=1)
        result.stdout.fnmatch_lines(
            ["*LakeflowError: setup= of 'my_job' failed before the run started: ValueError: no seed*"]
        )
        assert fake_ws.jobs.run_now_calls == []

    def test_teardown_runs_after_every_run(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, connect: ConnectSpark
    ) -> None:
        source = """
def test_job(lakeflow):
    job = lakeflow.job("my_job", teardown=["DROP TABLE t"])
    first = job.run()
    second = job.run()
    assert (first.teardown.ran_as, second.teardown.succeeded) == ("connect", True)
    assert job.teardown is None
"""
        run_tests(pytester, source).assert_outcomes(passed=1)
        assert TEARDOWN_TASK not in task_keys(fake_ws)
        assert connect.statements == ["DROP TABLE t", "DROP TABLE t"]
        assert connect.runs_started == [1, 2]

    def test_teardown_at_close_runs_when_the_job_closes(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, connect: ConnectSpark
    ) -> None:
        source = """
def test_job(lakeflow):
    with lakeflow.job("my_job", teardown=["DROP TABLE t"], teardown_at="close") as job:
        run = job.run()
        assert run.teardown is None
        assert job.teardown is None
    assert (job.teardown.ran_as, job.teardown.succeeded) == ("connect", True)
"""
        run_tests(pytester, source).assert_outcomes(passed=1)
        assert connect.statements == ["DROP TABLE t"]
        assert [call["only"] for call in fake_ws.jobs.run_now_calls] == [None]

    def test_a_failed_teardown_only_warns(self, pytester: pytest.Pytester, connect: ConnectSpark) -> None:
        source = """
def broken(spark):
    raise PermissionError("nope")


def test_job(lakeflow, caplog):
    run = lakeflow.job("my_job", teardown=broken).run()
    assert run.succeeded
    assert not run.teardown.succeeded
    assert run.teardown.error.type == "PermissionError"
    assert "The teardown step of run" in caplog.text and "PermissionError: nope" in caplog.text
"""
        run_tests(pytester, source).assert_outcomes(passed=1)

    def test_steps_reach_a_running_cluster_when_one_is_set(
        self, pytester: pytest.Pytester, connect: ConnectSpark
    ) -> None:
        configure_lakeflow(pytester, 'existing_cluster_id="c-1"')
        run_tests(pytester, 'def test_job(lakeflow):\n    lakeflow.job("my_job", setup=["SELECT 1"]).run()\n')
        assert connect.sessions == ["c-1"]

    def test_one_session_serves_every_step_and_stops_at_session_end(
        self, pytester: pytest.Pytester, connect: ConnectSpark
    ) -> None:
        source = """
def test_a(lakeflow):
    lakeflow.job("my_job", setup=["SELECT 1"], teardown=["SELECT 2"]).run()


def test_b(lakeflow):
    lakeflow.job("my_job", setup=["SELECT 3"]).run()
    lakeflow.once("rows", teardown=["SELECT 4"])
"""
        run_tests(pytester, source).assert_outcomes(passed=2)
        assert connect.sessions == [None]
        assert connect.statements == ["SELECT 1", "SELECT 2", "SELECT 3", "SELECT 4"]
        assert connect.stopped_after == [4]


class TestRunAsTask:
    """When a step runs as a task instead: a notebook file, `run_as_task=True`, or no databricks-connect."""

    @pytest.mark.parametrize(
        "arguments",
        [
            pytest.param('setup=["SELECT 1"], run_as_task=True', id="per call"),
            pytest.param('setup=Path("seed.py")', id="notebook file"),
        ],
    )
    def test_the_step_becomes_a_task(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, connect: ConnectSpark, arguments: str
    ) -> None:
        pytester.makefile(".py", seed="spark.sql('SELECT 1')\n")
        source = f"""
from pathlib import Path


def test_job(lakeflow):
    run = lakeflow.job("my_job", {arguments}).run()
    assert run.setup.ran_as == "task"
"""
        fake_ws.jobs.queue(make_run(77, {SETUP_TASK: "SUCCESS", "extract": "SUCCESS"}))
        run_tests(pytester, source).assert_outcomes(passed=1)
        assert task_keys(fake_ws)[0] == SETUP_TASK
        assert connect.statements == []

    def test_lakeflow_default_and_a_call_that_overrides_it(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, connect: ConnectSpark
    ) -> None:
        configure_lakeflow(pytester, "run_as_task=True")
        source = """
def test_job(lakeflow):
    lakeflow.job("my_job", setup=["SELECT 1"]).create()
    lakeflow.job("my_job", setup=["SELECT 2"], run_as_task=False).run()
"""
        run_tests(pytester, source).assert_outcomes(passed=1)
        assert task_keys(fake_ws, 0)[0] == SETUP_TASK
        assert SETUP_TASK not in task_keys(fake_ws, 1)
        assert connect.statements == ["SELECT 2"]

    def test_without_databricks_connect_steps_run_as_tasks_with_one_warning(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, no_connect: None
    ) -> None:
        source = """
def test_a(lakeflow):
    lakeflow.job("my_job", setup=["SELECT 1"]).create()


def test_b(lakeflow):
    lakeflow.job("my_job", teardown=["SELECT 2"]).create()
"""
        result = run_tests(pytester, source)
        result.assert_outcomes(passed=2, warnings=1)
        result.stdout.fnmatch_lines(
            ["*databricks-connect is not installed, so setup= and teardown= run as tasks*[connect]*Python 3.12*"]
        )
        assert task_keys(fake_ws, 0)[0] == SETUP_TASK
        assert task_keys(fake_ws, 1)[-1] == TEARDOWN_TASK

    def test_run_as_task_asks_for_no_warning(self, pytester: pytest.Pytester, no_connect: None) -> None:
        configure_lakeflow(pytester, "run_as_task=True")
        result = run_tests(pytester, 'def test_a(lakeflow):\n    lakeflow.job("my_job", setup=["SELECT 1"]).create()\n')
        result.assert_outcomes(passed=1, warnings=0)


class TestStepArguments:
    """What a step that runs over Connect refuses before anything runs, and what it no longer needs."""

    def test_a_lambda_and_a_closure_are_fine_over_connect(
        self, pytester: pytest.Pytester, connect: ConnectSpark
    ) -> None:
        source = """
def test_job(lakeflow):
    table = "c.s.t"

    def seed(spark):
        spark.sql(f"CREATE TABLE {table}")

    lakeflow.job("my_job", setup=seed, teardown=lambda spark: spark.sql("DROP TABLE c.s.t")).run()
"""
        run_tests(pytester, source).assert_outcomes(passed=1)
        assert connect.statements == ["CREATE TABLE c.s.t", "DROP TABLE c.s.t"]

    @pytest.mark.parametrize(
        ("step", "message"),
        [
            pytest.param("lambda spark, table: None", "setup=<lambda> takes [[]'table'[]]", id="unknown argument"),
            pytest.param('"SELECT 1"', "setup= takes a list of SQL statements, not a string", id="string"),
            pytest.param('["SELECT $"]', "setup= statement 1 has an invalid placeholder", id="stray dollar"),
        ],
    )
    def test_refused(self, pytester: pytest.Pytester, connect: ConnectSpark, step: str, message: str) -> None:
        result = run_tests(pytester, f'def test_job(lakeflow):\n    lakeflow.job("my_job", setup={step})\n')
        result.assert_outcomes(failed=1)
        result.stdout.fnmatch_lines([f"*ValueError: {message}*"])


class TestOnceOverConnect:
    """`once()` over Connect runs its setup from the test process, creates no job, and tears down at session end."""

    def test_setup_runs_once_without_a_job_and_teardown_at_session_end(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, connect: ConnectSpark
    ) -> None:
        source = """
STEP = dict(setup=["CREATE TABLE ${schema}.t"], teardown=["DROP TABLE ${schema}.t"], params={"schema": "c.s"})


def test_a(lakeflow):
    step = lakeflow.once("orders", **STEP)
    assert (step.ran_as, step.succeeded, step.output_json) == ("connect", True, {"statements": 1})


def test_b(lakeflow):
    assert lakeflow.once("orders", **STEP).ran_as == "connect"
"""
        run_tests(pytester, source).assert_outcomes(passed=2)
        assert fake_ws.jobs.created == []
        assert connect.statements == ["CREATE TABLE c.s.t", "DROP TABLE c.s.t"]

    def test_a_function_teardown_runs_from_its_source(self, pytester: pytest.Pytester, connect: ConnectSpark) -> None:
        source = """
def drop(spark, params):
    spark.sql(f"DROP TABLE {params['schema']}.t")


def test_a(lakeflow):
    assert lakeflow.once("orders", teardown=drop, params={"schema": "c.s"}) is None
"""
        run_tests(pytester, source).assert_outcomes(passed=1)
        assert connect.statements == ["DROP TABLE c.s.t"]

    def test_a_closure_teardown_is_refused(self, pytester: pytest.Pytester, connect: ConnectSpark) -> None:
        source = """
def test_a(lakeflow):
    table = "c.s.t"

    def drop(spark):
        spark.sql(f"DROP TABLE {table}")

    lakeflow.once("orders", teardown=drop)
"""
        result = run_tests(pytester, source)
        result.assert_outcomes(failed=1)
        result.stdout.fnmatch_lines(["*ValueError: teardown=drop uses variables of the function that encloses it*"])

    def test_a_failed_setup_fails_every_caller(self, pytester: pytest.Pytester, connect: ConnectSpark) -> None:
        source = """
def broken(spark):
    raise ValueError("no schema")


def test_a(lakeflow):
    lakeflow.once("orders", setup=broken)


def test_b(lakeflow):
    lakeflow.once("orders", setup=broken)
"""
        result = run_tests(pytester, source)
        result.assert_outcomes(failed=2)
        result.stdout.fnmatch_lines(
            [
                "*LakeflowError: once('orders'): the setup step failed: ValueError: no schema*",
                "*LakeflowError: once('orders') failed earlier in this session*",
            ]
        )

    def test_keep_skips_the_teardown(self, pytester: pytest.Pytester, connect: ConnectSpark) -> None:
        source = 'def test_a(lakeflow):\n    lakeflow.once("orders", setup=["SELECT 1"], teardown=["SELECT 2"])\n'
        result = run_tests(pytester, source, "--lakeflow-keep")
        result.assert_outcomes(passed=1)
        result.stdout.fnmatch_lines(["*lakeflow: kept what once('orders') set up, without its teardown*"])
        assert connect.statements == ["SELECT 1"]


class TestReport:
    """`as_dict()` and report.json carry the steps."""

    def test_as_dict_holds_the_steps(self) -> None:
        error = TaskError(type="ValueError", message="no", trace="Traceback")
        step = StepResult(
            role="teardown", ran_as="connect", succeeded=False, error=error, duration=timedelta(seconds=2)
        )
        run = JobRun(
            job_key="my_job",
            job_id=1,
            run_id=2,
            run_url="u",
            result_state="SUCCESS",
            termination_code=None,
            termination_message=None,
            duration=timedelta(0),
            tasks={},
            injected_tasks={},
            teardown=step,
        )
        assert run.as_dict()["setup"] is None
        assert run.as_dict()["teardown"] == {
            "role": "teardown",
            "ran_as": "connect",
            "succeeded": False,
            "output": None,
            "error": {"type": "ValueError", "message": "no", "trace": "Traceback", "root_cause": None, "code": None},
            "duration_seconds": 2.0,
            "task_key": None,
        }


class TestConnectSession:
    """The session takes its compute from the SDK config, because Connect refuses sdkConfig with .serverless()."""

    @pytest.mark.parametrize(
        ("cluster_id", "expected"),
        [
            pytest.param(None, {"serverless_compute_id": "auto", "cluster_id": None}, id="serverless"),
            pytest.param("c-1", {"serverless_compute_id": None, "cluster_id": "c-1"}, id="cluster"),
        ],
    )
    def test_compute_is_set_on_a_copy_of_the_client_config(
        self, monkeypatch: pytest.MonkeyPatch, cluster_id: str | None, expected: dict[str, str | None]
    ) -> None:
        given: list[FakeSdkConfig] = []

        class Builder:
            def sdkConfig(self, config: FakeSdkConfig) -> Builder:  # noqa: N802, as Databricks Connect spells it
                given.append(config)
                return self

            def create(self) -> str:
                return "session"

        connect = SimpleNamespace(DatabricksSession=SimpleNamespace(builder=Builder()))
        monkeypatch.setattr(steps_module.importlib, "import_module", lambda name: connect)
        config = FakeSdkConfig(host="https://example.cloud.databricks.com", cluster_id="from-profile")
        ws = SimpleNamespace(config=config)
        assert steps_module.connect_session(cast(Any, ws), cluster_id) == "session"
        [session_config] = given
        assert {key: getattr(session_config, key) for key in expected} == expected
        assert (config.cluster_id, config.serverless_compute_id) == ("from-profile", None)
