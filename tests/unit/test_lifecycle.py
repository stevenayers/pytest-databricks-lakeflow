from __future__ import annotations

import functools
import json
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any

import pytest
from databricks.sdk.service import jobs

from pytest_databricks_lakeflow.errors import LakeflowError
from pytest_databricks_lakeflow.helpers import PARAMS_WIDGET, lifecycle_notebook

from .fakes import FakeDbutils, FakeWorkspaceClient, make_run, run_notebook

pytestmark = pytest.mark.usefixtures("fake_cli", "bundle_dir")

HELPERS_FOLDER = "/Users/someone@example.com/.pytest-databricks-lakeflow"
JOB_TASKS = ["__pytest_setup", "extract", "transform", "load", "classic", "__pytest_teardown"]
WIDGETS = {PARAMS_WIDGET: json.dumps(["param1", "schema"]), "param1": "hello", "schema": "dev.s"}

LIFECYCLE_FUNCTIONS = """
def seed(spark, dbutils, params):
    spark.sql(f"CREATE OR REPLACE TABLE {params['schema']}.orders AS SELECT 1 AS id")
    return {"rows": 1}


def cleanup(dbutils, params):
    dbutils.fs.rm(f"/Volumes/{params['schema']}/landing/orders.csv")
"""


def seed(spark, params):
    spark.append(params["schema"])
    return {"rows": 1, "schema": params["schema"]}


def quiet(dbutils):
    dbutils.widgets.get("param1")


def reads_params(params):
    return params


def annotated(dbutils: FakeDbutils, params: dict[str, str]) -> dict[str, str]:
    return params


def registered(fn: Callable[..., object]) -> Callable[..., object]:
    return fn


@registered
def decorated(spark):
    pass


def make_closure() -> Callable[..., object]:
    table = "orders"

    def uses_table(spark):
        spark.sql(table)

    return uses_table


def defined_in_a_repl() -> Callable[..., object]:
    namespace: dict[str, Any] = {}
    exec("def typed(spark):\n    pass\n", namespace)
    return namespace["typed"]


def takes_a_table(spark, table):
    pass


def takes_anything(*args):
    pass


def run_tests(pytester: pytest.Pytester, source: str, *args: str) -> pytest.RunResult:
    pytester.makepyfile(test_jobs=LIFECYCLE_FUNCTIONS + source)
    return pytester.runpytest(*args)


def created_tasks(ws: FakeWorkspaceClient) -> dict[str, jobs.Task]:
    [created] = ws.jobs.created
    return {task.task_key: task for task in created["tasks"]}


def lifecycle_uploads(ws: FakeWorkspaceClient) -> list[str]:
    return [path for path in ws.workspace.upload_calls if path.rsplit("/", 1)[-1].startswith(("setup_", "teardown_"))]


class TestNotebookSource:
    """The generated notebook passes the declared arguments and exits with the return value as JSON."""

    def test_declared_arguments_only(self) -> None:
        source = lifecycle_notebook(seed, "setup")
        assert source.startswith("# Databricks notebook source\n")
        assert "def seed(spark, params):" in source
        assert "seed(spark=spark, params=" in source
        assert "dbutils=dbutils" not in source
        spark: list[str] = []
        output = run_notebook(source, WIDGETS, spark=spark)
        assert output is not None and json.loads(output) == {"rows": 1, "schema": "dev.s"}
        assert spark == ["dev.s"]

    def test_params_are_the_named_job_parameters_as_strings(self) -> None:
        widgets = {PARAMS_WIDGET: json.dumps(["count", "schema"]), "count": "3", "schema": "dev.s", "other": "x"}
        output = run_notebook(lifecycle_notebook(reads_params, "setup"), widgets)
        assert output is not None and json.loads(output) == {"count": "3", "schema": "dev.s"}

    def test_none_returned_exits_without_output(self) -> None:
        assert run_notebook(lifecycle_notebook(quiet, "teardown"), WIDGETS) is None

    def test_values_json_cannot_encode_become_strings(self) -> None:
        def returns_a_date(params):
            import datetime

            return {"day": datetime.date(2026, 10, 19)}

        output = run_notebook(lifecycle_notebook(returns_a_date, "setup"), WIDGETS)
        assert output == '{"day": "2026-10-19"}'

    def test_annotations_naming_what_the_notebook_does_not_import(self) -> None:
        source = lifecycle_notebook(annotated, "setup")
        output = run_notebook(source, WIDGETS)
        assert output is not None and json.loads(output) == {"param1": "hello", "schema": "dev.s"}
        assert source.startswith("# Databricks notebook source\nfrom __future__ import annotations\n")

    def test_nested_functions_are_dedented(self) -> None:
        def nested(spark):
            return "nested"

        source = lifecycle_notebook(nested, "setup")
        assert "\ndef nested(spark):\n" in source
        assert run_notebook(source, WIDGETS) == '"nested"'


class TestRejectedFunctions:
    """Functions that cannot run on the job's compute are refused with a message that says what to do."""

    @pytest.mark.parametrize(
        ("fn", "message"),
        [
            pytest.param(lambda spark: None, r"setup= cannot be a lambda.*Define it with def", id="lambda"),
            pytest.param(functools.partial(seed), r"setup= must be a plain function defined with def", id="partial"),
            pytest.param(print, r"setup= must be a plain function defined with def", id="builtin"),
            pytest.param(SimpleNamespace(), r"setup= must be a plain function defined with def", id="object"),
            pytest.param(defined_in_a_repl(), r"Cannot read the source of setup=typed.*Define it in a file", id="repl"),
            pytest.param(
                make_closure(),
                r"setup=uses_table uses variables of the function that encloses it: \['table'\].*through params",
                id="free_variables",
            ),
            pytest.param(
                takes_a_table,
                r"setup=takes_a_table takes \['table'\], but it can only take spark, dbutils and params",
                id="unknown_parameter",
            ),
            pytest.param(takes_anything, r"setup=takes_anything takes \['\*args'\]", id="varargs"),
            pytest.param(decorated, r"setup=decorated has a decorator.*Remove the decorator", id="decorator"),
        ],
    )
    def test_rejects(self, fn: Callable[..., object], message: str) -> None:
        with pytest.raises(LakeflowError, match=message):
            lifecycle_notebook(fn, "setup")

    def test_rejected_before_anything_is_created(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        result = run_tests(
            pytester,
            """
import pytest

from pytest_databricks_lakeflow import LakeflowError


def test_job(lakeflow):
    with pytest.raises(LakeflowError, match="teardown= cannot be a lambda"):
        lakeflow.job("my_job", setup=seed, teardown=lambda dbutils: None)
    with pytest.raises(LakeflowError, match="setup= cannot be a lambda"):
        lakeflow.task("my_job.load", setup=lambda spark: None)
""",
        )
        result.assert_outcomes(passed=1)
        assert fake_ws.jobs.created == []
        assert lifecycle_uploads(fake_ws) == []


class TestLifecycleJobs:
    """setup= and teardown= add the two tasks to the copy and upload their notebooks once per session."""

    def test_job_runs_setup_first_and_teardown_last(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        result = run_tests(
            pytester,
            """
def test_job(lakeflow):
    lakeflow.job("my_job", setup=seed, teardown=cleanup).run(only=["transform"])
""",
        )
        result.assert_outcomes(passed=1)
        tasks = created_tasks(fake_ws)
        assert list(tasks) == JOB_TASKS
        setup, teardown = tasks["__pytest_setup"], tasks["__pytest_teardown"]
        assert setup.depends_on is None
        assert setup.notebook_task is not None and teardown.notebook_task is not None
        assert setup.notebook_task.notebook_path.startswith(f"{HELPERS_FOLDER}/setup_")
        assert teardown.notebook_task.notebook_path.startswith(f"{HELPERS_FOLDER}/teardown_")
        assert setup.notebook_task.base_parameters == {PARAMS_WIDGET: '["param1", "schema"]'}
        assert [d.task_key for d in tasks["extract"].depends_on or []] == ["__pytest_setup"]
        assert [d.task_key for d in tasks["classic"].depends_on or []] == ["__pytest_setup"]
        assert [d.task_key for d in teardown.depends_on or []] == ["load", "classic"]
        assert teardown.run_if == jobs.RunIf.ALL_DONE
        uploaded = fake_ws.workspace.uploads[setup.notebook_task.notebook_path]["content"]
        assert "def seed(spark, dbutils, params):" in uploaded
        [call] = fake_ws.jobs.run_now_calls
        assert call["only"] == ["transform", "__pytest_setup", "__pytest_teardown"]

    def test_task_puts_stubs_downstream_of_setup(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        result = run_tests(
            pytester,
            """
def test_task(lakeflow):
    lakeflow.task(
        "my_job.transform",
        upstream_task_values={"extract": {"row_count": 1, "source_path": "/x"}},
        capture_task_values=["rows_written"],
        setup=seed,
        teardown=cleanup,
    ).create()
""",
        )
        result.assert_outcomes(passed=1)
        tasks = created_tasks(fake_ws)
        assert list(tasks) == [
            "__pytest_setup",
            "extract",
            "transform",
            "__pytest_probe__transform",
            "__pytest_teardown",
        ]
        assert [d.task_key for d in tasks["extract"].depends_on or []] == ["__pytest_setup"]
        assert [d.task_key for d in tasks["__pytest_teardown"].depends_on or []] == ["__pytest_probe__transform"]

    def test_one_upload_per_distinct_source(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        result = run_tests(
            pytester,
            """
def test_a(lakeflow):
    lakeflow.job("my_job", setup=seed, teardown=cleanup).create()


def test_b(lakeflow):
    lakeflow.job("my_job", setup=seed).create()
    lakeflow.task("my_job.load", setup=seed).create()
""",
        )
        result.assert_outcomes(passed=2)
        uploads = lifecycle_uploads(fake_ws)
        assert len(uploads) == 2
        assert sorted(path.rsplit("/", 1)[-1].split("_")[0] for path in uploads) == ["setup", "teardown"]
        assert len(fake_ws.jobs.created) == 3

    def test_neither_argument_changes_nothing(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        result = run_tests(pytester, 'def test_job(lakeflow):\n    lakeflow.job("my_job").run(only=["transform"])\n')
        result.assert_outcomes(passed=1)
        assert list(created_tasks(fake_ws)) == ["extract", "transform", "load", "classic"]
        assert lifecycle_uploads(fake_ws) == []
        assert fake_ws.jobs.run_now_calls[0]["only"] == ["transform"]

    def test_failed_setup_fails_the_run_and_leads_the_repr(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        states = {
            "__pytest_setup": "FAILED",
            "extract": "UPSTREAM_FAILED",
            "transform": "UPSTREAM_FAILED",
            "load": "UPSTREAM_FAILED",
            "classic": "UPSTREAM_FAILED",
            "__pytest_teardown": "SUCCESS",
        }
        fake_ws.jobs.queue(
            make_run(77, states, "FAILED", "RUN_EXECUTION_ERROR"),
            {7700: jobs.RunOutput(error="ValueError: no seed")},
        )
        result = run_tests(
            pytester,
            """
def test_job(lakeflow):
    run = lakeflow.job("my_job", setup=seed, teardown=cleanup).run()
    assert not run.succeeded
    assert run.setup.error.type == "ValueError"
    assert run.teardown.succeeded
    assert "failing=['setup: ValueError: no seed', 'extract: UPSTREAM_FAILED'" in repr(run)
""",
        )
        result.assert_outcomes(passed=1)

    def test_failed_teardown_warns_and_leaves_the_run_alone(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        states = {"__pytest_setup": "SUCCESS", "extract": "SUCCESS", "__pytest_teardown": "FAILED"}
        fake_ws.jobs.queue(
            make_run(77, states, "FAILED", "RUN_EXECUTION_ERROR"),
            {
                7700: jobs.RunOutput(notebook_output=jobs.NotebookOutput(result='{"rows": 1}')),
                7702: jobs.RunOutput(error="FileNotFoundError: /Volumes/x"),
            },
        )
        result = run_tests(
            pytester,
            """
import logging


def test_job(lakeflow, caplog):
    caplog.set_level(logging.WARNING, logger="pytest_databricks_lakeflow")
    run = lakeflow.job("my_job", setup=seed, teardown=cleanup).run()
    assert run.succeeded
    assert run.setup.output_json == {"rows": 1}
    assert not run.teardown.succeeded
    assert "FileNotFoundError: /Volumes/x" in caplog.text
    assert run.teardown.run_url in caplog.text
""",
        )
        result.assert_outcomes(passed=1)
