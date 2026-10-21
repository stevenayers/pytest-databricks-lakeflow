"""Setup and teardown steps that run first and last in every run of the copy, or when the job closes."""

from __future__ import annotations

import functools
import json
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from databricks.sdk.service import jobs

from pytest_databricks_lakeflow import LakeflowError
from pytest_databricks_lakeflow.helpers import LifecycleStep, lifecycle_source
from pytest_databricks_lakeflow.naming import ENV_KEY, PARAMS_WIDGET
from pytest_databricks_lakeflow.settings.compute import Helpers
from pytest_databricks_lakeflow.settings.lifecycle import check_lifecycle_params, inject_lifecycle
from pytest_databricks_lakeflow.settings.stubs import wrap_task

from .conftest import HELPERS, configure_lakeflow, task, three_tasks
from .fakes.notebooks import FakeDbutils, FakeSpark, run_notebook
from .fakes.runs import make_run
from .fakes.templates import example_settings
from .fakes.workspace import FakeWorkspaceClient

pytestmark = pytest.mark.usefixtures("fake_cli", "bundle_dir")
HELPERS_FOLDER = "/Users/someone@example.com/.pytest-databricks-lakeflow"
JOB_TASKS = ["__pytest_setup", "extract", "transform", "load", "classic", "__pytest_teardown"]
WIDGETS = {PARAMS_WIDGET: json.dumps(["param1", "schema"]), "param1": "hello", "schema": "dev.s"}
MARKER = "# Databricks notebook source"
SEED_STATEMENT = "CREATE OR REPLACE TABLE ${schema}.t AS SELECT 1"
PLAIN_FUNCTION_OR_LIST = r"setup= must be a plain function defined with def, a list of SQL statements or a Path"
RUN_DATE_STEPS = """params={"run_date": "2026-10-19"}, teardown=["DELETE FROM ${schema}.t WHERE day = '${run_date}'"]"""
DECLARE_RUN_DATE = """

from databricks.sdk.service import jobs


def declare_run_date(settings, item):
    settings.parameters = [*(settings.parameters or []), jobs.JobParameterDefinition(name="run_date", default="d")]
"""
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


def notebook(step: LifecycleStep, argument: str = "setup") -> str:
    return lifecycle_source(step, Path("/nowhere"), argument)[0]


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
        source = notebook(seed, "setup")
        assert source.startswith("# Databricks notebook source\n")
        assert "def seed(spark, params):" in source
        assert "seed(spark=spark, params=" in source
        assert "dbutils=dbutils" not in source
        spark: list[str] = []
        output = run_notebook(source, WIDGETS, spark=spark)
        assert output is not None and json.loads(output) == {"rows": 1, "schema": "dev.s"}
        assert spark == ["dev.s"]

    @pytest.mark.parametrize(
        ("get_all", "expected"),
        [
            pytest.param(True, {"count": "3", "schema": "dev.s", "run_date": "2026-10-19"}, id="every_job_parameter"),
            pytest.param(False, {"count": "3", "schema": "dev.s"}, id="named_without_getAll"),
        ],
    )
    def test_params_are_the_job_parameters_as_strings(self, get_all: bool, expected: dict[str, str]) -> None:
        # run_date stands for a parameter passed only to run(params=...), which the platform still makes a widget.
        widgets = {
            PARAMS_WIDGET: json.dumps(["count", "schema"]),
            "__pytest_test_id": "t.py::t",
            "count": "3",
            "schema": "dev.s",
            "run_date": "2026-10-19",
        }
        output = run_notebook(notebook(reads_params, "setup"), widgets, get_all=get_all)
        assert output is not None and json.loads(output) == expected

    def test_none_returned_exits_without_output(self) -> None:
        assert run_notebook(notebook(quiet, "teardown"), WIDGETS) is None

    def test_values_json_cannot_encode_become_strings(self) -> None:
        def returns_a_date(params):
            import datetime

            return {"day": datetime.date(2026, 10, 19)}

        output = run_notebook(notebook(returns_a_date, "setup"), WIDGETS)
        assert output == '{"day": "2026-10-19"}'

    def test_annotations_naming_what_the_notebook_does_not_import(self) -> None:
        source = notebook(annotated, "setup")
        output = run_notebook(source, WIDGETS)
        assert output is not None and json.loads(output) == {"param1": "hello", "schema": "dev.s"}
        assert source.startswith("# Databricks notebook source\nfrom __future__ import annotations\n")

    def test_nested_functions_are_dedented(self) -> None:
        def nested(spark):
            return "nested"

        source = notebook(nested, "setup")
        assert "\ndef nested(spark):\n" in source
        assert run_notebook(source, WIDGETS) == '"nested"'


class TestSqlStatements:
    """A list of SQL statements runs through spark.sql in order, rendered from the job parameters."""

    @pytest.mark.parametrize(
        ("statement", "rendered", "names"),
        [
            pytest.param(SEED_STATEMENT, "CREATE OR REPLACE TABLE dev.s.t AS SELECT 1", {"schema"}, id="placeholder"),
            pytest.param("SELECT '$$1' AS price, $param1", "SELECT '$1' AS price, hello", {"param1"}, id="dollars"),
            pytest.param(
                "SELECT \"a\\b\", '''{{x}}'''\n-- ${schema}",
                "SELECT \"a\\b\", '''{{x}}'''\n-- dev.s",
                {"schema"},
                id="quotes_braces_and_newlines",
            ),
        ],
    )
    def test_renders_and_runs(self, statement: str, rendered: str, names: set[str]) -> None:
        source, read = lifecycle_source([statement, "SELECT 2"], Path("/nowhere"), "setup")
        assert "spark.sql(" in source
        assert repr(statement) in source, "statements are shipped unrendered"
        assert read == frozenset(names)
        spark = FakeSpark()
        output = run_notebook(source, WIDGETS, spark=spark)
        assert spark.statements == [rendered, "SELECT 2"]
        assert output is not None and json.loads(output) == {"statements": 2}

    def test_unknown_placeholder_is_refused(self) -> None:
        names = lifecycle_source(["SELECT ${nope}, ${schema}"], Path("/nowhere"), "teardown")[1]
        with pytest.raises(LakeflowError, match=r"teardown= reads \['nope'\].*Declared: \['param1', 'schema'\]"):
            check_lifecycle_params("my_job", example_settings(), {}, "teardown", names)
        check_lifecycle_params("my_job", example_settings(), {"nope": "x"}, "teardown", names)


class TestNotebookFiles:
    """A .py file, resolved against the rootdir, becomes the notebook, with the notebook marker added if missing."""

    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            pytest.param("dbutils.notebook.exit('x')\n", f"{MARKER}\ndbutils.notebook.exit('x')\n", id="no_marker"),
            pytest.param(f"{MARKER}\nprint(1)\n", f"{MARKER}\nprint(1)\n", id="marker"),
        ],
    )
    def test_file_text_becomes_the_notebook(self, tmp_path: Path, text: str, expected: str) -> None:
        (tmp_path / "fixtures").mkdir()
        (tmp_path / "fixtures" / "seed.py").write_text(text)
        assert lifecycle_source(Path("fixtures/seed.py"), tmp_path, "setup") == (expected, frozenset())


class TestRejectedSteps:
    """Statements and files that cannot run are refused with a message that says what to do."""

    @pytest.mark.parametrize(
        ("step", "message"),
        [
            pytest.param(SEED_STATEMENT, r"setup= takes a list of SQL statements, not a string", id="bare_string"),
            pytest.param([SEED_STATEMENT, 1], r"setup= statement 2 is not a string: 1", id="not_a_string"),
            pytest.param([], r"setup= is an empty list of SQL statements", id="empty_list"),
            pytest.param({SEED_STATEMENT}, PLAIN_FUNCTION_OR_LIST, id="set"),
            pytest.param((s for s in [SEED_STATEMENT]), PLAIN_FUNCTION_OR_LIST, id="generator"),
            pytest.param(["SELECT '$1'"], r"setup= statement 1 has an invalid placeholder.*\$\$", id="bad_dollar"),
            pytest.param(Path("missing.py"), r"setup= file .*missing\.py does not exist", id="missing"),
            pytest.param(Path("seed.sql"), r"setup= must be a \.py file, got seed\.sql", id="sql_file"),
        ],
    )
    def test_rejects(self, tmp_path: Path, step: LifecycleStep, message: str) -> None:
        (tmp_path / "seed.sql").write_text("SELECT 1")
        with pytest.raises(ValueError, match=message):
            lifecycle_source(step, tmp_path, "setup")


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
        with pytest.raises(ValueError, match=message):
            notebook(fn, "setup")

    def test_rejected_before_anything_is_created(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        pytester.makefile(".sql", seed="SELECT 1")
        result = run_tests(
            pytester,
            """
from pathlib import Path

import pytest

from pytest_databricks_lakeflow import LakeflowError


def test_job(lakeflow):
    with pytest.raises(ValueError, match="teardown= cannot be a lambda"):
        lakeflow.job("my_job", setup=seed, teardown=lambda dbutils: None)
    with pytest.raises(ValueError, match="setup= cannot be a lambda"):
        lakeflow.task("my_job.load", setup=lambda spark: None)
    with pytest.raises(LakeflowError, match="nope"):
        lakeflow.job("my_job", params={"schema": "s"}, setup=["SELECT ${schema}"], teardown=["DROP ${nope}"])
    with pytest.raises(LakeflowError, match="nope"):
        lakeflow.task("my_job.load", setup=["SELECT ${nope}"])
    with pytest.raises(ValueError, match="not a string"):
        lakeflow.job("my_job", setup="SELECT 1")
    with pytest.raises(ValueError, match="does not exist"):
        lakeflow.job("my_job", teardown=Path("missing.py"))
    with pytest.raises(ValueError, match=r"\\.py file"):
        lakeflow.job("my_job", setup=Path("seed.sql"))
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
    lakeflow.job("my_job", setup=seed, teardown=cleanup).run(only=["load"])
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
        assert call["only"] == ["load", "__pytest_setup", "__pytest_teardown"]

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

    def test_sql_setup_uploads_once_per_session(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        result = run_tests(
            pytester,
            """
SEED = ["CREATE OR REPLACE TABLE ${schema}.orders AS SELECT 1"]


def test_a(lakeflow):
    lakeflow.job("my_job", params={"schema": "s"}, setup=SEED).create()


def test_b(lakeflow):
    lakeflow.job("my_job", params={"schema": "s"}, setup=list(SEED)).create()
""",
        )
        result.assert_outcomes(passed=2)
        [path] = lifecycle_uploads(fake_ws)
        assert path.startswith(f"{HELPERS_FOLDER}/setup_")
        assert "spark.sql(" in fake_ws.workspace.uploads[path]["content"]
        assert "${schema}.orders" in fake_ws.workspace.uploads[path]["content"]
        setup = fake_ws.jobs.created[0]["tasks"][0]
        assert setup.task_key == "__pytest_setup"
        assert setup.notebook_task.base_parameters == {PARAMS_WIDGET: '["param1", "schema"]'}

    def test_file_setup_is_read_from_the_rootdir(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        pytester.mkdir("fixtures")
        pytester.path.joinpath("fixtures", "seed.py").write_text("spark.sql('SELECT 1')\n")
        result = run_tests(
            pytester,
            """
from pathlib import Path


def test_job(lakeflow):
    lakeflow.job("my_job", setup=Path("fixtures/seed.py")).create()
""",
        )
        result.assert_outcomes(passed=1)
        [path] = lifecycle_uploads(fake_ws)
        assert fake_ws.workspace.uploads[path]["content"] == f"{MARKER}\nspark.sql('SELECT 1')\n"

    @pytest.mark.parametrize(
        ("arguments", "extra", "job_arguments"),
        [
            pytest.param("modify_settings=declare_run_date", DECLARE_RUN_DATE, RUN_DATE_STEPS, id="modify_settings"),
            pytest.param("allow_undeclared_params=True", "", RUN_DATE_STEPS, id="allow_undeclared_params"),
            pytest.param(
                "modify_settings=declare_run_date", DECLARE_RUN_DATE, "teardown=cleanup", id="declared_after_job()"
            ),
        ],
    )
    def test_params_widget_names_parameters_the_job_gets_later(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, arguments: str, extra: str, job_arguments: str
    ) -> None:
        configure_lakeflow(pytester, arguments, extra)
        result = run_tests(
            pytester, f'def test_job(lakeflow):\n    lakeflow.job("my_job", setup=seed, {job_arguments}).create()\n'
        )
        result.assert_outcomes(passed=1)
        tasks = created_tasks(fake_ws)
        for key in ("__pytest_setup", "__pytest_teardown"):
            notebook_task = tasks[key].notebook_task
            assert notebook_task is not None and notebook_task.base_parameters is not None
            assert json.loads(notebook_task.base_parameters[PARAMS_WIDGET]) == ["param1", "schema", "run_date"]

    def test_neither_argument_changes_nothing(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        result = run_tests(pytester, 'def test_job(lakeflow):\n    lakeflow.job("my_job").run(only=["load"])\n')
        result.assert_outcomes(passed=1)
        assert list(created_tasks(fake_ws)) == ["extract", "transform", "load", "classic"]
        assert lifecycle_uploads(fake_ws) == []
        assert fake_ws.jobs.run_now_calls[0]["only"] == ["load"]

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
    assert run.teardown.task.run_url in caplog.text
""",
        )
        result.assert_outcomes(passed=1)


FULL_RUN_STATES = {
    "__pytest_setup": "SUCCESS",
    "extract": "SUCCESS",
    "transform": "SUCCESS",
    "load": "SUCCESS",
    "classic": "SUCCESS",
    "__pytest_teardown": "DISABLED",
}
TEARDOWN_ONLY_STATES = {
    "__pytest_setup": "DISABLED",
    "extract": "DISABLED",
    "transform": "DISABLED",
    "load": "DISABLED",
    "classic": "DISABLED",
    "__pytest_teardown": "SUCCESS",
}
EVERY_TASK_BUT_TEARDOWN = ["__pytest_setup", "extract", "transform", "load", "classic"]
CLOSE_JOB = 'lakeflow.job("my_job", params={"schema": "s"}, setup=seed, teardown=cleanup, teardown_at="close")'


def queue_runs(ws: FakeWorkspaceClient, teardown: jobs.Run | None = None, **teardown_polls: Any) -> None:
    """Queue a finished full run as run 76, then the run that closing the job starts, by default a passing one."""
    ws.jobs.queue(make_run(76, FULL_RUN_STATES), polls=0)
    ws.jobs.queue(teardown or make_run(77, TEARDOWN_ONLY_STATES), **teardown_polls)


def only_of_each_run(ws: FakeWorkspaceClient) -> list[list[str] | None]:
    return [call["only"] for call in ws.jobs.run_now_calls]


class TestTeardownAtClose:
    """teardown_at="close" leaves the teardown task out of every run and runs it alone when the job closes."""

    @pytest.mark.parametrize(
        "source",
        [
            pytest.param(
                f"""
def test_job(lakeflow):
    with {CLOSE_JOB} as job:
        run = job.run(params={{"param1": "only this run"}})
        assert run.succeeded, run
        assert run.teardown is None
        assert job.teardown is None
    assert job.runs == [run]
    assert (job.teardown.ran_as, job.teardown.succeeded) == ("task", True)
    assert job.teardown_at == "close"
""",
                id="with",
            ),
            pytest.param(
                f"""
def test_job(lakeflow):
    job = {CLOSE_JOB}
    assert job.run(params={{"param1": "only this run"}}).teardown is None
""",
                id="finaliser",
            ),
        ],
    )
    def test_job_closes_with_a_run_of_the_teardown_alone(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, source: str
    ) -> None:
        queue_runs(fake_ws)
        result = run_tests(pytester, source)
        result.assert_outcomes(passed=1)
        first, last = fake_ws.jobs.run_now_calls
        assert first["only"] == EVERY_TASK_BUT_TEARDOWN
        assert first["job_parameters"] == {"schema": "s", "param1": "only this run"}
        assert last["job_id"] == 1001
        assert last["only"] == ["__pytest_teardown"]
        assert last["job_parameters"] == {"schema": "s"}
        # A run that the job's close cancelled may still be ending, and the copy allows one run at a time.
        assert last["queue"] == jobs.QueueSettings(enabled=True)
        assert fake_ws.jobs.deleted == [1001]

    def test_only_adds_the_setup_alone(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        result = run_tests(
            pytester,
            f"""
def test_job(lakeflow):
    with {CLOSE_JOB} as job:
        job.run(only=["load"])
        job.run(only=["+transform+"])
""",
        )
        result.assert_outcomes(passed=1)
        assert only_of_each_run(fake_ws) == [
            ["load", "__pytest_setup"],
            ["__pytest_setup", "extract", "load", "transform"],
            ["__pytest_teardown"],
        ]

    def test_full_run_leaves_out_disabled_tasks(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        # The platform runs a disabled task that only names, and a task whose upstream only leaves out.
        result = run_tests(
            pytester,
            'def test_job(lakeflow):\n    lakeflow.job("my_job", disable=["transform"], teardown=cleanup, '
            'teardown_at="close").run()\n',
        )
        result.assert_outcomes(passed=1)
        assert only_of_each_run(fake_ws) == [["extract", "classic"], ["__pytest_teardown"]]

    def test_only_refuses_disabled_tasks(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        result = run_tests(
            pytester,
            """
import pytest

from pytest_databricks_lakeflow import LakeflowError

def test_job(lakeflow):
    job = lakeflow.job("my_job", disable=["transform"], teardown=cleanup, teardown_at="close")
    with pytest.raises(LakeflowError, match=r"only names \\['transform'\\]"):
        job.run(only=["transform"])
""",
        )
        result.assert_outcomes(passed=1)
        assert fake_ws.jobs.run_now_calls == [], "a job that never ran needs no teardown run"

    def test_cancelled_run_is_still_cleaned_up(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        fake_ws.jobs.queue(make_run(76, FULL_RUN_STATES), polls=100)
        fake_ws.jobs.queue(make_run(77, TEARDOWN_ONLY_STATES))
        result = run_tests(pytester, f"def test_job(lakeflow):\n    with {CLOSE_JOB} as job:\n        job.start()\n")
        result.assert_outcomes(passed=1)
        assert fake_ws.jobs.cancelled == [76]
        assert only_of_each_run(fake_ws) == [EVERY_TASK_BUT_TEARDOWN, ["__pytest_teardown"]]
        assert fake_ws.jobs.deleted == [1001]

    @pytest.mark.parametrize(
        "source",
        [
            pytest.param(f"def test_job(lakeflow):\n    {CLOSE_JOB}\n", id="never_created"),
            pytest.param(f"def test_job(lakeflow):\n    with {CLOSE_JOB}:\n        pass\n", id="never_run"),
        ],
    )
    def test_no_teardown_run_without_a_run(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, source: str
    ) -> None:
        result = run_tests(pytester, source)
        result.assert_outcomes(passed=1)
        assert fake_ws.jobs.run_now_calls == []

    @pytest.mark.parametrize(
        ("arguments", "flags", "outcome", "teardown_runs", "deleted"),
        [
            pytest.param("keep=True", [], "passed", 0, [], id="keep"),
            pytest.param("", ["--lakeflow-keep"], "passed", 0, [], id="lakeflow_keep"),
            pytest.param("", [], "failed", 1, [], id="keep_failed"),
            pytest.param("", ["--no-lakeflow-keep-failed"], "failed", 1, [1001], id="failed_and_deleted"),
        ],
    )
    def test_only_keep_skips_the_teardown_run(
        self,
        pytester: pytest.Pytester,
        fake_ws: FakeWorkspaceClient,
        arguments: str,
        flags: list[str],
        outcome: str,
        teardown_runs: int,
        deleted: list[int],
    ) -> None:
        configure_lakeflow(pytester, arguments)
        queue_runs(fake_ws)
        fails = "\n        assert False, 'boom'" if outcome == "failed" else ""
        result = run_tests(
            pytester, f"def test_job(lakeflow):\n    with {CLOSE_JOB} as job:\n        job.run(){fails}\n", *flags
        )
        result.assert_outcomes(**{outcome: 1})
        assert len(fake_ws.jobs.run_now_calls) == 1 + teardown_runs
        assert fake_ws.jobs.deleted == deleted
        if outcome == "failed" and not deleted:
            result.stdout.fnmatch_lines(["*Kept run: https://example.cloud.databricks.com/jobs/runs/77"])

    @pytest.mark.parametrize(
        ("arguments", "teardown", "polls", "cancelled"),
        [
            pytest.param("", make_run(77, {**TEARDOWN_ONLY_STATES, "__pytest_teardown": "FAILED"}), 1, [], id="failed"),
            pytest.param("timeout=timedelta(0)", make_run(77, TEARDOWN_ONLY_STATES), 100, [77], id="timed_out"),
        ],
    )
    def test_teardown_run_that_does_not_succeed_only_warns(
        self,
        pytester: pytest.Pytester,
        fake_ws: FakeWorkspaceClient,
        arguments: str,
        teardown: jobs.Run,
        polls: int,
        cancelled: list[int],
    ) -> None:
        configure_lakeflow(pytester, arguments)
        queue_runs(fake_ws, teardown, polls=polls)
        result = run_tests(
            pytester,
            f"""
import logging
import re


def test_job(lakeflow, caplog):
    caplog.set_level(logging.WARNING, logger="pytest_databricks_lakeflow")
    with {CLOSE_JOB} as job:
        job.run()
    assert re.search(r"teardown .*https://example.cloud.databricks.com/jobs/runs/77\\b", caplog.text), caplog.text
""",
        )
        result.assert_outcomes(passed=1)
        assert fake_ws.jobs.cancelled == cancelled
        assert fake_ws.jobs.deleted == [1001]

    def test_rejected_before_anything_is_created(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        result = run_tests(
            pytester,
            """
import pytest


def test_job(lakeflow):
    with pytest.raises(ValueError, match=r'teardown_at="close" runs the teardown task .* needs teardown='):
        lakeflow.job("my_job", setup=seed, teardown_at="close")
    with pytest.raises(ValueError, match=r'teardown_at="close" runs the teardown task .* needs teardown='):
        lakeflow.task("my_job.load", teardown_at="close")
    with pytest.raises(ValueError, match=r"teardown_at must be one of run, close, got 'later'"):
        lakeflow.job("my_job", teardown=cleanup, teardown_at="later")
""",
        )
        result.assert_outcomes(passed=1)
        assert fake_ws.jobs.created == []
        assert lifecycle_uploads(fake_ws) == []


class TestInjectLifecycle:
    """inject_lifecycle runs setup before every root task and teardown after every leaf, whatever happened."""

    @staticmethod
    def dependencies(settings: jobs.JobSettings) -> dict[str, list[str]]:
        return {t.task_key: [d.task_key for d in t.depends_on or []] for t in settings.tasks or [] if t.task_key}

    def test_rewires_a_three_task_dag(self) -> None:
        settings = inject_lifecycle(three_tasks(), "/h/setup_1", "/h/teardown_1", HELPERS)
        assert self.dependencies(settings) == {
            "__pytest_setup": [],
            "a": ["__pytest_setup"],
            "b": ["a"],
            "c": ["__pytest_setup"],
            "__pytest_teardown": ["b", "c"],
        }
        setup, teardown = task(settings, "__pytest_setup"), task(settings, "__pytest_teardown")
        assert setup.run_if is None
        assert teardown.run_if == jobs.RunIf.ALL_DONE
        for injected, path in ((setup, "/h/setup_1"), (teardown, "/h/teardown_1")):
            assert injected.notebook_task == jobs.NotebookTask(
                notebook_path=path, base_parameters={"__pytest_params": '["day"]'}
            )
            assert injected.environment_key == ENV_KEY
        assert [e.environment_key for e in settings.environments or []] == [ENV_KEY]

    def test_teardown_follows_probes_and_stubs_follow_setup(self) -> None:
        settings = wrap_task(example_settings(), "transform", {"extract": {"row_count": 1}}, ["rows"], HELPERS)
        settings = inject_lifecycle(settings, "/h/setup_1", "/h/teardown_1", HELPERS)
        assert self.dependencies(settings) == {
            "__pytest_setup": [],
            "extract": ["__pytest_setup"],
            "transform": ["extract"],
            "__pytest_probe__transform": ["transform"],
            "__pytest_teardown": ["__pytest_probe__transform"],
        }
        assert [e.environment_key for e in settings.environments or []].count(ENV_KEY) == 1

    def test_either_alone(self) -> None:
        setup_only = inject_lifecycle(three_tasks(), "/h/setup_1", None, HELPERS)
        assert [t.task_key for t in setup_only.tasks or []] == ["__pytest_setup", "a", "b", "c"]
        teardown_only = inject_lifecycle(three_tasks(), None, "/h/teardown_1", HELPERS)
        assert self.dependencies(teardown_only) == {"a": [], "b": ["a"], "c": [], "__pytest_teardown": ["b", "c"]}
        assert inject_lifecycle(three_tasks(), None, None, HELPERS) == three_tasks()

    def test_on_the_helper_job_cluster(self) -> None:
        helpers = Helpers("/s", "/g", job_cluster_key="classic")
        settings = inject_lifecycle(example_settings(), "/h/setup_1", "/h/teardown_1", helpers)
        assert task(settings, "__pytest_setup").job_cluster_key == "classic"
        assert task(settings, "__pytest_teardown").job_cluster_key == "classic"
        assert ENV_KEY not in [e.environment_key for e in settings.environments or []]
