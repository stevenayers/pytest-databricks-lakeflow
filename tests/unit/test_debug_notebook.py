"""Debug notebooks that replay a failed notebook task's resolved parameters as widgets."""

from __future__ import annotations

import ast
from typing import Any, cast

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import PermissionDenied
from databricks.sdk.service import jobs
from databricks.sdk.service.workspace import ImportFormat, Language

from pytest_databricks_lakeflow.helpers import debug_notebook_source, upload_notebook

from .conftest import FakeCli, report_of, run_tests
from .fakes.runs import HOST, make_run, queue_failed_run
from .fakes.templates import example_settings
from .fakes.workspace import FakeWorkspaceClient

NOTEBOOK = "/Workspace/Users/someone@example.com/.bundle/example/test/files/src/transform"
CELL = "\n# COMMAND ----------\n"
TRICKY = "{{a}} \"b\"\nc \\ 'd'"


def widget_values(cell: str) -> dict[str, str]:
    """Read the name and default of every `dbutils.widgets.text(...)` line in `cell` as the notebook would."""
    values: dict[str, str] = {}
    for line in cell.splitlines():
        if line.startswith("dbutils.widgets.text("):
            call = ast.parse(line).body[0]
            assert isinstance(call, ast.Expr) and isinstance(call.value, ast.Call)
            name, value = (ast.literal_eval(argument) for argument in call.value.args)
            values[name] = value
    return values


class TestDebugNotebookSource:
    """The notebook that replays a task's resolved parameters as widgets and runs its notebook."""

    def test_debug_notebook_source_sets_widgets_then_runs(self) -> None:
        source = debug_notebook_source(NOTEBOOK, {"row_count": "7", "param1": TRICKY}, heading="Parameters of x")
        widgets, run = source.split(CELL)
        assert widgets.startswith("# Databricks notebook source\n# Parameters of x\n")
        assert "debugValue" in widgets
        assert widget_values(widgets) == {"row_count": "7", "param1": TRICKY}
        assert widgets.index('"row_count"') < widgets.index('"param1"'), "the platform's order is kept"
        assert run.strip() == f"# MAGIC %run {NOTEBOOK}"
        compile(widgets, "widgets", "exec")

    def test_values_over_the_widget_limit_are_named_and_kept_whole_beside_a_truncated_widget(self) -> None:
        long = "é" * 1500
        source = debug_notebook_source(NOTEBOOK, {"long": long, "short": "x", "big": "b" * 2049}, heading="h")
        named, widgets, _ = source.split(CELL)
        assert named.splitlines()[:2] == [
            "# Databricks notebook source",
            '# These widgets hold only the first 2048 bytes of their value: "long", "big". The notebook reads the',
        ]
        assert not [line for line in named.splitlines() if line and not line.startswith("#")], "comments only"
        values = widget_values(widgets)
        assert values["short"] == "x"
        assert len(values["long"].encode()) <= 2048
        assert long.startswith(values["long"])
        written = ast.literal_eval(widgets.split("parameters = ", 1)[1].splitlines()[0])
        assert written == {"long": long, "big": "b" * 2049}
        compile(widgets, "widgets", "exec")


class TestUploadNotebook:
    """Uploading a generated notebook into a folder that may not exist yet."""

    def test_upload_notebook_creates_the_folder_first(self) -> None:
        ws = FakeWorkspaceClient()
        path = "/Users/someone@example.com/.pytest-databricks-lakeflow/debug/my_job__transform__77"
        url = upload_notebook(cast(WorkspaceClient, ws), path, "# Databricks notebook source\n")
        assert ws.workspace.folders == ["/Users/someone@example.com/.pytest-databricks-lakeflow/debug"]
        upload = ws.workspace.uploads[path]
        assert upload["format"] == ImportFormat.SOURCE
        assert upload["language"] == Language.PYTHON
        assert upload["overwrite"] is True
        assert url == f"{HOST}/#notebook/{ws.workspace.get_status(path).object_id}"


DEBUG_FOLDER = "/Users/someone@example.com/.pytest-databricks-lakeflow/debug"
TRANSFORM_PATH = "/Workspace/Users/someone@example.com/.bundle/example/test/files/src/transform"
HELPER_UPLOADS = [
    "/Users/someone@example.com/.pytest-databricks-lakeflow/set_values",
    "/Users/someone@example.com/.pytest-databricks-lakeflow/get_values",
]
FAILS_IN_WITH = """
def test_job(lakeflow):
    with lakeflow.job("my_job") as job:
        job.run()
        assert False, "boom"
"""


def debug_uploads(ws: FakeWorkspaceClient) -> dict[str, dict[str, Any]]:
    return {path: upload for path, upload in ws.workspace.uploads.items() if path.startswith(f"{DEBUG_FOLDER}/")}


@pytest.mark.usefixtures("fake_cli", "bundle_dir")
class TestDebugNotebook:
    """`debug_notebook()` and `--lakeflow-debug-notebook`: a notebook that replays a task's resolved parameters."""

    def test_debug_notebook_uploads_under_helpers_path(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        queue_failed_run(fake_ws)
        result = run_tests(
            pytester,
            """
            def test_job(lakeflow, request):
                with lakeflow.job("my_job") as job:
                    job.run()
                    notebook = job.debug_notebook("transform")
                assert (notebook.task_key, notebook.run_id) == ("transform", 77)
                assert str(notebook) == notebook.url
                print("URL", notebook.url, notebook.path)
                assert ("lakeflow_debug_notebook", notebook.url) in request.node.user_properties
            """,
            "-s",
        )
        result.assert_outcomes(passed=1)
        path = f"{DEBUG_FOLDER}/my_job__transform__77"
        assert list(debug_uploads(fake_ws)) == [path]
        upload = fake_ws.workspace.uploads[path]
        assert f"# MAGIC %run {TRANSFORM_PATH}" in upload["content"]
        assert 'dbutils.widgets.text("row_count", "7")' in upload["content"]
        assert "/jobs/runs/7700" in upload["content"], "the heading links the task run"
        assert (upload["language"], upload["overwrite"]) == (Language.PYTHON, True)
        result.stdout.fnmatch_lines([f"*URL {HOST}/#notebook/{fake_ws.workspace.object_ids[path]} {path}"])

    @pytest.mark.parametrize(
        ("task_key", "message"),
        [
            pytest.param(
                "load",
                "Task 'load' of 'my_job' is not a notebook task, so there is no notebook to run. "
                "Notebook tasks: ['classic', 'extract', 'transform']",
                id="wheel_task",
            ),
            pytest.param(
                "nope",
                "Job 'my_job' has no task 'nope'. Available: ['classic', 'extract', 'load', 'transform']",
                id="unknown_task",
            ),
            pytest.param(
                "git",
                "Task 'git' of 'my_job' runs its notebook from Git (src/git), which %run cannot reach",
                id="git_source",
            ),
        ],
    )
    def test_debug_notebook_rejects_non_notebook_tasks(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, task_key: str, message: str
    ) -> None:
        settings = example_settings()
        if task_key == "git":
            notebook = jobs.NotebookTask(notebook_path="src/git", source=jobs.Source.GIT)
            settings.tasks = [*(settings.tasks or []), jobs.Task(task_key="git", notebook_task=notebook)]
        fake_ws.jobs.settings[fake_cli.deployed["my_job"]] = settings
        result = run_tests(
            pytester,
            f"""
            import re

            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            def test_job(lakeflow):
                job = lakeflow.job("my_job")
                with pytest.raises(LakeflowError, match=re.escape({message!r})):
                    job.debug_notebook({task_key!r})
            """,
        )
        result.assert_outcomes(passed=1)
        assert debug_uploads(fake_ws) == {}

    def test_debug_notebook_needs_resolved_parameters(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        queue_failed_run(fake_ws, {"transform": "UPSTREAM_FAILED"})
        result = run_tests(
            pytester,
            """
            import re

            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    job.run()
                    message = "Task 'transform' did not run in run 77 (UPSTREAM_FAILED), so it has no resolved"
                    with pytest.raises(LakeflowError, match=re.escape(message) + " parameters$"):
                        job.debug_notebook("transform")
                    with pytest.raises(KeyError, match="extract"):
                        job.debug_notebook("extract")
            """,
        )
        result.assert_outcomes(passed=1)
        assert debug_uploads(fake_ws) == {}

    def test_debug_notebook_before_any_run_fails_clearly(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        result = run_tests(
            pytester,
            """
            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    message = "Job 'my_job' has not run yet, so there are no resolved parameters to replay"
                    with pytest.raises(LakeflowError, match=message):
                        job.debug_notebook("transform")
            """,
        )
        result.assert_outcomes(passed=1)

    def test_debug_notebook_flag_writes_for_failed_tests(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        queue_failed_run(fake_ws)
        result = run_tests(pytester, FAILS_IN_WITH, "--lakeflow-debug-notebook", "--junitxml=report.xml")
        result.assert_outcomes(failed=1)
        path = f"{DEBUG_FOLDER}/my_job__transform__77"
        assert list(debug_uploads(fake_ws)) == [path]
        url = f"{HOST}/#notebook/{fake_ws.workspace.object_ids[path]}"
        result.stdout.fnmatch_lines(["*- lakeflow -*", f"lakeflow_debug_notebook: {url}"])
        assert f'<property name="lakeflow_debug_notebook" value="{url}"' in (pytester.path / "report.xml").read_text()

    @pytest.mark.parametrize(
        ("source", "states", "flags"),
        [
            pytest.param(FAILS_IN_WITH, {"transform": "FAILED"}, [], id="without_the_flag"),
            pytest.param(
                'def test_job(lakeflow):\n    with lakeflow.job("my_job") as job:\n        assert job.run().failed\n',
                {"transform": "FAILED"},
                ["--lakeflow-debug-notebook"],
                id="passing_test",
            ),
            pytest.param(FAILS_IN_WITH, {"transform": "SUCCESS"}, ["--lakeflow-debug-notebook"], id="successful_run"),
            pytest.param(FAILS_IN_WITH, {"load": "FAILED"}, ["--lakeflow-debug-notebook"], id="wheel_task_failed"),
        ],
    )
    def test_nothing_is_written_without_a_failed_notebook_task_in_a_failed_test(
        self,
        pytester: pytest.Pytester,
        fake_ws: FakeWorkspaceClient,
        source: str,
        states: dict[str, str],
        flags: list[str],
    ) -> None:
        queue_failed_run(fake_ws, states)
        result = run_tests(pytester, source, *flags)
        result.stdout.no_fnmatch_line("*lakeflow_debug_notebook*")
        result.stdout.no_fnmatch_line("*debug notebook*")
        assert fake_ws.workspace.upload_calls == HELPER_UPLOADS

    def test_debug_notebook_of_a_job_its_finaliser_closes(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        # The finaliser closes the job in the teardown phase, whose report section shows only with -rA, so the
        # terminal summary names the test, and report.json lists the notebook.
        parameters = {"row_count": "7", "payload": "x" * 3000}
        fake_ws.jobs.resolved[7700] = jobs.ResolvedValues(
            notebook_task=jobs.ResolvedNotebookTaskValues(base_parameters=parameters)
        )
        fake_ws.jobs.queue(make_run(77, {"transform": "FAILED"}, "FAILED", "RUN_EXECUTION_ERROR"))
        source = """
            import pytest

            @pytest.fixture
            def failed_run(lakeflow):
                return lakeflow.job("my_job").run()

            def test_job(failed_run):
                assert False, "boom"
            """
        result = run_tests(pytester, source, "--lakeflow-debug-notebook", "--lakeflow-report-dir=out")
        result.assert_outcomes(failed=1)
        path = f"{DEBUG_FOLDER}/my_job__transform__77"
        url = f"{HOST}/#notebook/{fake_ws.workspace.object_ids[path]}"
        result.stdout.fnmatch_lines(
            [
                "*= lakeflow =*",
                "lakeflow: 1 debug notebook written",
                f"  {url} (test_jobs.py::test_job, task 'transform'; widgets cut at 2048 bytes: 'payload')",
            ]
        )
        _, report = report_of(pytester.path / "out")
        assert report["debug_notebooks"] == [{"task_key": "transform", "run_id": 77, "path": path, "url": url}]

    @pytest.mark.parametrize(
        ("key", "message"),
        [
            pytest.param("transform", "no write access", id="upload_fails"),
            pytest.param("ghost", "No task 'ghost' in the job. Available: *", id="task_not_in_the_settings"),
        ],
    )
    def test_debug_notebook_flag_does_not_fail_teardown(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, key: str, message: str
    ) -> None:
        queue_failed_run(fake_ws, {key: "FAILED"})
        fake_ws.workspace.upload_errors[f"{DEBUG_FOLDER}/my_job__transform__77"] = PermissionDenied("no write access")
        result = run_tests(pytester, FAILS_IN_WITH, "--lakeflow-debug-notebook")
        result.assert_outcomes(failed=1)
        result.stdout.fnmatch_lines([f"*Could not write a debug notebook for task {key!r} of run 77: {message}"])
        result.stdout.fnmatch_lines(["*Keeping job 1001 from my_job*"])
        assert "AssertionError: boom" in result.stdout.str()
