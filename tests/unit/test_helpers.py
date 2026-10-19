from __future__ import annotations

import ast
from typing import cast

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language

from pytest_databricks_lakeflow.helpers import debug_notebook_source, upload_notebook

from .fakes import HOST, FakeWorkspaceClient

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

    def test_values_over_the_widget_limit_are_kept_whole_beside_a_truncated_widget(self) -> None:
        long = "é" * 1500
        source = debug_notebook_source(NOTEBOOK, {"long": long, "short": "x"}, heading="h")
        widgets = source.split(CELL)[0]
        values = widget_values(widgets)
        assert values["short"] == "x"
        assert len(values["long"].encode()) <= 2048
        assert long.startswith(values["long"])
        assert ast.literal_eval(widgets.split("parameters = ", 1)[1].splitlines()[0]) == {"long": long}
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
