"""Reading the cells of a notebook that `jobs.export_run` rendered."""

from __future__ import annotations

import warnings
from pathlib import Path

import pytest

from pytest_databricks_lakeflow import NotebookView
from pytest_databricks_lakeflow.results.notebook import parse_notebook_export

from .conftest import CLASSIC_EXPORT, FAILED_EXPORT
from .fakes.runs import notebook_export

HTML_OUTPUT = '<div class="ansiout">rows &gt; 0\nsecond\n</div>'


class TestNotebookExport:
    """parse_notebook_export reads the cells, printed text and failing cell of an exported notebook."""

    def test_parse_notebook_export_cells_and_stdout(self) -> None:
        # Recorded from the example bundle's transform task run with param1="forbidden".
        notebook = parse_notebook_export(FAILED_EXPORT)
        assert notebook.html == FAILED_EXPORT
        assert [cell.index for cell in notebook.cells] == [0, 1, 2, 3]
        assert notebook.cells[0].code.startswith("import time\n")
        assert notebook.cells[1].code.startswith("# The printed lines reach the test")
        printed = "transform: checking forbidden against /Volumes/example/raw/landing\ntransform: 7 rows expected\n"
        assert notebook.cells[1].output == printed
        assert [cell.output for cell in notebook.cells] == ["", printed, "", ""]
        assert notebook.stdout == printed

    def test_parse_recorded_classic_export(self) -> None:
        # Recorded from the classic bundle's printer task on a 17.3 job cluster. The 13.3 LTS export has the same shape:
        # `listResults` of `ansi` items named "stdout" and "stderr", not legacy `<div class="ansiout">` html results.
        notebook = parse_notebook_export(CLASSIC_EXPORT)
        [cell] = notebook.cells
        assert cell.code.startswith("import sys\n\nrows = spark.range(3).count()\n")
        assert cell.output == "printer: 3 rows\nprinter: to stderr\n"
        assert cell.stdout == "printer: 3 rows\n"
        assert notebook.stdout == "printer: 3 rows\n"
        assert cell.error is None

    def test_failed_cell_names_the_exception(self) -> None:
        notebook = parse_notebook_export(FAILED_EXPORT)
        failed = notebook.failed_cell
        assert failed is notebook.cells[2]
        assert failed.code.startswith('dbutils.jobs.taskValues.set(key="stage", value="validation")')
        assert failed.error == "PermissionError: forbidden may not read /Volumes/example/raw/landing"
        # The platform marks the cell after the failure as an error with the summary "Command skipped".
        assert notebook.cells[3].error is None
        assert [cell.error is None for cell in notebook.cells] == [True, True, False, True]

    def test_parse_handles_list_results_and_missing_results(self) -> None:
        commands = [
            {
                "position": 2.0,
                "command": "display(df)",
                "results": {
                    "type": "listResults",
                    "data": [
                        {"type": "html", "data": "<b>bold</b> &amp; more"},
                        {"type": "ansi", "name": "stdout", "data": "\x1b[31mred\x1b[39m <kept>\n"},
                    ],
                },
            },
            {"position": 1.0, "command": "print('rows')", "results": {"type": "html", "data": HTML_OUTPUT}},
            {"position": 3.0, "command": "x = 1"},
            {"position": 4.0, "command": "df.collect()", "results": {"type": "table", "data": [[1, "a"]]}},
            {
                "position": 5.0,
                "command": "raise KeyError('k')\nprint('never')",
                "results": {"type": "error", "data": '<span class="ansi-red-fg">KeyError</span>: &#39;k&#39;'},
            },
        ]
        notebook = parse_notebook_export(notebook_export(commands))
        assert [cell.code for cell in notebook.cells] == [
            "print('rows')",
            "display(df)",
            "x = 1",
            "df.collect()",
            "raise KeyError('k')\nprint('never')",
        ]
        outputs = ["rows > 0\nsecond\n", "bold & more\nred <kept>\n", "", "", ""]
        assert [cell.output for cell in notebook.cells] == outputs
        assert notebook.cells[0].output_html == HTML_OUTPUT
        assert notebook.stdout == "red <kept>\n"
        assert notebook.failed_cell is notebook.cells[4]
        assert notebook.failed_cell.error == "KeyError: 'k'"

    def test_stdout_keeps_only_the_stdout_stream(self) -> None:
        commands = [
            {
                "position": 1.0,
                "command": "print('a'); print('b', file=sys.stderr)",
                "results": {
                    "type": "listResults",
                    "data": [
                        {"type": "ansi", "name": "stdout", "data": "\x1b[32ma\x1b[39m\n"},
                        {"type": "ansi", "name": "stderr", "data": "b\n"},
                    ],
                },
            },
            {
                "position": 2.0,
                "command": "print('c')",
                "results": {
                    "type": "listResults",
                    "data": [
                        {"type": "ansi", "name": "stdout", "data": "c"},
                    ],
                },
            },
            {"position": 3.0, "command": "dbutils.notebook.exit('done')", "results": {"type": "raw", "data": "done"}},
        ]
        notebook = parse_notebook_export(notebook_export(commands))
        assert [cell.output for cell in notebook.cells] == ["a\nb\n", "c\n", "done\n"]
        assert [cell.stdout for cell in notebook.cells] == ["a\n", "c\n", ""]
        assert notebook.stdout == "a\nc\n"

    @pytest.mark.parametrize(
        ("summary", "trace", "expected"),
        [
            pytest.param("Command skipped", None, None, id="skipped"),
            pytest.param("Command skipped", "", None, id="skipped-empty-trace"),
            pytest.param("ValueError: bad input", None, "ValueError: bad input", id="summary-without-trace"),
            pytest.param(None, "Traceback\nKeyError: 'k'\n", "KeyError: 'k'", id="trace-without-summary"),
            pytest.param(None, None, None, id="no-error"),
        ],
    )
    def test_cell_error_decides_skipped_on_the_summary(
        self, summary: str | None, trace: str | None, expected: str | None
    ) -> None:
        command = {"position": 1.0, "command": "x", "results": None, "error": trace, "errorSummary": summary}
        notebook = parse_notebook_export(notebook_export([command]))
        assert notebook.cells[0].error == expected

    @pytest.mark.parametrize(
        "html",
        [
            pytest.param("<html><body>no model here</body></html>", id="missing"),
            pytest.param("<script>var __DATABRICKS_NOTEBOOK_MODEL = '!!not base64';</script>", id="unreadable"),
        ],
    )
    def test_parse_without_marker_warns_and_keeps_html(self, html: str) -> None:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            notebook = parse_notebook_export(html)
        assert (notebook.cells, notebook.html, notebook.stdout, notebook.failed_cell) == ([], html, "", None)
        assert len(caught) == 1
        assert caught[0].category is UserWarning
        assert "__DATABRICKS_NOTEBOOK_MODEL" in str(caught[0].message)

    def test_save_writes_the_exported_html(self, tmp_path: Path) -> None:
        notebook = NotebookView(html="<html>exported</html>")
        saved = notebook.save(tmp_path / "build" / "lakeflow" / "transform.html")
        assert saved == tmp_path / "build" / "lakeflow" / "transform.html"
        assert saved.read_text() == "<html>exported</html>"
