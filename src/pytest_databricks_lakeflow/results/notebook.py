"""Read the cells of a notebook that `jobs.export_run` rendered."""

from __future__ import annotations

import base64
import json
import re
import urllib.parse
import warnings
from html import unescape
from typing import Any

from pytest_databricks_lakeflow.results.models import (
    ANSI,
    NotebookCell,
    NotebookView,
    first_line,
    join_lines,
    last_exception_line,
)

NOTEBOOK_MODEL = "__DATABRICKS_NOTEBOOK_MODEL"
NOTEBOOK_MODEL_LINE = re.compile(NOTEBOOK_MODEL + r"\s*=\s*'([^']*)'")
HTML_TAG = re.compile(r"<[^>]*>")
HTML_RESULTS = frozenset({"html", "htmlSandbox"})
SKIPPED_SUMMARY = "Command skipped"


def parse_notebook_export(html: str) -> NotebookView:
    """Read the cells of a notebook view exported by `jobs.export_run`.

    The export embeds the notebook as a `__DATABRICKS_NOTEBOOK_MODEL` line holding base64 of URL-encoded JSON. The
    format is not documented, so parsing is best effort: when the model is missing or unreadable, the view has no
    cells, keeps the HTML, and a `UserWarning` names the marker.

    Args:
        html: The `content` of a `NOTEBOOK` view.

    Returns:
        The notebook's cells and the HTML.
    """
    model = _notebook_model(html)
    if model is None:
        warnings.warn(
            f"The exported notebook has no readable {NOTEBOOK_MODEL} model, so its cells are empty; "
            "NotebookView.html keeps the export",
            stacklevel=2,
        )
        return NotebookView(html=html)
    commands: list[dict[str, Any]] = []
    for command in model.get("commands") or []:
        if isinstance(command, dict):
            commands.append(command)
    commands.sort(key=_position)
    cells = [_notebook_cell(index, command) for index, command in enumerate(commands)]
    return NotebookView(html=html, cells=cells)


def _notebook_model(html: str) -> dict[str, Any] | None:
    match = NOTEBOOK_MODEL_LINE.search(html)
    if match is None:
        return None
    try:
        encoded = base64.b64decode(match[1]).decode()
        model = json.loads(urllib.parse.unquote(encoded))
    except ValueError:
        return None
    if not isinstance(model, dict):
        return None
    return model


def _position(command: dict[str, Any]) -> float:
    position = command.get("position")
    if isinstance(position, int | float):
        return float(position)
    return 0.0


def _notebook_cell(index: int, command: dict[str, Any]) -> NotebookCell:
    results = command.get("results")
    if not isinstance(results, dict):
        results = {}
    is_error = results.get("type") == "error"
    texts: list[str] = []
    raw: list[str] = []
    printed: list[str] = []
    if not is_error:
        for kind, name, data in _result_items(results):
            raw.append(data)
            if kind in HTML_RESULTS:
                texts.append(_html_text(data))
            else:
                texts.append(ANSI.sub("", data))
            # Printed text is an item named "stdout"; stderr is a sibling item, the exit value a "raw" result.
            if name == "stdout":
                printed.append(ANSI.sub("", data))
    trace = command.get("error")
    if not trace and is_error:
        trace = "".join(data for _, _, data in _result_items(results))
    code = command.get("command")
    if not isinstance(code, str):
        code = ""
    return NotebookCell(
        index=index,
        code=code,
        output=join_lines(texts),
        output_html="".join(raw),
        error=_cell_error(command.get("errorSummary"), trace),
        stdout=join_lines(printed),
    )


def _result_items(results: dict[str, Any]) -> list[tuple[str | None, str | None, str]]:
    # `results.data` is a string, or a list of items with their own `type`, `name` and `data`, or rows of a table.
    data = results.get("data")
    if isinstance(data, str):
        return [(results.get("type"), None, data)]
    if not isinstance(data, list):
        return []
    items: list[tuple[str | None, str | None, str]] = []
    for item in data:
        if isinstance(item, dict) and isinstance(item.get("data"), str):
            items.append((item.get("type"), item.get("name"), item["data"]))
    return items


def _cell_error(summary: object, trace: object) -> str | None:
    # The platform marks a cell skipped after a failure with the summary "Command skipped" and a null `error`.
    if isinstance(summary, str) and summary.strip() == SKIPPED_SUMMARY:
        return None
    if isinstance(summary, str) and summary.strip():
        return _error_text(summary).strip()
    if not isinstance(trace, str) or not trace.strip():
        return None
    text = _error_text(trace)
    last = last_exception_line(text.splitlines())
    if last is None:
        return first_line(text)
    name, message = last
    if message:
        return f"{name}: {message}"
    return name


def _html_text(text: str) -> str:
    return unescape(HTML_TAG.sub("", ANSI.sub("", text)))


def _error_text(text: str) -> str:
    # Older exports render errors as HTML; a plain trace keeps names such as `<locals>` and `<command-1>`.
    if "</" in text:
        return _html_text(text)
    return ANSI.sub("", text)
