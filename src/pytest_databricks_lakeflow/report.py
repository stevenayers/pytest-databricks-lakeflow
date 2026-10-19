"""Write the runs of each test to `--lakeflow-report-dir`, so a failure can be read after its job is deleted."""

from __future__ import annotations

import hashlib
import json
import logging
import re
from pathlib import Path
from typing import Any

import pytest

from pytest_databricks_lakeflow.ephemeral import REPORTS, RUNS
from pytest_databricks_lakeflow.results import serialise_with_files

log = logging.getLogger("pytest_databricks_lakeflow")

UNSAFE = re.compile(r"[^A-Za-z0-9._-]")
NAME_LIMIT = 80


def directory_name(nodeid: str) -> str:
    """Name a test's report directory: its node id made safe for a file name, cut short, plus a hash of it.

    The hash keeps apart two node ids that differ only in the characters replaced or cut off.
    """
    safe = UNSAFE.sub("_", nodeid)[:NAME_LIMIT]
    digest = hashlib.sha256(nodeid.encode()).hexdigest()[:8]
    return f"{safe}-{digest}"


def write(report_dir: Path, item: pytest.Item) -> None:
    """Write `report.json` and the side files of the runs `item` waited for, if it waited for any.

    An `OSError` is logged as a warning, so the report never fails the test.
    """
    runs = item.stash.get(RUNS, [])
    if not runs:
        return
    directory = report_dir / directory_name(item.nodeid)
    entries: list[dict[str, Any]] = []
    files: dict[str, str] = {}
    for job, run in runs:
        data, run_files = serialise_with_files(run)
        entry = {"job_url": job.url, "params": job.params, "settings": job.settings.as_dict(), **data}
        entries.append(entry)
        files.update(run_files)
    outcome: dict[str, str] = {}
    for when, test_report in item.stash.get(REPORTS, {}).items():
        outcome[when] = test_report.outcome
    report = {"nodeid": item.nodeid, "outcome": outcome, "runs": entries}
    try:
        _write_files(directory, report, files)
    except OSError as e:
        log.warning("Could not write the lakeflow report of %s to %s: %s", item.nodeid, directory, e)


def _write_files(directory: Path, report: dict[str, Any], files: dict[str, str]) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    # default=str, so that a value json cannot encode never breaks the test run from this hook.
    (directory / "report.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    for name, content in files.items():
        path = directory / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
