"""Write the runs of each test to `--lakeflow-report-dir`, so a failure can be read after its job is deleted."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import logging
import re
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from pytest_databricks_lakeflow.ephemeral import DEBUG_NOTEBOOKS, REPORTS, RUNS
from pytest_databricks_lakeflow.results import serialise_with_files

if TYPE_CHECKING:
    from pytest_databricks_lakeflow.ephemeral import EphemeralJob
    from pytest_databricks_lakeflow.results import JobRun

log = logging.getLogger("pytest_databricks_lakeflow")

UNSAFE = re.compile(r"[^A-Za-z0-9._-]")
NAME_LIMIT = 80
DIGEST_CHARS = 8
TEST_DIRECTORY = re.compile(rf"[A-Za-z0-9._-]+-[0-9a-f]{{{DIGEST_CHARS}}}")


def directory_name(nodeid: str) -> str:
    """Name a test's report directory: its node id made safe for a file name, cut short, plus a hash of it.

    The hash keeps apart two node ids that differ only in the characters replaced or cut off.
    """
    safe = UNSAFE.sub("_", nodeid)[:NAME_LIMIT]
    digest = hashlib.sha256(nodeid.encode()).hexdigest()[:DIGEST_CHARS]
    return f"{safe}-{digest}"


def clear(report_dir: Path) -> None:
    """Delete the test directories that an earlier session wrote to `report_dir`, so no stale file survives a rerun.

    Only a directory named as `directory_name` names it and holding a `report.json` is deleted; anything else in
    `report_dir` is left alone. An `OSError` is logged as a warning.
    """
    if not report_dir.is_dir():
        return
    try:
        for path in report_dir.iterdir():
            if TEST_DIRECTORY.fullmatch(path.name) and (path / "report.json").is_file():
                shutil.rmtree(path)
    except OSError as e:
        log.warning("Could not clear the lakeflow reports of an earlier session from %s: %s", report_dir, e)


def write(report_dir: Path, item: pytest.Item) -> None:
    """Write `report.json` and the side files of the runs `item` waited for, if it waited for any.

    A test that waited for no run but wrote a debug notebook, as the last test of a module whose fixture closes the
    job, gets a report too. The runs are then dropped from the item, which pytest keeps until the session ends. Any
    error from serialising or writing them is logged as a warning, so the report never fails the test nor ends the
    session.
    """
    runs = item.stash.get(RUNS, [])
    if not runs and DEBUG_NOTEBOOKS not in item.stash:
        return
    if runs:
        del item.stash[RUNS]
    directory = report_dir / directory_name(item.nodeid)
    # An error raised from this hook would end the session with INTERNALERROR.
    try:
        report, files = _build(item, runs)
        _write_files(directory, report, files)
    except Exception as e:
        log.warning("Could not write the lakeflow report of %s to %s: %s", item.nodeid, directory, e)


def _build(item: pytest.Item, runs: list[tuple[EphemeralJob, JobRun]]) -> tuple[dict[str, Any], dict[str, str]]:
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
    notebooks = [dataclasses.asdict(notebook) for notebook in item.stash.get(DEBUG_NOTEBOOKS, [])]
    report = {"nodeid": item.nodeid, "outcome": outcome, "runs": entries, "debug_notebooks": notebooks}
    return report, files


def _write_files(directory: Path, report: dict[str, Any], files: dict[str, str]) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    # default=str, so that a value json cannot encode never breaks the test run from this hook.
    (directory / "report.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    for name, content in files.items():
        path = directory / name
        path.parent.mkdir(parents=True, exist_ok=True)
        # errors="replace", because a log can hold a lone surrogate that UTF-8 cannot encode, and an error raised
        # from this hook ends the session with INTERNALERROR.
        path.write_text(content, encoding="utf-8", errors="replace")
