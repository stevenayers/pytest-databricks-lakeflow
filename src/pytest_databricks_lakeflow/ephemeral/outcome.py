"""Record what a test's runs did on the pytest item: user properties, report sections and terminal lines."""

from __future__ import annotations

import json
from contextlib import AbstractContextManager, nullcontext
from typing import TYPE_CHECKING

import pytest

from pytest_databricks_lakeflow.results.models import (
    DebugNotebook,
    JobRun,
    describe_failing,
    describe_notebooks,
    failing_parameters,
    format_parameters,
)

if TYPE_CHECKING:
    from pytest_databricks_lakeflow.ephemeral.job import EphemeralJob


REPORTS = pytest.StashKey[dict[str, pytest.TestReport]]()
PROGRESS = pytest.StashKey[list[str]]()
NOTEBOOKS = pytest.StashKey[list[str]]()
RUNS = pytest.StashKey[list[tuple["EphemeralJob", JobRun]]]()
DEBUG_NOTEBOOKS = pytest.StashKey[list[DebugNotebook]]()


def record_outcome(item: pytest.Item, result: JobRun) -> None:
    """Record the run's outcome in the test's user properties, with its resolved parameters when it failed.

    The notebook lines of the failing tasks go to the stash instead, for the report section of a failed test.
    """
    item.user_properties.extend(
        [
            ("lakeflow_run_id", result.run_id),
            ("lakeflow_result_state", result.result_state),
            ("lakeflow_duration", f"{result.duration.total_seconds():.3f}"),
        ]
    )
    if not result.failed:
        return
    item.user_properties.append(("lakeflow_failing", "; ".join(describe_failing(result))))
    item.stash.setdefault(NOTEBOOKS, []).extend(describe_notebooks(result))
    run_params = format_parameters(result.parameters)
    if run_params:
        item.user_properties.append(("lakeflow_params", run_params))
    task_params = failing_parameters(result)
    if task_params:
        shown = {key: json.loads(text) for key, text in task_params.items()}
        item.user_properties.append(("lakeflow_task_params", json.dumps(shown, sort_keys=True)))


def emit(item: pytest.Item, line: str) -> None:
    """Write `lakeflow: <line>` to the terminal straight away, past output capture, as pytest's live logging does.

    Does nothing without a terminal reporter, such as under `-p no:terminal`.
    """
    plugins = item.config.pluginmanager
    reporter = plugins.get_plugin("terminalreporter")
    if reporter is None:
        return
    # An xdist worker has a terminal reporter too, but execnet discards what it writes, so the line goes nowhere.
    capture = plugins.get_plugin("capturemanager")
    suspended: AbstractContextManager[object] = nullcontext()
    if capture is not None:
        suspended = capture.global_and_fixture_disabled()
    with suspended:
        reporter.write_line(f"lakeflow: {line}")


def item_failed(stash: pytest.Stash) -> bool:
    return any(report.failed for report in stash.get(REPORTS, {}).values())
