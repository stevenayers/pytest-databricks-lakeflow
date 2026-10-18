from __future__ import annotations

import shutil
import tempfile
from collections.abc import Generator
from pathlib import Path
from typing import Any, cast

import pytest

from pytest_databricks_lakeflow import bundle
from pytest_databricks_lakeflow.client import Lakeflow
from pytest_databricks_lakeflow.ephemeral import REPORTS, cancel_run
from pytest_databricks_lakeflow.errors import LakeflowBundleError
from pytest_databricks_lakeflow.session import SessionState, activate, deactivate, resolve_run_id

STATE = pytest.StashKey[SessionState]()


def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup("lakeflow", "Lakeflow Jobs from a Databricks Asset Bundle")
    group.addoption("--lakeflow-keep", action="store_true", help="Keep every ephemeral job and temporary schema.")
    group.addoption(
        "--lakeflow-keep-failed",
        action="store_true",
        dest="lakeflow_keep_failed",
        default=None,
        help="Keep the ephemeral job when its test fails, even with Lakeflow(keep_failed=False).",
    )
    group.addoption(
        "--no-lakeflow-keep-failed",
        action="store_false",
        dest="lakeflow_keep_failed",
        default=None,
        help="Delete the ephemeral job even when its test fails.",
    )
    group.addoption("--lakeflow-sweep", action="store_true", help="Delete leftover ephemeral jobs at session start.")
    group.addoption("--lakeflow-destroy", action="store_true", help="Run bundle destroy at the end of the session.")


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "lakeflow: runs Lakeflow Jobs in a Databricks workspace. Applied automatically to tests that use the "
        "lakeflow fixture.",
    )
    workerinput: dict[str, Any] = getattr(config, "workerinput", {})
    state_dir = workerinput.get("lakeflow_state_dir")
    state = SessionState(
        rootpath=config.rootpath,
        run_id=workerinput.get("lakeflow_run_id") or resolve_run_id(),
        state_dir=Path(state_dir) if state_dir else None,
        keep=config.getoption("lakeflow_keep"),
        keep_failed=config.getoption("lakeflow_keep_failed"),
        sweep=config.getoption("lakeflow_sweep"),
        destroy=config.getoption("lakeflow_destroy"),
    )
    config.stash[STATE] = state
    activate(state)


def pytest_unconfigure(config: pytest.Config) -> None:
    state = config.stash.get(STATE, None)
    if state is None:
        return
    deactivate(state)
    if state.worker_dir is not None:
        shutil.rmtree(state.worker_dir, ignore_errors=True)


@pytest.hookimpl(optionalhook=True)
def pytest_configure_node(node: Any) -> None:
    state = node.config.stash[STATE]
    if state.worker_dir is None:
        state.worker_dir = Path(tempfile.mkdtemp(prefix="pytest-lakeflow-"))
    node.workerinput["lakeflow_run_id"] = state.run_id
    node.workerinput["lakeflow_state_dir"] = str(state.worker_dir)


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        if "lakeflow" in getattr(item, "fixturenames", ()):
            item.add_marker("lakeflow")


@pytest.hookimpl(wrapper=True)
def pytest_runtest_protocol(item: pytest.Item) -> Generator[None, object, object]:
    state = item.config.stash[STATE]
    previous, state.item = state.item, item
    try:
        return (yield)
    finally:
        state.item = previous


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(item: pytest.Item) -> Generator[None, pytest.TestReport, pytest.TestReport]:
    report = yield
    item.stash.setdefault(REPORTS, {})[str(report.when)] = report
    if report.when == "call":
        urls = [f"{name}: {value}" for name, value in item.user_properties if name.startswith("lakeflow_")]
        if urls:
            report.sections.append(("lakeflow", "\n".join(urls)))
    return report


@pytest.fixture(scope="session")
def lakeflow() -> Lakeflow:
    """Get the `Lakeflow` that tests use. Override this fixture to configure it."""
    return Lakeflow()


@pytest.hookimpl(trylast=True)
def pytest_sessionfinish(session: pytest.Session) -> None:
    config = session.config
    state = config.stash.get(STATE, None)
    if state is None:
        return
    for used in state.lakeflows:
        for run_id in sorted(used.active_runs):
            cancel_run(used.ws, run_id)
        used.active_runs.clear()
    if hasattr(config, "workerinput"):
        cast(Any, config).workeroutput["lakeflow_deployed"] = state.deployed
        return
    if not state.destroy:
        return
    for root, target, profile in dict.fromkeys(state.deployed):
        try:
            bundle.destroy(Path(root), target, profile)
        except LakeflowBundleError as e:
            reporter = config.pluginmanager.get_plugin("terminalreporter")
            if reporter is not None:
                reporter.write_sep("=", "lakeflow: bundle destroy failed", red=True)
                reporter.write_line(str(e))
            if session.exitstatus == pytest.ExitCode.OK:
                session.exitstatus = pytest.ExitCode.TESTS_FAILED


@pytest.hookimpl(optionalhook=True)
def pytest_testnodedown(node: Any, error: object) -> None:
    deployed = getattr(node, "workeroutput", {}).get("lakeflow_deployed", [])
    node.config.stash[STATE].deployed.extend(tuple(entry) for entry in deployed)
