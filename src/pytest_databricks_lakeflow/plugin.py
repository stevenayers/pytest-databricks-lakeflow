from __future__ import annotations

import logging
import re
import shutil
import tempfile
from collections.abc import Generator
from datetime import timedelta
from functools import partial
from pathlib import Path
from typing import Any, cast

import pytest

from pytest_databricks_lakeflow import bundle, once
from pytest_databricks_lakeflow.client import PERFORMANCE_TARGETS, Lakeflow
from pytest_databricks_lakeflow.collection import BundleFile
from pytest_databricks_lakeflow.ephemeral.outcome import NOTEBOOKS, PROGRESS, REPORTS
from pytest_databricks_lakeflow.errors import LakeflowBundleError
from pytest_databricks_lakeflow.report import clear as clear_reports
from pytest_databricks_lakeflow.report import write as write_report
from pytest_databricks_lakeflow.session import SessionState, activate, deactivate, resolve_attribution, resolve_run_id
from pytest_databricks_lakeflow.sweep import SWEEP_SCOPES, SweepOptions
from pytest_databricks_lakeflow.workspace_api import cancel_run, stop_update

log = logging.getLogger("pytest_databricks_lakeflow")
STATE = pytest.StashKey[SessionState]()
DURATION = re.compile(r"([0-9]+)([hd])")
DURATION_UNITS = {"h": "hours", "d": "days"}
SWEEP_FLAGS = {
    "lakeflow_sweep_scope": "--lakeflow-sweep-scope",
    "lakeflow_sweep_schemas": "--lakeflow-sweep-schemas",
    "lakeflow_sweep_notebooks": "--lakeflow-sweep-notebooks",
    "lakeflow_sweep_branch": "--lakeflow-sweep-branch",
    "lakeflow_sweep_dry_run": "--lakeflow-sweep-dry-run",
    "lakeflow_sweep_report": "--lakeflow-sweep-report",
}


def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup("lakeflow", "Lakeflow Jobs from a Databricks Asset Bundle")
    group.addoption(
        "--lakeflow-keep", action="store_true", help="Keep every ephemeral job, pipeline copy and temporary schema."
    )
    group.addoption(
        "--lakeflow-keep-failed",
        action="store_true",
        dest="lakeflow_keep_failed",
        default=None,
        help="Keep the ephemeral job and pipeline copies when their test fails, even with Lakeflow(keep_failed=False).",
    )
    group.addoption(
        "--no-lakeflow-keep-failed",
        action="store_false",
        dest="lakeflow_keep_failed",
        default=None,
        help="Delete the ephemeral job and pipeline copies even when their test fails.",
    )
    group.addoption(
        "--lakeflow-keep-ttl",
        metavar="DURATION",
        help="Tag every ephemeral job and pipeline copy, and every temporary schema, with a pytest_expires time this "
        "far ahead, such as 48h or 7d. The sweep keeps them, and the notebooks the jobs use, until then, whatever "
        "their age.",
    )
    group.addoption(
        "--lakeflow-sweep", action="store_true", help="Delete leftover ephemeral jobs and pipelines at session start."
    )
    group.addoption(
        "--lakeflow-sweep-scope",
        choices=SWEEP_SCOPES,
        default=None,
        help="Sweep jobs, pipelines and schemas that you created (mine, the default) or that anyone created (all).",
    )
    group.addoption(
        "--lakeflow-sweep-schemas",
        metavar="CATALOG",
        help="Also drop old pytest_<run id>_* schemas of other runs from this catalog.",
    )
    group.addoption(
        "--lakeflow-sweep-notebooks",
        action="store_true",
        default=None,
        help="Also delete old setup_<hash>, teardown_<hash> and debug notebooks and query_<...>.sql files from the "
        "helpers folder that no job the sweep keeps uses. With scope all, from every user's default helpers folder.",
    )
    group.addoption(
        "--lakeflow-sweep-branch",
        metavar="BRANCH",
        help="Sweep only jobs, pipelines and schemas of this pytest_git_branch, and only the notebooks that their "
        "jobs alone use.",
    )
    group.addoption(
        "--lakeflow-sweep-dry-run",
        action="store_true",
        default=None,
        help="Delete nothing, and list what the sweep would delete.",
    )
    group.addoption(
        "--lakeflow-sweep-report",
        metavar="PATH",
        help="Write what the sweep deleted, or would delete, to this JSON file.",
    )
    group.addoption("--lakeflow-destroy", action="store_true", help="Run bundle destroy at the end of the session.")
    group.addoption(
        "--lakeflow-report-dir",
        dest="lakeflow_report_dir",
        default=None,
        metavar="DIR",
        help="Write each test's runs to DIR: a report.json per test, with raw logs and exported notebooks.",
    )
    group.addoption(
        "--lakeflow-debug-notebook",
        action="store_true",
        dest="lakeflow_debug_notebook",
        help="When a test fails, write a notebook for each failed notebook task of its job's last run, which sets the "
        "task's resolved parameters as widgets and runs the task's notebook.",
    )
    group.addoption(
        "--lakeflow-live",
        action="store_true",
        dest="lakeflow_live",
        help="Print each task state change to the terminal while a run waits.",
    )
    group.addoption(
        "--lakeflow-max-concurrent-runs",
        dest="lakeflow_max_concurrent_runs",
        type=int,
        default=None,
        metavar="N",
        help="Start a run only while fewer than N runs started by this process are active, waiting for one to end "
        "otherwise. The cap is per xdist worker. Overrides Lakeflow(max_concurrent_runs=...).",
    )
    group.addoption(
        "--lakeflow-performance-target",
        dest="lakeflow_performance_target",
        choices=PERFORMANCE_TARGETS,
        default=None,
        metavar="MODE",
        help="Run every ephemeral job in this serverless performance mode: PERFORMANCE_OPTIMIZED, the default, or "
        "STANDARD, which uses fewer DBUs and adds 4 to 6 minutes of start-up to every task, for runs that can wait. "
        "Overrides Lakeflow(performance_target=...).",
    )
    group.addoption(
        "--lakeflow-check",
        action="store_true",
        dest="lakeflow_check",
        help="Collect one test per job of each databricks.yml collected, which lints the job's template with "
        "Lakeflow.check and creates nothing.",
    )


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "lakeflow: runs Lakeflow Jobs in a Databricks workspace. Applied automatically to tests that use the "
        "lakeflow fixture.",
    )
    workerinput: dict[str, Any] = getattr(config, "workerinput", {})
    state_dir = workerinput.get("lakeflow_state_dir")
    attribution = workerinput.get("lakeflow_attribution")
    report_dir = None
    if config.getoption("lakeflow_report_dir"):
        # Relative to where pytest was invoked, as --junitxml is, not to the rootdir.
        report_dir = config.invocation_params.dir / config.getoption("lakeflow_report_dir")
    state = SessionState(
        rootpath=config.rootpath,
        run_id=workerinput.get("lakeflow_run_id") or resolve_run_id(),
        attribution=resolve_attribution(config.rootpath) if attribution is None else attribution,
        state_dir=Path(state_dir) if state_dir else None,
        keep=config.getoption("lakeflow_keep"),
        keep_failed=config.getoption("lakeflow_keep_failed"),
        sweep=config.getoption("lakeflow_sweep"),
        sweep_options=_sweep_options(config),
        keep_ttl=_keep_ttl(config),
        destroy=config.getoption("lakeflow_destroy"),
        report_dir=report_dir,
        debug_notebook=config.getoption("lakeflow_debug_notebook"),
        live=config.getoption("lakeflow_live"),
        max_concurrent_runs=_max_concurrent_runs(config),
        performance_target=config.getoption("lakeflow_performance_target"),
    )
    config.stash[STATE] = state
    activate(state)


def _max_concurrent_runs(config: pytest.Config) -> int | None:
    value = config.getoption("lakeflow_max_concurrent_runs")
    if value is not None and value < 1:
        raise pytest.UsageError(f"--lakeflow-max-concurrent-runs must be 1 or more, got {value}")
    return value


def parse_duration(value: str) -> timedelta:
    """Read a duration in whole hours or days, such as `48h` or `7d`.

    Raises:
        pytest.UsageError: For anything else, a zero duration included.
    """
    match = DURATION.fullmatch(value)
    if match is None or int(match[1]) == 0:
        raise pytest.UsageError(f"--lakeflow-keep-ttl takes hours or days, such as 48h or 7d, not {value!r}")
    return timedelta(**{DURATION_UNITS[match[2]]: int(match[1])})


def _keep_ttl(config: pytest.Config) -> timedelta | None:
    value = config.getoption("lakeflow_keep_ttl")
    if value is None:
        return None
    return parse_duration(value)


def _sweep_options(config: pytest.Config) -> SweepOptions:
    given = [flag for dest, flag in SWEEP_FLAGS.items() if config.getoption(dest) is not None]
    if given and not config.getoption("lakeflow_sweep"):
        verb = "needs" if len(given) == 1 else "need"
        raise pytest.UsageError(f"{', '.join(given)} {verb} --lakeflow-sweep")
    report = config.getoption("lakeflow_sweep_report")
    return SweepOptions(
        scope=config.getoption("lakeflow_sweep_scope") or "mine",
        schemas_catalog=config.getoption("lakeflow_sweep_schemas"),
        notebooks=bool(config.getoption("lakeflow_sweep_notebooks")),
        branch=config.getoption("lakeflow_sweep_branch"),
        dry_run=bool(config.getoption("lakeflow_sweep_dry_run")),
        report=None if report is None else config.invocation_params.dir / report,
    )


def pytest_unconfigure(config: pytest.Config) -> None:
    state = config.stash.get(STATE, None)
    if state is None:
        return
    deactivate(state)
    if state.worker_dir is not None:
        shutil.rmtree(state.worker_dir, ignore_errors=True)


@pytest.hookimpl(tryfirst=True)
def pytest_sessionstart(session: pytest.Session) -> None:
    config = session.config
    state = config.stash.get(STATE, None)
    # On the xdist controller only, before it starts the workers, so no worker deletes what another has written.
    if state is None or state.report_dir is None or hasattr(config, "workerinput"):
        return
    clear_reports(state.report_dir)


@pytest.hookimpl(optionalhook=True)
def pytest_configure_node(node: Any) -> None:
    state = node.config.stash[STATE]
    if state.worker_dir is None:
        state.worker_dir = Path(tempfile.mkdtemp(prefix="pytest-lakeflow-"))
    node.workerinput["lakeflow_run_id"] = state.run_id
    node.workerinput["lakeflow_attribution"] = state.attribution
    node.workerinput["lakeflow_state_dir"] = str(state.worker_dir)


def pytest_collect_file(file_path: Path, parent: pytest.Collector) -> pytest.Collector | None:
    if parent.config.getoption("lakeflow_check") and file_path.name in bundle.BUNDLE_FILES:
        return BundleFile.from_parent(parent, path=file_path)
    return None


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
        if state.report_dir is not None:
            write_report(state.report_dir, item)


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(item: pytest.Item) -> Generator[None, pytest.TestReport, pytest.TestReport]:
    report = yield
    item.stash.setdefault(REPORTS, {})[str(report.when)] = report
    lines = [f"{name}: {value}" for name, value in item.user_properties if name.startswith("lakeflow_")]
    lines.extend(item.stash.get(PROGRESS, []))
    if report.failed:
        lines.extend(item.stash.get(NOTEBOOKS, []))
    if lines:
        report.sections.append(("lakeflow", "\n".join(lines)))
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
    _cancel_active_runs(state)
    if hasattr(config, "workerinput"):
        _report_to_controller(cast(Any, config).workeroutput, state)
        return
    if state.once_jobs:
        once.finish(state, partial(_write_line, session))
    if state.destroy:
        _destroy_deployments(session, state)


def _write_line(session: pytest.Session, line: str) -> None:
    reporter = session.config.pluginmanager.get_plugin("terminalreporter")
    if reporter is not None:
        reporter.write_line(line)


def _cancel_active_runs(state: SessionState) -> None:
    for used in state.lakeflows:
        for run_id in sorted(used.active_runs):
            cancel_run(used.ws, run_id)
        state.cancelled_runs += len(used.active_runs)
        used.active_runs.clear()
        for pipeline_id in sorted(used.active_updates):
            stop_update(used.ws, pipeline_id)
        used.active_updates.clear()


def _report_to_controller(workeroutput: dict[str, Any], state: SessionState) -> None:
    workeroutput["lakeflow_deployed"] = state.deployed
    workeroutput["lakeflow_kept_jobs"] = state.kept_jobs
    workeroutput["lakeflow_kept_pipelines"] = state.kept_pipelines
    workeroutput["lakeflow_kept_schemas"] = state.kept_schemas
    workeroutput["lakeflow_debug_notebooks"] = state.debug_notebooks
    workeroutput["lakeflow_cancelled_runs"] = state.cancelled_runs
    workeroutput["lakeflow_sweep_dry_run"] = state.sweep_dry_run
    workeroutput["lakeflow_once_jobs"] = state.once_jobs


def _destroy_deployments(session: pytest.Session, state: SessionState) -> None:
    unique_deployments = dict.fromkeys(state.deployed)
    for root, target, profile, variables in unique_deployments:
        try:
            bundle.destroy(Path(root), target, profile, dict(variables))
        except LakeflowBundleError as e:
            reporter = session.config.pluginmanager.get_plugin("terminalreporter")
            if reporter is not None:
                reporter.write_sep("=", "lakeflow: bundle destroy failed", red=True)
                reporter.write_line(str(e))
            if session.exitstatus == pytest.ExitCode.OK:
                session.exitstatus = pytest.ExitCode.TESTS_FAILED


@pytest.hookimpl(optionalhook=True)
def pytest_testnodedown(node: Any, error: object) -> None:
    output = getattr(node, "workeroutput", {})
    state = node.config.stash[STATE]
    for root, target, profile, variables in output.get("lakeflow_deployed", []):
        state.deployed.append((root, target, profile, tuple(tuple(pair) for pair in variables)))
    state.kept_jobs.extend(output.get("lakeflow_kept_jobs", []))
    state.kept_pipelines.extend(output.get("lakeflow_kept_pipelines", []))
    state.kept_schemas.extend(output.get("lakeflow_kept_schemas", []))
    state.debug_notebooks.extend(output.get("lakeflow_debug_notebooks", []))
    state.cancelled_runs += output.get("lakeflow_cancelled_runs", 0)
    state.once_jobs.extend(output.get("lakeflow_once_jobs", []))
    if output.get("lakeflow_sweep_dry_run") is not None:
        state.sweep_dry_run = output["lakeflow_sweep_dry_run"]


def pytest_terminal_summary(terminalreporter: pytest.TerminalReporter, config: pytest.Config) -> None:
    state = config.stash.get(STATE, None)
    if state is None or hasattr(config, "workerinput"):
        return
    summarised = (
        state.kept_jobs,
        state.kept_pipelines,
        state.kept_schemas,
        state.debug_notebooks,
        state.cancelled_runs,
    )
    if not any(summarised) and state.sweep_dry_run is None:
        return
    terminalreporter.write_sep("=", "lakeflow")
    if state.sweep_dry_run is not None:
        _summarise_dry_run(terminalreporter, state.sweep_dry_run)
    kept = (("job", state.kept_jobs), ("pipeline", state.kept_pipelines), ("schema", state.kept_schemas))
    for noun, names in kept:
        if names:
            terminalreporter.write_line(f"lakeflow: {_count(len(names), noun)} kept")
            for name in names:
                terminalreporter.write_line(f"  {name}")
    if state.debug_notebooks:
        terminalreporter.write_line(f"lakeflow: {_count(len(state.debug_notebooks), 'debug notebook')} written")
        for url in state.debug_notebooks:
            terminalreporter.write_line(f"  {url}")
    if state.cancelled_runs:
        terminalreporter.write_line(f"lakeflow: {_count(state.cancelled_runs, 'run')} cancelled at session end")
    if state.kept_jobs:
        terminalreporter.write_line("hint: pytest --lakeflow-sweep removes kept jobs older than sweep_hours")


def _summarise_dry_run(terminalreporter: pytest.TerminalReporter, lines: list[str]) -> None:
    if not lines:
        terminalreporter.write_line("lakeflow: sweep dry run found nothing to delete")
        return
    terminalreporter.write_line("lakeflow: sweep dry run deleted nothing; it would delete:")
    for line in lines:
        terminalreporter.write_line(f"  {line}")


def _count(number: int, noun: str) -> str:
    return f"{number} {noun}" if number == 1 else f"{number} {noun}s"
