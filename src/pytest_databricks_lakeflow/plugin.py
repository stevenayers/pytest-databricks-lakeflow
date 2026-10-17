from __future__ import annotations

import logging
import re
import secrets
from collections.abc import Generator, Iterator
from typing import Any, cast

import pytest

from pytest_databricks_lakeflow import bundle, hooks, session
from pytest_databricks_lakeflow.client import Lakeflow
from pytest_databricks_lakeflow.ephemeral import REPORTS
from pytest_databricks_lakeflow.errors import LakeflowBundleError
from pytest_databricks_lakeflow.options import Options, add_options

log = logging.getLogger("pytest_databricks_lakeflow")

OPTIONS = pytest.StashKey[Options]()
USED = pytest.StashKey[bool]()
FIXTURES = frozenset({"lakeflow", "lakeflow_schema"})


def pytest_addoption(parser: pytest.Parser) -> None:
    add_options(parser)


def pytest_addhooks(pluginmanager: pytest.PytestPluginManager) -> None:
    pluginmanager.add_hookspecs(hooks)


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "lakeflow: runs Lakeflow Jobs in a Databricks workspace. Applied automatically to tests that use the "
        "lakeflow or lakeflow_schema fixtures.",
    )
    config.stash[OPTIONS] = Options.from_config(config)


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        if FIXTURES & set(getattr(item, "fixturenames", ())):
            item.add_marker("lakeflow")


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
def _lakeflow_session(request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory) -> Iterator[Lakeflow]:
    config = request.config
    options = config.stash[OPTIONS]
    ws = session.make_workspace_client(options.profile)
    state_dir = tmp_path_factory.getbasetemp().parent if hasattr(config, "workerinput") else None

    produced = False

    def produce() -> dict[str, Any]:
        nonlocal produced
        produced = True
        return session.setup(ws, options, config.rootpath)

    lakeflow = Lakeflow.from_state(ws, options, session.shared_state(state_dir, produce))
    config.stash[USED] = True
    if produced and options.sweep:
        lakeflow.sweep()
    yield lakeflow
    lakeflow.cancel_active_runs()


@pytest.fixture
def lakeflow(_lakeflow_session: Lakeflow, request: pytest.FixtureRequest) -> Lakeflow:
    """Get a `Lakeflow` bound to this test, for making throwaway copies of the bundle's jobs."""
    return _lakeflow_session.for_test(request)


@pytest.fixture
def lakeflow_schema(request: pytest.FixtureRequest) -> Iterator[str]:
    """Create a temporary schema `<lakeflow_catalog>.pytest_<run id>_<random>` and drop it after the test.

    Pass it to jobs as a job parameter: bundle variables cannot change per test. The schema is kept with
    `--lakeflow-keep`.

    Yields:
        The schema's full name.

    Raises:
        pytest.UsageError: If `lakeflow_catalog` is not set.
    """
    options = request.config.stash[OPTIONS]
    if not options.catalog:
        raise pytest.UsageError("lakeflow_schema needs the lakeflow_catalog ini option")
    lakeflow: Lakeflow = request.getfixturevalue("_lakeflow_session")
    name = f"pytest_{re.sub(r'[^a-z0-9_]', '_', lakeflow.run_id.lower())}_{secrets.token_hex(4)}"
    full_name = f"{options.catalog}.{name}"
    lakeflow.ws.schemas.create(name=name, catalog_name=options.catalog)
    log.info("Created schema %s", full_name)
    yield full_name
    if options.keep:
        log.warning("Keeping schema %s", full_name)
        return
    lakeflow.ws.schemas.delete(full_name, force=True)
    log.info("Dropped schema %s", full_name)


@pytest.hookimpl(trylast=True)
def pytest_sessionfinish(session: pytest.Session) -> None:
    config = session.config
    if hasattr(config, "workerinput"):
        cast(Any, config).workeroutput["lakeflow_used"] = config.stash.get(USED, False)
        return
    options = config.stash.get(OPTIONS, None)
    if not (options and options.destroy and options.sync == "deploy" and config.stash.get(USED, False)):
        return
    try:
        root = bundle.find_root(config.rootpath) if options.bundle_root is None else options.bundle_root
        bundle.destroy(root, options.target, options.profile)
    except LakeflowBundleError as e:
        reporter = config.pluginmanager.get_plugin("terminalreporter")
        if reporter is not None:
            reporter.write_sep("=", "lakeflow: bundle destroy failed", red=True)
            reporter.write_line(str(e))
        if session.exitstatus == pytest.ExitCode.OK:
            session.exitstatus = pytest.ExitCode.TESTS_FAILED


@pytest.hookimpl(optionalhook=True)
def pytest_testnodedown(node: Any, error: object) -> None:
    if getattr(node, "workeroutput", {}).get("lakeflow_used"):
        node.config.stash[USED] = True
