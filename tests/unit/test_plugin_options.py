from __future__ import annotations

from datetime import timedelta

import pytest

from pytest_databricks_lakeflow.options import Options

TESTS = """
import pytest

@pytest.fixture
def lakeflow():
    return None

def test_uses_lakeflow(lakeflow):
    pass

def test_plain():
    pass
"""


def test_marker_is_applied_automatically(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(TESTS)
    result = pytester.runpytest("-m", "lakeflow", "--collect-only", "-q")
    result.stdout.fnmatch_lines(["*test_uses_lakeflow*", "*1/2 tests collected (1 deselected)*"])


def test_not_lakeflow_deselects(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(TESTS)
    result = pytester.runpytest("-m", "not lakeflow")
    result.assert_outcomes(passed=1, deselected=1)


def test_marker_is_registered(pytester: pytest.Pytester) -> None:
    result = pytester.runpytest("--markers")
    result.stdout.fnmatch_lines(["@pytest.mark.lakeflow: runs Lakeflow Jobs*"])


def test_help_lists_options(pytester: pytest.Pytester) -> None:
    result = pytester.runpytest("--help")
    result.stdout.fnmatch_lines(
        ["Lakeflow Jobs from a Databricks Asset Bundle:", "*--lakeflow-target=*", "*--no-lakeflow-keep-failed*"]
    )


def test_defaults(pytester: pytest.Pytester) -> None:
    config = pytester.parseconfig()
    options = Options.from_config(config)
    assert options == Options(
        bundle_root=None,
        target="test",
        profile=None,
        sync="deploy",
        timeout=timedelta(minutes=30),
        catalog=None,
        helpers_path=None,
        helpers_compute=None,
        helpers_environment_version="4",
        sweep_hours=24.0,
        disable_auto_optimization=True,
        performance_optimized=False,
        allow_undeclared_params=False,
    )


def test_cli_overrides_ini(pytester: pytest.Pytester) -> None:
    pytester.makeini(
        """
        [pytest]
        lakeflow_bundle_root = bundle
        lakeflow_target = ci
        lakeflow_sync = none
        lakeflow_timeout_minutes = 20
        lakeflow_catalog = dev_sandbox
        lakeflow_performance_optimized = true
        """
    )
    config = pytester.parseconfig(
        "--lakeflow-target=other", "--lakeflow-sync=sync", "--lakeflow-timeout=5", "--no-lakeflow-keep-failed"
    )
    options = Options.from_config(config)
    assert options.bundle_root == pytester.path / "bundle"
    assert (options.target, options.sync, options.timeout) == ("other", "sync", timedelta(minutes=5))
    assert options.catalog == "dev_sandbox"
    assert options.performance_optimized
    assert not options.keep_failed


def test_rejects_bad_sync_strategy(pytester: pytest.Pytester) -> None:
    pytester.makeini("[pytest]\nlakeflow_sync = upload\n")
    result = pytester.runpytest()
    result.stderr.fnmatch_lines(["*lakeflow_sync must be one of deploy, sync, none, got 'upload'*"])
