from __future__ import annotations

from datetime import timedelta
from typing import Any, cast

import pytest

from pytest_databricks_lakeflow import Lakeflow

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


def test_help_lists_only_the_run_flags(pytester: pytest.Pytester) -> None:
    result = pytester.runpytest("--help")
    result.stdout.fnmatch_lines(
        [
            "Lakeflow Jobs from a Databricks Asset Bundle:",
            "*--lakeflow-keep *",
            "*--lakeflow-keep-failed*",
            "*--no-lakeflow-keep-failed*",
            "*--lakeflow-sweep*",
            "*--lakeflow-destroy*",
        ]
    )
    assert "--lakeflow-target" not in result.stdout.str()
    assert "lakeflow_target" not in result.stdout.str()


def test_defaults() -> None:
    lakeflow = Lakeflow()
    assert (lakeflow.bundle_root, lakeflow.target, lakeflow.profile, lakeflow.workspace_client) == (
        None,
        "test",
        None,
        None,
    )
    assert (lakeflow.sync, lakeflow.timeout, lakeflow.catalog) == ("deploy", timedelta(minutes=30), None)
    assert (lakeflow.helpers_path, lakeflow.helpers_compute, lakeflow.helpers_environment_version) == (None, None, "4")
    assert lakeflow.disable_auto_optimization
    assert not lakeflow.performance_optimized
    assert not lakeflow.allow_undeclared_params
    assert lakeflow.sweep_hours == 24
    assert (lakeflow.keep, lakeflow.keep_failed) == (False, True)
    assert (lakeflow.modify_settings, lakeflow.run_finished) == (None, None)


def test_arguments_are_keyword_only() -> None:
    with pytest.raises(TypeError):
        cast(Any, Lakeflow)("examples/bundle")


def test_rejects_an_unknown_sync_strategy() -> None:
    with pytest.raises(ValueError, match="sync must be one of deploy, sync, none, got 'upload'"):
        Lakeflow(sync=cast(Any, "upload"))
