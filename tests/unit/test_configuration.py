from __future__ import annotations

from datetime import timedelta
from typing import Any, cast

import pytest
from databricks.sdk.service import jobs

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
    assert not lakeflow.single_node
    assert (lakeflow.environment_version, lakeflow.budget_policy_id, lakeflow.existing_cluster_id) == (None, None, None)
    assert not lakeflow.allow_undeclared_params
    assert lakeflow.sweep_hours == 24
    assert lakeflow.side_effects == "neutralise"
    assert (lakeflow.keep, lakeflow.keep_failed) == (False, True)
    assert (lakeflow.modify_settings, lakeflow.run_finished) == (None, None)
    assert lakeflow.permissions is None
    assert lakeflow.access_control_list() is None
    assert lakeflow.variables is None
    assert lakeflow.deploy_flags == ()


def test_permissions_accept_dicts() -> None:
    lakeflow = Lakeflow(permissions=[{"group_name": "data-eng", "permission_level": "CAN_VIEW"}])
    assert lakeflow.access_control_list() == [
        jobs.JobAccessControlRequest(group_name="data-eng", permission_level=jobs.JobPermissionLevel.CAN_VIEW)
    ]


def test_permissions_accept_sdk_objects() -> None:
    request = jobs.JobAccessControlRequest(
        service_principal_name="0000-app-id", permission_level=jobs.JobPermissionLevel.CAN_MANAGE_RUN
    )
    assert Lakeflow(permissions=[request]).access_control_list() == [request]


def test_arguments_are_keyword_only() -> None:
    with pytest.raises(TypeError):
        cast(Any, Lakeflow)("examples/bundle")


def test_rejects_an_unknown_side_effects_mode() -> None:
    with pytest.raises(ValueError, match="side_effects must be one of neutralise, keep, got 'other'"):
        Lakeflow(side_effects=cast(Any, "other"))


def test_rejects_an_unknown_sync_strategy() -> None:
    with pytest.raises(ValueError, match="sync must be one of deploy, sync, none, got 'upload'"):
        Lakeflow(sync=cast(Any, "upload"))


@pytest.mark.parametrize(
    ("flags", "name", "argument"),
    [
        (["-t", "other"], "-t", "target="),
        (["-tother"], "-t", "target="),
        (["--target", "other"], "--target", "target="),
        (["--target=other"], "--target", "target="),
        (["-e", "other"], "-e", "target="),
        (["-eother"], "-e", "target="),
        (["--environment", "other"], "--environment", "target="),
        (["--environment=other"], "--environment", "target="),
        (["-p", "x"], "-p", "profile="),
        (["-pother"], "-p", "profile="),
        (["--profile", "x"], "--profile", "profile="),
        (["--var", "catalog=dev"], "--var", "variables="),
        (["--auto-approve"], "--auto-approve", "always passes --auto-approve"),
    ],
)
def test_deploy_flags_reject_plugin_owned_flags(flags: list[str], name: str, argument: str) -> None:
    with pytest.raises(ValueError, match=f"deploy_flags cannot contain {name}\\..*{argument}"):
        Lakeflow(deploy_flags=flags)


def test_deploy_flags_reject_a_bare_string() -> None:
    with pytest.raises(ValueError, match="deploy_flags must be a list of flags, not a string"):
        Lakeflow(deploy_flags="--fail-on-active-runs")


def test_deploy_flags_reject_a_flag_that_is_not_a_string() -> None:
    with pytest.raises(ValueError, match="deploy_flags must contain only strings, got 1"):
        Lakeflow(deploy_flags=cast(Any, [1]))


def test_deploy_flags_accept_other_flags() -> None:
    assert Lakeflow(deploy_flags=["--fail-on-active-runs"]).deploy_flags == ["--fail-on-active-runs"]
