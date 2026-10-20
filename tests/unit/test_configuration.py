from __future__ import annotations

from datetime import timedelta
from typing import Any, cast

import pytest
from databricks.sdk.service import jobs

from pytest_databricks_lakeflow import Lakeflow

from .conftest import configure_lakeflow
from .fakes import FakeWorkspaceClient, make_run

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

VIEW_REQUEST = jobs.JobAccessControlRequest(group_name="data-eng", permission_level=jobs.JobPermissionLevel.CAN_VIEW)
MANAGE_RUN_REQUEST = jobs.JobAccessControlRequest(
    service_principal_name="0000-app-id", permission_level=jobs.JobPermissionLevel.CAN_MANAGE_RUN
)


class TestMarker:
    """The `lakeflow` marker is registered and applied to every test that uses the `lakeflow` fixture."""

    @pytest.mark.parametrize(
        ("expression", "selected"), [("lakeflow", "test_uses_lakeflow"), ("not lakeflow", "test_plain")]
    )
    def test_marker_is_applied_automatically(self, pytester: pytest.Pytester, expression: str, selected: str) -> None:
        pytester.makepyfile(TESTS)
        result = pytester.runpytest("-m", expression, "-v")
        result.assert_outcomes(passed=1, deselected=1)
        result.stdout.fnmatch_lines([f"*::{selected} PASSED*"])

    def test_marker_is_registered(self, pytester: pytest.Pytester) -> None:
        result = pytester.runpytest("--markers")
        result.stdout.fnmatch_lines(["@pytest.mark.lakeflow: runs Lakeflow Jobs*"])


class TestCommandLine:
    """The plugin adds its run-time flags to `pytest --help`, and nothing that belongs on `Lakeflow`."""

    def test_help_lists_only_the_run_flags(self, pytester: pytest.Pytester) -> None:
        result = pytester.runpytest("--help")
        result.stdout.fnmatch_lines(
            [
                "Lakeflow Jobs from a Databricks Asset Bundle:",
                "*--lakeflow-keep *",
                "*--lakeflow-keep-failed*",
                "*--no-lakeflow-keep-failed*",
                "*--lakeflow-keep-ttl=*",
                "*--lakeflow-sweep *",
                "*--lakeflow-sweep-scope=*",
                "*--lakeflow-sweep-schemas=*",
                "*--lakeflow-sweep-notebooks*",
                "*--lakeflow-sweep-branch=*",
                "*--lakeflow-sweep-dry-run*",
                "*--lakeflow-sweep-report=*",
                "*--lakeflow-destroy*",
                "*--lakeflow-report-dir*",
                "*--lakeflow-debug-notebook*",
                "*--lakeflow-live*",
                "*--lakeflow-max-concurrent-runs=N*",
                "*--lakeflow-check*",
            ]
        )
        assert "The cap is per xdist worker." in " ".join(result.stdout.str().split())
        assert "--lakeflow-target" not in result.stdout.str()
        assert "lakeflow_target" not in result.stdout.str()


class TestLakeflowArguments:
    """`Lakeflow` is keyword-only, has the documented defaults and checks its choices on construction."""

    def test_defaults(self) -> None:
        lakeflow = Lakeflow()
        assert (lakeflow.bundle_root, lakeflow.target, lakeflow.profile, lakeflow.workspace_client) == (
            None,
            "test",
            None,
            None,
        )
        assert (lakeflow.sync, lakeflow.timeout, lakeflow.catalog) == ("deploy", timedelta(minutes=30), None)
        assert (lakeflow.helpers_path, lakeflow.helpers_compute, lakeflow.helpers_environment_version) == (
            None,
            None,
            "4",
        )
        assert lakeflow.disable_auto_optimization
        assert not lakeflow.performance_optimized
        assert not lakeflow.single_node
        assert (lakeflow.environment_version, lakeflow.budget_policy_id, lakeflow.existing_cluster_id) == (
            None,
            None,
            None,
        )
        assert not lakeflow.allow_undeclared_params
        assert not lakeflow.test_marker
        assert lakeflow.sweep_hours == 24
        assert lakeflow.side_effects == "neutralise"
        assert lakeflow.notebook_export == "failed"
        assert (lakeflow.keep, lakeflow.keep_failed) == (False, True)
        assert (lakeflow.modify_settings, lakeflow.run_finished) == (None, None)
        assert lakeflow.permissions is None
        assert lakeflow.access_control_list() is None
        assert lakeflow.variables is None
        assert lakeflow.deploy_flags == ()

    def test_arguments_are_keyword_only(self) -> None:
        with pytest.raises(TypeError):
            cast(Any, Lakeflow)("examples/bundle")

    @pytest.mark.parametrize(
        ("entry", "expected"),
        [
            pytest.param({"group_name": "data-eng", "permission_level": "CAN_VIEW"}, VIEW_REQUEST, id="dict"),
            pytest.param(MANAGE_RUN_REQUEST, MANAGE_RUN_REQUEST, id="sdk_object"),
        ],
    )
    def test_permissions_become_access_control_requests(
        self, entry: dict[str, str] | jobs.JobAccessControlRequest, expected: jobs.JobAccessControlRequest
    ) -> None:
        assert Lakeflow(permissions=[entry]).access_control_list() == [expected]

    @pytest.mark.parametrize(
        "entry",
        [
            pytest.param({"user_name": "x", "permission_level": "IS_OWNER"}, id="dict"),
            pytest.param(
                jobs.JobAccessControlRequest(user_name="x", permission_level=jobs.JobPermissionLevel.IS_OWNER),
                id="sdk_object",
            ),
        ],
    )
    def test_is_owner_is_rejected_at_construction(self, entry: dict[str, str] | jobs.JobAccessControlRequest) -> None:
        with pytest.raises(ValueError, match=r"IS_OWNER.*CAN_MANAGE"):
            Lakeflow(permissions=[{"group_name": "data-eng", "permission_level": "CAN_VIEW"}, entry])

    @pytest.mark.parametrize(
        ("arguments", "message"),
        [
            ({"sync": "upload"}, "sync must be one of deploy, sync, none, got 'upload'"),
            ({"side_effects": "other"}, "side_effects must be one of neutralise, keep, got 'other'"),
            ({"notebook_export": "bogus"}, "notebook_export must be one of never, failed, always, got 'bogus'"),
        ],
    )
    def test_rejects_an_unknown_choice(self, arguments: dict[str, Any], message: str) -> None:
        with pytest.raises(ValueError, match=message):
            Lakeflow(**arguments)

    @pytest.mark.parametrize("poll_interval", [timedelta(0), timedelta(seconds=-1)], ids=["zero", "negative"])
    def test_rejects_a_poll_interval_that_is_not_positive(self, poll_interval: timedelta) -> None:
        with pytest.raises(ValueError, match=r"poll_interval must be longer than zero, got datetime\.timedelta"):
            Lakeflow(poll_interval=poll_interval)


class TestDeployFlags:
    """`deploy_flags` takes extra `bundle deploy` flags, but not the ones the plugin sets itself."""

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
    def test_rejects_plugin_owned_flags(self, flags: list[str], name: str, argument: str) -> None:
        with pytest.raises(ValueError, match=f"deploy_flags cannot contain {name}\\..*{argument}"):
            Lakeflow(deploy_flags=flags)

    @pytest.mark.parametrize(
        ("flags", "message"),
        [
            pytest.param("--fail-on-active-runs", "deploy_flags must be a list of flags, not a string", id="string"),
            pytest.param([1], "deploy_flags must contain only strings, got 1", id="int"),
        ],
    )
    def test_must_be_a_list_of_strings(self, flags: Any, message: str) -> None:
        with pytest.raises(ValueError, match=message):
            Lakeflow(deploy_flags=flags)

    def test_accepts_other_flags(self) -> None:
        assert Lakeflow(deploy_flags=["--fail-on-active-runs"]).deploy_flags == ["--fail-on-active-runs"]


class TestRunCapFlag:
    """`--lakeflow-max-concurrent-runs` and `max_concurrent_runs` take 1 or more, and the flag wins."""

    @pytest.mark.parametrize("cap", [0, -1])
    def test_argument_below_one_is_rejected(self, cap: int) -> None:
        with pytest.raises(ValueError, match=f"^max_concurrent_runs must be 1 or more, got {cap}$"):
            Lakeflow(max_concurrent_runs=cap)

    def test_flag_below_one_is_rejected(self, pytester: pytest.Pytester) -> None:
        pytester.makepyfile(TESTS)
        result = pytester.runpytest("--lakeflow-max-concurrent-runs=0")
        assert result.ret == pytest.ExitCode.USAGE_ERROR
        result.stderr.fnmatch_lines(["*--lakeflow-max-concurrent-runs must be 1 or more, got 0*"])

    @pytest.mark.usefixtures("fake_cli", "bundle_dir")
    def test_flag_wins_over_the_argument(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        for run_id in (77, 78):
            fake_ws.jobs.queue(make_run(run_id, {"t": "SUCCESS"}), polls=100)
        configure_lakeflow(pytester, "max_concurrent_runs=5")
        pytester.makepyfile(
            """
            from datetime import timedelta

            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            def test_job(lakeflow):
                with lakeflow.job("my_job") as a, lakeflow.job("my_job") as b, lakeflow.job("my_job") as c:
                    a.start()
                    b.start()
                    with pytest.raises(LakeflowError, match="may have 2 runs active at once"):
                        c.start(timeout=timedelta(0))
            """
        )
        pytester.runpytest("--lakeflow-max-concurrent-runs=2").assert_outcomes(passed=1)
        assert len(fake_ws.jobs.run_now_calls) == 2
