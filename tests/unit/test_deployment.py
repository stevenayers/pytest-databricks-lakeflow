"""Setting a bundle target up: deploy or validate, the host and run_as checks, templates and artifact warnings."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest
from databricks.sdk.errors import PermissionDenied
from databricks.sdk.service import compute, jobs

from pytest_databricks_lakeflow import LakeflowBundleError, LakeflowError, deployment, workspace_api

from .conftest import (
    ARTIFACT_PATH,
    DEPLOY,
    SUMMARY,
    UPLOADED_WHEEL,
    VALIDATE,
    WHEEL,
    FakeCli,
    as_client,
    bundle_key,
    configure_lakeflow,
)
from .fakes.runs import HOST
from .fakes.templates import build_wheel, example_settings
from .fakes.workspace import ACCOUNT_ID, FakeWorkspaceClient, sp_rule_set

OTHER_HOST = "https://other.cloud.databricks.com"
APP_ID = "0000-app-id"
SP_RUN_AS = {"service_principal_name": APP_ID}
SP_USER = "roles/servicePrincipal.user"
SP_MANAGER = "roles/servicePrincipal.manager"
SP_FIX = "Ask a manager of that service principal to grant you the role."
RUN_AS_FIX = (
    "Deploy as that principal, set run_as to ${workspace.current_user.userName}, or deploy the bundle yourself and "
    'use Lakeflow(sync="none").'
)
WHEEL_ARTIFACT = {"type": "whl", "build": "uv build --wheel", "path": "/b", "files": [{"source": "/b/dist/*.whl"}]}
UNVERSIONED_MESSAGE = (
    "Artifacts ['lakeflow_example'] have no dynamic_version: true. Serverless caches wheels by file name, so a rebuilt "
    "wheel with the same version may not be picked up. Set artifacts.<name>.dynamic_version: true in databricks.yml."
)
STALE_MESSAGE = f"dist/{WHEEL} was built after its uploaded copy. Run `databricks bundle deploy -t test`."


def stale_wheel(ws: FakeWorkspaceClient, cli: FakeCli, root: Path, uploaded_after_build: int = -120) -> None:
    """Build dist/<WHEEL> under `root`, upload it `uploaded_after_build` seconds later, and validate a job using it."""
    (root / "dist").mkdir(exist_ok=True)
    built = 1_700_000_000
    wheel = root / "dist" / WHEEL
    wheel.write_text("")
    os.utime(wheel, (built, built))
    ws.workspace.objects.add(UPLOADED_WHEEL)
    ws.workspace.modified[UPLOADED_WHEEL] = (built + uploaded_after_build) * 1000
    spec = {"environment_version": "4", "dependencies": ["./dist/*.whl"]}
    task = {
        "task_key": "t",
        "environment_key": "default",
        "python_wheel_task": {"package_name": "pkg", "entry_point": "m"},
    }
    job = {"name": "j", "environments": [{"environment_key": "default", "spec": spec}], "tasks": [task]}
    cli.validate = {"workspace": {"artifact_path": ARTIFACT_PATH}, "resources": {"jobs": {"my_job": job}}}


class TestSetup:
    """What setting up a bundle target runs, and the state it produces for each sync strategy."""

    def test_templates_from_summary(self) -> None:
        ws = FakeWorkspaceClient()
        job_id = ws.jobs.add(example_settings())
        summary = {"resources": {"jobs": {"my_job": {"id": str(job_id)}, "gone": {"id": "1"}, "new": {}}}}
        templates, errors = deployment.templates_from_summary(as_client(ws), summary)
        assert templates == {"my_job": example_settings().as_dict()}
        assert "stale" in errors["gone"]
        assert "not deployed" in errors["new"]

    def test_setup_with_deploy(self, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path) -> None:
        state = deployment.setup(as_client(fake_ws), bundle_key(bundle_dir, profile="dev"))
        assert fake_cli.invocations == [
            (["--version"], bundle_dir, None),
            (VALIDATE, bundle_dir, "dev"),
            (DEPLOY, bundle_dir, "dev"),
            (SUMMARY, bundle_dir, "dev"),
        ]
        folder = "/Users/someone@example.com/.pytest-databricks-lakeflow"
        assert state == {
            "templates": {"my_job": example_settings().as_dict()},
            "template_errors": {},
            "helpers": {"set_values": f"{folder}/set_values", "get_values": f"{folder}/get_values"},
            "user": "someone@example.com",
            "job_ids": {"my_job": fake_cli.deployed["my_job"]},
            "pipeline_ids": {},
            "pipeline_errors": {},
            "unversioned_artifacts": [],
            "artifact_path": "",
            "root": str(bundle_dir),
            "stale_uploads": [],
        }
        assert set(fake_ws.workspace.uploads) == {f"{folder}/set_values", f"{folder}/get_values"}
        assert all(upload["overwrite"] for upload in fake_ws.workspace.uploads.values())
        assert json.loads(json.dumps(state)) == state

    def test_setup_with_none_uses_validate(
        self, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        fake_cli.validate = {"resources": {"jobs": {"other": {"name": "o", "tasks": [{"task_key": "t"}]}}}}
        key = bundle_key(bundle_dir, sync="none", helpers_path="/Shared/helpers/")
        state = deployment.setup(as_client(fake_ws), key)
        assert [call[:2] for call in fake_cli.calls] == [["--version"], ["bundle", "validate"]]
        assert state["templates"] == {"other": {"name": "o", "tasks": [{"task_key": "t"}]}}
        assert state["helpers"]["get_values"] == "/Shared/helpers/get_values"
        assert (state["job_ids"], state["pipeline_ids"], state["pipeline_errors"]) == ({}, {}, {})

    @pytest.mark.parametrize(("sync", "stale"), [("none", [f"dist/{WHEEL}"]), ("deploy", [])])
    def test_setup_records_artifact_path_and_stale_uploads(
        self, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path, sync: str, stale: list[str]
    ) -> None:
        stale_wheel(fake_ws, fake_cli, bundle_dir)
        fake_cli.summary = {"workspace": {"artifact_path": ARTIFACT_PATH}}
        state = deployment.setup(as_client(fake_ws), bundle_key(bundle_dir, sync=sync))
        assert state["artifact_path"] == ARTIFACT_PATH
        assert state["root"] == str(bundle_dir)
        assert state["stale_uploads"] == stale

    def test_setup_with_sync_syncs_first(
        self, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        deployment.setup(as_client(fake_ws), bundle_key(bundle_dir, sync="sync"))
        assert [call[:2] for call in fake_cli.calls] == [["--version"], ["bundle", "sync"], ["bundle", "validate"]]

    @pytest.mark.parametrize(("sync", "commands"), [("deploy", 3), ("sync", 2), ("none", 1)])
    def test_setup_passes_variables_to_every_cli_call(
        self, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path, sync: str, commands: int
    ) -> None:
        deployment.setup(as_client(fake_ws), bundle_key(bundle_dir, sync=sync, variables=(("catalog", "dev"),)))
        bundle_calls = [call for call in fake_cli.calls if call[0] == "bundle"]
        assert len(bundle_calls) == commands
        assert all(call[-2:] == ["--var", "catalog=dev"] for call in bundle_calls)

    @pytest.mark.parametrize(
        ("variables", "as_vars"), [((), []), ((("catalog", "dev"),), ["--var", "catalog=dev"])], ids=["plain", "vars"]
    )
    def test_setup_passes_deploy_flags_after_variables(
        self,
        fake_ws: FakeWorkspaceClient,
        fake_cli: FakeCli,
        bundle_dir: Path,
        variables: tuple[tuple[str, str], ...],
        as_vars: list[str],
    ) -> None:
        key = bundle_key(bundle_dir, variables=variables, deploy_flags=("--fail-on-active-runs",))
        deployment.setup(as_client(fake_ws), key)
        assert fake_cli.calls[2] == [*DEPLOY, *as_vars, "--fail-on-active-runs"]
        assert fake_cli.calls[3] == [*SUMMARY, *as_vars]

    def test_template_copies_are_independent(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        fake_cli.deployed["broken"] = 1
        pytester.makepyfile(
            r"""
            import pytest

            from pytest_databricks_lakeflow import LakeflowBundleError

            def test_copies(lakeflow):
                first = lakeflow.template("my_job")
                first.tasks = []
                first.tags["x"] = "y"
                assert lakeflow.template("my_job").tasks
                assert "x" not in lakeflow.template("my_job").tags
                with pytest.raises(KeyError, match=r"No job 'nope' in the bundle. Available: \['my_job'\]"):
                    lakeflow.template("nope")
                with pytest.raises(LakeflowBundleError, match="stale"):
                    lakeflow.template("broken")
            """
        )
        pytester.runpytest().assert_outcomes(passed=1)


class TestCheck:
    """`Lakeflow.check` lints a template from any scope and creates nothing in the workspace."""

    def test_check_from_a_session_scoped_fixture(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        fake_cli.deployed["broken"] = 1
        configure_lakeflow(
            pytester,
            "",
            '\n\n@pytest.fixture(scope="session")\ndef problems(lakeflow):\n    return lakeflow.check("my_job")\n',
        )
        pytester.makepyfile(
            r"""
            import pytest

            from pytest_databricks_lakeflow import LakeflowBundleError

            def test_check(lakeflow, problems):
                assert problems == []
                with pytest.raises(KeyError, match=r"No job 'nope' in the bundle. Available: \['my_job'\]"):
                    lakeflow.check("nope")
                with pytest.raises(LakeflowBundleError, match="stale"):
                    lakeflow.check("broken")
            """
        )
        pytester.runpytest().assert_outcomes(passed=1)
        assert fake_ws.jobs.created == []
        assert fake_ws.jobs.run_now_calls == []

    def test_check_outside_any_test(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        # The fixture above runs during the first test's setup; pytest_sessionstart runs before any test item exists.
        pytester.makeconftest(
            """
            from pytest_databricks_lakeflow import Lakeflow

            def pytest_sessionstart(session):
                print("problems:", Lakeflow().check("my_job"))
            """
        )
        pytester.makepyfile("def test_nothing():\n    pass\n")
        result = pytester.runpytest("-s")
        result.assert_outcomes(passed=1)
        result.stdout.fnmatch_lines(["problems: []"])
        assert fake_ws.jobs.created == []

    def test_check_with_sync_none_only_validates(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        settings = example_settings()
        settings.tasks = [jobs.Task(task_key="t", depends_on=[jobs.TaskDependency(task_key="missing")])]
        fake_cli.validate = {"resources": {"jobs": {"my_job": settings.as_dict()}}}
        configure_lakeflow(pytester, 'sync="none"')
        pytester.makepyfile(
            """
            def test_check(lakeflow):
                assert lakeflow.check("my_job") == [
                    "Task 't': depends on 'missing', which is not a task in the job. Tasks: ['t']"
                ]
            """
        )
        pytester.runpytest().assert_outcomes(passed=1)
        assert [args for args in fake_cli.calls if args[0] == "bundle"] == [VALIDATE]
        assert fake_ws.jobs.created == []
        assert fake_ws.jobs.run_now_calls == []

    def test_check_reads_the_built_wheels_and_the_workspace(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        settings = example_settings()
        load = next(task for task in settings.tasks or [] if task.task_key == "load")
        load.libraries = [compute.Library(whl="/Workspace/libs/gone.whl"), compute.Library(whl="/Workspace/libs/a.whl")]
        fake_ws.workspace.objects.add("/Workspace/libs/a.whl")
        fake_cli.deployed["my_job"] = fake_ws.jobs.add(settings)
        build_wheel(bundle_dir / "dist")
        pytester.makepyfile(
            """
            from pytest_databricks_lakeflow import DEFAULT_RULES

            def test_check(lakeflow):
                assert lakeflow.check("my_job") == [
                    "Task 'load': entry point 'load' is not in dist/pkg-0.1.0-py3-none-any.whl. Entry points: "
                    "['main'], and pkg/__init__.py does not define it.",
                    "Task 'load': /Workspace/libs/gone.whl does not exist.",
                ]
                assert lakeflow.check("my_job", rules=DEFAULT_RULES - {"wheel", "library"}) == []
            """
        )
        pytester.runpytest().assert_outcomes(passed=1)
        assert fake_ws.jobs.created == []
        assert fake_ws.jobs.run_now_calls == []


class TestHostCheck:
    """Setup refuses a bundle target that deploys to a different workspace from the client's."""

    @pytest.mark.parametrize(
        ("sync", "bundle_host"),
        [
            ("deploy", OTHER_HOST),
            ("none", OTHER_HOST),
            ("none", OTHER_HOST.removeprefix("https://")),
        ],
        ids=["deploy", "none", "none-schemeless"],
    )
    def test_setup_fails_on_host_mismatch(
        self, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path, sync: str, bundle_host: str
    ) -> None:
        fake_cli.summary = {"workspace": {"host": bundle_host}}
        fake_cli.validate = {"workspace": {"host": bundle_host}}
        with pytest.raises(LakeflowError) as excinfo:
            deployment.setup(as_client(fake_ws), bundle_key(bundle_dir, sync=sync))
        message = str(excinfo.value)
        assert f"'test' deploys to {bundle_host} but the workspace client is for {HOST}" in message
        assert "Pass profile= to Lakeflow" in message
        assert fake_ws.jobs.gets == []
        assert fake_ws.workspace.uploads == {}

    def test_deploy_checks_the_validated_host_before_run_as_and_the_deploy(
        self, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        # A run_as that the run_as check refuses, so the host check must come first to be the error raised.
        fake_cli.validate = {"workspace": {"host": OTHER_HOST}, "run_as": {"user_name": "other@example.com"}}
        with pytest.raises(LakeflowError) as excinfo:
            deployment.setup(as_client(fake_ws), bundle_key(bundle_dir))
        assert f"'test' deploys to {OTHER_HOST} but the workspace client is for {HOST}" in str(excinfo.value)
        assert [call[:2] for call in fake_cli.calls] == [["--version"], ["bundle", "validate"]]
        assert fake_ws.current_user.me_calls == 0

    @pytest.mark.parametrize(
        ("bundle_host", "client_host"),
        [
            (f"{HOST}/", HOST),
            (HOST.removeprefix("https://"), HOST),
            (HOST.upper(), HOST),
            ("http://example.cloud.databricks.com", HOST),
            (None, HOST),
            (OTHER_HOST, None),
        ],
        ids=["trailing-slash", "no-scheme", "upper-case", "http", "no-bundle-host", "no-client-host"],
    )
    def test_check_host_accepts(self, bundle_host: str | None, client_host: str | None) -> None:
        deployment.check_host("test", bundle_host, client_host)

    @pytest.mark.parametrize("sync", ["deploy", "none"])
    def test_setup_skips_the_host_check_for_a_null_workspace(
        self, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path, sync: str
    ) -> None:
        fake_cli.summary = {**fake_cli.summary, "workspace": None}
        fake_cli.validate = {**fake_cli.validate, "workspace": None}
        deployment.setup(as_client(fake_ws), bundle_key(bundle_dir, sync=sync))

    def test_check_host_names_both_hosts_and_the_target(self) -> None:
        with pytest.raises(LakeflowError) as excinfo:
            deployment.check_host("ci", "https://adb-1.azuredatabricks.net", "https://adb-2.azuredatabricks.net")
        assert str(excinfo.value) == (
            "Bundle target 'ci' deploys to https://adb-1.azuredatabricks.net but the workspace client is for "
            "https://adb-2.azuredatabricks.net. Pass profile= to Lakeflow, or set DATABRICKS_HOST and "
            "DATABRICKS_CONFIG_PROFILE to match the target."
        )

    def test_host_mismatch_fails_later_tests_too(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        fake_cli.summary = {"workspace": {"host": OTHER_HOST}}
        test = "def test_{0}(lakeflow):\n    lakeflow.template('my_job')\n"
        pytester.makepyfile(test.format("a") + "\n" + test.format("b"))
        result = pytester.runpytest()
        result.assert_outcomes(failed=2)
        result.stdout.fnmatch_lines(
            ["*LakeflowError: Bundle target 'test' deploys to*", "*failed earlier in this session: LakeflowError*"]
        )
        assert fake_cli.calls.count(DEPLOY) == 1
        assert fake_ws.jobs.gets == []


class TestRunAsCheck:
    """Before `bundle deploy`, setup checks that the deploying identity can deploy the target's and jobs' run_as."""

    @pytest.mark.parametrize(
        ("validate", "message"),
        [
            pytest.param(
                {"run_as": SP_RUN_AS, "resources": {"jobs": {"my_job": {"run_as": SP_RUN_AS}}}},
                f"Target 'test' sets run_as {APP_ID}, but this session deploys as someone@example.com, which does "
                f"not have the Service Principal User role on it. {SP_FIX} {RUN_AS_FIX}",
                id="target-service-principal",
            ),
            pytest.param(
                {"run_as": {"user_name": "other@example.com"}},
                f"Target 'test' sets run_as other@example.com, but this session deploys as someone@example.com. "
                f"{RUN_AS_FIX}",
                id="target-user",
            ),
            pytest.param(
                {"resources": {"jobs": {"mine": {}, "other": {"run_as": {"user_name": "other@example.com"}}}}},
                f"Job 'other' of target 'test' sets run_as other@example.com, but this session deploys as "
                f"someone@example.com. {RUN_AS_FIX}",
                id="job-user",
            ),
        ],
    )
    def test_setup_refuses_a_foreign_run_as(
        self,
        fake_ws: FakeWorkspaceClient,
        fake_cli: FakeCli,
        bundle_dir: Path,
        validate: dict[str, Any],
        message: str,
    ) -> None:
        fake_ws.account_access_control_proxy.grant(APP_ID, SP_MANAGER, "users/someone@example.com")
        fake_ws.account_access_control_proxy.grant(APP_ID, SP_USER, "users/other@example.com", "groups/other")
        fake_cli.validate = validate
        with pytest.raises(LakeflowBundleError) as excinfo:
            deployment.setup(as_client(fake_ws), bundle_key(bundle_dir))
        assert str(excinfo.value) == message
        assert DEPLOY not in fake_cli.calls

    @pytest.mark.parametrize(
        "principal",
        ["users/someone@example.com", "users/Someone@Example.com", "groups/data-eng"],
        ids=["user", "user-other-case", "group-of-the-user"],
    )
    def test_setup_accepts_run_as_with_the_user_role(
        self, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path, principal: str
    ) -> None:
        fake_ws.current_user.groups = ["data-eng"]
        fake_ws.account_access_control_proxy.grant(APP_ID, SP_USER, principal)
        fake_cli.validate = {"run_as": SP_RUN_AS, "resources": {"jobs": {"my_job": {"run_as": SP_RUN_AS}}}}
        deployment.setup(as_client(fake_ws), bundle_key(bundle_dir))
        assert [call[:2] for call in fake_cli.calls] == [
            ["--version"],
            ["bundle", "validate"],
            ["bundle", "deploy"],
            ["bundle", "summary"],
        ]
        assert fake_ws.account_access_control_proxy.names == [sp_rule_set(APP_ID)]

    @pytest.mark.parametrize(
        "run_as",
        [{"user_name": "someone@example.com"}, {"user_name": "Someone@Example.com"}, None],
        ids=["self", "self-other-case", "none"],
    )
    def test_setup_accepts_run_as_self(
        self, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path, run_as: dict[str, str] | None
    ) -> None:
        fake_cli.validate = {"run_as": run_as, "resources": {"jobs": {"my_job": example_settings().as_dict()}}}
        deployment.setup(as_client(fake_ws), bundle_key(bundle_dir))
        assert DEPLOY in fake_cli.calls
        assert fake_ws.account_access_control_proxy.names == []

    @pytest.mark.parametrize(
        ("account_id", "rule_set"),
        [
            pytest.param(None, None, id="no-account-id"),
            pytest.param(ACCOUNT_ID, PermissionDenied("not an account member"), id="rule-set-denied"),
            pytest.param(ACCOUNT_ID, None, id="unknown-service-principal"),
        ],
    )
    def test_role_check_is_skipped_without_an_account_id(
        self,
        fake_ws: FakeWorkspaceClient,
        fake_cli: FakeCli,
        bundle_dir: Path,
        account_id: str | None,
        rule_set: Exception | None,
    ) -> None:
        fake_ws.config.account_id = account_id
        if rule_set is not None:
            fake_ws.account_access_control_proxy.rule_sets[sp_rule_set(APP_ID)] = rule_set
        fake_cli.validate = {"run_as": SP_RUN_AS}
        deployment.setup(as_client(fake_ws), bundle_key(bundle_dir))
        assert DEPLOY in fake_cli.calls

    @pytest.mark.parametrize("sync", ["sync", "none"])
    def test_sync_and_none_never_run_the_check(
        self, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path, sync: str
    ) -> None:
        fake_cli.validate = {"run_as": {"user_name": "other@example.com"}}
        deployment.setup(as_client(fake_ws), bundle_key(bundle_dir, sync=sync))
        assert fake_ws.account_access_control_proxy.names == []

    def test_setup_accepts_a_service_principal_deploying_as_itself(
        self, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        fake_ws.current_user.user_name = APP_ID
        fake_cli.validate = {"run_as": SP_RUN_AS}
        deployment.setup(as_client(fake_ws), bundle_key(bundle_dir))
        assert DEPLOY in fake_cli.calls
        assert fake_ws.account_access_control_proxy.names == []


class TestUnversionedArtifacts:
    """Wheel artifacts without `dynamic_version` warn once per session, or fail with `strict_artifacts=True`."""

    @pytest.mark.parametrize("sync", ["deploy", "none"])
    def test_setup_records_unversioned_artifacts(
        self, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path, sync: str
    ) -> None:
        artifacts = {"artifacts": {"pkg": WHEEL_ARTIFACT, "versioned": {**WHEEL_ARTIFACT, "dynamic_version": True}}}
        if sync == "deploy":
            fake_cli.summary = artifacts
        else:
            fake_cli.validate = artifacts
        state = deployment.setup(as_client(fake_ws), bundle_key(bundle_dir, sync=sync))
        assert state["unversioned_artifacts"] == ["pkg"]

    @pytest.mark.parametrize(("dynamic_version", "warnings"), [(False, 1), (True, 0)])
    def test_unversioned_wheel_warns_once(
        self,
        pytester: pytest.Pytester,
        fake_ws: FakeWorkspaceClient,
        fake_cli: FakeCli,
        bundle_dir: Path,
        dynamic_version: bool,
        warnings: int,
    ) -> None:
        fake_cli.summary = {"artifacts": {"lakeflow_example": {**WHEEL_ARTIFACT, "dynamic_version": dynamic_version}}}
        pytester.makepyfile(
            """
            import dataclasses

            def test_one(lakeflow):
                lakeflow.template("my_job")
                lakeflow.template("my_job")

            def test_two(lakeflow):
                dataclasses.replace(lakeflow, catalog="other").template("my_job")
            """
        )
        result = pytester.runpytest()
        result.assert_outcomes(passed=2, warnings=warnings)
        assert result.stdout.str().count(f"UserWarning: {UNVERSIONED_MESSAGE}") == warnings

    def test_strict_artifacts_fails_the_first_template_lookup(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        fake_cli.summary = {"artifacts": {"lakeflow_example": WHEEL_ARTIFACT}}
        configure_lakeflow(pytester, "strict_artifacts=True")
        pytester.makepyfile(
            """
            def test_one(lakeflow):
                lakeflow.template("my_job")

            def test_two(lakeflow):
                lakeflow.job("my_job")
            """
        )
        result = pytester.runpytest("-rN")
        result.assert_outcomes(failed=2, warnings=0)
        assert result.stdout.str().count(f"LakeflowError: {UNVERSIONED_MESSAGE}") == 2
        assert fake_cli.calls.count(DEPLOY) == 1
        assert fake_ws.jobs.created == []


class TestStaleUploads:
    """Under `sync="none"`, a local wheel built after its uploaded copy warns once, or fails with `strict_artifacts`."""

    @pytest.mark.parametrize(("uploaded_after_build", "warnings"), [(-120, 1), (5, 0)], ids=["stale", "fresh"])
    def test_stale_upload_warns_once(
        self,
        pytester: pytest.Pytester,
        fake_ws: FakeWorkspaceClient,
        fake_cli: FakeCli,
        bundle_dir: Path,
        uploaded_after_build: int,
        warnings: int,
    ) -> None:
        stale_wheel(fake_ws, fake_cli, bundle_dir, uploaded_after_build)
        configure_lakeflow(pytester, 'sync="none"')
        pytester.makepyfile(
            """
            import dataclasses

            def test_one(lakeflow):
                lakeflow.template("my_job")
                lakeflow.template("my_job")

            def test_two(lakeflow):
                dataclasses.replace(lakeflow, catalog="other").template("my_job")
            """
        )
        result = pytester.runpytest()
        result.assert_outcomes(passed=2, warnings=warnings)
        assert result.stdout.str().count(f"UserWarning: {STALE_MESSAGE}") == warnings

    def test_strict_artifacts_fails_on_a_stale_upload(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        stale_wheel(fake_ws, fake_cli, bundle_dir)
        configure_lakeflow(pytester, 'sync="none", strict_artifacts=True')
        pytester.makepyfile('def test_one(lakeflow):\n    lakeflow.job("my_job")\n')
        result = pytester.runpytest("-rN")
        result.assert_outcomes(failed=1, warnings=0)
        assert f"LakeflowError: {STALE_MESSAGE}" in result.stdout.str()
        assert fake_ws.jobs.created == []

    def test_several_stale_wheels_share_one_warning(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        stale_wheel(fake_ws, fake_cli, bundle_dir)
        other = "other-0.2-py3-none-any.whl"
        (bundle_dir / "dist" / other).write_text("")
        fake_ws.workspace.objects.add(f"{ARTIFACT_PATH}/.internal/{other}")
        fake_ws.workspace.modified[f"{ARTIFACT_PATH}/.internal/{other}"] = 0
        configure_lakeflow(pytester, 'sync="none"')
        pytester.makepyfile('def test_one(lakeflow):\n    lakeflow.template("my_job")\n')
        result = pytester.runpytest()
        result.assert_outcomes(passed=1, warnings=1)
        result.stdout.fnmatch_lines(
            [
                f"*UserWarning: dist/{other}, dist/{WHEEL} were built after their uploaded copies. "
                "Run `databricks bundle deploy -t test`."
            ]
        )

    @pytest.mark.parametrize("folder", ["/Workspace/b/artifacts/.internal", "/Volumes/c/s/v"])
    def test_modified_at_reads_workspace_and_volumes_paths(self, fake_ws: FakeWorkspaceClient, folder: str) -> None:
        path = f"{folder}/{WHEEL}"
        fake_ws.workspace.objects.add(path)
        fake_ws.workspace.modified[path] = 1_700_000_000_000
        assert workspace_api.modified_at(as_client(fake_ws), path) == 1_700_000_000_000
        assert workspace_api.modified_at(as_client(fake_ws), f"{folder}/missing.whl") is None
