"""The `Lakeflow` object: its arguments, `schema()`, `attach()` and the package's public names."""

from __future__ import annotations

import inspect
import json
import re
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from typing import Any, cast

import pytest
from databricks.sdk.service import jobs

from pytest_databricks_lakeflow import EphemeralJob, Lakeflow

from .conftest import USES_SCHEMA, FakeCli, configure_lakeflow, run_tests
from .fakes.runs import HOST, make_run, running_run
from .fakes.templates import example_settings
from .fakes.workspace import FakeWorkspaceClient

VIEW_REQUEST = jobs.JobAccessControlRequest(group_name="data-eng", permission_level=jobs.JobPermissionLevel.CAN_VIEW)
MANAGE_RUN_REQUEST = jobs.JobAccessControlRequest(
    service_principal_name="0000-app-id", permission_level=jobs.JobPermissionLevel.CAN_MANAGE_RUN
)


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
        assert (lakeflow.run_as, lakeflow.queue, lakeflow.disable) == (None, None, None)
        assert (lakeflow.extra_dependencies, lakeflow.setup, lakeflow.teardown, lakeflow.environments) == (
            None,
            None,
            None,
            None,
        )
        assert lakeflow.children == "warn"
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
            ({"children": "other"}, "children must be one of warn, refuse, got 'other'"),
        ],
    )
    def test_rejects_an_unknown_choice(self, arguments: dict[str, Any], message: str) -> None:
        with pytest.raises(ValueError, match=message):
            Lakeflow(**arguments)

    @pytest.mark.parametrize(
        ("arguments", "message"),
        [
            pytest.param({"setup": "SELECT 1"}, "setup= takes a list of SQL statements, not a string", id="string"),
            pytest.param({"teardown": []}, "teardown= is an empty list of SQL statements", id="empty-list"),
            pytest.param({"setup": Path("seed.sql")}, "setup= must be a .py file, got seed.sql", id="not-py"),
            pytest.param({"teardown": lambda spark: None}, "teardown= cannot be a lambda", id="lambda"),
        ],
    )
    def test_rejects_a_bad_setup_default(self, arguments: dict[str, Any], message: str) -> None:
        with pytest.raises(ValueError, match=re.escape(message)):
            Lakeflow(**arguments)

    @pytest.mark.parametrize(
        ("environments", "message"),
        [
            pytest.param(
                {"__pytest_serverless": {"environment_version": "5"}},
                "Environment keys starting with '__pytest_' are reserved for the plugin: ['__pytest_serverless']",
                id="reserved-key",
            ),
            pytest.param(
                {"default": "5"},
                "environments['default'] must be a mapping of spec fields to values, not str",
                id="not-a-mapping",
            ),
            pytest.param(
                {"default": {"version": "5"}},
                "environments['default'] has fields that a serverless environment does not take: ['version']",
                id="unknown-field",
            ),
        ],
    )
    def test_rejects_a_bad_environments_default(self, environments: dict[str, Any], message: str) -> None:
        with pytest.raises(ValueError, match=re.escape(message)):
            Lakeflow(environments=environments)

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


class TestPublicApi:
    """The package exports and the shape of `EphemeralJob.run`."""

    def test_package_exports(self) -> None:
        import pytest_databricks_lakeflow

        assert Path(pytest_databricks_lakeflow.__file__).name == "__init__.py"
        for name in (
            "Lakeflow",
            "EphemeralJob",
            "JobRun",
            "TaskRun",
            "TaskError",
            "Timing",
            "LakeflowTimeout",
            "LakeflowRunFailed",
            "RunHandle",
            "PipelineUpdate",
            "EphemeralPipeline",
            "DebugNotebook",
            "Stub",
            "DbtArtifacts",
            "DbtResult",
        ):
            assert hasattr(pytest_databricks_lakeflow, name)
            assert name in pytest_databricks_lakeflow.__all__
        assert issubclass(pytest_databricks_lakeflow.LakeflowRunFailed, pytest_databricks_lakeflow.LakeflowError)

    @pytest.mark.parametrize("method", ["run", "start"])
    def test_pipeline_refresh_options_are_keyword_only(self, method: str) -> None:
        parameters = inspect.signature(getattr(EphemeralJob, method)).parameters
        names = (
            "full_refresh",
            "full_refresh_selection",
            "refresh_selection",
            "refresh_flow_selection",
            "reset_checkpoint_selection",
        )
        assert all(parameters[name].kind is inspect.Parameter.KEYWORD_ONLY for name in names)


@pytest.mark.usefixtures("fake_cli", "bundle_dir")
class TestSchemas:
    """`Lakeflow.schema()` creates a temporary schema and drops it unless the keep rules say otherwise."""

    def test_schema_is_created_and_dropped(
        self,
        pytester: pytest.Pytester,
        fake_ws: FakeWorkspaceClient,
        fake_cli: FakeCli,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("LAKEFLOW_RUN_ID", "CI-42")
        configure_lakeflow(pytester, 'catalog="dev_sandbox"')
        pytester.makepyfile(
            """
            import re

            def test_schema(lakeflow):
                with lakeflow.schema() as schema:
                    assert re.fullmatch(r"dev_sandbox\\.pytest_ci_42_[0-9a-f]{8}", schema), schema

            def test_failing(lakeflow):
                with lakeflow.schema():
                    assert False

            def test_other_catalog(lakeflow):
                with lakeflow.schema(catalog="other") as schema:
                    assert schema.startswith("other.pytest_ci_42_")
            """
        )
        pytester.runpytest().assert_outcomes(passed=2, failed=1)
        assert len(set(fake_ws.schemas.created)) == 3
        assert fake_ws.schemas.deleted == fake_ws.schemas.created
        assert fake_cli.calls == []

    @pytest.mark.parametrize(
        ("arguments", "flags"), [('catalog="c", keep=True', []), ('catalog="c"', ["--lakeflow-keep"])]
    )
    def test_schema_is_kept_with_keep(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, arguments: str, flags: list[str]
    ) -> None:
        configure_lakeflow(pytester, arguments)
        pytester.makepyfile(USES_SCHEMA)
        pytester.runpytest(*flags).assert_outcomes(passed=1)
        assert len(fake_ws.schemas.created) == 1
        assert fake_ws.schemas.deleted == []

    def test_schema_needs_a_catalog(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        pytester.makepyfile(USES_SCHEMA)
        result = pytester.runpytest()
        result.assert_outcomes(failed=1)
        result.stdout.fnmatch_lines(["*Lakeflow.schema() needs a catalog*"])
        assert fake_ws.schemas.created == []


def queue_probed_transform(ws: FakeWorkspaceClient) -> None:
    """Queue run 77 of a task job on transform, with a stub for extract and a probe that captured `row_count` 3."""
    run = make_run(77, {"extract": "SUCCESS", "transform": "SUCCESS", "__pytest_probe__transform": "SUCCESS"})
    probe_output = jobs.NotebookOutput(result=json.dumps({"values": {"row_count": 3}, "missing": []}))
    ws.jobs.queue(run, {7702: jobs.RunOutput(notebook_output=probe_output)})


def add_job_run(ws: FakeWorkspaceClient, run: jobs.Run, tags: dict[str, str]) -> int:
    """Add a job with `tags` and make `get_run` report `run` as a run of it, returning the job's id."""
    settings = example_settings()
    settings.tags = tags
    job_id = ws.jobs.add(settings)
    ws.jobs.add_run(replace(run, job_id=job_id))
    return job_id


def junit_properties(path: Path, test: str) -> dict[str, str]:
    """Read the junit properties of the test case `test` from a `--junitxml` report."""
    pattern = rf'<testcase [^>]*name="{test}".*?</testcase>'
    case = re.search(pattern, path.read_text(), re.DOTALL)
    assert case is not None, f"no test case {test} in {path}"
    return dict(re.findall(r'<property name="([^"]+)" value="([^"]*)"', case.group(0)))


PLUGIN_TAGS = {"pytest": "", "pytest_job_key": "my_job"}


@pytest.mark.usefixtures("fake_cli", "bundle_dir")
class TestAttach:
    """Rebuilding a JobRun from a kept job's run."""

    def test_attach_rebuilds_a_kept_run(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        queue_probed_transform(fake_ws)
        result = run_tests(
            pytester,
            """
            def test_kept(lakeflow):
                with lakeflow.task(
                    "my_job.transform",
                    upstream_task_values={"extract": {"row_count": 3}},
                    capture_task_values=["row_count"],
                ) as job:
                    job.run()
                    assert False, "keep the job"

            def test_attach(lakeflow):
                run = lakeflow.attach(77)
                assert run.job_key == "my_job"
                assert run.job_id == 1001
                assert set(run.tasks) == {"transform"}
                assert set(run.injected_tasks) == {"extract", "__pytest_probe__transform"}
                assert run.task("transform").task_values == {"row_count": 3}
            """,
            "--junitxml=report.xml",
        )
        result.assert_outcomes(passed=1, failed=1)
        assert fake_ws.jobs.deleted == []
        assert fake_ws.jobs.cancelled == []
        assert len(fake_ws.jobs.run_now_calls) == 1
        properties = junit_properties(pytester.path / "report.xml", "test_attach")
        assert properties["lakeflow_job_url"] == f"{HOST}/jobs/1001"
        assert properties["lakeflow_job_id"] == "1001"
        assert properties["lakeflow_run_url"] == f"{HOST}/jobs/runs/77"
        assert properties["lakeflow_run_id"] == "77"
        assert properties["lakeflow_result_state"] == "SUCCESS"

    @pytest.mark.parametrize(
        ("tags", "message"),
        [
            pytest.param(
                {"dev": "someone"},
                "Job 1001 of run 90 was not created by pytest-databricks-lakeflow (no pytest tag)",
                id="foreign",
            ),
            pytest.param(
                {"pytest": ""},
                "Job 1001 of run 90 was created by a release of pytest-databricks-lakeflow that did not record the "
                "bundle job key, so it cannot be attached",
                id="no-job-key",
            ),
        ],
    )
    def test_attach_rejects_foreign_jobs(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, tags: dict[str, str], message: str
    ) -> None:
        add_job_run(fake_ws, make_run(90, {"extract": "SUCCESS"}), tags)
        source = f"""
            import pytest
            from pytest_databricks_lakeflow import LakeflowError

            def test_attach(lakeflow):
                with pytest.raises(LakeflowError) as excinfo:
                    lakeflow.attach(90)
                assert str(excinfo.value) == {message!r}
            """
        run_tests(pytester, source).assert_outcomes(passed=1)

    @pytest.mark.parametrize(
        ("status", "state", "shown"),
        [
            pytest.param(jobs.RunStatus(state=jobs.RunLifecycleStateV2State.RUNNING), None, "RUNNING", id="v2"),
            pytest.param(None, jobs.RunState(life_cycle_state=jobs.RunLifeCycleState.PENDING), "PENDING", id="legacy"),
            pytest.param(None, None, "unknown", id="no-state"),
        ],
    )
    def test_attach_rejects_an_active_run(
        self,
        pytester: pytest.Pytester,
        fake_ws: FakeWorkspaceClient,
        status: jobs.RunStatus | None,
        state: jobs.RunState | None,
        shown: str,
    ) -> None:
        add_job_run(fake_ws, replace(running_run(91, {"extract": "RUNNING"}), status=status, state=state), PLUGIN_TAGS)
        message = f"Run 91 has not finished ({shown}). attach() reads finished runs only."
        source = f"""
            import pytest
            from pytest_databricks_lakeflow import LakeflowError

            def test_attach(lakeflow):
                with pytest.raises(LakeflowError) as excinfo:
                    lakeflow.attach(91)
                assert str(excinfo.value) == {message!r}
            """
        run_tests(pytester, source).assert_outcomes(passed=1)
        assert fake_ws.jobs.gets == [], "the job is not read for a run that has not finished"

    def test_attach_rejects_a_task_run(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        add_job_run(fake_ws, make_run(77, {"extract": "SUCCESS", "transform": "SUCCESS"}), PLUGIN_TAGS)
        message = "Run 7701 is a task run of job run 77. Pass the job run id, the one in the Kept run: URL."
        source = f"""
            import pytest
            from pytest_databricks_lakeflow import LakeflowError

            def test_attach(lakeflow):
                with pytest.raises(LakeflowError) as excinfo:
                    lakeflow.attach(7701)
                assert str(excinfo.value) == {message!r}
            """
        run_tests(pytester, source).assert_outcomes(passed=1)

    def test_attach_rejects_a_run_of_no_job(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        fake_ws.jobs.add_run(replace(make_run(92, {"extract": "SUCCESS"}), job_id=None))
        message = "Run 92 belongs to no job, so pytest-databricks-lakeflow did not start it"
        source = f"""
            import pytest
            from pytest_databricks_lakeflow import LakeflowError

            def test_attach(lakeflow):
                with pytest.raises(LakeflowError) as excinfo:
                    lakeflow.attach(92)
                assert str(excinfo.value) == {message!r}
            """
        run_tests(pytester, source).assert_outcomes(passed=1)
        assert fake_ws.jobs.gets == []

    def test_attach_to_a_run_that_does_not_exist(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        source = f"""
            import pytest
            from pytest_databricks_lakeflow import LakeflowError

            def test_attach(lakeflow):
                with pytest.raises(LakeflowError) as excinfo:
                    lakeflow.attach(5)
                assert str(excinfo.value) == "Run 5 does not exist in {HOST}"
            """
        run_tests(pytester, source).assert_outcomes(passed=1)

    def test_attach_after_the_job_was_deleted(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        queue_probed_transform(fake_ws)
        message = (
            "Job 1001 of run 77 no longer exists, so the run cannot be rebuilt. attach() needs the job the run belongs "
            "to, which keep_failed, keep and --lakeflow-keep preserve."
        )
        source = f"""
            import pytest
            from pytest_databricks_lakeflow import LakeflowError

            def test_deleted(lakeflow):
                with lakeflow.task("my_job.transform", upstream_task_values={{"extract": {{"row_count": 3}}}}) as job:
                    job.run()

            def test_attach(lakeflow):
                with pytest.raises(LakeflowError) as excinfo:
                    lakeflow.attach(77)
                assert str(excinfo.value) == {message!r}
            """
        run_tests(pytester, source).assert_outcomes(passed=2)
        assert fake_ws.jobs.deleted == [1001]

    def test_attach_outside_a_test(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        pytester.makepyfile(
            """
            from pytest_databricks_lakeflow import Lakeflow, LakeflowError

            try:
                Lakeflow().attach(1)
            except LakeflowError as e:
                ERROR = str(e)

            def test_error():
                assert "Use Lakeflow inside a test" in ERROR
            """
        )
        pytester.runpytest().assert_outcomes(passed=1)
        assert fake_ws.jobs.get_run_calls == []
