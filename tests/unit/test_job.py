"""The ephemeral job: how a test makes a copy, what reaches `jobs.create`, and how the copy is closed and kept."""

from __future__ import annotations

from typing import Any

import pytest
from databricks.sdk.errors import PermissionDenied
from databricks.sdk.service import compute, jobs

from .conftest import (
    ARTIFACT_PATH,
    DENIED,
    GITHUB,
    TEMPLATE_PARAMETERS,
    UPLOADED_WHEEL,
    FakeCli,
    configure_lakeflow,
    deny_create,
    make_repository,
    run_tests,
    set_ci,
)
from .fakes.runs import make_run
from .fakes.templates import example_settings, parent_settings, reporting_settings
from .fakes.workspace import FakeWorkspaceClient, sp_rule_set

SP_USER = "roles/servicePrincipal.user"
ROLE_FIX = (
    "Ask a manager of that service principal to grant you the role, or leave run_as out to run the copy as yourself."
)
MAPPED_RUN_AS = (
    f"Creating the copy of 'my_job' with run_as 0000-app-id was refused: {DENIED} To create a job that runs as a "
    f"service principal, someone@example.com needs the Service Principal User role on it. {ROLE_FIX}"
)
MISSING_ROLE = (
    "someone@example.com does not have the Service Principal User role on 0000-app-id, so the copy of 'my_job' "
    f"cannot run as it. {ROLE_FIX}"
)
CHILD_WARNING = (
    "*Task 'trigger_child' of job 'parent' runs the deployed job 123, not a copy. Its run keeps that job's run_as "
    'and notifications and is not cancelled or deleted with the ephemeral job. Pass children="refuse" to '
    "Lakeflow.job() to fail instead.*"
)
TEST_MARKER = jobs.JobParameterDefinition(name="__pytest_test_id", default="test_jobs.py::test_job")


def serverless_template(ws: FakeWorkspaceClient, cli: FakeCli) -> None:
    """Make `my_job` serverless only, deployed and validated, with its artifacts under `ARTIFACT_PATH`."""
    settings = example_settings()
    settings.tasks = [task for task in settings.tasks or [] if task.task_key != "classic"]
    settings.job_clusters = None
    ws.jobs.settings[cli.deployed["my_job"]] = settings
    workspace = {"artifact_path": ARTIFACT_PATH}
    cli.summary = {"workspace": workspace}
    cli.validate = {"workspace": workspace, "resources": {"jobs": {"my_job": settings.as_dict()}}}


@pytest.mark.usefixtures("fake_cli", "bundle_dir")
class TestLifecycle:
    """Creating the copy, running it and deleting it, with and without a `with` block."""

    def test_with_block_creates_runs_and_deletes(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        result = run_tests(
            pytester,
            """
            def test_job(lakeflow):
                with lakeflow.job("my_job", params={"param1": "hi"}) as job:
                    assert job.job_id is not None
                    assert job.url.endswith(f"/jobs/{job.job_id}")
                    run = job.run(params={"schema": "s"})
                    assert run.succeeded
                    assert job.runs == [run]
            """,
        )
        result.assert_outcomes(passed=1)
        [created] = fake_ws.jobs.created
        assert created["name"] == "[pytest] test_jobs.py::test_job"
        assert created["tags"]["pytest_nodeid"] == "test_jobs.py::test_job"
        assert created["tags"]["pytest"] == ""
        assert created["max_concurrent_runs"] == 1
        assert created["parameters"] == TEMPLATE_PARAMETERS
        assert "deployment" not in created
        assert "access_control_list" not in created
        assert "run_as" not in created
        assert fake_ws.jobs.acls == [None]
        [call] = fake_ws.jobs.run_now_calls
        assert call["job_parameters"] == {"param1": "hi", "schema": "s"}
        assert call["only"] is None
        assert call["pipeline_params"] is None
        assert fake_ws.jobs.deleted == [call["job_id"]]

    @pytest.mark.parametrize(("passes", "deleted"), [(True, [1001]), (False, [])], ids=["passed", "failed"])
    def test_plain_call_closes_at_teardown(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, passes: bool, deleted: list[int]
    ) -> None:
        result = run_tests(
            pytester,
            f"""
            def test_job(lakeflow):
                job = lakeflow.job("my_job")
                assert job.job_id is None
                assert job.run().succeeded
                assert job.job_id is not None
                assert {passes}
            """,
        )
        result.assert_outcomes(passed=int(passes), failed=int(not passes))
        assert fake_ws.jobs.deleted == deleted

    def test_unused_job_is_never_created(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        result = run_tests(pytester, 'def test_job(lakeflow):\n    lakeflow.job("my_job")\n')
        result.assert_outcomes(passed=1)
        assert (fake_ws.jobs.created, fake_ws.jobs.deleted) == ([], [])

    def test_run_after_the_block_fails_clearly(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        result = run_tests(
            pytester,
            """
            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    job.run()
                with pytest.raises(LakeflowError, match="has already been closed"):
                    job.run()
            """,
        )
        result.assert_outcomes(passed=1)
        assert len(fake_ws.jobs.run_now_calls) == 1

    def test_module_fixture_shares_one_job(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        result = run_tests(
            pytester,
            """
            import pytest

            @pytest.fixture(scope="module")
            def run(lakeflow):
                with lakeflow.job("my_job") as job:
                    yield job.run()

            def test_a(run):
                assert run.succeeded

            def test_b(run):
                assert run.succeeded
            """,
        )
        result.assert_outcomes(passed=2)
        assert len(fake_ws.jobs.created) == 1
        assert fake_ws.jobs.created[0]["name"] == "[pytest] test_jobs.py::test_a"
        assert len(fake_ws.jobs.run_now_calls) == 1
        assert fake_ws.jobs.deleted == [1001]

    def test_exit_cancels_active_runs(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def broken(run_id: int, **kwargs: Any) -> jobs.Run:
            raise RuntimeError("network down")

        fake_ws.jobs.queue(make_run(77, {"t": "SUCCESS"}))
        monkeypatch.setattr(fake_ws.jobs, "get_run", broken)
        result = run_tests(
            pytester,
            """
            import pytest

            def test_job(lakeflow):
                with pytest.raises(RuntimeError):
                    with lakeflow.job("my_job") as job:
                        job.run()
                assert lakeflow.ws.jobs.cancelled == [77]
                assert lakeflow.ws.jobs.deleted == []
            """,
        )
        result.assert_outcomes(passed=1)

    def test_create_errors_pass_through(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def denied(**kwargs: object) -> jobs.CreateResponse:
            raise PermissionDenied("User is not authorised to create jobs")

        monkeypatch.setattr(fake_ws.jobs, "create", denied)
        result = run_tests(
            pytester,
            """
            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    job.run()
            """,
        )
        result.assert_outcomes(failed=1)
        result.stdout.fnmatch_lines(["*PermissionDenied: User is not authorised to create jobs*"])
        assert fake_ws.jobs.run_now_calls == []


@pytest.mark.usefixtures("fake_cli", "bundle_dir")
class TestRunAsRole:
    """A copy that runs as a service principal needs the Service Principal User role, checked and explained."""

    def test_permission_denied_with_run_as_is_explained(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(fake_ws.jobs, "create", deny_create)
        result = run_tests(
            pytester,
            f"""
            import pytest
            from databricks.sdk.errors import PermissionDenied

            from pytest_databricks_lakeflow import LakeflowError

            def test_job(lakeflow):
                with pytest.raises(LakeflowError) as excinfo:
                    lakeflow.job("my_job", run_as="0000-app-id").create()
                assert str(excinfo.value) == {MAPPED_RUN_AS!r}
                assert isinstance(excinfo.value.__cause__, PermissionDenied)
                assert str(excinfo.value.__cause__) == {DENIED!r}
            """,
        )
        result.assert_outcomes(passed=1)

    @pytest.mark.parametrize("run_as", ["", ', run_as="someone@example.com"'], ids=["no-run-as", "user"])
    def test_permission_denied_without_run_as_passes_through(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, monkeypatch: pytest.MonkeyPatch, run_as: str
    ) -> None:
        monkeypatch.setattr(fake_ws.jobs, "create", deny_create)
        configure_lakeflow(pytester, 'permissions=[{"group_name": "data-eng", "permission_level": "CAN_VIEW"}]')
        result = run_tests(pytester, f'def test_job(lakeflow):\n    lakeflow.job("my_job"{run_as}).create()\n')
        result.assert_outcomes(failed=1)
        result.stdout.fnmatch_lines([f"*PermissionDenied: {DENIED}"])

    def test_missing_role_is_refused_before_create(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        fake_ws.account_access_control_proxy.grant("0000-app-id", SP_USER, "users/other@example.com")
        result = run_tests(
            pytester,
            """
            def test_job(lakeflow):
                lakeflow.task("my_job.extract", run_as="0000-app-id").create()
            """,
        )
        result.assert_outcomes(failed=1)
        result.stdout.fnmatch_lines([f"*LakeflowError: {MISSING_ROLE}"])
        assert fake_ws.jobs.created == []

    def test_granted_role_creates_the_copies_reading_it_once(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        fake_ws.account_access_control_proxy.grant("0000-app-id", SP_USER, "users/someone@example.com")
        result = run_tests(
            pytester,
            """
            def test_job(lakeflow):
                lakeflow.job("my_job", run_as="0000-app-id").create()

            def test_task(lakeflow):
                lakeflow.task("my_job.extract", run_as="0000-app-id").create()
            """,
        )
        result.assert_outcomes(passed=2)
        for created in fake_ws.jobs.created:
            assert created["run_as"] == jobs.JobRunAs(service_principal_name="0000-app-id")
        assert len(fake_ws.jobs.created) == 2
        assert fake_ws.account_access_control_proxy.names == [sp_rule_set("0000-app-id")]


@pytest.mark.usefixtures("fake_cli", "bundle_dir")
class TestKeepRules:
    """Which jobs survive the test that made them."""

    def test_failure_inside_with_keeps_the_job(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        result = run_tests(
            pytester,
            """
            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    job.run()
                    assert False, "boom"
            """,
        )
        result.assert_outcomes(failed=1)
        assert fake_ws.jobs.deleted == []
        result.stdout.fnmatch_lines(["*Keeping job 1001 from my_job: https://example.cloud.databricks.com/jobs/1001*"])
        result.stdout.fnmatch_lines(["*Kept run: https://example.cloud.databricks.com/jobs/runs/*"])

    def test_failure_after_with_still_deletes(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        result = run_tests(
            pytester,
            """
            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    run = job.run()
                assert not run.succeeded
            """,
        )
        result.assert_outcomes(failed=1)
        assert fake_ws.jobs.deleted == [1001]

    def test_skip_or_xfail_inside_with_deletes_the_job(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        result = run_tests(
            pytester,
            """
            import pytest

            def test_skip(lakeflow):
                with lakeflow.job("my_job") as job:
                    job.run()
                    pytest.skip("not ready")

            def test_xfail(lakeflow):
                with lakeflow.job("my_job") as job:
                    job.run()
                    pytest.xfail("known bug")
            """,
        )
        result.assert_outcomes(skipped=1, xfailed=1)
        assert fake_ws.jobs.deleted == [1001, 1003]

    @pytest.mark.parametrize(
        ("arguments", "flags", "deleted"),
        [
            ("", [], [1001]),
            ("", ["--lakeflow-keep"], []),
            ("", ["--no-lakeflow-keep-failed"], [1001, 1003]),
            ("keep=True", [], []),
            ("keep_failed=False", [], [1001, 1003]),
            ("keep_failed=False", ["--lakeflow-keep-failed"], [1001]),
            ("keep=True", ["--no-lakeflow-keep-failed"], []),
        ],
    )
    def test_keep_rules_with_flags_winning(
        self,
        pytester: pytest.Pytester,
        fake_ws: FakeWorkspaceClient,
        arguments: str,
        flags: list[str],
        deleted: list[int],
    ) -> None:
        configure_lakeflow(pytester, arguments)
        source = """
            def test_ok(lakeflow):
                with lakeflow.job("my_job") as job:
                    job.run()

            def test_failed(lakeflow):
                with lakeflow.job("my_job") as job:
                    job.run()
                    assert False
        """
        result = run_tests(pytester, source, *flags)
        result.assert_outcomes(passed=1, failed=1)
        assert fake_ws.jobs.deleted == deleted


@pytest.mark.usefixtures("fake_cli", "bundle_dir")
class TestCreateSettings:
    """What reaches `jobs.create`: the test marker, attribution, permissions, run_as and compute settings."""

    def test_output_reference_to_a_notebook_task_is_refused_before_creating(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        # Probed on the example bundle: jobs/create refuses it with InvalidParameterValue "Unsupported task type for
        # the 'output' namespace."
        message = (
            "Task 'transform' of 'my_job' refers to {{tasks.extract.output.result}}, but 'extract' is a notebook "
            'task, and Databricks refuses output references to notebook tasks ("Unsupported task type for the '
            "'output' namespace.\"). Set a task value in the notebook and refer to it with "
            "{{tasks.extract.values.<name>}} instead."
        )
        source = f"""
            import pytest
            from pytest_databricks_lakeflow import LakeflowError

            def test_job(lakeflow):
                reference = "{{{{tasks.extract.output.result}}}}"
                job = lakeflow.job("my_job", task_params={{"transform": {{"row": reference}}}})
                with pytest.raises(LakeflowError) as excinfo:
                    job.create()
                assert str(excinfo.value) == {message!r}
            """
        run_tests(pytester, source).assert_outcomes(passed=1)
        assert fake_ws.jobs.created == []

    @pytest.mark.parametrize(
        ("extra", "name"),
        [("", "[pytest] test_jobs.py::test_job"), (', name="custom"', "[pytest] custom")],
        ids=["default-name", "custom-name"],
    )
    def test_test_marker_declares_the_node_id(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, extra: str, name: str
    ) -> None:
        configure_lakeflow(pytester, "test_marker=True")
        result = run_tests(
            pytester,
            f"""
            def test_job(lakeflow):
                assert lakeflow.test_marker
                with lakeflow.job("my_job", params={{"param1": "hi"}}{extra}) as job:
                    assert job.run(params={{"schema": "s"}}).succeeded
            """,
        )
        result.assert_outcomes(passed=1)
        [created] = fake_ws.jobs.created
        assert created["name"] == name
        assert created["parameters"] == [*TEMPLATE_PARAMETERS, TEST_MARKER]
        [call] = fake_ws.jobs.run_now_calls
        assert call["job_parameters"] == {"param1": "hi", "schema": "s"}

    def test_jobs_carry_attribution_tags(
        self, pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch, fake_ws: FakeWorkspaceClient
    ) -> None:
        set_ci(monkeypatch, GITHUB)
        result = run_tests(pytester, 'def test_job(lakeflow):\n    with lakeflow.job("my_job"):\n        pass\n')
        result.assert_outcomes(passed=1)
        [created] = fake_ws.jobs.created
        assert created["tags"] == {
            "dev": "someone",
            "purpose": "pytest",
            "pytest": "",
            "pytest_run_id": "9",
            "pytest_nodeid": "test_jobs.py::test_job",
            "pytest_job_key": "my_job",
            "pytest_ci_url": "https://github.com/o/r/actions/runs/9",
            "pytest_user": "someone",
            "pytest_git_sha": "abc",
            "pytest_git_branch": "main",
        }
        assert created["description"] == "test_jobs.py::test_job https://github.com/o/r/actions/runs/9"

    @pytest.mark.parametrize(
        "make", ['lakeflow.job("my_job")', 'lakeflow.task("my_job.transform", upstream_task_values={"extract": {}})']
    )
    def test_created_jobs_carry_the_job_key_tag(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, make: str
    ) -> None:
        run_tests(pytester, f"def test_job(lakeflow):\n    with {make}:\n        pass\n").assert_outcomes(passed=1)
        assert fake_ws.jobs.created[0]["tags"]["pytest_job_key"] == "my_job"

    def test_attribution_user_falls_back_to_the_workspace_user(
        self, pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch, fake_ws: FakeWorkspaceClient
    ) -> None:
        set_ci(monkeypatch, {})
        monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(pytester.path.parent))
        result = run_tests(pytester, 'def test_job(lakeflow):\n    with lakeflow.job("my_job"):\n        pass\n')
        result.assert_outcomes(passed=1)
        [created] = fake_ws.jobs.created
        tags = created["tags"]
        assert tags["pytest_user"] == "someone@example.com"
        assert not {"pytest_ci_url", "pytest_git_sha", "pytest_git_branch"} & set(tags)
        assert created["description"] == "test_jobs.py::test_job"

    def test_attribution_from_git_in_the_rootdir(
        self, pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch, fake_ws: FakeWorkspaceClient
    ) -> None:
        set_ci(monkeypatch, {})
        sha = make_repository(pytester.path)
        result = run_tests(pytester, 'def test_job(lakeflow):\n    with lakeflow.job("my_job"):\n        pass\n')
        result.assert_outcomes(passed=1)
        tags = fake_ws.jobs.created[0]["tags"]
        assert tags["pytest_git_sha"] == sha
        assert tags["pytest_git_branch"] == "topic"

    def test_permissions_reach_jobs_create(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        configure_lakeflow(pytester, 'permissions=[{"group_name": "data-eng", "permission_level": "CAN_VIEW"}]')
        result = run_tests(pytester, 'def test_job(lakeflow):\n    lakeflow.job("my_job").create()\n')
        result.assert_outcomes(passed=1)
        view = jobs.JobPermissionLevel.CAN_VIEW
        assert fake_ws.jobs.acls == [[jobs.JobAccessControlRequest(group_name="data-eng", permission_level=view)]]
        assert "access_control_list" not in fake_ws.jobs.created[0]

    def test_run_as_keep_on_job_and_task(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        result = run_tests(
            pytester,
            """
            from databricks.sdk.service import jobs

            def test_job(lakeflow):
                lakeflow.job("my_job", run_as="keep").create()
                lakeflow.task(
                    "my_job.transform", upstream_task_values={"extract": {"row_count": 1}}, run_as="keep"
                ).create()
                lakeflow.job("my_job", run_as="0000-app-id").create()
                lakeflow.job("my_job", run_as="me@example.com").create()
                lakeflow.job("my_job", run_as=jobs.JobRunAs(service_principal_name="1111-object-id")).create()
            """,
        )
        result.assert_outcomes(passed=1)
        template = jobs.JobRunAs(user_name="someone@example.com")
        assert [created["run_as"] for created in fake_ws.jobs.created] == [
            template,
            template,
            jobs.JobRunAs(service_principal_name="0000-app-id"),
            jobs.JobRunAs(user_name="me@example.com"),
            jobs.JobRunAs(service_principal_name="1111-object-id"),
        ]

    def test_compute_knobs_reach_jobs_create(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        configure_lakeflow(pytester, 'environment_version="5", budget_policy_id="0123", existing_cluster_id="c-1"')
        result = run_tests(pytester, 'def test_job(lakeflow):\n    with lakeflow.job("my_job"):\n        pass\n')
        result.assert_outcomes(passed=1)
        [created] = fake_ws.jobs.created
        assert created["budget_policy_id"] == "0123"
        assert [e.spec.environment_version for e in created["environments"]] == ["5"]
        assert "job_clusters" not in created
        tasks = {t.task_key: t for t in created["tasks"]}
        assert (tasks["classic"].existing_cluster_id, tasks["classic"].job_cluster_key) == ("c-1", None)
        assert tasks["load"].environment_key == "default"
        assert tasks["load"].existing_cluster_id is None

    def test_single_node_reaches_jobs_create(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        configure_lakeflow(pytester, "single_node=True")
        result = run_tests(pytester, 'def test_job(lakeflow):\n    with lakeflow.job("my_job"):\n        pass\n')
        result.assert_outcomes(passed=1)
        [created] = fake_ws.jobs.created
        cluster = created["job_clusters"][0].new_cluster
        assert (cluster.num_workers, cluster.autoscale) == (0, None)
        assert cluster.spark_conf["spark.databricks.cluster.profile"] == "singleNode"
        assert cluster.custom_tags == {"ResourceClass": "SingleNode"}

    def test_single_node_with_existing_cluster_id_reaches_jobs_create(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        configure_lakeflow(pytester, 'single_node=True, existing_cluster_id="c-1"')
        result = run_tests(pytester, 'def test_job(lakeflow):\n    with lakeflow.job("my_job"):\n        pass\n')
        result.assert_outcomes(passed=1)
        [created] = fake_ws.jobs.created
        assert "job_clusters" not in created
        tasks = {t.task_key: t for t in created["tasks"]}
        assert (tasks["classic"].existing_cluster_id, tasks["classic"].new_cluster) == ("c-1", None)

    def test_alert_task_reaches_the_copy_intact(
        self, pytester: pytest.Pytester, fake_cli: FakeCli, fake_ws: FakeWorkspaceClient
    ) -> None:
        fake_cli.deployed["reporting"] = fake_ws.jobs.add(reporting_settings())
        result = run_tests(pytester, 'def test_job(lakeflow):\n    lakeflow.job("reporting").run()\n')
        result.assert_outcomes(passed=1)
        tasks = {t.task_key: t for t in fake_ws.jobs.created[0]["tasks"]}
        alert = tasks["check"].alert_task
        assert alert is not None
        assert (alert.alert_id, alert.warehouse_id) == ("a2", "w1")
        assert alert.subscribers == [jobs.AlertTaskSubscriber(user_name="someone@example.com")]

    def test_standard_performance_target_warns(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli
    ) -> None:
        fake_ws.jobs.settings[fake_cli.deployed["my_job"]].performance_target = jobs.PerformanceTarget.STANDARD
        result = run_tests(pytester, 'def test_job(lakeflow):\n    lakeflow.job("my_job")\n')
        result.assert_outcomes(passed=1, warnings=1)
        result.stdout.fnmatch_lines(["*Job 'my_job' has performance_target STANDARD*Pass performance_optimized=True*"])

    def test_performance_optimized(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli
    ) -> None:
        fake_ws.jobs.settings[fake_cli.deployed["my_job"]].performance_target = jobs.PerformanceTarget.STANDARD
        configure_lakeflow(pytester, "performance_optimized=True, disable_auto_optimization=False")
        result = run_tests(pytester, 'def test_job(lakeflow):\n    lakeflow.job("my_job").run()\n')
        result.assert_outcomes(passed=1)
        [created] = fake_ws.jobs.created
        assert created["performance_target"] == jobs.PerformanceTarget.PERFORMANCE_OPTIMIZED
        assert not any(task.disable_auto_optimization for task in created["tasks"])

    def test_extra_dependencies_reach_the_created_job(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        result = run_tests(
            pytester,
            """
            def test_job(lakeflow):
                with lakeflow.job("my_job", extra_dependencies=["pytest>=8"], capture_task_values={"load": ["k"]}):
                    pass

            def test_task(lakeflow):
                with lakeflow.task("my_job.classic", extra_dependencies=["/Volumes/c/s/v/helpers.whl"]):
                    pass
            """,
        )
        result.assert_outcomes(passed=2)
        job, single = fake_ws.jobs.created
        default, helper = job["environments"]
        assert default.spec.dependencies == ["pytest>=8"]
        assert (helper.environment_key, helper.spec.dependencies) == ("__pytest_serverless", None)
        libraries = {t.task_key: t.libraries for t in job["tasks"]}
        assert libraries["classic"] == [compute.Library(pypi=compute.PythonPyPiLibrary(package="pytest>=8"))]
        assert not libraries["extract"]
        assert not libraries["__pytest_probe__load"]
        [classic] = single["tasks"]
        assert classic.libraries == [compute.Library(whl="/Volumes/c/s/v/helpers.whl")]

    @pytest.mark.parametrize("sync", ["deploy", "none"])
    def test_local_wheel_extras_reach_the_environment(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, sync: str
    ) -> None:
        serverless_template(fake_ws, fake_cli)
        (pytester.path / "dist").mkdir()
        (pytester.path / "dist" / "pkg-0.1-py3-none-any.whl").write_text("")
        fake_ws.workspace.objects.add(UPLOADED_WHEEL)
        configure_lakeflow(pytester, f"sync={sync!r}")
        result = run_tests(
            pytester,
            """
            def test_job(lakeflow):
                dependencies = ["./dist/*.whl[test]"]
                with lakeflow.job("my_job", extra_dependencies=dependencies, capture_task_values={"load": ["k"]}):
                    pass
            """,
        )
        result.assert_outcomes(passed=1)
        [created] = fake_ws.jobs.created
        default, helper = created["environments"]
        assert default.spec.dependencies[-1] == f"{UPLOADED_WHEEL}[test]"
        assert (helper.environment_key, helper.spec.dependencies) == ("__pytest_serverless", None)

    @pytest.mark.parametrize(
        ("sync", "uploaded", "error"),
        [
            ("sync", True, r'which `bundle sync` does not upload. Use Lakeflow\(sync="deploy"\)'),
            ("none", False, r"pkg-0.1-py3-none-any.whl not found. Run `databricks bundle deploy` first"),
        ],
        ids=["sync", "not uploaded"],
    )
    def test_local_wheels_need_an_uploaded_copy(
        self,
        pytester: pytest.Pytester,
        fake_ws: FakeWorkspaceClient,
        fake_cli: FakeCli,
        sync: str,
        uploaded: bool,
        error: str,
    ) -> None:
        serverless_template(fake_ws, fake_cli)
        (pytester.path / "dist").mkdir()
        (pytester.path / "dist" / "pkg-0.1-py3-none-any.whl").write_text("")
        if uploaded:
            fake_ws.workspace.objects.add(UPLOADED_WHEEL)
        configure_lakeflow(pytester, f"sync={sync!r}")
        result = run_tests(
            pytester,
            f"""
            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            def test_job(lakeflow):
                with pytest.raises(LakeflowError, match={error!r}):
                    lakeflow.job("my_job", extra_dependencies=["./dist/*.whl[test]"])
            """,
        )
        result.assert_outcomes(passed=1)
        assert fake_ws.jobs.created == []

    def test_per_job_environments_reach_jobs_create(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        configure_lakeflow(pytester, 'environment_version="4", helpers_environment_version="3"')
        result = run_tests(
            pytester,
            """
            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            def test_job(lakeflow):
                environments = {"default": {"environment_version": "5"}}
                with lakeflow.job("my_job", environments=environments, capture_task_values={"load": ["k"]}):
                    pass

            def test_unchanged(lakeflow):
                with lakeflow.job("my_job", environments=None), lakeflow.job("my_job", environments={}):
                    pass

            def test_reserved(lakeflow):
                with pytest.raises(ValueError, match="reserved"):
                    lakeflow.job("my_job", environments={"__pytest_serverless": {"environment_version": "5"}})
            """,
        )
        result.assert_outcomes(passed=3)
        overridden, *unchanged = fake_ws.jobs.created
        versions = {e.environment_key: e.spec.environment_version for e in overridden["environments"]}
        assert versions == {"default": "5", "__pytest_serverless": "3"}
        for created in unchanged:
            assert [e.spec.environment_version for e in created["environments"]] == ["4"]

    def test_per_job_environments_on_task_jobs(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        result = run_tests(
            pytester,
            """
            def test_task(lakeflow):
                environments = {"default": {"dependencies": ["chispa"]}, "gpu": {"environment_version": "4"}}
                with lakeflow.task("my_job.load", extra_dependencies=["pytest>=8"], environments=environments):
                    pass
            """,
        )
        result.assert_outcomes(passed=1)
        [created] = fake_ws.jobs.created
        specs = {e.environment_key: e.spec.as_dict() for e in created["environments"]}
        assert specs == {
            "default": {"environment_version": "4", "dependencies": ["chispa"]},
            "gpu": {"environment_version": "4"},
        }

    def test_task_params_reach_the_created_job(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        result = run_tests(
            pytester,
            """
            import warnings

            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            def test_job(lakeflow):
                task_params = {"transform": {"limit": "10"}, "load": ["--rows", "10"]}
                with lakeflow.job("my_job", task_params=task_params, capture_task_values={"transform": ["k"]}):
                    pass

            def test_task(lakeflow):
                with pytest.warns(UserWarning, match="sets 'schema', also a job parameter of 'my_job'") as record:
                    task = lakeflow.task(
                        "my_job.transform",
                        upstream_task_values={"extract": {"row_count": 1}},
                        task_params={"transform": {"schema": "s"}},
                    )
                assert record[0].filename == __file__
                with task:
                    pass

            def test_task_rejects_other_tasks(lakeflow):
                with pytest.raises(LakeflowError, match=r"Unknown task 'load' .*: \\['transform'\\]"):
                    lakeflow.task("my_job.transform", task_params={"load": ["x"]})
                with warnings.catch_warnings(record=True) as caught:
                    warnings.simplefilter("always")
                    with pytest.raises(LakeflowError, match=r"No task 'transfrom' in the job. Available: \\['classic'"):
                        lakeflow.task("my_job.transfrom", task_params={"transfrom": {"schema": "s"}})
                assert caught == [], "no collision warning for a task that does not exist"
                with pytest.raises(LakeflowError, match=r"refers to task values of \\['extract'\\]"):
                    lakeflow.task("my_job.load", task_params={"load": {"n": "{{tasks.extract.values.n}}"}})
            """,
        )
        result.assert_outcomes(passed=3)
        job, single = fake_ws.jobs.created
        tasks = {t.task_key: t for t in job["tasks"]}
        assert tasks["transform"].notebook_task.base_parameters == {
            "row_count": "{{tasks.extract.values.row_count}}",
            "limit": "10",
        }
        assert tasks["load"].python_wheel_task.parameters == ["--rows", "10"]
        assert tasks["__pytest_probe__transform"].notebook_task.base_parameters["__pytest_keys"] == '["k"]'
        [stub, transform] = single["tasks"]
        assert stub.task_key == "extract"
        assert transform.notebook_task.base_parameters["schema"] == "s"

    @pytest.mark.parametrize(
        ("call", "exception", "error"),
        [
            pytest.param(
                'lakeflow.job("my_job", task_params={"transform": {"schema": "s"}, "nope": {"x": "1"}})',
                "LakeflowError",
                "Unknown task 'nope'",
                id="job_unknown_task",
            ),
            pytest.param(
                'lakeflow.job("my_job", task_params={"transform": {"schema": 1}})',
                "ValueError",
                "must be a mapping of names to strings",
                id="job_wrong_shape",
            ),
            pytest.param(
                'lakeflow.task("my_job.transform", task_params={"transform": {"schema": 1}})',
                "ValueError",
                "must be a mapping of names to strings",
                id="task_wrong_shape",
            ),
        ],
    )
    def test_rejected_task_params_give_no_collision_warning(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, call: str, exception: str, error: str
    ) -> None:
        result = run_tests(
            pytester,
            f"""
            import warnings

            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            def test_job(lakeflow):
                with warnings.catch_warnings(record=True) as caught:
                    warnings.simplefilter("always")
                    with pytest.raises({exception}, match={error!r}):
                        {call}
                assert [str(w.message) for w in caught] == []
            """,
        )
        result.assert_outcomes(passed=1)
        assert fake_ws.jobs.created == []

    def test_task_overrides_reach_the_created_job(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        result = run_tests(
            pytester,
            """
            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            LOAD = {"timeout_seconds": 60, "max_retries": 1, "retry_on_timeout": True}
            STUBS = {"transform": {"k": 1}}

            def test_job(lakeflow):
                with lakeflow.job("my_job", task_overrides={"load": LOAD}, capture_task_values={"load": ["k"]}):
                    pass

            def test_task(lakeflow):
                overrides = {"load": {"max_retries": 2}}
                with lakeflow.task("my_job.load", upstream_task_values=STUBS, task_overrides=overrides):
                    pass

            def test_probe_key(lakeflow):
                overrides = {"__pytest_probe__load": {"max_retries": 1}}
                with pytest.raises(LakeflowError, match=r"Unknown task '__pytest_probe__load'.*Available"):
                    lakeflow.job("my_job", task_overrides=overrides, capture_task_values={"load": ["k"]})

            def test_unknown_field(lakeflow):
                with pytest.raises(ValueError, match="notebook_task.*timeout_seconds"):
                    lakeflow.job("my_job", task_overrides={"load": {"notebook_task": {}}})

            def test_stub_key(lakeflow):
                with pytest.raises(ValueError, match=r"name 'load', the task under test, not \\['transform'\\]"):
                    lakeflow.task("my_job.load", upstream_task_values=STUBS, task_overrides={"transform": {}})
            """,
        )
        result.assert_outcomes(passed=5)
        job, single = fake_ws.jobs.created
        retries = {t.task_key: (t.timeout_seconds, t.max_retries, t.retry_on_timeout) for t in job["tasks"]}
        assert retries == {
            "extract": (None, None, None),
            "transform": (None, None, None),
            "load": (60, 1, True),
            "classic": (None, None, None),
            "__pytest_probe__load": (None, None, None),
        }
        assert {t.task_key: t.max_retries for t in single["tasks"]} == {"transform": None, "load": 2}


@pytest.mark.usefixtures("fake_cli", "bundle_dir")
class TestCallbacks:
    """`modify_settings` and `run_finished`, and where a conftest override applies."""

    def test_callbacks(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        pytester.makeconftest(
            """
            import pytest

            from pytest_databricks_lakeflow import Lakeflow

            finished = []

            def describe(settings, item):
                settings.description = f"from {item.name}"

            def record(run, item):
                finished.append((run.job_key, item.name))

            @pytest.fixture(scope="session")
            def lakeflow():
                return Lakeflow(modify_settings=describe, run_finished=record)
            """
        )
        result = run_tests(
            pytester,
            """
            import conftest

            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    job.run()
                assert conftest.finished == [("my_job", "test_job")]
            """,
        )
        result.assert_outcomes(passed=1)
        assert fake_ws.jobs.created[0]["description"] == "from test_job"

    def test_overrides_apply_only_below_their_conftest(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli
    ) -> None:
        pytester.mkpydir("team_a").joinpath("conftest.py").write_text(
            """
import dataclasses

import pytest


def describe(settings, item):
    settings.description = "team a"


@pytest.fixture(scope="session")
def lakeflow(lakeflow):
    return dataclasses.replace(lakeflow, modify_settings=describe, children="refuse")
"""
        )
        pytester.mkpydir("team_b")
        test = "def test_job(lakeflow):\n    with lakeflow.job('my_job') as job:\n        job.run()\n"
        test_a = f"{test}    assert lakeflow.children == 'refuse'\n"
        test_b = f"{test}    assert lakeflow.children == 'warn'\n"
        pytester.path.joinpath("team_a", "test_a.py").write_text(test_a)
        pytester.path.joinpath("team_b", "test_b.py").write_text(test_b)
        pytester.runpytest("team_a/test_a.py", "team_b/test_b.py").assert_outcomes(passed=2)
        descriptions = [created["description"] for created in fake_ws.jobs.created]
        assert descriptions[0] == "team a"
        assert descriptions[1].startswith("team_b/test_b.py::test_job")
        assert fake_ws.jobs.deleted == [1001, 1003]
        assert fake_cli.calls.count(["bundle", "deploy", "-t", "test", "--auto-approve"]) == 1

    def test_check_compute_runs_after_modify_settings(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        callback = """

def drop_environment(settings, item):
    next(t for t in settings.tasks if t.task_key == "load").environment_key = None
"""
        configure_lakeflow(pytester, "modify_settings=drop_environment", callback)
        result = run_tests(pytester, 'def test_job(lakeflow):\n    with lakeflow.job("my_job"):\n        pass\n')
        result.assert_outcomes(failed=1)
        result.stdout.fnmatch_lines(["*LakeflowError*['load']*environment_key*"])
        assert fake_ws.jobs.created == []


def environment_versions(created: dict[str, Any]) -> dict[str, str | None]:
    return {e.environment_key: e.spec.environment_version for e in created["environments"]}


def lifecycle_notebooks(ws: FakeWorkspaceClient, created: dict[str, Any]) -> dict[str, str]:
    """Map each setup and teardown task of a created job to the source of the notebook it runs."""
    sources: dict[str, str] = {}
    for task in created["tasks"]:
        if task.task_key in ("__pytest_setup", "__pytest_teardown"):
            sources[task.task_key] = ws.workspace.uploads[task.notebook_task.notebook_path]["content"]
    return sources


@pytest.mark.usefixtures("fake_cli", "bundle_dir")
class TestSessionDefaults:
    """`Lakeflow` fields that set the default of a per-call argument, and the call that replaces one."""

    def test_session_default_disable_applies_to_every_job(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, parent: int
    ) -> None:
        configure_lakeflow(pytester, 'disable=["load"]')
        source = (
            'def test_job(lakeflow):\n    lakeflow.job("my_job").create()\n'
            '    lakeflow.job("parent", children="warn").create()\n'
        )
        run_tests(pytester, source).assert_outcomes(passed=1, warnings=1)
        mine, other = fake_ws.jobs.created
        assert [t.task_key for t in mine["tasks"] if t.disabled] == ["load"]
        assert [t.task_key for t in other["tasks"] if t.disabled] == [], "a job without the task is left alone"

    def test_call_argument_replaces_the_session_default(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        configure_lakeflow(pytester, 'disable=["load"], environments={"default": {"environment_version": "5"}}')
        result = run_tests(
            pytester,
            """
            def test_job(lakeflow):
                lakeflow.job("my_job", disable=[]).create()
                lakeflow.job("my_job", environments={}).create()
                lakeflow.job("my_job", environments={"gpu": {"environment_version": "4"}}).create()
            """,
        )
        result.assert_outcomes(passed=1)
        no_disable, no_environments, appended = fake_ws.jobs.created
        assert [t.task_key for t in no_disable["tasks"] if t.disabled] == []
        assert environment_versions(no_disable) == {"default": "5"}
        assert environment_versions(no_environments) == {"default": "4"}
        assert environment_versions(appended) == {"default": "4", "gpu": "4"}

    def test_session_default_environments_apply_to_every_job(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        configure_lakeflow(
            pytester,
            'environments={"default": {"environment_version": "5"}, "gpu": {"environment_version": "4"}}, '
            'helpers_environment_version="3"',
        )
        result = run_tests(
            pytester,
            """
            def test_job(lakeflow):
                lakeflow.job("my_job", capture_task_values={"load": ["k"]}).create()
                lakeflow.task("my_job.load").create()
            """,
        )
        result.assert_outcomes(passed=1)
        job, task = fake_ws.jobs.created
        assert environment_versions(job) == {"default": "5", "__pytest_serverless": "3"}
        assert environment_versions(task) == {"default": "5"}

    def test_session_default_children_refuse(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, parent: int
    ) -> None:
        configure_lakeflow(pytester, 'children="refuse"')
        refused = """
            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            def test_job(lakeflow):
                with pytest.raises(LakeflowError, match="runs the deployed job 123, not a copy"):
                    lakeflow.job("parent")
        """
        run_tests(pytester, refused).assert_outcomes(passed=1)
        assert fake_ws.jobs.created == []
        warned = run_tests(pytester, 'def test_job(lakeflow):\n    lakeflow.job("parent", children="warn").create()\n')
        warned.assert_outcomes(passed=1, warnings=1)
        warned.stdout.fnmatch_lines([CHILD_WARNING])
        assert len(fake_ws.jobs.created) == 1

    def test_session_default_run_as_keep(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        configure_lakeflow(pytester, 'run_as="keep"')
        result = run_tests(
            pytester,
            """
            def test_job(lakeflow):
                lakeflow.job("my_job").create()
                lakeflow.task("my_job.transform", upstream_task_values={"extract": {"row_count": 1}}).create()
                lakeflow.job("my_job", run_as="me@example.com").create()
            """,
        )
        result.assert_outcomes(passed=1)
        template = jobs.JobRunAs(user_name="someone@example.com")
        assert [created["run_as"] for created in fake_ws.jobs.created] == [
            template,
            template,
            jobs.JobRunAs(user_name="me@example.com"),
        ]

    def test_session_default_queue(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        configure_lakeflow(pytester, "queue=False")
        result = run_tests(
            pytester,
            """
            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    job.run()
                    job.start().wait()
                    job.run(queue=True)
            """,
        )
        result.assert_outcomes(passed=1)
        assert [call["queue"] for call in fake_ws.jobs.run_now_calls] == [
            jobs.QueueSettings(enabled=False),
            jobs.QueueSettings(enabled=False),
            jobs.QueueSettings(enabled=True),
        ]

    def test_session_default_dependencies_and_lifecycle(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        configure_lakeflow(
            pytester,
            'extra_dependencies=["pytest>=8"], setup=Path("seed.py"), teardown=["SELECT 2"]',
            "\nfrom pathlib import Path\n",
        )
        pytester.path.joinpath("seed.py").write_text("spark.sql('SELECT 1')\n")
        result = run_tests(
            pytester,
            """
            def test_job(lakeflow):
                lakeflow.job("my_job").create()
                lakeflow.task("my_job.load", extra_dependencies=[], setup=None, teardown=["SELECT 3"]).create()
            """,
        )
        result.assert_outcomes(passed=1)
        job, task = fake_ws.jobs.created
        assert [e.spec.dependencies for e in job["environments"] if e.environment_key == "default"] == [["pytest>=8"]]
        assert [e.spec.dependencies for e in task["environments"] if e.environment_key == "default"] == [None]
        job_notebooks = lifecycle_notebooks(fake_ws, job)
        task_notebooks = lifecycle_notebooks(fake_ws, task)
        assert job_notebooks["__pytest_setup"] == task_notebooks["__pytest_setup"]
        assert "spark.sql('SELECT 1')" in job_notebooks["__pytest_setup"]
        assert "SELECT 2" in job_notebooks["__pytest_teardown"]
        assert "SELECT 3" in task_notebooks["__pytest_teardown"]
        assert "SELECT 2" not in task_notebooks["__pytest_teardown"]


@pytest.fixture
def parent(fake_ws: FakeWorkspaceClient, fake_cli: FakeCli) -> int:
    fake_cli.deployed["parent"] = job_id = fake_ws.jobs.add(parent_settings())
    return job_id


def add_loop(fake_ws: FakeWorkspaceClient, parent: int) -> None:
    """Add a `for_each_task` whose nested task triggers the deployed job 456 to the parent job."""
    settings = fake_ws.jobs.settings[parent]
    nested = jobs.Task(task_key="each_child", run_job_task=jobs.RunJobTask(job_id=456))
    loop = jobs.Task(task_key="loop", for_each_task=jobs.ForEachTask(inputs="[1]", task=nested))
    settings.tasks = [*(settings.tasks or []), loop]


@pytest.mark.usefixtures("fake_cli", "bundle_dir", "parent")
class TestChildren:
    """Run Job tasks: warning or refusing, and reading the child run."""

    @pytest.mark.parametrize(
        ("call", "method"),
        [('lakeflow.job("parent")', "Lakeflow.job()"), ('lakeflow.task("parent.trigger_child")', "Lakeflow.task()")],
        ids=["job", "task"],
    )
    def test_run_job_task_warns_and_still_creates_the_job(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, call: str, method: str
    ) -> None:
        source = f"def test_job(lakeflow):\n    with {call} as job:\n        job.run()\n"
        result = run_tests(pytester, source)
        result.assert_outcomes(passed=1, warnings=1)
        result.stdout.fnmatch_lines([CHILD_WARNING.replace("Lakeflow.job()", method)])
        assert len(fake_ws.jobs.created) == 1

    def test_run_job_task_warns_once_per_run_job_task(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, parent: int
    ) -> None:
        add_loop(fake_ws, parent)
        result = run_tests(pytester, 'def test_job(lakeflow):\n    lakeflow.job("parent")\n')
        result.assert_outcomes(passed=1, warnings=2)
        result.stdout.fnmatch_lines(["*Task 'each_child' of job 'parent' runs the deployed job 456, not a copy*"])

    @pytest.mark.parametrize(
        ("with_loop", "call"),
        [
            pytest.param(False, 'lakeflow.job("parent", disable=["trigger_child"])', id="disabled"),
            pytest.param(
                True, 'lakeflow.job("parent", disable=["trigger_child", "loop"], children="refuse")', id="disabled-loop"
            ),
            pytest.param(
                False, 'lakeflow.job("parent", disable=["prepare"], children="refuse")', id="downstream-of-disabled"
            ),
            pytest.param(False, 'lakeflow.task("parent.prepare", children="refuse")', id="other-task"),
            pytest.param(False, 'lakeflow.job("my_job").run()', id="no-run-job-task"),
        ],
    )
    def test_no_warning_without_an_enabled_run_job_task(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, parent: int, with_loop: bool, call: str
    ) -> None:
        if with_loop:
            add_loop(fake_ws, parent)
        result = run_tests(pytester, f"def test_job(lakeflow):\n    {call}\n")
        result.assert_outcomes(passed=1)
        assert "warnings" not in result.parseoutcomes()

    @pytest.mark.parametrize(
        "call",
        ['lakeflow.job("parent", children="refuse")', 'lakeflow.task("parent.trigger_child", children="refuse")'],
    )
    def test_children_refuse_raises_before_creating(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, call: str
    ) -> None:
        source = f"""
            import pytest
            from pytest_databricks_lakeflow import LakeflowError

            def test_job(lakeflow):
                with pytest.raises(LakeflowError) as e:
                    {call}
                assert "Task 'trigger_child' of job 'parent' runs the deployed job 123, not a copy" in str(e.value)
                assert "not cancelled or deleted with the ephemeral job" in str(e.value)
        """
        run_tests(pytester, source).assert_outcomes(passed=1)
        assert fake_ws.jobs.created == []

    def test_children_rejects_unknown_values(self, pytester: pytest.Pytester) -> None:
        source = """
            import pytest

            def test_job(lakeflow):
                with pytest.raises(ValueError, match="children must be one of warn, refuse, got 'copy'"):
                    lakeflow.job("my_job", children="copy")
        """
        run_tests(pytester, source).assert_outcomes(passed=1)

    def test_child_run_of_a_run_job_task_is_a_job_run(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli
    ) -> None:
        fake_ws.jobs.start_ids_at(123)
        fake_cli.deployed["child"] = fake_ws.jobs.add(example_settings())
        fake_ws.jobs.start_ids_at(2000)
        child = make_run(777, {"load": "SUCCESS"})
        child.job_id = 123
        fake_ws.jobs.add_run(child)
        output = jobs.RunOutput(run_job_output=jobs.RunJobOutput(run_id=777))
        fake_ws.jobs.queue(make_run(5, {"prepare": "SUCCESS", "trigger_child": "SUCCESS"}), {501: output})
        source = """
            def test_job(lakeflow):
                with lakeflow.job("parent") as job:
                    run = job.run()
                child = run.task("trigger_child").child_run
                assert child is not None
                assert (child.job_key, child.job_id, child.run_id) == ("child", 123, 777)
                assert child.task("load").succeeded
                assert run.task("prepare").child_run is None
        """
        run_tests(pytester, source).assert_outcomes(passed=1, warnings=1)
        [created] = fake_ws.jobs.created
        assert fake_ws.jobs.cancelled == []
        assert fake_ws.jobs.deleted == [2000]
        assert created["name"].startswith("[pytest]")
