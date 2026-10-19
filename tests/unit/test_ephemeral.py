from __future__ import annotations

import inspect
import re
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from databricks.sdk.errors import PermissionDenied
from databricks.sdk.service import compute, jobs

from pytest_databricks_lakeflow.ephemeral import EphemeralJob

from .conftest import GITHUB, NEEDS_REFRESH_SELECTION, FakeCli, configure_lakeflow, make_repository, set_ci
from .fakes import (
    DATA,
    HOST,
    FakeWorkspaceClient,
    example_settings,
    make_run,
    parent_settings,
    pipeline_settings,
    reporting_settings,
    running_run,
)

pytestmark = pytest.mark.usefixtures("fake_cli", "bundle_dir")

TEMPLATE_PARAMETERS = [
    jobs.JobParameterDefinition(name="param1", default="hello"),
    jobs.JobParameterDefinition(name="schema", default=""),
]
TEST_MARKER = jobs.JobParameterDefinition(name="__pytest_test_id", default="test_jobs.py::test_job")
PROGRESS_LINES = [
    "extract: PENDING",
    "transform: PENDING",
    "extract: PENDING -> SUCCESS in 0:00:02",
    "transform: PENDING -> RUNNING",
    "transform: RUNNING -> FAILED in 0:00:02 ValueError: no rows",
    "__pytest_probe__transform: SUCCESS in 0:00:02",
]
CHILD_WARNING = (
    "*Task 'trigger_child' of job 'parent' runs the deployed job 123, not a copy. Its run keeps that job's run_as "
    'and notifications and is not cancelled or deleted with the ephemeral job. Pass children="refuse" to '
    "Lakeflow.job() to fail instead.*"
)


def run_tests(pytester: pytest.Pytester, source: str, *args: str) -> pytest.RunResult:
    pytester.makepyfile(test_jobs=source)
    return pytester.runpytest(*args)


def queue_transform_failure(ws: FakeWorkspaceClient) -> None:
    """Queue a run whose extract goes PENDING then SUCCESS and whose transform goes PENDING, RUNNING, then FAILED."""
    final = make_run(
        77,
        {"extract": "SUCCESS", "transform": "FAILED", "__pytest_probe__transform": "SUCCESS"},
        "FAILED",
        "RUN_EXECUTION_ERROR",
    )
    polls = [
        running_run(77, {"extract": "PENDING", "transform": "PENDING"}),
        running_run(77, {"extract": "SUCCESS", "transform": "RUNNING"}),
        final,
    ]
    ws.jobs.queue_polls(polls, {7701: jobs.RunOutput(error="ValueError: no rows\nmore detail")})


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
            raise PermissionDenied("User is not authorised to set run_as to 0000-app-id")

        monkeypatch.setattr(fake_ws.jobs, "create", denied)
        result = run_tests(
            pytester,
            """
            def test_job(lakeflow):
                with lakeflow.job("my_job", run_as="0000-app-id") as job:
                    job.run()
            """,
        )
        result.assert_outcomes(failed=1)
        result.stdout.fnmatch_lines(["*PermissionDenied: User is not authorised to set run_as to 0000-app-id*"])
        assert fake_ws.jobs.run_now_calls == []


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


class TestCreateSettings:
    """What reaches `jobs.create`: the test marker, attribution, permissions, run_as and compute settings."""

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
            "pytest_ci_url": "https://github.com/o/r/actions/runs/9",
            "pytest_user": "someone",
            "pytest_git_sha": "abc",
            "pytest_git_branch": "main",
        }
        assert created["description"] == "test_jobs.py::test_job https://github.com/o/r/actions/runs/9"

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
            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            def test_job(lakeflow):
                with lakeflow.job("my_job", extra_dependencies=["pytest>=8"], capture_task_values={"load": ["k"]}):
                    pass

            def test_task(lakeflow):
                with lakeflow.task("my_job.classic", extra_dependencies=["/Volumes/c/s/v/helpers.whl"]):
                    pass

            def test_local_path(lakeflow):
                with pytest.raises(LakeflowError, match="Local paths are not yet supported"):
                    lakeflow.job("my_job", extra_dependencies=["./dist/*.whl[test]"])
            """,
        )
        result.assert_outcomes(passed=3)
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


class TestDisabledTasks:
    """Disabling tasks on the copy, and telling disabled tasks from those `only` left out."""

    def test_disabled_tasks_come_from_the_copy_not_the_run(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        fake_ws.jobs.queue(make_run(77, {"extract": "SUCCESS", "load": "DISABLED", "transform": "DISABLED"}))
        result = run_tests(
            pytester,
            """
            def test_job(lakeflow):
                run = lakeflow.job("my_job", disable=["load"]).run()
                assert sorted(run.disabled_tasks) == ["load"]
                assert sorted(run.skipped_tasks) == ["transform"]
                assert not run.task("extract").disabled
            """,
        )
        result.assert_outcomes(passed=1)
        [created] = fake_ws.jobs.created
        assert {t.task_key: t.disabled for t in created["tasks"]} == {
            "extract": None,
            "transform": None,
            "load": True,
            "classic": None,
        }

    def test_template_disabled_tasks_are_reported_separately(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        callback = """

def disable_classic(settings, item):
    next(t for t in settings.tasks if t.task_key == "classic").disabled = True
"""
        configure_lakeflow(pytester, "modify_settings=disable_classic", callback)
        states = {"extract": "SUCCESS", "transform": "SUCCESS", "load": "SUCCESS", "classic": "DISABLED"}
        fake_ws.jobs.queue(make_run(77, states))
        result = run_tests(
            pytester,
            """
            import pytest

            def test_job(lakeflow):
                run = lakeflow.job("my_job").run()
                assert run.succeeded
                assert sorted(run.disabled_tasks) == ["classic"]
                assert run.skipped_tasks == {}
                assert run.executed_tasks == ["extract", "load", "transform"]
                with pytest.raises(KeyError, match="Disabled in the job"):
                    run.task("classic")
            """,
        )
        result.assert_outcomes(passed=1)

    def test_disable_rejects_unknown_tasks(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        result = run_tests(pytester, 'def test_job(lakeflow):\n    lakeflow.job("my_job", disable=["nope"]).run()\n')
        result.assert_outcomes(failed=1)
        result.stdout.fnmatch_lines(["*Unknown task 'nope' in disable. Available: ['classic', 'extract', 'load', *"])
        assert fake_ws.jobs.created == []


class TestParams:
    """Checking parameters against the job's declarations."""

    def test_bad_params_fail_before_creating(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        result = run_tests(
            pytester,
            """
            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            def test_job(lakeflow):
                message = "does not declare the parameters ..parm1.*allow_undeclared_params"
                with pytest.raises(LakeflowError, match=message):
                    with lakeflow.job("my_job", params={"parm1": "x"}):
                        pass
                with lakeflow.job("my_job") as job:
                    with pytest.raises(LakeflowError, match="does not declare"):
                        job.run(params={"typo": "x"})
            """,
        )
        result.assert_outcomes(passed=1)
        assert fake_ws.jobs.run_now_calls == []

    def test_undeclared_params_when_allowed(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        configure_lakeflow(pytester, "allow_undeclared_params=True")
        source = (
            'def test_job(lakeflow):\n    lakeflow.job("my_job", params={"typo": "x"}).run(params={"other": "y"})\n'
        )
        run_tests(pytester, source).assert_outcomes(passed=1)
        assert fake_ws.jobs.run_now_calls[0]["job_parameters"] == {"typo": "x", "other": "y"}

    @pytest.mark.parametrize("arguments", ["", "test_marker=True"])
    def test_test_marker_cannot_be_passed(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, arguments: str
    ) -> None:
        configure_lakeflow(pytester, arguments)
        result = run_tests(
            pytester,
            """
            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            def test_job(lakeflow):
                with pytest.raises(LakeflowError, match="reserved for the plugin"):
                    lakeflow.job("my_job", params={"__pytest_test_id": "x"})
                with lakeflow.job("my_job") as job:
                    with pytest.raises(LakeflowError, match="reserved for the plugin"):
                        job.run(params={"__pytest_test_id": "x"})
            """,
        )
        result.assert_outcomes(passed=1)
        assert fake_ws.jobs.run_now_calls == []

    def test_params_declared_by_modify_settings_are_accepted(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        pytester.makeconftest(
            """
            import pytest
            from databricks.sdk.service import jobs

            from pytest_databricks_lakeflow import Lakeflow

            def declare_catalog(settings, item):
                settings.parameters = [*settings.parameters, jobs.JobParameterDefinition(name="catalog", default="")]

            @pytest.fixture(scope="session")
            def lakeflow():
                return Lakeflow(modify_settings=declare_catalog)
            """
        )
        result = run_tests(
            pytester,
            r"""
            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            def test_declared_by_callback(lakeflow):
                with lakeflow.job("my_job", params={"catalog": "x"}) as job:
                    job.run()

            def test_typo_still_fails(lakeflow):
                with pytest.raises(LakeflowError, match=r"does not declare the parameters \['typo'\]"):
                    with lakeflow.job("my_job", params={"typo": "x"}):
                        pass

            def test_reserved_prefix_fails_early(lakeflow):
                with pytest.raises(LakeflowError, match="reserved"):
                    lakeflow.job("my_job", params={"__pytest_keys": "x"})
            """,
        )
        result.assert_outcomes(passed=3)
        assert len(fake_ws.jobs.created) == 1
        assert fake_ws.jobs.run_now_calls[0]["job_parameters"] == {"catalog": "x"}


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
    return dataclasses.replace(lakeflow, modify_settings=describe)
"""
        )
        pytester.mkpydir("team_b")
        test = "def test_job(lakeflow):\n    with lakeflow.job('my_job') as job:\n        job.run()\n"
        pytester.path.joinpath("team_a", "test_a.py").write_text(test)
        pytester.path.joinpath("team_b", "test_b.py").write_text(test)
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


class TestRunOptions:
    """What `run()` sends to `run_now`."""

    def test_run_reads_resolved_values_from_each_task_run(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        run = make_run(77, {"extract": "SUCCESS", "transform": "SUCCESS"})
        fake_ws.jobs.queue(run)
        run_tests(pytester, 'def test_job(lakeflow):\n    lakeflow.job("my_job").run()\n').assert_outcomes(passed=1)
        with_flag = [call["run_id"] for call in fake_ws.jobs.get_run_calls if call.get("include_resolved_values")]
        assert sorted(with_flag) == [7700, 7701], "each task run is fetched once by its own id"
        polls = [call for call in fake_ws.jobs.get_run_calls if call["run_id"] == 77]
        assert polls and not any(call.get("include_resolved_values") for call in polls)

    def test_only_adds_probes(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        result = run_tests(
            pytester,
            """
            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    job.run(only=["+transform"])
                    job.run(only=[])
            """,
        )
        result.assert_outcomes(passed=1)
        assert [call["only"] for call in fake_ws.jobs.run_now_calls] == [["+transform"], None]

    def test_run_sends_a_fresh_idempotency_token(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        fake_ws.jobs.queue(make_run(77, {"t": "SUCCESS"}), polls=100)
        result = run_tests(
            pytester,
            """
            from datetime import timedelta

            import pytest

            from pytest_databricks_lakeflow import LakeflowTimeout

            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    with pytest.raises(LakeflowTimeout):
                        job.run(timeout=timedelta(0))
                    assert job.run().run_id != 77
                    assert job.run().run_id != 77
            """,
        )
        result.assert_outcomes(passed=1)
        tokens = [call["idempotency_token"] for call in fake_ws.jobs.run_now_calls]
        assert len(tokens) == 3
        assert all(tokens)
        assert len(set(tokens)) == 3
        assert all(len(token) <= 64 for token in tokens)

    @pytest.mark.parametrize(
        ("arguments", "target", "queue"),
        [
            pytest.param("", None, None, id="defaults"),
            pytest.param(
                'performance_target="PERFORMANCE_OPTIMIZED", queue=False',
                jobs.PerformanceTarget.PERFORMANCE_OPTIMIZED,
                jobs.QueueSettings(enabled=False),
                id="string",
            ),
            pytest.param(
                "performance_target=jobs.PerformanceTarget.STANDARD, queue=True",
                jobs.PerformanceTarget.STANDARD,
                jobs.QueueSettings(enabled=True),
                id="enum",
            ),
        ],
    )
    def test_run_forwards_performance_target_and_queue(
        self,
        pytester: pytest.Pytester,
        fake_ws: FakeWorkspaceClient,
        arguments: str,
        target: jobs.PerformanceTarget | None,
        queue: jobs.QueueSettings | None,
    ) -> None:
        result = run_tests(
            pytester,
            f"""
            from databricks.sdk.service import jobs

            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    job.run({arguments})
                    assert job.settings.performance_target is None
            """,
        )
        result.assert_outcomes(passed=1)
        [call] = fake_ws.jobs.run_now_calls
        assert call["performance_target"] == target
        assert call["queue"] == queue
        assert "performance_target" not in fake_ws.jobs.created[0]

    def test_run_rejects_an_unknown_performance_target(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        result = run_tests(
            pytester,
            """
            import pytest

            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    with pytest.raises(ValueError, match="bogus"):
                        job.run(performance_target="bogus")
            """,
        )
        result.assert_outcomes(passed=1)
        assert fake_ws.jobs.run_now_calls == []

    def test_run_passes_full_refresh(
        self, pytester: pytest.Pytester, fake_cli: FakeCli, fake_ws: FakeWorkspaceClient
    ) -> None:
        fake_cli.deployed["pipeline_job"] = fake_ws.jobs.add(pipeline_settings())
        source = 'def test_job(lakeflow):\n    lakeflow.job("pipeline_job").run(full_refresh=True)\n'
        run_tests(pytester, source).assert_outcomes(passed=1)
        assert [c["pipeline_params"] for c in fake_ws.jobs.run_now_calls] == [jobs.PipelineParams(full_refresh=True)]

    @NEEDS_REFRESH_SELECTION
    def test_run_passes_pipeline_refresh_selections(
        self, pytester: pytest.Pytester, fake_cli: FakeCli, fake_ws: FakeWorkspaceClient
    ) -> None:
        fake_cli.deployed["pipeline_job"] = fake_ws.jobs.add(pipeline_settings())
        result = run_tests(
            pytester,
            """
            def test_job(lakeflow):
                with lakeflow.job("pipeline_job") as job:
                    job.run(refresh_selection=["bronze"])
                    job.run(full_refresh_selection=["silver"], only=["refresh"])
            """,
        )
        result.assert_outcomes(passed=1)
        assert [c["pipeline_params"] for c in fake_ws.jobs.run_now_calls] == [
            jobs.PipelineParams(refresh_selection=["bronze"]),
            jobs.PipelineParams(full_refresh_selection=["silver"]),
        ]
        assert [c["only"] for c in fake_ws.jobs.run_now_calls] == [None, ["refresh"]]
        assert [c["job_parameters"] for c in fake_ws.jobs.run_now_calls] == [None, None]

    def test_pipeline_refresh_options_need_a_pipeline_task(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        result = run_tests(
            pytester,
            """
            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    with pytest.raises(LakeflowError, match="no pipeline task.*'extract', 'load'"):
                        job.run(full_refresh=True)
            """,
        )
        result.assert_outcomes(passed=1)
        assert fake_ws.jobs.run_now_calls == []


class TestTimeouts:
    """The run timeout, the job's `timeout_seconds` and cancelling on expiry."""

    @pytest.mark.parametrize(
        ("arguments", "call"),
        [
            pytest.param(None, "job.run(timeout=timedelta(0))", id="run"),
            pytest.param("timeout=timedelta(0)", "job.run()", id="lakeflow"),
        ],
    )
    def test_timeout_cancels_the_run(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, arguments: str | None, call: str
    ) -> None:
        fake_ws.jobs.queue(make_run(77, {"t": "SUCCESS"}), polls=100)
        if arguments is not None:
            configure_lakeflow(pytester, arguments)
        result = run_tests(
            pytester,
            f"""
            from datetime import timedelta

            import pytest

            from pytest_databricks_lakeflow import LakeflowTimeout

            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    with pytest.raises(LakeflowTimeout, match="Run 77 of 'my_job' did not finish within 0:00:00"):
                        {call}
                assert lakeflow.ws.jobs.cancelled == [77]
                assert lakeflow.active_runs == set()
            """,
        )
        result.assert_outcomes(passed=1)

    @pytest.mark.parametrize(
        ("arguments", "job_timeout"),
        [pytest.param(None, 1860, id="default"), pytest.param("timeout=timedelta(minutes=1)", 120, id="one-minute")],
    )
    def test_run_timeout_longer_than_the_job_fails_clearly(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, arguments: str | None, job_timeout: int
    ) -> None:
        if arguments is not None:
            configure_lakeflow(pytester, arguments)
        result = run_tests(
            pytester,
            f"""
            from datetime import timedelta

            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    with pytest.raises(LakeflowError, match=r"Lakeflow\\(timeout") as e:
                        job.run(timeout=timedelta(hours=1))
                assert "1:00:00" in str(e.value)
                assert "{timedelta(seconds=job_timeout)}" in str(e.value)
            """,
        )
        result.assert_outcomes(passed=1)
        assert fake_ws.jobs.run_now_calls == []
        assert fake_ws.jobs.created[0]["timeout_seconds"] == job_timeout

    def test_run_timeout_within_the_job_timeout_runs(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        result = run_tests(
            pytester,
            """
            from datetime import timedelta

            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    assert job.run(timeout=timedelta(minutes=31)).succeeded
            """,
        )
        result.assert_outcomes(passed=1)
        assert fake_ws.jobs.created[0]["timeout_seconds"] == 1860


class TestReportSection:
    """The `lakeflow` report section and the junit properties behind it."""

    def test_report_section_and_junit_properties(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        run = make_run(77, {"extract": "SUCCESS"})
        run.job_parameters = [jobs.JobParameter(name="param1", default="hello", value="hi")]
        fake_ws.jobs.queue(run)
        result = run_tests(
            pytester,
            """
            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    job.run()
                assert False
            """,
            "--junitxml=report.xml",
        )
        result.stdout.fnmatch_lines(
            [
                "*- lakeflow -*",
                f"lakeflow_job_url: {HOST}/jobs/1001",
                f"lakeflow_run_url: {HOST}/jobs/runs/77",
            ]
        )
        result.stdout.no_fnmatch_line("*lakeflow_params*")
        result.stdout.no_fnmatch_line("*lakeflow_task_params*")
        xml = (pytester.path / "report.xml").read_text()
        assert f'<property name="lakeflow_job_url" value="{HOST}/jobs/1001"' in xml
        assert f'<property name="lakeflow_run_url" value="{HOST}/jobs/runs/77"' in xml
        assert '<property name="lakeflow_job_id" value="1001"' in xml
        assert '<property name="lakeflow_run_id" value="77"' in xml
        assert '<property name="lakeflow_result_state" value="SUCCESS"' in xml
        assert re.search(r'<property name="lakeflow_duration" value="\d+\.\d{3}"', xml)
        assert "lakeflow_failing" not in xml
        assert "lakeflow_params" not in xml

    def test_failing_run_adds_a_failing_property(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        error = jobs.RunOutput(error="ValueError: no rows\nmore detail")
        fake_ws.jobs.queue(make_run(77, {"extract": "FAILED"}, "FAILED", "RUN_EXECUTION_ERROR"), {7700: error})
        result = run_tests(
            pytester,
            """
            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    assert job.run().failed
            """,
            "--junitxml=report.xml",
        )
        result.assert_outcomes(passed=1)
        xml = (pytester.path / "report.xml").read_text()
        assert '<property name="lakeflow_run_id" value="77"' in xml
        assert '<property name="lakeflow_result_state" value="FAILED"' in xml
        assert '<property name="lakeflow_duration" value="5.000"' in xml
        assert '<property name="lakeflow_failing" value="extract: ValueError: no rows"' in xml

    def test_failed_run_reports_parameters(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        run = make_run(77, {"extract": "FAILED"}, "FAILED", "RUN_EXECUTION_ERROR")
        run.job_parameters = [jobs.JobParameter(name="param1", default="hello", value="hi")]
        fake_ws.jobs.resolved[7700] = jobs.ResolvedValues(
            notebook_task=jobs.ResolvedNotebookTaskValues(
                base_parameters={"param1": "hi", "limit": "10", "__pytest_task_key": "extract"}
            )
        )
        fake_ws.jobs.queue(run, {7700: jobs.RunOutput(error="ValueError: no rows")})
        result = run_tests(
            pytester,
            """
            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    job.run()
                assert False
            """,
            "--junitxml=report.xml",
        )
        result.stdout.fnmatch_lines(
            [
                "*- lakeflow -*",
                '*lakeflow_params: {"param1": "hi"}*',
                '*lakeflow_task_params: {"extract": {"limit": "10", "param1": "hi"}}*',
            ]
        )
        xml = (pytester.path / "report.xml").read_text()
        assert '<property name="lakeflow_params" value="{&quot;param1&quot;: &quot;hi&quot;}"' in xml
        expected = "{&quot;extract&quot;: {&quot;limit&quot;: &quot;10&quot;, &quot;param1&quot;: &quot;hi&quot;}}"
        assert f'<property name="lakeflow_task_params" value="{expected}"' in xml

    def test_report_section_includes_notebook_stdout(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        for _ in range(2):
            run = make_run(77, {"extract": "SUCCESS", "transform": "FAILED"}, "FAILED", notebooks={"transform"})
            fake_ws.jobs.queue(run, {7701: jobs.RunOutput(error="PermissionError: forbidden")})
        fake_ws.jobs.exports[7701] = (DATA / "notebook_export_failed.html").read_text()
        result = run_tests(
            pytester,
            """
            def test_expects_the_failure(lakeflow):
                with lakeflow.job("my_job") as job:
                    assert job.run().failed

            def test_fails(lakeflow):
                with lakeflow.job("my_job") as job:
                    job.run()
                assert False
            """,
            "-rA",
        )
        result.assert_outcomes(passed=1, failed=1)
        result.stdout.fnmatch_lines(
            [
                "*_ test_fails _*",
                "*- lakeflow -*",
                'transform: notebook.cells[2] failed: dbutils.jobs.taskValues.set(key="stage", value="validation")',
                "transform: notebook stdout:",
                "  transform: checking forbidden against /Volumes/example/raw/landing",
                "  transform: 7 rows expected",
            ]
        )
        assert result.stdout.str().count("transform: checking forbidden") == 1

    def test_section_is_attached_to_setup_failures(self, pytester: pytest.Pytester) -> None:
        result = run_tests(
            pytester,
            """
            import pytest

            @pytest.fixture(scope="module")
            def job(lakeflow):
                with lakeflow.job("my_job") as job:
                    raise RuntimeError("setup broke")
                    yield job

            def test_job(job):
                pass
            """,
        )
        result.assert_outcomes(errors=1)
        result.stdout.fnmatch_lines(["*ERROR at setup*", "*- lakeflow -*", f"lakeflow_job_url: {HOST}/jobs/1001"])

    def test_raise_for_failure_fails_the_test_with_the_run_detail(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        fake_ws.jobs.queue(make_run(77, {"extract": "FAILED"}, "FAILED", "RUN_EXECUTION_ERROR"))
        result = run_tests(
            pytester,
            """
            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    job.run().raise_for_failure()
            """,
        )
        result.assert_outcomes(failed=1)
        result.stdout.fnmatch_lines(
            ["*LakeflowRunFailed: JobRun('my_job', FAILED*", "*extract: FAILED*/jobs/runs/7700*"]
        )


class TestProgress:
    """The task transition lines logged while polling and shown in the report section."""

    def test_progress_is_logged_per_task_transition(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        queue_transform_failure(fake_ws)
        source = 'def test_job(lakeflow):\n    assert lakeflow.job("my_job").run().failed\n'
        result = run_tests(pytester, source, "--log-cli-level=INFO")
        result.assert_outcomes(passed=1)
        result.stdout.fnmatch_lines([f"*INFO*pytest_databricks_lakeflow*{line}" for line in PROGRESS_LINES])
        for line in PROGRESS_LINES:
            assert sum(out.endswith(f" {line}") for out in result.stdout.lines) == 1, line
        assert sorted(fake_ws.jobs.output_calls) == [7700, 7701, 7702]

    def test_task_terminated_at_the_first_poll_logs_one_line(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        fake_ws.jobs.queue_polls([make_run(77, {"extract": "SUCCESS"})])
        source = 'def test_job(lakeflow):\n    lakeflow.job("my_job").run()\n'
        result = run_tests(pytester, source, "--log-cli-level=INFO")
        result.assert_outcomes(passed=1)
        progress = [line for line in result.stdout.lines if " extract: " in line]
        assert len(progress) == 1
        assert progress[0].endswith(" extract: SUCCESS in 0:00:02")
        assert fake_ws.jobs.output_calls == [7700]

    def test_late_result_state_does_not_log_a_second_line(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        details = jobs.TerminationDetails(code=jobs.TerminationCodeCode.RUN_EXECUTION_ERROR)
        status = jobs.RunStatus(state=jobs.RunLifecycleStateV2State.TERMINATED, termination_details=details)
        task = jobs.RunTask(task_key="transform", run_id=7700, status=status, start_time=1_000, end_time=3_000)
        running = jobs.Run(run_id=77, status=jobs.RunStatus(state=jobs.RunLifecycleStateV2State.RUNNING), tasks=[task])
        fake_ws.jobs.queue_polls([running, make_run(77, {"transform": "FAILED"}, "FAILED", "RUN_EXECUTION_ERROR")])
        source = 'def test_job(lakeflow):\n    assert lakeflow.job("my_job").run().failed\n'
        result = run_tests(pytester, source, "--log-cli-level=INFO")
        result.assert_outcomes(passed=1)
        progress = [line for line in result.stdout.lines if " transform: " in line and "INFO" in line]
        assert len(progress) == 1
        assert progress[0].endswith(" transform: RUN_EXECUTION_ERROR in 0:00:02")
        assert fake_ws.jobs.output_calls == [7700]

    def test_progress_falls_back_to_the_legacy_state(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        legacy = jobs.RunState(life_cycle_state=jobs.RunLifeCycleState.RUNNING)
        tasks = [jobs.RunTask(task_key="extract", run_id=7700, state=legacy), jobs.RunTask(task_key="bare")]
        running = jobs.Run(run_id=77, state=jobs.RunState(life_cycle_state=jobs.RunLifeCycleState.RUNNING), tasks=tasks)
        fake_ws.jobs.queue_polls([running, make_run(77, {"extract": "SUCCESS"})])
        source = 'def test_job(lakeflow):\n    lakeflow.job("my_job").run()\n'
        result = run_tests(pytester, source, "--log-cli-level=INFO")
        result.assert_outcomes(passed=1)
        result.stdout.fnmatch_lines(["*INFO* extract: RUNNING", "*INFO* extract: RUNNING -> SUCCESS in 0:00:02"])
        assert not [line for line in result.stdout.lines if " bare: " in line]
        assert fake_ws.jobs.output_calls == [7700]

    @pytest.mark.parametrize(
        ("source", "flags", "outcome", "heading"),
        [
            pytest.param(
                'def test_job(lakeflow):\n    lakeflow.job("my_job").run()\n    assert False\n',
                [],
                {"failed": 1},
                "*FAILURES*",
                id="failed",
            ),
            pytest.param(
                'def test_job(lakeflow):\n    assert lakeflow.job("my_job").run().failed\n',
                ["-rA"],
                {"passed": 1},
                "*PASSES*",
                id="passed-with-rA",
            ),
        ],
    )
    def test_progress_lines_in_report_section(
        self,
        pytester: pytest.Pytester,
        fake_ws: FakeWorkspaceClient,
        source: str,
        flags: list[str],
        outcome: dict[str, int],
        heading: str,
    ) -> None:
        queue_transform_failure(fake_ws)
        result = run_tests(pytester, source, *flags)
        result.assert_outcomes(**outcome)
        result.stdout.fnmatch_lines(
            [
                heading,
                "*- lakeflow -*",
                f"lakeflow_job_url: {HOST}/jobs/1001",
                f"lakeflow_run_url: {HOST}/jobs/runs/77",
                "lakeflow_failing: transform: ValueError: no rows",
                *PROGRESS_LINES,
            ]
        )

    def test_timeout_keeps_progress_in_the_report(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        fake_ws.jobs.queue_polls(
            [running_run(77, {"extract": "SUCCESS", "transform": "RUNNING"}), make_run(77, {"extract": "SUCCESS"})]
        )
        source = """
            from datetime import timedelta

            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    job.run(timeout=timedelta(0))
        """
        result = run_tests(pytester, source)
        result.assert_outcomes(failed=1)
        assert fake_ws.jobs.cancelled == [77]
        result.stdout.fnmatch_lines(
            [
                "*LakeflowTimeout: Run 77 of 'my_job' did not finish within 0:00:00*",
                "*- lakeflow -*",
                f"lakeflow_run_url: {HOST}/jobs/runs/77",
                "extract: SUCCESS in 0:00:02",
                "transform: RUNNING",
            ]
        )
        assert fake_ws.jobs.output_calls == [7700]


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


@pytest.mark.usefixtures("parent")
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
        ):
            assert hasattr(pytest_databricks_lakeflow, name)
            assert name in pytest_databricks_lakeflow.__all__
        assert issubclass(pytest_databricks_lakeflow.LakeflowRunFailed, pytest_databricks_lakeflow.LakeflowError)

    def test_pipeline_refresh_options_are_keyword_only(self) -> None:
        parameters = inspect.signature(EphemeralJob.run).parameters
        names = ("full_refresh", "full_refresh_selection", "refresh_selection")
        assert all(parameters[name].kind is inspect.Parameter.KEYWORD_ONLY for name in names)
