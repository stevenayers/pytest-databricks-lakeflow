from __future__ import annotations

import inspect
import json
import re
from collections.abc import Callable
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest
from databricks.sdk.errors import PermissionDenied
from databricks.sdk.service import compute, jobs
from databricks.sdk.service.workspace import Language

from pytest_databricks_lakeflow import ephemeral
from pytest_databricks_lakeflow.ephemeral import EphemeralJob
from pytest_databricks_lakeflow.report import directory_name

from .conftest import GITHUB, NEEDS_REFRESH_SELECTION, FakeCli, configure_lakeflow, make_repository, set_ci
from .fakes import (
    DATA,
    HOST,
    FakeWorkspaceClient,
    branching_settings,
    example_settings,
    make_run,
    parent_settings,
    pipeline_settings,
    queue_failed_run,
    reporting_settings,
    running_run,
    skipped_run,
    sp_rule_set,
)
from .test_build_job_run import QUERY_FILE, SQL_FILE_OUTPUT

if TYPE_CHECKING:
    from pytest_databricks_lakeflow import Lakeflow

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
ARTIFACT_PATH = "/Workspace/b/artifacts"
UPLOADED_WHEEL = f"{ARTIFACT_PATH}/.internal/pkg-0.1-py3-none-any.whl"
SP_USER = "roles/servicePrincipal.user"
DENIED = "Cannot bind the service principal provided in 'run_as' field to the job."
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


def run_tests(pytester: pytest.Pytester, source: str, *args: str) -> pytest.RunResult:
    pytester.makepyfile(test_jobs=source)
    return pytester.runpytest(*args)


def deny_create(**kwargs: object) -> jobs.CreateResponse:
    raise PermissionDenied(DENIED)


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


def queue_until_cancelled(ws: FakeWorkspaceClient) -> None:
    """Queue run 77, which keeps running for 100 polls unless it is cancelled, and then ends `CANCELED`."""
    cancelled = make_run(77, {"t": "CANCELED"}, result_state="CANCELED", code="USER_CANCELED")
    ws.jobs.queue(make_run(77, {"t": "SUCCESS"}), polls=100, after_cancel=cancelled)


def record_finished_runs(pytester: pytest.Pytester) -> None:
    """Configure a `run_finished` callback that appends each run id to `conftest.finished`."""
    extra = "\nfinished = []\n\n\ndef record(run, item):\n    finished.append(run.run_id)\n"
    configure_lakeflow(pytester, "run_finished=record", extra=extra)


def serverless_template(ws: FakeWorkspaceClient, cli: FakeCli) -> None:
    """Make `my_job` serverless only, deployed and validated, with its artifacts under `ARTIFACT_PATH`."""
    settings = example_settings()
    settings.tasks = [task for task in settings.tasks or [] if task.task_key != "classic"]
    settings.job_clusters = None
    ws.jobs.settings[cli.deployed["my_job"]] = settings
    workspace = {"artifact_path": ARTIFACT_PATH}
    cli.summary = {"workspace": workspace}
    cli.validate = {"workspace": workspace, "resources": {"jobs": {"my_job": settings.as_dict()}}}


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


class TestRunHandle:
    """Starting a run without waiting, then waiting for it or cancelling it."""

    def test_start_and_wait_two_jobs(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        fake_ws.jobs.queue(make_run(77, {"t": "SUCCESS"}))
        fake_ws.jobs.queue(make_run(78, {"t": "SUCCESS"}))
        result = run_tests(
            pytester,
            """
            from pytest_databricks_lakeflow import RunHandle

            def test_job(lakeflow):
                with lakeflow.job("my_job") as first, lakeflow.job("my_job") as second:
                    handle = first.start(params={"param1": "hi"})
                    other = second.start()
                    assert isinstance(handle, RunHandle)
                    assert (handle.job, handle.run_id, other.job, other.run_id) == (first, 77, second, 78)
                    assert lakeflow.active_runs == {77, 78}
                    run = handle.wait()
                    run2 = other.wait()
                    assert run.succeeded and run2.succeeded
                    assert (first.runs, second.runs) == ([run], [run2])
                    assert lakeflow.active_runs == set()
            """,
        )
        result.assert_outcomes(passed=1)
        assert [call["job_parameters"] for call in fake_ws.jobs.run_now_calls] == [{"param1": "hi"}, None]
        assert fake_ws.jobs.cancelled == []

    def test_handle_cancel_removes_the_active_run(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        fake_ws.jobs.queue(make_run(77, {"t": "SUCCESS"}), polls=100)
        result = run_tests(
            pytester,
            f"""
            def test_job(lakeflow, request):
                with lakeflow.job("my_job") as job:
                    handle = job.start()
                    assert handle.url == "{HOST}/jobs/runs/77"
                    assert ("lakeflow_run_url", handle.url) in request.node.user_properties
                    handle.cancel()
                    assert lakeflow.ws.jobs.cancelled == [77]
                    assert lakeflow.active_runs == set()
                assert lakeflow.ws.jobs.cancelled == [77]
            """,
        )
        result.assert_outcomes(passed=1)

    @pytest.mark.parametrize(
        "source",
        [
            pytest.param(
                'def test_job(lakeflow):\n    with lakeflow.job("my_job") as job:\n        job.start()\n',
                id="with-exit",
            ),
            # An interrupted test does not reach its teardown, so the run is still active at session end.
            pytest.param(
                'def test_job(lakeflow):\n    lakeflow.job("my_job").start()\n    raise KeyboardInterrupt\n',
                id="session-end",
            ),
        ],
    )
    def test_unwaited_handle_is_cancelled(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, source: str
    ) -> None:
        fake_ws.jobs.queue(make_run(77, {"t": "SUCCESS"}), polls=100)
        pytester.makepyfile(test_jobs=source)
        pytester.runpytest(no_reraise_ctrlc=True)
        assert fake_ws.jobs.cancelled == [77]
        assert fake_ws.jobs.get_run_calls == [{"run_id": 77}]

    def test_wait_twice_returns_the_same_run_and_calls_run_finished_once(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        fake_ws.jobs.queue(make_run(77, {"t": "SUCCESS"}))
        record_finished_runs(pytester)
        result = run_tests(
            pytester,
            """
            import conftest

            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    handle = job.start()
                    assert handle.wait() is handle.wait()
                    assert len(job.runs) == 1
                assert handle.wait() is job.runs[0]
                assert conftest.finished == [77]
            """,
        )
        result.assert_outcomes(passed=1)

    @pytest.mark.parametrize(
        ("elapsed", "timeout", "slept"),
        [(1, "None", [5.0]), (10, "timedelta(0)", [])],
        ids=["fresh-snapshot", "stale-snapshot"],
    )
    def test_wait_polls_at_once_when_the_start_snapshot_is_older_than_the_poll_interval(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, elapsed: int, timeout: str, slept: list[float]
    ) -> None:
        fake_ws.jobs.queue(make_run(77, {"t": "SUCCESS"}))
        configure_lakeflow(pytester, "poll_interval=timedelta(seconds=5)")
        result = run_tests(
            pytester,
            f"""
            from datetime import timedelta

            from pytest_databricks_lakeflow import ephemeral

            def test_job(lakeflow, monkeypatch):
                clock = [0.0]
                slept = []

                def sleep(seconds):
                    slept.append(seconds)
                    clock[0] += seconds

                monkeypatch.setattr(ephemeral.time, "monotonic", lambda: clock[0])
                monkeypatch.setattr(ephemeral.time, "sleep", sleep)
                with lakeflow.job("my_job") as job:
                    handle = job.start()
                    clock[0] += {elapsed}
                    assert handle.wait(timeout={timeout}).succeeded
                assert slept == {slept}
            """,
        )
        result.assert_outcomes(passed=1)
        assert [call for call in fake_ws.jobs.get_run_calls if call["run_id"] == 77] == [{"run_id": 77}] * 2
        assert fake_ws.jobs.cancelled == []

    def test_wait_after_the_job_closed_fails_clearly(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        fake_ws.jobs.queue(make_run(77, {"t": "SUCCESS"}), polls=100)
        result = run_tests(
            pytester,
            """
            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    handle = job.start()
                with pytest.raises(LakeflowError, match="The ephemeral job for 'my_job' has already been closed"):
                    handle.wait()
            """,
        )
        result.assert_outcomes(passed=1)
        assert fake_ws.jobs.cancelled == [77]


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
                with pytest.raises(LakeflowError, match="reserved"):
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
        ("call", "error"),
        [
            pytest.param(
                'lakeflow.job("my_job", task_params={"transform": {"schema": "s"}, "nope": {"x": "1"}})',
                "Unknown task 'nope'",
                id="job_unknown_task",
            ),
            pytest.param(
                'lakeflow.job("my_job", task_params={"transform": {"schema": 1}})',
                "must be a mapping of names to strings",
                id="job_wrong_shape",
            ),
            pytest.param(
                'lakeflow.task("my_job.transform", task_params={"transform": {"schema": 1}})',
                "must be a mapping of names to strings",
                id="task_wrong_shape",
            ),
        ],
    )
    def test_rejected_task_params_give_no_collision_warning(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, call: str, error: str
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
                    with pytest.raises(LakeflowError, match={error!r}):
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
                with pytest.raises(LakeflowError, match="notebook_task.*timeout_seconds"):
                    lakeflow.job("my_job", task_overrides={"load": {"notebook_task": {}}})

            def test_stub_key(lakeflow):
                with pytest.raises(LakeflowError, match=r"name 'load', the task under test, not \\['transform'\\]"):
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


ON_DEFAULT = ("environment_key", "default")


def unreached_warning(keys: list[str]) -> str:
    """The warning that `extra_dependencies=["faker==30.0.0"]` does not reach `keys` of `my_job`, as a fnmatch line."""
    message = (
        f"extra_dependencies ['faker==30.0.0'] do not reach tasks {keys} of job 'my_job': neither the environment "
        "nor the cluster libraries of these notebook tasks list them, and a serverless notebook task without an "
        "environment_key has neither. Install them with %pip install in the notebook, or run the task on an "
        "environment that lists them."
    )
    # fnmatch reads "[" as the start of a character class.
    return "*" + message.replace("[", "[[]") + "*"


def move_tasks(**changes: tuple[str, str]) -> str:
    """A `modify_settings` callback, `move`, that sets the `(field, value)` given for each task key on that task."""
    lines = ["", "", "def move(settings, item):", "    for task in settings.tasks:"]
    for key, (field, value) in changes.items():
        lines += [f"        if task.task_key == {key!r}:", f"            task.{field} = {value!r}"]
    return "\n".join(lines) + "\n"


class TestUnreachedDependencies:
    """The warning for test-only packages that notebook tasks do not get."""

    @pytest.mark.parametrize(
        ("modify", "call", "expected"),
        [
            pytest.param(None, 'lakeflow.job("my_job", {})', ["extract", "transform"], id="job"),
            pytest.param(None, 'lakeflow.task("my_job.extract", {})', ["extract"], id="task"),
            pytest.param(
                None,
                'lakeflow.job("my_job", setup=["SELECT 1"], teardown=["SELECT 2"], {})',
                ["extract", "transform"],
                id="setup-and-teardown",
            ),
            pytest.param(
                None,
                'lakeflow.task("my_job.transform", upstream_task_values={{"extract": {{"row_count": 3}}}}, {})',
                ["transform"],
                id="task-with-stub",
            ),
            pytest.param(
                move_tasks(transform=("environment_key", "default"), extract=("job_cluster_key", "classic")),
                'lakeflow.job("my_job", {})',
                ["extract"],
                id="modify-settings",
            ),
        ],
    )
    def test_warns_once_naming_the_tasks(
        self,
        pytester: pytest.Pytester,
        fake_ws: FakeWorkspaceClient,
        modify: str | None,
        call: str,
        expected: list[str],
    ) -> None:
        if modify is not None:
            configure_lakeflow(pytester, "modify_settings=move", modify)
        source = call.format('extra_dependencies=["faker==30.0.0"]')
        result = run_tests(pytester, f"def test_job(lakeflow):\n    with {source}:\n        pass\n")
        result.assert_outcomes(passed=1, warnings=1)
        result.stdout.fnmatch_lines([unreached_warning(expected)])
        assert len(fake_ws.jobs.created) == 1

    @pytest.mark.parametrize(
        ("environments", "key"),
        [({"test": {"environment_version": "4"}}, "test"), ({"default": {"dependencies": ["pkg==1.0"]}}, "default")],
        ids=["new", "replaced"],
    )
    def test_names_tasks_on_an_environment_without_the_packages(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, environments: dict[str, Any], key: str
    ) -> None:
        on_environment = ("environment_key", key)
        move = move_tasks(extract=on_environment, transform=on_environment)
        configure_lakeflow(pytester, "modify_settings=move", move)
        call = f'lakeflow.job("my_job", extra_dependencies=["faker==30.0.0"], environments={environments!r})'
        result = run_tests(pytester, f"def test_job(lakeflow):\n    with {call}:\n        pass\n")
        result.assert_outcomes(passed=1, warnings=1)
        result.stdout.fnmatch_lines([unreached_warning(["extract", "transform"])])

    @pytest.mark.parametrize(
        ("modify", "call"),
        [
            pytest.param(None, 'lakeflow.task("my_job.load", extra_dependencies=["faker==30.0.0"])', id="wheel-task"),
            pytest.param(None, 'lakeflow.task("my_job.classic", extra_dependencies=["faker==30.0.0"])', id="cluster"),
            pytest.param(None, 'lakeflow.job("my_job")', id="no-dependencies"),
            pytest.param(
                None,
                'lakeflow.job("my_job", extra_dependencies=["faker==30.0.0"], disable=["extract", "transform"])',
                id="disabled",
            ),
            pytest.param(
                move_tasks(extract=ON_DEFAULT, transform=ON_DEFAULT),
                'lakeflow.job("my_job", extra_dependencies=["faker==30.0.0"])',
                id="on-default",
            ),
        ],
    )
    def test_no_warning_when_every_task_is_reached(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, modify: str | None, call: str
    ) -> None:
        if modify is not None:
            configure_lakeflow(pytester, "modify_settings=move", modify)
        result = run_tests(pytester, f"def test_job(lakeflow):\n    with {call}:\n        pass\n")
        result.assert_outcomes(passed=1)
        assert "warnings" not in result.parseoutcomes()


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

    def test_tasks_downstream_of_a_disabled_task_are_disabled(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        states = {"extract": "DISABLED", "transform": "DISABLED", "load": "DISABLED", "classic": "SUCCESS"}
        fake_ws.jobs.queue(make_run(77, states))
        result = run_tests(
            pytester,
            """
            import pytest

            def test_job(lakeflow):
                run = lakeflow.job("my_job", disable=["extract"]).run()
                assert sorted(run.disabled_tasks) == ["extract", "load", "transform"]
                assert run.skipped_tasks == {}
                assert run.succeeded
                assert run.executed_tasks == ["classic"]
                with pytest.raises(KeyError, match=r"Disabled in the job: \\['extract', 'load', 'transform'\\]\\."):
                    run.task("transform")
            """,
        )
        result.assert_outcomes(passed=1)

    def test_tasks_left_out_by_only_stay_skipped(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        states = {"extract": "DISABLED", "transform": "DISABLED", "load": "DISABLED", "classic": "SUCCESS"}
        fake_ws.jobs.queue(make_run(77, states))
        result = run_tests(
            pytester,
            """
            import pytest

            def test_job(lakeflow):
                run = lakeflow.job("my_job").run(only=["classic"])
                assert sorted(run.skipped_tasks) == ["extract", "load", "transform"]
                assert run.disabled_tasks == {}
                with pytest.raises(KeyError, match=r"Skipped by only: \\['extract', 'load', 'transform'\\]"):
                    run.task("transform")
            """,
        )
        result.assert_outcomes(passed=1)

    def test_a_run_with_every_task_disabled_ran_nothing(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        keys = ["extract", "transform", "load", "classic"]
        fake_ws.jobs.queue(skipped_run(77, keys, notebooks=["extract", "transform", "classic"]))
        result = run_tests(
            pytester,
            """
            import pytest

            from pytest_databricks_lakeflow import LakeflowRunFailed

            def test_job(lakeflow):
                run = lakeflow.job("my_job", disable=["extract", "classic"]).run()
                assert run.tasks == {}
                assert sorted(run.disabled_tasks) == ["classic", "extract", "load", "transform"]
                assert not any(task.ran for task in run.disabled_tasks.values())
                assert run.executed_tasks == []
                assert not run.succeeded
                with pytest.raises(LakeflowRunFailed) as raised:
                    run.raise_for_failure()
                reason = (
                    "Skipped because every task is disabled. Re-enable at least one task to be able to run the job."
                )
                assert f"run: {reason}" in str(raised.value)
            """,
        )
        result.assert_outcomes(passed=1)
        assert fake_ws.jobs.output_calls == []
        assert fake_ws.jobs.export_calls == []

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


class TestForcedConditions:
    """Forcing the outcome of if/else condition tasks in the copy."""

    def test_forced_condition_takes_the_chosen_branch(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli
    ) -> None:
        fake_cli.deployed["branching_job"] = fake_ws.jobs.add(branching_settings())
        states = {"check_input": "SUCCESS", "check": "SUCCESS", "on_true": "SUCCESS", "on_false": "EXCLUDED"}
        fake_ws.jobs.queue(make_run(77, states, condition_outcomes={"check": "true"}))
        result = run_tests(
            pytester,
            """
            def test_job(lakeflow):
                run = lakeflow.job("branching_job", params={"rows": "0"}, force_conditions={"check": True}).run()
                assert run.branch_taken("check") is True
                assert run.task("on_false").excluded
                assert run.executed_tasks == ["check", "check_input", "on_true"]
            """,
        )
        result.assert_outcomes(passed=1)
        [created] = fake_ws.jobs.created
        tasks = {t.task_key: t for t in created["tasks"]}
        forced = jobs.ConditionTask(op=jobs.ConditionTaskOp.EQUAL_TO, left="true", right="true")
        assert tasks["check"].condition_task == forced
        assert tasks["on_true"].depends_on == [jobs.TaskDependency(task_key="check", outcome="true")]

    def test_force_conditions_rejects_tasks_without_a_condition(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli
    ) -> None:
        fake_cli.deployed["branching_job"] = fake_ws.jobs.add(branching_settings())
        result = run_tests(
            pytester,
            """
            def test_job(lakeflow):
                lakeflow.job("branching_job", force_conditions={"check_input": True}).run()
            """,
        )
        result.assert_outcomes(failed=1)
        result.stdout.fnmatch_lines(["*Task 'check_input' in force_conditions has no condition_task*"])
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

    @pytest.mark.parametrize("only", [["transform"], ["transform+"]])
    def test_only_with_an_unresolved_reference_fails_before_run_now(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, only: list[str]
    ) -> None:
        result = run_tests(
            pytester,
            f"""
            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            def test_job(lakeflow):
                job = lakeflow.job("my_job")
                with pytest.raises(LakeflowError, match=r"refers to task values of \\['extract'\\]"):
                    job.run(only={only!r})
                job.run(only=["+transform"])
            """,
        )
        result.assert_outcomes(passed=1)
        assert [call["only"] for call in fake_ws.jobs.run_now_calls] == [["+transform"]]

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

    @pytest.mark.parametrize("method", ["run", "start"])
    def test_an_unknown_performance_target_is_rejected_before_the_job_is_created(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, method: str
    ) -> None:
        result = run_tests(
            pytester,
            f"""
            import pytest

            def test_job(lakeflow):
                job = lakeflow.job("my_job")
                with pytest.raises(ValueError, match="bogus"):
                    job.{method}(performance_target="bogus")
            """,
        )
        result.assert_outcomes(passed=1)
        assert (fake_ws.jobs.created, fake_ws.jobs.run_now_calls) == ([], [])

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
                    job.run(refresh_flow_selection=["bronze_flow"], reset_checkpoint_selection=["bronze"])
            """,
        )
        result.assert_outcomes(passed=1)
        assert [c["pipeline_params"] for c in fake_ws.jobs.run_now_calls] == [
            jobs.PipelineParams(refresh_selection=["bronze"]),
            jobs.PipelineParams(full_refresh_selection=["silver"]),
            jobs.PipelineParams(refresh_flow_selection=["bronze_flow"], reset_checkpoint_selection=["bronze"]),
        ]
        assert [c["only"] for c in fake_ws.jobs.run_now_calls] == [None, ["refresh"], None]
        assert [c["job_parameters"] for c in fake_ws.jobs.run_now_calls] == [None, None, None]

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
    """The poll interval, the run timeout, the job's `timeout_seconds` and cancelling on expiry."""

    @pytest.mark.parametrize(
        ("poll_interval", "sleeps"), [(timedelta(seconds=5), [5.0]), (None, [3.0])], ids=["set", "default"]
    )
    def test_poll_interval(
        self,
        fake_ws: FakeWorkspaceClient,
        monkeypatch: pytest.MonkeyPatch,
        poll_interval: timedelta | None,
        sleeps: list[float],
    ) -> None:
        slept: list[float] = []
        monkeypatch.setattr(ephemeral, "POLL_SECONDS", 3.0)
        monkeypatch.setattr(ephemeral.time, "sleep", slept.append)
        fake_ws.jobs.queue(make_run(77, {"t": "SUCCESS"}))
        fake_ws.jobs.run_now(1001)
        lakeflow = SimpleNamespace(
            ws=fake_ws, poll_interval=poll_interval, active_runs=set(), timeout=timedelta(minutes=1), run_finished=None
        )
        job = EphemeralJob(cast("Lakeflow", lakeflow), "my_job", jobs.JobSettings())
        item = SimpleNamespace(stash=pytest.Stash(), user_properties=[])
        run, _ = job._wait(77, timedelta(minutes=1), cast("pytest.Item", item))
        assert run.state is not None and run.state.result_state == jobs.RunResultState.SUCCESS
        assert slept == sleeps

    def test_timeout_message_lists_task_states(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        running = running_run(77, {"extract": "SUCCESS", "transform": "RUNNING", "load": "PENDING"})
        fake_ws.jobs.queue(make_run(77, {"t": "SUCCESS"}), polls=100, running=running)
        result = run_tests(
            pytester,
            """
            from datetime import timedelta

            import pytest

            from pytest_databricks_lakeflow import LakeflowTimeout

            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    with pytest.raises(LakeflowTimeout) as e:
                        job.run(timeout=timedelta(0))
                assert str(e.value).endswith("Tasks when cancelled: extract SUCCESS, transform RUNNING, load PENDING")
            """,
        )
        result.assert_outcomes(passed=1)
        assert fake_ws.jobs.cancelled == [77]

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
        ("call", "tasks"),
        [
            pytest.param('lakeflow.job("my_job")', 4, id="job"),
            pytest.param('lakeflow.task("my_job.load")', 1, id="task"),
        ],
    )
    def test_longer_run_timeout_raises_the_job_timeout(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, call: str, tasks: int
    ) -> None:
        configure_lakeflow(pytester, "timeout=timedelta(minutes=1)")
        result = run_tests(
            pytester,
            f"""
            import logging
            from datetime import timedelta

            def test_job(lakeflow, caplog):
                caplog.set_level(logging.INFO, logger="pytest_databricks_lakeflow")
                with {call} as job:
                    job.run(timeout=timedelta(hours=1))
                    job.run(timeout=timedelta(hours=1))
                    settings = lakeflow.ws.jobs.get(job.job_id).settings
                assert settings.timeout_seconds == 3660
                assert [t.timeout_seconds for t in settings.tasks] == [None] * {tasks}
                [record] = [r for r in caplog.records if "timeout_seconds" in r.getMessage()]
                assert record.levelname == "INFO"
                assert record.getMessage() == (
                    "Raised timeout_seconds of job 1001 from 120 to 3660 for a run timeout of 1:00:00"
                )
            """,
        )
        result.assert_outcomes(passed=1)
        created = fake_ws.jobs.created[0]
        assert created["timeout_seconds"] == 120
        assert fake_ws.jobs.updated == [(1001, jobs.JobSettings(timeout_seconds=3660, tasks=created["tasks"]))]
        assert fake_ws.jobs.run_timeouts == [3660, 3660]

    @pytest.mark.parametrize(
        ("source", "limit", "first", "second"),
        [
            pytest.param("template", 600, ("timedelta(minutes=20)", "0:20:00", 1260), 1560, id="shorter-template"),
            pytest.param("modify_settings", 7200, ("timedelta(hours=3)", "3:00:00", 10860), 11160, id="longer-hook"),
        ],
    )
    def test_a_timeout_the_plugin_did_not_set_is_raised_with_a_warning(
        self,
        pytester: pytest.Pytester,
        fake_ws: FakeWorkspaceClient,
        source: str,
        limit: int,
        first: tuple[str, str, int],
        second: int,
    ) -> None:
        if source == "template":
            fake_ws.jobs.settings[1000].timeout_seconds = limit
        else:
            extra = f"\n\ndef lengthen(settings, item):\n    settings.timeout_seconds = {limit}\n"
            configure_lakeflow(pytester, "modify_settings=lengthen", extra=extra)
        timeout, shown, raised = first
        result = run_tests(
            pytester,
            f"""
            import logging
            from datetime import timedelta

            def test_job(lakeflow, caplog):
                with lakeflow.job("my_job") as job:
                    job.run(timeout={timeout})
                    job.run(timeout={timeout} + timedelta(minutes=5))
                warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
                assert warnings == [
                    "Raised timeout_seconds of job 1001 from {limit}, which the template or modify_settings set, to "
                    "{raised} for a run timeout of {shown}"
                ]
            """,
        )
        result.assert_outcomes(passed=1)
        assert fake_ws.jobs.created[0]["timeout_seconds"] == limit
        assert [settings.timeout_seconds for _, settings in fake_ws.jobs.updated if settings] == [raised, second]
        assert fake_ws.jobs.run_timeouts == [raised, second]

    def test_run_timeout_within_the_job_timeout_makes_no_update(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        configure_lakeflow(pytester, "timeout=timedelta(minutes=1)")
        result = run_tests(
            pytester,
            """
            from datetime import timedelta

            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    assert job.run(timeout=timedelta(seconds=10)).succeeded
                    assert job.run().succeeded
            """,
        )
        result.assert_outcomes(passed=1)
        assert fake_ws.jobs.created[0]["timeout_seconds"] == 120
        assert fake_ws.jobs.updated == []
        assert fake_ws.jobs.run_timeouts == [120, 120]

    def test_wait_timeout_longer_than_the_job_fails_clearly(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        configure_lakeflow(pytester, "timeout=timedelta(minutes=1)")
        result = run_tests(
            pytester,
            """
            from datetime import timedelta

            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    handle = job.start()
                    with pytest.raises(LakeflowError, match=r"wait\\(timeout=1:00:00\\) of 'my_job'") as e:
                        handle.wait(timeout=timedelta(hours=1))
                assert "timeout_seconds of 120 (0:02:00)" in str(e.value)
                assert "run(timeout=...)" in str(e.value)
                assert "start(timeout=...)" in str(e.value)
            """,
        )
        result.assert_outcomes(passed=1)
        assert fake_ws.jobs.updated == []

    def test_start_timeout_raises_the_job_timeout_before_the_run(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        # Lakeflow's timeout of zero would time the wait out, so a wait() that returns used start()'s timeout.
        configure_lakeflow(pytester, "timeout=timedelta(0)")
        fake_ws.jobs.queue(make_run(77, {"t": "SUCCESS"}), polls=3)
        result = run_tests(
            pytester,
            """
            from datetime import timedelta

            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    handle = job.start(timeout=timedelta(hours=1))
                    with pytest.raises(LakeflowError, match=r"wait\\(timeout=2:00:00\\) of 'my_job'"):
                        handle.wait(timeout=timedelta(hours=2))
                    assert handle.wait().succeeded
            """,
        )
        result.assert_outcomes(passed=1)
        created = fake_ws.jobs.created[0]
        assert created["timeout_seconds"] == 60
        assert fake_ws.jobs.updated == [(1001, jobs.JobSettings(timeout_seconds=3660, tasks=created["tasks"]))]
        assert fake_ws.jobs.run_timeouts == [3660]
        assert fake_ws.jobs.cancelled == []


class TestStopAfter:
    """`stop_after` cancels a run that does not end on its own and returns it as a cancelled `JobRun`."""

    @pytest.mark.parametrize(
        "call",
        [
            pytest.param("job.run(stop_after=timedelta(0))", id="run"),
            pytest.param("job.start(timeout=timedelta(minutes=5)).wait(stop_after=timedelta(0))", id="wait"),
        ],
    )
    def test_stop_after_returns_a_cancelled_run(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, call: str
    ) -> None:
        queue_until_cancelled(fake_ws)
        record_finished_runs(pytester)
        result = run_tests(
            pytester,
            f"""
            from datetime import timedelta

            import conftest

            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    run = {call}
                    assert (run.cancelled, run.result_state, run.failed) == (True, "CANCELED", True)
                    task = run.task("t")
                    assert (task.ran, task.error) == (False, None)
                    assert job.runs == [run]
                    assert lakeflow.active_runs == set()
                assert conftest.finished == [77]
            """,
        )
        result.assert_outcomes(passed=1)
        assert fake_ws.jobs.cancelled == [77]

    @pytest.mark.parametrize(
        ("call", "active", "started"),
        [
            pytest.param(
                "job.run(stop_after=timedelta(minutes=5), timeout=timedelta(minutes=1))", "set()", 0, id="run"
            ),
            # wait() learns stop_after only after the run started, so the run keeps going until the job closes.
            pytest.param(
                "job.start(timeout=timedelta(minutes=1)).wait(stop_after=timedelta(minutes=5))", "{77}", 1, id="wait"
            ),
        ],
    )
    def test_stop_after_longer_than_timeout_fails_early(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, call: str, active: str, started: int
    ) -> None:
        fake_ws.jobs.queue(make_run(77, {"t": "SUCCESS"}), polls=100)
        result = run_tests(
            pytester,
            f"""
            from datetime import timedelta

            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            def test_job(lakeflow):
                message = "stop_after=0:05:00 of 'my_job' is longer than its timeout of 0:01:00"
                with lakeflow.job("my_job") as job:
                    with pytest.raises(LakeflowError, match=message):
                        {call}
                    assert lakeflow.active_runs == {active}
                    assert lakeflow.ws.jobs.cancelled == []
            """,
        )
        result.assert_outcomes(passed=1)
        assert len(fake_ws.jobs.run_now_calls) == started

    def test_stop_after_then_timeout_still_raises(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        fake_ws.jobs.queue(make_run(77, {"t": "SUCCESS"}), polls=100)
        result = run_tests(
            pytester,
            """
            from datetime import timedelta

            import pytest

            from pytest_databricks_lakeflow import LakeflowTimeout

            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    with pytest.raises(LakeflowTimeout, match="Run 77 of 'my_job' did not finish within 0:00:00"):
                        job.run(stop_after=timedelta(0), timeout=timedelta(0))
                    assert job.runs == []
                    assert lakeflow.active_runs == set()
            """,
        )
        result.assert_outcomes(passed=1)
        assert fake_ws.jobs.cancelled == [77]


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


# Tests whose run the plugin cancels, at the wait() deadline or when the job closes, with the get_run calls made
# before the cancel.
CANCELLED_BY_THE_PLUGIN = [
    pytest.param(
        """
        from datetime import timedelta

        import conftest
        import pytest

        from pytest_databricks_lakeflow import LakeflowTimeout

        def test_job(lakeflow):
            with lakeflow.job("my_job") as job:
                with pytest.raises(LakeflowTimeout, match="did not finish within 0:00:00"):
                    job.run(timeout=timedelta(0))
                assert job.runs == []
            assert conftest.finished == []
        """,
        2,
        id="timeout",
    ),
    pytest.param(
        """
        import conftest

        def test_job(lakeflow):
            with lakeflow.job("my_job") as job:
                job.start()
            assert job.runs == []
            assert conftest.finished == []
        """,
        1,
        id="closed-job",
    ),
]

REPORTED_JOB = """
def test_job(lakeflow):
    with lakeflow.job("my_job", params={"param1": "hi"}) as job:
        job.run()

def test_without_a_run():
    pass
"""


def report_of(directory: Path, nodeid: str = "test_jobs.py::test_job") -> tuple[Path, dict[str, Any]]:
    """The report directory of the test `nodeid` under `directory`, and its report.json."""
    test_dir = directory / directory_name(nodeid)
    return test_dir, json.loads((test_dir / "report.json").read_text())


def block_the_report_dir(path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Put a file where `--lakeflow-report-dir=out` would make its directory under `path`."""
    (path / "out").write_text("a file, not a directory")


def break_serialise(path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Make serialising a run for the report raise an error that is not an `OSError`."""

    def broken(run: object) -> None:
        raise ValueError("cannot serialise")

    monkeypatch.setattr("pytest_databricks_lakeflow.report.serialise_with_files", broken)


class TestReportDir:
    """`--lakeflow-report-dir` writes a directory per test with a run: report.json, raw logs and exported notebooks."""

    def test_report_dir_writes_one_json_per_test(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        fake_ws.jobs.queue(make_run(77, {"extract": "SUCCESS", "load": "SUCCESS"}))
        pytester.makeconftest(
            """
            import json

            from pytest_databricks_lakeflow.ephemeral import RUNS

            def pytest_sessionfinish(session):
                held = [item.nodeid for item in session.items if RUNS in item.stash]
                with open("held.json", "w") as f:
                    json.dump(held, f)
            """
        )
        result = run_tests(pytester, REPORTED_JOB, "--lakeflow-report-dir=out")
        result.assert_outcomes(passed=2)
        assert json.loads((pytester.path / "held.json").read_text()) == [], "runs are dropped once written"
        out = pytester.path / "out"
        test_dir, report = report_of(out)
        assert [path.name for path in out.iterdir()] == [test_dir.name]
        assert report["nodeid"] == "test_jobs.py::test_job"
        assert report["outcome"] == {"setup": "passed", "call": "passed", "teardown": "passed"}
        [entry] = report["runs"]
        assert (entry["run_id"], entry["result_state"], entry["succeeded"]) == (77, "SUCCESS", True)
        assert (entry["job_key"], entry["job_id"], entry["job_url"]) == ("my_job", 1001, f"{HOST}/jobs/1001")
        assert entry["params"] == {"param1": "hi"}
        [created] = fake_ws.jobs.created
        assert entry["settings"]["name"] == created["name"]
        assert [task["task_key"] for task in entry["settings"]["tasks"]] == [task.task_key for task in created["tasks"]]
        assert sorted(entry["tasks"]) == ["extract", "load"]
        assert report["debug_notebooks"] == []

    @pytest.mark.parametrize(
        ("logs", "written"),
        [("hello", "hello"), ("lone \ud800 surrogate", "lone ? surrogate")],
        ids=["text", "lone-surrogate"],
    )
    def test_report_dir_writes_raw_logs(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, logs: str, written: str
    ) -> None:
        run = make_run(77, {"extract": "SUCCESS", "load": "SUCCESS"})
        fake_ws.jobs.queue(run, outputs={7701: jobs.RunOutput(logs=logs)})
        run_tests(pytester, REPORTED_JOB, "--lakeflow-report-dir=out").assert_outcomes(passed=2)
        test_dir, report = report_of(pytester.path / "out")
        assert (test_dir / "77" / "load.log").read_text(encoding="utf-8") == written
        assert [path.name for path in (test_dir / "77").iterdir()] == ["load.log"]
        tasks = report["runs"][0]["tasks"]
        assert (tasks["load"]["logs_file"], tasks["extract"]["logs_file"]) == ("77/load.log", None)

    def test_report_dir_writes_exported_notebooks(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        run = make_run(77, {"extract": "SUCCESS", "transform": "FAILED"}, "FAILED", notebooks={"transform"})
        fake_ws.jobs.queue(run, {7701: jobs.RunOutput(error="PermissionError: forbidden")})
        html = (DATA / "notebook_export_failed.html").read_text()
        fake_ws.jobs.exports[7701] = html
        source = """
            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    job.run()
                assert False
            """
        run_tests(pytester, source, "--lakeflow-report-dir=out").assert_outcomes(failed=1)
        test_dir, report = report_of(pytester.path / "out")
        assert report["outcome"]["call"] == "failed"
        assert (test_dir / "77" / "transform.html").read_text() == html
        assert report["runs"][0]["tasks"]["transform"]["notebook_file"] == "77/transform.html"

    def test_report_dir_is_relative_to_the_invocation_directory(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        fake_ws.jobs.queue(make_run(77, {"extract": "SUCCESS"}))
        pytester.makepyfile(test_jobs=REPORTED_JOB)
        invoked_from = pytester.mkdir("sub")
        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.chdir(invoked_from)
            result = pytester.runpytest(str(pytester.path / "test_jobs.py"), "--lakeflow-report-dir=out")
        result.assert_outcomes(passed=2)
        report_of(invoked_from / "out")
        assert not (pytester.path / "out").exists()

    def test_no_report_dir_writes_nothing(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        fake_ws.jobs.queue(make_run(77, {"extract": "SUCCESS", "load": "SUCCESS"}), {7701: jobs.RunOutput(logs="x")})
        before = set(pytester.path.rglob("*"))
        run_tests(pytester, REPORTED_JOB).assert_outcomes(passed=2)
        written = set(pytester.path.rglob("*")) - before
        assert not [path for path in written if path.name in ("report.json", "load.log")]

    @pytest.mark.parametrize(
        ("break_report", "error"),
        [
            pytest.param(block_the_report_dir, "*", id="unwritable"),
            pytest.param(break_serialise, "*: cannot serialise", id="unserialisable"),
        ],
    )
    def test_report_errors_never_fail_the_test(
        self,
        pytester: pytest.Pytester,
        fake_ws: FakeWorkspaceClient,
        monkeypatch: pytest.MonkeyPatch,
        break_report: Callable[[Path, pytest.MonkeyPatch], None],
        error: str,
    ) -> None:
        fake_ws.jobs.queue(make_run(77, {"extract": "SUCCESS"}))
        break_report(pytester.path, monkeypatch)
        result = run_tests(
            pytester, REPORTED_JOB, "--lakeflow-report-dir=out", "-o", "log_cli=true", "--log-cli-level=WARNING"
        )
        result.assert_outcomes(passed=2)
        assert result.ret == pytest.ExitCode.OK
        result.stdout.fnmatch_lines([f"*Could not write the lakeflow report of test_jobs.py::test_job to *out{error}"])

    def test_report_dir_drops_the_reports_of_earlier_sessions(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        fake_ws.jobs.queue(make_run(77, {"extract": "SUCCESS"}))
        out = pytester.path / "out"
        for nodeid in ("test_jobs.py::test_job", "test_jobs.py::test_removed"):
            earlier = out / directory_name(nodeid)
            (earlier / "70").mkdir(parents=True)
            (earlier / "70" / "load.log").write_text("from an earlier session")
            (earlier / "report.json").write_text("{}")
        (out / "notes.txt").write_text("not a report")
        (out / "kept-0123abcd").mkdir()
        run_tests(pytester, REPORTED_JOB, "--lakeflow-report-dir=out").assert_outcomes(passed=2)
        test_dir, report = report_of(out)
        assert [entry["run_id"] for entry in report["runs"]] == [77]
        assert sorted(path.name for path in out.iterdir()) == sorted([test_dir.name, "notes.txt", "kept-0123abcd"])
        assert not (test_dir / "70").exists()

    def test_report_dir_serialises_sql_results(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        fake_ws.jobs.queue(make_run(77, {"query": "SUCCESS"}, sql_files={"query": QUERY_FILE}))
        fake_ws.jobs.raw_outputs[7700] = SQL_FILE_OUTPUT
        run_tests(pytester, REPORTED_JOB, "--lakeflow-report-dir=out").assert_outcomes(passed=2)
        _, report = report_of(pytester.path / "out")
        assert report["runs"][0]["tasks"]["query"]["sql_result"] == {
            "task_key": "query",
            "columns": ["n"],
            "types": ["int"],
            "rows": [{"n": "1"}],
            "truncated": False,
            "warehouse_id": "0123456789abcdef",
            "query_text": "SELECT 1 AS n\n",
        }

    def test_a_run_cancelled_at_fixture_teardown_is_reported_with_the_test_torn_down(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        queue_until_cancelled(fake_ws)
        source = """
            import pytest

            @pytest.fixture(scope="module")
            def started(lakeflow):
                with lakeflow.job("my_job") as job:
                    job.start()
                    yield job

            def test_first(started):
                pass

            def test_last(started):
                pass
            """
        run_tests(pytester, source, "--lakeflow-report-dir=out").assert_outcomes(passed=2)
        assert fake_ws.jobs.cancelled == [77]
        out = pytester.path / "out"
        test_dir, report = report_of(out, "test_jobs.py::test_last")
        assert [path.name for path in out.iterdir()] == [test_dir.name]
        [entry] = report["runs"]
        assert (entry["run_id"], entry["result_state"], entry["succeeded"]) == (77, "CANCELED", False)

    def test_an_interrupt_while_reading_a_cancelled_run_still_deletes_the_job(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        queue_until_cancelled(fake_ws)
        get_run = fake_ws.jobs.get_run

        def interrupted_once_cancelled(run_id: int, **kwargs: Any) -> jobs.Run:
            if fake_ws.jobs.cancelled:
                raise KeyboardInterrupt
            return get_run(run_id, **kwargs)

        monkeypatch.setattr(fake_ws.jobs, "get_run", interrupted_once_cancelled)
        pytester.makepyfile(
            test_jobs='def test_job(lakeflow):\n    with lakeflow.job("my_job") as job:\n        job.start()\n'
        )
        result = pytester.runpytest("--lakeflow-report-dir=out", no_reraise_ctrlc=True)
        assert result.ret == pytest.ExitCode.INTERRUPTED
        assert fake_ws.jobs.cancelled == [77]
        assert fake_ws.jobs.deleted == [1001]

    @pytest.mark.parametrize(("source", "polls"), CANCELLED_BY_THE_PLUGIN)
    def test_a_run_the_plugin_cancels_is_reported(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, source: str, polls: int
    ) -> None:
        queue_until_cancelled(fake_ws)
        record_finished_runs(pytester)
        run_tests(pytester, source, "--lakeflow-report-dir=out").assert_outcomes(passed=1)
        assert fake_ws.jobs.cancelled == [77]
        _, report = report_of(pytester.path / "out")
        [entry] = report["runs"]
        assert (entry["run_id"], entry["result_state"], entry["succeeded"]) == (77, "CANCELED", False)
        assert entry["tasks"]["t"]["result_state"] == "CANCELED"

    @pytest.mark.parametrize(("source", "polls"), CANCELLED_BY_THE_PLUGIN)
    def test_without_the_flag_a_cancelled_run_is_not_polled_again(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, source: str, polls: int
    ) -> None:
        queue_until_cancelled(fake_ws)
        record_finished_runs(pytester)
        run_tests(pytester, source).assert_outcomes(passed=1)
        assert fake_ws.jobs.cancelled == [77]
        assert fake_ws.jobs.get_run_calls == [{"run_id": 77}] * polls
        assert not (pytester.path / "out").exists()

    def test_a_cancelled_run_that_does_not_end_is_left_out(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake_ws.jobs.queue(make_run(77, {"t": "SUCCESS"}), polls=100)
        monkeypatch.setattr(ephemeral, "TIMEOUT_GRACE_SECONDS", 0)
        source = 'def test_job(lakeflow):\n    with lakeflow.job("my_job") as job:\n        job.start()\n'
        result = run_tests(
            pytester, source, "--lakeflow-report-dir=out", "-o", "log_cli=true", "--log-cli-level=WARNING"
        )
        result.assert_outcomes(passed=1)
        result.stdout.fnmatch_lines(
            [
                "*Run 77 of 'my_job' had not ended 0 seconds after it was cancelled, so report.json does not list "
                f"it: {HOST}/jobs/runs/77*"
            ]
        )
        assert fake_ws.jobs.cancelled == [77]
        assert not (pytester.path / "out").exists()


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
        running = running_run(77, {"extract": "SUCCESS", "transform": "RUNNING"})
        fake_ws.jobs.queue_polls([running, running, make_run(77, {"extract": "SUCCESS"})])
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


DEBUG_FOLDER = "/Users/someone@example.com/.pytest-databricks-lakeflow/debug"
TRANSFORM_PATH = "/Workspace/Users/someone@example.com/.bundle/example/test/files/src/transform"
HELPER_UPLOADS = [
    "/Users/someone@example.com/.pytest-databricks-lakeflow/set_values",
    "/Users/someone@example.com/.pytest-databricks-lakeflow/get_values",
]
FAILS_IN_WITH = """
def test_job(lakeflow):
    with lakeflow.job("my_job") as job:
        job.run()
        assert False, "boom"
"""


def debug_uploads(ws: FakeWorkspaceClient) -> dict[str, dict[str, Any]]:
    return {path: upload for path, upload in ws.workspace.uploads.items() if path.startswith(f"{DEBUG_FOLDER}/")}


class TestDebugNotebook:
    """`debug_notebook()` and `--lakeflow-debug-notebook`: a notebook that replays a task's resolved parameters."""

    def test_debug_notebook_uploads_under_helpers_path(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        queue_failed_run(fake_ws)
        result = run_tests(
            pytester,
            """
            def test_job(lakeflow, request):
                with lakeflow.job("my_job") as job:
                    job.run()
                    notebook = job.debug_notebook("transform")
                assert (notebook.task_key, notebook.run_id) == ("transform", 77)
                assert str(notebook) == notebook.url
                print("URL", notebook.url, notebook.path)
                assert ("lakeflow_debug_notebook", notebook.url) in request.node.user_properties
            """,
            "-s",
        )
        result.assert_outcomes(passed=1)
        path = f"{DEBUG_FOLDER}/my_job__transform__77"
        assert list(debug_uploads(fake_ws)) == [path]
        upload = fake_ws.workspace.uploads[path]
        assert f"# MAGIC %run {TRANSFORM_PATH}" in upload["content"]
        assert 'dbutils.widgets.text("row_count", "7")' in upload["content"]
        assert "/jobs/runs/7700" in upload["content"], "the heading links the task run"
        assert (upload["language"], upload["overwrite"]) == (Language.PYTHON, True)
        result.stdout.fnmatch_lines([f"*URL {HOST}/#notebook/{fake_ws.workspace.object_ids[path]} {path}"])

    @pytest.mark.parametrize(
        ("task_key", "message"),
        [
            pytest.param(
                "load",
                "Task 'load' of 'my_job' is not a notebook task, so there is no notebook to run. "
                "Notebook tasks: ['classic', 'extract', 'transform']",
                id="wheel_task",
            ),
            pytest.param(
                "nope",
                "Job 'my_job' has no task 'nope'. Available: ['classic', 'extract', 'load', 'transform']",
                id="unknown_task",
            ),
            pytest.param(
                "git",
                "Task 'git' of 'my_job' runs its notebook from Git (src/git), which %run cannot reach",
                id="git_source",
            ),
        ],
    )
    def test_debug_notebook_rejects_non_notebook_tasks(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, task_key: str, message: str
    ) -> None:
        settings = example_settings()
        if task_key == "git":
            notebook = jobs.NotebookTask(notebook_path="src/git", source=jobs.Source.GIT)
            settings.tasks = [*(settings.tasks or []), jobs.Task(task_key="git", notebook_task=notebook)]
        fake_ws.jobs.settings[fake_cli.deployed["my_job"]] = settings
        result = run_tests(
            pytester,
            f"""
            import re

            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            def test_job(lakeflow):
                job = lakeflow.job("my_job")
                with pytest.raises(LakeflowError, match=re.escape({message!r})):
                    job.debug_notebook({task_key!r})
            """,
        )
        result.assert_outcomes(passed=1)
        assert debug_uploads(fake_ws) == {}

    def test_debug_notebook_needs_resolved_parameters(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        queue_failed_run(fake_ws, {"transform": "UPSTREAM_FAILED"})
        result = run_tests(
            pytester,
            """
            import re

            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    job.run()
                    message = "Task 'transform' did not run in run 77 (UPSTREAM_FAILED), so it has no resolved"
                    with pytest.raises(LakeflowError, match=re.escape(message) + " parameters$"):
                        job.debug_notebook("transform")
                    with pytest.raises(KeyError, match="extract"):
                        job.debug_notebook("extract")
            """,
        )
        result.assert_outcomes(passed=1)
        assert debug_uploads(fake_ws) == {}

    def test_debug_notebook_before_any_run_fails_clearly(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        result = run_tests(
            pytester,
            """
            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    message = "Job 'my_job' has not run yet, so there are no resolved parameters to replay"
                    with pytest.raises(LakeflowError, match=message):
                        job.debug_notebook("transform")
            """,
        )
        result.assert_outcomes(passed=1)

    def test_debug_notebook_flag_writes_for_failed_tests(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        queue_failed_run(fake_ws)
        result = run_tests(pytester, FAILS_IN_WITH, "--lakeflow-debug-notebook", "--junitxml=report.xml")
        result.assert_outcomes(failed=1)
        path = f"{DEBUG_FOLDER}/my_job__transform__77"
        assert list(debug_uploads(fake_ws)) == [path]
        url = f"{HOST}/#notebook/{fake_ws.workspace.object_ids[path]}"
        result.stdout.fnmatch_lines(["*- lakeflow -*", f"lakeflow_debug_notebook: {url}"])
        assert f'<property name="lakeflow_debug_notebook" value="{url}"' in (pytester.path / "report.xml").read_text()

    @pytest.mark.parametrize(
        ("source", "states", "flags"),
        [
            pytest.param(FAILS_IN_WITH, {"transform": "FAILED"}, [], id="without_the_flag"),
            pytest.param(
                'def test_job(lakeflow):\n    with lakeflow.job("my_job") as job:\n        assert job.run().failed\n',
                {"transform": "FAILED"},
                ["--lakeflow-debug-notebook"],
                id="passing_test",
            ),
            pytest.param(FAILS_IN_WITH, {"transform": "SUCCESS"}, ["--lakeflow-debug-notebook"], id="successful_run"),
            pytest.param(FAILS_IN_WITH, {"load": "FAILED"}, ["--lakeflow-debug-notebook"], id="wheel_task_failed"),
        ],
    )
    def test_nothing_is_written_without_a_failed_notebook_task_in_a_failed_test(
        self,
        pytester: pytest.Pytester,
        fake_ws: FakeWorkspaceClient,
        source: str,
        states: dict[str, str],
        flags: list[str],
    ) -> None:
        queue_failed_run(fake_ws, states)
        result = run_tests(pytester, source, *flags)
        result.stdout.no_fnmatch_line("*lakeflow_debug_notebook*")
        result.stdout.no_fnmatch_line("*debug notebook*")
        assert fake_ws.workspace.upload_calls == HELPER_UPLOADS

    def test_debug_notebook_of_a_job_its_finaliser_closes(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        # The finaliser closes the job in the teardown phase, whose report section shows only with -rA, so the
        # terminal summary names the test, and report.json lists the notebook.
        parameters = {"row_count": "7", "payload": "x" * 3000}
        fake_ws.jobs.resolved[7700] = jobs.ResolvedValues(
            notebook_task=jobs.ResolvedNotebookTaskValues(base_parameters=parameters)
        )
        fake_ws.jobs.queue(make_run(77, {"transform": "FAILED"}, "FAILED", "RUN_EXECUTION_ERROR"))
        source = """
            import pytest

            @pytest.fixture
            def failed_run(lakeflow):
                return lakeflow.job("my_job").run()

            def test_job(failed_run):
                assert False, "boom"
            """
        result = run_tests(pytester, source, "--lakeflow-debug-notebook", "--lakeflow-report-dir=out")
        result.assert_outcomes(failed=1)
        path = f"{DEBUG_FOLDER}/my_job__transform__77"
        url = f"{HOST}/#notebook/{fake_ws.workspace.object_ids[path]}"
        result.stdout.fnmatch_lines(
            [
                "*= lakeflow =*",
                "lakeflow: 1 debug notebook written",
                f"  {url} (test_jobs.py::test_job, task 'transform'; widgets cut at 2048 bytes: 'payload')",
            ]
        )
        _, report = report_of(pytester.path / "out")
        assert report["debug_notebooks"] == [{"task_key": "transform", "run_id": 77, "path": path, "url": url}]

    @pytest.mark.parametrize(
        ("key", "message"),
        [
            pytest.param("transform", "no write access", id="upload_fails"),
            pytest.param("ghost", "No task 'ghost' in the job. Available: *", id="task_not_in_the_settings"),
        ],
    )
    def test_debug_notebook_flag_does_not_fail_teardown(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, key: str, message: str
    ) -> None:
        queue_failed_run(fake_ws, {key: "FAILED"})
        fake_ws.workspace.upload_errors[f"{DEBUG_FOLDER}/my_job__transform__77"] = PermissionDenied("no write access")
        result = run_tests(pytester, FAILS_IN_WITH, "--lakeflow-debug-notebook")
        result.assert_outcomes(failed=1)
        result.stdout.fnmatch_lines([f"*Could not write a debug notebook for task {key!r} of run 77: {message}"])
        result.stdout.fnmatch_lines(["*Keeping job 1001 from my_job*"])
        assert "AssertionError: boom" in result.stdout.str()


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
            "DebugNotebook",
            "Stub",
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
