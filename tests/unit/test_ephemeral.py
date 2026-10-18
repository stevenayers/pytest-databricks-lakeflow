from __future__ import annotations

import re
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from databricks.sdk.errors import PermissionDenied
from databricks.sdk.service import jobs

from .conftest import GITHUB, FakeCli, configure_lakeflow, make_repository, set_ci
from .fakes import HOST, FakeWorkspaceClient, make_run

pytestmark = pytest.mark.usefixtures("fake_cli", "bundle_dir")


def run_tests(pytester: pytest.Pytester, source: str, *args: str) -> pytest.RunResult:
    pytester.makepyfile(test_jobs=source)
    return pytester.runpytest(*args)


def test_with_block_creates_runs_and_deletes(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
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
    assert "deployment" not in created
    [call] = fake_ws.jobs.run_now_calls
    assert call["job_parameters"] == {"param1": "hi", "schema": "s"}
    assert call["only"] is None
    assert fake_ws.jobs.deleted == [call["job_id"]]


def test_run_requests_resolved_values(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
    run_tests(pytester, 'def test_job(lakeflow):\n    lakeflow.job("my_job").run()\n').assert_outcomes(passed=1)
    assert fake_ws.jobs.get_run_calls
    assert all(call.get("include_resolved_values") is True for call in fake_ws.jobs.get_run_calls)


def test_jobs_carry_attribution_tags(
    pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch, fake_ws: FakeWorkspaceClient
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
    pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch, fake_ws: FakeWorkspaceClient
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
    pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch, fake_ws: FakeWorkspaceClient
) -> None:
    set_ci(monkeypatch, {})
    sha = make_repository(pytester.path)
    result = run_tests(pytester, 'def test_job(lakeflow):\n    with lakeflow.job("my_job"):\n        pass\n')
    result.assert_outcomes(passed=1)
    tags = fake_ws.jobs.created[0]["tags"]
    assert tags["pytest_git_sha"] == sha
    assert tags["pytest_git_branch"] == "topic"


def test_failure_inside_with_keeps_the_job(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
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


def test_failure_after_with_still_deletes(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
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


def test_plain_call_deletes_at_teardown(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
    result = run_tests(
        pytester,
        """
        def test_job(lakeflow):
            job = lakeflow.job("my_job")
            assert job.job_id is None
            assert job.run().succeeded
            assert job.job_id is not None
        """,
    )
    result.assert_outcomes(passed=1)
    assert fake_ws.jobs.deleted == [1001]


def test_plain_call_keeps_failed_test(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
    result = run_tests(
        pytester,
        """
        def test_job(lakeflow):
            job = lakeflow.job("my_job")
            job.run()
            assert False
        """,
    )
    result.assert_outcomes(failed=1)
    assert fake_ws.jobs.deleted == []


def test_disable_marks_tasks_on_the_copy(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
    source = 'def test_job(lakeflow):\n    with lakeflow.job("my_job", disable=["load"]):\n        pass\n'
    run_tests(pytester, source).assert_outcomes(passed=1)
    [created] = fake_ws.jobs.created
    assert {t.task_key: t.disabled for t in created["tasks"]} == {
        "extract": None,
        "transform": None,
        "load": True,
        "classic": None,
    }


def test_disable_rejects_unknown_tasks(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
    result = run_tests(pytester, 'def test_job(lakeflow):\n    lakeflow.job("my_job", disable=["nope"]).run()\n')
    result.assert_outcomes(failed=1)
    result.stdout.fnmatch_lines(["*Unknown task 'nope' in disable. Available: ['classic', 'extract', 'load', *"])
    assert fake_ws.jobs.created == []


def test_unused_job_is_never_created(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
    result = run_tests(pytester, 'def test_job(lakeflow):\n    lakeflow.job("my_job")\n')
    result.assert_outcomes(passed=1)
    assert (fake_ws.jobs.created, fake_ws.jobs.deleted) == ([], [])


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
    pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, arguments: str, flags: list[str], deleted: list[int]
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


def test_timeout_cancels_the_run(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
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
                    job.run(timeout=timedelta(0))
            assert lakeflow.ws.jobs.cancelled == [77]
            assert lakeflow.active_runs == set()
        """,
    )
    result.assert_outcomes(passed=1)


def test_timeout_from_lakeflow(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
    fake_ws.jobs.queue(make_run(77, {"t": "SUCCESS"}), polls=100)
    configure_lakeflow(pytester, "timeout=timedelta(0)")
    result = run_tests(
        pytester,
        """
        import pytest

        from pytest_databricks_lakeflow import LakeflowTimeout

        def test_job(lakeflow):
            with lakeflow.job("my_job") as job:
                with pytest.raises(LakeflowTimeout, match="did not finish within 0:00:00"):
                    job.run()
        """,
    )
    result.assert_outcomes(passed=1)
    assert fake_ws.jobs.cancelled == [77]


@pytest.mark.parametrize(
    ("arguments", "job_timeout"),
    [pytest.param(None, 1860, id="default"), pytest.param("timeout=timedelta(minutes=1)", 120, id="one-minute")],
)
def test_run_timeout_longer_than_the_job_fails_clearly(
    pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, arguments: str | None, job_timeout: int
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


def test_run_timeout_within_the_job_timeout_runs(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
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


def test_exit_cancels_active_runs(
    pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, monkeypatch: pytest.MonkeyPatch
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


def test_report_section_and_junit_properties(pytester: pytest.Pytester) -> None:
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
            f"lakeflow_run_url: {HOST}/jobs/runs/*",
        ]
    )
    xml = (pytester.path / "report.xml").read_text()
    assert f'<property name="lakeflow_job_url" value="{HOST}/jobs/1001"' in xml
    assert '<property name="lakeflow_run_url"' in xml
    assert '<property name="lakeflow_job_id" value="1001"' in xml
    assert re.search(r'<property name="lakeflow_run_id" value="\d+"', xml)
    assert '<property name="lakeflow_result_state" value="SUCCESS"' in xml
    assert re.search(r'<property name="lakeflow_duration" value="\d+\.\d{3}"', xml)
    assert "lakeflow_failing" not in xml


def test_failing_run_adds_a_failing_property(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
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


def test_section_is_attached_to_setup_failures(pytester: pytest.Pytester) -> None:
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


def test_callbacks(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
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


def test_only_adds_probes(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
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


def test_bad_params_fail_before_creating(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
    result = run_tests(
        pytester,
        """
        import pytest

        from pytest_databricks_lakeflow import LakeflowError

        def test_job(lakeflow):
            with pytest.raises(LakeflowError, match="does not declare the parameters ..parm1.*allow_undeclared_params"):
                with lakeflow.job("my_job", params={"parm1": "x"}):
                    pass
            with lakeflow.job("my_job") as job:
                with pytest.raises(LakeflowError, match="does not declare"):
                    job.run(params={"typo": "x"})
        """,
    )
    result.assert_outcomes(passed=1)
    assert fake_ws.jobs.run_now_calls == []


def test_undeclared_params_when_allowed(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
    configure_lakeflow(pytester, "allow_undeclared_params=True")
    source = 'def test_job(lakeflow):\n    lakeflow.job("my_job", params={"typo": "x"}).run(params={"other": "y"})\n'
    run_tests(pytester, source).assert_outcomes(passed=1)
    assert fake_ws.jobs.run_now_calls[0]["job_parameters"] == {"typo": "x", "other": "y"}


def test_standard_performance_target_warns(
    pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli
) -> None:
    fake_ws.jobs.settings[fake_cli.deployed["my_job"]].performance_target = jobs.PerformanceTarget.STANDARD
    result = run_tests(pytester, 'def test_job(lakeflow):\n    lakeflow.job("my_job")\n')
    result.assert_outcomes(passed=1, warnings=1)
    result.stdout.fnmatch_lines(["*Job 'my_job' has performance_target STANDARD*Pass performance_optimized=True*"])


def test_performance_optimized(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli) -> None:
    fake_ws.jobs.settings[fake_cli.deployed["my_job"]].performance_target = jobs.PerformanceTarget.STANDARD
    configure_lakeflow(pytester, "performance_optimized=True, disable_auto_optimization=False")
    result = run_tests(pytester, 'def test_job(lakeflow):\n    lakeflow.job("my_job").run()\n')
    result.assert_outcomes(passed=1)
    [created] = fake_ws.jobs.created
    assert created["performance_target"] == jobs.PerformanceTarget.PERFORMANCE_OPTIMIZED
    assert not any(task.disable_auto_optimization for task in created["tasks"])


def test_run_sends_a_fresh_idempotency_token(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
    result = run_tests(
        pytester,
        """
        def test_job(lakeflow):
            with lakeflow.job("my_job") as job:
                job.run()
                job.run()
        """,
    )
    result.assert_outcomes(passed=1)
    tokens = [call["idempotency_token"] for call in fake_ws.jobs.run_now_calls]
    assert len(tokens) == 2
    assert all(tokens)
    assert tokens[0] != tokens[1]
    assert all(len(token) <= 64 for token in tokens)


def test_token_changes_after_a_timeout(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
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
        """,
    )
    result.assert_outcomes(passed=1)
    first, second = (call["idempotency_token"] for call in fake_ws.jobs.run_now_calls)
    assert first != second


def test_run_forwards_performance_target_and_queue(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
    result = run_tests(
        pytester,
        """
        def test_job(lakeflow):
            with lakeflow.job("my_job") as job:
                job.run(performance_target="PERFORMANCE_OPTIMIZED", queue=False)
                assert job.settings.performance_target is None
        """,
    )
    result.assert_outcomes(passed=1)
    [call] = fake_ws.jobs.run_now_calls
    assert call["performance_target"] == jobs.PerformanceTarget.PERFORMANCE_OPTIMIZED
    assert call["queue"] == jobs.QueueSettings(enabled=False)
    assert "performance_target" not in fake_ws.jobs.created[0]


def test_run_accepts_the_enum_and_queue_true(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
    result = run_tests(
        pytester,
        """
        from databricks.sdk.service import jobs

        def test_job(lakeflow):
            with lakeflow.job("my_job") as job:
                job.run(performance_target=jobs.PerformanceTarget.STANDARD, queue=True)
        """,
    )
    result.assert_outcomes(passed=1)
    [call] = fake_ws.jobs.run_now_calls
    assert call["performance_target"] == jobs.PerformanceTarget.STANDARD
    assert call["queue"] == jobs.QueueSettings(enabled=True)


def test_run_without_options_forwards_none(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
    result = run_tests(pytester, 'def test_job(lakeflow):\n    lakeflow.job("my_job").run()\n')
    result.assert_outcomes(passed=1)
    [call] = fake_ws.jobs.run_now_calls
    assert call["performance_target"] is None
    assert call["queue"] is None


def test_run_rejects_an_unknown_performance_target(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
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


def test_module_fixture_shares_one_job(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
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


def test_package_exports() -> None:
    import pytest_databricks_lakeflow

    assert Path(pytest_databricks_lakeflow.__file__).name == "__init__.py"
    for name in ("Lakeflow", "EphemeralJob", "JobRun", "TaskRun", "TaskError", "LakeflowTimeout", "LakeflowRunFailed"):
        assert hasattr(pytest_databricks_lakeflow, name)
        assert name in pytest_databricks_lakeflow.__all__
    assert issubclass(pytest_databricks_lakeflow.LakeflowRunFailed, pytest_databricks_lakeflow.LakeflowError)


def test_raise_for_failure_fails_the_test_with_the_run_detail(
    pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
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
    result.stdout.fnmatch_lines(["*LakeflowRunFailed: JobRun('my_job', FAILED*", "*extract: FAILED*/jobs/runs/7700*"])


def test_skip_or_xfail_inside_with_deletes_the_job(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
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


def test_params_declared_by_modify_settings_are_accepted(
    pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
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


def test_run_after_the_block_fails_clearly(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
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


def test_overrides_apply_only_below_their_conftest(
    pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli
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


def test_permissions_reach_jobs_create(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
    configure_lakeflow(pytester, 'permissions=[{"group_name": "data-eng", "permission_level": "CAN_VIEW"}]')
    result = run_tests(pytester, 'def test_job(lakeflow):\n    lakeflow.job("my_job").create()\n')
    result.assert_outcomes(passed=1)
    view = jobs.JobPermissionLevel.CAN_VIEW
    assert fake_ws.jobs.acls == [[jobs.JobAccessControlRequest(group_name="data-eng", permission_level=view)]]
    assert "access_control_list" not in fake_ws.jobs.created[0]


def test_no_permissions_by_default(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
    result = run_tests(pytester, 'def test_job(lakeflow):\n    lakeflow.job("my_job").create()\n')
    result.assert_outcomes(passed=1)
    assert fake_ws.jobs.acls == [None]
    assert "access_control_list" not in fake_ws.jobs.created[0]
    assert "run_as" not in fake_ws.jobs.created[0]


def test_run_as_keep_on_job_and_task(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
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


def test_create_errors_pass_through(
    pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    def denied(**kwargs: object) -> jobs.CreateResponse:
        raise PermissionDenied("User is not authorised to set run_as to 0000-app-id")

    monkeypatch.setattr(fake_ws.jobs, "create", denied)
    result = run_tests(
        pytester,
        'def test_job(lakeflow):\n    with lakeflow.job("my_job", run_as="0000-app-id") as job:\n        job.run()\n',
    )
    result.assert_outcomes(failed=1)
    result.stdout.fnmatch_lines(["*PermissionDenied: User is not authorised to set run_as to 0000-app-id*"])
    assert fake_ws.jobs.run_now_calls == []


def test_compute_knobs_reach_jobs_create(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
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


def test_check_compute_runs_after_modify_settings(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
    callback = """

def drop_environment(settings, item):
    next(t for t in settings.tasks if t.task_key == "load").environment_key = None
"""
    configure_lakeflow(pytester, "modify_settings=drop_environment", callback)
    result = run_tests(pytester, 'def test_job(lakeflow):\n    with lakeflow.job("my_job"):\n        pass\n')
    result.assert_outcomes(failed=1)
    result.stdout.fnmatch_lines(["*LakeflowError*['load']*environment_key*"])
    assert fake_ws.jobs.created == []
