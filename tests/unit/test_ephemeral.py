from __future__ import annotations

from pathlib import Path
from typing import cast

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

from pytest_databricks_lakeflow.client import Lakeflow
from pytest_databricks_lakeflow.errors import LakeflowError

from .conftest import FakeCli
from .fakes import HOST, FakeWorkspaceClient, example_settings, make_run
from .test_session import options

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


def test_unused_job_is_never_created(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
    result = run_tests(pytester, 'def test_job(lakeflow):\n    lakeflow.job("my_job")\n')
    result.assert_outcomes(passed=1)
    assert (fake_ws.jobs.created, fake_ws.jobs.deleted) == ([], [])


@pytest.mark.parametrize(("flags", "deleted"), [(["--lakeflow-keep"], False), (["--no-lakeflow-keep-failed"], True)])
def test_keep_flags(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, flags: list[str], deleted: bool) -> None:
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
    assert fake_ws.jobs.deleted == ([1001, 1003] if deleted else [])


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


def test_exit_cancels_active_runs(
    pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken(run_id: int) -> jobs.Run:
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


def test_hooks(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
    pytester.makeconftest(
        """
        finished = []

        def pytest_lakeflow_modify_settings(settings, request):
            settings.description = f"from {request.node.name}"

        def pytest_lakeflow_run_finished(run, request):
            finished.append((run.job_key, request.node.name))
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
            with pytest.raises(LakeflowError, match="does not declare the parameters ..parm1.."):
                with lakeflow.job("my_job", params={"parm1": "x"}):
                    pass
            with lakeflow.job("my_job") as job:
                with pytest.raises(LakeflowError, match="does not declare"):
                    job.run(params={"typo": "x"})
        """,
    )
    result.assert_outcomes(passed=1)
    assert fake_ws.jobs.run_now_calls == []


def test_standard_performance_target_warns(
    pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli
) -> None:
    fake_ws.jobs.settings[fake_cli.deployed["my_job"]].performance_target = jobs.PerformanceTarget.STANDARD
    result = run_tests(pytester, 'def test_job(lakeflow):\n    lakeflow.job("my_job")\n')
    result.assert_outcomes(passed=1, warnings=1)
    result.stdout.fnmatch_lines(["*Job 'my_job' has performance_target STANDARD*"])


def test_session_cancels_runs_left_active() -> None:
    ws = FakeWorkspaceClient()
    lakeflow = Lakeflow(ws=cast(WorkspaceClient, ws), options=options(), run_id="r", templates={})
    lakeflow.for_test(cast(pytest.FixtureRequest, None)).active_runs.update({3, 1})
    lakeflow.cancel_active_runs()
    assert ws.jobs.cancelled == [1, 3]
    assert lakeflow.active_runs == set()


def test_jobs_need_a_test() -> None:
    lakeflow = Lakeflow(
        ws=cast(WorkspaceClient, FakeWorkspaceClient()),
        options=options(),
        run_id="r",
        templates={"my_job": example_settings().as_dict()},
    )
    with pytest.raises(LakeflowError, match="inside a test"):
        lakeflow.job("my_job")


def test_package_exports() -> None:
    import pytest_databricks_lakeflow

    assert Path(pytest_databricks_lakeflow.__file__).name == "__init__.py"
    for name in ("Lakeflow", "EphemeralJob", "JobRun", "TaskRun", "TaskError", "LakeflowTimeout"):
        assert hasattr(pytest_databricks_lakeflow, name)


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


def test_params_declared_by_the_hook_are_accepted(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
    pytester.makeconftest(
        """
        from databricks.sdk.service import jobs

        def pytest_lakeflow_modify_settings(settings, request):
            settings.parameters = [*settings.parameters, jobs.JobParameterDefinition(name="catalog", default="")]
        """
    )
    result = run_tests(
        pytester,
        r"""
        import pytest

        from pytest_databricks_lakeflow import LakeflowError

        def test_declared_by_hook(lakeflow):
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


def test_hooks_apply_only_below_their_conftest(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
    pytester.mkpydir("team_a").joinpath("conftest.py").write_text(
        "def pytest_lakeflow_modify_settings(settings, request):\n    settings.description = 'team a'\n"
    )
    pytester.mkpydir("team_b")
    test = "def test_job(lakeflow):\n    with lakeflow.job('my_job') as job:\n        job.run()\n"
    pytester.path.joinpath("team_a", "test_a.py").write_text(test)
    pytester.path.joinpath("team_b", "test_b.py").write_text(test)
    pytester.runpytest("team_a/test_a.py", "team_b/test_b.py").assert_outcomes(passed=2)
    assert [created.get("description") for created in fake_ws.jobs.created] == ["team a", None]
