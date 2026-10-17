from __future__ import annotations

import time
from typing import cast

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import PermissionDenied
from databricks.sdk.service import jobs

from pytest_databricks_lakeflow.client import Lakeflow
from pytest_databricks_lakeflow.errors import LakeflowBundleError

from .conftest import FakeCli
from .fakes import FakeWorkspaceClient
from .test_session import make_xdist_conftest, options

pytestmark = pytest.mark.usefixtures("fake_cli", "bundle_dir")

DESTROY = ["bundle", "destroy", "-t", "test", "--auto-approve"]
HOUR_MS = 3_600_000


def test_schema_is_created_and_dropped(
    pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("LAKEFLOW_RUN_ID", "CI-42")
    pytester.makeini("[pytest]\nlakeflow_catalog = dev_sandbox\n")
    pytester.makepyfile(
        """
        import re

        def test_schema(lakeflow_schema):
            assert re.fullmatch(r"dev_sandbox\\.pytest_ci_42_[0-9a-f]{8}", lakeflow_schema), lakeflow_schema

        def test_failing(lakeflow_schema):
            assert False
        """
    )
    pytester.runpytest().assert_outcomes(passed=1, failed=1)
    assert len(fake_ws.schemas.created) == 2
    assert len(set(fake_ws.schemas.created)) == 2
    assert fake_ws.schemas.deleted == fake_ws.schemas.created


def test_schema_is_kept_with_keep(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
    pytester.makeini("[pytest]\nlakeflow_catalog = dev_sandbox\n")
    pytester.makepyfile("def test_schema(lakeflow_schema):\n    pass\n")
    pytester.runpytest("--lakeflow-keep").assert_outcomes(passed=1)
    assert len(fake_ws.schemas.created) == 1
    assert fake_ws.schemas.deleted == []


def test_schema_needs_a_catalog(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
    pytester.makepyfile("def test_schema(lakeflow_schema):\n    pass\n")
    result = pytester.runpytest()
    result.assert_outcomes(errors=1)
    result.stdout.fnmatch_lines(["*lakeflow_schema needs the lakeflow_catalog ini option*"])
    assert fake_ws.schemas.created == []


def listed(job_id: int, age_hours: float, tags: dict[str, str], creator: str = "someone@example.com") -> jobs.BaseJob:
    created = int(time.time() * 1000 - age_hours * HOUR_MS)
    return jobs.BaseJob(
        job_id=job_id, created_time=created, creator_user_name=creator, settings=jobs.JobSettings(tags=tags)
    )


def sweep_candidates() -> list[jobs.BaseJob]:
    old = {"pytest": "", "pytest_run_id": "old-run"}
    return [
        listed(1, 25, old),
        listed(2, 25, {"pytest": "", "pytest_run_id": "this-run"}),
        listed(3, 1, old),
        listed(4, 25, old, creator="colleague@example.com"),
        listed(5, 25, {"pytest_run_id": "old-run"}),
        listed(6, 25, {"pytest": ""}),
        listed(7, 30, old),
        jobs.BaseJob(job_id=8, created_time=0),
    ]


def test_sweep_deletes_old_jobs_of_other_runs() -> None:
    ws = FakeWorkspaceClient()
    ws.jobs.listed = sweep_candidates()
    lakeflow = Lakeflow(ws=cast(WorkspaceClient, ws), options=options(), run_id="this-run", templates={})
    assert lakeflow.sweep() == [1, 7]
    assert ws.jobs.deleted == [1, 7]


def test_sweep_carries_on_after_a_failed_delete(caplog: pytest.LogCaptureFixture) -> None:
    ws = FakeWorkspaceClient()
    ws.jobs.listed = sweep_candidates()
    ws.jobs.delete_errors[1] = PermissionDenied("not yours")
    lakeflow = Lakeflow(ws=cast(WorkspaceClient, ws), options=options(sweep_hours=0.5), run_id="this-run", templates={})
    assert lakeflow.sweep() == [3, 7]
    assert "Could not delete job 1: not yours" in caplog.text


def test_sweep_flag_sweeps_once_at_session_start(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
    fake_ws.jobs.listed = sweep_candidates()
    pytester.makepyfile("def test_a(lakeflow):\n    pass\n\ndef test_b(lakeflow):\n    pass\n")
    pytester.runpytest("--lakeflow-sweep").assert_outcomes(passed=2)
    assert fake_ws.jobs.deleted == [1, 2, 7]
    assert fake_ws.jobs.list_calls == 1


def test_no_sweep_without_the_flag(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
    fake_ws.jobs.listed = sweep_candidates()
    pytester.makepyfile("def test_a(lakeflow):\n    pass\n")
    pytester.runpytest().assert_outcomes(passed=1)
    assert fake_ws.jobs.list_calls == 0


def test_destroy_at_session_end(pytester: pytest.Pytester, fake_cli: FakeCli) -> None:
    pytester.makepyfile("def test_a(lakeflow):\n    pass\n\ndef test_b(lakeflow):\n    pass\n")
    pytester.runpytest("--lakeflow-destroy").assert_outcomes(passed=2)
    assert fake_cli.calls[-1] == DESTROY
    assert fake_cli.calls.count(DESTROY) == 1


def test_no_destroy_without_the_flag(pytester: pytest.Pytester, fake_cli: FakeCli) -> None:
    pytester.makepyfile("def test_a(lakeflow):\n    pass\n")
    pytester.runpytest().assert_outcomes(passed=1)
    assert DESTROY not in fake_cli.calls


def test_no_destroy_when_no_lakeflow_test_ran(pytester: pytest.Pytester, fake_cli: FakeCli) -> None:
    pytester.makepyfile("def test_a(lakeflow):\n    pass\n\ndef test_b():\n    pass\n")
    pytester.runpytest("--lakeflow-destroy", "-m", "not lakeflow").assert_outcomes(passed=1, deselected=1)
    assert fake_cli.calls == []


def test_no_destroy_with_sync(pytester: pytest.Pytester, fake_cli: FakeCli) -> None:
    pytester.makepyfile("def test_a(lakeflow):\n    pass\n")
    pytester.runpytest("--lakeflow-destroy", "--lakeflow-sync=sync").assert_outcomes(passed=1)
    assert DESTROY not in fake_cli.calls


def test_xdist_controller_destroys_once(pytester: pytest.Pytester, fake_cli: FakeCli) -> None:
    pytest.importorskip("xdist")
    log = make_xdist_conftest(pytester)
    pytester.makepyfile("\n".join(f"def test_{i}(lakeflow):\n    pass\n" for i in range(4)))
    pytester.runpytest("-n", "2", "--lakeflow-destroy").assert_outcomes(passed=4)
    assert " ".join(DESTROY) not in log.read_text().splitlines()
    assert fake_cli.calls == [DESTROY]


def test_destroy_failure_is_reported_with_the_summary(pytester: pytest.Pytester, fake_cli: FakeCli) -> None:
    fake_cli.errors["destroy"] = LakeflowBundleError("`databricks bundle destroy` failed with exit code 1:\nError: x")
    pytester.makepyfile("def test_a(lakeflow):\n    pass\n\ndef test_b(lakeflow):\n    assert False\n")
    result = pytester.runpytest("--lakeflow-destroy")
    assert result.ret == 1
    result.stdout.fnmatch_lines(["*lakeflow*destroy failed*", "*Error: x*"])
    result.stdout.fnmatch_lines(
        ["FAILED test_destroy_failure_is_reported_with_the_summary.py::test_b*", "*1 failed, 1 passed*"]
    )


def test_destroy_failure_fails_a_passing_session(pytester: pytest.Pytester, fake_cli: FakeCli) -> None:
    fake_cli.errors["destroy"] = LakeflowBundleError("destroy broke")
    pytester.makepyfile("def test_a(lakeflow):\n    pass\n")
    result = pytester.runpytest("--lakeflow-destroy")
    assert result.ret == pytest.ExitCode.TESTS_FAILED
    result.stdout.fnmatch_lines(["*destroy broke*", "*1 passed*"])


def test_destroy_runs_after_interrupted_tests_are_cleaned_up(
    pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli
) -> None:
    order: list[str] = []

    def record(args: list[str]) -> None:
        if len(args) > 1:
            order.append(f"cli {args[1]} deleted={fake_ws.jobs.deleted}")

    fake_cli.before = record
    pytester.makepyfile(
        """
        from tests.unit.fakes import make_run

        def test_job(lakeflow):
            lakeflow.ws.jobs.queue(make_run(77, {"t": "SUCCESS"}))

            def interrupted(run_id):
                raise KeyboardInterrupt

            lakeflow.ws.jobs.get_run = interrupted
            lakeflow.job("my_job").run()
        """
    )
    pytester.runpytest("--lakeflow-destroy", no_reraise_ctrlc=True)
    assert fake_ws.jobs.cancelled == [77]
    assert order[-1] == "cli destroy deleted=[1001]"


def test_setup_failure_skips_destroy(pytester: pytest.Pytester, fake_cli: FakeCli) -> None:
    fake_cli.errors["deploy"] = LakeflowBundleError("deploy broke")
    pytester.makepyfile("def test_a(lakeflow):\n    pass\n")
    pytester.runpytest("--lakeflow-destroy").assert_outcomes(errors=1)
    assert DESTROY not in fake_cli.calls


def test_crashed_xdist_worker_is_reported_normally(pytester: pytest.Pytester) -> None:
    pytest.importorskip("xdist")
    pytester.makepyfile(
        "import os\n\ndef test_crash():\n    os._exit(3)\n\ndef test_a():\n    pass\n\ndef test_b():\n    pass\n"
    )
    result = pytester.runpytest("-n", "2")
    assert "INTERNALERROR" not in result.stdout.str() + result.stderr.str()
    result.stdout.fnmatch_lines(["*crashed while running*test_crash*", "*1 failed, 2 passed*"])
