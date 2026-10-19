from __future__ import annotations

import time
from typing import Any, cast

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import PermissionDenied
from databricks.sdk.service import jobs

from pytest_databricks_lakeflow import session
from pytest_databricks_lakeflow.errors import LakeflowBundleError

from .conftest import FakeCli, configure_lakeflow
from .fakes import HOST, FakeWorkspaceClient, make_run
from .test_session import STAGING_FIXTURE, make_xdist_conftest

pytestmark = pytest.mark.usefixtures("fake_cli", "bundle_dir")

DESTROY = ["bundle", "destroy", "-t", "test", "--auto-approve"]
DESTROY_STAGING = ["bundle", "destroy", "-t", "staging", "--auto-approve"]
DEPLOY_WITH_VARIABLES = ["bundle", "deploy", "-t", "test", "--auto-approve", "--var", "catalog=dev"]
HOUR_MS = 3_600_000
USES_LAKEFLOW = "def test_{0}(lakeflow):\n    lakeflow.template('my_job')\n"
USES_SCHEMA = "def test_schema(lakeflow):\n    with lakeflow.schema():\n        pass\n"
FAILING_JOB = "def test_{0}(lakeflow):\n    lakeflow.job('my_job').create()\n    assert False\n"
SWEEP_HINT = "hint: pytest --lakeflow-sweep removes kept jobs older than sweep_hours"
VARIABLES_FIXTURE = """

@pytest.fixture(scope="session")
def lakeflow():
    return Lakeflow(variables={"catalog": "dev"})
"""
XDIST_WORKER_IDS = """

import os


def make_client(profile):
    ws = FakeWorkspaceClient()
    ws.jobs.add(example_settings())
    ws.jobs.start_ids_at(1000 * (2 + int(os.environ["PYTEST_XDIST_WORKER"][2:])))
    return ws
"""


def uses_lakeflow(*names: str) -> str:
    return "\n".join(USES_LAKEFLOW.format(name) for name in names)


def leaves_runs_active(runs: str) -> str:
    return USES_LAKEFLOW.format("leak") + f"    lakeflow.active_runs.update({runs})\n"


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


class TestSweep:
    """`--lakeflow-sweep` deletes this user's old `pytest` jobs from other runs, once, when the bundle is set up."""

    def test_sweep_deletes_old_jobs_of_other_runs(self) -> None:
        ws = FakeWorkspaceClient()
        ws.jobs.listed = sweep_candidates()
        assert session.sweep(cast(WorkspaceClient, ws), "this-run", 24) == [1, 7]
        assert ws.jobs.deleted == [1, 7]

    def test_sweep_carries_on_after_a_failed_delete(self, caplog: pytest.LogCaptureFixture) -> None:
        ws = FakeWorkspaceClient()
        ws.jobs.listed = sweep_candidates()
        ws.jobs.delete_errors[1] = PermissionDenied("not yours")
        assert session.sweep(cast(WorkspaceClient, ws), "this-run", 0.5) == [3, 7]
        assert "Could not delete job 1: not yours" in caplog.text

    @pytest.mark.parametrize(
        ("arguments", "deleted"),
        [
            pytest.param("", [1, 2, 7], id="default_hours"),
            pytest.param("sweep_hours=0.5", [1, 2, 3, 7], id="half_hour"),
        ],
    )
    def test_sweep_flag_sweeps_once_at_session_start(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, arguments: str, deleted: list[int]
    ) -> None:
        fake_ws.jobs.listed = sweep_candidates()
        configure_lakeflow(pytester, arguments)
        pytester.makepyfile(uses_lakeflow("a", "b"))
        pytester.runpytest("--lakeflow-sweep").assert_outcomes(passed=2)
        assert fake_ws.jobs.deleted == deleted
        assert fake_ws.jobs.list_calls == 1
        assert fake_ws.current_user.me_calls == 1

    def test_no_sweep_without_the_flag(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        fake_ws.jobs.listed = sweep_candidates()
        pytester.makepyfile(uses_lakeflow("a"))
        pytester.runpytest().assert_outcomes(passed=1)
        assert fake_ws.jobs.list_calls == 0


class TestDestroy:
    """`--lakeflow-destroy` runs `bundle destroy` once per deployed target at session end, after cleanup."""

    def test_destroy_at_session_end(self, pytester: pytest.Pytester, fake_cli: FakeCli) -> None:
        pytester.makepyfile(uses_lakeflow("a", "b"))
        pytester.runpytest("--lakeflow-destroy").assert_outcomes(passed=2)
        assert fake_cli.calls[-1] == DESTROY
        assert fake_cli.calls.count(DESTROY) == 1

    def test_destroy_each_deployed_target(self, pytester: pytest.Pytester, fake_cli: FakeCli) -> None:
        configure_lakeflow(pytester, 'profile="dev"', STAGING_FIXTURE)
        pytester.makepyfile("def test_a(lakeflow, staging):\n    lakeflow.template('my_job')\n    staging.templates\n")
        pytester.runpytest("--lakeflow-destroy").assert_outcomes(passed=1)
        destroys = [(args, profile) for args, _, profile in fake_cli.invocations if args[:2] == ["bundle", "destroy"]]
        assert destroys == [(DESTROY, "dev"), (DESTROY_STAGING, None)]

    def test_destroy_uses_the_deploy_variables(self, pytester: pytest.Pytester, fake_cli: FakeCli) -> None:
        configure_lakeflow(pytester, 'variables={"catalog": "dev"}')
        pytester.makepyfile(uses_lakeflow("a"))
        pytester.runpytest("--lakeflow-destroy").assert_outcomes(passed=1)
        assert fake_cli.calls[-1] == [*DESTROY, "--var", "catalog=dev"]
        assert DEPLOY_WITH_VARIABLES in fake_cli.calls

    def test_xdist_destroy_uses_the_deploy_variables(self, pytester: pytest.Pytester, fake_cli: FakeCli) -> None:
        pytest.importorskip("xdist")
        log = make_xdist_conftest(pytester, VARIABLES_FIXTURE)
        pytester.makepyfile(uses_lakeflow(*map(str, range(4))))
        pytester.runpytest("-n", "2", "--lakeflow-destroy").assert_outcomes(passed=4)
        assert log.read_text().splitlines().count(" ".join(DEPLOY_WITH_VARIABLES)) == 1
        assert fake_cli.calls == [[*DESTROY, "--var", "catalog=dev"]]

    def test_xdist_controller_destroys_once(self, pytester: pytest.Pytester, fake_cli: FakeCli) -> None:
        pytest.importorskip("xdist")
        log = make_xdist_conftest(pytester)
        pytester.makepyfile(uses_lakeflow(*map(str, range(4))))
        pytester.runpytest("-n", "2", "--lakeflow-destroy").assert_outcomes(passed=4)
        assert " ".join(DESTROY) not in log.read_text().splitlines()
        assert fake_cli.calls == [DESTROY]

    def test_no_destroy_without_the_flag(self, pytester: pytest.Pytester, fake_cli: FakeCli) -> None:
        pytester.makepyfile(uses_lakeflow("a"))
        pytester.runpytest().assert_outcomes(passed=1)
        assert DESTROY not in fake_cli.calls

    @pytest.mark.parametrize(
        ("source", "flags", "outcomes"),
        [
            pytest.param("def test_a(lakeflow):\n    pass\n", [], {"passed": 1}, id="fixture_unused"),
            pytest.param(
                uses_lakeflow("a") + "\ndef test_b():\n    pass\n",
                ["-m", "not lakeflow"],
                {"passed": 1, "deselected": 1},
                id="lakeflow_tests_deselected",
            ),
        ],
    )
    def test_no_destroy_when_nothing_was_set_up(
        self, pytester: pytest.Pytester, fake_cli: FakeCli, source: str, flags: list[str], outcomes: dict[str, int]
    ) -> None:
        pytester.makepyfile(source)
        pytester.runpytest("--lakeflow-destroy", *flags).assert_outcomes(**outcomes)
        assert fake_cli.calls == []

    def test_no_destroy_with_sync(self, pytester: pytest.Pytester, fake_cli: FakeCli) -> None:
        configure_lakeflow(pytester, 'sync="sync"')
        pytester.makepyfile("def test_a(lakeflow):\n    lakeflow.templates\n")
        pytester.runpytest("--lakeflow-destroy").assert_outcomes(passed=1)
        assert ["bundle", "sync", "-t", "test"] in fake_cli.calls
        assert DESTROY not in fake_cli.calls

    def test_setup_failure_skips_destroy(self, pytester: pytest.Pytester, fake_cli: FakeCli) -> None:
        fake_cli.errors["deploy"] = LakeflowBundleError("deploy broke")
        pytester.makepyfile(uses_lakeflow("a"))
        pytester.runpytest("--lakeflow-destroy").assert_outcomes(failed=1)
        assert DESTROY not in fake_cli.calls

    @pytest.mark.parametrize(
        ("extra_source", "lines"),
        [
            pytest.param("", ["*1 passed*"], id="passing_session"),
            pytest.param(
                "\ndef test_b(lakeflow):\n    assert False\n",
                ["FAILED test_destroy_failure_fails_the_session.py::test_b*", "*1 failed, 1 passed*"],
                id="failing_session",
            ),
        ],
    )
    def test_destroy_failure_fails_the_session(
        self, pytester: pytest.Pytester, fake_cli: FakeCli, extra_source: str, lines: list[str]
    ) -> None:
        fake_cli.errors["destroy"] = LakeflowBundleError(
            "`databricks bundle destroy` failed with exit code 1:\nError: x"
        )
        pytester.makepyfile(uses_lakeflow("a") + extra_source)
        result = pytester.runpytest("--lakeflow-destroy")
        assert result.ret == pytest.ExitCode.TESTS_FAILED
        result.stdout.fnmatch_lines(["*lakeflow*destroy failed*", "*Error: x*"])
        result.stdout.fnmatch_lines(lines)

    def test_destroy_runs_after_interrupted_tests_are_cleaned_up(
        self,
        pytester: pytest.Pytester,
        fake_ws: FakeWorkspaceClient,
        fake_cli: FakeCli,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        order: list[str] = []

        def record(args: list[str]) -> None:
            if len(args) > 1:
                order.append(f"cli {args[1]} deleted={fake_ws.jobs.deleted}")

        def interrupted(run_id: int, **kwargs: Any) -> jobs.Run:
            raise KeyboardInterrupt

        fake_cli.before = record
        fake_ws.jobs.queue(make_run(77, {"t": "SUCCESS"}))
        monkeypatch.setattr(fake_ws.jobs, "get_run", interrupted)
        pytester.makepyfile('def test_job(lakeflow):\n    lakeflow.job("my_job").run()\n')
        pytester.runpytest("--lakeflow-destroy", no_reraise_ctrlc=True)
        assert fake_ws.jobs.cancelled == [77]
        assert order[-1] == "cli destroy deleted=[1001]"


class TestSessionEnd:
    """Session end cancels the runs that tests left active and copes with xdist workers that report nothing."""

    @pytest.mark.parametrize(("runs", "cancelled", "line"), [("{1}", [1], "1 run"), ("{3, 1}", [1, 3], "2 runs")])
    def test_runs_left_active_are_cancelled_and_counted(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, runs: str, cancelled: list[int], line: str
    ) -> None:
        pytester.makepyfile(leaves_runs_active(runs))
        result = pytester.runpytest()
        result.assert_outcomes(passed=1)
        assert fake_ws.jobs.cancelled == cancelled
        result.stdout.fnmatch_lines(["*= lakeflow =*", f"lakeflow: {line} cancelled at session end"])
        result.stdout.no_fnmatch_line("*kept*")

    def test_crashed_xdist_worker_is_reported_normally(self, pytester: pytest.Pytester) -> None:
        pytest.importorskip("xdist")
        pytester.makepyfile(
            "import os\n\ndef test_crash():\n    os._exit(3)\n\ndef test_a():\n    pass\n\ndef test_b():\n    pass\n"
        )
        result = pytester.runpytest("-n", "2")
        assert "INTERNALERROR" not in result.stdout.str() + result.stderr.str()
        result.stdout.fnmatch_lines(["*crashed while running*test_crash*", "*1 failed, 2 passed*"])


class TestTerminalSummary:
    """The `lakeflow` terminal section lists kept jobs and schemas from every xdist worker, only when there are any."""

    def test_lists_kept_jobs(self, pytester: pytest.Pytester) -> None:
        pytester.makepyfile(FAILING_JOB.format("kept") + "\n" + USES_LAKEFLOW.format("other"))
        result = pytester.runpytest()
        result.assert_outcomes(failed=1, passed=1)
        result.stdout.fnmatch_lines(["*= lakeflow =*", "lakeflow: 1 job kept", f"  {HOST}/jobs/1001", SWEEP_HINT])
        result.stdout.no_fnmatch_line("*schema* kept*")
        result.stdout.no_fnmatch_line("*cancelled at session end*")

    def test_lists_kept_schemas(self, pytester: pytest.Pytester) -> None:
        configure_lakeflow(pytester, 'catalog="c"')
        pytester.makepyfile(USES_SCHEMA)
        result = pytester.runpytest("--lakeflow-keep")
        result.assert_outcomes(passed=1)
        result.stdout.fnmatch_lines(["*= lakeflow =*", "lakeflow: 1 schema kept", "  c.pytest_*"])
        result.stdout.no_fnmatch_line(SWEEP_HINT)

    def test_no_summary_when_nothing_was_kept(self, pytester: pytest.Pytester) -> None:
        pytester.makepyfile("def test_job(lakeflow):\n    lakeflow.job('my_job').run()\n")
        result = pytester.runpytest()
        result.assert_outcomes(passed=1)
        result.stdout.no_fnmatch_line("*= lakeflow =*")
        result.stdout.no_fnmatch_line("lakeflow:*")

    def test_xdist_controller_summarises_kept_jobs(self, pytester: pytest.Pytester) -> None:
        pytest.importorskip("xdist")
        make_xdist_conftest(pytester, XDIST_WORKER_IDS)
        pytester.makepyfile(FAILING_JOB.format("a") + "\n" + FAILING_JOB.format("b"))
        result = pytester.runpytest("-n", "2")
        result.assert_outcomes(failed=2)
        result.stdout.fnmatch_lines(["*= lakeflow =*", "lakeflow: 2 jobs kept", f"  {HOST}/jobs/*", f"  {HOST}/jobs/*"])
        urls = [line for line in result.stdout.lines if line.startswith(f"  {HOST}/jobs/")]
        assert len(urls) == 2
        assert len(set(urls)) == 2
