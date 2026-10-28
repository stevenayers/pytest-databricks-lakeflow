"""The pytest plugin: its flags and marker, the end of the session, `--lakeflow-destroy` and the terminal summary."""

from __future__ import annotations

from typing import Any

import pytest
from databricks.sdk.service import jobs

from pytest_databricks_lakeflow import Lakeflow, LakeflowBundleError

from .conftest import (
    FOLDER,
    STAGING_FIXTURE,
    USES_LAKEFLOW,
    USES_SCHEMA,
    FakeCli,
    configure_lakeflow,
    make_xdist_conftest,
    uses_lakeflow,
)
from .fakes.runs import HOST, make_run, queue_failed_run
from .fakes.workspace import FakeWorkspaceClient

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
                "Lakeflow Jobs from a Databricks Asset Bundle.*",
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
                "*--lakeflow-performance-target=*",
                "*--lakeflow-check*",
            ]
        )
        assert "The cap is per xdist worker." in " ".join(result.stdout.str().split())
        assert "https://stevenayers.github.io/pytest-databricks-lakeflow/" in result.stdout.str()
        assert "lakeflow-skill install" in result.stdout.str()
        assert "--lakeflow-target" not in result.stdout.str()
        assert "lakeflow_target" not in result.stdout.str()


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


class TestPerformanceTargetFlag:
    """`--lakeflow-performance-target` sets every copy's performance mode, over `Lakeflow(performance_target=...)`."""

    @pytest.mark.usefixtures("fake_cli", "bundle_dir")
    def test_flag_wins_over_the_argument(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        configure_lakeflow(pytester, 'performance_target="PERFORMANCE_OPTIMIZED"')
        pytester.makepyfile('def test_job(lakeflow):\n    lakeflow.job("my_job").run()\n')
        pytester.runpytest("--lakeflow-performance-target=STANDARD").assert_outcomes(passed=1, warnings=0)
        [created] = fake_ws.jobs.created
        assert created["performance_target"] == jobs.PerformanceTarget.STANDARD

    def test_flag_takes_only_the_platform_values(self, pytester: pytest.Pytester) -> None:
        pytester.makepyfile(TESTS)
        result = pytester.runpytest("--lakeflow-performance-target=fast")
        assert result.ret == pytest.ExitCode.USAGE_ERROR
        result.stderr.fnmatch_lines(["*--lakeflow-performance-target: invalid choice: 'fast'*"])


DESTROY = ["bundle", "destroy", "-t", "test", "--auto-approve"]
DESTROY_STAGING = ["bundle", "destroy", "-t", "staging", "--auto-approve"]
DEPLOY_WITH_VARIABLES = ["bundle", "deploy", "-t", "test", "--auto-approve", "--var", "catalog=dev"]
FAILING_JOB = "def test_{0}(lakeflow):\n    lakeflow.job('my_job').create()\n    assert False\n"
FAILED_RUN = (
    "def test_{0}(lakeflow):\n    with lakeflow.job('my_job') as job:\n        job.run()\n        assert False\n"
)
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
# `my_job` is a job with one pipeline task, on pipeline `abc`, in every xdist worker.
XDIST_PIPELINE_JOB = """

from fakes.templates import pipeline_settings, pipeline_spec


def make_client(profile):
    ws = FakeWorkspaceClient()
    ws.jobs.add(pipeline_settings())
    ws.pipelines.specs["abc"] = pipeline_spec()
    return ws
"""
FAILING_COPY = (
    "def test_{0}(lakeflow):\n    lakeflow.job('my_job', pipelines='copy', schema='c.s').create()\n    assert False\n"
)
XDIST_FAILED_TRANSFORM = """

from databricks.sdk.service import jobs

from fakes.runs import make_run


def make_client(profile):
    ws = FakeWorkspaceClient()
    ws.jobs.add(example_settings())
    parameters = jobs.ResolvedNotebookTaskValues(base_parameters={"row_count": "7"})
    ws.jobs.resolved[7700] = jobs.ResolvedValues(notebook_task=parameters)
    # Terminated at the first poll, because the workers poll at the plugin's real interval.
    for _ in range(2):
        ws.jobs.queue_polls([make_run(77, {"transform": "FAILED"}, "FAILED")])
    return ws
"""


def leaves_runs_active(runs: str) -> str:
    return USES_LAKEFLOW.format("leak") + f"    lakeflow.active_runs.update({runs})\n"


@pytest.mark.usefixtures("fake_cli", "bundle_dir")
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


@pytest.mark.usefixtures("fake_cli", "bundle_dir")
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

    def test_updates_left_active_are_stopped(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        pytester.makepyfile(USES_LAKEFLOW.format("leak") + '    lakeflow.active_updates["p-1"] = "u-1"\n')
        pytester.runpytest().assert_outcomes(passed=1)
        assert fake_ws.pipelines.stopped == ["p-1"]

    def test_crashed_xdist_worker_is_reported_normally(self, pytester: pytest.Pytester) -> None:
        pytest.importorskip("xdist")
        pytester.makepyfile(
            "import os\n\ndef test_crash():\n    os._exit(3)\n\ndef test_a():\n    pass\n\ndef test_b():\n    pass\n"
        )
        result = pytester.runpytest("-n", "2")
        assert "INTERNALERROR" not in result.stdout.str() + result.stderr.str()
        result.stdout.fnmatch_lines(["*crashed while running*test_crash*", "*1 failed, 2 passed*"])


@pytest.mark.usefixtures("fake_cli", "bundle_dir")
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

    def test_xdist_controller_summarises_kept_pipelines(self, pytester: pytest.Pytester) -> None:
        pytest.importorskip("xdist")
        make_xdist_conftest(pytester, XDIST_PIPELINE_JOB)
        pytester.makepyfile(FAILING_COPY.format("a") + "\n" + FAILING_COPY.format("b"))
        result = pytester.runpytest("-n", "2")
        result.assert_outcomes(failed=2)
        pipeline = f"  {HOST}/pipelines/p-1000"
        result.stdout.fnmatch_lines(
            ["*= lakeflow =*", "lakeflow: 2 jobs kept", "lakeflow: 2 pipelines kept", pipeline, pipeline]
        )

    def test_lists_debug_notebooks(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        queue_failed_run(fake_ws)
        pytester.makepyfile(FAILED_RUN.format("failed"))
        result = pytester.runpytest("--lakeflow-debug-notebook")
        result.assert_outcomes(failed=1)
        path = f"{FOLDER}/debug/my_job__transform__77"
        url = f"{HOST}/#notebook/{fake_ws.workspace.object_ids[path]}"
        result.stdout.fnmatch_lines(
            [
                "*= lakeflow =*",
                "lakeflow: 1 job kept",
                "*",
                "lakeflow: 1 debug notebook written",
                f"  {url} (test_lists_debug_notebooks.py::test_failed, task 'transform')",
            ]
        )

    def test_xdist_controller_summarises_debug_notebooks(self, pytester: pytest.Pytester) -> None:
        pytest.importorskip("xdist")
        make_xdist_conftest(pytester, XDIST_FAILED_TRANSFORM)
        pytester.makepyfile(FAILED_RUN.format("a") + "\n" + FAILED_RUN.format("b"))
        result = pytester.runpytest("-n", "2", "--lakeflow-debug-notebook")
        result.assert_outcomes(failed=2)
        result.stdout.fnmatch_lines(["*= lakeflow =*", "lakeflow: 2 debug notebooks written", f"  {HOST}/#notebook/*"])
