"""Session steps: `lakeflow.once()` runs a setup once per session and its teardown at session end."""

from __future__ import annotations

from pathlib import Path

import pytest
from databricks.sdk.service import jobs

from .conftest import configure_lakeflow, make_xdist_conftest, run_tests
from .fakes.runs import make_run
from .fakes.workspace import FakeWorkspaceClient

pytestmark = pytest.mark.usefixtures("fake_cli", "bundle_dir")
ONCE_JOB = 1001
SETUP_RUN = 77
TEARDOWN_RUN = 78
STATEMENTS = '["CREATE TABLE ${schema}.a", "CREATE TABLE ${schema}.b"]'
STATEMENTS_OUTPUT = jobs.RunOutput(notebook_output=jobs.NotebookOutput(result='{"statements": 2}'))
SHARED_FIXTURE = f"""
import pytest


@pytest.fixture(scope="session")
def fixtures(lakeflow):
    return lakeflow.once("orders", setup={STATEMENTS}, params={{"schema": "c.s"}})


def test_a(lakeflow, fixtures):
    assert (fixtures.ran_as, fixtures.output_json) == ("task", {{"statements": 2}})
    assert fixtures.task.task_key == "__pytest_setup"


def test_b(lakeflow, fixtures):
    again = lakeflow.once("orders", setup={STATEMENTS}, params={{"schema": "c.s"}})
    assert again is fixtures
"""
DIRECT_CALLS = f"""
def test_a(lakeflow):
    lakeflow.once("orders", setup={STATEMENTS}, params={{"schema": "c.s"}})


def test_b(lakeflow):
    lakeflow.once("orders", setup={STATEMENTS}, params={{"schema": "c.s"}})
"""
WITH_TEARDOWN = """
def test_a(lakeflow):
    step = lakeflow.once("orders", setup=["CREATE TABLE t"], teardown=["DROP TABLE t"])
    assert step.succeeded
"""
ONCE_CLIENT = """

from pathlib import Path

from fakes.runs import make_run


def make_client(profile):
    ws = FakeWorkspaceClient()
    ws.jobs.add(example_settings())
    ws.jobs.create_log = Path({created!r})
    ws.jobs.delete_log = Path({deleted!r})
    run = make_run(601, {{"__pytest_setup": "SUCCESS"}})
    ws.jobs.add_run(run)
    ws.jobs.queue(run)
    return ws


def pytest_configure(config):
    bundle.run_cli = fake_cli
    workspace_api.make_workspace_client = make_client
"""


def queue_setup(ws: FakeWorkspaceClient, state: str = "SUCCESS") -> None:
    result_state = "SUCCESS" if state == "SUCCESS" else "FAILED"
    output = STATEMENTS_OUTPUT if state == "SUCCESS" else jobs.RunOutput(error="ValueError: no schema")
    ws.jobs.queue(make_run(SETUP_RUN, {"__pytest_setup": state}, result_state), {SETUP_RUN * 100: output})


def queue_teardown(ws: FakeWorkspaceClient, state: str = "SUCCESS") -> None:
    result_state = "SUCCESS" if state == "SUCCESS" else "FAILED"
    output = jobs.RunOutput() if state == "SUCCESS" else jobs.RunOutput(error="PermissionError: nope")
    ws.jobs.queue(make_run(TEARDOWN_RUN, {"__pytest_teardown": state}, result_state), {TEARDOWN_RUN * 100: output})


@pytest.fixture(autouse=True)
def steps_as_tasks(pytester: pytest.Pytester) -> None:
    """Run the steps as tasks; a test that needs other options configures them with `run_as_task=True` too."""
    configure_lakeflow(pytester, "run_as_task=True")


class TestOnce:
    """One job per session step: the first caller sets it up, every other caller reuses the outcome."""

    def test_setup_runs_once_for_the_session_and_the_job_goes_at_its_end(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        queue_setup(fake_ws)
        result = run_tests(pytester, SHARED_FIXTURE)
        result.assert_outcomes(passed=2)
        [created] = fake_ws.jobs.created
        assert created["name"] == "[pytest] once: orders"
        assert created["tags"]["pytest_once"] == "orders"
        assert created["tags"]["pytest"] == ""
        assert created["parameters"] == [jobs.JobParameterDefinition(name="schema", default="c.s")]
        assert created["max_concurrent_runs"] == 1
        assert created["timeout_seconds"] == 1860
        assert created["performance_target"] == jobs.PerformanceTarget.PERFORMANCE_OPTIMIZED
        [setup] = created["tasks"]
        assert (setup.task_key, setup.environment_key, setup.depends_on) == (
            "__pytest_setup",
            "__pytest_serverless",
            None,
        )
        assert setup.notebook_task is not None
        assert '["schema"]' in (setup.notebook_task.base_parameters or {}).values()
        [call] = fake_ws.jobs.run_now_calls
        assert (call["job_id"], call["only"], call["job_parameters"]) == (
            ONCE_JOB,
            ["__pytest_setup"],
            {"schema": "c.s"},
        )
        assert fake_ws.jobs.deleted == [ONCE_JOB]

    def test_second_call_in_a_process_reuses_the_outcome(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        queue_setup(fake_ws)
        run_tests(pytester, DIRECT_CALLS).assert_outcomes(passed=2)
        assert len(fake_ws.jobs.created) == 1
        assert len(fake_ws.jobs.run_now_calls) == 1
        # The first call polled the run; the second read nothing from the workspace.
        assert fake_ws.jobs.get_run_calls == fake_ws.jobs.get_run_calls[: len(fake_ws.jobs.get_run_calls)]
        assert all(call["run_id"] in (SETUP_RUN, SETUP_RUN * 100) for call in fake_ws.jobs.get_run_calls)

    def test_teardown_runs_alone_at_session_end_before_the_job_goes(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        queue_setup(fake_ws)
        queue_teardown(fake_ws)
        run_tests(pytester, WITH_TEARDOWN).assert_outcomes(passed=1)
        [created] = fake_ws.jobs.created
        setup, teardown = created["tasks"]
        assert [t.task_key for t in (setup, teardown)] == ["__pytest_setup", "__pytest_teardown"]
        assert [d.task_key for d in teardown.depends_on or []] == ["__pytest_setup"]
        assert teardown.run_if == jobs.RunIf.ALL_DONE
        setup_call, teardown_call = fake_ws.jobs.run_now_calls
        assert (setup_call["only"], teardown_call["only"]) == (["__pytest_setup"], ["__pytest_teardown"])
        assert teardown_call["queue"] == jobs.QueueSettings(enabled=True)
        assert fake_ws.jobs.deleted == [ONCE_JOB]

    def test_teardown_only_returns_nothing_now_and_runs_at_session_end(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        queue_teardown(fake_ws)
        source = 'def test_a(lakeflow):\n    assert lakeflow.once("files", teardown=["DROP TABLE t"]) is None\n'
        run_tests(pytester, source).assert_outcomes(passed=1)
        [created] = fake_ws.jobs.created
        [teardown] = created["tasks"]
        assert (teardown.task_key, teardown.depends_on) == ("__pytest_teardown", None)
        [call] = fake_ws.jobs.run_now_calls
        assert call["only"] == ["__pytest_teardown"]
        assert fake_ws.jobs.deleted == [ONCE_JOB]

    def test_failed_setup_fails_the_test_and_keeps_the_job(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        queue_setup(fake_ws, "FAILED")
        result = run_tests(pytester, DIRECT_CALLS)
        result.assert_outcomes(failed=2)
        result.stdout.fnmatch_lines(
            [
                "*LakeflowError: once('orders'): the setup step failed: ValueError: no schema: https://*/runs/77*",
                "*LakeflowError: once('orders') failed earlier in this session: LakeflowError: once('orders'): the "
                "setup step failed*",
                "*lakeflow: 1 job kept*",
                f"*/jobs/{ONCE_JOB}*",
            ]
        )
        assert len(fake_ws.jobs.run_now_calls) == 1
        assert fake_ws.jobs.deleted == []

    def test_failed_setup_is_deleted_without_keep_failed(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        queue_setup(fake_ws, "FAILED")
        configure_lakeflow(pytester, "keep_failed=False, run_as_task=True")
        run_tests(pytester, WITH_TEARDOWN).assert_outcomes(failed=1)
        assert fake_ws.jobs.deleted == [ONCE_JOB]
        assert len(fake_ws.jobs.run_now_calls) == 1

    def test_keep_keeps_the_job_and_skips_the_teardown(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        queue_setup(fake_ws)
        result = run_tests(pytester, WITH_TEARDOWN, "--lakeflow-keep")
        result.assert_outcomes(passed=1)
        result.stdout.fnmatch_lines([f"*lakeflow: kept the job of once('orders'): https://*/jobs/{ONCE_JOB}*"])
        assert len(fake_ws.jobs.run_now_calls) == 1
        assert fake_ws.jobs.deleted == []

    def test_failed_teardown_keeps_the_job(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        queue_setup(fake_ws)
        queue_teardown(fake_ws, "FAILED")
        result = run_tests(pytester, WITH_TEARDOWN)
        result.assert_outcomes(passed=1)
        result.stdout.fnmatch_lines([f"*lakeflow: kept the job of once('orders'): https://*/jobs/{ONCE_JOB}*"])
        assert fake_ws.jobs.deleted == []

    def test_failed_teardown_is_deleted_without_keep_failed(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        queue_setup(fake_ws)
        queue_teardown(fake_ws, "FAILED")
        configure_lakeflow(pytester, "keep_failed=False, run_as_task=True")
        run_tests(pytester, WITH_TEARDOWN).assert_outcomes(passed=1)
        assert fake_ws.jobs.deleted == [ONCE_JOB]

    def test_existing_cluster_id_moves_the_tasks_off_serverless(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        queue_setup(fake_ws)
        configure_lakeflow(pytester, 'existing_cluster_id="c-1", run_as_task=True')
        run_tests(pytester, WITH_TEARDOWN).assert_outcomes(passed=1)
        [created] = fake_ws.jobs.created
        assert "environments" not in created
        for task in created["tasks"]:
            assert (task.existing_cluster_id, task.environment_key) == ("c-1", None), task.task_key


class TestOnceArguments:
    """What `once()` refuses before anything is created."""

    def test_unknown_placeholder_is_refused(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        source = 'def test_a(lakeflow):\n    lakeflow.once("x", setup=["CREATE TABLE ${schema}.t"])\n'
        result = run_tests(pytester, source)
        result.assert_outcomes(failed=1)
        result.stdout.fnmatch_lines(
            ["*LakeflowError: setup= of once('x') reads [[]'schema'[]], which params does not hold. Pass them in*"]
        )
        assert fake_ws.jobs.created == []

    @pytest.mark.parametrize(
        ("call", "message"),
        [
            pytest.param('lakeflow.once("x")', "once('x') needs setup=, teardown= or both", id="no step"),
            pytest.param('lakeflow.once(" ", setup=["SELECT 1"])', "once() needs a name", id="blank name"),
            pytest.param(
                'lakeflow.once("x", setup=["SELECT 1"], params={"__pytest_a": "1"})',
                "Parameter names starting with '__pytest_' are reserved",
                id="reserved parameter",
            ),
            pytest.param(
                'lakeflow.once("x", setup="SELECT 1")',
                "setup= takes a list of SQL statements, not a string",
                id="string step",
            ),
        ],
    )
    def test_rejects(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, call: str, message: str) -> None:
        result = run_tests(pytester, f"def test_a(lakeflow):\n    {call}\n")
        result.assert_outcomes(failed=1)
        result.stdout.fnmatch_lines([f"*ValueError: {message}*"])
        assert fake_ws.jobs.created == []

    def test_helpers_compute_is_refused(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        configure_lakeflow(pytester, 'helpers_compute="classic", run_as_task=True')
        result = run_tests(pytester, 'def test_a(lakeflow):\n    lakeflow.once("x", setup=["SELECT 1"])\n')
        result.assert_outcomes(failed=1)
        result.stdout.fnmatch_lines(["*ValueError: once('x') runs on serverless, but helpers_compute is 'classic'*"])
        assert fake_ws.jobs.created == []


class TestOnceUnderXdist:
    """The first worker to ask sets the step up; the others wait for it and reuse it; the controller tears it down."""

    def test_workers_share_one_job(self, pytester: pytest.Pytester, bundle_dir: Path) -> None:
        pytest.importorskip("xdist")
        created = pytester.path / "created.log"
        deleted = pytester.path / "deleted.log"
        make_xdist_conftest(pytester, ONCE_CLIENT.format(created=str(created), deleted=str(deleted)))
        test = (
            "def test_{i}(lakeflow):\n"
            "    step = lakeflow.once('orders', setup=['CREATE TABLE t'], run_as_task=True)\n"
            "    assert (step.ran_as, step.succeeded) == ('task', True)\n"
        )
        pytester.makepyfile("\n".join(test.format(i=i) for i in range(6)))
        pytester.runpytest("-n", "2").assert_outcomes(passed=6)
        assert created.read_text().split() == [str(ONCE_JOB)]
        assert deleted.read_text().split() == [str(ONCE_JOB)]
