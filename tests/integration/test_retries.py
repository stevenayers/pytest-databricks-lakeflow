r"""Integration tests for the ``retries`` bundle (tests/integration/bundles/retries), run in a real workspace.

The bundle has two serverless notebook jobs: ``retrying_job``, whose ``flaky`` task always raises and retries once,
and ``slow_job``, whose ``sleep`` task sleeps for the ``seconds`` job parameter under a task ``timeout_seconds`` of 60.

Run them with:

    LAKEFLOW_INTEGRATION=1 DATABRICKS_CONFIG_PROFILE=<profile> LAKEFLOW_CATALOG=<catalog> \
        uv run pytest tests/integration/test_retries.py -n 4 --lakeflow-sweep
"""

from __future__ import annotations

import dataclasses
import time
from datetime import timedelta

import pytest
from databricks.sdk.service import jobs

from pytest_databricks_lakeflow import Lakeflow, LakeflowRunFailed, LakeflowTimeout
from pytest_databricks_lakeflow.settings import TIMEOUT_GRACE_SECONDS

from .conftest import lakeflow_for


@pytest.fixture(scope="module")
def lakeflow() -> Lakeflow:
    return lakeflow_for("retries")


def test_every_attempt_of_a_retried_task_is_kept(lakeflow: Lakeflow) -> None:
    with lakeflow.job("retrying_job") as job:
        run = job.run()
        assert run.failed
        assert not run.timed_out
        flaky = run.task("flaky")
        assert flaky.attempt == 1
        assert [a.attempt for a in flaky.attempts] == [0, 1]
        assert all(a.result_state == "FAILED" for a in flaky.attempts)
        assert all(a.attempts == [] for a in flaky.attempts)
        assert len({a.run_id for a in flaky.attempts}) == 2
        assert flaky.attempts[-1].run_id == flaky.run_id
        assert flaky.raised(RuntimeError)
        assert all(a.raised(RuntimeError) for a in flaky.attempts)
        assert all("flaky task failed on purpose" in a.error.message for a in flaky.attempts if a.error)
        with pytest.raises(LakeflowRunFailed) as excinfo:
            run.raise_for_failure()
        assert "RuntimeError" in str(excinfo.value)
        assert "flaky task failed on purpose" in str(excinfo.value)
        assert "flaky" in str(excinfo.value)


def test_a_task_stopped_at_its_timeout_seconds_is_timed_out(lakeflow: Lakeflow) -> None:
    # The template gives the sleep task timeout_seconds: 60, so the platform stops it while the plugin's own
    # 30 minute wait is still running and the run comes back as a JobRun rather than raising.
    with lakeflow.job("slow_job", params={"seconds": "180"}) as job:
        run = job.run()
        assert run.task("sleep").timed_out
        assert run.task("sleep").result_state == "TIMEDOUT"
        assert run.timed_out
        assert not run.succeeded
        assert run.task("sleep").attempt == 0
        assert len(run.task("sleep").attempts) == 1


def test_the_plugin_timeout_cancels_the_run_and_is_mirrored_on_the_job(lakeflow: Lakeflow) -> None:
    timeout = timedelta(seconds=20)
    quick = dataclasses.replace(lakeflow, timeout=timeout)
    with quick.job("slow_job", params={"seconds": "180"}) as job:
        job.create()
        assert job.job_id is not None
        settings = quick.ws.jobs.get(job.job_id).settings
        assert settings is not None
        # The template has no job-level timeout_seconds (the 60 is on the task), so the copy gets timeout + grace.
        assert settings.timeout_seconds == int(timeout.total_seconds()) + TIMEOUT_GRACE_SECONDS
        started = time.monotonic()
        with pytest.raises(LakeflowTimeout, match="did not finish within 0:00:20 and was cancelled"):
            job.run()
        assert time.monotonic() - started < 120
        assert job.runs == []
        assert not quick.active_runs
        state = _wait_for_termination(quick, _only_run_id(quick, job.job_id))
        assert state.result_state == jobs.RunResultState.CANCELED, state


def _only_run_id(lakeflow: Lakeflow, job_id: int) -> int:
    runs = list(lakeflow.ws.jobs.list_runs(job_id=job_id))
    assert len(runs) == 1, runs
    assert runs[0].run_id is not None
    return runs[0].run_id


def _wait_for_termination(lakeflow: Lakeflow, run_id: int, limit: float = 90.0) -> jobs.RunState:
    deadline = time.monotonic() + limit
    while True:
        run = lakeflow.ws.jobs.get_run(run_id)
        assert run.state is not None
        if run.state.life_cycle_state in {
            jobs.RunLifeCycleState.TERMINATED,
            jobs.RunLifeCycleState.INTERNAL_ERROR,
            jobs.RunLifeCycleState.SKIPPED,
        }:
            return run.state
        assert time.monotonic() < deadline, f"run {run_id} did not terminate after cancel: {run.state}"
        time.sleep(2)
