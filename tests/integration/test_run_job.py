r"""Integration tests for the ``run_job`` bundle (tests/integration/bundles/run_job), run in a real workspace.

The bundle has a ``parent_job`` whose ``trigger_child`` task runs the deployed ``child_job``. These tests cover the
warning and refusal for Run Job tasks and the ``child_run`` built from the triggered run. Run them with:

    LAKEFLOW_INTEGRATION=1 DATABRICKS_CONFIG_PROFILE=<profile> LAKEFLOW_CATALOG=<catalog> \
        uv run pytest tests/integration/test_run_job.py -n 4 --lakeflow-sweep
"""

from __future__ import annotations

import pytest

from pytest_databricks_lakeflow import JobRun, Lakeflow, LakeflowError

from .conftest import lakeflow_for


@pytest.fixture(scope="module")
def lakeflow() -> Lakeflow:
    return lakeflow_for("run_job")


def test_run_job_task_warns_by_default(lakeflow: Lakeflow) -> None:
    with pytest.warns(UserWarning, match="Task 'trigger_child' of job 'parent_job' runs the deployed job"):
        job = lakeflow.job("parent_job")
    with job:
        assert job.job_id is not None


def test_refuse_raises_before_creating_a_copy(lakeflow: Lakeflow, request: pytest.FixtureRequest) -> None:
    with pytest.raises(LakeflowError, match="'trigger_child'"):
        lakeflow.job("parent_job", children="refuse")
    # Copies are named "[pytest] <nodeid>" and the Jobs API name filter is an exact match, so this is empty only
    # when no copy of this test's job was created.
    assert not list(lakeflow.ws.jobs.list(name=f"[pytest] {request.node.nodeid}"))


def test_child_run_is_a_job_run(lakeflow: Lakeflow) -> None:
    with pytest.warns(UserWarning, match="trigger_child"):
        job = lakeflow.job("parent_job", params={"greeting": "hello"})
    with job:
        run = job.run()
        assert run.succeeded, run
        assert run.task("prepare").output == "prepared"
        trigger = run.task("trigger_child")
        assert trigger.run_job_output is not None
        child = trigger.child_run
        assert isinstance(child, JobRun)
        assert child.job_key == "child_job"
        assert child.run_id == trigger.run_job_output.run_id == trigger.child_run_id
        assert child.succeeded, child
        assert child.task("work").output == "hello from the child"
        assert child.parameters["greeting"] == "hello"
        assert run.task("prepare").child_run is None
