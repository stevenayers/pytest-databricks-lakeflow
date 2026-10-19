r"""Integration tests for the ``branches`` bundle (tests/integration/bundles/branches), run in a real workspace.

The bundle has one job, ``branching_job``: ``check_input`` sets the task value ``rows`` from the ``rows`` job
parameter, the condition task ``check`` compares it with ``> 0``, and ``on_true`` and ``on_false`` depend on the
two outcomes. Run the tests with:

    LAKEFLOW_INTEGRATION=1 DATABRICKS_CONFIG_PROFILE=<profile> LAKEFLOW_CATALOG=<catalog> \
        uv run pytest tests/integration/test_branches.py -n 4 --dist loadgroup --lakeflow-sweep
"""

from __future__ import annotations

import pytest

from pytest_databricks_lakeflow import Lakeflow, LakeflowError

from .conftest import lakeflow_for


@pytest.fixture(scope="module")
def lakeflow() -> Lakeflow:
    return lakeflow_for("branches")


@pytest.mark.parametrize(
    ("rows", "expected", "taken", "untaken"),
    [("5", True, "on_true", "on_false"), ("0", False, "on_false", "on_true")],
)
def test_if_else_branch(lakeflow: Lakeflow, rows: str, expected: bool, taken: str, untaken: str) -> None:
    with lakeflow.job("branching_job", params={"rows": rows}) as job:
        run = job.run()
        assert run.succeeded, run
        assert run.task("check").condition_outcome is expected
        assert run.branch_taken("check") is expected
        assert run.task(untaken).excluded
        assert not run.task(untaken).ran
        assert run.task(taken).ran
        assert run.task(taken).output == taken
        assert run.executed_tasks == ["check", "check_input", taken]
        assert run.task("check_input").condition_outcome is None
        with pytest.raises(LakeflowError, match="not a condition task"):
            run.branch_taken("check_input")


@pytest.mark.parametrize(
    ("forced", "rows", "taken", "untaken"),
    [(True, "0", "on_true", "on_false"), (False, "5", "on_false", "on_true")],
)
def test_forced_branch(lakeflow: Lakeflow, forced: bool, rows: str, taken: str, untaken: str) -> None:
    # The data says one branch and force_conditions takes the other.
    with lakeflow.job("branching_job", params={"rows": rows}, force_conditions={"check": forced}) as job:
        run = job.run()
        assert run.succeeded, run
        assert run.branch_taken("check") is forced
        assert run.task("check_input").ran
        assert run.task(taken).ran
        assert run.task(untaken).excluded
        assert run.executed_tasks == ["check", "check_input", taken]


@pytest.mark.parametrize(("rows", "expected"), [(0, False), (5, True)])
def test_condition_task_on_its_own(lakeflow: Lakeflow, rows: int, expected: bool) -> None:
    # The condition task is the task under test; a stub replaces check_input and sets the value it compares.
    run = lakeflow.task("branching_job.check", upstream_task_values={"check_input": {"rows": rows}}).run()
    assert run.succeeded, run
    assert run.task("check").condition_outcome is expected
    assert run.branch_taken("check") is expected
    # The stub is a plugin task, so it is in injected_tasks rather than tasks, and the branches are not in the run.
    assert sorted(run.tasks) == ["check"]
    assert run.executed_tasks == ["check"]
    assert run.injected_tasks["check_input"].ran
    with pytest.raises(KeyError, match="No task 'on_true' in run"):
        run.task("on_true")


def keep_one_task_enabled():
    return None


def test_disabling_the_upstream_of_a_forced_condition_disables_its_branches(lakeflow: Lakeflow) -> None:
    # The platform disables every task downstream of a disabled one ("This task was disabled because it is downstream
    # of a disabled task"), the forced condition task and both branches included. The setup task keeps one task
    # enabled: without it the platform skips the whole run with "Skipped because every task is disabled".
    job = lakeflow.job(
        "branching_job", disable=["check_input"], force_conditions={"check": True}, setup=keep_one_task_enabled
    )
    with job:
        run = job.run()
        assert run.result_state == "SUCCESS"
        assert run.setup is not None
        assert run.setup.succeeded
        assert sorted(run.disabled_tasks) == ["check_input"]
        downstream = {**run.tasks, **run.skipped_tasks}
        assert sorted(downstream) == ["check", "on_false", "on_true"]
        assert all(task.disabled for task in downstream.values()), downstream
        assert downstream["check"].condition_outcome is None
        assert run.executed_tasks == []
