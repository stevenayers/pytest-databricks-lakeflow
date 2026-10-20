"""Read the state of a run and its tasks while polling, and describe each change for the log."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

from databricks.sdk.service import jobs

from pytest_databricks_lakeflow.results.build import error_line
from pytest_databricks_lakeflow.results.models import duration

if TYPE_CHECKING:
    from pytest_databricks_lakeflow.client import Lakeflow


POLL_SECONDS = 2.0
TERMINATED = jobs.RunLifecycleStateV2State.TERMINATED
TERMINAL_LIFE_CYCLE = (
    jobs.RunLifeCycleState.TERMINATED,
    jobs.RunLifeCycleState.SKIPPED,
    jobs.RunLifeCycleState.INTERNAL_ERROR,
)
TERMINAL_UPDATE_STATES = frozenset({"COMPLETED", "FAILED", "CANCELED"})
# The last state logged for each task attempt, keyed by task key and attempt number, and whether it had terminated.
LoggedStates = dict[tuple[str, int], tuple[str, bool]]


def poll_seconds(lakeflow: Lakeflow) -> float:
    if lakeflow.poll_interval is not None:
        return lakeflow.poll_interval.total_seconds()
    return POLL_SECONDS


def terminated(run: jobs.Run) -> bool:
    """Whether the run has ended, from its V2 status, or its legacy life cycle when it has none."""
    if run.status and run.status.state:
        return run.status.state == TERMINATED
    state = run.state.life_cycle_state if run.state else None
    return state in TERMINAL_LIFE_CYCLE


def task_state(task: jobs.RunTask) -> tuple[str | None, bool]:
    """The state to report for a task run, and whether it has terminated.

    The V2 `status.state` while running and the result state once terminated, falling back to the legacy life cycle
    state when the run has no V2 status. Any field may be missing.
    """
    result = task.state.result_state.value if task.state and task.state.result_state else None
    if task.status is not None and task.status.state is not None:
        status = task.status.state
        if status != TERMINATED:
            return status.value, False
        details = task.status.termination_details
        code = details.code.value if details and details.code else None
        return result or code or status.value, True
    life_cycle = task.state.life_cycle_state if task.state else None
    if life_cycle in TERMINAL_LIFE_CYCLE:
        return result or life_cycle.value, True
    if life_cycle is None:
        return None, False
    return life_cycle.value, False


def task_states(run: jobs.Run) -> str:
    """List each task run's state, such as `extract SUCCESS, transform RUNNING, load PENDING`."""
    states = []
    for task in run.tasks or []:
        state, _ = task_state(task)
        states.append(f"{task_label(task)} {state or '?'}")
    return ", ".join(states) or "none"


def task_label(task: jobs.RunTask) -> str:
    """Name a task run by its key, adding the attempt number of a retry."""
    label = task.task_key or "?"
    if task.attempt_number:
        label = f"{label} (attempt {task.attempt_number})"
    return label


def transition(task: jobs.RunTask, previous: str | None, state: str, output: jobs.RunOutput | None, done: bool) -> str:
    """Describe a task run's change of state, such as `transform: RUNNING -> FAILED in 0:00:42 ValueError: x`."""
    label = task_label(task)
    line = f"{label}: {previous} -> {state}" if previous else f"{label}: {state}"
    if not done:
        return line
    elapsed = duration(task.start_time, task.end_time)
    if elapsed:
        line = f"{line} in {timedelta(seconds=int(elapsed.total_seconds()))}"
    error = error_line(task, output)
    if error:
        line = f"{line} {error}"
    return line
