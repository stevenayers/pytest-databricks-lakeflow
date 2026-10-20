r"""Integration tests for the ``for_each`` bundle (tests/integration/bundles/for_each), run in a real workspace.

The bundle has one serverless job, ``fanout``: the for-each task ``loop`` repeats the notebook task ``each`` over the
deployed inputs ``["a", "b", "c"]``, passing each one as the ``item`` widget, which ``each`` returns as its output, and
``after`` runs once ``loop`` is done. Run the tests with:

    LAKEFLOW_INTEGRATION=1 DATABRICKS_CONFIG_PROFILE=<profile> LAKEFLOW_CATALOG=<catalog> \
        uv run pytest tests/integration/test_for_each.py -n 8 --dist loadgroup --lakeflow-sweep
"""

from __future__ import annotations

import pytest
from databricks.sdk.service import jobs

from pytest_databricks_lakeflow import JobRun, Lakeflow

from .conftest import lakeflow_for


@pytest.fixture(scope="module")
def lakeflow() -> Lakeflow:
    return lakeflow_for("for_each")


def test_for_each_inputs_replace_the_deployed_list(lakeflow: Lakeflow) -> None:
    with lakeflow.job("fanout", for_each_inputs={"loop": ["x", "{{y}}"]}) as job:
        run = job.run()
        assert run.succeeded, run
        assert job.job_id is not None
        loop = _task_settings(lakeflow, job.job_id, "loop").for_each_task
        assert loop is not None
        assert loop.inputs == '["x", "{\\u007by}}"]'
        assert sorted(_iteration_outputs(lakeflow, run)) == ["x", "{{y}}"]
        assert run.task("after").output == "after"


def test_an_empty_list_runs_no_iteration_and_the_downstream_task(lakeflow: Lakeflow) -> None:
    with lakeflow.job("fanout", for_each_inputs={"loop": []}) as job:
        run = job.run()
        assert run.succeeded, run
        assert run.task("loop").result_state == "SUCCESS"
        assert _iteration_outputs(lakeflow, run) == []
        assert run.task("after").output == "after"


def test_for_each_inputs_on_a_task_job(lakeflow: Lakeflow) -> None:
    with lakeflow.task("fanout.loop", for_each_inputs=["z"]) as job:
        run = job.run()
        assert run.succeeded, run
        assert sorted(run.tasks) == ["loop"]
        assert _iteration_outputs(lakeflow, run) == ["z"]


def _task_settings(lakeflow: Lakeflow, job_id: int, task_key: str) -> jobs.Task:
    settings = lakeflow.ws.jobs.get(job_id).settings
    assert settings is not None
    return next(t for t in settings.tasks or [] if t.task_key == task_key)


def _iteration_outputs(lakeflow: Lakeflow, run: JobRun) -> list[str]:
    # The plugin does not surface iterations yet: the for-each task run, fetched by its own id, lists them.
    loop = lakeflow.ws.jobs.get_run(run.task("loop").run_id)
    outputs: list[str] = []
    for iteration in loop.iterations or []:
        assert iteration.run_id is not None
        notebook = lakeflow.ws.jobs.get_run_output(iteration.run_id).notebook_output
        assert notebook is not None and notebook.result is not None, iteration
        outputs.append(notebook.result)
    return outputs
