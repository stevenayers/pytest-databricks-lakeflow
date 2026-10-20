r"""Integration tests for the ``for_each`` bundle (tests/integration/bundles/for_each), run in a real workspace.

The bundle has two serverless jobs. In ``fanout``, the for-each task ``loop`` repeats the notebook task ``each`` over
the deployed inputs ``["a", "b", "c"]``, passing each one as the ``item`` widget, which ``each`` returns as its output,
and ``after`` runs once ``loop`` is done. ``fanout_objects`` iterates over objects: ``extract`` sets the task value
``rows``, then the body ``inner`` of ``loop`` reads the whole item, its ``id`` and ``rows`` and returns the three
widgets as JSON, and ``after`` runs last. In ``fanout``, ``each`` raises ``ValueError`` for an item starting with
``fail`` and calls ``taskValues.set`` for the item ``set_value``, which the platform refuses in an iteration. Run the
tests with:

    LAKEFLOW_INTEGRATION=1 DATABRICKS_CONFIG_PROFILE=<profile> LAKEFLOW_CATALOG=<catalog> \
        uv run pytest tests/integration/test_for_each.py -n 8 --dist loadgroup --lakeflow-sweep
"""

from __future__ import annotations

import json

import pytest
from databricks.sdk.service import jobs

from pytest_databricks_lakeflow import ForEachError, ForEachStats, JobRun, Lakeflow, LakeflowError

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
        assert _iteration_outputs(run) == ["x", "{{y}}"]
        assert run.task("after").output == "after"


def test_an_empty_list_runs_no_iteration_and_the_downstream_task(lakeflow: Lakeflow) -> None:
    with lakeflow.job("fanout", for_each_inputs={"loop": []}) as job:
        run = job.run()
        assert run.succeeded, run
        assert run.task("loop").result_state == "SUCCESS"
        assert _iteration_outputs(run) == []
        assert run.task("after").output == "after"


def test_for_each_inputs_on_a_task_job(lakeflow: Lakeflow) -> None:
    with lakeflow.task("fanout.loop", for_each_inputs=["z"]) as job:
        run = job.run()
        assert run.succeeded, run
        assert sorted(run.tasks) == ["loop"]
        assert _iteration_outputs(run) == ["z"]


def test_for_each_iterations(lakeflow: Lakeflow) -> None:
    # An iteration cannot set a task value, so a probe on the for-each task is refused before anything is created.
    with pytest.raises(LakeflowError, match="the for-each task 'loop', whose iterations cannot set task values"):
        lakeflow.job("fanout", capture_task_values={"loop": ["seen"]})
    inputs = {"loop": ["a", "b"]}
    with lakeflow.job("fanout", for_each_inputs=inputs) as job:
        run = job.run(performance_target="STANDARD")
    assert run.succeeded, run
    loop = run.task("loop")
    assert [iteration.task_key for iteration in loop.iterations] == ["each", "each"]
    assert [iteration.output for iteration in loop.iterations] == ["a", "b"]
    assert [iteration.parameters for iteration in loop.iterations] == [{"item": "a"}, {"item": "b"}]
    assert loop.for_each_stats == ForEachStats(total_iterations=2, succeeded_iterations=2, failed_iterations=0)


def test_a_failing_iteration(lakeflow: Lakeflow) -> None:
    inputs = {"loop": ["a", "fail", "set_value"]}
    with lakeflow.job("fanout", for_each_inputs=inputs) as job:
        run = job.run(performance_target="STANDARD")
    assert run.failed
    assert run.task("after").upstream_failed
    loop = run.task("loop")
    assert loop.result_state == "FAILED"
    assert [iteration.result_state for iteration in loop.iterations] == ["SUCCESS", "FAILED", "FAILED"]
    assert [iteration.parameters for iteration in loop.iterations] == [{"item": i} for i in inputs["loop"]]
    assert loop.iterations[1].raised(ValueError)
    refused = loop.iterations[2].error
    assert refused is not None and "setting task values is not supported for iterations" in refused.message
    errors = [ForEachError("Workload failed, see run output for details", 2, "RunExecutionError")]
    assert loop.for_each_stats == ForEachStats(3, 1, 2, errors)


def test_a_failing_iteration_is_not_retried(lakeflow: Lakeflow) -> None:
    # Before sanitise set disable_auto_optimization on the nested task, serverless retried each failing iteration once.
    with lakeflow.job("fanout", for_each_inputs={"loop": ["fail"]}) as job:
        assert job.job_id is not None
        loop = _task_settings(lakeflow, job.job_id, "loop").for_each_task
        assert loop is not None and loop.task is not None
        assert loop.task.disable_auto_optimization is True
        run = job.run(performance_target="STANDARD")
    assert run.failed
    [iteration] = run.task("loop").iterations
    assert iteration.raised(ValueError)
    assert [attempt.attempt for attempt in iteration.attempts] == [0]


def test_nested_task_runs_once(lakeflow: Lakeflow) -> None:
    with lakeflow.task(
        "fanout_objects.loop.inner",
        input={"id": 7, "path": "/x"},
        upstream_task_values={"extract": {"rows": 3}},
    ) as job:
        run = job.run()
        assert run.succeeded, run
        assert run.executed_tasks == ["inner"]
        inner = run.task("inner")
        assert isinstance(inner.parameters, dict)
        # As an iteration would, the body gets the numbers of an object input as doubles.
        assert inner.parameters["item_id"] == "7.0"
        assert inner.parameters["rows"] == "3"
        assert inner.output is not None
        assert json.loads(inner.output) == {"item": '{"id":7.0,"path":"/x"}', "item_id": "7.0", "rows": "3"}


def test_the_body_gets_an_object_input_as_the_loop_gives_it(lakeflow: Lakeflow) -> None:
    item = {"id": 7, "path": "/x"}
    with lakeflow.job("fanout_objects", for_each_inputs={"loop": [item]}) as job:
        looped = job.run()
        assert looped.succeeded, looped
        [iteration] = _iteration_outputs(looped)
        assert iteration is not None
    upstream = {"extract": {"rows": 3}}
    with lakeflow.task("fanout_objects.loop.inner", input=item, upstream_task_values=upstream) as job:
        lifted = job.run()
        assert lifted.succeeded, lifted
        output = lifted.task("inner").output
        assert output is not None
    assert json.loads(output) == json.loads(iteration)


def _task_settings(lakeflow: Lakeflow, job_id: int, task_key: str) -> jobs.Task:
    settings = lakeflow.ws.jobs.get(job_id).settings
    assert settings is not None
    return next(t for t in settings.tasks or [] if t.task_key == task_key)


def _iteration_outputs(run: JobRun) -> list[str | None]:
    return [iteration.output for iteration in run.task("loop").iterations]
