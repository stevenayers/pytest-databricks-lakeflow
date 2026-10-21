# Task tests

A task test runs one task of a bundle job in a job of its own. The plugin keeps the task's settings as deployed, replaces the upstream tasks you name with stub tasks that set task values, and adds a probe task that reports the values the task set.

## Run one task on its own

```python
from pytest_databricks_lakeflow import Lakeflow


def test_transform(lakeflow: Lakeflow) -> None:
    with lakeflow.task(
        "my_example_job.transform",
        params={"param1": "hello", "param2": "world"},
        upstream_task_values={"extract": {"row_count": 42, "source_path": "/Volumes/x/y/z"}},
        capture_task_values=["rows_written"],
    ) as task:
        run = task.run()
        assert run.succeeded, run
        assert run.task("transform").output == "hello world"
        assert run.task("transform").task_values["rows_written"] == 42
```

- `ref` names the job and the task as `"job_key.task_key"`, or a task nested in a for-each task as `"job_key.loop_key.nested_key"`, see [Run a for-each body once](#run-a-for-each-body-once).
- `params` are job parameters for every run. The job's declared parameters are kept, and the same rules apply as for [job tests](job-tests.md#make-a-copy-of-a-job).
- `upstream_task_values` maps upstream task keys to the task values their stubs set. Each named task becomes a stub with the same key, and the target's `depends_on` is replaced by the stubs. Upstream tasks you do not name are dropped from the job.
- `capture_task_values` lists the task value keys to capture from the task. It adds one probe task.

The result is an `EphemeralJob`, the same object that `lakeflow.job()` returns, so `run()`, `only`, the keep rules and the result objects work the same way, and `with` is optional.

Everything after `params` is keyword-only. Most options work as for `job()`:

| Option | Changes | Recipe |
| --- | --- | --- |
| `upstream_from` | Takes the stub values from a previous run | [Replay a task from a previous run](#replay-a-task-from-a-previous-run) |
| `input` | The item a for-each body runs with | [Run a for-each body once](#run-a-for-each-body-once) |
| `task_params` | The task's own parameters. It may name only the task under test, and is applied before the stubs, so a `{{tasks.x.values.y}}` it adds needs `x` in `upstream_task_values` | [Override one task's parameters](recipes/job-settings.md#override-one-tasks-parameters) |
| `task_overrides` | The task's timeout and retries. Any key but the task under test raises `ValueError` | [Test retries and timeouts](recipes/job-settings.md#test-retries-and-timeouts) |
| `for_each_inputs` | A list for a for-each task under test, in place of the deployed `inputs`. A task without a `for_each_task` raises `LakeflowError` | [For-each tasks](recipes/task-types.md#for-each-tasks) |
| `name`, `run_as`, `children`, `extra_dependencies`, `environments`, `setup`, `teardown`, `teardown_at`, `run_as_task`, `trigger`, `saved_queries`, `pipelines`, `schema` | As for `job()` | [Make a copy of a job](job-tests.md#make-a-copy-of-a-job) |

The stub and probe tasks never get `extra_dependencies`. The setup runs before the stubs, so a stub's values are never read before the data exists, and the teardown runs after the task and its probe: over Databricks Connect before and after the run, or as tasks inside it.

## How stubs resolve

Both `dbutils.jobs.taskValues.get(taskKey="extract", key="source_path")` in the task's code and `{{tasks.extract.values.row_count}}` in its settings resolve by the producer's task key. A stub with the key `extract` satisfies both, and `run.task("transform").parameters["row_count"]` shows the value the reference resolved to.

- A `{{tasks.<key>.values...}}` reference to a task that is not in `upstream_task_values` raises `LakeflowError` before anything is created, because the reference would fail at run time. Any other `{{tasks.<key>.` reference to such a task, such as `result_state`, raises too: `task()` drops the task from the job, and the Jobs API refuses a reference to a task the job does not have.
- Stub values arrive as JSON through a widget. `{{` inside a value is escaped, so the value reaches the task verbatim rather than being read as a reference.
- Other references to a stubbed task resolve against the stub's own run: `{{tasks.<key>.run_id}}` to the stub's task run id, `{{tasks.<key>.result_state}}` to `success`, `{{tasks.<key>.error_code}}` to an empty string, and `{{tasks.<key>.notebook_path}}` to the helper notebook's path.

### Rules for stub values

The plugin checks these before creating the job and raises `ValueError` otherwise:

- Each value must be JSON-serialisable and at most 49,152 characters as JSON, the platform's limit for a task value.
- Keys must be at most 512 characters.
- You cannot stub the task under test.

## Stub a task's output

A stub is a notebook task, so it has no SQL, alert or other task output, and the Jobs API refuses a `{{tasks.<key>.output.<path>}}` reference to a notebook task when the job is created. Give the outputs the task reads with a `Stub`, whose `values` are the task values it sets and whose `outputs` are literals for those references, by the path after `output.`. In the `sql` integration bundle, `use_results` reads `{{tasks.query.output.first_row.n}}`, `{{tasks.types.output.first_row}}` and `{{tasks.types.output.rows}}`:

```python
from pytest_databricks_lakeflow import Lakeflow, Stub


def test_use_results_with_stubbed_queries(lakeflow: Lakeflow) -> None:
    rows = [{"n": "7", "m": "", "d": "2.5", "day": "2026-02-01", "b": "false"}]
    upstream = {
        "query": Stub(outputs={"first_row.n": "7"}),
        "types": Stub(values={"stage": "done"}, outputs={"first_row": rows[0], "rows": rows}),
    }
    with lakeflow.task("sql_job.use_results", upstream_task_values=upstream) as task:
        run = task.run()
        assert run.succeeded, run
        assert run.task("use_results").parameters["count"] == "7"
        assert run.task("use_results").parameters["row"] == '{"n":"7","m":"","d":"2.5","day":"2026-02-01","b":"false"}'
```

- A plain mapping in `upstream_task_values` is the same as `Stub(values=mapping)`.
- The plugin replaces each reference in the task's settings with the literal before the job is created, so `run.task(key).parameters` shows the literal. A string is used verbatim and anything else as compact JSON, which is how the platform spells `rows` and `first_row`: every value a string, and `NULL` as an empty string.
- Spell the path exactly as the reference does: `first_row.n`, `first_row`, `rows`, `alert_state`, `catalog_name`, `schema_name` or `response`.
- A reference to an output the stub does not give raises `LakeflowError` before anything is created, naming the stub and the path. An output that contains `{{`, or one that is neither a string nor JSON-serialisable, raises `ValueError`.
- The stub task is still created, so `depends_on`, `run.injected_tasks` and `{{tasks.<key>.run_id}}` keep working. `lakeflow.job()` takes a `Stub` too, and rewrites the references of every task of the copy.

## Read captured values

`run.task("transform").task_values` is a dict of the captured values, and `missing_task_values` lists the keys that were not captured. A missing key raises a `KeyError` that says why, see [Read task values](job-tests.md#read-task-values). The probe has `run_if: ALL_DONE`, so values the task set before failing are still captured:

```python
def test_stage_is_recorded_before_the_failure(lakeflow: Lakeflow) -> None:
    with lakeflow.task(
        "my_example_job.transform",
        params={"param1": "explode"},
        upstream_task_values={"extract": {"row_count": 1, "source_path": "/Volumes/x/y/z"}},
        capture_task_values=["stage"],
    ) as task:
        run = task.run()
        assert run.failed
        assert run.task("transform").raised(ValueError)
        assert run.task("transform").task_values["stage"] == "validation"
```

## Replay a task from a previous run

`upstream_from` takes the stub values from a run of the same job instead of from your test. Each task that the target depends on in the deployed job becomes a stub that sets the values that task set in that run, so one task re-runs against real upstream data:

```python
def test_replay_transform_from_a_full_run(lakeflow: Lakeflow) -> None:
    first = lakeflow.job("my_example_job", capture_task_values={"extract": ["row_count", "source_path"]}).run()
    with lakeflow.task("my_example_job.transform", upstream_from=first) as task:
        run = task.run()
        assert run.succeeded, run
        assert run.task("transform").output == "hello world"
```

- The first run must capture the values with `capture_task_values`, naming every upstream task and every key the target reads. The Jobs API does not return task values, so a production run cannot be replayed.
- `ValueError` is raised before anything is created when `upstream_from` is a run of another job, and `LakeflowError` when an upstream task was not part of that run or has no captured values. An upstream task that sets no task values, such as one that only writes a table, has none to capture: stub it explicitly with `upstream_task_values={"write_table": {}}`.
- `upstream_task_values` works alongside it. An entry replaces the values `upstream_from` holds for that task, so a what-if test changes one value and keeps the rest:

```python
captured = first.task("extract").task_values
what_if = lakeflow.task(
    "my_example_job.transform",
    upstream_from=first,
    upstream_task_values={"extract": {**captured, "row_count": 0}},
)
```

To replay the downstream half of a whole job instead, see [Replay a run's task values](recipes/runs.md#replay-a-runs-task-values).

## Test a condition task on its own

An if/else condition task that compares an upstream task value can be tested alone, with the value set by a stub. Only the condition task and its stubs are in the job, so assert on its outcome rather than on the branches. In the `branches` integration bundle, `check` compares `{{tasks.check_input.values.rows}}` with `> 0`:

```python
import pytest

from pytest_databricks_lakeflow import Lakeflow


@pytest.mark.parametrize(("rows", "expected"), [(0, False), (5, True)])
def test_condition_task_on_its_own(lakeflow: Lakeflow, rows: int, expected: bool) -> None:
    run = lakeflow.task("branching_job.check", upstream_task_values={"check_input": {"rows": rows}}).run()
    assert run.succeeded, run
    assert run.task("check").condition_outcome is expected
    assert run.branch_taken("check") is expected
    assert run.executed_tasks == ["check"]
```

The stub is in `run.injected_tasks`, not `run.tasks`. To check which downstream tasks ran or were excluded, use a job test, see [Branches](recipes/task-types.md#branches).

## Run a for-each body once

A three-part `ref` names the task nested in a for-each task, and `input=` gives the one item it runs with. The job holds the body in place of the for-each task, so it runs once as an ordinary task, named by its own key. In the `for_each` integration bundle, the for-each task `loop` of `fanout_objects` runs `inner` over a list of objects, after `extract`, which sets the task value `rows`:

```python
def test_nested_task_runs_once(lakeflow: Lakeflow) -> None:
    with lakeflow.task(
        "fanout_objects.loop.inner",
        input={"id": 7, "path": "/x"},
        upstream_task_values={"extract": {"rows": 3}},
    ) as job:
        run = job.run()
        assert run.succeeded, run
        assert run.executed_tasks == ["inner"]
        assert run.task("inner").parameters["item_id"] == "7.0"
```

- `input` is a string, number, boolean or mapping, as a for-each item is. The body's `{{input}}` and `{{input.<key>}}` references become job parameters whose defaults hold the text an iteration would give them: a string as it is, an integer as written, and an object as compact JSON with every number as a double, so `{{input.id}}` is `7.0`. Read an object with `json.loads` rather than comparing its text, because the platform does not keep the key order of a larger object.
- The body keeps its own settings and takes the for-each task's `depends_on`, so `upstream_task_values` and `upstream_from` stub the for-each task's upstream tasks. The for-each task's `inputs` and `concurrency` are dropped. `task_params` and `task_overrides` name the body by its own key.
- `LakeflowError` is raised before anything is created when the for-each task has no task with that key, when the body refers to `{{input}}` and `input` is missing, when `input` is given and the body does not read it or lacks a key it reads, or when the job parameters would exceed the 10,000 characters as JSON that `jobs/create` accepts. `ValueError` is raised when `input` contains `{{`, has another type, or comes with a two-part `ref`.
- A body that sets task values fails in its loop, because an iteration cannot set them, but succeeds once lifted. `capture_task_values` on a body therefore emits a `UserWarning`.

To shorten the list that a for-each task iterates over instead, see [For-each tasks](recipes/task-types.md#for-each-tasks).

## Helper compute

Stubs, probes, and setup and teardown steps that run as tasks, are notebook tasks. The two helper notebooks are uploaded once per session to `helpers_path`, by default `/Users/<you>/.pytest-databricks-lakeflow/`. A setup or teardown step over Databricks Connect uses none of this: it runs from the test process, see [Run a step as a task](recipes/data.md#run-a-step-as-a-task).

By default the helper tasks run on serverless compute, in a plugin-owned environment named `__pytest_serverless` that is appended to the job's `environments`, with `helpers_environment_version` as its version, `"4"` by default. Each helper task takes 28 to 32 seconds of serverless compute before its own work, so a task test typically takes a minute end to end.

- A job holds at most 10 environments. When the template already has 10, the plugin raises `LakeflowError` listing them before creating the job: drop one from the bundle job, or run the helper tasks on a job cluster.
- Without serverless jobs, name a job cluster from the job's `job_clusters` with `Lakeflow(helpers_compute="main_cluster")`, see [Run the helper tasks on a job cluster](recipes/compute.md#run-the-helper-tasks-on-a-job-cluster). The task under test keeps its own compute either way.
- `environments=` cannot change `__pytest_serverless`. For one job whose setup task needs a newer runtime, derive a `Lakeflow` with another version, which shares the session's deploy: `dataclasses.replace(lakeflow, helpers_environment_version="5")`.

## What a single-task job loses

Only the target task is kept. On a shared job cluster a task can import libraries that other tasks declared, and on its own it loses them, so declare every library on the task that needs it. Serverless tasks with an `environment_key` are unaffected, because the job's `environments` are kept. A serverless notebook task without one takes no dependencies from the job settings, so `environments` and `extra_dependencies` never reach it: install them with `%pip install` in the notebook, or move it onto an environment, see [Add test-only dependencies](recipes/job-settings.md#add-test-only-dependencies).

## Tables, data and other task types

- A task that writes tables takes the schema as a job parameter, so `lakeflow.schema()` works as in a job test, and `setup=` and `teardown=` seed and clean up data around the task, see [Test data](recipes/data.md).
- A pipeline task writes to the schema its pipeline publishes to. `pipelines="copy"` with `schema=` runs it against a copy of the pipeline that publishes to the temporary schema, see [Copy the pipelines of a job](recipes/task-types.md#copy-the-pipelines-of-a-job).
- A dbt task's commands are a list, so `task_params` replaces them, and `Lakeflow(dbt_artifacts=True)` parses `run_results.json` into `run.task(...).dbt`, see [dbt tasks](recipes/task-types.md#dbt-tasks).
- `only` works on task jobs too: `run(only=["transform"])` still runs the stubs first and the probe afterwards.
