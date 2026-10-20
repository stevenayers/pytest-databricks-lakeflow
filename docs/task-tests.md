# Writing task tests

A task test runs one task of a bundle job in a job of its own. The plugin keeps the task's settings as deployed, replaces the upstream tasks you name with stub tasks that set task values, and adds a probe task that reports the values the task set.

## Make a task job

```python
with lakeflow.task(
    "my_example_job.transform",
    params={"param1": "hello", "param2": "world"},
    upstream_task_values={"extract": {"row_count": 42, "source_path": "/Volumes/x/y/z"}},
    capture_task_values=["rows_written"],
) as task:
    run = task.run()
    assert run.task("transform").task_values["rows_written"] == 42
```

- `ref` names the job and the task as `"job_key.task_key"`.
- `params` are job parameters for every run. The job's declared parameters are kept, and the same rules apply as for [job tests](job-tests.md#make-a-job).
- `upstream_task_values` maps upstream task keys to the task values their stubs set, or to a `Stub` that also answers the task's `{{tasks.<key>.output.<path>}}` references, see [Output references](#output-references). Each named task becomes a stub with the same key, and the target's `depends_on` is replaced by the stubs. Upstream tasks you do not name are dropped.
- `capture_task_values` lists the task value keys to capture from the task. It adds one probe task.
- `run_as` chooses who the job runs as: `None` by default, which runs it as you, `"keep"` for the template's `run_as`, a user's email, a service principal's application id, or a `jobs.JobRunAs`. See [Run as another identity](job-tests.md#run-as-another-identity).
- `children` and `extra_dependencies` work as for job tests, see [Jobs that run other jobs](job-tests.md#jobs-that-run-other-jobs) and [Test-only dependencies](job-tests.md#test-only-dependencies). The stub and probe tasks never get the dependencies.
- `environments` changes the job's serverless environments as for job tests, see [Change the settings before the job is created](job-tests.md#change-the-settings-before-the-job-is-created).
- `task_params` overrides the task's parameters as for [job tests](job-tests.md#make-a-job), such as `task_params={"transform": {"limit": "10"}}`. It is applied before the stubs are added, so only the task under test can be named, and a value that refers to `{{tasks.<key>.values.<name>}}` needs a stub for `<key>` like the task's own settings do.
- `task_overrides` sets the task's timeout and retry fields as for job tests, see [Retries and timeouts](job-tests.md#retries-and-timeouts). It can name only the task under test, as `{"<task_key>": {...}}`; any other key raises `LakeflowError`.
- `pipelines` and `schema` copy the pipeline that a pipeline task updates, as for job tests: with `pipelines="copy"` and `schema=` set to a name from `lakeflow.schema()`, the task updates a copy of its pipeline that publishes to that schema, see [Pipelines](job-tests.md#pipelines). A task that is not a pipeline task raises `LakeflowError` (`has no pipeline task to copy`).
- `for_each_inputs` gives a for-each task under test the list it iterates over, such as `lakeflow.task("fanout.loop", for_each_inputs=["a"])`, in place of the deployed `inputs`, as `for_each_inputs` does for [job tests](job-tests.md#for-each-tasks). The job holds the for-each task, which runs its nested task once per item. When the task under test has no `for_each_task`, it raises `LakeflowError`.
- `trigger` gives the task's `{{job.trigger.<path>}}` references the values a trigger would, such as `lakeflow.task("file_job.read_trigger", trigger={"type": "file_arrival", "file_arrival.location": "/Volumes/main/landing/orders/"})`, as for [job tests](job-tests.md#simulate-a-trigger). A path that the task's settings do not use raises `LakeflowError`.

`setup=` and `teardown=` work as for [job tests](job-tests.md#set-up-and-tear-down-data) and take the same three forms: a function, a list of SQL statements or a `Path` to a `.py` file. The setup task runs before the stubs, so a stub's values can never be read before the data exists, and the teardown task runs after the task and its probe:

```python
with lakeflow.schema() as schema:
    with lakeflow.task(
        "my_example_job.transform",
        params={"schema": schema, "mode": "incremental"},
        upstream_task_values={"extract": {"row_count": 2, "source_path": "/Volumes/x/y/z"}},
        setup=seed_orders,
        teardown=remove_landing_file,
    ) as task:
        assert task.run().succeeded
```

The result is an `EphemeralJob`, the same object that `lakeflow.job()` returns. `run()`, `only`, the keep rules and the result objects all work the same way, and `with` is optional.

## How stubs resolve

Both `dbutils.jobs.taskValues.get(taskKey="extract", key="source_path")` in the task's code and `{{tasks.extract.values.row_count}}` in its settings resolve by the producer's task key. A stub with the key `extract` satisfies both.

`run.task(key).parameters` shows the value a `{{tasks.x.values.y}}` reference in the task's parameters resolved to, for example `run.task("transform").parameters["row_count"] == "7"`. Job parameters are pushed to notebook tasks too, so the dict may hold more keys than the task declares, and a job parameter with the same name as a task parameter replaces it.

If the task's settings refer to `{{tasks.<key>.values...}}` of a task that is not in `upstream_task_values`, the plugin raises `LakeflowError` (`refers to task values of [...]`) before anything is created, because the reference would otherwise fail at run time. Any other `{{tasks.<key>.` reference to such a task, such as `result_state`, raises `LakeflowError` too, with a message saying that `task()` leaves the task out of the job. Unlike `run(only=...)`, which keeps a left-out task in the job so that these references resolve against its `DISABLED` run, `task()` drops the task, and the Jobs API refuses a reference to a task the job does not have.

Stub values arrive as JSON through a widget. `{{` inside a value is escaped so that the platform does not treat it as a reference, and the value reaches the task verbatim.

Other references to a stubbed task resolve against the stub's own run, as a run in the workspace confirmed: `{{tasks.<key>.run_id}}` to the stub's task run id, `{{tasks.<key>.result_state}}` to `success` and `{{tasks.<key>.error_code}}` to an empty string. `{{tasks.<key>.notebook_path}}` resolves to the helper notebook's path, not the real task's.

### Output references

A stub is a notebook task, so it has no SQL, alert or other task output, and Databricks refuses a `{{tasks.<key>.output.<path>}}` reference to a notebook task when the job is created ("Unsupported task type for the 'output' namespace."). The plugin replaces the references to a stub, and raises `LakeflowError` before anything is created for one to a notebook task of the copy that is not a stub, in `lakeflow.job()` too. Give the outputs the task reads with a `Stub`, whose `values` are the task values it sets and whose `outputs` are literals for those references, by the path after `output.`. A plain mapping is the same as `Stub(values=mapping)`. In the `sql` integration bundle, `use_results` reads `{{tasks.query.output.first_row.n}}`, `{{tasks.types.output.first_row}}` and `{{tasks.types.output.rows}}`:

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

The plugin replaces each such reference in the task's settings with the literal before the job is created, so the platform never sees it; `run.task(key).parameters` shows the literal. A string is used verbatim and any other value as compact JSON, as `row` shows, which is how the platform spells `rows` and `first_row` (see [Limitations](limitations.md)). The path must be spelt exactly as in the reference, such as `first_row.n`, `first_row`, `rows`, `alert_state`, `catalog_name`, `schema_name` or `response`. When the task refers to an output that its stub does not give, a plain mapping included, the plugin raises `LakeflowError` before anything is created: `Task 'use_results' refers to {{tasks.query.output.first_row.n}}, but the stub for 'query' has no output 'first_row.n'. Pass Stub(outputs={'first_row.n': ...}) for 'query' in upstream_task_values.` The stub task itself is still created, so `depends_on`, `run.injected_tasks` and `{{tasks.<key>.run_id}}` keep working. `lakeflow.job()` takes a `Stub` in its `upstream_task_values` too, and rewrites the references of every task of the copy.

## Rules for stub values

The plugin checks these before creating the job and raises `LakeflowError` otherwise:

- Each value must be JSON-serialisable.
- Each value must be at most 49,152 characters as JSON, which is the platform's limit for a task value.
- Keys must be at most 512 characters.
- You cannot stub the task under test.
- An output cannot contain `{{`, which the platform would resolve as a reference, and one that is not a string must be JSON-serialisable.

## Read captured values

`run.task("transform").task_values` is a dict of the captured values, and `missing_task_values` lists the keys that were not captured. Reading a missing key raises a `KeyError` whose message gives the reason:

- `Task 'transform' never set value 'rows_written'`: the task ran and did not set it.
- `Task 'load' did not run (UPSTREAM_FAILED), so value 'anything' was not captured`: the task was skipped.
- `Value 'x' of task 't' was not captured: probe task __pytest_probe__t ended FAILED ...`: the probe itself failed or its output could not be read.

The probe has `run_if: ALL_DONE`, so values the task set before failing are still captured:

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

`upstream_from` takes the stub values from a run of the same job instead of from your test. Each task that the task depends on in the deployed job becomes a stub that sets the values that task set in that run, so you can re-run one task against real upstream data without writing the values out:

```python
def test_replay_transform_from_a_full_run(lakeflow: Lakeflow) -> None:
    first = lakeflow.job("my_example_job", capture_task_values={"extract": ["row_count", "source_path"]}).run()
    with lakeflow.task("my_example_job.transform", upstream_from=first) as task:
        run = task.run()
        assert run.succeeded, run
        assert run.task("transform").output == "hello world"
```

The Jobs API does not return task values, so the first run must capture them: pass `capture_task_values` with every upstream task and every key the task reads. `lakeflow.task()` raises `LakeflowError` before anything is created when `upstream_from` is a run of another job, or when an upstream task was not part of that run (left out by `only` or disabled) or has no captured values. An upstream task that sets no task values, such as one that only writes a table, has none to capture, so `upstream_from` raises for it too; stub it by passing it explicitly, such as `upstream_task_values={"write_table": {}}`. Only runs made by the plugin qualify; a production run had no probes, so its task values cannot be recovered.

`upstream_task_values` still works alongside it. An entry replaces the values `upstream_from` holds for that task, and the stubs it names are added to those from the run, so a what-if test changes one value and keeps the rest:

```python
captured = first.task("extract").task_values
what_if = lakeflow.task(
    "my_example_job.transform",
    upstream_from=first,
    upstream_task_values={"extract": {**captured, "row_count": 0}},
)
```

The replayed values go through the [rules for stub values](#rules-for-stub-values) like any others.

## Test an if/else condition

A condition task that compares an upstream task value can be tested on its own, with the value set by a stub. Only the condition task and its stubs are in a task job, so the branches downstream of it are not part of the run; assert on its outcome instead. In the `branches` integration bundle, `check` compares `{{tasks.check_input.values.rows}}` with `> 0`:

```python
@pytest.mark.parametrize(("rows", "expected"), [(0, False), (5, True)])
def test_condition_task_on_its_own(lakeflow: Lakeflow, rows: int, expected: bool) -> None:
    run = lakeflow.task("branching_job.check", upstream_task_values={"check_input": {"rows": rows}}).run()
    assert run.succeeded, run
    assert run.task("check").condition_outcome is expected
    assert run.branch_taken("check") is expected
    assert run.executed_tasks == ["check"]
```

The stub is in `run.injected_tasks`, not `run.tasks`, so `executed_tasks` lists only the condition task.

`branch_taken` raises `LakeflowError` if the task is not a condition task. To check which downstream tasks ran or were `EXCLUDED`, use a [job test](job-tests.md#branches).

## Helper compute

Stubs, probes and the setup and teardown tasks are notebook tasks. The two helper notebooks are uploaded once per session to `helpers_path`, by default `/Users/<you>/.pytest-databricks-lakeflow/`, and overwritten on each upload.

By default the helper tasks run on serverless compute, in a plugin-owned environment named `__pytest_serverless` that is appended to the job's `environments`, with `helpers_environment_version` as its environment version. The default version is `"4"`. It applies to every job of that `Lakeflow`, and `environments=` cannot change `__pytest_serverless`. For one job whose setup function needs a newer runtime, derive a `Lakeflow` with another version, which shares the session's deploy: `dataclasses.replace(lakeflow, helpers_environment_version="5").task("my_example_job.load", setup=seed_orders)`.

A job holds at most 10 environments. When the template already has 10 and none of them is `__pytest_serverless`, the plugin raises `LakeflowError` before creating the job, listing the template's environment keys: one must go from the bundle job to make room for the helper environment, or pass `helpers_compute` to run the helper tasks on a job cluster.

Without serverless jobs, name a job cluster from the job's `job_clusters`:

```python
Lakeflow(helpers_compute="main_cluster")
```

The helper tasks then run on that cluster, and the plugin raises `LakeflowError` if the job has no cluster with that key. The task under test keeps its own compute either way. With `existing_cluster_id` as well, the helper tasks move to that running cluster along with every other task that was on a job cluster. With `single_node=True` instead, the `helpers_compute` cluster becomes a single node cluster like every other job cluster in the copy.

Each helper task costs 7 to 30 seconds of serverless compute, so a task test typically takes a minute end to end.

## What a single-task job loses

Only the target task is kept. On a shared job cluster, a task can import libraries that other tasks declared, and on its own it loses them. Declare every library on the task that needs it. Serverless tasks with an `environment_key` are unaffected, because their dependencies come from the environment it names, and the job's `environments` are kept. A serverless notebook task without one, such as `my_example_job.load`, takes no dependencies from the job settings, so `environments` and `extra_dependencies` never reach it: install them with `%pip install` in the notebook, or move it onto an environment, see [Test-only dependencies](job-tests.md#test-only-dependencies). To give a serverless task a dependency it needs on its own, pass `environments` with the task's environment, such as `lakeflow.task("my_example_job.my_wheel_task", environments={"default": {"dependencies": ["/Volumes/main/libs/wheels/my_pkg-0.2.0-py3-none-any.whl", "requests"]}})`; `dependencies` replaces the environment's list, so name everything the task needs, or use `extra_dependencies` to append.

## Task tests with tables

A task that writes tables takes the schema as a job parameter, so a temporary schema works the same as in a job test:

```python
@pytest.mark.parametrize(("greeting", "expected"), [("hello", "hello world"), ("goodbye", "goodbye world")])
def test_single_task(lakeflow: Lakeflow, greeting: str, expected: str) -> None:
    with lakeflow.schema() as schema:
        with lakeflow.task(
            "my_example_job.transform",
            params={"param1": greeting, "param2": "world", "schema": schema, "mode": "incremental"},
            upstream_task_values={"extract": {"row_count": 42, "source_path": "/Volumes/x/y/z"}},
            capture_task_values=["rows_written"],
        ) as task:
            run = task.run()
            assert run.succeeded
            assert run.task("transform").output == expected
            assert run.task("transform").task_values["rows_written"] == 42
        assert lakeflow.ws.tables.exists(f"{schema}.example_rows").table_exists
```

A pipeline task writes to the schema its pipeline publishes to, which a job parameter cannot change. `pipelines="copy"` runs the task against a copy of the pipeline that publishes to the temporary schema instead, here the `pipelines` integration bundle's `refresh` task:

```python
def test_refresh_into_a_temporary_schema(lakeflow: Lakeflow) -> None:
    with lakeflow.schema() as schema:
        with lakeflow.task("pipeline_job.refresh", pipelines="copy", schema=schema) as task:
            run = task.run(full_refresh=True)
            assert run.succeeded, run
            assert lakeflow.ws.tables.exists(f"{schema}.numbers").table_exists
```

The copy is deleted with the tables it publishes when the task job closes, so check them inside the `with` block. See [Pipelines](job-tests.md#pipelines) for what the copy keeps and drops.

## Running a subset of a task job

`only` works on task jobs too. The plugin adds the stubs that the selected tasks depend on, the probes on them and the setup and teardown tasks, so `run(only=["transform"])` still runs the stubs first and the probe afterwards.
