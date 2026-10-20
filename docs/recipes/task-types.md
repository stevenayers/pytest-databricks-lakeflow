# Task types

What each task type gives back on its `TaskRun`, and the options that exist for it. The examples name jobs from the plugin's [integration bundles](https://github.com/stevenayers/pytest-databricks-lakeflow/tree/main/tests/integration/bundles).

## Read the rows of a SQL file task

A SQL task that runs a file reports the rows it returned, and `TaskRun.sql_result` holds them as a `SqlResult`. In the `sql` bundle, the task `parameterised` runs `SELECT :n AS n` with `n` from the job parameter of the same name:

```python
def test_parameterised_query(lakeflow: Lakeflow) -> None:
    with lakeflow.job("sql_job", params={"n": "7"}) as job:
        run = job.run().raise_for_failure()
        result = run.task("parameterised").sql_result
        assert result is not None
        assert result.columns == ["n"]
        assert result.first_row == {"n": "7"}
        assert not result.truncated
```

- `rows` is a list of dicts by column name. Values are strings as the platform reports them, such as `"1"`, `"1.5"`, `"true"` or `"2026-01-01"`, and a NULL is `None`. `types` holds the platform's type name of each column, such as `int` or `decimal`.
- `first_row` raises `LakeflowError` when the query returned no rows. The platform keeps at most 1,000 rows, and `truncated` is true when it cut them.
- `query_text` is the file as it ran, with parameter markers such as `:n` kept; the values bound to them are in the task's `parameters`. The platform adds every job parameter to each SQL file task's parameters, so compare the entries you set.
- `sql_output` holds the platform's own output object, with `warehouse_id`, `output_link` and the SQL that ran, but no rows. The rows come from a field the Jobs API reference does not document, read with one extra request per SQL file task that ran, see [Limitations](../reference/limitations.md).

## Read the rows of a saved query

A SQL task that runs a saved query reports no rows: the platform keeps only the query text and a link to the result page. `saved_queries="inline"` runs each saved query task of the copy as a SQL file task instead, so `sql_result` holds its rows:

```python
def test_open_orders_are_counted(lakeflow: Lakeflow) -> None:
    with lakeflow.job("daily_report", saved_queries="inline") as job:
        run = job.run().raise_for_failure()
        result = run.task("open_orders").sql_result
        assert result is not None
        assert result.rows == [{"n": "3"}]
```

- `job()` reads each enabled saved query task's query and uploads its text to `helpers_path` as a workspace file named `query_<run id>_<task key>_<8 hex digits>.sql`, after `USE CATALOG` and `USE SCHEMA` lines for the catalog and schema the query sets, so unqualified table names resolve as they do in the saved query. The file is deleted with the job, or kept with it, and `--lakeflow-sweep-notebooks` deletes leftovers.
- A saved query falls back to its own parameter defaults, and a SQL file task has none, so the copy adds each default that neither the task nor a job parameter sets: text, a number or a single enum value, as a string. `task_params` wins over a default: `task_params={"open_orders": {"region": "apac"}}` runs the query with that region. A date, a date range, several enum values or a query-backed default raises `LakeflowError` before anything is created, naming the task and the parameter; set it with `task_params`.
- The inlined query runs as the job's `run_as`, normally you, not as the query's owner, and without the SQL editor's automatic `LIMIT 1000`. A saved query that does not exist raises `LakeflowError`.
- `Lakeflow(saved_queries="inline")` makes it the default; `saved_queries="deployed"` on a call runs that copy's saved queries as deployed, with `sql_result` `None`.

## Alert tasks

An alert task succeeds whether or not the alert triggered, so assert on `alert_state`:

```python
@pytest.mark.parametrize(("key", "state"), [("fires", "TRIGGERED"), ("quiet", "OK")])
def test_alert_task_reports_its_state(lakeflow: Lakeflow, key: str, state: str) -> None:
    with lakeflow.job("alert_job") as job:
        run = job.run(only=[key])
        assert run.succeeded, run
        assert run.task(key).alert_state == state
```

- `alert_state` is `TRIGGERED`, `OK` or `ERROR`, read from `alert_output` or, for a legacy SQL alert task, from `sql_output.alert_output`. It is `None` for any other task.
- When the alert cannot be evaluated, for example because its query failed, the task fails with `alert_state` `ERROR`. The Jobs API reports no error for it, so `task.error` holds the termination message with `type` `None`.
- An alert task notifies its subscribers, or the alert's own, and has no pause flag, so the copy cannot silence it. Disable it when the test does not need it: `lakeflow.job("reporting", disable=["check"])`. SQL alert subscriptions on legacy SQL tasks and dashboard subscriptions are paused in every copy by `side_effects="neutralise"`, the default.

## Branches

An if/else condition task records its outcome, and the tasks on the branch that was not taken end `EXCLUDED`, which does not fail the run. In the `branches` bundle, `check_input` sets the task value `rows`, `check` compares it with `> 0`, and `on_true` and `on_false` depend on the two outcomes:

```python
def test_no_rows_takes_the_false_branch(lakeflow: Lakeflow) -> None:
    with lakeflow.job("branching_job", params={"rows": "0"}) as job:
        run = job.run()
        assert run.succeeded, run
        assert run.branch_taken("check") is False
        assert run.task("on_true").excluded
        assert run.executed_tasks == ["check", "check_input", "on_false"]
```

- `run.branch_taken(key)` returns the condition task's `condition_outcome`. It raises `LakeflowError` for a task that is not a condition task or has no outcome; check `result_state` then.
- The `==` and `!=` operators compare their operands as text, so a task value of `0` and a right operand of `0.0` are not equal. `<`, `>`, `<=` and `>=` compare numbers. `lakeflow.check(key, rules=ALL_RULES)` reports an `==` or `!=` on a number, see [Check every job's wiring](ci.md#check-every-jobs-wiring-without-running-it).
- To test the condition alone, drive it with a stub in a [task test](../task-tests.md#test-a-condition-task-on-its-own).

### Force a branch

To test a branch's downstream tasks without arranging upstream data that sends the run down it, `force_conditions` maps condition task keys to the outcome every run of the copy takes:

```python
def test_forced_true_branch(lakeflow: Lakeflow) -> None:
    with lakeflow.job("branching_job", params={"rows": "0"}, force_conditions={"check": True}) as job:
        run = job.run()
        assert run.succeeded, run
        assert run.branch_taken("check") is True
        assert run.task("on_false").excluded
        assert run.executed_tasks == ["check", "check_input", "on_true"]
```

- The copy's condition task compares constants, `true == true` or `true == false`. The original operands are logged at INFO.
- The condition task keeps its `depends_on`, so the tasks upstream of it still run. To run one downstream task without them, use a [task test](../task-tests.md).
- A key that is not a task, a task that is not a condition task, and a condition task that is disabled or downstream of a disabled task raise `LakeflowError` before the job is created. An outcome that is not `True` or `False` raises `ValueError`. Condition tasks nested in a for-each task cannot be forced.

## For-each tasks

A for-each task runs its nested task once per item of its `inputs`, so a copy of a job that loops over two hundred items runs two hundred iterations. `for_each_inputs` gives the copy a short list. In the `for_each` bundle, the job `fanout` has a for-each task `loop` that runs the notebook task `each` once per item, then the task `after`:

```python
def test_one_iteration_fails(lakeflow: Lakeflow) -> None:
    with lakeflow.job("fanout", for_each_inputs={"loop": ["a", "fail"]}) as job:
        run = job.run()
    loop = run.task("loop")
    assert loop.result_state == "FAILED"
    assert [iteration.result_state for iteration in loop.iterations] == ["SUCCESS", "FAILED"]
    assert loop.iterations[0].output == "a"
    assert loop.iterations[1].parameters == {"item": "fail"}
    assert loop.iterations[1].raised(ValueError)
    assert (loop.for_each_stats.total_iterations, loop.for_each_stats.failed_iterations) == (2, 1)
```

- Each list replaces the task's `for_each_task.inputs` as JSON. A `{{` in an item is escaped, so it reaches the iteration as written. An empty list runs no iteration, succeeds, and lets the downstream tasks run. Without `for_each_inputs`, the copy keeps the deployed `inputs`.
- `run.task("loop")` describes the for-each task as one task, whose `result_state` is the platform's verdict on the whole loop. Its `iterations` list one `TaskRun` per input, in iteration order, each with its own `output`, `error`, `logs`, `timing` and `parameters`; the input itself shows wherever the nested task passes `{{input}}`. A retried iteration is folded into one `TaskRun` with every attempt in `attempts`. `for_each_stats` holds the platform's counts and its errors grouped by message.
- A key that is not a task of the job, or a task without a `for_each_task`, raises `LakeflowError` listing the job's for-each tasks. A list that is not JSON-serialisable, is longer than 5,000 characters as JSON, mixes objects with primitives, or holds `None` or a nested list raises `ValueError`, because `jobs/create` would refuse it.
- An iteration cannot set task values, so `capture_task_values` refuses a for-each task and the task nested in it. Return a value with `dbutils.notebook.exit()` and read it from the iteration's `output`.
- The nested task cannot be the target of `run(only=...)`. To run it once without its loop, see [Run a for-each body once](../task-tests.md#run-a-for-each-body-once). On `lakeflow.task("fanout.loop", for_each_inputs=["a"])`, the list is for the task under test.
- The iterations are read once the run has finished, with one `get_run` on the for-each task run and two more requests per attempt of each iteration, so a loop over many items makes the result slower to build.

## Simulate a trigger

A copy has no trigger and is started with run-now, so `{{job.trigger.type}}` resolves to `one_time`, and `jobs/create` refuses `{{job.trigger.file_arrival.*}}` and `{{job.trigger.table_update.*}}` references outright. `trigger` maps the path of each reference after `job.trigger.` to the value it takes. In the `triggers` bundle, `file_job` has a file arrival trigger and its task reads `{{job.trigger.type}}`, `{{job.trigger.file_arrival.location}}` and `{{job.trigger.time.iso_date}}`:

```python
def test_new_file(lakeflow: Lakeflow) -> None:
    trigger = {"type": "file_arrival", "file_arrival.location": "/Volumes/main/landing/orders/"}
    with lakeflow.job("file_job", trigger=trigger) as job:
        run = job.run()
        assert run.succeeded, run
        assert run.task("read_trigger").output_json["kind"] == "file_arrival"
        assert run.trigger_type == "ONE_TIME"
```

- The keys are dotted paths spelt as the references spell them, such as `type`, `table_update.updated_tables`, `file_arrival.location` or `time.iso_date`. Each value is a string, passed as given; anything else raises `ValueError`. A reference the platform fills with a list, such as `table_update.updated_tables`, gets exactly the string you write, so write it in the form the job code parses.
- Each reference to a path in `trigger` is rewritten to a `__pytest_trigger_<path>` job parameter with the value as its default. Every run of the copy sees the same values; make another copy for other values. `run.trigger_type` stays `ONE_TIME`: the simulated values are what the task code sees.
- A path that no reference uses raises `LakeflowError` before anything is created, listing the trigger references the copy has. A reference that `trigger` leaves out, such as `{{job.trigger.time.iso_date}}` above, emits a `UserWarning` and keeps the value run-now gives it. A file arrival or table update reference that `trigger` does not set raises `LakeflowError` when the job is created, because `jobs/create` would refuse it.
- `lakeflow.task()` takes `trigger` too, for the references of the task under test.

## Pipeline tasks

A pipeline task updates the deployed pipeline, so every test that runs it changes the deployed pipeline's tables, unless the job is made with `pipelines="copy"`.

### Choose how a pipeline task refreshes

```python
def test_full_refresh(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_pipeline_job") as job:
        assert job.run(full_refresh=True).succeeded
        assert job.run(refresh_selection=["bronze_orders"]).succeeded
        assert job.run(reset_checkpoint_selection=["bronze_orders"]).succeeded
```

- `full_refresh`, `full_refresh_selection`, `refresh_selection`, `refresh_flow_selection` and `reset_checkpoint_selection` are sent as the run's `pipeline_params` and leave the job's settings unchanged. The selections list tables, flows, and streaming flows whose checkpoints are reset so that they read their source again from the start.
- They apply to every pipeline task in the run. Passing one to a job without a pipeline task raises `LakeflowError` before the run starts. A bare string in place of a list raises `ValueError`. The four selections need databricks-sdk 0.110.0 or later.

### Read the pipeline update

`TaskRun.pipeline_update` is the update the task started, as a `PipelineUpdate`, so a failed refresh can be read from the test rather than from the pipeline's page:

```python
def test_pipeline_full_refresh(lakeflow: Lakeflow) -> None:
    with lakeflow.job("pipeline_job") as job:
        run = job.run(full_refresh=True)
    update = run.task("refresh").pipeline_update
    assert update is not None
    assert update.state == "COMPLETED"
    assert update.full_refresh
    assert update.events(level="ERROR") == []
```

- `pipeline_id` and `update_id` name the update, `state` is `COMPLETED`, `FAILED` or `CANCELED`, `cause` is `JOB_TASK` for a pipeline task and `API_CALL` for `EphemeralPipeline.run()`, and `full_refresh` says whether it refreshed every table.
- `events(level=None)` reads the events the update logged, newest first, as `pipelines.PipelineEvent`s with `level`, `event_type`, `message` and, for an error, `error`. `level` keeps one level, such as `"ERROR"`.
- The Jobs API does not say which update a pipeline task started, so the plugin takes the update created between the task's start and end. Two tasks updating the same pipeline in one run can make the match ambiguous. When none matches, `pipeline_update` is `None` and a warning is logged.
- The Jobs API reports the error "Please refer to the logs for this pipeline in the pipelines page." for every pipeline task, also one that succeeded, so the plugin drops it when the task succeeded.

### Copy the pipelines of a job

`pipelines="copy"` with `schema=` copies each pipeline that an enabled pipeline task of the copy updates and points those tasks at the copies, which publish to the schema you pass:

```python
def test_job_copy_points_at_the_pipeline_copy(lakeflow: Lakeflow) -> None:
    with lakeflow.schema() as schema, lakeflow.job("pipeline_job", pipelines="copy", schema=schema) as job:
        [copy] = job.pipeline_copies
        run = job.run(full_refresh=True)
        assert run.succeeded, run
        assert run.task("refresh").pipeline_update.pipeline_id == copy.pipeline_id
```

- Each pipeline is copied once, however many tasks update it, named `[pytest] <test id> <pipeline key>`. The copies are created just before the job, after `modify_settings`, and closed just after it with the same keep rules, so a kept job never points at a deleted pipeline. `job.pipeline_copies` holds them as `EphemeralPipeline`s.
- `pipelines="copy"` without `schema`, or `schema` without `pipelines="copy"`, raises `ValueError`. A job with no enabled pipeline task raises `LakeflowError`. `lakeflow.task()` takes both for a pipeline task under test.
- Put `lakeflow.schema()` first in the `with` statement, so the copy is deleted before its schema is dropped.

### Copy a pipeline on its own

`lakeflow.pipeline(key, schema)` copies the bundle pipeline `key`, and `run()` starts an update and waits for it:

```python
def test_pipeline_copy(lakeflow: Lakeflow) -> None:
    with lakeflow.schema() as schema, lakeflow.pipeline("numbers", schema=schema) as pipeline:
        update = pipeline.run(full_refresh=True)
        assert update.state == "COMPLETED", update.events(level="ERROR")
        assert lakeflow.ws.tables.exists(f"{schema}.numbers").table_exists
```

- The copy is made from the deployed pipeline's spec, so it runs the code the last `bundle deploy` uploaded. It publishes to `schema`, runs in development mode as you, is named `[pytest] <test id>` and tagged like a job, and leaves out the deployed pipeline's `deployment`, `continuous`, `trigger`, `notifications` and `event_log`. Its libraries, compute, `channel`, `edition` and `configuration` are the deployed pipeline's.
- Only tables the pipeline's code names without a schema follow the copy's schema. A fully qualified name such as `prod.sales.orders` still writes where it says, so read such names from the configuration and set them with `configuration=`, which merges over the deployed configuration. The deployed pipeline's `parameters` are not copied, because `pipelines/get` does not return them.
- `run()` takes `full_refresh`, `full_refresh_selection`, `refresh_selection` and `timeout`. A failed update is returned, not raised: assert on its `state` and read `events(level="ERROR")`. Each change of state is logged, such as `pipeline numbers: RUNNING -> COMPLETED`. An update that has not ended at the timeout is stopped, and `LakeflowTimeout` is raised. `pipeline.updates` holds every update `run()` waited for.
- The copy is deleted with its tables when its block ends, with `cascade=True` and `force=True` (databricks-sdk 0.110.0 or later; older versions keep the tables until the schema is dropped). A kept copy is listed as `lakeflow: N pipelines kept` in the terminal summary, and `--lakeflow-sweep` deletes leftovers.
- `lakeflow.pipeline()` needs `Lakeflow(sync="deploy")`, which finds the pipeline through `bundle summary`; any other `sync` raises `LakeflowBundleError`. An unknown key raises `KeyError` listing the bundle's pipelines. A `schema` without a catalog raises `ValueError`, and a pipeline that publishes to the Hive metastore raises `LakeflowError`.

## Jobs that run other jobs

The copy keeps each Run Job task as deployed, so running the copy starts a real run of the deployed child job, with the child's own `run_as` and notifications, which the plugin neither cancels nor deletes. `lakeflow.job()` and `lakeflow.task()` emit a `UserWarning` for each such task that is still enabled. Pass `children="refuse"` to raise `LakeflowError` instead, before anything is created, or disable the task to leave the child alone:

```python
import pytest

from pytest_databricks_lakeflow import Lakeflow, LakeflowError


def test_parent_without_its_child(lakeflow: Lakeflow) -> None:
    with lakeflow.job("parent", disable=["trigger_child"]) as job:
        assert job.run().succeeded


def test_parent_is_refused(lakeflow: Lakeflow) -> None:
    with pytest.raises(LakeflowError, match="runs the deployed job"):
        lakeflow.job("parent", children="refuse")
```

When the child does run, `TaskRun.child_run` is its run as a `JobRun`, so you assert on the child's tasks with the same API as the parent's:

```python
def test_parent_triggers_child(lakeflow: Lakeflow) -> None:
    with lakeflow.job("parent") as job:
        run = job.run().raise_for_failure()
    child = run.task("trigger_child").child_run
    assert child is not None
    assert child.job_key == "child"
    assert child.task("load").succeeded
```

- `child_run_id` is the id of the child's run, read from `run_job_output.run_id`. `child_run.job_key` is the child job's bundle resource key when the bundle deployed it, otherwise its job id as a string, and a child with its own Run Job tasks has nested `child_run`s. `only` never applies to a child run.
- `Lakeflow(children="refuse")` makes refusing the default, such as in CI, and a test that means to run the child passes `children="warn"`.
- Copying or stubbing the child job is not supported.

## dbt tasks

A dbt task's `commands` are a list, so `task_params` replaces them in the copy, and `job.run(dbt_commands=[...])` replaces them for one run. With `Lakeflow(dbt_artifacts=True)` the plugin downloads the artefacts tarball as soon as the run has finished and parses `run_results.json` and `manifest.json` into `run.task(...).dbt`:

```python
@pytest.fixture(scope="session")
def lakeflow() -> Lakeflow:
    return Lakeflow(dbt_artifacts=True)


def test_ci_models(lakeflow: Lakeflow) -> None:
    commands = ["dbt deps", "dbt build --select tag:ci"]
    with lakeflow.task("analytics.dbt_build", task_params={"dbt_build": commands}) as task:
        run = task.run()
    dbt = run.task("dbt_build").dbt
    assert dbt is not None
    assert dbt.all_passed, dbt.failed_models
    assert dbt.results["model.analytics.orders"].status == "success"
```

- The list replaces every command, so keep `dbt deps` when the project has packages. The task's `warehouse_id`, `catalog`, `schema` and `project_directory` stay as deployed. An empty list raises `ValueError`.
- `dbt` is a `DbtArtifacts` with `results` (one `DbtResult` per node, by unique id, with `status`, `message`, `failures` and `execution_time`), `failed_models` (the nodes that ended `error` or `fail`, tests included), `all_passed`, `run_results`, `manifest` and `artifacts_dir`, where the tarball was extracted, with `target/` and `logs/dbt.log`. Each run gets a temporary directory, removed at the end of the session unless the job is kept.
- `run_results.json` describes the last command that wrote it, such as `dbt build` after `dbt deps`. `dbt` is `None` by default, for any other task, and when the artefacts cannot be read, such as a tarball over 50 MB or an expired link, each with a warning naming the task.
- `dbt_output.artifacts_link` is the platform's pre-signed URL to the tarball, valid for 30 minutes.

`job.run(dbt_commands=[...])` replaces the commands of every dbt task in one run, so one job can run different selections:

```python
def test_selections(lakeflow: Lakeflow) -> None:
    with lakeflow.job("analytics") as job:
        ci = job.run(dbt_commands=["dbt build --select tag:ci"])
        full = job.run()
    assert ci.task("dbt_build").parameters == ["dbt build --select tag:ci"]
    assert full.task("dbt_build").parameters == ["dbt deps", "dbt build"]
```

The Jobs API refuses `dbt_commands` on a job that has job parameters, so on such a job, or with `test_marker`, `trigger=` or `input=`, which declare some, the plugin raises `LakeflowError` before the run starts: use `task_params`.

## Other task outputs

- `run_job_output`, `dashboard_output`, `sql_output`, `alert_output`, `dbt_output` and `clean_rooms_notebook_output` are the Databricks SDK objects from `runs/get-output`, each `None` for any other task. A clean room notebook task's `output` falls back to its `clean_rooms_notebook_output.notebook_output`.
- Dashboard subscriptions and SQL alert subscriptions are paused in every copy, so test runs do not email snapshots. Pass `Lakeflow(side_effects="keep")` to leave them as deployed.
- A Power BI task updates the semantic model whether or not `refresh_after_update` is set, so disable it in a test with `disable=`.
