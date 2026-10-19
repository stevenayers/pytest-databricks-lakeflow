# Writing job tests

A job test makes a throwaway copy of a bundle job, runs it one or more times, and asserts on the outcome. This page covers `lakeflow.job()`, `run()`, and the `JobRun` and `TaskRun` results. The [API reference](api.md) has the full signatures.

## Make a job

```python
with lakeflow.job("my_example_job", params={"param1": "hello", "param2": "world"}) as job:
    run = job.run()
```

- `key` is the bundle resource key of the job.
- `params` are job parameters for every run of the copy. The job must declare them, because the Jobs API accepts undeclared parameters silently. Pass `allow_undeclared_params=True` to `Lakeflow` to allow them anyway. Names starting with `__pytest_` are reserved.

    With `test_marker=True` on `Lakeflow`, every copy declares a `__pytest_test_id` job parameter whose default is the test node id, so job code can tell it is under test and skip or redirect side effects. It carries the node id like the `pytest_nodeid` tag, but job code can read it, and tests cannot pass it themselves:

    ```python
    # in a notebook of the job under test
    try:
        test_id = dbutils.widgets.get("__pytest_test_id")
    except Exception:
        test_id = None  # not under test
    ```

    Other task types can use the dynamic value reference `{{job.parameters.__pytest_test_id}}`.
- `capture_task_values` maps task keys to the task value keys to capture from them, see [below](#capture-task-values).
- `name` replaces the test node id in the job name, which is otherwise `[pytest] <node id>`.
- `run_as` chooses who the copy runs as, see [below](#run-as-another-identity).
- `disable` lists task keys to disable in the copy, see [below](#disable-a-task).
- `force_conditions` maps if/else condition task keys to the outcome every run of the copy takes, see [below](#force-a-branch).
- `extra_dependencies` adds test-only packages to the copy, see [below](#test-only-dependencies).

Entering the `with` block creates the job. Leaving it deletes the job, unless the block raised and `keep_failed` is on, which is the default. `pytest.skip` and `pytest.xfail` do not count as failures.

`lakeflow.job()` also works without `with`. The job is then created by the first `run()` and deleted at the end of the test, or kept if the test failed:

```python
def test_without_with(lakeflow: Lakeflow) -> None:
    run = lakeflow.job("my_example_job").run()
    assert run.succeeded
```

### Run as another identity

By default the copy has no `run_as`, so it runs as you, or as whoever runs the tests. That avoids needing rights on another identity, but it also means the permissions of the deployed job's `run_as` principal are never exercised. `run_as` takes three forms:

```python
lakeflow.job("my_example_job", run_as="keep")  # the template's run_as, as deployed
lakeflow.job("my_example_job", run_as="0000-app-id")  # a service principal, by application id
lakeflow.job("my_example_job", run_as="someone@example.com")  # a user
```

A string that contains `@` names a user and any other string names a service principal by its application id, not its display name. For anything else, such as a group, pass a `jobs.JobRunAs`. The Jobs API decides whether you may run as that principal: you need the Service Principal User role on a service principal, and a `PermissionDenied` from `jobs.create` fails the test unchanged. The principal also needs read access to the deployed notebooks and files under the target's `root_path`, or the run fails when its tasks start.

### Disable a task

A task that notifies people or refreshes a downstream system can be switched off in the copy without changing the bundle. `disable` sets `disabled` on those tasks, so every run of the copy skips them:

```python
def test_without_load(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job", disable=["load"]) as job:
        run = job.run()
        assert run.succeeded
        assert "load" in run.disabled_tasks
```

A disabled task ends `DISABLED` and lands in `run.disabled_tasks`, and `run.task("load")` raises a `KeyError` that says `Disabled in the job: ['load']`. Tasks the bundle already disables land there too. An unknown key raises `LakeflowError` listing the available tasks, and no job is created. Dashboard and SQL alert subscriptions are already paused in every copy, see `side_effects` in [Configuration](configuration.md).

### Test-only dependencies

Job code under test sometimes needs packages that must not ship in the production wheel, such as a fixtures package or `chispa`. `extra_dependencies` adds them to the copy only:

```python
def test_with_test_helpers(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job", extra_dependencies=["chispa>=0.10"]) as job:
        assert job.run().succeeded
```

Each entry is a PyPI requirement, or the `/Workspace` or `/Volumes` path of a wheel. It is appended to the `dependencies` of every environment in the copy, and to the `libraries` of every notebook, Python script and Python wheel task that runs on a cluster (a job cluster, a `new_cluster` or an `existing_cluster_id`), as a `whl` library for a path ending in `.whl` and a `pypi` library for anything else. Tasks nested in a `for_each_task` get them too. `lakeflow.task()` takes the same argument. The helper environment `__pytest_serverless` and the stub and probe tasks do not get them.

Serverless notebook tasks without an `environment_key` cannot take dependencies from the job settings, so they are left unchanged; install the package with `%pip install` in the notebook, or give the task an environment in the bundle. Local paths such as `./dist/*.whl[test]` are not supported yet and raise `LakeflowError` before anything is created. Declare a local wheel in the bundle job instead, where `sync="deploy"` maps it to the uploaded file.

The copy's settings can be changed further with [`modify_settings`](#change-the-settings-before-the-job-is-created). Two recipes that come up with dependencies, pinning the branch of a job that runs from Git and replacing the libraries of one task:

```python
import pytest
from databricks.sdk.service import compute, jobs

from pytest_databricks_lakeflow import Lakeflow


def pin_branch_and_libraries(settings: jobs.JobSettings, item: pytest.Item) -> None:
    if settings.git_source is not None:
        settings.git_source.git_branch = "feature/new-loader"
        settings.git_source.git_tag = None
        settings.git_source.git_commit = None
    for task in settings.tasks or []:
        if task.task_key == "load":
            task.libraries = [compute.Library(pypi=compute.PythonPyPiLibrary(package="requests==2.32.3"))]


@pytest.fixture(scope="session")
def lakeflow() -> Lakeflow:
    return Lakeflow(modify_settings=pin_branch_and_libraries)
```

## Run it

`job.run(params=None, only=None, timeout=None, *, performance_target=None, queue=None, full_refresh=None, full_refresh_selection=None, refresh_selection=None)` starts a run, waits for it to finish, and returns a `JobRun`.

- `params` are merged over the job's parameters for this run only.
- `only` lists the tasks to run. `+key` adds the tasks upstream of `key`, `key+` the tasks downstream, and `+key+` both, as `run_now` does.
- `timeout` is a `timedelta`. It defaults to `Lakeflow`'s `timeout`, which is 30 minutes. When it expires the run is cancelled and `LakeflowTimeout` is raised. The job itself is created with `timeout_seconds` set to `Lakeflow`'s `timeout` plus 60 seconds, so the platform stops the run if the test process dies. A `timeout` longer than that raises `LakeflowError` before the run starts; pass a longer `Lakeflow(timeout=...)` instead.
- `performance_target`, keyword only, sets the performance target of this run only, as a `jobs.PerformanceTarget` or its name, such as `"PERFORMANCE_OPTIMIZED"`. The job's settings are not changed. It applies to serverless tasks; tasks on classic clusters ignore it. An unknown name raises `ValueError` before the run starts.
- `queue`, keyword only, turns queueing on or off for this run only. `job.run(queue=False)` sends `QueueSettings(enabled=False)`.
- `full_refresh`, `full_refresh_selection` and `refresh_selection`, keyword only, set how the job's pipeline tasks refresh in this run. See [pipeline tasks](#pipeline-tasks).

Each call to `run()` sends a fresh idempotency token with the run request, so a request that the SDK retries cannot start a second run.

Running is explicit so that a test can run a job more than once, for example to compare a full load with an incremental one:

```python
def test_run_twice_in_one_job(lakeflow: Lakeflow) -> None:
    with lakeflow.schema() as schema, lakeflow.job("my_example_job", params={"schema": schema}) as job:
        first = job.run()
        second = job.run(params={"mode": "incremental"})
        assert first.succeeded and second.succeeded
        assert second.duration < first.duration
```

The copy allows one run at a time. `job.runs` holds every finished `JobRun`, and `job.job_id` and `job.url` identify the job in the workspace.

## Assert on the result

`run.succeeded` is true only if every one of your tasks that was part of the run ended `SUCCESS`. Tasks the plugin injected do not count, nor do tasks that were `EXCLUDED` because their `run_if` or if/else condition was not met. `run.failed` is `not run.succeeded`.

Do not use `run.result_state`, the parent run's state, as the verdict. A job with a probe that runs after a failed task ends `SUCCESS_WITH_FAILURES`.

`run.tasks` maps task keys to `TaskRun`s. `run.task("transform")` does the same and raises a `KeyError` that lists the available keys when the task was not part of the run. Tasks left out by `only` are in `run.skipped_tasks`, and tasks disabled in the job, in the bundle or with `disable`, are in `run.disabled_tasks`. Tasks in both groups end `DISABLED`; the run itself does not say why, so the plugin reads the copy's settings to tell them apart. The plugin's stubs and probes are in `run.injected_tasks`. `run.task(key)` says `Skipped by only: [...]` or `Disabled in the job: [...]` for such a task. `run.executed_tasks` is the sorted list of keys in `run.tasks` that ran.

`run.parameters` is a dict of the run's job parameters: the value passed for the run, or the default when none was. Parameters the plugin injects are left out.

Each `TaskRun` has:

- `result_state`, such as `SUCCESS`, `FAILED` or `UPSTREAM_FAILED`, and `succeeded` and `ran`. `timed_out` is true when the task ended `TIMEDOUT`, stopped by the platform at its `timeout_seconds`.
- `excluded`, true when the task ended `EXCLUDED` because its `run_if` or if/else condition was not met. Excluded tasks stay in `run.tasks` and do not fail the run.
- `upstream_failed`, `disabled` and `cancelled`, true when the task ended exactly `UPSTREAM_FAILED`, `DISABLED` or `CANCELED`. `UPSTREAM_CANCELED` sets none of them; `ran` is false for all of these states.
- `condition_outcome`, for an if/else condition task: `True` when the condition held, `False` when it did not, and `None` for any other task. See [Branches](#branches).
- `termination_code` and `termination_message`, from the task's termination details.
- `output`, the value a notebook passed to `dbutils.notebook.exit()`, and `output_truncated`. `output_json` parses `output` as JSON. For a clean room notebook task whose top-level notebook output is missing or has no result, `output` comes from `clean_rooms_notebook_output.notebook_output`.
- The outputs of other task types, as the Databricks SDK objects from `runs/get-output`, each `None` for any other task or when the platform reported none: `sql_output` (a `jobs.SqlOutput` whose `query_output`, `alert_output` or `dashboard_output` holds the `warehouse_id`, `output_link` and the SQL that ran, but no rows), `alert_output` (a `jobs.AlertTaskOutput`), `dbt_output` (`artifacts_link` and `artifacts_headers`), `run_job_output` (the triggered run's `run_id`), `dashboard_output` and `clean_rooms_notebook_output`.
- `child_run_id`, the id of the run a run-job task triggered, read from `run_job_output.run_id`, or `None` for any other task, and `child_run`, that run as a `JobRun`. See [Jobs that run other jobs](#jobs-that-run-other-jobs).
- `alert_state`, the state an alert evaluated to, such as `TRIGGERED`, `OK` or `UNKNOWN`, read from `alert_output` or, for a legacy SQL alert task, from `sql_output.alert_output`; `None` for any other task. An alert task succeeds whether or not the alert triggered, so assert on `alert_state`.
- `logs`, the stdout and stderr of a Python script, wheel or JAR task, plus `log_lines` and `logs_truncated`. Notebook tasks have no logs; what a notebook printed is in `notebook`, see [Read what a notebook printed](#read-what-a-notebook-printed).
- `notebook`, a `NotebookView` of a notebook task that ran and did not succeed, read from the rendered notebook, or `None`. See [Read what a notebook printed](#read-what-a-notebook-printed).
- `error`, a `TaskError` with `type`, `message`, `trace`, `root_cause` and `code`, or `None`.
- `task_values` and `missing_task_values`, task values captured by a probe.
- `parameters`, what the task received after job parameters and `{{...}}` references were resolved. It is a dict for notebook (`base_parameters`), SQL, run-job (`job_parameters`) and Python wheel tasks with named parameters, a list for Python wheel tasks with positional parameters, Python script, JAR and spark-submit tasks and dbt tasks (`commands`), and `None` for other task types or when the platform reported none. The platform gives job parameters precedence over task parameters with the same key, and the value here shows the job parameter's.
- `duration`, `attempt` and `run_url`.
- `timing`, a `Timing` with the `queue`, `setup`, `execution` and `cleanup` durations the platform reported and their `total`, and `effective_performance_target`, `STANDARD` or `PERFORMANCE_OPTIMIZED`. `JobRun` has both as well; a run with several tasks reports zero durations on the parent run, so read them on the tasks. A duration the platform did not report is zero, and a performance target it did not report is `None`.
- `attempts`, one `TaskRun` per attempt in attempt order, when the platform retried the task. The task itself describes the last attempt, and `attempts[-1]` equals it apart from `attempts`. Each element has its own `run_id`, `error` and `output`, and an empty `attempts`.

A test of a job with a SQL query, an alert and a run-job task reads like this:

```python
def test_alert_triggers_and_child_job_runs(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_reporting_job") as job:
        run = job.run().raise_for_failure()
        query = run.task("daily_query").sql_output
        assert query is not None and query.query_output is not None
        assert query.query_output.warehouse_id == "abc123"
        assert run.task("check").alert_state == "TRIGGERED"
        assert run.task("trigger").child_run_id is not None
```

`run.timed_out` is true when the parent run ended `TIMEDOUT` or any task in `run.tasks` timed out. Such a run has failed, and `repr(run)` lists a timed-out task without an error as `load: TIMEDOUT`.

A failure test reads like this:

```python
def test_job_surfaces_permission_error(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job", params={"param1": "forbidden", "param2": "world"}) as job:
        run = job.run()
        assert run.failed
        assert run.termination_code == "RUN_EXECUTION_ERROR"
        assert run.task("transform").raised(PermissionError)
        assert run.task("load").result_state == "UPSTREAM_FAILED"
```

`raised()` compares class names, because the exception happened in another process. `error.type` keeps the name as Python printed it, so `json.decoder.JSONDecodeError` matches `raised(json.JSONDecodeError)`.

`repr(run)` names the failing tasks with the first line of their errors and links to the run, so `assert run.succeeded, run` gives a useful failure message. When the run failed, it also prints `params={...}`, the run's job parameters, and `repr()` of a task with an error prints the task's `parameters`. Reprs of successful runs and tasks leave them out.

`run.raise_for_failure()` gives the full detail in one call. It returns the run when `run.succeeded` is true, so `run = job.run().raise_for_failure()` works, and otherwise raises `LakeflowRunFailed`. The message is `repr(run)` followed by one line per failing task with its whole error, its root cause when there is one, and its run URL. It also prints the resolved parameters: the run's job parameters on a `params:` line after the repr (in place of the repr's `params={...}`), and each failing task's `parameters` on an indented `params:` line after the task's own, when the platform reported them. The exception's `run` attribute holds the `JobRun`.

```text
LakeflowRunFailed: JobRun('etl', FAILED, https://.../runs/77, failing=['extract: ValueError: bad limit'])
params: {"date": "2026-10-01"}
extract: ValueError: bad limit https://.../runs/7701
  params: {"date": "2026-10-01", "limit": "10"}
```

```python
import pytest

from pytest_databricks_lakeflow import Lakeflow, LakeflowRunFailed


def test_failure_detail(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job", params={"param1": "forbidden", "param2": "world"}) as job:
        with pytest.raises(LakeflowRunFailed, match="PermissionError") as excinfo:
            job.run().raise_for_failure()
        assert excinfo.value.run.task("transform").raised(PermissionError)
```

### Branches

An if/else condition task records its outcome, and the tasks on the branch that was not taken end `EXCLUDED`. `run.branch_taken(key)` returns the condition task's `condition_outcome` and raises `LakeflowError` when the task is not a condition task or has no outcome, so check its `result_state` then. Excluded tasks do not fail the run. In the `branches` integration bundle, `check_input` sets the task value `rows` from the `rows` job parameter, `check` compares it with `> 0`, and `on_true` and `on_false` depend on the two outcomes. `tests/integration/test_branches.py` parametrises both outcomes; the false branch on its own looks like this:

```python
def test_no_rows_takes_the_false_branch(lakeflow: Lakeflow) -> None:
    with lakeflow.job("branching_job", params={"rows": "0"}) as job:
        run = job.run()
        assert run.succeeded, run
        assert run.branch_taken("check") is False
        assert run.task("on_true").excluded
        assert run.executed_tasks == ["check", "check_input", "on_false"]
```

The `==` and `!=` operators of a condition task compare their operands as strings, so a task value of `0` and a right operand of `0.0` are not equal. `<`, `>`, `<=` and `>=` compare them as numbers. To test the condition on its own, drive it with stub values in a [task test](task-tests.md#test-an-ifelse-condition).

#### Force a branch

To test a branch's downstream tasks without arranging upstream data that sends the run down it, `force_conditions` maps condition task keys to `True` or `False`. The copy's condition task then compares constants, `true == true` or `true == false`, so every run of the copy takes that branch whatever its operands would have said. The original operands are logged at INFO. The condition task keeps its `depends_on`, so the tasks upstream of it still run; to run one downstream task without them, use [`lakeflow.task()`](task-tests.md). Here the data says the false branch and the test takes the true one:

```python
def test_forced_true_branch(lakeflow: Lakeflow) -> None:
    with lakeflow.job("branching_job", params={"rows": "0"}, force_conditions={"check": True}) as job:
        run = job.run()
        assert run.succeeded, run
        assert run.branch_taken("check") is True
        assert run.task("check_input").ran
        assert run.task("on_false").excluded
        assert run.executed_tasks == ["check", "check_input", "on_true"]
```

A key that is not a task, a task that is not a condition task, a disabled condition task and an outcome that is not `True` or `False` raise `LakeflowError` before the job is created. The error for a task that is not a condition task lists the job's condition tasks.

### Errors from Spark

A Py4J or Spark failure often wraps the real problem. `error.root_cause` is the text of the last `Caused by:` line of the first exception in `error.trace`. It also follows Python chains, from `raise ... from e` or an exception raised while handling another, to the first exception, such as `ZeroDivisionError: division by zero`, while `error.type` and `error.message` still describe the outer one. It is `None` when the trace has neither. `error.code` is the Spark error class, such as `TABLE_OR_VIEW_NOT_FOUND` or `DELTA_INVALID_FORMAT.X`, when `error.message` or `error.root_cause` starts with one in square brackets, and `None` otherwise:

```python
error = run.task("transform").error
assert error is not None
assert error.code == "TABLE_OR_VIEW_NOT_FOUND"
```

### JSON output

A notebook can return structured data with `dbutils.notebook.exit(json.dumps({...}))`. `task.output_json` parses it:

```python
assert run.task("transform").output_json == {"rows_written": 7}
```

`output_json` raises a `ValueError` that names the task when the notebook never called `dbutils.notebook.exit()`, when the output was cut at 5 MB, or when the output is not JSON.

### Read what a notebook printed

The Jobs API returns no stdout for a notebook task, but it renders the notebook of a task run with every cell's output. After a run, the plugin exports the notebook of each notebook task that ran and did not succeed with `jobs.export_run`, one call per task, and parses it into `task.notebook`:

```python
def test_transform_prints_its_input(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job", params={"param1": "forbidden", "param2": "world"}) as job:
        run = job.run()
        notebook = run.task("transform").notebook
        assert notebook is not None
        assert "transform: checking forbidden" in notebook.stdout
        assert notebook.failed_cell is not None
        assert notebook.failed_cell.error == "PermissionError: forbidden may not read /Volumes/example/raw/landing"
        notebook.save("build/lakeflow/transform.html")
```

A `NotebookView` has:

- `cells`, the notebook's cells in order, each a `NotebookCell` with `index` (its position in `cells`), `code`, `output` (the text it printed or displayed, with HTML tags and ANSI colour codes removed), `output_html` (its results as exported) and `error` (the exception it raised as `Name: message`, or `None`).
- `stdout`, the text output of every cell, in order.
- `failed_cell`, the first cell that raised, or `None`. Cells skipped after it have no error.
- `html`, the exported HTML, unchanged, and `save(path)`, which writes it to a file that opens in a browser like the run page.

The output comes from the rendered notebook, so it is complete only once the task has terminated, which it has by the time `run()` returns. The export format is not documented, so the plugin parses it best effort: when the notebook model cannot be found in the HTML, `cells` is empty, `html` keeps the export and a `UserWarning` names the missing `__DATABRICKS_NOTEBOOK_MODEL` marker. When `export_run` fails, `notebook` is `None` and a warning with the task key and run URL is logged; the run's outcome is unchanged.

`Lakeflow(notebook_export=...)` chooses the tasks: `"failed"` (the default) exports the notebook tasks that ran and did not succeed, `"always"` every notebook task that ran, so that tests can assert on what a passing notebook printed, and `"never"` none. Only notebook tasks are exported, never Python script, wheel, JAR or SQL tasks or the plugin's stubs and probes, and only the last attempt of a retried task. When a test fails, its `lakeflow` report section shows, for each failing task with a notebook, the first code line of the cell that failed and the last 50 lines of `stdout`:

```text
transform: notebook.cells[2] failed: dbutils.jobs.taskValues.set(key="stage", value="validation")
transform: notebook stdout:
  transform: checking forbidden against /Volumes/example/raw/landing
  transform: 7 rows expected
```

### Truncated output and logs

The Jobs API returns at most 5 MB of notebook output and of logs. When a task's output or logs were cut, building the run emits a `UserWarning` that names the task, so it shows up in the pytest warnings summary. `output_truncated` and `logs_truncated` say which one was cut.

## Run part of a job

```python
def test_subset_of_job(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job") as job:
        run = job.run(only=["+transform"])
        assert run.succeeded
        assert "load" not in run.tasks
        assert sorted(run.skipped_tasks) == ["load", "my_wheel_task"]
```

Asking for a skipped task raises a `KeyError` that says it was skipped by `only`. When the job has probes or stubs, `only` is extended with the ones the selected tasks need.

## Pipeline tasks

A pipeline task runs the pipeline that the bundle deployed, with the refresh mode its `pipeline_task` sets. To choose the refresh for one run, pass `full_refresh`, `full_refresh_selection` or `refresh_selection`:

```python
def test_full_refresh(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_pipeline_job") as job:
        assert job.run(full_refresh=True).succeeded
        assert job.run(refresh_selection=["bronze_orders"]).succeeded
```

They are sent as the run's `pipeline_params` and leave the job's settings unchanged. `full_refresh_selection` and `refresh_selection` list tables, and need databricks-sdk 0.110.0 or later. A bare string in place of the list raises `LakeflowError`. Passing any of them to a job without a pipeline task, nested `for_each_task` tasks included, raises `LakeflowError` naming the job's tasks before the run starts.

The pipeline is the deployed one, not a copy: every test that runs it updates the same pipeline and its tables, and the options apply to every pipeline task in the run.

## Capture task values

Task values are not exposed by the Jobs API. `capture_task_values` adds a probe task after each task you name. The probe depends on the task, runs even when the task failed, and reports the values it finds:

```python
def test_values_captured_even_on_failure(lakeflow: Lakeflow) -> None:
    with lakeflow.job(
        "my_example_job", params={"param1": "explode"}, capture_task_values={"transform": ["stage"]}
    ) as job:
        run = job.run()
        assert run.failed
        assert run.task("transform").task_values["stage"] == "validation"
```

A missing value raises a `KeyError` whose message says why: the task never set it, the task did not run, or the probe failed:

```python
def test_missing_task_values_explain_themselves(lakeflow: Lakeflow) -> None:
    job = lakeflow.job("my_example_job", capture_task_values={"transform": ["never_set"], "load": ["anything"]})
    run = job.run(params={"param1": "explode"})
    assert run.task("transform").missing_task_values == ["never_set"]
    with pytest.raises(KeyError, match="Task 'transform' never set value 'never_set'"):
        run.task("transform").task_values["never_set"]
    with pytest.raises(KeyError, match=r"Task 'load' did not run \(UPSTREAM_FAILED\)"):
        run.task("load").task_values["anything"]
```

Each probe is a serverless notebook task and costs 7 to 30 seconds, even when its task did not run. [Writing task tests](task-tests.md) explains the probe and stub mechanics.

## Temporary schemas

`lakeflow.schema()` creates a schema named `<catalog>.pytest_<run id>_<random>` and drops it on leaving the block, even when the block failed, unless `keep` or `--lakeflow-keep` is set. It needs a catalog, from `Lakeflow(catalog=...)` or `schema(catalog=...)`. Pass the schema to the job as a parameter:

```python
def test_writes_table(lakeflow: Lakeflow) -> None:
    with lakeflow.schema() as schema, lakeflow.job("my_example_job", params={"schema": schema}) as job:
        run = job.run()
        assert run.succeeded, run
        assert lakeflow.ws.tables.exists(f"{schema}.example_rows").table_exists
```

`lakeflow.ws` is the `WorkspaceClient`, for whatever else the test needs to check.

## Set up and tear down data

`setup=` and `teardown=` take plain functions that run on the job's own compute, as tasks of the copy, so they get the real `spark` and `dbutils` without a seed task in the bundle or Databricks Connect in the test:

```python
def seed_orders(spark, dbutils, params):
    schema = params["schema"]
    spark.sql(f"CREATE OR REPLACE TABLE {schema}.seed_orders AS SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, sku)")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {schema}.landing")
    dbutils.fs.put(f"/Volumes/{schema.replace('.', '/')}/landing/orders.csv", "id,sku\n1,a\n2,b\n", True)
    return {"schema": schema, "param1": params["param1"]}


def remove_landing_file(dbutils, params):
    return dbutils.fs.rm(f"/Volumes/{params['schema'].replace('.', '/')}/landing/orders.csv")


def test_setup_seeds_a_table_and_teardown_removes_a_file(lakeflow: Lakeflow) -> None:
    with lakeflow.schema() as schema:
        with lakeflow.job(
            "my_example_job",
            params={"schema": schema, "mode": "incremental"},
            setup=seed_orders,
            teardown=remove_landing_file,
        ) as job:
            run = job.run(params={"param1": "seeded"})
            assert run.succeeded, run
            assert run.setup.output_json == {"schema": schema, "param1": "seeded"}
            assert run.task("load").output == "loaded 2 seeded orders"
            assert run.teardown.succeeded
```

- A function takes any of `spark`, `dbutils` and `params`, and the plugin passes only the ones it declares. `params` is a dict of the job parameters the job declares, with their values for the run (the job's `params` and then the run's `params` merged over the defaults), as strings, the way the job's own notebooks read them from their widgets. Only declared parameters reach it: the names are fixed when `job()` is called, so a parameter passed under `allow_undeclared_params=True`, or declared by `modify_settings`, is not in `params`.
- The setup task, `__pytest_setup`, runs first: every task that has no `depends_on`, stubs included, comes to depend on it. The teardown task, `__pytest_teardown`, runs last, after every task that no other task depends on, probes included, with `run_if` set to `ALL_DONE`, so it runs whether the job passed or failed. Both run on every `run()`, and `run(only=...)` always includes them. A selected task still waits for the setup when the tasks between them are left out, because the platform marks a left-out task `DISABLED` only once the tasks before it have finished.
- A value the function returns becomes the task's `output` as JSON, through `json.dumps(value, default=str)`, so `run.setup.output_json` gives it back.
- `run.setup` and `run.teardown` are the two tasks' `TaskRun`s, or `None` when the job has none. They are in `run.injected_tasks` and do not count towards `run.succeeded`.
- A failed setup leaves the job's tasks `UPSTREAM_FAILED`, so the run fails, and `repr(run)` names it first, as `setup: <error>`, so you do not chase a task that never ran.
- A failed teardown does not fail the run. The plugin logs a warning with the task's error and run page. Assert `run.teardown.succeeded` to fail the test on it.

The function runs on the cluster, not in the test process, so it must be self-contained. The plugin ships its source as a notebook, uploaded once per session to `helpers_path` as `setup_<hash>` or `teardown_<hash>`, and runs it on the [helper compute](task-tests.md#helper-compute). Put imports inside the function, and define any helper it calls inside it too, or install it on the compute: a `NameError` there comes back as `run.setup.error`. `lakeflow.job()` raises `LakeflowError` before creating anything when the function is a lambda or not a plain function defined with `def`, when its source cannot be read because it was defined in a REPL, when it uses variables of an enclosing function, which would not exist on the cluster, when it takes a parameter other than `spark`, `dbutils` and `params`, or when it has a decorator, whose line would be shipped with the source and fail on the cluster. Pass values as job parameters instead and read them from `params`. Type annotations are fine: the notebook does not evaluate them, so `spark: SparkSession` works without importing `SparkSession` inside the function.

The teardown runs inside the same job run, after the job's tasks and before the test's assertions. Use it for side effects the test does not assert on: files under a volume, checkpoints, tables outside the temporary schema, rows written to a source table. Tables the test asserts on belong in [`lakeflow.schema()`](#temporary-schemas), which drops them after the test, so most tests need `setup=` only. Make the functions idempotent: the setup runs on every run, including every run of a job shared by several tests, hence `CREATE OR REPLACE TABLE`.

## Share one run between tests

Run the job in a fixture with a broader scope, inside a `with` block so that the job is deleted when the fixture is torn down:

```python
from collections.abc import Iterator

import pytest

from pytest_databricks_lakeflow import JobRun, Lakeflow


@pytest.fixture(scope="module")
def nightly_run(lakeflow: Lakeflow) -> Iterator[JobRun]:
    with lakeflow.job("my_example_job") as job:
        yield job.run()


def test_extract(nightly_run: JobRun) -> None:
    assert nightly_run.task("extract").output == "extracted 7 rows"


def test_load(nightly_run: JobRun) -> None:
    assert nightly_run.task("load").succeeded
```

The job is named after the first test that used the fixture. Pass `name=` to `job()` to name it after the module instead.

## Parametrise and parallelise

`@pytest.mark.parametrize` works as usual. Each parameter set gets its own job, so the runs are independent and can run at the same time under pytest-xdist:

```bash
pytest -n 4
```

One worker deploys the bundle and loads the templates while the others wait, then all of them reuse the result. [How it works](how-it-works.md#parallel-sessions) has the details.

## Keep a job for debugging

- The default, `keep_failed=True`, keeps the job when the test fails. The job and run URLs are logged at WARNING and added to the test report.
- `--lakeflow-keep` keeps every job and schema of the run.
- `--no-lakeflow-keep-failed` deletes jobs even when their test fails, for CI.
- `--lakeflow-sweep` deletes the jobs you kept earlier, once they are older than `sweep_hours`.

A kept job carries the CI run URL and the commit in its `pytest_ci_url` and `pytest_git_sha` tags, and the test node id and CI run URL in its description.

Every kept job and schema is listed in a `lakeflow` block at the end of the run, with a hint to run `pytest --lakeflow-sweep` when jobs were kept, so you do not need `--log-cli-level` to find them.

## Jobs that run other jobs

The copy keeps each Run Job task as deployed, so its `run_job_task.job_id` still names the deployed child job. Running the copy starts a real run of that child, with the child's own `run_as` and notifications, and the plugin neither cancels nor deletes it. So that this does not happen unnoticed, `lakeflow.job()` and `lakeflow.task()` emit a `UserWarning` for each Run Job task in the copy that is not disabled and not inside a disabled `for_each_task` loop, naming the task, the job and the child's job id. The job is still created.

Pass `children="refuse"` to raise `LakeflowError` instead, before anything is created, or disable the task (or the loop that holds it) to leave the child alone:

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

When the child does run, `TaskRun.child_run_id` gives the id of its run and `TaskRun.child_run` gives the run itself as a `JobRun`, so you assert on the child's tasks with the same API as the parent's:

```python
def test_parent_triggers_child(lakeflow: Lakeflow) -> None:
    with lakeflow.job("parent") as job:
        run = job.run().raise_for_failure()
    child = run.task("trigger_child").child_run
    assert child is not None
    assert child.job_key == "child"
    assert child.task("load").succeeded
    assert run.task("prepare").child_run is None
```

The plugin reads the child run with `get_run` once the parent run has finished. `child_run.job_id` is the deployed child job's id, and `child_run.job_key` is that job's bundle resource key when the bundle deployed it, otherwise the job id as a string; with `sync="sync"` or `sync="none"` the plugin knows no job ids, so the key is always the id. `child_run.succeeded` follows the same rules as the parent's, and a child with its own Run Job tasks has nested `child_run`s. `only` never applies to a child run, so a task disabled in the child job is in `child_run.disabled_tasks`, not `skipped_tasks`. When a Run Job task was retried, each of its `attempts` carries the child run that attempt triggered, and `child_run` is the last one's. `child_run` is `None` for any other task, for a Run Job task that did not run, and when the child run cannot be read, which logs a warning.

## Change the settings before the job is created

`modify_settings` is called with each ephemeral job's `JobSettings` and the test's `pytest.Item`, just before the job is created. Change the settings in place:

```python
import pytest
from databricks.sdk.service import jobs

from pytest_databricks_lakeflow import Lakeflow


def tag_with_module(settings: jobs.JobSettings, item: pytest.Item) -> None:
    settings.tags = {**(settings.tags or {}), "module": item.path.stem}


@pytest.fixture(scope="session")
def lakeflow() -> Lakeflow:
    return Lakeflow(modify_settings=tag_with_module)
```

Parameters that `modify_settings` declares are accepted by `params`. `run_finished` is the matching callback for outcomes: it receives each `JobRun` and the test, for example to record durations.
