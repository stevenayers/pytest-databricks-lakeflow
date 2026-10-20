# Job tests

A job test makes a throwaway copy of a bundle job, runs it one or more times, and asserts on the outcome. This page covers the core flow: `lakeflow.job()`, `run()`, and the `JobRun` and `TaskRun` results. The [recipes](recipes/index.md) cover everything you can change about the copy and the run.

## Make a copy of a job

```python
from pytest_databricks_lakeflow import Lakeflow


def test_job_succeeds(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job", params={"param1": "hello", "param2": "world"}) as job:
        run = job.run()
        assert run.succeeded, run
        assert run.task("transform").output == "hello world"
```

- `key` is the bundle resource key of the job, the name that `databricks bundle run` takes.
- `params` are job parameters for every run of the copy. The job must declare them: the Jobs API accepts an undeclared parameter silently, so the plugin raises `LakeflowError` instead. `Lakeflow(allow_undeclared_params=True)` lets them through. Names starting with `__pytest_` are reserved and raise `ValueError`.
- Entering the `with` block creates the job. Leaving it deletes the job, unless the test failed and `keep_failed` is on, which is the default. `pytest.skip` and `pytest.xfail` do not count as failures.
- Without `with`, the first `run()` creates the job and the plugin deletes it at the end of the test:

```python
def test_without_with(lakeflow: Lakeflow) -> None:
    assert lakeflow.job("my_example_job").run().succeeded
```

Everything after `params` is keyword-only. Each option has a recipe:

| Option | Changes | Recipe |
| --- | --- | --- |
| `name` | The job name, `[pytest] <node id>` by default | [Share one run between tests](recipes/runs.md#share-one-run-between-tests) |
| `disable` | Switches tasks off in the copy | [Disable a task](recipes/job-settings.md#disable-a-task) |
| `task_params` | The parameters of single tasks | [Override one task's parameters](recipes/job-settings.md#override-one-tasks-parameters) |
| `task_overrides` | The timeout and retries of single tasks | [Test retries and timeouts](recipes/job-settings.md#test-retries-and-timeouts) |
| `force_conditions` | The outcome of if/else condition tasks | [Force a branch](recipes/task-types.md#force-a-branch) |
| `run_as` | Who the copy runs as | [Run as another identity](recipes/job-settings.md#run-as-another-identity) |
| `children` | Warn or refuse when the copy would run a deployed child job | [Jobs that run other jobs](recipes/task-types.md#jobs-that-run-other-jobs) |
| `capture_task_values` | Adds probe tasks that read task values | [Read task values](#read-task-values) |
| `upstream_task_values` | Replaces tasks with stubs that set task values | [Run the downstream half of a job](recipes/runs.md#run-the-downstream-half-of-a-job) |
| `for_each_inputs` | The list a for-each task iterates over | [For-each tasks](recipes/task-types.md#for-each-tasks) |
| `trigger` | The values of `{{job.trigger.*}}` references | [Simulate a trigger](recipes/task-types.md#simulate-a-trigger) |
| `setup`, `teardown`, `teardown_at` | Tasks that seed and clean up data | [Seed data with a setup task](recipes/data.md#seed-data-with-a-setup-task) |
| `extra_dependencies` | Test-only packages | [Add test-only dependencies](recipes/job-settings.md#add-test-only-dependencies) |
| `environments` | The copy's serverless environments | [Change a serverless environment](recipes/job-settings.md#change-a-serverless-environment) |
| `pipelines`, `schema` | Copies the pipelines that its pipeline tasks update | [Copy the pipelines of a job](recipes/task-types.md#copy-the-pipelines-of-a-job) |
| `saved_queries` | Runs saved query tasks as SQL files, so their rows come back | [Read the rows of a saved query](recipes/task-types.md#read-the-rows-of-a-saved-query) |

`run_as`, `disable`, `extra_dependencies`, `environments`, `children`, `setup`, `teardown` and `saved_queries` take their default from the `Lakeflow` argument of the same name, see [defaults for per-call arguments](reference/lakeflow.md#defaults-for-per-call-arguments).

## Run it

`job.run()` starts a run, waits for it to finish and returns a `JobRun`. Running is explicit so that a test can run one copy more than once:

```python
def test_run_twice_in_one_job(lakeflow: Lakeflow) -> None:
    with lakeflow.schema() as schema, lakeflow.job("my_example_job", params={"schema": schema}) as job:
        first = job.run()
        second = job.run(params={"mode": "incremental"})
        assert first.succeeded and second.succeeded
        assert second.duration < first.duration
```

- `params` are merged over the job's parameters for this run only.
- `only` lists the tasks to run, see [Run only some tasks](#run-only-some-tasks).
- `timeout` is a `timedelta`, by default `Lakeflow`'s `timeout` of 30 minutes. When it expires the run is cancelled and `LakeflowTimeout` is raised, naming the run's page and the state of each task, such as `Tasks when cancelled: extract SUCCESS, transform RUNNING, load PENDING`.
- A copy allows one run at a time. `job.runs` holds every finished `JobRun`, and `job.job_id` and `job.url` identify the job in the workspace.
- Every call sends a fresh idempotency token, so a request that the SDK retries cannot start a second run.

The keyword-only options each have a recipe:

| Option | Changes | Recipe |
| --- | --- | --- |
| `stop_after` | Cancels the run after a fixed time and returns it | [Stop a run that does not end](recipes/runs.md#stop-a-run-that-does-not-end) |
| `performance_target`, `queue` | The serverless performance mode and queueing of this run | [Speed up a run](recipes/compute.md#speed-up-a-run) |
| `full_refresh` and the refresh selections | How the run's pipeline tasks refresh | [Choose how a pipeline task refreshes](recipes/task-types.md#choose-how-a-pipeline-task-refreshes) |
| `replay` | Stubs the tasks that `only` leaves out with a previous run's task values | [Replay a run's task values](recipes/runs.md#replay-a-runs-task-values) |
| `dbt_commands` | The commands of every dbt task in this run | [dbt tasks](recipes/task-types.md#dbt-tasks) |
| `stream` | Prints the run's progress to the terminal | [Watch a run live](recipes/debugging.md#watch-a-run-live) |

`job.start()` starts a run without waiting and returns a `RunHandle`, see [Run jobs side by side](recipes/runs.md#run-jobs-side-by-side).

## Check the outcome

`run.succeeded` is true only when every one of your tasks that was part of the run ended `SUCCESS`. Tasks the plugin injected do not count, nor do tasks that ended `EXCLUDED` because their `run_if` or if/else condition was not met. `run.failed` is `not run.succeeded`.

Do not use `run.result_state`, the parent run's state, as the verdict: a job whose probe runs after a failed task ends `SUCCESS_WITH_FAILURES`.

```python
def test_job_surfaces_permission_error(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job", params={"param1": "forbidden", "param2": "world"}) as job:
        run = job.run()
        assert run.failed
        assert run.termination_code == "RUN_EXECUTION_ERROR"
        assert run.task("transform").raised(PermissionError)
        assert run.task("load").result_state == "UPSTREAM_FAILED"
```

- `run.task(key)` returns the task's `TaskRun`. For a task that was not part of the run it raises a `KeyError` that lists the available keys, or says `Skipped by only: [...]` or `Disabled in the job: [...]`.
- `run.tasks` holds your tasks that were part of the run, `run.skipped_tasks` those left out by `only`, `run.disabled_tasks` those disabled in the job and the tasks downstream of them, and `run.injected_tasks` the plugin's stubs, probes, setup and teardown tasks. `run.executed_tasks` is the sorted list of keys in `run.tasks` that ran.
- `run.parameters` is the run's job parameters by name: the value passed for the run, or the default. The plugin's own parameters are left out.
- `run.timed_out` is true when the run or any of its tasks ended `TIMEDOUT`, and `run.cancelled` when the run ended `CANCELED`. Both count as failed.

### Get the whole failure in one call

`run.raise_for_failure()` returns the run when it succeeded, so `run = job.run().raise_for_failure()` works, and otherwise raises `LakeflowRunFailed`. Its message is the run's repr, then one line per failing task with its whole error, its root cause and its run URL, then the resolved parameters:

```text
LakeflowRunFailed: JobRun('etl', FAILED, https://.../runs/77, failing=['extract: ValueError: bad limit'])
params: {"date": "2026-10-01"}
extract: ValueError: bad limit https://.../runs/7701
  params: {"date": "2026-10-01", "limit": "10"}
```

The exception's `run` attribute holds the `JobRun`:

```python
import pytest

from pytest_databricks_lakeflow import Lakeflow, LakeflowRunFailed


def test_failure_detail(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job", params={"param1": "forbidden", "param2": "world"}) as job:
        with pytest.raises(LakeflowRunFailed, match="PermissionError") as excinfo:
            job.run().raise_for_failure()
        assert excinfo.value.run.task("transform").raised(PermissionError)
```

`assert run.succeeded, run` is the shorter form: `repr(run)` names the failing tasks with the first line of their errors, links to the run, and adds `params={...}` when the run failed.

## Read a task's result

Each `TaskRun` describes one task of the run, or the last attempt of a retried task. The fields you will reach for most:

| Field | Holds |
| --- | --- |
| `result_state`, `succeeded`, `ran` | The platform's state, such as `SUCCESS`, `FAILED` or `UPSTREAM_FAILED`, and whether the task ran at all. `excluded`, `upstream_failed`, `disabled`, `cancelled` and `timed_out` each test for one state. |
| `output`, `output_json` | What a notebook passed to `dbutils.notebook.exit()`, as text or parsed as JSON. |
| `logs`, `log_lines` | stdout and stderr of a Python script, wheel or JAR task. Notebook tasks have no logs; what they printed is in `notebook`. |
| `notebook` | The rendered notebook of a notebook task that ran and did not succeed, with each cell's output and `stdout`. See [Read what a notebook printed](recipes/debugging.md#read-what-a-notebook-printed). |
| `error` | A `TaskError` with `type`, `message`, `trace`, `root_cause` and `code`, or `None`. |
| `parameters` | The task's parameters after job parameters and `{{...}}` references were resolved: a dict for notebook, SQL, run-job and named wheel parameters, a list for positional parameters and dbt commands. |
| `task_values`, `missing_task_values` | Task values a probe captured, see [Read task values](#read-task-values). |
| `attempts`, `attempt` | Every attempt of a retried task, in order, and the number of this one. |
| `duration`, `timing`, `effective_performance_target` | How long the task took, split into queue, setup, execution and cleanup, and the serverless performance mode it ran with. |
| `run_url`, `run_id` | The task run's page and id. |
| `sql_result`, `alert_state`, `dbt`, `pipeline_update`, `child_run`, `condition_outcome`, `iterations` | The result of a SQL file, alert, dbt, pipeline, run-job, condition or for-each task. See [Task types](recipes/task-types.md). |

The [API reference](reference/api.md#taskrun) lists every field. When you compare `parameters`, remember two platform rules: a job parameter wins over a task parameter of the same name, and every job parameter is pushed to notebook tasks, SQL file tasks and wheel tasks with named parameters. Compare the entries you set rather than the whole dict.

## Assert on a failure

```python
def test_root_cause_follows_the_exception_chain(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job", params={"param1": "chained", "param2": "world"}) as job:
        run = job.run()
        error = run.task("transform").error
        assert error is not None
        assert error.type == "RuntimeError"
        assert error.message == "wrapper failed"
        assert error.root_cause == "KeyError: 'root cause'"
```

- `task.raised(SomeError)` compares class names, because the exception happened in another process. `error.type` keeps the name as Python printed it, so `json.decoder.JSONDecodeError` matches `raised(json.JSONDecodeError)`.
- `error.root_cause` is the last `Caused by:` line of a Spark or Py4J trace, or the first exception of a Python chain (`raise ... from e`). `error.type` and `error.message` still describe the outer exception.
- `error.code` is the Spark error class, such as `TABLE_OR_VIEW_NOT_FOUND`, when the message or the root cause starts with one in square brackets.
- `task.output_json` parses the value of `dbutils.notebook.exit(json.dumps({...}))`. It raises a `ValueError` naming the task when the notebook never called `exit()`, when the output was cut at 5 MB, or when it is not JSON.
- The Jobs API returns at most 5 MB of output and of logs. When a task's output or logs were cut, building the run emits a `UserWarning` naming the task, and `output_truncated` or `logs_truncated` is true.

What a failed notebook printed is in `task.notebook.stdout`, see [Read what a notebook printed](recipes/debugging.md#read-what-a-notebook-printed).

## Read task values

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

Reading a value that was not captured raises a `KeyError` whose message says why:

- `Task 'transform' never set value 'never_set'`: the task ran and did not set it.
- `Task 'load' did not run (UPSTREAM_FAILED), so value 'anything' was not captured`: the task was skipped.
- `Value 'x' of task 't' was not captured: probe task __pytest_probe__t ended FAILED ...`: the probe failed or its output could not be read.

`missing_task_values` lists the keys that were not captured. Each probe is a serverless notebook task and costs 7 to 30 seconds, even when its task did not run. A for-each task, or the task nested in one, cannot be named, because an iteration cannot set task values.

[Task tests](task-tests.md) explain the probe and stub mechanics. A run that captured values can feed them to a task test or to a later run, see [Replay a task from a previous run](task-tests.md#replay-a-task-from-a-previous-run) and [Replay a run's task values](recipes/runs.md#replay-a-runs-task-values).

## Use a temporary schema

`lakeflow.schema()` creates a schema named `<catalog>.pytest_<run id>_<random>` and drops it on leaving the block, even when the block failed, unless `keep` or `--lakeflow-keep` is set. It needs a catalog, from `Lakeflow(catalog=...)` or `schema(catalog=...)`. Pass the schema to the job as a parameter:

```python
def test_writes_table(lakeflow: Lakeflow) -> None:
    with lakeflow.schema() as schema, lakeflow.job("my_example_job", params={"schema": schema}) as job:
        run = job.run()
        assert run.succeeded, run
        assert lakeflow.ws.tables.exists(f"{schema}.example_rows").table_exists
```

`lakeflow.ws` is the `WorkspaceClient`, for whatever else the test needs to check. To seed the schema before the run, see [Test data](recipes/data.md).

## Run only some tasks

`only` lists the task keys to run. `+key` adds the tasks upstream of `key`, `key+` the tasks downstream, and `+key+` both, as the Jobs API's run-now `only` does.

```python
def test_subset_of_job(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job") as job:
        run = job.run(only=["+transform"])
        assert run.succeeded
        assert "load" not in run.tasks
        assert sorted(run.skipped_tasks) == ["load", "my_wheel_task"]
```

Two things are checked before the run starts, because the platform would otherwise start the run and fail it:

- A selected task that refers to `{{tasks.<key>.values.<name>}}` or `{{tasks.<key>.output.<path>}}` of a task left out raises `LakeflowError` naming both tasks. Select that task too, as `+transform` does, or stub it, see [Run the downstream half of a job](recipes/runs.md#run-the-downstream-half-of-a-job). Other references to a left-out task, such as `{{tasks.extract.result_state}}`, resolve against its `DISABLED` run and are not checked.
- `only` cannot name a task that is disabled in the copy, or one downstream of a disabled task, because the platform runs a disabled task that `only` names. A `+` that only reaches a disabled task leaves it out.

When the job has stubs, probes, setup or teardown tasks, `only` is extended with the ones the selected tasks need.

## When a test fails

The job is kept, with its runs, so you can open it in the workspace. The job and run URLs appear in the test's `lakeflow` report section and as JUnit XML properties, and a `lakeflow` block at the end of the session lists every kept job. `pytest --lakeflow-sweep` deletes kept jobs older than `sweep_hours` at the start of a later session.

[Debug a failure](recipes/debugging.md) covers reading a kept run again, replaying a failed notebook task in your workspace, and keeping every run's output as a CI artefact.
