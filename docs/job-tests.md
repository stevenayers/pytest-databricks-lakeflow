# Writing job tests

A job test makes a throwaway copy of a bundle job, runs it one or more times, and asserts on the outcome. This page covers `lakeflow.job()`, `run()`, and the `JobRun` and `TaskRun` results. The [API reference](api.md) has the full signatures.

## Make a job

```python
with lakeflow.job("my_example_job", params={"param1": "hello", "param2": "world"}) as job:
    run = job.run()
```

- `key` is the bundle resource key of the job.
- `params` are job parameters for every run of the copy. The job must declare them, because the Jobs API accepts undeclared parameters silently. Pass `allow_undeclared_params=True` to `Lakeflow` to allow them anyway. Names starting with `__pytest_` are reserved.
- `capture_values` maps task keys to the task value keys to capture from them, see [below](#capture-task-values).
- `name` replaces the test node id in the job name, which is otherwise `[pytest] <node id>`.

Entering the `with` block creates the job. Leaving it deletes the job, unless the block raised and `keep_failed` is on, which is the default. `pytest.skip` and `pytest.xfail` do not count as failures.

`lakeflow.job()` also works without `with`. The job is then created by the first `run()` and deleted at the end of the test, or kept if the test failed:

```python
def test_without_with(lakeflow: Lakeflow) -> None:
    run = lakeflow.job("my_example_job").run()
    assert run.succeeded
```

## Run it

`job.run(params=None, only=None, timeout=None)` starts a run, waits for it to finish, and returns a `JobRun`.

- `params` are merged over the job's parameters for this run only.
- `only` lists the tasks to run. `+key` adds the tasks upstream of `key`, `key+` the tasks downstream, and `+key+` both, as `run_now` does.
- `timeout` is a `timedelta`. It defaults to `Lakeflow`'s `timeout`, which is 30 minutes. When it expires the run is cancelled and `LakeflowTimeout` is raised.

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

`run.tasks` maps task keys to `TaskRun`s. `run.task("transform")` does the same and raises a `KeyError` that lists the available keys when the task was not part of the run. Tasks left out by `only` are in `run.skipped_tasks`, and the plugin's stubs and probes in `run.injected_tasks`.

Each `TaskRun` has:

- `result_state`, such as `SUCCESS`, `FAILED` or `UPSTREAM_FAILED`, and `succeeded` and `ran`.
- `termination_code` and `termination_message`, from the task's termination details.
- `output`, the value a notebook passed to `dbutils.notebook.exit()`, and `output_truncated`.
- `logs`, the stdout and stderr of a Python script, wheel or JAR task, plus `log_lines` and `logs_truncated`. Notebook tasks have no logs, see [Limitations](limitations.md).
- `error`, a `TaskError` with `type`, `message` and `trace`, or `None`.
- `values` and `missing_values`, task values captured by a probe.
- `duration`, `attempt` and `run_url`.

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

`repr(run)` names the failing tasks with the first line of their errors and links to the run, so `assert run.succeeded, run` gives a useful failure message.

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

## Capture task values

Task values are not exposed by the Jobs API. `capture_values` adds a probe task after each task you name. The probe depends on the task, runs even when the task failed, and reports the values it finds:

```python
def test_values_captured_even_on_failure(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job", params={"param1": "explode"}, capture_values={"transform": ["stage"]}) as job:
        run = job.run()
        assert run.failed
        assert run.task("transform").values["stage"] == "validation"
```

A missing value raises a `KeyError` whose message says why: the task never set it, the task did not run, or the probe failed:

```python
def test_missing_values_explain_themselves(lakeflow: Lakeflow) -> None:
    job = lakeflow.job("my_example_job", capture_values={"transform": ["never_set"], "load": ["anything"]})
    run = job.run(params={"param1": "explode"})
    assert run.task("transform").missing_values == ["never_set"]
    with pytest.raises(KeyError, match="Task 'transform' never set value 'never_set'"):
        run.task("transform").values["never_set"]
    with pytest.raises(KeyError, match=r"Task 'load' did not run \(UPSTREAM_FAILED\)"):
        run.task("load").values["anything"]
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
