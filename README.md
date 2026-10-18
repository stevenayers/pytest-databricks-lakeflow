# pytest-databricks-lakeflow

Write ordinary pytest tests that run a Lakeflow Job, or a single task from one, defined in your Databricks Asset Bundle. Each test gets its own throwaway copy of the job, runs it with the parameters you choose, and asserts on the outcome: success or failure, task outputs, logs, task values, and the tables the job wrote.

```python
import pytest

from pytest_databricks_lakeflow import Lakeflow


def test_job_succeeds(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job", params={"param1": "hello", "param2": "world"}) as job:
        run = job.run()
        assert run.succeeded
        assert run.task("transform").output == "hello world"
        assert "Success!" in run.task("my_wheel_task").logs


def test_job_surfaces_permission_error(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job", params={"param1": "forbidden"}) as job:
        run = job.run()
        assert run.failed
        assert run.termination_code == "RUN_EXECUTION_ERROR"
        assert run.task("transform").raised(PermissionError)


def test_subset_of_job(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job") as job:
        run = job.run(only=["+transform"])
        assert run.succeeded
        assert "load" not in run.tasks


@pytest.mark.parametrize(("greeting", "expected"), [("hello", "hello world"), ("goodbye", "goodbye world")])
def test_single_task(lakeflow: Lakeflow, greeting: str, expected: str) -> None:
    with lakeflow.schema() as schema:
        with lakeflow.task(
            "my_example_job.transform",
            params={"param1": greeting, "param2": "world", "schema": schema},
            upstream_values={"extract": {"row_count": 42, "source_path": "/Volumes/x/y/z"}},
            capture_values=["rows_written"],
        ) as task:
            run = task.run()
            assert run.succeeded
            assert run.task("transform").output == expected
            assert run.task("transform").values["rows_written"] == 42


def test_run_twice_in_one_job(lakeflow: Lakeflow) -> None:
    with lakeflow.schema() as schema, lakeflow.job("my_example_job", params={"schema": schema}) as job:
        first = job.run()
        second = job.run(params={"mode": "incremental"})
        assert first.succeeded and second.succeeded
        assert second.duration < first.duration


def test_values_captured_even_on_failure(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job", params={"param1": "explode"}, capture_values={"transform": ["stage"]}) as job:
        run = job.run()
        assert run.failed
        assert run.task("transform").values["stage"] == "validation"
```

[`examples/bundle`](https://github.com/stevenayers/pytest-databricks-lakeflow/tree/main/examples/bundle) is a complete bundle that these tests run against.

## Install

```bash
pip install pytest-databricks-lakeflow
```

You also need:

- Python 3.11 or later, pytest 8 or later and `databricks-sdk` 0.54 or later. These are installed as dependencies.
- The [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install) v1.7.0 or later on `PATH`. The plugin shells out to it for `bundle deploy`, `summary`, `validate`, `sync` and `destroy`.
- Credentials that both the CLI and the SDK can use: a profile in `~/.databrickscfg` or the standard `DATABRICKS_*` environment variables.
- Serverless jobs in the workspace for the helper tasks, or see `helpers_compute` below.

For parallel tests, install the extra: `pip install "pytest-databricks-lakeflow[xdist]"`.

## Set up

Add a target for tests to `databricks.yml`. Development mode pauses schedules and triggers and lets runs overlap, and the separate `root_path` keeps the test deployment away from everyone else's:

```yaml
targets:
  test:
    mode: development
    workspace:
      root_path: ~/.bundle/${bundle.name}/test
    presets:
      trigger_pause_status: PAUSED
      tags:
        purpose: pytest
```

Tests get a `Lakeflow` from the `lakeflow` fixture. To configure it, override the fixture in a `conftest.py`:

```python
from datetime import timedelta

import pytest

from pytest_databricks_lakeflow import Lakeflow


@pytest.fixture(scope="session")
def lakeflow() -> Lakeflow:
    return Lakeflow(target="test", catalog="dev_sandbox", timeout=timedelta(minutes=20))
```

Without an override, the plugin's fixture returns `Lakeflow()`, which uses the defaults listed under [Configuration](#configuration).

The job code under test must read its catalog and schema from **job parameters**, not from bundle variables. Bundle variables are fixed at deploy time, so passing a different value per test only works through parameters.

## How it works

1. The first test that calls `lakeflow.job()`, `task()` or `template()` deploys the bundle once with `databricks bundle deploy -t test`, reads the deployed job ids from `databricks bundle summary`, and loads each job's settings with `jobs.get`. Those settings have resolved workspace paths for notebooks and uploaded wheels. It also uploads two small helper notebooks to `/Users/<you>/.pytest-databricks-lakeflow/`.
2. `lakeflow.job(key)` copies the settings of the bundle job `key` (the resource key, as used by `databricks bundle run`). It removes the schedule, triggers, notifications, `run_as` and the bundle deployment marker, allows one run at a time, names the job `[pytest] <test node id>` and tags it `pytest`, `pytest_run_id` and `pytest_nodeid`.
3. Entering the `with` block creates the job, and `run()` runs it and waits. You can call `run()` more than once.
4. Leaving the block deletes the job. If the block exited with an exception, other than `pytest.skip` or `pytest.xfail`, the job is kept so you can open the run in the UI. The job and run URLs are logged and added to the test report.

Your tests never run the deployed jobs directly.

`lakeflow.job()` and `lakeflow.task()` also work without `with`. The job is then created by the first `run()` and deleted at the end of the test, or kept if the test failed.

To share one run between several tests, run the job in a fixture with a broader scope, inside a `with` block so that it is deleted when the fixture is torn down:

```python
from collections.abc import Iterator

import pytest

from pytest_databricks_lakeflow import JobRun, Lakeflow


@pytest.fixture(scope="module")
def nightly_run(lakeflow: Lakeflow) -> Iterator[JobRun]:
    with lakeflow.job("my_example_job") as job:
        yield job.run()
```

### Single tasks and task values

`lakeflow.task("job_key.task_key", ...)` builds a job that holds only that task. For each upstream task named in `upstream_values` it adds a stub task with the same key, which sets those task values. Both `dbutils.jobs.taskValues.get(taskKey="extract", ...)` and `{{tasks.extract.values.row_count}}` in the task's settings then resolve against the stub.

Task values cannot be read through the Jobs API, so `capture_values` adds a probe task that runs after the target, even when it fails, and reports the values. `lakeflow.job(key, capture_values={"transform": ["stage"]})` does the same for a whole job.

When a value is missing, `run.task("t").values["k"]` raises a `KeyError` that says why: the task never set it, the task did not run, or the probe failed.

Stub values must be JSON-serialisable, at most 49,152 characters of JSON each, with keys of at most 512 characters. The plugin checks this before creating the job. If the target task refers to task values of an upstream task you did not stub, you get an error before anything is created.

## API

### The `lakeflow` fixture

Session-scoped, and returns `Lakeflow()` unless you override it. Tests that use it get the `lakeflow` marker automatically, so `pytest -m "not lakeflow"` runs only your fast tests. Nothing is deployed in a session that never creates a job.

### `Lakeflow`

- `job(key, params=None, capture_values=None, name=None) -> EphemeralJob`. `params` are job parameters for every run. `capture_values` maps task keys to lists of value keys. `name` replaces the node id in the job name.
- `task("job_key.task_key", params=None, upstream_values=None, capture_values=None) -> EphemeralJob`. `upstream_values` maps upstream task keys to the values their stubs set. `capture_values` is a list of value keys.
- `schema(catalog=None)`: a context manager that creates `<catalog>.pytest_<run id>_<random>` and yields its full name. It is dropped on leaving the block, even if the block failed, unless `keep` or `--lakeflow-keep` is set.
- `template(key) -> JobSettings`: a fresh copy of the deployed settings.
- `ws`: the `WorkspaceClient`, for example to check that a table exists.
- `run_id`: the id that tags every job of this session.

Parameters that the job does not declare raise an error, because the API would accept them silently. Pass `allow_undeclared_params=True` to pass them anyway. Names starting with `__pytest_` are reserved.

### `EphemeralJob`

- `run(params=None, only=None, timeout=None) -> JobRun`. `params` are merged over the job's params for this run. `only` selects tasks, and `+task`, `task+` and `+task+` add upstream and downstream tasks. `timeout` is a `timedelta` that defaults to `Lakeflow`'s `timeout`. On timeout the run is cancelled and `LakeflowTimeout` is raised.
- `job_id`, `url` and `runs`.

### `JobRun`

- `succeeded`: true only if every task in `tasks` ended `SUCCESS`, apart from tasks `EXCLUDED` because their `run_if` or if/else condition was not met. `failed` is `not succeeded`.
- `tasks`: your tasks that were part of the run, by key. `skipped_tasks`: tasks left out by `only`. `injected_tasks`: the plugin's stubs and probes.
- `task(key)`: raises a `KeyError` that lists the available keys.
- `result_state`, `termination_code`, `termination_message`, `duration`, `run_id` and `run_url` are those of the parent run. Do not use the parent's `result_state` as the verdict: a job with a probe that runs after a failure ends `SUCCESS_WITH_FAILURES`.

### `TaskRun`

- `succeeded`, `result_state`, `termination_code`, `termination_message`, `duration`, `run_url` and `attempt`.
- `output`: the value passed to `dbutils.notebook.exit()`.
- `logs` and `log_lines`: stdout and stderr of Python script and wheel tasks.
- `error`: a `TaskError` with `type` (for example `"PermissionError"`), `message` and `trace`. `raised(PermissionError)` compares class names.
- `values` and `missing_values`: captured task values.

### Callbacks

Pass these to `Lakeflow`:

- `modify_settings(settings, item)`: called just before each job is created, with its `JobSettings` and the test's `pytest.Item`. Change `settings` in place.
- `run_finished(run, item)`: called after each run with its `JobRun`.

## Configuration

`Lakeflow` takes these keyword arguments:

| Argument | Default | Meaning |
| --- | --- | --- |
| `bundle_root` | nearest directory with `databricks.yml` | Bundle root, relative to the rootdir. |
| `target` | `"test"` | Bundle target. |
| `profile` | standard SDK resolution | Profile for both the CLI and the SDK. `DATABRICKS_CONFIG_PROFILE` works too. |
| `workspace_client` | made from `profile` | A `WorkspaceClient` to use instead. The CLI uses `profile`, or else the client's profile. |
| `sync` | `"deploy"` | `"deploy"`, `"sync"` or `"none"`. See below. |
| `timeout` | 30 minutes | Default `timedelta` for one run. |
| `catalog` | | Catalog for `schema()`. |
| `helpers_path` | `/Users/<you>/.pytest-databricks-lakeflow` | Workspace folder for the helper notebooks. |
| `helpers_compute` | serverless | A `job_cluster_key` from the job for stub and probe tasks, for workspaces without serverless. |
| `helpers_environment_version` | `"4"` | Serverless environment version for stub and probe tasks. |
| `disable_auto_optimization` | `True` | Stop serverless from retrying failed tasks, so a failure fails once and fast. |
| `performance_optimized` | `False` | Run every ephemeral job as `PERFORMANCE_OPTIMIZED`. |
| `allow_undeclared_params` | `False` | Allow parameters the job does not declare. |
| `sweep_hours` | `24` | Age after which `--lakeflow-sweep` deletes leftover jobs. |
| `keep` | `False` | Keep every job and schema. |
| `keep_failed` | `True` | Keep the job when its test fails. |
| `modify_settings`, `run_finished` | | See [Callbacks](#callbacks). |

These command-line flags are for one run, and win over the arguments:

| Flag | Meaning |
| --- | --- |
| `--lakeflow-keep` | Keep every job and schema. |
| `--lakeflow-keep-failed`, `--no-lakeflow-keep-failed` | Keep, or delete, the job when its test fails. |
| `--lakeflow-sweep` | At session start, delete your own jobs tagged `pytest` from other runs that are older than `sweep_hours`. |
| `--lakeflow-destroy` | Run `databricks bundle destroy` at the end of the session for each target that was deployed. |

A `conftest.py` in a subdirectory can override `lakeflow` again for the tests below it. `Lakeflow` is a dataclass, so the override can start from the parent's:

```python
import dataclasses


@pytest.fixture(scope="session")
def lakeflow(lakeflow: Lakeflow) -> Lakeflow:
    return dataclasses.replace(lakeflow, catalog="team_b")
```

Every `Lakeflow` that uses the same bundle target shares one deploy. A second target, such as `Lakeflow(target="staging")` in another fixture, is deployed once too.

The run id that tags every job comes from `LAKEFLOW_RUN_ID`, then `GITHUB_RUN_ID`, then `BUILD_BUILDID`, and is otherwise random.

### Sync strategies

- `deploy` (the default) deploys once per session and reads templates from the deployed jobs. This is the only strategy that builds and uploads wheels.
- `sync` runs `databricks bundle sync` and reads templates from `databricks bundle validate`. Jobs that use local wheels or JARs are refused, because `sync` does not upload them.
- `none` does nothing and reads templates from `validate`. Local wheel paths are pointed at the copies that an earlier `deploy` uploaded, after checking that they exist.

With `sync` and `none`, a job that uses `${resources...}` references is refused, because `validate` does not resolve them.

## Running in parallel

With pytest-xdist, one worker deploys and loads the templates while the others wait, then all of them reuse the result. Each worker uses its own `WorkspaceClient`.

With `databricks-cli` (U2M) authentication, several processes refreshing the token at once can fail with `exit status 45`. The plugin retries, but OAuth machine-to-machine credentials or a token are more reliable in CI.

## Timing

Measured in one Azure workspace:

- A trivial serverless notebook test takes about 20 to 35 seconds end to end, of which about 2 seconds is API overhead.
- A job with `performance_target: STANDARD` adds 3 to 4 minutes to every run. The plugin warns about it, and `performance_optimized=True` overrides it.
- A classic job cluster takes 2 to 10 minutes to start.
- Five jobs at once took no longer than one.

## Limitations

- **Logs.** `TaskRun.logs` is only filled for Python script, wheel and JAR tasks, and holds only the last 5 MB. Notebook tasks have no logs; use `output` instead. Notebook output keeps only the first 5 MB. The Databricks documentation says that on serverless compute the logs of one task can include output from other tasks in the same run. In our tests each task's logs held only its own output, but this is not guaranteed.
- **Task values** are not available through the Jobs API, so they are read by probe tasks. Each probe costs 7 to 30 seconds of serverless compute, even when its task did not run.
- **Job code must read its catalog and schema from job parameters**, as described above.
- **Single-task jobs** keep only the target task. On a shared job cluster, a task can import libraries that other tasks declared. On its own it loses them, so declare every library on the task that needs it.
- **Serverless auto-retry** is turned off in ephemeral jobs by default, so a test sees one attempt. Pass `disable_auto_optimization=False` to keep the platform's behaviour. When a task is retried, `TaskRun` describes the last attempt.
- **Standard (shared) access mode** classic clusters were not tested. There, task values set from wheel or script tasks may be silently lost.
