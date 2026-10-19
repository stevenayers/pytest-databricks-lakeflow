# pytest-databricks-lakeflow

Test the Lakeflow Jobs in your Databricks Asset Bundle with plain pytest: deploy a throwaway copy of a job or a single task, run it, assert on the result, tear it down.

[![PyPI](https://img.shields.io/pypi/v/pytest-databricks-lakeflow?style=flat-square)](https://pypi.org/project/pytest-databricks-lakeflow/)
[![Python versions](https://img.shields.io/pypi/pyversions/pytest-databricks-lakeflow?style=flat-square)](https://pypi.org/project/pytest-databricks-lakeflow/)
[![CI](https://img.shields.io/github/actions/workflow/status/stevenayers/pytest-databricks-lakeflow/ci.yml?branch=main&style=flat-square&label=CI)](https://github.com/stevenayers/pytest-databricks-lakeflow/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/codecov/c/github/stevenayers/pytest-databricks-lakeflow?style=flat-square)](https://codecov.io/gh/stevenayers/pytest-databricks-lakeflow)
[![Docs](https://img.shields.io/github/actions/workflow/status/stevenayers/pytest-databricks-lakeflow/docs.yml?branch=main&style=flat-square&label=docs)](https://stevenayers.github.io/pytest-databricks-lakeflow/)
[![Licence](https://img.shields.io/github/license/stevenayers/pytest-databricks-lakeflow?style=flat-square)](https://github.com/stevenayers/pytest-databricks-lakeflow/blob/main/LICENSE)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json&style=flat-square)](https://github.com/astral-sh/ruff)

## Why this exists

- A job in a bundle is usually checked by deploying it and pressing Run. This turns that into a pytest test, with `assert`, `parametrize` and xdist.
- Every test runs its own copy of the job, so tests run in parallel and never touch the jobs your bundle deployed, except the children of Run Job tasks, which the plugin warns about.
- Task values cannot be read through the Jobs API. The plugin injects tasks that set and capture them, so one task can be tested on its own with the upstream values you choose.
- Test data needs no seed task in the bundle and no Databricks Connect. `setup=` and `teardown=` run plain Python functions with the real `spark` and `dbutils`, a list of SQL statements, or a notebook file as the first and last tasks of the copy.

## Quickstart

You need Python 3.11 or later, the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install) v1.7.0 or later on `PATH`, credentials that both the CLI and the SDK can use (a `~/.databrickscfg` profile or the `DATABRICKS_*` variables), and a workspace with serverless jobs.

```bash
uv add --dev pytest-databricks-lakeflow
```

Or `pip install pytest-databricks-lakeflow`. Add a `test` target to `databricks.yml`. Development mode pauses schedules, and the separate `root_path` keeps test deployments away from everyone else's:

```yaml
targets:
  test:
    mode: development
    workspace:
      root_path: ~/.bundle/${bundle.name}/test
```

Write a test. This one runs `my_example_job` from [the example bundle](https://github.com/stevenayers/pytest-databricks-lakeflow/tree/main/tests/integration/bundles/example); swap in your own job and task keys:

```python
from pytest_databricks_lakeflow import Lakeflow


def test_job_succeeds(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job", params={"param1": "hello", "param2": "world"}) as job:
        run = job.run()
        run.raise_for_failure()
        assert run.task("transform").output == "hello world"
        assert "Success! hello" in run.task("my_wheel_task").logs
```

Run `pytest`. The first test deploys the bundle's `test` target once, then every test creates, runs and deletes its own job. A failing test keeps its job, and the job and run URLs appear in the test report. Write `assert run.succeeded, run` and a failure reads:

```text
AssertionError: JobRun('my_example_job', FAILED, https://<workspace>/#job/123/run/456, params={'param1': 'forbidden', 'param2': 'world', 'mode': 'full', 'schema': ''}, failing=['transform: PermissionError: forbidden may not read /Volumes/example/raw/landing'])
```

`run.raise_for_failure()` does the same in one call. It returns the run when it succeeded and otherwise raises `LakeflowRunFailed` with that repr followed by one line per failing task: its full error, its root cause (the last `Caused by:` line, or the first exception of a Python chain) when the trace has one, and the task's run URL. The run's job parameters and each failing task's resolved parameters follow on `params:` lines.

`pytest --lakeflow-report-dir=build/lakeflow` also writes each test's runs, those the plugin cancelled included, to a `report.json`, with every task's state, output, error and task values, next to the raw logs and exported notebooks, so a CI build can keep them as an artefact after the jobs are deleted. `pytest --lakeflow-debug-notebook` writes, for each notebook task that failed in a failed test, a notebook in your workspace that sets the task's resolved parameters as widgets and runs the task's notebook, and `job.debug_notebook("transform")` writes one on demand.

To change the target, set a catalog for temporary schemas or lengthen the timeout, override the `lakeflow` fixture in `conftest.py`:

```python
from datetime import timedelta

import pytest

from pytest_databricks_lakeflow import Lakeflow


@pytest.fixture(scope="session")
def lakeflow() -> Lakeflow:
    return Lakeflow(target="test", catalog="dev_sandbox", timeout=timedelta(minutes=20))
```

To skip cluster start-up, pass `existing_cluster_id="<cluster id>"` to `Lakeflow`: tasks that the bundle puts on job clusters then run on that running cluster instead. To keep job clusters but run each on one node, pass `single_node=True`; single node compute needs dedicated access mode.

## Testing one task

`lakeflow.task("job_key.task_key")` builds a job that holds only that task. Each upstream task you name in `upstream_task_values` is replaced by a stub task that sets those task values, or by a `Stub` that also answers the task's `{{tasks.<key>.output.<path>}}` references with literals, and `capture_task_values` adds a probe task that reports the values the task set, even when it failed. `upstream_from=run` takes the stub values from the task values a previous run of the job captured, so a task can be replayed against real upstream data.

```python
import pytest

from pytest_databricks_lakeflow import Lakeflow


@pytest.mark.parametrize(("greeting", "expected"), [("hello", "hello world"), ("goodbye", "goodbye world")])
def test_transform(lakeflow: Lakeflow, greeting: str, expected: str) -> None:
    with lakeflow.task(
        "my_example_job.transform",
        params={"param1": greeting, "param2": "world"},
        upstream_task_values={"extract": {"row_count": 42, "source_path": "/Volumes/x/y/z"}},
        capture_task_values=["rows_written"],
    ) as task:
        run = task.run()
        assert run.succeeded
        assert run.task("transform").output == expected
        assert run.task("transform").task_values["rows_written"] == 42
```

Inside the task, `dbutils.jobs.taskValues.get(taskKey="extract", key="source_path")` and `{{tasks.extract.values.row_count}}` resolve against the stub. Reading a value the task never set raises a `KeyError` that says so.

## How it works

1. The first test that needs a job runs `databricks bundle deploy` for the target once per session and reads each deployed job's settings through the Jobs API.
2. `lakeflow.job()` copies those settings into a new job named `[pytest] <test id>`, with schedules, triggers, notifications, `run_as` and the bundle marker removed and dashboard and SQL alert subscriptions paused (alert and Power BI tasks are left as they are), tagged with the run id, the test, and, when known, the CI run URL, user, commit and branch. Pass `run_as="keep"` to keep the template's `run_as`.
3. `run()` starts the job, waits, and returns a `JobRun` with every task's state, output, logs, error and captured values.
4. Leaving the `with` block deletes the job unless the test failed. Session end cancels anything still running.
5. `lakeflow.task()` does the same for one task, with the stub and probe tasks on serverless compute or a job cluster you name.

## Documentation

Everything else is on the docs site: <https://stevenayers.github.io/pytest-databricks-lakeflow/>

- [Getting started](https://stevenayers.github.io/pytest-databricks-lakeflow/getting-started/): prerequisites, install, bundle target, first test
- [Writing job tests](https://stevenayers.github.io/pytest-databricks-lakeflow/job-tests/): parameters, `only`, stubbed upstream tasks and replay, repeated runs, failures, temporary schemas, shared runs
- [Writing task tests](https://stevenayers.github.io/pytest-databricks-lakeflow/task-tests/): stubs, probes, value rules, helper compute
- [Configuration](https://stevenayers.github.io/pytest-databricks-lakeflow/configuration/): every `Lakeflow` argument, command-line flag and sync strategy
- [How it works](https://stevenayers.github.io/pytest-databricks-lakeflow/how-it-works/): the deploy and teardown lifecycle, xdist, timing
- [Limitations and FAQ](https://stevenayers.github.io/pytest-databricks-lakeflow/limitations/): notebook output read from the rendered notebook, what probes cost, what stays in the workspace
- [API reference](https://stevenayers.github.io/pytest-databricks-lakeflow/api/)

## Contributing and licence

Bug reports and pull requests are welcome. See [CONTRIBUTING.md](https://github.com/stevenayers/pytest-databricks-lakeflow/blob/main/CONTRIBUTING.md) for the development setup, the test suites and how releases work, and [SUPPORT.md](https://github.com/stevenayers/pytest-databricks-lakeflow/blob/main/SUPPORT.md) for where to ask questions.

Apache-2.0. See [LICENSE](https://github.com/stevenayers/pytest-databricks-lakeflow/blob/main/LICENSE).
