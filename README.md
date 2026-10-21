# pytest-databricks-lakeflow

Test the Lakeflow Jobs in your Databricks Asset Bundle with plain pytest. Each test gets a throwaway copy of a job, or of one task, runs it with the parameters the test chooses, asserts on the result, and deletes the copy.

[![PyPI](https://img.shields.io/pypi/v/pytest-databricks-lakeflow?style=flat-square)](https://pypi.org/project/pytest-databricks-lakeflow/)
[![Python versions](https://img.shields.io/pypi/pyversions/pytest-databricks-lakeflow?style=flat-square)](https://pypi.org/project/pytest-databricks-lakeflow/)
[![CI](https://img.shields.io/github/actions/workflow/status/stevenayers/pytest-databricks-lakeflow/ci.yml?branch=main&style=flat-square&label=CI)](https://github.com/stevenayers/pytest-databricks-lakeflow/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/codecov/c/github/stevenayers/pytest-databricks-lakeflow?style=flat-square)](https://codecov.io/gh/stevenayers/pytest-databricks-lakeflow)
[![Docs](https://img.shields.io/github/actions/workflow/status/stevenayers/pytest-databricks-lakeflow/docs.yml?branch=main&style=flat-square&label=docs)](https://stevenayers.github.io/pytest-databricks-lakeflow/)
[![Licence](https://img.shields.io/github/license/stevenayers/pytest-databricks-lakeflow?style=flat-square)](https://github.com/stevenayers/pytest-databricks-lakeflow/blob/main/LICENSE)

## What you get

- **A job test is a pytest test.** `assert`, `parametrize`, fixtures and pytest-xdist work as they do everywhere else. Tests that need a workspace get the `lakeflow` marker, so `pytest -m "not lakeflow"` keeps the fast suite fast.
- **The deployed jobs are never run.** Every test runs its own copy, named and tagged after the test, with schedules, triggers, notifications and `run_as` removed. Copies run in parallel and are deleted when the test ends, unless it failed.
- **Results you can assert on.** `run.task("transform")` gives the task's state, output, logs, error, resolved parameters, task values, SQL rows, pipeline update, dbt artefacts and what a failed notebook printed.
- **One task at a time.** Task values are not exposed by the Jobs API, so `lakeflow.task("job.task")` runs a single task between a stub task that sets the upstream values you choose and a probe task that reads the values it set.
- **Test data without a seed task.** `lakeflow.once()` creates the fixtures every test reads one time per session, however many tests and xdist workers there are, and tears them down at the end. `setup=` and `teardown=` run a function with `spark` and `dbutils`, or a list of SQL statements, before and after each run of one copy, for data one test owns. By default they run from your machine over Databricks Connect, in a few seconds; a notebook file, or `run_as_task=True`, runs as a task of the copy instead. `lakeflow.schema()` gives each test a temporary schema.
- **Pipelines too.** `pipelines="copy"` points a job's pipeline tasks at copies that publish to the test's schema, and `lakeflow.pipeline()` copies one pipeline on its own.
- **Failures explain themselves.** A failed assertion names the failing tasks with their errors and links to the run. The job is kept for you to open, `--lakeflow-debug-notebook` writes a notebook that replays the failed task, and `--lakeflow-report-dir` keeps every run's output as a CI artefact.
- **Wiring checks without a run.** `lakeflow.check("my_job")` lists the mistakes that `bundle validate` passes, such as an undeclared `{{job.parameters.x}}`, and `pytest --lakeflow-check` collects one check per bundle job.

## Quickstart

You need Python 3.11 or later, the [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install) v1.7.0 or later on `PATH`, credentials that both the CLI and the SDK can use (a `~/.databrickscfg` profile or the `DATABRICKS_*` variables), a workspace with serverless jobs, and a [Databricks Asset Bundle](https://docs.databricks.com/aws/en/dev-tools/bundles/) with at least one job.

```bash
uv add --dev pytest-databricks-lakeflow
```

Or `pip install pytest-databricks-lakeflow`. It brings databricks-connect, which runs `setup=`, `teardown=` and `once()` steps from your machine in a few seconds rather than as 30 second tasks. No databricks-connect release installs on Python 3.13 or later, so there the steps run as tasks. Add a `test` target to `databricks.yml`. Development mode pauses schedules, and the separate `root_path` keeps the test deployment apart from your `dev` deployment:

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

Run `pytest`. The first test deploys the bundle's `test` target once, then every test creates, runs and deletes its own job. Write `assert run.succeeded, run` and a failure reads:

```text
AssertionError: JobRun('my_example_job', FAILED, https://<workspace>/#job/123/run/456, params={'param1': 'forbidden', 'param2': 'world', 'mode': 'full', 'schema': ''}, failing=['transform: PermissionError: forbidden may not read /Volumes/example/raw/landing'])
```

To test one task, name it as `job_key.task_key`, give the upstream task values it reads, and capture the values it sets:

```python
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

To change the target, set a catalog for temporary schemas or lengthen the timeout, override the `lakeflow` fixture in `conftest.py`:

```python
from datetime import timedelta

import pytest

from pytest_databricks_lakeflow import Lakeflow


@pytest.fixture(scope="session")
def lakeflow() -> Lakeflow:
    return Lakeflow(target="test", catalog="dev_sandbox", timeout=timedelta(minutes=20))
```

Fixtures that every test reads belong in `lakeflow.once()`, in a session fixture, not in each test's job: a setup in every copy runs the same statements once per test and stops the tests running in parallel, because every copy recreates the same tables. `once()` runs the step one time for the session, whatever the number of xdist workers, and its teardown when the session ends:

```python
SEED = ["CREATE SCHEMA IF NOT EXISTS ${schema}", "CREATE OR REPLACE TABLE ${schema}.orders AS SELECT 1 AS id"]


@pytest.fixture(scope="session")
def orders(lakeflow: Lakeflow) -> None:
    lakeflow.once(
        "orders", setup=SEED, teardown=["DROP SCHEMA ${schema} CASCADE"], params={"schema": "dev_sandbox.orders"}
    )


def test_job_reads_orders(lakeflow: Lakeflow, orders: None) -> None:
    with lakeflow.job("my_example_job", params={"schema": "dev_sandbox.orders"}) as job:
        assert job.run().succeeded
```

With the fixtures made once, the tests are independent, so run them in parallel with `pytest -n 4` (install the `pytest-databricks-lakeflow[xdist]` extra).

## Where next

The docs site is at <https://stevenayers.github.io/pytest-databricks-lakeflow/>.

- [Getting started](https://stevenayers.github.io/pytest-databricks-lakeflow/getting-started/): from an installed plugin to a passing test against your own bundle.
- [Job tests](https://stevenayers.github.io/pytest-databricks-lakeflow/job-tests/) and [task tests](https://stevenayers.github.io/pytest-databricks-lakeflow/task-tests/): the two kinds of test and what a run gives back.
- [Recipes](https://stevenayers.github.io/pytest-databricks-lakeflow/recipes/): an "I want to..." index covering every option, from disabling a task to running the suite in CI.
- [Reference](https://stevenayers.github.io/pytest-databricks-lakeflow/reference/lakeflow/): every `Lakeflow` argument, command-line flag, environment variable and report format, the API, and the limitations.
- [How it works](https://stevenayers.github.io/pytest-databricks-lakeflow/how-it-works/) and the [FAQ](https://stevenayers.github.io/pytest-databricks-lakeflow/faq/).

## Contributing and licence

Bug reports and pull requests are welcome. See [CONTRIBUTING.md](https://github.com/stevenayers/pytest-databricks-lakeflow/blob/main/CONTRIBUTING.md) for the development setup, the test suites and how releases work, and [SUPPORT.md](https://github.com/stevenayers/pytest-databricks-lakeflow/blob/main/SUPPORT.md) for where to ask questions.

Apache-2.0. See [LICENSE](https://github.com/stevenayers/pytest-databricks-lakeflow/blob/main/LICENSE).
