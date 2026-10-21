# Getting started

This page takes you from an installed plugin to a passing job test. It assumes you have a [Databricks Asset Bundle](https://docs.databricks.com/aws/en/dev-tools/bundles/) with at least one job, and that you know pytest.

## Prerequisites

- Python 3.11 or later. pytest 8 or later, `databricks-sdk` 0.100 or later and `filelock` are installed as dependencies.
- The [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install) v1.7.0 or later on `PATH`. The plugin shells out to it for `bundle deploy`, `bundle summary`, `bundle validate`, `bundle sync` and `bundle destroy`.
- Credentials that both the CLI and the SDK can use: a profile in `~/.databrickscfg`, or the standard `DATABRICKS_*` environment variables. `DATABRICKS_CONFIG_PROFILE` works for both.
- Serverless jobs in the workspace. The stub and probe tasks the plugin adds, and setup and teardown steps that run as tasks, run on serverless by default. Without serverless, point them at a job cluster, see [Run the helper tasks on a job cluster](recipes/compute.md#run-the-helper-tasks-on-a-job-cluster).
- Permission to create and run jobs, to write to a folder under your workspace home, and, for `lakeflow.schema()`, to create schemas in a catalog.

## Install

```bash
uv add --dev pytest-databricks-lakeflow databricks-connect pytest-xdist
```

Or `pip install pytest-databricks-lakeflow databricks-connect pytest-xdist`. The plugin installs neither of the other two, so add the ones you use:

- **databricks-connect** runs `setup=`, `teardown=` and `once()` steps from your machine over Databricks Connect in a few seconds, instead of as tasks of the job that wait about 30 seconds each for compute. Any release from 15.1 works, so install the one that matches your serverless environment or cluster runtime. Releases from 16.2 install only on Python 3.12, the Python of serverless environment 3 and later, and no release installs on Python 3.13 or later. Without it the steps run as tasks, with one warning per session. databricks-connect replaces pyspark, so remove pyspark from the environment the tests run in.
- **pytest-xdist** runs tests in parallel, with `pytest -n 4`.

The plugin registers itself through the `pytest11` entry point. There is nothing to add to `conftest.py` or `pytest.ini`.

## Add a test target to the bundle

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

[Development mode](https://docs.databricks.com/aws/en/dev-tools/bundles/deployment-modes) pauses schedules and triggers and lets runs overlap. The separate `root_path` keeps the test deployment apart from your `dev` deployment of the same bundle, so the two never fight over the deployment lock.

The plugin deploys this target itself and reads the job settings from it. Your tests never run the deployed jobs. The target is named `test` because that is the default; any name works with `Lakeflow(target=...)`.

## Read catalog and schema from job parameters

Bundle variables are fixed at deploy time, and the plugin deploys once per session. Anything that changes per test, such as the schema a job writes to, must arrive as a job parameter. Declare it on the job and read it in the task:

```yaml
resources:
  jobs:
    my_example_job:
      parameters:
        - name: schema
          default: ""
```

```python
schema = dbutils.widgets.get("schema")
```

[The example bundle](https://github.com/stevenayers/pytest-databricks-lakeflow/tree/main/tests/integration/bundles/example) is a complete bundle written this way. The plugin's own integration tests run against it, and the examples in these docs use its job and task keys.

## Configure the fixture

Tests get a `Lakeflow` from the session-scoped `lakeflow` fixture. Without an override it returns `Lakeflow()`, which deploys target `test`, finds the bundle by walking up from the pytest rootdir to the first `databricks.yml`, and uses the SDK's default credentials. To change any of that, override the fixture in a `conftest.py`:

```python
from datetime import timedelta

import pytest

from pytest_databricks_lakeflow import Lakeflow


@pytest.fixture(scope="session")
def lakeflow() -> Lakeflow:
    return Lakeflow(
        bundle_root="bundle",
        target="test",
        profile="my-workspace",
        catalog="dev_sandbox",
        timeout=timedelta(minutes=20),
    )
```

Creating a `Lakeflow` does nothing. The first test that asks it for a job sets up the bundle. The [Lakeflow arguments](reference/lakeflow.md) lists every argument.

## Write the first test

```python
from pytest_databricks_lakeflow import Lakeflow


def test_job_succeeds(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job", params={"param1": "hello", "param2": "world"}) as job:
        run = job.run()
        assert run.succeeded, run
        assert run.task("transform").output == "hello world"
```

`"my_example_job"` is the bundle resource key, the same name that `databricks bundle run` takes. `assert run.succeeded, run` puts the `JobRun` repr in the failure message, which names the failing tasks with their errors and links to the run.

## Run it

```bash
pytest --log-cli-level=INFO
```

On the first run expect a `bundle deploy`, which builds and uploads any wheels, before the test itself starts. An unchanged bundle deploys again in seconds in later sessions. On a Mac, run a long session under `caffeinate -i pytest ...`: the jobs go on running while the laptop sleeps, but pytest's clock stops, so the time it prints is not the time you waited.

The INFO log shows the deploy, the job's creation with its URL, the start and end of each run, and a line each time a task changes state, such as `transform: RUNNING -> FAILED in 0:00:42 ValueError: no rows`. `pytest --lakeflow-live` prints only those run and task lines, without INFO from every other logger.

When a test fails, its job is kept so you can open the run in the workspace. The job and run URLs and the task state lines appear in a `lakeflow` section of the test report, and a `lakeflow` block at the end of the session lists every kept job. [Debug a failure](recipes/debugging.md) covers what to do next.

Tests that use the fixture get the `lakeflow` marker automatically, so `pytest -m "not lakeflow"` runs only your fast tests. A session that never creates a job deploys nothing.

## Share fixtures, run in parallel

Data that every test reads, such as the source tables behind a job's config, goes in `lakeflow.once()`, in a session fixture. The step runs one time per session, over Databricks Connect from whichever xdist worker asks first, however many tests and workers ask for it, and its teardown runs when the session ends. Data that one test owns goes in `setup=` on that test's job, or in `lakeflow.schema()`:

```python
import pytest

from pytest_databricks_lakeflow import Lakeflow

SOURCES = [
    "CREATE SCHEMA IF NOT EXISTS ${schema}",
    "CREATE OR REPLACE TABLE ${schema}.orders AS SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, sku)",
]


@pytest.fixture(scope="session")
def sources(lakeflow: Lakeflow) -> None:
    lakeflow.once(
        "sources", setup=SOURCES, teardown=["DROP SCHEMA ${schema} CASCADE"], params={"schema": "dev_sandbox.sources"}
    )


def test_job_reads_the_sources(lakeflow: Lakeflow, sources: None) -> None:
    with lakeflow.job("my_example_job", params={"schema": "dev_sandbox.sources"}) as job:
        assert job.run().succeeded
```

A setup in every copy would run the same statements for every test, and would stop the tests running at the same time, because each copy recreates the same tables. With the fixtures made once, the tests are independent:

```bash
pytest -n 4
```

One worker deploys the bundle and runs the step while the others wait, then every worker reuses both. [Speed and cost](speed.md) explains where a session's time goes and how to keep it short.

## Next steps

- [Job tests](job-tests.md): what `job()` and `run()` take, and what a `JobRun` gives back.
- [Task tests](task-tests.md): testing one task with stubbed upstream values.
- [Recipes](recipes/index.md): an "I want to..." index of everything you can change about the copy, the run and the session, including [running the tests in CI](recipes/ci.md).
- [Reference](reference/lakeflow.md): every `Lakeflow` argument, [command-line flag](reference/flags.md) and report format.
