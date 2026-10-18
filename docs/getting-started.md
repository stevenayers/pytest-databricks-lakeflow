# Getting started

This page takes you from an installed plugin to a passing job test. It assumes you have a Databricks Asset Bundle with at least one job, and that you know pytest.

## Prerequisites

- Python 3.11 or later. pytest 8 or later, `databricks-sdk` 0.66 or later and `filelock` are installed as dependencies.
- The [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install) v1.7.0 or later on `PATH`. The plugin shells out to it for `bundle deploy`, `bundle summary`, `bundle validate`, `bundle sync` and `bundle destroy`, and checks the version before the first command.
- Credentials that both the CLI and the SDK can use: a profile in `~/.databrickscfg`, or the standard `DATABRICKS_*` environment variables. `DATABRICKS_CONFIG_PROFILE` works for both.
- Serverless jobs in the workspace. The stub and probe tasks that task tests add run on serverless by default. Without serverless, point them at a job cluster with `helpers_compute`, see [helper compute](task-tests.md#helper-compute).
- Permission to create and run jobs, to write to a folder under your workspace home, and, if you use `lakeflow.schema()`, to create schemas in a catalog.

## Install

```bash
uv add --dev pytest-databricks-lakeflow
```

Or `pip install pytest-databricks-lakeflow`. For parallel runs with pytest-xdist, install the extra `pytest-databricks-lakeflow[xdist]`.

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

`mode: development` pauses schedules and triggers and lets runs overlap. The separate `root_path` keeps the test deployment apart from your `dev` deployment of the same bundle, so the two never fight over the deployment lock. The plugin deploys this target itself and reads the job settings from it. Your tests never run the deployed jobs.

The target is named `test` because that is the default. Any name works if you pass it as `Lakeflow(target=...)`.

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

[`examples/bundle`](https://github.com/stevenayers/pytest-databricks-lakeflow/tree/main/examples/bundle) is a complete bundle written this way. The plugin's own integration tests run against it.

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

Creating a `Lakeflow` does nothing. The first test that asks it for a job sets up the bundle. [Configuration](configuration.md) lists every argument.

## Write the first test

```python
from pytest_databricks_lakeflow import Lakeflow


def test_job_succeeds(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job", params={"param1": "hello", "param2": "world"}) as job:
        run = job.run()
        assert run.succeeded, run
        assert run.task("transform").output == "hello world"
```

`"my_example_job"` is the bundle resource key, the same name that `databricks bundle run` takes. `assert run.succeeded, run` puts the `JobRun` repr in the failure message, which names the failing tasks with their errors and links to the run. `run.raise_for_failure()` is the alternative: it returns the run when it succeeded and otherwise raises `LakeflowRunFailed` with the same repr plus one line per failing task, so `run = job.run().raise_for_failure()` also works.

## Run it

```bash
pytest --log-cli-level=INFO
```

The plugin logs at INFO when it deploys, creates a job (with its URL), starts and finishes a run, and deletes the job. While a run is in flight it also logs a line each time a task changes state, such as `transform: PENDING -> RUNNING` and `transform: RUNNING -> FAILED in 0:00:42 ValueError: no rows`, so a long run shows its progress. On the first run expect a `bundle deploy`, which builds and uploads any wheels, before the test itself starts. Later sessions deploy again, but an unchanged bundle deploys in seconds.

When a test fails, its job is kept so you can open the run in the workspace. The job and run URLs, followed by the task state lines, are printed in a `lakeflow` section of the test report, and the URLs are also recorded as JUnit XML properties. A `lakeflow` block at the end of the run lists every kept job with its URL.

Tests that use the fixture get the `lakeflow` marker automatically, so `pytest -m "not lakeflow"` runs only your fast tests. Nothing is deployed in a session that never creates a job.

## Run in CI

Use a service principal with OAuth machine-to-machine credentials, and install the CLI with `databricks/setup-cli`:

```yaml
env:
  DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST }}
  DATABRICKS_CLIENT_ID: ${{ secrets.DATABRICKS_CLIENT_ID }}
  DATABRICKS_CLIENT_SECRET: ${{ secrets.DATABRICKS_CLIENT_SECRET }}
steps:
  - uses: databricks/setup-cli@<pinned sha>
  - run: uv run pytest -n 4 --lakeflow-sweep
```

The run id that tags every job is read from `GITHUB_RUN_ID` or `BUILD_BUILDID`, so jobs can be traced back to the pipeline run. `--lakeflow-sweep` deletes jobs that an earlier, crashed run left behind. Several processes refreshing a user-to-machine (`databricks-cli`) token at once can fail with `exit status 45`, which is why CI should use a service principal.

Jobs that the service principal creates are visible only to it and to workspace admins. Give your team access with `permissions`, so that they can open a kept job from the CI log:

```python
import pytest

from pytest_databricks_lakeflow import Lakeflow


@pytest.fixture(scope="session")
def lakeflow() -> Lakeflow:
    return Lakeflow(permissions=[{"group_name": "data-eng", "permission_level": "CAN_VIEW"}])
```

## Next steps

- [Writing job tests](job-tests.md): parameters, `only`, repeated runs, failures, temporary schemas and shared runs.
- [Writing task tests](task-tests.md): testing one task with stubbed upstream values.
- [Configuration](configuration.md): every `Lakeflow` argument and command-line flag.
- [How it works](how-it-works.md): what the plugin does in the workspace and when.
