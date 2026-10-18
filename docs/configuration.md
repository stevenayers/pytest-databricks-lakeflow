# Configuration

All configuration is on the `Lakeflow` object that the `lakeflow` fixture returns. Override the fixture in a `conftest.py` and pass keyword arguments. The few command-line flags cover what changes from one run to the next.

## `Lakeflow` arguments

All arguments are keyword-only.

| Argument | Default | Meaning |
| --- | --- | --- |
| `bundle_root` | nearest directory with `databricks.yml` | The bundle's directory. Relative paths are resolved against the pytest rootdir. Without it, the plugin walks up from the rootdir to the first directory holding `databricks.yml` or `databricks.yaml`. |
| `target` | `"test"` | The bundle target to deploy and read job templates from. |
| `profile` | SDK default resolution | The Databricks config profile for both the CLI and the SDK. Without it, both read `DATABRICKS_CONFIG_PROFILE` and the other `DATABRICKS_*` variables. |
| `workspace_client` | made from `profile` | A `WorkspaceClient` to use instead. The CLI still uses `profile`, or else the client's profile. |
| `sync` | `"deploy"` | How to prepare the bundle: `"deploy"`, `"sync"` or `"none"`. See [sync strategies](#sync-strategies). |
| `timeout` | 30 minutes | The default `timedelta` to wait for one run. |
| `catalog` | `None` | The catalog in which `schema()` creates temporary schemas. |
| `helpers_path` | `/Users/<you>/.pytest-databricks-lakeflow` | The workspace folder for the two helper notebooks. |
| `helpers_compute` | `None`, meaning serverless | A `job_cluster_key` from the job for stub and probe tasks, for workspaces without serverless. |
| `helpers_environment_version` | `"4"` | The serverless environment version for stub and probe tasks. |
| `disable_auto_optimization` | `True` | Set `disable_auto_optimization` on serverless code tasks so that a failure fails once and fast, instead of being retried by the platform. |
| `performance_optimized` | `False` | Run every ephemeral job with `performance_target: PERFORMANCE_OPTIMIZED`, whatever the template says. |
| `allow_undeclared_params` | `False` | Allow job parameters that the job does not declare. |
| `sweep_hours` | `24` | The age after which `--lakeflow-sweep` deletes leftover jobs. |
| `keep` | `False` | Keep every ephemeral job and temporary schema. |
| `keep_failed` | `True` | Keep the ephemeral job when its test fails. |
| `modify_settings` | `None` | Called with each job's `JobSettings` and the test's `pytest.Item` just before the job is created. |
| `run_finished` | `None` | Called with each `JobRun` and the test after a run finishes. |

`Lakeflow` is a dataclass, so a `conftest.py` in a subdirectory can derive its own from the parent's:

```python
import dataclasses

import pytest

from pytest_databricks_lakeflow import Lakeflow


@pytest.fixture(scope="session")
def lakeflow(lakeflow: Lakeflow) -> Lakeflow:
    return dataclasses.replace(lakeflow, catalog="team_b")
```

The override applies to the tests below that directory only. Every `Lakeflow` that names the same bundle target shares one deploy. A second target, such as `Lakeflow(target="staging")` in another fixture, is deployed once too.

## Command-line flags

These apply to one pytest run and win over the matching arguments.

| Flag | Meaning |
| --- | --- |
| `--lakeflow-keep` | Keep every ephemeral job and temporary schema. |
| `--lakeflow-keep-failed` | Keep the ephemeral job when its test fails, even with `Lakeflow(keep_failed=False)`. |
| `--no-lakeflow-keep-failed` | Delete the ephemeral job even when its test fails. |
| `--lakeflow-sweep` | At session start, delete your own jobs tagged `pytest` from other runs that are older than `sweep_hours`. |
| `--lakeflow-destroy` | Run `databricks bundle destroy` at the end of the session for each target that the session deployed. |

## Environment variables

| Variable | Meaning |
| --- | --- |
| `LAKEFLOW_RUN_ID` | The run id that tags every job and names every temporary schema. Without it the plugin reads `GITHUB_RUN_ID`, then `BUILD_BUILDID`, then makes a random 8-character id. |
| `DATABRICKS_CONFIG_PROFILE`, `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET` and the rest of the SDK's resolution | Credentials, when `profile` is not set. The CLI reads the same variables. |

## Sync strategies

- `deploy`, the default, runs `databricks bundle deploy -t <target> --auto-approve` once per session and reads the templates from the deployed jobs with the Jobs API. This is the only strategy that builds and uploads wheels, and the only one in which every path is already a resolved workspace path.
- `sync` runs `databricks bundle sync -t <target>` and reads the templates from `databricks bundle validate -o json`. Jobs that use local wheels or JARs are refused, because `sync` does not upload them.
- `none` runs no CLI command apart from `validate`. Local wheel and JAR paths are pointed at the copies that an earlier `deploy` uploaded under the target's artifact path, after checking that they exist.

With `sync` and `none`, a job that still contains `${...}` references after `validate`, such as `${resources.jobs.other.id}`, is refused, because `validate` does not resolve them. Use these strategies when you deploy the target yourself, for example once per CI pipeline, and want test sessions to start without a deploy.

## Callbacks

`modify_settings(settings, item)` runs after the plugin has sanitised the copied settings and before `jobs.create`. Change `settings` in place, for example to shrink a cluster, swap an environment or add a tag. Job parameters it declares are accepted by `params`.

`run_finished(run, item)` runs after each `JobRun` is built, after the run's URL has been added to the test's `user_properties`.

## Logging and reports

The plugin logs to the `pytest_databricks_lakeflow` logger: INFO when it deploys, creates a job, starts and finishes a run, and deletes a job or drops a schema; WARNING when it keeps something or a cleanup call fails. Show the INFO lines with `pytest --log-cli-level=INFO`.

Each test's job URL and run URLs are appended to `item.user_properties` as `lakeflow_job_url` and `lakeflow_run_url`, so they appear in JUnit XML, and in a `lakeflow` section of the report for the `call` phase.
