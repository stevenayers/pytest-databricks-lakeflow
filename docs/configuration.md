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
| `timeout` | 30 minutes | The default `timedelta` to wait for one run. Each job is created with `timeout_seconds` equal to this timeout plus 60 seconds, so the platform stops a run that the plugin can no longer cancel, for example after the pytest process was killed. A template with a smaller `timeout_seconds` keeps it. |
| `catalog` | `None` | The catalog in which `schema()` creates temporary schemas. |
| `helpers_path` | `/Users/<you>/.pytest-databricks-lakeflow` | The workspace folder for the two helper notebooks. |
| `helpers_compute` | `None`, meaning serverless | A `job_cluster_key` from the job for stub and probe tasks, for workspaces without serverless. |
| `helpers_environment_version` | `"4"` | The serverless environment version for stub and probe tasks. |
| `disable_auto_optimization` | `True` | Set `disable_auto_optimization` on serverless code tasks so that a failure fails once and fast, instead of being retried by the platform. |
| `performance_optimized` | `False` | Run every ephemeral job with `performance_target: PERFORMANCE_OPTIMIZED`, whatever the template says. |
| `side_effects` | `"neutralise"` | `"neutralise"` pauses dashboard subscriptions and SQL alert subscriptions in every ephemeral job, so test runs do not email snapshots or notify subscribers. `"keep"` leaves them as the template has them. Any other value raises `ValueError`. |
| `environment_version` | `None` | The serverless environment version of every environment copied from the template, such as `"5"`. Environments whose spec sets `base_environment` keep it, and the helper environment `__pytest_serverless` keeps `helpers_environment_version`. |
| `budget_policy_id` | `None` | The budget policy of every ephemeral job. |
| `existing_cluster_id` | `None` | A running cluster for every task that the template puts on a job cluster or a new cluster, stub, probe and nested `for_each_task` tasks included, so that tests skip cluster start-up. The job's `job_clusters` are dropped. Serverless and warehouse tasks keep their compute. |
| `allow_undeclared_params` | `False` | Allow job parameters that the job does not declare. |
| `sweep_hours` | `24` | The age after which `--lakeflow-sweep` deletes leftover jobs. |
| `keep` | `False` | Keep every ephemeral job and temporary schema. |
| `keep_failed` | `True` | Keep the ephemeral job when its test fails. |
| `permissions` | `None` | The `access_control_list` given to every ephemeral job when it is created, so that people other than its creator can open it. Each entry is a `jobs.JobAccessControlRequest` or a dict such as `{"group_name": "data-eng", "permission_level": "CAN_VIEW"}`. See [Permissions](#permissions). |
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
| `GITHUB_SERVER_URL`, `GITHUB_REPOSITORY`, `GITHUB_RUN_ID`, `GITHUB_SHA`, `GITHUB_HEAD_REF`, `GITHUB_REF_NAME`, `GITHUB_ACTOR` | On GitHub Actions, fill the `pytest_ci_url`, `pytest_git_sha`, `pytest_git_branch` and `pytest_user` tags of every job. `GITHUB_HEAD_REF` wins over `GITHUB_REF_NAME` for the branch. |
| `SYSTEM_TEAMFOUNDATIONCOLLECTIONURI`, `SYSTEM_TEAMPROJECT`, `BUILD_BUILDID`, `BUILD_SOURCEVERSION`, `BUILD_SOURCEBRANCHNAME`, `BUILD_REQUESTEDFOREMAIL` | The same on Azure DevOps, when the GitHub variables are not set. Without either, the commit and branch come from `git` in the rootdir and the user from the workspace. |
| `DATABRICKS_CONFIG_PROFILE`, `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET` and the rest of the SDK's resolution | Credentials, when `profile` is not set. The CLI reads the same variables. |

## Sync strategies

- `deploy`, the default, runs `databricks bundle deploy -t <target> --auto-approve` once per session and reads the templates from the deployed jobs with the Jobs API. This is the only strategy that builds and uploads wheels, and the only one in which every path is already a resolved workspace path.
- `sync` runs `databricks bundle sync -t <target>` and reads the templates from `databricks bundle validate -o json`. Jobs that use local wheels or JARs are refused, because `sync` does not upload them.
- `none` runs no CLI command apart from `validate`. Local wheel and JAR paths are pointed at the copies that an earlier `deploy` uploaded under the target's artifact path, after checking that they exist.

With `sync` and `none`, a job that still contains `${...}` references after `validate`, such as `${resources.jobs.other.id}`, is refused, because `validate` does not resolve them. Use these strategies when you deploy the target yourself, for example once per CI pipeline, and want test sessions to start without a deploy.

## Callbacks

`modify_settings(settings, item)` runs after the plugin has sanitised the copied settings and before `jobs.create`. Change `settings` in place, for example to shrink a cluster, swap an environment or add a tag. Job parameters it declares are accepted by `params`.

`access_control_list` cannot be set from `modify_settings`: it is an argument of `jobs.create`, not a `JobSettings` field. Use `permissions` instead.

`run_finished(run, item)` runs after each `JobRun` is built, after the run's URL has been added to the test's `user_properties`.

## Permissions

Only the identity that creates a job, and workspace admins, can open it. In CI that identity is usually a service principal, so the engineer reading the log cannot open a kept job's URL. `permissions` grants access on every ephemeral job when it is created:

```python
import pytest

from pytest_databricks_lakeflow import Lakeflow


@pytest.fixture(scope="session")
def lakeflow() -> Lakeflow:
    return Lakeflow(permissions=[{"group_name": "data-eng", "permission_level": "CAN_VIEW"}])
```

The entries go to `jobs.create` unchanged, so they take `user_name`, `group_name` or `service_principal_name` and a `permission_level` such as `CAN_VIEW`, `CAN_MANAGE_RUN` or `CAN_MANAGE`. Errors from the API, for example for an unknown group or `IS_OWNER`, fail the test as they are. To choose who a job runs as, pass `run_as` to [`lakeflow.job()`](job-tests.md#make-a-job) or `lakeflow.task()`.

## Logging and reports

The plugin logs to the `pytest_databricks_lakeflow` logger: INFO when it deploys, creates a job, starts and finishes a run, and deletes a job or drops a schema; WARNING when it keeps something or a cleanup call fails. Show the INFO lines with `pytest --log-cli-level=INFO`.

Each test's job and run coordinates are appended to `item.user_properties`, so they appear as JUnit XML properties:

| Property | Added | Value |
| --- | --- | --- |
| `lakeflow_job_url` | when the job is created | the job's page in the workspace |
| `lakeflow_job_id` | when the job is created | the job's id |
| `lakeflow_run_url` | when a run starts | the run's page in the workspace |
| `lakeflow_run_id` | when a run finishes | the run's id |
| `lakeflow_result_state` | when a run finishes | the run's result state, such as `SUCCESS` or `FAILED` |
| `lakeflow_duration` | when a run finishes | the run's duration in seconds, with three decimals |
| `lakeflow_failing` | when a run fails | each failing task as `key: error`, with the error's first line, joined by `; ` |

A job that runs more than once adds a set of run properties for each run. `lakeflow_failing` is added whenever the run failed, also when the test expects the failure.

The same lines are attached as a `lakeflow` section to the setup, call and teardown reports of the test, so a job created in a fixture whose setup fails still shows its URL in the error. A test that fails in both call and teardown shows the section twice.

At the end of the session, a `lakeflow` block in the terminal summary lists every kept job with its URL, every kept schema, and the number of runs cancelled at session end, followed by a reminder of `--lakeflow-sweep` when jobs were kept:

```text
=================================== lakeflow ===================================
lakeflow: 2 jobs kept
  https://adb-123.azuredatabricks.net/jobs/1001
  https://adb-123.azuredatabricks.net/jobs/1002
lakeflow: 1 schema kept
  dev_sandbox.pytest_ab12cd34_9f0e1d2c
lakeflow: 1 run cancelled at session end
hint: pytest --lakeflow-sweep removes kept jobs older than sweep_hours
```

It does not depend on `--log-cli-level`. Under xdist the workers report what they kept to the controller, which prints one summary. A session that kept nothing and cancelled nothing prints no block.
