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
| `variables` | `None` | Bundle variables as a mapping of name to value, passed as `--var name=value`, sorted by name, to every `bundle deploy`, `summary`, `validate`, `sync` and `destroy` call. Two `Lakeflow` instances for the same target with different variables are set up, and deployed, separately. See [Bundle targets and variables](#bundle-targets-and-variables). |
| `deploy_flags` | `()` | Extra flags for `databricks bundle deploy`, added at the end of the command, such as `["--fail-on-active-runs"]`. Used only when `sync` is `deploy`. `Lakeflow` instances with different flags deploy separately. `-t`, `--target`, `-e`, `--environment`, `-p`, `--profile`, `--var` and `--auto-approve` raise `ValueError`: use `target`, `profile` or `variables` instead. See [isolating test deployments](how-it-works.md#isolating-test-deployments). |
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
| `single_node` | `False` | Make every job cluster and every task's `new_cluster`, nested `for_each_task` tasks and the `helpers_compute` cluster included, a single node cluster, so that runs start faster and cost one node: `num_workers` becomes 0, `autoscale` is removed, `spark.master` (`local[*, 4]`) and `spark.databricks.cluster.profile` (`singleNode`) are merged into `spark_conf` and `ResourceClass` (`SingleNode`) into `custom_tags`. Raises `LakeflowError` naming the cluster when one uses standard access mode (`USER_ISOLATION` or `DATA_SECURITY_MODE_STANDARD`), because single node compute needs dedicated access mode. Serverless tasks are left alone, and with `existing_cluster_id` there is no cluster left to change. |
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
| `BUNDLE_VAR_<name>` | Read by the CLI, not by the plugin, as the value of the bundle variable `<name>`. The plugin's CLI calls inherit the environment, so these work, but they are not part of the setup cache key: two `Lakeflow` instances for one target share one deploy whatever these say. Use `variables` when instances need different values. |

## Bundle targets and variables

The plugin always passes `-t <target>` to the CLI. It does not read `DATABRICKS_BUNDLE_TARGET` or any other variable for the target, so choose it in the fixture:

```python
import os

import pytest

from pytest_databricks_lakeflow import Lakeflow


@pytest.fixture(scope="session")
def lakeflow() -> Lakeflow:
    return Lakeflow(
        target=os.environ.get("LAKEFLOW_TARGET", "test"),
        variables={"catalog": "dev_sandbox"},
    )
```

The CLI takes the target's host from `databricks.yml`, while the SDK client takes it from `profile` or the `DATABRICKS_*` variables. At setup the plugin compares the `workspace.host` that `bundle summary` (with `sync="deploy"`) or `bundle validate` (with `sync` and `none`) reports with the client's host, ignoring scheme, trailing slash and case, and fails the session's tests with a `LakeflowError` naming both hosts when they differ. A target without a host is not checked. To give pytest the same credentials as the bundle target, run it through the CLI, which sets the target's `DATABRICKS_*` variables for the command:

```bash
databricks bundle run -t test -- uv run pytest
```

## Sync strategies

- `deploy`, the default, runs `databricks bundle deploy -t <target> --auto-approve`, followed by any `deploy_flags`, once per session and reads the templates from the deployed jobs with the Jobs API. This is the only strategy that builds and uploads wheels, and the only one in which every path is already a resolved workspace path.
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

While `run()` waits, every poll logs a line at INFO for each task run whose state changed, the plugin's stub and probe tasks included. A task run seen for the first time logs its state on its own; a later change logs the old and new state. The state is the V2 lifecycle state, such as `PENDING`, `QUEUED` or `RUNNING`, until the task run terminates, and its result state after that. A terminated task run adds its duration and the first line of its error, if it reported one:

```text
extract: PENDING
transform: PENDING
extract: PENDING -> SUCCESS in 0:00:31
transform: PENDING -> RUNNING
transform: RUNNING -> FAILED in 0:00:42 ValueError: no rows
```

A retried task run is labelled with its attempt, such as `transform (attempt 1): PENDING`. A task that has already terminated when it is first seen logs one line with its final state. The output of each task run is read once, as soon as it has terminated, which supplies the error for the line and is reused when the `JobRun` is built.

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
| `lakeflow_params` | when a run fails and has job parameters | the run's job parameters as one line of JSON with sorted keys, such as `{"date": "2026-10-01"}` |
| `lakeflow_task_params` | when a run fails and a failing task reported resolved parameters | one JSON object of each such failing task's parameters, by task key |

A job that runs more than once adds a set of run properties for each run. `lakeflow_failing`, `lakeflow_params` and `lakeflow_task_params` are added for failed runs only, also when the test expects the failure. Each parameter value is cut to its first line, at most 200 characters, and the parameters the plugin injects are left out.

The same lines are attached as a `lakeflow` section to the setup, call and teardown reports of the test, so a job created in a fixture whose setup fails still shows its URL in the error. The task state lines of every run follow the property lines in that section. They are recorded as they are logged, so a run that raises `LakeflowTimeout` still shows what its tasks did before the deadline. The section is shown for a failed test, and for every test with `-rA`. A test that fails in both call and teardown shows the section twice.

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
