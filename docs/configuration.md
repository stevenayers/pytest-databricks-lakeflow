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
| `timeout` | 30 minutes | The default `timedelta` to wait for one run. Each job is created with `timeout_seconds` equal to this timeout plus 60 seconds, so the platform stops a run that the plugin can no longer cancel, for example after the pytest process was killed. A template with a smaller `timeout_seconds` keeps it. A longer `timeout` passed to `run()` or `start()` lengthens the job's `timeout_seconds` before that run starts. |
| `poll_interval` | two seconds | The `timedelta` to wait between two polls of a run that has not finished. A longer interval makes fewer `get_run` calls; a shorter one notices a finished run sooner. Zero or less raises `ValueError`. |
| `max_concurrent_runs` | `None`, no cap | How many runs this process may have active at once, for a workspace with a tight serverless quota. When that many are active, `start()` and `run()` wait for one to end before calling `run_now`, polling the active runs every `poll_interval`, and raise `LakeflowError` naming them when none ends within the run's `timeout`. Every `Lakeflow` in the process shares the cap of the first run started under one. The cap is per xdist worker. `--lakeflow-max-concurrent-runs` overrides it. Less than 1 raises `ValueError`. See [Parametrise and parallelise](job-tests.md#parametrise-and-parallelise). |
| `catalog` | `None` | The catalog in which `schema()` creates temporary schemas. |
| `helpers_path` | `/Users/<you>/.pytest-databricks-lakeflow` | The workspace folder for the two helper notebooks. |
| `helpers_compute` | `None`, meaning serverless | A `job_cluster_key` from the job for stub, probe, setup and teardown tasks, for workspaces without serverless. |
| `helpers_environment_version` | `"4"` | The serverless environment version for stub, probe, setup and teardown tasks. |
| `disable_auto_optimization` | `True` | Set `disable_auto_optimization` on serverless code tasks, tasks nested in a `for_each_task` included, so that a failure fails once and fast, instead of being retried by the platform. |
| `performance_optimized` | `False` | Run every ephemeral job with `performance_target: PERFORMANCE_OPTIMIZED`, whatever the template says. When a run reports that it used `STANDARD` all the same, a workspace policy or the job's compute overrode it, and the plugin warns once. |
| `side_effects` | `"neutralise"` | `"neutralise"` pauses dashboard subscriptions and SQL alert subscriptions in every ephemeral job, so test runs do not email snapshots or notify subscribers. `"keep"` leaves them as the template has them. Alert tasks and Power BI tasks are left as they are in both modes; disable them with `disable=`. Any other value raises `ValueError`. |
| `environment_version` | `None` | The serverless environment version of every environment copied from the template, such as `"5"`. Environments whose spec sets `base_environment` keep it, and the helper environment `__pytest_serverless` keeps `helpers_environment_version`. `environments` overrides it for the environments it names, on `Lakeflow` for every copy or on a call for one, see [Change the settings before the job is created](job-tests.md#change-the-settings-before-the-job-is-created). |
| `budget_policy_id` | `None` | The budget policy of every ephemeral job and pipeline copy. A pipeline copy without it keeps the deployed pipeline's. |
| `existing_cluster_id` | `None` | A running cluster for every task that the template puts on a job cluster or a new cluster, stub, probe and nested `for_each_task` tasks included, so that tests skip cluster start-up. The job's `job_clusters` are dropped. Serverless and warehouse tasks keep their compute. |
| `single_node` | `False` | Make every job cluster and every task's `new_cluster`, nested `for_each_task` tasks and the `helpers_compute` cluster included, a single node cluster, so that runs start faster and cost one node: `num_workers` becomes 0, `autoscale` is removed, `spark.master` (`local[*, 4]`) and `spark.databricks.cluster.profile` (`singleNode`) are merged into `spark_conf` and `ResourceClass` (`SingleNode`) into `custom_tags`. Raises `LakeflowError` naming the cluster when one uses standard access mode (`USER_ISOLATION` or `DATA_SECURITY_MODE_STANDARD`), because single node compute needs dedicated access mode. Serverless tasks are left alone, and with `existing_cluster_id` there is no cluster left to change. |
| `allow_undeclared_params` | `False` | Allow job parameters that the job does not declare. |
| `test_marker` | `False` | Declare a `__pytest_test_id` job parameter on every ephemeral job, with the test node id as its default, so job code can tell that it runs under test. See [Writing job tests](job-tests.md#make-a-job). |
| `notebook_export` | `"failed"` | Which notebook tasks to read with `jobs.export_run` after a run, into `TaskRun.notebook`: `"failed"` exports each notebook task that ran and did not succeed, `"always"` every notebook task that ran, `"never"` none. Any other value raises `ValueError`. See [Read what a notebook printed](job-tests.md#read-what-a-notebook-printed). |
| `dbt_artifacts` | `False` | Download the artefacts tarball of each dbt task as soon as its run finishes, while the platform's 30 minute link is valid, and parse `run_results.json` and `manifest.json` into `TaskRun.dbt`. Each run gets a temporary directory, removed at the end of the session unless `keep` or `--lakeflow-keep` is set. A tarball over 50 MB, an expired link or a tarball without `run_results.json` leaves `TaskRun.dbt` `None` with a warning naming the task. See [dbt tasks](task-tests.md#dbt-tasks). |
| `sweep_hours` | `24` | The age after which `--lakeflow-sweep` and `Lakeflow.sweep()` delete leftover jobs and pipeline copies. |
| `keep` | `False` | Keep every ephemeral job, pipeline copy and temporary schema. |
| `keep_failed` | `True` | Keep the ephemeral job and pipeline copies when their test fails. |
| `strict_artifacts` | `False` | Raise `LakeflowError` from the first template lookup when the bundle has a wheel artifact without `dynamic_version: true`, or, with `sync="none"`, a local wheel built after its uploaded copy, instead of the `UserWarning` the plugin emits once per session for each. Later lookups fail with the same message. See [Limitations](limitations.md). |
| `permissions` | `None` | The `access_control_list` given to every ephemeral job when it is created, so that people other than its creator can open it. Each entry is a `jobs.JobAccessControlRequest` or a dict such as `{"group_name": "data-eng", "permission_level": "CAN_VIEW"}`. `IS_OWNER` raises `ValueError`: the creator owns every copy, so use `CAN_MANAGE`. See [Permissions](#permissions). |
| `run_as` | `None`, run as you | The default of `run_as=` on `job()` and `task()`: `"keep"`, a user, a service principal's application id or a `jobs.JobRunAs`. See [Run as another identity](job-tests.md#run-as-another-identity). |
| `queue` | `None`, the job's own setting | The default of `queue=` on `EphemeralJob.run()` and `start()`. |
| `disable` | `None` | The default of `disable=` on `job()`: task keys to disable in every copy. A key that a job does not have is skipped for that job, whereas `disable=` on a call refuses it. |
| `extra_dependencies` | `None` | The default of `extra_dependencies=` on `job()` and `task()`. See [Test-only dependencies](job-tests.md#test-only-dependencies). |
| `children` | `"warn"` | The default of `children=` on `job()` and `task()`, `"warn"` or `"refuse"`, such as `"refuse"` in CI. Any other value raises `ValueError`. See [Jobs that run other jobs](job-tests.md#jobs-that-run-other-jobs). |
| `setup`, `teardown` | `None` | The defaults of `setup=` and `teardown=` on `job()` and `task()`. A string, an empty list, a statement with a stray `$`, a path that is not a `.py` file or a function the plugin cannot ship raises `LakeflowError` when `Lakeflow` is made. See [Set up and tear down data](job-tests.md#set-up-and-tear-down-data). |
| `environments` | `None` | The default of `environments=` on `job()` and `task()`, such as `{"default": {"environment_version": "5"}}`. Each copy takes the entries for the environments its job has; a key the job does not have is skipped, whereas `environments=` on a call adds it. A key starting with `__pytest_`, an entry that is not a mapping or an unknown field raises `LakeflowError` when `Lakeflow` is made. |
| `saved_queries` | `"deployed"` | The default of `saved_queries=` on `job()` and `task()`, `"deployed"` or `"inline"`, which runs each saved query task as a SQL file task so that `sql_result` holds its rows. Any other value raises `ValueError`. See [Saved query rows](job-tests.md#saved-query-rows). |
| `modify_settings` | `None` | Called with each job's `JobSettings` and the test's `pytest.Item` just before the job is created. |
| `run_finished` | `None` | Called with each `JobRun` and the test after a run finishes, once per run however often `RunHandle.wait()` is called. |

### Defaults for per-call arguments

`run_as`, `queue`, `disable`, `extra_dependencies`, `children`, `setup`, `teardown`, `environments` and `saved_queries` hold the default of the argument of the same name on `lakeflow.job()` and `lakeflow.task()` (`queue` on `run()` and `start()`, `disable` on `job()` only). A call that leaves the argument out, or passes `None`, takes the `Lakeflow` value. Anything else replaces it whole, without merging: `disable=[]` disables nothing, `extra_dependencies=[]` adds none and `environments={}` leaves the template's environments as they are.

```python
@pytest.fixture(scope="session")
def lakeflow() -> Lakeflow:
    return Lakeflow(
        children="refuse",
        disable=["notify_slack"],
        extra_dependencies=["chispa"],
        environments={"default": {"environment_version": "5"}},
    )


def test_notifies(lakeflow):
    with lakeflow.job("my_job", disable=[]) as job:  # [] replaces the default
        assert job.run().succeeded
```

`disable` and `environments` on `Lakeflow` skip a task key or environment key that a job does not have, so one default can serve every job of the bundle. On a call, `disable=` refuses an unknown task and `environments=` adds a new environment.

Once a default is set, a call cannot opt out of `setup`, `teardown` or `run_as`: `None` takes the `Lakeflow` value, and `setup=[]` or `teardown=[]` raises `LakeflowError` like any empty list of statements. For tests that need no setup or teardown, derive a `Lakeflow` without it in their directory's `conftest.py`, such as `dataclasses.replace(lakeflow, setup=None)`, as shown below. To run one copy as you while `Lakeflow` sets another `run_as`, name yourself: pass your user name, or a `jobs.JobRunAs(user_name=...)`.

`Lakeflow` is a dataclass, so a `conftest.py` in a subdirectory can derive its own from the parent's:

```python
import dataclasses

import pytest

from pytest_databricks_lakeflow import Lakeflow


@pytest.fixture(scope="session")
def lakeflow(lakeflow: Lakeflow) -> Lakeflow:
    return dataclasses.replace(lakeflow, catalog="team_b", children="refuse")
```

The override applies to the tests below that directory only. Every `Lakeflow` that names the same bundle target shares one deploy. A second target, such as `Lakeflow(target="staging")` in another fixture, is deployed once too.

## Command-line flags

These apply to one pytest run and win over the matching arguments.

| Flag | Meaning |
| --- | --- |
| `--lakeflow-keep` | Keep every ephemeral job, pipeline copy and temporary schema. |
| `--lakeflow-keep-failed` | Keep the ephemeral job and pipeline copies when their test fails, even with `Lakeflow(keep_failed=False)`. |
| `--no-lakeflow-keep-failed` | Delete the ephemeral job and pipeline copies even when their test fails. |
| `--lakeflow-keep-ttl=48h` | Tag every ephemeral job and pipeline copy with `pytest_expires`, an ISO 8601 UTC time this far ahead, in whole hours (`h`) or days (`d`), and give every temporary schema the same `pytest_expires` property. The sweep keeps a tagged job, pipeline or schema until then, whatever its age, with the notebooks such a job uses, and deletes it once that time has passed. |
| `--lakeflow-sweep` | At session start, delete your own jobs tagged `pytest`, and your own `[pytest]` pipeline copies, from other runs that are older than `sweep_hours`. |
| `--lakeflow-sweep-scope=all` | Sweep jobs, pipeline copies and schemas whoever created them, not only your own, and with `--lakeflow-sweep-notebooks` every user's default helpers folder. The default is `mine`. |
| `--lakeflow-sweep-schemas=<catalog>` | Also drop the temporary `pytest_<run id>_<8 hex digits>` schemas of other runs in that catalog that are older than `sweep_hours`. |
| `--lakeflow-sweep-notebooks` | Also delete the `setup_<hash>`, `teardown_<hash>` and debug notebooks and the inlined saved query files in the helpers folder that were last uploaded more than `sweep_hours` ago and that no job the sweep keeps uses. |
| `--lakeflow-sweep-branch=<branch>` | Sweep only jobs, pipeline copies and schemas whose `pytest_git_branch` tag or property is that branch, and only the notebooks that those jobs alone use. |
| `--lakeflow-sweep-dry-run` | Delete nothing, and list what the sweep would delete. |
| `--lakeflow-sweep-report=<path>` | Write what the sweep deleted, or would delete, to a JSON file, relative to the directory pytest was started in. |
| `--lakeflow-destroy` | Run `databricks bundle destroy` at the end of the session for each target that the session deployed. |
| `--lakeflow-debug-notebook` | When a test fails, write a notebook for each notebook task that failed in the last run of each of its jobs, which sets the task's resolved parameters as widgets and runs the task's notebook. See [Reproduce a failed notebook task](job-tests.md#reproduce-a-failed-notebook-task). |
| `--lakeflow-report-dir=DIR` | Write the runs of each test to `DIR`, relative to the directory pytest was invoked from: one `report.json` per test, with raw logs and exported notebooks as files. See [Report directory](#report-directory). |
| `--lakeflow-live` | Print each task state change to the terminal while a run waits, with the start and end of the run, without `--log-cli-level` or `-s`. `run(stream=...)` overrides it for one run. Under xdist the lines are only logged. See [logging and reports](#logging-and-reports). |
| `--lakeflow-max-concurrent-runs=N` | Start a run only while fewer than `N` runs started by this process are active, and otherwise wait for one to end, as `Lakeflow(max_concurrent_runs=N)` does. The cap is per xdist worker, so `-n 4` with `N=2` can have eight runs active. Less than 1 is a usage error. |
| `--lakeflow-check` | Collect one test per job of each `databricks.yml` that pytest collects, named `databricks.yml::<key>`, which calls `lakeflow.check(key, rules=DEFAULT_RULES - {"trigger"})`, every default rule but `trigger`, and fails with the problems it finds, or with the `LakeflowBundleError` of a job that cannot be used as a template. The bundle is set up while collecting, with the `lakeflow` fixture that the `databricks.yml` sees. See [Check a job without running it](job-tests.md#check-a-job-without-running-it). |

### Sweeping leftovers

`--lakeflow-sweep` runs once per session, in the process that sets up the first bundle target (under xdist, the first worker's), with that target's workspace and helpers folder, and every other `--lakeflow-sweep-*` flag needs it. A bundle or target that the session sets up later is not swept: when it has another helpers folder, because it sets another `helpers_path` or runs as another profile's user, its notebooks are left, and so is everything in a workspace other than the first target's. Sweep those from a session that sets that target up first, or with [`Lakeflow.sweep()`](#sweep-from-a-script) on a `Lakeflow` with that profile and `helpers_path`. It only ever deletes what the plugin makes, recognised by name and tags:

- Jobs tagged `pytest` and `pytest_run_id`, from a run other than this one, created more than `sweep_hours` ago. A job tagged `pytest_expires` is deleted once that time has passed instead, whatever its age. With the default scope only jobs you created are considered; `--lakeflow-sweep-scope=all` considers everyone's, and a delete you are not allowed to make is recorded as an error.
- Pipelines named `[pytest] ...` and tagged `pytest` and `pytest_run_id`, from a run other than this one, last changed more than `sweep_hours` ago, or past their `pytest_expires` time, by you unless the scope is `all`, and tagged with the branch under `--lakeflow-sweep-branch`. The Pipelines API lists pipelines without their tags or creation time, so the sweep lists those whose name starts with `[pytest] ` and reads each one, and a pipeline's age is the time since it last changed. A copy is deleted with its tables, as at teardown.
- With `--lakeflow-sweep-schemas=<catalog>`, schemas named `pytest_<run id>_<8 hex digits>`, from another run, created more than `sweep_hours` ago or past their `pytest_expires` time, by you unless the scope is `all`, and with the branch under `--lakeflow-sweep-branch`. Schema tags need `APPLY TAG` and a SQL warehouse, so `lakeflow.schema()` writes `pytest_expires` (with `--lakeflow-keep-ttl`) and `pytest_git_branch` (when the branch is known) as schema properties, which the Unity Catalog API returns unchanged. A schema without them, such as one made by an earlier version of the plugin, is judged by its age, and a branch sweep leaves it.
- With `--lakeflow-sweep-notebooks`, notebooks in the helpers folder named exactly `setup_` or `teardown_` followed by 12 hexadecimal digits, debug notebooks in its `debug` folder named `<job key>__<task key>__<run id>`, and workspace files in the helpers folder named `query_<run id>_<task key>_<8 hex digits>.sql`, which [`saved_queries="inline"`](job-tests.md#saved-query-rows) uploads, last uploaded more than `sweep_hours` ago. Every session that uses a setup or teardown function uploads its notebook again, so a notebook in use stays young. `set_values`, `get_values` and any other name are never deleted. A notebook is deleted only when no job that the sweep keeps uses it, whoever created that job: a setup or teardown notebook or a query file is used by every job with a task that runs it, and a debug notebook by the job of the run it replays, while the Jobs list still lists that job. So a job kept with `--lakeflow-keep-ttl` keeps its notebooks until its `pytest_expires` time, and a young job of a crashed session keeps them until the sweep deletes the job. When the jobs cannot be listed, no notebook is deleted.
- With `--lakeflow-sweep-scope=all`, the notebooks of every user's default helpers folder, `/Users/<user>/.pytest-databricks-lakeflow`, are swept too. That lists one folder per user under `/Users`, in parallel, so dry-run it first on a large workspace, or sweep each principal with the default scope instead. A user without a helpers folder is skipped; a folder that cannot be listed is recorded as an error. A custom `helpers_path` of another session cannot be found, so only your own is swept.
- With `--lakeflow-sweep-branch`, a notebook is deleted only when at least one job uses it and the sweep deletes every job that does. A notebook that no job uses any more, or that a job of another branch shares, is left to an unfiltered sweep.

A failed delete or listing is logged at WARNING and the sweep carries on. Any other error, such as a `--lakeflow-sweep-report` path that cannot be written, ends the sweep with a `UserWarning` (`Lakeflow sweep failed: ...`) in the warnings summary, raised once, by the process that swept. The sweep only tidies up, so no test fails because of it, and under xdist the other workers carry on as if it had succeeded. To see first what a sweep would delete, run it with `--lakeflow-sweep-dry-run`: nothing is deleted, and the terminal summary lists each job, pipeline, schema and notebook it would delete, then any error:

```text
=================================== lakeflow ===================================
lakeflow: sweep dry run deleted nothing; it would delete:
  job https://adb-123.azuredatabricks.net/jobs/1001
  pipeline https://adb-123.azuredatabricks.net/pipelines/0b9e2c1e-5d4f-4c3a-9b8e-7f6a5d4c3b2a
  schema dev_sandbox.pytest_ab12cd34_9f0e1d2c
  notebook /Users/someone@example.com/.pytest-databricks-lakeflow/setup_3f9a1c07b2d4
```

`--lakeflow-sweep-report=sweep.json` writes the same as JSON, for a dry run or a real sweep:

```json
{
  "version": 1,
  "dry_run": true,
  "jobs": [1001],
  "pipelines": ["0b9e2c1e-5d4f-4c3a-9b8e-7f6a5d4c3b2a"],
  "schemas": ["dev_sandbox.pytest_ab12cd34_9f0e1d2c"],
  "notebooks": ["/Users/someone@example.com/.pytest-databricks-lakeflow/setup_3f9a1c07b2d4"],
  "errors": []
}
```

### Sweep from a script

`Lakeflow.sweep()` runs the same sweep from plain Python, without a pytest session, a bundle or a test, so a nightly job does not need a smoke test that deploys the bundle. It uses the instance's workspace client, `sweep_hours` and `helpers_path`, or your default helpers folder, and returns a `SweepReport`. It spares no run, so every job older than `sweep_hours` goes, unless its `pytest_expires` time is still ahead:

```python
from pytest_databricks_lakeflow import Lakeflow

report = Lakeflow(profile="ci").sweep(scope="all", schemas_catalog="dev_sandbox", notebooks=True, dry_run=True)
print(report.jobs, report.schemas, report.notebooks, report.errors)
```

It takes `scope`, `schemas_catalog`, `notebooks`, `branch`, `dry_run` and `report` (a path for the JSON report), which mean what the matching `--lakeflow-sweep-*` flags do.

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

The CLI takes the target's host from `databricks.yml`, while the SDK client takes it from `profile` or the `DATABRICKS_*` variables. At setup the plugin compares the `workspace.host` that `bundle validate` reports with the client's host, before `bundle deploy` and before the `run_as` check with `sync="deploy"` (and again with the `bundle summary` after the deploy, for a validate output without a host), ignoring scheme, trailing slash and case, and fails the session's tests with a `LakeflowError` naming both hosts when they differ. A target without a host is not checked. To give pytest the same credentials as the bundle target, run it through the CLI, which sets the target's `DATABRICKS_*` variables for the command:

```bash
databricks bundle run -t test -- uv run pytest
```

## Sync strategies

- `deploy`, the default, runs `databricks bundle validate -t <target> -o json` to [check the target's `run_as`](how-it-works.md#session-setup), then `databricks bundle deploy -t <target> --auto-approve`, followed by any `deploy_flags`, once per session and reads the templates from the deployed jobs with the Jobs API. This is the only strategy that builds and uploads wheels, and the only one in which every path is already a resolved workspace path.
- `sync` runs `databricks bundle sync -t <target>` and reads the templates from `databricks bundle validate -o json`. Jobs that use local wheels or JARs are refused, because `sync` does not upload them.
- `none` runs no CLI command apart from `validate`. Local wheel and JAR paths are pointed at the copies that an earlier `deploy` uploaded under the target's artifact path, after checking that they exist. When a local file is newer than its uploaded copy by more than 60 seconds, so it was most likely built after the last deploy, the first template lookup emits one `UserWarning` per session naming it, such as `` dist/pkg-0.1-py3-none-any.whl was built after its uploaded copy. Run `databricks bundle deploy -t test`. ``, and `strict_artifacts=True` raises `LakeflowError` instead. See [Limitations](limitations.md) for what the comparison cannot detect.

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

The entries go to `jobs.create` unchanged, so they take `user_name`, `group_name` or `service_principal_name` and a `permission_level` such as `CAN_VIEW`, `CAN_MANAGE_RUN` or `CAN_MANAGE`. `IS_OWNER` is refused when `Lakeflow` is created, with a `ValueError`, because the identity that creates a job owns it and a job has one owner; grant `CAN_MANAGE` instead. Errors from the API, for example for an unknown group, fail the test as they are. To choose who a job runs as, pass `run_as` to [`lakeflow.job()`](job-tests.md#make-a-job) or `lakeflow.task()`.

## Logging and reports

The plugin logs to the `pytest_databricks_lakeflow` logger: INFO when it deploys, creates a job, starts and finishes a run, and deletes a job or drops a schema; WARNING when it keeps something or a cleanup call fails. Show the INFO lines with `pytest --log-cli-level=INFO`.

While `run()` or `RunHandle.wait()` waits, every poll logs a line at INFO for each task run whose state changed, the plugin's stub, probe, setup and teardown tasks included. A task run seen for the first time logs its state on its own; a later change logs the old and new state. The state is the V2 lifecycle state, such as `PENDING`, `QUEUED` or `RUNNING`, until the task run terminates, and its result state after that. A terminated task run adds its duration and the first line of its error, if it reported one:

```text
extract: PENDING
transform: PENDING
extract: PENDING -> SUCCESS in 0:00:31
transform: PENDING -> RUNNING
transform: RUNNING -> FAILED in 0:00:42 ValueError: no rows
```

A retried task run is labelled with its attempt, such as `transform (attempt 1): PENDING`. A task that has already terminated when it is first seen logs one line with its final state. The output of each task run is read once, as soon as it has terminated, which supplies the error for the line and is reused when the `JobRun` is built.

To watch a run without turning on INFO for every logger, pass `--lakeflow-live`, or `stream=True` to one `job.run()`. The plugin then also prints the start of each run, each of these lines and the end of the run straight to the terminal, prefixed `lakeflow: `, while the test is still running, past pytest's output capture as its own live logging does, so neither `--log-cli-level` nor `-s` is needed. `job.run(stream=False)` keeps one run quiet under `--lakeflow-live`, and a run from `start()` streams when `--lakeflow-live` is set. The log lines and the report section are the same either way, so with `--log-cli-level=INFO` as well each line shows twice. Under xdist (`-n`) a worker cannot write to the controller's terminal, so `--lakeflow-live` and `stream=True` print nothing there and the lines are only logged and kept in the report section. The start line leaves out the run page when the workspace has not reported it yet.

```text
lakeflow: Started run 77 of job 1001: https://<workspace host>/jobs/runs/77
lakeflow: transform: PENDING -> RUNNING
lakeflow: transform: RUNNING -> FAILED in 0:00:07 ValueError: no rows
lakeflow: Run 77 finished FAILED in 0:00:51: https://<workspace host>/jobs/runs/77
```

Each test's job and run coordinates are appended to `item.user_properties`, so they appear as JUnit XML properties:

| Property | Added | Value |
| --- | --- | --- |
| `lakeflow_job_url` | when the job is created | the job's page in the workspace |
| `lakeflow_job_id` | when the job is created | the job's id |
| `lakeflow_run_url` | when a run starts, also with `start()` and no `wait()` | the run's page in the workspace |
| `lakeflow_run_id` | when a run finishes | the run's id |
| `lakeflow_result_state` | when a run finishes | the run's result state, such as `SUCCESS` or `FAILED` |
| `lakeflow_duration` | when a run finishes | the run's duration in seconds, with three decimals |
| `lakeflow_failing` | when a run fails | each failing task as `key: error`, with the error's first line, joined by `; ` |
| `lakeflow_params` | when a run fails and has job parameters | the run's job parameters as one line of JSON with sorted keys, such as `{"date": "2026-10-01"}` |
| `lakeflow_task_params` | when a run fails and a failing task reported resolved parameters | one JSON object of each such failing task's parameters, by task key |
| `lakeflow_debug_notebook` | when `debug_notebook()` or `--lakeflow-debug-notebook` writes a notebook | the notebook's page in the workspace |

A job that runs more than once adds a set of run properties for each run. `lakeflow_failing`, `lakeflow_params` and `lakeflow_task_params` are added for failed runs only, also when the test expects the failure. Each parameter value is cut to its first line, at most 200 characters, and the parameters the plugin injects are left out.

The same lines are attached as a `lakeflow` section to the setup, call and teardown reports of the test, so a job created in a fixture whose setup fails still shows its URL in the error. The task state lines of every run follow the property lines in that section. They are recorded as they are logged, so a run that raises `LakeflowTimeout` still shows what its tasks did before the deadline. The section is shown for a failed test, and for every test with `-rA`. A test that fails in both call and teardown shows the section twice. In the section of a failed report, each failing task with an exported notebook adds the first code line of the cell that failed and the last 50 lines of the notebook's `stdout`, see [Read what a notebook printed](job-tests.md#read-what-a-notebook-printed).

At the end of the session, a `lakeflow` block in the terminal summary lists every kept job with its URL, every kept schema, every debug notebook written with the test and task it belongs to, and the number of runs cancelled at session end, followed by a reminder of `--lakeflow-sweep` when jobs were kept:

```text
=================================== lakeflow ===================================
lakeflow: 2 jobs kept
  https://adb-123.azuredatabricks.net/jobs/1001
  https://adb-123.azuredatabricks.net/jobs/1002
lakeflow: 1 schema kept
  dev_sandbox.pytest_ab12cd34_9f0e1d2c
lakeflow: 1 debug notebook written
  https://adb-123.azuredatabricks.net/#notebook/4096 (tests/test_jobs.py::test_transform, task 'transform')
lakeflow: 1 run cancelled at session end
hint: pytest --lakeflow-sweep removes kept jobs older than sweep_hours
```

A debug notebook whose parameters did not all fit a text widget adds them to its line, such as `(tests/test_jobs.py::test_transform, task 'transform'; widgets cut at 2048 bytes: 'payload')`. Naming the test matters for a job that a fixture's finaliser closes: its `lakeflow_debug_notebook` property then lands on the teardown report, which pytest shows only with `-rA`.

It does not depend on `--log-cli-level`. Under xdist the workers report what they kept to the controller, which prints one summary. A session that kept nothing, wrote no debug notebook, cancelled nothing and ran no sweep dry run prints no block. A sweep dry run adds its list at the top of the block, see [sweeping leftovers](#sweeping-leftovers).

### Report directory

`pytest --lakeflow-report-dir=build/lakeflow` keeps a record of every run that outlives the job, to attach to a CI build as an artefact. The path is resolved against the directory pytest was invoked from, as `--junitxml` is. Each test that waited for at least one run, or whose run the plugin cancelled, gets a directory of its own, named after its node id with every character outside letters, digits, `.`, `_` and `-` replaced by `_`, cut to 80 characters, plus `-` and the first 8 hex characters of the node id's SHA-256:

```text
build/lakeflow/
  test_jobs.py__test_job-3f9a2c1b/
    report.json            # the test's outcome and one entry per run
    77/load.log            # raw logs, by run id and task key
    77/transform.html      # an exported notebook, which opens in a browser
```

`report.json` holds the test's `nodeid`, its `outcome` per phase, such as `{"setup": "passed", "call": "failed", "teardown": "passed"}`, `debug_notebooks`, the `task_key`, `run_id`, `path` and `url` of each [debug notebook](job-tests.md#reproduce-a-failed-notebook-task) the test wrote (empty when it wrote none), and `runs`, one entry per `JobRun` the test waited for, in the order they finished, and one for each run the plugin cancelled: a run that hit its `timeout` and raised `LakeflowTimeout`, and a run from `start()` still active when its job closed. After the cancel the plugin polls such a run until it has ended, which takes a few seconds, for at most 60 seconds, and lists it with `result_state` `"CANCELED"` and `succeeded` false. It is not appended to `job.runs` nor passed to `run_finished`. A run that has not ended after 60 seconds logs a warning and is left out. Without the flag the plugin does not poll a run after cancelling it. Runs made in a function-scoped fixture are included. A run made in a module- or session-scoped fixture belongs to the test that was running when the fixture started it: the first test to use the fixture for a run made while setting it up, and the last test of the module or session, whether or not it uses the fixture, for one made while tearing it down. The other tests that share the fixture do not list it. A run the plugin cancels when its job closes is listed by the test running then, whose report is still to be written: for a job that a module- or session-scoped fixture closes, the last test of the module or session, which is being torn down. The job is deleted even when a second Ctrl-C stops the polling. Each entry is [`serialise(run)`](api.md#serialise) with three more keys: `job_url`, `params` (the job's `params`) and `settings` (the copy's `JobSettings` as the Jobs API's JSON). A dbt task's `dbt_output` keeps its `artifacts_link` and leaves out `artifacts_headers`, which can carry the signature of the link, and its `dbt` keeps `artifacts_dir`, `results`, `failed_models` and `all_passed` and leaves out `run_results` and `manifest`, which stay in `artifacts_dir`.

`serialise` keeps every field of the run and its tasks. Durations are in seconds, as `duration_seconds` and a `timing` of `queue_seconds`, `setup_seconds`, `execution_seconds` and `cleanup_seconds`; `error` is a dict of `type`, `message`, `trace`, `root_cause` and `code`; `missing_task_value_reasons` says why each of `missing_task_values` is missing. Logs, up to 5 MB per task, and the exported notebook HTML are written as files instead, named in `logs_file` and `notebook_file` relative to the test's directory, or `None` (`null` in the file) when the task has none. An earlier attempt of a retried task gets `.attempt-<n>` before the suffix, such as `77/load.attempt-0.log`, an iteration of a for-each task gets `.iteration-<n>`, counting from 0, such as `77/loop.iteration-1.log`, and the tasks of a `child_run` are under the child run's id. A for-each task's `iterations` is a list of its iterations, each serialised as a task, and its `for_each_stats` a dict of `total_iterations`, `succeeded_iterations`, `failed_iterations` and `errors`, a list of dicts of `message`, `count` and `termination_category`, or `None`.

```python
from pytest_databricks_lakeflow import Lakeflow
from pytest_databricks_lakeflow.results import serialise


def test_rejects_forbidden_input(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job", params={"param1": "forbidden", "param2": "world"}) as job:
        run = job.run()
    data = serialise(run)
    assert data["tasks"]["transform"]["error"]["type"] == "PermissionError"
    assert data["tasks"]["transform"]["notebook_file"] == f"{run.run_id}/transform.html"
```

Only the notebooks that `notebook_export` selects are exported, by default those of notebook tasks that ran and did not succeed; pass `Lakeflow(notebook_export="always")` to keep every notebook. A test without a run writes nothing, and without the flag nothing is written. A report that cannot be serialised or written logs a warning and never fails the test nor ends the session. At the start of each session, before any test runs (on the xdist controller, before the workers start), the plugin deletes the test directories an earlier session left in the directory, so a rerun never mixes stale logs, notebooks or a `report.json` with its own, even for a test that no longer runs. Only directories named as above that hold a `report.json` are deleted; any other file or directory in it is left alone.
