# Lakeflow arguments

Every argument of `Lakeflow`, the object that the `lakeflow` fixture returns. Override the fixture in a `conftest.py` and pass keyword arguments; all of them are keyword-only. Command-line flags for one run are listed in [Command-line flags](flags.md).

## Where and what

| Argument | Default | Meaning |
| --- | --- | --- |
| `bundle_root` | nearest directory with `databricks.yml` | The bundle's directory. Relative paths are resolved against the pytest rootdir. Without it, the plugin walks up from the rootdir to the first directory holding `databricks.yml` or `databricks.yaml`. |
| `target` | `"test"` | The bundle target to deploy and read job templates from. |
| `profile` | SDK default resolution | The Databricks config profile for both the CLI and the SDK. Without it, both read `DATABRICKS_CONFIG_PROFILE` and the other `DATABRICKS_*` variables. |
| `workspace_client` | made from `profile` | A `WorkspaceClient` to use instead. The CLI still uses `profile`, or else the client's profile. |
| `sync` | `"deploy"` | How to prepare the bundle: `"deploy"`, `"sync"` or `"none"`. See [Skip the deploy](../recipes/deployments.md#skip-the-deploy). |
| `variables` | `None` | Bundle variables as a mapping of name to value, passed as `--var name=value`, sorted by name, to every `bundle` command. Instances for the same target with different variables are set up, and deployed, separately. |
| `deploy_flags` | `()` | Extra flags for `databricks bundle deploy`, added at the end of the command, such as `["--fail-on-active-runs"]`. Used only when `sync` is `deploy`. `-t`, `--target`, `-e`, `--environment`, `-p`, `--profile`, `--var` and `--auto-approve` raise `ValueError`. |
| `catalog` | `None` | The catalog in which `schema()` creates temporary schemas. |
| `helpers_path` | `/Users/<you>/.pytest-databricks-lakeflow` | The workspace folder for the helper notebooks, the setup and teardown notebooks, the debug notebooks and the inlined saved query files. |
| `strict_artifacts` | `False` | Raise `LakeflowError` from the first template lookup when the bundle has a wheel artifact without `dynamic_version: true`, or, with `sync="none"`, a local wheel built after its uploaded copy, instead of the `UserWarning` emitted once per session for each. |
| `allow_undeclared_params` | `False` | Allow job parameters that the job does not declare. |
| `test_marker` | `False` | Declare a `__pytest_test_id` job parameter on every copy, with the test node id as its default. See [Let job code know it is under test](../recipes/data.md#let-job-code-know-it-is-under-test). |

## Runs

| Argument | Default | Meaning |
| --- | --- | --- |
| `timeout` | 30 minutes | The default `timedelta` to wait for one run. Each job is created with `timeout_seconds` equal to this plus 60 seconds, so the platform stops a run that the plugin can no longer cancel. A template with a smaller `timeout_seconds` keeps it. A longer `timeout` passed to `run()` or `start()` lengthens the job's `timeout_seconds` before that run starts. |
| `poll_interval` | two seconds | The `timedelta` between two polls of a run that has not finished. Zero or less raises `ValueError`. |
| `max_concurrent_runs` | `None`, no cap | How many runs this process may have active at once. Every `Lakeflow` in the process shares the cap of the first run started under one, and the cap is per xdist worker. `--lakeflow-max-concurrent-runs` overrides it. Less than 1 raises `ValueError`. See [Cap the number of active runs](../recipes/runs.md#cap-the-number-of-active-runs). |
| `queue` | `None`, the job's own setting | The default of `queue=` on `run()` and `start()`. |
| `notebook_export` | `"failed"` | Which notebook tasks to export into `TaskRun.notebook` after a run: `"failed"` exports each notebook task that ran and did not succeed, `"always"` every notebook task that ran, `"never"` none. Any other value raises `ValueError`. |
| `dbt_artifacts` | `False` | Download the artefacts tarball of each dbt task as soon as its run finishes and parse it into `TaskRun.dbt`. Each run gets a temporary directory, removed at the end of the session unless `keep` or `--lakeflow-keep` is set. |
| `run_finished` | `None` | Called with each `JobRun` and the test after a run finishes, once per run however often `RunHandle.wait()` is called. See [Record run timings](../recipes/compute.md#record-run-timings). |

## Compute

| Argument | Default | Meaning |
| --- | --- | --- |
| `helpers_compute` | `None`, meaning serverless | A `job_cluster_key` from the job for the stub, probe, setup and teardown tasks, for workspaces without serverless. |
| `helpers_environment_version` | `"4"` | The serverless environment version of the plugin's `__pytest_serverless` environment, which the stub, probe, setup and teardown tasks use. |
| `environment_version` | `None` | The serverless environment version of every environment copied from the template, such as `"5"`. Environments whose spec sets `base_environment` keep it. `environments` wins for the environments it names. |
| `existing_cluster_id` | `None` | A running cluster for every task that the template puts on a job cluster or a new cluster, helper and nested for-each tasks included. The job's `job_clusters` are dropped. Serverless and warehouse tasks keep their compute. |
| `single_node` | `False` | Make every job cluster and every task's `new_cluster` a single node cluster. Raises `LakeflowError` naming the cluster when one uses standard access mode. See [Run job clusters on one node](../recipes/compute.md#run-job-clusters-on-one-node). |
| `budget_policy_id` | `None` | The budget policy of every copy and pipeline copy. A pipeline copy without it keeps the deployed pipeline's. |
| `performance_target` | `"PERFORMANCE_OPTIMIZED"` | The serverless performance mode of every copy and every `once()` job, whatever the template says: `"PERFORMANCE_OPTIMIZED"` starts each task in seconds; `"STANDARD"` bills fewer DBUs and adds 4 to 6 minutes of start-up to every task, for CI runs that can wait; `None` keeps the template's own setting, with a warning when that is `STANDARD`. `--lakeflow-performance-target` overrides it for one run of pytest. Anything else raises `ValueError`. When a run asked for `PERFORMANCE_OPTIMIZED` and the platform used `STANDARD`, the plugin warns once per `Lakeflow`. |
| `disable_auto_optimization` | `True` | Set `disable_auto_optimization` on serverless code tasks, nested for-each tasks included, so that a failure fails once instead of being retried by the platform. |

## What the copy keeps

| Argument | Default | Meaning |
| --- | --- | --- |
| `side_effects` | `"neutralise"` | `"neutralise"` pauses dashboard subscriptions and SQL alert subscriptions in every copy. `"keep"` leaves them as the template has them. Alert tasks and Power BI tasks are left alone in both modes. Any other value raises `ValueError`. |
| `run_as` | `None`, run as you | The default of `run_as=` on `job()` and `task()`: `"keep"`, a user, a service principal's application id or a `jobs.JobRunAs`. |
| `disable` | `None` | The default of `disable=` on `job()`: task keys to disable in every copy. A key that a job does not have is skipped, whereas `disable=` on a call refuses it. |
| `extra_dependencies` | `None` | The default of `extra_dependencies=` on `job()` and `task()`. |
| `environments` | `None` | The default of `environments=` on `job()` and `task()`. A key the job does not have is skipped, whereas `environments=` on a call adds it. A key starting with `__pytest_`, an entry that is not a mapping or an unknown field raises `ValueError` when `Lakeflow` is made. |
| `children` | `"warn"` | The default of `children=` on `job()` and `task()`: `"warn"` or `"refuse"`. Any other value raises `ValueError`. |
| `setup`, `teardown` | `None` | The defaults of `setup=` and `teardown=` on `job()` and `task()`. A string, an empty list, a statement with a stray `$`, a path that is not a `.py` file or a function the plugin cannot ship raises `ValueError` when `Lakeflow` is made. |
| `saved_queries` | `"deployed"` | The default of `saved_queries=` on `job()` and `task()`: `"deployed"` or `"inline"`. Any other value raises `ValueError`. |
| `permissions` | `None` | The `access_control_list` given to every copy when it is created, as `jobs.JobAccessControlRequest`s or dicts such as `{"group_name": "data-eng", "permission_level": "CAN_VIEW"}`. `IS_OWNER` raises `ValueError`. |
| `modify_settings` | `None` | Called with each copy's `JobSettings` and the test's `pytest.Item` just before the job is created. Change the settings in place. Job parameters it declares are accepted by `params`. `access_control_list` is not a settings field; use `permissions`. |

## Keeping and sweeping

| Argument | Default | Meaning |
| --- | --- | --- |
| `keep` | `False` | Keep every copy, pipeline copy and temporary schema. `--lakeflow-keep` also turns this on. |
| `keep_failed` | `True` | Keep the copy and its pipeline copies when their test fails. `--lakeflow-keep-failed` and `--no-lakeflow-keep-failed` override it. |
| `sweep_hours` | `24` | The age after which `--lakeflow-sweep` and `sweep()` delete leftover jobs, pipeline copies, schemas and notebooks. |

## Defaults for per-call arguments

`run_as`, `queue`, `disable`, `extra_dependencies`, `children`, `setup`, `teardown`, `environments` and `saved_queries` hold the default of the argument of the same name on `job()` and `task()` (`queue` on `run()` and `start()`, `disable` on `job()` only). A call that leaves the argument out, or passes `None`, takes the `Lakeflow` value. Anything else replaces it whole, without merging: `disable=[]` disables nothing, `extra_dependencies=[]` adds none and `environments={}` leaves the template's environments as they are.

Once a default is set, a call cannot opt out of `setup`, `teardown` or `run_as`: `None` takes the `Lakeflow` value, and `setup=[]` raises `ValueError` like any empty list of statements. For tests that need no setup, derive a `Lakeflow` without it, such as `dataclasses.replace(lakeflow, setup=None)`, in their directory's `conftest.py`. To run one copy as yourself while `Lakeflow` sets another `run_as`, pass your own user name.

## Methods

| Method | Does |
| --- | --- |
| `lakeflow.job(key, ...)`, `lakeflow.task(ref, ...)` | Make a throwaway copy of a job, or of one task. See [Job tests](../job-tests.md) and [Task tests](../task-tests.md). |
| `lakeflow.once(name, *, setup=None, teardown=None, params=None, timeout=None)` | Run a setup step once for the whole session, across xdist workers, and its teardown when the session ends; returns the setup run, or `None` without a setup. See [Seed once for the whole session](../recipes/data.md#seed-once-for-the-whole-session). |
| `lakeflow.schema(catalog=None)` | A temporary schema, dropped when the block ends. |
| `lakeflow.pipeline(key, schema, ...)` | A throwaway copy of a bundle pipeline. |
| `lakeflow.check(key, *, rules=None)` | The wiring mistakes in a job's template, without creating anything. |
| `lakeflow.attach(run_id)` | The `JobRun` of a finished run of a kept job. |
| `lakeflow.sweep(...)` | The `--lakeflow-sweep` sweep from plain Python. |

## Properties

| Property | Holds |
| --- | --- |
| `lakeflow.ws` | The `WorkspaceClient`. Use it in tests instead of a `WorkspaceClient()` of your own, which on a user-to-machine profile would refresh the CLI's token once more. |
| `lakeflow.run_id` | The run id that tags every job and names every temporary schema. |
| `lakeflow.templates` | The usable job templates as dicts, by bundle resource key. |
| `lakeflow.template_errors` | Why each other job cannot be used as a template, by key. |
| `lakeflow.template(key)` | A fresh `JobSettings` copy of one template. Raises `KeyError` for an unknown job and `LakeflowBundleError` for one that cannot be used, such as one that is not deployed. |
