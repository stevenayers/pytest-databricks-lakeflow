# How it works

This page follows a test session from the first `lakeflow.job()` call to the last cleanup, and says what the plugin leaves in the workspace.

## Session setup

Creating a `Lakeflow` does nothing. The first time a test asks it for a job template, the plugin sets up the bundle target. Setup is keyed on the bundle root, target, profile, sync strategy and helpers path, and happens once per key per session, however many `Lakeflow` instances share it.

1. **CLI check.** `databricks --version` must report v1.7.0 or later.
2. **Templates.** With `sync="deploy"`, the plugin runs `databricks bundle deploy -t <target> --auto-approve`, then `databricks bundle summary -t <target> -o json` for the deployed job ids, then `jobs.get` for each id in parallel. The deployed settings are the templates, with workspace paths for notebooks and uploaded wheels. A job that the summary lists without an id is recorded as not deployed, and a job whose id no longer exists in the workspace as stale, so that using it fails with a clear message instead of a bare API error. With `sync="sync"` or `"none"`, the templates come from `databricks bundle validate -o json` instead, after a `bundle sync` for `sync`. See [sync strategies](configuration.md#sync-strategies) for what those refuse.
3. **Helper notebooks.** Two small Python notebooks, one that sets task values from a widget and one that reads them and returns them as JSON, are uploaded to `helpers_path` with overwrite. Their widget names start with `__pytest_` because every job parameter is pushed into every notebook task's widgets.
4. **Sweep.** With `--lakeflow-sweep`, the process that did the setup lists your jobs and deletes those tagged `pytest` whose `pytest_run_id` differs from this run and whose creation time is older than `sweep_hours`.

The result, job templates as dicts, template errors and helper paths, is JSON. If setup fails, the error is remembered and every later use in the session fails with the same message instead of retrying the deploy.

### Parallel sessions

Under pytest-xdist, the controller creates a temporary directory and passes it and the run id to every worker. The first worker to need a bundle target takes a `FileLock` on a state file named after the bundle key, runs the setup and writes the JSON. Workers that arrive later read the file. A setup error is written too, so all workers report it. Each worker builds its own `WorkspaceClient`, and the controller deletes the directory at the end.

## One ephemeral job

### Build the settings

1. `lakeflow.job(key)` or `lakeflow.task("key.task")` deep-copies the template into a fresh `JobSettings`.
2. For a job test, `inject_probes` appends a probe task for each entry in `capture_task_values`. For a task test, `wrap_task` keeps only the target task, gives it a stub task for each entry in `upstream_task_values` as its only dependencies, and appends a probe if `capture_task_values` is set. Helper tasks get the `__pytest_serverless` environment, or the `helpers_compute` job cluster.
3. `sanitise` strips what would make the copy run on its own, notify people, run as someone else or count as bundle-managed: `deployment`, `edit_mode`, `schedule`, `trigger`, `triggers`, `continuous`, `email_notifications`, `webhook_notifications`, `notification_settings`, `run_as` and `parent_path` on the job, and the notification fields on every task. It sets `disable_auto_optimization` on serverless code tasks unless told otherwise, overrides the performance target if `performance_optimized` is set, allows one concurrent run, names the job `[pytest] <test node id>` within the 4096-byte limit, and adds the tags `pytest`, `pytest_run_id` and `pytest_nodeid` to the template's tags within the 25-tag limit.
4. A `performance_target` of `STANDARD` in the template triggers a warning, because it adds 3 to 4 minutes to every run.
5. A finaliser is registered on the test so the job is closed at teardown if no `with` block closes it first.

### Create and run

1. Entering the `with` block, or the first `run()`, calls `modify_settings` if set, checks that `params` are declared, and calls `jobs.create` with the settings fields that `create` accepts. The job URL is logged and recorded in the test's `user_properties`.
2. `run()` merges the run's parameters over the job's, extends `only` with the stubs and probes the selected tasks need, and calls `run_now` with `job_parameters` and `only`.
3. The plugin polls `get_run` every two seconds until the run terminates, rather than using the SDK waiter, which raises on an ordinary task failure. The run URL is recorded as soon as it is known. When the timeout expires the run is cancelled and `LakeflowTimeout` is raised.
4. Building the `JobRun` takes the latest attempt of each task, calls `get_run_output` for each one in parallel, parses errors from the `error` field (falling back to the last `Name: message` line of the trace), parses each probe's JSON output into the `values` of the task it watched, and sorts tasks into `tasks`, `injected_tasks` and `skipped_tasks`.
5. `run_finished` is called if set.

### Tear down

1. Leaving the `with` block cancels any run of this job that is still active, then deletes the job unless a keep rule applies. The rules, in order: `--lakeflow-keep` or `keep=True` keeps everything; `--lakeflow-keep-failed` and `--no-lakeflow-keep-failed` override `keep_failed`; otherwise a job is kept when the block raised anything other than `pytest.skip` or `pytest.xfail` and `keep_failed` is on. Kept jobs are logged at WARNING with every run URL.
2. Without `with`, the finaliser applies the same rules, treating the test as failed if its setup, call or teardown report failed.
3. `lakeflow.schema()` drops its schema with `force=True` in a `finally` block, unless `keep` or `--lakeflow-keep` is set.

## Session end

1. Every process cancels the runs it started that are still active.
2. xdist workers report the bundle targets they deployed to the controller.
3. With `--lakeflow-destroy`, the controller, or the single process, runs `databricks bundle destroy -t <target> --auto-approve` for each target deployed with `sync="deploy"`. A failed destroy is printed with the summary and turns a passing session into a failed one.

## What is left in the workspace

- The deployed test target, unless you pass `--lakeflow-destroy`. It is only a template source, and leaving it makes the next session's deploy fast.
- The two helper notebooks under `helpers_path`.
- Jobs kept by the keep rules. Their runs stay with them. `--lakeflow-sweep` deletes them once they are older than `sweep_hours`.
- Nothing else. Deleting an ephemeral job removes its runs.

## Timing

Measured in one Azure workspace:

- A trivial serverless notebook test takes about 20 to 35 seconds end to end, of which about 2 seconds is API overhead.
- A probe or stub task adds 7 to 30 seconds.
- A job with `performance_target: STANDARD` adds 3 to 4 minutes to every run.
- A classic job cluster takes 2 to 10 minutes to start.
- Five jobs run at once took no longer than one.
