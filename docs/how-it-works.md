# How it works

What the plugin does in the workspace, from the first `lakeflow.job()` call to the last cleanup, and what it leaves behind. Read this to understand a log line or an unexpected state; the [recipes](recipes/index.md) say how to use each feature.

## Session setup

Creating a `Lakeflow` does nothing. The first time a test asks it for a job template, the plugin sets up the bundle target. Setup is keyed on the bundle root, target, profile, variables, sync strategy, helpers path and deploy flags, and happens once per key per session, however many `Lakeflow` instances share it.

1. **CLI check.** `databricks --version` must report v1.7.0 or later.
2. **Pre-deploy checks.** With `sync="deploy"`, `databricks bundle validate -t <target> -o json` runs first. The target's `workspace.host` is compared with the workspace client's host, ignoring scheme, trailing slash and case, and a mismatch fails setup before anything reaches either workspace. The `run_as` of the target and of each job is compared with the deploying identity: another user, or a service principal on which that identity lacks the Service Principal User role, fails setup with `LakeflowBundleError`, because the deploy would fail late or deploy jobs that run as someone else.
3. **Templates.** `databricks bundle deploy -t <target> --auto-approve` runs, then `bundle summary -t <target> -o json` for the deployed job and pipeline ids, then `jobs.get` for each job id in parallel. The deployed settings are the templates, with workspace paths for notebooks and uploaded wheels. A job the summary lists without an id is recorded as not deployed, and one whose id no longer exists as stale, so using it fails with a clear message. With `sync="sync"` or `"none"`, the templates come from `bundle validate -o json` instead, after a `bundle sync` for `sync`.
4. **Artifact checks.** The summary's `artifacts` that build wheels without `dynamic_version: true` are noted, and with `sync="none"` each local library file is compared with its uploaded copy's modification time. The first template lookup warns once per session about each, or raises with `strict_artifacts=True`.
5. **Helper notebooks.** Two small Python notebooks, one that sets task values from a widget and one that reads them and returns them as JSON, are uploaded to `helpers_path` with overwrite. Their widget names start with `__pytest_` because every job parameter is pushed into every notebook task's widgets.
6. **Sweep.** With `--lakeflow-sweep`, the first process to finish a setup sweeps, once for the session, see [What the sweep deletes](reference/flags.md#what-the-sweep-deletes).

The result, job templates as dicts, template errors, helper paths and the artifact notes, is JSON. If setup fails, the error is remembered and every later use in the session fails with the same message instead of retrying the deploy.

### Parallel sessions

Under pytest-xdist, the controller creates a temporary directory and passes it and the run id to every worker. The first worker to need a bundle target takes a `FileLock` on a state file named after the bundle key, runs the setup and writes the JSON. Workers that arrive later read the file. A setup error is written too, so every worker reports it. Each worker builds its own `WorkspaceClient`, and the controller deletes the directory at the end.

## One copy of a job

### Build the settings

`lakeflow.job(key)` or `lakeflow.task("key.task")` deep-copies the template into a fresh `JobSettings` and applies the options in this order. Every check raises before anything is created in the workspace.

1. For a three-part `task()` ref, the nested task replaces its for-each task, keeping its own settings and taking the loop's `depends_on`.
2. `task_params` merges or replaces each named task's parameters, then warns about names that a job parameter will win over.
3. For a job test: `disable` sets `disabled` on the named tasks, `force_conditions` rewrites each named condition task's operands to constants, and `extra_dependencies` is appended to every template environment and to the libraries of every task on a cluster. The helper tasks and environment are added later, so they never get the dependencies.
4. `task_overrides` sets the timeout and retry fields of the named tasks. It runs before any helper task exists, so stub, probe, setup and teardown keys are unknown to it.
5. For a job test: `upstream_task_values` replaces each named task with a stub of the same key, without its `depends_on`, and rewrites every `{{tasks.<key>.output.<path>}}` reference to it with the stub's outputs; `capture_task_values` appends a probe after each named task; `for_each_inputs` replaces each named loop's `inputs` as JSON. For a task test: `input` rewrites the body's `{{input}}` references to `__pytest_input` job parameters; the target is kept alone with a stub for each entry in `upstream_task_values` as its only dependencies, and a probe when values are captured.
6. `trigger` rewrites each `{{job.trigger.<path>}}` reference it names to a `__pytest_trigger_<path>` job parameter, warns about the references it leaves, and the job's creation later refuses a `file_arrival` or `table_update` reference that is left.
7. `setup=` and `teardown=` add the `__pytest_setup` task, which every task without `depends_on` comes to depend on, and the `__pytest_teardown` task, which depends on every leaf with `run_if: ALL_DONE`. Their notebooks are uploaded once per distinct notebook per session as `setup_<hash>` and `teardown_<hash>`. Helper tasks get the `__pytest_serverless` environment, or the `helpers_compute` job cluster.
8. **Sanitise.** The copy loses what would make it run on its own, notify people or count as bundle-managed: `deployment`, `edit_mode`, `schedule`, `trigger`, `triggers`, `continuous`, the notification fields and `parent_path` on the job, and the notification fields on every task. Dashboard and SQL alert subscriptions are paused unless `side_effects="keep"`. `run_as` is stripped unless the test passed one. `disable_auto_optimization` is set on serverless code tasks. The compute overrides apply: `environment_version`, `budget_policy_id`, `existing_cluster_id`, then `single_node`. `performance_optimized` sets the performance target, `max_concurrent_runs` becomes 1, and `timeout_seconds` becomes `Lakeflow`'s `timeout` plus 60 seconds unless the template has a smaller positive one. The job is named `[pytest] <test node id>` within the 4096-byte limit, and tagged `pytest`, `pytest_run_id`, `pytest_nodeid` and `pytest_job_key` within the 25-tag limit, then with `pytest_expires`, `pytest_ci_url`, `pytest_user`, `pytest_git_sha` and `pytest_git_branch` while room remains. The description becomes the test node id and the CI run URL, then the template's description. `test_marker` declares `__pytest_test_id`.
9. `environments` merges each override into the copy's environments.
10. A `performance_target` of `STANDARD` triggers a warning, and each enabled Run Job task triggers a warning, or `LakeflowError` with `children="refuse"`.
11. With `pipelines="copy"`, the pipelines that enabled pipeline tasks update are listed and their deployed specs read. Nothing is created yet.
12. A finaliser is registered on the test, so the job is closed at teardown if no `with` block closes it first.

### Create and run

1. With `replay`, `run()` first stubs the tasks that `only` leaves out with the values the replayed run captured.
2. Entering the `with` block, or the first `run()` or `start()`, calls `modify_settings`, checks that `params` are declared and that every serverless Python script, wheel, JAR and dbt task has an `environment_key`, warns about notebook tasks that `extra_dependencies` cannot reach, creates the pipeline copies and points the pipeline tasks at them, and calls `jobs.create` with `access_control_list` from `permissions`. When `run_as` names a service principal, the Service Principal User role is checked first, once per session, and a `PermissionDenied` is turned into a `LakeflowError` that explains the role. The job URL is logged and recorded in the test's `user_properties`.
3. A `run()` or `start()` whose `timeout` plus 60 seconds exceeds the job's `timeout_seconds` first lengthens it with `jobs.update`, sending the tasks as well, because `jobs/update` reads a body without `tasks` on a one-task job in the legacy single-task format. `RunHandle.wait()` cannot do this, so a longer wait timeout raises `LakeflowError`.
4. With `only`, the selected tasks' `{{tasks.<key>.values...}}` and `{{tasks.<key>.output...}}` references to tasks that are neither selected nor stubs are checked, because the platform would start the run and fail it. `only` is extended with the stubs, probes, setup and teardown tasks the selection needs, and `run_now` is called with the merged job parameters, a fresh idempotency token, and the performance target, queue and pipeline parameters the test passed.
5. **Waiting** polls `get_run` every `poll_interval`, rather than using the SDK waiter, which raises on an ordinary task failure. Each poll logs a line for each task run whose state changed and reads the output of each task run that terminated since the previous poll. When the timeout expires the run is cancelled and `LakeflowTimeout` is raised with each task's last state. With `stop_after`, the run is cancelled once that long has passed and polled until it has ended, then returned as a cancelled `JobRun`.
6. **Building the `JobRun`** keeps every attempt of each task and calls `get_run_output` for each attempt whose output was not already read, in parallel. It parses errors, probe outputs and condition outcomes, sorts tasks into `tasks`, `injected_tasks`, `skipped_tasks` and `disabled_tasks`, and reads each task's resolved parameters with `get_run` by the task run's id, because the Jobs API fills `resolved_values` only there. It reads the raw `runs/get-output` of each SQL file task for its rows, lists each pipeline task's pipeline updates to find the one the task started, reads the iterations of each for-each task from the for-each task run, downloads dbt artefacts when asked, reads each Run Job task's child run, and exports the notebooks that `notebook_export` selects with `jobs.export_run`.
7. A teardown task that did not succeed is logged at WARNING. A run that asked for `PERFORMANCE_OPTIMIZED` and used `STANDARD` warns once per `Lakeflow`. `run_finished` is called, once per run.

### Tear down

1. Leaving the `with` block cancels any run of this job that is still active. With `teardown_at="close"`, it then runs the teardown task alone, with `run_now(only=["__pytest_teardown"])`, the job's `params` and queueing on, and waits for it for `Lakeflow`'s `timeout`, unless no run was started or `keep` is set.
2. The job is deleted unless a keep rule applies. In order: `--lakeflow-keep` or `keep=True` keeps everything; `--lakeflow-keep-failed` and `--no-lakeflow-keep-failed` override `keep_failed`; otherwise a job is kept when the block raised anything other than `pytest.skip` or `pytest.xfail` and `keep_failed` is on. Kept jobs are logged at WARNING with every run URL and collected for the terminal summary.
3. Without `with`, the finaliser applies the same rules, treating the test as failed if its setup, call or teardown report failed.
4. The job's pipeline copies are closed after the job, with the same keep rules, so a kept job never points at a deleted pipeline.
5. `lakeflow.schema()` drops its schema with `force=True` in a `finally` block, unless `keep` or `--lakeflow-keep` is set.

## A pipeline copy

1. `lakeflow.pipeline()` looks the key up among the pipeline ids that `bundle summary` printed; `pipelines="copy"` takes the ids from the copy's pipeline tasks. `pipelines.get` reads the deployed spec, whose libraries and `root_path` are workspace paths after the deploy, so nothing is uploaded for the copy.
2. The copy keeps every field of the spec that `pipelines.create` takes, except `id`, `deployment`, `continuous`, `trigger`, `notifications` and `event_log`. It sets the catalog and schema from the schema's full name, in the spec's publishing mode, `development: true`, `allow_duplicate_names: true`, the name `[pytest] <test id>` cut to 255 characters, the job tags over the spec's tags, `configuration` over the spec's, and `budget_policy_id` when `Lakeflow` has one. A pipeline spec has no `run_as`, so the copy runs as its creator.
3. Entering the `with` block, or the first `run()`, calls `pipelines.create`, and the copy's URL is recorded as `lakeflow_pipeline_url` and `lakeflow_pipeline_id`.
4. `run()` calls `pipelines.start_update` and polls `pipelines.get_update` every `poll_interval` until the update is `COMPLETED`, `FAILED` or `CANCELED`, logging each change of state. At the timeout it calls `pipelines.stop` and raises `LakeflowTimeout`. The `PipelineUpdate` reads events from a minute before the update was created, because the update's first event is logged a few milliseconds before its creation time.
5. Closing the copy stops an update that is still running and, unless the keep rules keep it, calls `pipelines.delete` with `cascade=True` and `force=True` where the installed databricks-sdk takes them.

## Session end

1. Every process cancels the runs it started that are still active, and stops the pipeline updates that `EphemeralPipeline.run()` started.
2. xdist workers report the targets they deployed, what they kept, the debug notebooks they wrote and the runs they cancelled to the controller.
3. With `--lakeflow-destroy`, the controller, or the single process, runs `databricks bundle destroy -t <target> --auto-approve` for each target deployed with `sync="deploy"`, with the same `--var` arguments as its deploy. A failed destroy turns a passing session into a failed one.
4. A `lakeflow` block in the terminal summary lists the kept jobs, pipeline copies and schemas, the debug notebooks and the cancelled runs, see [The terminal summary](reference/output.md#the-terminal-summary).

## What is left in the workspace

- The deployed test target, unless you pass `--lakeflow-destroy`. It is only a template source, and leaving it makes the next session's deploy fast.
- The two helper notebooks under `helpers_path`, and the `setup_<hash>` and `teardown_<hash>` notebooks of any `setup=` and `teardown=` arguments. Each edit of a function, and each plugin upgrade that changes the notebook around it, uploads a new one.
- The `query_<run id>_<task key>_<hex>.sql` files of `saved_queries="inline"` under `helpers_path`, uploaded when the job is created and deleted with it. A kept job keeps its files.
- Debug notebooks from `debug_notebook()` and `--lakeflow-debug-notebook`, under `helpers_path/debug/`, one per job, task and run.
- Jobs, pipeline copies and schemas kept by the keep rules. Their tags say which test, run, person and commit made them, and `lakeflow.attach(run_id)` rebuilds the `JobRun` of a kept job's run.
- Nothing else. `--lakeflow-sweep` with its flags removes all of the above but the deployed target and the two helper notebooks, see [Clean up leftovers](recipes/ci.md#clean-up-leftovers).

## Timing

Measured in one Azure workspace:

- A trivial serverless notebook test takes about 20 to 35 seconds end to end, of which about 2 seconds is API overhead.
- A probe, stub, setup or teardown task adds 7 to 30 seconds.
- A job with `performance_target: STANDARD` adds 3 to 4 minutes to every run.
- A classic job cluster takes 2 to 10 minutes to start.
- Five jobs run at once took no longer than one.
- An update of a serverless pipeline copy with one materialised view over literal rows took 28 to 29 seconds from `start_update` to `COMPLETED`; a pipeline task running the same pipeline took 31 to 42 seconds.

To measure your own workspace, read `run.task(key).timing`, which splits each task's time into queue, setup, execution and cleanup, and `run.effective_performance_target`, which shows which performance mode the run used.
