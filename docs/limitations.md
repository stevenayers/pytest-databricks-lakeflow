# Limitations and FAQ

## Limitations

- **Logs.** `TaskRun.logs` is only filled for Python script, wheel and JAR tasks, and holds only the last 5 MB. Notebook tasks have no logs; what a notebook printed is in `task.notebook.stdout`, read from the notebook that `jobs.export_run` renders, by default only for notebook tasks that ran and did not succeed. That export format is not documented, so it is parsed best effort: when it cannot be read, `task.notebook.cells` is empty, `task.notebook.html` keeps the export and a `UserWarning` is emitted. Notebook output keeps only the first 5 MB. A truncated output or log raises a `UserWarning` naming the task, and `output_json` refuses truncated output with a `ValueError`. Databricks documents that on serverless compute the logs of one task can include output from other tasks in the same run. In our tests each task's logs held only its own output, but this is not guaranteed.
- **Resolved task parameters cost one request per task.** The Jobs API fills a task's `resolved_values` only when the task run is fetched by its own run id with `include_resolved_values=true`; on the job run the field is always null. So building a `JobRun` calls `runs/get` once per task run that ran, in parallel, on top of `runs/get-output`. A task run that cannot be read leaves `TaskRun.parameters` as `None` with a warning in the log.
- **SQL rows come from SQL file tasks only.** `TaskRun.sql_result` is read from `sql_output.file_output`, a field of `runs/get-output` that the Jobs API reference does not document and databricks-sdk does not model, so the plugin reads the raw response: one extra request per SQL file task that ran. Saved query, alert and dashboard tasks give `None`; query a table the job wrote instead. Values are strings as the platform reports them, a NULL is `None`, and the platform keeps at most 1,000 rows (`truncated` says when it cut them) for 7 days.
- **Alert tasks succeed whether or not the alert triggered.** Databricks marks an alert task succeeded when the alert was evaluated, so `succeeded` says nothing about the alert. Assert on `TaskRun.alert_state`.
- **Alert tasks and Power BI tasks run as the template has them.** An alert task notifies its subscribers, or the alert's own subscribers when the task sets none, and the task has no pause flag, so the copy cannot silence it without disabling it. A Power BI task updates the semantic model whether or not `refresh_after_update` is set; the flag only adds an Import-mode data refresh. Disable such a task in a test with `disable=`, for example `lakeflow.job("reporting", disable=["check", "refresh_powerbi"])`.
- **Pipeline tasks run the deployed pipeline.** An ephemeral job copies the job, not the pipelines its pipeline tasks point at, so every test updates the same pipeline and its tables. `full_refresh`, `full_refresh_selection` and `refresh_selection` on `job.run()` apply to every pipeline task in the run.
- **`setup=` and `teardown=` functions ship as source.** They run as notebooks on the job's compute, so they must be self-contained: imports inside the function, no helper functions defined elsewhere in the test module, no variables of an enclosing function. Lambdas, closures, decorated functions and functions defined in a REPL are refused with `LakeflowError`. Only parameters the job declares reach `params`. Each costs one extra task start per run, like a stub.
- **Task values need probes.** Task values are not available through the Jobs API, so they are read by probe tasks. Each probe costs 7 to 30 seconds of serverless compute, even when its task did not run. For the same reason `lakeflow.task(..., upstream_from=run)` can only replay a task from a run that captured its upstream tasks' values with `capture_task_values`, never from a production run.
- **Concurrent sessions sharing one target.** Sessions that deploy the same target share one deployment, so a deploy from one session replaces the wheels that another session's jobs are still running, and those jobs fail. `deploy_flags=["--fail-on-active-runs"]` does not prevent this, because it only sees runs of the deployed jobs, not the ephemeral copies. Use the recipe in [isolating test deployments](how-it-works.md#isolating-test-deployments).
- **Job code must read catalog and schema from job parameters.** Bundle variables are fixed at deploy time, and the plugin deploys each target once per session for each set of `variables`.
- **Different setups of one target can collide under xdist.** `Lakeflow` instances for the same target that differ in `variables`, `sync` or `helpers_path` are set up separately, so two xdist workers can deploy that target at the same time and one of them can fail on the CLI's deployment lock. Give each set of variables its own target.
- **Serverless caches wheels by file name.** A bundle wheel artifact without `dynamic_version: true` is uploaded under the same file name on every deploy while its version is unchanged, and serverless compute can keep running the cached old build, so a test can pass or fail against code you have already changed. When the bundle summary (or, with `sync="sync"` or `"none"`, the validate output) lists such an artifact, an artifact of `type: whl` or one with a `build` command and no `type`, the first template lookup of the session emits one `UserWarning` (one per xdist worker): `Artifacts ['lakeflow_example'] have no dynamic_version: true. Serverless caches wheels by file name, so a rebuilt wheel with the same version may not be picked up. Set artifacts.<name>.dynamic_version: true in databricks.yml.` Set `dynamic_version: true` on the artifact, which makes the CLI add a unique local version to every build, or bump the version. `Lakeflow(strict_artifacts=True)` raises `LakeflowError` with the same text instead.
- **`extra_dependencies` takes no local paths yet, and skips serverless notebook tasks without an environment.** Local wheels such as `./dist/*.whl[test]` raise `LakeflowError`; pass a `/Workspace` or `/Volumes` path or declare the wheel in the bundle job. A serverless notebook task without an `environment_key` cannot take dependencies from the job settings, so it is left unchanged.
- **Single-task jobs keep only the target task.** On a shared job cluster a task can import libraries that other tasks declared, and on its own it loses them. Declare every library on the task that needs it.
- **Serverless auto-retry is off by default** in ephemeral jobs, for notebook, Python script, Python wheel and JAR tasks, so a test sees one attempt. Pass `disable_auto_optimization=False` to keep the platform's behaviour. When a task is retried, `TaskRun` describes the last attempt and its `attempts` holds every attempt.
- **The Databricks CLI is required.** Deploying is only possible through the CLI, so the plugin shells out to it. There is no pure-SDK mode.
- **One run at a time per ephemeral job.** Each copy is created with `max_concurrent_runs: 1`. Parallelism comes from separate tests, each with its own job, or from several copies in one test, each started with [`start()`](job-tests.md#start-a-run-without-waiting) and waited for afterwards.
- **`sync` and `none` strategies read `bundle validate` output**, which leaves library paths local and `${resources...}` references unresolved. Jobs affected by that are refused, with an error that says to use `deploy`.
- **`single_node` needs dedicated access mode.** Single node compute does not support standard access mode, so `Lakeflow(single_node=True)` raises `LakeflowError` naming the first job cluster or task cluster whose `data_security_mode` is `USER_ISOLATION` or `DATA_SECURITY_MODE_STANDARD`. It sets the explicit single node fields (`num_workers`, `spark_conf` and the `ResourceClass` tag), not `kind` and `is_single_node`; the Clusters API accepted them as they are on a `SINGLE_USER` job cluster without a policy, and a cluster policy that forbids `spark.databricks.cluster.profile` would refuse them.
- **Standard (shared) access mode** classic clusters were not tested. There, task values set from wheel or script tasks may be silently lost.
- **Run Job tasks run the deployed child job.** The copy keeps `run_job_task.job_id` as deployed, so a test of a parent job starts a real run of the child, with the child's `run_as` and notifications, which is not cancelled or deleted with the ephemeral job. `lakeflow.job()` and `lakeflow.task()` warn about each such task that is not disabled and not inside a disabled loop; `children="refuse"` raises `LakeflowError` instead, before anything is created. `TaskRun.child_run_id` gives the id of the child's run, and `TaskRun.child_run` gives the run as a `JobRun` with the child's tasks. Copying or stubbing the child is not supported yet.
- **Forced conditions apply to the whole copy.** `force_conditions` rewrites the copy's settings, so every run of that copy takes the same branch; make another copy, for example in a parametrised test, for the other branch. The condition task keeps its `depends_on`, so the tasks upstream of it still run. If `disable` disables one of them, the condition task and its branches do not run either, because a disabled parent does not satisfy the default `All succeeded` run-if. Condition tasks nested in a `for_each_task` cannot be forced.
- **Names starting with `__pytest_`** are reserved for the plugin's tasks, environments, widgets and parameters, such as the `__pytest_test_id` job parameter that `test_marker=True` declares. Tests cannot pass them as `params`.

## FAQ

### Why deploy the bundle instead of reading `databricks bundle validate`?

`validate` resolves notebook paths but leaves wheel and JAR paths pointing at local files, and does not resolve `${resources...}` references. A deploy builds and uploads the wheels and gives job settings in which every path is a workspace path, read back with `jobs.get`. The deploy happens once per session, and a deploy of an unchanged bundle takes seconds. If you deploy the target yourself, `Lakeflow(sync="none")` skips it, see [sync strategies](configuration.md#sync-strategies).

### Why do my tests never run the deployed job?

The deployed job keeps its schedule, notifications, `run_as` and bundle lock, and running it would mix test runs with real ones. Each test instead gets a copy with those removed and its dashboard and SQL alert subscriptions paused, named and tagged after the test, so runs are isolated and attributable and do not email subscribers. Alert and Power BI tasks are left alone; see [Limitations](#limitations). Jobs are cheap to create and delete.

The copy runs as you rather than as the deployed job's `run_as`. To exercise the permissions of that principal, opt in with `lakeflow.job(key, run_as="keep")`, see [Run as another identity](job-tests.md#run-as-another-identity).

### Why is my job parameter rejected?

The Jobs API accepts undeclared parameters silently, so a typo would pass unnoticed. The plugin checks `params` against the job's declared `parameters` and raises `LakeflowError` for anything undeclared. Declare the parameter on the job, or pass `allow_undeclared_params=True`.

### Why is every run 3 to 4 minutes slower than in the UI?

The job's `performance_target` is `STANDARD`. The plugin warns about it. Pass `performance_optimized=True` to run tests with `PERFORMANCE_OPTIMIZED`, or `job.run(performance_target="PERFORMANCE_OPTIMIZED")` to speed up one run without changing the job's settings. `run.effective_performance_target` and each task's `effective_performance_target` show which mode the run actually used.

### How do I clean up after a crashed session?

Ephemeral jobs are tagged `pytest` and `pytest_run_id`, and, when they can be found out, `pytest_ci_url`, `pytest_user`, `pytest_git_sha` and `pytest_git_branch`, so a leftover job can be traced to its CI run, person and commit from the Jobs list. `pytest --lakeflow-sweep` deletes your own jobs from other runs that are older than `sweep_hours`, 24 hours by default. `pytest --lakeflow-destroy` runs `bundle destroy` for the test target at the end of the session.

### What does the plugin leave in my workspace?

The deployed test target, the two helper notebooks under `helpers_path` with the `setup_<hash>` and `teardown_<hash>` notebooks of any `setup=` and `teardown=` functions, and any jobs kept by the keep rules. A kept job's tags (`pytest_run_id`, `pytest_nodeid`, `pytest_ci_url`, `pytest_user`, `pytest_git_sha`, `pytest_git_branch`) and description say which test, run, person and commit made it. See [How it works](how-it-works.md#what-is-left-in-the-workspace).

### Why does setup fail with "Bundle target 'test' deploys to ... but the workspace client is for ..."?

The CLI and the SDK resolved different workspaces. The CLI takes the host from the target's `workspace.host` in `databricks.yml`, while the plugin's `WorkspaceClient` takes it from `profile` or the `DATABRICKS_*` variables. Without this check, every `jobs.get` would look in the wrong workspace and the jobs would be reported as stale. Make the two agree: pass `profile=` to `Lakeflow` for a profile whose host is the target's, set `DATABRICKS_HOST` and `DATABRICKS_CONFIG_PROFILE` to match the target, or run pytest through the CLI with `databricks bundle run -t test -- uv run pytest`, which gives it the target's credentials. A target without `workspace.host` is not checked.

### Why did the CLI fail with `exit status 45` under xdist?

Several processes refreshed a user-to-machine (`databricks-cli`) OAuth token at the same time. The plugin retries building its client, but the CLI call that hit the race fails. Use a service principal with OAuth machine-to-machine credentials or a token in CI.

### Does it work alongside databricks-labs-pytester?

Yes. The plugin defines only the `lakeflow` fixture and does not import pytester, so the two can be installed together. `lakeflow.ws` gives you the `WorkspaceClient` without a second fixture.

### Can I use `Lakeflow` outside pytest?

No. It raises `LakeflowError` outside a pytest session, and outside a test. Jobs are tied to the running test for naming, tagging and teardown.

### How do I see what is happening?

Run with `--log-cli-level=INFO` for the deploy, create, run and delete lines with their URLs, and a line for each task's change of state while a run is in flight, such as `transform: RUNNING -> FAILED in 0:00:42 ValueError: no rows`. The URLs, followed by the task lines, are in the `lakeflow` section of each test report (for setup and teardown errors as well as failures, and for passing tests with `-rA`), and in JUnit XML as the `lakeflow_job_url` and `lakeflow_run_url` properties, beside `lakeflow_job_id`, `lakeflow_run_id`, `lakeflow_result_state`, `lakeflow_duration` and, for a failed run, `lakeflow_failing`, `lakeflow_params` and `lakeflow_task_params`. At the end of the run, a `lakeflow` block in the terminal summary lists the kept jobs and schemas and the runs cancelled at session end. See [logging and reports](configuration.md#logging-and-reports).

### Which clouds and runtimes are supported?

The plugin uses only the Jobs API, the Workspace API, the Unity Catalog schemas API and the CLI, which are the same on AWS, Azure and GCP. It was developed and tested against an Azure workspace with serverless jobs and classic job clusters in dedicated access mode.
