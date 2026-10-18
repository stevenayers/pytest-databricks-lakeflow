# Limitations and FAQ

## Limitations

- **Logs.** `TaskRun.logs` is only filled for Python script, wheel and JAR tasks, and holds only the last 5 MB. Notebook tasks have no logs; use `output` instead. Notebook output keeps only the first 5 MB. A truncated output or log raises a `UserWarning` naming the task, and `output_json` refuses truncated output with a `ValueError`. Databricks documents that on serverless compute the logs of one task can include output from other tasks in the same run. In our tests each task's logs held only its own output, but this is not guaranteed.
- **Resolved task parameters may be missing.** `TaskRun.parameters` comes from the `resolved_values` that `runs/get` returns with `include_resolved_values=true`. On the serverless workspace we tested, the API returned no resolved values for notebook or wheel tasks, so `parameters` was `None`. We do not know whether the workspace, the compute type or a platform setting decides this. `JobRun.parameters` comes from the run's job parameters and was always filled.
- **Task values need probes.** Task values are not available through the Jobs API, so they are read by probe tasks. Each probe costs 7 to 30 seconds of serverless compute, even when its task did not run.
- **Job code must read catalog and schema from job parameters.** Bundle variables are fixed at deploy time, and the plugin deploys once per session.
- **Single-task jobs keep only the target task.** On a shared job cluster a task can import libraries that other tasks declared, and on its own it loses them. Declare every library on the task that needs it.
- **Serverless auto-retry is off by default** in ephemeral jobs, for notebook, Python script, Python wheel and JAR tasks, so a test sees one attempt. Pass `disable_auto_optimization=False` to keep the platform's behaviour. When a task is retried, `TaskRun` describes the last attempt and its `attempts` holds every attempt.
- **The Databricks CLI is required.** Deploying is only possible through the CLI, so the plugin shells out to it. There is no pure-SDK mode.
- **One run at a time per ephemeral job.** Each copy is created with `max_concurrent_runs: 1`. Parallelism comes from separate tests, each with its own job.
- **`sync` and `none` strategies read `bundle validate` output**, which leaves library paths local and `${resources...}` references unresolved. Jobs affected by that are refused, with an error that says to use `deploy`.
- **Standard (shared) access mode** classic clusters were not tested. There, task values set from wheel or script tasks may be silently lost.
- **Names starting with `__pytest_`** are reserved for the plugin's tasks, environments, widgets and parameters.

## FAQ

### Why deploy the bundle instead of reading `databricks bundle validate`?

`validate` resolves notebook paths but leaves wheel and JAR paths pointing at local files, and does not resolve `${resources...}` references. A deploy builds and uploads the wheels and gives job settings in which every path is a workspace path, read back with `jobs.get`. The deploy happens once per session, and a deploy of an unchanged bundle takes seconds. If you deploy the target yourself, `Lakeflow(sync="none")` skips it, see [sync strategies](configuration.md#sync-strategies).

### Why do my tests never run the deployed job?

The deployed job keeps its schedule, notifications, `run_as` and bundle lock, and running it would mix test runs with real ones. Each test instead gets a copy with those removed and its dashboard and SQL alert subscriptions paused, named and tagged after the test, so runs are isolated and attributable and do not email subscribers. Jobs are cheap to create and delete.

The copy runs as you rather than as the deployed job's `run_as`. To exercise the permissions of that principal, opt in with `lakeflow.job(key, run_as="keep")`, see [Run as another identity](job-tests.md#run-as-another-identity).

### Why is my job parameter rejected?

The Jobs API accepts undeclared parameters silently, so a typo would pass unnoticed. The plugin checks `params` against the job's declared `parameters` and raises `LakeflowError` for anything undeclared. Declare the parameter on the job, or pass `allow_undeclared_params=True`.

### Why is every run 3 to 4 minutes slower than in the UI?

The job's `performance_target` is `STANDARD`. The plugin warns about it. Pass `performance_optimized=True` to run tests with `PERFORMANCE_OPTIMIZED`, or `job.run(performance_target="PERFORMANCE_OPTIMIZED")` to speed up one run without changing the job's settings. `run.effective_performance_target` and each task's `effective_performance_target` show which mode the run actually used.

### How do I clean up after a crashed session?

Ephemeral jobs are tagged `pytest` and `pytest_run_id`, and, when they can be found out, `pytest_ci_url`, `pytest_user`, `pytest_git_sha` and `pytest_git_branch`, so a leftover job can be traced to its CI run, person and commit from the Jobs list. `pytest --lakeflow-sweep` deletes your own jobs from other runs that are older than `sweep_hours`, 24 hours by default. `pytest --lakeflow-destroy` runs `bundle destroy` for the test target at the end of the session.

### What does the plugin leave in my workspace?

The deployed test target, the two helper notebooks under `helpers_path`, and any jobs kept by the keep rules. A kept job's tags (`pytest_run_id`, `pytest_nodeid`, `pytest_ci_url`, `pytest_user`, `pytest_git_sha`, `pytest_git_branch`) and description say which test, run, person and commit made it. See [How it works](how-it-works.md#what-is-left-in-the-workspace).

### Why did the CLI fail with `exit status 45` under xdist?

Several processes refreshed a user-to-machine (`databricks-cli`) OAuth token at the same time. The plugin retries building its client, but the CLI call that hit the race fails. Use a service principal with OAuth machine-to-machine credentials or a token in CI.

### Does it work alongside databricks-labs-pytester?

Yes. The plugin defines only the `lakeflow` fixture and does not import pytester, so the two can be installed together. `lakeflow.ws` gives you the `WorkspaceClient` without a second fixture.

### Can I use `Lakeflow` outside pytest?

No. It raises `LakeflowError` outside a pytest session, and outside a test. Jobs are tied to the running test for naming, tagging and teardown.

### How do I see what is happening?

Run with `--log-cli-level=INFO` for the deploy, create, run and delete lines with their URLs, and a line for each task's change of state while a run is in flight, such as `transform: RUNNING -> FAILED in 0:00:42 ValueError: no rows`. The task lines follow the URLs in the `lakeflow` section of the test report, which `-rA` shows for passing tests too. The URLs are also in the `lakeflow` section of each test report, for setup and teardown errors as well as failures, and in JUnit XML as the `lakeflow_job_url` and `lakeflow_run_url` properties, beside `lakeflow_job_id`, `lakeflow_run_id`, `lakeflow_result_state`, `lakeflow_duration` and, for a failed run, `lakeflow_failing`. At the end of the run, a `lakeflow` block in the terminal summary lists the kept jobs and schemas and the runs cancelled at session end. See [logging and reports](configuration.md#logging-and-reports).

### Which clouds and runtimes are supported?

The plugin uses only the Jobs API, the Workspace API, the Unity Catalog schemas API and the CLI, which are the same on AWS, Azure and GCP. It was developed and tested against an Azure workspace with serverless jobs and classic job clusters in dedicated access mode.
