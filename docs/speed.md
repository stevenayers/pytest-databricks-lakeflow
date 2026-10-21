# Speed and cost

Where a session's time and money go, and the three changes that cut both. The numbers are from one Azure workspace in October 2026, read from the Jobs API and the billing system tables; measure your own with the tools at the end.

## Where the time goes

A session has one fixed cost and then a cost per test:

- **The deploy, once.** `bundle validate`, `bundle deploy` and `bundle summary` run one time per bundle target, however many tests and xdist workers there are. The CLI has used its direct deployment engine, without Terraform, as the only engine since 1.20.0, and an unchanged bundle deploys in seconds; a wheel build adds whatever your build takes.
- **Each copy: create, run, delete.** The plugin's own work between one run ending and the next starting, the job delete, the next `jobs.create` and `run_now`, is 3 to 6 seconds. Polling every 2 seconds adds at most one interval per run.
- **Each serverless task: a floor of about 30 seconds.** A serverless notebook task that does nothing takes 28 to 32 seconds end to end. Every task in the copy pays something like it, and so does every task the plugin adds: a stub, a probe, or a `setup=` or `teardown=` step that runs as a task. A SQL step that runs as a task then runs its statements through `spark.sql` at about 1.5 seconds each.
- **Each setup and teardown step over Databricks Connect: seconds.** By default `setup=`, `teardown=` and `once()` steps run from the test process over Databricks Connect. The first step of a `Lakeflow` waits 2.4 to 2.7 seconds for a serverless session, and each statement then takes about 0.45 seconds. On Python 3.13 or later, where no databricks-connect release installs, they run as tasks, see [Run a step as a task](recipes/data.md#run-a-step-as-a-task).
- **Standard performance mode: 4 to 6 minutes per task.** The default `performance_target` of `PERFORMANCE_OPTIMIZED` starts tasks from a warm pool; `STANDARD` waits for compute and pays less for it.

In the suite that prompted this page, seven tests took 24 minutes of job run time. Every test recreated the same 47 fixture statements inside its copy: 72 seconds per test, 40% of the whole, and the reason the tests could not run at the same time.

## Three rules

1. **Fixtures every test reads are made once.** Put them in [`lakeflow.once()`](recipes/data.md#seed-once-for-the-whole-session) in a session fixture. The step runs one time for the session, whatever the number of xdist workers, and its teardown runs when the session ends. `setup=` on `job()` is for data one test owns, as `lakeflow.schema()` is.
2. **One worker per test.** With the fixtures made once, the tests are independent, so `pytest -n <tests>` runs them all at the same time and the session takes as long as its longest test plus the deploy and the step. Tests that share one run keep their `xdist_group`, see [Share one run between tests](recipes/runs.md#share-one-run-between-tests).
3. **Choose credentials that parallel workers can share.** A `databricks auth login` profile refreshes one cached token for every client each worker builds, and refreshes from several workers at once fail with `exit status 45`. On Azure, run the session with `DATABRICKS_AUTH_TYPE=azure-cli`: you stay yourself, and the workers never share the Databricks CLI's token cache, see [How do I run tests in parallel with my own login?](faq.md#how-do-i-run-tests-in-parallel-with-my-own-login). Elsewhere, use a service principal, or keep to `lakeflow.ws` rather than your own `WorkspaceClient()` so that each worker refreshes once.

Applied to that suite: about 8 minutes instead of 24, and a third fewer serverless task-minutes.

## How many workers

The Jobs API limits are per workspace and far above what a test suite needs: 100 `runs/get` requests per second, 20 `jobs/create`, 20 `runs/run-now`, 20 `runs/get-output`, and 2,000 tasks running at once. Sixteen workers polling every 2 seconds use 8 `runs/get` requests per second. When a run ends the plugin reads every task's output in parallel, with `16 // <worker count>` threads per worker so that every worker finishing at once stays under the `get-output` limit, and the SDK retries any 429 for up to five minutes. In practice the limit is your serverless quota and the number of tests: one worker per test is the most that helps.

## Performance modes and what they cost

Serverless jobs are billed per DBU, at a list price that depends on the region and the cloud, and a task consumes DBUs for the minutes it runs. Two levers follow:

- **Fewer task-minutes.** Every task the plugin adds is a 30 second task before its own work. Make shared fixtures once, keep setup and teardown steps over Databricks Connect, capture task values only where a test asserts on them, and skip `teardown=` for tables a temporary schema drops anyway.
- **Standard mode for runs that can wait.** Databricks bills both modes on the same SKU and documents standard mode as consuming fewer DBUs, up to 70% fewer, for 4 to 6 minutes of start-up per task. `pytest --lakeflow-performance-target=STANDARD` switches a whole session, so a nightly run can take it while the runs people wait for keep the default, see [Choose the performance mode](recipes/compute.md#choose-the-performance-mode).

Steps over Databricks Connect bill on the interactive serverless SKU, not the jobs one: in one Azure region it listed at USD 1.00 per DBU, against USD 0.50 for serverless jobs. A step over Connect skips the 30 seconds a task spends starting, so it should use far fewer DBUs, but the net effect on the bill has not been measured. `run_as_task=True` keeps every step on the jobs SKU.

The environment cache is not a lever: the setup phase of a serverless task was 2 to 4 seconds with a freshly uploaded `dynamic_version` wheel, and API calls are not billed.

## Measuring

- `run.task(key).timing` splits each task's time into `queue`, `setup`, `execution` and `cleanup`, and `run.effective_performance_target` says which mode the platform used. [Record run timings](recipes/compute.md#record-run-timings) appends them to a CSV per session.
- Every copy and every `once()` job carries the `pytest`, `pytest_run_id` and `pytest_nodeid` tags, which reach `custom_tags` in the billing system table, so a session can be priced once its rows arrive, about three hours later. Steps over Databricks Connect are not jobs, so these rows leave them out: they are billed to the identity that ran the tests, on the interactive serverless SKU. For the jobs of a session:

```sql
SELECT custom_tags['pytest_nodeid'] AS test, usage_metadata.job_run_id, sku_name,
       round(sum(usage_quantity), 4) AS dbus
FROM system.billing.usage
WHERE usage_date >= current_date() - INTERVAL 2 DAYS
  AND custom_tags['pytest_run_id'] = '<run id>'
GROUP BY ALL
ORDER BY dbus DESC;
```

The run id is `lakeflow.run_id`, or `GITHUB_RUN_ID` and `BUILD_BUILDID` in CI. Join `system.billing.list_prices` on `sku_name` for a price.

- On a Mac, run a long local session under `caffeinate -i pytest ...`. pytest measures the session with a clock that does not advance while the laptop sleeps, so the time it prints is shorter than the time you waited, while the runs go on in the workspace.
