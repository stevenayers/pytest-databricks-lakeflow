# Logs and reports

Everything the plugin prints, logs and records, and where to find it.

## Log lines

The plugin logs to the `pytest_databricks_lakeflow` logger: INFO when it deploys, creates a job, starts and finishes a run, and deletes a job or drops a schema; WARNING when it keeps something or a cleanup call fails. Show the INFO lines with `pytest --log-cli-level=INFO`.

While `run()` or `RunHandle.wait()` waits, every poll logs a line at INFO for each task run whose state changed, the plugin's own tasks included. The state is the lifecycle state, such as `PENDING`, `QUEUED` or `RUNNING`, until the task run terminates, and its result state after that. A terminated task run adds its duration and the first line of its error:

```text
extract: PENDING
transform: PENDING
extract: PENDING -> SUCCESS in 0:00:31
transform: PENDING -> RUNNING
transform: RUNNING -> FAILED in 0:00:42 ValueError: no rows
```

A retried task run is labelled with its attempt, such as `transform (attempt 1): PENDING`. The output of each task run is read once, as soon as it has terminated, which supplies the error for the line and is reused when the `JobRun` is built.

## Live output

`pytest --lakeflow-live`, or `stream=True` on one `job.run()`, prints the start of each run, each state line and the end of the run straight to the terminal, prefixed `lakeflow: `, past pytest's output capture, so neither `--log-cli-level` nor `-s` is needed:

```text
lakeflow: Started run 77 of job 1001: https://<workspace host>/jobs/runs/77
lakeflow: transform: PENDING -> RUNNING
lakeflow: transform: RUNNING -> FAILED in 0:00:07 ValueError: no rows
lakeflow: Run 77 finished FAILED in 0:00:51: https://<workspace host>/jobs/runs/77
```

`job.run(stream=False)` keeps one run quiet under `--lakeflow-live`, and a run from `start()` streams when the flag is set. The log lines and the report section are the same either way, so with `--log-cli-level=INFO` as well each line shows twice. Under xdist a worker cannot write to the controller's terminal, so nothing is printed there and the lines are only logged. The start line leaves out the run page when the workspace has not reported it yet.

## JUnit XML properties

Each test's job and run coordinates are appended to `item.user_properties`, so they appear as JUnit XML properties:

| Property | Added | Value |
| --- | --- | --- |
| `lakeflow_job_url` | when the job is created | the job's page in the workspace |
| `lakeflow_job_id` | when the job is created | the job's id |
| `lakeflow_pipeline_url` | when a pipeline copy is created | the copy's page in the workspace |
| `lakeflow_pipeline_id` | when a pipeline copy is created | the copy's id |
| `lakeflow_run_url` | when a run starts, also with `start()` and no `wait()` | the run's page in the workspace |
| `lakeflow_run_id` | when a run finishes | the run's id |
| `lakeflow_result_state` | when a run finishes | the run's result state, such as `SUCCESS` or `FAILED` |
| `lakeflow_duration` | when a run finishes | the run's duration in seconds, with three decimals |
| `lakeflow_failing` | when a run fails | each failing task as `key: error`, with the error's first line, joined by `; ` |
| `lakeflow_params` | when a run fails and has job parameters | the run's job parameters as one line of JSON with sorted keys |
| `lakeflow_task_params` | when a run fails and a failing task reported resolved parameters | one JSON object of each such failing task's parameters, by task key |
| `lakeflow_debug_notebook` | when `debug_notebook()` or `--lakeflow-debug-notebook` writes a notebook | the notebook's page in the workspace |

A job that runs more than once adds a set of run properties for each run. `lakeflow_failing`, `lakeflow_params` and `lakeflow_task_params` are added for failed runs only, also when the test expects the failure. Each parameter value is cut to its first line, at most 200 characters, and the parameters the plugin injects are left out.

## The report section

The same lines are attached as a `lakeflow` section to the setup, call and teardown reports of the test, so a job created in a fixture whose setup fails still shows its URL in the error. The task state lines of every run follow the property lines, recorded as they are logged, so a run that raises `LakeflowTimeout` still shows what its tasks did before the deadline. The section is shown for a failed test, and for every test with `-rA`.

In the section of a failed report, each failing task with an exported notebook adds the first code line of the cell that failed and the last 50 lines of the notebook's `stdout`:

```text
transform: notebook.cells[2] failed: dbutils.jobs.taskValues.set(key="stage", value="validation")
transform: notebook stdout:
  transform: checking forbidden against /Volumes/example/raw/landing
  transform: 7 rows expected
```

## The terminal summary

At the end of the session, a `lakeflow` block in the terminal summary lists every kept job with its URL, every kept pipeline copy and schema, every debug notebook written with its test and task, and the number of runs cancelled at session end, followed by a reminder of `--lakeflow-sweep` when jobs were kept:

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

A debug notebook whose parameters did not all fit a text widget adds them to its line, such as `; widgets cut at 2048 bytes: 'payload'`. The block does not depend on `--log-cli-level`. Under xdist the workers report what they kept to the controller, which prints one summary. A session that kept nothing, wrote no debug notebook, cancelled nothing and ran no sweep dry run prints no block. A sweep dry run adds its list at the top of the block:

```text
=================================== lakeflow ===================================
lakeflow: sweep dry run deleted nothing; it would delete:
  job https://adb-123.azuredatabricks.net/jobs/1001
  pipeline https://adb-123.azuredatabricks.net/pipelines/0b9e2c1e-5d4f-4c3a-9b8e-7f6a5d4c3b2a
  schema dev_sandbox.pytest_ab12cd34_9f0e1d2c
  notebook /Users/someone@example.com/.pytest-databricks-lakeflow/setup_3f9a1c07b2d4
```

## Report directory

`pytest --lakeflow-report-dir=build/lakeflow` keeps a record of every run that outlives the job. The path is resolved against the directory pytest was invoked from, as `--junitxml` is. Each test that waited for at least one run, or whose run the plugin cancelled, gets a directory of its own, named after its node id with every character outside letters, digits, `.`, `_` and `-` replaced by `_`, cut to 80 characters, plus `-` and the first 8 hex characters of the node id's SHA-256:

```text
build/lakeflow/
  test_jobs.py__test_job-3f9a2c1b/
    report.json            # the test's outcome and one entry per run
    77/load.log            # raw logs, by run id and task key
    77/transform.html      # an exported notebook, which opens in a browser
```

`report.json` holds:

- `nodeid`, and `outcome` per phase, such as `{"setup": "passed", "call": "failed", "teardown": "passed"}`.
- `debug_notebooks`: the `task_key`, `run_id`, `path` and `url` of each debug notebook the test wrote, empty when it wrote none.
- `runs`: one entry per `JobRun` the test waited for, in the order they finished, and one for each run the plugin cancelled: a run that hit its `timeout` and raised `LakeflowTimeout`, and a run from `start()` still active when its job closed. After the cancel the plugin polls such a run until it has ended, for at most 60 seconds, and lists it with `result_state` `"CANCELED"` and `succeeded` false; it is not appended to `job.runs` nor passed to `run_finished`. A run that has not ended after 60 seconds logs a warning and is left out.

Each entry is [`run.as_dict()`](api.md#jobrun) with three more keys: `job_url`, `params` (the job's `params`) and `settings` (the copy's `JobSettings` as the Jobs API's JSON). `as_dict()` keeps every field of the run and its tasks:

- Durations are in seconds, as `duration_seconds` and a `timing` of `queue_seconds`, `setup_seconds`, `execution_seconds` and `cleanup_seconds`.
- `error` is a dict of `type`, `message`, `trace`, `root_cause` and `code`; `missing_task_value_reasons` says why each of `missing_task_values` is missing.
- Logs, up to 5 MB per task, and the exported notebook HTML are written as files, named in `logs_file` and `notebook_file` relative to the test's directory, or `null` when the task has none. An earlier attempt of a retried task gets `.attempt-<n>` before the suffix, such as `77/load.attempt-0.log`, an iteration of a for-each task `.iteration-<n>`, counting from 0, and the tasks of a `child_run` are under the child run's id.
- A for-each task's `iterations` is a list of its iterations, each serialised as a task, and its `for_each_stats` a dict with `errors` as a list of dicts, or `null`.
- A dbt task's `dbt_output` keeps its `artifacts_link` and leaves out `artifacts_headers`, which can carry the signature of the link, and its `dbt` keeps `artifacts_dir`, `results`, `failed_models` and `all_passed` and leaves out `run_results` and `manifest`, which stay in `artifacts_dir`.

A run made in a module- or session-scoped fixture belongs to the test that was running when the fixture started it: the first test to use the fixture for a run made while setting it up, and the last test of the module or session for one made while tearing it down. A run the plugin cancels when its job closes is listed by the test running then.

Only the notebooks that `notebook_export` selects are exported. A test without a run writes nothing, and without the flag nothing is written. A report that cannot be serialised or written logs a warning and never fails the test. At the start of each session, before any test runs, the plugin deletes the test directories an earlier session left in the directory, so a rerun never mixes stale output with its own. Only directories named as above that hold a `report.json` are deleted.
