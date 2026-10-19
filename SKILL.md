---
name: pytest-databricks-lakeflow
description: Write pytest tests for Lakeflow Jobs defined in a Databricks Asset Bundle with the pytest-databricks-lakeflow plugin. Use when asked to test a bundle job or a single task of one, to assert on job runs, task outputs, logs, errors or task values, or to set up or configure the lakeflow fixture.
---

# Testing Lakeflow Jobs with pytest-databricks-lakeflow

The plugin deploys a bundle target once per session, then gives each test a throwaway copy of a job or of one task, runs it with chosen parameters, and deletes it. Full docs: https://stevenayers.github.io/pytest-databricks-lakeflow/ (machine-readable index: https://stevenayers.github.io/pytest-databricks-lakeflow/llms.txt).

## Before writing a test

1. Read the job in the bundle (`resources/*.yml` or wherever `include:` points). Note the resource key, the task keys, the declared `parameters`, and which tasks set or read task values.
2. Check `databricks.yml` has the target the tests use (default `test`). Recommend `mode: development` and a separate `workspace.root_path`. If several branches or people share the workspace, recommend the per-branch recipe from the docs ("Isolating test deployments"): a `lakeflow_suffix` variable in `root_path`, `experimental.skip_artifact_cleanup: true`, the sanitised branch name passed as `Lakeflow(variables={"lakeflow_suffix": ...})` from the `lakeflow` fixture in `conftest.py`, and `pytest --lakeflow-destroy` at the end of the pipeline.
3. Check the job code reads catalog and schema from job parameters, not bundle variables. If it does not, say so; per-test schemas cannot work otherwise.
4. Look for an existing `lakeflow` fixture override in `conftest.py`. Reuse it. Create one only if configuration is needed (another target, a `catalog`, a `profile`, a longer `timeout`).

## The API, and nothing else

Import from `pytest_databricks_lakeflow`: `Lakeflow`, `EphemeralJob`, `JobRun`, `TaskRun`, `TaskValues`, `TaskError`, `Timing`, `NotebookView`, `NotebookCell`, `RunHandle`, `SqlResult`, `LakeflowError`, `LakeflowBundleError`, `LakeflowTimeout`, `LakeflowRunFailed`.

- `lakeflow` fixture (session scope) returns a `Lakeflow`. Override it to configure; all `Lakeflow` arguments are keyword-only: `bundle_root`, `target`, `profile`, `workspace_client`, `sync`, `variables`, `deploy_flags`, `timeout`, `poll_interval`, `catalog`, `helpers_path`, `helpers_compute`, `helpers_environment_version`, `disable_auto_optimization`, `performance_optimized`, `side_effects`, `environment_version`, `budget_policy_id`, `existing_cluster_id`, `single_node`, `allow_undeclared_params`, `test_marker`, `notebook_export`, `sweep_hours`, `keep`, `keep_failed`, `strict_artifacts`, `permissions`, `modify_settings`, `run_finished`. `strict_artifacts=True` raises `LakeflowError` on the first template lookup when the bundle has a wheel artifact (`type: whl`, or a `build` command and no `type`) without `dynamic_version: true`; by default that is one `UserWarning` per session ("Artifacts [...] have no dynamic_version: true. ..."), because serverless caches wheels by file name. `timeout` also sets each job's `timeout_seconds` to the timeout plus 60 seconds, unless the template is stricter; `run(timeout=...)` longer than that raises `LakeflowError`. `poll_interval` is the `timedelta` between two `get_run` polls of a running run (default two seconds; zero or less raises `ValueError`). `permissions` is a list of `jobs.JobAccessControlRequest` or dicts such as `{"group_name": "data-eng", "permission_level": "CAN_VIEW"}`, applied as the `access_control_list` of every ephemeral job. `deploy_flags` is a list of extra `bundle deploy` flags such as `["--fail-on-active-runs"]` (used only with `sync="deploy"`; not `-t`, `--target`, `-e`, `--environment`, `-p`, `--profile`, `--var` or `--auto-approve`, which raise `ValueError`). `side_effects` is `"neutralise"` (default: pause dashboard and SQL alert subscriptions in the copy) or `"keep"`; alert tasks and Power BI tasks are left as they are in both, so disable them with `disable=`. The plugin needs databricks-sdk 0.100 or later, the first release whose `JobSettings` keeps `alert_task`. `notebook_export` is `"failed"` (default: export the notebook of each notebook task that ran and did not succeed into `TaskRun.notebook`), `"always"` (every notebook task that ran) or `"never"`; anything else raises `ValueError`. `test_marker=True` declares a `__pytest_test_id` job parameter on every copy with the test node id as its default (also when `name=` is passed), so job code can read it with `dbutils.widgets.get("__pytest_test_id")` or `{{job.parameters.__pytest_test_id}}`; it is off by default and is never in `run_now` parameters. `variables` is a mapping of bundle variable names to values, passed as `--var name=value` to every `bundle` command including `destroy`; instances with different `variables` deploy separately. Setup raises `LakeflowError` when the bundle target's `workspace.host` differs from the workspace client's host. The plugin does not read the target from the environment: use `target=os.environ.get("LAKEFLOW_TARGET", "test")`.
- `lakeflow.job(key, params=None, capture_task_values=None, name=None, run_as=None, disable=None, *, force_conditions=None, children="warn", setup=None, teardown=None, extra_dependencies=None, environments=None) -> EphemeralJob`. `setup` and `teardown` are plain `def` functions taking any of `spark`, `dbutils`, `params` (the job's declared parameters for the run, as strings; undeclared ones are not passed); they run on the job's compute as the injected tasks `__pytest_setup` (before every task without `depends_on`) and `__pytest_teardown` (after every leaf, `run_if` `ALL_DONE`) on every run, `only` included. They are shipped as source, so they must be self-contained (imports inside, no closures, no decorators, no helpers from the test module; annotations are not evaluated); a lambda, closure, decorated or REPL-defined function or other parameter raises `LakeflowError`. A returned value becomes the task's `output` as JSON. A failed setup fails the run and `repr(run)` names it first; a failed teardown only logs a warning, so assert `run.teardown.succeeded` if it matters. `extra_dependencies` is a list of PyPI requirements or `/Workspace` or `/Volumes` wheel paths appended to the `dependencies` of every environment taken from the template and, as `pypi` or `whl` libraries, to every notebook, Python script and Python wheel task on a cluster (nested `for_each_task` tasks included); the helper environment `__pytest_serverless` never gets them, serverless notebook tasks without `environment_key` are left unchanged, and a local path such as `./dist/*.whl[test]` raises `LakeflowError` before anything is created. `environments` is `{environment_key: {spec_field: value}}` with `compute.Environment` fields (`environment_version`, `base_environment`, `dependencies`, ...): for a key the copy has, each field replaces the template's (so `dependencies` replaces the list, `extra_dependencies` included, and `environment_version` beats `Lakeflow(environment_version=...)`); a new key appends an environment holding only the fields passed (no `Lakeflow(environment_version=...)`, no `extra_dependencies`) that no task uses unless `modify_settings` sets a task's `environment_key`. A key starting with `__pytest_`, an override that is not a mapping, an unknown field such as `version`, both `environment_version` and `base_environment` on one spec, or more than 10 environments (helper environment included) raises `LakeflowError` before anything is created. A Run Job task keeps its deployed `job_id`, so the copy runs the deployed child job: `children="warn"` emits a `UserWarning` per such task that is not disabled (nor inside a disabled `for_each_task` loop), `children="refuse"` raises `LakeflowError` before creating anything; disable the task or its loop to skip the child. `disable` is a list of task keys to disable in the copy; they end `DISABLED` in `run.disabled_tasks`, not `run.skipped_tasks`. `force_conditions` is `{condition_task_key: True | False}`: the copy's condition task compares constants (`true == true` or `true == false`), so every run takes that branch while the upstream tasks still run; a key that is not an enabled condition task, or an outcome that is not a bool, raises `LakeflowError`. `capture_task_values` is `{task_key: [value_key, ...]}`. `run_as` is `None` (strip it, run as the caller), `"keep"` (the template's), a user's email, a service principal's application id, or a `jobs.JobRunAs`.
- `lakeflow.task("job_key.task_key", params=None, upstream_task_values=None, capture_task_values=None, run_as=None, *, upstream_from=None, children="warn", setup=None, teardown=None, extra_dependencies=None, environments=None) -> EphemeralJob`. `children`, `setup`, `teardown`, `extra_dependencies` and `environments` as for `job()`; setup runs before the stubs. `upstream_task_values` is `{upstream_task_key: {value_key: value}}`; `capture_task_values` is a list. `upstream_from` is a `JobRun` of the same job that captured every upstream task's values (`capture_task_values={upstream_task_key: [...]}` on `job()`); it stubs each task the target depends on with them, and an `upstream_task_values` entry replaces the values for that task.
- `lakeflow.schema(catalog=None)`: context manager yielding a temporary `catalog.schema` name. Needs `catalog`.
- `lakeflow.ws`: the `WorkspaceClient`. `lakeflow.template(key)`: a fresh `JobSettings` copy.
- `EphemeralJob.run(params=None, only=None, timeout=None, *, performance_target=None, queue=None, full_refresh=None, full_refresh_selection=None, refresh_selection=None) -> JobRun`. `only` takes task keys with `+key`, `key+`, `+key+`. `performance_target` (`"PERFORMANCE_OPTIMIZED"` or `"STANDARD"`) and `queue` (a bool) are keyword-only and apply to that run only. `full_refresh` (a bool), `full_refresh_selection` and `refresh_selection` (lists of tables, databricks-sdk 0.110.0 or later) are keyword-only, go to the run's pipeline tasks as `pipeline_params`, and raise `LakeflowError` on a job with no pipeline task; the pipeline is the deployed one, not a copy. Also `job_id`, `url`, `runs`. On timeout `LakeflowTimeout` cancels the run and its message ends with `Tasks when cancelled: extract SUCCESS, transform RUNNING, ...`.
- `EphemeralJob.start(params=None, only=None, *, performance_target=None, queue=None, full_refresh=None, full_refresh_selection=None, refresh_selection=None) -> RunHandle` starts a run as `run()` does without waiting; `run()` is `start().wait(timeout)`. `RunHandle` has `job`, `run_id`, `url` (known once `start()` returns), `wait(timeout=None) -> JobRun` (appends to `job.runs`; a second call returns the same `JobRun` and does not call `run_finished` again; after the job closed it raises `LakeflowError`) and `cancel()`. A copy allows one run at a time, so to run jobs side by side start runs of separate `lakeflow.job(...)` copies, then wait for each. A run never waited for is cancelled when its job closes or at session end.
- `JobRun`: `succeeded`, `failed`, `raise_for_failure()` (returns the run, or raises `LakeflowRunFailed` whose `run` is the run), `tasks`, `task(key)` (a `KeyError` says `Skipped by only` or `Disabled in the job`), `executed_tasks` (sorted keys of `tasks` that ran), `branch_taken(key)` (a condition task's `condition_outcome`; `LakeflowError` for any other task), `skipped_tasks` (left out by `only`), `disabled_tasks` (disabled in the bundle or with `disable`), `injected_tasks`, `setup` and `teardown` (the `TaskRun` of the setup or teardown task, or `None`), `result_state`, `termination_code`, `termination_message`, `duration`, `run_id`, `run_url`, `timed_out` (the run or any task ended `TIMEDOUT`), `parameters` (job parameters by name, the run's value or the default, without the plugin's own), `timing` (a `Timing` with `queue`, `setup`, `execution`, `cleanup` as `timedelta` and `total`; zero when not reported, and zero on the parent of a multi-task run, so read it on tasks), `effective_performance_target` (`"STANDARD"`, `"PERFORMANCE_OPTIMIZED"` or `None`), `job_key` and `job_id` (the bundle key and the ephemeral job's id; for a `child_run`, the child job's). `repr(run)` adds `params={...}` when the run failed.
- `TaskRun`: `succeeded`, `ran`, `excluded` (`EXCLUDED` by `run_if` or if/else), `upstream_failed`, `disabled`, `cancelled` (each an exact match of `UPSTREAM_FAILED`, `DISABLED`, `CANCELED`), `condition_outcome` (`True`, `False`, or `None` for a task that is not an if/else condition task), `result_state`, `termination_code`, `termination_message`, `output` (falls back to a clean room notebook task's output), `output_json`, `output_truncated`, `sql_output`, `alert_output`, `dbt_output`, `run_job_output`, `dashboard_output`, `clean_rooms_notebook_output` (the SDK objects from `runs/get-output`, each `None` for other task types; `sql_output` holds no query rows), `sql_result` (a SQL file task's rows as a `SqlResult`, read from the raw `runs/get-output` field `sql_output.file_output` that databricks-sdk does not model: `columns`, `types`, `rows` (dicts by column name, values as strings, NULL as `None`), `first_row` (raises `LakeflowError` "returned no rows" when empty), `truncated` (cut at 1,000 rows), `warehouse_id`, `query_text` (`:name` markers kept); `None` for saved query, alert and dashboard tasks, a task that did not run, or an unreadable output), `alert_state` (`"TRIGGERED"`, `"OK"`, `"UNKNOWN"` or `None`; an alert task succeeds whether or not it triggered, so assert on this), `child_run_id` (the run id a run-job task triggered, from `run_job_output.run_id`; `None` for other tasks), `child_run` (that run as a `JobRun`: `job_id` is the child job's, `job_key` its bundle key when the bundle deployed it, else `str(job_id)`; nested for a child's own run-job tasks; a task disabled in the child job is in its `disabled_tasks`; each of `attempts` has its own; `None` for other tasks, when the task did not run or the run could not be read), `logs`, `log_lines`, `logs_truncated`, `notebook` (a `NotebookView` read with `jobs.export_run` for the notebook tasks `notebook_export` selects, else `None`: `cells`, each a `NotebookCell` with `index`, `code`, `output` (text, HTML and ANSI removed), `output_html`, `error` (`"Name: message"` or `None`) and `stdout`; `stdout` (what every cell wrote to standard output in order, what `print()` produced; stderr, displayed results and the `dbutils.notebook.exit()` value are only in the cells' `output`); `failed_cell` (the first cell that raised, or `None`); `html` (the export, unchanged); `save(path)`; an unreadable export gives empty `cells` and a `UserWarning`), `error` (`type`, `message`, `trace`, `root_cause`, `code`), `raised(ExcType)`, `task_values`, `missing_task_values`, `duration`, `timing`, `effective_performance_target`, `attempt`, `attempts` (one `TaskRun` per attempt in order, the last equal to the task apart from `attempts`), `timed_out` (ended `TIMEDOUT`), `run_url`, `parameters` (resolved task parameters: a dict for notebook, SQL, run-job and named wheel parameters, a list for positional wheel, Python script, JAR, spark-submit and dbt tasks, else `None`, or when the task did not run or its task run could not be read; printed by `repr()` when the task has an error).
- Flags: `--lakeflow-keep`, `--lakeflow-keep-failed`, `--no-lakeflow-keep-failed`, `--lakeflow-sweep`, `--lakeflow-destroy`. There are no ini options and no other fixtures.

Do not invent arguments, fixtures, markers or flags beyond these. Check the signature in the API reference when unsure.

## Patterns

Job test:

```python
from pytest_databricks_lakeflow import Lakeflow


def test_job_succeeds(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job", params={"param1": "hello", "param2": "world"}) as job:
        run = job.run()
        assert run.succeeded, run  # or: run.raise_for_failure()
        assert run.task("transform").output == "hello world"
```

Expected failure:

```python
def test_rejects_forbidden_input(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job", params={"param1": "forbidden", "param2": "world"}) as job:
        run = job.run()
        assert run.failed
        assert run.task("transform").raised(PermissionError)
        assert run.task("load").result_state == "UPSTREAM_FAILED"
```

Single task with stubbed upstream values and captured values:

```python
def test_transform(lakeflow: Lakeflow) -> None:
    with lakeflow.task(
        "my_example_job.transform",
        params={"param1": "hello", "param2": "world"},
        upstream_task_values={"extract": {"row_count": 42, "source_path": "/Volumes/x/y/z"}},
        capture_task_values=["rows_written"],
    ) as task:
        run = task.run()
        assert run.succeeded, run
        assert run.task("transform").task_values["rows_written"] == 42
```

Replay one task with the upstream values a full run captured:

```python
def test_replay_transform(lakeflow: Lakeflow) -> None:
    first = lakeflow.job("my_example_job", capture_task_values={"extract": ["row_count", "source_path"]}).run()
    with lakeflow.task("my_example_job.transform", upstream_from=first) as task:
        run = task.run()
        assert run.succeeded, run
```

Tables in a temporary schema:

```python
def test_writes_rows(lakeflow: Lakeflow) -> None:
    with lakeflow.schema() as schema, lakeflow.job("my_example_job", params={"schema": schema}) as job:
        assert job.run().succeeded
        assert lakeflow.ws.tables.exists(f"{schema}.example_rows").table_exists
```

Seed data before the job runs, on the job's own compute:

```python
def seed_orders(spark, params):
    spark.sql(f"CREATE OR REPLACE TABLE {params['schema']}.seed_orders AS SELECT 1 AS id, 'a' AS sku")


def test_reads_seeded_orders(lakeflow: Lakeflow) -> None:
    with lakeflow.schema() as schema:
        with lakeflow.job("my_example_job", params={"schema": schema}, setup=seed_orders) as job:
            run = job.run()
            assert run.succeeded, run
            assert run.setup.succeeded
```

Use `@pytest.mark.parametrize` for input variations and `job.run(only=["+task"])` for a subset. Call `run()` twice on one job to compare runs.

## Rules

- Assert with plain `assert`. Put the run in the message: `assert run.succeeded, run`, or call `run.raise_for_failure()`.
- Use `run.succeeded`, never `run.result_state`, as the verdict.
- Pass only parameters the job declares, or the plugin raises `LakeflowError`. Names starting with `__pytest_` are reserved.
- `logs` exist only for Python script, wheel and JAR tasks. Notebook tasks have `output` (the `dbutils.notebook.exit()` value) and, when they ran and did not succeed, `notebook.stdout` with what they printed; pass `Lakeflow(notebook_export="always")` to assert on what a passing notebook printed.
- Stub values must be JSON-serialisable and under 49,152 characters each. Stub every upstream task whose values the task reads.
- Do not run the deployed jobs, call `jobs.run_now` yourself, or delete jobs in tests. The plugin owns the lifecycle.
- Keep tests that need a workspace separate from fast tests: they get the `lakeflow` marker automatically, so `pytest -m "not lakeflow"` skips them.

## Running

```bash
pytest tests/ --log-cli-level=INFO          # deploys the target, runs each test, logs each task's state changes
pytest tests/ -n 4                          # parallel, needs the xdist extra
pytest tests/ --lakeflow-keep               # keep jobs and schemas to inspect
pytest tests/ --lakeflow-sweep              # delete leftovers from earlier runs first
```

While a run is in flight, each task's change of state is logged at INFO, such as `transform: RUNNING -> FAILED in 0:00:42 ValueError: no rows`, and the same lines follow the job and run URLs in the test's `lakeflow` report section, shown for failures and with `-rA`. A failed run also reports its job parameters and its failing tasks' resolved parameters, as `lakeflow_params` and `lakeflow_task_params` in that section and in JUnit XML, and as `params:` lines in the `raise_for_failure()` message. Kept jobs and schemas are listed in a `lakeflow` block at the end of the run. Every ephemeral job is tagged `pytest`, `pytest_run_id` and `pytest_nodeid`, plus `pytest_ci_url`, `pytest_user`, `pytest_git_sha` and `pytest_git_branch` when they can be resolved.

Needs the Databricks CLI v1.7.0 or later on `PATH`, credentials for the CLI and the SDK, and serverless jobs (or `helpers_compute="<job_cluster_key>"`).
