---
name: pytest-databricks-lakeflow
description: Write pytest tests for Lakeflow Jobs defined in a Databricks Asset Bundle with the pytest-databricks-lakeflow plugin. Use when asked to test a bundle job or a single task of one, to assert on job runs, task outputs, logs, errors or task values, or to set up or configure the lakeflow fixture.
---

# Testing Lakeflow Jobs with pytest-databricks-lakeflow

The plugin deploys a bundle target once per session, then gives each test a throwaway copy of a job or of one task, runs it with chosen parameters, and deletes it. Full docs: https://stevenayers.github.io/pytest-databricks-lakeflow/ (machine-readable index: https://stevenayers.github.io/pytest-databricks-lakeflow/llms.txt).

## Before writing a test

1. Read the job in the bundle (`resources/*.yml` or wherever `include:` points). Note the resource key, the task keys, the declared `parameters`, and which tasks set or read task values.
2. Check `databricks.yml` has the target the tests use (default `test`). Recommend `mode: development` and a separate `workspace.root_path`.
3. Check the job code reads catalog and schema from job parameters, not bundle variables. If it does not, say so; per-test schemas cannot work otherwise.
4. Look for an existing `lakeflow` fixture override in `conftest.py`. Reuse it. Create one only if configuration is needed (another target, a `catalog`, a `profile`, a longer `timeout`).

## The API, and nothing else

Import from `pytest_databricks_lakeflow`: `Lakeflow`, `EphemeralJob`, `JobRun`, `TaskRun`, `TaskValues`, `TaskError`, `LakeflowError`, `LakeflowBundleError`, `LakeflowTimeout`, `LakeflowRunFailed`.

- `lakeflow` fixture (session scope) returns a `Lakeflow`. Override it to configure; all `Lakeflow` arguments are keyword-only: `bundle_root`, `target`, `profile`, `workspace_client`, `sync`, `timeout`, `catalog`, `helpers_path`, `helpers_compute`, `helpers_environment_version`, `disable_auto_optimization`, `performance_optimized`, `allow_undeclared_params`, `sweep_hours`, `keep`, `keep_failed`, `permissions`, `modify_settings`, `run_finished`. `permissions` is a list of `jobs.JobAccessControlRequest` or dicts such as `{"group_name": "data-eng", "permission_level": "CAN_VIEW"}`, applied as the `access_control_list` of every ephemeral job.
- `lakeflow.job(key, params=None, capture_task_values=None, name=None, run_as=None) -> EphemeralJob`. `capture_task_values` is `{task_key: [value_key, ...]}`. `run_as` is `None` (strip it, run as the caller), `"keep"` (the template's), a user's email, a service principal's application id, or a `jobs.JobRunAs`.
- `lakeflow.task("job_key.task_key", params=None, upstream_task_values=None, capture_task_values=None, run_as=None) -> EphemeralJob`. `upstream_task_values` is `{upstream_task_key: {value_key: value}}`; `capture_task_values` is a list.
- `lakeflow.schema(catalog=None)`: context manager yielding a temporary `catalog.schema` name. Needs `catalog`.
- `lakeflow.ws`: the `WorkspaceClient`. `lakeflow.template(key)`: a fresh `JobSettings` copy.
- `EphemeralJob.run(params=None, only=None, timeout=None, *, performance_target=None, queue=None) -> JobRun`. `only` takes task keys with `+key`, `key+`, `+key+`. `performance_target` (`"PERFORMANCE_OPTIMIZED"` or `"STANDARD"`) and `queue` (a bool) are keyword-only and apply to that run only. Also `job_id`, `url`, `runs`.
- `JobRun`: `succeeded`, `failed`, `raise_for_failure()` (returns the run, or raises `LakeflowRunFailed` whose `run` is the run), `tasks`, `task(key)`, `skipped_tasks`, `injected_tasks`, `result_state`, `termination_code`, `termination_message`, `duration`, `run_id`, `run_url`, `parameters` (job parameters by name, the run's value or the default, without the plugin's own). `repr(run)` adds `params={...}` when the run failed.
- `TaskRun`: `succeeded`, `ran`, `result_state`, `termination_code`, `termination_message`, `output`, `output_json`, `output_truncated`, `logs`, `log_lines`, `logs_truncated`, `error` (`type`, `message`, `trace`, `root_cause`, `code`), `raised(ExcType)`, `task_values`, `missing_task_values`, `duration`, `attempt`, `run_url`, `parameters` (resolved task parameters: a dict for notebook, SQL, run-job and named wheel parameters, a list for positional wheel, Python script, JAR, spark-submit and dbt tasks, else `None`; printed by `repr()` when the task has an error).
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

Tables in a temporary schema:

```python
def test_writes_rows(lakeflow: Lakeflow) -> None:
    with lakeflow.schema() as schema, lakeflow.job("my_example_job", params={"schema": schema}) as job:
        assert job.run().succeeded
        assert lakeflow.ws.tables.exists(f"{schema}.example_rows").table_exists
```

Use `@pytest.mark.parametrize` for input variations and `job.run(only=["+task"])` for a subset. Call `run()` twice on one job to compare runs.

## Rules

- Assert with plain `assert`. Put the run in the message: `assert run.succeeded, run`, or call `run.raise_for_failure()`.
- Use `run.succeeded`, never `run.result_state`, as the verdict.
- Pass only parameters the job declares, or the plugin raises `LakeflowError`. Names starting with `__pytest_` are reserved.
- `logs` exist only for Python script, wheel and JAR tasks. Notebook tasks have `output` instead.
- Stub values must be JSON-serialisable and under 49,152 characters each. Stub every upstream task whose values the task reads.
- Do not run the deployed jobs, call `jobs.run_now` yourself, or delete jobs in tests. The plugin owns the lifecycle.
- Keep tests that need a workspace separate from fast tests: they get the `lakeflow` marker automatically, so `pytest -m "not lakeflow"` skips them.

## Running

```bash
pytest tests/ --log-cli-level=INFO          # first run deploys the target, then runs each test
pytest tests/ -n 4                          # parallel, needs the xdist extra
pytest tests/ --lakeflow-keep               # keep jobs and schemas to inspect
pytest tests/ --lakeflow-sweep              # delete leftovers from earlier runs first
```

Kept jobs and schemas are listed in a `lakeflow` block at the end of the run. Every ephemeral job is tagged `pytest`, `pytest_run_id` and `pytest_nodeid`, plus `pytest_ci_url`, `pytest_user`, `pytest_git_sha` and `pytest_git_branch` when they can be resolved.

Needs the Databricks CLI v1.7.0 or later on `PATH`, credentials for the CLI and the SDK, and serverless jobs (or `helpers_compute="<job_cluster_key>"`).
