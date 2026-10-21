# Test data

Give each test its own tables, seed them before the run, and clean up afterwards, without a seed task in the bundle.

## Give each test its own schema

`lakeflow.schema()` creates a schema named `<catalog>.pytest_<run id>_<random>` and drops it, with everything in it, when the block ends. It needs a catalog from `Lakeflow(catalog=...)` or `schema(catalog=...)`, and the job must read the schema from a job parameter:

```python
def test_writes_table(lakeflow: Lakeflow) -> None:
    with lakeflow.schema() as schema, lakeflow.job("my_example_job", params={"schema": schema}) as job:
        run = job.run()
        assert run.succeeded, run
        assert lakeflow.ws.tables.exists(f"{schema}.example_rows").table_exists
```

- The schema is dropped even when the block failed, unless `Lakeflow(keep=True)` or `--lakeflow-keep` is set. Kept schemas are listed in the `lakeflow` block at the end of the session, and `--lakeflow-sweep --lakeflow-sweep-schemas=<catalog>` drops leftovers.
- The schema carries `pytest_git_branch` and, with `--lakeflow-keep-ttl`, `pytest_expires` as schema properties.
- Tables the test asserts on belong here, because the drop cleans them up. Use a teardown step for side effects outside the schema.

## Seed once for the whole session

`lakeflow.once()` runs a setup step one time per session, for every test and every xdist worker, and its teardown when the session ends. It is for data that every test reads and none changes: the source tables behind a job's config, a schema the jobs write into, landing files in a volume. Call it from a session fixture:

```python
import pytest

from pytest_databricks_lakeflow import Lakeflow

SOURCES = [
    "CREATE SCHEMA IF NOT EXISTS ${schema}",
    "CREATE OR REPLACE TABLE ${schema}.orders AS SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, sku)",
]


@pytest.fixture(scope="session")
def sources(lakeflow: Lakeflow) -> None:
    step = lakeflow.once(
        "sources", setup=SOURCES, teardown=["DROP SCHEMA ${schema} CASCADE"], params={"schema": "dev_sandbox.sources"}
    )
    assert step.output_json == {"statements": 2}


def test_job_reads_the_sources(lakeflow: Lakeflow, sources: None) -> None:
    with lakeflow.job("my_example_job", params={"schema": "dev_sandbox.sources"}) as job:
        assert job.run().succeeded
```

- `setup=` and `teardown=` take the same forms as on `job()`: a function taking any of `spark`, `dbutils` and `params`, a list of SQL statements rendered from `params`, or a `Path` to a `.py` notebook file. A `${name}` that `params` does not hold raises `LakeflowError` before anything runs.
- By default the step runs over Databricks Connect, from whichever process asks for it first, and no job is made. The other xdist workers wait for it and read its outcome, and a second call with the same name, steps and parameters anywhere in the session returns the same outcome without running anything.
- It returns the setup's [`StepResult`](#what-a-step-reports), or `None` when the step has no setup. A step whose setup failed raises `LakeflowError` in every test that asks for it, naming the error.
- When the session ends, the teardown runs, on the xdist controller when there is one. That may be another process from the one that called `once()`, so a teardown function must be self-contained, as for [a step that runs as a task](#run-a-step-as-a-task): it is rebuilt from its source. `keep` and `--lakeflow-keep` skip the teardown, with a line at the end of the terminal output.
- With `run_as_task=True`, with a notebook file, or without databricks-connect, the step is one serverless job named `[pytest] once: <name>` and tagged `pytest_once`, whose setup task the first caller runs alone and whose teardown task runs alone at session end before the job is deleted. `params` are declared on that job with these values as defaults. `keep_failed` keeps the job after a failed setup or teardown, and `--lakeflow-sweep` removes leftovers from crashed sessions. Such a step runs on the [helper compute](../task-tests.md#helper-compute); `helpers_compute` names a job cluster of a bundle job, which a step's job does not have, so a `Lakeflow` with it refuses `once()` as a task.

The rule of thumb: data every test reads goes in `once()`, data one test owns goes in `setup=` or `lakeflow.schema()`. A setup in every copy runs the same statements for every test, and stops the tests running in parallel when they recreate the same tables; in one suite, as a task, it was 72 seconds per test, 40% of the run time.

## Seed data with a setup step

`setup=` seeds data that one test owns, before every run of the copy; data every test reads belongs in [`once()`](#seed-once-for-the-whole-session) instead. By default it runs from the test process over Databricks Connect, on serverless compute, in a few seconds:

```python
def seed_orders(spark, dbutils, params):
    schema = params["schema"]
    spark.sql(f"CREATE OR REPLACE TABLE {schema}.seed_orders AS SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, sku)")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {schema}.landing")
    dbutils.fs.put(f"/Volumes/{schema.replace('.', '/')}/landing/orders.csv", "id,sku\n1,a\n2,b\n", True)
    return {"schema": schema, "param1": params["param1"]}


def test_setup_seeds_a_table(lakeflow: Lakeflow) -> None:
    with lakeflow.schema() as schema:
        with lakeflow.job("my_example_job", params={"schema": schema, "mode": "incremental"}, setup=seed_orders) as job:
            run = job.run(params={"param1": "seeded"})
            assert run.succeeded, run
            assert run.setup.output_json == {"schema": schema, "param1": "seeded"}
            assert run.task("load").output == "loaded 2 seeded orders"
```

- The function takes any of `spark`, `dbutils` and `params`, and the plugin passes only the ones it declares. `spark` is a Databricks Connect session. `dbutils` is the workspace client's, which reads and writes files, `/Volumes` included, and reads secrets; it has no widgets, `notebook.exit` or task values. `params` is a dict of the run's job parameters as strings: the declared ones at their value for the run or their default, those `modify_settings` declares, and those passed under `allow_undeclared_params=True`. A default that holds a `{{...}}` reference arrives as written, because only the platform resolves it.
- The function runs in the test process, so it can be a lambda or a closure and use the test's imports and helpers.
- It runs before every `run()` and `start()`, with that run's parameters, whatever `only` selects. A failed setup raises `LakeflowError` (`setup= of 'my_example_job' failed before the run started: ...`) and no run starts.
- A value the function returns becomes the step's `output` as JSON, so `run.setup.output_json` gives it back. `run.setup` is a [`StepResult`](#what-a-step-reports) and does not count towards `run.succeeded`.
- Make the function idempotent, hence `CREATE OR REPLACE TABLE`: it runs on every run, including every run of a job that several tests share.

`Lakeflow(setup=...)` sets a default for every copy. A call cannot opt out of it with `None`; derive a `Lakeflow` without it for tests that need none.

## Clean up with a teardown step

`teardown=` takes the same forms and runs after each run of the copy has ended, whether the job passed or failed, when `run()` or `RunHandle.wait()` reads the run back:

```python
def remove_landing_file(dbutils, params):
    return dbutils.fs.rm(f"/Volumes/{params['schema'].replace('.', '/')}/landing/orders.csv")


def test_teardown_removes_a_file(lakeflow: Lakeflow) -> None:
    with lakeflow.schema() as schema:
        with lakeflow.job(
            "my_example_job", params={"schema": schema}, setup=seed_orders, teardown=remove_landing_file
        ) as job:
            run = job.run()
            assert run.succeeded, run
            assert run.teardown.succeeded
```

- `run.teardown` is a [`StepResult`](#what-a-step-reports). A failed teardown does not fail the run; the plugin logs a warning with the step's error. Assert `run.teardown.succeeded` to fail the test on it.
- Use it for side effects the test does not assert on: files under a volume, checkpoints, tables outside the temporary schema, rows written to a source table. Most tests need `setup=` only.

### Tear down when the job closes

The teardown runs at the end of every run, before the test's assertions. To assert on what it removes, pass `teardown_at="close"`: runs then leave the teardown out, and it runs once when the job closes, before the job is deleted:

```python
def test_teardown_at_close_runs_after_the_assertions(lakeflow: Lakeflow) -> None:
    with lakeflow.schema() as schema:
        landing_file = f"/Volumes/{schema.replace('.', '/')}/landing/orders.csv"
        with lakeflow.job(
            "my_example_job",
            params={"schema": schema},
            setup=seed_orders,
            teardown=remove_landing_file,
            teardown_at="close",
        ) as job:
            run = job.run(only=["load"])
            assert run.succeeded, run
            assert run.teardown is None
            lakeflow.ws.files.get_metadata(landing_file)
        assert job.teardown.succeeded
```

- `job.teardown` is the teardown's [`StepResult`](#what-a-step-reports). It is `None` when no run was started, and when `keep` or `--lakeflow-keep` keeps the job, so what the job wrote stays for you to look at. A job that `keep_failed` keeps is still cleaned up.
- The teardown gets the job's `params`, not those of any one run. A teardown that fails, or as a task does not finish within `Lakeflow`'s `timeout`, logs a warning and never fails the test.
- As a task it costs one more run start per job. `teardown_at="close"` without `teardown=` raises `ValueError`.

## Seed with SQL statements or a notebook file

`setup=` and `teardown=` also take a list of SQL statements, or a `Path` to a `.py` notebook file:

```python
from pathlib import Path


def test_seeded_with_sql(lakeflow: Lakeflow) -> None:
    with lakeflow.schema() as schema:
        with lakeflow.job(
            "my_example_job",
            params={"schema": schema},
            setup=["CREATE OR REPLACE TABLE ${schema}.seed AS SELECT 1 AS id, '$$1' AS price"],
            teardown=Path("tests/notebooks/read_seed.py"),
        ) as job:
            run = job.run()
            assert run.succeeded, run
            assert run.setup.output_json == {"statements": 1}
            assert run.teardown.ran_as == "task"
```

- Each statement is rendered with Python's `string.Template` from the job parameters and run in order through `spark.sql`, so no SQL warehouse is needed. `${schema}` or `$schema` is replaced by the parameter's value as text: quote it where it is a string, as in `DATE'${run_date}'`. Write `$$` for a literal `$`.
- A `${name}` must be declared by the bundle job or be a key of the call's own `params`, because the placeholders are checked when `job()` or `task()` is called. `LakeflowError` names an unknown one; `ValueError` is raised for a stray `$`, a bare string instead of a list, or an empty list.
- The step's output is `{"statements": <count>}`. Statements return no rows: assert on the tables they write, or read them in a function step.
- A `Path` is resolved against the pytest rootdir and must be a `.py` file, else `ValueError`. It needs a notebook, so it always runs as a task, whatever the other step does. Its text is the notebook, with the `# Databricks notebook source` line added when missing. It reads the job parameters from its widgets and calls `dbutils.notebook.exit(...)` itself if the test should see an output. A `.sql` file is not accepted, because splitting it on `;` breaks on string literals and comments.

## Run a step as a task

A step runs as a notebook task of the copy, on the [helper compute](../task-tests.md#helper-compute), instead of over Databricks Connect, in three cases:

- It is a `Path` to a notebook file.
- `run_as_task=True` is passed to `job()`, `task()` or `once()`, or set on `Lakeflow` for every call.
- databricks-connect cannot be imported. The plugin brings it on Python 3.11 and 3.12, but no release installs on Python 3.13 or later, see [Install](../getting-started.md#install). The plugin then warns once per session:

```text
UserWarning: databricks-connect is not installed, so setup= and teardown= run as tasks, about 30 seconds each.
pytest-databricks-lakeflow installs it on Python 3.11 and 3.12, but no release installs on Python 3.13 or later.
Run the tests on Python 3.12 to run steps over Databricks Connect in a few seconds, or pass run_as_task=True to
Lakeflow to choose tasks and silence this warning.
```

As a task, the setup task `__pytest_setup` runs first: every task that has no `depends_on`, stubs included, comes to depend on it, and `run(only=...)` always includes it. The teardown task `__pytest_teardown` depends on every task that nothing else depends on, with `run_if: ALL_DONE`. A failed setup task leaves the job's tasks `UPSTREAM_FAILED`, so the run fails and `repr(run)` names it first, as `setup: <error>`. `run.setup.task` and `run.teardown.task` are the tasks' `TaskRun`s, also in `run.injected_tasks`.

A function that runs as a task ships as source, so it must be self-contained: put imports inside it, define any helper inside it, and pass values as job parameters. `ValueError` is raised before anything is created for a lambda, a function without readable source (defined in a REPL), a closure over an enclosing function's variables, a parameter other than the three, or a decorator. Type annotations are fine, because the notebook does not evaluate them. Notebooks are uploaded once per session under `setup_<hash>` and `teardown_<hash>` in `helpers_path`, so tests that share a step share one notebook. For a step that needs a newer serverless runtime, derive a `Lakeflow` with `dataclasses.replace(lakeflow, helpers_environment_version="5")`.

Choose `run_as_task=True` when the step must run where the job runs: as the copy's `run_as` identity, with `run_as="keep"`, or in the job's serverless environment rather than your own Python. Over Connect a step always runs as you.

## What a step reports

`run.setup`, `run.teardown`, `job.teardown` and what `once()` returns are each a `StepResult`:

| Field | Holds |
| --- | --- |
| `role` | `"setup"` or `"teardown"`. |
| `ran_as` | `"connect"` when the step ran over Databricks Connect, `"task"` when it ran as a task. |
| `succeeded` | Whether the step finished without an error. |
| `output`, `output_json` | What the step gave back, as JSON text and parsed: a function's return value, or `{"statements": <count>}`. `output_json` raises `ValueError` naming the step when there is none or it is not JSON. |
| `error` | What the step raised, as a `TaskError`, or `None`. Over Connect, `type` is the exception's class, dotted with its module unless it is a built-in, and `trace` the Python traceback. |
| `duration` | How long the step ran. |
| `task` | The step's `TaskRun` when it ran as a task, else `None`. |

See [`StepResult`](../reference/api.md#stepresult) in the API reference.

## Seed from the test process

Teams that already use databricks-labs-pytester can seed a `lakeflow.schema()` from the test process too, before the job runs. The `pytester` extra installs [databricks-labs-pytester](https://github.com/databrickslabs/pytester), whose fixtures create tables and other resources and remove them afterwards. The two plugins coexist: this one defines only the `lakeflow` fixture, and `lakeflow.ws` gives you the `WorkspaceClient` without a second fixture. The plugin's own tests do not run pytester; check it against your environment.

Choose `once()` when the seed is shared by many tests: it runs one time for the session. Choose `setup=` when one test owns the seed; over Connect it adds a few seconds per run, as a task 28 to 32 seconds before its first statement. Choose pytester when you already build your test resources with its fixtures.

## Let job code know it is under test

With `Lakeflow(test_marker=True)`, every copy declares a `__pytest_test_id` job parameter whose default is the test node id, so job code can tell it is under test and skip or redirect a side effect:

```python
# in a notebook of the job under test
try:
    test_id = dbutils.widgets.get("__pytest_test_id")
except Exception:
    test_id = None  # not under test
```

Other task types can read `{{job.parameters.__pytest_test_id}}`. Tests cannot pass the parameter themselves, and it is left out of `run.parameters`. It declares a job parameter, so `job.run(dbt_commands=...)` is refused on such a job, see [dbt tasks](task-types.md#dbt-tasks).
