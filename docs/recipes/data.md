# Test data

Give each test its own tables, seed them before the run, and clean up afterwards, without a seed task in the bundle or Databricks Connect in the test.

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
- Tables the test asserts on belong here, because the drop cleans them up. Use a teardown task for side effects outside the schema.

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
    run = lakeflow.once(
        "sources", setup=SOURCES, teardown=["DROP SCHEMA ${schema} CASCADE"], params={"schema": "dev_sandbox.sources"}
    )
    assert run.setup.output_json == {"statements": 2}


def test_job_reads_the_sources(lakeflow: Lakeflow, sources: None) -> None:
    with lakeflow.job("my_example_job", params={"schema": "dev_sandbox.sources"}) as job:
        assert job.run().succeeded
```

- `setup=` and `teardown=` take the same forms as on `job()`: a plain function taking any of `spark`, `dbutils` and `params`, a list of SQL statements rendered from `params`, or a `Path` to a `.py` notebook file. `params` are declared on the step's job with these values as defaults, and a `${name}` that `params` does not hold raises `LakeflowError` before anything is created.
- The step is one serverless job named `[pytest] once: <name>`, tagged `pytest_once`, with a `__pytest_setup` task, a `__pytest_teardown` task or both. The first test to ask for it creates the job and runs the setup task alone; the other xdist workers wait for that run and read its outcome; a second call with the same name, steps and parameters anywhere in the session returns the same outcome without running anything.
- It returns the setup run as a `JobRun`, whose `setup` is the task's `TaskRun`, or `None` when the step has no setup. Its `succeeded` is false, because a step has no task under test; assert on `run.setup`. A step whose setup failed raises `LakeflowError` in every test that asks for it, naming the error and the run, and the job is kept as `keep_failed` keeps a copy.
- When the session ends, the teardown task runs alone, on the xdist controller when there is one, and the job is deleted. `keep` and `--lakeflow-keep` keep it and skip the teardown; `keep_failed` keeps it after a failed teardown, with a line at the end of the terminal output. `--lakeflow-sweep` removes leftovers from crashed sessions.
- The step runs on the [helper compute](../task-tests.md#helper-compute). `helpers_compute` names a job cluster of a bundle job, which a step's job does not have, so a `Lakeflow` with it refuses `once()`; `existing_cluster_id` moves the step onto that running cluster instead.

The rule of thumb: data every test reads goes in `once()`, data one test owns goes in `setup=` or `lakeflow.schema()`. A setup task inside every copy costs a task start plus the statements per test, and stops the tests running in parallel when they recreate the same tables; in one suite it was 72 seconds per test, 40% of the run time.

## Seed data with a setup task

`setup=` seeds data that one test owns. It takes a plain function that runs on the job's own compute, as the first task of the copy, so it gets the real `spark` and `dbutils`; data every test reads belongs in [`once()`](#seed-once-for-the-whole-session) instead:

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

- The function takes any of `spark`, `dbutils` and `params`, and the plugin passes only the ones it declares. `params` is a dict of the run's job parameters as strings, the way the job's notebooks read them from their widgets: the declared ones, those `modify_settings` declares, and those passed under `allow_undeclared_params=True`.
- The setup task, `__pytest_setup`, runs first: every task that has no `depends_on`, stubs included, comes to depend on it. It runs on every `run()`, and `run(only=...)` always includes it.
- A value the function returns becomes the task's `output` as JSON, so `run.setup.output_json` gives it back. `run.setup` is the task's `TaskRun`, in `run.injected_tasks`, and does not count towards `run.succeeded`.
- A failed setup leaves the job's tasks `UPSTREAM_FAILED`, so the run fails, and `repr(run)` names it first, as `setup: <error>`.
- The function ships as source and runs on the cluster, so it must be self-contained: put imports inside it, define any helper inside it, and pass values as job parameters. `ValueError` is raised before anything is created for a lambda, a function without readable source (defined in a REPL), a closure over an enclosing function's variables, a parameter other than the three, or a decorator. Type annotations are fine, because the notebook does not evaluate them.
- Make the function idempotent, hence `CREATE OR REPLACE TABLE`: it runs on every run, including every run of a job that several tests share.
- It runs on the [helper compute](../task-tests.md#helper-compute). For a job whose setup needs a newer serverless runtime, derive a `Lakeflow` with `dataclasses.replace(lakeflow, helpers_environment_version="5")`.

`Lakeflow(setup=...)` sets a default for every copy. A call cannot opt out of it with `None`; derive a `Lakeflow` without it for tests that need none.

## Clean up with a teardown task

`teardown=` takes the same forms and runs as the last task of the copy, after every task that nothing else depends on, with `run_if: ALL_DONE`, so it runs whether the job passed or failed:

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

- `run.teardown` is the task's `TaskRun`. A failed teardown does not fail the run; the plugin logs a warning with the task's error and run page. Assert `run.teardown.succeeded` to fail the test on it.
- Use it for side effects the test does not assert on: files under a volume, checkpoints, tables outside the temporary schema, rows written to a source table. Most tests need `setup=` only.

### Tear down when the job closes

The teardown runs inside the same job run, before the test's assertions. To assert on what it removes, pass `teardown_at="close"`: every run then leaves the teardown task out, and the job runs it alone when it closes, before it is deleted:

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
        assert job.teardown_run.teardown.succeeded
```

- `job.teardown_run` is the teardown run's `JobRun`, and `job.teardown_run.teardown` the task's `TaskRun`. Assert on the task: the run's own `succeeded` is false, because none of the job's tasks ran in it. The run is not in `job.runs`.
- The teardown run gets the job's `params`, not those of any one run, and waits for `Lakeflow`'s `timeout`. A teardown run that fails or does not finish in time logs a warning and never fails the test.
- `teardown_run` is `None` when no run was started, and when `keep` or `--lakeflow-keep` keeps the job, so what the job wrote stays for you to look at. A job that `keep_failed` keeps is still cleaned up.
- It costs one more run start per job, so the default stays `teardown_at="run"`. `teardown_at="close"` without `teardown=` raises `ValueError`.

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
```

- Each statement is rendered with Python's `string.Template` from the job parameters and run in order through `spark.sql` on the job's compute, so no SQL warehouse is needed. `${schema}` or `$schema` is replaced by the parameter's value as text: quote it where it is a string, as in `DATE'${run_date}'`. Write `$$` for a literal `$`.
- A `${name}` must be declared by the bundle job or be a key of the call's own `params`, because the placeholders are checked when `job()` or `task()` is called. `LakeflowError` names an unknown one; `ValueError` is raised for a stray `$`, a bare string instead of a list, or an empty list.
- The task's output is `{"statements": <count>}`. Statements return no rows: assert on the tables they write, or read them in a function teardown.
- A `Path` is resolved against the pytest rootdir and must be a `.py` file, else `ValueError`. Its text is the notebook, with the `# Databricks notebook source` line added when missing. It reads the job parameters from its widgets and calls `dbutils.notebook.exit(...)` itself if the test should see an output. A `.sql` file is not accepted, because splitting it on `;` breaks on string literals and comments.

Both forms are uploaded once per session under `setup_<hash>` and `teardown_<hash>` in `helpers_path`, so tests that share a list share one notebook.

## Seed from the test process

Teams that already use Databricks Connect or databricks-labs-pytester can seed a `lakeflow.schema()` from the test process instead, before the job runs. The plugin's own tests do not run these snippets; check them against your environment.

With [Databricks Connect](https://docs.databricks.com/aws/en/dev-tools/databricks-connect/python/), using the same profile as the plugin:

```python
from databricks.connect import DatabricksSession


def test_seeded_through_connect(lakeflow: Lakeflow) -> None:
    spark = DatabricksSession.builder.serverless(True).getOrCreate()
    with lakeflow.schema() as schema:
        spark.sql(f"CREATE TABLE {schema}.seed_orders AS SELECT 1 AS id, 'a' AS sku")
        with lakeflow.job("my_example_job", params={"schema": schema, "mode": "incremental"}) as job:
            assert job.run().succeeded
```

The `pytester` extra installs [databricks-labs-pytester](https://github.com/databrickslabs/pytester), whose fixtures create tables and other resources and remove them afterwards. The two plugins coexist: this one defines only the `lakeflow` fixture, and `lakeflow.ws` gives you the `WorkspaceClient` without a second fixture.

Choose `once()` when the seed is shared by many tests: it runs on serverless with the real `spark` and `dbutils`, one time for the session. Choose `setup=` when one test owns the seed and it needs `spark` or `dbutils`; a setup task costs one task start per run, 28 to 32 seconds on serverless before its first statement. Choose Connect or pytester when the seed is built from local files or needs a library the test process already has.

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
