# Writing task tests

A task test runs one task of a bundle job in a job of its own. The plugin keeps the task's settings as deployed, replaces the upstream tasks you name with stub tasks that set task values, and adds a probe task that reports the values the task set.

## Make a task job

```python
with lakeflow.task(
    "my_example_job.transform",
    params={"param1": "hello", "param2": "world"},
    upstream_task_values={"extract": {"row_count": 42, "source_path": "/Volumes/x/y/z"}},
    capture_task_values=["rows_written"],
) as task:
    run = task.run()
    assert run.task("transform").task_values["rows_written"] == 42
```

- `ref` names the job and the task as `"job_key.task_key"`.
- `params` are job parameters for every run. The job's declared parameters are kept, and the same rules apply as for [job tests](job-tests.md#make-a-job).
- `upstream_task_values` maps upstream task keys to the task values their stubs set. Each named task becomes a stub with the same key, and the target's `depends_on` is replaced by the stubs. Upstream tasks you do not name are dropped.
- `capture_task_values` lists the task value keys to capture from the task. It adds one probe task.
- `run_as` chooses who the job runs as: `None` by default, which runs it as you, `"keep"` for the template's `run_as`, a user's email, a service principal's application id, or a `jobs.JobRunAs`. See [Run as another identity](job-tests.md#run-as-another-identity).

The result is an `EphemeralJob`, the same object that `lakeflow.job()` returns. `run()`, `only`, the keep rules and the result objects all work the same way, and `with` is optional.

## How stubs resolve

Both `dbutils.jobs.taskValues.get(taskKey="extract", key="source_path")` in the task's code and `{{tasks.extract.values.row_count}}` in its settings resolve by the producer's task key. A stub with the key `extract` satisfies both.

`run.task(key).parameters` shows the value a `{{tasks.x.values.y}}` reference in the task's parameters resolved to, for example `run.task("transform").parameters["row_count"] == "7"`. Job parameters are pushed to notebook tasks too, so the dict may hold more keys than the task declares. The platform does not always return resolved values, and then `parameters` is `None`; see [Limitations](limitations.md).

If the task's settings refer to `{{tasks.<key>.values...}}` of a task that is not in `upstream_task_values`, the plugin raises `LakeflowError` before anything is created, because the reference would otherwise fail at run time.

Stub values arrive as JSON through a widget. `{{` inside a value is escaped so that the platform does not treat it as a reference, and the value reaches the task verbatim.

## Rules for stub values

The plugin checks these before creating the job and raises `LakeflowError` otherwise:

- Each value must be JSON-serialisable.
- Each value must be at most 49,152 characters as JSON, which is the platform's limit for a task value.
- Keys must be at most 512 characters.
- You cannot stub the task under test.

## Read captured values

`run.task("transform").task_values` is a dict of the captured values, and `missing_task_values` lists the keys that were not captured. Reading a missing key raises a `KeyError` whose message gives the reason:

- `Task 'transform' never set value 'rows_written'`: the task ran and did not set it.
- `Task 'load' did not run (UPSTREAM_FAILED), so value 'anything' was not captured`: the task was skipped.
- `Value 'x' of task 't' was not captured: probe task __pytest_probe__t ended FAILED ...`: the probe itself failed or its output could not be read.

The probe has `run_if: ALL_DONE`, so values the task set before failing are still captured:

```python
def test_stage_is_recorded_before_the_failure(lakeflow: Lakeflow) -> None:
    with lakeflow.task(
        "my_example_job.transform",
        params={"param1": "explode"},
        upstream_task_values={"extract": {"row_count": 1, "source_path": "/Volumes/x/y/z"}},
        capture_task_values=["stage"],
    ) as task:
        run = task.run()
        assert run.failed
        assert run.task("transform").raised(ValueError)
        assert run.task("transform").task_values["stage"] == "validation"
```

## Test an if/else condition

A condition task that compares an upstream task value can be tested on its own, with the value set by a stub. Only the condition task and its stubs are in a task job, so the branches downstream of it are not part of the run; assert on its outcome instead. In the `branches` integration bundle, `check` compares `{{tasks.check_input.values.rows}}` with `> 0`:

```python
@pytest.mark.parametrize(("rows", "expected"), [(0, False), (5, True)])
def test_condition_task_on_its_own(lakeflow: Lakeflow, rows: int, expected: bool) -> None:
    run = lakeflow.task("branching_job.check", upstream_task_values={"check_input": {"rows": rows}}).run()
    assert run.succeeded, run
    assert run.task("check").condition_outcome is expected
    assert run.branch_taken("check") is expected
    assert run.executed_tasks == ["check"]
```

The stub is in `run.injected_tasks`, not `run.tasks`, so `executed_tasks` lists only the condition task.

`branch_taken` raises `LakeflowError` if the task is not a condition task. To check which downstream tasks ran or were `EXCLUDED`, use a [job test](job-tests.md#branches).

## Helper compute

Stubs and probes are notebook tasks. The two helper notebooks are uploaded once per session to `helpers_path`, by default `/Users/<you>/.pytest-databricks-lakeflow/`, and overwritten on each upload.

By default the helper tasks run on serverless compute, in a plugin-owned environment named `__pytest_serverless` that is appended to the job's `environments`, with `helpers_environment_version` as its environment version. The default version is `"4"`.

A job holds at most 10 environments. When the template already has 10 and none of them is `__pytest_serverless`, the plugin raises `LakeflowError` before creating the job, listing the template's environment keys: one must go from the bundle job to make room for the helper environment, or pass `helpers_compute` to run the helper tasks on a job cluster.

Without serverless jobs, name a job cluster from the job's `job_clusters`:

```python
Lakeflow(helpers_compute="main_cluster")
```

The helper tasks then run on that cluster, and the plugin raises `LakeflowError` if the job has no cluster with that key. The task under test keeps its own compute either way. With `existing_cluster_id` as well, the helper tasks move to that running cluster along with every other task that was on a job cluster. With `single_node=True` instead, the `helpers_compute` cluster becomes a single node cluster like every other job cluster in the copy.

Each helper task costs 7 to 30 seconds of serverless compute, so a task test typically takes a minute end to end.

## What a single-task job loses

Only the target task is kept. On a shared job cluster, a task can import libraries that other tasks declared, and on its own it loses them. Declare every library on the task that needs it. Serverless tasks are unaffected, because their dependencies come from the environment named by their `environment_key`, and the job's `environments` are kept.

## Task tests with tables

A task that writes tables takes the schema as a job parameter, so a temporary schema works the same as in a job test:

```python
@pytest.mark.parametrize(("greeting", "expected"), [("hello", "hello world"), ("goodbye", "goodbye world")])
def test_single_task(lakeflow: Lakeflow, greeting: str, expected: str) -> None:
    with lakeflow.schema() as schema:
        with lakeflow.task(
            "my_example_job.transform",
            params={"param1": greeting, "param2": "world", "schema": schema, "mode": "incremental"},
            upstream_task_values={"extract": {"row_count": 42, "source_path": "/Volumes/x/y/z"}},
            capture_task_values=["rows_written"],
        ) as task:
            run = task.run()
            assert run.succeeded
            assert run.task("transform").output == expected
            assert run.task("transform").task_values["rows_written"] == 42
        assert lakeflow.ws.tables.exists(f"{schema}.example_rows").table_exists
```

## Running a subset of a task job

`only` works on task jobs too. The plugin adds the stubs that the selected tasks depend on and the probes on them, so `run(only=["transform"])` still runs the stubs first and the probe afterwards.
