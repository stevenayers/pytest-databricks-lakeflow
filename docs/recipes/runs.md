# Control the run

These recipes change what one run does, or how several runs relate. For the arguments of `run()` itself, see [Run it](../job-tests.md#run-it).

## Run the downstream half of a job

`upstream_task_values` on `lakeflow.job()` replaces the tasks you name with stub tasks that set the task values you give, so the rest of the job runs against controlled values:

```python
def test_downstream_half_with_stubbed_extract(lakeflow: Lakeflow) -> None:
    stubbed = {"extract": {"row_count": 7, "source_path": "/Volumes/x/y/z"}}
    with lakeflow.job("my_example_job", upstream_task_values=stubbed) as job:
        run = job.run(only=["transform+"])
        assert run.succeeded, run
        assert "extract" in run.injected_tasks
```

- A stub keeps the key and place of the task it replaces, so the tasks after it still depend on it, `{{tasks.extract.values.row_count}}` and `dbutils.jobs.taskValues.get(taskKey="extract", ...)` resolve against it, and `only` adds it whenever a selected task depends on it. A stub that no selected task depends on is left out like any other task.
- A stub drops its task's `depends_on`, so the tasks upstream of it no longer hold it back. They still run in a run without `only`.
- The stub's outcome is in `run.injected_tasks`, not `run.tasks`. It runs on the [helper compute](../task-tests.md#helper-compute) and its values follow the [rules for stub values](../task-tests.md#rules-for-stub-values).
- A stub replaces the task after `task_params`, `disable`, `force_conditions`, `extra_dependencies` and `task_overrides` are applied, so what they set on that task is lost. An unknown key raises `LakeflowError` listing the job's tasks.
- A stub has no SQL, alert or other output. When a task of the copy reads one, such as `{{tasks.query.output.first_row.n}}`, pass a `Stub` with the literal in `outputs`, see [Stub a task's output](../task-tests.md#stub-a-tasks-output). A reference the stub does not answer raises `LakeflowError` before anything is created.

## Replay a run's task values

`replay` on `run()` takes the stub values from a previous run of the same job. Each task that a selected task depends on and `only` leaves out becomes a stub that sets the values that task set in that run:

```python
def test_replay_downstream_half(lakeflow: Lakeflow) -> None:
    first = lakeflow.job("my_example_job", capture_task_values={"extract": ["row_count", "source_path"]}).run()
    again = lakeflow.job("my_example_job").run(only=["transform+"], replay=first)
    assert again.succeeded, again
```

- The first run must capture the values with `capture_task_values`, as for [`upstream_from`](../task-tests.md#replay-a-task-from-a-previous-run) on task tests.
- The stubs are part of the job's settings, so `replay` needs `only` and a job that has not been created yet: pass it to the first run of a new `lakeflow.job(...)`. `replay` without `only` raises `ValueError`, a run of another job raises `ValueError`, and a job that already exists, or a task to stub that was not part of that run or has no captured values, raises `LakeflowError`.
- A replayed stub sets task values only. A task of the copy that reads an output of a replayed task, such as `{{tasks.query.output.first_row.n}}`, raises `LakeflowError` even when `only` leaves it out. Stub that task yourself on `job()` with `upstream_task_values={"query": Stub(values={...}, outputs={...})}`; `replay` leaves a task you stubbed as it is.

## Run jobs side by side

A copy allows one run at a time, so to run jobs in parallel inside one test, start a run of each copy and wait for them afterwards. `job.start()` takes the arguments of `run()` apart from `stop_after`, `replay` and `stream`, and returns a `RunHandle` straight away:

```python
def test_start_two_jobs_and_wait(lakeflow: Lakeflow) -> None:
    hello = lakeflow.job("my_example_job", params={"param1": "hello", "param2": "world"})
    hi = lakeflow.job("my_example_job", params={"param1": "hi", "param2": "there"})
    with hello, hi:
        first = hello.start()
        second = hi.start(timeout=timedelta(hours=1))
        first_run = first.wait()
        second_run = second.wait()
        assert first_run.succeeded and second_run.succeeded
        assert second_run.task("transform").output == "hi there"
```

- `handle.run_id`, `handle.job` and `handle.url` are known as soon as `start()` returns. The run's page is recorded in the test's report even if the test never waits.
- `handle.wait(timeout=None, *, stop_after=None)` waits for the run and returns its `JobRun`, which is appended to `job.runs`. `timeout` counts from the call to `wait()` and defaults to the `timeout` passed to `start()`, else `Lakeflow`'s. Waiting again returns the same `JobRun`, and `run_finished` is called once.
- The run has already started, so `wait()` cannot lengthen the job's `timeout_seconds`: a `timeout` longer than it raises `LakeflowError`. Pass a long timeout to `start()` or `run()`, which lengthen the job's `timeout_seconds` before the run starts. Waiting after the job was closed raises `LakeflowError`, because closing cancelled the run.
- `handle.cancel()` cancels the run. A later `wait()` returns it as a `JobRun` that ended `CANCELED`.
- A run that is still active when its job closes, or at the end of the session, is cancelled.

## Stop a run that does not end

A streaming job, or any job whose task never returns, ends only when something stops it. `stop_after` cancels the run after a fixed time and returns it, instead of raising:

```python
def test_streaming_job_stops_after(lakeflow: Lakeflow) -> None:
    with lakeflow.schema() as schema, lakeflow.job("streaming_job", params={"schema": schema}) as job:
        run = job.run(stop_after=timedelta(minutes=3))
        assert run.cancelled
        assert run.result_state == "CANCELED"
        assert not run.task("stream").ran
```

- `stop_after` counts from the start of the run. Once it has passed, the plugin cancels the run once and keeps polling until it has ended, which takes a few seconds.
- `timeout` still bounds the whole wait, so give the run a `timeout` a minute or more longer than `stop_after`. A `stop_after` longer than the `timeout` raises `ValueError` before the run starts, and one equal to it always ends in `LakeflowTimeout`, because the deadline passes before the cancelled run has ended.
- A cancelled run has `cancelled` true and `failed` true as well, so assert on `cancelled` and on the data the job wrote. Its cancelled tasks have `ran` false and no `error`, `output` or `logs`.
- `handle.wait(stop_after=...)` works the same way and counts from `start()`.

## Cap the number of active runs

A workspace with a tight serverless quota, or a test that fans out with `start()`, can cap the runs each process has active at once with `Lakeflow(max_concurrent_runs=N)` or `pytest --lakeflow-max-concurrent-runs=N`; the flag wins:

```python
def test_fan_out(lakeflow: Lakeflow) -> None:
    copies = [lakeflow.job("my_example_job", name=f"fan out {i}") for i in range(6)]
    handles = [copy.start() for copy in copies]  # with N=2, the third start() waits for a run to end
    assert all(handle.wait().succeeded for handle in handles)
```

- When `N` runs started by the process are active, the next `start()` or `run()` waits for one of them to end before it calls `run_now`, logging `Waiting to start a run of job ...` at INFO. While it waits it polls the active runs every `poll_interval`, so a run that has ended frees its slot even before anyone waits for it.
- A `start()` or `run()` that finds no free slot within its `timeout` raises `LakeflowError` naming the active runs.
- The cap is per xdist worker, not per session: `-n 4 --lakeflow-max-concurrent-runs=2` can have eight runs active. Lower `N` or the `-n` count to stay under a quota for the whole session.

## Share one run between tests

Run the job in a fixture with a broader scope, inside a `with` block, so the job is deleted when the fixture is torn down:

```python
from collections.abc import Iterator

import pytest

from pytest_databricks_lakeflow import JobRun, Lakeflow


@pytest.fixture(scope="module")
def nightly_run(lakeflow: Lakeflow) -> Iterator[JobRun]:
    with lakeflow.job("my_example_job", name="nightly") as job:
        yield job.run()


def test_extract(nightly_run: JobRun) -> None:
    assert nightly_run.task("extract").output == "extracted 7 rows"


def test_load(nightly_run: JobRun) -> None:
    assert nightly_run.task("load").succeeded
```

- Without `name=`, the job is named `[pytest] <node id>` after the first test that used the fixture.
- Under pytest-xdist, mark the tests that share the fixture with the same `@pytest.mark.xdist_group("nightly_run")` and run with `--dist loadgroup`, so one worker runs them and the job runs once.

## Parametrise and run in parallel

`@pytest.mark.parametrize` works as usual. Each parameter set gets its own job, so the runs are independent and can run at the same time under pytest-xdist:

```bash
pytest -n 4
```

One worker deploys the bundle and loads the templates while the others wait, then all of them reuse the result. A test that waits for each run with `run()` has at most one run active, so `-n 4` keeps at most four runs active at once; see [Cap the number of active runs](#cap-the-number-of-active-runs) for a fan-out. [How it works](../how-it-works.md#parallel-sessions) describes how the workers share the setup.

- Tests are independent only when they do not rebuild each other's data. Fixtures that every test reads go in [`lakeflow.once()`](data.md#seed-once-for-the-whole-session), which the workers share too; a `setup=` that recreates the same tables in every copy makes parallel tests fail each other.
- With a user-to-machine profile (`databricks auth login`), every worker refreshes the same cached token at session start, and two refreshes at once can fail with `exit status 45`. The plugin retries, and `lakeflow.ws` in tests keeps the refreshes to one per worker. On an Azure workspace, `DATABRICKS_AUTH_TYPE=azure-cli` avoids the race while you still run as yourself, see [How do I run tests in parallel with my own login?](../faq.md#how-do-i-run-tests-in-parallel-with-my-own-login); CI should use a service principal.
- [Speed and cost](../speed.md) says how many workers the Jobs API limits allow, which is more than you need.
