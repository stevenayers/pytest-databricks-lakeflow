# Compute and cost

Make test runs start faster and cost less, without changing the bundle. Every option here is an argument of `Lakeflow`, so it applies to every copy a `Lakeflow` makes; derive one with `dataclasses.replace(lakeflow, ...)` for a subset of tests.

## Choose the performance mode

Every copy, and every `once()` step, runs with `performance_target: PERFORMANCE_OPTIMIZED` by default, whatever the template says, so a serverless task starts in seconds. `Lakeflow(performance_target="STANDARD")` runs them in standard mode instead, which Databricks bills on the same SKU with fewer DBUs, up to 70% fewer, and which adds 4 to 6 minutes of start-up to every task; use it for CI runs that can wait, and prefer the flag so nothing changes in the code:

```bash
pytest --lakeflow-performance-target=STANDARD
```

`Lakeflow(performance_target=None)` keeps each template's own setting, and the plugin then warns about a job whose template says `STANDARD`. `job.run(performance_target="PERFORMANCE_OPTIMIZED")` chooses the mode for one run without changing the job's settings:

```python
def test_run_with_performance_target_override(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job") as job:
        run = job.run(performance_target="PERFORMANCE_OPTIMIZED")
        assert run.succeeded
        assert run.effective_performance_target == "PERFORMANCE_OPTIMIZED"
```

- `run.effective_performance_target` and each task's `effective_performance_target` show which mode the run used. When a run asked for `PERFORMANCE_OPTIMIZED` and used `STANDARD`, a workspace policy or the job's compute decided, and the plugin emits one `UserWarning` per `Lakeflow`. Nothing in the test can change that; ask a workspace admin about the policy.
- The performance target applies to serverless tasks; tasks on classic clusters ignore it. An unknown name raises `ValueError`, on `Lakeflow`, on `run()` and on the flag.
- `job.run(queue=False)` turns queueing off for one run, and `Lakeflow(queue=...)` sets the default. `None` leaves the job's own setting.

Each task's `timing` splits its time into `queue`, `setup`, `execution` and `cleanup`, so you can see where a slow run spends it. [Speed and cost](../speed.md) explains where a session's time and money go, and [How it works](../how-it-works.md#timing) lists what to expect.

## Skip cluster start-up

A classic job cluster takes 2 to 10 minutes to start. `Lakeflow(existing_cluster_id="<cluster id>")` moves every task that the template puts on a job cluster or a new cluster onto that running cluster, the plugin's helper tasks and nested for-each tasks included. The job's `job_clusters` are dropped, and serverless and warehouse tasks keep their compute.

## Where setup and teardown steps run

A `setup=`, `teardown=` or `once()` step that runs over Databricks Connect uses one Databricks Connect session per `Lakeflow`, opened from the test process the first time a step needs it:

- Without `existing_cluster_id`, the session is on serverless compute. It is ready in about two seconds, and each statement takes about half a second.
- With `existing_cluster_id`, the session is on that running cluster, which can be a classic all-purpose cluster. The databricks-connect version must match the cluster's Databricks Runtime, so pin it in your project.
- `helpers_compute` and `helpers_environment_version` apply only to steps that run as tasks, see [Run a step as a task](data.md#run-a-step-as-a-task).

Connect sessions bill on the interactive serverless SKU, not the jobs one. In one Azure region its list price was USD 1.00 per DBU against USD 0.50 for serverless jobs. A step over Connect skips the 30 seconds of start-up a task pays first, so it uses far less compute time; the net effect on the bill has not been measured.

## Run job clusters on one node

To keep job clusters but run each on one node, pass `Lakeflow(single_node=True)`:

- Every job cluster and every task's `new_cluster` gets `num_workers` 0, no `autoscale`, the single node Spark configuration (`spark.master` and `spark.databricks.cluster.profile`) merged into `spark_conf`, and the `ResourceClass: SingleNode` tag merged into `custom_tags`. Existing `spark_conf` and `custom_tags` entries survive.
- Single node compute needs dedicated access mode, so a cluster whose `data_security_mode` is `USER_ISOLATION` or `DATA_SECURITY_MODE_STANDARD` raises `LakeflowError` naming it before the job is created. A cluster policy that forbids `spark.databricks.cluster.profile` refuses the copy.
- Serverless tasks are left alone, and with `existing_cluster_id` there is no cluster left to change.

## Run the helper tasks on a job cluster

Stubs, probes, and setup and teardown steps that run as tasks, run on serverless compute by default. Without serverless jobs, name a job cluster from the job's `job_clusters`:

```python
Lakeflow(helpers_compute="main_cluster")
```

The helper tasks then run on that cluster, and the plugin raises `LakeflowError` before creating the job when the job has no cluster with that key. Setup and teardown steps over Databricks Connect cannot use a job cluster, which exists only during a run: pass `run_as_task=True` to run them there too, or `existing_cluster_id` to run them on a running cluster. The task under test keeps its own compute. With `existing_cluster_id` as well, the helper tasks move to that running cluster, and with `single_node=True` the helper cluster becomes a single node cluster like every other.

## Pin the serverless environment version

- `Lakeflow(environment_version="5")` sets the environment version of every environment copied from the template. Environments whose spec sets `base_environment` keep it. `environments=` wins for the environments it names, see [Change a serverless environment](job-settings.md#change-a-serverless-environment).
- `Lakeflow(helpers_environment_version="5")` sets the version of the plugin's own `__pytest_serverless` environment, which the stub and probe tasks and setup and teardown steps that run as tasks use. The default is `"4"`.

## Charge runs to a budget policy

`Lakeflow(budget_policy_id="<policy id>")` sets the budget policy of every copy and pipeline copy. A pipeline copy without it keeps the deployed pipeline's.

## Change compute with modify_settings

For anything `Lakeflow` has no argument for, `modify_settings` edits the copy's `JobSettings` just before the job is created, see [Change anything else before the job is created](job-settings.md#change-anything-else-before-the-job-is-created). Two compute recipes:

```python
import pytest
from databricks.sdk.service import jobs

from pytest_databricks_lakeflow import Lakeflow


def shrink_clusters(settings: jobs.JobSettings, item: pytest.Item) -> None:
    """Run every job cluster and task cluster on one small worker."""
    clusters = [c.new_cluster for c in settings.job_clusters or [] if c.new_cluster is not None]
    clusters += [t.new_cluster for t in settings.tasks or [] if t.new_cluster is not None]
    for cluster in clusters:
        cluster.num_workers = 1
        cluster.autoscale = None
        cluster.node_type_id = "Standard_D4s_v5"


def enable_queue(settings: jobs.JobSettings, item: pytest.Item) -> None:
    """Queue a run instead of skipping it when the workspace is at its concurrency limit."""
    settings.queue = jobs.QueueSettings(enabled=True)


@pytest.fixture(scope="session")
def lakeflow() -> Lakeflow:
    return Lakeflow(modify_settings=shrink_clusters)
```

`modify_settings` sees the copy after the plugin's own changes, so `settings.environments` includes `__pytest_serverless`; leave it alone when you change environments by hand.

## Record run timings

`Lakeflow(run_finished=...)` is called with each `JobRun` and the test after a run finishes. This callback appends one row per run to a CSV, one file per xdist worker, for trend tracking:

```python
import csv
import os
from pathlib import Path

import pytest

from pytest_databricks_lakeflow import JobRun, Lakeflow

FIELDS = ["nodeid", "job_key", "run_id", "succeeded", "duration_seconds", "execution_seconds", "run_url"]


def append_timing(run: JobRun, item: pytest.Item) -> None:
    worker = os.environ.get("PYTEST_XDIST_WORKER", "main")
    path = Path(item.config.rootpath) / "build" / f"timings-{worker}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    new = not path.exists()
    with path.open("a", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        if new:
            writer.writeheader()
        execution = sum((task.timing.execution for task in run.tasks.values()), run.timing.execution)
        writer.writerow(
            {
                "nodeid": item.nodeid,
                "job_key": run.job_key,
                "run_id": run.run_id,
                "succeeded": run.succeeded,
                "duration_seconds": run.duration.total_seconds(),
                "execution_seconds": execution.total_seconds(),
                "run_url": run.run_url,
            }
        )


@pytest.fixture(scope="session")
def lakeflow() -> Lakeflow:
    return Lakeflow(run_finished=append_timing)
```

A run with several tasks reports zero durations on the parent run and the real ones on its tasks, hence the sum. `run_finished` runs once per run, however often `RunHandle.wait()` is called, and not for a run that `lakeflow.attach()` rebuilt.
