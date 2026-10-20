# FAQ

Answers to the questions and error messages that come up most. The headings quote the message where there is one, so a search for it lands here.

## Why does the plugin deploy the bundle instead of reading `bundle validate`?

`validate` resolves notebook paths but leaves wheel and JAR paths pointing at local files, and does not resolve `${resources...}` references. A deploy builds and uploads the wheels and gives job settings in which every path is a workspace path, read back with `jobs.get`. The deploy happens once per session, and a deploy of an unchanged bundle takes seconds. If you deploy the target yourself, `Lakeflow(sync="none")` skips it, see [Skip the deploy](recipes/deployments.md#skip-the-deploy).

## Why not rely on `bundle validate`?

Checking a bundle's schema and types is the CLI's job, and `bundle validate` does it. It does not model the Jobs API's own rules, so it passes a `{{job.parameters.<name>}}` the job does not declare, a `{{tasks.<key>.values.<name>}}` whose task is not upstream, a `depends_on` task that is not in the job and retries on a for-each task, and the deploy refuses most of them later, one at a time. `lakeflow.check(key)` checks those rules on the template without creating anything, see [Check every job's wiring](recipes/ci.md#check-every-jobs-wiring-without-running-it).

Checked against CLI v1.7.0, the minimum the plugin requires, and checked again each time that minimum is raised. A rule is dropped once the CLI reports what it does.

| Rule | `bundle validate` |
| --- | --- |
| Undeclared `{{job.parameters.<name>}}` (always applied) | Passes it |
| `{{tasks.<key>.values.<name>}}` whose task is not upstream (always applied) | Passes it |
| `depends_on` task that is not in the job (always applied) | Passes it |
| Retries on a `for_each_task` (always applied) | Passes it |
| `wheel`: entry point missing from the built wheel | Passes it |
| `library`: absolute library, Python file or environment dependency path that does not exist | Passes it |
| `dash`: task reference to a task key with a dash | Passes it |
| `trigger`: `{{job.trigger.*}}` reference | Passes it |
| `equal_to`: `EQUAL_TO` or `NOT_EQUAL` on a number or a task value | Passes it |

## Why do my tests never run the deployed job?

The deployed job keeps its schedule, notifications, `run_as` and bundle lock, and running it would mix test runs with real ones. Each test instead gets a copy with those removed and its dashboard and SQL alert subscriptions paused, named and tagged after the test, so runs are isolated and attributable and do not email subscribers. Jobs are cheap to create and delete.

The copy runs as you rather than as the deployed job's `run_as`. To test the permissions of that principal, pass `run_as="keep"`, see [Run as another identity](recipes/job-settings.md#run-as-another-identity).

## Why is my job parameter rejected?

The Jobs API accepts undeclared parameters silently, so a typo would pass unnoticed. The plugin checks `params` against the job's declared `parameters` and raises `LakeflowError` for anything undeclared. Declare the parameter on the job, or pass `Lakeflow(allow_undeclared_params=True)`.

## Why is every run 3 to 4 minutes slower than in the UI?

The job's `performance_target` is `STANDARD`, and the plugin warns about it. Pass `Lakeflow(performance_optimized=True)`, or `job.run(performance_target="PERFORMANCE_OPTIMIZED")` for one run, see [Speed up a run](recipes/compute.md#speed-up-a-run).

The other cause is a workspace that does not honour the request. When a run asked for `PERFORMANCE_OPTIMIZED` and `run.effective_performance_target` is `STANDARD`, a workspace policy or the job's compute decided the mode, and the plugin warns once per `Lakeflow`:

```text
UserWarning: Run 123 of 'my_job' asked for PERFORMANCE_OPTIMIZED but used STANDARD. A workspace policy or the
job's compute decides the performance mode; expect 3 to 4 minutes extra per run.
```

Nothing in the test can change that; ask a workspace admin about the policy.

## How do I clean up after a crashed session?

`pytest --lakeflow-sweep` deletes your own jobs and pipeline copies from other runs that are older than `sweep_hours`, 24 hours by default. Add `--lakeflow-sweep-schemas=<catalog>` for the temporary schemas, `--lakeflow-sweep-notebooks` for the setup, teardown and debug notebooks, and `--lakeflow-sweep-scope=all` to clean up after everyone. Run it with `--lakeflow-sweep-dry-run` first. `Lakeflow(...).sweep()` runs the same sweep from a script. See [Clean up leftovers](recipes/ci.md#clean-up-leftovers).

## What does the plugin leave in my workspace?

The deployed test target, the helper notebooks under `helpers_path` with any setup, teardown and debug notebooks and inlined saved query files, and the jobs, pipeline copies and schemas kept by the keep rules. A kept job's tags and description say which test, run, person and commit made it. See [What is left in the workspace](how-it-works.md#what-is-left-in-the-workspace).

## Setup fails with "Bundle target 'test' deploys to ... but the workspace client is for ..."

The CLI and the SDK resolved different workspaces. The CLI takes the host from the target's `workspace.host` in `databricks.yml`, while the plugin's `WorkspaceClient` takes it from `profile` or the `DATABRICKS_*` variables. Without this check, every `jobs.get` would look in the wrong workspace and the jobs would be reported as stale. Make the two agree: pass `profile=` to `Lakeflow` for a profile whose host is the target's, set `DATABRICKS_HOST` and `DATABRICKS_CONFIG_PROFILE` to match the target, or run pytest through the CLI with `databricks bundle run -t test -- uv run pytest`, which gives it the target's credentials. A target without `workspace.host` is not checked.

## Setup fails with "Target 'test' sets run_as ..."

Before `bundle deploy`, the plugin reads the `run_as` of the target and of each job from `bundle validate` and compares it with the identity that deploys. A deploy with another user's `run_as`, or a service principal on which you do not have the Service Principal User role, would either fail inside the CLI after a long deploy or leave jobs that run as someone else. Set the test target's `run_as` to `user_name: ${workspace.current_user.userName}`, so that each identity deploys jobs that run as itself; deploy as that principal, for example from CI; or ask a manager of the service principal to grant you the role. To keep a `run_as` you cannot deploy, deploy the bundle yourself as the right identity and pass `sync="none"`.

## The CLI failed with `exit status 45` under xdist

Each xdist worker builds its own `WorkspaceClient`. On a user-to-machine profile (`auth_type = databricks-cli`, from `databricks auth login`), databricks-sdk runs `databricks auth token --force-refresh` to get the token, so every worker refreshes the same cached token at the start of the session. When two refreshes update the CLI's token cache at the same moment, one of them fails with `cannot get access token: Error: forced token refresh: cache update: exit status 45`. The plugin tries to build the client up to four times, waiting a random 2 to 16 seconds between attempts, so that workers that collided do not collide again. A test still fails if every attempt collides.

If you still see it, run with fewer workers, or use a profile that does not refresh through the CLI for parallel runs: a service principal with OAuth machine-to-machine credentials (`client_id` and `client_secret`) or a personal access token (`token`). For example, keep the `databricks-cli` profile `dev` for everyday runs and choose a machine-to-machine profile `dev-sp` for parallel runs through an environment variable:

```python
import os

import pytest

from pytest_databricks_lakeflow import Lakeflow


@pytest.fixture(scope="session")
def lakeflow() -> Lakeflow:
    return Lakeflow(target="test", profile=os.environ.get("LAKEFLOW_PROFILE", "dev"))
```

```bash
LAKEFLOW_PROFILE=dev-sp uv run pytest tests -n 4
```

## Does it work alongside databricks-labs-pytester?

Yes. The plugin defines only the `lakeflow` fixture and does not import pytester, so the two can be installed together. `lakeflow.ws` gives you the `WorkspaceClient` without a second fixture. See [Seed from the test process](recipes/data.md#seed-from-the-test-process).

## How does `lakeflow.pipeline()` differ from pytester's `make_pipeline`?

pytester's `make_pipeline` builds a new pipeline from the keyword arguments you give it, by default with a random name, a generated notebook and a classic cluster, and deletes it afterwards. `lakeflow.pipeline()` copies a pipeline your bundle deployed, from its deployed spec, so the copy runs your pipeline's own code on its own compute, and redirects it into the schema you pass. `pipelines="copy"` on `lakeflow.job()` goes one step further and points a job's pipeline tasks at such copies. Use `make_pipeline` for a pipeline that exists only for the test, and `lakeflow.pipeline()` to test the one in your bundle.

## Can I use `Lakeflow` outside pytest?

No. It raises `LakeflowError` outside a pytest session, and outside a test. Jobs are tied to the running test for naming, tagging and teardown. `lakeflow.attach(run_id)` also runs inside a test. `Lakeflow.sweep()` is the exception: it runs from any script.

## How do I see what is happening?

Run with `--lakeflow-live` to print each task's change of state to the terminal while a run is in flight, or `--log-cli-level=INFO` for the deploy, create, run and delete lines as well. The URLs and task lines are in the `lakeflow` section of each test report and in JUnit XML, and a `lakeflow` block at the end of the run lists the kept jobs, pipeline copies and schemas. See [Watch a run live](recipes/debugging.md#watch-a-run-live) and [Logs and reports](reference/output.md).

## Which clouds and runtimes are supported?

The plugin uses only the Jobs API, the Pipelines API, the Workspace API, the Unity Catalog schemas API and the CLI, which are the same on AWS, Azure and GCP. It was developed and tested against an Azure workspace with serverless jobs and classic job clusters in dedicated access mode.
