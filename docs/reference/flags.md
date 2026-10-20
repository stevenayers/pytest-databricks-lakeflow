# Command-line flags and environment variables

Flags apply to one pytest run and win over the matching `Lakeflow` arguments.

## Command-line flags

| Flag | Meaning |
| --- | --- |
| `--lakeflow-keep` | Keep every copy, pipeline copy and temporary schema, and skip the teardown runs of `teardown_at="close"`. |
| `--lakeflow-keep-failed` | Keep the copy and its pipeline copies when their test fails, even with `Lakeflow(keep_failed=False)`. |
| `--no-lakeflow-keep-failed` | Delete the copy and its pipeline copies even when their test fails. |
| `--lakeflow-keep-ttl=48h` | Tag every copy and pipeline copy with `pytest_expires`, an ISO 8601 UTC time this far ahead, in whole hours (`h`) or days (`d`), and give every temporary schema the same `pytest_expires` property. The sweep keeps a tagged job, pipeline or schema until then, whatever its age, with the notebooks such a job uses. |
| `--lakeflow-sweep` | At session start, delete your own jobs tagged `pytest`, and your own `[pytest]` pipeline copies with their tables, from other runs that are older than `sweep_hours`. |
| `--lakeflow-sweep-scope=all` | Sweep jobs, pipeline copies and schemas whoever created them, and with `--lakeflow-sweep-notebooks` every user's default helpers folder. The default is `mine`. |
| `--lakeflow-sweep-schemas=<catalog>` | Also drop the temporary `pytest_<run id>_<8 hex digits>` schemas of other runs in that catalog. |
| `--lakeflow-sweep-notebooks` | Also delete the `setup_<hash>`, `teardown_<hash>` and debug notebooks and the inlined saved query files in the helpers folder that no job the sweep keeps uses. |
| `--lakeflow-sweep-branch=<branch>` | Sweep only jobs, pipeline copies and schemas whose `pytest_git_branch` tag or property is that branch, and only the notebooks that those jobs alone use. |
| `--lakeflow-sweep-dry-run` | Delete nothing, and list what the sweep would delete in the terminal summary. |
| `--lakeflow-sweep-report=<path>` | Write what the sweep deleted, or would delete, to a JSON file, relative to the directory pytest was started in. |
| `--lakeflow-destroy` | Run `databricks bundle destroy` at the end of the session for each target that the session deployed. |
| `--lakeflow-debug-notebook` | When a test fails, write a notebook for each notebook task that failed in the last run of each of its jobs, which sets the task's resolved parameters as widgets and runs the task's notebook. See [Reproduce a failed notebook task](../recipes/debugging.md#reproduce-a-failed-notebook-task). |
| `--lakeflow-report-dir=DIR` | Write the runs of each test to `DIR`, relative to the directory pytest was invoked from: one `report.json` per test, with raw logs and exported notebooks as files. See [Report directory](output.md#report-directory). |
| `--lakeflow-live` | Print each run's start, each task state change and the run's end to the terminal while a run waits. `run(stream=...)` overrides it for one run. Under xdist the lines are only logged. |
| `--lakeflow-max-concurrent-runs=N` | Start a run only while fewer than `N` runs started by this process are active, and otherwise wait for one to end. The cap is per xdist worker. Less than 1 is a usage error. |
| `--lakeflow-check` | Collect one test per job of each `databricks.yml` that pytest collects, named `databricks.yml::<key>`, which calls `lakeflow.check(key, rules=DEFAULT_RULES - {"trigger"})` and fails with the problems it finds. See [Check every job's wiring](../recipes/ci.md#check-every-jobs-wiring-without-running-it). |

Every `--lakeflow-sweep-*` flag needs `--lakeflow-sweep`.

## Environment variables

| Variable | Meaning |
| --- | --- |
| `LAKEFLOW_RUN_ID` | The run id that tags every job and names every temporary schema. Without it the plugin reads `GITHUB_RUN_ID`, then `BUILD_BUILDID`, then makes a random 8-character id. |
| `GITHUB_SERVER_URL`, `GITHUB_REPOSITORY`, `GITHUB_RUN_ID`, `GITHUB_SHA`, `GITHUB_HEAD_REF`, `GITHUB_REF_NAME`, `GITHUB_ACTOR` | On GitHub Actions, fill the `pytest_ci_url`, `pytest_git_sha`, `pytest_git_branch` and `pytest_user` tags of every job. `GITHUB_HEAD_REF` wins over `GITHUB_REF_NAME` for the branch. |
| `SYSTEM_TEAMFOUNDATIONCOLLECTIONURI`, `SYSTEM_TEAMPROJECT`, `BUILD_BUILDID`, `BUILD_SOURCEVERSION`, `BUILD_SOURCEBRANCHNAME`, `BUILD_REQUESTEDFOREMAIL` | The same on Azure DevOps, when the GitHub variables are not set. Without either, the commit and branch come from `git` in the rootdir and the user from the workspace. |
| `DATABRICKS_CONFIG_PROFILE`, `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET` and the rest of the SDK's resolution | Credentials, when `profile` is not set. The CLI reads the same variables. |
| `BUNDLE_VAR_<name>` | Read by the CLI, not by the plugin, as the value of the bundle variable `<name>`. Not part of the setup cache key: two `Lakeflow` instances for one target share one deploy whatever these say. Use `variables` when instances need different values. |

## What the sweep deletes

The sweep runs once per session, in the process that sets up the first bundle target, with that target's workspace and helpers folder. A target set up later is covered only as far as that first sweep reaches: its jobs when it uses the same workspace, its schemas when they are in the catalog named, and its notebooks when its helpers folder is the first target's. It only ever deletes what the plugin makes, recognised by name and tags:

- **Jobs** tagged `pytest` and `pytest_run_id`, from a run other than this one, created more than `sweep_hours` ago. A job tagged `pytest_expires` is deleted once that time has passed instead, whatever its age. With the default scope only jobs you created are considered; `--lakeflow-sweep-scope=all` considers everyone's.
- **Pipelines** named `[pytest] ...` and tagged `pytest` and `pytest_run_id`, from a run other than this one, last changed more than `sweep_hours` ago or past their `pytest_expires` time. The Pipelines API lists pipelines without their tags or creation time, so the sweep lists those whose name starts with `[pytest] ` and reads each one, and a pipeline's age is the time since it last changed. A copy is deleted with its tables.
- **Schemas**, with `--lakeflow-sweep-schemas=<catalog>`, named `pytest_<run id>_<8 hex digits>`, from another run, created more than `sweep_hours` ago or past their `pytest_expires` time. `lakeflow.schema()` writes `pytest_expires` and `pytest_git_branch` as schema properties, because schema tags need `APPLY TAG` and a SQL warehouse. A schema without them is judged by its age, and a branch sweep leaves it.
- **Notebooks and files**, with `--lakeflow-sweep-notebooks`, in the helpers folder: notebooks named exactly `setup_` or `teardown_` followed by 12 hexadecimal digits, debug notebooks in its `debug` folder named `<job key>__<task key>__<run id>`, and workspace files named `query_<run id>_<task key>_<8 hex digits>.sql`, last uploaded more than `sweep_hours` ago. Every session that uses a setup or teardown function uploads its notebook again, so a notebook in use stays young. A notebook is deleted only when no job that the sweep keeps uses it, whoever created that job, so a job kept with `--lakeflow-keep-ttl` keeps its notebooks until its `pytest_expires` time. When the jobs cannot be listed, no notebook is deleted. The helper notebooks themselves are never deleted.
- With `--lakeflow-sweep-scope=all`, the notebooks of every user's default helpers folder, `/Users/<user>/.pytest-databricks-lakeflow`, are swept too, which lists one folder per user under `/Users`. A custom `helpers_path` of another session cannot be found, so only your own is swept.
- With `--lakeflow-sweep-branch`, a notebook is deleted only when at least one job uses it and the sweep deletes every job that does.

A failed delete or listing is logged at WARNING and the sweep carries on. Any other error, such as a report path that cannot be written, ends the sweep with a `UserWarning` (`Lakeflow sweep failed: ...`), raised once by the process that swept. Under xdist the other workers carry on as if it had succeeded.

The dry run lists what it would delete in the terminal summary, and the report writes the same as JSON:

```json
{
  "version": 1,
  "dry_run": true,
  "jobs": [1001],
  "pipelines": ["0b9e2c1e-5d4f-4c3a-9b8e-7f6a5d4c3b2a"],
  "schemas": ["dev_sandbox.pytest_ab12cd34_9f0e1d2c"],
  "notebooks": ["/Users/someone@example.com/.pytest-databricks-lakeflow/setup_3f9a1c07b2d4"],
  "errors": []
}
```
