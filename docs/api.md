# API reference

Everything on this page is importable from `pytest_databricks_lakeflow`, apart from `serialise`, which is in `pytest_databricks_lakeflow.results`. The `lakeflow` fixture and the command-line flags are registered by the plugin's `pytest11` entry point.

## The `lakeflow` fixture

Session-scoped. It returns `Lakeflow()` unless you override it in a `conftest.py`. Tests that request it get the `lakeflow` marker, so `pytest -m "not lakeflow"` deselects them.

## Command-line flags

These apply to one pytest run and win over the matching `Lakeflow` arguments. The `--lakeflow-sweep-*` flags need `--lakeflow-sweep`; see [sweeping leftovers](configuration.md#sweeping-leftovers).

| Flag | Meaning |
| --- | --- |
| `--lakeflow-keep` | Keep every ephemeral job, pipeline copy and temporary schema. |
| `--lakeflow-keep-failed` | Keep the ephemeral job and pipeline copies when their test fails, even with `Lakeflow(keep_failed=False)`. |
| `--no-lakeflow-keep-failed` | Delete the ephemeral job and pipeline copies even when their test fails. |
| `--lakeflow-keep-ttl=48h` | Tag every ephemeral job and pipeline copy, and every temporary schema as a property, with `pytest_expires`, this many hours (`h`) or days (`d`) ahead in UTC. The sweep keeps a tagged job, pipeline or schema, and the notebooks such a job uses, until then, whatever its age. |
| `--lakeflow-sweep` | At session start, delete your leftover ephemeral jobs and pipeline copies from other runs that are older than `sweep_hours`. |
| `--lakeflow-sweep-scope=all` | Sweep jobs, pipeline copies and schemas whoever created them, and every user's default helpers folder. The default is `mine`. |
| `--lakeflow-sweep-schemas=<catalog>` | Also drop old `pytest_<run id>_*` schemas of other runs in that catalog. |
| `--lakeflow-sweep-notebooks` | Also delete old `setup_<hash>`, `teardown_<hash>` and debug notebooks from the helpers folder that no job the sweep keeps uses. |
| `--lakeflow-sweep-branch=<branch>` | Sweep only jobs, pipeline copies and schemas of `pytest_git_branch=<branch>`, and the notebooks that those jobs alone use. |
| `--lakeflow-sweep-dry-run` | Delete nothing, and list what the sweep would delete in the terminal summary. |
| `--lakeflow-sweep-report=<path>` | Write what the sweep deleted, or would delete, to a JSON file. |
| `--lakeflow-destroy` | Run `databricks bundle destroy` at the end of the session for each target that the session deployed. |
| `--lakeflow-debug-notebook` | When a test fails, write a notebook for each notebook task that failed in the last run of each of its jobs, which replays the task's resolved parameters. See [Reproduce a failed notebook task](job-tests.md#reproduce-a-failed-notebook-task). |
| `--lakeflow-report-dir=DIR` | Write the runs of each test to `DIR`: one `report.json` per test, with raw logs and exported notebooks as files. See [Report directory](configuration.md#report-directory). |
| `--lakeflow-live` | Print each task state change to the terminal while a run waits, as `run(stream=True)` does for one run. Under xdist the lines are only logged. See [logging and reports](configuration.md#logging-and-reports). |
| `--lakeflow-max-concurrent-runs=N` | Start a run only while fewer than `N` runs started by this process are active, and otherwise wait for one to end. Overrides `Lakeflow(max_concurrent_runs=...)`. The cap is per xdist worker. |
| `--lakeflow-check` | Collect one test per job of each `databricks.yml` that pytest collects, `databricks.yml::<key>`, which fails with the problems `Lakeflow.check` finds with every rule of `DEFAULT_RULES` but `trigger`, without creating or running a job. See [Check a job without running it](job-tests.md#check-a-job-without-running-it). |

## `Lakeflow`

::: pytest_databricks_lakeflow.Lakeflow

## `EphemeralJob`

::: pytest_databricks_lakeflow.EphemeralJob

## `EphemeralPipeline`

::: pytest_databricks_lakeflow.EphemeralPipeline

## `RunHandle`

::: pytest_databricks_lakeflow.RunHandle

## `JobRun`

::: pytest_databricks_lakeflow.JobRun

## `TaskRun`

::: pytest_databricks_lakeflow.TaskRun

## `ForEachStats`

::: pytest_databricks_lakeflow.ForEachStats

## `ForEachError`

::: pytest_databricks_lakeflow.ForEachError

## `ALL_RULES` and `DEFAULT_RULES`

The names of the rules that `Lakeflow.check(key, rules=...)` applies on top of the four it always applies, as frozensets: `ALL_RULES` is `wheel`, `library`, `dash`, `trigger` and `equal_to`, and `DEFAULT_RULES`, what `check` applies when `rules` is `None`, is every rule but `equal_to`. See [Check a job without running it](job-tests.md#check-a-job-without-running-it).

## `Stub`

::: pytest_databricks_lakeflow.Stub

## `TaskValues`

::: pytest_databricks_lakeflow.TaskValues

## `TaskError`

::: pytest_databricks_lakeflow.TaskError

## `Timing`

::: pytest_databricks_lakeflow.Timing

## `SqlResult`

::: pytest_databricks_lakeflow.SqlResult

## `PipelineUpdate`

::: pytest_databricks_lakeflow.PipelineUpdate

## `NotebookView`

::: pytest_databricks_lakeflow.NotebookView

## `NotebookCell`

::: pytest_databricks_lakeflow.NotebookCell

## `DebugNotebook`

::: pytest_databricks_lakeflow.DebugNotebook

## `SweepReport`

::: pytest_databricks_lakeflow.SweepReport

## `serialise`

::: pytest_databricks_lakeflow.results.serialise

## Errors

::: pytest_databricks_lakeflow.LakeflowError

::: pytest_databricks_lakeflow.LakeflowBundleError

::: pytest_databricks_lakeflow.LakeflowTimeout

::: pytest_databricks_lakeflow.LakeflowRunFailed
