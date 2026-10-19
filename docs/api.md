# API reference

Everything on this page is importable from `pytest_databricks_lakeflow`. The `lakeflow` fixture and the command-line flags are registered by the plugin's `pytest11` entry point.

## The `lakeflow` fixture

Session-scoped. It returns `Lakeflow()` unless you override it in a `conftest.py`. Tests that request it get the `lakeflow` marker, so `pytest -m "not lakeflow"` deselects them.

## Command-line flags

These apply to one pytest run and win over the matching `Lakeflow` arguments.

| Flag | Meaning |
| --- | --- |
| `--lakeflow-keep` | Keep every ephemeral job and temporary schema. |
| `--lakeflow-keep-failed` | Keep the ephemeral job when its test fails, even with `Lakeflow(keep_failed=False)`. |
| `--no-lakeflow-keep-failed` | Delete the ephemeral job even when its test fails. |
| `--lakeflow-sweep` | At session start, delete your leftover ephemeral jobs from other runs that are older than `sweep_hours`. |
| `--lakeflow-destroy` | Run `databricks bundle destroy` at the end of the session for each target that the session deployed. |

## `Lakeflow`

::: pytest_databricks_lakeflow.Lakeflow

## `EphemeralJob`

::: pytest_databricks_lakeflow.EphemeralJob

## `JobRun`

::: pytest_databricks_lakeflow.JobRun

## `TaskRun`

::: pytest_databricks_lakeflow.TaskRun

## `TaskValues`

::: pytest_databricks_lakeflow.TaskValues

## `TaskError`

::: pytest_databricks_lakeflow.TaskError

## `Timing`

::: pytest_databricks_lakeflow.Timing

## `NotebookView`

::: pytest_databricks_lakeflow.NotebookView

## `NotebookCell`

::: pytest_databricks_lakeflow.NotebookCell

## Errors

::: pytest_databricks_lakeflow.LakeflowError

::: pytest_databricks_lakeflow.LakeflowBundleError

::: pytest_databricks_lakeflow.LakeflowTimeout

::: pytest_databricks_lakeflow.LakeflowRunFailed
