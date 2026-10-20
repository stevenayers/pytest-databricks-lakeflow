# API reference

Everything on this page is importable from `pytest_databricks_lakeflow`. The `lakeflow` fixture and the [command-line flags](flags.md) are registered by the plugin's `pytest11` entry point.

## The `lakeflow` fixture

Session-scoped. It returns `Lakeflow()` unless you override it in a `conftest.py`. Tests that request it get the `lakeflow` marker, so `pytest -m "not lakeflow"` deselects them.

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

## `DbtArtifacts`

::: pytest_databricks_lakeflow.DbtArtifacts

## `DbtResult`

::: pytest_databricks_lakeflow.DbtResult

## `ForEachStats`

::: pytest_databricks_lakeflow.ForEachStats

## `ForEachError`

::: pytest_databricks_lakeflow.ForEachError

## `ALL_RULES` and `DEFAULT_RULES`

The names of the rules that `Lakeflow.check(key, rules=...)` applies on top of the four it always applies, as frozensets: `ALL_RULES` is `wheel`, `library`, `dash`, `trigger` and `equal_to`, and `DEFAULT_RULES`, what `check` applies when `rules` is `None`, is every rule but `equal_to`. See [Check every job's wiring](../recipes/ci.md#check-every-jobs-wiring-without-running-it).

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

## Errors

Two kinds of error leave the plugin. A `ValueError` means an argument is wrong on its own: a choice outside the ones listed, a `setup=` that is neither a function, a list nor a `.py` path, a stub value that is not JSON-serialisable, a `stop_after` longer than its `timeout`. A `LakeflowError`, or one of its subclasses, means the bundle, the job or the workspace did not fit: a task key the job does not have, a reference the copy cannot resolve, a run that did not finish, a deploy that failed. Catch `LakeflowError` for the second kind; the first kind is a mistake in the test to fix.

::: pytest_databricks_lakeflow.LakeflowError

::: pytest_databricks_lakeflow.LakeflowBundleError

::: pytest_databricks_lakeflow.LakeflowTimeout

::: pytest_databricks_lakeflow.LakeflowRunFailed
