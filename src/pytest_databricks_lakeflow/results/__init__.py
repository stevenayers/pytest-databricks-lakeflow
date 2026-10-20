"""What a run reports back.

The result dataclasses, how they are built from the Jobs API, and how they are written to `report.json`.
`models` holds the dataclasses tests read, `build` fills them from a terminated run, `notebook` reads an exported
notebook's cells and `serialise` writes a run as the JSON of `JobRun.as_dict`.
"""

from __future__ import annotations

from pytest_databricks_lakeflow.results.models import (
    DebugNotebook,
    ForEachError,
    ForEachStats,
    JobRun,
    NotebookCell,
    NotebookView,
    PipelineUpdate,
    SqlResult,
    TaskError,
    TaskRun,
    TaskValues,
    Timing,
)

__all__ = [
    "DebugNotebook",
    "ForEachError",
    "ForEachStats",
    "JobRun",
    "NotebookCell",
    "NotebookView",
    "PipelineUpdate",
    "SqlResult",
    "TaskError",
    "TaskRun",
    "TaskValues",
    "Timing",
]
