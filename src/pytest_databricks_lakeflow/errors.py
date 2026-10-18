from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pytest_databricks_lakeflow.results import JobRun


class LakeflowError(Exception):
    """Base class for errors raised by pytest-databricks-lakeflow."""


class LakeflowBundleError(LakeflowError):
    """A Databricks CLI bundle command failed, or the bundle cannot supply a job template."""


class LakeflowTimeout(LakeflowError):
    """A job run did not finish within its timeout. The run has been cancelled."""


class LakeflowRunFailed(LakeflowError):
    """A job run did not succeed. Raised by `JobRun.raise_for_failure()`.

    Attributes:
        run: The run that failed.
    """

    def __init__(self, message: str, run: JobRun) -> None:
        super().__init__(message)
        self.run = run
