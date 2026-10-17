from __future__ import annotations


class LakeflowError(Exception):
    """Base class for errors raised by pytest-databricks-lakeflow."""


class LakeflowBundleError(LakeflowError):
    """A Databricks CLI bundle command failed, or the bundle cannot supply a job template."""


class LakeflowTimeout(LakeflowError):
    """A job run did not finish within its timeout. The run has been cancelled."""
