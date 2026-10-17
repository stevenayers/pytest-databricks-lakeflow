from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from databricks.sdk.service import jobs

    from pytest_databricks_lakeflow.results import JobRun


@pytest.hookspec
def pytest_lakeflow_modify_settings(settings: jobs.JobSettings, request: pytest.FixtureRequest) -> None:
    """Change an ephemeral job's settings just before it is created.

    Args:
        settings: The settings the job will be created with. Change them in place.
        request: The test's fixture request.
    """


@pytest.hookspec
def pytest_lakeflow_run_finished(run: JobRun, request: pytest.FixtureRequest) -> None:
    """Inspect each run of an ephemeral job after it finishes.

    Args:
        run: The run's outcome.
        request: The test's fixture request.
    """
