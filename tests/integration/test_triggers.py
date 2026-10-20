r"""Integration tests for the ``triggers`` bundle (tests/integration/bundles/triggers), run in a real workspace.

The bundle deploys a schema and a managed volume in the catalog from ``LAKEFLOW_CATALOG``, passed as the bundle
variable ``catalog``, and one serverless job, ``file_job``, with a paused file arrival trigger on that volume. Its
notebook task ``read_trigger`` takes ``{{job.trigger.type}}``, ``{{job.trigger.file_arrival.location}}`` and
``{{job.trigger.time.iso_date}}`` as widgets and returns them as JSON. The copies the tests make have no trigger, so
``trigger=`` stands in for one. Run them with:

    LAKEFLOW_INTEGRATION=1 DATABRICKS_CONFIG_PROFILE=<profile> LAKEFLOW_CATALOG=<catalog> \
        uv run pytest tests/integration/test_triggers.py -n 8 --dist loadgroup --lakeflow-sweep
"""

from __future__ import annotations

import dataclasses
import os
import re

import pytest

from pytest_databricks_lakeflow import Lakeflow, LakeflowError

from .conftest import lakeflow_for

CATALOG = os.environ.get("LAKEFLOW_CATALOG")
# Any path will do: the copy is started with run-now, so nothing watches it.
LOCATION = "/Volumes/main/landing/orders/"

pytestmark = pytest.mark.skipif(not CATALOG, reason="set LAKEFLOW_CATALOG to the catalog the bundle's volume is in")


@pytest.fixture(scope="module")
def lakeflow() -> Lakeflow:
    assert CATALOG is not None
    return dataclasses.replace(lakeflow_for("triggers"), variables={"catalog": CATALOG})


def test_a_simulated_trigger_reaches_the_task(lakeflow: Lakeflow) -> None:
    trigger = {"type": "file_arrival", "file_arrival.location": LOCATION}
    with pytest.warns(UserWarning, match=r"\['job.trigger.time.iso_date'\], which trigger does not set"):
        job = lakeflow.job("file_job", trigger=trigger)
    with job:
        run = job.run()
    assert run.succeeded, run
    assert run.trigger_type == "ONE_TIME"
    seen = run.task("read_trigger").output_json
    assert isinstance(seen, dict)
    assert seen["kind"] == "file_arrival"
    assert seen["location"] == LOCATION
    # Not rewritten, so run-now resolves it to the day of the run.
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", seen["day"])
    parameters = run.task("read_trigger").parameters
    assert isinstance(parameters, dict)
    assert parameters["kind"] == "file_arrival"
    assert "__pytest_trigger_type" not in run.parameters


def test_a_copy_without_the_trigger_is_refused(lakeflow: Lakeflow) -> None:
    # The copy has no trigger, so jobs/create would refuse the file arrival reference that trigger= rewrites. The
    # plugin refuses it first, naming the task and trigger=.
    job = lakeflow.job("file_job")
    with pytest.raises(LakeflowError) as e:
        job.create()
    assert str(e.value) == (
        "Task 'read_trigger' of 'file_job' refers to {{job.trigger.file_arrival.location}}, but the copy has no file "
        "arrival trigger, and jobs/create refuses the reference without one. Set its value with "
        "trigger={'file_arrival.location': ...} on job() or task()."
    )
    assert job.job_id is None
