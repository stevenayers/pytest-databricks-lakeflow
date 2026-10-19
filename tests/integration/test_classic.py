r"""Integration tests for the ``classic`` bundle (tests/integration/bundles/classic), run in a real workspace.

The bundle's one job runs on a classic job cluster, which takes several minutes to start, so these tests are
skipped unless LAKEFLOW_CLASSIC=1 is set as well as LAKEFLOW_INTEGRATION=1:

    LAKEFLOW_INTEGRATION=1 LAKEFLOW_CLASSIC=1 DATABRICKS_CONFIG_PROFILE=<profile> LAKEFLOW_CATALOG=<catalog> \
        uv run pytest tests/integration/test_classic.py --lakeflow-sweep
"""

from __future__ import annotations

import dataclasses
import os

import pytest

from pytest_databricks_lakeflow import Lakeflow

from .conftest import lakeflow_for

pytestmark = pytest.mark.skipif(
    os.environ.get("LAKEFLOW_CLASSIC") != "1", reason="set LAKEFLOW_CLASSIC=1 to start a classic job cluster"
)


@pytest.fixture(scope="module")
def lakeflow() -> Lakeflow:
    return lakeflow_for("classic")


def test_single_node_runs_the_job_cluster_on_one_node(lakeflow: Lakeflow) -> None:
    single_node = dataclasses.replace(lakeflow, single_node=True)
    with single_node.job("cluster_job") as job:
        assert job.job_id is not None
        settings = single_node.ws.jobs.get(job.job_id).settings
        clusters = {c.job_cluster_key: c.new_cluster for c in (settings.job_clusters if settings else None) or []}
        cluster = clusters["main"]
        assert cluster is not None
        assert cluster.num_workers == 0
        assert cluster.autoscale is None
        assert (cluster.spark_conf or {})["spark.databricks.cluster.profile"] == "singleNode"
        assert (cluster.spark_conf or {})["spark.master"].startswith("local[")
        assert (cluster.custom_tags or {})["ResourceClass"] == "SingleNode"
        run = job.run()
        assert run.succeeded, run
        # hello exits with spark.conf.get("spark.master"), so a single node cluster reports a local master.
        output = run.task("hello").output
        assert output is not None and output.startswith("local["), output
