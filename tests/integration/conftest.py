"""Shared fixtures for the integration tests.

Every bundle the integration tests use lives under ``bundles/<name>/``. One module per bundle, named
``test_<name>.py``, overrides the ``lakeflow`` fixture at module scope with ``lakeflow_for("<name>")``.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from pytest_databricks_lakeflow import Lakeflow

HERE = Path(__file__).parent
BUNDLES = HERE / "bundles"


def lakeflow_for(name: str) -> Lakeflow:
    """Return a ``Lakeflow`` for the bundle under ``bundles/<name>``, with the catalog from LAKEFLOW_CATALOG."""
    return Lakeflow(bundle_root=BUNDLES / name, catalog=os.environ.get("LAKEFLOW_CATALOG"))


@pytest.fixture(scope="session")
def lakeflow() -> Lakeflow:
    return lakeflow_for("example")


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    # This hook sees every collected item, not only the ones under this directory, so filter by path.
    if os.environ.get("LAKEFLOW_INTEGRATION") == "1":
        return
    skip = pytest.mark.skip(reason="set LAKEFLOW_INTEGRATION=1 to run against a workspace")
    for item in items:
        if item.path.is_relative_to(HERE):
            item.add_marker(skip)
