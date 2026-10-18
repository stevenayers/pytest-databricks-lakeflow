from __future__ import annotations

import os
from pathlib import Path

import pytest

from pytest_databricks_lakeflow import Lakeflow

EXAMPLE_BUNDLE = Path(__file__).parents[2] / "examples" / "bundle"


@pytest.fixture(scope="session")
def lakeflow() -> Lakeflow:
    return Lakeflow(bundle_root=EXAMPLE_BUNDLE, catalog=os.environ.get("LAKEFLOW_CATALOG"))
