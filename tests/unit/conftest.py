from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from pytest_databricks_lakeflow import bundle, ephemeral, session

from .fakes import FakeWorkspaceClient, example_settings


class FakeCli:
    """Stands in for `bundle.run_cli`. Summary reports every job registered in `deployed`."""

    def __init__(self) -> None:
        self.calls: list[list[str]] = []
        self.deployed: dict[str, int] = {}
        self.validate: dict[str, Any] = {}
        self.errors: dict[str, Exception] = {}
        self.before: Callable[[list[str]], None] = lambda args: None

    def __call__(self, args: list[str], root: Path, profile: str | None = None) -> str:
        self.calls.append(args)
        self.before(args)
        if len(args) > 1 and args[1] in self.errors:
            raise self.errors[args[1]]
        if args == ["--version"]:
            return "Databricks CLI v1.7.0\n"
        if args[:2] == ["bundle", "summary"]:
            jobs = {key: {"id": str(job_id)} for key, job_id in self.deployed.items()}
            return json.dumps({"resources": {"jobs": jobs}})
        if args[:2] == ["bundle", "validate"]:
            return json.dumps(self.validate)
        return ""


@pytest.fixture
def fake_ws(monkeypatch: pytest.MonkeyPatch) -> FakeWorkspaceClient:
    ws = FakeWorkspaceClient()
    monkeypatch.setattr(session, "make_workspace_client", lambda profile: ws)
    monkeypatch.setattr(ephemeral, "POLL_SECONDS", 0)
    return ws


@pytest.fixture
def fake_cli(monkeypatch: pytest.MonkeyPatch, fake_ws: FakeWorkspaceClient) -> FakeCli:
    cli = FakeCli()
    cli.deployed["my_job"] = fake_ws.jobs.add(example_settings())
    monkeypatch.setattr(bundle, "run_cli", cli)
    return cli


@pytest.fixture
def bundle_dir(pytester: pytest.Pytester) -> Path:
    pytester.makefile(".yml", databricks="bundle:\n  name: example\n")
    return pytester.path
