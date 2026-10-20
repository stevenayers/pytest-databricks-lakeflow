from __future__ import annotations

import json
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from databricks.sdk.service import jobs

from pytest_databricks_lakeflow import bundle, ephemeral, session

from .fakes import FakeWorkspaceClient, example_settings

NEEDS_REFRESH_SELECTION = pytest.mark.skipif(
    "refresh_selection" not in jobs.PipelineParams.__dataclass_fields__,
    reason="this databricks-sdk has no PipelineParams.refresh_selection",
)

LAKEFLOW_CONFTEST = """
from datetime import timedelta

import pytest

from pytest_databricks_lakeflow import Lakeflow


@pytest.fixture(scope="session")
def lakeflow():
    return Lakeflow({arguments})
"""


ATTRIBUTION_VARIABLES = (
    "GITHUB_SERVER_URL",
    "GITHUB_REPOSITORY",
    "GITHUB_RUN_ID",
    "GITHUB_SHA",
    "GITHUB_HEAD_REF",
    "GITHUB_REF_NAME",
    "GITHUB_ACTOR",
    "SYSTEM_TEAMFOUNDATIONCOLLECTIONURI",
    "SYSTEM_TEAMPROJECT",
    "BUILD_BUILDID",
    "BUILD_SOURCEVERSION",
    "BUILD_SOURCEBRANCHNAME",
    "BUILD_REQUESTEDFOREMAIL",
)
GITHUB = {
    "GITHUB_SERVER_URL": "https://github.com",
    "GITHUB_REPOSITORY": "o/r",
    "GITHUB_RUN_ID": "9",
    "GITHUB_SHA": "abc",
    "GITHUB_REF_NAME": "main",
    "GITHUB_ACTOR": "someone",
}
AZURE_DEVOPS = {
    "SYSTEM_TEAMFOUNDATIONCOLLECTIONURI": "https://dev.azure.com/org/",
    "SYSTEM_TEAMPROJECT": "proj",
    "BUILD_BUILDID": "42",
    "BUILD_SOURCEVERSION": "def",
    "BUILD_SOURCEBRANCHNAME": "feature",
    "BUILD_REQUESTEDFOREMAIL": "person@example.com",
}


def set_ci(monkeypatch: pytest.MonkeyPatch, env: dict[str, str]) -> None:
    """Clear every CI variable that the run id and attribution read, then set `env`."""
    for name in (*session.RUN_ID_VARIABLES, *ATTRIBUTION_VARIABLES):
        monkeypatch.delenv(name, raising=False)
    for name, value in env.items():
        monkeypatch.setenv(name, value)


def git(cwd: Path, *args: str) -> str:
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


def make_repository(path: Path, branch: str = "topic") -> str:
    """Make a git repository in `path` with one empty commit on `branch`, and return the commit's sha."""
    git(path, "init", "-q")
    identity = ["-c", "user.name=t", "-c", "user.email=t@example.com", "-c", "commit.gpgsign=false"]
    git(path, *identity, "commit", "-q", "--allow-empty", "-m", "x")
    git(path, "checkout", "-q", "-b", branch)
    return git(path, "rev-parse", "HEAD")


class FakeCli:
    """Stands in for `bundle.run_cli`.

    Summary reports every job registered in `deployed` and every pipeline in `deployed_pipelines`, merged into
    `summary`. A pipeline whose id is empty is reported without one, as a pipeline that is not deployed.
    """

    def __init__(self) -> None:
        self.calls: list[list[str]] = []
        self.invocations: list[tuple[list[str], Path, str | None]] = []
        self.deployed: dict[str, int] = {}
        self.deployed_pipelines: dict[str, str] = {}
        self.summary: dict[str, Any] = {}
        self.validate: dict[str, Any] = {}
        self.errors: dict[str, Exception] = {}
        self.before: Callable[[list[str]], None] = lambda args: None

    def __call__(self, args: list[str], root: Path, profile: str | None = None) -> str:
        self.calls.append(args)
        self.invocations.append((args, root, profile))
        self.before(args)
        if len(args) > 1 and args[1] in self.errors:
            raise self.errors[args[1]]
        if args == ["--version"]:
            return "Databricks CLI v1.7.0\n"
        if args[:2] == ["bundle", "summary"]:
            jobs = {key: {"id": str(job_id)} for key, job_id in self.deployed.items()}
            pipelines: dict[str, dict[str, str]] = {}
            for key, pipeline_id in self.deployed_pipelines.items():
                pipelines[key] = {"id": pipeline_id} if pipeline_id else {"modified_status": "created"}
            return json.dumps({**self.summary, "resources": {"jobs": jobs, "pipelines": pipelines}})
        if args[:2] == ["bundle", "validate"]:
            return json.dumps(self.validate)
        return ""


def configure_lakeflow(pytester: pytest.Pytester, arguments: str, extra: str = "") -> None:
    """Write a conftest that overrides the `lakeflow` fixture with `Lakeflow(<arguments>)`, followed by `extra`."""
    pytester.makeconftest(LAKEFLOW_CONFTEST.format(arguments=arguments) + extra)


@pytest.fixture
def fake_ws(monkeypatch: pytest.MonkeyPatch) -> FakeWorkspaceClient:
    ws = FakeWorkspaceClient()

    def make_workspace_client(profile: str | None) -> FakeWorkspaceClient:
        ws.profiles.append(profile)
        return ws

    monkeypatch.setattr(session, "make_workspace_client", make_workspace_client)
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
