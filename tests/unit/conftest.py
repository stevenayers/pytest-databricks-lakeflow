from __future__ import annotations

import json
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import PermissionDenied
from databricks.sdk.service import jobs

from pytest_databricks_lakeflow import bundle, session, workspace_api
from pytest_databricks_lakeflow.ephemeral import polling
from pytest_databricks_lakeflow.report import directory_name
from pytest_databricks_lakeflow.settings.compute import Helpers, add_dependencies, merge_environments

from .fakes.runs import DATA, make_run
from .fakes.templates import example_settings
from .fakes.workspace import FakeWorkspaceClient

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

    monkeypatch.setattr(workspace_api, "make_workspace_client", make_workspace_client)
    monkeypatch.setattr(polling, "POLL_SECONDS", 0)
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


def run_tests(pytester: pytest.Pytester, source: str, *args: str) -> pytest.RunResult:
    pytester.makepyfile(test_jobs=source)
    return pytester.runpytest(*args)


DENIED = "Cannot bind the service principal provided in 'run_as' field to the job."


def deny_create(**kwargs: object) -> jobs.CreateResponse:
    raise PermissionDenied(DENIED)


def queue_until_cancelled(ws: FakeWorkspaceClient) -> None:
    """Queue run 77, which keeps running for 100 polls unless it is cancelled, and then ends `CANCELED`."""
    cancelled = make_run(77, {"t": "CANCELED"}, result_state="CANCELED", code="USER_CANCELED")
    ws.jobs.queue(make_run(77, {"t": "SUCCESS"}), polls=100, after_cancel=cancelled)


def record_finished_runs(pytester: pytest.Pytester) -> None:
    """Configure a `run_finished` callback that appends each run id to `conftest.finished`."""
    extra = "\nfinished = []\n\n\ndef record(run, item):\n    finished.append(run.run_id)\n"
    configure_lakeflow(pytester, "run_finished=record", extra=extra)


def report_of(directory: Path, nodeid: str = "test_jobs.py::test_job") -> tuple[Path, dict[str, Any]]:
    """The report directory of the test `nodeid` under `directory`, and its report.json."""
    test_dir = directory / directory_name(nodeid)
    return test_dir, json.loads((test_dir / "report.json").read_text())


TEMPLATE_PARAMETERS = [
    jobs.JobParameterDefinition(name="param1", default="hello"),
    jobs.JobParameterDefinition(name="schema", default=""),
]


def as_client(ws: FakeWorkspaceClient) -> WorkspaceClient:
    return cast(WorkspaceClient, ws)


FOLDER = "/Users/someone@example.com/.pytest-databricks-lakeflow"
USES_LAKEFLOW = "def test_{0}(lakeflow):\n    lakeflow.template('my_job')\n"
USES_SCHEMA = "def test_schema(lakeflow):\n    with lakeflow.schema():\n        pass\n"


def uses_lakeflow(*names: str) -> str:
    return "\n".join(USES_LAKEFLOW.format(name) for name in names)


UNIT_TESTS = Path(__file__).parent
DEPLOY = ["bundle", "deploy", "-t", "test", "--auto-approve"]
DEPLOY_STAGING = ["bundle", "deploy", "-t", "staging", "--auto-approve"]
SUMMARY = ["bundle", "summary", "-t", "test", "-o", "json"]
VALIDATE = ["bundle", "validate", "-t", "test", "-o", "json"]
WHEEL = "pkg-0.1-py3-none-any.whl"
ARTIFACT_PATH = "/Workspace/b/artifacts"
UPLOADED_WHEEL = f"{ARTIFACT_PATH}/.internal/{WHEEL}"
STAGING_FIXTURE = """

@pytest.fixture(scope="session")
def staging():
    return Lakeflow(target="staging")
"""
XDIST_CONFTEST = """
import json
import sys

import pytest

from pytest_databricks_lakeflow import Lakeflow, bundle, session, workspace_api

sys.path.insert(0, {unit_tests!r})

from fakes.runs import sweep_candidates
from fakes.templates import example_settings
from fakes.workspace import FakeWorkspaceClient


def fake_cli(args, root, profile=None):
    with open({log!r}, "a") as f:
        f.write(" ".join(args) + "\\n")
    if args == ["--version"]:
        return "Databricks CLI v1.7.0"
    if args[:2] == ["bundle", "summary"]:
        return json.dumps({{"resources": {{"jobs": {{"my_job": {{"id": "1000"}}}}}}}})
    if args[:2] == ["bundle", "validate"]:
        return "{{}}"
    return ""


def make_client(profile):
    ws = FakeWorkspaceClient()
    ws.jobs.add(example_settings())
    return ws


def pytest_configure(config):
    if hasattr(config, "workerinput"):
        bundle.run_cli = fake_cli
        workspace_api.make_workspace_client = make_client
"""
SWEEP_LOGGING = """

from pathlib import Path


def make_client(profile):
    ws = FakeWorkspaceClient()
    ws.jobs.add(example_settings())
    ws.jobs.listed = sweep_candidates()
    ws.jobs.delete_log = Path({deleted!r})
    return ws
"""


def bundle_key(root: Path, **overrides: Any) -> session.BundleKey:
    values: dict[str, Any] = {"root": root, "target": "test", "profile": None, "sync": "deploy", "helpers_path": None}
    return session.BundleKey(**{**values, **overrides})


def make_xdist_conftest(pytester: pytest.Pytester, extra: str = "") -> Path:
    log = pytester.path / "cli.log"
    pytester.makeconftest(XDIST_CONFTEST.format(unit_tests=str(UNIT_TESTS), log=str(log)) + extra)
    return log


NODEID = "tests/test_x.py::test_y"
TAGS = {"pytest": "", "pytest_run_id": "r1", "pytest_nodeid": NODEID}
ATTRIBUTION = {
    "pytest_git_branch": "main",
    "pytest_git_sha": "abc",
    "pytest_user": "someone",
    "pytest_ci_url": "https://github.com/o/r/actions/runs/9",
}
HELPERS = Helpers(set_values="/h/set_values", get_values="/h/get_values")


def task(settings: jobs.JobSettings, key: str) -> jobs.Task:
    return next(t for t in settings.tasks or [] if t.task_key == key)


def with_tasks(*tasks: dict[str, Any]) -> jobs.JobSettings:
    settings = example_settings()
    settings.tasks = [*(settings.tasks or []), *(jobs.Task.from_dict(t) for t in tasks)]
    return settings


def three_tasks() -> jobs.JobSettings:
    """Build a job with three tasks, `a`, then `b` after it, and `c` on its own, and a `day` parameter."""
    return jobs.JobSettings.from_dict(
        {
            "parameters": [{"name": "day", "default": ""}, {"name": "__pytest_test_id", "default": "x"}],
            "tasks": [
                {"task_key": "a", "notebook_task": {"notebook_path": "/a"}},
                {"task_key": "b", "depends_on": [{"task_key": "a"}], "notebook_task": {"notebook_path": "/b"}},
                {"task_key": "c", "notebook_task": {"notebook_path": "/c"}},
            ],
        }
    )


def for_each(inner: dict[str, Any]) -> dict[str, Any]:
    return {"task_key": "loop", "for_each_task": {"inputs": "[1, 2]", "task": inner}}


def nested_task(settings: jobs.JobSettings, key: str) -> jobs.Task:
    loop = task(settings, key).for_each_task
    assert loop is not None and loop.task is not None
    return loop.task


def notebook(key: str, *deps: str, **parameters: str) -> dict[str, Any]:
    """Build a notebook task `key` that depends on `deps` and passes `parameters` as its base parameters."""
    return {
        "task_key": key,
        "depends_on": [{"task_key": dep} for dep in deps],
        "notebook_task": {"notebook_path": f"/{key}", "base_parameters": parameters},
    }


DEPS = ["faker==30.0.0"]


def reached(settings: jobs.JobSettings | None = None) -> jobs.JobSettings:
    """Add `DEPS` to `settings`, `example_settings()` by default, as `extra_dependencies` does."""
    return add_dependencies(settings or example_settings(), DEPS)


def moved(environments: dict[str, dict[str, Any]], **compute: str) -> jobs.JobSettings:
    """Add `DEPS`, merge `environments`, then set `compute` on `transform`, as `modify_settings` would."""
    settings = merge_environments(reached(), environments)
    for name, value in compute.items():
        setattr(task(settings, "transform"), name, value)
    return settings


UPSTREAM = {"extract": {"row_count": 42, "source_path": "/Volumes/x/y/z"}}


def keys(settings: jobs.JobSettings) -> list[str]:
    return [t.task_key for t in settings.tasks or []]


def environment_keys(settings: jobs.JobSettings) -> list[str]:
    return [e.environment_key for e in settings.environments or []]


FAILED_EXPORT = (DATA / "notebook_export_failed.html").read_text()
CLASSIC_EXPORT = (DATA / "notebook_export_classic.html").read_text()
