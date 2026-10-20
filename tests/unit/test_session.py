from __future__ import annotations

import json
import logging
import os
import random
import subprocess
import time
from pathlib import Path
from typing import Any, cast

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import PermissionDenied
from databricks.sdk.service import compute, jobs

from pytest_databricks_lakeflow import Lakeflow, helpers, session
from pytest_databricks_lakeflow.errors import LakeflowBundleError, LakeflowError

from .conftest import AZURE_DEVOPS, GITHUB, FakeCli, configure_lakeflow, git, make_repository, set_ci
from .fakes import (
    ACCOUNT_ID,
    HOST,
    TOKEN_RACE,
    ContendedTokenCache,
    FakeWorkspaceClient,
    build_wheel,
    example_settings,
    sp_rule_set,
)

UNIT_TESTS = Path(__file__).parent
DEPLOY = ["bundle", "deploy", "-t", "test", "--auto-approve"]
DEPLOY_STAGING = ["bundle", "deploy", "-t", "staging", "--auto-approve"]
SUMMARY = ["bundle", "summary", "-t", "test", "-o", "json"]
VALIDATE = ["bundle", "validate", "-t", "test", "-o", "json"]
OTHER_HOST = "https://other.cloud.databricks.com"
APP_ID = "0000-app-id"
SP_RUN_AS = {"service_principal_name": APP_ID}
SP_USER = "roles/servicePrincipal.user"
SP_MANAGER = "roles/servicePrincipal.manager"
SP_FIX = "Ask a manager of that service principal to grant you the role."
RUN_AS_FIX = (
    "Deploy as that principal, set run_as to ${workspace.current_user.userName}, or deploy the bundle yourself and "
    'use Lakeflow(sync="none").'
)
WHEEL_ARTIFACT = {"type": "whl", "build": "uv build --wheel", "path": "/b", "files": [{"source": "/b/dist/*.whl"}]}
UNVERSIONED_MESSAGE = (
    "Artifacts ['lakeflow_example'] have no dynamic_version: true. Serverless caches wheels by file name, so a rebuilt "
    "wheel with the same version may not be picked up. Set artifacts.<name>.dynamic_version: true in databricks.yml."
)
WHEEL = "pkg-0.1-py3-none-any.whl"
ARTIFACT_PATH = "/Workspace/b/artifacts"
UPLOADED_WHEEL = f"{ARTIFACT_PATH}/.internal/{WHEEL}"
STALE_MESSAGE = f"dist/{WHEEL} was built after its uploaded copy. Run `databricks bundle deploy -t test`."
STAGING_FIXTURE = """

@pytest.fixture(scope="session")
def staging():
    return Lakeflow(target="staging")
"""

XDIST_CONFTEST = """
import json
import sys

import pytest

from pytest_databricks_lakeflow import Lakeflow, bundle, session

sys.path.insert(0, {unit_tests!r})

from fakes import FakeWorkspaceClient, example_settings, sweep_candidates


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
        session.make_workspace_client = make_client
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

GIT_LOGGING = """

import os
import subprocess

if os.environ.get("PYTEST_XDIST_WORKER"):
    _run = subprocess.run

    def logged_run(args, *rest, **kwargs):
        if args and args[0] == "git":
            with open({calls!r}, "a") as f:
                f.write(" ".join(args) + "\\n")
        return _run(args, *rest, **kwargs)

    subprocess.run = logged_run
"""


def bundle_key(root: Path, **overrides: Any) -> session.BundleKey:
    values: dict[str, Any] = {"root": root, "target": "test", "profile": None, "sync": "deploy", "helpers_path": None}
    return session.BundleKey(**{**values, **overrides})


def as_client(ws: FakeWorkspaceClient) -> WorkspaceClient:
    return cast(WorkspaceClient, ws)


def stale_wheel(ws: FakeWorkspaceClient, cli: FakeCli, root: Path, uploaded_after_build: int = -120) -> None:
    """Build dist/<WHEEL> under `root`, upload it `uploaded_after_build` seconds later, and validate a job using it."""
    (root / "dist").mkdir(exist_ok=True)
    built = 1_700_000_000
    wheel = root / "dist" / WHEEL
    wheel.write_text("")
    os.utime(wheel, (built, built))
    ws.workspace.objects.add(UPLOADED_WHEEL)
    ws.workspace.modified[UPLOADED_WHEEL] = (built + uploaded_after_build) * 1000
    spec = {"environment_version": "4", "dependencies": ["./dist/*.whl"]}
    task = {
        "task_key": "t",
        "environment_key": "default",
        "python_wheel_task": {"package_name": "pkg", "entry_point": "m"},
    }
    job = {"name": "j", "environments": [{"environment_key": "default", "spec": spec}], "tasks": [task]}
    cli.validate = {"workspace": {"artifact_path": ARTIFACT_PATH}, "resources": {"jobs": {"my_job": job}}}


def make_xdist_conftest(pytester: pytest.Pytester, extra: str = "") -> Path:
    log = pytester.path / "cli.log"
    pytester.makeconftest(XDIST_CONFTEST.format(unit_tests=str(UNIT_TESTS), log=str(log)) + extra)
    return log


class TestSharedSetup:
    """One setup per bundle target, shared between xdist workers through a file in the state directory."""

    def test_session_state_defaults(self, tmp_path: Path) -> None:
        state = session.SessionState(rootpath=tmp_path, run_id="r")
        assert state.kept_jobs == []
        assert state.kept_schemas == []
        assert state.cancelled_runs == 0

    def test_bundle_keys_name_distinct_state_files(self, tmp_path: Path) -> None:
        names = {bundle_key(tmp_path).file_name(), bundle_key(tmp_path, target="ci").file_name()}
        assert len(names) == 2
        assert bundle_key(tmp_path).file_name() == bundle_key(tmp_path).file_name()
        a = bundle_key(tmp_path, variables=(("catalog", "a"),))
        b = bundle_key(tmp_path, variables=(("catalog", "b"),))
        assert len({a.file_name(), b.file_name(), bundle_key(tmp_path).file_name()}) == 3
        assert a.file_name() == bundle_key(tmp_path, variables=(("catalog", "a"),)).file_name()
        flagged = bundle_key(tmp_path, deploy_flags=("--fail-on-active-runs",)).file_name()
        assert flagged not in names
        assert flagged == bundle_key(tmp_path, deploy_flags=("--fail-on-active-runs",)).file_name()

    def test_shared_state_produces_once_for_all_workers(self, tmp_path: Path) -> None:
        produced: list[str] = []

        def produce() -> dict[str, Any]:
            produced.append("gw0")
            return {"templates": {}}

        path = tmp_path / "state.json"
        assert session.shared_state(path, produce) == {"templates": {}}
        assert session.shared_state(path, produce) == {"templates": {}}
        assert produced == ["gw0"]

    def test_shared_state_shares_setup_errors(self, tmp_path: Path) -> None:
        def fail() -> dict[str, Any]:
            raise LakeflowBundleError("deploy failed")

        path = tmp_path / "state.json"
        with pytest.raises(LakeflowBundleError, match="deploy failed"):
            session.shared_state(path, fail)
        with pytest.raises(LakeflowError, match="failed in another xdist worker: LakeflowBundleError: deploy failed"):
            session.shared_state(path, lambda: {"templates": {}})

    def test_xdist_workers_share_one_setup_and_run_id(self, pytester: pytest.Pytester, bundle_dir: Path) -> None:
        pytest.importorskip("xdist")
        log = make_xdist_conftest(pytester)
        run_ids = pytester.path / "run_ids.log"
        test = (
            "def test_{i}(lakeflow):\n"
            "    assert lakeflow.template('my_job').tasks[0].task_key == 'extract'\n"
            f"    with open({str(run_ids)!r}, 'a') as f:\n"
            "        f.write(lakeflow.run_id + '\\n')\n"
        )
        pytester.makepyfile("\n".join(test.format(i=i) for i in range(6)))
        result = pytester.runpytest("-n", "2")
        result.assert_outcomes(passed=6)
        calls = log.read_text().splitlines()
        assert calls.count(" ".join(DEPLOY)) == 1
        assert calls.count(" ".join(SUMMARY)) == 1
        assert len(run_ids.read_text().split()) == 6
        assert len(set(run_ids.read_text().split())) == 1

    def test_xdist_workers_collect_the_same_check_items(self, pytester: pytest.Pytester, bundle_dir: Path) -> None:
        pytest.importorskip("xdist")
        log = make_xdist_conftest(pytester)
        pytester.makepyfile("def test_nothing():\n    pass\n")
        result = pytester.runpytest("-n", "2", "--lakeflow-check", "-v")
        result.assert_outcomes(passed=2)
        result.stdout.fnmatch_lines(["*PASSED*databricks.yml::my_job*"])
        calls = log.read_text().splitlines()
        assert calls.count(" ".join(DEPLOY)) == 1
        assert calls.count(" ".join(SUMMARY)) == 1

    def test_xdist_workers_share_each_bundle_target(self, pytester: pytest.Pytester, bundle_dir: Path) -> None:
        pytest.importorskip("xdist")
        log = make_xdist_conftest(pytester, STAGING_FIXTURE)
        test = "def test_{i}(lakeflow, staging):\n    lakeflow.template('my_job')\n    staging.template('my_job')\n"
        pytester.makepyfile("\n".join(test.format(i=i) for i in range(6)))
        pytester.runpytest("-n", "2").assert_outcomes(passed=6)
        calls = log.read_text().splitlines()
        assert calls.count(" ".join(DEPLOY)) == 1
        assert calls.count(" ".join(DEPLOY_STAGING)) == 1

    def test_xdist_workers_share_attribution(
        self, pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch, bundle_dir: Path
    ) -> None:
        pytest.importorskip("xdist")
        set_ci(monkeypatch, {})
        # Under an outer `-n`, the in-process controller inherits the outer worker's id and would log its own git
        # calls.
        monkeypatch.delenv("PYTEST_XDIST_WORKER", raising=False)
        sha = make_repository(pytester.path)
        calls = pytester.path / "git.log"
        make_xdist_conftest(pytester, GIT_LOGGING.format(calls=str(calls)))
        tags = pytester.path / "tags.log"
        test = (
            "def test_{i}(lakeflow):\n"
            "    with lakeflow.job('my_job'):\n"
            "        pass\n"
            f"    with open({str(tags)!r}, 'a') as f:\n"
            "        tags = lakeflow.ws.jobs.created[-1]['tags']\n"
            "        f.write(json.dumps({{**tags, 'worker': os.environ['PYTEST_XDIST_WORKER']}}) + '\\n')\n"
        )
        pytester.makepyfile("import json\nimport os\n\n" + "\n".join(test.format(i=i) for i in range(6)))
        pytester.runpytest("-n", "2").assert_outcomes(passed=6)
        created = [json.loads(line) for line in tags.read_text().splitlines()]
        assert len(created) == 6
        assert {t["worker"] for t in created} == {"gw0", "gw1"}
        assert {t["pytest_git_sha"] for t in created} == {sha}
        assert {t["pytest_git_branch"] for t in created} == {"topic"}
        assert {t["pytest_user"] for t in created} == {"someone@example.com"}
        assert not calls.exists(), calls.read_text()

    def test_xdist_sweeps_once_with_a_report(self, pytester: pytest.Pytester, bundle_dir: Path) -> None:
        pytest.importorskip("xdist")
        deleted = pytester.path / "deleted.log"
        make_xdist_conftest(pytester, STAGING_FIXTURE + SWEEP_LOGGING.format(deleted=str(deleted)))
        test = "def test_{i}(lakeflow, staging):\n    lakeflow.template('my_job')\n    staging.template('my_job')\n"
        pytester.makepyfile("\n".join(test.format(i=i) for i in range(6)))
        result = pytester.runpytest("-n", "2", "--lakeflow-sweep", "--lakeflow-sweep-report=sweep.json")
        result.assert_outcomes(passed=6)
        assert deleted.read_text().split() == ["1", "2", "7"]
        report = json.loads((pytester.path / "sweep.json").read_text())
        assert (report["dry_run"], report["jobs"]) == (False, [1, 2, 7])


class TestRunCapPerWorker:
    """`--lakeflow-max-concurrent-runs` caps the runs of each xdist worker, not of the whole session."""

    def test_cap_is_per_worker(self, pytester: pytest.Pytester, bundle_dir: Path) -> None:
        pytest.importorskip("xdist")
        make_xdist_conftest(pytester)
        started = pytester.path / "started.log"
        # Each worker holds its one slot until the other worker has started a run too.
        pytester.makepyfile(
            f"""
            import os
            import time

            def started():
                with open({str(started)!r}) as f:
                    return f.read().split()

            def test_start(lakeflow):
                with lakeflow.job("my_job") as job:
                    job.start()
                    with open({str(started)!r}, "a") as f:
                        f.write(os.environ["PYTEST_XDIST_WORKER"] + "\\n")
                    deadline = time.monotonic() + 10
                    while len(started()) < 2 and time.monotonic() < deadline:
                        time.sleep(0.05)
                    assert len(started()) == 2
            """
        )
        result = pytester.runpytest("-n", "2", "--dist", "each", "--lakeflow-max-concurrent-runs=1")
        result.assert_outcomes(passed=2)
        assert sorted(started.read_text().split()) == ["gw0", "gw1"]


class TestRunId:
    """The session's run id comes from the CI environment, or is random."""

    @pytest.mark.parametrize(
        ("env", "expected"),
        [
            ({"LAKEFLOW_RUN_ID": "mine", "GITHUB_RUN_ID": "123"}, "mine"),
            ({"GITHUB_RUN_ID": "123", "BUILD_BUILDID": "456"}, "123"),
            ({"BUILD_BUILDID": "456"}, "456"),
        ],
    )
    def test_run_id_from_environment(self, monkeypatch: pytest.MonkeyPatch, env: dict[str, str], expected: str) -> None:
        set_ci(monkeypatch, env)
        assert session.resolve_run_id() == expected

    def test_random_run_id(self, monkeypatch: pytest.MonkeyPatch) -> None:
        set_ci(monkeypatch, {})
        run_id = session.resolve_run_id()
        assert len(run_id) == 8
        assert run_id != session.resolve_run_id()


class TestAttribution:
    """The CI run, commit, branch and user that tag ephemeral jobs."""

    @pytest.mark.parametrize(
        ("env", "expected"),
        [
            pytest.param(
                {**AZURE_DEVOPS, **GITHUB},
                {
                    "pytest_ci_url": "https://github.com/o/r/actions/runs/9",
                    "pytest_git_sha": "abc",
                    "pytest_git_branch": "main",
                    "pytest_user": "someone",
                },
                id="github-wins-over-azure-devops",
            ),
            pytest.param(
                {**GITHUB, "GITHUB_HEAD_REF": "feature/x"},
                {
                    "pytest_ci_url": "https://github.com/o/r/actions/runs/9",
                    "pytest_git_sha": "abc",
                    "pytest_git_branch": "feature/x",
                    "pytest_user": "someone",
                },
                id="github-head-ref-wins-over-ref-name",
            ),
            pytest.param(
                AZURE_DEVOPS,
                {
                    "pytest_ci_url": "https://dev.azure.com/org/proj/_build/results?buildId=42",
                    "pytest_git_sha": "def",
                    "pytest_git_branch": "feature",
                    "pytest_user": "person@example.com",
                },
                id="azure-devops",
            ),
            pytest.param(
                {**AZURE_DEVOPS, "SYSTEM_TEAMPROJECT": "My Project"},
                {
                    "pytest_ci_url": "https://dev.azure.com/org/My%20Project/_build/results?buildId=42",
                    "pytest_git_sha": "def",
                    "pytest_git_branch": "feature",
                    "pytest_user": "person@example.com",
                },
                id="azure-devops-project-is-url-quoted",
            ),
        ],
    )
    def test_attribution_from_ci(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, env: dict[str, str], expected: dict[str, str]
    ) -> None:
        set_ci(monkeypatch, env)
        assert session.resolve_attribution(tmp_path) == expected

    def test_attribution_from_git(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        set_ci(monkeypatch, {})
        sha = make_repository(tmp_path)
        assert session.resolve_attribution(tmp_path) == {"pytest_git_sha": sha, "pytest_git_branch": "topic"}

    def test_attribution_on_a_detached_head_has_no_branch(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        set_ci(monkeypatch, {})
        sha = make_repository(tmp_path)
        git(tmp_path, "checkout", "-q", "--detach")
        assert session.resolve_attribution(tmp_path) == {"pytest_git_sha": sha}

    def test_attribution_without_git_is_empty(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        set_ci(monkeypatch, {})

        def missing(*args: Any, **kwargs: Any) -> None:
            raise FileNotFoundError("git")

        monkeypatch.setattr(subprocess, "run", missing)
        assert session.resolve_attribution(tmp_path) == {}

    def test_attribution_outside_a_repository_is_empty(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        set_ci(monkeypatch, {})
        monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path.parent))
        assert session.resolve_attribution(tmp_path) == {}


class TestWorkspaceClient:
    """How `Lakeflow` gets its workspace client, and which profile reaches the CLI."""

    def test_client_construction_retries_token_contention(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        contended = ContendedTokenCache(collisions=2)
        sleeps: list[float] = []
        monkeypatch.setattr(session, "WorkspaceClient", contended)
        monkeypatch.setattr(time, "sleep", sleeps.append)
        monkeypatch.setattr(random, "uniform", lambda low, high: 0)
        with caplog.at_level(logging.INFO, logger="pytest_databricks_lakeflow"):
            assert cast(Any, session.make_workspace_client("dev")) == "client"
        assert contended.profiles == ["dev", "dev", "dev"]
        assert sleeps == [2, 4]
        assert caplog.messages == [f"Retrying workspace client after: {TOKEN_RACE}"] * 2

    @pytest.mark.parametrize(
        ("bound", "expected"), [pytest.param(0, [2, 4], id="lower"), pytest.param(1, [4, 8], id="upper")]
    )
    def test_collided_workers_retry_at_different_times(
        self, monkeypatch: pytest.MonkeyPatch, bound: int, expected: list[float]
    ) -> None:
        sleeps: list[float] = []
        monkeypatch.setattr(session, "WorkspaceClient", ContendedTokenCache(collisions=2))
        monkeypatch.setattr(time, "sleep", sleeps.append)
        monkeypatch.setattr(random, "uniform", lambda low, high: (low, high)[bound])
        session.make_workspace_client("dev")
        assert sleeps == expected

    def test_client_construction_gives_up_after_the_last_attempt(self, monkeypatch: pytest.MonkeyPatch) -> None:
        contended = ContendedTokenCache(collisions=session.CLIENT_ATTEMPTS)
        monkeypatch.setattr(session, "WorkspaceClient", contended)
        monkeypatch.setattr(time, "sleep", lambda seconds: None)
        with pytest.raises(ValueError, match="cannot get access token"):
            session.make_workspace_client("dev")
        assert len(contended.profiles) == session.CLIENT_ATTEMPTS

    def test_client_construction_fails_fast_on_other_errors(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def broken(profile: str | None = None) -> None:
            raise ValueError("default auth: cannot configure default credentials")

        monkeypatch.setattr(session, "WorkspaceClient", broken)
        monkeypatch.setattr(time, "sleep", lambda seconds: pytest.fail("should not sleep"))
        with pytest.raises(ValueError, match="cannot configure default credentials"):
            session.make_workspace_client(None)

    def test_workspace_client_is_made_once_from_the_profile(self, fake_ws: FakeWorkspaceClient) -> None:
        lakeflow = Lakeflow(profile="dev")
        assert lakeflow.ws is cast(Any, fake_ws)
        assert lakeflow.ws is cast(Any, fake_ws)
        assert fake_ws.profiles == ["dev"]

    def test_a_given_workspace_client_is_used_as_is(self, fake_ws: FakeWorkspaceClient) -> None:
        client = as_client(FakeWorkspaceClient())
        assert Lakeflow(workspace_client=client).ws is client
        assert fake_ws.profiles == []

    def test_a_given_workspace_client_profile_goes_to_the_cli(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        pytester.makeconftest(
            f"""
            import sys

            import pytest

            from pytest_databricks_lakeflow import Lakeflow

            sys.path.insert(0, {str(UNIT_TESTS)!r})

            from fakes import FakeWorkspaceClient, example_settings

            CLIENT = FakeWorkspaceClient()
            CLIENT.config.profile = "from-client"
            CLIENT.jobs.add(example_settings())


            @pytest.fixture(scope="session")
            def lakeflow():
                return Lakeflow(workspace_client=CLIENT)
            """
        )
        pytester.makepyfile(
            """
            import conftest

            def test_one(lakeflow):
                assert lakeflow.ws is conftest.CLIENT
                assert lakeflow.template("my_job").tasks
            """
        )
        pytester.runpytest().assert_outcomes(passed=1)
        assert fake_ws.profiles == []
        assert {profile for args, _, profile in fake_cli.invocations if args[0] == "bundle"} == {"from-client"}


class TestBundleRoot:
    """Where the bundle is found when `bundle_root` is given, relative or absolute, or not given."""

    def test_resolve_root(self, tmp_path: Path) -> None:
        (tmp_path / "databricks.yml").write_text("bundle: {name: x}\n")
        (tmp_path / "tests").mkdir()
        (tmp_path / "sub").mkdir()
        (tmp_path / "sub" / "databricks.yml").write_text("bundle: {name: y}\n")
        assert session.resolve_root(None, tmp_path / "tests") == tmp_path
        assert session.resolve_root("sub", tmp_path) == tmp_path / "sub"
        assert session.resolve_root(tmp_path / "sub", tmp_path / "tests") == tmp_path / "sub"
        with pytest.raises(LakeflowBundleError, match=r"^bundle_root .*examples/bundle has no databricks.yml"):
            session.resolve_root("examples/bundle", tmp_path)

    def test_bundle_root_is_relative_to_the_rootdir(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli
    ) -> None:
        pytester.makeini("[pytest]\n")
        pytester.mkdir("bundle").joinpath("databricks.yml").write_text("bundle:\n  name: example\n")
        configure_lakeflow(pytester, 'bundle_root="bundle"')
        pytester.mkdir("tests").joinpath("test_one.py").write_text("def test_one(lakeflow):\n    lakeflow.templates\n")
        pytester.runpytest("tests").assert_outcomes(passed=1)
        assert (DEPLOY, pytester.path / "bundle", None) in fake_cli.invocations


class TestSetup:
    """What setting up a bundle target runs, and the state it produces for each sync strategy."""

    def test_templates_from_summary(self) -> None:
        ws = FakeWorkspaceClient()
        job_id = ws.jobs.add(example_settings())
        summary = {"resources": {"jobs": {"my_job": {"id": str(job_id)}, "gone": {"id": "1"}, "new": {}}}}
        templates, errors = session.templates_from_summary(as_client(ws), summary)
        assert templates == {"my_job": example_settings().as_dict()}
        assert "stale" in errors["gone"]
        assert "not deployed" in errors["new"]

    def test_setup_with_deploy(self, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path) -> None:
        state = session.setup(as_client(fake_ws), bundle_key(bundle_dir, profile="dev"))
        assert fake_cli.invocations == [
            (["--version"], bundle_dir, None),
            (VALIDATE, bundle_dir, "dev"),
            (DEPLOY, bundle_dir, "dev"),
            (SUMMARY, bundle_dir, "dev"),
        ]
        folder = "/Users/someone@example.com/.pytest-databricks-lakeflow"
        assert state == {
            "templates": {"my_job": example_settings().as_dict()},
            "template_errors": {},
            "helpers": {"set_values": f"{folder}/set_values", "get_values": f"{folder}/get_values"},
            "user": "someone@example.com",
            "job_ids": {"my_job": fake_cli.deployed["my_job"]},
            "pipeline_ids": {},
            "pipeline_errors": {},
            "unversioned_artifacts": [],
            "artifact_path": "",
            "root": str(bundle_dir),
            "stale_uploads": [],
        }
        assert set(fake_ws.workspace.uploads) == {f"{folder}/set_values", f"{folder}/get_values"}
        assert all(upload["overwrite"] for upload in fake_ws.workspace.uploads.values())
        assert json.loads(json.dumps(state)) == state

    def test_setup_with_none_uses_validate(
        self, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        fake_cli.validate = {"resources": {"jobs": {"other": {"name": "o", "tasks": [{"task_key": "t"}]}}}}
        key = bundle_key(bundle_dir, sync="none", helpers_path="/Shared/helpers/")
        state = session.setup(as_client(fake_ws), key)
        assert [call[:2] for call in fake_cli.calls] == [["--version"], ["bundle", "validate"]]
        assert state["templates"] == {"other": {"name": "o", "tasks": [{"task_key": "t"}]}}
        assert state["helpers"]["get_values"] == "/Shared/helpers/get_values"
        assert (state["job_ids"], state["pipeline_ids"], state["pipeline_errors"]) == ({}, {}, {})

    @pytest.mark.parametrize(("sync", "stale"), [("none", [f"dist/{WHEEL}"]), ("deploy", [])])
    def test_setup_records_artifact_path_and_stale_uploads(
        self, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path, sync: str, stale: list[str]
    ) -> None:
        stale_wheel(fake_ws, fake_cli, bundle_dir)
        fake_cli.summary = {"workspace": {"artifact_path": ARTIFACT_PATH}}
        state = session.setup(as_client(fake_ws), bundle_key(bundle_dir, sync=sync))
        assert state["artifact_path"] == ARTIFACT_PATH
        assert state["root"] == str(bundle_dir)
        assert state["stale_uploads"] == stale

    def test_setup_with_sync_syncs_first(
        self, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        session.setup(as_client(fake_ws), bundle_key(bundle_dir, sync="sync"))
        assert [call[:2] for call in fake_cli.calls] == [["--version"], ["bundle", "sync"], ["bundle", "validate"]]

    @pytest.mark.parametrize(("sync", "commands"), [("deploy", 3), ("sync", 2), ("none", 1)])
    def test_setup_passes_variables_to_every_cli_call(
        self, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path, sync: str, commands: int
    ) -> None:
        session.setup(as_client(fake_ws), bundle_key(bundle_dir, sync=sync, variables=(("catalog", "dev"),)))
        bundle_calls = [call for call in fake_cli.calls if call[0] == "bundle"]
        assert len(bundle_calls) == commands
        assert all(call[-2:] == ["--var", "catalog=dev"] for call in bundle_calls)

    @pytest.mark.parametrize(
        ("variables", "as_vars"), [((), []), ((("catalog", "dev"),), ["--var", "catalog=dev"])], ids=["plain", "vars"]
    )
    def test_setup_passes_deploy_flags_after_variables(
        self,
        fake_ws: FakeWorkspaceClient,
        fake_cli: FakeCli,
        bundle_dir: Path,
        variables: tuple[tuple[str, str], ...],
        as_vars: list[str],
    ) -> None:
        key = bundle_key(bundle_dir, variables=variables, deploy_flags=("--fail-on-active-runs",))
        session.setup(as_client(fake_ws), key)
        assert fake_cli.calls[2] == [*DEPLOY, *as_vars, "--fail-on-active-runs"]
        assert fake_cli.calls[3] == [*SUMMARY, *as_vars]

    def test_template_copies_are_independent(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        fake_cli.deployed["broken"] = 1
        pytester.makepyfile(
            r"""
            import pytest

            from pytest_databricks_lakeflow import LakeflowBundleError

            def test_copies(lakeflow):
                first = lakeflow.template("my_job")
                first.tasks = []
                first.tags["x"] = "y"
                assert lakeflow.template("my_job").tasks
                assert "x" not in lakeflow.template("my_job").tags
                with pytest.raises(KeyError, match=r"No job 'nope' in the bundle. Available: \['my_job'\]"):
                    lakeflow.template("nope")
                with pytest.raises(LakeflowBundleError, match="stale"):
                    lakeflow.template("broken")
            """
        )
        pytester.runpytest().assert_outcomes(passed=1)


class TestCheck:
    """`Lakeflow.check` lints a template from any scope and creates nothing in the workspace."""

    def test_check_from_a_session_scoped_fixture(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        fake_cli.deployed["broken"] = 1
        configure_lakeflow(
            pytester,
            "",
            '\n\n@pytest.fixture(scope="session")\ndef problems(lakeflow):\n    return lakeflow.check("my_job")\n',
        )
        pytester.makepyfile(
            r"""
            import pytest

            from pytest_databricks_lakeflow import LakeflowBundleError

            def test_check(lakeflow, problems):
                assert problems == []
                with pytest.raises(KeyError, match=r"No job 'nope' in the bundle. Available: \['my_job'\]"):
                    lakeflow.check("nope")
                with pytest.raises(LakeflowBundleError, match="stale"):
                    lakeflow.check("broken")
            """
        )
        pytester.runpytest().assert_outcomes(passed=1)
        assert fake_ws.jobs.created == []
        assert fake_ws.jobs.run_now_calls == []

    def test_check_outside_any_test(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        # The fixture above runs during the first test's setup; pytest_sessionstart runs before any test item exists.
        pytester.makeconftest(
            """
            from pytest_databricks_lakeflow import Lakeflow

            def pytest_sessionstart(session):
                print("problems:", Lakeflow().check("my_job"))
            """
        )
        pytester.makepyfile("def test_nothing():\n    pass\n")
        result = pytester.runpytest("-s")
        result.assert_outcomes(passed=1)
        result.stdout.fnmatch_lines(["problems: []"])
        assert fake_ws.jobs.created == []

    def test_check_with_sync_none_only_validates(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        settings = example_settings()
        settings.tasks = [jobs.Task(task_key="t", depends_on=[jobs.TaskDependency(task_key="missing")])]
        fake_cli.validate = {"resources": {"jobs": {"my_job": settings.as_dict()}}}
        configure_lakeflow(pytester, 'sync="none"')
        pytester.makepyfile(
            """
            def test_check(lakeflow):
                assert lakeflow.check("my_job") == [
                    "Task 't': depends on 'missing', which is not a task in the job. Tasks: ['t']"
                ]
            """
        )
        pytester.runpytest().assert_outcomes(passed=1)
        assert [args for args in fake_cli.calls if args[0] == "bundle"] == [VALIDATE]
        assert fake_ws.jobs.created == []
        assert fake_ws.jobs.run_now_calls == []

    def test_check_reads_the_built_wheels_and_the_workspace(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        settings = example_settings()
        load = next(task for task in settings.tasks or [] if task.task_key == "load")
        load.libraries = [compute.Library(whl="/Workspace/libs/gone.whl"), compute.Library(whl="/Workspace/libs/a.whl")]
        fake_ws.workspace.objects.add("/Workspace/libs/a.whl")
        fake_cli.deployed["my_job"] = fake_ws.jobs.add(settings)
        build_wheel(bundle_dir / "dist")
        pytester.makepyfile(
            """
            from pytest_databricks_lakeflow import DEFAULT_RULES

            def test_check(lakeflow):
                assert lakeflow.check("my_job") == [
                    "Task 'load': entry point 'load' is not in dist/pkg-0.1.0-py3-none-any.whl. console_scripts: "
                    "['main'], and pkg/__init__.py does not define it.",
                    "Task 'load': /Workspace/libs/gone.whl does not exist.",
                ]
                assert lakeflow.check("my_job", rules=DEFAULT_RULES - {"wheel", "library"}) == []
            """
        )
        pytester.runpytest().assert_outcomes(passed=1)
        assert fake_ws.jobs.created == []
        assert fake_ws.jobs.run_now_calls == []


class TestHostCheck:
    """Setup refuses a bundle target that deploys to a different workspace from the client's."""

    @pytest.mark.parametrize(
        ("sync", "bundle_host"),
        [
            ("deploy", OTHER_HOST),
            ("none", OTHER_HOST),
            ("none", OTHER_HOST.removeprefix("https://")),
        ],
        ids=["deploy", "none", "none-schemeless"],
    )
    def test_setup_fails_on_host_mismatch(
        self, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path, sync: str, bundle_host: str
    ) -> None:
        fake_cli.summary = {"workspace": {"host": bundle_host}}
        fake_cli.validate = {"workspace": {"host": bundle_host}}
        with pytest.raises(LakeflowError) as excinfo:
            session.setup(as_client(fake_ws), bundle_key(bundle_dir, sync=sync))
        message = str(excinfo.value)
        assert f"'test' deploys to {bundle_host} but the workspace client is for {HOST}" in message
        assert "Pass profile= to Lakeflow" in message
        assert fake_ws.jobs.gets == []
        assert fake_ws.workspace.uploads == {}

    def test_deploy_checks_the_validated_host_before_run_as_and_the_deploy(
        self, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        # A run_as that the run_as check refuses, so the host check must come first to be the error raised.
        fake_cli.validate = {"workspace": {"host": OTHER_HOST}, "run_as": {"user_name": "other@example.com"}}
        with pytest.raises(LakeflowError) as excinfo:
            session.setup(as_client(fake_ws), bundle_key(bundle_dir))
        assert f"'test' deploys to {OTHER_HOST} but the workspace client is for {HOST}" in str(excinfo.value)
        assert [call[:2] for call in fake_cli.calls] == [["--version"], ["bundle", "validate"]]
        assert fake_ws.current_user.me_calls == 0

    @pytest.mark.parametrize(
        ("bundle_host", "client_host"),
        [
            (f"{HOST}/", HOST),
            (HOST.removeprefix("https://"), HOST),
            (HOST.upper(), HOST),
            ("http://example.cloud.databricks.com", HOST),
            (None, HOST),
            (OTHER_HOST, None),
        ],
        ids=["trailing-slash", "no-scheme", "upper-case", "http", "no-bundle-host", "no-client-host"],
    )
    def test_check_host_accepts(self, bundle_host: str | None, client_host: str | None) -> None:
        session.check_host("test", bundle_host, client_host)

    @pytest.mark.parametrize("sync", ["deploy", "none"])
    def test_setup_skips_the_host_check_for_a_null_workspace(
        self, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path, sync: str
    ) -> None:
        fake_cli.summary = {**fake_cli.summary, "workspace": None}
        fake_cli.validate = {**fake_cli.validate, "workspace": None}
        session.setup(as_client(fake_ws), bundle_key(bundle_dir, sync=sync))

    def test_check_host_names_both_hosts_and_the_target(self) -> None:
        with pytest.raises(LakeflowError) as excinfo:
            session.check_host("ci", "https://adb-1.azuredatabricks.net", "https://adb-2.azuredatabricks.net")
        assert str(excinfo.value) == (
            "Bundle target 'ci' deploys to https://adb-1.azuredatabricks.net but the workspace client is for "
            "https://adb-2.azuredatabricks.net. Pass profile= to Lakeflow, or set DATABRICKS_HOST and "
            "DATABRICKS_CONFIG_PROFILE to match the target."
        )

    def test_host_mismatch_fails_later_tests_too(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        fake_cli.summary = {"workspace": {"host": OTHER_HOST}}
        test = "def test_{0}(lakeflow):\n    lakeflow.template('my_job')\n"
        pytester.makepyfile(test.format("a") + "\n" + test.format("b"))
        result = pytester.runpytest()
        result.assert_outcomes(failed=2)
        result.stdout.fnmatch_lines(
            ["*LakeflowError: Bundle target 'test' deploys to*", "*failed earlier in this session: LakeflowError*"]
        )
        assert fake_cli.calls.count(DEPLOY) == 1
        assert fake_ws.jobs.gets == []


class TestRunAsCheck:
    """Before `bundle deploy`, setup checks that the deploying identity can deploy the target's and jobs' run_as."""

    @pytest.mark.parametrize(
        ("validate", "message"),
        [
            pytest.param(
                {"run_as": SP_RUN_AS, "resources": {"jobs": {"my_job": {"run_as": SP_RUN_AS}}}},
                f"Target 'test' sets run_as {APP_ID}, but this session deploys as someone@example.com, which does "
                f"not have the Service Principal User role on it. {SP_FIX} {RUN_AS_FIX}",
                id="target-service-principal",
            ),
            pytest.param(
                {"run_as": {"user_name": "other@example.com"}},
                f"Target 'test' sets run_as other@example.com, but this session deploys as someone@example.com. "
                f"{RUN_AS_FIX}",
                id="target-user",
            ),
            pytest.param(
                {"resources": {"jobs": {"mine": {}, "other": {"run_as": {"user_name": "other@example.com"}}}}},
                f"Job 'other' of target 'test' sets run_as other@example.com, but this session deploys as "
                f"someone@example.com. {RUN_AS_FIX}",
                id="job-user",
            ),
        ],
    )
    def test_setup_refuses_a_foreign_run_as(
        self,
        fake_ws: FakeWorkspaceClient,
        fake_cli: FakeCli,
        bundle_dir: Path,
        validate: dict[str, Any],
        message: str,
    ) -> None:
        fake_ws.account_access_control_proxy.grant(APP_ID, SP_MANAGER, "users/someone@example.com")
        fake_ws.account_access_control_proxy.grant(APP_ID, SP_USER, "users/other@example.com", "groups/other")
        fake_cli.validate = validate
        with pytest.raises(LakeflowBundleError) as excinfo:
            session.setup(as_client(fake_ws), bundle_key(bundle_dir))
        assert str(excinfo.value) == message
        assert DEPLOY not in fake_cli.calls

    @pytest.mark.parametrize(
        "principal",
        ["users/someone@example.com", "users/Someone@Example.com", "groups/data-eng"],
        ids=["user", "user-other-case", "group-of-the-user"],
    )
    def test_setup_accepts_run_as_with_the_user_role(
        self, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path, principal: str
    ) -> None:
        fake_ws.current_user.groups = ["data-eng"]
        fake_ws.account_access_control_proxy.grant(APP_ID, SP_USER, principal)
        fake_cli.validate = {"run_as": SP_RUN_AS, "resources": {"jobs": {"my_job": {"run_as": SP_RUN_AS}}}}
        session.setup(as_client(fake_ws), bundle_key(bundle_dir))
        assert [call[:2] for call in fake_cli.calls] == [
            ["--version"],
            ["bundle", "validate"],
            ["bundle", "deploy"],
            ["bundle", "summary"],
        ]
        assert fake_ws.account_access_control_proxy.names == [sp_rule_set(APP_ID)]

    @pytest.mark.parametrize(
        "run_as",
        [{"user_name": "someone@example.com"}, {"user_name": "Someone@Example.com"}, None],
        ids=["self", "self-other-case", "none"],
    )
    def test_setup_accepts_run_as_self(
        self, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path, run_as: dict[str, str] | None
    ) -> None:
        fake_cli.validate = {"run_as": run_as, "resources": {"jobs": {"my_job": example_settings().as_dict()}}}
        session.setup(as_client(fake_ws), bundle_key(bundle_dir))
        assert DEPLOY in fake_cli.calls
        assert fake_ws.account_access_control_proxy.names == []

    @pytest.mark.parametrize(
        ("account_id", "rule_set"),
        [
            pytest.param(None, None, id="no-account-id"),
            pytest.param(ACCOUNT_ID, PermissionDenied("not an account member"), id="rule-set-denied"),
            pytest.param(ACCOUNT_ID, None, id="unknown-service-principal"),
        ],
    )
    def test_role_check_is_skipped_without_an_account_id(
        self,
        fake_ws: FakeWorkspaceClient,
        fake_cli: FakeCli,
        bundle_dir: Path,
        account_id: str | None,
        rule_set: Exception | None,
    ) -> None:
        fake_ws.config.account_id = account_id
        if rule_set is not None:
            fake_ws.account_access_control_proxy.rule_sets[sp_rule_set(APP_ID)] = rule_set
        fake_cli.validate = {"run_as": SP_RUN_AS}
        session.setup(as_client(fake_ws), bundle_key(bundle_dir))
        assert DEPLOY in fake_cli.calls

    @pytest.mark.parametrize("sync", ["sync", "none"])
    def test_sync_and_none_never_run_the_check(
        self, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path, sync: str
    ) -> None:
        fake_cli.validate = {"run_as": {"user_name": "other@example.com"}}
        session.setup(as_client(fake_ws), bundle_key(bundle_dir, sync=sync))
        assert fake_ws.account_access_control_proxy.names == []

    def test_setup_accepts_a_service_principal_deploying_as_itself(
        self, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        fake_ws.current_user.user_name = APP_ID
        fake_cli.validate = {"run_as": SP_RUN_AS}
        session.setup(as_client(fake_ws), bundle_key(bundle_dir))
        assert DEPLOY in fake_cli.calls
        assert fake_ws.account_access_control_proxy.names == []


class TestLazySetup:
    """`Lakeflow` sets up each bundle target once, on first use, and remembers a failure for the session."""

    def test_lakeflow_sets_up_once_when_first_used(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        pytester.makepyfile(
            """
            def test_one(lakeflow):
                assert sorted(lakeflow.templates) == ["my_job"]

            def test_two(lakeflow):
                assert lakeflow.template("my_job").tasks[0].task_key == "extract"
            """
        )
        pytester.runpytest().assert_outcomes(passed=2)
        assert fake_cli.calls.count(DEPLOY) == 1
        assert fake_ws.profiles == [None]

    def test_requesting_lakeflow_without_using_it_sets_nothing_up(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        pytester.makepyfile("def test_unused(lakeflow):\n    assert lakeflow.target == 'test'\n")
        pytester.runpytest().assert_outcomes(passed=1)
        assert fake_cli.calls == []
        assert fake_ws.profiles == []

    def test_no_setup_when_lakeflow_tests_are_deselected(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        pytester.makepyfile("def test_one(lakeflow):\n    lakeflow.template('my_job')\n\ndef test_two():\n    pass\n")
        pytester.runpytest("-m", "not lakeflow").assert_outcomes(passed=1, deselected=1)
        assert fake_cli.calls == []

    def test_overriding_the_fixture_configures_lakeflow(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        configure_lakeflow(pytester, 'target="ci", profile="dev"')
        pytester.makepyfile("def test_one(lakeflow):\n    assert lakeflow.template('my_job').tasks\n")
        pytester.runpytest().assert_outcomes(passed=1)
        assert (["bundle", "deploy", "-t", "ci", "--auto-approve"], bundle_dir, "dev") in fake_cli.invocations
        assert fake_ws.profiles == ["dev"]

    def test_lakeflows_for_the_same_bundle_share_one_setup(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        pytester.makeconftest(
            """
            import dataclasses

            import pytest

            from pytest_databricks_lakeflow import Lakeflow


            @pytest.fixture
            def lakeflow():
                return Lakeflow(catalog="a")


            @pytest.fixture
            def other(lakeflow):
                return dataclasses.replace(lakeflow, catalog="b")
            """
        )
        pytester.makepyfile(
            """
            def test_one(lakeflow):
                lakeflow.template("my_job")

            def test_two(lakeflow, other):
                assert other.catalog == "b"
                lakeflow.template("my_job")
                other.template("my_job")
            """
        )
        pytester.runpytest().assert_outcomes(passed=2)
        assert fake_cli.calls.count(DEPLOY) == 1

    def test_each_bundle_target_is_set_up_once(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        configure_lakeflow(pytester, "", STAGING_FIXTURE)
        test = "def test_{0}(lakeflow, staging):\n    lakeflow.template('my_job')\n    staging.template('my_job')\n"
        pytester.makepyfile(test.format("a") + "\n" + test.format("b"))
        pytester.runpytest().assert_outcomes(passed=2)
        assert [call for call in fake_cli.calls if call[:2] == ["bundle", "deploy"]] == [DEPLOY, DEPLOY_STAGING]

    def test_variables_are_part_of_the_bundle_target(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        configure_lakeflow(
            pytester,
            'variables={"catalog": "a"}',
            '\n\n@pytest.fixture(scope="session")\ndef same():\n    return Lakeflow(variables={"catalog": "a"})\n'
            '\n\n@pytest.fixture(scope="session")\ndef other():\n    return Lakeflow(variables={"catalog": "b"})\n',
        )
        pytester.makepyfile(
            "def test_a(lakeflow, same, other):\n"
            "    lakeflow.template('my_job')\n    same.template('my_job')\n    other.template('my_job')\n"
        )
        pytester.runpytest().assert_outcomes(passed=1)
        assert [call for call in fake_cli.calls if call[:2] == ["bundle", "deploy"]] == [
            [*DEPLOY, "--var", "catalog=a"],
            [*DEPLOY, "--var", "catalog=b"],
        ]

    def test_deploy_flags_from_the_fixture(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        configure_lakeflow(pytester, 'deploy_flags=["--fail-on-active-runs"]')
        pytester.makepyfile("def test_one(lakeflow):\n    assert lakeflow.template('my_job').tasks\n")
        pytester.runpytest().assert_outcomes(passed=1)
        assert fake_cli.calls[2] == [*DEPLOY, "--fail-on-active-runs"]

    def test_lakeflows_with_different_deploy_flags_deploy_separately(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        pytester.makeconftest(
            "import pytest\nfrom pytest_databricks_lakeflow import Lakeflow\n\n"
            "@pytest.fixture(scope='session')\ndef plain():\n    return Lakeflow()\n\n"
            "@pytest.fixture(scope='session')\ndef same():\n    return Lakeflow()\n\n"
            "@pytest.fixture(scope='session')\ndef flagged():\n"
            "    return Lakeflow(deploy_flags=['--fail-on-active-runs'])\n"
        )
        pytester.makepyfile(
            "def test_one(plain, same, flagged):\n"
            "    for lakeflow in (plain, same, flagged):\n"
            "        assert lakeflow.template('my_job').tasks\n"
        )
        pytester.runpytest().assert_outcomes(passed=1)
        deploys = [call for call in fake_cli.calls if call[:2] == ["bundle", "deploy"]]
        assert deploys == [DEPLOY, [*DEPLOY, "--fail-on-active-runs"]]

    def test_deploy_flags_do_not_split_sessions_that_do_not_deploy(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        pytester.makeconftest(
            "import pytest\nfrom pytest_databricks_lakeflow import Lakeflow\n\n"
            "@pytest.fixture(scope='session')\ndef plain():\n    return Lakeflow(sync='sync')\n\n"
            "@pytest.fixture(scope='session')\ndef flagged():\n"
            "    return Lakeflow(sync='sync', deploy_flags=['--fail-on-active-runs'])\n"
        )
        pytester.makepyfile("def test_one(plain, flagged):\n    assert plain.templates == flagged.templates\n")
        pytester.runpytest().assert_outcomes(passed=1)
        assert [call[:2] for call in fake_cli.calls].count(["bundle", "sync"]) == 1

    def test_a_failed_setup_is_not_repeated(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        fake_cli.errors["deploy"] = LakeflowBundleError("deploy broke")
        pytester.makepyfile(
            "def test_a(lakeflow):\n    lakeflow.template('my_job')\n\ndef test_b(lakeflow):\n    lakeflow.templates\n"
        )
        result = pytester.runpytest()
        result.assert_outcomes(failed=2)
        result.stdout.fnmatch_lines(
            [
                "*LakeflowBundleError: deploy broke*",
                "*failed earlier in this session: LakeflowBundleError: deploy broke*",
            ]
        )
        assert fake_cli.calls.count(DEPLOY) == 1

    def test_lakeflow_outside_a_test_fails_clearly(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        pytester.makepyfile(
            """
            from pytest_databricks_lakeflow import Lakeflow, LakeflowError

            try:
                Lakeflow().job("my_job")
            except LakeflowError as e:
                ERROR = str(e)

            def test_error():
                assert "inside a test" in ERROR
            """
        )
        pytester.runpytest().assert_outcomes(passed=1)
        assert fake_cli.calls == []


class TestUnversionedArtifacts:
    """Wheel artifacts without `dynamic_version` warn once per session, or fail with `strict_artifacts=True`."""

    @pytest.mark.parametrize("sync", ["deploy", "none"])
    def test_setup_records_unversioned_artifacts(
        self, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path, sync: str
    ) -> None:
        artifacts = {"artifacts": {"pkg": WHEEL_ARTIFACT, "versioned": {**WHEEL_ARTIFACT, "dynamic_version": True}}}
        if sync == "deploy":
            fake_cli.summary = artifacts
        else:
            fake_cli.validate = artifacts
        state = session.setup(as_client(fake_ws), bundle_key(bundle_dir, sync=sync))
        assert state["unversioned_artifacts"] == ["pkg"]

    @pytest.mark.parametrize(("dynamic_version", "warnings"), [(False, 1), (True, 0)])
    def test_unversioned_wheel_warns_once(
        self,
        pytester: pytest.Pytester,
        fake_ws: FakeWorkspaceClient,
        fake_cli: FakeCli,
        bundle_dir: Path,
        dynamic_version: bool,
        warnings: int,
    ) -> None:
        fake_cli.summary = {"artifacts": {"lakeflow_example": {**WHEEL_ARTIFACT, "dynamic_version": dynamic_version}}}
        pytester.makepyfile(
            """
            import dataclasses

            def test_one(lakeflow):
                lakeflow.template("my_job")
                lakeflow.template("my_job")

            def test_two(lakeflow):
                dataclasses.replace(lakeflow, catalog="other").template("my_job")
            """
        )
        result = pytester.runpytest()
        result.assert_outcomes(passed=2, warnings=warnings)
        assert result.stdout.str().count(f"UserWarning: {UNVERSIONED_MESSAGE}") == warnings

    def test_strict_artifacts_fails_the_first_template_lookup(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        fake_cli.summary = {"artifacts": {"lakeflow_example": WHEEL_ARTIFACT}}
        configure_lakeflow(pytester, "strict_artifacts=True")
        pytester.makepyfile(
            """
            def test_one(lakeflow):
                lakeflow.template("my_job")

            def test_two(lakeflow):
                lakeflow.job("my_job")
            """
        )
        result = pytester.runpytest("-rN")
        result.assert_outcomes(failed=2, warnings=0)
        assert result.stdout.str().count(f"LakeflowError: {UNVERSIONED_MESSAGE}") == 2
        assert fake_cli.calls.count(DEPLOY) == 1
        assert fake_ws.jobs.created == []


class TestStaleUploads:
    """Under `sync="none"`, a local wheel built after its uploaded copy warns once, or fails with `strict_artifacts`."""

    @pytest.mark.parametrize(("uploaded_after_build", "warnings"), [(-120, 1), (5, 0)], ids=["stale", "fresh"])
    def test_stale_upload_warns_once(
        self,
        pytester: pytest.Pytester,
        fake_ws: FakeWorkspaceClient,
        fake_cli: FakeCli,
        bundle_dir: Path,
        uploaded_after_build: int,
        warnings: int,
    ) -> None:
        stale_wheel(fake_ws, fake_cli, bundle_dir, uploaded_after_build)
        configure_lakeflow(pytester, 'sync="none"')
        pytester.makepyfile(
            """
            import dataclasses

            def test_one(lakeflow):
                lakeflow.template("my_job")
                lakeflow.template("my_job")

            def test_two(lakeflow):
                dataclasses.replace(lakeflow, catalog="other").template("my_job")
            """
        )
        result = pytester.runpytest()
        result.assert_outcomes(passed=2, warnings=warnings)
        assert result.stdout.str().count(f"UserWarning: {STALE_MESSAGE}") == warnings

    def test_strict_artifacts_fails_on_a_stale_upload(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        stale_wheel(fake_ws, fake_cli, bundle_dir)
        configure_lakeflow(pytester, 'sync="none", strict_artifacts=True')
        pytester.makepyfile('def test_one(lakeflow):\n    lakeflow.job("my_job")\n')
        result = pytester.runpytest("-rN")
        result.assert_outcomes(failed=1, warnings=0)
        assert f"LakeflowError: {STALE_MESSAGE}" in result.stdout.str()
        assert fake_ws.jobs.created == []

    def test_several_stale_wheels_share_one_warning(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        stale_wheel(fake_ws, fake_cli, bundle_dir)
        other = "other-0.2-py3-none-any.whl"
        (bundle_dir / "dist" / other).write_text("")
        fake_ws.workspace.objects.add(f"{ARTIFACT_PATH}/.internal/{other}")
        fake_ws.workspace.modified[f"{ARTIFACT_PATH}/.internal/{other}"] = 0
        configure_lakeflow(pytester, 'sync="none"')
        pytester.makepyfile('def test_one(lakeflow):\n    lakeflow.template("my_job")\n')
        result = pytester.runpytest()
        result.assert_outcomes(passed=1, warnings=1)
        result.stdout.fnmatch_lines(
            [
                f"*UserWarning: dist/{other}, dist/{WHEEL} were built after their uploaded copies. "
                "Run `databricks bundle deploy -t test`."
            ]
        )

    @pytest.mark.parametrize("folder", ["/Workspace/b/artifacts/.internal", "/Volumes/c/s/v"])
    def test_modified_at_reads_workspace_and_volumes_paths(self, fake_ws: FakeWorkspaceClient, folder: str) -> None:
        path = f"{folder}/{WHEEL}"
        fake_ws.workspace.objects.add(path)
        fake_ws.workspace.modified[path] = 1_700_000_000_000
        assert session.modified_at(as_client(fake_ws), path) == 1_700_000_000_000
        assert session.modified_at(as_client(fake_ws), f"{folder}/missing.whl") is None


class TestHelperNotebooks:
    """The helper notebooks that setup uploads read the plugin's prefixed widgets."""

    def test_helper_sources_use_prefixed_widgets(self) -> None:
        assert f'dbutils.widgets.get("{helpers.VALUES_WIDGET}")' in helpers.SET_VALUES
        assert f'dbutils.widgets.get("{helpers.TASK_KEY_WIDGET}")' in helpers.GET_VALUES
        assert f'dbutils.widgets.get("{helpers.KEYS_WIDGET}")' in helpers.GET_VALUES
        for source in helpers.NOTEBOOKS.values():
            compile(source, "notebook", "exec")
