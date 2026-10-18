from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path
from typing import Any, cast

import pytest
from databricks.sdk import WorkspaceClient

from pytest_databricks_lakeflow import Lakeflow, helpers, session
from pytest_databricks_lakeflow.errors import LakeflowBundleError, LakeflowError

from .conftest import AZURE_DEVOPS, GITHUB, FakeCli, configure_lakeflow, git, make_repository, set_ci
from .fakes import HOST, FakeWorkspaceClient, example_settings

UNIT_TESTS = Path(__file__).parent
DEPLOY = ["bundle", "deploy", "-t", "test", "--auto-approve"]
DEPLOY_STAGING = ["bundle", "deploy", "-t", "staging", "--auto-approve"]
STAGING_FIXTURE = """

@pytest.fixture(scope="session")
def staging():
    return Lakeflow(target="staging")
"""


def bundle_key(root: Path, **overrides: Any) -> session.BundleKey:
    values: dict[str, Any] = {"root": root, "target": "test", "profile": None, "sync": "deploy", "helpers_path": None}
    return session.BundleKey(**{**values, **overrides})


def as_client(ws: FakeWorkspaceClient) -> WorkspaceClient:
    return cast(WorkspaceClient, ws)


def test_session_state_defaults(tmp_path: Path) -> None:
    state = session.SessionState(rootpath=tmp_path, run_id="r")
    assert state.kept_jobs == []
    assert state.kept_schemas == []
    assert state.cancelled_runs == 0


def test_shared_state_without_xdist_produces_directly() -> None:
    assert session.shared_state(None, lambda: {"templates": {}}) == {"templates": {}}


def test_shared_state_produces_once_for_all_workers(tmp_path: Path) -> None:
    produced: list[str] = []

    def produce() -> dict[str, Any]:
        produced.append("gw0")
        return {"templates": {}}

    path = tmp_path / "state.json"
    assert session.shared_state(path, produce) == {"templates": {}}
    assert session.shared_state(path, produce) == {"templates": {}}
    assert produced == ["gw0"]


def test_shared_state_shares_setup_errors(tmp_path: Path) -> None:
    def fail() -> dict[str, Any]:
        raise LakeflowBundleError("deploy failed")

    path = tmp_path / "state.json"
    with pytest.raises(LakeflowBundleError, match="deploy failed"):
        session.shared_state(path, fail)
    with pytest.raises(LakeflowError, match="failed in another xdist worker: LakeflowBundleError: deploy failed"):
        session.shared_state(path, lambda: {"templates": {}})


def test_bundle_keys_name_distinct_state_files(tmp_path: Path) -> None:
    names = {bundle_key(tmp_path).file_name(), bundle_key(tmp_path, target="ci").file_name()}
    assert len(names) == 2
    assert bundle_key(tmp_path).file_name() == bundle_key(tmp_path).file_name()
    a = bundle_key(tmp_path, variables=(("catalog", "a"),))
    b = bundle_key(tmp_path, variables=(("catalog", "b"),))
    assert len({a.file_name(), b.file_name(), bundle_key(tmp_path).file_name()}) == 3
    assert a.file_name() == bundle_key(tmp_path, variables=(("catalog", "a"),)).file_name()


@pytest.mark.parametrize(
    ("env", "expected"),
    [
        ({"LAKEFLOW_RUN_ID": "mine", "GITHUB_RUN_ID": "123"}, "mine"),
        ({"GITHUB_RUN_ID": "123", "BUILD_BUILDID": "456"}, "123"),
        ({"BUILD_BUILDID": "456"}, "456"),
    ],
)
def test_run_id_from_environment(monkeypatch: pytest.MonkeyPatch, env: dict[str, str], expected: str) -> None:
    for name in session.RUN_ID_VARIABLES:
        monkeypatch.delenv(name, raising=False)
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    assert session.resolve_run_id() == expected


def test_random_run_id(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in session.RUN_ID_VARIABLES:
        monkeypatch.delenv(name, raising=False)
    run_id = session.resolve_run_id()
    assert len(run_id) == 8
    assert run_id != session.resolve_run_id()


def test_attribution_from_github(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    set_ci(monkeypatch, {**AZURE_DEVOPS, **GITHUB})
    assert session.resolve_attribution(tmp_path) == {
        "pytest_ci_url": "https://github.com/o/r/actions/runs/9",
        "pytest_git_sha": "abc",
        "pytest_git_branch": "main",
        "pytest_user": "someone",
    }
    monkeypatch.setenv("GITHUB_HEAD_REF", "feature/x")
    assert session.resolve_attribution(tmp_path)["pytest_git_branch"] == "feature/x"


def test_attribution_from_azure_devops(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    set_ci(monkeypatch, AZURE_DEVOPS)
    assert session.resolve_attribution(tmp_path) == {
        "pytest_ci_url": "https://dev.azure.com/org/proj/_build/results?buildId=42",
        "pytest_git_sha": "def",
        "pytest_git_branch": "feature",
        "pytest_user": "person@example.com",
    }


def test_attribution_quotes_the_azure_devops_project(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    set_ci(monkeypatch, {**AZURE_DEVOPS, "SYSTEM_TEAMPROJECT": "My Project"})
    url = session.resolve_attribution(tmp_path)["pytest_ci_url"]
    assert url == "https://dev.azure.com/org/My%20Project/_build/results?buildId=42"


def test_attribution_from_git(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    set_ci(monkeypatch, {})
    sha = make_repository(tmp_path)
    assert session.resolve_attribution(tmp_path) == {"pytest_git_sha": sha, "pytest_git_branch": "topic"}


def test_attribution_on_a_detached_head_has_no_branch(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    set_ci(monkeypatch, {})
    sha = make_repository(tmp_path)
    git(tmp_path, "checkout", "-q", "--detach")
    assert session.resolve_attribution(tmp_path) == {"pytest_git_sha": sha}


def test_attribution_without_git_is_empty(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    set_ci(monkeypatch, {})

    def missing(*args: Any, **kwargs: Any) -> None:
        raise FileNotFoundError("git")

    monkeypatch.setattr(subprocess, "run", missing)
    assert session.resolve_attribution(tmp_path) == {}


def test_attribution_outside_a_repository_is_empty(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    set_ci(monkeypatch, {})
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path.parent))
    assert session.resolve_attribution(tmp_path) == {}


def test_client_construction_retries_token_contention(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts: list[str | None] = []
    sleeps: list[float] = []

    def flaky(profile: str | None = None) -> str:
        attempts.append(profile)
        if len(attempts) < 3:
            raise ValueError("databricks-cli auth: cannot get access token: forced token refresh: exit status 45")
        return "client"

    monkeypatch.setattr(session, "WorkspaceClient", flaky)
    monkeypatch.setattr(time, "sleep", sleeps.append)
    assert cast(Any, session.make_workspace_client("dev")) == "client"
    assert attempts == ["dev", "dev", "dev"]
    assert sleeps == [2, 4]


def test_client_construction_fails_fast_on_other_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    def broken(profile: str | None = None) -> None:
        raise ValueError("default auth: cannot configure default credentials")

    monkeypatch.setattr(session, "WorkspaceClient", broken)
    monkeypatch.setattr(time, "sleep", lambda seconds: pytest.fail("should not sleep"))
    with pytest.raises(ValueError, match="cannot configure default credentials"):
        session.make_workspace_client(None)


def test_workspace_client_is_made_once_from_the_profile(fake_ws: FakeWorkspaceClient) -> None:
    lakeflow = Lakeflow(profile="dev")
    assert lakeflow.ws is cast(Any, fake_ws)
    assert lakeflow.ws is cast(Any, fake_ws)
    assert fake_ws.profiles == ["dev"]


def test_a_given_workspace_client_is_used_as_is(fake_ws: FakeWorkspaceClient) -> None:
    client = as_client(FakeWorkspaceClient())
    assert Lakeflow(workspace_client=client).ws is client
    assert fake_ws.profiles == []


def test_templates_from_summary() -> None:
    ws = FakeWorkspaceClient()
    job_id = ws.jobs.add(example_settings())
    summary = {"resources": {"jobs": {"my_job": {"id": str(job_id)}, "gone": {"id": "1"}, "new": {}}}}
    templates, errors = session.templates_from_summary(as_client(ws), summary)
    assert templates == {"my_job": example_settings().as_dict()}
    assert "stale" in errors["gone"]
    assert "not deployed" in errors["new"]


def test_resolve_root(tmp_path: Path) -> None:
    (tmp_path / "databricks.yml").write_text("bundle: {name: x}\n")
    (tmp_path / "tests").mkdir()
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "databricks.yml").write_text("bundle: {name: y}\n")
    assert session.resolve_root(None, tmp_path / "tests") == tmp_path
    assert session.resolve_root("sub", tmp_path) == tmp_path / "sub"
    assert session.resolve_root(tmp_path / "sub", tmp_path / "tests") == tmp_path / "sub"
    with pytest.raises(LakeflowBundleError, match=r"^bundle_root .*examples/bundle has no databricks.yml"):
        session.resolve_root("examples/bundle", tmp_path)


def test_setup_with_deploy(fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path) -> None:
    state = session.setup(as_client(fake_ws), bundle_key(bundle_dir, profile="dev"))
    assert fake_cli.invocations == [
        (["--version"], bundle_dir, None),
        (DEPLOY, bundle_dir, "dev"),
        (["bundle", "summary", "-t", "test", "-o", "json"], bundle_dir, "dev"),
    ]
    folder = "/Users/someone@example.com/.pytest-databricks-lakeflow"
    assert state == {
        "templates": {"my_job": example_settings().as_dict()},
        "template_errors": {},
        "helpers": {"set_values": f"{folder}/set_values", "get_values": f"{folder}/get_values"},
        "user": "someone@example.com",
    }
    assert set(fake_ws.workspace.uploads) == {f"{folder}/set_values", f"{folder}/get_values"}
    assert all(upload["overwrite"] for upload in fake_ws.workspace.uploads.values())
    assert json.loads(json.dumps(state)) == state


def test_setup_with_none_uses_validate(fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path) -> None:
    fake_cli.validate = {"resources": {"jobs": {"other": {"name": "o", "tasks": [{"task_key": "t"}]}}}}
    key = bundle_key(bundle_dir, sync="none", helpers_path="/Shared/helpers/")
    state = session.setup(as_client(fake_ws), key)
    assert [call[:2] for call in fake_cli.calls] == [["--version"], ["bundle", "validate"]]
    assert state["templates"] == {"other": {"name": "o", "tasks": [{"task_key": "t"}]}}
    assert state["helpers"]["get_values"] == "/Shared/helpers/get_values"


def test_setup_with_sync_syncs_first(fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path) -> None:
    session.setup(as_client(fake_ws), bundle_key(bundle_dir, sync="sync"))
    assert [call[:2] for call in fake_cli.calls] == [["--version"], ["bundle", "sync"], ["bundle", "validate"]]


def test_setup_fails_on_host_mismatch(fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path) -> None:
    fake_cli.summary = {"workspace": {"host": "https://other.cloud.databricks.com"}}
    with pytest.raises(LakeflowError) as excinfo:
        session.setup(as_client(fake_ws), bundle_key(bundle_dir))
    message = str(excinfo.value)
    assert "'test'" in message
    assert "https://other.cloud.databricks.com" in message
    assert HOST in message
    assert "Pass profile= to Lakeflow" in message
    assert fake_ws.jobs.gets == []
    assert fake_ws.workspace.uploads == {}


def test_setup_checks_the_host_from_validate(fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path) -> None:
    fake_cli.validate = {"workspace": {"host": "other.cloud.databricks.com"}}
    with pytest.raises(LakeflowError, match="deploys to other.cloud.databricks.com but the workspace client is for"):
        session.setup(as_client(fake_ws), bundle_key(bundle_dir, sync="none"))


@pytest.mark.parametrize(
    "bundle_host",
    [
        f"{HOST}/",
        HOST.removeprefix("https://"),
        HOST.upper(),
        "http://example.cloud.databricks.com",
        None,
    ],
)
def test_check_host_normalises(bundle_host: str | None) -> None:
    session.check_host("test", bundle_host, HOST)


def test_check_host_names_both_hosts_and_the_target() -> None:
    with pytest.raises(LakeflowError) as excinfo:
        session.check_host("ci", "https://adb-1.azuredatabricks.net", "https://adb-2.azuredatabricks.net")
    assert str(excinfo.value) == (
        "Bundle target 'ci' deploys to https://adb-1.azuredatabricks.net but the workspace client is for "
        "https://adb-2.azuredatabricks.net. Pass profile= to Lakeflow, or set DATABRICKS_HOST and "
        "DATABRICKS_CONFIG_PROFILE to match the target."
    )


def test_setup_passes_variables_to_every_cli_call(
    fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
) -> None:
    for sync, commands in (("deploy", 2), ("sync", 2), ("none", 1)):
        fake_cli.calls.clear()
        session.setup(as_client(fake_ws), bundle_key(bundle_dir, sync=sync, variables=(("catalog", "dev"),)))
        bundle_calls = [call for call in fake_cli.calls if call[0] == "bundle"]
        assert len(bundle_calls) == commands
        assert all(call[-2:] == ["--var", "catalog=dev"] for call in bundle_calls)


def test_host_mismatch_fails_later_tests_too(
    pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
) -> None:
    fake_cli.summary = {"workspace": {"host": "https://other.cloud.databricks.com"}}
    test = "def test_{0}(lakeflow):\n    lakeflow.template('my_job')\n"
    pytester.makepyfile(test.format("a") + "\n" + test.format("b"))
    result = pytester.runpytest()
    result.assert_outcomes(failed=2)
    result.stdout.fnmatch_lines(
        ["*LakeflowError: Bundle target 'test' deploys to*", "*failed earlier in this session: LakeflowError*"]
    )
    assert fake_cli.calls.count(DEPLOY) == 1
    assert fake_ws.jobs.gets == []


def test_variables_are_part_of_the_bundle_target(
    pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
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


def test_helper_sources_use_prefixed_widgets() -> None:
    assert f'dbutils.widgets.get("{helpers.VALUES_WIDGET}")' in helpers.SET_VALUES
    assert f'dbutils.widgets.get("{helpers.TASK_KEY_WIDGET}")' in helpers.GET_VALUES
    assert f'dbutils.widgets.get("{helpers.KEYS_WIDGET}")' in helpers.GET_VALUES
    for source in helpers.NOTEBOOKS.values():
        compile(source, "notebook", "exec")


def test_template_copies_are_independent(
    pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
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


def test_lakeflow_sets_up_once_when_first_used(
    pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
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
    pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
) -> None:
    pytester.makepyfile("def test_unused(lakeflow):\n    assert lakeflow.target == 'test'\n")
    pytester.runpytest().assert_outcomes(passed=1)
    assert fake_cli.calls == []
    assert fake_ws.profiles == []


def test_no_setup_when_lakeflow_tests_are_deselected(
    pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
) -> None:
    pytester.makepyfile("def test_one(lakeflow):\n    lakeflow.template('my_job')\n\ndef test_two():\n    pass\n")
    pytester.runpytest("-m", "not lakeflow").assert_outcomes(passed=1, deselected=1)
    assert fake_cli.calls == []


def test_overriding_the_fixture_configures_lakeflow(
    pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
) -> None:
    configure_lakeflow(pytester, 'target="ci", profile="dev"')
    pytester.makepyfile("def test_one(lakeflow):\n    assert lakeflow.template('my_job').tasks\n")
    pytester.runpytest().assert_outcomes(passed=1)
    assert (["bundle", "deploy", "-t", "ci", "--auto-approve"], bundle_dir, "dev") in fake_cli.invocations
    assert fake_ws.profiles == ["dev"]


def test_bundle_root_is_relative_to_the_rootdir(
    pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli
) -> None:
    pytester.makeini("[pytest]\n")
    pytester.mkdir("bundle").joinpath("databricks.yml").write_text("bundle:\n  name: example\n")
    configure_lakeflow(pytester, 'bundle_root="bundle"')
    pytester.mkdir("tests").joinpath("test_one.py").write_text("def test_one(lakeflow):\n    lakeflow.templates\n")
    pytester.runpytest("tests").assert_outcomes(passed=1)
    assert (DEPLOY, pytester.path / "bundle", None) in fake_cli.invocations


def test_a_given_workspace_client_profile_goes_to_the_cli(
    pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
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


def test_lakeflows_for_the_same_bundle_share_one_setup(
    pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
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
    pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
) -> None:
    configure_lakeflow(pytester, "", STAGING_FIXTURE)
    test = "def test_{0}(lakeflow, staging):\n    lakeflow.template('my_job')\n    staging.template('my_job')\n"
    pytester.makepyfile(test.format("a") + "\n" + test.format("b"))
    pytester.runpytest().assert_outcomes(passed=2)
    assert [call for call in fake_cli.calls if call[:2] == ["bundle", "deploy"]] == [DEPLOY, DEPLOY_STAGING]


def test_a_failed_setup_is_not_repeated(
    pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
) -> None:
    fake_cli.errors["deploy"] = LakeflowBundleError("deploy broke")
    pytester.makepyfile(
        "def test_a(lakeflow):\n    lakeflow.template('my_job')\n\ndef test_b(lakeflow):\n    lakeflow.templates\n"
    )
    result = pytester.runpytest()
    result.assert_outcomes(failed=2)
    result.stdout.fnmatch_lines(
        ["*LakeflowBundleError: deploy broke*", "*failed earlier in this session: LakeflowBundleError: deploy broke*"]
    )
    assert fake_cli.calls.count(DEPLOY) == 1


def test_lakeflow_outside_a_test_fails_clearly(
    pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
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


XDIST_CONFTEST = """
import json
import sys

import pytest

from pytest_databricks_lakeflow import Lakeflow, bundle, session

sys.path.insert(0, {unit_tests!r})

from fakes import FakeWorkspaceClient, example_settings


def fake_cli(args, root, profile=None):
    with open({log!r}, "a") as f:
        f.write(" ".join(args) + "\\n")
    if args == ["--version"]:
        return "Databricks CLI v1.7.0"
    if args[:2] == ["bundle", "summary"]:
        return json.dumps({{"resources": {{"jobs": {{"my_job": {{"id": "1000"}}}}}}}})
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


def make_xdist_conftest(pytester: pytest.Pytester, extra: str = "") -> Path:
    log = pytester.path / "cli.log"
    pytester.makeconftest(XDIST_CONFTEST.format(unit_tests=str(UNIT_TESTS), log=str(log)) + extra)
    return log


def test_xdist_workers_share_one_setup_and_run_id(pytester: pytest.Pytester, bundle_dir: Path) -> None:
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
    assert calls.count("bundle summary -t test -o json") == 1
    assert len(run_ids.read_text().split()) == 6
    assert len(set(run_ids.read_text().split())) == 1


def test_xdist_workers_share_each_bundle_target(pytester: pytest.Pytester, bundle_dir: Path) -> None:
    pytest.importorskip("xdist")
    log = make_xdist_conftest(pytester, STAGING_FIXTURE)
    test = "def test_{i}(lakeflow, staging):\n    lakeflow.template('my_job')\n    staging.template('my_job')\n"
    pytester.makepyfile("\n".join(test.format(i=i) for i in range(6)))
    pytester.runpytest("-n", "2").assert_outcomes(passed=6)
    calls = log.read_text().splitlines()
    assert calls.count(" ".join(DEPLOY)) == 1
    assert calls.count(" ".join(DEPLOY_STAGING)) == 1


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


def test_xdist_workers_share_attribution(
    pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch, bundle_dir: Path
) -> None:
    pytest.importorskip("xdist")
    set_ci(monkeypatch, {})
    # Under an outer `-n`, the in-process controller inherits the outer worker's id and would log its own git calls.
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
