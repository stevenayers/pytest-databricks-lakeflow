from __future__ import annotations

import json
import time
from datetime import timedelta
from pathlib import Path
from typing import Any, cast

import pytest
from databricks.sdk import WorkspaceClient

from pytest_databricks_lakeflow import helpers, session
from pytest_databricks_lakeflow.client import Lakeflow
from pytest_databricks_lakeflow.errors import LakeflowBundleError, LakeflowError
from pytest_databricks_lakeflow.options import Options

from .conftest import FakeCli
from .fakes import FakeWorkspaceClient, example_settings

REPO = Path(__file__).parents[2]


def options(**overrides: Any) -> Options:
    values: dict[str, Any] = {
        "bundle_root": None,
        "target": "test",
        "profile": None,
        "sync": "deploy",
        "timeout": timedelta(minutes=30),
        "catalog": None,
        "helpers_path": None,
        "helpers_compute": None,
        "helpers_environment_version": "4",
        "sweep_hours": 24.0,
        "disable_auto_optimization": True,
        "performance_optimized": False,
        "allow_undeclared_params": False,
    }
    return Options(**{**values, **overrides})


def as_client(ws: FakeWorkspaceClient) -> WorkspaceClient:
    return cast(WorkspaceClient, ws)


def test_shared_state_without_xdist_produces_directly() -> None:
    assert session.shared_state(None, lambda: {"run_id": "abc"}) == {"run_id": "abc"}


def test_shared_state_produces_once_for_all_workers(tmp_path: Path) -> None:
    produced: list[str] = []

    def produce() -> dict[str, Any]:
        produced.append("gw0")
        return {"run_id": "abc"}

    assert session.shared_state(tmp_path, produce) == {"run_id": "abc"}
    assert session.shared_state(tmp_path, produce) == {"run_id": "abc"}
    assert produced == ["gw0"]


def test_shared_state_shares_setup_errors(tmp_path: Path) -> None:
    def fail() -> dict[str, Any]:
        raise LakeflowBundleError("deploy failed")

    with pytest.raises(LakeflowBundleError, match="deploy failed"):
        session.shared_state(tmp_path, fail)
    with pytest.raises(LakeflowError, match="failed in another xdist worker: LakeflowBundleError: deploy failed"):
        session.shared_state(tmp_path, lambda: {"run_id": "never"})


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


def test_templates_from_summary() -> None:
    ws = FakeWorkspaceClient()
    job_id = ws.jobs.add(example_settings())
    summary = {"resources": {"jobs": {"my_job": {"id": str(job_id)}, "gone": {"id": "1"}, "new": {}}}}
    templates, errors = session.templates_from_summary(as_client(ws), summary)
    assert templates == {"my_job": example_settings().as_dict()}
    assert "stale" in errors["gone"]
    assert "not deployed" in errors["new"]


def test_setup_with_deploy(fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path) -> None:
    state = session.setup(as_client(fake_ws), options(), bundle_dir / "tests")
    assert fake_cli.calls == [
        ["--version"],
        ["bundle", "deploy", "-t", "test", "--auto-approve"],
        ["bundle", "summary", "-t", "test", "-o", "json"],
    ]
    folder = "/Users/someone@example.com/.pytest-databricks-lakeflow"
    assert state["bundle_root"] == str(bundle_dir)
    assert state["templates"] == {"my_job": example_settings().as_dict()}
    assert state["helpers"] == {"set_values": f"{folder}/set_values", "get_values": f"{folder}/get_values"}
    assert set(fake_ws.workspace.uploads) == {f"{folder}/set_values", f"{folder}/get_values"}
    assert all(upload["overwrite"] for upload in fake_ws.workspace.uploads.values())
    assert json.loads(json.dumps(state)) == state


def test_setup_with_none_uses_validate(fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path) -> None:
    fake_cli.validate = {"resources": {"jobs": {"other": {"name": "o", "tasks": [{"task_key": "t"}]}}}}
    state = session.setup(as_client(fake_ws), options(sync="none", helpers_path="/Shared/helpers/"), bundle_dir)
    assert [call[:2] for call in fake_cli.calls] == [["--version"], ["bundle", "validate"]]
    assert state["templates"] == {"other": {"name": "o", "tasks": [{"task_key": "t"}]}}
    assert state["helpers"]["get_values"] == "/Shared/helpers/get_values"


def test_setup_with_sync_syncs_first(fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path) -> None:
    session.setup(as_client(fake_ws), options(sync="sync"), bundle_dir)
    assert [call[:2] for call in fake_cli.calls] == [["--version"], ["bundle", "sync"], ["bundle", "validate"]]


def test_helper_sources_use_prefixed_widgets() -> None:
    assert f'dbutils.widgets.get("{helpers.VALUES_WIDGET}")' in helpers.SET_VALUES
    assert f'dbutils.widgets.get("{helpers.TASK_KEY_WIDGET}")' in helpers.GET_VALUES
    assert f'dbutils.widgets.get("{helpers.KEYS_WIDGET}")' in helpers.GET_VALUES
    for source in helpers.NOTEBOOKS.values():
        compile(source, "notebook", "exec")


def test_template_copies_are_independent() -> None:
    lakeflow = Lakeflow(
        ws=as_client(FakeWorkspaceClient()),
        options=options(),
        run_id="r",
        templates={"my_job": example_settings().as_dict()},
        template_errors={"broken": "Job 'broken' is not deployed."},
    )
    first = lakeflow.template("my_job")
    first.tasks = []
    assert first.tags is not None
    first.tags["x"] = "y"
    assert lakeflow.template("my_job") == example_settings()
    with pytest.raises(KeyError, match=r"No job 'nope' in the bundle. Available: \['my_job'\]"):
        lakeflow.template("nope")
    with pytest.raises(LakeflowBundleError, match="not deployed"):
        lakeflow.template("broken")


def test_lakeflow_fixture_loads_templates(
    pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
) -> None:
    pytester.makepyfile(
        """
        def test_one(lakeflow):
            assert sorted(lakeflow.templates) == ["my_job"]
            assert lakeflow.request.node.name == "test_one"

        def test_two(lakeflow):
            assert lakeflow.template("my_job").tasks[0].task_key == "extract"
        """
    )
    result = pytester.runpytest()
    result.assert_outcomes(passed=2)
    assert fake_cli.calls.count(["bundle", "deploy", "-t", "test", "--auto-approve"]) == 1


def test_no_setup_when_lakeflow_tests_are_deselected(
    pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
) -> None:
    pytester.makepyfile("def test_one(lakeflow):\n    pass\n\ndef test_two():\n    pass\n")
    pytester.runpytest("-m", "not lakeflow").assert_outcomes(passed=1, deselected=1)
    assert fake_cli.calls == []


XDIST_CONFTEST = """
import json
import time
import sys

sys.path.insert(0, {repo!r})

from tests.unit.fakes import FakeWorkspaceClient, example_settings
from pytest_databricks_lakeflow import bundle, session


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


def make_xdist_conftest(pytester: pytest.Pytester) -> Path:
    log = pytester.path / "cli.log"
    pytester.makeconftest(XDIST_CONFTEST.format(repo=str(REPO), log=str(log)))
    return log


def test_xdist_workers_share_one_setup(pytester: pytest.Pytester, bundle_dir: Path) -> None:
    pytest.importorskip("xdist")
    log = make_xdist_conftest(pytester)
    pytester.makepyfile(
        "\n".join(
            f"def test_{i}(lakeflow):\n    assert lakeflow.template('my_job').tasks[0].task_key == 'extract'\n"
            for i in range(6)
        )
    )
    result = pytester.runpytest("-n", "2")
    result.assert_outcomes(passed=6)
    calls = log.read_text().splitlines()
    assert calls.count("bundle deploy -t test --auto-approve") == 1
    assert calls.count("bundle summary -t test -o json") == 1


def test_setup_rejects_a_configured_root_without_a_bundle(fake_ws: FakeWorkspaceClient, tmp_path: Path) -> None:
    missing = tmp_path / "examples" / "bundle"
    with pytest.raises(LakeflowBundleError, match=r"lakeflow_bundle_root .*examples/bundle has no databricks.yml"):
        session.setup(as_client(fake_ws), options(bundle_root=missing), tmp_path)
