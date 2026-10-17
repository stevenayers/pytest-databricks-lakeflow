from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

import pytest

from pytest_databricks_lakeflow import bundle
from pytest_databricks_lakeflow.errors import LakeflowBundleError


class FakeRunner:
    def __init__(self, *outputs: str) -> None:
        self.outputs = list(outputs)
        self.calls: list[tuple[list[str], Path, str | None]] = []

    def __call__(self, args: list[str], root: Path, profile: str | None = None) -> str:
        self.calls.append((args, root, profile))
        return self.outputs.pop(0) if self.outputs else ""


@pytest.fixture
def runner(monkeypatch: pytest.MonkeyPatch) -> FakeRunner:
    fake = FakeRunner()
    monkeypatch.setattr(bundle, "run_cli", fake)
    return fake


def test_find_root_walks_up(tmp_path: Path) -> None:
    (tmp_path / "databricks.yml").write_text("bundle: {name: x}\n")
    nested = tmp_path / "tests" / "integration"
    nested.mkdir(parents=True)
    assert bundle.find_root(nested) == tmp_path


def test_find_root_fails_clearly(tmp_path: Path) -> None:
    with pytest.raises(LakeflowBundleError, match="No databricks.yml"):
        bundle.find_root(tmp_path)


def test_run_cli_reports_stderr(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def fail(cmd: list[str], **kwargs: Any) -> None:
        raise subprocess.CalledProcessError(1, cmd, output="", stderr="Error: no such target\n")

    monkeypatch.setattr(subprocess, "run", fail)
    with pytest.raises(LakeflowBundleError, match="exit code 1:\nError: no such target$") as excinfo:
        bundle.run_cli(["bundle", "deploy"], tmp_path, "dev")
    assert "`databricks bundle deploy --profile dev`" in str(excinfo.value)


def test_run_cli_reports_a_missing_directory(tmp_path: Path) -> None:
    with pytest.raises(LakeflowBundleError, match="Bundle root .*nope does not exist"):
        bundle.run_cli(["bundle", "summary"], tmp_path / "nope")


def test_run_cli_reports_missing_cli(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def missing(cmd: list[str], **kwargs: Any) -> None:
        raise FileNotFoundError(cmd[0])

    monkeypatch.setattr(subprocess, "run", missing)
    with pytest.raises(LakeflowBundleError, match="not on PATH"):
        bundle.run_cli(["--version"], tmp_path)


def test_run_cli_returns_stdout(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    seen: dict[str, Any] = {}

    def ok(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        seen.update(cmd=cmd, **kwargs)
        return subprocess.CompletedProcess(cmd, 0, stdout="{}", stderr="")

    monkeypatch.setattr(subprocess, "run", ok)
    assert bundle.run_cli(["bundle", "summary"], tmp_path) == "{}"
    assert seen == {
        "cmd": ["databricks", "bundle", "summary"],
        "cwd": tmp_path,
        "check": True,
        "capture_output": True,
        "text": True,
    }


def test_wrappers_build_commands(runner: FakeRunner, tmp_path: Path) -> None:
    runner.outputs = ['{"a": 1}', '{"b": 2}']
    assert bundle.validate(tmp_path, "test", "p") == {"a": 1}
    assert bundle.summary(tmp_path, "test", None) == {"b": 2}
    bundle.deploy(tmp_path, "test", None)
    bundle.sync(tmp_path, "test", None)
    bundle.destroy(tmp_path, "test", None)
    assert [call[0] for call in runner.calls] == [
        ["bundle", "validate", "-t", "test", "-o", "json"],
        ["bundle", "summary", "-t", "test", "-o", "json"],
        ["bundle", "deploy", "-t", "test", "--auto-approve"],
        ["bundle", "sync", "-t", "test"],
        ["bundle", "destroy", "-t", "test", "--auto-approve"],
    ]
    assert runner.calls[0][2] == "p"


def test_wrappers_reject_non_json(runner: FakeRunner, tmp_path: Path) -> None:
    runner.outputs = ["Warning: something\n"]
    with pytest.raises(LakeflowBundleError, match="did not print JSON"):
        bundle.summary(tmp_path, "test", None)


@pytest.mark.parametrize(
    ("output", "ok"),
    [
        ("Databricks CLI v1.7.0\n", True),
        ("Databricks CLI v2.0.1\n", True),
        ("Databricks CLI v0.0.0-dev\n", True),
        ("Databricks CLI v1.6.9\n", False),
        ("Databricks CLI v0.295.0\n", False),
    ],
)
def test_check_cli_version(runner: FakeRunner, tmp_path: Path, output: str, ok: bool) -> None:
    runner.outputs = [output]
    if ok:
        bundle.check_cli_version(tmp_path)
    else:
        with pytest.raises(LakeflowBundleError, match="needs Databricks CLI v1.7.0 or later"):
            bundle.check_cli_version(tmp_path)


def test_job_ids_from_summary() -> None:
    summary = {
        "resources": {
            "jobs": {
                "deployed": {"id": "244227227330072", "url": "https://host/jobs/244227227330072", "name": "x"},
                "removed": {"id": "1", "url": "https://host/jobs/1", "modified_status": "deleted"},
                "new": {"name": "y", "modified_status": "created"},
            }
        }
    }
    ids, errors = bundle.job_ids(summary)
    assert ids == {"deployed": 244227227330072}
    assert list(errors) == ["new"]
    assert "not deployed" in errors["new"]
    assert bundle.job_ids({"resources": {}}) == ({}, {})


def validate_output() -> dict[str, Any]:
    notebook = {"task_key": "nb", "notebook_task": {"notebook_path": "/Workspace/b/files/src/nb"}}
    wheel = {
        "task_key": "whl",
        "environment_key": "default",
        "python_wheel_task": {"package_name": "pkg", "entry_point": "main"},
    }
    classic = {
        "task_key": "classic",
        "job_cluster_key": "c",
        "libraries": [{"whl": "dist/*.whl"}, {"pypi": {"package": "requests"}}, {"whl": "/Volumes/a/b/c.whl"}],
        "python_wheel_task": {"package_name": "pkg", "entry_point": "main"},
    }
    spec = {"environment_version": "4", "dependencies": ["./dist/*.whl", "rich>=13"]}
    environments = [{"environment_key": "default", "spec": spec}]
    run_job = {"task_key": "t", "run_job_task": {"job_id": "${resources.jobs.x.id}"}}
    return {
        "workspace": {"artifact_path": "/Workspace/b/artifacts"},
        "resources": {
            "jobs": {
                "notebooks": {"name": "n", "tasks": [notebook], "permissions": [{"level": "CAN_VIEW"}]},
                "wheels": {"name": "w", "tasks": [wheel, classic], "environments": environments},
                "refs": {"name": "r", "tasks": [run_job]},
            }
        },
    }


def test_templates_from_validate_with_none_points_at_uploaded_wheels(tmp_path: Path) -> None:
    (tmp_path / "dist").mkdir()
    (tmp_path / "dist" / "pkg-0.1-py3-none-any.whl").write_text("")
    checked: list[str] = []

    def exists(path: str) -> bool:
        checked.append(path)
        return True

    templates, errors = bundle.templates_from_validate(validate_output(), tmp_path, "none", exists)
    uploaded = "/Workspace/b/artifacts/.internal/pkg-0.1-py3-none-any.whl"
    assert sorted(templates) == ["notebooks", "wheels"]
    assert "permissions" not in templates["notebooks"]
    wheels = templates["wheels"]
    assert wheels["environments"][0]["spec"]["dependencies"] == [uploaded, "rich>=13"]
    assert wheels["tasks"][1]["libraries"] == [
        {"whl": uploaded},
        {"pypi": {"package": "requests"}},
        {"whl": "/Volumes/a/b/c.whl"},
    ]
    assert checked == [uploaded, uploaded]
    assert list(errors) == ["refs"]
    assert "${resources.jobs.x.id}" in errors["refs"]


def test_templates_from_validate_reports_missing_uploads(tmp_path: Path) -> None:
    (tmp_path / "dist").mkdir()
    (tmp_path / "dist" / "pkg-0.1-py3-none-any.whl").write_text("")
    templates, errors = bundle.templates_from_validate(validate_output(), tmp_path, "none", lambda path: False)
    assert "wheels" not in templates
    assert "pkg-0.1-py3-none-any.whl not found. Run `databricks bundle deploy` first." in errors["wheels"]


def test_templates_from_validate_reports_unbuilt_wheels(tmp_path: Path) -> None:
    templates, errors = bundle.templates_from_validate(validate_output(), tmp_path, "none", lambda path: True)
    assert "no file under" in errors["wheels"]
    assert "notebooks" in templates


def test_templates_from_validate_with_sync_rejects_local_libraries(tmp_path: Path) -> None:
    templates, errors = bundle.templates_from_validate(validate_output(), tmp_path, "sync", lambda path: True)
    assert sorted(templates) == ["notebooks"]
    assert "uses local libraries (dist/*.whl, ./dist/*.whl)" in errors["wheels"]
    assert "Use lakeflow_sync = deploy" in errors["wheels"]
