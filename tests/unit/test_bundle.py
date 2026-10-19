from __future__ import annotations

import os
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


class TestFindRoot:
    """Finding the bundle root from a directory inside it."""

    def test_find_root_walks_up(self, tmp_path: Path) -> None:
        (tmp_path / "databricks.yml").write_text("bundle: {name: x}\n")
        nested = tmp_path / "tests" / "integration"
        nested.mkdir(parents=True)
        assert bundle.find_root(nested) == tmp_path

    def test_find_root_fails_clearly(self, tmp_path: Path) -> None:
        with pytest.raises(LakeflowBundleError, match="No databricks.yml .* Pass bundle_root to Lakeflow"):
            bundle.find_root(tmp_path)


class TestRunCli:
    """Running the Databricks CLI and turning its failures into `LakeflowBundleError`."""

    def test_run_cli_returns_stdout(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
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

    def test_run_cli_reports_stderr(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        def fail(cmd: list[str], **kwargs: Any) -> None:
            raise subprocess.CalledProcessError(1, cmd, output="", stderr="Error: no such target\n")

        monkeypatch.setattr(subprocess, "run", fail)
        with pytest.raises(LakeflowBundleError, match="exit code 1:\nError: no such target$") as excinfo:
            bundle.run_cli(["bundle", "deploy"], tmp_path, "dev")
        assert "`databricks bundle deploy --profile dev`" in str(excinfo.value)

    def test_run_cli_reports_a_missing_directory(self, tmp_path: Path) -> None:
        with pytest.raises(LakeflowBundleError, match="Bundle root .*nope does not exist"):
            bundle.run_cli(["bundle", "summary"], tmp_path / "nope")

    def test_run_cli_reports_missing_cli(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        def missing(cmd: list[str], **kwargs: Any) -> None:
            raise FileNotFoundError(cmd[0])

        monkeypatch.setattr(subprocess, "run", missing)
        with pytest.raises(LakeflowBundleError, match="not on PATH"):
            bundle.run_cli(["--version"], tmp_path)


class TestCommands:
    """The `bundle` subcommands the plugin runs, and how their arguments and output are handled."""

    @pytest.mark.parametrize(
        ("variables", "as_vars"),
        [(None, []), ({"b": "2", "a": "1"}, ["--var", "a=1", "--var", "b=2"])],
        ids=["no-variables", "sorted-variables"],
    )
    def test_wrappers_build_commands(
        self, runner: FakeRunner, tmp_path: Path, variables: dict[str, str] | None, as_vars: list[str]
    ) -> None:
        runner.outputs = ['{"a": 1}', '{"b": 2}']
        assert bundle.validate(tmp_path, "test", "p", variables) == {"a": 1}
        assert bundle.summary(tmp_path, "test", None, variables) == {"b": 2}
        bundle.deploy(tmp_path, "test", None, variables)
        bundle.sync(tmp_path, "test", None, variables)
        bundle.destroy(tmp_path, "test", None, variables)
        assert [call[0] for call in runner.calls] == [
            ["bundle", "validate", "-t", "test", "-o", "json", *as_vars],
            ["bundle", "summary", "-t", "test", "-o", "json", *as_vars],
            ["bundle", "deploy", "-t", "test", "--auto-approve", *as_vars],
            ["bundle", "sync", "-t", "test", *as_vars],
            ["bundle", "destroy", "-t", "test", "--auto-approve", *as_vars],
        ]
        assert [call[2] for call in runner.calls] == ["p", None, None, None, None]

    def test_deploy_appends_flags(self, runner: FakeRunner, tmp_path: Path) -> None:
        bundle.deploy(tmp_path, "test", None, flags=("--fail-on-active-runs", "--force-lock"))
        assert runner.calls[0][0] == [
            "bundle",
            "deploy",
            "-t",
            "test",
            "--auto-approve",
            "--fail-on-active-runs",
            "--force-lock",
        ]

    def test_wrappers_reject_non_json(self, runner: FakeRunner, tmp_path: Path) -> None:
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
    def test_check_cli_version(self, runner: FakeRunner, tmp_path: Path, output: str, ok: bool) -> None:
        runner.outputs = [output]
        if ok:
            bundle.check_cli_version(tmp_path)
        else:
            with pytest.raises(LakeflowBundleError, match="needs Databricks CLI v1.7.0 or later"):
                bundle.check_cli_version(tmp_path)


class TestJobIds:
    """Reading deployed job ids from `bundle summary`."""

    def test_job_ids_from_summary(self) -> None:
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


class TestUnversionedWheelArtifacts:
    """Finding the wheel artifacts that serverless may run a cached build of, from `bundle summary` or `validate`."""

    WHEEL = {"type": "whl", "build": "uv build --wheel", "path": "/b", "files": [{"source": "/b/dist/*.whl"}]}

    @pytest.mark.parametrize(
        ("config", "expected"),
        [
            pytest.param({}, [], id="no_artifacts"),
            pytest.param({"artifacts": None}, [], id="null_artifacts"),
            pytest.param({"artifacts": {"pkg": WHEEL}}, ["pkg"], id="whl"),
            pytest.param({"artifacts": {"pkg": {**WHEEL, "dynamic_version": True}}}, [], id="dynamic_version"),
            pytest.param({"artifacts": {"pkg": {**WHEEL, "dynamic_version": False}}}, ["pkg"], id="dynamic_false"),
            pytest.param({"artifacts": {"pkg": {"build": "make", "path": "/b"}}}, ["pkg"], id="untyped_build"),
            pytest.param({"artifacts": {"lib": {"type": "jar", "build": "sbt package"}}}, [], id="jar"),
            pytest.param({"artifacts": {"b": WHEEL, "a": WHEEL}}, ["a", "b"], id="sorted"),
        ],
    )
    def test_unversioned_wheel_artifacts(self, config: dict[str, Any], expected: list[str]) -> None:
        assert bundle.unversioned_wheel_artifacts(config) == expected


class TestTemplatesFromValidate:
    """Building templates from `bundle validate` for the sync and none strategies, and their local libraries."""

    def test_with_none_points_at_uploaded_wheels(self, tmp_path: Path) -> None:
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

    def test_with_none_reports_missing_uploads(self, tmp_path: Path) -> None:
        (tmp_path / "dist").mkdir()
        (tmp_path / "dist" / "pkg-0.1-py3-none-any.whl").write_text("")
        templates, errors = bundle.templates_from_validate(validate_output(), tmp_path, "none", lambda path: False)
        assert "wheels" not in templates
        assert "pkg-0.1-py3-none-any.whl not found. Run `databricks bundle deploy` first." in errors["wheels"]

    def test_with_none_reports_unbuilt_wheels(self, tmp_path: Path) -> None:
        templates, errors = bundle.templates_from_validate(validate_output(), tmp_path, "none", lambda path: True)
        assert "no file under" in errors["wheels"]
        assert "notebooks" in templates

    def test_with_sync_rejects_local_libraries(self, tmp_path: Path) -> None:
        templates, errors = bundle.templates_from_validate(validate_output(), tmp_path, "sync", lambda path: True)
        assert sorted(templates) == ["notebooks"]
        assert "uses local libraries (dist/*.whl, ./dist/*.whl)" in errors["wheels"]
        assert 'Use Lakeflow(sync="deploy")' in errors["wheels"]


class TestLocalWheels:
    """Local wheel patterns with extras, and local wheels built after the copies a deploy uploaded."""

    WHEEL = "pkg-0.1-py3-none-any.whl"
    UPLOADED = "/Workspace/b/artifacts/.internal/pkg-0.1-py3-none-any.whl"

    @pytest.mark.parametrize(
        ("dependency", "expected"),
        [
            ("./dist/*.whl[test]", ("./dist/*.whl", "[test]")),
            ("../dist/pkg-0.1-py3-none-any.whl[test,dev]", ("../dist/pkg-0.1-py3-none-any.whl", "[test,dev]")),
            ("/Workspace/b/pkg.whl[test]", ("/Workspace/b/pkg.whl", "[test]")),
            ("./dist/*.whl", ("./dist/*.whl", "")),
            ("rich[all]>=13", ("rich[all]>=13", "")),
            (
                "pkg[extra]@https://host/pkg-1.0-py3-none-any.whl",
                ("pkg[extra]@https://host/pkg-1.0-py3-none-any.whl", ""),
            ),
        ],
        ids=["glob", "file with two extras", "remote path", "no extras", "pypi with extras", "direct reference"],
    )
    def test_split_extras(self, dependency: str, expected: tuple[str, str]) -> None:
        assert bundle.split_extras(dependency) == expected

    def test_uploaded_keeps_extras_after_globbing(self, tmp_path: Path) -> None:
        (tmp_path / "dist").mkdir()
        (tmp_path / "dist" / self.WHEEL).write_text("")
        checked: list[str] = []

        def exists(path: str) -> bool:
            checked.append(path)
            return True

        uploaded = bundle.uploaded("./dist/*.whl[test]", tmp_path, "/Workspace/b/artifacts", exists)
        assert uploaded == [f"{self.UPLOADED}[test]"]
        assert checked == [self.UPLOADED]

    @pytest.mark.parametrize(
        ("uploaded_after_build", "stale"),
        [(-120, ["dist/pkg-0.1-py3-none-any.whl"]), (-30, []), (5, []), (None, [])],
        ids=["built after upload", "within clock skew", "uploaded after build", "not uploaded"],
    )
    def test_templates_from_validate_reports_stale_uploads(
        self, tmp_path: Path, uploaded_after_build: int | None, stale: list[str]
    ) -> None:
        (tmp_path / "dist").mkdir()
        wheel = tmp_path / "dist" / self.WHEEL
        wheel.write_text("")
        built = 1_700_000_000
        os.utime(wheel, (built, built))
        checked: list[str] = []

        def modified_at(path: str) -> int | None:
            checked.append(path)
            if uploaded_after_build is None:
                return None
            return (built + uploaded_after_build) * 1000

        assert bundle.stale_uploads(validate_output(), tmp_path, modified_at) == stale
        assert checked == [self.UPLOADED], "each uploaded copy is looked up once"
