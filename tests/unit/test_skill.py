"""The offline skill installer and repository metadata."""

import runpy
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

from pytest_databricks_lakeflow.skill import _skill_directory, main

ROOT = Path(__file__).resolve().parents[2]
NAME = "pytest-databricks-lakeflow"
VERSION = tomllib.loads(ROOT.joinpath("pyproject.toml").read_text())["project"]["version"]
CANONICAL = ROOT / "skills" / NAME / "SKILL.md"
BUNDLED = _skill_directory().joinpath("SKILL.md")


@pytest.mark.parametrize(
    ("agent", "directories"),
    [
        (None, [".agents", ".claude"]),
        ("claude", [".claude"]),
        ("codex", [".agents"]),
        ("copilot", [".github"]),
        ("cursor", [".cursor"]),
        ("all", [".claude", ".agents", ".github", ".cursor"]),
    ],
)
@pytest.mark.parametrize("user", [False, True])
def test_install(tmp_path, monkeypatch, capsys, agent, directories, user):
    project = tmp_path / "project"
    home = tmp_path / "home"
    project.mkdir()
    home.mkdir()
    monkeypatch.chdir(project)
    monkeypatch.setattr(Path, "home", lambda: home)
    args = ["install"]
    if agent:
        args += ["--agent", agent]
    if user:
        args += ["--user"]
    main(args)
    base = home if user else project
    expected = {".copilot" if user and directory == ".github" else directory for directory in directories}
    assert {path.name for path in base.iterdir()} == expected
    output = capsys.readouterr().out
    for directory in expected:
        target = base / directory / "skills" / NAME
        assert target.joinpath("SKILL.md").read_bytes() == CANONICAL.read_bytes()
        assert str(target) in output
    assert "Reload your agent" in output
    assert not list((project if user else home).iterdir())


def test_refuse_and_force(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    target = tmp_path / ".claude" / "skills" / NAME
    target.mkdir(parents=True)
    target.joinpath("custom.txt").write_text("keep me")
    with pytest.raises(SystemExit) as error:
        main(["install"])
    assert error.value.code == 2
    assert target.joinpath("custom.txt").read_text() == "keep me"
    assert not tmp_path.joinpath(".agents").exists()
    main(["install", "--force"])
    assert not target.joinpath("custom.txt").exists()
    assert target.joinpath("SKILL.md").read_bytes() == CANONICAL.read_bytes()


def test_refuse_symlink(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    target = tmp_path / ".claude" / "skills" / NAME
    target.parent.mkdir(parents=True)
    target.symlink_to(outside, target_is_directory=True)
    with pytest.raises(SystemExit):
        main(["install", "--force"])
    assert outside.is_dir()
    assert target.is_symlink()


def test_show_and_path(capsys):
    main(["show"])
    assert capsys.readouterr().out == BUNDLED.read_text(encoding="utf-8")
    main(["path"])
    assert Path(capsys.readouterr().out.strip()).joinpath("SKILL.md").is_file()
    assert BUNDLED.read_bytes() == CANONICAL.read_bytes()


def test_module():
    result = subprocess.run(
        [sys.executable, "-m", "pytest_databricks_lakeflow.skill", "show"],
        check=True,
        capture_output=True,
    )
    assert result.stdout == CANONICAL.read_bytes()


def test_repository_metadata():
    subprocess.run([sys.executable, str(ROOT / "scripts" / "check_skill.py")], check=True)


@pytest.mark.parametrize(
    "content",
    [
        "no frontmatter",
        "---\nname: pytest-databricks-lakeflow\n",
        "---\nname: wrong-name\ndescription: Test jobs\n---",
        "---\nname: pytest-databricks-lakeflow\n---",
        "---\nname: pytest-databricks-lakeflow\ndescription: " + "x" * 1025 + "\n---",
        "---\nname: pytest-databricks-lakeflow\ndescription: null\n---",
        "---\nname: pytest-databricks-lakeflow\ndescription: 'unterminated\n---",
        "---\nname: pytest-databricks-lakeflow\ndescription: Test: jobs\n---",
        "---\nname: pytest-databricks-lakeflow\ndescription: Test jobs\nextra: unsupported\n---",
        "---\nname: pytest-databricks-lakeflow\nname: duplicate\ndescription: Test jobs\n---",
    ],
)
def test_invalid_frontmatter(tmp_path, content):
    skill = tmp_path / NAME / "SKILL.md"
    skill.parent.mkdir()
    skill.write_text(content)
    check = runpy.run_path(str(ROOT / "scripts" / "check_skill.py"))
    with pytest.raises(ValueError):
        check["frontmatter"](skill)


@pytest.mark.parametrize(
    ("name", "filename", "content"),
    [
        ("bad_name", "SKILL.md", "---\nname: bad_name\ndescription: Test jobs\n---"),
        ("x" * 65, "SKILL.md", "---\nname: " + "x" * 65 + "\ndescription: Test jobs\n---"),
    ],
)
def test_invalid_skill_name(tmp_path, name, filename, content):
    skill = tmp_path / name / filename
    skill.parent.mkdir()
    skill.write_text(content)
    check = runpy.run_path(str(ROOT / "scripts" / "check_skill.py"))
    with pytest.raises(ValueError):
        check["frontmatter"](skill)


@pytest.mark.parametrize(
    ("path", "old", "new"),
    [
        (f"skills/{NAME}/SKILL.md", None, None),
        ("SKILL.md", None, "root skill"),
        (".claude-plugin/plugin.json", f'"version": "{VERSION}"', '"version": "0.0.0"'),
        (".claude-plugin/marketplace.json", f'"version": "{VERSION}"', '"version": "0.0.0"'),
        (".claude-plugin/plugin.json", '"skills": ["./skills"]', '"skills": ["./missing"]'),
        (".claude-plugin/marketplace.json", '"source": "./"', '"source": "./missing"'),
        (".claude-plugin/plugin.json", None, "{broken"),
        (".claude-plugin/marketplace.json", None, "{broken"),
        ("llms.txt", f"main/skills/{NAME}/SKILL.md", "main/missing/SKILL.md"),
    ],
)
def test_metadata_drift(tmp_path, path, old, new):
    for directory in ["skills", ".claude-plugin"]:
        shutil.copytree(ROOT / directory, tmp_path / directory)
    for filename in ["pyproject.toml", "llms.txt"]:
        shutil.copyfile(ROOT / filename, tmp_path / filename)
    target = tmp_path / path
    if new is None:
        target.unlink()
    elif old is None:
        target.write_text(new)
    else:
        original = target.read_text()
        assert old in original
        target.write_text(original.replace(old, new))
    check = runpy.run_path(str(ROOT / "scripts" / "check_skill.py"))
    with pytest.raises(ValueError):
        check["validate"](tmp_path)
