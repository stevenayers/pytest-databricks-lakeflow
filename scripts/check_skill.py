"""Validate the canonical Agent Skill and Claude Code marketplace using only the stdlib."""

from __future__ import annotations

import json
import re
import tomllib
from pathlib import Path
from urllib.parse import unquote, urlsplit


def frontmatter(path: Path) -> dict[str, str]:
    """Read the skill's deliberately limited, single-line YAML string fields.

    Args:
        path: SKILL.md containing name and description scalars.

    Returns:
        The validated name and description.

    Raises:
        ValueError: If the frontmatter is missing, malformed, or unsupported.
    """
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines or lines[0] != "---" or "---" not in lines[1:]:
        raise ValueError(f"{path}: missing frontmatter delimiters")
    fields: dict[str, str] = {}
    for line in lines[1 : lines.index("---", 1)]:
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        match = re.fullmatch(r"(name|description):[ \t]+(.+)", line)
        if not match or match[1] in fields:
            raise ValueError(f"{path}: use unique name/description fields with single-line string values")
        key, value = match.groups()
        value = value.strip()
        if not value:
            raise ValueError(f"{path}: {key} must be a nonempty string")
        if value.startswith('"'):
            value = json.loads(value)
        elif value.startswith("'"):
            if not re.fullmatch(r"'(?:[^']|'')*'", value):
                raise ValueError(f"{path}: invalid quoted value")
            value = value[1:-1].replace("''", "'")
        elif (
            value[0] in "|>!&*{}[],#%@`"
            or ": " in value
            or " #" in value
            or value.lower() in {"null", "true", "false", "~"}
            or re.fullmatch(r"[-+]?\d+(\.\d+)?", value)
        ):
            raise ValueError(f"{path}: use plain or quoted single-line strings")
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{path}: {key} must be a nonempty string")
        fields[key] = value
    name = fields.get("name", "")
    if name != path.parent.name or len(name) > 64 or not re.fullmatch(r"[a-z0-9]+(-[a-z0-9]+)*", name):
        raise ValueError(f"{path}: name must match the directory and be a lowercase slug of at most 64 characters")
    if not fields.get("description") or len(fields["description"]) > 1024:
        raise ValueError(f"{path}: description must contain 1–1024 characters")
    return fields


def _directory(root: Path, value: str) -> Path:
    path = (root / value).resolve()
    if not value.startswith("./") or not path.is_relative_to(root.resolve()) or not path.is_dir():
        raise ValueError(f"Missing or nonlocal directory: {value}")
    return path


def validate(root: Path) -> None:
    """Check skill layout, metadata versions, local paths, and llms.txt links.

    Args:
        root: Repository root.

    Raises:
        ValueError: If metadata or links violate the repository's skill invariants.
        OSError: If a required file cannot be read.
    """
    if root.joinpath("SKILL.md").exists():
        raise ValueError("Move the root SKILL.md into skills/<name>/SKILL.md")
    skills = sorted(root.glob("skills/*/SKILL.md"))
    if not skills:
        raise ValueError("No skills/*/SKILL.md found")
    for skill in skills:
        frontmatter(skill)
    version = tomllib.loads(root.joinpath("pyproject.toml").read_text(encoding="utf-8"))["project"]["version"]
    marketplace = json.loads(root.joinpath(".claude-plugin/marketplace.json").read_text(encoding="utf-8"))
    if not marketplace.get("name") or not marketplace.get("owner", {}).get("name") or not marketplace.get("plugins"):
        raise ValueError("Marketplace requires name, owner.name, and plugins")
    for entry in marketplace["plugins"]:
        plugin_root = _directory(root, entry["source"])
        plugin = json.loads(plugin_root.joinpath(".claude-plugin/plugin.json").read_text(encoding="utf-8"))
        if not entry.get("name") or entry["name"] != plugin.get("name"):
            raise ValueError("Marketplace entry and plugin names must match")
        if entry.get("version") != version or plugin.get("version") != version:
            raise ValueError("Marketplace and plugin versions must equal project.version")
        skill_paths = plugin.get("skills", "./skills")
        if isinstance(skill_paths, str):
            skill_paths = [skill_paths]
        if not skill_paths:
            raise ValueError("Plugin must reference at least one skill directory")
        for skill_path in skill_paths:
            skill_root = _directory(plugin_root, skill_path)
            plugin_skills = list(skill_root.glob("*/SKILL.md"))
            if not plugin_skills:
                raise ValueError(f"No skills found under {skill_root}")
            for skill in plugin_skills:
                frontmatter(skill)
    links = re.findall(r"\[[^\]]*\]\(([^)]+)\)", root.joinpath("llms.txt").read_text(encoding="utf-8"))
    for link in links:
        path = unquote(urlsplit(link).path)
        if not path.endswith("SKILL.md"):
            continue
        if "/blob/main/" in path:
            path = path.split("/blob/main/", 1)[1]
        elif urlsplit(link).scheme:
            # The docs workflow publishes the canonical skill at the site root.
            if link == "https://stevenayers.github.io/pytest-databricks-lakeflow/SKILL.md":
                path = "skills/pytest-databricks-lakeflow/SKILL.md"
            else:
                raise ValueError(f"Unrecognized SKILL.md link: {link}")
        if not root.joinpath(path).is_file():
            raise ValueError(f"Broken SKILL.md link: {link}")


if __name__ == "__main__":
    try:
        validate(Path(__file__).resolve().parents[1])
    except (ValueError, OSError, KeyError, TypeError, AttributeError) as error:
        raise SystemExit(f"Skill validation failed: {error}") from error
    print("Skill and marketplace metadata are valid.")
