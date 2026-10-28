"""Install the bundled Agent Skill without a network connection."""

from __future__ import annotations

import argparse
import shutil
from importlib.resources import files
from importlib.resources.abc import Traversable
from pathlib import Path

_NAME = "pytest-databricks-lakeflow"
_AGENTS = {
    "claude": ".claude",
    "codex": ".agents",
    "copilot": ".github",
    "cursor": ".cursor",
}


def _skill_directory() -> Traversable:
    source = files("pytest_databricks_lakeflow").joinpath("skills", _NAME)
    if source.joinpath("SKILL.md").is_file():
        return source
    # Hatch's editable install resolves package resources under src/, not the wheel's force-include directory.
    return Path(__file__).resolve().parents[2] / "skills" / _NAME


def main(argv: list[str] | None = None) -> None:
    """Show, locate, or install the bundled skill.

    Args:
        argv: Command-line arguments, or None to read them from sys.argv.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    install = commands.add_parser("install", help="Copy the skill into agent discovery directories")
    install.add_argument("--agent", choices=[*_AGENTS, "all"], help="Default: Claude Code and Codex directories")
    install.add_argument("--user", action="store_true", help="Install under your home instead of the current directory")
    install.add_argument("--force", action="store_true", help="Replace an existing skill directory")
    commands.add_parser("show", help="Print SKILL.md")
    commands.add_parser("path", help="Print the bundled skill directory")
    args = parser.parse_args(argv)
    source = _skill_directory()
    if args.command == "show":
        print(source.joinpath("SKILL.md").read_text(encoding="utf-8"), end="")
        return
    if args.command == "path":
        print(source)
        return

    agents = list(_AGENTS) if args.agent == "all" else [args.agent] if args.agent else ["codex", "claude"]
    base = Path.home() if args.user else Path.cwd()
    targets = [
        base / (".copilot" if args.user and agent == "copilot" else _AGENTS[agent]) / "skills" / _NAME
        for agent in agents
    ]
    # Check every destination before writing, so a conflict does not leave a partial install.
    for target in targets:
        if target.is_symlink() or (target.exists() and not target.is_dir()):
            parser.error(f"Not a regular skill directory: {target}")
        if target.exists() and not args.force:
            parser.error(f"Skill directory already exists: {target}; use --force to replace it")
    content = source.joinpath("SKILL.md").read_bytes()
    for target in targets:
        if target.exists():
            shutil.rmtree(target)
        target.mkdir(parents=True)
        target.joinpath("SKILL.md").write_bytes(content)
        print(target)
    print("Reload your agent to discover the installed skill.")


if __name__ == "__main__":
    main()
