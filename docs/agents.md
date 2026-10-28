# AI coding agents

The repo ships an [Agent Skill](https://github.com/stevenayers/pytest-databricks-lakeflow/tree/main/skills/pytest-databricks-lakeflow) for writing and debugging Lakeflow job and task tests. Installing the Python package does not activate it automatically: agents do not discover skills in `site-packages`.

## Install

With the standard skills CLI:

```bash
npx skills add stevenayers/pytest-databricks-lakeflow
```

Or, with the Python package installed, use the offline installer:

```bash
lakeflow-skill install
```

This writes to `.agents/skills/` and `.claude/skills/` in the current project. Reload your agent after installing. Claude Code, Codex, Copilot and Cursor can then load the skill when you ask them to write or debug a Lakeflow job test.

## Claude Code marketplace

Run these commands inside Claude Code:

```text
/plugin marketplace add stevenayers/pytest-databricks-lakeflow
/plugin install pytest-databricks-lakeflow
```

If you have more than one marketplace with this plugin name, use `/plugin install pytest-databricks-lakeflow@pytest-databricks-lakeflow`.

This repository is the marketplace; there is no separate publish command. The `skills/` layout lets the skills CLI discover it directly. Directory listings and crawler indexing depend on the external services, not on pip installation.

## Command reference

```text
lakeflow-skill install [--agent claude|codex|copilot|cursor|all] [--user] [--force]
lakeflow-skill show
lakeflow-skill path
```

| Agent | Project directory | User directory (`--user`) |
| --- | --- | --- |
| Claude Code | `.claude/skills/` | `~/.claude/skills/` |
| Codex | `.agents/skills/` | `~/.agents/skills/` |
| GitHub Copilot | `.github/skills/` | `~/.copilot/skills/` |
| Cursor | `.cursor/skills/` | `~/.cursor/skills/` |

Without `--agent`, the installer uses both Claude Code and Codex directories. `--agent all` writes all four. `--user` uses your home directory instead of the current project. Existing skill directories are left untouched unless you pass `--force`, which replaces them. Each destination is printed. Reload your agent to pick up the skill.

`show` prints the bundled instructions to stdout. `path` prints their directory. Every command also works as `python -m pytest_databricks_lakeflow.skill`, for example:

```bash
python -m pytest_databricks_lakeflow.skill install --agent all
```

For agents that fetch URLs, [llms.txt](https://stevenayers.github.io/pytest-databricks-lakeflow/llms.txt) indexes the docs and [SKILL.md](https://stevenayers.github.io/pytest-databricks-lakeflow/SKILL.md) contains the instructions.
