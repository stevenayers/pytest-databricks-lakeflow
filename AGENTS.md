# Working in this repository

pytest-databricks-lakeflow is a pytest plugin for testing Databricks Asset Bundle
Lakeflow jobs and individual tasks with throwaway workspace resources.

Use Python 3.11 or later and uv. Local checks:

- `uv run pytest tests/unit`
- `uv run ruff check . && uv run ruff format --check .`
- `uv run ty check`
- `uv run python scripts/check_skill.py`
- `uv run --group docs mkdocs build --strict`

Integration tests need a configured Databricks workspace and credentials.
The canonical Agent Skill lives in `skills/pytest-databricks-lakeflow/SKILL.md`;
hatch copies it into the wheel at build time. Do not maintain a second copy.
Plugin metadata versions are updated automatically by release-please.
Use conventional commits; do not edit the generated `CHANGELOG.md`.
See [CONTRIBUTING.md](CONTRIBUTING.md) for setup, tests, and releases.
