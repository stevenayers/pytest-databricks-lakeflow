## Summary

<!-- What changes, and why. Link to the docs or spec section if there is one. -->

## Linked issue

<!-- "Closes #123", or "None" for small fixes. -->

## Type of change

- [ ] Bug fix (`fix:`)
- [ ] New feature (`feat:`)
- [ ] Breaking change (`feat!:` or `fix!:`, with a `BREAKING CHANGE:` footer)
- [ ] Documentation (`docs:`)
- [ ] CI, build or maintenance (`ci:`, `build:`, `chore:`)

## Checklist

- [ ] Tests added or updated, and `uv run pytest -m "not lakeflow"` passes
- [ ] Docs updated (`README.md`, `docs/`) where behaviour changed, and `uv run --group docs mkdocs build --strict` passes
- [ ] `uv run ruff check`, `uv run ruff format --check` and `uv run ty check` pass
- [ ] The PR title is a conventional commit message, because it becomes the squash commit on `main`
