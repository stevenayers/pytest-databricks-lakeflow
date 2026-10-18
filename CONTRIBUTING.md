# Contributing

Thanks for helping with pytest-databricks-lakeflow. This page covers the development setup, the two test suites, the commit and pull request conventions, and how releases happen.

## Development setup

You need Python 3.11 or later, [uv](https://docs.astral.sh/uv/), and, for the integration tests, the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install) v1.7.0 or later.

```bash
git clone https://github.com/stevenayers/pytest-databricks-lakeflow.git
cd pytest-databricks-lakeflow
uv sync --all-groups
```

This creates `.venv` with the plugin installed in editable mode plus the `dev`, `docs` and `audit` dependency groups. Run every tool through `uv run`.

## Checks

```bash
uv run ruff check                           # lint
uv run ruff format --check                  # formatting; `uv run ruff format` fixes it
uv run ty check                             # types
uv run pytest -m "not lakeflow"             # unit tests, no workspace needed
uv run coverage run -m pytest -m "not lakeflow" && uv run coverage report   # with coverage
uv run --group docs mkdocs build --strict   # docs
```

The unit tests use pytest's `pytester` and an in-memory fake of the parts of `WorkspaceClient` the plugin uses, in `tests/unit/fakes.py`. They never touch the network. CI runs them on every supported Python version and once more on the oldest supported versions of the dependencies.

## Integration tests

`tests/integration` holds the acceptance tests. They run against [`examples/bundle`](examples/bundle) in a real workspace, which needs serverless jobs, a catalog you can create schemas in, and credentials that both the CLI and the SDK can use:

```bash
LAKEFLOW_INTEGRATION=1 DATABRICKS_CONFIG_PROFILE=<profile> LAKEFLOW_CATALOG=<catalog> \
    uv run pytest tests/integration -n 4 --lakeflow-sweep
```

The first test deploys the example bundle to its `test` target, which builds the example wheel with `uv build --wheel`. Pass `--lakeflow-destroy` to remove the deployment at the end, and `--lakeflow-keep` to inspect the jobs afterwards.

In CI, the `Integration tests` job runs on pushes to `main` once these settings exist: the repository variables `DATABRICKS_HOST` and `LAKEFLOW_CATALOG`, and, in a GitHub environment named `integration`, the secrets `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET` for a service principal that can create jobs, use serverless compute and create schemas in that catalog. Until `DATABRICKS_HOST` is set, the job is skipped.

## Making a change

- Start with a test. Add or change a unit test, watch it fail, then change the code.
- Follow the conventions in the code: `from __future__ import annotations`, frozen dataclasses for results, docstrings on the public API only, UK English in prose, and no em-dashes anywhere.
- Update `README.md` and `docs/` when behaviour changes. Every snippet must use the real public API; the README is included in the docs site verbatim.
- Run the checks above before opening a pull request.

## Commit messages

Commits on `main` follow [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/). release-please reads them to choose the next version and to write `CHANGELOG.md`:

| Prefix | Use for | Effect on the next release |
| --- | --- | --- |
| `feat:` | a new capability | minor bump, listed under Features |
| `fix:` | a bug fix | patch bump, listed under Bug Fixes |
| `perf:` | a performance improvement | patch bump, listed under Performance Improvements |
| `docs:`, `test:`, `refactor:`, `build:`, `ci:`, `chore:` | everything else | no release on their own |
| `feat!:`, `fix!:`, or a `BREAKING CHANGE:` footer | an incompatible change | minor bump while the version is below 1.0.0, major after |

Pull requests are squash-merged, so the pull request title becomes the commit message on `main`. Give it a conventional title.

## Pull requests

1. Branch from `main`, make the change, add tests and docs.
2. Open a pull request. The template asks for a summary, the linked issue, the type of change and a checklist.
3. CI must pass: lint, type check, unit tests on every supported Python version, the dependency audit, the docs build, zizmor and CodeQL.
4. A maintainer reviews and squash-merges.

## Releases

Releases are automated with [release-please](https://github.com/googleapis/release-please) and [PyPI trusted publishing](https://docs.pypi.org/trusted-publishers/). A release is one pull request merge:

1. Every push to `main` runs `.github/workflows/release-please.yml`. It reads the conventional commits since the last release and opens, or updates, a pull request titled `chore(main): release X.Y.Z` that bumps `version` in `pyproject.toml` and adds the new entries to `CHANGELOG.md`. The version comes from the commit types above. Because the pull request is opened with the workflow token, GitHub does not start CI on it, and the branch ruleset will not let it merge without passing checks. Close the pull request and reopen it. That reopen event comes from you, so CI, CodeQL, zizmor and the docs build run as normal.
2. Merge that pull request. release-please creates the tag `vX.Y.Z` and the GitHub release, then dispatches `.github/workflows/release.yml` for the tag with `gh workflow run`. It has to dispatch it explicitly because GitHub does not start tag-triggered workflows for tags created with the workflow token.
3. `release.yml` checks that the tag matches the version in `pyproject.toml`, runs ruff, ty and the unit tests on the tagged commit, builds the sdist and wheel with `uv build`, and publishes them with trusted publishing. The publish job runs in the `pypi` environment. Give that environment a required reviewer in the repository settings, and the publish job waits for approval under the Actions tab before uploading. No PyPI API token is stored anywhere.

To force a particular version, add a `Release-As: X.Y.Z` footer to a commit on `main`; release-please opens the release pull request for that version regardless of the commit types.

Pushing a tag by hand also works, for example `git tag v0.2.0 && git push origin v0.2.0` after bumping `pyproject.toml` yourself. It runs the same `release.yml`, but `CHANGELOG.md` is then not updated.

## Code of conduct and security

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md). Report security problems privately as described in [SECURITY.md](SECURITY.md). For questions, see [SUPPORT.md](SUPPORT.md).
