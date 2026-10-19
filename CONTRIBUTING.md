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

The unit tests use pytest's `pytester` and an in-memory fake of the parts of `WorkspaceClient` the plugin uses, in `tests/unit/fakes.py`. They never touch the network. CI runs them on every supported Python version and once more on the oldest supported versions of the dependencies: Python 3.11, pytest 8, databricks-sdk 0.66 and filelock 3.

## Integration tests

`tests/integration` holds the acceptance tests. They run against the bundles under [`tests/integration/bundles`](tests/integration/bundles) in a real workspace, which needs serverless jobs, a catalog you can create schemas in, and credentials that both the CLI and the SDK can use:

```bash
LAKEFLOW_INTEGRATION=1 DATABRICKS_CONFIG_PROFILE=<profile> LAKEFLOW_CATALOG=<catalog> \
    LAKEFLOW_WAREHOUSE_ID=<warehouse id> uv run pytest tests/integration -n 4 --lakeflow-sweep
```

`LAKEFLOW_CATALOG` is the catalog the tests create schemas in. `LAKEFLOW_WAREHOUSE_ID` is the id of a SQL warehouse the `sql` bundle runs its queries on, passed as the bundle variable `warehouse_id`; `test_sql.py` skips when it is unset. Without `LAKEFLOW_INTEGRATION=1`, `tests/integration/conftest.py` skips every test in the directory, so a plain `uv run pytest` is safe. The first test of each module deploys its bundle to the `test` target; for the example bundle that also builds the example wheel with `uv build --wheel`. Pass `--lakeflow-destroy` to remove the deployments at the end, and `--lakeflow-keep` to inspect the jobs afterwards.

### Bundles

Each use case gets its own bundle, so a bundle only holds the jobs and tasks its tests need:

```text
tests/integration/
    conftest.py                  # BUNDLES, lakeflow_for(name), the session fixture, the skip hook
    bundles/
        example/                 # name: lakeflow_example, the bundle the docs snippets use
            databricks.yml
            resources/*.job.yml
            src/notebooks/*.py
        sql/                     # name: lakeflow_sql, a SQL file task on the warehouse from LAKEFLOW_WAREHOUSE_ID
            databricks.yml
            resources/*.job.yml
            src/queries/*.sql
        branches/                # name: lakeflow_branches, an if/else condition job
    test_example.py              # one module per bundle, named after it
    test_sql.py
    test_branches.py
```

`conftest.py` defines `BUNDLES = Path(__file__).parent / "bundles"` and `lakeflow_for(name)`, which returns `Lakeflow(bundle_root=BUNDLES / name, catalog=os.environ.get("LAKEFLOW_CATALOG"))`. The session `lakeflow` fixture points at the `example` bundle. To add a bundle:

1. Create `tests/integration/bundles/<name>/` with a `databricks.yml` (a unique `bundle.name`, a `test` target with `mode: development` and `presets: trigger_pause_status: PAUSED`, like the example), the job YAML under `resources/` and the notebooks under `src/`. Prefer notebook-only, serverless bundles so deploys stay fast, and keep the jobs and tasks to what the tests need. Check the YAML with `databricks bundle validate -t test` from the bundle directory before deploying.
2. Create `tests/integration/test_<name>.py` and override the `lakeflow` fixture at module scope so the plugin's automatic `lakeflow` marker still applies:

    ```python
    import pytest

    from pytest_databricks_lakeflow import Lakeflow

    from .conftest import lakeflow_for


    @pytest.fixture(scope="module")
    def lakeflow() -> Lakeflow:
        return lakeflow_for("<name>")
    ```

3. Write the tests as plain pytest functions in the style of `test_example.py`, run the module against a workspace and watch it fail for the right reason before writing the bundle.

Names that reach the workspace (bundle names, job names, task keys, notebooks, schemas, tags, parameters) must be neutral and specific to the use case.

In CI, the `Integration tests` job runs on pushes to `main` once these settings exist: the repository variables `DATABRICKS_HOST` and `LAKEFLOW_CATALOG`, and, in a GitHub environment named `integration`, the secrets `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET` for a service principal that can create jobs, use serverless compute and create schemas in that catalog. Until `DATABRICKS_HOST` is set, the job is skipped. The optional repository variable `LAKEFLOW_WAREHOUSE_ID` names a SQL warehouse the service principal can use; without it the `sql` bundle's tests are skipped.

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

Releases are automated with [release-please](https://github.com/googleapis/release-please) and [PyPI trusted publishing](https://docs.pypi.org/trusted-publishers/). Merging a `feat:`, `fix:` or `perf:` pull request is enough to publish a release. Nothing else needs a click.

1. Every push to `main` runs `.github/workflows/release-please.yml`. It reads the conventional commits since the last release and opens, or updates, a pull request titled `chore(main): release X.Y.Z` that bumps `version` in `pyproject.toml` and adds the new entries to `CHANGELOG.md`. The version comes from the commit types above. The workflow enables auto-merge on that pull request, and it squash-merges itself as soon as the checks pass.
2. That merge runs release-please again. It creates the tag `vX.Y.Z` and the GitHub release.
3. The tag starts `.github/workflows/release.yml`. It checks that the tag matches the version in `pyproject.toml`, runs ruff, ty and the unit tests on the tagged commit, builds the sdist and wheel with `uv build`, and publishes them from the `pypi` environment with trusted publishing. PyPI trusts that workflow and environment directly, so no PyPI token is stored anywhere.

Because the release pull request merges itself, every `feat:`, `fix:` or `perf:` merge is on PyPI a few minutes later. To ship several changes as one release, land them in one pull request, or give the intermediate pull requests a non-releasing type such as `refactor:` or `chore:` and the final one `feat:`.

To force a particular version, add a `Release-As: X.Y.Z` footer to a commit on `main`; release-please opens the release pull request for that version regardless of the commit types. The squash commit body is empty by default, so type the footer into the commit message box when merging.

Pushing a tag by hand also works, for example `git tag v0.2.0 && git push origin v0.2.0` after bumping `pyproject.toml` yourself. It runs the same `release.yml`, but `CHANGELOG.md` is then not updated.

### The release App

release-please runs with a GitHub App installation token instead of the workflow's own `GITHUB_TOKEN`. Pull requests opened with `GITHUB_TOKEN` start their CI runs in an approval-required state, and tags pushed with it start no workflows, so neither the auto-merge nor the publish would happen without a click. An App has neither restriction. Setting it up is a one-time job for the repository owner:

1. Create the App at <https://github.com/settings/apps/new>. Any name works, and the homepage URL can be this repository. Untick **Active** under Webhook. Under Repository permissions grant **Contents: Read and write**, **Pull requests: Read and write** and **Issues: Read and write** (release-please labels the pull request through the issues API). Leave "Where can this GitHub App be installed?" on **Only on this account**.
2. On the App's settings page, note the **Client ID** and generate a key under **Private keys**. Then open **Install App** and install it on this repository only.
3. In the repository, create an environment named `release`. Add the variable `RELEASE_APP_CLIENT_ID` with the Client ID and the secret `RELEASE_APP_PRIVATE_KEY` with the contents of the downloaded `.pem` file. Under "Deployment branches and tags", allow only `main`.

If a merged release pull request has not produced a tag within a few minutes, start release-please by hand with `gh workflow run release-please.yml`. If the tag exists but nothing reached PyPI, run `gh workflow run release.yml --ref vX.Y.Z` and read the publish step's log.

## Code of conduct and security

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md). Report security problems privately as described in [SECURITY.md](SECURITY.md). For questions, see [SUPPORT.md](SUPPORT.md).
