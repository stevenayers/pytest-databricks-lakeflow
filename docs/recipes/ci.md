# CI and housekeeping

Run the suite in a pipeline, keep the workspace tidy, and lint every job's wiring without running it.

## Run the tests in CI

Use a service principal with [OAuth machine-to-machine credentials](https://docs.databricks.com/aws/en/dev-tools/auth/oauth-m2m), and install the CLI with `databricks/setup-cli`:

```yaml
env:
  DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST }}
  DATABRICKS_CLIENT_ID: ${{ secrets.DATABRICKS_CLIENT_ID }}
  DATABRICKS_CLIENT_SECRET: ${{ secrets.DATABRICKS_CLIENT_SECRET }}
steps:
  - uses: databricks/setup-cli@<pinned sha>
  - run: uv run pytest tests bundle/databricks.yml -n 4 --lakeflow-sweep --lakeflow-check --lakeflow-report-dir=build/lakeflow
  - uses: actions/upload-artifact@<pinned sha>
    if: always()
    with:
      name: lakeflow
      path: build/lakeflow
```

- `-n 4` runs four tests at a time with pytest-xdist. One worker deploys and the others wait, then every worker reuses the result.
- `--lakeflow-sweep` deletes the jobs that an earlier, crashed run left behind, see [Clean up leftovers](#clean-up-leftovers).
- `--lakeflow-check` adds one test per bundle job that fails on a wiring mistake without running the job (below). Name the bundle's `databricks.yml` on the command line so pytest collects it although it is outside `tests`.
- `--lakeflow-report-dir` keeps every run's output as a build artefact after the jobs are deleted, see [Keep every run's output as an artefact](debugging.md#keep-every-runs-output-as-an-artefact).
- The run id that tags every job is read from `GITHUB_RUN_ID` or `BUILD_BUILDID`, and the CI run URL, user, commit and branch go into the `pytest_ci_url`, `pytest_user`, `pytest_git_sha` and `pytest_git_branch` tags, so a job can be traced back to the pipeline run.
- A service principal has no CLI token to refresh, so xdist workers never race on one. A user-to-machine profile can fail with `exit status 45` under xdist, see [the FAQ](../faq.md#the-cli-failed-with-exit-status-45-under-xdist).
- When several branches or pipelines share one test target, give each its own deployment and end the pipeline with `--lakeflow-destroy`, see [Give each branch and identity its own deployment](deployments.md#give-each-branch-and-identity-its-own-deployment).

## Be stricter in CI

A CI run can refuse what a developer's run only warns about:

```python
import os

import pytest

from pytest_databricks_lakeflow import Lakeflow


@pytest.fixture(scope="session")
def lakeflow() -> Lakeflow:
    if os.environ.get("CI"):
        return Lakeflow(children="refuse", disable=["notify_slack"], keep_failed=False)
    return Lakeflow()
```

- `children="refuse"` fails a test whose copy would run a deployed child job, rather than warn, see [Jobs that run other jobs](task-types.md#jobs-that-run-other-jobs).
- `disable=["notify_slack"]` switches off a task in every copy; a job without it is left alone, and a test that needs it passes `disable=[]`.
- `keep_failed=False`, or `pytest --no-lakeflow-keep-failed`, deletes jobs even when their test fails, so the workspace does not fill with jobs nobody opens. Keep them when engineers can open them (below), and let the sweep remove them.

## Let the team open kept jobs

Only the identity that creates a job, and workspace admins, can open it. In CI that identity is a service principal, so the engineer reading the log cannot open a kept job's URL. `permissions` grants access on every copy when it is created:

```python
@pytest.fixture(scope="session")
def lakeflow() -> Lakeflow:
    return Lakeflow(permissions=[{"group_name": "data-eng", "permission_level": "CAN_VIEW"}])
```

- Each entry is a dict or a `jobs.JobAccessControlRequest` with `user_name`, `group_name` or `service_principal_name` and a `permission_level` such as `CAN_VIEW`, `CAN_MANAGE_RUN` or `CAN_MANAGE`. The entries go to `jobs.create` unchanged, so an unknown group fails the test with the API's error.
- `IS_OWNER` raises `ValueError` when `Lakeflow` is created: the creator owns every copy. Grant `CAN_MANAGE` instead.
- The debug notebooks of `--lakeflow-debug-notebook` land in the service principal's home folder. Set `helpers_path` to a `/Shared/...` folder so engineers can open them.

## Check every job's wiring without running it

`lakeflow.check(key)` lints the job's template and returns one message per wiring mistake, without creating or running anything:

```python
def test_my_example_job_is_wired(lakeflow: Lakeflow) -> None:
    problems = lakeflow.check("my_example_job")
    assert not problems, "\n".join(problems)
```

It always reports a `{{job.parameters.<name>}}` reference to a parameter the job does not declare, a `{{tasks.<key>.values.<name>}}` reference to a task that is not upstream of the task that makes it, a `depends_on` entry naming a task that is not in the job, and retry settings on a for-each task, which cannot be retried. `bundle validate` passes all four, see [the FAQ](../faq.md#why-not-rely-on-bundle-validate). It also applies the rules that `rules=` names, from `ALL_RULES`; by default `DEFAULT_RULES`, every rule but `equal_to`:

| Rule | Reports |
| --- | --- |
| `wheel` | A `python_wheel_task` whose package has no wheel under the bundle's `dist/`, or whose `entry_point` is neither an entry point of the newest wheel of that package nor a name its `<package_name>/__init__.py` defines. Skipped when `dist/` holds no wheel. |
| `library` | A `libraries` `whl` or `jar`, a `spark_python_task.python_file` or an environment dependency that starts with `/` and does not exist in the workspace or the Volume. One read call per path. |
| `dash` | A task key with a dash that a task refers to with `{{tasks.<key>.…}}`, since the identifiers of a reference support only letters, digits and underscores. Rename the task, or escape the key with backticks, as in ``{{tasks.`load-raw`.values.n}}``. |
| `trigger` | Each distinct `{{job.trigger.*}}` reference, which a test's run-now run does not resolve as the trigger would; set its value with `trigger=`, see [Simulate a trigger](task-types.md#simulate-a-trigger). |
| `equal_to` | An `EQUAL_TO` or `NOT_EQUAL` condition with a number or a task value reference as an operand. Off by default: these operators compare text, so only a number written another way, such as `7.0`, misfires. |

```python
from pytest_databricks_lakeflow import ALL_RULES, DEFAULT_RULES, Lakeflow


def test_wired_without_trigger_references(lakeflow: Lakeflow) -> None:
    assert lakeflow.check("my_example_job", rules=DEFAULT_RULES - {"trigger"}) == []


def test_compares_text(lakeflow: Lakeflow) -> None:
    assert lakeflow.check("my_example_job", rules=ALL_RULES) == []
```

- The messages name the task, environment or parameter, task messages first in task order. A name outside `ALL_RULES` raises `ValueError`. `check` reads the workspace for the `library` rule and `dist/` for the `wheel` rule, and writes nothing.
- With the default `sync="deploy"`, the deploy refuses the four always-on mistakes one at a time, before any test runs. With `Lakeflow(sync="sync")` or `sync="none"`, `check` lints the job as `bundle validate` reads it and finds them all at once.

`pytest --lakeflow-check` runs `check` on every job of the bundle without a test of your own:

```bash
pytest tests databricks.yml --lakeflow-check
```

- Each `databricks.yml` that pytest collects becomes one test per job, `databricks.yml::<key>`, which passes when `check` finds nothing and fails with its messages otherwise. It applies every default rule but `trigger`, because a deployed job that reads its own trigger context is wired correctly. The tests get the `lakeflow` marker, so `-k` and `--deselect` pick jobs by key.
- pytest collects a `databricks.yml` only under the paths it is given, so name it when the bundle root is outside them.
- To know the job keys, the plugin sets the bundle up while collecting, once for all xdist workers. Fixtures cannot be requested while collecting, so it calls the `lakeflow` fixture that the `databricks.yml` sees, with no arguments. Put the override in a `conftest.py` in the bundle's directory or above it, such as one at the repository root, and do not make it request other fixtures. An override in `tests/conftest.py` beside `bundle/` is found when its `bundle_root` points at that bundle; one the `databricks.yml` cannot see makes collection fail rather than check the bundle with the default `Lakeflow()`.
- The default `sync="deploy"` stops at a deploy the platform refuses, which fails collection. Use `Lakeflow(sync="none")` against a deployed bundle, or `sync="sync"`, to get every mistake as a failed test.

## Clean up leftovers

A crashed session, a kept job or `--lakeflow-keep` leaves jobs, pipeline copies, schemas and notebooks in the workspace. The sweep deletes what the plugin made, recognised by its names and tags, from runs other than the current one, once it is older than `sweep_hours` (24 by default) or past its `pytest_expires` time:

```bash
pytest --lakeflow-sweep --lakeflow-sweep-schemas=dev_sandbox --lakeflow-sweep-notebooks --lakeflow-sweep-dry-run
```

| Flag | Adds |
| --- | --- |
| `--lakeflow-sweep` | Your own jobs tagged `pytest` and your own `[pytest]` pipeline copies, with their tables. Runs once per session, at the first bundle setup, in that target's workspace. |
| `--lakeflow-sweep-schemas=<catalog>` | The temporary `pytest_<run id>_<8 hex digits>` schemas of other runs in that catalog. |
| `--lakeflow-sweep-notebooks` | The `setup_<hash>`, `teardown_<hash>` and debug notebooks and the inlined saved query files in the helpers folder that no job the sweep keeps uses. |
| `--lakeflow-sweep-scope=all` | Everyone's, not only yours, and every user's default helpers folder. Needs the right to delete their jobs and schemas; a refused delete is recorded as an error. |
| `--lakeflow-sweep-branch=<branch>` | Only what is tagged `pytest_git_branch=<branch>`. |
| `--lakeflow-sweep-dry-run` | Deletes nothing and lists what it would delete in the terminal summary. |
| `--lakeflow-sweep-report=<path>` | Writes what the sweep deleted, or would delete, to a JSON file. |

- Dry-run a sweep first on a shared workspace. The terminal summary then lists each job, pipeline, schema and notebook it would delete, and `--lakeflow-sweep-report` writes the same as JSON.
- A failed delete or listing is logged at WARNING and the sweep carries on. No test fails because of the sweep.
- `--lakeflow-keep-ttl=48h` tags a run's jobs with `pytest_expires`; the sweep keeps them until then, however old they are.

[What the sweep deletes](../reference/flags.md#what-the-sweep-deletes) lists the exact rules for each kind of object.

## Sweep from a nightly script

`Lakeflow.sweep()` runs the same sweep from plain Python, without a pytest session, a bundle or a test, so a nightly job can clean up after every pipeline and developer that shares the workspace:

```python
from pytest_databricks_lakeflow import Lakeflow

report = Lakeflow(profile="ci").sweep(
    scope="all", schemas_catalog="dev_sandbox", notebooks=True, dry_run=True, report="sweep.json"
)
print(report.jobs, report.schemas, report.notebooks, report.errors)
```

- It takes `scope`, `schemas_catalog`, `notebooks`, `branch`, `dry_run` and `report`, which mean what the matching flags do, and uses the instance's workspace client, `sweep_hours` and `helpers_path`.
- It spares no run, so every job older than `sweep_hours` goes, unless its `pytest_expires` time is still ahead. Drop `dry_run=True` once the report lists only what you expect.
- It returns a `SweepReport` with `dry_run`, `jobs`, `pipelines`, `schemas`, `notebooks`, `errors` and `as_dict()`.
