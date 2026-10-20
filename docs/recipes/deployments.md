# Bundle deployments

How the plugin deploys the bundle target it tests against, and how to keep deployments apart when several people and pipelines share a workspace.

## Choose the target and pass bundle variables

The plugin always passes `-t <target>` to the CLI and does not read `DATABRICKS_BUNDLE_TARGET`, so choose the target in the fixture. `variables` go to every `bundle` command as `--var name=value`, `bundle destroy` included:

```python
import os

import pytest

from pytest_databricks_lakeflow import Lakeflow


@pytest.fixture(scope="session")
def lakeflow() -> Lakeflow:
    return Lakeflow(
        target=os.environ.get("LAKEFLOW_TARGET", "test"),
        variables={"catalog": "dev_sandbox"},
    )
```

- Bundle variables are fixed at deploy time, and the plugin deploys once per session. Anything that changes per test must arrive as a job parameter instead, see [Read catalog and schema from job parameters](../getting-started.md#read-catalog-and-schema-from-job-parameters).
- Two `Lakeflow` instances for the same target with different `variables`, `sync`, `deploy_flags` or `helpers_path` are set up, and deployed, separately. Under xdist that can put two deploys of one target side by side, and one can fail on the CLI's deployment lock, so give each set of variables its own target.
- `BUNDLE_VAR_<name>` variables are read by the CLI, not by the plugin, so they work but are not part of the setup key.
- The CLI takes the target's host from `databricks.yml`, while the SDK client takes it from `profile` or the `DATABRICKS_*` variables. The plugin compares the two before deploying and fails with `LakeflowError` naming both hosts when they differ. To give pytest the target's own credentials, run it through the CLI: `databricks bundle run -t test -- uv run pytest`.

## Skip the deploy

`sync` chooses how the bundle is prepared:

| `sync` | What runs | When to use it |
| --- | --- | --- |
| `"deploy"` (default) | `bundle validate`, `bundle deploy`, `bundle summary`, then `jobs.get` for each deployed job. The only strategy that builds and uploads wheels. | Everyday development and CI. |
| `"sync"` | `bundle sync`, then the templates come from `bundle validate -o json`. Jobs that use local wheels or JARs are refused, because `sync` does not upload them. | Notebook-only bundles whose target is already deployed. |
| `"none"` | `bundle validate` only. Local wheel and JAR paths are pointed at the copies that an earlier deploy uploaded, after checking that they exist. | A target you deploy yourself, once per pipeline. |

- With `"sync"` and `"none"`, a job that still contains `${...}` references after `validate`, such as `${resources.jobs.other.id}`, is refused, because `validate` does not resolve them.
- A bundle wheel artifact without `dynamic_version: true` is uploaded under the same file name on every deploy, and serverless is reported to cache wheels by file name, so a test could run stale code. The first template lookup emits one `UserWarning` per session naming such artifacts. Set `artifacts.<name>.dynamic_version: true` in `databricks.yml`.
- With `"none"`, a local wheel built more than 60 seconds after its uploaded copy emits one more `UserWarning` per session, asking for a `databricks bundle deploy`. The comparison reads times, not contents, see [Limitations](../reference/limitations.md).
- `Lakeflow(strict_artifacts=True)` turns both warnings into `LakeflowError`.

## Give each branch and identity its own deployment

Every session that deploys the same target deploys into the same `workspace.root_path`. When a second session deploys that target while the first session's jobs are running, the deploy replaces the wheels under `artifact_path/.internal/`, and the running copies fail when they look for the old ones. Give each identity and branch its own deployment in `databricks.yml`:

```yaml
variables:
  lakeflow_suffix:
    description: Keeps one test deployment per branch apart from the others.
    default: local

experimental:
  skip_artifact_cleanup: true

targets:
  test:
    mode: development
    workspace:
      root_path: ~/.bundle/${bundle.name}/test/${var.lakeflow_suffix}
```

`~` is the home folder of the identity that deploys, so a developer and a CI service principal never share a deployment. `${var.lakeflow_suffix}` separates branches. `experimental.skip_artifact_cleanup` stops a deploy from deleting the wheels that the previous deploy of the same root uploaded, so a redeploy from the same branch does not break jobs that are still running.

Pass the variable from the `lakeflow` fixture in the root `conftest.py`. Branch names can contain `/`, so replace anything outside letters, digits, `_` and `-`:

```python
import os
import re

import pytest

from pytest_databricks_lakeflow import Lakeflow


@pytest.fixture(scope="session")
def lakeflow() -> Lakeflow:
    return Lakeflow(
        variables={"lakeflow_suffix": re.sub(r"[^A-Za-z0-9_-]", "-", os.environ.get("GITHUB_HEAD_REF") or "local")},
        deploy_flags=["--fail-on-active-runs"],
    )
```

Each branch then leaves its own deployment behind, so end the pipeline with `pytest --lakeflow-destroy` (below).

## Pass flags to bundle deploy

`deploy_flags` adds flags at the end of `databricks bundle deploy`, such as `["--fail-on-active-runs"]`, which refuses to deploy while a deployed job of the bundle has an active run. It catches someone running the deployed test target's jobs by hand. It does not catch a concurrent test session, because the plugin never runs the deployed jobs. Only separate deployments protect one session from another.

`-t`, `--target`, `-e`, `--environment`, `-p`, `--profile`, `--var` and `--auto-approve` raise `ValueError`: use `target`, `profile` or `variables` instead. Instances with different flags deploy separately.

## Destroy the test deployment

```bash
pytest --lakeflow-destroy
```

runs `databricks bundle destroy -t <target> --auto-approve` at the end of the session for each target the session deployed with `sync="deploy"`, with the same `--var` arguments as its deploy. Under xdist each worker reports the deployments it made to the controller, which destroys them once. A failed destroy is printed with the summary and turns a passing session into a failed one.

Without it the deployed target stays, which makes the next session's deploy fast: an unchanged bundle deploys in seconds.
