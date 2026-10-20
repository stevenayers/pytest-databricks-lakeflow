# Change the job settings

These options change the settings of the job copy before it is created, so they apply to every run of it. They work on `lakeflow.job()` and, unless noted, on `lakeflow.task()`. Nothing here changes the deployed job.

## Disable a task

Switch off a task that notifies people or refreshes a downstream system, without changing the bundle:

```python
def test_without_load(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job", disable=["load"]) as job:
        run = job.run()
        assert run.succeeded, run
        assert "load" in run.disabled_tasks
```

- A disabled task ends `DISABLED` and lands in `run.disabled_tasks`. `run.task("load")` raises a `KeyError` that says `Disabled in the job: ['load']`. Tasks the bundle already disables land there too.
- The platform also disables every task downstream of a disabled one, with the termination message "This task was disabled because it is downstream of a disabled task". `run.succeeded` looks only at `run.tasks`, so assert on `run.executed_tasks` to be sure the tasks you meant to test ran:

```python
def test_without_extract(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job", disable=["extract"]) as job:
        run = job.run()
        assert run.succeeded  # only my_wheel_task ran
        assert sorted(run.disabled_tasks) == ["extract", "load", "transform"]
        assert run.executed_tasks == ["my_wheel_task"]
```

- When nothing is left enabled, the platform skips the whole run. Every task is then in `run.disabled_tasks` with no result state, `run.succeeded` is false, and `repr(run)` names the reason as `run: Skipped because every task is disabled`.
- An unknown key raises `LakeflowError` listing the job's tasks before anything is created. `run(only=...)` refuses to name a disabled task, see [Run only some tasks](../job-tests.md#run-only-some-tasks).
- `Lakeflow(disable=[...])` disables the tasks in every copy and skips a key that a job does not have, so one default such as `disable=["notify_slack"]` serves the whole bundle. A test that needs the task passes `disable=[]`. `lakeflow.task()` has no `disable`.

Dashboard and SQL alert subscriptions are already paused in every copy. Alert tasks and Power BI tasks are not, so disable those, see [Alert tasks](task-types.md#alert-tasks).

## Override one task's parameters

`task_params` changes the parameters of single tasks, by task key, while the job parameters and the other tasks stay as deployed:

```python
def test_transform_with_a_limit(lakeflow: Lakeflow) -> None:
    with lakeflow.job(
        "my_example_job",
        task_params={"transform": {"limit": "10"}, "my_wheel_task": ["goodbye"]},
    ) as job:
        run = job.run()
        assert run.task("transform").parameters["limit"] == "10"
```

- A mapping of names to strings is merged into a notebook task's `base_parameters`, a Python wheel task's `named_parameters` or a SQL task's `parameters`, so the template's other parameters, such as `{{tasks.extract.values.row_count}}`, survive.
- A list of strings replaces a Python script, JAR or Python wheel task's `parameters`, or a dbt task's `commands`. A wheel task takes one or the other: a mapping clears its `parameters` and a list clears its `named_parameters`.
- Values are passed as they are, so references such as `{{job.parameters.param1}}` work. A task nested in a for-each task is named by its own key, and every iteration gets the parameters.
- A job parameter wins over a task parameter of the same name, so a name in a mapping that is also a job parameter emits a `UserWarning` saying that the job value wins.
- `LakeflowError` is raised before the job is created for a key that is not a task of the job and for a task type that has no parameters, a for-each task included: name its nested task instead. `ValueError` is raised for a value that does not fit the task type, such as a list for a notebook task, for an empty dbt command list, and for a name starting with `__pytest_`.

On `lakeflow.task()` it may name only the task under test, and it is applied before the stubs, so a `{{tasks.x.values.y}}` it adds needs `x` in `upstream_task_values`.

## Test retries and timeouts

`task_overrides` sets the retry and timeout fields of single tasks in the copy:

```python
def test_transform_is_retried_once(lakeflow: Lakeflow) -> None:
    overrides = {"transform": {"max_retries": 1, "min_retry_interval_millis": 0}}
    with lakeflow.job("my_example_job", params={"param1": "forbidden"}, task_overrides=overrides) as job:
        run = job.run()
        assert run.failed
        assert len(run.task("transform").attempts) == 2
```

- The fields are `timeout_seconds`, `max_retries`, `min_retry_interval_millis` and `retry_on_timeout`, and they replace the template's. A task's `timeout_seconds` applies to each attempt, and `retry_on_timeout` decides whether an attempt the platform stopped is retried.
- Every attempt is in the task's `attempts`, in order. The `TaskRun` itself describes the last one.
- A task the platform stops at its `timeout_seconds` ends `TIMEDOUT`: `task.timed_out` and `run.timed_out` are true, and the run is returned, not raised. The plugin's own `timeout` on `run()` is the limit for the whole run and raises `LakeflowTimeout`.
- A key the job does not have raises `LakeflowError` listing the available tasks, and so does a stub, probe, setup or teardown task, because the overrides are applied before those are added. A field other than the four raises `ValueError` listing them. Either way no job is created.
- Serverless retries failed tasks on its own, which would hide a failure behind a second attempt. `Lakeflow(disable_auto_optimization=True)`, the default, switches that off for every serverless code task, nested for-each tasks included. It does not touch `max_retries`.

On `lakeflow.task()` it may name only the task under test; any other key raises `ValueError`.

## Run as another identity

By default the copy has no `run_as`, so it runs as whoever runs the tests. That avoids needing rights on another identity, but it also means the permissions of the deployed job's `run_as` principal are never tested. `run_as` takes three forms:

```python
lakeflow.job("my_example_job", run_as="keep")  # the template's run_as, as deployed
lakeflow.job("my_example_job", run_as="0000-app-id")  # a service principal, by application id
lakeflow.job("my_example_job", run_as="someone@example.com")  # a user
```

- A string with `@` names a user, any other string a service principal by its application id. For anything else, such as a group, pass a `jobs.JobRunAs`.
- To create a job that runs as a service principal, you need the Service Principal User role on it, granted to you or to one of your groups. The Service Principal Manager role does not include it, and workspace admins need it too. When the workspace client knows its account id (`account_id` in the profile, or `DATABRICKS_ACCOUNT_ID`), the plugin checks the role once per session before the first `jobs.create` and raises `LakeflowError` naming you and the principal when it is missing. When it cannot read the rule set, it creates the job anyway and turns a `PermissionDenied` from `jobs.create` into a `LakeflowError` that explains the role.
- The principal also needs read access to the deployed notebooks and files under the target's `root_path`, or the run fails when its tasks start.
- `Lakeflow(run_as=...)` sets the default for every copy. To run one copy as yourself while a default is set, pass your own user name.

## Add test-only dependencies

Job code under test sometimes needs packages that must not ship in the production wheel, such as a fixtures package or `chispa`. `extra_dependencies` adds them to the copy only:

```python
def test_with_test_helpers(lakeflow: Lakeflow) -> None:
    with lakeflow.task("my_example_job.my_wheel_task", extra_dependencies=["chispa>=0.10"]) as task:
        assert task.run().succeeded
```

- Each entry is a PyPI requirement, the `/Workspace` or `/Volumes` path of a wheel, or a local wheel (below). It is appended to the `dependencies` of every environment the copy takes from the template, and to the `libraries` of every notebook, Python script and Python wheel task that runs on a cluster, as a `whl` library for a path ending in `.whl` and a `pypi` library for anything else. Tasks nested in a for-each task get them too; the plugin's helper tasks and environment do not.
- A serverless notebook task without an `environment_key` cannot take dependencies from the job settings. Rather than let it fail minutes into the run on a `ModuleNotFoundError`, the plugin emits a `UserWarning` naming the tasks the dependencies do not reach when the job is created. Install them with `%pip install` in the notebook, or move the task onto an environment that lists them (below). To silence the warning for one test, mark it with `@pytest.mark.filterwarnings("ignore:extra_dependencies .* do not reach tasks")`.
- `Lakeflow(extra_dependencies=[...])` sets the default for every copy, and `extra_dependencies=[]` on a call adds none.

??? note "Which notebook tasks the warning names"
    The check reads the settings the job is created with, after `environments=` and `modify_settings`. It names every enabled notebook task whose cluster `libraries` or environment `dependencies` do not list every entry, so also a task that `modify_settings` moves onto a job cluster or onto an environment that `environments=` adds. It skips disabled tasks, tasks downstream of one, tasks in a disabled loop, notebook tasks on a SQL warehouse and the plugin's own tasks. A local wheel appears in the warning as its uploaded `/Workspace/.../.internal/<file name>` path. The warning is a reminder: the job is created and runs as before.

### Install a local wheel with extras

A path starting with `./` or `../` is a local wheel, relative to the bundle root, and can be a glob. Add extras after it to install the test-only packages your own wheel declares, such as `[project.optional-dependencies] test = ["chispa>=0.10"]`:

```python
def test_with_the_test_extra(lakeflow: Lakeflow) -> None:
    with lakeflow.task("my_example_job.my_wheel_task", extra_dependencies=["./dist/*.whl[test]"]) as task:
        assert task.run().succeeded
```

- Each matching file is pointed at the copy that the last `bundle deploy` uploaded under `<workspace.artifact_path>/.internal/`, with the extras added back, so the environment gains `/Workspace/.../.internal/pkg-0.1-py3-none-any.whl[test]`.
- The wheel must be a library or environment dependency of a job in the bundle, because a deploy uploads only those. With `sync="deploy"` the session's own deploy uploads it, with `sync="none"` an earlier deploy must have, and `sync="sync"` uploads no wheels, so a local path raises `LakeflowError`. A pattern that matches no file, or a file without an uploaded copy, raises `LakeflowBundleError`. A wheel built only for the tests has no uploaded copy: add it to a job of the bundle, or upload it to a volume and pass its `/Volumes` path.
- A wheel with extras reaches only the environments. The `whl` library of a task on a cluster takes no extras, so a wheel with extras raises `LakeflowError` when the copy has a notebook, Python script or Python wheel task on a cluster. Pass the packages the extras add as PyPI requirements instead.
- A local path that does not start with `./` or `../`, such as `dist/pkg.whl`, raises `ValueError`.

## Change a serverless environment

`environments` maps environment keys to fields of the environment's spec, for one copy or, on `Lakeflow`, for every copy:

```python
def test_on_environment_5(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job", environments={"default": {"environment_version": "5"}}) as job:
        assert job.run().succeeded
```

```python
@pytest.fixture(scope="session")
def lakeflow() -> Lakeflow:
    return Lakeflow(environments={"default": {"environment_version": "5"}})
```

- For a key the job already has, each field you pass replaces the template's field and the others are kept. `dependencies` replaces the whole list, the bundle's wheel included; to add packages, use `extra_dependencies` instead.
- A key the job does not have adds a new environment, such as `{"gpu": {"environment_version": "4", "dependencies": ["torch"]}}`, holding only the fields you pass. Moving a task onto it is a job for `modify_settings` (below). `Lakeflow(environments=...)` skips a key that a job does not have, so one default serves the whole bundle, and `environments={}` on a call keeps the template's environments.
- The fields are those of `compute.Environment`: `environment_version`, `base_environment`, `dependencies`, `java_dependencies` and the others your databricks-sdk knows. `base_environment` takes the `/Workspace` or `/Volumes` path of a custom base environment file; the Jobs API refuses a Databricks-provided id such as `workspace-base-environments/databricks_ml_v5`.
- `ValueError` is raised for a key starting with `__pytest_`, an entry that is not a mapping and a field the spec does not have. `LakeflowError` is raised for an environment that would set both `environment_version` and `base_environment` (set the one you do not want to `None`), and when the job would have more than 10 environments, the helper environment included.

To give a serverless notebook task a test-only package, add an environment that lists it and move the task onto it:

```python
import dataclasses

import pytest
from databricks.sdk.service import jobs

from pytest_databricks_lakeflow import Lakeflow


def extract_on_the_test_environment(settings: jobs.JobSettings, item: pytest.Item) -> None:
    for task in settings.tasks or []:
        if task.task_key == "extract":
            task.environment_key = "test"


def test_extract_with_synthetic_rows(lakeflow: Lakeflow) -> None:
    on_test_environment = dataclasses.replace(lakeflow, modify_settings=extract_on_the_test_environment)
    with on_test_environment.task(
        "my_example_job.extract",
        environments={"test": {"environment_version": "4", "dependencies": ["faker==30.0.0"]}},
    ) as task:
        assert task.run().succeeded
```

## Change anything else before the job is created

`Lakeflow(modify_settings=...)` is called with each copy's `JobSettings` and the test's `pytest.Item`, after the plugin has sanitised the copy and just before `jobs.create`. Change the settings in place:

```python
import pytest
from databricks.sdk.service import jobs

from pytest_databricks_lakeflow import Lakeflow


def tag_with_module(settings: jobs.JobSettings, item: pytest.Item) -> None:
    settings.tags = {**(settings.tags or {}), "module": item.path.stem}


@pytest.fixture(scope="session")
def lakeflow() -> Lakeflow:
    return Lakeflow(modify_settings=tag_with_module)
```

Two more that come up: pin the branch of a job that runs from Git, and replace the libraries of one task:

```python
from databricks.sdk.service import compute, jobs


def pin_branch_and_libraries(settings: jobs.JobSettings, item: pytest.Item) -> None:
    if settings.git_source is not None:
        settings.git_source.git_branch = "feature/new-loader"
        settings.git_source.git_tag = None
        settings.git_source.git_commit = None
    for task in settings.tasks or []:
        if task.task_key == "load":
            task.libraries = [compute.Library(pypi=compute.PythonPyPiLibrary(package="requests==2.32.3"))]
```

- Job parameters that `modify_settings` declares are accepted by `params`.
- `access_control_list` is not a `JobSettings` field: use `Lakeflow(permissions=...)`, see [Let the team open kept jobs](ci.md#let-the-team-open-kept-jobs).
- To apply a callback to some tests only, derive a `Lakeflow` with `dataclasses.replace(lakeflow, modify_settings=...)`; it shares the session's deploy.
- Compute recipes for `modify_settings`, such as shrinking a cluster, are in [Compute and cost](compute.md#change-compute-with-modify_settings).

## Set a default for every copy

`run_as`, `disable`, `extra_dependencies`, `environments`, `children`, `setup`, `teardown`, `saved_queries` and `queue` on `Lakeflow` are the defaults of the arguments of the same name on `job()`, `task()`, `run()` and `start()`. A call that leaves the argument out takes the default; anything else replaces it whole, so `disable=[]` disables nothing.

`Lakeflow` is a dataclass, so a `conftest.py` in a subdirectory can derive its own from the parent's. The override applies to the tests below that directory, and every `Lakeflow` that names the same bundle target shares one deploy:

```python
import dataclasses

import pytest

from pytest_databricks_lakeflow import Lakeflow


@pytest.fixture(scope="session")
def lakeflow(lakeflow: Lakeflow) -> Lakeflow:
    return dataclasses.replace(lakeflow, catalog="team_b", children="refuse")
```

The [configuration reference](../reference/lakeflow.md#defaults-for-per-call-arguments) lists which defaults a call can opt out of.
