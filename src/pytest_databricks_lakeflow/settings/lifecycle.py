"""The setup and teardown tasks that run first and last in every run of the copy."""

from __future__ import annotations

from collections.abc import Collection, Sequence

from databricks.sdk.service import jobs

from pytest_databricks_lakeflow.errors import LakeflowError
from pytest_databricks_lakeflow.naming import PARAMS_WIDGET, PREFIX, SETUP_KEY, TEARDOWN_KEY
from pytest_databricks_lakeflow.settings.compute import Helpers, add_helper_compute, on_helper_compute
from pytest_databricks_lakeflow.settings.params import declared_params
from pytest_databricks_lakeflow.settings.references import escape


def inject_lifecycle(
    settings: jobs.JobSettings,
    setup: str | None,
    teardown: str | None,
    helpers: Helpers,
    params: Collection[str] = (),
) -> jobs.JobSettings:
    """Add a setup task that runs before everything else and a teardown task that runs after everything else.

    Every task without `depends_on`, stubs included, comes to depend on the setup task. The teardown task depends
    on every task that no other task depends on, probes included, with `run_if` `ALL_DONE`, so it runs whether the
    tasks before it passed or failed. Both are notebook tasks on the helper compute, with a `__pytest_params` widget
    as `name_lifecycle_params` writes it.

    Args:
        settings: The job's settings. They are changed in place.
        setup: Workspace path of the setup notebook, or `None` for no setup task.
        teardown: Workspace path of the teardown notebook, or `None` for no teardown task.
        helpers: Where the helper notebooks are and what they run on.
        params: The names of the job parameters the test passes to every run.

    Returns:
        `settings`.
    """
    if setup is None and teardown is None:
        return settings
    tasks = list(settings.tasks or [])
    if setup is not None:
        for task in tasks:
            if not task.depends_on:
                task.depends_on = [jobs.TaskDependency(task_key=SETUP_KEY)]
        tasks.insert(0, _lifecycle_task(SETUP_KEY, setup, helpers))
    if teardown is not None:
        last = _lifecycle_task(TEARDOWN_KEY, teardown, helpers)
        last.depends_on = _leaves(tasks)
        last.run_if = jobs.RunIf.ALL_DONE
        tasks.append(last)
    settings.tasks = tasks
    name_lifecycle_params(settings, params)
    add_helper_compute(settings, helpers)
    return settings


def _leaves(tasks: Sequence[jobs.Task]) -> list[jobs.TaskDependency]:
    """A dependency on each of `tasks` that no other of them depends on, in order."""
    upstream = {d.task_key for t in tasks for d in t.depends_on or []}
    leaves: list[jobs.TaskDependency] = []
    for task in tasks:
        if task.task_key not in upstream:
            leaves.append(jobs.TaskDependency(task_key=task.task_key))
    return leaves


def rewire_teardown(settings: jobs.JobSettings) -> jobs.JobSettings:
    """Make the teardown task depend again on every task that no other task depends on.

    For tasks replaced after `inject_lifecycle`, such as the stubs of `run(replay=...)`, which drop their
    `depends_on` and so can leave the tasks upstream of them with no task after them.

    Args:
        settings: The job's settings. They are changed in place.

    Returns:
        `settings`.
    """
    others: list[jobs.Task] = []
    teardown = None
    for task in settings.tasks or []:
        if task.task_key == TEARDOWN_KEY:
            teardown = task
        else:
            others.append(task)
    if teardown is not None:
        teardown.depends_on = _leaves(others)
    return settings


def _lifecycle_task(task_key: str, notebook_path: str, helpers: Helpers) -> jobs.Task:
    notebook = jobs.NotebookTask(notebook_path=notebook_path)
    return on_helper_compute(jobs.Task(task_key=task_key, notebook_task=notebook), helpers)


def name_lifecycle_params(settings: jobs.JobSettings, params: Collection[str]) -> jobs.JobSettings:
    """Write the names of the job's parameters into the `__pytest_params` widget of the setup and teardown tasks.

    The names are the parameters the job declares and then those in `params`, which `allow_undeclared_params` may
    let through undeclared. The notebook reads the parameters from the widgets that the platform fills with them,
    and falls back to these names where `dbutils.widgets.getAll` is missing. `EphemeralJob.create` calls this again
    after `modify_settings`, which may declare more.

    Args:
        settings: The job's settings. They are changed in place.
        params: The names of the job parameters the test passes to every run.

    Returns:
        `settings`.
    """
    widget = escape(lifecycle_params(settings, params))
    for task in settings.tasks or []:
        if task.task_key in (SETUP_KEY, TEARDOWN_KEY) and task.notebook_task is not None:
            task.notebook_task.base_parameters = {**(task.notebook_task.base_parameters or {}), PARAMS_WIDGET: widget}
    return settings


def lifecycle_params(settings: jobs.JobSettings, params: Collection[str]) -> list[str]:
    """The job parameters a setup or teardown step can read: those the job declares, then the others in `params`."""
    names: list[str] = []
    for name in [*declared_params(settings), *params]:
        if name not in names and not name.startswith(PREFIX):
            names.append(name)
    return names


def check_lifecycle_params(
    job_key: str, settings: jobs.JobSettings, params: Collection[str], argument: str, names: Collection[str]
) -> None:
    """Fail early on a `${name}` in `setup=` or `teardown=` statements that names no parameter of the job.

    Args:
        job_key: The bundle resource key of the job, for error messages.
        settings: The job's settings.
        params: The names of the job parameters the test passes to every run.
        argument: `"setup"` or `"teardown"`, for error messages.
        names: The placeholders the statements use.

    Raises:
        LakeflowError: If a name is neither declared by the job nor in `params`.
    """
    unknown = sorted(set(names) - set(lifecycle_params(settings, params)))
    if unknown:
        declared = sorted(declared_params(settings))
        raise LakeflowError(
            f"{argument}= reads {unknown}, which job {job_key!r} does not declare and params does not hold. "
            f"Declared: {declared}. Pass them in params, or write $$ for a literal dollar."
        )
