"""Inline a saved query task as a SQL file task, so that its rows come back with the run."""

from __future__ import annotations

from databricks.sdk.service import jobs, sql

from pytest_databricks_lakeflow.errors import LakeflowError
from pytest_databricks_lakeflow.settings.params import declared_params
from pytest_databricks_lakeflow.settings.tasks import disabled_task_keys, enabled_tasks


def saved_query_tasks(settings: jobs.JobSettings) -> list[jobs.Task]:
    """Get the enabled tasks that run a saved query, tasks nested in a `for_each_task` included.

    A disabled task, a task downstream of one and a task nested in a disabled `for_each_task` does not run, so it is
    left out, as `run_job_tasks` leaves it out.

    Args:
        settings: The job's settings.

    Returns:
        Every enabled task with a `sql_task.query`, in the order of `settings.tasks`, nested tasks last.
    """
    found: list[jobs.Task] = []
    for task in enabled_tasks(settings, skip=disabled_task_keys(settings)):
        if saved_query_id(task):
            found.append(task)
    return found


def saved_query_id(task: jobs.Task) -> str | None:
    """The id of the saved query that `task` runs, or `None` when it runs none."""
    if task.sql_task is None or task.sql_task.query is None:
        return None
    return task.sql_task.query.query_id


def inline_saved_query(job_key: str, settings: jobs.JobSettings, task: jobs.Task, query: sql.Query, path: str) -> str:
    """Point a saved query task at the SQL file `path`, carrying the query's parameter defaults, and build the file.

    A saved query task falls back to the query's own default for a parameter that `sql_task.parameters` does not set,
    and a SQL file task has no defaults: `:name` then fails with `UNBOUND_SQL_PARAMETER` and `{{ name }}` with
    `PARSE_SYNTAX_ERROR`. So each default of a parameter that neither the task nor a job parameter sets is added to
    the task's `parameters`; a job parameter binds the marker of its name in every SQL file task. A legacy
    `{{ name }}` marker binds from `parameters` in a SQL file task as in a saved query, so the text is kept as
    written. The file starts with `USE CATALOG` and `USE SCHEMA` for the catalog and schema the query sets, which
    resolve unqualified names as the saved query does; the task's rows are those of the last statement.

    Args:
        job_key: The bundle resource key of the job, for error messages.
        settings: The job's settings, whose job parameters bind their markers themselves.
        task: The task, whose `sql_task` is changed in place: `query` is dropped and `file` set.
        query: The saved query, as `queries.get` returns it.
        path: The workspace path the file will be uploaded to.

    Returns:
        The file's SQL text.

    Raises:
        LakeflowError: If a parameter that neither the task nor a job parameter sets has a default other than text, a
            number or a single enum value.
    """
    assert task.sql_task is not None
    parameters = dict(task.sql_task.parameters or {})
    job_parameters = declared_params(settings)
    for parameter in query.parameters or []:
        name = parameter.name
        if not name or name in parameters or name in job_parameters:
            continue
        parameters[name] = _parameter_default(job_key, settings, task, parameter)
    task.sql_task.query = None
    task.sql_task.file = jobs.SqlTaskFile(path=path, source=jobs.Source.WORKSPACE)
    task.sql_task.parameters = parameters or None
    lines: list[str] = []
    if query.catalog:
        lines.append(f"USE CATALOG {_quoted_identifier(query.catalog)};")
    if query.schema:
        lines.append(f"USE SCHEMA {_quoted_identifier(query.schema)};")
    lines.append(query.query_text or "")
    return "\n".join(lines) + "\n"


def _parameter_default(job_key: str, settings: jobs.JobSettings, task: jobs.Task, parameter: sql.QueryParameter) -> str:
    """The default of a saved query parameter as the string a SQL task parameter takes."""
    if parameter.text_value is not None:
        return parameter.text_value.value or ""
    if parameter.numeric_value is not None and parameter.numeric_value.value is not None:
        number = float(parameter.numeric_value.value)
        if number.is_integer():
            return str(int(number))
        return repr(number)
    enum = parameter.enum_value
    if enum is not None and enum.multi_values_options is None:
        values = enum.values or []
        if len(values) == 1:
            return values[0]
    raise LakeflowError(
        f"Task {task.task_key!r} of {job_key!r} runs saved query {saved_query_id(task)}, whose parameter "
        f'{parameter.name!r} has {_default_kind(parameter)}, which saved_queries="inline" cannot carry into '
        f"the SQL file. Set it with {_task_params_hint(job_key, settings, task, parameter.name or '')}."
    )


def _task_params_hint(job_key: str, settings: jobs.JobSettings, task: jobs.Task, name: str) -> str:
    """The `task_params` that set `name` on `task`, through `lakeflow.task()` for a task nested in a for-each task."""
    task_params = f"task_params={{{task.task_key!r}: {{{name!r}: '<value>'}}}}"
    for loop in settings.tasks or []:
        if loop.for_each_task is not None and loop.for_each_task.task is task:
            return f"lakeflow.task('{job_key}.{loop.task_key}.{task.task_key}', {task_params})"
    return task_params


def _default_kind(parameter: sql.QueryParameter) -> str:
    """Describe a saved query parameter's default that `_parameter_default` cannot carry."""
    if parameter.date_value is not None:
        return "a date default"
    if parameter.date_range_value is not None:
        return "a date range default"
    if parameter.query_backed_value is not None:
        return "a query-backed default"
    if parameter.enum_value is not None and parameter.enum_value.multi_values_options is not None:
        return "a multi-value enum default"
    if parameter.enum_value is not None:
        return "an enum default that is not one value"
    return "no default"


def _quoted_identifier(name: str) -> str:
    """`name` in backticks, with each backtick in it doubled, as Databricks SQL quotes an identifier."""
    escaped = name.replace("`", "``")
    return f"`{escaped}`"
