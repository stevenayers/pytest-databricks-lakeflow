from __future__ import annotations

import hashlib
import inspect
import json
import posixpath
import re
import secrets
import string
import textwrap
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language

from pytest_databricks_lakeflow.limits import WIDGET_LIMIT_BYTES
from pytest_databricks_lakeflow.naming import (
    KEYS_WIDGET,
    MISSING,
    NOTEBOOK_DIGEST_CHARS,
    PARAMS_WIDGET,
    PREFIX,
    TASK_KEY_WIDGET,
    VALUES_WIDGET,
)

ARGUMENT_KINDS = (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
SET_VALUES = f"""# Databricks notebook source
import json

values = json.loads(dbutils.widgets.get("{VALUES_WIDGET}"))
for key, value in values.items():
    dbutils.jobs.taskValues.set(key=key, value=value)
"""
GET_VALUES = f"""# Databricks notebook source
import json

task_key = dbutils.widgets.get("{TASK_KEY_WIDGET}")
keys = json.loads(dbutils.widgets.get("{KEYS_WIDGET}"))
missing = "{MISSING}"

found = {{k: dbutils.jobs.taskValues.get(taskKey=task_key, key=k, default=missing) for k in keys}}
dbutils.notebook.exit(json.dumps({{
    "values": {{k: v for k, v in found.items() if v != missing}},
    "missing": [k for k, v in found.items() if v == missing],
}}))
"""
NOTEBOOKS = {"set_values": SET_VALUES, "get_values": GET_VALUES}
# Job parameters are read from the widgets, because the platform pushes every job parameter to every notebook task,
# one passed to run_now that the job does not declare included. `getAll` returns them with the task's own base
# parameters, which for these tasks all start with the plugin's prefix. Runtimes before Databricks Runtime 13.3 LTS
# have no `getAll`, so the notebook then reads the names in the `__pytest_params` widget.
READ_PARAMS = f"""try:
    __pytest_widgets = dbutils.widgets.getAll()
except Exception:
    __pytest_names = json.loads(dbutils.widgets.get("{PARAMS_WIDGET}"))
    __pytest_widgets = {{name: dbutils.widgets.get(name) for name in __pytest_names}}
__pytest_job_params = {{k: v for k, v in __pytest_widgets.items() if not k.startswith("{PREFIX}")}}"""
# The source of `fn`, then a call that passes the arguments it declares.
# The future import keeps annotations unevaluated: serverless environment 4 runs Python 3.12, which evaluates them
# when the def runs, and the names they use, such as SparkSession, are not imported in the notebook.
LIFECYCLE = """# Databricks notebook source
from __future__ import annotations

import json

{source}

{read_params}
__pytest_result = {call}
if __pytest_result is not None:
    dbutils.notebook.exit(json.dumps(__pytest_result, default=str))
"""
LIFECYCLE_VALUES = {"spark": "spark", "dbutils": "dbutils", "params": "__pytest_job_params"}
# The statements are a Python literal in the source, not a widget, so `{{` in them needs no escaping.
LIFECYCLE_SQL = """# Databricks notebook source
import json
import string

{read_params}
__pytest_statements = {statements}
for __pytest_statement in __pytest_statements:
    spark.sql(string.Template(__pytest_statement).substitute(__pytest_job_params))
dbutils.notebook.exit(json.dumps({{"statements": len(__pytest_statements)}}))
"""
NOTEBOOK_MARKER = "# Databricks notebook source"
CELL_SEPARATOR = "# COMMAND ----------"
DEBUG_HINT = (
    "# Edit the widgets above, then run all cells. dbutils.jobs.taskValues.get() raises outside a job\n"
    "# unless the notebook passes debugValue=."
)
CUT_WIDGETS_HINT = (
    f"# These widgets hold only the first {WIDGET_LIMIT_BYTES} bytes of their value: {{names}}. "
    "The notebook reads the\n"
    "# truncated values; the full values are in the parameters dict in the next cell."
)
LONG_VALUES_HINT = (
    f"# Values longer than the {WIDGET_LIMIT_BYTES} bytes a text widget holds, in full. "
    "Their widgets hold a truncated copy, which is\n"
    "# what the notebook reads."
)
LifecycleStep = Callable[..., object] | Sequence[str] | Path


def upload(ws: WorkspaceClient, folder: str) -> dict[str, str]:
    """Upload the helper notebooks, creating the folder first.

    Args:
        ws: The workspace client.
        folder: The workspace folder for the notebooks.

    Returns:
        The notebooks' workspace paths by name.
    """
    folder = folder.rstrip("/")
    ws.workspace.mkdirs(folder)
    paths: dict[str, str] = {}
    for name, source in NOTEBOOKS.items():
        path = f"{folder}/{name}"
        _upload_python(ws, path, source)
        paths[name] = path
    return paths


def lifecycle_source(step: LifecycleStep, rootpath: Path, argument: str) -> tuple[str, frozenset[str]]:
    """Build the notebook for a `setup=` or `teardown=` step, and name the job parameters it reads.

    A function becomes a notebook that calls it with the arguments it declares and exits with what it returns. A
    list of SQL statements becomes a notebook that renders each with `string.Template` from the job parameters,
    runs them in order through `spark.sql` and exits with `{"statements": <count>}`. A `Path` to a `.py` file is
    read, relative to `rootpath`, and becomes the notebook as it is, with the notebook marker added when it lacks it.

    Args:
        step: The function, statements or file given as `setup=` or `teardown=`.
        rootpath: The pytest rootdir, against which a relative path is resolved.
        argument: `"setup"` or `"teardown"`, for error messages.

    Returns:
        The notebook's source, and the `${name}` placeholders of the statements. Empty for a function or a file,
        whose reads cannot be known before they run.

    Raises:
        ValueError: If `step` is a string or an empty list, a statement is not a string or has a `$` that is neither a
            placeholder nor `$$`, the file does not exist or is not a `.py` file, or the function is not a plain
            function defined with `def`, uses variables of an enclosing function, takes a parameter other than
            `spark`, `dbutils` and `params`, has no source to read, or has a decorator.
    """
    if isinstance(step, Path):
        return _file_source(step, rootpath, argument), frozenset()
    if isinstance(step, str):
        raise ValueError(
            f"{argument}= takes a list of SQL statements, not a string: pass [{step!r}], or a Path to a .py file."
        )
    if isinstance(step, Sequence):
        return _sql_source(step, argument)
    return _function_source(step, argument), frozenset()


def check_lifecycle_step(step: LifecycleStep, argument: str) -> None:
    """Run the checks of `lifecycle_source` that need no rootdir, so that a bad `Lakeflow` default fails early.

    Raises:
        ValueError: In the cases where `lifecycle_source` raises it, except that a file is not looked for.
    """
    if isinstance(step, Path):
        _check_file_suffix(step, argument)
        return
    lifecycle_source(step, Path(), argument)


def _file_source(path: Path, rootpath: Path, argument: str) -> str:
    _check_file_suffix(path, argument)
    resolved = rootpath / path
    if not resolved.is_file():
        raise ValueError(f"{argument}= file {resolved} does not exist. A relative path starts at {rootpath}.")
    text = resolved.read_text(encoding="utf-8")
    if text.startswith(NOTEBOOK_MARKER):
        return text
    return f"{NOTEBOOK_MARKER}\n{text}"


def _check_file_suffix(path: Path, argument: str) -> None:
    if path.suffix != ".py":
        raise ValueError(
            f"{argument}= must be a .py file, got {path}. Put SQL statements in a list, or run them from a .py file "
            "through spark.sql."
        )


def _sql_source(statements: Sequence[object], argument: str) -> tuple[str, frozenset[str]]:
    if not statements:
        raise ValueError(
            f"{argument}= is an empty list of SQL statements. Pass at least one statement, or leave {argument}= out."
        )
    names: set[str] = set()
    for number, statement in enumerate(statements, start=1):
        if not isinstance(statement, str):
            raise ValueError(f"{argument}= statement {number} is not a string: {statement!r}")
        template = string.Template(statement)
        if not template.is_valid():
            raise ValueError(
                f"{argument}= statement {number} has an invalid placeholder: {statement!r}. Write ${{name}} for a job "
                "parameter and $$ for a literal dollar."
            )
        names.update(template.get_identifiers())
    source = LIFECYCLE_SQL.format(read_params=READ_PARAMS, statements=repr(list(statements)))
    return source, frozenset(names)


def _function_source(fn: Callable[..., object], argument: str) -> str:
    """Build a notebook that calls `fn` with the arguments it declares and exits with what it returns."""
    if not inspect.isfunction(fn):
        raise ValueError(
            f"{argument}= must be a plain function defined with def, a list of SQL statements or a Path to a .py "
            f"file, got {fn!r}. A function runs as a notebook on the job's compute, so the plugin ships its source."
        )
    name = fn.__name__
    if name == "<lambda>":
        raise ValueError(
            f"{argument}= cannot be a lambda, because the plugin ships the function's source to the job's compute. "
            "Define it with def instead."
        )
    if fn.__code__.co_freevars:
        raise ValueError(
            f"{argument}={name} uses variables of the function that encloses it: {sorted(fn.__code__.co_freevars)}. "
            "It runs on the job's compute, where they do not exist. Pass the values as job parameters and read them "
            "through params instead."
        )
    declared = _declared_arguments(fn, f"{argument}={name}")
    try:
        source = textwrap.dedent(inspect.getsource(fn)).strip()
    except (OSError, TypeError) as e:
        raise ValueError(
            f"Cannot read the source of {argument}={name}: {e}. Define it in a file, not in a REPL or notebook cell, "
            "so the plugin can ship its source."
        ) from None
    if source.startswith("@"):
        raise ValueError(
            f"{argument}={name} has a decorator, which would be shipped with its source to the job's compute, where "
            "it does not exist. Remove the decorator."
        )
    call = f"{name}({', '.join(f'{arg}={LIFECYCLE_VALUES[arg]}' for arg in declared)})"
    return LIFECYCLE.format(source=source, read_params=READ_PARAMS, call=call)


def _declared_arguments(fn: Callable[..., object], label: str) -> list[str]:
    declared: list[str] = []
    refused: list[str] = []
    for parameter in inspect.signature(fn).parameters.values():
        if parameter.name in LIFECYCLE_VALUES and parameter.kind in ARGUMENT_KINDS:
            declared.append(parameter.name)
        else:
            refused.append(str(parameter))
    if refused:
        raise ValueError(
            f"{label} takes {refused}, but it can only take spark, dbutils and params. Remove the others and pass "
            "values as job parameters, which it reads through params."
        )
    return declared


def upload_lifecycle(ws: WorkspaceClient, folder: str, role: str, source: str, uploaded: set[str]) -> str:
    """Upload a setup or teardown notebook under a name made from its content, unless it is in `uploaded`.

    Args:
        ws: The workspace client.
        folder: The helper notebooks' folder, which exists already.
        role: `"setup"` or `"teardown"`, which starts the notebook's name.
        source: The notebook's source.
        uploaded: The paths uploaded so far in this session. The new path is added.

    Returns:
        The notebook's workspace path, `<folder>/<role>_<12 hex digits of its SHA-256>`.
    """
    digest = hashlib.sha256(source.encode()).hexdigest()[:NOTEBOOK_DIGEST_CHARS]
    path = f"{folder.rstrip('/')}/{role}_{digest}"
    if path not in uploaded:
        _upload_python(ws, path, source)
        uploaded.add(path)
    return path


def query_file_path(folder: str, run_id: str, task_key: str) -> str:
    """Name the SQL file that inlines the saved query of task `task_key` for one job of the run `run_id`.

    Args:
        folder: The helper notebooks' folder.
        run_id: The id of the test session, with each character other than a letter, digit, `_` or `-` as `_`.
        task_key: The key of the task that runs the saved query.

    Returns:
        `<folder>/query_<run id>_<task key>_<8 random hex digits>.sql`, unique to the job, so that deleting it with
        the job leaves the files of other jobs alone.
    """
    run = re.sub(r"[^A-Za-z0-9_-]", "_", run_id)
    return f"{folder.rstrip('/')}/query_{run}_{task_key}_{secrets.token_hex(4)}.sql"


def upload_query_file(ws: WorkspaceClient, path: str, source: str) -> None:
    """Upload the SQL file of an inlined saved query as a workspace file, replacing any at `path`.

    Args:
        ws: The workspace client.
        path: The file's workspace path, in a folder that exists.
        source: The SQL text.
    """
    ws.workspace.upload(path, source.encode(), format=ImportFormat.RAW, overwrite=True)


def debug_notebook_source(notebook_path: str, parameters: Mapping[str, str], *, heading: str) -> str:
    """Build a notebook that sets `parameters` as text widgets, then runs `notebook_path` with `%run`.

    Widgets set in the caller reach the notebook that `%run` runs. Each name and value is written with `json.dumps`,
    which gives a Python string literal for any text, so a value holding `{{`, quotes or newlines arrives unchanged.
    A value longer than a text widget takes is written in full to a `parameters` dict, and its widget gets the
    longest prefix that fits. A first cell of comments then names those parameters.

    Args:
        notebook_path: The workspace path of the task's notebook.
        parameters: The task's resolved parameters, in the order the platform returned them.
        heading: A comment line that says where the parameters came from.

    Returns:
        The notebook's source, in the SOURCE format.
    """
    cut = cut_parameters(parameters)
    lines = [NOTEBOOK_MARKER]
    if cut:
        names = ", ".join(json.dumps(key) for key in cut)
        lines += [CUT_WIDGETS_HINT.format(names=names), "", CELL_SEPARATOR, ""]
    lines += [f"# {heading}", DEBUG_HINT]
    if cut:
        long_values = {key: parameters[key] for key in cut}
        lines += [LONG_VALUES_HINT, f"parameters = {json.dumps(long_values)}"]
    for key, value in parameters.items():
        lines.append(f"dbutils.widgets.text({json.dumps(key)}, {json.dumps(_widget_value(value))})")
    lines += ["", CELL_SEPARATOR, "", f"# MAGIC %run {notebook_path}", ""]
    return "\n".join(lines)


def cut_parameters(parameters: Mapping[str, str]) -> list[str]:
    """The keys of the values in `parameters` that are longer than a text widget takes, in their order."""
    cut = []
    for key, value in parameters.items():
        if len(value.encode()) > WIDGET_LIMIT_BYTES:
            cut.append(key)
    return cut


def _widget_value(value: str) -> str:
    """The longest prefix of `value` that a text widget takes, cut at a character boundary."""
    return value.encode()[:WIDGET_LIMIT_BYTES].decode(errors="ignore")


def upload_notebook(ws: WorkspaceClient, path: str, source: str) -> str:
    """Upload a Python notebook, replacing any at `path`, and creating its folder first.

    Args:
        ws: The workspace client.
        path: The notebook's workspace path.
        source: The notebook's source, in the SOURCE format.

    Returns:
        The notebook's page in the workspace, `<host>/#notebook/<object id>`.
    """
    ws.workspace.mkdirs(posixpath.dirname(path))
    _upload_python(ws, path, source)
    # The documented notebook URL names the notebook by its object id, which the upload does not return.
    object_id = ws.workspace.get_status(path).object_id
    return f"{ws.config.host}/#notebook/{object_id}"


def _upload_python(ws: WorkspaceClient, path: str, source: str) -> None:
    """Upload a Python notebook in the SOURCE format, replacing any at `path`, into a folder that exists."""
    ws.workspace.upload(path, source.encode(), format=ImportFormat.SOURCE, language=Language.PYTHON, overwrite=True)
