from __future__ import annotations

import hashlib
import inspect
import string
import textwrap
from collections.abc import Callable, Sequence
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language

from pytest_databricks_lakeflow.errors import LakeflowError
from pytest_databricks_lakeflow.results import PREFIX

# Widget names carry the injected-task prefix because every job parameter is also pushed to every notebook task's
# widgets, overriding base_parameters of the same name.
VALUES_WIDGET = "__pytest_values"
TASK_KEY_WIDGET = "__pytest_task_key"
KEYS_WIDGET = "__pytest_keys"
PARAMS_WIDGET = "__pytest_params"
MISSING = "__pytest_missing__"
ARGUMENT_KINDS = (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
NOTEBOOK_DIGEST_CHARS = 12

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

LifecycleStep = Callable[..., object] | Sequence[str] | Path


def default_folder(user: str | None) -> str:
    return f"/Users/{user}/.pytest-databricks-lakeflow"


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
        ws.workspace.upload(path, source.encode(), format=ImportFormat.SOURCE, language=Language.PYTHON, overwrite=True)
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
        LakeflowError: If `step` is a string or an empty list, a statement is not a string or has a `$` that is
            neither a placeholder nor `$$`, the file does not exist or is not a `.py` file, or the function is not a
            plain function defined with `def`, uses variables of an enclosing function, takes a parameter other than
            `spark`, `dbutils` and `params`, has no source to read, or has a decorator.
    """
    if isinstance(step, Path):
        return _file_source(step, rootpath, argument), frozenset()
    if isinstance(step, str):
        raise LakeflowError(
            f"{argument}= takes a list of SQL statements, not a string: pass [{step!r}], or a Path to a .py file."
        )
    if isinstance(step, Sequence):
        return _sql_source(step, argument)
    return _function_source(step, argument), frozenset()


def _file_source(path: Path, rootpath: Path, argument: str) -> str:
    if path.suffix != ".py":
        raise LakeflowError(
            f"{argument}= must be a .py file, got {path}. Put SQL statements in a list, or run them from a .py file "
            "through spark.sql."
        )
    resolved = rootpath / path
    if not resolved.is_file():
        raise LakeflowError(f"{argument}= file {resolved} does not exist. A relative path starts at {rootpath}.")
    text = resolved.read_text(encoding="utf-8")
    if text.startswith(NOTEBOOK_MARKER):
        return text
    return f"{NOTEBOOK_MARKER}\n{text}"


def _sql_source(statements: Sequence[object], argument: str) -> tuple[str, frozenset[str]]:
    if not statements:
        raise LakeflowError(
            f"{argument}= is an empty list of SQL statements. Pass at least one statement, or leave {argument}= out."
        )
    names: set[str] = set()
    for number, statement in enumerate(statements, start=1):
        if not isinstance(statement, str):
            raise LakeflowError(f"{argument}= statement {number} is not a string: {statement!r}")
        template = string.Template(statement)
        if not template.is_valid():
            raise LakeflowError(
                f"{argument}= statement {number} has an invalid placeholder: {statement!r}. Write ${{name}} for a job "
                "parameter and $$ for a literal dollar."
            )
        names.update(template.get_identifiers())
    source = LIFECYCLE_SQL.format(read_params=READ_PARAMS, statements=repr(list(statements)))
    return source, frozenset(names)


def _function_source(fn: Callable[..., object], argument: str) -> str:
    """Build a notebook that calls `fn` with the arguments it declares and exits with what it returns."""
    if not inspect.isfunction(fn):
        raise LakeflowError(
            f"{argument}= must be a plain function defined with def, a list of SQL statements or a Path to a .py "
            f"file, got {fn!r}. A function runs as a notebook on the job's compute, so the plugin ships its source."
        )
    name = fn.__name__
    if name == "<lambda>":
        raise LakeflowError(
            f"{argument}= cannot be a lambda, because the plugin ships the function's source to the job's compute. "
            "Define it with def instead."
        )
    if fn.__code__.co_freevars:
        raise LakeflowError(
            f"{argument}={name} uses variables of the function that encloses it: {sorted(fn.__code__.co_freevars)}. "
            "It runs on the job's compute, where they do not exist. Pass the values as job parameters and read them "
            "through params instead."
        )
    declared = _declared_arguments(fn, f"{argument}={name}")
    try:
        source = textwrap.dedent(inspect.getsource(fn)).strip()
    except (OSError, TypeError) as e:
        raise LakeflowError(
            f"Cannot read the source of {argument}={name}: {e}. Define it in a file, not in a REPL or notebook cell, "
            "so the plugin can ship its source."
        ) from None
    if source.startswith("@"):
        raise LakeflowError(
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
        raise LakeflowError(
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
        ws.workspace.upload(path, source.encode(), format=ImportFormat.SOURCE, language=Language.PYTHON, overwrite=True)
        uploaded.add(path)
    return path
