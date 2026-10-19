from __future__ import annotations

import hashlib
import inspect
import textwrap
from collections.abc import Callable

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language

from pytest_databricks_lakeflow.errors import LakeflowError

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

# The source of `fn`, then a call that passes the arguments it declares. Job parameters are read from the widgets
# named in the `__pytest_params` widget, because the platform pushes every job parameter to every notebook task.
LIFECYCLE = """# Databricks notebook source
import json

{source}

__pytest_names = json.loads(dbutils.widgets.get("{widget}"))
__pytest_job_params = {{name: dbutils.widgets.get(name) for name in __pytest_names}}
__pytest_result = {call}
if __pytest_result is not None:
    dbutils.notebook.exit(json.dumps(__pytest_result, default=str))
"""
LIFECYCLE_VALUES = {"spark": "spark", "dbutils": "dbutils", "params": "__pytest_job_params"}


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


def lifecycle_notebook(fn: Callable[..., object], argument: str) -> str:
    """Build a notebook that calls `fn` with the arguments it declares and exits with what it returns.

    Args:
        fn: The function given as `setup=` or `teardown=`.
        argument: `"setup"` or `"teardown"`, for error messages.

    Returns:
        The notebook's source.

    Raises:
        LakeflowError: If `fn` is not a plain function defined with `def`, uses variables of an enclosing
            function, takes a parameter other than `spark`, `dbutils` and `params`, or has no source to read.
    """
    if not inspect.isfunction(fn):
        raise LakeflowError(
            f"{argument}= must be a plain function defined with def, got {fn!r}. It runs as a notebook on the job's "
            "compute, so the plugin ships its source."
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
    call = f"{name}({', '.join(f'{arg}={LIFECYCLE_VALUES[arg]}' for arg in declared)})"
    return LIFECYCLE.format(source=source, widget=PARAMS_WIDGET, call=call)


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
