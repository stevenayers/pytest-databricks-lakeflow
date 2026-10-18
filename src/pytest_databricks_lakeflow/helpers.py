from __future__ import annotations

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language

# Widget names carry the injected-task prefix because every job parameter is also pushed to every notebook task's
# widgets, overriding base_parameters of the same name.
VALUES_WIDGET = "__pytest_values"
TASK_KEY_WIDGET = "__pytest_task_key"
KEYS_WIDGET = "__pytest_keys"
MISSING = "__pytest_missing__"

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
    paths = {name: f"{folder}/{name}" for name in NOTEBOOKS}
    for name, source in NOTEBOOKS.items():
        ws.workspace.upload(
            paths[name], source.encode(), format=ImportFormat.SOURCE, language=Language.PYTHON, overwrite=True
        )
    return paths
