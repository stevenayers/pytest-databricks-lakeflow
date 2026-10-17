from __future__ import annotations

import json
import logging
import os
import secrets
import time
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from filelock import FileLock

from pytest_databricks_lakeflow import bundle, helpers
from pytest_databricks_lakeflow.errors import LakeflowError
from pytest_databricks_lakeflow.options import Options

log = logging.getLogger("pytest_databricks_lakeflow")

RUN_ID_VARIABLES = ("LAKEFLOW_RUN_ID", "GITHUB_RUN_ID", "BUILD_BUILDID")
STATE_FILE = "lakeflow_state.json"
CLIENT_ATTEMPTS = 4


def make_workspace_client(profile: str | None) -> WorkspaceClient:
    """Build a workspace client, retrying when several processes refresh a databricks-cli token at once.

    Args:
        profile: The config profile, or `None` for the SDK's standard resolution.

    Returns:
        The client.
    """
    for attempt in range(1, CLIENT_ATTEMPTS):
        try:
            return WorkspaceClient(profile=profile)
        except ValueError as e:
            if "cannot get access token" not in str(e):
                raise
            log.info("Retrying workspace client after: %s", e)
            time.sleep(2**attempt)
    return WorkspaceClient(profile=profile)


def resolve_run_id() -> str:
    for name in RUN_ID_VARIABLES:
        if value := os.environ.get(name):
            return value
    return secrets.token_hex(4)


def shared_state(state_dir: Path | None, produce: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    """Call `produce` once per session, even under xdist.

    The first worker to take the lock produces the state and writes it to a file, and the others read it. A setup
    error is shared the same way.

    Args:
        state_dir: A directory shared by the session's xdist workers, or `None` without xdist.
        produce: Builds the state. It must be JSON-serialisable.

    Returns:
        The state.

    Raises:
        LakeflowError: If setup failed in another worker.
    """
    if state_dir is None:
        return produce()
    path = state_dir / STATE_FILE
    with FileLock(f"{path}.lock"):
        if path.is_file():
            state: dict[str, Any] = json.loads(path.read_text())
        else:
            try:
                state = produce()
            except Exception as e:
                path.write_text(json.dumps({"error": f"{type(e).__name__}: {e}"}))
                raise
            path.write_text(json.dumps(state))
    if "error" in state:
        raise LakeflowError(f"Lakeflow session setup failed in another xdist worker: {state['error']}")
    return state


def setup(ws: WorkspaceClient, options: Options, rootpath: Path) -> dict[str, Any]:
    """Prepare the bundle, load job templates and upload the helper notebooks.

    Args:
        ws: The workspace client.
        options: The plugin's options.
        rootpath: Where to start looking for the bundle root when `lakeflow_bundle_root` is not set.

    Returns:
        The session state, which is JSON-serialisable so that xdist workers can share it.

    Raises:
        LakeflowBundleError: If the bundle cannot be found, or a CLI command fails.
    """
    root = bundle.find_root(rootpath) if options.bundle_root is None else bundle.require_root(options.bundle_root)
    bundle.check_cli_version(root)
    if options.sync == "deploy":
        bundle.deploy(root, options.target, options.profile)
        templates, errors = templates_from_summary(ws, bundle.summary(root, options.target, options.profile))
    else:
        if options.sync == "sync":
            bundle.sync(root, options.target, options.profile)
        config = bundle.validate(root, options.target, options.profile)
        templates, errors = bundle.templates_from_validate(config, root, options.sync, lambda path: exists(ws, path))
    folder = options.helpers_path or helpers.default_folder(ws)
    state = {
        "run_id": resolve_run_id(),
        "bundle_root": str(root),
        "templates": templates,
        "template_errors": errors,
        "helpers": helpers.upload(ws, folder),
    }
    log.info("Loaded %d job templates from %s; helpers in %s", len(templates), root, folder)
    return state


def templates_from_summary(
    ws: WorkspaceClient, bundle_summary: Mapping[str, Any]
) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    """Load deployed job settings with `jobs.get`, so every path is a resolved workspace path.

    Args:
        ws: The workspace client.
        bundle_summary: The parsed output of `databricks bundle summary -o json`.

    Returns:
        The settings by resource key, and an error for each job that is not deployed or no longer exists.
    """
    ids, errors = bundle.job_ids(bundle_summary)

    def fetch(job_id: int) -> dict[str, Any] | None:
        try:
            settings = ws.jobs.get(job_id).settings
        except NotFound:
            return None
        return settings.as_dict() if settings else {}

    templates: dict[str, dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        for (key, job_id), settings in zip(ids.items(), pool.map(fetch, ids.values()), strict=True):
            if settings is None:
                errors[key] = (
                    f"Job {key!r} (id {job_id}) does not exist in the workspace, so the bundle state is stale. "
                    "Run `databricks bundle deploy` again."
                )
            else:
                templates[key] = settings
    return templates, errors


def exists(ws: WorkspaceClient, path: str) -> bool:
    try:
        if path.startswith("/Volumes/"):
            ws.files.get_metadata(path)
        else:
            ws.workspace.get_status(path)
    except NotFound:
        return False
    return True
