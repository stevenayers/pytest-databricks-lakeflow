"""Set up a bundle target once per session.

Deploy or validate it, check the host and `run_as`, load the job templates and upload the helper notebooks.
"""

from __future__ import annotations

import logging
import posixpath
import urllib.parse
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

from pytest_databricks_lakeflow import bundle, helpers
from pytest_databricks_lakeflow.errors import LakeflowBundleError, LakeflowError
from pytest_databricks_lakeflow.naming import default_folder
from pytest_databricks_lakeflow.results.build import fetch_each
from pytest_databricks_lakeflow.workspace_api import GRANT_ROLE, check_sp_user_role, exists, modified_at

if TYPE_CHECKING:
    from pytest_databricks_lakeflow.session import BundleKey

log = logging.getLogger("pytest_databricks_lakeflow")
RUN_AS_FIX = (
    "Deploy as that principal, set run_as to ${workspace.current_user.userName}, or deploy the bundle yourself and "
    'use Lakeflow(sync="none").'
)


def setup(ws: WorkspaceClient, key: BundleKey) -> dict[str, Any]:
    """Prepare the bundle, load job templates and upload the helper notebooks.

    Args:
        ws: The workspace client.
        key: The bundle target.

    Returns:
        The target's state, which is JSON-serialisable so that xdist workers can share it.

    Raises:
        LakeflowBundleError: If a CLI command fails.
        LakeflowError: If the bundle target deploys to a different workspace from the one the client is for.
    """
    root = key.root
    variables = dict(key.variables)
    bundle.check_cli_version(root)
    if key.sync == "deploy":
        validated = bundle.validate(root, key.target, key.profile, variables)
        # Checked before anything reaches the workspace: the run_as check calls it, and a deploy writes to it.
        check_host(key.target, _workspace_host(validated), ws.config.host)
        check_run_as(ws, validated, key.target)
        bundle.deploy(root, key.target, key.profile, variables, flags=key.deploy_flags)
        bundle_summary = bundle.summary(root, key.target, key.profile, variables)
        # Checked again, for a validate output that gave no host.
        check_host(key.target, _workspace_host(bundle_summary), ws.config.host)
        templates, errors = templates_from_summary(ws, bundle_summary)
        job_ids, _ = bundle.job_ids(bundle_summary)
        pipeline_ids, pipeline_errors = bundle.pipeline_ids(bundle_summary)
        unversioned = bundle.unversioned_wheel_artifacts(bundle_summary)
        artifact_path = bundle.artifact_path_of(bundle_summary)
        stale: list[str] = []
    else:
        if key.sync == "sync":
            bundle.sync(root, key.target, key.profile, variables)
        config = bundle.validate(root, key.target, key.profile, variables)
        check_host(key.target, _workspace_host(config), ws.config.host)
        templates, errors = bundle.templates_from_validate(config, root, key.sync, lambda path: exists(ws, path))
        job_ids = {}
        # Validate leaves `${resources.pipelines.<key>.id}` unresolved, and `Lakeflow.pipeline()` needs deploy.
        pipeline_ids = {}
        pipeline_errors = {}
        unversioned = bundle.unversioned_wheel_artifacts(config)
        artifact_path = bundle.artifact_path_of(config)
        stale = []
        if key.sync == "none":
            stale = bundle.stale_uploads(config, root, lambda path: modified_at(ws, path))
    user = ws.current_user.me().user_name
    folder = key.helpers_path or default_folder(user)
    helper_paths = helpers.upload(ws, folder)
    log.info(f"Loaded {len(templates)} job templates from {root}; helpers in {folder}")
    return {
        "templates": templates,
        "template_errors": errors,
        "helpers": helper_paths,
        "user": user,
        "job_ids": job_ids,
        "pipeline_ids": pipeline_ids,
        "pipeline_errors": pipeline_errors,
        "unversioned_artifacts": unversioned,
        "artifact_path": artifact_path,
        "root": str(root),
        "stale_uploads": stale,
    }


def folder_of_helpers(state: Mapping[str, Any]) -> str:
    """The workspace folder of the helper notebooks, from a bundle target's state."""
    return posixpath.dirname(state["helpers"]["set_values"])


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

    def fetch(entry: tuple[str, int]) -> dict[str, Any] | str:
        key, job_id = entry
        try:
            settings = ws.jobs.get(job_id).settings
        except NotFound:
            return (
                f"Job {key!r} (id {job_id}) does not exist in the workspace, so the bundle state is stale. "
                "Run `databricks bundle deploy` again."
            )
        return settings.as_dict() if settings else {}

    templates: dict[str, dict[str, Any]] = {}
    for (key, _), fetched in fetch_each(list(ids.items()), fetch):
        if isinstance(fetched, str):
            errors[key] = fetched
        else:
            templates[key] = fetched
    return templates, errors


def check_run_as(ws: WorkspaceClient, config: Mapping[str, Any], target: str) -> None:
    """Check that the identity deploying the bundle can deploy the run_as of its target and of each of its jobs.

    A user other than the deploying identity is refused, and so is a service principal on which the deploying
    identity does not have the Service Principal User role. A service principal whose role cannot be read is let
    through, so `bundle deploy` reports the problem if there is one.

    Args:
        ws: The workspace client, which acts as the identity the CLI deploys as.
        config: The parsed output of `databricks bundle validate -o json`.
        target: The bundle target.

    Raises:
        LakeflowBundleError: If a run_as cannot be deployed by this identity.
    """
    sources = _run_as_sources(config, target)
    if not sources:
        return
    identity = ws.current_user.me().user_name or ""
    for source, run_as in sources:
        user_name = run_as.get("user_name")
        application_id = run_as.get("service_principal_name")
        if user_name and user_name.casefold() != identity.casefold():
            raise LakeflowBundleError(
                f"{source} sets run_as {user_name}, but this session deploys as {identity}. {RUN_AS_FIX}"
            )
        if application_id and check_sp_user_role(ws, application_id) is False:
            raise LakeflowBundleError(
                f"{source} sets run_as {application_id}, but this session deploys as {identity}, which does not have "
                f"the Service Principal User role on it. {GRANT_ROLE}. {RUN_AS_FIX}"
            )


def _run_as_sources(config: Mapping[str, Any], target: str) -> list[tuple[str, Mapping[str, str]]]:
    """The run_as of the target and of each job that sets a different one, each with what sets it."""
    # `bundle validate` copies the target's run_as into every job that sets none.
    target_run_as = config.get("run_as") or {}
    sources: list[tuple[str, Mapping[str, str]]] = []
    if target_run_as:
        sources.append((f"Target {target!r}", target_run_as))
    for key, job in sorted(((config.get("resources") or {}).get("jobs") or {}).items()):
        run_as = job.get("run_as") or {}
        if run_as and run_as != target_run_as:
            sources.append((f"Job {key!r} of target {target!r}", run_as))
    return sources


def _workspace_host(config: Mapping[str, Any]) -> str | None:
    # `bundle summary` and `bundle validate` can print "workspace": null rather than an object.
    workspace = config.get("workspace") or {}
    return workspace.get("host")


def check_host(target: str, bundle_host: str | None, client_host: str | None) -> None:
    """Check that the CLI deploys the bundle target to the workspace that the client is for.

    Hosts are compared without their scheme, path or case. A target or client without a host is not checked.

    Args:
        target: The bundle target.
        bundle_host: The target's `workspace.host`, as the CLI resolved it.
        client_host: The workspace client's host.

    Raises:
        LakeflowError: If the hosts differ.
    """
    if not bundle_host or not client_host:
        return
    if _netloc(bundle_host) == _netloc(client_host):
        return
    raise LakeflowError(
        f"Bundle target {target!r} deploys to {bundle_host} but the workspace client is for {client_host}. "
        "Pass profile= to Lakeflow, or set DATABRICKS_HOST and DATABRICKS_CONFIG_PROFILE to match the target."
    )


def _netloc(host: str) -> str:
    return urllib.parse.urlparse(host if "://" in host else f"https://{host}").netloc.lower()
