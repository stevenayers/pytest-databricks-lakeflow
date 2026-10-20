"""Workspace operations shared by the setup, the ephemeral copies and the sweep: the client, lookups, cancellations."""

from __future__ import annotations

import inspect
import logging
import random
import time
from email.utils import parsedate_to_datetime

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import BadRequest, NotFound, PermissionDenied
from databricks.sdk.errors.base import DatabricksError
from databricks.sdk.service import iam, pipelines

log = logging.getLogger("pytest_databricks_lakeflow")
CLIENT_ATTEMPTS = 4
SP_USER_ROLE = "roles/servicePrincipal.user"
GRANT_ROLE = "Ask a manager of that service principal to grant you the role"
# `cascade` came in databricks-sdk 0.110.0 and `force` in 0.80.0.
PIPELINE_DELETE_PARAMETERS = frozenset(inspect.signature(pipelines.PipelinesAPI.delete).parameters)


def make_workspace_client(profile: str | None) -> WorkspaceClient:
    """Build a workspace client, retrying when several processes refresh a databricks-cli token at once.

    Args:
        profile: The config profile, or `None` for the SDK's standard resolution.

    Returns:
        The client.
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            return WorkspaceClient(profile=profile)
        except ValueError as e:
            if "cannot get access token" not in str(e) or attempt == CLIENT_ATTEMPTS:
                raise
            log.info(f"Retrying workspace client after: {e}")
            # Every worker whose forced refresh collided would retry at the same moment and collide again: the CLI
            # fails a forced refresh with "cache update: exit status 45" when another process updates its token cache
            # at the same time (six of eight concurrent `databricks auth token --force-refresh` calls failed, #140).
            time.sleep(2**attempt + random.uniform(0, 2**attempt))


def check_sp_user_role(ws: WorkspaceClient, application_id: str) -> bool | None:
    """Check whether the identity of the workspace client may make jobs that run as a service principal.

    That needs the Service Principal User role, granted to the identity itself or to one of its groups. Workspace
    admins need it too, and the Service Principal Manager role does not include it.

    Args:
        ws: The workspace client.
        application_id: The service principal's application id, as `run_as.service_principal_name` takes it.

    Returns:
        Whether the identity has the role, `True` when it is that service principal, or `None` when the client has
            no account id or the service principal's rule set cannot be read.
    """
    me = ws.current_user.me()
    if application_id == me.user_name:
        return True
    account_id = ws.config.account_id
    if not account_id:
        return None
    # Rule set names take the application id; the workspace's numeric service principal id is a BadRequest, and so
    # is an application id the account does not know.
    name = f"accounts/{account_id}/servicePrincipals/{application_id}/ruleSets/default"
    try:
        rule_set = ws.account_access_control_proxy.get_rule_set(name=name, etag="")
    except (BadRequest, NotFound, PermissionDenied) as e:
        log.debug(f"Could not read the rule set of service principal {application_id}: {e}")
        return None
    principals = _principals_of(me)
    for rule in rule_set.grant_rules or []:
        if rule.role != SP_USER_ROLE:
            continue
        for principal in rule.principals or []:
            if _principal_key(principal) in principals:
                return True
    return False


def _principals_of(me: iam.User) -> set[str]:
    """The names under which a rule set can grant a role to this identity: itself and each of its groups."""
    user_name = me.user_name or ""
    principals: set[str] = set()
    if "@" in user_name:
        principals.add(_principal_key(f"users/{user_name}"))
    else:
        # A service principal's user name is its application id.
        principals.add(f"servicePrincipals/{user_name}")
    for group in me.groups or []:
        if group.display:
            principals.add(f"groups/{group.display}")
    return principals


def _principal_key(principal: str) -> str:
    """The principal as `_principals_of` compares it: a user's name without case, as `check_run_as` compares it."""
    if principal.startswith("users/"):
        return principal.casefold()
    return principal


def exists(ws: WorkspaceClient, path: str) -> bool:
    try:
        _modified_at(ws, path)
    except NotFound:
        return False
    return True


def modified_at(ws: WorkspaceClient, path: str) -> int | None:
    """Get the time a workspace or Volumes file was last modified.

    Args:
        ws: The workspace client.
        path: A `/Workspace` or `/Volumes` path.

    Returns:
        The time in epoch milliseconds, or `None` if the path does not exist or the API reports no time.
    """
    try:
        return _modified_at(ws, path)
    except NotFound:
        return None


def _modified_at(ws: WorkspaceClient, path: str) -> int | None:
    if not path.startswith("/Volumes/"):
        return ws.workspace.get_status(path).modified_at
    # The Files API reports the time only as an HTTP date in its last-modified header.
    last_modified = ws.files.get_metadata(path).last_modified
    if last_modified is None:
        return None
    return int(parsedate_to_datetime(last_modified).timestamp() * 1000)


def delete_pipeline(ws: WorkspaceClient, pipeline_id: str) -> None:
    """Delete a pipeline and the tables it publishes.

    Deleting a pipeline that publishes to Unity Catalog keeps its tables unless the request sets `cascade`, whatever
    the SDK's docstring says: a materialised view was still there 27 seconds after a delete without it, and gone
    3 seconds after one with `cascade=True`. `force` lets the delete go ahead when cleaning up the tables fails. Each
    is passed when the installed databricks-sdk takes it.

    Args:
        ws: The workspace client.
        pipeline_id: The pipeline's id.
    """
    flags: dict[str, bool] = {}
    for name in ("cascade", "force"):
        if name in PIPELINE_DELETE_PARAMETERS:
            flags[name] = True
    ws.pipelines.delete(pipeline_id, **flags)


def cancel_run(ws: WorkspaceClient, run_id: int) -> None:
    try:
        ws.jobs.cancel_run(run_id)
        log.info(f"Cancelled run {run_id}")
    except DatabricksError as e:
        log.warning(f"Could not cancel run {run_id}: {e}")


def stop_update(ws: WorkspaceClient, pipeline_id: str) -> None:
    try:
        ws.pipelines.stop(pipeline_id)
        log.info(f"Stopped the update of pipeline {pipeline_id}")
    except DatabricksError as e:
        log.warning(f"Could not stop the update of pipeline {pipeline_id}: {e}")
