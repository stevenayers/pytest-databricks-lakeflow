from __future__ import annotations

import hashlib
import json
import logging
import os
import secrets
import subprocess
import time
import urllib.parse
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.errors.base import DatabricksError
from databricks.sdk.service import jobs
from filelock import FileLock

from pytest_databricks_lakeflow import bundle, helpers
from pytest_databricks_lakeflow.errors import LakeflowError

if TYPE_CHECKING:
    from pytest_databricks_lakeflow.client import Lakeflow

log = logging.getLogger("pytest_databricks_lakeflow")

RUN_ID_VARIABLES = ("LAKEFLOW_RUN_ID", "GITHUB_RUN_ID", "BUILD_BUILDID")
GITHUB_URL_VARIABLES = ("GITHUB_SERVER_URL", "GITHUB_REPOSITORY", "GITHUB_RUN_ID")
AZURE_DEVOPS_URL_VARIABLES = ("SYSTEM_TEAMFOUNDATIONCOLLECTIONURI", "SYSTEM_TEAMPROJECT", "BUILD_BUILDID")
SHA_VARIABLES = ("GITHUB_SHA", "BUILD_SOURCEVERSION")
BRANCH_VARIABLES = ("GITHUB_HEAD_REF", "GITHUB_REF_NAME", "BUILD_SOURCEBRANCHNAME")
USER_VARIABLES = ("GITHUB_ACTOR", "BUILD_REQUESTEDFOREMAIL")
GIT_TIMEOUT_SECONDS = 10
CLIENT_ATTEMPTS = 4


@dataclass(frozen=True)
class BundleKey:
    """A bundle target that is set up once per session, with everything that changes how it is set up."""

    root: Path
    target: str
    profile: str | None
    sync: str
    helpers_path: str | None
    variables: tuple[tuple[str, str], ...] = ()
    deploy_flags: tuple[str, ...] = ()

    def file_name(self) -> str:
        """Name the file in which xdist workers share this target's setup."""
        fields = [
            str(self.root),
            self.target,
            self.profile,
            self.sync,
            self.helpers_path,
            self.variables,
            list(self.deploy_flags),
        ]
        return f"lakeflow_{hashlib.sha256(json.dumps(fields).encode()).hexdigest()[:16]}.json"


@dataclass(eq=False)
class SessionState:
    """What the `Lakeflow` instances of one pytest session share.

    Attributes:
        rootpath: The pytest rootdir, against which relative bundle roots are resolved.
        run_id: The id of this test session, the same in every xdist worker.
        attribution: The `pytest_ci_url`, `pytest_user`, `pytest_git_sha` and `pytest_git_branch` tags that could be
            resolved, the same in every xdist worker.
        state_dir: A directory shared by the session's xdist workers, or `None` without xdist.
        keep: Whether `--lakeflow-keep` is set.
        keep_failed: `True` for `--lakeflow-keep-failed`, `False` for `--no-lakeflow-keep-failed`, or `None`.
        sweep: Whether `--lakeflow-sweep` is set.
        destroy: Whether `--lakeflow-destroy` is set.
        item: The test that is running, or `None` between tests.
        setups: The outcome of setting up each bundle target: its state, or the error it failed with.
        lakeflows: Every `Lakeflow` used in this session.
        deployed: The root, target, profile and variables of each bundle target that this session deployed.
        swept: Whether this process has swept leftover jobs.
        kept_jobs: The URL of each ephemeral job that was kept, for the terminal summary.
        kept_schemas: The full name of each temporary schema that was kept.
        cancelled_runs: The number of runs cancelled at session end.
        worker_dir: On the xdist controller, the directory that it shares with the workers.
    """

    rootpath: Path
    run_id: str
    attribution: dict[str, str] = field(default_factory=dict)
    state_dir: Path | None = None
    keep: bool = False
    keep_failed: bool | None = None
    sweep: bool = False
    destroy: bool = False
    item: pytest.Item | None = None
    setups: dict[BundleKey, dict[str, Any] | Exception] = field(default_factory=dict)
    lakeflows: list[Lakeflow] = field(default_factory=list)
    deployed: list[tuple[str, str, str | None, tuple[tuple[str, str], ...]]] = field(default_factory=list)
    swept: bool = False
    kept_jobs: list[str] = field(default_factory=list)
    kept_schemas: list[str] = field(default_factory=list)
    cancelled_runs: int = 0
    worker_dir: Path | None = None

    def keeps(self, keep: bool, keep_failed: bool, failed: bool) -> bool:
        """Decide whether to keep a job or schema. The command-line flags win over `Lakeflow`'s arguments.

        Args:
            keep: `Lakeflow`'s `keep`.
            keep_failed: `Lakeflow`'s `keep_failed`.
            failed: Whether the test failed.

        Returns:
            Whether to keep it.
        """
        if self.keep_failed is not None:
            keep_failed = self.keep_failed
        return keep or self.keep or (failed and keep_failed)

    def bundle_state(self, key: BundleKey, ws: WorkspaceClient, sweep_hours: float) -> dict[str, Any]:
        """Set up a bundle target the first time it is used in this session, once for all xdist workers.

        The worker that sets it up also sweeps leftover jobs when `--lakeflow-sweep` is set.

        Args:
            key: The bundle target.
            ws: The workspace client.
            sweep_hours: The age after which leftover jobs are swept.

        Returns:
            The target's state: its job templates, the errors of jobs that cannot be used, the helper paths, and the
                workspace user.

        Raises:
            LakeflowError: If setting it up failed earlier in this session.
        """
        if key not in self.setups:
            path = None if self.state_dir is None else self.state_dir / key.file_name()
            produced = False

            def produce() -> dict[str, Any]:
                nonlocal produced
                produced = True
                return setup(ws, key)

            try:
                produced_state = shared_state(path, produce)
            except Exception as e:
                self.setups[key] = e
                raise
            self.setups[key] = produced_state
            if key.sync == "deploy":
                self.deployed.append((str(key.root), key.target, key.profile, key.variables))
            if produced and self.sweep and not self.swept:
                self.swept = True
                sweep(ws, self.run_id, sweep_hours, user=produced_state.get("user"))
        state = self.setups[key]
        if isinstance(state, Exception):
            raise LakeflowError(
                f"Setting up {key.root} for target {key.target!r} failed earlier in this session: "
                f"{type(state).__name__}: {state}"
            )
        return state


_sessions: list[SessionState] = []


def activate(state: SessionState) -> None:
    _sessions.append(state)


def deactivate(state: SessionState) -> None:
    _sessions[:] = [s for s in _sessions if s is not state]


def current() -> SessionState:
    if not _sessions:
        raise LakeflowError("Lakeflow works only in a pytest session with the pytest-databricks-lakeflow plugin")
    return _sessions[-1]


def current_item() -> pytest.Item:
    item = current().item
    if item is None:
        raise LakeflowError("Use Lakeflow inside a test, or in a fixture that a test uses, to create ephemeral jobs")
    return item


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


def resolve_attribution(rootpath: Path) -> dict[str, str]:
    """Find out which CI run, commit, branch and person a session belongs to, for tagging ephemeral jobs.

    Reads the GitHub Actions and Azure DevOps variables, and falls back to `git` in `rootpath` for the commit and
    branch. Errors from `git`, including a missing `git`, are ignored, and a detached HEAD has no branch.

    Args:
        rootpath: The pytest rootdir.

    Returns:
        The `pytest_ci_url`, `pytest_git_sha`, `pytest_git_branch` and `pytest_user` tags that have a value.
    """
    env = os.environ
    ci_url = None
    if all(env.get(name) for name in GITHUB_URL_VARIABLES):
        server, repository, run_id = (env[name] for name in GITHUB_URL_VARIABLES)
        ci_url = f"{server.rstrip('/')}/{repository}/actions/runs/{run_id}"
    elif all(env.get(name) for name in AZURE_DEVOPS_URL_VARIABLES):
        collection, project, build_id = (env[name] for name in AZURE_DEVOPS_URL_VARIABLES)
        ci_url = f"{collection}{urllib.parse.quote(project)}/_build/results?buildId={build_id}"
    attribution = {
        "pytest_ci_url": ci_url,
        "pytest_git_sha": _first(SHA_VARIABLES) or _git(rootpath, "rev-parse", "HEAD"),
        "pytest_git_branch": _first(BRANCH_VARIABLES) or _git_branch(rootpath),
        "pytest_user": _first(USER_VARIABLES),
    }
    return {key: value for key, value in attribution.items() if value}


def _first(names: tuple[str, ...]) -> str | None:
    return next((value for name in names if (value := os.environ.get(name))), None)


def _git_branch(cwd: Path) -> str | None:
    branch = _git(cwd, "rev-parse", "--abbrev-ref", "HEAD")
    return None if branch == "HEAD" else branch


def _git(cwd: Path, *args: str) -> str | None:
    try:
        result = subprocess.run(
            ["git", *args], cwd=cwd, capture_output=True, text=True, check=True, timeout=GIT_TIMEOUT_SECONDS
        )
    except (OSError, subprocess.SubprocessError) as e:
        log.debug("git %s failed: %s", " ".join(args), e)
        return None
    return result.stdout.strip() or None


def resolve_root(bundle_root: str | Path | None, rootpath: Path) -> Path:
    """Find the bundle root.

    Args:
        bundle_root: `Lakeflow`'s `bundle_root`, which is relative to the rootdir unless absolute.
        rootpath: The pytest rootdir.

    Returns:
        `bundle_root`, or without one the nearest directory with a databricks.yml, starting at the rootdir.

    Raises:
        LakeflowBundleError: If there is no databricks.yml.
    """
    if bundle_root is None:
        return bundle.find_root(rootpath)
    return bundle.require_root((rootpath / bundle_root).resolve())


def shared_state(path: Path | None, produce: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    """Call `produce` once, even under xdist.

    The first worker to take the lock produces the state and writes it to `path`, and the others read it. A setup
    error is shared the same way.

    Args:
        path: A file in a directory shared by the session's xdist workers, or `None` without xdist.
        produce: Builds the state. It must be JSON-serialisable.

    Returns:
        The state.

    Raises:
        LakeflowError: If setup failed in another worker.
    """
    if path is None:
        return produce()
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
        bundle.deploy(root, key.target, key.profile, variables, flags=key.deploy_flags)
        bundle_summary = bundle.summary(root, key.target, key.profile, variables)
        check_host(key.target, (bundle_summary.get("workspace") or {}).get("host"), ws.config.host)
        templates, errors = templates_from_summary(ws, bundle_summary)
    else:
        if key.sync == "sync":
            bundle.sync(root, key.target, key.profile, variables)
        config = bundle.validate(root, key.target, key.profile, variables)
        check_host(key.target, (config.get("workspace") or {}).get("host"), ws.config.host)
        templates, errors = bundle.templates_from_validate(config, root, key.sync, lambda path: exists(ws, path))
    user = ws.current_user.me().user_name
    folder = key.helpers_path or helpers.default_folder(user)
    state = {"templates": templates, "template_errors": errors, "helpers": helpers.upload(ws, folder), "user": user}
    log.info("Loaded %d job templates from %s; helpers in %s", len(templates), root, folder)
    return state


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
    if not bundle_host or not client_host or _netloc(bundle_host) == _netloc(client_host):
        return
    raise LakeflowError(
        f"Bundle target {target!r} deploys to {bundle_host} but the workspace client is for {client_host}. "
        "Pass profile= to Lakeflow, or set DATABRICKS_HOST and DATABRICKS_CONFIG_PROFILE to match the target."
    )


def _netloc(host: str) -> str:
    return urllib.parse.urlparse(host if "://" in host else f"https://{host}").netloc.lower()


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


def sweep(ws: WorkspaceClient, run_id: str, hours: float, user: str | None = None) -> list[int]:
    """Delete this user's ephemeral jobs from other runs that are older than `hours`.

    Args:
        ws: The workspace client.
        run_id: The id of this run, whose jobs are kept.
        hours: The age after which a job is deleted.
        user: The workspace user whose jobs are swept. Looked up when not given.

    Returns:
        The ids of the deleted jobs.
    """
    me = user or ws.current_user.me().user_name
    cutoff = (time.time() - hours * 3600) * 1000
    stale = [
        job.job_id
        for job in ws.jobs.list(limit=100)
        if job.job_id is not None and job.creator_user_name == me and _stale(job, run_id, cutoff)
    ]
    deleted = []
    for job_id in stale:
        try:
            ws.jobs.delete(job_id)
        except DatabricksError as e:
            log.warning("Could not delete job %s: %s", job_id, e)
        else:
            deleted.append(job_id)
    log.info("Swept %d leftover ephemeral jobs", len(deleted))
    return deleted


def _stale(job: jobs.BaseJob, run_id: str, cutoff_ms: float) -> bool:
    tags = (job.settings.tags if job.settings else None) or {}
    other_run = tags.get("pytest_run_id", run_id) != run_id
    return "pytest" in tags and other_run and (job.created_time or 0) < cutoff_ms
