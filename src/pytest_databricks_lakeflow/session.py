from __future__ import annotations

import hashlib
import inspect
import json
import logging
import os
import posixpath
import random
import re
import secrets
import subprocess
import threading
import time
import urllib.parse
import warnings
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, TypeVar, get_args

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import BadRequest, NotFound, PermissionDenied
from databricks.sdk.errors.base import DatabricksError
from databricks.sdk.service import iam, jobs, pipelines, workspace
from filelock import FileLock

from pytest_databricks_lakeflow import bundle, helpers
from pytest_databricks_lakeflow.errors import LakeflowBundleError, LakeflowError
from pytest_databricks_lakeflow.settings import NAME_PREFIX, tag_value

if TYPE_CHECKING:
    from pytest_databricks_lakeflow.client import Lakeflow
    from pytest_databricks_lakeflow.ephemeral import EphemeralJob

log = logging.getLogger("pytest_databricks_lakeflow")

RUN_ID_VARIABLES = ("LAKEFLOW_RUN_ID", "GITHUB_RUN_ID", "BUILD_BUILDID")
GITHUB_URL_VARIABLES = ("GITHUB_SERVER_URL", "GITHUB_REPOSITORY", "GITHUB_RUN_ID")
AZURE_DEVOPS_URL_VARIABLES = ("SYSTEM_TEAMFOUNDATIONCOLLECTIONURI", "SYSTEM_TEAMPROJECT", "BUILD_BUILDID")
SHA_VARIABLES = ("GITHUB_SHA", "BUILD_SOURCEVERSION")
BRANCH_VARIABLES = ("GITHUB_HEAD_REF", "GITHUB_REF_NAME", "BUILD_SOURCEBRANCHNAME")
USER_VARIABLES = ("GITHUB_ACTOR", "BUILD_REQUESTEDFOREMAIL")
GIT_TIMEOUT_SECONDS = 10
CLIENT_ATTEMPTS = 4
HOUR_MS = 3_600_000
SweepScope = Literal["mine", "all"]
SWEEP_SCOPES: tuple[str, ...] = get_args(SweepScope)
SWEEP_REPORT_VERSION = 1
SWEEP_STATE_FILE = "lakeflow_sweep.json"
EXPIRES_TAG = "pytest_expires"
EXPIRES_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
LIFECYCLE_NOTEBOOK = re.compile(rf"(setup|teardown)_[0-9a-f]{{{helpers.NOTEBOOK_DIGEST_CHARS}}}")
# `<job_key>__<task_key>__<run id>`, as `EphemeralJob.debug_notebook` names it in the helpers folder's `debug` folder.
DEBUG_NOTEBOOK = re.compile(r".+__.+__(?P<run_id>[0-9]+)")
USERS_FOLDER = "/Users"
FOLDER_LISTING_THREADS = 16
SCHEMA_SUFFIX = "[0-9a-f]{8}"
SP_USER_ROLE = "roles/servicePrincipal.user"
GRANT_ROLE = "Ask a manager of that service principal to grant you the role"
RUN_AS_FIX = (
    "Deploy as that principal, set run_as to ${workspace.current_user.userName}, or deploy the bundle yourself and "
    'use Lakeflow(sync="none").'
)
PYTEST_SCHEMA = re.compile(rf"pytest_[a-z0-9_]+_{SCHEMA_SUFFIX}")
# list_pipelines filters only by notebook path or by `name LIKE '<pattern>'`, in which the brackets of the prefix are
# literal. Its listing carries no tags and no creation time, so each pipeline it finds is read with `get`.
PIPELINE_FILTER = f"name LIKE '{NAME_PREFIX}%'"
# `cascade` came in databricks-sdk 0.110.0 and `force` in 0.80.0.
PIPELINE_DELETE_PARAMETERS = frozenset(inspect.signature(pipelines.PipelinesAPI.delete).parameters)
T = TypeVar("T")


@dataclass(frozen=True)
class SweepOptions:
    """What `--lakeflow-sweep` deletes besides this user's old ephemeral jobs and pipeline copies.

    Attributes:
        scope: `mine` for jobs, pipelines and schemas this user created, `all` for anyone's.
        schemas_catalog: A catalog whose old `pytest_<run id>_<hex>` schemas from other runs are dropped too.
        notebooks: Also delete old `setup_<hash>`, `teardown_<hash>` and debug notebooks from the helpers folder, or
            with scope `all` from every user's default helpers folder too, unless a job the sweep keeps uses them.
        branch: Delete only jobs, pipelines and schemas whose `pytest_git_branch` tag or property is this branch, and
            only the notebooks that those jobs alone use.
        dry_run: Delete nothing, and report what would be deleted.
        report: A file to write the report to, as JSON.
    """

    scope: SweepScope = "mine"
    schemas_catalog: str | None = None
    notebooks: bool = False
    branch: str | None = None
    dry_run: bool = False
    report: Path | None = None


DEFAULT_SWEEP_OPTIONS = SweepOptions()


@dataclass(frozen=True)
class SweepReport:
    """What a sweep deleted, or with `dry_run` would have deleted, as `Lakeflow.sweep()` returns it.

    Attributes:
        dry_run: Whether the sweep only listed what it would delete.
        jobs: The ids of the jobs.
        pipelines: The ids of the pipelines.
        schemas: The full names of the schemas.
        notebooks: The workspace paths of the notebooks.
        errors: What could not be listed or deleted.
    """

    dry_run: bool
    jobs: tuple[int, ...] = ()
    pipelines: tuple[str, ...] = ()
    schemas: tuple[str, ...] = ()
    notebooks: tuple[str, ...] = ()
    errors: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        """Get the report as `--lakeflow-sweep-report` writes it, with a `version` key."""
        return {
            "version": SWEEP_REPORT_VERSION,
            "dry_run": self.dry_run,
            "jobs": list(self.jobs),
            "pipelines": list(self.pipelines),
            "schemas": list(self.schemas),
            "notebooks": list(self.notebooks),
            "errors": list(self.errors),
        }


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
            self.deploy_flags,
        ]
        digest = hashlib.sha256(json.dumps(fields).encode()).hexdigest()
        return f"lakeflow_{digest[:16]}.json"


class RunSlots:
    """The cap on the runs that one process has active at once.

    It comes from `--lakeflow-max-concurrent-runs` or `max_concurrent_runs`. Each xdist worker has its own.

    Attributes:
        cap: How many runs may be active at once.
        semaphore: One slot per run, taken before `run_now`.
        holders: The job of each run that holds a slot, by run id.
        ignored: Caps that other `Lakeflow` instances asked for and that have been warned about.
    """

    def __init__(self, cap: int) -> None:
        self.cap = cap
        self.semaphore = threading.BoundedSemaphore(cap)
        self.holders: dict[int, EphemeralJob] = {}
        self.ignored: set[int] = set()

    def hold(self, run_id: int, job: EphemeralJob) -> None:
        """Record that run `run_id` of `job` holds the slot taken for it."""
        self.holders[run_id] = job

    def release(self, run_id: int) -> None:
        """Give back the slot of run `run_id`, once, however often the run is forgotten."""
        if self.holders.pop(run_id, None) is not None:
            self.semaphore.release()


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
        sweep_options: What the sweep covers, from the `--lakeflow-sweep-*` flags.
        keep_ttl: How long the `pytest_expires` tag protects each new job from the sweep, from
            `--lakeflow-keep-ttl`, or `None`.
        destroy: Whether `--lakeflow-destroy` is set.
        item: The test that is running, or `None` between tests.
        setups: The outcome of setting up each bundle target: its state, or the error it failed with.
        lakeflows: Every `Lakeflow` used in this session.
        deployed: The root, target, profile and variables of each bundle target that this session deployed.
        swept: Whether this process has swept leftover jobs.
        sweep_dry_run: After a sweep dry run, in the process that ran it and on the xdist controller, a line for each
            job, pipeline, schema and notebook it would delete and for each error. `None` without a dry run.
        kept_jobs: The URL of each ephemeral job that was kept, for the terminal summary.
        kept_pipelines: The URL of each pipeline copy that was kept.
        kept_schemas: The full name of each temporary schema that was kept.
        cancelled_runs: The number of runs cancelled at session end.
        warned_artifacts: The bundle targets whose wheel artifacts without `dynamic_version` have been warned about.
        warned_uploads: The bundle targets whose local libraries built after their uploaded copies have been warned
            about.
        worker_dir: On the xdist controller, the directory that it shares with the workers.
        uploaded_notebooks: The workspace paths of the setup and teardown notebooks this process uploaded.
        report_dir: Where `--lakeflow-report-dir` writes each test's runs, resolved against the invocation
            directory, or `None` when the flag is not set.
        debug_notebook: Whether `--lakeflow-debug-notebook` is set.
        debug_notebooks: One line per debug notebook written, its URL with the test, the task and any widgets cut
            short, for the terminal summary.
        sp_user_roles: Whether an identity has the Service Principal User role on a service principal, as
            `check_sp_user_role` found it, by workspace host, identity and application id.
        live: Whether `--lakeflow-live` is set.
        max_concurrent_runs: The value of `--lakeflow-max-concurrent-runs`, or `None`.
        slots: The cap on the runs this process has active, made by the first run under a cap, or `None`.
    """

    rootpath: Path
    run_id: str
    attribution: dict[str, str] = field(default_factory=dict)
    state_dir: Path | None = None
    keep: bool = False
    keep_failed: bool | None = None
    sweep: bool = False
    sweep_options: SweepOptions = field(default_factory=SweepOptions)
    keep_ttl: timedelta | None = None
    destroy: bool = False
    item: pytest.Item | None = None
    setups: dict[BundleKey, dict[str, Any] | Exception] = field(default_factory=dict)
    lakeflows: list[Lakeflow] = field(default_factory=list)
    deployed: list[tuple[str, str, str | None, tuple[tuple[str, str], ...]]] = field(default_factory=list)
    swept: bool = False
    sweep_dry_run: list[str] | None = None
    kept_jobs: list[str] = field(default_factory=list)
    kept_pipelines: list[str] = field(default_factory=list)
    kept_schemas: list[str] = field(default_factory=list)
    cancelled_runs: int = 0
    warned_artifacts: set[BundleKey] = field(default_factory=set)
    warned_uploads: set[BundleKey] = field(default_factory=set)
    worker_dir: Path | None = None
    uploaded_notebooks: set[str] = field(default_factory=set)
    report_dir: Path | None = None
    debug_notebook: bool = False
    debug_notebooks: list[str] = field(default_factory=list)
    sp_user_roles: dict[tuple[str, str, str], bool | None] = field(default_factory=dict)
    live: bool = False
    max_concurrent_runs: int | None = None
    slots: RunSlots | None = None

    def run_slots(self, cap: int | None) -> RunSlots | None:
        """Get the cap on this process's active runs, making it on first use. The command-line flag wins over `cap`.

        Every `Lakeflow` in the process shares one cap, sized by the first run started under one. A later `Lakeflow`
        that asks for a different cap gets the same one, with a warning.

        Args:
            cap: `Lakeflow`'s `max_concurrent_runs`.

        Returns:
            The cap, or `None` when neither the flag nor `cap` sets one.
        """
        if self.max_concurrent_runs is not None:
            cap = self.max_concurrent_runs
        if cap is None:
            return None
        if self.slots is None:
            self.slots = RunSlots(cap)
            return self.slots
        if cap != self.slots.cap and cap not in self.slots.ignored:
            self.slots.ignored.add(cap)
            log.warning(
                "max_concurrent_runs=%s differs from the cap of %s that this process already uses, which every "
                "Lakeflow in the process shares, so it is ignored",
                cap,
                self.slots.cap,
            )
        return self.slots

    def keeps(self, keep: bool, keep_failed: bool, failed: bool) -> bool:
        """Decide whether to keep a job, pipeline or schema. The command-line flags win over `Lakeflow`'s arguments.

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

        With `--lakeflow-sweep`, the first setup in the session also sweeps, once for all xdist workers.

        Args:
            key: The bundle target.
            ws: The workspace client.
            sweep_hours: The age after which leftover jobs are swept.

        Returns:
            The target's state: its job templates, the errors of jobs that cannot be used, the job ids by bundle key,
                the pipeline ids by bundle key and the errors of pipelines that are not deployed, the helper paths,
                the workspace user, the wheel artifacts without `dynamic_version`, the bundle root and artifact path,
                and the local libraries built after their uploaded copies.

        Raises:
            LakeflowError: If setting it up failed earlier in this session.
        """
        if key not in self.setups:
            self._set_up(key, ws, sweep_hours)
        state = self.setups[key]
        if isinstance(state, Exception):
            raise LakeflowError(
                f"Setting up {key.root} for target {key.target!r} failed earlier in this session: "
                f"{type(state).__name__}: {state}"
            )
        return state

    def _set_up(self, key: BundleKey, ws: WorkspaceClient, sweep_hours: float) -> None:
        path = None if self.state_dir is None else self.state_dir / key.file_name()

        try:
            state = shared_state(path, lambda: setup(ws, key))
        except Exception as e:
            self.setups[key] = e
            raise
        self.setups[key] = state
        if key.sync == "deploy":
            self.deployed.append((str(key.root), key.target, key.profile, key.variables))
        if self.sweep and not self.swept:
            self.swept = True
            self._sweep(ws, state, sweep_hours)

    def _sweep(self, ws: WorkspaceClient, state: dict[str, Any], sweep_hours: float) -> None:
        path = None if self.state_dir is None else self.state_dir / SWEEP_STATE_FILE
        swept_here = False

        def produce() -> dict[str, Any]:
            nonlocal swept_here
            swept_here = True
            folder = folder_of_helpers(state)
            try:
                report = sweep(ws, self.run_id, sweep_hours, folder, self.sweep_options, user=state.get("user"))
            # The sweep only tidies up, so its failure fails no test. It is not shared as an error, so the other
            # xdist workers carry on without it.
            except Exception as e:
                warnings.warn(f"Lakeflow sweep failed: {type(e).__name__}: {e}", stacklevel=1)
                return {"failed": True}
            return report.as_dict()

        report = shared_state(path, produce, "sweep")
        if swept_here and report.get("dry_run"):
            self.sweep_dry_run = _dry_run_lines(report, ws.config.host)

    def expiry_tag(self) -> dict[str, str]:
        """The `pytest_expires` tag for a new job, `keep_ttl` from now in UTC, or no tag without `keep_ttl`."""
        if self.keep_ttl is None:
            return {}
        return {EXPIRES_TAG: (datetime.now(UTC) + self.keep_ttl).strftime(EXPIRES_FORMAT)}


def _dry_run_lines(report: Mapping[str, Any], host: str | None) -> list[str]:
    lines: list[str] = []
    for job_id in report["jobs"]:
        lines.append(f"job {host}/jobs/{job_id}")
    for pipeline_id in report["pipelines"]:
        lines.append(f"pipeline {host}/pipelines/{pipeline_id}")
    for name in report["schemas"]:
        lines.append(f"schema {name}")
    for path in report["notebooks"]:
        lines.append(f"notebook {path}")
    lines.extend(report["errors"])
    return lines


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
    attempt = 0
    while True:
        attempt += 1
        try:
            return WorkspaceClient(profile=profile)
        except ValueError as e:
            if "cannot get access token" not in str(e) or attempt == CLIENT_ATTEMPTS:
                raise
            log.info("Retrying workspace client after: %s", e)
            # Every worker whose forced refresh collided would retry at the same moment and collide again: the CLI
            # fails a forced refresh with "cache update: exit status 45" when another process updates its token cache
            # at the same time (six of eight concurrent `databricks auth token --force-refresh` calls failed, #140).
            time.sleep(2**attempt + random.uniform(0, 2**attempt))


def resolve_run_id() -> str:
    return _first_set(RUN_ID_VARIABLES) or secrets.token_hex(4)


def resolve_attribution(rootpath: Path) -> dict[str, str]:
    """Find out which CI run, commit, branch and person a session belongs to, for tagging ephemeral jobs.

    Reads the GitHub Actions and Azure DevOps variables, and falls back to `git` in `rootpath` for the commit and
    branch. Errors from `git`, including a missing `git`, are ignored, and a detached HEAD has no branch.

    Args:
        rootpath: The pytest rootdir.

    Returns:
        The `pytest_ci_url`, `pytest_git_sha`, `pytest_git_branch` and `pytest_user` tags that have a value.
    """
    attribution = {
        "pytest_ci_url": _ci_url(),
        "pytest_git_sha": _first_set(SHA_VARIABLES) or _git(rootpath, "rev-parse", "HEAD"),
        "pytest_git_branch": _first_set(BRANCH_VARIABLES) or _git_branch(rootpath),
        "pytest_user": _first_set(USER_VARIABLES),
    }
    return {key: value for key, value in attribution.items() if value}


def _ci_url() -> str | None:
    env = os.environ
    if all(env.get(name) for name in GITHUB_URL_VARIABLES):
        server, repository, run_id = (env[name] for name in GITHUB_URL_VARIABLES)
        return f"{server.rstrip('/')}/{repository}/actions/runs/{run_id}"
    if all(env.get(name) for name in AZURE_DEVOPS_URL_VARIABLES):
        collection, project, build_id = (env[name] for name in AZURE_DEVOPS_URL_VARIABLES)
        return f"{collection}{urllib.parse.quote(project)}/_build/results?buildId={build_id}"
    return None


def _first_set(names: tuple[str, ...]) -> str | None:
    """The value of the first of `names` that is set to a non-empty string in the environment."""
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    return None


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


def schema_prefix(run_id: str) -> str:
    """Start the name of a temporary schema of the run `run_id`: `pytest_<run id>_`, lower case, `_` for the rest."""
    return f"pytest_{re.sub(r'[^a-z0-9_]', '_', run_id.lower())}_"


def folder_of_helpers(state: Mapping[str, Any]) -> str:
    """The workspace folder of the helper notebooks, from a bundle target's state."""
    return posixpath.dirname(state["helpers"]["set_values"])


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


def shared_state(
    path: Path | None, produce: Callable[[], dict[str, Any]], what: str = "session setup"
) -> dict[str, Any]:
    """Call `produce` once, even under xdist.

    The first worker to take the lock produces the state and writes it to `path`, and the others read it. An error
    from `produce` is shared the same way.

    Args:
        path: A file in a directory shared by the session's xdist workers, or `None` without xdist.
        produce: Builds the state. It must be JSON-serialisable.
        what: What `produce` does, for the error the other workers raise.

    Returns:
        The state.

    Raises:
        LakeflowError: If `produce` failed in another worker.
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
        raise LakeflowError(f"Lakeflow {what} failed in another xdist worker: {state['error']}")
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
    folder = key.helpers_path or helpers.default_folder(user)
    helper_paths = helpers.upload(ws, folder)
    log.info("Loaded %d job templates from %s; helpers in %s", len(templates), root, folder)
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
        log.debug("Could not read the rule set of service principal %s: %s", application_id, e)
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


def sweep(
    ws: WorkspaceClient,
    run_id: str | None,
    hours: float,
    helpers_folder: str,
    options: SweepOptions = DEFAULT_SWEEP_OPTIONS,
    *,
    user: str | None = None,
) -> SweepReport:
    """Delete ephemeral jobs and pipeline copies of other runs older than `hours`, and what else `options` asks for.

    A job, pipeline or schema tagged `pytest_expires` is old once that time has passed, whatever its age. A pipeline
    reports no creation time, so its last change counts as its age. A notebook is deleted only when every job that
    uses it is deleted too, and with `options.branch` only when at least one is; a debug notebook is used by the job of
    the run it replays, while that job is listed. When the jobs cannot all be listed, no notebook is deleted. A failed
    delete or listing is logged and recorded in the report's `errors`, and the sweep carries on.

    Args:
        ws: The workspace client.
        run_id: The id of this run, whose jobs and schemas are kept, or `None` to keep no run's.
        hours: The age after which a job, pipeline, schema or notebook is deleted.
        helpers_folder: The folder of the helper notebooks, for `options.notebooks`.
        options: What else to sweep, and whether only to report it.
        user: The workspace user whose jobs, pipelines and schemas are swept with scope `mine`. Looked up when not
            given.

    Returns:
        What was deleted, or with `options.dry_run` what would have been. Written to `options.report` when set.
    """
    me = None
    if options.scope == "mine":
        me = user or ws.current_user.me().user_name
    now_ms = time.time() * 1000
    cutoff_ms = now_ms - hours * HOUR_MS
    errors: list[str] = []
    rule = _SweepRule(run_id=run_id, user=me, cutoff_ms=cutoff_ms, now_ms=now_ms, branch=options.branch)
    # Each listing finishes before the first delete, so deleting does not disturb the paging.
    listed_jobs, all_listed = _list_jobs(ws, options.notebooks, errors)
    stale_jobs = _stale_jobs(listed_jobs, rule)
    deleted_jobs = _delete_each("job", stale_jobs, ws.jobs.delete, options.dry_run, errors)
    stale_pipelines = _stale_pipelines(ws, rule, errors)
    drop_pipeline = partial(delete_pipeline, ws)
    deleted_pipelines = _delete_each("pipeline", stale_pipelines, drop_pipeline, options.dry_run, errors)
    deleted_schemas: list[str] = []
    if options.schemas_catalog:
        stale = _stale_schemas(ws, options.schemas_catalog, rule, errors)
        drop = partial(ws.schemas.delete, force=True)
        deleted_schemas = _delete_each("schema", stale, drop, options.dry_run, errors)
    deleted_notebooks: list[str] = []
    if options.notebooks and all_listed:
        folders = _helpers_folders(ws, helpers_folder, options.scope, errors)
        candidates = _old_notebooks(ws, folders, cutoff_ms, errors)
        users = _notebook_users(ws, listed_jobs, candidates, errors)
        stale = _unused_notebooks(users, set(deleted_jobs), options.branch)
        deleted_notebooks = _delete_each("notebook", stale, ws.workspace.delete, options.dry_run, errors)
    report = SweepReport(
        dry_run=options.dry_run,
        jobs=tuple(deleted_jobs),
        pipelines=tuple(deleted_pipelines),
        schemas=tuple(deleted_schemas),
        notebooks=tuple(deleted_notebooks),
        errors=tuple(errors),
    )
    if options.dry_run:
        log.info(
            "Sweep dry run: would delete %d jobs, %d pipelines, %d schemas and %d notebooks",
            len(deleted_jobs),
            len(deleted_pipelines),
            len(deleted_schemas),
            len(deleted_notebooks),
        )
    else:
        log.info("Swept %d leftover ephemeral jobs and %d pipeline copies", len(deleted_jobs), len(deleted_pipelines))
        if options.schemas_catalog or options.notebooks:
            log.info("Swept %d leftover schemas and %d notebooks", len(deleted_schemas), len(deleted_notebooks))
    if options.report is not None:
        options.report.parent.mkdir(parents=True, exist_ok=True)
        options.report.write_text(json.dumps(report.as_dict(), indent=2) + "\n")
    return report


@dataclass(frozen=True)
class _SweepRule:
    """Which tagged jobs, pipelines and schemas a sweep deletes.

    Those of runs other than `run_id` unless it is `None`, created by `user` unless it is `None`, tagged with `branch`
    unless it is `None`, and past their `pytest_expires` time or, without one, created or last changed before the
    cutoff.
    """

    run_id: str | None
    user: str | None
    cutoff_ms: float
    now_ms: float
    branch: str | None

    def matches(self, creator: str | None, tags: Mapping[str, str], age_from_ms: int | None) -> bool:
        if "pytest" not in tags or "pytest_run_id" not in tags:
            return False
        if tags["pytest_run_id"] == self.run_id:
            return False
        return self.selects(creator, tags, age_from_ms)

    def selects(self, creator: str | None, tags: Mapping[str, str], age_from_ms: int | None) -> bool:
        """Apply the creator, branch and age rules to anything `pytest` made, by its tags or schema properties."""
        if self.user is not None and creator != self.user:
            return False
        if self.branch is not None and tags.get("pytest_git_branch") != tag_value(self.branch):
            return False
        expiry_ms = _expiry_ms(tags.get(EXPIRES_TAG))
        if expiry_ms is not None:
            return expiry_ms < self.now_ms
        return (age_from_ms or 0) < self.cutoff_ms


def _list_jobs(ws: WorkspaceClient, expand_tasks: bool, errors: list[str]) -> tuple[list[jobs.BaseJob], bool]:
    """List every job, with its tasks when `expand_tasks` is set, and whether the listing finished."""
    listed: list[jobs.BaseJob] = []
    try:
        for job in ws.jobs.list(limit=100, expand_tasks=expand_tasks):
            listed.append(job)
    except DatabricksError as e:
        _record(errors, f"Could not list the jobs: {e}")
        return listed, False
    return listed, True


def _stale_jobs(listed: list[jobs.BaseJob], rule: _SweepRule) -> list[int]:
    stale: list[int] = []
    for job in listed:
        tags = (job.settings.tags if job.settings else None) or {}
        if job.job_id is not None and rule.matches(job.creator_user_name, tags, job.created_time):
            stale.append(job.job_id)
    return stale


def _stale_pipelines(ws: WorkspaceClient, rule: _SweepRule, errors: list[str]) -> list[str]:
    """The pipeline copies that `rule` matches, by the tags and the last change that `pipelines.get` reports."""
    candidates: list[str] = []
    try:
        for info in ws.pipelines.list_pipelines(filter=PIPELINE_FILTER):
            if info.pipeline_id is None:
                continue
            if rule.user is not None and info.creator_user_name != rule.user:
                continue
            candidates.append(info.pipeline_id)
    except DatabricksError as e:
        _record(errors, f"Could not list the pipelines: {e}")
    stale: list[str] = []
    for pipeline_id in candidates:
        try:
            pipeline = ws.pipelines.get(pipeline_id)
        except NotFound:
            continue
        except DatabricksError as e:
            _record(errors, f"Could not read pipeline {pipeline_id}: {e}")
            continue
        tags = (pipeline.spec.tags if pipeline.spec else None) or {}
        if rule.matches(pipeline.creator_user_name, tags, pipeline.last_modified):
            stale.append(pipeline_id)
    return stale


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


def _expiry_ms(value: str | None) -> float | None:
    """Read a `pytest_expires` tag. A missing or unreadable tag gives `None`, and the job's age decides."""
    if not value:
        return None
    try:
        expiry = datetime.fromisoformat(value)
    except ValueError:
        log.warning("Ignoring the unreadable %s tag %r", EXPIRES_TAG, value)
        return None
    if expiry.tzinfo is None:
        expiry = expiry.replace(tzinfo=UTC)
    return expiry.timestamp() * 1000


def _stale_schemas(ws: WorkspaceClient, catalog: str, rule: _SweepRule, errors: list[str]) -> list[str]:
    """The temporary schemas in `catalog` of other runs that `rule` selects by their properties and creation time."""
    this_run = None
    if rule.run_id is not None:
        this_run = re.compile(re.escape(schema_prefix(rule.run_id)) + SCHEMA_SUFFIX)
    stale: list[str] = []
    try:
        for info in ws.schemas.list(catalog_name=catalog):
            name = info.name or ""
            if not PYTEST_SCHEMA.fullmatch(name) or not info.full_name:
                continue
            if this_run is not None and this_run.fullmatch(name):
                continue
            if rule.selects(info.created_by, info.properties or {}, info.created_at):
                stale.append(info.full_name)
    except DatabricksError as e:
        _record(errors, f"Could not list the schemas in {catalog}: {e}")
    return stale


def _helpers_folders(ws: WorkspaceClient, helpers_folder: str, scope: SweepScope, errors: list[str]) -> list[str]:
    """The helpers folders to sweep: `helpers_folder`, and with scope `all` every user's default one."""
    folders = [helpers_folder.rstrip("/")]
    if scope != "all":
        return folders
    try:
        for info in ws.workspace.list(USERS_FOLDER):
            if info.object_type != workspace.ObjectType.DIRECTORY or not info.path:
                continue
            folder = helpers.default_folder(posixpath.basename(info.path))
            if folder not in folders:
                folders.append(folder)
    except DatabricksError as e:
        _record(errors, f"Could not list the user folders in {USERS_FOLDER}: {e}")
    return folders


@dataclass(frozen=True)
class _OldNotebook:
    """A setup, teardown or debug notebook last changed before the cutoff. A debug notebook has its `run_id`."""

    path: str
    run_id: int | None = None


def _old_notebooks(ws: WorkspaceClient, folders: list[str], cutoff_ms: float, errors: list[str]) -> list[_OldNotebook]:
    """The old helper notebooks in each of `folders` and its `debug` folder, listed one folder per user in parallel.

    A folder that does not exist is skipped: most users have no helpers folder.
    """
    with ThreadPoolExecutor(min(FOLDER_LISTING_THREADS, len(folders))) as pool:
        listings = list(pool.map(partial(_old_notebooks_in, ws, cutoff_ms=cutoff_ms), folders))
    found: list[_OldNotebook] = []
    for notebooks, error in listings:
        found.extend(notebooks)
        if error is not None:
            _record(errors, error)
    return found


def _old_notebooks_in(ws: WorkspaceClient, folder: str, *, cutoff_ms: float) -> tuple[list[_OldNotebook], str | None]:
    found: list[_OldNotebook] = []
    debug_folder = f"{folder}/{helpers.DEBUG_FOLDER}"
    has_debug_folder = False
    try:
        for info in ws.workspace.list(folder):
            path = info.path or ""
            if info.object_type == workspace.ObjectType.DIRECTORY and path == debug_folder:
                has_debug_folder = True
            elif _is_old_notebook(info, cutoff_ms) and LIFECYCLE_NOTEBOOK.fullmatch(posixpath.basename(path)):
                found.append(_OldNotebook(path))
        if has_debug_folder:
            for info in ws.workspace.list(debug_folder):
                match = DEBUG_NOTEBOOK.fullmatch(posixpath.basename(info.path or ""))
                if match and _is_old_notebook(info, cutoff_ms):
                    found.append(_OldNotebook(info.path or "", int(match["run_id"])))
    except NotFound:
        return found, None
    except DatabricksError as e:
        return found, f"Could not list the notebooks in {folder}: {e}"
    return found, None


def _is_old_notebook(info: workspace.ObjectInfo, cutoff_ms: float) -> bool:
    return info.object_type == workspace.ObjectType.NOTEBOOK and (info.modified_at or 0) < cutoff_ms


def _notebook_users(
    ws: WorkspaceClient, listed: list[jobs.BaseJob], notebooks: list[_OldNotebook], errors: list[str]
) -> dict[str, set[int] | None]:
    """The jobs that use each notebook, by its path, or `None` when that could not be found out.

    A setup or teardown notebook is used by every job with a task that runs it. A debug notebook is used by the job
    of the run it replays while that job is listed. `runs/get` still names the job after it is deleted, so a run of a
    job deleted before this sweep, like a run that no longer exists, has no job.
    """
    listed_ids: set[int] = set()
    by_path: dict[str, set[int]] = {}
    for job in listed:
        if job.job_id is None:
            continue
        listed_ids.add(job.job_id)
        if job.settings is None:
            continue
        for path in _paths_used(job.settings.tasks or []):
            by_path.setdefault(path, set()).add(job.job_id)
    users: dict[str, set[int] | None] = {}
    for notebook in notebooks:
        if notebook.run_id is None:
            users[notebook.path] = by_path.get(_workspace_path(notebook.path), set())
            continue
        try:
            job_id = ws.jobs.get_run(notebook.run_id).job_id
        except NotFound:
            users[notebook.path] = set()
        except DatabricksError as e:
            _record(errors, f"Could not read run {notebook.run_id} of notebook {notebook.path}: {e}")
            users[notebook.path] = None
        else:
            if job_id in listed_ids:
                users[notebook.path] = {job_id}
            else:
                users[notebook.path] = set()
    return users


def _paths_used(tasks: list[jobs.Task]) -> list[str]:
    """The workspace paths of the notebooks and SQL files that `tasks` run, those nested in a for-each task included."""
    paths: list[str] = []
    for task in tasks:
        if task.for_each_task is not None and task.for_each_task.task is not None:
            paths.extend(_paths_used([task.for_each_task.task]))
        if task.notebook_task is not None and task.notebook_task.notebook_path:
            paths.append(_workspace_path(task.notebook_task.notebook_path))
        if task.sql_task is not None and task.sql_task.file is not None and task.sql_task.file.path:
            paths.append(_workspace_path(task.sql_task.file.path))
    return paths


def _workspace_path(path: str) -> str:
    """`path` without a leading `/Workspace`, which a task may name and `workspace.list` does not report."""
    if path.startswith("/Workspace/"):
        return path.removeprefix("/Workspace")
    return path


def _unused_notebooks(users: Mapping[str, set[int] | None], deleted_jobs: set[int], branch: str | None) -> list[str]:
    """The notebooks whose every job is deleted, and with `branch`, that at least one deleted job uses."""
    unused: list[str] = []
    for path, job_ids in users.items():
        if job_ids is None or not job_ids <= deleted_jobs:
            continue
        if branch is not None and not job_ids:
            continue
        unused.append(path)
    return unused


def _delete_each(kind: str, names: list[T], delete: Callable[[T], None], dry_run: bool, errors: list[str]) -> list[T]:
    """Delete each of `names`, or with `dry_run` only log them, and return those deleted or that would be."""
    if dry_run:
        for name in names:
            log.info("Would delete %s %s", kind, name)
        return list(names)
    deleted: list[T] = []
    for name in names:
        try:
            delete(name)
        except DatabricksError as e:
            _record(errors, f"Could not delete {kind} {name}: {e}")
        else:
            deleted.append(name)
    return deleted


def _record(errors: list[str], message: str) -> None:
    log.warning("%s", message)
    errors.append(message)
