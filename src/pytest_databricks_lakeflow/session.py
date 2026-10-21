from __future__ import annotations

import hashlib
import json
import logging
import os
import secrets
import subprocess
import threading
import urllib.parse
import warnings
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from databricks.sdk import WorkspaceClient
from filelock import FileLock

from pytest_databricks_lakeflow.deployment import folder_of_helpers, setup
from pytest_databricks_lakeflow.errors import LakeflowError
from pytest_databricks_lakeflow.naming import EXPIRES_FORMAT, EXPIRES_TAG
from pytest_databricks_lakeflow.sweep import SweepOptions
from pytest_databricks_lakeflow.sweep import sweep as run_sweep

if TYPE_CHECKING:
    from pytest_databricks_lakeflow.client import Lakeflow
    from pytest_databricks_lakeflow.ephemeral.job import EphemeralJob
    from pytest_databricks_lakeflow.results.models import JobRun

log = logging.getLogger("pytest_databricks_lakeflow")
RUN_ID_VARIABLES = ("LAKEFLOW_RUN_ID", "GITHUB_RUN_ID", "BUILD_BUILDID")
GITHUB_URL_VARIABLES = ("GITHUB_SERVER_URL", "GITHUB_REPOSITORY", "GITHUB_RUN_ID")
AZURE_DEVOPS_URL_VARIABLES = ("SYSTEM_TEAMFOUNDATIONCOLLECTIONURI", "SYSTEM_TEAMPROJECT", "BUILD_BUILDID")
SHA_VARIABLES = ("GITHUB_SHA", "BUILD_SOURCEVERSION")
BRANCH_VARIABLES = ("GITHUB_HEAD_REF", "GITHUB_REF_NAME", "BUILD_SOURCEBRANCHNAME")
USER_VARIABLES = ("GITHUB_ACTOR", "BUILD_REQUESTEDFOREMAIL")
GIT_TIMEOUT_SECONDS = 10
SWEEP_STATE_FILE = "lakeflow_sweep.json"


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
        performance_target: The value of `--lakeflow-performance-target`, or `None`.
        once_records: The outcome of each `Lakeflow.once()` step this process has asked for, by the step's digest:
            the record of its job, or the error it failed with.
        once_results: The setup run of each `once()` step read in this process, by run id.
        once_jobs: The records of the `once()` jobs this process set up, torn down at session end. Under xdist the
            workers report theirs to the controller, which tears them all down.
        once_clients: The workspace client that made each `once()` job in this process, by job id.
        slots: The cap on the runs this process has active, made by the first run under a cap, or `None`.
        slots_lock: Held while `run_slots` makes or reads `slots`, so threads starting their first capped runs at
            once share one cap.
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
    performance_target: str | None = None
    once_records: dict[str, dict[str, Any] | Exception] = field(default_factory=dict)
    once_results: dict[int, JobRun] = field(default_factory=dict)
    once_jobs: list[dict[str, Any]] = field(default_factory=list)
    once_clients: dict[int, WorkspaceClient] = field(default_factory=dict)
    slots: RunSlots | None = None
    slots_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

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
        with self.slots_lock:
            if self.slots is None:
                self.slots = RunSlots(cap)
                return self.slots
            if cap != self.slots.cap and cap not in self.slots.ignored:
                self.slots.ignored.add(cap)
                log.warning(
                    f"max_concurrent_runs={cap} differs from the cap of {self.slots.cap} that this process already "
                    f"uses, which every Lakeflow in the process shares, so it is ignored"
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
                report = run_sweep(ws, self.run_id, sweep_hours, folder, self.sweep_options, user=state.get("user"))
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
        log.debug(f"git {' '.join(args)} failed: {e}")
        return None
    return result.stdout.strip() or None
