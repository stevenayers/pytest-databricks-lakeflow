"""Delete the ephemeral jobs, pipeline copies, schemas and notebooks that earlier sessions left behind."""

from __future__ import annotations

import json
import logging
import posixpath
import re
import time
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from typing import Any, Literal, TypeVar, get_args

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.errors.base import DatabricksError
from databricks.sdk.service import jobs, workspace

from pytest_databricks_lakeflow.naming import (
    DEBUG_FOLDER,
    DEBUG_NOTEBOOK,
    EXPIRES_TAG,
    LIFECYCLE_NOTEBOOK,
    NAME_PREFIX,
    PYTEST_SCHEMA,
    QUERY_FILE,
    SCHEMA_SUFFIX,
    default_folder,
    schema_prefix,
    tag_value,
)
from pytest_databricks_lakeflow.workspace_api import delete_pipeline

log = logging.getLogger("pytest_databricks_lakeflow")
HOUR_MS = 3_600_000
SweepScope = Literal["mine", "all"]
SWEEP_SCOPES: tuple[str, ...] = get_args(SweepScope)
SWEEP_REPORT_VERSION = 1
USERS_FOLDER = "/Users"
FOLDER_LISTING_THREADS = 16
# list_pipelines filters only by notebook path or by `name LIKE '<pattern>'`, in which the brackets of the prefix are
# literal. Its listing carries no tags and no creation time, so each pipeline it finds is read with `get`.
PIPELINE_FILTER = f"name LIKE '{NAME_PREFIX}%'"
T = TypeVar("T")


@dataclass(frozen=True)
class SweepOptions:
    """What `--lakeflow-sweep` deletes besides this user's old ephemeral jobs and pipeline copies.

    Attributes:
        scope: `mine` for jobs, pipelines and schemas this user created, `all` for anyone's.
        schemas_catalog: A catalog whose old `pytest_<run id>_<hex>` schemas from other runs are dropped too.
        notebooks: Also delete old `setup_<hash>`, `teardown_<hash>` and debug notebooks and inlined saved query
            files from the helpers folder, or with scope `all` from every user's default helpers folder too, unless a
            job the sweep keeps uses them.
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
        notebooks: The workspace paths of the notebooks and of the inlined saved query files.
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
            f"Sweep dry run: would delete {len(deleted_jobs)} jobs, {len(deleted_pipelines)} pipelines, "
            f"{len(deleted_schemas)} schemas and {len(deleted_notebooks)} notebooks"
        )
    else:
        log.info(f"Swept {len(deleted_jobs)} leftover ephemeral jobs and {len(deleted_pipelines)} pipeline copies")
        if options.schemas_catalog or options.notebooks:
            log.info(f"Swept {len(deleted_schemas)} leftover schemas and {len(deleted_notebooks)} notebooks")
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


def _expiry_ms(value: str | None) -> float | None:
    """Read a `pytest_expires` tag. A missing or unreadable tag gives `None`, and the job's age decides."""
    if not value:
        return None
    try:
        expiry = datetime.fromisoformat(value)
    except ValueError:
        log.warning(f"Ignoring the unreadable {EXPIRES_TAG} tag {value!r}")
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
            folder = default_folder(posixpath.basename(info.path))
            if folder not in folders:
                folders.append(folder)
    except DatabricksError as e:
        _record(errors, f"Could not list the user folders in {USERS_FOLDER}: {e}")
    return folders


@dataclass(frozen=True)
class _OldNotebook:
    """A setup, teardown or debug notebook or a query file last changed before the cutoff, with a debug one's run."""

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
    debug_folder = f"{folder}/{DEBUG_FOLDER}"
    has_debug_folder = False
    try:
        for info in ws.workspace.list(folder):
            path = info.path or ""
            if info.object_type == workspace.ObjectType.DIRECTORY and path == debug_folder:
                has_debug_folder = True
            elif _is_old_helper(info, cutoff_ms):
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


def _is_old_helper(info: workspace.ObjectInfo, cutoff_ms: float) -> bool:
    """Whether `info` is a setup or teardown notebook or an inlined saved query file, last changed before the cutoff."""
    name = posixpath.basename(info.path or "")
    if LIFECYCLE_NOTEBOOK.fullmatch(name):
        return _is_old(info, workspace.ObjectType.NOTEBOOK, cutoff_ms)
    if QUERY_FILE.fullmatch(name):
        return _is_old(info, workspace.ObjectType.FILE, cutoff_ms)
    return False


def _is_old_notebook(info: workspace.ObjectInfo, cutoff_ms: float) -> bool:
    return _is_old(info, workspace.ObjectType.NOTEBOOK, cutoff_ms)


def _is_old(info: workspace.ObjectInfo, object_type: workspace.ObjectType, cutoff_ms: float) -> bool:
    return info.object_type == object_type and (info.modified_at or 0) < cutoff_ms


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
            log.info(f"Would delete {kind} {name}")
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
    log.warning(message)
    errors.append(message)
