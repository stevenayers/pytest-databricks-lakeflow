"""Session steps: `Lakeflow.once()`, a setup run once per session and its teardown at session end.

A step is one serverless job holding a `__pytest_setup` task, a `__pytest_teardown` task or both, named after the step.
The first caller in the session creates it and runs the setup task alone; every later caller, in any xdist worker,
waits for that run and reads its outcome. The teardown task runs alone when the session ends, on the xdist controller
when there is one, and the job is deleted unless the keep rules keep it.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
import uuid
from collections.abc import Callable, Mapping
from datetime import timedelta
from functools import partial
from typing import TYPE_CHECKING, Any, cast

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

from pytest_databricks_lakeflow import deployment, helpers, session, workspace_api
from pytest_databricks_lakeflow.ephemeral.job import EphemeralJob
from pytest_databricks_lakeflow.ephemeral.polling import poll_seconds, terminated
from pytest_databricks_lakeflow.errors import LakeflowError
from pytest_databricks_lakeflow.helpers import LifecycleStep
from pytest_databricks_lakeflow.naming import PREFIX, SETUP_KEY, TEARDOWN_KEY
from pytest_databricks_lakeflow.results.build import build_job_run
from pytest_databricks_lakeflow.results.models import JobRun, task_outcome
from pytest_databricks_lakeflow.settings.lifecycle import inject_lifecycle
from pytest_databricks_lakeflow.settings.template import sanitise

if TYPE_CHECKING:
    from pytest_databricks_lakeflow.client import Lakeflow

log = logging.getLogger("pytest_databricks_lakeflow")
# The tag holding the step's name on its job.
ONCE_TAG = "pytest_once"
DIGEST_CHARS = 16
Notebooks = Mapping[str, tuple[str, frozenset[str]]]


def run_once(
    lakeflow: Lakeflow,
    name: str,
    *,
    setup: LifecycleStep | None,
    teardown: LifecycleStep | None,
    params: Mapping[str, str] | None,
    timeout: timedelta | None,
) -> JobRun | None:
    """Run `setup` once in this session and register `teardown` for its end. See `Lakeflow.once()`."""
    if not isinstance(name, str) or not name.strip():
        raise ValueError(f"once() needs a name for the step, got {name!r}")
    if setup is None and teardown is None:
        raise ValueError(f"once({name!r}) needs setup=, teardown= or both")
    if lakeflow.helpers_compute:
        raise ValueError(
            f"once({name!r}) runs on serverless, but helpers_compute is {lakeflow.helpers_compute!r}, a job cluster "
            "of a bundle job, which a session step does not have. Pass existing_cluster_id to run it on a running "
            "cluster, or derive a Lakeflow without helpers_compute."
        )
    params = dict(params or {})
    reserved = sorted(key for key in params if key.startswith(PREFIX))
    if reserved:
        raise ValueError(f"Parameter names starting with {PREFIX!r} are reserved for the plugin: {reserved}")
    item = session.current_item()
    current = session.current()
    if timeout is None:
        timeout = lakeflow.timeout
    notebooks: dict[str, tuple[str, frozenset[str]]] = {}
    for role, step in (("setup", setup), ("teardown", teardown)):
        if step is not None:
            notebooks[role] = helpers.lifecycle_source(step, current.rootpath, role)
    for role, (_, names) in notebooks.items():
        unknown = sorted(set(names) - set(params))
        if unknown:
            raise LakeflowError(
                f"{role}= of once({name!r}) reads {unknown}, which params does not hold. Pass them in params, or "
                "write $$ for a literal dollar."
            )
    digest = _digest(name, notebooks, params)
    record = current.once_records.get(digest)
    if record is None:
        path = None if current.state_dir is None else current.state_dir / f"once_{digest}.json"
        produce = partial(_produce, lakeflow, item, name, notebooks, params, timeout)
        try:
            record = session.shared_state(path, produce, f"once({name!r})")
        except Exception as e:
            current.once_records[digest] = e
            raise
        current.once_records[digest] = record
    if isinstance(record, Exception):
        raise LakeflowError(f"once({name!r}) failed earlier in this session: {type(record).__name__}: {record}")
    return _result(lakeflow, name, record)


def _digest(name: str, notebooks: Notebooks, params: Mapping[str, str]) -> str:
    sources = {role: source for role, (source, _) in notebooks.items()}
    fields = [name, sources, sorted(params.items())]
    return hashlib.sha256(json.dumps(fields).encode()).hexdigest()[:DIGEST_CHARS]


def _produce(
    lakeflow: Lakeflow,
    item: pytest.Item,
    name: str,
    notebooks: Notebooks,
    params: dict[str, str],
    timeout: timedelta,
) -> dict[str, Any]:
    """Create the step's job, run its setup task and return the record every worker shares."""
    current = session.current()
    folder = deployment.folder_of_helpers(lakeflow._state())
    paths: dict[str, str] = {}
    for role, (source, _) in notebooks.items():
        paths[role] = helpers.upload_lifecycle(lakeflow.ws, folder, role, source, current.uploaded_notebooks)
    settings = _settings(lakeflow, item, name, paths, params, timeout)
    job = EphemeralJob(lakeflow, name, settings, params, teardown_at="close")
    job.create()
    assert job.job_id is not None
    record: dict[str, Any] = {
        "name": name,
        "job_id": job.job_id,
        "job_url": job.url,
        "run_id": None,
        "run_url": None,
        "teardown": "teardown" in paths,
        "profile": lakeflow.profile or lakeflow.ws.config.profile,
        "timeout_seconds": timeout.total_seconds(),
        "poll_seconds": poll_seconds(lakeflow),
        "keep": current.keeps(lakeflow.keep, keep_failed=False, failed=False),
        "keep_failed": lakeflow.keep_failed,
    }
    if "setup" in paths:
        result = job._run_alone(SETUP_KEY, timeout=timeout)
        record["run_id"] = result.run_id
        record["run_url"] = result.run_url
        setup = result.setup
        if setup is None or not setup.succeeded:
            job._delete_unless_kept(failed=True)
            outcome = f"ended {result.result_state} without running the setup task"
            if setup is not None:
                outcome = task_outcome(setup)
            raise LakeflowError(f"once({name!r}): the setup step failed: {outcome}: {result.run_url}")
        current.once_results[result.run_id] = result
    current.once_jobs.append(record)
    current.once_clients[job.job_id] = lakeflow.ws
    log.info(f"once({name!r}): job {job.job_id} is set up for the session: {job.url}")
    return record


def _settings(
    lakeflow: Lakeflow,
    item: pytest.Item,
    name: str,
    paths: Mapping[str, str],
    params: Mapping[str, str],
    timeout: timedelta,
) -> jobs.JobSettings:
    """Build the step's job: its parameters, the lifecycle tasks on the helper compute, and the plugin's tags."""
    parameters = [jobs.JobParameterDefinition(name=key, default=value) for key, value in params.items()]
    settings = jobs.JobSettings(parameters=parameters or None, tasks=[])
    inject_lifecycle(settings, paths.get("setup"), paths.get("teardown"), lakeflow._helpers(), params)
    for task in settings.tasks or []:
        if not task.depends_on:
            task.depends_on = None
    if lakeflow.existing_cluster_id:
        _on_existing_cluster(settings, lakeflow.existing_cluster_id)
    tags = {**lakeflow._tags(item), ONCE_TAG: name}
    description = (
        f"Session step {name!r} of pytest run {lakeflow.run_id}: set up once for every test and torn down when the "
        f"session ends. First asked for by {item.nodeid}."
    )
    return sanitise(
        settings,
        name=f"once: {name}",
        tags=tags,
        description=description,
        disable_auto_optimization=lakeflow.disable_auto_optimization,
        performance_target=lakeflow._performance_target(),
        timeout=timeout,
        budget_policy_id=lakeflow.budget_policy_id,
    )


def _on_existing_cluster(settings: jobs.JobSettings, cluster_id: str) -> None:
    """Move the step's tasks from the helper environment onto a running cluster."""
    for task in settings.tasks or []:
        task.existing_cluster_id = cluster_id
        task.environment_key = None
    settings.environments = None


def _result(lakeflow: Lakeflow, name: str, record: Mapping[str, Any]) -> JobRun | None:
    """The setup run's outcome: the one this process built, or one rebuilt from the Jobs API."""
    run_id = record["run_id"]
    if run_id is None:
        return None
    current = session.current()
    result = current.once_results.get(run_id)
    if result is None:
        run = lakeflow.ws.jobs.get_run(run_id)
        result = build_job_run(
            lakeflow.ws,
            run,
            job_key=name,
            job_id=record["job_id"],
            job_keys=lakeflow._job_keys(),
            notebook_export=lakeflow.notebook_export,
        )
        current.once_results[run_id] = result
    return result


def finish(state: session.SessionState, write: Callable[[str], None]) -> None:
    """Tear down every `once()` job of the session: run its teardown task, then delete it unless it is kept.

    A kept job is reported through `write`, because the terminal summary has been printed by then. An error only logs
    a warning, so the session's own result stands.

    Args:
        state: The session, with the records the workers reported.
        write: Writes one line to the terminal.
    """
    for record in state.once_jobs:
        name, job_id = record["name"], record["job_id"]
        try:
            ws = state.once_clients.get(job_id) or workspace_api.make_workspace_client(record["profile"])
            _finish_one(ws, record, write)
        except Exception as e:
            log.warning(f"once({name!r}): job {job_id} could not be torn down: {type(e).__name__}: {e}")


def _finish_one(ws: WorkspaceClient, record: Mapping[str, Any], write: Callable[[str], None]) -> None:
    name, job_id = record["name"], record["job_id"]
    keep = record["keep"]
    teardown_failed = False
    if record["teardown"] and not keep:
        teardown_failed = not _run_teardown(ws, record)
    if keep or (teardown_failed and record["keep_failed"]):
        log.warning(f"Keeping job {job_id} of once({name!r}): {record['job_url']}")
        write(f"lakeflow: kept the job of once({name!r}): {record['job_url']}")
        return
    ws.jobs.delete(job_id)
    log.info(f"Deleted job {job_id} of once({name!r})")


def _run_teardown(ws: WorkspaceClient, record: Mapping[str, Any]) -> bool:
    """Run the step's teardown task alone and wait for it. Returns whether it succeeded."""
    name, job_id = record["name"], record["job_id"]
    timeout = timedelta(seconds=record["timeout_seconds"])
    waiter = ws.jobs.run_now(
        job_id=job_id, idempotency_token=uuid.uuid4().hex, only=[TEARDOWN_KEY], queue=jobs.QueueSettings(enabled=True)
    )
    run_id = cast(jobs.RunNowResponse, waiter.response).run_id
    assert run_id is not None
    log.info(f"Started the teardown run {run_id} of once({name!r})")
    deadline = time.monotonic() + timeout.total_seconds()
    run = ws.jobs.get_run(run_id)
    while not terminated(run):
        if time.monotonic() >= deadline:
            workspace_api.cancel_run(ws, run_id)
            log.warning(
                f"The teardown run {run_id} of once({name!r}) did not finish within {timeout} and was cancelled: "
                f"{run.run_page_url}"
            )
            return False
        time.sleep(record["poll_seconds"])
        run = ws.jobs.get_run(run_id)
    result = build_job_run(ws, run, job_key=name, job_id=job_id)
    teardown = result.teardown
    if teardown is not None and teardown.succeeded:
        log.info(f"Teardown run {run_id} of once({name!r}) finished SUCCESS: {result.run_url}")
        return True
    outcome = f"ended {result.result_state} without running the teardown task"
    if teardown is not None:
        outcome = f"failed: {task_outcome(teardown)}"
    log.warning(f"The teardown run {run_id} of once({name!r}) {outcome}: {result.run_url}")
    return False
