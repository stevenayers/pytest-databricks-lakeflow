"""In-memory stand-ins for the parts of `WorkspaceClient` the plugin uses."""

from __future__ import annotations

import base64
import itertools
import json
import os
import re
import tarfile
import time
import urllib.error
import urllib.parse
import zipfile
from collections.abc import Collection, Iterator, Mapping, Sequence
from dataclasses import dataclass, field, fields, replace
from datetime import UTC, datetime
from email.message import Message
from email.utils import format_datetime
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from databricks.sdk.errors import BadRequest, InvalidParameterValue, NotFound
from databricks.sdk.service import catalog, iam, jobs, pipelines, workspace

DATA = Path(__file__).parent / "data"
HOST = "https://example.cloud.databricks.com"
HOUR_MS = 3_600_000
ACCOUNT_ID = "a1"
TERMINATED = jobs.RunLifecycleStateV2State.TERMINATED
SKIPPED = jobs.RunLifeCycleState.SKIPPED
# What the platform reports, on the run and on each of its task runs, when every task of the job is disabled.
ALL_DISABLED = "Skipped because every task is disabled. Re-enable at least one task to be able to run the job."
# The error `runs/get-output` reports for every pipeline task run, also one that ended SUCCESS.
PIPELINE_LOGS = "Please refer to the logs for this pipeline in the pipelines page."
# The termination message of an alert task whose query failed, as recorded on the alerts bundle.
ALERT_FAILED = (
    "An error occurred during execution of task Alert task: Query execution failed. Please check the alert query "
    "text and evaluation conditions first."
)
# What the platform reports for a for-each task whose iterations failed: one entry per distinct error, counted.
ITERATION_FAILED = "Workload failed, see run output for details"
# The one filter shape the plugin sends to `list_pipeline_events`. The platform refuses `level="ERROR"` with
# "Invalid filter expression"; the value must be in single quotes.
LEVEL_FILTER = re.compile(r"^level='(?P<level>[A-Z]+)'$")
# The one filter shape the plugin sends to `list_pipelines`. The platform takes `[` and `]` in it literally.
NAME_FILTER = re.compile(r"^name LIKE '(?P<prefix>[^%']*)%'$")
# The platform refuses a longer pipeline name with "Name cannot be longer than 255 characters.", counting characters,
# not bytes.
MAX_PIPELINE_NAME_CHARS = 255
PIPELINE_SPEC_FIELDS = frozenset(pipelines.PipelineSpec.__dataclass_fields__)
# A pre-signed link to a dbt task's artefacts, with a signature that must never reach a log line or report.json.
DBT_LINK = "https://storage.example.com/dbt/artifacts.tar.gz?sig=secret"
DBT_HEADERS = {"x-ms-blob-type": "BlockBlob"}


@dataclass
class RunWithIterations(jobs.Run):
    """A job run that also carries the iterations of its for-each tasks, which `FakeJobs` serves.

    The Jobs API lists no iterations on a job run, only on a for-each task run fetched by its own id, so `make_run`
    and `recorded` hand them to `FakeJobs.add_run` and `FakeJobs.run_now` here.

    Attributes:
        for_each_iterations: The iteration runs of each for-each task, by the for-each task run's id.
    """

    for_each_iterations: dict[int, list[jobs.RunTask]] = field(default_factory=dict)


class FakeJobs:
    def __init__(self) -> None:
        self.settings: dict[int, jobs.JobSettings] = {}
        self.created: list[dict[str, Any]] = []
        self.acls: list[list[jobs.JobAccessControlRequest] | None] = []
        self.deleted: list[int] = []
        self.cancelled: list[int] = []
        self.run_now_calls: list[dict[str, Any]] = []
        self.tokens: dict[str, int] = {}
        self.listed: list[jobs.BaseJob] = []
        self.queued: list[list[jobs.Run]] = []
        self.after_cancel: dict[int, list[jobs.Run]] = {}
        self.polls: dict[int, list[jobs.Run]] = {}
        self.run_errors: dict[int, Exception] = {}
        self.outputs: dict[int, jobs.RunOutput | Exception] = {}
        self.raw_outputs: dict[int, dict[str, Any] | Exception] = {}
        self.output_calls: list[int] = []
        self.resolved: dict[int, jobs.ResolvedValues | Exception] = {}
        self.iterations: dict[int, list[jobs.RunTask] | Exception] = {}
        self.get_run_calls: list[dict[str, Any]] = []
        self.exports: dict[int, str | Exception] = {}
        self.export_calls: list[dict[str, Any]] = []
        self.delete_errors: dict[int, Exception] = {}
        self.delete_log: Path | None = None
        self.deletions: list[str] = []
        self.list_calls = 0
        self.list_kwargs: list[dict[str, Any]] = []
        self.list_error: Exception | None = None
        self.gets: list[int] = []
        self.updated: list[tuple[int, jobs.JobSettings | None]] = []
        self.run_timeouts: list[int | None] = []
        self._ids = itertools.count(1000)

    def start_ids_at(self, first: int) -> None:
        """Number the jobs and runs made from now on from `first`."""
        self._ids = itertools.count(first)

    def add(self, settings: jobs.JobSettings) -> int:
        job_id = next(self._ids)
        self.settings[job_id] = settings
        return job_id

    def get(self, job_id: int) -> jobs.Job:
        self.gets.append(job_id)
        if job_id not in self.settings:
            raise NotFound(f"Job {job_id} does not exist.")
        return jobs.Job(job_id=job_id, settings=self.settings[job_id])

    def create(self, **kwargs: Any) -> jobs.CreateResponse:
        self.acls.append(kwargs.pop("access_control_list", None))
        self.created.append(kwargs)
        return jobs.CreateResponse(job_id=self.add(jobs.JobSettings(**kwargs)))

    def update(
        self,
        job_id: int,
        *,
        new_settings: jobs.JobSettings | None = None,
        fields_to_remove: Sequence[str] | None = None,
    ) -> None:
        """Merge the fields `new_settings` sets into the job's settings, then clear `fields_to_remove`.

        Like the platform, a body without `tasks` on a job with one task is read in the legacy single-task format:
        its `timeout_seconds` lands on the task and the job's own is unchanged.
        """
        self.updated.append((job_id, new_settings))
        if job_id not in self.settings:
            raise NotFound(f"Job {job_id} does not exist.")
        current = self.settings[job_id]
        if new_settings is not None:
            legacy = new_settings.tasks is None and len(current.tasks or []) == 1
            for name in jobs.JobSettings.__dataclass_fields__:
                value = getattr(new_settings, name)
                if value is None:
                    continue
                if legacy and name == "timeout_seconds":
                    current.tasks = [replace((current.tasks or [])[0], timeout_seconds=value)]
                    continue
                setattr(current, name, value)
        for name in fields_to_remove or []:
            setattr(current, name, None)

    def delete(self, job_id: int) -> None:
        if job_id in self.delete_errors:
            raise self.delete_errors[job_id]
        self.deleted.append(job_id)
        self.deletions.append(f"job {job_id}")
        self.settings.pop(job_id, None)
        if self.delete_log is not None:
            with self.delete_log.open("a") as f:
                f.write(f"{job_id}\n")

    def queue(
        self,
        run: jobs.Run,
        outputs: Mapping[int, jobs.RunOutput] | None = None,
        polls: int = 1,
        running: jobs.Run | None = None,
        after_cancel: jobs.Run | None = None,
    ) -> None:
        """Make the next `run_now` return `run`, reported as running for `polls` calls to `get_run`.

        `running` is the snapshot those calls report, by default a running run with no tasks. When `after_cancel` is
        set, `cancel_run` replaces the polls left with one `running` poll and then `after_cancel`, as a run that the
        platform stops a few seconds after the cancel.
        """
        if running is None:
            status = jobs.RunStatus(state=jobs.RunLifecycleStateV2State.RUNNING)
            running = jobs.Run(run_id=run.run_id, run_page_url=run.run_page_url, status=status)
        self.queued.append([running] * polls + [run])
        self.outputs.update(outputs or {})
        if after_cancel is not None:
            assert run.run_id is not None
            self.after_cancel[run.run_id] = [running, after_cancel]

    def queue_polls(self, polls: Sequence[jobs.Run], outputs: Mapping[int, jobs.RunOutput] | None = None) -> None:
        """Make the next `run_now` report `polls` in order, one per call to `get_run`, then keep reporting the last."""
        self.queued.append(list(polls))
        self.outputs.update(outputs or {})

    def run_now(
        self,
        job_id: int,
        *,
        idempotency_token: str | None = None,
        job_parameters: dict[str, str] | None = None,
        only: Sequence[str] | None = None,
        performance_target: jobs.PerformanceTarget | None = None,
        queue: jobs.QueueSettings | None = None,
        pipeline_params: jobs.PipelineParams | None = None,
        dbt_commands: Sequence[str] | None = None,
    ) -> SimpleNamespace:
        self.run_now_calls.append(
            {
                "job_id": job_id,
                "idempotency_token": idempotency_token,
                "job_parameters": job_parameters,
                "only": only,
                "performance_target": performance_target,
                "queue": queue,
                "pipeline_params": pipeline_params,
                "dbt_commands": dbt_commands,
            }
        )
        if idempotency_token is not None and idempotency_token in self.tokens:
            return SimpleNamespace(response=jobs.RunNowResponse(run_id=self.tokens[idempotency_token]))
        # The job's timeout_seconds when the run starts, which is the one that binds the run.
        job = self.settings.get(job_id)
        self.run_timeouts.append(job.timeout_seconds if job else None)
        polls = self.queued.pop(0) if self.queued else [make_run(next(self._ids), {"task": "SUCCESS"})]
        run_id = polls[-1].run_id
        assert run_id is not None
        # Like the Jobs API, every snapshot of the run names the job it belongs to, and the dbt_commands it was
        # started with under overriding_parameters.
        overriding = polls[-1].overriding_parameters
        if dbt_commands is not None:
            overriding = jobs.RunParameters(dbt_commands=list(dbt_commands))
        self.polls[run_id] = [replace(run, job_id=job_id, overriding_parameters=overriding) for run in polls]
        self._add_iterations(polls[-1])
        if idempotency_token is not None:
            self.tokens[idempotency_token] = run_id
        return SimpleNamespace(response=jobs.RunNowResponse(run_id=run_id))

    def add_run(self, run: jobs.Run) -> None:
        """Make `get_run` report `run`, a run started outside the plugin such as a Run Job task's child run."""
        assert run.run_id is not None
        self.polls[run.run_id] = [run]
        self._add_iterations(run)

    def _add_iterations(self, run: jobs.Run) -> None:
        """Serve the iterations a `RunWithIterations` carries, with the resolved values recorded on each.

        A for-each task run whose `iterations` a test already set, such as to an error, keeps them.
        """
        if not isinstance(run, RunWithIterations):
            return
        for loop_run_id, iterations in run.for_each_iterations.items():
            listed: list[jobs.RunTask] = []
            for iteration in iterations:
                # The listing carries no resolved values: the platform fills them only on an iteration fetched by its
                # own id with `include_resolved_values`, which `get_run` serves from `resolved`.
                if iteration.resolved_values is not None and iteration.run_id is not None:
                    self.resolved[iteration.run_id] = iteration.resolved_values
                listed.append(replace(iteration, resolved_values=None))
            self.iterations.setdefault(loop_run_id, listed)

    def get_run(self, run_id: int, **kwargs: Any) -> jobs.Run:
        """Serve a job run, or a task run or iteration fetched by its own id.

        A for-each task run fetched by its own id lists every one of its `iterations`. The platform pages them 100 at
        a time, and databricks-sdk's `jobs.get_run` follows `next_page_token` until it has them all, so the client
        returns them in one `Run`.
        """
        self.get_run_calls.append({"run_id": run_id, **kwargs})
        if run_id in self.run_errors:
            raise self.run_errors[run_id]
        if run_id in self.polls:
            polls = self.polls[run_id]
            return polls.pop(0) if len(polls) > 1 else polls[0]
        found = self._task_run(run_id)
        if found is None:
            raise NotFound(f"Run {run_id} does not exist.")
        parent, task = found
        # Like the Jobs API, a task run fetched by its own id is a run with that one task, which names the job and
        # the job run it belongs to, and `resolved_values` is filled only then and only with
        # `include_resolved_values`. On a job run it is always null.
        resolved = self.resolved.get(run_id) if kwargs.get("include_resolved_values") else None
        if isinstance(resolved, Exception):
            raise resolved
        iterations = self.iterations.get(run_id)
        if isinstance(iterations, Exception):
            raise iterations
        return jobs.Run(
            run_id=run_id,
            job_id=parent.job_id,
            job_run_id=parent.run_id,
            tasks=[replace(task, resolved_values=resolved)],
            iterations=iterations,
        )

    def _task_run(self, run_id: int) -> tuple[jobs.Run, jobs.RunTask] | None:
        """Find the task run or iteration `run_id`, with the last snapshot of the job run it belongs to."""
        for polls in self.polls.values():
            parent = polls[-1]
            for task in parent.tasks or []:
                if task.run_id == run_id:
                    return parent, task
                iterations = self.iterations.get(task.run_id or 0)
                if isinstance(iterations, Exception):
                    continue
                for iteration in iterations or []:
                    if iteration.run_id == run_id:
                        return parent, iteration
        return None

    def get_run_output(self, run_id: int) -> jobs.RunOutput:
        self.output_calls.append(run_id)
        output = self.outputs.get(run_id, jobs.RunOutput())
        if isinstance(output, Exception):
            raise output
        return output

    def export_run(self, run_id: int, *, views_to_export: jobs.ViewsToExport | None = None) -> jobs.ExportRunOutput:
        """Serve the notebook HTML recorded in `exports` for a task run, or no views when none was recorded."""
        self.export_calls.append({"run_id": run_id, "views_to_export": views_to_export})
        content = self.exports.get(run_id)
        if isinstance(content, Exception):
            raise content
        if content is None:
            return jobs.ExportRunOutput(views=[])
        view = jobs.ViewItem(content=content, name="notebook", type=jobs.ViewType.NOTEBOOK)
        return jobs.ExportRunOutput(views=[view])

    def cancel_run(self, run_id: int) -> None:
        self.cancelled.append(run_id)
        if run_id in self.after_cancel:
            self.polls[run_id] = self.after_cancel.pop(run_id)

    def list(self, **kwargs: Any) -> Iterator[jobs.BaseJob]:
        """List `listed`, or raise `list_error`.

        Like the Jobs API, each job's tasks come back only with `expand_tasks`.
        """
        self.list_calls += 1
        self.list_kwargs.append(kwargs)
        if self.list_error is not None:
            raise self.list_error
        if kwargs.get("expand_tasks"):
            return iter(self.listed)
        bare = []
        for job in self.listed:
            if job.settings is not None and job.settings.tasks:
                job = replace(job, settings=replace(job.settings, tasks=None))
            bare.append(job)
        return iter(bare)


class FakeApiClient:
    """Stands in for `WorkspaceClient.api_client`, serving `runs/get-output` as raw JSON from `FakeJobs.raw_outputs`."""

    RUN_OUTPUT = "/api/2.2/jobs/runs/get-output"

    def __init__(self, jobs: FakeJobs) -> None:
        self.jobs = jobs
        self.calls: list[dict[str, Any]] = []

    def do(self, method: str, path: str, query: dict[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
        self.calls.append({"method": method, "path": path, "query": query, **kwargs})
        if (method, path) != ("GET", self.RUN_OUTPUT):
            raise AssertionError(f"FakeApiClient serves only GET {self.RUN_OUTPUT}, not {method} {path}")
        run_id = (query or {}).get("run_id")
        if run_id not in self.jobs.raw_outputs:
            raise NotFound(f"Run {run_id} does not exist.")
        output = self.jobs.raw_outputs[run_id]
        if isinstance(output, Exception):
            raise output
        return output


class FakeWorkspace:
    def __init__(self) -> None:
        self.objects: set[str] = set()
        self.folders: list[str] = []
        self.uploads: dict[str, dict[str, Any]] = {}
        self.upload_calls: list[str] = []
        self.listed: list[workspace.ObjectInfo] = []
        self.deleted: list[str] = []
        self.modified: dict[str, int] = {}
        self.object_ids: dict[str, int] = {}
        self.upload_errors: dict[str, Exception] = {}
        self.list_calls: list[str] = []
        self.list_errors: dict[str, Exception] = {}
        self._ids = itertools.count(5000)

    def mkdirs(self, path: str) -> None:
        self.folders.append(path)

    def upload(self, path: str, content: bytes, **kwargs: Any) -> None:
        parent = path.rsplit("/", 1)[0]
        if parent not in self.folders:
            raise NotFound(f"The parent folder ({parent}) does not exist.")
        if path in self.upload_errors:
            raise self.upload_errors[path]
        self.upload_calls.append(path)
        self.uploads[path] = {"content": content.decode(), **kwargs}
        self.objects.add(path)
        self.object_ids.setdefault(path, next(self._ids))

    def get_status(self, path: str) -> SimpleNamespace:
        if path not in self.objects:
            raise NotFound(f"Path ({path}) doesn't exist.")
        return SimpleNamespace(
            path=path,
            modified_at=self.modified.get(path),
            object_id=self.object_ids.get(path),
            object_type=workspace.ObjectType.NOTEBOOK,
        )

    def download(self, path: str, *, format: workspace.ExportFormat | None = None) -> BytesIO:
        if path not in self.uploads:
            raise NotFound(f"Path ({path}) doesn't exist.")
        return BytesIO(self.uploads[path]["content"].encode())

    def list(self, path: str, **kwargs: Any) -> Iterator[workspace.ObjectInfo]:
        """List the objects in `listed` directly inside the folder `path`.

        Like the Workspace API, a folder that does not exist is a `NotFound`: one with nothing listed in it that was
        neither made with `mkdirs` nor listed as a directory. A folder in `list_errors` raises its error.
        """
        folder = path.rstrip("/")
        self.list_calls.append(folder)
        if folder in self.list_errors:
            raise self.list_errors[folder]
        inside = []
        for info in self.listed:
            if (info.path or "").rsplit("/", 1)[0] == folder:
                inside.append(info)
        directories = set(self.folders)
        for info in self.listed:
            if info.object_type == workspace.ObjectType.DIRECTORY:
                directories.add(info.path or "")
        if not inside and folder not in directories:
            raise NotFound(f"Path ({folder}) doesn't exist.")
        return iter(inside)

    def delete(self, path: str, **kwargs: Any) -> None:
        self.deleted.append(path)
        self.objects.discard(path)


class FakeFiles:
    def __init__(self, workspace: FakeWorkspace) -> None:
        self.workspace = workspace

    def get_metadata(self, path: str) -> SimpleNamespace:
        """Report `FakeWorkspace.modified` as the HTTP date the Files API returns in its last-modified header."""
        modified_at = self.workspace.get_status(path).modified_at
        if modified_at is None:
            return SimpleNamespace(last_modified=None)
        when = datetime.fromtimestamp(modified_at / 1000, tz=UTC)
        return SimpleNamespace(last_modified=format_datetime(when, usegmt=True))


class FakeSchemas:
    def __init__(self) -> None:
        self.created: list[str] = []
        # The properties each schema was created with, by full name.
        self.properties: dict[str, dict[str, str] | None] = {}
        self.deleted: list[str] = []
        self.listed: list[catalog.SchemaInfo] = []
        self.list_errors: dict[str, Exception] = {}
        self.deletions: list[str] = []

    def create(
        self, name: str, catalog_name: str, *, properties: dict[str, str] | None = None, **kwargs: Any
    ) -> SimpleNamespace:
        self.created.append(f"{catalog_name}.{name}")
        self.properties[f"{catalog_name}.{name}"] = properties
        return SimpleNamespace(full_name=f"{catalog_name}.{name}", properties=properties)

    def delete(self, full_name: str, *, force: bool | None = None) -> None:
        assert force
        self.deleted.append(full_name)
        self.deletions.append(f"schema {full_name}")

    def list(self, catalog_name: str, **kwargs: Any) -> Iterator[catalog.SchemaInfo]:
        if catalog_name in self.list_errors:
            raise self.list_errors[catalog_name]
        return iter([info for info in self.listed if info.catalog_name == catalog_name])


class FakePipelines:
    """Stands in for `WorkspaceClient.pipelines`, serving updates and events by pipeline id and recording each call.

    `updates` holds the updates `list_updates` returns, or an error it raises, in pages of `page_size` when it is
    set; a `page_token` is the position of the page's first update. `events` holds the events `list_pipeline_events`
    yields, newest first as the platform returns them, applying a `level='...'` filter. `read_events` lists the id of
    every event yielded, so a test can tell how far a caller paged.

    `specs` holds the spec `get` returns for each pipeline, which `create` adds to under the ids `p-1000`, `p-1001`
    and so on, and `delete` removes from. `update_polls` holds the updates `get_update` reports for a pipeline, one
    per call and then the last one again, like `FakeJobs.get_run`; without them every update has `COMPLETED`.
    `listed` holds what `list_pipelines` lists, applying a `name LIKE '<prefix>%'` filter.
    """

    def __init__(self) -> None:
        self.updates: dict[str, list[pipelines.UpdateInfo] | Exception] = {}
        self.page_size: int | None = None
        self.events: dict[str, list[pipelines.PipelineEvent]] = {}
        self.calls: list[dict[str, Any]] = []
        self.read_events: list[str | None] = []
        self.specs: dict[str, pipelines.PipelineSpec] = {}
        self.last_modified: dict[str, int] = {}
        self.creators: dict[str, str] = {}
        self.created: list[dict[str, Any]] = []
        self.deleted: list[dict[str, Any]] = []
        self.delete_errors: dict[str, Exception] = {}
        self.deletions: list[str] = []
        self.stopped: list[str] = []
        self.start_update_calls: list[dict[str, Any]] = []
        self.update_polls: dict[str, list[pipelines.UpdateInfo]] = {}
        self.listed: list[pipelines.PipelineStateInfo] = []
        self.list_filters: list[str | None] = []
        self._ids = itertools.count(1000)
        self._update_ids = itertools.count(1)

    def get(self, pipeline_id: str) -> pipelines.GetPipelineResponse:
        if pipeline_id not in self.specs:
            raise NotFound(f"The specified pipeline {pipeline_id} was not found.")
        spec = self.specs[pipeline_id]
        return pipelines.GetPipelineResponse(
            pipeline_id=pipeline_id,
            name=spec.name,
            spec=spec,
            creator_user_name=self.creators.get(pipeline_id, "someone@example.com"),
            last_modified=self.last_modified.get(pipeline_id),
        )

    def create(self, **kwargs: Any) -> pipelines.CreatePipelineResponse:
        if len(kwargs.get("name") or "") > MAX_PIPELINE_NAME_CHARS:
            raise InvalidParameterValue("Name cannot be longer than 255 characters.")
        self.created.append(kwargs)
        pipeline_id = f"p-{next(self._ids)}"
        fields: dict[str, Any] = {key: value for key, value in kwargs.items() if key in PIPELINE_SPEC_FIELDS}
        fields["id"] = pipeline_id
        self.specs[pipeline_id] = pipelines.PipelineSpec(**fields)
        self.last_modified[pipeline_id] = int(time.time() * 1000)
        return pipelines.CreatePipelineResponse(pipeline_id=pipeline_id)

    def delete(self, pipeline_id: str, *, cascade: bool | None = None, force: bool | None = None) -> None:
        if pipeline_id in self.delete_errors:
            raise self.delete_errors[pipeline_id]
        self.deleted.append({"pipeline_id": pipeline_id, "cascade": cascade, "force": force})
        self.deletions.append(f"pipeline {pipeline_id}")
        self.specs.pop(pipeline_id, None)

    def start_update(self, pipeline_id: str, **kwargs: Any) -> pipelines.StartUpdateResponse:
        if pipeline_id not in self.specs:
            raise NotFound(f"The specified pipeline {pipeline_id} was not found.")
        self.start_update_calls.append({"pipeline_id": pipeline_id, **kwargs})
        return pipelines.StartUpdateResponse(update_id=f"u-{next(self._update_ids)}")

    def get_update(self, pipeline_id: str, update_id: str) -> pipelines.GetUpdateResponse:
        polls = self.update_polls.get(pipeline_id)
        if polls:
            info = polls.pop(0) if len(polls) > 1 else polls[0]
        else:
            info = pipelines.UpdateInfo(state=pipelines.UpdateInfoState.COMPLETED)
        update = replace(info, pipeline_id=pipeline_id, update_id=update_id)
        return pipelines.GetUpdateResponse(update=update)

    def stop(self, pipeline_id: str) -> SimpleNamespace:
        self.stopped.append(pipeline_id)
        return SimpleNamespace()

    def list_pipelines(self, *, filter: str | None = None, **kwargs: Any) -> Iterator[pipelines.PipelineStateInfo]:
        self.list_filters.append(filter)
        if filter is None:
            return iter(self.listed)
        match = NAME_FILTER.match(filter)
        if match is None:
            raise InvalidParameterValue(f"Invalid filter expression: {filter}")
        return iter([info for info in self.listed if (info.name or "").startswith(match["prefix"])])

    def leftover(
        self,
        pipeline_id: str,
        age_hours: float,
        tags: dict[str, str],
        creator: str = "someone@example.com",
        name: str = "[pytest] test_x.py::test_x my_pipeline",
    ) -> None:
        """List a pipeline last modified `age_hours` ago by `creator`, with `tags`, as a sweep finds one."""
        self.specs[pipeline_id] = pipelines.PipelineSpec(id=pipeline_id, name=name, tags=tags)
        self.last_modified[pipeline_id] = int(time.time() * 1000 - age_hours * HOUR_MS)
        self.creators[pipeline_id] = creator
        self.listed.append(pipelines.PipelineStateInfo(pipeline_id=pipeline_id, name=name, creator_user_name=creator))

    def list_updates(
        self, pipeline_id: str, *, page_token: str | None = None, **kwargs: Any
    ) -> pipelines.ListUpdatesResponse:
        call = {"method": "list_updates", "pipeline_id": pipeline_id, **kwargs}
        if page_token is not None:
            call["page_token"] = page_token
        self.calls.append(call)
        updates = self.updates.get(pipeline_id, [])
        if isinstance(updates, Exception):
            raise updates
        if self.page_size is None:
            return pipelines.ListUpdatesResponse(updates=list(updates))
        first = int(page_token or 0)
        last = first + self.page_size
        next_page_token = None
        if last < len(updates):
            next_page_token = str(last)
        return pipelines.ListUpdatesResponse(updates=updates[first:last], next_page_token=next_page_token)

    def list_pipeline_events(
        self, pipeline_id: str, *, filter: str | None = None, **kwargs: Any
    ) -> Iterator[pipelines.PipelineEvent]:
        self.calls.append({"method": "list_pipeline_events", "pipeline_id": pipeline_id, "filter": filter, **kwargs})
        level = None
        if filter is not None:
            match = LEVEL_FILTER.match(filter)
            if match is None:
                raise InvalidParameterValue(f"Invalid filter expression: {filter}")
            level = pipelines.EventLevel(match["level"])
        return self._yield_events(self.events.get(pipeline_id, []), level)

    def _yield_events(
        self, events: Sequence[pipelines.PipelineEvent], level: pipelines.EventLevel | None
    ) -> Iterator[pipelines.PipelineEvent]:
        for event in events:
            if level is not None and event.level != level:
                continue
            self.read_events.append(event.id)
            yield event


class FakeCurrentUser:
    def __init__(self) -> None:
        self.me_calls = 0
        self.user_name = "someone@example.com"
        self.groups: list[str] = []

    def me(self) -> iam.User:
        self.me_calls += 1
        groups = [iam.ComplexValue(display=name, type="direct") for name in self.groups]
        return iam.User(user_name=self.user_name, groups=groups or None)


class FakeAccountAccessControlProxy:
    """Serves service principal rule sets from `rule_sets`, by rule set name."""

    def __init__(self) -> None:
        self.rule_sets: dict[str, iam.RuleSetResponse | Exception] = {}
        self.names: list[str] = []

    def grant(self, application_id: str, role: str, *principals: str) -> None:
        """Add a rule granting `role` on the service principal `application_id` to `principals`."""
        name = sp_rule_set(application_id)
        rule_set = self.rule_sets.setdefault(name, iam.RuleSetResponse(name=name, etag="e", grant_rules=[]))
        assert isinstance(rule_set, iam.RuleSetResponse) and rule_set.grant_rules is not None
        rule_set.grant_rules.append(iam.GrantRule(role=role, principals=list(principals)))

    def get_rule_set(self, name: str, etag: str) -> iam.RuleSetResponse:
        """Like the platform, an unknown service principal is a `BadRequest`, not a `NotFound`."""
        self.names.append(name)
        if name not in self.rule_sets:
            raise BadRequest(f"ServicePrincipal {name.split('/')[3]} not found")
        rule_set = self.rule_sets[name]
        if isinstance(rule_set, Exception):
            raise rule_set
        return rule_set


def sp_rule_set(application_id: str) -> str:
    return f"accounts/{ACCOUNT_ID}/servicePrincipals/{application_id}/ruleSets/default"


class FakeDbtResponse(BytesIO):
    """The body of a pre-signed link, with the headers the storage service sends with it."""

    def __init__(self, body: bytes, headers: Mapping[str, str]) -> None:
        super().__init__(body)
        self.headers = dict(headers)


class FakeDbtLinks:
    """Stands in for `dbt._open`, serving the tarball in `bodies` for each link and recording every request.

    A link without a body fails with HTTP 403, as an expired pre-signed link does, and one whose body is an exception
    raises it. A link in `lengths` is served with that `Content-Length` header instead of the body's own length.
    """

    def __init__(self) -> None:
        self.bodies: dict[str, bytes | Exception] = {}
        self.lengths: dict[str, int] = {}
        self.requests: list[tuple[str, dict[str, str]]] = []

    def __call__(self, url: str, headers: Mapping[str, str]) -> FakeDbtResponse:
        self.requests.append((url, dict(headers)))
        if url not in self.bodies:
            raise urllib.error.HTTPError(url, 403, "Forbidden", Message(), None)
        body = self.bodies[url]
        if isinstance(body, Exception):
            raise body
        return FakeDbtResponse(body, {"Content-Length": str(self.lengths.get(url, len(body)))})


def dbt_tarball(
    statuses: Mapping[str, str] | None, *, manifest: bool = True, extra: Mapping[str, bytes] | None = None
) -> bytes:
    """Build a dbt artefacts tarball whose `target/run_results.json` gives each node in `statuses` that status.

    `statuses` of `None` leaves `run_results.json` out. `manifest` adds `target/manifest.json` naming the same nodes.
    `extra` adds members by name, such as a path that climbs out of the archive.
    """
    members: dict[str, bytes] = {}
    if statuses is not None:
        members.update(_dbt_target(statuses, manifest=manifest))
    members.update(extra or {})
    buffer = BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        for name, data in members.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            archive.addfile(info, BytesIO(data))
    return buffer.getvalue()


def _dbt_target(statuses: Mapping[str, str], *, manifest: bool) -> dict[str, bytes]:
    results = []
    for unique_id, status in statuses.items():
        results.append(
            {
                "unique_id": unique_id,
                "status": status,
                "message": "Database Error" if status in ("error", "fail") else "OK",
                "failures": 1 if status == "fail" else None,
                "execution_time": 0.5,
            }
        )
    members = {
        "target/run_results.json": json.dumps({"metadata": {"dbt_version": "1.9.0"}, "results": results}).encode()
    }
    if manifest:
        nodes = {unique_id: {"unique_id": unique_id, "resource_type": "model"} for unique_id in statuses}
        members["target/manifest.json"] = json.dumps({"nodes": nodes}).encode()
    return members


class FakeWorkspaceClient:
    def __init__(self) -> None:
        self.jobs = FakeJobs()
        self.api_client = FakeApiClient(self.jobs)
        self.workspace = FakeWorkspace()
        self.files = FakeFiles(self.workspace)
        self.schemas = FakeSchemas()
        self.pipelines = FakePipelines()
        self.current_user = FakeCurrentUser()
        self.account_access_control_proxy = FakeAccountAccessControlProxy()
        self.config = SimpleNamespace(host=HOST, profile=None, account_id=ACCOUNT_ID)
        self.profiles: list[str | None] = []
        # Every job, pipeline and schema deleted, in order, such as "job 1001", "pipeline p-1000" and "schema c.s".
        self.deletions: list[str] = []
        self.jobs.deletions = self.pipelines.deletions = self.schemas.deletions = self.deletions


# What `WorkspaceClient(profile=...)` raises on a databricks-cli profile when another process refreshes the CLI's
# token at the same moment, recorded from eight concurrent clients (#140); the real text goes on with the Config.
TOKEN_RACE = (
    "default auth: databricks-cli: cannot get access token: Error: forced token refresh: cache update: exit status 45. "
    "Try logging in again with `databricks auth login --profile dev` before retrying."
)


class ContendedTokenCache:
    """Stands in for `WorkspaceClient` while other xdist workers refresh the same databricks-cli token.

    The first `collisions` clients fail as the real SDK does; later ones return `client`.
    """

    def __init__(self, collisions: int, client: object = "client") -> None:
        self.collisions = collisions
        self.client = client
        self.profiles: list[str | None] = []

    def __call__(self, profile: str | None = None) -> object:
        self.profiles.append(profile)
        if len(self.profiles) <= self.collisions:
            raise ValueError(TOKEN_RACE)
        return self.client


class NotebookExit(Exception):
    """Raised by `FakeDbutils.notebook.exit`, which ends a notebook like the real one does."""

    def __init__(self, value: str) -> None:
        super().__init__(value)
        self.value = value


class FakeDbutils:
    """Stands in for the `dbutils` of a notebook task: widgets from a mapping, and a `notebook.exit` that stops.

    Every job parameter and base parameter of a task is a widget, and `widgets.getAll()` returns them all. Runtimes
    before Databricks Runtime 13.3 LTS have no `getAll`, which `get_all=False` stands in for.
    """

    def __init__(self, widgets: Mapping[str, str], get_all: bool = True) -> None:
        self.widgets = SimpleNamespace(get=lambda name: widgets[name])
        if get_all:
            self.widgets.getAll = lambda: dict(widgets)
        self.notebook = SimpleNamespace(exit=self._exit)

    @staticmethod
    def _exit(value: str) -> None:
        raise NotebookExit(value)


class FakeSpark:
    """Stands in for a notebook's `spark`, recording each statement passed to `sql`."""

    def __init__(self) -> None:
        self.statements: list[str] = []

    def sql(self, statement: str) -> None:
        self.statements.append(statement)


def run_notebook(source: str, widgets: Mapping[str, str], spark: object = None, get_all: bool = True) -> str | None:
    """Run a generated notebook's source with `spark` and a `FakeDbutils`, and return its exit value or `None`."""
    namespace = {"spark": spark, "dbutils": FakeDbutils(widgets, get_all)}
    # dont_inherit, so the source runs without the future imports of this module, as it does on the cluster.
    code = compile(source, "<notebook>", "exec", dont_inherit=True)
    try:
        exec(code, namespace)
    except NotebookExit as e:
        return e.value
    return None


def notebook_export(commands: Sequence[dict[str, Any]]) -> str:
    """Build the HTML of a notebook view as `jobs.export_run` returns it, with a notebook model of `commands`.

    Like the real export, the model is JSON, URL-encoded as `encodeURIComponent` does, then base64-encoded.
    """
    model = json.dumps({"version": "NotebookV1", "name": "notebook", "language": "python", "commands": commands})
    encoded = base64.b64encode(urllib.parse.quote(model, safe="!~*'()").encode()).decode()
    return (
        "<!DOCTYPE html>\n<html>\n<head>\n<title>notebook - Databricks</title>\n"
        f"<script>var __DATABRICKS_NOTEBOOK_MODEL = '{encoded}';</script>\n</head>\n<body></body>\n</html>"
    )


def listed_job(
    job_id: int,
    age_hours: float,
    tags: dict[str, str],
    creator: str = "someone@example.com",
    notebooks: Sequence[str] = (),
    tasks: Sequence[jobs.Task] = (),
) -> jobs.BaseJob:
    """Build a job as `jobs.list` returns it, created `age_hours` ago by `creator`.

    It has `tasks` and one notebook task for each of `notebooks`, which `jobs.list` returns only with `expand_tasks`.
    """
    created = int(time.time() * 1000 - age_hours * HOUR_MS)
    tasks = list(tasks)
    for i, path in enumerate(notebooks):
        tasks.append(jobs.Task(task_key=f"task_{i}", notebook_task=jobs.NotebookTask(notebook_path=path)))
    settings = jobs.JobSettings(tags=tags, tasks=tasks or None)
    return jobs.BaseJob(job_id=job_id, created_time=created, creator_user_name=creator, settings=settings)


def sweep_candidates() -> list[jobs.BaseJob]:
    """Jobs for `--lakeflow-sweep` to choose from. With a 24 hour cutoff and run id `this-run`, it deletes 1 and 7."""
    old = {"pytest": "", "pytest_run_id": "old-run"}
    return [
        listed_job(1, 25, old),
        listed_job(2, 25, {"pytest": "", "pytest_run_id": "this-run"}),
        listed_job(3, 1, old),
        listed_job(4, 25, old, creator="colleague@example.com"),
        listed_job(5, 25, {"pytest_run_id": "old-run"}),
        listed_job(6, 25, {"pytest": ""}),
        listed_job(7, 30, old),
        jobs.BaseJob(job_id=8, created_time=0),
    ]


def recorded(name: str) -> tuple[jobs.Run, dict[int, jobs.RunOutput]]:
    """Load a run recorded during the spikes, with its task outputs by task run id.

    A recording of a run with for-each tasks has an `iterations` key: the iterations that each for-each task run
    lists, by its run id, each with the `resolved_values` its own `get_run` reported. The run then carries them, as
    a `RunWithIterations`.
    """
    data = json.loads((DATA / "runs" / f"{name}.json").read_text())
    outputs = {int(run_id): jobs.RunOutput.from_dict(output) for run_id, output in data["outputs"].items()}
    run = jobs.Run.from_dict(data["run"])
    iterations: dict[int, list[jobs.RunTask]] = {}
    for loop_run_id, listed in data.get("iterations", {}).items():
        iterations[int(loop_run_id)] = [jobs.RunTask.from_dict(iteration) for iteration in listed]
    return with_iterations(run, iterations), outputs


def with_iterations(run: jobs.Run, iterations: Mapping[int, list[jobs.RunTask]]) -> jobs.Run:
    """Return `run` carrying `iterations`, by for-each task run id, or `run` itself when there are none."""
    if not iterations:
        return run
    values = {f.name: getattr(run, f.name) for f in fields(jobs.Run)}
    return RunWithIterations(**values, for_each_iterations=dict(iterations))


def make_run(
    run_id: int,
    states: Mapping[str, str | None],
    result_state: str = "SUCCESS",
    code: str = "SUCCESS",
    attempts: Mapping[str, Sequence[str | None]] | None = None,
    condition_outcomes: Mapping[str, str] | None = None,
    notebooks: Collection[str] = (),
    sql_files: Mapping[str, str] | None = None,
    pipeline_tasks: Mapping[str, str] | None = None,
    trigger: jobs.TriggerType = jobs.TriggerType.ONE_TIME,
    iterations: Mapping[str, Sequence[str]] | None = None,
) -> jobs.Run:
    """Build a terminated run whose tasks ended in `states`, keyed by task key.

    Like the Jobs API, a `DISABLED` task run never carries `disabled`, whether the job disabled it or `only` left it
    out: only the job's settings say which tasks are disabled.

    A state of `None` gives a task run with no result state and legacy life cycle `SKIPPED`, as the platform reports
    every task of a run it skipped.

    `attempts` gives the state of every attempt of the tasks it names, in order, numbered from attempt 0. Task run
    ids are `run_id * 100` plus the position of the attempt among all task runs.

    `condition_outcomes` makes the tasks it names condition tasks, with the given `outcome`, such as `"true"`.

    `notebooks` makes the tasks it names notebook tasks, which the run reports with their `notebook_task`.

    `sql_files` makes the tasks it names SQL file tasks on warehouse `w1`, running the file at the given path, which
    the run reports with their `sql_task`.

    `pipeline_tasks` makes the tasks it names pipeline tasks that update the pipeline with the given id. Every task run
    starts at 1,000 ms and ends at 3,000 ms, so an update created in between is the one the task started.

    `trigger` is what started the run, `ONE_TIME` for a run-now run, as the platform reports it.

    `iterations` makes the tasks it names for-each tasks over the inputs `"0"`, `"1"` and so on, repeating the task
    `each`, whose iterations ended in the given states, in order, each in one attempt. The task run reports its
    `for_each_task` with the platform's `stats`, and the run is a `RunWithIterations`, so `FakeJobs` lists the
    iterations on the task run fetched by its own id. An iteration's run id is the for-each task run's id times 1,000
    plus its position.
    """
    states_by_key = {key: [state] for key, state in states.items()}
    for key, key_states in (attempts or {}).items():
        states_by_key[key] = list(key_states)
    tasks = []
    for key, key_states in states_by_key.items():
        for attempt, state in enumerate(key_states):
            task_run_id = run_id * 100 + len(tasks)
            tasks.append(
                jobs.RunTask(
                    task_key=key,
                    run_id=task_run_id,
                    attempt_number=attempt,
                    state=_state(state),
                    status=jobs.RunStatus(state=TERMINATED, termination_details=_details(state)),
                    run_page_url=f"{HOST}/jobs/runs/{task_run_id}",
                    start_time=1_000,
                    end_time=3_000,
                    condition_task=_condition(condition_outcomes, key),
                    notebook_task=_notebook(notebooks, key),
                    sql_task=_sql_file(sql_files, key),
                    pipeline_task=_pipeline(pipeline_tasks, key),
                    for_each_task=_for_each(iterations, key),
                )
            )
    loops: dict[int, list[jobs.RunTask]] = {}
    for task in tasks:
        if iterations is not None and task.task_key in iterations and task.run_id is not None:
            loops[task.run_id] = _iteration_runs(task.run_id, iterations[task.task_key])
    run = jobs.Run(
        run_id=run_id,
        job_run_id=run_id,
        state=jobs.RunState(result_state=jobs.RunResultState(result_state)),
        status=jobs.RunStatus(state=TERMINATED, termination_details=_details(code)),
        tasks=tasks,
        run_page_url=f"{HOST}/jobs/runs/{run_id}",
        start_time=1_000,
        end_time=6_000,
        trigger=trigger,
    )
    return with_iterations(run, loops)


def skipped_run(run_id: int, keys: Sequence[str], notebooks: Collection[str] = ()) -> jobs.Run:
    """Build the run the platform reports when every task of the job is disabled, with one task run per key.

    As recorded on a real run: neither the run nor its task runs have a result state, their legacy life cycle is
    `SKIPPED`, and each carries termination code `RUN_EXECUTION_ERROR` with the message in `ALL_DISABLED`. The
    platform ends the run at once, so it starts and ends at the same time. `notebooks` makes the tasks it names
    notebook tasks, as in `make_run`.
    """
    run = make_run(run_id, dict.fromkeys(keys), notebooks=notebooks)
    details = jobs.TerminationDetails(
        code=jobs.TerminationCodeCode.RUN_EXECUTION_ERROR,
        message=ALL_DISABLED,
        type=jobs.TerminationTypeType.CLIENT_ERROR,
    )
    status = jobs.RunStatus(state=TERMINATED, termination_details=details)
    state = jobs.RunState(life_cycle_state=SKIPPED, state_message=ALL_DISABLED)
    tasks = [replace(task, state=state, status=status) for task in run.tasks or []]
    return replace(run, state=state, status=status, tasks=tasks, end_time=run.start_time)


def queue_failed_run(ws: FakeWorkspaceClient, states: Mapping[str, str | None] | None = None) -> None:
    """Queue failed run 77, whose tasks end in `states`, by default transform `FAILED`.

    Its first task run, 7700, reports resolved notebook parameters, so a debug notebook can replay them.
    """
    ws.jobs.resolved[7700] = jobs.ResolvedValues(
        notebook_task=jobs.ResolvedNotebookTaskValues(base_parameters={"row_count": "7", "param1": "hello"})
    )
    if states is None:
        states = {"transform": "FAILED"}
    ws.jobs.queue(make_run(77, states, "FAILED", "RUN_EXECUTION_ERROR"))


def failed_alert_run(run_id: int, key: str = "broken") -> jobs.Run:
    """Build the run of one alert task, `key`, whose query failed, as recorded on the alerts bundle.

    The task ends FAILED with termination code `RUN_EXECUTION_ERROR`, type `CLIENT_ERROR` and the message in
    `ALERT_FAILED`, which is also its state message, and the run's life cycle is `INTERNAL_ERROR`. The task's
    `runs/get-output` holds only `alert_output.alert_state` `ERROR`, with no error: see `failed_alert_output`.
    """
    run = make_run(run_id, {key: "FAILED"}, "FAILED", "RUN_EXECUTION_ERROR")
    details = jobs.TerminationDetails(
        code=jobs.TerminationCodeCode.RUN_EXECUTION_ERROR,
        message=ALERT_FAILED,
        type=jobs.TerminationTypeType.CLIENT_ERROR,
    )
    status = jobs.RunStatus(state=TERMINATED, termination_details=details)
    state = jobs.RunState(
        life_cycle_state=jobs.RunLifeCycleState.TERMINATED,
        result_state=jobs.RunResultState.FAILED,
        state_message=ALERT_FAILED,
    )
    alert = jobs.AlertTask(alert_id="3926452559074312", warehouse_id="0123456789abcdef")
    tasks = [replace(task, state=state, status=status, alert_task=alert) for task in run.tasks or []]
    run_state = jobs.RunState(
        life_cycle_state=jobs.RunLifeCycleState.INTERNAL_ERROR, result_state=jobs.RunResultState.FAILED
    )
    return replace(run, state=run_state, status=status, tasks=tasks)


def failed_alert_output() -> jobs.RunOutput:
    """The `runs/get-output` of the alert task of `failed_alert_run`: an alert state and no error."""
    return jobs.RunOutput(alert_output=jobs.AlertTaskOutput(alert_state=jobs.AlertEvaluationState.ERROR))


def running_run(run_id: int, states: dict[str, str]) -> jobs.Run:
    """Build a run that has not terminated, with one task run per key of `states`.

    A value that is a V2 lifecycle state, such as `PENDING` or `RUNNING`, gives a task run still in that state. Any
    other value is a result state, such as `SUCCESS`, of a task run that has terminated. Task run ids are numbered as
    in `make_run`, so outputs keyed by them also match the terminated run.
    """
    lifecycle = {state.value for state in jobs.RunLifecycleStateV2State} - {TERMINATED.value}
    tasks = []
    for position, (key, state) in enumerate(states.items()):
        task_run_id = run_id * 100 + position
        url = f"{HOST}/jobs/runs/{task_run_id}"
        if state in lifecycle:
            status = jobs.RunStatus(state=jobs.RunLifecycleStateV2State(state))
            tasks.append(jobs.RunTask(task_key=key, run_id=task_run_id, status=status, run_page_url=url))
            continue
        tasks.append(
            jobs.RunTask(
                task_key=key,
                run_id=task_run_id,
                state=jobs.RunState(result_state=jobs.RunResultState(state)),
                status=jobs.RunStatus(state=TERMINATED, termination_details=_details(state)),
                run_page_url=url,
                start_time=1_000,
                end_time=3_000,
            )
        )
    status = jobs.RunStatus(state=jobs.RunLifecycleStateV2State.RUNNING)
    return jobs.Run(
        run_id=run_id, job_run_id=run_id, status=status, tasks=tasks, run_page_url=f"{HOST}/jobs/runs/{run_id}"
    )


def _condition(outcomes: Mapping[str, str] | None, key: str) -> jobs.RunConditionTask | None:
    if outcomes is None or key not in outcomes:
        return None
    return jobs.RunConditionTask(op=jobs.ConditionTaskOp.EQUAL_TO, left="", right="", outcome=outcomes[key])


def _notebook(notebooks: Collection[str], key: str) -> jobs.NotebookTask | None:
    if key not in notebooks:
        return None
    return jobs.NotebookTask(notebook_path=f"/Workspace/src/{key}")


def _sql_file(sql_files: Mapping[str, str] | None, key: str) -> jobs.SqlTask | None:
    if sql_files is None or key not in sql_files:
        return None
    return jobs.SqlTask(warehouse_id="w1", file=jobs.SqlTaskFile(path=sql_files[key]))


def _pipeline(pipeline_ids: Mapping[str, str] | None, key: str) -> jobs.PipelineTask | None:
    if pipeline_ids is None or key not in pipeline_ids:
        return None
    return jobs.PipelineTask(pipeline_id=pipeline_ids[key])


def _for_each(iterations: Mapping[str, Sequence[str]] | None, key: str) -> jobs.RunForEachTask | None:
    if iterations is None or key not in iterations:
        return None
    states = iterations[key]
    succeeded = states.count("SUCCESS")
    failed = len(states) - succeeded
    # As recorded, the platform counts each iteration once, however often it was retried, and groups the errors by
    # message, with the same generic message for every iteration that failed.
    errors = []
    if failed:
        errors.append(
            jobs.ForEachTaskErrorMessageStats(
                count=failed, error_message=ITERATION_FAILED, termination_category="RunExecutionError"
            )
        )
    task_run_stats = jobs.ForEachTaskTaskRunStats(
        total_iterations=len(states),
        scheduled_iterations=len(states),
        active_iterations=0,
        completed_iterations=len(states),
        succeeded_iterations=succeeded,
        failed_iterations=failed,
    )
    stats = jobs.ForEachStats(task_run_stats=task_run_stats, error_message_stats=errors)
    inputs = json.dumps([str(position) for position in range(len(states))])
    return jobs.RunForEachTask(inputs=inputs, task=jobs.Task(task_key="each"), stats=stats)


def _iteration_runs(loop_run_id: int, states: Sequence[str]) -> list[jobs.RunTask]:
    runs: list[jobs.RunTask] = []
    for position, state in enumerate(states):
        iteration_run_id = loop_run_id * 1000 + position
        runs.append(
            jobs.RunTask(
                task_key="each",
                run_id=iteration_run_id,
                attempt_number=0,
                state=_state(state),
                status=jobs.RunStatus(state=TERMINATED, termination_details=_details(state)),
                run_page_url=f"{HOST}/jobs/runs/{iteration_run_id}",
                start_time=1_000,
                end_time=2_000,
            )
        )
    return runs


def _state(state: str | None) -> jobs.RunState:
    if state is None:
        return jobs.RunState(life_cycle_state=SKIPPED)
    return jobs.RunState(result_state=jobs.RunResultState(state))


def _details(state: str | None) -> jobs.TerminationDetails:
    if state is None:
        return jobs.TerminationDetails(code=jobs.TerminationCodeCode.RUN_EXECUTION_ERROR)
    codes = {
        "FAILED": "RUN_EXECUTION_ERROR",
        "UPSTREAM_FAILED": "SKIPPED",
        "CANCELED": "USER_CANCELED",
        "EXCLUDED": "SKIPPED",
        "TIMEDOUT": "RUN_EXECUTION_ERROR",
    }
    return jobs.TerminationDetails(code=jobs.TerminationCodeCode(codes.get(state, state)))


def example_settings() -> jobs.JobSettings:
    """Build a deployed bundle job: extract, transform and load, with notifications, a schedule and parameters."""
    nb = "/Workspace/Users/someone@example.com/.bundle/example/test/files/src"
    return jobs.JobSettings.from_dict(
        {
            "name": "[dev someone] my_job",
            "deployment": {"kind": "BUNDLE", "metadata_file_path": "/Workspace/x/state/metadata.json"},
            "edit_mode": "UI_LOCKED",
            "schedule": {"quartz_cron_expression": "0 0 * * * ?", "timezone_id": "UTC", "pause_status": "PAUSED"},
            "email_notifications": {"on_failure": ["someone@example.com"]},
            "run_as": {"user_name": "someone@example.com"},
            "max_concurrent_runs": 4,
            "tags": {"dev": "someone", "purpose": "pytest"},
            "parameters": [{"name": "param1", "default": "hello"}, {"name": "schema", "default": ""}],
            "environments": [{"environment_key": "default", "spec": {"environment_version": "4"}}],
            "job_clusters": [{"job_cluster_key": "classic", "new_cluster": {"spark_version": "17.3.x-scala2.13"}}],
            "tasks": [
                {"task_key": "extract", "notebook_task": {"notebook_path": f"{nb}/extract"}},
                {
                    "task_key": "transform",
                    "depends_on": [{"task_key": "extract"}],
                    "notebook_task": {
                        "notebook_path": f"{nb}/transform",
                        "base_parameters": {"row_count": "{{tasks.extract.values.row_count}}"},
                    },
                    "email_notifications": {"on_failure": ["someone@example.com"]},
                },
                {
                    "task_key": "load",
                    "depends_on": [{"task_key": "transform"}],
                    "environment_key": "default",
                    "python_wheel_task": {"package_name": "pkg", "entry_point": "load"},
                },
                {
                    "task_key": "classic",
                    "job_cluster_key": "classic",
                    "notebook_task": {"notebook_path": f"{nb}/classic"},
                },
            ],
        }
    )


def pipeline_settings() -> jobs.JobSettings:
    """Build a deployed bundle job with a pipeline task and a notebook task that depends on it."""
    nb = "/Workspace/Users/someone@example.com/.bundle/example/test/files/src"
    return jobs.JobSettings.from_dict(
        {
            "name": "[dev someone] pipeline_job",
            "tasks": [
                {"task_key": "refresh", "pipeline_task": {"pipeline_id": "abc"}},
                {
                    "task_key": "check",
                    "depends_on": [{"task_key": "refresh"}],
                    "notebook_task": {"notebook_path": f"{nb}/check"},
                },
            ],
        }
    )


def pipeline_spec() -> pipelines.PipelineSpec:
    """Build the spec of a deployed bundle pipeline, `abc`: what a copy must drop, and a glob library to keep.

    `pipeline_settings()` points its pipeline task at it, so `fake_ws.pipelines.specs["abc"] = pipeline_spec()` wires
    the two together.
    """
    files = "/Workspace/Users/someone@example.com/.bundle/example/test/files"
    return pipelines.PipelineSpec.from_dict(
        {
            "id": "abc",
            "name": "[dev someone] my_pipeline",
            "catalog": "prod_catalog",
            "schema": "prod",
            "serverless": True,
            "development": False,
            "continuous": True,
            "channel": "CURRENT",
            "deployment": {"kind": "BUNDLE", "metadata_file_path": "/Workspace/x/state/metadata.json"},
            "notifications": [{"email_recipients": ["someone@example.com"], "alerts": ["on-update-failure"]}],
            "event_log": {"catalog": "prod_catalog", "schema": "ops", "name": "event_log"},
            "trigger": {"cron": {"quartz_cron_schedule": "0 0 * * * ?", "timezone_id": "UTC"}},
            "libraries": [{"glob": {"include": f"{files}/src/pipeline/**"}}],
            "root_path": f"{files}/src/pipeline",
            "configuration": {"schema": "prod"},
            "tags": {"dev": "someone"},
        }
    )


def reporting_settings() -> jobs.JobSettings:
    """Build a reporting job: subscribed dashboard and SQL alert tasks, alert and Power BI tasks, and a notify task."""
    return jobs.JobSettings.from_dict(
        {
            "name": "[dev someone] reporting",
            "tasks": [
                {
                    "task_key": "dashboard",
                    "dashboard_task": {
                        "dashboard_id": "d1",
                        "warehouse_id": "w1",
                        "subscription": {
                            "custom_subject": "Daily report",
                            "subscribers": [{"user_name": "someone@example.com"}, {"destination_id": "slack"}],
                        },
                    },
                },
                {
                    "task_key": "alert",
                    "sql_task": {
                        "warehouse_id": "w1",
                        "alert": {"alert_id": "a1", "subscriptions": [{"user_name": "someone@example.com"}]},
                    },
                },
                {
                    "task_key": "check",
                    "alert_task": {
                        "alert_id": "a2",
                        "warehouse_id": "w1",
                        "subscribers": [{"user_name": "someone@example.com"}],
                    },
                },
                {
                    "task_key": "refresh_powerbi",
                    "power_bi_task": {
                        "connection_resource_name": "pbi",
                        "warehouse_id": "w1",
                        "refresh_after_update": True,
                        "power_bi_model": {
                            "workspace_name": "Reports",
                            "model_name": "Sales",
                            "storage_mode": "IMPORT",
                            "authentication_method": "OAUTH",
                            "overwrite_existing": False,
                        },
                        "tables": [{"catalog": "main", "schema": "gold", "name": "sales", "storage_mode": "IMPORT"}],
                    },
                },
                {
                    "task_key": "notify",
                    "depends_on": [
                        {"task_key": "dashboard"},
                        {"task_key": "alert"},
                        {"task_key": "check"},
                        {"task_key": "refresh_powerbi"},
                    ],
                    "notebook_task": {"notebook_path": "/Workspace/src/notify"},
                },
            ],
        }
    )


def parent_settings() -> jobs.JobSettings:
    """Build a parent job: a notebook task, then a Run Job task that triggers the deployed job 123."""
    return jobs.JobSettings.from_dict(
        {
            "name": "[dev someone] parent",
            "deployment": {"kind": "BUNDLE", "metadata_file_path": "/Workspace/x/state/metadata.json"},
            "parameters": [{"name": "schema", "default": ""}],
            "tasks": [
                {"task_key": "prepare", "notebook_task": {"notebook_path": "/Workspace/src/prepare"}},
                {
                    "task_key": "trigger_child",
                    "depends_on": [{"task_key": "prepare"}],
                    "run_job_task": {"job_id": 123, "job_parameters": {"schema": "{{job.parameters.schema}}"}},
                },
            ],
        }
    )


def branching_settings() -> jobs.JobSettings:
    """Build an if/else job like the `branches` bundle: `check` compares a value `check_input` sets, two branches."""
    nb = "/Workspace/Users/someone@example.com/.bundle/lakeflow_branches/test/files/src/notebooks"
    return jobs.JobSettings.from_dict(
        {
            "name": "[dev someone] branching_job",
            "parameters": [{"name": "rows", "default": "0"}],
            "tasks": [
                {"task_key": "check_input", "notebook_task": {"notebook_path": f"{nb}/check_input"}},
                {
                    "task_key": "check",
                    "depends_on": [{"task_key": "check_input"}],
                    "condition_task": {
                        "op": "GREATER_THAN",
                        "left": "{{tasks.check_input.values.rows}}",
                        "right": "0",
                    },
                },
                {
                    "task_key": "on_true",
                    "depends_on": [{"task_key": "check", "outcome": "true"}],
                    "notebook_task": {"notebook_path": f"{nb}/on_true"},
                },
                {
                    "task_key": "on_false",
                    "depends_on": [{"task_key": "check", "outcome": "false"}],
                    "notebook_task": {"notebook_path": f"{nb}/on_false"},
                },
            ],
        }
    )


def for_each_settings() -> jobs.JobSettings:
    """Build a fan-out job: `loop` repeats the notebook task `each` over three inputs, then `after` runs."""
    nb = "/Workspace/Users/someone@example.com/.bundle/lakeflow_for_each/test/files/src/notebooks"
    return jobs.JobSettings.from_dict(
        {
            "name": "[dev someone] fanout",
            "tasks": [
                {
                    "task_key": "loop",
                    "for_each_task": {
                        "inputs": '["a", "b", "c"]',
                        "task": {
                            "task_key": "each",
                            "notebook_task": {"notebook_path": f"{nb}/each", "base_parameters": {"item": "{{input}}"}},
                        },
                    },
                },
                {
                    "task_key": "after",
                    "depends_on": [{"task_key": "loop"}],
                    "notebook_task": {"notebook_path": f"{nb}/after"},
                },
            ],
        }
    )


def fanout_objects_settings() -> jobs.JobSettings:
    """Build the `for_each` bundle's `fanout_objects`: `extract`, then `loop` over objects with the body `inner`.

    `inner` reads the whole item, its `id` and a task value of `extract`, and `after` runs once `loop` is done.
    """
    nb = "/Workspace/Users/someone@example.com/.bundle/lakeflow_for_each/test/files/src/notebooks"
    return jobs.JobSettings.from_dict(
        {
            "name": "[dev someone] fanout_objects",
            "tasks": [
                {"task_key": "extract", "notebook_task": {"notebook_path": f"{nb}/extract"}},
                {
                    "task_key": "loop",
                    "depends_on": [{"task_key": "extract"}],
                    "for_each_task": {
                        "inputs": '[{"id": 1, "path": "/a"}, {"id": 2, "path": "/b"}]',
                        "concurrency": 2,
                        "task": {
                            "task_key": "inner",
                            "timeout_seconds": 600,
                            "notebook_task": {
                                "notebook_path": f"{nb}/inner",
                                "base_parameters": {
                                    "item": "{{input}}",
                                    "item_id": "{{input.id}}",
                                    "rows": "{{tasks.extract.values.rows}}",
                                },
                            },
                        },
                    },
                },
                {
                    "task_key": "after",
                    "depends_on": [{"task_key": "loop"}],
                    "notebook_task": {"notebook_path": f"{nb}/after"},
                },
            ],
        }
    )


def dbt_settings() -> jobs.JobSettings:
    """Build a deployed bundle job with a serverless dbt task, `dbt_build`, and a notebook task downstream of it."""
    files = "/Workspace/Users/someone@example.com/.bundle/analytics/test/files"
    return jobs.JobSettings.from_dict(
        {
            "name": "[dev someone] analytics",
            "environments": [{"environment_key": "dbt", "spec": {"environment_version": "4"}}],
            "tasks": [
                {
                    "task_key": "dbt_build",
                    "environment_key": "dbt",
                    "dbt_task": {
                        "commands": ["dbt deps", "dbt build"],
                        "warehouse_id": "w1",
                        "catalog": "prod_catalog",
                        "schema": "analytics",
                        "project_directory": f"{files}/dbt",
                    },
                },
                {
                    "task_key": "check",
                    "depends_on": [{"task_key": "dbt_build"}],
                    "notebook_task": {"notebook_path": f"{files}/src/check"},
                },
            ],
        }
    )


def triggered_settings() -> jobs.JobSettings:
    """Build `example_settings` with `transform` reading the trigger type and the tables a table update trigger saw."""
    settings = example_settings()
    transform = next(task for task in settings.tasks or [] if task.task_key == "transform")
    assert transform.notebook_task is not None
    transform.notebook_task.base_parameters = {
        **(transform.notebook_task.base_parameters or {}),
        "kind": "{{job.trigger.type}}",
        "tables": "{{job.trigger.table_update.updated_tables}}",
    }
    return settings


def build_wheel(
    dist: Path,
    name: str = "pkg",
    *,
    version: str = "0.1.0",
    entry_points: str = "[console_scripts]\nmain = pkg.main:main\n",
    init: str = "",
    modified: float | None = None,
) -> Path:
    """Write a wheel of project `name` to `dist`, with `entry_points` as its entry_points.txt and `init` in its package.

    The package is `name` with underscores, as `uv build` names it, and `modified` is the wheel's modification time.
    """
    package = re.sub(r"[-.]+", "_", name).lower()
    stem = f"{package}-{version}"
    dist.mkdir(parents=True, exist_ok=True)
    path = dist / f"{stem}-py3-none-any.whl"
    with zipfile.ZipFile(path, "w") as wheel:
        wheel.writestr(f"{package}/__init__.py", init)
        wheel.writestr(f"{package}/main.py", "def main():\n    pass\n")
        wheel.writestr(f"{stem}.dist-info/METADATA", f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n")
        if entry_points:
            wheel.writestr(f"{stem}.dist-info/entry_points.txt", entry_points)
    if modified is not None:
        os.utime(path, (modified, modified))
    return path
