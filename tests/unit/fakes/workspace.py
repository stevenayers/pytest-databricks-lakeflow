"""In-memory stand-ins for the parts of `WorkspaceClient` the plugin uses."""

from __future__ import annotations

import fnmatch
import itertools
import re
import time
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import replace
from datetime import UTC, datetime
from email.utils import format_datetime
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from databricks.sdk.errors import BadRequest, InvalidParameterValue, NotFound
from databricks.sdk.service import catalog, iam, jobs, pipelines, sql, workspace

from .runs import HOST, HOUR_MS, RunWithIterations, make_run

ACCOUNT_ID = "a1"
# The one filter shape the plugin sends to `list_pipeline_events`. The platform refuses `level="ERROR"` with
# "Invalid filter expression"; the value must be in single quotes.
LEVEL_FILTER = re.compile(r"^level='(?P<level>[A-Z]+)'$")
# The one filter shape the plugin sends to `list_pipelines`. The platform takes `[` and `]` in it literally.
NAME_FILTER = re.compile(r"^name LIKE '(?P<prefix>[^%']*)%'$")
# The platform refuses a longer pipeline name with "Name cannot be longer than 255 characters.", counting characters,
# not bytes.
MAX_PIPELINE_NAME_CHARS = 255
PIPELINE_SPEC_FIELDS = frozenset(pipelines.PipelineSpec.__dataclass_fields__)


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
        self.create_log: Path | None = None
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
        job_id = self.add(jobs.JobSettings(**kwargs))
        if self.create_log is not None:
            with self.create_log.open("a") as f:
                f.write(f"{job_id}\n")
        return jobs.CreateResponse(job_id=job_id)

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
        self.delete_errors: dict[str, Exception] = {}
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
        """Delete `path`, unless it matches a glob in `delete_errors`, which raises that glob's error."""
        for pattern, error in self.delete_errors.items():
            if fnmatch.fnmatch(path, pattern):
                raise error
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


class FakeQueries:
    """Serves the saved queries in `queries`, by id, as `WorkspaceClient.queries` does."""

    def __init__(self) -> None:
        self.queries: dict[str, sql.Query] = {}
        self.get_calls: list[str] = []

    def get(self, id: str) -> sql.Query:
        self.get_calls.append(id)
        if id not in self.queries:
            raise NotFound(f"Query {id} does not exist.")
        return self.queries[id]


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


class FakeWorkspaceClient:
    def __init__(self) -> None:
        self.jobs = FakeJobs()
        self.api_client = FakeApiClient(self.jobs)
        self.workspace = FakeWorkspace()
        self.files = FakeFiles(self.workspace)
        self.schemas = FakeSchemas()
        self.queries = FakeQueries()
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
