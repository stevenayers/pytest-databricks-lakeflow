"""In-memory stand-ins for the parts of `WorkspaceClient` the plugin uses."""

from __future__ import annotations

import base64
import itertools
import json
import re
import time
import urllib.parse
from collections.abc import Collection, Iterator, Mapping, Sequence
from dataclasses import replace
from datetime import UTC, datetime
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
# The one filter shape the plugin sends to `list_pipeline_events`. The platform refuses `level="ERROR"` with
# "Invalid filter expression"; the value must be in single quotes.
LEVEL_FILTER = re.compile(r"^level='(?P<level>[A-Z]+)'$")


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
        self.outputs: dict[int, jobs.RunOutput | Exception] = {}
        self.raw_outputs: dict[int, dict[str, Any] | Exception] = {}
        self.output_calls: list[int] = []
        self.resolved: dict[int, jobs.ResolvedValues | Exception] = {}
        self.get_run_calls: list[dict[str, Any]] = []
        self.exports: dict[int, str | Exception] = {}
        self.export_calls: list[dict[str, Any]] = []
        self.delete_errors: dict[int, Exception] = {}
        self.delete_log: Path | None = None
        self.list_calls = 0
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
        self.polls[run_id] = polls
        if idempotency_token is not None:
            self.tokens[idempotency_token] = run_id
        return SimpleNamespace(response=jobs.RunNowResponse(run_id=run_id))

    def add_run(self, run: jobs.Run) -> None:
        """Make `get_run` report `run`, a run started outside the plugin such as a Run Job task's child run."""
        assert run.run_id is not None
        self.polls[run.run_id] = [run]

    def get_run(self, run_id: int, **kwargs: Any) -> jobs.Run:
        self.get_run_calls.append({"run_id": run_id, **kwargs})
        if run_id in self.polls:
            polls = self.polls[run_id]
            return polls.pop(0) if len(polls) > 1 else polls[0]
        task = self._task_run(run_id)
        if task is None:
            raise NotFound(f"Run {run_id} does not exist.")
        # Like the Jobs API, a task run fetched by its own id is a run with that one task, and `resolved_values`
        # is filled only then and only with `include_resolved_values`. On a job run it is always null.
        resolved = self.resolved.get(run_id) if kwargs.get("include_resolved_values") else None
        if isinstance(resolved, Exception):
            raise resolved
        return jobs.Run(run_id=run_id, tasks=[replace(task, resolved_values=resolved)])

    def _task_run(self, run_id: int) -> jobs.RunTask | None:
        for polls in self.polls.values():
            for task in polls[-1].tasks or []:
                if task.run_id == run_id:
                    return task
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
        self.list_calls += 1
        return iter(self.listed)


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
        """List the objects in `listed` directly inside the folder `path`."""
        folder = path.rstrip("/")
        return iter([info for info in self.listed if (info.path or "").rsplit("/", 1)[0] == folder])

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
        self.deleted: list[str] = []
        self.listed: list[catalog.SchemaInfo] = []
        self.list_errors: dict[str, Exception] = {}

    def create(self, name: str, catalog_name: str, **kwargs: Any) -> SimpleNamespace:
        self.created.append(f"{catalog_name}.{name}")
        return SimpleNamespace(full_name=f"{catalog_name}.{name}")

    def delete(self, full_name: str, *, force: bool | None = None) -> None:
        assert force
        self.deleted.append(full_name)

    def list(self, catalog_name: str, **kwargs: Any) -> Iterator[catalog.SchemaInfo]:
        if catalog_name in self.list_errors:
            raise self.list_errors[catalog_name]
        return iter([info for info in self.listed if info.catalog_name == catalog_name])


class FakePipelines:
    """Stands in for `WorkspaceClient.pipelines`, serving updates and events by pipeline id and recording each call.

    `updates` holds the updates `list_updates` returns, or an error it raises. `events` holds the events
    `list_pipeline_events` yields, newest first as the platform returns them, applying a `level='...'` filter.
    `read_events` lists the id of every event yielded, so a test can tell how far a caller paged.
    """

    def __init__(self) -> None:
        self.updates: dict[str, list[pipelines.UpdateInfo] | Exception] = {}
        self.events: dict[str, list[pipelines.PipelineEvent]] = {}
        self.calls: list[dict[str, Any]] = []
        self.read_events: list[str | None] = []

    def list_updates(self, pipeline_id: str, **kwargs: Any) -> pipelines.ListUpdatesResponse:
        self.calls.append({"method": "list_updates", "pipeline_id": pipeline_id, **kwargs})
        updates = self.updates.get(pipeline_id, [])
        if isinstance(updates, Exception):
            raise updates
        return pipelines.ListUpdatesResponse(updates=list(updates))

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
        self.pipelines = FakePipelines()
        self.current_user = FakeCurrentUser()
        self.account_access_control_proxy = FakeAccountAccessControlProxy()
        self.config = SimpleNamespace(host=HOST, profile=None, account_id=ACCOUNT_ID)
        self.profiles: list[str | None] = []


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
    job_id: int, age_hours: float, tags: dict[str, str], creator: str = "someone@example.com"
) -> jobs.BaseJob:
    """Build a job as `jobs.list` returns it, created `age_hours` ago by `creator`."""
    created = int(time.time() * 1000 - age_hours * HOUR_MS)
    return jobs.BaseJob(
        job_id=job_id, created_time=created, creator_user_name=creator, settings=jobs.JobSettings(tags=tags)
    )


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
    """Load a run recorded during the spikes, with its task outputs by task run id."""
    data = json.loads((DATA / "runs" / f"{name}.json").read_text())
    outputs = {int(run_id): jobs.RunOutput.from_dict(output) for run_id, output in data["outputs"].items()}
    return jobs.Run.from_dict(data["run"]), outputs


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
                )
            )
    return jobs.Run(
        run_id=run_id,
        state=jobs.RunState(result_state=jobs.RunResultState(result_state)),
        status=jobs.RunStatus(state=TERMINATED, termination_details=_details(code)),
        tasks=tasks,
        run_page_url=f"{HOST}/jobs/runs/{run_id}",
        start_time=1_000,
        end_time=6_000,
    )


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
    return jobs.Run(run_id=run_id, status=status, tasks=tasks, run_page_url=f"{HOST}/jobs/runs/{run_id}")


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
