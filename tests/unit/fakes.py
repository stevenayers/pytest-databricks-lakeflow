"""In-memory stand-ins for the parts of `WorkspaceClient` the plugin uses."""

from __future__ import annotations

import itertools
import json
from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from databricks.sdk.errors import NotFound
from databricks.sdk.service import iam, jobs

DATA = Path(__file__).parent / "data"
HOST = "https://example.cloud.databricks.com"
TERMINATED = jobs.RunLifecycleStateV2State.TERMINATED


class FakeJobs:
    def __init__(self) -> None:
        self.settings: dict[int, jobs.JobSettings] = {}
        self.created: list[dict[str, Any]] = []
        self.deleted: list[int] = []
        self.cancelled: list[int] = []
        self.run_now_calls: list[dict[str, Any]] = []
        self.listed: list[jobs.BaseJob] = []
        self.queued: list[list[jobs.Run]] = []
        self.polls: dict[int, list[jobs.Run]] = {}
        self.outputs: dict[int, jobs.RunOutput | Exception] = {}
        self.output_calls: list[int] = []
        self.delete_errors: dict[int, Exception] = {}
        self.list_calls = 0
        self._ids = itertools.count(1000)

    def add(self, settings: jobs.JobSettings) -> int:
        job_id = next(self._ids)
        self.settings[job_id] = settings
        return job_id

    def get(self, job_id: int) -> jobs.Job:
        if job_id not in self.settings:
            raise NotFound(f"Job {job_id} does not exist.")
        return jobs.Job(job_id=job_id, settings=self.settings[job_id])

    def create(self, **kwargs: Any) -> jobs.CreateResponse:
        self.created.append(kwargs)
        return jobs.CreateResponse(job_id=self.add(jobs.JobSettings(**kwargs)))

    def delete(self, job_id: int) -> None:
        if job_id in self.delete_errors:
            raise self.delete_errors[job_id]
        self.deleted.append(job_id)
        self.settings.pop(job_id, None)

    def queue(self, run: jobs.Run, outputs: Mapping[int, jobs.RunOutput] | None = None, polls: int = 1) -> None:
        """Make the next `run_now` return `run`, reported as running for `polls` calls to `get_run`."""
        status = jobs.RunStatus(state=jobs.RunLifecycleStateV2State.RUNNING)
        running = jobs.Run(run_id=run.run_id, run_page_url=run.run_page_url, status=status)
        self.queued.append([running] * polls + [run])
        self.outputs.update(outputs or {})

    def run_now(
        self, job_id: int, *, job_parameters: dict[str, str] | None = None, only: Sequence[str] | None = None
    ) -> SimpleNamespace:
        self.run_now_calls.append({"job_id": job_id, "job_parameters": job_parameters, "only": only})
        polls = self.queued.pop(0) if self.queued else [make_run(next(self._ids), {"task": "SUCCESS"})]
        run_id = polls[-1].run_id
        assert run_id is not None
        self.polls[run_id] = polls
        return SimpleNamespace(response=jobs.RunNowResponse(run_id=run_id))

    def get_run(self, run_id: int) -> jobs.Run:
        polls = self.polls[run_id]
        return polls.pop(0) if len(polls) > 1 else polls[0]

    def get_run_output(self, run_id: int) -> jobs.RunOutput:
        self.output_calls.append(run_id)
        output = self.outputs.get(run_id, jobs.RunOutput())
        if isinstance(output, Exception):
            raise output
        return output

    def cancel_run(self, run_id: int) -> None:
        self.cancelled.append(run_id)

    def list(self, **kwargs: Any) -> Iterator[jobs.BaseJob]:
        self.list_calls += 1
        return iter(self.listed)


class FakeWorkspace:
    def __init__(self) -> None:
        self.objects: set[str] = set()
        self.folders: list[str] = []
        self.uploads: dict[str, dict[str, Any]] = {}

    def mkdirs(self, path: str) -> None:
        self.folders.append(path)

    def upload(self, path: str, content: bytes, **kwargs: Any) -> None:
        parent = path.rsplit("/", 1)[0]
        if parent not in self.folders:
            raise NotFound(f"The parent folder ({parent}) does not exist.")
        self.uploads[path] = {"content": content.decode(), **kwargs}
        self.objects.add(path)

    def get_status(self, path: str) -> SimpleNamespace:
        if path not in self.objects:
            raise NotFound(f"Path ({path}) doesn't exist.")
        return SimpleNamespace(path=path)


class FakeFiles:
    def __init__(self, workspace: FakeWorkspace) -> None:
        self.workspace = workspace

    def get_metadata(self, path: str) -> SimpleNamespace:
        return self.workspace.get_status(path)


class FakeSchemas:
    def __init__(self) -> None:
        self.created: list[str] = []
        self.deleted: list[str] = []

    def create(self, name: str, catalog_name: str, **kwargs: Any) -> SimpleNamespace:
        self.created.append(f"{catalog_name}.{name}")
        return SimpleNamespace(full_name=f"{catalog_name}.{name}")

    def delete(self, full_name: str, *, force: bool | None = None) -> None:
        assert force
        self.deleted.append(full_name)


class FakeCurrentUser:
    def me(self) -> iam.User:
        return iam.User(user_name="someone@example.com")


class FakeWorkspaceClient:
    def __init__(self) -> None:
        self.jobs = FakeJobs()
        self.workspace = FakeWorkspace()
        self.files = FakeFiles(self.workspace)
        self.schemas = FakeSchemas()
        self.current_user = FakeCurrentUser()
        self.config = SimpleNamespace(host=HOST)


def recorded(name: str) -> tuple[jobs.Run, dict[int, jobs.RunOutput]]:
    """Load a run recorded during the spikes, with its task outputs by task run id."""
    data = json.loads((DATA / "runs" / f"{name}.json").read_text())
    outputs = {int(run_id): jobs.RunOutput.from_dict(output) for run_id, output in data["outputs"].items()}
    return jobs.Run.from_dict(data["run"]), outputs


def make_run(run_id: int, states: dict[str, str], result_state: str = "SUCCESS", code: str = "SUCCESS") -> jobs.Run:
    """Build a terminated run whose tasks ended in `states`, keyed by task key."""
    tasks = [
        jobs.RunTask(
            task_key=key,
            run_id=run_id * 100 + index,
            attempt_number=0,
            state=jobs.RunState(result_state=jobs.RunResultState(state)),
            status=jobs.RunStatus(state=TERMINATED, termination_details=_details(state)),
            run_page_url=f"{HOST}/jobs/runs/{run_id * 100 + index}",
            start_time=1_000,
            end_time=3_000,
        )
        for index, (key, state) in enumerate(states.items())
    ]
    return jobs.Run(
        run_id=run_id,
        state=jobs.RunState(result_state=jobs.RunResultState(result_state)),
        status=jobs.RunStatus(state=TERMINATED, termination_details=_details(code)),
        tasks=tasks,
        run_page_url=f"{HOST}/jobs/runs/{run_id}",
        start_time=1_000,
        end_time=6_000,
    )


def _details(state: str) -> jobs.TerminationDetails:
    codes = {
        "FAILED": "RUN_EXECUTION_ERROR",
        "UPSTREAM_FAILED": "SKIPPED",
        "CANCELED": "USER_CANCELED",
        "EXCLUDED": "SKIPPED",
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
