"""A throwaway copy of a bundle job, and the handle of a run it started."""

from __future__ import annotations

import logging
import shutil
import tempfile
import time
import uuid
import warnings
from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import timedelta
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast, get_args

import pytest
from databricks.sdk.errors import PermissionDenied
from databricks.sdk.errors.base import DatabricksError
from databricks.sdk.service import jobs

from pytest_databricks_lakeflow import deployment, helpers, session, steps, workspace_api
from pytest_databricks_lakeflow.ephemeral.base import EphemeralResource
from pytest_databricks_lakeflow.ephemeral.outcome import (
    DEBUG_NOTEBOOKS,
    PROGRESS,
    RUNS,
    emit,
    record_outcome,
)
from pytest_databricks_lakeflow.ephemeral.pipeline import EphemeralPipeline, close_all
from pytest_databricks_lakeflow.ephemeral.polling import (
    LoggedStates,
    poll_seconds,
    task_state,
    task_states,
    terminated,
    transition,
)
from pytest_databricks_lakeflow.errors import LakeflowError, LakeflowTimeout
from pytest_databricks_lakeflow.limits import TIMEOUT_GRACE_SECONDS, WIDGET_LIMIT_BYTES
from pytest_databricks_lakeflow.naming import DEBUG_FOLDER, PREFIX, TEARDOWN_KEY
from pytest_databricks_lakeflow.results.build import build_job_run, fetch_outputs, has_output
from pytest_databricks_lakeflow.results.models import DebugNotebook, JobRun, StepResult, step_outcome
from pytest_databricks_lakeflow.settings.compute import check_compute, unreached_notebook_tasks
from pytest_databricks_lakeflow.settings.lifecycle import name_lifecycle_params, rewire_teardown
from pytest_databricks_lakeflow.settings.params import check_params, pipeline_params
from pytest_databricks_lakeflow.settings.selection import (
    check_disabled,
    check_output_references,
    check_references,
    check_trigger_references,
    select,
    selected_tasks,
)
from pytest_databricks_lakeflow.settings.stubs import stub_upstream, upstream_values_from
from pytest_databricks_lakeflow.settings.tasks import (
    disabled_task_keys,
    pipeline_task_ids,
    require_task,
    rewire_pipelines,
)
from pytest_databricks_lakeflow.settings.template import create_kwargs, job_timeout_seconds
from pytest_databricks_lakeflow.workspace_api import GRANT_ROLE, cancel_run

if TYPE_CHECKING:
    from pytest_databricks_lakeflow.client import Lakeflow
    from pytest_databricks_lakeflow.steps import LocalStep

log = logging.getLogger("pytest_databricks_lakeflow")
ROLE_FIX = f"{GRANT_ROLE}, or leave run_as out to run the copy as yourself."
TeardownAt = Literal["run", "close"]
TEARDOWN_AT: tuple[str, ...] = get_args(TeardownAt)


@dataclass
class _Polling:
    """What polling one run has seen: the task states logged, the outputs read, and whether the plugin cancelled it.

    With `stream`, each task state change is also written to the terminal.
    """

    run_id: int
    item: pytest.Item
    stream: bool = False
    logged: LoggedStates = field(default_factory=dict)
    outputs: dict[int, jobs.RunOutput] = field(default_factory=dict)
    cancelled: bool = False


class EphemeralJob(EphemeralResource):
    """A throwaway copy of a bundle job.

    The job is created on entering the `with` block, or by the first `run()` or `start()` when used without `with`.
    It is deleted on leaving the block, or at the end of the test that made it, unless `keep` is set or the test
    failed and `keep_failed` is on. Runs that are still active then are cancelled first. With
    `teardown_at="close"`, the teardown task then runs alone, before the job is deleted or kept.

    Attributes:
        lakeflow: The `Lakeflow` that made the job.
        job_key: The bundle resource key of the job it copies.
        settings: The settings the job is created with.
        params: Job parameters for every run.
        capture_task_values: The task value keys captured by probes, by task key.
        stubs: Keys of the stub tasks.
        extra_dependencies: The test-only dependencies passed to `job()` or `task()`, local wheels resolved to their
            uploaded copies.
        pipeline_copies: The pipeline copies that `pipelines="copy"` made, created just before the job and closed
            just after it. They are chosen again when the job is created, after `modify_settings`, so a pipeline
            task that it repoints or adds gets a copy too.
        teardown_at: When the teardown task runs. `"run"` runs it at the end of every run. `"close"` leaves it out
            of every run and runs it alone when the job closes, so the test's assertions still see what it removes.
        job_id: The job's id, once created.
        runs: The outcome of each finished run, in order. The teardown run that closing the job starts is not one.
        query_files: The SQL text of each saved query that `saved_queries="inline"` inlined, by the workspace path of
            its file. They are uploaded just before the job is created and deleted with it, or kept with it.
        local_steps: The `setup=` and `teardown=` steps that run over Databricks Connect, by role, rather than as
            tasks of the copy.
        teardown: The outcome of the teardown that ran when the job closed, with `teardown_at="close"`. `None` until
            then, and when the job never ran, `keep` or `--lakeflow-keep` kept the job, or a teardown task's run did
            not finish.
    """

    def __init__(
        self,
        lakeflow: Lakeflow,
        job_key: str,
        settings: jobs.JobSettings,
        params: Mapping[str, str] | None = None,
        capture_task_values: Mapping[str, Sequence[str]] | None = None,
        stubs: Collection[str] = (),
        *,
        extra_dependencies: Sequence[str] = (),
        pipeline_copies: Sequence[EphemeralPipeline] = (),
        teardown_at: TeardownAt = "run",
        query_files: Mapping[str, str] | None = None,
        local_steps: Mapping[str, LocalStep] | None = None,
    ) -> None:
        super().__init__(lakeflow)
        self.job_key = job_key
        self.settings = settings
        self.params = dict(params or {})
        self.capture_task_values = {key: list(keys) for key, keys in (capture_task_values or {}).items()}
        self.stubs = frozenset(stubs)
        self.extra_dependencies = list(extra_dependencies)
        self.pipeline_copies = list(pipeline_copies)
        self.teardown_at = teardown_at
        self.query_files = dict(query_files or {})
        self._query_files_uploaded = False
        # Set by `pipelines="copy"`: copies the pipeline with the given id, once modify_settings has run.
        self._copy_pipeline: Callable[[str], EphemeralPipeline] | None = None
        self.job_id: int | None = None
        self.runs: list[JobRun] = []
        self.local_steps = dict(local_steps or {})
        self.teardown: StepResult | None = None
        self.ran = False
        self._run_urls: dict[int, str] = {}
        self._active: dict[int, pytest.Item] = {}
        # The cap whose slots this job's runs hold, under --lakeflow-max-concurrent-runs or max_concurrent_runs.
        self._slots: session.RunSlots | None = None
        self._timeout_raised = False

    @property
    def url(self) -> str | None:
        """The job's page in the workspace, or `None` before it is created."""
        if self.job_id is None:
            return None
        return f"{self.lakeflow.ws.config.host}/jobs/{self.job_id}"

    def create(self) -> None:
        """Create the job if it does not exist yet, after the pipeline copies its pipeline tasks point at.

        Emits a `UserWarning` naming the notebook tasks that `extra_dependencies` does not reach, checked after
        `modify_settings`, before `jobs.create`. `modify_settings` sees the deployed pipelines' ids, which are replaced
        by the copies' ids after it. With `pipelines="copy"`, the pipelines to copy are chosen after it too.

        Raises:
            LakeflowError: If the job has been closed, `params` names a parameter that the job does not
                declare after `modify_settings` has run, a serverless task that needs an environment has none, a
                task refers to the output of a notebook task, or a task or job parameter default refers to a table
                update or file arrival trigger that the copy does not have, which `jobs.create` refuses.
                Also if the job runs as a service principal and you do not have the Service Principal User role on
                it, or `jobs.create` refuses the job with `PERMISSION_DENIED`, with the platform's error as its
                cause. Without a service principal in `run_as`, errors from `jobs.create` are raised unchanged.
        """
        if self._closed:
            raise LakeflowError(f"The ephemeral job for {self.job_key!r} has already been closed")
        if self.job_id is not None:
            return
        item = session.current_item()
        if self.lakeflow.modify_settings is not None:
            self.lakeflow.modify_settings(self.settings, item)
            name_lifecycle_params(self.settings, self.params)
        check_params(self.job_key, self.settings, self.params, self.lakeflow.allow_undeclared_params)
        check_compute(self.settings)
        check_output_references(self.job_key, self.settings)
        check_trigger_references(self.job_key, self.settings)
        self._warn_unreached_dependencies()
        self._create_pipelines()
        self._upload_query_files()
        self.job_id = self._create_job()
        log.info(f"Created job {self.job_id} from {self.job_key}: {self.url}")
        item.user_properties.append(("lakeflow_job_url", self.url))
        item.user_properties.append(("lakeflow_job_id", self.job_id))

    def _warn_unreached_dependencies(self) -> None:
        unreached = unreached_notebook_tasks(self.settings, self.extra_dependencies, self.stubs)
        if not unreached:
            return
        warnings.warn(
            f"extra_dependencies {self.extra_dependencies} do not reach tasks {unreached} of job {self.job_key!r}: "
            "neither the environment nor the cluster libraries of these notebook tasks list them, and a serverless "
            "notebook task without an environment_key has neither. Install them with %pip install in the notebook, "
            "or run the task on an environment that lists them.",
            stacklevel=4,
        )

    def _create_pipelines(self) -> None:
        """Create the pipeline copies and point the job's pipeline tasks at them."""
        if self._copy_pipeline is not None:
            self.pipeline_copies = self._chosen_pipelines(self._copy_pipeline)
            # Chosen once: a second create() after a failed one finds the tasks on the copies already.
            self._copy_pipeline = None
        copies: dict[str, str] = {}
        for copy in self.pipeline_copies:
            copy.create()
            assert copy.pipeline_id is not None
            copies[copy.source_id] = copy.pipeline_id
        rewire_pipelines(self.settings, copies)

    def _chosen_pipelines(self, copy_pipeline: Callable[[str], EphemeralPipeline]) -> list[EphemeralPipeline]:
        """Copy each pipeline that an enabled pipeline task updates after `modify_settings`, reusing the copies made.

        A copy that `job()` made for a pipeline that no task updates any more is dropped, unless it has been created.
        """
        made = {copy.source_id: copy for copy in self.pipeline_copies}
        chosen: list[EphemeralPipeline] = []
        for pipeline_id in pipeline_task_ids(self.settings):
            if pipeline_id in made:
                chosen.append(made.pop(pipeline_id))
            else:
                chosen.append(copy_pipeline(pipeline_id))
        for copy in made.values():
            if copy.pipeline_id is not None:
                chosen.append(copy)
        return chosen

    def _upload_query_files(self) -> None:
        if not self.query_files:
            return
        # Set first, so that closing the job deletes the files a failed upload left behind.
        self._query_files_uploaded = True
        for path, source in self.query_files.items():
            helpers.upload_query_file(self.lakeflow.ws, path, source)

    def _create_job(self) -> int | None:
        """Create the job, checking first that its creator may make it run as the service principal it names."""
        ws = self.lakeflow.ws
        acl = self.lakeflow.access_control_list()
        application_id = None
        if self.settings.run_as is not None:
            application_id = self.settings.run_as.service_principal_name
        if application_id and self._has_sp_user_role(application_id) is False:
            raise LakeflowError(
                f"{self._creator()} does not have the Service Principal User role on {application_id}, so the copy "
                f"of {self.job_key!r} cannot run as it. {ROLE_FIX}"
            )
        try:
            return ws.jobs.create(**create_kwargs(self.settings), access_control_list=acl).job_id
        except PermissionDenied as e:
            if not application_id:
                raise
            raise LakeflowError(
                f"Creating the copy of {self.job_key!r} with run_as {application_id} was refused: {e} To create a "
                f"job that runs as a service principal, {self._creator()} needs the Service Principal User role on "
                f"it. {ROLE_FIX}"
            ) from e

    def _has_sp_user_role(self, application_id: str) -> bool | None:
        """`workspace_api.check_sp_user_role` for the creator, read once per session for each service principal."""
        ws = self.lakeflow.ws
        roles = session.current().sp_user_roles
        key = (str(ws.config.host), self._creator(), application_id)
        if key not in roles:
            roles[key] = workspace_api.check_sp_user_role(ws, application_id)
        return roles[key]

    def _creator(self) -> str:
        return self.lakeflow._state()["user"]

    def run(
        self,
        params: Mapping[str, str] | None = None,
        only: Sequence[str] | None = None,
        *,
        timeout: timedelta | None = None,
        stop_after: timedelta | None = None,
        performance_target: jobs.PerformanceTarget | str | None = None,
        queue: bool | None = None,
        full_refresh: bool | None = None,
        full_refresh_selection: Sequence[str] | None = None,
        refresh_selection: Sequence[str] | None = None,
        refresh_flow_selection: Sequence[str] | None = None,
        reset_checkpoint_selection: Sequence[str] | None = None,
        replay: JobRun | None = None,
        stream: bool | None = None,
        dbt_commands: Sequence[str] | None = None,
    ) -> JobRun:
        """Run the job and wait for it to finish, creating it first if needed.

        Every call sends a fresh idempotency token with the run request, so a request the SDK retries cannot
        start a second run. It is `start(params, only, timeout=timeout, ...).wait(stop_after=stop_after)`.

        Args:
            params: Job parameters for this run only, merged over the job's params.
            only: Task keys to run. `+key`, `key+` and `+key+` add upstream and downstream tasks.
            timeout: How long to wait. Defaults to `Lakeflow`'s `timeout`. When this plus 60 seconds is longer than
                the job's `timeout_seconds`, which `Lakeflow`'s `timeout` sets, the job's `timeout_seconds` is
                raised to it before the run starts, with a warning when the template or `modify_settings` set the
                shorter value.
            stop_after: How long the run may go on before it is cancelled, for a job that does not end on its own,
                such as a streaming job. The run is then polled until it has ended and returned as a `JobRun` whose
                `cancelled` is true, instead of raising. It counts from the start of the run and must not be longer
                than `timeout`.
            performance_target: The performance target of this run only, such as `"PERFORMANCE_OPTIMIZED"`. It
                applies to serverless tasks and leaves the job's settings unchanged. Defaults to the job's own.
            queue: Whether this run may queue behind other runs. Defaults to `Lakeflow`'s `queue`, and when that is
                `None` to the job's own queue setting.
            full_refresh: Whether the job's pipeline tasks run a full refresh in this run. Defaults to what each
                `pipeline_task` sets. The pipeline is the deployed one, unless the job was made with
                `pipelines="copy"`.
            full_refresh_selection: Tables that the job's pipeline tasks fully refresh in this run. Needs
                databricks-sdk 0.110.0 or later.
            refresh_selection: Tables that the job's pipeline tasks refresh in this run. It applies to every
                pipeline task in the run. Needs databricks-sdk 0.110.0 or later.
            refresh_flow_selection: Flows that the job's pipeline tasks refresh in this run, by flow name. Needs
                databricks-sdk 0.110.0 or later.
            reset_checkpoint_selection: Streaming flows whose checkpoints the job's pipeline tasks reset in this run,
                so that they process their source again from the start. Needs databricks-sdk 0.110.0 or later.
            replay: A run of the same job whose captured task values stub the tasks that `only` leaves out and a
                selected task depends on, as `upstream_task_values` on `job()` does. The platform returns no task
                values with a run, so that run must have captured them, with
                `capture_task_values={"<upstream key>": [...]}` on `job()`. The stubs are part of the job's settings,
                so `replay` needs `only` and a job that has not been created yet: pass it to the first run of a new
                `lakeflow.job(...)`.
            stream: Whether to print the start of the run, each task state change and the end of the run to the
                terminal while waiting, as `lakeflow: <line>`, whatever the output capture and `--log-cli-level`.
                Defaults to whether `--lakeflow-live` is set. An xdist worker cannot write to the terminal, so there
                the lines are only logged.
            dbt_commands: The commands of every dbt task in this run, such as `["dbt deps", "dbt build --select
                tag:ci"]`, in place of each task's own `commands`, which the job's settings keep. `TaskRun.parameters`
                of each dbt task shows them. The Jobs API refuses them on a job with job parameters, including the
                ones the plugin declares for `test_marker`, `trigger=` and `input=`. To change the commands of one dbt
                task in the copy, pass `task_params` to `job()` or `task()` instead.

        Returns:
            The run's outcome.

        Raises:
            ValueError: If `performance_target` is not a known performance target, `stop_after` is longer than
                `timeout`, `dbt_commands` is empty or a string, or `replay` is passed without `only` or is a run of
                another job.
            LakeflowError: If the job has been closed, a parameter is not declared, `only` names an unknown
                task or a task that is disabled in the copy or downstream of a disabled task, which the platform
                would run, a selected task refers to the task values or the output of a task that `only` leaves out
                and no stub replaces, a pipeline refresh option is set and the job has no pipeline task, a selection
                is set and the installed databricks-sdk cannot send it, `replay` is passed to a job that has been
                created or lacks the captured values of a task it would stub, or `dbt_commands` is passed to a job
                with job parameters. Each is raised before the run starts. Also if no run ends within `timeout`
                while the process has as many runs active as `max_concurrent_runs` allows, as for `start()`.
            LakeflowTimeout: If the run did not finish in time. The run is cancelled first.
        """
        if stop_after is not None:
            limit = timeout
            if limit is None:
                limit = self.lakeflow.timeout
            _check_stop_after(self.job_key, stop_after, limit)
        if replay is not None:
            self._replay(replay, only)
        handle = self._start(
            params,
            only,
            timeout=timeout,
            performance_target=performance_target,
            queue=queue,
            full_refresh=full_refresh,
            full_refresh_selection=full_refresh_selection,
            refresh_selection=refresh_selection,
            refresh_flow_selection=refresh_flow_selection,
            reset_checkpoint_selection=reset_checkpoint_selection,
            stream=stream,
            dbt_commands=dbt_commands,
        )
        return handle.wait(stop_after=stop_after)

    def start(
        self,
        params: Mapping[str, str] | None = None,
        only: Sequence[str] | None = None,
        *,
        timeout: timedelta | None = None,
        performance_target: jobs.PerformanceTarget | str | None = None,
        queue: bool | None = None,
        full_refresh: bool | None = None,
        full_refresh_selection: Sequence[str] | None = None,
        refresh_selection: Sequence[str] | None = None,
        refresh_flow_selection: Sequence[str] | None = None,
        reset_checkpoint_selection: Sequence[str] | None = None,
        dbt_commands: Sequence[str] | None = None,
    ) -> RunHandle:
        """Start a run of the job without waiting for it, creating the job first if needed.

        The run is cancelled when the job closes, unless `RunHandle.wait()` has returned by then. Its page is
        recorded in the test's report straight away, so a run that is never waited for still appears there, and
        with `--lakeflow-report-dir` the cancelled run is listed in report.json. The job allows one run at a time,
        so start runs of different jobs to run them side by side.

        Under `--lakeflow-max-concurrent-runs` or `Lakeflow`'s `max_concurrent_runs`, when the process already has
        that many runs active, it waits for one of them to end before starting the run, for at most `timeout`. While
        it waits it polls those runs, so a run that has ended frees its slot even if nobody has waited for it.

        Args:
            params: Job parameters for this run only, as for `run()`.
            only: Task keys to run, as for `run()`.
            timeout: How long `RunHandle.wait()` waits when it is given no timeout of its own. Defaults to
                `Lakeflow`'s `timeout`. As for `run()`, when this plus 60 seconds is longer than the job's
                `timeout_seconds`, the job's `timeout_seconds` is raised to it before the run starts.
            performance_target: The performance target of this run only, as for `run()`.
            queue: Whether this run may queue behind other runs, as for `run()`.
            full_refresh: Whether the job's pipeline tasks run a full refresh in this run, as for `run()`.
            full_refresh_selection: Tables that the job's pipeline tasks fully refresh in this run, as for `run()`.
            refresh_selection: Tables that the job's pipeline tasks refresh in this run, as for `run()`.
            refresh_flow_selection: Flows that the job's pipeline tasks refresh in this run, as for `run()`.
            reset_checkpoint_selection: Streaming flows whose checkpoints the job's pipeline tasks reset in this run,
                as for `run()`.
            dbt_commands: The commands of every dbt task in this run, as for `run()`.

        Returns:
            A handle to wait for the run or cancel it.

        Raises:
            ValueError: If `performance_target` is not a known performance target, or `dbt_commands` is empty or a
                string.
            LakeflowError: If the job has been closed, a parameter is not declared, `only` names an unknown
                task or a task that is disabled in the copy or downstream of a disabled task, which the platform
                would run, a selected task refers to the task values or the output of a task that `only` leaves out
                and no stub replaces, a pipeline refresh option is set and the job has no pipeline task, a selection
                is set and the installed databricks-sdk cannot send it, or `dbt_commands` is passed to a job with
                job parameters. Also if no run ends within `timeout` while the process has as many runs active as
                `max_concurrent_runs` allows.
        """
        return self._start(
            params,
            only,
            timeout=timeout,
            performance_target=performance_target,
            queue=queue,
            full_refresh=full_refresh,
            full_refresh_selection=full_refresh_selection,
            refresh_selection=refresh_selection,
            refresh_flow_selection=refresh_flow_selection,
            reset_checkpoint_selection=reset_checkpoint_selection,
            stream=None,
            dbt_commands=dbt_commands,
        )

    def _start(
        self,
        params: Mapping[str, str] | None,
        only: Sequence[str] | None,
        *,
        timeout: timedelta | None,
        performance_target: jobs.PerformanceTarget | str | None,
        queue: bool | None,
        full_refresh: bool | None,
        full_refresh_selection: Sequence[str] | None,
        refresh_selection: Sequence[str] | None,
        refresh_flow_selection: Sequence[str] | None,
        reset_checkpoint_selection: Sequence[str] | None,
        stream: bool | None,
        dbt_commands: Sequence[str] | None,
    ) -> RunHandle:
        """Start a run as `start()` does, with the handle streaming as `stream` says, `None` following the flag."""
        commands = _dbt_commands(dbt_commands)
        target = _performance_target(performance_target)
        if queue is None:
            queue = self.lakeflow.queue
        queue_settings = None if queue is None else jobs.QueueSettings(enabled=queue)
        self.create()
        assert self.job_id is not None
        if commands is not None:
            self._check_dbt_commands_allowed()
        item = session.current_item()
        run_params = dict(params or {})
        check_params(self.job_key, self.settings, run_params, self.lakeflow.allow_undeclared_params)
        refresh = pipeline_params(
            self.settings,
            full_refresh=full_refresh,
            full_refresh_selection=full_refresh_selection,
            refresh_selection=refresh_selection,
            refresh_flow_selection=refresh_flow_selection,
            reset_checkpoint_selection=reset_checkpoint_selection,
        )
        merged = {**self.params, **run_params}
        selected = self._selection(only)
        if timeout is not None:
            self._raise_timeout(timeout)
        setup = None
        if "setup" in self.local_steps:
            setup = self._run_local("setup", merged)
            if not setup.succeeded:
                raise LakeflowError(f"setup= of {self.job_key!r} failed before the run started: {step_outcome(setup)}")
        run_id, first = self._start_run(
            item,
            merged,
            selected,
            performance_target=target,
            queue=queue_settings,
            pipeline_params=refresh,
            timeout=timeout,
            dbt_commands=commands,
        )
        return RunHandle(
            self, run_id, item, first, timeout, stream=stream, performance_target=target, setup=setup, params=merged
        )

    def _check_dbt_commands_allowed(self) -> None:
        # run_now refuses dbt_commands, a legacy run parameter, on a job that declares job parameters, with
        # InvalidParameterValue "Cannot use legacy parameters (...) because the job has job parameters configured."
        names = [parameter.name for parameter in self.settings.parameters or []]
        if not names:
            return
        raise LakeflowError(
            f"dbt_commands cannot be passed to a run of {self.job_key!r}, because the job has job parameters {names}, "
            "and the Jobs API then refuses them. Pass task_params={'<dbt task key>': [...]} to job() or task() "
            "instead, which sets the commands of one dbt task in the copy."
        )

    def _selection(self, only: Sequence[str] | None) -> list[str] | None:
        """The `only` of a run: what `only` selects and the injected tasks it needs, less a teardown left to close."""
        exclude: set[str] = set()
        if self.teardown_at == "close" and "teardown" not in self.local_steps:
            exclude.add(TEARDOWN_KEY)
        if only:
            check_disabled(self.settings, only)
            check_references(self.settings, selected_tasks(self.settings, only, self.stubs), self.stubs)
            return select(self.settings, only, self.stubs, exclude)
        if not exclude:
            return None
        # The platform runs a disabled task that only names, and a task whose upstream only leaves out, so a full run
        # leaves out the disabled tasks and those downstream of them, which it would not run.
        exclude |= disabled_task_keys(self.settings)
        keys: list[str] = []
        for task in self.settings.tasks or []:
            if task.task_key not in exclude:
                keys.append(task.task_key)
        return keys

    def _start_run(
        self,
        item: pytest.Item,
        params: Mapping[str, str],
        only: list[str] | None,
        *,
        performance_target: jobs.PerformanceTarget | None = None,
        queue: jobs.QueueSettings | None = None,
        pipeline_params: jobs.PipelineParams | None = None,
        timeout: timedelta | None = None,
        dbt_commands: list[str] | None = None,
    ) -> tuple[int, jobs.Run]:
        """Start a run with a fresh idempotency token, track it until it ends, and return its id and first poll.

        Under a cap on active runs, a slot is taken first, waiting up to `timeout`, by default `Lakeflow`'s.
        """
        assert self.job_id is not None
        ws = self.lakeflow.ws
        if timeout is None:
            timeout = self.lakeflow.timeout
        slots = self._take_slot(timeout)
        token = uuid.uuid4().hex
        try:
            waiter = ws.jobs.run_now(
                job_id=self.job_id,
                idempotency_token=token,
                job_parameters=dict(params) or None,
                only=only,
                performance_target=performance_target,
                queue=queue,
                pipeline_params=pipeline_params,
                dbt_commands=dbt_commands,
            )
        except BaseException:
            if slots is not None:
                slots.semaphore.release()
            raise
        run_id = cast(jobs.RunNowResponse, waiter.response).run_id
        assert run_id is not None
        if slots is not None:
            slots.hold(run_id, self)
            self._slots = slots
        self._active[run_id] = item
        self.lakeflow.active_runs.add(run_id)
        self.ran = True
        log.info(f"Started run {run_id} of job {self.job_id}")
        log.debug(f"Run {run_id} was started with idempotency token {token}")
        first = ws.jobs.get_run(run_id)
        self._record_url(run_id, first, item)
        return run_id, first

    def _take_slot(self, timeout: timedelta) -> session.RunSlots | None:
        """Take a slot under the cap on this process's active runs, waiting up to `timeout`. `None` without a cap.

        While every slot is held, the runs holding them are polled every poll interval and the slot of each run that
        has ended is given back, so that runs started one after another in one thread do not wait for a `wait()`.

        Raises:
            LakeflowError: If no slot frees within `timeout`.
        """
        slots = session.current().run_slots(self.lakeflow.max_concurrent_runs)
        if slots is None:
            return None
        if slots.semaphore.acquire(blocking=False):
            return slots
        log.info(
            f"Waiting to start a run of job {self.job_id} until one of the active runs {sorted(slots.holders)} ends, "
            f"under max_concurrent_runs={slots.cap}"
        )
        deadline = time.monotonic() + timeout.total_seconds()
        interval = poll_seconds(self.lakeflow)
        while True:
            _release_ended(slots)
            if slots.semaphore.acquire(timeout=min(interval, max(deadline - time.monotonic(), 0.0))):
                return slots
            if time.monotonic() >= deadline:
                break
        runs = "run" if slots.cap == 1 else "runs"
        raise LakeflowError(
            f"No run slot freed within {timeout} to start a run of {self.job_key!r}: this process may have "
            f"{slots.cap} {runs} active at once (--lakeflow-max-concurrent-runs or max_concurrent_runs), and runs "
            f"{sorted(slots.holders)} are still active. Wait for or cancel one of them first, or pass a longer timeout."
        )

    def _replay(self, run: JobRun, only: Sequence[str] | None) -> None:
        """Stub the tasks that `only` leaves out and a selected task depends on, with their values in `run`."""
        if self.job_id is not None:
            raise LakeflowError(
                "replay stubs tasks in the job's settings, so it must be passed before the job is created, and job "
                f"{self.job_id} of {self.job_key!r} already exists. Pass replay to the first run of a new "
                "lakeflow.job(...)."
            )
        if not only:
            raise ValueError("replay stubs the tasks that only leaves out, so it needs only")
        selected = selected_tasks(self.settings, only, self.stubs)
        explicit = selected | self.stubs
        for task in self.settings.tasks or []:
            if task.task_key.startswith(PREFIX):
                explicit.add(task.task_key)
        upstream: dict[str, dict[str, object]] = {}
        for task_key in sorted(selected - self.stubs):
            # `key+` selects the teardown task, which depends on every leaf; injected tasks read no task values.
            if task_key.startswith(PREFIX):
                continue
            values = upstream_values_from(self.job_key, self.settings, task_key, run, explicit, argument="replay")
            upstream.update(values)
        if not upstream:
            return
        helpers = self.lakeflow._helpers()
        if helpers.job_cluster_key and self.lakeflow.existing_cluster_id:
            # sanitise has already moved the job clusters to the existing cluster, so the helper cluster is gone.
            raise LakeflowError(
                "replay cannot add stubs when helpers_compute and existing_cluster_id are both set. Pass the "
                "values in upstream_task_values on job() instead."
            )
        stub_upstream(self.settings, upstream, helpers)
        # The stubs drop their depends_on after inject_lifecycle chose the teardown's, so it is chosen again.
        rewire_teardown(self.settings)
        for key in upstream:
            stub = require_task(self.settings, key)
            # sanitise has already run, so the stubs get what it gives serverless helper tasks.
            if self.lakeflow.disable_auto_optimization and not stub.job_cluster_key:
                stub.disable_auto_optimization = True
        self.stubs = self.stubs | upstream.keys()

    def _raise_timeout(self, timeout: timedelta) -> None:
        """Raise the job's `timeout_seconds` to `timeout` plus the grace period when it is shorter."""
        limit = self.settings.timeout_seconds
        needed = job_timeout_seconds(timeout)
        if not limit or needed <= limit:
            return
        assert self.job_id is not None
        # Without tasks in the body, jobs/update reads a one-task job in the legacy single-task format and sets
        # timeout_seconds on the task, leaving the job's own unchanged. Sending the tasks the job was created with
        # changes only the job's.
        changes = jobs.JobSettings(timeout_seconds=needed, tasks=self.settings.tasks)
        self.lakeflow.ws.jobs.update(self.job_id, new_settings=changes)
        level = logging.INFO
        source = ""
        # Any other value than the one Lakeflow's timeout sets came from the template or modify_settings.
        if limit != job_timeout_seconds(self.lakeflow.timeout) and not self._timeout_raised:
            level = logging.WARNING
            source = ", which the template or modify_settings set,"
        log.log(
            level,
            f"Raised timeout_seconds of job {self.job_id} from {limit}{source} to {needed} for a run timeout of "
            f"{timeout}",
        )
        self.settings.timeout_seconds = needed
        self._timeout_raised = True

    def _check_timeout(self, timeout: timedelta) -> None:
        limit = self.settings.timeout_seconds
        if limit and timeout.total_seconds() > limit:
            raise LakeflowError(
                f"wait(timeout={timeout}) of {self.job_key!r} is longer than the job's timeout_seconds of {limit} "
                f"({timedelta(seconds=limit)}), so the platform would stop the run first. Pass the timeout to "
                "run(timeout=...) or start(timeout=...), which raise the job's timeout before the run starts, or a "
                "shorter wait timeout."
            )

    def _wait(
        self,
        run_id: int,
        timeout: timedelta,
        item: pytest.Item,
        first: jobs.Run | None = None,
        first_at: float = 0.0,
        stop_after: timedelta | None = None,
        stream: bool = False,
    ) -> tuple[jobs.Run, dict[int, jobs.RunOutput]]:
        """Poll the run until it terminates, starting from `first`, the poll that `start()` made at `first_at`.

        With `stop_after`, the run is cancelled once `stop_after` has passed since `first_at` and polled until it
        terminates. When `timeout` expires first, the run is cancelled, unless `stop_after` already did that, reported
        with `--lakeflow-report-dir`, and `LakeflowTimeout` is raised. With `stream`, each task state change is also
        written to the terminal.
        """
        deadline = time.monotonic() + timeout.total_seconds()
        stop_at = None
        if stop_after is not None:
            stop_at = first_at + stop_after.total_seconds()
        # A snapshot older than one poll interval is polled again at once, without a sleep or the deadline check, so
        # a wait() long after start() neither sleeps on it nor times out a run that has finished since.
        stale = first is not None and time.monotonic() - first_at >= poll_seconds(self.lakeflow)
        polling = _Polling(run_id, item, stream)
        run = self._poll(polling, deadline, first, stale, stop_at)
        if terminated(run):
            return run, polling.outputs
        if not polling.cancelled:
            cancel_run(self.lakeflow.ws, run_id)
        self._forget(run_id)
        message = (
            f"Run {run_id} of {self.job_key!r} did not finish within {timeout} and was cancelled: "
            f"{run.run_page_url}\nTasks when cancelled: {task_states(run)}"
        )
        self._report_cancelled(polling)
        raise LakeflowTimeout(message)

    def _poll(
        self,
        polling: _Polling,
        deadline: float,
        first: jobs.Run | None = None,
        stale: bool = False,
        stop_at: float | None = None,
    ) -> jobs.Run:
        """Poll the run until it terminates or `deadline` passes, and return the last snapshot.

        The run is cancelled once `stop_at` passes, and polled on until it terminates. Like the deadline, `stop_at` is
        checked only after a fresh poll, so a `stale` snapshot is polled again first.
        """
        ws = self.lakeflow.ws
        interval = poll_seconds(self.lakeflow)
        progress = polling.item.stash.setdefault(PROGRESS, [])
        run = first
        if run is None:
            run = ws.jobs.get_run(polling.run_id)
        while True:
            self._record_url(polling.run_id, run, polling.item)
            self._log_progress(run, polling, progress)
            if terminated(run):
                return run
            if not stale:
                now = time.monotonic()
                if stop_at is not None and now >= stop_at and not polling.cancelled:
                    # The run stays active until it has terminated, so a job that closes meanwhile still cancels it.
                    cancel_run(ws, polling.run_id)
                    polling.cancelled = True
                if now >= deadline:
                    return run
                time.sleep(interval)
            stale = False
            run = ws.jobs.get_run(polling.run_id)

    def _report_cancelled(self, polling: _Polling) -> None:
        """List a run the plugin cancelled in the report.json of the running test, with `--lakeflow-report-dir`.

        The run is neither appended to `runs` nor passed to `run_finished`.
        """
        if session.current().report_dir is None:
            return
        try:
            result = self._read_cancelled(polling)
        # The report never stops the job from being deleted, nor hides the LakeflowTimeout about to be raised.
        except Exception as e:
            log.warning(f"Could not read cancelled run {polling.run_id} of {self.job_key!r} for the report: {e}")
            return
        if result is not None:
            polling.item.stash.setdefault(RUNS, []).append((self, result))

    def _read_cancelled(self, polling: _Polling) -> JobRun | None:
        """Poll a cancelled run until it terminates, for at most the grace period, and build its `JobRun`."""
        run = self._poll(polling, time.monotonic() + TIMEOUT_GRACE_SECONDS)
        if terminated(run):
            return self._job_run(run, polling.outputs)
        log.warning(
            f"Run {polling.run_id} of {self.job_key!r} had not ended {TIMEOUT_GRACE_SECONDS} seconds after it was "
            f"cancelled, so report.json does not list it: {run.run_page_url}"
        )
        return None

    def _job_run(self, run: jobs.Run, outputs: Mapping[int, jobs.RunOutput]) -> JobRun:
        """Build the `JobRun` of a terminated run of this job, from the task outputs read while polling.

        With `dbt_artifacts`, the artefacts of its dbt tasks are downloaded into a temporary directory of the run's.
        """
        assert self.job_id is not None
        dbt_dir = None
        if self.lakeflow.dbt_artifacts:
            dbt_dir = Path(tempfile.mkdtemp(prefix="lakeflow-dbt-"))
        try:
            return build_job_run(
                self.lakeflow.ws,
                run,
                job_key=self.job_key,
                job_id=self.job_id,
                stubs=self.stubs,
                capture_task_values=self.capture_task_values,
                disabled=disabled_task_keys(self.settings),
                outputs=outputs,
                job_keys=self.lakeflow._job_keys(),
                notebook_export=self.lakeflow.notebook_export,
                dbt_artifacts_dir=dbt_dir,
            )
        finally:
            if dbt_dir is not None:
                self._settle_dbt_artifacts(dbt_dir, run.run_id)

    def _settle_dbt_artifacts(self, directory: Path, run_id: int | None) -> None:
        """Remove an empty artefacts directory now, keep it under `keep`, or remove it at the end of the session.

        Not at the end of the test: a run that a module-scoped fixture makes is read by every test that uses it.
        """
        if not any(directory.iterdir()):
            directory.rmdir()
            return
        if session.current().keeps(self.lakeflow.keep, keep_failed=False, failed=False):
            log.warning(f"Keeping the dbt artefacts of run {run_id} in {directory}")
            return
        session.current_item().config.add_cleanup(partial(shutil.rmtree, directory, ignore_errors=True))

    def _record_url(self, run_id: int, run: jobs.Run, item: pytest.Item) -> None:
        if run_id in self._run_urls or not run.run_page_url:
            return
        self._run_urls[run_id] = run.run_page_url
        item.user_properties.append(("lakeflow_run_url", run.run_page_url))

    def _log_progress(self, run: jobs.Run, polling: _Polling, progress: list[str]) -> None:
        """Log each task run whose state changed since the last poll, reading the output of those that finished.

        A task attempt that was logged as terminated is not logged again, so a result state that appears on a later
        poll does not log a second line. Each line is also written to the terminal when `polling` streams.
        """
        logged = polling.logged
        outputs = polling.outputs
        states: list[tuple[jobs.RunTask, str | None, bool]] = []
        finished: list[jobs.RunTask] = []
        for task in run.tasks or []:
            state, done = task_state(task)
            states.append((task, state, done))
            if done and task.run_id is not None and task.run_id not in outputs and has_output(task):
                finished.append(task)
        outputs.update(fetch_outputs(self.lakeflow.ws, finished))
        for task, state, done in states:
            key = (task.task_key or "", task.attempt_number or 0)
            previous, was_done = logged.get(key, (None, False))
            if state is None or was_done or previous == state:
                continue
            line = transition(task, previous, state, outputs.get(task.run_id or 0), done)
            logged[key] = (state, done)
            log.info(line)
            if polling.stream:
                emit(polling.item, line)
            progress.append(line)

    def debug_notebook(self, task_key: str, run: JobRun | None = None) -> DebugNotebook:
        """Write a notebook that reproduces a notebook task of a run, to debug it interactively.

        The notebook sets the task's resolved parameters as widgets and runs the task's notebook with `%run`. It is
        written to `<helpers_path>/debug/<job_key>__<task_key>__<run_id>`, replacing any notebook of the same run,
        and its URL is recorded in the test's report as `lakeflow_debug_notebook`. It replays the parameters only:
        a notebook that reads upstream task values needs `debugValue=` in `dbutils.jobs.taskValues.get()`.

        Args:
            task_key: The notebook task to reproduce.
            run: The run whose parameters to replay. Defaults to the job's last run.

        Returns:
            The notebook written.

        Raises:
            LakeflowError: If the job has no task `task_key`, the task is not a notebook task or runs its notebook
                from Git, the job has not run, or the task has no resolved parameters in the run, as when it did not
                run.
            KeyError: If the task was not part of the run, as `JobRun.task()` raises.
        """
        notebook = self._notebook_task(task_key)
        if run is None:
            if not self.runs:
                raise LakeflowError(
                    f"Job {self.job_key!r} has not run yet, so there are no resolved parameters to replay"
                )
            run = self.runs[-1]
        task = run.task(task_key)
        if not task.ran:
            raise LakeflowError(
                f"Task {task_key!r} did not run in run {run.run_id} ({task.result_state}), so it has no resolved "
                "parameters"
            )
        parameters = task.parameters
        if not isinstance(parameters, dict):
            raise LakeflowError(
                f"Task {task_key!r} has no resolved parameters in run {run.run_id}: the platform returned none, or "
                "the task run could not be read"
            )
        heading = (
            f"Parameters of task {task_key!r} in run {run.run_id} of job {self.job_key!r} ({task.run_url}), as the "
            "platform resolved them"
        )
        source = helpers.debug_notebook_source(notebook.notebook_path, parameters, heading=heading)
        folder = deployment.folder_of_helpers(self.lakeflow._state())
        path = f"{folder}/{DEBUG_FOLDER}/{self.job_key}__{task_key}__{run.run_id}"
        url = helpers.upload_notebook(self.lakeflow.ws, path, source)
        log.info(f"Wrote a debug notebook for task {task_key!r} of run {run.run_id}: {url}")
        written = DebugNotebook(task_key=task_key, run_id=run.run_id, path=path, url=url)
        state = session.current()
        state.debug_notebooks.append(_summary_line(written, state.item, helpers.cut_parameters(parameters)))
        if state.item is not None:
            state.item.user_properties.append(("lakeflow_debug_notebook", url))
            state.item.stash.setdefault(DEBUG_NOTEBOOKS, []).append(written)
        return written

    def _notebook_task(self, task_key: str) -> jobs.NotebookTask:
        """The notebook task `task_key` of the job, which `%run` can reach."""
        tasks = {}
        for task in self.settings.tasks or []:
            if not task.task_key.startswith(PREFIX):
                tasks[task.task_key] = task
        if task_key not in tasks:
            raise LakeflowError(f"Job {self.job_key!r} has no task {task_key!r}. Available: {sorted(tasks)}")
        notebook = tasks[task_key].notebook_task
        if notebook is None or task_key in self.stubs:
            notebook_tasks = []
            for key, task in tasks.items():
                if task.notebook_task is not None and key not in self.stubs:
                    notebook_tasks.append(key)
            raise LakeflowError(
                f"Task {task_key!r} of {self.job_key!r} is not a notebook task, so there is no notebook to run. "
                f"Notebook tasks: {sorted(notebook_tasks)}"
            )
        # A notebook task without a source reads from Git when the job has a git_source.
        from_git = notebook.source == jobs.Source.GIT
        if notebook.source is None and self.settings.git_source is not None:
            from_git = True
        if from_git:
            raise LakeflowError(
                f"Task {task_key!r} of {self.job_key!r} runs its notebook from Git ({notebook.notebook_path}), which "
                "%run cannot reach"
            )
        return notebook

    def close(self, failed: bool = False) -> None:
        """Cancel active runs, then delete the job unless the keep rules say to keep it, then its pipeline copies.

        With `teardown_at="close"`, the teardown task runs alone between the two, with the job's `params` and no
        run-level ones, and is waited for for `Lakeflow`'s `timeout`, unless the job never ran or `keep` or
        `--lakeflow-keep` keeps it. `keep_failed` does not skip it: a job kept because its test failed is cleaned
        up too, and its teardown run is among the kept runs. A teardown run that fails or times out only logs a
        warning with its page, and the job is deleted or kept as usual.

        With `--lakeflow-report-dir`, each cancelled run is polled until it has ended, for at most 60 seconds, and
        listed in the report.json of the test that is running, whose report is still to be written: for a job that a
        module- or session-scoped fixture closes, the test being torn down. Outside a test the run is not polled.
        The job is deleted even when the polling is interrupted. The pipeline copies go after the job, with the same
        keep rules, so a kept job never points at a deleted pipeline, and they go even when deleting the job fails.
        When the job was never created, no job points at them, so `keep_failed` does not keep them. The SQL files of
        `query_files` stay with a kept job and are deleted otherwise. Unlike the pipeline copies, they are deleted when
        the job was never created even when `keep` or `--lakeflow-keep` is on. A failure to delete one only logs a
        warning.

        Args:
            failed: Whether the test failed, which keeps the job when `keep_failed` is on.
        """
        if self._closed:
            return
        self._closed = True
        cancelled = sorted(self._active)
        for run_id in cancelled:
            self._cancel(run_id)
        item = session.current().item
        try:
            if item is not None:
                for run_id in cancelled:
                    self._report_cancelled(_Polling(run_id, item))
            if failed and session.current().debug_notebook:
                self._write_debug_notebooks()
            self._run_teardown_alone()
        finally:
            try:
                self._delete_unless_kept(failed)
            finally:
                self._delete_query_files(failed)
                self._close_pipelines(failed)

    def _run_teardown_alone(self) -> None:
        """Run the teardown once for `teardown_at="close"`, setting `teardown`. Errors are only logged."""
        if self.teardown_at != "close" or self.job_id is None or not self.ran:
            return
        # As for Lakeflow.schema(), only keep skips the clean-up: a job kept because its test failed is still cleaned.
        if session.current().keeps(self.lakeflow.keep, keep_failed=False, failed=False):
            log.warning(f"Not running the teardown of job {self.job_id}, which is kept with what its runs wrote")
            return
        if "teardown" in self.local_steps:
            self.teardown = self._run_local("teardown", self.params)
            if not self.teardown.succeeded:
                log.warning(
                    f"The teardown step of job {self.job_id} from {self.job_key!r} failed: "
                    f"{step_outcome(self.teardown)}"
                )
            return
        try:
            result = self._wait_for_teardown()
        # The teardown run never stops the job from being deleted, nor fails the test that made it.
        except Exception as e:
            log.warning(f"The teardown run of job {self.job_id} from {self.job_key!r} did not finish: {e}")
            return
        self.teardown = result.teardown
        if self.teardown is not None and self.teardown.succeeded:
            return
        outcome = f"ended {result.result_state} without running the teardown task"
        if self.teardown is not None:
            outcome = f"failed: {step_outcome(self.teardown)}"
        log.warning(f"The teardown run {result.run_id} of job {self.job_id} {outcome}: {result.run_url}")

    def _run_local(self, role: str, params: Mapping[str, str]) -> StepResult:
        """Run the `setup=` or `teardown=` step over Databricks Connect with the run's job parameters."""
        step = self.local_steps[role]
        values = steps.step_params(self.settings, params)
        spark = steps.session_for(self.lakeflow)
        result = steps.run_local(spark, self.lakeflow.ws.dbutils, step, values)
        log.info(f"The {role} step of job {self.job_id} ran over Databricks Connect in {result.duration}")
        return result

    def _with_local_steps(self, result: JobRun, setup: StepResult | None, params: Mapping[str, str]) -> JobRun:
        """Add the steps that ran over Connect to a finished run: its setup, and its teardown, run now."""
        if setup is not None:
            result = replace(result, setup=setup)
        if "teardown" in self.local_steps and self.teardown_at == "run":
            result = replace(result, teardown=self._run_local("teardown", params))
        return result

    def _wait_for_teardown(self) -> JobRun:
        # A run that closing the job cancelled may still be ending, and the copy allows one run at a time, so the
        # teardown run queues behind it rather than being skipped.
        result = self._run_alone(TEARDOWN_KEY, queue=jobs.QueueSettings(enabled=True))
        log.info(f"Teardown run {result.run_id} of job {self.job_id} finished {result.result_state}: {result.run_url}")
        return result

    def _run_alone(
        self, task_key: str, *, timeout: timedelta | None = None, queue: jobs.QueueSettings | None = None
    ) -> JobRun:
        """Run one task of the job on its own, with the job's `params`, and wait for it.

        For the teardown task of `teardown_at="close"` and for the steps of `Lakeflow.once()`. The run is neither
        appended to `runs` nor passed to `run_finished`, and nothing is recorded on the test.

        Args:
            task_key: The task to run, which `only` names alone.
            timeout: How long to wait, by default `Lakeflow`'s `timeout`.
            queue: The run's queue setting, or `None` for the job's own.

        Returns:
            The run's outcome.

        Raises:
            LakeflowTimeout: If the run did not finish in time. It is cancelled first.
        """
        item = session.current_item()
        if timeout is None:
            timeout = self.lakeflow.timeout
        run_id, first = self._start_run(item, self.params, [task_key], queue=queue, timeout=timeout)
        run, outputs = self._wait(run_id, timeout, item, first, time.monotonic())
        self._forget(run_id)
        return self._job_run(run, outputs)

    def _close_pipelines(self, failed: bool) -> None:
        if self.job_id is None:
            self._warn_copies_not_kept(failed)
            failed = False
        close_all(self.pipeline_copies, failed)

    def _warn_copies_not_kept(self, failed: bool) -> None:
        """Warn when `keep_failed` alone would have kept the copies of a job that was never created."""
        state = session.current()
        kept_anyway = state.keeps(self.lakeflow.keep, self.lakeflow.keep_failed, failed=False)
        kept_if_failed = state.keeps(self.lakeflow.keep, self.lakeflow.keep_failed, failed)
        if kept_anyway or not kept_if_failed:
            return
        made = [copy.pipeline_id for copy in self.pipeline_copies if copy.pipeline_id is not None]
        if made:
            log.warning(
                f"The job copying {self.job_key!r} was not created, so no job points at its pipeline copies "
                f"{made}, and they are deleted whatever keep_failed says"
            )

    def _write_debug_notebooks(self) -> None:
        """Write a debug notebook for each notebook task that failed in the last run. Errors are only logged."""
        if not self.runs:
            return
        run = self.runs[-1]
        for key, task in run.tasks.items():
            if task.succeeded or not task.ran:
                continue
            # Teardown never fails because of a debugging aid.
            try:
                if require_task(self.settings, key).notebook_task is None:
                    continue
                self.debug_notebook(key, run)
            except (LakeflowError, DatabricksError) as e:
                log.warning(f"Could not write a debug notebook for task {key!r} of run {run.run_id}: {e}")

    def _delete_unless_kept(self, failed: bool) -> None:
        if self.job_id is None:
            return
        state = session.current()
        if state.keeps(self.lakeflow.keep, self.lakeflow.keep_failed, failed):
            log.warning(f"Keeping job {self.job_id} from {self.job_key}: {self.url}")
            state.kept_jobs.append(str(self.url))
            for url in self._run_urls.values():
                log.warning(f"Kept run: {url}")
            return
        self.lakeflow.ws.jobs.delete(self.job_id)
        log.info(f"Deleted job {self.job_id}")

    def _delete_query_files(self, failed: bool) -> None:
        if not self._query_files_uploaded:
            return
        if self.job_id is not None and session.current().keeps(self.lakeflow.keep, self.lakeflow.keep_failed, failed):
            return
        for path in self.query_files:
            try:
                self.lakeflow.ws.workspace.delete(path)
            except DatabricksError as e:
                log.warning(f"Could not delete {path}, which --lakeflow-sweep-notebooks deletes later: {e}")

    def _cancel(self, run_id: int) -> None:
        cancel_run(self.lakeflow.ws, run_id)
        self._forget(run_id)

    def _forget(self, run_id: int) -> None:
        self._active.pop(run_id, None)
        self.lakeflow.active_runs.discard(run_id)
        if self._slots is not None:
            self._slots.release(run_id)


class RunHandle:
    """A run started by `EphemeralJob.start()`, to wait for or cancel later.

    Attributes:
        job: The job the run belongs to.
        run_id: The run's id.
    """

    def __init__(
        self,
        job: EphemeralJob,
        run_id: int,
        item: pytest.Item,
        first: jobs.Run,
        timeout: timedelta | None = None,
        *,
        stream: bool | None = None,
        performance_target: jobs.PerformanceTarget | None = None,
        setup: StepResult | None = None,
        params: Mapping[str, str] | None = None,
    ) -> None:
        self.job = job
        self.run_id = run_id
        self._item = item
        self._first = first
        self._first_at = time.monotonic()
        self._timeout = timeout
        # From run(stream=...); None follows --lakeflow-live.
        self._stream = stream
        # From run(performance_target=...); None asks for the job's own.
        self._performance_target = performance_target
        # The setup that ran over Connect before the run started, and the run's job parameters for its teardown.
        self._setup = setup
        self._params = dict(params or {})
        self._result: JobRun | None = None

    @property
    def url(self) -> str | None:
        """The run's page in the workspace, or `None` if the workspace has not reported it yet."""
        return self.job._run_urls.get(self.run_id)

    def wait(self, timeout: timedelta | None = None, *, stop_after: timedelta | None = None) -> JobRun:
        """Wait for the run to finish.

        Waiting again returns the same `JobRun`, and `run_finished` is called only the first time.

        Args:
            timeout: How long to wait from this call. Defaults to the `timeout` passed to `start()`, else
                `Lakeflow`'s `timeout`. It must not be longer than the job's `timeout_seconds`, which `Lakeflow`'s
                `timeout` sets. The run has already started, so pass a longer timeout to `start()` or `run()`, which
                raise `timeout_seconds` first.
            stop_after: How long the run may go on before it is cancelled, counted from `start()`, for a job that does
                not end on its own. The run is then polled until it has ended and returned as a `JobRun` whose
                `cancelled` is true, instead of raising. It must not be longer than `timeout`.

        Returns:
            The run's outcome, which is also appended to the job's `runs`.

        Raises:
            ValueError: If `stop_after` is longer than `timeout`. The run goes on.
            LakeflowError: If the job closed before the run finished, which cancelled the run, or `timeout` is longer
                than the job's `timeout_seconds`, in which case the run goes on.
            LakeflowTimeout: If the run did not finish in time. The run is cancelled first.
        """
        if self._result is not None:
            return self._result
        job = self.job
        if job._closed:
            raise LakeflowError(f"The ephemeral job for {job.job_key!r} has already been closed")
        if timeout is not None:
            job._check_timeout(timeout)
        elif self._timeout is not None:
            timeout = self._timeout
        else:
            timeout = job.lakeflow.timeout
        if stop_after is not None:
            _check_stop_after(job.job_key, stop_after, timeout)
        stream = self._stream
        if stream is None:
            stream = session.current().live
        if stream:
            self._emit_start()
        first = self._first
        self._first = None
        run, outputs = job._wait(self.run_id, timeout, self._item, first, self._first_at, stop_after, stream)
        job._forget(self.run_id)
        result = job._with_local_steps(job._job_run(run, outputs), self._setup, self._params)
        self._result = result
        job.runs.append(result)
        # Held on the test only for --lakeflow-report-dir, because a run can carry megabytes of logs and notebooks.
        if session.current().report_dir is not None:
            self._item.stash.setdefault(RUNS, []).append((job, result))
        record_outcome(self._item, result)
        finished = f"Run {self.run_id} finished {result.result_state} in {result.duration}: {result.run_url}"
        log.info(finished)
        if stream:
            emit(self._item, finished)
        _warn_on_failed_teardown(result)
        self._check_performance_target(result)
        if job.lakeflow.run_finished is not None:
            job.lakeflow.run_finished(result, self._item)
        return result

    def _check_performance_target(self, result: JobRun) -> None:
        """Warn when the run asked for `PERFORMANCE_OPTIMIZED`, from `run()` or the job's settings, and used `STANDARD`.

        The job's settings say `PERFORMANCE_OPTIMIZED` when `Lakeflow`'s `performance_target` is, as it is by default,
        or when the template or `modify_settings` chose it.
        """
        requested = self._performance_target
        if requested is None:
            requested = self.job.settings.performance_target
        if requested != jobs.PerformanceTarget.PERFORMANCE_OPTIMIZED:
            return
        if result.effective_performance_target == "STANDARD":
            self.job.lakeflow._warn_performance_override(self.job.job_key, result)

    def _emit_start(self) -> None:
        started = f"Started run {self.run_id} of job {self.job.job_id}"
        # The first poll may not carry the run's page yet.
        if self.url is not None:
            started = f"{started}: {self.url}"
        emit(self._item, started)

    def cancel(self) -> None:
        """Cancel the run. It ends `CANCELED` shortly after, which a later `wait()` returns."""
        self.job._cancel(self.run_id)


def _release_ended(slots: session.RunSlots) -> None:
    """Give back the slot of each run holding one that has ended, though nobody has waited for it yet."""
    for run_id, job in list(slots.holders.items()):
        try:
            run = job.lakeflow.ws.jobs.get_run(run_id)
        except DatabricksError as e:
            log.debug(f"Could not poll run {run_id} while waiting for a run slot: {e}")
            continue
        if terminated(run):
            slots.release(run_id)


def _dbt_commands(commands: Sequence[str] | None) -> list[str] | None:
    """Check the `dbt_commands` of a run, which `run_now` would send as a list of characters if given a string."""
    if commands is None:
        return None
    if isinstance(commands, str):
        raise ValueError(f"dbt_commands must be a list of commands, not a string: {commands!r}")
    if not commands:
        raise ValueError(
            "dbt_commands must not be empty. Pass at least one command, such as ['dbt build'], or leave dbt_commands "
            "out to run each dbt task's own commands."
        )
    return list(commands)


def _check_stop_after(job_key: str, stop_after: timedelta, timeout: timedelta) -> None:
    if stop_after > timeout:
        raise ValueError(
            f"stop_after={stop_after} of {job_key!r} is longer than its timeout of {timeout}, so the run would time "
            "out before it is stopped. Pass a longer timeout or a shorter stop_after."
        )


def _performance_target(value: jobs.PerformanceTarget | str | None) -> jobs.PerformanceTarget | None:
    if value is None:
        return None
    return jobs.PerformanceTarget(value)


def _warn_on_failed_teardown(result: JobRun) -> None:
    """Log a failed teardown task, which leaves `succeeded` as it was, so that it does not pass unnoticed."""
    teardown = result.teardown
    if teardown is None or teardown.succeeded:
        return
    if teardown.task is None:
        log.warning(f"The teardown step of run {result.run_id} failed: {step_outcome(teardown)}")
        return
    log.warning(f"The teardown task of run {result.run_id} failed: {step_outcome(teardown)} {teardown.task.run_url}")


def _summary_line(notebook: DebugNotebook, item: pytest.Item | None, cut: list[str]) -> str:
    """Describe a debug notebook for the terminal summary, such as `<url> (test_x.py::test_y, task 'transform')`.

    Naming the test matters when the job closed in a finaliser: the `lakeflow_debug_notebook` line then lands on the
    teardown report, which pytest shows only with `-rA`.
    """
    where = f"task {notebook.task_key!r}"
    if item is not None:
        where = f"{item.nodeid}, {where}"
    if cut:
        names = ", ".join(repr(key) for key in cut)
        where = f"{where}; widgets cut at {WIDGET_LIMIT_BYTES} bytes: {names}"
    return f"{notebook.url} ({where})"
