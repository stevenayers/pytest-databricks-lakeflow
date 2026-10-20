"""A throwaway copy of a deployed pipeline, publishing to a schema the test chose."""

from __future__ import annotations

import logging
import time
from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from databricks.sdk.service import pipelines

from pytest_databricks_lakeflow import session, workspace_api
from pytest_databricks_lakeflow.ephemeral.base import EphemeralResource
from pytest_databricks_lakeflow.ephemeral.outcome import PROGRESS
from pytest_databricks_lakeflow.ephemeral.polling import TERMINAL_UPDATE_STATES, poll_seconds
from pytest_databricks_lakeflow.errors import LakeflowError, LakeflowTimeout
from pytest_databricks_lakeflow.results.models import PipelineUpdate
from pytest_databricks_lakeflow.settings.params import check_selections
from pytest_databricks_lakeflow.workspace_api import stop_update

if TYPE_CHECKING:
    from pytest_databricks_lakeflow.client import Lakeflow

log = logging.getLogger("pytest_databricks_lakeflow")


class EphemeralPipeline(EphemeralResource):
    """A throwaway copy of a deployed pipeline, publishing to a schema that the test chose.

    The copy is created on entering the `with` block, or by the first `run()` when used without `with`. It is
    deleted with its tables on leaving the block, or at the end of the test that made it, unless `keep` is set or the
    test failed and `keep_failed` is on. An update that is still running then is stopped first. The copies that
    `pipelines="copy"` makes for a job are created and closed by that job instead.

    Attributes:
        lakeflow: The `Lakeflow` that made the copy.
        pipeline_key: The bundle resource key of the pipeline it copies, or that pipeline's id when the bundle does
            not define it.
        source_id: The id of the pipeline it copies.
        create_kwargs: The arguments the copy is created with, as `pipelines.create` takes them.
        schema: The full name of the schema the copy publishes to.
        pipeline_id: The copy's id, once created.
        updates: The outcome of each update that `run()` waited for, in order.
    """

    def __init__(
        self,
        lakeflow: Lakeflow,
        pipeline_key: str,
        source_id: str,
        create_kwargs: Mapping[str, Any],
        schema: str,
    ) -> None:
        super().__init__(lakeflow)
        self.pipeline_key = pipeline_key
        self.source_id = source_id
        self.create_kwargs = dict(create_kwargs)
        self.schema = schema
        self.pipeline_id: str | None = None
        self.updates: list[PipelineUpdate] = []
        self._active_update: str | None = None

    @property
    def url(self) -> str | None:
        """The copy's page in the workspace, or `None` before it is created."""
        if self.pipeline_id is None:
            return None
        return f"{self.lakeflow.ws.config.host}/pipelines/{self.pipeline_id}"

    def create(self) -> None:
        """Create the copy if it does not exist yet.

        Raises:
            LakeflowError: If the copy has been closed. Errors from `pipelines.create` are raised unchanged.
        """
        if self._closed:
            raise LakeflowError(f"The ephemeral pipeline for {self.pipeline_key!r} has already been closed")
        if self.pipeline_id is not None:
            return
        item = session.current_item()
        self.pipeline_id = self.lakeflow.ws.pipelines.create(**self.create_kwargs).pipeline_id
        log.info(f"Created pipeline {self.pipeline_id} from {self.pipeline_key}: {self.url}")
        item.user_properties.append(("lakeflow_pipeline_url", self.url))
        item.user_properties.append(("lakeflow_pipeline_id", self.pipeline_id))

    def run(
        self,
        *,
        full_refresh: bool | None = None,
        full_refresh_selection: Sequence[str] | None = None,
        refresh_selection: Sequence[str] | None = None,
        timeout: timedelta | None = None,
    ) -> PipelineUpdate:
        """Run an update of the copy and wait for it to end, creating the copy first if needed.

        A failed update is returned, not raised: check its `state`, and read what went wrong with
        `events(level="ERROR")`. Each change of the update's state is logged and shown in the `lakeflow` report
        section of the test.

        Args:
            full_refresh: Whether the update refreshes every table from scratch.
            full_refresh_selection: Tables that the update refreshes from scratch.
            refresh_selection: Tables that the update refreshes.
            timeout: How long to wait. Defaults to `Lakeflow`'s `timeout`.

        Returns:
            The update, which is also appended to `updates`.

        Raises:
            ValueError: If a selection is a string rather than a list of names.
            LakeflowError: If the copy has been closed.
            LakeflowTimeout: If the update did not end in time. It is stopped first.
        """
        check_selections({"full_refresh_selection": full_refresh_selection, "refresh_selection": refresh_selection})
        self.create()
        assert self.pipeline_id is not None
        ws = self.lakeflow.ws
        response = ws.pipelines.start_update(
            self.pipeline_id,
            full_refresh=full_refresh,
            full_refresh_selection=_as_list(full_refresh_selection),
            refresh_selection=_as_list(refresh_selection),
        )
        update_id = response.update_id
        assert update_id is not None
        self._active_update = update_id
        self.lakeflow.active_updates[self.pipeline_id] = update_id
        log.info(f"Started update {update_id} of pipeline {self.pipeline_id}")
        if timeout is None:
            timeout = self.lakeflow.timeout
        info = self._wait(update_id, timeout)
        self._forget_update()
        update = PipelineUpdate.from_info(ws, info)
        self.updates.append(update)
        log.info(f"Update {update_id} of pipeline {self.pipeline_id} finished {update.state}: {self.url}")
        return update

    def _wait(self, update_id: str, timeout: timedelta) -> pipelines.UpdateInfo:
        """Poll the update until it ends, logging each change of its state. On timeout, stop it and raise."""
        assert self.pipeline_id is not None
        ws = self.lakeflow.ws
        progress = session.current_item().stash.setdefault(PROGRESS, [])
        deadline = time.monotonic() + timeout.total_seconds()
        previous: str | None = None
        while True:
            info = ws.pipelines.get_update(self.pipeline_id, update_id).update or pipelines.UpdateInfo()
            state = info.state.value if info.state else None
            if state is not None and state != previous:
                line = f"pipeline {self.pipeline_key}: {state}"
                if previous is not None:
                    line = f"pipeline {self.pipeline_key}: {previous} -> {state}"
                log.info(line)
                progress.append(line)
                previous = state
            if state in TERMINAL_UPDATE_STATES:
                return info
            if time.monotonic() >= deadline:
                break
            time.sleep(poll_seconds(self.lakeflow))
        stop_update(ws, self.pipeline_id)
        self._forget_update()
        raise LakeflowTimeout(
            f"Update {update_id} of pipeline {self.pipeline_key!r} did not finish within {timeout} and was stopped: "
            f"{self.url}"
        )

    def close(self, failed: bool = False) -> None:
        """Stop an update that is still running, then delete the copy with its tables unless the keep rules say not to.

        Args:
            failed: Whether the test failed, which keeps the copy when `keep_failed` is on.
        """
        if self._closed:
            return
        self._closed = True
        if self.pipeline_id is None:
            return
        if self._active_update is not None:
            stop_update(self.lakeflow.ws, self.pipeline_id)
            self._forget_update()
        state = session.current()
        if state.keeps(self.lakeflow.keep, self.lakeflow.keep_failed, failed):
            log.warning(f"Keeping pipeline {self.pipeline_id} from {self.pipeline_key}: {self.url}")
            state.kept_pipelines.append(str(self.url))
            return
        workspace_api.delete_pipeline(self.lakeflow.ws, self.pipeline_id)
        log.info(f"Deleted pipeline {self.pipeline_id}")

    def _forget_update(self) -> None:
        self._active_update = None
        if self.pipeline_id is not None:
            self.lakeflow.active_updates.pop(self.pipeline_id, None)


def _as_list(names: Sequence[str] | None) -> list[str] | None:
    if names is None:
        return None
    return list(names)


def close_all(copies: Sequence[EphemeralPipeline], failed: bool) -> None:
    """Close every pipeline copy, even when closing one of them fails, and then raise the first error."""
    errors: list[Exception] = []
    for copy in copies:
        try:
            copy.close(failed=failed)
        except Exception as e:
            errors.append(e)
    if errors:
        raise errors[0]
