"""What an ephemeral job and an ephemeral pipeline have in common: how a test opens, closes and keeps them."""

from __future__ import annotations

from abc import ABC, abstractmethod
from types import TracebackType
from typing import TYPE_CHECKING, Self

import pytest

from pytest_databricks_lakeflow.ephemeral.outcome import item_failed

if TYPE_CHECKING:
    from pytest_databricks_lakeflow.client import Lakeflow


class EphemeralResource(ABC):
    """A throwaway copy that a test makes in the workspace.

    It is created on entering a `with` block, or by the first use without one, and closed on leaving the block, or
    at the end of the test that made it. Closing deletes it unless the keep rules say otherwise.

    Attributes:
        lakeflow: The `Lakeflow` that made the copy.
    """

    def __init__(self, lakeflow: Lakeflow) -> None:
        self.lakeflow = lakeflow
        self._entered = False
        self._closed = False

    @property
    @abstractmethod
    def url(self) -> str | None:
        """The copy's page in the workspace, or `None` before it is created."""

    @abstractmethod
    def create(self) -> None:
        """Create the copy if it does not exist yet."""

    @abstractmethod
    def close(self, failed: bool = False) -> None:
        """Close the copy: stop what is still running, then delete it unless the keep rules say to keep it."""

    def __enter__(self) -> Self:
        self.create()
        self._entered = True
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        failed = exc_type is not None and not issubclass(exc_type, (pytest.skip.Exception, pytest.xfail.Exception))
        self.close(failed=failed)

    def _teardown(self, item: pytest.Item) -> None:
        """Close the copy at the end of the test that made it, unless a `with` block closes it."""
        if not self._entered:
            self.close(failed=item_failed(item.stash))
