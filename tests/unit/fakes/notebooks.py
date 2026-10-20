"""Run a generated helper notebook with a fake `dbutils` and `spark`."""

from __future__ import annotations

from collections.abc import Mapping
from types import SimpleNamespace


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
