"""Collect one check per bundle job under `--lakeflow-check`, from each `databricks.yml` pytest sees."""

from __future__ import annotations

import contextlib
import dataclasses
import inspect
import warnings
from collections.abc import Callable, Generator, Iterator
from pathlib import Path
from typing import Any, cast

import pytest

from pytest_databricks_lakeflow import bundle
from pytest_databricks_lakeflow.bundle import resolve_root
from pytest_databricks_lakeflow.client import Lakeflow
from pytest_databricks_lakeflow.errors import LakeflowBundleError, LakeflowError
from pytest_databricks_lakeflow.session import current
from pytest_databricks_lakeflow.settings.lint import DEFAULT_RULES

# The trigger rule is advice for a test that runs the job, which --lakeflow-check does not: a job that reads its own
# trigger context is wired correctly.
CHECK_RULES = DEFAULT_RULES - {"trigger"}
# The module that defines the default `lakeflow` fixture, which an override replaces.
PLUGIN_MODULE = f"{__package__}.plugin"


class BundleFile(pytest.File):
    """A bundle's databricks.yml, collected under `--lakeflow-check` as one item per job template."""

    def collect(self) -> Iterator[pytest.Item]:
        directory = self.path.parent
        try:
            with _configured_lakeflow(self) as configured:
                lakeflow = _for_bundle(configured, directory)
                keys = sorted({**lakeflow.templates, **lakeflow.template_errors})
        except LakeflowError as e:
            raise self.CollectError(str(e)) from e
        bundle_lakeflow = _BundleLakeflow(directory)
        for key in keys:
            yield CheckItem.from_parent(self, name=key, callobj=_check(key, bundle_lakeflow))


class CheckItem(pytest.Function):
    """One job template of a bundle, linted by `Lakeflow.check` with the `lakeflow` fixture."""

    def reportinfo(self) -> tuple[Path, None, str]:
        return self.path, None, self.name


@contextlib.contextmanager
def _configured_lakeflow(collector: pytest.Collector) -> Generator[Lakeflow]:
    # Fixtures cannot be requested during collection, so call the definition of `lakeflow` that this file sees,
    # the conftest override included, and finish a yield fixture as pytest would once the file is collected.
    function = _fixture_function(collector)
    requested = list(inspect.signature(function).parameters)
    if requested:
        raise LakeflowError(
            "--lakeflow-check calls the lakeflow fixture while collecting, so it cannot request other fixtures, "
            f"and this one requests {', '.join(requested)}"
        )
    with _called(function) as lakeflow:
        yield lakeflow


@contextlib.contextmanager
def _called(function: Callable[..., Any]) -> Generator[Lakeflow]:
    value = function()
    if not inspect.isgenerator(value):
        yield cast(Lakeflow, value)
        return
    try:
        yield cast(Lakeflow, next(value))
    finally:
        next(value, None)


def _fixture_function(collector: pytest.Collector) -> Callable[..., Any]:
    # The lookup is private pytest API, which took a node id before pytest 8.1.
    manager: Any = collector.session._fixturemanager
    if "nodeid" in inspect.signature(manager.getfixturedefs).parameters:
        visible = manager.getfixturedefs("lakeflow", collector.nodeid)
    else:
        visible = manager.getfixturedefs("lakeflow", collector)
    function = visible[-1].func
    if function.__module__ != PLUGIN_MODULE:
        return function
    # Only the default is visible here. An override elsewhere, such as in tests/conftest.py beside a bundle at the
    # repository root, configures another deployment than the one this would check.
    for definition in manager._arg2fixturedefs.get("lakeflow", []):
        conftest = _override_for(definition.func, collector)
        if conftest is not None:
            raise LakeflowError(
                f"--lakeflow-check cannot see the lakeflow fixture that {_relative(conftest, collector)} overrides, "
                "so it would check this bundle with the default Lakeflow(): put the override in a conftest.py in "
                "the bundle's directory or above it"
            )
    return function


def _override_for(function: Callable[..., Any], collector: pytest.Collector) -> Path | None:
    # Only a conftest.py that configures this bundle counts: one inside this bundle, or one outside every bundle that
    # points at it. Not a test module's own fixture, not one that derives from a parent's lakeflow (which here is the
    # default), and not one inside another bundle.
    if function.__module__ == PLUGIN_MODULE or "lakeflow" in inspect.signature(function).parameters:
        return None
    conftest = Path(inspect.getfile(function))
    if conftest.name != "conftest.py":
        return None
    try:
        root = bundle.find_root(conftest.parent)
    except LakeflowBundleError:
        return _pointed_at(function, conftest, collector)
    if root.samefile(collector.path.parent):
        return conftest
    return None


def _pointed_at(function: Callable[..., Any], conftest: Path, collector: pytest.Collector) -> Path | None:
    # Outside every bundle, as tests/conftest.py beside bundle/ or services/a/conftest.py above services/a/bundle/,
    # the directories cannot tell which bundle an override is for, but its bundle_root can.
    requested = list(inspect.signature(function).parameters)
    if requested:
        warnings.warn(
            f"--lakeflow-check cannot call the lakeflow fixture that {_relative(conftest, collector)} overrides while "
            f"collecting, because it requests {', '.join(requested)}, so it checks this bundle with the default "
            "Lakeflow(): put an override meant for this bundle in a conftest.py in the bundle's directory or above it",
            stacklevel=1,
        )
        return None
    # The call happens while collecting every bundle that sees only the default fixture, so a skip or an error from an
    # override meant for another bundle must not reach this one's check.
    try:
        with _called(function) as lakeflow:
            points = _points_at(lakeflow, collector.path.parent)
    except pytest.exit.Exception:
        raise
    except (Exception, pytest.skip.Exception, pytest.fail.Exception) as e:
        warnings.warn(
            f"--lakeflow-check called the lakeflow fixture that {_relative(conftest, collector)} overrides while "
            f"collecting, to read its bundle_root, and it {_outcome(e)}. So it checks this bundle with the default "
            "Lakeflow(): put an override meant for this bundle in a conftest.py in the bundle's directory or above it",
            stacklevel=1,
        )
        return None
    if points:
        return conftest
    return None


def _outcome(error: BaseException) -> str:
    if isinstance(error, pytest.skip.Exception):
        return f"skipped: {error}"
    return f"raised {type(error).__name__}: {error}"


def _relative(path: Path, collector: pytest.Collector) -> str:
    try:
        return path.relative_to(collector.config.rootpath).as_posix()
    except ValueError:
        return str(path)


def _for_bundle(lakeflow: Lakeflow, directory: Path) -> Lakeflow:
    if _points_at(lakeflow, directory):
        return lakeflow
    return dataclasses.replace(lakeflow, bundle_root=directory)


def _points_at(lakeflow: Lakeflow, directory: Path) -> bool:
    try:
        root = resolve_root(lakeflow.bundle_root, current().rootpath)
    except LakeflowBundleError:
        return False
    return root.samefile(directory)


class _BundleLakeflow:
    def __init__(self, directory: Path) -> None:
        self.directory = directory
        self._fixture: Lakeflow | None = None
        self._lakeflow: Lakeflow | None = None

    def __call__(self, lakeflow: Lakeflow) -> Lakeflow:
        if self._lakeflow is None or lakeflow is not self._fixture:
            self._fixture = lakeflow
            self._lakeflow = _for_bundle(lakeflow, self.directory)
        return self._lakeflow


def _check(key: str, bundle_lakeflow: _BundleLakeflow) -> Callable[[Lakeflow], None]:
    def check(lakeflow: Lakeflow) -> None:
        try:
            problems = bundle_lakeflow(lakeflow).check(key, rules=CHECK_RULES)
        except LakeflowError as e:
            problems = [f"{type(e).__name__}: {e}"]
        if problems:
            pytest.fail("\n".join(problems), pytrace=False)

    return check
