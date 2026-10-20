from __future__ import annotations

import json
import logging
import posixpath
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import PermissionDenied
from databricks.sdk.service import catalog, jobs, pipelines, workspace

from pytest_databricks_lakeflow import Lakeflow, SweepReport, session
from pytest_databricks_lakeflow.errors import LakeflowBundleError, LakeflowError
from pytest_databricks_lakeflow.plugin import parse_duration

from .conftest import FakeCli, configure_lakeflow, set_ci
from .fakes import HOST, HOUR_MS, FakeWorkspaceClient, listed_job, make_run, queue_failed_run, sweep_candidates
from .test_session import STAGING_FIXTURE, SWEEP_LOGGING, make_xdist_conftest

pytestmark = pytest.mark.usefixtures("fake_cli", "bundle_dir")

DESTROY = ["bundle", "destroy", "-t", "test", "--auto-approve"]
DESTROY_STAGING = ["bundle", "destroy", "-t", "staging", "--auto-approve"]
DEPLOY_WITH_VARIABLES = ["bundle", "deploy", "-t", "test", "--auto-approve", "--var", "catalog=dev"]
FOLDER = "/Users/someone@example.com/.pytest-databricks-lakeflow"
COLLEAGUE_FOLDER = "/Users/colleague@example.com/.pytest-databricks-lakeflow"
DEBUG_FOLDER = f"{FOLDER}/debug"
OLD = {"pytest": "", "pytest_run_id": "old-run"}
USES_LAKEFLOW = "def test_{0}(lakeflow):\n    lakeflow.template('my_job')\n"
USES_SCHEMA = "def test_schema(lakeflow):\n    with lakeflow.schema():\n        pass\n"
FAILING_JOB = "def test_{0}(lakeflow):\n    lakeflow.job('my_job').create()\n    assert False\n"
CREATES_JOBS = "def test_{0}(lakeflow):\n    with lakeflow.job('my_job'):\n        pass\n"
FAILED_RUN = (
    "def test_{0}(lakeflow):\n    with lakeflow.job('my_job') as job:\n        job.run()\n        assert False\n"
)
SWEEP_HINT = "hint: pytest --lakeflow-sweep removes kept jobs older than sweep_hours"
VARIABLES_FIXTURE = """

@pytest.fixture(scope="session")
def lakeflow():
    return Lakeflow(variables={"catalog": "dev"})
"""
XDIST_WORKER_IDS = """

import os


def make_client(profile):
    ws = FakeWorkspaceClient()
    ws.jobs.add(example_settings())
    ws.jobs.start_ids_at(1000 * (2 + int(os.environ["PYTEST_XDIST_WORKER"][2:])))
    return ws
"""
# `my_job` is a job with one pipeline task, on pipeline `abc`, in every xdist worker.
XDIST_PIPELINE_JOB = """

from fakes import pipeline_settings, pipeline_spec


def make_client(profile):
    ws = FakeWorkspaceClient()
    ws.jobs.add(pipeline_settings())
    ws.pipelines.specs["abc"] = pipeline_spec()
    return ws
"""
FAILING_COPY = (
    "def test_{0}(lakeflow):\n    lakeflow.job('my_job', pipelines='copy', schema='c.s').create()\n    assert False\n"
)
XDIST_FAILED_TRANSFORM = """

from databricks.sdk.service import jobs

from fakes import make_run


def make_client(profile):
    ws = FakeWorkspaceClient()
    ws.jobs.add(example_settings())
    parameters = jobs.ResolvedNotebookTaskValues(base_parameters={"row_count": "7"})
    ws.jobs.resolved[7700] = jobs.ResolvedValues(notebook_task=parameters)
    # Terminated at the first poll, because the workers poll at the plugin's real interval.
    for _ in range(2):
        ws.jobs.queue_polls([make_run(77, {"transform": "FAILED"}, "FAILED")])
    return ws
"""


def uses_lakeflow(*names: str) -> str:
    return "\n".join(USES_LAKEFLOW.format(name) for name in names)


def leaves_runs_active(runs: str) -> str:
    return USES_LAKEFLOW.format("leak") + f"    lakeflow.active_runs.update({runs})\n"


def as_client(ws: FakeWorkspaceClient) -> WorkspaceClient:
    return cast(WorkspaceClient, ws)


def ago(hours: float) -> int:
    return int(time.time() * 1000 - hours * HOUR_MS)


def expires(hours: float) -> str:
    """A `pytest_expires` tag value `hours` from now, in the past for a negative number."""
    return (datetime.now(UTC) + timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ")


def schema(
    name: str, age_hours: float, creator: str = "someone@example.com", properties: dict[str, str] | None = None
) -> catalog.SchemaInfo:
    return catalog.SchemaInfo(
        catalog_name="dev_sandbox",
        name=name,
        full_name=f"dev_sandbox.{name}",
        created_at=ago(age_hours),
        created_by=creator,
        properties=properties,
    )


def notebook(
    name: str,
    age_hours: float,
    object_type: workspace.ObjectType = workspace.ObjectType.NOTEBOOK,
    folder: str = FOLDER,
) -> workspace.ObjectInfo:
    return workspace.ObjectInfo(path=f"{folder}/{name}", object_type=object_type, modified_at=ago(age_hours))


def home(user: str) -> workspace.ObjectInfo:
    """A user's home folder, as listing `/Users` returns it."""
    return workspace.ObjectInfo(path=f"/Users/{user}", object_type=workspace.ObjectType.DIRECTORY)


def sweep_pipelines(ws: FakeWorkspaceClient) -> None:
    """List pipeline copies for the sweep. With a 24 hour cutoff and run id `this-run`, it deletes p-old and p-expired.

    p-old is tagged with the branch `main`. p-deployed has old `pytest` tags but not the `[pytest] ` name prefix, so
    the name filter leaves it out.
    """
    ws.pipelines.leftover("p-old", 25, {**OLD, "pytest_git_branch": "main"})
    ws.pipelines.leftover("p-this-run", 25, {"pytest": "", "pytest_run_id": "this-run"})
    ws.pipelines.leftover("p-recent", 1, OLD)
    ws.pipelines.leftover("p-colleague", 25, OLD, creator="colleague@example.com")
    ws.pipelines.leftover("p-untagged", 25, {"dev": "someone"})
    ws.pipelines.leftover("p-expired", 1, {**OLD, "pytest_expires": expires(-1)})
    ws.pipelines.leftover("p-deployed", 25, OLD, name="[dev someone] my_pipeline")


def sweep_workspace(ws: FakeWorkspaceClient | None = None) -> FakeWorkspaceClient:
    """Fill `ws` with the sweep candidates, schemas of this run and others, and helper notebooks young and old."""
    ws = ws or FakeWorkspaceClient()
    ws.jobs.listed = sweep_candidates()
    ws.schemas.listed = [
        schema("pytest_old_run_1a2b3c4d", 25),
        schema("pytest_this_run_0f0f0f0f", 25),
        schema("pytest_old_run_2b3c4d5e", 1),
        schema("pytest_old_run_3c4d5e6f", 25, creator="colleague@example.com"),
        schema("analytics", 25),
    ]
    ws.workspace.listed = [
        notebook("set_values", 25),
        notebook("get_values", 25),
        notebook("setup_0123456789ab", 25),
        notebook("teardown_0123456789ab", 25),
        notebook("setup_ba9876543210", 1),
        notebook("teardown_ba9876543210", 1),
        notebook("setup_tables", 25),
        notebook("teardown_tables", 25),
        notebook("setup_00000000000a", 25, workspace.ObjectType.DIRECTORY),
    ]
    return ws


class TestSchemas:
    """`Lakeflow.schema()` creates a temporary schema and drops it unless the keep rules say otherwise."""

    def test_schema_is_created_and_dropped(
        self,
        pytester: pytest.Pytester,
        fake_ws: FakeWorkspaceClient,
        fake_cli: FakeCli,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("LAKEFLOW_RUN_ID", "CI-42")
        configure_lakeflow(pytester, 'catalog="dev_sandbox"')
        pytester.makepyfile(
            """
            import re

            def test_schema(lakeflow):
                with lakeflow.schema() as schema:
                    assert re.fullmatch(r"dev_sandbox\\.pytest_ci_42_[0-9a-f]{8}", schema), schema

            def test_failing(lakeflow):
                with lakeflow.schema():
                    assert False

            def test_other_catalog(lakeflow):
                with lakeflow.schema(catalog="other") as schema:
                    assert schema.startswith("other.pytest_ci_42_")
            """
        )
        pytester.runpytest().assert_outcomes(passed=2, failed=1)
        assert len(set(fake_ws.schemas.created)) == 3
        assert fake_ws.schemas.deleted == fake_ws.schemas.created
        assert fake_cli.calls == []

    @pytest.mark.parametrize(
        ("arguments", "flags"), [('catalog="c", keep=True', []), ('catalog="c"', ["--lakeflow-keep"])]
    )
    def test_schema_is_kept_with_keep(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, arguments: str, flags: list[str]
    ) -> None:
        configure_lakeflow(pytester, arguments)
        pytester.makepyfile(USES_SCHEMA)
        pytester.runpytest(*flags).assert_outcomes(passed=1)
        assert len(fake_ws.schemas.created) == 1
        assert fake_ws.schemas.deleted == []

    def test_schema_needs_a_catalog(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        pytester.makepyfile(USES_SCHEMA)
        result = pytester.runpytest()
        result.assert_outcomes(failed=1)
        result.stdout.fnmatch_lines(["*Lakeflow.schema() needs a catalog*"])
        assert fake_ws.schemas.created == []


class TestSweep:
    """`--lakeflow-sweep` deletes this user's old `pytest` jobs from other runs, once, when the bundle is set up."""

    def test_sweep_deletes_old_jobs_of_other_runs(self) -> None:
        ws = FakeWorkspaceClient()
        ws.jobs.listed = sweep_candidates()
        assert session.sweep(as_client(ws), "this-run", 24, FOLDER).jobs == (1, 7)
        assert ws.jobs.deleted == [1, 7]

    def test_sweep_carries_on_after_a_failed_delete(self, caplog: pytest.LogCaptureFixture) -> None:
        ws = FakeWorkspaceClient()
        ws.jobs.listed = sweep_candidates()
        ws.jobs.delete_errors[1] = PermissionDenied("not yours")
        report = session.sweep(as_client(ws), "this-run", 0.5, FOLDER)
        assert report.jobs == (3, 7)
        assert report.errors == ("Could not delete job 1: not yours",)
        assert "Could not delete job 1: not yours" in caplog.text

    @pytest.mark.parametrize(
        ("arguments", "deleted"),
        [
            pytest.param("", [1, 2, 7], id="default_hours"),
            pytest.param("sweep_hours=0.5", [1, 2, 3, 7], id="half_hour"),
        ],
    )
    def test_sweep_flag_sweeps_once_at_session_start(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, arguments: str, deleted: list[int]
    ) -> None:
        fake_ws.jobs.listed = sweep_candidates()
        configure_lakeflow(pytester, arguments)
        pytester.makepyfile(uses_lakeflow("a", "b"))
        pytester.runpytest("--lakeflow-sweep").assert_outcomes(passed=2)
        assert fake_ws.jobs.deleted == deleted
        assert fake_ws.jobs.list_calls == 1
        assert fake_ws.current_user.me_calls == 1

    def test_xdist_sweep_error_names_the_sweep(self, tmp_path: Path) -> None:
        state = session.SessionState(rootpath=tmp_path, run_id="this-run", state_dir=tmp_path, sweep=True)
        # A hand-written state file: the sweep never shares an error, because produce catches every exception, so
        # this checks only that shared_state relays an error with the sweep's name.
        (tmp_path / session.SWEEP_STATE_FILE).write_text(json.dumps({"error": "PermissionDenied: not yours"}))
        message = r"^Lakeflow sweep failed in another xdist worker: PermissionDenied: not yours$"
        with pytest.raises(LakeflowError, match=message):
            state._sweep(as_client(FakeWorkspaceClient()), {}, 24)

    def test_a_failed_sweep_warns_on_the_sweeping_worker_only(self, pytester: pytest.Pytester) -> None:
        pytest.importorskip("xdist")
        deleted = pytester.path / "deleted.log"
        make_xdist_conftest(pytester, SWEEP_LOGGING.format(deleted=str(deleted)))
        pytester.makepyfile(uses_lakeflow(*"abcdef"))
        (pytester.path / "blocked").write_text("a file, not a directory")
        result = pytester.runpytest("-n", "2", "--lakeflow-sweep", "--lakeflow-sweep-report=blocked/sweep.json")
        result.assert_outcomes(passed=6, warnings=1)
        result.stdout.fnmatch_lines(["*UserWarning: Lakeflow sweep failed: FileExistsError: *blocked*"])
        assert deleted.read_text().split() == ["1", "2", "7"]

    def test_no_sweep_without_the_flag(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        fake_ws.jobs.listed = sweep_candidates()
        pytester.makepyfile(uses_lakeflow("a"))
        pytester.runpytest().assert_outcomes(passed=1)
        assert fake_ws.jobs.list_calls == 0

    def test_sweep_scope_all_ignores_the_creator(self) -> None:
        ws = FakeWorkspaceClient()
        ws.jobs.listed = sweep_candidates()
        options = session.SweepOptions(scope="all")
        assert session.sweep(as_client(ws), "this-run", 24, FOLDER, options).jobs == (1, 4, 7)
        assert ws.jobs.deleted == [1, 4, 7]

    def test_sweep_branch_filter(self) -> None:
        ws = FakeWorkspaceClient()
        ws.jobs.listed = [
            listed_job(1, 25, {**OLD, "pytest_git_branch": "main"}),
            listed_job(2, 25, {**OLD, "pytest_git_branch": "feature/x"}),
            listed_job(3, 25, OLD),
            listed_job(4, 1, {**OLD, "pytest_git_branch": "main"}),
        ]
        options = session.SweepOptions(branch="main")
        assert session.sweep(as_client(ws), "this-run", 24, FOLDER, options).jobs == (1,)
        assert ws.jobs.deleted == [1]

    def test_sweep_deletes_old_pipelines_of_other_runs(self) -> None:
        ws = FakeWorkspaceClient()
        sweep_pipelines(ws)
        report = session.sweep(as_client(ws), "this-run", 24, FOLDER)
        assert report.pipelines == ("p-old", "p-expired")
        assert [each["pipeline_id"] for each in ws.pipelines.deleted] == ["p-old", "p-expired"]
        assert ws.pipelines.list_filters == ["name LIKE '[pytest] %'"]

    @pytest.mark.parametrize(
        ("options", "swept"),
        [
            pytest.param(session.SweepOptions(scope="all"), ("p-old", "p-colleague", "p-expired"), id="scope_all"),
            pytest.param(session.SweepOptions(branch="main"), ("p-old",), id="branch"),
        ],
    )
    def test_sweep_applies_the_job_rules_to_pipelines(
        self, options: session.SweepOptions, swept: tuple[str, ...]
    ) -> None:
        ws = FakeWorkspaceClient()
        sweep_pipelines(ws)
        assert session.sweep(as_client(ws), "this-run", 24, FOLDER, options).pipelines == swept

    def test_sweep_records_a_pipeline_it_cannot_delete(self) -> None:
        ws = FakeWorkspaceClient()
        sweep_pipelines(ws)
        ws.pipelines.delete_errors["p-old"] = PermissionDenied("not yours")
        report = session.sweep(as_client(ws), "this-run", 24, FOLDER)
        assert report.pipelines == ("p-expired",)
        assert report.errors == ("Could not delete pipeline p-old: not yours",)

    def test_sweep_flag_lists_pipelines_once(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        sweep_pipelines(fake_ws)
        pytester.makepyfile(uses_lakeflow("a", "b"))
        pytester.runpytest("--lakeflow-sweep").assert_outcomes(passed=2)
        assert fake_ws.pipelines.list_filters == ["name LIKE '[pytest] %'"]
        swept = [each["pipeline_id"] for each in fake_ws.pipelines.deleted]
        assert swept == ["p-old", "p-this-run", "p-expired"]

    def test_sweep_honours_pytest_expires(self) -> None:
        ws = FakeWorkspaceClient()
        ws.jobs.listed = [
            listed_job(1, 30, {**OLD, "pytest_expires": expires(24)}),
            listed_job(2, 1, {**OLD, "pytest_expires": expires(-24)}),
            listed_job(3, 25, {**OLD, "pytest_expires": "next week"}),
            listed_job(4, 1, {**OLD, "pytest_expires": "next week"}),
            listed_job(5, 1, {"pytest": "", "pytest_run_id": "this-run", "pytest_expires": expires(-24)}),
        ]
        assert session.sweep(as_client(ws), "this-run", 24, FOLDER).jobs == (2, 3)


class TestSweepSchemasAndNotebooks:
    """`--lakeflow-sweep-schemas` and `--lakeflow-sweep-notebooks` drop only what the plugin made, once it is old."""

    @pytest.mark.parametrize(
        ("scope", "dropped"),
        [
            pytest.param("mine", ("dev_sandbox.pytest_old_run_1a2b3c4d",), id="mine"),
            pytest.param(
                "all",
                ("dev_sandbox.pytest_old_run_1a2b3c4d", "dev_sandbox.pytest_old_run_3c4d5e6f"),
                id="all",
            ),
        ],
    )
    def test_sweep_schemas_drops_stale_pytest_schemas(self, scope: str, dropped: tuple[str, ...]) -> None:
        ws = sweep_workspace()
        options = session.SweepOptions(scope=cast(Any, scope), schemas_catalog="dev_sandbox")
        report = session.sweep(as_client(ws), "this-run", 24, FOLDER, options)
        assert report.schemas == dropped
        assert tuple(ws.schemas.deleted) == dropped
        assert ws.workspace.deleted == []

    @pytest.mark.parametrize("role", ["setup", "teardown"])
    def test_sweep_notebooks_keeps_the_helpers(self, role: str) -> None:
        ws = sweep_workspace()
        report = session.sweep(as_client(ws), "this-run", 24, FOLDER, session.SweepOptions(notebooks=True))
        assert f"{FOLDER}/{role}_0123456789ab" in report.notebooks
        assert f"{FOLDER}/{role}_ba9876543210" not in report.notebooks
        assert f"{FOLDER}/{role}_tables" not in report.notebooks
        assert report.notebooks == (f"{FOLDER}/setup_0123456789ab", f"{FOLDER}/teardown_0123456789ab")
        assert ws.workspace.deleted == list(report.notebooks)
        assert ws.schemas.deleted == []

    def test_sweep_records_a_listing_it_cannot_read(self) -> None:
        ws = sweep_workspace()
        ws.schemas.list_errors["dev_sandbox"] = PermissionDenied("no USE CATALOG")
        options = session.SweepOptions(schemas_catalog="dev_sandbox", notebooks=True)
        report = session.sweep(as_client(ws), "this-run", 24, FOLDER, options)
        assert report.errors == ("Could not list the schemas in dev_sandbox: no USE CATALOG",)
        assert report.jobs == (1, 7)
        assert len(report.notebooks) == 2

    @pytest.mark.parametrize(
        ("tags", "age_hours", "swept"),
        [
            pytest.param({**OLD, "pytest_expires": expires(24)}, 30, False, id="expires_tomorrow"),
            pytest.param({**OLD, "pytest_expires": expires(-24)}, 30, True, id="expired_yesterday"),
            pytest.param(OLD, 1, False, id="young"),
            pytest.param({"pytest": "", "pytest_run_id": "this-run"}, 30, False, id="this_run"),
        ],
    )
    def test_sweep_notebooks_spares_those_protected_jobs_use(
        self, fake_ws: FakeWorkspaceClient, tags: dict[str, str], age_hours: float, swept: bool
    ) -> None:
        used = f"{FOLDER}/setup_0123456789ab"
        fake_ws.jobs.listed = [listed_job(1, age_hours, tags, notebooks=[used])]
        fake_ws.workspace.listed = [notebook("setup_0123456789ab", 30)]
        report = session.sweep(as_client(fake_ws), "this-run", 24, FOLDER, session.SweepOptions(notebooks=True))
        assert report.jobs == ((1,) if swept else ())
        assert report.notebooks == ((used,) if swept else ())
        assert fake_ws.jobs.list_kwargs == [{"limit": 100, "expand_tasks": True}]

    def test_sweep_notebooks_reaches_debug_notebooks(self, fake_ws: FakeWorkspaceClient) -> None:
        fake_ws.jobs.listed = [listed_job(1, 30, {**OLD, "pytest_expires": expires(24)}), listed_job(2, 30, OLD)]
        fake_ws.jobs.polls[11] = [jobs.Run(run_id=11, job_id=1)]
        fake_ws.jobs.polls[12] = [jobs.Run(run_id=12, job_id=2)]
        fake_ws.workspace.listed = [
            notebook("debug", 30, workspace.ObjectType.DIRECTORY),
            notebook("my_job__transform__11", 30, folder=DEBUG_FOLDER),
            notebook("my_job__transform__12", 30, folder=DEBUG_FOLDER),
            notebook("my_job__transform__13", 30, folder=DEBUG_FOLDER),
            notebook("my_job__transform__14", 1, folder=DEBUG_FOLDER),
            notebook("notes", 30, folder=DEBUG_FOLDER),
        ]
        report = session.sweep(as_client(fake_ws), "this-run", 24, FOLDER, session.SweepOptions(notebooks=True))
        # Run 11 is of a job the expiry keeps, run 12 of a job this sweep deletes, and run 13 no longer exists.
        assert report.notebooks == (f"{DEBUG_FOLDER}/my_job__transform__12", f"{DEBUG_FOLDER}/my_job__transform__13")
        assert report.jobs == (2,)
        assert report.errors == ()

    def test_scope_all_sweeps_every_users_helpers_folder(self, fake_ws: FakeWorkspaceClient) -> None:
        locked = "/Users/locked@example.com/.pytest-databricks-lakeflow"
        fake_ws.workspace.listed = [
            home("someone@example.com"),
            home("colleague@example.com"),
            home("nobody@example.com"),
            home("locked@example.com"),
            notebook("setup_0123456789ab", 25),
            notebook("setup_0123456789ab", 25, folder=COLLEAGUE_FOLDER),
            notebook("teardown_0123456789ab", 1, folder=COLLEAGUE_FOLDER),
        ]
        fake_ws.workspace.list_errors[locked] = PermissionDenied("not yours")
        options = session.SweepOptions(scope="all", notebooks=True)
        report = session.sweep(as_client(fake_ws), "this-run", 24, FOLDER, options)
        assert report.notebooks == (f"{FOLDER}/setup_0123456789ab", f"{COLLEAGUE_FOLDER}/setup_0123456789ab")
        assert report.errors == (f"Could not list the notebooks in {locked}: not yours",)
        assert "/Users/nobody@example.com/.pytest-databricks-lakeflow" in fake_ws.workspace.list_calls

    def test_branch_sweep_deletes_notebooks_only_its_jobs_use(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        only_main, shared, unused = (f"{FOLDER}/setup_{digit * 12}" for digit in "abc")
        fake_ws.jobs.listed = [
            listed_job(1, 25, {**OLD, "pytest_git_branch": "main"}, notebooks=[only_main, shared]),
            listed_job(2, 25, {**OLD, "pytest_git_branch": "feature-x"}, notebooks=[shared]),
        ]
        fake_ws.workspace.listed = [notebook(posixpath.basename(path), 25) for path in (only_main, shared, unused)]
        pytester.makepyfile(uses_lakeflow("a"))
        flags = ["--lakeflow-sweep-branch=main", "--lakeflow-sweep-notebooks", "--lakeflow-sweep-schemas=dev_sandbox"]
        pytester.runpytest("--lakeflow-sweep", *flags).assert_outcomes(passed=1)
        assert fake_ws.jobs.deleted == [1]
        assert fake_ws.workspace.deleted == [only_main]

    @pytest.mark.parametrize(
        ("branch", "dropped"),
        [
            pytest.param(None, ("expired", "unreadable", "on_main", "on_feature"), id="any_branch"),
            pytest.param("main", ("on_main",), id="main"),
        ],
    )
    def test_sweep_schemas_honours_expiry_and_branch(
        self, fake_ws: FakeWorkspaceClient, branch: str | None, dropped: tuple[str, ...]
    ) -> None:
        names = {
            "kept": (30, {"pytest_expires": expires(24)}),
            "expired": (1, {"pytest_expires": expires(-24)}),
            "unreadable": (25, {"pytest_expires": "next week"}),
            "on_main": (25, {"pytest_git_branch": "main"}),
            "on_feature": (25, {"pytest_git_branch": "feature-x"}),
        }
        for i, (age_hours, properties) in enumerate(names.values()):
            fake_ws.schemas.listed.append(schema(f"pytest_old_run_0000000{i}", age_hours, properties=properties))
        options = session.SweepOptions(schemas_catalog="dev_sandbox", branch=branch)
        report = session.sweep(as_client(fake_ws), "this-run", 24, FOLDER, options)
        expected = tuple(f"dev_sandbox.pytest_old_run_0000000{list(names).index(name)}" for name in dropped)
        assert report.schemas == expected


class TestSweepReport:
    """`--lakeflow-sweep-dry-run` deletes nothing, and `--lakeflow-sweep-report` writes what was or would be swept."""

    def test_sweep_dry_run_deletes_nothing(self, caplog: pytest.LogCaptureFixture) -> None:
        caplog.set_level(logging.INFO, logger="pytest_databricks_lakeflow")
        ws = sweep_workspace()
        options = session.SweepOptions(dry_run=True, schemas_catalog="dev_sandbox", notebooks=True)
        report = session.sweep(as_client(ws), "this-run", 24, FOLDER, options)
        assert (ws.jobs.deleted, ws.schemas.deleted, ws.workspace.deleted) == ([], [], [])
        assert report.dry_run
        assert report.jobs == (1, 7)
        assert report.schemas == ("dev_sandbox.pytest_old_run_1a2b3c4d",)
        assert report.notebooks == (f"{FOLDER}/setup_0123456789ab", f"{FOLDER}/teardown_0123456789ab")
        assert "Would delete job 7" in caplog.text

    def test_sweep_report_is_written(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        fake_ws.jobs.listed = sweep_candidates()
        fake_ws.jobs.delete_errors[1] = PermissionDenied("not yours")
        pytester.makepyfile(uses_lakeflow("a"))
        pytester.runpytest("--lakeflow-sweep", "--lakeflow-sweep-report=out/sweep.json").assert_outcomes(passed=1)
        report = json.loads((pytester.path / "out" / "sweep.json").read_text())
        assert report == {
            "version": 1,
            "dry_run": False,
            "jobs": [2, 7],
            "pipelines": [],
            "schemas": [],
            "notebooks": [],
            "errors": ["Could not delete job 1: not yours"],
        }

    @pytest.mark.parametrize(
        ("flags", "lines"),
        [
            pytest.param(
                [],
                [
                    "lakeflow: sweep dry run deleted nothing; it would delete:",
                    *(f"  job {HOST}/jobs/{i}" for i in (1, 2, 7)),
                ],
                id="jobs",
            ),
            pytest.param(
                ["--lakeflow-sweep-scope=all", "--lakeflow-sweep-schemas=dev_sandbox", "--lakeflow-sweep-notebooks"],
                [
                    "lakeflow: sweep dry run deleted nothing; it would delete:",
                    *(f"  job {HOST}/jobs/{i}" for i in (1, 2, 4, 7)),
                    "  schema dev_sandbox.pytest_old_run_1a2b3c4d",
                    "  schema dev_sandbox.pytest_this_run_0f0f0f0f",
                    "  schema dev_sandbox.pytest_old_run_3c4d5e6f",
                    f"  notebook {FOLDER}/setup_0123456789ab",
                    f"  notebook {FOLDER}/teardown_0123456789ab",
                ],
                id="everything",
            ),
            pytest.param(
                ["--lakeflow-sweep-branch=main"], ["lakeflow: sweep dry run found nothing to delete"], id="branch"
            ),
        ],
    )
    def test_dry_run_is_listed_in_the_terminal_summary(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, flags: list[str], lines: list[str]
    ) -> None:
        sweep_workspace(fake_ws)
        pytester.makepyfile(uses_lakeflow("a"))
        result = pytester.runpytest("--lakeflow-sweep", "--lakeflow-sweep-dry-run", *flags)
        result.assert_outcomes(passed=1)
        assert (fake_ws.jobs.deleted, fake_ws.schemas.deleted, fake_ws.workspace.deleted) == ([], [], [])
        result.stdout.fnmatch_lines(["*= lakeflow =*", *lines])
        result.stdout.no_fnmatch_line("  notebook */set_values")

    def test_dry_run_lists_pipelines(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        sweep_pipelines(fake_ws)
        pytester.makepyfile(uses_lakeflow("a"))
        result = pytester.runpytest("--lakeflow-sweep", "--lakeflow-sweep-dry-run")
        result.assert_outcomes(passed=1)
        assert fake_ws.pipelines.deleted == []
        lines = [f"  pipeline {HOST}/pipelines/{name}" for name in ("p-old", "p-this-run", "p-expired")]
        result.stdout.fnmatch_lines(
            ["*= lakeflow =*", "lakeflow: sweep dry run deleted nothing; it would delete:", *lines]
        )

    def test_xdist_dry_run_is_listed_once_by_the_controller(self, pytester: pytest.Pytester) -> None:
        pytest.importorskip("xdist")
        deleted = pytester.path / "deleted.log"
        make_xdist_conftest(pytester, SWEEP_LOGGING.format(deleted=str(deleted)))
        pytester.makepyfile(uses_lakeflow(*"abcd"))
        result = pytester.runpytest("-n", "2", "--lakeflow-sweep", "--lakeflow-sweep-dry-run")
        result.assert_outcomes(passed=4)
        assert not deleted.exists()
        lines = [
            "lakeflow: sweep dry run deleted nothing; it would delete:",
            *(f"  job {HOST}/jobs/{i}" for i in (1, 2, 7)),
        ]
        result.stdout.fnmatch_lines(["*= lakeflow =*", *lines])
        assert result.stdout.str().count("sweep dry run deleted nothing") == 1

    @pytest.mark.parametrize(
        ("flags", "message"),
        [
            pytest.param(
                ["--lakeflow-sweep-dry-run"],
                "*--lakeflow-sweep-dry-run needs --lakeflow-sweep*",
                id="without_sweep",
            ),
        ],
    )
    def test_sweep_flags_that_cannot_apply_are_usage_errors(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, flags: list[str], message: str
    ) -> None:
        pytester.makepyfile(uses_lakeflow("a"))
        result = pytester.runpytest(*flags)
        assert result.ret == pytest.ExitCode.USAGE_ERROR
        result.stderr.fnmatch_lines([message])
        assert fake_ws.jobs.list_calls == 0


class TestKeepTtl:
    """`--lakeflow-keep-ttl` tags every job with an expiry, which the sweep honours."""

    def test_keep_ttl_tags_every_job(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        pytester.makepyfile("\n".join(CREATES_JOBS.format(name) for name in ("a", "b")))
        before = datetime.now(UTC).replace(microsecond=0)
        pytester.runpytest("--lakeflow-keep-ttl=48h").assert_outcomes(passed=2)
        after = datetime.now(UTC)
        assert len(fake_ws.jobs.created) == 2
        for created in fake_ws.jobs.created:
            value = created["tags"]["pytest_expires"]
            assert value.endswith("Z")
            expiry = datetime.fromisoformat(value)
            assert before + timedelta(hours=48) <= expiry <= after + timedelta(hours=48)

    @pytest.mark.parametrize("value", ["48", "2w", "-1h"])
    def test_keep_ttl_rejects_bad_durations(self, pytester: pytest.Pytester, value: str) -> None:
        pytester.makepyfile(CREATES_JOBS.format("a"))
        result = pytester.runpytest(f"--lakeflow-keep-ttl={value}")
        assert result.ret == pytest.ExitCode.USAGE_ERROR
        result.stderr.fnmatch_lines([f"*--lakeflow-keep-ttl takes hours or days, such as 48h or 7d, not {value!r}*"])

    @pytest.mark.parametrize(("value", "expected"), [("48h", timedelta(hours=48)), ("7d", timedelta(days=7))])
    def test_parse_duration(self, value: str, expected: timedelta) -> None:
        assert parse_duration(value) == expected

    def test_schemas_carry_the_expiry_and_branch(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        set_ci(monkeypatch, {"GITHUB_REF_NAME": "feature/x"})
        configure_lakeflow(pytester, 'catalog="dev_sandbox"')
        pytester.makepyfile(USES_SCHEMA)
        before = datetime.now(UTC).replace(microsecond=0)
        pytester.runpytest("--lakeflow-keep-ttl=48h").assert_outcomes(passed=1)
        after = datetime.now(UTC)
        (properties,) = fake_ws.schemas.properties.values()
        assert properties is not None
        assert properties["pytest_git_branch"] == "feature/x"
        expiry = datetime.fromisoformat(properties["pytest_expires"])
        assert before + timedelta(hours=48) <= expiry <= after + timedelta(hours=48)


class TestSweepFromAScript:
    """`Lakeflow.sweep()` sweeps from plain Python, with no pytest session, bundle or test."""

    def test_lakeflow_sweep_runs_without_a_session(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(session, "_sessions", [])
        ws = sweep_workspace()
        lakeflow = Lakeflow(workspace_client=as_client(ws))
        report = lakeflow.sweep(schemas_catalog="dev_sandbox", notebooks=True, dry_run=True)
        assert isinstance(report, SweepReport)
        assert (ws.jobs.deleted, ws.schemas.deleted, ws.workspace.deleted) == ([], [], [])
        # No run is spared, so the jobs and schemas of "this-run" are listed too.
        assert report.jobs == (1, 2, 7)
        assert report.schemas == ("dev_sandbox.pytest_old_run_1a2b3c4d", "dev_sandbox.pytest_this_run_0f0f0f0f")
        assert report.notebooks == (f"{FOLDER}/setup_0123456789ab", f"{FOLDER}/teardown_0123456789ab")

    def test_lakeflow_sweep_uses_the_instance_settings(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        monkeypatch.setattr(session, "_sessions", [])
        ws = FakeWorkspaceClient()
        ws.jobs.listed = sweep_candidates()
        helpers_path = "/Shared/helpers"
        ws.workspace.listed = [notebook("setup_0123456789ab", 1, folder=helpers_path)]
        lakeflow = Lakeflow(workspace_client=as_client(ws), sweep_hours=0.5, helpers_path=helpers_path)
        report = lakeflow.sweep(branch="main", notebooks=True, report=tmp_path / "sweep.json")
        assert report.jobs == ()
        assert report.notebooks == ()
        lakeflow.sweep(notebooks=True, report=tmp_path / "sweep.json")
        written = json.loads((tmp_path / "sweep.json").read_text())
        assert written["jobs"] == [1, 2, 3, 7]
        assert written["notebooks"] == [f"{helpers_path}/setup_0123456789ab"]


class TestDeletePipeline:
    """A pipeline copy goes with its tables, on every databricks-sdk the plugin supports."""

    @pytest.mark.parametrize(
        ("parameters", "flags"),
        [
            pytest.param({"pipeline_id", "cascade", "force"}, {"cascade": True, "force": True}, id="sdk_0.110"),
            pytest.param({"pipeline_id", "force"}, {"cascade": None, "force": True}, id="sdk_0.100"),
        ],
    )
    def test_delete_pipeline_passes_what_the_sdk_takes(
        self, monkeypatch: pytest.MonkeyPatch, parameters: set[str], flags: dict[str, bool | None]
    ) -> None:
        monkeypatch.setattr(session, "PIPELINE_DELETE_PARAMETERS", frozenset(parameters))
        ws = FakeWorkspaceClient()
        ws.pipelines.specs["p-1"] = pipelines.PipelineSpec(id="p-1")
        session.delete_pipeline(as_client(ws), "p-1")
        assert ws.pipelines.deleted == [{"pipeline_id": "p-1", **flags}]


class TestDestroy:
    """`--lakeflow-destroy` runs `bundle destroy` once per deployed target at session end, after cleanup."""

    def test_destroy_at_session_end(self, pytester: pytest.Pytester, fake_cli: FakeCli) -> None:
        pytester.makepyfile(uses_lakeflow("a", "b"))
        pytester.runpytest("--lakeflow-destroy").assert_outcomes(passed=2)
        assert fake_cli.calls[-1] == DESTROY
        assert fake_cli.calls.count(DESTROY) == 1

    def test_destroy_each_deployed_target(self, pytester: pytest.Pytester, fake_cli: FakeCli) -> None:
        configure_lakeflow(pytester, 'profile="dev"', STAGING_FIXTURE)
        pytester.makepyfile("def test_a(lakeflow, staging):\n    lakeflow.template('my_job')\n    staging.templates\n")
        pytester.runpytest("--lakeflow-destroy").assert_outcomes(passed=1)
        destroys = [(args, profile) for args, _, profile in fake_cli.invocations if args[:2] == ["bundle", "destroy"]]
        assert destroys == [(DESTROY, "dev"), (DESTROY_STAGING, None)]

    def test_destroy_uses_the_deploy_variables(self, pytester: pytest.Pytester, fake_cli: FakeCli) -> None:
        configure_lakeflow(pytester, 'variables={"catalog": "dev"}')
        pytester.makepyfile(uses_lakeflow("a"))
        pytester.runpytest("--lakeflow-destroy").assert_outcomes(passed=1)
        assert fake_cli.calls[-1] == [*DESTROY, "--var", "catalog=dev"]
        assert DEPLOY_WITH_VARIABLES in fake_cli.calls

    def test_xdist_destroy_uses_the_deploy_variables(self, pytester: pytest.Pytester, fake_cli: FakeCli) -> None:
        pytest.importorskip("xdist")
        log = make_xdist_conftest(pytester, VARIABLES_FIXTURE)
        pytester.makepyfile(uses_lakeflow(*map(str, range(4))))
        pytester.runpytest("-n", "2", "--lakeflow-destroy").assert_outcomes(passed=4)
        assert log.read_text().splitlines().count(" ".join(DEPLOY_WITH_VARIABLES)) == 1
        assert fake_cli.calls == [[*DESTROY, "--var", "catalog=dev"]]

    def test_xdist_controller_destroys_once(self, pytester: pytest.Pytester, fake_cli: FakeCli) -> None:
        pytest.importorskip("xdist")
        log = make_xdist_conftest(pytester)
        pytester.makepyfile(uses_lakeflow(*map(str, range(4))))
        pytester.runpytest("-n", "2", "--lakeflow-destroy").assert_outcomes(passed=4)
        assert " ".join(DESTROY) not in log.read_text().splitlines()
        assert fake_cli.calls == [DESTROY]

    def test_no_destroy_without_the_flag(self, pytester: pytest.Pytester, fake_cli: FakeCli) -> None:
        pytester.makepyfile(uses_lakeflow("a"))
        pytester.runpytest().assert_outcomes(passed=1)
        assert DESTROY not in fake_cli.calls

    @pytest.mark.parametrize(
        ("source", "flags", "outcomes"),
        [
            pytest.param("def test_a(lakeflow):\n    pass\n", [], {"passed": 1}, id="fixture_unused"),
            pytest.param(
                uses_lakeflow("a") + "\ndef test_b():\n    pass\n",
                ["-m", "not lakeflow"],
                {"passed": 1, "deselected": 1},
                id="lakeflow_tests_deselected",
            ),
        ],
    )
    def test_no_destroy_when_nothing_was_set_up(
        self, pytester: pytest.Pytester, fake_cli: FakeCli, source: str, flags: list[str], outcomes: dict[str, int]
    ) -> None:
        pytester.makepyfile(source)
        pytester.runpytest("--lakeflow-destroy", *flags).assert_outcomes(**outcomes)
        assert fake_cli.calls == []

    def test_no_destroy_with_sync(self, pytester: pytest.Pytester, fake_cli: FakeCli) -> None:
        configure_lakeflow(pytester, 'sync="sync"')
        pytester.makepyfile("def test_a(lakeflow):\n    lakeflow.templates\n")
        pytester.runpytest("--lakeflow-destroy").assert_outcomes(passed=1)
        assert ["bundle", "sync", "-t", "test"] in fake_cli.calls
        assert DESTROY not in fake_cli.calls

    def test_setup_failure_skips_destroy(self, pytester: pytest.Pytester, fake_cli: FakeCli) -> None:
        fake_cli.errors["deploy"] = LakeflowBundleError("deploy broke")
        pytester.makepyfile(uses_lakeflow("a"))
        pytester.runpytest("--lakeflow-destroy").assert_outcomes(failed=1)
        assert DESTROY not in fake_cli.calls

    @pytest.mark.parametrize(
        ("extra_source", "lines"),
        [
            pytest.param("", ["*1 passed*"], id="passing_session"),
            pytest.param(
                "\ndef test_b(lakeflow):\n    assert False\n",
                ["FAILED test_destroy_failure_fails_the_session.py::test_b*", "*1 failed, 1 passed*"],
                id="failing_session",
            ),
        ],
    )
    def test_destroy_failure_fails_the_session(
        self, pytester: pytest.Pytester, fake_cli: FakeCli, extra_source: str, lines: list[str]
    ) -> None:
        fake_cli.errors["destroy"] = LakeflowBundleError(
            "`databricks bundle destroy` failed with exit code 1:\nError: x"
        )
        pytester.makepyfile(uses_lakeflow("a") + extra_source)
        result = pytester.runpytest("--lakeflow-destroy")
        assert result.ret == pytest.ExitCode.TESTS_FAILED
        result.stdout.fnmatch_lines(["*lakeflow*destroy failed*", "*Error: x*"])
        result.stdout.fnmatch_lines(lines)

    def test_destroy_runs_after_interrupted_tests_are_cleaned_up(
        self,
        pytester: pytest.Pytester,
        fake_ws: FakeWorkspaceClient,
        fake_cli: FakeCli,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        order: list[str] = []

        def record(args: list[str]) -> None:
            if len(args) > 1:
                order.append(f"cli {args[1]} deleted={fake_ws.jobs.deleted}")

        def interrupted(run_id: int, **kwargs: Any) -> jobs.Run:
            raise KeyboardInterrupt

        fake_cli.before = record
        fake_ws.jobs.queue(make_run(77, {"t": "SUCCESS"}))
        monkeypatch.setattr(fake_ws.jobs, "get_run", interrupted)
        pytester.makepyfile('def test_job(lakeflow):\n    lakeflow.job("my_job").run()\n')
        pytester.runpytest("--lakeflow-destroy", no_reraise_ctrlc=True)
        assert fake_ws.jobs.cancelled == [77]
        assert order[-1] == "cli destroy deleted=[1001]"


class TestSessionEnd:
    """Session end cancels the runs that tests left active and copes with xdist workers that report nothing."""

    @pytest.mark.parametrize(("runs", "cancelled", "line"), [("{1}", [1], "1 run"), ("{3, 1}", [1, 3], "2 runs")])
    def test_runs_left_active_are_cancelled_and_counted(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, runs: str, cancelled: list[int], line: str
    ) -> None:
        pytester.makepyfile(leaves_runs_active(runs))
        result = pytester.runpytest()
        result.assert_outcomes(passed=1)
        assert fake_ws.jobs.cancelled == cancelled
        result.stdout.fnmatch_lines(["*= lakeflow =*", f"lakeflow: {line} cancelled at session end"])
        result.stdout.no_fnmatch_line("*kept*")

    def test_updates_left_active_are_stopped(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        pytester.makepyfile(USES_LAKEFLOW.format("leak") + '    lakeflow.active_updates["p-1"] = "u-1"\n')
        pytester.runpytest().assert_outcomes(passed=1)
        assert fake_ws.pipelines.stopped == ["p-1"]

    def test_crashed_xdist_worker_is_reported_normally(self, pytester: pytest.Pytester) -> None:
        pytest.importorskip("xdist")
        pytester.makepyfile(
            "import os\n\ndef test_crash():\n    os._exit(3)\n\ndef test_a():\n    pass\n\ndef test_b():\n    pass\n"
        )
        result = pytester.runpytest("-n", "2")
        assert "INTERNALERROR" not in result.stdout.str() + result.stderr.str()
        result.stdout.fnmatch_lines(["*crashed while running*test_crash*", "*1 failed, 2 passed*"])


class TestTerminalSummary:
    """The `lakeflow` terminal section lists kept jobs and schemas from every xdist worker, only when there are any."""

    def test_lists_kept_jobs(self, pytester: pytest.Pytester) -> None:
        pytester.makepyfile(FAILING_JOB.format("kept") + "\n" + USES_LAKEFLOW.format("other"))
        result = pytester.runpytest()
        result.assert_outcomes(failed=1, passed=1)
        result.stdout.fnmatch_lines(["*= lakeflow =*", "lakeflow: 1 job kept", f"  {HOST}/jobs/1001", SWEEP_HINT])
        result.stdout.no_fnmatch_line("*schema* kept*")
        result.stdout.no_fnmatch_line("*cancelled at session end*")

    def test_lists_kept_schemas(self, pytester: pytest.Pytester) -> None:
        configure_lakeflow(pytester, 'catalog="c"')
        pytester.makepyfile(USES_SCHEMA)
        result = pytester.runpytest("--lakeflow-keep")
        result.assert_outcomes(passed=1)
        result.stdout.fnmatch_lines(["*= lakeflow =*", "lakeflow: 1 schema kept", "  c.pytest_*"])
        result.stdout.no_fnmatch_line(SWEEP_HINT)

    def test_no_summary_when_nothing_was_kept(self, pytester: pytest.Pytester) -> None:
        pytester.makepyfile("def test_job(lakeflow):\n    lakeflow.job('my_job').run()\n")
        result = pytester.runpytest()
        result.assert_outcomes(passed=1)
        result.stdout.no_fnmatch_line("*= lakeflow =*")
        result.stdout.no_fnmatch_line("lakeflow:*")

    def test_xdist_controller_summarises_kept_jobs(self, pytester: pytest.Pytester) -> None:
        pytest.importorskip("xdist")
        make_xdist_conftest(pytester, XDIST_WORKER_IDS)
        pytester.makepyfile(FAILING_JOB.format("a") + "\n" + FAILING_JOB.format("b"))
        result = pytester.runpytest("-n", "2")
        result.assert_outcomes(failed=2)
        result.stdout.fnmatch_lines(["*= lakeflow =*", "lakeflow: 2 jobs kept", f"  {HOST}/jobs/*", f"  {HOST}/jobs/*"])
        urls = [line for line in result.stdout.lines if line.startswith(f"  {HOST}/jobs/")]
        assert len(urls) == 2
        assert len(set(urls)) == 2

    def test_xdist_controller_summarises_kept_pipelines(self, pytester: pytest.Pytester) -> None:
        pytest.importorskip("xdist")
        make_xdist_conftest(pytester, XDIST_PIPELINE_JOB)
        pytester.makepyfile(FAILING_COPY.format("a") + "\n" + FAILING_COPY.format("b"))
        result = pytester.runpytest("-n", "2")
        result.assert_outcomes(failed=2)
        pipeline = f"  {HOST}/pipelines/p-1000"
        result.stdout.fnmatch_lines(
            ["*= lakeflow =*", "lakeflow: 2 jobs kept", "lakeflow: 2 pipelines kept", pipeline, pipeline]
        )

    def test_lists_debug_notebooks(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        queue_failed_run(fake_ws)
        pytester.makepyfile(FAILED_RUN.format("failed"))
        result = pytester.runpytest("--lakeflow-debug-notebook")
        result.assert_outcomes(failed=1)
        path = f"{FOLDER}/debug/my_job__transform__77"
        url = f"{HOST}/#notebook/{fake_ws.workspace.object_ids[path]}"
        result.stdout.fnmatch_lines(
            [
                "*= lakeflow =*",
                "lakeflow: 1 job kept",
                "*",
                "lakeflow: 1 debug notebook written",
                f"  {url} (test_lists_debug_notebooks.py::test_failed, task 'transform')",
            ]
        )

    def test_xdist_controller_summarises_debug_notebooks(self, pytester: pytest.Pytester) -> None:
        pytest.importorskip("xdist")
        make_xdist_conftest(pytester, XDIST_FAILED_TRANSFORM)
        pytester.makepyfile(FAILED_RUN.format("a") + "\n" + FAILED_RUN.format("b"))
        result = pytester.runpytest("-n", "2", "--lakeflow-debug-notebook")
        result.assert_outcomes(failed=2)
        result.stdout.fnmatch_lines(["*= lakeflow =*", "lakeflow: 2 debug notebooks written", f"  {HOST}/#notebook/*"])
