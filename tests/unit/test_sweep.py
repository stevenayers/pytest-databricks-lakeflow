"""Sweeping the jobs, pipeline copies, schemas and notebooks that earlier sessions left behind."""

from __future__ import annotations

import json
import logging
import posixpath
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest
from databricks.sdk.errors import PermissionDenied
from databricks.sdk.service import catalog, jobs, pipelines, workspace

from pytest_databricks_lakeflow import Lakeflow, LakeflowError, SweepReport, session, sweep, workspace_api
from pytest_databricks_lakeflow.plugin import parse_duration

from .conftest import (
    FOLDER,
    SWEEP_LOGGING,
    USES_SCHEMA,
    as_client,
    configure_lakeflow,
    make_xdist_conftest,
    set_ci,
    uses_lakeflow,
)
from .fakes.runs import HOST, HOUR_MS, listed_job, sweep_candidates
from .fakes.workspace import FakeWorkspaceClient

COLLEAGUE_FOLDER = "/Users/colleague@example.com/.pytest-databricks-lakeflow"
DEBUG_FOLDER = f"{FOLDER}/debug"
OLD = {"pytest": "", "pytest_run_id": "old-run"}
SETUP_NOTEBOOK = f"{FOLDER}/setup_0123456789ab"
CREATES_JOBS = "def test_{0}(lakeflow):\n    with lakeflow.job('my_job'):\n        pass\n"


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


@pytest.mark.usefixtures("fake_cli", "bundle_dir")
class TestSweep:
    """`--lakeflow-sweep` deletes this user's old `pytest` jobs from other runs, once, when the bundle is set up."""

    def test_sweep_deletes_old_jobs_of_other_runs(self) -> None:
        ws = FakeWorkspaceClient()
        ws.jobs.listed = sweep_candidates()
        assert sweep.sweep(as_client(ws), "this-run", 24, FOLDER).jobs == (1, 7)
        assert ws.jobs.deleted == [1, 7]

    def test_sweep_carries_on_after_a_failed_delete(self, caplog: pytest.LogCaptureFixture) -> None:
        ws = FakeWorkspaceClient()
        ws.jobs.listed = sweep_candidates()
        ws.jobs.delete_errors[1] = PermissionDenied("not yours")
        report = sweep.sweep(as_client(ws), "this-run", 0.5, FOLDER)
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
        options = sweep.SweepOptions(scope="all")
        assert sweep.sweep(as_client(ws), "this-run", 24, FOLDER, options).jobs == (1, 4, 7)
        assert ws.jobs.deleted == [1, 4, 7]

    def test_sweep_branch_filter(self) -> None:
        ws = FakeWorkspaceClient()
        ws.jobs.listed = [
            listed_job(1, 25, {**OLD, "pytest_git_branch": "main"}),
            listed_job(2, 25, {**OLD, "pytest_git_branch": "feature/x"}),
            listed_job(3, 25, OLD),
            listed_job(4, 1, {**OLD, "pytest_git_branch": "main"}),
        ]
        options = sweep.SweepOptions(branch="main")
        assert sweep.sweep(as_client(ws), "this-run", 24, FOLDER, options).jobs == (1,)
        assert ws.jobs.deleted == [1]

    def test_sweep_deletes_old_pipelines_of_other_runs(self) -> None:
        ws = FakeWorkspaceClient()
        sweep_pipelines(ws)
        report = sweep.sweep(as_client(ws), "this-run", 24, FOLDER)
        assert report.pipelines == ("p-old", "p-expired")
        assert [each["pipeline_id"] for each in ws.pipelines.deleted] == ["p-old", "p-expired"]
        assert ws.pipelines.list_filters == ["name LIKE '[pytest] %'"]

    @pytest.mark.parametrize(
        ("options", "swept"),
        [
            pytest.param(sweep.SweepOptions(scope="all"), ("p-old", "p-colleague", "p-expired"), id="scope_all"),
            pytest.param(sweep.SweepOptions(branch="main"), ("p-old",), id="branch"),
        ],
    )
    def test_sweep_applies_the_job_rules_to_pipelines(
        self, options: sweep.SweepOptions, swept: tuple[str, ...]
    ) -> None:
        ws = FakeWorkspaceClient()
        sweep_pipelines(ws)
        assert sweep.sweep(as_client(ws), "this-run", 24, FOLDER, options).pipelines == swept

    def test_sweep_records_a_pipeline_it_cannot_delete(self) -> None:
        ws = FakeWorkspaceClient()
        sweep_pipelines(ws)
        ws.pipelines.delete_errors["p-old"] = PermissionDenied("not yours")
        report = sweep.sweep(as_client(ws), "this-run", 24, FOLDER)
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
        assert sweep.sweep(as_client(ws), "this-run", 24, FOLDER).jobs == (2, 3)


@pytest.mark.usefixtures("fake_cli", "bundle_dir")
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
        options = sweep.SweepOptions(scope=cast(Any, scope), schemas_catalog="dev_sandbox")
        report = sweep.sweep(as_client(ws), "this-run", 24, FOLDER, options)
        assert report.schemas == dropped
        assert tuple(ws.schemas.deleted) == dropped
        assert ws.workspace.deleted == []

    @pytest.mark.parametrize("role", ["setup", "teardown"])
    def test_sweep_notebooks_keeps_the_helpers(self, role: str) -> None:
        ws = sweep_workspace()
        report = sweep.sweep(as_client(ws), "this-run", 24, FOLDER, sweep.SweepOptions(notebooks=True))
        assert f"{FOLDER}/{role}_0123456789ab" in report.notebooks
        assert f"{FOLDER}/{role}_ba9876543210" not in report.notebooks
        assert f"{FOLDER}/{role}_tables" not in report.notebooks
        assert report.notebooks == (f"{FOLDER}/setup_0123456789ab", f"{FOLDER}/teardown_0123456789ab")
        assert ws.workspace.deleted == list(report.notebooks)
        assert ws.schemas.deleted == []

    def test_sweep_records_a_listing_it_cannot_read(self) -> None:
        ws = sweep_workspace()
        ws.schemas.list_errors["dev_sandbox"] = PermissionDenied("no USE CATALOG")
        options = sweep.SweepOptions(schemas_catalog="dev_sandbox", notebooks=True)
        report = sweep.sweep(as_client(ws), "this-run", 24, FOLDER, options)
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
        report = sweep.sweep(as_client(fake_ws), "this-run", 24, FOLDER, sweep.SweepOptions(notebooks=True))
        assert report.jobs == ((1,) if swept else ())
        assert report.notebooks == ((used,) if swept else ())
        assert fake_ws.jobs.list_kwargs == [{"limit": 100, "expand_tasks": True}]

    def test_sweep_notebooks_reaches_debug_notebooks(self, fake_ws: FakeWorkspaceClient) -> None:
        fake_ws.jobs.listed = [listed_job(1, 30, {**OLD, "pytest_expires": expires(24)}), listed_job(2, 30, OLD)]
        fake_ws.jobs.polls[11] = [jobs.Run(run_id=11, job_id=1)]
        fake_ws.jobs.polls[12] = [jobs.Run(run_id=12, job_id=2)]
        # Like runs/get, a run of a job deleted before this sweep still names its job.
        fake_ws.jobs.polls[15] = [jobs.Run(run_id=15, job_id=99)]
        fake_ws.workspace.listed = [
            notebook("debug", 30, workspace.ObjectType.DIRECTORY),
            notebook("my_job__transform__11", 30, folder=DEBUG_FOLDER),
            notebook("my_job__transform__12", 30, folder=DEBUG_FOLDER),
            notebook("my_job__transform__13", 30, folder=DEBUG_FOLDER),
            notebook("my_job__transform__14", 1, folder=DEBUG_FOLDER),
            notebook("my_job__transform__15", 30, folder=DEBUG_FOLDER),
            notebook("notes", 30, folder=DEBUG_FOLDER),
        ]
        report = sweep.sweep(as_client(fake_ws), "this-run", 24, FOLDER, sweep.SweepOptions(notebooks=True))
        # Run 11 is of a job the expiry keeps, run 12 of a job this sweep deletes, run 13 no longer exists, and run 15
        # is of a job that is no longer listed.
        assert report.notebooks == (
            f"{DEBUG_FOLDER}/my_job__transform__12",
            f"{DEBUG_FOLDER}/my_job__transform__13",
            f"{DEBUG_FOLDER}/my_job__transform__15",
        )
        assert report.jobs == (2,)
        assert report.errors == ()

    @pytest.mark.parametrize(
        ("broken", "error", "swept"),
        [
            pytest.param("jobs.list", "Could not list the jobs: no access", (), id="jobs_unlisted"),
            pytest.param(
                "jobs.get_run",
                f"Could not read run 12 of notebook {DEBUG_FOLDER}/my_job__transform__12: no access",
                (f"{FOLDER}/setup_0123456789ab",),
                id="run_unreadable",
            ),
        ],
    )
    def test_sweep_notebooks_keeps_what_it_cannot_judge(
        self, fake_ws: FakeWorkspaceClient, broken: str, error: str, swept: tuple[str, ...]
    ) -> None:
        fake_ws.jobs.listed = [listed_job(1, 30, OLD)]
        fake_ws.jobs.polls[12] = [jobs.Run(run_id=12, job_id=1)]
        fake_ws.workspace.listed = [
            notebook("setup_0123456789ab", 30),
            notebook("debug", 30, workspace.ObjectType.DIRECTORY),
            notebook("my_job__transform__12", 30, folder=DEBUG_FOLDER),
        ]
        if broken == "jobs.list":
            fake_ws.jobs.list_error = PermissionDenied("no access")
        else:
            fake_ws.jobs.run_errors[12] = PermissionDenied("no access")
        report = sweep.sweep(as_client(fake_ws), "this-run", 24, FOLDER, sweep.SweepOptions(notebooks=True))
        assert report.notebooks == swept
        assert fake_ws.workspace.deleted == list(swept)
        assert report.errors == (error,)

    @pytest.mark.parametrize(
        "task",
        [
            pytest.param(
                jobs.Task(
                    task_key="loop",
                    for_each_task=jobs.ForEachTask(
                        inputs="[1]",
                        task=jobs.Task(task_key="body", notebook_task=jobs.NotebookTask(notebook_path=SETUP_NOTEBOOK)),
                    ),
                ),
                id="for_each_body",
            ),
            pytest.param(
                jobs.Task(
                    task_key="query", sql_task=jobs.SqlTask(warehouse_id="w1", file=jobs.SqlTaskFile(SETUP_NOTEBOOK))
                ),
                id="sql_file",
            ),
            pytest.param(
                jobs.Task(
                    task_key="setup", notebook_task=jobs.NotebookTask(notebook_path=f"/Workspace{SETUP_NOTEBOOK}")
                ),
                id="workspace_prefix",
            ),
        ],
    )
    def test_sweep_notebooks_spares_every_kind_of_use(self, fake_ws: FakeWorkspaceClient, task: jobs.Task) -> None:
        fake_ws.jobs.listed = [listed_job(1, 30, {**OLD, "pytest_expires": expires(24)}, tasks=[task])]
        fake_ws.workspace.listed = [notebook("setup_0123456789ab", 30), notebook("teardown_0123456789ab", 30)]
        report = sweep.sweep(as_client(fake_ws), "this-run", 24, FOLDER, sweep.SweepOptions(notebooks=True))
        assert report.notebooks == (f"{FOLDER}/teardown_0123456789ab",)

    def test_sweep_notebooks_reaches_inlined_query_files(self, fake_ws: FakeWorkspaceClient) -> None:
        used = f"{FOLDER}/query_old-run_totals_0123abcd.sql"
        query = jobs.Task(
            task_key="totals", sql_task=jobs.SqlTask(warehouse_id="w1", file=jobs.SqlTaskFile(f"/Workspace{used}"))
        )
        fake_ws.jobs.listed = [listed_job(1, 30, {**OLD, "pytest_expires": expires(24)}, tasks=[query])]
        fake_ws.workspace.listed = [
            notebook("query_old-run_totals_0123abcd.sql", 30, workspace.ObjectType.FILE),
            notebook("query_old-run_each_89abcdef.sql", 30, workspace.ObjectType.FILE),
            notebook("query_old-run_each_fedcba98.sql", 1, workspace.ObjectType.FILE),
            notebook("query_totals.sql", 30, workspace.ObjectType.FILE),
            notebook("query_old-run_each_76543210.sql", 30),
        ]
        report = sweep.sweep(as_client(fake_ws), "this-run", 24, FOLDER, sweep.SweepOptions(notebooks=True))
        # The first is used by a job the expiry keeps, the third is young, and the last two are not named as the
        # plugin names a query file.
        assert report.notebooks == (f"{FOLDER}/query_old-run_each_89abcdef.sql",)
        assert fake_ws.workspace.deleted == list(report.notebooks)

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
        options = sweep.SweepOptions(scope="all", notebooks=True)
        report = sweep.sweep(as_client(fake_ws), "this-run", 24, FOLDER, options)
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
        options = sweep.SweepOptions(schemas_catalog="dev_sandbox", branch=branch)
        report = sweep.sweep(as_client(fake_ws), "this-run", 24, FOLDER, options)
        expected = tuple(f"dev_sandbox.pytest_old_run_0000000{list(names).index(name)}" for name in dropped)
        assert report.schemas == expected


@pytest.mark.usefixtures("fake_cli", "bundle_dir")
class TestSweepReport:
    """`--lakeflow-sweep-dry-run` deletes nothing, and `--lakeflow-sweep-report` writes what was or would be swept."""

    def test_sweep_dry_run_deletes_nothing(self, caplog: pytest.LogCaptureFixture) -> None:
        caplog.set_level(logging.INFO, logger="pytest_databricks_lakeflow")
        ws = sweep_workspace()
        options = sweep.SweepOptions(dry_run=True, schemas_catalog="dev_sandbox", notebooks=True)
        report = sweep.sweep(as_client(ws), "this-run", 24, FOLDER, options)
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


@pytest.mark.usefixtures("fake_cli", "bundle_dir")
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
        monkeypatch.setattr(workspace_api, "PIPELINE_DELETE_PARAMETERS", frozenset(parameters))
        ws = FakeWorkspaceClient()
        ws.pipelines.specs["p-1"] = pipelines.PipelineSpec(id="p-1")
        workspace_api.delete_pipeline(as_client(ws), "p-1")
        assert ws.pipelines.deleted == [{"pipeline_id": "p-1", **flags}]
