"""Saved query tasks inlined as SQL file tasks with `saved_queries="inline"`."""

from __future__ import annotations

import re
from typing import Any

import pytest
from databricks.sdk.errors import PermissionDenied
from databricks.sdk.service import jobs, sql
from databricks.sdk.service.workspace import ImportFormat

from .conftest import DENIED, FakeCli, configure_lakeflow, deny_create, run_tests
from .fakes.templates import QUERY_TEXT, saved_queries, saved_query_settings
from .fakes.workspace import FakeWorkspaceClient

QUERY_FILE_PATH = re.compile(
    r"/Users/someone@example\.com/\.pytest-databricks-lakeflow/query_[A-Za-z0-9_-]+_(?P<task>\w+)_[0-9a-f]{8}\.sql"
)
TOTALS_SOURCE = f"USE CATALOG `main`;\nUSE SCHEMA `sales`;\n{QUERY_TEXT}\n"


@pytest.fixture
def reports(fake_ws: FakeWorkspaceClient, fake_cli: FakeCli) -> None:
    """Deploy `saved_query_settings` as `reports`, with its saved queries q1 and q2 in the workspace."""
    fake_cli.deployed["reports"] = fake_ws.jobs.add(saved_query_settings())
    fake_ws.queries.queries = saved_queries()


def created_sql_tasks(created: dict[str, Any]) -> dict[str, jobs.SqlTask]:
    """The `sql_task` of each SQL task of a created job, nested ones included, by task key."""
    found: dict[str, jobs.SqlTask] = {}
    for task in created["tasks"]:
        if task.for_each_task is not None:
            task = task.for_each_task.task
        if task.sql_task is not None:
            found[task.task_key] = task.sql_task
    return found


def query_files(ws: FakeWorkspaceClient) -> dict[str, dict[str, Any]]:
    """The uploads of inlined saved query files, by workspace path."""
    return {path: upload for path, upload in ws.workspace.uploads.items() if QUERY_FILE_PATH.fullmatch(path)}


@pytest.mark.usefixtures("fake_cli", "bundle_dir", "reports")
class TestSavedQueries:
    """`saved_queries="inline"` runs each saved query task of a copy as a SQL file task holding the query's text."""

    def test_inline_runs_the_query_text_from_a_helper_file(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        result = run_tests(
            pytester,
            'def test_job(lakeflow):\n    with lakeflow.job("reports", saved_queries="inline"):\n        pass\n',
        )
        result.assert_outcomes(passed=1)
        assert fake_ws.queries.get_calls == ["q1", "q2"]
        tasks = created_sql_tasks(fake_ws.jobs.created[0])
        uploads = query_files(fake_ws)
        sources: dict[str, str] = {}
        for key, sql_task in tasks.items():
            assert sql_task.query is None
            assert sql_task.warehouse_id == "w1"
            assert sql_task.file is not None and sql_task.file.source == jobs.Source.WORKSPACE
            match = QUERY_FILE_PATH.fullmatch(sql_task.file.path)
            assert match is not None and match["task"] == key
            assert uploads[sql_task.file.path]["format"] == ImportFormat.RAW
            sources[key] = uploads[sql_task.file.path]["content"]
        # A legacy {{ }} marker binds from the parameters once inlined, so the text is kept as written.
        assert sources == {"totals": TOTALS_SOURCE, "each": "SELECT {{ size }} AS size\n"}
        # The task's own value wins, a job parameter binds its marker itself, and the other defaults are carried.
        assert tasks["totals"].parameters == {"limit": "5", "region": "emea", "ratio": "2.5"}
        assert tasks["each"].parameters == {"size": "small"}
        assert sorted(fake_ws.workspace.deleted) == sorted(uploads)

    def test_deployed_is_the_default(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        result = run_tests(pytester, 'def test_job(lakeflow):\n    lakeflow.job("reports").create()\n')
        result.assert_outcomes(passed=1)
        assert fake_ws.queries.get_calls == []
        assert query_files(fake_ws) == {}
        tasks = created_sql_tasks(fake_ws.jobs.created[0])
        assert {key: task.query for key, task in tasks.items()} == {
            "totals": jobs.SqlTaskQuery(query_id="q1"),
            "each": jobs.SqlTaskQuery(query_id="q2"),
        }

    def test_session_default_applies_to_job_and_task(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        configure_lakeflow(pytester, 'saved_queries="inline"')
        result = run_tests(
            pytester,
            """
            def test_job(lakeflow):
                lakeflow.job("reports").create()
                lakeflow.task("reports.totals", upstream_task_values={"extract": {}}).create()
                lakeflow.job("reports", saved_queries="deployed").create()
            """,
        )
        result.assert_outcomes(passed=1)
        assert fake_ws.queries.get_calls == ["q1", "q2", "q1"]
        job, task, deployed = (created_sql_tasks(created) for created in fake_ws.jobs.created)
        assert [sql_task.query for sql_task in job.values()] == [None, None]
        assert task["totals"].file is not None and task["totals"].query is None
        assert [sql_task.file for sql_task in deployed.values()] == [None, None]

    def test_task_params_win_over_the_defaults(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        result = run_tests(
            pytester,
            """
            def test_job(lakeflow):
                task_params = {"totals": {"region": "apac"}}
                lakeflow.job("reports", saved_queries="inline", task_params=task_params).create()
            """,
        )
        result.assert_outcomes(passed=1)
        totals = created_sql_tasks(fake_ws.jobs.created[0])["totals"]
        assert totals.parameters == {"limit": "5", "region": "apac", "ratio": "2.5"}

    def test_disabled_tasks_are_left_as_deployed(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        result = run_tests(
            pytester,
            """
            def test_job(lakeflow):
                lakeflow.job("reports", saved_queries="inline", disable=["extract"]).create()
            """,
        )
        result.assert_outcomes(passed=1)
        assert fake_ws.queries.get_calls == ["q2"], "totals is downstream of the disabled extract"
        assert created_sql_tasks(fake_ws.jobs.created[0])["totals"].query == jobs.SqlTaskQuery(query_id="q1")

    @pytest.mark.parametrize(
        ("default", "kind"),
        [
            pytest.param({"date_value": {"date_value": "2026-01-01"}}, "a date default", id="date"),
            pytest.param(
                {"date_range_value": {"date_range_value": {"start": "2026-01-01", "end": "2026-01-31"}}},
                "a date range default",
                id="date_range",
            ),
            pytest.param(
                {"enum_value": {"values": ["a", "b"], "multi_values_options": {"separator": ","}}},
                "a multi-value enum default",
                id="multi_enum",
            ),
            pytest.param(
                {"query_backed_value": {"query_id": "q9", "values": ["a"]}}, "a query-backed default", id="query"
            ),
        ],
    )
    def test_a_default_that_cannot_be_carried_is_refused(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, default: dict[str, Any], kind: str
    ) -> None:
        for query_id, name in (("q1", "region"), ("q2", "size")):
            query = fake_ws.queries.queries[query_id]
            query.parameters = [sql.QueryParameter.from_dict({"name": name, **default})]
        result = run_tests(
            pytester,
            """
            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            def test_job(lakeflow):
                with pytest.raises(LakeflowError) as top_level:
                    lakeflow.job("reports", saved_queries="inline")
                print(top_level.value)
                with pytest.raises(LakeflowError) as nested:
                    lakeflow.job("reports", saved_queries="inline", task_params={"totals": {"region": "apac"}})
                print(nested.value)
                task_params = {"each": {"size": "large"}}
                lakeflow.task("reports.loop.each", saved_queries="inline", task_params=task_params).create()
            """,
            "-s",
        )
        result.assert_outcomes(passed=1)
        refused = f'has {kind}, which saved_queries="inline" cannot carry into the SQL file. Set it with'
        result.stdout.fnmatch_lines(
            [
                f"*Task 'totals' of 'reports' runs saved query q1, whose parameter 'region' {refused} "
                "task_params={'totals': {'region': '<value>'}}.",
                f"Task 'each' of 'reports' runs saved query q2, whose parameter 'size' {refused} "
                "lakeflow.task('reports.loop.each', task_params={'each': {'size': '<value>'}}).",
            ]
        )
        assert len(fake_ws.jobs.created) == 1
        assert query_files(fake_ws) != {}
        assert created_sql_tasks(fake_ws.jobs.created[0])["each"].parameters == {"size": "large"}

    def test_a_missing_query_is_refused(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        del fake_ws.queries.queries["q1"]
        result = run_tests(pytester, 'def test_job(lakeflow):\n    lakeflow.job("reports", saved_queries="inline")\n')
        result.assert_outcomes(failed=1)
        result.stdout.fnmatch_lines(["*LakeflowError: Task 'totals' of 'reports' runs saved query q1, which*"])
        assert fake_ws.jobs.created == []
        assert query_files(fake_ws) == {}

    def test_files_stay_with_a_kept_job(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        result = run_tests(
            pytester,
            'def test_job(lakeflow):\n    lakeflow.job("reports", saved_queries="inline").create()\n    assert False\n',
        )
        result.assert_outcomes(failed=1)
        assert len(query_files(fake_ws)) == 2
        assert fake_ws.workspace.deleted == []
        assert fake_ws.jobs.deleted == []

    def test_files_go_when_the_job_is_never_created(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(fake_ws.jobs, "create", deny_create)
        result = run_tests(
            pytester, 'def test_job(lakeflow):\n    lakeflow.job("reports", saved_queries="inline").create()\n'
        )
        result.assert_outcomes(failed=1)
        result.stdout.fnmatch_lines([f"*PermissionDenied: {DENIED}"])
        # No job points at the files, so keep_failed, which keeps a failed test's job, does not keep them.
        uploads = query_files(fake_ws)
        assert len(uploads) == 2
        assert sorted(fake_ws.workspace.deleted) == sorted(uploads)

    def test_a_failed_delete_only_warns(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        fake_ws.workspace.delete_errors["*/query_*_totals_*.sql"] = PermissionDenied("No permission to delete.")
        result = run_tests(
            pytester,
            'def test_job(lakeflow):\n    lakeflow.job("reports", saved_queries="inline").create()\n',
            "-o",
            "log_cli=true",
            "--log-cli-level=WARNING",
        )
        result.assert_outcomes(passed=1)
        result.stdout.fnmatch_lines(
            ["*Could not delete */query_*_totals_*.sql, which --lakeflow-sweep-notebooks deletes later: No permission*"]
        )
        [each] = [path for path in query_files(fake_ws) if "_each_" in path]
        assert fake_ws.workspace.deleted == [each]
        assert len(fake_ws.jobs.deleted) == 1

    def test_a_bad_value_raises_value_error(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        result = run_tests(
            pytester,
            """
            import pytest

            from pytest_databricks_lakeflow import Lakeflow

            def test_job(lakeflow):
                with pytest.raises(ValueError, match="saved_queries must be one of deployed, inline, got 'copy'"):
                    Lakeflow(saved_queries="copy")
                with pytest.raises(ValueError, match="saved_queries must be one of deployed, inline, got 'rerun'"):
                    lakeflow.job("reports", saved_queries="rerun")
            """,
        )
        result.assert_outcomes(passed=1)
        assert fake_ws.queries.get_calls == []
