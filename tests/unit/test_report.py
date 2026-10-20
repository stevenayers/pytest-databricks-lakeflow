"""What a test records about its runs: the report section, the JUnit properties and `--lakeflow-report-dir`."""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from databricks.sdk.service import jobs

from pytest_databricks_lakeflow.ephemeral import job
from pytest_databricks_lakeflow.report import directory_name

from .conftest import queue_until_cancelled, record_finished_runs, report_of, run_tests
from .fakes.runs import DATA, HOST, QUERY_FILE, SQL_FILE_OUTPUT, make_run
from .fakes.workspace import FakeWorkspaceClient


class TestDirectoryName:
    """Each test's report directory has a name that is safe on any file system and that no other test shares."""

    def test_directory_name_is_safe_and_unique(self) -> None:
        name = directory_name("tests/test_jobs.py::test_job[a/b]")
        assert re.fullmatch(r"tests_test_jobs\.py__test_job_a_b_-[0-9a-f]{8}", name)
        assert directory_name("tests/test_jobs.py::test_job[a/b]") == name
        assert directory_name("tests/test_jobs.py::test_job[a_b]") != name
        long_name = directory_name("test_jobs.py::test_job[" + "x" * 200 + "]")
        assert re.fullmatch(r"test_jobs\.py__test_job_x{57}-[0-9a-f]{8}", long_name)


@pytest.mark.usefixtures("fake_cli", "bundle_dir")
class TestReportSection:
    """The `lakeflow` report section and the junit properties behind it."""

    def test_report_section_and_junit_properties(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        run = make_run(77, {"extract": "SUCCESS"})
        run.job_parameters = [jobs.JobParameter(name="param1", default="hello", value="hi")]
        fake_ws.jobs.queue(run)
        result = run_tests(
            pytester,
            """
            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    job.run()
                assert False
            """,
            "--junitxml=report.xml",
        )
        result.stdout.fnmatch_lines(
            [
                "*- lakeflow -*",
                f"lakeflow_job_url: {HOST}/jobs/1001",
                f"lakeflow_run_url: {HOST}/jobs/runs/77",
            ]
        )
        result.stdout.no_fnmatch_line("*lakeflow_params*")
        result.stdout.no_fnmatch_line("*lakeflow_task_params*")
        xml = (pytester.path / "report.xml").read_text()
        assert f'<property name="lakeflow_job_url" value="{HOST}/jobs/1001"' in xml
        assert f'<property name="lakeflow_run_url" value="{HOST}/jobs/runs/77"' in xml
        assert '<property name="lakeflow_job_id" value="1001"' in xml
        assert '<property name="lakeflow_run_id" value="77"' in xml
        assert '<property name="lakeflow_result_state" value="SUCCESS"' in xml
        assert re.search(r'<property name="lakeflow_duration" value="\d+\.\d{3}"', xml)
        assert "lakeflow_failing" not in xml
        assert "lakeflow_params" not in xml

    def test_failing_run_adds_a_failing_property(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        error = jobs.RunOutput(error="ValueError: no rows\nmore detail")
        fake_ws.jobs.queue(make_run(77, {"extract": "FAILED"}, "FAILED", "RUN_EXECUTION_ERROR"), {7700: error})
        result = run_tests(
            pytester,
            """
            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    assert job.run().failed
            """,
            "--junitxml=report.xml",
        )
        result.assert_outcomes(passed=1)
        xml = (pytester.path / "report.xml").read_text()
        assert '<property name="lakeflow_run_id" value="77"' in xml
        assert '<property name="lakeflow_result_state" value="FAILED"' in xml
        assert '<property name="lakeflow_duration" value="5.000"' in xml
        assert '<property name="lakeflow_failing" value="extract: ValueError: no rows"' in xml

    def test_failed_run_reports_parameters(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        run = make_run(77, {"extract": "FAILED"}, "FAILED", "RUN_EXECUTION_ERROR")
        run.job_parameters = [jobs.JobParameter(name="param1", default="hello", value="hi")]
        fake_ws.jobs.resolved[7700] = jobs.ResolvedValues(
            notebook_task=jobs.ResolvedNotebookTaskValues(
                base_parameters={"param1": "hi", "limit": "10", "__pytest_task_key": "extract"}
            )
        )
        fake_ws.jobs.queue(run, {7700: jobs.RunOutput(error="ValueError: no rows")})
        result = run_tests(
            pytester,
            """
            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    job.run()
                assert False
            """,
            "--junitxml=report.xml",
        )
        result.stdout.fnmatch_lines(
            [
                "*- lakeflow -*",
                '*lakeflow_params: {"param1": "hi"}*',
                '*lakeflow_task_params: {"extract": {"limit": "10", "param1": "hi"}}*',
            ]
        )
        xml = (pytester.path / "report.xml").read_text()
        assert '<property name="lakeflow_params" value="{&quot;param1&quot;: &quot;hi&quot;}"' in xml
        expected = "{&quot;extract&quot;: {&quot;limit&quot;: &quot;10&quot;, &quot;param1&quot;: &quot;hi&quot;}}"
        assert f'<property name="lakeflow_task_params" value="{expected}"' in xml

    def test_report_section_includes_notebook_stdout(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        for _ in range(2):
            run = make_run(77, {"extract": "SUCCESS", "transform": "FAILED"}, "FAILED", notebooks={"transform"})
            fake_ws.jobs.queue(run, {7701: jobs.RunOutput(error="PermissionError: forbidden")})
        fake_ws.jobs.exports[7701] = (DATA / "notebook_export_failed.html").read_text()
        result = run_tests(
            pytester,
            """
            def test_expects_the_failure(lakeflow):
                with lakeflow.job("my_job") as job:
                    assert job.run().failed

            def test_fails(lakeflow):
                with lakeflow.job("my_job") as job:
                    job.run()
                assert False
            """,
            "-rA",
        )
        result.assert_outcomes(passed=1, failed=1)
        result.stdout.fnmatch_lines(
            [
                "*_ test_fails _*",
                "*- lakeflow -*",
                'transform: notebook.cells[2] failed: dbutils.jobs.taskValues.set(key="stage", value="validation")',
                "transform: notebook stdout:",
                "  transform: checking forbidden against /Volumes/example/raw/landing",
                "  transform: 7 rows expected",
            ]
        )
        assert result.stdout.str().count("transform: checking forbidden") == 1

    def test_section_is_attached_to_setup_failures(self, pytester: pytest.Pytester) -> None:
        result = run_tests(
            pytester,
            """
            import pytest

            @pytest.fixture(scope="module")
            def job(lakeflow):
                with lakeflow.job("my_job") as job:
                    raise RuntimeError("setup broke")
                    yield job

            def test_job(job):
                pass
            """,
        )
        result.assert_outcomes(errors=1)
        result.stdout.fnmatch_lines(["*ERROR at setup*", "*- lakeflow -*", f"lakeflow_job_url: {HOST}/jobs/1001"])

    def test_raise_for_failure_fails_the_test_with_the_run_detail(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        fake_ws.jobs.queue(make_run(77, {"extract": "FAILED"}, "FAILED", "RUN_EXECUTION_ERROR"))
        result = run_tests(
            pytester,
            """
            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    job.run().raise_for_failure()
            """,
        )
        result.assert_outcomes(failed=1)
        result.stdout.fnmatch_lines(
            ["*LakeflowRunFailed: JobRun('my_job', FAILED*", "*extract: FAILED*/jobs/runs/7700*"]
        )


# Tests whose run the plugin cancels, at the wait() deadline or when the job closes, with the get_run calls made
# before the cancel.
CANCELLED_BY_THE_PLUGIN = [
    pytest.param(
        """
        from datetime import timedelta

        import conftest
        import pytest

        from pytest_databricks_lakeflow import LakeflowTimeout

        def test_job(lakeflow):
            with lakeflow.job("my_job") as job:
                with pytest.raises(LakeflowTimeout, match="did not finish within 0:00:00"):
                    job.run(timeout=timedelta(0))
                assert job.runs == []
            assert conftest.finished == []
        """,
        2,
        id="timeout",
    ),
    pytest.param(
        """
        import conftest

        def test_job(lakeflow):
            with lakeflow.job("my_job") as job:
                job.start()
            assert job.runs == []
            assert conftest.finished == []
        """,
        1,
        id="closed-job",
    ),
]
REPORTED_JOB = """
def test_job(lakeflow):
    with lakeflow.job("my_job", params={"param1": "hi"}) as job:
        job.run()

def test_without_a_run():
    pass
"""


def block_the_report_dir(path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Put a file where `--lakeflow-report-dir=out` would make its directory under `path`."""
    (path / "out").write_text("a file, not a directory")


def break_serialise(path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Make serialising a run for the report raise an error that is not an `OSError`."""

    def broken(run: object) -> None:
        raise ValueError("cannot serialise")

    monkeypatch.setattr("pytest_databricks_lakeflow.report.serialise_with_files", broken)


@pytest.mark.usefixtures("fake_cli", "bundle_dir")
class TestReportDir:
    """`--lakeflow-report-dir` writes a directory per test with a run: report.json, raw logs and exported notebooks."""

    def test_report_dir_writes_one_json_per_test(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        fake_ws.jobs.queue(make_run(77, {"extract": "SUCCESS", "load": "SUCCESS"}))
        pytester.makeconftest(
            """
            import json

            from pytest_databricks_lakeflow.ephemeral.outcome import RUNS

            def pytest_sessionfinish(session):
                held = [item.nodeid for item in session.items if RUNS in item.stash]
                with open("held.json", "w") as f:
                    json.dump(held, f)
            """
        )
        result = run_tests(pytester, REPORTED_JOB, "--lakeflow-report-dir=out")
        result.assert_outcomes(passed=2)
        assert json.loads((pytester.path / "held.json").read_text()) == [], "runs are dropped once written"
        out = pytester.path / "out"
        test_dir, report = report_of(out)
        assert [path.name for path in out.iterdir()] == [test_dir.name]
        assert report["nodeid"] == "test_jobs.py::test_job"
        assert report["outcome"] == {"setup": "passed", "call": "passed", "teardown": "passed"}
        [entry] = report["runs"]
        assert (entry["run_id"], entry["result_state"], entry["succeeded"]) == (77, "SUCCESS", True)
        assert (entry["job_key"], entry["job_id"], entry["job_url"]) == ("my_job", 1001, f"{HOST}/jobs/1001")
        assert entry["params"] == {"param1": "hi"}
        [created] = fake_ws.jobs.created
        assert entry["settings"]["name"] == created["name"]
        assert [task["task_key"] for task in entry["settings"]["tasks"]] == [task.task_key for task in created["tasks"]]
        assert sorted(entry["tasks"]) == ["extract", "load"]
        assert report["debug_notebooks"] == []

    @pytest.mark.parametrize(
        ("logs", "written"),
        [("hello", "hello"), ("lone \ud800 surrogate", "lone ? surrogate")],
        ids=["text", "lone-surrogate"],
    )
    def test_report_dir_writes_raw_logs(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, logs: str, written: str
    ) -> None:
        run = make_run(77, {"extract": "SUCCESS", "load": "SUCCESS"})
        fake_ws.jobs.queue(run, outputs={7701: jobs.RunOutput(logs=logs)})
        run_tests(pytester, REPORTED_JOB, "--lakeflow-report-dir=out").assert_outcomes(passed=2)
        test_dir, report = report_of(pytester.path / "out")
        assert (test_dir / "77" / "load.log").read_text(encoding="utf-8") == written
        assert [path.name for path in (test_dir / "77").iterdir()] == ["load.log"]
        tasks = report["runs"][0]["tasks"]
        assert (tasks["load"]["logs_file"], tasks["extract"]["logs_file"]) == ("77/load.log", None)

    def test_report_dir_writes_exported_notebooks(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        run = make_run(77, {"extract": "SUCCESS", "transform": "FAILED"}, "FAILED", notebooks={"transform"})
        fake_ws.jobs.queue(run, {7701: jobs.RunOutput(error="PermissionError: forbidden")})
        html = (DATA / "notebook_export_failed.html").read_text()
        fake_ws.jobs.exports[7701] = html
        source = """
            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    job.run()
                assert False
            """
        run_tests(pytester, source, "--lakeflow-report-dir=out").assert_outcomes(failed=1)
        test_dir, report = report_of(pytester.path / "out")
        assert report["outcome"]["call"] == "failed"
        assert (test_dir / "77" / "transform.html").read_text() == html
        assert report["runs"][0]["tasks"]["transform"]["notebook_file"] == "77/transform.html"

    def test_report_dir_is_relative_to_the_invocation_directory(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        fake_ws.jobs.queue(make_run(77, {"extract": "SUCCESS"}))
        pytester.makepyfile(test_jobs=REPORTED_JOB)
        invoked_from = pytester.mkdir("sub")
        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.chdir(invoked_from)
            result = pytester.runpytest(str(pytester.path / "test_jobs.py"), "--lakeflow-report-dir=out")
        result.assert_outcomes(passed=2)
        report_of(invoked_from / "out")
        assert not (pytester.path / "out").exists()

    def test_no_report_dir_writes_nothing(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        fake_ws.jobs.queue(make_run(77, {"extract": "SUCCESS", "load": "SUCCESS"}), {7701: jobs.RunOutput(logs="x")})
        before = set(pytester.path.rglob("*"))
        run_tests(pytester, REPORTED_JOB).assert_outcomes(passed=2)
        written = set(pytester.path.rglob("*")) - before
        assert not [path for path in written if path.name in ("report.json", "load.log")]

    @pytest.mark.parametrize(
        ("break_report", "error"),
        [
            pytest.param(block_the_report_dir, "*", id="unwritable"),
            pytest.param(break_serialise, "*: cannot serialise", id="unserialisable"),
        ],
    )
    def test_report_errors_never_fail_the_test(
        self,
        pytester: pytest.Pytester,
        fake_ws: FakeWorkspaceClient,
        monkeypatch: pytest.MonkeyPatch,
        break_report: Callable[[Path, pytest.MonkeyPatch], None],
        error: str,
    ) -> None:
        fake_ws.jobs.queue(make_run(77, {"extract": "SUCCESS"}))
        break_report(pytester.path, monkeypatch)
        result = run_tests(
            pytester, REPORTED_JOB, "--lakeflow-report-dir=out", "-o", "log_cli=true", "--log-cli-level=WARNING"
        )
        result.assert_outcomes(passed=2)
        assert result.ret == pytest.ExitCode.OK
        result.stdout.fnmatch_lines([f"*Could not write the lakeflow report of test_jobs.py::test_job to *out{error}"])

    def test_report_dir_drops_the_reports_of_earlier_sessions(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        fake_ws.jobs.queue(make_run(77, {"extract": "SUCCESS"}))
        out = pytester.path / "out"
        for nodeid in ("test_jobs.py::test_job", "test_jobs.py::test_removed"):
            earlier = out / directory_name(nodeid)
            (earlier / "70").mkdir(parents=True)
            (earlier / "70" / "load.log").write_text("from an earlier session")
            (earlier / "report.json").write_text("{}")
        (out / "notes.txt").write_text("not a report")
        (out / "kept-0123abcd").mkdir()
        run_tests(pytester, REPORTED_JOB, "--lakeflow-report-dir=out").assert_outcomes(passed=2)
        test_dir, report = report_of(out)
        assert [entry["run_id"] for entry in report["runs"]] == [77]
        assert sorted(path.name for path in out.iterdir()) == sorted([test_dir.name, "notes.txt", "kept-0123abcd"])
        assert not (test_dir / "70").exists()

    def test_report_dir_serialises_sql_results(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        fake_ws.jobs.queue(make_run(77, {"query": "SUCCESS"}, sql_files={"query": QUERY_FILE}))
        fake_ws.jobs.raw_outputs[7700] = SQL_FILE_OUTPUT
        run_tests(pytester, REPORTED_JOB, "--lakeflow-report-dir=out").assert_outcomes(passed=2)
        _, report = report_of(pytester.path / "out")
        assert report["runs"][0]["tasks"]["query"]["sql_result"] == {
            "task_key": "query",
            "columns": ["n"],
            "types": ["int"],
            "rows": [{"n": "1"}],
            "truncated": False,
            "warehouse_id": "0123456789abcdef",
            "query_text": "SELECT 1 AS n\n",
        }

    def test_a_run_cancelled_at_fixture_teardown_is_reported_with_the_test_torn_down(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        queue_until_cancelled(fake_ws)
        source = """
            import pytest

            @pytest.fixture(scope="module")
            def started(lakeflow):
                with lakeflow.job("my_job") as job:
                    job.start()
                    yield job

            def test_first(started):
                pass

            def test_last(started):
                pass
            """
        run_tests(pytester, source, "--lakeflow-report-dir=out").assert_outcomes(passed=2)
        assert fake_ws.jobs.cancelled == [77]
        out = pytester.path / "out"
        test_dir, report = report_of(out, "test_jobs.py::test_last")
        assert [path.name for path in out.iterdir()] == [test_dir.name]
        [entry] = report["runs"]
        assert (entry["run_id"], entry["result_state"], entry["succeeded"]) == (77, "CANCELED", False)

    def test_an_interrupt_while_reading_a_cancelled_run_still_deletes_the_job(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        queue_until_cancelled(fake_ws)
        get_run = fake_ws.jobs.get_run

        def interrupted_once_cancelled(run_id: int, **kwargs: Any) -> jobs.Run:
            if fake_ws.jobs.cancelled:
                raise KeyboardInterrupt
            return get_run(run_id, **kwargs)

        monkeypatch.setattr(fake_ws.jobs, "get_run", interrupted_once_cancelled)
        pytester.makepyfile(
            test_jobs='def test_job(lakeflow):\n    with lakeflow.job("my_job") as job:\n        job.start()\n'
        )
        result = pytester.runpytest("--lakeflow-report-dir=out", no_reraise_ctrlc=True)
        assert result.ret == pytest.ExitCode.INTERRUPTED
        assert fake_ws.jobs.cancelled == [77]
        assert fake_ws.jobs.deleted == [1001]

    @pytest.mark.parametrize(("source", "polls"), CANCELLED_BY_THE_PLUGIN)
    def test_a_run_the_plugin_cancels_is_reported(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, source: str, polls: int
    ) -> None:
        queue_until_cancelled(fake_ws)
        record_finished_runs(pytester)
        run_tests(pytester, source, "--lakeflow-report-dir=out").assert_outcomes(passed=1)
        assert fake_ws.jobs.cancelled == [77]
        _, report = report_of(pytester.path / "out")
        [entry] = report["runs"]
        assert (entry["run_id"], entry["result_state"], entry["succeeded"]) == (77, "CANCELED", False)
        assert entry["tasks"]["t"]["result_state"] == "CANCELED"

    @pytest.mark.parametrize(("source", "polls"), CANCELLED_BY_THE_PLUGIN)
    def test_without_the_flag_a_cancelled_run_is_not_polled_again(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, source: str, polls: int
    ) -> None:
        queue_until_cancelled(fake_ws)
        record_finished_runs(pytester)
        run_tests(pytester, source).assert_outcomes(passed=1)
        assert fake_ws.jobs.cancelled == [77]
        assert fake_ws.jobs.get_run_calls == [{"run_id": 77}] * polls
        assert not (pytester.path / "out").exists()

    def test_a_cancelled_run_that_does_not_end_is_left_out(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake_ws.jobs.queue(make_run(77, {"t": "SUCCESS"}), polls=100)
        monkeypatch.setattr(job, "TIMEOUT_GRACE_SECONDS", 0)
        source = 'def test_job(lakeflow):\n    with lakeflow.job("my_job") as job:\n        job.start()\n'
        result = run_tests(
            pytester, source, "--lakeflow-report-dir=out", "-o", "log_cli=true", "--log-cli-level=WARNING"
        )
        result.assert_outcomes(passed=1)
        result.stdout.fnmatch_lines(
            [
                "*Run 77 of 'my_job' had not ended 0 seconds after it was cancelled, so report.json does not list "
                f"it: {HOST}/jobs/runs/77*"
            ]
        )
        assert fake_ws.jobs.cancelled == [77]
        assert not (pytester.path / "out").exists()
