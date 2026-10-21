"""Running an ephemeral job: `run()`, `start()` and `RunHandle`, the run cap, timeouts, progress and run options."""

from __future__ import annotations

import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest
from databricks.sdk.service import jobs

from pytest_databricks_lakeflow import EphemeralJob, Lakeflow, dbt, session
from pytest_databricks_lakeflow.ephemeral import outcome, polling

from .conftest import (
    NEEDS_REFRESH_SELECTION,
    FakeCli,
    configure_lakeflow,
    queue_until_cancelled,
    record_finished_runs,
    run_tests,
)
from .fakes.dbt import DBT_HEADERS, DBT_LINK, FakeDbtLinks, dbt_tarball
from .fakes.runs import HOST, make_run, running_run
from .fakes.templates import dbt_settings, pipeline_settings
from .fakes.workspace import FakeWorkspaceClient

PROGRESS_LINES = [
    "extract: PENDING",
    "transform: PENDING",
    "extract: PENDING -> SUCCESS in 0:00:02",
    "transform: PENDING -> RUNNING",
    "transform: RUNNING -> FAILED in 0:00:02 ValueError: no rows",
    "__pytest_probe__transform: SUCCESS in 0:00:02",
]


def queue_transform_failure(ws: FakeWorkspaceClient) -> None:
    """Queue a run whose extract goes PENDING then SUCCESS and whose transform goes PENDING, RUNNING, then FAILED."""
    final = make_run(
        77,
        {"extract": "SUCCESS", "transform": "FAILED", "__pytest_probe__transform": "SUCCESS"},
        "FAILED",
        "RUN_EXECUTION_ERROR",
    )
    polls = [
        running_run(77, {"extract": "PENDING", "transform": "PENDING"}),
        running_run(77, {"extract": "SUCCESS", "transform": "RUNNING"}),
        final,
    ]
    ws.jobs.queue_polls(polls, {7701: jobs.RunOutput(error="ValueError: no rows\nmore detail")})


@pytest.mark.usefixtures("fake_cli", "bundle_dir")
class TestRunHandle:
    """Starting a run without waiting, then waiting for it or cancelling it."""

    def test_start_and_wait_two_jobs(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        fake_ws.jobs.queue(make_run(77, {"t": "SUCCESS"}))
        fake_ws.jobs.queue(make_run(78, {"t": "SUCCESS"}))
        result = run_tests(
            pytester,
            """
            from pytest_databricks_lakeflow import RunHandle

            def test_job(lakeflow):
                with lakeflow.job("my_job") as first, lakeflow.job("my_job") as second:
                    handle = first.start(params={"param1": "hi"})
                    other = second.start()
                    assert isinstance(handle, RunHandle)
                    assert (handle.job, handle.run_id, other.job, other.run_id) == (first, 77, second, 78)
                    assert lakeflow.active_runs == {77, 78}
                    run = handle.wait()
                    run2 = other.wait()
                    assert run.succeeded and run2.succeeded
                    assert (first.runs, second.runs) == ([run], [run2])
                    assert lakeflow.active_runs == set()
            """,
        )
        result.assert_outcomes(passed=1)
        assert [call["job_parameters"] for call in fake_ws.jobs.run_now_calls] == [{"param1": "hi"}, None]
        assert fake_ws.jobs.cancelled == []

    def test_handle_cancel_removes_the_active_run(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        fake_ws.jobs.queue(make_run(77, {"t": "SUCCESS"}), polls=100)
        result = run_tests(
            pytester,
            f"""
            def test_job(lakeflow, request):
                with lakeflow.job("my_job") as job:
                    handle = job.start()
                    assert handle.url == "{HOST}/jobs/runs/77"
                    assert ("lakeflow_run_url", handle.url) in request.node.user_properties
                    handle.cancel()
                    assert lakeflow.ws.jobs.cancelled == [77]
                    assert lakeflow.active_runs == set()
                assert lakeflow.ws.jobs.cancelled == [77]
            """,
        )
        result.assert_outcomes(passed=1)

    @pytest.mark.parametrize(
        "source",
        [
            pytest.param(
                'def test_job(lakeflow):\n    with lakeflow.job("my_job") as job:\n        job.start()\n',
                id="with-exit",
            ),
            # An interrupted test does not reach its teardown, so the run is still active at session end.
            pytest.param(
                'def test_job(lakeflow):\n    lakeflow.job("my_job").start()\n    raise KeyboardInterrupt\n',
                id="session-end",
            ),
        ],
    )
    def test_unwaited_handle_is_cancelled(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, source: str
    ) -> None:
        fake_ws.jobs.queue(make_run(77, {"t": "SUCCESS"}), polls=100)
        pytester.makepyfile(test_jobs=source)
        pytester.runpytest(no_reraise_ctrlc=True)
        assert fake_ws.jobs.cancelled == [77]
        assert fake_ws.jobs.get_run_calls == [{"run_id": 77}]

    def test_wait_twice_returns_the_same_run_and_calls_run_finished_once(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        fake_ws.jobs.queue(make_run(77, {"t": "SUCCESS"}))
        record_finished_runs(pytester)
        result = run_tests(
            pytester,
            """
            import conftest

            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    handle = job.start()
                    assert handle.wait() is handle.wait()
                    assert len(job.runs) == 1
                assert handle.wait() is job.runs[0]
                assert conftest.finished == [77]
            """,
        )
        result.assert_outcomes(passed=1)

    @pytest.mark.parametrize(
        ("elapsed", "timeout", "slept"),
        [(1, "None", [5.0]), (10, "timedelta(0)", [])],
        ids=["fresh-snapshot", "stale-snapshot"],
    )
    def test_wait_polls_at_once_when_the_start_snapshot_is_older_than_the_poll_interval(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, elapsed: int, timeout: str, slept: list[float]
    ) -> None:
        fake_ws.jobs.queue(make_run(77, {"t": "SUCCESS"}))
        configure_lakeflow(pytester, "poll_interval=timedelta(seconds=5)")
        result = run_tests(
            pytester,
            f"""
            from datetime import timedelta

            import time

            def test_job(lakeflow, monkeypatch):
                clock = [0.0]
                slept = []

                def sleep(seconds):
                    slept.append(seconds)
                    clock[0] += seconds

                monkeypatch.setattr(time, "monotonic", lambda: clock[0])
                monkeypatch.setattr(time, "sleep", sleep)
                with lakeflow.job("my_job") as job:
                    handle = job.start()
                    clock[0] += {elapsed}
                    assert handle.wait(timeout={timeout}).succeeded
                assert slept == {slept}
            """,
        )
        result.assert_outcomes(passed=1)
        assert [call for call in fake_ws.jobs.get_run_calls if call["run_id"] == 77] == [{"run_id": 77}] * 2
        assert fake_ws.jobs.cancelled == []

    def test_wait_after_the_job_closed_fails_clearly(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        fake_ws.jobs.queue(make_run(77, {"t": "SUCCESS"}), polls=100)
        result = run_tests(
            pytester,
            """
            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    handle = job.start()
                with pytest.raises(LakeflowError, match="The ephemeral job for 'my_job' has already been closed"):
                    handle.wait()
            """,
        )
        result.assert_outcomes(passed=1)
        assert fake_ws.jobs.cancelled == [77]


@pytest.mark.usefixtures("fake_cli", "bundle_dir")
class TestRunCap:
    """How `--lakeflow-max-concurrent-runs` and `max_concurrent_runs` hold back the next run of a process."""

    def test_max_concurrent_runs_blocks_the_next_start(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        queue_until_cancelled(fake_ws)
        fake_ws.jobs.queue(make_run(78, {"t": "SUCCESS"}))
        configure_lakeflow(pytester, "poll_interval=timedelta(milliseconds=20)")
        result = run_tests(
            pytester,
            """
            import threading

            def test_job(lakeflow):
                with lakeflow.job("my_job") as first, lakeflow.job("my_job") as second:
                    handle = first.start()
                    started = []
                    thread = threading.Thread(target=lambda: started.append(second.start()))
                    thread.start()
                    thread.join(0.2)
                    assert thread.is_alive()
                    assert len(lakeflow.ws.jobs.run_now_calls) == 1
                    handle.cancel()
                    thread.join(5)
                    assert len(lakeflow.ws.jobs.run_now_calls) == 2
                    assert started[0].wait().succeeded
            """,
            "--lakeflow-max-concurrent-runs=1",
        )
        result.assert_outcomes(passed=1)

    def test_a_blocked_start_starts_once_a_run_ends(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        fake_ws.jobs.queue(make_run(77, {"t": "SUCCESS"}), polls=3)
        fake_ws.jobs.queue(make_run(78, {"t": "SUCCESS"}))
        result = run_tests(
            pytester,
            """
            import logging
            from datetime import timedelta

            def test_job(lakeflow, caplog):
                caplog.set_level(logging.INFO, logger="pytest_databricks_lakeflow")
                with lakeflow.job("my_job") as first, lakeflow.job("my_job") as second:
                    handles = [first.start(), second.start(timeout=timedelta(seconds=5))]
                    assert [handle.wait().succeeded for handle in handles] == [True, True]
                messages = [r.getMessage() for r in caplog.records]
                lines = [m for m in messages if m.startswith(("Started run", "Waiting"))]
                assert lines == [
                    "Started run 77 of job 1001",
                    "Waiting to start a run of job 1002 until one of the active runs [77] ends, under "
                    "max_concurrent_runs=1",
                    "Started run 78 of job 1002",
                ]
            """,
            "--lakeflow-max-concurrent-runs=1",
        )
        result.assert_outcomes(passed=1)
        assert fake_ws.jobs.cancelled == []

    @pytest.mark.parametrize(
        ("polls", "end"),
        [
            pytest.param(1, "first.start().wait()", id="wait"),
            pytest.param(100, "pytest.raises(LakeflowTimeout, first.run, timeout=timedelta(0))", id="timeout"),
            pytest.param(100, "first.start().cancel()", id="cancel"),
            pytest.param(100, "first.start()", id="close"),
        ],
    )
    def test_ending_a_run_releases_its_slot(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, polls: int, end: str
    ) -> None:
        fake_ws.jobs.queue(make_run(77, {"t": "SUCCESS"}), polls=polls)
        fake_ws.jobs.queue(make_run(78, {"t": "SUCCESS"}))
        result = run_tests(
            pytester,
            f"""
            import logging
            from datetime import timedelta

            import pytest

            from pytest_databricks_lakeflow import LakeflowTimeout

            def test_job(lakeflow, caplog):
                caplog.set_level(logging.INFO, logger="pytest_databricks_lakeflow")
                with lakeflow.job("my_job") as first:
                    {end}
                with lakeflow.job("my_job") as second:
                    second.start(timeout=timedelta(0))
                assert not [r for r in caplog.records if r.getMessage().startswith("Waiting")]
            """,
            "--lakeflow-max-concurrent-runs=1",
        )
        result.assert_outcomes(passed=1)
        assert len(fake_ws.jobs.run_now_calls) == 2

    def test_forget_twice_releases_once(self, fake_ws: FakeWorkspaceClient) -> None:
        slots = session.RunSlots(1)
        lakeflow = SimpleNamespace(ws=fake_ws, active_runs={77})
        job = EphemeralJob(cast("Lakeflow", lakeflow), "my_job", jobs.JobSettings())
        assert slots.semaphore.acquire(blocking=False)
        job._active[77] = cast("pytest.Item", SimpleNamespace())
        job._slots = slots
        slots.hold(77, job)
        job._forget(77)
        job._forget(77)
        assert slots.semaphore.acquire(blocking=False)
        assert not slots.semaphore.acquire(blocking=False)

    def test_first_capped_starts_at_once_share_one_cap(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        state = session.SessionState(rootpath=tmp_path, run_id="r")
        made: list[session.RunSlots] = []

        class SlowRunSlots(session.RunSlots):
            def __init__(self, cap: int) -> None:
                # Slow enough that every thread reaches run_slots while the first is still making the cap.
                time.sleep(0.05)
                super().__init__(cap)
                made.append(self)

        monkeypatch.setattr(session, "RunSlots", SlowRunSlots)
        barrier = threading.Barrier(4)

        def first_start(_: int) -> session.RunSlots | None:
            barrier.wait()
            return state.run_slots(2)

        with ThreadPoolExecutor(4) as pool:
            slots = list(pool.map(first_start, range(4)))
        assert len(made) == 1
        assert all(each is made[0] for each in slots)

    def test_a_start_that_finds_no_slot_in_time_names_the_active_runs(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        fake_ws.jobs.queue(make_run(77, {"t": "SUCCESS"}), polls=100)
        result = run_tests(
            pytester,
            """
            from datetime import timedelta

            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            def test_job(lakeflow):
                with lakeflow.job("my_job") as first, lakeflow.job("my_job") as second:
                    first.start()
                    with pytest.raises(LakeflowError) as e:
                        second.start(timeout=timedelta(0))
                assert str(e.value) == (
                    "No run slot freed within 0:00:00 to start a run of 'my_job': this process may have 1 run active "
                    "at once (--lakeflow-max-concurrent-runs or max_concurrent_runs), and runs [77] are still active. "
                    "Wait for or cancel one of them first, or pass a longer timeout."
                )
            """,
            "--lakeflow-max-concurrent-runs=1",
        )
        result.assert_outcomes(passed=1)
        assert len(fake_ws.jobs.run_now_calls) == 1

    def test_without_a_cap_nothing_blocks(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        fake_ws.jobs.queue(make_run(77, {"t": "SUCCESS"}), polls=100)
        fake_ws.jobs.queue(make_run(78, {"t": "SUCCESS"}), polls=100)
        result = run_tests(
            pytester,
            """
            from datetime import timedelta

            from pytest_databricks_lakeflow import session

            def test_job(lakeflow):
                with lakeflow.job("my_job") as first, lakeflow.job("my_job") as second:
                    first.start()
                    second.start(timeout=timedelta(0))
                    assert lakeflow.active_runs == {77, 78}
                assert session.current().slots is None
            """,
        )
        result.assert_outcomes(passed=1)

    def test_a_second_cap_in_the_process_keeps_the_first(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        fake_ws.jobs.queue(make_run(77, {"t": "SUCCESS"}), polls=100)
        extra = '\n\n@pytest.fixture(scope="session")\ndef wider():\n    return Lakeflow(max_concurrent_runs=3)\n'
        configure_lakeflow(pytester, "max_concurrent_runs=1", extra=extra)
        result = run_tests(
            pytester,
            """
            import logging
            from datetime import timedelta

            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            def test_job(lakeflow, wider, caplog):
                with lakeflow.job("my_job") as first, wider.job("my_job") as second:
                    first.start()
                    with pytest.raises(LakeflowError, match="may have 1 run active at once"):
                        second.start(timeout=timedelta(0))
                warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
                assert warnings == [
                    "max_concurrent_runs=3 differs from the cap of 1 that this process already uses, which every "
                    "Lakeflow in the process shares, so it is ignored"
                ]
            """,
        )
        result.assert_outcomes(passed=1)


@pytest.mark.usefixtures("fake_cli", "bundle_dir")
class TestRunOptions:
    """What `run()` sends to `run_now`."""

    def test_run_reads_resolved_values_from_each_task_run(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        run = make_run(77, {"extract": "SUCCESS", "transform": "SUCCESS"})
        fake_ws.jobs.queue(run)
        run_tests(pytester, 'def test_job(lakeflow):\n    lakeflow.job("my_job").run()\n').assert_outcomes(passed=1)
        with_flag = [call["run_id"] for call in fake_ws.jobs.get_run_calls if call.get("include_resolved_values")]
        assert sorted(with_flag) == [7700, 7701], "each task run is fetched once by its own id"
        polls = [call for call in fake_ws.jobs.get_run_calls if call["run_id"] == 77]
        assert polls and not any(call.get("include_resolved_values") for call in polls)

    def test_only_adds_probes(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        result = run_tests(
            pytester,
            """
            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    job.run(only=["+transform"])
                    job.run(only=[])
            """,
        )
        result.assert_outcomes(passed=1)
        assert [call["only"] for call in fake_ws.jobs.run_now_calls] == [["+transform"], None]

    @pytest.mark.parametrize("only", [["transform"], ["transform+"]])
    def test_only_with_an_unresolved_reference_fails_before_run_now(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, only: list[str]
    ) -> None:
        result = run_tests(
            pytester,
            f"""
            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            def test_job(lakeflow):
                job = lakeflow.job("my_job")
                with pytest.raises(LakeflowError, match=r"refers to task values of \\['extract'\\]"):
                    job.run(only={only!r})
                job.run(only=["+transform"])
            """,
        )
        result.assert_outcomes(passed=1)
        assert [call["only"] for call in fake_ws.jobs.run_now_calls] == [["+transform"]]

    def test_run_sends_a_fresh_idempotency_token(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        fake_ws.jobs.queue(make_run(77, {"t": "SUCCESS"}), polls=100)
        result = run_tests(
            pytester,
            """
            from datetime import timedelta

            import pytest

            from pytest_databricks_lakeflow import LakeflowTimeout

            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    with pytest.raises(LakeflowTimeout):
                        job.run(timeout=timedelta(0))
                    assert job.run().run_id != 77
                    assert job.run().run_id != 77
            """,
        )
        result.assert_outcomes(passed=1)
        tokens = [call["idempotency_token"] for call in fake_ws.jobs.run_now_calls]
        assert len(tokens) == 3
        assert all(tokens)
        assert len(set(tokens)) == 3
        assert all(len(token) <= 64 for token in tokens)

    @pytest.mark.parametrize(
        ("arguments", "target", "queue"),
        [
            pytest.param("", None, None, id="defaults"),
            pytest.param(
                'performance_target="PERFORMANCE_OPTIMIZED", queue=False',
                jobs.PerformanceTarget.PERFORMANCE_OPTIMIZED,
                jobs.QueueSettings(enabled=False),
                id="string",
            ),
            pytest.param(
                "performance_target=jobs.PerformanceTarget.STANDARD, queue=True",
                jobs.PerformanceTarget.STANDARD,
                jobs.QueueSettings(enabled=True),
                id="enum",
            ),
        ],
    )
    def test_run_forwards_performance_target_and_queue(
        self,
        pytester: pytest.Pytester,
        fake_ws: FakeWorkspaceClient,
        arguments: str,
        target: jobs.PerformanceTarget | None,
        queue: jobs.QueueSettings | None,
    ) -> None:
        result = run_tests(
            pytester,
            f"""
            from databricks.sdk.service import jobs

            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    job.run({arguments})
                    # The run's own target goes to run_now; the job keeps the session default.
                    assert job.settings.performance_target == jobs.PerformanceTarget.PERFORMANCE_OPTIMIZED
            """,
        )
        result.assert_outcomes(passed=1)
        [call] = fake_ws.jobs.run_now_calls
        assert call["performance_target"] == target
        assert call["queue"] == queue
        assert fake_ws.jobs.created[0]["performance_target"] == jobs.PerformanceTarget.PERFORMANCE_OPTIMIZED

    @pytest.mark.parametrize("method", ["run", "start"])
    def test_an_unknown_performance_target_is_rejected_before_the_job_is_created(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, method: str
    ) -> None:
        result = run_tests(
            pytester,
            f"""
            import pytest

            def test_job(lakeflow):
                job = lakeflow.job("my_job")
                with pytest.raises(ValueError, match="bogus"):
                    job.{method}(performance_target="bogus")
            """,
        )
        result.assert_outcomes(passed=1)
        assert (fake_ws.jobs.created, fake_ws.jobs.run_now_calls) == ([], [])

    def test_run_passes_full_refresh(
        self, pytester: pytest.Pytester, fake_cli: FakeCli, fake_ws: FakeWorkspaceClient
    ) -> None:
        fake_cli.deployed["pipeline_job"] = fake_ws.jobs.add(pipeline_settings())
        source = 'def test_job(lakeflow):\n    lakeflow.job("pipeline_job").run(full_refresh=True)\n'
        run_tests(pytester, source).assert_outcomes(passed=1)
        assert [c["pipeline_params"] for c in fake_ws.jobs.run_now_calls] == [jobs.PipelineParams(full_refresh=True)]

    @NEEDS_REFRESH_SELECTION
    def test_run_passes_pipeline_refresh_selections(
        self, pytester: pytest.Pytester, fake_cli: FakeCli, fake_ws: FakeWorkspaceClient
    ) -> None:
        fake_cli.deployed["pipeline_job"] = fake_ws.jobs.add(pipeline_settings())
        result = run_tests(
            pytester,
            """
            def test_job(lakeflow):
                with lakeflow.job("pipeline_job") as job:
                    job.run(refresh_selection=["bronze"])
                    job.run(full_refresh_selection=["silver"], only=["refresh"])
                    job.run(refresh_flow_selection=["bronze_flow"], reset_checkpoint_selection=["bronze"])
            """,
        )
        result.assert_outcomes(passed=1)
        assert [c["pipeline_params"] for c in fake_ws.jobs.run_now_calls] == [
            jobs.PipelineParams(refresh_selection=["bronze"]),
            jobs.PipelineParams(full_refresh_selection=["silver"]),
            jobs.PipelineParams(refresh_flow_selection=["bronze_flow"], reset_checkpoint_selection=["bronze"]),
        ]
        assert [c["only"] for c in fake_ws.jobs.run_now_calls] == [None, ["refresh"], None]
        assert [c["job_parameters"] for c in fake_ws.jobs.run_now_calls] == [None, None, None]

    def test_pipeline_refresh_options_need_a_pipeline_task(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        result = run_tests(
            pytester,
            """
            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    with pytest.raises(LakeflowError, match="no pipeline task.*'extract', 'load'"):
                        job.run(full_refresh=True)
            """,
        )
        result.assert_outcomes(passed=1)
        assert fake_ws.jobs.run_now_calls == []

    @pytest.mark.parametrize("method", ["run", "start"])
    def test_dbt_commands_are_forwarded(
        self, pytester: pytest.Pytester, fake_cli: FakeCli, fake_ws: FakeWorkspaceClient, method: str
    ) -> None:
        fake_cli.deployed["analytics"] = fake_ws.jobs.add(dbt_settings())
        for run_id in (81, 82):
            fake_ws.jobs.queue(make_run(run_id, {"dbt_build": "SUCCESS"}))
            deployed = jobs.ResolvedDbtTaskValues(commands=["dbt deps", "dbt build"])
            fake_ws.jobs.resolved[run_id * 100] = jobs.ResolvedValues(dbt_task=deployed)
        wait = ".wait()" if method == "start" else ""
        result = run_tests(
            pytester,
            f"""
            def test_job(lakeflow):
                with lakeflow.job("analytics") as job:
                    run = job.{method}(dbt_commands=("dbt build --select tag:ci",)){wait}
                    assert run.task("dbt_build").parameters == ["dbt build --select tag:ci"]
                    run = job.{method}(){wait}
                    assert run.task("dbt_build").parameters == ["dbt deps", "dbt build"]
            """,
        )
        result.assert_outcomes(passed=1)
        commands = [call["dbt_commands"] for call in fake_ws.jobs.run_now_calls]
        assert commands == [["dbt build --select tag:ci"], None]
        # The job's own commands stay as deployed: dbt_commands applies to one run.
        [dbt_build, _] = fake_ws.jobs.created[0]["tasks"]
        assert dbt_build.dbt_task.commands == ["dbt deps", "dbt build"]

    @pytest.mark.parametrize(
        ("commands", "message"),
        [
            pytest.param("[]", "dbt_commands must not be empty*", id="empty"),
            pytest.param('"dbt build"', "dbt_commands must be a list of commands, not a string: 'dbt build'", id="str"),
        ],
    )
    def test_bad_dbt_commands_are_refused_before_the_job_is_created(
        self, pytester: pytest.Pytester, fake_cli: FakeCli, fake_ws: FakeWorkspaceClient, commands: str, message: str
    ) -> None:
        fake_cli.deployed["analytics"] = fake_ws.jobs.add(dbt_settings())
        result = run_tests(
            pytester,
            f"""
            def test_job(lakeflow):
                lakeflow.job("analytics").run(dbt_commands={commands})
            """,
        )
        result.assert_outcomes(failed=1)
        result.stdout.fnmatch_lines([f"E*ValueError: {message}"])
        assert (fake_ws.jobs.created, fake_ws.jobs.run_now_calls) == ([], [])

    def test_dbt_commands_are_refused_on_a_job_with_job_parameters(
        self, pytester: pytest.Pytester, fake_cli: FakeCli, fake_ws: FakeWorkspaceClient
    ) -> None:
        # Observed on the dbt bundle with test_marker=True: run_now refuses dbt_commands with InvalidParameterValue
        # "Cannot use legacy parameters (notebook_params, jar_params, python_params, spark_submit_params, sql_params)
        # because the job has job parameters configured."
        settings = dbt_settings()
        settings.parameters = [jobs.JobParameterDefinition(name="env", default="test")]
        fake_cli.deployed["analytics"] = fake_ws.jobs.add(settings)
        result = run_tests(
            pytester,
            """
            def test_job(lakeflow):
                lakeflow.job("analytics").run(dbt_commands=["dbt build"])
            """,
        )
        result.assert_outcomes(failed=1)
        assert (
            "LakeflowError: dbt_commands cannot be passed to a run of 'analytics', because the job has job "
            "parameters ['env'], and the Jobs API then refuses them. Pass task_params={'<dbt task key>': [...]} "
            "to job() or task() instead, which sets the commands of one dbt task in the copy."
        ) in result.stdout.str()
        assert fake_ws.jobs.run_now_calls == []


@pytest.mark.usefixtures("fake_cli", "bundle_dir")
class TestDbtArtifacts:
    """Lakeflow(dbt_artifacts=True) downloads the artefacts of each dbt task and removes them after the session."""

    @pytest.mark.parametrize(
        ("arguments", "flags", "downloaded", "kept"),
        [
            pytest.param("", (), False, False, id="default"),
            pytest.param("dbt_artifacts=True", (), True, False, id="removed"),
            pytest.param("dbt_artifacts=True", ("--lakeflow-keep", "--log-cli-level=WARNING"), True, True, id="kept"),
        ],
    )
    def test_artifacts_of_a_dbt_task(
        self,
        pytester: pytest.Pytester,
        monkeypatch: pytest.MonkeyPatch,
        fake_cli: FakeCli,
        fake_ws: FakeWorkspaceClient,
        arguments: str,
        flags: tuple[str, ...],
        downloaded: bool,
        kept: bool,
    ) -> None:
        links = FakeDbtLinks()
        monkeypatch.setattr(dbt, "_open", links)
        links.bodies[DBT_LINK] = dbt_tarball({"model.proj.orders": "success", "model.proj.bad": "error"})
        fake_cli.deployed["analytics"] = fake_ws.jobs.add(dbt_settings())
        output = jobs.RunOutput(dbt_output=jobs.DbtOutput(artifacts_link=DBT_LINK, artifacts_headers=DBT_HEADERS))
        fake_ws.jobs.queue(make_run(81, {"dbt_build": "FAILED", "check": "UPSTREAM_FAILED"}, "FAILED"), {8100: output})
        configure_lakeflow(pytester, arguments)
        result = run_tests(
            pytester,
            f"""
            from pathlib import Path

            def test_job(lakeflow):
                with lakeflow.job("analytics") as job:
                    run = job.run()
                assert run.task("check").dbt is None
                dbt = run.task("dbt_build").dbt
                if not {downloaded}:
                    assert dbt is None
                    return
                assert dbt.failed_models == ["model.proj.bad"]
                assert not dbt.all_passed
                assert (dbt.artifacts_dir / "target" / "manifest.json").is_file()
                Path("artifacts_dir.txt").write_text(str(dbt.artifacts_dir))
            """,
            *flags,
        )
        result.assert_outcomes(passed=1)
        assert len(links.requests) == int(downloaded)
        if not downloaded:
            return
        artifacts_dir = Path((pytester.path / "artifacts_dir.txt").read_text())
        assert artifacts_dir.exists() is kept
        if kept:
            result.stdout.fnmatch_lines([f"*Keeping the dbt artefacts of run 81 in {artifacts_dir.parent.parent}*"])
            shutil.rmtree(artifacts_dir.parent.parent)


@pytest.mark.usefixtures("fake_cli", "bundle_dir")
class TestPerformanceOverride:
    """The warning when a run asked for `PERFORMANCE_OPTIMIZED` and used `STANDARD`."""

    def test_overridden_performance_target_warns_once(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        fake_ws.jobs.queue(make_run(7, {"task": "SUCCESS"}, effective_performance_target="STANDARD"))
        fake_ws.jobs.queue(make_run(8, {"task": "SUCCESS"}, effective_performance_target="STANDARD"))
        result = run_tests(
            pytester,
            """
            def test_job(lakeflow):
                run = lakeflow.job("my_job").run()
                assert run.succeeded
                assert run.effective_performance_target == "STANDARD"
                lakeflow.job("my_job").run()
            """,
        )
        result.assert_outcomes(passed=1, warnings=1)
        result.stdout.fnmatch_lines(
            [
                "*Run 7 of 'my_job' asked for PERFORMANCE_OPTIMIZED but used STANDARD. A workspace policy or the "
                "job's compute decides the performance mode; expect 3 to 4 minutes extra per run.*"
            ]
        )

    @pytest.mark.parametrize(
        ("target", "warnings"),
        [
            pytest.param("PERFORMANCE_OPTIMIZED", 1, id="optimized"),
            pytest.param("STANDARD", 0, id="standard"),
        ],
    )
    def test_per_run_performance_target_decides(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, target: str, warnings: int
    ) -> None:
        fake_ws.jobs.queue(make_run(7, {"task": "SUCCESS"}, effective_performance_target="STANDARD"))
        result = run_tests(
            pytester, f'def test_job(lakeflow):\n    lakeflow.job("my_job").run(performance_target="{target}")\n'
        )
        result.assert_outcomes(passed=1, warnings=warnings)

    def test_no_warning_when_nothing_was_requested(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        fake_ws.jobs.queue(make_run(7, {"task": "SUCCESS"}, effective_performance_target="STANDARD"))
        configure_lakeflow(pytester, "performance_target=None")
        result = run_tests(pytester, 'def test_job(lakeflow):\n    lakeflow.job("my_job").run()\n')
        result.assert_outcomes(passed=1, warnings=0)

    def test_template_target_counts_as_a_request(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli
    ) -> None:
        template = fake_ws.jobs.settings[fake_cli.deployed["my_job"]]
        template.performance_target = jobs.PerformanceTarget.PERFORMANCE_OPTIMIZED
        fake_ws.jobs.queue(make_run(7, {"task": "SUCCESS"}, effective_performance_target="STANDARD"))
        configure_lakeflow(pytester, "performance_target=None")
        result = run_tests(pytester, 'def test_job(lakeflow):\n    lakeflow.job("my_job").run()\n')
        result.assert_outcomes(passed=1, warnings=1)

    def test_no_warning_without_an_effective_target(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        fake_ws.jobs.queue(make_run(7, {"task": "SUCCESS"}))
        result = run_tests(pytester, 'def test_job(lakeflow):\n    lakeflow.job("my_job").run()\n')
        result.assert_outcomes(passed=1, warnings=0)


@pytest.mark.usefixtures("fake_cli", "bundle_dir")
class TestTimeouts:
    """The poll interval, the run timeout, the job's `timeout_seconds` and cancelling on expiry."""

    @pytest.mark.parametrize(
        ("poll_interval", "sleeps"), [(timedelta(seconds=5), [5.0]), (None, [3.0])], ids=["set", "default"]
    )
    def test_poll_interval(
        self,
        fake_ws: FakeWorkspaceClient,
        monkeypatch: pytest.MonkeyPatch,
        poll_interval: timedelta | None,
        sleeps: list[float],
    ) -> None:
        slept: list[float] = []
        monkeypatch.setattr(polling, "POLL_SECONDS", 3.0)
        monkeypatch.setattr(time, "sleep", slept.append)
        fake_ws.jobs.queue(make_run(77, {"t": "SUCCESS"}))
        fake_ws.jobs.run_now(1001)
        lakeflow = SimpleNamespace(
            ws=fake_ws, poll_interval=poll_interval, active_runs=set(), timeout=timedelta(minutes=1), run_finished=None
        )
        job = EphemeralJob(cast("Lakeflow", lakeflow), "my_job", jobs.JobSettings())
        item = SimpleNamespace(stash=pytest.Stash(), user_properties=[])
        run, _ = job._wait(77, timedelta(minutes=1), cast("pytest.Item", item))
        assert run.state is not None and run.state.result_state == jobs.RunResultState.SUCCESS
        assert slept == sleeps

    def test_timeout_message_lists_task_states(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        running = running_run(77, {"extract": "SUCCESS", "transform": "RUNNING", "load": "PENDING"})
        fake_ws.jobs.queue(make_run(77, {"t": "SUCCESS"}), polls=100, running=running)
        result = run_tests(
            pytester,
            """
            from datetime import timedelta

            import pytest

            from pytest_databricks_lakeflow import LakeflowTimeout

            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    with pytest.raises(LakeflowTimeout) as e:
                        job.run(timeout=timedelta(0))
                assert str(e.value).endswith("Tasks when cancelled: extract SUCCESS, transform RUNNING, load PENDING")
            """,
        )
        result.assert_outcomes(passed=1)
        assert fake_ws.jobs.cancelled == [77]

    @pytest.mark.parametrize(
        ("arguments", "call"),
        [
            pytest.param(None, "job.run(timeout=timedelta(0))", id="run"),
            pytest.param("timeout=timedelta(0)", "job.run()", id="lakeflow"),
        ],
    )
    def test_timeout_cancels_the_run(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, arguments: str | None, call: str
    ) -> None:
        fake_ws.jobs.queue(make_run(77, {"t": "SUCCESS"}), polls=100)
        if arguments is not None:
            configure_lakeflow(pytester, arguments)
        result = run_tests(
            pytester,
            f"""
            from datetime import timedelta

            import pytest

            from pytest_databricks_lakeflow import LakeflowTimeout

            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    with pytest.raises(LakeflowTimeout, match="Run 77 of 'my_job' did not finish within 0:00:00"):
                        {call}
                assert lakeflow.ws.jobs.cancelled == [77]
                assert lakeflow.active_runs == set()
            """,
        )
        result.assert_outcomes(passed=1)

    @pytest.mark.parametrize(
        ("call", "tasks"),
        [
            pytest.param('lakeflow.job("my_job")', 4, id="job"),
            pytest.param('lakeflow.task("my_job.load")', 1, id="task"),
        ],
    )
    def test_longer_run_timeout_raises_the_job_timeout(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, call: str, tasks: int
    ) -> None:
        configure_lakeflow(pytester, "timeout=timedelta(minutes=1)")
        result = run_tests(
            pytester,
            f"""
            import logging
            from datetime import timedelta

            def test_job(lakeflow, caplog):
                caplog.set_level(logging.INFO, logger="pytest_databricks_lakeflow")
                with {call} as job:
                    job.run(timeout=timedelta(hours=1))
                    job.run(timeout=timedelta(hours=1))
                    settings = lakeflow.ws.jobs.get(job.job_id).settings
                assert settings.timeout_seconds == 3660
                assert [t.timeout_seconds for t in settings.tasks] == [None] * {tasks}
                [record] = [r for r in caplog.records if "timeout_seconds" in r.getMessage()]
                assert record.levelname == "INFO"
                assert record.getMessage() == (
                    "Raised timeout_seconds of job 1001 from 120 to 3660 for a run timeout of 1:00:00"
                )
            """,
        )
        result.assert_outcomes(passed=1)
        created = fake_ws.jobs.created[0]
        assert created["timeout_seconds"] == 120
        assert fake_ws.jobs.updated == [(1001, jobs.JobSettings(timeout_seconds=3660, tasks=created["tasks"]))]
        assert fake_ws.jobs.run_timeouts == [3660, 3660]

    @pytest.mark.parametrize(
        ("source", "limit", "first", "second"),
        [
            pytest.param("template", 600, ("timedelta(minutes=20)", "0:20:00", 1260), 1560, id="shorter-template"),
            pytest.param("modify_settings", 7200, ("timedelta(hours=3)", "3:00:00", 10860), 11160, id="longer-hook"),
        ],
    )
    def test_a_timeout_the_plugin_did_not_set_is_raised_with_a_warning(
        self,
        pytester: pytest.Pytester,
        fake_ws: FakeWorkspaceClient,
        source: str,
        limit: int,
        first: tuple[str, str, int],
        second: int,
    ) -> None:
        if source == "template":
            fake_ws.jobs.settings[1000].timeout_seconds = limit
        else:
            extra = f"\n\ndef lengthen(settings, item):\n    settings.timeout_seconds = {limit}\n"
            configure_lakeflow(pytester, "modify_settings=lengthen", extra=extra)
        timeout, shown, raised = first
        result = run_tests(
            pytester,
            f"""
            import logging
            from datetime import timedelta

            def test_job(lakeflow, caplog):
                with lakeflow.job("my_job") as job:
                    job.run(timeout={timeout})
                    job.run(timeout={timeout} + timedelta(minutes=5))
                warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
                assert warnings == [
                    "Raised timeout_seconds of job 1001 from {limit}, which the template or modify_settings set, to "
                    "{raised} for a run timeout of {shown}"
                ]
            """,
        )
        result.assert_outcomes(passed=1)
        assert fake_ws.jobs.created[0]["timeout_seconds"] == limit
        assert [settings.timeout_seconds for _, settings in fake_ws.jobs.updated if settings] == [raised, second]
        assert fake_ws.jobs.run_timeouts == [raised, second]

    def test_run_timeout_within_the_job_timeout_makes_no_update(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        configure_lakeflow(pytester, "timeout=timedelta(minutes=1)")
        result = run_tests(
            pytester,
            """
            from datetime import timedelta

            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    assert job.run(timeout=timedelta(seconds=10)).succeeded
                    assert job.run().succeeded
            """,
        )
        result.assert_outcomes(passed=1)
        assert fake_ws.jobs.created[0]["timeout_seconds"] == 120
        assert fake_ws.jobs.updated == []
        assert fake_ws.jobs.run_timeouts == [120, 120]

    def test_wait_timeout_longer_than_the_job_fails_clearly(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        configure_lakeflow(pytester, "timeout=timedelta(minutes=1)")
        result = run_tests(
            pytester,
            """
            from datetime import timedelta

            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    handle = job.start()
                    with pytest.raises(LakeflowError, match=r"wait\\(timeout=1:00:00\\) of 'my_job'") as e:
                        handle.wait(timeout=timedelta(hours=1))
                assert "timeout_seconds of 120 (0:02:00)" in str(e.value)
                assert "run(timeout=...)" in str(e.value)
                assert "start(timeout=...)" in str(e.value)
            """,
        )
        result.assert_outcomes(passed=1)
        assert fake_ws.jobs.updated == []

    def test_start_timeout_raises_the_job_timeout_before_the_run(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        # Lakeflow's timeout of zero would time the wait out, so a wait() that returns used start()'s timeout.
        configure_lakeflow(pytester, "timeout=timedelta(0)")
        fake_ws.jobs.queue(make_run(77, {"t": "SUCCESS"}), polls=3)
        result = run_tests(
            pytester,
            """
            from datetime import timedelta

            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    handle = job.start(timeout=timedelta(hours=1))
                    with pytest.raises(LakeflowError, match=r"wait\\(timeout=2:00:00\\) of 'my_job'"):
                        handle.wait(timeout=timedelta(hours=2))
                    assert handle.wait().succeeded
            """,
        )
        result.assert_outcomes(passed=1)
        created = fake_ws.jobs.created[0]
        assert created["timeout_seconds"] == 60
        assert fake_ws.jobs.updated == [(1001, jobs.JobSettings(timeout_seconds=3660, tasks=created["tasks"]))]
        assert fake_ws.jobs.run_timeouts == [3660]
        assert fake_ws.jobs.cancelled == []


@pytest.mark.usefixtures("fake_cli", "bundle_dir")
class TestStopAfter:
    """`stop_after` cancels a run that does not end on its own and returns it as a cancelled `JobRun`."""

    @pytest.mark.parametrize(
        "call",
        [
            pytest.param("job.run(stop_after=timedelta(0))", id="run"),
            pytest.param("job.start(timeout=timedelta(minutes=5)).wait(stop_after=timedelta(0))", id="wait"),
        ],
    )
    def test_stop_after_returns_a_cancelled_run(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, call: str
    ) -> None:
        queue_until_cancelled(fake_ws)
        record_finished_runs(pytester)
        result = run_tests(
            pytester,
            f"""
            from datetime import timedelta

            import conftest

            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    run = {call}
                    assert (run.cancelled, run.result_state, run.failed) == (True, "CANCELED", True)
                    task = run.task("t")
                    assert (task.ran, task.error) == (False, None)
                    assert job.runs == [run]
                    assert lakeflow.active_runs == set()
                assert conftest.finished == [77]
            """,
        )
        result.assert_outcomes(passed=1)
        assert fake_ws.jobs.cancelled == [77]

    @pytest.mark.parametrize(
        ("call", "active", "started"),
        [
            pytest.param(
                "job.run(stop_after=timedelta(minutes=5), timeout=timedelta(minutes=1))", "set()", 0, id="run"
            ),
            # wait() learns stop_after only after the run started, so the run keeps going until the job closes.
            pytest.param(
                "job.start(timeout=timedelta(minutes=1)).wait(stop_after=timedelta(minutes=5))", "{77}", 1, id="wait"
            ),
        ],
    )
    def test_stop_after_longer_than_timeout_fails_early(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, call: str, active: str, started: int
    ) -> None:
        fake_ws.jobs.queue(make_run(77, {"t": "SUCCESS"}), polls=100)
        result = run_tests(
            pytester,
            f"""
            from datetime import timedelta

            import pytest

            def test_job(lakeflow):
                message = "stop_after=0:05:00 of 'my_job' is longer than its timeout of 0:01:00"
                with lakeflow.job("my_job") as job:
                    with pytest.raises(ValueError, match=message):
                        {call}
                    assert lakeflow.active_runs == {active}
                    assert lakeflow.ws.jobs.cancelled == []
            """,
        )
        result.assert_outcomes(passed=1)
        assert len(fake_ws.jobs.run_now_calls) == started

    def test_stop_after_then_timeout_still_raises(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        fake_ws.jobs.queue(make_run(77, {"t": "SUCCESS"}), polls=100)
        result = run_tests(
            pytester,
            """
            from datetime import timedelta

            import pytest

            from pytest_databricks_lakeflow import LakeflowTimeout

            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    with pytest.raises(LakeflowTimeout, match="Run 77 of 'my_job' did not finish within 0:00:00"):
                        job.run(stop_after=timedelta(0), timeout=timedelta(0))
                    assert job.runs == []
                    assert lakeflow.active_runs == set()
            """,
        )
        result.assert_outcomes(passed=1)
        assert fake_ws.jobs.cancelled == [77]


@pytest.mark.usefixtures("fake_cli", "bundle_dir")
class TestProgress:
    """The task transition lines logged while polling and shown in the report section."""

    def test_progress_is_logged_per_task_transition(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        queue_transform_failure(fake_ws)
        source = 'def test_job(lakeflow):\n    assert lakeflow.job("my_job").run().failed\n'
        result = run_tests(pytester, source, "--log-cli-level=INFO")
        result.assert_outcomes(passed=1)
        result.stdout.fnmatch_lines([f"*INFO*pytest_databricks_lakeflow*{line}" for line in PROGRESS_LINES])
        for line in PROGRESS_LINES:
            assert sum(out.endswith(f" {line}") for out in result.stdout.lines) == 1, line
        assert sorted(fake_ws.jobs.output_calls) == [7700, 7701, 7702]

    def test_task_terminated_at_the_first_poll_logs_one_line(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        fake_ws.jobs.queue_polls([make_run(77, {"extract": "SUCCESS"})])
        source = 'def test_job(lakeflow):\n    lakeflow.job("my_job").run()\n'
        result = run_tests(pytester, source, "--log-cli-level=INFO")
        result.assert_outcomes(passed=1)
        progress = [line for line in result.stdout.lines if " extract: " in line]
        assert len(progress) == 1
        assert progress[0].endswith(" extract: SUCCESS in 0:00:02")
        assert fake_ws.jobs.output_calls == [7700]

    def test_late_result_state_does_not_log_a_second_line(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        details = jobs.TerminationDetails(code=jobs.TerminationCodeCode.RUN_EXECUTION_ERROR)
        status = jobs.RunStatus(state=jobs.RunLifecycleStateV2State.TERMINATED, termination_details=details)
        task = jobs.RunTask(task_key="transform", run_id=7700, status=status, start_time=1_000, end_time=3_000)
        running = jobs.Run(run_id=77, status=jobs.RunStatus(state=jobs.RunLifecycleStateV2State.RUNNING), tasks=[task])
        fake_ws.jobs.queue_polls([running, make_run(77, {"transform": "FAILED"}, "FAILED", "RUN_EXECUTION_ERROR")])
        source = 'def test_job(lakeflow):\n    assert lakeflow.job("my_job").run().failed\n'
        result = run_tests(pytester, source, "--log-cli-level=INFO")
        result.assert_outcomes(passed=1)
        progress = [line for line in result.stdout.lines if " transform: " in line and "INFO" in line]
        assert len(progress) == 1
        assert progress[0].endswith(" transform: RUN_EXECUTION_ERROR in 0:00:02")
        assert fake_ws.jobs.output_calls == [7700]

    def test_progress_falls_back_to_the_legacy_state(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        legacy = jobs.RunState(life_cycle_state=jobs.RunLifeCycleState.RUNNING)
        tasks = [jobs.RunTask(task_key="extract", run_id=7700, state=legacy), jobs.RunTask(task_key="bare")]
        running = jobs.Run(run_id=77, state=jobs.RunState(life_cycle_state=jobs.RunLifeCycleState.RUNNING), tasks=tasks)
        fake_ws.jobs.queue_polls([running, make_run(77, {"extract": "SUCCESS"})])
        source = 'def test_job(lakeflow):\n    lakeflow.job("my_job").run()\n'
        result = run_tests(pytester, source, "--log-cli-level=INFO")
        result.assert_outcomes(passed=1)
        result.stdout.fnmatch_lines(["*INFO* extract: RUNNING", "*INFO* extract: RUNNING -> SUCCESS in 0:00:02"])
        assert not [line for line in result.stdout.lines if " bare: " in line]
        assert fake_ws.jobs.output_calls == [7700]

    @pytest.mark.parametrize(
        ("source", "flags", "outcome", "heading"),
        [
            pytest.param(
                'def test_job(lakeflow):\n    lakeflow.job("my_job").run()\n    assert False\n',
                [],
                {"failed": 1},
                "*FAILURES*",
                id="failed",
            ),
            pytest.param(
                'def test_job(lakeflow):\n    assert lakeflow.job("my_job").run().failed\n',
                ["-rA"],
                {"passed": 1},
                "*PASSES*",
                id="passed-with-rA",
            ),
        ],
    )
    def test_progress_lines_in_report_section(
        self,
        pytester: pytest.Pytester,
        fake_ws: FakeWorkspaceClient,
        source: str,
        flags: list[str],
        outcome: dict[str, int],
        heading: str,
    ) -> None:
        queue_transform_failure(fake_ws)
        result = run_tests(pytester, source, *flags)
        result.assert_outcomes(**outcome)
        result.stdout.fnmatch_lines(
            [
                heading,
                "*- lakeflow -*",
                f"lakeflow_job_url: {HOST}/jobs/1001",
                f"lakeflow_run_url: {HOST}/jobs/runs/77",
                "lakeflow_failing: transform: ValueError: no rows",
                *PROGRESS_LINES,
            ]
        )

    def test_timeout_keeps_progress_in_the_report(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        running = running_run(77, {"extract": "SUCCESS", "transform": "RUNNING"})
        fake_ws.jobs.queue_polls([running, running, make_run(77, {"extract": "SUCCESS"})])
        source = """
            from datetime import timedelta

            def test_job(lakeflow):
                with lakeflow.job("my_job") as job:
                    job.run(timeout=timedelta(0))
        """
        result = run_tests(pytester, source)
        result.assert_outcomes(failed=1)
        assert fake_ws.jobs.cancelled == [77]
        result.stdout.fnmatch_lines(
            [
                "*LakeflowTimeout: Run 77 of 'my_job' did not finish within 0:00:00*",
                "*- lakeflow -*",
                f"lakeflow_run_url: {HOST}/jobs/runs/77",
                "extract: SUCCESS in 0:00:02",
                "transform: RUNNING",
            ]
        )
        assert fake_ws.jobs.output_calls == [7700]


@pytest.mark.usefixtures("fake_cli", "bundle_dir")
class TestLive:
    """What --lakeflow-live and run(stream=...) print to the terminal while a run waits."""

    def test_live_flag_prints_transitions(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        queue_transform_failure(fake_ws)
        source = 'def test_job(lakeflow):\n    assert lakeflow.job("my_job").run().failed\n'
        result = run_tests(pytester, source, "--lakeflow-live")
        result.assert_outcomes(passed=1)
        result.stdout.fnmatch_lines(
            [
                f"lakeflow: Started run 77 of job 1001: {HOST}/jobs/runs/77",
                "lakeflow: transform: PENDING",
                "lakeflow: transform: PENDING -> RUNNING",
                "lakeflow: transform: RUNNING -> FAILED in 0:00:02 ValueError: no rows",
                f"lakeflow: Run 77 finished FAILED in *: {HOST}/jobs/runs/77",
            ]
        )
        assert result.stdout.str().count("PENDING -> RUNNING") == 1
        assert result.stdout.str().count("RUNNING -> FAILED") == 1

    def test_start_line_leaves_out_a_url_not_reported_yet(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        first = replace(running_run(77, {"extract": "PENDING"}), run_page_url=None)
        fake_ws.jobs.queue_polls([first, make_run(77, {"extract": "SUCCESS"})])
        source = 'def test_job(lakeflow):\n    assert lakeflow.job("my_job").run(stream=True).succeeded\n'
        result = run_tests(pytester, source)
        result.assert_outcomes(passed=1)
        assert "lakeflow: Started run 77 of job 1001" in result.stdout.lines
        result.stdout.fnmatch_lines([f"lakeflow: Run 77 finished SUCCESS in *: {HOST}/jobs/runs/77"])

    @pytest.mark.parametrize(
        ("call", "flags", "printed"),
        [
            pytest.param("run(stream=True)", [], True, id="stream-without-flag"),
            pytest.param("run(stream=False)", ["--lakeflow-live"], False, id="no-stream-with-flag"),
            pytest.param("start().wait()", ["--lakeflow-live"], True, id="start-with-flag"),
            pytest.param("run()", [], False, id="neither"),
        ],
    )
    def test_stream_argument_overrides_the_flag(
        self,
        pytester: pytest.Pytester,
        fake_ws: FakeWorkspaceClient,
        call: str,
        flags: list[str],
        printed: bool,
    ) -> None:
        queue_transform_failure(fake_ws)
        source = f'def test_job(lakeflow):\n    assert lakeflow.job("my_job").{call}.failed\n'
        result = run_tests(pytester, source, "--log-cli-level=INFO", *flags)
        result.assert_outcomes(passed=1)
        result.stdout.fnmatch_lines([f"*INFO*pytest_databricks_lakeflow*{line}" for line in PROGRESS_LINES])
        streamed = [line for line in result.stdout.lines if line.startswith("lakeflow: ")]
        assert bool(streamed) == printed
        if printed:
            assert "lakeflow: transform: PENDING -> RUNNING" in streamed

    def test_emit_without_a_terminal_reporter(self) -> None:
        plugins = SimpleNamespace(get_plugin=lambda name: None)
        item = cast(pytest.Item, SimpleNamespace(config=SimpleNamespace(pluginmanager=plugins)))
        outcome.emit(item, "transform: PENDING -> RUNNING")
