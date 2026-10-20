"""Editing the copy's tasks: disabling, retries and timeouts, forced conditions and the tasks that run other jobs."""

from __future__ import annotations

import copy
import json
import logging
from typing import Any

import pytest
from databricks.sdk.service import jobs

from pytest_databricks_lakeflow import LakeflowError
from pytest_databricks_lakeflow.settings.tasks import (
    TASK_OVERRIDES,
    disable_tasks,
    disabled_task_keys,
    force_conditions,
    override_tasks,
    run_job_tasks,
)

from .conftest import FakeCli, configure_lakeflow, run_tests, task
from .fakes.runs import make_run, skipped_run
from .fakes.templates import branching_settings, example_settings, parent_settings
from .fakes.workspace import FakeWorkspaceClient


class TestDisableAndRunJobTasks:
    """disable_tasks marks tasks disabled, and run_job_tasks lists the enabled tasks that run another job."""

    def test_disable_tasks(self) -> None:
        settings = disable_tasks(example_settings(), ["load"])
        assert {t.task_key: t.disabled for t in settings.tasks or []} == {
            "extract": None,
            "transform": None,
            "load": True,
            "classic": None,
        }
        with pytest.raises(LakeflowError, match=r"Unknown task 'nope' in disable. Available: \['classic', 'extract'"):
            disable_tasks(example_settings(), ["nope"])

    def test_run_job_tasks_finds_run_job_tasks(self) -> None:
        assert run_job_tasks(example_settings()) == []
        [found] = run_job_tasks(parent_settings())
        assert found.task_key == "trigger_child"
        assert found.run_job_task is not None and found.run_job_task.job_id == 123

    def test_run_job_tasks_skips_disabled_tasks_their_dependants_and_tasks_in_disabled_loops(self) -> None:
        settings = parent_settings()
        nested = jobs.Task(task_key="each_child", run_job_task=jobs.RunJobTask(job_id=456))
        loop = jobs.Task(
            task_key="loop",
            depends_on=[jobs.TaskDependency(task_key="prepare")],
            for_each_task=jobs.ForEachTask(inputs="[1, 2]", task=nested),
        )
        settings.tasks = [*(settings.tasks or []), loop]
        assert [t.task_key for t in run_job_tasks(settings)] == ["trigger_child", "each_child"]
        loop_disabled = disable_tasks(copy.deepcopy(settings), ["loop"])
        assert [t.task_key for t in run_job_tasks(loop_disabled)] == ["trigger_child"]
        assert run_job_tasks(disable_tasks(copy.deepcopy(settings), ["trigger_child", "loop"])) == []
        # The platform disables every task downstream of a disabled task: here trigger_child and the loop.
        assert run_job_tasks(disable_tasks(settings, ["prepare"])) == []


class TestDisabledTaskKeys:
    """Which tasks a disabled task takes with it."""

    @pytest.mark.parametrize(
        ("disabled", "all_done", "expected"),
        [
            ([], False, set()),
            (["extract"], False, {"extract", "transform", "load"}),
            (["transform"], False, {"transform", "load"}),
            (["load"], False, {"load"}),
            (["classic"], False, {"classic"}),
            (["extract"], True, {"extract", "transform", "load"}),
        ],
    )
    def test_disabled_task_keys(self, disabled: list[str], all_done: bool, expected: set[str]) -> None:
        settings = disable_tasks(example_settings(), disabled)
        if all_done:
            task(settings, "load").run_if = jobs.RunIf.ALL_DONE
        assert disabled_task_keys(settings) == expected


class TestOverrideTasks:
    """override_tasks sets the timeout and retry fields of named tasks, and refuses unknown tasks and fields."""

    def test_override_tasks_sets_timeout_and_retry_fields(self) -> None:
        overrides = {"timeout_seconds": 60, "max_retries": 1, "min_retry_interval_millis": 0, "retry_on_timeout": True}
        settings = override_tasks(example_settings(), {"load": overrides})
        load = task(settings, "load")
        assert (load.timeout_seconds, load.max_retries, load.min_retry_interval_millis, load.retry_on_timeout) == (
            60,
            1,
            0,
            True,
        )
        assert task(settings, "load").python_wheel_task is not None
        for key in ("extract", "transform", "classic"):
            assert task(settings, key) == task(example_settings(), key)
        assert set(overrides) == TASK_OVERRIDES

    @pytest.mark.parametrize(
        ("overrides", "error", "match"),
        [
            pytest.param(
                {"nope": {"max_retries": 1}},
                LakeflowError,
                r"Unknown task 'nope' in task_overrides. Available: \['classic', 'extract', 'load', 'transform'\]",
                id="task",
            ),
            pytest.param(
                {"load": {"max_retries": 1, "notebook_task": {}}},
                ValueError,
                r"Unknown field 'notebook_task' in task_overrides\['load'\]. Allowed: \['max_retries', "
                r"'min_retry_interval_millis', 'retry_on_timeout', 'timeout_seconds'\]",
                id="field",
            ),
        ],
    )
    def test_override_tasks_rejects_unknown_task_or_field(
        self, overrides: dict[str, dict[str, object]], error: type[Exception], match: str
    ) -> None:
        settings = example_settings()
        with pytest.raises(error, match=match):
            override_tasks(settings, overrides)
        assert settings == example_settings(), "nothing is changed when an entry is refused"


class TestForceConditions:
    """force_conditions replaces the operands of condition tasks with constants that give the chosen outcome."""

    @pytest.mark.parametrize(("outcome", "expected"), [(True, "true"), (False, "false")])
    def test_force_conditions_rewrites_operands(
        self, caplog: pytest.LogCaptureFixture, outcome: bool, expected: str
    ) -> None:
        template = branching_settings()
        with caplog.at_level(logging.INFO, logger="pytest_databricks_lakeflow"):
            forced = force_conditions(branching_settings(), {"check": outcome})
        check = task(forced, "check")
        assert check.condition_task == jobs.ConditionTask(op=jobs.ConditionTaskOp.EQUAL_TO, left="true", right=expected)
        assert check.depends_on == task(template, "check").depends_on
        assert check.run_if == task(template, "check").run_if
        assert "{{tasks." not in json.dumps(check.as_dict())
        others = [t.as_dict() for t in forced.tasks or [] if t.task_key != "check"]
        assert others == [t.as_dict() for t in template.tasks or [] if t.task_key != "check"]
        assert (
            f"Forcing condition task 'check' to {expected}; it compared '{{{{tasks.check_input.values.rows}}}}' "
            "GREATER_THAN '0'"
        ) in caplog.messages

    def test_force_conditions_accepts_an_op_the_sdk_does_not_know(self, caplog: pytest.LogCaptureFixture) -> None:
        settings = branching_settings()
        check = task(settings, "check")
        assert check.condition_task is not None
        # The SDK reads an operator it does not know as None.
        check.condition_task = jobs.ConditionTask.from_dict({"op": "SOME_NEW_OP", "left": "a", "right": "b"})
        with caplog.at_level(logging.INFO, logger="pytest_databricks_lakeflow"):
            forced = force_conditions(settings, {"check": True})
        assert task(forced, "check").condition_task == jobs.ConditionTask(
            op=jobs.ConditionTaskOp.EQUAL_TO, left="true", right="true"
        )
        assert "Forcing condition task 'check' to true; it compared 'a' None 'b'" in caplog.messages

    @pytest.mark.parametrize(
        ("disable", "outcomes", "error", "match"),
        [
            (
                [],
                {"on_true": True},
                LakeflowError,
                r"Task 'on_true' in force_conditions has no condition_task. .*\['check'\]",
            ),
            (
                [],
                {"nope": True},
                LakeflowError,
                r"Unknown task 'nope' in force_conditions. Available: \['check', 'check_input'",
            ),
            ([], {"check": "true"}, ValueError, r"force_conditions\['check'\] must be True or False, got 'true'"),
            (["check"], {"check": True}, LakeflowError, "Task 'check' in force_conditions is disabled"),
            (
                ["check_input"],
                {},
                LakeflowError,
                "Task 'check' in force_conditions is disabled, so its condition is never evaluated",
            ),
        ],
    )
    def test_force_conditions_rejects(
        self, disable: list[str], outcomes: dict[str, Any], error: type[Exception], match: str
    ) -> None:
        settings = disable_tasks(branching_settings(), disable)
        with pytest.raises(error, match=match):
            force_conditions(settings, {"check": False, **outcomes})
        assert settings == disable_tasks(branching_settings(), disable)

    def test_force_conditions_without_outcomes_changes_nothing(self) -> None:
        assert force_conditions(example_settings(), {}) == example_settings()


@pytest.mark.usefixtures("fake_cli", "bundle_dir")
class TestDisabledTasks:
    """Disabling tasks on the copy, and telling disabled tasks from those `only` left out."""

    def test_disabled_tasks_come_from_the_copy_not_the_run(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        fake_ws.jobs.queue(make_run(77, {"extract": "SUCCESS", "load": "DISABLED", "transform": "DISABLED"}))
        result = run_tests(
            pytester,
            """
            def test_job(lakeflow):
                run = lakeflow.job("my_job", disable=["load"]).run()
                assert sorted(run.disabled_tasks) == ["load"]
                assert sorted(run.skipped_tasks) == ["transform"]
                assert not run.task("extract").disabled
            """,
        )
        result.assert_outcomes(passed=1)
        [created] = fake_ws.jobs.created
        assert {t.task_key: t.disabled for t in created["tasks"]} == {
            "extract": None,
            "transform": None,
            "load": True,
            "classic": None,
        }

    def test_tasks_downstream_of_a_disabled_task_are_disabled(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        states = {"extract": "DISABLED", "transform": "DISABLED", "load": "DISABLED", "classic": "SUCCESS"}
        fake_ws.jobs.queue(make_run(77, states))
        result = run_tests(
            pytester,
            """
            import pytest

            def test_job(lakeflow):
                run = lakeflow.job("my_job", disable=["extract"]).run()
                assert sorted(run.disabled_tasks) == ["extract", "load", "transform"]
                assert run.skipped_tasks == {}
                assert run.succeeded
                assert run.executed_tasks == ["classic"]
                with pytest.raises(KeyError, match=r"Disabled in the job: \\['extract', 'load', 'transform'\\]\\."):
                    run.task("transform")
            """,
        )
        result.assert_outcomes(passed=1)

    def test_tasks_left_out_by_only_stay_skipped(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        states = {"extract": "DISABLED", "transform": "DISABLED", "load": "DISABLED", "classic": "SUCCESS"}
        fake_ws.jobs.queue(make_run(77, states))
        result = run_tests(
            pytester,
            """
            import pytest

            def test_job(lakeflow):
                run = lakeflow.job("my_job").run(only=["classic"])
                assert sorted(run.skipped_tasks) == ["extract", "load", "transform"]
                assert run.disabled_tasks == {}
                with pytest.raises(KeyError, match=r"Skipped by only: \\['extract', 'load', 'transform'\\]"):
                    run.task("transform")
            """,
        )
        result.assert_outcomes(passed=1)

    def test_a_run_with_every_task_disabled_ran_nothing(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        keys = ["extract", "transform", "load", "classic"]
        fake_ws.jobs.queue(skipped_run(77, keys, notebooks=["extract", "transform", "classic"]))
        result = run_tests(
            pytester,
            """
            import pytest

            from pytest_databricks_lakeflow import LakeflowRunFailed

            def test_job(lakeflow):
                run = lakeflow.job("my_job", disable=["extract", "classic"]).run()
                assert run.tasks == {}
                assert sorted(run.disabled_tasks) == ["classic", "extract", "load", "transform"]
                assert not any(task.ran for task in run.disabled_tasks.values())
                assert run.executed_tasks == []
                assert not run.succeeded
                with pytest.raises(LakeflowRunFailed) as raised:
                    run.raise_for_failure()
                reason = (
                    "Skipped because every task is disabled. Re-enable at least one task to be able to run the job."
                )
                assert f"run: {reason}" in str(raised.value)
            """,
        )
        result.assert_outcomes(passed=1)
        assert fake_ws.jobs.output_calls == []
        assert fake_ws.jobs.export_calls == []

    def test_template_disabled_tasks_are_reported_separately(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        callback = """

def disable_classic(settings, item):
    next(t for t in settings.tasks if t.task_key == "classic").disabled = True
"""
        configure_lakeflow(pytester, "modify_settings=disable_classic", callback)
        states = {"extract": "SUCCESS", "transform": "SUCCESS", "load": "SUCCESS", "classic": "DISABLED"}
        fake_ws.jobs.queue(make_run(77, states))
        result = run_tests(
            pytester,
            """
            import pytest

            def test_job(lakeflow):
                run = lakeflow.job("my_job").run()
                assert run.succeeded
                assert sorted(run.disabled_tasks) == ["classic"]
                assert run.skipped_tasks == {}
                assert run.executed_tasks == ["extract", "load", "transform"]
                with pytest.raises(KeyError, match="Disabled in the job"):
                    run.task("classic")
            """,
        )
        result.assert_outcomes(passed=1)

    @pytest.mark.parametrize("call", ['run(only=["transform+"])', 'start(only=["load"])'], ids=["run", "start"])
    def test_only_refuses_disabled_tasks_before_the_run(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, call: str
    ) -> None:
        result = run_tests(
            pytester,
            f"""
import pytest

from pytest_databricks_lakeflow import LakeflowError

def test_job(lakeflow):
    with pytest.raises(LakeflowError, match="the platform runs a disabled task that only names"):
        lakeflow.job("my_job", disable=["transform"]).{call}
""",
        )
        result.assert_outcomes(passed=1)
        assert fake_ws.jobs.run_now_calls == []

    def test_disable_rejects_unknown_tasks(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        result = run_tests(pytester, 'def test_job(lakeflow):\n    lakeflow.job("my_job", disable=["nope"]).run()\n')
        result.assert_outcomes(failed=1)
        result.stdout.fnmatch_lines(["*Unknown task 'nope' in disable. Available: ['classic', 'extract', 'load', *"])
        assert fake_ws.jobs.created == []


@pytest.mark.usefixtures("fake_cli", "bundle_dir")
class TestForcedConditions:
    """Forcing the outcome of if/else condition tasks in the copy."""

    def test_forced_condition_takes_the_chosen_branch(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli
    ) -> None:
        fake_cli.deployed["branching_job"] = fake_ws.jobs.add(branching_settings())
        states = {"check_input": "SUCCESS", "check": "SUCCESS", "on_true": "SUCCESS", "on_false": "EXCLUDED"}
        fake_ws.jobs.queue(make_run(77, states, condition_outcomes={"check": "true"}))
        result = run_tests(
            pytester,
            """
            def test_job(lakeflow):
                run = lakeflow.job("branching_job", params={"rows": "0"}, force_conditions={"check": True}).run()
                assert run.branch_taken("check") is True
                assert run.task("on_false").excluded
                assert run.executed_tasks == ["check", "check_input", "on_true"]
            """,
        )
        result.assert_outcomes(passed=1)
        [created] = fake_ws.jobs.created
        tasks = {t.task_key: t for t in created["tasks"]}
        forced = jobs.ConditionTask(op=jobs.ConditionTaskOp.EQUAL_TO, left="true", right="true")
        assert tasks["check"].condition_task == forced
        assert tasks["on_true"].depends_on == [jobs.TaskDependency(task_key="check", outcome="true")]

    def test_force_conditions_rejects_tasks_without_a_condition(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli
    ) -> None:
        fake_cli.deployed["branching_job"] = fake_ws.jobs.add(branching_settings())
        result = run_tests(
            pytester,
            """
            def test_job(lakeflow):
                lakeflow.job("branching_job", force_conditions={"check_input": True}).run()
            """,
        )
        result.assert_outcomes(failed=1)
        result.stdout.fnmatch_lines(["*Task 'check_input' in force_conditions has no condition_task*"])
        assert fake_ws.jobs.created == []
