"""What `run(only=...)` selects, and the references a selection cannot resolve."""

from __future__ import annotations

import pytest
from databricks.sdk.service import jobs

from pytest_databricks_lakeflow import LakeflowError
from pytest_databricks_lakeflow.settings.lifecycle import inject_lifecycle
from pytest_databricks_lakeflow.settings.selection import check_disabled, check_references, select
from pytest_databricks_lakeflow.settings.tasks import disable_tasks

from .conftest import HELPERS, task, three_tasks
from .fakes.templates import example_settings


class TestSelect:
    """select adds the probes and stubs that the tasks in only need, and check_references what they refer to."""

    @staticmethod
    def chain() -> jobs.JobSettings:
        def t(key: str, *deps: str) -> jobs.Task:
            return jobs.Task(task_key=key, depends_on=[jobs.TaskDependency(task_key=d) for d in deps])

        return jobs.JobSettings(
            tasks=[
                t("extract"),
                t("transform", "extract"),
                t("load", "transform"),
                t("report", "load"),
                t("other"),
                t("__pytest_probe__transform", "transform"),
                t("__pytest_probe__other", "other"),
            ]
        )

    @pytest.mark.parametrize(
        ("only", "expected"),
        [
            (["transform"], ["transform", "__pytest_probe__transform"]),
            (["+transform"], ["+transform", "__pytest_probe__transform"]),
            (["load+"], ["load+"]),
            (["+transform+"], ["+transform+"]),
            (["other", "extract"], ["other", "extract", "__pytest_probe__other"]),
        ],
    )
    def test_select_adds_probes(self, only: list[str], expected: list[str]) -> None:
        assert select(self.chain(), only) == expected

    def test_select_adds_stubs_of_selected_tasks(self) -> None:
        chain = self.chain()
        assert select(chain, ["transform"], stubs={"extract"}) == ["transform", "__pytest_probe__transform", "extract"]
        assert select(chain, ["+transform"], stubs={"extract"}) == ["+transform", "__pytest_probe__transform"]

    def test_select_rejects_unknown_tasks(self) -> None:
        with pytest.raises(LakeflowError, match=r"Unknown task 'lod' in only. Available: \['extract', 'load'"):
            select(self.chain(), ["lod+"])

    def test_select_always_adds_setup_and_teardown(self) -> None:
        settings = inject_lifecycle(three_tasks(), "/h/setup_1", "/h/teardown_1", HELPERS)
        assert select(settings, ["b"]) == ["b", "__pytest_setup", "__pytest_teardown"]
        assert select(settings, ["+b"]) == ["+b", "__pytest_teardown"]
        assert select(inject_lifecycle(three_tasks(), "/h/setup_1", None, HELPERS), ["b"]) == ["b", "__pytest_setup"]

    @pytest.mark.parametrize(
        ("only", "expected"),
        [
            pytest.param(["transform"], ["transform", "__pytest_setup"], id="plain"),
            pytest.param(["+transform"], ["+transform"], id="upstream"),
            # run_now cannot leave a task out of a `key+` selection, so a selection that reaches it is spelt out.
            pytest.param(["transform+"], ["__pytest_setup", "load", "transform"], id="downstream"),
        ],
    )
    def test_select_leaves_out_excluded_tasks(self, only: list[str], expected: list[str]) -> None:
        settings = inject_lifecycle(example_settings(), "/h/setup_1", "/h/teardown_1", HELPERS)
        assert select(settings, only, exclude={"__pytest_teardown"}) == expected

    def test_spelt_out_selection_leaves_out_the_disabled_tasks_it_reaches(self) -> None:
        # The platform runs a disabled task that only names, but not one that a `+` reaches.
        settings = inject_lifecycle(disable_tasks(example_settings(), ["transform"]), "/h/s", "/h/t", HELPERS)
        assert select(settings, ["extract+"], exclude={"__pytest_teardown"}) == ["__pytest_setup", "extract"]
        assert select(settings, ["transform"], exclude={"__pytest_teardown"}) == ["transform", "__pytest_setup"]

    @pytest.mark.parametrize(
        ("only", "named"),
        [
            pytest.param(["transform"], "['transform']", id="disabled"),
            pytest.param(["transform+"], "['transform']", id="disabled_with_downstream"),
            pytest.param(["extract", "+load"], "['load']", id="downstream_of_disabled"),
            pytest.param(["transform", "load"], "['load', 'transform']", id="several"),
            # A `+` that reaches a disabled task does not run it, nor the tasks downstream of it.
            pytest.param(["extract+"], None, id="reached"),
            pytest.param(["extract", "classic"], None, id="enabled"),
        ],
    )
    def test_check_disabled_refuses_naming_a_disabled_task(self, only: list[str], named: str | None) -> None:
        # Probed: the platform runs a disabled task that only names, and with `key+` the tasks downstream of it.
        settings = inject_lifecycle(disable_tasks(example_settings(), ["transform"]), "/h/s", "/h/t", HELPERS)
        if named is None:
            check_disabled(settings, only)
            return
        with pytest.raises(LakeflowError) as raised:
            check_disabled(settings, only)
        assert str(raised.value) == (
            f"only names {named}, which are disabled in the copy or downstream of a disabled task, and the platform "
            "runs a disabled task that only names. Leave them out of only, or do not disable them."
        )

    @pytest.mark.parametrize(
        ("selected", "stubs", "missing"),
        [
            pytest.param({"transform"}, set(), "extract", id="left_out"),
            pytest.param({"transform", "load"}, set(), "extract", id="left_out_by_key_plus"),
            pytest.param({"transform", "extract"}, set(), None, id="selected"),
            pytest.param({"transform"}, {"extract"}, None, id="stubbed"),
            pytest.param({"load", "classic"}, set(), None, id="no_references"),
        ],
    )
    def test_check_references_over_selected_tasks(
        self, selected: set[str], stubs: set[str], missing: str | None
    ) -> None:
        if missing is None:
            check_references(example_settings(), selected, stubs)
            return
        with pytest.raises(LakeflowError, match=rf"Task 'transform' refers to task values of \['{missing}'\]"):
            check_references(example_settings(), selected, stubs)

    @pytest.mark.parametrize("field", ["result_state", "error_code", "run_id"])
    def test_check_references_allows_other_references_to_a_left_out_task(self, field: str) -> None:
        settings = example_settings()
        transform = (settings.tasks or [])[1]
        assert transform.notebook_task is not None
        transform.notebook_task.base_parameters = {"upstream": f"{{{{tasks.extract.{field}}}}}"}
        check_references(settings, {"transform"}, set())

    def test_check_references_refuses_output_references_to_a_left_out_task(self) -> None:
        # Probed on the sql bundle: the platform fails such a run with INVALID_RUN_CONFIGURATION "Failed to resolve
        # references: Cannot access output from task 'query' because the task was skipped."
        settings = example_settings()
        transform = task(settings, "transform")
        assert transform.notebook_task is not None
        transform.notebook_task.base_parameters = {"row": "{{tasks.extract.output.first_row}}"}
        message = (
            "Task 'transform' refers to the output of ['extract'], which only leaves out, so the platform could not "
            "resolve it. Select them in only, or stub them with Stub(outputs=...) in upstream_task_values on job()."
        )
        with pytest.raises(LakeflowError) as excinfo:
            check_references(settings, {"transform"}, set())
        assert str(excinfo.value) == message
        check_references(settings, {"transform", "extract"}, set())

    def test_check_references_counts_only_the_stubs_that_run(self) -> None:
        settings = example_settings()
        load = task(settings, "load")
        assert load.python_wheel_task is not None
        load.python_wheel_task.named_parameters = {"rows": "{{tasks.extract.values.row_count}}"}
        assert select(settings, ["load"], stubs={"extract"}) == ["load"], "load does not depend on the stub directly"
        with pytest.raises(LakeflowError, match=r"Task 'load' refers to task values of \['extract'\]"):
            check_references(settings, {"load"}, {"extract"})
        check_references(settings, {"load", "transform"}, {"extract"})
