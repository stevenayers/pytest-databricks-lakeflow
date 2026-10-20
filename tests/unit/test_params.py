"""Job and task parameters of the copy: checks, `task_params`, the test marker and simulated triggers."""

from __future__ import annotations

import copy
from collections.abc import Callable
from typing import Any

import pytest
from databricks.sdk.service import jobs

from pytest_databricks_lakeflow import LakeflowError
from pytest_databricks_lakeflow.settings.params import (
    apply_task_params,
    check_task_params,
    inject_test_marker,
    inject_trigger,
)
from pytest_databricks_lakeflow.settings.stubs import inject_probes

from .conftest import (
    HELPERS,
    NODEID,
    TEMPLATE_PARAMETERS,
    FakeCli,
    configure_lakeflow,
    for_each,
    nested_task,
    run_tests,
    task,
    with_tasks,
)
from .fakes.runs import make_run
from .fakes.templates import dbt_settings, example_settings, for_each_settings, triggered_settings
from .fakes.workspace import FakeWorkspaceClient


class TestTestMarker:
    """inject_test_marker declares __pytest_test_id, replacing the template's definition."""

    @pytest.mark.parametrize(
        ("parameters", "expected"),
        [
            (None, [("__pytest_test_id", NODEID)]),
            (
                [{"name": "param1", "default": "hello"}, {"name": "schema", "default": ""}],
                [("param1", "hello"), ("schema", ""), ("__pytest_test_id", NODEID)],
            ),
            (
                [
                    {"name": "__pytest_test_id", "default": "stale"},
                    {"name": "param1", "default": "hello"},
                    {"name": "schema", "default": ""},
                ],
                [("param1", "hello"), ("schema", ""), ("__pytest_test_id", NODEID)],
            ),
        ],
    )
    def test_inject_test_marker_declares_the_parameter(
        self, parameters: list[dict[str, str]] | None, expected: list[tuple[str, str]]
    ) -> None:
        settings = example_settings()
        settings.parameters = (
            None if parameters is None else [jobs.JobParameterDefinition.from_dict(p) for p in parameters]
        )
        inject_test_marker(settings, NODEID)
        assert [(p.name, p.default) for p in settings.parameters or []] == expected

    def test_inject_test_marker_trims_a_long_node_id(self) -> None:
        nodeid = "tests/test_päth.py::test_rows[" + "中" * 5000 + "]"
        [*_, marker] = inject_test_marker(example_settings(), nodeid).parameters or []
        assert marker.default is not None
        assert len(marker.default.encode()) <= 4096
        assert nodeid.startswith(marker.default)
        assert marker.default.startswith("tests/test_päth.py::test_rows[中")


class TestTaskParams:
    """apply_task_params edits one task's parameters in the shape its type takes, and check_task_params warns."""

    def test_apply_task_params_merges_notebook_base_parameters(self) -> None:
        overrides = {"transform": {"limit": "10", "day": "{{job.parameters.param1}}"}}
        settings = apply_task_params("my_job", example_settings(), overrides)
        notebook = task(settings, "transform").notebook_task
        assert notebook is not None
        assert notebook.base_parameters == {
            "row_count": "{{tasks.extract.values.row_count}}",
            "limit": "10",
            "day": "{{job.parameters.param1}}",
        }
        assert task(settings, "extract") == task(example_settings(), "extract")

    def test_apply_task_params_wheel_named_or_positional(self) -> None:
        settings = example_settings()
        wheel = task(settings, "load").python_wheel_task
        assert wheel is not None
        wheel.named_parameters = {"day": "1"}
        apply_task_params("my_job", settings, {"load": ["--rows", "10"]})
        assert (wheel.parameters, wheel.named_parameters) == (["--rows", "10"], None)
        apply_task_params("my_job", settings, {"load": {"rows": "10"}})
        assert (wheel.parameters, wheel.named_parameters) == (None, {"rows": "10"})
        apply_task_params("my_job", settings, {"load": {"day": "2"}})
        assert wheel.named_parameters == {"rows": "10", "day": "2"}

    @pytest.mark.parametrize(
        ("code", "value", "expected"),
        [
            (
                {"sql_task": {"warehouse_id": "w", "file": {"path": "/q.sql"}, "parameters": {"n": "1", "m": "2"}}},
                {"n": "3"},
                {"sql_task": {"warehouse_id": "w", "file": {"path": "/q.sql"}, "parameters": {"n": "3", "m": "2"}}},
            ),
            (
                {"spark_python_task": {"python_file": "/x.py", "parameters": ["--all"]}},
                ["--rows", "10"],
                {"spark_python_task": {"python_file": "/x.py", "parameters": ["--rows", "10"]}},
            ),
            (
                {"spark_jar_task": {"main_class_name": "x", "parameters": ["--all"]}},
                ["--rows", "10"],
                {"spark_jar_task": {"main_class_name": "x", "parameters": ["--rows", "10"]}},
            ),
            (
                {"dbt_task": {"commands": ["dbt deps", "dbt build"]}},
                ["dbt build --select orders"],
                {"dbt_task": {"commands": ["dbt build --select orders"]}},
            ),
        ],
        ids=["sql-merged", "spark-python-replaced", "jar-replaced", "dbt-replaced"],
    )
    def test_apply_task_params_merges_mappings_and_replaces_lists(
        self, code: dict[str, Any], value: Any, expected: dict[str, Any]
    ) -> None:
        settings = apply_task_params("my_job", with_tasks({"task_key": "code", **code}), {"code": value})
        assert task(settings, "code") == jobs.Task.from_dict({"task_key": "code", **expected})

    @pytest.mark.parametrize(
        ("code", "value", "error", "match"),
        [
            (
                None,
                ["--rows", "10"],
                ValueError,
                r"task_params\['extract'\] must be a mapping of names to strings, merged into "
                r"notebook_task.base_parameters, for task 'extract'; got \['--rows', '10'\]",
            ),
            (
                None,
                {"limit": 10, 1: "x"},
                ValueError,
                r"must be a mapping of names to strings.*got \{'limit': 10, 1: 'x'\}",
            ),
            (
                {"spark_python_task": {"python_file": "/x.py"}},
                {"rows": "10"},
                ValueError,
                r"must be a list of strings, which replaces spark_python_task.parameters, for task 'extract'",
            ),
            (
                {"python_wheel_task": {"package_name": "pkg", "entry_point": "main"}},
                "--rows 10",
                ValueError,
                r"must be a mapping of names to strings, merged into python_wheel_task.named_parameters, or a list "
                r"of strings, which replaces python_wheel_task.parameters, for task 'extract'; got '--rows 10'",
            ),
            (
                {"condition_task": {"op": "EQUAL_TO", "left": "a", "right": "b"}},
                {"limit": "10"},
                LakeflowError,
                r"task_params cannot set the parameters of task 'extract': it is not a notebook, Python script, JAR, "
                r"Python wheel, SQL or dbt task",
            ),
        ],
        ids=["list-for-notebook", "not-strings", "mapping-for-script", "string-for-wheel", "condition"],
    )
    def test_apply_task_params_rejects_wrong_shape(
        self, code: dict[str, Any] | None, value: Any, error: type[Exception], match: str
    ) -> None:
        settings = example_settings()
        if code is not None:
            others = [t for t in settings.tasks or [] if t.task_key != "extract"]
            settings.tasks = [jobs.Task.from_dict({"task_key": "extract", **code}), *others]
        before = copy.deepcopy(settings)
        with pytest.raises(error, match=match):
            apply_task_params("my_job", settings, {"transform": {"limit": "10"}, "extract": value})
        assert settings == before

    def test_apply_task_params_replaces_dbt_commands_and_refuses_none(self) -> None:
        settings = apply_task_params("analytics", dbt_settings(), {"dbt_build": ["dbt build --select tag:ci"]})
        dbt = task(settings, "dbt_build").dbt_task
        assert dbt is not None
        assert dbt.commands == ["dbt build --select tag:ci"]
        assert (dbt.warehouse_id, dbt.catalog, dbt.schema) == ("w1", "prod_catalog", "analytics")
        settings = dbt_settings()
        before = copy.deepcopy(settings)
        with pytest.raises(ValueError, match=r"task_params\['dbt_build'\]: dbt commands must not be empty"):
            apply_task_params("analytics", settings, {"dbt_build": []})
        assert settings == before
        script = with_tasks({"task_key": "code", "spark_python_task": {"python_file": "/x.py", "parameters": ["-v"]}})
        python = task(apply_task_params("my_job", script, {"code": []}), "code").spark_python_task
        assert python is not None and python.parameters == []

    def test_apply_task_params_rejects_unknown_and_reserved_keys(self) -> None:
        settings = inject_probes(example_settings(), {"transform": ["k"]}, HELPERS)
        available = r"Available: \['classic', 'extract', 'load', 'transform'\]"
        with pytest.raises(LakeflowError, match=rf"Unknown task 'nope' in task_params of job 'my_job'. {available}$"):
            apply_task_params("my_job", settings, {"nope": {"limit": "10"}})
        with pytest.raises(LakeflowError, match=rf"Unknown task '__pytest_probe__transform' .*{available}$"):
            apply_task_params("my_job", settings, {"__pytest_probe__transform": {"limit": "10"}})
        with pytest.raises(LakeflowError, match=r"Unknown task 'extract' in task_params .*Available: \['load'\]$"):
            apply_task_params("my_job", example_settings(), {"extract": {"limit": "10"}}, allowed=["load"])

    @pytest.mark.parametrize(
        ("inner", "value", "expected"),
        [
            (
                {"notebook_task": {"notebook_path": "/x", "base_parameters": {"item": "{{input}}"}}},
                {"limit": "10"},
                {"notebook_task": {"notebook_path": "/x", "base_parameters": {"item": "{{input}}", "limit": "10"}}},
            ),
            (
                {"spark_python_task": {"python_file": "/x.py", "parameters": ["{{input}}"]}},
                ["{{input}}", "--rows", "10"],
                {"spark_python_task": {"python_file": "/x.py", "parameters": ["{{input}}", "--rows", "10"]}},
            ),
        ],
        ids=["notebook-merged", "spark-python-replaced"],
    )
    def test_apply_task_params_reaches_a_task_nested_in_a_for_each_task(
        self, inner: dict[str, Any], value: Any, expected: dict[str, Any]
    ) -> None:
        settings = apply_task_params("fanout", with_tasks(for_each({"task_key": "inner", **inner})), {"inner": value})
        assert nested_task(settings, "loop") == jobs.Task.from_dict({"task_key": "inner", **expected})

    def test_apply_task_params_names_the_nested_task_of_a_for_each_task(self) -> None:
        settings = for_each_settings()
        before = copy.deepcopy(settings)
        with pytest.raises(LakeflowError, match=r"of task 'loop': it is not a .* task. Name its nested task, 'each', "):
            apply_task_params("fanout", settings, {"loop": {"limit": "10"}})
        with pytest.raises(LakeflowError, match=r"Unknown task 'nope' .*Available: \['after', 'each', 'loop'\]$"):
            apply_task_params("fanout", settings, {"nope": {"limit": "10"}})
        # task() of the for-each task allows only the for-each task, so its nested task cannot be named.
        with pytest.raises(LakeflowError, match=r"of task 'loop': .* Change it with modify_settings instead.$"):
            apply_task_params("fanout", settings, {"loop": {"limit": "10"}}, allowed=["loop"])
        with pytest.raises(LakeflowError, match=r"Unknown task 'each' .*Available: \['loop'\]$"):
            apply_task_params("fanout", settings, {"each": {"limit": "10"}}, allowed=["loop"])
        assert settings == before

    def test_check_task_params_warns_on_job_parameter_collision(self, recwarn: pytest.WarningsRecorder) -> None:
        settings = example_settings()
        with pytest.warns(UserWarning, match="job value wins") as record:
            check_task_params("my_job", settings, {"transform": {"param1": "x", "limit": "10"}}, {})
        assert [str(w.message) for w in record] == [
            "task_params for 'transform' sets 'param1', also a job parameter of 'my_job'. The job value wins."
        ]
        with pytest.warns(UserWarning, match="sets 'day', also a job parameter"):
            check_task_params("my_job", settings, {"transform": {"day": "1"}, "load": ["day"]}, {"day": "2"})
        recwarn.clear()
        not_strings: Any = {1: "x"}
        quiet = {"transform": {"limit": "10"}, "load": ["param1"], "extract": not_strings}
        check_task_params("my_job", settings, quiet, {"day": "2"})
        assert not recwarn.list
        with pytest.raises(ValueError, match=r"reserved for the plugin: \['__pytest_values'\]"):
            check_task_params("my_job", settings, {"transform": {"__pytest_values": "{}"}}, {})


TABLE_TRIGGER = {"type": "table", "table_update.updated_tables": "cat.sch.orders"}


class TestInjectTrigger:
    """inject_trigger points {{job.trigger.*}} references at job parameters that hold the simulated values."""

    def test_trigger_references_become_parameters_with_the_values_as_defaults(self) -> None:
        template = triggered_settings()
        settings = inject_trigger(template, TABLE_TRIGGER)
        assert template == triggered_settings(), "the argument is not changed"
        assert task(settings, "transform").as_dict()["notebook_task"]["base_parameters"] == {
            "row_count": "{{tasks.extract.values.row_count}}",
            "kind": "{{job.parameters.__pytest_trigger_type}}",
            "tables": "{{job.parameters.__pytest_trigger_table_update_updated_tables}}",
        }
        assert settings.parameters is not None
        assert settings.parameters[:2] == example_settings().parameters
        assert settings.parameters[2:] == [
            jobs.JobParameterDefinition(name="__pytest_trigger_type", default="table"),
            jobs.JobParameterDefinition(name="__pytest_trigger_table_update_updated_tables", default="cat.sch.orders"),
        ]

    def test_references_in_nested_tasks_and_parameter_lists_are_rewritten(self) -> None:
        inner = {
            "task_key": "each",
            "notebook_task": {"notebook_path": "/e", "base_parameters": {"k": "{{ job.trigger.type }}"}},
        }
        wheel = {
            "task_key": "wheel",
            "python_wheel_task": {
                "package_name": "pkg",
                "entry_point": "main",
                "parameters": ["--kind", "on_{{job.trigger.type}}", "{{job.trigger.file_arrival.location}}"],
            },
        }
        trigger = {"type": "file_arrival", "file_arrival.location": "/Volumes/c/s/v"}
        settings = inject_trigger(with_tasks(for_each(inner), wheel), trigger)
        assert nested_task(settings, "loop").as_dict()["notebook_task"]["base_parameters"] == {
            "k": "{{job.parameters.__pytest_trigger_type}}"
        }
        assert task(settings, "wheel").as_dict()["python_wheel_task"]["parameters"] == [
            "--kind",
            "on_{{job.parameters.__pytest_trigger_type}}",
            "{{job.parameters.__pytest_trigger_file_arrival_location}}",
        ]

    @pytest.mark.parametrize(
        ("trigger", "error", "match"),
        [
            pytest.param(
                {"nope": "x"},
                LakeflowError,
                r"trigger names 'nope', but the job has no \{\{job.trigger.nope\}\} reference. Trigger references in "
                r"the job: \['\{\{job.trigger.table_update.updated_tables\}\}', '\{\{job.trigger.type\}\}'\]",
                id="unknown",
            ),
            pytest.param(
                {"type": ["table"]},
                ValueError,
                r"trigger\['type'\] must be a string, the value a job parameter holds, got \['table'\]",
                id="not-a-string",
            ),
        ],
    )
    def test_inject_trigger_rejects(self, trigger: dict[str, Any], error: type[Exception], match: str) -> None:
        settings = triggered_settings()
        with pytest.raises(error, match=match):
            inject_trigger(settings, trigger)
        assert settings == triggered_settings()

    def test_references_left_unrewritten_are_named_in_a_warning(self) -> None:
        wheel = {
            "task_key": "wheel",
            "python_wheel_task": {
                "package_name": "p",
                "entry_point": "m",
                "parameters": ["{{job.trigger.time.iso_date}}"],
            },
        }
        settings = triggered_settings()
        settings.tasks = [*(settings.tasks or []), jobs.Task.from_dict(wheel)]
        with pytest.warns(
            UserWarning, match=r"\['job.trigger.table_update.updated_tables', 'job.trigger.time.iso_date'\]"
        ):
            inject_trigger(settings, {"type": "table"})

    @pytest.mark.filterwarnings("error")
    @pytest.mark.parametrize("template", [triggered_settings, example_settings])
    def test_no_trigger_changes_nothing(self, template: Callable[[], jobs.JobSettings]) -> None:
        assert inject_trigger(template(), {}).as_dict() == template().as_dict()


TRIGGER_PARAMETERS = [
    jobs.JobParameterDefinition(name="__pytest_trigger_type", default="table"),
    jobs.JobParameterDefinition(name="__pytest_trigger_table_update_updated_tables", default="cat.sch.orders"),
]


@pytest.mark.usefixtures("fake_cli", "bundle_dir")
class TestTrigger:
    """Simulating a trigger with trigger= on job() and task()."""

    @pytest.mark.parametrize(
        ("call", "states"),
        [
            pytest.param('lakeflow.job("triggered", trigger=TRIGGER)', {"transform": "SUCCESS"}, id="job"),
            pytest.param(
                'lakeflow.task("triggered.transform", upstream_task_values={"extract": {"row_count": 1}}, '
                "trigger=TRIGGER)",
                {"extract": "SUCCESS", "transform": "SUCCESS"},
                id="task",
            ),
        ],
    )
    def test_trigger_values_reach_the_created_job(
        self,
        pytester: pytest.Pytester,
        fake_ws: FakeWorkspaceClient,
        fake_cli: FakeCli,
        call: str,
        states: dict[str, str],
    ) -> None:
        fake_cli.deployed["triggered"] = fake_ws.jobs.add(triggered_settings())
        fake_ws.jobs.queue(make_run(77, states))
        source = f"""
            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            TRIGGER = {{"type": "table", "table_update.updated_tables": "cat.sch.orders"}}

            def test_job(lakeflow):
                with {call} as job:
                    run = job.run()
                    assert run.trigger_type == "ONE_TIME"
                    assert "__pytest_trigger_type" not in run.parameters
                    with pytest.raises(ValueError, match="reserved for the plugin"):
                        job.run(params={{"__pytest_trigger_type": "file_arrival"}})
            """
        run_tests(pytester, source).assert_outcomes(passed=1)
        [created] = fake_ws.jobs.created
        assert created["parameters"] == [*TEMPLATE_PARAMETERS, *TRIGGER_PARAMETERS]
        transform = next(t for t in created["tasks"] if t.task_key == "transform")
        assert transform.notebook_task.base_parameters["kind"] == "{{job.parameters.__pytest_trigger_type}}"

    def test_a_trigger_reference_the_copy_cannot_resolve_fails_before_creation(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli
    ) -> None:
        fake_cli.deployed["triggered"] = fake_ws.jobs.add(triggered_settings())
        source = """
            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            def test_job(lakeflow):
                job = lakeflow.job("triggered", trigger={"type": "table"})
                with pytest.raises(LakeflowError) as e:
                    job.create()
                assert str(e.value) == (
                    "Task 'transform' of 'triggered' refers to {{job.trigger.table_update.updated_tables}}, but the "
                    "copy has no table update trigger, and jobs/create refuses the reference without one. Set its "
                    "value with trigger={'table_update.updated_tables': ...} on job() or task()."
                )
            """
        run_tests(pytester, source).assert_outcomes(passed=1)
        assert fake_ws.jobs.created == []

    def test_an_unknown_trigger_key_fails_before_creation(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli
    ) -> None:
        fake_cli.deployed["triggered"] = fake_ws.jobs.add(triggered_settings())
        source = 'def test_job(lakeflow):\n    lakeflow.job("triggered", trigger={"nope": "x"})\n'
        result = run_tests(pytester, source)
        result.assert_outcomes(failed=1)
        result.stdout.fnmatch_lines(["*trigger names 'nope', but the job has no {{job.trigger.nope}} reference*"])
        assert fake_ws.jobs.created == []


@pytest.mark.usefixtures("fake_cli", "bundle_dir")
class TestParams:
    """Checking parameters against the job's declarations."""

    def test_bad_params_fail_before_creating(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        result = run_tests(
            pytester,
            """
            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            def test_job(lakeflow):
                message = "does not declare the parameters ..parm1.*allow_undeclared_params"
                with pytest.raises(LakeflowError, match=message):
                    with lakeflow.job("my_job", params={"parm1": "x"}):
                        pass
                with lakeflow.job("my_job") as job:
                    with pytest.raises(LakeflowError, match="does not declare"):
                        job.run(params={"typo": "x"})
            """,
        )
        result.assert_outcomes(passed=1)
        assert fake_ws.jobs.run_now_calls == []

    def test_undeclared_params_when_allowed(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        configure_lakeflow(pytester, "allow_undeclared_params=True")
        source = (
            'def test_job(lakeflow):\n    lakeflow.job("my_job", params={"typo": "x"}).run(params={"other": "y"})\n'
        )
        run_tests(pytester, source).assert_outcomes(passed=1)
        assert fake_ws.jobs.run_now_calls[0]["job_parameters"] == {"typo": "x", "other": "y"}

    @pytest.mark.parametrize("arguments", ["", "test_marker=True"])
    def test_test_marker_cannot_be_passed(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, arguments: str
    ) -> None:
        configure_lakeflow(pytester, arguments)
        result = run_tests(
            pytester,
            """
            import pytest

            def test_job(lakeflow):
                with pytest.raises(ValueError, match="reserved for the plugin"):
                    lakeflow.job("my_job", params={"__pytest_test_id": "x"})
                with lakeflow.job("my_job") as job:
                    with pytest.raises(ValueError, match="reserved for the plugin"):
                        job.run(params={"__pytest_test_id": "x"})
            """,
        )
        result.assert_outcomes(passed=1)
        assert fake_ws.jobs.run_now_calls == []

    def test_params_declared_by_modify_settings_are_accepted(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        pytester.makeconftest(
            """
            import pytest
            from databricks.sdk.service import jobs

            from pytest_databricks_lakeflow import Lakeflow

            def declare_catalog(settings, item):
                settings.parameters = [*settings.parameters, jobs.JobParameterDefinition(name="catalog", default="")]

            @pytest.fixture(scope="session")
            def lakeflow():
                return Lakeflow(modify_settings=declare_catalog)
            """
        )
        result = run_tests(
            pytester,
            r"""
            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            def test_declared_by_callback(lakeflow):
                with lakeflow.job("my_job", params={"catalog": "x"}) as job:
                    job.run()

            def test_typo_still_fails(lakeflow):
                with pytest.raises(LakeflowError, match=r"does not declare the parameters \['typo'\]"):
                    with lakeflow.job("my_job", params={"typo": "x"}):
                        pass

            def test_reserved_prefix_fails_early(lakeflow):
                with pytest.raises(ValueError, match="reserved"):
                    lakeflow.job("my_job", params={"__pytest_keys": "x"})
            """,
        )
        result.assert_outcomes(passed=3)
        assert len(fake_ws.jobs.created) == 1
        assert fake_ws.jobs.run_now_calls[0]["job_parameters"] == {"catalog": "x"}


@pytest.mark.usefixtures("fake_cli", "bundle_dir")
class TestParamsEndToEnd:
    """`task_params` and the test marker reach a task job."""

    def test_dbt_commands_on_a_task_job(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli
    ) -> None:
        fake_cli.deployed["analytics"] = fake_ws.jobs.add(dbt_settings())
        fake_ws.jobs.queue(make_run(81, {"dbt_build": "SUCCESS"}))
        pytester.makepyfile(
            """
            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            def test_task(lakeflow):
                commands = ["dbt build --select tag:ci"]
                with lakeflow.task("analytics.dbt_build", task_params={"dbt_build": commands}) as task:
                    assert task.run().succeeded

            def test_no_commands(lakeflow):
                with pytest.raises(ValueError, match="dbt commands must not be empty"):
                    lakeflow.task("analytics.dbt_build", task_params={"dbt_build": []})
            """
        )
        pytester.runpytest().assert_outcomes(passed=2)
        [created] = fake_ws.jobs.created
        [dbt_build] = created["tasks"]
        assert dbt_build.dbt_task.commands == ["dbt build --select tag:ci"]
        assert dbt_build.dbt_task.warehouse_id == "w1"

    def test_test_marker_on_a_task_job(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        pytester.makepyfile(
            """
            def test_job(lakeflow):
                upstream = {"extract": {"row_count": 42}}
                with lakeflow.task("my_job.transform", upstream_task_values=upstream, capture_task_values=["stage"]):
                    pass
            """
        )
        pytester.runpytest().assert_outcomes(passed=1)
        configure_lakeflow(pytester, "test_marker=True")
        pytester.runpytest().assert_outcomes(passed=1)
        plain, marked = fake_ws.jobs.created
        assert marked["parameters"] == [
            *plain["parameters"],
            jobs.JobParameterDefinition(name="__pytest_test_id", default="test_test_marker_on_a_task_job.py::test_job"),
        ]
        assert marked["tasks"] == plain["tasks"]
        assert [t.task_key for t in marked["tasks"]] == ["extract", "transform", "__pytest_probe__transform"]
