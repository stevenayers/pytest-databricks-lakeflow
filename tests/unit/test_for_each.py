"""For-each tasks: shorter input lists, a body lifted out of its loop, and the iteration input."""

from __future__ import annotations

import copy
import json
from collections.abc import Callable
from dataclasses import replace
from typing import Any

import pytest
from databricks.sdk.service import jobs

from pytest_databricks_lakeflow import LakeflowError
from pytest_databricks_lakeflow.settings.for_each import (
    check_input_size,
    for_each_bodies,
    inject_input,
    lift_nested_task,
    override_for_each_inputs,
)
from pytest_databricks_lakeflow.settings.references import escape

from .conftest import FakeCli, configure_lakeflow, nested_task, run_tests, task
from .fakes.runs import make_run
from .fakes.templates import example_settings, fanout_objects_settings, for_each_settings
from .fakes.workspace import FakeWorkspaceClient


class TestForEachInputs:
    """override_for_each_inputs replaces the input list of for-each tasks, and refuses other tasks and bad lists."""

    def test_for_each_inputs_replace_the_deployed_list(self) -> None:
        template = for_each_settings()
        settings = override_for_each_inputs(for_each_settings(), {"loop": ["a", "b"]})
        loop = task(settings, "loop").for_each_task
        assert loop is not None
        assert loop.inputs == '["a", "b"]'
        assert loop.task == nested_task(template, "loop")
        assert task(settings, "after") == task(template, "after")

    @pytest.mark.parametrize(
        ("inputs", "expected"),
        [
            pytest.param([{"k": "{{x}}"}], '[{"k": "{\\u007bx}}"}]', id="reference"),
            pytest.param([], "[]", id="empty"),
            pytest.param(("a", 1), '["a", 1]', id="tuple"),
            pytest.param(["x" * 4996], json.dumps(["x" * 4996]), id="at-the-limit"),
            # jobs/create accepts primitives of mixed types, and objects whose values are lists, objects or null.
            pytest.param([1, "a", True, 1.5], '[1, "a", true, 1.5]', id="primitives"),
            pytest.param([{"k": [1]}, {"j": {"x": None}}], '[{"k": [1]}, {"j": {"x": null}}]', id="objects"),
        ],
    )
    def test_for_each_inputs_are_escaped_json(self, inputs: Any, expected: str) -> None:
        settings = override_for_each_inputs(for_each_settings(), {"loop": inputs})
        loop = task(settings, "loop").for_each_task
        assert loop is not None
        assert loop.inputs == expected == escape(list(inputs))

    @pytest.mark.parametrize(
        ("template", "inputs", "error", "match"),
        [
            pytest.param(
                example_settings,
                {"extract": [1]},
                LakeflowError,
                r"Task 'extract' in for_each_inputs has no for_each_task. For-each tasks: \[\]",
                id="not-for-each",
            ),
            pytest.param(
                for_each_settings,
                {"nope": [1]},
                LakeflowError,
                r"Unknown task 'nope' in for_each_inputs. For-each tasks: \['loop'\]",
                id="unknown",
            ),
            pytest.param(
                for_each_settings,
                {"loop": ["a"], "after": [1]},
                LakeflowError,
                r"Task 'after' in for_each_inputs has no for_each_task. For-each tasks: \['loop'\]",
                id="checked-before-any-change",
            ),
            pytest.param(
                for_each_settings,
                {"loop": [object()]},
                ValueError,
                r"Inputs for task 'loop' in for_each_inputs are not JSON-serialisable: Object of type object",
                id="not-serialisable",
            ),
            pytest.param(
                for_each_settings,
                {"loop": "ab"},
                ValueError,
                r"for_each_inputs\['loop'\] must be a list, got 'ab'",
                id="string",
            ),
            pytest.param(
                for_each_settings,
                {"loop": ["x" * 4997]},
                ValueError,
                r"Inputs for task 'loop' in for_each_inputs are 5001 characters as JSON. A for-each task allows 5000.",
                id="too-long",
            ),
            pytest.param(
                for_each_settings,
                {"loop": ["a", None]},
                ValueError,
                r"Inputs for task 'loop' in for_each_inputs hold None at index 1. jobs/create takes a list of all "
                r"primitives or all objects, without null or nested lists.",
                id="null",
            ),
            pytest.param(
                for_each_settings,
                {"loop": [[1, 2]]},
                ValueError,
                r"Inputs for task 'loop' in for_each_inputs hold a nested list at index 0. jobs/create takes",
                id="nested-list",
            ),
            pytest.param(
                for_each_settings,
                {"loop": [1, {"k": 1}]},
                ValueError,
                r"Inputs for task 'loop' in for_each_inputs hold an object at index 1 after a primitive at index 0. "
                r"jobs/create takes",
                id="object-after-primitive",
            ),
            pytest.param(
                for_each_settings,
                {"loop": [{"k": 1}, "a"]},
                ValueError,
                r"Inputs for task 'loop' in for_each_inputs hold a primitive at index 1 after an object at index 0.",
                id="primitive-after-object",
            ),
        ],
    )
    def test_for_each_inputs_rejects(
        self, template: Callable[[], jobs.JobSettings], inputs: dict[str, Any], error: type[Exception], match: str
    ) -> None:
        settings = template()
        with pytest.raises(error, match=match):
            override_for_each_inputs(settings, inputs)
        assert settings == template()


class TestLiftNestedTask:
    """lift_nested_task puts a for-each task's body in its place, and for_each_bodies lists the bodies."""

    def test_lift_replaces_the_loop_in_place(self) -> None:
        template = fanout_objects_settings()
        settings = fanout_objects_settings()
        lifted = lift_nested_task(settings, "loop", "inner")
        assert [t.task_key for t in settings.tasks or []] == ["extract", "inner", "after"]
        assert lifted is task(settings, "inner")
        # The body keeps everything of its own and takes the loop's upstream tasks, which upstream_from reads.
        assert lifted == replace(nested_task(template, "loop"), depends_on=[jobs.TaskDependency(task_key="extract")])
        assert "for_each_task" not in json.dumps(settings.as_dict())
        assert task(settings, "after") == task(template, "after")

    @pytest.mark.parametrize(
        ("loop_key", "nested_key", "match"),
        [
            pytest.param(
                "extract",
                "x",
                r"Task 'extract' has no for_each_task, so 'extract.x' names no nested task. For-each bodies: "
                r"\['loop.inner'\]",
                id="not-for-each",
            ),
            pytest.param(
                "loop",
                "nope",
                r"for_each_task 'loop' nests 'inner', not 'nope'. For-each bodies: \['loop.inner'\]",
                id="other-body",
            ),
            pytest.param(
                "nope",
                "inner",
                r"No task 'nope' in the job. Available: \['after', 'extract', 'loop'\]",
                id="unknown-loop",
            ),
        ],
    )
    def test_lift_rejects(self, loop_key: str, nested_key: str, match: str) -> None:
        settings = fanout_objects_settings()
        with pytest.raises(LakeflowError, match=match):
            lift_nested_task(settings, loop_key, nested_key)
        assert settings == fanout_objects_settings()

    @pytest.mark.parametrize(
        ("template", "expected"),
        [
            pytest.param(fanout_objects_settings, {"loop": "inner"}, id="objects"),
            pytest.param(for_each_settings, {"loop": "each"}, id="strings"),
            pytest.param(example_settings, {}, id="no-loops"),
        ],
    )
    def test_for_each_bodies(self, template: Callable[[], jobs.JobSettings], expected: dict[str, str]) -> None:
        assert for_each_bodies(template()) == expected


def lifted_each(**parameters: str) -> jobs.JobSettings:
    """Lift `each` out of `for_each_settings()`, giving it `parameters` as its base parameters when any are given."""
    settings = for_each_settings()
    each = lift_nested_task(settings, "loop", "each")
    if parameters:
        assert each.notebook_task is not None
        each.notebook_task.base_parameters = parameters
    return settings


def base_parameters(settings: jobs.JobSettings, key: str) -> dict[str, str] | None:
    task = next(t for t in settings.tasks or [] if t.task_key == key)
    return task.notebook_task.base_parameters if task.notebook_task else None


class TestInjectInput:
    """inject_input points the lifted body's {{input}} references at job parameters that hold the chosen input."""

    def test_scalar_and_object(self) -> None:
        settings = inject_input(lifted_each(), "each", "a")
        assert base_parameters(settings, "each") == {"item": "{{job.parameters.__pytest_input}}"}
        assert settings.parameters == [jobs.JobParameterDefinition(name="__pytest_input", default="a")]

        settings = fanout_objects_settings()
        lift_nested_task(settings, "loop", "inner")
        inject_input(settings, "inner", {"id": 7, "path": "/x"})
        assert base_parameters(settings, "inner") == {
            "item": "{{job.parameters.__pytest_input}}",
            "item_id": "{{job.parameters.__pytest_input_id}}",
            "rows": "{{tasks.extract.values.rows}}",
        }
        # A for-each iteration over [{"id": 7, "path": "/x"}] gives these two texts, the id as a double.
        assert settings.parameters == [
            jobs.JobParameterDefinition(name="__pytest_input", default='{"id":7.0,"path":"/x"}'),
            jobs.JobParameterDefinition(name="__pytest_input_id", default="7.0"),
        ]

    # What for-each iterations gave {{input}} and {{input.<key>}} in the workspace, items of primitives and of objects.
    @pytest.mark.parametrize(
        ("reference", "value", "expected"),
        [
            pytest.param("{{input}}", "t {x}", "t {x}", id="string"),
            pytest.param("{{input}}", 1, "1", id="integer"),
            pytest.param("{{input}}", 12345678901234567890, "12345678901234567890", id="big-integer"),
            pytest.param("{{input}}", 2.5, "2.5", id="float"),
            pytest.param("{{input}}", 1e20, "1.0E20", id="large-float"),
            pytest.param("{{input}}", -0.0, "-0.0", id="negative-zero"),
            pytest.param("{{input}}", True, "true", id="boolean"),
            pytest.param(
                "{{input}}",
                {"e": {"f": 1, "g": "x"}, "u": "é", "neg": -3, "c": None, "d": [1, 2]},
                '{"e":{"f":1.0,"g":"x"},"u":"é","neg":-3.0,"c":null,"d":[1.0,2.0]}',
                id="object",
            ),
            pytest.param("{{ input.k }}", {"k": "é"}, "é", id="key-string"),
            pytest.param("{{input.k}}", {"k": 10}, "10.0", id="key-integer"),
            pytest.param("{{input.k}}", {"k": 0.1}, "0.1", id="key-float"),
            pytest.param("{{input.k}}", {"k": 12345678901234567890}, "1.2345678901234567E19", id="key-big-integer"),
            pytest.param("{{input.k}}", {"k": 12345678}, "1.2345678E7", id="key-seven-digits"),
            pytest.param("{{input.k}}", {"k": 0.00001}, "1.0E-5", id="key-small-float"),
            pytest.param("{{input.k}}", {"k": False}, "false", id="key-boolean"),
            pytest.param("{{input.k}}", {"k": None}, "null", id="key-null"),
            pytest.param("{{input.k}}", {"k": []}, "[]", id="key-list"),
            pytest.param("{{input.k}}", {"k": {"g": "y", "f": 2}}, '{"g":"y","f":2.0}', id="key-object"),
        ],
    )
    def test_values_render_as_an_iteration_gives_them(self, reference: str, value: object, expected: str) -> None:
        settings = inject_input(lifted_each(item=reference), "each", value)
        [definition] = settings.parameters or []
        assert base_parameters(settings, "each") == {"item": f"{{{{job.parameters.{definition.name}}}}}"}
        assert definition.default == expected

    def test_a_template_definition_is_replaced(self) -> None:
        settings = lifted_each(a="{{input.a}}", c="on_{{input.c}}_{{input}}")
        settings.parameters = [
            jobs.JobParameterDefinition(name="__pytest_input_c", default="old"),
            jobs.JobParameterDefinition(name="day", default=""),
        ]
        inject_input(settings, "each", {"a": "x", "c": "y"})
        assert base_parameters(settings, "each") == {
            "a": "{{job.parameters.__pytest_input_a}}",
            "c": "on_{{job.parameters.__pytest_input_c}}_{{job.parameters.__pytest_input}}",
        }
        assert settings.parameters == [
            jobs.JobParameterDefinition(name="day", default=""),
            jobs.JobParameterDefinition(name="__pytest_input", default='{"a":"x","c":"y"}'),
            jobs.JobParameterDefinition(name="__pytest_input_a", default="x"),
            jobs.JobParameterDefinition(name="__pytest_input_c", default="y"),
        ]

    @pytest.mark.parametrize(
        ("parameters", "value", "error", "match"),
        [
            pytest.param(
                {"item": "{{input}}", "item_id": "{{input.id}}"},
                None,
                LakeflowError,
                r"Task 'each' refers to \{\{input\}\} and \{\{input.id\}\} in its settings. Pass input= to "
                r"lakeflow.task\(\)",
                id="required",
            ),
            pytest.param(
                {"other": "x"},
                "a",
                LakeflowError,
                r"Task 'each' does not refer to \{\{input\}\}, so input= has no effect",
                id="no-references",
            ),
            pytest.param(
                {"item": "{{input}}"},
                [1, 2],
                ValueError,
                r"input for task 'each' is \[1, 2\]. A for-each item is a string, number, boolean or object.",
                id="list",
            ),
            pytest.param(
                {"item_id": "{{input.id}}"},
                {"path": "/x"},
                LakeflowError,
                r"\{\{input.id\}\} in task 'each' needs key 'id', but input has \['path'\]",
                id="missing-key",
            ),
            pytest.param(
                {"item_id": "{{input.id}}"},
                "a",
                LakeflowError,
                r"\{\{input.id\}\} in task 'each' needs an object as input, got 'a'",
                id="not-an-object",
            ),
            pytest.param(
                {"item": "{{input}}"},
                {"k": "{{job.id}}"},
                ValueError,
                r"input for task 'each' contains '\{\{', which the platform would substitute",
                id="braces",
            ),
            pytest.param(
                {"item": "{{input}}"},
                {"k": object()},
                ValueError,
                r"input for task 'each' is not JSON-serialisable: Object of type object",
                id="not-serialisable",
            ),
            pytest.param(
                {"item": "{{input}}"},
                float("nan"),
                ValueError,
                r"input for task 'each' is not JSON-serialisable: Out of range float values",
                id="nan",
            ),
        ],
    )
    def test_rejects(self, parameters: dict[str, str], value: object, error: type[Exception], match: str) -> None:
        settings = lifted_each(**parameters)
        before = copy.deepcopy(settings)
        with pytest.raises(error, match=match):
            inject_input(settings, "each", value)
        assert settings == before

    @pytest.mark.parametrize(
        ("template", "length", "message"),
        [
            pytest.param([], 9_955, None, id="fits"),
            pytest.param(
                [],
                9_956,
                r"input for task 'each' makes the job parameters 10001 characters as JSON. jobs/create allows 10000.",
                id="too-long",
            ),
            # A template parameter counts too, with its default JSON-escaped.
            pytest.param(
                [jobs.JobParameterDefinition(name="day", default='"')],
                9_955,
                "makes the job parameters 10035 characters as JSON",
                id="with-template-parameter",
            ),
        ],
    )
    def test_the_job_parameters_fit_jobs_create(
        self, template: list[jobs.JobParameterDefinition], length: int, message: str | None
    ) -> None:
        settings = lifted_each(item="{{input}}")
        settings.parameters = template
        inject_input(settings, "each", "x" * length)
        if message is None:
            check_input_size(settings, "each")
            return
        with pytest.raises(LakeflowError, match=message):
            check_input_size(settings, "each")

    def test_no_change_without_references_or_input(self) -> None:
        settings = lifted_each(other="x")
        before = copy.deepcopy(settings)
        assert inject_input(settings, "each", None) == before


@pytest.mark.usefixtures("fake_cli", "bundle_dir")
class TestForEachInputsEndToEnd:
    """Replacing the input list of the copy's for-each tasks."""

    @pytest.mark.parametrize(
        ("arguments", "inputs"),
        [
            pytest.param(', for_each_inputs={"loop": ["a", "b"]}', '["a", "b"]', id="override"),
            pytest.param("", '["a", "b", "c"]', id="as-deployed"),
        ],
    )
    def test_for_each_inputs_reach_the_created_job(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, arguments: str, inputs: str
    ) -> None:
        fake_cli.deployed["fanout"] = fake_ws.jobs.add(for_each_settings())
        fake_ws.jobs.queue(make_run(81, {"loop": "SUCCESS", "after": "SUCCESS"}))
        source = f'def test_job(lakeflow):\n    with lakeflow.job("fanout"{arguments}) as job:\n        job.run()\n'
        run_tests(pytester, source).assert_outcomes(passed=1)
        [created] = fake_ws.jobs.created
        tasks = {t.task_key: t for t in created["tasks"]}
        template = {t.task_key: t for t in for_each_settings().tasks or []}
        assert tasks["loop"].for_each_task.inputs == inputs
        assert tasks["loop"].for_each_task.task.task_key == "each"
        assert tasks["after"].depends_on == [jobs.TaskDependency(task_key="loop")]
        assert tasks["after"].notebook_task == template["after"].notebook_task

    @pytest.mark.parametrize(
        ("arguments", "message"),
        [
            pytest.param(
                'for_each_inputs={"nope": []}',
                "*Unknown task 'nope' in for_each_inputs. For-each tasks: ?'loop'?*",
                id="unknown",
            ),
            pytest.param(
                'for_each_inputs={"loop": ["a"]}, upstream_task_values={"loop": {"k": "v"}}',
                "*Task 'loop' in for_each_inputs is stubbed by upstream_task_values, so it has no for_each_task. "
                "For-each tasks: ??*",
                id="stubbed",
            ),
        ],
    )
    def test_for_each_inputs_fail_before_creation(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, arguments: str, message: str
    ) -> None:
        fake_cli.deployed["fanout"] = fake_ws.jobs.add(for_each_settings())
        source = f'def test_job(lakeflow):\n    lakeflow.job("fanout", {arguments})\n'
        result = run_tests(pytester, source)
        result.assert_outcomes(failed=1)
        result.stdout.fnmatch_lines([message])
        assert fake_ws.jobs.created == []


@pytest.mark.usefixtures("fake_cli", "bundle_dir")
class TestForEachEndToEnd:
    """For-each inputs, lifted bodies and their inputs reach the created job and come back on the run."""

    def test_for_each_inputs_on_a_task_job(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli
    ) -> None:
        fake_cli.deployed["fanout"] = fake_ws.jobs.add(for_each_settings())
        fake_ws.jobs.queue(make_run(80, {"loop": "SUCCESS"}))
        pytester.makepyfile(
            """
            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            def test_task(lakeflow):
                with lakeflow.task("fanout.loop", for_each_inputs=["a"]) as task:
                    assert task.run().succeeded

            def test_not_for_each(lakeflow):
                match = r"Task 'transform' in for_each_inputs has no for_each_task. For-each tasks: \\[\\]"
                with pytest.raises(LakeflowError, match=match):
                    lakeflow.task("my_job.transform", upstream_task_values=UPSTREAM, for_each_inputs=["a"])

            UPSTREAM = {"extract": {"row_count": 1}}
            """
        )
        pytester.runpytest().assert_outcomes(passed=2)
        [created] = fake_ws.jobs.created
        assert [t.task_key for t in created["tasks"]] == ["loop"]
        assert created["tasks"][0].for_each_task.inputs == '["a"]'

    def test_nested_task_end_to_end(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli
    ) -> None:
        fake_cli.deployed["fanout"] = fake_ws.jobs.add(fanout_objects_settings())
        fake_cli.deployed["strings"] = fake_ws.jobs.add(for_each_settings())
        fake_ws.jobs.queue(make_run(82, {"extract": "SUCCESS", "inner": "SUCCESS"}))
        pytester.makepyfile(
            """
            def test_body(lakeflow):
                with lakeflow.task(
                    "fanout.loop.inner",
                    input={"id": 7, "path": "/x"},
                    upstream_task_values={"extract": {"rows": 3}},
                    task_params={"inner": {"limit": "10"}},
                ) as task:
                    run = task.run()
                assert run.succeeded
                assert run.executed_tasks == ["inner"]

            def test_string_body(lakeflow):
                with lakeflow.task("strings.loop.each", input="a"):
                    pass
            """
        )
        pytester.runpytest().assert_outcomes(passed=2)
        body, strings = fake_ws.jobs.created
        assert [t.task_key for t in body["tasks"]] == ["extract", "inner"]
        assert "for_each_task" not in json.dumps(jobs.JobSettings(tasks=body["tasks"]).as_dict())
        inner = body["tasks"][1]
        assert inner.depends_on == [jobs.TaskDependency(task_key="extract")]
        assert inner.timeout_seconds == 600
        assert inner.notebook_task.base_parameters == {
            "item": "{{job.parameters.__pytest_input}}",
            "item_id": "{{job.parameters.__pytest_input_id}}",
            "rows": "{{tasks.extract.values.rows}}",
            "limit": "10",
        }
        assert body["parameters"] == [
            jobs.JobParameterDefinition(name="__pytest_input", default='{"id":7.0,"path":"/x"}'),
            jobs.JobParameterDefinition(name="__pytest_input_id", default="7.0"),
        ]
        assert [t.task_key for t in strings["tasks"]] == ["each"]
        assert strings["tasks"][0].notebook_task.notebook_path.endswith("/each")
        assert strings["parameters"][-1] == jobs.JobParameterDefinition(name="__pytest_input", default="a")

    def test_nested_task_rejects(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli
    ) -> None:
        fake_cli.deployed["fanout"] = fake_ws.jobs.add(fanout_objects_settings())
        pytester.makepyfile(
            """
            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            UPSTREAM = {"extract": {"rows": 3}}

            @pytest.mark.parametrize(
                ("ref", "kwargs", "error", "match"),
                [
                    ("fanout.loop.nope", {"input": "a"}, LakeflowError,
                        r"for_each_task 'loop' nests 'inner', not 'nope'"),
                    ("fanout.extract.x", {"input": "a"}, LakeflowError, r"Task 'extract' has no for_each_task"),
                    ("fanout.loop", {"input": "a"}, ValueError,
                        r"input= gives the iteration input of a for_each_task body, so "
                        r"ref must be 'job_key.loop_key.nested_key', got 'fanout.loop'"),
                    ("a.b.c.d", {}, ValueError,
                        r"Expected 'job_key.task_key' or 'job_key.loop_key.nested_key', got 'a.b.c.d'"),
                    ("fanout..inner", {}, ValueError, r"Expected 'job_key.task_key' or 'job_key.loop_key.nested_key'"),
                    ("fanout.loop.inner", {}, LakeflowError,
                        r"Task 'inner' refers to \\{\\{input\\}\\} and \\{\\{input.id\\}\\}"),
                    ("fanout.loop.inner", {"input": {"path": "/x"}}, LakeflowError,
                        r"needs key 'id', but input has \\['path'\\]"),
                    ("fanout.loop.inner", {"input": "a", "task_params": {"loop": {"k": "v"}}}, LakeflowError,
                        r"Unknown task 'loop' in task_params of job 'fanout'. Available: \\['inner'\\]"),
                ],
            )
            def test_rejects(lakeflow, ref, kwargs, error, match):
                with pytest.raises(error, match=match):
                    lakeflow.task(ref, upstream_task_values=UPSTREAM, **kwargs)
            """
        )
        pytester.runpytest().assert_outcomes(passed=8)
        assert fake_ws.jobs.created == []

    def test_for_each_loops_end_to_end(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli
    ) -> None:
        fake_cli.deployed["fanout"] = fake_ws.jobs.add(fanout_objects_settings())
        fake_ws.jobs.queue(make_run(83, {"extract": "SUCCESS", "loop": "SUCCESS", "after": "SUCCESS"}))
        pytester.makepyfile(
            """
            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            def test_task_params_reach_the_body(lakeflow):
                with lakeflow.job("fanout", task_params={"inner": {"limit": "10"}}) as job:
                    assert job.run().succeeded

            @pytest.mark.parametrize("ref", ["job", "task"])
            def test_capturing_a_loop_is_refused(lakeflow, ref):
                with pytest.raises(LakeflowError, match=r"the for-each task 'loop', whose iterations cannot set"):
                    if ref == "job":
                        lakeflow.job("fanout", capture_task_values={"loop": ["seen"]})
                    else:
                        lakeflow.task("fanout.loop", capture_task_values=["seen"], upstream_task_values=UPSTREAM)

            def test_capturing_a_lifted_body_warns(lakeflow):
                match = (
                    r"capture_task_values on 'fanout.loop.inner': 'inner' cannot set task values when it runs in "
                    r"its loop 'loop'"
                )
                with pytest.warns(UserWarning, match=match):
                    lakeflow.task("fanout.loop.inner", input={"id": 1}, upstream_task_values=UPSTREAM,
                                  capture_task_values=["rows"])

            UPSTREAM = {"extract": {"rows": 3}}
            """
        )
        pytester.runpytest().assert_outcomes(passed=4)
        [created] = fake_ws.jobs.created
        loop = next(t for t in created["tasks"] if t.task_key == "loop")
        assert loop.for_each_task.task.notebook_task.base_parameters == {
            "item": "{{input}}",
            "item_id": "{{input.id}}",
            "rows": "{{tasks.extract.values.rows}}",
            "limit": "10",
        }

    @pytest.mark.parametrize(
        ("arguments", "kwargs", "size"),
        [
            # 59 more: a comma, 29 for the definition, the name __pytest_trigger_type and the default one_time.
            pytest.param(
                "",
                'task_params={"each": {"t": "{{job.trigger.type}}"}}, trigger={"type": "one_time"}',
                10_059,
                id="trigger",
            ),
            # 101 more: a comma, 29 for the definition, the name __pytest_test_id and the 55-character node id.
            pytest.param("test_marker=True", "", 10_101, id="test_marker"),
        ],
    )
    def test_input_size_counts_every_job_parameter(
        self,
        pytester: pytest.Pytester,
        fake_ws: FakeWorkspaceClient,
        fake_cli: FakeCli,
        arguments: str,
        kwargs: str,
        size: int,
    ) -> None:
        """The input alone fits the 10,000 characters; the parameters added after it do not."""
        fake_cli.deployed["strings"] = fake_ws.jobs.add(for_each_settings())
        configure_lakeflow(pytester, arguments)
        pytester.makepyfile(
            f"""
            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            def test_job(lakeflow):
                match = "input for task 'each' makes the job parameters {size} characters as JSON"
                with pytest.raises(LakeflowError, match=match):
                    lakeflow.task("strings.loop.each", input="x" * 9_955, {kwargs})
            """
        )
        pytester.runpytest().assert_outcomes(passed=1)
        assert fake_ws.jobs.created == []
