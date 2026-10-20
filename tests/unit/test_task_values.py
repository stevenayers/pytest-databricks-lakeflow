from __future__ import annotations

import json
import re
from collections.abc import Callable, Collection, Mapping
from datetime import timedelta
from typing import Any

import pytest
from databricks.sdk.service import jobs

from pytest_databricks_lakeflow.errors import LakeflowError
from pytest_databricks_lakeflow.results import JobRun, TaskRun, TaskValues
from pytest_databricks_lakeflow.settings import (
    ENV_KEY,
    Helpers,
    Stub,
    add_dependencies,
    check_values,
    escape,
    inject_lifecycle,
    inject_probes,
    merge_environments,
    stub_upstream,
    upstream_values_from,
    wrap_task,
)

from .conftest import FakeCli, configure_lakeflow
from .fakes import FakeWorkspaceClient, example_settings, for_each_settings, make_run, recorded

HELPERS = Helpers(set_values="/h/set_values", get_values="/h/get_values")
UPSTREAM = {"extract": {"row_count": 42, "source_path": "/Volumes/x/y/z"}}


def keys(settings: jobs.JobSettings) -> list[str]:
    return [t.task_key for t in settings.tasks or []]


def environment_keys(settings: jobs.JobSettings) -> list[str]:
    return [e.environment_key for e in settings.environments or []]


def with_environment_count(count: int) -> jobs.JobSettings:
    settings = example_settings()
    extra = [jobs.JobEnvironment.from_dict({"environment_key": f"env{i}"}) for i in range(1, count)]
    settings.environments = [*(settings.environments or []), *extra]
    return settings


def probe_transform(settings: jobs.JobSettings) -> jobs.JobSettings:
    return inject_probes(settings, {"transform": ["x"]}, HELPERS)


def wrap_transform(settings: jobs.JobSettings) -> jobs.JobSettings:
    return wrap_task(settings, "transform", UPSTREAM, [], HELPERS)


def task_run(key: str, values: Mapping[str, object]) -> TaskRun:
    return TaskRun(
        task_key=key,
        run_id=1,
        result_state="SUCCESS",
        termination_code=None,
        termination_message=None,
        run_url=f"https://example.com/{key}",
        duration=timedelta(seconds=1),
        task_values=TaskValues(values),
    )


def previous_run(
    values: Mapping[str, Mapping[str, object]], job_key: str = "my_job", skipped: Collection[str] = ()
) -> JobRun:
    """Build a finished run whose tasks captured `values`, by task key, with the tasks in `skipped` left out by only."""
    return JobRun(
        job_key=job_key,
        job_id=1,
        run_id=2,
        run_url="https://example.com/run",
        result_state="SUCCESS",
        termination_code="SUCCESS",
        termination_message=None,
        duration=timedelta(seconds=3),
        tasks={key: task_run(key, task_values) for key, task_values in values.items()},
        injected_tasks={},
        skipped_tasks={key: task_run(key, {}) for key in skipped},
    )


def queue_capturing_run(fake_ws: FakeWorkspaceClient, values: Mapping[str, object], probed: str = "extract") -> None:
    """Queue a full run of my_job whose probe on the task `probed` captured `values`."""
    states = {"extract": "SUCCESS", "transform": "SUCCESS", "load": "SUCCESS", "classic": "SUCCESS"}
    run = make_run(5, {**states, f"__pytest_probe__{probed}": "SUCCESS"})
    probe_output = jobs.NotebookOutput(result=json.dumps({"values": dict(values), "missing": []}))
    fake_ws.jobs.queue(run, {504: jobs.RunOutput(notebook_output=probe_output)})


def with_target(target: Mapping[str, object]) -> jobs.JobSettings:
    """Build a job of `extract` and a task `target`, made of the fields in `target`, that depends on it."""
    return jobs.JobSettings.from_dict(
        {
            "name": "my_job",
            "tasks": [
                {"task_key": "extract", "notebook_task": {"notebook_path": "/src/extract"}},
                {"task_key": "target", "depends_on": [{"task_key": "extract"}], **target},
            ],
        }
    )


def transform_reading(parameters: Mapping[str, str]) -> jobs.JobSettings:
    """Build `example_settings()` with `parameters` added to the `transform` task's `base_parameters`."""
    settings = example_settings()
    transform = (settings.tasks or [])[1]
    assert transform.notebook_task is not None and transform.notebook_task.base_parameters is not None
    transform.notebook_task.base_parameters.update(parameters)
    return settings


def stub_values(created: Mapping[str, Any], key: str) -> str:
    tasks: list[jobs.Task] = created["tasks"]
    stub = next(t for t in tasks if t.task_key == key)
    assert stub.notebook_task is not None and stub.notebook_task.base_parameters is not None
    return stub.notebook_task.base_parameters["__pytest_values"]


class TestWrapTask:
    """`wrap_task` keeps one task, stubs its upstream task values and probes the values it sets."""

    def test_keeps_one_task_with_stubs_and_a_probe(self) -> None:
        settings = wrap_task(example_settings(), "transform", UPSTREAM, ["rows_written"], HELPERS)
        assert keys(settings) == ["extract", "transform", "__pytest_probe__transform"]
        stub, target, probe = settings.tasks or []
        assert stub.notebook_task == jobs.NotebookTask(
            notebook_path="/h/set_values", base_parameters={"__pytest_values": json.dumps(UPSTREAM["extract"])}
        )
        assert stub.environment_key == ENV_KEY
        assert target.depends_on == [jobs.TaskDependency(task_key="extract")]
        original = next(t for t in example_settings().tasks or [] if t.task_key == "transform")
        assert target.notebook_task == original.notebook_task
        assert probe.depends_on == [jobs.TaskDependency(task_key="transform")]
        assert probe.run_if == jobs.RunIf.ALL_DONE
        assert probe.notebook_task is not None
        assert probe.notebook_task.base_parameters == {
            "__pytest_task_key": "transform",
            "__pytest_keys": '["rows_written"]',
        }
        envs = {e.environment_key: e.spec for e in settings.environments or []}
        helper_env = envs[ENV_KEY]
        assert helper_env is not None and helper_env.environment_version == "4"
        assert "default" in envs

    def test_without_upstream_or_capture(self) -> None:
        settings = wrap_task(example_settings(), "load", {}, [], HELPERS)
        assert keys(settings) == ["load"]
        assert (settings.tasks or [])[0].depends_on is None
        assert ENV_KEY not in environment_keys(settings)

    @pytest.mark.parametrize(
        ("task_key", "upstream", "message"),
        [
            pytest.param(
                "transform",
                {},
                r"refers to task values of \['extract'\].*Pass them in upstream_task_values",
                id="missing_stubs",
            ),
            pytest.param("transfrom", {}, r"No task 'transfrom' in the job. Available: \['classic'", id="unknown_task"),
            pytest.param("load", {"load": {}}, "cannot stub 'load', the task under test", id="self_stub"),
        ],
    )
    def test_rejects(self, task_key: str, upstream: dict[str, dict[str, object]], message: str) -> None:
        with pytest.raises(LakeflowError, match=message):
            wrap_task(example_settings(), task_key, upstream, [], HELPERS)

    def test_rejects_any_reference_to_a_task_left_out(self) -> None:
        settings = example_settings()
        transform = (settings.tasks or [])[1]
        assert transform.notebook_task is not None
        transform.notebook_task.base_parameters = {"upstream": "{{tasks.extract.result_state}}"}
        message = (
            "Task 'transform' refers to ['extract'] in its settings, which task() leaves out of the job, and "
            "jobs/create refuses a reference to a task that is not in the job. Pass them in upstream_task_values."
        )
        with pytest.raises(LakeflowError, match=re.escape(message)):
            wrap_task(settings, "transform", {}, [], HELPERS)


FIRST_ROW = "{{tasks.extract.output.first_row.n}}"


class TestStubOutputs:
    """A `Stub` answers the target's `{{tasks.<key>.output.<path>}}` references with literals."""

    def test_rewrites_output_references(self) -> None:
        settings = transform_reading(
            {
                "n": FIRST_ROW,
                "msg": 'got {{ tasks.extract.output.alert_state }} "ok"',
                "rows": "{{tasks.extract.output.rows}}",
            }
        )
        outputs = {"first_row.n": 3, "alert_state": "OK", "rows": [{"n": 3, "s": "a\\b", "u": "ü"}]}
        wrap_task(settings, "transform", {"extract": Stub(outputs=outputs)}, [], HELPERS)
        assert keys(settings) == ["extract", "transform"]
        stub, target = settings.tasks or []
        assert target.notebook_task is not None
        assert target.notebook_task.base_parameters == {
            "row_count": "{{tasks.extract.values.row_count}}",
            "n": "3",
            "msg": 'got OK "ok"',
            "rows": '[{"n":3,"s":"a\\\\b","u":"ü"}]',
        }
        assert target.depends_on == [jobs.TaskDependency(task_key="extract")]
        assert stub.notebook_task is not None and stub.notebook_task.base_parameters is not None
        assert stub.notebook_task.base_parameters["__pytest_values"] == json.dumps({})

    @pytest.mark.parametrize(
        "target",
        [
            pytest.param({"condition_task": {"op": "EQUAL_TO", "left": FIRST_ROW, "right": "3"}}, id="condition"),
            pytest.param(
                {"sql_task": {"warehouse_id": "w1", "file": {"path": "/q.sql"}, "parameters": {"n": FIRST_ROW}}},
                id="sql",
            ),
            pytest.param({"run_job_task": {"job_id": 1, "job_parameters": {"n": FIRST_ROW}}}, id="run_job"),
            pytest.param(
                {"python_wheel_task": {"package_name": "pkg", "entry_point": "main", "parameters": ["--n", FIRST_ROW]}},
                id="wheel_list",
            ),
            pytest.param(
                {
                    "for_each_task": {
                        "inputs": f"[{FIRST_ROW}]",
                        "task": {"task_key": "inner", "notebook_task": {"notebook_path": "/src/inner"}},
                    }
                },
                id="for_each",
            ),
        ],
    )
    def test_rewrites_every_field(self, target: dict[str, object]) -> None:
        settings = wrap_task(
            with_target(target), "target", {"extract": Stub(outputs={"first_row.n": "3"})}, [], HELPERS
        )
        literal = json.loads(json.dumps(target).replace(FIRST_ROW, "3"))
        expected = wrap_task(with_target(literal), "target", {"extract": {}}, [], HELPERS)
        assert settings.tasks == expected.tasks
        assert FIRST_ROW not in json.dumps(settings.as_dict())

    @pytest.mark.parametrize(
        "stub",
        [pytest.param({}, id="plain_mapping"), pytest.param(Stub(outputs={"alert_state": "OK"}), id="other_path")],
    )
    def test_rejects_unstubbed_output_paths(self, stub: Mapping[str, object] | Stub) -> None:
        settings = transform_reading({"n": FIRST_ROW})
        message = (
            r"Task 'transform' refers to \{\{tasks.extract.output.first_row.n\}\}, but the stub for 'extract' has "
            r"no output 'first_row.n'. Pass Stub\(outputs=\{'first_row.n': ...\}\) for 'extract' in "
            r"upstream_task_values."
        )
        with pytest.raises(LakeflowError, match=message):
            wrap_task(settings, "transform", {"extract": stub}, [], HELPERS)
        assert settings == transform_reading({"n": FIRST_ROW}), "nothing is changed when a reference is rejected"

    @pytest.mark.parametrize(
        ("value", "message"),
        [
            pytest.param(
                "{{job.id}}",
                r"Output 'first_row.n' of stub 'extract' contains '\{\{', which the platform would resolve as a "
                r"reference: '\{\{job.id\}\}'. Stubbed outputs are literals.",
                id="string",
            ),
            pytest.param(["{{x}}"], r"contains '\{\{'.*'\[\"\{\{x\}\}\"\]'", id="json"),
            pytest.param(object(), r"Output 'first_row.n' of stub 'extract' is not JSON-serialisable", id="not_json"),
        ],
    )
    def test_rejects_references_in_outputs(self, value: object, message: str) -> None:
        settings = transform_reading({"n": FIRST_ROW})
        with pytest.raises(LakeflowError, match=message):
            wrap_task(settings, "transform", {"extract": Stub(outputs={"first_row.n": value})}, [], HELPERS)
        with pytest.raises(LakeflowError, match=message):
            stub_upstream(example_settings(), {"extract": Stub(outputs={"first_row.n": value})}, HELPERS)

    def test_values_and_outputs_together(self) -> None:
        stub = Stub(values={"row_count": 42}, outputs={"first_row.n": "3"})
        settings = wrap_task(transform_reading({"n": FIRST_ROW}), "transform", {"extract": stub}, [], HELPERS)
        plain = wrap_task(example_settings(), "transform", {"extract": {"row_count": 42}}, [], HELPERS)
        stub_task, target = settings.tasks or []
        assert stub_task == (plain.tasks or [])[0], "a Stub sets its values exactly as a plain mapping does"
        assert stub_task.notebook_task is not None and stub_task.notebook_task.base_parameters is not None
        assert stub_task.notebook_task.base_parameters["__pytest_values"] == json.dumps({"row_count": 42})
        assert target.notebook_task is not None and target.notebook_task.base_parameters is not None
        assert target.notebook_task.base_parameters["n"] == "3"

    def test_leaves_a_target_without_output_references_alone(self) -> None:
        settings = example_settings()
        target = (settings.tasks or [])[1]
        wrap_task(
            settings, "transform", {"extract": Stub(values={"row_count": 1}, outputs={"rows": "[]"})}, [], HELPERS
        )
        assert (settings.tasks or [])[1] is target

    def test_stub_upstream_rewrites_the_tasks_downstream(self) -> None:
        settings = transform_reading({"n": FIRST_ROW, "state": "{{tasks.load.output.alert_state}}"})
        stub_upstream(settings, {"extract": Stub(values={"row_count": 1}, outputs={"first_row.n": "3"})}, HELPERS)
        stub, transform = (settings.tasks or [])[:2]
        assert stub.notebook_task is not None and stub.notebook_task.base_parameters is not None
        assert stub.notebook_task.base_parameters["__pytest_values"] == escape({"row_count": 1})
        assert transform.notebook_task is not None
        assert transform.notebook_task.base_parameters == {
            "row_count": "{{tasks.extract.values.row_count}}",
            "n": "3",
            "state": "{{tasks.load.output.alert_state}}",
        }, "a reference to a task that is not stubbed is left alone"
        assert transform.depends_on == [jobs.TaskDependency(task_key="extract")]

    def test_stub_upstream_rejects_unstubbed_output_paths(self) -> None:
        settings = transform_reading({"n": FIRST_ROW})
        with pytest.raises(LakeflowError, match=r"Task 'transform' refers to .*has no output 'first_row.n'"):
            stub_upstream(settings, {"extract": {"row_count": 1}}, HELPERS)
        assert settings == transform_reading({"n": FIRST_ROW}), "nothing is changed when a reference is rejected"


class TestStubUpstream:
    """`stub_upstream` replaces the named tasks of a whole job with stubs, in place."""

    def test_stub_upstream_replaces_tasks_in_place(self) -> None:
        settings = stub_upstream(example_settings(), UPSTREAM, HELPERS)
        assert keys(settings) == ["extract", "transform", "load", "classic"]
        stub, transform = (settings.tasks or [])[:2]
        assert stub.notebook_task == jobs.NotebookTask(
            notebook_path="/h/set_values", base_parameters={"__pytest_values": escape(UPSTREAM["extract"])}
        )
        assert stub.environment_key == ENV_KEY
        assert stub.depends_on is None
        assert transform.depends_on == [jobs.TaskDependency(task_key="extract")]
        assert ENV_KEY in environment_keys(settings)
        assert stub_upstream(example_settings(), {}, HELPERS) == example_settings()

    def test_stub_upstream_drops_depends_on_but_keeps_the_setup_task(self) -> None:
        settings = inject_lifecycle(example_settings(), "/h/setup_1", None, HELPERS)
        stub_upstream(settings, {"transform": {"stage": "done"}}, HELPERS)
        stub = next(t for t in settings.tasks or [] if t.task_key == "transform")
        assert stub.depends_on == [jobs.TaskDependency(task_key="__pytest_setup")]

    @pytest.mark.parametrize(
        ("upstream", "message"),
        [
            pytest.param(
                {"extrct": {"row_count": 1}},
                r"Unknown task 'extrct' in upstream_task_values. Available: \['classic', 'extract', 'load'",
                id="unknown_task",
            ),
            pytest.param(
                {"extract": {"row_count": "x" * 49_153}},
                "'row_count' for task 'extract' is 49155 characters as JSON",
                id="value_too_large",
            ),
        ],
    )
    def test_stub_upstream_rejects_unknown_tasks(self, upstream: dict[str, dict[str, object]], message: str) -> None:
        settings = example_settings()
        with pytest.raises(LakeflowError, match=message):
            stub_upstream(settings, {"load": {"rows": 1}, **upstream}, HELPERS)
        assert settings == example_settings(), "nothing is changed when an entry is rejected"


class TestInjectProbes:
    """`inject_probes` appends one probe task per captured task and one helper environment."""

    def test_inject_probes(self) -> None:
        settings = inject_probes(example_settings(), {"transform": ["stage"], "load": ["rows"]}, HELPERS)
        assert keys(settings) == [
            "extract",
            "transform",
            "load",
            "classic",
            "__pytest_probe__transform",
            "__pytest_probe__load",
        ]
        assert environment_keys(settings).count(ENV_KEY) == 1
        assert inject_probes(example_settings(), {}, HELPERS) == example_settings()
        with pytest.raises(LakeflowError, match=r"not in the job: \['nope'\]"):
            inject_probes(example_settings(), {"nope": ["k"]}, HELPERS)


class TestHelperCompute:
    """Stub and probe tasks run on the helper environment or on the `helpers_compute` job cluster."""

    def test_helpers_on_a_job_cluster(self) -> None:
        helpers = Helpers(set_values="/s", get_values="/g", job_cluster_key="classic")
        settings = wrap_task(example_settings(), "transform", UPSTREAM, ["k"], helpers)
        assert all(t.job_cluster_key == "classic" for t in settings.tasks or [] if t.task_key != "transform")
        assert ENV_KEY not in environment_keys(settings)
        with pytest.raises(LakeflowError, match=r"^helpers_compute is 'gpu'.*Available: \['classic'\]"):
            wrap_task(example_settings(), "transform", UPSTREAM, [], Helpers("/s", "/g", job_cluster_key="gpu"))

    @pytest.mark.parametrize(
        "inject", [pytest.param(probe_transform, id="inject_probes"), pytest.param(wrap_transform, id="wrap_task")]
    )
    def test_extra_dependencies_do_not_reach_the_helper_environment(
        self, inject: Callable[[jobs.JobSettings], jobs.JobSettings]
    ) -> None:
        settings = inject(add_dependencies(example_settings(), ["pytest>=8"]))
        specs = {e.environment_key: e.spec for e in settings.environments or []}
        default, helper = specs["default"], specs[ENV_KEY]
        assert default is not None and default.dependencies == ["pytest>=8"]
        assert helper is not None and helper.dependencies is None

    def test_helper_environment_version(self) -> None:
        settings = inject_probes(example_settings(), {"load": ["k"]}, Helpers("/s", "/g", environment_version="6"))
        env = next(e for e in settings.environments or [] if e.environment_key == ENV_KEY)
        assert env.spec is not None and env.spec.environment_version == "6"

    @pytest.mark.parametrize(
        "inject", [pytest.param(probe_transform, id="inject_probes"), pytest.param(wrap_transform, id="wrap_task")]
    )
    def test_helpers_refuse_an_eleventh_environment(
        self, inject: Callable[[jobs.JobSettings], jobs.JobSettings]
    ) -> None:
        with pytest.raises(LakeflowError, match=r"10 environments.*'default', 'env1'.*'env9'.*make room") as e:
            inject(with_environment_count(10))
        assert ENV_KEY in str(e.value)
        settings = inject(with_environment_count(9))
        assert len(settings.environments or []) == 10
        assert environment_keys(settings)[-1] == ENV_KEY


class TestTaskValues:
    """Stubbed task values are escaped against reference substitution and checked against the platform limits."""

    def test_escape_stops_reference_substitution(self) -> None:
        values = {"a": "{{job.id}}", "b": "{{tasks.x.values.k}}", "c": "x{{{y", "d": ["{{", {"e": "}}"}]}
        escaped = escape(values)
        assert "{{" not in escaped
        assert json.loads(escaped) == values

    def test_check_values(self) -> None:
        check_values("extract", {"ok": "x" * 49_000, "n": None})
        with pytest.raises(LakeflowError, match="'big' for task 'extract' is 49155 characters as JSON"):
            check_values("extract", {"big": "x" * 49_153})
        with pytest.raises(LakeflowError, match="not JSON-serialisable"):
            check_values("extract", {"bad": object()})
        with pytest.raises(LakeflowError, match="longer than 512"):
            check_values("extract", {"k" * 513: 1})
        with pytest.raises(LakeflowError, match="longer than 512"):
            inject_probes(example_settings(), {"load": ["k" * 513]}, HELPERS)


class TestUpstreamFrom:
    """`upstream_values_from` takes the stub values for a task's upstream tasks from a run that captured them."""

    def test_upstream_values_from_a_previous_run(self) -> None:
        run = previous_run({"extract": {"row_count": 42}, "load": {"rows": 1}})
        assert upstream_values_from("my_job", example_settings(), "transform", run) == {"extract": {"row_count": 42}}

    @pytest.mark.parametrize(
        ("run", "message"),
        [
            pytest.param(
                previous_run({"extract": {}}),
                r"upstream_from has no task values for 'extract'.*capture_task_values=\{'extract': \[",
                id="empty_values",
            ),
            pytest.param(
                previous_run({"transform": {}}, skipped=["extract"]),
                r"'extract'.*was not part of the run.*capture_task_values",
                id="left_out_by_only",
            ),
        ],
    )
    def test_upstream_from_needs_captured_values(self, run: JobRun, message: str) -> None:
        with pytest.raises(LakeflowError, match=message):
            upstream_values_from("my_job", example_settings(), "transform", run)

    def test_explicit_task_keys_are_not_read_from_the_run(self) -> None:
        run = previous_run({"extract": {}})
        assert upstream_values_from("my_job", example_settings(), "transform", run, explicit=["extract"]) == {}

    def test_upstream_from_rejects_another_job(self) -> None:
        run = previous_run({"extract": {"row_count": 42}}, job_key="other")
        with pytest.raises(LakeflowError, match=r"upstream_from is a run of job 'other', not of 'my_job'"):
            upstream_values_from("my_job", example_settings(), "transform", run)

    def test_replayed_values_go_through_check_values(self) -> None:
        run = previous_run({"extract": {"row_count": "x" * 49_153}})
        upstream = upstream_values_from("my_job", example_settings(), "transform", run)
        with pytest.raises(LakeflowError, match="'row_count' for task 'extract' is 49155 characters as JSON"):
            wrap_task(example_settings(), "transform", upstream, [], HELPERS)


@pytest.mark.usefixtures("fake_cli", "bundle_dir")
class TestEndToEnd:
    """`Lakeflow.task()` and `Lakeflow.job()` wire the stubs, probes and helper compute into the created job."""

    def test_single_task_end_to_end(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        fake_ws.jobs.queue(*recorded("probe_success"))
        pytester.makepyfile(
            """
            import pytest

            def test_task(lakeflow):
                with lakeflow.task(
                    "my_job.transform",
                    params={"param1": "hello"},
                    upstream_task_values={"extract": {"row_count": 42}},
                    capture_task_values=["stage", "rows_written"],
                ) as task:
                    run = task.run()
                assert run.succeeded
                assert sorted(run.tasks) == ["transform"]
                assert sorted(run.injected_tasks) == ["__pytest_probe__transform", "extract"]
                assert run.task("transform").task_values == {"stage": "done", "rows_written": 7}

            def test_bad_reference(lakeflow):
                with pytest.raises(KeyError, match="No job 'nope'"):
                    lakeflow.task("nope.transform")
                with pytest.raises(Exception, match="Expected 'job_key.task_key'"):
                    lakeflow.task("my_job")
            """
        )
        pytester.runpytest().assert_outcomes(passed=2)
        [created] = fake_ws.jobs.created
        assert [t.task_key for t in created["tasks"]] == ["extract", "transform", "__pytest_probe__transform"]
        assert all(t.disable_auto_optimization for t in created["tasks"])
        assert created["environments"][-1].environment_key == ENV_KEY

    def test_stubbed_outputs_end_to_end(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        fake_ws.jobs.queue(make_run(79, {"extract": "SUCCESS", "transform": "SUCCESS"}))
        pytester.makepyfile(
            """
            from pytest_databricks_lakeflow import Stub

            def test_task(lakeflow):
                with lakeflow.task(
                    "my_job.transform",
                    task_params={"transform": {"n": "{{tasks.extract.output.first_row.n}}"}},
                    upstream_task_values={"extract": Stub(values={"row_count": 42}, outputs={"first_row.n": "7"})},
                ) as task:
                    run = task.run()
                assert run.succeeded
                assert sorted(run.injected_tasks) == ["extract"]
            """
        )
        pytester.runpytest().assert_outcomes(passed=1)
        [created] = fake_ws.jobs.created
        assert [t.task_key for t in created["tasks"]] == ["extract", "transform"]
        assert created["tasks"][1].notebook_task.base_parameters == {
            "row_count": "{{tasks.extract.values.row_count}}",
            "n": "7",
        }
        assert stub_values(created, "extract") == escape({"row_count": 42})

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

    def test_job_capture_task_values_end_to_end(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        fake_ws.jobs.queue(*recorded("probe_failure"))
        pytester.makepyfile(
            """
            def test_job(lakeflow):
                captured = {"transform": ["stage"]}
                with lakeflow.job("my_job", params={"param1": "explode"}, capture_task_values=captured) as job:
                    run = job.run()
                assert run.failed
                assert run.task("transform").task_values["stage"] == "validation"
                assert run.task("transform").raised(ValueError)
            """
        )
        pytester.runpytest().assert_outcomes(passed=1)
        [created] = fake_ws.jobs.created
        assert [t.task_key for t in created["tasks"]][-1] == "__pytest_probe__transform"

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

    def test_helper_settings_from_lakeflow(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        configure_lakeflow(pytester, 'helpers_path="/Shared/helpers", helpers_compute="classic"')
        pytester.makepyfile(
            """
            def test_job(lakeflow):
                with lakeflow.job("my_job", capture_task_values={"load": ["k"]}):
                    pass
            """
        )
        pytester.runpytest().assert_outcomes(passed=1)
        [created] = fake_ws.jobs.created
        probe = created["tasks"][-1]
        assert probe.job_cluster_key == "classic"
        assert probe.notebook_task.notebook_path == "/Shared/helpers/get_values"

    def test_helpers_on_existing_cluster(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        configure_lakeflow(pytester, 'helpers_compute="classic", existing_cluster_id="0123-456789-abcdef"')
        pytester.makepyfile(
            """
            def test_task(lakeflow):
                with lakeflow.task("my_job.transform", upstream_task_values={"extract": {"row_count": 1}}):
                    pass
            """
        )
        pytester.runpytest().assert_outcomes(passed=1)
        [created] = fake_ws.jobs.created
        stub = created["tasks"][0]
        assert stub.task_key == "extract"
        assert (stub.existing_cluster_id, stub.job_cluster_key) == ("0123-456789-abcdef", None)
        assert "job_clusters" not in created

    def test_replay_end_to_end(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        queue_capturing_run(fake_ws, {"row_count": 42})
        fake_ws.jobs.queue(*recorded("probe_success"))
        pytester.makepyfile(
            """
            def test_replay(lakeflow):
                first = lakeflow.job("my_job", capture_task_values={"extract": ["row_count"]}).run()
                assert first.task("extract").task_values == {"row_count": 42}
                again = lakeflow.task("my_job.transform", upstream_from=first, capture_task_values=["stage"]).run()
                assert again.succeeded
                assert sorted(again.injected_tasks) == ["__pytest_probe__transform", "extract"]
            """
        )
        pytester.runpytest().assert_outcomes(passed=1)
        replay = fake_ws.jobs.created[1]
        assert [t.task_key for t in replay["tasks"]] == ["extract", "transform", "__pytest_probe__transform"]
        assert replay["tasks"][0].notebook_task.base_parameters["__pytest_values"] == '{"row_count": 42}'

    def test_explicit_values_win_over_upstream_from(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        queue_capturing_run(fake_ws, {"row_count": 42})
        pytester.makepyfile(
            """
            import dataclasses

            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            def test_replay(lakeflow):
                first = lakeflow.job("my_job", capture_task_values={"extract": ["row_count"]}).run()
                explicit = {"extract": {"row_count": 0}, "audit": {"checked": True}}
                with lakeflow.task("my_job.transform", upstream_from=first, upstream_task_values=explicit):
                    pass
                other = dataclasses.replace(first, job_key="other_job")
                with pytest.raises(LakeflowError, match="run of job 'other_job', not of 'my_job'"):
                    lakeflow.task("my_job.transform", upstream_from=other)
            """
        )
        pytester.runpytest().assert_outcomes(passed=1)
        assert len(fake_ws.jobs.created) == 2, "the run of another job creates nothing"
        replay = fake_ws.jobs.created[1]
        assert [t.task_key for t in replay["tasks"]] == ["extract", "audit", "transform"]
        assert stub_values(replay, "extract") == '{"row_count": 0}'
        assert stub_values(replay, "audit") == '{"checked": true}'

    def test_job_stubs_end_to_end(self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
        fake_ws.jobs.queue(make_run(77, {"extract": "SUCCESS", "transform": "SUCCESS"}))
        pytester.makepyfile(
            """
            def test_job(lakeflow):
                with lakeflow.job("my_job", upstream_task_values={"extract": {"row_count": 10}}) as job:
                    run = job.run(only=["transform"])
                assert run.succeeded
                assert sorted(run.injected_tasks) == ["extract"]
                assert "extract" not in run.tasks
            """
        )
        pytester.runpytest().assert_outcomes(passed=1)
        [created] = fake_ws.jobs.created
        assert [t.task_key for t in created["tasks"]] == ["extract", "transform", "load", "classic"]
        assert stub_values(created, "extract") == escape({"row_count": 10})
        assert created["tasks"][1].depends_on == [jobs.TaskDependency(task_key="extract")]
        assert fake_ws.jobs.run_now_calls[0]["only"] == ["transform", "extract"]

    def test_replay_fills_stubs_from_a_previous_run(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        queue_capturing_run(fake_ws, {"row_count": 42})
        fake_ws.jobs.queue(make_run(78, {"extract": "SUCCESS", "transform": "SUCCESS", "load": "SUCCESS"}))
        pytester.makepyfile(
            """
            def test_replay(lakeflow):
                first = lakeflow.job("my_job", capture_task_values={"extract": ["row_count"]}).run()
                assert first.task("extract").task_values == {"row_count": 42}
                again = lakeflow.job("my_job").run(only=["transform+"], replay=first)
                assert again.succeeded
                assert sorted(again.injected_tasks) == ["extract"]
                assert sorted(again.tasks) == ["load", "transform"]
            """
        )
        pytester.runpytest().assert_outcomes(passed=1)
        replay = fake_ws.jobs.created[1]
        assert [t.task_key for t in replay["tasks"]] == ["extract", "transform", "load", "classic"]
        assert stub_values(replay, "extract") == '{"row_count": 42}'
        assert replay["tasks"][0].disable_auto_optimization
        assert fake_ws.jobs.run_now_calls[1]["only"] == ["transform+", "extract"]

    def test_replay_stubs_no_task_upstream_of_teardown(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        queue_capturing_run(fake_ws, {"row_count": 42})
        states = {"extract": "SUCCESS", "transform": "SUCCESS", "load": "SUCCESS", "__pytest_teardown": "SUCCESS"}
        fake_ws.jobs.queue(make_run(78, states))
        pytester.makepyfile(
            """
            def cleanup(dbutils, params):
                pass

            def test_replay(lakeflow):
                first = lakeflow.job("my_job", capture_task_values={"extract": ["row_count"]}).run()
                again = lakeflow.job("my_job", teardown=cleanup).run(only=["transform+"], replay=first)
                assert again.succeeded
            """
        )
        pytester.runpytest().assert_outcomes(passed=1)
        replay = fake_ws.jobs.created[1]
        assert [t.task_key for t in replay["tasks"]] == ["extract", "transform", "load", "classic", "__pytest_teardown"]
        assert stub_values(replay, "extract") == '{"row_count": 42}'
        classic = replay["tasks"][3]
        assert classic.notebook_task.notebook_path.endswith("/src/classic"), "upstream of teardown only, so no stub"
        assert fake_ws.jobs.run_now_calls[1]["only"] == ["transform+", "extract"]

    def test_replay_keeps_teardown_after_the_tasks_a_stub_cuts_loose(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        queue_capturing_run(fake_ws, {"k": 1}, probed="transform")
        fake_ws.jobs.queue(make_run(78, {"transform": "SUCCESS", "load": "SUCCESS", "__pytest_teardown": "SUCCESS"}))
        pytester.makepyfile(
            """
            def cleanup(dbutils, params):
                pass

            def test_replay(lakeflow):
                first = lakeflow.job("my_job", capture_task_values={"transform": ["k"]}).run()
                assert lakeflow.job("my_job", teardown=cleanup).run(only=["load"], replay=first).succeeded
            """
        )
        pytester.runpytest().assert_outcomes(passed=1)
        replay = {t.task_key: t for t in fake_ws.jobs.created[1]["tasks"]}
        assert stub_values(fake_ws.jobs.created[1], "transform") == '{"k": 1}'
        assert replay["transform"].depends_on is None
        teardown = [d.task_key for d in replay["__pytest_teardown"].depends_on]
        assert teardown == ["extract", "load", "classic"], "extract no longer has a task after it but the teardown"

    def test_replay_on_a_created_job_fails_clearly(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient
    ) -> None:
        queue_capturing_run(fake_ws, {"row_count": 42})
        pytester.makepyfile(
            """
            import dataclasses

            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            def rejects(message):
                return pytest.raises(LakeflowError, match=message)

            def test_replay(lakeflow):
                first = lakeflow.job("my_job", capture_task_values={"extract": ["row_count"]}).run()
                with lakeflow.job("my_job") as job:
                    with rejects(r"replay.*before the job is created.*job \\d+ of 'my_job' already exists"):
                        job.run(only=["transform"], replay=first)
                job = lakeflow.job("my_job")
                with rejects("replay stubs the tasks that only leaves out, so it needs only"):
                    job.run(replay=first)
                other = dataclasses.replace(first, job_key="other_job")
                with rejects("replay is a run of job 'other_job', not of 'my_job'"):
                    job.run(only=["transform"], replay=other)
                with rejects("replay has no task values for 'transform', upstream of 'load': it has no captured"):
                    job.run(only=["load"], replay=first)
            """
        )
        pytester.runpytest().assert_outcomes(passed=1)
        assert len(fake_ws.jobs.created) == 2, "a rejected replay creates nothing"
        assert len(fake_ws.jobs.run_now_calls) == 1


class TestEnvironmentCap:
    """Per-job environments count towards the cap of ten together with the helper environment."""

    def test_merge_environments_respects_the_cap(self) -> None:
        settings = inject_probes(with_environment_count(9), {"load": ["k"]}, HELPERS)
        with pytest.raises(LakeflowError, match="10") as e:
            merge_environments(settings, {"extra": {"environment_version": "4"}})
        assert "'default', 'env1'" in str(e.value) and f"'{ENV_KEY}', 'extra'" in str(e.value)
        settings = inject_probes(with_environment_count(8), {"load": ["k"]}, HELPERS)
        merged = merge_environments(settings, {"extra": {"environment_version": "4"}})
        assert environment_keys(merged)[-2:] == [ENV_KEY, "extra"]
