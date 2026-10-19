from __future__ import annotations

import json
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
    add_dependencies,
    check_values,
    escape,
    inject_probes,
    upstream_values_from,
    wrap_task,
)

from .conftest import configure_lakeflow
from .fakes import FakeWorkspaceClient, example_settings, make_run, recorded

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


def queue_capturing_run(fake_ws: FakeWorkspaceClient, values: Mapping[str, object]) -> None:
    """Queue a full run of my_job whose probe on extract captured `values`."""
    states = {"extract": "SUCCESS", "transform": "SUCCESS", "load": "SUCCESS", "classic": "SUCCESS"}
    run = make_run(5, {**states, "__pytest_probe__extract": "SUCCESS"})
    probe_output = jobs.NotebookOutput(result=json.dumps({"values": dict(values), "missing": []}))
    fake_ws.jobs.queue(run, {504: jobs.RunOutput(notebook_output=probe_output)})


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
