from __future__ import annotations

import json

import pytest
from databricks.sdk.service import jobs

from pytest_databricks_lakeflow.errors import LakeflowError
from pytest_databricks_lakeflow.settings import ENV_KEY, Helpers, check_values, escape, inject_probes, wrap_task

from .conftest import configure_lakeflow
from .fakes import FakeWorkspaceClient, example_settings, recorded

HELPERS = Helpers(set_values="/h/set_values", get_values="/h/get_values")
UPSTREAM = {"extract": {"row_count": 42, "source_path": "/Volumes/x/y/z"}}

pytestmark = pytest.mark.usefixtures("fake_cli", "bundle_dir")


def keys(settings: jobs.JobSettings) -> list[str]:
    return [t.task_key for t in settings.tasks or []]


def test_wrap_task_keeps_one_task_with_stubs_and_a_probe() -> None:
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


def test_wrap_task_without_upstream_or_capture() -> None:
    settings = wrap_task(example_settings(), "load", {}, [], HELPERS)
    assert keys(settings) == ["load"]
    assert (settings.tasks or [])[0].depends_on is None
    assert ENV_KEY not in {e.environment_key for e in settings.environments or []}


def test_wrap_task_needs_stubs_for_referenced_values() -> None:
    expected = r"refers to task values of \['extract'\].*Pass them in upstream_task_values"
    with pytest.raises(LakeflowError, match=expected):
        wrap_task(example_settings(), "transform", {}, [], HELPERS)


def test_wrap_task_rejects_unknown_and_self_stubbing() -> None:
    with pytest.raises(LakeflowError, match=r"No task 'transfrom' in the job. Available: \['classic'"):
        wrap_task(example_settings(), "transfrom", {}, [], HELPERS)
    with pytest.raises(LakeflowError, match="cannot stub 'load', the task under test"):
        wrap_task(example_settings(), "load", {"load": {}}, [], HELPERS)


def test_helpers_on_a_job_cluster() -> None:
    helpers = Helpers(set_values="/s", get_values="/g", job_cluster_key="classic")
    settings = wrap_task(example_settings(), "transform", UPSTREAM, ["k"], helpers)
    assert all(t.job_cluster_key == "classic" for t in settings.tasks or [] if t.task_key != "transform")
    assert ENV_KEY not in {e.environment_key for e in settings.environments or []}
    with pytest.raises(LakeflowError, match=r"^helpers_compute is 'gpu'.*Available: \['classic'\]"):
        wrap_task(example_settings(), "transform", UPSTREAM, [], Helpers("/s", "/g", job_cluster_key="gpu"))


def test_helper_environment_version() -> None:
    settings = inject_probes(example_settings(), {"load": ["k"]}, Helpers("/s", "/g", environment_version="6"))
    env = next(e for e in settings.environments or [] if e.environment_key == ENV_KEY)
    assert env.spec is not None and env.spec.environment_version == "6"


def test_inject_probes() -> None:
    settings = inject_probes(example_settings(), {"transform": ["stage"], "load": ["rows"]}, HELPERS)
    assert keys(settings) == [
        "extract",
        "transform",
        "load",
        "classic",
        "__pytest_probe__transform",
        "__pytest_probe__load",
    ]
    assert [e.environment_key for e in settings.environments or []].count(ENV_KEY) == 1
    assert inject_probes(example_settings(), {}, HELPERS) == example_settings()
    with pytest.raises(LakeflowError, match=r"not in the job: \['nope'\]"):
        inject_probes(example_settings(), {"nope": ["k"]}, HELPERS)


def test_escape_stops_reference_substitution() -> None:
    values = {"a": "{{job.id}}", "b": "{{tasks.x.values.k}}", "c": "x{{{y", "d": ["{{", {"e": "}}"}]}
    escaped = escape(values)
    assert "{{" not in escaped
    assert json.loads(escaped) == values


def test_check_values() -> None:
    check_values("extract", {"ok": "x" * 49_000, "n": None})
    with pytest.raises(LakeflowError, match="'big' for task 'extract' is 49155 characters as JSON"):
        check_values("extract", {"big": "x" * 49_153})
    with pytest.raises(LakeflowError, match="not JSON-serialisable"):
        check_values("extract", {"bad": object()})
    with pytest.raises(LakeflowError, match="longer than 512"):
        check_values("extract", {"k" * 513: 1})
    with pytest.raises(LakeflowError, match="longer than 512"):
        inject_probes(example_settings(), {"load": ["k" * 513]}, HELPERS)


def test_single_task_end_to_end(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
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


def test_job_capture_task_values_end_to_end(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
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


def test_test_marker_on_a_task_job(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
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


def test_helper_settings_from_lakeflow(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
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


def with_environment_count(count: int) -> jobs.JobSettings:
    settings = example_settings()
    extra = [jobs.JobEnvironment.from_dict({"environment_key": f"env{i}"}) for i in range(1, count)]
    settings.environments = [*(settings.environments or []), *extra]
    return settings


def test_helpers_refuse_an_eleventh_environment() -> None:
    with pytest.raises(LakeflowError, match=r"10 environments.*'default', 'env1'.*'env9'.*make room") as e:
        inject_probes(with_environment_count(10), {"transform": ["x"]}, HELPERS)
    assert ENV_KEY in str(e.value)
    settings = inject_probes(with_environment_count(9), {"transform": ["x"]}, HELPERS)
    assert len(settings.environments or []) == 10
    assert (settings.environments or [])[-1].environment_key == ENV_KEY


def test_wrap_task_refuses_an_eleventh_environment() -> None:
    with pytest.raises(LakeflowError, match=r"10 environments.*'default', 'env1'.*'env9'.*make room"):
        wrap_task(with_environment_count(10), "transform", UPSTREAM, [], HELPERS)
    settings = wrap_task(with_environment_count(9), "transform", UPSTREAM, [], HELPERS)
    assert (settings.environments or [])[-1].environment_key == ENV_KEY


def test_helpers_on_existing_cluster(pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient) -> None:
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
