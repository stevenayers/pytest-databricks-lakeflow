from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest
from databricks.sdk.service import compute, jobs

from pytest_databricks_lakeflow.errors import LakeflowError
from pytest_databricks_lakeflow.settings import (
    ENV_KEY,
    check_compute,
    check_params,
    create_kwargs,
    disable_tasks,
    sanitise,
    select,
    tag_value,
)

from .fakes import example_settings, reporting_settings

CLUSTER = "0123-456789-abcdef"
TAGS = {"pytest": "", "pytest_run_id": "r1", "pytest_nodeid": "tests/test_x.py::test_y"}


def sanitised(settings: jobs.JobSettings | None = None, **kwargs: Any) -> jobs.JobSettings:
    options: dict[str, Any] = {"disable_auto_optimization": True, "performance_optimized": False, **kwargs}
    return sanitise(settings or example_settings(), "tests/test_x.py::test_y", TAGS, **options)


def task(settings: jobs.JobSettings, key: str) -> jobs.Task:
    return next(t for t in settings.tasks or [] if t.task_key == key)


def test_sanitise_strips_what_would_run_notify_or_lock_the_copy() -> None:
    settings = sanitised()
    for attr in ("deployment", "edit_mode", "schedule", "email_notifications", "run_as", "trigger", "continuous"):
        assert getattr(settings, attr) is None, attr
    assert task(settings, "transform").email_notifications is None
    assert settings.max_concurrent_runs == 1
    assert settings.name == "[pytest] tests/test_x.py::test_y"
    assert settings.tags == {"dev": "someone", "purpose": "pytest", **TAGS}


def test_run_as_is_stripped_by_default() -> None:
    assert sanitised().run_as is None
    assert sanitised(run_as=None).run_as is None


def test_run_as_keep_preserves_the_template() -> None:
    assert sanitised(run_as="keep").run_as == jobs.JobRunAs(user_name="someone@example.com")


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("me@example.com", jobs.JobRunAs(user_name="me@example.com")),
        ("0000-app-id", jobs.JobRunAs(service_principal_name="0000-app-id")),
    ],
)
def test_run_as_string_picks_user_or_service_principal(value: str, expected: jobs.JobRunAs) -> None:
    assert sanitised(run_as=value).run_as == expected


def test_run_as_object_is_used_as_is() -> None:
    run_as = jobs.JobRunAs(service_principal_name="1111-object-id")
    assert sanitised(run_as=run_as).run_as is run_as


def test_sanitise_keeps_the_rest() -> None:
    before, after = example_settings(), sanitised()
    for attr in ("parameters", "environments", "job_clusters"):
        assert getattr(after, attr) == getattr(before, attr)
    assert [t.task_key for t in after.tasks or []] == [t.task_key for t in before.tasks or []]
    assert task(after, "transform").depends_on == task(before, "transform").depends_on


def test_sanitise_strips_triggers() -> None:
    settings = example_settings()
    hours = jobs.PeriodicTriggerConfigurationTimeUnit.HOURS
    settings.trigger = jobs.TriggerSettings(periodic=jobs.PeriodicTriggerConfiguration(interval=1, unit=hours))
    assert sanitised(settings).trigger is None


def test_auto_optimisation_is_disabled_on_serverless_code_tasks_only() -> None:
    settings = sanitised()
    assert task(settings, "extract").disable_auto_optimization
    assert task(settings, "load").disable_auto_optimization
    assert task(settings, "classic").disable_auto_optimization is None
    kept = sanitised(disable_auto_optimization=False)
    assert task(kept, "extract").disable_auto_optimization is None


def test_auto_optimisation_reaches_serverless_jar_tasks() -> None:
    settings = example_settings()
    jar = {"main_class_name": "x"}
    settings.tasks = [
        *(settings.tasks or []),
        jobs.Task.from_dict({"task_key": "jar", "environment_key": "default", "spark_jar_task": jar}),
        jobs.Task.from_dict({"task_key": "classic_jar", "job_cluster_key": "classic", "spark_jar_task": jar}),
    ]
    sanitised_settings = sanitised(settings)
    assert task(sanitised_settings, "jar").disable_auto_optimization is True
    assert task(sanitised_settings, "classic_jar").disable_auto_optimization is None


def with_environments(*environments: dict[str, Any]) -> jobs.JobSettings:
    settings = example_settings()
    settings.environments = [
        *(settings.environments or []),
        *(jobs.JobEnvironment.from_dict(e) for e in environments),
        jobs.JobEnvironment.from_dict({"environment_key": ENV_KEY, "spec": {"environment_version": "4"}}),
    ]
    return settings


def versions(settings: jobs.JobSettings) -> dict[str, str | None]:
    return {e.environment_key: e.spec.environment_version if e.spec else None for e in settings.environments or []}


def test_environment_version_override() -> None:
    other = {"environment_key": "other", "spec": {"environment_version": "3"}}
    settings = sanitised(with_environments(other), environment_version="5")
    assert versions(settings) == {"default": "5", "other": "5", ENV_KEY: "4"}


def test_environment_version_gives_a_spec_to_an_environment_without_one() -> None:
    settings = sanitised(with_environments({"environment_key": "bare"}), environment_version="5")
    assert versions(settings) == {"default": "5", "bare": "5", ENV_KEY: "4"}


@pytest.mark.skipif(
    "base_environment" not in compute.Environment.__dataclass_fields__,
    reason="this databricks-sdk has no base_environment",
)
def test_environment_version_leaves_base_environments_alone() -> None:
    base = {"environment_key": "custom", "spec": {"base_environment": "/Workspace/envs/base.yml"}}
    settings = sanitised(with_environments(base), environment_version="5")
    assert versions(settings) == {"default": "5", "custom": None, ENV_KEY: "4"}


def test_budget_policy_id_is_set() -> None:
    assert sanitised(budget_policy_id="0123").budget_policy_id == "0123"


def test_existing_cluster_id_moves_job_cluster_tasks() -> None:
    settings = example_settings()
    fresh = {"task_key": "fresh", "new_cluster": {"spark_version": "17.3.x"}, "notebook_task": {"notebook_path": "/x"}}
    sql = {"task_key": "sql", "notebook_task": {"notebook_path": "/q", "warehouse_id": "w1"}}
    settings.tasks = [*(settings.tasks or []), jobs.Task.from_dict(fresh), jobs.Task.from_dict(sql)]
    after = sanitised(settings, existing_cluster_id=CLUSTER)
    assert after.job_clusters is None
    for key in ("classic", "fresh"):
        moved = task(after, key)
        assert (moved.existing_cluster_id, moved.job_cluster_key, moved.new_cluster) == (CLUSTER, None, None)
    assert task(after, "sql").existing_cluster_id is None
    before = sanitised()
    for key in ("extract", "transform", "load"):
        assert task(after, key) == task(before, key)


def test_existing_cluster_id_moves_for_each_inner_tasks() -> None:
    settings = example_settings()
    inner = {"task_key": "inner", "job_cluster_key": "classic", "notebook_task": {"notebook_path": "/x"}}
    loop = {"task_key": "loop", "for_each_task": {"inputs": "[1, 2]", "task": inner}}
    settings.tasks = [*(settings.tasks or []), jobs.Task.from_dict(loop)]
    after = sanitised(settings, existing_cluster_id=CLUSTER)
    assert after.job_clusters is None
    for_each = task(after, "loop").for_each_task
    assert for_each is not None and for_each.task is not None
    moved = for_each.task
    assert (moved.existing_cluster_id, moved.job_cluster_key, moved.new_cluster) == (CLUSTER, None, None)


def test_sanitise_without_compute_knobs_changes_nothing() -> None:
    plain = sanitised()
    assert sanitised(environment_version=None, budget_policy_id=None, existing_cluster_id=None) == plain
    assert plain.budget_policy_id is None
    assert plain.job_clusters == example_settings().job_clusters
    assert plain.environments == example_settings().environments


@pytest.mark.parametrize(
    "code",
    [
        {"python_wheel_task": {"package_name": "pkg", "entry_point": "main"}},
        {"spark_python_task": {"python_file": "/x.py"}},
        {"spark_jar_task": {"main_class_name": "x"}},
        {"dbt_task": {"commands": ["dbt run"]}},
    ],
)
def test_check_compute_names_tasks_without_environment(code: dict[str, Any]) -> None:
    settings = example_settings()
    settings.tasks = [*(settings.tasks or []), jobs.Task.from_dict({"task_key": "bare", **code})]
    with pytest.raises(LakeflowError, match=r"\['bare'\].*environment_key"):
        check_compute(settings)


def test_check_compute_names_for_each_inner_tasks_without_environment() -> None:
    settings = example_settings()
    inner = {"task_key": "inner", "python_wheel_task": {"package_name": "pkg", "entry_point": "main"}}
    loop = {"task_key": "loop", "for_each_task": {"inputs": "[1, 2]", "task": inner}}
    settings.tasks = [*(settings.tasks or []), jobs.Task.from_dict(loop)]
    with pytest.raises(LakeflowError, match=r"\['inner'\].*environment_key"):
        check_compute(settings)


def test_check_compute_accepts_example_settings() -> None:
    check_compute(example_settings())
    settings = example_settings()
    wheel = {"package_name": "pkg", "entry_point": "main"}
    on_cluster = {"task_key": "c", "existing_cluster_id": "c-1", "python_wheel_task": wheel}
    settings.tasks = [*(settings.tasks or []), jobs.Task.from_dict(on_cluster)]
    check_compute(settings)


def test_performance_target() -> None:
    settings = example_settings()
    settings.performance_target = jobs.PerformanceTarget.STANDARD
    assert sanitised(settings).performance_target == jobs.PerformanceTarget.STANDARD
    forced = sanitised(performance_optimized=True)
    assert forced.performance_target == jobs.PerformanceTarget.PERFORMANCE_OPTIMIZED


def test_long_node_ids_fit_names_and_tags() -> None:
    nodeid = "tests/test_päth.py::test_rows[" + "x" * 5000 + "]\nline two 中"
    settings = sanitise(
        example_settings(),
        nodeid,
        {"pytest_nodeid": nodeid},
        disable_auto_optimization=True,
        performance_optimized=False,
    )
    assert settings.name is not None and settings.tags is not None
    assert len(settings.name.encode()) <= 4096
    assert "\n" not in settings.name
    value = settings.tags["pytest_nodeid"]
    assert len(value.encode()) <= 256
    assert value.startswith("tests/test_päth.py::test_rows[xxx")


def test_tag_value() -> None:
    assert tag_value("a\r\nb 中文 é") == "a b ?? é"
    assert tag_value("é" * 200) == "é" * 128


def test_too_many_tags() -> None:
    settings = example_settings()
    settings.tags = {f"t{i}": "" for i in range(23)}
    with pytest.raises(LakeflowError, match="26 tags, more than the 25 allowed. Remove 1"):
        sanitised(settings)


ATTRIBUTION = {
    "pytest_git_branch": "main",
    "pytest_git_sha": "abc",
    "pytest_user": "someone",
    "pytest_ci_url": "https://github.com/o/r/actions/runs/9",
}


def test_attribution_tags_are_added() -> None:
    settings = sanitise(
        example_settings(),
        "tests/test_x.py::test_y",
        {**TAGS, **ATTRIBUTION},
        disable_auto_optimization=True,
        performance_optimized=False,
    )
    assert settings.tags == {"dev": "someone", "purpose": "pytest", **TAGS, **ATTRIBUTION}


def test_attribution_tags_fit_within_the_cap(caplog: pytest.LogCaptureFixture) -> None:
    settings = example_settings()
    settings.tags = {f"t{i}": "" for i in range(20)}
    caplog.set_level("DEBUG", logger="pytest_databricks_lakeflow")
    sanitised_settings = sanitise(
        settings,
        "tests/test_x.py::test_y",
        {**TAGS, **ATTRIBUTION},
        disable_auto_optimization=True,
        performance_optimized=False,
    )
    tags = sanitised_settings.tags or {}
    assert len(tags) == 25
    assert {key for key in tags if key in ATTRIBUTION} == {"pytest_ci_url", "pytest_user"}
    assert "pytest_git_sha" in caplog.text and "pytest_git_branch" in caplog.text


def test_attribution_tags_never_push_out_the_required_tags() -> None:
    settings = example_settings()
    settings.tags = {f"t{i}": "" for i in range(22)}
    tags = sanitise(
        settings,
        "tests/test_x.py::test_y",
        {**TAGS, **ATTRIBUTION},
        disable_auto_optimization=True,
        performance_optimized=False,
    ).tags
    assert tags is not None
    assert len(tags) == 25
    assert set(TAGS) <= set(tags)
    assert not set(ATTRIBUTION) & set(tags)
    settings.tags = {f"t{i}": "" for i in range(23)}
    with pytest.raises(LakeflowError, match="26 tags, more than the 25 allowed. Remove 1"):
        sanitise(
            settings,
            "tests/test_x.py::test_y",
            {**TAGS, **ATTRIBUTION},
            disable_auto_optimization=True,
            performance_optimized=False,
        )


def test_attribution_values_are_made_acceptable() -> None:
    tags = sanitise(
        example_settings(),
        "tests/test_x.py::test_y",
        {**TAGS, "pytest_git_branch": "feature/中\nx" + "y" * 300},
        disable_auto_optimization=True,
        performance_optimized=False,
    ).tags
    assert tags is not None
    branch = tags["pytest_git_branch"]
    assert branch.startswith("feature/? x")
    assert len(branch.encode()) <= 256
    branch.encode("latin-1")


def test_description_prepends_the_test() -> None:
    settings = example_settings()
    settings.description = "The bundle's job."
    described = sanitised(settings, description="tests/test_x.py::test_y https://github.com/o/r/actions/runs/9")
    assert described.description == (
        "tests/test_x.py::test_y https://github.com/o/r/actions/runs/9\n\nThe bundle's job."
    )


def test_description_without_ci_url() -> None:
    assert sanitised(description="tests/test_x.py::test_y").description == "tests/test_x.py::test_y"
    assert sanitised().description is None


def test_long_descriptions_are_trimmed() -> None:
    settings = example_settings()
    settings.description = "中" * 20000
    description = sanitised(settings, description="tests/test_x.py::test_y").description
    assert description is not None
    assert len(description.encode()) <= 27700
    assert description.startswith("tests/test_x.py::test_y\n\n中")


def test_create_kwargs() -> None:
    settings = sanitised()
    settings.triggers = []
    kwargs = create_kwargs(settings)
    assert "triggers" not in kwargs
    assert "deployment" not in kwargs
    assert kwargs["name"] == settings.name
    assert kwargs["tasks"] == settings.tasks
    assert set(kwargs) <= set(jobs.JobSettings.__dataclass_fields__)


def test_check_params() -> None:
    settings = example_settings()
    check_params("my_job", settings, {"param1": "x", "schema": "y"}, allow_undeclared=False)
    with pytest.raises(LakeflowError, match=r"does not declare the parameters \['parm1'\]. Declared: \['param1'") as e:
        check_params("my_job", settings, {"parm1": "x"}, allow_undeclared=False)
    assert str(e.value).endswith("Pass allow_undeclared_params=True to Lakeflow to pass them anyway.")
    check_params("my_job", settings, {"parm1": "x"}, allow_undeclared=True)
    with pytest.raises(LakeflowError, match="reserved for the plugin"):
        check_params("my_job", settings, {"__pytest_values": "x"}, allow_undeclared=True)


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
def test_select_adds_probes(only: list[str], expected: list[str]) -> None:
    assert select(chain(), only) == expected


def test_select_adds_stubs_of_selected_tasks() -> None:
    assert select(chain(), ["transform"], stubs={"extract"}) == ["transform", "__pytest_probe__transform", "extract"]
    assert select(chain(), ["+transform"], stubs={"extract"}) == ["+transform", "__pytest_probe__transform"]


def test_select_rejects_unknown_tasks() -> None:
    with pytest.raises(LakeflowError, match=r"Unknown task 'lod' in only. Available: \['extract', 'load'"):
        select(chain(), ["lod+"])


def test_sanitise_mirrors_the_timeout() -> None:
    assert sanitised(timeout=timedelta(minutes=20)).timeout_seconds == 1260


def test_sanitise_mirrors_the_timeout_over_a_looser_template() -> None:
    settings = example_settings()
    settings.timeout_seconds = 7200
    assert sanitised(settings, timeout=timedelta(minutes=20)).timeout_seconds == 1260


def test_sanitise_rounds_the_timeout_up() -> None:
    assert sanitised(timeout=timedelta(seconds=1.2)).timeout_seconds == 62


def test_sanitise_keeps_a_stricter_template_timeout() -> None:
    settings = example_settings()
    settings.timeout_seconds = 300
    assert sanitised(settings, timeout=timedelta(minutes=30)).timeout_seconds == 300


def test_sanitise_replaces_a_zero_template_timeout() -> None:
    settings = example_settings()
    settings.timeout_seconds = 0
    assert sanitised(settings, timeout=timedelta(minutes=1)).timeout_seconds == 120


def test_sanitise_without_timeout() -> None:
    assert sanitised().timeout_seconds is None


def test_sanitise_pauses_dashboard_and_alert_subscriptions() -> None:
    before, after = reporting_settings(), sanitised(reporting_settings())
    dashboard, alert = task(after, "dashboard").dashboard_task, task(after, "alert").sql_task
    assert dashboard is not None and dashboard.subscription is not None
    assert alert is not None and alert.alert is not None
    assert dashboard.subscription.paused is True
    assert alert.alert.pause_subscriptions is True
    template = task(before, "dashboard").dashboard_task
    assert template is not None and template.subscription is not None
    assert (dashboard.dashboard_id, dashboard.warehouse_id) == ("d1", "w1")
    assert dashboard.subscription.subscribers == template.subscription.subscribers
    assert alert.alert.subscriptions == [jobs.SqlTaskSubscription(user_name="someone@example.com")]
    assert task(after, "notify").notebook_task == task(before, "notify").notebook_task


def test_sanitise_keeps_side_effects_when_asked() -> None:
    settings = sanitised(reporting_settings(), side_effects="keep")
    dashboard, alert = task(settings, "dashboard").dashboard_task, task(settings, "alert").sql_task
    assert dashboard is not None and dashboard.subscription is not None
    assert alert is not None and alert.alert is not None
    assert dashboard.subscription.paused is None
    assert alert.alert.pause_subscriptions is None


def test_sanitise_leaves_dashboards_without_a_subscription_alone() -> None:
    settings = reporting_settings()
    dashboard = task(settings, "dashboard").dashboard_task
    assert dashboard is not None
    dashboard.subscription = None
    assert task(sanitised(settings), "dashboard").dashboard_task == jobs.DashboardTask(
        dashboard_id="d1", warehouse_id="w1"
    )


def test_disable_tasks() -> None:
    settings = disable_tasks(example_settings(), ["load"])
    assert {t.task_key: t.disabled for t in settings.tasks or []} == {
        "extract": None,
        "transform": None,
        "load": True,
        "classic": None,
    }
    with pytest.raises(LakeflowError, match=r"Unknown task 'nope' in disable. Available: \['classic', 'extract'"):
        disable_tasks(example_settings(), ["nope"])
