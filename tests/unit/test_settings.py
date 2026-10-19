from __future__ import annotations

import copy
import json
import logging
from datetime import timedelta
from typing import Any

import pytest
from databricks.sdk.service import compute, jobs

from pytest_databricks_lakeflow import settings as settings_module
from pytest_databricks_lakeflow.errors import LakeflowError
from pytest_databricks_lakeflow.settings import (
    ENV_KEY,
    TASK_OVERRIDES,
    Helpers,
    add_dependencies,
    apply_task_params,
    check_compute,
    check_params,
    check_references,
    check_task_params,
    create_kwargs,
    disable_tasks,
    force_conditions,
    inject_lifecycle,
    inject_probes,
    inject_test_marker,
    merge_environments,
    override_tasks,
    pipeline_params,
    run_job_tasks,
    sanitise,
    select,
    tag_value,
    wrap_task,
)

from .conftest import NEEDS_REFRESH_SELECTION
from .fakes import (
    branching_settings,
    example_settings,
    parent_settings,
    pipeline_settings,
    reporting_settings,
)

CLUSTER = "0123-456789-abcdef"
NODEID = "tests/test_x.py::test_y"
TAGS = {"pytest": "", "pytest_run_id": "r1", "pytest_nodeid": NODEID}
ATTRIBUTION = {
    "pytest_git_branch": "main",
    "pytest_git_sha": "abc",
    "pytest_user": "someone",
    "pytest_ci_url": "https://github.com/o/r/actions/runs/9",
}
SINGLE_NODE_CONF = {"spark.master": "local[*, 4]", "spark.databricks.cluster.profile": "singleNode"}
HELPERS = Helpers(set_values="/h/set_values", get_values="/h/get_values")


def sanitised(
    settings: jobs.JobSettings | None = None, tags: dict[str, str] = TAGS, name: str = NODEID, **kwargs: Any
) -> jobs.JobSettings:
    options: dict[str, Any] = {"disable_auto_optimization": True, "performance_optimized": False, **kwargs}
    return sanitise(settings or example_settings(), name, tags, **options)


def task(settings: jobs.JobSettings, key: str) -> jobs.Task:
    return next(t for t in settings.tasks or [] if t.task_key == key)


def with_tasks(*tasks: dict[str, Any]) -> jobs.JobSettings:
    settings = example_settings()
    settings.tasks = [*(settings.tasks or []), *(jobs.Task.from_dict(t) for t in tasks)]
    return settings


def three_tasks() -> jobs.JobSettings:
    """Build a job with three tasks, `a`, then `b` after it, and `c` on its own, and a `day` parameter."""
    return jobs.JobSettings.from_dict(
        {
            "parameters": [{"name": "day", "default": ""}, {"name": "__pytest_test_id", "default": "x"}],
            "tasks": [
                {"task_key": "a", "notebook_task": {"notebook_path": "/a"}},
                {"task_key": "b", "depends_on": [{"task_key": "a"}], "notebook_task": {"notebook_path": "/b"}},
                {"task_key": "c", "notebook_task": {"notebook_path": "/c"}},
            ],
        }
    )


def for_each(inner: dict[str, Any]) -> dict[str, Any]:
    return {"task_key": "loop", "for_each_task": {"inputs": "[1, 2]", "task": inner}}


def nested_task(settings: jobs.JobSettings, key: str) -> jobs.Task:
    loop = task(settings, key).for_each_task
    assert loop is not None and loop.task is not None
    return loop.task


class TestStripping:
    """What sanitise removes from the template, and what it leaves as deployed."""

    def test_sanitise_strips_what_would_run_notify_or_lock_the_copy(self) -> None:
        settings = example_settings()
        hours = jobs.PeriodicTriggerConfigurationTimeUnit.HOURS
        settings.trigger = jobs.TriggerSettings(periodic=jobs.PeriodicTriggerConfiguration(interval=1, unit=hours))
        sanitised(settings)
        for attr in ("deployment", "edit_mode", "schedule", "email_notifications", "run_as", "trigger", "continuous"):
            assert getattr(settings, attr) is None, attr
        assert task(settings, "transform").email_notifications is None
        assert settings.max_concurrent_runs == 1
        assert settings.name == "[pytest] tests/test_x.py::test_y"
        assert settings.tags == {"dev": "someone", "purpose": "pytest", **TAGS}

    def test_sanitise_keeps_the_rest(self) -> None:
        before, after = example_settings(), sanitised()
        for attr in ("parameters", "environments", "job_clusters"):
            assert getattr(after, attr) == getattr(before, attr)
        assert [t.task_key for t in after.tasks or []] == [t.task_key for t in before.tasks or []]
        assert task(after, "transform").depends_on == task(before, "transform").depends_on

    def test_sanitise_leaves_pipeline_tasks_alone(self) -> None:
        settings = pipeline_settings()
        deployed = task(settings, "refresh").pipeline_task
        assert deployed is not None
        deployed.full_refresh = True
        expected = deployed.as_dict()
        copied = task(sanitised(settings), "refresh").pipeline_task
        assert copied is not None
        assert copied.as_dict() == expected == {"pipeline_id": "abc", "full_refresh": True}

    def test_sanitise_keeps_run_job_tasks_as_deployed(self) -> None:
        expected = task(parent_settings(), "trigger_child").run_job_task
        assert task(sanitised(parent_settings()), "trigger_child").run_job_task == expected


class TestRunAs:
    """The run_as option: stripped by default, kept on request, or replaced by a principal."""

    def test_run_as_is_stripped_by_default(self) -> None:
        assert sanitised().run_as is None

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("keep", jobs.JobRunAs(user_name="someone@example.com")),
            ("me@example.com", jobs.JobRunAs(user_name="me@example.com")),
            ("0000-app-id", jobs.JobRunAs(service_principal_name="0000-app-id")),
        ],
    )
    def test_run_as_option_is_applied(self, value: str, expected: jobs.JobRunAs) -> None:
        assert sanitised(run_as=value).run_as == expected

    def test_run_as_object_is_used_as_is(self) -> None:
        run_as = jobs.JobRunAs(service_principal_name="1111-object-id")
        assert sanitised(run_as=run_as).run_as is run_as


class TestAutoOptimisation:
    """disable_auto_optimization reaches serverless code tasks and nothing else."""

    def test_auto_optimisation_is_disabled_on_serverless_code_tasks_only(self) -> None:
        jar = {"main_class_name": "x"}
        settings = with_tasks(
            {"task_key": "jar", "environment_key": "default", "spark_jar_task": jar},
            {"task_key": "classic_jar", "job_cluster_key": "classic", "spark_jar_task": jar},
        )
        sanitised(settings)
        for key in ("extract", "load", "jar"):
            assert task(settings, key).disable_auto_optimization is True, key
        for key in ("classic", "classic_jar"):
            assert task(settings, key).disable_auto_optimization is None, key
        kept = sanitised(disable_auto_optimization=False)
        assert task(kept, "extract").disable_auto_optimization is None


class TestComputeOverrides:
    """environment_version, budget_policy_id, existing_cluster_id and performance_optimized."""

    @staticmethod
    def with_environments(*environments: dict[str, Any]) -> jobs.JobSettings:
        settings = example_settings()
        settings.environments = [
            *(settings.environments or []),
            *(jobs.JobEnvironment.from_dict(e) for e in environments),
            jobs.JobEnvironment.from_dict({"environment_key": ENV_KEY, "spec": {"environment_version": "4"}}),
        ]
        return settings

    @staticmethod
    def versions(settings: jobs.JobSettings) -> dict[str, str | None]:
        return {e.environment_key: e.spec.environment_version if e.spec else None for e in settings.environments or []}

    @pytest.mark.parametrize(
        ("environment", "expected"),
        [
            ({"environment_key": "other", "spec": {"environment_version": "3"}}, {"other": "5"}),
            ({"environment_key": "bare"}, {"bare": "5"}),
            ({"environment_key": "custom", "spec": {"base_environment": "/Workspace/envs/base.yml"}}, {"custom": None}),
        ],
    )
    def test_environment_version_override(self, environment: dict[str, Any], expected: dict[str, str | None]) -> None:
        settings = sanitised(self.with_environments(environment), environment_version="5")
        assert self.versions(settings) == {"default": "5", **expected, ENV_KEY: "4"}

    def test_budget_policy_id_is_set(self) -> None:
        assert sanitised(budget_policy_id="0123").budget_policy_id == "0123"

    def test_existing_cluster_id_moves_job_cluster_tasks(self) -> None:
        inner = {"task_key": "inner", "job_cluster_key": "classic", "notebook_task": {"notebook_path": "/x"}}
        settings = with_tasks(
            {"task_key": "fresh", "new_cluster": {"spark_version": "17.3.x"}, "notebook_task": {"notebook_path": "/x"}},
            {"task_key": "sql", "notebook_task": {"notebook_path": "/q", "warehouse_id": "w1"}},
            for_each(inner),
        )
        after = sanitised(settings, existing_cluster_id=CLUSTER)
        assert after.job_clusters is None
        moved = [task(after, "classic"), task(after, "fresh"), nested_task(after, "loop")]
        for t in moved:
            assert (t.existing_cluster_id, t.job_cluster_key, t.new_cluster) == (CLUSTER, None, None), t.task_key
        assert task(after, "sql").existing_cluster_id is None
        before = sanitised()
        for key in ("extract", "transform", "load"):
            assert task(after, key) == task(before, key)

    def test_performance_target(self) -> None:
        settings = example_settings()
        settings.performance_target = jobs.PerformanceTarget.STANDARD
        assert sanitised(settings).performance_target == jobs.PerformanceTarget.STANDARD
        forced = sanitised(performance_optimized=True)
        assert forced.performance_target == jobs.PerformanceTarget.PERFORMANCE_OPTIMIZED

    def test_sanitise_without_compute_knobs_changes_nothing(self) -> None:
        plain = sanitised()
        explicit = sanitised(
            environment_version=None, budget_policy_id=None, existing_cluster_id=None, single_node=False
        )
        assert explicit == plain
        assert plain.budget_policy_id is None
        assert plain.job_clusters == example_settings().job_clusters
        assert plain.environments == example_settings().environments


class TestSingleNode:
    """single_node rewrites every job cluster and inline cluster, and refuses standard access mode."""

    @staticmethod
    def with_inline_cluster(cluster: dict[str, Any] | None = None) -> jobs.JobSettings:
        spec = cluster or {"spark_version": "17.3.x-scala2.13", "num_workers": 8}
        return with_tasks({"task_key": "inline", "new_cluster": spec, "notebook_task": {"notebook_path": "/x"}})

    @staticmethod
    def classic_cluster(settings: jobs.JobSettings) -> compute.ClusterSpec:
        [classic] = settings.job_clusters or []
        assert classic.job_cluster_key == "classic" and classic.new_cluster is not None
        return classic.new_cluster

    @staticmethod
    def assert_single_node(cluster: compute.ClusterSpec | None) -> None:
        assert cluster is not None
        assert (cluster.num_workers, cluster.autoscale) == (0, None)
        assert {k: (cluster.spark_conf or {}).get(k) for k in SINGLE_NODE_CONF} == SINGLE_NODE_CONF
        assert (cluster.custom_tags or {}).get("ResourceClass") == "SingleNode"

    def test_single_node_rewrites_job_clusters(self) -> None:
        settings = example_settings()
        classic = self.classic_cluster(settings)
        classic.autoscale = compute.AutoScale(min_workers=2, max_workers=8)
        classic.spark_conf = {"spark.sql.shuffle.partitions": "8"}
        classic.custom_tags = {"team": "data"}
        cluster = self.classic_cluster(sanitised(settings, single_node=True))
        self.assert_single_node(cluster)
        assert cluster.spark_version == "17.3.x-scala2.13"
        assert cluster.spark_conf and cluster.spark_conf["spark.sql.shuffle.partitions"] == "8"
        assert cluster.custom_tags and cluster.custom_tags["team"] == "data"

    @pytest.mark.parametrize("cluster", [None, {"spark_version": "17.3.x", "data_security_mode": "SINGLE_USER"}])
    def test_single_node_rewrites_inline_task_clusters(self, cluster: dict[str, Any] | None) -> None:
        after = sanitised(self.with_inline_cluster(cluster), single_node=True)
        self.assert_single_node(task(after, "inline").new_cluster)

    def test_single_node_rewrites_inline_clusters_in_for_each_tasks(self) -> None:
        inner = {
            "task_key": "inner",
            "new_cluster": {"spark_version": "17.3.x"},
            "notebook_task": {"notebook_path": "/x"},
        }
        after = sanitised(with_tasks(for_each(inner)), single_node=True)
        self.assert_single_node(nested_task(after, "loop").new_cluster)

    def test_single_node_leaves_serverless_tasks_alone(self) -> None:
        before, after = sanitised(), sanitised(single_node=True)
        for key in ("extract", "transform", "load", "classic"):
            assert task(after, key) == task(before, key)
        assert after.environments == before.environments
        assert [c.job_cluster_key for c in after.job_clusters or []] == ["classic"]

    @pytest.mark.parametrize(
        "mode", [compute.DataSecurityMode.USER_ISOLATION, compute.DataSecurityMode.DATA_SECURITY_MODE_STANDARD]
    )
    def test_single_node_rejects_standard_access_mode(self, mode: compute.DataSecurityMode) -> None:
        settings = example_settings()
        self.classic_cluster(settings).data_security_mode = mode
        with pytest.raises(LakeflowError, match=r"'classic'.*dedicated"):
            sanitised(settings, single_node=True)

    def test_single_node_changes_no_cluster_when_an_inline_cluster_is_rejected(self) -> None:
        settings = self.with_inline_cluster({"spark_version": "17.3.x", "data_security_mode": "USER_ISOLATION"})
        before = self.classic_cluster(settings).as_dict()
        with pytest.raises(LakeflowError, match=r"'inline'.*dedicated"):
            sanitised(settings, single_node=True)
        assert self.classic_cluster(settings).as_dict() == before

    def test_single_node_with_existing_cluster_id_has_nothing_to_rewrite(self) -> None:
        settings = self.with_inline_cluster({"spark_version": "17.3.x", "data_security_mode": "USER_ISOLATION"})
        after = sanitised(settings, single_node=True, existing_cluster_id=CLUSTER)
        assert after.job_clusters is None
        assert task(after, "inline").new_cluster is None
        assert after == sanitised(self.with_inline_cluster(), existing_cluster_id=CLUSTER)


class TestTimeouts:
    """The job's timeout_seconds mirrors the plugin's timeout plus the grace period."""

    @pytest.mark.parametrize(
        ("template", "timeout", "expected"),
        [
            (None, timedelta(minutes=20), 1260),
            (7200, timedelta(minutes=20), 1260),
            (None, timedelta(seconds=1.2), 62),
            (300, timedelta(minutes=30), 300),
            (0, timedelta(minutes=1), 120),
            (None, None, None),
        ],
    )
    def test_sanitise_mirrors_the_timeout(
        self, template: int | None, timeout: timedelta | None, expected: int | None
    ) -> None:
        settings = example_settings()
        settings.timeout_seconds = template
        assert sanitised(settings, timeout=timeout).timeout_seconds == expected


class TestSideEffects:
    """Dashboard and SQL alert subscriptions are paused unless side_effects is "keep"; alert and Power BI tasks stay."""

    def test_sanitise_pauses_dashboard_and_alert_subscriptions(self) -> None:
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

    def test_sanitise_keeps_side_effects_when_asked(self) -> None:
        settings = sanitised(reporting_settings(), side_effects="keep")
        dashboard, alert = task(settings, "dashboard").dashboard_task, task(settings, "alert").sql_task
        assert dashboard is not None and dashboard.subscription is not None
        assert alert is not None and alert.alert is not None
        assert dashboard.subscription.paused is None
        assert alert.alert.pause_subscriptions is None

    def test_sanitise_leaves_dashboards_without_a_subscription_alone(self) -> None:
        settings = reporting_settings()
        dashboard = task(settings, "dashboard").dashboard_task
        assert dashboard is not None
        dashboard.subscription = None
        assert task(sanitised(settings), "dashboard").dashboard_task == jobs.DashboardTask(
            dashboard_id="d1", warehouse_id="w1"
        )

    @pytest.mark.parametrize("side_effects", ["neutralise", "keep"])
    def test_sanitise_leaves_alert_and_power_bi_tasks_alone(self, side_effects: str) -> None:
        before = reporting_settings()
        after = sanitised(reporting_settings(), side_effects=side_effects)
        alert = task(after, "check").alert_task
        power_bi = task(after, "refresh_powerbi").power_bi_task
        assert alert is not None and power_bi is not None
        assert alert == task(before, "check").alert_task
        assert alert.subscribers == [jobs.AlertTaskSubscriber(user_name="someone@example.com")]
        assert power_bi == task(before, "refresh_powerbi").power_bi_task
        assert power_bi.refresh_after_update is True

    def test_sanitise_keeps_dashboard_filters(self) -> None:
        settings = reporting_settings()
        template = task(settings, "dashboard").dashboard_task
        assert template is not None
        template.filters = {"region": "emea"}
        dashboard = task(sanitised(settings), "dashboard").dashboard_task
        assert dashboard is not None and dashboard.subscription is not None
        assert dashboard.subscription.paused is True
        assert dashboard.filters == {"region": "emea"}


class TestNamesAndTags:
    """The job name, the required and attribution tags, and the platform's limits on them."""

    def test_long_node_ids_fit_names_and_tags(self) -> None:
        nodeid = "tests/test_päth.py::test_rows[" + "x" * 5000 + "]\nline two 中"
        settings = sanitised(tags={"pytest_nodeid": nodeid}, name=nodeid)
        assert settings.name is not None and settings.tags is not None
        assert len(settings.name.encode()) <= 4096
        assert "\n" not in settings.name
        value = settings.tags["pytest_nodeid"]
        assert len(value.encode()) <= 256
        assert value.startswith("tests/test_päth.py::test_rows[xxx")

    def test_tag_value(self) -> None:
        assert tag_value("a\r\nb 中文 é") == "a b ?? é"
        assert tag_value("é" * 200) == "é" * 128

    def test_attribution_tags_are_added(self) -> None:
        settings = sanitised(tags={**TAGS, **ATTRIBUTION})
        assert settings.tags == {"dev": "someone", "purpose": "pytest", **TAGS, **ATTRIBUTION}

    def test_attribution_tags_fit_within_the_cap(self, caplog: pytest.LogCaptureFixture) -> None:
        settings = example_settings()
        settings.tags = {f"t{i}": "" for i in range(20)}
        caplog.set_level("DEBUG", logger="pytest_databricks_lakeflow")
        tags = sanitised(settings, tags={**TAGS, **ATTRIBUTION}).tags or {}
        assert len(tags) == 25
        assert {key for key in tags if key in ATTRIBUTION} == {"pytest_ci_url", "pytest_user"}
        assert "pytest_git_sha" in caplog.text and "pytest_git_branch" in caplog.text

    def test_attribution_tags_never_push_out_the_required_tags(self) -> None:
        settings = example_settings()
        settings.tags = {f"t{i}": "" for i in range(22)}
        tags = sanitised(settings, tags={**TAGS, **ATTRIBUTION}).tags
        assert tags is not None
        assert len(tags) == 25
        assert set(TAGS) <= set(tags)
        assert not set(ATTRIBUTION) & set(tags)
        settings.tags = {f"t{i}": "" for i in range(23)}
        with pytest.raises(LakeflowError, match="26 tags, more than the 25 allowed. Remove 1"):
            sanitised(settings, tags={**TAGS, **ATTRIBUTION})

    def test_attribution_values_are_made_acceptable(self) -> None:
        tags = sanitised(tags={**TAGS, "pytest_git_branch": "feature/中\nx" + "y" * 300}).tags
        assert tags is not None
        branch = tags["pytest_git_branch"]
        assert branch.startswith("feature/? x")
        assert len(branch.encode()) <= 256
        branch.encode("latin-1")


class TestDescription:
    """The description names the test and comes before the template's description."""

    @pytest.mark.parametrize(
        ("template", "description", "expected"),
        [
            (
                "The bundle's job.",
                f"{NODEID} {ATTRIBUTION['pytest_ci_url']}",
                f"{NODEID} {ATTRIBUTION['pytest_ci_url']}\n\nThe bundle's job.",
            ),
            (None, NODEID, NODEID),
            (None, None, None),
        ],
    )
    def test_description_prepends_the_test(
        self, template: str | None, description: str | None, expected: str | None
    ) -> None:
        settings = example_settings()
        settings.description = template
        assert sanitised(settings, description=description).description == expected

    def test_long_descriptions_are_trimmed(self) -> None:
        settings = example_settings()
        settings.description = "中" * 20000
        description = sanitised(settings, description=NODEID).description
        assert description is not None
        assert len(description.encode()) <= 27700
        assert description.startswith("tests/test_x.py::test_y\n\n中")


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


class TestCreateChecks:
    """The checks before jobs.create: parameters, serverless compute, and the kwargs sent."""

    def test_check_params(self) -> None:
        settings = example_settings()
        check_params("my_job", settings, {"param1": "x", "schema": "y"}, allow_undeclared=False)
        with pytest.raises(
            LakeflowError, match=r"does not declare the parameters \['parm1'\]. Declared: \['param1'"
        ) as e:
            check_params("my_job", settings, {"parm1": "x"}, allow_undeclared=False)
        assert str(e.value).endswith("Pass allow_undeclared_params=True to Lakeflow to pass them anyway.")
        check_params("my_job", settings, {"parm1": "x"}, allow_undeclared=True)
        with pytest.raises(LakeflowError, match="reserved for the plugin"):
            check_params("my_job", settings, {"__pytest_values": "x"}, allow_undeclared=True)

    @pytest.mark.parametrize(
        "code",
        [
            {"python_wheel_task": {"package_name": "pkg", "entry_point": "main"}},
            {"spark_python_task": {"python_file": "/x.py"}},
            {"spark_jar_task": {"main_class_name": "x"}},
            {"dbt_task": {"commands": ["dbt run"]}},
        ],
    )
    def test_check_compute_names_tasks_without_environment(self, code: dict[str, Any]) -> None:
        with pytest.raises(LakeflowError, match=r"\['bare'\].*environment_key"):
            check_compute(with_tasks({"task_key": "bare", **code}))

    def test_check_compute_names_for_each_inner_tasks_without_environment(self) -> None:
        inner = {"task_key": "inner", "python_wheel_task": {"package_name": "pkg", "entry_point": "main"}}
        with pytest.raises(LakeflowError, match=r"\['inner'\].*environment_key"):
            check_compute(with_tasks(for_each(inner)))

    def test_check_compute_accepts_example_settings(self) -> None:
        check_compute(example_settings())
        wheel = {"package_name": "pkg", "entry_point": "main"}
        check_compute(with_tasks({"task_key": "c", "existing_cluster_id": "c-1", "python_wheel_task": wheel}))

    def test_create_kwargs(self) -> None:
        settings = sanitised()
        settings.triggers = []
        kwargs = create_kwargs(settings)
        assert "triggers" not in kwargs
        assert "deployment" not in kwargs
        assert kwargs["name"] == settings.name
        assert kwargs["tasks"] == settings.tasks
        assert set(kwargs) <= set(jobs.JobSettings.__dataclass_fields__)


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
        ("code", "value", "match"),
        [
            (
                None,
                ["--rows", "10"],
                r"task_params\['extract'\] must be a mapping of names to strings, merged into "
                r"notebook_task.base_parameters, for task 'extract'; got \['--rows', '10'\]",
            ),
            (None, {"limit": 10, 1: "x"}, r"must be a mapping of names to strings.*got \{'limit': 10, 1: 'x'\}"),
            (
                {"spark_python_task": {"python_file": "/x.py"}},
                {"rows": "10"},
                r"must be a list of strings, which replaces spark_python_task.parameters, for task 'extract'",
            ),
            (
                {"python_wheel_task": {"package_name": "pkg", "entry_point": "main"}},
                "--rows 10",
                r"must be a mapping of names to strings, merged into python_wheel_task.named_parameters, or a list "
                r"of strings, which replaces python_wheel_task.parameters, for task 'extract'; got '--rows 10'",
            ),
            (
                {"condition_task": {"op": "EQUAL_TO", "left": "a", "right": "b"}},
                {"limit": "10"},
                r"task_params cannot set the parameters of task 'extract': it is not a notebook, Python script, JAR, "
                r"Python wheel, SQL or dbt task",
            ),
        ],
        ids=["list-for-notebook", "not-strings", "mapping-for-script", "string-for-wheel", "condition"],
    )
    def test_apply_task_params_rejects_wrong_shape(self, code: dict[str, Any] | None, value: Any, match: str) -> None:
        settings = example_settings()
        if code is not None:
            others = [t for t in settings.tasks or [] if t.task_key != "extract"]
            settings.tasks = [jobs.Task.from_dict({"task_key": "extract", **code}), *others]
        before = copy.deepcopy(settings)
        with pytest.raises(LakeflowError, match=match):
            apply_task_params("my_job", settings, {"transform": {"limit": "10"}, "extract": value})
        assert settings == before

    def test_apply_task_params_rejects_unknown_and_reserved_keys(self) -> None:
        settings = inject_probes(example_settings(), {"transform": ["k"]}, HELPERS)
        available = r"Available: \['classic', 'extract', 'load', 'transform'\]"
        with pytest.raises(LakeflowError, match=rf"Unknown task 'nope' in task_params of job 'my_job'. {available}$"):
            apply_task_params("my_job", settings, {"nope": {"limit": "10"}})
        with pytest.raises(LakeflowError, match=rf"Unknown task '__pytest_probe__transform' .*{available}$"):
            apply_task_params("my_job", settings, {"__pytest_probe__transform": {"limit": "10"}})
        with pytest.raises(LakeflowError, match=r"Unknown task 'extract' in task_params .*Available: \['load'\]$"):
            apply_task_params("my_job", example_settings(), {"extract": {"limit": "10"}}, allowed=["load"])

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
        with pytest.raises(LakeflowError, match=r"reserved for the plugin: \['__pytest_values'\]"):
            check_task_params("my_job", settings, {"transform": {"__pytest_values": "{}"}}, {})


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


class TestInjectLifecycle:
    """inject_lifecycle runs setup before every root task and teardown after every leaf, whatever happened."""

    @staticmethod
    def dependencies(settings: jobs.JobSettings) -> dict[str, list[str]]:
        return {t.task_key: [d.task_key for d in t.depends_on or []] for t in settings.tasks or [] if t.task_key}

    def test_rewires_a_three_task_dag(self) -> None:
        settings = inject_lifecycle(three_tasks(), "/h/setup_1", "/h/teardown_1", HELPERS)
        assert self.dependencies(settings) == {
            "__pytest_setup": [],
            "a": ["__pytest_setup"],
            "b": ["a"],
            "c": ["__pytest_setup"],
            "__pytest_teardown": ["b", "c"],
        }
        setup, teardown = task(settings, "__pytest_setup"), task(settings, "__pytest_teardown")
        assert setup.run_if is None
        assert teardown.run_if == jobs.RunIf.ALL_DONE
        for injected, path in ((setup, "/h/setup_1"), (teardown, "/h/teardown_1")):
            assert injected.notebook_task == jobs.NotebookTask(
                notebook_path=path, base_parameters={"__pytest_params": '["day"]'}
            )
            assert injected.environment_key == ENV_KEY
        assert [e.environment_key for e in settings.environments or []] == [ENV_KEY]

    def test_teardown_follows_probes_and_stubs_follow_setup(self) -> None:
        settings = wrap_task(example_settings(), "transform", {"extract": {"row_count": 1}}, ["rows"], HELPERS)
        settings = inject_lifecycle(settings, "/h/setup_1", "/h/teardown_1", HELPERS)
        assert self.dependencies(settings) == {
            "__pytest_setup": [],
            "extract": ["__pytest_setup"],
            "transform": ["extract"],
            "__pytest_probe__transform": ["transform"],
            "__pytest_teardown": ["__pytest_probe__transform"],
        }
        assert [e.environment_key for e in settings.environments or []].count(ENV_KEY) == 1

    def test_either_alone(self) -> None:
        setup_only = inject_lifecycle(three_tasks(), "/h/setup_1", None, HELPERS)
        assert [t.task_key for t in setup_only.tasks or []] == ["__pytest_setup", "a", "b", "c"]
        teardown_only = inject_lifecycle(three_tasks(), None, "/h/teardown_1", HELPERS)
        assert self.dependencies(teardown_only) == {"a": [], "b": ["a"], "c": [], "__pytest_teardown": ["b", "c"]}
        assert inject_lifecycle(three_tasks(), None, None, HELPERS) == three_tasks()

    def test_on_the_helper_job_cluster(self) -> None:
        helpers = Helpers("/s", "/g", job_cluster_key="classic")
        settings = inject_lifecycle(example_settings(), "/h/setup_1", "/h/teardown_1", helpers)
        assert task(settings, "__pytest_setup").job_cluster_key == "classic"
        assert task(settings, "__pytest_teardown").job_cluster_key == "classic"
        assert ENV_KEY not in [e.environment_key for e in settings.environments or []]


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

    def test_run_job_tasks_skips_disabled_tasks_and_tasks_in_disabled_loops(self) -> None:
        settings = parent_settings()
        nested = jobs.Task(task_key="each_child", run_job_task=jobs.RunJobTask(job_id=456))
        loop = jobs.Task(task_key="loop", for_each_task=jobs.ForEachTask(inputs="[1, 2]", task=nested))
        settings.tasks = [*(settings.tasks or []), loop]
        assert [t.task_key for t in run_job_tasks(settings)] == ["trigger_child", "each_child"]
        assert [t.task_key for t in run_job_tasks(disable_tasks(settings, ["loop"]))] == ["trigger_child"]
        assert run_job_tasks(disable_tasks(settings, ["trigger_child", "loop"])) == []


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
        ("overrides", "match"),
        [
            pytest.param(
                {"nope": {"max_retries": 1}},
                r"Unknown task 'nope' in task_overrides. Available: \['classic', 'extract', 'load', 'transform'\]",
                id="task",
            ),
            pytest.param(
                {"load": {"max_retries": 1, "notebook_task": {}}},
                r"Unknown field 'notebook_task' in task_overrides\['load'\]. Allowed: \['max_retries', "
                r"'min_retry_interval_millis', 'retry_on_timeout', 'timeout_seconds'\]",
                id="field",
            ),
        ],
    )
    def test_override_tasks_rejects_unknown_task_or_field(
        self, overrides: dict[str, dict[str, object]], match: str
    ) -> None:
        settings = example_settings()
        with pytest.raises(LakeflowError, match=match):
            override_tasks(settings, overrides)
        assert settings == example_settings(), "nothing is changed when an entry is refused"


class TestAddDependencies:
    """add_dependencies appends test-only packages to every environment and to Python tasks on clusters."""

    def test_add_dependencies_to_environments_and_cluster_tasks(self) -> None:
        inner = {
            "task_key": "inner",
            "new_cluster": {"spark_version": "17.3.x"},
            "spark_python_task": {"python_file": "/x"},
        }
        settings = with_tasks(for_each(inner))
        settings = add_dependencies(settings, ["pytest>=8"])
        [default] = settings.environments or []
        assert default.spec is not None and default.spec.dependencies == ["pytest>=8"]
        pytest_library = compute.Library(pypi=compute.PythonPyPiLibrary(package="pytest>=8"))
        assert task(settings, "classic").libraries == [pytest_library]
        assert nested_task(settings, "loop").libraries == [pytest_library]
        for key in ("extract", "transform", "load", "loop"):
            assert not task(settings, key).libraries, key

    def test_add_dependencies_appends_to_what_the_template_declares(self) -> None:
        settings = example_settings()
        [default] = settings.environments or []
        assert default.spec is not None
        default.spec.dependencies = ["/Workspace/libs/pkg.whl"]
        task(settings, "classic").libraries = [compute.Library(whl="/Workspace/libs/pkg.whl")]
        add_dependencies(settings, ["pytest>=8", "chispa"])
        assert default.spec.dependencies == ["/Workspace/libs/pkg.whl", "pytest>=8", "chispa"]
        assert [library.as_dict() for library in task(settings, "classic").libraries or []] == [
            {"whl": "/Workspace/libs/pkg.whl"},
            {"pypi": {"package": "pytest>=8"}},
            {"pypi": {"package": "chispa"}},
        ]

    @pytest.mark.parametrize(
        ("dependency", "library"),
        [
            (
                "/Workspace/Shared/libs/helpers-0.1-py3-none-any.whl",
                compute.Library(whl="/Workspace/Shared/libs/helpers-0.1-py3-none-any.whl"),
            ),
            (
                "/Workspace/Users/user@example.com/libs/helpers-0.1-py3-none-any.whl",
                compute.Library(whl="/Workspace/Users/user@example.com/libs/helpers-0.1-py3-none-any.whl"),
            ),
            (
                "pkg @ https://host/pkg-1.0-py3-none-any.whl",
                compute.Library(pypi=compute.PythonPyPiLibrary(package="pkg @ https://host/pkg-1.0-py3-none-any.whl")),
            ),
            (
                "pkg[extra]@https://host/pkg-1.0-py3-none-any.whl",
                compute.Library(
                    pypi=compute.PythonPyPiLibrary(package="pkg[extra]@https://host/pkg-1.0-py3-none-any.whl")
                ),
            ),
        ],
        ids=["wheel path", "wheel path under a user folder", "direct reference", "direct reference without spaces"],
    )
    def test_add_dependencies_picks_the_library_type(self, dependency: str, library: compute.Library) -> None:
        settings = add_dependencies(example_settings(), [dependency])
        [default] = settings.environments or []
        assert default.spec is not None and default.spec.dependencies == [dependency]
        assert task(settings, "classic").libraries == [library]

    @pytest.mark.parametrize("dependency", ["./dist/*.whl[test]", "../dist/pkg-0.1-py3-none-any.whl", "dist/pkg.whl"])
    def test_add_dependencies_rejects_local_paths(self, dependency: str) -> None:
        settings = example_settings()
        with pytest.raises(LakeflowError, match=r"Local paths are not yet supported in extra_dependencies") as e:
            add_dependencies(settings, ["pytest", dependency])
        assert repr(dependency) in str(e.value)
        assert 'sync="deploy"' in str(e.value)
        assert settings == example_settings(), "nothing changes before the error"


class TestMergeEnvironments:
    """merge_environments merges per-job environment overrides into the copy, and refuses what would break it."""

    @staticmethod
    def with_dependencies() -> jobs.JobSettings:
        settings = example_settings()
        settings.environments = [
            jobs.JobEnvironment.from_dict(
                {"environment_key": "default", "spec": {"environment_version": "4", "dependencies": ["./dist/*.whl"]}}
            )
        ]
        return settings

    @staticmethod
    def specs(settings: jobs.JobSettings) -> dict[str, dict[str, Any]]:
        return {e.environment_key: e.spec.as_dict() if e.spec else {} for e in settings.environments or []}

    @pytest.mark.parametrize(
        ("override", "expected"),
        [
            ({"environment_version": "5"}, {"environment_version": "5", "dependencies": ["./dist/*.whl"]}),
            ({"dependencies": ["chispa"]}, {"environment_version": "4", "dependencies": ["chispa"]}),
        ],
        ids=["version", "dependencies-replaced"],
    )
    def test_merge_environments_replaces_fields_of_an_existing_key(
        self, override: dict[str, Any], expected: dict[str, Any]
    ) -> None:
        settings = merge_environments(self.with_dependencies(), {"default": override})
        assert self.specs(settings) == {"default": expected}

    def test_merge_environments_appends_a_new_key(self) -> None:
        gpu = {"environment_version": "4", "dependencies": ["torch"]}
        settings = merge_environments(example_settings(), {"gpu": gpu})
        assert self.specs(settings) == {"default": {"environment_version": "4"}, "gpu": gpu}
        assert merge_environments(example_settings(), {}) == example_settings()

    def test_merge_environments_rejects_reserved_keys(self) -> None:
        settings = example_settings()
        with pytest.raises(LakeflowError, match=r"starting with '__pytest_' are reserved.*\['__pytest_serverless'\]"):
            merge_environments(settings, {"__pytest_serverless": {"environment_version": "5"}})
        assert settings == example_settings(), "nothing changes before the error"

    def test_merge_environments_rejects_unknown_fields(self) -> None:
        settings = example_settings()
        with pytest.raises(LakeflowError, match=r"environments\['default'\] has fields.*\['version'\]") as e:
            merge_environments(settings, {"default": {"version": "5", "dependencies": ["x"]}})
        assert "environment_version" in str(e.value), "the error lists the fields that are accepted"
        assert settings == example_settings(), "nothing changes before the error"

    @pytest.mark.parametrize("override", [None, "5", ["dependencies"]], ids=["none", "string", "list"])
    def test_merge_environments_rejects_an_override_that_is_not_a_mapping(self, override: Any) -> None:
        settings = example_settings()
        with pytest.raises(LakeflowError, match=r"environments\['default'\] must be a mapping of spec fields"):
            merge_environments(settings, {"default": override})
        assert settings == example_settings(), "nothing changes before the error"

    def test_merge_environments_rejects_version_and_base_environment_together(self) -> None:
        base = {"base_environment": "/Workspace/envs/base.yml"}
        with pytest.raises(LakeflowError, match=r"environments\['default'\] would set both environment_version and"):
            merge_environments(example_settings(), {"default": base})
        with pytest.raises(LakeflowError, match=r"environments\['new'\] would set both"):
            merge_environments(example_settings(), {"new": {**base, "environment_version": "5"}})
        settings = merge_environments(example_settings(), {"default": {**base, "environment_version": None}})
        assert self.specs(settings) == {"default": base}


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
        ("disable", "outcomes", "match"),
        [
            ([], {"on_true": True}, r"Task 'on_true' in force_conditions has no condition_task. .*\['check'\]"),
            ([], {"nope": True}, r"Unknown task 'nope' in force_conditions. Available: \['check', 'check_input'"),
            ([], {"check": "true"}, r"force_conditions\['check'\] must be True or False, got 'true'"),
            (["check"], {"check": True}, "Task 'check' in force_conditions is disabled"),
        ],
    )
    def test_force_conditions_rejects(self, disable: list[str], outcomes: dict[str, Any], match: str) -> None:
        settings = disable_tasks(branching_settings(), disable)
        with pytest.raises(LakeflowError, match=match):
            force_conditions(settings, {"check": False, **outcomes})
        assert settings == disable_tasks(branching_settings(), disable)

    def test_force_conditions_without_outcomes_changes_nothing(self) -> None:
        assert force_conditions(example_settings(), {}) == example_settings()


class TestPipelineParams:
    """pipeline_params builds the refresh options of a run and checks they can apply."""

    @staticmethod
    def refresh(settings: jobs.JobSettings, **kwargs: Any) -> jobs.PipelineParams | None:
        return pipeline_params(
            settings,
            full_refresh=kwargs.get("full_refresh"),
            full_refresh_selection=kwargs.get("full_refresh_selection"),
            refresh_selection=kwargs.get("refresh_selection"),
        )

    def test_pipeline_params_full_refresh(self) -> None:
        assert self.refresh(pipeline_settings(), full_refresh=True) == jobs.PipelineParams(full_refresh=True)

    @NEEDS_REFRESH_SELECTION
    def test_pipeline_params_selections(self) -> None:
        assert self.refresh(pipeline_settings(), refresh_selection=["bronze_orders"]) == jobs.PipelineParams(
            refresh_selection=["bronze_orders"]
        )
        assert self.refresh(pipeline_settings(), full_refresh_selection=("silver",)) == jobs.PipelineParams(
            full_refresh_selection=["silver"]
        )

    def test_pipeline_params_none_without_arguments(self) -> None:
        assert self.refresh(pipeline_settings()) is None
        assert self.refresh(example_settings()) is None

    def test_pipeline_params_selections_need_a_recent_sdk(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(settings_module, "PIPELINE_PARAMS_FIELDS", frozenset({"full_refresh"}))
        assert self.refresh(pipeline_settings(), full_refresh=True) == jobs.PipelineParams(full_refresh=True)
        message = (
            r"full_refresh_selection and refresh_selection need databricks-sdk 0.110.0 or later; "
            r"got \['refresh_selection'\]"
        )
        with pytest.raises(LakeflowError, match=message):
            self.refresh(pipeline_settings(), refresh_selection=["bronze"])

    @pytest.mark.parametrize("name", ["full_refresh_selection", "refresh_selection"])
    def test_pipeline_params_reject_a_bare_string_selection(self, name: str) -> None:
        with pytest.raises(LakeflowError, match=rf"{name} must be a list of table names, not a string: 'bronze'"):
            self.refresh(pipeline_settings(), **{name: "bronze"})

    def test_pipeline_params_need_a_pipeline_task(self) -> None:
        with pytest.raises(LakeflowError, match=r"has no pipeline task.*\['classic', 'extract', 'load', 'transform'\]"):
            self.refresh(example_settings(), full_refresh=True)

    def test_pipeline_params_count_a_for_each_inner_pipeline_task(self) -> None:
        inner = {"task_key": "each_inner", "pipeline_task": {"pipeline_id": "abc"}}
        settings = jobs.JobSettings.from_dict({"tasks": [for_each(inner)]})
        assert self.refresh(settings, full_refresh=False) == jobs.PipelineParams(full_refresh=False)
