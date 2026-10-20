"""Sanitising a template into the copy's settings: stripping, naming, tagging, compute overrides and timeouts."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from datetime import timedelta
from typing import Any

import pytest
from databricks.sdk.service import compute, jobs

from pytest_databricks_lakeflow import LakeflowError
from pytest_databricks_lakeflow.naming import ENV_KEY, tag_value
from pytest_databricks_lakeflow.settings.compute import check_compute
from pytest_databricks_lakeflow.settings.params import check_params
from pytest_databricks_lakeflow.settings.selection import check_trigger_references
from pytest_databricks_lakeflow.settings.template import create_kwargs, sanitise

from .conftest import ATTRIBUTION, NODEID, TAGS, for_each, nested_task, task, with_tasks
from .fakes.templates import (
    example_settings,
    for_each_settings,
    parent_settings,
    pipeline_settings,
    reporting_settings,
    triggered_settings,
)

CLUSTER = "0123-456789-abcdef"
SINGLE_NODE_CONF = {"spark.master": "local[*, 4]", "spark.databricks.cluster.profile": "singleNode"}


def sanitised(
    settings: jobs.JobSettings | None = None, tags: dict[str, str] = TAGS, name: str = NODEID, **kwargs: Any
) -> jobs.JobSettings:
    options: dict[str, Any] = {"disable_auto_optimization": True, "performance_optimized": False, **kwargs}
    return sanitise(settings or example_settings(), name, tags, **options)


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

    @pytest.mark.parametrize("disable", [True, False])
    def test_auto_optimisation_reaches_tasks_nested_in_a_for_each_task(self, disable: bool) -> None:
        # Without it on the nested task, serverless retried each failing iteration once.
        settings = for_each_settings()
        on_cluster = {"task_key": "classic_each", "job_cluster_key": "c", "notebook_task": {"notebook_path": "/x"}}
        settings.tasks = [*(settings.tasks or []), jobs.Task.from_dict({**for_each(on_cluster), "task_key": "c_loop"})]
        sanitised(settings, disable_auto_optimization=disable)
        assert nested_task(settings, "loop").disable_auto_optimization is (True if disable else None)
        assert nested_task(settings, "c_loop").disable_auto_optimization is None
        assert task(settings, "loop").disable_auto_optimization is None


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
        with pytest.raises(ValueError, match="reserved for the plugin"):
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

    @pytest.mark.parametrize(
        ("template", "match"),
        [
            pytest.param(
                triggered_settings,
                r"Task 'transform' of 'my_job' refers to \{\{job.trigger.table_update.updated_tables\}\}, but the "
                r"copy has no table update trigger, and jobs/create refuses the reference without one. Set its value "
                r"with trigger=\{'table_update.updated_tables': ...\} on job\(\) or task\(\).",
                id="table-update",
            ),
            pytest.param(
                lambda: with_tasks(
                    for_each(
                        {
                            "task_key": "inner",
                            "notebook_task": {
                                "notebook_path": "/n",
                                "base_parameters": {"at": "{{ job.trigger.file_arrival.location }}"},
                            },
                        }
                    )
                ),
                r"Task 'inner' of 'my_job' refers to \{\{ job.trigger.file_arrival.location \}\}, but the copy has no "
                r"file arrival trigger",
                id="nested-file-arrival",
            ),
            pytest.param(
                lambda: replace(
                    with_tasks({"task_key": "t", "notebook_task": {"notebook_path": "/n"}}),
                    parameters=[
                        jobs.JobParameterDefinition(name="day", default="{{job.trigger.time.iso_date}}"),
                        jobs.JobParameterDefinition(name="landed", default="{{job.trigger.file_arrival.location}}"),
                    ],
                ),
                r"Job parameter 'landed' of 'my_job' refers to \{\{job.trigger.file_arrival.location\}\}, but the copy "
                r"has no file arrival trigger, and jobs/create refuses the reference without one. Set its value with "
                r"trigger=\{'file_arrival.location': ...\} on job\(\) or task\(\).",
                id="parameter-default",
            ),
        ],
    )
    def test_check_trigger_references_refuses_references_the_copy_cannot_resolve(
        self, template: Callable[[], jobs.JobSettings], match: str
    ) -> None:
        with pytest.raises(LakeflowError, match=match):
            check_trigger_references("my_job", template())

    def test_check_trigger_references_accepts_what_jobs_create_accepts(self) -> None:
        run_now = {
            "notebook_path": "/n",
            "base_parameters": {"k": "{{job.trigger.type}}", "d": "{{job.trigger.time.iso_date}}"},
        }
        check_trigger_references("my_job", with_tasks({"task_key": "t", "notebook_task": run_now}))
        settings = triggered_settings()
        settings.trigger = jobs.TriggerSettings(
            table_update=jobs.TableUpdateTriggerConfiguration(table_names=["c.s.t"])
        )
        check_trigger_references("my_job", settings)

    def test_create_kwargs(self) -> None:
        settings = sanitised()
        settings.triggers = []
        kwargs = create_kwargs(settings)
        assert "triggers" not in kwargs
        assert "deployment" not in kwargs
        assert kwargs["name"] == settings.name
        assert kwargs["tasks"] == settings.tasks
        assert set(kwargs) <= set(jobs.JobSettings.__dataclass_fields__)
