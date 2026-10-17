from __future__ import annotations

from typing import Any

import pytest
from databricks.sdk.service import jobs

from pytest_databricks_lakeflow.errors import LakeflowError
from pytest_databricks_lakeflow.settings import check_params, create_kwargs, sanitise, select, tag_value

from .fakes import example_settings

TAGS = {"pytest": "", "pytest_run_id": "r1", "pytest_nodeid": "tests/test_x.py::test_y"}


def sanitised(settings: jobs.JobSettings | None = None, **kwargs: Any) -> jobs.JobSettings:
    options = {"disable_auto_optimization": True, "performance_optimized": False, **kwargs}
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
    with pytest.raises(LakeflowError, match=r"does not declare the parameters \['parm1'\]. Declared: \['param1'"):
        check_params("my_job", settings, {"parm1": "x"}, allow_undeclared=False)
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
