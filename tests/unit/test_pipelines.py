"""Pipeline tasks and pipeline copies: refresh options, `lakeflow.pipeline()` and `pipelines="copy"`."""

from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest
from databricks.sdk.service import jobs, pipelines

from pytest_databricks_lakeflow import LakeflowError
from pytest_databricks_lakeflow.settings import params as params_module
from pytest_databricks_lakeflow.settings.params import pipeline_params
from pytest_databricks_lakeflow.settings.tasks import pipeline_task_ids, rewire_pipelines
from pytest_databricks_lakeflow.settings.template import PIPELINE_CREATE_PARAMETERS, sanitise_pipeline

from .conftest import (
    ATTRIBUTION,
    NEEDS_REFRESH_SELECTION,
    TAGS,
    FakeCli,
    configure_lakeflow,
    deny_create,
    for_each,
    nested_task,
    run_tests,
)
from .fakes.runs import HOST
from .fakes.templates import example_settings, pipeline_settings, pipeline_spec
from .fakes.workspace import FakeWorkspaceClient

SELECTIONS = ["full_refresh_selection", "refresh_selection", "refresh_flow_selection", "reset_checkpoint_selection"]


class TestPipelineParams:
    """pipeline_params builds the refresh options of a run and checks they can apply."""

    @staticmethod
    def refresh(settings: jobs.JobSettings, **kwargs: Any) -> jobs.PipelineParams | None:
        return pipeline_params(
            settings,
            full_refresh=kwargs.get("full_refresh"),
            full_refresh_selection=kwargs.get("full_refresh_selection"),
            refresh_selection=kwargs.get("refresh_selection"),
            refresh_flow_selection=kwargs.get("refresh_flow_selection"),
            reset_checkpoint_selection=kwargs.get("reset_checkpoint_selection"),
        )

    def test_pipeline_params_full_refresh(self) -> None:
        assert self.refresh(pipeline_settings(), full_refresh=True) == jobs.PipelineParams(full_refresh=True)

    @NEEDS_REFRESH_SELECTION
    @pytest.mark.parametrize("name", SELECTIONS)
    def test_pipeline_params_selections(self, name: str) -> None:
        expected = jobs.PipelineParams.from_dict({name: ["bronze_orders"]})
        assert self.refresh(pipeline_settings(), **{name: ("bronze_orders",)}) == expected

    def test_pipeline_params_none_without_arguments(self) -> None:
        assert self.refresh(pipeline_settings()) is None
        assert self.refresh(example_settings()) is None

    def test_pipeline_params_selections_need_a_recent_sdk(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(params_module, "PIPELINE_PARAMS_FIELDS", frozenset({"full_refresh"}))
        assert self.refresh(pipeline_settings(), full_refresh=True) == jobs.PipelineParams(full_refresh=True)
        message = (
            r"full_refresh_selection, refresh_selection, refresh_flow_selection and reset_checkpoint_selection need "
            r"databricks-sdk 0.110.0 or later; got \['refresh_selection', 'reset_checkpoint_selection'\]"
        )
        with pytest.raises(LakeflowError, match=message):
            self.refresh(pipeline_settings(), refresh_selection=["bronze"], reset_checkpoint_selection=["t"])

    @pytest.mark.parametrize("name", SELECTIONS)
    def test_pipeline_params_reject_a_bare_string_selection(self, name: str) -> None:
        with pytest.raises(ValueError, match=rf"{name} must be a list of names, not a string: 'bronze'"):
            self.refresh(pipeline_settings(), **{name: "bronze"})

    def test_pipeline_params_need_a_pipeline_task(self) -> None:
        with pytest.raises(LakeflowError, match=r"has no pipeline task.*\['classic', 'extract', 'load', 'transform'\]"):
            self.refresh(example_settings(), full_refresh=True)

    def test_pipeline_params_count_a_for_each_inner_pipeline_task(self) -> None:
        inner = {"task_key": "each_inner", "pipeline_task": {"pipeline_id": "abc"}}
        settings = jobs.JobSettings.from_dict({"tasks": [for_each(inner)]})
        assert self.refresh(settings, full_refresh=False) == jobs.PipelineParams(full_refresh=False)


class TestSanitisePipeline:
    """sanitise_pipeline turns a deployed pipeline's spec into a copy that publishes to the test's schema."""

    def test_sanitise_pipeline_builds_the_create_kwargs(self) -> None:
        spec = pipeline_spec()
        kwargs = sanitise_pipeline(spec, "test_x", TAGS, schema="cat.pytest_x")
        assert set(kwargs) <= PIPELINE_CREATE_PARAMETERS
        assert (kwargs["catalog"], kwargs["schema"]) == ("cat", "pytest_x")
        assert (kwargs["development"], kwargs["allow_duplicate_names"]) == (True, True)
        assert kwargs["name"] == "[pytest] test_x"
        for name in ("continuous", "trigger", "notifications", "event_log", "deployment", "id", "target"):
            assert name not in kwargs
        assert kwargs["libraries"] == spec.libraries
        assert (kwargs["serverless"], kwargs["root_path"], kwargs["channel"]) == (True, spec.root_path, "CURRENT")
        assert kwargs["tags"] == {"dev": "someone", **TAGS}
        assert kwargs["configuration"] == {"schema": "prod"}
        assert "budget_policy_id" not in kwargs
        assert spec == pipeline_spec()

    def test_sanitise_pipeline_keeps_the_publishing_mode(self) -> None:
        spec = replace(pipeline_spec(), schema=None, target="prod")
        kwargs = sanitise_pipeline(spec, "test_x", TAGS, schema="cat.pytest_x")
        assert (kwargs["catalog"], kwargs["target"]) == ("cat", "pytest_x")
        assert "schema" not in kwargs

    @pytest.mark.parametrize(
        ("catalog", "schema", "error", "match"),
        [
            pytest.param(
                None,
                "cat.pytest_x",
                LakeflowError,
                r"Pipeline '\[dev someone\] my_pipeline' publishes to the Hive metastore \(no catalog\), so it cannot "
                r"be copied into a Unity Catalog schema",
                id="hive_metastore",
            ),
            pytest.param(
                "prod_catalog",
                "pytest_x",
                ValueError,
                r"schema must be a full name, catalog.schema, as lakeflow.schema\(\) yields: 'pytest_x'",
                id="no_catalog_in_schema",
            ),
        ],
    )
    def test_sanitise_pipeline_refuses(
        self, catalog: str | None, schema: str, error: type[Exception], match: str
    ) -> None:
        spec = replace(pipeline_spec(), catalog=catalog)
        with pytest.raises(error, match=match):
            sanitise_pipeline(spec, "test_x", TAGS, schema=schema)

    def test_sanitise_pipeline_merges_configuration_and_sets_the_budget_policy(self) -> None:
        configuration = {"schema": "cat.pytest_x", "rows": "3"}
        kwargs = sanitise_pipeline(
            pipeline_spec(), "test_x", TAGS, schema="cat.pytest_x", configuration=configuration, budget_policy_id="bp"
        )
        assert kwargs["configuration"] == configuration
        assert kwargs["budget_policy_id"] == "bp"

    def test_sanitise_pipeline_names_fit_the_limit(self) -> None:
        nodeid = "tests/test_päth.py::test_rows[" + "é" * 300 + "]\nline two"
        kwargs = sanitise_pipeline(pipeline_spec(), nodeid, {"pytest_nodeid": nodeid}, schema="cat.pytest_x")
        assert len(kwargs["name"]) == 255
        assert "\n" not in kwargs["name"]
        assert kwargs["name"].startswith("[pytest] tests/test_päth.py::test_rows[é")
        assert len(kwargs["tags"]["pytest_nodeid"].encode()) <= 256

    def test_sanitise_pipeline_tags_share_the_job_limit(self) -> None:
        spec = replace(pipeline_spec(), tags={f"t{i}": "" for i in range(20)})
        tags = sanitise_pipeline(spec, "test_x", {**TAGS, **ATTRIBUTION}, schema="cat.pytest_x")["tags"]
        assert len(tags) == 25
        assert {key for key in tags if key in ATTRIBUTION} == {"pytest_ci_url", "pytest_user"}
        spec.tags = {f"t{i}": "" for i in range(23)}
        with pytest.raises(LakeflowError, match="The pipeline would have 26 tags, more than the 25 allowed. Remove 1"):
            sanitise_pipeline(spec, "test_x", TAGS, schema="cat.pytest_x")


class TestPipelineTasks:
    """Which pipelines a job copy copies, and pointing its pipeline tasks at the copies."""

    @staticmethod
    def pipeline_job() -> jobs.JobSettings:
        """Two tasks on `abc`, a disabled task and one downstream of it, and a loop over a pipeline task."""
        return jobs.JobSettings.from_dict(
            {
                "tasks": [
                    {"task_key": "refresh", "pipeline_task": {"pipeline_id": "abc"}},
                    {
                        "task_key": "again",
                        "depends_on": [{"task_key": "refresh"}],
                        "pipeline_task": {"pipeline_id": "abc"},
                    },
                    {"task_key": "off", "disabled": True, "pipeline_task": {"pipeline_id": "off"}},
                    {
                        "task_key": "after_off",
                        "depends_on": [{"task_key": "off"}],
                        "pipeline_task": {"pipeline_id": "after"},
                    },
                    for_each({"task_key": "each_inner", "pipeline_task": {"pipeline_id": "inner"}}),
                    {"task_key": "check", "notebook_task": {"notebook_path": "/n"}},
                ]
            }
        )

    def test_pipeline_task_ids_leave_out_disabled_tasks_and_repeats(self) -> None:
        assert pipeline_task_ids(self.pipeline_job()) == ["abc", "inner"]
        assert pipeline_task_ids(example_settings()) == []

    def test_rewire_pipelines_rewrites_nested_tasks_too(self) -> None:
        settings = self.pipeline_job()
        rewire_pipelines(settings, {"abc": "p-1000", "inner": "p-1001"})
        pipeline_ids = []
        for each in settings.tasks or []:
            if each.pipeline_task is not None:
                pipeline_ids.append(each.pipeline_task.pipeline_id)
        assert pipeline_ids == ["p-1000", "p-1000", "off", "after"]
        inner = nested_task(settings, "loop").pipeline_task
        assert inner is not None and inner.pipeline_id == "p-1001"


PIPELINE_URL = f"{HOST}/pipelines/p-1000"
PIPELINE_ERROR_TEST = """
import pytest

from pytest_databricks_lakeflow import LakeflowBundleError, LakeflowError

def test_job(lakeflow):
    with pytest.raises({error}) as e:
        {call}
    assert e.value.args[0] == {message!r}
"""


def deploy_pipeline(ws: FakeWorkspaceClient, cli: FakeCli, settings: jobs.JobSettings | None = None) -> None:
    """Deploy pipeline `abc` as `my_pipeline`, and `pipeline_job`, by default with one task, `refresh`, updating it."""
    cli.deployed_pipelines["my_pipeline"] = "abc"
    ws.pipelines.specs["abc"] = pipeline_spec()
    cli.deployed["pipeline_job"] = ws.jobs.add(settings or pipeline_settings())


def update_in(state: str) -> pipelines.UpdateInfo:
    return pipelines.UpdateInfo(state=pipelines.UpdateInfoState(state))


def pipeline_ids_by_task(created: dict[str, Any]) -> dict[str, str | None]:
    ids: dict[str, str | None] = {}
    for each in created["tasks"]:
        if each.pipeline_task is not None:
            ids[each.task_key] = each.pipeline_task.pipeline_id
    return ids


@pytest.mark.usefixtures("fake_cli", "bundle_dir")
class TestEphemeralPipeline:
    """lakeflow.pipeline() copies a deployed pipeline into a schema, runs updates of it and deletes it."""

    def test_copy_publishes_to_the_schema_and_goes_before_it(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli
    ) -> None:
        deploy_pipeline(fake_ws, fake_cli)
        configure_lakeflow(pytester, 'catalog="c"')
        result = run_tests(
            pytester,
            """
            import pytest

            from pytest_databricks_lakeflow import LakeflowError

            def test_pipeline(lakeflow):
                with lakeflow.schema() as s, lakeflow.pipeline("my_pipeline", schema=s) as p:
                    assert (p.pipeline_key, p.source_id, p.schema) == ("my_pipeline", "abc", s)
                    assert p.url == "https://example.cloud.databricks.com/pipelines/p-1000"
                    update = p.run(full_refresh=True)
                    assert (update.pipeline_id, update.update_id, update.state) == ("p-1000", "u-1", "COMPLETED")
                    assert p.updates == [update]
                with pytest.raises(LakeflowError, match="has already been closed"):
                    p.run()
            """,
        )
        result.assert_outcomes(passed=1)
        [schema] = fake_ws.schemas.created
        [created] = fake_ws.pipelines.created
        assert (created["catalog"], created["schema"]) == ("c", schema.split(".")[1])
        assert created["name"] == "[pytest] test_jobs.py::test_pipeline"
        assert created["tags"]["pytest_nodeid"] == "test_jobs.py::test_pipeline"
        assert created["development"] is True
        [call] = fake_ws.pipelines.start_update_calls
        assert (call["pipeline_id"], call["full_refresh"]) == ("p-1000", True)
        assert [deleted["pipeline_id"] for deleted in fake_ws.pipelines.deleted] == ["p-1000"]
        assert fake_ws.deletions == ["pipeline p-1000", f"schema {schema}"]

    @pytest.mark.parametrize(("passes", "deleted"), [(True, ["p-1000"]), (False, [])], ids=["passed", "failed"])
    def test_plain_call_closes_at_teardown(
        self,
        pytester: pytest.Pytester,
        fake_ws: FakeWorkspaceClient,
        fake_cli: FakeCli,
        passes: bool,
        deleted: list[str],
    ) -> None:
        deploy_pipeline(fake_ws, fake_cli)
        result = run_tests(
            pytester,
            f"""
            def test_pipeline(lakeflow):
                unused = lakeflow.pipeline("my_pipeline", schema="c.s")
                p = lakeflow.pipeline("my_pipeline", schema="c.s")
                assert p.pipeline_id is None
                p.run(refresh_selection=["numbers"])
                assert p.pipeline_id == "p-1000"
                assert {passes}
            """,
        )
        result.assert_outcomes(passed=int(passes), failed=int(not passes))
        assert len(fake_ws.pipelines.created) == 1
        assert fake_ws.pipelines.start_update_calls[0]["refresh_selection"] == ["numbers"]
        assert [each["pipeline_id"] for each in fake_ws.pipelines.deleted] == deleted

    def test_failed_update_is_returned_and_its_states_logged(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli
    ) -> None:
        deploy_pipeline(fake_ws, fake_cli)
        states = ["WAITING_FOR_RESOURCES", "WAITING_FOR_RESOURCES", "RUNNING", "FAILED"]
        fake_ws.pipelines.update_polls["p-1000"] = [update_in(state) for state in states]
        source = """
            def test_pipeline(lakeflow):
                with lakeflow.pipeline("my_pipeline", schema="c.s") as p:
                    assert p.run().state == "FAILED"
        """
        result = run_tests(pytester, source, "--log-cli-level=INFO")
        result.assert_outcomes(passed=1)
        lines = [
            "pipeline my_pipeline: WAITING_FOR_RESOURCES",
            "pipeline my_pipeline: WAITING_FOR_RESOURCES -> RUNNING",
            "pipeline my_pipeline: RUNNING -> FAILED",
        ]
        result.stdout.fnmatch_lines([f"*INFO*pytest_databricks_lakeflow*{line}" for line in lines])
        assert [each["pipeline_id"] for each in fake_ws.pipelines.deleted] == ["p-1000"]

    def test_timeout_stops_the_update(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli
    ) -> None:
        deploy_pipeline(fake_ws, fake_cli)
        fake_ws.pipelines.update_polls["p-1000"] = [update_in("RUNNING")]
        message = f"Update u-1 of pipeline 'my_pipeline' did not finish within 0:00:00 and was stopped: {PIPELINE_URL}"
        result = run_tests(
            pytester,
            f"""
            from datetime import timedelta

            import pytest

            from pytest_databricks_lakeflow import LakeflowTimeout

            def test_pipeline(lakeflow):
                with lakeflow.pipeline("my_pipeline", schema="c.s") as p:
                    with pytest.raises(LakeflowTimeout) as e:
                        p.run(timeout=timedelta(0))
                    assert lakeflow.active_updates == {{}}
                assert str(e.value) == {message!r}
            """,
        )
        result.assert_outcomes(passed=1)
        assert fake_ws.pipelines.stopped == ["p-1000"]
        assert [each["pipeline_id"] for each in fake_ws.pipelines.deleted] == ["p-1000"]

    @pytest.mark.parametrize(
        ("arguments", "key", "error", "message"),
        [
            pytest.param(
                "", "nope", "KeyError", "No pipeline 'nope' in the bundle. Available: ['gone', 'my_pipeline']", id="key"
            ),
            pytest.param(
                'sync="none"',
                "my_pipeline",
                "LakeflowBundleError",
                'Pipelines need Lakeflow(sync="deploy"), which reads their deployed specs.',
                id="sync",
            ),
            pytest.param(
                "",
                "new",
                "LakeflowBundleError",
                "Pipeline 'new' is not deployed. Run `databricks bundle deploy` or use Lakeflow(sync=\"deploy\").",
                id="not_deployed",
            ),
            pytest.param(
                "",
                "gone",
                "LakeflowBundleError",
                "Pipeline 'gone' (id zzz) does not exist in the workspace, so the bundle state is stale. Run "
                "`databricks bundle deploy` again.",
                id="stale",
            ),
        ],
    )
    def test_refused_before_anything_is_created(
        self,
        pytester: pytest.Pytester,
        fake_ws: FakeWorkspaceClient,
        fake_cli: FakeCli,
        arguments: str,
        key: str,
        error: str,
        message: str,
    ) -> None:
        deploy_pipeline(fake_ws, fake_cli)
        fake_cli.deployed_pipelines.update({"new": "", "gone": "zzz"})
        configure_lakeflow(pytester, arguments)
        call = f'lakeflow.pipeline("{key}", schema="c.s")'
        result = run_tests(pytester, PIPELINE_ERROR_TEST.format(error=error, call=call, message=message))
        result.assert_outcomes(passed=1)
        assert fake_ws.pipelines.created == []

    def test_failed_test_keeps_the_copy_and_lists_it(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli
    ) -> None:
        deploy_pipeline(fake_ws, fake_cli)
        source = """
            def test_pipeline(lakeflow):
                with lakeflow.pipeline("my_pipeline", schema="c.s") as p:
                    p.run()
                    assert False
        """
        result = run_tests(pytester, source)
        result.assert_outcomes(failed=1)
        assert fake_ws.pipelines.deleted == []
        result.stdout.fnmatch_lines([f"*Keeping pipeline p-1000 from my_pipeline: {PIPELINE_URL}*"])
        result.stdout.fnmatch_lines([f"lakeflow_pipeline_url: {PIPELINE_URL}", "lakeflow_pipeline_id: p-1000"])
        result.stdout.fnmatch_lines(["*= lakeflow =*", "lakeflow: 1 pipeline kept", f"  {PIPELINE_URL}"])


@pytest.mark.usefixtures("fake_cli", "bundle_dir")
class TestPipelineCopies:
    """job() and task() with pipelines="copy" point each pipeline task at a copy that publishes to the schema."""

    @pytest.mark.parametrize(
        "call",
        [
            'lakeflow.job("pipeline_job", pipelines="copy", schema=s)',
            'lakeflow.task("pipeline_job.refresh", pipelines="copy", schema=s)',
        ],
        ids=["job", "task"],
    )
    def test_job_copy_points_at_a_pipeline_copy(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, call: str
    ) -> None:
        deploy_pipeline(fake_ws, fake_cli)
        configure_lakeflow(pytester, 'catalog="c"')
        result = run_tests(
            pytester,
            f"""
            def test_job(lakeflow):
                with lakeflow.schema() as s, {call} as job:
                    assert [copy.pipeline_id for copy in job.pipeline_copies] == ["p-1000"]
                    assert job.run().succeeded
            """,
        )
        result.assert_outcomes(passed=1)
        [schema] = fake_ws.schemas.created
        [created] = fake_ws.pipelines.created
        assert created["name"] == "[pytest] test_jobs.py::test_job my_pipeline"
        assert created["schema"] == schema.split(".")[1]
        [job] = fake_ws.jobs.created
        assert pipeline_ids_by_task(job) == {"refresh": "p-1000"}
        assert fake_ws.deletions == ["job 1002", "pipeline p-1000", f"schema {schema}"]

    def test_one_copy_per_pipeline(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli
    ) -> None:
        settings = pipeline_settings()
        settings.tasks = [
            *(settings.tasks or []),
            jobs.Task(task_key="again", pipeline_task=jobs.PipelineTask(pipeline_id="abc")),
            jobs.Task(task_key="other", pipeline_task=jobs.PipelineTask(pipeline_id="xyz")),
        ]
        deploy_pipeline(fake_ws, fake_cli, settings)
        fake_ws.pipelines.specs["xyz"] = replace(pipeline_spec(), id="xyz", name="other")
        source = """
            def test_job(lakeflow):
                with lakeflow.job("pipeline_job", name="copies", pipelines="copy", schema="c.s") as job:
                    job.run()
        """
        run_tests(pytester, source).assert_outcomes(passed=1)
        names = [created["name"] for created in fake_ws.pipelines.created]
        assert names == ["[pytest] copies my_pipeline", "[pytest] copies xyz"]
        [job] = fake_ws.jobs.created
        assert pipeline_ids_by_task(job) == {"refresh": "p-1000", "again": "p-1000", "other": "p-1001"}
        assert fake_ws.deletions == ["job 1002", "pipeline p-1000", "pipeline p-1001"]

    @pytest.mark.parametrize(
        ("call", "error", "message"),
        [
            pytest.param(
                'lakeflow.job("pipeline_job", pipelines="copy")',
                "ValueError",
                'pipelines="copy" needs schema: pass the name that lakeflow.schema() yields',
                id="no_schema",
            ),
            pytest.param(
                'lakeflow.job("pipeline_job", schema="c.s")',
                "ValueError",
                'schema applies to pipeline copies: pass pipelines="copy" as well',
                id="schema_without_copy",
            ),
            pytest.param(
                'lakeflow.job("my_job", pipelines="copy", schema="c.s")',
                "LakeflowError",
                "Job 'my_job' has no pipeline task to copy. Tasks: ['classic', 'extract', 'load', 'transform']",
                id="no_pipeline_task",
            ),
            pytest.param(
                'lakeflow.task("pipeline_job.check", pipelines="copy", schema="c.s")',
                "LakeflowError",
                "Job 'pipeline_job' has no pipeline task to copy. Tasks: ['check']",
                id="task_without_pipeline",
            ),
            pytest.param(
                'lakeflow.job("pipeline_job", pipelines="copies", schema="c.s")',
                "ValueError",
                "pipelines must be one of deployed, copy, got 'copies'",
                id="unknown_value",
            ),
        ],
    )
    def test_refused_before_anything_is_created(
        self,
        pytester: pytest.Pytester,
        fake_ws: FakeWorkspaceClient,
        fake_cli: FakeCli,
        call: str,
        error: str,
        message: str,
    ) -> None:
        deploy_pipeline(fake_ws, fake_cli)
        result = run_tests(pytester, PIPELINE_ERROR_TEST.format(error=error, call=call, message=message))
        result.assert_outcomes(passed=1)
        assert (fake_ws.pipelines.created, fake_ws.jobs.created) == ([], [])

    def test_modify_settings_sees_the_deployed_pipeline(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli
    ) -> None:
        deploy_pipeline(fake_ws, fake_cli)
        extra = "\nseen = []\n\n\ndef record(settings, item):\n"
        extra += "    seen.append(settings.tasks[0].pipeline_task.pipeline_id)\n"
        configure_lakeflow(pytester, "modify_settings=record", extra)
        source = """
            import conftest

            def test_job(lakeflow):
                with lakeflow.job("pipeline_job", pipelines="copy", schema="c.s"):
                    assert conftest.seen == ["abc"]
        """
        run_tests(pytester, source).assert_outcomes(passed=1)
        [job] = fake_ws.jobs.created
        assert pipeline_ids_by_task(job) == {"refresh": "p-1000"}

    @pytest.mark.parametrize(
        ("change", "expected"),
        [
            pytest.param(
                "settings.tasks[0].pipeline_task.pipeline_id = 'xyz'",
                ({"refresh": "p-1000"}, ["[pytest] test_jobs.py::test_job xyz"]),
                id="repointed",
            ),
            pytest.param(
                "settings.tasks.append(jobs.Task(task_key='more', pipeline_task=jobs.PipelineTask(pipeline_id='xyz')))",
                (
                    {"refresh": "p-1000", "more": "p-1001"},
                    ["[pytest] test_jobs.py::test_job my_pipeline", "[pytest] test_jobs.py::test_job xyz"],
                ),
                id="added",
            ),
        ],
    )
    def test_copies_follow_the_pipeline_tasks_modify_settings_leaves(
        self,
        pytester: pytest.Pytester,
        fake_ws: FakeWorkspaceClient,
        fake_cli: FakeCli,
        change: str,
        expected: tuple[dict[str, str], list[str]],
    ) -> None:
        deploy_pipeline(fake_ws, fake_cli)
        fake_ws.pipelines.specs["xyz"] = replace(pipeline_spec(), id="xyz", name="other")
        extra = f"\nfrom databricks.sdk.service import jobs\n\n\ndef change(settings, item):\n    {change}\n"
        configure_lakeflow(pytester, "modify_settings=change", extra)
        tasks, names = expected
        source = f"""
            def test_job(lakeflow):
                with lakeflow.job("pipeline_job", pipelines="copy", schema="c.s") as job:
                    assert [copy.pipeline_id for copy in job.pipeline_copies] == {sorted(set(tasks.values()))!r}
        """
        run_tests(pytester, source).assert_outcomes(passed=1)
        [job] = fake_ws.jobs.created
        assert pipeline_ids_by_task(job) == tasks
        assert [created["name"] for created in fake_ws.pipelines.created] == names

    @pytest.mark.parametrize(
        ("arguments", "body", "outcome", "warned", "kept"),
        [
            pytest.param("", "with copying_job(lakeflow):\n        pass", "failed", True, False, id="keep_failed"),
            pytest.param("keep=True", "with copying_job(lakeflow):\n        pass", "failed", False, True, id="keep"),
            pytest.param(
                "keep_failed=False",
                "with copying_job(lakeflow):\n        pass",
                "failed",
                False,
                False,
                id="keep_failed off",
            ),
            pytest.param(
                "",
                "with pytest.raises(PermissionDenied):\n        copying_job(lakeflow).create()",
                "passed",
                False,
                False,
                id="test passed",
            ),
        ],
    )
    def test_copies_of_a_job_not_created_are_kept_only_by_keep(
        self,
        pytester: pytest.Pytester,
        fake_ws: FakeWorkspaceClient,
        fake_cli: FakeCli,
        monkeypatch: pytest.MonkeyPatch,
        arguments: str,
        body: str,
        outcome: str,
        warned: bool,
        kept: bool,
    ) -> None:
        """Only the case where `keep_failed` alone would have kept the copies warns that they are deleted."""
        deploy_pipeline(fake_ws, fake_cli)
        monkeypatch.setattr(fake_ws.jobs, "create", deny_create)
        configure_lakeflow(pytester, arguments)
        source = f"""
import pytest
from databricks.sdk.errors import PermissionDenied


def copying_job(lakeflow):
    return lakeflow.job("pipeline_job", pipelines="copy", schema="c.s")


def test_job(lakeflow):
    {body}
"""
        result = run_tests(pytester, source, "--log-level=INFO", "-rA")
        result.assert_outcomes(**{outcome: 1})
        warning = (
            "*The job copying 'pipeline_job' was not created, so no job points at its pipeline copies ?'p-1000'?, "
            "and they are deleted whatever keep_failed says*"
        )
        if warned:
            result.stdout.fnmatch_lines([warning])
        else:
            result.stdout.no_fnmatch_line(warning)
        if kept:
            assert fake_ws.deletions == []
            result.stdout.fnmatch_lines(["lakeflow: 1 pipeline kept"])
        else:
            assert fake_ws.deletions == ["pipeline p-1000"]
            assert "lakeflow: 1 pipeline kept" not in result.stdout.str()

    def test_failed_test_keeps_the_job_and_its_copies(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli
    ) -> None:
        deploy_pipeline(fake_ws, fake_cli)
        source = """
            def test_job(lakeflow):
                with lakeflow.job("pipeline_job", pipelines="copy", schema="c.s") as job:
                    job.run()
                    assert False
        """
        result = run_tests(pytester, source)
        result.assert_outcomes(failed=1)
        assert fake_ws.deletions == []
        result.stdout.fnmatch_lines(["*= lakeflow =*", "lakeflow: 1 job kept", "*", "lakeflow: 1 pipeline kept"])
