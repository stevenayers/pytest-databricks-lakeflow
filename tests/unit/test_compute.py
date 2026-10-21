"""Where the copy's tasks run: dependencies, environments, the helper compute and the tasks they cannot reach."""

from __future__ import annotations

from collections.abc import Callable, Collection
from typing import Any

import pytest
from databricks.sdk.service import compute, jobs

from pytest_databricks_lakeflow import LakeflowError
from pytest_databricks_lakeflow.naming import ENV_KEY
from pytest_databricks_lakeflow.settings.compute import (
    Helpers,
    add_dependencies,
    merge_environments,
    unreached_notebook_tasks,
)
from pytest_databricks_lakeflow.settings.stubs import inject_probes, wrap_task
from pytest_databricks_lakeflow.settings.tasks import disable_tasks

from .conftest import (
    DEPS,
    HELPERS,
    UPSTREAM,
    configure_lakeflow,
    environment_keys,
    for_each,
    moved,
    nested_task,
    reached,
    run_tests,
    task,
    with_tasks,
)
from .fakes.templates import example_settings
from .fakes.workspace import FakeWorkspaceClient

WHEEL_WITH_EXTRAS = "/Workspace/b/artifacts/.internal/pkg-0.1-py3-none-any.whl[test]"


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

    @pytest.mark.parametrize("dependency", ["dist/pkg.whl", "dist/*.whl[test]", "pkg-0.1-py3-none-any.whl"])
    def test_add_dependencies_rejects_local_paths(self, dependency: str) -> None:
        settings = example_settings()
        with pytest.raises(ValueError, match=r"Local paths in extra_dependencies must start with \./ or \.\./") as e:
            add_dependencies(settings, ["pytest", dependency])
        assert repr(dependency) in str(e.value)
        assert settings == example_settings(), "nothing changes before the error"

    def test_wheel_extras_reach_environments_as_they_are(self) -> None:
        settings = example_settings()
        settings.tasks = [t for t in settings.tasks or [] if t.task_key != "classic"]
        add_dependencies(settings, [WHEEL_WITH_EXTRAS, "rich[all]>=13"])
        [default] = settings.environments or []
        assert default.spec is not None and default.spec.dependencies == [WHEEL_WITH_EXTRAS, "rich[all]>=13"]

    def test_wheel_extras_are_refused_for_cluster_tasks(self) -> None:
        settings = example_settings()
        with pytest.raises(LakeflowError, match=r"Tasks \['classic'\] run on a cluster") as e:
            add_dependencies(settings, ["pytest", WHEEL_WITH_EXTRAS])
        assert repr(WHEEL_WITH_EXTRAS) in str(e.value)
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
        with pytest.raises(ValueError, match=r"starting with '__pytest_' are reserved.*\['__pytest_serverless'\]"):
            merge_environments(settings, {"__pytest_serverless": {"environment_version": "5"}})
        assert settings == example_settings(), "nothing changes before the error"

    def test_merge_environments_rejects_unknown_fields(self) -> None:
        settings = example_settings()
        with pytest.raises(ValueError, match=r"environments\['default'\] has fields.*\['version'\]") as e:
            merge_environments(settings, {"default": {"version": "5", "dependencies": ["x"]}})
        assert "environment_version" in str(e.value), "the error lists the fields that are accepted"
        assert settings == example_settings(), "nothing changes before the error"

    @pytest.mark.parametrize("override", [None, "5", ["dependencies"]], ids=["none", "string", "list"])
    def test_merge_environments_rejects_an_override_that_is_not_a_mapping(self, override: Any) -> None:
        settings = example_settings()
        with pytest.raises(ValueError, match=r"environments\['default'\] must be a mapping of spec fields"):
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


CLUSTER_HELPERS = Helpers(set_values="/h/set_values", get_values="/h/get_values", job_cluster_key="classic")
NOTEBOOK = {"notebook_path": "/n"}
STUB = {"extract": {"row_count": 3}}


def looped(disabled: bool) -> jobs.JobSettings:
    loop = for_each({"task_key": "inner", "notebook_task": NOTEBOOK})
    loop["disabled"] = disabled
    return reached(with_tasks(loop))


class TestUnreachedNotebookTasks:
    """Which tasks extra_dependencies cannot reach."""

    @pytest.mark.parametrize(
        ("build", "stubs", "expected"),
        [
            pytest.param(reached, (), ["extract", "transform"], id="serverless"),
            pytest.param(lambda: disable_tasks(reached(), ["transform"]), (), ["extract"], id="disabled"),
            pytest.param(lambda: disable_tasks(reached(), ["extract"]), (), [], id="downstream-of-disabled"),
            pytest.param(lambda: moved({}, environment_key="default"), (), ["extract"], id="moved-to-default"),
            pytest.param(
                lambda: moved({"test": {"environment_version": "4"}}, environment_key="test"),
                (),
                ["extract", "transform"],
                id="moved-to-new-environment",
            ),
            pytest.param(
                lambda: moved({}, job_cluster_key="classic"), (), ["extract", "transform"], id="moved-to-cluster"
            ),
            pytest.param(
                lambda: moved({"default": {"dependencies": ["pkg==1.0"]}}, environment_key="default"),
                (),
                ["extract", "transform"],
                id="default-dependencies-replaced",
            ),
            pytest.param(
                lambda: reached(with_tasks({"task_key": "query", "notebook_task": {**NOTEBOOK, "warehouse_id": "w1"}})),
                (),
                ["extract", "transform"],
                id="warehouse",
            ),
            pytest.param(lambda: looped(disabled=False), (), ["extract", "transform", "inner"], id="enabled-loop"),
            pytest.param(lambda: looped(disabled=True), (), ["extract", "transform"], id="disabled-loop"),
            pytest.param(
                lambda: inject_probes(reached(), {"transform": ["k"]}, HELPERS),
                (),
                ["extract", "transform"],
                id="probe-serverless",
            ),
            pytest.param(
                lambda: inject_probes(reached(), {"transform": ["k"]}, CLUSTER_HELPERS),
                (),
                ["extract", "transform"],
                id="probe-on-cluster",
            ),
            pytest.param(
                lambda: wrap_task(reached(), "transform", STUB, ["k"], HELPERS),
                {"extract"},
                ["transform"],
                id="stub-serverless",
            ),
            pytest.param(
                lambda: wrap_task(reached(), "transform", STUB, ["k"], CLUSTER_HELPERS),
                {"extract"},
                ["transform"],
                id="stub-on-cluster",
            ),
        ],
    )
    def test_unreached_notebook_tasks(
        self, build: Callable[[], jobs.JobSettings], stubs: Collection[str], expected: list[str]
    ) -> None:
        assert unreached_notebook_tasks(build(), DEPS, stubs) == expected


def with_environment_count(count: int) -> jobs.JobSettings:
    settings = example_settings()
    extra = [jobs.JobEnvironment.from_dict({"environment_key": f"env{i}"}) for i in range(1, count)]
    settings.environments = [*(settings.environments or []), *extra]
    return settings


def probe_transform(settings: jobs.JobSettings) -> jobs.JobSettings:
    return inject_probes(settings, {"transform": ["x"]}, HELPERS)


def wrap_transform(settings: jobs.JobSettings) -> jobs.JobSettings:
    return wrap_task(settings, "transform", UPSTREAM, [], HELPERS)


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


ON_DEFAULT = ("environment_key", "default")


def unreached_warning(keys: list[str]) -> str:
    """The warning that `extra_dependencies=["faker==30.0.0"]` does not reach `keys` of `my_job`, as a fnmatch line."""
    message = (
        f"extra_dependencies ['faker==30.0.0'] do not reach tasks {keys} of job 'my_job': neither the environment "
        "nor the cluster libraries of these notebook tasks list them, and a serverless notebook task without an "
        "environment_key has neither. Install them with %pip install in the notebook, or run the task on an "
        "environment that lists them."
    )
    # fnmatch reads "[" as the start of a character class.
    return "*" + message.replace("[", "[[]") + "*"


def move_tasks(**changes: tuple[str, str]) -> str:
    """A `modify_settings` callback, `move`, that sets the `(field, value)` given for each task key on that task."""
    lines = ["", "", "def move(settings, item):", "    for task in settings.tasks:"]
    for key, (field, value) in changes.items():
        lines += [f"        if task.task_key == {key!r}:", f"            task.{field} = {value!r}"]
    return "\n".join(lines) + "\n"


@pytest.mark.usefixtures("fake_cli", "bundle_dir")
class TestUnreachedDependencies:
    """The warning for test-only packages that notebook tasks do not get."""

    @pytest.mark.parametrize(
        ("modify", "call", "expected"),
        [
            pytest.param(None, 'lakeflow.job("my_job", {})', ["extract", "transform"], id="job"),
            pytest.param(None, 'lakeflow.task("my_job.extract", {})', ["extract"], id="task"),
            pytest.param(
                None,
                'lakeflow.job("my_job", setup=["SELECT 1"], teardown=["SELECT 2"], run_as_task=True, {})',
                ["extract", "transform"],
                id="setup-and-teardown",
            ),
            pytest.param(
                None,
                'lakeflow.task("my_job.transform", upstream_task_values={{"extract": {{"row_count": 3}}}}, {})',
                ["transform"],
                id="task-with-stub",
            ),
            pytest.param(
                move_tasks(transform=("environment_key", "default"), extract=("job_cluster_key", "classic")),
                'lakeflow.job("my_job", {})',
                ["extract"],
                id="modify-settings",
            ),
        ],
    )
    def test_warns_once_naming_the_tasks(
        self,
        pytester: pytest.Pytester,
        fake_ws: FakeWorkspaceClient,
        modify: str | None,
        call: str,
        expected: list[str],
    ) -> None:
        if modify is not None:
            configure_lakeflow(pytester, "modify_settings=move", modify)
        source = call.format('extra_dependencies=["faker==30.0.0"]')
        result = run_tests(pytester, f"def test_job(lakeflow):\n    with {source}:\n        pass\n")
        result.assert_outcomes(passed=1, warnings=1)
        result.stdout.fnmatch_lines([unreached_warning(expected)])
        assert len(fake_ws.jobs.created) == 1

    @pytest.mark.parametrize(
        ("environments", "key"),
        [({"test": {"environment_version": "4"}}, "test"), ({"default": {"dependencies": ["pkg==1.0"]}}, "default")],
        ids=["new", "replaced"],
    )
    def test_names_tasks_on_an_environment_without_the_packages(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, environments: dict[str, Any], key: str
    ) -> None:
        on_environment = ("environment_key", key)
        move = move_tasks(extract=on_environment, transform=on_environment)
        configure_lakeflow(pytester, "modify_settings=move", move)
        call = f'lakeflow.job("my_job", extra_dependencies=["faker==30.0.0"], environments={environments!r})'
        result = run_tests(pytester, f"def test_job(lakeflow):\n    with {call}:\n        pass\n")
        result.assert_outcomes(passed=1, warnings=1)
        result.stdout.fnmatch_lines([unreached_warning(["extract", "transform"])])

    @pytest.mark.parametrize(
        ("modify", "call"),
        [
            pytest.param(None, 'lakeflow.task("my_job.load", extra_dependencies=["faker==30.0.0"])', id="wheel-task"),
            pytest.param(None, 'lakeflow.task("my_job.classic", extra_dependencies=["faker==30.0.0"])', id="cluster"),
            pytest.param(None, 'lakeflow.job("my_job")', id="no-dependencies"),
            pytest.param(
                None,
                'lakeflow.job("my_job", extra_dependencies=["faker==30.0.0"], disable=["extract", "transform"])',
                id="disabled",
            ),
            pytest.param(
                move_tasks(extract=ON_DEFAULT, transform=ON_DEFAULT),
                'lakeflow.job("my_job", extra_dependencies=["faker==30.0.0"])',
                id="on-default",
            ),
        ],
    )
    def test_no_warning_when_every_task_is_reached(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, modify: str | None, call: str
    ) -> None:
        if modify is not None:
            configure_lakeflow(pytester, "modify_settings=move", modify)
        result = run_tests(pytester, f"def test_job(lakeflow):\n    with {call}:\n        pass\n")
        result.assert_outcomes(passed=1)
        assert "warnings" not in result.parseoutcomes()


@pytest.mark.usefixtures("fake_cli", "bundle_dir")
class TestHelperComputeEndToEnd:
    """The helper compute settings of `Lakeflow` reach the created job."""

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
