"""`Lakeflow.check()`: the wiring mistakes `lint` finds in a template without running it."""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import pytest
from databricks.sdk.service import jobs

from pytest_databricks_lakeflow import ALL_RULES, DEFAULT_RULES
from pytest_databricks_lakeflow.settings.lint import lint

from .conftest import for_each, notebook, task, with_tasks
from .fakes.templates import build_wheel, example_settings


class TestLint:
    """lint reports the wiring mistakes in a template that jobs/create would refuse, one message per problem."""

    def test_the_example_template_has_no_problems(self) -> None:
        assert lint(example_settings()) == []

    def test_undeclared_job_parameter(self) -> None:
        settings = example_settings()
        transform = task(settings, "transform").notebook_task
        assert transform is not None
        transform.base_parameters = {"x": "{{job.parameters.nope}}", "y": "{{ job.parameters.param1 }}"}
        assert lint(settings) == [
            "Task 'transform': {{job.parameters.nope}} is not declared. Declared: ['param1', 'schema']"
        ]

    def test_task_values_need_a_transitive_upstream_task(self) -> None:
        settings = example_settings()
        load = task(settings, "load").python_wheel_task
        assert load is not None
        load.named_parameters = {"n": "{{tasks.extract.values.n}}"}
        assert lint(settings) == []
        task(settings, "extract").notebook_task = jobs.NotebookTask("/extract", {"n": "{{tasks.load.values.n}}"})
        assert lint(settings) == [
            "Task 'extract': refers to the task values of 'load', which is not upstream of it. Upstream: []"
        ]

    def test_unknown_dependency(self) -> None:
        settings = example_settings()
        transform = task(settings, "transform")
        transform.depends_on = [*(transform.depends_on or []), jobs.TaskDependency(task_key="missing")]
        assert lint(settings) == [
            "Task 'transform': depends on 'missing', which is not a task in the job. "
            "Tasks: ['classic', 'extract', 'load', 'transform']"
        ]

    @pytest.mark.parametrize(
        ("retries", "sets"),
        [
            ({"max_retries": 2}, "max_retries=2"),
            ({"max_retries": -1, "retry_on_timeout": True}, "max_retries=-1, retry_on_timeout=True"),
            ({"min_retry_interval_millis": 1000}, "min_retry_interval_millis=1000"),
        ],
    )
    def test_retries_on_a_for_each_task(self, retries: dict[str, Any], sets: str) -> None:
        settings = with_tasks({**for_each(notebook("inner")), **retries})
        assert lint(settings) == [
            f"Task 'loop': a for_each task cannot be retried, but it sets {sets}. "
            "Set retries on its nested task 'inner' instead."
        ]

    def test_retries_on_the_nested_task_and_the_defaults_jobs_get_returns_are_fine(self) -> None:
        # jobs/get returns max_retries 0 and retry_on_timeout false beside any retry field a task sets, and
        # jobs/create accepts them on a for_each task.
        inner = {**notebook("inner"), "max_retries": 2, "retry_on_timeout": True}
        settings = with_tasks({**for_each(inner), "max_retries": 0, "retry_on_timeout": False})
        assert lint(settings) == []

    def test_a_nested_task_shares_the_upstream_of_its_for_each_task(self) -> None:
        # jobs/create accepts a nested task's reference to a task upstream of its for_each task, and refuses one to
        # the for_each task itself: "Task 'loop' must be a dependency of task 'inner'."
        upstream = for_each(notebook("inner", n="{{tasks.extract.values.n}}"))
        settings = with_tasks({**upstream, "depends_on": [{"task_key": "transform"}]})
        assert lint(settings) == []
        settings = with_tasks(for_each(notebook("inner", n="{{tasks.loop.values.n}}", p="{{job.parameters.p}}")))
        assert lint(settings) == [
            "Task 'inner': {{job.parameters.p}} is not declared. Declared: ['param1', 'schema']",
            "Task 'inner': refers to the task values of 'loop', which is not upstream of it. Upstream: []",
        ]

    def test_one_message_per_problem_in_task_order(self) -> None:
        settings = jobs.JobSettings.from_dict(
            {
                "parameters": [{"name": "day", "default": ""}],
                "tasks": [
                    notebook("a", "ghost", x="{{job.parameters.x}}", y="{{tasks.c.values.n}}"),
                    notebook("b", "a", x="{{job.parameters.x}}", y="{{job.parameters.day}}"),
                    notebook("c", z="{{job.parameters.z}}", w="{{job.parameters.x}}"),
                ],
            }
        )
        assert lint(settings) == [
            "Task 'a': {{job.parameters.x}} is not declared. Declared: ['day']",
            "Task 'a': refers to the task values of 'c', which is not upstream of it. Upstream: []",
            "Task 'a': depends on 'ghost', which is not a task in the job. Tasks: ['a', 'b', 'c']",
            "Task 'b': {{job.parameters.x}} is not declared. Declared: ['day']",
            "Task 'c': {{job.parameters.x}} is not declared. Declared: ['day']",
            "Task 'c': {{job.parameters.z}} is not declared. Declared: ['day']",
        ]

    def test_lint_leaves_the_settings_unchanged(self) -> None:
        settings = with_tasks({**for_each(notebook("inner", "ghost", n="{{tasks.x.values.n}}")), "max_retries": 2})
        before = copy.deepcopy(settings.as_dict())
        assert lint(settings)
        assert settings.as_dict() == before


def wheel_settings(entry_point: str, package: str = "pkg") -> jobs.JobSettings:
    """Build `example_settings` with `load` running `entry_point` of the wheel `package`."""
    settings = example_settings()
    task(settings, "load").python_wheel_task = jobs.PythonWheelTask(package_name=package, entry_point=entry_point)
    return settings


CONSOLE_SCRIPTS = "[console_scripts]\nmain = pkg.main:main\n"


def trigger_message(owner: str, path: str) -> str:
    return (
        f"{owner}: refers to {{{{job.trigger.{path}}}}}, which a test's run-now run does not resolve as its trigger "
        f"would. Set its value with trigger={{{path!r}: ...}} on job() or task()."
    )


class TestLintRules:
    """The named rules of lint: built wheels, workspace paths, dashed task keys, trigger references and EQUAL_TO."""

    @pytest.mark.parametrize(
        ("entry_point", "entry_points", "init", "problems"),
        [
            ("main", CONSOLE_SCRIPTS, "", []),
            # The Databricks docs build their wheel with entry_points={"group_1": "run=..."}: any group will do.
            ("run", "[group_1]\nrun = pkg.main:main\n", "", []),
            ("run", CONSOLE_SCRIPTS, "def run():\n    pass\n", []),
            ("run", CONSOLE_SCRIPTS, "from pkg.main import main as run\n", []),
            (
                "run",
                "[console_scripts]\nmain = pkg.main:main\n[group_1]\nstart = pkg.main:main\nmain = pkg.main:main\n",
                "def other():\n    pass\n",
                [
                    "Task 'load': entry point 'run' is not in dist/pkg-0.1.0-py3-none-any.whl. Entry points: "
                    "['main', 'start'], and pkg/__init__.py does not define it."
                ],
            ),
        ],
        ids=["console-script", "other-group", "init-def", "init-import", "undefined"],
    )
    def test_lint_wheel_rule(
        self, tmp_path: Path, entry_point: str, entry_points: str, init: str, problems: list[str]
    ) -> None:
        build_wheel(tmp_path / "dist", entry_points=entry_points, init=init)
        assert lint(wheel_settings(entry_point), bundle_root=tmp_path) == problems

    def test_lint_wheel_rule_reads_the_newest_wheel_of_the_normalised_package(self, tmp_path: Path) -> None:
        build_wheel(tmp_path / "dist", "Lakeflow.Example", init="def run():\n    pass\n", modified=1_000)
        build_wheel(tmp_path / "dist", "Lakeflow.Example", version="0.2.0", modified=2_000)
        build_wheel(tmp_path / "dist", "other")
        # A wheel that is not a zip, such as one an interrupted build left behind, is skipped.
        (tmp_path / "dist" / "broken-0.1.0-py3-none-any.whl").write_bytes(b"partial")
        settings = wheel_settings("main", "lakeflow_example")
        assert lint(settings, bundle_root=tmp_path) == []
        inner = {"task_key": "inner", "python_wheel_task": {"package_name": "lakeflow-example", "entry_point": "run"}}
        nested = for_each(inner)
        settings.tasks = [*(settings.tasks or []), jobs.Task.from_dict(nested)]
        assert lint(settings, bundle_root=tmp_path) == [
            "Task 'inner': entry point 'run' is not in dist/lakeflow_example-0.2.0-py3-none-any.whl. Entry points: "
            "['main'], and lakeflow-example/__init__.py does not define it."
        ]
        assert lint(wheel_settings("main", "missing"), bundle_root=tmp_path) == [
            "Task 'load': no wheel in dist/ is package 'missing'. Packages: ['lakeflow-example', 'other']"
        ]

    def test_lint_skips_wheel_rule_without_dist(self, tmp_path: Path) -> None:
        settings = wheel_settings("nope")
        assert lint(settings, bundle_root=tmp_path) == []
        (tmp_path / "dist").mkdir()
        assert lint(settings, bundle_root=tmp_path) == []
        build_wheel(tmp_path / "dist")
        assert lint(settings) == []
        assert lint(settings, rules=DEFAULT_RULES - {"wheel"}, bundle_root=tmp_path) == []
        assert lint(settings, bundle_root=tmp_path)

    def test_lint_library_paths(self) -> None:
        settings = with_tasks(
            {
                "task_key": "py",
                "spark_python_task": {"python_file": "/Workspace/files/main.py"},
                "libraries": [
                    {"whl": "/Workspace/libs/a.whl"},
                    {"jar": "/Volumes/c/s/v/b.jar"},
                    {"whl": "dbfs:/libs/c.whl"},
                    {"pypi": {"package": "inflection"}},
                ],
            },
            for_each({"task_key": "inner", "notebook_task": {"notebook_path": "/n"}, "libraries": [{"whl": "/w.whl"}]}),
        )
        spec = (settings.environments or [])[0].spec
        assert spec is not None
        spec.dependencies = ["/Workspace/libs/env.whl", "pandas==2.2.3", "./dist/*.whl"]
        called: list[str] = []

        def exists(path: str) -> bool:
            called.append(path)
            return path == "/Workspace/libs/a.whl"

        assert lint(settings, exists=exists) == [
            "Task 'py': /Volumes/c/s/v/b.jar does not exist.",
            "Task 'py': /Workspace/files/main.py does not exist.",
            "Task 'inner': /w.whl does not exist.",
            "Environment 'default': /Workspace/libs/env.whl does not exist.",
        ]
        assert called == [
            "/Workspace/libs/a.whl",
            "/Volumes/c/s/v/b.jar",
            "/Workspace/files/main.py",
            "/w.whl",
            "/Workspace/libs/env.whl",
        ]
        called.clear()
        assert lint(settings, rules=DEFAULT_RULES - {"library"}, exists=exists) == []
        assert called == []

    def test_lint_dashed_task_keys(self) -> None:
        loop = for_each(notebook("inner", n="{{tasks.load-raw.values.n}}"))
        settings = jobs.JobSettings.from_dict(
            {
                "tasks": [
                    notebook("load-raw"),
                    notebook(
                        "a",
                        "load-raw",
                        n="{{tasks.load-raw.values.n}}",
                        m="{{ tasks.load-raw.values.m }}",
                        s="{{tasks.load-raw.result_state}}",
                    ),
                    {**loop, "depends_on": [{"task_key": "load-raw"}]},
                    # A key in backticks may hold a dash.
                    notebook("b", "load-raw", n="{{tasks.`load-raw`.values.n}}"),
                ]
            }
        )
        message = (
            "refers to task 'load-raw', whose key has a dash, which task references do not support. Rename the task "
            "without dashes, or escape the key with backticks, as in {{tasks.`load-raw`.values.<name>}}."
        )
        assert lint(settings) == [f"Task 'a': {message}", f"Task 'inner': {message}"]
        assert lint(settings, rules=DEFAULT_RULES - {"dash"}) == []

    def test_lint_trigger_references(self) -> None:
        settings = jobs.JobSettings.from_dict(
            {
                "parameters": [{"name": "day", "default": "{{job.trigger.time.iso_date}}"}],
                "tasks": [
                    notebook(
                        "a", t="{{job.trigger.type}}", u="{{ job.trigger.type }}", d="{{job.trigger.time.iso_date}}"
                    ),
                    for_each(notebook("inner", t="{{job.trigger.type}}")),
                ],
            }
        )
        assert lint(settings) == [
            trigger_message("Task 'a'", "time.iso_date"),
            trigger_message("Task 'a'", "type"),
            trigger_message("Task 'inner'", "type"),
            trigger_message("Parameter 'day'", "time.iso_date"),
        ]
        assert lint(settings, rules=DEFAULT_RULES - {"trigger"}) == []

    def test_lint_equal_to_is_opt_in(self) -> None:
        assert "equal_to" not in DEFAULT_RULES
        assert {*DEFAULT_RULES, "equal_to"} == ALL_RULES

        def condition(key: str, op: str, left: str, right: str) -> dict[str, Any]:
            return {
                "task_key": key,
                "depends_on": [{"task_key": "a"}],
                "condition_task": {"op": op, "left": left, "right": right},
            }

        settings = jobs.JobSettings.from_dict(
            {
                "tasks": [
                    notebook("a"),
                    condition("c", "EQUAL_TO", "{{tasks.a.values.n}}", "0"),
                    condition("d", "NOT_EQUAL", "yes", "07"),
                    condition("e", "EQUAL_TO", "true", "false"),
                    condition("f", "GREATER_THAN", "{{tasks.a.values.n}}", "0"),
                ]
            }
        )
        assert lint(settings) == []
        assert lint(settings, rules=ALL_RULES) == [
            "Task 'c': EQUAL_TO compares '{{tasks.a.values.n}}' with '0' as text, so a number written another way, "
            "such as 7.0 for 7, does not match.",
            "Task 'd': NOT_EQUAL compares 'yes' with '07' as text, so a number written another way, such as 7.0 for "
            "7, does not match.",
        ]

    def test_unknown_rule(self) -> None:
        with pytest.raises(
            ValueError,
            match=r"Unknown lint rules \['wheels'\]. Rules: \['dash', 'equal_to', 'library', 'trigger', 'wheel'\]",
        ):
            lint(example_settings(), rules={"wheels"})
