"""Find the wiring mistakes in a job's settings that `bundle validate` passes, without changing them."""

from __future__ import annotations

import ast
import json
import re
import zipfile
from collections.abc import Callable, Collection, Mapping, Sequence
from email.parser import HeaderParser
from pathlib import Path
from typing import Any

from databricks.sdk.service import jobs

from pytest_databricks_lakeflow.settings.params import declared_params
from pytest_databricks_lakeflow.settings.references import (
    JOB_PARAMETER_REFERENCE,
    TASK_REFERENCE,
    TASK_VALUES_REFERENCE,
    TRIGGER_REFERENCE,
    own_fields,
)
from pytest_databricks_lakeflow.settings.tasks import reachable

# The rules `lint` applies by name, on top of the four it always applies. equal_to is opt-in: EQUAL_TO compares text,
# so "7" equals "7" and only a number written another way, such as 7.0 or 07, misfires, which the lint cannot see.
ALL_RULES = frozenset({"wheel", "library", "dash", "trigger", "equal_to"})
DEFAULT_RULES = ALL_RULES - {"equal_to"}


def lint(
    settings: jobs.JobSettings,
    *,
    rules: Collection[str] = DEFAULT_RULES,
    bundle_root: Path | None = None,
    exists: Callable[[str], bool] | None = None,
) -> list[str]:
    """Find the wiring mistakes in a job's settings that `bundle validate` passes, without changing them.

    jobs/create refuses most of them, one at a time; this finds them all at once, before anything reaches the
    workspace. Four rules always apply, and `rules` names the others. A named rule whose context is `None` is skipped.

    Args:
        settings: The job's settings.
        rules: The named rules to apply, from `ALL_RULES`: `wheel` checks each `python_wheel_task` against the newest
            wheel of its package under `bundle_root / "dist"`, `library` passes each library, Python file and
            environment dependency path that starts with `/` to `exists`, `dash` reports a task reference to a task
            key with a dash, `trigger` reports each `{{job.trigger.*}}` reference, and `equal_to`, which only
            `ALL_RULES` holds, reports an `EQUAL_TO` or `NOT_EQUAL` condition on a number or a task value.
        bundle_root: The bundle's directory, for the `wheel` rule. Without it, or without wheels in its `dist`, the
            rule is skipped.
        exists: Whether a workspace or Volumes path exists, for the `library` rule.

    Returns:
        One message per problem. The problems of the tasks come first, each starting with `Task '<key>':`, in the
        order of `settings.tasks` with each nested task after its `for_each_task`: a `{{job.parameters.<name>}}` the
        job does not declare, a `{{tasks.<key>.values.<name>}}` whose task is not upstream, a `depends_on` task that
        is not in the job, retry settings on a `for_each_task`, then what the named rules find. The missing
        dependencies of each environment follow, starting with `Environment '<key>':`, then the trigger references
        in the default of each job parameter, starting with `Parameter '<name>':`.

    Raises:
        ValueError: If `rules` names a rule that is not in `ALL_RULES`.
    """
    unknown = sorted(set(rules) - ALL_RULES)
    if unknown:
        raise ValueError(f"Unknown lint rules {unknown}. Rules: {sorted(ALL_RULES)}")
    wheels: dict[str, Path] = {}
    if "wheel" in rules and bundle_root is not None:
        wheels = _wheels_by_project(bundle_root / "dist")
    library_exists = exists if "library" in rules else None
    declared = sorted(declared_params(settings))
    keys = [task.task_key for task in settings.tasks or []]
    graph = {task.task_key: {dep.task_key for dep in task.depends_on or []} for task in settings.tasks or []}
    problems: list[str] = []
    for task in settings.tasks or []:
        upstream = reachable(task.task_key, graph) & graph.keys()
        problems += _task_problems(task, upstream, declared, keys)
        problems += _rule_problems(task, rules, wheels, library_exists)
        if task.for_each_task and task.for_each_task.task:
            # A nested task may refer to the task values of any task upstream of its for_each task, and jobs/create
            # refuses a reference to the for_each task itself: "Task 'loop' must be a dependency of task 'inner'."
            problems += _task_problems(task.for_each_task.task, upstream, declared, keys)
            problems += _rule_problems(task.for_each_task.task, rules, wheels, library_exists)
    if library_exists is not None:
        for environment in settings.environments or []:
            dependencies = environment.spec.dependencies if environment.spec else None
            prefix = f"Environment {environment.environment_key!r}:"
            problems += _missing_paths(prefix, dependencies or [], library_exists)
    if "trigger" in rules:
        for parameter in settings.parameters or []:
            problems += _trigger_problems(f"Parameter {parameter.name!r}:", parameter.default)
    return problems


def _task_problems(task: jobs.Task, upstream: set[str], declared: list[str], keys: list[str]) -> list[str]:
    body = task.as_dict()
    if "for_each_task" in body:
        body["for_each_task"].pop("task", None)
    text = json.dumps(body)
    prefix = f"Task {task.task_key!r}:"
    problems: list[str] = []
    for name in sorted(set(JOB_PARAMETER_REFERENCE.findall(text)) - set(declared)):
        problems.append(f"{prefix} {{{{job.parameters.{name}}}}} is not declared. Declared: {declared}")
    for key in sorted(set(TASK_VALUES_REFERENCE.findall(text)) - upstream):
        problems.append(
            f"{prefix} refers to the task values of {key!r}, which is not upstream of it. Upstream: {sorted(upstream)}"
        )
    for dependency in task.depends_on or []:
        if dependency.task_key not in keys:
            problems.append(
                f"{prefix} depends on {dependency.task_key!r}, which is not a task in the job. Tasks: {sorted(keys)}"
            )
    retries = _retries(task)
    if task.for_each_task and retries:
        # jobs/create refuses max_retries above 0 on a for_each task ("For each task cannot be retried but has
        # retries configured") and accepts min_retry_interval_millis and retry_on_timeout there, to no effect. jobs/get
        # returns max_retries 0 and retry_on_timeout false beside either, so those defaults are not reported.
        nested = "its nested task"
        if task.for_each_task.task:
            nested += f" {task.for_each_task.task.task_key!r}"
        problems.append(
            f"{prefix} a for_each task cannot be retried, but it sets {retries}. Set retries on {nested} instead."
        )
    return problems


def _retries(task: jobs.Task) -> str:
    """The retry settings a task sets to other than their defaults, as `name=value` pairs."""
    pairs: list[str] = []
    for name in ("max_retries", "min_retry_interval_millis", "retry_on_timeout"):
        value = getattr(task, name)
        if value:
            pairs.append(f"{name}={value}")
    return ", ".join(pairs)


def _rule_problems(
    task: jobs.Task, rules: Collection[str], wheels: Mapping[str, Path], exists: Callable[[str], bool] | None
) -> list[str]:
    """What the named rules find in a task, leaving out the task nested in its `for_each_task`."""
    prefix = f"Task {task.task_key!r}:"
    own = own_fields(task)
    problems: list[str] = []
    if task.python_wheel_task and wheels:
        problems += _wheel_problems(prefix, task.python_wheel_task, wheels)
    if exists is not None:
        problems += _missing_paths(prefix, _task_paths(task), exists)
    if "dash" in rules:
        for key in sorted(set(TASK_REFERENCE.findall(json.dumps(own)))):
            if "-" in key:
                problems.append(
                    f"{prefix} refers to task {key!r}, whose key has a dash, which task references do not support. "
                    f"Rename the task without dashes, or escape the key with backticks, as in "
                    f"{{{{tasks.`{key}`.values.<name>}}}}."
                )
    if "trigger" in rules:
        problems += _trigger_problems(prefix, own)
    if "equal_to" in rules and task.condition_task:
        problems += _equal_to_problems(prefix, task.condition_task)
    return problems


def _wheel_problems(prefix: str, task: jobs.PythonWheelTask, wheels: Mapping[str, Path]) -> list[str]:
    package = task.package_name
    path = wheels.get(_project_name(package))
    if path is None:
        return [f"{prefix} no wheel in dist/ is package {package!r}. Packages: {sorted(wheels)}"]
    with zipfile.ZipFile(path) as wheel:
        entry_points = _entry_points(wheel)
        init = _read_member(wheel, f"{package}/__init__.py")
    if task.entry_point in entry_points:
        return []
    # Without an entry point of that name, the platform calls the attribute of that name of the package's module.
    if init is not None and _binds(init, task.entry_point):
        return []
    return [
        f"{prefix} entry point {task.entry_point!r} is not in dist/{path.name}. Entry points: {entry_points}, and "
        f"{package}/__init__.py does not define it."
    ]


def _wheels_by_project(dist: Path) -> dict[str, Path]:
    """The newest wheel under `dist` of each project, by its normalised name: the one the last build wrote."""
    wheels: dict[str, Path] = {}
    for path in sorted(dist.glob("*.whl"), key=lambda path: (path.stat().st_mtime, path.name)):
        try:
            with zipfile.ZipFile(path) as wheel:
                metadata = _dist_info(wheel, "METADATA")
        except zipfile.BadZipFile:
            # A partial wheel, such as one an interrupted build left behind.
            continue
        if metadata is None:
            continue
        name = HeaderParser().parsestr(metadata)["Name"]
        if name:
            wheels[_project_name(name)] = path
    return wheels


def _project_name(name: str) -> str:
    """A project name normalised as PEP 503 does, so `Lakeflow.Example` and `lakeflow_example` match."""
    return re.sub(r"[-_.]+", "-", name).lower()


def _dist_info(wheel: zipfile.ZipFile, name: str) -> str | None:
    for member in wheel.namelist():
        folder, _, file_name = member.partition("/")
        if folder.endswith(".dist-info") and file_name == name:
            return wheel.read(member).decode()
    return None


def _read_member(wheel: zipfile.ZipFile, name: str) -> str | None:
    if name not in wheel.namelist():
        return None
    return wheel.read(name).decode()


def _entry_points(wheel: zipfile.ZipFile) -> list[str]:
    """The names of the wheel's entry points, of every group.

    The platform looks `entry_point` up in any group: the Databricks docs build their wheel with
    `entry_points={"group_1": "run=..."}`, and older ones used a `packages` group.
    """
    text = _dist_info(wheel, "entry_points.txt") or ""
    names: set[str] = set()
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("[") or "=" not in line:
            continue
        names.add(line.split("=", 1)[0].strip())
    return sorted(names)


def _binds(source: str, name: str) -> bool:
    """Whether a module binds `name` at its top level, by a def, a class, an import or an assignment."""
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return True
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and node.name == name:
            return True
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                # A star import may bind any name.
                if alias.name == "*" or (alias.asname or alias.name.split(".")[0]) == name:
                    return True
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == name:
                    return True
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.target.id == name:
            return True
    return False


def _task_paths(task: jobs.Task) -> list[str | None]:
    paths: list[str | None] = []
    for library in task.libraries or []:
        paths += [library.whl, library.jar]
    if task.spark_python_task:
        paths.append(task.spark_python_task.python_file)
    return paths


def _missing_paths(prefix: str, paths: Sequence[str | None], exists: Callable[[str], bool]) -> list[str]:
    """One message per distinct path starting with `/` that does not exist. Others are local, remote or PyPI."""
    problems: list[str] = []
    for path in dict.fromkeys(paths):
        if path is None or not path.startswith("/"):
            continue
        if not exists(path):
            problems.append(f"{prefix} {path} does not exist.")
    return problems


def _trigger_problems(prefix: str, fields: Any) -> list[str]:
    problems: list[str] = []
    for path in sorted(set(TRIGGER_REFERENCE.findall(json.dumps(fields)))):
        problems.append(
            f"{prefix} refers to {{{{job.trigger.{path}}}}}, which a test's run-now run does not resolve as its "
            f"trigger would. Set its value with trigger={{{path!r}: ...}} on job() or task()."
        )
    return problems


def _equal_to_problems(prefix: str, condition: jobs.ConditionTask) -> list[str]:
    if condition.op not in (jobs.ConditionTaskOp.EQUAL_TO, jobs.ConditionTaskOp.NOT_EQUAL):
        return []
    if not (_number_or_task_value(condition.left) or _number_or_task_value(condition.right)):
        return []
    return [
        f"{prefix} {condition.op.value} compares {condition.left!r} with {condition.right!r} as text, so a number "
        "written another way, such as 7.0 for 7, does not match."
    ]


def _number_or_task_value(operand: str) -> bool:
    if TASK_VALUES_REFERENCE.search(operand):
        return True
    try:
        float(operand)
    except ValueError:
        return False
    return True
