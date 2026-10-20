from __future__ import annotations

import copy
import glob
import json
import logging
import os
import re
import subprocess
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

from databricks.sdk.service import jobs

from pytest_databricks_lakeflow.errors import LakeflowBundleError

log = logging.getLogger("pytest_databricks_lakeflow")

BUNDLE_FILES = ("databricks.yml", "databricks.yaml")
MIN_CLI_VERSION = (1, 7, 0)
UNRESOLVED = re.compile(r"\$\{[^}]+\}")
VERSION = re.compile(r"v(\d+)\.(\d+)\.(\d+)")
CLI_DOCS = "https://docs.databricks.com/dev-tools/cli/install"
WHEEL_EXTRAS = re.compile(r"(.*\.whl)(\[[^\]]*\])")
# The build machine's clock and the workspace's can disagree, so a wheel counts as built after its upload only when
# its modification time is later by more than this.
CLOCK_SKEW_MS = 60_000


def find_root(start: Path) -> Path:
    for path in (start, *start.parents):
        if _has_bundle(path):
            return path
    raise LakeflowBundleError(f"No databricks.yml in {start} or any parent directory. Pass bundle_root to Lakeflow.")


def require_root(path: Path) -> Path:
    if not _has_bundle(path):
        raise LakeflowBundleError(f"bundle_root {path} has no databricks.yml")
    return path


def _has_bundle(path: Path) -> bool:
    return any((path / name).is_file() for name in BUNDLE_FILES)


def run_cli(args: list[str], root: Path, profile: str | None = None) -> str:
    if not root.is_dir():
        raise LakeflowBundleError(f"Bundle root {root} does not exist")
    cmd = ["databricks", *args]
    if profile:
        cmd += ["--profile", profile]
    try:
        result = subprocess.run(cmd, cwd=root, check=True, capture_output=True, text=True)
    except FileNotFoundError:
        raise LakeflowBundleError(f"The databricks CLI is not on PATH. Install it from {CLI_DOCS}") from None
    except subprocess.CalledProcessError as e:
        raise LakeflowBundleError(f"`{' '.join(cmd)}` failed with exit code {e.returncode}:\n{e.stderr.strip()}") from e
    return result.stdout


def check_cli_version(root: Path) -> None:
    out = run_cli(["--version"], root)
    match = VERSION.search(out)
    if match is None:
        return
    version = tuple(int(part) for part in match.groups())
    if version == (0, 0, 0):
        # A CLI built from source reports v0.0.0-dev, which is accepted.
        return
    if version < MIN_CLI_VERSION:
        minimum = ".".join(map(str, MIN_CLI_VERSION))
        raise LakeflowBundleError(f"pytest-databricks-lakeflow needs Databricks CLI v{minimum} or later: {out.strip()}")


def deploy(
    root: Path,
    target: str,
    profile: str | None,
    variables: Mapping[str, str] | None = None,
    *,
    flags: Sequence[str] = (),
) -> None:
    log.info("Deploying bundle %s to target %s", root, target)
    run_cli(["bundle", "deploy", "-t", target, "--auto-approve", *_var_args(variables), *flags], root, profile)


def sync(root: Path, target: str, profile: str | None, variables: Mapping[str, str] | None = None) -> None:
    log.info("Syncing bundle %s to target %s", root, target)
    run_cli(["bundle", "sync", "-t", target, *_var_args(variables)], root, profile)


def validate(
    root: Path, target: str, profile: str | None, variables: Mapping[str, str] | None = None
) -> dict[str, Any]:
    return _json_command("validate", root, target, profile, variables)


def summary(root: Path, target: str, profile: str | None, variables: Mapping[str, str] | None = None) -> dict[str, Any]:
    return _json_command("summary", root, target, profile, variables)


def destroy(root: Path, target: str, profile: str | None, variables: Mapping[str, str] | None = None) -> None:
    log.info("Destroying bundle %s target %s", root, target)
    run_cli(["bundle", "destroy", "-t", target, "--auto-approve", *_var_args(variables)], root, profile)


def _var_args(variables: Mapping[str, str] | None) -> list[str]:
    args: list[str] = []
    for name, value in sorted((variables or {}).items()):
        args += ["--var", f"{name}={value}"]
    return args


def _json_command(
    command: str, root: Path, target: str, profile: str | None, variables: Mapping[str, str] | None = None
) -> dict[str, Any]:
    out = run_cli(["bundle", command, "-t", target, "-o", "json", *_var_args(variables)], root, profile)
    try:
        data = json.loads(out)
    except json.JSONDecodeError as e:
        raise LakeflowBundleError(f"`databricks bundle {command}` did not print JSON: {e}\n{out[:2000]}") from e
    if not isinstance(data, dict):
        raise LakeflowBundleError(f"`databricks bundle {command}` printed {type(data).__name__}, expected an object")
    return data


def job_ids(bundle_summary: Mapping[str, Any]) -> tuple[dict[str, int], dict[str, str]]:
    """Read deployed job ids from `bundle summary` output.

    Args:
        bundle_summary: The parsed output of `databricks bundle summary -o json`.

    Returns:
        The job ids by resource key, and an error for each job that is defined but not deployed.
    """
    ids, errors = _deployed_ids(bundle_summary, "jobs", "Job")
    return {key: int(job_id) for key, job_id in ids.items()}, errors


def pipeline_ids(bundle_summary: Mapping[str, Any]) -> tuple[dict[str, str], dict[str, str]]:
    """Read deployed pipeline ids from `bundle summary` output, which prints a pipeline as it prints a job.

    Args:
        bundle_summary: The parsed output of `databricks bundle summary -o json`.

    Returns:
        The pipeline ids by resource key, and an error for each pipeline that is defined but not deployed.
    """
    return _deployed_ids(bundle_summary, "pipelines", "Pipeline")


def _deployed_ids(bundle_summary: Mapping[str, Any], kind: str, noun: str) -> tuple[dict[str, str], dict[str, str]]:
    ids: dict[str, str] = {}
    errors: dict[str, str] = {}
    for key, resource in bundle_summary.get("resources", {}).get(kind, {}).items():
        if resource.get("modified_status") == "deleted":
            continue
        if resource.get("id"):
            ids[key] = str(resource["id"])
        else:
            errors[key] = (
                f'{noun} {key!r} is not deployed. Run `databricks bundle deploy` or use Lakeflow(sync="deploy").'
            )
    return ids, errors


def unversioned_wheel_artifacts(config: Mapping[str, Any]) -> list[str]:
    """Find the wheel artifacts that are deployed under the same file name every time.

    Args:
        config: The parsed output of `databricks bundle summary -o json` or `databricks bundle validate -o json`.

    Returns:
        The sorted names of the artifacts whose `type` is `whl`, or that have no `type` but a `build` command, and
            whose `dynamic_version` is not true.
    """
    names: list[str] = []
    for name, artifact in (config.get("artifacts") or {}).items():
        kind = artifact.get("type")
        is_wheel = kind == "whl" or (kind is None and bool(artifact.get("build")))
        if is_wheel and artifact.get("dynamic_version") is not True:
            names.append(name)
    return sorted(names)


def templates_from_validate(
    config: Mapping[str, Any], root: Path, strategy: str, exists: Callable[[str], bool]
) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    """Build job templates from `bundle validate` output, for the sync and none strategies.

    Validate resolves notebook and script paths but leaves library paths local. With `none`, each local library
    is pointed at the copy that an earlier `bundle deploy` uploaded, after checking that it is there. With `sync`,
    which uploads no libraries, a job with local libraries gets an error instead.

    Args:
        config: The parsed output of `databricks bundle validate -o json`.
        root: The bundle root, against which local library paths are resolved.
        strategy: `sync` or `none`.
        exists: Checks whether a workspace or Volumes path exists.

    Returns:
        The templates by resource key, and an error for each job that cannot be used.
    """
    artifact_path = artifact_path_of(config)
    templates: dict[str, dict[str, Any]] = {}
    errors: dict[str, str] = {}
    for key, job in config.get("resources", {}).get("jobs", {}).items():
        refs = sorted(set(UNRESOLVED.findall(json.dumps(job))))
        if refs:
            errors[key] = (
                f"Job {key!r} contains {', '.join(refs)}, which `bundle validate` does not resolve. "
                'Use Lakeflow(sync="deploy").'
            )
            continue
        local = _local_libraries(job)
        if local and strategy == "sync":
            errors[key] = (
                f"Job {key!r} uses local libraries ({', '.join(local)}), which `bundle sync` does not upload. "
                'Use Lakeflow(sync="deploy").'
            )
            continue
        patched = job
        if local:
            try:
                patched = _map_libraries(job, lambda path: uploaded(path, root, artifact_path, exists))
            except LakeflowBundleError as e:
                errors[key] = f"Job {key!r}: {e}"
                continue
        templates[key] = jobs.JobSettings.from_dict(patched).as_dict()
    return templates, errors


def artifact_path_of(config: Mapping[str, Any]) -> str:
    """Read `workspace.artifact_path` from `bundle summary` or `bundle validate` output, or `""` if it is not set."""
    return (config.get("workspace") or {}).get("artifact_path") or ""


def split_extras(dependency: str) -> tuple[str, str]:
    """Split the extras off a wheel path, such as `("./dist/*.whl", "[test]")` for `"./dist/*.whl[test]"`.

    Args:
        dependency: A wheel path or a requirement.

    Returns:
        The path and its extras, brackets included, or the dependency unchanged and `""` when it is not a wheel path
            followed by extras.
    """
    match = WHEEL_EXTRAS.fullmatch(dependency)
    if match is None:
        return dependency, ""
    return match.group(1), match.group(2)


def stale_uploads(config: Mapping[str, Any], root: Path, modified_at: Callable[[str], int | None]) -> list[str]:
    """Find the local libraries of the bundle's jobs that were built after the copies an earlier deploy uploaded.

    Args:
        config: The parsed output of `databricks bundle validate -o json`.
        root: The bundle root, against which local library paths are resolved.
        modified_at: The time a workspace or Volumes path was last modified, in epoch milliseconds, or `None` if it
            does not exist.

    Returns:
        The sorted paths, relative to `root`, of the local files more than `CLOCK_SKEW_MS` newer than their uploaded
            copies. A file without an uploaded copy is left out, because `templates_from_validate` reports it.
    """
    artifact_path = artifact_path_of(config)
    remote_of: dict[str, str] = {}
    for job in config.get("resources", {}).get("jobs", {}).values():
        for pattern in _local_libraries(job):
            local, _ = split_extras(pattern)
            for match in _matches(local, root):
                remote_of[match] = _remote(artifact_path, match)
    stale: list[str] = []
    for match, remote in remote_of.items():
        uploaded = modified_at(remote)
        if uploaded is None:
            continue
        if Path(match).stat().st_mtime * 1000 - uploaded > CLOCK_SKEW_MS:
            stale.append(os.path.relpath(match, root))
    return sorted(stale)


def uploaded(pattern: str, root: Path, artifact_path: str, exists: Callable[[str], bool]) -> list[str]:
    """Map a local library pattern to the copy of each matching file that `bundle deploy` uploaded.

    Args:
        pattern: A path or glob relative to `root`, such as `./dist/*.whl`. A wheel's extras, as in
            `./dist/*.whl[test]`, are split off before globbing and added back to each uploaded path.
        root: The bundle root.
        artifact_path: The target's `workspace.artifact_path`, under whose `.internal` folder deploys upload.
        exists: Checks whether a workspace or Volumes path exists. It is given the paths without extras.

    Returns:
        The uploaded paths, with the extras.

    Raises:
        LakeflowBundleError: If no file matches, or a match has no uploaded copy.
    """
    local, extras = split_extras(pattern)
    matches = _matches(local, root)
    if not matches:
        raise LakeflowBundleError(f"no file under {root} matches {local!r}. Build and deploy the bundle first.")
    remote = [_remote(artifact_path, match) for match in matches]
    missing = [path for path in remote if not exists(path)]
    if missing:
        raise LakeflowBundleError(f"{', '.join(missing)} not found. Run `databricks bundle deploy` first.")
    return [f"{path}{extras}" for path in remote]


def _matches(pattern: str, root: Path) -> list[str]:
    return sorted(glob.glob(str(root / pattern)))


def _remote(artifact_path: str, match: str) -> str:
    return f"{artifact_path}/.internal/{Path(match).name}"


def _local_libraries(job: Mapping[str, Any]) -> list[str]:
    found: list[str] = []

    def record(path: str) -> list[str]:
        found.append(path)
        return [path]

    _map_libraries(job, record)
    return found


def _map_libraries(job: Mapping[str, Any], fn: Callable[[str], list[str]]) -> dict[str, Any]:
    """Copy a job, replacing each local library path with the paths `fn` returns for it."""
    copied: dict[str, Any] = copy.deepcopy(dict(job))
    for task in _all_tasks(copied):
        if "libraries" in task:
            task["libraries"] = _map_task_libraries(task["libraries"], fn)
    for env in copied.get("environments", []):
        spec = env.get("spec", {})
        if "dependencies" in spec:
            spec["dependencies"] = _map_dependencies(spec["dependencies"], fn)
    return copied


def _all_tasks(job: Mapping[str, Any]) -> list[dict[str, Any]]:
    """The job's tasks, followed by the task that each for_each_task repeats."""
    tasks = list(job.get("tasks", []))
    for task in list(tasks):
        for_each = task.get("for_each_task", {})
        if "task" in for_each:
            tasks.append(for_each["task"])
    return tasks


def _map_task_libraries(libraries: list[dict[str, Any]], fn: Callable[[str], list[str]]) -> list[dict[str, Any]]:
    mapped: list[dict[str, Any]] = []
    for library in libraries:
        kind = _local_library_kind(library)
        if kind is None:
            mapped.append(library)
            continue
        for path in fn(library[kind]):
            mapped.append({kind: path})
    return mapped


def _local_library_kind(library: Mapping[str, Any]) -> str | None:
    for kind in ("whl", "jar"):
        if _is_local(library.get(kind)):
            return kind
    return None


def _map_dependencies(dependencies: list[str], fn: Callable[[str], list[str]]) -> list[str]:
    mapped: list[str] = []
    for dep in dependencies:
        if _is_local_dependency(dep):
            mapped.extend(fn(dep))
        else:
            mapped.append(dep)
    return mapped


def _is_local_dependency(dep: str) -> bool:
    if dep.startswith(("./", "../")):
        return True
    return dep.endswith((".whl", ".jar")) and _is_local(dep)


def _is_local(path: object) -> bool:
    return isinstance(path, str) and bool(path) and not path.startswith("/") and ":" not in path
