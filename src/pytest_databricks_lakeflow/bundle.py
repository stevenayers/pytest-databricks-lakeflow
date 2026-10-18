from __future__ import annotations

import copy
import glob
import json
import logging
import re
import subprocess
from collections.abc import Callable, Mapping
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
    cmd = ["databricks", *args, *(["--profile", profile] if profile else [])]
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
    version = tuple(int(part) for part in match.groups()) if match else None
    if version and (0, 0, 0) < version < MIN_CLI_VERSION:
        minimum = ".".join(map(str, MIN_CLI_VERSION))
        raise LakeflowBundleError(f"pytest-databricks-lakeflow needs Databricks CLI v{minimum} or later: {out.strip()}")


def deploy(root: Path, target: str, profile: str | None, variables: Mapping[str, str] | None = None) -> None:
    log.info("Deploying bundle %s to target %s", root, target)
    run_cli(["bundle", "deploy", "-t", target, "--auto-approve", *_var_args(variables)], root, profile)


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
    return [arg for name, value in sorted((variables or {}).items()) for arg in ("--var", f"{name}={value}")]


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
    ids: dict[str, int] = {}
    errors: dict[str, str] = {}
    for key, job in bundle_summary.get("resources", {}).get("jobs", {}).items():
        if job.get("modified_status") == "deleted":
            continue
        if job.get("id"):
            ids[key] = int(job["id"])
        else:
            errors[key] = f'Job {key!r} is not deployed. Run `databricks bundle deploy` or use Lakeflow(sync="deploy").'
    return ids, errors


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
    artifact_path = (config.get("workspace") or {}).get("artifact_path", "")
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
                patched = _map_libraries(job, lambda path: _uploaded(path, root, artifact_path, exists))
            except LakeflowBundleError as e:
                errors[key] = f"Job {key!r}: {e}"
                continue
        templates[key] = jobs.JobSettings.from_dict(patched).as_dict()
    return templates, errors


def _uploaded(pattern: str, root: Path, artifact_path: str, exists: Callable[[str], bool]) -> list[str]:
    matches = sorted(glob.glob(str(root / pattern)))
    if not matches:
        raise LakeflowBundleError(f"no file under {root} matches {pattern!r}. Build and deploy the bundle first.")
    remote = [f"{artifact_path}/.internal/{Path(match).name}" for match in matches]
    missing = [path for path in remote if not exists(path)]
    if missing:
        raise LakeflowBundleError(f"{', '.join(missing)} not found. Run `databricks bundle deploy` first.")
    return remote


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
    tasks = [*copied.get("tasks", [])]
    tasks += [t["for_each_task"]["task"] for t in tasks if "task" in t.get("for_each_task", {})]
    for task in tasks:
        if "libraries" not in task:
            continue
        libraries = []
        for library in task["libraries"]:
            kind = next((k for k in ("whl", "jar") if _is_local(library.get(k))), None)
            libraries += [{kind: path} for path in fn(library[kind])] if kind else [library]
        task["libraries"] = libraries
    for env in copied.get("environments", []):
        spec = env.get("spec", {})
        if "dependencies" in spec:
            spec["dependencies"] = [path for dep in spec["dependencies"] for path in _dependency(dep, fn)]
    return copied


def _dependency(dep: str, fn: Callable[[str], list[str]]) -> list[str]:
    local = dep.startswith(("./", "../")) or (dep.endswith((".whl", ".jar")) and _is_local(dep))
    return fn(dep) if local else [dep]


def _is_local(path: object) -> bool:
    return isinstance(path, str) and bool(path) and not path.startswith("/") and ":" not in path
