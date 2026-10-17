from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Literal

import pytest

SYNC_STRATEGIES = ("deploy", "sync", "none")

INI_OPTIONS: list[tuple[str, str, Literal["bool"] | None, object]] = [
    ("lakeflow_bundle_root", "Bundle root. Defaults to the nearest directory with a databricks.yml.", None, None),
    ("lakeflow_target", "Bundle target to deploy and read job templates from.", None, "test"),
    ("lakeflow_profile", "Databricks config profile. Empty means the SDK's standard resolution.", None, ""),
    ("lakeflow_sync", "How to prepare the bundle at session start: deploy, sync or none.", None, "deploy"),
    ("lakeflow_timeout_minutes", "Default timeout for one job run, in minutes.", None, "30"),
    ("lakeflow_catalog", "Catalog in which lakeflow_schema creates temporary schemas.", None, ""),
    ("lakeflow_helpers_path", "Workspace folder for the helper notebooks.", None, ""),
    ("lakeflow_helpers_compute", "job_cluster_key for helper tasks. Empty means serverless.", None, ""),
    ("lakeflow_helpers_environment_version", "Serverless environment version for helper tasks.", None, "4"),
    ("lakeflow_sweep_hours", "Age in hours after which --lakeflow-sweep deletes leftover jobs.", None, "24"),
    (
        "lakeflow_disable_auto_optimization",
        "Stop serverless from retrying failed tasks in ephemeral jobs.",
        "bool",
        True,
    ),
    (
        "lakeflow_performance_optimized",
        "Run ephemeral jobs with performance_target PERFORMANCE_OPTIMIZED, whatever the template says.",
        "bool",
        False,
    ),
    (
        "lakeflow_allow_undeclared_params",
        "Allow params that the job does not declare as job parameters.",
        "bool",
        False,
    ),
]


def add_options(parser: pytest.Parser) -> None:
    for name, help_text, kind, default in INI_OPTIONS:
        parser.addini(name, help_text, type=kind, default=default)
    group = parser.getgroup("lakeflow", "Lakeflow Jobs from a Databricks Asset Bundle")
    group.addoption("--lakeflow-target", help="Bundle target. Overrides lakeflow_target.")
    group.addoption("--lakeflow-sync", choices=SYNC_STRATEGIES, help="Overrides lakeflow_sync.")
    group.addoption("--lakeflow-timeout", type=float, metavar="MINUTES", help="Overrides lakeflow_timeout_minutes.")
    group.addoption("--lakeflow-keep", action="store_true", help="Keep every ephemeral job and temporary schema.")
    group.addoption(
        "--lakeflow-keep-failed",
        action="store_true",
        dest="lakeflow_keep_failed",
        default=True,
        help="Keep the ephemeral job when its test fails. This is the default.",
    )
    group.addoption(
        "--no-lakeflow-keep-failed",
        action="store_false",
        dest="lakeflow_keep_failed",
        help="Delete the ephemeral job even when its test fails.",
    )
    group.addoption("--lakeflow-destroy", action="store_true", help="Run bundle destroy at the end of the session.")
    group.addoption("--lakeflow-sweep", action="store_true", help="Delete leftover ephemeral jobs at session start.")


@dataclass(frozen=True)
class Options:
    bundle_root: Path | None
    target: str
    profile: str | None
    sync: str
    timeout: timedelta
    catalog: str | None
    helpers_path: str | None
    helpers_compute: str | None
    helpers_environment_version: str
    sweep_hours: float
    disable_auto_optimization: bool
    performance_optimized: bool
    allow_undeclared_params: bool
    keep: bool = False
    keep_failed: bool = True
    destroy: bool = False
    sweep: bool = False

    @classmethod
    def from_config(cls, config: pytest.Config) -> Options:
        sync = config.getoption("lakeflow_sync") or config.getini("lakeflow_sync")
        if sync not in SYNC_STRATEGIES:
            raise pytest.UsageError(f"lakeflow_sync must be one of {', '.join(SYNC_STRATEGIES)}, got {sync!r}")
        root = config.getini("lakeflow_bundle_root")
        timeout = config.getoption("lakeflow_timeout") or _number(config, "lakeflow_timeout_minutes")
        return cls(
            bundle_root=(config.rootpath / root).resolve() if root else None,
            target=config.getoption("lakeflow_target") or config.getini("lakeflow_target"),
            profile=config.getini("lakeflow_profile") or None,
            sync=sync,
            timeout=timedelta(minutes=timeout),
            catalog=config.getini("lakeflow_catalog") or None,
            helpers_path=config.getini("lakeflow_helpers_path") or None,
            helpers_compute=config.getini("lakeflow_helpers_compute") or None,
            helpers_environment_version=config.getini("lakeflow_helpers_environment_version"),
            sweep_hours=_number(config, "lakeflow_sweep_hours"),
            disable_auto_optimization=config.getini("lakeflow_disable_auto_optimization"),
            performance_optimized=config.getini("lakeflow_performance_optimized"),
            allow_undeclared_params=config.getini("lakeflow_allow_undeclared_params"),
            keep=config.getoption("lakeflow_keep"),
            keep_failed=config.getoption("lakeflow_keep_failed"),
            destroy=config.getoption("lakeflow_destroy"),
            sweep=config.getoption("lakeflow_sweep"),
        )


def _number(config: pytest.Config, name: str) -> float:
    value = config.getini(name)
    try:
        return float(value)
    except ValueError:
        raise pytest.UsageError(f"{name} must be a number, got {value!r}") from None
