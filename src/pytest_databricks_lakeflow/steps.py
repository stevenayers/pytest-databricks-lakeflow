"""How a `setup=` or `teardown=` step runs: from the test process over Databricks Connect, or as a task of the job.

Over Connect a step runs on serverless compute, or on `existing_cluster_id`, through one Databricks Connect session
per `Lakeflow`. The session is ready in about two seconds and each statement takes about half a second, where a task
waits about 30 seconds for serverless compute first. A step runs as a task when it is a `Path` to a notebook file,
which needs a notebook, when `run_as_task` is set, and when databricks-connect cannot be imported.
"""

from __future__ import annotations

import importlib
import importlib.util
import json
import logging
import string
import time
import traceback
import warnings
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from databricks.sdk.service import jobs

from pytest_databricks_lakeflow import helpers, session
from pytest_databricks_lakeflow.helpers import LifecycleStep
from pytest_databricks_lakeflow.results.models import StepResult, TaskError
from pytest_databricks_lakeflow.settings.lifecycle import lifecycle_params

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

    from pytest_databricks_lakeflow.client import Lakeflow

log = logging.getLogger("pytest_databricks_lakeflow")
CONNECT_MISSING = (
    "databricks-connect is not installed, so setup= and teardown= run as tasks, about 30 seconds each. "
    "pytest-databricks-lakeflow installs it on Python 3.11 and 3.12, but no release installs on Python 3.13 or later. "
    "Run the tests on Python 3.12 to run steps over Databricks Connect in a few seconds, or pass run_as_task=True to "
    "Lakeflow to choose tasks and silence this warning."
)


@dataclass(frozen=True)
class LocalStep:
    """A `setup=` or `teardown=` step that runs over Databricks Connect.

    Attributes:
        role: `"setup"` or `"teardown"`.
        statements: The SQL statements, for a list of them.
        function: The function, for a function.
        arguments: The arguments the function declares, of `spark`, `dbutils` and `params`.
        names: The `${name}` placeholders of the statements.
    """

    role: str
    statements: tuple[str, ...] | None = None
    function: Callable[..., object] | None = field(default=None, compare=False)
    arguments: tuple[str, ...] = ()
    names: frozenset[str] = frozenset()


def connect_available() -> bool:
    """Whether databricks-connect can be imported, found without importing it."""
    try:
        return importlib.util.find_spec("databricks.connect") is not None
    except ModuleNotFoundError:
        return False


def runs_as_task(step: LifecycleStep, run_as_task: bool) -> bool:
    """Whether `step` runs as a task: a notebook file, `run_as_task`, or no databricks-connect, which warns once."""
    if isinstance(step, Path) or run_as_task:
        return True
    if connect_available():
        return False
    current = session.current()
    if not current.warned_connect:
        current.warned_connect = True
        warnings.warn(CONNECT_MISSING, stacklevel=5)
    return True


def check_step(step: LifecycleStep, role: str, run_as_task: bool) -> None:
    """Check a `Lakeflow` default for `setup` or `teardown` as far as it can be without a test.

    Raises:
        ValueError: In the cases where the step would be refused when a job uses it.
    """
    if isinstance(step, Path) or run_as_task:
        helpers.check_lifecycle_step(step, role)
        return
    local_step(step, role)


def local_step(step: LifecycleStep, role: str) -> LocalStep:
    """Check a function or a list of SQL statements for a run over Connect, and wrap it.

    A function may be a lambda or a closure, because it runs in this process; it can take only `spark`, `dbutils` and
    `params`.

    Raises:
        ValueError: If `step` is a string, an empty list, a list with a statement that is not a string or has a stray
            `$`, something that cannot be called, or a function that takes another parameter.
    """
    if isinstance(step, (str, Sequence)):
        _, names = helpers.lifecycle_source(step, Path(), role)
        return LocalStep(role=role, statements=tuple(step), names=names)
    if not callable(step):
        raise ValueError(f"{role}= must be a function, a list of SQL statements or a Path to a .py file, got {step!r}")
    name = getattr(step, "__name__", repr(step))
    arguments = helpers.declared_arguments(step, f"{role}={name}")
    return LocalStep(role=role, function=step, arguments=tuple(arguments))


def step_params(settings: jobs.JobSettings, params: Mapping[str, str]) -> dict[str, str]:
    """The job parameters a step reads: each the job declares, at its value for the run or its default, then the rest.

    A default that refers to a `{{...}}` value is passed as it is, because only the platform resolves it.

    Args:
        settings: The job's settings.
        params: The job parameters of the run.

    Returns:
        The parameters by name, without those the plugin injects.
    """
    defaults: dict[str, str] = {}
    for parameter in settings.parameters or []:
        if parameter.name:
            defaults[parameter.name] = parameter.default or ""
    values: dict[str, str] = {}
    for name in lifecycle_params(settings, params):
        values[name] = params.get(name, defaults.get(name, ""))
    return values


def session_for(lakeflow: Lakeflow) -> Any:
    """The Databricks Connect session of `lakeflow`, made on first use: serverless, or on `existing_cluster_id`."""
    if lakeflow._connect is None:
        lakeflow._connect = connect_session(lakeflow.ws, lakeflow.existing_cluster_id)
    return lakeflow._connect


def stop_sessions(state: session.SessionState) -> None:
    """Stop the Databricks Connect session of every `Lakeflow` this process used, once its steps have run.

    A session that fails to stop is only logged: the platform ends an idle session on its own.
    """
    used: dict[int, Lakeflow] = {}
    for lakeflow in [*state.lakeflows, *state.once_lakeflows.values()]:
        used[id(lakeflow)] = lakeflow
    for lakeflow in used.values():
        if lakeflow._connect is None:
            continue
        try:
            lakeflow._connect.stop()
        except Exception as e:
            log.debug(f"Could not stop a Databricks Connect session: {e}")
        lakeflow._connect = None


def connect_session(ws: WorkspaceClient, cluster_id: str | None) -> Any:
    """Open a Databricks Connect session with the workspace client's configuration.

    Args:
        ws: The workspace client, whose configuration, credentials included, the session uses.
        cluster_id: A running cluster to connect to, or `None` for serverless compute.

    Returns:
        A new Spark session.
    """
    # Databricks Connect refuses sdkConfig() beside serverless() or clusterId() ("sdkConfig must not be set when
    # connection parameters are explicitly configured"), so the compute goes on the config. Its copy() shares the
    # fields with the client's config, so the copy gets fields of its own and the client is left as it was.
    fields = dict(ws.config._inner)
    if cluster_id:
        fields.pop("serverless_compute_id", None)
        fields["cluster_id"] = cluster_id
    else:
        fields.pop("cluster_id", None)
        fields["serverless_compute_id"] = "auto"
    config = ws.config.copy()
    config._inner = fields
    return importlib.import_module("databricks.connect").DatabricksSession.builder.sdkConfig(config).create()


def run_local(spark: Any, dbutils: Any, step: LocalStep, params: Mapping[str, str]) -> StepResult:
    """Run a step over Connect and describe how it went. What the step raises is reported, not raised.

    Args:
        spark: The Databricks Connect session.
        dbutils: The workspace client's `dbutils`, which reads files and secrets remotely.
        step: The step.
        params: The job parameters the step reads.

    Returns:
        The step's outcome. Its output is the function's return value as JSON, or `{"statements": <count>}`.
    """
    started = time.monotonic()
    try:
        output = _run(spark, dbutils, step, params)
    except Exception as e:
        error = TaskError(type=_exception_name(e), message=str(e), trace=traceback.format_exc())
        return StepResult(role=step.role, ran_as="connect", succeeded=False, error=error, duration=_since(started))
    return StepResult(role=step.role, ran_as="connect", succeeded=True, output=output, duration=_since(started))


def _run(spark: Any, dbutils: Any, step: LocalStep, params: Mapping[str, str]) -> str | None:
    if step.statements is not None:
        for statement in step.statements:
            spark.sql(string.Template(statement).substitute(params))
        return json.dumps({"statements": len(step.statements)})
    assert step.function is not None
    values = {"spark": spark, "dbutils": dbutils, "params": dict(params)}
    result = step.function(**{name: values[name] for name in step.arguments})
    if result is None:
        return None
    return json.dumps(result, default=str)


def _exception_name(error: BaseException) -> str:
    """The exception's class as Python prints it in a traceback: dotted with its module, unless it is a built-in."""
    kind = type(error)
    if kind.__module__ == "builtins":
        return kind.__qualname__
    return f"{kind.__module__}.{kind.__qualname__}"


def _since(started: float) -> timedelta:
    return timedelta(seconds=time.monotonic() - started)


def step_spec(step: LocalStep) -> dict[str, Any]:
    """Describe a step so that another process can run it, as `Lakeflow.once()` does with its teardown.

    Raises:
        ValueError: If the step is a function that cannot run from its source alone: a lambda, a closure, a function
            without readable source or with a decorator.
    """
    if step.statements is not None:
        return {"statements": list(step.statements)}
    assert step.function is not None
    source, name, arguments = helpers.shippable_function(step.function, step.role)
    return {"source": source, "name": name, "arguments": arguments}


def run_spec(spark: Any, dbutils: Any, role: str, spec: Mapping[str, Any], params: Mapping[str, str]) -> StepResult:
    """Run a step that `step_spec` described, building a function again from its source."""
    if "statements" in spec:
        return run_local(spark, dbutils, LocalStep(role=role, statements=tuple(spec["statements"])), params)
    namespace: dict[str, Any] = {}
    # The future import keeps annotations unevaluated, as in the notebook a task runs, since the names they use are
    # not imported here.
    code = compile(f"from __future__ import annotations\n{spec['source']}", f"<{role}>", "exec", dont_inherit=True)
    exec(code, namespace)
    step = LocalStep(role=role, function=namespace[spec["name"]], arguments=tuple(spec["arguments"]))
    return run_local(spark, dbutils, step, params)


def step_record(step: StepResult) -> dict[str, Any]:
    """A step that ran over Connect as JSON, for the xdist workers that did not run it."""
    error = None
    if step.error is not None:
        error = {"type": step.error.type, "message": step.error.message, "trace": step.error.trace}
    return {
        "role": step.role,
        "succeeded": step.succeeded,
        "output": step.output,
        "error": error,
        "seconds": step.duration.total_seconds(),
    }


def step_from_record(record: Mapping[str, Any]) -> StepResult:
    """Read back a step that `step_record` wrote."""
    error = None
    if record["error"] is not None:
        error = TaskError(**record["error"])
    return StepResult(
        role=record["role"],
        ran_as="connect",
        succeeded=record["succeeded"],
        output=record["output"],
        error=error,
        duration=timedelta(seconds=record["seconds"]),
    )
