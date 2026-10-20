"""The `{{...}}` references a job's settings can hold, and how the platform resolves each of them."""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from typing import Any

from databricks.sdk.service import jobs

# jobs/create refuses any reference to a task that is not in the job: "Invalid reference:
# 'tasks.ghost.result_state'. Task 'ghost' does not exist."
TASK_REFERENCE = re.compile(r"\{\{\s*tasks\.([\w-]+)\.")
# jobs/create refuses a reference to an undeclared parameter: "Reference 'job.parameters.nope' in task 't' refers to
# an undefined job parameter 'nope'."
JOB_PARAMETER_REFERENCE = re.compile(r"\{\{\s*job\.parameters\.([\w-]+)\s*\}\}")
# Of the references to a task that run_now(only=...) leaves out, task values fail the run, and so do outputs
# ("Cannot access output from task '<key>' because the task was skipped."). The others resolve against the task's
# DISABLED run: result_state to "disabled", error_code to "Disabled", run_id to its run id.
TASK_VALUES_REFERENCE = re.compile(r"\{\{\s*tasks\.([\w-]+)\.values\.")
# A stub is a notebook task, which has no SQL, alert or clean room output, so these references are replaced by the
# literals its outputs give. The platform's paths after `output.` include rows, first_row, first_row.<column>,
# alert_state, catalog_name, schema_name and response.
OUTPUT_REFERENCE = re.compile(r"\{\{\s*tasks\.([\w-]+)\.output\.([\w.-]+)\s*\}\}")
# A run-now run resolves {{job.trigger.type}} to one_time and {{job.trigger.time.*}} to the time of the run, and
# jobs/create refuses a table_update or file_arrival reference in a job without that trigger ("Table update trigger
# references can not be used unless the job has a table update trigger configured.", "Job does not have a file
# arrival trigger configured.") and any other path ("Invalid reference: 'job.trigger.nope'. 'trigger' is unknown.").
TRIGGER_REFERENCE = re.compile(r"\{\{\s*job\.trigger\.([\w.]+)\s*\}\}")
# The trigger references that jobs/create refuses in a job without that trigger, by the first part of their path.
TRIGGER_KINDS = {"table_update": "table update", "file_arrival": "file arrival"}
# jobs/create refuses a deeper reference: "Invalid reference: 'input.e.f'. Input reference can only access top-level
# keys." So a deployed template has none, and the pattern matches `{{input}}` and `{{input.<key>}}` only.
INPUT_REFERENCE = re.compile(r"\{\{\s*input(?:\.([^.\s}]+))?\s*\}\}")


def point_at_parameters(text: str, pattern: re.Pattern[str], name_for: Callable[[str], str | None]) -> str:
    """Replace each match of `pattern` in `text` with a reference to the job parameter `name_for` gives its group.

    A group that did not take part in the match is passed as `""`, and a match `name_for` gives no name for is left as
    it is.
    """

    def replacement(match: re.Match[str]) -> str:
        name = name_for(match[1] or "")
        if name is None:
            return match[0]
        return f"{{{{job.parameters.{name}}}}}"

    return pattern.sub(replacement, text)


def own_fields(task: jobs.Task) -> dict[str, Any]:
    """The task as a dict, without the task nested in its `for_each_task`, which `with_nested` lists on its own."""
    own = task.as_dict()
    loop = own.get("for_each_task")
    if loop is not None:
        own["for_each_task"] = {key: value for key, value in loop.items() if key != "task"}
    return own


def escape(value: object) -> str:
    """Encode a value as JSON for a helper widget, escaping `{{` so the platform does not treat it as a reference."""
    return json.dumps(value).replace("{{", "{\\u007b")
