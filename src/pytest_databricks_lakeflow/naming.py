"""The names the plugin writes into a workspace, and the patterns that recognise them later.

Every task, parameter, environment and widget the plugin injects starts with `PREFIX`, so that it never collides with
a bundle's own names and so that the sweep and `Lakeflow.attach()` can tell them apart.
"""

from __future__ import annotations

import re

from pytest_databricks_lakeflow.limits import MAX_TAG_BYTES

PREFIX = "__pytest_"
PROBE_PREFIX = f"{PREFIX}probe__"
SETUP_KEY = f"{PREFIX}setup"
TEARDOWN_KEY = f"{PREFIX}teardown"
ENV_KEY = f"{PREFIX}serverless"
TEST_ID_PARAMETER = f"{PREFIX}test_id"
TRIGGER_PARAM_PREFIX = f"{PREFIX}trigger_"
INPUT_PARAMETER = f"{PREFIX}input"

NAME_PREFIX = "[pytest] "
NEWLINES = re.compile(r"[\r\n]+")

ATTRIBUTION_TAGS = ("pytest_ci_url", "pytest_user", "pytest_git_sha", "pytest_git_branch")
# The tag holding the bundle resource key of the job an ephemeral job copies, which attach() reads.
JOB_KEY_TAG = "pytest_job_key"
EXPIRES_TAG = "pytest_expires"
EXPIRES_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

# Widget names carry the injected-task prefix because every job parameter is also pushed to every notebook task's
# widgets, overriding base_parameters of the same name.
VALUES_WIDGET = "__pytest_values"
TASK_KEY_WIDGET = "__pytest_task_key"
KEYS_WIDGET = "__pytest_keys"
PARAMS_WIDGET = "__pytest_params"
MISSING = "__pytest_missing__"

DEBUG_FOLDER = "debug"
NOTEBOOK_DIGEST_CHARS = 12
# `query_<run id>_<task key>_<8 hex digits>.sql`, as `query_file_path` names the SQL file of an inlined saved query.
QUERY_FILE = re.compile(r"query_.+_[0-9a-f]{8}\.sql")
LIFECYCLE_NOTEBOOK = re.compile(rf"(setup|teardown)_[0-9a-f]{{{NOTEBOOK_DIGEST_CHARS}}}")
# `<job_key>__<task_key>__<run id>`, as `EphemeralJob.debug_notebook` names it in the helpers folder's `debug` folder.
DEBUG_NOTEBOOK = re.compile(r".+__.+__(?P<run_id>[0-9]+)")

SCHEMA_SUFFIX = "[0-9a-f]{8}"
PYTEST_SCHEMA = re.compile(rf"pytest_[a-z0-9_]+_{SCHEMA_SUFFIX}")


def default_folder(user: str | None) -> str:
    return f"/Users/{user}/.pytest-databricks-lakeflow"


def schema_prefix(run_id: str) -> str:
    """Start the name of a temporary schema of the run `run_id`: `pytest_<run id>_`, lower case, `_` for the rest."""
    return f"pytest_{re.sub(r'[^a-z0-9_]', '_', run_id.lower())}_"


def tag_value(value: str) -> str:
    """Make a string acceptable as a job tag value: Latin-1, one line, at most 256 UTF-8 bytes."""
    latin1 = "".join(c if ord(c) < 256 else "?" for c in NEWLINES.sub(" ", value))
    return trim(latin1, MAX_TAG_BYTES)


def trim(text: str, max_bytes: int) -> str:
    return text.encode()[:max_bytes].decode(errors="ignore")
