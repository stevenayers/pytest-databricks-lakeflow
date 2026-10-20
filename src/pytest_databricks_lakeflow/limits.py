"""The limits the Jobs, Pipelines and Workspace APIs enforce, as measured against a workspace.

Each comment records how the platform refused what went over the limit, so that a change here can be checked against
the same request.
"""

from __future__ import annotations

MAX_NAME_BYTES = 4096
# pipelines/create refuses a longer name with "Name cannot be longer than 255 characters.", counting characters, not
# bytes: 255 characters of 479 bytes are accepted.
MAX_PIPELINE_NAME_CHARS = 255
MAX_TAG_BYTES = 256
MAX_TAGS = 25
MAX_DESCRIPTION_BYTES = 27700

MAX_PARAMETER_DEFAULT_BYTES = 4096
MAX_ENVIRONMENTS = 10

MAX_VALUE_CHARS = 49152
MAX_KEY_CHARS = 512

# jobs/create refuses a literal for_each_task.inputs longer than this ("Invalid inputs: cannot exceed 5000
# characters"), although the docs give 48 KB for an array that comes from a task value reference.
MAX_FOR_EACH_INPUTS_CHARS = 5000
# jobs/create refuses job parameters longer than this as JSON: "The provided parameters (in json representation) are
# too long (10032 bytes); it cannot exceed 10000 bytes." for one parameter `p` with a default of 10,000 characters.
# It counts 29 characters for each definition plus its name and its JSON-escaped default, a comma between two
# definitions and 2 for the brackets, and a non-ASCII character as one. The limit is on the parameter list as a whole,
# not on each value as the For each docs say, so the input's room depends on every other parameter of the copy.
MAX_PARAMETERS_JSON_CHARS = 10_000

TIMEOUT_GRACE_SECONDS = 60

# dbutils.widgets.text refuses a longer default with DefaultValueIsTooLong ("longer than 2048 bytes").
WIDGET_LIMIT_BYTES = 2048
