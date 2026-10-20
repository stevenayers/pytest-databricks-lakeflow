"""Download and read the artefacts that a dbt task leaves behind."""

from __future__ import annotations

import json
import logging
import shutil
import tarfile
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from databricks.sdk.service import jobs

log = logging.getLogger("pytest_databricks_lakeflow")

MAX_ARTIFACT_BYTES = 50 * 1024 * 1024
FAILED_STATUSES = frozenset({"error", "fail"})
TIMEOUT_SECONDS = 60
CHUNK_BYTES = 1024 * 1024
ARCHIVE_NAME = "artifacts.tar.gz"
EXTRACTED_NAME = "artifacts"


class _Unreadable(Exception):
    """The artefacts could not be read. The message never holds the link, whose query string signs it."""


@dataclass(frozen=True)
class DbtResult:
    """The outcome of one dbt node, a model, test, seed or snapshot, as `run_results.json` reports it.

    Attributes:
        unique_id: The node's unique id, such as `model.my_project.orders`.
        status: What dbt reported: `success`, `error` or `skipped` for a model, seed or snapshot, and `pass`, `fail`,
            `warn`, `error` or `skipped` for a test.
        message: dbt's message for the node, such as the database error, or `None`.
        failures: The number of failing rows of a test, or `None` for any other node.
        execution_time: How long the node took, in seconds.
    """

    unique_id: str
    status: str
    message: str | None = None
    failures: int | None = None
    execution_time: float = 0.0


@dataclass(frozen=True, repr=False)
class DbtArtifacts:
    """The artefacts a dbt task wrote, downloaded and extracted once its run finished.

    The platform runs each of the task's commands in turn, so `run_results.json` describes the last command that
    wrote one, such as `dbt build` after `dbt deps`.

    Attributes:
        artifacts_dir: Where the tarball was extracted, in a temporary directory removed at the end of the pytest
            session, unless `keep` or `--lakeflow-keep` is set.
        results: One `DbtResult` per node in `run_results.json`, by unique id, in the order dbt reported them.
        run_results: `run_results.json` as parsed JSON.
        manifest: `manifest.json` from the same folder as parsed JSON, or `None` when the tarball has none.
    """

    artifacts_dir: Path
    results: dict[str, DbtResult]
    run_results: dict[str, Any]
    manifest: dict[str, Any] | None

    @property
    def failed_models(self) -> list[str]:
        """The unique ids of the nodes that ended `error` or `fail`, in the order dbt reported them."""
        failed: list[str] = []
        for unique_id, result in self.results.items():
            if result.status in FAILED_STATUSES:
                failed.append(unique_id)
        return failed

    @property
    def all_passed(self) -> bool:
        """Whether no node ended `error` or `fail`. A node skipped because an upstream node failed is not counted."""
        return not self.failed_models

    def __repr__(self) -> str:
        return f"DbtArtifacts({len(self.results)} results, failed={self.failed_models!r})"


def read_artifacts(
    task_key: str, output: jobs.DbtOutput, dest: Path, max_bytes: int = MAX_ARTIFACT_BYTES
) -> DbtArtifacts | None:
    """Download a dbt task's artefacts tarball into `dest`, extract it there and parse it.

    Args:
        task_key: The task's key, named in the warning when the artefacts cannot be read.
        output: The task's `dbt_output`, with the pre-signed `artifacts_link` and the headers to fetch it with.
        dest: A directory for this task run's artefacts, created if missing.
        max_bytes: The largest tarball to download.

    Returns:
        The parsed artefacts, or `None` when the output has no link or the artefacts cannot be read, which logs a
        warning naming the task.
    """
    if not output.artifacts_link:
        return None
    try:
        archive = download(output.artifacts_link, output.artifacts_headers or {}, dest / ARCHIVE_NAME, max_bytes)
        directory = extract(archive, dest / EXTRACTED_NAME)
        return parse(directory)
    except Exception as e:
        # Anything that goes wrong here, from a body cut off mid-read (http.client.IncompleteRead) to a truncated
        # gzip stream (EOFError), leaves the task's dbt None with a warning rather than failing the whole run.
        log.warning("Could not read the dbt artefacts of task %r: %s", task_key, _reason(e, output.artifacts_link))
        return None


def _reason(error: Exception, link: str) -> str:
    # The link's query string signs it, so it never reaches the log. urllib's HTTP errors leave it out anyway, but a
    # malformed URL is quoted in full.
    message = str(error).replace(link, "<artifacts_link>") or type(error).__name__
    if isinstance(error, tarfile.ReadError):
        return f"not a tarball ({message})"
    if isinstance(error, EOFError):
        return f"the tarball is truncated ({message})"
    return message


def download(link: str, headers: Mapping[str, str], path: Path, max_bytes: int = MAX_ARTIFACT_BYTES) -> Path:
    """Stream the body of `link` into `path`, stopping once it is larger than `max_bytes`.

    Raises:
        OSError: If the request fails, such as with HTTP 403 once the link has expired.
        http.client.HTTPException: If the body is cut off part-way, which removes what was written.
        _Unreadable: If the body is larger than `max_bytes`, which removes what was written.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    too_large = _Unreadable(f"the tarball is larger than {max_bytes} bytes")
    with _open(link, headers) as response:
        length = response.headers.get("Content-Length")
        if length is not None and length.isdigit() and int(length) > max_bytes:
            raise too_large
        try:
            written = _write(response, path, max_bytes)
        except Exception:
            path.unlink(missing_ok=True)
            raise
    if written > max_bytes:
        path.unlink()
        raise too_large
    return path


def _write(response: Any, path: Path, max_bytes: int) -> int:
    written = 0
    with path.open("wb") as file:
        while True:
            chunk = response.read(CHUNK_BYTES)
            written += len(chunk)
            if not chunk or written > max_bytes:
                break
            file.write(chunk)
    return written


def _open(link: str, headers: Mapping[str, str]) -> Any:
    request = urllib.request.Request(link, headers=dict(headers))
    return urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS)


def extract(archive: Path, directory: Path) -> Path:
    """Extract `archive` into `directory` with the `data` filter, then remove the archive.

    The filter refuses members that would land outside `directory`, links that point outside it, and device files.

    Raises:
        tarfile.TarError: If the archive is not a tarball or a member is refused, which removes `directory`.
        EOFError: If the archive is truncated, which removes `directory`.
        _Unreadable: If this Python has no extraction filters, which came in 3.11.4.
    """
    if not hasattr(tarfile, "data_filter"):
        archive.unlink()
        raise _Unreadable("extracting the tarball safely needs Python 3.11.4 or later")
    try:
        with tarfile.open(archive) as tar:
            tar.extractall(directory, filter="data")
    except Exception:
        shutil.rmtree(directory, ignore_errors=True)
        raise
    finally:
        archive.unlink()
    return directory


def parse(artifacts_dir: Path) -> DbtArtifacts:
    """Read `run_results.json`, and `manifest.json` beside it, from an extracted artefacts tarball.

    The first `run_results.json` by depth and then by path is read, so `target/run_results.json` wins over one in a
    subfolder.

    Raises:
        _Unreadable: If there is no `run_results.json`, or a file or a result in it is not a JSON object.
        ValueError: If a file is not JSON.
    """
    found = sorted(artifacts_dir.rglob("run_results.json"), key=lambda p: (len(p.parts), str(p)))
    if not found:
        raise _Unreadable("the tarball has no run_results.json")
    run_results = _read_object(found[0])
    manifest = None
    manifest_path = found[0].with_name("manifest.json")
    if manifest_path.is_file():
        manifest = _read_object(manifest_path)
    results: dict[str, DbtResult] = {}
    for entry in run_results.get("results") or []:
        if not isinstance(entry, Mapping):
            raise _Unreadable("run_results.json has a result that is not a JSON object")
        result = _result(entry)
        results[result.unique_id] = result
    return DbtArtifacts(artifacts_dir=artifacts_dir, results=results, run_results=run_results, manifest=manifest)


def _read_object(path: Path) -> dict[str, Any]:
    loaded = json.loads(path.read_text())
    if not isinstance(loaded, dict):
        raise _Unreadable(f"{path.name} is not a JSON object")
    return loaded


def _result(entry: Mapping[str, Any]) -> DbtResult:
    return DbtResult(
        unique_id=str(entry.get("unique_id") or ""),
        status=str(entry.get("status") or ""),
        message=entry.get("message"),
        failures=entry.get("failures"),
        execution_time=float(entry.get("execution_time") or 0.0),
    )
