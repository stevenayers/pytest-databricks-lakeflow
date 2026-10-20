"""The pre-signed artefact links of dbt tasks and the tarballs behind them."""

from __future__ import annotations

import http.client
import json
import tarfile
import urllib.error
import urllib.parse
from collections.abc import Mapping
from email.message import Message
from io import BytesIO

# A pre-signed link to a dbt task's artefacts, with a signature that must never reach a log line or report.json.
DBT_LINK = "https://storage.example.com/dbt/artifacts.tar.gz?sig=secret"
DBT_HEADERS = {"x-ms-blob-type": "BlockBlob"}


class FakeDbtResponse(BytesIO):
    """The body of a pre-signed link, with the headers the storage service sends with it.

    A response with `cut` set stops after that many bytes with `http.client.IncompleteRead`, as a connection dropped
    part-way through the body does.
    """

    def __init__(self, body: bytes, headers: Mapping[str, str], cut: int | None = None) -> None:
        super().__init__(body)
        self.headers = dict(headers)
        self.cut = cut

    def read(self, size: int | None = -1) -> bytes:
        if self.cut is not None and self.tell() >= self.cut:
            raise http.client.IncompleteRead(b"", len(self.getvalue()) - self.cut)
        if self.cut is not None and size is not None and size >= 0:
            size = min(size, self.cut - self.tell())
        return super().read(size)


class FakeDbtLinks:
    """Stands in for `dbt._open`, serving the tarball in `bodies` for each link and recording every request.

    A link without a body fails with HTTP 403, as an expired pre-signed link does, and one whose body is an exception
    raises it. A link in `lengths` is served with that `Content-Length` header instead of the body's own length, and
    one in `cuts` stops part-way through the body after that many bytes.
    """

    def __init__(self) -> None:
        self.bodies: dict[str, bytes | Exception] = {}
        self.lengths: dict[str, int] = {}
        self.cuts: dict[str, int] = {}
        self.requests: list[tuple[str, dict[str, str]]] = []

    def __call__(self, url: str, headers: Mapping[str, str]) -> FakeDbtResponse:
        self.requests.append((url, dict(headers)))
        if url not in self.bodies:
            raise urllib.error.HTTPError(url, 403, "Forbidden", Message(), None)
        body = self.bodies[url]
        if isinstance(body, Exception):
            raise body
        return FakeDbtResponse(body, {"Content-Length": str(self.lengths.get(url, len(body)))}, self.cuts.get(url))


def dbt_tarball(
    statuses: Mapping[str, str] | None, *, manifest: bool = True, extra: Mapping[str, bytes] | None = None
) -> bytes:
    """Build a dbt artefacts tarball whose `target/run_results.json` gives each node in `statuses` that status.

    `statuses` of `None` leaves `run_results.json` out. `manifest` adds `target/manifest.json` naming the same nodes.
    `extra` adds members by name, such as a path that climbs out of the archive.
    """
    members: dict[str, bytes] = {}
    if statuses is not None:
        members.update(_dbt_target(statuses, manifest=manifest))
    members.update(extra or {})
    buffer = BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        for name, data in members.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            archive.addfile(info, BytesIO(data))
    return buffer.getvalue()


def _dbt_target(statuses: Mapping[str, str], *, manifest: bool) -> dict[str, bytes]:
    results = []
    for unique_id, status in statuses.items():
        results.append(
            {
                "unique_id": unique_id,
                "status": status,
                "message": "Database Error" if status in ("error", "fail") else "OK",
                "failures": 1 if status == "fail" else None,
                "execution_time": 0.5,
            }
        )
    members = {
        "target/run_results.json": json.dumps({"metadata": {"dbt_version": "1.9.0"}, "results": results}).encode()
    }
    if manifest:
        nodes = {unique_id: {"unique_id": unique_id, "resource_type": "model"} for unique_id in statuses}
        members["target/manifest.json"] = json.dumps({"nodes": nodes}).encode()
    return members
