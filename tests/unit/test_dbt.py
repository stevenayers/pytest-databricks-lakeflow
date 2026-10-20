from __future__ import annotations

import logging
from pathlib import Path

import pytest
from databricks.sdk.service import jobs

from pytest_databricks_lakeflow import dbt

from .fakes import DBT_HEADERS, DBT_LINK, FakeDbtLinks, dbt_tarball

OUTPUT = jobs.DbtOutput(artifacts_link=DBT_LINK, artifacts_headers=DBT_HEADERS)
ORDERS_AND_BAD = {"model.proj.orders": "success", "model.proj.bad": "error"}


@pytest.fixture
def links(monkeypatch: pytest.MonkeyPatch) -> FakeDbtLinks:
    fake = FakeDbtLinks()
    monkeypatch.setattr(dbt, "_open", fake)
    return fake


class TestParse:
    """Reading the artefacts tarball of a dbt task into DbtArtifacts."""

    def test_results_failed_models_and_manifest(self, links: FakeDbtLinks, tmp_path: Path) -> None:
        links.bodies[DBT_LINK] = dbt_tarball(ORDERS_AND_BAD)
        artifacts = dbt.read_artifacts("dbt_build", OUTPUT, tmp_path)
        assert artifacts is not None
        assert links.requests == [(DBT_LINK, DBT_HEADERS)]
        assert artifacts.results["model.proj.orders"].status == "success"
        assert artifacts.results["model.proj.bad"].message == "Database Error"
        assert artifacts.failed_models == ["model.proj.bad"]
        assert artifacts.all_passed is False
        assert artifacts.run_results["metadata"] == {"dbt_version": "1.9.0"}
        assert artifacts.manifest is not None
        assert sorted(artifacts.manifest["nodes"]) == ["model.proj.bad", "model.proj.orders"]
        assert (artifacts.artifacts_dir / "target" / "run_results.json").is_file()
        assert artifacts.artifacts_dir.is_relative_to(tmp_path)
        # Only the extracted files are kept, not the tarball.
        assert list(tmp_path.rglob("*.tar.gz")) == []

    @pytest.mark.parametrize(
        ("statuses", "failed"),
        [
            pytest.param(
                {"seed.p.s": "success", "model.p.a": "success", "test.p.t": "pass", "test.p.w": "warn"},
                [],
                id="passed",
            ),
            pytest.param(
                {"model.p.a": "success", "model.p.bad": "error", "test.p.t": "fail", "model.p.after": "skipped"},
                ["model.p.bad", "test.p.t"],
                id="failed",
            ),
        ],
    )
    def test_failed_models_are_errors_and_failures(
        self, links: FakeDbtLinks, tmp_path: Path, statuses: dict[str, str], failed: list[str]
    ) -> None:
        links.bodies[DBT_LINK] = dbt_tarball(statuses)
        artifacts = dbt.read_artifacts("dbt_build", OUTPUT, tmp_path)
        assert artifacts is not None
        assert artifacts.failed_models == failed
        assert artifacts.all_passed is (not failed)

    def test_a_missing_manifest_is_none(self, links: FakeDbtLinks, tmp_path: Path) -> None:
        links.bodies[DBT_LINK] = dbt_tarball(ORDERS_AND_BAD, manifest=False)
        artifacts = dbt.read_artifacts("dbt_build", OUTPUT, tmp_path)
        assert artifacts is not None
        assert artifacts.manifest is None
        assert list(artifacts.results) == ["model.proj.orders", "model.proj.bad"]

    def test_repr_leaves_out_the_manifest(self, links: FakeDbtLinks, tmp_path: Path) -> None:
        links.bodies[DBT_LINK] = dbt_tarball(ORDERS_AND_BAD)
        artifacts = dbt.read_artifacts("dbt_build", OUTPUT, tmp_path)
        assert repr(artifacts) == "DbtArtifacts(2 results, failed=['model.proj.bad'])"


class TestUnreadable:
    """Artefacts that cannot be read give no DbtArtifacts and a warning naming the task, never the signed link."""

    @pytest.mark.parametrize(
        ("body", "length", "max_bytes", "reason"),
        [
            pytest.param(dbt_tarball(ORDERS_AND_BAD), None, 100, "larger than 100 bytes", id="body-over-the-limit"),
            pytest.param(
                dbt_tarball(ORDERS_AND_BAD), 10**9, dbt.MAX_ARTIFACT_BYTES, "larger than", id="length-over-the-limit"
            ),
            pytest.param(None, None, dbt.MAX_ARTIFACT_BYTES, "HTTP Error 403: Forbidden", id="http-error"),
            pytest.param(
                ValueError(f"unknown url type: {DBT_LINK!r}"),
                None,
                dbt.MAX_ARTIFACT_BYTES,
                "unknown url type: '<artifacts_link>'",
                id="bad-url",
            ),
            pytest.param(b"not a tarball", None, dbt.MAX_ARTIFACT_BYTES, "not a tarball", id="not-a-tarball"),
            pytest.param(
                dbt_tarball(None, extra={"target/manifest.json": b"{}"}),
                None,
                dbt.MAX_ARTIFACT_BYTES,
                "no run_results.json",
                id="no-run-results",
            ),
            pytest.param(
                dbt_tarball(ORDERS_AND_BAD, extra={"../escape.txt": b"x"}),
                None,
                dbt.MAX_ARTIFACT_BYTES,
                "outside the destination",
                id="path-outside",
            ),
        ],
    )
    def test_unreadable_artifacts_log_a_warning(
        self,
        links: FakeDbtLinks,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
        body: bytes | Exception | None,
        length: int | None,
        max_bytes: int,
        reason: str,
    ) -> None:
        if body is not None:
            links.bodies[DBT_LINK] = body
        if length is not None:
            links.lengths[DBT_LINK] = length
        dest = tmp_path / "run" / "123"
        with caplog.at_level(logging.WARNING, logger="pytest_databricks_lakeflow"):
            assert dbt.read_artifacts("dbt_build", OUTPUT, dest, max_bytes=max_bytes) is None
        [record] = caplog.records
        message = record.getMessage()
        assert message.startswith("Could not read the dbt artefacts of task 'dbt_build': ")
        assert reason in message
        assert "sig=secret" not in message
        assert list(tmp_path.rglob("escape.txt")) == []
