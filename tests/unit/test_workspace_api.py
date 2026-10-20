"""The workspace client and the lookups shared across the plugin."""

from __future__ import annotations

import logging
import random
import time
from pathlib import Path
from typing import Any, cast

import pytest

from pytest_databricks_lakeflow import Lakeflow, workspace_api

from .conftest import UNIT_TESTS, FakeCli, as_client
from .fakes.workspace import TOKEN_RACE, ContendedTokenCache, FakeWorkspaceClient


class TestWorkspaceClient:
    """How `Lakeflow` gets its workspace client, and which profile reaches the CLI."""

    def test_client_construction_retries_token_contention(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        contended = ContendedTokenCache(collisions=2)
        sleeps: list[float] = []
        monkeypatch.setattr(workspace_api, "WorkspaceClient", contended)
        monkeypatch.setattr(time, "sleep", sleeps.append)
        monkeypatch.setattr(random, "uniform", lambda low, high: 0)
        with caplog.at_level(logging.INFO, logger="pytest_databricks_lakeflow"):
            assert cast(Any, workspace_api.make_workspace_client("dev")) == "client"
        assert contended.profiles == ["dev", "dev", "dev"]
        assert sleeps == [2, 4]
        assert caplog.messages == [f"Retrying workspace client after: {TOKEN_RACE}"] * 2

    @pytest.mark.parametrize(
        ("bound", "expected"), [pytest.param(0, [2, 4], id="lower"), pytest.param(1, [4, 8], id="upper")]
    )
    def test_collided_workers_retry_at_different_times(
        self, monkeypatch: pytest.MonkeyPatch, bound: int, expected: list[float]
    ) -> None:
        sleeps: list[float] = []
        monkeypatch.setattr(workspace_api, "WorkspaceClient", ContendedTokenCache(collisions=2))
        monkeypatch.setattr(time, "sleep", sleeps.append)
        monkeypatch.setattr(random, "uniform", lambda low, high: (low, high)[bound])
        workspace_api.make_workspace_client("dev")
        assert sleeps == expected

    def test_client_construction_gives_up_after_the_last_attempt(self, monkeypatch: pytest.MonkeyPatch) -> None:
        contended = ContendedTokenCache(collisions=workspace_api.CLIENT_ATTEMPTS)
        monkeypatch.setattr(workspace_api, "WorkspaceClient", contended)
        monkeypatch.setattr(time, "sleep", lambda seconds: None)
        with pytest.raises(ValueError, match="cannot get access token"):
            workspace_api.make_workspace_client("dev")
        assert len(contended.profiles) == workspace_api.CLIENT_ATTEMPTS

    def test_client_construction_fails_fast_on_other_errors(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def broken(profile: str | None = None) -> None:
            raise ValueError("default auth: cannot configure default credentials")

        monkeypatch.setattr(workspace_api, "WorkspaceClient", broken)
        monkeypatch.setattr(time, "sleep", lambda seconds: pytest.fail("should not sleep"))
        with pytest.raises(ValueError, match="cannot configure default credentials"):
            workspace_api.make_workspace_client(None)

    def test_workspace_client_is_made_once_from_the_profile(self, fake_ws: FakeWorkspaceClient) -> None:
        lakeflow = Lakeflow(profile="dev")
        assert lakeflow.ws is cast(Any, fake_ws)
        assert lakeflow.ws is cast(Any, fake_ws)
        assert fake_ws.profiles == ["dev"]

    def test_a_given_workspace_client_is_used_as_is(self, fake_ws: FakeWorkspaceClient) -> None:
        client = as_client(FakeWorkspaceClient())
        assert Lakeflow(workspace_client=client).ws is client
        assert fake_ws.profiles == []

    def test_a_given_workspace_client_profile_goes_to_the_cli(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        pytester.makeconftest(
            f"""
            import sys

            import pytest

            from pytest_databricks_lakeflow import Lakeflow

            sys.path.insert(0, {str(UNIT_TESTS)!r})

            from fakes.templates import example_settings
            from fakes.workspace import FakeWorkspaceClient

            CLIENT = FakeWorkspaceClient()
            CLIENT.config.profile = "from-client"
            CLIENT.jobs.add(example_settings())


            @pytest.fixture(scope="session")
            def lakeflow():
                return Lakeflow(workspace_client=CLIENT)
            """
        )
        pytester.makepyfile(
            """
            import conftest

            def test_one(lakeflow):
                assert lakeflow.ws is conftest.CLIENT
                assert lakeflow.template("my_job").tasks
            """
        )
        pytester.runpytest().assert_outcomes(passed=1)
        assert fake_ws.profiles == []
        assert {profile for args, _, profile in fake_cli.invocations if args[0] == "bundle"} == {"from-client"}
