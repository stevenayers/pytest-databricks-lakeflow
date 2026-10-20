"""Session state: the run id, the attribution tags, lazy setup and what xdist workers share."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

import pytest

from pytest_databricks_lakeflow import LakeflowBundleError, LakeflowError, bundle, session

from .conftest import (
    AZURE_DEVOPS,
    DEPLOY,
    DEPLOY_STAGING,
    GITHUB,
    STAGING_FIXTURE,
    SUMMARY,
    SWEEP_LOGGING,
    FakeCli,
    bundle_key,
    configure_lakeflow,
    git,
    make_repository,
    make_xdist_conftest,
    set_ci,
)
from .fakes.workspace import FakeWorkspaceClient

GIT_LOGGING = """

import os
import subprocess

if os.environ.get("PYTEST_XDIST_WORKER"):
    _run = subprocess.run

    def logged_run(args, *rest, **kwargs):
        if args and args[0] == "git":
            with open({calls!r}, "a") as f:
                f.write(" ".join(args) + "\\n")
        return _run(args, *rest, **kwargs)

    subprocess.run = logged_run
"""


class TestSharedSetup:
    """One setup per bundle target, shared between xdist workers through a file in the state directory."""

    def test_session_state_defaults(self, tmp_path: Path) -> None:
        state = session.SessionState(rootpath=tmp_path, run_id="r")
        assert state.kept_jobs == []
        assert state.kept_schemas == []
        assert state.cancelled_runs == 0

    def test_bundle_keys_name_distinct_state_files(self, tmp_path: Path) -> None:
        names = {bundle_key(tmp_path).file_name(), bundle_key(tmp_path, target="ci").file_name()}
        assert len(names) == 2
        assert bundle_key(tmp_path).file_name() == bundle_key(tmp_path).file_name()
        a = bundle_key(tmp_path, variables=(("catalog", "a"),))
        b = bundle_key(tmp_path, variables=(("catalog", "b"),))
        assert len({a.file_name(), b.file_name(), bundle_key(tmp_path).file_name()}) == 3
        assert a.file_name() == bundle_key(tmp_path, variables=(("catalog", "a"),)).file_name()
        flagged = bundle_key(tmp_path, deploy_flags=("--fail-on-active-runs",)).file_name()
        assert flagged not in names
        assert flagged == bundle_key(tmp_path, deploy_flags=("--fail-on-active-runs",)).file_name()

    def test_shared_state_produces_once_for_all_workers(self, tmp_path: Path) -> None:
        produced: list[str] = []

        def produce() -> dict[str, Any]:
            produced.append("gw0")
            return {"templates": {}}

        path = tmp_path / "state.json"
        assert session.shared_state(path, produce) == {"templates": {}}
        assert session.shared_state(path, produce) == {"templates": {}}
        assert produced == ["gw0"]

    def test_shared_state_shares_setup_errors(self, tmp_path: Path) -> None:
        def fail() -> dict[str, Any]:
            raise LakeflowBundleError("deploy failed")

        path = tmp_path / "state.json"
        with pytest.raises(LakeflowBundleError, match="deploy failed"):
            session.shared_state(path, fail)
        with pytest.raises(LakeflowError, match="failed in another xdist worker: LakeflowBundleError: deploy failed"):
            session.shared_state(path, lambda: {"templates": {}})

    def test_xdist_workers_share_one_setup_and_run_id(self, pytester: pytest.Pytester, bundle_dir: Path) -> None:
        pytest.importorskip("xdist")
        log = make_xdist_conftest(pytester)
        run_ids = pytester.path / "run_ids.log"
        test = (
            "def test_{i}(lakeflow):\n"
            "    assert lakeflow.template('my_job').tasks[0].task_key == 'extract'\n"
            f"    with open({str(run_ids)!r}, 'a') as f:\n"
            "        f.write(lakeflow.run_id + '\\n')\n"
        )
        pytester.makepyfile("\n".join(test.format(i=i) for i in range(6)))
        result = pytester.runpytest("-n", "2")
        result.assert_outcomes(passed=6)
        calls = log.read_text().splitlines()
        assert calls.count(" ".join(DEPLOY)) == 1
        assert calls.count(" ".join(SUMMARY)) == 1
        assert len(run_ids.read_text().split()) == 6
        assert len(set(run_ids.read_text().split())) == 1

    def test_xdist_workers_collect_the_same_check_items(self, pytester: pytest.Pytester, bundle_dir: Path) -> None:
        pytest.importorskip("xdist")
        log = make_xdist_conftest(pytester)
        pytester.makepyfile("def test_nothing():\n    pass\n")
        result = pytester.runpytest("-n", "2", "--lakeflow-check", "-v")
        result.assert_outcomes(passed=2)
        result.stdout.fnmatch_lines(["*PASSED*databricks.yml::my_job*"])
        calls = log.read_text().splitlines()
        assert calls.count(" ".join(DEPLOY)) == 1
        assert calls.count(" ".join(SUMMARY)) == 1

    def test_xdist_workers_share_each_bundle_target(self, pytester: pytest.Pytester, bundle_dir: Path) -> None:
        pytest.importorskip("xdist")
        log = make_xdist_conftest(pytester, STAGING_FIXTURE)
        test = "def test_{i}(lakeflow, staging):\n    lakeflow.template('my_job')\n    staging.template('my_job')\n"
        pytester.makepyfile("\n".join(test.format(i=i) for i in range(6)))
        pytester.runpytest("-n", "2").assert_outcomes(passed=6)
        calls = log.read_text().splitlines()
        assert calls.count(" ".join(DEPLOY)) == 1
        assert calls.count(" ".join(DEPLOY_STAGING)) == 1

    def test_xdist_workers_share_attribution(
        self, pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch, bundle_dir: Path
    ) -> None:
        pytest.importorskip("xdist")
        set_ci(monkeypatch, {})
        # Under an outer `-n`, the in-process controller inherits the outer worker's id and would log its own git
        # calls.
        monkeypatch.delenv("PYTEST_XDIST_WORKER", raising=False)
        sha = make_repository(pytester.path)
        calls = pytester.path / "git.log"
        make_xdist_conftest(pytester, GIT_LOGGING.format(calls=str(calls)))
        tags = pytester.path / "tags.log"
        test = (
            "def test_{i}(lakeflow):\n"
            "    with lakeflow.job('my_job'):\n"
            "        pass\n"
            f"    with open({str(tags)!r}, 'a') as f:\n"
            "        tags = lakeflow.ws.jobs.created[-1]['tags']\n"
            "        f.write(json.dumps({{**tags, 'worker': os.environ['PYTEST_XDIST_WORKER']}}) + '\\n')\n"
        )
        pytester.makepyfile("import json\nimport os\n\n" + "\n".join(test.format(i=i) for i in range(6)))
        pytester.runpytest("-n", "2").assert_outcomes(passed=6)
        created = [json.loads(line) for line in tags.read_text().splitlines()]
        assert len(created) == 6
        assert {t["worker"] for t in created} == {"gw0", "gw1"}
        assert {t["pytest_git_sha"] for t in created} == {sha}
        assert {t["pytest_git_branch"] for t in created} == {"topic"}
        assert {t["pytest_user"] for t in created} == {"someone@example.com"}
        assert not calls.exists(), calls.read_text()

    def test_xdist_sweeps_once_with_a_report(self, pytester: pytest.Pytester, bundle_dir: Path) -> None:
        pytest.importorskip("xdist")
        deleted = pytester.path / "deleted.log"
        make_xdist_conftest(pytester, STAGING_FIXTURE + SWEEP_LOGGING.format(deleted=str(deleted)))
        test = "def test_{i}(lakeflow, staging):\n    lakeflow.template('my_job')\n    staging.template('my_job')\n"
        pytester.makepyfile("\n".join(test.format(i=i) for i in range(6)))
        result = pytester.runpytest("-n", "2", "--lakeflow-sweep", "--lakeflow-sweep-report=sweep.json")
        result.assert_outcomes(passed=6)
        assert deleted.read_text().split() == ["1", "2", "7"]
        report = json.loads((pytester.path / "sweep.json").read_text())
        assert (report["dry_run"], report["jobs"]) == (False, [1, 2, 7])


class TestRunCapPerWorker:
    """`--lakeflow-max-concurrent-runs` caps the runs of each xdist worker, not of the whole session."""

    def test_cap_is_per_worker(self, pytester: pytest.Pytester, bundle_dir: Path) -> None:
        pytest.importorskip("xdist")
        make_xdist_conftest(pytester)
        started = pytester.path / "started.log"
        # Each worker holds its one slot until the other worker has started a run too.
        pytester.makepyfile(
            f"""
            import os
            import time

            def started():
                with open({str(started)!r}) as f:
                    return f.read().split()

            def test_start(lakeflow):
                with lakeflow.job("my_job") as job:
                    job.start()
                    with open({str(started)!r}, "a") as f:
                        f.write(os.environ["PYTEST_XDIST_WORKER"] + "\\n")
                    deadline = time.monotonic() + 10
                    while len(started()) < 2 and time.monotonic() < deadline:
                        time.sleep(0.05)
                    assert len(started()) == 2
            """
        )
        result = pytester.runpytest("-n", "2", "--dist", "each", "--lakeflow-max-concurrent-runs=1")
        result.assert_outcomes(passed=2)
        assert sorted(started.read_text().split()) == ["gw0", "gw1"]


class TestRunId:
    """The session's run id comes from the CI environment, or is random."""

    @pytest.mark.parametrize(
        ("env", "expected"),
        [
            ({"LAKEFLOW_RUN_ID": "mine", "GITHUB_RUN_ID": "123"}, "mine"),
            ({"GITHUB_RUN_ID": "123", "BUILD_BUILDID": "456"}, "123"),
            ({"BUILD_BUILDID": "456"}, "456"),
        ],
    )
    def test_run_id_from_environment(self, monkeypatch: pytest.MonkeyPatch, env: dict[str, str], expected: str) -> None:
        set_ci(monkeypatch, env)
        assert session.resolve_run_id() == expected

    def test_random_run_id(self, monkeypatch: pytest.MonkeyPatch) -> None:
        set_ci(monkeypatch, {})
        run_id = session.resolve_run_id()
        assert len(run_id) == 8
        assert run_id != session.resolve_run_id()


class TestAttribution:
    """The CI run, commit, branch and user that tag ephemeral jobs."""

    @pytest.mark.parametrize(
        ("env", "expected"),
        [
            pytest.param(
                {**AZURE_DEVOPS, **GITHUB},
                {
                    "pytest_ci_url": "https://github.com/o/r/actions/runs/9",
                    "pytest_git_sha": "abc",
                    "pytest_git_branch": "main",
                    "pytest_user": "someone",
                },
                id="github-wins-over-azure-devops",
            ),
            pytest.param(
                {**GITHUB, "GITHUB_HEAD_REF": "feature/x"},
                {
                    "pytest_ci_url": "https://github.com/o/r/actions/runs/9",
                    "pytest_git_sha": "abc",
                    "pytest_git_branch": "feature/x",
                    "pytest_user": "someone",
                },
                id="github-head-ref-wins-over-ref-name",
            ),
            pytest.param(
                AZURE_DEVOPS,
                {
                    "pytest_ci_url": "https://dev.azure.com/org/proj/_build/results?buildId=42",
                    "pytest_git_sha": "def",
                    "pytest_git_branch": "feature",
                    "pytest_user": "person@example.com",
                },
                id="azure-devops",
            ),
            pytest.param(
                {**AZURE_DEVOPS, "SYSTEM_TEAMPROJECT": "My Project"},
                {
                    "pytest_ci_url": "https://dev.azure.com/org/My%20Project/_build/results?buildId=42",
                    "pytest_git_sha": "def",
                    "pytest_git_branch": "feature",
                    "pytest_user": "person@example.com",
                },
                id="azure-devops-project-is-url-quoted",
            ),
        ],
    )
    def test_attribution_from_ci(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, env: dict[str, str], expected: dict[str, str]
    ) -> None:
        set_ci(monkeypatch, env)
        assert session.resolve_attribution(tmp_path) == expected

    def test_attribution_from_git(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        set_ci(monkeypatch, {})
        sha = make_repository(tmp_path)
        assert session.resolve_attribution(tmp_path) == {"pytest_git_sha": sha, "pytest_git_branch": "topic"}

    def test_attribution_on_a_detached_head_has_no_branch(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        set_ci(monkeypatch, {})
        sha = make_repository(tmp_path)
        git(tmp_path, "checkout", "-q", "--detach")
        assert session.resolve_attribution(tmp_path) == {"pytest_git_sha": sha}

    def test_attribution_without_git_is_empty(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        set_ci(monkeypatch, {})

        def missing(*args: Any, **kwargs: Any) -> None:
            raise FileNotFoundError("git")

        monkeypatch.setattr(subprocess, "run", missing)
        assert session.resolve_attribution(tmp_path) == {}

    def test_attribution_outside_a_repository_is_empty(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        set_ci(monkeypatch, {})
        monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path.parent))
        assert session.resolve_attribution(tmp_path) == {}


class TestBundleRoot:
    """Where the bundle is found when `bundle_root` is given, relative or absolute, or not given."""

    def test_resolve_root(self, tmp_path: Path) -> None:
        (tmp_path / "databricks.yml").write_text("bundle: {name: x}\n")
        (tmp_path / "tests").mkdir()
        (tmp_path / "sub").mkdir()
        (tmp_path / "sub" / "databricks.yml").write_text("bundle: {name: y}\n")
        assert bundle.resolve_root(None, tmp_path / "tests") == tmp_path
        assert bundle.resolve_root("sub", tmp_path) == tmp_path / "sub"
        assert bundle.resolve_root(tmp_path / "sub", tmp_path / "tests") == tmp_path / "sub"
        with pytest.raises(LakeflowBundleError, match=r"^bundle_root .*examples/bundle has no databricks.yml"):
            bundle.resolve_root("examples/bundle", tmp_path)

    def test_bundle_root_is_relative_to_the_rootdir(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli
    ) -> None:
        pytester.makeini("[pytest]\n")
        pytester.mkdir("bundle").joinpath("databricks.yml").write_text("bundle:\n  name: example\n")
        configure_lakeflow(pytester, 'bundle_root="bundle"')
        pytester.mkdir("tests").joinpath("test_one.py").write_text("def test_one(lakeflow):\n    lakeflow.templates\n")
        pytester.runpytest("tests").assert_outcomes(passed=1)
        assert (DEPLOY, pytester.path / "bundle", None) in fake_cli.invocations


class TestLazySetup:
    """`Lakeflow` sets up each bundle target once, on first use, and remembers a failure for the session."""

    def test_lakeflow_sets_up_once_when_first_used(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        pytester.makepyfile(
            """
            def test_one(lakeflow):
                assert sorted(lakeflow.templates) == ["my_job"]

            def test_two(lakeflow):
                assert lakeflow.template("my_job").tasks[0].task_key == "extract"
            """
        )
        pytester.runpytest().assert_outcomes(passed=2)
        assert fake_cli.calls.count(DEPLOY) == 1
        assert fake_ws.profiles == [None]

    def test_requesting_lakeflow_without_using_it_sets_nothing_up(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        pytester.makepyfile("def test_unused(lakeflow):\n    assert lakeflow.target == 'test'\n")
        pytester.runpytest().assert_outcomes(passed=1)
        assert fake_cli.calls == []
        assert fake_ws.profiles == []

    def test_no_setup_when_lakeflow_tests_are_deselected(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        pytester.makepyfile("def test_one(lakeflow):\n    lakeflow.template('my_job')\n\ndef test_two():\n    pass\n")
        pytester.runpytest("-m", "not lakeflow").assert_outcomes(passed=1, deselected=1)
        assert fake_cli.calls == []

    def test_overriding_the_fixture_configures_lakeflow(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        configure_lakeflow(pytester, 'target="ci", profile="dev"')
        pytester.makepyfile("def test_one(lakeflow):\n    assert lakeflow.template('my_job').tasks\n")
        pytester.runpytest().assert_outcomes(passed=1)
        assert (["bundle", "deploy", "-t", "ci", "--auto-approve"], bundle_dir, "dev") in fake_cli.invocations
        assert fake_ws.profiles == ["dev"]

    def test_lakeflows_for_the_same_bundle_share_one_setup(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        pytester.makeconftest(
            """
            import dataclasses

            import pytest

            from pytest_databricks_lakeflow import Lakeflow


            @pytest.fixture
            def lakeflow():
                return Lakeflow(catalog="a")


            @pytest.fixture
            def other(lakeflow):
                return dataclasses.replace(lakeflow, catalog="b")
            """
        )
        pytester.makepyfile(
            """
            def test_one(lakeflow):
                lakeflow.template("my_job")

            def test_two(lakeflow, other):
                assert other.catalog == "b"
                lakeflow.template("my_job")
                other.template("my_job")
            """
        )
        pytester.runpytest().assert_outcomes(passed=2)
        assert fake_cli.calls.count(DEPLOY) == 1

    def test_each_bundle_target_is_set_up_once(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        configure_lakeflow(pytester, "", STAGING_FIXTURE)
        test = "def test_{0}(lakeflow, staging):\n    lakeflow.template('my_job')\n    staging.template('my_job')\n"
        pytester.makepyfile(test.format("a") + "\n" + test.format("b"))
        pytester.runpytest().assert_outcomes(passed=2)
        assert [call for call in fake_cli.calls if call[:2] == ["bundle", "deploy"]] == [DEPLOY, DEPLOY_STAGING]

    def test_variables_are_part_of_the_bundle_target(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        configure_lakeflow(
            pytester,
            'variables={"catalog": "a"}',
            '\n\n@pytest.fixture(scope="session")\ndef same():\n    return Lakeflow(variables={"catalog": "a"})\n'
            '\n\n@pytest.fixture(scope="session")\ndef other():\n    return Lakeflow(variables={"catalog": "b"})\n',
        )
        pytester.makepyfile(
            "def test_a(lakeflow, same, other):\n"
            "    lakeflow.template('my_job')\n    same.template('my_job')\n    other.template('my_job')\n"
        )
        pytester.runpytest().assert_outcomes(passed=1)
        assert [call for call in fake_cli.calls if call[:2] == ["bundle", "deploy"]] == [
            [*DEPLOY, "--var", "catalog=a"],
            [*DEPLOY, "--var", "catalog=b"],
        ]

    def test_deploy_flags_from_the_fixture(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        configure_lakeflow(pytester, 'deploy_flags=["--fail-on-active-runs"]')
        pytester.makepyfile("def test_one(lakeflow):\n    assert lakeflow.template('my_job').tasks\n")
        pytester.runpytest().assert_outcomes(passed=1)
        assert fake_cli.calls[2] == [*DEPLOY, "--fail-on-active-runs"]

    def test_lakeflows_with_different_deploy_flags_deploy_separately(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        pytester.makeconftest(
            "import pytest\nfrom pytest_databricks_lakeflow import Lakeflow\n\n"
            "@pytest.fixture(scope='session')\ndef plain():\n    return Lakeflow()\n\n"
            "@pytest.fixture(scope='session')\ndef same():\n    return Lakeflow()\n\n"
            "@pytest.fixture(scope='session')\ndef flagged():\n"
            "    return Lakeflow(deploy_flags=['--fail-on-active-runs'])\n"
        )
        pytester.makepyfile(
            "def test_one(plain, same, flagged):\n"
            "    for lakeflow in (plain, same, flagged):\n"
            "        assert lakeflow.template('my_job').tasks\n"
        )
        pytester.runpytest().assert_outcomes(passed=1)
        deploys = [call for call in fake_cli.calls if call[:2] == ["bundle", "deploy"]]
        assert deploys == [DEPLOY, [*DEPLOY, "--fail-on-active-runs"]]

    def test_deploy_flags_do_not_split_sessions_that_do_not_deploy(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        pytester.makeconftest(
            "import pytest\nfrom pytest_databricks_lakeflow import Lakeflow\n\n"
            "@pytest.fixture(scope='session')\ndef plain():\n    return Lakeflow(sync='sync')\n\n"
            "@pytest.fixture(scope='session')\ndef flagged():\n"
            "    return Lakeflow(sync='sync', deploy_flags=['--fail-on-active-runs'])\n"
        )
        pytester.makepyfile("def test_one(plain, flagged):\n    assert plain.templates == flagged.templates\n")
        pytester.runpytest().assert_outcomes(passed=1)
        assert [call[:2] for call in fake_cli.calls].count(["bundle", "sync"]) == 1

    def test_a_failed_setup_is_not_repeated(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        fake_cli.errors["deploy"] = LakeflowBundleError("deploy broke")
        pytester.makepyfile(
            "def test_a(lakeflow):\n    lakeflow.template('my_job')\n\ndef test_b(lakeflow):\n    lakeflow.templates\n"
        )
        result = pytester.runpytest()
        result.assert_outcomes(failed=2)
        result.stdout.fnmatch_lines(
            [
                "*LakeflowBundleError: deploy broke*",
                "*failed earlier in this session: LakeflowBundleError: deploy broke*",
            ]
        )
        assert fake_cli.calls.count(DEPLOY) == 1

    def test_lakeflow_outside_a_test_fails_clearly(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        pytester.makepyfile(
            """
            from pytest_databricks_lakeflow import Lakeflow, LakeflowError

            try:
                Lakeflow().job("my_job")
            except LakeflowError as e:
                ERROR = str(e)

            def test_error():
                assert "inside a test" in ERROR
            """
        )
        pytester.runpytest().assert_outcomes(passed=1)
        assert fake_cli.calls == []
