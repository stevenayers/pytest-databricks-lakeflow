from __future__ import annotations

from pathlib import Path

import pytest
from databricks.sdk.service import jobs

from .conftest import LAKEFLOW_CONFTEST, FakeCli, configure_lakeflow
from .fakes import FakeWorkspaceClient, example_settings

UNDECLARED = "Task 'transform': {{job.parameters.nope}} is not declared. Declared: ['param1', 'schema']"


def assert_nothing_written(ws: FakeWorkspaceClient) -> None:
    assert ws.jobs.created == []
    assert ws.jobs.run_now_calls == []
    assert ws.deletions == []


def miswired_settings() -> jobs.JobSettings:
    settings = example_settings()
    assert settings.tasks is not None
    transform = settings.tasks[1]
    assert transform.notebook_task is not None
    transform.notebook_task.base_parameters = {"x": "{{job.parameters.nope}}"}
    return settings


BUNDLE = "bundle:\n  name: example\n"

DERIVED_CONFTEST = """
import dataclasses

import pytest


@pytest.fixture(scope="session")
def lakeflow(lakeflow):
    return dataclasses.replace(lakeflow, catalog="team_b")
"""


def write(pytester: pytest.Pytester, name: str, content: str) -> None:
    path = pytester.path / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


class TestCheckItems:
    """`--lakeflow-check` collects one item per job template that lints it and writes nothing to the workspace."""

    def test_passes_a_clean_template(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        result = pytester.runpytest("--lakeflow-check", "-v")
        result.stdout.fnmatch_lines(["*databricks.yml::my_job PASSED*"])
        result.assert_outcomes(passed=1)
        assert_nothing_written(fake_ws)

    def test_fails_a_template_with_wiring_mistakes(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        fake_cli.deployed["miswired"] = fake_ws.jobs.add(miswired_settings())
        result = pytester.runpytest("--lakeflow-check", "-v")
        result.stdout.fnmatch_lines(
            ["*databricks.yml::miswired FAILED*", "*databricks.yml::my_job PASSED*", "*_ miswired _*"]
        )
        assert UNDECLARED in result.stdout.lines
        result.assert_outcomes(passed=1, failed=1)
        assert_nothing_written(fake_ws)

    def test_fails_a_job_that_cannot_be_a_template(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        # Job 1 is in the bundle summary but not in the workspace, as after a deploy to another workspace.
        fake_cli.deployed["broken"] = 1
        result = pytester.runpytest("--lakeflow-check", "-v")
        result.stdout.fnmatch_lines(
            ["*databricks.yml::broken FAILED*", "LakeflowBundleError: Job 'broken' (id 1)*stale*"]
        )
        result.assert_outcomes(passed=1, failed=1)

    def test_without_the_flag_collection_is_unchanged(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        pytester.makepyfile("def test_nothing():\n    pass\n")
        result = pytester.runpytest("--collect-only", "-q")
        assert "databricks.yml" not in result.stdout.str()
        result.stdout.fnmatch_lines(["*test_nothing*"])
        assert fake_cli.calls == []
        assert fake_ws.profiles == []

    def test_items_carry_the_lakeflow_marker(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        pytester.makepyfile("def test_nothing():\n    pass\n")
        result = pytester.runpytest("--lakeflow-check", "-m", "not lakeflow")
        result.assert_outcomes(passed=1, deselected=1)


class TestConfiguredLakeflow:
    """Check items use the `lakeflow` fixture that the bundle file sees, pointed at that bundle."""

    @pytest.mark.parametrize("give", ["return", "yield"])
    def test_the_fixture_target_reaches_the_cli(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path, give: str
    ) -> None:
        configure_lakeflow(pytester, 'target="staging"')
        conftest = pytester.path / "conftest.py"
        conftest.write_text(conftest.read_text().replace("return Lakeflow(", f"{give} Lakeflow("))
        pytester.runpytest("--lakeflow-check").assert_outcomes(passed=1)
        bundle_calls = [args for args, _, _ in fake_cli.invocations if args[0] == "bundle"]
        assert bundle_calls
        assert all(args[2:4] == ["-t", "staging"] for args in bundle_calls)

    def test_a_yield_fixture_is_torn_down_after_collection(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        configure_lakeflow(pytester, "")
        conftest = pytester.path / "conftest.py"
        teardown = 'yield Lakeflow()\n    with open("teardowns.txt", "a") as f:\n        f.write("x")'
        conftest.write_text(conftest.read_text().replace("return Lakeflow()", teardown))
        pytester.runpytest("--lakeflow-check").assert_outcomes(passed=1)
        # Once for the call made while collecting, once for the session's fixture.
        assert (pytester.path / "teardowns.txt").read_text() == "xx"

    @pytest.mark.parametrize(
        ("conftest", "bundle", "arguments"),
        [
            ("tests/conftest.py", "databricks.yml", 'target="staging"'),
            ("tests/conftest.py", "bundle/databricks.yml", 'bundle_root="bundle", target="staging"'),
            ("bundles/a/tests/conftest.py", "bundles/a/databricks.yml", 'target="staging"'),
        ],
    )
    def test_an_override_for_the_bundle_it_cannot_see_is_refused(
        self,
        pytester: pytest.Pytester,
        fake_ws: FakeWorkspaceClient,
        fake_cli: FakeCli,
        conftest: str,
        bundle: str,
        arguments: str,
    ) -> None:
        write(pytester, bundle, "bundle:\n  name: a\n")
        write(pytester, conftest, LAKEFLOW_CONFTEST.format(arguments=arguments))
        result = pytester.runpytest(str(Path(conftest).parent), bundle, "--lakeflow-check")
        result.stdout.fnmatch_lines(
            [
                f"*--lakeflow-check cannot see the lakeflow fixture that {conftest} overrides, so it would "
                "check this bundle with the default Lakeflow(): put the override in a conftest.py in the bundle's "
                "directory or above it*"
            ]
        )
        result.assert_outcomes(errors=1)
        assert fake_cli.calls == []

    @pytest.mark.parametrize(
        ("files", "args", "passed"),
        [
            pytest.param(
                {"databricks.yml": BUNDLE, "tests/team_b/conftest.py": DERIVED_CONFTEST},
                ["tests", "databricks.yml"],
                1,
                id="derived-from-the-default",
            ),
            pytest.param(
                {
                    "databricks.yml": BUNDLE,
                    "a/databricks.yml": "bundle:\n  name: a\n",
                    "a/conftest.py": LAKEFLOW_CONFTEST.format(arguments='target="staging"'),
                    "b/databricks.yml": "bundle:\n  name: b\n",
                },
                [],
                3,
                id="in-a-sibling-bundle",
            ),
            pytest.param(
                {
                    "databricks.yml": BUNDLE,
                    "tests/test_jobs.py": LAKEFLOW_CONFTEST.format(arguments="")
                    + "\n\ndef test_x(lakeflow):\n    pass\n",
                },
                ["tests", "databricks.yml"],
                2,
                id="in-a-test-module",
            ),
            *(
                pytest.param(
                    {
                        f"services/{name}/conftest.py": LAKEFLOW_CONFTEST.format(
                            arguments=f'bundle_root="services/{name}/bundle", target="staging"'
                        ),
                        f"services/{name}/bundle/databricks.yml": "bundle:\n  name: a\n",
                        "services/bar/databricks.yml": "bundle:\n  name: b\n",
                    },
                    [],
                    2,
                    id=f"above-its-own-bundle-{name}",
                )
                # alpha's conftest is loaded before bar is collected, zulu's after.
                for name in ("alpha", "zulu")
            ),
        ],
    )
    def test_an_override_meant_for_other_tests_is_ignored(
        self,
        pytester: pytest.Pytester,
        fake_ws: FakeWorkspaceClient,
        fake_cli: FakeCli,
        files: dict[str, str],
        args: list[str],
        passed: int,
    ) -> None:
        for name, content in files.items():
            write(pytester, name, content)
        pytester.runpytest(*args, "--lakeflow-check").assert_outcomes(passed=passed)

    def test_an_override_outside_every_bundle_that_requests_fixtures_warns(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli
    ) -> None:
        write(pytester, "bundle/databricks.yml", BUNDLE)
        conftest = LAKEFLOW_CONFTEST.format(arguments='bundle_root="bundle"')
        write(pytester, "tests/conftest.py", conftest.replace("def lakeflow():", "def lakeflow(tmp_path_factory):"))
        result = pytester.runpytest("tests", "bundle/databricks.yml", "--lakeflow-check")
        result.stdout.fnmatch_lines(
            [
                "*UserWarning: --lakeflow-check cannot call the lakeflow fixture that tests/conftest.py overrides "
                "while collecting, because it requests tmp_path_factory, so it checks this bundle with the default "
                "Lakeflow()*"
            ]
        )
        result.assert_outcomes(passed=1, warnings=1)

    def test_sync_none_fails_a_job_validate_cannot_resolve(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        job = {"name": "j", "tasks": [{"task_key": "t", "run_job_task": {"job_id": "${resources.jobs.x.id}"}}]}
        fake_cli.validate = {"resources": {"jobs": {"my_job": job}}}
        configure_lakeflow(pytester, 'sync="none"')
        result = pytester.runpytest("--lakeflow-check", "-v")
        result.stdout.fnmatch_lines(["*databricks.yml::my_job FAILED*", "*does not resolve*"])
        result.assert_outcomes(failed=1)

    def test_a_nested_bundle_is_checked_at_its_own_root(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        nested = pytester.mkdir("other")
        (nested / "databricks.yml").write_text("bundle:\n  name: other\n")
        result = pytester.runpytest("--lakeflow-check", "-v")
        result.stdout.fnmatch_lines(["*databricks.yml::my_job PASSED*", "*other/databricks.yml::my_job PASSED*"])
        result.assert_outcomes(passed=2)
        roots = {root for args, root, _ in fake_cli.invocations if args[:2] == ["bundle", "deploy"]}
        assert roots == {bundle_dir, nested}

    def test_the_jobs_of_a_nested_bundle_share_one_lakeflow(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        fake_cli.deployed["second"] = fake_ws.jobs.add(example_settings())
        fake_cli.deployed["third"] = fake_ws.jobs.add(example_settings())
        nested = pytester.mkdir("other")
        (nested / "databricks.yml").write_text("bundle:\n  name: other\n")
        pytester.runpytest("--lakeflow-check").assert_outcomes(passed=6)
        # One client per Lakeflow: the fixture's and the nested bundle's, each made once while collecting and
        # once at run time, however many jobs the bundles have.
        assert len(fake_ws.profiles) == 4

    def test_a_fixture_that_requests_other_fixtures_is_refused(
        self, pytester: pytest.Pytester, fake_ws: FakeWorkspaceClient, fake_cli: FakeCli, bundle_dir: Path
    ) -> None:
        pytester.makeconftest(
            """
            import pytest

            from pytest_databricks_lakeflow import Lakeflow

            @pytest.fixture(scope="session")
            def lakeflow(tmp_path_factory):
                return Lakeflow()
            """
        )
        result = pytester.runpytest("--lakeflow-check")
        result.stdout.fnmatch_lines(
            [
                "*--lakeflow-check calls the lakeflow fixture while collecting, so it cannot request other fixtures, "
                "and this one requests tmp_path_factory*"
            ]
        )
        result.assert_outcomes(errors=1)
        assert fake_cli.calls == []
