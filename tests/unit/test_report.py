from __future__ import annotations

import re

from pytest_databricks_lakeflow.report import directory_name


class TestDirectoryName:
    """Each test's report directory has a name that is safe on any file system and that no other test shares."""

    def test_directory_name_is_safe_and_unique(self) -> None:
        name = directory_name("tests/test_jobs.py::test_job[a/b]")
        assert re.fullmatch(r"tests_test_jobs\.py__test_job_a_b_-[0-9a-f]{8}", name)
        assert directory_name("tests/test_jobs.py::test_job[a/b]") == name
        assert directory_name("tests/test_jobs.py::test_job[a_b]") != name
        long_name = directory_name("test_jobs.py::test_job[" + "x" * 200 + "]")
        assert re.fullmatch(r"test_jobs\.py__test_job_x{57}-[0-9a-f]{8}", long_name)
