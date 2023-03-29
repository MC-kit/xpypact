from __future__ import annotations

import os

from pathlib import Path

import pytest

from xpypact.utils.resource import path_resolver


@pytest.fixture(scope="session")
def data() -> Path:
    """Get Path to tests/data directory.

    Returns:
        Path to tests/data directory
    """
    return path_resolver("tests")("data")


@pytest.fixture()
def cd_tmpdir(tmpdir):  # noqa: PT004
    """Temporarily switch to temp directory.

    Args:
        tmpdir: pytest fixture for temp directory

    Yields:
        None
    """
    old_dir = tmpdir.chdir()
    try:
        yield
    finally:
        os.chdir(old_dir)
