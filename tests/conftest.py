from __future__ import annotations

import bz2
import os

from pathlib import Path

import pytest
import xpypact.data_arrays as da

from xpypact import inventory

HERE = Path(__file__).parent
DATA = HERE / "data"


@pytest.fixture(scope="session")
def data() -> Path:
    """Get Path to tests/data directory.

    Returns:
        Path to tests/data directory
    """
    return DATA


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


@pytest.fixture()
def inventory_with_gamma() -> inventory.Inventory:
    """Load inventory with gamma information.

    Returns:
        Inventory with gamma information.
    """
    with bz2.open(DATA / "with-gamma.json.bz2") as fid:
        return inventory.from_json(fid.read().decode("utf-8"))


@pytest.fixture()
def dataset_with_gamma(data):
    """Load dataset with gamma information.

    Args:
        data: fixture - path to test data

    Returns:
        Dataset with gamma information.
    """
    with bz2.open(data / "with-gamma.json.bz2") as fid:
        return da.from_json(fid.read().decode("utf-8"))
