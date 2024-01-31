from __future__ import annotations

from typing import TYPE_CHECKING

import bz2
import os

from pathlib import Path

import numpy as np

import pytest

from xpypact import inventory

if TYPE_CHECKING:
    from xpypact.inventory import Inventory


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


@pytest.fixture(scope="session")
def inventory_without_gamma() -> inventory.Inventory:
    """Load inventory without gamma information.

    Returns:
        Inventory without gamma information.
    """
    return inventory.from_json((DATA / "Ag-1.json").read_text(encoding="utf-8"))


@pytest.fixture(scope="session")
def inventory_with_gamma() -> inventory.Inventory:
    """Load inventory with gamma information.

    Returns:
        Inventory with gamma information.
    """
    with bz2.open(DATA / "with-gamma.json.bz2") as fid:
        return inventory.from_json(fid.read().decode("utf-8"))


@pytest.fixture(scope="session")
def one_cell(data) -> Inventory:
    """Load inventory from one-cell JSON.

    Args:
        data: fixture - path to test data

    Returns:
       ... with gamma information.
    """
    return inventory.from_json((data / "inventory_1.json").read_text(encoding="utf-8"))


@pytest.fixture(scope="session")
def one_cell_time_step7_gamma() -> list[tuple[int, float]]:
    """Expected content for gamma spectrum from one-cell JSON at ts=7.

    Returns:
        Emulated gamma group distribution.
    """
    return [
        (g + 1, r)
        for g, r in enumerate(
            [
                0.38227672039721178e2,
                0.40146534717498406e-1,
                0.0e0,
                0.0e0,
                0.4372402541439171e1,
                0.28039478777768124e-4,
                0.10584970766116328e0,
                0.34337831118118902e4,
                0.34570402101038802e-1,
                0.18192170306017219e5,
                0.1316980894893718e3,
                0.13376874974122669e3,
                0.9574954161209176e-1,
                0.19472945475089941e3,
                0.14379812078405673e-2,
                0.27820386742720345e-5,
                0.0e0,
                0.0e0,
                0.0e0,
                0.0e0,
                0.0e0,
                0.0e0,
                0.0e0,
                0.0e0,
            ],
        )
    ]


@pytest.fixture(scope="session")
def one_cell_time_step7_gamma_spectrum() -> list[tuple[int, float]]:
    """Expected content for gamma spectrum from one-cell JSON at ts=7.

    Returns:
        Emulated gamma spectrum.
    """
    boundaries = np.array(
        [
            0.99999997473787511e-11,
            0.1e-1,
            0.2e-1,
            0.49999999999999996e-1,
            0.99999999999999992e-1,
            0.19999999999999998e0,
            0.29999999999999999e0,
            0.39999999999999997e0,
            0.59999999999999998e0,
            0.79999999999999993e0,
            0.1e1,
            0.122e1,
            0.14399999999999999e1,
            0.16599999999999999e1,
            0.2e1,
            0.25e1,
            0.3e1,
            0.4e1,
            0.5e1,
            0.65e1,
            0.8e1,
            0.1e2,
            0.12e2,
            0.14e2,
            0.2e2,
        ],
        dtype=float,
    )
    mids = 0.5 * (boundaries[1:] + boundaries[:-1])
    values = np.array(
        [
            0.38227672039721178e2,
            0.40146534717498406e-1,
            0.0e0,
            0.0e0,
            0.4372402541439171e1,
            0.28039478777768124e-4,
            0.10584970766116328e0,
            0.34337831118118902e4,
            0.34570402101038802e-1,
            0.18192170306017219e5,
            0.1316980894893718e3,
            0.13376874974122669e3,
            0.9574954161209176e-1,
            0.19472945475089941e3,
            0.14379812078405673e-2,
            0.27820386742720345e-5,
            0.0e0,
            0.0e0,
            0.0e0,
            0.0e0,
            0.0e0,
            0.0e0,
            0.0e0,
            0.0e0,
        ],
        dtype=float,
    )
    intensities = values / mids
    # noinspection PyTypeChecker
    return [(g + 1, r) for g, r in enumerate(intensities)]
