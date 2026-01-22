from __future__ import annotations

from typing import TYPE_CHECKING

import duckdb as db
import polars as pl
import pytest

from numpy.testing import assert_allclose
from polars.testing import assert_frame_equal

from xpypact.collector import FullDataCollector
from xpypact.dao.duckdb.implementation import save

if TYPE_CHECKING:
    from pathlib import Path

    from xpypact.inventory import Inventory


def test_collector(inventory_with_gamma: Inventory) -> None:
    collector = FullDataCollector()
    assert collector.rundata.height == 0
    collector.append(inventory_with_gamma, 1, 1)
    assert collector.rundata.height == 1
    assert collector.timesteps.height == 2
    collector.append(inventory_with_gamma, 1, 2)
    assert collector.timesteps.height == 4

    filt_11 = pl.col("material_id").eq(1) & pl.col("case_id").eq(1)
    rundata = collector.rundata.filter(filt_11)
    assert rundata.height == 1

    assert rundata.select("timestamp").item().year == 2022

    filt_112 = filt_11 & pl.col("time_step_number").eq(2)
    timesteps = collector.timesteps.filter(filt_112)
    assert timesteps.height == 1
    assert timesteps.select("activity").item() == pytest.approx(61794984.60241412)

    timestep_nuclides = collector.timestep_nuclides.filter(filt_112, zai=832100)
    assert timestep_nuclides.select("dose").item() == pytest.approx(2.4815e-20, rel=1e-4)

    gbins = collector.get_gbins()
    assert gbins is not None
    assert gbins.height == 25


def test_collector_without_gamma(inventory_without_gamma: Inventory) -> None:
    collector = FullDataCollector()
    assert collector.rundata.height == 0
    collector.append(inventory_without_gamma, 1, 1)
    assert collector.rundata.height == 1
    assert collector.timesteps.height == 2

    filt_11 = pl.col("material_id").eq(1) & pl.col("case_id").eq(1)
    rundata = collector.rundata.filter(filt_11)
    assert rundata.height == 1

    assert rundata.select("timestamp").item().year == 2020

    filt_112 = filt_11 & pl.col("time_step_number").eq(2)
    timesteps = collector.timesteps.filter(filt_112)
    assert timesteps.height == 1
    assert timesteps.select("activity").item() == pytest.approx(6123275960.275331)

    gbins = collector.get_gbins()
    assert gbins is None


def test_one_cell_json(
    one_cell: Inventory, one_cell_time_step7_gamma_spectrum: list[tuple[int, float]], tmp_path: Path
) -> None:
    """Check gamma spectrum from the last time step in the one-cell JSON."""
    collector = FullDataCollector()
    collector.append(one_cell, material_id=1, case_id=54)
    collector.append(one_cell, material_id=2, case_id=54)

    spectra = collector.get_timestep_gamma_as_spectrum()
    assert spectra is not None
    for material_id in (1, 2):
        actual = (
            spectra.filter(
                material_id=material_id,
                time_step_number=7,
            )
            .select("g", "rate")
            .rows()
        )
        # noinspection PyTypeChecker
        assert_allclose(
            actual,
            one_cell_time_step7_gamma_spectrum,
            rtol=1e-7,
            verbose=True,
            err_msg="Fails on gamma spectrum comparison",
        )

    con = db.connect()
    save(con, collector.get_result())
    gamma_from_db = con.sql(
        """
        select
        g, rate
        from timestep_gamma
        where material_id = 1 and case_id = 54 and time_step_number = 7
        order by g
        """,
    ).fetchall()

    # noinspection PyTypeChecker
    assert_allclose(
        gamma_from_db,
        one_cell_time_step7_gamma_spectrum,
        rtol=1e-7,
        err_msg="Fails on gamma spectrum comparison",
    )
    collected = collector.get_result()
    assert collected.time_step_times.height == 7
    collected.save_to_parquets(tmp_path)
    collected.save_to_parquets(tmp_path, override=True)
    with pytest.raises(FileExistsError):
        collected.save_to_parquets(tmp_path, override=False)


def test_polars_filter() -> None:
    """Trying to reproduce the unexpected Polars behavior in the above test."""
    initial = pl.DataFrame(
        {
            "a": [1, 1, 1, 1, 2, 2, 2, 2],
            "b": [1, 1, 2, 2, 1, 1, 2, 2],
            "c": list("abcdefgh"),
        },
        schema={
            "a": pl.UInt32,
            "b": pl.UInt32,
            "c": pl.String,
        },
    ).sort("b")
    actual = initial.filter(a=1, b=2)
    expected = pl.DataFrame(
        {
            "a": [1, 1],
            "b": [2, 2],
            "c": ["c", "d"],
        },
        schema={
            "a": pl.UInt32,
            "b": pl.UInt32,
            "c": pl.String,
        },
    )
    assert_frame_equal(actual, expected)


if __name__ == "__main__":
    pytest.main()
