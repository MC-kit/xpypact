from __future__ import annotations

import polars as pl
import pytest

from xpypact.collector import FullDataCollector


def test_collector(inventory_with_gamma) -> None:
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
    assert timesteps.select("activity").item() == 61794984.60241412

    timestep_nuclides = collector.timestep_nuclides.filter(filt_112, zai=832100)
    assert timestep_nuclides.select("dose").item() == pytest.approx(2.4815e-20, rel=1e-4)


if __name__ == "__main__":
    pytest.main()
