"""Tests for DuckDB DAO."""
from __future__ import annotations

from typing import TYPE_CHECKING

from contextlib import closing

import pandas as pd
import pytest

from duckdb import InvalidInputException, connect
from xpypact.dao.duckdb import CommonDataCollector
from xpypact.dao.duckdb import DuckDBDAO as DataAccessObject
from xpypact.dao.duckdb import create_indices, load_parquets, write_parquets

if TYPE_CHECKING:
    from pathlib import Path

    from xpypact.inventory import Inventory


def test_ddl(tmp_path):
    """Test schema creation."""
    duckdb_path = tmp_path / "test-ddl.duckdb"
    with closing(connect(str(duckdb_path))) as con:
        dao = DataAccessObject(con)
        assert not dao.has_schema()
        dao.create_schema()
        assert dao.has_schema()
        dao.drop_schema()
        assert not dao.has_schema()
        dao.create_schema()
        assert dao.has_schema()

    with closing(connect(str(duckdb_path), read_only=True)) as con:
        dao = DataAccessObject(con)
        assert dao.has_schema()
        with pytest.raises(InvalidInputException, match="Cannot execute statement"):
            dao.drop_schema()


def test_save(inventory_with_gamma) -> None:
    """Test saving of dataset to a database.

    Args:
        inventory_with_gamma: inventory to save (fixture)
    """
    with closing(connect()) as con:
        dao = DataAccessObject(con)
        dao.create_schema()
        cdc = CommonDataCollector()
        dao.save(inventory_with_gamma, material_id=1, case_id=1, cdc=cdc)
        dao.save(inventory_with_gamma, material_id=2, case_id=1, cdc=cdc)
        cdc.save(con)
        run_data = dao.load_rundata().df().loc[0]
        assert run_data["timestamp"] == pd.Timestamp("2022-02-21 01:52:45")
        assert run_data["run_name"] == "* Material Cu, fluxes 104_2_1_1"
        nuclides = dao.load_nuclides().df()
        nuclides = nuclides.set_index(["element", "mass_number", "state"])
        assert not nuclides.loc["Cu"].empty
        time_steps = dao.load_time_steps().df()
        assert not time_steps.empty
        time_steps = time_steps.set_index("time_step_number")
        assert not time_steps.loc[2].empty
        time_step_nuclides = dao.load_time_step_nuclides().filter("material_id=1").df()
        assert not time_step_nuclides.empty
        time_step_nuclides = time_step_nuclides.set_index(
            [
                "time_step_number",
                "zai",
            ],
        )
        assert not time_step_nuclides.loc[2, 290630].empty
        _check_gbins(dao)


def _check_gbins(dao):
    gbins = dao.load_gbins().df().set_index("g")
    assert gbins.loc[0].boundary == pytest.approx(1e-11)
    gamma = dao.load_gamma().filter("material_id=1").df()
    assert not gamma.empty
    gamma = gamma.set_index(["time_step_number", "g"])
    assert not gamma.loc[2, 1].empty
    gamma2 = dao.load_gamma(2).df()
    assert not gamma2.empty


def test_write_parquets(tmp_path: Path, inventory_with_gamma: Inventory) -> None:
    cdc = CommonDataCollector()
    write_parquets(tmp_path, inventory_with_gamma, material_id=1, case_id=1, cdc=cdc)
    write_parquets(tmp_path, inventory_with_gamma, material_id=2, case_id=1, cdc=cdc)
    assert (tmp_path / "material_id=1/case_id=1").is_dir(), "Should create case directory"
    assert (tmp_path / "material_id=2/case_id=1").is_dir(), "Should create case directory"
    con = connect()
    load_parquets(con, tmp_path)
    cdc.save(con)
    create_indices(con)
    dao = DataAccessObject(con)
    _check_gbins(dao)
    timestep_nuclide_df = (
        con.table("timestep_nuclide")
        .filter("material_id=1")
        .df()
        .set_index(["material_id", "case_id", "time_step_number", "zai"])
    )
    assert not timestep_nuclide_df.loc[(1, 1, 1, 80160)].empty
