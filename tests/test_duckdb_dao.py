"""Tests for DuckDB DAO."""

from __future__ import annotations

import datetime as dt

from contextlib import closing

import polars as pl
import pytest

from duckdb import InvalidInputException, connect
from xpypact.collector import UTC, FullDataCollector
from xpypact.dao.duckdb import DuckDBDAO as DataAccessObject
from xpypact.dao.duckdb import create_indices
from xpypact.dao.duckdb.implementation import save


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
        dc = FullDataCollector()
        dc.append(inventory_with_gamma, material_id=1, case_id=1)
        save(con, dc.get_result())
        run_data = dao.load_rundata().pl()
        assert run_data.select("timestamp").item() == dt.datetime(
            2022,
            2,
            21,
            1,
            52,
            45,
            tzinfo=UTC,
        )
        assert run_data["run_name"].item() == "* Material Cu, fluxes 104_2_1_1"
        nuclides = dao.load_nuclides().pl()
        assert not nuclides.filter(pl.col("element").eq("Cu")).is_empty()
        timestep_times = dao.load_timestep_times().pl()
        assert timestep_times.height == 2
        time_steps = dao.load_time_steps().pl()
        assert not time_steps.is_empty()
        assert not time_steps.filter(time_step_number=2).is_empty()
        time_step_nuclides = dao.load_time_step_nuclides().filter("material_id=1").pl()
        assert not time_step_nuclides.is_empty()
        assert not time_step_nuclides.filter(time_step_number=2, zai=290630).is_empty()
        _check_gbins(dao)
        create_indices(con)  # check integrity


def _check_gbins(dao: DataAccessObject) -> None:
    gbins = dao.load_gbins().pl()
    assert gbins.filter(g=0).select("boundary").item() == pytest.approx(1e-11)
    gamma = dao.load_gamma().filter("material_id=1").pl()
    assert not gamma.is_empty()
    assert not gamma.filter(time_step_number=2, g=1).is_empty()
    gamma2 = dao.load_gamma(2).pl()
    assert not gamma2.is_empty()
