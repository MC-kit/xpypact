"""Tests for DuckDB DAO."""
from __future__ import annotations

from contextlib import closing

import pandas as pd
import pytest

from duckdb import InvalidInputException, connect
from xpypact.dao.duckdb import DuckDBDAO as DataAccessObject


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
        dataset_with_gamma: dataset to save (fixture)
    """
    with closing(connect()) as con:
        dao = DataAccessObject(con)
        dao.create_schema()
        dao.save(inventory_with_gamma)
        run_data = dao.load_rundata().df()
        assert run_data["timestamp"].item() == pd.Timestamp("2022-02-21 01:52:45")
        assert run_data["run_name"].item() == "* Material Cu, fluxes 104_2_1_1"
        nuclides = dao.load_nuclides().df()
        nuclides = nuclides.set_index(["element", "mass_number", "state"])
        assert not nuclides.loc["Cu"].empty
        time_steps = dao.load_time_steps().df()
        assert not time_steps.empty
        time_steps = time_steps.set_index("time_step_number")
        assert not time_steps.loc[2].empty
        time_step_nuclides = dao.load_time_step_nuclides().df()
        assert not time_step_nuclides.empty
        time_step_nuclides = time_step_nuclides.set_index(
            [
                "time_step_number",
                "element",
                "mass_number",
                "state",
            ],
        )
        assert not time_step_nuclides.loc[2, "Cu"].empty
        gamma = dao.load_gamma().df()
        assert not gamma.empty
        gamma = gamma.set_index(["time_step_number", "boundary"])
        assert not gamma.loc[2, 1.0].empty
        gamma2 = dao.load_gamma(2).df()
        assert not gamma2.empty
