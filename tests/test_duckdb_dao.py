from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from duckdb import InvalidInputException, connect
from xpypact.dao.duckdb import DuckDBDAO as DataAccessObject
from xpypact.dao.duckdb import write_parquet


def test_ddl(tmp_path):
    with DataAccessObject(tmp_path / "test-ddl.duckdb") as dao:
        tables = dao.get_tables_info()
        assert len(tables) == 5
        table_names = tables["table_name"]
        for name in ["rundata", "timestep", "nuclide", "timestep_nuclide", "timestep_gamma"]:
            assert name in table_names.to_numpy()
        dao.drop_schema()
        tables = dao.get_tables_info()
        assert len(tables) == 0
        dao.create_schema()
        tables = dao.get_tables_info()
        assert len(tables) == 5
    with DataAccessObject(tmp_path / "test-ddl.duckdb", read_only=True) as dao:
        tables = dao.get_tables_info()
        assert len(tables) == 5
        with pytest.raises(InvalidInputException, match="Cannot execute statement"):
            dao.drop_schema()


def test_save(dataset_with_gamma):
    with DataAccessObject(config={"default_null_order": "nulls_last"}) as dao:
        dao.save(dataset_with_gamma)
        run_data = dao.load_rundata()
        assert run_data["timestamp"].item() == pd.Timestamp("2022-02-21 01:52:45")
        assert run_data["run_name"].item() == "* Material Cu, fluxes 104_2_1_1"
        nuclides = dao.load_nuclides()
        nuclides = nuclides.set_index(["element", "mass_number", "state"])
        assert not nuclides.loc["Cu"].empty
        time_steps = dao.load_time_steps()
        assert not time_steps.empty
        time_steps = time_steps.set_index("time_step_number")
        assert not time_steps.loc[2].empty
        time_step_nuclides = dao.load_time_step_nuclides()
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
        gamma = dao.load_gamma()
        assert not gamma.empty
        gamma = gamma.set_index(["time_step_number", "boundary"])
        assert not gamma.loc[2, 1.0].empty
        gamma2 = dao.load_gamma(2)
        assert not gamma2.empty


# noinspection SqlNoDataSourceInspection
def test_write_parquet(tmp_path, dataset_with_gamma):
    write_parquet(tmp_path, dataset_with_gamma, 1, 1)
    assert Path(tmp_path / "time_steps/material_id=1").exists()
    assert Path(tmp_path / "time_steps/material_id=1/case_id=1").exists()
    write_parquet(tmp_path, dataset_with_gamma, 1, 2)
    assert Path(tmp_path / "time_steps/material_id=1/case_id=2").exists()
    con = connect(":memory:")
    path = tmp_path / "time_steps/*/*/*.parquet"
    sql = f"select * from read_parquet('{path}', hive_partitioning=true)"  # noqa: S608
    time_steps = con.execute(sql).df()
    assert not time_steps.loc[2].empty
