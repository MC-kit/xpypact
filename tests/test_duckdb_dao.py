from __future__ import annotations

from contextlib import closing
from pathlib import Path

import pandas as pd
import pytest

from duckdb import InvalidInputException, connect
from xpypact.dao.duckdb import DuckDBDAO as DataAccessObject
from xpypact.dao.duckdb import write_parquet


def test_ddl(tmp_path):
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


def test_save(dataset_with_gamma):
    with closing(connect(":memory:")) as con:
        dao = DataAccessObject(con)
        dao.create_schema()
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
    assert Path(tmp_path / "time_steps/time_step_number=1/material_id=1").exists()
    assert Path(tmp_path / "time_steps/time_step_number=1/material_id=1/case_id=1").exists()
    write_parquet(tmp_path, dataset_with_gamma, 1, 2)
    assert Path(tmp_path / "time_steps/time_step_number=1/material_id=1/case_id=2").exists()
    con = connect(":memory:")
    path = tmp_path / "nuclides/*/*/*.parquet"
    sql = f"select * from read_parquet('{path}', hive_partitioning=true)"  # noqa: S608
    nuclides = con.execute(sql).df()
    assert not nuclides.loc[2].empty
    path = tmp_path / "time_steps/*/*/*/*.parquet"
    sql = f"select * from read_parquet('{path}', hive_partitioning=true)"  # noqa: S608
    time_steps = con.execute(sql).df()
    assert not time_steps.loc[2].empty
