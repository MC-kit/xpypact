"""Code to implement DuckDB DAO."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import duckdb as db
import pandas as pd
import xarray as xr

from xpypact import get_gamma, get_nuclides, get_run_data, get_time_steps, get_timestep_nuclides
from xpypact.utils.resource import path_resolver

from ...dao import DataAccessInterface


# noinspection SqlNoDataSourceInspection
@dataclass
class DuckDBDAO(DataAccessInterface):
    """Implementation of DataAccessInterface for DuckDB."""

    path: Path | None = None
    read_only: bool = False
    config: dict | None = None
    con: db.DuckDBPyConnection = field(init=False)

    def __enter__(self):
        if self.path is not None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        database_name = str(self.path) if self.path else ":memory:"
        # duckdb.connect(... config=None) - doesn't work
        if self.config is None:
            self.con = db.connect(database_name, self.read_only)
        else:
            self.con = db.connect(database_name, self.read_only, self.config)
        if not self.read_only:
            self.create_schema()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.con.close()

    def get_tables_info(self):
        """Get information on tables in schema."""
        return self.con.execute("select * from information_schema.tables").df()

    def create_schema(self) -> None:
        """Create tables to store xpypact dataset.

        Retain existing tables.
        """
        sql_path: Path = path_resolver(__package__)("create_schema.sql")
        sql = sql_path.read_text(encoding="utf-8")
        self.con.execute(sql)

    def drop_schema(self):
        """Drop our DB objects."""
        tables = [
            "timestep_nuclide",
            "timestep_gamma",
            "timestep",
            "nuclide",
            "rundata",
        ]
        for table in tables:
            self.con.execute(f"drop table if exists {table}")

    def save(self, ds: xr.Dataset, material_id=1, case_id="") -> None:
        """Save xpypact dataset to database.

        Args:
            ds: xpypact dataset to save
            material_id: additional key to distinguish multiple FISPACT run
            case_id: second additional key
        """
        self._save_run_data(ds, material_id, case_id)
        self._save_nuclides(ds)
        self._save_time_steps(ds)
        self._save_time_step_nuclides(ds)
        self._save_gamma(ds)

    def load_rundata(self) -> pd.DataFrame:
        """Load FISPACT run data as table.

        Returns:
            FISPACT run data
        """
        return self.con.execute("select * from rundata").df()

    def load_nuclides(self) -> pd.DataFrame:
        """Load nuclide table.

        Returns:
            time nuclide
        """
        return self.con.execute("select * from nuclide").df()

    def load_time_steps(self) -> pd.DataFrame:
        """Load time step table.

        Returns:
            time step table
        """
        return self.con.execute("select * from timestep").df()

    def load_time_step_nuclides(self) -> pd.DataFrame:
        """Load time step x nuclides table.

        Returns:
            time step x nuclides table
        """
        return self.con.execute("select * from timestep_nuclide").df()

    def load_gamma(self, time_step_number: int = None) -> pd.DataFrame:
        """Load time step x gamma table.

        Args:
            time_step_number: filter for time_step_number

        Returns:
            time step x gamma table
        """
        sql = "select * from timestep_gamma"
        if time_step_number is not None:
            sql += f" where time_step_number == {time_step_number}"
        return self.con.execute(sql).df()

    def _save_run_data(self, ds: xr.Dataset, material_id=1, case_id=""):
        _table = get_run_data(ds)
        _table = _add_material_and_case_columns(_table, material_id, case_id)
        self.con.execute("insert into rundata from _table")
        self.con.commit()

    def _save_nuclides(self, ds: xr.Dataset):
        _table = get_nuclides(ds)  # noqa: F841 - used below
        sql = "insert or ignore into nuclide select * from _table"
        self.con.execute(sql)
        self.con.commit()

    def _save_time_steps(self, ds: xr.Dataset, material_id=1, case_id=""):
        _table = get_time_steps(ds)
        _table = _add_material_and_case_columns(_table, material_id, case_id)
        self.con.execute("insert into timestep from _table")
        self.con.commit()

    def _save_time_step_nuclides(self, ds: xr.Dataset, material_id=1, case_id=""):
        _table = get_timestep_nuclides(ds)
        _table = _add_material_and_case_columns(_table, material_id, case_id)
        self.con.execute("insert into timestep_nuclide from _table")
        self.con.commit()

    def _save_gamma(self, ds: xr.Dataset, material_id=1, case_id=""):
        _table = get_gamma(ds)
        _table = _add_material_and_case_columns(_table, material_id, case_id)
        self.con.execute("insert into timestep_gamma from _table")
        self.con.commit()


def _add_material_and_case_columns(
    table: pd.DataFrame, material_id: int, case_id: str
) -> pd.DataFrame:
    columns = table.columns.values
    table["material_id"] = material_id
    table["case_id"] = case_id
    new_columns = ["material_id", "case_id"]
    new_columns.extend(columns)
    return table[new_columns]
