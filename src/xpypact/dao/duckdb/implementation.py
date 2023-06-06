"""Code to implement DuckDB DAO."""
from __future__ import annotations

from typing import TYPE_CHECKING, cast

from dataclasses import dataclass
from multiprocessing import cpu_count
from pathlib import Path

import duckdb as db

from xpypact import get_gamma, get_nuclides, get_run_data, get_time_steps, get_timestep_nuclides
from xpypact.dao import DataAccessInterface
from xpypact.utils.resource import path_resolver

if TYPE_CHECKING:
    import pandas as pd
    import xarray as xr


# On using DuckDB with multiple threads, see
# https://duckdb.org/docs/guides/python/multiple_threads.html


# noinspection SqlNoDataSourceInspection
@dataclass
class DuckDBDAO(DataAccessInterface):
    """Implementation of DataAccessInterface for DuckDB."""

    con: db.DuckDBPyConnection

    def get_tables_info(self) -> pd.DataFrame:
        """Get information on tables in schema."""
        return self.con.execute("select * from information_schema.tables").df()

    @property
    def tables(self) -> tuple[str, str, str, str, str]:
        """List tables being used by xpypact dao.

        Returns:
            Tuple with table names.
        """
        return "rundata", "timestep", "nuclide", "timestep_nuclide", "timestep_gamma"

    def has_schema(self) -> bool:
        """Check if the schema is available in a database."""
        db_tables = self.get_tables_info()

        if len(db_tables) < len(self.tables):
            return False

        table_names = db_tables["table_name"].to_numpy()

        return all(name in table_names for name in self.tables)

    def create_schema(self) -> None:
        """Create tables to store xpypact dataset.

        Retain existing tables.
        """
        sql_path: Path = cast(Path, path_resolver(__package__)("create_schema.sql"))
        sql = sql_path.read_text(encoding="utf-8")
        self.con.execute(sql)

    def drop_schema(self) -> None:
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

    def save(self, ds: xr.Dataset, material_id=1, case_id=1) -> None:
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

    def load_rundata(self) -> db.DuckDBPyRelation:
        """Load FISPACT run data as table.

        Returns:
            FISPACT run data ad RelObj
        """
        return self.con.table("rundata")

    def load_nuclides(self) -> db.DuckDBPyRelation:
        """Load nuclide table.

        Returns:
            time nuclide
        """
        return self.con.table("nuclide")

    def load_time_steps(self) -> db.DuckDBPyRelation:
        """Load time step table.

        Returns:
            time step table
        """
        return self.con.table("timestep")

    def load_time_step_nuclides(self) -> db.DuckDBPyRelation:
        """Load time step x nuclides table.

        Returns:
            time step x nuclides table
        """
        return self.con.table("timestep_nuclide")

    def load_gamma(self, time_step_number: int | None = None) -> db.DuckDBPyRelation:
        """Load time step x gamma table.

        Args:
            time_step_number: filter for time_step_number

        Returns:
            time step x gamma table
        """
        sql = "select * from timestep_gamma"
        if time_step_number is not None:
            sql += f" where time_step_number == {time_step_number}"
        return self.con.sql(sql)

    def _save_run_data(self, ds: xr.Dataset, material_id=1, case_id=1) -> None:
        _table = get_run_data(ds)
        _table = _add_material_and_case_columns(_table, material_id, case_id)
        sql = """
            begin transaction;
            insert into rundata from _table;
            commit;
            """
        self.con.sql(sql)

    def _save_nuclides(self, ds: xr.Dataset):
        _table = get_nuclides(ds)  # noqa: F841 - used below
        sql = """
            begin transaction;
            insert or ignore into nuclide select * from _table;
            commit;
        """
        self.con.sql(sql)

    def _save_time_steps(self, ds: xr.Dataset, material_id=1, case_id=1):
        _table = get_time_steps(ds)
        _table = _add_material_and_case_columns(_table, material_id, case_id)
        sql = """
            begin transaction;
            insert into timestep from _table;
            commit;
            """
        self.con.execute(sql)
        self.con.commit()

    def _save_time_step_nuclides(self, ds: xr.Dataset, material_id=1, case_id=1):
        _table = get_timestep_nuclides(ds)
        _table = _add_material_and_case_columns(_table, material_id, case_id)
        sql = """
            begin transaction;
            insert into timestep_nuclide from _table;
            commit;
            """
        self.con.sql(sql)

    def _save_gamma(self, ds: xr.Dataset, material_id=1, case_id=1):
        _table = get_gamma(ds)
        _table = _add_material_and_case_columns(_table, material_id, case_id)
        self.con.sql(
            """
            begin transaction;
            insert into timestep_gamma from _table;
            commit;
            """,
        )


def compute_optimal_row_group_size(frame_size: int, _cpu_count: int = 0) -> int:
    """Compute DuckDB row group size for writing to parquet files.

    This should optimize speed of reading from parquet files.

    Args:
        frame_size: length of rows to be writed
        _cpu_count: number to split the rows

    Returns:
        the row group size
    """
    if not _cpu_count:
        _cpu_count = max(4, cpu_count())
    size = frame_size // _cpu_count
    return min(max(size, 2048), 1_000_000)


def write_parquet(target_dir: Path, ds: xr.Dataset, material_id: int, case_id: int) -> None:
    """Store xpypact dataset to parquet directories.

    Create in 4 subdirectories in `target_dir` for data subjects run_data, time_steps,
    time_step_nuclides, and gamma dataframes.
    Save the dataframes as parquet files.
    Hive partiotioning is not used in this version, because resulting partions are too small.
    The data for  can be  saved in the same `target_dir` as long as the material_id and case_id are
    unique for an data subject.

    This structure can be easily and efficiently accessed from DuckDB as external data.
    For instance to collect all the inventories:
    ```
        select * from read_parquet('<target_dir>/inventory/*/*/*.parquet', hive_partitioning=true)
    ```

    We use in memory DuckDB instance to transfer data to parquet to ensure compatibility
    for later reading back to DuckDB.

    See about row_group_size: https://duckdb.org/docs/data/parquet/tips

    Args:
        target_dir: root directory to store a dataset in subdirectories
        ds: dataset to store
        material_id: the level 1 key to distinguish with other datasets in the folder,
                     may correspond to material in R2S
        case_id: the level 2 key, may correspond neutron group or case in R2S of fispact run.
                 The case may be registered in the database to provide additional information.
    """
    to_proces: dict[str, pd.DataFrame] = {
        "run_data": get_run_data(ds),
        "nuclides": get_nuclides(ds),
        "time_steps": get_time_steps(ds),
        "timestep_nuclides": get_timestep_nuclides(ds),
        "gamma": get_gamma(ds),
    }
    con = db.connect()
    try:
        for k, v in to_proces.items():
            path: Path = target_dir / k
            path.mkdir(parents=True, exist_ok=True)
            frame = _add_material_and_case_columns(
                v,
                material_id,
                case_id,
            )
            sql = f"""
                copy
                (select * from frame)
                to '{path}'
                (
                    format parquet,
                    per_thread_output true,
                    overwrite_or_ignore true,
                    filename_pattern "data_material_id={material_id}_case_id={case_id}_{{i}}",
                    row_group_size {compute_optimal_row_group_size(frame.shape[0])}
                )
                """  # noqa: S608 - sql injection
            con.execute(sql)
    finally:
        con.close()


def _add_material_and_case_columns(
    table: pd.DataFrame,
    material_id: int,
    case_id: int,
) -> pd.DataFrame:
    columns = table.columns.to_numpy()
    table["material_id"] = material_id
    table["case_id"] = case_id
    new_columns = ["material_id", "case_id", *columns]
    return table[new_columns]
