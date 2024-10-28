"""Code to implement DuckDB DAO."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pathlib import Path

import msgspec as ms

if TYPE_CHECKING:
    import duckdb as db

    from xpypact.collector import FullDataCollector

HERE = Path(__file__).parent

# On using DuckDB with multiple threads, see
# https://duckdb.org/docs/guides/python/multiple_threads.html


class DuckDBDAOSaveError(ValueError):
    """Error on accessing/saving FISPACT data."""


# noinspection SqlNoDataSourceInspection
class DuckDBDAO(ms.Struct):
    """Implementation of DataAccessInterface for DuckDB."""

    con: db.DuckDBPyConnection

    def get_tables_info(self) -> db.DuckDBPyRelation:
        """Get information on tables in schema."""
        return self.con.sql("select * from information_schema.tables")

    def tables(self) -> tuple[str, str, str, str, str]:
        """List tables being used by xpypact dao.

        Returns:
            Tuple with table names.
        """
        return "rundata", "timestep", "nuclide", "timestep_nuclide", "timestep_gamma"

    def has_schema(self) -> bool:
        """Check if the schema is available in a database."""
        table_names = self.get_tables_info().select("table_name").fetchnumpy()["table_name"]

        if len(table_names) < len(self.tables()):
            return False

        return all(name in table_names for name in self.tables())

    def create_schema(self) -> None:
        """Create tables to store xpypact dataset.

        Retain existing tables.
        """
        sql_path: Path = HERE / "create_schema.sql"
        sql = sql_path.read_text(encoding="utf-8")
        self.con.execute(sql)

    def drop_schema(self) -> None:
        """Drop our DB objects."""
        tables = [
            "timestep_nuclide",
            "timestep_gamma",
            "gbins",
            "timestep",
            "nuclide",
            "rundata",
        ]
        for table in tables:
            self.con.execute(f"drop table if exists {table}")

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

    def load_timestep_times(self) -> db.DuckDBPyRelation:
        """Load time step table.

        Returns:
            time step table
        """
        return self.con.table("time_step_times")

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

    def load_gbins(self) -> db.DuckDBPyRelation:
        """Load gbins table.

        Returns:
            gbins table
        """
        return self.con.table("gbins")

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


def save(
    cursor: db.DuckDBPyConnection,
    collector_result: FullDataCollector.Result,
) -> None:
    """Save collected inventories to a DuckDB database.

    Args:
        cursor: separate multi-threaded cursor to access DuckDB, use con.cursor() in caller
        collector_result: collected inventories as Polars frames
    """
    collected = ms.structs.asdict(collector_result)
    for name, df in collected.items():
        if df is not None:  # pragma: no cover
            cursor.execute(f"create or replace table {name} as select * from df")  # noqa: S608


def create_indices(con: db.DuckDBPyConnection) -> db.DuckDBPyConnection:
    """Create primary key like indices on tables after loading.

    Note:
        indexes are not used and, even more, are harmful in DuckDB.
        This is provided to use the indexes only for testing and debugging the database.
        The test is, that in a valid database with the loaded content
        the indexes should be created successfully.
    """
    return con.execute(
        """
        create unique index rundata_pk on rundata(
            material_id, case_id
        );
        create unique index time_step_times_pk on time_step_times(
            time_step_number
        );
        create unique index timestep_pk on timestep(
            material_id, case_id, time_step_number
        );
        create unique index timestep_nuclide_pk on timestep_nuclide(
            material_id, case_id, time_step_number, zai
        );
        create unique index timestep_gamma_pk on timestep_gamma(
            material_id, case_id, time_step_number, g
        );
        """,
    )
