"""Code to implement DuckDB DAO."""
from __future__ import annotations

from typing import TYPE_CHECKING

from dataclasses import dataclass
from pathlib import Path

import numpy as np

import msgspec as ms

from xpypact.dao import DataAccessInterface

if TYPE_CHECKING:
    import duckdb as db
    import pandas as pd

    from xpypact.inventory import Inventory

HERE = Path(__file__).parent

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
        sql_path: Path = HERE / "create_schema.sql"
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

    def save(self, inventory: Inventory, material_id=1, case_id=1) -> None:
        """Save xpypact dataset to database.

        Args:
            inventory: xpypact dataset to save
            material_id: additional key to distinguish multiple FISPACT run
            case_id: second additional key
        """
        # use separate cursor for multiprocessing (single connection is locked for a query)
        # https://duckdb.org/docs/api/python/dbapi
        cursor = self.con.cursor()
        _save_run_data(cursor, inventory, material_id, case_id)
        _save_nuclides(cursor, inventory)
        _save_time_steps(cursor, inventory, material_id, case_id)
        _save_time_step_nuclides(cursor, inventory, material_id, case_id)
        _save_gamma(cursor, inventory, material_id, case_id)

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


# noinspection SqlNoDataSourceInspection
def _save_run_data(
    cursor: db.DuckDBPyConnection,
    inventory: Inventory,
    material_id=1,
    case_id=1,
) -> None:
    mi = inventory.meta_info
    # Time stamp in run data:
    # 23:01:19 12 July 2020
    # Format:
    # %H:%M:%S %d %B %Y
    # https://duckdb.org/docs/sql/functions/dateformat
    sql = """
        insert into rundata values
        (
            ?, ?, strptime(?, '%H:%M:%S %d %B %Y'), ?, ?, ?, ?
        )
    """
    record = (material_id, case_id, *(ms.structs.astuple(mi)))
    cursor.execute(sql, record)


# noinspection SqlNoDataSourceInspection
def _save_nuclides(cursor: db.DuckDBPyConnection, inventory: Inventory):
    nuclides = inventory.extract_nuclides()
    sql = """
        insert or ignore
        into nuclide
        values (?,?,?,?,?)
    """
    cursor.executemany(sql, (ms.structs.astuple(x) for x in nuclides))


# noinspection SqlNoDataSourceInspection
def _save_time_steps(cursor: db.DuckDBPyConnection, inventory: Inventory, material_id=1, case_id=1):
    sql = """
        insert into timestep
        values(
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?
        )
    """
    cursor.executemany(
        sql,
        (
            (
                material_id,
                case_id,
                x.number,
                x.elapsed_time,
                x.irradiation_time,
                x.cooling_time,
                x.duration,
                x.flux,
                x.total_atoms,
                x.total_activity,
                x.alpha_activity,
                x.beta_activity,
                x.gamma_activity,
                x.total_mass,
                x.total_heat,
                x.alpha_heat,
                x.beta_heat,
                x.gamma_heat,
                x.ingestion_dose,
                x.inhalation_dose,
                x.dose_rate.dose,
            )
            for x in inventory.inventory_data
        ),
    )


# noinspection SqlNoDataSourceInspection
def _save_time_step_nuclides(
    cursor: db.DuckDBPyConnection,
    inventory: Inventory,
    material_id=1,
    case_id=1,
):
    sql = """
        insert into timestep_nuclide values(
            ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?
        )
        """
    cursor.executemany(
        sql,
        (
            (
                material_id,
                case_id,
                t.number,
                n.zai,
                n.atoms,
                n.grams,
                n.activity,
                n.alpha_activity,
                n.beta_activity,
                n.gamma_activity,
                n.heat,
                n.alpha_heat,
                n.beta_heat,
                n.gamma_heat,
                n.dose,
                n.ingestion,
                n.inhalation,
            )
            for t in inventory.inventory_data
            for n in t.nuclides
        ),
    )


# noinspection SqlNoDataSourceInspection
def _save_gamma(cursor: db.DuckDBPyConnection, inventory: Inventory, material_id=1, case_id=1):
    gs = inventory[0].gamma_spectrum
    if gs is None:
        return  # pragma: no coverage
    sql = """
        insert into gbins values(?, ?);
    """
    boundaries = np.asarray(gs.boundaries, dtype=float)
    cursor.executemany(sql, enumerate(boundaries))
    sql = """
        insert into timestep_gamma values(?, ?, ?, ?, ?);
    """
    mids = 0.5 * (boundaries[:-1] + boundaries[1:])
    cursor.executemany(
        sql,
        (
            (material_id, case_id, t.number, x[0] + 1, x[1])  # use gbins index for upper boundary
            for t in inventory.inventory_data
            if t.gamma_spectrum
            for x in enumerate(np.asarray(t.gamma_spectrum.values, dtype=float) / mids)
            # convert rate MeV/s -> photon/s
        ),
    )
