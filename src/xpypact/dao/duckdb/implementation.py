"""Code to implement DuckDB DAO."""
from __future__ import annotations

from typing import TYPE_CHECKING

import threading

from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

import duckdb as db
import msgspec as ms

if TYPE_CHECKING:
    import pandas as pd

    from xpypact.inventory import Inventory, NuclideInfo
    from xpypact.time_step import GammaSpectrum

HERE = Path(__file__).parent

# On using DuckDB with multiple threads, see
# https://duckdb.org/docs/guides/python/multiple_threads.html


class DuckDBDAOSaveError(ValueError):
    """Error on accessing/saving FISPACT data."""


@dataclass
class CommonDataCollector:
    """Class to collect nuclides and gamma bins on multiple run of save() method."""

    nuclides: set[NuclideInfo] = field(default_factory=set)
    nuclides_lock: threading.RLock = field(default_factory=threading.RLock)
    gbins_boundaries: np.ndarray | None = None
    gbins_boundaries_lock: threading.RLock = field(default_factory=threading.RLock)

    def update_nuclides(self, nuclides: set[NuclideInfo]) -> None:
        """Collect nuclides."""
        with self.nuclides_lock:
            self.nuclides.update(nuclides)

    def store_gbins(self, gs: GammaSpectrum) -> None:
        """Store gbins once - should be the same on all save() runs."""
        with self.gbins_boundaries_lock:
            if self.gbins_boundaries is None:
                self.gbins_boundaries = np.asarray(gs.boundaries, dtype=float)

    def save(self, con: db.DuckDBPyConnection) -> None:
        """Save information accumulated on multithreading processing all the inventories.

        Args:
            con: connection to store the two 'nuclide' and 'gbins' tables.

        Call this after all the JSON files are imported.
        """
        self._save_nuclides(con)
        if self.gbins_boundaries is not None:  # pragma: no coverage
            _save_gbins(con, self.gbins_boundaries)

    def _save_nuclides(self, con: db.DuckDBPyConnection) -> None:
        """Save nuclides on multithreading saving is complete.

        Call this when all the inventories are saved.

        Args:
            con: where to save
        """
        sql = """
            insert
            into nuclide
            values (?,?,?,?,?)
            ;
        """
        con.executemany(sql, (ms.structs.astuple(x) for x in self.nuclides))


# noinspection SqlNoDataSourceInspection
class DuckDBDAO(ms.Struct):
    """Implementation of DataAccessInterface for DuckDB."""

    con: db.DuckDBPyConnection

    def get_tables_info(self) -> pd.DataFrame:
        """Get information on tables in schema."""
        return self.con.execute("select * from information_schema.tables").df()

    def tables(self) -> tuple[str, str, str, str, str]:
        """List tables being used by xpypact dao.

        Returns:
            Tuple with table names.
        """
        return "rundata", "timestep", "nuclide", "timestep_nuclide", "timestep_gamma"

    def has_schema(self) -> bool:
        """Check if the schema is available in a database."""
        db_tables = self.get_tables_info()

        if len(db_tables) < len(self.tables()):
            return False

        table_names = db_tables["table_name"].to_numpy()

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
    inventory: Inventory,
    material_id: int,
    case_id: int,
    cdc: CommonDataCollector,
) -> None:
    """Save xpypact dataset to database.

    This can be used in multithreading mode.

    Args:
        cursor: separate multi-threaded cursor to access DuckDB, use con.cursor() in caller
        inventory: xpypact dataset to save
        material_id: additional key to distinguish multiple FISPACT run
        case_id: second additional key
        cdc: common data collector to store nuclides and gbins
             over multiple and multithreaded runs
    """
    # use separate cursor for multithreading (single connection is locked for a query)
    # https://duckdb.org/docs/api/python/dbapi
    _save_run_data(cursor, inventory, material_id, case_id)
    _save_time_steps(cursor, inventory, material_id, case_id)
    _save_time_step_nuclides(cursor, inventory, material_id, case_id)
    _save_gamma(cursor, inventory, material_id, case_id)
    cdc.update_nuclides(inventory.extract_nuclides())
    gs = inventory[-1].gamma_spectrum
    if gs:  # pragma: no coverage
        cdc.store_gbins(gs)


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
            for x in inventory
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
        sorted(
            ((material_id, case_id, *t) for t in inventory.iterate_time_step_nuclides()),
            key=lambda x: x[0:4],  # sort items to improve DuckDB performance and size
        ),
    )


# noinspection SqlNoDataSourceInspection
def _save_gamma(
    cursor: db.DuckDBPyConnection,
    inventory: Inventory,
    material_id=1,
    case_id=1,
) -> None:
    gs = inventory[0].gamma_spectrum
    if gs is None:
        return  # pragma: no coverage
    boundaries = np.asarray(gs.boundaries, dtype=float)

    sql = """
        insert into timestep_gamma values(?, ?, ?, ?, ?);
    """
    mids = 0.5 * (boundaries[:-1] + boundaries[1:])
    cursor.executemany(
        sql,
        sorted(
            (
                # use gbins index for upper boundary
                (material_id, case_id, t.number, x[0] + 1, x[1])
                for t in inventory.inventory_data
                if t.gamma_spectrum
                for x in enumerate(np.asarray(t.gamma_spectrum.values, dtype=float) / mids)
                # convert rate MeV/s -> photon/s
            ),
            key=lambda x: x[0:4],
        ),
    )


def _save_gbins(cursor: db.DuckDBPyConnection, boundaries: np.ndarray) -> None:
    sql = """
        insert into gbins values(?, ?);
    """
    cursor.executemany(sql, enumerate(boundaries))


def write_parquets(
    intermediate_dir: Path,
    inventory: Inventory,
    material_id: int,
    case_id: int,
    cdc: CommonDataCollector,
) -> None:
    """Save an Inventory as parquet files.

    Also creates SQL files to create and load the data.

    Args:
        intermediate_dir: where to save parquets
        inventory: what to save
        material_id: ... to distinguish runs by material
        case_id: ... to distinguish runs by case
        cdc: common data collector
    """
    case_dir_path = intermediate_dir / f"material_id={material_id}/case_id={case_id}"
    if case_dir_path.is_dir():
        msg = f"Directory {case_dir_path} already exists"
        raise ValueError(msg)
    case_dir_path.mkdir(parents=True, exist_ok=True)
    con = db.connect()
    dao = DuckDBDAO(con)
    dao.create_schema()
    save(con, inventory, material_id, case_id, cdc)
    con.execute(
        f"""
        export database {str(case_dir_path)!r}
        (
            format 'parquet',
            row_group_size 100000,
            per_thread_output 'true',
            compression 'snappy'
        );
        """,
    )


def load_parquets(con: db.DuckDBPyConnection, intermediate_dir: Path) -> DuckDBDAO:
    """Load parquet files in bulk mode.

    Args:
        con: destination
        intermediate_dir: where to look for parquets
        cdc: common data collected
    """
    dao = DuckDBDAO(con)
    dao.drop_schema()
    dao.create_schema()
    for table in dao.tables():
        suffix = f"*/*/{table}.parquet/*.parquet"
        con.execute(
            f"""
            copy {table} from {str(intermediate_dir / suffix)!r}
            (
                format 'parquet',
                compression 'snappy'
            )
            """,
        )
    return dao


def create_indices(con: db.DuckDBPyConnection) -> db.DuckDBPyConnection:
    """Create primary key like indices on tables after loading."""
    return con.execute(
        """
        create unique index rundata_pk on rundata(
            material_id, case_id
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
