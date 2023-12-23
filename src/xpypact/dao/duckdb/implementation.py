"""Code to implement DuckDB DAO."""
from __future__ import annotations

from pathlib import Path

import duckdb as db
import msgspec as ms

from xpypact.dao import DataAccessInterface

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


def _save_run_data(
    cursor: db.DuckDBPyConnection,
    inventory: Inventory,
    material_id=1,
    case_id=1,
) -> None:
    mi = inventory.meta_info
    # Time stamp in run data:
    # "23:01:19 12 July 2020"
    # Format:
    # '%H:%M:%S %d %B %Y'
    # https://duckdb.org/docs/sql/functions/dateformat
    sql = """
        insert into rundata values
        (
            ?, ?, strptime(?, '%H:%M:%S %d %B %Y'), ?, ?, ?, ?
        )
    """
    record = (material_id, case_id, *(mi.astuple()[1:]))
    cursor.execute(sql, record)


def _save_nuclides(cursor: db.DuckDBPyConnection, inventory: Inventory):
    nuclides = inventory.extract_nuclides()
    sql = """
        insert or ignore
        into nuclide
        values (?,?,?,?,?)
    """
    cursor.executemany(sql, (ms.structs.astuple(x) for x in nuclides))


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
                x.atoms,
                x.activity,
                x.alpha_activity,
                x.beta_activity,
                x.gamma_activity,
                x.mass,
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


def _save_time_step_nuclides(
    cursor: db.DuckDBPyConnection,
    ds: Inventory,
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
            for n in t.nuclides
            for t in inventory.inventory_data
        ),
    )


def _save_gamma(cursor: db.DuckDBPyConnection, inventory: Inventory, material_id=1, case_id=1):
    """Material_id uinteger not null,.

    case_id uinteger not null,
    time_step_number uinteger not null,
    boundary real not null check (0 <= boundary),
    rate real not null,
    """
    sql = """
        insert into timestep_gamma values(?, ?, ?, ?, ?);
    """
    cursor.executemany(
        sql,
        (
            (material_id, case_id, t.number, x[0], x[1])
            for x in zip(t.gamma_spectrum.boundaries[1:], t.gamma_spectrum.values)
            for t in inventory.inventory_data
            if t.gamma_spectrum
        ),
    )


# def compute_optimal_row_group_size(frame_size: int, _cpu_count: int = 0) -> int:
#     """Compute DuckDB row group size for writing to parquet files.
#
#     This should optimize speed of reading from parquet files.
#
#     Args:
#         frame_size: length of rows to be written
#         _cpu_count: number to split the rows
#
#     Returns:
#         the row group size
#     """
#     if not _cpu_count:
#         _cpu_count = max(4, cpu_count())
#     size = frame_size // _cpu_count
#     return min(max(size, 2048), 1_000_000)


# def write_parquet(target_dir: Path, ds: xr.Dataset, material_id: int, case_id: int) -> None:
#     """Store xpypact dataset to parquet directories.
#
#     Create in 4 subdirectories in `target_dir` for data subjects run_data, time_steps,
#     time_step_nuclides, and gamma dataframes.
#     Save the dataframes as parquet files.
#     Hive partiotioning is not used in this version, because resulting partions are too small.
#     The data for  can be  saved in the same `target_dir` as long as the material_id and case_id are
#     unique for an data subject.
#
#     This structure can be easily and efficiently accessed from DuckDB as external data.
#
#     For instance to collect all the inventories::
#
#         select * from read_parquet('<target_dir>/inventory/*/*/*.parquet', hive_partitioning=true)
#
#     We use in memory DuckDB instance to transfer data to parquet to ensure compatibility
#     for later reading back to DuckDB.
#
#     See about row_group_size: https://duckdb.org/docs/data/parquet/tips
#
#     Args:
#         target_dir: root directory to store a dataset in subdirectories
#         ds: dataset to store
#         material_id: the level 1 key to distinguish with other datasets in the folder,
#                      may correspond to material in R2S
#         case_id: the level 2 key, may correspond neutron group or case in R2S of fispact run.
#                  The case may be registered in the database to provide additional information.
#     """
#     to_proces: dict[str, pd.DataFrame] = {
#         "run_data": get_run_data(ds),
#         "nuclides": get_nuclides(ds),
#         "time_steps": get_time_steps(ds),
#         "timestep_nuclides": get_timestep_nuclides(ds),
#         "gamma": get_gamma(ds),
#     }
#     con = db.connect()
#     try:
#         for k, v in to_proces.items():
#             if v is None:
#                 if k != "gamma":
#                     msg = f"Not found data {k!r}"
#                     raise ValueError(msg)
#                 continue
#             path: Path = target_dir / k
#             path.mkdir(parents=True, exist_ok=True)
#             frame = _add_material_and_case_columns(
#                 v,
#                 material_id,
#                 case_id,
#             )
#             sql = f"""
#                 copy
#                 (select * from frame)
#                 to '{path}'
#                 (
#                     format parquet,
#                     per_thread_output true,
#                     overwrite_or_ignore true,
#                     filename_pattern "data_material_id={material_id}_case_id={case_id}_{{i}}",
#                     row_group_size {compute_optimal_row_group_size(frame.shape[0])}
#                 )
#                 """  # - sql injection
#             con.execute(sql)
#     finally:
#         con.close()
#
#
# def _add_material_and_case_columns(
#     table: pd.DataFrame,
#     material_id: int,
#     case_id: int,
# ) -> pd.DataFrame:
#     columns = table.columns.to_numpy()
#     table["material_id"] = material_id
#     table["case_id"] = case_id
#     new_columns = ["material_id", "case_id", *columns]
#     return table[new_columns]
