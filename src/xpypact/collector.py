"""Collect data from multiple inventories to Polars tables."""

from __future__ import annotations

from typing import TYPE_CHECKING

import datetime as dt
import sys
import threading

from collections import OrderedDict
from time import strptime

import msgspec as ms
import numpy as np
import polars as pl

if TYPE_CHECKING:
    from pathlib import Path

    import numpy.typing as npt

    from xpypact.inventory import Inventory
    from xpypact.nuclide import NuclideInfo


if sys.version_info >= (3, 11):  # pragma: no cover
    UTC = dt.UTC
else:
    UTC = dt.timezone.utc  # pragma: no cover

RunDataSchema = OrderedDict(
    material_id=pl.UInt32,
    case_id=pl.UInt32,
    timestamp=pl.Datetime,
    run_name=pl.String,
    flux_name=pl.String,
    dose_rate_type=pl.Enum(["Point source", "Plane source"]),
    dose_rate_distance=pl.Float32,
)

TimeStepSchema = OrderedDict(
    material_id=pl.UInt32,
    case_id=pl.UInt32,
    time_step_number=pl.UInt32,
    irradiation_time=pl.Float32,
    cooling_time=pl.Float32,
    duration=pl.Float32,
    elapsed_time=pl.Float32,
    flux=pl.Float32,
    atoms=pl.Float32,
    activity=pl.Float32,
    alpha_activity=pl.Float32,
    beta_activity=pl.Float32,
    gamma_activity=pl.Float32,
    mass=pl.Float32,
    heat=pl.Float32,
    alpha_heat=pl.Float32,
    beta_heat=pl.Float32,
    gamma_heat=pl.Float32,
    ingestion=pl.Float32,
    inhalation=pl.Float32,
    dose=pl.Float32,
)

TimeStepNuclideSchema = OrderedDict(
    material_id=pl.UInt32,
    case_id=pl.UInt32,
    time_step_number=pl.UInt32,
    zai=pl.UInt32,
    atoms=pl.Float32,
    grams=pl.Float32,
    activity=pl.Float32,
    alpha_activity=pl.Float32,
    beta_activity=pl.Float32,
    gamma_activity=pl.Float32,
    heat=pl.Float32,
    alpha_heat=pl.Float32,
    beta_heat=pl.Float32,
    gamma_heat=pl.Float32,
    dose=pl.Float32,
    ingestion=pl.Float32,
    inhalation=pl.Float32,
)

NuclideSchema = OrderedDict(
    zai=pl.UInt32,
    element=pl.String,
    mass_number=pl.UInt16,
    state=pl.UInt8,
    half_life=pl.Float32,
)


GammaSchema = OrderedDict(
    material_id=pl.UInt32,
    case_id=pl.UInt32,
    time_step_number=pl.UInt32,
    g=pl.UInt8,  # index of upper bound in gbins table (>=1)
    rate=pl.Float32,  # to store all the digits from a FISPACT JSON file we need 64 bits
)


class FullDataCollector(ms.Struct):
    """Class to collect inventory over multiple inventories method.

    Note:
        we assume that all the gamma boundaries are the same over all
        JSON files to be appended.
    """

    lock = threading.RLock()
    rundata: pl.DataFrame = ms.field(
        default_factory=lambda: pl.DataFrame(schema=RunDataSchema),  # type: ignore[arg-type]
    )
    timesteps: pl.DataFrame = ms.field(default_factory=lambda: pl.DataFrame(schema=TimeStepSchema))
    timestep_nuclides: pl.DataFrame = ms.field(
        default_factory=lambda: pl.DataFrame(schema=TimeStepNuclideSchema),
    )
    timestep_gamma: pl.DataFrame = ms.field(
        default_factory=lambda: pl.DataFrame(schema=GammaSchema),
    )
    nuclides: set[NuclideInfo] = ms.field(default_factory=set)
    gbins_boundaries: npt.NDArray[np.float64] | None = None

    def append(self, inventory: Inventory, material_id: int, case_id: int) -> FullDataCollector:
        """Append inventory to this collector.

        Args:
            inventory: what to append
            material_id: identified #1 to distinguish multiple inventories
            case_id: identifier #2 ...

        Returns:
            self - for chaining
        """
        with self.lock:
            self.nuclides.update(inventory.extract_nuclides())
            self._append_rundata(inventory, material_id, case_id)
            self._append_timesteps(inventory, material_id, case_id)
            self._append_timestep_nuclides(inventory, material_id, case_id)
            self._append_timestep_gamma(inventory, material_id, case_id)

        return self

    def _append_rundata(self, inventory: Inventory, material_id: int, case_id: int) -> None:
        rundata = inventory.meta_info
        st = strptime(rundata.timestamp, "%H:%M:%S %d %B %Y")
        ts = dt.datetime(  # noqa: DTZ001 - no tzinfo is available from the FISPACT output
            year=st.tm_year,
            month=st.tm_mon,
            day=st.tm_mday,
            hour=st.tm_hour,
            minute=st.tm_min,
            second=st.tm_sec,
            tzinfo=None,
        )
        rundata_df = pl.DataFrame(
            [
                (
                    material_id,
                    case_id,
                    ts,
                    rundata.run_name,
                    rundata.flux_name,
                    rundata.dose_rate_type,
                    rundata.dose_rate_distance,
                ),
            ],
            schema=RunDataSchema,  # type: ignore[arg-type]
            orient="row",
        ).with_columns(pl.col("dose_rate_distance").round(5))
        self.rundata = pl.concat([self.rundata, rundata_df], how="vertical", rechunk=False)

    def _append_timesteps(self, inventory: Inventory, material_id: int, case_id: int) -> None:
        timesteps_df = pl.DataFrame(
            (
                (
                    material_id,
                    case_id,
                    ts.number,
                    ts.irradiation_time,
                    ts.cooling_time,
                    ts.duration,
                    ts.elapsed_time,
                    ts.flux,
                    ts.total_atoms,
                    ts.total_activity,
                    ts.alpha_activity,
                    ts.beta_activity,
                    ts.gamma_activity,
                    ts.total_mass,
                    ts.total_heat,
                    ts.alpha_heat,
                    ts.beta_heat,
                    ts.gamma_heat,
                    ts.ingestion_dose,
                    ts.inhalation_dose,
                    ts.dose_rate.dose,
                )
                for ts in inventory
            ),
            schema=TimeStepSchema,
        )
        self.timesteps = pl.concat([self.timesteps, timesteps_df], how="vertical", rechunk=False)

    def _append_timestep_nuclides(
        self, inventory: Inventory, material_id: int, case_id: int
    ) -> None:
        timesteps_nuclides_df = pl.DataFrame(
            (
                (
                    material_id,
                    case_id,
                    *n,
                )
                for n in inventory.iterate_time_step_nuclides()
            ),
            schema=TimeStepNuclideSchema,
        )
        self.timestep_nuclides = pl.concat(
            [self.timestep_nuclides, timesteps_nuclides_df],
            how="vertical",
            rechunk=False,
        )

    def _append_timestep_gamma(self, inventory: Inventory, material_id: int, case_id: int) -> None:
        gs = inventory.inventory_data[-1].gamma_spectrum

        if not gs:
            return  # there's no gamma spectrum in the inventory

        if self.gbins_boundaries is None:
            self.gbins_boundaries = np.asarray(gs.boundaries, dtype=float)
        elif not np.array_equal(self.gbins_boundaries, gs.boundaries):  # pragma: no cover
            msg = "Assumption fails: all the gamma boundaries are the same"
            raise ValueError(msg)

        timesteps_gamma_df = pl.DataFrame(
            (
                (
                    material_id,
                    case_id,
                    *tsg,
                )
                for tsg in inventory.iterate_time_step_gamma()
            ),
            schema=GammaSchema,
        )
        self.timestep_gamma = pl.concat(
            [self.timestep_gamma, timesteps_gamma_df],
            how="vertical",
            rechunk=False,
        )

    def _get_timestep_times(self) -> pl.DataFrame:
        first_case = self.timesteps.lazy().select("material_id", "case_id").limit(1)
        return (
            self.timesteps.lazy()
            .join(first_case, on=("material_id", "case_id"))
            .with_columns((pl.col("flux") > 0.0).alias("with_flux"))
            .sort(by="time_step_number")
            .select(
                "time_step_number",
                "elapsed_time",
                "irradiation_time",
                "cooling_time",
                "duration",
                "with_flux",
            )
            .collect()
        )

    def get_nuclides_as_df(self) -> pl.DataFrame:
        """Retrieve collected nuclides.

        Returns:
            table of collected nuclides
        """
        nuclides = sorted(self.nuclides, key=lambda x: x.zai)
        return pl.DataFrame(
            (
                (
                    n.zai,
                    n.element,
                    n.isotope,
                    1 if n.state else 0,
                    n.half_life,
                )
                for n in nuclides
            ),
            schema=NuclideSchema,
        ).with_columns(pl.col("zai").set_sorted())

    def get_gbins(self) -> pl.DataFrame | None:
        """Retrieve gbins.

        Returns:
            Polars table with gbins: g [0..N], boundary[g]
        """
        if self.gbins_boundaries is None:
            return None

        return pl.DataFrame(
            enumerate(self.gbins_boundaries),
            schema=OrderedDict(g=pl.UInt8, boundary=pl.Float32),
        ).with_columns(pl.col("g").set_sorted(), pl.col("boundary").set_sorted())

    def get_timestep_gamma_as_spectrum(self) -> pl.DataFrame | None:
        """Convert gamma values MeV/s -> photon/s.

        In FISPACT JSON gamma emission is presented in MeV/s,
        but we need intensities in photon/s to represent gamma source.

        Returns:
            time_step_gamma with rates in photon/s
        """
        if self.timestep_gamma.is_empty():  # pragma: no cover
            return None

        mids = pl.DataFrame(
            (
                (g + 1, m)
                for g, m in enumerate(
                    0.5 * (self.gbins_boundaries[:-1] + self.gbins_boundaries[1:]),  # type: ignore[index]
                )
            ),
            schema={"g": pl.UInt8, "mid": pl.Float32},
        ).lazy()
        # Convert
        ql = (
            self.timestep_gamma.lazy()
            .join(mids, on="g")
            .with_columns((pl.col("rate") / pl.col("mid")).alias("rate"))
            .select(pl.all().exclude("mid"))
            .sort("material_id", "case_id", "time_step_number", "g", maintain_order=True)
            .set_sorted("material_id")
        )
        return ql.collect()

    class Result(ms.Struct):
        """Finished collected data.

        The only function of this class is to save
        the collected data to parquet files.
        """

        rundata: pl.DataFrame
        time_step_times: pl.DataFrame  # use time_step_ prefix here
        timestep: pl.DataFrame
        nuclide: pl.DataFrame
        timestep_nuclide: pl.DataFrame
        gbins: pl.DataFrame | None
        timestep_gamma: pl.DataFrame | None

        def save_to_parquets(self, out: Path, *, override: bool = False) -> None:
            """Save collectd data as parquet files.

            Args:
                out: directory where to save
                override: override existing files, default - raise exception

            Raises:
                FileExistError: if destination file exists and override is not specified.
            """
            collected = ms.structs.asdict(self)
            out.mkdir(parents=True, exist_ok=True)
            for name, df in collected.items():
                if df is None:  # pragma: no cover
                    continue
                dst = out / f"{name}.parquet"
                if dst.exists() and not override:
                    msg = f"File {dst} already exists and override is not specified."
                    raise FileExistsError(msg)
                df.write_parquet(dst)

    def get_result(self) -> FullDataCollector.Result:
        """Finish and present collected data."""
        return FullDataCollector.Result(
            rundata=self.rundata.sort("material_id", "case_id").set_sorted("material_id"),
            time_step_times=self._get_timestep_times(),
            timestep=self.timesteps.sort(
                "material_id",
                "case_id",
                "time_step_number",
            ).set_sorted("material_id"),
            nuclide=self.get_nuclides_as_df(),
            timestep_nuclide=self.timestep_nuclides.sort(
                "material_id",
                "case_id",
                "time_step_number",
                "zai",
            ).set_sorted("material_id"),
            gbins=self.get_gbins(),
            timestep_gamma=self.get_timestep_gamma_as_spectrum(),
        )
