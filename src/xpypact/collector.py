"""Collect data from multiple inventories to Polars tables."""
from __future__ import annotations

from typing import TYPE_CHECKING

import datetime
import sys
import threading

from collections import OrderedDict
from time import strptime

import numpy as np

import msgspec as ms
import polars as pl

if TYPE_CHECKING:
    import numpy.typing as npt

    from xpypact.inventory import Inventory
    from xpypact.nuclide import NuclideInfo


if sys.version_info >= (3, 11):
    UTC = datetime.UTC
else:
    UTC = datetime.timezone.utc

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

    def _append_rundata(self, inventory, material_id, case_id):
        rundata = inventory.meta_info
        st = strptime(rundata.timestamp, "%H:%M:%S %d %B %Y")
        ts = datetime.datetime(
            year=st.tm_year,
            month=st.tm_mon,
            day=st.tm_mday,
            hour=st.tm_hour,
            minute=st.tm_min,
            second=st.tm_sec,
            tzinfo=UTC,
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
            schema=RunDataSchema,
        ).with_columns(pl.col("dose_rate_distance").round(5))
        self.rundata = pl.concat([self.rundata, rundata_df], how="vertical", rechunk=False)

    def _append_timesteps(self, inventory, material_id, case_id):
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

    def _append_timestep_nuclides(self, inventory, material_id, case_id):
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

    def _append_timestep_gamma(self, inventory, material_id, case_id):
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

    def get_timestep_gamma_as_spectrum(self) -> pl.DataFrame:
        """Convert gamma values MeV/s -> photon/s.

        In FISPACT JSON gamma emission is presented in MeV/s,
        but we need intensities in photon/s to represent gamma source.

        Returns:
            time_step_gamma with rates in photon/s
        """
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
        )
        return ql.collect()
