"""Collect data from multiple inventories to Polars tables."""
from __future__ import annotations

from typing import TYPE_CHECKING

import threading

from collections import OrderedDict
from datetime import datetime
from time import strptime

import numpy as np

import msgspec as ms
import polars as pl

if TYPE_CHECKING:
    import numpy.typing as npt

    from xpypact.inventory import Inventory
    from xpypact.nuclide import NuclideInfo

from datetime import UTC  # check import for pythons older than 3.11

RunDataSchema = OrderedDict(
    material_id=pl.UInt32,
    case_id=pl.UInt32,
    timestamp=pl.Datetime,
    flux_name=pl.String,
    dose_rate_type=pl.Categorical,
    dose_rate_distance=pl.Float64,
)

TimeStepSchema = OrderedDict(
    material_id=pl.UInt32,
    case_id=pl.UInt32,
    time_step_number=pl.UInt32,
    irradiation_time=pl.Float64,
    cooling_time=pl.Float64,
    duration=pl.Float64,
    elapsed_time=pl.Float64,
    flux=pl.Float64,
    atoms=pl.Float64,
    activity=pl.Float64,
    alpha_activity=pl.Float64,
    beta_activity=pl.Float64,
    gamma_activity=pl.Float64,
    mass=pl.Float64,
    heat=pl.Float64,
    alpha_heat=pl.Float64,
    beta_heat=pl.Float64,
    gamma_heat=pl.Float64,
    ingestion=pl.Float64,
    inhalation=pl.Float64,
    dose=pl.Float64,
)

TimeStepNuclideSchema = OrderedDict(
    material_id=pl.UInt32,
    case_id=pl.UInt32,
    time_step_number=pl.UInt32,
    zai=pl.UInt32,
    atoms=pl.Float64,
    grams=pl.Float64,
    activity=pl.Float64,
    alpha_activity=pl.Float64,
    beta_activity=pl.Float64,
    gamma_activity=pl.Float64,
    heat=pl.Float64,
    alpha_heat=pl.Float64,
    beta_heat=pl.Float64,
    gamma_heat=pl.Float64,
    dose=pl.Float64,
    ingestion=pl.Float64,
    inhalation=pl.Float64,
)

NuclideSchema = OrderedDict(
    zai=pl.UInt32,
    element=pl.String,
    isotope=pl.UInt16,
    state=pl.UInt8,
    half_life=pl.Float64,
)


GammaSchema = OrderedDict(
    material_id=pl.UInt32,
    case_id=pl.UInt32,
    time_step_number=pl.UInt32,
    g=pl.UInt8,  # index of upper bound in gbins table (>=1)
    rate=pl.Float64,
)


class FullDataCollector(ms.Struct):
    """Class to collect inventory over multiple inventories method.

    Note:
        we assume that all the gamma boundaries are the same over all
        JSON files to be appended.
    """

    lock = threading.RLock()
    rundata: pl.DataFrame = ms.field(default_factory=lambda: pl.DataFrame(schema=RunDataSchema))
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
        ts = datetime(
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
                    rundata.flux_name,
                    rundata.dose_rate_type,
                    rundata.dose_rate_distance,
                ),
            ],
            schema=RunDataSchema,
        )
        self.rundata.vstack(rundata_df, in_place=True)

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
        self.timesteps.vstack(timesteps_df, in_place=True)

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
        self.timestep_nuclides.vstack(timesteps_nuclides_df, in_place=True)

    def _append_timestep_gamma(self, inventory, material_id, case_id):
        gs = inventory.inventory_data[-1].gamma_spectrum

        if not gs:
            return  # there's no gamma spectrum in the inventory

        if self.gbins_boundaries is None:
            self.gbins_boundaries = gs.boundaries
        elif not np.array_equal(self.gbins_boundaries, gs.boundaries):
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
        self.timestep_gamma.vstack(timesteps_gamma_df, in_place=True)

    @property
    def nuclides_as_df(self) -> pl.DataFrame:
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
        )

    @property
    def gbins(self) -> pl.DataFrame:
        """Retrieve gbins.

        Returns:
            Polars table with gbins: g [0..N], boundary[g]
        """
        return pl.DataFrame(
            enumerate(self.gbins_boundaries),  # type: ignore[arg-type]
            schema=OrderedDict(g=pl.UInt8, boundary=pl.Float64),
        )
