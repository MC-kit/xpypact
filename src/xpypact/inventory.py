"""Classes to load information from FISPACT output JSON file."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import io  # - needed for dispatch

from functools import singledispatch
from pathlib import Path  # - needed for dispatch

import msgspec as ms
import numpy as np

# noinspection PyUnresolvedReferences
from xpypact.time_step import TimeStep  # noqa: TC001  - required for Struct field

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from xpypact.nuclide import NuclideInfo
    from xpypact.types import NDArrayFloat

FLOAT_ZERO = 0.0


class RunDataCorrected(ms.Struct):
    """Common data for an FISPACT inventory.

    Note:
        Correction - dose_rate_type and dose_rate_distance are duplicated
        the FISPACT time steps. This information is extracted to this header.
    """

    timestamp: str
    run_name: str
    flux_name: str
    dose_rate_type: str
    dose_rate_distance: float


class InventoryError(ValueError):
    """Base class for inventory exception."""

    def __str__(self) -> str:
        """Use the class __doc__ as a message.

        Returns:
            The __doc__ of the exception class.
        """
        return cast(str, self.__class__.__doc__)  # pragma: no cover


class InventoryNonMonotonicTimesError(InventoryError):
    """Irradiation and cooling times in FISPACT output should be monotonically increasing."""


class Inventory(ms.Struct):
    """Helper class to load FISPACT inventory (output) from JSON file.

    Implements list interface over time steps.
    """

    run_data: RunData
    inventory_data: list[TimeStep]

    @property
    def meta_info(self) -> RunDataCorrected:
        """Create corrected Inventory header.

        Returns:
            RunDataCorrected: with common data for the inventory.
        """
        ts = self.inventory_data[-1]  # The dose_rate in the first timestep can be empty.
        return RunDataCorrected(
            self.run_data.timestamp,
            self.run_data.run_name,
            self.run_data.flux_name,
            ts.dose_rate.type,
            ts.dose_rate.distance,
        )

    def extract_times(self) -> NDArrayFloat:
        """Create vector of elapsed time for all the time steps in the inventory.

        Returns:
            Vector with elapsed times.
        """
        return extract_times(self.inventory_data)

    def extract_nuclides(self) -> set[NuclideInfo]:
        """Extract.

        Returns:
            Set of nuclides present in this inventory.
        """
        nuclides: set[NuclideInfo] = set()
        for ts in self:
            for nuclide in ts.nuclides:
                nuclides.add(nuclide.info)
        return nuclides

    def iterate_time_step_nuclides(
        self,
    ) -> Iterator[
        tuple[
            int,
            int,
            float,
            float,
            float,
            float,
            float,
            float,
            float,
            float,
            float,
            float,
            float,
            float,
            float,
        ]
    ]:
        """Scan the time steps and nuclides in these steps.

        Returns:
            Iterator over all the nuclides collected in the Inventory.
        """
        return (
            (
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
            for t in self.inventory_data
            for n in t.nuclides
        )

    def iterate_time_step_gamma(self) -> Iterator[tuple[int, int, float]]:
        """Scan the time steps and gamma spectrum.

        Returns:
            Iterator over all the gamma bins presented in the Inventory.
        """
        return (
            (
                t.number,
                g + 1,  # g + 1 corresponds to index of the upper bound of a bin in gbins table
                rate,
            )
            for t in self.inventory_data
            if t.gamma_spectrum
            for g, rate in enumerate(t.gamma_spectrum.values)
        )

    def __post_init__(self) -> None:
        """Define time steps durations and elapsed time.

        Raises:
            InventoryNonMonotonicTimesError: if time sequences in JSON are not in order
        """
        number = 1
        prev_irradiation_time = prev_cooling_time = prev_elapsed_time = 0.0
        for ts in self.inventory_data:
            duration = ts.irradiation_time - prev_irradiation_time
            if duration == FLOAT_ZERO:
                duration = ts.cooling_time - prev_cooling_time
            if duration < FLOAT_ZERO:
                raise InventoryNonMonotonicTimesError  # pragma: no cover
            ts.duration = duration
            prev_elapsed_time = ts.elapsed_time = prev_elapsed_time + duration
            if duration == FLOAT_ZERO:
                ts.flux = FLOAT_ZERO
            ts.number = number
            number += 1
            prev_irradiation_time, prev_cooling_time = (
                ts.irradiation_time,
                ts.cooling_time,
            )

    def __iter__(self) -> Iterator[TimeStep]:
        """Iterate over time steps.

        Returns:
            Iterator over the time steps.
        """
        return iter(self.inventory_data)

    def __len__(self) -> int:
        """Length, delegated to time steps.

        Returns:
            int: length of the time steps list.
        """
        return len(self.inventory_data)

    def __getitem__(self, item: int) -> TimeStep:
        """List interface delegated to the time steps.

        Args:
            item: an item index or slice

        Returns:
            Selected item or items.
        """
        return self.inventory_data[item]


def extract_times(time_steps: Iterable[TimeStep]) -> NDArrayFloat:
    """Create vector of elapsed time for all the time steps.

    Args:
        time_steps: list of time steps

    Returns:
        Vector with elapsed times.
    """
    return np.fromiter((x.elapsed_time for x in time_steps), dtype=float)


# @dispatch(str)
@singledispatch
def from_json(text: str) -> Inventory:
    """Construct Inventory instance from JSON text.

    Args:
        text: Text source.

    Returns:
        The loaded Inventory instance.
    """
    return ms.json.decode(text, type=Inventory)


@from_json.register
def _(stream: io.IOBase) -> Inventory:  # type: ignore[misc]
    """Construct Inventory instance from JSON stream.

    Args:
        stream: IO stream source.

    Returns:
        The loaded Inventory instance.
    """
    return from_json(stream.read())


@from_json.register
def _(path: Path) -> Inventory:  # type: ignore[misc]
    """Construct Inventory instance from JSON path.

    Args:
        path: path to source.

    Returns:
        The loaded Inventory instance.
    """
    return from_json(path.read_text(encoding="utf8"))


class RunData(ms.Struct, frozen=True, gc=False):
    """FISPACT run title data."""

    timestamp: str
    run_name: str
    flux_name: str

    def asdict(self) -> dict[str, str]:
        """Get dict representation."""
        return ms.structs.asdict(self)

    def astuple(self) -> tuple[str, str, str]:
        """Get tuple representation."""
        return cast(tuple[str, str, str], ms.structs.astuple(self))

    @classmethod
    def from_json(cls, json_dict: dict[str, str]) -> RunData:
        """Construct RunData instance from JSON.

        Args:
            json_dict: source dictionary

        Returns:
            The loaded instance of RunData
        """
        return ms.convert(json_dict, cls)
