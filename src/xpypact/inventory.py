"""Classes to load information from FISPACT output JSON file."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import io  # - needed for dispatch

from functools import singledispatch
from pathlib import Path  # - needed for dispatch

import numpy as np

import msgspec as ms

from xpypact.run_data import RunData
from xpypact.time_step import TimeStep

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator

    from xpypact.utils.types import NDArrayFloat

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


def _create_json_inventory_data_mapper() -> Callable[[dict[str, Any]], TimeStep]:
    prev_irradiation_time = prev_cooling_time = prev_elapsed_time = FLOAT_ZERO
    number = 1

    def json_inventory_data_mapper(jts: dict[str, Any]) -> TimeStep:
        nonlocal number, prev_irradiation_time, prev_cooling_time, prev_elapsed_time
        ts = TimeStep.from_json(jts)
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
        return ts

    return json_inventory_data_mapper


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

    # @classmethod
    # def from_json(cls, json_dict: dict[str, Any]) -> Inventory:
    #     """Construct Inventory instance from JSON dictionary.
    #
    #     Args:
    #         json_dict: a JSON dictionary
    #
    #     Returns:
    #         Inventory: the new instance
    #     """
    #     json_run_data = json_dict.pop("run_data")
    #     run_data = RunData.from_json(json_run_data)
    #     json_inventory_data = json_dict.pop("inventory_data")
    #     mapper = _create_json_inventory_data_mapper()
    #     inventory_data = [mapper(ts) for ts in json_inventory_data]
    #
    #     return cls(run_data, inventory_data)

    def extract_times(self) -> NDArrayFloat:
        """Create vector of elapsed time for all the time steps in the inventory.

        Returns:
            Vector with elapsed times.
        """
        return extract_times(self.inventory_data)

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

    def __getitem__(self, item):
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
    # json_dict = json.loads(text)  # pylint: disable=no-member
    # return Inventory.from_json(json_dict)
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
