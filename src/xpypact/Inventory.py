"""Classes to load information from FISPACT output JSON file."""
from __future__ import annotations

from typing import Callable, Iterable, TextIO, cast

from dataclasses import dataclass
from io import TextIOBase
from pathlib import Path

import numpy as np
import orjson as json

from multipledispatch import dispatch
from numpy import ndarray as array

from .RunData import RunData
from .TimeStep import TimeStep


@dataclass
class RunDataCorrected:
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


def _create_json_inventory_data_mapper() -> Callable[[dict], TimeStep]:
    prev_irradiation_time = prev_cooling_time = prev_elapsed_time = 0.0
    number = 1

    def json_inventory_data_mapper(jts: dict) -> TimeStep:
        nonlocal number, prev_irradiation_time, prev_cooling_time, prev_elapsed_time
        ts = TimeStep.from_json(jts)
        duration = ts.irradiation_time - prev_irradiation_time
        if duration == 0.0:
            duration = ts.cooling_time - prev_cooling_time
        if duration < 0.0:
            raise InventoryNonMonotonicTimesError()  # pragma: no cover
        ts.duration = duration
        prev_elapsed_time = ts.elapsed_time = prev_elapsed_time + duration
        if duration == 0.0:
            ts.flux = 0.0
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


@dataclass
class Inventory:
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

    @classmethod
    def from_json(cls, json_dict: dict) -> "Inventory":
        """Construct Inventory instance from JSON dictionary.

        Args:
            json_dict: a JSON dictionary

        Returns:
            Inventory: the new instance
        """
        json_run_data = json_dict.pop("run_data")
        run_data = RunData.from_json(json_run_data)
        json_inventory_data = json_dict.pop("inventory_data")
        mapper = _create_json_inventory_data_mapper()
        inventory_data = [mapper(ts) for ts in json_inventory_data]

        return cls(run_data, inventory_data)

    def extract_times(self) -> array:
        """Create vector of elapsed time for all the time steps in the inventory.

        Returns:
            Vector with elapsed times.
        """
        return extract_times(self.inventory_data)

    def __iter__(self) -> Iterable[TimeStep]:
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


def extract_times(time_steps: Iterable[TimeStep]) -> array:
    """Create vector of elapsed time for all the time steps.

    Args:
        time_steps: list of time steps

    Returns:
        Vector with elapsed times.
    """
    return cast(array, np.fromiter((x.elapsed_time for x in time_steps), dtype=float))


@dispatch(str)
def from_json(text: str) -> Inventory:
    """Construct Inventory instance from JSON text.

    Args:
        text: Text source.

    Returns:
        The loaded Inventory instance.
    """
    json_dict = json.loads(text)
    return Inventory.from_json(json_dict)


@dispatch(TextIOBase)  # type: ignore[no-redef]
def from_json(stream: TextIO) -> Inventory:
    """Construct Inventory instance from JSON stream.

    Args:
        stream: IO stream source.

    Returns:
        The loaded Inventory instance.
    """
    return from_json(stream.read())


@dispatch(Path)  # type: ignore[no-redef]
def from_json(path: Path) -> Inventory:
    """Construct Inventory instance from JSON path.

    Args:
        path: path to source.

    Returns:
        The loaded Inventory instance.
    """
    return from_json(path.read_text(encoding="utf8"))
