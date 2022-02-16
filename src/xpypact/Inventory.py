from typing import Dict, Iterable, List, TextIO

from dataclasses import dataclass
from io import TextIOWrapper
from pathlib import Path

import numpy as np
import orjson as json

from multipledispatch import dispatch
from numpy import ndarray as array

from .RunData import RunData
from .TimeStep import TimeStep


@dataclass
class RunDataCorrected:
    timestamp: str
    run_name: str
    flux_name: str
    dose_rate_type: str
    dose_rate_distance: float


@dataclass
class Inventory:
    run_data: RunData
    inventory_data: List[TimeStep]

    @classmethod
    def from_json(cls, json_dict: Dict):
        json_run_data = json_dict.pop("run_data")
        run_data = RunData.from_json(json_run_data)
        json_inventory_data = json_dict.pop("inventory_data")

        def create_json_inventory_data_mapper():
            prev_irradiation_time = prev_cooling_time = prev_elapsed_time = 0.0
            number = 1

            def json_inventory_data_mapper(jts: Dict):
                nonlocal number, prev_irradiation_time, prev_cooling_time, prev_elapsed_time
                ts = TimeStep.from_json(jts)
                duration = ts.irradiation_time - prev_irradiation_time
                if 0.0 == duration:
                    duration = ts.cooling_time - prev_cooling_time
                assert (
                    0.0 <= duration
                ), "Irradiation and cooling times in FISPACT JSON output should be monotonically increasing"
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

        inventory_data = list(
            map(create_json_inventory_data_mapper(), json_inventory_data)
        )

        return cls(run_data, inventory_data)

    @property
    def meta_info(self) -> RunDataCorrected:
        ts = self.inventory_data[-1]  # The dose_rate in the first timestep is empty.
        dose_rate_type, dose_rate_distance = ts.dose_rate_type_and_distance
        return RunDataCorrected(
            self.run_data.timestamp,
            self.run_data.run_name,
            self.run_data.flux_name,
            dose_rate_type,
            dose_rate_distance,
        )

    # def as_array(self, time_step_extractor=total_extractor) -> array:
    #     return np.vstack(list(map(time_step_extractor, self.inventory_data)))

    def __iter__(self) -> Iterable[TimeStep]:
        return iter(self.inventory_data)

    def __len__(self) -> int:
        return len(self.inventory_data)

    def __getitem__(self, item):
        return self.inventory_data[item]


@dispatch(list)
def extract_times(time_steps: List[TimeStep]) -> array:
    return np.fromiter(map(lambda x: x.elapsed_time, time_steps), dtype=float)


@dispatch(Inventory)
def extract_times(inventory: Inventory) -> array:
    return extract_times(inventory.inventory_data)


@dispatch(str)
def from_json(text: str):
    json_dict = json.loads(text)
    return Inventory.from_json(json_dict)


@dispatch(TextIOWrapper)
def from_json(stream: TextIO):
    return from_json(stream.read())


@dispatch(Path)
def from_json(path: Path):
    return from_json(path.read_text())
