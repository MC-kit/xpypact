"""Test loading Inventory from FISPACT JSON file."""
from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np

import pytest

from xpypact.inventory import Inventory, RunData, from_json
from xpypact.time_step import DoseRate, TimeStep

if TYPE_CHECKING:
    from xpypact.inventory import RunDataCorrected

SECONDS_PER_YEAR = int(365.24 * 24 * 3600)


@pytest.fixture(scope="module")
def inventory(data):
    return from_json(data / "Ag-1.json")


@pytest.mark.parametrize(
    "inp,expected",
    [
        (
            {
                "timestamp": "23:01:19 12 July 2020",
                "run_name": "* Material Ag, fluxes 1",
                "flux_name": "55.F9.10 11-L2-02W HFS_GLRY_08_U",
            },
            RunData(
                timestamp="23:01:19 12 July 2020",
                run_name="* Material Ag, fluxes 1",
                flux_name="55.F9.10 11-L2-02W HFS_GLRY_08_U",
            ),
        ),
    ],
)
def test_run_data(inp, expected):
    actual = RunData.from_json(inp)
    assert actual == expected
    assert inp == actual.asdict(), "Dictionary representation is not equivalent to input"
    assert (
        inp["timestamp"],
        inp["run_name"],
        inp["flux_name"],
    ) == actual.astuple(), "Tuple representation is not equivalent to input"


def test_constructor():
    rd = RunData(timestamp="23:01:19 12 July 2020", run_name="b", flux_name="c")
    assert rd.timestamp == "23:01:19 12 July 2020"
    assert rd.run_name == "b"
    assert rd.flux_name == "c"
    dr = DoseRate(distance=1.0, dose=1e-7)
    ts = TimeStep(
        irradiation_time=1.0,
        dose_rate=dr,
    )
    inventory_data = [ts]
    inventory = Inventory(run_data=rd, inventory_data=inventory_data)
    assert inventory.run_data is rd
    assert inventory.inventory_data
    dose = inventory.inventory_data[0].dose_rate.dose
    assert isinstance(dose, float), "String values should be converted to floats"
    assert dose == 1.0e-7


def test_loading(inventory):
    assert len(inventory) == 2
    assert len(inventory.inventory_data) == 2
    assert inventory.run_data.timestamp == "23:01:19 12 July 2020"
    ts1, ts2 = inventory.inventory_data
    assert len(ts1.nuclides) == 2
    assert ts1.is_cooling
    assert ts1.flux == 0.0
    assert len(ts2.nuclides) == 48
    assert not ts2.is_cooling
    assert ts2.elapsed_time == 0.631152e8
    assert ts2.flux == 0.24452e11
    assert ts1.nuclides_mass == pytest.approx(1e-3, rel=1e-3)


def test_loading_from_stream(data, inventory):
    with (data / "Ag-1.json").open(encoding="utf8") as fid:
        inv = from_json(fid)
        assert len(inv) == len(inventory)


def test_first_time_step(inventory):
    first_time_step = inventory.inventory_data[0]
    assert (
        first_time_step.dose_rate.mass == 1.0e-3
    ), "FISPACT value for the the dose_rate.mass in the first time step is to be 1.0e-3"


def test_inventory_get_meta_info(inventory):
    actual: RunDataCorrected = inventory.meta_info
    assert actual.run_name == "* Material Ag, fluxes 1"
    assert actual.flux_name == "55.F9.10 11-L2-02W HFS_GLRY_08_U"
    assert actual.dose_rate_type == "Point source"
    assert actual.dose_rate_distance == 1.0  # meters


def test_elapsed_time(inventory):
    elapsed_time = inventory.extract_times()
    assert elapsed_time[0] == 0
    assert int(elapsed_time[-1]) == 0.631152e8


# noinspection PyTypeChecker
def test_iterate_time_step_gamma(
    one_cell: Inventory,
    one_cell_time_step7_gamma: list[tuple[int, float]],
) -> None:
    """Check gamma spectrum from the last time step in the one-cell JSON."""
    actual = [(r[1], r[2]) for r in one_cell.iterate_time_step_gamma() if r[0] == 7]
    assert np.array_equal(actual, one_cell_time_step7_gamma)
    gamma_spectrum = one_cell[-1].gamma_spectrum
    assert np.array_equal([r[1] for r in actual], gamma_spectrum.values)


if __name__ == "__main__":
    pytest.main()
