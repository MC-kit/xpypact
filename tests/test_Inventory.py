"""Test loading Inventory from FISPACT JSON file."""
import bz2

import pytest

from xpypact.Inventory import Inventory, RunDataCorrected, from_json  # extract_times,
from xpypact.RunData import RunData
from xpypact.TimeStep import DoseRate, TimeStep

SECONDS_PER_YEAR = int(365.24 * 24 * 3600)


@pytest.fixture(scope="module")
def inventory(data):
    return from_json(data / "Ag-1.json")


def test_constructors():
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


def test_inventory_with_gamma(data):
    with bz2.open(data / "with-gamma.json.bz2") as fid:
        inventory = from_json(fid.read().decode("utf-8"))
        assert inventory[1].gamma_spectrum is not None


if __name__ == "__main__":
    pytest.main()
