"""Test loading Inventory from FISPACT JSON file."""
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
    # times = extract_times(inventory)
    # assert times.size == 2
    # assert times[0] == ts1.elapsed_time
    # assert times[1] == ts2.elapsed_time


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
    elapsed_time = [x.elapsed_time for x in inventory.inventory_data]
    assert elapsed_time[0] == 0
    assert int(elapsed_time[-1]) == 0.631152e8


# def test_inventory_as_array(inventory):
#     actual = inventory.as_array()
#     assert actual.shape == (2, 18)


# TODO dvp: move to benchmarks
# @pytest.fixture(scope="module")
# def big_inventory() -> Inventory:
#     with bz2.open(benchmark_data_path_resolver("data/Ag-1.json.bz2")) as fid:
#         return from_json(fid.read().decode("utf-8"))


# def test_big_inventory(big_inventory):
#     assert len(big_inventory.inventory_data) == 65
#     times_steps_with_nuclides = sum(
#         map(lambda x: 1 if x.nuclides else 0, big_inventory.inventory_data)
#     )
#     assert times_steps_with_nuclides == 65


if __name__ == "__main__":
    pytest.main()
