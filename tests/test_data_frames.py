from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
import pytest
import xpypact.data_arrays as da

from xpypact.data_frames import (
    get_gamma,
    get_nuclides,
    get_run_data,
    get_time_steps,
    get_timestep_nuclides,
)
from xpypact.inventory import from_json

if TYPE_CHECKING:
    import xarray as xr

    from xpypact.inventory import Inventory


@pytest.fixture(scope="module")
def inventory(data) -> Inventory:
    return from_json(data / "Ag-1.json")


@pytest.fixture(scope="module")
def ds(inventory: Inventory) -> xr.Dataset:
    return da.create_dataset(inventory)


def test_get_run_data(ds):
    run_data = get_run_data(ds)
    assert isinstance(run_data, pd.DataFrame)
    assert run_data["timestamp"].item() == pd.Timestamp("2020-07-12 23:01:19")
    assert run_data["run_name"].item() == "* Material Ag, fluxes 1"


def test_get_time_steps(ds):
    time_steps = get_time_steps(ds)
    assert isinstance(time_steps, pd.DataFrame)
    assert "time_step_number" in time_steps.columns
    assert time_steps.iloc[1].flux == 0.24452e11
    assert time_steps.iloc[1].heat == 1.103016503439716e-06, "Should have column 'heat'"


def test_get_nuclides(ds):
    nuclides = get_nuclides(ds)
    nuclides = nuclides.set_index(["element", "mass_number", "state"])
    assert nuclides.loc[("H", 1, "")].half_life == 0.0


def test_get_timestep_nuclides(ds):
    timestep_nuclides = get_timestep_nuclides(ds)
    timestep_nuclides = timestep_nuclides.set_index(
        ["time_step_number", "element", "mass_number", "state"],
    )
    assert timestep_nuclides.loc[2, "H", 1, ""].grams == 0.11468776343339874e-10


def test_get_gamma_with_dataset_without_gamma(ds):
    gamma = get_gamma(ds)
    assert gamma is None


def test_get_gamma_with_dataset_with_gamma(dataset_with_gamma):
    gamma = get_gamma(dataset_with_gamma)
    gamma = gamma.set_index(["time_step_number", "boundary"])
    assert gamma.loc[2, 1.0].rate == pytest.approx(17857.24443195)


if __name__ == "__main__":
    pytest.main()
