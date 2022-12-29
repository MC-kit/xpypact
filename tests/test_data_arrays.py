from __future__ import annotations

from typing import cast

import bz2

from copy import deepcopy

import numpy as np
import pandas as pd
import pytest
import xarray as xr
import xpypact.data_arrays as da

from numpy.testing import assert_array_equal, assert_equal
from pytest import approx
from xpypact.Inventory import Inventory, from_json


@pytest.fixture(scope="module")
def inventory(data) -> Inventory:
    return cast(Inventory, from_json(data / "Ag-1.json"))


@pytest.fixture(scope="module")
def ds(inventory: Inventory) -> xr.Dataset:
    return da.create_dataset(inventory)


def test_dataset_attributes(ds: xr.Dataset) -> None:
    assert ds.attrs["run_name"] == "* Material Ag, fluxes 1"
    assert ds.attrs["flux_name"] == "55.F9.10 11-L2-02W HFS_GLRY_08_U"
    assert ds.attrs["dose_rate_type"] == "Point source"
    assert ds.attrs["dose_rate_distance"] == 1.0


def test_first_time_step(ds):
    ts = ds.sel(time_step_number=1)
    ts = ts.isel(nuclide=(ts.nuclide_atoms > 0.0))
    assert ts.nuclide.size == 2, "Only Ag107 and Ag109 present before irradiation"
    assert ts.nuclide_grams.units == "g"
    assert ts.nuclide_grams.sum().item() == approx(1.0, rel=1e-3), "Expected 1g mass"
    assert ts.total_mass.units == "kg"
    assert ts.total_mass == approx(1e-3, rel=1e-3), "Total mass is to be about 0.001kg"


def test_elapsed_time(ds: xr.Dataset, inventory: Inventory) -> None:
    assert_array_equal(ds.elapsed_time, inventory.extract_times())


def test_extract_dose(ds: xr.Dataset) -> None:
    doses = ds.total_dose_rate
    assert doses.size == 2
    assert_array_equal(doses.to_numpy(), [0.0, 0.91720603767081113e-4])


def test_access_by_z(ds: xr.Dataset) -> None:
    ds = deepcopy(ds)
    da.add_atomic_number_coordinate(ds, "z")
    z = ds.z
    assert len(z) == 48
    argentum = ds.sel(nuclide=(ds.z == 47))
    assert (
        argentum.element is not None
    ), "The above selection preserves 'element' in nuclide index"
    assert argentum.nuclide.size == 14
    argentum2 = ds.sel(element="Ag")
    assert argentum2.nuclide.size == 14
    assert argentum2.element.item() == "Ag", " the element coordinate is preserved"


def test_atomic_masses_column(ds: xr.Dataset) -> None:
    ds = deepcopy(ds)
    da.add_atomic_number_coordinate(ds, "z")
    da.add_atomic_masses(ds, "a")
    a = ds.a
    assert len(a) == 48
    argentum = ds.sel(nuclide=("Ag", 111, ""))
    assert argentum.element == "Ag"
    a = argentum.a
    assert a.item() == approx(
        110.905, rel=1e-4
    ), " the Ag atomic mass is to be about 110.905"


def test_scale_by_mass(ds):
    actual = da.scale_by_mass(ds, 2)
    actual_atoms, expected_atoms = (
        x.nuclide_atoms.sel(element="Ag", mass_number=107, state="") for x in [actual, ds]
    )
    assert actual_atoms.size == 2
    assert expected_atoms.size == 2
    assert_array_equal(actual_atoms, expected_atoms * 2)


def test_time_stamp(ds):
    assert ds.timestamp[0] == pd.Timestamp("23:01:19 12 July 2020")
    actual = da.get_timestamp(ds)
    assert actual.year == 2020
    assert actual.month == 7
    assert actual.day == 12


def test_scale_by_flux(ds):
    actual = da.scale_by_flux(deepcopy(ds), 2)
    actual_grams, expected_grams = (
        x.nuclide_grams.sel(element="Ag", mass_number=111, state="") for x in [actual, ds]
    )
    assert actual_grams.size == 2
    assert expected_grams.size == 2
    assert_array_equal(
        actual_grams,
        expected_grams * 2.0,
        err_msg="Column 'nuclide_grams' should be scaled.",
    )


def test_scale_by_flux2(ds):
    actual = da.scale_by_flux(ds, 2)
    actual_values, expected_values = (x.total_heat for x in [actual, ds])
    assert actual_values.size == 2
    assert expected_values.size == 2
    assert (
        actual_values[0] == expected_values[0]
    ), "Initial value  shouldn't change on scaling by flux"
    actual_diff = np.diff(actual_values).item()
    expected_diff = 2 * np.diff(expected_values).item()
    assert_equal(
        actual_diff,
        expected_diff,
        "The difference with initial value should be scaled by flux",
    )


def test_scale_by_flux_on_dose_rate(ds):
    actual = da.scale_by_flux(ds, 0.5)
    actual_values, expected_values = (x.total_dose_rate for x in [actual, ds])
    assert actual_values.size == 2
    assert expected_values.size == 2
    assert (
        actual_values[0] == expected_values[0]
    ), "Initial value  shouldn't change on scaling by flux"
    actual_diff = np.diff(actual_values).item()
    expected_diff = 0.5 * np.diff(expected_values).item()
    assert_equal(
        actual_diff,
        expected_diff,
        "The difference with initial value should be scaled by flux",
    )
    assert_equal(actual.attrs, ds.attrs)


def test_net_cdf_writing(cd_tmpdir, ds):
    assert ds.total_dose_rate is not None
    da.save_nc(ds, "Ag-1.nc")
    actual = da.load_nc("Ag-1.nc")
    assert_equal(actual, ds, "The loaded dataset should be equal to thw written one.")
    assert np.array_equal(actual.total_dose_rate, ds.total_dose_rate)
    assert actual.total_dose_rate.attrs["units"] == "Sv/h"


def test_net_cdf_writing_with_group_and_appending(cd_tmpdir, ds):
    group = "some-group"
    assert ds.total_dose_rate is not None
    da.save_nc(ds, "Ag-1.nc", mode="a", group=group)
    actual = da.load_nc("Ag-1.nc", group=group)
    assert_equal(actual, ds, "The loaded dataset should be equal to thw written one.")
    assert np.array_equal(actual.total_dose_rate, ds.total_dose_rate)
    assert actual.total_dose_rate.attrs["units"] == "Sv/h"


def test_inventory_with_gamma(data):
    with bz2.open(data / "with-gamma.json.bz2") as fid:
        ds = da.from_json(fid.read().decode("utf-8"))
        assert ds.gamma is not None
        assert ds.gamma.sel(time_step_number=2, gamma_boundaries=1.0).item() == approx(
            17857.24443195
        )


if __name__ == "__main__":
    pytest.main()
