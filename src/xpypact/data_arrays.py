"""Methods to convert Inventory to xarray DataSet.

The dimensions  ot the nodes are:
    time_step_number
    nuclide (symbol, atomic_number, state)
    gamma_boundaries if available

The dataset facilitates FISPACT output slicing and aggregation.

Also, the module provides methods to scale activation properties in
activation and time_steps nodes are provided, both for mass scaling
(all the values proportional to mass are multiplied by scaling factor)
and for flux scaling:
the difference with initial state is scaled with a given factor.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, TextIO, Tuple, Union, cast

from functools import reduce
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

from mckit_nuclides.elements import ELEMENTS_TABLE
from mckit_nuclides.nuclides import NUCLIDES_TABLE
from numpy.typing import ArrayLike
from xarray.core.accessor_dt import DatetimeAccessor
from xpypact.Inventory import Inventory
from xpypact.Inventory import from_json as inventory_from_json
from xpypact.TimeStep import TimeStep
from xpypact.utils.types import MayBePath

if TYPE_CHECKING:  # pragma: no cover
    try:
        from dask.delayed import Delayed
    except ImportError:
        Delayed = None
    try:
        from dask.dataframe import DataFrame as DaskDataFrame
    except ImportError:
        DaskDataFrame = None

SCALABLE_COLUMNS = [
    "total_mass",
    "total_heat",
    "total_dose_rate",
    "total_alpha_heat",
    "total_beta_heat",
    "total_gamma_heat",
    "total_activity",
    "total_alpha_activity",
    "total_beta_activity",
    "total_gamma_activity",
    "nuclide_atoms",
    "nuclide_grams",
    "nuclide_activity",
    "nuclide_alpha_activity",
    "nuclide_beta_activity",
    "nuclide_gamma_activity",
    "nuclide_heat",
    "nuclide_alpha_heat",
    "nuclide_beta_heat",
    "nuclide_gamma_heat",
    "gamma",
]


def _make_var(value, dtype=float) -> Tuple[str, ArrayLike]:
    value = np.array([value], dtype=dtype)
    return "time_step_number", value


def _make_nuclide_var(getter, nuclides, dtype=float) -> Tuple[str, ArrayLike]:
    return "nuclide", np.fromiter(map(getter, nuclides), dtype=dtype)


def _make_time_step_and_nuclide_var(
    getter, nuclides, dtype=float
) -> Tuple[Tuple[str, str], ArrayLike]:
    _data = np.fromiter(map(getter, nuclides), dtype=dtype)
    return ("time_step_number", "nuclide"), _data.reshape(1, _data.size)


def _add_time_step_record(_ds: xr.Dataset, ts: TimeStep) -> xr.Dataset:

    data_vars = {
        "irradiation_time": _make_var(ts.irradiation_time),
        "cooling_time": _make_var(ts.cooling_time),
        "duration": _make_var(ts.duration),
        "flux": _make_var(ts.flux),
        "total_atoms": _make_var(ts.total_atoms),
        "total_activity": _make_var(ts.total_activity),
        "total_alpha_activity": _make_var(ts.alpha_activity),
        "total_beta_activity": _make_var(ts.beta_activity),
        "total_gamma_activity": _make_var(ts.gamma_activity),
        "total_mass": _make_var(ts.total_mass),
        "total_heat": _make_var(ts.total_heat),
        "total_alpha_heat": _make_var(ts.alpha_heat),
        "total_beta_heat": _make_var(ts.beta_heat),
        "total_gamma_heat": _make_var(ts.gamma_heat),
        "total_ingest1ion_dose": _make_var(ts.ingestion_dose),
        "total_inhalation_dose": _make_var(ts.inhalation_dose),
        "total_dose_rate": _make_var(ts.dose_rate.dose),
        "nuclide_half_life": _make_nuclide_var(lambda n: n.half_life, ts.nuclides),
        "nuclide_atoms": _make_time_step_and_nuclide_var(lambda n: n.atoms, ts.nuclides),
        "nuclide_grams": _make_time_step_and_nuclide_var(lambda n: n.grams, ts.nuclides),
        "nuclide_activity": _make_time_step_and_nuclide_var(
            lambda n: n.activity, ts.nuclides
        ),
        "nuclide_alpha_activity": _make_time_step_and_nuclide_var(
            lambda n: n.alpha_activity, ts.nuclides
        ),
        "nuclide_beta_activity": _make_time_step_and_nuclide_var(
            lambda n: n.beta_activity, ts.nuclides
        ),
        "nuclide_gamma_activity": _make_time_step_and_nuclide_var(
            lambda n: n.gamma_activity, ts.nuclides
        ),
        "nuclide_heat": _make_time_step_and_nuclide_var(lambda n: n.heat, ts.nuclides),
        "nuclide_alpha_heat": _make_time_step_and_nuclide_var(
            lambda n: n.alpha_heat, ts.nuclides
        ),
        "nuclide_beta_heat": _make_time_step_and_nuclide_var(
            lambda n: n.beta_heat, ts.nuclides
        ),
        "nuclide_gamma_heat": _make_time_step_and_nuclide_var(
            lambda n: n.gamma_heat, ts.nuclides
        ),
        "nuclide_dose": _make_time_step_and_nuclide_var(lambda n: n.dose, ts.nuclides),
        "nuclide_ingestion": _make_time_step_and_nuclide_var(
            lambda n: n.ingestion, ts.nuclides
        ),
        "nuclide_inhalation": _make_time_step_and_nuclide_var(
            lambda n: n.inhalation, ts.nuclides
        ),
    }

    nuclide_coordinate = pd.MultiIndex.from_tuples(
        ((n.element, n.a, n.state) for n in ts.nuclides),
        names=["element", "mass_number", "state"],
    )

    coords = {
        "time_step_number": _make_var(ts.number, dtype=int),
        "elapsed_time": _make_var(ts.elapsed_time),
        "nuclide": nuclide_coordinate,
        "zai": _make_nuclide_var(lambda n: n.zai, ts.nuclides, dtype=int),
    }

    if ts.gamma_spectrum is not None:
        gamma_boundaries = ts.gamma_spectrum.boundaries
        gamma_value = ts.gamma_spectrum.intensities
        gamma_value = np.insert(gamma_value, 0, 0.0).reshape(1, len(gamma_boundaries))
        data_vars["gamma"] = (("time_step_number", "gamma_boundaries"), gamma_value)
        coords["gamma_boundaries"] = gamma_boundaries

    tsr = xr.Dataset(data_vars=data_vars, coords=coords)
    _ds = xr.merge([_ds, tsr])
    return _ds


def create_dataset(inventory: Inventory) -> xr.Dataset:
    """Convert Inventory to a Dataset.

    Args:
        inventory: source

    Returns:
        Dataset: the representation of a FISPACT inventory.
    """
    ds = reduce(_add_time_step_record, iter(inventory), xr.Dataset())
    meta_info = inventory.meta_info
    ds.coords["timestamp"] = pd.date_range(meta_info.timestamp, periods=1)
    ds.coords["timestamp"].attrs[
        "long description"
    ] = "FISPACT datasets can be merged and then selected by timestamp as coordinate"
    ds.attrs["run_name"] = meta_info.run_name
    ds.attrs["flux_name"] = meta_info.flux_name
    ds.attrs["dose_rate_type"] = meta_info.dose_rate_type
    ds.attrs["dose_rate_distance"] = meta_info.dose_rate_distance
    ds.irradiation_time.attrs["units"] = "s"
    ds.cooling_time.attrs["units"] = "s"
    ds.duration.attrs["units"] = "s"
    ds.elapsed_time.attrs["units"] = "s"
    ds.flux.attrs["units"] = "n/cm^2/s"

    if "gamma_boundaries" in ds.coords:
        ds.coords["gamma_boundaries"].attrs["units"] = "MeV"

    ds.total_activity.attrs["units"] = "Bq"
    ds.total_alpha_activity.attrs["units"] = "Bq"
    ds.total_beta_activity.attrs["units"] = "Bq"
    ds.total_gamma_activity.attrs["units"] = "Bq"

    ds.total_mass.attrs["units"] = "kg"

    ds.total_heat.attrs["units"] = "kW"
    ds.total_alpha_heat.attrs["units"] = "kW"
    ds.total_beta_heat.attrs["units"] = "kW"
    ds.total_gamma_heat.attrs["units"] = "kW"

    ds.total_ingest1ion_dose.attrs["units"] = "Sv"
    ds.total_inhalation_dose.attrs["units"] = "Sv"
    ds.total_dose_rate.attrs["units"] = "Sv/h"

    ds.nuclide_half_life.attrs["units"] = "s"
    ds.nuclide_grams.attrs["units"] = "g"

    ds.nuclide_activity.attrs["units"] = "Bq"
    ds.nuclide_alpha_activity.attrs["units"] = "Bq"
    ds.nuclide_beta_activity.attrs["units"] = "Bq"
    ds.nuclide_gamma_activity.attrs["units"] = "Bq"

    ds.nuclide_heat.attrs["units"] = "kW"
    ds.nuclide_alpha_heat.attrs["units"] = "kW"
    ds.nuclide_beta_heat.attrs["units"] = "kW"
    ds.nuclide_gamma_heat.attrs["units"] = "kW"

    ds.nuclide_dose.attrs["units"] = "Sv"
    ds.nuclide_ingestion.attrs["units"] = "Sv"
    ds.nuclide_inhalation.attrs["units"] = "Sv"

    # Restore int type for 'zai', which became float after the merges.
    # see https://xarray.pydata.org/en/stable/user-guide/combining.html
    # ...  due  to the underlying representation of
    # missing values as floating point numbers(NaN),
    # variable  data type is not always  preserved when merging in this manner.
    zais = np.asarray(ds.coords["zai"].to_numpy().flatten(), dtype=int)
    ds.coords["zai"] = ("nuclide", zais)

    return ds


def get_timestamp(ds: xr.Dataset) -> DatetimeAccessor:
    """Get access to time stamp value.

    Args:
        ds: Dataset with FISPACT results

    Returns:
        accessor to timestamp
    """
    return cast(DatetimeAccessor, ds.timestamp[0].dt)


def get_atomic_numbers(ds: xr.Dataset) -> ArrayLike:
    """Create column of atomic numbers (Z) corresponding to `nuclide` coordinate.

    Args:
        ds: dataset with the 'nuclides' coordinate

    Returns:
        Z values for the nuclides

    """
    elements = ds.nuclide.element.to_numpy()
    return cast(
        ArrayLike,
        ELEMENTS_TABLE.loc[elements, ["atomic_number"]].to_numpy().flatten(),
    )


def add_atomic_number_coordinate(
    ds: xr.Dataset, coordinate_name: str = "atomic_number"
) -> xr.Dataset:
    """Add coordinate for Z.

    This allows indexing of a dataset with integer Z (atomic number) values.

    Args:
        ds: FISPACT results dataset
        coordinate_name: name for the coordinate attribute

    Returns:
        The 'ds' with the added coordinate.
    """
    ds.coords[coordinate_name] = ("nuclide", get_atomic_numbers(ds))
    return ds


def get_atomic_masses(ds: xr.Dataset) -> ArrayLike:
    """Create column with relative atomic masses along nuclide coordinate.

    Args:
        ds: FISPACT output as Dataset

    Returns:
        Vector with relative atomic masses.
    """
    atomic_numbers = get_atomic_numbers(ds)
    mass_numbers = ds.nuclide.mass_number.to_numpy().flatten()
    mi = pd.MultiIndex.from_arrays([atomic_numbers, mass_numbers])
    return cast(ArrayLike, NUCLIDES_TABLE.loc[mi, ["nuclide_mass"]].to_numpy().flatten())


def add_atomic_masses(ds: xr.Dataset, variable_name="atomic_mass") -> xr.Dataset:
    """Add variable with relative atomic masses with 'nuclide' coordinate.

    Useful for bulk operations based on coordinates.

    Args:
        ds: FISPACT output as Dataset
        variable_name: the data array name to use

    Returns:
        The 'ds' with added data array with relative atomic masses.
    """
    ds[variable_name] = ("nuclide", get_atomic_masses(ds))
    return ds


def scale_by_flux(ds: xr.Dataset, scale: float) -> xr.Dataset:
    """Normalize irradiation results for actual flux value.

    A FISPACT irradiation scenario may use flux value,
    which is different from actual one.
    This is the case for standard ITER SA2 scenario.
    For flux, only difference with initial activation properties is to be scaled.

    Notes:
        Assuming that the first time step presents initial values.

    Args:
        ds: FISPACT output as Dataset
        scale: factor to apply

    Returns:
        A new dataset with scaled columns.
    """
    initial = ds.sel(time_step_number=1).fillna(0.0)
    result = scale_by_mass(ds - initial, scale) + initial
    result.attrs.update(ds.attrs)

    return result


def scale_by_mass(ds: xr.Dataset, scale: float) -> xr.Dataset:
    """All the activation values including initial can be scaled to actual weight.

    Args:
        ds: FISPACT output as Dataset
        scale: factor to apply

    Returns:
        A new dataset with scaled columns.

    """
    columns = [x for x in SCALABLE_COLUMNS if x in ds.variables]
    scaled = ds.merge(ds[columns] * scale, overwrite_vars=columns, join="exact")
    return scaled


def _encode_multiindex(ds: xr.Dataset, idx_name: str) -> xr.Dataset:
    coordinate = ds.indexes[idx_name].to_frame().reset_index(drop=True)
    columns = coordinate.columns.to_numpy()
    columns_to_drop = (idx_name, *columns)
    encoded = ds.reset_index(columns_to_drop, drop=True)
    encoded.attrs[idx_name + "_columns"] = columns
    for c in columns:
        encoded.attrs[idx_name + f"_{c}"] = coordinate[c].to_numpy().flatten()
    return encoded


def _decode_to_multiindex(encoded: xr.Dataset, idx_name: str) -> xr.Dataset:
    columns = encoded.attrs.pop(idx_name + "_columns")
    mi_columns = [encoded.attrs.pop(idx_name + f"_{c}") for c in columns]
    mi = pd.MultiIndex.from_arrays(mi_columns, names=columns)
    encoded.coords[idx_name] = mi
    return encoded


# noinspection PyShadowingBuiltins
def save_nc(
    ds: xr.Dataset,
    path: MayBePath = None,
    **kwargs: Any,
) -> bytes | Delayed | None:
    """Save a dataset with nuclide index.

    Encodes MultiIndex instance in a dataset and saves the result.
    See: https://github.com/pydata/xarray/issues/1077

    Args:
        ds: FISPACT output as dataset
        path: see :meth:`xarray.Dataset.to_netcdf()`
        kwargs: ...

    Returns:
        see :meth:`xarray.Dataset.to_netcdf()`
    """
    encoded = _encode_multiindex(ds, "nuclide")
    engine = kwargs.pop("engine", "h5netcdf")
    return encoded.to_netcdf(path, engine=engine, **kwargs)


def load_nc(
    cn: str | Path,
    engine: Literal["netcdf4", "scipy", "h5netcdf"] | None = "h5netcdf",
    **kwargs: Any,
) -> xr.Dataset:
    """Load a dataset from a netcdf file.

    The nuclide index is restored.

    Args:
        cn: path to dataset file
        engine: netcdf engine to use
        kwargs: arguments for :meth:`xarray.Dataset.load_dataset()`

    Returns:
        The loaded dataset
    """
    encoded = xr.load_dataset(cn, engine=engine, **kwargs)
    ds = _decode_to_multiindex(encoded, "nuclide")
    return ds


def from_json(path: Union[str, Path, TextIO]) -> xr.Dataset:
    """Load Dataset from the FISPACT JSON output.

    Args:
        path: path to JSON file, stream or text with FISPACT JSON output.

    Returns:
        The loaded Dataset.
    """
    return create_dataset(inventory_from_json(path))
