"""Export xpypact dataset to pandas dataframes.

This can be used to store the data in databases or parquet datasets.
"""
from __future__ import annotations

import pandas as pd

from xarray import Dataset


def get_run_data(ds: Dataset) -> pd.DataFrame:
    """Extract FISPACT run data as dataframe.

    Args:
        ds: xpypact dataset loaded from FISPACT output JSON file

    Returns:
        pd.DataFrame: one row table with the run data
    """
    return pd.DataFrame.from_dict(
        {
            "timestamp": ds.timestamp,
            "run_name": ds.attrs["run_name"],
            "flux_name": ds.attrs["flux_name"],
            "dose_rate_type": ds.attrs["dose_rate_type"],
            "dose_rate_distance": ds.attrs["dose_rate_distance"],
        }
    )


def get_time_steps(ds: Dataset) -> pd.DataFrame:
    """Extract time steps table from xpypact dataset.

    Drops "total_" prefix from column names.

    Args:
        ds: xpypact dataset to extract from

    Returns:
        Time step totals table.
    """
    return (
        ds[
            [
                "time_step_number",
                "elapsed_time",
                "irradiation_time",
                "cooling_time",
                "duration",
                "flux",
                "total_atoms",
                "total_activity",
                "total_alpha_activity",
                "total_beta_activity",
                "total_gamma_activity",
                "total_mass",
                "total_heat",
                "total_alpha_heat",
                "total_beta_heat",
                "total_gamma_heat",
                "total_ingest1ion_dose",
                "total_inhalation_dose",
                "total_dose_rate",
            ]
        ]
        .to_dataframe()
        .reset_index()
        .rename(
            columns={
                "total_atoms": "atoms",
                "total_activity": "activity",
                "total_alpha_activity": "alpha_activity",
                "total_beta_activity": "beta_activity",
                "total_gamma_activity": "gamma_activity",
                "total_mass": "mass",
                "total_heat": "heat",
                "total_alpha_heat": "alpha_heat",
                "total_beta_heat": "beta_heat",
                "total_gamma_heat": "gamma_heat",
                "total_ingest1ion_dose": "ingest1ion_dose",
                "total_inhalation_dose": "inhalation_dose",
                "total_dose_rate": "dose_rate",
            }
        )
    )


def get_nuclides(ds: Dataset) -> pd.DataFrame:
    """Extract nuclides table from xpypact dataset.

    Sets 'half_life' column name instead 'nuclide_half_life'.

    Args:
        ds: xpypact dataset to extract from

    Returns:
        Nuclide table
    """
    columns_all = ["element", "mass_number", "state", "zai", "nuclide_half_life"]
    columns = ["zai", "nuclide_half_life"]
    return (
        ds[columns]
        .to_dataframe()[columns]
        .reset_index()[columns_all]
        .rename(columns={"nuclide_half_life": "half_life"})
    )


def get_timestep_nuclides(ds: Dataset) -> pd.DataFrame:
    """Extract timestep X nuclides table from xpypact dataset.

    Removes "nuclide_" prefix from column names.

    Args:
        ds: xpypact dataset to extract from

    Returns:
        Time step x Nuclide table
    """
    columns = [
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
        "nuclide_dose",
        "nuclide_ingestion",
        "nuclide_inhalation",
    ]
    columns_all = [
        "time_step_number",
        "element",
        "mass_number",
        "state",
    ] + columns

    return (
        ds[columns]
        .to_dataframe()[columns]
        .reset_index()[columns_all]
        .fillna(0.0)
        .rename(
            columns={
                "nuclide_atoms": "atoms",
                "nuclide_grams": "grams",
                "nuclide_activity": "activity",
                "nuclide_alpha_activity": "alpha_activity",
                "nuclide_beta_activity": "beta_activity",
                "nuclide_gamma_activity": "gamma_activity",
                "nuclide_heat": "heat",
                "nuclide_alpha_heat": "alpha_heat",
                "nuclide_beta_heat": "beta_heat",
                "nuclide_gamma_heat": "gamma_heat",
                "nuclide_dose": "dose",
                "nuclide_ingestion": "ingestion",
                "nuclide_inhalation": "inhalation",
            }
        )
    )


def get_gamma(ds: Dataset) -> pd.DataFrame | None:
    """Extract timestep X gamma table from xpypact dataset.

    Set column names to "boundary" and "rate".

    Args:
        ds: xpypact dataset to extract from

    Returns:
        Time step x gamma rate table
    """
    if "gamma" not in ds:
        return None
    columns = [
        "time_step_number",
        "gamma_boundaries",
        "gamma",
    ]
    return (
        ds.gamma.to_dataframe()
        .reset_index()[columns]
        .rename(columns={"gamma_boundaries": "boundary", "gamma": "rate"})
    )
