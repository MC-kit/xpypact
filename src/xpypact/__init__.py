"""The `xpypact` package.

Wraps FISPACT workflow. Transforms FISPACT output to xarray datasets.
"""
from __future__ import annotations

from importlib import metadata as _meta
from importlib.metadata import PackageNotFoundError, version

from .data_arrays import from_json, scale_by_flux, scale_by_mass
from .data_frames import (
    get_gamma,
    get_nuclides,
    get_run_data,
    get_time_steps,
    get_timestep_nuclides,
)

try:
    __version__ = version(__name__)
except PackageNotFoundError:  # pragma: no cover
    __version__ = "unknown"

__distribution__ = _meta.distribution(__name__)
__meta_data__ = __distribution__.metadata
__author__ = __meta_data__["Author"]
__author_email__ = __meta_data__["Author-email"]
__license__ = __meta_data__["License"]
__summary__ = __meta_data__["Summary"]
__copyright__ = f"Copyright 2021 {__author__}"

__all__ = [
    "__version__",
    "__distribution__",
    "__meta_data__",
    "__author__",
    "__author_email__",
    "__license__",
    "__summary__",
    "__copyright__",
    "get_run_data",
    "get_gamma",
    "get_nuclides",
    "get_timestep_nuclides",
    "get_time_steps",
    "from_json",
    "scale_by_flux",
    "scale_by_mass",
]
