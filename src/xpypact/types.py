"""Utility types for the package."""

from __future__ import annotations

from typing import Union

import os

import numpy as np

from numpy.typing import NDArray

MayBePath = Union[str, os.PathLike, None]
NDArrayFloat = NDArray[np.float64]
NDArrayInt = NDArray[np.int_]
