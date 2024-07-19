"""Utility types for the package."""

from __future__ import annotations

import os

import numpy as np

from numpy.typing import NDArray

MayBePath = str | os.PathLike | None
NDArrayFloat = NDArray[np.float64]
NDArrayInt = NDArray[np.int_]
