"""Utility types for the package."""

from __future__ import annotations

from typing import Any

import os

import numpy as np

from numpy.typing import NDArray

MayBePath = str | os.PathLike[Any] | None
NDArrayFloat = NDArray[np.float64]
NDArrayInt = NDArray[np.int_]
