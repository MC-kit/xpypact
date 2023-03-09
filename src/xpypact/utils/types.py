"""Utility types for the package."""
from typing import Union

import os

import numpy as np

from numpy.typing import NDArray

MayBePath = Union[str, os.PathLike, None]

NDArrayFloat = NDArray[np.float_]
NDArrayInt = NDArray[np.int_]
