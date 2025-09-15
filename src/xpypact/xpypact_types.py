"""Utility types for the package."""

from __future__ import annotations  # pragma: no cover

from typing import TYPE_CHECKING  # pragma: no cover

if TYPE_CHECKING:
    from typing import Any

    import os

    import numpy as np

    from numpy.typing import NDArray

    MayBePath = str | os.PathLike[Any] | None
    NDArrayFloat = NDArray[np.floating]
    NDArrayInt = NDArray[np.int_]
