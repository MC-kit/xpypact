"""Output utilities."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import sys

if TYPE_CHECKING:
    from collections.abc import Iterable

    from _typeshed import SupportsWrite


def print_cols(
    seq: Iterable[Any],
    fid: SupportsWrite[str] = sys.stdout,
    max_columns: int = 6,
    fmt: str = "{}",
) -> int:
    """Print sequence in columns.

    Args:
        seq: sequence to print
        fid: output
        max_columns: max columns in a line
        fmt: format string

    Returns:
        int: the number of the last column printed on the last row
    """
    i = 0
    for s in seq:
        if i > 0:
            print(" ", file=fid, end="")
        print(fmt.format(s), file=fid, end="")
        i += 1
        if i >= max_columns:
            print(file=fid)
            i = 0

    return i
