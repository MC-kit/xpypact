"""Output utilities."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, TextIO

import sys

if TYPE_CHECKING:
    from collections.abc import Iterable


def print_cols(
    seq: Iterable[Any],
    fid: TextIO = sys.stdout,
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
        print(fmt.format(s), file=fid, end="")
        if (i > 0) and (i % max_columns == 0):
            print(file=fid)
        else:
            print(" ", file=fid, end="")
        i += 1
    return i % max_columns
