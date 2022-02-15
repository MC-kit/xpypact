"""Output utilities."""

from typing import Any, Iterable, TextIO

import sys


def print_cols(
    seq: Iterable[Any], fid: TextIO = sys.stdout, max_columns: int = 6, fmt: str = "{}"
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
    column = 0
    for s in seq:
        print(fmt.format(s), file=fid, end="")
        column += 1
        if column == max_columns:
            print(file=fid)
            column = 0
        else:
            print(" ", file=fid, end="")
    return column
