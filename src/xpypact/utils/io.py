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
    for i, s in enumerate(seq):
        print(fmt.format(s), file=fid, end="")
        if 0 < i and i % max_columns == 0:
            print(file=fid)
        else:
            print(" ", file=fid, end="")
    return i % max_columns
