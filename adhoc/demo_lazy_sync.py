"""Demonstrate polars LazyFrame capability stream large data to a parquet file.

Instead of collecting large amount of data in regular pl.DataFrame,
let's use LazyFrame with generators as initializers in
the LazyFrame constructor 'data' parameter and sync_parquet().
"""
from __future__ import annotations

import polars as pl


LENGTH = 900_000_000


def gen():
    for i in range(LENGTH):
        if i % 100_000 == 1:
            print(f"{i=}")
        yield i


def main():
    lz = pl.LazyFrame(
        {
            "x": range(LENGTH),
            "y": gen()
        },
        schema = {
            "x": pl.Int64,
            "y": pl.Int64,
        }
    )
    path = "demo_lazy_sync.parquet"
    lz.sink_parquet(path, row_group_size=100_000, lazy=False)
    # lz_actual = pl.scan_parquet(path,)
    # actual = lz_actual.collect()
    # assert actual.shape[0] == LENGTH
    # print(actual.head())
    # print(actual.describe())


if __name__ == '__main__':
    main()