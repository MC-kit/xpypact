"""Demonstrate polars LazyFrame capability stream large data to a parquet file.

Instead of collecting large amount of data in regular pl.DataFrame,
let's use LazyFrame with generators as initializers in
the LazyFrame constructor 'data' parameter and sync_parquet().
"""

from __future__ import annotations

from pathlib import Path
from time import perf_counter

import polars as pl
import psutil as ps
import pyarrow.parquet as pq

LENGTH = 1_000_000_000
# LENGTH = 1_000

# from https://gist.github.com/borgstrom/936ca741e885a1438c374824efb038b3

TIME_DURATION_UNITS = (
    ("week", 60 * 60 * 24 * 7),
    ("day", 60 * 60 * 24),
    ("hour", 60 * 60),
    ("min", 60),
    ("sec", 1),
)


def human_time_duration(seconds):
    if seconds == 0:
        return "inf"
    parts = []
    for unit, div in TIME_DURATION_UNITS:
        amount, seconds = divmod(int(seconds), div)
        if amount > 0:
            parts.append("{} {}{}".format(amount, unit, "" if amount == 1 else "s"))
    return ", ".join(parts)


MEMORY_VOLUME_UNITS = (
    ("GB", 1024 * 1024 * 1024),
    ("MB", 1024 * 1024),
    ("kB", 1024),
    ("B", 1),
)


def human_mem_volume(memory_volume):
    if memory_volume <= 0:
        return memory_volume

    for unit, div in MEMORY_VOLUME_UNITS:
        amount, seconds = divmod(int(memory_volume), div)
        if amount > 0:
            return f"{amount:5d}{unit}"

    return memory_volume


def gen():
    initial_mem = min_avail = ps.virtual_memory().available
    for i in range(LENGTH):
        if i % 1_000_000 == 0:
            mem_available = ps.virtual_memory().available  # this is expensive operation
            min_avail = min(min_avail, mem_available)
            print(i, ":", human_mem_volume(min_avail))
        yield min_avail
    print("Max memory consumption: ", human_mem_volume(initial_mem - min_avail))


def main():
    start = perf_counter()
    path = Path("demo_lazy_sync.parquet")
    path.unlink(missing_ok=True)
    row_group_size = 1_000_000
    print("Params: row_group_size:", row_group_size)
    # noinspection PyTypeChecker
    lf = pl.LazyFrame(
        {
            "x": range(LENGTH),
            "y": gen(),
        },
        schema={
            "x": pl.Int64,
            "y": pl.Int64,
        },
    ).sink_parquet(path, row_group_size=row_group_size)
    # lz.sink_parquet(path, row_group_size=200_000, lazy=False)
    # lz_actual = pl.scan_parquet(path,)
    # actual = lz_actual.collect()
    # assert actual.shape[0] == LENGTH
    # print(actual.head())
    # print(actual.describe())
    end = perf_counter()
    print("Elapsed time:", human_time_duration(end - start))
    if path.exists():
        file_size = path.stat().st_size
        print("File size:", human_mem_volume(file_size))
    # parquet_file = pq.ParquetFile(path)
    # print("Metadata: ", str(parquet_file.metadata))
    # mem_stat = pl.scan_parquet(path).select("y").min().collect().item()
    # print("Min. mem: ", human_mem_volume(mem_stat))
    # tail = pl.scan_parquet(path).filter(pl.col("x") > LENGTH - 5).collect()
    # print(tail)


if __name__ == "__main__":
    main()

# rgs=200_000
# Max memory consumption:      7GB
# Elapsed time: 1 min, 49 secs

# Params: row_group_size: 1000000 , lazy: False, without tqdm
# Max memory consumption:      7GB
# Elapsed time: 1 min, 46 secs
# File size:   969MB

# Params: row_group_size: 1000000 , lazy: False, with tqdm
# Max memory consumption:      7GB
# Elapsed time: 3 mins, 44 secs
# File size:   969MB

# ..., without tqdm
# Max memory consumption:      7GB
# Elapsed time: 1 min, 46 secs
# File size:   969MB

# With tqdm the processing takes more than twice more time.
