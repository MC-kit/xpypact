"""TODO..."""

from __future__ import annotations

from pathlib import Path

import duckdb as db
import pandas as pd

ddf1 = pd.DataFrame({"material": [100, 101, 102], "g": [1, 2, 3], "a": [1, 2, 3], "b": [4, 5, 6]})
ddf2 = pd.DataFrame({"material": [100, 200, 200], "g": [4, 5, 6], "a": [1, 2, 3], "b": [4, 5, 6]})

con = db.connect(":memory:")
con.execute(
    """
    copy
    (select * from ddf1)
    to
    'wrk/test.parquet'
    (format parquet, partition_by (material, g), allow_overwrite 1)
    """
)
con.execute(
    """
    copy
    (select * from ddf2)
    to
    'wrk/test.parquet'
    (format parquet, partition_by (material, g), allow_overwrite 1)
    """,
)


# sql = (
#     "describe select * from read_parquet('wrk/test.parquet/*/*/*.parquet', hive_partitioning=true)"
# )
sql = "select * from read_parquet('wrk/test.parquet/*/*/*.parquet', hive_partitioning=true)"

dfin = con.execute(sql).df()


def main():
    """TODO..."""
    pass


if __name__ == "__main__":
    main()
