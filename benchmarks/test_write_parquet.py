"""Tests with benchmarks on large FISPACT JSON files.

See https://pytest-benchmark.readthedocs.io/en/latest/index.html
"""
from __future__ import annotations

import bz2

from pathlib import Path

import pytest

from xpypact.dao.duckdb import write_parquet
from xpypact.data_arrays import from_json

DATA = Path(__file__).parent / "data"

with bz2.open(DATA / "inventory_1.json.bz2") as fid:
    INV_1_TEXT = fid.read().decode("utf-8")


@pytest.mark.skip(reason="Use for profiling")
def test_profile_write_parquet(tmp_path) -> None:
    """Profile load write operations."""
    dataset = from_json(INV_1_TEXT)
    write_parquet(tmp_path, dataset, 1, 1)


def test_load_dataset(benchmark) -> None:
    """Benchmark creation of the dataset from JSON."""
    benchmark(from_json, INV_1_TEXT)


def test_write_parquet(benchmark, tmp_path) -> None:
    """Benchmark writing to parquet."""
    dataset = from_json(INV_1_TEXT)
    benchmark.pedantic(write_parquet, args=(tmp_path, dataset, 1, 1))
