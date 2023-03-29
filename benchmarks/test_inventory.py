"""Tests with benchmarks on large FISPACT JSON files.

See https://pytest-benchmark.readthedocs.io/en/latest/index.html
"""
from __future__ import annotations

import bz2

from xpypact.inventory import Inventory, from_json
from xpypact.utils.resource import path_resolver

EXPECTED_TIME_STEPS = 65

data_path_resolver = path_resolver("benchmarks")

with bz2.open(data_path_resolver("data/Ag-1.json.bz2")) as fid:
    AG_1_TEXT = fid.read().decode("utf-8")


def test_load_from_string(benchmark) -> None:
    """Loading from string."""
    inventory: Inventory = benchmark(from_json, AG_1_TEXT)
    assert len(inventory.inventory_data) == EXPECTED_TIME_STEPS
