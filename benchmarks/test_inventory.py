"""Tests with benchmarks on large FISPACT JSON files.

See https://pytest-benchmark.readthedocs.io/en/latest/index.html
"""
from __future__ import annotations

from typing import TYPE_CHECKING, cast

import bz2

from pathlib import Path

from xpypact.inventory import from_json
from xpypact.utils.resource import path_resolver

if TYPE_CHECKING:
    from xpypact.inventory import Inventory

EXPECTED_TIME_STEPS = 65

data_path_resolver = path_resolver("benchmarks")

with bz2.open(cast(Path, data_path_resolver("data/Ag-1.json.bz2"))) as fid:
    AG_1_TEXT = fid.read().decode("utf-8")


def test_load_from_string(benchmark) -> None:
    """Loading from string."""
    inventory: Inventory = benchmark(from_json, AG_1_TEXT)
    assert len(inventory.inventory_data) == EXPECTED_TIME_STEPS
