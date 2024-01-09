"""DAO implementation for DuckDB."""
from __future__ import annotations

from .implementation import (
    CommonDataCollector,
    DuckDBDAO,
    create_indices,
    load_parquets,
    write_parquets,
)

__all__ = [
    "CommonDataCollector",
    "DuckDBDAO",
    "create_indices",
    "load_parquets",
    "write_parquets",
]
