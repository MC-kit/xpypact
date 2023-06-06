"""DAO implementation for DuckDB."""
from __future__ import annotations

from .implementation import DuckDBDAO, compute_optimal_row_group_size, write_parquet

__all__ = ["DuckDBDAO", "compute_optimal_row_group_size", "write_parquet"]
