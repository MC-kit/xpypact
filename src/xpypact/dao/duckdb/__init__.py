"""DAO implementation for DuckDB."""
from __future__ import annotations

from .implementation import DuckDBDAO, write_parquet

__all__ = ["DuckDBDAO", "write_parquet"]
