"""DAO implementation for DuckDB."""
from __future__ import annotations

from .implementation import DuckDBDAO, create_indices

__all__ = [
    "DuckDBDAO",
    "create_indices",
]
