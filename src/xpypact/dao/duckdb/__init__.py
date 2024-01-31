"""DAO implementation for DuckDB."""

from __future__ import annotations

from .implementation import DuckDBDAO, create_indices, save

__all__ = [
    "DuckDBDAO",
    "create_indices",
    "save",
]
