"""FISPACT run title data class."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class RunData:
    """FISPACT run title data."""

    timestamp: str
    run_name: str
    flux_name: str

    @classmethod
    def from_json(cls, json_dict: dict) -> "RunData":
        """Construct RunData instance from JSON.

        Args:
            json_dict: source dictionary

        Returns:
            The loaded instance of RunData
        """
        return cls(**json_dict)
