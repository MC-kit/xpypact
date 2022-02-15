"""FISPACT run title data class."""
from typing import Dict

from dataclasses import dataclass


@dataclass
class RunData:
    """FISPACT run title data."""

    timestamp: str
    run_name: str
    flux_name: str

    @classmethod
    def from_json(cls, json_dict: Dict) -> "RunData":
        """Retrieve data from JSON."""
        return cls(**json_dict)
