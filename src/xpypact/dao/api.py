"""Interface to data access facilities."""

from __future__ import annotations

from typing import TYPE_CHECKING

from abc import ABC, abstractmethod

if TYPE_CHECKING:
    import pandas as pd

    from xpypact.inventory import Inventory


class DataAccessInterface(ABC):
    """Abstract DAO for to save/load xpypact dataset.

    The subclasses implement methods to save/load xpypact dataset to/from
    database or filesystem.

    A caller is to provide connection or whatever resource to subclasses.
    """

    @abstractmethod
    def get_tables_info(self) -> pd.DataFrame:
        """Get information on tables in schema."""

    @abstractmethod
    def has_schema(self) -> bool:
        """Check if the schema is available in a database."""

    @abstractmethod
    def create_schema(self) -> None:
        """Create tables to store xpypact dataset.

        Retain existing tables.
        """

    @abstractmethod
    def drop_schema(self) -> None:
        """Drop our DB objects."""

    @abstractmethod
    def save(self, inventory: Inventory, material_id: int = 1, case_id: int = 1) -> None:
        """Save xpypact inventory to database.

        Args:
            inventory: what to save
            material_id: additional key to distinguish multiple FISPACT runs
            case_id: second additional key
        """

    @abstractmethod
    def on_save_complete(self) -> None:
        """Execute on saving all the innventories.

        Save information that is to be saved after multithreading processing.
        """

    @abstractmethod
    def load_rundata(self) -> pd.DataFrame:
        """Load FISPACT run data as table.

        Returns:
            FISPACT run data
        """

    @abstractmethod
    def load_nuclides(self) -> pd.DataFrame:
        """Load nuclide table.

        Returns:
            time nuclide
        """

    @abstractmethod
    def load_time_steps(self) -> pd.DataFrame:
        """Load time step table.

        Returns:
            time step table
        """

    @abstractmethod
    def load_time_step_nuclides(self) -> pd.DataFrame:
        """Load time step x nuclides table.

        Returns:
            time step x nuclides table
        """

    @abstractmethod
    def load_gamma(self, time_step_number: int | None = None) -> pd.DataFrame:
        """Load time step x gamma table.

        Args:
            time_step_number: filter for time_step_number

        Returns:
            time step x gamma table
        """
