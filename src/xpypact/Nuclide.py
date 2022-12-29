"""Nuclide specification in FISPACT JSON."""
from __future__ import annotations

from dataclasses import dataclass

try:
    from scipy.constants import Avogadro
except ImportError:  # pragma: no cover
    Avogadro = 6.02214075999999987023872e23

from mckit_nuclides.elements import z
from mckit_nuclides.nuclides import get_nuclide_mass

__all__ = ["Avogadro", "Nuclide"]


@dataclass
class Nuclide:
    """Nuclide properties from FISPACT JSON."""

    element: str
    isotope: int
    state: str = ""
    zai: int = 0  # Introduced in FISPACT-II v5: z*10000 + a*10 + (state? 1: 0)
    half_life: float = 0.0
    atoms: float = 0.0
    grams: float = 0.0
    activity: float = 0.0
    alpha_activity: float = 0.0  # FISPACT 5.0
    beta_activity: float = 0.0  # -/-
    gamma_activity: float = 0.0  # -/-
    heat: float = 0.0
    alpha_heat: float = 0.0
    beta_heat: float = 0.0
    gamma_heat: float = 0.0
    dose: float = 0.0
    ingestion: float = 0.0
    inhalation: float = 0.0

    def __post_init__(self) -> None:
        """Make the values consistent in data from old FISPACT."""
        _z = z(self.element)
        if self.zai == 0:
            self.zai = _z * 10000 + self.isotope * 10
            if self.state != "":
                self.zai += 1
        if self.atoms == 0.0 and 0.0 < self.grams:
            self.atoms = Avogadro * self.grams / get_nuclide_mass(_z, self.isotope)

    @property
    def a(self) -> int:
        """Synonym to mass number, isotope, A.

        Returns:
            A, mass number

        """
        return self.isotope

    @classmethod
    def from_json(cls, json_dict: dict) -> "Nuclide":
        """Construct the Nuclide from JSON dictionary.

        Args:
            json_dict: information in json

        Returns:
            Nuclide: the Nuclide object
        """
        return cls(**json_dict)
