"""Nuclide specification in FISPACT JSON."""

from typing import Dict

from dataclasses import dataclass

try:
    from scipy.constants import Avogadro
except ImportError:
    Avogadro = 6.02214075999999987023872e23

from mckit_nuclides.elements import Element

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
        if self.zai == 0:
            # TODO dvp: when mckit-nuclides updates, switch to direct z() method.
            #           There's no need to construct Element accessor here.
            e = Element(self.element)
            self.zai = e.z * 10000 + self.isotope * 10
            if self.state != "":
                self.zai += 1
        if self.atoms == 0.0 and 0.0 < self.grams:
            self.atoms = Avogadro * self.grams / self.isotope

    @classmethod
    def from_json(cls, json_dict: Dict) -> "Nuclide":
        """Construct the Nuclide from JSON dictionary.

        Args:
            json_dict: information in json

        Returns:
            Nuclide: the Nuclide object
        """
        return cls(**json_dict)

    @property
    def a(self) -> int:
        """Atomic mass."""
        return self.isotope
