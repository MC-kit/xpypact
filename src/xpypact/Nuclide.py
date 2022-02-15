"""Nuclide specification in FISPACT JSON."""

from typing import Dict

from dataclasses import dataclass, field

import numpy as np

from numpy import array

try:
    from scipy.constants import Avogadro
except ImportError:
    Avogadro = 6.02214075999999987023872e23

from mckit_nuclides.nuclides import SYMBOL_2_ATOMIC_NUMBER, get_nuclide_mass

from .Extractable import Extractable


@dataclass
class NuclideKey:
    element: str
    isotope: int
    state: str
    half_life: float = field(compare=False)
    z: int = field(default=-1, compare=False)
    atomic_mass: float = field(default=-1.0, compare=False)

    def __post_init__(self) -> None:
        self.z = SYMBOL_2_ATOMIC_NUMBER[self.element]
        self.atomic_mass = get_nuclide_mass(self.z, self.isotope)


@dataclass
class Nuclide(Extractable):
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
    _key: NuclideKey = None

    def __post_init__(self) -> None:
        if self._key is None:
            self._key = NuclideKey(
                self.element, self.isotope, self.state, self.half_life
            )
        zai = self._key.z * 10000 + self.isotope * 10 + (1 if self.state != "" else 0)
        if self.zai == 0:
            self.zai = zai
        elif self.zai != zai:
            raise ValueError(f"Incorrect zai expected: {zai} actual {self.zai}")
        if self.atoms == 0.0:
            self.atoms = Avogadro * self.grams / self._key.atomic_mass

    def key(self) -> NuclideKey:
        return self._key

    def properties(self) -> array:
        return np.array(
            [
                self.atoms,
                self.grams,
                self.activity,
                self.alpha_activity,
                self.beta_activity,
                self.gamma_activity,
                self.heat,
                self.alpha_heat,
                self.beta_heat,
                self.gamma_heat,
                self.dose,
                self.ingestion,
                self.inhalation,
            ],
            dtype=float,
        )

    @classmethod
    def from_json(cls, json_dict: Dict) -> "Nuclide":
        return cls(**json_dict)


# NUCLIDE_PROPERTIES = list(map(lambda x: x.name, fields(Nuclide)))[5:-1]
# assert NUCLIDE_PROPERTIES[0] == "atoms"
# assert NUCLIDE_PROPERTIES[-1] == "inhalation"
# NUCLIDE_PROPERTIES_NUMBER = len(NUCLIDE_PROPERTIES)
# digits = "0123456789"
#
#
# def find_letters_end(isotope_name: str):
#     i = -1
#     for i, c in enumerate(isotope_name):
#         if c in digits:
#             return i
#     return i + 1
#
#
# def get_element_name(isotope_name: str) -> str:
#     return isotope_name[: find_letters_end(isotope_name)]
