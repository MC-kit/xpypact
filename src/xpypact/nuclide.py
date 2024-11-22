"""Nuclide specification in FISPACT JSON."""

from __future__ import annotations

import msgspec as ms

from mckit_nuclides import z
from mckit_nuclides.nuclides import get_nuclide_mass

Avogadro = 6.02214076e23
"""Mol-1,  `CODATA <https://pml.nist.gov/cgi-bin/cuu/Value?na>`_."""

eV = 1.602176634e-19  # noqa: N816
"""J/eV, `CODATA <https://pml.nist.gov/cgi-bin/cuu/Value?evj>`_."""

MeV = 1e6 * eV
"""J/MeV."""

FLOAT_ZERO = 0.0


class _NuclideID(ms.Struct, order=True, frozen=True, gc=False):
    """The class organizes NuclideInfo equality and ordering on zai."""

    zai: int


class NuclideInfo(_NuclideID, frozen=True, gc=False):
    """Basic information on a nuclide.

    This is extracted as a separate database entity to improve normalization.
    """

    element: str
    isotope: int
    state: str = ""
    half_life: float = 0.0


class Nuclide(ms.Struct):  # pylint: disable=too-many-instance-attributes
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
        if self.zai == 0 or (
            self.atoms == FLOAT_ZERO and self.grams > FLOAT_ZERO
        ):  # pragma: no cover
            _z = z(self.element)
            if self.zai == 0:
                self.zai = _z * 10000 + self.isotope * 10
                if self.state:
                    self.zai += 1
            if self.atoms == FLOAT_ZERO and self.grams > FLOAT_ZERO:
                self.atoms = Avogadro * self.grams / get_nuclide_mass(_z, self.isotope)

    @property
    def a(self) -> int:
        """Synonym to mass number, isotope, A.

        Returns:
            A, mass number
        """
        return self.isotope

    @property
    def info(self) -> NuclideInfo:
        """Extract a nuclide specific information.

        Returns:
            element, a, state, zai, half_life
        """
        return NuclideInfo(self.zai, self.element, self.a, self.state, self.half_life)


__all__ = ["FLOAT_ZERO", "Avogadro", "MeV", "Nuclide", "NuclideInfo", "eV"]
