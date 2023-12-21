"""Classes to read a FISPACT time step attributes from JSON."""
from __future__ import annotations

from typing import Any

import msgspec as ms

from xpypact.nuclide import Nuclide

FLOAT_ZERO = 0.0


class DoseRate(ms.Struct, frozen=True, gc=False):
    """Dose rate attributes.

    Don't scale by mass, for point source mass is always 1g, for contact dose mass is meaningless.

    Attrs:
        type: "Plain source" for contact dose or "Point source"
        distance: specified for "point source", meters
        mass: mass for point source, always 1g
    """

    type: str = ""  # noqa: A003
    distance: float = 0.0
    mass: float = 0.0
    dose: float = 0.0

    def __post_init__(self) -> None:
        """Correct wrong value coming from FISPACT."""
        if self.mass == FLOAT_ZERO:
            # According to FISPACT manual (both v.4 and v.5 (p.63))
            # should be 1 gram always, but FISPACT shows 0 at the first step
            # and less, than 1 in the following steps. Fixing here.
            self.mass = 1.0e-3


class GammaSpectrum(ms.Struct):
    """Data on gamma emission.

    Attrs:
        boundaries:
            Energy boundaries, MeV
        intensities:
            Gamma emission intensity.
    """

    boundaries: list[float]
    values: list[float]

    @property
    def intensities(self) -> list[float]:
        return self.values

    # @classmethod
    # def from_json(cls, json: dict[str, list[float]]) -> GammaSpectrum:
    #     """Construct GammaSpectrum instance from JSON dictionary.
    #
    #     Args:
    #         json: dictionary
    #
    #     Returns:
    #         The new GammaSpectrum instance with loaded boundaries and values.
    #     """
    #     return cls(
    #         boundaries=np.array(json["boundaries"], dtype=float),
    #         intensities=np.array(json["values"], dtype=float),
    #     )


class TimeStep(ms.Struct):  # pylint: disable=too-many-instance-attributes
    """Time step attributes.

    All names must be the same as in FISPACT JSON file.
    """

    number: int = -1
    irradiation_time: float = 0.0
    cooling_time: float = 0.0
    duration: float = 0.0
    elapsed_time: float = 0.0
    flux: float = 0.0
    total_atoms: float = 0.0  # FISPACT 5.0
    total_activity: float = 0.0  # -/-
    alpha_activity: float = 0.0  # -/-
    beta_activity: float = 0.0  # -/-
    gamma_activity: float = 0.0  # -/-
    total_mass: float = 0.0  # -/-
    total_heat: float = 0.0
    alpha_heat: float = 0.0
    beta_heat: float = 0.0
    gamma_heat: float = 0.0
    ingestion_dose: float = 0.0
    inhalation_dose: float = 0.0
    dose_rate: DoseRate = field(default_factory=DoseRate)
    nuclides: list[Nuclide] = field(default_factory=list)
    gamma_spectrum: GammaSpectrum | None = None

    def __post_init__(self) -> None:
        """Correct data missed in FISPACT-4."""
        # workarounds for FISPACT v.4
        if self.total_mass == FLOAT_ZERO:
            self.total_mass = 1e-3 * sum(n.grams for n in self.nuclides)
        if self.total_atoms == FLOAT_ZERO:
            self.total_atoms = sum(n.atoms for n in self.nuclides)
        if self.total_activity == FLOAT_ZERO:
            self.total_activity = sum(n.activity for n in self.nuclides)
        if self.alpha_activity == FLOAT_ZERO:
            self.alpha_activity = sum(n.alpha_activity for n in self.nuclides)
        if self.beta_activity == FLOAT_ZERO:
            self.beta_activity = sum(n.beta_activity for n in self.nuclides)
        if self.gamma_activity == FLOAT_ZERO:
            self.gamma_activity = sum(n.gamma_activity for n in self.nuclides)

    @property
    def nuclides_mass(self) -> float:
        """Synonym for total mass.

        Returns:
            Total mass of the nuclides in kg.
        """
        return self.total_mass

    @property
    def is_cooling(self) -> bool:
        """Is the time step for cooling?

        Returns:
            Is the irradiation flux zero?
        """
        return self.flux == FLOAT_ZERO

    @classmethod
    def from_json(cls, json_dict: dict[str, Any]) -> TimeStep:
        """Construct TimeStep instance from JSON dictionary.

        Args:
            json_dict: source dictionary

        Returns:
            The new TimeStep instance.
        """
        json_dose_rate = json_dict.pop("dose_rate")
        dose_rate = DoseRate(**json_dose_rate)
        json_nuclides = json_dict.pop("nuclides")
        nuclides = [Nuclide.from_json(n) for n in json_nuclides] if json_nuclides else []
        json_gamma_spectrum = json_dict.pop("gamma_spectrum", None)
        if json_gamma_spectrum:
            gamma_spectrum = GammaSpectrum.from_json(json_gamma_spectrum)
        else:
            gamma_spectrum = None
        return cls(
            dose_rate=dose_rate,
            nuclides=nuclides,
            gamma_spectrum=gamma_spectrum,
            **json_dict,
        )
