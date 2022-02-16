"""Classes to read a FISPACT time step attributes from JSON."""

from typing import Dict, List, Tuple

from dataclasses import dataclass, field

import numpy as np

from numpy import ndarray as array

from .Nuclide import Nuclide


@dataclass
class DoseRate:
    """Dose rate attributes."""

    type: str = ""
    distance: float = 0.0
    mass: float = 0.0
    dose: float = 0.0

    def __post_init__(self) -> None:
        """Correct wrong value coming from FISPACT"""
        # TODO dvp: check behaviour with FISPACT v.5 and try to correct scenarios.
        if self.mass == 0.0:
            # According to FISPACT manual (v.4)
            # should be 1 gram always, but FISPACT shows 0 at the first step
            # and less, than 1 in the following steps. Fixing here.
            self.mass = 1.0e-3


@dataclass
class GammaSpectrum:
    """Data on gamma emission."""

    boundaries: array
    """Energy boundaries, MeV"""
    values: array
    """Gamma emission intensity."""  # TODO dvp: identify units.

    @classmethod
    def from_json(cls, data: Dict) -> "GammaSpectrum":
        return cls(
            boundaries=np.array(data["boundaries"], dtype=float),
            values=np.array(data["values"], dtype=float),
        )


@dataclass
class TimeStep:
    """Time step attributes.

    All names must be the same as in FISPACT JSON file.
    """

    number: int = -1
    irradiation_time: float = 0.0
    cooling_time: float = 0.0
    duration: float = 0.0
    elapsed_time: float = 0.0
    flux: float = 0.0
    # TODO dvp: compute the values in case of FISPACT v.4 to maintain same logic.
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
    nuclides: List[Nuclide] = field(default_factory=list)
    gamma_spectrum: GammaSpectrum = None

    def __post_init__(self) -> None:
        if 0.0 == self.total_mass:  # workaround for FISPACT v.4
            # Mass of all nuclides in the TimeStep (kg)
            # TODO dvp: check for FISPACT v.5
            self.total_mass = 1e-3 * sum(map(lambda x: x.grams, self.nuclides))

    @property
    def nuclides_mass(self) -> float:
        """Synonym for total mass."""
        return self.total_mass

    @property
    def is_cooling(self) -> bool:
        """Is the time step for cooling?"""
        return self.flux == 0.0

    @property
    def dose_rate_type_and_distance(self) -> Tuple[str, float]:
        """Get Dose rate type and distance.

        These two properties are duplicated over all the TimeSteps (lack of normalization?).
        The attributes are moved to RunData on postprocessing.
        """
        return self.dose_rate.type, self.dose_rate.distance

    # def key(self):
    #     return self.number
    #
    # def properties(self) -> array:
    #     return np.array(
    #         [
    #             self.irradiation_time,
    #             self.cooling_time,
    #             self.duration,
    #             self.elapsed_time,
    #             self.flux,
    #             self.total_atoms,
    #             self.total_activity,
    #             self.alpha_activity,
    #             self.beta_activity,
    #             self.gamma_activity,
    #             self.total_mass,
    #             self.total_heat,
    #             self.alpha_heat,
    #             self.beta_heat,
    #             self.gamma_heat,
    #             self.ingestion_dose,
    #             self.inhalation_dose,
    #             # TODO dvp: don't see use case for the "dose_rate.mass" parameter, check if it's needed at all.
    #             self.dose_rate.dose,
    #         ],
    #         dtype=float,
    #     )

    @classmethod
    def from_json(cls, json_dict: Dict) -> "TimeStep":
        json_dose_rate = json_dict.pop("dose_rate")
        dose_rate = DoseRate(**json_dose_rate)
        json_nuclides = json_dict.pop("nuclides", None)
        if json_nuclides:
            nuclides = list(map(Nuclide.from_json, json_nuclides))
        else:
            nuclides = []
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


# TIME_STEP_PROPERTIES = list(map(lambda x: x.name, dataclasses.fields(TimeStep)))[
#     1:-3
# ] + [
#     "dose"
# ]  # omitted number and original dose_rate and nuclides, added extracted dose values
# assert TIME_STEP_PROPERTIES[0] == "irradiation_time"
# assert TIME_STEP_PROPERTIES[-2] == "inhalation_dose"
# assert len(TIME_STEP_PROPERTIES) == 18
