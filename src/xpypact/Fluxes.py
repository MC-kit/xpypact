"""The Class to represent FISPACT fluxes file content."""

from typing import Callable, TextIO, Tuple

from dataclasses import dataclass
from io import StringIO, TextIOWrapper
from pathlib import Path

import numpy as np

from multipledispatch import dispatch
from numpy import allclose, array_equal
from numpy import ndarray as array
from xpypact.utils.io import print_cols

LAST_TWO_DECADES = np.fromstring(
    """
        1.0000E+9
        9.6000E+8
        9.2000E+8
        8.8000E+8
        8.4000E+8
        8.0000E+8
        7.6000E+8
        7.2000E+8
        6.8000E+8
        6.4000E+8
        6.0000E+8
        5.6000E+8
        5.2000E+8
        4.8000E+8
        4.4000E+8
        4.0000E+8
        3.6000E+8
        3.2000E+8
        2.8000E+8
        2.4000E+8
        2.0000E+8
        1.8000E+8
        1.6000E+8
        1.5000E+8
        1.4000E+8
        1.3000E+8
        1.2000E+8
        1.1000E+8
        1.0000E+8
        9.0000E+7
        8.0000E+7
        7.5000E+7
        7.0000E+7
        6.5000E+7
        6.0000E+7
        5.8000E+7
        5.6000E+7
        5.4000E+7
        5.2000E+7
        5.0000E+7
        4.8000E+7
        4.6000E+7
        4.4000E+7
        4.2000E+7
        4.0000E+7
        3.8000E+7
        3.6000E+7
        3.4000E+7
        3.2000E+7
        3.0000E+7
        2.9000E+7
        2.8000E+7
        2.7000E+7
        2.6000E+7
        2.5000E+7
        2.4000E+7
        2.3000E+7
        2.2000E+7
        2.1000E+7
        2.0000E+7
        1.9800E+7
        1.9600E+7
        1.9400E+7
        1.9200E+7
        1.9000E+7
        1.8800E+7
        1.8600E+7
        1.8400E+7
        1.8200E+7
        1.8000E+7
        1.7800E+7
        1.7600E+7
        1.7400E+7
        1.7200E+7
        1.7000E+7
        1.6800E+7
        1.6600E+7
        1.6400E+7
        1.6200E+7
        1.6000E+7
        1.5800E+7
        1.5600E+7
        1.5400E+7
        1.5200E+7
        1.5000E+7
        1.4800E+7
        1.4600E+7
        1.4400E+7
        1.4200E+7
        1.4000E+7
        1.3800E+7
        1.3600E+7
        1.3400E+7
        1.3200E+7
        1.3000E+7
        1.2800E+7
        1.2600E+7
        1.2400E+7
        1.2200E+7
        1.2000E+7
        1.1800E+7
        1.1600E+7
        1.1400E+7
        1.1200E+7
        1.1000E+7
        1.0800E+7
        1.0600E+7
        1.0400E+7
        1.0200E+7
    """,
    dtype=float,
    sep=" \n",
)[::-1]


def compute_709_bins() -> np.ndarray:
    """Computes bins for 709 groups according to the specification.

    Decades from -5 to 9 are divided to 50 bins equidistant in log scale.

    Note:
        The computed bins show better correspondence with FISPACT converted fluxes,
        then available tabulated data.
        See notebooks/dvp/demo_fispact_flux_generator.ipynb

    Returns:
        np.ndarray: array of 709 bins
    """
    template = np.logspace(0, 1, 51)
    res = np.hstack([template[1:] * 10**i for i in range(-5, 7)] + [LAST_TWO_DECADES])
    return np.insert(res, 0, 1e-5)


FISPACT_709_BINS = compute_709_bins()


@dataclass(eq=False, order=False)
class Fluxes:
    """Class to represent flux data."""

    energy_bins: array
    fluxes: array
    comment: str
    norm: float = 1.0

    def __post_init__(self) -> None:
        """Validate sizes of arrays.

        Raises:
            ValueError: if sizes of bins and fluxes are not compatible.
        """
        if self.fluxes.size != self.energy_bins.size - 1:
            raise ValueError(
                "Incompatible sizes of bins and fluxes, "
                f"{self.fluxes.size} and {self.energy_bins.size}"
            )

    @property
    def total(self) -> float:
        """Return flux total.

        Returns:
            float: flux total
        """
        return np.sum(self.fluxes).item()

    def __hash__(self) -> int:
        """Use available information for hash.

        Returns:
            hash
        """
        return hash((self.energy_bins.size, self.fluxes.size, self.comment, self.norm))

    def __eq__(self, other: "Fluxes") -> bool:
        """Compare fluxes.

        Args:
            other: other fluxes

        Returns:
            bool: True, if the fluxes are equal, otherwise - False
        """
        return self is other or (
            array_equal(self.energy_bins, other.energy_bins)
            and array_equal(self.fluxes, other.fluxes)
            and self.comment == other.comment
            and self.norm == other.norm
        )

    def __ne__(self, other: "Fluxes") -> bool:
        """Compare fluxes.

        Args:
            other: other fluxes

        Returns:
            bool: False, if the fluxes are equal, otherwise - True
        """
        return not self.__eq__(other)


def is_709_fluxes(fluxes: Fluxes) -> bool:
    """Check if fluxes are 709-kind of fluxes.

    This fluxes can be output withou energy bins and
    can be used with 'flux' statement in FISPACT run configuration.

    Args:
        fluxes: what to check

    Returns:
        bool: True, if fluxes are 709 kind of fluxes.
    """
    return fluxes.energy_bins is FISPACT_709_BINS and fluxes.fluxes.size == 709


def are_fluxes_equal(a: Fluxes, b: Fluxes) -> bool:
    """Compare data of fluxes.

    Check if fluxes are equivalent by data, disregarding differences in comment and norm.

    Args:
        a: the first flux to compare
        b: the second flux to compare

    Returns:
        bool: True, if the fluxes are equivalent by data
    """
    return array_equal(a.energy_bins, b.energy_bins) and array_equal(a.fluxes, b.fluxes)


def are_fluxes_close(
    a: Fluxes,
    b: Fluxes,
    rtol: float = 1.0e-5,
    atol: float = 1.0e-8,
    equal_nan: bool = False,
) -> bool:
    """Compare data of fluxes approximately.

    Check if fluxes are equivalent by data with some tolerance, disregarding differences in comment and norm.

    Args:
        a: the first flux to compare
        b: the second flux to compare
        rtol: relative tolerance
        atol: absolute tolerance

    Returns:
        bool: True, if the fluxes are equivalent by data
    """
    return allclose(
        a.energy_bins, b.energy_bins, rtol=rtol, atol=atol, equal_nan=equal_nan
    ) and allclose(a.fluxes, b.fluxes, rtol=rtol, atol=atol, equal_nan=equal_nan)


def read_fluxes(
    stream: TextIO, define_bins_and_fluxes: Callable[[array], Tuple[array, array]]
) -> Fluxes:
    lines = stream.readlines()
    comment = lines[-1].strip()
    norm = float(lines[-2])
    lines = lines[:-2]
    data = np.fromstring(" ".join(lines), dtype=float, count=-1, sep=" ")
    energy_bins, fluxes = define_bins_and_fluxes(data)
    return Fluxes(energy_bins, fluxes, comment, norm)


@dispatch(TextIOWrapper)
def read_arb_fluxes(
    stream: TextIO,
) -> Fluxes:
    return read_fluxes(stream, define_arb_bins_and_fluxes)


@dispatch(Path)
def read_arb_fluxes(path: Path) -> Fluxes:
    with path.open() as stream:
        return read_arb_fluxes(stream)


@dispatch(str)
def read_arb_fluxes(text: str) -> Fluxes:
    with StringIO(text) as stream:
        return read_fluxes(stream, define_arb_bins_and_fluxes)


@dispatch(TextIOWrapper)
def read_709_fluxes(
    stream: TextIO,
) -> Fluxes:
    return read_fluxes(stream, define_709_bins_and_fluxes)


@dispatch(Path)
def read_709_fluxes(path: Path) -> Fluxes:
    with path.open() as stream:
        return read_709_fluxes(stream)


@dispatch(str)
def read_709_fluxes(text: str) -> Fluxes:
    with StringIO(text) as stream:
        return read_709_fluxes(stream)


def define_arb_bins_and_fluxes(data: array) -> Tuple[array, array]:
    sz = data.size
    assert sz & 1, "The number of float values from arb_flux file should be odd"
    bins_end = sz // 2 + 1
    energy_bins = data[:bins_end]
    fluxes = data[bins_end:]
    energy_bins = energy_bins[::-1]
    fluxes = fluxes[::-1]
    return energy_bins, fluxes


def define_709_bins_and_fluxes(data: array) -> Tuple[array, array]:
    assert data.size == 709, "Invalid data for FISPACT fluxes"
    return FISPACT_709_BINS, data[::-1]


# def make_709_fluxes(fluxes: Fluxes):
#     energies, hist, comment, norm = (
#         fluxes.energy_bins,
#         fluxes.fluxes,
#         fluxes.comment,
#         fluxes.norm,
#     )
#     e_min_max = energies[:: energies.size - 1]
#     i_min, i_max = np.searchsorted(FISPACT_709_BINS, e_min_max)
#     if FISPACT_709_BINS[i_max] == e_min_max[1]:
#         i_max += 1
#     rebin_hist = ut.rebin_1d(
#         hist,
#         np.log(energies),
#         np.log(FISPACT_709_BINS[i_min:i_max]),
#         grouped=True,
#         assume_sorted=True,
#     )
#     leading_zeros = np.zeros(i_min, dtype=float)
#     trailing_zeros = np.zeros(709 - i_max + 1, dtype=float)
#     assert leading_zeros.size + rebin_hist.size + trailing_zeros.size == 709
#     rebin_hist = np.hstack(
#         [
#             leading_zeros,
#             rebin_hist,
#             trailing_zeros,
#         ]
#     )
#     result = Fluxes(FISPACT_709_BINS, rebin_hist, comment, norm)
#     assert is_709_fluxes(result)
#     return result


def print_fluxes(fluxes: Fluxes, fid: TextIO, arbitrary: bool, max_columns=7):
    if arbitrary:
        sequence = fluxes.energy_bins[::-1]
        column = print_cols(sequence, fid, max_columns, fmt="{:g}")
        if column != 0:
            print(file=fid)
    sequence = fluxes.fluxes[::-1]
    column = print_cols(sequence, fid, max_columns)
    if column != 0:
        print(file=fid)
    print(fluxes.norm, file=fid)
    print(fluxes.comment, file=fid, end="")


# def print_709_fluxes(fluxes: Fluxes, fid: TextIO, max_columns=7):
#     if not is_709_fluxes(fluxes):
#         fluxes = make_709_fluxes(fluxes)
#     print_fluxes(fluxes, fid, False, max_columns)


def print_arbitrary_fluxes(fluxes: Fluxes, fid: TextIO, max_columns=7) -> None:
    """Print fluxes in FISPACT arbitrary flux format.

    Args:
        fluxes: what to print
        fid: output stream
        max_columns: max number of columns in a row

    """
    print_fluxes(fluxes, fid, True, max_columns)
