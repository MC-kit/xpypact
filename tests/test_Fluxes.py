from typing import Any

from copy import copy, deepcopy
from functools import reduce
from io import StringIO

import numpy as np
import pytest

from numpy.testing import assert_almost_equal, assert_array_equal
from xpypact.Fluxes import (
    Fluxes,
    StandardFluxesDataSizeError,
    are_fluxes_close,
    are_fluxes_equal,
    define_709_bins_and_fluxes,
    define_arb_bins_and_fluxes,
    is_709_fluxes,
    print_709_fluxes,
    print_arbitrary_fluxes,
    read_709_fluxes,
    read_arb_fluxes,
)


@pytest.fixture(scope="module")
def arb_flux_1(data) -> Fluxes:
    path = data / "arb_flux_1"
    arb_fluxes: Fluxes = read_arb_fluxes(path)
    assert arb_fluxes.comment == "total flux=9.100000e+01", "Wrong comment"
    assert arb_fluxes.norm == 1.0
    assert arb_fluxes.energy_bins.size == 3
    assert arb_fluxes.fluxes.size == 2
    return arb_fluxes


def test_flux_constructor():
    ebins = np.linspace(0, 20, 10)
    bins = np.ones(9, dtype=float)
    flux = Fluxes(ebins, bins, "test comment", 2.0)
    assert flux.total == 9.0
    assert flux.comment == "test comment"
    assert flux.norm == 2.0
    assert not is_709_fluxes(flux)
    assert are_fluxes_close(flux, flux)
    with pytest.raises(ValueError) as exc_info:
        Fluxes(ebins[:-2], bins, "test")
    assert exc_info.type == ValueError
    assert exc_info.value.args[0].startswith("Incompatible sizes of bins and fluxes")
    ebins, bins = define_709_bins_and_fluxes(np.ones(709, dtype=float))
    f709 = Fluxes(ebins, bins, "test 709")
    assert f709.total == 709
    assert not are_fluxes_close(flux, f709)
    with pytest.raises(StandardFluxesDataSizeError) as exc_info:
        define_709_bins_and_fluxes(np.ones(5))
    assert str(exc_info.value) == "Invalid data for standard FISPACT 709-group fluxes."


@pytest.mark.parametrize(
    "_bin, expected_e0, expected_e1,expected_flux",
    [
        (0, 1e-5, 1e6, 1),
        (1, 1e6, 1e7, 90),
    ],
)
def test_reading_arbitrary_fluxes_1(
    arb_flux_1, _bin, expected_e0, expected_e1, expected_flux
):
    assert_bin(arb_flux_1, _bin, expected_e0, expected_e1, expected_flux)


@pytest.fixture(scope="module")
def arb_flux_2(data):
    path = data / "arb_flux_2"
    arb_fluxes = read_arb_fluxes(path)
    assert arb_fluxes.comment == "total flux=1.014956e+10", "Wrong comment"
    assert arb_fluxes.norm == 1.0
    assert arb_fluxes.energy_bins.size == 8
    assert arb_fluxes.fluxes.size == 7
    return arb_fluxes


def test_eq_and_copy(arb_flux_1, arb_flux_2):
    def assert_one(_: Any, f: Any) -> Any:
        assert hash(f) == hash(f)
        assert f == f
        assert f == copy(f)
        assert f == deepcopy(f)

    reduce(assert_one, [arb_flux_1, arb_flux_2])
    assert hash(arb_flux_1) != hash(arb_flux_2)
    assert arb_flux_1 != arb_flux_2


@pytest.mark.parametrize(
    "_bin, expected_e0, expected_e1,expected_flux",
    [
        (0, 1.1e-5, 0.25, 3.600000e05),
        (6, 7.800000e06, 1.410000e07, 2.870000e03),
    ],
)
def test_test_reading_arbitrary_fluxes_2(
    arb_flux_2, _bin, expected_e0, expected_e1, expected_flux
):
    assert_bin(arb_flux_2, _bin, expected_e0, expected_e1, expected_flux)


@pytest.fixture(scope="module")
def fluxes_1(data):
    path = data / "fluxes_1"
    fluxes = read_709_fluxes(path)
    assert is_709_fluxes(fluxes)
    assert fluxes.comment == "total flux=9.100000e+01", "Wrong comment"
    assert fluxes.norm == 1.0
    return fluxes


@pytest.mark.parametrize(
    "_bin, expected_e0, expected_e1,expected_flux",
    [
        (0, 1e-5, 1.0471e-5, 1.8182e-3),
        (1, 1.0471e-5, 1.0965e-5, 1.8182e-3),
        (708, 9.6000e8, 1.0000e9, 0.0),
    ],
)
def test_reading_fluxes_1(fluxes_1, _bin, expected_e0, expected_e1, expected_flux):
    assert_bin(fluxes_1, _bin, expected_e0, expected_e1, expected_flux)


def test_fispact_conversion(arb_flux_1, fluxes_1):
    assert_almost_equal(arb_flux_1.total, fluxes_1.total, decimal=5)


# def test_make_709_fluxes_1(arb_flux_1, fluxes_1):
#     actual = make_709_fluxes(arb_flux_1)
#     assert_almost_equal(arb_flux_1.total, actual.total, decimal=8)
#     assert_almost_equal(
#         actual.fluxes, fluxes_1.fluxes, decimal=7
#     ), "Fluxes generated with python and FISPACT should be equal"


def test_print_arbitrary_fluxes(data, arb_flux_1):
    original_text = (data / "arb_flux_1").read_text()
    stream = StringIO()
    print_arbitrary_fluxes(arb_flux_1, stream, max_columns=3)
    actual = stream.getvalue()
    assert_fluxes_text_equal(actual, original_text)
    read_flux = read_arb_fluxes(actual)
    assert read_flux == arb_flux_1
    stream = StringIO()
    print_arbitrary_fluxes(arb_flux_1, stream, max_columns=1)
    actual = stream.getvalue()
    assert_fluxes_text_equal(actual, original_text)
    read_flux = read_arb_fluxes(actual)
    assert read_flux == arb_flux_1
    stream = StringIO()
    print_arbitrary_fluxes(arb_flux_1, stream, max_columns=10)
    actual = stream.getvalue()
    assert_fluxes_text_equal(actual, original_text)
    read_flux = read_arb_fluxes(actual)
    assert read_flux == arb_flux_1
    assert are_fluxes_equal(read_flux, arb_flux_1)


def test_define_arb_flux_fail():
    data = np.linspace(1, 10, 4)
    with pytest.raises(ValueError):
        define_arb_bins_and_fluxes(data)


def test_print_709_fluxes_fail(arb_flux_1):
    with pytest.raises(ValueError) as exc_info:
        stream = StringIO()
        print_709_fluxes(arb_flux_1, stream, max_columns=3)
    assert str(exc_info.value) == "Expected 709-group fluxes."


def test_print_709_fluxes(fluxes_1):
    stream = StringIO()
    print_709_fluxes(fluxes_1, stream, max_columns=3)
    actual = stream.getvalue()
    read_flux = read_709_fluxes(actual)
    assert read_flux == fluxes_1


def test_read_709_fluxes(data):
    text = (data / "fluxes_1").read_text(encoding="utf8")
    fluxes = read_709_fluxes(text)
    assert fluxes.fluxes.size == 709


def assert_bin(
    fluxes: Fluxes, _bin: int, expected_e0, expected_e1, expected_flux
) -> None:
    actual_e0 = fluxes.energy_bins[_bin]
    assert_almost_equal(actual_e0, expected_e0)
    actual_e1 = fluxes.energy_bins[_bin + 1]
    assert_almost_equal(actual_e1, expected_e1)
    actual_flux = fluxes.fluxes[_bin]
    assert actual_flux == expected_flux


def assert_fluxes_text_equal(a: str, b: str) -> None:
    lines1, lines2 = (i.split("\n") for i in [a, b])
    assert lines1[-1] == lines2[-1], "Comments should be the same"
    assert float(lines1[-2]) == float(lines2[-2]), "Norms should be the same"
    s_lines1, s_lines2 = (" ".join(i[:-2]) for i in [lines1, lines2])
    array1, array2 = (np.fromiter(i.split(), dtype=float) for i in [s_lines1, s_lines2])
    assert_array_equal(array1, array2, err_msg="Bins and fluxes should be equal")


if __name__ == "__main__":
    pytest.main()
