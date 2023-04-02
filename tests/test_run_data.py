"""Tests for FISPACT header."""
from __future__ import annotations

import pytest

from xpypact.run_data import RunData


@pytest.mark.parametrize(
    ["inp", "expected"],
    [
        (
            {
                "timestamp": "23:01:19 12 July 2020",
                "run_name": "* Material Ag, fluxes 1",
                "flux_name": "55.F9.10 11-L2-02W HFS_GLRY_08_U",
            },
            RunData(
                timestamp="23:01:19 12 July 2020",
                run_name="* Material Ag, fluxes 1",
                flux_name="55.F9.10 11-L2-02W HFS_GLRY_08_U",
            ),
        ),
    ],
)
def test_from_json(inp, expected):
    actual = RunData.from_json(inp)
    assert actual == expected
