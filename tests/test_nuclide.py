from __future__ import annotations

import pytest

from xpypact.nuclide import Nuclide


@pytest.mark.parametrize(
    "a,b,eq,order",
    [
        (("H", 1, "", 10010), ("H", 1, "", 10010), True, False),
        (("H", 1, "", 10010), ("H", 2, "", 10020), False, True),
    ],
)
def test_equality_and_comparison(
    a: tuple[str, int, str, int],
    b: tuple[str, int, str, int],
    eq: bool,  # noqa: FBT001
    order: bool,  # noqa: FBT001
) -> None:
    _a = Nuclide(*a).info
    _b = Nuclide(*b).info
    assert eq == (_a == _b)
    assert order == (_a < _b)
