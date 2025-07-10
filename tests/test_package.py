"""Test if installed package is of the current version."""

from __future__ import annotations

import re
import tomllib

from pathlib import Path
from re import sub as substitute

from xpypact import __version__


def _find_version_from_project_toml() -> str:
    toml_path = Path(__file__).parent.parent / "pyproject.toml"
    assert toml_path.exists()
    pyproject = tomllib.loads(toml_path.read_text())
    return pyproject["project"]["version"]


_VERSION_NORM_PATTERN = re.compile(r"-(?P<letter>.)[^.]*\.(?P<prepatch>.*)$")


def _normalize_version(version: str) -> str:
    return substitute(_VERSION_NORM_PATTERN, r"\1\2", version)


def test_package() -> None:
    """This test checks if only current version is installed in working environment."""
    version = _find_version_from_project_toml()
    assert __version__ == _normalize_version(version), "Run 'uv install'"
