from __future__ import annotations

import re
import sys

from pathlib import Path
from re import sub as substitute

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

from xpypact import __version__


def find_version_from_project_toml():
    toml_path = Path(__file__).parent.parent / "pyproject.toml"
    assert toml_path.exists()
    pyproject = tomllib.loads(toml_path.read_text())
    return pyproject["tool"]["poetry"]["version"]


_VERSION_NORM_PATTERN = re.compile(r"-(?P<letter>.)[^.]*\.(?P<prepatch>.*)$")


def normalize_version(version: str):
    return substitute(_VERSION_NORM_PATTERN, r"\1\2", version)


def test_package():
    """This test checks if only current version is installed in working environment."""
    version = find_version_from_project_toml()
    assert __version__ == normalize_version(
        version
    ), "Run 'poetry install' and, if this doesn't help, run `tools/clear-prev-dist-info.py`"
