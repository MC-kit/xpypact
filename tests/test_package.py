import re

from pathlib import Path

import tomli

from xpypact import __version__


def find_version_from_project_toml():
    toml_path = Path(__file__).parent.parent / "pyproject.toml"
    assert toml_path.exists()
    pyproject = tomli.loads(toml_path.read_text())
    version = pyproject["tool"]["poetry"]["version"]
    return version


_VERSION_NORM_PATTERN = re.compile(r"-(?P<letter>.)[^.]*\.(?P<prepatch>.*)$")


def normalize_version(version: str):
    return re.sub(_VERSION_NORM_PATTERN, r"\1\2", version)


def test_package():
    """This test checks if only current version is installed in working environment."""
    version = find_version_from_project_toml()
    assert __version__ == normalize_version(
        version
    ), "Run 'poetry install' and, if this doesn't help, run `dev/clear-prev-dist-info.py`"
