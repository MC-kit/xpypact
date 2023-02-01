==============================================================================
*xpypact*: FISPACT output to datasets converter
==============================================================================



|Maintained| |License| |Versions| |PyPI| |Docs|

.. contents::


.. todo::

    This document is in progress.

Description
-----------

The module loads FISPACT JSON output as xarray dataset.
This allows efficient data extraction and aggregation.

.. configures and runs FISPACT, converts FISPACT output to xarray datasets.

.. todo::

    Currently available FISPACT v.5 API uses rather old python version (3.6).
    That prevents direct use of their API in our package (>=3.8).
    Check if own python integration with FISPACT is reasonable and feasible.


Installation
------------

From PyPI

.. code-block::

    pip install xpypact


As dependency

.. code-block::

    poetry add xpypact


From source

.. code-block::

    pip install htpps://github.com/MC-kit/xpypact.git


Examples
--------

.. todo::

    Add examples

Contributing
------------

.. image:: https://github.com/MC-kit/xpypact/workflows/Tests/badge.svg
   :target: https://github.com/MC-kit/xpypact/actions?query=workflow%3ATests
   :alt: Tests
.. image:: https://codecov.io/gh/MC-kit/xpypact/branch/master/graph/badge.svg?token=P6DPGSWM94
   :target: https://codecov.io/gh/MC-kit/xpypact
   :alt: Coverage
.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
   :target: https://github.com/psf/black
.. image:: https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336
   :target: https://pycqa.github.io/isort/
.. image:: https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white
   :target: https://github.com/pre-commit/pre-commit
   :alt: pre-commit
.. image:: https://img.shields.io/badge/try%2Fexcept%20style-tryceratops%20%F0%9F%A6%96%E2%9C%A8-black
   :target: https://github.com/guilatrova/tryceratops
   :alt: try/except style: tryceratops

Just follow ordinary practice:

    - `Commit message <https://github.com/angular/angular/blob/22b96b9/CONTRIBUTING.md#-commit-message-guidelines>`_
    - `Conventional commits <https://www.conventionalcommits.org/en/v1.0.0/#summary>`_


References
----------

.. todo::

    add references to FISPACT, pypact and used tools:  poetry etc


.. Substitutions

.. |Maintained| image:: https://img.shields.io/badge/Maintained%3F-yes-green.svg
   :target: https://github.com/MC-kit/xpypact/graphs/commit-activity
.. |Tests| image:: https://github.com/MC-kit/xpypact/workflows/Tests/badge.svg
   :target: https://github.com/MC-kit/xpypact/actions?workflow=Tests
   :alt: Tests
.. |License| image:: https://img.shields.io/github/license/MC-kit/xpypact
   :target: https://github.com/MC-kit/xpypact
.. |Versions| image:: https://img.shields.io/pypi/pyversions/xpypact
   :alt: PyPI - Python Version
.. |PyPI| image:: https://img.shields.io/pypi/v/xpypact
   :target: https://pypi.org/project/xpypact/
   :alt: PyPI
.. |Docs| image:: https://readthedocs.org/projects/xpypact/badge/?version=latest
   :target: https://xpypact.readthedocs.io/en/latest/?badge=latest
   :alt: Documentation Status
