==============================================================================
*xpypact*: FISPACT output to datasets converter
==============================================================================



|Maintained| |License| |Versions| |PyPI| |Docs|

.. contents::


Note:
    This document is in progress.

Description
-----------

The module loads FISPACT JSON output as xarray dataset.
This allows efficient data extraction and aggregation.

.. configures and runs FISPACT, converts FISPACT output to xarray datasets.

.. TODO dvp: apply FISPACT v.5 API and describe here.


Installation
------------

::

    pip install xpypact

.. warning:: Install the hdf5 before installing xpypact for Python3.11.

    Reason:

    We depend on h5py through h5netcdf.
    The h5py package as for recent version 3.7.0 doesn't provide wheels for Python3.11.
    So, for python 3.11 pip tries to build the h5py package from sources. This fails, if hdf5 library is not preinstalled.



.. TODO dvp: check and report all possible ways to install (pip, poetry)


Examples
--------

.. TODO

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

https://www.conventionalcommits.org/en/v1.0.0/#summary
https://github.com/angular/angular/blob/22b96b9/CONTRIBUTING.md#-commit-message-guidelinesi


References
----------

.. TODO dvp: add references to FISPACT, pypact and used libraries:  poetry, xarray etc


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
