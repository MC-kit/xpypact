=================================
Welcome to xpypact documentation!
=================================

.. todo::

    The documentation is under development.


The package helps:

    - to create FISPACT run configuration,
    - to load FISPACT JSON output as `xarray <https://xarray.dev/>`_ dataset,
    - filter, aggregate data from dataset, join with external data
    - save results to netcdf files




.. todo::

    - Implement prepare multiple configurations run for given case ids, materials, fluxes
    - Parallel (may be with dask?) execution of FISPACT
    - prototype alternatives for data saving to parquet, SQL, whatever
    - parallel collecting results to selected alternative


Installation
============

From PyPI (recommended):

.. code-block::

   pip install xpypact

With package manager (as dependency):

.. code-block::

   poetry add xpypact

From source:

.. code-block::

   pip install https://github.com/MC-kit/xpypact.git

Details
=======

.. toctree::
   :maxdepth: 2

   readme
   modules
   license
   todo



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
