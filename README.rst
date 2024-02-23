==============================================================================
*xpypact*: FISPACT output to Polars or DuckDB converter
==============================================================================



|Maintained| |License| |Versions| |PyPI| |Docs|

.. contents::


.. note::

    This document is in progress.

Description
-----------

The module loads FISPACT JSON output files and converts to Polars dataframes
with minor data normalization.
This allows efficient data extraction and aggregation.
Multiple JSON files can be combined using simple additional identification for different
FISPACT runs. So far we use just two-dimensional identification by material
and case. The case usually identifies certain neutron flux.


Implemented functionality
-------------------------

- export to DuckDB
- export to parquet files

.. note::

    Currently available FISPACT v.5 API uses rather old python version (3.6).
    That prevents direct use of their API in our package (>=3.10).
    Check if own python integration with FISPACT is reasonable and feasible.
    Or provide own FISPACT python binding.


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

.. code-block::

    from xpypact import FullDataCollector, Inventory

    def get_material_id(p: Path) -> int:
        ...

    def get_case_id(p: Path) -> int:
        ...

    jsons = [path1, path2, ...]
    material_ids = {p: get_material_id(p) for p in jsons }
    case_ids = {c: get_case_id(p) for p in jsons }

    collector = FullDataCollector()

    if sequential_load:
        for json in jsons:
            inventory = Inventory.from_json(json)
            collector.append(inventory, material_id=material_ids[json], case_id=case_ids[json])

    else:  # multithreading is allowed for collector as well

        task_list = ...  # list of tuples[directory, case_id, tasks_sequence]
        threads = 16  # whatever

        def _find_path(arg) -> tuple[int, int, Path]:
            _case, path, inventory = arg
            json_path: Path = (Path(path) / inventory).with_suffix(".json")
            if not json_path.exists():
                msg = f"Cannot find file {json_path}"
                raise FindPathError(msg)
            try:
                material_id = int(inventory[_LEN_INVENTORY:])
                case_str = json_path.parent.parts[-1]
                case_id = int(case_str[_LEN_CASE:])
            except (ValueError, IndexError) as x:
                msg = f"Cannot define material_id and case_id from {json_path}"
                raise FindPathError(msg) from x
            if case_id != _case:
                msg = f"Contradicting values of case_id in case path and database: {case_id} != {_case}"
                raise FindPathError(msg)
            return material_id, case_id, json_path

        with futures.ThreadPoolExecutor(max_workers=threads) as executor:
            mcp_futures = [
                executor.submit(_find_path, arg)
                for arg in (
                    (task_case[0], task_case[1], task)
                    for task_case in task_list
                    for task in task_case[2].split(",")
                    if task.startswith("inventory-")
                )
            ]

        mips = [x.result() for x in futures.as_completed(mcp_futures)]
        mips.sort(key=lambda x: x[0:2])  # sort by material_id, case_id

        def _load_json(arg) -> None:
            collector, material_id, case_id, json_path = arg
            collector.append(from_json(json_path.read_text(encoding="utf8")), material_id, case_id)

        with futures.ThreadPoolExecutor(max_workers=threads) as executor:
            executor.map(_load_json, ((collector, *mip) for mip in mips))


    collected = collector.get_result()

    # save to parquet files

    collected.save_to_parquets(Path.cwd() / "parquets")

    # or use DuckDB database

    import from xpypact.dao save
    import duckdb as db

    con = db.connect()
    save(con, collected)

    gamma_from_db = con.sql(
        """
        select
        g, rate
        from timestep_gamma
        where material_id = 1 and case_id = 54 and time_step_number = 7
        order by g
        """,
    ).fetchall()


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
.. image:: https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/charliermarsh/ruff/main/assets/badge/v1.json
   :target: https://github.com/charliermarsh/ruff
   :alt: linter

Just follow ordinary practice:

    - `Commit message <https://github.com/angular/angular/blob/22b96b9/CONTRIBUTING.md#-commit-message-guidelines>`_
    - `Conventional commits <https://www.conventionalcommits.org/en/v1.0.0/#summary>`_


References
----------

.. note::

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
