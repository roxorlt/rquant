"""Read-only web API for the React front end under ``web/`` (served at ``/app/api/``).

The package reads Serving generations and nothing else: no ``rquant.config`` (it reads
``.env`` on construction), no ``rquant.storage``, no direct DuckDB connections.
``tests/unit/test_web_import_isolation.py`` holds that boundary in place.
"""
