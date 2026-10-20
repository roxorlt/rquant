"""Named screener schemes.

Stored in a small sqlite file, not the DuckDB market database. The schema is
applied on first open of ``SqliteSchemeStore`` (local and tests only until the
owner points a deployment at ``RQUANT_SCREENER_SCHEMES_DB``). Fixture / preview
mode uses ``MemorySchemeStore`` and does not create a file.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

SCHEMA = """
CREATE TABLE IF NOT EXISTS screener_scheme (
    name TEXT PRIMARY KEY,
    filters_json TEXT NOT NULL,
    ranks_json TEXT NOT NULL,
    saved_at TEXT NOT NULL
);
"""


class SchemeStore(Protocol):
    def list(self) -> list[dict[str, Any]]: ...

    def save(self, name: str, filters: list[Any], ranks: list[Any]) -> dict[str, Any]: ...

    def delete(self, name: str) -> None: ...


def _row(name: str, filters: list[Any], ranks: list[Any], saved_at: str) -> dict[str, Any]:
    return {"name": name, "filters": filters, "ranks": ranks, "saved_at": saved_at}


class MemorySchemeStore:
    def __init__(self) -> None:
        self._rows: dict[str, dict[str, Any]] = {}

    def list(self) -> list[dict[str, Any]]:
        return [self._rows[name] for name in sorted(self._rows)]

    def save(self, name: str, filters: list[Any], ranks: list[Any]) -> dict[str, Any]:
        saved = datetime.now(UTC).isoformat()
        row = _row(name, filters, ranks, saved)
        self._rows[name] = row
        return row

    def delete(self, name: str) -> None:
        self._rows.pop(name, None)


class SqliteSchemeStore:
    """Creates ``screener_scheme`` on first use. Pass a test path; do not use DuckDB."""

    def __init__(self, path: Path) -> None:
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as con:
            con.executescript(SCHEMA)

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.path, timeout=5.0)
        con.row_factory = sqlite3.Row
        return con

    def list(self) -> list[dict[str, Any]]:
        with self._connect() as con:
            rows = con.execute(
                "SELECT name, filters_json, ranks_json, saved_at FROM screener_scheme ORDER BY name"
            ).fetchall()
        return [
            _row(
                r["name"],
                json.loads(r["filters_json"]),
                json.loads(r["ranks_json"]),
                r["saved_at"],
            )
            for r in rows
        ]

    def save(self, name: str, filters: list[Any], ranks: list[Any]) -> dict[str, Any]:
        saved = datetime.now(UTC).isoformat()
        with self._connect() as con:
            con.execute(
                "INSERT INTO screener_scheme(name, filters_json, ranks_json, saved_at) "
                "VALUES (?, ?, ?, ?) "
                "ON CONFLICT(name) DO UPDATE SET filters_json = excluded.filters_json, "
                "ranks_json = excluded.ranks_json, saved_at = excluded.saved_at",
                (
                    name,
                    json.dumps(filters, ensure_ascii=False),
                    json.dumps(ranks, ensure_ascii=False),
                    saved,
                ),
            )
        return _row(name, filters, ranks, saved)

    def delete(self, name: str) -> None:
        with self._connect() as con:
            con.execute("DELETE FROM screener_scheme WHERE name = ?", (name,))
