"""Named screener schemes and per-user page preferences.

Stored in one small sqlite file (the web store), not the DuckDB market database.
Tables: ``screener_scheme`` (named screener schemes) and ``ui_preference``
(column visibility, sort orders, query history and similar page settings,
one text value per user and key). The schema is
applied on first open of ``SqliteSchemeStore`` (local and tests only until the
owner points a deployment at ``RQUANT_SCREENER_SCHEMES_DB``). Fixture / preview
mode uses ``MemorySchemeStore`` and does not create a file.
"""

from __future__ import annotations

import json
import re
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
CREATE TABLE IF NOT EXISTS ui_preference (
    owner TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (owner, key)
);
"""

#: Keys that follow the account. Theme and the collapsed rail stay in the browser.
PREFERENCE_KEY = re.compile(r"^(cols|sort|pref)\.[^\s]{1,60}$|^query\.history$")
PREFERENCE_MAX_CHARS = 16_000
PREFERENCE_MAX_KEYS = 200


def preference_key_ok(key: str) -> bool:
    return bool(PREFERENCE_KEY.match(key))


class SchemeStore(Protocol):
    def list(self) -> list[dict[str, Any]]: ...

    def save(self, name: str, filters: list[Any], ranks: list[Any]) -> dict[str, Any]: ...

    def delete(self, name: str) -> None: ...

    def preferences(self, owner: str) -> dict[str, str]: ...

    def set_preference(self, owner: str, key: str, value: str | None) -> None: ...


def _row(name: str, filters: list[Any], ranks: list[Any], saved_at: str) -> dict[str, Any]:
    return {"name": name, "filters": filters, "ranks": ranks, "saved_at": saved_at}


class MemorySchemeStore:
    def __init__(self) -> None:
        self._rows: dict[str, dict[str, Any]] = {}
        self._prefs: dict[str, dict[str, str]] = {}

    def list(self) -> list[dict[str, Any]]:
        return [self._rows[name] for name in sorted(self._rows)]

    def save(self, name: str, filters: list[Any], ranks: list[Any]) -> dict[str, Any]:
        saved = datetime.now(UTC).isoformat()
        row = _row(name, filters, ranks, saved)
        self._rows[name] = row
        return row

    def delete(self, name: str) -> None:
        self._rows.pop(name, None)

    def preferences(self, owner: str) -> dict[str, str]:
        return dict(self._prefs.get(owner, {}))

    def set_preference(self, owner: str, key: str, value: str | None) -> None:
        prefs = self._prefs.setdefault(owner, {})
        if value is None:
            prefs.pop(key, None)
        else:
            prefs[key] = value


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

    def preferences(self, owner: str) -> dict[str, str]:
        with self._connect() as con:
            rows = con.execute(
                "SELECT key, value FROM ui_preference WHERE owner = ? ORDER BY key", (owner,)
            ).fetchall()
        return {r["key"]: r["value"] for r in rows}

    def set_preference(self, owner: str, key: str, value: str | None) -> None:
        with self._connect() as con:
            if value is None:
                con.execute("DELETE FROM ui_preference WHERE owner = ? AND key = ?", (owner, key))
                return
            count = con.execute(
                "SELECT COUNT(*) FROM ui_preference WHERE owner = ? AND key != ?", (owner, key)
            ).fetchone()[0]
            if count >= PREFERENCE_MAX_KEYS:
                raise ValueError("too many preferences")
            con.execute(
                "INSERT INTO ui_preference(owner, key, value, updated_at) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(owner, key) DO UPDATE SET value = excluded.value, "
                "updated_at = excluded.updated_at",
                (owner, key, value, datetime.now(UTC).isoformat()),
            )
