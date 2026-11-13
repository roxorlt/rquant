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
    upstream_json TEXT NOT NULL DEFAULT '{}',
    blocks_json TEXT NOT NULL DEFAULT '[]',
    saved_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS screener_custom_block (
    block_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    kind TEXT NOT NULL,
    spec_json TEXT NOT NULL,
    created_at TEXT NOT NULL
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
CUSTOM_BLOCKS_MAX = 200


def preference_key_ok(key: str) -> bool:
    return bool(PREFERENCE_KEY.match(key))


class SchemeStore(Protocol):
    def list(self) -> list[dict[str, Any]]: ...

    def save(
        self,
        name: str,
        filters: list[Any],
        ranks: list[Any],
        upstream: dict[str, Any] | None = None,
        blocks: list[Any] | None = None,
    ) -> dict[str, Any]: ...

    def delete(self, name: str) -> None: ...

    def list_blocks(self) -> list[dict[str, Any]]: ...

    def save_block(
        self,
        block_id: str,
        name: str,
        description: str,
        kind: str,
        spec: dict[str, Any],
    ) -> dict[str, Any]: ...

    def delete_block(self, block_id: str) -> None: ...

    def preferences(self, owner: str) -> dict[str, str]: ...

    def set_preference(self, owner: str, key: str, value: str | None) -> None: ...


def _row(
    name: str,
    filters: list[Any],
    ranks: list[Any],
    saved_at: str,
    upstream: dict[str, Any] | None = None,
    blocks: list[Any] | None = None,
) -> dict[str, Any]:
    return {
        "name": name,
        "filters": filters,
        "ranks": ranks,
        "upstream": upstream or {},
        "blocks": blocks or [],
        "saved_at": saved_at,
    }


def _block_row(
    block_id: str,
    name: str,
    description: str,
    kind: str,
    spec: dict[str, Any],
    created_at: str,
) -> dict[str, Any]:
    return {
        "block_id": block_id,
        "name": name,
        "description": description,
        "kind": kind,
        "spec": spec,
        "created_at": created_at,
    }


class MemorySchemeStore:
    def __init__(self) -> None:
        self._rows: dict[str, dict[str, Any]] = {}
        self._blocks: dict[str, dict[str, Any]] = {}
        self._prefs: dict[str, dict[str, str]] = {}

    def list(self) -> list[dict[str, Any]]:
        return [self._rows[name] for name in sorted(self._rows)]

    def save(
        self,
        name: str,
        filters: list[Any],
        ranks: list[Any],
        upstream: dict[str, Any] | None = None,
        blocks: list[Any] | None = None,
    ) -> dict[str, Any]:
        saved = datetime.now(UTC).isoformat()
        row = _row(name, filters, ranks, saved, upstream=upstream, blocks=blocks)
        self._rows[name] = row
        return row

    def delete(self, name: str) -> None:
        self._rows.pop(name, None)

    def list_blocks(self) -> list[dict[str, Any]]:
        return sorted(self._blocks.values(), key=lambda b: b["created_at"], reverse=True)

    def save_block(
        self,
        block_id: str,
        name: str,
        description: str,
        kind: str,
        spec: dict[str, Any],
    ) -> dict[str, Any]:
        if block_id not in self._blocks and len(self._blocks) >= CUSTOM_BLOCKS_MAX:
            raise ValueError("too many custom blocks")
        created_at = self._blocks.get(block_id, {}).get(
            "created_at", datetime.now(UTC).isoformat()
        )
        row = _block_row(block_id, name, description, kind, spec, created_at)
        self._blocks[block_id] = row
        return row

    def delete_block(self, block_id: str) -> None:
        self._blocks.pop(block_id, None)

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
            cols = {
                str(r["name"])
                for r in con.execute("PRAGMA table_info(screener_scheme)").fetchall()
            }
            if "upstream_json" not in cols:
                con.execute(
                    "ALTER TABLE screener_scheme "
                    "ADD COLUMN upstream_json TEXT NOT NULL DEFAULT '{}'"
                )
            if "blocks_json" not in cols:
                con.execute(
                    "ALTER TABLE screener_scheme "
                    "ADD COLUMN blocks_json TEXT NOT NULL DEFAULT '[]'"
                )

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.path, timeout=5.0)
        con.row_factory = sqlite3.Row
        return con

    def list(self) -> list[dict[str, Any]]:
        with self._connect() as con:
            rows = con.execute(
                "SELECT name, filters_json, ranks_json, upstream_json, blocks_json, saved_at "
                "FROM screener_scheme ORDER BY name"
            ).fetchall()
        return [
            _row(
                r["name"],
                json.loads(r["filters_json"]),
                json.loads(r["ranks_json"]),
                r["saved_at"],
                upstream=json.loads(r["upstream_json"] or "{}"),
                blocks=json.loads(r["blocks_json"] or "[]"),
            )
            for r in rows
        ]

    def save(
        self,
        name: str,
        filters: list[Any],
        ranks: list[Any],
        upstream: dict[str, Any] | None = None,
        blocks: list[Any] | None = None,
    ) -> dict[str, Any]:
        saved = datetime.now(UTC).isoformat()
        upstream_val = upstream or {}
        blocks_val = blocks or []
        with self._connect() as con:
            con.execute(
                "INSERT INTO screener_scheme("
                "name, filters_json, ranks_json, upstream_json, blocks_json, saved_at"
                ") VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(name) DO UPDATE SET "
                "filters_json = excluded.filters_json, "
                "ranks_json = excluded.ranks_json, "
                "upstream_json = excluded.upstream_json, "
                "blocks_json = excluded.blocks_json, "
                "saved_at = excluded.saved_at",
                (
                    name,
                    json.dumps(filters, ensure_ascii=False),
                    json.dumps(ranks, ensure_ascii=False),
                    json.dumps(upstream_val, ensure_ascii=False),
                    json.dumps(blocks_val, ensure_ascii=False),
                    saved,
                ),
            )
        return _row(name, filters, ranks, saved, upstream=upstream_val, blocks=blocks_val)

    def delete(self, name: str) -> None:
        with self._connect() as con:
            con.execute("DELETE FROM screener_scheme WHERE name = ?", (name,))

    def list_blocks(self) -> list[dict[str, Any]]:
        with self._connect() as con:
            rows = con.execute(
                "SELECT block_id, name, description, kind, spec_json, created_at "
                "FROM screener_custom_block ORDER BY created_at DESC, name ASC"
            ).fetchall()
        return [
            _block_row(
                r["block_id"],
                r["name"],
                r["description"],
                r["kind"],
                json.loads(r["spec_json"] or "{}"),
                r["created_at"],
            )
            for r in rows
        ]

    def save_block(
        self,
        block_id: str,
        name: str,
        description: str,
        kind: str,
        spec: dict[str, Any],
    ) -> dict[str, Any]:
        now = datetime.now(UTC).isoformat()
        with self._connect() as con:
            count = con.execute(
                "SELECT COUNT(*) FROM screener_custom_block WHERE block_id != ?",
                (block_id,),
            ).fetchone()[0]
            if count >= CUSTOM_BLOCKS_MAX:
                raise ValueError("too many custom blocks")
            existing = con.execute(
                "SELECT created_at FROM screener_custom_block WHERE block_id = ?",
                (block_id,),
            ).fetchone()
            created_at = str(existing["created_at"]) if existing else now
            con.execute(
                "INSERT INTO screener_custom_block("
                "block_id, name, description, kind, spec_json, created_at"
                ") VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(block_id) DO UPDATE SET "
                "name = excluded.name, "
                "description = excluded.description, "
                "kind = excluded.kind, "
                "spec_json = excluded.spec_json",
                (
                    block_id,
                    name,
                    description,
                    kind,
                    json.dumps(spec, ensure_ascii=False),
                    created_at,
                ),
            )
        return _block_row(block_id, name, description, kind, spec, created_at)

    def delete_block(self, block_id: str) -> None:
        with self._connect() as con:
            con.execute("DELETE FROM screener_custom_block WHERE block_id = ?", (block_id,))

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
