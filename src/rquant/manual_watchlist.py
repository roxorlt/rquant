"""Transaction-owned manual watchlist state for future PageControl commands."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Literal, Self

from pydantic import BeforeValidator, Field, StrictInt, StringConstraints, model_validator

from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, normalize_aware_utc


def _require_canonical_owner(value: object) -> object:
    if isinstance(value, str) and value != value.strip():
        raise ValueError("owner identity must not contain surrounding whitespace")
    return value


OwnerId = Annotated[
    str,
    StringConstraints(min_length=1, max_length=128),
    BeforeValidator(_require_canonical_owner),
]
TsCode = Annotated[str, StringConstraints(pattern=r"^[0-9]{6}\.(?:SH|SZ|BJ)$")]
PriceLevel = Annotated[Decimal, Field(gt=0, allow_inf_nan=False)]
MAX_ACTIVE_MEMBERS = 500

_COLUMNS = (
    "owner_id, ts_code, version, deleted, source, price_levels_json, expires_at_utc, updated_at_utc"
)


class WatchlistSource(StrEnum):
    DETAIL = "detail"
    SCREEN_RESULT = "screen_result"
    POOL_MEMBER = "pool_member"


class WatchlistVersionConflictError(RuntimeError):
    """The supplied revision does not identify an applicable current state."""


class WatchlistCapacityError(RuntimeError):
    """The owner already has 500 effective active members."""


class WatchlistTransactionError(RuntimeError):
    """Writes require the caller's existing SQLite transaction."""


class ManualWatchlistKey(RuntimeContractModel):
    owner_id: OwnerId
    ts_code: TsCode


class ManualWatchlistUpsert(ManualWatchlistKey):
    expected_version: StrictInt | None = Field(default=None, ge=1)
    source: WatchlistSource
    price_levels: tuple[PriceLevel, ...] = Field(default=(), max_length=8)
    expires_at: AwareUtcDatetime | None = None

    @model_validator(mode="after")
    def require_ascending_distinct_levels(self) -> Self:
        if any(
            left >= right
            for left, right in zip(self.price_levels, self.price_levels[1:], strict=False)
        ):
            raise ValueError("price levels must be strictly ascending and distinct")
        return self


class ManualWatchlistDelete(ManualWatchlistKey):
    expected_version: StrictInt = Field(ge=1)


class ManualWatchlistScan(RuntimeContractModel):
    owner_id: OwnerId
    now: AwareUtcDatetime
    limit: StrictInt = Field(default=100, ge=1, le=MAX_ACTIVE_MEMBERS)
    after_ts_code: TsCode | None = None


class ManualWatchlistEntry(ManualWatchlistKey):
    version: StrictInt = Field(ge=1)
    status: Literal["active", "expired", "deleted"]
    source: WatchlistSource | None
    price_levels: tuple[PriceLevel, ...]
    expires_at: AwareUtcDatetime | None
    updated_at: AwareUtcDatetime | None


class ManualWatchlistPage(RuntimeContractModel):
    entries: tuple[ManualWatchlistEntry, ...]
    next_after_ts_code: TsCode | None


class ManualWatchlistRepository:
    """Use one borrowed connection; the caller owns BEGIN IMMEDIATE and commit."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def _require_transaction(self) -> None:
        if not self._connection.in_transaction:
            raise WatchlistTransactionError("manual watchlist writes require caller transaction")

    def install_schema(self) -> None:
        self._require_transaction()
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS manual_watchlist (
                owner_id TEXT NOT NULL,
                ts_code TEXT NOT NULL,
                version INTEGER NOT NULL CHECK (version >= 1),
                deleted INTEGER NOT NULL CHECK (deleted IN (0, 1)),
                source TEXT,
                price_levels_json TEXT NOT NULL,
                expires_at_utc TEXT,
                updated_at_utc TEXT,
                PRIMARY KEY (owner_id, ts_code),
                CHECK (
                    (deleted = 1 AND source IS NULL AND price_levels_json = '[]'
                     AND expires_at_utc IS NULL AND updated_at_utc IS NULL)
                    OR
                    (deleted = 0 AND source IN ('detail', 'screen_result', 'pool_member')
                     AND updated_at_utc IS NOT NULL)
                )
            )
            """
        )

    @staticmethod
    def _read_entry(row: tuple[object, ...], *, now: datetime) -> ManualWatchlistEntry:
        owner_id, ts_code, version, deleted, source, levels_json, expires_at, updated_at = row
        expiry = datetime.fromisoformat(expires_at) if isinstance(expires_at, str) else None
        status: Literal["active", "expired", "deleted"]
        if deleted:
            status = "deleted"
        elif expiry is not None and expiry <= now:
            status = "expired"
        else:
            status = "active"
        return ManualWatchlistEntry(
            owner_id=owner_id,
            ts_code=ts_code,
            version=version,
            status=status,
            source=source,
            price_levels=tuple(Decimal(value) for value in json.loads(levels_json)),
            expires_at=expiry,
            updated_at=datetime.fromisoformat(updated_at) if isinstance(updated_at, str) else None,
        )

    def get(self, key: ManualWatchlistKey, *, now: datetime) -> ManualWatchlistEntry | None:
        observed_at = normalize_aware_utc(now)
        row = self._connection.execute(
            f"SELECT {_COLUMNS} FROM manual_watchlist WHERE owner_id = ? AND ts_code = ?",
            (key.owner_id, key.ts_code),
        ).fetchone()
        return self._read_entry(row, now=observed_at) if row is not None else None

    def scan(self, request: ManualWatchlistScan) -> ManualWatchlistPage:
        rows = self._connection.execute(
            f"SELECT {_COLUMNS} FROM manual_watchlist "
            "WHERE owner_id = ? AND deleted = 0 AND ts_code > ? "
            "ORDER BY ts_code LIMIT ?",
            (request.owner_id, request.after_ts_code or "", request.limit + 1),
        ).fetchall()
        entries = tuple(self._read_entry(row, now=request.now) for row in rows[: request.limit])
        next_after = entries[-1].ts_code if len(rows) > request.limit else None
        return ManualWatchlistPage(entries=entries, next_after_ts_code=next_after)

    def _active_count(self, owner_id: str, *, now: datetime) -> int:
        row = self._connection.execute(
            "SELECT COUNT(*) FROM manual_watchlist WHERE owner_id = ? AND deleted = 0 "
            "AND (expires_at_utc IS NULL OR expires_at_utc > ?)",
            (owner_id, now.isoformat(timespec="microseconds")),
        ).fetchone()
        assert row is not None
        return int(row[0])

    def upsert(self, command: ManualWatchlistUpsert, *, now: datetime) -> ManualWatchlistEntry:
        self._require_transaction()
        observed_at = normalize_aware_utc(now)
        key = ManualWatchlistKey(owner_id=command.owner_id, ts_code=command.ts_code)
        current = self.get(key, now=observed_at)
        if current is None:
            if command.expected_version is not None:
                raise WatchlistVersionConflictError("manual watchlist head does not exist")
            next_version = 1
        elif command.expected_version != current.version:
            raise WatchlistVersionConflictError("manual watchlist version does not match")
        else:
            next_version = current.version + 1

        becomes_active = command.expires_at is None or command.expires_at > observed_at
        if (
            becomes_active
            and (current is None or current.status != "active")
            and self._active_count(command.owner_id, now=observed_at) >= MAX_ACTIVE_MEMBERS
        ):
            raise WatchlistCapacityError("manual watchlist effective member limit reached")

        levels_json = json.dumps(
            [str(value) for value in command.price_levels], separators=(",", ":")
        )
        expires_at = (
            command.expires_at.isoformat(timespec="microseconds")
            if command.expires_at is not None
            else None
        )
        updated_at = observed_at.isoformat(timespec="microseconds")
        if current is None:
            self._connection.execute(
                "INSERT INTO manual_watchlist "
                "(owner_id, ts_code, version, deleted, source, price_levels_json, "
                "expires_at_utc, updated_at_utc) VALUES (?, ?, ?, 0, ?, ?, ?, ?)",
                (
                    command.owner_id,
                    command.ts_code,
                    next_version,
                    command.source.value,
                    levels_json,
                    expires_at,
                    updated_at,
                ),
            )
        else:
            result = self._connection.execute(
                "UPDATE manual_watchlist SET version = ?, deleted = 0, source = ?, "
                "price_levels_json = ?, expires_at_utc = ?, updated_at_utc = ? "
                "WHERE owner_id = ? AND ts_code = ? AND version = ?",
                (
                    next_version,
                    command.source.value,
                    levels_json,
                    expires_at,
                    updated_at,
                    command.owner_id,
                    command.ts_code,
                    current.version,
                ),
            )
            if result.rowcount != 1:
                raise WatchlistVersionConflictError("manual watchlist version changed")
        written = self.get(key, now=observed_at)
        assert written is not None
        return written

    def delete(self, command: ManualWatchlistDelete, *, now: datetime) -> ManualWatchlistEntry:
        self._require_transaction()
        observed_at = normalize_aware_utc(now)
        key = ManualWatchlistKey(owner_id=command.owner_id, ts_code=command.ts_code)
        current = self.get(key, now=observed_at)
        if (
            current is None
            or current.status != "active"
            or command.expected_version != current.version
        ):
            raise WatchlistVersionConflictError("manual watchlist active version does not match")
        result = self._connection.execute(
            "UPDATE manual_watchlist SET version = ?, deleted = 1, source = NULL, "
            "price_levels_json = '[]', expires_at_utc = NULL, updated_at_utc = NULL "
            "WHERE owner_id = ? AND ts_code = ? AND version = ? AND deleted = 0 "
            "AND (expires_at_utc IS NULL OR expires_at_utc > ?)",
            (
                current.version + 1,
                command.owner_id,
                command.ts_code,
                current.version,
                observed_at.isoformat(timespec="microseconds"),
            ),
        )
        if result.rowcount != 1:
            raise WatchlistVersionConflictError("manual watchlist active version changed")
        written = self.get(key, now=observed_at)
        assert written is not None
        return written
