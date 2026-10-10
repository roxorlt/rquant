"""Read one owner's manual watchlist from one verified Serving generation."""

from __future__ import annotations

from datetime import datetime

from rquant.manual_watchlist import MAX_ACTIVE_MEMBERS
from rquant.runtime_contracts import normalize_aware_utc
from rquant.serving_manual_watchlist_projection import (
    ManualWatchlistProjectionRow,
)
from rquant.serving_manual_watchlist_read import read_manual_watchlist_authority
from rquant.web.serving import BorrowedGeneration

_COLUMNS = "owner_id, ts_code, version, deleted, source, price_levels_json, expires_at, updated_at"


def _authority_time(borrowed: BorrowedGeneration) -> datetime | None:
    authority = read_manual_watchlist_authority(borrowed.manifest, borrowed.cursor)
    return None if authority is None else authority.available_at


def read_manual_watchlist(
    borrowed: BorrowedGeneration,
    *,
    owner_id: str,
    now: datetime,
) -> tuple[datetime | None, tuple[ManualWatchlistProjectionRow, ...]]:
    available_at = _authority_time(borrowed)
    if available_at is None:
        return None, ()
    rows = borrowed.cursor.execute(
        f"SELECT {_COLUMNS} FROM manual_watchlist "
        "WHERE owner_id = ? AND deleted = FALSE AND (expires_at IS NULL OR expires_at > ?) "
        "ORDER BY ts_code LIMIT ?",
        (owner_id, normalize_aware_utc(now), MAX_ACTIVE_MEMBERS + 1),
    ).fetchall()
    if len(rows) > MAX_ACTIVE_MEMBERS:
        raise ValueError("manual watchlist exceeds active member limit")
    members = tuple(
        ManualWatchlistProjectionRow.model_validate(
            dict(zip(_COLUMNS.split(", "), row, strict=True))
        )
        for row in rows
    )
    if any(row.owner_id != owner_id or row.deleted for row in members):
        raise ValueError("manual watchlist owner filter failed")
    return available_at, members


def read_manual_watchlist_item(
    borrowed: BorrowedGeneration,
    *,
    owner_id: str,
    ts_code: str,
) -> tuple[datetime | None, ManualWatchlistProjectionRow | None]:
    available_at = _authority_time(borrowed)
    if available_at is None:
        return None, None
    rows = borrowed.cursor.execute(
        f"SELECT {_COLUMNS} FROM manual_watchlist WHERE owner_id = ? AND ts_code = ? LIMIT 2",
        (owner_id, ts_code),
    ).fetchall()
    if len(rows) > 1:
        raise ValueError("manual watchlist identity is not unique")
    item = (
        ManualWatchlistProjectionRow.model_validate(
            dict(zip(_COLUMNS.split(", "), rows[0], strict=True))
        )
        if rows
        else None
    )
    if item is not None and (item.owner_id != owner_id or item.ts_code != ts_code):
        raise ValueError("manual watchlist identity filter failed")
    return available_at, item
