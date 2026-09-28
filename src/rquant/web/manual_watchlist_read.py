"""Read one owner's manual watchlist from one verified Serving generation."""

from __future__ import annotations

import re
from datetime import datetime

from rquant.manual_watchlist import MAX_ACTIVE_MEMBERS
from rquant.runtime_contracts import normalize_aware_utc
from rquant.serving_manual_watchlist_projection import (
    MAX_MANUAL_WATCHLIST_ROWS,
    ManualWatchlistProjectionRow,
)
from rquant.web.serving import BorrowedGeneration

_TABLES = ("manual_watchlist", "manual_watchlist_state")
_COLUMNS = "owner_id, ts_code, version, deleted, source, price_levels_json, expires_at, updated_at"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


def _authority_time(borrowed: BorrowedGeneration) -> datetime | None:
    """Only a paired, published state can authorize membership or absence."""
    marks = borrowed.cursor.execute(
        "SELECT table_name, available, row_count, owner_dataset_id, "
        "owner_generation_id, available_at FROM projection_status "
        "WHERE table_name IN (?, ?) ORDER BY table_name LIMIT 3",
        _TABLES,
    ).fetchall()
    if len(marks) != 2 or tuple(mark[0] for mark in marks) != _TABLES:
        raise ValueError("manual watchlist projection status is incomplete")
    by_name = {mark[0]: mark for mark in marks}
    source_generation = borrowed.manifest.source_generations.get("signals")
    watermark = next(
        (mark for mark in borrowed.manifest.watermarks if mark.dataset_id == "signals"), None
    )
    if (
        source_generation is None
        or watermark is None
        or watermark.generation_id != source_generation
    ):
        raise ValueError("manual watchlist source generation is missing")
    for name, available, count, owner, generation, at in marks:
        if (
            type(available) is not bool
            or type(count) is not int
            or owner != "signals"
            or count != borrowed.manifest.row_counts.get(name)
            or not 0 <= count <= MAX_MANUAL_WATCHLIST_ROWS
        ):
            raise ValueError("manual watchlist manifest and status differ")
        if available:
            if generation != source_generation or not isinstance(at, datetime):
                raise ValueError("manual watchlist source is inconsistent")
            if normalize_aware_utc(at) > borrowed.manifest.built_at:
                raise ValueError("manual watchlist source time is in the future")
        elif generation is not None or at is not None or count != 0:
            raise ValueError("unpublished manual watchlist has rows")

    state_mark, members_mark = by_name["manual_watchlist_state"], by_name["manual_watchlist"]
    if not state_mark[1]:
        if members_mark[1]:
            raise ValueError("manual watchlist rows lack authority")
        return None
    if state_mark[2] != 1:
        raise ValueError("manual watchlist state is not singular")
    raw = borrowed.cursor.execute(
        "SELECT snapshot_key, state, activated_at, row_count, rows_sha256 "
        "FROM manual_watchlist_state LIMIT 2"
    ).fetchall()
    if len(raw) != 1 or raw[0][0] != "current":
        raise ValueError("manual watchlist state is malformed")
    _key, state, activated_at, row_count, digest = raw[0]
    if state == "unavailable":
        if members_mark[1] or any(value is not None for value in (activated_at, row_count, digest)):
            raise ValueError("unavailable manual watchlist carries facts")
        return None
    if state != "ready" or not members_mark[1]:
        raise ValueError("ready manual watchlist lacks paired members")
    if (
        state_mark[5] != members_mark[5]
        or type(row_count) is not int
        or row_count != members_mark[2]
        or not isinstance(digest, str)
        or _SHA256.fullmatch(digest) is None
        or not isinstance(activated_at, datetime)
        or normalize_aware_utc(activated_at) > normalize_aware_utc(state_mark[5])
    ):
        raise ValueError("manual watchlist state and members disagree")
    return normalize_aware_utc(state_mark[5])


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
