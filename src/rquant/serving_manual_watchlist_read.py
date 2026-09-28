"""Verified Serving manual-watchlist facts for Web and future alert scope readers."""

from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Literal

import duckdb
from pydantic import Field, StrictInt

from rquant.manual_watchlist import MAX_ACTIVE_MEMBERS
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, normalize_aware_utc
from rquant.serving_contracts import ServingGenerationManifest
from rquant.serving_manual_watchlist_projection import (
    MAX_MANUAL_WATCHLIST_ROWS,
    ManualWatchlistAuthoritySnapshot,
    ManualWatchlistProjectionRow,
)
from rquant.serving_publisher import ServingGenerationLease

_TABLES = ("manual_watchlist", "manual_watchlist_state")
_COLUMNS = "owner_id, ts_code, version, deleted, source, price_levels_json, expires_at, updated_at"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class ManualWatchlistAuthorityEvidence(RuntimeContractModel):
    source_generation_id: str
    available_at: AwareUtcDatetime
    activated_at: AwareUtcDatetime
    row_count: StrictInt = Field(ge=0, le=MAX_MANUAL_WATCHLIST_ROWS)
    rows_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class ReadyManualAlertScope(RuntimeContractModel):
    availability: Literal["ready"] = "ready"
    generation_id: str
    source_generation_id: str
    evaluated_at: AwareUtcDatetime
    available_at: AwareUtcDatetime
    members: tuple[ManualWatchlistProjectionRow, ...]


class UnavailableManualAlertScope(RuntimeContractModel):
    availability: Literal["unavailable"] = "unavailable"
    generation_id: str | None
    evaluated_at: AwareUtcDatetime


ManualAlertScope = ReadyManualAlertScope | UnavailableManualAlertScope


def read_manual_watchlist_authority(
    manifest: ServingGenerationManifest,
    cursor: duckdb.DuckDBPyConnection,
) -> ManualWatchlistAuthorityEvidence | None:
    """Require the paired state and member projections from this one verified generation."""
    marks = cursor.execute(
        "SELECT table_name, available, row_count, owner_dataset_id, "
        "owner_generation_id, available_at FROM projection_status "
        "WHERE table_name IN (?, ?) ORDER BY table_name LIMIT 3",
        _TABLES,
    ).fetchall()
    if len(marks) != 2 or tuple(mark[0] for mark in marks) != _TABLES:
        raise ValueError("manual watchlist projection status is incomplete")
    by_name = {mark[0]: mark for mark in marks}
    source_generation = manifest.source_generations.get("signals")
    watermark = next((mark for mark in manifest.watermarks if mark.dataset_id == "signals"), None)
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
            or count != manifest.row_counts.get(name)
            or not 0 <= count <= MAX_MANUAL_WATCHLIST_ROWS
        ):
            raise ValueError("manual watchlist manifest and status differ")
        if available:
            if generation != source_generation or not isinstance(at, datetime):
                raise ValueError("manual watchlist source is inconsistent")
            if normalize_aware_utc(at) > manifest.built_at:
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
    raw = cursor.execute(
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
    return ManualWatchlistAuthorityEvidence(
        source_generation_id=source_generation,
        available_at=state_mark[5],
        activated_at=activated_at,
        row_count=row_count,
        rows_sha256=digest,
    )


def read_manual_alert_scope(
    lease: ServingGenerationLease | None,
    *,
    evaluated_at: datetime,
    max_generation_age: timedelta,
) -> ManualAlertScope:
    """Return active owner identities from one current-pointer lease, or no usable scope.

    The caller must reacquire the current Serving lease for each later evaluation;
    holding this immutable lease does not follow a newly published pointer.
    """
    now = normalize_aware_utc(evaluated_at)
    if max_generation_age <= timedelta(0):
        raise ValueError("max_generation_age must be positive")
    generation_id = None if lease is None else lease.manifest.generation_id

    def unavailable() -> UnavailableManualAlertScope:
        return UnavailableManualAlertScope(generation_id=generation_id, evaluated_at=now)

    if lease is None:
        return unavailable()
    try:
        manifest = lease.manifest
        if (
            lease.closed
            or lease.pointer is None
            or lease.pointer.generation_id != generation_id
            or manifest.built_at > now
            or now - manifest.built_at > max_generation_age
        ):
            return unavailable()
        cursor = lease.connection.cursor()
        try:
            authority = read_manual_watchlist_authority(manifest, cursor)
            if authority is None:
                return unavailable()
            rows = cursor.execute(
                f"SELECT {_COLUMNS} FROM manual_watchlist ORDER BY owner_id, ts_code LIMIT ?",
                (MAX_MANUAL_WATCHLIST_ROWS + 1,),
            ).fetchall()
        finally:
            cursor.close()
        members = tuple(
            ManualWatchlistProjectionRow.model_validate(
                dict(zip(_COLUMNS.split(", "), row, strict=True))
            )
            for row in rows
        )
        snapshot = ManualWatchlistAuthoritySnapshot(
            activated_at=authority.activated_at,
            rows=members,
            row_count=authority.row_count,
            rows_sha256=authority.rows_sha256,
        )
        if any(
            row.updated_at is not None and row.updated_at > authority.available_at
            for row in snapshot.rows
        ):
            raise ValueError("manual watchlist source time precedes a row")
        active = tuple(
            row
            for row in snapshot.rows
            if not row.deleted and (row.expires_at is None or row.expires_at > now)
        )
        counts: dict[str, int] = {}
        for row in active:
            counts[row.owner_id] = counts.get(row.owner_id, 0) + 1
            if counts[row.owner_id] > MAX_ACTIVE_MEMBERS:
                raise ValueError("manual watchlist exceeds owner capacity")
        return ReadyManualAlertScope(
            generation_id=generation_id,
            source_generation_id=authority.source_generation_id,
            evaluated_at=now,
            available_at=authority.available_at,
            members=active,
        )
    except Exception:
        return unavailable()
