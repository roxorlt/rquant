"""Verify rule and watchlist facts together from one borrowed Serving generation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from rquant.runtime_contracts import normalize_aware_utc
from rquant.serving_manual_watchlist_projection import (
    MAX_MANUAL_WATCHLIST_ROWS,
    ManualWatchlistAuthoritySnapshot,
    ManualWatchlistProjectionRow,
)
from rquant.serving_manual_watchlist_read import read_manual_watchlist_authority
from rquant.serving_price_alert_rule_projection import (
    MAX_PRICE_ALERT_RULE_ROWS,
    PriceAlertRuleAuthoritySnapshot,
    PriceAlertRuleProjectionRow,
)
from rquant.web.serving import BorrowedGeneration

_RULE_COLUMNS = (
    "owner_id, rule_id, version, deleted, ts_code, membership_version, name, priority, "
    "enabled, comparison, threshold, valid_from, valid_until, updated_at"
)
_MEMBER_COLUMNS = (
    "owner_id, ts_code, version, deleted, source, price_levels_json, expires_at, updated_at"
)


@dataclass(frozen=True)
class PriceRuleRead:
    availability: Literal["ready", "not_ready", "unavailable"]
    available_at: datetime | None
    rules: tuple[PriceAlertRuleProjectionRow, ...]
    members: tuple[ManualWatchlistProjectionRow, ...]


def read_price_alert_rules(borrowed: BorrowedGeneration, *, now: datetime) -> PriceRuleRead:
    """Check projection marks and full bounded digests before owner filtering."""
    observed = normalize_aware_utc(now)
    manifest = borrowed.manifest
    cursor = borrowed.cursor
    source_generation = manifest.source_generations.get("signals")
    watermark = next((mark for mark in manifest.watermarks if mark.dataset_id == "signals"), None)
    if (
        source_generation is None
        or watermark is None
        or watermark.generation_id != source_generation
    ):
        raise ValueError("price rule source generation is missing")
    marks = cursor.execute(
        "SELECT table_name, available, row_count, owner_dataset_id, "
        "owner_generation_id, available_at FROM projection_status "
        "WHERE table_name IN ('price_alert_rule', 'price_alert_rule_state') "
        "ORDER BY table_name LIMIT 3"
    ).fetchall()
    if len(marks) != 2 or tuple(mark[0] for mark in marks) != (
        "price_alert_rule",
        "price_alert_rule_state",
    ):
        raise ValueError("price rule projection status is incomplete")
    for name, available, count, owner, generation, available_at in marks:
        if (
            type(available) is not bool
            or type(count) is not int
            or owner != "signals"
            or count != manifest.row_counts.get(name)
            or not 0 <= count <= MAX_PRICE_ALERT_RULE_ROWS
        ):
            raise ValueError("price rule manifest and status differ")
        if available:
            if generation != source_generation or not isinstance(available_at, datetime):
                raise ValueError("price rule source is inconsistent")
            if normalize_aware_utc(available_at) > min(observed, manifest.built_at):
                raise ValueError("price rule source time is in the future")
        elif generation is not None or available_at is not None or count != 0:
            raise ValueError("unpublished price rule projection carries rows")

    facts_mark, state_mark = marks
    if not state_mark[1]:
        if facts_mark[1]:
            raise ValueError("price rule facts lack authority state")
        return PriceRuleRead("unavailable", None, (), ())
    if state_mark[2] != 1:
        raise ValueError("price rule state is unavailable")
    raw_state = cursor.execute(
        "SELECT snapshot_key, state, activated_at, row_count, rows_sha256 "
        "FROM price_alert_rule_state LIMIT 2"
    ).fetchall()
    if len(raw_state) != 1 or raw_state[0][0] != "current":
        raise ValueError("price rule state is malformed")
    _key, state, activated_at, row_count, digest = raw_state[0]
    if state in {"not_activated", "unavailable"}:
        if facts_mark[1] or any(value is not None for value in (activated_at, row_count, digest)):
            raise ValueError("unready price rule state carries facts")
        if state == "not_activated":
            return PriceRuleRead("not_ready", None, (), ())
        return PriceRuleRead("unavailable", None, (), ())
    if state != "ready" or not facts_mark[1]:
        raise ValueError("ready price rule state lacks facts")
    if (
        state_mark[5] != facts_mark[5]
        or type(row_count) is not int
        or row_count != facts_mark[2]
        or not isinstance(digest, str)
        or not isinstance(activated_at, datetime)
        or normalize_aware_utc(activated_at) > normalize_aware_utc(state_mark[5])
    ):
        raise ValueError("price rule state and facts disagree")
    member_authority = read_manual_watchlist_authority(manifest, cursor)
    if member_authority is None or member_authority.source_generation_id != source_generation:
        raise ValueError("manual watchlist authority is unavailable")
    if member_authority.available_at > observed:
        raise ValueError("manual watchlist source time is in the future")
    raw_members = cursor.execute(
        f"SELECT {_MEMBER_COLUMNS} FROM manual_watchlist ORDER BY owner_id, ts_code LIMIT ?",
        (MAX_MANUAL_WATCHLIST_ROWS + 1,),
    ).fetchall()
    members = tuple(
        ManualWatchlistProjectionRow.model_validate(
            dict(zip(_MEMBER_COLUMNS.split(", "), row, strict=True))
        )
        for row in raw_members
    )
    ManualWatchlistAuthoritySnapshot(
        activated_at=member_authority.activated_at,
        rows=members,
        row_count=member_authority.row_count,
        rows_sha256=member_authority.rows_sha256,
    )
    if any(
        row.updated_at is not None and row.updated_at > member_authority.available_at
        for row in members
    ):
        raise ValueError("manual watchlist source time precedes a member")
    raw_rules = cursor.execute(
        f"SELECT {_RULE_COLUMNS} FROM price_alert_rule ORDER BY owner_id, rule_id LIMIT ?",
        (MAX_PRICE_ALERT_RULE_ROWS + 1,),
    ).fetchall()
    rules = tuple(
        PriceAlertRuleProjectionRow.model_validate(
            dict(zip(_RULE_COLUMNS.split(", "), row, strict=True))
        )
        for row in raw_rules
    )
    PriceAlertRuleAuthoritySnapshot(
        activated_at=activated_at,
        rows=rules,
        row_count=row_count,
        rows_sha256=digest,
    )
    if any(row.updated_at > state_mark[5] for row in rules):
        raise ValueError("price rule source time precedes a rule")
    return PriceRuleRead(
        "ready",
        max(normalize_aware_utc(state_mark[5]), member_authority.available_at),
        rules,
        members,
    )
