"""One bounded acknowledgment read model for the timeline and overview."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import duckdb
from pydantic import ValidationError

from rquant.alert_ack import AlertSource, alert_window_start
from rquant.page_control import AlertAcknowledgment
from rquant.runtime_contracts import canonical_sha256
from rquant.serving_alert_projection import AlertAckAuthoritySnapshot
from rquant.web import readers
from rquant.web.envelope import ServingMeta, ServingState
from rquant.web.models.alert_ack import AlertAcknowledgmentView, UnacknowledgedSummary
from rquant.web.serving import BorrowedGeneration

_SOURCES: frozenset[str] = frozenset({"signal", "monitor_event", "surge_event"})
_SHA = frozenset("0123456789abcdef")
_MAX_EVENTS = 20_000
_MAX_ACKS = 10_000
_UNAVAILABLE = UnacknowledgedSummary(
    state="unavailable",
    count=None,
    count_as_of=None,
    label="确认状态暂不可用",
    note="确认信息尚未发布，请稍后查看。",
)
_INCOMPLETE = UnacknowledgedSummary(
    state="source_incomplete",
    count=None,
    count_as_of=None,
    label="待确认数暂不可用",
    note="告警来源尚未核对完整。",
)


def _utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("alert time is naive")
    return value.astimezone(UTC)


def _sha(value: str) -> bool:
    return len(value) == 64 and set(value) <= _SHA


@dataclass(frozen=True)
class _Event:
    source: AlertSource
    alert_id: str
    occurred_at: datetime
    confirmation_id: str | None
    confirmed_at: datetime | None
    eligible: bool


@dataclass(frozen=True)
class _Coverage:
    source: AlertSource
    state: str
    window_start: datetime | None
    window_end: datetime | None
    count_as_of: datetime | None
    source_generation_id: str | None
    high_watermark: str | None
    row_count: int
    row_digest: str


@dataclass(frozen=True)
class AlertReadModel:
    summary: UnacknowledgedSummary
    activated_at: datetime | None
    events: dict[tuple[AlertSource, str], _Event]
    acknowledgments: dict[str, AlertAcknowledgment]

    def status_for(self, source: AlertSource, alert_id: str) -> AlertAcknowledgmentView:
        event = self.events.get((source, alert_id))
        if event is None:
            return AlertAcknowledgmentView(
                state="unavailable",
                eligible=False,
                label="确认状态暂不可用",
                note="这条告警尚未核对。",
            )
        acknowledged = self.acknowledgments.get(alert_id)
        if acknowledged is not None and event.confirmation_id == acknowledged.confirmation_id:
            return AlertAcknowledgmentView(
                alert_id=alert_id,
                state="confirmed",
                eligible=False,
                confirmation_id=acknowledged.confirmation_id,
                confirmed_at=acknowledged.confirmed_at,
                label="已确认",
            )
        if self.activated_at is not None and event.occurred_at < self.activated_at:
            return AlertAcknowledgmentView(
                alert_id=alert_id,
                state="historical",
                eligible=False,
                label="历史告警",
                note="这条告警早于确认功能启用时间。",
            )
        if (
            self.summary.state == "ready"
            and self.summary.count_as_of is not None
            and self.activated_at is not None
            and alert_window_start(
                count_as_of=self.summary.count_as_of, activated_at=self.activated_at
            )
            <= event.occurred_at
            <= self.summary.count_as_of
            and event.eligible
        ):
            return AlertAcknowledgmentView(
                alert_id=alert_id,
                state="unconfirmed",
                eligible=True,
                label="待确认",
            )
        return AlertAcknowledgmentView(
            alert_id=alert_id,
            state="unavailable",
            eligible=False,
            label="确认状态暂不可用",
            note="告警来源尚未核对完整。",
        )


def _empty() -> AlertReadModel:
    return AlertReadModel(_UNAVAILABLE, None, {}, {})


def _present(states: dict[str, readers.TableState], name: str) -> bool:
    state = states.get(name)
    return state is not None and state.available


def _read_rows(cursor: Any, query: str, limit: int) -> list[tuple[Any, ...]]:
    rows = cursor.execute(query, (limit + 1,)).fetchall()
    if len(rows) > limit:
        raise ValueError("alert projection exceeds bound")
    return rows


def _read_events(cursor: Any) -> dict[tuple[AlertSource, str], _Event]:
    rows = _read_rows(
        cursor,
        "SELECT source, alert_id, occurred_at, confirmation_id, confirmed_at, eligible "
        "FROM alert_event LIMIT ?",
        _MAX_EVENTS,
    )
    result: dict[tuple[AlertSource, str], _Event] = {}
    for source, alert_id, occurred, confirmation, confirmed, eligible in rows:
        if source not in _SOURCES or not isinstance(alert_id, str) or not _sha(alert_id):
            raise ValueError("invalid published alert identity")
        if (confirmation is None) != (confirmed is None) or type(eligible) is not bool:
            raise ValueError("invalid published alert state")
        at = _utc(occurred)
        confirmed_at = _utc(confirmed)
        if at is None or (confirmed_at is not None and confirmed_at < at):
            raise ValueError("invalid published alert time")
        key = (source, alert_id)
        if key in result:
            raise ValueError("duplicate published alert")
        result[key] = _Event(source, alert_id, at, confirmation, confirmed_at, eligible)
    return result


def _read_ack_snapshot(cursor: Any) -> AlertAckAuthoritySnapshot | None:
    state = cursor.execute(
        "SELECT state, activated_at, row_count, rows_sha256 "
        "FROM alert_ack_state WHERE snapshot_key = 'current' LIMIT 2"
    ).fetchall()
    if len(state) != 1 or state[0][0] != "ready":
        return None
    rows = _read_rows(
        cursor,
        "SELECT alert_id, confirmation_id, actor_id, confirmed_at, generation_id "
        "FROM alert_ack ORDER BY alert_id LIMIT ?",
        _MAX_ACKS,
    )
    acknowledgments = tuple(
        AlertAcknowledgment(
            alert_id=alert_id,
            confirmation_id=confirmation_id,
            actor_id=actor_id,
            confirmed_at=confirmed_at,
            generation_id=generation_id,
        )
        for alert_id, confirmation_id, actor_id, confirmed_at, generation_id in rows
    )
    return AlertAckAuthoritySnapshot(
        activated_at=state[0][1],
        rows=acknowledgments,
        row_count=state[0][2],
        rows_sha256=state[0][3],
    )


def _read_coverage(cursor: Any) -> dict[AlertSource, _Coverage]:
    rows = _read_rows(
        cursor,
        "SELECT source, state, window_start, window_end, count_as_of, "
        "source_generation_id, high_watermark, row_count, row_digest "
        "FROM alert_source_coverage LIMIT ?",
        3,
    )
    result: dict[AlertSource, _Coverage] = {}
    for source, state, start, end, cutoff, generation, watermark, count, digest in rows:
        if (
            source not in _SOURCES
            or source in result
            or type(count) is not int
            or count < 0
            or not isinstance(digest, str)
            or not _sha(digest)
        ):
            raise ValueError("invalid alert source coverage")
        result[source] = _Coverage(
            source,
            state,
            _utc(start),
            _utc(end),
            _utc(cutoff),
            generation,
            watermark,
            count,
            digest,
        )
    return result


def _ready_summary(
    *,
    overview: tuple[Any, ...],
    snapshot: AlertAckAuthoritySnapshot,
    coverage: dict[AlertSource, _Coverage],
    events: dict[tuple[AlertSource, str], _Event],
    meta: ServingMeta,
    now: datetime,
    stale_after: timedelta,
) -> UnacknowledgedSummary | None:
    state, count, cutoff, activated = overview
    cutoff = _utc(cutoff)
    activated = _utc(activated)
    if (
        state != "ready"
        or meta.state is not ServingState.READY
        or type(count) is not int
        or count < 0
        or cutoff is None
        or activated != snapshot.activated_at
        or cutoff > now
        or now - cutoff > stale_after
        or set(coverage) != _SOURCES
    ):
        return None
    first = alert_window_start(count_as_of=cutoff, activated_at=snapshot.activated_at)
    for source in _SOURCES:
        proof = coverage[source]
        source_rows = tuple(
            {
                "source": event.source,
                "alert_id": event.alert_id,
                "occurred_at": event.occurred_at.isoformat(),
                "confirmation_id": event.confirmation_id,
                "confirmed_at": (
                    None if event.confirmed_at is None else event.confirmed_at.isoformat()
                ),
                "eligible": event.eligible,
            }
            for event in sorted(
                (item for item in events.values() if item.source == source),
                key=lambda item: item.alert_id,
            )
        )
        if (
            proof.state != "complete"
            or proof.window_start != first
            or proof.window_end != cutoff
            or proof.count_as_of != cutoff
            or not proof.source_generation_id
            or not proof.high_watermark
            or proof.row_count != len(source_rows)
            or proof.row_digest
            != canonical_sha256(
                {"contract": "alert-observed-rows/v1", "source": source, "rows": source_rows}
            )
        ):
            return None
    expected = 0
    for event in events.values():
        if not first <= event.occurred_at <= cutoff:
            continue
        if event.confirmation_id is None:
            if not event.eligible:
                return None
            expected += 1
    if expected != count:
        return None
    return UnacknowledgedSummary(
        state="ready",
        count=count,
        count_as_of=cutoff,
        label=f"{count:,} 条待确认",
        note="已核对全部告警来源。",
    )


def read_alert_ack(
    borrowed: BorrowedGeneration | None,
    *,
    meta: ServingMeta,
    now: datetime,
    stale_after: timedelta,
) -> AlertReadModel:
    """Read all five projections through one borrowed cursor; any gap fails closed."""
    if borrowed is None:
        return _empty()
    try:
        cursor = borrowed.cursor
        states = readers.table_states(cursor)
        required = {
            "alert_event",
            "alert_ack_state",
            "alert_ack",
            "alert_source_coverage",
            "alert_overview",
        }
        if not all(_present(states, name) for name in required):
            return _empty()
        snapshot = _read_ack_snapshot(cursor)
        if snapshot is None:
            return _empty()
        events = _read_events(cursor)
        coverage = _read_coverage(cursor)
        overview_rows = cursor.execute(
            "SELECT state, unacknowledged_count, count_as_of, activated_at "
            "FROM alert_overview WHERE snapshot_key = 'current' LIMIT 2"
        ).fetchall()
        if len(overview_rows) != 1:
            raise ValueError("alert overview projection is incomplete")
        ack_by_id = {row.alert_id: row for row in snapshot.rows}
        for event in events.values():
            acknowledged = ack_by_id.get(event.alert_id)
            if (acknowledged is None and event.confirmation_id is not None) or (
                acknowledged is not None
                and (
                    event.confirmation_id != acknowledged.confirmation_id
                    or event.confirmed_at != acknowledged.confirmed_at
                )
            ):
                raise ValueError("alert acknowledgment projection disagrees with authority")
        ready = _ready_summary(
            overview=overview_rows[0],
            snapshot=snapshot,
            coverage=coverage,
            events=events,
            meta=meta,
            now=now,
            stale_after=stale_after,
        )
        return AlertReadModel(ready or _INCOMPLETE, snapshot.activated_at, events, ack_by_id)
    except (duckdb.Error, TypeError, ValueError, ValidationError):
        return _empty()
