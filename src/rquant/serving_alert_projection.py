"""Bounded alert projections derived from one immutable Serving input."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from typing import Self

from pydantic import Field, StrictInt, model_validator

from rquant.alert_ack import AlertSource, alert_event_at, alert_window_start, stable_alert_id
from rquant.page_control import AlertAcknowledgment
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.serving_read_models import (
    ServingProjectionInput,
    ServingProjectionPayload,
    ServingReadModelInput,
)

_SOURCES: tuple[AlertSource, ...] = ("signal", "monitor_event", "surge_event")
_MAX_ALERT_EVENTS = 20_000
_MAX_ALERT_EVENT_BYTES = 3 * 1024 * 1024
_MAX_ACK_ROWS = 10_000
_UNAVAILABLE_AT = datetime(1970, 1, 1, tzinfo=UTC)


class AlertAckAuthoritySnapshot(RuntimeContractModel):
    """Activation and all confirmed rows from one PageControl read transaction."""

    activated_at: AwareUtcDatetime
    rows: tuple[AlertAcknowledgment, ...] = ()
    row_count: StrictInt = Field(ge=0, le=_MAX_ACK_ROWS)
    rows_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @classmethod
    def create(
        cls,
        *,
        activated_at: datetime,
        rows: Iterable[AlertAcknowledgment],
    ) -> AlertAckAuthoritySnapshot:
        ordered = tuple(sorted(rows, key=lambda row: row.alert_id))
        return cls(
            activated_at=activated_at,
            rows=ordered,
            row_count=len(ordered),
            rows_sha256=cls.digest(ordered),
        )

    @staticmethod
    def digest(rows: Iterable[AlertAcknowledgment]) -> str:
        return canonical_sha256({"contract": "alert-ack-snapshot/v1", "rows": tuple(rows)})

    @model_validator(mode="after")
    def validate_snapshot(self) -> Self:
        if self.row_count != len(self.rows) or self.rows_sha256 != self.digest(self.rows):
            raise ValueError("alert acknowledgment snapshot count or digest mismatch")
        ids = tuple(row.alert_id for row in self.rows)
        confirmations = tuple(row.confirmation_id for row in self.rows)
        if ids != tuple(sorted(ids)) or len(set(ids)) != len(ids):
            raise ValueError("alert acknowledgment identities are not sorted and unique")
        if len(set(confirmations)) != len(confirmations):
            raise ValueError("alert confirmation IDs are not unique")
        for row in self.rows:
            if (
                len(row.alert_id) != 64
                or any(character not in "0123456789abcdef" for character in row.alert_id)
                or len(row.generation_id) != 64
                or any(character not in "0123456789abcdef" for character in row.generation_id)
                or not row.confirmation_id
                or len(row.confirmation_id) > 128
                or not row.actor_id
                or len(row.actor_id) > 256
                or row.confirmed_at < self.activated_at
            ):
                raise ValueError("alert acknowledgment row is invalid")
        return self


def build_ack_source_projections(
    snapshot: AlertAckAuthoritySnapshot | None,
    *,
    observed_at: datetime,
) -> tuple[ServingProjectionPayload, ...]:
    """Publish a usable authority only when activation and the row set were read together."""
    observed = observed_at.astimezone(UTC)
    if snapshot is None:
        state_row: Mapping[str, object] = {
            "snapshot_key": "current",
            "state": "unavailable",
            "activated_at": None,
            "row_count": None,
            "rows_sha256": None,
        }
        return (
            ServingProjectionPayload(
                table_name="alert_ack_state", available_at=_UNAVAILABLE_AT, rows=(state_row,)
            ),
        )
    if snapshot.activated_at > observed or any(
        row.confirmed_at > observed for row in snapshot.rows
    ):
        raise ValueError("alert acknowledgment snapshot contains future evidence")
    available = max((snapshot.activated_at, *(row.confirmed_at for row in snapshot.rows)))
    state = ServingProjectionPayload(
        table_name="alert_ack_state",
        available_at=available,
        rows=(
            {
                "snapshot_key": "current",
                "state": "ready",
                "activated_at": snapshot.activated_at.isoformat(),
                "row_count": snapshot.row_count,
                "rows_sha256": snapshot.rows_sha256,
            },
        ),
    )
    acknowledgments = ServingProjectionPayload(
        table_name="alert_ack",
        available_at=available,
        rows=tuple(
            {
                "alert_id": row.alert_id,
                "confirmation_id": row.confirmation_id,
                "actor_id": row.actor_id,
                "confirmed_at": row.confirmed_at.isoformat(),
                "generation_id": row.generation_id,
            }
            for row in snapshot.rows
        ),
    )
    return state, acknowledgments


def _ack_snapshot_from_input(
    projections: Mapping[str, ServingProjectionInput],
) -> AlertAckAuthoritySnapshot | None:
    status = projections.get("alert_ack_state")
    if status is None:
        return None
    if len(status.rows) != 1:
        raise ValueError("alert acknowledgment state projection is incomplete")
    row = status.rows[0]
    if row["state"] == "unavailable":
        if projections.get("alert_ack") is not None:
            raise ValueError("unavailable alert state carries acknowledgment rows")
        return None
    if row["state"] != "ready":
        raise ValueError("alert acknowledgment state is invalid")
    ack = projections.get("alert_ack")
    if ack is None:
        raise ValueError("ready alert state lacks acknowledgment projection")
    return AlertAckAuthoritySnapshot(
        activated_at=row["activated_at"],
        rows=tuple(AlertAcknowledgment.model_validate(item) for item in ack.rows),
        row_count=row["row_count"],
        rows_sha256=row["rows_sha256"],
    )


def _source_events(
    source: AlertSource,
    *,
    input: ServingReadModelInput,
    projections: Mapping[str, ServingProjectionInput],
    first_at: datetime,
) -> tuple[tuple[dict[str, object], ...], str]:
    if source == "signal":
        events = tuple(record.signal for record in input.signals)
    else:
        projection = projections.get(source)
        if projection is None:
            return (), "source_unpublished"
        events = projection.rows
    selected: list[dict[str, object]] = []
    seen: set[str] = set()
    try:
        for event in events:
            alert_id = stable_alert_id(source, event)
            occurred_at = alert_event_at(source, event)
            if occurred_at > input.observed_at:
                raise ValueError("alert event is from the future")
            if occurred_at < first_at:
                continue
            if alert_id in seen:
                raise ValueError("duplicate alert identity")
            seen.add(alert_id)
            selected.append(
                {
                    "source": source,
                    "alert_id": alert_id,
                    "occurred_at": occurred_at.isoformat(),
                    "confirmation_id": None,
                    "confirmed_at": None,
                    "eligible": False,
                }
            )
            if len(selected) > _MAX_ALERT_EVENTS:
                raise ValueError("alert event source exceeds bound")
    except ValueError:
        return (), "identity_invalid"
    return tuple(sorted(selected, key=lambda row: str(row["alert_id"]))), "coverage_unverified"


def build_alert_read_projections(
    source: ServingReadModelInput,
    *,
    signal_generation_id: str | None = None,
) -> tuple[ServingProjectionPayload, ...]:
    """Create observed alerts and honest unknown coverage from one fixed source input.

    None of the three legacy source producers publishes a positive 30-day window proof.
    Their observed rows are useful timeline evidence, never proof of a complete count.
    """
    by_name = {item.table_name: item for item in source.projections}
    ack = _ack_snapshot_from_input(by_name)
    first_at = alert_window_start(
        count_as_of=source.observed_at,
        activated_at=datetime(1970, 1, 1, tzinfo=UTC),
    )
    window_start = (
        None
        if ack is None
        else alert_window_start(
            count_as_of=source.observed_at, activated_at=ack.activated_at
        ).isoformat()
    )
    acknowledgments = {} if ack is None else {row.alert_id: row for row in ack.rows}
    events: list[dict[str, object]] = []
    coverage: list[dict[str, object]] = []
    for event_source in _SOURCES:
        rows, reason = _source_events(
            event_source, input=source, projections=by_name, first_at=first_at
        )
        for row in rows:
            acknowledgment = acknowledgments.get(str(row["alert_id"]))
            occurred_at = datetime.fromisoformat(str(row["occurred_at"]))
            if (
                acknowledgment is not None
                and ack is not None
                and ack.activated_at <= occurred_at <= acknowledgment.confirmed_at
            ):
                row["confirmation_id"] = acknowledgment.confirmation_id
                row["confirmed_at"] = acknowledgment.confirmed_at.isoformat()
        if rows and (
            len(events) + len(rows) > _MAX_ALERT_EVENTS
            or len(
                json.dumps(
                    (*events, *rows),
                    ensure_ascii=True,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode("utf-8")
            )
            > _MAX_ALERT_EVENT_BYTES
        ):
            rows, reason = (), "event_bound_exceeded"
        events.extend(rows)
        generation_id = (
            signal_generation_id
            if event_source == "signal"
            else by_name[event_source].owner_generation_id
            if event_source in by_name
            else None
        )
        coverage.append(
            {
                "source": event_source,
                "state": "unavailable",
                "reason": reason,
                "window_start": window_start,
                "window_end": None,
                "count_as_of": None,
                "source_generation_id": generation_id,
                "high_watermark": None,
                "row_count": len(rows),
                "row_digest": canonical_sha256(
                    {"contract": "alert-observed-rows/v1", "source": event_source, "rows": rows}
                ),
            }
        )
    events.sort(key=lambda row: (str(row["source"]), str(row["alert_id"])))
    observed = source.observed_at
    return (
        ServingProjectionPayload(
            table_name="alert_event", available_at=observed, rows=tuple(events)
        ),
        ServingProjectionPayload(
            table_name="alert_source_coverage", available_at=observed, rows=tuple(coverage)
        ),
        ServingProjectionPayload(
            table_name="alert_overview",
            available_at=observed,
            rows=(
                {
                    "snapshot_key": "current",
                    "state": "ack_unavailable" if ack is None else "source_incomplete",
                    "unacknowledged_count": None,
                    "count_as_of": None,
                    "activated_at": None if ack is None else ack.activated_at.isoformat(),
                },
            ),
        ),
    )
