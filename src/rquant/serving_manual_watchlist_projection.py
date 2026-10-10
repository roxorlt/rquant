"""Read-only, bounded manual watchlist facts for one Serving generation."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Self

from pydantic import Field, StrictBool, StrictInt, StrictStr, model_validator

from rquant.manual_watchlist import ManualWatchlistUpsert, OwnerId, TsCode, WatchlistSource
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.serving_read_models import ServingProjectionInput, ServingProjectionPayload

MAX_MANUAL_WATCHLIST_ROWS = 10_000
_UNAVAILABLE_AT = datetime(1970, 1, 1, tzinfo=UTC)


class ManualWatchlistProjectionRow(RuntimeContractModel):
    owner_id: OwnerId
    ts_code: TsCode
    version: StrictInt = Field(ge=1)
    deleted: StrictBool
    source: WatchlistSource | None
    price_levels_json: StrictStr = Field(max_length=512)
    expires_at: AwareUtcDatetime | None
    updated_at: AwareUtcDatetime | None

    @model_validator(mode="after")
    def validate_facts(self) -> Self:
        if self.deleted:
            if (
                self.source is not None
                or self.price_levels_json != "[]"
                or self.expires_at is not None
                or self.updated_at is not None
            ):
                raise ValueError("manual watchlist tombstone contains live facts")
            return self
        if self.source is None or self.updated_at is None:
            raise ValueError("live manual watchlist row lacks source or update time")
        try:
            raw_levels = json.loads(self.price_levels_json)
            if not isinstance(raw_levels, list) or any(
                not isinstance(value, str) for value in raw_levels
            ):
                raise ValueError("manual watchlist prices must be decimal strings")
            levels = tuple(Decimal(value) for value in raw_levels)
            ManualWatchlistUpsert(
                owner_id=self.owner_id,
                ts_code=self.ts_code,
                source=self.source,
                price_levels=levels,
                expires_at=self.expires_at,
            )
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ValueError("manual watchlist price levels are invalid") from exc
        if json.dumps(raw_levels, separators=(",", ":")) != self.price_levels_json:
            raise ValueError("manual watchlist price JSON is not canonical")
        return self

    def projection_row(self) -> dict[str, object]:
        return {
            "owner_id": self.owner_id,
            "ts_code": self.ts_code,
            "version": self.version,
            "deleted": self.deleted,
            "source": None if self.source is None else self.source.value,
            "price_levels_json": self.price_levels_json,
            "expires_at": None if self.expires_at is None else self.expires_at.isoformat(),
            "updated_at": None if self.updated_at is None else self.updated_at.isoformat(),
        }


class ManualWatchlistAuthoritySnapshot(RuntimeContractModel):
    activated_at: AwareUtcDatetime
    rows: tuple[ManualWatchlistProjectionRow, ...] = ()
    row_count: StrictInt = Field(ge=0, le=MAX_MANUAL_WATCHLIST_ROWS)
    rows_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @staticmethod
    def digest(rows: Iterable[ManualWatchlistProjectionRow]) -> str:
        return canonical_sha256(
            {
                "contract": "manual-watchlist-snapshot/v1",
                "rows": tuple(row.projection_row() for row in rows),
            }
        )

    @classmethod
    def create(
        cls, *, activated_at: datetime, rows: Iterable[ManualWatchlistProjectionRow]
    ) -> ManualWatchlistAuthoritySnapshot:
        ordered = tuple(sorted(rows, key=lambda row: (row.owner_id, row.ts_code)))
        return cls(
            activated_at=activated_at,
            rows=ordered,
            row_count=len(ordered),
            rows_sha256=cls.digest(ordered),
        )

    @model_validator(mode="after")
    def validate_snapshot(self) -> Self:
        identities = tuple((row.owner_id, row.ts_code) for row in self.rows)
        if identities != tuple(sorted(set(identities))):
            raise ValueError("manual watchlist identities must be sorted and unique")
        if self.row_count != len(self.rows) or self.rows_sha256 != self.digest(self.rows):
            raise ValueError("manual watchlist count or digest mismatch")
        return self


def build_manual_watchlist_projections(
    snapshot: ManualWatchlistAuthoritySnapshot | None,
    *,
    observed_at: datetime,
) -> tuple[ServingProjectionPayload, ...]:
    observed = observed_at.astimezone(UTC)
    if snapshot is None:
        return (
            ServingProjectionPayload(
                table_name="manual_watchlist_state",
                available_at=_UNAVAILABLE_AT,
                rows=(
                    {
                        "snapshot_key": "current",
                        "state": "unavailable",
                        "activated_at": None,
                        "row_count": None,
                        "rows_sha256": None,
                    },
                ),
            ),
        )
    if snapshot.activated_at > observed or any(
        row.updated_at is not None and row.updated_at > observed for row in snapshot.rows
    ):
        raise ValueError("manual watchlist snapshot contains future evidence")
    evidence_times = [snapshot.activated_at]
    evidence_times.extend(row.updated_at for row in snapshot.rows if row.updated_at is not None)
    available = max(evidence_times)
    state = ServingProjectionPayload(
        table_name="manual_watchlist_state",
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
    members = ServingProjectionPayload(
        table_name="manual_watchlist",
        available_at=available,
        rows=tuple(row.projection_row() for row in snapshot.rows),
    )
    return state, members


def validate_manual_watchlist_projections(
    projections: Mapping[str, ServingProjectionPayload | ServingProjectionInput],
) -> None:
    state = projections.get("manual_watchlist_state")
    members = projections.get("manual_watchlist")
    if state is None:
        if members is not None:
            raise ValueError("manual watchlist rows lack an authority state")
        return
    if len(state.rows) != 1:
        raise ValueError("manual watchlist state projection is incomplete")
    status = state.rows[0]
    if status["snapshot_key"] != "current":
        raise ValueError("manual watchlist state key is invalid")
    if status["state"] == "unavailable":
        if (
            members is not None
            or status["activated_at"] is not None
            or status["row_count"] is not None
            or status["rows_sha256"] is not None
        ):
            raise ValueError("unavailable manual watchlist state carries authority")
        return
    if status["state"] != "ready" or members is None:
        raise ValueError("ready manual watchlist state lacks rows")
    if state.available_at != members.available_at:
        raise ValueError("manual watchlist state and rows have different source times")
    snapshot = ManualWatchlistAuthoritySnapshot(
        activated_at=status["activated_at"],
        rows=tuple(ManualWatchlistProjectionRow.model_validate(row) for row in members.rows),
        row_count=status["row_count"],
        rows_sha256=status["rows_sha256"],
    )
    if snapshot.activated_at > state.available_at or any(
        row.updated_at is not None and row.updated_at > state.available_at for row in snapshot.rows
    ):
        raise ValueError("manual watchlist source time precedes its rows")
