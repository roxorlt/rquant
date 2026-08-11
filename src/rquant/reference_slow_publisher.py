"""Publish sealed daily facts into the point-in-time slow reference registry."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from datetime import date, datetime, time, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Annotated, Literal, Self
from zoneinfo import ZoneInfo

from pydantic import Field, StringConstraints, field_serializer, field_validator, model_validator

from rquant.live_contracts import ConsumerCursor
from rquant.reference_data_registry import (
    ReferenceDataset,
    ReferencePublicationDeadlineError,
    ReferencePublicationRollback,
    ReferenceRecord,
    ReferenceRegistry,
)
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.security_status import normalize_name
from rquant.serving_contracts import FreshnessStatus
from rquant.serving_read_models import ServingProjectionPayload
from rquant.state.derive import (
    _classify_board,
    _historical_limit_pct,
    _requires_five_day_listing_window,
)

if TYPE_CHECKING:
    from rquant.runtime_serving_snapshot import SourceReadResult

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
CommitSha = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]

_TS_CODE_PATTERN = re.compile(r"^[0-9]{6}\.(?:SH|SZ|BJ)$")
_SOURCE_KEYS = frozenset({"daily", "security", "suspension", "calendar"})
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_COMMIT_VISIBILITY_GUARD = timedelta(seconds=5)
_REFERENCE_PROJECTION_TABLES = frozenset(
    {
        "stock_basic",
        "risk_blacklist",
        "dc_board",
        "dc_board_member",
        "kpl_concept_member",
        "market_liquidity",
        "daily_bar",
        "trade_calendar",
        "nl_screen_universe",
    }
)


class ReferenceSlowPublicationError(RuntimeError):
    """Sealed source evidence cannot produce an unambiguous reference generation."""


class ReferenceDailyFact(RuntimeContractModel):
    ts_code: str
    trade_date: date
    close_raw: float = Field(gt=0, allow_inf_nan=False)
    prior_adj_factor: float = Field(gt=0, allow_inf_nan=False)
    adj_factor: float = Field(gt=0, allow_inf_nan=False)

    @field_validator("ts_code")
    @classmethod
    def validate_ts_code(cls, value: str) -> str:
        if _TS_CODE_PATTERN.fullmatch(value) is None:
            raise ValueError("ts_code must be a canonical Tushare A-share code")
        return value


class ReferenceSecurityFact(RuntimeContractModel):
    ts_code: str
    name: str = Field(min_length=1)
    is_st: bool | None = None
    list_date: date
    delist_date: date | None = None
    source_list_status: Literal["L", "D"] = "L"
    market: str = Field(min_length=1)

    @field_validator("ts_code")
    @classmethod
    def validate_ts_code(cls, value: str) -> str:
        if _TS_CODE_PATTERN.fullmatch(value) is None:
            raise ValueError("ts_code must be a canonical Tushare A-share code")
        return value


class ReferenceSlowSourceSnapshot(RuntimeContractModel):
    schema_version: int = Field(default=1, ge=1)
    target_trade_date: date
    captured_at: AwareUtcDatetime
    producer_commit: CommitSha
    source_snapshot_ids: Mapping[str, Sha256]
    daily_facts: tuple[ReferenceDailyFact, ...] = Field(min_length=1)
    security_facts: tuple[ReferenceSecurityFact, ...] = Field(min_length=1)
    suspended_codes: tuple[str, ...] = ()
    projections: tuple[ServingProjectionPayload, ...] = ()
    content_sha256: Sha256

    @field_validator("source_snapshot_ids", mode="after")
    @classmethod
    def canonicalize_source_snapshot_ids(
        cls,
        value: Mapping[str, str],
    ) -> Mapping[str, str]:
        if set(value) != _SOURCE_KEYS:
            raise ValueError("source_snapshot_ids must bind all slow-reference sources")
        return MappingProxyType(dict(sorted(value.items())))

    @field_serializer("source_snapshot_ids")
    def serialize_source_snapshot_ids(self, value: Mapping[str, str]) -> dict[str, str]:
        return dict(value)

    @field_validator("daily_facts")
    @classmethod
    def canonicalize_daily_facts(
        cls,
        value: tuple[ReferenceDailyFact, ...],
    ) -> tuple[ReferenceDailyFact, ...]:
        return tuple(sorted(value, key=lambda fact: fact.ts_code))

    @field_validator("security_facts")
    @classmethod
    def canonicalize_security_facts(
        cls,
        value: tuple[ReferenceSecurityFact, ...],
    ) -> tuple[ReferenceSecurityFact, ...]:
        return tuple(sorted(value, key=lambda fact: fact.ts_code))

    @field_validator("suspended_codes")
    @classmethod
    def canonicalize_suspended_codes(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(_TS_CODE_PATTERN.fullmatch(code) is None for code in value):
            raise ValueError("suspended_codes contain an invalid ts_code")
        return tuple(sorted(value))

    @field_validator("projections")
    @classmethod
    def canonicalize_projections(
        cls,
        value: tuple[ServingProjectionPayload, ...],
    ) -> tuple[ServingProjectionPayload, ...]:
        return tuple(sorted(value, key=lambda projection: projection.table_name))

    @model_validator(mode="after")
    def validate_snapshot(self) -> Self:
        daily_codes = tuple(fact.ts_code for fact in self.daily_facts)
        security_codes = tuple(fact.ts_code for fact in self.security_facts)
        if len(daily_codes) != len(set(daily_codes)):
            raise ValueError("daily_facts contain duplicate ts_code values")
        if len(security_codes) != len(set(security_codes)):
            raise ValueError("security_facts contain duplicate ts_code values")
        if len(self.suspended_codes) != len(set(self.suspended_codes)):
            raise ValueError("suspended_codes contain duplicate ts_code values")
        if daily_codes != security_codes:
            raise ValueError("daily_facts and security_facts must cover the same codes")
        if not set(self.suspended_codes).issubset(daily_codes):
            raise ValueError("suspended_codes must be present in the sealed universe")
        projection_names = tuple(projection.table_name for projection in self.projections)
        if projection_names and (
            len(projection_names) != len(set(projection_names))
            or set(projection_names) != _REFERENCE_PROJECTION_TABLES
        ):
            raise ValueError("reference source projections must publish the complete contract")
        if any(projection.available_at > self.captured_at for projection in self.projections):
            raise ValueError("reference source projection contains future evidence")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"content_sha256"}))
        if self.content_sha256 != expected:
            raise ValueError("content_sha256 does not bind the sealed source snapshot")
        return self

    @property
    def revision_content_sha256(self) -> str:
        """Hash factual source content without discovery time or code version."""

        return canonical_sha256(
            {
                "contract": "reference-slow-revision-content/v1",
                "target_trade_date": self.target_trade_date,
                "source_snapshot_ids": dict(self.source_snapshot_ids),
                "daily_facts": self.daily_facts,
                "security_facts": self.security_facts,
                "suspended_codes": self.suspended_codes,
                "projections": tuple(
                    {
                        "table_name": projection.table_name,
                        "rows": projection.rows,
                    }
                    for projection in self.projections
                ),
            }
        )

    @classmethod
    def create(
        cls,
        *,
        target_trade_date: date,
        captured_at: datetime,
        producer_commit: str,
        source_snapshot_ids: Mapping[str, str],
        daily_facts: tuple[ReferenceDailyFact, ...],
        security_facts: tuple[ReferenceSecurityFact, ...],
        suspended_codes: tuple[str, ...] = (),
        projections: tuple[ServingProjectionPayload, ...] | None = None,
        trade_calendar_open_dates: tuple[date, ...] = (),
    ) -> ReferenceSlowSourceSnapshot:
        normalized_captured_at = normalize_aware_utc(captured_at)
        prepared_projections = projections or _default_reference_projections(
            target_trade_date=target_trade_date,
            captured_at=normalized_captured_at,
            daily_facts=daily_facts,
            security_facts=security_facts,
            trade_calendar_open_dates=trade_calendar_open_dates,
        )
        identity = {
            "schema_version": 1,
            "target_trade_date": target_trade_date,
            "captured_at": normalized_captured_at,
            "producer_commit": producer_commit,
            "source_snapshot_ids": dict(source_snapshot_ids),
            "daily_facts": tuple(sorted(daily_facts, key=lambda fact: fact.ts_code)),
            "security_facts": tuple(sorted(security_facts, key=lambda fact: fact.ts_code)),
            "suspended_codes": tuple(sorted(suspended_codes)),
            "projections": tuple(
                sorted(prepared_projections, key=lambda projection: projection.table_name)
            ),
        }
        return cls(**identity, content_sha256=canonical_sha256(identity))


class ReferenceSlowPublishReceipt(RuntimeContractModel):
    target_trade_date: date
    generation_id: Sha256
    source_snapshot_id: Sha256
    inserted_record_count: int = Field(ge=0)
    security_count: int = Field(ge=0)
    revision: int = Field(ge=1)
    available_at: AwareUtcDatetime


def _default_reference_projections(
    *,
    target_trade_date: date,
    captured_at: datetime,
    daily_facts: tuple[ReferenceDailyFact, ...],
    security_facts: tuple[ReferenceSecurityFact, ...],
    trade_calendar_open_dates: tuple[date, ...] = (),
) -> tuple[ServingProjectionPayload, ...]:
    available = normalize_aware_utc(captured_at)
    daily_by_code = {fact.ts_code: fact for fact in daily_facts}
    stock_rows = []
    blacklist_rows = []
    daily_rows = []
    universe_rows = []
    for security in sorted(security_facts, key=lambda fact: fact.ts_code):
        daily = daily_by_code[security.ts_code]
        normalized_name, name_is_st = normalize_name(security.name)
        if normalized_name is None or name_is_st is None:
            raise ReferenceSlowPublicationError(f"{security.ts_code} has an invalid security name")
        is_st = name_is_st if security.is_st is None else security.is_st
        stock_rows.append(
            {
                "ts_code": security.ts_code,
                "name": normalized_name,
                "industry": security.market,
            }
        )
        if is_st:
            blacklist_rows.append(
                {
                    "ts_code": security.ts_code,
                    "list_label": "ST",
                    "expires_at": None,
                    "imported_at": available.isoformat(),
                }
            )
        daily_rows.append(
            {
                "ts_code": daily.ts_code,
                "trade_date": daily.trade_date.isoformat(),
                "open": daily.close_raw,
                "high": daily.close_raw,
                "low": daily.close_raw,
                "close": daily.close_raw,
                "vol": 0.0,
            }
        )
        session_pre_close = daily.close_raw * daily.prior_adj_factor / daily.adj_factor
        universe_rows.append(
            {
                "trade_date": daily.trade_date.isoformat(),
                "ts_code": security.ts_code,
                "name": normalized_name,
                "is_st": is_st,
                "is_bj": security.ts_code.endswith(".BJ"),
                "board_type": _classify_board(security.ts_code),
                "CLOSE[0]": session_pre_close,
                "PCT_CHG[0]": 0.0,
            }
        )
    rows_by_table: Mapping[str, tuple[Mapping[str, object], ...]] = {
        "stock_basic": tuple(stock_rows),
        "risk_blacklist": tuple(blacklist_rows),
        "dc_board": (),
        "dc_board_member": (),
        "kpl_concept_member": (),
        "market_liquidity": (),
        "daily_bar": tuple(daily_rows),
        "trade_calendar": tuple(
            {
                "trade_date": trade_date.isoformat(),
                "exchange": "SSE",
                "is_open": True,
            }
            for trade_date in (trade_calendar_open_dates or (target_trade_date,))
        ),
        "nl_screen_universe": tuple(universe_rows),
    }
    return tuple(
        ServingProjectionPayload(
            table_name=table_name,
            available_at=available,
            rows=rows_by_table[table_name],
        )
        for table_name in sorted(rows_by_table)
    )


def _reference_generation_revision(
    registry: ReferenceRegistry,
    generation_id: str,
) -> int:
    revision = 0
    current: str | None = generation_id
    visited: set[str] = set()
    while current is not None:
        if current in visited:
            raise ReferenceSlowPublicationError("reference generation ancestry contains a cycle")
        visited.add(current)
        manifest = registry.generation(current)
        revision += 1
        current = manifest.previous_generation_id
    return revision


def build_reference_slow_serving_result(
    *,
    snapshot: ReferenceSlowSourceSnapshot,
    receipt: ReferenceSlowPublishReceipt,
) -> SourceReadResult:
    """Build the immutable reference owner result without reading mutable current state."""

    from rquant.runtime_serving_snapshot import (
        REFERENCE_SLOW_AUTHORITY_DATASET_ID,
        ReferenceSlowPayload,
        SourceReadResult,
    )

    snapshot = ReferenceSlowSourceSnapshot.model_validate(snapshot)
    receipt = ReferenceSlowPublishReceipt.model_validate(receipt)
    projections = snapshot.projections or _default_reference_projections(
        target_trade_date=snapshot.target_trade_date,
        captured_at=snapshot.captured_at,
        daily_facts=snapshot.daily_facts,
        security_facts=snapshot.security_facts,
    )
    payload = ReferenceSlowPayload(
        reference_generation_id=receipt.generation_id,
        revision=receipt.revision,
        price_basis="raw_session",
        adjustment_basis="tushare_adj_factor",
        available_at=receipt.available_at,
        projections=projections,
    )
    values: dict[str, object] = {
        "dataset_id": REFERENCE_SLOW_AUTHORITY_DATASET_ID,
        "sequence": receipt.revision,
        "event_time": snapshot.captured_at,
        "published_at": receipt.available_at,
        "status": FreshnessStatus.FRESH,
        "reason": None,
        "payload": payload,
    }
    values["generation_id"] = canonical_sha256(values)
    return SourceReadResult.model_validate(values)


def _session_boundary(trade_date: date) -> datetime:
    return datetime.combine(trade_date, time.min, tzinfo=_SHANGHAI)


def _rounded_price(value: float) -> float:
    return float(Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _limit_eligible(
    *,
    target_trade_date: date,
    list_date: date,
    board_type: str,
    open_dates: tuple[date, ...],
) -> bool:
    if target_trade_date < list_date:
        raise ReferenceSlowPublicationError("target trade date precedes a security listing date")
    if target_trade_date == list_date:
        return False
    if not _requires_five_day_listing_window(board_type, list_date):
        return True
    observed_listing_sessions = tuple(
        item for item in open_dates if list_date <= item <= target_trade_date
    )
    if list_date < open_dates[0] and len(observed_listing_sessions) >= 5:
        return True
    return len(observed_listing_sessions) > 5


def _record_payloads(
    *,
    daily: ReferenceDailyFact,
    security: ReferenceSecurityFact,
    suspended: bool,
    target_trade_date: date,
    open_dates: tuple[date, ...],
) -> tuple[tuple[ReferenceDataset, dict[str, object]], ...]:
    normalized_name, name_is_st = normalize_name(security.name)
    if normalized_name is None or name_is_st is None:
        raise ReferenceSlowPublicationError(f"{security.ts_code} has an invalid security name")
    is_st = security.is_st if security.is_st is not None else name_is_st
    board_type = _classify_board(security.ts_code)
    eligible = _limit_eligible(
        target_trade_date=target_trade_date,
        list_date=security.list_date,
        board_type=board_type,
        open_dates=open_dates,
    )
    limit_percent = _historical_limit_pct(is_st, board_type, target_trade_date)
    session_pre_close_raw = daily.close_raw * daily.prior_adj_factor / daily.adj_factor
    return (
        (ReferenceDataset.ST_STATUS, {"is_st": is_st, "name": normalized_name}),
        (ReferenceDataset.SUSPENSION_STATUS, {"is_suspended": suspended}),
        (
            ReferenceDataset.LISTING_STATUS,
            {
                "delist_date": (
                    security.delist_date.isoformat() if security.delist_date is not None else None
                ),
                "list_date": security.list_date.isoformat(),
                "source_list_status": security.source_list_status,
                "status": "listed",
            },
        ),
        (
            ReferenceDataset.BOARD_MEMBERSHIP,
            {"board_type": board_type, "market": security.market},
        ),
        (
            ReferenceDataset.ADJUSTMENT_FACTOR,
            {"adj_factor": daily.adj_factor, "price_basis": "raw_session"},
        ),
        (
            ReferenceDataset.PRICE_LIMIT_REGIME,
            {
                "limit_down_price": _rounded_price(session_pre_close_raw * (1 - limit_percent)),
                "limit_eligible": eligible,
                "limit_percent": limit_percent,
                "limit_up_price": _rounded_price(session_pre_close_raw * (1 + limit_percent)),
                "session_pre_close_raw": _rounded_price(session_pre_close_raw),
            },
        ),
    )


def _next_revision(
    current: ReferenceRecord | None,
    *,
    dataset_id: str,
    key: str,
    effective_from: datetime,
    effective_to: datetime,
    payload: Mapping[str, object],
) -> tuple[int, str | None] | None:
    if current is None:
        return 1, None
    if (
        current.dataset_id != dataset_id
        or current.key != key
        or current.effective_from != normalize_aware_utc(effective_from)
    ):
        raise ReferenceSlowPublicationError("lineage head does not match candidate key")
    if current.effective_to == normalize_aware_utc(effective_to) and dict(current.payload) == dict(
        payload
    ):
        return None
    return current.revision + 1, "sealed source correction"


def publish_reference_slow_snapshot(
    *,
    registry: ReferenceRegistry,
    calendar: MarketCalendarAuthority,
    snapshot: ReferenceSlowSourceSnapshot,
    completion_clock: Callable[[], datetime],
) -> ReferenceSlowPublishReceipt:
    """Append one sealed pre-market snapshot and publish its immutable generation."""

    started_at = normalize_aware_utc(completion_clock())
    try:
        receipt, _rollback = _publish_reference_slow_snapshot_with_rollback(
            registry=registry,
            calendar=calendar,
            snapshot=snapshot,
            completion_clock=completion_clock,
            started_at=started_at,
            retain_intent=False,
            publication_id=None,
            completion_receipt_path=None,
            target_cursor=None,
        )
    except ReferencePublicationDeadlineError as exc:
        raise ReferenceSlowPublicationError("publication completed after 09:25") from exc
    return receipt


def _publish_reference_slow_snapshot_with_rollback(
    *,
    registry: ReferenceRegistry,
    calendar: MarketCalendarAuthority,
    snapshot: ReferenceSlowSourceSnapshot,
    completion_clock: Callable[[], datetime],
    started_at: datetime,
    retain_intent: bool,
    publication_id: str | None,
    completion_receipt_path: Path | None,
    target_cursor: ConsumerCursor | None,
) -> tuple[ReferenceSlowPublishReceipt, ReferencePublicationRollback]:
    """Publish one snapshot and retain the exact compensation token for its cursor."""

    snapshot = ReferenceSlowSourceSnapshot.model_validate(snapshot)
    calendar = MarketCalendarAuthority.model_validate(calendar)
    started = normalize_aware_utc(started_at)
    if calendar.content_sha256 != snapshot.source_snapshot_ids["calendar"]:
        raise ReferenceSlowPublicationError("calendar source snapshot does not match authority")
    if calendar.generated_at > snapshot.captured_at:
        raise ReferenceSlowPublicationError(
            "calendar was not visible when the snapshot was captured"
        )
    if snapshot.target_trade_date not in calendar.open_dates:
        raise ReferenceSlowPublicationError("target_trade_date is not an authoritative open date")
    prior_dates = tuple(item for item in calendar.open_dates if item < snapshot.target_trade_date)
    next_dates = tuple(item for item in calendar.open_dates if item > snapshot.target_trade_date)
    if not prior_dates or not next_dates:
        raise ReferenceSlowPublicationError("calendar lacks adjacent open-session coverage")
    prior_trade_date = prior_dates[-1]
    next_trade_date = next_dates[0]
    if any(fact.trade_date != prior_trade_date for fact in snapshot.daily_facts):
        raise ReferenceSlowPublicationError("daily facts must be from the exact prior open session")
    if started < snapshot.captured_at:
        raise ReferenceSlowPublicationError("publication start precedes source evidence")
    discovery_local = started.astimezone(_SHANGHAI)
    captured_local = snapshot.captured_at.astimezone(_SHANGHAI)
    if captured_local.date() != discovery_local.date():
        raise ReferenceSlowPublicationError(
            "source evidence must complete on its discovery session"
        )
    if snapshot.target_trade_date > discovery_local.date():
        raise ReferenceSlowPublicationError("historical revision cannot target a future session")
    decision_time = datetime.combine(
        discovery_local.date(),
        time(9, 25),
        tzinfo=_SHANGHAI,
    )
    if started > decision_time:
        raise ReferencePublicationDeadlineError("publication started after deadline")

    effective_from = _session_boundary(snapshot.target_trade_date)
    effective_to = _session_boundary(next_trade_date)
    daily_by_code = {fact.ts_code: fact for fact in snapshot.daily_facts}
    lineage_heads = {
        (record.dataset_id, record.key): record
        for record in registry.latest_lineage_heads(effective_from=effective_from)
    }
    pending_values: list[tuple[str, str, int, str | None, Mapping[str, object]]] = []
    for security in snapshot.security_facts:
        for dataset_id, payload in _record_payloads(
            daily=daily_by_code[security.ts_code],
            security=security,
            suspended=security.ts_code in snapshot.suspended_codes,
            target_trade_date=snapshot.target_trade_date,
            open_dates=calendar.open_dates,
        ):
            revision = _next_revision(
                lineage_heads.get((dataset_id, security.ts_code)),
                dataset_id=dataset_id,
                key=security.ts_code,
                effective_from=effective_from,
                effective_to=effective_to,
                payload=payload,
            )
            if revision is None:
                continue
            revision_number, replacement_reason = revision
            pending_values.append(
                (
                    dataset_id,
                    security.ts_code,
                    revision_number,
                    replacement_reason,
                    payload,
                )
            )
    prepared_at = normalize_aware_utc(completion_clock())
    if prepared_at < snapshot.captured_at:
        raise ReferenceSlowPublicationError("publication availability precedes source evidence")
    if prepared_at > decision_time:
        raise ReferencePublicationDeadlineError("publication completed after deadline")
    available = min(prepared_at + _COMMIT_VISIBILITY_GUARD, decision_time)
    pending = tuple(
        ReferenceRecord(
            dataset_id=dataset_id,
            key=key,
            effective_from=effective_from,
            effective_to=effective_to,
            revision=revision,
            source=f"reference-slow/{snapshot.content_sha256}",
            first_available_at=available,
            replacement_reason=replacement_reason,
            payload=payload,
        )
        for dataset_id, key, revision, replacement_reason, payload in pending_values
    )
    results, manifest, rollback = registry.append_many_and_publish_before(
        pending,
        published_at=available,
        completion_clock=completion_clock,
        not_after=decision_time,
        retain_intent=retain_intent,
        publication_id=publication_id,
        completion_receipt_path=completion_receipt_path,
        target_cursor=target_cursor,
    )
    inserted = sum(int(result.inserted) for result in results)
    revision = max(
        (
            record.revision
            for record in (
                *lineage_heads.values(),
                *(result.record for result in results),
            )
        ),
        default=1,
    )
    return (
        ReferenceSlowPublishReceipt(
            target_trade_date=snapshot.target_trade_date,
            generation_id=manifest.generation_id,
            source_snapshot_id=snapshot.content_sha256,
            inserted_record_count=inserted,
            security_count=len(snapshot.security_facts),
            revision=revision,
            available_at=manifest.published_at,
        ),
        rollback,
    )


__all__ = [
    "ReferenceDailyFact",
    "ReferenceSecurityFact",
    "ReferenceSlowPublicationError",
    "ReferenceSlowPublishReceipt",
    "ReferenceSlowSourceSnapshot",
    "build_reference_slow_serving_result",
    "publish_reference_slow_snapshot",
]
