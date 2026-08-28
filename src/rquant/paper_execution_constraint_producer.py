"""Produce paper-execution constraints from point-in-time reference and minute evidence."""

from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from types import MappingProxyType
from typing import Annotated, Self
from zoneinfo import ZoneInfo

from pydantic import Field, StringConstraints, field_validator, model_validator

from rquant.live_contracts import BatchQualityStatus, LiveChannel
from rquant.live_spool import LiveBatchSpool
from rquant.market_minute_gateway import MarketMinuteGateway
from rquant.paper_execution_constraints import (
    PaperExecutionConstraintBatch,
    PaperExecutionConstraintPointer,
    PaperExecutionConstraintPublisher,
    PaperExecutionConstraintSnapshot,
)
from rquant.reference_data_registry import (
    ReferenceDataset,
    ReferenceDataUnavailableError,
    ReferenceLookup,
    ReferenceRegistry,
)
from rquant.research_run_spec import InstrumentClassificationProvenance, InstrumentContext
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
CommitSha = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]
_SHANGHAI = ZoneInfo("Asia/Shanghai")


class PaperExecutionConstraintEvidenceError(RuntimeError):
    """Required point-in-time evidence is absent, stale, or internally inconsistent."""


class PaperExecutionConstraintProductionRequest(RuntimeContractModel):
    """One explicit point-in-time production request."""

    trade_date: date
    ts_codes: tuple[str, ...] = Field(min_length=1)
    observed_at: AwareUtcDatetime
    reference_generation_id: Sha256
    sequence: int = Field(ge=0)

    @field_validator("ts_codes")
    @classmethod
    def canonicalize_codes(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(sorted(value))
        if len(normalized) != len(set(normalized)):
            raise ValueError("ts_codes must be unique")
        return normalized

    @model_validator(mode="after")
    def validate_trade_date(self) -> Self:
        if self.observed_at.astimezone(_SHANGHAI).date() != self.trade_date:
            raise ValueError("trade_date must match observed_at in Asia/Shanghai")
        return self


class PaperExecutionConstraintPublication(RuntimeContractModel):
    """The immutable batch and authority pointer published for one request."""

    batch: PaperExecutionConstraintBatch
    pointer: PaperExecutionConstraintPointer


class _MinuteEvidence(RuntimeContractModel):
    ts_code: str
    trade_time: AwareUtcDatetime
    available_at: AwareUtcDatetime
    close: float
    source_snapshot_id: Sha256
    source_sequence: int = Field(ge=0)


class _ReferenceState(RuntimeContractModel):
    suspended: bool
    risk_rejected: bool
    limit_up_price: Decimal
    limit_down_price: Decimal
    source_snapshot_id: Sha256
    listing_snapshot_id: Sha256
    instrument_context: InstrumentContext


class PaperExecutionConstraintProducer:
    """Build and atomically publish broker constraints without future evidence."""

    def __init__(
        self,
        *,
        reference_registry: ReferenceRegistry,
        minute_spool: LiveBatchSpool,
        publisher: PaperExecutionConstraintPublisher,
        producer_commit: str,
        quote_ttl: timedelta = timedelta(minutes=2),
    ) -> None:
        if not isinstance(reference_registry, ReferenceRegistry):
            raise TypeError("reference_registry must be a ReferenceRegistry")
        if not isinstance(minute_spool, LiveBatchSpool):
            raise TypeError("minute_spool must be a LiveBatchSpool")
        if not isinstance(publisher, PaperExecutionConstraintPublisher):
            raise TypeError("publisher must be a PaperExecutionConstraintPublisher")
        if publisher.producer_commit != producer_commit:
            raise ValueError("publisher producer_commit does not match producer")
        if quote_ttl <= timedelta(0):
            raise ValueError("quote_ttl must be positive")
        self.reference_registry = reference_registry
        self.minute_spool = minute_spool
        self.publisher = publisher
        self.producer_commit = producer_commit
        self.quote_ttl = quote_ttl

    def produce(
        self,
        request: PaperExecutionConstraintProductionRequest,
    ) -> PaperExecutionConstraintPublication:
        validated = PaperExecutionConstraintProductionRequest.model_validate(request)
        observed_at = normalize_aware_utc(validated.observed_at)
        manifest = self.reference_registry.generation(validated.reference_generation_id)
        if manifest.published_at > observed_at:
            raise PaperExecutionConstraintEvidenceError(
                "reference generation is future evidence at observed_at"
            )
        minute_evidence = self._visible_minutes(
            ts_codes=validated.ts_codes,
            trade_date=validated.trade_date,
            observed_at=observed_at,
        )
        records: list[PaperExecutionConstraintSnapshot] = []
        for ts_code in validated.ts_codes:
            code_evidence = minute_evidence.get(ts_code, ())
            if not code_evidence:
                raise PaperExecutionConstraintEvidenceError(
                    f"{ts_code} has no visible minute evidence at observed_at"
                )
            records.extend(
                self._records_for_code(
                    ts_code=ts_code,
                    trade_date=validated.trade_date,
                    evidence=code_evidence,
                    observed_at=observed_at,
                    reference_generation_id=validated.reference_generation_id,
                    reference_published_at=manifest.published_at,
                )
            )
        batch_payload: dict[str, object] = {
            "schema_version": 1,
            "sequence": validated.sequence,
            "producer_commit": self.producer_commit,
            "records": tuple(records),
        }
        batch = PaperExecutionConstraintBatch.model_validate(
            {**batch_payload, "content_hash": canonical_sha256(batch_payload)}
        )
        pointer = self.publisher.publish(batch)
        return PaperExecutionConstraintPublication(batch=batch, pointer=pointer)

    def _visible_minutes(
        self,
        *,
        ts_codes: tuple[str, ...],
        trade_date: date,
        observed_at: datetime,
    ) -> Mapping[str, tuple[_MinuteEvidence, ...]]:
        visible_batches = tuple(
            record
            for record in self.minute_spool.list_after(
                LiveChannel.MARKET_MINUTE,
                sequence=-1,
            )
            if record.envelope.available_at <= observed_at
        )
        if not visible_batches:
            raise PaperExecutionConstraintEvidenceError(
                "no visible minute batch exists at observed_at"
            )
        latest = visible_batches[-1].envelope
        if latest.quality_status is not BatchQualityStatus.PUBLISHED:
            raise PaperExecutionConstraintEvidenceError(
                f"latest visible market-minute batch is {latest.quality_status.value}"
            )

        requested = set(ts_codes)
        by_identity: dict[tuple[str, datetime], _MinuteEvidence] = {}
        for record in visible_batches:
            envelope = record.envelope
            if envelope.quality_status is not BatchQualityStatus.PUBLISHED:
                continue
            frame = MarketMinuteGateway.decode_payload(self.minute_spool.read_payload(record))
            required = {"ts_code", "trade_time", "close"}
            if not required.issubset(frame.columns):
                raise PaperExecutionConstraintEvidenceError(
                    "market-minute payload is missing required columns"
                )
            for row in frame.loc[:, ["ts_code", "trade_time", "close"]].itertuples(index=False):
                ts_code = str(row.ts_code)
                if ts_code not in requested:
                    continue
                trade_time = _as_utc_datetime(row.trade_time, name="minute trade_time")
                if trade_time > envelope.available_at:
                    raise PaperExecutionConstraintEvidenceError(
                        "minute event time is future relative to its batch availability"
                    )
                if trade_time > observed_at:
                    continue
                if trade_time.astimezone(_SHANGHAI).date() != trade_date:
                    continue
                close = _finite_float(row.close, name="minute close")
                candidate = _MinuteEvidence(
                    ts_code=ts_code,
                    trade_time=trade_time,
                    available_at=envelope.available_at,
                    close=close,
                    source_snapshot_id=envelope.identity_sha256,
                    source_sequence=envelope.sequence,
                )
                key = (ts_code, envelope.available_at)
                previous = by_identity.get(key)
                if previous is None or (
                    candidate.trade_time,
                    candidate.source_sequence,
                ) > (previous.trade_time, previous.source_sequence):
                    by_identity[key] = candidate

        grouped: dict[str, list[_MinuteEvidence]] = {code: [] for code in ts_codes}
        for evidence in sorted(
            by_identity.values(),
            key=lambda item: (item.ts_code, item.available_at, item.source_sequence),
        ):
            grouped[evidence.ts_code].append(evidence)
        return MappingProxyType({key: tuple(value) for key, value in grouped.items()})

    def _records_for_code(
        self,
        *,
        ts_code: str,
        trade_date: date,
        evidence: tuple[_MinuteEvidence, ...],
        observed_at: datetime,
        reference_generation_id: str,
        reference_published_at: datetime,
    ) -> tuple[PaperExecutionConstraintSnapshot, ...]:
        records: list[PaperExecutionConstraintSnapshot] = []
        for index, minute in enumerate(evidence):
            if reference_published_at > minute.available_at:
                raise PaperExecutionConstraintEvidenceError(
                    "reference generation was not visible when minute evidence arrived"
                )
            state = self._reference_state(
                ts_code=ts_code,
                event_time=minute.trade_time,
                decision_time=minute.available_at,
                generation_id=reference_generation_id,
            )
            close = Decimal(str(minute.close))
            if close < state.limit_down_price or close > state.limit_up_price:
                raise PaperExecutionConstraintEvidenceError(
                    f"{ts_code} minute close is outside the visible price limit boundary"
                )
            next_available = (
                evidence[index + 1].available_at
                if index + 1 < len(evidence)
                else minute.available_at + self.quote_ttl
            )
            expires_at = min(
                next_available,
                minute.available_at + self.quote_ttl,
                _end_of_trade_date(trade_date),
            )
            if expires_at <= minute.available_at:
                raise PaperExecutionConstraintEvidenceError(
                    f"{ts_code} minute evidence has no positive validity interval"
                )
            snapshot_payload: dict[str, object] = {
                "ts_code": ts_code,
                "trade_date": trade_date,
                "available_at": minute.available_at,
                "expires_at": expires_at,
                "suspended": state.suspended,
                "buy_limit_locked": close == state.limit_up_price,
                "sell_limit_locked": close == state.limit_down_price,
                "risk_rejected": state.risk_rejected,
                "instrument_context": state.instrument_context.model_dump(mode="json"),
                "source_snapshot_ids": {
                    "market_minute": minute.source_snapshot_id,
                    "reference_slow": state.source_snapshot_id,
                    "reference_listing": state.listing_snapshot_id,
                },
                "producer_commit": self.producer_commit,
            }
            records.append(
                PaperExecutionConstraintSnapshot.model_validate(
                    {
                        **snapshot_payload,
                        "content_hash": canonical_sha256(snapshot_payload),
                    }
                )
            )
        if not records or not (records[-1].available_at <= observed_at < records[-1].expires_at):
            raise PaperExecutionConstraintEvidenceError(
                f"{ts_code} latest visible minute evidence is stale at observed_at"
            )
        return tuple(records)

    def _reference_state(
        self,
        *,
        ts_code: str,
        event_time: datetime,
        decision_time: datetime,
        generation_id: str,
    ) -> _ReferenceState:
        try:
            st = self.reference_registry.as_of(
                dataset_id=ReferenceDataset.ST_STATUS,
                key=ts_code,
                event_time=event_time,
                decision_time=decision_time,
                generation_id=generation_id,
            )
            suspension = self.reference_registry.as_of(
                dataset_id=ReferenceDataset.SUSPENSION_STATUS,
                key=ts_code,
                event_time=event_time,
                decision_time=decision_time,
                generation_id=generation_id,
            )
            price_limit = self.reference_registry.as_of(
                dataset_id=ReferenceDataset.PRICE_LIMIT_REGIME,
                key=ts_code,
                event_time=event_time,
                decision_time=decision_time,
                generation_id=generation_id,
            )
        except ReferenceDataUnavailableError as exc:
            raise PaperExecutionConstraintEvidenceError(
                f"{ts_code} required reference evidence is unavailable"
            ) from exc
        try:
            listing = self.reference_registry.as_of(
                dataset_id=ReferenceDataset.LISTING_STATUS,
                key=ts_code,
                event_time=event_time,
                decision_time=decision_time,
                generation_id=generation_id,
            )
        except ReferenceDataUnavailableError as exc:
            raise PaperExecutionConstraintEvidenceError(
                f"{ts_code} listing classification is unavailable"
            ) from exc
        is_st = _required_bool(st, "is_st")
        is_suspended = _required_bool(suspension, "is_suspended")
        limit_up = _required_price(price_limit, "limit_up_price")
        limit_down = _required_price(price_limit, "limit_down_price")
        if limit_down >= limit_up:
            raise PaperExecutionConstraintEvidenceError(
                f"{ts_code} price limit boundaries are not ordered"
            )
        instrument_context = _required_a_share_instrument_context(listing)
        return _ReferenceState(
            suspended=is_suspended,
            risk_rejected=is_st,
            limit_up_price=limit_up,
            limit_down_price=limit_down,
            source_snapshot_id=generation_id,
            listing_snapshot_id=listing.record.record_id,
            instrument_context=instrument_context,
        )


def _required_bool(lookup: ReferenceLookup, field: str) -> bool:
    value = lookup.record.payload.get(field)
    if not isinstance(value, bool):
        raise PaperExecutionConstraintEvidenceError(
            f"{lookup.record.key} {field} reference value must be boolean"
        )
    return value


def _required_a_share_instrument_context(lookup: ReferenceLookup) -> InstrumentContext:
    """Build execution context only from an attested listing-status record."""

    payload = lookup.record.payload
    fields = ("market", "exchange", "instrument_class", "security_class")
    values: dict[str, str] = {}
    for field in fields:
        value = payload.get(field)
        if not isinstance(value, str) or not value.strip():
            raise PaperExecutionConstraintEvidenceError(
                f"{lookup.record.key} listing classification {field} is missing"
            )
        values[field] = value
    try:
        context = InstrumentContext(
            ts_code=lookup.record.key,
            **values,
            classification_provenance=InstrumentClassificationProvenance(
                reference_dataset=ReferenceDataset.LISTING_STATUS.value,
                reference_record_id=lookup.record.record_id,
                reference_generation_id=lookup.generation_id,
            ),
        )
    except ValueError as exc:
        raise PaperExecutionConstraintEvidenceError(
            f"{lookup.record.key} listing classification is invalid"
        ) from exc
    if (
        context.market != "CN"
        or context.instrument_class != "EQUITY"
        or context.security_class != "A_SHARE"
    ):
        raise PaperExecutionConstraintEvidenceError(
            f"{lookup.record.key} listing classification is not an A_SHARE"
        )
    return context


def _required_price(lookup: ReferenceLookup, field: str) -> Decimal:
    value = lookup.record.payload.get(field)
    if isinstance(value, bool) or not isinstance(value, (int, float, str, Decimal)):
        raise PaperExecutionConstraintEvidenceError(
            f"{lookup.record.key} price limit field {field} is missing or invalid"
        )
    try:
        price = Decimal(str(value))
    except InvalidOperation as exc:
        raise PaperExecutionConstraintEvidenceError(
            f"{lookup.record.key} price limit field {field} is invalid"
        ) from exc
    if not price.is_finite() or price <= 0:
        raise PaperExecutionConstraintEvidenceError(
            f"{lookup.record.key} price limit field {field} is invalid"
        )
    return price


def _finite_float(value: object, *, name: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise PaperExecutionConstraintEvidenceError(f"{name} is invalid") from exc
    if not math.isfinite(number):
        raise PaperExecutionConstraintEvidenceError(f"{name} is invalid")
    return number


def _as_utc_datetime(value: object, *, name: str) -> datetime:
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    if not isinstance(value, datetime):
        raise PaperExecutionConstraintEvidenceError(f"{name} is invalid")
    try:
        return normalize_aware_utc(value)
    except ValueError as exc:
        raise PaperExecutionConstraintEvidenceError(f"{name} is invalid") from exc


def _end_of_trade_date(trade_date: date) -> datetime:
    return datetime.combine(trade_date, time.max, tzinfo=_SHANGHAI).astimezone(UTC)


__all__ = [
    "PaperExecutionConstraintEvidenceError",
    "PaperExecutionConstraintProducer",
    "PaperExecutionConstraintProductionRequest",
    "PaperExecutionConstraintPublication",
]
