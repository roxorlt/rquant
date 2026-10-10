"""Produce paper-execution constraints from point-in-time reference and minute evidence."""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from types import MappingProxyType
from typing import Annotated, Self
from zoneinfo import ZoneInfo

import pandas as pd
from pydantic import Field, StringConstraints, field_validator, model_validator

from rquant.live_contracts import BatchEnvelope, BatchQualityStatus, LiveChannel
from rquant.live_spool import LiveBatchRecord, LiveBatchSpool
from rquant.market_minute_gateway import MarketMinuteGateway
from rquant.paper_execution_constraints import (
    PaperExecutionConstraintBatch,
    PaperExecutionConstraintPointer,
    PaperExecutionConstraintPublisher,
    PaperExecutionConstraintSnapshot,
)
from rquant.reference_data_registry import (
    ReferenceAsOfSnapshot,
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
_REFERENCE_DATASETS = (
    ReferenceDataset.ST_STATUS,
    ReferenceDataset.SUSPENSION_STATUS,
    ReferenceDataset.PRICE_LIMIT_REGIME,
    ReferenceDataset.LISTING_STATUS,
)


class PaperExecutionConstraintEvidenceError(RuntimeError):
    """Required point-in-time evidence is absent, stale, or internally inconsistent."""


class PaperExecutionConstraintNoEvidenceError(PaperExecutionConstraintEvidenceError):
    """No requested code has a same-day minute at observed_at, so there is nothing to publish.

    Until #307 one such code refused the constraints of every other code; now such a code is
    left out and counted, and only a request in which *every* code is left out raises this.
    The runtime step reports it as an idle round, not a failure: the authority keeps its last
    generation, whose intervals have expired, so nothing is tradable.
    """


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


class PaperExecutionConstraintCoverage(RuntimeContractModel):
    """Which requested codes are not tradable at observed_at, and why (#307).

    `stale_codes` have records, but none covering observed_at: the lunch break, the minutes
    after the close, or a code whose newest minute is not in the latest batch. Their
    intervals are published as they always were and have expired, so the broker refuses them
    ("constraint has expired"). `codes_without_evidence` have no same-day minute at all and
    no record. Neither fails the round any more.
    """

    stale_codes: tuple[str, ...] = ()
    codes_without_evidence: tuple[str, ...] = ()


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


class _RequestReferences:
    """One production request's reference reads: one registry read, on first use (#299).

    `_reference_state` runs once per visible minute batch per code -- codes x batches x 4
    `as_of` calls per two-second round, each one a lock, a connection and a validation of
    the generation's whole manifest. The snapshot is read where the first of those calls
    used to be, so a registry that cannot be read still refuses as that code's lookup did.
    """

    def __init__(
        self,
        registry: ReferenceRegistry,
        *,
        keys: tuple[str, ...],
        generation_id: str,
    ) -> None:
        self._registry = registry
        self._keys = keys
        self._generation_id = generation_id
        self._snapshot: ReferenceAsOfSnapshot | None = None

    def as_of(
        self,
        *,
        dataset_id: str,
        key: str,
        event_time: datetime,
        decision_time: datetime,
    ) -> ReferenceLookup:
        if self._snapshot is None:
            self._snapshot = self._registry.as_of_snapshot(
                dataset_ids=_REFERENCE_DATASETS,
                keys=self._keys,
                generation_id=self._generation_id,
            )
        return self._snapshot.as_of(
            dataset_id=dataset_id,
            key=key,
            event_time=event_time,
            decision_time=decision_time,
        )


@dataclass(frozen=True)
class _BatchMinuteEvidence:
    """One visible market-minute batch's newest same-day minute of every code in it (#302).

    Request-independent: the rows of every code, reduced exactly as the request loop
    reduces the rows of a requested code. A batch in which some row would refuse (an
    invalid time or close, a minute later than the batch) is not reduced at all (`clean`
    false): it is scanned with the request's codes every time, so it refuses exactly when
    and as it did before.
    """

    envelope: BatchEnvelope
    clean: bool
    by_code: Mapping[str, _MinuteEvidence]


@dataclass(frozen=True)
class _Production:
    """The last successful production, reused while nothing it was built from has moved."""

    key: tuple[object, ...]
    batch: PaperExecutionConstraintBatch
    last_record_by_code: Mapping[str, PaperExecutionConstraintSnapshot]
    codes_without_evidence: tuple[str, ...]
    publication: PaperExecutionConstraintPublication | None


class PaperExecutionConstraintProducer:
    """Build and atomically publish broker constraints without future evidence.

    The producer keeps, for the trade date it last served, what the visible minute batches
    and the reference lookups already told it (#302): each batch is decoded once, each
    (code, minute) reference state and constraint record is built once, and a round whose
    inputs -- visible batch manifests, requested codes, reference generation, sequence --
    are exactly the last round's publishes the very batch it published then. Every refusal
    is the one the full rebuild gives, because nothing that can refuse is remembered: a
    batch or record that fails is built again on the next round. A new process, or a new
    trade date, starts from nothing.
    """

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
        self._cache_trade_date: date | None = None
        self._batch_evidence: dict[int, _BatchMinuteEvidence] = {}
        self._reference_states: dict[tuple[str, str, datetime, datetime], _ReferenceState] = {}
        self._records: dict[tuple[object, ...], PaperExecutionConstraintSnapshot] = {}
        self._last: _Production | None = None

    def produce(
        self,
        request: PaperExecutionConstraintProductionRequest,
    ) -> PaperExecutionConstraintPublication:
        return self.produce_with_coverage(request)[0]

    def produce_with_coverage(
        self,
        request: PaperExecutionConstraintProductionRequest,
    ) -> tuple[PaperExecutionConstraintPublication, PaperExecutionConstraintCoverage]:
        validated = PaperExecutionConstraintProductionRequest.model_validate(request)
        observed_at = normalize_aware_utc(validated.observed_at)
        manifest = self.reference_registry.generation(validated.reference_generation_id)
        if manifest.published_at > observed_at:
            raise PaperExecutionConstraintEvidenceError(
                "reference generation is future evidence at observed_at"
            )
        if self._cache_trade_date != validated.trade_date:
            self._batch_evidence.clear()
            self._reference_states.clear()
            self._records.clear()
            self._last = None
            self._cache_trade_date = validated.trade_date
        visible = self._visible_batches(observed_at=observed_at)
        key: tuple[object, ...] = (
            validated.trade_date,
            validated.ts_codes,
            validated.reference_generation_id,
            manifest.published_at,
            validated.sequence,
            tuple(record.envelope for record in visible),
        )
        last = self._last
        if last is None or last.key != key:
            last = self._build(
                validated,
                visible=visible,
                observed_at=observed_at,
                reference_published_at=manifest.published_at,
                key=key,
            )
        pointer = self.publisher.publish(last.batch)
        publication = last.publication
        if publication is None or publication.pointer != pointer:
            publication = PaperExecutionConstraintPublication(batch=last.batch, pointer=pointer)
        self._last = _Production(
            key=last.key,
            batch=last.batch,
            last_record_by_code=last.last_record_by_code,
            codes_without_evidence=last.codes_without_evidence,
            publication=publication,
        )
        stale = tuple(
            ts_code
            for ts_code, record in last.last_record_by_code.items()
            if not record.available_at <= observed_at < record.expires_at
        )
        return publication, PaperExecutionConstraintCoverage(
            stale_codes=stale,
            codes_without_evidence=last.codes_without_evidence,
        )

    def _build(
        self,
        validated: PaperExecutionConstraintProductionRequest,
        *,
        visible: tuple[LiveBatchRecord, ...],
        observed_at: datetime,
        reference_published_at: datetime,
        key: tuple[object, ...],
    ) -> _Production:
        minute_evidence = self._visible_minutes(
            visible,
            ts_codes=validated.ts_codes,
            trade_date=validated.trade_date,
            observed_at=observed_at,
        )
        references = _RequestReferences(
            self.reference_registry,
            keys=validated.ts_codes,
            generation_id=validated.reference_generation_id,
        )
        records: list[PaperExecutionConstraintSnapshot] = []
        last_record_by_code: dict[str, PaperExecutionConstraintSnapshot] = {}
        without_evidence: list[str] = []
        for ts_code in validated.ts_codes:
            code_evidence = minute_evidence.get(ts_code, ())
            if not code_evidence:
                #: #307: left out and counted -- it has no interval, so it is not tradable
                without_evidence.append(ts_code)
                continue
            code_records = self._records_for_code(
                ts_code=ts_code,
                trade_date=validated.trade_date,
                evidence=code_evidence,
                references=references,
                reference_generation_id=validated.reference_generation_id,
                reference_published_at=reference_published_at,
            )
            records.extend(code_records)
            last_record_by_code[ts_code] = code_records[-1]
        if not records:
            raise PaperExecutionConstraintNoEvidenceError(
                "no requested code has visible same-day minute evidence at observed_at"
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
        return _Production(
            key=key,
            batch=batch,
            last_record_by_code=MappingProxyType(last_record_by_code),
            codes_without_evidence=tuple(without_evidence),
            publication=None,
        )

    def _visible_batches(self, *, observed_at: datetime) -> tuple[LiveBatchRecord, ...]:
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
        return visible_batches

    def _visible_minutes(
        self,
        visible_batches: tuple[LiveBatchRecord, ...],
        *,
        ts_codes: tuple[str, ...],
        trade_date: date,
        observed_at: datetime,
    ) -> Mapping[str, tuple[_MinuteEvidence, ...]]:
        requested = set(ts_codes)
        by_identity: dict[tuple[str, datetime], _MinuteEvidence] = {}
        for record in visible_batches:
            envelope = record.envelope
            if envelope.quality_status is not BatchQualityStatus.PUBLISHED:
                continue
            batch = self._batch_minute_evidence(record, trade_date=trade_date)
            if batch.clean:
                candidates: Iterable[_MinuteEvidence] = (
                    batch.by_code[ts_code] for ts_code in requested & batch.by_code.keys()
                )
            else:
                candidates = self._scan_batch(
                    record,
                    requested=requested,
                    trade_date=trade_date,
                    observed_at=observed_at,
                )
            for candidate in candidates:
                key = (candidate.ts_code, envelope.available_at)
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

    def _batch_frame(self, record: LiveBatchRecord) -> pd.DataFrame:
        frame = MarketMinuteGateway.decode_payload(self.minute_spool.read_payload(record))
        required = {"ts_code", "trade_time", "close"}
        if not required.issubset(frame.columns):
            raise PaperExecutionConstraintEvidenceError(
                "market-minute payload is missing required columns"
            )
        return frame

    def _batch_minute_evidence(
        self,
        record: LiveBatchRecord,
        *,
        trade_date: date,
    ) -> _BatchMinuteEvidence:
        envelope = record.envelope
        cached = self._batch_evidence.get(envelope.sequence)
        if cached is not None and (cached.envelope is envelope or cached.envelope == envelope):
            return cached
        frame = self._batch_frame(record)
        source_snapshot_id = envelope.identity_sha256
        by_code: dict[str, _MinuteEvidence] = {}
        clean = True
        for row in frame.loc[:, ["ts_code", "trade_time", "close"]].itertuples(index=False):
            ts_code = str(row.ts_code)
            try:
                trade_time = _as_utc_datetime(row.trade_time, name="minute trade_time")
                if trade_time > envelope.available_at:
                    clean = False
                    break
                #: the request loop's `trade_time > observed_at` skip cannot fire here: the
                #: batch is visible, so available_at <= observed_at, and trade_time is not
                #: later than available_at
                if trade_time.astimezone(_SHANGHAI).date() != trade_date:
                    continue
                close = _finite_float(row.close, name="minute close")
            except PaperExecutionConstraintEvidenceError:
                clean = False
                break
            previous = by_code.get(ts_code)
            if previous is None or trade_time > previous.trade_time:
                by_code[ts_code] = _MinuteEvidence(
                    ts_code=ts_code,
                    trade_time=trade_time,
                    available_at=envelope.available_at,
                    close=close,
                    source_snapshot_id=source_snapshot_id,
                    source_sequence=envelope.sequence,
                )
        evidence = _BatchMinuteEvidence(
            envelope=envelope,
            clean=clean,
            by_code=MappingProxyType(by_code if clean else {}),
        )
        self._batch_evidence[envelope.sequence] = evidence
        return evidence

    def _scan_batch(
        self,
        record: LiveBatchRecord,
        *,
        requested: set[str],
        trade_date: date,
        observed_at: datetime,
    ) -> tuple[_MinuteEvidence, ...]:
        """The request loop of before #302, for a batch with a row that may refuse."""

        envelope = record.envelope
        frame = self._batch_frame(record)
        found: list[_MinuteEvidence] = []
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
            found.append(
                _MinuteEvidence(
                    ts_code=ts_code,
                    trade_time=trade_time,
                    available_at=envelope.available_at,
                    close=close,
                    source_snapshot_id=envelope.identity_sha256,
                    source_sequence=envelope.sequence,
                )
            )
        return tuple(found)

    def _records_for_code(
        self,
        *,
        ts_code: str,
        trade_date: date,
        evidence: tuple[_MinuteEvidence, ...],
        references: _RequestReferences,
        reference_generation_id: str,
        reference_published_at: datetime,
    ) -> tuple[PaperExecutionConstraintSnapshot, ...]:
        records: list[PaperExecutionConstraintSnapshot] = []
        for index, minute in enumerate(evidence):
            if reference_published_at > minute.available_at:
                raise PaperExecutionConstraintEvidenceError(
                    "reference generation was not visible when minute evidence arrived"
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
            record_key: tuple[object, ...] = (
                ts_code,
                trade_date,
                minute.trade_time,
                minute.available_at,
                minute.close,
                minute.source_snapshot_id,
                minute.source_sequence,
                expires_at,
                reference_generation_id,
            )
            cached = self._records.get(record_key)
            if cached is not None:
                records.append(cached)
                continue
            state = self._cached_reference_state(
                references=references,
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
            snapshot = PaperExecutionConstraintSnapshot.model_validate(
                {
                    **snapshot_payload,
                    "content_hash": canonical_sha256(snapshot_payload),
                }
            )
            self._records[record_key] = snapshot
            records.append(snapshot)
        return tuple(records)

    def _cached_reference_state(
        self,
        *,
        references: _RequestReferences,
        ts_code: str,
        event_time: datetime,
        decision_time: datetime,
        generation_id: str,
    ) -> _ReferenceState:
        state_key = (generation_id, ts_code, event_time, decision_time)
        state = self._reference_states.get(state_key)
        if state is None:
            state = self._reference_state(
                references=references,
                ts_code=ts_code,
                event_time=event_time,
                decision_time=decision_time,
                generation_id=generation_id,
            )
            self._reference_states[state_key] = state
        return state

    def _reference_state(
        self,
        *,
        references: _RequestReferences,
        ts_code: str,
        event_time: datetime,
        decision_time: datetime,
        generation_id: str,
    ) -> _ReferenceState:
        try:
            st = references.as_of(
                dataset_id=ReferenceDataset.ST_STATUS,
                key=ts_code,
                event_time=event_time,
                decision_time=decision_time,
            )
            suspension = references.as_of(
                dataset_id=ReferenceDataset.SUSPENSION_STATUS,
                key=ts_code,
                event_time=event_time,
                decision_time=decision_time,
            )
            price_limit = references.as_of(
                dataset_id=ReferenceDataset.PRICE_LIMIT_REGIME,
                key=ts_code,
                event_time=event_time,
                decision_time=decision_time,
            )
        except ReferenceDataUnavailableError as exc:
            raise PaperExecutionConstraintEvidenceError(
                f"{ts_code} required reference evidence is unavailable"
            ) from exc
        try:
            listing = references.as_of(
                dataset_id=ReferenceDataset.LISTING_STATUS,
                key=ts_code,
                event_time=event_time,
                decision_time=decision_time,
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
    "PaperExecutionConstraintCoverage",
    "PaperExecutionConstraintEvidenceError",
    "PaperExecutionConstraintNoEvidenceError",
    "PaperExecutionConstraintProducer",
    "PaperExecutionConstraintProductionRequest",
    "PaperExecutionConstraintPublication",
]
