"""Select a frozen 09:25 reference fact slice for historical portfolio research.

The caller owns the validated `ReferenceAsOfSnapshot` and its generation. Registry
`as_of` proves each selected record's first visibility and effective interval; this
slice does not prove when the generation itself was published or switched current.
It contains no opening quote or locked-limit execution conclusion.
"""

from __future__ import annotations

import re
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from typing import Annotated, Literal, Self
from zoneinfo import ZoneInfo

from pydantic import Field, StrictBool, model_validator

from rquant.reference_data_registry import (
    ReferenceAsOfSnapshot,
    ReferenceDataIntegrityError,
    ReferenceDataset,
    ReferenceDataUnavailableError,
    ReferenceLookup,
)
from rquant.research_run_spec import InstrumentClassificationProvenance, InstrumentContext
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256

MAX_REFERENCE_CODES = 6000
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_CODE = re.compile(r"^[0-9]{6}\.(SH|SZ|BJ)$")
_EXCHANGE_BY_SUFFIX = {"SH": "SSE", "SZ": "SZSE", "BJ": "BSE"}
_DATASETS = (
    ReferenceDataset.LISTING_STATUS,
    ReferenceDataset.ST_STATUS,
    ReferenceDataset.SUSPENSION_STATUS,
    ReferenceDataset.PRICE_LIMIT_REGIME,
)
Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
PositivePrice = Annotated[Decimal, Field(gt=0, allow_inf_nan=False)]


class BacktestReferenceSourceError(ValueError):
    """The requested 09:25 reference slice cannot be attested."""


class ReferenceFactEvidence(RuntimeContractModel):
    """The exact generation member selected by a point-in-time registry lookup."""

    reference_dataset: ReferenceDataset
    reference_record_id: Sha256
    reference_generation_id: Sha256
    first_available_at: AwareUtcDatetime


class BacktestReferenceFact(RuntimeContractModel):
    """One instrument's four reference domains, without inferred execution conditions."""

    ts_code: str = Field(pattern=r"^[0-9]{6}\.(SH|SZ|BJ)$")
    instrument_context: InstrumentContext
    listing_status: Literal["listed"]
    is_st: StrictBool
    is_suspended: StrictBool
    limit_up_price: PositivePrice
    limit_down_price: PositivePrice
    listing: ReferenceFactEvidence
    st: ReferenceFactEvidence
    suspension: ReferenceFactEvidence
    price_limit: ReferenceFactEvidence

    @model_validator(mode="after")
    def validate_provenance(self) -> Self:
        if self.limit_down_price >= self.limit_up_price:
            raise ValueError("price limit boundaries must be ordered")
        context = self.instrument_context
        if context.ts_code != self.ts_code or context.scope_key != (
            "CN",
            _EXCHANGE_BY_SUFFIX[self.ts_code.rsplit(".", 1)[1]],
            "EQUITY",
            "A_SHARE",
        ):
            raise ValueError("instrument classification is not a matching CN A-share")
        provenance = context.classification_provenance
        if provenance is None or provenance.reference_record_id != self.listing.reference_record_id:
            raise ValueError("listing classification provenance does not match selected record")
        for evidence, dataset in zip(
            (self.listing, self.st, self.suspension, self.price_limit),
            _DATASETS,
            strict=True,
        ):
            if evidence.reference_dataset != dataset:
                raise ValueError("reference evidence has the wrong dataset")
            if evidence.reference_generation_id != self.listing.reference_generation_id:
                raise ValueError("reference evidence generations do not match")
        if provenance.reference_generation_id != self.listing.reference_generation_id:
            raise ValueError("listing classification provenance has the wrong generation")
        return self

    @property
    def last_reference_available_at(self) -> datetime:
        return max(
            evidence.first_available_at
            for evidence in (self.listing, self.st, self.suspension, self.price_limit)
        )

    @property
    def source_identity(self) -> str:
        return canonical_sha256({"contract": "backtest-reference-fact/v1", "fact": self})


class BacktestReferenceSnapshot(RuntimeContractModel):
    """A deterministic, immutable batch from one caller-frozen reference generation."""

    source_mode: Literal["reference_as_of_snapshot"] = "reference_as_of_snapshot"
    trade_date: date
    decision_time: AwareUtcDatetime
    generation_id: Sha256
    facts: tuple[BacktestReferenceFact, ...] = Field(min_length=1, max_length=MAX_REFERENCE_CODES)

    @model_validator(mode="after")
    def validate_batch(self) -> Self:
        local = self.decision_time.astimezone(_SHANGHAI)
        if local.date() != self.trade_date or local.time() != time(9, 25):
            raise ValueError("reference decision must be exactly 09:25 Asia/Shanghai")
        codes = tuple(fact.ts_code for fact in self.facts)
        if codes != tuple(sorted(set(codes))):
            raise ValueError("reference facts must be ordered and unique")
        if any(fact.listing.reference_generation_id != self.generation_id for fact in self.facts):
            raise ValueError("reference facts do not match the selected generation")
        if any(fact.last_reference_available_at > self.decision_time for fact in self.facts):
            raise ValueError("reference facts are future evidence")
        return self

    @property
    def source_identity(self) -> str:
        return canonical_sha256(
            {
                "contract": "backtest-reference-snapshot/v1",
                "source_mode": self.source_mode,
                "trade_date": self.trade_date,
                "decision_time": self.decision_time,
                "generation_id": self.generation_id,
                "fact_identities": tuple(fact.source_identity for fact in self.facts),
            }
        )


def _evidence(lookup: ReferenceLookup) -> ReferenceFactEvidence:
    return ReferenceFactEvidence(
        reference_dataset=ReferenceDataset(lookup.record.dataset_id),
        reference_record_id=lookup.record.record_id,
        reference_generation_id=lookup.generation_id,
        first_available_at=lookup.record.first_available_at,
    )


def _required_bool(lookup: ReferenceLookup, field: str) -> bool:
    value = lookup.record.payload.get(field)
    if not isinstance(value, bool):
        raise BacktestReferenceSourceError(f"{lookup.record.key} {field} must be boolean")
    return value


def _required_price(lookup: ReferenceLookup, field: str) -> Decimal:
    value = lookup.record.payload.get(field)
    if isinstance(value, bool) or not isinstance(value, (int, float, str, Decimal)):
        raise BacktestReferenceSourceError(f"{lookup.record.key} {field} must be a positive price")
    try:
        price = Decimal(str(value))
    except InvalidOperation as exc:
        raise BacktestReferenceSourceError(
            f"{lookup.record.key} {field} must be a positive price"
        ) from exc
    if not price.is_finite() or price <= 0:
        raise BacktestReferenceSourceError(f"{lookup.record.key} {field} must be a positive price")
    return price


def _context(listing: ReferenceLookup) -> InstrumentContext:
    payload = listing.record.payload
    if payload.get("status") != "listed" or not isinstance(payload.get("status"), str):
        raise BacktestReferenceSourceError(f"{listing.record.key} is not authoritatively listed")
    fields = ("market", "exchange", "instrument_class", "security_class")
    values: dict[str, str] = {}
    for field in fields:
        value = payload.get(field)
        if not isinstance(value, str) or not value.strip():
            raise BacktestReferenceSourceError(
                f"{listing.record.key} listing classification {field} is invalid"
            )
        values[field] = value
    try:
        return InstrumentContext(
            ts_code=listing.record.key,
            **values,
            classification_provenance=InstrumentClassificationProvenance(
                reference_dataset=ReferenceDataset.LISTING_STATUS.value,
                reference_record_id=listing.record.record_id,
                reference_generation_id=listing.generation_id,
            ),
        )
    except ValueError as exc:
        raise BacktestReferenceSourceError(
            f"{listing.record.key} listing classification is invalid"
        ) from exc


def select_backtest_reference_facts(
    snapshot: ReferenceAsOfSnapshot,
    *,
    trade_date: date,
    decision_time: datetime,
    ts_codes: tuple[str, ...],
) -> BacktestReferenceSnapshot:
    """Select four reference domains at exactly 09:25 from an existing frozen snapshot.

    The caller must separately prove the generation was published by the decision time.
    No reference value is substituted from daily OHLCV or an unfrozen live pointer.
    """

    if not isinstance(snapshot, ReferenceAsOfSnapshot):
        raise BacktestReferenceSourceError("a validated reference snapshot is required")
    if type(trade_date) is not date:
        raise BacktestReferenceSourceError("trade_date must be a civil date")
    if not isinstance(decision_time, datetime) or decision_time.tzinfo is None:
        raise BacktestReferenceSourceError("decision must be exactly 09:25 Asia/Shanghai")
    if decision_time.utcoffset() is None:
        raise BacktestReferenceSourceError("decision must be exactly 09:25 Asia/Shanghai")
    local = decision_time.astimezone(_SHANGHAI)
    if local.date() != trade_date or local.time() != time(9, 25):
        raise BacktestReferenceSourceError("decision must be exactly 09:25 Asia/Shanghai")
    if not isinstance(ts_codes, tuple) or not ts_codes:
        raise BacktestReferenceSourceError("a nonempty tuple of reference codes is required")
    if len(ts_codes) > MAX_REFERENCE_CODES:
        raise BacktestReferenceSourceError(f"maximum {MAX_REFERENCE_CODES} reference codes")
    if any(not isinstance(code, str) or _CODE.fullmatch(code) is None for code in ts_codes):
        raise BacktestReferenceSourceError("reference code must be a CN A-share ts_code")
    if len(set(ts_codes)) != len(ts_codes):
        raise BacktestReferenceSourceError("duplicate reference code")
    if not set(_DATASETS).issubset(snapshot.dataset_ids) or not set(ts_codes).issubset(
        snapshot.keys
    ):
        raise BacktestReferenceSourceError("reference snapshot does not cover the requested codes")

    facts: list[BacktestReferenceFact] = []
    for code in sorted(ts_codes):
        try:
            selected = {
                dataset: snapshot.as_of(
                    dataset_id=dataset,
                    key=code,
                    event_time=decision_time,
                    decision_time=decision_time,
                )
                for dataset in _DATASETS
            }
        except (ReferenceDataUnavailableError, ReferenceDataIntegrityError, LookupError) as exc:
            raise BacktestReferenceSourceError(
                f"{code} required reference evidence is unavailable"
            ) from exc
        listing = selected[ReferenceDataset.LISTING_STATUS]
        st = selected[ReferenceDataset.ST_STATUS]
        suspension = selected[ReferenceDataset.SUSPENSION_STATUS]
        price_limit = selected[ReferenceDataset.PRICE_LIMIT_REGIME]
        try:
            facts.append(
                BacktestReferenceFact(
                    ts_code=code,
                    instrument_context=_context(listing),
                    listing_status="listed",
                    is_st=_required_bool(st, "is_st"),
                    is_suspended=_required_bool(suspension, "is_suspended"),
                    limit_up_price=_required_price(price_limit, "limit_up_price"),
                    limit_down_price=_required_price(price_limit, "limit_down_price"),
                    listing=_evidence(listing),
                    st=_evidence(st),
                    suspension=_evidence(suspension),
                    price_limit=_evidence(price_limit),
                )
            )
        except ValueError as exc:
            raise BacktestReferenceSourceError(f"{code} reference values are invalid") from exc
    return BacktestReferenceSnapshot(
        trade_date=trade_date,
        decision_time=decision_time,
        generation_id=snapshot.generation_id,
        facts=tuple(facts),
    )
