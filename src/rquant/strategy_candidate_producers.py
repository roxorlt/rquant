"""Pure point-in-time producers for built-in strategy candidates."""

from __future__ import annotations

import math
from collections.abc import Callable, Mapping, Sequence
from datetime import date
from decimal import Decimal
from numbers import Real
from types import MappingProxyType
from typing import Annotated, ClassVar, Literal, TypeVar
from zoneinfo import ZoneInfo

from pydantic import (
    AliasChoices,
    BeforeValidator,
    Field,
    JsonValue,
    StrictBool,
    StrictInt,
    StringConstraints,
    field_serializer,
    field_validator,
    model_validator,
)

from rquant.live_contracts import BatchQualityStatus
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.state.derive import _classify_board
from rquant.strategy_candidate_snapshot import (
    StrategyCandidatePriceBasis,
    StrategyCandidateRecord,
)

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
CommitSha = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]
TsCode = Annotated[str, StringConstraints(pattern=r"^[0-9]{6}\.(?:SH|SZ|BJ)$")]

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_CANDIDATE_BASIS = "raw_session"
DAILY_VOLUME_LOT_SIZE_SHARES = 100.0
N_SHAPE_REQUIRED_LINEAGE_KEYS = frozenset(
    {
        "pool",
        "daily",
        "adj_factor",
        "session",
        "status",
        "limit",
        "trade_calendar",
    }
)
GROWTH_BOARD_REQUIRED_LINEAGE_KEYS = frozenset(
    {
        "daily",
        "moneyflow_t1",
        "session",
        "status",
        "limit",
        "trade_calendar",
    }
)
AUCTION_GAP_REQUIRED_LINEAGE_KEYS = frozenset({"session", "status", "limit"})


def _finite_number(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (Real, Decimal)):
        raise ValueError("value must be a real number and bool is forbidden")
    normalized = float(value)
    if not math.isfinite(normalized):
        raise ValueError("value must be finite")
    return normalized


FiniteNumber = Annotated[
    float,
    BeforeValidator(_finite_number),
    Field(allow_inf_nan=False),
]
PositiveFiniteNumber = Annotated[
    float,
    BeforeValidator(_finite_number),
    Field(gt=0, allow_inf_nan=False),
]
NonnegativeFiniteNumber = Annotated[
    float,
    BeforeValidator(_finite_number),
    Field(ge=0, allow_inf_nan=False),
]
NonnegativeStrictInt = Annotated[StrictInt, Field(ge=0)]


class CandidateProducerError(RuntimeError):
    """Base error for candidate inputs that cannot be trusted."""


class CandidateAuthorityQualityError(CandidateProducerError):
    """Raised when a non-published authority is presented to a producer."""


class CandidatePointInTimeError(CandidateProducerError):
    """Raised when an input was unavailable at the claimed decision point."""


class CandidateInputConflictError(CandidateProducerError):
    """Raised when equal-priority facts disagree for one security."""


class PublishedCandidateInputAuthority(RuntimeContractModel):
    trade_date: date
    captured_at: AwareUtcDatetime
    quality_status: BatchQualityStatus
    authority_snapshot_id: Sha256
    producer_commit: CommitSha

    @field_validator("quality_status", mode="before")
    @classmethod
    def require_quality_enum(cls, value: object) -> object:
        if not isinstance(value, BatchQualityStatus):
            raise ValueError("quality_status must be a BatchQualityStatus")
        return value

    @model_validator(mode="after")
    def validate_capture_session(self) -> PublishedCandidateInputAuthority:
        if self.captured_at.astimezone(_SHANGHAI).date() != self.trade_date:
            raise ValueError("captured_at must fall on trade_date in Asia/Shanghai")
        return self


class _SessionCandidateFact(RuntimeContractModel):
    required_lineage_keys: ClassVar[frozenset[str]] = frozenset()

    ts_code: TsCode
    session_pre_close_raw: PositiveFiniteNumber
    limit_pct: PositiveFiniteNumber
    limit_up_price_session_raw: PositiveFiniteNumber
    is_st: StrictBool
    is_suspended: StrictBool
    is_listed: StrictBool
    limit_eligible: StrictBool
    available_at: AwareUtcDatetime
    reference_snapshot_ids: Mapping[str, Sha256]

    @field_validator("reference_snapshot_ids")
    @classmethod
    def freeze_reference_snapshot_ids(
        cls,
        value: Mapping[str, str],
    ) -> Mapping[str, str]:
        if not value:
            raise ValueError("reference_snapshot_ids must not be empty")
        if any(not isinstance(key, str) or not key for key in value):
            raise ValueError("reference_snapshot_ids keys must be non-empty strings")
        return MappingProxyType(dict(sorted(value.items())))

    @field_serializer("reference_snapshot_ids")
    def serialize_reference_snapshot_ids(
        self,
        value: Mapping[str, str],
    ) -> dict[str, str]:
        return dict(value)

    @model_validator(mode="after")
    def validate_limit_geometry(self) -> _SessionCandidateFact:
        if self.limit_up_price_session_raw <= self.session_pre_close_raw:
            raise ValueError("limit_up_price_session_raw must exceed session_pre_close_raw")
        missing = self.required_lineage_keys.difference(self.reference_snapshot_ids)
        if missing:
            raise ValueError(
                "reference_snapshot_ids missing required lineage keys: "
                f"{', '.join(sorted(missing))}"
            )
        return self


class NShapePoolFact(_SessionCandidateFact):
    required_lineage_keys: ClassVar[frozenset[str]] = N_SHAPE_REQUIRED_LINEAGE_KEYS

    variant: Literal["pool1", "pool2"]
    reference_trade_date: date
    t_close_raw: FiniteNumber
    t_high_raw: FiniteNumber
    reference_adj_factor: FiniteNumber
    prior_session_trade_date: date
    expected_prior_session_trade_date: date
    prior_session_close_raw: FiniteNumber
    prior_session_adj_factor: FiniteNumber

    @model_validator(mode="after")
    def validate_static_dates_and_lineage(self) -> NShapePoolFact:
        if self.prior_session_trade_date != self.expected_prior_session_trade_date:
            raise ValueError("prior_session_trade_date must match the authoritative prior session")
        if self.reference_trade_date > self.prior_session_trade_date:
            raise ValueError("reference_trade_date cannot follow prior_session_trade_date")
        available_date = self.available_at.astimezone(_SHANGHAI).date()
        if self.prior_session_trade_date > available_date:
            raise ValueError("prior session facts cannot be available before their date")
        return self


class GrowthBoardFact(_SessionCandidateFact):
    required_lineage_keys: ClassVar[frozenset[str]] = GROWTH_BOARD_REQUIRED_LINEAGE_KEYS

    board_type: Literal["gem", "star"]
    reference_trade_date: date
    prior_session_trade_date: date
    expected_prior_session_trade_date: date
    ma5: FiniteNumber
    ma10: FiniteNumber
    ma20: FiniteNumber
    ma60: FiniteNumber
    large_net_vol_t1: FiniteNumber
    historical_sessions: NonnegativeStrictInt

    @model_validator(mode="after")
    def validate_reference_lineage(self) -> GrowthBoardFact:
        if self.board_type != _classify_board(self.ts_code):
            raise ValueError("board_type must match the board derived from ts_code")
        if not (
            self.reference_trade_date
            == self.prior_session_trade_date
            == self.expected_prior_session_trade_date
        ):
            raise ValueError(
                "reference_trade_date and prior_session_trade_date must match "
                "the authoritative prior session"
            )
        if self.reference_trade_date > self.available_at.astimezone(_SHANGHAI).date():
            raise ValueError("reference facts cannot be available before their date")
        return self


class PriorDailyVolumeFact(RuntimeContractModel):
    trade_date: date
    daily_volume_lots: NonnegativeFiniteNumber
    available_at: AwareUtcDatetime
    source_snapshot_id: Sha256 | None = None

    @model_validator(mode="after")
    def validate_availability(self) -> PriorDailyVolumeFact:
        if self.trade_date > self.available_at.astimezone(_SHANGHAI).date():
            raise ValueError("daily volume cannot be available before its trade_date")
        return self


class AuctionMatchFact(_SessionCandidateFact):
    required_lineage_keys: ClassVar[frozenset[str]] = AUCTION_GAP_REQUIRED_LINEAGE_KEYS

    trade_date: date
    auction_price_raw: PositiveFiniteNumber
    auction_vol_shares: NonnegativeFiniteNumber = Field(
        validation_alias=AliasChoices("auction_vol_shares", "auction_vol", "vol")
    )
    source_snapshot_id: Sha256
    source_volume_ratio: NonnegativeFiniteNumber | None = Field(
        default=None,
        validation_alias=AliasChoices("source_volume_ratio", "volume_ratio"),
    )
    expected_prior5_trade_dates: tuple[date, ...]
    calendar_available_at: AwareUtcDatetime
    calendar_snapshot_id: Sha256
    prior5_daily_volumes: tuple[PriorDailyVolumeFact, ...]
    daily_snapshot_id: Sha256 | None = None

    @model_validator(mode="after")
    def validate_prior_five_sessions(self) -> AuctionMatchFact:
        if self.available_at.astimezone(_SHANGHAI).date() != self.trade_date:
            raise ValueError("auction available_at must fall on trade_date")
        expected_dates = self.expected_prior5_trade_dates
        if len(expected_dates) != 5:
            raise ValueError("expected_prior5_trade_dates must contain exactly five sessions")
        if expected_dates != tuple(sorted(expected_dates)) or len(set(expected_dates)) != len(
            expected_dates
        ):
            raise ValueError("authoritative prior-five calendar must be strictly increasing")
        if any(trade_date >= self.trade_date for trade_date in expected_dates):
            raise ValueError("authoritative prior-five calendar must precede trade_date")
        if len(self.prior5_daily_volumes) != 5:
            raise ValueError("prior5_daily_volumes must contain exactly five sessions")
        dates = tuple(item.trade_date for item in self.prior5_daily_volumes)
        if dates != tuple(sorted(dates)) or len(set(dates)) != len(dates):
            raise ValueError("prior5 daily dates must be strictly increasing")
        if any(item.trade_date >= self.trade_date for item in self.prior5_daily_volumes):
            raise ValueError("prior5 daily dates must be completed before trade_date")
        if dates != expected_dates:
            raise ValueError(
                "prior5 daily dates must exactly match the authoritative prior-five calendar"
            )
        individual_snapshots = tuple(item.source_snapshot_id for item in self.prior5_daily_volumes)
        if self.daily_snapshot_id is None and any(
            snapshot is None for snapshot in individual_snapshots
        ):
            raise ValueError("daily volume facts require individual snapshots or daily_snapshot_id")
        if self.daily_snapshot_id is not None and any(
            snapshot is not None for snapshot in individual_snapshots
        ):
            raise ValueError("daily_snapshot_id cannot be mixed with individual daily snapshots")
        return self


FactT = TypeVar("FactT", bound=_SessionCandidateFact)


def _validated_authority(
    authority: PublishedCandidateInputAuthority,
) -> PublishedCandidateInputAuthority:
    if not isinstance(authority, PublishedCandidateInputAuthority):
        raise TypeError("authority must be a PublishedCandidateInputAuthority")
    validated = PublishedCandidateInputAuthority.model_validate(authority)
    if validated.quality_status is not BatchQualityStatus.PUBLISHED:
        raise CandidateAuthorityQualityError(
            f"candidate authority must be published, got {validated.quality_status.value}"
        )
    return validated


def _validated_facts(
    facts: Sequence[FactT],
    fact_type: type[FactT],
) -> tuple[FactT, ...]:
    if isinstance(facts, (str, bytes, bytearray)) or not isinstance(facts, Sequence):
        raise TypeError("facts must be a sequence of frozen fact models")
    validated: list[FactT] = []
    for fact in facts:
        if not isinstance(fact, fact_type):
            raise TypeError(f"facts must contain only {fact_type.__name__}")
        validated.append(fact_type.model_validate(fact))
    return tuple(validated)


def _ensure_available_by_capture(
    *,
    available_at: AwareUtcDatetime,
    authority: PublishedCandidateInputAuthority,
    label: str,
) -> None:
    if available_at > authority.captured_at:
        raise CandidatePointInTimeError(
            f"{label} available_at cannot be later than authority captured_at"
        )


def _ensure_prior_reference(
    *,
    reference_trade_date: date,
    authority: PublishedCandidateInputAuthority,
    label: str = "reference_trade_date",
) -> None:
    if reference_trade_date >= authority.trade_date:
        raise CandidatePointInTimeError(
            f"{label} must be earlier than the current session trade_date"
        )


def _eligible(fact: _SessionCandidateFact) -> bool:
    return (
        fact.is_st is False
        and fact.is_suspended is False
        and fact.is_listed is True
        and fact.limit_eligible is True
    )


def _merge_lineage(
    authority: PublishedCandidateInputAuthority,
    references: Mapping[str, str],
    additions: Sequence[tuple[str, str]],
) -> dict[str, str]:
    merged = dict(references)
    for key, snapshot_id in (
        ("candidate_authority", authority.authority_snapshot_id),
        *additions,
    ):
        existing = merged.get(key)
        if existing is not None and existing != snapshot_id:
            raise CandidateInputConflictError(
                f"reference lineage key {key!r} binds conflicting snapshots"
            )
        merged[key] = snapshot_id
    return dict(sorted(merged.items()))


def _deduplicate_by_code(
    facts: Sequence[FactT],
    *,
    strategy_id: str,
    fingerprint: Callable[[FactT], str] = canonical_sha256,
) -> tuple[FactT, ...]:
    grouped: dict[str, list[FactT]] = {}
    for fact in facts:
        ts_code = fact.ts_code
        grouped.setdefault(ts_code, []).append(fact)
    result: list[FactT] = []
    for ts_code, duplicates in sorted(grouped.items()):
        fingerprints = {fingerprint(fact) for fact in duplicates}
        if len(fingerprints) != 1:
            raise CandidateInputConflictError(f"{strategy_id} has conflicting facts for {ts_code}")
        result.append(duplicates[0])
    return tuple(result)


def _auction_deduplication_sha256(fact: AuctionMatchFact) -> str:
    payload = fact.model_dump(mode="python")
    payload.pop("source_volume_ratio", None)
    return canonical_sha256(payload)


def _candidate_record(
    *,
    strategy_id: Literal["n_shape", "growth_board_surge", "auction_gap"],
    candidate_id: str,
    variant: str,
    available_at: AwareUtcDatetime,
    authority: PublishedCandidateInputAuthority,
    reference_trade_date: date,
    static_features: Mapping[str, JsonValue],
    reference_snapshot_ids: Mapping[str, str],
) -> StrategyCandidateRecord:
    return StrategyCandidateRecord(
        strategy_id=strategy_id,
        strategy_version="1",
        candidate_id=candidate_id,
        variant=variant,
        decision_at=available_at,
        available_at=available_at,
        effective_trade_date=authority.trade_date,
        reference_trade_date=reference_trade_date,
        price_basis=StrategyCandidatePriceBasis.RAW,
        static_features=static_features,
        reference_snapshot_ids=reference_snapshot_ids,
    )


def produce_n_shape_candidates(
    *,
    authority: PublishedCandidateInputAuthority,
    facts: Sequence[NShapePoolFact],
) -> tuple[StrategyCandidateRecord, ...]:
    authority = _validated_authority(authority)
    validated = _validated_facts(facts, NShapePoolFact)
    grouped: dict[str, dict[str, list[NShapePoolFact]]] = {}
    for fact in validated:
        _ensure_available_by_capture(
            available_at=fact.available_at,
            authority=authority,
            label=f"n_shape {fact.ts_code}",
        )
        _ensure_prior_reference(
            reference_trade_date=fact.reference_trade_date,
            authority=authority,
        )
        _ensure_prior_reference(
            reference_trade_date=fact.prior_session_trade_date,
            authority=authority,
            label="prior_session_trade_date",
        )
        _ensure_prior_reference(
            reference_trade_date=fact.expected_prior_session_trade_date,
            authority=authority,
            label="expected_prior_session_trade_date",
        )
        grouped.setdefault(fact.ts_code, {}).setdefault(fact.variant, []).append(fact)

    selected: list[NShapePoolFact] = []
    for ts_code, by_variant in sorted(grouped.items()):
        unique: dict[str, NShapePoolFact] = {}
        for variant, duplicates in sorted(by_variant.items()):
            fingerprints = {canonical_sha256(fact) for fact in duplicates}
            if len(fingerprints) != 1:
                raise CandidateInputConflictError(
                    f"n_shape has conflicting facts for {ts_code} {variant}"
                )
            unique[variant] = duplicates[0]
        selected.append(unique["pool2"] if "pool2" in unique else unique["pool1"])

    records: list[StrategyCandidateRecord] = []
    for fact in selected:
        if not _eligible(fact):
            continue
        raw_values = (
            fact.t_close_raw,
            fact.t_high_raw,
            fact.reference_adj_factor,
            fact.prior_session_close_raw,
            fact.prior_session_adj_factor,
        )
        if any(value <= 0.0 for value in raw_values) or fact.t_high_raw < fact.t_close_raw:
            continue
        ratio = (
            fact.reference_adj_factor
            / fact.prior_session_adj_factor
            * fact.session_pre_close_raw
            / fact.prior_session_close_raw
        )
        t_close_session_raw = fact.t_close_raw * ratio
        t_high_session_raw = fact.t_high_raw * ratio
        if (
            not math.isfinite(ratio)
            or ratio <= 0.0
            or not math.isfinite(t_close_session_raw)
            or not math.isfinite(t_high_session_raw)
            or t_close_session_raw <= 0.0
            or t_high_session_raw < t_close_session_raw
        ):
            continue
        records.append(
            _candidate_record(
                strategy_id="n_shape",
                candidate_id=fact.ts_code,
                variant=fact.variant,
                available_at=max(authority.captured_at, fact.available_at),
                authority=authority,
                reference_trade_date=fact.reference_trade_date,
                static_features={
                    "candidate_price_basis": _CANDIDATE_BASIS,
                    "t_close_session_raw": t_close_session_raw,
                    "t_high_session_raw": t_high_session_raw,
                    "limit_up_price_session_raw": fact.limit_up_price_session_raw,
                    "limit_pct": fact.limit_pct,
                },
                reference_snapshot_ids=_merge_lineage(
                    authority,
                    fact.reference_snapshot_ids,
                    (),
                ),
            )
        )
    return tuple(sorted(records, key=lambda record: record.identity))


def produce_growth_board_surge_candidates(
    *,
    authority: PublishedCandidateInputAuthority,
    facts: Sequence[GrowthBoardFact],
) -> tuple[StrategyCandidateRecord, ...]:
    authority = _validated_authority(authority)
    validated = _validated_facts(facts, GrowthBoardFact)
    for fact in validated:
        _ensure_available_by_capture(
            available_at=fact.available_at,
            authority=authority,
            label=f"growth_board_surge {fact.ts_code}",
        )
        _ensure_prior_reference(
            reference_trade_date=fact.reference_trade_date,
            authority=authority,
        )
        _ensure_prior_reference(
            reference_trade_date=fact.prior_session_trade_date,
            authority=authority,
            label="prior_session_trade_date",
        )
        _ensure_prior_reference(
            reference_trade_date=fact.expected_prior_session_trade_date,
            authority=authority,
            label="expected_prior_session_trade_date",
        )

    records: list[StrategyCandidateRecord] = []
    for fact in _deduplicate_by_code(validated, strategy_id="growth_board_surge"):
        ma_values = (fact.ma5, fact.ma10, fact.ma20, fact.ma60)
        if (
            not _eligible(fact)
            or any(value <= 0.0 for value in ma_values)
            or not (fact.ma5 > fact.ma10 > fact.ma20 > fact.ma60)
            or fact.large_net_vol_t1 <= 0.0
            or fact.historical_sessions < 5
        ):
            continue
        records.append(
            _candidate_record(
                strategy_id="growth_board_surge",
                candidate_id=fact.ts_code,
                variant=fact.board_type,
                available_at=max(authority.captured_at, fact.available_at),
                authority=authority,
                reference_trade_date=fact.reference_trade_date,
                static_features={
                    "candidate_price_basis": _CANDIDATE_BASIS,
                    "session_pre_close_raw": fact.session_pre_close_raw,
                    "limit_up_price_session_raw": fact.limit_up_price_session_raw,
                    "board_type": fact.board_type,
                    "ma_alignment": True,
                    "large_net_vol_t1": fact.large_net_vol_t1,
                },
                reference_snapshot_ids=_merge_lineage(
                    authority,
                    fact.reference_snapshot_ids,
                    (),
                ),
            )
        )
    return tuple(sorted(records, key=lambda record: record.identity))


def produce_auction_gap_candidates(
    *,
    authority: PublishedCandidateInputAuthority,
    facts: Sequence[AuctionMatchFact],
) -> tuple[StrategyCandidateRecord, ...]:
    authority = _validated_authority(authority)
    validated = _validated_facts(facts, AuctionMatchFact)
    for fact in validated:
        if fact.trade_date != authority.trade_date:
            raise CandidatePointInTimeError("auction trade_date must match authority trade_date")
        _ensure_available_by_capture(
            available_at=fact.available_at,
            authority=authority,
            label=f"auction_gap {fact.ts_code}",
        )
        _ensure_available_by_capture(
            available_at=fact.calendar_available_at,
            authority=authority,
            label=f"trade calendar {fact.ts_code}",
        )
        for daily in fact.prior5_daily_volumes:
            _ensure_available_by_capture(
                available_at=daily.available_at,
                authority=authority,
                label=f"daily volume {fact.ts_code} {daily.trade_date.isoformat()}",
            )

    records: list[StrategyCandidateRecord] = []
    for fact in _deduplicate_by_code(
        validated,
        strategy_id="auction_gap",
        fingerprint=_auction_deduplication_sha256,
    ):
        if not _eligible(fact):
            continue
        prior_average_shares = (
            sum(item.daily_volume_lots for item in fact.prior5_daily_volumes)
            / 5.0
            * DAILY_VOLUME_LOT_SIZE_SHARES
        )
        if not math.isfinite(prior_average_shares) or prior_average_shares <= 0.0:
            continue
        auction_ratio = fact.auction_vol_shares / prior_average_shares
        gap_pct_close = (fact.auction_price_raw / fact.session_pre_close_raw - 1.0) * 100.0
        if (
            not math.isfinite(auction_ratio)
            or not math.isfinite(gap_pct_close)
            or not 0.15 <= auction_ratio <= 5.0
            or gap_pct_close <= 0.0
            or fact.auction_price_raw >= fact.limit_up_price_session_raw
        ):
            continue
        additions: list[tuple[str, str]] = [
            ("auction_match", fact.source_snapshot_id),
            ("trade_calendar", fact.calendar_snapshot_id),
        ]
        if fact.daily_snapshot_id is not None:
            additions.append(("daily_volume", fact.daily_snapshot_id))
        else:
            additions.extend(
                (
                    f"daily_volume:{daily.trade_date.isoformat()}",
                    daily.source_snapshot_id,
                )
                for daily in fact.prior5_daily_volumes
                if daily.source_snapshot_id is not None
            )
        available_at = max(
            (
                authority.captured_at,
                fact.available_at,
                fact.calendar_available_at,
                *(item.available_at for item in fact.prior5_daily_volumes),
            )
        )
        records.append(
            _candidate_record(
                strategy_id="auction_gap",
                candidate_id=fact.ts_code,
                variant="auction_gap",
                available_at=available_at,
                authority=authority,
                reference_trade_date=fact.trade_date,
                static_features={
                    "candidate_price_basis": _CANDIDATE_BASIS,
                    "auction_price_raw": fact.auction_price_raw,
                    "auction_vol_ratio_5d": auction_ratio,
                    "gap_pct_close": gap_pct_close,
                    "limit_up_price_session_raw": fact.limit_up_price_session_raw,
                },
                reference_snapshot_ids=_merge_lineage(
                    authority,
                    fact.reference_snapshot_ids,
                    additions,
                ),
            )
        )
    return tuple(sorted(records, key=lambda record: record.identity))


__all__ = (
    "AUCTION_GAP_REQUIRED_LINEAGE_KEYS",
    "AuctionMatchFact",
    "CandidateAuthorityQualityError",
    "CandidateInputConflictError",
    "CandidatePointInTimeError",
    "CandidateProducerError",
    "DAILY_VOLUME_LOT_SIZE_SHARES",
    "GROWTH_BOARD_REQUIRED_LINEAGE_KEYS",
    "GrowthBoardFact",
    "N_SHAPE_REQUIRED_LINEAGE_KEYS",
    "NShapePoolFact",
    "PriorDailyVolumeFact",
    "PublishedCandidateInputAuthority",
    "produce_auction_gap_candidates",
    "produce_growth_board_surge_candidates",
    "produce_n_shape_candidates",
)
