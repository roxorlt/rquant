"""Pure daily pool selection over trusted, normalized A-share source claims.

The supplied security manifest is checked against its facts. It does not prove
provider coverage, point-in-time collection, or the ability to trade a stock.
"""

from __future__ import annotations

import re
from datetime import date, timedelta, timezone
from typing import Annotated, Literal

from pydantic import (
    AfterValidator,
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)
from pydantic_core import PydanticCustomError

from rquant.runtime_contracts import canonical_sha256, normalize_aware_utc

MAX_UNIVERSE_SECURITIES = 7_000
UniverseSelection = Literal["all", "hs300", "zz1000", "gem"]
SecurityBoard = Literal["main", "gem", "star", "bse"]
UniverseErrorReason = Literal[
    "security_source_missing",
    "security_date_mismatch",
    "security_source_not_visible",
    "security_date_in_future",
    "index_source_missing",
    "index_selection_mismatch",
    "index_date_mismatch",
    "index_source_not_visible",
    "index_date_in_future",
    "index_member_missing",
    "index_member_not_listed",
    "unexpected_index_source",
    "st_status_unknown",
    "missing_board_fact",
]
_IMMUTABLE = ConfigDict(extra="forbid", frozen=True, strict=True, revalidate_instances="always")
_MARKET_TZ = timezone(timedelta(hours=8))
_STOCK_PATTERN = re.compile(r"[0-9]{6}\.(?:SH|SZ|BJ)\Z")
_BOARD_EXCHANGES = {
    "main": frozenset({"SH", "SZ"}),
    "gem": frozenset({"SZ"}),
    "star": frozenset({"SH"}),
    "bse": frozenset({"BJ"}),
}


def _valid_stock_code(value: str) -> str:
    if _STOCK_PATTERN.fullmatch(value) is None:
        raise PydanticCustomError("factor_universe_invalid_stock_code", "invalid security code")
    return value


def _valid_source_id(value: str) -> str:
    if not value or value != value.strip() or not value.isprintable():
        raise PydanticCustomError("factor_universe_invalid_source_id", "invalid source identity")
    return value


StockCode = Annotated[str, AfterValidator(_valid_stock_code)]
SourceId = Annotated[str, AfterValidator(_valid_source_id)]
Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
ObservedTime = Annotated[AwareDatetime, AfterValidator(normalize_aware_utc)]


class FactorUniverseError(ValueError):
    """Stable refusal of the whole daily selection."""

    def __init__(self, reason: UniverseErrorReason) -> None:
        self.reason = reason
        super().__init__(reason)


class DailySecurityFact(BaseModel):
    model_config = _IMMUTABLE

    stock_code: StockCode
    exchange: Literal["SH", "SZ", "BJ"]
    board: SecurityBoard | None
    is_listed: bool
    is_st: bool | None

    @model_validator(mode="after")
    def _consistent_exchange(self) -> DailySecurityFact:
        if self.stock_code[-2:] != self.exchange:
            raise PydanticCustomError(
                "factor_universe_exchange_suffix_mismatch", "exchange differs from code suffix"
            )
        if self.board is not None and self.exchange not in _BOARD_EXCHANGES[self.board]:
            raise PydanticCustomError(
                "factor_universe_exchange_board_mismatch", "board differs from exchange"
            )
        return self


class DailySecurityBatch(BaseModel):
    """Caller-declared China A-share scope and complete single-day manifest."""

    model_config = _IMMUTABLE

    trade_date: date
    source_id: SourceId
    source_sha256: Sha256
    source_mode: Literal["historical_retrospective"]
    security_scope: Literal["china_a_share"]
    observed_at: ObservedTime
    complete_stock_codes: tuple[StockCode, ...] = Field(max_length=MAX_UNIVERSE_SECURITIES)
    facts: tuple[DailySecurityFact, ...] = Field(max_length=MAX_UNIVERSE_SECURITIES)

    @field_validator("complete_stock_codes")
    @classmethod
    def _unique_manifest(cls, codes: tuple[str, ...]) -> tuple[str, ...]:
        if len(set(codes)) != len(codes):
            raise PydanticCustomError(
                "factor_universe_duplicate_security_manifest", "duplicate security manifest entry"
            )
        return tuple(sorted(codes))

    @field_validator("facts")
    @classmethod
    def _unique_facts(cls, facts: tuple[DailySecurityFact, ...]) -> tuple[DailySecurityFact, ...]:
        if len({fact.stock_code for fact in facts}) != len(facts):
            raise PydanticCustomError(
                "factor_universe_duplicate_security_fact", "duplicate security fact"
            )
        return tuple(sorted(facts, key=lambda fact: fact.stock_code))

    @model_validator(mode="after")
    def _same_manifest(self) -> DailySecurityBatch:
        if set(self.complete_stock_codes) != {fact.stock_code for fact in self.facts}:
            raise PydanticCustomError(
                "factor_universe_security_manifest_mismatch", "security manifest differs from facts"
            )
        return self


class DailyIndexConstituentBatch(BaseModel):
    """Only a source-normalized complete list for this particular day."""

    model_config = _IMMUTABLE

    selection: Literal["hs300", "zz1000"]
    trade_date: date
    source_id: SourceId
    source_sha256: Sha256
    source_mode: Literal["historical_retrospective"]
    source_kind: Literal["daily_complete_membership"]
    observed_at: ObservedTime
    stock_codes: tuple[StockCode, ...] = Field(max_length=MAX_UNIVERSE_SECURITIES)

    @field_validator("stock_codes")
    @classmethod
    def _unique_members(cls, codes: tuple[str, ...]) -> tuple[str, ...]:
        if len(set(codes)) != len(codes):
            raise PydanticCustomError(
                "factor_universe_duplicate_index_member", "duplicate index member"
            )
        return tuple(sorted(codes))


class FactorUniverseRequest(BaseModel):
    """One retrospective day and the sources required by its chosen pool."""

    model_config = _IMMUTABLE

    selection: UniverseSelection
    trade_date: date
    as_of: ObservedTime
    securities: DailySecurityBatch | None = None
    membership: DailyIndexConstituentBatch | None = None


class FactorUniverseExclusions(BaseModel):
    """Disjoint removals in listing, exchange, then status/board order."""

    model_config = _IMMUTABLE

    not_listed: int = Field(default=0, ge=0, le=MAX_UNIVERSE_SECURITIES)
    beijing: int = Field(default=0, ge=0, le=MAX_UNIVERSE_SECURITIES)
    st: int = Field(default=0, ge=0, le=MAX_UNIVERSE_SECURITIES)
    non_target_board: int = Field(default=0, ge=0, le=MAX_UNIVERSE_SECURITIES)


class FactorUniverseResult(BaseModel):
    """Input count is the whole security batch or the supplied index member count."""

    model_config = _IMMUTABLE

    selection: UniverseSelection
    trade_date: date
    source_mode: Literal["historical_retrospective"]
    stock_codes: tuple[StockCode, ...] = Field(max_length=MAX_UNIVERSE_SECURITIES)
    security_count: int = Field(ge=0, le=MAX_UNIVERSE_SECURITIES)
    input_count: int = Field(ge=0, le=MAX_UNIVERSE_SECURITIES)
    selected_count: int = Field(ge=0, le=MAX_UNIVERSE_SECURITIES)
    excluded: FactorUniverseExclusions
    security_observed_at: ObservedTime
    index_observed_at: ObservedTime | None
    input_sha256: Sha256


def select_factor_universe(request: FactorUniverseRequest) -> FactorUniverseResult:
    """Select one day's pool without reading sources or modifying research state."""
    request = FactorUniverseRequest.model_validate(request)
    batch = request.securities
    if batch is None:
        raise FactorUniverseError("security_source_missing")
    if batch.trade_date != request.trade_date:
        raise FactorUniverseError("security_date_mismatch")
    if batch.observed_at > request.as_of:
        raise FactorUniverseError("security_source_not_visible")
    if batch.trade_date > batch.observed_at.astimezone(_MARKET_TZ).date():
        raise FactorUniverseError("security_date_in_future")
    facts = {fact.stock_code: fact for fact in batch.facts}
    if request.selection in ("hs300", "zz1000"):
        members = request.membership
        if members is None:
            raise FactorUniverseError("index_source_missing")
        if members.selection != request.selection:
            raise FactorUniverseError("index_selection_mismatch")
        if members.trade_date != request.trade_date:
            raise FactorUniverseError("index_date_mismatch")
        if members.observed_at > request.as_of:
            raise FactorUniverseError("index_source_not_visible")
        if members.trade_date > members.observed_at.astimezone(_MARKET_TZ).date():
            raise FactorUniverseError("index_date_in_future")
        if any(code not in facts for code in members.stock_codes):
            raise FactorUniverseError("index_member_missing")
        candidates = tuple(facts[code] for code in members.stock_codes)
        if any(not fact.is_listed for fact in candidates):
            raise FactorUniverseError("index_member_not_listed")
    else:
        if request.membership is not None:
            raise FactorUniverseError("unexpected_index_source")
        candidates = batch.facts
    selected: list[str] = []
    not_listed = beijing = st = non_target_board = 0
    for fact in candidates:
        if not fact.is_listed:
            not_listed += 1
        elif request.selection == "all" and fact.exchange == "BJ":
            beijing += 1
        elif request.selection == "all" and fact.is_st is None:
            raise FactorUniverseError("st_status_unknown")
        elif request.selection == "all" and fact.is_st:
            st += 1
        elif request.selection == "gem" and fact.exchange != "BJ" and fact.board is None:
            raise FactorUniverseError("missing_board_fact")
        elif request.selection == "gem" and fact.board not in ("gem", "star"):
            non_target_board += 1
        else:
            selected.append(fact.stock_code)
    return FactorUniverseResult(
        selection=request.selection,
        trade_date=request.trade_date,
        source_mode=batch.source_mode,
        stock_codes=tuple(sorted(selected)),
        security_count=len(batch.facts),
        input_count=len(candidates),
        selected_count=len(selected),
        excluded=FactorUniverseExclusions(
            not_listed=not_listed, beijing=beijing, st=st, non_target_board=non_target_board
        ),
        security_observed_at=batch.observed_at,
        index_observed_at=request.membership.observed_at if request.membership else None,
        input_sha256=canonical_sha256(request),
    )
