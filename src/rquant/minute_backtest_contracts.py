"""Closed, pathless input for replaying the original paper runtime."""

from __future__ import annotations

import base64
import hashlib
import re
from datetime import date, time
from decimal import Decimal
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Annotated, Literal, Self
from zoneinfo import ZoneInfo

from pydantic import ConfigDict, Field, StringConstraints, model_validator

from rquant.intraday_feature_engine import IntradayFeatureConfig
from rquant.live_contracts import CurrentPointer
from rquant.paper_contracts import PaperAccountSnapshot
from rquant.paper_signal_worker import PaperQuoteSnapshot, PaperSignalPolicy
from rquant.paper_execution_constraints import PaperExecutionConstraintBatch, PaperExecutionConstraintPointer
from rquant.research_run_spec import ExecutionCostSpec
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.runtime_definition_bootstrap import BuiltinDefinitionStrategyBinding
from rquant.runtime_market_session import MarketCalendarAuthority

if TYPE_CHECKING:
    from rquant.minute_backtest_parameter_contracts import MinuteParameterRuntimeContent


MAX_INPUT_BYTES = 16_777_216
MAX_CODES = 500
MAX_DATE_SPAN = 1_830
MAX_WORK_UNITS = 20_000
MAX_RESULT_TABLE_BYTES = 33_554_432
MAX_RESULT_TOTAL_BYTES = 62_128_104
MAX_RESULT_WIRE_BYTES = 83_886_080
MINUTE_RUNTIME_SOURCE_CONTRACT = "minute-runtime-replay-input/v1"
MINUTE_RUNTIME_INPUT_TABLE = "minute_runtime_replay_input"
Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
CommitSha = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]


class MinuteReplayModel(RuntimeContractModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True, revalidate_instances="always")


def _material_path(value: str) -> str:
    path = PurePosixPath(value)
    if path.is_absolute() or path.as_posix() != value or any(part in {".", ".."} for part in path.parts):
        raise ValueError("replay material must use a closed relative path")
    if value in {"calendar.json", "warmup.parquet", "routing-policy.json", "seed/broker.sqlite3", "seed/runner.sqlite3"}:
        return value
    if re.fullmatch(r"seed/features/(?:current\.json|source-identity\.json|batches/[0-9]{20}\.(?:json|payload)|cursors/[0-9a-f]{64}\.json|sessions/[0-9]{4}-[0-9]{2}-[0-9]{2}/segment\.json)", value):
        return value
    if re.fullmatch(r"seed/raw-cursors/[0-9a-f]{64}\.json", value):
        return value
    if re.fullmatch(r"(?:candidates|constraints)/(?:authority\.json|current\.json|generation-index\.json|generations/[a-zA-Z0-9_.-]+\.json)", value):
        return value
    if re.fullmatch(r"market/(?:batches|publication-receipts)/market_minute/[a-zA-Z0-9_.-]+", value):
        return value
    if re.fullmatch(r"market/(?:current|sources)/market_minute\.json", value):
        return value
    if re.fullmatch(r"constraint-publications/[0-9]+\.json", value):
        return value
    raise ValueError("replay material path is outside the closed input namespaces")


class MinuteReplayMaterial(MinuteReplayModel):
    relative_path: str = Field(min_length=1, max_length=256)
    content_base64: str = Field(max_length=((MAX_INPUT_BYTES + 2) // 3) * 4)
    content_sha256: Sha256

    @model_validator(mode="after")
    def validate_material(self) -> Self:
        _material_path(self.relative_path)
        payload = self.payload()
        if len(payload) > MAX_INPUT_BYTES:
            raise ValueError("restored replay material exceeds byte budget")
        if hashlib.sha256(payload).hexdigest() != self.content_sha256:
            raise ValueError("replay material hash differs from original bytes")
        return self

    def payload(self) -> bytes:
        try:
            result = base64.b64decode(self.content_base64, validate=True)
        except (ValueError, UnicodeEncodeError) as error:
            raise ValueError("replay material is not canonical base64") from error
        if base64.b64encode(result).decode("ascii") != self.content_base64:
            raise ValueError("replay material is not canonical base64")
        return result


class MinuteReplayWork(MinuteReplayModel):
    raw_rows: int = Field(ge=0)
    warmup_rows: int = Field(ge=0)
    static_rows: int = Field(ge=0)
    market_batches: int = Field(ge=1)
    union_codes: int = Field(ge=1, le=MAX_CODES)
    daily_observations: int = Field(ge=1, le=MAX_DATE_SPAN)

    @model_validator(mode="after")
    def bound_work(self) -> Self:
        if self.work_units > MAX_WORK_UNITS:
            raise ValueError("minute replay work exceeds the original finite input budget")
        return self

    @property
    def candidate_bound(self) -> int:
        return self.market_batches * self.union_codes

    @property
    def paper_bound(self) -> int:
        return self.candidate_bound

    @property
    def daily_price_bound(self) -> int:
        return self.daily_observations * self.union_codes

    @property
    def work_units(self) -> int:
        return (self.raw_rows + self.warmup_rows + self.static_rows + self.candidate_bound
            + self.paper_bound + self.market_batches + self.daily_observations + self.daily_price_bound)

    @property
    def static_duration_ms(self) -> int:
        return self.work_units * 1_000


class MinuteReplayResultBudget(MinuteReplayModel):
    """Original fixed artifact limits, including both required daily result tables."""

    table_count: Literal[8] = 8
    table_bytes: Literal[33_554_432] = MAX_RESULT_TABLE_BYTES
    total_bytes: Literal[62_128_104] = MAX_RESULT_TOTAL_BYTES
    wire_bytes: Literal[83_886_080] = MAX_RESULT_WIRE_BYTES


class MinuteReplayConstraintPublication(MinuteReplayModel):
    pointer: PaperExecutionConstraintPointer
    batch: PaperExecutionConstraintBatch

    @model_validator(mode="after")
    def original_publication(self) -> Self:
        if (self.pointer.sequence, self.pointer.batch_hash, self.pointer.producer_commit) != (
            self.batch.sequence, self.batch.content_hash, self.batch.producer_commit
        ) or any(record.available_at > self.pointer.published_at for record in self.batch.records):
            raise ValueError("minute replay constraint publication is detached or future")
        return self


class MinuteReplayExecutionProfile(MinuteReplayModel):
    key: str = Field(pattern=r"^[a-zA-Z0-9_.:-]{1,128}$")
    version: int = Field(ge=1)
    initial_cash: Decimal = Field(gt=0, allow_inf_nan=False)
    execution_costs: ExecutionCostSpec
    paper_policy: PaperSignalPolicy
    routing_policy_fingerprint: Sha256
    timestamp_semantics: Literal["bar_end", "provider_snapshot"] = "provider_snapshot"
    candidate_max_age_seconds: int = Field(default=7 * 24 * 60 * 60, gt=0)
    quote_max_age_seconds: int = Field(default=90, gt=0, le=300)
    max_finalize_scan_batches: int = Field(default=32, gt=0, le=120)
    max_visible_scan_batches: int = Field(default=120, gt=0, le=1_000)

    @model_validator(mode="after")
    def require_known_costs(self) -> Self:
        if self.execution_costs.schema_version != 3:
            raise ValueError("minute runtime requires the original known v3 execution costs")
        return self

    @property
    def profile_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class MinuteReplayDailyPriceProof(MinuteReplayModel):
    entry_signal_id: Sha256
    quote: PaperQuoteSnapshot


class MinuteRuntimeDailyValuation(MinuteReplayModel):
    input_hash: Sha256
    trade_date: date
    as_of: AwareUtcDatetime
    observed_at: AwareUtcDatetime | None
    basis: Literal["pit_asof_15:00"] = "pit_asof_15:00"
    profile_hash: Sha256
    calendar_sha256: Sha256
    status: Literal["complete", "unavailable"]
    market_pointer: CurrentPointer | None = None
    constraint_pointer: PaperExecutionConstraintPointer | None = None
    price_proofs: tuple[MinuteReplayDailyPriceProof, ...] = Field(default=(), max_length=MAX_CODES)
    account: PaperAccountSnapshot | None = None
    unavailable_reasons: tuple[str, ...] = Field(default=(), max_length=MAX_CODES + 1)

    @model_validator(mode="after")
    def explicit_original_valuation(self) -> Self:
        local = self.as_of.astimezone(ZoneInfo("Asia/Shanghai"))
        if local.date() != self.trade_date or local.time().replace(tzinfo=None) != time(15):
            raise ValueError("daily valuation requires the original trading day 15:00 cutoff")
        if self.observed_at is not None and self.observed_at != self.as_of:
            raise ValueError("daily valuation cannot move its original observation clock")
        for pointer in (self.market_pointer, self.constraint_pointer):
            if pointer is not None and pointer.published_at > self.as_of:
                raise ValueError("daily valuation source pointer is from the future")
        codes = tuple(proof.quote.ts_code for proof in self.price_proofs)
        if codes != tuple(sorted(set(codes))):
            raise ValueError("daily valuation price proofs must be unique and ordered")
        if any(proof.quote.available_at > self.as_of or proof.quote.event_time > self.as_of for proof in self.price_proofs):
            raise ValueError("daily valuation cannot use future prices")
        if self.status == "unavailable":
            if self.account is not None or not self.unavailable_reasons:
                raise ValueError("unavailable daily valuation cannot supply a complete account")
        elif (self.account is None or self.observed_at is None or self.market_pointer is None
            or self.unavailable_reasons or self.account.as_of_time != self.as_of):
            raise ValueError("complete daily valuation requires its original clock and account evidence")
        elif tuple(holding.code for holding in self.account.holdings) != codes or any(
            holding.market_price != proof.quote.context.executable_price
            for holding, proof in zip(self.account.holdings, self.price_proofs, strict=True)
        ):
            raise ValueError("daily valuation held prices differ from original PIT quote evidence")
        return self


class FrozenMinuteRuntimeInput(MinuteReplayModel):
    contract: Literal["minute-runtime-replay-input/v1"] = MINUTE_RUNTIME_SOURCE_CONTRACT
    source_key: str = Field(pattern=r"^[a-zA-Z0-9_.:-]{1,128}$")
    source_version: int = Field(ge=1)
    owner_id: str = Field(min_length=1, max_length=128)
    producer_commit: CommitSha
    available_at: AwareUtcDatetime
    start_date: date
    end_date: date
    complete_through: AwareUtcDatetime
    warmup_available_at: AwareUtcDatetime
    warmup_complete: Literal[True]
    holding_tail_complete: Literal[True]
    audit_run_id: str = Field(min_length=1, max_length=128)
    dataset_snapshot_id: Sha256
    strategy: BuiltinDefinitionStrategyBinding
    feature_config: IntradayFeatureConfig
    market_calendar: MarketCalendarAuthority
    execution_profile: MinuteReplayExecutionProfile
    work: MinuteReplayWork
    result_budget: MinuteReplayResultBudget = MinuteReplayResultBudget()
    tick_times: tuple[AwareUtcDatetime, ...] = Field(min_length=1, max_length=MAX_WORK_UNITS)
    materials: tuple[MinuteReplayMaterial, ...] = Field(min_length=1, max_length=MAX_WORK_UNITS * 4 + 128)

    @model_validator(mode="after")
    def closed_input(self) -> Self:
        _validate_minute_replay_archive(self)
        if self.feature_config.producer_commit != self.producer_commit:
            raise ValueError("minute replay code provenance differs across its inputs")
        from rquant.minute_backtest_validation import original_builtin_minute_plan
        plan = original_builtin_minute_plan(producer_commit=self.producer_commit)
        if self.strategy not in plan.strategies:
            raise ValueError("minute replay strategy registration or executable differs from the original definition")
        return self

    @property
    def input_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))

    @property
    def daily_trade_dates(self) -> tuple[date, ...]:
        return tuple(day for day in self.market_calendar.open_dates if self.start_date <= day <= self.end_date)


class MinuteRuntimeSourceReceipt(MinuteReplayModel):
    """Independent trusted caller receipt; payload cannot grant its own authority."""

    source_key: str = Field(pattern=r"^[a-zA-Z0-9_.:-]{1,128}$")
    source_version: int = Field(ge=1)
    owner_id: str = Field(min_length=1, max_length=128)
    input_hash: Sha256
    producer_commit: CommitSha
    start_date: date
    end_date: date
    audit_run_id: str = Field(min_length=1, max_length=128)
    dataset_snapshot_id: Sha256
    work: MinuteReplayWork
    result_budget: MinuteReplayResultBudget = MinuteReplayResultBudget()
    profile_hash: Sha256
    strategy_id: Literal["n_shape", "auction_gap", "growth_board_surge"]
    strategy_version: Literal[1]

    def verify(self, value: FrozenMinuteRuntimeInput) -> None:
        for name in ("source_key", "source_version", "owner_id", "input_hash", "producer_commit", "start_date", "end_date", "audit_run_id", "dataset_snapshot_id", "work", "result_budget"):
            if getattr(self, name) != getattr(value, name):
                raise ValueError(f"minute runtime independent source receipt differs: {name}")
        if (self.profile_hash, self.strategy_id, self.strategy_version) != (value.execution_profile.profile_hash, value.strategy.strategy_id, value.strategy.strategy_version):
            raise ValueError("minute runtime independent source receipt differs: execution profile or strategy")


def _validate_minute_replay_archive(
    value: FrozenMinuteRuntimeInput | MinuteParameterRuntimeContent,
) -> None:
    """Shared original archive bounds; strategy-specific authority stays closed."""
    if not 1 <= (value.end_date - value.start_date).days + 1 <= MAX_DATE_SPAN:
        raise ValueError("minute replay range exceeds date budget")
    if any(left >= right for left, right in zip(value.tick_times, value.tick_times[1:])):
        raise ValueError("minute replay clock must be strictly ordered")
    if value.available_at > value.tick_times[0] or value.warmup_available_at > value.tick_times[0]:
        raise ValueError("minute replay input or warmup was not visible at the first observation")
    if value.complete_through < value.tick_times[-1]:
        raise ValueError("minute replay is missing its complete holding tail")
    if value.market_calendar.generated_at > value.tick_times[0]:
        raise ValueError("minute replay calendar is from the future")
    if not (value.market_calendar.coverage_start <= value.start_date <= value.end_date <= value.market_calendar.coverage_end):
        raise ValueError("minute replay date range is outside calendar coverage")
    if not any(day > value.end_date for day in value.market_calendar.open_dates):
        raise ValueError("minute replay calendar lacks the next acquisition session")
    if value.work.daily_observations != len(value.daily_trade_dates):
        raise ValueError("minute replay daily work differs from the original covered open days")
    if {value.market_calendar.producer_commit, value.execution_profile.paper_policy.producer_commit} != {value.producer_commit}:
        raise ValueError("minute replay code provenance differs across its inputs")
    paths = tuple(item.relative_path for item in value.materials)
    if paths != tuple(sorted(set(paths))):
        raise ValueError("minute replay materials must be sorted and unique")
    if not {"calendar.json", "warmup.parquet", "routing-policy.json", "candidates/authority.json", "candidates/current.json", "constraints/current.json", "market/sources/market_minute.json", "market/current/market_minute.json"}.issubset(paths):
        raise ValueError("minute replay is missing original source authority material")
    if not any(path.startswith("constraint-publications/") for path in paths):
        raise ValueError("minute replay requires original constraint publication receipts")
    routing = next(item for item in value.materials if item.relative_path == "routing-policy.json")
    if routing.content_sha256 != value.execution_profile.routing_policy_fingerprint:
        raise ValueError("minute replay original routing policy differs from its profile")
    if ("seed/broker.sqlite3" in paths) != ("seed/runner.sqlite3" in paths):
        raise ValueError("minute replay requires broker and runner seed together")
    seeded = "seed/broker.sqlite3" in paths
    if any(path.startswith("seed/") for path in paths) and not seeded:
        raise ValueError("minute replay cursor seed lacks original broker and runner")
    if seeded and (not {"seed/features/current.json", "seed/features/source-identity.json"}.issubset(paths)
        or not any(path.startswith("seed/features/cursors/") for path in paths)
        or not any(path.startswith("seed/raw-cursors/") for path in paths)):
        raise ValueError("minute replay seed requires complete original feature and raw cursors")
    if sum(len(item.payload()) for item in value.materials) > MAX_INPUT_BYTES:
        raise ValueError("restored minute source materials exceed byte budget")
    if len(value.model_dump_json().encode("utf-8")) > MAX_INPUT_BYTES:
        raise ValueError("minute replay payload exceeds UTF-8 byte budget")
