"""The frozen 2048-path, day-major backtest-return bootstrap."""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from datetime import date
from decimal import ROUND_HALF_EVEN, Decimal, localcontext
from typing import Literal, Self
from uuid import UUID

from pydantic import Field, TypeAdapter, model_validator

from rquant.backtest.contracts import SSECalendar
from rquant.paper_portfolio_models import PaperPortfolioConfiguration, Sha256
from rquant.research_run_spec import _parse_decimal
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.strategy_promotion_contracts import NativeMinuteForwardConfiguration

BOOTSTRAP_ALGORITHM = "paper-bootstrap-splitmix64-day-major-nearest-rank-v1"
BOOTSTRAP_SEED = 20261005
BOOTSTRAP_PATHS = 2048
MAX_BOOTSTRAP_DAYS = 2520


def require_complete_band_dates(dates: tuple[date, ...], calendar: SSECalendar) -> tuple[date, ...]:
    if not dates or dates != tuple(sorted(set(dates))):
        raise ValueError("paper band calendar dates are absent or out of order")
    period = calendar.dates[bisect_left(calendar.dates, dates[0]):bisect_right(calendar.dates, dates[-1])]
    if dates != period or len(period) > MAX_BOOTSTRAP_DAYS:
        raise ValueError("paper band calendar has a missing original open day or exceeds its budget")
    return dates


class BootstrapPoint(RuntimeContractModel):
    day_index: int = Field(strict=True, ge=1, le=MAX_BOOTSTRAP_DAYS)
    lower: Decimal = Field(ge=0, allow_inf_nan=False)
    upper: Decimal = Field(ge=0, allow_inf_nan=False)


def _draw(state: int, size: int) -> tuple[int, int]:
    mask = (1 << 64) - 1
    boundary = ((1 << 64) // size) * size
    while True:
        state = (state + 0x9E3779B97F4A7C15) & mask
        value = state
        value = ((value ^ (value >> 30)) * 0xBF58476D1CE4E5B9) & mask
        value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & mask
        value ^= value >> 31
        if value < boundary:
            return state, value % size


def bootstrap_daily_band(returns: tuple[Decimal, ...], *, days: int) -> tuple[BootstrapPoint, ...]:
    if type(days) is not int or not 1 <= days <= MAX_BOOTSTRAP_DAYS or not 1 <= len(returns) <= MAX_BOOTSTRAP_DAYS:
        raise ValueError("bootstrap daily input exceeds its fixed 2520-day budget")
    admitted = tuple(_parse_decimal(value, field_name="sealed daily return") for value in returns)
    if any(value < -1 or abs(value) > Decimal("1000000000000") for value in admitted):
        raise ValueError("sealed daily return exceeds the original finite monetary ratio budget")
    # This list is the entire path work set; no 2048 by 2520 matrix is allocated.
    paths = [Decimal(1)] * BOOTSTRAP_PATHS
    state = BOOTSTRAP_SEED
    result = []
    with localcontext() as context:
        context.prec = 34
        context.rounding = ROUND_HALF_EVEN
        multipliers = tuple(1 + value for value in admitted)
        for day in range(days):
            for path in range(BOOTSTRAP_PATHS):
                state, index = _draw(state, len(admitted))
                paths[path] = paths[path] * multipliers[index]
            ordered = sorted(paths)
            result.append(BootstrapPoint(day_index=day + 1, lower=ordered[102], upper=ordered[1945]))
    return tuple(result)


class SealedPaperDailyReturn(RuntimeContractModel):
    trade_date: date
    daily_return: Decimal = Field(ge=-1, allow_inf_nan=False)


class SealedPaperBacktestReturns(RuntimeContractModel):
    contract: Literal["paper-sealed-backtest-returns/v1"] = "paper-sealed-backtest-returns/v1"
    job_id: UUID
    owner_id: str
    strategy_id: str
    strategy_version: str
    parameter_fingerprint: Sha256
    cost_spec_id: Sha256
    calendar_source_identity: Sha256
    definition_fingerprint: Sha256
    definition_record_hash: Sha256
    spec_hash: Sha256
    manifest_hash: Sha256
    complete_result_hash: Sha256
    backtest_content_hash: Sha256
    returns: tuple[SealedPaperDailyReturn, ...] = Field(min_length=1, max_length=MAX_BOOTSTRAP_DAYS)

    @model_validator(mode="after")
    def ordered(self) -> Self:
        dates = tuple(item.trade_date for item in self.returns)
        if dates != tuple(sorted(set(dates))):
            raise ValueError("sealed backtest daily returns are not complete and ordered")
        for item in self.returns:
            _parse_decimal(item.daily_return, field_name="sealed daily return")
        return self

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class PaperBacktestBandInput(RuntimeContractModel):
    contract: Literal["paper-backtest-band-input/v1"] = "paper-backtest-band-input/v1"
    configuration: PaperPortfolioConfiguration
    backtest: SealedPaperBacktestReturns
    calendar: SSECalendar
    comparison_dates: tuple[date, ...] = Field(min_length=1, max_length=MAX_BOOTSTRAP_DAYS)
    algorithm: Literal["paper-bootstrap-splitmix64-day-major-nearest-rank-v1"] = BOOTSTRAP_ALGORITHM
    seed: Literal[20261005] = BOOTSTRAP_SEED
    paths: Literal[2048] = BOOTSTRAP_PATHS

    @model_validator(mode="after")
    def exact_source(self) -> Self:
        binding, source = self.configuration.binding, self.backtest
        if (source.owner_id, source.strategy_id, source.strategy_version, source.parameter_fingerprint, source.cost_spec_id) != (
            binding.owner_id, binding.strategy_id, binding.strategy_version, binding.parameter_fingerprint, binding.cost_spec_id
        ):
            raise ValueError("paper band source differs from the exact account strategy version or cost")
        if self.calendar.source_identity != source.calendar_source_identity:
            raise ValueError("paper band calendar differs from the original sealed backtest")
        require_complete_band_dates(self.comparison_dates, self.calendar)
        require_complete_band_dates(tuple(item.trade_date for item in source.returns), self.calendar)
        if len(self.model_dump_json().encode()) > 1024 * 1024:
            raise ValueError("paper band input exceeds its immutable source byte budget")
        return self

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class NativeSealedPaperBacktestReturns(SealedPaperBacktestReturns):
    contract: Literal["native-sealed-backtest-returns/v1"] = "native-sealed-backtest-returns/v1"
    native_spec_fingerprint: Sha256
    profile_hash: Sha256
    full_input_hash: Sha256
    source_kind: Literal["captured", "reconstructed"]


class NativePaperBacktestBandInput(PaperBacktestBandInput):
    contract: Literal["native-paper-backtest-band-input/v1"] = "native-paper-backtest-band-input/v1"
    configuration: NativeMinuteForwardConfiguration
    backtest: NativeSealedPaperBacktestReturns

    @model_validator(mode="after")
    def exact_native_source_and_forward_start(self) -> Self:
        source, config = self.backtest, self.configuration
        if (source.definition_fingerprint, source.definition_record_hash,
            source.native_spec_fingerprint, source.profile_hash) != (
            config.target.head.registration_fingerprint, config.target.head.record_hash,
            config.target.head.spec_fingerprint, config.execution_profile.profile_hash
        ):
            raise ValueError("native band differs from the complete original native head/profile")
        from zoneinfo import ZoneInfo
        if self.comparison_dates[0] <= config.paper_approved_at.astimezone(ZoneInfo("Asia/Shanghai")).date():
            raise ValueError("native forward comparison cannot reuse a day before manual paper approval")
        return self

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


PaperResearchBandInput = PaperBacktestBandInput | NativePaperBacktestBandInput


class PaperBacktestBandResult(RuntimeContractModel):
    contract: Literal["paper-backtest-band-result/v1"] = "paper-backtest-band-result/v1"
    input_hash: Sha256
    configuration_fingerprint: Sha256
    backtest_source_hash: Sha256
    algorithm: Literal["paper-bootstrap-splitmix64-day-major-nearest-rank-v1"] = BOOTSTRAP_ALGORITHM
    seed: Literal[20261005] = BOOTSTRAP_SEED
    paths: Literal[2048] = BOOTSTRAP_PATHS
    dates: tuple[date, ...]
    points: tuple[BootstrapPoint, ...]

    @model_validator(mode="after")
    def complete(self) -> Self:
        if len(self.dates) != len(self.points) or tuple(point.day_index for point in self.points) != tuple(range(1, len(self.dates) + 1)):
            raise ValueError("paper band result has an incomplete path calendar")
        return self

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


def execute_paper_backtest_band(value: PaperResearchBandInput) -> PaperBacktestBandResult:
    value = TypeAdapter(PaperResearchBandInput).validate_python(value.model_dump(mode="python"))
    points = bootstrap_daily_band(tuple(item.daily_return for item in value.backtest.returns), days=len(value.comparison_dates))
    return PaperBacktestBandResult(input_hash=value.fingerprint, configuration_fingerprint=value.configuration.fingerprint,
                                  backtest_source_hash=value.backtest.fingerprint, dates=value.comparison_dates, points=points)
