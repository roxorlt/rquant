"""Bounded data-only contracts for the portfolio backtest product."""

from __future__ import annotations

import hashlib
import json
from datetime import date
from decimal import Decimal
from typing import Annotated, Literal, Self

from pydantic import Field, StringConstraints, field_validator, model_validator

from rquant.backtest.benchmark import SUPPORTED_BENCHMARK_CODES, BenchmarkSeries
from rquant.backtest.contracts import BacktestRequest, BacktestResult, RebalanceRule, Sha256
from rquant.backtest.report import MAX_BACKTEST_HTML_BYTES
from rquant.perf import PerformanceSummary, RelativeMetrics, ReturnDistribution, StreakSummary
from rquant.perf.trades import RoundTrip, RoundTripAnalysis
from rquant.portfolio.drawdown import DrawdownRule
from rquant.portfolio.weights import PortfolioWeightRule
from rquant.research_run_spec import ExecutionCostSpec
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256

MAX_CONFIG_BYTES = 32 * 1024
MAX_BUNDLE_BYTES = 16 * 1024 * 1024
MAX_SOURCE_CODES = 500
MAX_SOURCE_PAIRS = 20_000
MAX_DATE_SPAN = 5 * 366
MAX_ZIP_BYTES = 32 * 1024 * 1024
PORTFOLIO_TABLE_NAMES = (
    "portfolio_bundle",
    "portfolio_nav",
    "portfolio_trades",
    "portfolio_holdings",
    "portfolio_daily",
    "portfolio_monthly",
    "portfolio_log",
)
SOURCE_ASSUMPTION = "历史日线按前一日收盘价在次日决策前可用重放，不证明实际历史观测或开盘成交。"


class PortfolioBacktestConfig(RuntimeContractModel):
    source_key: str = Field(default="verified-screen", pattern=r"^[a-zA-Z0-9_.:-]{1,128}$")
    source_version: int = Field(default=1, strict=True, ge=1)
    start_date: date
    end_date: date
    initial_cash: Decimal = Field(gt=0, le=Decimal("1000000000000"), allow_inf_nan=False)
    benchmark_code: str = "000300.SH"
    weight_rule: PortfolioWeightRule
    rebalance_rule: RebalanceRule
    execution_cost_spec: ExecutionCostSpec
    drawdown_rule: DrawdownRule | None = None

    @model_validator(mode="after")
    def validate_bounds(self) -> Self:
        if not 1 <= (self.end_date - self.start_date).days + 1 <= MAX_DATE_SPAN:
            raise ValueError("date range exceeds portfolio budget")
        if self.initial_cash * 100 != (self.initial_cash * 100).to_integral_value():
            raise ValueError("initial cash must be exact to a cent")
        if self.benchmark_code not in SUPPORTED_BENCHMARK_CODES:
            raise ValueError("unsupported benchmark")
        if self.weight_rule.max_positions > MAX_SOURCE_CODES:
            raise ValueError("position count exceeds portfolio budget")
        if not self.execution_cost_spec.is_alignment_eligible:
            raise ValueError("portfolio requires the shared v3 cost spec")
        if len(self.model_dump_json().encode()) > MAX_CONFIG_BYTES:
            raise ValueError("portfolio config exceeds byte budget")
        return self

    @classmethod
    def from_request(cls, request: BacktestRequest) -> PortfolioBacktestConfig:
        return cls(
            start_date=request.days[0].trade_date,
            end_date=request.days[-1].trade_date,
            initial_cash=request.initial_cash,
            weight_rule=request.weight_rule,
            rebalance_rule=request.rebalance_rule,
            execution_cost_spec=request.execution_cost_spec,
            drawdown_rule=getattr(request, "drawdown_rule", None),
        )

    @property
    def config_hash(self) -> str:
        return canonical_sha256(self)


class PortfolioSourceManifest(RuntimeContractModel):
    contract: Literal["portfolio-source/v1"] = "portfolio-source/v1"
    source_mode: Literal["captured_with_retrospective_prices"]
    assumption: Literal[SOURCE_ASSUMPTION] = SOURCE_ASSUMPTION
    market_hash: Sha256
    reference_hash: Sha256
    opening_hash: Sha256 | None
    ranking_hash: Sha256 | None = None
    industry_hash: Sha256 | None = None


class FrozenPortfolioInput(RuntimeContractModel):
    contract: Literal["portfolio-input/v1"] = "portfolio-input/v1"
    config: PortfolioBacktestConfig
    request: BacktestRequest
    sources: PortfolioSourceManifest
    benchmark_closes: tuple[tuple[date, float], ...] | None
    benchmark_unavailable: Literal["missing_source", "missing_dates"] | None = None
    input_hash: Sha256 | None = None

    @field_validator("benchmark_closes", mode="before")
    @classmethod
    def reject_boolean_prices(cls, value: object) -> object:
        if isinstance(value, (tuple, list)) and any(
            isinstance(row, (tuple, list)) and len(row) == 2 and isinstance(row[1], bool)
            for row in value
        ):
            raise ValueError("benchmark prices cannot be boolean")
        return value

    @model_validator(mode="after")
    def validate_input(self) -> Self:
        expected_config = PortfolioBacktestConfig.from_request(self.request)
        actual = self.config.model_dump(
            mode="python", exclude={"source_key", "source_version", "benchmark_code"}
        )
        expected = expected_config.model_dump(
            mode="python", exclude={"source_key", "source_version", "benchmark_code"}
        )
        if actual != expected:
            raise ValueError("frozen request does not match config")
        instruments = [item for day in self.request.days for item in day.instruments]
        if (
            len(instruments) > MAX_SOURCE_PAIRS
            or len({item.ts_code for item in instruments}) > MAX_SOURCE_CODES
        ):
            raise ValueError("frozen input exceeds source pair/code budget")
        if self.sources.opening_hash is None and any(
            item.conditions is not None for item in instruments
        ):
            raise ValueError("opening conditions need source evidence")
        if self.config.weight_rule.method == "rank_score" and self.sources.ranking_hash is None:
            raise ValueError("ranking source is unavailable")
        if self.sources.ranking_hash is None and any(
            len(day.ranking.candidates) > self.config.weight_rule.max_positions
            for day in self.request.days
        ):
            raise ValueError("top-N requires ranking source evidence")
        if (
            self.config.weight_rule.max_industry_weight is not None
            and self.sources.industry_hash is None
        ):
            raise ValueError("industry source is unavailable")
        if (self.benchmark_closes is None) != (self.benchmark_unavailable is not None):
            raise ValueError("missing benchmark must have an explicit reason")
        if self.benchmark_closes is not None:
            import math

            calendar = self.request.calendar.dates
            first = calendar.index(self.request.days[0].trade_date)
            expected_dates = (calendar[first - 1], *(day.trade_date for day in self.request.days))
            if tuple(row[0] for row in self.benchmark_closes) != expected_dates:
                raise ValueError("benchmark must cover exact baseline and trading dates")
            if any(
                isinstance(price, bool) or not math.isfinite(price) or price <= 0
                for _, price in self.benchmark_closes
            ):
                raise ValueError("benchmark close must be finite and positive")
        expected_hash = canonical_sha256(self.model_dump(mode="python", exclude={"input_hash"}))
        if self.input_hash is None:
            object.__setattr__(self, "input_hash", expected_hash)
        elif self.input_hash != expected_hash:
            raise ValueError("input hash differs from frozen content")
        if len(self.model_dump_json().encode()) > MAX_BUNDLE_BYTES:
            raise ValueError("frozen input exceeds byte budget")
        return self


class PortfolioRollingMetric(RuntimeContractModel):
    trade_date: date
    volatility: float | None = Field(allow_inf_nan=False)
    sharpe: float | None = Field(allow_inf_nan=False)


class PortfolioPerformance(RuntimeContractModel):
    summary: PerformanceSummary
    benchmark_summary: PerformanceSummary | None
    relative: RelativeMetrics | None
    annualized_turnover: float | None = Field(allow_inf_nan=False)
    rolling: tuple[PortfolioRollingMetric, ...]
    round_trips: tuple[RoundTrip, ...]
    round_trip_analysis: RoundTripAnalysis
    distribution: ReturnDistribution
    streaks: StreakSummary
    overfit_state: Literal["not_evaluated"] = "not_evaluated"


class PortfolioBundle(RuntimeContractModel):
    contract: Literal["portfolio-bundle/v1"] = "portfolio-bundle/v1"
    frozen: FrozenPortfolioInput
    result: BacktestResult
    benchmark: BenchmarkSeries | None
    performance: PortfolioPerformance | None
    html: Annotated[str, StringConstraints(strip_whitespace=False)] | None
    html_sha256: Sha256 | None
    bundle_hash: Sha256 | None = None

    @model_validator(mode="after")
    def validate_integrity(self) -> Self:
        request = self.frozen.request
        if (
            self.result.request_id,
            self.result.producer_commit,
            self.result.input_generation_id,
            self.result.calendar_source_identity,
            self.result.cost_spec_id,
        ) != (
            request.request_id,
            request.producer_commit,
            request.input_generation_id,
            request.calendar.source_identity,
            request.execution_cost_spec.cost_spec_id,
        ):
            raise ValueError("bundle result differs from frozen request")
        if tuple(day.trade_date for day in self.result.days) != tuple(
            day.trade_date for day in request.days[: len(self.result.days)]
        ):
            raise ValueError("bundle result dates differ from request")
        if self.result.status == "complete" and len(self.result.days) != len(request.days):
            raise ValueError("complete bundle must cover all requested days")
        for index, day in enumerate(self.result.days):
            if (day.risk is None) != (request.drawdown_rule is None):
                raise ValueError("risk result differs from frozen config")
            if day.risk is not None:
                prior_nav = (
                    request.initial_cash if index == 0 else self.result.days[index - 1].account.nav
                )
                active = day.risk.state.active
                rule = request.drawdown_rule
                if (
                    day.risk.state.rule != rule
                    or day.risk.state.last_nav != prior_nav
                    or day.risk.allow_new_positions
                    != (not (active and rule.action == "block_new_positions"))
                    or day.risk.max_total_risk_weight
                    != (
                        rule.total_risk_weight_cap
                        if active and rule.action == "cap_total_risk_weight"
                        else None
                    )
                ):
                    raise ValueError("risk must bind the prior NAV and configured action")
        if self.benchmark is not None:
            if (
                self.benchmark.backtest_content_hash != self.result.content_hash
                or self.benchmark.calendar_source_identity != request.calendar.source_identity
            ):
                raise ValueError("benchmark differs from bundle result")
            if tuple(day.trade_date for day in self.benchmark.days) != tuple(
                day.trade_date for day in self.result.days
            ):
                raise ValueError("benchmark dates differ from result")
            expected_closes = self.frozen.benchmark_closes
            actual_closes = (
                (self.benchmark.baseline_trade_date, self.benchmark.baseline_close),
                *((day.trade_date, day.close) for day in self.benchmark.days),
            )
            if (
                self.benchmark.ts_code != self.frozen.config.benchmark_code
                or actual_closes != expected_closes
            ):
                raise ValueError("benchmark differs from the frozen code or closes")
        if self.result.status == "complete" and (self.benchmark is None) != (
            self.frozen.benchmark_closes is None
        ):
            raise ValueError("benchmark availability differs from frozen source")
        if self.result.status == "incomplete":
            if any(
                value is not None
                for value in (self.performance, self.html, self.html_sha256, self.benchmark)
            ):
                raise ValueError("incomplete result cannot claim complete performance or report")
        elif self.performance is None or self.html is None or self.html_sha256 is None:
            raise ValueError("complete result requires performance and sealed HTML")
        if (self.html is None) != (self.html_sha256 is None):
            raise ValueError("HTML and digest must appear together")
        if self.html is not None:
            encoded = self.html.encode()
            if (
                len(encoded) > MAX_BACKTEST_HTML_BYTES
                or hashlib.sha256(encoded).hexdigest() != self.html_sha256
            ):
                raise ValueError("HTML exceeds budget or digest differs")
        expected = canonical_sha256(self.model_dump(mode="json", exclude={"bundle_hash"}))
        if self.bundle_hash is None:
            object.__setattr__(self, "bundle_hash", expected)
        elif self.bundle_hash != expected:
            raise ValueError("bundle hash differs from content")
        if len(self.json_bytes()) > MAX_BUNDLE_BYTES:
            raise ValueError("bundle exceeds byte budget")
        return self

    def json_bytes(self) -> bytes:
        return json.dumps(
            self.model_dump(mode="json"),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode()
