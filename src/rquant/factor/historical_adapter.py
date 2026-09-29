"""Retrospective daily facts adapted from one admitted factor read lease."""

from __future__ import annotations

import ast
import hashlib
import json
import math
import re
from collections import Counter
from datetime import date, datetime, time, timedelta, timezone
from itertools import pairwise
from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

from rquant.factor.definition import FactorDefinition
from rquant.factor.result import (
    FactorForwardReturn,
    FactorResearchRequest,
    FactorResearchResult,
    HoldingSessions,
    ReturnMissingReason,
    assemble_factor_research_result,
    factor_research_request_sha256,
)
from rquant.factor.time_series import (
    MAX_OBSERVATIONS,
    MAX_RESULT_POINTS,
    MAX_TRADE_DAYS,
    DecisionTime,
    FactorTimeSeriesInput,
    FeatureObservation,
)
from rquant.factor_snapshot_admission import FactorSnapshotAdmissionDecision
from rquant.research_snapshot import (
    FactorAdjFactorBatch,
    FactorAdjFactorRow,
    FactorDailyBarBatch,
    FactorDailyBarRow,
    FactorReadLease,
    FactorReadQuery,
    FactorSourceBoundaryReceipt,
    FactorSSECalendarBatch,
    FactorSSECalendarRow,
)

_IMMUTABLE = ConfigDict(extra="forbid", frozen=True, strict=True, revalidate_instances="always")
_MARKET_TZ = timezone(timedelta(hours=8))
_STOCK_PATTERN = re.compile(r"[0-9]{6}\.(?:SZ|SH|BJ)\Z")
_DAILY_COLUMNS = frozenset({"open", "high", "low", "close", "vol", "amount"})
_SOURCE_COLUMNS = tuple(
    sorted(
        (
            *(f"daily_bar.{column}" for column in ("ts_code", "trade_date", *_DAILY_COLUMNS)),
            "adj_factor.ts_code",
            "adj_factor.trade_date",
            "adj_factor.adj_factor",
            "trade_calendar.cal_date",
            "trade_calendar.is_open",
            "trade_calendar.pretrade_date",
        )
    )
)
_CONTEXT_FUNCTIONS = frozenset({"industry_neutralize", "size_neutralize"})
_MAX_STOCKS = 500
_MAX_QUERY_DAYS = 366
_MAX_QUERY_ROWS = 100_000
_MAX_TOTAL_SOURCE_ROWS = 100_000
_MAX_RANGE_DAYS = 1_024
Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
MissingSourceReason = Literal[
    "panel_row_missing",
    "panel_value_missing",
    "return_window_unfinished",
    "return_visibility_pending",
    "return_price_missing",
    "return_suspended",
    "return_adjustment_missing",
    "return_adjustment_nonpositive",
    "return_adjustment_invalid",
]


class HistoricalFactorAdapterRequest(BaseModel):
    """A fixed pool and a bounded retrospective date window."""

    model_config = _IMMUTABLE

    definition: FactorDefinition
    stock_codes: tuple[str, ...] = Field(min_length=1, max_length=_MAX_STOCKS)
    pool_basis: Literal["explicit_fixed_list"]
    evaluation_days: tuple[date, ...] = Field(min_length=1, max_length=MAX_TRADE_DAYS)
    query_start_date: date
    query_end_date: date
    holding_sessions: HoldingSessions
    as_of: AwareDatetime

    @field_validator("stock_codes")
    @classmethod
    def _valid_pool(cls, codes: tuple[str, ...]) -> tuple[str, ...]:
        if len(set(codes)) != len(codes):
            raise ValueError("fixed stock pool contains duplicates")
        if any(_STOCK_PATTERN.fullmatch(code) is None for code in codes):
            raise ValueError("fixed stock pool contains an invalid code")
        return tuple(sorted(codes))

    @model_validator(mode="after")
    def _valid_dates(self) -> HistoricalFactorAdapterRequest:
        if (
            self.query_start_date > self.query_end_date
            or (self.query_end_date - self.query_start_date).days >= _MAX_RANGE_DAYS
        ):
            raise ValueError("historical source date range is invalid or too large")
        if any(left >= right for left, right in pairwise(self.evaluation_days)):
            raise ValueError("evaluation days must ascend without duplicates")
        if not all(
            self.query_start_date <= day <= self.query_end_date for day in self.evaluation_days
        ):
            raise ValueError("evaluation day is outside historical source range")
        return self


class HistoricalPanelDate(BaseModel):
    model_config = _IMMUTABLE

    decision_date: date
    panel_date: date
    first_visible_at: AwareDatetime


class HistoricalReturnWindow(BaseModel):
    model_config = _IMMUTABLE

    decision_date: date
    end_date: date
    return_end_at: AwareDatetime
    expected_available_at: AwareDatetime


class HistoricalMissingCount(BaseModel):
    model_config = _IMMUTABLE

    reason: MissingSourceReason
    count: int = Field(ge=1)


class HistoricalFactorSourceReceipt(BaseModel):
    """A source claim, explicitly limited to retrospective visibility assumptions."""

    model_config = _IMMUTABLE

    snapshot_id: str
    binding_hash: Sha256
    snapshot_as_of_time: AwareDatetime
    source_mode: Literal["historical_retrospective"]
    source_read_boundary: Literal["single_snapshot_transaction"]
    visibility_basis: Literal["retrospective_adapter_assumption"]
    pool_basis: Literal["explicit_fixed_list"]
    pool_sha256: Sha256
    stock_codes: tuple[str, ...]
    allowed_columns: tuple[str, ...]
    feature_columns: tuple[str, ...]
    query_start_date: date
    query_end_date: date
    calculation_days: tuple[date, ...]
    evaluation_days: tuple[date, ...]
    panel_dates: tuple[HistoricalPanelDate, ...]
    return_windows: tuple[HistoricalReturnWindow, ...]
    return_price_basis: Literal["forward_adjusted"]
    return_formula: Literal["close_end*adj_end/(open_d*adj_d)-1"]
    result_kind: Literal["research_diagnostic"]
    missing_counts: tuple[HistoricalMissingCount, ...]
    source_sha256: Sha256


class HistoricalFactorAdaptation(BaseModel):
    model_config = _IMMUTABLE

    receipt: HistoricalFactorSourceReceipt
    request: FactorResearchRequest

    @model_validator(mode="after")
    def _same_source(self) -> HistoricalFactorAdaptation:
        if self.receipt.source_sha256 != _digest(
            self.receipt.model_dump(mode="json", exclude={"source_sha256"})
        ):
            raise ValueError("historical source receipt digest differs")
        if (
            self.request.factor_source_id != self.receipt.source_sha256
            or self.request.return_source_id != self.receipt.source_sha256
        ):
            raise ValueError("factor and return facts must share the historical source")
        return self


class HistoricalFactorResearch(BaseModel):
    model_config = _IMMUTABLE

    receipt: HistoricalFactorSourceReceipt
    request: FactorResearchRequest
    result: FactorResearchResult

    @model_validator(mode="after")
    def _same_source(self) -> HistoricalFactorResearch:
        if self.result.input_sha256 != factor_research_request_sha256(self.request):
            raise ValueError("historical result input differs from its request")
        if self.result.sha256 != _digest(self.result.model_dump(mode="json", exclude={"sha256"})):
            raise ValueError("historical result content digest differs")
        if self.receipt.source_sha256 != _digest(
            self.receipt.model_dump(mode="json", exclude={"source_sha256"})
        ):
            raise ValueError("historical source receipt digest differs")
        digest = self.receipt.source_sha256
        if any(
            source != digest
            for source in (
                self.request.factor_source_id,
                self.request.return_source_id,
                self.result.factor_source_id,
                self.result.return_source_id,
            )
        ):
            raise ValueError("research result differs from its historical source")
        return self


def _digest(payload: object) -> str:
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _market_time(day: date, hour: int, minute: int = 0) -> datetime:
    return datetime.combine(day, time(hour, minute), _MARKET_TZ)


def _chunks(start: date, end: date, *, stock_count: int) -> tuple[tuple[date, date], ...]:
    days_per_chunk = min(_MAX_QUERY_DAYS, _MAX_QUERY_ROWS // stock_count)
    chunks: list[tuple[date, date]] = []
    current = start
    while current <= end:
        last = min(end, current + timedelta(days=days_per_chunk - 1))
        chunks.append((current, last))
        current = last + timedelta(days=1)
    return tuple(chunks)


def _check_receipt(
    receipt: FactorSourceBoundaryReceipt,
    *,
    decision: FactorSnapshotAdmissionDecision,
    query: FactorReadQuery,
    dataset_id: Literal["daily_bar", "adj_factor", "trade_calendar"],
) -> None:
    checked = FactorSourceBoundaryReceipt.model_validate(receipt)
    if checked != FactorSourceBoundaryReceipt(
        snapshot_id=decision.snapshot_id,
        binding_hash=decision.binding_hash,
        as_of_time=decision.as_of_time,
        source_mode="historical_retrospective",
        source_read_boundary="single_snapshot_transaction",
        dataset_id=dataset_id,
        stock_codes=query.stock_codes,
        start_date=query.start_date,
        end_date=query.end_date,
    ):
        raise ValueError("historical source batch differs from admitted lease")


def _read_calendar(
    lease: FactorReadLease,
    decision: FactorSnapshotAdmissionDecision,
    request: HistoricalFactorAdapterRequest,
) -> dict[date, FactorSSECalendarRow]:
    calendar: dict[date, FactorSSECalendarRow] = {}
    for start, end in _chunks(
        request.query_start_date, request.query_end_date, stock_count=len(request.stock_codes)
    ):
        query = FactorReadQuery(
            binding_hash=decision.binding_hash,
            stock_codes=request.stock_codes,
            start_date=start,
            end_date=end,
            row_limit=_MAX_QUERY_ROWS,
        )
        calendar_batch = FactorSSECalendarBatch.model_validate(lease.query_sse_calendar(query))
        _check_receipt(
            calendar_batch.receipt,
            decision=decision,
            query=query,
            dataset_id="trade_calendar",
        )
        for row in calendar_batch.rows:
            if not start <= row.cal_date <= end or row.cal_date in calendar:
                raise ValueError("SSE calendar has an out-of-range or duplicate date")
            calendar[row.cal_date] = row
    return calendar


def _read_stock_facts(
    lease: FactorReadLease,
    decision: FactorSnapshotAdmissionDecision,
    request: HistoricalFactorAdapterRequest,
) -> tuple[
    dict[tuple[date, str], FactorDailyBarRow],
    dict[tuple[date, str], FactorAdjFactorRow],
]:
    bars: dict[tuple[date, str], FactorDailyBarRow] = {}
    adjustments: dict[tuple[date, str], FactorAdjFactorRow] = {}
    for start, end in _chunks(
        request.query_start_date, request.query_end_date, stock_count=len(request.stock_codes)
    ):
        query = FactorReadQuery(
            binding_hash=decision.binding_hash,
            stock_codes=request.stock_codes,
            start_date=start,
            end_date=end,
            row_limit=_MAX_QUERY_ROWS,
        )
        bar_batch = FactorDailyBarBatch.model_validate(lease.query_daily_bars(query))
        adj_batch = FactorAdjFactorBatch.model_validate(lease.query_adj_factors(query))
        _check_receipt(bar_batch.receipt, decision=decision, query=query, dataset_id="daily_bar")
        _check_receipt(adj_batch.receipt, decision=decision, query=query, dataset_id="adj_factor")
        for row in bar_batch.rows:
            key = (row.trade_date, row.ts_code)
            if (
                not start <= row.trade_date <= end
                or row.ts_code not in request.stock_codes
                or key in bars
            ):
                raise ValueError("daily_bar has an out-of-range or duplicate business key")
            bars[key] = row
        for row in adj_batch.rows:
            key = (row.trade_date, row.ts_code)
            if (
                not start <= row.trade_date <= end
                or row.ts_code not in request.stock_codes
                or key in adjustments
            ):
                raise ValueError("adj_factor has an out-of-range or duplicate business key")
            adjustments[key] = row
    return bars, adjustments


def _open_days(
    request: HistoricalFactorAdapterRequest,
    calendar: dict[date, FactorSSECalendarRow],
) -> tuple[date, ...]:
    opened: list[date] = []
    day = request.query_start_date
    while day <= request.query_end_date:
        row = calendar.get(day)
        if row is None:
            raise ValueError(f"SSE calendar is incomplete at {day.isoformat()}")
        if row.is_open:
            if not opened and (
                row.pretrade_date is None or row.pretrade_date >= request.query_start_date
            ):
                raise ValueError(
                    "SSE calendar first pretrade_date must precede query start "
                    f"at {day.isoformat()}"
                )
            if opened and row.pretrade_date != opened[-1]:
                raise ValueError(f"SSE calendar pretrade_date differs at {day.isoformat()}")
            opened.append(day)
        day += timedelta(days=1)
    return tuple(opened)


def _validate_definition(definition: FactorDefinition) -> None:
    if (
        not set(definition.feature_catalog.columns) <= _DAILY_COLUMNS
        or not set(definition.dependency_columns) <= _DAILY_COLUMNS
    ):
        raise ValueError("factor definition requires a column without a historical daily contract")
    tree = ast.parse(definition.expression, mode="eval")
    if any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id in _CONTEXT_FUNCTIONS
        for node in ast.walk(tree)
    ):
        raise ValueError("factor definition requires unavailable industry or size context")


def _missing_return(
    *,
    code: str,
    day: date,
    decision_at: datetime,
    end_at: datetime,
    reason: ReturnMissingReason,
    expected_at: datetime | None = None,
) -> FactorForwardReturn:
    return FactorForwardReturn(
        stock_code=code,
        decision_date=day,
        decision_at=decision_at,
        return_end_at=end_at,
        value=None,
        missing_reason=reason,
        first_available_at=None,
        expected_available_at=expected_at,
    )


def _forward_return(
    *,
    code: str,
    window: HistoricalReturnWindow,
    as_of: datetime,
    bars: dict[tuple[date, str], FactorDailyBarRow],
    adjustments: dict[tuple[date, str], FactorAdjFactorRow],
) -> tuple[FactorForwardReturn, MissingSourceReason | None]:
    day = window.decision_date
    decision_at = _market_time(day, 9, 25)
    end_at = window.return_end_at
    available_at = window.expected_available_at
    if as_of < end_at:
        return (
            _missing_return(
                code=code,
                day=day,
                decision_at=decision_at,
                end_at=end_at,
                reason="window_unfinished",
            ),
            "return_window_unfinished",
        )
    if as_of < available_at:
        return (
            _missing_return(
                code=code,
                day=day,
                decision_at=decision_at,
                end_at=end_at,
                reason="visibility_pending",
                expected_at=available_at,
            ),
            "return_visibility_pending",
        )
    start_bar = bars.get((day, code))
    end_bar = bars.get((window.end_date, code))
    if start_bar is None or end_bar is None:
        reason: MissingSourceReason = "return_price_missing"
        missing = "missing_price"
    elif start_bar.vol == 0 or end_bar.vol == 0:
        reason = "return_suspended"
        missing = "suspended"
    elif (
        start_bar.open is None or end_bar.close is None or start_bar.open <= 0 or end_bar.close <= 0
    ):
        reason = "return_price_missing"
        missing = "missing_price"
    else:
        start_adj = adjustments.get((day, code))
        end_adj = adjustments.get((window.end_date, code))
        if start_adj is None or end_adj is None:
            reason = "return_adjustment_missing"
            missing = "source_unavailable"
        elif start_adj.adj_factor <= 0 or end_adj.adj_factor <= 0:
            reason = "return_adjustment_nonpositive"
            missing = "source_unavailable"
        else:
            numerator = end_bar.close * end_adj.adj_factor
            denominator = start_bar.open * start_adj.adj_factor
            value = numerator / denominator - 1 if denominator else math.nan
            if math.isfinite(value) and value >= -1:
                return (
                    FactorForwardReturn(
                        stock_code=code,
                        decision_date=day,
                        decision_at=decision_at,
                        return_end_at=end_at,
                        value=float(value),
                        missing_reason=None,
                        first_available_at=available_at,
                    ),
                    None,
                )
            reason = "return_adjustment_invalid"
            missing = "source_unavailable"
    return (
        _missing_return(code=code, day=day, decision_at=decision_at, end_at=end_at, reason=missing),
        reason,
    )


def adapt_historical_factor_source(
    lease: FactorReadLease,
    decision: FactorSnapshotAdmissionDecision,
    request: HistoricalFactorAdapterRequest,
) -> HistoricalFactorAdaptation:
    """Build one factor/return grid without querying outside the admitted lease."""
    checked = HistoricalFactorAdapterRequest.model_validate(request)
    admitted = FactorSnapshotAdmissionDecision.model_validate(decision)
    if (
        not isinstance(lease, FactorReadLease)
        or not admitted.allowed
        or admitted.research_status != "exploratory"
        or admitted.as_of_time is None
        or admitted.source_mode != "historical_retrospective"
        or admitted.source_read_boundary != "single_snapshot_transaction"
    ):
        raise ValueError("factor historical source requires one admitted read lease")
    _validate_definition(checked.definition)
    calendar = _read_calendar(lease, admitted, checked)
    open_days = _open_days(checked, calendar)
    open_index = {day: index for index, day in enumerate(open_days)}
    if any(day not in open_index for day in checked.evaluation_days):
        raise ValueError("factor evaluation day is not open on the SSE calendar")
    history = checked.definition.max_history_window
    first_index = open_index[checked.evaluation_days[0]]
    if first_index < history:
        raise ValueError("factor source lacks a preceding panel or declared warmup")
    calculation_days = open_days[
        first_index - history + 1 : open_index[checked.evaluation_days[-1]] + 1
    ]
    if (
        len(calculation_days) > MAX_TRADE_DAYS
        or len(calculation_days) * len(checked.stock_codes) > MAX_RESULT_POINTS
        or len(calculation_days)
        * len(checked.stock_codes)
        * len(checked.definition.dependency_columns)
        > MAX_OBSERVATIONS
        or len(open_days) * len(checked.stock_codes) > _MAX_TOTAL_SOURCE_ROWS
        or len(checked.evaluation_days) * len(checked.stock_codes) > MAX_RESULT_POINTS
    ):
        raise ValueError("historical factor input exceeds its bounded pure model")
    windows: list[HistoricalReturnWindow] = []
    for day in checked.evaluation_days:
        end_index = open_index[day] + checked.holding_sessions - 1
        if end_index + 1 >= len(open_days):
            raise ValueError("factor return endpoint or next SSE open day is missing")
        end_day = open_days[end_index]
        windows.append(
            HistoricalReturnWindow(
                decision_date=day,
                end_date=end_day,
                return_end_at=_market_time(end_day, 15),
                expected_available_at=_market_time(open_days[end_index + 1], 9, 25),
            )
        )
    if any(
        previous.return_end_at > _market_time(current.decision_date, 9, 25)
        for previous, current in pairwise(windows)
    ):
        raise ValueError("factor return windows overlap")
    bars, adjustments = _read_stock_facts(lease, admitted, checked)
    panel_dates = tuple(
        HistoricalPanelDate(
            decision_date=day,
            panel_date=open_days[open_index[day] - 1],
            first_visible_at=_market_time(day, 9, 25),
        )
        for day in calculation_days
    )
    missing: Counter[MissingSourceReason] = Counter()
    observations: list[FeatureObservation] = []
    for panel in panel_dates:
        for code in checked.stock_codes:
            bar = bars.get((panel.panel_date, code))
            if bar is None:
                missing["panel_row_missing"] += len(checked.definition.dependency_columns)
                continue
            for column in checked.definition.dependency_columns:
                value = getattr(bar, column)
                if value is None:
                    missing["panel_value_missing"] += 1
                observations.append(
                    FeatureObservation(
                        stock_code=code,
                        trade_date=panel.decision_date,
                        column=column,
                        value=value,
                        first_visible_at=panel.first_visible_at,
                    )
                )
    forward_returns: list[FactorForwardReturn] = []
    for window in windows:
        for code in checked.stock_codes:
            row, reason = _forward_return(
                code=code,
                window=window,
                as_of=checked.as_of,
                bars=bars,
                adjustments=adjustments,
            )
            forward_returns.append(row)
            if reason is not None:
                missing[reason] += 1
    factor_input = FactorTimeSeriesInput(
        definition=checked.definition,
        universe=checked.stock_codes,
        trading_days=calculation_days,
        decision_times=tuple(
            DecisionTime(trade_date=day, decision_at=_market_time(day, 9, 25))
            for day in calculation_days
        ),
        observations=tuple(observations),
    )
    receipt_fields: dict[str, object] = {
        "snapshot_id": admitted.snapshot_id,
        "binding_hash": admitted.binding_hash,
        "snapshot_as_of_time": admitted.as_of_time,
        "source_mode": "historical_retrospective",
        "source_read_boundary": "single_snapshot_transaction",
        "visibility_basis": "retrospective_adapter_assumption",
        "pool_basis": "explicit_fixed_list",
        "pool_sha256": _digest(list(checked.stock_codes)),
        "stock_codes": checked.stock_codes,
        "allowed_columns": _SOURCE_COLUMNS,
        "feature_columns": checked.definition.dependency_columns,
        "query_start_date": checked.query_start_date,
        "query_end_date": checked.query_end_date,
        "calculation_days": calculation_days,
        "evaluation_days": checked.evaluation_days,
        "panel_dates": panel_dates,
        "return_windows": tuple(windows),
        "return_price_basis": "forward_adjusted",
        "return_formula": "close_end*adj_end/(open_d*adj_d)-1",
        "result_kind": "research_diagnostic",
        "missing_counts": tuple(
            HistoricalMissingCount(reason=reason, count=count)
            for reason, count in sorted(missing.items())
        ),
    }
    provisional = HistoricalFactorSourceReceipt(**receipt_fields, source_sha256="0" * 64)
    source_sha256 = _digest(provisional.model_dump(mode="json", exclude={"source_sha256"}))
    receipt = HistoricalFactorSourceReceipt(**receipt_fields, source_sha256=source_sha256)
    pure_request = FactorResearchRequest(
        factor_input=factor_input,
        evaluation_days=checked.evaluation_days,
        forward_returns=tuple(forward_returns),
        as_of=checked.as_of,
        factor_source_id=source_sha256,
        return_source_id=source_sha256,
        return_price_basis="forward_adjusted",
        holding_sessions=checked.holding_sessions,
    )
    return HistoricalFactorAdaptation(receipt=receipt, request=pure_request)


def assemble_historical_factor_research(
    lease: FactorReadLease,
    decision: FactorSnapshotAdmissionDecision,
    request: HistoricalFactorAdapterRequest,
) -> HistoricalFactorResearch:
    adapted = adapt_historical_factor_source(lease, decision, request)
    result = assemble_factor_research_result(adapted.request)
    return HistoricalFactorResearch(receipt=adapted.receipt, request=adapted.request, result=result)
