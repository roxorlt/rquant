"""Bounded v2 raw facts mapped to retrospective daily formula and return facts.

Archive digests bind caller-supplied facts; they do not prove provider coverage.
09:25 visibility is the existing retrospective assumption, not observed PIT.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Iterator
from datetime import date, timedelta
from itertools import pairwise
from typing import Literal, cast

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from rquant.factor.daily_feature_source import (
    MARKET_TEMPERATURE_COLUMNS,
    FactorDailyFeatureCounts,
    FactorDailyFeatureInput,
    FactorDailyFeatureReadLease,
    FactorDailyFeatureSource,
    FactorDailyFeatureSources,
    FactorMarketTemperatureDayValue,
    read_factor_daily_feature_input,
)
from rquant.factor.daily_stream import (
    FactorDailyStreamBatch,
    FactorDailyStreamRequest,
    FactorDailyStreamSources,
    factor_daily_stream_request_sha256,
)
from rquant.factor.extended_statistics import FactorExtendedStatisticsRequest
from rquant.factor.formula_stream import (
    FactorFormulaFeaturePoint,
    FactorFormulaStreamBatch,
    FactorFormulaStreamDay,
    FactorFormulaStreamRequest,
    factor_formula_stream_batch_sha256,
    factor_formula_stream_request_sha256,
)
from rquant.factor.historical_adapter import (
    HistoricalMissingCount,
    HistoricalReturnWindow,
    _forward_return,
    _market_time,
)
from rquant.factor.neutralization_context import (
    FactorNeutralizationContext,
    FactorNeutralizationDayBatch,
    FactorNeutralizationReadLease,
    FactorNeutralizationSources,
)
from rquant.factor.result import HoldingSessions
from rquant.factor.stream_snapshot import (
    FactorStreamSnapshotAdmissionDecision,
    FactorStreamSnapshotAdmissionRequest,
)
from rquant.factor.time_series import MAX_TRADE_DAYS
from rquant.factor.universe import FactorUniverseRequest, Sha256
from rquant.research_snapshot import (
    FactorAdjFactorBatch,
    FactorAdjFactorRow,
    FactorDailyBarBatch,
    FactorDailyBarRow,
    FactorReadQuery,
    FactorSourceBoundaryReceipt,
    FactorSSECalendarBatch,
    FactorStreamReadLease,
)
from rquant.runtime_contracts import canonical_sha256

_IMMUTABLE = ConfigDict(extra="forbid", frozen=True, strict=True, revalidate_instances="always")


class FactorStreamAdapterError(ValueError):
    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


class FactorStreamAdapterRequest(BaseModel):
    model_config = _IMMUTABLE

    adapter_version: Literal["factor-raw-daily-adapter-v1"] = "factor-raw-daily-adapter-v1"
    source: FactorStreamSnapshotAdmissionRequest
    scope_content_hash: Sha256
    formula: FactorFormulaStreamRequest
    evaluation_days: tuple[date, ...] = Field(min_length=1, max_length=MAX_TRADE_DAYS)
    holding_sessions: HoldingSessions
    context: FactorNeutralizationContext | None = Field(
        default=None, exclude_if=lambda v: v is None
    )
    daily_feature_source: FactorDailyFeatureSource | None = Field(
        default=None, exclude_if=lambda v: v is None
    )
    extended_statistics: bool = Field(default=False, exclude_if=lambda v: v is False)
    ic_method: Literal["rank", "normal"] | None = Field(
        default=None, exclude_if=lambda v: v is None
    )

    @field_validator("evaluation_days")
    @classmethod
    def _ascending(cls, days: tuple[date, ...]) -> tuple[date, ...]:
        if any(left >= right for left, right in pairwise(days)):
            raise ValueError("evaluation schedule must ascend uniquely")
        return days

    @model_validator(mode="after")
    def _bound_request(self) -> FactorStreamAdapterRequest:
        stored, selected = self.daily_feature_source, self.formula.sources.daily_features
        if (
            (stored is None) != (selected is None)
            or stored is not None
            and (
                stored.select(tuple(f.column for f in selected.fields)) != selected
                or stored.prepared_snapshot_id != self.source.snapshot_id
                or stored.prepared_binding_hash != self.source.binding_hash
                or stored.scope_content_hash != self.scope_content_hash
                or stored.scope != self.source.scope
            )
        ):
            raise ValueError(
                "stored daily facts differ from admitted raw source or dependency subset"
            )
        if self.extended_statistics != (self.ic_method is not None):
            raise ValueError("extended statistics require their frozen IC method")
        if (self.context is None) != (self.formula.sources.context is None):
            raise ValueError("formula context lacks its sealed source package")
        if self.context is not None and (
            self.context.sources != self.formula.sources.context
            or self.context.snapshot_id != self.source.snapshot_id
            or self.context.binding_hash != self.source.binding_hash
            or self.context.scope_content_hash != self.scope_content_hash
            or self.context.scope != self.source.scope
        ):
            raise ValueError("context differs from admitted raw source")
        if self.formula.computation_stock_codes != self.source.scope.stock_codes:
            raise ValueError("formula calculation codes differ from complete bound scope")
        if (
            self.formula.sources.feature_source_id != self.source.snapshot_id
            or self.formula.sources.feature_source_sha256 != self.source.binding_hash
        ):
            raise ValueError("formula features differ from bound raw source")
        if self.formula.as_of > self.source.scope.as_of_time:
            raise ValueError("analysis cutoff follows frozen source cutoff")
        if any(
            not self.source.scope.start_date <= day <= self.source.scope.end_date
            for day in self.formula.trading_days
        ):
            raise ValueError("calculation dates exceed raw scope")
        if not set(self.evaluation_days) <= set(self.formula.trading_days):
            raise ValueError("evaluation dates are outside calculation schedule")
        if any(
            item.decision_at != _market_time(item.trade_date, 9, 25)
            for item in self.formula.decision_times
        ):
            raise ValueError("daily adapter decision must be 09:25 Shanghai")
        return self


class FactorStreamFeatureDayReceipt(BaseModel):
    model_config = _IMMUTABLE

    trade_date: date
    panel_date: date
    input_sha256: Sha256
    raw_sha256: Sha256
    missing_observation_count: int = Field(ge=0, le=7000 * 58)
    known_null_count: int = Field(ge=0, le=7000 * 58)
    context_input_sha256: Sha256 | None = Field(default=None, exclude_if=lambda v: v is None)
    daily_feature_input_sha256: Sha256 | None = Field(default=None, exclude_if=lambda v: v is None)
    daily_feature_counts: tuple[FactorDailyFeatureCounts, ...] | None = Field(
        default=None, max_length=52, exclude_if=lambda v: v is None
    )
    market_temperature_values: tuple[FactorMarketTemperatureDayValue, ...] | None = Field(
        default=None, min_length=1, max_length=2, exclude_if=lambda v: v is None
    )

    @model_validator(mode="after")
    def _market_columns(self) -> FactorStreamFeatureDayReceipt:
        market = tuple(
            c.column
            for c in self.daily_feature_counts or ()
            if c.column in MARKET_TEMPERATURE_COLUMNS
        )
        if tuple(v.column for v in self.market_temperature_values or ()) != market:
            raise ValueError("market day values differ from selected daily coverage fields")
        return self


class FactorStreamReturnDayReceipt(BaseModel):
    model_config = _IMMUTABLE

    window: HistoricalReturnWindow
    selected_count: int = Field(ge=0, le=7_000)
    raw_sha256: Sha256
    statistics_batch_sha256: Sha256
    missing_counts: tuple[HistoricalMissingCount, ...]


class FactorStreamAdapterCompletion(BaseModel):
    model_config = _IMMUTABLE

    admission: FactorStreamSnapshotAdmissionDecision
    request_sha256: Sha256
    formula_request_sha256: Sha256
    statistics_request_sha256: Sha256
    processed_days: int = Field(ge=1, le=MAX_TRADE_DAYS)
    calendar_sha256: Sha256
    feature_days: tuple[FactorStreamFeatureDayReceipt, ...] = Field(
        min_length=1, max_length=MAX_TRADE_DAYS
    )
    return_days: tuple[FactorStreamReturnDayReceipt, ...] = Field(
        min_length=1, max_length=MAX_TRADE_DAYS
    )
    read_query_count: int = Field(ge=1)
    daily_bar_row_count: int = Field(ge=0)
    adj_factor_row_count: int = Field(ge=0)
    context: FactorNeutralizationSources | None = Field(
        default=None, exclude_if=lambda v: v is None
    )
    context_read_query_count: int | None = Field(default=None, ge=1, exclude_if=lambda v: v is None)
    daily_features: FactorDailyFeatureSources | None = Field(
        default=None, exclude_if=lambda v: v is None
    )
    daily_feature_read_query_count: int | None = Field(
        default=None, ge=1, exclude_if=lambda v: v is None
    )
    input_sha256: Sha256
    sha256: Sha256

    @model_validator(mode="after")
    def _receipt_digest(self) -> FactorStreamAdapterCompletion:
        if self.processed_days != len(self.feature_days):
            raise ValueError("adapter completion count differs from feature schedule")
        market_queries = int(
            self.daily_features is not None and self.daily_features.market_temperature is not None
        )
        stock_queries = (
            (len(self.admission.scope.stock_codes) + 499) // 500
            if self.daily_features is not None
            and any(f.column not in MARKET_TEMPERATURE_COLUMNS for f in self.daily_features.fields)
            else 0
        )
        if (
            (self.daily_features is None) != (self.daily_feature_read_query_count is None)
            or any(
                (day.daily_feature_input_sha256 is None) != (self.daily_features is None)
                or (day.daily_feature_counts is None) != (self.daily_features is None)
                for day in self.feature_days
            )
            or self.daily_features is not None
            and (
                self.daily_features.prepared_snapshot_id != self.admission.snapshot_id
                or self.daily_features.prepared_binding_hash != self.admission.binding_hash
                or self.daily_features.scope_content_hash != self.admission.scope_content_hash
                or self.daily_feature_read_query_count
                != self.processed_days * (market_queries + stock_queries)
            )
        ):
            raise ValueError("stored daily completion differs from complete daily grid")
        if self.daily_features is not None and any(
            tuple(c.column for c in day.daily_feature_counts)
            != tuple(f.column for f in self.daily_features.fields)
            or any(
                c.valid + c.missing + c.null + c.non_finite != len(self.admission.scope.stock_codes)
                for c in day.daily_feature_counts
            )
            for day in self.feature_days
        ):
            raise ValueError("stored daily coverage differs from bound field and code grid")
        if (self.context is None) != (self.context_read_query_count is None) or any(
            (day.context_input_sha256 is None) != (self.context is None)
            for day in self.feature_days
        ):
            raise ValueError("adapter context completion lacks a daily input")
        if self.context is not None:
            self.context.require_binding(
                mode="none",
                snapshot_id=self.admission.snapshot_id,
                binding_hash=self.admission.binding_hash,
                as_of=self.admission.scope.as_of_time,
            )
            chunks = (len(self.admission.scope.stock_codes) + 499) // 500
            if self.context_read_query_count != self.processed_days * chunks * (
                int(self.context.industry is not None) + int(self.context.market_cap is not None)
            ):
                raise ValueError("adapter context query count differs from complete schedule")
        if self.input_sha256 != canonical_sha256(
            self.model_dump(exclude={"input_sha256", "sha256"})
        ):
            raise ValueError("adapter input digest differs")
        if self.sha256 != canonical_sha256(self.model_dump(exclude={"sha256"})):
            raise ValueError("adapter completion digest differs")
        return self


def factor_stream_adapter_request_sha256(request: FactorStreamAdapterRequest) -> str:
    return canonical_sha256(FactorStreamAdapterRequest.model_validate(request))


def factor_stream_statistics_request(
    request: FactorStreamAdapterRequest,
) -> FactorDailyStreamRequest:
    request = FactorStreamAdapterRequest.model_validate(request)
    formula_sha = factor_formula_stream_request_sha256(request.formula)
    request_sha = factor_stream_adapter_request_sha256(request)
    archive_sha = canonical_sha256(
        (
            request.formula.selection,
            request.formula.sources.security_source_id,
            request.formula.sources.security_source_sha256,
            request.formula.sources.index_source_id,
            request.formula.sources.index_source_sha256,
        )
    )
    return FactorDailyStreamRequest(
        definition=request.formula.definition,
        selection=request.formula.selection,
        evaluation_days=request.evaluation_days,
        as_of=request.formula.as_of,
        sources=FactorDailyStreamSources(
            source_mode="historical_retrospective",
            universe_source_id=f"archive:{archive_sha}",
            universe_source_sha256=archive_sha,
            factor_source_id=f"formula:{formula_sha}",
            factor_source_sha256=formula_sha,
            return_source_id=f"returns:{request_sha}",
            return_source_sha256=request_sha,
        ),
        return_price_basis="forward_adjusted",
        holding_sessions=request.holding_sessions,
        extended_statistics=None
        if not request.extended_statistics
        else FactorExtendedStatisticsRequest(
            ic_method=request.ic_method,
            sources=request.formula.sources.context
            if request.context is not None and request.context.industry is not None
            else None,
        ),
    )


class FactorStreamAdapter(Iterator[FactorFormulaStreamBatch]):
    """One owned pool iterator and bounded raw reads from an admitted v2 lease."""

    def __init__(
        self,
        request: FactorStreamAdapterRequest,
        *,
        lease: FactorStreamReadLease,
        decision: FactorStreamSnapshotAdmissionDecision,
        universe_requests: Iterable[FactorUniverseRequest],
        context_lease: FactorNeutralizationReadLease | None = None,
        daily_feature_lease: FactorDailyFeatureReadLease | None = None,
    ) -> None:
        self.request = FactorStreamAdapterRequest.model_validate(request)
        self.admission = FactorStreamSnapshotAdmissionDecision.model_validate(decision)
        if (
            not isinstance(lease, FactorStreamReadLease)
            or lease.scope != self.request.source.scope
            or self.admission.snapshot_id != self.request.source.snapshot_id
            or self.admission.binding_hash != self.request.source.binding_hash
            or self.admission.scope != self.request.source.scope
            or self.admission.scope_content_hash != self.request.scope_content_hash
        ):
            raise FactorStreamAdapterError("source_binding_mismatch")
        self._lease = lease
        if (context_lease is None) != (self.request.context is None) or (
            context_lease is not None
            and (context_lease.closed or context_lease.context != self.request.context)
        ):
            raise FactorStreamAdapterError("context_binding_mismatch")
        self._context_lease = context_lease
        self._current_context: FactorNeutralizationDayBatch | None = None
        if (daily_feature_lease is None) != (self.request.daily_feature_source is None) or (
            daily_feature_lease is not None
            and (
                daily_feature_lease.closed
                or daily_feature_lease.source != self.request.daily_feature_source
            )
        ):
            raise FactorStreamAdapterError("stored_daily_binding_mismatch")
        self._daily_feature_lease = daily_feature_lease
        self._current_daily_features: FactorDailyFeatureInput | None = None
        self._pools: Iterable[FactorUniverseRequest] | None = universe_requests
        self._pool_iterator: Iterator[FactorUniverseRequest] | None = None
        self._completion: FactorStreamAdapterCompletion | None = None
        self._features_done = False
        self._closed = False
        self._failed = False
        self._features: list[FactorStreamFeatureDayReceipt] = []
        self._returns: list[FactorStreamReturnDayReceipt] = []
        self._query_count = self._bar_count = self._adj_count = 0
        self._request_sha = factor_stream_adapter_request_sha256(self.request)
        self._formula_sha = factor_formula_stream_request_sha256(self.request.formula)
        self.statistics_request = factor_stream_statistics_request(self.request)
        self._statistics_sha = factor_daily_stream_request_sha256(self.statistics_request)
        try:
            self._panels, self._windows, self._calendar_sha = self._calendar()
        except BaseException:
            self._close_pools()
            raise
        self._iterator = self._iterate()

    @property
    def completion(self) -> FactorStreamAdapterCompletion | None:
        return self._completion

    def __iter__(self) -> FactorStreamAdapter:
        return self

    def __next__(self) -> FactorFormulaStreamBatch:
        if self._closed:
            raise StopIteration
        try:
            return next(self._iterator)
        except StopIteration:
            self._features_done = True
            self._finish()
            raise
        except BaseException:
            self._failed = True
            self.close()
            raise

    def _close_pools(self) -> None:
        target = self._pool_iterator if self._pool_iterator is not None else self._pools
        self._pool_iterator = None
        self._pools = None
        close = getattr(target, "close", None)
        if close is not None:
            close()

    def close(self) -> None:
        self._current_context = None
        self._current_daily_features = None
        if not self._closed:
            self._closed = True
            try:
                self._iterator.close()
            finally:
                self._close_pools()

    def _check_receipt(
        self,
        receipt: FactorSourceBoundaryReceipt,
        query: FactorReadQuery,
        dataset: Literal["daily_bar", "adj_factor", "trade_calendar"],
    ) -> None:
        expected = FactorSourceBoundaryReceipt(
            snapshot_id=self.admission.snapshot_id,
            binding_hash=self.admission.binding_hash,
            as_of_time=self.admission.scope.as_of_time,
            source_mode="historical_retrospective",
            source_read_boundary="single_snapshot_transaction",
            dataset_id=dataset,
            stock_codes=query.stock_codes,
            start_date=query.start_date,
            end_date=query.end_date,
        )
        if FactorSourceBoundaryReceipt.model_validate(receipt) != expected:
            raise FactorStreamAdapterError("raw_receipt_mismatch")

    def _calendar(self) -> tuple[dict[date, date], dict[date, HistoricalReturnWindow], str]:
        scope = self.request.source.scope
        rows: dict[date, tuple[bool, date | None]] = {}
        hashes: list[str] = []
        start = scope.start_date
        while start <= scope.end_date:
            end = min(start + timedelta(days=365), scope.end_date)
            query = FactorReadQuery(
                binding_hash=self.admission.binding_hash,
                stock_codes=(scope.stock_codes[0],),
                start_date=start,
                end_date=end,
                row_limit=366,
            )
            batch = FactorSSECalendarBatch.model_validate(self._lease.query_sse_calendar(query))
            self._check_receipt(batch.receipt, query, "trade_calendar")
            ordered = tuple(sorted(batch.rows, key=lambda row: row.cal_date))
            for row in ordered:
                if not start <= row.cal_date <= end or row.cal_date in rows:
                    raise FactorStreamAdapterError("calendar_date_mismatch")
                rows[row.cal_date] = row.is_open, row.pretrade_date
            self._query_count += 1
            hashes.append(canonical_sha256((batch.receipt, ordered)))
            start = end + timedelta(days=1)
        opened: list[date] = []
        day = scope.start_date
        while day <= scope.end_date:
            if day not in rows:
                raise FactorStreamAdapterError("calendar_incomplete")
            is_open, previous = rows[day]
            if is_open:
                if (not opened and (previous is None or previous >= scope.start_date)) or (
                    opened and previous != opened[-1]
                ):
                    raise FactorStreamAdapterError("calendar_pretrade_mismatch")
                opened.append(day)
            day += timedelta(days=1)
        positions = {day: index for index, day in enumerate(opened)}
        calculation = self.request.formula.trading_days
        if any(day not in positions for day in calculation):
            raise FactorStreamAdapterError("calculation_day_closed")
        if tuple(opened[positions[calculation[0]] : positions[calculation[-1]] + 1]) != calculation:
            raise FactorStreamAdapterError("calculation_schedule_gap")
        if positions[calculation[0]] == 0:
            raise FactorStreamAdapterError("preceding_panel_outside_scope")
        first_eval = calculation.index(self.request.evaluation_days[0])
        if first_eval < self.request.formula.definition.max_history_window - 1:
            raise FactorStreamAdapterError("insufficient_warmup")
        panels = {day: opened[positions[day] - 1] for day in calculation}
        windows: dict[date, HistoricalReturnWindow] = {}
        for day in self.request.evaluation_days:
            end_index = positions[day] + self.request.holding_sessions - 1
            if end_index + 1 >= len(opened):
                raise FactorStreamAdapterError("return_endpoint_outside_scope")
            end_day = opened[end_index]
            windows[day] = HistoricalReturnWindow(
                decision_date=day,
                end_date=end_day,
                return_end_at=_market_time(end_day, 15),
                expected_available_at=_market_time(opened[end_index + 1], 9, 25),
            )
        if any(
            windows[left].return_end_at > _market_time(right, 9, 25)
            for left, right in pairwise(self.request.evaluation_days)
        ):
            raise FactorStreamAdapterError("window_overlap")
        return panels, windows, canonical_sha256(tuple(hashes))

    def _read_stock(
        self,
        dataset: Literal["daily_bar", "adj_factor"],
        days: tuple[date, ...],
        codes: tuple[str, ...],
    ) -> tuple[dict[tuple[date, str], FactorDailyBarRow | FactorAdjFactorRow], str]:
        result: dict[tuple[date, str], FactorDailyBarRow | FactorAdjFactorRow] = {}
        hashes: list[str] = []
        for day in sorted(set(days)):
            for start in range(0, len(codes), 500):
                chunk = codes[start : start + 500]
                query = FactorReadQuery(
                    binding_hash=self.admission.binding_hash,
                    stock_codes=chunk,
                    start_date=day,
                    end_date=day,
                    row_limit=len(chunk),
                )
                batch = (
                    FactorDailyBarBatch.model_validate(self._lease.query_daily_bars(query))
                    if dataset == "daily_bar"
                    else FactorAdjFactorBatch.model_validate(self._lease.query_adj_factors(query))
                )
                self._check_receipt(batch.receipt, query, dataset)
                ordered = tuple(sorted(batch.rows, key=lambda row: (row.trade_date, row.ts_code)))
                if len(ordered) > query.row_limit:
                    raise FactorStreamAdapterError("raw_row_limit")
                for row in ordered:
                    key = row.trade_date, row.ts_code
                    if row.trade_date != day or row.ts_code not in chunk or key in result:
                        raise FactorStreamAdapterError("raw_grid_mismatch")
                    result[key] = row
                self._query_count += 1
                if dataset == "daily_bar":
                    self._bar_count += len(ordered)
                else:
                    self._adj_count += len(ordered)
                hashes.append(canonical_sha256((batch.receipt, ordered)))
        return result, canonical_sha256((dataset, days, codes, tuple(hashes)))

    def _iterate(self) -> Iterator[FactorFormulaStreamBatch]:
        try:
            assert self._pools is not None
            self._pool_iterator = iter(self._pools)
            for day in self.request.formula.trading_days:
                try:
                    raw = next(self._pool_iterator)
                except StopIteration:
                    raise FactorStreamAdapterError("missing_universe_batch") from None
                universe = FactorUniverseRequest.model_validate(raw)
                if universe.trade_date != day:
                    raise FactorStreamAdapterError("universe_date_mismatch")
                if (
                    universe.selection != self.request.formula.selection
                    or universe.as_of != self.request.formula.as_of
                ):
                    raise FactorStreamAdapterError("universe_binding_mismatch")
                sources = self.request.formula.sources
                if universe.securities is not None:
                    if (
                        universe.securities.source_id != sources.security_source_id
                        or universe.securities.source_sha256 != sources.security_source_sha256
                    ):
                        raise FactorStreamAdapterError("archive_binding_mismatch")
                    if not set(universe.securities.complete_stock_codes) <= set(
                        self.request.source.scope.stock_codes
                    ):
                        raise FactorStreamAdapterError("security_outside_scope")
                if universe.membership is not None and (
                    universe.membership.source_id != sources.index_source_id
                    or universe.membership.source_sha256 != sources.index_source_sha256
                ):
                    raise FactorStreamAdapterError("archive_binding_mismatch")
                panel = self._panels[day]
                from rquant.factor.capability import HISTORICAL_DAILY_V1

                if (
                    set(self.request.formula.definition.dependency_columns)
                    & set(HISTORICAL_DAILY_V1.feature_catalog().columns)
                    or self._daily_feature_lease is None
                ):
                    bars, raw_sha = self._read_stock(
                        "daily_bar", (panel,), self.request.formula.computation_stock_codes
                    )
                else:
                    bars, raw_sha = {}, canonical_sha256(("stored_daily_only", panel))
                points: list[FactorFormulaFeaturePoint] = []
                stored_input = None
                stored_values = {}
                if self._daily_feature_lease is not None:
                    stored_input = read_factor_daily_feature_input(
                        self._daily_feature_lease,
                        sources.daily_features,
                        trade_date=day,
                        panel_date=panel,
                        stock_codes=self.request.formula.computation_stock_codes,
                    )
                    stored_values = {
                        (row.stock_code, field.column): value
                        for row in stored_input.rows
                        for field, value in zip(stored_input.stock_fields, row.values, strict=True)
                    }
                    raw_sha = canonical_sha256((raw_sha, stored_input.sha256))
                    self._current_daily_features = (
                        stored_input if day in self.request.evaluation_days else None
                    )
                missing = known_null = 0
                for code in self.request.formula.computation_stock_codes:
                    bar = bars.get((panel, code))
                    for column in self.request.formula.definition.dependency_columns:
                        fact = stored_values.get((code, column))
                        if fact is None and stored_input is not None:
                            fact = stored_input.market_value(column)
                        if fact is not None:
                            value = fact.value
                            state = (
                                "value"
                                if fact.status == "valid"
                                else "missing_observation"
                                if fact.status == "missing"
                                else "known_null"
                            )
                        else:
                            value = None if bar is None else getattr(bar, column)
                            state = (
                                "missing_observation"
                                if bar is None
                                else "known_null"
                                if value is None
                                else "value"
                            )
                        missing += state == "missing_observation"
                        known_null += state == "known_null"
                        points.append(
                            FactorFormulaFeaturePoint(
                                stock_code=code,
                                trade_date=day,
                                column=column,
                                state=state,
                                value=value,
                                first_visible_at=None
                                if state == "missing_observation"
                                else _market_time(day, 9, 25),
                            )
                        )
                batch = FactorFormulaStreamBatch(
                    request_sha256=self._formula_sha,
                    sources=sources,
                    universe=universe,
                    feature_points=tuple(points),
                    daily_features=stored_input,
                    context=None
                    if self._context_lease is None
                    else self._context_lease.query(
                        trade_date=day,
                        panel_date=panel,
                        stock_codes=self.request.source.scope.stock_codes,
                        assumed_visible_at=_market_time(day, 9, 25),
                    ),
                )
                if (
                    self.statistics_request.extended_statistics is not None
                    and self.statistics_request.extended_statistics.sources is not None
                ):
                    self._current_context = (
                        batch.context if day in self.request.evaluation_days else None
                    )
                self._features.append(
                    FactorStreamFeatureDayReceipt(
                        trade_date=day,
                        panel_date=panel,
                        input_sha256=factor_formula_stream_batch_sha256(batch),
                        raw_sha256=raw_sha,
                        missing_observation_count=missing,
                        known_null_count=known_null,
                        daily_feature_input_sha256=None
                        if stored_input is None
                        else stored_input.sha256,
                        daily_feature_counts=None if stored_input is None else stored_input.counts,
                        market_temperature_values=None
                        if stored_input is None
                        else stored_input.market_temperature_values,
                        context_input_sha256=None
                        if batch.context is None
                        else batch.context.sha256,
                    )
                )
                del raw, universe, bars, points, bar, stored_input, stored_values
                yield batch
                del batch
            try:
                next(self._pool_iterator)
            except StopIteration:
                pass
            else:
                raise FactorStreamAdapterError("unexpected_universe_batch")
        finally:
            self._close_pools()

    def statistics_batch(self, day: FactorFormulaStreamDay) -> FactorDailyStreamBatch:
        if self._closed or self._failed:
            raise FactorStreamAdapterError("adapter_closed")
        try:
            day = FactorFormulaStreamDay.model_validate(day)
            if (
                day.request_sha256 != self._formula_sha
                or day.sources != self.request.formula.sources
                or day.definition_sha256 != canonical_sha256(self.request.formula.definition)
                or day.factor_id != self.request.formula.definition.factor_id
                or day.version != self.request.formula.definition.version
                or day.sha256 != canonical_sha256(day.model_dump(exclude={"sha256"}))
            ):
                raise FactorStreamAdapterError("formula_binding_mismatch")
            index = len(self._returns)
            if (
                index >= len(self.request.evaluation_days)
                or day.universe.trade_date != self.request.evaluation_days[index]
            ):
                raise FactorStreamAdapterError("return_schedule_mismatch")
            window = self._windows[day.universe.trade_date]
            codes = day.universe.stock_codes
            days = (window.decision_date, window.end_date)
            raw_bars, bar_sha = self._read_stock("daily_bar", days, codes)
            raw_adjs, adj_sha = self._read_stock("adj_factor", days, codes)
            bars = cast("dict[tuple[date, str], FactorDailyBarRow]", raw_bars)
            adjustments = cast("dict[tuple[date, str], FactorAdjFactorRow]", raw_adjs)
            returns = []
            missing: Counter[str] = Counter()
            for code in codes:
                row, reason = _forward_return(
                    code=code,
                    window=window,
                    as_of=self.request.formula.as_of,
                    bars=bars,
                    adjustments=adjustments,
                )
                returns.append(row)
                if reason is not None:
                    missing[reason] += 1
            batch = FactorDailyStreamBatch(
                request_sha256=self._statistics_sha,
                sources=self.statistics_request.sources,
                universe=day.universe,
                decision_at=day.decision_at,
                return_end_at=window.return_end_at,
                factor_values=day.values,
                forward_returns=tuple(returns),
                context=self._current_context,
                daily_features=self._current_daily_features,
            )
            self._current_context = None
            self._current_daily_features = None
            self._returns.append(
                FactorStreamReturnDayReceipt(
                    window=window,
                    selected_count=len(codes),
                    raw_sha256=canonical_sha256((bar_sha, adj_sha)),
                    statistics_batch_sha256=canonical_sha256(batch),
                    missing_counts=tuple(
                        HistoricalMissingCount(reason=reason, count=count)
                        for reason, count in sorted(missing.items())
                    ),
                )
            )
            self._finish()
            return batch
        except BaseException:
            self._failed = True
            self.close()
            raise

    def _finish(self) -> None:
        if (
            not self._features_done
            or len(self._returns) != len(self.request.evaluation_days)
            or self._closed
            or self._failed
        ):
            return
        fields = {
            "admission": self.admission,
            "request_sha256": self._request_sha,
            "formula_request_sha256": self._formula_sha,
            "statistics_request_sha256": self._statistics_sha,
            "processed_days": len(self._features),
            "calendar_sha256": self._calendar_sha,
            "feature_days": tuple(self._features),
            "return_days": tuple(self._returns),
            "read_query_count": self._query_count,
            "daily_bar_row_count": self._bar_count,
            "adj_factor_row_count": self._adj_count,
        }
        if self._context_lease is not None:
            fields["context"] = self._context_lease.context.sources
            fields["context_read_query_count"] = self._context_lease.read_query_count
        if self._daily_feature_lease is not None:
            fields["daily_features"] = self.request.formula.sources.daily_features
            fields["daily_feature_read_query_count"] = self._daily_feature_lease.query_count
        fields["input_sha256"] = canonical_sha256(fields)
        self._completion = FactorStreamAdapterCompletion(**fields, sha256=canonical_sha256(fields))
