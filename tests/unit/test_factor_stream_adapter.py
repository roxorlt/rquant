"""Synthetic v2 prices map to complete daily factor and return facts."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb
import pytest

from rquant.factor.capability import HISTORICAL_DAILY_V1
from rquant.factor.definition import build_factor_definition
from rquant.factor.formula_stream import FactorFormulaStreamRequest, FactorFormulaStreamSources
from rquant.factor.stream_snapshot import open_factor_stream_snapshot_admission
from rquant.factor.time_series import DecisionTime
from rquant.factor.universe import (
    DailyIndexConstituentBatch,
    DailySecurityBatch,
    DailySecurityFact,
    FactorUniverseRequest,
)
from tests.unit.test_factor_stream_snapshot import _AS_OF, _FIRST, _build, _source

if TYPE_CHECKING:
    from rquant.factor.stream_adapter import FactorStreamAdapterRequest
    from rquant.factor.universe import UniverseSelection
    from rquant.storage.duckdb import DuckDBStore

_TZ = timezone(timedelta(hours=8))


def _at(day: date, hour: int, minute: int = 0) -> datetime:
    return datetime.combine(day, time(hour, minute), _TZ)


@contextmanager
def _prepared(
    tmp_path: Path,
    *,
    count: int = 12,
    raw_days: int = 31,
    calculation: tuple[int, ...] = (1, 2, 3, 4),
    evaluation: tuple[int, ...] = (2, 3),
    holding: int = 1,
    expression: str = "ts_mean(close, 2)",
    as_of: datetime = _AS_OF,
    selection: UniverseSelection = "all",
    mutate: Callable[[duckdb.DuckDBPyConnection], None] | None = None,
) -> Iterator[tuple[DuckDBStore, Path, FactorStreamAdapterRequest]]:
    from rquant.factor.stream_adapter import FactorStreamAdapterRequest

    with _source(tmp_path, count=count, days=raw_days, read_only=mutate is None) as (
        metadata,
        source,
        snapshot,
        lake,
    ):
        if mutate is not None:
            mutate(source)
        binding, admission = _build(metadata, source, snapshot, lake, count=count, days=raw_days)
        formula = FactorFormulaStreamRequest(
            definition=build_factor_definition(
                factor_id="stream_price",
                name_zh="流式价格样本",
                category="technical",
                direction="higher_is_better",
                version=1,
                earliest_available_date=None,
                expression=expression,
                feature_catalog=HISTORICAL_DAILY_V1.feature_catalog(),
            ),
            computation_stock_codes=admission.scope.stock_codes,
            trading_days=tuple(_FIRST + timedelta(days=offset) for offset in calculation),
            decision_times=tuple(
                DecisionTime(
                    trade_date=_FIRST + timedelta(days=offset),
                    decision_at=_at(_FIRST + timedelta(days=offset), 9, 25),
                )
                for offset in calculation
            ),
            as_of=as_of,
            selection=selection,
            sources=FactorFormulaStreamSources(
                source_mode="historical_retrospective",
                feature_source_id=admission.snapshot_id,
                feature_source_sha256=admission.binding_hash,
                security_source_id="synthetic-security-archive",
                security_source_sha256="b" * 64,
                index_source_id="synthetic-index-archive"
                if selection in ("hs300", "zz1000")
                else None,
                index_source_sha256="c" * 64 if selection in ("hs300", "zz1000") else None,
            ),
        )
        scope_artifact = next(
            a for a in binding.manifest.artifacts if a.table_name == "factor_computation_scope"
        )
        yield (
            metadata,
            lake,
            FactorStreamAdapterRequest(
                source=admission,
                scope_content_hash=scope_artifact.content_hash,
                formula=formula,
                evaluation_days=tuple(_FIRST + timedelta(days=offset) for offset in evaluation),
                holding_sessions=holding,
            ),
        )


def _pools(
    request: FactorStreamAdapterRequest,
    members: dict[date, tuple[str, ...]] | None = None,
) -> Iterator[FactorUniverseRequest]:
    for day in request.formula.trading_days:
        selected = request.formula.computation_stock_codes if members is None else members[day]
        is_index = request.formula.selection in ("hs300", "zz1000")
        securities = DailySecurityBatch(
            trade_date=day,
            source_id=request.formula.sources.security_source_id,
            source_sha256=request.formula.sources.security_source_sha256,
            source_mode="historical_retrospective",
            security_scope="china_a_share",
            observed_at=_at(day, 9),
            complete_stock_codes=request.formula.computation_stock_codes,
            facts=tuple(
                DailySecurityFact(
                    stock_code=code,
                    exchange="SZ",
                    board="gem" if request.formula.selection == "gem" else "main",
                    is_listed=True,
                    is_st=False if is_index else code not in selected,
                )
                for code in request.formula.computation_stock_codes
            ),
        )
        membership = None
        if is_index:
            membership = DailyIndexConstituentBatch(
                selection=request.formula.selection,
                trade_date=day,
                source_id=request.formula.sources.index_source_id,
                source_sha256=request.formula.sources.index_source_sha256,
                source_mode="historical_retrospective",
                source_kind="daily_complete_membership",
                observed_at=_at(day, 9),
                stock_codes=selected,
            )
        yield FactorUniverseRequest(
            selection=request.formula.selection,
            trade_date=day,
            as_of=request.formula.as_of,
            securities=securities,
            membership=membership,
        )


def test_raw_null_missing_and_value_remain_distinct(tmp_path: Path) -> None:
    from rquant.factor.stream_adapter import FactorStreamAdapter

    def change(source: duckdb.DuckDBPyConnection) -> None:
        source.execute(
            "UPDATE daily_bar SET close = NULL WHERE ts_code = '000001.SZ' AND trade_date = ?",
            [_FIRST],
        )
        source.execute(
            "DELETE FROM daily_bar WHERE ts_code = '000002.SZ' AND trade_date = ?", [_FIRST]
        )

    with (
        _prepared(tmp_path, count=3, mutate=change) as (metadata, lake, request),
        open_factor_stream_snapshot_admission(
            request.source, metadata_store=metadata, lake_root=lake
        ) as (lease, decision),
    ):
        adapter = FactorStreamAdapter(
            request, lease=lease, decision=decision, universe_requests=_pools(request)
        )
        batch = next(adapter)
        assert [(p.stock_code, p.state) for p in batch.feature_points] == [
            ("000001.SZ", "known_null"),
            ("000002.SZ", "missing_observation"),
            ("000003.SZ", "value"),
        ]
        assert batch.feature_points[0].first_visible_at == _at(_FIRST + timedelta(days=1), 9, 25)
        assert batch.feature_points[1].first_visible_at is None
        assert batch.feature_points[2].value == pytest.approx(10.0003)
        assert adapter.completion is None
        adapter.close()
        assert adapter.completion is None


@pytest.mark.parametrize("holding", [1, 5, 10, 20])
def test_complete_adapter_uses_exact_sse_end_and_maturity(tmp_path: Path, holding: int) -> None:
    from rquant.factor.formula_stream import evaluate_factor_formula_stream
    from rquant.factor.stream_adapter import FactorStreamAdapter

    with (
        _prepared(tmp_path, holding=holding, evaluation=(2,)) as (metadata, lake, request),
        open_factor_stream_snapshot_admission(
            request.source, metadata_store=metadata, lake_root=lake
        ) as (lease, decision),
    ):
        adapter = FactorStreamAdapter(
            request, lease=lease, decision=decision, universe_requests=_pools(request)
        )
        formula = evaluate_factor_formula_stream(request.formula, adapter)
        batches = [
            adapter.statistics_batch(day)
            for day in formula
            if day.universe.trade_date in request.evaluation_days
        ]
        assert formula.completion is not None and adapter.completion is not None
        row = batches[0].forward_returns[0]
        day = request.evaluation_days[0]
        assert row.return_end_at == _at(day + timedelta(days=holding - 1), 15)
        assert row.first_available_at == _at(day + timedelta(days=holding), 9, 25)
        assert row.value == pytest.approx((10.0001 + 2 + holding - 1) / 10 - 1)
        assert adapter.completion.processed_days == len(request.formula.trading_days)
        adapter.close()


@pytest.mark.parametrize(
    "cutoff,reason", [(time(14), "window_unfinished"), (time(16), "visibility_pending")]
)
def test_analysis_cutoff_preserves_return_maturity(
    tmp_path: Path, cutoff: time, reason: str
) -> None:
    from rquant.factor.formula_stream import evaluate_factor_formula_stream
    from rquant.factor.stream_adapter import FactorStreamAdapter

    day = _FIRST + timedelta(days=2)
    as_of = datetime.combine(day, cutoff, _TZ)
    with (
        _prepared(tmp_path, calculation=(1, 2), evaluation=(2,), as_of=as_of) as (
            metadata,
            lake,
            request,
        ),
        open_factor_stream_snapshot_admission(
            request.source, metadata_store=metadata, lake_root=lake
        ) as (lease, decision),
    ):
        adapter = FactorStreamAdapter(
            request, lease=lease, decision=decision, universe_requests=_pools(request)
        )
        formula = evaluate_factor_formula_stream(request.formula, adapter)
        batches = [
            adapter.statistics_batch(item) for item in formula if item.universe.trade_date == day
        ]
        assert {r.missing_reason for r in batches[0].forward_returns} == {reason}
        assert all(r.value is None for r in batches[0].forward_returns)
        assert adapter.completion is not None
        adapter.close()


@pytest.mark.parametrize("expression", ["ref(close, 1)", "ref(cs_rank(close), 1)"])
def test_dynamic_pool_preserves_raw_history_but_not_past_cs_membership(
    tmp_path: Path, expression: str
) -> None:
    from rquant.factor.formula_stream import evaluate_factor_formula_stream
    from rquant.factor.stream_adapter import FactorStreamAdapter

    with _prepared(tmp_path, count=3, expression=expression) as (metadata, lake, request):
        codes = request.formula.computation_stock_codes
        members = {day: codes[1:] for day in request.formula.trading_days}
        members[request.formula.trading_days[0]] = codes[:2]
        with open_factor_stream_snapshot_admission(
            request.source, metadata_store=metadata, lake_root=lake
        ) as (lease, decision):
            adapter = FactorStreamAdapter(
                request, lease=lease, decision=decision, universe_requests=_pools(request, members)
            )
            formula = evaluate_factor_formula_stream(request.formula, adapter)
            next(formula)
            selected = next(formula)
            assert selected.universe.stock_codes == codes[1:]
            if expression == "ref(close, 1)":
                assert [row.value for row in selected.values] == pytest.approx([10.0002, 10.0003])
            else:
                assert selected.values[0].value == 1.0
                assert selected.values[1].value is None
                assert selected.values[1].missing_reason == "missing_observation"
            formula.close()
            assert adapter.completion is None


def test_legal_empty_pool_and_missing_archive_are_distinct_at_pipeline_boundary(
    tmp_path: Path,
) -> None:
    from rquant.factor.stream_runner import run_factor_stream_research

    with _prepared(tmp_path) as (metadata, lake, request):
        result = run_factor_stream_research(
            request,
            metadata_store=metadata,
            lake_root=lake,
            universe_requests=_pools(request, {day: () for day in request.formula.trading_days}),
        )
        assert all(
            day.status == "no_samples" and day.coverage.expected_count == 0
            for day in result.statistics.days
        )
        assert all(day.selected_count == 0 for day in result.adapter_completion.return_days)

        def missing() -> Iterator[FactorUniverseRequest]:
            for pool in _pools(request):
                yield pool.model_copy(update={"securities": None})

        with pytest.raises(ValueError, match="security_source_missing"):
            run_factor_stream_research(
                request, metadata_store=metadata, lake_root=lake, universe_requests=missing()
            )
        assert not list((lake / ".execution_sessions").iterdir())


def test_return_missing_reasons_survive_raw_to_statistics_pairing(tmp_path: Path) -> None:
    from rquant.factor.stream_runner import run_factor_stream_research

    day = _FIRST + timedelta(days=2)

    def gaps(source: duckdb.DuckDBPyConnection) -> None:
        source.execute(
            "DELETE FROM daily_bar WHERE ts_code = '000001.SZ' AND trade_date = ?", [day]
        )
        source.execute(
            "UPDATE daily_bar SET close = NULL WHERE ts_code = '000002.SZ' AND trade_date = ?",
            [day],
        )
        source.execute(
            "UPDATE daily_bar SET vol = 0 WHERE ts_code = '000003.SZ' AND trade_date = ?", [day]
        )
        source.execute(
            "DELETE FROM adj_factor WHERE ts_code = '000004.SZ' AND trade_date = ?", [day]
        )
        source.execute(
            "UPDATE adj_factor SET adj_factor = 0 WHERE ts_code = '000005.SZ' AND trade_date = ?",
            [day],
        )

    with _prepared(tmp_path, count=6, evaluation=(2,), mutate=gaps) as (metadata, lake, request):
        result = run_factor_stream_research(
            request, metadata_store=metadata, lake_root=lake, universe_requests=_pools(request)
        )
        coverage = result.statistics.days[0].coverage
        assert coverage.expected_count == 6 and coverage.valid_count == 1
        counts = {
            row.reason: row.count for row in result.adapter_completion.return_days[0].missing_counts
        }
        assert counts == {
            "return_price_missing": 2,
            "return_suspended": 1,
            "return_adjustment_missing": 1,
            "return_adjustment_nonpositive": 1,
        }
        assert {row.reason: row.count for row in coverage.return_missing_by_reason} == {
            "missing_price": 2,
            "suspended": 1,
            "source_unavailable": 2,
        }


def test_archive_binding_mismatch_is_actually_compared(tmp_path: Path) -> None:
    from rquant.factor.stream_runner import run_factor_stream_research

    with _prepared(tmp_path) as (metadata, lake, request):

        def mismatched() -> Iterator[FactorUniverseRequest]:
            for pool in _pools(request):
                assert pool.securities is not None
                yield pool.model_copy(
                    update={
                        "securities": pool.securities.model_copy(update={"source_sha256": "0" * 64})
                    }
                )

        with pytest.raises(ValueError, match="archive_binding_mismatch"):
            run_factor_stream_research(
                request, metadata_store=metadata, lake_root=lake, universe_requests=mismatched()
            )
        assert not list((lake / ".execution_sessions").iterdir())
