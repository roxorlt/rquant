"""Only full source and formula exhaustion can publish final statistics."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from rquant.factor.universe import FactorUniverseRequest
from rquant.runtime_contracts import canonical_sha256
from tests.unit.test_factor_stream_adapter import _FIRST, _pools, _prepared


class _OwnedPools(Iterator[FactorUniverseRequest]):
    def __init__(self, rows: Iterator[FactorUniverseRequest]) -> None:
        self.rows = rows
        self.iterations = 0
        self.closed = False

    def __iter__(self) -> _OwnedPools:
        self.iterations += 1
        if self.iterations != 1:
            raise AssertionError("pool source was iterated twice")
        return self

    def __next__(self) -> FactorUniverseRequest:
        return next(self.rows)

    def close(self) -> None:
        self.closed = True
        self.rows.close()


def test_runner_returns_three_bound_completed_layers_and_cleans_session(tmp_path: Path) -> None:
    from rquant.factor.stream_runner import run_factor_stream_research

    with _prepared(tmp_path) as (metadata, lake, request):
        result = run_factor_stream_research(
            request, metadata_store=metadata, lake_root=lake, universe_requests=_pools(request)
        )
        assert result.adapter_completion.processed_days == 4
        assert result.formula_completion.processed_days == 4
        assert tuple(day.trade_date for day in result.statistics.days) == request.evaluation_days
        assert result.statistics.days[0].coverage.valid_count == 12
        assert result.request == request
        assert not list((lake / ".execution_sessions").glob("*"))


@pytest.mark.parametrize("tail", ["exception", "missing", "extra", "cancel"])
def test_failure_after_last_evaluation_cannot_publish_success(tmp_path: Path, tail: str) -> None:
    from rquant.factor.stream_runner import run_factor_stream_research

    with _prepared(tmp_path) as (metadata, lake, request):
        closed = []

        def source() -> Iterator[FactorUniverseRequest]:
            try:
                for item in _pools(request):
                    if item.trade_date == _FIRST + timedelta(days=4):
                        if tail == "exception":
                            raise RuntimeError("late archive failure")
                        if tail == "cancel":
                            raise KeyboardInterrupt("explicit synthetic cancellation")
                        if tail == "missing":
                            return
                    yield item
                if tail == "extra":
                    yield next(_pools(request))
            finally:
                closed.append(True)

        expected = (
            KeyboardInterrupt
            if tail == "cancel"
            else (RuntimeError if tail == "exception" else ValueError)
        )
        with pytest.raises(expected):
            run_factor_stream_research(
                request, metadata_store=metadata, lake_root=lake, universe_requests=source()
            )
        assert closed == [True]
        assert not list((lake / ".execution_sessions").iterdir())


def test_admission_failure_closes_owned_unstarted_source(tmp_path: Path) -> None:
    from rquant.factor.stream_runner import run_factor_stream_research
    from rquant.factor.stream_snapshot import FactorStreamSnapshotAdmissionError

    with _prepared(tmp_path) as (metadata, lake, request):
        source = _OwnedPools(_pools(request))
        invalid = request.model_copy(
            update={"source": request.source.model_copy(update={"binding_hash": "0" * 64})}
        )
        invalid = invalid.model_copy(
            update={
                "formula": request.formula.model_copy(
                    update={
                        "sources": request.formula.sources.model_copy(
                            update={"feature_source_sha256": "0" * 64}
                        )
                    }
                )
            }
        )
        with pytest.raises(FactorStreamSnapshotAdmissionError, match="source_identity"):
            run_factor_stream_research(
                invalid, metadata_store=metadata, lake_root=lake, universe_requests=source
            )
        assert source.closed
        assert source.iterations == 0
        assert not list((lake / ".execution_sessions").glob("*"))


def test_rehashed_completion_cannot_change_input_digest(tmp_path: Path) -> None:
    from rquant.factor.stream_adapter import FactorStreamAdapterCompletion
    from rquant.factor.stream_runner import run_factor_stream_research

    with _prepared(tmp_path) as (metadata, lake, request):
        result = run_factor_stream_research(
            request, metadata_store=metadata, lake_root=lake, universe_requests=_pools(request)
        )
        fields = result.adapter_completion.model_dump(exclude={"sha256"})
        fields["input_sha256"] = "0" * 64
        with pytest.raises(ValidationError, match="input digest"):
            FactorStreamAdapterCompletion(**fields, sha256=canonical_sha256(fields))


@pytest.mark.parametrize("holding", [1, 5, 10, 20])
def test_four_periods_match_actual_legacy_adapter_and_batch_statistics(
    tmp_path: Path, holding: int
) -> None:
    import duckdb

    from rquant.data_metadata import DatasetSnapshot, DatasetSnapshotFinalization
    from rquant.factor.historical_adapter import (
        HistoricalFactorAdapterRequest,
        assemble_historical_factor_research,
    )
    from rquant.factor.stream_runner import run_factor_stream_research
    from rquant.factor_snapshot_admission import (
        FactorSnapshotAdmissionRequest,
        open_factor_snapshot_admission,
    )
    from rquant.research_snapshot import build_factor_snapshot_binding
    from tests.unit.test_factor_stream_adapter import _AS_OF

    evaluation = (2,) if holding == 20 else (2, 2 + holding)

    def adjustments(source: duckdb.DuckDBPyConnection) -> None:
        source.execute(
            "UPDATE adj_factor SET adj_factor = 1. + date_diff('day', ?, trade_date) / 100.",
            [_FIRST],
        )

    with _prepared(
        tmp_path,
        holding=holding,
        evaluation=evaluation,
        calculation=tuple(range(1, evaluation[-1] + 2)),
        mutate=adjustments,
    ) as (metadata, lake, request):
        streamed = run_factor_stream_research(
            request, metadata_store=metadata, lake_root=lake, universe_requests=_pools(request)
        )
        initial = DatasetSnapshot.create(
            strategy_name="factor_eval",
            manifest_id="g" * 64,
            as_of_time=_AS_OF,
            code_commit="a" * 40,
            origin="synthetic-v1-golden",
            created_at=_AS_OF,
        )
        metadata.begin_dataset_snapshot(initial)
        snapshot = metadata.finalize_dataset_snapshot(
            initial.snapshot_id,
            DatasetSnapshotFinalization(
                table_watermarks={
                    "manifest_start_date": request.source.scope.start_date.isoformat(),
                    "manifest_end_date": request.source.scope.end_date.isoformat(),
                },
                completed_at=_AS_OF,
            ),
        )
        with duckdb.connect(str(tmp_path / "source.duckdb")) as source:
            binding = build_factor_snapshot_binding(
                metadata_store=metadata,
                source_connection=source,
                lake_root=lake,
                snapshot_id=snapshot.snapshot_id,
                start_date=request.source.scope.start_date,
                end_date=request.source.scope.end_date,
                ts_codes=request.formula.computation_stock_codes,
                now=lambda: _AS_OF,
            )
        admission = FactorSnapshotAdmissionRequest(
            snapshot_id=snapshot.snapshot_id,
            binding_hash=binding.binding_hash,
            start_date=request.source.scope.start_date,
            end_date=request.source.scope.end_date,
            source_mode="historical_retrospective",
        )
        with open_factor_snapshot_admission(admission, metadata_store=metadata, lake_root=lake) as (
            lease,
            decision,
        ):
            legacy = assemble_historical_factor_research(
                lease,
                decision,
                HistoricalFactorAdapterRequest(
                    definition=request.formula.definition,
                    stock_codes=request.formula.computation_stock_codes,
                    pool_basis="explicit_fixed_list",
                    evaluation_days=request.evaluation_days,
                    query_start_date=request.source.scope.start_date,
                    query_end_date=request.source.scope.end_date,
                    holding_sessions=holding,
                    as_of=request.formula.as_of,
                ),
            )
        assert streamed.statistics.ic_summary == legacy.result.ic_summary
        assert legacy.result.portfolio_diagnostics is not None
        for streamed_day, legacy_day, legacy_portfolio in zip(
            streamed.statistics.days,
            legacy.result.days,
            legacy.result.portfolio_diagnostics.days,
            strict=True,
        ):
            assert streamed_day.evaluation == legacy_day.evaluation
            assert streamed_day.coverage.valid_count == legacy_day.coverage.valid_count == 12
            for grouping, expected in zip(
                streamed_day.portfolio_groupings, legacy_portfolio.groupings, strict=True
            ):
                assert grouping.model_dump(exclude={"cumulative_status"}) == expected.model_dump()
        assert not list((lake / ".execution_sessions").iterdir())


def test_raw_row_order_does_not_change_bound_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor.stream_runner import run_factor_stream_research
    from rquant.research_snapshot import FactorDailyBarBatch, FactorReadQuery, FactorStreamReadLease

    original = FactorStreamReadLease.query_daily_bars

    def reversed_rows(lease: FactorStreamReadLease, query: FactorReadQuery) -> FactorDailyBarBatch:
        batch = original(lease, query)
        return batch.model_copy(update={"rows": tuple(reversed(batch.rows))})

    with _prepared(tmp_path) as (metadata, lake, request):
        first = run_factor_stream_research(
            request, metadata_store=metadata, lake_root=lake, universe_requests=_pools(request)
        )
        monkeypatch.setattr(FactorStreamReadLease, "query_daily_bars", reversed_rows)
        second = run_factor_stream_research(
            request, metadata_store=metadata, lake_root=lake, universe_requests=_pools(request)
        )
        assert first == second


def test_7000_stock_16_day_pipeline_is_bounded_and_releases_all_source_objects(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import gc
    import tracemalloc
    import weakref
    from time import perf_counter

    import duckdb

    from rquant.factor import stream_snapshot
    from rquant.factor.stream_runner import run_factor_stream_research
    from rquant.research_snapshot import (
        FactorAdjFactorBatch,
        FactorDailyBarBatch,
        FactorReadQuery,
        FactorStreamReadLease,
    )

    sessions = []
    source_refs = []
    raw_refs = []
    query_count = 0
    session_class = stream_snapshot.ResearchExecutionSession
    daily_query = FactorStreamReadLease.query_daily_bars
    adj_query = FactorStreamReadLease.query_adj_factors

    def session(**kwargs: object) -> object:
        created = session_class(**kwargs)
        sessions.append(created)
        return created

    def check_query(query: FactorReadQuery) -> None:
        nonlocal query_count
        query_count += 1
        assert len(query.stock_codes) <= 500
        assert query.start_date == query.end_date
        assert query.row_limit <= 100_000

    def bars(lease: FactorStreamReadLease, query: FactorReadQuery) -> FactorDailyBarBatch:
        check_query(query)
        batch = daily_query(lease, query)
        raw_refs.extend((weakref.ref(batch), weakref.ref(batch.rows[0])))
        return batch

    def adjustments(lease: FactorStreamReadLease, query: FactorReadQuery) -> FactorAdjFactorBatch:
        check_query(query)
        batch = adj_query(lease, query)
        raw_refs.extend((weakref.ref(batch), weakref.ref(batch.rows[0])))
        return batch

    class MeasuredPools(_OwnedPools):
        def __next__(self) -> FactorUniverseRequest:
            assert all(ref() is None for ref in source_refs)
            item = super().__next__()
            assert item.securities is not None
            source_refs.extend(
                (
                    weakref.ref(item),
                    weakref.ref(item.securities),
                    weakref.ref(item.securities.facts[0]),
                )
            )
            return item

    with _prepared(
        tmp_path,
        count=7_000,
        raw_days=18,
        calculation=tuple(range(1, 17)),
        evaluation=tuple(range(2, 16)),
        expression="cs_rank(ts_mean(close, 2))",
    ) as (metadata, lake, request):
        source = MeasuredPools(_pools(request))
        monkeypatch.setattr(stream_snapshot, "ResearchExecutionSession", session)
        monkeypatch.setattr(FactorStreamReadLease, "query_daily_bars", bars)
        monkeypatch.setattr(FactorStreamReadLease, "query_adj_factors", adjustments)
        print("SCALE_START: one synthetic 16-day × 7,000-stock rolling + cs run", flush=True)
        started = perf_counter()
        tracemalloc.start()
        try:
            result = run_factor_stream_research(
                request, metadata_store=metadata, lake_root=lake, universe_requests=source
            )
            gc.collect()
            retained, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        assert result.adapter_completion.processed_days == 16
        assert all(day.coverage.valid_count == 7_000 for day in result.statistics.days)
        assert result.adapter_completion.daily_bar_row_count == 210_000
        assert result.adapter_completion.adj_factor_row_count == 98_000
        assert query_count == 616 and result.adapter_completion.read_query_count == 617
        assert source.closed and source.iterations == 1
        assert len(source_refs) == 48 and all(ref() is None for ref in source_refs)
        assert len(raw_refs) == 1_232 and all(ref() is None for ref in raw_refs)
        assert len(sessions) == 1
        assert not list((lake / ".execution_sessions").iterdir())
        with pytest.raises(duckdb.ConnectionException):
            sessions[0].query_index_daily(_FIRST)
        assert peak < 256 * 1024 * 1024 and retained < 20 * 1024 * 1024
        print(
            f"SCALE_RESULT: days=16 stocks=7000 source_refs_released={len(source_refs)} "
            f"raw_refs_released={len(raw_refs)} queries=617 daily_rows=210000 adj_rows=98000 "
            f"python_peak_bytes={peak} python_retained_bytes={retained} "
            f"elapsed_seconds={perf_counter() - started:.3f} "
            "sessions=1 session_closed=true; traced Python allocations only, "
            "not RSS or real-copy load",
            flush=True,
        )
