"""Historical factor inputs come only from one admitted frozen read lease."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime, time, timedelta, timezone
from pathlib import Path

import duckdb
import pytest
from pydantic import ValidationError

from rquant.data_metadata import (
    DatasetSnapshot,
    DatasetSnapshotBinding,
    DatasetSnapshotFinalization,
)
from rquant.factor import FeatureCatalog, build_factor_definition
from rquant.factor.definition import FactorDefinition
from rquant.factor_snapshot_admission import (
    FactorSnapshotAdmissionDecision,
    FactorSnapshotAdmissionRequest,
    open_factor_snapshot_admission,
)
from rquant.research_snapshot import FactorReadLease, build_factor_snapshot_binding
from rquant.storage.duckdb import DuckDBStore

_TZ = timezone(timedelta(hours=8))
_FIRST = date(2026, 7, 13)
_LAST = date(2026, 7, 30)
_STOCKS = ("000001.SZ", "000002.SZ", "000003.SZ")
_EVALUATION_DAYS = (date(2026, 7, 16), date(2026, 7, 23))


def _at(day: date, hour: int, minute: int = 0) -> datetime:
    return datetime.combine(day, time(hour, minute), _TZ)


def _definition(expression: str = "ts_mean(close, 3)") -> FactorDefinition:
    return build_factor_definition(
        factor_id="historical_price",
        name_zh="历史价格因子",
        category="technical",
        direction="higher_is_better",
        version=1,
        earliest_available_date=_FIRST,
        expression=expression,
        feature_catalog=FeatureCatalog(columns=("close",)),
    )


def _seed(store: DuckDBStore, start: date, end: date) -> None:
    previous = start - timedelta(days=3)
    calendar: list[tuple[object, ...]] = []
    bars: list[tuple[object, ...]] = []
    adjustments: list[tuple[object, ...]] = []
    open_index = 0
    day = start
    while day <= end:
        is_open = day.weekday() < 5
        calendar.append(("SSE", day, is_open, previous, "test", _at(end, 16)))
        if is_open:
            for stock_index, code in enumerate(_STOCKS):
                price = float(10 + stock_index + open_index)
                bars.append((code, day, price, price + 1, price - 1, price, 100.0, 1000.0))
                adjustments.append((code, day, 1.0))
            previous = day
            open_index += 1
        day += timedelta(days=1)
    store._conn.executemany("INSERT INTO trade_calendar VALUES (?, ?, ?, ?, ?, ?)", calendar)
    store._conn.executemany(
        """INSERT INTO daily_bar
        (ts_code, trade_date, open, high, low, close, vol, amount)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        bars,
    )
    store._conn.executemany("INSERT INTO adj_factor VALUES (?, ?, ?)", adjustments)
    for code, opened, closed, adjustment in (
        (_STOCKS[0], 10.0, 11.0, 1.0),
        (_STOCKS[1], 20.0, 12.0, 2.0),
        (_STOCKS[2], 30.0, 26.0, 1.5),
    ):
        store._conn.execute(
            "UPDATE daily_bar SET open = ? WHERE ts_code = ? AND trade_date = ?",
            [opened, code, _EVALUATION_DAYS[0]],
        )
        store._conn.execute(
            "UPDATE daily_bar SET close = ? WHERE ts_code = ? AND trade_date = ?",
            [closed, code, date(2026, 7, 22)],
        )
        store._conn.execute(
            "UPDATE adj_factor SET adj_factor = ? WHERE ts_code = ? AND trade_date = ?",
            [adjustment, code, date(2026, 7, 22)],
        )
    for code, opened, closed in (
        (_STOCKS[0], 10.0, 13.0),
        (_STOCKS[1], 20.0, 24.0),
        (_STOCKS[2], 30.0, 33.0),
    ):
        store._conn.execute(
            "UPDATE daily_bar SET open = ? WHERE ts_code = ? AND trade_date = ?",
            [opened, code, _EVALUATION_DAYS[1]],
        )
        store._conn.execute(
            "UPDATE daily_bar SET close = ? WHERE ts_code = ? AND trade_date = ?",
            [closed, code, date(2026, 7, 29)],
        )


@contextmanager
def _admitted(
    tmp_path: Path,
    *,
    start: date = _FIRST,
    end: date = _LAST,
    before_binding: Callable[[DuckDBStore], None] | None = None,
) -> Iterator[
    tuple[
        DuckDBStore,
        FactorReadLease,
        FactorSnapshotAdmissionDecision,
        DatasetSnapshot,
        DatasetSnapshotBinding,
    ]
]:
    db_path = tmp_path / "source.duckdb"
    lake_root = tmp_path / "lake"
    as_of = datetime.combine(end + timedelta(days=1), time(8), UTC)
    with DuckDBStore(db_path) as store:
        _seed(store, start, end)
        if before_binding is not None:
            before_binding(store)
        initial = DatasetSnapshot.create(
            strategy_name="factor_eval",
            manifest_id="m" * 64,
            as_of_time=as_of,
            code_commit="a" * 40,
            origin="synthetic-test",
            created_at=as_of,
        )
        store.begin_dataset_snapshot(initial)
        snapshot = store.finalize_dataset_snapshot(
            initial.snapshot_id,
            DatasetSnapshotFinalization(
                table_watermarks={
                    "manifest_start_date": start.isoformat(),
                    "manifest_end_date": end.isoformat(),
                },
                completed_at=as_of,
            ),
        )
        with duckdb.connect(str(db_path)) as source:
            binding = build_factor_snapshot_binding(
                metadata_store=store,
                source_connection=source,
                lake_root=lake_root,
                snapshot_id=snapshot.snapshot_id,
                start_date=start,
                end_date=end,
                ts_codes=_STOCKS,
                now=lambda: as_of,
            )
        admission = FactorSnapshotAdmissionRequest(
            snapshot_id=snapshot.snapshot_id,
            binding_hash=binding.binding_hash,
            start_date=start,
            end_date=end,
            source_mode="historical_retrospective",
        )
        with open_factor_snapshot_admission(
            admission, metadata_store=store, lake_root=lake_root
        ) as (lease, decision):
            yield store, lease, decision, snapshot, binding


def test_historical_adapter_builds_manual_adjusted_returns_and_paired_result(
    tmp_path: Path,
) -> None:
    from rquant.factor.historical_adapter import (
        HistoricalFactorAdapterRequest,
        assemble_historical_factor_research,
    )

    with _admitted(tmp_path) as (_store, lease, decision, snapshot, binding):
        request = HistoricalFactorAdapterRequest(
            definition=_definition(),
            stock_codes=tuple(reversed(_STOCKS)),
            pool_basis="explicit_fixed_list",
            evaluation_days=_EVALUATION_DAYS,
            query_start_date=_FIRST,
            query_end_date=_LAST,
            holding_sessions=5,
            as_of=_at(_LAST, 9, 25),
        )
        paired = assemble_historical_factor_research(lease, decision, request)
        again = assemble_historical_factor_research(lease, decision, request)

    assert paired == again
    assert paired.receipt.snapshot_id == snapshot.snapshot_id
    assert paired.receipt.binding_hash == binding.binding_hash
    assert paired.receipt.source_mode == "historical_retrospective"
    assert paired.receipt.visibility_basis == "retrospective_adapter_assumption"
    assert paired.receipt.pool_basis == "explicit_fixed_list"
    assert paired.receipt.feature_columns == ("close",)
    assert "daily_bar.open" in paired.receipt.allowed_columns
    assert "adj_factor.adj_factor" in paired.receipt.allowed_columns
    assert "trade_calendar.pretrade_date" in paired.receipt.allowed_columns
    assert paired.receipt.panel_dates[-1].decision_date == _EVALUATION_DAYS[1]
    first_panel = next(
        row for row in paired.receipt.panel_dates if row.decision_date == _EVALUATION_DAYS[0]
    )
    assert first_panel.panel_date == date(2026, 7, 15)
    assert first_panel.first_visible_at == _at(_EVALUATION_DAYS[0], 9, 25)
    assert paired.receipt.return_windows[0].end_date == date(2026, 7, 22)
    assert paired.receipt.return_windows[0].expected_available_at == _at(_EVALUATION_DAYS[1], 9, 25)
    assert paired.receipt.return_windows[1].end_date == date(2026, 7, 29)
    assert paired.receipt.return_windows[1].expected_available_at == _at(_LAST, 9, 25)
    assert paired.request.factor_source_id == paired.receipt.source_sha256
    assert paired.request.return_source_id == paired.receipt.source_sha256
    assert paired.result.factor_source_id == paired.receipt.source_sha256
    assert paired.result.return_source_id == paired.receipt.source_sha256
    assert paired.request.return_price_basis == "forward_adjusted"
    assert paired.request.factor_input.trading_days[0] == date(2026, 7, 14)
    observations = {
        (row.trade_date, row.stock_code): row.value
        for row in paired.request.factor_input.observations
    }
    assert observations[(_EVALUATION_DAYS[0], _STOCKS[0])] == 12.0
    assert all(
        row.first_visible_at == _at(row.trade_date, 9, 25)
        for row in paired.request.factor_input.observations
    )
    returns = {
        (row.decision_date, row.stock_code): row.value for row in paired.request.forward_returns
    }
    assert returns[(_EVALUATION_DAYS[0], _STOCKS[0])] == pytest.approx(0.1)
    assert returns[(_EVALUATION_DAYS[0], _STOCKS[1])] == pytest.approx(0.2)
    assert returns[(_EVALUATION_DAYS[0], _STOCKS[2])] == pytest.approx(0.3)
    assert [day.coverage.valid_count for day in paired.result.days] == [3, 3]
    assert paired.result.days[0].evaluation.normal_ic.value == pytest.approx(1.0)


def test_historical_adapter_friday_return_waits_until_monday_0925(tmp_path: Path) -> None:
    from rquant.factor.historical_adapter import (
        HistoricalFactorAdapterRequest,
        adapt_historical_factor_source,
    )

    friday = date(2026, 7, 17)
    with _admitted(tmp_path) as (_store, lease, decision, _snapshot, _binding):
        common = dict(
            definition=_definition("close"),
            stock_codes=_STOCKS,
            pool_basis="explicit_fixed_list",
            evaluation_days=(friday,),
            query_start_date=_FIRST,
            query_end_date=_LAST,
            holding_sessions=1,
        )
        unfinished = adapt_historical_factor_source(
            lease, decision, HistoricalFactorAdapterRequest(**common, as_of=_at(friday, 14))
        )
        pending = adapt_historical_factor_source(
            lease,
            decision,
            HistoricalFactorAdapterRequest(**common, as_of=_at(date(2026, 7, 20), 9, 24)),
        )
        ready = adapt_historical_factor_source(
            lease,
            decision,
            HistoricalFactorAdapterRequest(**common, as_of=_at(date(2026, 7, 20), 9, 25)),
        )
    assert {row.missing_reason for row in unfinished.request.forward_returns} == {
        "window_unfinished"
    }
    assert {row.missing_reason for row in pending.request.forward_returns} == {"visibility_pending"}
    assert {row.expected_available_at for row in pending.request.forward_returns} == {
        _at(date(2026, 7, 20), 9, 25)
    }
    assert all(row.value is not None for row in ready.request.forward_returns)


def test_revised_decision_price_changes_return_and_source_not_panel_factor(
    tmp_path: Path,
) -> None:
    from rquant.factor.historical_adapter import (
        HistoricalFactorAdapterRequest,
        adapt_historical_factor_source,
    )

    original_dir = tmp_path / "original"
    revised_dir = tmp_path / "revised"
    original_dir.mkdir()
    revised_dir.mkdir()
    request = HistoricalFactorAdapterRequest(
        definition=_definition(),
        stock_codes=_STOCKS,
        pool_basis="explicit_fixed_list",
        evaluation_days=_EVALUATION_DAYS,
        query_start_date=_FIRST,
        query_end_date=_LAST,
        holding_sessions=5,
        as_of=_at(_LAST, 9, 25),
    )
    with _admitted(original_dir) as (_store, lease, decision, _snapshot, binding):
        original = adapt_historical_factor_source(lease, decision, request)

    def revise(store: DuckDBStore) -> None:
        store._conn.execute(
            "UPDATE daily_bar SET open = 11 WHERE ts_code = ? AND trade_date = ?",
            [_STOCKS[0], _EVALUATION_DAYS[0]],
        )

    with _admitted(revised_dir, before_binding=revise) as (
        _store,
        lease,
        decision,
        _snapshot,
        revised_binding,
    ):
        revised = adapt_historical_factor_source(lease, decision, request)
    assert binding.binding_hash != revised_binding.binding_hash
    assert original.request.factor_input.observations == revised.request.factor_input.observations
    assert original.request.forward_returns != revised.request.forward_returns
    assert original.receipt.source_sha256 != revised.receipt.source_sha256
    assert original.request.factor_source_id == original.request.return_source_id
    assert revised.request.factor_source_id == revised.request.return_source_id


@pytest.mark.parametrize("change", ["missing_day", "wrong_pretrade_date"])
def test_historical_adapter_rejects_incomplete_or_inconsistent_sse_calendar(
    tmp_path: Path, change: str
) -> None:
    from rquant.factor.historical_adapter import (
        HistoricalFactorAdapterRequest,
        adapt_historical_factor_source,
    )

    def alter(store: DuckDBStore) -> None:
        if change == "missing_day":
            store._conn.execute(
                "DELETE FROM trade_calendar WHERE cal_date = ?", [date(2026, 7, 18)]
            )
        else:
            store._conn.execute(
                "UPDATE trade_calendar SET pretrade_date = ? WHERE cal_date = ?",
                [date(2026, 7, 16), date(2026, 7, 20)],
            )

    with _admitted(tmp_path, before_binding=alter) as (
        _store,
        lease,
        decision,
        _snapshot,
        _binding,
    ):
        request = HistoricalFactorAdapterRequest(
            definition=_definition(),
            stock_codes=_STOCKS,
            pool_basis="explicit_fixed_list",
            evaluation_days=_EVALUATION_DAYS,
            query_start_date=_FIRST,
            query_end_date=_LAST,
            holding_sessions=5,
            as_of=_at(_LAST, 9, 25),
        )
        with pytest.raises(ValueError, match="calendar"):
            adapt_historical_factor_source(lease, decision, request)


@pytest.mark.parametrize(
    "pretrade_date",
    [None, _FIRST, _FIRST + timedelta(days=1)],
    ids=["null", "self", "inside_range"],
)
def test_historical_adapter_rejects_invalid_first_open_predecessor(
    tmp_path: Path, pretrade_date: date | None
) -> None:
    from rquant.factor.historical_adapter import (
        HistoricalFactorAdapterRequest,
        adapt_historical_factor_source,
    )

    def alter(store: DuckDBStore) -> None:
        store._conn.execute(
            "UPDATE trade_calendar SET pretrade_date = ? WHERE cal_date = ?",
            [pretrade_date, _FIRST],
        )

    with _admitted(tmp_path, before_binding=alter) as (
        _store,
        lease,
        decision,
        _snapshot,
        _binding,
    ):
        request = HistoricalFactorAdapterRequest(
            definition=_definition(),
            stock_codes=_STOCKS,
            pool_basis="explicit_fixed_list",
            evaluation_days=_EVALUATION_DAYS,
            query_start_date=_FIRST,
            query_end_date=_LAST,
            holding_sessions=5,
            as_of=_at(_LAST, 9, 25),
        )
        with pytest.raises(ValueError, match="pretrade_date"):
            adapt_historical_factor_source(lease, decision, request)


@pytest.mark.parametrize(
    "evaluation_days,expression,query_end,holding_sessions,expected",
    [
        ((date(2026, 7, 13),), "close", _LAST, 1, "preceding panel"),
        ((date(2026, 7, 15),), "ts_mean(close, 3)", _LAST, 1, "warmup"),
        ((date(2026, 7, 18),), "close", _LAST, 1, "not open"),
        ((date(2026, 7, 23),), "close", date(2026, 7, 29), 5, "next SSE"),
        ((date(2026, 7, 16), date(2026, 7, 17)), "close", _LAST, 5, "overlap"),
    ],
)
def test_historical_adapter_rejects_missing_predecessor_warmup_maturity_or_overlap(
    tmp_path: Path,
    evaluation_days: tuple[date, ...],
    expression: str,
    query_end: date,
    holding_sessions: int,
    expected: str,
) -> None:
    from rquant.factor.historical_adapter import (
        HistoricalFactorAdapterRequest,
        adapt_historical_factor_source,
    )

    with _admitted(tmp_path) as (_store, lease, decision, _snapshot, _binding):
        request = HistoricalFactorAdapterRequest(
            definition=_definition(expression),
            stock_codes=_STOCKS,
            pool_basis="explicit_fixed_list",
            evaluation_days=evaluation_days,
            query_start_date=_FIRST,
            query_end_date=query_end,
            holding_sessions=holding_sessions,
            as_of=_at(_LAST, 9, 25),
        )
        with pytest.raises(ValueError, match=expected):
            adapt_historical_factor_source(lease, decision, request)


def test_historical_adapter_rejects_unknown_catalog_context_and_market_claim(
    tmp_path: Path,
) -> None:
    from rquant.factor.historical_adapter import (
        HistoricalFactorAdapterRequest,
        adapt_historical_factor_source,
    )

    with _admitted(tmp_path) as (_store, lease, decision, _snapshot, _binding):
        for definition in (
            build_factor_definition(
                factor_id="unknown_financial",
                name_zh="财务因子",
                category="fundamental",
                direction="higher_is_better",
                version=1,
                earliest_available_date=_FIRST,
                expression="pe",
                feature_catalog=FeatureCatalog(columns=("pe",)),
            ),
            _definition("industry_neutralize(close)"),
        ):
            request = HistoricalFactorAdapterRequest(
                definition=definition,
                stock_codes=_STOCKS,
                pool_basis="explicit_fixed_list",
                evaluation_days=_EVALUATION_DAYS,
                query_start_date=_FIRST,
                query_end_date=_LAST,
                holding_sessions=5,
                as_of=_at(_LAST, 9, 25),
            )
            with pytest.raises(ValueError, match="column|context"):
                adapt_historical_factor_source(lease, decision, request)
        with pytest.raises(ValidationError):
            HistoricalFactorAdapterRequest(
                definition=_definition(),
                stock_codes=_STOCKS,
                pool_basis="all_market",
                evaluation_days=_EVALUATION_DAYS,
                query_start_date=_FIRST,
                query_end_date=_LAST,
                holding_sessions=5,
                as_of=_at(_LAST, 9, 25),
            )


def test_historical_adapter_keeps_price_suspension_and_adjustment_absences_explicit(
    tmp_path: Path,
) -> None:
    from rquant.factor.historical_adapter import (
        HistoricalFactorAdapterRequest,
        adapt_historical_factor_source,
    )

    def alter(store: DuckDBStore) -> None:
        store._conn.execute(
            "DELETE FROM adj_factor WHERE ts_code = ? AND trade_date = ?",
            [_STOCKS[0], _EVALUATION_DAYS[0]],
        )
        store._conn.execute(
            "UPDATE adj_factor SET adj_factor = 0 WHERE ts_code = ? AND trade_date = ?",
            [_STOCKS[1], date(2026, 7, 22)],
        )
        store._conn.execute(
            "UPDATE daily_bar SET vol = 0 WHERE ts_code = ? AND trade_date = ?",
            [_STOCKS[2], _EVALUATION_DAYS[0]],
        )
        store._conn.execute(
            "UPDATE daily_bar SET open = 0 WHERE ts_code = ? AND trade_date = ?",
            [_STOCKS[0], _EVALUATION_DAYS[1]],
        )

    with _admitted(tmp_path, before_binding=alter) as (
        _store,
        lease,
        decision,
        _snapshot,
        _binding,
    ):
        request = HistoricalFactorAdapterRequest(
            definition=_definition(),
            stock_codes=_STOCKS,
            pool_basis="explicit_fixed_list",
            evaluation_days=_EVALUATION_DAYS,
            query_start_date=_FIRST,
            query_end_date=_LAST,
            holding_sessions=5,
            as_of=_at(_LAST, 9, 25),
        )
        adapted = adapt_historical_factor_source(lease, decision, request)
    reasons = {
        (row.decision_date, row.stock_code): row.missing_reason
        for row in adapted.request.forward_returns
    }
    assert reasons[(_EVALUATION_DAYS[0], _STOCKS[0])] == "source_unavailable"
    assert reasons[(_EVALUATION_DAYS[0], _STOCKS[1])] == "source_unavailable"
    assert reasons[(_EVALUATION_DAYS[0], _STOCKS[2])] == "suspended"
    assert reasons[(_EVALUATION_DAYS[1], _STOCKS[0])] == "missing_price"
    details = {item.reason: item.count for item in adapted.receipt.missing_counts}
    assert details["return_adjustment_missing"] == 1
    assert details["return_adjustment_nonpositive"] == 1
    assert details["return_suspended"] == 1
    assert details["return_price_missing"] == 1


def test_historical_adapter_preserves_absent_panel_row_and_null_feature(
    tmp_path: Path,
) -> None:
    from rquant.factor.historical_adapter import (
        HistoricalFactorAdapterRequest,
        assemble_historical_factor_research,
    )

    def alter(store: DuckDBStore) -> None:
        store._conn.execute(
            "DELETE FROM daily_bar WHERE ts_code = ? AND trade_date = ?",
            [_STOCKS[0], date(2026, 7, 15)],
        )
        store._conn.execute(
            "UPDATE daily_bar SET close = NULL WHERE ts_code = ? AND trade_date = ?",
            [_STOCKS[1], date(2026, 7, 15)],
        )

    with _admitted(tmp_path, before_binding=alter) as (
        _store,
        lease,
        decision,
        _snapshot,
        _binding,
    ):
        paired = assemble_historical_factor_research(
            lease,
            decision,
            HistoricalFactorAdapterRequest(
                definition=_definition(),
                stock_codes=_STOCKS,
                pool_basis="explicit_fixed_list",
                evaluation_days=_EVALUATION_DAYS,
                query_start_date=_FIRST,
                query_end_date=_LAST,
                holding_sessions=5,
                as_of=_at(_LAST, 9, 25),
            ),
        )
    source_missing = {item.reason: item.count for item in paired.receipt.missing_counts}
    assert source_missing["panel_row_missing"] >= 1
    assert source_missing["panel_value_missing"] >= 1
    factor_missing = {
        item.reason: item.count for item in paired.result.days[0].coverage.factor_missing_by_reason
    }
    assert factor_missing == {"missing_observation": 1, "missing_value": 1}


@pytest.mark.parametrize("bad_batch", ["wrong_generation", "duplicate_row"])
def test_historical_adapter_rejects_cross_generation_or_duplicate_public_batch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, bad_batch: str
) -> None:
    from rquant.factor.historical_adapter import (
        HistoricalFactorAdapterRequest,
        adapt_historical_factor_source,
    )

    original = FactorReadLease.query_daily_bars

    def altered(self: FactorReadLease, query: object):
        batch = original(self, query)
        if bad_batch == "wrong_generation":
            receipt = batch.receipt.model_copy(update={"binding_hash": "f" * 64})
            return batch.model_copy(update={"receipt": receipt})
        return batch.model_copy(update={"rows": (*batch.rows, batch.rows[0])})

    with _admitted(tmp_path) as (_store, lease, decision, _snapshot, _binding):
        monkeypatch.setattr(FactorReadLease, "query_daily_bars", altered)
        request = HistoricalFactorAdapterRequest(
            definition=_definition(),
            stock_codes=_STOCKS,
            pool_basis="explicit_fixed_list",
            evaluation_days=_EVALUATION_DAYS,
            query_start_date=_FIRST,
            query_end_date=_LAST,
            holding_sessions=5,
            as_of=_at(_LAST, 9, 25),
        )
        with pytest.raises(ValueError, match="admitted lease|duplicate"):
            adapt_historical_factor_source(lease, decision, request)


def test_historical_adapter_reads_more_than_one_bounded_calendar_segment(tmp_path: Path) -> None:
    from rquant.factor.historical_adapter import (
        HistoricalFactorAdapterRequest,
        adapt_historical_factor_source,
    )

    long_start = date(2025, 7, 1)
    with _admitted(tmp_path, start=long_start) as (_store, lease, decision, _snapshot, _binding):
        request = HistoricalFactorAdapterRequest(
            definition=_definition("close"),
            stock_codes=_STOCKS,
            pool_basis="explicit_fixed_list",
            evaluation_days=(_EVALUATION_DAYS[0],),
            query_start_date=long_start,
            query_end_date=_LAST,
            holding_sessions=1,
            as_of=_at(_LAST, 9, 25),
        )
        adapted = adapt_historical_factor_source(lease, decision, request)
    assert adapted.receipt.query_start_date == long_start
    assert adapted.request.forward_returns[0].value is not None


def test_oversized_pool_calendar_refuses_before_price_fact_queries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor.historical_adapter import (
        HistoricalFactorAdapterRequest,
        adapt_historical_factor_source,
    )

    long_start = date(2025, 7, 1)
    original = FactorReadLease.query_daily_bars
    price_queries = 0

    def track(self: FactorReadLease, query: object):
        nonlocal price_queries
        price_queries += 1
        return original(self, query)

    with _admitted(tmp_path, start=long_start) as (_store, lease, decision, _snapshot, _binding):
        monkeypatch.setattr(FactorReadLease, "query_daily_bars", track)
        request = HistoricalFactorAdapterRequest(
            definition=_definition("close"),
            stock_codes=tuple(f"{index:06d}.SZ" for index in range(500)),
            pool_basis="explicit_fixed_list",
            evaluation_days=(_EVALUATION_DAYS[0],),
            query_start_date=long_start,
            query_end_date=_LAST,
            holding_sessions=1,
            as_of=_at(_LAST, 9, 25),
        )
        with pytest.raises(ValueError, match="bounded pure model"):
            adapt_historical_factor_source(lease, decision, request)
    assert price_queries == 0


def test_historical_adapter_has_public_factor_exports() -> None:
    from rquant import factor
    from rquant.factor import historical_adapter

    assert (
        factor.HistoricalFactorAdapterRequest is historical_adapter.HistoricalFactorAdapterRequest
    )
    assert (
        factor.adapt_historical_factor_source is historical_adapter.adapt_historical_factor_source
    )
    assert (
        factor.assemble_historical_factor_research
        is historical_adapter.assemble_historical_factor_research
    )


def test_historical_adapter_request_rejects_duplicate_oversized_or_ambiguous_inputs() -> None:
    from rquant.factor.historical_adapter import HistoricalFactorAdapterRequest

    common = {
        "definition": _definition("close"),
        "stock_codes": _STOCKS,
        "pool_basis": "explicit_fixed_list",
        "evaluation_days": (_EVALUATION_DAYS[0],),
        "query_start_date": _FIRST,
        "query_end_date": _LAST,
        "holding_sessions": 1,
        "as_of": _at(_LAST, 9, 25),
    }
    for update in (
        {"stock_codes": (_STOCKS[0], _STOCKS[0])},
        {"stock_codes": tuple(f"{index:06d}.SZ" for index in range(501))},
        {"query_end_date": _FIRST + timedelta(days=1024)},
        {"holding_sessions": 2},
        {"as_of": datetime(2026, 7, 30, 9, 25)},
    ):
        with pytest.raises(ValidationError):
            HistoricalFactorAdapterRequest(**{**common, **update})


def test_paired_historical_result_rejects_a_result_for_another_input(tmp_path: Path) -> None:
    from rquant.factor.historical_adapter import (
        HistoricalFactorAdapterRequest,
        HistoricalFactorResearch,
        assemble_historical_factor_research,
    )

    with _admitted(tmp_path) as (_store, lease, decision, _snapshot, _binding):
        paired = assemble_historical_factor_research(
            lease,
            decision,
            HistoricalFactorAdapterRequest(
                definition=_definition(),
                stock_codes=_STOCKS,
                pool_basis="explicit_fixed_list",
                evaluation_days=_EVALUATION_DAYS,
                query_start_date=_FIRST,
                query_end_date=_LAST,
                holding_sessions=5,
                as_of=_at(_LAST, 9, 25),
            ),
        )
    changed = paired.result.model_copy(update={"input_sha256": "f" * 64})
    with pytest.raises(ValidationError, match="input"):
        HistoricalFactorResearch(receipt=paired.receipt, request=paired.request, result=changed)
