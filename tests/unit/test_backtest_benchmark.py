"""A benchmark is an exact close-to-close comparison of frozen index facts."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest
from pydantic import ValidationError

from rquant.backtest import BacktestDayResult, BacktestResult, SSECalendar
from rquant.backtest.benchmark import (
    BenchmarkSeries,
    BenchmarkSourceError,
    compare_backtest_to_benchmark,
    load_benchmark_series,
)
from rquant.paper_contracts import PaperAccountSnapshot
from rquant.storage.schema import INDEX_DAILY_BAR_DDL

_BASELINE = date(2026, 8, 7)
_FIRST = date(2026, 8, 10)
_SECOND = date(2026, 8, 11)
_THIRD = date(2026, 8, 12)
_CALENDAR_ID = "f" * 64
_INDEX = "000300.SH"


def _calendar(*, identity: str = _CALENDAR_ID) -> SSECalendar:
    dates = (_BASELINE, _FIRST, _SECOND, _THIRD)
    return SSECalendar(
        source_identity=identity,
        coverage_start=dates[0],
        coverage_end=dates[-1],
        dates=dates,
    )


def _result(
    dates: tuple[date, ...] = (_FIRST, _SECOND),
    navs: tuple[Decimal, ...] = (Decimal("1050"), Decimal("1029")),
    *,
    calendar_identity: str = _CALENDAR_ID,
) -> BacktestResult:
    previous = Decimal("1000")
    days = []
    for trade_date, nav in zip(dates, navs, strict=True):
        account = PaperAccountSnapshot(
            account_id="offline-test",
            as_of_time=datetime(trade_date.year, trade_date.month, trade_date.day, tzinfo=UTC),
            cash=nav,
            available_cash=nav,
            frozen_cash=Decimal("0"),
            realized_pnl=Decimal("0"),
            unrealized_pnl=Decimal("0"),
            nav=nav,
        )
        days.append(
            BacktestDayResult(
                trade_date=trade_date,
                rebalanced=False,
                decisions=(),
                orders=(),
                skipped=(),
                fees=Decimal("0"),
                account=account,
                market_value=Decimal("0"),
                daily_return=nav / previous - Decimal("1"),
                normalized_nav=nav / Decimal("1000"),
            )
        )
        previous = nav
    return BacktestResult(
        request_id="a" * 64,
        producer_commit="b" * 40,
        input_generation_id="c" * 64,
        calendar_source_identity=calendar_identity,
        cost_spec_id="d" * 64,
        status="complete",
        days=tuple(days),
    )


def _frozen_db(
    path: Path,
    rows: list[tuple[str, date, float | None]],
    *,
    allow_duplicates: bool = False,
) -> Path:
    connection = duckdb.connect(str(path))
    try:
        if allow_duplicates:
            connection.execute(
                "CREATE TABLE index_daily_bar (ts_code VARCHAR, trade_date DATE, close DOUBLE)"
            )
        else:
            connection.execute(INDEX_DAILY_BAR_DDL)
        for code, trade_date, close in rows:
            connection.execute(
                "INSERT INTO index_daily_bar (ts_code, trade_date, close) VALUES (?, ?, ?)",
                [code, trade_date, close],
            )
    finally:
        connection.close()
    return path


def _rows() -> list[tuple[str, date, float | None]]:
    return [(_INDEX, _BASELINE, 100.0), (_INDEX, _FIRST, 110.0), (_INDEX, _SECOND, 99.0)]


def test_two_day_close_benchmark_and_pure_comparison_are_hand_checkable(tmp_path: Path) -> None:
    result = _result()
    path = _frozen_db(tmp_path / "frozen.duckdb", _rows())

    benchmark = load_benchmark_series(path, _calendar(), result)
    comparison = compare_backtest_to_benchmark(result, benchmark)

    assert benchmark.source_mode == "retrospective_daily_bar"
    assert benchmark.source_table == "index_daily_bar"
    assert benchmark.ts_code == _INDEX
    assert benchmark.calendar_source_identity == _CALENDAR_ID
    assert benchmark.backtest_content_hash == result.content_hash
    assert len(benchmark.source_identity) == 64
    assert benchmark.baseline_trade_date == _BASELINE
    assert benchmark.baseline_close == 100.0
    assert [(day.trade_date, day.close) for day in benchmark.days] == [
        (_FIRST, 110.0),
        (_SECOND, 99.0),
    ]
    assert [day.daily_return for day in benchmark.days] == pytest.approx([0.1, -0.1])
    assert [day.normalized_nav for day in benchmark.days] == pytest.approx([1.1, 0.99])
    assert comparison.strategy_performance.total_return == pytest.approx(0.029)
    assert comparison.benchmark_performance.total_return == pytest.approx(-0.01)
    assert comparison.relative.aligned_observations == 2
    assert comparison.relative.excess_total_return == pytest.approx(1.029 / 0.99 - 1)


@pytest.mark.parametrize(
    ("missing_date", "message"),
    [(_BASELINE, "baseline"), (_FIRST, "missing"), (_SECOND, "missing")],
)
def test_missing_baseline_or_any_result_day_fails_closed(
    tmp_path: Path, missing_date: date, message: str
) -> None:
    path = _frozen_db(
        tmp_path / "missing.duckdb", [row for row in _rows() if row[1] != missing_date]
    )
    with pytest.raises(BenchmarkSourceError, match=message):
        load_benchmark_series(path, _calendar(), _result())


@pytest.mark.parametrize("bad_close", [None, 0.0, -1.0, float("nan"), float("inf")])
def test_invalid_or_nonfinite_close_fails_closed(tmp_path: Path, bad_close: float | None) -> None:
    rows = _rows()
    rows[1] = (_INDEX, _FIRST, bad_close)
    path = _frozen_db(tmp_path / "bad-close.duckdb", rows)
    with pytest.raises(BenchmarkSourceError, match="close"):
        load_benchmark_series(path, _calendar(), _result())


def test_duplicate_index_day_fails_even_if_frozen_table_lacks_primary_key(tmp_path: Path) -> None:
    path = _frozen_db(
        tmp_path / "duplicate.duckdb",
        [*_rows(), (_INDEX, _FIRST, 111.0)],
        allow_duplicates=True,
    )
    with pytest.raises(BenchmarkSourceError, match="duplicate"):
        load_benchmark_series(path, _calendar(), _result())


def test_unknown_code_and_wrong_calendar_or_result_dates_are_rejected(tmp_path: Path) -> None:
    path = _frozen_db(tmp_path / "date-mismatch.duckdb", _rows())
    with pytest.raises(BenchmarkSourceError, match="unsupported"):
        load_benchmark_series(path, _calendar(), _result(), ts_code="999999.SH")
    with pytest.raises(BenchmarkSourceError, match="calendar source"):
        load_benchmark_series(path, _calendar(identity="e" * 64), _result())
    with pytest.raises(BenchmarkSourceError, match="consecutive"):
        load_benchmark_series(
            path,
            _calendar(),
            _result(dates=(_FIRST, _THIRD), navs=(Decimal("1050"), Decimal("1029"))),
        )


def test_another_supported_index_can_be_selected_and_is_source_bound(tmp_path: Path) -> None:
    path = _frozen_db(
        tmp_path / "alternates.duckdb",
        [
            *_rows(),
            ("000905.SH", _BASELINE, 200.0),
            ("000905.SH", _FIRST, 190.0),
            ("000905.SH", _SECOND, 209.0),
        ],
    )
    first = load_benchmark_series(path, _calendar(), _result())
    alternate = load_benchmark_series(path, _calendar(), _result(), ts_code="000905.SH")
    assert first.source_identity != alternate.source_identity
    assert [day.normalized_nav for day in alternate.days] == pytest.approx([0.95, 1.045])


def test_comparison_rejects_a_different_backtest_result(tmp_path: Path) -> None:
    path = _frozen_db(tmp_path / "binding.duckdb", _rows())
    benchmark = load_benchmark_series(path, _calendar(), _result())
    changed = _result(navs=(Decimal("1100"), Decimal("1000")))
    with pytest.raises(BenchmarkSourceError, match="backtest result"):
        compare_backtest_to_benchmark(changed, benchmark)


def test_calendar_without_previous_sse_session_cannot_define_baseline(tmp_path: Path) -> None:
    path = _frozen_db(tmp_path / "no-prior-session.duckdb", _rows())
    with pytest.raises(BenchmarkSourceError, match="previous trading day"):
        load_benchmark_series(
            path,
            _calendar(),
            _result(dates=(_BASELINE,), navs=(Decimal("1000"),)),
        )


def test_close_ratio_underflow_is_a_source_error_not_a_zero_nav(tmp_path: Path) -> None:
    rows = _rows()
    rows[0] = (_INDEX, _BASELINE, 1e300)
    rows[1] = (_INDEX, _FIRST, 1e-300)
    path = _frozen_db(tmp_path / "underflow.duckdb", rows)
    with pytest.raises(BenchmarkSourceError, match="return"):
        load_benchmark_series(path, _calendar(), _result())


def test_derived_returns_cannot_change_without_changing_index_closes(tmp_path: Path) -> None:
    path = _frozen_db(tmp_path / "derived.duckdb", _rows())
    benchmark = load_benchmark_series(path, _calendar(), _result())
    altered = benchmark.model_dump(mode="python")
    altered["days"][0]["daily_return"] += 5e-11
    with pytest.raises(ValidationError, match="daily return"):
        BenchmarkSeries.model_validate(altered)


def _incomplete_result() -> BacktestResult:
    data = _result().model_dump(mode="python", exclude={"content_hash"})
    data["status"] = "incomplete"
    final = data["days"][-1]
    final["account"] = None
    final["market_value"] = None
    final["daily_return"] = None
    final["normalized_nav"] = None
    final["incomplete_reason"] = "missing_held_close"
    return BacktestResult.model_validate(data)


@pytest.mark.parametrize("frozen_exists", [True, False])
def test_loader_rejects_legitimate_incomplete_last_day_before_reading_frozen_db(
    tmp_path: Path, frozen_exists: bool
) -> None:
    result = _incomplete_result()
    assert result.status == "incomplete"
    assert result.days[-1].account is None
    path = tmp_path / "incomplete.duckdb"
    if frozen_exists:
        _frozen_db(path, _rows())
    with pytest.raises(BenchmarkSourceError, match="incomplete backtest result"):
        load_benchmark_series(path, _calendar(), result)


@pytest.mark.parametrize("missing_field", ["daily_return", "normalized_nav"])
def test_loader_rejects_complete_status_with_missing_last_day_return_evidence(
    tmp_path: Path, missing_field: str
) -> None:
    data = _result().model_dump(mode="python", exclude={"content_hash"})
    data["days"][-1][missing_field] = None
    result = BacktestResult.model_validate(data)
    assert result.status == "complete"
    path = _frozen_db(tmp_path / "missing-return.duckdb", _rows())
    with pytest.raises(BenchmarkSourceError, match="incomplete backtest result"):
        load_benchmark_series(path, _calendar(), result)
