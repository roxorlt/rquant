"""全市场日线历史回补与 PIT daily_state 重算测试。"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import pytest

from rquant.market_backfill import backfill_market_daily, recompute_daily_state
from rquant.security_status import SHANGHAI, SecurityStatusDaily
from rquant.storage.duckdb import DuckDBStore

INGESTED_AT = datetime(2020, 1, 5, 8, 0, tzinfo=UTC)
SOURCE_AS_OF = date(2020, 1, 5)


def _no_sleep(_: float) -> None:
    return None


class _FakeMarketAdapter:
    def __init__(self) -> None:
        self.trade_cal_calls: list[tuple[date, date]] = []
        self.daily_calls: list[date] = []
        self.basic_calls: list[date] = []
        self.factor_calls: list[date] = []
        self.namechange_calls: list[tuple[date, date, str | None]] = []
        self.stock_st_calls: list[date] = []

    def trade_cal(self, start: date, end: date) -> list[date]:
        self.trade_cal_calls.append((start, end))
        all_dates = [date(2020, 1, 2), date(2020, 1, 3)]
        return [d for d in all_dates if start <= d <= end]

    def daily_by_date(self, trade_date: date) -> pd.DataFrame:
        self.daily_calls.append(trade_date)
        return pd.DataFrame(
            [
                {
                    "ts_code": "600000.SH",
                    "trade_date": trade_date,
                    "open": 10.0,
                    "high": 10.5,
                    "low": 9.9,
                    "close": 10.2,
                    "pre_close": 10.0,
                    "change": 0.2,
                    "pct_chg": 2.0,
                    "vol": 1000.0,
                    "amount": 10200.0,
                }
            ]
        )

    def daily_basic_by_date(self, trade_date: date) -> pd.DataFrame:
        self.basic_calls.append(trade_date)
        return pd.DataFrame(
            [
                {
                    "ts_code": "600000.SH",
                    "trade_date": trade_date,
                    "turnover_rate": 1.0,
                    "volume_ratio": 1.2,
                    "total_mv": 100000.0,
                    "circ_mv": 80000.0,
                }
            ]
        )

    def adj_factor_by_date(self, trade_date: date) -> pd.DataFrame:
        self.factor_calls.append(trade_date)
        return pd.DataFrame(
            [
                {
                    "ts_code": "600000.SH",
                    "trade_date": trade_date,
                    "adj_factor": 1.23,
                }
            ]
        )

    def namechange_raw(
        self,
        start_date: date,
        end_date: date,
        ts_code: str | None = None,
    ) -> pd.DataFrame:
        self.namechange_calls.append((start_date, end_date, ts_code))
        return pd.DataFrame(
            [
                {
                    "ts_code": "600000.SH",
                    "name": "浦发银行",
                    "start_date": "19991110",
                    "end_date": None,
                    "ann_date": "19991110",
                    "change_reason": "上市",
                },
                {
                    "ts_code": "000001.SZ",
                    "name": "平安银行",
                    "start_date": "19910403",
                    "end_date": None,
                    "ann_date": "19910403",
                    "change_reason": "上市",
                },
            ]
        )

    def stock_st_raw(self, trade_date: date) -> pd.DataFrame:
        self.stock_st_calls.append(trade_date)
        return pd.DataFrame(
            columns=["ts_code", "name", "trade_date", "type", "type_name"]
        )


class _FailingDayAdapter(_FakeMarketAdapter):
    def daily_by_date(self, trade_date: date) -> pd.DataFrame:
        if trade_date == date(2020, 1, 3):
            self.daily_calls.append(trade_date)
            raise RuntimeError("rate limit")
        return super().daily_by_date(trade_date)


class _UnknownStatusAdapter(_FakeMarketAdapter):
    def namechange_raw(
        self,
        start_date: date,
        end_date: date,
        ts_code: str | None = None,
    ) -> pd.DataFrame:
        self.namechange_calls.append((start_date, end_date, ts_code))
        return pd.DataFrame()


class _FailingStatusAdapter(_FakeMarketAdapter):
    def namechange_raw(
        self,
        start_date: date,
        end_date: date,
        ts_code: str | None = None,
    ) -> pd.DataFrame:
        del start_date, end_date, ts_code
        raise RuntimeError("historical status unavailable")


class _OneDayAdapter(_FakeMarketAdapter):
    def trade_cal(self, start: date, end: date) -> list[date]:
        self.trade_cal_calls.append((start, end))
        target = date(2020, 1, 2)
        return [target] if start <= target <= end else []


class _FailingStockSTAdapter(_OneDayAdapter):
    def stock_st_raw(self, trade_date: date) -> pd.DataFrame:
        self.stock_st_calls.append(trade_date)
        raise RuntimeError("stock_st unavailable")


class _EmptyCalendarAdapter(_FakeMarketAdapter):
    def trade_cal(self, start: date, end: date) -> list[date]:
        self.trade_cal_calls.append((start, end))
        return []


class _RetryingDailyAdapter(_OneDayAdapter):
    def __init__(self) -> None:
        super().__init__()
        self.physical_daily_attempts = 0

    def daily_by_date(self, trade_date: date) -> pd.DataFrame:
        self.physical_daily_attempts += 3
        return super().daily_by_date(trade_date)


class _OutOfRangeCalendarAdapter(_FakeMarketAdapter):
    def trade_cal(self, start: date, end: date) -> list[date]:
        self.trade_cal_calls.append((start, end))
        return [date(2019, 12, 31), date(2020, 1, 2), date(2020, 1, 6)]


class _FailingOnlyDailyAdapter(_OneDayAdapter):
    def daily_by_date(self, trade_date: date) -> pd.DataFrame:
        self.daily_calls.append(trade_date)
        raise RuntimeError("daily unavailable")


class _FactOnlyAdapter(_OneDayAdapter):
    def daily_by_date(self, trade_date: date) -> pd.DataFrame:
        self.daily_calls.append(trade_date)
        return pd.DataFrame()


class _TwoCodeAdapter(_OneDayAdapter):
    def daily_by_date(self, trade_date: date) -> pd.DataFrame:
        frame = super().daily_by_date(trade_date)
        return pd.concat(
            [
                frame,
                pd.DataFrame(
                    [
                        {
                            **frame.iloc[0].to_dict(),
                            "ts_code": "000001.SZ",
                            "close": 5.1,
                            "pre_close": 5.0,
                        }
                    ]
                ),
            ],
            ignore_index=True,
        )


class _StoreFactory:
    def __init__(
        self,
        db_path: Path,
        store_type: type[DuckDBStore] = DuckDBStore,
    ) -> None:
        self.db_path = db_path
        self.store_type = store_type
        self.calls = 0

    def __call__(self) -> DuckDBStore:
        self.calls += 1
        return self.store_type(self.db_path)


@pytest.fixture()
def db_path(tmp_path: Path) -> Path:
    path = tmp_path / "test.duckdb"
    with DuckDBStore(path):
        pass
    return path


def _backfill(
    db_path: Path,
    adapter: _FakeMarketAdapter,
    *,
    dry_run: bool = False,
    store_factory: Callable[[], DuckDBStore] | None = None,
) -> dict[str, object]:
    return backfill_market_daily(
        "2020-01-01",
        "2020-01-05",
        adapter,
        store_factory=store_factory or _StoreFactory(db_path),
        dry_run=dry_run,
        api_sleep=0.0,
        status_ingested_at=INGESTED_AT,
        status_source_as_of=SOURCE_AS_OF,
        status_namechange_start=date(2020, 1, 1),
        status_request_interval_seconds=0,
        sleep=_no_sleep,
    )


def _recompute(
    store: DuckDBStore,
    *,
    codes: list[str] | None = None,
) -> int:
    return recompute_daily_state(
        store,
        codes=codes,
        status_mode="verified_no_fetch",
    )


def test_backfill_writes_market_tables_then_status(db_path: Path) -> None:
    adapter = _FakeMarketAdapter()

    summary = _backfill(db_path, adapter)

    assert summary["trading_dates_count"] == 2
    assert summary["planned_logical_api_operations"] == 10
    assert summary["planned_requests"] == 10
    assert summary["planned_requests_is_upper_bound"] is True
    assert summary["planned_request_breakdown"] == {
        "trade_cal": 1,
        "daily_daily_basic_adj_factor": 6,
        "namechange_windows_upper_bound": 1,
        "stock_st_dates_upper_bound": 2,
    }
    assert summary["attempted_logical_api_operations"] == 10
    assert summary["completed_logical_api_operations"] == 10
    assert summary["executed_requests"] == 10
    assert summary["request_count_semantics"] == (
        "logical_adapter_operations; internal retries are not observable or countable"
    )
    assert summary["executed_dates"] == 2
    assert summary["failed_dates"] == []
    assert summary["daily_rows"] == 2
    assert summary["daily_basic_rows"] == 2
    assert summary["adj_factor_rows"] == 2
    assert summary["security_status_rows"] == 2
    assert adapter.daily_calls == [date(2020, 1, 2), date(2020, 1, 3)]
    assert adapter.stock_st_calls == [date(2020, 1, 2), date(2020, 1, 3)]
    with DuckDBStore(db_path, read_only=True) as store:
        assert store.count_daily("600000.SH") == 2
        assert store.count_daily_basic("600000.SH") == 2
        assert store.count_adj_factor("600000.SH") == 2
        assert len(store.list_stock_status(date(2020, 1, 1), date(2020, 1, 5))) == 2


def test_backfill_status_resume_is_missing_only(db_path: Path) -> None:
    adapter = _FakeMarketAdapter()

    first = _backfill(db_path, adapter)
    namechange_count = len(adapter.namechange_calls)
    stock_st_count = len(adapter.stock_st_calls)
    second = _backfill(db_path, adapter)

    assert first["security_status_rows"] == 2
    assert second["security_status_rows"] == 0
    assert len(adapter.namechange_calls) == namechange_count + 1
    assert len(adapter.stock_st_calls) == stock_st_count


def test_backfill_dry_run_only_calls_trade_cal(db_path: Path) -> None:
    adapter = _FakeMarketAdapter()

    summary = _backfill(db_path, adapter, dry_run=True)

    assert summary["dry_run"] is True
    assert summary["trading_dates_count"] == 2
    assert summary["planned_logical_api_operations"] == 10
    assert summary["completed_logical_api_operations"] == 1
    assert summary["executed_dates"] == 0
    assert adapter.trade_cal_calls == [(date(2020, 1, 1), date(2020, 1, 5))]
    assert adapter.daily_calls == []
    assert adapter.basic_calls == []
    assert adapter.factor_calls == []
    assert adapter.namechange_calls == []
    assert adapter.stock_st_calls == []
    with DuckDBStore(db_path, read_only=True) as store:
        assert store.count_daily() == 0


def test_backfill_isolates_single_day_failure_and_statuses_successful_days(
    db_path: Path,
) -> None:
    adapter = _FailingDayAdapter()

    summary = _backfill(db_path, adapter)

    assert summary["executed_dates"] == 2
    assert summary["failed_dates"] == ["2020-01-03"]
    assert summary["daily_rows"] == 1
    assert summary["security_status_rows"] == 1
    assert adapter.stock_st_calls == [date(2020, 1, 2)]
    with DuckDBStore(db_path, read_only=True) as store:
        assert store.count_daily("600000.SH") == 1


def test_backfill_rejects_reversed_range(db_path: Path) -> None:
    with pytest.raises(ValueError, match="start_date"):
        backfill_market_daily(
            "2020-01-05",
            "2020-01-01",
            _FakeMarketAdapter(),
            store_factory=_StoreFactory(db_path),
        )


def _seed_market_bar_and_state_tail(db_path: Path) -> tuple[object, list[object]]:
    with DuckDBStore(db_path) as store:
        store._conn.execute(
            """
            INSERT INTO daily_bar (
                ts_code, trade_date, open, high, low, close, pre_close,
                change, pct_chg, vol, amount
            ) VALUES
                ('600000.SH', DATE '2020-01-02', 9, 9, 9, 9, 9, 0, 0, 1, 1),
                ('600000.SH', DATE '2020-01-03', 9, 9, 9, 9, 9, 0, 0, 1, 1)
            """
        )
        store._conn.execute(
            """
            INSERT INTO daily_state (
                ts_code, trade_date, is_st, board_type, limit_pct,
                is_limit_up, is_first_limit_up, consecutive_limit_ups,
                body_upper, body_lower
            ) VALUES
                ('600000.SH', DATE '2020-01-02', FALSE, 'main', 0.1,
                 FALSE, FALSE, 0, 9, 9),
                ('600000.SH', DATE '2020-01-03', FALSE, 'main', 0.1,
                 TRUE, TRUE, 1, 9, 9)
            """
        )
        bar = store._conn.execute(
            "SELECT * FROM daily_bar WHERE ts_code = ? AND trade_date = ?",
            ["600000.SH", date(2020, 1, 2)],
        ).fetchone()
        states = store._conn.execute(
            "SELECT * FROM daily_state WHERE ts_code = ? ORDER BY trade_date",
            ["600000.SH"],
        ).fetchall()
    return bar, states


def test_historical_status_failure_changes_neither_bar_nor_state_tail(
    db_path: Path,
) -> None:
    old_bar, old_states = _seed_market_bar_and_state_tail(db_path)

    summary = _backfill(db_path, _FailingStockSTAdapter())

    assert summary["failed_dates"] == ["2020-01-02", "2020-01-03"]
    with DuckDBStore(db_path, read_only=True) as store:
        bar = store._conn.execute(
            "SELECT * FROM daily_bar WHERE ts_code = ? AND trade_date = ?",
            ["600000.SH", date(2020, 1, 2)],
        ).fetchone()
        states = store._conn.execute(
            "SELECT * FROM daily_state WHERE ts_code = ? ORDER BY trade_date",
            ["600000.SH"],
        ).fetchall()
    assert bar == old_bar
    assert states == old_states


def test_historical_apply_failure_rolls_back_status_bar_and_state(
    db_path: Path,
) -> None:
    old_status = SecurityStatusDaily(
        ts_code="600000.SH",
        trade_date=date(2020, 1, 2),
        name=None,
        is_st=None,
        name_source="unknown",
        st_source=None,
        available_at=None,
        ingested_at=INGESTED_AT - pd.Timedelta(seconds=1),
    )
    with DuckDBStore(db_path) as store:
        store._conn.execute(
            """
            INSERT INTO daily_bar (
                ts_code, trade_date, open, high, low, close, pre_close
            ) VALUES ('600000.SH', DATE '2020-01-02', 9, 9, 9, 9, 9)
            """
        )
        store._conn.execute(
            """
            INSERT INTO daily_state (
                ts_code, trade_date, is_limit_up, is_first_limit_up,
                consecutive_limit_ups
            ) VALUES ('600000.SH', DATE '2020-01-02', FALSE, FALSE, 0)
            """
        )
        store.upsert_stock_status((old_status,))
        old_bar = store._conn.execute(
            "SELECT * FROM daily_bar WHERE ts_code = ? AND trade_date = ?",
            ["600000.SH", date(2020, 1, 2)],
        ).fetchone()
        old_state = store._conn.execute(
            "SELECT * FROM daily_state WHERE ts_code = ? AND trade_date = ?",
            ["600000.SH", date(2020, 1, 2)],
        ).fetchone()

    class _FailingStore(DuckDBStore):
        def upsert_adj_factor(self, frame: pd.DataFrame) -> int:
            rows = super().upsert_adj_factor(frame)
            raise RuntimeError(f"factor write failed after {rows} rows")

    summary = _backfill(
        db_path,
        _OneDayAdapter(),
        store_factory=_StoreFactory(db_path, _FailingStore),
    )

    assert summary["failed_dates"] == ["2020-01-02"]
    with DuckDBStore(db_path, read_only=True) as store:
        status = store.list_stock_status(date(2020, 1, 2), date(2020, 1, 2))
        bar = store._conn.execute(
            "SELECT * FROM daily_bar WHERE ts_code = ? AND trade_date = ?",
            ["600000.SH", date(2020, 1, 2)],
        ).fetchone()
        state = store._conn.execute(
            "SELECT * FROM daily_state WHERE ts_code = ? AND trade_date = ?",
            ["600000.SH", date(2020, 1, 2)],
        ).fetchone()
    assert status == [old_status]
    assert bar == old_bar
    assert state == old_state


def test_historical_successful_update_invalidates_state_tail_without_rewrite(
    db_path: Path,
) -> None:
    _seed_market_bar_and_state_tail(db_path)

    class _RejectStateWriteStore(DuckDBStore):
        def upsert_state(self, frame: pd.DataFrame) -> int:
            del frame
            raise AssertionError("per-date apply must not rewrite daily_state")

    summary = _backfill(
        db_path,
        _OneDayAdapter(),
        store_factory=_StoreFactory(db_path, _RejectStateWriteStore),
    )

    assert summary["failed_dates"] == []
    with DuckDBStore(db_path, read_only=True) as store:
        close = store._conn.execute(
            "SELECT close FROM daily_bar WHERE ts_code = ? AND trade_date = ?",
            ["600000.SH", date(2020, 1, 2)],
        ).fetchone()[0]
        states = store._conn.execute(
            """
            SELECT trade_date, is_limit_up FROM daily_state
            WHERE ts_code = ? AND trade_date >= ?
            ORDER BY trade_date
            """,
            ["600000.SH", date(2020, 1, 2)],
        ).fetchall()
    assert close == pytest.approx(10.2)
    assert states == []


def test_basic_and_factor_only_rows_do_not_invalidate_state_tail(
    db_path: Path,
) -> None:
    complete = SecurityStatusDaily(
        ts_code="600000.SH",
        trade_date=date(2020, 1, 2),
        name="浦发银行",
        is_st=False,
        name_source="tushare.namechange",
        st_source="tushare.namechange",
        available_at=datetime(2020, 1, 2, 9, 25, tzinfo=SHANGHAI),
        ingested_at=INGESTED_AT,
    )
    with DuckDBStore(db_path) as store:
        store._conn.execute(
            """
            INSERT INTO daily_bar (ts_code, trade_date, close)
            VALUES ('600000.SH', DATE '2020-01-02', 9)
            """
        )
        store._conn.execute(
            """
            INSERT INTO daily_state (
                ts_code, trade_date, is_limit_up, consecutive_limit_ups
            ) VALUES ('600000.SH', DATE '2020-01-02', FALSE, 0)
            """
        )
        store.upsert_stock_status((complete,))

    summary = _backfill(db_path, _FactOnlyAdapter())

    assert summary["failed_dates"] == []
    assert summary["daily_rows"] == 0
    assert summary["daily_basic_rows"] == 1
    assert summary["adj_factor_rows"] == 1
    assert summary["affected_codes"] == []
    with DuckDBStore(db_path, read_only=True) as store:
        state_dates = store._conn.execute(
            "SELECT trade_date FROM daily_state ORDER BY trade_date"
        ).fetchall()
    assert state_dates == [(date(2020, 1, 2),)]


def test_request_plan_includes_existing_incomplete_date_when_calendar_empty(
    db_path: Path,
) -> None:
    with DuckDBStore(db_path) as store:
        store._conn.execute(
            """
            INSERT INTO daily_bar (ts_code, trade_date, close)
            VALUES ('600000.SH', DATE '2020-01-02', 9)
            """
        )

    summary = _backfill(db_path, _EmptyCalendarAdapter(), dry_run=True)

    assert summary["calendar_trading_dates_count"] == 0
    assert summary["existing_incomplete_status_dates_count"] == 1
    assert summary["trading_dates_count"] == 1
    assert summary["planned_logical_api_operations"] == 6


def test_market_noop_skips_status_prefetch_and_counts_only_trade_calendar(
    db_path: Path,
) -> None:
    adapter = _EmptyCalendarAdapter()

    summary = _backfill(db_path, adapter)

    assert summary["trading_dates_count"] == 0
    assert summary["attempted_logical_api_operations"] == 1
    assert summary["completed_logical_api_operations"] == 1
    assert adapter.daily_calls == []
    assert adapter.namechange_calls == []
    assert adapter.stock_st_calls == []


def test_market_filters_and_reports_out_of_range_calendar_dates(
    db_path: Path,
) -> None:
    adapter = _OutOfRangeCalendarAdapter()

    summary = _backfill(db_path, adapter)

    assert summary["calendar_returned_dates_count"] == 3
    assert summary["calendar_trading_dates_count"] == 1
    assert summary["calendar_out_of_range_dates"] == [
        "2019-12-31",
        "2020-01-06",
    ]
    assert adapter.daily_calls == [date(2020, 1, 2)]
    with DuckDBStore(db_path, read_only=True) as store:
        dates = store._conn.execute(
            "SELECT DISTINCT trade_date FROM daily_bar ORDER BY trade_date"
        ).fetchall()
    assert dates == [(date(2020, 1, 2),)]


def test_failed_daily_operation_is_attempted_but_not_completed(
    db_path: Path,
) -> None:
    adapter = _FailingOnlyDailyAdapter()

    summary = _backfill(db_path, adapter)

    assert summary["failed_dates"] == ["2020-01-02"]
    assert summary["attempted_logical_api_operations"] == 3
    assert summary["completed_logical_api_operations"] == 2
    assert summary["executed_requests"] == 3
    assert summary["internal_adapter_retries_observable"] is False
    assert summary["request_count_semantics"] == (
        "logical_adapter_operations; internal retries are not observable or countable"
    )


def test_operation_metrics_do_not_claim_exact_physical_retry_count(
    db_path: Path,
) -> None:
    adapter = _RetryingDailyAdapter()

    summary = _backfill(db_path, adapter)

    assert adapter.physical_daily_attempts == 3
    assert summary["attempted_logical_api_operations"] == 6
    assert summary["completed_logical_api_operations"] == 6
    assert summary["executed_requests"] == 6
    assert summary["request_count_semantics"] == (
        "logical_adapter_operations; internal retries are not observable or countable"
    )
    assert "physical_network_requests" not in summary


def test_market_remote_callbacks_run_without_duckdb_writer_lock(
    db_path: Path,
) -> None:
    class _LockProbeAdapter(_OneDayAdapter):
        def _probe(self) -> None:
            with DuckDBStore(db_path, read_only=True):
                pass

        def trade_cal(self, start: date, end: date) -> list[date]:
            self._probe()
            return super().trade_cal(start, end)

        def daily_by_date(self, trade_date: date) -> pd.DataFrame:
            self._probe()
            return super().daily_by_date(trade_date)

        def daily_basic_by_date(self, trade_date: date) -> pd.DataFrame:
            self._probe()
            return super().daily_basic_by_date(trade_date)

        def adj_factor_by_date(self, trade_date: date) -> pd.DataFrame:
            self._probe()
            return super().adj_factor_by_date(trade_date)

        def namechange_raw(
            self,
            start_date: date,
            end_date: date,
            ts_code: str | None = None,
        ) -> pd.DataFrame:
            self._probe()
            return super().namechange_raw(start_date, end_date, ts_code)

        def stock_st_raw(self, trade_date: date) -> pd.DataFrame:
            self._probe()
            return super().stock_st_raw(trade_date)

    summary = _backfill(db_path, _LockProbeAdapter())

    assert summary["failed_dates"] == []


def test_market_missing_only_preserves_complete_peer_byte_for_byte(
    db_path: Path,
) -> None:
    complete = SecurityStatusDaily(
        ts_code="600000.SH",
        trade_date=date(2020, 1, 2),
        name="浦发银行",
        is_st=False,
        name_source="tushare.namechange",
        st_source="tushare.namechange",
        available_at=datetime(2020, 1, 2, 9, 25, tzinfo=SHANGHAI),
        ingested_at=INGESTED_AT,
    )
    with DuckDBStore(db_path) as store:
        store._conn.execute(
            """
            INSERT INTO daily_bar (ts_code, trade_date, close) VALUES
                ('600000.SH', DATE '2020-01-02', 9),
                ('000001.SZ', DATE '2020-01-02', 5)
            """
        )
        store.upsert_stock_status((complete,))

    summary = _backfill(db_path, _TwoCodeAdapter())

    assert summary["security_status_rows"] == 1
    with DuckDBStore(db_path, read_only=True) as store:
        rows = store.list_stock_status(date(2020, 1, 2), date(2020, 1, 2))
    by_code = {row.ts_code: row for row in rows}
    assert by_code["600000.SH"] == complete
    assert by_code["000001.SZ"].is_st is False


def _seed_daily_history(
    store: DuckDBStore,
    *,
    status_kind: str = "known",
) -> None:
    store._conn.execute(
        """
        INSERT INTO trade_calendar (
            exchange, cal_date, is_open, pretrade_date, source, updated_at
        ) VALUES
            ('SSE', DATE '2020-01-02', TRUE, DATE '2019-12-31',
             'test', TIMESTAMPTZ '2020-01-05 08:00:00+00'),
            ('SSE', DATE '2020-01-03', TRUE, DATE '2020-01-02',
             'test', TIMESTAMPTZ '2020-01-05 08:00:00+00')
        """
    )
    store.upsert_daily(
        pd.DataFrame(
            [
                {
                    "ts_code": "600000.SH",
                    "trade_date": date(2020, 1, 2),
                    "open": 10.0,
                    "high": 10.1,
                    "low": 9.9,
                    "close": 10.0,
                    "pre_close": 10.0,
                    "change": 0.0,
                    "pct_chg": 0.0,
                    "vol": 1.0,
                    "amount": 1.0,
                },
                {
                    "ts_code": "600000.SH",
                    "trade_date": date(2020, 1, 3),
                    "open": 10.5,
                    "high": 11.0,
                    "low": 10.4,
                    "close": 11.0,
                    "pre_close": 10.0,
                    "change": 1.0,
                    "pct_chg": 10.0,
                    "vol": 1.0,
                    "amount": 1.0,
                },
                {
                    "ts_code": "000001.SZ",
                    "trade_date": date(2020, 1, 2),
                    "open": 5.0,
                    "high": 5.1,
                    "low": 4.9,
                    "close": 5.0,
                    "pre_close": 5.0,
                    "change": 0.0,
                    "pct_chg": 0.0,
                    "vol": 1.0,
                    "amount": 1.0,
                },
            ]
        )
    )
    store.upsert_stock_basic(
        pd.DataFrame(
            [
                {
                    "ts_code": "600000.SH",
                    "symbol": "600000",
                    "name": "*ST当前浦发",
                    "area": "上海",
                    "industry": "银行",
                    "list_date": "19991110",
                    "market": "主板",
                }
            ]
        )
    )

    if status_kind == "missing":
        return
    if status_kind not in {"known", "unknown"}:
        raise ValueError(f"unsupported status kind: {status_kind}")
    known = status_kind == "known"
    rows = [
        SecurityStatusDaily(
            ts_code=code,
            trade_date=trade_date,
            name="测试证券" if known else None,
            is_st=False if known else None,
            name_source="tushare.namechange" if known else "unknown",
            st_source="tushare.namechange" if known else None,
            available_at=(
                datetime.combine(
                    trade_date,
                    datetime.min.time(),
                    tzinfo=SHANGHAI,
                ).replace(hour=9, minute=25)
                if known
                else None
            ),
            ingested_at=INGESTED_AT,
        )
        for code, trade_date in (
            ("600000.SH", date(2020, 1, 2)),
            ("600000.SH", date(2020, 1, 3)),
            ("000001.SZ", date(2020, 1, 2)),
        )
    ]
    store.upsert_stock_status(rows)


def test_recompute_daily_state_uses_verified_status_and_ignores_current_name(
    db_path: Path,
) -> None:
    with DuckDBStore(db_path) as store:
        _seed_daily_history(store)
        total = _recompute(store)

        assert total == 3
        state = store.get_state("600000.SH")
        assert len(state) == 2
        limit_day = state.iloc[1]
        assert limit_day["is_st"] == False  # noqa: E712
        assert limit_day["limit_pct"] == pytest.approx(0.10)
        assert bool(limit_day["is_limit_up"])
        assert bool(limit_day["is_first_limit_up"])
        assert int(limit_day["consecutive_limit_ups"]) == 1
        assert store.count_state("000001.SZ") == 1


def test_recompute_daily_state_with_codes_subset_uses_exact_parameter(
    db_path: Path,
) -> None:
    with DuckDBStore(db_path) as store:
        _seed_daily_history(store)
        total = _recompute(store, codes=["000001.SZ"])

        assert total == 1
        assert store.count_state("000001.SZ") == 1
        assert store.count_state("600000.SH") == 0
        stored_status = store.list_stock_status(date(2020, 1, 2), date(2020, 1, 3))
        assert [row.ts_code for row in stored_status] == [
            "000001.SZ",
            "600000.SH",
            "600000.SH",
        ]


def test_recompute_does_not_interpolate_code_into_sql(db_path: Path) -> None:
    with DuckDBStore(db_path) as store:
        _seed_daily_history(store)
        total = _recompute(
            store,
            codes=["600000.SH' OR 1=1 --"],
        )

        assert total == 0
        assert store.count_state() == 0


def test_recompute_persists_unknown_status_as_duckdb_nulls(
    db_path: Path,
) -> None:
    with DuckDBStore(db_path) as store:
        _seed_daily_history(store, status_kind="unknown")
        total = _recompute(store)

        assert total == 3
        state = store.get_state("600000.SH")
        nullable_columns = [
            "is_st",
            "limit_pct",
            "limit_up_price",
            "limit_down_price",
            "is_limit_up",
            "is_limit_down",
            "is_first_limit_up",
            "is_yiziban",
            "consecutive_limit_ups",
        ]
        assert state[nullable_columns].isna().all().all()
        assert state["board_type"].tolist() == ["main", "main"]
        assert state["body_upper"].notna().all()


def test_recompute_missing_verified_status_fails_closed(db_path: Path) -> None:
    with DuckDBStore(db_path) as store:
        _seed_daily_history(store, status_kind="missing")

        with pytest.raises(RuntimeError, match="verified status coverage"):
            _recompute(store)

        assert store.count_state() == 0
