"""Tushare 涨跌停榜（limit_list_d）回补 / 当日增量 / 存储层测试。"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from rquant.adapter.tushare import TushareAdapter
from rquant.limit_list_backfill import backfill_limit_list, capture_today
from rquant.storage.duckdb import DuckDBStore


def _limit_row(trade_date: date, ts_code: str, status: str) -> dict:
    return {
        "ts_code": ts_code,
        "trade_date": trade_date,
        "name": "测试股",
        "industry": "银行",
        "close": 10.2,
        "pct_chg": 10.0,
        "amount": 1.0e8,
        "limit_amount": None if status == "U" else 5.0e7,
        "float_mv": 5.0e9,
        "total_mv": 8.0e9,
        "turnover_ratio": 3.2,
        "fd_amount": 2.0e8,
        "first_time": "093100",
        "last_time": "093100",
        "open_times": 0,
        "up_stat": "1/1",
        "limit_times": 1,
        "limit": status,
    }


class _FakeLimitListAdapter:
    def __init__(self) -> None:
        self.trade_cal_calls: list[tuple[date, date]] = []
        self.limit_list_calls: list[tuple[date, str]] = []

    def trade_cal(self, start: date, end: date) -> list[date]:
        self.trade_cal_calls.append((start, end))
        all_dates = [date(2020, 1, 2), date(2020, 1, 3)]
        return [d for d in all_dates if start <= d <= end]

    def limit_list_by_date(
        self, trade_date: date, limit_type: str = ""
    ) -> pd.DataFrame:
        self.limit_list_calls.append((trade_date, limit_type))
        return pd.DataFrame([
            _limit_row(trade_date, "600000.SH", "U"),
            _limit_row(trade_date, "000001.SZ", "Z"),
        ])


class _FailingDayAdapter(_FakeLimitListAdapter):
    def limit_list_by_date(
        self, trade_date: date, limit_type: str = ""
    ) -> pd.DataFrame:
        if trade_date == date(2020, 1, 3):
            self.limit_list_calls.append((trade_date, limit_type))
            raise RuntimeError("rate limit")
        return super().limit_list_by_date(trade_date, limit_type)


class _EmptyAdapter(_FakeLimitListAdapter):
    def limit_list_by_date(
        self, trade_date: date, limit_type: str = ""
    ) -> pd.DataFrame:
        self.limit_list_calls.append((trade_date, limit_type))
        return pd.DataFrame()


class _RaisingAdapter(_FakeLimitListAdapter):
    def limit_list_by_date(
        self, trade_date: date, limit_type: str = ""
    ) -> pd.DataFrame:
        raise RuntimeError("Tushare limit_list_d 调用失败：权限不足")


@pytest.fixture()
def store(tmp_path: Path) -> Iterator[DuckDBStore]:
    s = DuckDBStore(tmp_path / "test.duckdb")
    yield s
    s.close()


# ── backfill_limit_list ──


def test_backfill_writes_rows_and_renames_limit_status(store: DuckDBStore) -> None:
    adapter = _FakeLimitListAdapter()

    summary = backfill_limit_list(
        "2020-01-01", "2020-01-05", store, adapter, api_sleep=0.0
    )

    assert summary["trading_dates_count"] == 2
    assert summary["planned_requests"] == 2
    assert summary["executed_dates"] == 2
    assert summary["failed_dates"] == []
    assert summary["rows"] == 4
    assert adapter.limit_list_calls == [
        (date(2020, 1, 2), ""),
        (date(2020, 1, 3), ""),
    ]

    df = store.query_limit_list(start=date(2020, 1, 1), end=date(2020, 1, 5))
    assert len(df) == 4
    assert set(df["limit_status"]) == {"U", "Z"}
    assert "limit" not in df.columns


def test_backfill_dry_run_only_calls_trade_cal(store: DuckDBStore) -> None:
    adapter = _FakeLimitListAdapter()

    summary = backfill_limit_list(
        "2020-01-01", "2020-01-05", store, adapter, dry_run=True
    )

    assert summary["dry_run"] is True
    assert summary["trading_dates_count"] == 2
    assert summary["planned_requests"] == 2
    assert summary["executed_dates"] == 0
    assert adapter.trade_cal_calls == [(date(2020, 1, 1), date(2020, 1, 5))]
    assert adapter.limit_list_calls == []
    assert store.query_limit_list(start="2020-01-01", end="2020-01-05").empty


def test_backfill_isolates_single_day_failure(store: DuckDBStore) -> None:
    adapter = _FailingDayAdapter()

    summary = backfill_limit_list(
        "2020-01-01", "2020-01-05", store, adapter, api_sleep=0.0
    )

    assert summary["executed_dates"] == 2
    assert summary["failed_dates"] == ["2020-01-03"]
    assert summary["rows"] == 2
    assert len(store.query_limit_list(date(2020, 1, 2))) == 2
    assert store.query_limit_list(date(2020, 1, 3)).empty


def test_backfill_rejects_reversed_range(store: DuckDBStore) -> None:
    with pytest.raises(ValueError, match="start_date"):
        backfill_limit_list(
            "2020-01-05", "2020-01-01", store, _FakeLimitListAdapter()
        )


def test_backfill_idempotent(store: DuckDBStore) -> None:
    adapter = _FakeLimitListAdapter()
    backfill_limit_list("2020-01-01", "2020-01-05", store, adapter, api_sleep=0.0)
    backfill_limit_list("2020-01-01", "2020-01-05", store, adapter, api_sleep=0.0)

    df = store.query_limit_list(start="2020-01-01", end="2020-01-05")
    assert len(df) == 4


# ── capture_today ──


def test_capture_today_writes_rows(store: DuckDBStore) -> None:
    adapter = _FakeLimitListAdapter()

    rows = capture_today(store, adapter)

    assert rows == 2
    assert adapter.limit_list_calls == [(date.today(), "")]
    assert len(store.query_limit_list(date.today())) == 2


def test_capture_today_adapter_error_returns_zero(store: DuckDBStore) -> None:
    assert capture_today(store, _RaisingAdapter()) == 0
    assert store.query_limit_list(date.today()).empty


def test_capture_today_empty_returns_zero(store: DuckDBStore) -> None:
    assert capture_today(store, _EmptyAdapter()) == 0


# ── DuckDBStore.query_limit_list ──


def test_query_limit_list_by_status_and_range(store: DuckDBStore) -> None:
    backfill_limit_list(
        "2020-01-01", "2020-01-05", store, _FakeLimitListAdapter(), api_sleep=0.0
    )

    only_u = store.query_limit_list(date(2020, 1, 2), limit_status="U")
    assert len(only_u) == 1
    assert only_u.iloc[0]["ts_code"] == "600000.SH"

    day2_only = store.query_limit_list(start=date(2020, 1, 3), end=date(2020, 1, 3))
    assert len(day2_only) == 2

    with pytest.raises(ValueError, match="trade_date 或 start/end"):
        store.query_limit_list()


def test_upsert_accepts_pre_renamed_limit_status(store: DuckDBStore) -> None:
    row = _limit_row(date(2020, 1, 2), "600000.SH", "U")
    row["limit_status"] = row.pop("limit")
    assert store.upsert_limit_list(pd.DataFrame([row])) == 1
    assert len(store.query_limit_list(date(2020, 1, 2))) == 1


# ── TushareAdapter.limit_list_by_date（stub _pro，不触网） ──


class _StubPro:
    def __init__(
        self, df: pd.DataFrame | None = None, error: Exception | None = None
    ) -> None:
        self._df = df
        self._error = error
        self.calls: list[dict] = []

    def limit_list_d(self, **kwargs: object) -> pd.DataFrame | None:
        self.calls.append(dict(kwargs))
        if self._error is not None:
            raise self._error
        return self._df


def _adapter_with_stub(pro: _StubPro) -> TushareAdapter:
    adapter = TushareAdapter.__new__(TushareAdapter)
    adapter._pro = pro
    return adapter


def _raw_tushare_df() -> pd.DataFrame:
    rows = [
        _limit_row(date(2020, 1, 2), "600000.SH", "U"),
        _limit_row(date(2020, 1, 2), "000001.SZ", "Z"),
    ]
    df = pd.DataFrame(rows)
    df["trade_date"] = "20200102"
    return df


def test_adapter_normalizes_dates_and_passes_limit_type() -> None:
    pro = _StubPro(df=_raw_tushare_df())
    adapter = _adapter_with_stub(pro)

    out = adapter.limit_list_by_date(date(2020, 1, 2))

    assert len(pro.calls) == 1
    assert pro.calls[0]["trade_date"] == "20200102"
    assert pro.calls[0]["limit_type"] == ""
    assert len(out) == 2
    assert all(td == date(2020, 1, 2) for td in out["trade_date"])
    assert list(out["limit"]) == ["U", "Z"]


def test_adapter_forwards_explicit_limit_type() -> None:
    pro = _StubPro(df=_raw_tushare_df())
    adapter = _adapter_with_stub(pro)

    adapter.limit_list_by_date(date(2020, 1, 2), limit_type="U")

    assert pro.calls[0]["limit_type"] == "U"


def test_adapter_empty_returns_empty_frame() -> None:
    adapter = _adapter_with_stub(_StubPro(df=pd.DataFrame()))
    out = adapter.limit_list_by_date(date(2020, 1, 2))
    assert out.empty


def test_adapter_error_raises_runtime_error_without_backup_switch() -> None:
    adapter = _adapter_with_stub(_StubPro(error=Exception("no permission")))
    with pytest.raises(RuntimeError, match="limit_list_d"):
        adapter.limit_list_by_date(date(2020, 1, 2))


def test_adapter_missing_column_raises() -> None:
    df = _raw_tushare_df().drop(columns=["fd_amount"])
    adapter = _adapter_with_stub(_StubPro(df=df))
    with pytest.raises(RuntimeError, match="fd_amount"):
        adapter.limit_list_by_date(date(2020, 1, 2))
