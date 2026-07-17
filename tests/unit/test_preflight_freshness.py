"""Contract-driven freshness checks for preflight."""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from rquant.data_contracts import (
    CONTRACTS_BY_ID,
    EXCHANGE_TIMEZONE,
    DatasetContract,
    FreshnessRule,
    PriceBasis,
    VisibilityRule,
)
from rquant.preflight import (
    PRODUCTION_FRESHNESS_DATASET_IDS,
    RESEARCH_FRESHNESS_DATASET_IDS,
    check_data_freshness,
)
from rquant.trade_calendar import TradeCalendarDay


class _Cursor:
    def __init__(self, row: tuple[object, ...]) -> None:
        self._row = row

    def fetchone(self) -> tuple[object, ...]:
        return self._row


class _Connection:
    def __init__(
        self,
        watermarks: dict[str, tuple[object, int]],
        visible_watermarks: dict[str, tuple[object, int]] | None = None,
    ) -> None:
        self._watermarks = watermarks
        self._visible_watermarks = visible_watermarks or {}
        self.queries: list[tuple[str, object]] = []

    def execute(self, query: str, params: object = None, **__: object) -> _Cursor:
        table = query.rsplit("FROM", 1)[1].strip().split()[0]
        self.queries.append((query, params))
        if "WHERE" in query and table in self._visible_watermarks:
            return _Cursor(self._visible_watermarks[table])
        return _Cursor(self._watermarks[table])


class _ReadonlyStore:
    def __init__(
        self,
        watermarks: dict[str, tuple[object, int]],
        calendar: tuple[TradeCalendarDay, ...],
        visible_watermarks: dict[str, tuple[object, int]] | None = None,
    ) -> None:
        self._conn = _Connection(watermarks, visible_watermarks)
        self._calendar = calendar

    def __enter__(self) -> _ReadonlyStore:
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def is_trading_day(self, exchange: str, cal_date: date) -> bool:
        row = next(
            item
            for item in self._calendar
            if item.exchange == exchange and item.cal_date == cal_date
        )
        return row.is_open

    def latest_trading_day(self, anchor: date, *, exchange: str = "SSE") -> date:
        return max(
            item.cal_date
            for item in self._calendar
            if item.exchange == exchange and item.is_open and item.cal_date <= anchor
        )

    def previous_trading_day(self, anchor: date, *, exchange: str = "SSE") -> date:
        return max(
            item.cal_date
            for item in self._calendar
            if item.exchange == exchange and item.is_open and item.cal_date < anchor
        )

    def list_trade_calendar(
        self, exchange: str, start: date, end: date
    ) -> list[TradeCalendarDay]:
        return [
            item
            for item in self._calendar
            if item.exchange == exchange and start <= item.cal_date <= end
        ]


def _calendar() -> tuple[TradeCalendarDay, ...]:
    return tuple(
        TradeCalendarDay(
            exchange="SSE",
            cal_date=day,
            is_open=is_open,
            pretrade_date=date(2026, 7, 9),
        )
        for day, is_open in (
            (date(2026, 7, 10), True),
            (date(2026, 7, 11), False),
            (date(2026, 7, 12), False),
            (date(2026, 7, 13), True),
        )
    )


def _patch_store(
    monkeypatch: Any,
    watermarks: dict[str, tuple[object, int]],
    *,
    visible_watermarks: dict[str, tuple[object, int]] | None = None,
) -> None:
    store = _ReadonlyStore(watermarks, _calendar(), visible_watermarks)
    monkeypatch.setattr(
        "rquant.storage.duckdb.open_readonly_store",
        lambda **_: store,
    )


def test_suspension_coverage_is_a_required_production_daily_contract() -> None:
    contract = CONTRACTS_BY_ID["stock_suspend_coverage"]

    assert "stock_suspend_coverage" in PRODUCTION_FRESHNESS_DATASET_IDS
    assert contract.freshness.watermark_column == "trade_date"
    assert contract.freshness.max_trading_session_lag == 0
    assert contract.freshness.required_on_open_day is True


def test_auction_freshness_belongs_to_research_not_production() -> None:
    assert "auction_bar" not in PRODUCTION_FRESHNESS_DATASET_IDS
    assert "auction_bar" in RESEARCH_FRESHNESS_DATASET_IDS


def test_trading_session_lag_ignores_weekend_calendar_days(monkeypatch: Any) -> None:
    _patch_store(monkeypatch, {"daily_bar": (date(2026, 7, 10), 123)})

    result = check_data_freshness(
        (CONTRACTS_BY_ID["daily_bar"],),
        as_of=datetime(2026, 7, 13, 8, 30, tzinfo=EXCHANGE_TIMEZONE),
        replica_path=None,
    )

    assert result.status == "ok"
    assert any("0 个交易日" in line for line in result.details)


def test_panel_dataset_requires_previous_session_during_current_session(
    monkeypatch: Any,
) -> None:
    _patch_store(
        monkeypatch,
        {"stock_suspend_coverage": (date(2026, 7, 10), 1)},
    )

    result = check_data_freshness(
        (CONTRACTS_BY_ID["stock_suspend_coverage"],),
        as_of=datetime(2026, 7, 13, 14, 0, tzinfo=EXCHANGE_TIMEZONE),
        replica_path=None,
    )

    assert result.status == "ok"
    assert any("0 个交易日" in line for line in result.details)


def test_panel_freshness_ignores_same_day_rows_until_next_session(
    monkeypatch: Any,
) -> None:
    store = _ReadonlyStore(
        {"daily_bar": (date(2026, 7, 13), 200)},
        _calendar(),
        {"daily_bar": (date(2026, 7, 10), 100)},
    )
    monkeypatch.setattr(
        "rquant.storage.duckdb.open_readonly_store",
        lambda **_: store,
    )

    result = check_data_freshness(
        (CONTRACTS_BY_ID["daily_bar"],),
        as_of=datetime(2026, 7, 13, 18, 0, tzinfo=EXCHANGE_TIMEZONE),
        replica_path=None,
    )

    assert result.status == "ok"
    query, params = store._conn.queries[0]
    assert "WHERE trade_date < ?" in query
    assert params == [date(2026, 7, 13)]
    assert any("latest=2026-07-10" in line for line in result.details)


def test_wall_clock_lag_fails_required_intraday_dataset(monkeypatch: Any) -> None:
    _patch_store(
        monkeypatch,
        {"minute_bar": (datetime(2026, 7, 13, 10, 1), 123)},
    )

    result = check_data_freshness(
        (CONTRACTS_BY_ID["minute_bar"],),
        as_of=datetime(2026, 7, 13, 10, 7, tzinfo=EXCHANGE_TIMEZONE),
        replica_path=None,
    )

    assert result.status == "fail"
    assert any("6 分钟" in line and "阈值 5 分钟" in line for line in result.details)


def test_wall_clock_lag_clips_expected_time_during_lunch(monkeypatch: Any) -> None:
    _patch_store(
        monkeypatch,
        {"minute_bar": (datetime(2026, 7, 13, 11, 29), 123)},
    )

    result = check_data_freshness(
        (CONTRACTS_BY_ID["minute_bar"],),
        as_of=datetime(2026, 7, 13, 12, 30, tzinfo=EXCHANGE_TIMEZONE),
        replica_path=None,
    )

    assert result.status == "ok"
    assert any("1 分钟" in line for line in result.details)


def test_unknown_static_freshness_is_explicitly_not_blocking(monkeypatch: Any) -> None:
    contract = DatasetContract(
        dataset_id="static_snapshot",
        table_name="static_snapshot",
        sources=("test",),
        physical_primary_key=("id",),
        logical_key=("id",),
        price_basis=PriceBasis.NOT_APPLICABLE,
        visibility=VisibilityRule.UNKNOWN,
        freshness=FreshnessRule(
            watermark_column="updated_at",
            required_on_open_day=False,
        ),
        historized=False,
    )
    _patch_store(monkeypatch, {"static_snapshot": (datetime(2020, 1, 1), 3)})

    result = check_data_freshness(
        (contract,),
        as_of=datetime(2026, 7, 13, 10, 7, tzinfo=EXCHANGE_TIMEZONE),
        replica_path=None,
    )

    assert result.status == "warn"
    assert any("契约未声明时效阈值" in line for line in result.details)


def test_required_empty_table_fails_and_optional_empty_table_warns(monkeypatch: Any) -> None:
    required = CONTRACTS_BY_ID["daily_bar"]
    optional = CONTRACTS_BY_ID["stock_status_daily"]
    _patch_store(
        monkeypatch,
        {"daily_bar": (None, 0), "stock_status_daily": (None, 0)},
    )

    required_result = check_data_freshness(
        (required,),
        as_of=datetime(2026, 7, 13, 10, 7, tzinfo=EXCHANGE_TIMEZONE),
        replica_path=None,
    )
    optional_result = check_data_freshness(
        (optional,),
        as_of=datetime(2026, 7, 13, 10, 7, tzinfo=EXCHANGE_TIMEZONE),
        replica_path=None,
    )

    assert required_result.status == "fail"
    assert optional_result.status == "warn"


def test_stale_readonly_replica_is_reported(monkeypatch: Any, tmp_path: Path) -> None:
    _patch_store(monkeypatch, {"daily_bar": (date(2026, 7, 10), 123)})
    replica = tmp_path / "rquant_ro.duckdb"
    primary = tmp_path / "rquant.duckdb"
    replica.touch()
    primary.touch()
    stale = datetime(2026, 7, 13, 9, 50, tzinfo=EXCHANGE_TIMEZONE)
    current = datetime(2026, 7, 13, 10, 7, tzinfo=EXCHANGE_TIMEZONE)
    os.utime(replica, (stale.timestamp(), stale.timestamp()))
    os.utime(primary, (current.timestamp(), current.timestamp()))

    result = check_data_freshness(
        (CONTRACTS_BY_ID["daily_bar"],),
        as_of=datetime(2026, 7, 13, 10, 7, tzinfo=EXCHANGE_TIMEZONE),
        replica_path=replica,
        primary_path=primary,
        replica_max_source_lag=timedelta(minutes=10),
    )

    assert result.status == "fail"
    assert any("落后主库工件 17 分钟" in line for line in result.details)


def test_closed_day_watermark_fails_closed(monkeypatch: Any) -> None:
    _patch_store(monkeypatch, {"daily_bar": (date(2026, 7, 12), 123)})

    result = check_data_freshness(
        (CONTRACTS_BY_ID["daily_bar"],),
        as_of=datetime(2026, 7, 13, 10, 7, tzinfo=EXCHANGE_TIMEZONE),
        replica_path=None,
    )

    assert result.status == "fail"
    assert any("落在非交易日" in line for line in result.details)


def test_minute_opening_grace_accepts_previous_close(monkeypatch: Any) -> None:
    _patch_store(
        monkeypatch,
        {"minute_bar": (datetime(2026, 7, 10, 15, 0), 123)},
    )

    result = check_data_freshness(
        (CONTRACTS_BY_ID["minute_bar"],),
        as_of=datetime(2026, 7, 13, 9, 32, tzinfo=EXCHANGE_TIMEZONE),
        replica_path=None,
    )

    assert result.status == "ok"
