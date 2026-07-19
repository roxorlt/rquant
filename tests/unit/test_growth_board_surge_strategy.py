"""科创/创业板盘中放量追击策略回放测试。"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, date, datetime, time, timedelta

import pandas as pd
import pytest

from rquant.security_status import SHANGHAI, SecurityStatusDaily
from rquant.storage.duckdb import DuckDBStore
from rquant.trade_calendar import TradeCalendarDay


@pytest.fixture()
def store(tmp_path) -> Iterator[DuckDBStore]:
    s = DuckDBStore(tmp_path / "test.duckdb")
    yield s
    s.close()


def _status_row(
    ts_code: str,
    trade_date: date,
    *,
    name: str | None,
    is_st: bool | None,
    available_at: datetime | None = None,
    conflict_reason: str | None = None,
) -> SecurityStatusDaily:
    return SecurityStatusDaily(
        ts_code=ts_code,
        trade_date=trade_date,
        name=name,
        is_st=is_st,
        name_source="test_name" if conflict_reason is None else "conflict",
        st_source="test_st" if is_st is not None else None,
        available_at=available_at,
        ingested_at=datetime(2026, 7, 1, tzinfo=UTC),
        conflict_reason=conflict_reason,
    )


def _known_status(
    ts_code: str,
    trade_date: date,
    *,
    name: str,
    is_st: bool = False,
    hour: int = 9,
    minute: int = 25,
    second: int = 0,
) -> SecurityStatusDaily:
    return _status_row(
        ts_code,
        trade_date,
        name=name,
        is_st=is_st,
        available_at=datetime(
            trade_date.year,
            trade_date.month,
            trade_date.day,
            hour,
            minute,
            second,
            tzinfo=SHANGHAI,
        ),
    )


def _daily_row(ts_code: str, trade_date: date, close: float) -> dict[str, object]:
    return {
        "ts_code": ts_code,
        "trade_date": trade_date,
        "open": close,
        "high": close * 1.02,
        "low": close * 0.98,
        "close": close,
        "pre_close": close / 1.01,
        "change": close - close / 1.01,
        "pct_chg": 1.0,
        "vol": 1000.0,
        "amount": close * 100000.0,
    }


def _state_row(
    ts_code: str,
    trade_date: date,
    *,
    board_type: str,
    limit_up_price: float,
    limit_pct: float = 0.20,
    is_yiziban: bool = False,
) -> dict[str, object]:
    return {
        "ts_code": ts_code,
        "trade_date": trade_date,
        "is_st": False,
        "is_bj": False,
        "board_type": board_type,
        "limit_pct": limit_pct,
        "limit_up_price": limit_up_price,
        "limit_down_price": limit_up_price / 1.5,
        "is_limit_up": False,
        "is_limit_down": False,
        "is_first_limit_up": False,
        "is_yiziban": is_yiziban,
        "consecutive_limit_ups": 0,
        "body_upper": limit_up_price / 1.2,
        "body_lower": limit_up_price / 1.25,
    }


def _indicator_row(
    ts_code: str,
    trade_date: date,
    *,
    bull: bool = True,
) -> dict[str, object]:
    return {
        "ts_code": ts_code,
        "trade_date": trade_date,
        "ma5": 11.0 if bull else 9.0,
        "ma10": 10.5,
        "ma20": 10.0,
        "ma60": 9.5,
        "rsi6": None,
        "rsi14": None,
        "macd": None,
        "macd_signal": None,
        "macd_hist": None,
        "kdj_k": None,
        "kdj_d": None,
        "kdj_j": None,
    }


def _minute_row(
    ts_code: str,
    trade_time: datetime,
    price: float,
    *,
    amount: float,
    low: float | None = None,
    high: float | None = None,
    open_price: float | None = None,
) -> dict[str, object]:
    return {
        "ts_code": ts_code,
        "trade_time": trade_time,
        "freq": "1min",
        "open": open_price if open_price is not None else price,
        "high": high if high is not None else price,
        "low": low if low is not None else price,
        "close": price,
        "vol": amount / price,
        "amount": amount,
        "source": "tushare",
    }


def _seed_open_calendar(store: DuckDBStore, dates: list[date]) -> None:
    previous = dates[0] - timedelta(days=1)
    rows: list[TradeCalendarDay] = []
    for trade_date in dates:
        rows.append(
            TradeCalendarDay(
                exchange="SSE",
                cal_date=trade_date,
                is_open=True,
                pretrade_date=previous,
                source="test",
                updated_at=datetime(2026, 7, 1, tzinfo=UTC),
            )
        )
        previous = trade_date
    store.upsert_trade_calendar(rows)


def _seed_base_market(store: DuckDBStore) -> None:
    dates = [
        date(2026, 6, 22),
        date(2026, 6, 23),
        date(2026, 6, 24),
        date(2026, 6, 25),
        date(2026, 6, 26),
    ]
    _seed_open_calendar(store, dates)
    rows: list[dict[str, object]] = []
    for i, trade_date in enumerate(dates):
        rows.append(_daily_row("300001.SZ", trade_date, 10.0 + i * 0.2))
        rows.append(_daily_row("688001.SH", trade_date, 20.0 + i * 0.2))
        rows.append(_daily_row("600001.SH", trade_date, 30.0 + i * 0.2))
    store.upsert_daily(pd.DataFrame(rows))
    store.upsert_stock_basic(pd.DataFrame([
        {
            "ts_code": "300001.SZ",
            "symbol": "300001",
            "name": "*ST当前创业",
            "area": "深圳",
            "industry": "测试",
            "list_date": "20200101",
            "market": "创业板",
        },
        {
            "ts_code": "688001.SH",
            "symbol": "688001",
            "name": "科创样本",
            "area": "上海",
            "industry": "测试",
            "list_date": "20200101",
            "market": "科创板",
        },
        {
            "ts_code": "600001.SH",
            "symbol": "600001",
            "name": "主板样本",
            "area": "上海",
            "industry": "测试",
            "list_date": "20000101",
            "market": "主板",
        },
    ]))
    store.upsert_stock_status(tuple(
        _known_status(ts_code, trade_date, name=name)
        for trade_date in dates
        for ts_code, name in (
            ("300001.SZ", "历史创业"),
            ("688001.SH", "历史科创"),
            ("600001.SH", "历史主板"),
        )
    ))
    store.upsert_state(pd.DataFrame([
        _state_row("300001.SZ", date(2026, 6, 25), board_type="gem", limit_up_price=13.0),
        _state_row("300001.SZ", date(2026, 6, 26), board_type="gem", limit_up_price=13.2),
        _state_row("688001.SH", date(2026, 6, 25), board_type="star", limit_up_price=25.0),
        _state_row("688001.SH", date(2026, 6, 26), board_type="star", limit_up_price=25.2),
        _state_row("600001.SH", date(2026, 6, 25), board_type="main", limit_up_price=33.0),
        _state_row("600001.SH", date(2026, 6, 26), board_type="main", limit_up_price=33.2),
    ]))
    store.upsert_indicators(pd.DataFrame([
        _indicator_row("300001.SZ", date(2026, 6, 24), bull=True),
        _indicator_row("688001.SH", date(2026, 6, 24), bull=False),
        _indicator_row("600001.SH", date(2026, 6, 24), bull=True),
    ]))


def _seed_volume_surge_minutes(store: DuckDBStore) -> None:
    rows: list[dict[str, object]] = []
    for hist_day in [date(2026, 6, 22), date(2026, 6, 23)]:
        for minute in [30, 31, 32, 33]:
            rows.append(_minute_row(
                "300001.SZ",
                datetime.combine(hist_day, time(9, minute)),
                10.0,
                amount=1000.0,
            ))
    rows.extend([
        _minute_row("300001.SZ", datetime(2026, 6, 25, 9, 30), 10.20, amount=1000.0),
        _minute_row("300001.SZ", datetime(2026, 6, 25, 9, 31), 10.30, amount=1000.0),
        _minute_row("300001.SZ", datetime(2026, 6, 25, 9, 32), 10.45, amount=2000.0),
        _minute_row("300001.SZ", datetime(2026, 6, 25, 9, 33), 10.70, amount=3000.0),
        _minute_row("300001.SZ", datetime(2026, 6, 25, 9, 34), 10.80, amount=1200.0),
        _minute_row("300001.SZ", datetime(2026, 6, 26, 9, 30), 11.30, amount=1000.0),
        _minute_row("300001.SZ", datetime(2026, 6, 26, 15, 0), 11.60, amount=1000.0),
    ])
    store.upsert_minute_bars(pd.DataFrame(rows))


def test_growth_board_surge_replay_uses_intraday_volume_signal(store: DuckDBStore) -> None:
    from rquant.growth_board_surge_strategy import (
        GrowthBoardSurgeConfig,
        run_growth_board_surge_replay,
    )

    _seed_base_market(store)
    _seed_volume_surge_minutes(store)
    signal_date = date(2026, 6, 25)
    store._conn.execute(
        "DELETE FROM stock_status_daily WHERE ts_code = ? AND trade_date = ?",
        ["300001.SZ", signal_date],
    )
    store.upsert_stock_status(
        (
            _known_status(
                "300001.SZ",
                signal_date,
                name="历史创业",
                hour=9,
                minute=33,
            ),
        )
    )
    trades = run_growth_board_surge_replay(
        store,
        start_date=date(2026, 6, 25),
        end_date=date(2026, 6, 25),
        config=GrowthBoardSurgeConfig(
            min_signal_time=time(9, 33),
            lookback_days=2,
            min_hist_days=2,
            max_hold_days=1,
            min_cum_amount_ratio=1.4,
            min_same_minute_amount_ratio=2.0,
        ),
    )

    assert len(trades) == 1
    row = trades.iloc[0]
    assert row["ts_code"] == "300001.SZ"
    assert row["entry_time"] == datetime(2026, 6, 25, 9, 34)
    assert row["entry_signal"] == "growth_board_volume_surge"
    assert row["name"] == "历史创业"
    assert row["signal_rel_cum_amount_asof"] == pytest.approx(1.75)
    assert row["signal_rel_amount_same_minute"] == pytest.approx(3.0)
    assert row["hist_intraday_days"] == 2
    assert row["intraday_order_flow_available"] is False
    assert "外盘/内盘" in row["unsupported_intraday_conditions"]
    assert row["ret_pct"] > 0


def test_growth_board_replay_classifies_opening_structure_once(
    monkeypatch: pytest.MonkeyPatch,
    store: DuckDBStore,
) -> None:
    import rquant.growth_board_surge_strategy as growth

    dates = [
        date(2026, 6, 22),
        date(2026, 6, 23),
        date(2026, 6, 24),
        date(2026, 6, 25),
    ]
    _seed_open_calendar(store, dates)
    classification_calls: list[dict[date, date]] = []
    resolver_calls: list[tuple[date, set[str] | None]] = []

    def classify_once(
        _store: DuckDBStore,
        date_pairs: dict[date, date],
    ) -> tuple[object, ...]:
        classification_calls.append(date_pairs)
        return ()

    def resolve_with_precomputed_structure(
        _store: DuckDBStore,
        trading_date: date,
        _previous_date: date,
        _min_signal_time: time,
        *,
        structural_excluded_codes: set[str] | None = None,
    ) -> list[object]:
        resolver_calls.append((trading_date, structural_excluded_codes))
        return []

    monkeypatch.setattr(
        growth,
        "classify_growth_opening_structure",
        classify_once,
    )
    monkeypatch.setattr(
        growth,
        "resolve_growth_board_candidates",
        resolve_with_precomputed_structure,
    )

    result = growth.run_growth_board_surge_replay(
        store,
        start_date=dates[1],
        end_date=dates[2],
        config=growth.GrowthBoardSurgeConfig(max_hold_days=1),
    )

    assert result.empty
    assert classification_calls == [
        {
            dates[1]: dates[0],
            dates[2]: dates[1],
        }
    ]
    assert resolver_calls == [
        (dates[1], set()),
        (dates[2], set()),
    ]


@pytest.mark.parametrize(
    "status_case",
    ["true", "missing", "adjacent_only", "conflict", "nullable", "future"],
)
def test_growth_board_surge_replay_requires_known_non_st_status(
    store: DuckDBStore,
    status_case: str,
) -> None:
    from rquant.growth_board_surge_strategy import (
        GrowthBoardSurgeConfig,
        run_growth_board_surge_replay,
    )

    _seed_base_market(store)
    _seed_volume_surge_minutes(store)
    signal_date = date(2026, 6, 25)
    if status_case == "missing":
        store._conn.execute(
            "DELETE FROM stock_status_daily WHERE ts_code = ?",
            ["300001.SZ"],
        )
    else:
        store._conn.execute(
            "DELETE FROM stock_status_daily WHERE ts_code = ? AND trade_date = ?",
            ["300001.SZ", signal_date],
        )
        if status_case == "true":
            store.upsert_stock_status((
                _known_status(
                    "300001.SZ",
                    signal_date,
                    name="*ST历史创业",
                    is_st=True,
                    hour=9,
                    minute=33,
                ),
            ))
        elif status_case == "conflict":
            store.upsert_stock_status((
                _status_row(
                    "300001.SZ",
                    signal_date,
                    name=None,
                    is_st=None,
                    conflict_reason="test_conflict",
                ),
            ))
        elif status_case == "nullable":
            store.upsert_stock_status((
                _status_row(
                    "300001.SZ",
                    signal_date,
                    name=None,
                    is_st=None,
                ),
            ))
        elif status_case == "future":
            store.upsert_stock_status((
                _known_status(
                    "300001.SZ",
                    signal_date,
                    name="历史创业",
                    hour=9,
                    minute=33,
                    second=1,
                ),
            ))

    trades = run_growth_board_surge_replay(
        store,
        start_date=signal_date,
        end_date=signal_date,
        config=GrowthBoardSurgeConfig(
            min_signal_time=time(9, 33),
            lookback_days=2,
            min_hist_days=2,
            max_hold_days=1,
            min_cum_amount_ratio=1.4,
            min_same_minute_amount_ratio=2.0,
        ),
    )

    assert trades.empty


def test_growth_board_candidates_use_historical_gem_limit_pct_before_2020_reform(
    store: DuckDBStore,
) -> None:
    from rquant.growth_board_surge_strategy import _query_candidates

    signal_date = date(2020, 8, 21)
    previous_date = date(2020, 8, 20)
    _seed_open_calendar(store, [previous_date, signal_date])
    store.upsert_daily(
        pd.DataFrame(
            [
                _daily_row("300001.SZ", previous_date, 10.0),
                _daily_row("300001.SZ", signal_date, 10.1),
            ]
        )
    )
    store.upsert_stock_basic(
        pd.DataFrame(
            [
                {
                    "ts_code": "300001.SZ",
                    "symbol": "300001",
                    "name": "历史创业板",
                    "area": "深圳",
                    "industry": "测试",
                    "list_date": "20100101",
                    "market": "创业板",
                }
            ]
        )
    )
    store.upsert_indicators(
        pd.DataFrame(
            [
                _indicator_row("300001.SZ", previous_date, bull=True),
            ]
        )
    )
    store.upsert_stock_status(
        (
            _known_status(
                "300001.SZ",
                signal_date,
                name="历史创业板",
                hour=9,
                minute=30,
            ),
        )
    )
    store.upsert_state(
        pd.DataFrame(
            [
                _state_row(
                    "300001.SZ",
                    signal_date,
                    board_type="gem",
                    limit_pct=0.10,
                    limit_up_price=11.0,
                ),
            ]
        )
    )

    candidates = _query_candidates(
        store,
        signal_date,
        previous_date,
        time(9, 30),
    )

    assert len(candidates) == 1
    assert candidates[0].limit_up_price == pytest.approx(11.00)


def test_growth_board_candidates_reject_short_listing_with_stale_ma60(
    store: DuckDBStore,
) -> None:
    from rquant.growth_board_surge_strategy import resolve_growth_board_candidates

    days = [
        date(2026, 6, 15) + timedelta(days=index)
        for index in range(11)
    ]
    previous_date = days[-2]
    signal_date = days[-1]
    _seed_open_calendar(store, days)
    store.upsert_daily(
        pd.DataFrame(
            [
                _daily_row("300001.SZ", trading_date, 10.0 + index * 0.1)
                for index, trading_date in enumerate(days[:-1])
            ]
        )
    )
    store.upsert_stock_basic(
        pd.DataFrame(
            [
                {
                    "ts_code": "300001.SZ",
                    "symbol": "300001",
                    "name": "短上市样本",
                    "area": "深圳",
                    "industry": "测试",
                    "list_date": days[0].strftime("%Y%m%d"),
                    "market": "创业板",
                }
            ]
        )
    )
    store.upsert_indicators(
        pd.DataFrame(
            [_indicator_row("300001.SZ", previous_date, bull=True)]
        )
    )
    store.upsert_stock_status(
        (
            _known_status(
                "300001.SZ",
                signal_date,
                name="短上市样本",
                hour=9,
                minute=25,
            ),
        )
    )

    candidates = resolve_growth_board_candidates(
        store,
        signal_date,
        previous_date,
        time(9, 30),
    )

    assert candidates == []


def test_growth_board_candidates_reject_suspension_input_conflict(
    store: DuckDBStore,
) -> None:
    from rquant.growth_board_surge_strategy import resolve_growth_board_candidates

    _seed_base_market(store)
    signal_date = date(2026, 6, 25)
    previous_date = date(2026, 6, 24)
    store._conn.execute(
        """
        INSERT INTO stock_suspend_coverage
        (source, trade_date, coverage_state, row_count, snapshot_hash, queried_at)
        VALUES ('tushare', ?, 'complete', 1, 'snapshot', ?)
        """,
        [previous_date, datetime(2026, 6, 24, 16, tzinfo=UTC)],
    )
    store._conn.execute(
        """
        INSERT INTO stock_suspend_event
        (source, ts_code, trade_date, suspend_type, suspend_timing,
         session_scope, available_at, ingested_at)
        VALUES
        ('tushare', '300001.SZ', ?, 'S', '09:30-15:00',
         'full_day', ?, ?)
        """,
        [
            previous_date,
            datetime(2026, 6, 24, 8, tzinfo=UTC),
            datetime(2026, 6, 24, 16, tzinfo=UTC),
        ],
    )

    candidates = resolve_growth_board_candidates(
        store,
        signal_date,
        previous_date,
        time(9, 30),
    )

    assert candidates == []


def test_growth_board_candidates_exclude_unsupported_price_limit_state(
    store: DuckDBStore,
) -> None:
    from rquant.growth_board_surge_strategy import _query_candidates

    signal_date = date(2026, 6, 25)
    previous_date = date(2026, 6, 24)
    _seed_open_calendar(store, [previous_date, signal_date])
    store.upsert_daily(
        pd.DataFrame(
            [
                _daily_row("300001.SZ", previous_date, 10.0),
                _daily_row("300001.SZ", signal_date, 10.1),
            ]
        )
    )
    store.upsert_stock_basic(
        pd.DataFrame(
            [
                {
                    "ts_code": "300001.SZ",
                    "symbol": "300001",
                    "name": "上市初期样本",
                    "area": "深圳",
                    "industry": "测试",
                    "list_date": signal_date.strftime("%Y%m%d"),
                    "market": "创业板",
                }
            ]
        )
    )
    store.upsert_indicators(
        pd.DataFrame(
            [
                _indicator_row("300001.SZ", previous_date, bull=True),
            ]
        )
    )
    store.upsert_stock_status((_known_status("300001.SZ", signal_date, name="上市初期样本"),))
    unsupported = _state_row(
        "300001.SZ",
        signal_date,
        board_type="gem",
        limit_up_price=12.0,
    )
    unsupported["limit_pct"] = None
    unsupported["limit_up_price"] = None
    unsupported["is_limit_up"] = None
    unsupported["is_first_limit_up"] = None
    unsupported["is_yiziban"] = None
    unsupported["consecutive_limit_ups"] = None
    store.upsert_state(pd.DataFrame([unsupported]))

    candidates = _query_candidates(
        store,
        signal_date,
        previous_date,
        time(9, 30),
    )

    assert candidates == []


def test_growth_board_candidates_ignore_signal_day_close_and_state(
    store: DuckDBStore,
) -> None:
    from rquant.growth_board_surge_strategy import resolve_growth_board_candidates

    _seed_base_market(store)
    signal_date = date(2026, 6, 25)
    previous_date = date(2026, 6, 24)

    before = resolve_growth_board_candidates(
        store,
        signal_date,
        previous_date,
        time(9, 30),
    )
    store._conn.execute(
        "UPDATE daily_bar SET close = 999, pre_close = 888 "
        "WHERE ts_code = ? AND trade_date = ?",
        ["300001.SZ", signal_date],
    )
    store._conn.execute(
        "DELETE FROM daily_state WHERE ts_code = ? AND trade_date = ?",
        ["300001.SZ", signal_date],
    )
    after = resolve_growth_board_candidates(
        store,
        signal_date,
        previous_date,
        time(9, 30),
    )

    assert before == after
    assert len(after) == 1
    assert after[0].pre_close == pytest.approx(10.4)
    assert after[0].limit_up_price == pytest.approx(12.48)


def test_growth_board_candidates_fail_closed_when_bound_keys_disagree(
    store: DuckDBStore,
) -> None:
    from rquant.growth_board_surge_strategy import resolve_growth_board_candidates

    _seed_base_market(store)
    store._conn.execute(
        """
        CREATE TABLE strategy_eligibility (
            eligibility_id VARCHAR PRIMARY KEY,
            strategy_id VARCHAR NOT NULL,
            strategy_version VARCHAR NOT NULL,
            ts_code VARCHAR NOT NULL,
            eligibility_date DATE NOT NULL,
            entry_date DATE NOT NULL,
            decision_at TIMESTAMPTZ NOT NULL,
            variant VARCHAR NOT NULL,
            resolution_hash VARCHAR NOT NULL
        );
        INSERT INTO strategy_eligibility VALUES (
            'eligibility-1', 'growth_board_surge', 'v1', '300002.SZ',
            DATE '2026-06-25', DATE '2026-06-25',
            TIMESTAMPTZ '2026-06-25 01:30:00+00', 'gem', 'hash'
        );
        """
    )

    with pytest.raises(ValueError, match="bound growth-board eligibility"):
        resolve_growth_board_candidates(
            store,
            date(2026, 6, 25),
            date(2026, 6, 24),
            time(9, 30),
        )


def test_growth_board_candidates_fail_closed_without_authoritative_listing_fact(
    store: DuckDBStore,
) -> None:
    from rquant.growth_board_surge_strategy import resolve_growth_board_candidates

    _seed_base_market(store)
    store.upsert_daily(
        pd.DataFrame([_daily_row("300001.SZ", date(2020, 1, 2), 5.0)])
    )
    store._conn.execute(
        "DELETE FROM stock_basic WHERE ts_code = '300001.SZ'"
    )

    candidates = resolve_growth_board_candidates(
        store,
        date(2026, 6, 25),
        date(2026, 6, 24),
        time(9, 30),
    )

    assert candidates == []


def test_growth_board_candidates_use_known_st_fact_without_name(
    store: DuckDBStore,
) -> None:
    from rquant.growth_board_surge_strategy import resolve_growth_board_candidates

    _seed_base_market(store)
    signal_date = date(2026, 6, 25)
    store._conn.execute(
        "DELETE FROM stock_status_daily WHERE ts_code = ? AND trade_date = ?",
        ["300001.SZ", signal_date],
    )
    store.upsert_stock_status((
        SecurityStatusDaily(
            ts_code="300001.SZ",
            trade_date=signal_date,
            name=None,
            is_st=False,
            name_source="unknown",
            st_source="tushare.stock_st_absence",
            available_at=datetime(2026, 6, 25, 9, 25, tzinfo=SHANGHAI),
            ingested_at=datetime(2026, 7, 1, tzinfo=UTC),
        ),
    ))

    candidates = resolve_growth_board_candidates(
        store,
        signal_date,
        date(2026, 6, 24),
        time(9, 30),
    )

    assert [candidate.ts_code for candidate in candidates] == ["300001.SZ"]
    assert candidates[0].name == "300001.SZ"


def test_growth_board_surge_replay_uses_default_config_when_omitted(
    store: DuckDBStore,
) -> None:
    from rquant.growth_board_surge_strategy import run_growth_board_surge_replay

    _seed_base_market(store)

    trades = run_growth_board_surge_replay(
        store,
        start_date=date(2026, 6, 23),
        end_date=date(2026, 6, 23),
    )

    assert trades.empty


def test_growth_board_surge_filter_supports_ablation_flags() -> None:
    from rquant.growth_board_surge_strategy import (
        GrowthBoardSurgeConfig,
        _passes_surge_filter,
    )

    same_minute_only = {
        "hist_intraday_days": 20,
        "signal_rel_cum_amount_asof": 1.5,
        "signal_rel_amount_same_minute": 3.0,
        "signal_amount_accel_5m": 1.0,
    }
    accel_only = {
        "hist_intraday_days": 20,
        "signal_rel_cum_amount_asof": 1.5,
        "signal_rel_amount_same_minute": 1.0,
        "signal_amount_accel_5m": 3.0,
    }

    assert _passes_surge_filter(same_minute_only, GrowthBoardSurgeConfig())
    assert not _passes_surge_filter(
        same_minute_only,
        GrowthBoardSurgeConfig(use_same_minute_surge=False),
    )
    assert _passes_surge_filter(accel_only, GrowthBoardSurgeConfig())
    assert not _passes_surge_filter(
        accel_only,
        GrowthBoardSurgeConfig(use_accel_surge=False),
    )
    assert _passes_surge_filter(
        same_minute_only,
        GrowthBoardSurgeConfig(use_same_minute_surge=False, use_accel_surge=False),
    )


def test_prior_days_had_surge_freshness(tmp_path) -> None:
    """首爆过滤：前 N 日量比判定（放过量/首爆/数据不足）。"""
    from datetime import date

    from rquant.growth_board_surge_strategy import _prior_days_had_surge
    from rquant.storage.duckdb import DuckDBStore

    store = DuckDBStore(tmp_path / "fresh.duckdb")
    # 600001：前 5 日量比全 <2（首爆）；600002：其中一日量比 3.5（前期放过量）
    rows = []
    for i, d in enumerate(range(11, 16)):  # 2026-06-11..15
        rows.append(("600001.SH", f"2026-06-{d}", 1.2))
        rows.append(("600002.SH", f"2026-06-{d}", 3.5 if i == 2 else 1.1))
    for ts, d, vr in rows:
        store._conn.execute(
            "INSERT INTO daily_basic (ts_code, trade_date, volume_ratio) "
            f"VALUES ('{ts}', DATE '{d}', {vr})"
        )
    signal = date(2026, 6, 16)
    assert _prior_days_had_surge(store, "600001.SH", signal, 5, 2.0) is False  # 首爆
    assert _prior_days_had_surge(store, "600002.SH", signal, 5, 2.0) is True   # 非首爆
    # 数据不足（只有 5 行，要求 6 日）→ None 保守通过
    assert _prior_days_had_surge(store, "600001.SH", signal, 6, 2.0) is None
    store.close()


def test_listed_trading_days_counts_prior_daily_bars(tmp_path) -> None:
    """不做新股：信号日之前日线根数 ≈ 已上市交易日数。"""
    from datetime import date

    from rquant.growth_board_surge_strategy import _listed_trading_days
    from rquant.storage.duckdb import DuckDBStore

    store = DuckDBStore(tmp_path / "listing.duckdb")
    for d in range(1, 21):  # 20 根日线，2026-06-0x..
        store._conn.execute(
            "INSERT INTO daily_bar (ts_code, trade_date, close) "
            f"VALUES ('300999.SZ', DATE '2026-06-{d:02d}', 10.0)"
        )
    signal = date(2026, 6, 21)
    assert _listed_trading_days(store, "300999.SZ", signal) == 20
    # 未上市的票 → 0
    assert _listed_trading_days(store, "301000.SZ", signal) == 0
    store.close()


def test_growth_board_surge_replay_calculates_ma_from_daily_bar(
    store: DuckDBStore,
) -> None:
    from rquant.growth_board_surge_strategy import (
        GrowthBoardSurgeConfig,
        run_growth_board_surge_replay,
    )

    dates = [date(2026, 3, 1) + timedelta(days=i) for i in range(63)]
    _seed_open_calendar(store, dates)
    ts_code = "300002.SZ"
    daily_rows = [
        _daily_row(ts_code, trade_date, 10.0 + idx * 0.1)
        for idx, trade_date in enumerate(dates)
    ]
    store.upsert_daily(pd.DataFrame(daily_rows))
    store.upsert_stock_basic(pd.DataFrame([{
        "ts_code": ts_code,
        "symbol": "300002",
        "name": "无指标样本",
        "area": "深圳",
        "industry": "测试",
        "list_date": "20200101",
        "market": "创业板",
    }]))
    signal_date = dates[-2]
    exit_date = dates[-1]
    store.upsert_stock_status(tuple(
        _known_status(ts_code, trade_date, name="历史无指标样本")
        for trade_date in dates
    ))
    store.upsert_state(pd.DataFrame([
        _state_row(ts_code, signal_date, board_type="gem", limit_up_price=20.0),
    ]))
    hist_dates = dates[-5:-3]
    minute_rows = []
    for hist_day in hist_dates:
        for minute in [30, 31, 32, 33]:
            minute_rows.append(
                _minute_row(
                    ts_code,
                    datetime.combine(hist_day, time(9, minute)),
                    15.0,
                    amount=1000.0,
                )
            )
    minute_rows.extend([
        _minute_row(ts_code, datetime.combine(signal_date, time(9, 30)), 16.10, amount=1000.0),
        _minute_row(ts_code, datetime.combine(signal_date, time(9, 31)), 16.20, amount=1000.0),
        _minute_row(ts_code, datetime.combine(signal_date, time(9, 32)), 16.35, amount=2000.0),
        _minute_row(ts_code, datetime.combine(signal_date, time(9, 33)), 16.60, amount=3000.0),
        _minute_row(ts_code, datetime.combine(signal_date, time(9, 34)), 16.70, amount=1200.0),
        _minute_row(ts_code, datetime.combine(exit_date, time(15, 0)), 17.20, amount=1000.0),
    ])
    store.upsert_minute_bars(pd.DataFrame(minute_rows))

    trades = run_growth_board_surge_replay(
        store,
        start_date=signal_date,
        end_date=signal_date,
        config=GrowthBoardSurgeConfig(
            min_signal_time=time(9, 33),
            lookback_days=2,
            min_hist_days=2,
            max_hold_days=1,
            min_cum_amount_ratio=1.4,
            min_same_minute_amount_ratio=2.0,
        ),
    )

    assert len(trades) == 1
    assert trades.iloc[0]["ts_code"] == ts_code


def test_growth_board_surge_replay_requires_next_day_exit_minutes(
    store: DuckDBStore,
) -> None:
    from rquant.growth_board_surge_strategy import (
        GrowthBoardSurgeConfig,
        run_growth_board_surge_replay,
    )

    _seed_base_market(store)
    rows: list[dict[str, object]] = []
    for hist_day in [date(2026, 6, 22), date(2026, 6, 23)]:
        for minute in [30, 31, 32, 33]:
            rows.append(_minute_row(
                "300001.SZ",
                datetime.combine(hist_day, time(9, minute)),
                10.0,
                amount=1000.0,
            ))
    rows.extend([
        _minute_row("300001.SZ", datetime(2026, 6, 25, 9, 30), 10.20, amount=1000.0),
        _minute_row("300001.SZ", datetime(2026, 6, 25, 9, 31), 10.30, amount=1000.0),
        _minute_row("300001.SZ", datetime(2026, 6, 25, 9, 32), 10.45, amount=2000.0),
        _minute_row("300001.SZ", datetime(2026, 6, 25, 9, 33), 10.70, amount=3000.0),
        _minute_row("300001.SZ", datetime(2026, 6, 25, 9, 34), 10.80, amount=1200.0),
    ])
    store.upsert_minute_bars(pd.DataFrame(rows))

    trades = run_growth_board_surge_replay(
        store,
        start_date=date(2026, 6, 25),
        end_date=date(2026, 6, 25),
        config=GrowthBoardSurgeConfig(
            min_signal_time=time(9, 33),
            lookback_days=2,
            min_hist_days=2,
            max_hold_days=1,
            min_cum_amount_ratio=1.4,
            min_same_minute_amount_ratio=2.0,
        ),
    )

    assert trades.empty


# ── 用户三条件 + factor_confirm（tick-rule 内外盘 / T-1 大单净量 / 评分确认）──


def _seed_inner_dominant_minutes(store: DuckDBStore) -> None:
    """信号日 9:33 触发放量宽门，且 tick-rule 内盘量 > 外盘量的分钟序列。

    9:30 open=close 均分；9:31 大跌量记内盘；9:32/9:33 上涨记外盘。
    9:33 时 inner≈927、outer≈624 → ratio≈1.49。
    """
    rows: list[dict[str, object]] = []
    for hist_day in [date(2026, 6, 22), date(2026, 6, 23)]:
        for minute in [30, 31, 32, 33]:
            rows.append(_minute_row(
                "300001.SZ",
                datetime.combine(hist_day, time(9, minute)),
                10.0,
                amount=1000.0,
            ))
    rows.extend([
        _minute_row("300001.SZ", datetime(2026, 6, 25, 9, 30), 10.50, amount=3000.0),
        _minute_row("300001.SZ", datetime(2026, 6, 25, 9, 31), 10.20, amount=8000.0),
        _minute_row("300001.SZ", datetime(2026, 6, 25, 9, 32), 10.30, amount=2000.0),
        _minute_row("300001.SZ", datetime(2026, 6, 25, 9, 33), 10.45, amount=3000.0),
        _minute_row("300001.SZ", datetime(2026, 6, 25, 9, 34), 10.55, amount=1200.0),
        _minute_row("300001.SZ", datetime(2026, 6, 26, 15, 0), 10.80, amount=1000.0),
    ])
    store.upsert_minute_bars(pd.DataFrame(rows))


def _seed_moneyflow_t1(store: DuckDBStore, large_net_vol: float) -> None:
    store.upsert_moneyflow_daily(pd.DataFrame([{
        "ts_code": "300001.SZ",
        "trade_date": date(2026, 6, 24),
        "buy_lg_vol": max(large_net_vol, 0.0),
        "sell_lg_vol": max(-large_net_vol, 0.0),
        "buy_elg_vol": 0.0,
        "sell_elg_vol": 0.0,
        "large_net_vol": large_net_vol,
        "large_net_amount": large_net_vol * 10.0,
    }]))


def test_tick_rule_split_classifies_direction() -> None:
    from rquant.growth_board_surge_strategy import _tick_rule_split

    assert _tick_rule_split(10.0, 10.1, 100.0) == (0.0, 100.0)  # 升=外盘
    assert _tick_rule_split(10.0, 9.9, 100.0) == (100.0, 0.0)  # 降=内盘
    assert _tick_rule_split(10.0, 10.0, 100.0) == (50.0, 50.0)  # 平=均分
    assert _tick_rule_split(10.0, 10.1, 0.0) == (0.0, 0.0)


def test_growth_board_surge_inner_outer_gate_blocks_outer_dominant(
    store: DuckDBStore,
) -> None:
    """全程上涨的信号日外盘占优，内盘>外盘闸门应拦截；baseline 仍带观察值。"""
    from rquant.growth_board_surge_strategy import (
        GrowthBoardSurgeConfig,
        run_growth_board_surge_replay,
    )

    _seed_base_market(store)
    _seed_volume_surge_minutes(store)
    common = {
        "min_signal_time": time(9, 33),
        "lookback_days": 2,
        "min_hist_days": 2,
        "max_hold_days": 1,
        "min_cum_amount_ratio": 1.4,
        "min_same_minute_amount_ratio": 2.0,
    }

    baseline = run_growth_board_surge_replay(
        store,
        start_date=date(2026, 6, 25),
        end_date=date(2026, 6, 25),
        config=GrowthBoardSurgeConfig(**common),
    )
    assert len(baseline) == 1
    assert baseline.iloc[0]["inner_outer_ratio"] < 1

    gated = run_growth_board_surge_replay(
        store,
        start_date=date(2026, 6, 25),
        end_date=date(2026, 6, 25),
        config=GrowthBoardSurgeConfig(**common, require_inner_outer=True),
    )
    assert gated.empty


def test_growth_board_surge_inner_outer_gate_allows_inner_dominant(
    store: DuckDBStore,
) -> None:
    from rquant.growth_board_surge_strategy import (
        GrowthBoardSurgeConfig,
        run_growth_board_surge_replay,
    )

    _seed_base_market(store)
    _seed_inner_dominant_minutes(store)
    common = {
        "min_signal_time": time(9, 33),
        "lookback_days": 2,
        "min_hist_days": 2,
        "max_hold_days": 1,
        "min_cum_amount_ratio": 1.4,
        "min_same_minute_amount_ratio": 2.0,
    }

    trades = run_growth_board_surge_replay(
        store,
        start_date=date(2026, 6, 25),
        end_date=date(2026, 6, 25),
        config=GrowthBoardSurgeConfig(**common, require_inner_outer=True),
    )
    assert len(trades) == 1
    row = trades.iloc[0]
    assert row["entry_time"] == datetime(2026, 6, 25, 9, 34)
    assert row["inner_outer_ratio"] == pytest.approx(1.4854, abs=0.001)
    assert row["entry_signal"] == "growth_board_volume_surge"

    stricter = run_growth_board_surge_replay(
        store,
        start_date=date(2026, 6, 25),
        end_date=date(2026, 6, 25),
        config=GrowthBoardSurgeConfig(
            **common, require_inner_outer=True, min_inner_outer_ratio=2.0
        ),
    )
    assert stricter.empty


def test_growth_board_surge_large_net_vol_gate_uses_t_minus_1(
    store: DuckDBStore,
) -> None:
    """T-1 moneyflow 大单净量>0 才放行；缺行按不满足处理（防未来函数口径）。"""
    from rquant.growth_board_surge_strategy import (
        GrowthBoardSurgeConfig,
        run_growth_board_surge_replay,
    )

    _seed_base_market(store)
    _seed_volume_surge_minutes(store)
    common = {
        "min_signal_time": time(9, 33),
        "lookback_days": 2,
        "min_hist_days": 2,
        "max_hold_days": 1,
        "min_cum_amount_ratio": 1.4,
        "min_same_minute_amount_ratio": 2.0,
    }
    config = GrowthBoardSurgeConfig(**common, require_large_net_vol=True)

    missing = run_growth_board_surge_replay(
        store,
        start_date=date(2026, 6, 25),
        end_date=date(2026, 6, 25),
        config=config,
    )
    assert missing.empty

    _seed_moneyflow_t1(store, -500.0)
    negative = run_growth_board_surge_replay(
        store,
        start_date=date(2026, 6, 25),
        end_date=date(2026, 6, 25),
        config=config,
    )
    assert negative.empty

    _seed_moneyflow_t1(store, 500.0)
    positive = run_growth_board_surge_replay(
        store,
        start_date=date(2026, 6, 25),
        end_date=date(2026, 6, 25),
        config=config,
    )
    assert len(positive) == 1
    assert positive.iloc[0]["large_net_vol_t1"] == pytest.approx(500.0)


def test_growth_board_surge_factor_confirm_scores_and_gates(
    store: DuckDBStore,
) -> None:
    from rquant.growth_board_surge_strategy import (
        GrowthBoardSurgeConfig,
        run_growth_board_surge_replay,
    )
    from rquant.signal_provenance import GROWTH_SURGE_V1_FACTORS

    _seed_base_market(store)
    _seed_volume_surge_minutes(store)
    _seed_moneyflow_t1(store, 500.0)
    common = {
        "min_signal_time": time(9, 33),
        "lookback_days": 2,
        "min_hist_days": 2,
        "max_hold_days": 1,
        "min_cum_amount_ratio": 1.4,
        "min_same_minute_amount_ratio": 2.0,
    }

    trades = run_growth_board_surge_replay(
        store,
        start_date=date(2026, 6, 25),
        end_date=date(2026, 6, 25),
        config=GrowthBoardSurgeConfig(
            **common, enable_factor_confirm=True, factor_score_threshold=10.0
        ),
    )
    assert len(trades) == 1
    row = trades.iloc[0]
    assert row["entry_signal"] == "growth_board_factor_confirm"
    assert row["growth_factor_score"] >= 10.0
    assert row["growth_factor_score_threshold"] == pytest.approx(10.0)
    assert row["growth_factor_hit_count"] >= 1
    assert row["growth_factor_evaluated_count"] >= row["growth_factor_hit_count"]

    blocked = run_growth_board_surge_replay(
        store,
        start_date=date(2026, 6, 25),
        end_date=date(2026, 6, 25),
        config=GrowthBoardSurgeConfig(
            **common, enable_factor_confirm=True, factor_score_threshold=99.0
        ),
    )
    assert blocked.empty

    # 因子键名子集锁死 GROWTH_SURGE_V1
    locked = {spec.name for spec in GROWTH_SURGE_V1_FACTORS}
    assert {
        "inner_outer_ratio", "large_net_vol_t1",
    } <= locked


def test_growth_board_surge_classic_volume_ratio_observed(
    store: DuckDBStore,
) -> None:
    """经典量比观察值：当日每分钟均量 / T-1 可知的 5 日每分钟均量。"""
    from rquant.growth_board_surge_strategy import (
        GrowthBoardSurgeConfig,
        run_growth_board_surge_replay,
    )

    _seed_base_market(store)
    # 补足 T-1（6/24）前的 5 根日线（基础 fixture 只有 6/22 起 3 根 ≤ 6/24）
    store.upsert_daily(pd.DataFrame([
        _daily_row("300001.SZ", date(2026, 6, 18), 9.6),
        _daily_row("300001.SZ", date(2026, 6, 19), 9.8),
    ]))
    _seed_volume_surge_minutes(store)

    trades = run_growth_board_surge_replay(
        store,
        start_date=date(2026, 6, 25),
        end_date=date(2026, 6, 25),
        config=GrowthBoardSurgeConfig(
            min_signal_time=time(9, 33),
            lookback_days=2,
            min_hist_days=2,
            max_hold_days=1,
            min_cum_amount_ratio=1.4,
            min_same_minute_amount_ratio=2.0,
        ),
    )
    assert len(trades) == 1
    row = trades.iloc[0]
    # 日线 vol=1000 手 → 5 日均每分钟量 = 1000*100/240；当日 4 分钟累计量见 fixture
    cum_vol = (
        1000.0 / 10.20 + 1000.0 / 10.30 + 2000.0 / 10.45 + 3000.0 / 10.70
    )
    expected = (cum_vol / 4) / (1000.0 * 100 / 240)
    assert row["classic_volume_ratio"] == pytest.approx(expected, abs=0.001)


# ── 板块集合竞价强度闸门（require_board_favor）──


def _auction_row(
    ts_code: str, trade_date: date, price: float, amount: float
) -> dict[str, object]:
    return {
        "ts_code": ts_code,
        "trade_date": trade_date,
        "auction_type": "open_realtime",
        "price": price,
        "vol": amount / price,
        "amount": amount,
        "turnover_rate": 1.0,
        "volume_ratio": 1.0,
        "source": "tushare",
    }


def _seed_board_favor(store: DuckDBStore) -> None:
    """题材 000100.KP 含候选 300001.SZ + 300010.SZ，信号日 6/25 竞价强：
    高开占比 1.0、竞价资金 6000 vs 历史中位 1000 → ratio 6.0。"""
    x, peer = "300001.SZ", "300010.SZ"
    punch = date(2026, 6, 24)  # ≤ 信号日 6/25 的最近打点
    store.upsert_dataset("kpl_concept_member_daily", pd.DataFrame([
        {"trade_date": punch, "board_code": "000100.KP", "board_name": "题材A",
         "con_code": x, "con_name": "创业样本", "hot_num": 100},
        {"trade_date": punch, "board_code": "000100.KP", "board_name": "题材A",
         "con_code": peer, "con_name": "同板样本", "hot_num": 80},
    ]))
    # peer 的 T-1 昨收（300001.SZ 已由 base market 提供 6/24 close=10.4）
    store.upsert_daily(pd.DataFrame([_daily_row(peer, date(2026, 6, 24), 10.0)]))
    auctions = [
        # 信号日 6/25：两只都高开（X>10.4、peer>10）
        _auction_row(x, date(2026, 6, 25), 12.0, 3000.0),
        _auction_row(peer, date(2026, 6, 25), 11.0, 3000.0),
        # 历史 6/23、6/24：每日合计 1000 → 中位 1000
        _auction_row(x, date(2026, 6, 24), 10.0, 500.0),
        _auction_row(peer, date(2026, 6, 24), 10.0, 500.0),
        _auction_row(x, date(2026, 6, 23), 10.0, 500.0),
        _auction_row(peer, date(2026, 6, 23), 10.0, 500.0),
    ]
    store.upsert_auction_bars(pd.DataFrame(auctions))


def test_growth_board_surge_board_favor_gate_allows_strong_board(
    store: DuckDBStore,
) -> None:
    from rquant.growth_board_surge_strategy import (
        GrowthBoardSurgeConfig,
        run_growth_board_surge_replay,
    )

    _seed_base_market(store)
    _seed_volume_surge_minutes(store)
    _seed_board_favor(store)
    common = {
        "min_signal_time": time(9, 33),
        "lookback_days": 2,
        "min_hist_days": 2,
        "max_hold_days": 1,
        "min_cum_amount_ratio": 1.4,
        "min_same_minute_amount_ratio": 2.0,
    }

    trades = run_growth_board_surge_replay(
        store,
        start_date=date(2026, 6, 25),
        end_date=date(2026, 6, 25),
        config=GrowthBoardSurgeConfig(
            **common,
            require_board_favor=True,
            min_board_gap_up_ratio=0.5,
            min_board_auction_amount_ratio=1.0,
        ),
    )
    assert len(trades) == 1
    row = trades.iloc[0]
    assert row["board_code"] == "000100.KP"
    assert row["board_gap_up_ratio"] == pytest.approx(1.0)
    assert row["board_auction_amount_ratio"] == pytest.approx(6.0)
    assert row["board_member_count"] == 2


def test_growth_board_surge_board_favor_gate_blocks_weak_amount(
    store: DuckDBStore,
) -> None:
    from rquant.growth_board_surge_strategy import (
        GrowthBoardSurgeConfig,
        run_growth_board_surge_replay,
    )

    _seed_base_market(store)
    _seed_volume_surge_minutes(store)
    _seed_board_favor(store)
    common = {
        "min_signal_time": time(9, 33),
        "lookback_days": 2,
        "min_hist_days": 2,
        "max_hold_days": 1,
        "min_cum_amount_ratio": 1.4,
        "min_same_minute_amount_ratio": 2.0,
    }

    # 资金比阈值抬到 99 → 不达标拦截
    blocked = run_growth_board_surge_replay(
        store,
        start_date=date(2026, 6, 25),
        end_date=date(2026, 6, 25),
        config=GrowthBoardSurgeConfig(
            **common,
            require_board_favor=True,
            min_board_auction_amount_ratio=99.0,
        ),
    )
    assert blocked.empty


def test_growth_board_surge_board_favor_gate_blocks_no_membership(
    store: DuckDBStore,
) -> None:
    """无题材归属（未采集日度成分）→ require_board_favor 保守拦截。"""
    from rquant.growth_board_surge_strategy import (
        GrowthBoardSurgeConfig,
        run_growth_board_surge_replay,
    )

    _seed_base_market(store)
    _seed_volume_surge_minutes(store)
    # 不 seed kpl_concept_member_daily / auction_bar → 无归属
    trades = run_growth_board_surge_replay(
        store,
        start_date=date(2026, 6, 25),
        end_date=date(2026, 6, 25),
        config=GrowthBoardSurgeConfig(
            min_signal_time=time(9, 33),
            lookback_days=2,
            min_hist_days=2,
            max_hold_days=1,
            min_cum_amount_ratio=1.4,
            min_same_minute_amount_ratio=2.0,
            require_board_favor=True,
        ),
    )
    assert trades.empty


def test_growth_board_surge_replay_filters_intraday_yiziban(store: DuckDBStore) -> None:
    from rquant.growth_board_surge_strategy import (
        GrowthBoardSurgeConfig,
        run_growth_board_surge_replay,
    )

    _seed_base_market(store)
    rows = []
    for hist_day in [date(2026, 6, 22), date(2026, 6, 23)]:
        rows.append(
            _minute_row(
                "300001.SZ",
                datetime.combine(hist_day, time(9, 30)),
                10.0,
                amount=1000.0,
            )
        )
    rows.extend([
        _minute_row(
            "300001.SZ",
            datetime(2026, 6, 25, 9, 30),
            13.0,
            amount=10000.0,
            low=13.0,
            high=13.0,
            open_price=13.0,
        ),
        _minute_row("300001.SZ", datetime(2026, 6, 25, 9, 31), 13.0, amount=0.0),
        _minute_row("300001.SZ", datetime(2026, 6, 26, 15, 0), 13.2, amount=1000.0),
    ])
    store.upsert_minute_bars(pd.DataFrame(rows))

    trades = run_growth_board_surge_replay(
        store,
        start_date=date(2026, 6, 25),
        end_date=date(2026, 6, 25),
        config=GrowthBoardSurgeConfig(
            min_signal_time=time(9, 30),
            lookback_days=2,
            min_hist_days=1,
        ),
    )

    assert trades.empty


def test_exit_structure_defaults_hold3_stop5() -> None:
    """退出结构默认：持仓上限 3 日、单票止损 -5%（用户 2026-07-04 验证段确认）。"""
    from rquant.growth_board_surge_strategy import GrowthBoardSurgeConfig

    cfg = GrowthBoardSurgeConfig()
    assert cfg.max_hold_days == 3
    assert cfg.paper.stop_loss_pct == 0.05
    # take_profit / trailing 不动
    assert cfg.paper.take_profit_pct == 0.08
    assert cfg.paper.trailing_stop_pct == 0.03


def test_board_hist_days_defaults_to_three_and_decoupled_from_lookback() -> None:
    """板块竞价窗口 board_hist_days 默认 3，且与核心爆量窗口 lookback_days 解耦。

    原先板块窗口误复用 lookback_days（20），2026-07-04 实验后独立成字段并改默认 3。
    """
    from rquant.growth_board_surge_strategy import GrowthBoardSurgeConfig

    cfg = GrowthBoardSurgeConfig()
    assert cfg.board_hist_days == 3
    assert cfg.lookback_days == 20
    # 改一个不动另一个
    tuned = GrowthBoardSurgeConfig(lookback_days=10)
    assert tuned.board_hist_days == 3
