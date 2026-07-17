"""Controlled daily_indicator derivation and backfill tests."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

import rquant.indicator_backfill as backfill_module
from rquant.storage.duckdb import DuckDBStore

SHANGHAI = ZoneInfo("Asia/Shanghai")


def _seed_history(
    store: DuckDBStore,
    *,
    periods: int = 65,
) -> list[date]:
    trade_dates = list(
        pd.bdate_range(end="2024-04-01", periods=periods).date
    )
    store.upsert_daily(
        pd.DataFrame(
            [
                {
                    "ts_code": "600000.SH",
                    "trade_date": trade_date,
                    "open": 9.8 + index * 0.01,
                    "high": 10.5 + index * 0.01,
                    "low": 9.5 + index * 0.01,
                    "close": 10.0 + index * 0.01,
                    "pre_close": 10.0 + max(0, index - 1) * 0.01,
                    "change": 0.01,
                    "pct_chg": 0.1,
                    "vol": 1000.0,
                    "amount": 10000.0,
                }
                for index, trade_date in enumerate(trade_dates)
            ]
        )
    )
    store.upsert_adj_factor(
        pd.DataFrame(
            [
                {
                    "ts_code": "600000.SH",
                    "trade_date": trade_date,
                    "adj_factor": 1.0 if index < 63 else 2.0 + index - 63,
                }
                for index, trade_date in enumerate(trade_dates)
            ]
        )
    )
    return trade_dates


@pytest.fixture()
def store(tmp_path: Path) -> DuckDBStore:
    value = DuckDBStore(tmp_path / "indicator-backfill.duckdb")
    try:
        yield value
    finally:
        value.close()


def test_dry_run_reports_scope_without_writing(store: DuckDBStore) -> None:
    trade_dates = _seed_history(store)
    start_date, end_date = trade_dates[-3], trade_dates[-1]

    result = backfill_module.backfill_daily_indicators(
        store,
        start_date=start_date,
        end_date=end_date,
        apply=False,
        now=datetime(2024, 4, 1, 10, 0, tzinfo=SHANGHAI),
    )

    assert result.model_dump(mode="json") == {
        "code_count": 1,
        "estimated_rows": 3,
        "actual_rows": 0,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "dry_run": True,
    }
    assert store.count_indicators() == 0


def test_apply_replaces_only_requested_range(store: DuckDBStore) -> None:
    trade_dates = _seed_history(store)
    preserved_date = trade_dates[-3]
    start_date, end_date = trade_dates[-2], trade_dates[-1]
    sentinel_rows = pd.DataFrame(
        [
            {
                "ts_code": "600000.SH",
                "trade_date": indicator_date,
                "ma5": 999.0,
                "ma10": 999.0,
                "ma20": 999.0,
                "ma60": 999.0,
                "rsi6": 999.0,
                "rsi14": 999.0,
                "macd": 999.0,
                "macd_signal": 999.0,
                "macd_hist": 999.0,
                "kdj_k": 999.0,
                "kdj_d": 999.0,
                "kdj_j": 999.0,
            }
            for indicator_date in (preserved_date, start_date)
        ]
    )
    store.upsert_indicators(sentinel_rows)

    result = backfill_module.backfill_daily_indicators(
        store,
        start_date=start_date,
        end_date=end_date,
        apply=True,
        now=datetime(2024, 3, 30, 10, 0, tzinfo=SHANGHAI),
    )

    rows = store._conn.execute(
        """
        SELECT trade_date, ma5
        FROM daily_indicator
        WHERE ts_code = '600000.SH'
        ORDER BY trade_date
        """
    ).fetchall()
    assert result.model_dump(mode="json") == {
        "code_count": 1,
        "estimated_rows": 2,
        "actual_rows": 2,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "dry_run": False,
    }
    assert rows[0] == (preserved_date, 999.0)
    assert [row[0] for row in rows[1:]] == [start_date, end_date]
    assert all(row[1] != 999.0 for row in rows[1:])


def test_batch_derivation_keeps_each_output_on_its_own_factor_basis(
    store: DuckDBStore,
) -> None:
    trade_dates = _seed_history(store)
    earlier_date, later_date = trade_dates[-2], trade_dates[-1]

    earlier_only = backfill_module.derive_daily_indicators(
        store,
        start_date=earlier_date,
        end_date=earlier_date,
    )
    wider_range = backfill_module.derive_daily_indicators(
        store,
        start_date=earlier_date,
        end_date=later_date,
    )

    earlier_from_wider = wider_range.loc[
        pd.to_datetime(wider_range["trade_date"]).dt.date == earlier_date
    ].reset_index(drop=True)
    pd.testing.assert_frame_equal(
        earlier_from_wider,
        earlier_only.reset_index(drop=True),
    )


@pytest.mark.parametrize(
    "invalid_factor",
    [None, float("nan"), float("inf"), 0.0, -1.0],
    ids=["missing", "nan", "inf", "zero", "negative"],
)
def test_derivation_fails_closed_after_invalid_historical_factor(
    store: DuckDBStore,
    invalid_factor: float | None,
) -> None:
    trade_dates = _seed_history(store)
    invalid_date = trade_dates[-2]
    if invalid_factor is None:
        store._conn.execute(
            """
            DELETE FROM adj_factor
            WHERE ts_code = '600000.SH' AND trade_date = ?
            """,
            [invalid_date],
        )
    else:
        store._conn.execute(
            """
            UPDATE adj_factor
            SET adj_factor = ?
            WHERE ts_code = '600000.SH' AND trade_date = ?
            """,
            [invalid_factor, invalid_date],
        )

    result = backfill_module.derive_daily_indicators(
        store,
        start_date=trade_dates[-1],
        end_date=trade_dates[-1],
    )

    assert result.empty


def test_short_valid_listing_keeps_output_with_normal_ma60_nan(
    store: DuckDBStore,
) -> None:
    trade_dates = _seed_history(store, periods=10)

    result = backfill_module.derive_daily_indicators(
        store,
        start_date=trade_dates[-1],
        end_date=trade_dates[-1],
    )

    assert len(result) == 1
    assert pd.isna(result.iloc[0]["ma60"])


@pytest.mark.parametrize(
    "now",
    [
        datetime(2024, 4, 1, 9, 14, tzinfo=SHANGHAI),
        datetime(2024, 4, 1, 9, 15, tzinfo=SHANGHAI),
        datetime(2024, 4, 1, 12, 0, tzinfo=SHANGHAI),
        datetime(2024, 4, 1, 15, 10, tzinfo=SHANGHAI),
    ],
)
def test_apply_rejects_protected_window_without_writing(
    store: DuckDBStore,
    now: datetime,
) -> None:
    trade_dates = _seed_history(store)

    with pytest.raises(RuntimeError, match="09:15-15:10"):
        backfill_module.backfill_daily_indicators(
            store,
            start_date=trade_dates[-2],
            end_date=trade_dates[-1],
            apply=True,
            now=now,
        )

    assert store.count_indicators() == 0


def test_apply_rolls_back_deleted_rows_when_indicator_upsert_fails(
    tmp_path: Path,
) -> None:
    class _FailingStore(DuckDBStore):
        def upsert_indicators(self, df: pd.DataFrame) -> int:
            super().upsert_indicators(df)
            raise RuntimeError("indicator upsert failed")

    store = _FailingStore(tmp_path / "indicator-rollback.duckdb")
    try:
        trade_dates = _seed_history(store)
        start_date, end_date = trade_dates[-2], trade_dates[-1]
        sentinel_rows = pd.DataFrame(
            [
                {
                    "ts_code": "600000.SH",
                    "trade_date": indicator_date,
                    "ma5": 999.0,
                    "ma10": 999.0,
                    "ma20": 999.0,
                    "ma60": 999.0,
                    "rsi6": 999.0,
                    "rsi14": 999.0,
                    "macd": 999.0,
                    "macd_signal": 999.0,
                    "macd_hist": 999.0,
                    "kdj_k": 999.0,
                    "kdj_d": 999.0,
                    "kdj_j": 999.0,
                }
                for indicator_date in (start_date, end_date)
            ]
        )
        DuckDBStore.upsert_indicators(store, sentinel_rows)

        with pytest.raises(RuntimeError, match="indicator upsert failed"):
            backfill_module.backfill_daily_indicators(
                store,
                start_date=start_date,
                end_date=end_date,
                apply=True,
                now=datetime(2024, 3, 30, 10, 0, tzinfo=SHANGHAI),
            )

        rows = store._conn.execute(
            """
            SELECT trade_date, ma5
            FROM daily_indicator
            WHERE ts_code = '600000.SH'
              AND trade_date BETWEEN ? AND ?
            ORDER BY trade_date
            """,
            [start_date, end_date],
        ).fetchall()
        assert rows == [(start_date, 999.0), (end_date, 999.0)]
    finally:
        store.close()


def test_apply_rechecks_window_after_readonly_derivation(
    store: DuckDBStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trade_dates = _seed_history(store)
    times = iter(
        [
            datetime(2024, 4, 1, 8, 0, tzinfo=SHANGHAI),
            datetime(2024, 4, 1, 9, 15, tzinfo=SHANGHAI),
        ]
    )
    monkeypatch.setattr(backfill_module, "_now", lambda: next(times))

    with pytest.raises(RuntimeError, match="09:15-15:10"):
        backfill_module.backfill_daily_indicators(
            store,
            start_date=trade_dates[-2],
            end_date=trade_dates[-1],
            apply=True,
        )

    assert store.count_indicators() == 0
