"""Tracked script orchestration tests for PIT daily state."""

from __future__ import annotations

import importlib.util
from datetime import UTC, date, datetime
from pathlib import Path
from types import ModuleType

import pandas as pd
import pytest

from rquant.security_status import SHANGHAI, SecurityStatusDaily
from rquant.storage.duckdb import DuckDBStore

ROOT = Path(__file__).resolve().parents[2]
INGESTED_AT = datetime(2020, 1, 3, 8, tzinfo=UTC)
INJECTED_CODE = "600000.SH' OR 1=1 --"


def _load_script(name: str) -> ModuleType:
    path = ROOT / "scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(f"test_script_{name}", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _ScriptAdapter:
    def daily(
        self,
        *,
        ts_codes: list[str],
        start: date,
        end: date,
    ) -> pd.DataFrame:
        assert ts_codes == [INJECTED_CODE]
        assert (start, end) == (date(2020, 1, 2), date(2020, 1, 2))
        return pd.DataFrame(
            [
                {
                    "ts_code": INJECTED_CODE,
                    "trade_date": date(2020, 1, 2),
                    "open": 10.0,
                    "high": 10.2,
                    "low": 9.9,
                    "close": 10.1,
                    "pre_close": 10.0,
                    "change": 0.1,
                    "pct_chg": 1.0,
                    "vol": 1.0,
                    "amount": 1.0,
                }
            ]
        )

    def adj_factor(
        self,
        *,
        ts_codes: list[str],
        start: date,
        end: date,
    ) -> pd.DataFrame:
        del ts_codes, start, end
        return pd.DataFrame()

    def daily_basic(
        self,
        *,
        ts_codes: list[str],
        trade_date: date,
    ) -> pd.DataFrame:
        del ts_codes, trade_date
        return pd.DataFrame()

    def stock_basic(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "ts_code": INJECTED_CODE,
                    "symbol": "600000",
                    "name": "*ST当前名",
                    "area": "上海",
                    "industry": "测试",
                    "list_date": "19991110",
                    "market": "主板",
                }
            ]
        )

    def namechange_raw(
        self,
        start_date: date,
        end_date: date,
        ts_code: str | None = None,
    ) -> pd.DataFrame:
        del start_date, end_date
        assert ts_code == INJECTED_CODE
        return pd.DataFrame(
            [
                {
                    "ts_code": INJECTED_CODE,
                    "name": "历史正常名",
                    "start_date": "19991110",
                    "end_date": None,
                    "ann_date": "19991110",
                    "change_reason": "上市",
                }
            ]
        )

    def stock_st_raw(self, trade_date: date) -> pd.DataFrame:
        del trade_date
        return pd.DataFrame(
            columns=["ts_code", "name", "trade_date", "type", "type_name"]
        )


def test_historical_ingest_script_uses_dated_status_and_parameterized_code_scope(
    tmp_path: Path,
) -> None:
    module = _load_script("ingest_daily")
    db_path = tmp_path / "script.duckdb"
    with DuckDBStore(db_path) as store:
        store._conn.execute(
            """
            INSERT INTO daily_bar (
                ts_code, trade_date, open, high, low, close, pre_close
            ) VALUES ('000001.SZ', DATE '2019-01-02', 5, 5.1, 4.9, 5, 5)
            """
        )

    result = module.run_historical_ingest(
        [INJECTED_CODE],
        date(2020, 1, 2),
        date(2020, 1, 2),
        adapter=_ScriptAdapter(),
        store_factory=lambda: DuckDBStore(db_path),
        ingested_at=INGESTED_AT,
        status_request_interval_seconds=0,
        sleep=lambda _: None,
        compute=lambda frame: pd.DataFrame(),
    )

    assert result == 0
    with DuckDBStore(db_path, read_only=True) as store:
        state = store.get_state(INJECTED_CODE)
        status = store.list_stock_status(date(2020, 1, 2), date(2020, 1, 2))
    assert pd.to_datetime(state["trade_date"]).dt.date.tolist() == [date(2020, 1, 2)]
    assert state["is_st"].tolist() == [False]
    assert [(row.ts_code, row.is_st) for row in status] == [(INJECTED_CODE, False)]


def test_historical_ingest_filters_provider_rows_to_requested_date_range(
    tmp_path: Path,
) -> None:
    module = _load_script("ingest_daily")

    class _ExtraDateAdapter(_ScriptAdapter):
        def daily(
            self,
            *,
            ts_codes: list[str],
            start: date,
            end: date,
        ) -> pd.DataFrame:
            target = super().daily(ts_codes=ts_codes, start=start, end=end)
            extra = target.copy()
            extra["trade_date"] = date(2020, 1, 3)
            return pd.concat([target, extra], ignore_index=True)

        def adj_factor(
            self,
            *,
            ts_codes: list[str],
            start: date,
            end: date,
        ) -> pd.DataFrame:
            del start, end
            return pd.DataFrame(
                [
                    {
                        "ts_code": ts_codes[0],
                        "trade_date": day,
                        "adj_factor": 1.0,
                    }
                    for day in (date(2020, 1, 2), date(2020, 1, 3))
                ]
            )

        def daily_basic(
            self,
            *,
            ts_codes: list[str],
            trade_date: date,
        ) -> pd.DataFrame:
            del trade_date
            return pd.DataFrame(
                [
                    {
                        "ts_code": ts_codes[0],
                        "trade_date": day,
                        "turnover_rate": 1.0,
                        "volume_ratio": 1.0,
                        "total_mv": 100.0,
                        "circ_mv": 80.0,
                    }
                    for day in (date(2020, 1, 2), date(2020, 1, 3))
                ]
            )

    db_path = tmp_path / "extra-date.duckdb"
    result = module.run_historical_ingest(
        [INJECTED_CODE],
        date(2020, 1, 2),
        date(2020, 1, 2),
        adapter=_ExtraDateAdapter(),
        store_factory=lambda: DuckDBStore(db_path),
        ingested_at=INGESTED_AT,
        status_request_interval_seconds=0,
        sleep=lambda _: None,
        compute=lambda frame: pd.DataFrame(),
    )

    assert result == 0
    with DuckDBStore(db_path, read_only=True) as store:
        dates_by_table = {
            table: store._conn.execute(
                f"SELECT DISTINCT trade_date FROM {table} ORDER BY trade_date"
            ).fetchall()
            for table in (
                "daily_bar",
                "daily_basic",
                "adj_factor",
                "stock_status_daily",
                "daily_state",
            )
        }
    assert dates_by_table == {
        table: [(date(2020, 1, 2),)] for table in dates_by_table
    }


def test_historical_ingest_updates_indicator_tail_without_pre_start_rows(
    tmp_path: Path,
) -> None:
    module = _load_script("ingest_daily")

    class _FactorAdapter(_ScriptAdapter):
        def adj_factor(
            self,
            *,
            ts_codes: list[str],
            start: date,
            end: date,
        ) -> pd.DataFrame:
            assert (start, end) == (date(2020, 1, 2), date(2020, 1, 2))
            return pd.DataFrame(
                [
                    {
                        "ts_code": ts_codes[0],
                        "trade_date": date(2020, 1, 2),
                        "adj_factor": 1.0,
                    }
                ]
            )

    db_path = tmp_path / "indicator-scope.duckdb"
    with DuckDBStore(db_path) as store:
        store._conn.execute(
            """
            INSERT INTO daily_bar (
                ts_code, trade_date, open, high, low, close, pre_close,
                change, pct_chg, vol, amount
            ) VALUES
                (?, DATE '2019-12-31', 9, 9, 9, 9, 9, 0, 0, 1, 1),
                (?, DATE '2020-01-03', 11, 11, 11, 11, 10, 1, 10, 1, 1)
            """,
            [INJECTED_CODE, INJECTED_CODE],
        )
        store._conn.execute(
            """
            INSERT INTO adj_factor (ts_code, trade_date, adj_factor) VALUES
                (?, DATE '2019-12-31', 1),
                (?, DATE '2020-01-03', 1)
            """,
            [INJECTED_CODE, INJECTED_CODE],
        )
        store._conn.execute(
            """
            INSERT INTO daily_indicator (
                ts_code, trade_date, ma5, rsi14
            ) VALUES (?, DATE '2020-01-03', -1, -1)
            """,
            [INJECTED_CODE],
        )
    compute_inputs: list[list[date]] = []

    def compute(frame: pd.DataFrame) -> pd.DataFrame:
        dates = pd.to_datetime(frame["trade_date"]).dt.date.tolist()
        compute_inputs.append(dates)
        return pd.DataFrame(
            [
                {
                    "ts_code": INJECTED_CODE,
                    "trade_date": day,
                    **{
                        column: float(day.day)
                        for column in (
                            "ma5",
                            "ma10",
                            "ma20",
                            "ma60",
                            "rsi6",
                            "rsi14",
                            "macd",
                            "macd_signal",
                            "macd_hist",
                            "kdj_k",
                            "kdj_d",
                            "kdj_j",
                        )
                    },
                }
                for day in (
                    date(2019, 12, 31),
                    date(2020, 1, 2),
                    date(2020, 1, 3),
                )
            ]
        )

    result = module.run_historical_ingest(
        [INJECTED_CODE],
        date(2020, 1, 2),
        date(2020, 1, 2),
        adapter=_FactorAdapter(),
        store_factory=lambda: DuckDBStore(db_path),
        ingested_at=INGESTED_AT,
        status_request_interval_seconds=0,
        sleep=lambda _: None,
        compute=compute,
    )

    assert result == 0
    assert compute_inputs == [
        [date(2019, 12, 31), date(2020, 1, 2), date(2020, 1, 3)]
    ]
    with DuckDBStore(db_path, read_only=True) as store:
        indicators = store._conn.execute(
            """
            SELECT trade_date, ma5, rsi14
            FROM daily_indicator
            ORDER BY trade_date
            """
        ).fetchall()
    assert indicators == [
        (date(2020, 1, 2), 2.0, 2.0),
        (date(2020, 1, 3), 3.0, 3.0),
    ]


@pytest.mark.parametrize("failure_point", ["indicator", "state"])
def test_historical_ingest_failure_rolls_back_all_requested_writes(
    tmp_path: Path,
    failure_point: str,
) -> None:
    module = _load_script("ingest_daily")

    class _CompleteAdapter(_ScriptAdapter):
        def adj_factor(
            self,
            *,
            ts_codes: list[str],
            start: date,
            end: date,
        ) -> pd.DataFrame:
            del start, end
            return pd.DataFrame(
                [
                    {
                        "ts_code": ts_codes[0],
                        "trade_date": date(2020, 1, 2),
                        "adj_factor": 1.0,
                    }
                ]
            )

        def daily_basic(
            self,
            *,
            ts_codes: list[str],
            trade_date: date,
        ) -> pd.DataFrame:
            return pd.DataFrame(
                [
                    {
                        "ts_code": ts_codes[0],
                        "trade_date": trade_date,
                        "turnover_rate": 1.0,
                        "volume_ratio": 1.0,
                        "total_mv": 100.0,
                        "circ_mv": 80.0,
                    }
                ]
            )

    class _FailingStore(DuckDBStore):
        def upsert_indicators(self, frame: pd.DataFrame) -> int:
            rows = super().upsert_indicators(frame)
            if failure_point == "indicator":
                raise RuntimeError("injected indicator failure")
            return rows

        def upsert_state(self, frame: pd.DataFrame) -> int:
            rows = super().upsert_state(frame)
            if failure_point == "state":
                raise RuntimeError("injected state failure")
            return rows

    def compute(_: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "ts_code": INJECTED_CODE,
                    "trade_date": date(2020, 1, 2),
                    **{
                        column: 1.0
                        for column in (
                            "ma5",
                            "ma10",
                            "ma20",
                            "ma60",
                            "rsi6",
                            "rsi14",
                            "macd",
                            "macd_signal",
                            "macd_hist",
                            "kdj_k",
                            "kdj_d",
                            "kdj_j",
                        )
                    },
                }
            ]
        )

    db_path = tmp_path / f"atomic-{failure_point}.duckdb"
    with pytest.raises(RuntimeError, match=f"injected {failure_point} failure"):
        module.run_historical_ingest(
            [INJECTED_CODE],
            date(2020, 1, 2),
            date(2020, 1, 2),
            adapter=_CompleteAdapter(),
            store_factory=lambda: _FailingStore(db_path),
            ingested_at=INGESTED_AT,
            status_request_interval_seconds=0,
            sleep=lambda _: None,
            compute=compute,
        )

    with DuckDBStore(db_path, read_only=True) as store:
        counts = {
            table: store._conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in (
                "stock_status_daily",
                "daily_bar",
                "adj_factor",
                "stock_basic",
                "daily_basic",
                "daily_indicator",
                "daily_state",
            )
        }
    assert counts == {table: 0 for table in counts}


def test_historical_ingest_correction_recomputes_future_state_tail(
    tmp_path: Path,
) -> None:
    module = _load_script("ingest_daily")

    class _CorrectionAdapter(_ScriptAdapter):
        def daily(
            self,
            *,
            ts_codes: list[str],
            start: date,
            end: date,
        ) -> pd.DataFrame:
            frame = super().daily(ts_codes=ts_codes, start=start, end=end)
            frame[["open", "high", "low", "close"]] = 11.0
            return frame

    db_path = tmp_path / "historical-state-tail.duckdb"
    with DuckDBStore(db_path) as store:
        store._conn.execute(
            """
            INSERT INTO trade_calendar (
                exchange, cal_date, is_open, pretrade_date, source, updated_at
            ) VALUES
                ('SSE', DATE '2020-01-02', TRUE, DATE '2019-12-31',
                 'test', TIMESTAMPTZ '2020-01-03 08:00:00+00'),
                ('SSE', DATE '2020-01-03', TRUE, DATE '2020-01-02',
                 'test', TIMESTAMPTZ '2020-01-03 08:00:00+00')
            """
        )
        store._conn.execute(
            """
            INSERT INTO daily_bar (
                ts_code, trade_date, open, high, low, close, pre_close,
                change, pct_chg, vol, amount
            ) VALUES
                (?, DATE '2019-12-31', 10, 10, 10, 10, 10, 0, 0, 1, 1),
                (?, DATE '2020-01-02', 10, 10, 10, 10, 10, 0, 0, 1, 1),
                (?, DATE '2020-01-03', 12.1, 12.1, 12.1, 12.1, 11,
                 1.1, 10, 1, 1)
            """,
            [INJECTED_CODE, INJECTED_CODE, INJECTED_CODE],
        )
        store._conn.execute(
            """
            INSERT INTO daily_state (
                ts_code, trade_date, is_limit_up, is_first_limit_up,
                consecutive_limit_ups
            ) VALUES
                (?, DATE '2019-12-31', FALSE, FALSE, 0),
                (?, DATE '2020-01-02', FALSE, FALSE, 0),
                (?, DATE '2020-01-03', TRUE, TRUE, 1)
            """,
            [INJECTED_CODE, INJECTED_CODE, INJECTED_CODE],
        )
        store.upsert_stock_status(
            (
                SecurityStatusDaily(
                    ts_code=INJECTED_CODE,
                    trade_date=date(2020, 1, 3),
                    name="历史正常名",
                    is_st=False,
                    name_source="tushare.namechange",
                    st_source="tushare.namechange",
                    available_at=datetime(2020, 1, 3, 9, 25, tzinfo=SHANGHAI),
                    ingested_at=INGESTED_AT,
                ),
            )
        )

    result = module.run_historical_ingest(
        [INJECTED_CODE],
        date(2020, 1, 2),
        date(2020, 1, 2),
        adapter=_CorrectionAdapter(),
        store_factory=lambda: DuckDBStore(db_path),
        ingested_at=INGESTED_AT,
        status_request_interval_seconds=0,
        sleep=lambda _: None,
        compute=lambda _: pd.DataFrame(),
    )

    assert result == 0
    with DuckDBStore(db_path, read_only=True) as store:
        tail = store._conn.execute(
            """
            SELECT trade_date, is_limit_up, is_first_limit_up,
                   consecutive_limit_ups
            FROM daily_state
            WHERE ts_code = ? AND trade_date >= DATE '2020-01-02'
            ORDER BY trade_date
            """,
            [INJECTED_CODE],
        ).fetchall()
    assert tail == [
        (date(2020, 1, 2), True, True, 1),
        (date(2020, 1, 3), True, False, 2),
    ]


def test_market_backfill_script_calls_ingest_without_preopened_store() -> None:
    module = _load_script("backfill_market")
    calls: list[tuple[str, str]] = []

    def ingest(trade_date: str, *, state_mode: str) -> int:
        calls.append((trade_date, state_mode))
        return 123

    result = module.ingest_backfill_date("2024-01-02", ingest=ingest)

    assert result == 123
    assert calls == [("2024-01-02", "invalidate_tail")]


def test_market_backfill_main_finalizes_once_after_all_dates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_script("backfill_market")
    dates = ["2024-01-03", "2024-01-02", "2024-01-01"]
    ingested: list[str] = []
    finalized: list[tuple[str, ...]] = []

    class _Store:
        def __enter__(self) -> _Store:
            return self

        def __exit__(self, *_: object) -> None:
            return None

    class _Progress:
        def __init__(self, **_: object) -> None:
            return None

        def set_description(self, _: str) -> None:
            return None

        def update(self, _: int) -> None:
            return None

        def close(self) -> None:
            return None

        @staticmethod
        def write(_: str) -> None:
            return None

    monkeypatch.setattr(module, "DuckDBStore", _Store)
    monkeypatch.setattr(module, "get_dates_to_backfill", lambda *_: dates)
    monkeypatch.setattr(
        module,
        "ingest_backfill_date",
        lambda day: ingested.append(day) or 1,
    )
    monkeypatch.setattr(
        module,
        "finalize_backfill_state",
        lambda successful: finalized.append(tuple(successful)) or (3, 3),
        raising=False,
    )
    monkeypatch.setattr(module, "tqdm", _Progress)
    monkeypatch.setattr(module.logger, "remove", lambda: None)
    monkeypatch.setattr(module.logger, "add", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        module.sys,
        "argv",
        ["backfill_market.py", "--sleep", "0"],
    )

    result = module.main()

    assert result == 0
    assert ingested == dates
    assert finalized == [tuple(dates)]


def test_finalize_market_backfill_recomputes_state_and_sentiment_once(
    tmp_path: Path,
) -> None:
    module = _load_script("backfill_market")
    db_path = tmp_path / "batch-finalize.duckdb"
    with DuckDBStore(db_path) as store:
        store._conn.execute(
            """
            INSERT INTO daily_bar (ts_code, trade_date, close) VALUES
                ('600000.SH', DATE '2024-01-02', 10),
                ('600000.SH', DATE '2024-01-03', 11),
                ('000001.SZ', DATE '2024-01-03', 5)
            """
        )
    state_calls: list[tuple[list[str], str]] = []
    sentiment_calls: list[tuple[date, date]] = []

    def recompute(
        store: DuckDBStore,
        codes: list[str] | None,
        *,
        status_mode: str,
    ) -> int:
        del store
        assert codes is not None
        state_calls.append((codes, status_mode))
        return 3

    def recompute_sentiment(
        start_date: date,
        end_date: date,
        *,
        store: DuckDBStore,
    ) -> int:
        del store
        sentiment_calls.append((start_date, end_date))
        return 2

    result = module.finalize_backfill_state(
        ["2024-01-03", "2024-01-02"],
        store_factory=lambda: DuckDBStore(db_path),
        recompute=recompute,
        recompute_sentiment=recompute_sentiment,
    )

    assert result == (3, 2)
    assert state_calls == [
        (["000001.SZ", "600000.SH"], "verified_no_fetch")
    ]
    assert sentiment_calls == [(date(2024, 1, 2), date(2024, 1, 3))]
