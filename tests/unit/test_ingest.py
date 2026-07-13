"""每日 ingest 的历史证券状态编排测试。"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import pytest

import rquant.ingest as ingest_module
from rquant.ingest import ingest_daily
from rquant.security_status import (
    SHANGHAI,
    SecurityStatusDaily,
    SecurityStatusWriteConflictError,
)
from rquant.storage.duckdb import DuckDBStore

INGESTED_AT = datetime(2024, 1, 2, 8, 0, tzinfo=UTC)


class _FakeDailyPro:
    def __init__(
        self,
        *,
        close: float = 10.5,
        high: float | None = None,
        include_daily_basic: bool = False,
    ) -> None:
        self.close = close
        self.high = high if high is not None else max(close, 10.5)
        self.include_daily_basic = include_daily_basic

    def stock_basic(self, **kwargs: object) -> pd.DataFrame:
        del kwargs
        return pd.DataFrame(
            [
                {
                    "ts_code": "600000.SH",
                    "symbol": "600000",
                    "name": "*ST当前名称",
                    "area": "上海",
                    "industry": "银行",
                    "list_date": "19991110",
                    "market": "主板",
                }
            ]
        )

    def daily(self, **kwargs: object) -> pd.DataFrame:
        assert kwargs == {"trade_date": "20240102"}
        return pd.DataFrame(
            [
                {
                    "ts_code": "600000.SH",
                    "trade_date": "20240102",
                    "open": 10.1,
                    "high": self.high,
                    "low": 10.0,
                    "close": self.close,
                    "pre_close": 10.0,
                    "change": 0.5,
                    "pct_chg": 5.0,
                    "vol": 1000.0,
                    "amount": 10500.0,
                }
            ]
        )

    def index_daily(self, **kwargs: object) -> pd.DataFrame:
        del kwargs
        return pd.DataFrame()

    def adj_factor(self, **kwargs: object) -> pd.DataFrame:
        del kwargs
        return pd.DataFrame()

    def daily_basic(self, **kwargs: object) -> pd.DataFrame:
        del kwargs
        if self.include_daily_basic:
            return pd.DataFrame(
                [
                    {
                        "ts_code": "600000.SH",
                        "trade_date": "20240102",
                        "turnover_rate": 1.0,
                        "volume_ratio": 1.0,
                        "total_mv": 100.0,
                        "circ_mv": 80.0,
                    }
                ]
            )
        return pd.DataFrame()


class _StatusAdapter:
    def __init__(
        self,
        db_path: Path,
        *,
        fail: bool = False,
        available_name: str = "浦发银行",
        ann_date: str = "19991110",
        expected_daily_count: int = 0,
        expected_state_count: int = 0,
    ) -> None:
        self.db_path = db_path
        self.fail = fail
        self.available_name = available_name
        self.ann_date = ann_date
        self.expected_daily_count = expected_daily_count
        self.expected_state_count = expected_state_count
        self.namechange_calls: list[tuple[date, date, str | None]] = []
        self.stock_st_calls: list[date] = []

    def namechange_raw(
        self,
        start_date: date,
        end_date: date,
        ts_code: str | None = None,
    ) -> pd.DataFrame:
        with DuckDBStore(self.db_path, read_only=True) as store:
            assert store.count_daily("600000.SH") == self.expected_daily_count
            assert store.count_state("600000.SH") == self.expected_state_count
        self.namechange_calls.append((start_date, end_date, ts_code))
        if self.fail:
            raise RuntimeError("namechange unavailable")
        return pd.DataFrame(
            [
                {
                    "ts_code": "600000.SH",
                    "name": self.available_name,
                    "start_date": "19991110",
                    "end_date": None,
                    "ann_date": self.ann_date,
                    "change_reason": "上市",
                }
            ]
        )

    def stock_st_raw(self, trade_date: date) -> pd.DataFrame:
        self.stock_st_calls.append(trade_date)
        return pd.DataFrame(
            columns=["ts_code", "name", "trade_date", "type", "type_name"]
        )


class _WriterFactory:
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
    path = tmp_path / "ingest.duckdb"
    with DuckDBStore(path):
        pass
    return path


def _no_sleep(_: float) -> None:
    return None


def _run_ingest(
    db_path: Path,
    status_adapter: _StatusAdapter,
    *,
    sleeper: Callable[[float], None] = _no_sleep,
    pro: _FakeDailyPro | None = None,
    writer_factory: Callable[[], DuckDBStore] | None = None,
) -> int:
    return ingest_daily(
        "2024-01-02",
        pro=pro or _FakeDailyPro(),
        status_adapter=status_adapter,
        writer_factory=writer_factory or _WriterFactory(db_path),
        ingested_at=INGESTED_AT,
        api_sleep=0,
        sleep=sleeper,
    )


def test_ingest_prefetches_status_and_ignores_current_stock_name(
    db_path: Path,
) -> None:
    status_adapter = _StatusAdapter(db_path)

    rows = _run_ingest(db_path, status_adapter)

    assert rows == 1
    assert status_adapter.namechange_calls
    assert status_adapter.stock_st_calls == [date(2024, 1, 2)]
    with DuckDBStore(db_path, read_only=True) as store:
        status = store.list_stock_status(date(2024, 1, 2), date(2024, 1, 2))
        state = store.get_state("600000.SH").iloc[0]
    assert len(status) == 1
    assert status[0].is_st is False
    assert state["is_st"] == False  # noqa: E712
    assert state["limit_pct"] == pytest.approx(0.10)
    assert state["is_limit_up"] == False  # noqa: E712


def test_ingest_source_failure_happens_before_any_database_mutation(
    db_path: Path,
) -> None:
    status_adapter = _StatusAdapter(db_path, fail=True)

    with pytest.raises(RuntimeError, match="namechange unavailable"):
        _run_ingest(db_path, status_adapter)

    with DuckDBStore(db_path, read_only=True) as store:
        assert store.count_daily("600000.SH") == 0
        assert store.count_state("600000.SH") == 0
        assert store.list_stock_status(date(2024, 1, 2), date(2024, 1, 2)) == []


def test_ingest_late_announcement_keeps_same_day_state_unknown(
    db_path: Path,
) -> None:
    status_adapter = _StatusAdapter(
        db_path,
        available_name="*ST迟报",
        ann_date="20240103",
    )

    _run_ingest(db_path, status_adapter)

    with DuckDBStore(db_path, read_only=True) as store:
        status = store.list_stock_status(date(2024, 1, 2), date(2024, 1, 2))[0]
        state = store.get_state("600000.SH").iloc[0]
    assert status.is_st is True
    assert pd.isna(state["is_st"])
    assert pd.isna(state["limit_pct"])
    assert pd.isna(state["is_limit_up"])


def test_ingest_target_date_update_preserves_older_daily_state(
    db_path: Path,
) -> None:
    with DuckDBStore(db_path) as store:
        store._conn.execute(
            """
            INSERT INTO daily_bar (
                ts_code, trade_date, open, high, low, close, pre_close
            ) VALUES ('600000.SH', DATE '2023-12-29', 9.8, 10.1, 9.7, 10, 9.9)
            """
        )
        store._conn.execute(
            """
            INSERT INTO daily_state (
                ts_code, trade_date, is_st, limit_pct, is_limit_up,
                is_first_limit_up, consecutive_limit_ups, body_upper, body_lower
            ) VALUES (
                '600000.SH', DATE '2023-12-29', FALSE, 0.1, FALSE,
                FALSE, 0, 10, 9.8
            )
            """
        )
        before = store._conn.execute(
            "SELECT * FROM daily_state WHERE ts_code = ? AND trade_date = ?",
            ["600000.SH", date(2023, 12, 29)],
        ).fetchone()

    _run_ingest(
        db_path,
        _StatusAdapter(
            db_path,
            expected_daily_count=1,
            expected_state_count=1,
        ),
    )

    with DuckDBStore(db_path, read_only=True) as store:
        after = store.get_state("600000.SH").sort_values("trade_date").reset_index(drop=True)
        old_after = after[
            pd.to_datetime(after["trade_date"]).dt.date == date(2023, 12, 29)
        ].reset_index(drop=True)
        stored_old_after = store._conn.execute(
            "SELECT * FROM daily_state WHERE ts_code = ? AND trade_date = ?",
            ["600000.SH", date(2023, 12, 29)],
        ).fetchone()
    assert len(old_after) == 1
    assert stored_old_after == before
    assert pd.to_datetime(after["trade_date"]).dt.date.tolist() == [
        date(2023, 12, 29),
        date(2024, 1, 2),
    ]


def test_ingest_failed_status_rerun_keeps_old_bar_and_target_state_coherent(
    db_path: Path,
) -> None:
    _run_ingest(db_path, _StatusAdapter(db_path))
    with DuckDBStore(db_path, read_only=True) as store:
        old_bar = store._conn.execute(
            "SELECT close FROM daily_bar WHERE ts_code = ? AND trade_date = ?",
            ["600000.SH", date(2024, 1, 2)],
        ).fetchone()[0]
        old_state = store.get_state("600000.SH").copy()

    failing = _StatusAdapter(
        db_path,
        fail=True,
        expected_daily_count=1,
        expected_state_count=1,
    )
    with pytest.raises(RuntimeError, match="namechange unavailable"):
        _run_ingest(db_path, failing, pro=_FakeDailyPro(close=8.0))

    with DuckDBStore(db_path, read_only=True) as store:
        new_bar = store._conn.execute(
            "SELECT close FROM daily_bar WHERE ts_code = ? AND trade_date = ?",
            ["600000.SH", date(2024, 1, 2)],
        ).fetchone()[0]
        new_state = store.get_state("600000.SH")
    assert new_bar == old_bar == pytest.approx(10.5)
    pd.testing.assert_frame_equal(new_state, old_state)


def test_ingest_target_limit_chain_is_unknown_with_incomplete_predecessor(
    db_path: Path,
) -> None:
    with DuckDBStore(db_path) as store:
        store._conn.execute(
            """
            INSERT INTO daily_bar (
                ts_code, trade_date, open, high, low, close, pre_close
            ) VALUES ('600000.SH', DATE '2023-12-29', 9.8, 10, 9.7, 10, NULL)
            """
        )

    _run_ingest(
        db_path,
        _StatusAdapter(db_path, expected_daily_count=1),
        pro=_FakeDailyPro(close=11.0, high=11.0),
    )

    with DuckDBStore(db_path, read_only=True) as store:
        state = store.get_state("600000.SH")
    target = state.iloc[-1]
    assert bool(target["is_limit_up"])
    assert pd.isna(target["is_first_limit_up"])
    assert pd.isna(target["consecutive_limit_ups"])


def test_ingest_opens_production_writer_only_after_all_remote_fetches(
    db_path: Path,
) -> None:
    writer_factory = _WriterFactory(db_path)

    class _AssertingPro(_FakeDailyPro):
        def _assert_unlocked(self) -> None:
            assert writer_factory.calls == 0
            with DuckDBStore(db_path, read_only=True):
                pass

        def stock_basic(self, **kwargs: object) -> pd.DataFrame:
            self._assert_unlocked()
            return super().stock_basic(**kwargs)

        def daily(self, **kwargs: object) -> pd.DataFrame:
            self._assert_unlocked()
            return super().daily(**kwargs)

        def index_daily(self, **kwargs: object) -> pd.DataFrame:
            self._assert_unlocked()
            return super().index_daily(**kwargs)

        def adj_factor(self, **kwargs: object) -> pd.DataFrame:
            self._assert_unlocked()
            return super().adj_factor(**kwargs)

        def daily_basic(self, **kwargs: object) -> pd.DataFrame:
            self._assert_unlocked()
            return super().daily_basic(**kwargs)

    class _AssertingStatusAdapter:
        def namechange_raw(
            self,
            start_date: date,
            end_date: date,
            ts_code: str | None = None,
        ) -> pd.DataFrame:
            del start_date, end_date, ts_code
            assert writer_factory.calls == 0
            with DuckDBStore(db_path, read_only=True):
                pass
            return pd.DataFrame(
                [
                    {
                        "ts_code": "600000.SH",
                        "name": "浦发银行",
                        "start_date": "19991110",
                        "end_date": None,
                        "ann_date": "19991110",
                        "change_reason": "上市",
                    }
                ]
            )

        def stock_st_raw(self, trade_date: date) -> pd.DataFrame:
            del trade_date
            assert writer_factory.calls == 0
            with DuckDBStore(db_path, read_only=True):
                pass
            return pd.DataFrame(
                columns=["ts_code", "name", "trade_date", "type", "type_name"]
            )

    rows = ingest_daily(
        "2024-01-02",
        pro=_AssertingPro(),
        status_adapter=_AssertingStatusAdapter(),
        writer_factory=writer_factory,
        ingested_at=INGESTED_AT,
        api_sleep=0,
        sleep=_no_sleep,
    )

    assert rows == 1
    assert writer_factory.calls == 1


def _seed_target_bar_and_state(db_path: Path) -> tuple[object, object]:
    with DuckDBStore(db_path) as store:
        store._conn.execute(
            """
            INSERT INTO daily_bar (
                ts_code, trade_date, open, high, low, close, pre_close,
                change, pct_chg, vol, amount
            ) VALUES (
                '600000.SH', DATE '2024-01-02', 10.1, 10.5, 10, 10.5, 10,
                0.5, 5, 1000, 10500
            )
            """
        )
        store._conn.execute(
            """
            INSERT INTO daily_state (
                ts_code, trade_date, is_st, is_bj, board_type, limit_pct,
                limit_up_price, limit_down_price, is_limit_up, is_limit_down,
                is_first_limit_up, is_yiziban, consecutive_limit_ups,
                body_upper, body_lower
            ) VALUES (
                '600000.SH', DATE '2024-01-02', FALSE, FALSE, 'main', 0.1,
                11, 9, FALSE, FALSE, FALSE, FALSE, 0, 10.5, 10.1
            )
            """
        )
        bar = store._conn.execute(
            "SELECT * FROM daily_bar WHERE ts_code = ? AND trade_date = ?",
            ["600000.SH", date(2024, 1, 2)],
        ).fetchone()
        state = store._conn.execute(
            "SELECT * FROM daily_state WHERE ts_code = ? AND trade_date = ?",
            ["600000.SH", date(2024, 1, 2)],
        ).fetchone()
    return bar, state


def test_ingest_status_conflict_leaves_old_bar_and_state_intact(
    db_path: Path,
) -> None:
    old_bar, old_state = _seed_target_bar_and_state(db_path)
    existing_status = SecurityStatusDaily(
        ts_code="600000.SH",
        trade_date=date(2024, 1, 2),
        name="*ST旧状态",
        is_st=True,
        name_source="tushare.namechange",
        st_source="tushare.namechange",
        available_at=datetime(2024, 1, 2, 9, 25, tzinfo=SHANGHAI),
        ingested_at=INGESTED_AT,
    )
    with DuckDBStore(db_path) as store:
        store.upsert_stock_status((existing_status,))

    with pytest.raises(SecurityStatusWriteConflictError):
        _run_ingest(
            db_path,
            _StatusAdapter(
                db_path,
                expected_daily_count=1,
                expected_state_count=1,
            ),
            pro=_FakeDailyPro(close=8.0),
        )

    with DuckDBStore(db_path, read_only=True) as store:
        bar = store._conn.execute(
            "SELECT * FROM daily_bar WHERE ts_code = ? AND trade_date = ?",
            ["600000.SH", date(2024, 1, 2)],
        ).fetchone()
        state = store._conn.execute(
            "SELECT * FROM daily_state WHERE ts_code = ? AND trade_date = ?",
            ["600000.SH", date(2024, 1, 2)],
        ).fetchone()
    assert bar == old_bar
    assert state == old_state


def test_ingest_rolls_back_bar_and_state_when_atomic_apply_fails(
    db_path: Path,
) -> None:
    old_bar, old_state = _seed_target_bar_and_state(db_path)
    old_status = SecurityStatusDaily(
        ts_code="600000.SH",
        trade_date=date(2024, 1, 2),
        name="*ST旧状态",
        is_st=True,
        name_source="tushare.namechange",
        st_source="tushare.namechange",
        available_at=datetime(2024, 1, 2, 9, 25, tzinfo=SHANGHAI),
        ingested_at=INGESTED_AT - pd.Timedelta(seconds=1),
    )
    with DuckDBStore(db_path) as store:
        store.upsert_stock_status((old_status,))

    class _FailingStore(DuckDBStore):
        def upsert_daily_basic(self, df: pd.DataFrame) -> int:
            del df
            raise RuntimeError("daily_basic write failed")

    with pytest.raises(RuntimeError, match="daily_basic write failed"):
        _run_ingest(
            db_path,
            _StatusAdapter(
                db_path,
                expected_daily_count=1,
                expected_state_count=1,
            ),
            pro=_FakeDailyPro(close=8.0, include_daily_basic=True),
            writer_factory=_WriterFactory(db_path, _FailingStore),
        )

    with DuckDBStore(db_path, read_only=True) as store:
        bar = store._conn.execute(
            "SELECT * FROM daily_bar WHERE ts_code = ? AND trade_date = ?",
            ["600000.SH", date(2024, 1, 2)],
        ).fetchone()
        state = store._conn.execute(
            "SELECT * FROM daily_state WHERE ts_code = ? AND trade_date = ?",
            ["600000.SH", date(2024, 1, 2)],
        ).fetchone()
        status = store.list_stock_status(date(2024, 1, 2), date(2024, 1, 2))
    assert bar == old_bar
    assert state == old_state
    assert status == [old_status]


def test_ingest_older_date_recomputes_existing_future_state_tail(
    db_path: Path,
) -> None:
    with DuckDBStore(db_path) as store:
        store._conn.execute(
            """
            INSERT INTO trade_calendar (
                exchange, cal_date, is_open, pretrade_date, source, updated_at
            ) VALUES
                ('SSE', DATE '2024-01-02', TRUE, DATE '2023-12-29',
                 'test', TIMESTAMPTZ '2024-01-03 08:00:00+00'),
                ('SSE', DATE '2024-01-03', TRUE, DATE '2024-01-02',
                 'test', TIMESTAMPTZ '2024-01-03 08:00:00+00')
            """
        )
        store._conn.execute(
            """
            INSERT INTO daily_bar (
                ts_code, trade_date, open, high, low, close, pre_close,
                change, pct_chg, vol, amount
            ) VALUES
                ('600000.SH', DATE '2023-12-29', 10, 10, 10, 10, 10,
                 0, 0, 1, 1),
                ('600000.SH', DATE '2024-01-02', 10, 10, 10, 10, 10,
                 0, 0, 1, 1),
                ('600000.SH', DATE '2024-01-03', 11, 11, 11, 11, 10,
                 1, 10, 1, 1)
            """
        )
        store._conn.execute(
            """
            INSERT INTO daily_state (
                ts_code, trade_date, is_limit_up, is_first_limit_up,
                consecutive_limit_ups
            ) VALUES
                ('600000.SH', DATE '2023-12-29', FALSE, FALSE, 0),
                ('600000.SH', DATE '2024-01-02', FALSE, FALSE, 0),
                ('600000.SH', DATE '2024-01-03', TRUE, TRUE, 1)
            """
        )
        store.upsert_stock_status(
            (
                SecurityStatusDaily(
                    ts_code="600000.SH",
                    trade_date=date(2024, 1, 3),
                    name="浦发银行",
                    is_st=False,
                    name_source="tushare.namechange",
                    st_source="tushare.namechange",
                    available_at=datetime(
                        2024, 1, 3, 9, 25, tzinfo=SHANGHAI
                    ),
                    ingested_at=INGESTED_AT,
                ),
            )
        )

    _run_ingest(
        db_path,
        _StatusAdapter(
            db_path,
            expected_daily_count=3,
            expected_state_count=3,
        ),
        pro=_FakeDailyPro(close=11.0, high=11.0),
    )

    with DuckDBStore(db_path, read_only=True) as store:
        state = store.get_state("600000.SH")
    tail = state.loc[
        pd.to_datetime(state["trade_date"]).dt.date >= date(2024, 1, 2)
    ].reset_index(drop=True)
    assert tail["is_limit_up"].tolist() == [True, True]
    assert tail["is_first_limit_up"].tolist() == [True, False]
    assert tail["consecutive_limit_ups"].tolist() == [1, 2]


def test_target_state_input_query_is_bounded_to_one_row_per_5000_codes(
    db_path: Path,
) -> None:
    codes = [f"{index:06d}.SZ" for index in range(5000)]
    with DuckDBStore(db_path) as store:
        code_frame = pd.DataFrame({"ts_code": codes})
        store._conn.register("code_fixture", code_frame)
        store._conn.execute(
            """
            INSERT INTO daily_bar (
                ts_code, trade_date, open, high, low, close, pre_close
            )
            SELECT ts_code, day, 10, 10.5, 9.9, 10, 10
            FROM code_fixture
            CROSS JOIN (
                VALUES (DATE '2023-12-28'), (DATE '2023-12-29'),
                       (DATE '2024-01-02')
            ) AS history(day)
            """
        )
        store._conn.execute(
            """
            INSERT INTO daily_state (
                ts_code, trade_date, is_limit_up, consecutive_limit_ups
            )
            SELECT ts_code, DATE '2023-12-29', FALSE, 0
            FROM code_fixture
            """
        )
        store._conn.execute(
            """
            INSERT INTO trade_calendar (
                exchange, cal_date, is_open, pretrade_date, source, updated_at
            ) VALUES (
                'SSE', DATE '2024-01-02', TRUE, DATE '2023-12-29',
                'test', TIMESTAMPTZ '2024-01-02 08:00:00+00'
            )
            """
        )
        store._conn.unregister("code_fixture")

        target_rows, seeds = ingest_module._load_target_daily_state_inputs(
            store,
            date(2024, 1, 2),
            codes,
        )

    assert len(target_rows) == 5000
    assert set(pd.to_datetime(target_rows["trade_date"]).dt.date) == {
        date(2024, 1, 2)
    }
    assert len(seeds) == 5000
    assert all(seed.trade_date == date(2023, 12, 29) for seed in seeds.values())


def test_target_state_seed_uses_each_codes_first_actual_bar_after_start(
    db_path: Path,
) -> None:
    codes = ["600000.SH", "000001.SZ"]
    with DuckDBStore(db_path) as store:
        store._conn.execute(
            """
            INSERT INTO daily_bar (
                ts_code, trade_date, open, high, low, close, pre_close
            ) VALUES
                ('600000.SH', DATE '2023-12-29', 10, 10, 10, 10, 10),
                ('600000.SH', DATE '2024-01-02', 10, 10, 10, 10, 10),
                ('000001.SZ', DATE '2023-12-28', 5, 5, 5, 5, 5),
                ('000001.SZ', DATE '2024-01-03', 5, 5, 5, 5, 5)
            """
        )
        store._conn.execute(
            """
            INSERT INTO daily_state (
                ts_code, trade_date, is_limit_up, consecutive_limit_ups
            ) VALUES
                ('600000.SH', DATE '2023-12-29', FALSE, 0),
                ('000001.SZ', DATE '2023-12-28', TRUE, 2)
            """
        )
        store._conn.execute(
            """
            INSERT INTO trade_calendar (
                exchange, cal_date, is_open, pretrade_date, source, updated_at
            ) VALUES
                ('SSE', DATE '2024-01-02', TRUE, DATE '2023-12-29',
                 'test', TIMESTAMPTZ '2024-01-03 08:00:00+00'),
                ('SSE', DATE '2024-01-03', TRUE, DATE '2024-01-02',
                 'test', TIMESTAMPTZ '2024-01-03 08:00:00+00')
            """
        )

        rows, seeds = ingest_module._load_target_daily_state_inputs(
            store,
            date(2024, 1, 1),
            codes,
        )

    first_dates = (
        rows.groupby("ts_code")["trade_date"]
        .min()
        .map(lambda value: pd.Timestamp(value).date())
        .to_dict()
    )
    assert first_dates == {
        "000001.SZ": date(2024, 1, 3),
        "600000.SH": date(2024, 1, 2),
    }
    assert seeds["600000.SH"].trade_date == date(2023, 12, 29)
    assert seeds["000001.SZ"].trade_date == date(2023, 12, 28)


def test_ingest_invalidate_tail_mode_skips_state_and_sentiment(
    db_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with DuckDBStore(db_path) as store:
        store._conn.execute(
            """
            INSERT INTO daily_bar (
                ts_code, trade_date, open, high, low, close, pre_close
            ) VALUES
                ('600000.SH', DATE '2024-01-02', 10, 10, 10, 10, 10),
                ('600000.SH', DATE '2024-01-03', 11, 11, 11, 11, 10)
            """
        )
        store._conn.execute(
            """
            INSERT INTO daily_state (
                ts_code, trade_date, is_limit_up, consecutive_limit_ups
            ) VALUES
                ('600000.SH', DATE '2024-01-02', FALSE, 0),
                ('600000.SH', DATE '2024-01-03', TRUE, 1)
            """
        )

    monkeypatch.setattr(
        ingest_module,
        "sync_market_sentiment",
        lambda *_: pytest.fail("sentiment must wait for final state rebuild"),
    )
    rows = ingest_daily(
        "2024-01-02",
        pro=_FakeDailyPro(),
        status_adapter=_StatusAdapter(
            db_path,
            expected_daily_count=2,
            expected_state_count=2,
        ),
        writer_factory=_WriterFactory(db_path),
        ingested_at=INGESTED_AT,
        api_sleep=0,
        sleep=_no_sleep,
        state_mode="invalidate_tail",
    )

    assert rows == 1
    with DuckDBStore(db_path, read_only=True) as store:
        state_dates = store._conn.execute(
            "SELECT trade_date FROM daily_state ORDER BY trade_date"
        ).fetchall()
        sentiment_count = store._conn.execute(
            "SELECT COUNT(*) FROM market_sentiment_daily"
        ).fetchone()[0]
    assert state_dates == []
    assert sentiment_count == 0
