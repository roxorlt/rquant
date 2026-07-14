"""涨停池每日采集测试。"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, cast

import duckdb
import pandas as pd
import pytest

import rquant.limit_up_pool as limit_up_pool
from rquant.limit_up_pool import capture_zt_pool, normalize_zt_pool, to_ts_code
from rquant.storage.duckdb import DuckDBStore
from rquant.trade_calendar import TradeCalendarDay

_CALENDAR_UPDATED_AT = datetime(2026, 7, 1, 16, 0, tzinfo=UTC)


@pytest.fixture()
def store(tmp_path: Path) -> Iterator[DuckDBStore]:
    s = DuckDBStore(tmp_path / "test.duckdb")
    calendar_dates = {date(2026, 7, 2), date.today()}
    s.upsert_trade_calendar([
        TradeCalendarDay(
            exchange="SSE",
            cal_date=cal_date,
            is_open=True,
            source="test",
            updated_at=_CALENDAR_UPDATED_AT,
        )
        for cal_date in calendar_dates
    ])
    yield s
    s.close()


def _raw_zt_pool() -> pd.DataFrame:
    return pd.DataFrame([
        {
            "序号": 1, "代码": "002273", "名称": "水晶光电", "涨跌幅": 10.02,
            "最新价": 20.5, "成交额": 1.5e9, "流通市值": 2.0e10,
            "总市值": 2.5e10, "换手率": 7.5, "封板资金": 3.2e8,
            "首次封板时间": "092500", "最后封板时间": "142000",
            "炸板次数": 1, "涨停统计": "3/2", "连板数": 2,
            "所属行业": "光学光电子",
        },
        {
            "序号": 2, "代码": "600519", "名称": "贵州茅台", "涨跌幅": 10.0,
            "最新价": 1800.0, "成交额": 9.9e9, "流通市值": 2.2e12,
            "总市值": 2.2e12, "换手率": 0.5, "封板资金": 8.8e8,
            # akshare 偶发返回 int 时间：92500 丢首位 0
            "首次封板时间": 92500, "最后封板时间": 145900,
            "炸板次数": 0, "涨停统计": "1/1", "连板数": 1,
            "所属行业": "白酒",
        },
        {
            "序号": 3, "代码": "430047", "名称": "诺思兰德", "涨跌幅": 30.0,
            "最新价": 12.0, "成交额": 1.0e8, "流通市值": 1.0e9,
            "总市值": 1.5e9, "换手率": 15.0, "封板资金": 5.0e7,
            "首次封板时间": "100000", "最后封板时间": "100000",
            "炸板次数": 0, "涨停统计": "1/1", "连板数": 1,
            "所属行业": "生物制品",
        },
    ])


class _RecordingConnection:
    def __init__(self, inner: duckdb.DuckDBPyConnection) -> None:
        self.inner = inner
        self.transaction_commands: list[str] = []

    def execute(self, query: str, *args: object, **kwargs: object) -> Any:
        normalized = " ".join(query.split()).upper()
        if normalized in {"BEGIN", "COMMIT", "ROLLBACK"}:
            self.transaction_commands.append(normalized)
        return self.inner.execute(query, *args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)


class TestToTsCode:
    @pytest.mark.parametrize(
        ("symbol", "expected"),
        [
            ("600000", "600000.SH"),
            ("688981", "688981.SH"),
            ("002273", "002273.SZ"),
            ("000001", "000001.SZ"),
            ("300750", "300750.SZ"),
            ("430047", "430047.BJ"),
            ("839167", "839167.BJ"),
            ("920108", "920108.BJ"),
        ],
    )
    def test_exchange_mapping(self, symbol: str, expected: str) -> None:
        assert to_ts_code(symbol) == expected

    @pytest.mark.parametrize("symbol", ["123456", "12345", "abcdef", "", "60000A"])
    def test_unmappable_returns_none(self, symbol: str) -> None:
        assert to_ts_code(symbol) is None


class TestNormalizeZtPool:
    def test_maps_all_fields(self) -> None:
        trading_date = date(2026, 7, 2)

        df = normalize_zt_pool(_raw_zt_pool(), trading_date)

        assert len(df) == 3
        assert set(df["ts_code"]) == {"002273.SZ", "600519.SH", "430047.BJ"}
        row = df[df["ts_code"] == "002273.SZ"].iloc[0]
        assert row["trade_date"] == trading_date
        assert row["name"] == "水晶光电"
        assert row["pct_chg"] == 10.02
        assert row["close"] == 20.5
        assert row["amount"] == 1.5e9
        assert row["circ_mv"] == 2.0e10
        assert row["total_mv"] == 2.5e10
        assert row["turnover_rate"] == 7.5
        assert row["seal_amount"] == 3.2e8
        assert row["first_seal_time"] == "092500"
        assert row["last_seal_time"] == "142000"
        assert int(row["break_count"]) == 1
        assert row["limit_up_stat"] == "3/2"
        assert int(row["consecutive_boards"]) == 2
        assert row["industry"] == "光学光电子"
        assert row["source"] == "eastmoney"

    def test_int_seal_time_zero_padded(self) -> None:
        df = normalize_zt_pool(_raw_zt_pool(), date(2026, 7, 2))

        row = df[df["ts_code"] == "600519.SH"].iloc[0]
        assert row["first_seal_time"] == "092500"
        assert row["last_seal_time"] == "145900"

    def test_drops_unmappable_symbol(self) -> None:
        raw = _raw_zt_pool()
        raw.loc[0, "代码"] = "123456"

        df = normalize_zt_pool(raw, date(2026, 7, 2))

        assert len(df) == 2
        assert "123456" not in set(df["ts_code"])

    def test_missing_code_column_raises(self) -> None:
        with pytest.raises(ValueError, match="代码"):
            normalize_zt_pool(pd.DataFrame([{"名称": "x"}]), date(2026, 7, 2))

    def test_empty_raw_returns_empty(self) -> None:
        assert normalize_zt_pool(pd.DataFrame(), date(2026, 7, 2)).empty


class TestCaptureZtPool:
    def test_capture_joins_outer_transaction_without_managing_it(
        self,
        store: DuckDBStore,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        trading_date = date(2026, 7, 2)
        monkeypatch.setattr(
            limit_up_pool,
            "_fetch_zt_pool",
            lambda ds: _raw_zt_pool(),
        )
        store._conn.execute("BEGIN")  # noqa: SLF001
        recording = _RecordingConnection(store._conn)  # noqa: SLF001
        store._conn = cast(Any, recording)  # noqa: SLF001

        assert capture_zt_pool(trading_date, store) == 3
        assert recording.transaction_commands == []
        assert store.query_limit_up_pool(trading_date).shape[0] == 3

        store._conn.execute("ROLLBACK")  # noqa: SLF001
        assert store.query_limit_up_pool(trading_date).empty
        guard = store._conn.execute(  # noqa: SLF001
            "SELECT generation FROM limit_up_pool_write_guard "
            "WHERE guard_id = 'limit_up_pool_daily'"
        ).fetchone()
        assert guard == (0,)

    def test_final_calendar_rejection_does_not_roll_back_outer_transaction(
        self,
        store: DuckDBStore,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        trading_date = date(2026, 7, 2)

        def fetch_then_close(ds: str) -> pd.DataFrame:
            store._conn.execute(  # noqa: SLF001
                "UPDATE trade_calendar SET is_open = FALSE, source = 'outer' "
                "WHERE exchange = 'SSE' AND cal_date = ?",
                [trading_date],
            )
            return _raw_zt_pool()

        monkeypatch.setattr(limit_up_pool, "_fetch_zt_pool", fetch_then_close)
        store._conn.execute("BEGIN")  # noqa: SLF001
        recording = _RecordingConnection(store._conn)  # noqa: SLF001
        store._conn = cast(Any, recording)  # noqa: SLF001

        with pytest.raises(
            limit_up_pool.LimitUpPoolCalendarGuardError,
            match="calendar.*changed",
        ):
            capture_zt_pool(trading_date, store)

        assert recording.transaction_commands == []
        store._conn.execute("COMMIT")  # noqa: SLF001
        calendar = store.get_trade_calendar_day("SSE", trading_date)
        issue = store._conn.execute(  # noqa: SLF001
            "SELECT rule_id, severity FROM data_quality_issue WHERE scope_key = ?",
            [trading_date.isoformat()],
        ).fetchone()
        assert calendar is not None
        assert calendar.is_open is False
        assert calendar.source == "outer"
        assert issue == ("limit_up_pool.calendar_changed_during_capture", "P0")
        assert store.query_limit_up_pool(trading_date).empty

    def test_outer_transaction_preserves_business_write_conflict_classification(
        self,
        store: DuckDBStore,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        trading_date = date(2026, 7, 2)
        transaction_modes: list[str] = []
        monkeypatch.setattr(
            limit_up_pool,
            "_fetch_zt_pool",
            lambda ds: _raw_zt_pool(),
        )

        def conflicting_pool_write(
            df: pd.DataFrame,
            *,
            transaction_mode: str = "auto",
        ) -> int:
            transaction_modes.append(transaction_mode)
            raise duckdb.TransactionException("pool guard conflict")

        monkeypatch.setattr(
            store,
            "upsert_limit_up_pool",
            conflicting_pool_write,
        )
        store._conn.execute("BEGIN")  # noqa: SLF001
        recording = _RecordingConnection(store._conn)  # noqa: SLF001
        store._conn = cast(Any, recording)  # noqa: SLF001

        with pytest.raises(
            limit_up_pool.LimitUpPoolWriteConflictError,
            match="business write",
        ):
            capture_zt_pool(trading_date, store)

        assert recording.transaction_commands == []
        assert transaction_modes == ["existing"]
        store._conn.execute("COMMIT")  # noqa: SLF001
        issues = store._conn.execute(  # noqa: SLF001
            "SELECT rule_id, severity FROM data_quality_issue WHERE scope_key = ? ORDER BY rule_id",
            [trading_date.isoformat()],
        ).fetchall()
        assert issues == [("limit_up_pool.concurrent_business_write", "P0")]
        assert store.query_limit_up_pool(trading_date).empty

    def test_real_outer_transaction_conflict_persists_issue_independently(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        db_path = tmp_path / "outer-calendar-conflict.duckdb"
        trading_date = date(2026, 7, 2)
        with DuckDBStore(db_path) as setup:
            setup.upsert_trade_calendar([
                TradeCalendarDay(
                    exchange="SSE",
                    cal_date=trading_date,
                    is_open=True,
                    source="test",
                    updated_at=_CALENDAR_UPDATED_AT,
                )
            ])

        capture_store = DuckDBStore(db_path)
        calendar_writer = DuckDBStore(db_path)

        def fetch_after_calendar_commit(ds: str) -> pd.DataFrame:
            calendar_writer._conn.execute(  # noqa: SLF001
                "UPDATE trade_calendar SET is_open = FALSE "
                "WHERE exchange = 'SSE' AND cal_date = ?",
                [trading_date],
            )
            return _raw_zt_pool()

        monkeypatch.setattr(
            limit_up_pool,
            "_fetch_zt_pool",
            fetch_after_calendar_commit,
        )
        capture_store._conn.execute("BEGIN")  # noqa: SLF001
        try:
            with pytest.raises(
                limit_up_pool.LimitUpPoolCalendarGuardError,
                match="concurrent",
            ):
                capture_zt_pool(trading_date, capture_store)
            with pytest.raises(duckdb.TransactionException, match="aborted"):
                capture_store._conn.execute("SELECT 1")  # noqa: SLF001
            with DuckDBStore(db_path) as check:
                issue = check._conn.execute(  # noqa: SLF001
                    "SELECT rule_id, severity FROM data_quality_issue "
                    "WHERE scope_key = ?",
                    [trading_date.isoformat()],
                ).fetchone()
            assert issue == (
                "limit_up_pool.calendar_changed_during_capture",
                "P0",
            )
        finally:
            capture_store._conn.execute("ROLLBACK")  # noqa: SLF001
            capture_store.close()
            calendar_writer.close()

    def test_known_closed_day_records_p1_without_remote_fetch(
        self, store: DuckDBStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        closed_date = date(2026, 7, 4)
        store.upsert_trade_calendar([
            TradeCalendarDay(
                exchange="SSE",
                cal_date=closed_date,
                is_open=False,
                source="test",
                updated_at=_CALENDAR_UPDATED_AT,
            )
        ])

        def unexpected_fetch(ds: str) -> pd.DataFrame:
            pytest.fail(f"closed-day capture must not fetch: {ds}")

        monkeypatch.setattr(limit_up_pool, "_fetch_zt_pool", unexpected_fetch)

        assert capture_zt_pool(closed_date, store) == 0
        issue = store._conn.execute(  # noqa: SLF001
            """
            SELECT severity, scope_key
            FROM data_quality_issue
            WHERE dataset_id = 'limit_up_pool_daily'
            """
        ).fetchone()
        assert issue == ("P1", closed_date.isoformat())
        assert store.query_limit_up_pool(closed_date).empty

    def test_unknown_calendar_records_p0_and_fails_before_remote_fetch(
        self, store: DuckDBStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        unknown_date = date(2026, 7, 3)

        def unexpected_fetch(ds: str) -> pd.DataFrame:
            pytest.fail(f"unknown-calendar capture must not fetch: {ds}")

        monkeypatch.setattr(limit_up_pool, "_fetch_zt_pool", unexpected_fetch)

        with pytest.raises(RuntimeError, match="calendar.*unknown"):
            capture_zt_pool(unknown_date, store)

        issue = store._conn.execute(  # noqa: SLF001
            """
            SELECT severity, scope_key
            FROM data_quality_issue
            WHERE dataset_id = 'limit_up_pool_daily'
            """
        ).fetchone()
        assert issue == ("P0", unknown_date.isoformat())
        assert store.query_limit_up_pool(unknown_date).empty

    def test_successful_retry_resolves_previous_unknown_calendar_issue(
        self, store: DuckDBStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        trading_date = date(2026, 7, 3)
        monkeypatch.setattr(
            limit_up_pool,
            "_fetch_zt_pool",
            lambda ds: _raw_zt_pool(),
        )
        with pytest.raises(limit_up_pool.LimitUpPoolCalendarGuardError):
            capture_zt_pool(trading_date, store)

        store.upsert_trade_calendar([
            TradeCalendarDay(
                exchange="SSE",
                cal_date=trading_date,
                is_open=True,
                source="test-correction",
                updated_at=datetime(2026, 7, 3, 9, 0, tzinfo=UTC),
            )
        ])

        assert capture_zt_pool(trading_date, store) == 3
        issue = store._conn.execute(  # noqa: SLF001
            """
            SELECT status, resolved_at IS NOT NULL
            FROM data_quality_issue
            WHERE rule_id = 'limit_up_pool.calendar_unknown'
              AND scope_key = ?
            """,
            [trading_date.isoformat()],
        ).fetchone()
        assert issue == ("resolved", True)

    def test_calendar_is_rechecked_after_fetch_before_business_write(
        self, store: DuckDBStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        trading_date = date(2026, 7, 2)

        def fetch_then_close(ds: str) -> pd.DataFrame:
            store.upsert_trade_calendar([
                TradeCalendarDay(
                    exchange="SSE",
                    cal_date=trading_date,
                    is_open=False,
                    source="test-correction",
                    updated_at=datetime(2026, 7, 2, 16, 30, tzinfo=UTC),
                )
            ])
            return _raw_zt_pool()

        monkeypatch.setattr(limit_up_pool, "_fetch_zt_pool", fetch_then_close)

        with pytest.raises(RuntimeError, match="calendar.*changed"):
            capture_zt_pool(trading_date, store)

        issue = store._conn.execute(  # noqa: SLF001
            """
            SELECT severity, scope_key
            FROM data_quality_issue
            WHERE dataset_id = 'limit_up_pool_daily'
            """
        ).fetchone()
        assert issue == ("P0", trading_date.isoformat())
        assert store.query_limit_up_pool(trading_date).empty

    def test_final_check_fences_concurrent_calendar_correction(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        db_path = tmp_path / "capture-calendar-concurrency.duckdb"
        trading_date = date(2026, 7, 2)
        with DuckDBStore(db_path) as setup:
            setup.upsert_trade_calendar([
                TradeCalendarDay(
                    exchange="SSE",
                    cal_date=trading_date,
                    is_open=True,
                    source="test",
                    updated_at=_CALENDAR_UPDATED_AT,
                )
            ])

        monkeypatch.setattr(
            limit_up_pool,
            "_fetch_zt_pool",
            lambda ds: _raw_zt_pool(),
        )
        writer_outcomes: list[str] = []
        with (
            DuckDBStore(db_path) as capture_store,
            DuckDBStore(db_path) as calendar_writer,
        ):
            original_get = capture_store.get_trade_calendar_day
            reads = 0

            def read_then_correct(
                exchange: str,
                cal_date: date,
            ) -> TradeCalendarDay | None:
                nonlocal reads
                calendar_day = original_get(exchange, cal_date)
                reads += 1
                if reads == 2:
                    try:
                        calendar_writer._conn.execute(  # noqa: SLF001
                            """
                            UPDATE trade_calendar
                            SET is_open = FALSE
                            WHERE exchange = 'SSE' AND cal_date = ?
                            """,
                            [trading_date],
                        )
                    except duckdb.TransactionException:
                        writer_outcomes.append("conflicted")
                    else:
                        writer_outcomes.append("committed")
                return calendar_day

            monkeypatch.setattr(
                capture_store,
                "get_trade_calendar_day",
                read_then_correct,
            )
            assert capture_zt_pool(trading_date, capture_store) == 3

        with DuckDBStore(db_path, read_only=True) as check:
            calendar = check.get_trade_calendar_day("SSE", trading_date)
            pool_count = check._conn.execute(  # noqa: SLF001
                "SELECT COUNT(*) FROM limit_up_pool_daily"
            ).fetchone()

        assert writer_outcomes == ["conflicted"]
        assert calendar is not None and calendar.is_open is True
        assert pool_count == (3,)

    def test_final_check_records_p0_when_calendar_writer_wins_fence(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        db_path = tmp_path / "capture-calendar-writer-wins.duckdb"
        trading_date = date(2026, 7, 2)
        with DuckDBStore(db_path) as setup:
            setup.upsert_trade_calendar([
                TradeCalendarDay(
                    exchange="SSE",
                    cal_date=trading_date,
                    is_open=True,
                    source="test",
                    updated_at=_CALENDAR_UPDATED_AT,
                )
            ])

        capture_store = DuckDBStore(db_path)
        calendar_writer = DuckDBStore(db_path)

        def fetch_after_writer_takes_fence(ds: str) -> pd.DataFrame:
            calendar_writer._conn.execute("BEGIN")  # noqa: SLF001
            calendar_writer._conn.execute(  # noqa: SLF001
                """
                UPDATE trade_calendar
                SET is_open = FALSE
                WHERE exchange = 'SSE' AND cal_date = ?
                """,
                [trading_date],
            )
            return _raw_zt_pool()

        monkeypatch.setattr(
            limit_up_pool,
            "_fetch_zt_pool",
            fetch_after_writer_takes_fence,
        )
        try:
            with pytest.raises(
                limit_up_pool.LimitUpPoolCalendarGuardError,
                match="concurrent",
            ):
                capture_zt_pool(trading_date, capture_store)
            calendar_writer._conn.execute("COMMIT")  # noqa: SLF001
        finally:
            capture_store.close()
            calendar_writer.close()

        with DuckDBStore(db_path, read_only=True) as check:
            calendar = check.get_trade_calendar_day("SSE", trading_date)
            pool_count = check._conn.execute(  # noqa: SLF001
                "SELECT COUNT(*) FROM limit_up_pool_daily"
            ).fetchone()
            issue = check._conn.execute(  # noqa: SLF001
                """
                SELECT severity, scope_key
                FROM data_quality_issue
                WHERE rule_id = 'limit_up_pool.calendar_changed_during_capture'
                """
            ).fetchone()

        assert calendar is not None and calendar.is_open is False
        assert pool_count == (0,)
        assert issue == ("P0", trading_date.isoformat())

    def test_pool_writer_conflict_is_not_mislabeled_as_calendar_change(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        db_path = tmp_path / "capture-pool-writer-conflict.duckdb"
        trading_date = date(2026, 7, 2)
        with DuckDBStore(db_path) as setup:
            setup.upsert_trade_calendar([
                TradeCalendarDay(
                    exchange="SSE",
                    cal_date=trading_date,
                    is_open=True,
                    source="test",
                    updated_at=_CALENDAR_UPDATED_AT,
                )
            ])

        capture_store = DuckDBStore(db_path)
        monkeypatch.setattr(
            limit_up_pool,
            "_fetch_zt_pool",
            lambda ds: _raw_zt_pool(),
        )

        def conflicting_pool_write(
            df: pd.DataFrame,
            *,
            transaction_mode: str = "auto",
        ) -> int:
            raise duckdb.TransactionException("pool guard conflict")

        monkeypatch.setattr(
            capture_store,
            "upsert_limit_up_pool",
            conflicting_pool_write,
        )
        try:
            with pytest.raises(
                limit_up_pool.LimitUpPoolWriteConflictError,
                match="business write",
            ):
                capture_zt_pool(trading_date, capture_store)
        finally:
            capture_store.close()

        with DuckDBStore(db_path, read_only=True) as check:
            pool_count = check._conn.execute(  # noqa: SLF001
                "SELECT COUNT(*) FROM limit_up_pool_daily"
            ).fetchone()
            issues = check._conn.execute(  # noqa: SLF001
                """
                SELECT rule_id, severity
                FROM data_quality_issue
                WHERE scope_key = ?
                ORDER BY rule_id
                """,
                [trading_date.isoformat()],
            ).fetchall()

        assert pool_count == (0,)
        assert issues == [("limit_up_pool.concurrent_business_write", "P0")]

    def test_post_commit_issue_resolution_conflict_does_not_misreport_write(
        self,
        store: DuckDBStore,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(
            limit_up_pool,
            "_fetch_zt_pool",
            lambda ds: _raw_zt_pool(),
        )
        resolve_calls = 0

        def fail_post_commit_resolution(
            current_store: DuckDBStore,
            trading_date: date,
            rule_ids: tuple[str, ...],
        ) -> None:
            nonlocal resolve_calls
            resolve_calls += 1
            if resolve_calls == 2:
                raise duckdb.TransactionException("issue resolution conflict")

        monkeypatch.setattr(
            limit_up_pool,
            "_resolve_open_issues",
            fail_post_commit_resolution,
        )

        assert capture_zt_pool(date(2026, 7, 2), store) == 3
        assert resolve_calls == 2
        assert store.query_limit_up_pool(date(2026, 7, 2)).shape[0] == 3
        assert store._conn.execute(  # noqa: SLF001
            """
            SELECT COUNT(*)
            FROM data_quality_issue
            WHERE rule_id = 'limit_up_pool.concurrent_business_write'
            """
        ).fetchone() == (0,)

    def test_capture_writes_to_store(
        self, store: DuckDBStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fetched: list[str] = []

        def fake_fetch(ds: str) -> pd.DataFrame:
            fetched.append(ds)
            return _raw_zt_pool()

        monkeypatch.setattr(limit_up_pool, "_fetch_zt_pool", fake_fetch)

        rows = capture_zt_pool(date(2026, 7, 2), store)

        assert rows == 3
        assert fetched == ["20260702"]
        out = store.query_limit_up_pool(date(2026, 7, 2))
        assert len(out) == 3
        # 连板数倒序，2 连板在最前
        assert out.iloc[0]["ts_code"] == "002273.SZ"
        assert out.iloc[0]["consecutive_boards"] == 2

    def test_capture_is_idempotent(
        self, store: DuckDBStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            limit_up_pool, "_fetch_zt_pool", lambda ds: _raw_zt_pool()
        )

        capture_zt_pool(date(2026, 7, 2), store)
        capture_zt_pool(date(2026, 7, 2), store)

        assert len(store.query_limit_up_pool(date(2026, 7, 2))) == 3

    def test_capture_defaults_to_today(
        self, store: DuckDBStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fetched: list[str] = []

        def fake_fetch(ds: str) -> pd.DataFrame:
            fetched.append(ds)
            return _raw_zt_pool()

        monkeypatch.setattr(limit_up_pool, "_fetch_zt_pool", fake_fetch)

        capture_zt_pool(store=store)

        assert fetched == [date.today().strftime("%Y%m%d")]

    def test_fetch_failure_returns_zero(
        self, store: DuckDBStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def boom(ds: str) -> pd.DataFrame:
            raise RuntimeError("eastmoney blocked")

        monkeypatch.setattr(limit_up_pool, "_fetch_zt_pool", boom)

        assert capture_zt_pool(date(2026, 7, 2), store) == 0
        assert store.query_limit_up_pool(date(2026, 7, 2)).empty

    def test_empty_result_returns_zero(
        self, store: DuckDBStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            limit_up_pool, "_fetch_zt_pool", lambda ds: pd.DataFrame()
        )

        assert capture_zt_pool(date(2026, 7, 2), store) == 0

    def test_column_change_returns_zero_not_raise(
        self, store: DuckDBStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """东财列名变更（缺 '代码'）不炸：warning + 0，日终链路容忍缺采。"""
        monkeypatch.setattr(
            limit_up_pool,
            "_fetch_zt_pool",
            lambda ds: pd.DataFrame([{"名称": "x"}]),
        )

        assert capture_zt_pool(date(2026, 7, 2), store) == 0
