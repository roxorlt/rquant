"""Historical security-name and ST-status fact tests."""

from __future__ import annotations

from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from threading import Barrier, Event
from typing import Any, cast
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd
import pytest
from pydantic import ValidationError

import rquant.security_status as security_status
import rquant.storage.duckdb as duckdb_storage
from rquant.adapter.tushare import TushareAdapter
from rquant.security_status import (
    DEFAULT_REQUEST_INTERVAL_SECONDS,
    DailySecurityKey,
    NameChangeHistory,
    NameChangeInterval,
    NameChangeTruncatedError,
    SecurityStatusConcurrentWriteError,
    SecurityStatusCoverage,
    SecurityStatusDaily,
    SecurityStatusEligibilityChangedError,
    SecurityStatusPrefetchBatch,
    SecurityStatusWriteConflictError,
    StockSTHistory,
    StockSTIncompleteError,
    StockSTObservation,
    StockSTTruncatedError,
    backfill_historical_security_status,
    fetch_namechange_history,
    materialize_security_status,
    normalize_name,
    normalize_namechange_history,
    normalize_stock_st_history,
    plan_historical_security_status_backfill,
    prefetch_security_status,
)
from rquant.storage.duckdb import DuckDBStore

SHANGHAI = ZoneInfo("Asia/Shanghai")
INGESTED_AT = datetime(2026, 7, 14, 8, tzinfo=UTC)


def _key(day: date, ts_code: str = "600000.SH") -> DailySecurityKey:
    return DailySecurityKey(ts_code=ts_code, trade_date=day)


def _interval(
    name: str,
    start: date,
    end: date | None,
    *,
    ann_date: date | None = None,
    ts_code: str = "600000.SH",
) -> NameChangeInterval:
    return NameChangeInterval(
        ts_code=ts_code,
        name=name,
        start_date=start,
        end_date=end,
        ann_date=ann_date,
        change_reason="更名",
    )


@pytest.mark.parametrize(
    "raw_name",
    ["ST浦发", "*ST浦发", "SST浦发", "S*ST浦发", "ＳＴ　浦发", " * ST 浦发 "],
)
def test_normalize_name_recognizes_historical_st_prefixes(raw_name: str) -> None:
    normalized, is_st = normalize_name(raw_name)

    assert normalized is not None
    assert " " not in normalized
    assert "　" not in normalized
    assert is_st is True


def test_security_status_models_are_frozen_and_unknown_is_none() -> None:
    status = SecurityStatusDaily(
        ts_code="600000.SH",
        trade_date=date(2020, 1, 2),
        name=None,
        is_st=None,
        name_source="unknown",
        st_source=None,
        available_at=None,
        ingested_at=INGESTED_AT,
    )

    assert status.is_st is None
    with pytest.raises(ValidationError, match="frozen"):
        status.is_st = False  # type: ignore[misc]


def test_security_status_coverage_rejects_conflicts_exceeding_unknowns() -> None:
    with pytest.raises(ValidationError, match="conflict_count"):
        SecurityStatusCoverage(
            start=date(2020, 1, 2),
            end=date(2020, 1, 2),
            expected_count=1,
            persisted_count=1,
            missing_count=0,
            unknown_count=0,
            conflict_count=1,
            invalid_count=0,
        )


@pytest.mark.parametrize(
    ("overrides", "error"),
    [
        ({"name": None}, "name_source"),
        ({"available_at": None}, "available_at"),
        ({"name_source": "unknown"}, "name_source"),
        ({"name_source": "conflict"}, "name_source"),
        ({"st_source": None}, "st_source"),
        ({"st_source": "unknown"}, "st_source"),
        ({"st_source": "conflict"}, "st_source"),
    ],
)
def test_known_security_status_requires_complete_valid_fact_sources(
    overrides: dict[str, object],
    error: str,
) -> None:
    payload: dict[str, object] = {
        "ts_code": "600000.SH",
        "trade_date": date(2020, 1, 2),
        "name": "浦发银行",
        "is_st": False,
        "name_source": "tushare.namechange",
        "st_source": "tushare.namechange",
        "available_at": datetime(2020, 1, 2, 9, 25, tzinfo=SHANGHAI),
        "ingested_at": INGESTED_AT,
    }
    payload.update(overrides)

    with pytest.raises(ValidationError, match=error):
        SecurityStatusDaily.model_validate(payload)


def test_known_st_status_does_not_require_a_historical_display_name() -> None:
    status = SecurityStatusDaily(
        ts_code="689009.SH",
        trade_date=date(2026, 7, 14),
        name=None,
        is_st=False,
        name_source="unknown",
        st_source="tushare.stock_st_absence",
        available_at=datetime(2026, 7, 14, 9, 25, tzinfo=SHANGHAI),
        ingested_at=INGESTED_AT,
    )

    assert status.name is None
    assert status.is_st is False


def test_conflicted_security_status_must_keep_fact_unknown() -> None:
    with pytest.raises(ValidationError, match="conflicted status"):
        SecurityStatusDaily(
            ts_code="600000.SH",
            trade_date=date(2020, 1, 2),
            name="浦发银行",
            is_st=False,
            name_source="conflict",
            st_source=None,
            available_at=None,
            ingested_at=INGESTED_AT,
            conflict_reason="overlap",
        )


def test_materialize_uses_inclusive_intervals_open_end_and_pit_visibility() -> None:
    keys = [_key(date(2020, 1, day)) for day in range(1, 7)]
    names = NameChangeHistory(
        intervals=(
            _interval("浦发银行", date(2020, 1, 2), date(2020, 1, 3)),
            _interval(
                "*ST 浦发",
                date(2020, 1, 4),
                None,
                ann_date=date(2020, 1, 5),
            ),
        )
    )

    rows = materialize_security_status(
        keys,
        names,
        StockSTHistory(),
        ingested_at=INGESTED_AT,
    )

    by_day = {row.trade_date: row for row in rows}
    assert by_day[date(2020, 1, 1)].name is None
    assert by_day[date(2020, 1, 1)].is_st is None
    assert by_day[date(2020, 1, 2)].name == "浦发银行"
    assert by_day[date(2020, 1, 2)].is_st is False
    assert by_day[date(2020, 1, 3)].is_st is False
    assert by_day[date(2020, 1, 4)].name == "*ST浦发"
    assert by_day[date(2020, 1, 4)].is_st is True
    assert by_day[date(2020, 1, 4)].available_at == datetime(
        2020, 1, 5, 9, 25, tzinfo=SHANGHAI
    )
    assert by_day[date(2020, 1, 6)].is_st is True


@pytest.mark.parametrize("change_reason", ["重新上市", "恢复上市"])
def test_materialize_marks_relisting_first_day_unsupported(
    change_reason: str,
) -> None:
    relisting_date = date(2020, 1, 2)
    names = NameChangeHistory(
        intervals=(
            NameChangeInterval(
                ts_code="600000.SH",
                name="浦发银行",
                start_date=relisting_date,
                end_date=None,
                ann_date=relisting_date,
                change_reason=change_reason,
            ),
        )
    )

    rows = materialize_security_status(
        [_key(relisting_date), _key(date(2020, 1, 3))],
        names,
        StockSTHistory(),
        ingested_at=INGESTED_AT,
    )

    assert rows[0].name is None
    assert rows[0].is_st is None
    assert rows[0].conflict_reason == "unsupported_relisting_price_limit"
    assert rows[1].name == "浦发银行"
    assert rows[1].is_st is False


@pytest.mark.parametrize(
    ("change_reason", "expected_conflict"),
    [
        (None, "unknown_namechange_boundary"),
        ("重返上市", "unsupported_listing_transition"),
        ("无法分类", "unknown_namechange_reason"),
    ],
)
def test_materialize_fails_closed_on_ambiguous_listing_boundary(
    change_reason: str | None,
    expected_conflict: str,
) -> None:
    boundary_date = date(2020, 1, 2)
    names = NameChangeHistory(
        intervals=(
            NameChangeInterval(
                ts_code="600000.SH",
                name="浦发银行",
                start_date=boundary_date,
                end_date=None,
                ann_date=boundary_date,
                change_reason=change_reason,
            ),
        )
    )

    rows = materialize_security_status(
        [_key(boundary_date), _key(date(2020, 1, 3))],
        names,
        StockSTHistory(),
        ingested_at=INGESTED_AT,
    )

    assert rows[0].name is None
    assert rows[0].is_st is None
    assert rows[0].conflict_reason == expected_conflict
    assert rows[1].name == "浦发银行"
    assert rows[1].is_st is False


def test_materialize_accepts_provider_other_reason_as_a_known_name_interval() -> None:
    listing_date = date(2026, 7, 10)
    names = NameChangeHistory(
        intervals=(
            NameChangeInterval(
                ts_code="301583.SZ",
                name="托伦斯",
                start_date=listing_date,
                ann_date=listing_date,
                change_reason="其他",
            ),
        )
    )

    row = materialize_security_status(
        [_key(listing_date, "301583.SZ")],
        names,
        StockSTHistory(),
        ingested_at=INGESTED_AT,
    )[0]

    assert row.name == "托伦斯"
    assert row.is_st is False
    assert row.conflict_reason is None


def test_materialize_conflicts_and_invalid_source_rows_fail_to_unknown() -> None:
    raw = pd.DataFrame(
        [
            {
                "ts_code": "600000.SH",
                "name": "浦发银行",
                "start_date": "20200101",
                "end_date": "20200103",
                "ann_date": "20200101",
                "change_reason": "更名",
            },
            {
                "ts_code": "600000.SH",
                "name": "*ST浦发",
                "start_date": "20200102",
                "end_date": "20200104",
                "ann_date": "20200102",
                "change_reason": "更名",
            },
            {
                "ts_code": "000001.SZ",
                "name": "平安银行",
                "start_date": "not-a-date",
                "end_date": None,
                "ann_date": None,
                "change_reason": "bad",
            },
        ]
    )
    history = normalize_namechange_history(raw)

    rows = materialize_security_status(
        [_key(date(2020, 1, 2)), _key(date(2020, 1, 2), "000001.SZ")],
        history,
        StockSTHistory(),
        ingested_at=INGESTED_AT,
    )

    assert len(history.issues) == 1
    assert all(row.name is None and row.is_st is None for row in rows)
    assert all(row.conflict_reason is not None for row in rows)


def test_stock_st_positive_confirms_true_without_requiring_display_name_match() -> None:
    keys = [
        _key(date(2020, 1, 2)),
        _key(date(2020, 1, 3)),
        _key(date(2020, 1, 4)),
    ]
    names = NameChangeHistory(
        intervals=(
            _interval("浦发银行", date(2020, 1, 3), date(2020, 1, 3)),
            _interval("*ST浦发", date(2020, 1, 4), date(2020, 1, 4)),
        )
    )
    positives = StockSTHistory(
        observations=(
            StockSTObservation(
                ts_code="600000.SH",
                name="ST浦发",
                trade_date=date(2020, 1, 2),
                type="S",
                type_name="ST",
            ),
            StockSTObservation(
                ts_code="600000.SH",
                name="*ST浦发",
                trade_date=date(2020, 1, 3),
                type="S",
                type_name="ST",
            ),
            StockSTObservation(
                ts_code="600000.SH",
                name="*ST浦发",
                trade_date=date(2020, 1, 4),
                type="S",
                type_name="ST",
            ),
        )
    )

    rows = materialize_security_status(
        keys,
        names,
        positives,
        ingested_at=INGESTED_AT,
    )
    by_day = {row.trade_date: row for row in rows}

    assert by_day[date(2020, 1, 2)].name == "ST浦发"
    assert by_day[date(2020, 1, 2)].is_st is True
    assert by_day[date(2020, 1, 2)].name_source == "tushare.stock_st"
    assert by_day[date(2020, 1, 2)].st_source == "tushare.stock_st"
    assert by_day[date(2020, 1, 2)].available_at == datetime(
        2020, 1, 2, 9, 25, tzinfo=SHANGHAI
    )
    assert by_day[date(2020, 1, 3)].name == "浦发银行"
    assert by_day[date(2020, 1, 3)].is_st is True
    assert by_day[date(2020, 1, 3)].conflict_reason is None
    assert by_day[date(2020, 1, 3)].st_source == (
        "tushare.namechange+tushare.stock_st"
    )
    assert by_day[date(2020, 1, 4)].name == "*ST浦发"
    assert by_day[date(2020, 1, 4)].is_st is True
    assert by_day[date(2020, 1, 4)].st_source == (
        "tushare.namechange+tushare.stock_st"
    )


def test_complete_stock_st_list_absence_confirms_non_st_without_name() -> None:
    trade_date = date(2026, 7, 14)
    rows = materialize_security_status(
        [_key(trade_date, "689009.SH")],
        NameChangeHistory(),
        StockSTHistory(is_complete=True),
        ingested_at=INGESTED_AT,
    )

    assert rows[0].name is None
    assert rows[0].is_st is False
    assert rows[0].name_source == "unknown"
    assert rows[0].st_source == "tushare.stock_st_absence"
    assert rows[0].available_at == datetime(
        2026, 7, 14, 9, 25, tzinfo=SHANGHAI
    )


def test_status_coverage_accepts_known_st_fact_without_name(tmp_path: Path) -> None:
    trade_date = date(2026, 7, 14)
    with DuckDBStore(tmp_path / "status-without-name.duckdb") as store:
        store._conn.execute(
            "INSERT INTO daily_bar (ts_code, trade_date, close) VALUES (?, ?, 10)",
            ["689009.SH", trade_date],
        )
        store.upsert_stock_status(
            (
                SecurityStatusDaily(
                    ts_code="689009.SH",
                    trade_date=trade_date,
                    name=None,
                    is_st=False,
                    name_source="unknown",
                    st_source="tushare.stock_st_absence",
                    available_at=datetime(
                        2026, 7, 14, 9, 25, tzinfo=SHANGHAI
                    ),
                    ingested_at=INGESTED_AT,
                ),
            )
        )

        coverage = store.stock_status_coverage(trade_date, trade_date)

    assert coverage.unknown_count == 0
    assert coverage.conflict_count == 0
    assert coverage.invalid_count == 0


class _StubSecurityStatusPro:
    def __init__(
        self,
        *,
        namechange: pd.DataFrame | None = None,
        stock_st: pd.DataFrame | None = None,
    ) -> None:
        self.namechange_frame = pd.DataFrame() if namechange is None else namechange
        self.stock_st_frame = pd.DataFrame() if stock_st is None else stock_st
        self.namechange_calls: list[dict[str, object]] = []
        self.stock_st_calls: list[dict[str, object]] = []

    def namechange(self, **kwargs: object) -> pd.DataFrame:
        self.namechange_calls.append(dict(kwargs))
        return self.namechange_frame.copy()

    def stock_st(self, **kwargs: object) -> pd.DataFrame:
        self.stock_st_calls.append(dict(kwargs))
        return self.stock_st_frame.copy()


def _adapter_with_pro(pro: object) -> TushareAdapter:
    adapter = TushareAdapter.__new__(TushareAdapter)
    adapter._pro = pro
    adapter._primary_token = "primary"
    adapter._backup_token = ""
    adapter._using_backup = False
    return adapter


def test_tushare_security_status_raw_methods_fix_fields_and_empty_shape() -> None:
    pro = _StubSecurityStatusPro()
    adapter = _adapter_with_pro(pro)

    namechange = adapter.namechange_raw(
        date(2020, 1, 1), date(2020, 12, 31), ts_code="600000.SH"
    )
    stock_st = adapter.stock_st_raw(date(2020, 1, 2))

    assert list(namechange.columns) == [
        "ts_code",
        "name",
        "start_date",
        "end_date",
        "ann_date",
        "change_reason",
    ]
    assert pro.namechange_calls == [
        {
            "start_date": "20200101",
            "end_date": "20201231",
            "fields": "ts_code,name,start_date,end_date,ann_date,change_reason",
            "ts_code": "600000.SH",
        }
    ]
    assert list(stock_st.columns) == [
        "ts_code",
        "name",
        "trade_date",
        "type",
        "type_name",
    ]
    assert pro.stock_st_calls == [
        {
            "trade_date": "20200102",
            "fields": "ts_code,name,trade_date,type,type_name",
        }
    ]


class _WindowedAdapter:
    def __init__(self, rows: int = 1) -> None:
        self.rows = rows
        self.calls: list[tuple[date, date, str | None]] = []

    def namechange_raw(
        self,
        start_date: date,
        end_date: date,
        ts_code: str | None = None,
    ) -> pd.DataFrame:
        self.calls.append((start_date, end_date, ts_code))
        row = {
            "ts_code": "600000.SH",
            "name": "浦发银行",
            "start_date": "19991110",
            "end_date": None,
            "ann_date": "19991110",
            "change_reason": "上市",
        }
        return pd.DataFrame([row] * self.rows)


def test_namechange_window_fetch_deduplicates_overlaps() -> None:
    adapter = _WindowedAdapter()

    history = fetch_namechange_history(
        adapter,
        start=date(2018, 1, 1),
        end=date(2023, 12, 31),
        window_years=3,
        request_interval_seconds=0,
    )

    assert len(adapter.calls) == 2
    assert adapter.calls[0][:2] == (date(2018, 1, 1), date(2021, 1, 1))
    assert adapter.calls[1][:2] == (date(2021, 1, 1), date(2023, 12, 31))
    assert len(history.intervals) == 1


def test_namechange_window_at_provider_limit_fails_closed() -> None:
    adapter = _WindowedAdapter(rows=10_000)

    with pytest.raises(NameChangeTruncatedError, match="10000"):
        fetch_namechange_history(
            adapter,
            start=date(2020, 1, 1),
            end=date(2020, 12, 31),
            request_interval_seconds=0,
        )


class _LateAnnouncementAdapter:
    def __init__(self) -> None:
        self.namechange_calls: list[tuple[date, date]] = []

    def namechange_raw(
        self,
        start_date: date,
        end_date: date,
        ts_code: str | None = None,
    ) -> pd.DataFrame:
        del ts_code
        self.namechange_calls.append((start_date, end_date))
        if end_date < date(2020, 1, 5):
            return pd.DataFrame()
        return pd.DataFrame(
            [
                {
                    "ts_code": "600000.SH",
                    "name": "*ST迟报",
                    "start_date": "20200102",
                    "end_date": None,
                    "ann_date": "20200105",
                    "change_reason": "更名",
                }
            ]
        )

    def stock_st_raw(self, trade_date: date) -> pd.DataFrame:
        del trade_date
        return pd.DataFrame(
            columns=["ts_code", "name", "trade_date", "type", "type_name"]
        )


def test_backfill_source_as_of_finds_late_announcement_independent_of_target_end(
    tmp_path: Path,
) -> None:
    stored_rows: list[SecurityStatusDaily] = []
    calls: list[tuple[date, date]] = []
    for target_end in (date(2020, 1, 2), date(2020, 1, 3)):
        adapter = _LateAnnouncementAdapter()
        db_path = tmp_path / f"late-{target_end}.duckdb"
        with DuckDBStore(db_path) as store:
            store._conn.execute(
                "INSERT INTO daily_bar (ts_code, trade_date, close) VALUES "
                "('600000.SH', DATE '2020-01-02', 10)"
            )
        result = backfill_historical_security_status(
            adapter,
            store_factory=lambda db_path=db_path: DuckDBStore(db_path),
            start=date(2020, 1, 2),
            end=target_end,
            source_as_of=date(2020, 1, 5),
            namechange_start=date(2020, 1, 1),
            ingested_at=INGESTED_AT,
            request_interval_seconds=0,
        )
        with DuckDBStore(db_path, read_only=True) as store:
            stored_rows.append(
                store.list_stock_status(date(2020, 1, 2), date(2020, 1, 2))[0]
            )
        assert result.source_as_of == date(2020, 1, 5)
        calls.extend(adapter.namechange_calls)

    assert stored_rows[0] == stored_rows[1]
    assert stored_rows[0].name == "*ST迟报"
    assert stored_rows[0].is_st is True
    assert stored_rows[0].available_at == datetime(
        2020, 1, 5, 9, 25, tzinfo=SHANGHAI
    )
    assert calls == [
        (date(2020, 1, 1), date(2020, 1, 5)),
        (date(2020, 1, 1), date(2020, 1, 5)),
    ]


class _BackfillAdapter(_WindowedAdapter):
    def __init__(self) -> None:
        super().__init__()
        self.stock_dates: list[date] = []

    def stock_st_raw(self, trade_date: date) -> pd.DataFrame:
        self.stock_dates.append(trade_date)
        return pd.DataFrame(
            columns=["ts_code", "name", "trade_date", "type", "type_name"]
        )


def test_backfill_closes_planning_store_before_provider_calls(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "two-phase-status.duckdb"
    with DuckDBStore(db_path) as store:
        store._conn.execute(
            """
            INSERT INTO daily_bar (ts_code, trade_date, close)
            VALUES ('600000.SH', DATE '2020-01-02', 10)
            """
        )

    class _LockProbeAdapter(_BackfillAdapter):
        def _probe(self) -> None:
            with DuckDBStore(db_path, read_only=True):
                pass

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

    result = backfill_historical_security_status(
        _LockProbeAdapter(),
        store_factory=lambda: DuckDBStore(db_path),
        start=date(2020, 1, 2),
        end=date(2020, 1, 2),
        source_as_of=date(2020, 1, 2),
        namechange_start=date(2020, 1, 1),
        ingested_at=INGESTED_AT,
        request_interval_seconds=0,
    )

    assert result.upserted_count == 1


def test_security_status_backfill_plan_counts_missing_keys_and_api_operations(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "status-plan.duckdb"
    with DuckDBStore(db_path) as store:
        store._conn.execute(
            """
            INSERT INTO daily_bar (ts_code, trade_date, close)
            VALUES
                ('600000.SH', DATE '2020-01-02', 10),
                ('000001.SZ', DATE '2020-01-02', 11),
                ('600000.SH', DATE '2020-01-03', 12)
            """
        )
        store.upsert_stock_status(
            [
                SecurityStatusDaily(
                    ts_code="000001.SZ",
                    trade_date=date(2020, 1, 2),
                    name="平安银行",
                    is_st=False,
                    name_source="namechange",
                    st_source="stock_st",
                    available_at=datetime(2020, 1, 2, 9, 25, tzinfo=SHANGHAI),
                    ingested_at=INGESTED_AT,
                )
            ],
            require_daily_keys=True,
        )

    plan = plan_historical_security_status_backfill(
        store_factory=lambda: DuckDBStore(db_path),
        start=date(2020, 1, 2),
        end=date(2020, 1, 3),
        source_as_of=date(2026, 7, 15),
        namechange_start=date(1990, 1, 1),
        missing_only=True,
    )

    assert plan.eligible_count == 2
    assert plan.trade_date_count == 2
    assert plan.namechange_logical_api_operations == 13
    assert plan.stock_st_logical_api_operations == 2
    assert plan.total_logical_api_operations == 15


def test_backfill_rejects_key_deleted_between_plan_and_apply(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "status-key-deleted.duckdb"
    target = _key(date(2020, 1, 2))
    with DuckDBStore(db_path) as store:
        store._conn.execute(
            """
            INSERT INTO daily_bar (ts_code, trade_date, close)
            VALUES (?, ?, 10)
            """,
            [target.ts_code, target.trade_date],
        )

    class _DeletingAdapter(_BackfillAdapter):
        def namechange_raw(
            self,
            start_date: date,
            end_date: date,
            ts_code: str | None = None,
        ) -> pd.DataFrame:
            with DuckDBStore(db_path) as concurrent_writer:
                concurrent_writer._conn.execute(
                    "DELETE FROM daily_bar WHERE ts_code = ? AND trade_date = ?",
                    [target.ts_code, target.trade_date],
                )
            return super().namechange_raw(start_date, end_date, ts_code)

    with pytest.raises(SecurityStatusEligibilityChangedError) as exc_info:
        backfill_historical_security_status(
            _DeletingAdapter(),
            store_factory=lambda: DuckDBStore(db_path),
            start=target.trade_date,
            end=target.trade_date,
            source_as_of=target.trade_date,
            namechange_start=date(2020, 1, 1),
            ingested_at=INGESTED_AT,
            request_interval_seconds=0,
        )

    assert exc_info.value.missing_keys == (target,)
    with DuckDBStore(db_path, read_only=True) as store:
        assert store.list_stock_status(target.trade_date, target.trade_date) == []


def test_backfill_empty_code_scope_skips_store_and_provider_calls() -> None:
    adapter = _StreamingAdapter()
    store_calls = 0

    def store_factory() -> DuckDBStore:
        nonlocal store_calls
        store_calls += 1
        raise AssertionError("empty scope must not open storage")

    result = backfill_historical_security_status(
        adapter,
        store_factory=store_factory,
        start=date(2020, 1, 2),
        end=date(2020, 1, 2),
        ts_codes=[],
        source_as_of=date(2020, 1, 2),
        ingested_at=INGESTED_AT,
        request_interval_seconds=0,
    )

    assert result.eligible_count == 0
    assert store_calls == 0
    assert adapter.provider_calls == []


def test_prefetch_materializes_typed_batch_without_store_access() -> None:
    adapter = _BackfillAdapter()
    keys = [
        _key(date(2020, 1, 2)),
        _key(date(2020, 1, 3)),
    ]

    batch = prefetch_security_status(
        adapter,
        keys,
        source_as_of=date(2020, 1, 3),
        namechange_start=date(2020, 1, 1),
        ingested_at=INGESTED_AT,
        request_interval_seconds=0,
    )

    assert isinstance(batch, SecurityStatusPrefetchBatch)
    assert [(row.trade_date, row.is_st) for row in batch.rows] == [
        (date(2020, 1, 2), False),
        (date(2020, 1, 3), False),
    ]
    assert batch.namechange_request_count == 1
    assert batch.stock_st_request_count == 2
    assert adapter.calls == [
        (date(2020, 1, 1), date(2020, 1, 3), "600000.SH")
    ]
    assert adapter.stock_dates == [date(2020, 1, 2), date(2020, 1, 3)]


class _ScopedAdapter:
    def __init__(self) -> None:
        self.namechange_codes: list[str | None] = []
        self.stock_dates: list[date] = []

    def namechange_raw(
        self,
        start_date: date,
        end_date: date,
        ts_code: str | None = None,
    ) -> pd.DataFrame:
        del start_date, end_date
        self.namechange_codes.append(ts_code)
        codes = [ts_code] if ts_code else ["000001.SZ", "600000.SH"]
        return pd.DataFrame(
            [
                {
                    "ts_code": code,
                    "name": "平安银行" if code == "000001.SZ" else "浦发银行",
                    "start_date": "20200101",
                    "end_date": None,
                    "ann_date": "20200101",
                    "change_reason": "更名",
                }
                for code in codes
            ]
        )

    def stock_st_raw(self, trade_date: date) -> pd.DataFrame:
        self.stock_dates.append(trade_date)
        return pd.DataFrame(columns=list(security_status._STOCK_ST_COLUMNS))


def test_backfill_ts_code_scope_cannot_write_other_daily_codes(tmp_path: Path) -> None:
    adapter = _ScopedAdapter()
    db_path = tmp_path / "scoped-code.duckdb"
    with DuckDBStore(db_path) as store:
        store._conn.execute(
            "INSERT INTO daily_bar (ts_code, trade_date, close) VALUES "
            "('000001.SZ', DATE '2020-01-02', 10), "
            "('600000.SH', DATE '2020-01-02', 11)"
        )

    result = backfill_historical_security_status(
        adapter,
        store_factory=lambda: DuckDBStore(db_path),
        start=date(2020, 1, 2),
        end=date(2020, 1, 2),
        ts_codes=["000001.SZ"],
        source_as_of=date(2020, 1, 2),
        namechange_start=date(2020, 1, 1),
        ingested_at=INGESTED_AT,
        request_interval_seconds=0,
    )
    with DuckDBStore(db_path, read_only=True) as store:
        stored = store.list_stock_status(date(2020, 1, 2), date(2020, 1, 2))

    assert result.eligible_count == 1
    assert [row.ts_code for row in stored] == ["000001.SZ"]
    assert adapter.namechange_codes == ["000001.SZ"]


def test_backfill_eligible_key_scope_and_parameterized_store_lookup(
    tmp_path: Path,
) -> None:
    adapter = _ScopedAdapter()
    target = _key(date(2020, 1, 2), "600000.SH")
    db_path = tmp_path / "scoped-key.duckdb"
    with DuckDBStore(db_path) as store:
        store._conn.execute(
            "INSERT INTO daily_bar (ts_code, trade_date, close) VALUES "
            "('000001.SZ', DATE '2020-01-02', 10), "
            "('600000.SH', DATE '2020-01-02', 11)"
        )

    result = backfill_historical_security_status(
        adapter,
        store_factory=lambda: DuckDBStore(db_path),
        start=date(2020, 1, 2),
        end=date(2020, 1, 2),
        eligible_keys=[target],
        source_as_of=date(2020, 1, 2),
        namechange_start=date(2020, 1, 1),
        ingested_at=INGESTED_AT,
        missing_only=False,
        request_interval_seconds=0,
    )
    with DuckDBStore(db_path, read_only=True) as store:
        stored = store.list_stock_status(date(2020, 1, 2), date(2020, 1, 2))
        exact = store.list_daily_security_keys(
            date(2020, 1, 2),
            date(2020, 1, 2),
            ts_codes=["600000.SH"],
        )
        injected = store.list_daily_security_keys(
            date(2020, 1, 2),
            date(2020, 1, 2),
            ts_codes=["600000.SH') OR TRUE --"],
        )

    assert result.eligible_count == 1
    assert [(row.ts_code, row.trade_date) for row in stored] == [
        (target.ts_code, target.trade_date)
    ]
    assert exact == [target]
    assert injected == []


def test_backfill_uses_daily_bar_eligibility_and_store_is_typed_idempotent(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "status.duckdb"
    with DuckDBStore(db_path) as store:
        store._conn.execute(
            "INSERT INTO daily_bar (ts_code, trade_date, close) VALUES "
            "('600000.SH', DATE '2020-01-02', 10), "
            "('600000.SH', DATE '2020-01-03', 11)"
        )
    adapter = _BackfillAdapter()
    result = backfill_historical_security_status(
        adapter,
        store_factory=lambda: DuckDBStore(db_path),
        start=date(2020, 1, 1),
        end=date(2020, 1, 3),
        ingested_at=INGESTED_AT,
        request_interval_seconds=0,
    )
    repeated = backfill_historical_security_status(
        adapter,
        store_factory=lambda: DuckDBStore(db_path),
        start=date(2020, 1, 1),
        end=date(2020, 1, 3),
        ingested_at=INGESTED_AT + timedelta(seconds=1),
        request_interval_seconds=0,
    )
    with DuckDBStore(db_path, read_only=True) as store:
        stored = store.list_stock_status(date(2020, 1, 1), date(2020, 1, 3))
        coverage = store.stock_status_coverage(
            date(2020, 1, 1), date(2020, 1, 3)
        )

    assert result.eligible_count == 2
    assert result.upserted_count == 2
    assert repeated.eligible_count == 0
    assert repeated.upserted_count == 0
    assert adapter.stock_dates == [
        date(2020, 1, 2),
        date(2020, 1, 3),
    ]
    assert len(stored) == 2
    assert all(isinstance(row, SecurityStatusDaily) for row in stored)
    assert coverage.expected_count == 2
    assert coverage.persisted_count == 2
    assert coverage.missing_count == 0
    assert coverage.unknown_count == 0


def test_security_status_audit_rules_emit_p0_for_missing_unknown_and_conflict(
    tmp_path: Path,
) -> None:
    from rquant.data_quality import historical_security_status_audit_rules, run_audit

    db_path = tmp_path / "audit.duckdb"
    with DuckDBStore(db_path) as store:
        store._conn.execute(
            "INSERT INTO daily_bar (ts_code, trade_date, close) VALUES "
            "('600000.SH', DATE '2020-01-02', 10), "
            "('000001.SZ', DATE '2020-01-02', 11), "
            "('000002.SZ', DATE '2020-01-02', 12)"
        )
        store.upsert_stock_status(
            (
                SecurityStatusDaily(
                    ts_code="000001.SZ",
                    trade_date=date(2020, 1, 2),
                    name=None,
                    is_st=None,
                    name_source="unknown",
                    st_source=None,
                    available_at=None,
                    ingested_at=INGESTED_AT,
                ),
                SecurityStatusDaily(
                    ts_code="000002.SZ",
                    trade_date=date(2020, 1, 2),
                    name=None,
                    is_st=None,
                    name_source="conflict",
                    st_source=None,
                    available_at=None,
                    ingested_at=INGESTED_AT,
                    conflict_reason="overlapping_namechange_intervals",
                ),
            )
        )

    rules = historical_security_status_audit_rules(
        date(2020, 1, 2), date(2020, 1, 2)
    )
    with DuckDBStore(db_path, read_only=True) as readonly:
        report = run_audit(readonly, rules, observed_at=INGESTED_AT)

    assert report.is_blocked is True
    assert {finding.severity for finding in report.findings} == {"P0"}
    assert len(rules) == 1
    findings = {finding.scope_key: finding for finding in report.findings}
    scopes = {
        category: f"{category}/2020-01-02/2020-01-02"
        for category in ("missing", "unknown", "conflict")
    }
    assert set(findings) == set(scopes.values())
    assert {scope: finding.evidence["count"] for scope, finding in findings.items()} == {
        scopes["missing"]: 1,
        scopes["unknown"]: 2,
        scopes["conflict"]: 1,
    }
    assert findings[scopes["missing"]].evidence["samples"] == [
        "600000.SH/2020-01-02"
    ]
    assert findings[scopes["conflict"]].evidence["samples"] == [
        "000002.SZ/2020-01-02"
    ]


def test_million_row_audit_aggregates_counts_with_bounded_stable_samples(
    tmp_path: Path,
) -> None:
    from rquant.data_quality import historical_security_status_audit_rules, run_audit

    db_path = tmp_path / "million-audit.duckdb"
    with DuckDBStore(db_path) as store:
        store._conn.execute(
            """
            INSERT INTO daily_bar (ts_code, trade_date, close)
            SELECT printf('%07d.SH', index), DATE '2020-01-02', 10
            FROM range(1000000) AS generated(index)
            """
        )
        store._conn.execute(
            """
            INSERT INTO stock_status_daily
            (ts_code, trade_date, name, is_st, name_source, st_source,
             available_at, ingested_at, conflict_reason)
            SELECT
                printf('%07d.SH', index),
                DATE '2020-01-02',
                CASE WHEN index < 100000 THEN 'known' ELSE NULL END,
                CASE WHEN index < 100000 THEN FALSE ELSE NULL END,
                CASE WHEN index < 100000 THEN 'tushare.namechange' ELSE 'unknown' END,
                CASE WHEN index < 100000 THEN 'tushare.namechange' ELSE NULL END,
                CASE WHEN index < 100000
                     THEN TIMESTAMPTZ '2020-01-02 01:25:00+00'
                     ELSE NULL END,
                TIMESTAMPTZ '2026-07-14 08:00:00+00',
                CASE WHEN index >= 300000
                     THEN 'overlapping_namechange_intervals'
                     ELSE NULL END
            FROM range(400000) AS generated(index)
            """
        )
        coverage = store.stock_status_coverage(
            date(2020, 1, 2),
            date(2020, 1, 2),
            sample_limit=20,
        )

    assert coverage.expected_count == 1_000_000
    assert coverage.persisted_count == 400_000
    assert coverage.missing_count == 600_000
    assert coverage.unknown_count == 300_000
    assert coverage.conflict_count == 100_000
    assert len(coverage.missing_samples) == 20
    assert len(coverage.unknown_samples) == 20
    assert len(coverage.conflict_samples) == 20

    rules = historical_security_status_audit_rules(
        date(2020, 1, 2), date(2020, 1, 2), sample_limit=20
    )
    with DuckDBStore(db_path, read_only=True) as readonly:
        first = run_audit(readonly, rules, observed_at=INGESTED_AT)
        second = run_audit(readonly, rules, observed_at=INGESTED_AT)

    assert len(first.findings) == 3
    assert first.issue_ids == second.issue_ids
    assert all(len(finding.evidence["samples"]) == 20 for finding in first.findings)


def test_legacy_invalid_rows_are_p0_and_all_samples_are_latest_first(
    tmp_path: Path,
) -> None:
    from rquant.data_quality import historical_security_status_audit_rules, run_audit

    db_path = tmp_path / "legacy-invalid-status.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute(
        "CREATE TABLE daily_bar (ts_code VARCHAR, trade_date DATE, close DOUBLE)"
    )
    conn.execute(
        """
        CREATE TABLE stock_status_daily (
            ts_code VARCHAR,
            trade_date DATE,
            name VARCHAR,
            is_st BOOLEAN,
            name_source VARCHAR,
            st_source VARCHAR,
            available_at TIMESTAMPTZ,
            ingested_at TIMESTAMPTZ,
            conflict_reason VARCHAR
        )
        """
    )
    categories = ("missing", "unknown", "conflict", "invalid")
    days_and_codes = (
        (date(2020, 1, 3), "000002.SZ"),
        (date(2020, 1, 3), "000001.SZ"),
        (date(2020, 1, 2), "000003.SZ"),
    )
    daily_rows: list[tuple[str, date, float]] = []
    status_rows: list[tuple[object, ...]] = []
    for category in categories:
        for day, suffix in days_and_codes:
            ts_code = f"{category[0].upper()}{suffix}"
            daily_rows.append((ts_code, day, 10.0))
            if category == "missing":
                continue
            if category == "unknown":
                status_rows.append(
                    (ts_code, day, None, None, "unknown", None, None, INGESTED_AT, None)
                )
            elif category == "conflict":
                status_rows.append(
                    (
                        ts_code,
                        day,
                        None,
                        None,
                        "conflict",
                        None,
                        None,
                        INGESTED_AT,
                        "overlap",
                    )
                )
            else:
                status_rows.append(
                    (
                        ts_code,
                        day,
                        None,
                        False,
                        "tushare.namechange",
                        "tushare.namechange",
                        datetime(2020, 1, 3, 9, 25, tzinfo=SHANGHAI),
                        INGESTED_AT,
                        None,
                    )
                )
    conn.executemany("INSERT INTO daily_bar VALUES (?, ?, ?)", daily_rows)
    conn.executemany(
        "INSERT INTO stock_status_daily VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        status_rows,
    )
    conn.close()

    rules = historical_security_status_audit_rules(
        date(2020, 1, 2), date(2020, 1, 3), sample_limit=3
    )
    with DuckDBStore(db_path, read_only=True) as readonly:
        coverage = readonly.stock_status_coverage(
            date(2020, 1, 2), date(2020, 1, 3), sample_limit=3
        )
        report = run_audit(readonly, rules, observed_at=INGESTED_AT)

    assert coverage.invalid_count == 3
    findings = {
        finding.scope_key.split("/", maxsplit=1)[0]: finding
        for finding in report.findings
    }
    assert set(findings) == set(categories)
    for category in ("missing", "conflict", "invalid"):
        prefix = category[0].upper()
        assert findings[category].evidence["samples"] == [
            f"{prefix}000001.SZ/2020-01-03",
            f"{prefix}000002.SZ/2020-01-03",
            f"{prefix}000003.SZ/2020-01-02",
        ]
    assert findings["unknown"].evidence["samples"] == [
        "C000001.SZ/2020-01-03",
        "C000002.SZ/2020-01-03",
        "U000001.SZ/2020-01-03",
    ]


def test_normalize_stock_st_rejects_invalid_positive_without_inventing_false() -> None:
    history = normalize_stock_st_history(
        pd.DataFrame(
            [
                {
                    "ts_code": "600000.SH",
                    "name": None,
                    "trade_date": "20200102",
                    "type": "S",
                    "type_name": "ST",
                }
            ]
        )
    )

    rows = materialize_security_status(
        [_key(date(2020, 1, 2))],
        NameChangeHistory(),
        history,
        ingested_at=INGESTED_AT,
    )

    assert history.observations == ()
    assert len(history.issues) == 1
    assert history.issues[0].blocking is True
    assert history.is_complete is False
    assert rows[0].is_st is None
    assert rows[0].conflict_reason is not None


@pytest.mark.parametrize(
    "response_dates",
    [
        ["20260711"],
        ["20260714", "20260711"],
    ],
)
def test_stock_st_response_dates_must_match_requested_trade_date(
    response_dates: list[str],
) -> None:
    requested = date(2026, 7, 14)
    history = normalize_stock_st_history(
        pd.DataFrame(
            [
                {
                    "ts_code": f"60000{index}.SH",
                    "name": "*ST错日样本",
                    "trade_date": response_date,
                    "type": "S",
                    "type_name": "ST",
                }
                for index, response_date in enumerate(response_dates)
            ]
        ),
        requested_trade_date=requested,
    )

    rows = materialize_security_status(
        [_key(requested)],
        NameChangeHistory(),
        history,
        ingested_at=INGESTED_AT,
    )

    assert history.is_complete is False
    assert {item.trade_date for item in history.observations} <= {requested}
    assert any(
        issue.trade_date == requested
        and issue.reason == "stock_st_response_trade_date_mismatch"
        for issue in history.issues
    )
    assert rows[0].is_st is None
    assert rows[0].conflict_reason == "invalid_stock_st_fields"


def test_stock_st_without_requested_date_cannot_claim_complete_list() -> None:
    history = normalize_stock_st_history(
        pd.DataFrame(
            [
                {
                    "ts_code": "600000.SH",
                    "name": "*ST样本",
                    "trade_date": "20260714",
                    "type": "S",
                    "type_name": "ST",
                }
            ]
        )
    )

    row = materialize_security_status(
        [_key(date(2026, 7, 14), "689009.SH")],
        NameChangeHistory(),
        history,
        ingested_at=INGESTED_AT,
    )[0]

    assert history.is_complete is False
    assert row.is_st is None
    assert row.st_source is None


def test_unscoped_invalid_namechange_row_fails_closed_for_all_eligible_keys() -> None:
    history = normalize_namechange_history(
        pd.DataFrame(
            [
                {
                    "ts_code": None,
                    "name": "无法归属",
                    "start_date": "20200101",
                    "end_date": None,
                    "ann_date": "20200101",
                    "change_reason": "bad key",
                }
            ]
        )
    )

    rows = materialize_security_status(
        [_key(date(2020, 1, 2)), _key(date(2020, 1, 2), "000001.SZ")],
        history,
        StockSTHistory(),
        ingested_at=INGESTED_AT,
    )

    assert all(row.is_st is None for row in rows)
    assert {row.conflict_reason for row in rows} == {"invalid_namechange_fields"}


def test_unscoped_invalid_stock_st_row_fails_closed_for_its_trade_date() -> None:
    history = normalize_stock_st_history(
        pd.DataFrame(
            [
                {
                    "ts_code": None,
                    "name": "*ST无法归属",
                    "trade_date": "20200102",
                    "type": "S",
                    "type_name": "ST",
                }
            ]
        )
    )

    rows = materialize_security_status(
        [_key(date(2020, 1, 2)), _key(date(2020, 1, 3))],
        NameChangeHistory(
            intervals=(
                _interval("浦发银行", date(2020, 1, 1), date(2020, 1, 3)),
            )
        ),
        history,
        ingested_at=INGESTED_AT,
    )

    by_day = {row.trade_date: row for row in rows}
    assert by_day[date(2020, 1, 2)].is_st is None
    assert by_day[date(2020, 1, 2)].conflict_reason == "invalid_stock_st_fields"
    assert by_day[date(2020, 1, 3)].is_st is False


@pytest.mark.parametrize(
    ("trade_date", "reason"),
    [
        (date(2015, 12, 31), "stock_st_unavailable_before_2016"),
        (date(2016, 1, 4), "empty_stock_st_response"),
    ],
)
def test_empty_stock_st_response_has_explicit_nonblocking_completeness_issue(
    trade_date: date,
    reason: str,
) -> None:
    history = normalize_stock_st_history(
        pd.DataFrame(),
        requested_trade_date=trade_date,
    )

    assert history.is_complete is False
    assert len(history.issues) == 1
    assert history.issues[0].blocking is False
    assert history.issues[0].trade_date == trade_date
    assert history.issues[0].reason == reason


def test_default_security_status_request_throttle_keeps_provider_margin() -> None:
    assert pytest.approx(60 / 480) == DEFAULT_REQUEST_INTERVAL_SECONDS


def _stored_status(
    *,
    ts_code: str = "600000.SH",
    name: str = "浦发银行",
    is_st: bool = False,
    ingested_at: datetime = INGESTED_AT,
) -> SecurityStatusDaily:
    return SecurityStatusDaily(
        ts_code=ts_code,
        trade_date=date(2020, 1, 2),
        name=name,
        is_st=is_st,
        name_source="tushare.namechange",
        st_source="tushare.namechange",
        available_at=datetime(2020, 1, 2, 9, 25, tzinfo=SHANGHAI),
        ingested_at=ingested_at,
    )


class _StatusConnectionProxy:
    def __init__(self, inner: duckdb.DuckDBPyConnection) -> None:
        self.inner = inner
        self.queries: list[str] = []
        self.registered_frames: list[pd.DataFrame] = []
        self.executemany_count = 0

    def execute(self, query: str, *args: Any, **kwargs: Any) -> Any:
        self.queries.append(" ".join(query.split()))
        return self.inner.execute(query, *args, **kwargs)

    def executemany(self, query: str, parameters: object) -> Any:
        self.executemany_count += 1
        self.queries.append("EXECUTEMANY " + " ".join(query.split()))
        return self.inner.executemany(query, parameters)

    def register(self, name: str, frame: pd.DataFrame) -> Any:
        self.registered_frames.append(frame)
        return self.inner.register(name, frame)

    def unregister(self, name: str) -> Any:
        return self.inner.unregister(name)

    def close(self) -> None:
        self.inner.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)


class _FailingStatusConnection(_StatusConnectionProxy):
    def __init__(
        self,
        inner: duckdb.DuckDBPyConnection,
        *,
        primary: BaseException,
        fail_on_commit: bool = False,
        rollback_error: BaseException | None = None,
    ) -> None:
        super().__init__(inner)
        self.primary = primary
        self.fail_on_commit = fail_on_commit
        self.rollback_error = rollback_error

    def execute(self, query: str, *args: Any, **kwargs: Any) -> Any:
        normalized = " ".join(query.split())
        self.queries.append(normalized)
        upper = normalized.upper()
        if upper == "ROLLBACK" and self.rollback_error is not None:
            raise self.rollback_error
        if self.fail_on_commit and upper == "COMMIT":
            raise self.primary
        if not self.fail_on_commit and upper.startswith(
            "INSERT INTO STOCK_STATUS_DAILY"
        ):
            raise self.primary
        return self.inner.execute(query, *args, **kwargs)

    def executemany(self, query: str, parameters: object) -> Any:
        self.executemany_count += 1
        self.queries.append("EXECUTEMANY " + " ".join(query.split()))
        raise self.primary


class _CoordinatedCommitConnection(_StatusConnectionProxy):
    def __init__(
        self,
        inner: duckdb.DuckDBPyConnection,
        *,
        commit_barrier: Barrier,
        winner_committed: Event,
        wins_commit: bool,
    ) -> None:
        super().__init__(inner)
        self.commit_barrier = commit_barrier
        self.winner_committed = winner_committed
        self.wins_commit = wins_commit
        self.failure_operation: str | None = None
        self.raw_error: duckdb.Error | None = None

    def execute(self, query: str, *args: Any, **kwargs: Any) -> Any:
        normalized = " ".join(query.split())
        self.queries.append(normalized)
        if normalized.upper() != "COMMIT":
            return self.inner.execute(query, *args, **kwargs)

        self.commit_barrier.wait(timeout=10)
        if self.wins_commit:
            result = self.inner.execute(query, *args, **kwargs)
            self.winner_committed.set()
            return result
        if not self.winner_committed.wait(timeout=10):
            raise TimeoutError("winning DuckDB transaction did not commit")
        try:
            return self.inner.execute(query, *args, **kwargs)
        except duckdb.Error as error:
            self.failure_operation = "commit"
            self.raw_error = error
            raise


class _ExistingQueryRaceConnection(_StatusConnectionProxy):
    def __init__(
        self,
        inner: duckdb.DuckDBPyConnection,
        *,
        query_barrier: Barrier,
        winner_committed: Event | None = None,
        wait_for_winner: bool = False,
        publish_commit: bool = False,
    ) -> None:
        super().__init__(inner)
        self.query_barrier = query_barrier
        self.winner_committed = winner_committed
        self.wait_for_winner = wait_for_winner
        self.publish_commit = publish_commit
        self.failure_operation: str | None = None
        self.raw_error: duckdb.Error | None = None

    def execute(self, query: str, *args: Any, **kwargs: Any) -> Any:
        normalized = " ".join(query.split())
        self.queries.append(normalized)
        upper = normalized.upper()
        existing_conflict_query = (
            upper.startswith("SELECT STAGE.TS_CODE")
            and "JOIN STOCK_STATUS_DAILY AS TARGET" in upper
        )
        if existing_conflict_query:
            result = self.inner.execute(query, *args, **kwargs)
            self.query_barrier.wait(timeout=10)
            if self.wait_for_winner and (
                self.winner_committed is None
                or not self.winner_committed.wait(timeout=10)
            ):
                raise TimeoutError("winning DuckDB transaction did not commit")
            return result

        try:
            result = self.inner.execute(query, *args, **kwargs)
        except duckdb.Error as error:
            if upper.startswith("INSERT INTO STOCK_STATUS_DAILY"):
                operation = "insert"
            elif upper == "COMMIT":
                operation = "commit"
            else:
                operation = None
            if operation is not None and self.raw_error is None:
                self.failure_operation = operation
                self.raw_error = error
            raise
        if self.publish_commit and upper == "COMMIT":
            if self.winner_committed is None:
                raise AssertionError("winner commit event is required")
            self.winner_committed.set()
        return result


def test_stock_status_write_boundary_revalidates_without_per_row_deepcopy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden_deepcopy(value: object) -> object:
        del value
        raise AssertionError("stock status write path must not deepcopy each row")

    monkeypatch.setattr(duckdb_storage, "deepcopy", forbidden_deepcopy)
    with DuckDBStore(tmp_path / "no-deepcopy.duckdb") as store:
        assert store.upsert_stock_status((_stored_status(),)) == 1


def test_stock_status_write_boundary_rejects_mutated_model(tmp_path: Path) -> None:
    row = _stored_status()
    object.__setattr__(row, "name", None)

    with (
        DuckDBStore(tmp_path / "mutated-model.duckdb") as store,
        pytest.raises(ValueError, match="write-boundary validation"),
    ):
        store.upsert_stock_status((row,))


def test_upsert_stock_status_uses_one_registered_insert_select_batch(
    tmp_path: Path,
) -> None:
    store = DuckDBStore(tmp_path / "bulk-status.duckdb")
    proxy = _StatusConnectionProxy(store._conn)
    store._conn = cast(Any, proxy)
    rows = tuple(
        _stored_status(ts_code=f"{index:06d}.SH") for index in range(1000)
    )
    try:
        assert store.upsert_stock_status(rows) == 1000
    finally:
        store.close()

    insert_queries = [
        query
        for query in proxy.queries
        if query.upper().startswith("INSERT INTO STOCK_STATUS_DAILY")
    ]
    assert proxy.executemany_count == 0
    assert len(proxy.registered_frames) == 1
    assert len(proxy.registered_frames[0]) == 1000
    assert len(insert_queries) == 1
    assert "SELECT" in insert_queries[0].upper()


def test_upsert_stock_status_checks_existing_facts_inside_transaction(
    tmp_path: Path,
) -> None:
    store = DuckDBStore(tmp_path / "transaction-order.duckdb")
    proxy = _StatusConnectionProxy(store._conn)
    store._conn = cast(Any, proxy)
    try:
        store.upsert_stock_status((_stored_status(),))
    finally:
        store.close()

    begin_index = proxy.queries.index("BEGIN")
    first_status_read = next(
        index
        for index, query in enumerate(proxy.queries)
        if query.upper().startswith("SELECT")
        and "STOCK_STATUS_DAILY" in query.upper()
    )
    assert begin_index < first_status_read


def test_upsert_stock_status_rolls_back_keyboard_interrupt(tmp_path: Path) -> None:
    store = DuckDBStore(tmp_path / "keyboard-interrupt.duckdb")
    proxy = _FailingStatusConnection(
        store._conn,
        primary=KeyboardInterrupt("stop"),
    )
    store._conn = cast(Any, proxy)
    try:
        with pytest.raises(KeyboardInterrupt, match="stop"):
            store.upsert_stock_status((_stored_status(),))
        assert "ROLLBACK" in proxy.queries
    finally:
        store.close()


def test_upsert_stock_status_preserves_primary_and_rollback_failures(
    tmp_path: Path,
) -> None:
    store = DuckDBStore(tmp_path / "rollback-failure.duckdb")
    proxy = _FailingStatusConnection(
        store._conn,
        primary=RuntimeError("primary write failure"),
        rollback_error=OSError("rollback failure"),
    )
    store._conn = cast(Any, proxy)
    try:
        with pytest.raises(BaseExceptionGroup) as raised:
            store.upsert_stock_status((_stored_status(),))
    finally:
        store.close()

    messages = {str(error) for error in raised.value.exceptions}
    assert messages == {"primary write failure", "rollback failure"}


def test_upsert_stock_status_commit_conflict_is_explicitly_retryable(
    tmp_path: Path,
) -> None:
    store = DuckDBStore(tmp_path / "commit-conflict.duckdb")
    proxy = _FailingStatusConnection(
        store._conn,
        primary=duckdb.TransactionException("forced commit conflict"),
        fail_on_commit=True,
    )
    store._conn = cast(Any, proxy)
    try:
        with pytest.raises(
            SecurityStatusConcurrentWriteError,
            match="retry stock status upsert",
        ):
            store.upsert_stock_status((_stored_status(),))
    finally:
        store.close()


def test_upsert_stock_status_check_constraint_error_is_not_retryable(
    tmp_path: Path,
) -> None:
    store = DuckDBStore(tmp_path / "check-constraint.duckdb")
    proxy = _FailingStatusConnection(
        store._conn,
        primary=duckdb.ConstraintException(
            "Constraint Error: CHECK constraint failed on table stock_status_daily"
        ),
    )
    store._conn = cast(Any, proxy)
    try:
        with pytest.raises(duckdb.ConstraintException, match="CHECK constraint"):
            store.upsert_stock_status((_stored_status(),))
    finally:
        store.close()


@pytest.mark.parametrize("attempt", range(5))
def test_real_duckdb_commit_conflict_is_retryable_after_auto_rollback(
    tmp_path: Path,
    attempt: int,
) -> None:
    db_path = tmp_path / f"real-commit-conflict-{attempt}.duckdb"
    winner = DuckDBStore(db_path)
    loser = DuckDBStore(db_path)
    commit_barrier = Barrier(2)
    winner_committed = Event()
    winner_proxy = _CoordinatedCommitConnection(
        winner._conn,
        commit_barrier=commit_barrier,
        winner_committed=winner_committed,
        wins_commit=True,
    )
    loser_proxy = _CoordinatedCommitConnection(
        loser._conn,
        commit_barrier=commit_barrier,
        winner_committed=winner_committed,
        wins_commit=False,
    )
    winner._conn = cast(Any, winner_proxy)
    loser._conn = cast(Any, loser_proxy)
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            winner_future = executor.submit(
                winner.upsert_stock_status,
                (_stored_status(),),
            )
            loser_future = executor.submit(
                loser.upsert_stock_status,
                (_stored_status(),),
            )

            assert winner_future.result(timeout=15) == 1
            with pytest.raises(
                SecurityStatusConcurrentWriteError,
                match="retry stock status upsert",
            ):
                loser_future.result(timeout=15)
        assert loser_proxy.failure_operation == "commit"
        assert isinstance(loser_proxy.raw_error, duckdb.TransactionException)
    finally:
        winner.close()
        loser.close()

    with DuckDBStore(db_path) as verifier:
        assert verifier._conn.execute(
            "SELECT COUNT(*) FROM stock_status_daily"
        ).fetchone()[0] == 1


@pytest.mark.parametrize("attempt", range(5))
def test_real_duckdb_insert_conflict_is_retryable_after_winner_commits(
    tmp_path: Path,
    attempt: int,
) -> None:
    db_path = tmp_path / f"real-insert-conflict-{attempt}.duckdb"
    winner = DuckDBStore(db_path)
    loser = DuckDBStore(db_path)
    query_barrier = Barrier(2)
    winner_committed = Event()
    winner_proxy = _ExistingQueryRaceConnection(
        winner._conn,
        query_barrier=query_barrier,
        winner_committed=winner_committed,
        publish_commit=True,
    )
    loser_proxy = _ExistingQueryRaceConnection(
        loser._conn,
        query_barrier=query_barrier,
        winner_committed=winner_committed,
        wait_for_winner=True,
    )
    winner._conn = cast(Any, winner_proxy)
    loser._conn = cast(Any, loser_proxy)
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            winner_future = executor.submit(
                winner.upsert_stock_status,
                (_stored_status(),),
            )
            loser_future = executor.submit(
                loser.upsert_stock_status,
                (_stored_status(),),
            )

            assert winner_future.result(timeout=15) == 1
            with pytest.raises(
                SecurityStatusConcurrentWriteError,
                match="retry stock status upsert",
            ):
                loser_future.result(timeout=15)
        assert loser_proxy.failure_operation == "insert"
        assert isinstance(loser_proxy.raw_error, duckdb.ConstraintException)
    finally:
        winner.close()
        loser.close()

    with DuckDBStore(db_path) as verifier:
        assert verifier._conn.execute(
            "SELECT COUNT(*) FROM stock_status_daily"
        ).fetchone()[0] == 1


@pytest.mark.parametrize("attempt", range(10))
def test_unordered_real_duckdb_upsert_race_is_retryable(
    tmp_path: Path,
    attempt: int,
) -> None:
    db_path = tmp_path / f"unordered-upsert-race-{attempt}.duckdb"
    stores = (DuckDBStore(db_path), DuckDBStore(db_path))
    query_barrier = Barrier(2)
    proxies = tuple(
        _ExistingQueryRaceConnection(
            store._conn,
            query_barrier=query_barrier,
        )
        for store in stores
    )
    for store, proxy in zip(stores, proxies, strict=True):
        store._conn = cast(Any, proxy)
    try:
        successful_results: list[int] = []
        retryable_errors: list[SecurityStatusConcurrentWriteError] = []
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = tuple(
                executor.submit(store.upsert_stock_status, (_stored_status(),))
                for store in stores
            )
            for future in futures:
                try:
                    successful_results.append(future.result(timeout=15))
                except SecurityStatusConcurrentWriteError as error:
                    retryable_errors.append(error)

        failed_proxies = [proxy for proxy in proxies if proxy.raw_error is not None]
        assert successful_results == [1]
        assert len(retryable_errors) == 1
        assert len(failed_proxies) == 1
        assert failed_proxies[0].failure_operation in {"insert", "commit"}
        assert isinstance(
            failed_proxies[0].raw_error,
            (duckdb.ConstraintException, duckdb.TransactionException),
        )
    finally:
        for store in stores:
            store.close()

    with DuckDBStore(db_path) as verifier:
        assert verifier._conn.execute(
            "SELECT COUNT(*) FROM stock_status_daily"
        ).fetchone()[0] == 1


@pytest.mark.parametrize("reverse", [False, True])
def test_upsert_rejects_batch_equal_time_fact_conflict_before_write(
    tmp_path: Path,
    reverse: bool,
) -> None:
    first = _stored_status()
    conflicting = _stored_status(name="*ST浦发", is_st=True)
    rows = (conflicting, first) if reverse else (first, conflicting)

    with DuckDBStore(tmp_path / f"batch-conflict-{reverse}.duckdb") as store:
        with pytest.raises(SecurityStatusWriteConflictError, match="600000.SH"):
            store.upsert_stock_status(rows)
        count = store._conn.execute(
            "SELECT COUNT(*) FROM stock_status_daily"
        ).fetchone()[0]

    assert count == 0


def test_upsert_rejects_stored_equal_time_conflict_before_any_batch_write(
    tmp_path: Path,
) -> None:
    existing = _stored_status()
    conflicting = _stored_status(name="*ST浦发", is_st=True)
    unrelated = _stored_status(ts_code="000001.SZ", name="平安银行")

    with DuckDBStore(tmp_path / "stored-conflict.duckdb") as store:
        store.upsert_stock_status((existing,))
        with pytest.raises(SecurityStatusWriteConflictError, match="600000.SH"):
            store.upsert_stock_status((unrelated, conflicting))
        rows = store.list_stock_status(date(2020, 1, 2), date(2020, 1, 2))

    assert rows == [existing]


def test_upsert_rejects_stored_equal_time_conflict_hidden_by_newer_batch_row(
    tmp_path: Path,
) -> None:
    existing = _stored_status()
    equal_time_conflict = _stored_status(name="*ST浦发", is_st=True)
    newer = _stored_status(
        name="浦发银行新名",
        ingested_at=INGESTED_AT + timedelta(seconds=1),
    )

    with DuckDBStore(tmp_path / "stored-hidden-conflict.duckdb") as store:
        store.upsert_stock_status((existing,))
        with pytest.raises(SecurityStatusWriteConflictError, match="600000.SH"):
            store.upsert_stock_status((equal_time_conflict, newer))
        rows = store.list_stock_status(date(2020, 1, 2), date(2020, 1, 2))

    assert rows == [existing]


def test_upsert_exact_equal_time_retry_is_idempotent(tmp_path: Path) -> None:
    row = _stored_status()

    with DuckDBStore(tmp_path / "exact-retry.duckdb") as store:
        assert store.upsert_stock_status((row, row)) == 1
        assert store.upsert_stock_status((row,)) == 1
        stored = store.list_stock_status(date(2020, 1, 2), date(2020, 1, 2))

    assert stored == [row]


def test_upsert_existing_transaction_rolls_back_with_caller(tmp_path: Path) -> None:
    row = _stored_status()

    with DuckDBStore(tmp_path / "status-caller-transaction.duckdb") as store:
        store._conn.execute("BEGIN")
        store.upsert_stock_status((row,), transaction_mode="existing")
        store._conn.execute("ROLLBACK")
        stored = store.list_stock_status(date(2020, 1, 2), date(2020, 1, 2))

    assert stored == []


def test_upsert_existing_transaction_keeps_equal_time_conflict_check(
    tmp_path: Path,
) -> None:
    existing = _stored_status()
    conflict = _stored_status(name="*ST浦发", is_st=True)

    with DuckDBStore(tmp_path / "status-caller-conflict.duckdb") as store:
        store.upsert_stock_status((existing,))
        store._conn.execute("BEGIN")
        with pytest.raises(SecurityStatusWriteConflictError):
            store.upsert_stock_status((conflict,), transaction_mode="existing")
        store._conn.execute("ROLLBACK")
        stored = store.list_stock_status(date(2020, 1, 2), date(2020, 1, 2))

    assert stored == [existing]


class _StreamingAdapter:
    def __init__(self, *, fail_on: date | None = None) -> None:
        self.fail_on = fail_on
        self.provider_calls: list[str] = []

    def namechange_raw(
        self,
        start_date: date,
        end_date: date,
        ts_code: str | None = None,
    ) -> pd.DataFrame:
        del start_date, end_date, ts_code
        self.provider_calls.append("namechange")
        return pd.DataFrame(
            [
                {
                    "ts_code": "600000.SH",
                    "name": "浦发银行",
                    "start_date": "20200101",
                    "end_date": None,
                    "ann_date": "20200101",
                    "change_reason": "更名",
                }
            ]
        )

    def stock_st_raw(self, trade_date: date) -> pd.DataFrame:
        self.provider_calls.append(f"stock_st:{trade_date.isoformat()}")
        if trade_date == self.fail_on:
            raise RuntimeError(f"stock_st failed on {trade_date.isoformat()}")
        return pd.DataFrame(
            columns=["ts_code", "name", "trade_date", "type", "type_name"]
        )


class _CappedStockSTAdapter(_StreamingAdapter):
    def stock_st_raw(self, trade_date: date) -> pd.DataFrame:
        self.provider_calls.append(f"stock_st:{trade_date.isoformat()}")
        return pd.DataFrame(
            {
                "ts_code": [f"{index:06d}.SH" for index in range(1000)],
                "name": ["ST测试"] * 1000,
                "trade_date": [trade_date.strftime("%Y%m%d")] * 1000,
                "type": ["S"] * 1000,
                "type_name": ["ST"] * 1000,
            }
        )


def _insert_three_status_days(store: DuckDBStore) -> None:
    store._conn.execute(
        "INSERT INTO daily_bar (ts_code, trade_date, close) VALUES "
        "('600000.SH', DATE '2020-01-02', 10), "
        "('600000.SH', DATE '2020-01-03', 11), "
        "('600000.SH', DATE '2020-01-06', 12)"
    )


def test_backfill_plans_exact_keys_before_remote_prefetch(tmp_path: Path) -> None:
    adapter = _StreamingAdapter()
    db_path = tmp_path / "streaming.duckdb"
    with DuckDBStore(db_path) as store:
        _insert_three_status_days(store)
    key_ranges: list[tuple[date, date]] = []

    class _TrackingStore(DuckDBStore):
        def list_daily_security_keys(
            self,
            start: date,
            end: date,
            *,
            ts_codes: Sequence[str] | None = None,
        ) -> list[DailySecurityKey]:
            key_ranges.append((start, end))
            return super().list_daily_security_keys(
                start,
                end,
                ts_codes=ts_codes,
            )

    result = backfill_historical_security_status(
        adapter,
        store_factory=lambda: _TrackingStore(db_path),
        start=date(2020, 1, 2),
        end=date(2020, 1, 6),
        source_as_of=date(2020, 1, 6),
        namechange_start=date(2020, 1, 1),
        ingested_at=INGESTED_AT,
        request_interval_seconds=0,
    )

    assert key_ranges == [(date(2020, 1, 2), date(2020, 1, 6))]
    assert result.eligible_count == 3
    assert result.upserted_count == 3


def test_backfill_resume_processes_only_incomplete_dates(tmp_path: Path) -> None:
    adapter = _StreamingAdapter()
    db_path = tmp_path / "resume-incomplete.duckdb"
    with DuckDBStore(db_path) as store:
        _insert_three_status_days(store)
        store.upsert_stock_status(
            (
                _stored_status(),
                SecurityStatusDaily(
                    ts_code="600000.SH",
                    trade_date=date(2020, 1, 3),
                    name=None,
                    is_st=None,
                    name_source="unknown",
                    st_source=None,
                    available_at=None,
                    ingested_at=INGESTED_AT,
                ),
            )
        )

    first = backfill_historical_security_status(
        adapter,
        store_factory=lambda: DuckDBStore(db_path),
        start=date(2020, 1, 2),
        end=date(2020, 1, 6),
        source_as_of=date(2020, 1, 6),
        namechange_start=date(2020, 1, 1),
        ingested_at=INGESTED_AT + timedelta(seconds=1),
        request_interval_seconds=0,
    )
    calls_after_first = list(adapter.provider_calls)
    second = backfill_historical_security_status(
        adapter,
        store_factory=lambda: DuckDBStore(db_path),
        start=date(2020, 1, 2),
        end=date(2020, 1, 6),
        source_as_of=date(2020, 1, 6),
        namechange_start=date(2020, 1, 1),
        ingested_at=INGESTED_AT + timedelta(seconds=2),
        request_interval_seconds=0,
    )

    assert calls_after_first == [
        "namechange",
        "stock_st:2020-01-03",
        "stock_st:2020-01-06",
    ]
    assert adapter.provider_calls == calls_after_first
    assert first.eligible_count == 2
    assert first.upserted_count == 2
    assert second.eligible_count == 0
    assert second.upserted_count == 0


def test_backfill_missing_only_refreshes_individual_key_not_complete_peer(
    tmp_path: Path,
) -> None:
    adapter = _ScopedAdapter()
    day = date(2020, 1, 2)
    complete = _stored_status(ts_code="000001.SZ", name="平安银行")
    db_path = tmp_path / "individual-missing-key.duckdb"
    snapshot_sql = """
        SELECT ts_code, trade_date::VARCHAR, name, is_st, name_source,
               st_source, available_at::VARCHAR, ingested_at::VARCHAR,
               conflict_reason
        FROM stock_status_daily
        WHERE ts_code = ? AND trade_date = ?
    """
    with DuckDBStore(db_path) as store:
        store._conn.execute(
            "INSERT INTO daily_bar (ts_code, trade_date, close) VALUES "
            "('000001.SZ', DATE '2020-01-02', 10), "
            "('600000.SH', DATE '2020-01-02', 11)"
        )
        store.upsert_stock_status((complete,))
        before = store._conn.execute(
            snapshot_sql,
            [complete.ts_code, day],
        ).fetchone()

    result = backfill_historical_security_status(
        adapter,
        store_factory=lambda: DuckDBStore(db_path),
        start=day,
        end=day,
        source_as_of=day,
        namechange_start=date(2020, 1, 1),
        ingested_at=INGESTED_AT + timedelta(seconds=1),
        request_interval_seconds=0,
    )
    with DuckDBStore(db_path, read_only=True) as store:
        after = store._conn.execute(
            snapshot_sql,
            [complete.ts_code, day],
        ).fetchone()
        stored = store.list_stock_status(day, day)

    assert result.eligible_count == 1
    assert before == after
    assert [(row.ts_code, row.ingested_at) for row in stored] == [
        ("000001.SZ", INGESTED_AT),
        ("600000.SH", INGESTED_AT + timedelta(seconds=1)),
    ]


def test_backfill_missing_only_false_forces_complete_date_refresh(
    tmp_path: Path,
) -> None:
    adapter = _StreamingAdapter()
    db_path = tmp_path / "force-refresh.duckdb"
    with DuckDBStore(db_path) as store:
        store._conn.execute(
            "INSERT INTO daily_bar (ts_code, trade_date, close) VALUES "
            "('600000.SH', DATE '2020-01-02', 10)"
        )
        store.upsert_stock_status((_stored_status(),))

    result = backfill_historical_security_status(
        adapter,
        store_factory=lambda: DuckDBStore(db_path),
        start=date(2020, 1, 2),
        end=date(2020, 1, 2),
        source_as_of=date(2020, 1, 2),
        namechange_start=date(2020, 1, 1),
        ingested_at=INGESTED_AT + timedelta(seconds=1),
        missing_only=False,
        request_interval_seconds=0,
    )

    assert adapter.provider_calls == ["namechange", "stock_st:2020-01-02"]
    assert result.eligible_count == 1
    assert result.upserted_count == 1


def test_backfill_uses_injectable_request_throttle(tmp_path: Path) -> None:
    adapter = _StreamingAdapter()
    sleeps: list[float] = []
    db_path = tmp_path / "throttle.duckdb"
    with DuckDBStore(db_path) as store:
        store._conn.execute(
            "INSERT INTO daily_bar (ts_code, trade_date, close) VALUES "
            "('600000.SH', DATE '2020-01-02', 10), "
            "('600000.SH', DATE '2020-01-03', 11)"
        )
    backfill_historical_security_status(
        adapter,
        store_factory=lambda: DuckDBStore(db_path),
        start=date(2020, 1, 2),
        end=date(2020, 1, 3),
        source_as_of=date(2020, 1, 3),
        namechange_start=date(2020, 1, 1),
        ingested_at=INGESTED_AT,
        request_interval_seconds=0.12,
        sleep=sleeps.append,
    )

    assert adapter.provider_calls == [
        "namechange",
        "stock_st:2020-01-02",
        "stock_st:2020-01-03",
    ]
    assert sleeps == [0.12, 0.12, 0.12]


def test_stock_st_at_provider_cap_fails_before_writing_that_day(
    tmp_path: Path,
) -> None:
    adapter = _CappedStockSTAdapter()
    db_path = tmp_path / "stock-st-cap.duckdb"
    with DuckDBStore(db_path) as store:
        store._conn.execute(
            "INSERT INTO daily_bar (ts_code, trade_date, close) VALUES "
            "('600000.SH', DATE '2020-01-02', 10)"
        )
    with pytest.raises(StockSTTruncatedError, match="1000"):
        backfill_historical_security_status(
            adapter,
            store_factory=lambda: DuckDBStore(db_path),
            start=date(2020, 1, 2),
            end=date(2020, 1, 2),
            source_as_of=date(2020, 1, 2),
            namechange_start=date(2020, 1, 1),
            ingested_at=INGESTED_AT,
            request_interval_seconds=0,
        )
    with DuckDBStore(db_path, read_only=True) as store:
        stored_count = store._conn.execute(
            "SELECT COUNT(*) FROM stock_status_daily"
        ).fetchone()[0]
    assert stored_count == 0


def test_post_2016_empty_stock_st_warns_without_negating_namechange_fact(
    tmp_path: Path,
) -> None:
    adapter = _StreamingAdapter()
    db_path = tmp_path / "stock-st-empty-warning.duckdb"
    with DuckDBStore(db_path) as store:
        store._conn.execute(
            "INSERT INTO daily_bar (ts_code, trade_date, close) VALUES "
            "('600000.SH', DATE '2020-01-02', 10)"
        )
    result = backfill_historical_security_status(
        adapter,
        store_factory=lambda: DuckDBStore(db_path),
        start=date(2020, 1, 2),
        end=date(2020, 1, 2),
        source_as_of=date(2020, 1, 2),
        namechange_start=date(2020, 1, 1),
        ingested_at=INGESTED_AT,
        request_interval_seconds=0,
    )
    with DuckDBStore(db_path, read_only=True) as store:
        stored = store.list_stock_status(date(2020, 1, 2), date(2020, 1, 2))

    assert result.source_issue_count == 1
    assert result.source_issue_dates == (date(2020, 1, 2),)
    assert len(stored) == 1
    assert stored[0].name == "浦发银行"
    assert stored[0].is_st is False
    assert stored[0].conflict_reason is None


def test_strict_stock_st_crosscheck_fails_closed_on_empty_response(
    tmp_path: Path,
) -> None:
    adapter = _StreamingAdapter()
    db_path = tmp_path / "stock-st-strict.duckdb"
    with DuckDBStore(db_path) as store:
        store._conn.execute(
            "INSERT INTO daily_bar (ts_code, trade_date, close) VALUES "
            "('600000.SH', DATE '2020-01-02', 10)"
        )
    with pytest.raises(StockSTIncompleteError, match="incomplete"):
        backfill_historical_security_status(
            adapter,
            store_factory=lambda: DuckDBStore(db_path),
            start=date(2020, 1, 2),
            end=date(2020, 1, 2),
            source_as_of=date(2020, 1, 2),
            namechange_start=date(2020, 1, 1),
            ingested_at=INGESTED_AT,
            strict_stock_st_crosscheck=True,
            request_interval_seconds=0,
        )
    with DuckDBStore(db_path, read_only=True) as store:
        stored_count = store._conn.execute(
            "SELECT COUNT(*) FROM stock_status_daily"
        ).fetchone()[0]
    assert stored_count == 0


def test_backfill_remote_failure_writes_no_partial_batch_and_audit_reports_all(
    tmp_path: Path,
) -> None:
    from rquant.data_quality import historical_security_status_audit_rules, run_audit

    db_path = tmp_path / "partial.duckdb"
    adapter = _StreamingAdapter(fail_on=date(2020, 1, 3))
    with DuckDBStore(db_path) as store:
        _insert_three_status_days(store)
    with pytest.raises(RuntimeError, match="2020-01-03"):
        backfill_historical_security_status(
            adapter,
            store_factory=lambda: DuckDBStore(db_path),
            start=date(2020, 1, 2),
            end=date(2020, 1, 6),
            source_as_of=date(2020, 1, 6),
            namechange_start=date(2020, 1, 1),
            ingested_at=INGESTED_AT,
            request_interval_seconds=0,
        )
    with DuckDBStore(db_path, read_only=True) as store:
        stored = store.list_stock_status(date(2020, 1, 2), date(2020, 1, 6))
        coverage = store.stock_status_coverage(date(2020, 1, 2), date(2020, 1, 6))

    assert stored == []
    assert coverage.persisted_count == 0
    assert coverage.missing_count == 3

    rules = historical_security_status_audit_rules(
        date(2020, 1, 2), date(2020, 1, 6)
    )
    with DuckDBStore(db_path, read_only=True) as readonly:
        report = run_audit(readonly, rules, observed_at=INGESTED_AT)
    missing = next(
        finding for finding in report.findings if finding.scope_key.startswith("missing/")
    )
    assert missing.severity == "P0"
    assert missing.evidence["count"] == 3
