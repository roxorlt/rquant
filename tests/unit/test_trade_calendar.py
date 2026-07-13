"""Authoritative civil-date trade calendar contracts."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest
from pydantic import ValidationError

from rquant.adapter.tushare import TushareAdapter
from rquant.storage.duckdb import DuckDBStore
from rquant.trade_calendar import (
    TradeCalendarConflictError,
    TradeCalendarDay,
    TradeCalendarGapError,
    normalize_trade_calendar,
    refresh_trade_calendar,
)

UPDATED_AT = datetime(2026, 1, 6, 8, 30, tzinfo=UTC)


def _day(
    cal_date: date,
    is_open: bool,
    *,
    pretrade_date: date | None = None,
    source: str = "tushare",
    updated_at: datetime = UPDATED_AT,
) -> TradeCalendarDay:
    return TradeCalendarDay(
        exchange="SSE",
        cal_date=cal_date,
        is_open=is_open,
        pretrade_date=pretrade_date,
        source=source,
        updated_at=updated_at,
    )


def _complete_days() -> list[TradeCalendarDay]:
    return [
        _day(date(2026, 1, 1), False, pretrade_date=date(2025, 12, 31)),
        _day(date(2026, 1, 2), True, pretrade_date=date(2025, 12, 31)),
        _day(date(2026, 1, 3), False, pretrade_date=date(2026, 1, 2)),
        _day(date(2026, 1, 4), False, pretrade_date=date(2026, 1, 2)),
        _day(date(2026, 1, 5), True, pretrade_date=date(2026, 1, 2)),
    ]


@pytest.fixture()
def store(tmp_path: Path) -> DuckDBStore:
    calendar_store = DuckDBStore(tmp_path / "calendar.duckdb")
    yield calendar_store
    calendar_store.close()


def test_trade_calendar_day_is_frozen_strict_and_normalizes_utc() -> None:
    china_time = datetime(
        2026, 1, 6, 16, 30, tzinfo=timezone(timedelta(hours=8))
    )
    row = _day(date(2026, 1, 5), True, updated_at=china_time)

    assert row.updated_at == UPDATED_AT
    assert row.updated_at.tzinfo is UTC
    with pytest.raises(ValidationError, match="frozen"):
        row.source = "changed"  # type: ignore[misc]
    with pytest.raises(ValidationError, match="extra"):
        TradeCalendarDay(
            exchange="SSE",
            cal_date=date(2026, 1, 5),
            is_open=True,
            updated_at=UPDATED_AT,
            unexpected=True,
        )


def test_trade_calendar_day_rejects_naive_time_and_invalid_pretrade_date() -> None:
    with pytest.raises(ValidationError, match="timezone-aware"):
        _day(
            date(2026, 1, 5),
            True,
            updated_at=datetime(2026, 1, 6, 8, 30),
        )
    with pytest.raises(ValidationError, match="strictly before"):
        _day(
            date(2026, 1, 5),
            True,
            pretrade_date=date(2026, 1, 5),
        )


def test_normalize_trade_calendar_accepts_provider_types_without_truthiness() -> None:
    raw = pd.DataFrame(
        [
            {
                "exchange": "SSE",
                "cal_date": "20260101",
                "is_open": "0",
                "pretrade_date": "20251231",
            },
            {
                "exchange": "SSE",
                "cal_date": pd.Timestamp("2026-01-02"),
                "is_open": "1",
                "pretrade_date": pd.Timestamp("2025-12-31"),
            },
            {
                "exchange": "SSE",
                "cal_date": date(2026, 1, 3),
                "is_open": 0,
                "pretrade_date": None,
            },
            {
                "exchange": "SSE",
                "cal_date": date(2026, 1, 4),
                "is_open": 1,
                "pretrade_date": "",
            },
            {
                "exchange": "SSE",
                "cal_date": date(2026, 1, 5),
                "is_open": False,
                "pretrade_date": pd.NaT,
            },
            {
                "exchange": "SSE",
                "cal_date": date(2026, 1, 6),
                "is_open": True,
                "pretrade_date": date(2026, 1, 5),
            },
        ]
    )

    rows = normalize_trade_calendar(raw, updated_at=UPDATED_AT)

    assert [row.cal_date for row in rows] == [
        date(2026, 1, day) for day in range(1, 7)
    ]
    assert [row.is_open for row in rows] == [False, True, False, True, False, True]
    assert rows[0].pretrade_date == date(2025, 12, 31)
    assert rows[2].pretrade_date is None
    assert all(row.updated_at == UPDATED_AT for row in rows)


@pytest.mark.parametrize("unknown", ["yes", 2, None])
def test_normalize_trade_calendar_rejects_unknown_open_values(unknown: object) -> None:
    raw = pd.DataFrame(
        [{"exchange": "SSE", "cal_date": "20260105", "is_open": unknown}]
    )

    with pytest.raises(ValueError, match="is_open"):
        normalize_trade_calendar(raw, updated_at=UPDATED_AT)


class _StubTradeCalPro:
    def __init__(
        self,
        frame: pd.DataFrame | None = None,
        *,
        error: Exception | None = None,
    ) -> None:
        self.frame = pd.DataFrame() if frame is None else frame
        self.error = error
        self.calls: list[dict[str, object]] = []

    def trade_cal(self, **kwargs: object) -> pd.DataFrame:
        self.calls.append(dict(kwargs))
        if self.error is not None:
            raise self.error
        return self.frame.copy()


def _adapter_with_pro(pro: _StubTradeCalPro) -> TushareAdapter:
    adapter = TushareAdapter.__new__(TushareAdapter)
    adapter._pro = pro
    adapter._primary_token = "primary"
    adapter._backup_token = ""
    adapter._using_backup = False
    return adapter


def _provider_calendar() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "exchange": "SSE",
                "cal_date": "20260105",
                "is_open": "1",
                "pretrade_date": "20260102",
            },
            {
                "exchange": "SSE",
                "cal_date": "20260104",
                "is_open": "0",
                "pretrade_date": "20260102",
            },
            {
                "exchange": "SSE",
                "cal_date": "20260103",
                "is_open": "0",
                "pretrade_date": "20260102",
            },
            {
                "exchange": "SSE",
                "cal_date": "20260102",
                "is_open": "1",
                "pretrade_date": "20251231",
            },
            {
                "exchange": "SSE",
                "cal_date": "20260101",
                "is_open": "0",
                "pretrade_date": "20251231",
            },
        ]
    )


def test_trade_cal_raw_keeps_closed_days_and_wrapper_uses_one_call() -> None:
    pro = _StubTradeCalPro(_provider_calendar())
    adapter = _adapter_with_pro(pro)

    raw = adapter.trade_cal_raw(date(2026, 1, 1), date(2026, 1, 5))
    open_days = adapter.trade_cal(date(2026, 1, 1), date(2026, 1, 5))

    assert list(raw.columns) == [
        "exchange",
        "cal_date",
        "is_open",
        "pretrade_date",
    ]
    assert raw["cal_date"].tolist() == [date(2026, 1, day) for day in range(1, 6)]
    assert raw["is_open"].tolist() == [False, True, False, False, True]
    assert open_days == [date(2026, 1, 2), date(2026, 1, 5)]
    assert len(pro.calls) == 2
    assert all("is_open" not in call for call in pro.calls)
    assert pro.calls[0] == {
        "exchange": "SSE",
        "start_date": "20260101",
        "end_date": "20260105",
    }


def test_trade_cal_raw_retains_backup_switch(monkeypatch: pytest.MonkeyPatch) -> None:
    primary = _StubTradeCalPro(error=RuntimeError("primary failed"))
    backup = _StubTradeCalPro(_provider_calendar())
    adapter = _adapter_with_pro(primary)

    def switch() -> bool:
        adapter._pro = backup
        return True

    monkeypatch.setattr(adapter, "_switch_to_backup", switch)

    raw = adapter.trade_cal_raw(date(2026, 1, 1), date(2026, 1, 5))

    assert len(raw) == 5
    assert len(primary.calls) == 1
    assert len(backup.calls) == 1


def test_trade_cal_raw_empty_and_reversed_ranges_are_explicit() -> None:
    pro = _StubTradeCalPro(pd.DataFrame())
    adapter = _adapter_with_pro(pro)

    empty = adapter.trade_cal_raw(date(2026, 1, 1), date(2026, 1, 5))
    reversed_range = adapter.trade_cal_raw(date(2026, 1, 5), date(2026, 1, 1))

    assert empty.empty
    assert list(empty.columns) == [
        "exchange",
        "cal_date",
        "is_open",
        "pretrade_date",
    ]
    assert reversed_range.empty
    assert len(pro.calls) == 1


def test_trade_cal_raw_and_wrapper_deduplicate_exact_provider_rows() -> None:
    provider = pd.concat(
        [_provider_calendar(), _provider_calendar().iloc[[3]]],
        ignore_index=True,
    )
    pro = _StubTradeCalPro(provider)
    adapter = _adapter_with_pro(pro)

    raw = adapter.trade_cal_raw(date(2026, 1, 1), date(2026, 1, 5))
    open_days = adapter.trade_cal(date(2026, 1, 1), date(2026, 1, 5))

    assert len(raw) == 5
    assert open_days == [date(2026, 1, 2), date(2026, 1, 5)]


def test_upsert_get_list_and_missing_dates_are_typed_and_idempotent(
    store: DuckDBStore,
) -> None:
    rows = _complete_days()

    assert store.upsert_trade_calendar(rows) == 5
    assert store.upsert_trade_calendar(rows) == 5

    stored = store.get_trade_calendar_day("SSE", date(2026, 1, 2))
    assert stored == rows[1]
    assert stored is not None
    assert stored.updated_at.tzinfo is UTC
    assert store.list_trade_calendar(
        "SSE", date(2026, 1, 2), date(2026, 1, 4)
    ) == rows[1:4]
    assert store.missing_trade_calendar_dates(
        "SSE", date(2026, 1, 1), date(2026, 1, 6)
    ) == [date(2026, 1, 6)]
    assert store._conn.execute("SELECT COUNT(*) FROM trade_calendar").fetchone()[0] == 5


def test_upsert_older_row_never_overwrites_newer_stored_facts(
    store: DuckDBStore,
) -> None:
    current = _day(
        date(2026, 1, 5),
        True,
        pretrade_date=date(2026, 1, 2),
        source="current",
    )
    stale = _day(
        date(2026, 1, 5),
        False,
        pretrade_date=date(2025, 12, 31),
        source="stale",
        updated_at=UPDATED_AT - timedelta(hours=1),
    )
    store.upsert_trade_calendar([current])

    assert store.upsert_trade_calendar([stale]) == 1
    assert store.get_trade_calendar_day("SSE", date(2026, 1, 5)) == current


def test_upsert_newer_row_updates_stored_facts_and_provenance(
    store: DuckDBStore,
) -> None:
    current = _day(
        date(2026, 1, 5),
        True,
        pretrade_date=date(2026, 1, 2),
        source="current",
    )
    newer = _day(
        date(2026, 1, 5),
        False,
        pretrade_date=date(2025, 12, 31),
        source="newer",
        updated_at=UPDATED_AT + timedelta(hours=1),
    )
    store.upsert_trade_calendar([current])

    assert store.upsert_trade_calendar([newer]) == 1
    assert store.get_trade_calendar_day("SSE", date(2026, 1, 5)) == newer


def test_upsert_equal_time_identical_facts_is_idempotent(
    store: DuckDBStore,
) -> None:
    current = _day(
        date(2026, 1, 5),
        True,
        pretrade_date=date(2026, 1, 2),
        source="current",
    )
    same_facts = current.model_copy(update={"source": "duplicate-provider"})
    store.upsert_trade_calendar([current])

    assert store.upsert_trade_calendar([same_facts]) == 1
    assert store.get_trade_calendar_day("SSE", date(2026, 1, 5)) == current


def test_upsert_equal_time_conflicting_facts_raises_without_overwrite(
    store: DuckDBStore,
) -> None:
    current = _day(
        date(2026, 1, 5),
        True,
        pretrade_date=date(2026, 1, 2),
    )
    conflict = _day(
        date(2026, 1, 5),
        False,
        pretrade_date=date(2026, 1, 2),
    )
    store.upsert_trade_calendar([current])

    with pytest.raises(TradeCalendarConflictError, match="equal updated_at"):
        store.upsert_trade_calendar([conflict])

    assert store.get_trade_calendar_day("SSE", date(2026, 1, 5)) == current


def test_upsert_direct_duplicates_select_newest_independent_of_input_order(
    store: DuckDBStore,
) -> None:
    older = _day(
        date(2026, 1, 5),
        True,
        source="older",
        updated_at=UPDATED_AT - timedelta(hours=1),
    )
    newer = _day(
        date(2026, 1, 5),
        False,
        source="newer",
        updated_at=UPDATED_AT + timedelta(hours=1),
    )

    assert store.upsert_trade_calendar([newer, older]) == 1
    assert store.get_trade_calendar_day("SSE", date(2026, 1, 5)) == newer


def test_upsert_exact_duplicates_are_deduplicated(store: DuckDBStore) -> None:
    row = _day(date(2026, 1, 5), True)

    assert store.upsert_trade_calendar([row, row]) == 1
    assert store._conn.execute("SELECT COUNT(*) FROM trade_calendar").fetchone()[0] == 1


def test_upsert_duplicate_conflict_writes_zero_rows(store: DuckDBStore) -> None:
    first = _day(date(2026, 1, 5), True)
    conflict = _day(date(2026, 1, 5), False)
    unrelated = _day(date(2026, 1, 6), True)

    with pytest.raises(TradeCalendarConflictError, match="equal updated_at"):
        store.upsert_trade_calendar([first, unrelated, conflict])

    assert store._conn.execute("SELECT COUNT(*) FROM trade_calendar").fetchone()[0] == 0


def test_upsert_rejects_equal_time_conflict_hidden_behind_newer_duplicate(
    store: DuckDBStore,
) -> None:
    older = _day(
        date(2026, 1, 5),
        True,
        updated_at=UPDATED_AT - timedelta(hours=1),
    )
    newer = _day(
        date(2026, 1, 5),
        True,
        updated_at=UPDATED_AT + timedelta(hours=1),
    )
    older_conflict = _day(
        date(2026, 1, 5),
        False,
        updated_at=older.updated_at,
    )

    with pytest.raises(TradeCalendarConflictError, match="equal updated_at"):
        store.upsert_trade_calendar([older, newer, older_conflict])

    assert store._conn.execute("SELECT COUNT(*) FROM trade_calendar").fetchone()[0] == 0


def test_known_open_weekend_and_legal_holiday_are_not_calendar_gaps(
    store: DuckDBStore,
) -> None:
    store.upsert_trade_calendar(_complete_days())

    assert store.is_trading_day("SSE", date(2026, 1, 2)) is True
    assert store.is_trading_day("SSE", date(2026, 1, 3)) is False
    assert store.is_trading_day("SSE", date(2026, 1, 1)) is False


def test_previous_next_and_latest_are_strict_or_inclusive_as_documented(
    store: DuckDBStore,
) -> None:
    store.upsert_trade_calendar(_complete_days())

    assert store.previous_trading_day(date(2026, 1, 5)) == date(2026, 1, 2)
    assert store.next_trading_day(date(2026, 1, 2)) == date(2026, 1, 5)
    assert store.latest_trading_day(date(2026, 1, 4)) == date(2026, 1, 2)
    assert store.latest_trading_day(date(2026, 1, 5)) == date(2026, 1, 5)


def test_missing_anchor_and_interior_gap_raise_typed_gap_error(
    store: DuckDBStore,
) -> None:
    store.upsert_trade_calendar(_complete_days())

    with pytest.raises(TradeCalendarGapError) as missing_anchor:
        store.previous_trading_day(date(2026, 1, 6))
    assert missing_anchor.value.exchange == "SSE"
    assert missing_anchor.value.missing_dates == (date(2026, 1, 6),)

    store._conn.execute(
        "DELETE FROM trade_calendar WHERE exchange = 'SSE' AND cal_date = DATE '2026-01-03'"
    )
    with pytest.raises(TradeCalendarGapError) as interior_gap:
        store.previous_trading_day(date(2026, 1, 5))
    assert interior_gap.value.missing_dates == (date(2026, 1, 3),)


def test_missing_day_never_falls_back_to_daily_bar(store: DuckDBStore) -> None:
    store._conn.execute(
        "INSERT INTO daily_bar (ts_code, trade_date, close) "
        "VALUES ('600000.SH', DATE '2026-01-06', 10.0)"
    )

    with pytest.raises(TradeCalendarGapError, match="2026-01-06"):
        store.is_trading_day("SSE", date(2026, 1, 6))


def test_no_open_candidate_inside_stored_coverage_raises(store: DuckDBStore) -> None:
    store.upsert_trade_calendar([_day(date(2026, 1, 1), False)])

    with pytest.raises(TradeCalendarGapError, match="no previous trading day"):
        store.previous_trading_day(date(2026, 1, 1))
    with pytest.raises(TradeCalendarGapError, match="no latest trading day"):
        store.latest_trading_day(date(2026, 1, 1))


class _RefreshAdapter:
    def __init__(self, frame: pd.DataFrame) -> None:
        self.frame = frame
        self.calls: list[tuple[date, date, str]] = []

    def trade_cal_raw(
        self, start: date, end: date, exchange: str = "SSE"
    ) -> pd.DataFrame:
        self.calls.append((start, end, exchange))
        return self.frame.copy()


def test_refresh_fetches_normalizes_and_upserts_full_civil_range(
    store: DuckDBStore,
) -> None:
    adapter = _RefreshAdapter(_provider_calendar())

    result = refresh_trade_calendar(
        adapter,
        store,
        exchange="SSE",
        start=date(2026, 1, 1),
        end=date(2026, 1, 5),
        updated_at=UPDATED_AT,
    )

    assert result.exchange == "SSE"
    assert result.requested_days == 5
    assert result.fetched_days == 5
    assert result.upserted_days == 5
    assert adapter.calls == [(date(2026, 1, 1), date(2026, 1, 5), "SSE")]
    assert store.missing_trade_calendar_dates(
        "SSE", date(2026, 1, 1), date(2026, 1, 5)
    ) == []
    with pytest.raises(ValidationError, match="frozen"):
        result.fetched_days = 0  # type: ignore[misc]


def test_refresh_empty_source_cannot_claim_coverage(store: DuckDBStore) -> None:
    adapter = _RefreshAdapter(pd.DataFrame())

    with pytest.raises(TradeCalendarGapError, match="returned no calendar rows"):
        refresh_trade_calendar(
            adapter,
            store,
            exchange="SSE",
            start=date(2026, 1, 1),
            end=date(2026, 1, 5),
            updated_at=UPDATED_AT,
        )


def test_refresh_exact_duplicates_count_and_write_unique_civil_days(
    store: DuckDBStore,
) -> None:
    provider = pd.concat(
        [_provider_calendar(), _provider_calendar().iloc[[3]]],
        ignore_index=True,
    )

    result = refresh_trade_calendar(
        _RefreshAdapter(provider),
        store,
        exchange="SSE",
        start=date(2026, 1, 1),
        end=date(2026, 1, 5),
        updated_at=UPDATED_AT,
    )

    assert result.fetched_days == 5
    assert result.upserted_days == 5
    assert store._conn.execute("SELECT COUNT(*) FROM trade_calendar").fetchone()[0] == 5


def test_refresh_conflicting_duplicates_write_zero_rows(
    store: DuckDBStore,
) -> None:
    conflict = _provider_calendar().iloc[[3]].copy()
    conflict.loc[:, "is_open"] = "0"
    provider = pd.concat([_provider_calendar(), conflict], ignore_index=True)

    with pytest.raises(TradeCalendarConflictError, match="equal updated_at"):
        refresh_trade_calendar(
            _RefreshAdapter(provider),
            store,
            exchange="SSE",
            start=date(2026, 1, 1),
            end=date(2026, 1, 5),
            updated_at=UPDATED_AT,
        )

    assert store._conn.execute("SELECT COUNT(*) FROM trade_calendar").fetchone()[0] == 0


def test_refresh_out_of_range_date_writes_zero_rows(store: DuckDBStore) -> None:
    outside = pd.DataFrame(
        [
            {
                "exchange": "SSE",
                "cal_date": "20251231",
                "is_open": "1",
                "pretrade_date": "20251230",
            }
        ]
    )
    provider = pd.concat([_provider_calendar(), outside], ignore_index=True)

    with pytest.raises(ValueError, match="outside requested range"):
        refresh_trade_calendar(
            _RefreshAdapter(provider),
            store,
            exchange="SSE",
            start=date(2026, 1, 1),
            end=date(2026, 1, 5),
            updated_at=UPDATED_AT,
        )

    assert store._conn.execute("SELECT COUNT(*) FROM trade_calendar").fetchone()[0] == 0


def test_refresh_wrong_exchange_writes_zero_rows(store: DuckDBStore) -> None:
    provider = _provider_calendar()
    provider.loc[0, "exchange"] = "SZSE"

    with pytest.raises(ValueError, match="unexpected exchanges"):
        refresh_trade_calendar(
            _RefreshAdapter(provider),
            store,
            exchange="SSE",
            start=date(2026, 1, 1),
            end=date(2026, 1, 5),
            updated_at=UPDATED_AT,
        )

    assert store._conn.execute("SELECT COUNT(*) FROM trade_calendar").fetchone()[0] == 0


def test_refresh_incomplete_provider_range_writes_zero_rows(
    store: DuckDBStore,
) -> None:
    provider = _provider_calendar().iloc[1:].reset_index(drop=True)

    with pytest.raises(TradeCalendarGapError) as gap:
        refresh_trade_calendar(
            _RefreshAdapter(provider),
            store,
            exchange="SSE",
            start=date(2026, 1, 1),
            end=date(2026, 1, 5),
            updated_at=UPDATED_AT,
        )

    assert gap.value.missing_dates == (date(2026, 1, 5),)
    assert store._conn.execute("SELECT COUNT(*) FROM trade_calendar").fetchone()[0] == 0
