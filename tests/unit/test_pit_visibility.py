"""Point-in-time visibility guard tests."""

from __future__ import annotations

from datetime import date, datetime, timedelta, tzinfo
from zoneinfo import ZoneInfo

import duckdb
import pytest
from pydantic import ValidationError

from rquant.data_contracts import VisibilityRule
from rquant.pit_visibility import (
    VisibilityDecision,
    VisibilityInput,
    VisibilityQueryScope,
    _normalize_timestamp_type,
    available_at_for_input,
    derive_available_at,
    evaluate_visibility,
    is_input_visible,
    query_visible_rows,
)
from rquant.storage.duckdb import DuckDBStore

SHANGHAI = ZoneInfo("Asia/Shanghai")
UTC = ZoneInfo("UTC")


class _BrokenTimezone(tzinfo):
    def utcoffset(self, dt: datetime | None) -> timedelta | None:
        return None

    def dst(self, dt: datetime | None) -> timedelta | None:
        return None


@pytest.fixture()
def store(tmp_path) -> DuckDBStore:
    value = DuckDBStore(tmp_path / "pit.duckdb")
    yield value
    value.close()


def test_panel_close_available_at_is_next_civil_day_and_same_day_is_hidden() -> None:
    item = VisibilityInput(dataset_id="moneyflow", event_date=date(2026, 7, 13))

    assert available_at_for_input(item) == datetime(2026, 7, 14, tzinfo=SHANGHAI)
    assert not is_input_visible(
        item,
        as_of_time=datetime(2026, 7, 13, 20, 0, tzinfo=SHANGHAI),
    )
    assert is_input_visible(
        item,
        as_of_time=datetime(2026, 7, 14, 0, 0, tzinfo=SHANGHAI),
    )


def test_visibility_evaluation_returns_a_typed_decision() -> None:
    decision = evaluate_visibility(
        VisibilityInput(dataset_id="moneyflow", event_date=date(2026, 7, 12)),
        as_of_time=datetime(2026, 7, 13, 9, 30, tzinfo=SHANGHAI),
    )

    assert isinstance(decision, VisibilityDecision)
    assert decision.visibility is VisibilityRule.PANEL_CLOSE_NEXT_SESSION
    assert decision.visible is True
    assert decision.reason == "visible"


@pytest.mark.parametrize(
    ("source", "cutoff"),
    [
        ("tushare", datetime(2026, 7, 13, 9, 26, tzinfo=SHANGHAI)),
        (
            "minute_0930_fallback",
            datetime(2026, 7, 13, 9, 31, tzinfo=SHANGHAI),
        ),
    ],
)
def test_auction_uses_each_source_contract_cutoff(
    source: str,
    cutoff: datetime,
) -> None:
    item = VisibilityInput(
        dataset_id="auction_bar",
        event_date=date(2026, 7, 13),
        source=source,
    )

    assert available_at_for_input(item) == cutoff
    assert not is_input_visible(item, as_of_time=cutoff - timedelta(microseconds=1))
    assert is_input_visible(item, as_of_time=cutoff)
    assert is_input_visible(
        item.model_copy(update={"event_date": date(2026, 7, 12)}),
        as_of_time=datetime(2026, 7, 13, 9, 0, tzinfo=SHANGHAI),
    )
    assert not is_input_visible(
        item.model_copy(update={"event_date": date(2026, 7, 14)}),
        as_of_time=datetime(2026, 7, 13, 12, 0, tzinfo=SHANGHAI),
    )


def test_unknown_auction_source_and_unknown_visibility_fail_closed() -> None:
    unknown_source = VisibilityInput(
        dataset_id="auction_bar",
        event_date=date(2026, 7, 12),
        source="not_registered",
    )
    unknown_dataset_visibility = VisibilityInput(dataset_id="ths_index")

    assert available_at_for_input(unknown_source) is None
    assert not is_input_visible(
        unknown_source,
        as_of_time=datetime(2026, 7, 13, 10, 0, tzinfo=SHANGHAI),
    )
    assert not is_input_visible(
        unknown_dataset_visibility,
        as_of_time=datetime(2026, 7, 13, 10, 0, tzinfo=SHANGHAI),
    )


def test_minute_available_at_localizes_naive_exchange_time_and_hides_future() -> None:
    current = VisibilityInput(
        dataset_id="minute_bar",
        event_time=datetime(2026, 7, 13, 9, 31),
    )
    future = current.model_copy(update={"event_time": datetime(2026, 7, 13, 9, 32)})
    as_of = datetime(2026, 7, 13, 1, 31, tzinfo=UTC)

    assert available_at_for_input(current) == datetime(2026, 7, 13, 9, 31, tzinfo=SHANGHAI)
    assert is_input_visible(current, as_of_time=as_of)
    assert not is_input_visible(future, as_of_time=as_of)


@pytest.mark.parametrize(
    "as_of",
    [
        datetime(2026, 7, 13, 9, 31),
        datetime(2026, 7, 13, 9, 31, tzinfo=_BrokenTimezone()),
    ],
)
def test_visibility_requires_a_sensible_aware_as_of(as_of: datetime) -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        is_input_visible(
            VisibilityInput(
                dataset_id="minute_bar",
                event_time=datetime(2026, 7, 13, 9, 31),
            ),
            as_of_time=as_of,
        )


def test_missing_rule_input_is_an_explicit_error() -> None:
    with pytest.raises(ValueError, match="event_date"):
        available_at_for_input(VisibilityInput(dataset_id="moneyflow"))
    with pytest.raises(ValueError, match="event_time"):
        available_at_for_input(VisibilityInput(dataset_id="minute_bar"))


def test_derived_available_at_is_latest_input_in_exchange_timezone() -> None:
    result = derive_available_at(
        [
            datetime(2026, 7, 13, 9, 31, tzinfo=SHANGHAI),
            datetime(2026, 7, 13, 1, 32, tzinfo=UTC),
        ]
    )

    assert result == datetime(2026, 7, 13, 9, 32, tzinfo=SHANGHAI)


@pytest.mark.parametrize(
    "inputs",
    [
        [],
        [None],
        [datetime(2026, 7, 13, 9, 31)],
        [datetime(2026, 7, 13, 9, 31, tzinfo=_BrokenTimezone())],
    ],
)
def test_derived_available_at_rejects_empty_missing_or_naive_inputs(
    inputs: list[datetime | None],
) -> None:
    with pytest.raises(ValueError, match="available_at"):
        derive_available_at(inputs)


def test_query_visible_rows_filters_panel_auction_and_minute_rows(
    store: DuckDBStore,
) -> None:
    store._conn.executemany(
        """
        INSERT INTO moneyflow_daily (ts_code, trade_date, source)
        VALUES (?, ?, 'tushare')
        """,
        [("OLD.SZ", date(2026, 7, 12)), ("TODAY.SZ", date(2026, 7, 13))],
    )
    store._conn.executemany(
        """
        INSERT INTO auction_bar
            (ts_code, trade_date, auction_type, price, source)
        VALUES (?, ?, 'open', 10.0, ?)
        """,
        [
            ("PRIMARY.SZ", date(2026, 7, 13), "tushare"),
            ("FALLBACK.SZ", date(2026, 7, 13), "minute_0930_fallback"),
            ("OLD.SZ", date(2026, 7, 12), "tushare"),
            ("UNKNOWN.SZ", date(2026, 7, 12), "unknown"),
            ("FUTURE.SZ", date(2026, 7, 14), "tushare"),
        ],
    )
    store._conn.executemany(
        """
        INSERT INTO minute_bar
            (ts_code, trade_time, freq, close, source)
        VALUES (?, ?, '1min', 10.0, 'tushare')
        """,
        [
            ("NOW.SZ", datetime(2026, 7, 13, 9, 31)),
            ("FUTURE.SZ", datetime(2026, 7, 13, 9, 32)),
        ],
    )

    moneyflow = query_visible_rows(
        store,
        "moneyflow",
        datetime(2026, 7, 13, 20, 0, tzinfo=SHANGHAI),
        scope=VisibilityQueryScope(
            start_date=date(2026, 7, 12),
            end_date=date(2026, 7, 13),
        ),
    )
    auction = query_visible_rows(
        store._conn,
        "auction_bar",
        datetime(2026, 7, 13, 9, 28, tzinfo=SHANGHAI),
    )
    minute = query_visible_rows(
        store,
        "minute_bar",
        datetime(2026, 7, 13, 9, 31, tzinfo=SHANGHAI),
        scope=VisibilityQueryScope(
            start_time=datetime(2026, 7, 13, 9, 30, tzinfo=SHANGHAI),
            end_time=datetime(2026, 7, 13, 9, 31, tzinfo=SHANGHAI),
        ),
    )

    assert set(moneyflow["ts_code"]) == {"OLD.SZ"}
    assert set(auction["ts_code"]) == {"PRIMARY.SZ", "OLD.SZ"}
    assert set(minute["ts_code"]) == {"NOW.SZ"}


def test_query_minute_as_of_preserves_timezone_aware_physical_columns(
    store: DuckDBStore,
) -> None:
    store._conn.execute("SET TimeZone = 'UTC'")
    store._conn.executemany(
        """
        INSERT INTO stock_status_daily
            (ts_code, trade_date, name_source, available_at, ingested_at)
        VALUES (?, DATE '2026-07-13', 'tushare', ?, ?)
        """,
        [
            (
                "NOW.SZ",
                datetime(2026, 7, 13, 9, 31, tzinfo=SHANGHAI),
                datetime(2026, 7, 13, 9, 31, tzinfo=SHANGHAI),
            ),
            (
                "FUTURE.SZ",
                datetime(2026, 7, 13, 9, 32, tzinfo=SHANGHAI),
                datetime(2026, 7, 13, 9, 32, tzinfo=SHANGHAI),
            ),
        ],
    )

    visible = query_visible_rows(
        store,
        "stock_status_daily",
        datetime(2026, 7, 13, 9, 31, tzinfo=SHANGHAI),
        scope=VisibilityQueryScope(
            start_time=datetime(2026, 7, 13, 9, 30, tzinfo=SHANGHAI),
            end_time=datetime(2026, 7, 13, 9, 31, tzinfo=SHANGHAI),
        ),
    )

    assert set(visible["ts_code"]) == {"NOW.SZ"}


def test_query_unknown_visibility_returns_empty_and_unknown_id_is_rejected(
    store: DuckDBStore,
) -> None:
    hidden = query_visible_rows(
        store,
        "ths_index",
        datetime(2026, 7, 13, 10, 0, tzinfo=SHANGHAI),
    )

    assert hidden.empty
    with pytest.raises(ValueError, match="unknown dataset_id"):
        query_visible_rows(
            store,
            "moneyflow; DROP TABLE minute_bar",
            datetime(2026, 7, 13, 10, 0, tzinfo=SHANGHAI),
        )


def test_query_requires_aware_as_of_before_executing_sql() -> None:
    conn = duckdb.connect(":memory:")
    try:
        with pytest.raises(ValueError, match="timezone-aware"):
            query_visible_rows(
                conn,
                "minute_bar",
                datetime(2026, 7, 13, 9, 31),
            )
    finally:
        conn.close()


def test_minute_query_requires_a_finite_aware_range_not_beyond_as_of(
    store: DuckDBStore,
) -> None:
    as_of = datetime(2026, 7, 13, 9, 31, tzinfo=SHANGHAI)

    with pytest.raises(ValueError, match="start_time.*end_time"):
        query_visible_rows(store, "minute_bar", as_of)
    with pytest.raises(ValidationError, match="timezone-aware"):
        VisibilityQueryScope(
            start_time=datetime(2026, 7, 13, 9, 30),
            end_time=datetime(2026, 7, 13, 9, 31, tzinfo=SHANGHAI),
        )
    with pytest.raises(ValidationError, match="start_time.*end_time"):
        VisibilityQueryScope(
            start_time=datetime(2026, 7, 13, 9, 32, tzinfo=SHANGHAI),
            end_time=datetime(2026, 7, 13, 9, 31, tzinfo=SHANGHAI),
        )
    with pytest.raises(ValueError, match="end_time.*as_of"):
        query_visible_rows(
            store,
            "minute_bar",
            as_of,
            scope=VisibilityQueryScope(
                start_time=datetime(2026, 7, 13, 9, 31, tzinfo=SHANGHAI),
                end_time=datetime(2026, 7, 13, 9, 32, tzinfo=SHANGHAI),
            ),
        )


def test_minute_query_deduplicates_by_contract_source_priority_and_scope(
    store: DuckDBStore,
) -> None:
    store._conn.executemany(
        """
        INSERT INTO minute_bar
            (ts_code, trade_time, freq, close, source)
        VALUES (?, ?, '1min', ?, ?)
        """,
        [
            ("BOTH.SZ", datetime(2026, 7, 13, 9, 30), 10.0, "tushare"),
            ("BOTH.SZ", datetime(2026, 7, 13, 9, 30), 11.0, "tushare_rt"),
            ("BOTH.SZ", datetime(2026, 7, 13, 9, 30), 12.0, "unknown"),
            ("RT.SZ", datetime(2026, 7, 13, 9, 31), 13.0, "tushare_rt"),
            ("OUTSIDE.SZ", datetime(2026, 7, 13, 9, 32), 14.0, "tushare"),
        ],
    )
    scope = VisibilityQueryScope(
        ts_codes=("BOTH.SZ", "RT.SZ", "OUTSIDE.SZ"),
        start_time=datetime(2026, 7, 13, 9, 30, tzinfo=SHANGHAI),
        end_time=datetime(2026, 7, 13, 9, 31, tzinfo=SHANGHAI),
        columns=("ts_code", "trade_time", "close", "source"),
    )

    visible = query_visible_rows(
        store,
        "minute_bar",
        datetime(2026, 7, 13, 9, 31, tzinfo=SHANGHAI),
        scope=scope,
    )

    assert list(visible.columns) == ["ts_code", "trade_time", "close", "source"]
    assert visible.set_index("ts_code")["source"].to_dict() == {
        "BOTH.SZ": "tushare",
        "RT.SZ": "tushare_rt",
    }
    assert visible.set_index("ts_code")["close"].to_dict() == {
        "BOTH.SZ": 10.0,
        "RT.SZ": 13.0,
    }


def test_explicit_source_scope_is_registered_and_overrides_default_priority(
    store: DuckDBStore,
) -> None:
    store._conn.executemany(
        """
        INSERT INTO minute_bar
            (ts_code, trade_time, freq, close, source)
        VALUES ('BOTH.SZ', TIMESTAMP '2026-07-13 09:30:00', '1min', ?, ?)
        """,
        [(10.0, "tushare"), (11.0, "tushare_rt")],
    )
    base = {
        "start_time": datetime(2026, 7, 13, 9, 30, tzinfo=SHANGHAI),
        "end_time": datetime(2026, 7, 13, 9, 30, tzinfo=SHANGHAI),
    }

    rt_only = query_visible_rows(
        store,
        "minute_bar",
        datetime(2026, 7, 13, 9, 30, tzinfo=SHANGHAI),
        scope=VisibilityQueryScope(**base, sources=("tushare_rt",)),
    )

    assert rt_only.iloc[0]["source"] == "tushare_rt"
    assert rt_only.iloc[0]["close"] == 11.0
    with pytest.raises(ValueError, match="unregistered source"):
        query_visible_rows(
            store,
            "minute_bar",
            datetime(2026, 7, 13, 9, 30, tzinfo=SHANGHAI),
            scope=VisibilityQueryScope(**base, sources=("unknown",)),
        )


def test_panel_query_excludes_unknown_sources_and_applies_narrow_scope(
    store: DuckDBStore,
) -> None:
    store._conn.executemany(
        """
        INSERT INTO moneyflow_daily (ts_code, trade_date, source, large_net_vol)
        VALUES (?, ?, ?, ?)
        """,
        [
            ("KEEP.SZ", date(2026, 7, 12), "tushare", 1.0),
            ("KEEP.SZ", date(2026, 7, 12), "unknown", 2.0),
            ("OTHER.SZ", date(2026, 7, 12), "tushare", 3.0),
            ("OLD.SZ", date(2026, 7, 11), "tushare", 4.0),
        ],
    )

    visible = query_visible_rows(
        store,
        "moneyflow",
        datetime(2026, 7, 13, 10, 0, tzinfo=SHANGHAI),
        scope=VisibilityQueryScope(
            ts_codes=("KEEP.SZ",),
            start_date=date(2026, 7, 12),
            end_date=date(2026, 7, 12),
            columns=("ts_code", "trade_date", "source", "large_net_vol"),
        ),
    )

    assert visible.drop(columns="trade_date").to_dict("records") == [
        {"ts_code": "KEEP.SZ", "source": "tushare", "large_net_vol": 1.0}
    ]
    assert visible["trade_date"].dt.date.tolist() == [date(2026, 7, 12)]


def test_auction_filters_cutoff_before_deduplicating_sources(
    store: DuckDBStore,
) -> None:
    store._conn.executemany(
        """
        INSERT INTO auction_bar
            (ts_code, trade_date, auction_type, price, source)
        VALUES ('BOTH.SZ', DATE '2026-07-13', 'open', ?, ?)
        """,
        [(10.0, "tushare"), (11.0, "minute_0930_fallback")],
    )
    scope = VisibilityQueryScope(
        start_date=date(2026, 7, 13),
        end_date=date(2026, 7, 13),
    )

    before_fallback = query_visible_rows(
        store,
        "auction_bar",
        datetime(2026, 7, 13, 9, 28, tzinfo=SHANGHAI),
        scope=scope,
    )
    after_fallback = query_visible_rows(
        store,
        "auction_bar",
        datetime(2026, 7, 13, 9, 31, tzinfo=SHANGHAI),
        scope=scope,
    )

    assert before_fallback[["source", "price"]].to_dict("records") == [
        {"source": "tushare", "price": 10.0}
    ]
    assert after_fallback[["source", "price"]].to_dict("records") == [
        {"source": "tushare", "price": 10.0}
    ]


def test_scope_rejects_unknown_or_sql_like_columns(store: DuckDBStore) -> None:
    as_of = datetime(2026, 7, 13, 10, 0, tzinfo=SHANGHAI)

    for column in ("missing_column", "ts_code; DROP TABLE minute_bar"):
        with pytest.raises(ValueError, match="unknown column"):
            query_visible_rows(
                store,
                "moneyflow",
                as_of,
                scope=VisibilityQueryScope(columns=(column,)),
            )


def test_event_time_query_rejects_unexpected_physical_type() -> None:
    conn = duckdb.connect(":memory:")
    try:
        conn.execute(
            """
            CREATE TABLE minute_bar (
                ts_code VARCHAR,
                trade_time VARCHAR,
                freq VARCHAR,
                source VARCHAR
            )
            """
        )
        with pytest.raises(ValueError, match="unsupported event-time type"):
            query_visible_rows(
                conn,
                "minute_bar",
                datetime(2026, 7, 13, 9, 31, tzinfo=SHANGHAI),
                scope=VisibilityQueryScope(
                    start_time=datetime(2026, 7, 13, 9, 30, tzinfo=SHANGHAI),
                    end_time=datetime(2026, 7, 13, 9, 31, tzinfo=SHANGHAI),
                ),
            )
    finally:
        conn.close()


@pytest.mark.parametrize(
    ("raw_type", "normalized"),
    [
        (" timestamp(6) ", "TIMESTAMP"),
        ("TIMESTAMP ( 3 ) WITH   TIME ZONE", "TIMESTAMP WITH TIME ZONE"),
    ],
)
def test_event_time_type_normalization_accepts_precision_text(
    raw_type: str,
    normalized: str,
) -> None:
    assert _normalize_timestamp_type(raw_type) == normalized
