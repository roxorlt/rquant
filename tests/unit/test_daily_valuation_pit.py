"""Daily valuation observations stay separate from legacy daily_basic rows."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd
import pytest

from rquant.adapter.tushare import TushareAdapter
from rquant.daily_valuation_pit import (
    DailyValuationBatch,
    DailyValuationPITQuery,
    DailyValuationPITSelection,
    DailyValuationRow,
    _record_daily_valuation_batch_in_transaction,
    query_daily_valuation_pit,
)
from rquant.data_contracts import CONTRACTS_BY_ID, VisibilityRule, is_visible
from rquant.pit_visibility import VisibilityInput, evaluate_visibility, query_visible_rows
from rquant.storage.migrations import initialize_schema

SHANGHAI = ZoneInfo("Asia/Shanghai")
SYMBOL = "600000.SH"
FRIDAY = date(2026, 9, 25)
MONDAY = date(2026, 9, 28)


def _db() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    initialize_schema(conn)
    for index in range(4):
        day = FRIDAY + timedelta(days=index)
        conn.execute(
            "INSERT INTO trade_calendar "
            "(exchange, cal_date, is_open, pretrade_date, source, updated_at) "
            "VALUES ('SSE', ?, ?, ?, 'tushare', ?)",
            [
                day,
                day in {FRIDAY, MONDAY},
                date(2026, 9, 24) if day == FRIDAY else FRIDAY,
                datetime(2026, 9, 25, tzinfo=UTC),
            ],
        )
    return conn


def _batch(
    revision: int,
    at: datetime,
    *,
    trade_date: date = FRIDAY,
    pe_ttm: float | None = 10.0,
    pb: float | None = 2.0,
    dv_ttm: float | None = 1.5,
    rows: tuple[DailyValuationRow, ...] | None = None,
    valuation_observed: bool = True,
) -> DailyValuationBatch:
    return DailyValuationBatch(
        candidate_generation_id=f"{revision:064x}",
        source_generation_id="a" * 64,
        source_sequence=revision,
        source_batch_id=f"{revision + 100:064x}",
        revision=revision,
        trade_date=trade_date,
        observed_at=at,
        valuation_observed=valuation_observed,
        rows=(
            (
                DailyValuationRow(
                    ts_code=SYMBOL, trade_date=trade_date, pe_ttm=pe_ttm, pb=pb, dv_ttm=dv_ttm
                ),
            )
            if rows is None and valuation_observed
            else (() if rows is None else rows)
        ),
    )


def _select(conn: duckdb.DuckDBPyConnection, day: date = MONDAY) -> DailyValuationPITSelection:
    return query_daily_valuation_pit(
        conn, DailyValuationPITQuery(ts_code=SYMBOL, decision_date=day)
    )


def test_legacy_daily_basic_row_never_becomes_pit_evidence() -> None:
    with _db() as conn:
        conn.execute(
            "INSERT INTO daily_basic (ts_code, trade_date, turnover_rate) VALUES (?, ?, 1.2)",
            [SYMBOL, FRIDAY],
        )
        selected = _select(conn)
        assert selected.status == "unknown"
        assert selected.reason == "no_observed_batch"


def test_observations_preserve_a_b_a_and_first_seen_content() -> None:
    with _db() as conn:
        a = _batch(1, datetime(2026, 9, 25, 8, tzinfo=UTC))
        b = _batch(2, datetime(2026, 9, 25, 9, tzinfo=UTC), pe_ttm=11.0)
        again = _batch(3, datetime(2026, 9, 28, 7, tzinfo=UTC))
        for batch in (a, b, again):
            _record_daily_valuation_batch_in_transaction(conn, batch)
        assert _record_daily_valuation_batch_in_transaction(conn, again) == 0
        selected = _select(conn)
        assert selected.status == "selected"
        assert selected.pe_ttm == 10.0
        assert selected.pb == 2.0
        assert selected.dv_ttm == 1.5
        assert selected.observed_at == again.observed_at
        assert selected.first_observed_at == a.observed_at
        assert selected.source_batch_id == again.source_batch_id
        count = conn.execute("SELECT count(*) FROM daily_basic_valuation_observation").fetchone()
        assert count == (3,)


def test_late_revision_does_not_backfill_prior_decision() -> None:
    with _db() as conn:
        early = _batch(1, datetime(2026, 9, 25, 8, tzinfo=UTC))
        late = _batch(2, datetime(2026, 9, 28, 10, tzinfo=UTC), pe_ttm=99.0)
        _record_daily_valuation_batch_in_transaction(conn, early)
        _record_daily_valuation_batch_in_transaction(conn, late)
        selected = _select(conn)
        assert selected.status == "selected"
        assert selected.pe_ttm == 10.0


def test_null_pe_stays_unknown_with_other_values_from_same_row() -> None:
    with _db() as conn:
        _record_daily_valuation_batch_in_transaction(
            conn, _batch(1, datetime(2026, 9, 25, 8, tzinfo=UTC), pe_ttm=None)
        )
        selected = _select(conn)
        assert selected.status == "selected"
        assert selected.pe_ttm is None
        assert selected.pb == 2.0
        assert selected.dv_ttm == 1.5


def test_latest_batch_missing_symbol_blocks_older_row_fallback() -> None:
    with _db() as conn:
        _record_daily_valuation_batch_in_transaction(
            conn, _batch(1, datetime(2026, 9, 25, 8, tzinfo=UTC))
        )
        _record_daily_valuation_batch_in_transaction(
            conn,
            _batch(
                2,
                datetime(2026, 9, 25, 9, tzinfo=UTC),
                rows=(
                    DailyValuationRow(
                        ts_code="000001.SZ", trade_date=FRIDAY, pe_ttm=8.0, pb=1.0, dv_ttm=1.0
                    ),
                ),
            ),
        )
        selected = _select(conn)
        assert selected.status == "unknown"
        assert selected.reason == "missing_symbol_row"


def test_later_legacy_batch_blocks_older_observed_valuations() -> None:
    with _db() as conn:
        _record_daily_valuation_batch_in_transaction(
            conn, _batch(1, datetime(2026, 9, 25, 8, tzinfo=UTC))
        )
        _record_daily_valuation_batch_in_transaction(
            conn,
            _batch(
                2,
                datetime(2026, 9, 25, 9, tzinfo=UTC),
                valuation_observed=False,
            ),
        )
        assert _select(conn).reason == "valuation_not_observed"


def test_missing_expected_trading_day_never_uses_older_existing_row() -> None:
    with _db() as conn:
        _record_daily_valuation_batch_in_transaction(
            conn, _batch(1, datetime(2026, 9, 25, 8, tzinfo=UTC), trade_date=date(2026, 9, 24))
        )
        assert _select(conn).reason == "no_observed_batch"


def test_incomplete_calendar_or_closed_decision_date_fails_closed() -> None:
    with _db() as conn:
        _record_daily_valuation_batch_in_transaction(
            conn, _batch(1, datetime(2026, 9, 25, 8, tzinfo=UTC))
        )
        conn.execute("DELETE FROM trade_calendar WHERE cal_date = ?", [date(2026, 9, 27)])
        assert _select(conn).reason == "incomplete_calendar"
        assert _select(conn, date(2026, 9, 26)).reason == "decision_not_open"


@pytest.mark.parametrize("day", [FRIDAY, date(2026, 9, 26), MONDAY])
def test_untrusted_calendar_source_cannot_select_valuation(day: date) -> None:
    with _db() as conn:
        _record_daily_valuation_batch_in_transaction(
            conn, _batch(1, datetime(2026, 9, 25, 8, tzinfo=UTC))
        )
        assert _select(conn).status == "selected"
        conn.execute(
            "UPDATE trade_calendar SET source = 'test' WHERE exchange = 'SSE' AND cal_date = ?",
            [day],
        )
        assert _select(conn).reason == "incomplete_calendar"


@pytest.mark.parametrize(
    ("day", "pretrade_date"),
    [
        (FRIDAY, None),
        (date(2026, 9, 26), None),
        (MONDAY, date(2026, 9, 24)),
    ],
)
def test_broken_calendar_pretrade_chain_cannot_select_valuation(
    day: date, pretrade_date: date | None
) -> None:
    with _db() as conn:
        _record_daily_valuation_batch_in_transaction(
            conn, _batch(1, datetime(2026, 9, 25, 8, tzinfo=UTC))
        )
        assert _select(conn).status == "selected"
        conn.execute(
            "UPDATE trade_calendar SET pretrade_date = ? "
            "WHERE exchange = 'SSE' AND cal_date = ?",
            [pretrade_date, day],
        )
        assert _select(conn).reason == "incomplete_calendar"


def test_long_holiday_uses_exact_prior_open_date() -> None:
    with _db() as conn:
        conn.execute("UPDATE trade_calendar SET is_open = FALSE WHERE cal_date = ?", [MONDAY])
        for index in range(4, 15):
            day = FRIDAY + timedelta(days=index)
            conn.execute(
                "INSERT INTO trade_calendar "
                "(exchange, cal_date, is_open, pretrade_date, source, updated_at) "
                "VALUES ('SSE', ?, ?, ?, 'tushare', ?)",
                [day, day == date(2026, 10, 9), FRIDAY, datetime(2026, 9, 25, tzinfo=UTC)],
            )
        _record_daily_valuation_batch_in_transaction(
            conn, _batch(1, datetime(2026, 9, 25, 8, tzinfo=UTC))
        )
        selected = _select(conn, date(2026, 10, 9))
        assert selected.status == "selected"
        assert selected.trade_date == FRIDAY


def test_transaction_rollback_removes_batch_and_observations() -> None:
    with _db() as conn:
        conn.execute("BEGIN")
        _record_daily_valuation_batch_in_transaction(
            conn, _batch(1, datetime(2026, 9, 25, 8, tzinfo=UTC))
        )
        conn.execute("ROLLBACK")
        assert conn.execute("SELECT count(*) FROM daily_basic_valuation_batch").fetchone() == (0,)
        count = conn.execute("SELECT count(*) FROM daily_basic_valuation_observation").fetchone()
        assert count == (0,)


def test_mid_batch_failure_can_roll_back_all_observations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.daily_valuation_pit as module

    original = module._row_sha256
    calls = 0

    def crash_on_second(row: DailyValuationRow) -> str:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("injected write interruption")
        return original(row)

    monkeypatch.setattr(module, "_row_sha256", crash_on_second)
    rows = (
        DailyValuationRow(ts_code=SYMBOL, trade_date=FRIDAY, pe_ttm=10.0, pb=2.0, dv_ttm=1.5),
        DailyValuationRow(ts_code="000001.SZ", trade_date=FRIDAY, pe_ttm=8.0, pb=1.0, dv_ttm=1.0),
    )
    with _db() as conn:
        conn.execute("BEGIN")
        with pytest.raises(RuntimeError, match="injected write interruption"):
            _record_daily_valuation_batch_in_transaction(
                conn, _batch(1, datetime(2026, 9, 25, 8, tzinfo=UTC), rows=rows)
            )
        conn.execute("ROLLBACK")
        assert conn.execute("SELECT count(*) FROM daily_basic_valuation_batch").fetchone() == (0,)
        count = conn.execute("SELECT count(*) FROM daily_basic_valuation_observation").fetchone()
        assert count == (0,)


def test_revision_time_regression_and_replay_conflict_fail_closed() -> None:
    with _db() as conn:
        initial = _batch(1, datetime(2026, 9, 25, 8, tzinfo=UTC))
        _record_daily_valuation_batch_in_transaction(conn, initial)
        with pytest.raises(ValueError, match="regressed"):
            _record_daily_valuation_batch_in_transaction(
                conn, _batch(2, datetime(2026, 9, 25, 7, tzinfo=UTC), pe_ttm=11.0)
            )
        with pytest.raises(ValueError, match="replay conflicts"):
            _record_daily_valuation_batch_in_transaction(
                conn, _batch(1, datetime(2026, 9, 25, 8, tzinfo=UTC), pe_ttm=11.0)
            )


def test_observation_before_market_close_cannot_claim_daily_valuation() -> None:
    with pytest.raises(ValueError, match="market close"):
        _batch(1, datetime(2026, 9, 25, 5, tzinfo=UTC))


def test_tampered_row_hash_is_not_selected() -> None:
    with _db() as conn:
        _record_daily_valuation_batch_in_transaction(
            conn, _batch(1, datetime(2026, 9, 25, 8, tzinfo=UTC))
        )
        conn.execute("UPDATE daily_basic_valuation_observation SET pb = 999")
        assert _select(conn).reason == "invalid_observation_evidence"


def test_nonfinite_stored_value_is_unknown() -> None:
    with _db() as conn:
        _record_daily_valuation_batch_in_transaction(
            conn, _batch(1, datetime(2026, 9, 25, 8, tzinfo=UTC))
        )
        conn.execute("UPDATE daily_basic_valuation_observation SET pb = 'NaN'::DOUBLE")
        assert _select(conn).reason == "invalid_observation_evidence"


def test_dedicated_contract_rejects_generic_visibility() -> None:
    with _db() as conn:
        contract = CONTRACTS_BY_ID["daily_basic_valuation_observation"]
        assert contract.visibility is VisibilityRule.DAILY_VALUATION_PIT
        with pytest.raises(ValueError, match="daily valuation PIT"):
            is_visible(contract, as_of_time=datetime(2026, 9, 28, 17, tzinfo=SHANGHAI))
        with pytest.raises(ValueError, match="daily valuation PIT"):
            evaluate_visibility(
                VisibilityInput(dataset_id="daily_basic_valuation_observation"),
                as_of_time=datetime(2026, 9, 28, 17, tzinfo=SHANGHAI),
            )
        with pytest.raises(ValueError, match="daily valuation PIT"):
            query_visible_rows(
                conn,
                "daily_basic_valuation_observation",
                datetime(2026, 9, 28, 17, tzinfo=SHANGHAI),
            )


def test_official_daily_basic_request_includes_typed_valuation_columns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str]] = []

    class FakePro:
        def daily_basic(self, *, trade_date: str, fields: str) -> pd.DataFrame:
            calls.append((trade_date, fields))
            return pd.DataFrame(
                [
                    {
                        "ts_code": SYMBOL,
                        "trade_date": "20260925",
                        "turnover_rate": 1.0,
                        "volume_ratio": 1.0,
                        "total_mv": 10.0,
                        "circ_mv": 8.0,
                        "pe_ttm": None,
                        "pb": 1.25,
                        "dv_ttm": 2.5,
                    }
                ]
            )

    monkeypatch.setattr("rquant.adapter.tushare.ts.pro_api", lambda _token: FakePro())
    frame = TushareAdapter(token="offline-fake", backup_token="").daily_basic_by_date(FRIDAY)
    assert calls == [
        (
            FRIDAY.strftime("%Y%m%d"),
            "ts_code,trade_date,turnover_rate,volume_ratio,total_mv,circ_mv,pe_ttm,pb,dv_ttm",
        )
    ]
    assert frame.loc[0, "pe_ttm"] is None
    assert frame.loc[0, "pb"] == 1.25
