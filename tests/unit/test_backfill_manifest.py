"""Strategy eligibility and minute-backfill manifest contracts."""

from __future__ import annotations

from datetime import UTC, date, datetime, time

import pandas as pd
import pytest
from pydantic import ValidationError


def _eligibility(
    ts_code: str,
    *,
    strategy_id: str = "growth_board_surge",
    eligibility_date: date = date(2026, 6, 26),
    entry_date: date = date(2026, 6, 26),
    variant: str = "gem",
):
    from rquant.backfill_manifest import EligibilityRecord

    return EligibilityRecord(
        strategy_id=strategy_id,
        strategy_version="v1",
        ts_code=ts_code,
        eligibility_date=eligibility_date,
        entry_date=entry_date,
        decision_at=datetime.combine(
            eligibility_date,
            time(9, 30),
            tzinfo=UTC,
        ),
        variant=variant,
    )


def test_strategy_backfill_specs_define_reproducible_windows() -> None:
    from rquant.backfill_manifest import STRATEGY_BACKFILL_SPECS

    assert set(STRATEGY_BACKFILL_SPECS) == {
        "auction_gap",
        "growth_board_surge",
        "n_shape",
    }
    assert STRATEGY_BACKFILL_SPECS["auction_gap"].eligibility_basis == "daily+auction"
    assert STRATEGY_BACKFILL_SPECS["growth_board_surge"].eligibility_basis == "daily"
    assert STRATEGY_BACKFILL_SPECS["n_shape"].eligibility_basis == "daily"
    for spec in STRATEGY_BACKFILL_SPECS.values():
        assert spec.minute_frequency == "1min"
        assert spec.window.baseline_trading_days == 90
        assert spec.window.entry_trading_days == 1
        assert spec.window.exit_trading_days == 10


def test_strategy_specs_and_eligibility_records_are_frozen() -> None:
    from rquant.backfill_manifest import STRATEGY_BACKFILL_SPECS

    record = _eligibility("300001.SZ")
    with pytest.raises(ValidationError):
        record.ts_code = "300002.SZ"
    with pytest.raises(ValidationError):
        STRATEGY_BACKFILL_SPECS["growth_board_surge"].minute_frequency = "5min"


def test_eligibility_id_is_stable_and_rejects_naive_decision_time() -> None:
    from rquant.backfill_manifest import EligibilityRecord

    first = _eligibility("300001.SZ")
    second = _eligibility("300001.SZ")
    assert first.eligibility_id == second.eligibility_id

    with pytest.raises(ValidationError, match="timezone-aware"):
        EligibilityRecord(
            strategy_id="growth_board_surge",
            strategy_version="v1",
            ts_code="300001.SZ",
            eligibility_date=date(2026, 6, 26),
            entry_date=date(2026, 6, 26),
            decision_at=datetime(2026, 6, 26, 9, 30),
            variant="gem",
        )


def test_manifest_id_is_independent_of_eligibility_input_order() -> None:
    from rquant.backfill_manifest import (
        STRATEGY_BACKFILL_SPECS,
        BackfillManifest,
    )

    records = [_eligibility("688001.SH", variant="star"), _eligibility("300001.SZ")]
    kwargs = {
        "spec": STRATEGY_BACKFILL_SPECS["growth_board_surge"],
        "start_date": date(2026, 6, 1),
        "end_date": date(2026, 6, 30),
        "as_of_time": datetime(2026, 7, 1, tzinfo=UTC),
        "code_commit": "a" * 40,
    }

    forward = BackfillManifest.build(eligibilities=records, **kwargs)
    reverse = BackfillManifest.build(eligibilities=list(reversed(records)), **kwargs)

    assert forward.manifest_id == reverse.manifest_id
    assert [row.ts_code for row in forward.eligibilities] == [
        "300001.SZ",
        "688001.SH",
    ]


def test_manifest_rejects_records_from_another_strategy() -> None:
    from rquant.backfill_manifest import (
        STRATEGY_BACKFILL_SPECS,
        BackfillManifest,
    )

    with pytest.raises(ValidationError, match="strategy"):
        BackfillManifest.build(
            spec=STRATEGY_BACKFILL_SPECS["growth_board_surge"],
            start_date=date(2026, 6, 1),
            end_date=date(2026, 6, 30),
            as_of_time=datetime(2026, 7, 1, tzinfo=UTC),
            code_commit="b" * 40,
            eligibilities=[
                _eligibility("300001.SZ", strategy_id="auction_gap"),
            ],
        )


def test_auction_eligibility_uses_parameter_superset(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.auction_gap_strategy as auction
    from rquant.backfill_manifest import resolve_auction_gap_eligibility
    from rquant.storage.duckdb import DuckDBStore

    captured = None

    def fake_replay(store, config):
        nonlocal captured
        del store
        captured = config
        return pd.DataFrame([
            {
                "ts_code": "300001.SZ",
                "signal_date": date(2026, 6, 26),
            }
        ])

    monkeypatch.setattr(auction, "run_auction_gap_replay", fake_replay)
    with DuckDBStore(tmp_path / "market.duckdb") as store:
        records = resolve_auction_gap_eligibility(
            store,
            start_date=date(2026, 6, 26),
            end_date=date(2026, 6, 26),
        )

    assert len(records) == 1
    assert captured is not None
    assert captured.min_auction_vol_ratio_5d == 0.0
    assert captured.max_auction_vol_ratio_5d == float("inf")
    assert captured.st_filter == "none"
    assert captured.gap_mode == "close"
    assert captured.require_next_day is False


def _seed_open_calendar(store, days: list[date]) -> None:
    from rquant.trade_calendar import TradeCalendarDay

    previous = date(2026, 6, 23)
    rows = []
    for day in days:
        rows.append(
            TradeCalendarDay(
                exchange="SSE",
                cal_date=day,
                is_open=True,
                pretrade_date=previous,
                source="test",
                updated_at=datetime(2026, 7, 1, tzinfo=UTC),
            )
        )
        previous = day
    store.upsert_trade_calendar(rows)


def test_n_shape_eligibility_rebuilds_dependencies_from_authoritative_calendar(
    tmp_path,
) -> None:
    from rquant.backfill_manifest import resolve_n_shape_eligibility
    from rquant.storage.duckdb import DuckDBStore

    days = [
        date(2026, 6, 24),
        date(2026, 6, 25),
        date(2026, 6, 26),
        date(2026, 6, 29),
    ]
    pool1_hits = {
        days[0]: ["300001.SZ"],
        days[1]: ["300002.SZ"],
        days[2]: ["300003.SZ"],
    }
    pool2_whitelists: dict[date, tuple[str, ...]] = {}

    def fake_screen(
        trade_date: str,
        _rules,
        *,
        include_columns=None,
        store=None,
        ts_code_whitelist=None,
    ) -> pd.DataFrame:
        del include_columns, store
        day = date.fromisoformat(trade_date)
        if ts_code_whitelist is None:
            codes = pool1_hits.get(day, [])
        else:
            pool2_whitelists[day] = tuple(sorted(ts_code_whitelist))
            codes = list(ts_code_whitelist[:1])
        return pd.DataFrame(
            {
                "ts_code": codes,
                "name": codes,
                "CLOSE[0]": [10.0] * len(codes),
                "PCT_CHG[0]": [1.0] * len(codes),
            }
        )

    with DuckDBStore(tmp_path / "nshape.duckdb") as store:
        _seed_open_calendar(store, days)
        records = resolve_n_shape_eligibility(
            store,
            start_date=days[2],
            end_date=days[2],
            screen_runner=fake_screen,
        )

    assert pool2_whitelists[days[2]] == ("300001.SZ", "300002.SZ")
    assert {(row.ts_code, row.variant, row.entry_date) for row in records} == {
        ("300001.SZ", "pool2", days[3]),
        ("300003.SZ", "pool1", days[3]),
    }


def test_growth_eligibility_uses_shared_daily_resolver_before_minute_reads(
    monkeypatch,
    tmp_path,
) -> None:
    import rquant.growth_board_surge_strategy as growth
    from rquant.backfill_manifest import resolve_growth_board_eligibility
    from rquant.storage.duckdb import DuckDBStore

    days = [date(2026, 6, 24), date(2026, 6, 25)]
    calls: list[tuple[date, date]] = []

    def fake_candidates(store, trading_date, previous_date, min_signal_time):
        del store, min_signal_time
        calls.append((trading_date, previous_date))
        return [
            growth.GrowthBoardCandidate(
                ts_code="300001.SZ",
                name="样本",
                trade_date=trading_date,
                previous_date=previous_date,
                board_type="gem",
                pre_close=10.0,
                limit_up_price=12.0,
            )
        ]

    monkeypatch.setattr(growth, "resolve_growth_board_candidates", fake_candidates)

    with DuckDBStore(tmp_path / "growth.duckdb") as store:
        _seed_open_calendar(store, days)
        monkeypatch.setattr(
            store,
            "query_minute_bars",
            lambda *args, **kwargs: pytest.fail("eligibility must not read minute_bar"),
        )
        records = resolve_growth_board_eligibility(
            store,
            start_date=days[1],
            end_date=days[1],
        )

    assert calls == [(days[1], days[0])]
    assert [(row.ts_code, row.variant, row.entry_date) for row in records] == [
        ("300001.SZ", "gem", days[1])
    ]


def test_auction_eligibility_is_daily_plus_auction_and_never_checks_minutes(
    monkeypatch,
    tmp_path,
) -> None:
    import rquant.auction_gap_strategy as auction
    from rquant.backfill_manifest import resolve_auction_gap_eligibility
    from rquant.storage.duckdb import DuckDBStore

    signal_date = date(2026, 6, 26)
    observed_require_next_day: list[bool] = []

    def fake_replay(store, config):
        del store
        observed_require_next_day.append(config.require_next_day)
        return pd.DataFrame(
            [{"signal_date": signal_date, "ts_code": "605366.SH"}]
        )

    monkeypatch.setattr(
        auction,
        "run_auction_gap_replay",
        fake_replay,
    )

    with DuckDBStore(tmp_path / "auction.duckdb") as store:
        monkeypatch.setattr(
            store,
            "query_minute_bars",
            lambda *args, **kwargs: pytest.fail("eligibility must not read minute_bar"),
        )
        records = resolve_auction_gap_eligibility(
            store,
            start_date=signal_date,
            end_date=signal_date,
        )

    assert [(row.ts_code, row.variant, row.entry_date) for row in records] == [
        ("605366.SH", "auction_gap", signal_date)
    ]
    assert observed_require_next_day == [False]
