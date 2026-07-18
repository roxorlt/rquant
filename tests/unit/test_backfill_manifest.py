"""Strategy eligibility and minute-backfill manifest contracts."""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta

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
    assert (
        STRATEGY_BACKFILL_SPECS["n_shape"].eligibility_entry_delay_trading_days
        == 1
    )
    assert (
        STRATEGY_BACKFILL_SPECS[
            "growth_board_surge"
        ].eligibility_entry_delay_trading_days
        == 0
    )
    assert (
        STRATEGY_BACKFILL_SPECS["auction_gap"].eligibility_entry_delay_trading_days
        == 0
    )
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


def test_legacy_manifest_without_entry_delay_keeps_its_identity() -> None:
    from rquant.backfill_manifest import (
        BackfillManifest,
        StrategyWindowRequirement,
        _canonical_hash,
    )

    start_date = date(2026, 6, 1)
    end_date = date(2026, 6, 30)
    as_of_time = datetime(2026, 7, 1, tzinfo=UTC)
    legacy_spec = {
        "strategy_id": "n_shape",
        "strategy_version": "v1",
        "eligibility_basis": "daily",
        "minute_frequency": "1min",
        "window": StrategyWindowRequirement().model_dump(mode="json"),
    }
    legacy_id = _canonical_hash(
        {
            "spec": legacy_spec,
            "start_date": start_date,
            "end_date": end_date,
            "as_of_time": as_of_time,
            "code_commit": "a" * 40,
            "eligibility_ids": [],
        }
    )

    restored = BackfillManifest.model_validate(
        {
            "manifest_id": legacy_id,
            "spec": legacy_spec,
            "start_date": start_date,
            "end_date": end_date,
            "as_of_time": as_of_time,
            "code_commit": "a" * 40,
            "eligibilities": [],
        }
    )

    assert restored.manifest_id == legacy_id
    assert restored.spec.eligibility_entry_delay_trading_days == 0


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


def test_eligibility_resolution_distinguishes_zero_hits_from_unresolved_dates() -> None:
    from rquant.backfill_manifest import (
        EligibilityResolution,
        EligibilityResolutionGap,
    )

    resolved_empty = date(2026, 6, 25)
    resolved_with_hit = date(2026, 6, 26)
    unresolved = date(2026, 6, 29)
    record = _eligibility(
        "300001.SZ",
        eligibility_date=resolved_with_hit,
        entry_date=resolved_with_hit,
    )

    resolution = EligibilityResolution(
        strategy_id="growth_board_surge",
        strategy_version="v1",
        requested_dates=(resolved_empty, resolved_with_hit, unresolved),
        evaluated_dates=(resolved_empty, resolved_with_hit, unresolved),
        complete_dates=(resolved_empty, resolved_with_hit),
        incomplete=(
            EligibilityResolutionGap(
                eligibility_date=unresolved,
                reason="daily_inputs_incomplete",
            ),
        ),
        records=(record,),
    )

    assert resolution.expected_count == 3
    assert resolution.available_count == 2
    assert resolution.coverage_ratio == pytest.approx(2 / 3)
    assert resolved_empty in resolution.complete_dates
    assert not any(
        row.eligibility_date == resolved_empty for row in resolution.records
    )
    assert len(resolution.resolution_hash) == 64


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


def _seed_growth_input_panel(
    store,
    *,
    previous_date: date,
    signal_date: date,
    codes: tuple[str, ...] = ("300001.SZ",),
) -> None:
    for index, code in enumerate(codes):
        close = 10.0 + index
        store._conn.execute(
            """
            INSERT INTO daily_bar
            (ts_code, trade_date, close)
            VALUES (?, ?, ?)
            """,
            [code, previous_date, close],
        )
        store._conn.execute(
            """
            INSERT INTO daily_indicator
            (ts_code, trade_date, ma5, ma10, ma20, ma60)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [code, previous_date, close, close, close, close],
        )
        store._conn.execute(
            """
            INSERT INTO stock_status_daily
            (ts_code, trade_date, name, is_st, name_source, st_source,
             available_at, ingested_at, conflict_reason)
            VALUES (?, ?, ?, FALSE, 'test', 'test', ?, ?, NULL)
            """,
            [
                code,
                signal_date,
                f"样本{index}",
                datetime.combine(signal_date, time(1, 0), tzinfo=UTC),
                datetime.combine(signal_date, time(1, 0), tzinfo=UTC),
            ],
        )


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

    def fake_candidates(
        store,
        trading_date,
        previous_date,
        min_signal_time,
        *,
        structural_excluded_codes,
    ):
        del store, min_signal_time
        assert structural_excluded_codes == set()
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


def test_daily_zero_hit_date_is_complete_eligibility_evidence(
    monkeypatch,
    tmp_path,
) -> None:
    import rquant.growth_board_surge_strategy as growth
    from rquant.backfill_manifest import resolve_strategy_eligibility
    from rquant.storage.duckdb import DuckDBStore

    days = [date(2026, 6, 24), date(2026, 6, 25)]
    monkeypatch.setattr(
        growth,
        "resolve_growth_board_candidates",
        lambda *_args, **_kwargs: [],
    )
    with DuckDBStore(tmp_path / "growth-empty.duckdb") as store:
        _seed_open_calendar(store, days)
        _seed_growth_input_panel(
            store,
            previous_date=days[0],
            signal_date=days[1],
        )
        store._conn.execute(
            """
            INSERT INTO stock_basic (ts_code, list_date, market)
            VALUES ('300001.SZ', DATE '2020-01-01', '创业板')
            """
        )
        resolution = resolve_strategy_eligibility(
            store,
            strategy_id="growth_board_surge",
            start_date=days[1],
            end_date=days[1],
        )

    assert resolution.requested_dates == (days[1],)
    assert resolution.complete_dates == (days[1],)
    assert resolution.records == ()
    assert resolution.coverage_ratio == 1.0


def test_daily_zero_hits_are_incomplete_when_input_panel_is_missing(
    monkeypatch,
    tmp_path,
) -> None:
    import rquant.growth_board_surge_strategy as growth
    from rquant.backfill_manifest import resolve_strategy_eligibility
    from rquant.storage.duckdb import DuckDBStore

    days = [date(2026, 6, 24), date(2026, 6, 25)]
    monkeypatch.setattr(
        growth,
        "resolve_growth_board_candidates",
        lambda *_args, **_kwargs: [],
    )
    with DuckDBStore(tmp_path / "growth-missing-panel.duckdb") as store:
        _seed_open_calendar(store, days)
        resolution = resolve_strategy_eligibility(
            store,
            strategy_id="growth_board_surge",
            start_date=days[1],
            end_date=days[1],
        )

    assert resolution.complete_dates == ()
    assert resolution.incomplete[0].reason == "daily_input_panel_below_99pct"


def test_missing_auction_snapshot_is_not_self_certified_as_zero_hits(
    monkeypatch,
    tmp_path,
) -> None:
    import rquant.auction_gap_strategy as auction
    from rquant.backfill_manifest import resolve_strategy_eligibility
    from rquant.storage.duckdb import DuckDBStore

    previous_date = date(2026, 6, 25)
    signal_date = date(2026, 6, 26)
    monkeypatch.setattr(
        auction,
        "run_auction_gap_replay",
        lambda *_args, **_kwargs: pd.DataFrame(),
    )
    with DuckDBStore(tmp_path / "auction-empty.duckdb") as store:
        _seed_open_calendar(store, [previous_date, signal_date])
        _seed_growth_input_panel(
            store,
            previous_date=previous_date,
            signal_date=signal_date,
        )
        resolution = resolve_strategy_eligibility(
            store,
            strategy_id="auction_gap",
            start_date=signal_date,
            end_date=signal_date,
        )

    assert resolution.requested_dates == (signal_date,)
    assert resolution.complete_dates == ()
    assert resolution.coverage_ratio == 0.0
    assert resolution.incomplete[0].reason == "auction_input_panel_below_99pct"


def test_one_auction_row_does_not_certify_a_partially_missing_universe(
    monkeypatch,
    tmp_path,
) -> None:
    import rquant.auction_gap_strategy as auction
    from rquant.backfill_manifest import resolve_strategy_eligibility
    from rquant.storage.duckdb import DuckDBStore

    previous_date = date(2026, 6, 25)
    signal_date = date(2026, 6, 26)
    monkeypatch.setattr(
        auction,
        "run_auction_gap_replay",
        lambda *_args, **_kwargs: pd.DataFrame(),
    )
    with DuckDBStore(tmp_path / "auction-partial.duckdb") as store:
        _seed_open_calendar(store, [previous_date, signal_date])
        _seed_growth_input_panel(
            store,
            previous_date=previous_date,
            signal_date=signal_date,
            codes=("300001.SZ", "300002.SZ"),
        )
        store._conn.execute(
            """
            INSERT INTO auction_bar
            (ts_code, trade_date, auction_type, price, vol, source)
            VALUES (
                '300001.SZ', ?, 'open_realtime', 10.2, 1000, 'tushare'
            )
            """,
            [signal_date],
        )
        resolution = resolve_strategy_eligibility(
            store,
            strategy_id="auction_gap",
            start_date=signal_date,
            end_date=signal_date,
        )

    assert resolution.complete_dates == ()
    assert resolution.incomplete[0].reason == "auction_input_panel_below_99pct"


def test_listing_universe_detects_stock_missing_from_daily_bar(
    monkeypatch,
    tmp_path,
) -> None:
    import rquant.growth_board_surge_strategy as growth
    from rquant.backfill_manifest import resolve_strategy_eligibility
    from rquant.storage.duckdb import DuckDBStore

    previous_date = date(2026, 6, 25)
    signal_date = date(2026, 6, 26)
    monkeypatch.setattr(
        growth,
        "resolve_growth_board_candidates",
        lambda *_args, **_kwargs: [],
    )
    with DuckDBStore(tmp_path / "growth-independent-universe.duckdb") as store:
        _seed_open_calendar(store, [previous_date, signal_date])
        _seed_growth_input_panel(
            store,
            previous_date=previous_date,
            signal_date=signal_date,
            codes=("300001.SZ",),
        )
        store._conn.execute(
            """
            INSERT INTO stock_basic (ts_code, list_date, market)
            VALUES
                ('300001.SZ', DATE '2020-01-01', '创业板'),
                ('300002.SZ', DATE '2020-01-01', '创业板')
            """
        )

        resolution = resolve_strategy_eligibility(
            store,
            strategy_id="growth_board_surge",
            start_date=signal_date,
            end_date=signal_date,
        )

    assert resolution.complete_dates == ()
    assert resolution.incomplete[0].reason == "daily_input_panel_below_99pct"


def test_growth_panel_excludes_listing_without_60_prior_open_sessions(
    monkeypatch,
    tmp_path,
) -> None:
    import rquant.growth_board_surge_strategy as growth
    from rquant.backfill_manifest import (
        _opening_panel_counts,
        resolve_strategy_eligibility,
    )
    from rquant.storage.duckdb import DuckDBStore

    days = [date(2026, 6, 24) + timedelta(days=index) for index in range(62)]
    previous_date = days[-2]
    signal_date = days[-1]

    def stale_candidate(*_args, **_kwargs):
        return [
            growth.GrowthBoardCandidate(
                ts_code="300002.SZ",
                name="短上市脏指标",
                trade_date=signal_date,
                previous_date=previous_date,
                board_type="gem",
                pre_close=10.0,
                limit_up_price=12.0,
            )
        ]

    monkeypatch.setattr(
        growth,
        "resolve_growth_board_candidates",
        stale_candidate,
    )
    with DuckDBStore(tmp_path / "growth-new-listing.duckdb") as store:
        _seed_open_calendar(store, days)
        _seed_growth_input_panel(
            store,
            previous_date=previous_date,
            signal_date=signal_date,
            codes=("300001.SZ",),
        )
        store._conn.execute(
            """
            INSERT INTO stock_basic (ts_code, list_date, market)
            VALUES
                ('300001.SZ', ?, '创业板'),
                ('300002.SZ', ?, '创业板')
            """,
            [days[0], days[-10]],
        )

        resolution = resolve_strategy_eligibility(
            store,
            strategy_id="growth_board_surge",
            start_date=signal_date,
            end_date=signal_date,
        )
        counts = _opening_panel_counts(
            store,
            requested_dates=(signal_date,),
            calendar=days,
            strategy_id="growth_board_surge",
        )

    assert counts[signal_date] == (2, 2)
    assert resolution.complete_dates == (signal_date,)
    assert resolution.incomplete == ()
    assert resolution.records == ()


def test_growth_panel_excludes_verified_previous_full_day_suspension(
    monkeypatch,
    tmp_path,
) -> None:
    import rquant.growth_board_surge_strategy as growth
    from rquant.backfill_manifest import (
        _opening_panel_counts,
        resolve_strategy_eligibility,
    )
    from rquant.storage.duckdb import DuckDBStore

    previous_date = date(2026, 6, 25)
    signal_date = date(2026, 6, 26)
    monkeypatch.setattr(
        growth,
        "resolve_growth_board_candidates",
        lambda *_args, **_kwargs: [],
    )
    with DuckDBStore(tmp_path / "growth-suspended.duckdb") as store:
        _seed_open_calendar(store, [previous_date, signal_date])
        _seed_growth_input_panel(
            store,
            previous_date=previous_date,
            signal_date=signal_date,
            codes=("300001.SZ",),
        )
        store._conn.execute(
            """
            INSERT INTO stock_basic (ts_code, list_date, market)
            VALUES
                ('300001.SZ', DATE '2020-01-01', '创业板'),
                ('300002.SZ', DATE '2020-01-01', '创业板')
            """
        )
        store._conn.execute(
            """
            INSERT INTO stock_suspend_coverage
            (source, trade_date, coverage_state, row_count, snapshot_hash, queried_at)
            VALUES ('tushare', ?, 'complete', 1, 'snapshot', ?)
            """,
            [previous_date, datetime(2026, 6, 25, 16, tzinfo=UTC)],
        )
        store._conn.execute(
            """
            INSERT INTO stock_suspend_event
            (source, ts_code, trade_date, suspend_type, suspend_timing,
             session_scope, available_at, ingested_at)
            VALUES
            ('tushare', '300002.SZ', ?, 'S', '09:30-15:00',
             'full_day', ?, ?)
            """,
            [
                previous_date,
                datetime(2026, 6, 25, 8, tzinfo=UTC),
                datetime(2026, 6, 25, 16, tzinfo=UTC),
            ],
        )

        resolution = resolve_strategy_eligibility(
            store,
            strategy_id="growth_board_surge",
            start_date=signal_date,
            end_date=signal_date,
        )
        counts = _opening_panel_counts(
            store,
            requested_dates=(signal_date,),
            calendar=[previous_date, signal_date],
            strategy_id="growth_board_surge",
        )

    assert counts[signal_date] == (2, 2)
    assert resolution.complete_dates == (signal_date,)
    assert resolution.incomplete == ()


@pytest.mark.parametrize(
    ("coverage_state", "session_scope", "has_resume"),
    [
        ("unverified_empty", "full_day", False),
        ("unsupported", "full_day", False),
        ("complete", "partial", False),
        ("complete", "full_day", True),
    ],
)
def test_growth_panel_keeps_unproven_suspension_in_denominator(
    monkeypatch,
    tmp_path,
    coverage_state: str,
    session_scope: str,
    has_resume: bool,
) -> None:
    import rquant.growth_board_surge_strategy as growth
    from rquant.backfill_manifest import resolve_strategy_eligibility
    from rquant.storage.duckdb import DuckDBStore

    previous_date = date(2026, 6, 25)
    signal_date = date(2026, 6, 26)
    monkeypatch.setattr(
        growth,
        "resolve_growth_board_candidates",
        lambda *_args, **_kwargs: [],
    )
    suffix = "resume" if has_resume else "no-resume"
    with DuckDBStore(
        tmp_path / f"growth-{coverage_state}-{session_scope}-{suffix}.duckdb"
    ) as store:
        _seed_open_calendar(store, [previous_date, signal_date])
        _seed_growth_input_panel(
            store,
            previous_date=previous_date,
            signal_date=signal_date,
            codes=("300001.SZ",),
        )
        store._conn.execute(
            """
            INSERT INTO stock_basic (ts_code, list_date, market)
            VALUES
                ('300001.SZ', DATE '2020-01-01', '创业板'),
                ('300002.SZ', DATE '2020-01-01', '创业板')
            """
        )
        store._conn.execute(
            """
            INSERT INTO stock_suspend_coverage
            (source, trade_date, coverage_state, row_count, snapshot_hash, queried_at)
            VALUES ('tushare', ?, ?, 1, 'snapshot', ?)
            """,
            [
                previous_date,
                coverage_state,
                datetime(2026, 6, 25, 16, tzinfo=UTC),
            ],
        )
        store._conn.execute(
            """
            INSERT INTO stock_suspend_event
            (source, ts_code, trade_date, suspend_type, suspend_timing,
             session_scope, available_at, ingested_at)
            VALUES
            ('tushare', '300002.SZ', ?, 'S', '09:30-15:00', ?, ?, ?)
            """,
            [
                previous_date,
                session_scope,
                datetime(2026, 6, 25, 8, tzinfo=UTC),
                datetime(2026, 6, 25, 16, tzinfo=UTC),
            ],
        )
        if has_resume:
            store._conn.execute(
                """
                INSERT INTO stock_suspend_event
                (source, ts_code, trade_date, suspend_type, suspend_timing,
                 session_scope, available_at, ingested_at)
                VALUES
                ('tushare', '300002.SZ', ?, 'R', '09:30', 'partial', ?, ?)
                """,
                [
                    previous_date,
                    datetime(2026, 6, 25, 8, tzinfo=UTC),
                    datetime(2026, 6, 25, 16, tzinfo=UTC),
                ],
            )

        resolution = resolve_strategy_eligibility(
            store,
            strategy_id="growth_board_surge",
            start_date=signal_date,
            end_date=signal_date,
        )

    assert resolution.complete_dates == ()
    assert resolution.incomplete[0].reason == "daily_input_panel_below_99pct"


def test_growth_panel_keeps_missing_authoritative_listing_date_fail_closed(
    monkeypatch,
    tmp_path,
) -> None:
    import rquant.growth_board_surge_strategy as growth
    from rquant.backfill_manifest import (
        _opening_panel_counts,
        resolve_strategy_eligibility,
    )
    from rquant.storage.duckdb import DuckDBStore

    days = [date(2026, 6, 24) + timedelta(days=index) for index in range(62)]
    previous_date = days[-2]
    signal_date = days[-1]
    monkeypatch.setattr(
        growth,
        "resolve_growth_board_candidates",
        lambda *_args, **_kwargs: [],
    )
    with DuckDBStore(tmp_path / "growth-missing-list-date.duckdb") as store:
        _seed_open_calendar(store, days)
        _seed_growth_input_panel(
            store,
            previous_date=previous_date,
            signal_date=signal_date,
            codes=("300001.SZ",),
        )
        store._conn.execute(
            """
            INSERT INTO stock_basic (ts_code, list_date, market)
            VALUES ('300001.SZ', ?, '创业板')
            """,
            [days[0]],
        )
        store._conn.execute(
            """
            INSERT INTO daily_bar (ts_code, trade_date, close)
            VALUES ('300002.SZ', ?, 10)
            """,
            [previous_date],
        )
        store._conn.execute(
            """
            INSERT INTO stock_status_daily
            (ts_code, trade_date, name, is_st, name_source, st_source,
             available_at, ingested_at, conflict_reason)
            VALUES ('300002.SZ', ?, '缺上市日期', FALSE, 'test', 'test',
                    ?, ?, NULL)
            """,
            [
                signal_date,
                datetime.combine(signal_date, time(1), tzinfo=UTC),
                datetime.combine(signal_date, time(1), tzinfo=UTC),
            ],
        )

        resolution = resolve_strategy_eligibility(
            store,
            strategy_id="growth_board_surge",
            start_date=signal_date,
            end_date=signal_date,
        )
        counts = _opening_panel_counts(
            store,
            requested_dates=(signal_date,),
            calendar=days,
            strategy_id="growth_board_surge",
        )

    assert counts[signal_date] == (2, 1)
    assert resolution.complete_dates == ()


def test_growth_panel_all_short_listings_keep_observable_nonzero_denominator(
    monkeypatch,
    tmp_path,
) -> None:
    import rquant.growth_board_surge_strategy as growth
    from rquant.backfill_manifest import (
        _opening_panel_counts,
        resolve_strategy_eligibility,
    )
    from rquant.storage.duckdb import DuckDBStore

    days = [date(2026, 6, 24) + timedelta(days=index) for index in range(12)]
    signal_date = days[-1]
    monkeypatch.setattr(
        growth,
        "resolve_growth_board_candidates",
        lambda *_args, **_kwargs: [],
    )
    with DuckDBStore(tmp_path / "growth-all-new.duckdb") as store:
        _seed_open_calendar(store, days)
        store._conn.execute(
            """
            INSERT INTO stock_basic (ts_code, list_date, market)
            VALUES ('300002.SZ', ?, '创业板')
            """,
            [days[2]],
        )

        resolution = resolve_strategy_eligibility(
            store,
            strategy_id="growth_board_surge",
            start_date=signal_date,
            end_date=signal_date,
        )
        counts = _opening_panel_counts(
            store,
            requested_dates=(signal_date,),
            calendar=days,
            strategy_id="growth_board_surge",
        )

    assert counts[signal_date] == (1, 1)
    assert resolution.complete_dates == (signal_date,)
    assert resolution.records == ()


def test_growth_panel_60_session_listing_missing_ma60_is_not_structural(
    monkeypatch,
    tmp_path,
) -> None:
    import rquant.growth_board_surge_strategy as growth
    from rquant.backfill_manifest import (
        _opening_panel_counts,
        resolve_strategy_eligibility,
    )
    from rquant.storage.duckdb import DuckDBStore

    days = [date(2026, 6, 24) + timedelta(days=index) for index in range(61)]
    previous_date = days[-2]
    signal_date = days[-1]
    monkeypatch.setattr(
        growth,
        "resolve_growth_board_candidates",
        lambda *_args, **_kwargs: [],
    )
    with DuckDBStore(tmp_path / "growth-60-session-boundary.duckdb") as store:
        _seed_open_calendar(store, days)
        _seed_growth_input_panel(
            store,
            previous_date=previous_date,
            signal_date=signal_date,
            codes=("300001.SZ",),
        )
        store._conn.execute(
            """
            INSERT INTO stock_basic (ts_code, list_date, market)
            VALUES
                ('300001.SZ', ?, '创业板'),
                ('300002.SZ', ?, '创业板')
            """,
            [days[0], days[0]],
        )
        store._conn.execute(
            """
            INSERT INTO daily_bar (ts_code, trade_date, close)
            VALUES ('300002.SZ', ?, 10)
            """,
            [previous_date],
        )
        store._conn.execute(
            """
            INSERT INTO stock_status_daily
            (ts_code, trade_date, name, is_st, name_source, st_source,
             available_at, ingested_at, conflict_reason)
            VALUES ('300002.SZ', ?, '第60日', FALSE, 'test', 'test',
                    ?, ?, NULL)
            """,
            [
                signal_date,
                datetime.combine(signal_date, time(1), tzinfo=UTC),
                datetime.combine(signal_date, time(1), tzinfo=UTC),
            ],
        )

        resolution = resolve_strategy_eligibility(
            store,
            strategy_id="growth_board_surge",
            start_date=signal_date,
            end_date=signal_date,
        )
        counts = _opening_panel_counts(
            store,
            requested_dates=(signal_date,),
            calendar=days,
            strategy_id="growth_board_surge",
        )

    assert counts[signal_date] == (2, 1)
    assert resolution.complete_dates == ()


def test_growth_panel_rejects_historical_suspension_with_residual_bar(
    monkeypatch,
    tmp_path,
) -> None:
    import rquant.growth_board_surge_strategy as growth
    from rquant.backfill_manifest import (
        _opening_panel_counts,
        resolve_strategy_eligibility,
    )
    from rquant.storage.duckdb import DuckDBStore

    days = [date(2026, 6, 24) + timedelta(days=index) for index in range(61)]
    historical_suspension = days[10]
    previous_date = days[-2]
    signal_date = days[-1]
    monkeypatch.setattr(
        growth,
        "resolve_growth_board_candidates",
        lambda *_args, **_kwargs: [],
    )
    with DuckDBStore(tmp_path / "growth-suspension-conflict.duckdb") as store:
        _seed_open_calendar(store, days)
        _seed_growth_input_panel(
            store,
            previous_date=previous_date,
            signal_date=signal_date,
        )
        store._conn.execute(
            """
            INSERT INTO stock_basic (ts_code, list_date, market)
            VALUES ('300001.SZ', ?, '创业板')
            """,
            [days[0]],
        )
        store._conn.execute(
            """
            INSERT INTO daily_bar (ts_code, trade_date, close)
            VALUES ('300001.SZ', ?, 9)
            """,
            [historical_suspension],
        )
        store._conn.execute(
            """
            INSERT INTO stock_suspend_coverage
            (source, trade_date, coverage_state, row_count, snapshot_hash, queried_at)
            VALUES ('tushare', ?, 'complete', 1, 'snapshot', ?)
            """,
            [
                historical_suspension,
                datetime(2026, 8, 24, 16, tzinfo=UTC),
            ],
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
                historical_suspension,
                datetime(2026, 7, 4, 8, tzinfo=UTC),
                datetime(2026, 8, 24, 16, tzinfo=UTC),
            ],
        )

        resolution = resolve_strategy_eligibility(
            store,
            strategy_id="growth_board_surge",
            start_date=signal_date,
            end_date=signal_date,
        )
        counts = _opening_panel_counts(
            store,
            requested_dates=(signal_date,),
            calendar=days,
            strategy_id="growth_board_surge",
        )

    assert counts[signal_date] == (1, 0)
    assert resolution.complete_dates == ()


def test_growth_panel_rejects_future_listing_fact(
    monkeypatch,
    tmp_path,
) -> None:
    import rquant.growth_board_surge_strategy as growth
    from rquant.backfill_manifest import (
        _opening_panel_counts,
        resolve_strategy_eligibility,
    )
    from rquant.storage.duckdb import DuckDBStore

    previous_date = date(2026, 6, 25)
    signal_date = date(2026, 6, 26)
    monkeypatch.setattr(
        growth,
        "resolve_growth_board_candidates",
        lambda *_args, **_kwargs: [],
    )
    with DuckDBStore(tmp_path / "growth-future-listing.duckdb") as store:
        _seed_open_calendar(store, [previous_date, signal_date])
        _seed_growth_input_panel(
            store,
            previous_date=previous_date,
            signal_date=signal_date,
        )
        store._conn.execute(
            """
            INSERT INTO stock_basic (ts_code, list_date, market)
            VALUES ('300001.SZ', DATE '2026-07-01', '创业板')
            """
        )

        resolution = resolve_strategy_eligibility(
            store,
            strategy_id="growth_board_surge",
            start_date=signal_date,
            end_date=signal_date,
        )
        counts = _opening_panel_counts(
            store,
            requested_dates=(signal_date,),
            calendar=[previous_date, signal_date],
            strategy_id="growth_board_surge",
        )

    assert counts[signal_date] == (1, 0)
    assert resolution.complete_dates == ()


def test_growth_panel_calendar_gap_cannot_prove_short_listing(
    monkeypatch,
    tmp_path,
) -> None:
    import rquant.growth_board_surge_strategy as growth
    from rquant.backfill_manifest import (
        _opening_panel_counts,
        resolve_strategy_eligibility,
    )
    from rquant.storage.duckdb import DuckDBStore

    civil_days = [
        date(2026, 6, 24) + timedelta(days=index)
        for index in range(61)
    ]
    missing_day = civil_days[20]
    known_days = [day for day in civil_days if day != missing_day]
    signal_date = civil_days[-1]
    monkeypatch.setattr(
        growth,
        "resolve_growth_board_candidates",
        lambda *_args, **_kwargs: [],
    )
    with DuckDBStore(tmp_path / "growth-calendar-gap.duckdb") as store:
        _seed_open_calendar(store, known_days)
        store._conn.execute(
            """
            INSERT INTO stock_basic (ts_code, list_date, market)
            VALUES ('300002.SZ', ?, '创业板')
            """,
            [civil_days[0]],
        )

        resolution = resolve_strategy_eligibility(
            store,
            strategy_id="growth_board_surge",
            start_date=signal_date,
            end_date=signal_date,
        )
        counts = _opening_panel_counts(
            store,
            requested_dates=(signal_date,),
            calendar=known_days,
            strategy_id="growth_board_surge",
        )

    assert counts[signal_date] == (1, 0)
    assert resolution.complete_dates == ()


def test_auction_panel_excludes_b_shares_from_a_share_denominator(
    monkeypatch,
    tmp_path,
) -> None:
    import rquant.auction_gap_strategy as auction
    from rquant.backfill_manifest import resolve_strategy_eligibility
    from rquant.storage.duckdb import DuckDBStore

    previous_date = date(2026, 6, 25)
    signal_date = date(2026, 6, 26)
    monkeypatch.setattr(
        auction,
        "run_auction_gap_replay",
        lambda *_args, **_kwargs: pd.DataFrame(),
    )
    with DuckDBStore(tmp_path / "auction-a-share-universe.duckdb") as store:
        _seed_open_calendar(store, [previous_date, signal_date])
        _seed_growth_input_panel(
            store,
            previous_date=previous_date,
            signal_date=signal_date,
            codes=("300001.SZ",),
        )
        store._conn.execute(
            """
            INSERT INTO stock_basic (ts_code, list_date, market)
            VALUES
                ('300001.SZ', DATE '2020-01-01', '创业板'),
                ('200001.SZ', DATE '2020-01-01', '主板'),
                ('900901.SH', DATE '2020-01-01', '主板')
            """
        )
        store._conn.execute(
            """
            INSERT INTO auction_bar
            (ts_code, trade_date, auction_type, price, vol, source)
            VALUES
            ('300001.SZ', ?, 'open_realtime', 10.2, 1000, 'tushare')
            """,
            [signal_date],
        )

        resolution = resolve_strategy_eligibility(
            store,
            strategy_id="auction_gap",
            start_date=signal_date,
            end_date=signal_date,
        )

    assert resolution.complete_dates == (signal_date,)
    assert resolution.incomplete == ()


def test_n_shape_panel_excludes_b_shares_from_a_share_denominator(
    tmp_path,
) -> None:
    from rquant.backfill_manifest import _n_shape_complete_dates
    from rquant.storage.duckdb import DuckDBStore

    days = [date(2026, 6, 24) + timedelta(days=index) for index in range(121)]
    target = days[-1]
    with DuckDBStore(tmp_path / "n-shape-a-share-universe.duckdb") as store:
        _seed_open_calendar(store, days)
        store._conn.execute(
            """
            INSERT INTO stock_basic (ts_code, list_date, market)
            VALUES
                ('000001.SZ', DATE '1991-04-03', '主板'),
                ('200001.SZ', DATE '1992-02-28', '主板'),
                ('900901.SH', DATE '1992-02-21', '主板')
            """
        )
        for trading_date in days:
            store._conn.execute(
                """
                INSERT INTO daily_bar
                (ts_code, trade_date, open, high, low, close)
                VALUES ('000001.SZ', ?, 10, 11, 9, 10)
                """,
                [trading_date],
            )
            store._conn.execute(
                """
                INSERT INTO daily_state
                (ts_code, trade_date, is_st, is_limit_up, is_limit_down,
                 is_first_limit_up, is_yiziban, consecutive_limit_ups,
                 body_upper, body_lower)
                VALUES
                ('000001.SZ', ?, FALSE, FALSE, FALSE, FALSE, FALSE, 0, 10, 10)
                """,
                [trading_date],
            )
            store._conn.execute(
                """
                INSERT INTO stock_status_daily
                (ts_code, trade_date, name, is_st, name_source, st_source,
                 available_at, ingested_at, conflict_reason)
                VALUES
                ('000001.SZ', ?, '平安银行', FALSE, 'test', 'test', ?, ?, NULL)
                """,
                [
                    trading_date,
                    datetime.combine(trading_date, time(8, 0), tzinfo=UTC),
                    datetime.combine(trading_date, time(8, 0), tzinfo=UTC),
                ],
            )
        store._conn.execute(
            """
            INSERT INTO daily_basic (ts_code, trade_date, circ_mv)
            VALUES ('000001.SZ', ?, 1000000)
            """,
            [target],
        )

        complete_dates = _n_shape_complete_dates(
            store,
            requested_dates=(target,),
            calendar=days,
        )

    assert complete_dates == (target,)


def _seed_n_shape_panel_with_missing_suspended_code(
    store,
    *,
    days: list[date],
    coverage_state: str,
    has_resume: bool,
) -> None:
    store._conn.execute(
        """
        INSERT INTO stock_basic (ts_code, list_date, market)
        VALUES
            ('000001.SZ', DATE '1991-04-03', '主板'),
            ('000002.SZ', DATE '1991-04-03', '主板')
        """
    )
    for trading_date in days:
        observed_at = datetime.combine(trading_date, time(8, 0), tzinfo=UTC)
        store._conn.execute(
            """
            INSERT INTO daily_bar
            (ts_code, trade_date, open, high, low, close)
            VALUES ('000001.SZ', ?, 10, 11, 9, 10)
            """,
            [trading_date],
        )
        store._conn.execute(
            """
            INSERT INTO daily_state
            (ts_code, trade_date, is_st, is_limit_up, is_limit_down,
             is_first_limit_up, is_yiziban, consecutive_limit_ups,
             body_upper, body_lower)
            VALUES
            ('000001.SZ', ?, FALSE, FALSE, FALSE, FALSE, FALSE, 0, 10, 10)
            """,
            [trading_date],
        )
        store._conn.execute(
            """
            INSERT INTO stock_status_daily
            (ts_code, trade_date, name, is_st, name_source, st_source,
             available_at, ingested_at, conflict_reason)
            VALUES
            ('000001.SZ', ?, '平安银行', FALSE, 'test', 'test', ?, ?, NULL)
            """,
            [trading_date, observed_at, observed_at],
        )
        store._conn.execute(
            """
            INSERT INTO stock_suspend_coverage
            (source, trade_date, coverage_state, row_count, snapshot_hash, queried_at)
            VALUES ('tushare', ?, ?, 1, 'snapshot', ?)
            """,
            [trading_date, coverage_state, observed_at],
        )
        store._conn.execute(
            """
            INSERT INTO stock_suspend_event
            (source, ts_code, trade_date, suspend_type, suspend_timing,
             session_scope, available_at, ingested_at)
            VALUES
            ('tushare', '000002.SZ', ?, 'S', '', 'unknown', ?, ?)
            """,
            [trading_date, observed_at, observed_at],
        )
        if has_resume:
            store._conn.execute(
                """
                INSERT INTO stock_suspend_event
                (source, ts_code, trade_date, suspend_type, suspend_timing,
                 session_scope, available_at, ingested_at)
                VALUES
                ('tushare', '000002.SZ', ?, 'R', '', 'unknown', ?, ?)
                """,
                [trading_date, observed_at, observed_at],
            )
    store._conn.execute(
        """
        INSERT INTO daily_basic (ts_code, trade_date, circ_mv)
        VALUES ('000001.SZ', ?, 1000000)
        """,
        [days[-1]],
    )


def test_n_shape_panel_accepts_complete_s_only_day_without_daily_bar(
    tmp_path,
) -> None:
    from rquant.backfill_manifest import _n_shape_complete_dates
    from rquant.storage.duckdb import DuckDBStore

    days = [date(2026, 6, 24) + timedelta(days=index) for index in range(121)]
    target = days[-1]
    with DuckDBStore(tmp_path / "n-shape-s-only-suspension.duckdb") as store:
        _seed_open_calendar(store, days)
        _seed_n_shape_panel_with_missing_suspended_code(
            store,
            days=days,
            coverage_state="complete",
            has_resume=False,
        )

        complete_dates = _n_shape_complete_dates(
            store,
            requested_dates=(target,),
            calendar=days,
        )

    assert complete_dates == (target,)


def test_n_shape_panel_rejects_suspension_with_same_day_resume(
    tmp_path,
) -> None:
    from rquant.backfill_manifest import _n_shape_complete_dates
    from rquant.storage.duckdb import DuckDBStore

    days = [date(2026, 6, 24) + timedelta(days=index) for index in range(121)]
    target = days[-1]
    with DuckDBStore(tmp_path / "n-shape-suspend-resume.duckdb") as store:
        _seed_open_calendar(store, days)
        _seed_n_shape_panel_with_missing_suspended_code(
            store,
            days=days,
            coverage_state="complete",
            has_resume=True,
        )

        complete_dates = _n_shape_complete_dates(
            store,
            requested_dates=(target,),
            calendar=days,
        )

    assert complete_dates == ()


def test_n_shape_panel_rejects_suspension_without_complete_coverage(
    tmp_path,
) -> None:
    from rquant.backfill_manifest import _n_shape_complete_dates
    from rquant.storage.duckdb import DuckDBStore

    days = [date(2026, 6, 24) + timedelta(days=index) for index in range(121)]
    target = days[-1]
    with DuckDBStore(tmp_path / "n-shape-unverified-suspension.duckdb") as store:
        _seed_open_calendar(store, days)
        _seed_n_shape_panel_with_missing_suspended_code(
            store,
            days=days,
            coverage_state="unverified_empty",
            has_resume=False,
        )

        complete_dates = _n_shape_complete_dates(
            store,
            requested_dates=(target,),
            calendar=days,
        )

    assert complete_dates == ()


def test_n_shape_panel_rejects_s_only_suspension_with_residual_daily_bar(
    tmp_path,
) -> None:
    from rquant.backfill_manifest import _n_shape_complete_dates
    from rquant.storage.duckdb import DuckDBStore

    days = [date(2026, 6, 24) + timedelta(days=index) for index in range(121)]
    target = days[-1]
    with DuckDBStore(tmp_path / "n-shape-suspension-bar-conflict.duckdb") as store:
        _seed_open_calendar(store, days)
        _seed_n_shape_panel_with_missing_suspended_code(
            store,
            days=days,
            coverage_state="complete",
            has_resume=False,
        )
        store._conn.executemany(
            """
            INSERT INTO daily_bar
            (ts_code, trade_date, open, high, low, close)
            VALUES ('000002.SZ', ?, 10, 11, 9, 10)
            """,
            [(trading_date,) for trading_date in days],
        )

        complete_dates = _n_shape_complete_dates(
            store,
            requested_dates=(target,),
            calendar=days,
        )

    assert complete_dates == ()


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
