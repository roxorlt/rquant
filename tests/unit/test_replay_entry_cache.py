"""分钟 replay 入场层缓存测试。"""

from __future__ import annotations

from datetime import date, datetime


def _snapshot():
    from rquant.replay_entry_cache import (
        ReplayEntrySnapshot,
        ReplayWatchItem,
        RiskReplayQuote,
    )

    return ReplayEntrySnapshot(
        item=ReplayWatchItem(
            ts_code="600000.SH",
            pool="pool1",
            name="浦发银行",
            entry_date=date(2026, 6, 24),
            reference_date=date(2026, 6, 24),
            limit_up_date=date(2026, 6, 24),
            t_close=10.0,
            t_high=10.2,
            limit_up_price_next=None,
            stop_weak=0.0,
        ),
        execution_quote=RiskReplayQuote(
            ts_code="600000.SH",
            trade_time=datetime(2026, 6, 25, 9, 33),
            price=10.0,
            low=10.0,
            high=10.0,
        ),
        signal={
            "level": "minute_vwap_confirm",
            "trigger_type": "minute_vwap_confirm",
            "level_price": 10.2,
            "signal_price": 10.24,
        },
        entry_time=datetime(2026, 6, 25, 9, 33),
        earliest_exit_date=date(2026, 6, 26),
        window_dates=(date(2026, 6, 25), date(2026, 6, 26)),
        exit_quotes=(
            RiskReplayQuote(
                ts_code="600000.SH",
                trade_time=datetime(2026, 6, 25, 9, 33),
                price=10.4,
                low=10.0,
                high=10.6,
            ),
            RiskReplayQuote(
                ts_code="600000.SH",
                trade_time=datetime(2026, 6, 26, 9, 31),
                price=10.4,
                low=10.35,
                high=10.42,
            ),
        ),
        feature_payload={"signal_rel_amount_same_minute_20d": 2.5},
    )


def test_replay_snapshot_exit_reopens_position_with_new_risk_config() -> None:
    from rquant.paper import PaperTradeConfig
    from rquant.replay_entry_cache import replay_snapshot_exit

    cached = _snapshot()

    trailing = replay_snapshot_exit(
        cached,
        PaperTradeConfig(
            candidate_id="tp5",
            take_profit_pct=0.05,
            trailing_stop_pct=0.02,
        ),
    )
    patient = replay_snapshot_exit(
        cached,
        PaperTradeConfig(
            candidate_id="tp8",
            take_profit_pct=0.08,
            trailing_stop_pct=0.02,
        ),
    )

    assert trailing is not None
    assert patient is not None
    assert trailing.candidate_id == "tp5"
    assert trailing.exit_reason == "take_profit_trailing"
    assert trailing.exit_price == 10.38
    assert patient.candidate_id == "tp8"
    assert patient.exit_reason == "time_1d"
    assert patient.exit_price == 10.4


def test_entry_replay_cache_reuses_snapshot_loader() -> None:
    from rquant.replay_entry_cache import EntryReplayCache, ReplayEntryCacheKey

    calls = 0

    def loader():
        nonlocal calls
        calls += 1
        return [_snapshot()]

    cache = EntryReplayCache()
    key = ReplayEntryCacheKey(
        preset_name="n-shape-combined",
        start_date="2026-06-24",
        end_date="2026-06-24",
        entry_mode="vwap_confirm",
        profile_variant="vp_risk_only",
        max_hold_days=1,
    )

    first = cache.get_or_load(key, loader)
    second = cache.get_or_load(key, loader)

    assert calls == 1
    assert first == second
    assert first is not second
