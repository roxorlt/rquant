"""分钟 replay 交易样本缓存测试。"""

from __future__ import annotations

import pandas as pd


def test_replay_trade_cache_reuses_same_key() -> None:
    from rquant.replay_cache import ReplayCacheKey, ReplayTradeCache

    calls = 0

    def loader() -> pd.DataFrame:
        nonlocal calls
        calls += 1
        return pd.DataFrame([{"ts_code": "600000.SH", "ret_pct": 3.0}])

    cache = ReplayTradeCache()
    key = ReplayCacheKey(
        preset_name="n-shape-combined",
        start_date="2026-04-16",
        end_date="2026-06-08",
        entry_mode="vwap_confirm",
        profile_variant="vp_risk_only",
        max_hold_days=5,
        paper_candidate_id="tp5_trailing25_stop3",
    )

    first = cache.get_or_load(key, loader)
    second = cache.get_or_load(key, loader)

    assert calls == 1
    assert first.equals(second)
    assert first is not second


def test_replay_cache_key_changes_with_risk_profile() -> None:
    from rquant.replay_cache import ReplayCacheKey

    base = ReplayCacheKey(
        preset_name="n-shape-combined",
        start_date="2026-04-16",
        end_date="2026-06-08",
        entry_mode="vwap_confirm",
        profile_variant="vp_risk_only",
        max_hold_days=5,
        paper_candidate_id="tp5_trailing25_stop3",
    )
    wider_take_profit = base.model_copy(
        update={"paper_candidate_id": "tp8_trailing30_stop3"}
    )

    assert base.cache_key != wider_take_profit.cache_key
