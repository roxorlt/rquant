"""分钟价量分布特征测试。"""

from __future__ import annotations

from datetime import date, datetime

import pandas as pd
import pytest

from rquant.storage.duckdb import DuckDBStore


@pytest.fixture()
def store(tmp_path):
    s = DuckDBStore(tmp_path / "test.duckdb")
    yield s
    s.close()


def _seed_profile_data(store: DuckDBStore) -> None:
    store.upsert_daily(pd.DataFrame([
        {
            "ts_code": "600000.SH",
            "trade_date": date(2026, 6, day),
            "open": 10.0,
            "high": 11.0,
            "low": 9.0,
            "close": 10.0,
            "pre_close": 10.0,
            "change": 0.0,
            "pct_chg": 0.0,
            "vol": 1,
            "amount": 1,
        }
        for day in [1, 2, 3, 4]
    ]))
    store.upsert_adj_factor(pd.DataFrame([
        {
            "ts_code": "600000.SH",
            "trade_date": date(2026, 6, day),
            "adj_factor": 1.0,
        }
        for day in [1, 2, 3, 4]
    ]))
    store.upsert_minute_bars(pd.DataFrame([
        {
            "ts_code": "600000.SH",
            "trade_time": datetime(2026, 6, 1, 9, 30),
            "freq": "1min",
            "open": 10.0,
            "high": 10.0,
            "low": 10.0,
            "close": 10.0,
            "vol": 100.0,
            "amount": 1000.0,
            "source": "tushare",
        },
        {
            "ts_code": "600000.SH",
            "trade_time": datetime(2026, 6, 2, 9, 30),
            "freq": "1min",
            "open": 11.0,
            "high": 11.0,
            "low": 11.0,
            "close": 11.0,
            "vol": 300.0,
            "amount": 3300.0,
            "source": "tushare",
        },
        {
            "ts_code": "600000.SH",
            "trade_time": datetime(2026, 6, 3, 9, 30),
            "freq": "1min",
            "open": 9.0,
            "high": 9.0,
            "low": 9.0,
            "close": 9.0,
            "vol": 50.0,
            "amount": 450.0,
            "source": "tushare",
        },
    ]))


def test_calculate_volume_profile_uses_previous_trading_days(
    store: DuckDBStore,
) -> None:
    from rquant.volume_profile import calculate_volume_profile

    _seed_profile_data(store)

    profile = calculate_volume_profile(
        store,
        "600000.SH",
        reference_date=date(2026, 6, 4),
        lookback_days=3,
    )

    assert profile is not None
    assert profile.rows_count == 3
    assert profile.weight_basis == "adjusted_share_volume"
    assert profile.total_vol == 450.0
    assert profile.total_amount == 4750.0
    assert profile.vwap == pytest.approx(4750.0 / 450.0)
    assert profile.poc_price == 11.0
    assert profile.value_area_low <= 11.0 <= profile.value_area_high
    assert profile.above_reference_amount_pct == pytest.approx(3300 / 4750 * 100)


def test_volume_profile_rule_config_defaults_to_90_day_window() -> None:
    from rquant.volume_profile import VolumeProfileRuleConfig

    assert VolumeProfileRuleConfig().lookback_days == (90,)


def test_calculate_volume_profile_returns_none_without_minutes(
    store: DuckDBStore,
) -> None:
    from rquant.volume_profile import calculate_volume_profile

    store.upsert_daily(pd.DataFrame([{
        "ts_code": "600000.SH",
        "trade_date": date(2026, 6, 4),
        "open": 10.0,
        "high": 10.0,
        "low": 10.0,
        "close": 10.0,
        "pre_close": 10.0,
        "change": 0.0,
        "pct_chg": 0.0,
        "vol": 1,
        "amount": 1,
    }]))

    assert calculate_volume_profile(
        store,
        "600000.SH",
        reference_date=date(2026, 6, 4),
        lookback_days=30,
    ) is None


def test_calculate_volume_profile_scales_minutes_to_reference_factor(
    store: DuckDBStore,
) -> None:
    from rquant.volume_profile import calculate_volume_profile

    store.upsert_daily(pd.DataFrame([
        {
            "ts_code": "600000.SH",
            "trade_date": date(2026, 6, day),
            "open": 10.0,
            "high": 20.0,
            "low": 9.0,
            "close": 10.0,
            "pre_close": 10.0,
            "change": 0.0,
            "pct_chg": 0.0,
            "vol": 1,
            "amount": 1,
        }
        for day in [1, 2, 3, 4]
    ]))
    store.upsert_adj_factor(pd.DataFrame([
        {"ts_code": "600000.SH", "trade_date": date(2026, 6, 1), "adj_factor": 0.5},
        {"ts_code": "600000.SH", "trade_date": date(2026, 6, 2), "adj_factor": 1.0},
        {"ts_code": "600000.SH", "trade_date": date(2026, 6, 3), "adj_factor": 1.0},
        {"ts_code": "600000.SH", "trade_date": date(2026, 6, 4), "adj_factor": 1.0},
    ]))
    store.upsert_minute_bars(pd.DataFrame([
        {
            "ts_code": "600000.SH",
            "trade_time": datetime(2026, 6, 1, 9, 30),
            "freq": "1min",
            "open": 20.0,
            "high": 20.0,
            "low": 20.0,
            "close": 20.0,
            "vol": 1000.0,
            "amount": 20000.0,
            "source": "tushare",
        },
        {
            "ts_code": "600000.SH",
            "trade_time": datetime(2026, 6, 2, 9, 30),
            "freq": "1min",
            "open": 10.0,
            "high": 10.0,
            "low": 10.0,
            "close": 10.0,
            "vol": 100.0,
            "amount": 1000.0,
            "source": "tushare",
        },
    ]))

    profile = calculate_volume_profile(
        store,
        "600000.SH",
        reference_date=date(2026, 6, 4),
        lookback_days=3,
    )

    assert profile is not None
    assert profile.poc_price == 10.0


def test_build_volume_profile_risk_plan_uses_support_and_fallback_target(
    store: DuckDBStore,
) -> None:
    from rquant.volume_profile import (
        VolumeProfileRuleConfig,
        build_volume_profile_risk_plan,
        calculate_volume_profile,
    )

    _seed_profile_data(store)
    profile = calculate_volume_profile(
        store,
        "600000.SH",
        reference_date=date(2026, 6, 4),
        lookback_days=3,
    )
    assert profile is not None

    plan = build_volume_profile_risk_plan(
        [profile],
        entry_price=11.20,
        config=VolumeProfileRuleConfig(
            enabled=True,
            lookback_days=(3,),
            min_reclaimed_poc_count=1,
        ),
    )

    assert plan.entry_allowed is True
    assert plan.stop_loss_price == 10.97
    assert plan.stop_loss_basis == "vp3_value_high"
    assert plan.take_profit_basis == "profile_fallback_pct"
    assert plan.payload["reward_risk"] > 1.2


def test_build_volume_profile_risk_plan_rejects_close_overhead_supply() -> None:
    from rquant.volume_profile import (
        VolumeProfile,
        VolumeProfileRuleConfig,
        build_volume_profile_risk_plan,
    )

    profile = VolumeProfile(
        ts_code="600000.SH",
        reference_date=date(2026, 6, 4),
        lookback_days=30,
        start_date=date(2026, 5, 1),
        end_date=date(2026, 6, 3),
        rows_count=100,
        total_vol=1000,
        total_amount=10000,
        vwap=10.10,
        poc_price=10.00,
        value_area_low=9.80,
        value_area_high=10.40,
        concentration_top5_pct=60,
        below_reference_amount_pct=55,
        above_reference_amount_pct=45,
        below_reference_volume_pct=55,
        above_reference_volume_pct=45,
    )

    plan = build_volume_profile_risk_plan(
        [profile],
        entry_price=10.20,
        config=VolumeProfileRuleConfig(enabled=True, min_reclaimed_poc_count=1),
    )

    assert plan.entry_allowed is False
    assert plan.reject_reason == "overhead_resistance_too_close"


def _sample_volume_profile():
    from rquant.volume_profile import VolumeProfile

    return VolumeProfile(
        ts_code="600000.SH",
        reference_date=date(2026, 6, 24),
        lookback_days=30,
        start_date=date(2026, 5, 1),
        end_date=date(2026, 6, 23),
        rows_count=100,
        total_vol=1000.0,
        total_amount=10000.0,
        vwap=10.0,
        poc_price=9.8,
        value_area_low=9.5,
        value_area_high=10.5,
        concentration_top5_pct=60.0,
        below_reference_amount_pct=55.0,
        above_reference_amount_pct=45.0,
        below_reference_volume_pct=65.0,
        above_reference_volume_pct=35.0,
    )


@pytest.mark.parametrize("ratio", [0.5, 2.0])
def test_scale_volume_profile_scales_price_and_share_basis_inversely(
    ratio: float,
) -> None:
    from rquant.volume_profile import scale_volume_profile

    profile = _sample_volume_profile()
    target_date = date(2026, 6, 25)

    scaled = scale_volume_profile(
        profile,
        ratio,
        target_reference_date=target_date,
    )

    assert scaled.reference_date == target_date
    assert scaled.vwap == pytest.approx(profile.vwap * ratio)
    assert scaled.poc_price == pytest.approx(profile.poc_price * ratio)
    assert scaled.value_area_low == pytest.approx(profile.value_area_low * ratio)
    assert scaled.value_area_high == pytest.approx(profile.value_area_high * ratio)
    assert scaled.total_vol == pytest.approx(profile.total_vol / ratio)
    assert scaled.vwap * scaled.total_vol == pytest.approx(
        profile.vwap * profile.total_vol
    )
    assert scaled.total_amount == profile.total_amount
    assert scaled.concentration_top5_pct == profile.concentration_top5_pct
    assert scaled.below_reference_amount_pct == profile.below_reference_amount_pct
    assert scaled.above_reference_amount_pct == profile.above_reference_amount_pct
    assert scaled.below_reference_volume_pct == profile.below_reference_volume_pct
    assert scaled.above_reference_volume_pct == profile.above_reference_volume_pct


def test_scale_volume_profile_ratio_one_can_update_reference_date() -> None:
    from rquant.volume_profile import scale_volume_profile

    profile = _sample_volume_profile()

    scaled = scale_volume_profile(
        profile,
        1.0,
        target_reference_date=date(2026, 6, 25),
    )

    assert scaled.reference_date == date(2026, 6, 25)
    assert scaled.total_vol == profile.total_vol
    assert scaled.vwap == profile.vwap


@pytest.mark.parametrize("ratio", [0.0, -0.5])
def test_scale_volume_profile_rejects_non_positive_ratio(ratio: float) -> None:
    from rquant.volume_profile import scale_volume_profile

    with pytest.raises(ValueError, match="ratio"):
        scale_volume_profile(_sample_volume_profile(), ratio)


def _bins(rows: list[tuple[float, float]]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["price_bin", "weight"])


def _legacy_value_area(
    bins: pd.DataFrame,
    *,
    weight_column: str,
    ratio: float = 0.70,
) -> tuple[float, float]:
    """G20 修复前的实现：按成交量降序累加到 ratio，再取被选中 bin 的 min/max。"""
    ranked = bins.sort_values(weight_column, ascending=False).reset_index(drop=True)
    threshold = bins[weight_column].sum() * ratio
    selected: list[float] = []
    weight_sum = 0.0
    for _, row in ranked.iterrows():
        selected.append(float(row["price_bin"]))
        weight_sum += float(row[weight_column])
        if weight_sum >= threshold:
            break
    return min(selected), max(selected)


def _bimodal_bins() -> pd.DataFrame:
    """双峰：主峰 10.00–10.03 占 72.6%，次峰 10.20–10.22，中间是低成交谷。"""
    rows: list[tuple[float, float]] = [
        (10.00, 200.0),
        (10.01, 600.0),
        (10.02, 250.0),
        (10.03, 150.0),
    ]
    rows += [(round(10.04 + i * 0.01, 2), 2.0) for i in range(16)]
    rows += [(10.20, 100.0), (10.21, 220.0), (10.22, 100.0)]
    return _bins(rows)


def test_value_area_stays_contiguous_on_bimodal_distribution() -> None:
    from rquant.volume_profile import _value_area

    bins = _bimodal_bins()

    value_low, value_high = _value_area(bins, weight_column="weight")

    assert (value_low, value_high) == (10.00, 10.03)
    covered = bins.loc[
        bins["price_bin"].between(value_low, value_high), "weight"
    ].sum()
    assert covered >= bins["weight"].sum() * 0.70
    # 价值区不得越过中间的低成交谷去够次峰
    assert value_high < 10.20


def test_value_area_bimodal_is_much_narrower_than_legacy_ranking() -> None:
    from rquant.volume_profile import _value_area

    bins = _bimodal_bins()

    legacy_low, legacy_high = _legacy_value_area(bins, weight_column="weight")
    new_low, new_high = _value_area(bins, weight_column="weight")

    # 旧算法同时选中两个峰，min/max 把中间的谷一并圈进价值区
    assert (legacy_low, legacy_high) == (10.00, 10.21)
    assert legacy_high - legacy_low == pytest.approx(0.21)
    assert new_high - new_low == pytest.approx(0.03)
    assert (new_high - new_low) / (legacy_high - legacy_low) < 0.2


def test_value_area_matches_legacy_on_unimodal_distribution() -> None:
    from rquant.volume_profile import _value_area

    bins = _bins([
        (10.00, 10.0),
        (10.01, 30.0),
        (10.02, 80.0),
        (10.03, 150.0),
        (10.04, 300.0),
        (10.05, 150.0),
        (10.06, 80.0),
        (10.07, 30.0),
        (10.08, 10.0),
    ])

    legacy = _legacy_value_area(bins, weight_column="weight")
    new = _value_area(bins, weight_column="weight")

    assert new == (10.03, 10.05)
    assert new == legacy
    assert abs((new[1] - new[0]) - (legacy[1] - legacy[0])) <= 0.01 + 1e-9


def test_value_area_expands_to_the_only_open_side_at_boundary() -> None:
    from rquant.volume_profile import _value_area

    bins = _bins([(10.00, 300.0), (10.01, 100.0), (10.02, 50.0)])

    assert _value_area(bins, weight_column="weight") == (10.00, 10.01)


def test_value_area_single_bin_returns_that_bin() -> None:
    from rquant.volume_profile import _value_area

    assert _value_area(_bins([(10.00, 500.0)]), weight_column="weight") == (10.00, 10.00)


def test_poc_index_tie_break_prefers_bin_nearest_reference_price() -> None:
    from rquant.volume_profile import _poc_index

    prices = [9.90, 10.00, 10.10]
    weights = [200.0, 50.0, 200.0]

    assert _poc_index(prices, weights, 10.08) == 2
    assert _poc_index(prices, weights, 9.92) == 0
    # 等距并列与无参考价都回落到低价侧，保证可复现
    assert _poc_index(prices, weights, 10.00) == 0
    assert _poc_index(prices, weights, None) == 0


def _seed_two_peak_minutes(store: DuckDBStore, reference_close: float) -> None:
    store.upsert_daily(pd.DataFrame([
        {
            "ts_code": "600000.SH",
            "trade_date": date(2026, 6, day),
            "open": reference_close,
            "high": reference_close,
            "low": reference_close,
            "close": reference_close,
            "pre_close": reference_close,
            "change": 0.0,
            "pct_chg": 0.0,
            "vol": 1,
            "amount": 1,
        }
        for day in [1, 2, 3, 4]
    ]))
    store.upsert_adj_factor(pd.DataFrame([
        {"ts_code": "600000.SH", "trade_date": date(2026, 6, day), "adj_factor": 1.0}
        for day in [1, 2, 3, 4]
    ]))
    store.upsert_minute_bars(pd.DataFrame([
        {
            "ts_code": "600000.SH",
            "trade_time": datetime(2026, 6, day, 9, 30),
            "freq": "1min",
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "vol": 100.0,
            "amount": price * 100.0,
            "source": "tushare",
        }
        for day, price in [(1, 9.0), (2, 11.0)]
    ]))


@pytest.mark.parametrize(
    ("reference_close", "poc_above_ten"),
    [(10.60, True), (9.40, False)],
)
def test_tied_poc_follows_reference_price(
    store: DuckDBStore,
    reference_close: float,
    poc_above_ten: bool,
) -> None:
    from rquant.volume_profile import calculate_volume_profile

    _seed_two_peak_minutes(store, reference_close)

    profile = calculate_volume_profile(
        store,
        "600000.SH",
        reference_date=date(2026, 6, 4),
        lookback_days=3,
    )

    assert profile is not None
    assert (profile.poc_price > 10.0) is poc_above_ten
    assert profile.value_area_low <= profile.poc_price <= profile.value_area_high


@pytest.mark.parametrize(
    ("reference_price", "expected"),
    [(3.0, 0.015), (10.0, 0.05), (300.0, 1.5)],
)
def test_resolve_bin_size_without_bin_ratio_keeps_legacy_formula(
    reference_price: float,
    expected: float,
) -> None:
    from rquant.volume_profile import _resolve_bin_size

    assert _resolve_bin_size(
        reference_price, bin_pct=0.005, bin_ratio=None
    ) == pytest.approx(expected)
    assert _resolve_bin_size(
        reference_price, bin_pct=0.005, bin_ratio=0
    ) == pytest.approx(expected)


def test_resolve_bin_size_scales_with_price_when_bin_ratio_given() -> None:
    from rquant.volume_profile import _resolve_bin_size

    cheap = _resolve_bin_size(3.0, bin_pct=0.005, bin_ratio=0.002)
    mid = _resolve_bin_size(50.0, bin_pct=0.005, bin_ratio=0.002)
    pricey = _resolve_bin_size(300.0, bin_pct=0.005, bin_ratio=0.002)

    assert cheap == pytest.approx(0.01)  # 低价股被 0.01 的最小变动价位托底
    assert mid == pytest.approx(0.10)
    assert pricey == pytest.approx(0.60)
    assert cheap < mid < pricey


def test_volume_profile_rule_config_bin_ratio_defaults_to_none() -> None:
    from rquant.volume_profile import VolumeProfileRuleConfig

    assert VolumeProfileRuleConfig().bin_ratio is None
    assert VolumeProfileRuleConfig(bin_ratio=0.002).bin_ratio == pytest.approx(0.002)


def _seed_high_priced_minutes(store: DuckDBStore) -> None:
    store.upsert_daily(pd.DataFrame([
        {
            "ts_code": "600519.SH",
            "trade_date": date(2026, 6, day),
            "open": 300.0,
            "high": 302.0,
            "low": 300.0,
            "close": 300.0,
            "pre_close": 300.0,
            "change": 0.0,
            "pct_chg": 0.0,
            "vol": 1,
            "amount": 1,
        }
        for day in [1, 2, 3, 4]
    ]))
    store.upsert_adj_factor(pd.DataFrame([
        {"ts_code": "600519.SH", "trade_date": date(2026, 6, day), "adj_factor": 1.0}
        for day in [1, 2, 3, 4]
    ]))
    store.upsert_minute_bars(pd.DataFrame([
        {
            "ts_code": "600519.SH",
            "trade_time": datetime(2026, 6, day, 9, 30),
            "freq": "1min",
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "vol": vol,
            "amount": price * vol,
            "source": "tushare",
        }
        for day, price, vol in [(1, 300.0, 100.0), (2, 300.8, 150.0), (3, 302.0, 120.0)]
    ]))


def test_bin_ratio_splits_bins_that_default_binning_merges(
    store: DuckDBStore,
) -> None:
    from rquant.volume_profile import calculate_volume_profile

    _seed_high_priced_minutes(store)

    default_profile = calculate_volume_profile(
        store,
        "600519.SH",
        reference_date=date(2026, 6, 4),
        lookback_days=3,
    )
    narrow_profile = calculate_volume_profile(
        store,
        "600519.SH",
        reference_date=date(2026, 6, 4),
        lookback_days=3,
        bin_ratio=0.002,
    )

    assert default_profile is not None
    assert narrow_profile is not None
    # 默认 0.5% 分桶把 300.8 与 302.0 并进同一个 1.5 元宽的桶
    assert default_profile.poc_price == pytest.approx(301.5)
    # 0.2% 分桶（0.6 元宽）把它们拆开，POC 落到真正的最大单桶
    assert narrow_profile.poc_price == pytest.approx(300.6)
    assert default_profile.total_vol == narrow_profile.total_vol


def test_default_binning_is_unchanged_by_bin_ratio_support(
    store: DuckDBStore,
) -> None:
    from rquant.volume_profile import calculate_volume_profile

    _seed_profile_data(store)

    baseline = calculate_volume_profile(
        store,
        "600000.SH",
        reference_date=date(2026, 6, 4),
        lookback_days=3,
    )
    explicit_none = calculate_volume_profile(
        store,
        "600000.SH",
        reference_date=date(2026, 6, 4),
        lookback_days=3,
        bin_pct=0.005,
        bin_ratio=None,
    )

    assert baseline is not None
    assert explicit_none is not None
    assert baseline == explicit_none
    # 改动前后的固化结果：ref_price=10 → bin=0.05，bins 为 9.0/10.0/11.0
    assert baseline.poc_price == 11.0
    assert baseline.value_area_low == 10.0
    assert baseline.value_area_high == 11.0
