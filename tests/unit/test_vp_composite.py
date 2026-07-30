"""Composite VP 单元测试（规格 §3.3.5 / §8.2 增量滚动、§2.3 除权重置）。

核心是一条恒等式：**滚动 60 天的结果 == 用同样这 60 天数据一次性重建的结果**。
规格 §8.2 禁止每日全量重算（5400 × 60 × 240 ≈ 7.8 亿行），所以「增量」是唯一
可行实现，而增量最容易悄悄错在挤出旧日那一步——这里用随机 70 天数据把它钉死。
"""

from __future__ import annotations

import random
from collections.abc import Sequence
from datetime import date, datetime, time, timedelta

import pytest

from rquant.vp.engine import (
    CompositeSnapshot,
    CompositeVP,
    CompositeVPState,
    DayHistogram,
    MinuteBar,
    ProfileAccumulator,
    VPBin,
    VPEngineConfig,
    VPProfile,
)

TradingDay = tuple[date, list[MinuteBar]]


@pytest.fixture()
def config() -> VPEngineConfig:
    return VPEngineConfig()


def _trade_dates(count: int, *, start: date = date(2026, 3, 2)) -> list[date]:
    dates: list[date] = []
    cursor = start
    while len(dates) < count:
        if cursor.weekday() < 5:
            dates.append(cursor)
        cursor += timedelta(days=1)
    return dates


def _minutes_at_prices(
    trade_date: date,
    prices_volumes: Sequence[tuple[float, float]],
) -> list[MinuteBar]:
    """每根分钟只成交在一个价位，直方图因此完全可控。"""
    opening = datetime.combine(trade_date, time(9, 30))
    return [
        MinuteBar(
            trade_time=opening + timedelta(minutes=offset),
            high=price,
            low=price,
            close=price,
            volume=volume,
            amount=round(price * volume, 2),
        )
        for offset, (price, volume) in enumerate(prices_volumes)
    ]


def _random_day(
    rng: random.Random,
    trade_date: date,
    *,
    open_price: float,
    bars: int = 24,
) -> tuple[list[MinuteBar], float]:
    """一天的随机分钟线，返回 (分钟集, 收盘价)。价格走随机游走，形态不做任何假设。"""
    opening = datetime.combine(trade_date, time(9, 30))
    price = open_price
    minutes: list[MinuteBar] = []
    for step in range(bars):
        price = max(2.0, price * (1.0 + rng.gauss(0.0, 0.0025)))
        half = price * rng.uniform(0.0005, 0.004)
        low = round(price - half, 2)
        high = max(round(price + half, 2), low)
        close = min(max(round(rng.uniform(low, high), 2), low), high)
        volume = round(rng.uniform(0.0, 5000.0), 1)
        minutes.append(
            MinuteBar(
                trade_time=opening + timedelta(minutes=step),
                high=high,
                low=low,
                close=close,
                volume=volume,
                amount=round(close * volume, 2),
            )
        )
    return minutes, price


def _random_series(rng: random.Random, count: int, *, open_price: float = 10.0) -> list[TradingDay]:
    days: list[TradingDay] = []
    price = open_price
    for trade_date in _trade_dates(count):
        minutes, price = _random_day(rng, trade_date, open_price=price)
        days.append((trade_date, minutes))
    return days


def _rebuild(
    days: Sequence[TradingDay],
    *,
    config: VPEngineConfig,
    reference_price: float,
) -> VPProfile:
    """一次性重建：把窗口内所有分钟灌进一个累加器，不经过任何滚动逻辑。"""
    accumulator = ProfileAccumulator(config, reference_price=reference_price)
    for _, minutes in days:
        for bar in minutes:
            accumulator.add(
                high=bar.high,
                low=bar.low,
                close=bar.close,
                volume=bar.volume,
                amount=bar.amount,
            )
    profile = accumulator.snapshot()
    assert profile is not None
    return profile


def _bins_of(composite: CompositeVP) -> tuple[VPBin, ...]:
    snapshot = composite.snapshot()
    assert snapshot is not None
    return snapshot.profile.bins


def _assert_profiles_match(rolled: VPProfile, rebuilt: VPProfile) -> None:
    """逐 bin 比对。

    bin 集合必须**精确**相等——挤出旧日靠引用计数整桶删除，不是「减完约等于 0」，
    所以不允许出现浮点残渣撑出来的幽灵 bin。
    量值只能到浮点级：滚动是「加 61 天再减 1 天」，重建是「加 60 天」，结合律在
    浮点下不成立，残差量级 ~ eps × 窗口总量。
    """
    assert [item.index for item in rolled.bins] == [item.index for item in rebuilt.bins]
    assert rolled.bin_width == rebuilt.bin_width
    tolerance = 1e-9 * rebuilt.total_volume
    for left, right in zip(rolled.volumes, rebuilt.volumes, strict=True):
        assert abs(left - right) <= tolerance
    assert rolled.total_volume == pytest.approx(rebuilt.total_volume, rel=1e-9)
    assert rolled.total_amount == pytest.approx(rebuilt.total_amount, rel=1e-9)
    assert rolled.bar_count == rebuilt.bar_count
    assert rolled.price_low == rebuilt.price_low
    assert rolled.price_high == rebuilt.price_high
    assert rolled.poc_price == rebuilt.poc_price
    assert (rolled.value_area_low, rolled.value_area_high) == (
        rebuilt.value_area_low,
        rebuilt.value_area_high,
    )


# ── 增量正确性金标准 ──────────────────────────────────────────────────


def test_rolling_window_matches_one_shot_rebuild(config: VPEngineConfig) -> None:
    """随机 70 天逐日 push，第 60~70 天的快照与「对应 60 天一次性重建」逐 bin 一致。"""
    rng = random.Random(20260729)
    days = _random_series(rng, 70)
    window = config.composite_window

    composite = CompositeVP(config)
    checked = 0
    for index, (trade_date, minutes) in enumerate(days):
        composite.push_day(trade_date, minutes)
        if index + 1 < window:
            continue
        snapshot = composite.snapshot()
        assert snapshot is not None
        assert snapshot.days_count == window
        assert snapshot.first_date == days[index - window + 1][0]
        assert snapshot.last_date == trade_date

        anchor = composite.reference_price
        assert anchor is not None
        rebuilt = _rebuild(
            days[index - window + 1 : index + 1],
            config=config,
            # 网格锚在窗口首日，重建必须用同一把尺子——否则比的是两套分桶
            reference_price=anchor,
        )
        _assert_profiles_match(snapshot.profile, rebuilt)
        checked += 1

    assert checked == 11  # 第 60 天（刚满窗）+ 第 61~70 天（每天挤出一天）


def test_reference_price_is_anchored_on_first_day(config: VPEngineConfig) -> None:
    composite = CompositeVP(config)
    composite.push_day(date(2026, 7, 1), _minutes_at_prices(date(2026, 7, 1), [(10.00, 100.0)]))
    composite.push_day(date(2026, 7, 2), _minutes_at_prices(date(2026, 7, 2), [(99.00, 100.0)]))

    # 参考价随窗口滚动而变 → bin 宽度跟着变 → 历史 POC 被今天的数据改写（未来函数）
    assert composite.reference_price == 10.00
    assert composite.bin_width == 0.02
    snapshot = composite.snapshot()
    assert snapshot is not None
    assert snapshot.profile.bin_width == 0.02


def test_window_evicts_oldest_day_entirely(config: VPEngineConfig) -> None:
    """挤出的那天独占的 bin 必须整桶消失，不留浮点残渣撑出的幽灵 bin。"""
    small = VPEngineConfig(composite_window=3)
    dates = _trade_dates(4)

    composite = CompositeVP(small)
    composite.push_day(dates[0], _minutes_at_prices(dates[0], [(10.00, 123.456)]))
    for trade_date in dates[1:]:
        composite.push_day(trade_date, _minutes_at_prices(trade_date, [(10.20, 77.7)]))

    assert composite.days_count == 3
    assert composite.first_date == dates[1]
    snapshot = composite.snapshot()
    assert snapshot is not None
    # floor(10.00 / 0.02) = 500 那个桶只有首日贡献过，挤出后整个消失
    assert [item.index for item in snapshot.profile.bins] == [510]
    assert snapshot.profile.total_volume == pytest.approx(77.7 * 3)
    assert snapshot.profile.bar_count == 3


def test_evicting_a_multi_day_bin_leaves_no_floating_point_ghost() -> None:
    """两天喂同一个 bin、两天都被挤出后，该 bin 必须整桶消失。

    这是「减完约等于 0 就当没有」那种写法唯一会露馅的地方，也是本模块最阴的坑：
    0.1 + 0.2 - 0.1 - 0.2 == 2.78e-17，**是正数**，而 `build_profile` 只过滤
    `volume > 0`，于是残渣凭空撑出一个 bin，把稠密区间从 [510] 拉成 [500, 510]，
    POC 不变但 LVN 段宽、价值区边界全跟着歪。靠引用计数整桶删除才是精确的。
    """
    small = VPEngineConfig(composite_window=2)
    dates = _trade_dates(4)

    composite = CompositeVP(small)
    composite.push_day(dates[0], _minutes_at_prices(dates[0], [(10.00, 0.1)]))
    composite.push_day(dates[1], _minutes_at_prices(dates[1], [(10.00, 0.2), (10.20, 1000.0)]))
    composite.push_day(dates[2], _minutes_at_prices(dates[2], [(10.20, 1000.0)]))
    # 此刻窗口 = [dates[1], dates[2]]，bin 500 上还剩 dates[1] 的 0.2（引用计数 1）
    assert 500 in {item.index for item in _bins_of(composite)}

    composite.push_day(dates[3], _minutes_at_prices(dates[3], [(10.20, 1000.0)]))

    # dates[1] 被挤出 → bin 500 引用计数归零 → 整桶删除，不留 2.78e-17 的幽灵
    assert [item.index for item in _bins_of(composite)] == [510]


def test_zero_volume_day_still_occupies_a_window_slot(config: VPEngineConfig) -> None:
    small = VPEngineConfig(composite_window=2)
    dates = _trade_dates(3)

    composite = CompositeVP(small, reference_price=10.0)
    composite.push_day(dates[0], _minutes_at_prices(dates[0], [(10.00, 500.0)]))
    histogram = composite.push_day(dates[1], _minutes_at_prices(dates[1], [(10.00, 0.0)]))

    assert histogram.counts == {}
    assert histogram.bar_count == 1
    assert composite.days_count == 2

    composite.push_day(dates[2], _minutes_at_prices(dates[2], [(10.04, 300.0)]))
    # 零成交那天照样占坑：首日被挤掉，窗口里只剩「零成交日 + 第三天」
    assert composite.first_date == dates[1]
    snapshot = composite.snapshot()
    assert snapshot is not None
    assert snapshot.profile.total_volume == pytest.approx(300.0)
    assert snapshot.profile.bar_count == 2


# ── 网格扩展 ──────────────────────────────────────────────────────────


def test_grid_extends_without_rebinning_history(config: VPEngineConfig) -> None:
    """价格漂 30%：旧 bin 的 index 与量值一个字节不动，新 bin 落在同一把刻度尺上。

    bin index = floor(price / bin_width)，锚点是价格 0——网格是无限刻度尺而不是
    有界数组，所以「向两端扩展」不需要任何重分桶。
    """
    dates = _trade_dates(3)
    composite = CompositeVP(config)
    composite.push_day(dates[0], _minutes_at_prices(dates[0], [(10.00, 100.0), (10.02, 300.0)]))

    before = composite.snapshot()
    assert before is not None
    old_bins = {item.index: item.volume for item in before.profile.bins}
    assert old_bins == {500: 100.0, 501: 300.0}

    # 上漂 30%
    composite.push_day(dates[1], _minutes_at_prices(dates[1], [(13.00, 500.0)]))
    # 下漂到首日之下，验证低端也能扩
    composite.push_day(dates[2], _minutes_at_prices(dates[2], [(9.00, 50.0)]))

    after = composite.snapshot()
    assert after is not None
    assert after.profile.bin_width == before.profile.bin_width == 0.02
    new_bins = {item.index: item.volume for item in after.profile.bins}
    for index, volume in old_bins.items():
        assert new_bins[index] == volume
    assert new_bins[650] == 500.0  # floor(13.00 / 0.02)
    assert new_bins[450] == 50.0  # floor(9.00 / 0.02)
    # 稠密连续：中间空档补 0，不是稀疏跳跃（§3.3.3 的「连续 bin 段」靠这个成立）
    assert [item.index for item in after.profile.bins] == list(range(450, 651))
    assert sum(1 for volume in new_bins.values() if volume == 0.0) == 197


def test_grid_extension_still_matches_rebuild(config: VPEngineConfig) -> None:
    dates = _trade_dates(3)
    days: list[TradingDay] = [
        (dates[0], _minutes_at_prices(dates[0], [(10.00, 100.0), (10.02, 300.0)])),
        (dates[1], _minutes_at_prices(dates[1], [(13.00, 500.0), (13.04, 200.0)])),
        (dates[2], _minutes_at_prices(dates[2], [(9.00, 50.0)])),
    ]

    composite = CompositeVP(config)
    for trade_date, minutes in days:
        composite.push_day(trade_date, minutes)

    snapshot = composite.snapshot()
    assert snapshot is not None
    _assert_profiles_match(snapshot.profile, _rebuild(days, config=config, reference_price=10.0))


# ── 除权重置（§2.3） ──────────────────────────────────────────────────


def test_reset_clears_state_and_records_reason(config: VPEngineConfig) -> None:
    dates = _trade_dates(4)
    composite = CompositeVP(config)
    for trade_date in dates[:3]:
        composite.push_day(trade_date, _minutes_at_prices(trade_date, [(10.00, 100.0)]))

    composite.reset("除权除息 10 送 10")

    assert composite.days_count == 0
    assert composite.total_volume == 0.0
    assert composite.bar_count == 0
    assert composite.day_histograms == ()
    assert composite.first_date is None
    assert composite.snapshot() is None
    # 参考价一并清空：价格腰斩后沿用旧 bin 宽度等于把分辨率砍半
    assert composite.reference_price is None
    assert composite.bin_width is None
    assert composite.reset_reason == "除权除息 10 送 10"
    assert composite.reset_at == dates[2]  # 缺省取最后入窗日


def test_reset_reanchors_grid_on_next_day(config: VPEngineConfig) -> None:
    dates = _trade_dates(2)
    composite = CompositeVP(config)
    composite.push_day(dates[0], _minutes_at_prices(dates[0], [(10.00, 100.0)]))

    composite.reset("除权除息 10 送 10", as_of=dates[1])
    composite.push_day(dates[1], _minutes_at_prices(dates[1], [(5.00, 400.0)]))

    snapshot = composite.snapshot()
    assert snapshot is not None
    assert snapshot.days_count == 1
    assert snapshot.profile.reference_price == 5.00
    assert snapshot.profile.bin_width == 0.01  # max(0.01, round(5.00 × 0.002, 2))
    # 除权前的分布作废，一分钱都不留
    assert snapshot.profile.total_volume == pytest.approx(400.0)
    assert snapshot.reset_at == dates[1]
    assert snapshot.reset_reason == "除权除息 10 送 10"


def test_reset_accepts_explicit_reference_price(config: VPEngineConfig) -> None:
    composite = CompositeVP(config, reference_price=10.0)
    composite.reset("除权", reference_price=5.0)

    assert composite.reference_price == 5.0
    assert composite.bin_width == 0.01


def test_reset_requires_a_reason(config: VPEngineConfig) -> None:
    composite = CompositeVP(config)
    with pytest.raises(ValueError, match="原因"):
        composite.reset("   ")


# ── 窗口未满 ──────────────────────────────────────────────────────────


def test_snapshot_available_before_window_is_full(config: VPEngineConfig) -> None:
    dates = _trade_dates(3)
    composite = CompositeVP(config)
    for trade_date in dates:
        composite.push_day(trade_date, _minutes_at_prices(trade_date, [(10.00, 100.0)]))

    snapshot = composite.snapshot()
    assert isinstance(snapshot, CompositeSnapshot)
    # 照常给快照，由 days_count 说明窗口只堆了 3 天；要不要因此不下单是策略层的判断
    assert (snapshot.days_count, snapshot.window) == (3, 60)
    assert snapshot.is_full_window is False
    assert snapshot.poc_price == 10.01
    assert (snapshot.value_area_low, snapshot.value_area_high) == (10.01, 10.01)


def test_snapshot_is_none_before_any_day(config: VPEngineConfig) -> None:
    assert CompositeVP(config).snapshot() is None


# ── 序列化 ────────────────────────────────────────────────────────────


def test_state_round_trip_is_lossless(config: VPEngineConfig) -> None:
    rng = random.Random(7)
    days = _random_series(rng, 5)
    composite = CompositeVP.from_days(days, config=config)

    state = composite.to_state()
    restored = CompositeVP.from_state(state)

    assert restored.to_state() == state
    assert restored.days_count == composite.days_count
    assert restored.reference_price == composite.reference_price
    assert restored.bin_width == composite.bin_width

    left = composite.snapshot()
    right = restored.snapshot()
    assert left is not None and right is not None
    _assert_profiles_match(right.profile, left.profile)


def test_state_survives_json_round_trip(config: VPEngineConfig) -> None:
    """落库路径：dict 的 int 键在 JSON 里会变成字符串，还原后必须仍是 int。"""
    dates = _trade_dates(3)
    composite = CompositeVP(config)
    for trade_date in dates[:2]:
        composite.push_day(trade_date, _minutes_at_prices(trade_date, [(10.00, 100.0)]))
    composite.reset("除权", as_of=dates[1])
    composite.push_day(dates[2], _minutes_at_prices(dates[2], [(5.0, 10.0)]))

    payload = composite.to_state().model_dump(mode="json")
    assert isinstance(payload["days"][0]["counts"], dict)
    assert all(isinstance(key, str) for key in payload["days"][0]["counts"])

    revived = CompositeVPState.model_validate(payload)
    assert revived == composite.to_state()
    assert all(isinstance(key, int) for key in revived.days[0].counts)
    assert revived.reset_reason == "除权"
    assert revived.reset_at == dates[1]


def test_restored_state_keeps_rolling_correctly(config: VPEngineConfig) -> None:
    """重启不能改变滚动结果：恢复后继续推日，仍与一次性重建一致。"""
    small = VPEngineConfig(composite_window=3)
    rng = random.Random(99)
    days = _random_series(rng, 6)

    composite = CompositeVP.from_days(days[:4], config=small)
    restored = CompositeVP.from_state(composite.to_state())
    for trade_date, minutes in days[4:]:
        restored.push_day(trade_date, minutes)

    anchor = restored.reference_price
    assert anchor is not None
    snapshot = restored.snapshot()
    assert snapshot is not None
    assert snapshot.days_count == 3
    _assert_profiles_match(
        snapshot.profile,
        _rebuild(days[3:], config=small, reference_price=anchor),
    )


def test_from_state_rejects_foreign_version(config: VPEngineConfig) -> None:
    state = CompositeVP(config).to_state()
    with pytest.raises(ValueError, match="状态版本"):
        CompositeVP.from_state(state.model_copy(update={"state_version": 99}))


def test_state_rejects_mixed_grid_widths(config: VPEngineConfig) -> None:
    dates = _trade_dates(2)
    with pytest.raises(ValueError, match="网格宽度"):
        CompositeVPState(
            config=config,
            reference_price=10.0,
            bin_width=0.02,
            days=(
                DayHistogram(
                    trade_date=dates[0],
                    bin_width=0.02,
                    counts={500: 100.0},
                    bar_count=1,
                    price_low=10.0,
                    price_high=10.0,
                ),
                DayHistogram(
                    trade_date=dates[1],
                    bin_width=0.05,
                    counts={200: 100.0},
                    bar_count=1,
                    price_low=10.0,
                    price_high=10.0,
                ),
            ),
        )


# ── 输入校验 ──────────────────────────────────────────────────────────


def test_push_day_rejects_out_of_order_dates(config: VPEngineConfig) -> None:
    dates = _trade_dates(2)
    composite = CompositeVP(config)
    composite.push_day(dates[1], _minutes_at_prices(dates[1], [(10.0, 100.0)]))

    with pytest.raises(ValueError, match="乱序"):
        composite.push_day(dates[0], _minutes_at_prices(dates[0], [(10.0, 100.0)]))
    with pytest.raises(ValueError, match="乱序"):
        composite.push_day(dates[1], _minutes_at_prices(dates[1], [(10.0, 100.0)]))


def test_push_day_rejects_empty_and_foreign_minutes(config: VPEngineConfig) -> None:
    dates = _trade_dates(2)
    composite = CompositeVP(config)

    with pytest.raises(ValueError, match="没有分钟数据"):
        composite.push_day(dates[0], [])
    with pytest.raises(ValueError, match="其他交易日"):
        composite.push_day(dates[0], _minutes_at_prices(dates[1], [(10.0, 100.0)]))


def test_push_day_rejects_duplicate_timestamps(config: VPEngineConfig) -> None:
    trade_date = _trade_dates(1)[0]
    minutes = _minutes_at_prices(trade_date, [(10.0, 100.0)])
    composite = CompositeVP(config)

    with pytest.raises(ValueError, match="重复时间戳"):
        composite.push_day(trade_date, [*minutes, *minutes])


def test_push_day_is_order_insensitive_within_a_day(config: VPEngineConfig) -> None:
    trade_date = _trade_dates(1)[0]
    minutes = _minutes_at_prices(trade_date, [(10.00, 100.0), (10.02, 300.0), (10.04, 50.0)])

    forward = CompositeVP(config, reference_price=10.0)
    forward.push_day(trade_date, minutes)
    backward = CompositeVP(config, reference_price=10.0)
    backward.push_day(trade_date, list(reversed(minutes)))

    left = forward.snapshot()
    right = backward.snapshot()
    assert left is not None and right is not None
    assert left.profile == right.profile


def test_push_histogram_requires_matching_grid(config: VPEngineConfig) -> None:
    dates = _trade_dates(2)
    composite = CompositeVP(config, reference_price=10.0)
    histogram = DayHistogram(
        trade_date=dates[0],
        bin_width=0.05,
        counts={200: 100.0},
        bar_count=1,
        price_low=10.0,
        price_high=10.0,
    )

    with pytest.raises(ValueError, match="网格宽度"):
        composite.push_histogram(histogram)

    aligned = histogram.model_copy(update={"bin_width": 0.02, "counts": {500: 100.0}})
    composite.push_histogram(aligned)
    assert composite.days_count == 1


def test_push_histogram_requires_anchored_grid(config: VPEngineConfig) -> None:
    composite = CompositeVP(config)
    histogram = DayHistogram(
        trade_date=_trade_dates(1)[0],
        bin_width=0.02,
        counts={500: 100.0},
        bar_count=1,
        price_low=10.0,
        price_high=10.0,
    )

    with pytest.raises(ValueError, match="网格尚未锚定"):
        composite.push_histogram(histogram)
