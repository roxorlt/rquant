"""VP 引擎单元测试（规格 docs/vp-strategy/VP-STRATEGY-SPEC.md §3.3）。

三类不变量固化在这里：增量累加与重建一致、算法 U 的量守恒、LVN 三条件各自的否决
路径。POC / 价值区的期望值全部手算，不从实现里反抄。
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from rquant.vp.engine import (
    DailyBar,
    MinuteBar,
    SessionVP,
    VPEngineConfig,
    VPProfile,
    allocate_uniform,
    anchored_profile_from_daily,
    anchored_profile_from_minutes,
    build_profile,
    detect_hvn,
    detect_lvn,
    detect_nodes,
    resolve_anchor_date,
    resolve_bin_width,
    select_anchor_date,
)


@pytest.fixture()
def config() -> VPEngineConfig:
    return VPEngineConfig()


def _minute(
    index: int,
    *,
    low: float,
    high: float,
    close: float,
    volume: float,
    amount: float = 0.0,
) -> MinuteBar:
    return MinuteBar(
        trade_time=datetime(2026, 7, 1, 9, 30) + timedelta(minutes=index),
        high=high,
        low=low,
        close=close,
        volume=volume,
        amount=amount,
    )


def _point_minutes(prices_volumes: Sequence[tuple[float, float]]) -> list[MinuteBar]:
    """每根分钟只成交在一个价位，直方图因此完全可控。"""
    return [
        _minute(index, low=price, high=price, close=price, volume=volume)
        for index, (price, volume) in enumerate(prices_volumes)
    ]


def _profile_from_volumes(
    volumes: Sequence[float],
    *,
    config: VPEngineConfig,
    first_index: int = 500,
    bin_width: float = 0.02,
    reference_price: float = 10.0,
) -> VPProfile:
    counts = {first_index + offset: volume for offset, volume in enumerate(volumes)}
    profile = build_profile(
        counts,
        bin_width=bin_width,
        reference_price=reference_price,
        total_amount=0.0,
        bar_count=len(volumes),
        price_low=reference_price,
        price_high=reference_price,
        config=config,
    )
    assert profile is not None
    return profile


# ── 分桶 ──────────────────────────────────────────────────────────────


def test_bin_width_follows_spec_formula(config: VPEngineConfig) -> None:
    # 规格 §3.3.1：bin_width = max(0.01, round(ref_price × bin_ratio, 2))
    assert resolve_bin_width(10.0, bin_ratio=config.bin_ratio) == 0.02
    assert resolve_bin_width(100.0, bin_ratio=config.bin_ratio) == 0.2
    # 3 元股按比例只有 0.006，被 0.01 的最小报价单位托底
    assert resolve_bin_width(3.0, bin_ratio=config.bin_ratio) == 0.01


def test_bin_ratio_comes_from_config_not_hardcoded() -> None:
    loose = VPEngineConfig(bin_ratio=0.01)
    assert resolve_bin_width(10.0, bin_ratio=loose.bin_ratio) == 0.1


def test_va_pct_is_pinned_by_spec() -> None:
    # 规格 §10.1 把 va_pct 列为固定值：允许它被调 = 允许调宽价值区让回测好看
    with pytest.raises(ValidationError, match="va_pct"):
        VPEngineConfig(va_pct=0.8)


# ── 算法 U ────────────────────────────────────────────────────────────


def test_uniform_allocation_spreads_over_all_covered_bins() -> None:
    shares = allocate_uniform(low=10.00, high=10.05, volume=600.0, bin_width=0.02)

    # [10.00, 10.05] 覆盖 idx 500/501/502 三个桶，均摊
    assert shares == {500: 200.0, 501: 200.0, 502: 200.0}


def test_uniform_allocation_conserves_volume(config: VPEngineConfig) -> None:
    bars = [
        _minute(0, low=9.97, high=10.11, close=10.00, volume=333.0),
        _minute(1, low=10.00, high=10.00, close=10.00, volume=17.0),
        _minute(2, low=9.99, high=10.23, close=10.20, volume=1234.5),
        _minute(3, low=10.05, high=10.06, close=10.06, volume=7.0),
    ]
    session = SessionVP(config)
    session.update_many(bars)

    profile = session.snapshot()
    assert profile is not None
    expected = sum(bar.volume for bar in bars)
    assert sum(profile.volumes) == pytest.approx(expected, rel=1e-12)
    assert profile.total_volume == pytest.approx(expected, rel=1e-12)


def test_zero_volume_minute_is_counted_but_not_allocated(config: VPEngineConfig) -> None:
    session = SessionVP(config)
    session.update(_minute(0, low=10.0, high=10.0, close=10.0, volume=0.0))
    assert session.snapshot() is None

    session.update(_minute(1, low=10.0, high=10.0, close=10.0, volume=100.0))
    profile = session.snapshot()
    assert profile is not None
    assert profile.bar_count == 2
    assert profile.total_volume == 100.0


# ── Session VP ────────────────────────────────────────────────────────


def test_incremental_update_matches_one_shot_rebuild(config: VPEngineConfig) -> None:
    bars = [
        _minute(index, low=9.98 + index * 0.01, high=10.04 + index * 0.01,
                close=10.00 + index * 0.01, volume=100.0 + index * 7.0)
        for index in range(24)
    ]

    incremental = SessionVP(config)
    for cut in (5, 11, 24):
        incremental.update_many(bars[incremental.bar_count : cut])
        incremental.snapshot()  # 盘中取快照不得影响后续累加

    # 重建即重放：进程重启后从库里读回分钟列表，走的是同一条累加路径
    rebuilt = SessionVP.from_minutes(list(reversed(bars)), config=config)

    left = incremental.snapshot()
    right = rebuilt.snapshot()
    assert left is not None and right is not None
    assert left == right
    # bit 级：逐桶浮点值严格相等，不是 approx
    assert left.volumes == right.volumes
    assert (left.poc_price, left.value_area_low, left.value_area_high) == (
        right.poc_price,
        right.value_area_low,
        right.value_area_high,
    )


def test_snapshot_matches_manual_poc_and_value_area(config: VPEngineConfig) -> None:
    # 单价位分钟：直方图 = {10.01: 100, 10.03: 300, 10.05: 1000, 10.07: 200, 10.09: 400}
    session = SessionVP(config)
    session.update_many(
        _point_minutes(
            [(10.00, 100.0), (10.02, 300.0), (10.04, 1000.0), (10.06, 200.0), (10.08, 400.0)]
        )
    )

    profile = session.snapshot()
    assert profile is not None
    assert profile.bin_width == 0.02
    assert profile.reference_price == 10.00
    assert profile.prices == (10.01, 10.03, 10.05, 10.07, 10.09)
    assert profile.volumes == (100.0, 300.0, 1000.0, 200.0, 400.0)
    # 手算：POC = 1000 那桶 = 10.05；阈值 2000 × 0.70 = 1400
    # 从 POC 出发 → 下侧 300 > 上侧 200，先向下（1300）→ 再比 上侧 200 > 下侧 100，
    # 向上（1500 ≥ 1400）停 → VA = [10.03, 10.07]
    assert profile.poc_price == 10.05
    assert (profile.value_area_low, profile.value_area_high) == (10.03, 10.07)


def test_value_area_stays_contiguous_on_bimodal_profile(config: VPEngineConfig) -> None:
    # 复用 volume_profile 的 _value_area（PR #140 G20 修复版）：价值区必须连续，
    # 不得越过中间的低成交谷去够次峰
    session = SessionVP(config)
    session.update_many(
        _point_minutes(
            [
                (10.00, 200.0),
                (10.02, 600.0),
                (10.04, 250.0),
                (10.06, 2.0),
                (10.08, 2.0),
                (10.10, 2.0),
                (10.12, 100.0),
                (10.14, 220.0),
                (10.16, 100.0),
            ]
        )
    )

    profile = session.snapshot()
    assert profile is not None
    assert profile.poc_price == 10.03
    assert profile.value_area_high < 10.13


def test_session_rejects_out_of_order_minutes(config: VPEngineConfig) -> None:
    session = SessionVP(config)
    session.update(_minute(1, low=10.0, high=10.0, close=10.0, volume=100.0))

    with pytest.raises(ValueError, match="乱序"):
        session.update(_minute(0, low=10.0, high=10.0, close=10.0, volume=100.0))


def test_explicit_reference_price_overrides_first_close(config: VPEngineConfig) -> None:
    session = SessionVP(config, reference_price=100.0)
    session.update(_minute(0, low=99.9, high=100.1, close=100.0, volume=100.0))

    profile = session.snapshot()
    assert profile is not None
    assert profile.bin_width == 0.2


def test_bin_width_frozen_after_first_bar(config: VPEngineConfig) -> None:
    # 参考价一经确定不再变更：否则已累加的桶全部作废
    session = SessionVP(config)
    session.update(_minute(0, low=10.0, high=10.0, close=10.0, volume=100.0))
    session.update(_minute(1, low=99.0, high=99.0, close=99.0, volume=100.0))

    profile = session.snapshot()
    assert profile is not None
    assert profile.bin_width == 0.02
    assert profile.reference_price == 10.0


# ── 分桶一致性（自适应分桶的意义） ────────────────────────────────────


def _scale(value: float, factor: int) -> float:
    return float(Decimal(str(value)) * factor)


def test_profile_shape_invariant_under_price_scaling(config: VPEngineConfig) -> None:
    """同一形态放到 10 倍价位上，直方图形态与 POC/VA 的相对位置完全不变。

    固定 0.01 元分桶做不到这一点——300 元股会被切成三万个桶，POC 退化成噪声。
    """
    shape = [(10.00, 100.0), (10.02, 300.0), (10.04, 1000.0), (10.06, 200.0), (10.08, 400.0)]

    base = SessionVP(config)
    base.update_many(_point_minutes(shape))
    scaled = SessionVP(config)
    scaled.update_many(_point_minutes([(_scale(price, 10), volume) for price, volume in shape]))

    left = base.snapshot()
    right = scaled.snapshot()
    assert left is not None and right is not None

    assert right.bin_width == pytest.approx(left.bin_width * 10)
    assert len(right.bins) == len(left.bins)
    assert right.volumes == left.volumes
    assert right.poc_position == left.poc_position
    assert right.poc_price == pytest.approx(left.poc_price * 10)
    assert right.value_area_low == pytest.approx(left.value_area_low * 10)
    assert right.value_area_high == pytest.approx(left.value_area_high * 10)


# ── HVN / LVN ─────────────────────────────────────────────────────────

# 山-谷-山：两侧 500 / 400 是 HVN（≥ 最大 bin × 0.6 = 300），
# 中间四个 10 ≤ min(500,400) × 0.3 = 120，段宽 4 ≥ 3 → 检出
_MOUNTAIN_VALLEY_MOUNTAIN = [100.0, 500.0, 200.0, 10.0, 10.0, 10.0, 10.0, 200.0, 400.0, 100.0]


def test_hvn_requires_local_max_and_threshold(config: VPEngineConfig) -> None:
    profile = _profile_from_volumes(_MOUNTAIN_VALLEY_MOUNTAIN, config=config)

    hvns = detect_hvn(profile, config=config)

    assert [node.position for node in hvns] == [1, 8]
    assert [node.volume for node in hvns] == [500.0, 400.0]
    # 谷底 10 是局部极大（两侧等高），但远低于最大 bin × 60%，不是 HVN
    assert all(node.volume >= 500.0 * config.hvn_threshold for node in hvns)


def test_lvn_detected_between_two_hvns(config: VPEngineConfig) -> None:
    profile = _profile_from_volumes(_MOUNTAIN_VALLEY_MOUNTAIN, config=config)

    lvns = detect_lvn(profile, config=config)

    assert len(lvns) == 1
    node = lvns[0]
    assert (node.start_position, node.end_position, node.width) == (3, 6, 4)
    assert node.volume_limit == pytest.approx(400.0 * config.lvn_threshold)
    assert node.max_volume <= node.volume_limit
    assert (node.left_hvn.position, node.right_hvn.position) == (1, 8)
    assert node.trough_price == profile.bins[3].price
    assert (node.low_price, node.high_price) == (profile.bins[3].price, profile.bins[6].price)


def test_lvn_rejected_when_segment_too_narrow(config: VPEngineConfig) -> None:
    # 谷只有 2 个 bin < lvn_min_width=3
    profile = _profile_from_volumes(
        [100.0, 500.0, 200.0, 10.0, 10.0, 200.0, 400.0, 100.0], config=config
    )

    assert [node.position for node in detect_hvn(profile, config=config)] == [1, 6]
    assert detect_lvn(profile, config=config) == ()


def test_lvn_rejected_when_valley_not_deep_enough(config: VPEngineConfig) -> None:
    # 谷底 150 > min(500,400) × 0.3 = 120，不够深
    profile = _profile_from_volumes(
        [100.0, 500.0, 200.0, 150.0, 150.0, 150.0, 150.0, 200.0, 400.0, 100.0], config=config
    )

    assert [node.position for node in detect_hvn(profile, config=config)] == [1, 8]
    assert detect_lvn(profile, config=config) == ()


def test_lvn_rejected_without_hvn_on_one_side(config: VPEngineConfig) -> None:
    # 右侧只有一条递减的尾巴，最高只有 20 < 500 × 0.6，没有第二座山
    profile = _profile_from_volumes(
        [100.0, 500.0, 200.0, 10.0, 10.0, 10.0, 10.0, 20.0, 15.0, 10.0], config=config
    )

    assert [node.position for node in detect_hvn(profile, config=config)] == [1]
    assert detect_lvn(profile, config=config) == ()


def test_lvn_thresholds_come_from_config() -> None:
    profile = _profile_from_volumes(
        [100.0, 500.0, 200.0, 150.0, 150.0, 150.0, 150.0, 200.0, 400.0, 100.0],
        config=VPEngineConfig(),
    )

    loosened = VPEngineConfig(lvn_threshold=0.4)
    lvns = detect_lvn(profile, config=loosened)

    # 阈值 400 × 0.4 = 160 > 150，同一份数据在放松阈值下才检出
    assert [(node.start_position, node.end_position) for node in lvns] == [(3, 6)]


def test_detect_nodes_returns_both(config: VPEngineConfig) -> None:
    profile = _profile_from_volumes(_MOUNTAIN_VALLEY_MOUNTAIN, config=config)

    nodes = detect_nodes(profile, config=config)

    assert len(nodes.hvns) == 2
    assert len(nodes.lvns) == 1


# ── Anchored VP ───────────────────────────────────────────────────────


def _trade_dates(count: int) -> list[date]:
    dates: list[date] = []
    cursor = date(2026, 3, 2)
    while len(dates) < count:
        if cursor.weekday() < 5:
            dates.append(cursor)
        cursor += timedelta(days=1)
    return dates


def _daily_series(
    overrides: dict[int, tuple[float, float]] | None = None,
    *,
    count: int = 40,
) -> list[DailyBar]:
    """基线：每日成交额 100、涨幅 0。overrides = {下标: (pct_chg, amount)}。"""
    overrides = overrides or {}
    bars: list[DailyBar] = []
    for index, trade_date in enumerate(_trade_dates(count)):
        pct_chg, amount = overrides.get(index, (0.0, 100.0))
        bars.append(
            DailyBar(
                trade_date=trade_date,
                high=10.5,
                low=9.5,
                close=10.0,
                volume=amount,
                amount=amount,
                pct_chg=pct_chg,
            )
        )
    return bars


def test_anchor_takes_most_recent_not_largest(config: VPEngineConfig) -> None:
    bars = _daily_series({25: (12.0, 500.0), 32: (9.0, 300.0)})
    as_of = bars[-1].trade_date

    # 25 号那天涨幅更大（12% vs 9%），规格要求取最近的一个
    assert select_anchor_date(bars, as_of=as_of, config=config) == bars[32].trade_date


def test_anchor_returns_none_without_qualifying_day(config: VPEngineConfig) -> None:
    bars = _daily_series()
    assert select_anchor_date(bars, as_of=bars[-1].trade_date, config=config) is None

    # 涨幅够但量能不足（300 < 2 × 100 是假的，这里给 150）
    weak_volume = _daily_series({32: (9.0, 150.0)})
    assert select_anchor_date(weak_volume, as_of=weak_volume[-1].trade_date, config=config) is None

    # 量能够但涨幅不足
    weak_gain = _daily_series({32: (6.0, 500.0)})
    assert select_anchor_date(weak_gain, as_of=weak_gain[-1].trade_date, config=config) is None


def test_anchor_ignores_qualifying_day_outside_lookback(config: VPEngineConfig) -> None:
    bars = _daily_series({22: (12.0, 500.0)})
    as_of = bars[-1].trade_date

    # 近 15 个交易日窗口是下标 25..39，22 号那天已经出界
    assert select_anchor_date(bars, as_of=as_of, config=config) is None


def test_anchor_does_not_drift_once_selected(config: VPEngineConfig) -> None:
    bars = _daily_series({25: (12.0, 500.0), 32: (9.0, 300.0)})
    as_of = bars[-1].trade_date
    locked = bars[25].trade_date

    # 32 号那天更近且合格，但锚点已选定 → 不漂移（否则 AVP_POC 每天跳变）
    assert resolve_anchor_date(bars, as_of=as_of, config=config, current_anchor=locked) == locked
    # 退出 L1 后调用方传 None 重选，才会跟随最近的合格日
    assert (
        resolve_anchor_date(bars, as_of=as_of, config=config, current_anchor=None)
        == bars[32].trade_date
    )


def test_anchor_skips_candidate_without_full_average_window(config: VPEngineConfig) -> None:
    # 前置样本不足 anchor_avg_window，avg20 无从计算，跳过而不是猜
    bars = _daily_series({5: (12.0, 500.0)}, count=18)
    assert select_anchor_date(bars, as_of=bars[-1].trade_date, config=config) is None


def test_anchored_profile_from_minutes_starts_at_anchor(config: VPEngineConfig) -> None:
    before = [
        MinuteBar(
            trade_time=datetime(2026, 6, 30, 9, 30),
            high=9.5,
            low=9.5,
            close=9.5,
            volume=9999.0,
        )
    ]
    after = [
        MinuteBar(
            trade_time=datetime(2026, 7, 1, 9, 30 + offset),
            high=10.02,
            low=10.00,
            close=10.00,
            volume=100.0,
        )
        for offset in range(3)
    ]

    profile = anchored_profile_from_minutes(
        [*after, *before],
        anchor_date=date(2026, 7, 1),
        config=config,
    )

    assert profile is not None
    assert profile.total_volume == pytest.approx(300.0)
    assert profile.price_low == 10.00
    assert profile.reference_price == 10.00


def test_anchored_profile_from_daily_starts_at_anchor(config: VPEngineConfig) -> None:
    bars = [
        DailyBar(
            trade_date=date(2026, 6, 29),
            high=9.6,
            low=9.4,
            close=9.5,
            volume=8888.0,
            amount=88880.0,
        ),
        DailyBar(
            trade_date=date(2026, 6, 30),
            high=10.04,
            low=10.00,
            close=10.00,
            volume=300.0,
            amount=3000.0,
        ),
        DailyBar(
            trade_date=date(2026, 7, 1),
            high=10.06,
            low=10.02,
            close=10.04,
            volume=600.0,
            amount=6000.0,
        ),
    ]

    profile = anchored_profile_from_daily(bars, anchor_date=date(2026, 6, 30), config=config)

    assert profile is not None
    assert profile.total_volume == pytest.approx(900.0)
    assert profile.bar_count == 2
    assert profile.price_low == 10.00
    assert profile.price_high == 10.06


def test_anchored_profiles_agree_on_shape_between_inputs(config: VPEngineConfig) -> None:
    """同一段行情，日线输入比分钟输入粗，但两条入口输出同一种模型且口径可比。"""
    minutes = [
        MinuteBar(
            trade_time=datetime(2026, 7, 1, 9, 30 + offset),
            high=10.04,
            low=10.00,
            close=10.02,
            volume=100.0,
        )
        for offset in range(4)
    ]
    daily = [
        DailyBar(
            trade_date=date(2026, 7, 1),
            high=10.04,
            low=10.00,
            close=10.02,
            volume=400.0,
            amount=4000.0,
        )
    ]

    from_minutes = anchored_profile_from_minutes(
        minutes, anchor_date=date(2026, 7, 1), config=config, reference_price=10.0
    )
    from_daily = anchored_profile_from_daily(
        daily, anchor_date=date(2026, 7, 1), config=config, reference_price=10.0
    )

    assert from_minutes is not None and from_daily is not None
    assert from_minutes.bin_width == from_daily.bin_width
    assert from_minutes.prices == from_daily.prices
    assert from_minutes.volumes == pytest.approx(from_daily.volumes)


def test_anchored_profile_returns_none_when_window_empty(config: VPEngineConfig) -> None:
    assert anchored_profile_from_minutes([], anchor_date=date(2026, 7, 1), config=config) is None
    assert anchored_profile_from_daily([], anchor_date=date(2026, 7, 1), config=config) is None


# ── 输入校验 ──────────────────────────────────────────────────────────


def test_minute_bar_rejects_inverted_range() -> None:
    with pytest.raises(ValidationError):
        MinuteBar(
            trade_time=datetime(2026, 7, 1, 9, 30),
            high=10.0,
            low=10.5,
            close=10.2,
            volume=1.0,
        )


def test_daily_bar_rejects_close_outside_range() -> None:
    with pytest.raises(ValidationError):
        DailyBar(
            trade_date=date(2026, 7, 1),
            high=10.0,
            low=9.0,
            close=10.5,
            volume=1.0,
            amount=1.0,
        )
