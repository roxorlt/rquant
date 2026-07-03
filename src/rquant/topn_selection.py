"""触发后按特征评分选择每日 topN。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from math import isfinite, log1p
from typing import Literal

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

FeatureTransform = Literal["log_ratio", "linear", "inverse_linear", "band"]


class TopNComparisonResult(BaseModel):
    """topN 选择对比结果。"""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    summary: pd.DataFrame
    trades: pd.DataFrame


class FeatureScoreTerm(BaseModel):
    """单个特征评分项。"""

    name: str
    group: str
    weight: float
    transform: FeatureTransform
    low: float | None = None
    high: float | None = None
    center: float | None = None
    half_width: float | None = None
    cap: float | None = None
    # 缺失值时该项贡献 0 分，而不是走 transform 的默认值路径。
    # V2 新因子（新股无 250 日历史）必须开，V1 旧项保持原行为以便历史 run 可对比。
    skip_if_missing: bool = False


class EnvGateConfig(BaseModel):
    """市场环境门控：市场级字段（T-1 可知）低于阈值时跳过当日或整体降权。

    温度字段对同一 signal_date 的所有个股取值相同，multiplier>0 的降权
    不改变当日内 topN 排序，只有 multiplier<=0（跳过当日）会改变选中集合。
    字段缺失（trades 未带该列 / 当日无温度数据）时门控自动放行。
    """

    feature: str
    min_value: float
    multiplier: float = 0.0


class FeatureScoreProfile(BaseModel):
    """触发后 topN 排序使用的评分画像。"""

    name: str
    label: str
    terms: tuple[FeatureScoreTerm, ...]
    disabled_groups: tuple[str, ...] = ()
    group_multipliers: dict[str, float] = Field(default_factory=dict)
    env_gate: EnvGateConfig | None = None


def _num(value: object, default: float = 0.0) -> float:
    if value is None or pd.isna(value):
        return default
    out = float(value)
    return out if isfinite(out) else default


def _clip(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, value))


def _score_log_ratio(value: object, cap: float = 5.0) -> float:
    ratio = max(_num(value), 0.0)
    return _clip(log1p(ratio) / log1p(cap))


def _score_linear(value: object, low: float, high: float) -> float:
    numeric = _num(value)
    if high <= low:
        return 0.0
    return _clip((numeric - low) / (high - low))


def _score_band(value: object, center: float, half_width: float) -> float:
    numeric = _num(value, default=center)
    if half_width <= 0:
        return 0.0
    return _clip(1 - abs(numeric - center) / half_width)


BASE_SCORE_TERMS: tuple[FeatureScoreTerm, ...] = (
    FeatureScoreTerm(
        name="signal_rel_amount_same_minute_20d",
        group="intraday",
        weight=15.0,
        transform="log_ratio",
        cap=5.0,
    ),
    FeatureScoreTerm(
        name="signal_rel_cum_amount_asof_20d",
        group="intraday",
        weight=12.0,
        transform="log_ratio",
        cap=5.0,
    ),
    FeatureScoreTerm(
        name="signal_amount_accel_5m",
        group="intraday",
        weight=8.0,
        transform="log_ratio",
        cap=5.0,
    ),
    FeatureScoreTerm(
        name="signal_amount_accel_10m",
        group="intraday",
        weight=5.0,
        transform="log_ratio",
        cap=5.0,
    ),
    FeatureScoreTerm(
        name="accum_obv_change_20d_pct",
        group="accumulation",
        weight=12.0,
        transform="linear",
        low=-30.0,
        high=50.0,
    ),
    FeatureScoreTerm(
        name="accum_ad_flow_20d_pct",
        group="accumulation",
        weight=10.0,
        transform="linear",
        low=-30.0,
        high=50.0,
    ),
    FeatureScoreTerm(
        name="accum_up_down_amount_ratio_20d",
        group="accumulation",
        weight=8.0,
        transform="linear",
        low=0.7,
        high=2.5,
    ),
    FeatureScoreTerm(
        name="accum_heavy_no_drop_days_20d",
        group="accumulation",
        weight=6.0,
        transform="linear",
        low=0.0,
        high=5.0,
    ),
    FeatureScoreTerm(
        name="price_position_90d_pct",
        group="position",
        weight=10.0,
        transform="band",
        center=70.0,
        half_width=45.0,
    ),
    FeatureScoreTerm(
        name="distance_to_high_90d_pct",
        group="position",
        weight=5.0,
        transform="inverse_linear",
        low=0.0,
        high=25.0,
    ),
    FeatureScoreTerm(
        name="market_up_ratio_pct",
        group="market",
        weight=8.0,
        transform="linear",
        low=15.0,
        high=70.0,
    ),
    FeatureScoreTerm(
        name="index_csi1000_pct_chg",
        group="market",
        weight=6.0,
        transform="linear",
        low=-2.0,
        high=3.0,
    ),
)


# 环境门控消费的市场温度列名。replay 明细当前不自带，
# strategy_optimizer 会按 signal_date 从 market_sentiment_daily 补上。
MARKET_TEMPERATURE_FEATURE = "market_above_ma20_ratio_pct"

# 2026H1 上 above_ma20_ratio_pct 的 p30 约 27，取 30 大致门掉最冷的三成交易日。
_ENV_GATE_MIN_ABOVE_MA20_PCT = 30.0

_MA_ALIGNMENT_TERM = FeatureScoreTerm(
    name="ma_alignment",
    group="trend",
    weight=6.0,
    transform="linear",
    low=0.0,
    high=1.0,
    skip_if_missing=True,
)


def _price_percentile_term(*, prefer_low: bool) -> FeatureScoreTerm:
    """250 日价格百分位项：低位反转与高位动量两个方向假设各建一个画像。"""
    return FeatureScoreTerm(
        name="price_percentile_250d",
        group="trend",
        weight=8.0,
        transform="inverse_linear" if prefer_low else "linear",
        low=0.0,
        high=1.0,
        skip_if_missing=True,
    )


def build_score_profile(
    *,
    name: str,
    label: str,
    disabled_groups: tuple[str, ...] = (),
    group_multipliers: dict[str, float] | None = None,
    extra_terms: tuple[FeatureScoreTerm, ...] = (),
    env_gate: EnvGateConfig | None = None,
) -> FeatureScoreProfile:
    """从基础特征项生成一个可比较的评分画像。"""
    multipliers = group_multipliers or {}
    terms: list[FeatureScoreTerm] = []
    for term in (*BASE_SCORE_TERMS, *extra_terms):
        if term.group in disabled_groups:
            continue
        multiplier = multipliers.get(term.group, 1.0)
        terms.append(term.model_copy(update={"weight": term.weight * multiplier}))
    return FeatureScoreProfile(
        name=name,
        label=label,
        terms=tuple(terms),
        disabled_groups=disabled_groups,
        group_multipliers=multipliers,
        env_gate=env_gate,
    )


def default_score_profiles() -> list[FeatureScoreProfile]:
    """内置评分画像，包含基础版、消融版和轻量权重偏置版。"""
    return [
        build_score_profile(name="v1", label="V1 基础评分"),
        build_score_profile(
            name="no_intraday",
            label="消融：不看分时放量",
            disabled_groups=("intraday",),
        ),
        build_score_profile(
            name="no_accumulation",
            label="消融：不看建仓代理",
            disabled_groups=("accumulation",),
        ),
        build_score_profile(
            name="no_position",
            label="消融：不看高低位",
            disabled_groups=("position",),
        ),
        build_score_profile(
            name="no_market",
            label="消融：不看市场环境",
            disabled_groups=("market",),
        ),
        build_score_profile(
            name="intraday_heavy",
            label="偏重分时放量",
            group_multipliers={"intraday": 1.5},
        ),
        build_score_profile(
            name="accumulation_heavy",
            label="偏重建仓代理",
            group_multipliers={"accumulation": 1.4},
        ),
        build_score_profile(
            name="position_heavy",
            label="偏重高低位结构",
            group_multipliers={"position": 1.5},
        ),
        build_score_profile(
            name="v2_low_position",
            label="V2 低位反转：250日低百分位加分",
            extra_terms=(_MA_ALIGNMENT_TERM, _price_percentile_term(prefer_low=True)),
        ),
        build_score_profile(
            name="v2_momentum",
            label="V2 高位动量：250日高百分位加分",
            extra_terms=(_MA_ALIGNMENT_TERM, _price_percentile_term(prefer_low=False)),
        ),
        build_score_profile(
            name="v2_env_gate",
            label="V2 温度门控：T-1 MA20上方占比<30% 跳过当日",
            env_gate=EnvGateConfig(
                feature=MARKET_TEMPERATURE_FEATURE,
                min_value=_ENV_GATE_MIN_ABOVE_MA20_PCT,
                multiplier=0.0,
            ),
        ),
    ]


def available_score_profile_names() -> list[str]:
    """返回可在页面/API 中选择的评分画像名称。"""
    return [profile.name for profile in default_score_profiles()]


def resolve_score_profiles(names: list[str] | tuple[str, ...] | None) -> list[FeatureScoreProfile]:
    """按名称解析评分画像。"""
    profiles = {profile.name: profile for profile in default_score_profiles()}
    selected_names = list(names) if names else ["v1"]
    resolved: list[FeatureScoreProfile] = []
    for name in selected_names:
        if name not in profiles:
            msg = f"未知评分画像: {name}"
            raise ValueError(msg)
        resolved.append(profiles[name])
    return resolved


def _is_missing(value: object) -> bool:
    if value is None or pd.isna(value):
        return True
    return not isfinite(float(value))


def _env_gate_triggered(row: pd.Series, gate: EnvGateConfig) -> bool:
    value = row.get(gate.feature)
    if _is_missing(value):
        return False
    return float(value) < gate.min_value


def _score_term(row: pd.Series, term: FeatureScoreTerm) -> float:
    if term.skip_if_missing and _is_missing(row.get(term.name)):
        return 0.0
    if term.transform == "log_ratio":
        return _score_log_ratio(row.get(term.name), cap=term.cap or 5.0)
    if term.transform == "linear":
        return _score_linear(row.get(term.name), term.low or 0.0, term.high or 1.0)
    if term.transform == "inverse_linear":
        return 1 - _score_linear(row.get(term.name), term.low or 0.0, term.high or 1.0)
    if term.transform == "band":
        return _score_band(row.get(term.name), term.center or 0.0, term.half_width or 1.0)
    return 0.0


def score_feature_terms(
    values: Mapping[str, object],
    terms: Sequence[FeatureScoreTerm],
) -> float:
    """按给定评分项对一组原始因子值求加权总分。

    _score_term 的薄公共入口，供 minute_replay 等画像体系之外的评分器复用，
    避免复制 transform 公式。缺失值行为与画像一致（skip_if_missing 项记 0）。
    """
    row = pd.Series(dict(values), dtype=object)
    return round(sum(term.weight * _score_term(row, term) for term in terms), 4)


def feature_score(row: pd.Series, score_profile: FeatureScoreProfile | None = None) -> float:
    """按评分画像计算特征分。

    这里只用于排序与消融，不代表已经训练出的模型参数。
    """
    profile = score_profile or resolve_score_profiles(["v1"])[0]
    score = sum(term.weight * _score_term(row, term) for term in profile.terms)
    if profile.env_gate is not None and _env_gate_triggered(row, profile.env_gate):
        score *= max(profile.env_gate.multiplier, 0.0)
    return round(score, 4)


def feature_score_v1(row: pd.Series) -> float:
    """兼容旧调用的 V1 特征分。"""
    return feature_score(row, resolve_score_profiles(["v1"])[0])


def add_feature_scores(
    trades: pd.DataFrame,
    score_profile: FeatureScoreProfile | None = None,
) -> pd.DataFrame:
    """给交易明细补 feature_score。"""
    if trades.empty:
        return trades.copy()
    profile = score_profile or resolve_score_profiles(["v1"])[0]
    out = trades.copy()
    out["feature_score"] = out.apply(feature_score, axis=1, score_profile=profile)
    out["score_profile"] = profile.name
    out["score_profile_label"] = profile.label
    if profile.env_gate is not None:
        out["env_gate_triggered"] = out.apply(
            _env_gate_triggered, axis=1, gate=profile.env_gate
        )
    return out


def select_topn_by_feature_score(
    trades: pd.DataFrame,
    *,
    top_n: int,
    group_col: str = "buy_date",
    score_profile: FeatureScoreProfile | None = None,
) -> pd.DataFrame:
    """每个交易日按 feature_score 取 topN。"""
    if trades.empty:
        return trades.copy()
    if top_n < 1:
        msg = "top_n must be >= 1"
        raise ValueError(msg)

    profile = score_profile or resolve_score_profiles(["v1"])[0]
    scored = add_feature_scores(trades, score_profile=profile)
    gate = profile.env_gate
    if gate is not None and gate.multiplier <= 0 and "env_gate_triggered" in scored.columns:
        scored = scored[~scored["env_gate_triggered"].fillna(False)].copy()
    group_key = group_col if group_col in scored.columns else "signal_date"
    ranked = scored.sort_values(
        [group_key, "feature_score", "entry_time", "ts_code"],
        ascending=[True, False, True, True],
    ).copy()
    ranked["feature_rank"] = ranked.groupby(group_key).cumcount() + 1
    selected = ranked[ranked["feature_rank"] <= top_n].copy()
    return selected.sort_values([group_key, "feature_rank"]).reset_index(drop=True)


def _summary_row(
    trades: pd.DataFrame,
    *,
    selection: str,
    score_profile: FeatureScoreProfile,
    all_mean: float | None,
    all_median: float | None,
    all_win_rate: float | None,
) -> dict[str, object]:
    if trades.empty:
        return {
            "score_profile": score_profile.name,
            "score_profile_label": score_profile.label,
            "selection": selection,
            "groups": 0,
            "trades": 0,
            "mean_ret_pct": None,
            "median_ret_pct": None,
            "win_rate_pct": None,
            "best_ret_pct": None,
            "worst_ret_pct": None,
            "gap_stop_rate_pct": None,
            "avg_feature_score": None,
            "mean_delta_vs_all_pct": None,
            "median_delta_vs_all_pct": None,
            "win_delta_vs_all_pct": None,
        }

    group_col = "buy_date" if "buy_date" in trades.columns else "signal_date"
    mean_ret = float(trades["ret_pct"].mean())
    median_ret = float(trades["ret_pct"].median())
    win_rate = float((trades["ret_pct"] > 0).mean() * 100)
    return {
        "score_profile": score_profile.name,
        "score_profile_label": score_profile.label,
        "selection": selection,
        "groups": int(trades[group_col].nunique()) if group_col in trades.columns else None,
        "trades": len(trades),
        "mean_ret_pct": round(mean_ret, 4),
        "median_ret_pct": round(median_ret, 4),
        "win_rate_pct": round(win_rate, 2),
        "best_ret_pct": round(float(trades["ret_pct"].max()), 4),
        "worst_ret_pct": round(float(trades["ret_pct"].min()), 4),
        "gap_stop_rate_pct": round(
            float((trades["exit_reason"] == "gap_stop").mean() * 100),
            2,
        )
        if "exit_reason" in trades.columns
        else None,
        "avg_feature_score": round(
            float(trades.get("feature_score", pd.Series(dtype=float)).mean()),
            4,
        )
        if "feature_score" in trades.columns
        else None,
        "mean_delta_vs_all_pct": round(mean_ret - all_mean, 4)
        if all_mean is not None
        else None,
        "median_delta_vs_all_pct": round(median_ret - all_median, 4)
        if all_median is not None
        else None,
        "win_delta_vs_all_pct": round(win_rate - all_win_rate, 2)
        if all_win_rate is not None
        else None,
    }


def run_topn_comparison(
    trades: pd.DataFrame,
    *,
    top_n_options: list[int] | tuple[int, ...],
    group_col: str = "buy_date",
    score_profiles: list[FeatureScoreProfile] | tuple[FeatureScoreProfile, ...] | None = None,
) -> TopNComparisonResult:
    """比较全量触发和每日 topN 触发后的收益。"""
    profiles = list(score_profiles) if score_profiles else resolve_score_profiles(["v1"])
    rows: list[dict[str, object]] = []
    selected_frames: list[pd.DataFrame] = []

    for profile in profiles:
        scored = add_feature_scores(trades, score_profile=profile)
        if scored.empty:
            rows.append(
                _summary_row(
                    scored,
                    selection="all",
                    score_profile=profile,
                    all_mean=None,
                    all_median=None,
                    all_win_rate=None,
                )
            )
            continue

        all_mean = float(scored["ret_pct"].mean())
        all_median = float(scored["ret_pct"].median())
        all_win_rate = float((scored["ret_pct"] > 0).mean() * 100)
        rows.append(
            _summary_row(
                scored,
                selection="all",
                score_profile=profile,
                all_mean=all_mean,
                all_median=all_median,
                all_win_rate=all_win_rate,
            )
        )
        for top_n in top_n_options:
            selected = select_topn_by_feature_score(
                scored,
                top_n=int(top_n),
                group_col=group_col,
                score_profile=profile,
            )
            selected = selected.copy()
            selected.insert(0, "selection", f"top{int(top_n)}")
            selected_frames.append(selected)
            rows.append(
                _summary_row(
                    selected,
                    selection=f"top{int(top_n)}",
                    score_profile=profile,
                    all_mean=all_mean,
                    all_median=all_median,
                    all_win_rate=all_win_rate,
                )
            )

    selected_trades = (
        pd.concat(selected_frames, ignore_index=True)
        if selected_frames else pd.DataFrame()
    )
    return TopNComparisonResult(summary=pd.DataFrame(rows), trades=selected_trades)
