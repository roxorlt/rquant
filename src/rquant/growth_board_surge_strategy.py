"""科创/创业板盘中放量追击策略回放。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from zoneinfo import ZoneInfo

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

from rquant.board_auction_strength import board_auction_strength
from rquant.growth_eligibility import classify_growth_opening_structure
from rquant.paper import (
    PaperPosition,
    PaperRiskPlan,
    PaperTradeConfig,
    adjust_open_position_price_basis,
    check_position_exit,
    mark_position_to_quote,
    open_position_from_signal,
)
from rquant.pit_visibility import VisibilityQueryScope, query_visible_rows
from rquant.signal_provenance import GROWTH_SURGE_V1, build_signal_factors
from rquant.state.derive import (
    ListingPriceLimitFact,
    _classify_board,
    _historical_limit_pct,
    _price_limit_eligibility,
    _round_half_up,
)
from rquant.stock_features import (
    build_daily_stock_features,
    build_intraday_relative_volume_features,
)
from rquant.storage.duckdb import DuckDBStore
from rquant.strategy_dependencies import query_bound_strategy_eligibility
from rquant.topn_selection import FeatureScoreTerm, score_feature_terms

UNSUPPORTED_ORDER_FLOW_CONDITIONS = (
    "外盘/内盘、今日大单净量缺少可回测的盘中历史订单流数据；"
    "以分钟 tick-rule 近似内外盘、T-1 moneyflow 大单净量代理"
)

# factor_confirm 评分阈值默认档：训练段（2025-04-28~2025-12-31）宽门入场分钟
# growth_surge_b_v1 得分的中位档，见 jobs 回测 growth_backtest.md（2026-07-03）
DEFAULT_GROWTH_FACTOR_SCORE_THRESHOLD = 45.0

# 日线 vol 单位为手，分钟 vol 为股；经典量比分母按每交易日 240 个成交分钟折算
_DAILY_VOL_UNIT_FACTOR = 100.0
_TRADING_MINUTES_PER_DAY = 240.0
SHANGHAI = ZoneInfo("Asia/Shanghai")

# growth_surge_b_v1 权重表：确认层评分（满分 100），与 GROWTH_SURGE_V1_FACTORS
# 命中矩阵同键名。放量三件套沿用 log_ratio（BASE_SCORE_TERMS 同款口径）；
# inner_outer_ratio 按用户口径「内盘越大越强」线性加分；large_net_vol_t1 只看
# 正负号（>0 即接近满分，linear low=0 high=1 手，实质二值化）；250 日低位为
# 已验证方向（inverse_linear）。classic_volume_ratio 与市场温度只观察不计分。
# 全部 skip_if_missing：缺数据按 0 贡献降级而非默认值路径。
GROWTH_SURGE_B_V1_SCORE_TERMS: tuple[FeatureScoreTerm, ...] = (
    FeatureScoreTerm(
        name="rel_cum_amount_asof", group="volume", weight=20.0,
        transform="log_ratio", cap=5.0, skip_if_missing=True,
    ),
    FeatureScoreTerm(
        name="rel_amount_same_minute", group="volume", weight=15.0,
        transform="log_ratio", cap=5.0, skip_if_missing=True,
    ),
    FeatureScoreTerm(
        name="amount_accel_5m", group="volume", weight=5.0,
        transform="log_ratio", cap=5.0, skip_if_missing=True,
    ),
    FeatureScoreTerm(
        name="inner_outer_ratio", group="order_flow", weight=20.0,
        transform="linear", low=1.0, high=2.0, skip_if_missing=True,
    ),
    FeatureScoreTerm(
        name="large_net_vol_t1", group="order_flow", weight=15.0,
        transform="linear", low=0.0, high=1.0, skip_if_missing=True,
    ),
    FeatureScoreTerm(
        name="price_percentile_250d", group="trend", weight=25.0,
        transform="inverse_linear", low=0.0, high=1.0, skip_if_missing=True,
    ),
)


class GrowthBoardSurgeConfig(BaseModel):
    """科创/创业板盘中放量追击 replay 参数。"""

    model_config = ConfigDict(frozen=True)

    freq: str = "1min"
    # 9:30 早入（用户 2026-07-03，v2 验证段确认真增益）：开盘四根均参与爆量判断。
    # 9:30 集合竞价那根只走同刻口径（vs 历史 9:30），accel 因缺前序分钟天然失效，
    # 不与前后连续竞价分钟比较——见 build_intraday_relative_volume_features。
    min_signal_time: time = time(9, 30)
    lookback_days: int = Field(default=20, ge=1, le=90)
    min_hist_days: int = Field(default=10, ge=1, le=90)
    min_cum_amount_ratio: float = Field(default=1.4, gt=0)
    min_same_minute_amount_ratio: float = Field(default=2.0, gt=0)
    min_amount_accel_5m: float = Field(default=2.0, gt=0)
    use_same_minute_surge: bool = True
    use_accel_surge: bool = True
    require_vwap_strength: bool = True
    vwap_buffer_pct: float = Field(default=0.0, ge=0, lt=0.05)
    # 用户三条件（2026-07-03）：量比宽门沿用 min_cum_amount_ratio（成交额口径）；
    # 内盘>外盘为分钟 tick-rule 近似（升=外盘/降=内盘/平=均分，首分钟对比自身
    # open）；大单净量用 T-1 moneyflow_daily.large_net_vol（T 日盘中不可知，防
    # 未来函数，与用户口径「今日大单净量」有 1 个交易日滞后）
    require_inner_outer: bool = False
    min_inner_outer_ratio: float = Field(default=1.0, gt=0)
    require_large_net_vol: bool = False
    min_large_net_vol: float = 0.0
    # 首爆过滤（用户 2026-07-03）：放量当天之前 N 个交易日没放量过 → 只打首次爆量。
    # 日线口径用 daily_basic.volume_ratio（经典量比），前 N 日任一日 ≥ 阈值即非首爆。
    # 数据不足（前 N 日量比缺行）保守视为通过，不误杀。
    require_fresh_surge: bool = False
    fresh_lookback_days: int = Field(default=5, ge=1, le=20)
    fresh_max_prior_volume_ratio: float = Field(default=2.0, gt=0)
    # 不做新股（用户 2026-07-03）：上市不满 N 个交易日过滤。用 daily_bar 里
    # 信号日之前的日线根数近似「已上市交易日数」，不足则跳过。0=关闭。
    min_listing_trading_days: int = Field(default=0, ge=0)
    # 板块集合竞价强度闸门（候选级，用户 2026-07-03）：候选票所在题材（≤T 最近
    # 打点还原成分）当日集合竞价整体强度不达标则跳过。板块高开占比与竞价资金
    # 相对历史双阈值，竞价快照 09:25 已知（无未来函数，口径见 board_auction_strength）。
    require_board_favor: bool = False
    min_board_gap_up_ratio: float = Field(default=0.5, ge=0, le=1)
    min_board_auction_amount_ratio: float = Field(default=1.0, gt=0)
    # 板块竞价额历史比较窗口，与核心爆量因子的 lookback_days 解耦（原先误复用
    # lookback_days，改板块窗口会连带改爆量同刻中位口径）。默认 3：板块资金青睐是
    # 短周期情绪，20 日中位把它摊平；3 日窗口在训练/验证两段都优于 20 日（2026-07-04
    # 实验，见 docs/analysis/2026-07-04-growth-board-window-3d.md），故取 3。
    board_hist_days: int = Field(default=3, ge=1, le=90)
    # factor_confirm：宽门（现有放量过滤）不动，入场加一层 growth_surge_b_v1
    # 加权评分过阈值判定（minute_replay factor_confirm 同款「宽门+评分」模式）
    enable_factor_confirm: bool = False
    factor_score_threshold: float = Field(
        default=DEFAULT_GROWTH_FACTOR_SCORE_THRESHOLD, ge=0
    )
    # 持仓上限 3 日（用户 2026-07-04，验证段确认真增益）：T+1 硬出把赢家砍早了，
    # 放到 3 日让 2-3 日延续涨幅接住，右尾变肥；训练/验证同向抬升（入场不变）。
    # 见 docs/analysis/2026-07-05-growth-exit-structure-hold3-stop5.md。
    max_hold_days: int = Field(default=3, ge=1, le=10)
    price_tol: float = Field(default=0.01, gt=0, lt=1)
    # 单票止损 -5%（用户 2026-07-04）：尾部由隔夜 gap_stop 主导、非止损位，4%→5% 对
    # worst 值中性，与持仓 3 日捆绑上（放宽止损避免多日持仓被日内噪音过早打出）。
    paper: PaperTradeConfig = Field(
        default_factory=lambda: PaperTradeConfig(
            candidate_id="growth_board_surge_v0",
            stop_loss_pct=0.05,
            take_profit_pct=0.08,
            trailing_stop_pct=0.03,
        )
    )

    @property
    def factor_layer_enabled(self) -> bool:
        """任一用户条件闸门或评分确认开启时，才预取静态因子并输出命中矩阵。"""
        return (
            self.enable_factor_confirm
            or self.require_inner_outer
            or self.require_large_net_vol
            or self.require_board_favor
        )


@dataclass(frozen=True)
class GrowthBoardCandidate:
    ts_code: str
    name: str
    trade_date: date
    previous_date: date
    board_type: str
    pre_close: float
    limit_up_price: float


@dataclass(frozen=True)
class _WatchItem:
    ts_code: str
    pool: str
    name: str
    entry_date: date | None
    reference_date: date | None
    limit_up_date: date
    t_close: float | None
    t_high: float | None
    limit_up_price_next: float | None
    stop_weak: float


@dataclass(frozen=True)
class _Quote:
    ts_code: str
    price: float
    low: float
    high: float | None


def _as_date(value: object) -> date:
    if isinstance(value, pd.Timestamp):
        return value.date()
    if isinstance(value, date):
        return value
    if hasattr(value, "date"):
        return value.date()
    if isinstance(value, str):
        return date.fromisoformat(value[:10])
    msg = f"无法转换为日期: {value!r}"
    raise ValueError(msg)


def _as_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    msg = f"无法转换为时间: {value!r}"
    raise ValueError(msg)


def _trading_dates(store: DuckDBStore) -> list[date]:
    df = store._conn.execute(
        """
        SELECT cal_date
        FROM trade_calendar
        WHERE exchange = 'SSE' AND is_open = TRUE
        ORDER BY cal_date
        """
    ).fetchdf()
    return [_as_date(value) for value in df["cal_date"].tolist()]


def _window_trading_dates(
    calendar: list[date],
    start: date,
    max_hold_days: int,
) -> list[date]:
    return [trading_date for trading_date in calendar if trading_date >= start][
        : max_hold_days + 1
    ]


def _previous_trading_date(calendar: list[date], current: date) -> date | None:
    previous = [trading_date for trading_date in calendar if trading_date < current]
    if not previous:
        return None
    return previous[-1]


def _day_bounds(trading_date: date) -> tuple[datetime, datetime]:
    return (
        datetime.combine(trading_date, time(9, 30)),
        datetime.combine(trading_date, time(15, 0)),
    )


def _query_daily_bar(
    store: DuckDBStore,
    ts_code: str,
    trading_date: date,
) -> pd.Series | None:
    df = store._conn.execute(
        """
        SELECT ts_code, trade_date, close, pre_close
        FROM daily_bar
        WHERE ts_code = ?
          AND trade_date = ?
        """,
        [ts_code, trading_date],
    ).fetchdf()
    if df.empty:
        return None
    return df.iloc[0]


def resolve_growth_board_candidates(
    store: DuckDBStore,
    trading_date: date,
    previous_date: date,
    min_signal_time: time = time(9, 30),
    *,
    structural_excluded_codes: set[str] | None = None,
) -> list[GrowthBoardCandidate]:
    """Resolve the opening candidate universe from PIT-visible inputs only."""
    decision_at = datetime.combine(trading_date, min_signal_time, tzinfo=SHANGHAI)
    bound_eligibility = query_bound_strategy_eligibility(
        store,
        strategy_id="growth_board_surge",
        start_date=trading_date,
        end_date=trading_date,
    )
    bound_codes = (
        None
        if bound_eligibility is None
        else {row.ts_code for row in bound_eligibility}
    )
    if bound_codes == set():
        return []
    status = query_visible_rows(
        store,
        "stock_status_daily",
        decision_at,
        scope=VisibilityQueryScope(
            start_date=trading_date,
            end_date=trading_date,
            start_time=datetime.combine(
                trading_date,
                time.min,
                tzinfo=SHANGHAI,
            ),
            end_time=decision_at,
            columns=(
                "ts_code",
                "trade_date",
                "name",
                "is_st",
                "available_at",
                "conflict_reason",
            ),
        ),
    )
    if status.empty:
        return []
    status = status[
        status["conflict_reason"].isna()
        & status["is_st"].notna()
        & ~status["is_st"].astype(bool)
    ].copy()
    status["board_type"] = status["ts_code"].astype(str).map(_classify_board)
    status = status[status["board_type"].isin(("gem", "star"))]
    excluded_codes = structural_excluded_codes
    if excluded_codes is None:
        excluded_codes = {
            fact.ts_code
            for fact in classify_growth_opening_structure(
                store,
                {trading_date: previous_date},
            )
        }
    status = status[~status["ts_code"].astype(str).isin(excluded_codes)]
    if bound_codes is not None:
        status = status[status["ts_code"].astype(str).isin(bound_codes)]
    if status.empty:
        if bound_codes:
            raise ValueError(
                "bound growth-board eligibility has no matching PIT status rows"
            )
        return []

    codes = tuple(sorted(status["ts_code"].astype(str).unique()))
    calendar_rows = store._conn.execute(
        """
        SELECT cal_date
        FROM trade_calendar
        WHERE exchange = 'SSE'
          AND is_open = TRUE
          AND cal_date <= ?
        ORDER BY cal_date DESC
        LIMIT 60
        """,
        [previous_date],
    ).fetchall()
    if not calendar_rows:
        return []
    window_start = min(_as_date(row[0]) for row in calendar_rows)
    daily = query_visible_rows(
        store,
        "daily_bar",
        decision_at,
        scope=VisibilityQueryScope(
            ts_codes=codes,
            start_date=window_start,
            end_date=previous_date,
            columns=("ts_code", "trade_date", "close"),
        ),
    )
    if daily.empty:
        if bound_codes:
            raise ValueError(
                "bound growth-board eligibility has no matching daily history"
            )
        return []
    daily["trade_date"] = pd.to_datetime(daily["trade_date"]).dt.date

    indicators = store._conn.execute(
        """
        SELECT ts_code, ma5, ma10, ma20, ma60
        FROM daily_indicator
        WHERE trade_date = ?
        """,
        [previous_date],
    ).fetchdf()
    indicator_by_code = {
        str(row["ts_code"]): row
        for _, row in indicators.iterrows()
        if str(row["ts_code"]) in codes
    }

    placeholders = ", ".join("?" for _ in codes)
    listing = store._conn.execute(
        f"""
        SELECT basic.ts_code,
               basic.list_date,
               (
                   SELECT CASE WHEN count(*) = 5 THEN max(cal_date) END
                   FROM (
                       SELECT calendar.cal_date
                       FROM trade_calendar AS calendar
                       WHERE calendar.exchange = 'SSE'
                         AND calendar.is_open = TRUE
                         AND calendar.cal_date >= basic.list_date
                       ORDER BY calendar.cal_date
                       LIMIT 5
                   )
               ) AS fifth_listing_trade_date
        FROM stock_basic AS basic
        WHERE basic.ts_code IN ({placeholders})
          AND basic.list_date IS NOT NULL
        """,
        list(codes),
    ).fetchdf()
    listing_by_code = {
        str(row["ts_code"]): row for _, row in listing.iterrows()
    }

    candidates: list[GrowthBoardCandidate] = []
    for _, status_row in status.iterrows():
        ts_code = str(status_row["ts_code"])
        history = daily[daily["ts_code"].astype(str) == ts_code].sort_values(
            "trade_date"
        )
        previous = history[history["trade_date"] == previous_date]
        if previous.empty:
            continue
        pre_close = float(previous.iloc[-1]["close"])
        if pre_close <= 0:
            continue

        indicator = indicator_by_code.get(ts_code)
        if indicator is None or any(
            pd.isna(indicator[column])
            for column in ("ma5", "ma10", "ma20", "ma60")
        ):
            closes = pd.to_numeric(history["close"], errors="coerce").dropna()
            ma_values = [
                closes.tail(window).mean() if len(closes) >= window else pd.NA
                for window in (5, 10, 20, 60)
            ]
        else:
            ma_values = [
                indicator[column]
                for column in ("ma5", "ma10", "ma20", "ma60")
            ]
        if any(pd.isna(value) for value in ma_values):
            continue
        ma5, ma10, ma20, ma60 = [float(value) for value in ma_values]
        if not (ma5 > ma10 > ma20 > ma60):
            continue

        board_type = str(status_row["board_type"])
        listing_row = listing_by_code.get(ts_code)
        if listing_row is None or pd.isna(listing_row["list_date"]):
            continue
        fact = ListingPriceLimitFact(
            list_date=_as_date(listing_row["list_date"]),
            fifth_listing_trade_date=(
                None
                if pd.isna(listing_row["fifth_listing_trade_date"])
                else _as_date(listing_row["fifth_listing_trade_date"])
            ),
        )
        limit_eligible = _price_limit_eligibility(
            [trading_date],
            board_type=board_type,
            fact=fact,
        ).iloc[0]
        if pd.isna(limit_eligible) or not bool(limit_eligible):
            continue
        limit_pct = _historical_limit_pct(False, board_type, trading_date)
        limit_up_price = float(
            _round_half_up(pd.Series([pre_close * (1 + limit_pct)])).iloc[0]
        )
        candidates.append(
            GrowthBoardCandidate(
                ts_code=ts_code,
                name=(
                    ts_code
                    if pd.isna(status_row["name"])
                    else str(status_row["name"]).strip()
                ),
                trade_date=trading_date,
                previous_date=previous_date,
                board_type=board_type,
                pre_close=pre_close,
                limit_up_price=limit_up_price,
            )
        )
    if bound_codes is not None and {
        candidate.ts_code for candidate in candidates
    } != bound_codes:
        raise ValueError(
            "bound growth-board eligibility disagrees with execution inputs"
        )
    return candidates


def _query_candidates(
    store: DuckDBStore,
    trading_date: date,
    previous_date: date,
    min_signal_time: time = time(9, 30),
) -> list[GrowthBoardCandidate]:
    """Compatibility wrapper for older tests and private callers."""
    return resolve_growth_board_candidates(
        store,
        trading_date,
        previous_date,
        min_signal_time,
    )


def _quote_from_close(row: pd.Series) -> _Quote:
    return _Quote(
        ts_code=str(row["ts_code"]),
        price=float(row["close"]),
        low=float(row["low"]),
        high=float(row["high"]) if pd.notna(row["high"]) else None,
    )


def _quote_from_open(row: pd.Series) -> _Quote:
    price = float(row["open"])
    return _Quote(ts_code=str(row["ts_code"]), price=price, low=price, high=price)


def _is_intraday_yiziban(
    day_minutes: pd.DataFrame,
    limit_up_price: float,
    price_tol: float,
) -> bool:
    if day_minutes.empty or limit_up_price <= 0:
        return False
    first = day_minutes.iloc[0]
    first_time = _as_datetime(first["trade_time"]).time()
    if first_time != time(9, 30):
        return False
    prices = [first["open"], first["high"], first["low"], first["close"]]
    if any(pd.isna(value) for value in prices):
        return False
    return all(float(value) >= limit_up_price - price_tol for value in prices)


def _extract_relative_volume_features(
    features: dict[str, float | int | None],
    lookback_days: int,
) -> dict[str, float | int | None]:
    suffix = f"{lookback_days}d"
    return {
        "signal_minute_amount": features.get("signal_minute_amount"),
        "signal_cum_amount_asof": features.get("signal_cum_amount_asof"),
        "hist_same_minute_amount_median": features.get(
            f"hist_same_minute_amount_median_{suffix}"
        ),
        "hist_cum_amount_asof_median": features.get(
            f"hist_cum_amount_asof_median_{suffix}"
        ),
        "signal_rel_amount_same_minute": features.get(
            f"signal_rel_amount_same_minute_{suffix}"
        ),
        "signal_rel_cum_amount_asof": features.get(
            f"signal_rel_cum_amount_asof_{suffix}"
        ),
        "hist_intraday_days": features.get(f"hist_intraday_days_{suffix}"),
        "signal_opening_segment": features.get("signal_opening_segment"),
        "signal_opening_segment_amount": features.get("signal_opening_segment_amount"),
        "signal_amount_accel_5m": features.get("signal_amount_accel_5m"),
        "signal_amount_accel_10m": features.get("signal_amount_accel_10m"),
    }


def _tick_rule_split(
    reference_price: float,
    close: float,
    vol: float,
) -> tuple[float, float]:
    """按 tick-rule 把单分钟成交量归为 (内盘增量, 外盘增量)。

    close > 参照价记外盘（主动买）、< 记内盘（主动卖）、= 均分。
    """
    if vol <= 0:
        return 0.0, 0.0
    if close > reference_price:
        return 0.0, vol
    if close < reference_price:
        return vol, 0.0
    return vol / 2, vol / 2


def _listed_trading_days(
    store: DuckDBStore,
    ts_code: str,
    signal_date: date,
) -> int:
    """信号日之前已有的日线根数 ≈ 已上市交易日数（不做新股用）。"""
    row = store._conn.execute(
        "SELECT COUNT(*) FROM daily_bar WHERE ts_code = ? AND trade_date < ?",
        [ts_code, signal_date],
    ).fetchone()
    return int(row[0]) if row else 0


def _prior_days_had_surge(
    store: DuckDBStore,
    ts_code: str,
    signal_date: date,
    lookback_days: int,
    max_prior_volume_ratio: float,
) -> bool | None:
    """信号日之前 lookback_days 个交易日是否放量过（经典量比口径）。

    返回 True=前期放过量（非首爆）、False=前期都没放量（首爆）、
    None=数据不足（前期量比缺行，判定不了）。用 daily_basic.volume_ratio。
    """
    rows = store._conn.execute(
        """
        SELECT volume_ratio
        FROM daily_basic
        WHERE ts_code = ? AND trade_date < ? AND volume_ratio IS NOT NULL
        ORDER BY trade_date DESC
        LIMIT ?
        """,
        [ts_code, signal_date, lookback_days],
    ).fetchall()
    if len(rows) < lookback_days:
        return None
    return any(float(r[0]) >= max_prior_volume_ratio for r in rows)


def _query_large_net_vol(
    store: DuckDBStore,
    ts_code: str,
    trading_date: date,
) -> float | None:
    row = store._conn.execute(
        """
        SELECT large_net_vol
        FROM moneyflow_daily
        WHERE ts_code = ?
          AND trade_date = ?
        ORDER BY CASE WHEN source = 'tushare' THEN 0 ELSE 1 END
        LIMIT 1
        """,
        [ts_code, trading_date],
    ).fetchone()
    if row is None or row[0] is None:
        return None
    return float(row[0])


def _query_avg_minute_vol_5d(
    store: DuckDBStore,
    ts_code: str,
    as_of: date,
) -> float | None:
    """截至 T-1 的 5 日平均每分钟成交量（股），经典量比分母；不足 5 根日线记缺。"""
    df = store._conn.execute(
        """
        SELECT vol
        FROM daily_bar
        WHERE ts_code = ?
          AND trade_date <= ?
        ORDER BY trade_date DESC
        LIMIT 5
        """,
        [ts_code, as_of],
    ).fetchdf()
    vols = pd.to_numeric(df["vol"], errors="coerce").dropna() if not df.empty else df
    if len(vols) < 5:
        return None
    return float(vols.mean()) * _DAILY_VOL_UNIT_FACTOR / _TRADING_MINUTES_PER_DAY


def _query_above_ma20_ratio(
    store: DuckDBStore,
    trading_date: date,
) -> float | None:
    row = store._conn.execute(
        """
        SELECT above_ma20_ratio_pct
        FROM market_sentiment_daily
        WHERE trade_date = ?
        """,
        [trading_date],
    ).fetchone()
    if row is None or row[0] is None:
        return None
    return float(row[0])


def _prefetch_growth_static_factors(
    store: DuckDBStore,
    *,
    ts_code: str,
    previous_date: date,
) -> dict[str, float | int | None]:
    """factor 层静态因子（T-1 收盘可知），每个候选最多取一次，缺数据记 None。"""
    trend = build_daily_stock_features(store, ts_code, previous_date)
    return {
        "large_net_vol_t1": _query_large_net_vol(store, ts_code, previous_date),
        "price_percentile_250d": trend.get("price_percentile_250d"),
        "market_above_ma20_ratio_pct": _query_above_ma20_ratio(store, previous_date),
    }


def _passes_surge_filter(
    compact_features: dict[str, float | int | None],
    config: GrowthBoardSurgeConfig,
) -> bool:
    hist_days = compact_features["hist_intraday_days"]
    rel_cum = compact_features["signal_rel_cum_amount_asof"]
    rel_same = compact_features["signal_rel_amount_same_minute"]
    accel_5m = compact_features["signal_amount_accel_5m"]
    if hist_days is None or int(hist_days) < config.min_hist_days:
        return False
    if rel_cum is None or float(rel_cum) < config.min_cum_amount_ratio:
        return False
    if not config.use_same_minute_surge and not config.use_accel_surge:
        return True
    has_same_minute_surge = bool(
        config.use_same_minute_surge
        and rel_same is not None
        and float(rel_same) >= config.min_same_minute_amount_ratio
    )
    has_accel_surge = bool(
        config.use_accel_surge
        and accel_5m is not None
        and float(accel_5m) >= config.min_amount_accel_5m
    )
    return has_same_minute_surge or has_accel_surge


def _previous_window_date(window_dates: list[date], trading_date: date) -> date | None:
    try:
        idx = window_dates.index(trading_date)
    except ValueError:
        return None
    if idx <= 0:
        return None
    return window_dates[idx - 1]


def _close_position(
    position: PaperPosition,
    exit_time: datetime,
    exit_price: float,
    exit_reason: str,
    holding_trading_days: int,
) -> PaperPosition:
    pnl_pct = round((exit_price / position.entry_price - 1) * 100, 4)
    return position.model_copy(update={
        "status": "closed",
        "exit_time": exit_time,
        "exit_price": exit_price,
        "exit_reason": exit_reason,
        "holding_trading_days": holding_trading_days,
        "pnl_pct": pnl_pct,
    })


def _run_exit_scan(
    store: DuckDBStore,
    position: PaperPosition,
    minutes: pd.DataFrame,
    window_dates: list[date],
    config: GrowthBoardSurgeConfig,
) -> PaperPosition | None:
    if minutes.empty:
        return None
    active_date = position.entry_time.date()
    for _, row in minutes.sort_values("trade_time").iterrows():
        quote_time = _as_datetime(row["trade_time"])
        if quote_time < position.entry_time:
            continue
        quote_date = quote_time.date()
        if quote_date != active_date:
            previous_date = _previous_window_date(window_dates, quote_date)
            previous_daily = (
                _query_daily_bar(store, position.ts_code, previous_date)
                if previous_date is not None
                else None
            )
            current_daily = _query_daily_bar(store, position.ts_code, quote_date)
            if (
                previous_daily is not None
                and current_daily is not None
                and pd.notna(previous_daily["close"])
                and pd.notna(current_daily["pre_close"])
            ):
                position = adjust_open_position_price_basis(
                    position,
                    float(previous_daily["close"]),
                    float(current_daily["pre_close"]),
                )
            active_date = quote_date

        quote = _quote_from_close(row)
        exit_event = check_position_exit(position, quote, quote_time, config.paper)
        if exit_event is None:
            position = mark_position_to_quote(position, quote)
            continue

        holding_days = sum(
            1
            for trading_date in window_dates
            if position.trade_date < trading_date <= exit_event.exit_time.date()
        )
        return _close_position(
            position,
            exit_event.exit_time,
            exit_event.exit_price,
            exit_event.exit_reason,
            holding_days,
        )

    last = minutes.sort_values("trade_time").iloc[-1]
    last_time = _as_datetime(last["trade_time"])
    if last_time.date() < position.earliest_exit_date:
        return None
    holding_days = sum(
        1
        for trading_date in window_dates
        if position.trade_date < trading_date <= last_time.date()
    )
    return _close_position(
        position,
        last_time,
        float(last["close"]),
        f"time_{config.max_hold_days}d",
        holding_days,
    )


def _position_to_row(
    position: PaperPosition,
    candidate: GrowthBoardCandidate,
    day_minutes: pd.DataFrame,
) -> dict[str, object]:
    payload = position.risk_payload or {}
    hit_limit_up_today = bool(
        not day_minutes.empty
        and float(pd.to_numeric(day_minutes["high"], errors="coerce").max())
        >= candidate.limit_up_price - 0.01
    )
    return {
        "ts_code": position.ts_code,
        "name": position.name,
        "board_type": candidate.board_type,
        "signal_date": candidate.trade_date,
        "previous_date": candidate.previous_date,
        "entry_time": position.entry_time,
        "entry_price_raw": position.entry_price_raw,
        "entry_price": position.entry_price,
        "entry_signal": position.entry_signal,
        "limit_up_price": candidate.limit_up_price,
        "entry_to_limit_room_pct": round(
            (candidate.limit_up_price / position.entry_price - 1) * 100,
            4,
        ),
        "hit_limit_up_today": hit_limit_up_today,
        "stop_loss_price": position.stop_loss_price,
        "take_profit_price": position.take_profit_price,
        "trailing_stop_price": position.trailing_stop_price,
        "exit_time": position.exit_time,
        "exit_price": position.exit_price,
        "exit_reason": position.exit_reason,
        "holding_trading_days": position.holding_trading_days,
        "ret_pct": position.pnl_pct,
        "signal_minute_amount": payload.get("signal_minute_amount"),
        "signal_cum_amount_asof": payload.get("signal_cum_amount_asof"),
        "hist_same_minute_amount_median": payload.get(
            "hist_same_minute_amount_median"
        ),
        "hist_cum_amount_asof_median": payload.get("hist_cum_amount_asof_median"),
        "signal_rel_amount_same_minute": payload.get("signal_rel_amount_same_minute"),
        "signal_rel_cum_amount_asof": payload.get("signal_rel_cum_amount_asof"),
        "hist_intraday_days": payload.get("hist_intraday_days"),
        "signal_opening_segment": payload.get("signal_opening_segment"),
        "signal_amount_accel_5m": payload.get("signal_amount_accel_5m"),
        "signal_amount_accel_10m": payload.get("signal_amount_accel_10m"),
        "signal_vwap": payload.get("signal_vwap"),
        "signal_limit_progress_pct": payload.get("signal_limit_progress_pct"),
        "inner_vol_asof": payload.get("inner_vol_asof"),
        "outer_vol_asof": payload.get("outer_vol_asof"),
        "inner_outer_ratio": payload.get("inner_outer_ratio"),
        "classic_volume_ratio": payload.get("classic_volume_ratio"),
        "large_net_vol_t1": payload.get("large_net_vol_t1"),
        "price_percentile_250d": payload.get("price_percentile_250d"),
        "market_above_ma20_ratio_pct": payload.get("market_above_ma20_ratio_pct"),
        "growth_factor_score": payload.get("growth_factor_score"),
        "growth_factor_score_threshold": payload.get("growth_factor_score_threshold"),
        "growth_factor_hit_count": payload.get("growth_factor_hit_count"),
        "growth_factor_evaluated_count": payload.get("growth_factor_evaluated_count"),
        "board_code": payload.get("board_code"),
        "board_gap_up_ratio": payload.get("board_gap_up_ratio"),
        "board_auction_amount_ratio": payload.get("board_auction_amount_ratio"),
        "board_member_count": payload.get("board_member_count"),
        "intraday_order_flow_available": False,
        "unsupported_intraday_conditions": UNSUPPORTED_ORDER_FLOW_CONDITIONS,
    }


def _find_entry_position(
    store: DuckDBStore,
    candidate: GrowthBoardCandidate,
    minutes: pd.DataFrame,
    window_dates: list[date],
    config: GrowthBoardSurgeConfig,
    *,
    board_strength: dict[str, object] | None = None,
) -> tuple[PaperPosition, pd.DataFrame] | None:
    day_minutes = minutes[
        pd.to_datetime(minutes["trade_time"]).dt.date == candidate.trade_date
    ].sort_values("trade_time").reset_index(drop=True)
    if day_minutes.empty:
        return None
    if _is_intraday_yiziban(day_minutes, candidate.limit_up_price, config.price_tol):
        return None

    cum_vol = 0.0
    cum_amount = 0.0
    inner_vol = 0.0
    outer_vol = 0.0
    prev_minute_close: float | None = None
    static_factors: dict[str, float | int | None] | None = None
    clocked_amount_history: list[tuple[time, float]] = []
    for idx, row in day_minutes.iterrows():
        quote_time = _as_datetime(row["trade_time"])
        minute_amount = float(row["amount"]) if pd.notna(row["amount"]) else 0.0
        minute_vol = float(row["vol"]) if pd.notna(row["vol"]) else 0.0
        if minute_vol > 0:
            cum_vol += minute_vol
        if minute_amount > 0:
            cum_amount += minute_amount
        if pd.notna(row["close"]):
            minute_close = float(row["close"])
            reference = prev_minute_close
            if reference is None:
                reference = (
                    float(row["open"]) if pd.notna(row["open"]) else minute_close
                )
            inner_add, outer_add = _tick_rule_split(reference, minute_close, minute_vol)
            inner_vol += inner_add
            outer_vol += outer_add
            prev_minute_close = minute_close
        vwap = cum_amount / cum_vol if cum_vol > 0 else None
        quote = _quote_from_close(row)
        if quote_time.time() < config.min_signal_time:
            clocked_amount_history.append((quote_time.time(), minute_amount))
            continue
        if quote.price >= candidate.limit_up_price - config.price_tol:
            clocked_amount_history.append((quote_time.time(), minute_amount))
            continue
        if (
            config.require_vwap_strength
            and vwap is not None
            and quote.price < vwap * (1 + config.vwap_buffer_pct)
        ):
            clocked_amount_history.append((quote_time.time(), minute_amount))
            continue

        raw_features = build_intraday_relative_volume_features(
            store,
            candidate.ts_code,
            quote_time,
            current_minute_amount=minute_amount,
            current_cum_amount=cum_amount,
            current_day_amounts=clocked_amount_history,
            lookback_days=config.lookback_days,
            freq=config.freq,
        )
        features = _extract_relative_volume_features(
            raw_features,
            config.lookback_days,
        )
        if not _passes_surge_filter(features, config):
            clocked_amount_history.append((quote_time.time(), minute_amount))
            continue

        inner_outer_ratio = (
            round(inner_vol / outer_vol, 4) if outer_vol > 0 else None
        )
        if config.require_inner_outer and (
            inner_outer_ratio is None
            or inner_outer_ratio <= config.min_inner_outer_ratio
        ):
            clocked_amount_history.append((quote_time.time(), minute_amount))
            continue
        if config.factor_layer_enabled and static_factors is None:
            static_factors = _prefetch_growth_static_factors(
                store,
                ts_code=candidate.ts_code,
                previous_date=candidate.previous_date,
            )
        if config.require_large_net_vol:
            large_net = (static_factors or {}).get("large_net_vol_t1")
            # T-1 静态条件当日不再变化，不满足（含缺数据）直接放弃该候选
            if large_net is None or float(large_net) <= config.min_large_net_vol:
                return None
        factor_score: float | None = None
        raw_factor_values: dict[str, object] | None = None
        if config.factor_layer_enabled:
            raw_factor_values = {
                **(static_factors or {}),
                "rel_cum_amount_asof": features.get("signal_rel_cum_amount_asof"),
                "rel_amount_same_minute": features.get(
                    "signal_rel_amount_same_minute"
                ),
                "amount_accel_5m": features.get("signal_amount_accel_5m"),
                "inner_outer_ratio": inner_outer_ratio,
            }
            if board_strength is not None:
                raw_factor_values["board_gap_up_ratio"] = board_strength.get(
                    "board_gap_up_ratio"
                )
                raw_factor_values["board_auction_amount_ratio"] = (
                    board_strength.get("board_auction_amount_ratio")
                )
            factor_score = score_feature_terms(
                raw_factor_values, GROWTH_SURGE_B_V1_SCORE_TERMS
            )
            if (
                config.enable_factor_confirm
                and factor_score < config.factor_score_threshold
            ):
                clocked_amount_history.append((quote_time.time(), minute_amount))
                continue

        execution_index = idx + 1
        if execution_index >= len(day_minutes):
            return None
        execution_row = day_minutes.iloc[execution_index]
        execution_quote = _quote_from_open(execution_row)
        if execution_quote.price >= candidate.limit_up_price - config.price_tol:
            return None
        execution_time = _as_datetime(execution_row["trade_time"])
        limit_progress = (
            (quote.price - candidate.pre_close)
            / (candidate.limit_up_price - candidate.pre_close)
            * 100
            if candidate.limit_up_price > candidate.pre_close
            else None
        )
        avg_minute_vol_5d = _query_avg_minute_vol_5d(
            store, candidate.ts_code, candidate.previous_date
        )
        classic_volume_ratio = (
            round((cum_vol / (int(idx) + 1)) / avg_minute_vol_5d, 4)
            if avg_minute_vol_5d is not None and avg_minute_vol_5d > 0
            else None
        )
        risk_payload = {
            **features,
            "signal_vwap": round(vwap, 4) if vwap is not None else None,
            "signal_limit_progress_pct": (
                round(limit_progress, 4) if limit_progress is not None else None
            ),
            "inner_vol_asof": round(inner_vol, 2),
            "outer_vol_asof": round(outer_vol, 2),
            "inner_outer_ratio": inner_outer_ratio,
            "classic_volume_ratio": classic_volume_ratio,
            "intraday_order_flow_available": False,
            "unsupported_intraday_conditions": UNSUPPORTED_ORDER_FLOW_CONDITIONS,
        }
        if raw_factor_values is not None:
            raw_factor_values["classic_volume_ratio"] = classic_volume_ratio
            matrix = build_signal_factors(
                raw_factor_values,
                factor_set=GROWTH_SURGE_V1,
                threshold_overrides={
                    "rel_cum_amount_asof": config.min_cum_amount_ratio,
                    "rel_amount_same_minute": config.min_same_minute_amount_ratio,
                    "amount_accel_5m": config.min_amount_accel_5m,
                    "inner_outer_ratio": config.min_inner_outer_ratio,
                    "large_net_vol_t1": config.min_large_net_vol,
                    "board_gap_up_ratio": config.min_board_gap_up_ratio,
                    "board_auction_amount_ratio": (
                        config.min_board_auction_amount_ratio
                    ),
                },
            )
            risk_payload.update({
                "large_net_vol_t1": (static_factors or {}).get("large_net_vol_t1"),
                "price_percentile_250d": (
                    (static_factors or {}).get("price_percentile_250d")
                ),
                "market_above_ma20_ratio_pct": (
                    (static_factors or {}).get("market_above_ma20_ratio_pct")
                ),
                "growth_factor_score": factor_score,
                "growth_factor_score_threshold": (
                    config.factor_score_threshold
                    if config.enable_factor_confirm
                    else None
                ),
                "growth_factor_hit_count": matrix.hit_count,
                "growth_factor_evaluated_count": matrix.evaluated_count,
                "growth_signal_factors": matrix.to_json_payload(),
                "board_code": (board_strength or {}).get("board_code"),
                "board_gap_up_ratio": (board_strength or {}).get("board_gap_up_ratio"),
                "board_auction_amount_ratio": (
                    (board_strength or {}).get("board_auction_amount_ratio")
                ),
                "board_member_count": (
                    (board_strength or {}).get("board_member_count")
                ),
            })
        risk_plan = PaperRiskPlan(payload=risk_payload)
        watch_item = _WatchItem(
            ts_code=candidate.ts_code,
            pool="growth_board_surge",
            name=candidate.name,
            entry_date=candidate.trade_date,
            reference_date=candidate.previous_date,
            limit_up_date=candidate.trade_date,
            t_close=candidate.pre_close,
            t_high=None,
            limit_up_price_next=candidate.limit_up_price,
            stop_weak=0.0,
        )
        level = (
            "growth_board_factor_confirm"
            if config.enable_factor_confirm
            else "growth_board_volume_surge"
        )
        signal = {
            "level": level,
            "trigger_type": level,
            "level_price": candidate.limit_up_price,
            "signal_price": quote.price,
        }
        position = open_position_from_signal(
            watch_item,
            execution_quote,
            signal,
            execution_time,
            config.paper,
            earliest_exit_date=window_dates[1],
            risk_plan=risk_plan,
        )
        return position, day_minutes

    return None


def run_growth_board_surge_replay(
    store: DuckDBStore,
    *,
    start_date: str | date,
    end_date: str | date,
    config: GrowthBoardSurgeConfig | None = None,
) -> pd.DataFrame:
    """回放“科创/创业板 + 均线多头 + 分钟放量”入场与 T+1 离场。

    盘中入场只使用信号分钟及之前的数据。用户三条件的可回测口径：

    - 量比：沿用 ``min_cum_amount_ratio`` 成交额口径宽门；另输出经典量比观察值
      ``classic_volume_ratio``（当日每分钟均量 / T-1 收盘可知的 5 日每分钟均量）
    - 内盘>外盘：真实盘中内外盘无历史数据，用分钟 tick-rule 近似（close 对比前
      一分钟 close：升=外盘、降=内盘、平=均分，首分钟对比自身 open），
      ``require_inner_outer`` 开启时要求 inner/outer > ``min_inner_outer_ratio``
    - 大单净量：T 日盘中不可知，用 T-1 ``moneyflow_daily.large_net_vol`` 防未来
      函数（与用户口径「今日大单净量」有 1 个交易日滞后），
      ``require_large_net_vol`` 开启时要求 > ``min_large_net_vol``

    ``require_board_favor`` 开启时在候选级加一层板块集合竞价强度闸门（题材成分
    按 ≤T 最近打点还原，竞价 09:25 已知无未来函数，口径见 board_auction_strength）：
    板块高开占比与竞价资金相对历史双阈值不达标（含缺归属/缺竞价历史）跳过。

    ``enable_factor_confirm`` 开启时宽门不动，入场再过 ``growth_surge_b_v1``
    加权评分阈值（``factor_score_threshold``），命中矩阵随行输出。
    """
    cfg = config or GrowthBoardSurgeConfig()
    start = _as_date(start_date)
    end = _as_date(end_date)
    calendar = _trading_dates(store)
    if not calendar:
        return pd.DataFrame()

    rows: list[dict[str, object]] = []
    for trading_date in [day for day in calendar if start <= day <= end]:
        previous_date = _previous_trading_date(calendar, trading_date)
        if previous_date is None:
            continue
        window_dates = _window_trading_dates(calendar, trading_date, cfg.max_hold_days)
        if len(window_dates) <= cfg.max_hold_days:
            continue
        _, window_end = _day_bounds(window_dates[-1])
        for candidate in resolve_growth_board_candidates(
            store,
            trading_date,
            previous_date,
            cfg.min_signal_time,
        ):
            # 不做新股（候选级）：上市不满 N 个交易日跳过
            if cfg.min_listing_trading_days > 0 and (
                _listed_trading_days(store, candidate.ts_code, trading_date)
                < cfg.min_listing_trading_days
            ):
                continue
            # 首爆过滤（候选级日线条件，早于分钟查询以省算力）：前 N 日放过量则跳过
            if cfg.require_fresh_surge:
                prior_surge = _prior_days_had_surge(
                    store,
                    candidate.ts_code,
                    trading_date,
                    cfg.fresh_lookback_days,
                    cfg.fresh_max_prior_volume_ratio,
                )
                if prior_surge is True:
                    continue
            # 板块集合竞价强度闸门（候选级，早于分钟查询）：题材竞价整体不达标跳过。
            # 缺题材归属或缺竞价历史（ratio=None）保守拦截，与其他闸门一致。
            board_strength: dict[str, object] | None = None
            if cfg.require_board_favor:
                board_strength = board_auction_strength(
                    store,
                    candidate.ts_code,
                    trading_date,
                    hist_days=cfg.board_hist_days,
                    decision_at=datetime.combine(
                        trading_date,
                        cfg.min_signal_time,
                        tzinfo=SHANGHAI,
                    ),
                )
                if board_strength is None:
                    continue
                gap_ratio = board_strength.get("board_gap_up_ratio")
                amt_ratio = board_strength.get("board_auction_amount_ratio")
                if gap_ratio is None or float(gap_ratio) < cfg.min_board_gap_up_ratio:
                    continue
                if (
                    amt_ratio is None
                    or float(amt_ratio) < cfg.min_board_auction_amount_ratio
                ):
                    continue
            day_start, _ = _day_bounds(trading_date)
            minutes = store.query_minute_bars(
                candidate.ts_code,
                day_start,
                window_end,
                freq=cfg.freq,
            )
            if minutes.empty:
                continue
            entry = _find_entry_position(
                store,
                candidate,
                minutes,
                window_dates,
                cfg,
                board_strength=board_strength,
            )
            if entry is None:
                continue
            position, day_minutes = entry
            closed = _run_exit_scan(store, position, minutes, window_dates, cfg)
            if closed is None:
                continue
            rows.append(_position_to_row(closed, candidate, day_minutes))

    out = pd.DataFrame(rows)
    if not out.empty:
        out["intraday_order_flow_available"] = out[
            "intraday_order_flow_available"
        ].astype(object)
    return out
