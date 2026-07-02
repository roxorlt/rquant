"""准实盘模拟仓：B 入场、止损线与退出判断。"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import date, datetime, timedelta
from decimal import ROUND_FLOOR, ROUND_HALF_UP, Decimal
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field

from rquant.price_adjustment import resolve_price_basis_adjustment


class WatchLike(Protocol):
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


class QuoteLike(Protocol):
    ts_code: str
    price: float
    low: float
    high: float | None


class PaperTradeConfig(BaseModel):
    """模拟盘候选参数配置。

    这里的默认值只用于单测和 baseline replay，不代表 N 字战法最终参数。
    生产准实盘应记录 candidate_id，并通过历史分时 replay + walk-forward 选择参数。
    """

    candidate_id: str = "baseline"
    stop_loss_pct: float = Field(default=0.03, gt=0, lt=1)
    entry_buffer_pct: float = Field(default=0.005, gt=0, lt=1)
    entry_slippage_pct: float = Field(default=0.0, ge=0, lt=0.05)
    take_profit_pct: float = Field(default=0.05, gt=0, lt=1)
    trailing_stop_pct: float = Field(default=0.025, gt=0, lt=1)


class PaperRiskPlan(BaseModel):
    """入场前生成的可解释风控计划。"""

    model_config = ConfigDict(frozen=True)

    entry_allowed: bool = True
    reject_reason: str | None = None
    stop_loss_price: float | None = None
    stop_loss_basis: str | None = None
    take_profit_price: float | None = None
    take_profit_basis: str | None = None
    trailing_stop_pct: float | None = None
    payload: dict[str, object] = Field(default_factory=dict)


class PaperPosition(BaseModel):
    """模拟盘持仓。"""

    model_config = ConfigDict(frozen=True)

    position_id: str
    trade_date: date
    ts_code: str
    name: str = ""
    pool: str = ""
    entry_time: datetime
    entry_price: float
    entry_price_raw: float | None = None
    entry_signal: str
    candidate_id: str = "baseline"
    entry_level_price: float | None = None
    entry_t_date: date | None = None
    earliest_exit_date: date
    t_close: float | None = None
    t_high: float | None = None
    limit_up_price_next: float | None = None
    stop_loss_price: float
    stop_loss_basis: str
    stop_loss_pct: float
    take_profit_price: float
    take_profit_pct: float
    take_profit_basis: str | None = None
    trailing_stop_pct: float
    trailing_stop_price: float | None = None
    status: str = "open"
    exit_time: datetime | None = None
    exit_price: float | None = None
    exit_reason: str | None = None
    holding_trading_days: int | None = None
    pnl_pct: float | None = None
    max_price_seen: float | None = None
    max_drawdown_pct: float | None = None
    risk_payload: dict[str, object] | None = None


class PaperExit(BaseModel):
    """模拟盘退出事件。"""

    model_config = ConfigDict(frozen=True)

    position_id: str
    exit_time: datetime
    exit_price: float
    exit_reason: str
    pnl_pct: float


def _round_price(value: float) -> float:
    return float(Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _floor_price(value: float) -> float:
    return float(Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_FLOOR))


def _pct_basis(stop_loss_pct: float) -> str:
    pct = stop_loss_pct * 100
    if pct.is_integer():
        return f"pct_{int(pct)}"
    return f"pct_{pct:g}"


def make_position_id(trade_date: date, ts_code: str, entry_signal: str) -> str:
    """生成稳定的单日单信号模拟仓 ID。"""
    return f"{trade_date:%Y%m%d}-{ts_code}-{entry_signal}"


def calculate_initial_stop_loss(
    item: WatchLike,
    quote: QuoteLike,
    entry_price: float,
    config: PaperTradeConfig,
    risk_plan: PaperRiskPlan | None = None,
) -> tuple[float, str]:
    """B 入场时冻结初始止损线。

    结构止损优先，但至少使用百分比止损兜底；若结构位太贴近买入价，则保留
    `entry_buffer_pct` 的最小缓冲，避免止损线等于或高于入场价。
    """
    if (
        risk_plan is not None
        and risk_plan.stop_loss_price is not None
        and 0 < risk_plan.stop_loss_price < entry_price
    ):
        profile_candidates: list[tuple[str, float]] = [(
            risk_plan.stop_loss_basis or "risk_plan_stop",
            float(risk_plan.stop_loss_price),
        )]
        for label, value in (
            ("t_close", item.t_close),
            ("pool2_stop_weak", item.stop_weak if item.pool == "pool2" else None),
        ):
            if value is not None and 0 < value < entry_price:
                profile_candidates.append((label, float(value)))
        basis, candidate = max(
            profile_candidates,
            key=lambda candidate_item: candidate_item[1],
        )
        entry_buffer_floor = entry_price * (1 - config.entry_buffer_pct)
        if candidate > entry_buffer_floor:
            return _round_price(entry_buffer_floor), "entry_buffer_cap"
        return _round_price(candidate), basis

    structural_candidates: list[tuple[str, float]] = []

    for label, value in (
        ("t_close", item.t_close),
        ("intraday_low", quote.low),
        ("pool2_stop_weak", item.stop_weak if item.pool == "pool2" else None),
    ):
        if value is not None and 0 < value < entry_price:
            structural_candidates.append((label, float(value)))

    structural_basis: str | None = None
    structural_floor: float | None = None
    if structural_candidates:
        structural_basis, structural_floor = max(
            structural_candidates,
            key=lambda candidate: candidate[1],
        )

    percent_floor = entry_price * (1 - config.stop_loss_pct)
    percent_basis = _pct_basis(config.stop_loss_pct)

    if structural_floor is not None and structural_floor >= percent_floor:
        candidate = structural_floor
        basis = structural_basis or "structure"
    else:
        candidate = percent_floor
        basis = percent_basis

    entry_buffer_floor = entry_price * (1 - config.entry_buffer_pct)
    if candidate > entry_buffer_floor:
        return _round_price(entry_buffer_floor), "entry_buffer_cap"

    return _round_price(candidate), basis


def open_position_from_signal(
    item: WatchLike,
    quote: QuoteLike,
    signal: Mapping[str, object],
    now: datetime,
    config: PaperTradeConfig,
    earliest_exit_date: date | None = None,
    risk_plan: PaperRiskPlan | None = None,
) -> PaperPosition:
    """从第一条 B 入场信号生成模拟仓。"""
    entry_signal = str(signal.get("level") or signal.get("trigger_type") or "attack")
    entry_price_raw = _round_price(quote.price)
    entry_price = _round_price(entry_price_raw * (1 + config.entry_slippage_pct))
    stop_loss_price, stop_loss_basis = calculate_initial_stop_loss(
        item,
        quote,
        entry_price,
        config,
        risk_plan,
    )
    high = quote.high if quote.high is not None else entry_price
    risk_take_profit = (
        risk_plan.take_profit_price
        if risk_plan is not None and risk_plan.take_profit_price is not None
        else None
    )
    if risk_take_profit is not None and risk_take_profit > entry_price:
        take_profit_price = _round_price(risk_take_profit)
        take_profit_basis = risk_plan.take_profit_basis or "risk_plan_take_profit"
    else:
        take_profit_price = _round_price(entry_price * (1 + config.take_profit_pct))
        take_profit_basis = f"pct_{config.take_profit_pct * 100:g}"
    trailing_stop_pct = (
        risk_plan.trailing_stop_pct
        if risk_plan is not None and risk_plan.trailing_stop_pct is not None
        else config.trailing_stop_pct
    )

    return PaperPosition(
        position_id=make_position_id(now.date(), item.ts_code, entry_signal),
        trade_date=now.date(),
        ts_code=item.ts_code,
        name=item.name,
        pool=item.pool,
        entry_time=now,
        entry_price=entry_price,
        entry_price_raw=entry_price_raw,
        entry_signal=entry_signal,
        candidate_id=config.candidate_id,
        entry_level_price=(
            float(signal["level_price"])
            if signal.get("level_price") is not None
            else None
        ),
        entry_t_date=item.reference_date or item.entry_date or item.limit_up_date,
        earliest_exit_date=earliest_exit_date or now.date() + timedelta(days=1),
        t_close=item.t_close,
        t_high=item.t_high,
        limit_up_price_next=item.limit_up_price_next,
        stop_loss_price=stop_loss_price,
        stop_loss_basis=stop_loss_basis,
        stop_loss_pct=config.stop_loss_pct,
        take_profit_price=take_profit_price,
        take_profit_pct=config.take_profit_pct,
        take_profit_basis=take_profit_basis,
        trailing_stop_pct=trailing_stop_pct,
        status="open",
        max_price_seen=max(float(high), entry_price),
        risk_payload=risk_plan.payload if risk_plan is not None else None,
    )


def mark_position_to_quote(
    position: PaperPosition,
    quote: QuoteLike,
) -> PaperPosition:
    """用最新行情更新最高价和移动止盈线，不产生退出副作用。"""
    if position.status != "open":
        return position

    quote_high = quote.high if quote.high is not None else quote.price
    max_price_seen = max(position.max_price_seen or position.entry_price, float(quote_high))
    trailing_stop_price = position.trailing_stop_price

    if max_price_seen >= position.take_profit_price:
        candidate = _floor_price(max_price_seen * (1 - position.trailing_stop_pct))
        trailing_stop_price = (
            max(trailing_stop_price, candidate)
            if trailing_stop_price is not None
            else candidate
        )

    return position.model_copy(update={
        "max_price_seen": _round_price(max_price_seen),
        "trailing_stop_price": trailing_stop_price,
    })


def _scale_optional_price(value: float | None, ratio: float) -> float | None:
    if value is None:
        return None
    return float(value) * ratio


def adjust_open_position_price_basis(
    position: PaperPosition,
    previous_close: float | None,
    current_pre_close: float | None,
) -> PaperPosition:
    """持仓跨除权/复权价格断点时，把成本和风控线缩放到新价格基准。"""
    if position.status != "open":
        return position

    adjustment = resolve_price_basis_adjustment(previous_close, current_pre_close)
    if not adjustment.adjusted:
        return position

    ratio = adjustment.ratio
    return position.model_copy(update={
        "entry_price": position.entry_price * ratio,
        "entry_level_price": _scale_optional_price(position.entry_level_price, ratio),
        "t_close": _scale_optional_price(position.t_close, ratio),
        "t_high": _scale_optional_price(position.t_high, ratio),
        "limit_up_price_next": _scale_optional_price(
            position.limit_up_price_next,
            ratio,
        ),
        "stop_loss_price": position.stop_loss_price * ratio,
        "take_profit_price": position.take_profit_price * ratio,
        "trailing_stop_price": _scale_optional_price(
            position.trailing_stop_price,
            ratio,
        ),
        "max_price_seen": _scale_optional_price(position.max_price_seen, ratio),
    })


def check_position_exit(
    position: PaperPosition,
    quote: QuoteLike,
    now: datetime,
    config: PaperTradeConfig,  # noqa: ARG001 - reserved for time exits / trailing stops.
) -> PaperExit | None:
    """检查当前行情是否触发模拟仓退出。"""
    if position.status != "open":
        return None
    if now.date() < position.earliest_exit_date:
        return None

    if quote.price < position.stop_loss_price:
        exit_price = _round_price(quote.price)
        exit_reason = "gap_stop"
    elif position.trailing_stop_price is not None and quote.price < position.trailing_stop_price:
        exit_price = _round_price(quote.price)
        exit_reason = "take_profit_gap"
    elif position.trailing_stop_price is not None and quote.low <= position.trailing_stop_price:
        exit_price = position.trailing_stop_price
        exit_reason = "take_profit_trailing"
    elif quote.low <= position.stop_loss_price:
        exit_price = position.stop_loss_price
        exit_reason = "stop_loss"
    else:
        return None

    pnl_pct = (exit_price / position.entry_price - 1) * 100
    return PaperExit(
        position_id=position.position_id,
        exit_time=now,
        exit_price=exit_price,
        exit_reason=exit_reason,
        pnl_pct=round(pnl_pct, 4),
    )


def can_open_position(
    existing_positions: Iterable[PaperPosition],
    ts_code: str,
    trade_date: date,
) -> bool:
    """同一只票同一交易日只允许开一笔模拟仓。"""
    return not any(
        position.ts_code == ts_code and position.trade_date == trade_date
        for position in existing_positions
    )
