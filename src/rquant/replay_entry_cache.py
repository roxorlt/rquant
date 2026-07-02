"""分钟 replay 入场事件缓存与风控重放。"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from datetime import date, datetime

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

from rquant.paper import (
    PaperPosition,
    PaperRiskPlan,
    PaperTradeConfig,
    adjust_open_position_price_basis,
    check_position_exit,
    mark_position_to_quote,
    open_position_from_signal,
)


class ReplayWatchItem(BaseModel):
    """可重建入场仓位的候选标的信息。"""

    model_config = ConfigDict(frozen=True)

    ts_code: str
    pool: str
    name: str = ""
    entry_date: date | None = None
    reference_date: date | None = None
    limit_up_date: date
    t_close: float | None = None
    t_high: float | None = None
    limit_up_price_next: float | None = None
    stop_weak: float = 0.0


class RiskReplayQuote(BaseModel):
    """风控重放所需的分钟行情切片。"""

    model_config = ConfigDict(frozen=True)

    ts_code: str
    trade_time: datetime
    price: float
    low: float
    high: float | None = None


class PriceBasisCheck(BaseModel):
    """跨日价格基准检查，用于处理除权/复权导致的价格断点。"""

    model_config = ConfigDict(frozen=True)

    trade_date: date
    previous_close: float | None = None
    current_pre_close: float | None = None


class ReplayEntrySnapshot(BaseModel):
    """一个已触发 B 点的可重放入场快照。"""

    model_config = ConfigDict(frozen=True)

    item: ReplayWatchItem
    execution_quote: RiskReplayQuote
    signal: Mapping[str, object]
    entry_time: datetime
    earliest_exit_date: date
    window_dates: tuple[date, ...]
    exit_quotes: tuple[RiskReplayQuote, ...]
    risk_plan: PaperRiskPlan = Field(default_factory=PaperRiskPlan)
    basis_checks: tuple[PriceBasisCheck, ...] = ()
    feature_payload: dict[str, object] = Field(default_factory=dict)

    def open_position(self, config: PaperTradeConfig) -> PaperPosition:
        """用新的风控参数从同一入场事件重新开仓。"""
        return open_position_from_signal(
            self.item,
            self.execution_quote,
            self.signal,
            self.entry_time,
            config,
            earliest_exit_date=self.earliest_exit_date,
            risk_plan=self.risk_plan,
        )


class ReplayEntryCacheKey(BaseModel):
    """入场事件缓存 key，不包含退出风控参数。"""

    model_config = ConfigDict(frozen=True)

    preset_name: str
    start_date: str
    end_date: str
    entry_mode: str
    profile_variant: str
    max_hold_days: int
    freq: str = "1min"

    @property
    def cache_key(self) -> str:
        return "|".join(
            [
                self.preset_name,
                self.start_date,
                self.end_date,
                self.entry_mode,
                self.profile_variant,
                f"h{self.max_hold_days}",
                self.freq,
            ]
        )


class EntryReplayCache:
    """进程内入场事件缓存。"""

    def __init__(self) -> None:
        self._snapshots: dict[str, list[ReplayEntrySnapshot]] = {}

    def clear(self) -> None:
        self._snapshots.clear()

    def get_or_load(
        self,
        key: ReplayEntryCacheKey,
        loader: Callable[[], Iterable[ReplayEntrySnapshot]],
    ) -> list[ReplayEntrySnapshot]:
        if key.cache_key not in self._snapshots:
            self._snapshots[key.cache_key] = [
                snapshot.model_copy(deep=True) for snapshot in loader()
            ]
        return [
            snapshot.model_copy(deep=True)
            for snapshot in self._snapshots[key.cache_key]
        ]


def _holding_days(window_dates: tuple[date, ...], entry_date: date, exit_date: date) -> int:
    return sum(1 for trading_date in window_dates if entry_date < trading_date <= exit_date)


def _basis_checks_by_date(
    checks: tuple[PriceBasisCheck, ...],
) -> dict[date, PriceBasisCheck]:
    return {check.trade_date: check for check in checks}


def _close_position(
    position: PaperPosition,
    *,
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


def replay_snapshot_exit(
    snapshot: ReplayEntrySnapshot,
    config: PaperTradeConfig,
) -> PaperPosition | None:
    """对单个入场快照用新的风控参数重放退出。"""
    if not snapshot.exit_quotes:
        return None

    position = snapshot.open_position(config)
    active_date = position.entry_time.date()
    basis_checks = _basis_checks_by_date(snapshot.basis_checks)

    for quote in sorted(snapshot.exit_quotes, key=lambda item: item.trade_time):
        quote_time = quote.trade_time
        if quote_time < position.entry_time:
            continue

        quote_date = quote_time.date()
        if quote_date != active_date:
            check = basis_checks.get(quote_date)
            if check is not None:
                position = adjust_open_position_price_basis(
                    position,
                    check.previous_close,
                    check.current_pre_close,
                )
            active_date = quote_date

        exit_event = check_position_exit(position, quote, quote_time, config)
        if exit_event is None:
            position = mark_position_to_quote(position, quote)
            continue

        return _close_position(
            position,
            exit_time=exit_event.exit_time,
            exit_price=exit_event.exit_price,
            exit_reason=exit_event.exit_reason,
            holding_trading_days=_holding_days(
                snapshot.window_dates,
                position.trade_date,
                exit_event.exit_time.date(),
            ),
        )

    last = sorted(snapshot.exit_quotes, key=lambda item: item.trade_time)[-1]
    holding_days = _holding_days(
        snapshot.window_dates,
        position.trade_date,
        last.trade_time.date(),
    )
    return _close_position(
        position,
        exit_time=last.trade_time,
        exit_price=last.price,
        exit_reason=f"time_{holding_days}d",
        holding_trading_days=holding_days,
    )


def position_to_replay_row(
    position: PaperPosition,
    *,
    feature_payload: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """把重放后的持仓转成与分钟 replay 明细兼容的行。"""
    risk_payload = position.risk_payload or {}
    row = {
        "ts_code": position.ts_code,
        "name": position.name,
        "pool": position.pool,
        "signal_date": position.entry_t_date,
        "buy_date": position.trade_date,
        "entry_time": position.entry_time,
        "entry_price_raw": position.entry_price_raw,
        "entry_price": position.entry_price,
        "entry_signal": position.entry_signal,
        "stop_loss_price": position.stop_loss_price,
        "stop_loss_basis": position.stop_loss_basis,
        "take_profit_price": position.take_profit_price,
        "take_profit_basis": position.take_profit_basis,
        "trailing_stop_price": position.trailing_stop_price,
        "exit_time": position.exit_time,
        "exit_price": position.exit_price,
        "exit_reason": position.exit_reason,
        "holding_trading_days": position.holding_trading_days,
        "ret_pct": position.pnl_pct,
        "candidate_id": position.candidate_id,
        "volume_profile_lookbacks": ",".join(
            str(value) for value in risk_payload.get("lookbacks_used", [])
        ),
        "volume_profile_rr": risk_payload.get("reward_risk"),
        "volume_profile_poc_reclaimed": risk_payload.get("reclaimed_poc_count"),
        "signal_minute_amount": risk_payload.get("signal_minute_amount"),
        "signal_cum_amount_asof": risk_payload.get("signal_cum_amount_asof"),
        "hist_same_minute_amount_median_20d": risk_payload.get(
            "hist_same_minute_amount_median_20d"
        ),
        "hist_cum_amount_asof_median_20d": risk_payload.get(
            "hist_cum_amount_asof_median_20d"
        ),
        "signal_rel_amount_same_minute_20d": risk_payload.get(
            "signal_rel_amount_same_minute_20d"
        ),
        "signal_rel_cum_amount_asof_20d": risk_payload.get(
            "signal_rel_cum_amount_asof_20d"
        ),
        "hist_intraday_days_20d": risk_payload.get("hist_intraday_days_20d"),
        "signal_opening_segment": risk_payload.get("signal_opening_segment"),
        "signal_opening_segment_amount": risk_payload.get(
            "signal_opening_segment_amount"
        ),
        "signal_amount_accel_5m": risk_payload.get("signal_amount_accel_5m"),
        "signal_amount_accel_10m": risk_payload.get("signal_amount_accel_10m"),
    }
    if feature_payload:
        row.update(feature_payload)
    return row


def replay_entry_snapshots_to_trades(
    snapshots: Iterable[ReplayEntrySnapshot],
    config: PaperTradeConfig,
) -> pd.DataFrame:
    """批量重放入场快照并生成交易明细。"""
    rows: list[dict[str, object]] = []
    for snapshot in snapshots:
        position = replay_snapshot_exit(snapshot, config)
        if position is None:
            continue
        rows.append(
            position_to_replay_row(
                position,
                feature_payload=snapshot.feature_payload,
            )
        )
    return pd.DataFrame(rows)
