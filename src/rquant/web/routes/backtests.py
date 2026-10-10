"""Bounded, read-only minute replay results from one borrowed Serving generation."""

from __future__ import annotations

import re
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, Response

from rquant.web import readers
from rquant.web.envelope import Envelope
from rquant.web.models.backtests import (
    BacktestDetailData,
    BacktestGroup,
    BacktestListData,
    BacktestRun,
    BacktestTrade,
)
from rquant.web.security import current_user
from rquant.web.serving import serving_meta

router = APIRouter(prefix="/backtests")
_MODE_LABELS = {
    "first_break": "第一次突破",
    "break_retest": "突破回踩确认",
    "late_confirm": "10:30 后确认",
    "vwap_confirm": "均价线确认",
    "amount_surge": "成交额突增",
    "factor_confirm": "多因子确认",
}
_VARIANT_LABELS = {
    "baseline": "基础风控",
    "vp_risk_only": "价量动态风控",
    "vp_90": "价量过滤与风控",
}
_EXIT_LABELS = {
    "gap_stop": "跳空止损",
    "take_profit_gap": "跳空止盈",
    "take_profit_trailing": "跟踪止盈",
    "stop_loss": "止损",
    "time_exit": "持有到期",
    "next_auction_weak": "次日竞价偏弱",
}
_MAX_RUNS = 2_000
_MAX_TRADES = 10_000


def _exit_label(reason: str | None) -> str:
    if reason is not None and re.fullmatch(r"time_[1-9][0-9]*d", reason):
        return "持有到期"
    return _EXIT_LABELS.get(reason, "其他退出原因")


def _readable(states: dict[str, readers.TableState], name: str) -> bool:
    state = states.get(name)
    return state is not None and state.available


def _run(row: tuple) -> BacktestRun:
    return BacktestRun(
        run_id=str(row[0]),
        computed_at=row[1],
        start_date=row[2],
        end_date=row[3],
        max_hold_days=int(row[4]),
        candidates=int(row[5]),
        trades=int(row[6]),
        configurations=int(row[7]),
    )


def _run_by_id(cursor: object, run_id: str) -> BacktestRun | None:
    row = cursor.execute(
        "SELECT run_id, max(computed_at), min(start_date), max(end_date), "
        "max(max_hold_days), max(candidates), sum(trades), count(*) "
        "FROM strategy_summary WHERE run_id = ? GROUP BY run_id",
        (run_id,),
    ).fetchone()
    return None if row is None else _run(row)


def _group(row: tuple) -> BacktestGroup:
    mode, variant = str(row[0]), str(row[1])
    return BacktestGroup(
        entry_mode=mode,
        entry_mode_label=_MODE_LABELS.get(mode, "其他入场方式"),
        profile_variant=variant,
        profile_variant_label=_VARIANT_LABELS.get(variant, "其他风控"),
        candidates=int(row[2]),
        trades=int(row[3]),
        trigger_rate_pct=row[4],
        mean_ret_pct=row[5],
        median_ret_pct=row[6],
        win_rate_pct=row[7],
        best_ret_pct=row[8],
        worst_ret_pct=row[9],
        gap_stop_rate_pct=row[10],
    )


def _trade(row: tuple) -> BacktestTrade:
    mode, variant = str(row[1]), str(row[2])
    reason = None if row[10] is None else str(row[10])
    return BacktestTrade(
        trade_id=str(row[0]),
        entry_mode=mode,
        entry_mode_label=_MODE_LABELS.get(mode, "其他入场方式"),
        profile_variant=variant,
        profile_variant_label=_VARIANT_LABELS.get(variant, "其他风控"),
        signal_date=row[3],
        ts_code=str(row[4]),
        name=row[5],
        entry_time=row[6],
        entry_price=row[7],
        exit_time=row[8],
        exit_price=row[9],
        exit_reason=reason,
        exit_reason_label=_exit_label(reason),
        ret_pct=row[11],
    )


@router.get("", response_model=Envelope[BacktestListData], summary="最近分钟回放")
def list_backtests(
    request: Request,
    response: Response,
    _viewer: Annotated[str | None, Depends(current_user)],
    limit: Annotated[int, Query(ge=1, le=50)] = 20,
    offset: Annotated[int, Query(ge=0, le=_MAX_RUNS)] = 0,
    generation_id: Annotated[str | None, Query(pattern=r"^[0-9a-f]{64}$")] = None,
) -> Envelope[BacktestListData]:
    if offset > 0 and generation_id is None:
        raise HTTPException(status_code=422, detail="请从首批重新查看回放记录。")
    web = request.app.state.web
    with web.tracker.borrow() as borrowed:
        meta = serving_meta(
            borrowed,
            now=web.clock(),
            stale_after=web.settings.stale_after,
            failure=web.tracker.failure,
        )
        if generation_id is not None and meta.generation_id != generation_id:
            raise HTTPException(status_code=409, detail="数据已更新，请重新查看回放记录。")
        states = readers.table_states(borrowed.cursor) if borrowed is not None else {}
        available = meta.state != "unavailable" and _readable(states, "strategy_summary")
        if available:
            (total,) = borrowed.cursor.execute(
                "SELECT count(DISTINCT run_id) FROM strategy_summary"
            ).fetchone()
            rows = borrowed.cursor.execute(
                "SELECT run_id, max(computed_at), min(start_date), max(end_date), "
                "max(max_hold_days), max(candidates), sum(trades), count(*) "
                "FROM strategy_summary GROUP BY run_id "
                "ORDER BY max(computed_at) DESC, run_id DESC LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()
            runs = [_run(row) for row in rows]
        else:
            total, runs = 0, []
    if meta.generation_id is not None:
        response.headers["X-Rquant-Generation"] = meta.generation_id
    return Envelope[BacktestListData](
        data=BacktestListData(
            available=available,
            runs=runs,
            total=total,
            next_offset=offset + len(runs) if offset + len(runs) < total else None,
        ),
        serving=meta,
    )


@router.get("/{run_id}", response_model=Envelope[BacktestDetailData], summary="分钟回放详情")
def get_backtest(
    request: Request,
    response: Response,
    _viewer: Annotated[str | None, Depends(current_user)],
    run_id: Annotated[str, Path(min_length=1, max_length=128)],
    generation_id: Annotated[str, Query(pattern=r"^[0-9a-f]{64}$")],
    limit: Annotated[int, Query(ge=1, le=50)] = 20,
    offset: Annotated[int, Query(ge=0, le=_MAX_TRADES)] = 0,
    entry_mode: Annotated[str | None, Query(max_length=64)] = None,
    profile_variant: Annotated[str | None, Query(max_length=64)] = None,
) -> Envelope[BacktestDetailData]:
    if (entry_mode is None) != (profile_variant is None):
        raise HTTPException(status_code=422, detail="请选择完整的回放配置。")
    web = request.app.state.web
    with web.tracker.borrow() as borrowed:
        meta = serving_meta(
            borrowed,
            now=web.clock(),
            stale_after=web.settings.stale_after,
            failure=web.tracker.failure,
        )
        if borrowed is None or meta.generation_id != generation_id:
            raise HTTPException(status_code=409, detail="数据已更新，请重新选择回放。")
        states = readers.table_states(borrowed.cursor)
        summary_available = meta.state != "unavailable" and _readable(states, "strategy_summary")
        trades_available = meta.state != "unavailable" and _readable(states, "strategy_trade")
        run = _run_by_id(borrowed.cursor, run_id) if summary_available else None
        if summary_available and run is None:
            raise HTTPException(status_code=404, detail="找不到这次回放，请重新选择。")
        if run is not None:
            rows = borrowed.cursor.execute(
                "SELECT entry_mode, profile_variant, candidates, trades, trigger_rate_pct, "
                "mean_ret_pct, median_ret_pct, win_rate_pct, best_ret_pct, worst_ret_pct, "
                "gap_stop_rate_pct FROM strategy_summary WHERE run_id = ? "
                "ORDER BY entry_mode, profile_variant LIMIT ?",
                (run_id, _MAX_RUNS),
            ).fetchall()
            groups = [_group(row) for row in rows]
        else:
            groups = []
        if run is not None and trades_available:
            where = "run_id = ?"
            parameters: tuple[object, ...] = (run_id,)
            if entry_mode is not None and profile_variant is not None:
                where += " AND entry_mode = ? AND profile_variant = ?"
                parameters += (entry_mode, profile_variant)
            (total_trades,) = borrowed.cursor.execute(
                f"SELECT count(*) FROM strategy_trade WHERE {where}", parameters
            ).fetchone()
            trade_rows = borrowed.cursor.execute(
                "SELECT trade_id, entry_mode, profile_variant, signal_date, ts_code, name, "
                "entry_time, entry_price, exit_time, exit_price, exit_reason, ret_pct "
                f"FROM strategy_trade WHERE {where} "
                "ORDER BY signal_date DESC, entry_time DESC, trade_id DESC LIMIT ? OFFSET ?",
                (*parameters, limit, offset),
            ).fetchall()
            trades = [_trade(row) for row in trade_rows]
        else:
            total_trades, trades = 0, []
    response.headers["X-Rquant-Generation"] = meta.generation_id or ""
    return Envelope[BacktestDetailData](
        data=BacktestDetailData(
            summary_available=summary_available,
            trades_available=trades_available,
            run=run,
            groups=groups,
            trades=trades,
            total_trades=total_trades,
            next_offset=offset + len(trades) if offset + len(trades) < total_trades else None,
        ),
        serving=meta,
    )
