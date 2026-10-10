"""Read-only routes plus the three forwarded writes. Plain SQL, no projections layer."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable
from datetime import date
from typing import Annotated, Any, TypeVar

from fastapi import APIRouter, Depends, HTTPException, Request

from rquant.web import page_control
from rquant.web.backtest_perf import backtest_perf
from rquant.web.models import (
    AckAlertRequest,
    AddWatchRequest,
    AlertItem,
    AlertsData,
    BacktestDetailData,
    BacktestListData,
    BacktestRun,
    BacktestTrade,
    BoardItem,
    CommandReceipt,
    Envelope,
    FreshnessItem,
    HealthData,
    HoldingItem,
    Kpi,
    MarketPulse,
    MetaData,
    OverviewData,
    PanoramaData,
    PaperAccount,
    PaperData,
    PoolItem,
    PoolMember,
    PoolsData,
    SavePoolRequest,
    ScreenData,
    ScreenRow,
    ServiceItem,
    ServingMeta,
    SignalItem,
)
from rquant.web.source import Source, table_missing

router = APIRouter(prefix="/api/v1")
T = TypeVar("T")


def get_source(request: Request) -> Source:
    return request.app.state.source


SourceDep = Annotated[Source, Depends(get_source)]


def _envelope(source: Source, data: T) -> Envelope[T]:
    generation_id, built_at = source.generation()
    return Envelope[type(data)](  # type: ignore[misc]
        data=data,
        serving=ServingMeta(state="ready", generation_id=generation_id, generated_at=built_at),
    )


def _names(source: Source) -> dict[str, str]:
    return {r["ts_code"]: r["name"] for r in source.query(
        "SELECT ts_code, name FROM stock_basic LIMIT 8000")}


def _f(value: Any) -> float | None:
    return None if value is None else float(value)


def alert_id(row: dict[str, Any]) -> str:
    key = f"{row['trade_date']}|{row['trigger_time']}|{row['ts_code']}|{row['level']}"
    return hashlib.sha256(key.encode()).hexdigest()


@router.get("/meta", response_model=Envelope[MetaData])
def meta(request: Request, source: SourceDep) -> Envelope[MetaData]:
    generation_id, built_at = source.generation()
    gen = ServingMeta(state="ready", generation_id=generation_id, generated_at=built_at)
    return Envelope[MetaData](
        data=MetaData(
            version=request.app.version,
            generation=gen,
            notice=os.environ.get("RQUANT_WEB_NOTICE") or None,
        ),
        serving=gen,
    )


@router.get("/overview", response_model=Envelope[OverviewData], summary="总览")
def overview(source: SourceDep) -> Envelope[OverviewData]:
    names = _names(source)
    screen = source.query(
        "SELECT trade_date, count(*) AS n FROM screen_result "
        "GROUP BY trade_date ORDER BY trade_date DESC LIMIT 1")
    alerts = source.query(
        "SELECT count(*) AS n FROM monitor_event "
        "WHERE trade_date = (SELECT max(trade_date) FROM monitor_event)")
    services = source.query("SELECT status, stale FROM runtime_services LIMIT 500")
    bad = sum(1 for s in services if s["stale"] or s["status"] in {"failed", "error"})
    nav = source.query("SELECT sum(nav) AS nav FROM paper_accounts")
    signals = source.query(
        "SELECT global_sequence, available_at, candidate_id, strategy_id, action "
        "FROM signals ORDER BY global_sequence DESC LIMIT 20")
    trade_date = screen[0]["trade_date"] if screen else None
    kpis = [
        Kpi(key="candidates", label="候选股", value=str(screen[0]["n"] if screen else 0)),
        Kpi(key="alerts", label="今日告警", value=str(alerts[0]["n"] if alerts else 0)),
        Kpi(key="services", label="服务异常", value=str(bad), tone="crit" if bad else "ok"),
        Kpi(key="paper_nav", label="模拟净值",
            value="-" if not nav or nav[0]["nav"] is None else f"{float(nav[0]['nav']):,.0f}"),
    ]
    items = [
        SignalItem(sequence=s["global_sequence"], at=s["available_at"], code=s["candidate_id"],
                   name=names.get(s["candidate_id"]), strategy_id=s["strategy_id"],
                   action=s["action"])
        for s in signals
    ]
    return _envelope(source, OverviewData(trade_date=trade_date, kpis=kpis, signals=items))


@router.get("/health", response_model=Envelope[HealthData], summary="系统健康")
def health(source: SourceDep) -> Envelope[HealthData]:
    services = [ServiceItem(**{k: r.get(k) for k in ServiceItem.model_fields}) for r in
                source.query(
                    "SELECT service_id, plane, status, stale, heartbeat_at, backlog_count, "
                    "consecutive_failures, last_error FROM runtime_services "
                    "ORDER BY service_id LIMIT 500")]
    summary = source.query(
        "SELECT latest_daily_bar, latest_screen, daily_bar_rows, monitor_event_rows, "
        "minute_bar_rows FROM dashboard_summary LIMIT 1")
    labels = {"latest_daily_bar": "日线最新", "latest_screen": "选股最新",
              "daily_bar_rows": "日线行数", "monitor_event_rows": "盯盘事件数",
              "minute_bar_rows": "分钟线行数"}
    row = summary[0] if summary else {}
    freshness = [FreshnessItem(key=k, label=v, value=None if row.get(k) is None else str(row[k]))
                 for k, v in labels.items()]
    return _envelope(source, HealthData(services=services, freshness=freshness))


@router.get("/panorama", response_model=Envelope[PanoramaData], summary="市场全景")
def panorama(source: SourceDep) -> Envelope[PanoramaData]:
    snap = source.query(
        "SELECT as_of, pct_chg FROM market_snapshot "
        "WHERE as_of = (SELECT max(as_of) FROM market_snapshot) LIMIT 8000")
    pcts = [float(r["pct_chg"]) for r in snap if r["pct_chg"] is not None]
    pulse = MarketPulse(
        up=sum(p > 0 for p in pcts), down=sum(p < 0 for p in pcts), flat=sum(p == 0 for p in pcts),
        limit_up=sum(p >= 9.8 for p in pcts), limit_down=sum(p <= -9.8 for p in pcts))
    boards = [BoardItem(**{k: r.get(k) for k in BoardItem.model_fields}) for r in source.query(
        "SELECT system, board_code, board_name, amount, main_net_amount, pct_chg_median, "
        "limit_up_count, stock_count, leading_stock FROM market_overview "
        "WHERE as_of = (SELECT max(as_of) FROM market_overview) "
        "ORDER BY amount DESC NULLS LAST LIMIT 300")]
    as_of = snap[0]["as_of"] if snap else None
    return _envelope(source, PanoramaData(as_of=as_of, pulse=pulse, boards=boards))


@router.get("/screen", response_model=Envelope[ScreenData], summary="选股结果与排序")
def screen(source: SourceDep, trade_date: date | None = None,
           preset: str | None = None) -> Envelope[ScreenData]:
    if trade_date is None:
        latest = source.query("SELECT max(trade_date) AS d FROM screen_result")
        trade_date = latest[0]["d"] if latest else None
    if trade_date is None:
        return _envelope(source, ScreenData(trade_date=None, presets=[], rows=[]))
    rows = source.query(
        "SELECT trade_date, ts_code, preset_name, name, close, pct_chg FROM screen_result "
        "WHERE trade_date = ? ORDER BY pct_chg DESC NULLS LAST LIMIT 5000", [trade_date])
    presets = sorted({r["preset_name"] for r in rows})
    items = [ScreenRow(trade_date=r["trade_date"], code=r["ts_code"], name=r["name"],
                       preset=r["preset_name"], close=_f(r["close"]), pct_chg=_f(r["pct_chg"]))
             for r in rows if preset is None or r["preset_name"] == preset]
    return _envelope(source, ScreenData(trade_date=trade_date, presets=presets, rows=items))


@router.get("/pools", response_model=Envelope[PoolsData], summary="池子")
def pools(source: SourceDep) -> Envelope[PoolsData]:
    names = _names(source)
    latest = source.query("SELECT max(trade_date) AS d FROM canvas_hit")
    trade_date = latest[0]["d"] if latest else None
    hits = source.query(
        "SELECT preset_name, ts_code, row_json FROM canvas_hit WHERE trade_date = ? LIMIT 20000",
        [trade_date]) if trade_date else []
    by_preset: dict[str, list[PoolMember]] = {}
    for h in hits:
        try:
            detail = json.loads(h["row_json"] or "{}")
        except ValueError:
            detail = {}
        by_preset.setdefault(h["preset_name"], []).append(PoolMember(
            code=h["ts_code"], name=names.get(h["ts_code"]), preset=h["preset_name"],
            detail=detail if isinstance(detail, dict) else {}))
    out = []
    for c in source.query(
            "SELECT name, description, pool_refs_json, updated_at FROM canvas_definition "
            "ORDER BY name LIMIT 512"):
        refs = json.loads(c["pool_refs_json"] or "[]")
        members = [m for ref in refs for m in by_preset.get(ref, [])]
        out.append(PoolItem(name=c["name"], description=c["description"] or "", pool_refs=refs,
                            updated_at=c["updated_at"], members=members))
    return _envelope(source, PoolsData(trade_date=trade_date, pools=out))


_RUN_SQL = ("SELECT run_id, computed_at, start_date, end_date, entry_mode, profile_variant, "
            "trades, win_rate_pct, mean_ret_pct, median_ret_pct, best_ret_pct, worst_ret_pct "
            "FROM strategy_summary")


@router.get("/backtests", response_model=Envelope[BacktestListData], summary="回测结果")
def backtests(source: SourceDep) -> Envelope[BacktestListData]:
    runs = [BacktestRun(**r) for r in source.query(
        _RUN_SQL + " ORDER BY computed_at DESC LIMIT 200")]
    return _envelope(source, BacktestListData(runs=runs))


@router.get("/backtests/{run_id}", response_model=Envelope[BacktestDetailData],
            summary="回测详情")
def backtest_detail(run_id: str, source: SourceDep, entry_mode: str | None = None,
                    benchmark: str = "000300.SH") -> Envelope[BacktestDetailData]:
    runs = [r for r in source.query(_RUN_SQL + " WHERE run_id = ? LIMIT 50", [run_id])
            if entry_mode is None or r["entry_mode"] == entry_mode]
    if not runs:
        raise HTTPException(404, "backtest run not found")
    run = runs[0]
    raw = source.query(
        "SELECT trade_id, signal_date, ts_code, name, entry_time, entry_price, exit_time, "
        "exit_price, exit_reason, ret_pct FROM strategy_trade "
        "WHERE run_id = ? AND entry_mode = ? AND profile_variant = ? "
        "ORDER BY entry_time LIMIT 10000",
        [run_id, run["entry_mode"], run["profile_variant"]])
    try:
        bench_rows = source.query(
            "SELECT trade_date, close FROM benchmark_daily WHERE ts_code = ? "
            "ORDER BY trade_date LIMIT 1200", [benchmark])
    except Exception as exc:  # noqa: BLE001 - optional projection
        if not table_missing(exc):
            raise
        bench_rows = []
    perf = backtest_perf(raw, (benchmark, bench_rows) if bench_rows else None)
    trades = [BacktestTrade(code=t.pop("ts_code"), **t) for t in raw]
    return _envelope(
        source, BacktestDetailData(run=BacktestRun(**run), trades=trades, perf=perf))


@router.get("/alerts", response_model=Envelope[AlertsData], summary="告警时间线")
def alerts(source: SourceDep, limit: int = 200) -> Envelope[AlertsData]:
    names = _names(source)
    rows = source.query(
        "SELECT trade_date, trigger_time, ts_code, level, trigger_price, level_price, "
        "trigger_type, pool FROM monitor_event ORDER BY trigger_time DESC LIMIT ?",
        [max(1, min(limit, 1000))])
    acks = _acks(source)
    items = []
    for r in rows:
        aid = alert_id(r)
        ack = acks.get(aid, {})
        items.append(AlertItem(
            alert_id=aid, trade_date=r["trade_date"], at=r["trigger_time"], code=r["ts_code"],
            name=names.get(r["ts_code"]), level=r["level"], trigger_type=r["trigger_type"],
            trigger_price=_f(r["trigger_price"]), level_price=_f(r["level_price"]),
            pool=r["pool"], acked_at=ack.get("acked_at"), acked_by=ack.get("actor_id")))
    return _envelope(source, AlertsData(items=items))


def _acks(source: Source) -> dict[str, dict[str, Any]]:
    try:
        rows = source.query(
            "SELECT alert_id, acked_at, actor_id FROM alert_ack LIMIT 20000")
    except Exception as exc:  # noqa: BLE001 - only "not published yet" is tolerated
        if table_missing(exc):
            return {}
        raise
    return {r["alert_id"]: r for r in rows}


@router.get("/paper", response_model=Envelope[PaperData], summary="模拟盘")
def paper(source: SourceDep) -> Envelope[PaperData]:
    names = _names(source)
    holdings: dict[str, list[HoldingItem]] = {}
    for h in source.query(
            "SELECT account_id, ts_code, quantity, average_cost, market_price, market_value, "
            "unrealized_pnl FROM paper_holdings ORDER BY market_value DESC LIMIT 500"):
        holdings.setdefault(h["account_id"], []).append(HoldingItem(
            code=h["ts_code"], name=names.get(h["ts_code"]), quantity=float(h["quantity"]),
            average_cost=float(h["average_cost"]), market_price=float(h["market_price"]),
            market_value=float(h["market_value"]), unrealized_pnl=float(h["unrealized_pnl"])))
    accounts = [PaperAccount(
        account_id=a["account_id"], as_of=a["as_of_time"], nav=float(a["nav"]),
        cash=float(a["cash"]), unrealized_pnl=float(a["unrealized_pnl"]),
        realized_pnl=float(a["realized_pnl"]), holdings=holdings.get(a["account_id"], []))
        for a in source.query(
            "SELECT account_id, as_of_time, nav, cash, unrealized_pnl, realized_pnl "
            "FROM paper_accounts ORDER BY account_id LIMIT 50")]
    return _envelope(source, PaperData(accounts=accounts))


# ---- writes: all through page_control.forward --------------------------------


def _send(request: Request, kind: str, fields: dict[str, Any]) -> CommandReceipt:
    transport: Callable | None = getattr(request.app.state, "page_control_transport", None)
    try:
        receipt = page_control.forward(kind, fields, transport)
    except page_control.PageControlUnavailableError as exc:
        raise HTTPException(503, f"page control unavailable: {exc}") from exc
    return CommandReceipt(command_id=receipt.get("command_id"),
                          status=str(receipt.get("status", "unknown")),
                          detail=receipt.get("detail") or receipt.get("error"))


@router.post("/pools", response_model=CommandReceipt, summary="保存池子")
def save_pool(body: SavePoolRequest, request: Request) -> CommandReceipt:
    return _send(request, "save_canvas", {"name": body.name, "description": body.description,
                                          "pool_refs": body.pool_refs})


@router.post("/alerts/ack", response_model=CommandReceipt, summary="确认告警")
def ack_alert(body: AckAlertRequest, request: Request, source: SourceDep) -> CommandReceipt:
    generation_id, _ = source.generation()
    gen = generation_id if generation_id and len(generation_id) == 64 else hashlib.sha256(
        str(generation_id).encode()).hexdigest()
    actor = request.headers.get("X-Forwarded-User", "owner")
    return _send(request, "ack_alert",
                 {"alert_id": body.alert_id, "generation_id": gen, "actor_id": actor})


@router.post("/watchlist", response_model=CommandReceipt, summary="加入自选")
def add_watch(body: AddWatchRequest, request: Request) -> CommandReceipt:
    return _send(request, "add_watchlist_item", {"item": {"ts_code": body.code,
                                                           "note": body.note}})
