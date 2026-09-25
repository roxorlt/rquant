"""``GET /api/v1/overview``: 总览 — one trading day at a glance.

The day shown is today once today's session has started, otherwise the last trading day
before today (a holiday, a weekend, before 09:15): see ``rquant.web.calendar.session_date``.
Everything comes from tables the Serving generation already publishes; a number whose
source is not published is left out rather than shown as zero.
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from collections.abc import Sequence
from datetime import date, datetime
from decimal import Decimal
from typing import Annotated

from fastapi import APIRouter, Depends, Request, Response

from rquant.dashboard.runtime_console_data import DeliveryRow, SignalRow
from rquant.web import readers
from rquant.web.calendar import session_date
from rquant.web.envelope import Envelope, ServingMeta, ServingState
from rquant.web.labels import ACTION_LABELS, PRESET_LABELS, split_service_id, strategy_label
from rquant.web.market import MarketPhase, shanghai_trade_date
from rquant.web.models.common import StateCounts
from rquant.web.models.health import FreshnessItem, ServiceItem
from rquant.web.models.overview import (
    ActionCount,
    AttentionItem,
    CandidateGroup,
    CandidateItem,
    CandidatesSummary,
    DeliveriesSummary,
    DeliveryState,
    FreshnessSummary,
    HoldingItem,
    OverviewData,
    PaperSummary,
    PipelineStage,
    SessionInfo,
    SignalItem,
    SignalsSummary,
    StageState,
)
from rquant.web.routes.health import (
    GenerationContext,
    freshness_items,
    generation_context,
    service_items,
    state_counts,
)
from rquant.web.security import current_user
from rquant.web.serving import serving_meta
from rquant.web.status import (
    SHADOW_NOTE,
    DeliveryMode,
    UserState,
    delivery_mode,
    watermark_status,
)

router = APIRouter()

_MAX_SIGNAL_ITEMS = 20
_MAX_CANDIDATE_ITEMS = 60
_MAX_ATTENTION = 8
_STAGE_LABELS: dict[StageState, str] = {
    "done": "已完成",
    "running": "进行中",
    "waiting": "未开始",
    "paused": "午休暂停",
    "late": "未生效",
}
#: Signal reason codes worth showing, in plain words; the rest are internal.
_REASON_LABELS = {
    "auction_gap_observer": "竞价跳空观察",
    "auction_gap_confirmed": "竞价跳空确认",
    "vwap_supported": "均价线支撑",
}
_SENDING = {"pending", "leased", "retry"}


def _float(value: Decimal | float | int) -> float:
    return float(value)


def _on(day: date | None, at: datetime) -> bool:
    return day is not None and shanghai_trade_date(at) == day


# ------------------------------------------------------------------ signals & deliveries


_FINISHED: dict[str, DeliveryState] = {
    "live": "delivered",
    "shadow": "recorded",
    "unknown": "unconfirmed",
}


def _delivery_state(rows: Sequence[DeliveryRow], mode: DeliveryMode) -> DeliveryState:
    statuses = {row.status for row in rows}
    if not statuses:
        return "none"
    if "dead_letter" in statuses:
        return "failed"
    if statuses & _SENDING:
        return "sending"
    if "succeeded" in statuses:
        return _FINISHED[mode.mode]
    return "expired"


_DELIVERY_STATE_LABELS: dict[DeliveryState, str] = {
    "delivered": "已送达",
    "recorded": "仅记录",
    "unconfirmed": "未确认",
    "sending": "发送中",
    "failed": "失败",
    "expired": "已过期",
    "none": "未推送",
}


def _delivery_mode(context: GenerationContext) -> DeliveryMode:
    return delivery_mode(
        [
            (row.status, row.stale, row.consecutive_failures, row.last_error)
            for row in context.sections.services
            if split_service_id(row.service_id)[0] == "notifier"
        ]
    )


def _reasons(signal: SignalRow) -> list[str]:
    try:
        codes = json.loads(signal.reason_codes_json)
    except (TypeError, ValueError):
        return []
    if not isinstance(codes, list):
        return []
    return [
        _REASON_LABELS[code] for code in codes if isinstance(code, str) and code in _REASON_LABELS
    ]


def _signals(
    signals: Sequence[SignalRow],
    deliveries: Sequence[DeliveryRow],
    names: dict[str, str],
    mode: DeliveryMode,
) -> SignalsSummary:
    by_signal: dict[str, list[DeliveryRow]] = defaultdict(list)
    for delivery in deliveries:
        by_signal[delivery.signal_id].append(delivery)
    ordered = sorted(signals, key=lambda row: (row.available_at, row.global_sequence), reverse=True)
    actions = Counter(row.action for row in signals)
    items = []
    for row in ordered[:_MAX_SIGNAL_ITEMS]:
        state = _delivery_state(by_signal.get(row.signal_id, ()), mode)
        items.append(
            SignalItem(
                signal_id=row.signal_id,
                sequence=row.global_sequence,
                at=row.available_at,
                code=row.candidate_id,
                name=names.get(row.candidate_id),
                strategy_id=row.strategy_id,
                strategy_name=strategy_label(row.strategy_id),
                action=row.action,
                action_label=ACTION_LABELS.get(row.action, "其他"),
                delivery=state,
                delivery_label=_DELIVERY_STATE_LABELS[state],
                reasons=_reasons(row),
            )
        )
    return SignalsSummary(
        total=len(signals),
        by_action=[
            ActionCount(action=action, label=ACTION_LABELS.get(action, "其他"), count=count)
            for action, count in sorted(actions.items(), key=lambda pair: (-pair[1], pair[0]))
        ],
        items=items,
    )


def _deliveries(deliveries: Sequence[DeliveryRow], mode: DeliveryMode) -> DeliveriesSummary:
    statuses = Counter(row.status for row in deliveries)
    return DeliveriesSummary(
        total=len(deliveries),
        delivered=statuses["succeeded"],
        sending=sum(statuses[name] for name in _SENDING),
        failed=statuses["dead_letter"],
        expired=statuses["expired"],
        mode=mode.mode,  # type: ignore[arg-type]
        mode_label=mode.label,
        mode_note=mode.note,
    )


def _empty_deliveries() -> DeliveriesSummary:
    return DeliveriesSummary(
        total=0,
        delivered=0,
        sending=0,
        failed=0,
        expired=0,
        mode="unknown",
        mode_label="未确认",
        mode_note=None,
    )


# ------------------------------------------------------------------ candidates


def _candidates(
    context: GenerationContext,
    session_signals: Sequence[SignalRow],
    names: dict[str, str],
) -> CandidatesSummary:
    groups: list[CandidateGroup] = []
    items: list[CandidateItem] = []
    hits = readers.canvas_hits(context.borrowed.cursor, context.tables)
    by_preset: dict[str, list[readers.CanvasHit]] = defaultdict(list)
    for hit in hits:
        by_preset[hit.preset_name].append(hit)
    for preset, members in sorted(by_preset.items()):
        name = PRESET_LABELS.get(preset, "选股结果")
        groups.append(
            CandidateGroup(
                key=f"screen:{preset}",
                name=name,
                count=len(members),
                as_of=members[0].trade_date,
                source="screen",
            )
        )
        items.extend(
            CandidateItem(
                code=hit.ts_code,
                name=hit.name or names.get(hit.ts_code),
                group=f"screen:{preset}",
                group_name=name,
                close=hit.close,
                pct_chg=hit.pct_chg,
                first_seen_at=None,
            )
            for hit in members
        )
    first_seen: dict[str, dict[str, datetime]] = defaultdict(dict)
    for signal in sorted(session_signals, key=lambda row: row.available_at):
        first_seen[signal.strategy_id].setdefault(signal.candidate_id, signal.available_at)
    for strategy_id, seen in sorted(first_seen.items()):
        name = strategy_label(strategy_id)
        groups.append(
            CandidateGroup(
                key=f"signals:{strategy_id}",
                name=name,
                count=len(seen),
                as_of=shanghai_trade_date(min(seen.values())),
                source="signals",
            )
        )
        items.extend(
            CandidateItem(
                code=code,
                name=names.get(code),
                group=f"signals:{strategy_id}",
                group_name=name,
                close=None,
                pct_chg=None,
                first_seen_at=at,
            )
            for code, at in sorted(seen.items(), key=lambda pair: pair[1])
        )
    return CandidatesSummary(
        total=sum(group.count for group in groups),
        groups=groups,
        items=items[:_MAX_CANDIDATE_ITEMS],
    )


# ------------------------------------------------------------------ paper account


def _paper(context: GenerationContext, names: dict[str, str]) -> PaperSummary | None:
    accounts = context.sections.paper_accounts
    if not accounts:
        return None
    account = accounts[0]
    holdings = [
        row for row in context.sections.paper_holdings if row.account_id == account.account_id
    ]
    note = None
    for watermark in context.manifest.watermarks:
        if watermark.dataset_id == "paper_accounts":
            status = watermark_status(watermark)
            if status.state is UserState.WARN:
                note = status.reason
    items = []
    for row in holdings:
        cost = _float(row.average_cost) * _float(row.quantity)
        items.append(
            HoldingItem(
                code=row.ts_code,
                name=names.get(row.ts_code),
                quantity=_float(row.quantity),
                available_quantity=_float(row.available_quantity),
                average_cost=_float(row.average_cost),
                market_price=_float(row.market_price),
                market_value=_float(row.market_value),
                unrealized_pnl=_float(row.unrealized_pnl),
                unrealized_pct=(_float(row.unrealized_pnl) / cost * 100) if cost else None,
            )
        )
    return PaperSummary(
        account_id=account.account_id,
        as_of=account.as_of_time,
        nav=_float(account.nav),
        cash=_float(account.cash),
        unrealized_pnl=_float(account.unrealized_pnl),
        realized_pnl=_float(account.realized_pnl),
        holdings=items,
        note=note,
    )


# ------------------------------------------------------------------ pipeline


def _stage_state(
    *,
    is_today: bool,
    phase: MarketPhase,
    starts: frozenset[MarketPhase],
    runs: frozenset[MarketPhase],
    pauses: frozenset[MarketPhase] = frozenset(),
) -> StageState:
    if not is_today:
        return "done"
    if phase in pauses:
        return "paused"
    if phase in runs:
        return "running"
    if phase in starts:
        return "waiting"
    return "done"


_BEFORE_OPEN = frozenset({MarketPhase.PRE_OPEN, MarketPhase.UNKNOWN})
_AUCTION = frozenset({MarketPhase.CALL_AUCTION})
_SESSION = frozenset({MarketPhase.CONTINUOUS, MarketPhase.CLOSING_AUCTION})
_NOON = frozenset({MarketPhase.NOON_BREAK})


def _stage(
    key: str,
    name: str,
    window: str,
    state: StageState,
    value: str | None,
    hint: str,
) -> PipelineStage:
    return PipelineStage(
        key=key,
        name=name,
        window=window,
        state=state,
        state_label=_STAGE_LABELS[state],
        value=value if state != "waiting" else None,
        hint=hint,
    )


def _pipeline(
    context: GenerationContext,
    *,
    day: date | None,
    is_today: bool,
    candidates: CandidatesSummary,
    signals: SignalsSummary,
    deliveries: DeliveriesSummary,
    paper: PaperSummary | None,
) -> list[PipelineStage]:
    phase = context.phase
    reference = next(
        (item for item in context.manifest.watermarks if item.dataset_id == "reference_slow"),
        None,
    )
    reference_ready = (
        reference is not None
        and day is not None
        and shanghai_trade_date(reference.event_time) >= day
        and watermark_status(reference).state is UserState.OK
    )
    count = readers.stock_count(context.borrowed.cursor, context.tables)
    if reference_ready:
        reference_state: StageState = "done"
    elif is_today and phase in _BEFORE_OPEN:
        reference_state = "waiting"
    else:
        reference_state = "late"
    auction = sum(group.count for group in candidates.groups if group.key == "signals:auction_gap")
    session_kwargs = {
        "is_today": is_today,
        "phase": phase,
        "starts": _BEFORE_OPEN | _AUCTION,
        "runs": _SESSION,
        "pauses": _NOON,
    }
    holdings = None if paper is None else len(paper.holdings)
    return [
        _stage(
            "reference",
            "参考数据",
            "09:20",
            reference_state,
            None if count is None else f"{count:,} 只",
            "股票列表、停牌、涨跌停价等当天的参考数据"
            + ("" if reference_ready else "；当天的参考数据还没有生效"),
        ),
        _stage(
            "auction",
            "竞价候选",
            "09:25–09:30",
            _stage_state(is_today=is_today, phase=phase, starts=_BEFORE_OPEN, runs=_AUCTION),
            f"{auction} 只",
            "集合竞价后按跳空和量比选出的观察名单",
        ),
        _stage(
            "signals",
            "盘中信号",
            "09:30–15:00",
            _stage_state(**session_kwargs),
            f"{signals.total} 条",
            "各策略在分钟线上确认后发出的信号",
        ),
        _stage(
            "paper",
            "模拟成交",
            "09:30–15:00",
            _stage_state(**session_kwargs),
            None if holdings is None else f"{holdings} 只持仓",
            "模拟账户按信号撮合成交",
        ),
        _stage(
            "notify",
            "通知推送",
            "全天",
            _stage_state(
                is_today=is_today,
                phase=phase,
                starts=frozenset(),
                runs=_BEFORE_OPEN | _AUCTION | _SESSION | _NOON,
            ),
            f"{deliveries.delivered} 条{deliveries.mode_label}",
            "信号推送到手机" + (f"；{deliveries.mode_note}" if deliveries.mode_note else ""),
        ),
    ]


# ------------------------------------------------------------------ attention


def _attention(
    meta: ServingMeta,
    services: Sequence[ServiceItem],
    freshness: Sequence[FreshnessItem],
    deliveries: DeliveriesSummary,
) -> list[AttentionItem]:
    items: list[AttentionItem] = []
    if meta.state is not ServingState.READY and meta.message:
        items.append(
            AttentionItem(
                level="crit",
                title="页面数据没有按时更新",
                reason=meta.message,
                to="/health",
                action="看健康",
            )
        )
    if deliveries.failed:
        items.append(
            AttentionItem(
                level="crit",
                title=f"{deliveries.failed} 条推送失败",
                reason="手机可能没有收到这些信号",
                to="/health",
                action="看健康",
            )
        )
    crit = [item for item in services if item.status.state is UserState.CRIT]
    warn = [item for item in services if item.status.state is UserState.WARN]
    if deliveries.mode == "shadow":
        # One plain item instead of the notifier's generic 注意.
        warn = [item for item in warn if split_service_id(item.service_id)[0] != "notifier"]
        items.append(
            AttentionItem(
                level="warn",
                title="推送还没有正式开通",
                reason=SHADOW_NOTE,
                to="/health",
                action="看健康",
            )
        )
    items.extend(
        AttentionItem(
            level="crit",
            title=f"{item.name}异常",
            reason=item.status.reason,
            to="/health",
            action="看健康",
        )
        for item in crit
    )
    if len(warn) > 3:
        items.append(
            AttentionItem(
                level="warn",
                title=f"{len(warn)} 个服务需要注意",
                reason="、".join(item.name for item in warn[:6]),
                to="/health",
                action="看健康",
            )
        )
    else:
        items.extend(
            AttentionItem(
                level="warn",
                title=f"{item.name}需要注意",
                reason=item.status.reason,
                to="/health",
                action="看健康",
            )
            for item in warn
        )
    warned = [item for item in freshness if item.status.state is UserState.WARN]
    late = [item for item in warned if item.status.label != "注意"]
    caveats = [item for item in warned if item.status.label == "注意"]
    if len(late) > 2:
        items.append(
            AttentionItem(
                level="warn",
                title=f"{len(late)} 项数据没有按时更新",
                reason="、".join(item.name for item in late),
                to="/health",
                action="看健康",
            )
        )
    else:
        items.extend(
            AttentionItem(
                level="warn",
                title=f"{item.name}{item.status.label}",
                reason=item.status.reason,
                to="/health",
                action="看健康",
            )
            for item in late
        )
    items.extend(
        AttentionItem(
            level="warn",
            title=f"{item.name}需要注意",
            reason=item.status.reason,
            to="/health",
            action="看健康",
        )
        for item in caveats
    )
    return items[:_MAX_ATTENTION]


def _freshness_summary(items: Sequence[FreshnessItem]) -> FreshnessSummary:
    checked = [item for item in items if item.status.state in {UserState.OK, UserState.WARN}]
    return FreshnessSummary(
        on_time=sum(item.status.state is UserState.OK for item in checked),
        checked=len(checked),
        no_source=sum(item.status.state is UserState.IDLE for item in items),
        late=[
            item.name
            for item in checked
            if item.status.state is UserState.WARN and item.status.label != "注意"
        ],
        caveats=[
            item.name
            for item in checked
            if item.status.state is UserState.WARN and item.status.label == "注意"
        ],
    )


# ------------------------------------------------------------------ assembly


def build_overview(context: GenerationContext, meta: ServingMeta) -> OverviewData:
    day = session_date(context.day, context.phase)
    is_today = day is not None and day == context.day.trade_date
    session_signals = [row for row in context.sections.signals if _on(day, row.available_at)]
    session_ids = {row.signal_id for row in session_signals}
    session_deliveries = [
        row for row in context.sections.deliveries if row.signal_id in session_ids
    ]
    codes = {row.candidate_id for row in session_signals} | {
        row.ts_code for row in context.sections.paper_holdings
    }
    names = readers.stock_names(context.borrowed.cursor, context.tables, codes)
    candidates = _candidates(context, session_signals, names)
    mode = _delivery_mode(context)
    signals = _signals(session_signals, session_deliveries, names, mode)
    deliveries = _deliveries(session_deliveries, mode)
    paper = _paper(context, names)
    services = service_items(context)
    freshness = freshness_items(context)
    return OverviewData(
        session=SessionInfo(
            today=context.day.trade_date,
            trade_date=day,
            is_today=is_today,
            phase=context.phase,
            next_trading_day=context.day.next_trading_day,
        ),
        pipeline=_pipeline(
            context,
            day=day,
            is_today=is_today,
            candidates=candidates,
            signals=signals,
            deliveries=deliveries,
            paper=paper,
        ),
        candidates=candidates,
        signals=signals,
        deliveries=deliveries,
        paper=paper,
        services=state_counts([item.status.state for item in services]),
        freshness=_freshness_summary(freshness),
        attention=_attention(meta, services, freshness, deliveries),
    )


def empty_overview(now: datetime, meta: ServingMeta) -> OverviewData:
    today = shanghai_trade_date(now)
    return OverviewData(
        session=SessionInfo(
            today=today,
            trade_date=None,
            is_today=False,
            phase=MarketPhase.UNKNOWN,
            next_trading_day=None,
        ),
        pipeline=[],
        candidates=CandidatesSummary(total=0, groups=[], items=[]),
        signals=SignalsSummary(total=0, by_action=[], items=[]),
        deliveries=_empty_deliveries(),
        paper=None,
        services=StateCounts(total=0, ok=0, warn=0, crit=0, idle=0, waiting=0),
        freshness=FreshnessSummary(on_time=0, checked=0, no_source=0, late=[], caveats=[]),
        attention=_attention(
            meta,
            (),
            (),
            _empty_deliveries(),
        ),
    )


@router.get("/overview", response_model=Envelope[OverviewData], summary="总览")
def get_overview(
    request: Request,
    response: Response,
    _viewer: Annotated[str | None, Depends(current_user)],
) -> Envelope[OverviewData]:
    web = request.app.state.web
    now = web.clock()
    with web.tracker.borrow() as borrowed:
        meta = serving_meta(
            borrowed, now=now, stale_after=web.settings.stale_after, failure=web.tracker.failure
        )
        if borrowed is None:
            data = empty_overview(now, meta)
        else:
            data = build_overview(generation_context(borrowed, now), meta)
    if meta.generation_id is not None:
        response.headers["X-Rquant-Generation"] = meta.generation_id
    return Envelope[OverviewData](data=data, serving=meta)


__all__ = ["build_overview", "router"]
