"""``GET /api/v1/health``: 系统健康 — services by plane, data freshness, page data."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, Response

from rquant.dashboard.runtime_console_data import (
    ConsoleLimits,
    RuntimeConsoleSections,
    RuntimeServiceRow,
    read_runtime_console_sections,
    read_runtime_health_service_details,
)
from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_health_details import (
    RuntimeHealthAsOfValidity,
    RuntimeHealthComparisonScope,
    RuntimeHealthPortfolioScope,
    RuntimeHealthRealtimeValidity,
    RuntimeHealthRetainedOrderScope,
    RuntimeHealthValidatedMetric,
)
from rquant.serving_contracts import FreshnessStatus, ServingGenerationManifest
from rquant.web import readers
from rquant.web.calendar import CalendarDay, calendar_day
from rquant.web.envelope import Envelope
from rquant.web.labels import PLANE_LABELS, dataset_label, service_label, table_label
from rquant.web.market import MarketPhase, market_phase, shanghai_trade_date
from rquant.web.models.common import StateCounts, StatusInfo
from rquant.web.models.health import (
    ErrorItem,
    FreshnessItem,
    HealthData,
    HealthExposureItem,
    HealthLayer,
    HealthLink,
    HealthMetricItem,
    HealthObservation,
    HealthServiceDetail,
    PageDataStatus,
    ServiceItem,
    TableItem,
)
from rquant.web.paper_portfolio_reader import read_paper_portfolios
from rquant.web.security import current_user
from rquant.web.serving import BorrowedGeneration, serving_meta
from rquant.web.status import (
    STATE_ORDER,
    Status,
    UserState,
    daily_status,
    expected_daily_date,
    generation_status,
    is_reference_publisher,
    reference_publisher_status,
    service_status,
    watermark_status,
)

router = APIRouter()

_PLANE_ORDER = {"live": 0, "serving": 1, "research": 2}
#: Once-a-day data is due after the evening jobs (rquant-daily 17:00, research-ingest 18:10).
_DAILY_READY = time(18, 30)
_MINUTE_READY = time(19, 30)
#: The calendar should reach at least this far ahead of today.
_CALENDAR_HORIZON = timedelta(days=30)
_MAX_ERRORS = 20


@dataclass(frozen=True)
class GenerationContext:
    """Everything one request derives from its borrowed generation."""

    borrowed: BorrowedGeneration
    now: datetime
    day: CalendarDay
    phase: MarketPhase
    tables: Mapping[str, readers.TableState]
    sections: RuntimeConsoleSections

    @property
    def manifest(self) -> ServingGenerationManifest:
        return self.borrowed.manifest


def generation_context(borrowed: BorrowedGeneration, now: datetime) -> GenerationContext:
    day = calendar_day(borrowed.cursor, shanghai_trade_date(now))
    tables = readers.table_states(borrowed.cursor)
    sections = read_runtime_console_sections(
        borrowed.cursor,
        limits=ConsoleLimits(
            services=500,
            signals=500,
            deliveries=500,
            paper_accounts=20,
            paper_holdings=500,
            lab_jobs=1,
            promotions=1,
        ),
    )
    if (
        tables.get("runtime_health_detail_context") is not None
        and tables["runtime_health_detail_context"].available
    ):
        sections = RuntimeConsoleSections.model_validate(
            sections.model_dump(mode="python")
            | {"services": read_runtime_health_service_details(borrowed.cursor, sections.services)}
        )
    return GenerationContext(
        borrowed=borrowed,
        now=now,
        day=day,
        phase=market_phase(now, day.is_trading_day),
        tables=tables,
        sections=sections,
    )


# ------------------------------------------------------------------ services

_LAYER_SPECS = (
    (
        "host",
        "主机与服务",
        ("host_cpu", "slice_cpu", "host_memory", "slice_memory"),
        (("/tasks", "任务与调度"),),
    ),
    (
        "market",
        "行情数据",
        ("minute_delay", "minute_missing_codes"),
        (("/datacenter", "数据中心"),),
    ),
    (
        "strategy",
        "策略与信号",
        ("strategy_duration", "strategy_candidates"),
        (("/strategies", "策略"), ("/screener", "选股"), ("/monitor", "盯盘")),
    ),
    ("orders", "模拟订单", ("order_rejection_ratio",), (("/paper", "模拟盘"),)),
    ("risk", "组合风险", ("portfolio_risk", "portfolio_exposure"), (("/paper", "组合风控"),)),
    (
        "comparison",
        "收益对照",
        ("return_comparison",),
        (("/paper", "模拟账户"), ("/backtest", "回测结果")),
    ),
)
_METRIC_NAMES = {
    "host_cpu": "主机 CPU",
    "slice_cpu": "服务组 CPU",
    "host_memory": "主机可用内存",
    "slice_memory": "服务组内存",
    "minute_delay": "批次延迟",
    "minute_missing_codes": "缺失股票",
    "strategy_duration": "评估耗时",
    "strategy_candidates": "已处理候选",
    "order_rejection_ratio": "拒单率",
    "portfolio_risk": "风控结果",
    "portfolio_exposure": "现金权重",
    "return_comparison": "回测对照",
}
_OBSERVATION_NAMES = {
    "processed_candidates": "已处理候选",
    "accepted": "已接收",
    "received": "已接收",
    "orders": "订单",
    "signals": "信号",
    "batch_rows": "批次行数",
}


def _metric_status(item: RuntimeHealthValidatedMetric) -> StatusInfo:
    if item.availability != "available":
        return StatusInfo(
            state=UserState.IDLE, label="未运行", reason="尚无可核验观测，缺失值不代表零"
        )
    state = {"normal": UserState.OK, "attention": UserState.WARN, "abnormal": UserState.CRIT}.get(
        item.verdict, UserState.WARN
    )
    return StatusInfo(
        state=state,
        label={UserState.OK: "正常", UserState.WARN: "注意", UserState.CRIT: "异常"}[state],
        reason="已核验数值，尚无判断规则" if item.verdict == "unassessed" else "按原业务规则判断",
    )


def health_layers(context: GenerationContext, viewer: str | None) -> list[HealthLayer]:
    from rquant.paper_portfolio_projection import PaperPortfolioPublishedAccount

    private_scopes = (
        RuntimeHealthRetainedOrderScope,
        RuntimeHealthPortfolioScope,
        RuntimeHealthComparisonScope,
    )
    original = tuple(
        item
        for row in context.sections.services
        if row.details is not None
        for item in row.details.metrics_at(context.now)
    )
    accounts: dict[str, PaperPortfolioPublishedAccount] = {}
    account_labels: dict[str, str] = {}
    if viewer is not None and any(
        isinstance(item.metric.scope, private_scopes) for item in original
    ):
        snapshot = read_paper_portfolios(context.borrowed)
        if snapshot is not None:
            accounts = {
                row.configuration.binding.account_id: row for row in snapshot.for_owner(viewer)
            }
            account_labels = {key: f"组合 {index + 1}" for index, key in enumerate(accounts)}
    paper_generation = next(
        (
            row.generation_id
            for row in context.manifest.watermarks
            if row.dataset_id == "paper_accounts"
        ),
        None,
    )
    visible: list[RuntimeHealthValidatedMetric] = []
    exposure: list[HealthExposureItem] = []
    for item in original:
        metric, scope = item.metric, item.metric.scope
        if isinstance(scope, private_scopes):
            account = accounts.get(scope.account_id)
            if account is None:
                continue
            cfg = account.configuration
            exact = metric.source_generation_id == paper_generation
            if isinstance(scope, (RuntimeHealthRetainedOrderScope, RuntimeHealthPortfolioScope)):
                exact = (
                    exact
                    and scope.configuration_identity == cfg.fingerprint
                    and account.frame is not None
                    and scope.ledger_revision == (
                        account.risk.ledger_revision
                        if metric.metric_id == "portfolio_risk"
                        and isinstance(metric.validity, RuntimeHealthAsOfValidity)
                        and metric.validity.basis == "past_risk_decision"
                        and account.risk is not None
                        else account.frame.ledger_revision
                    )
                )
            else:
                band = account.band
                exact = (
                    exact
                    and band is not None
                    and scope.baseline_identity == band.backtest_source_hash
                    and scope.strategy_version == cfg.binding.strategy_version
                    and scope.parameter_fingerprint == cfg.binding.parameter_fingerprint
                    and scope.cost_identity == cfg.binding.cost_spec_id
                    and account.calendar is not None
                    and scope.calendar_identity == account.calendar.source_identity
                    and scope.comparison_dates == account.complete_comparison_dates()
                )
            if not exact:
                item = RuntimeHealthValidatedMetric(
                    metric=metric, availability="unavailable", reason_code="owner_source_changed"
                )
            elif metric.metric_id == "portfolio_exposure" and item.availability == "available":
                # The original complete account graph retains every original row.
                result = None if account.exposure is None else account.exposure.exposure
                receipt = account.exposure_receipt
                cash = (
                    None
                    if result is None
                    else next((row for row in result.rows if row.kind == "cash"), None)
                )
                if receipt is None or cash is None or cash.portfolio_weight != item.value:
                    raise ValueError("health cash weight differs from original same-read exposure")
                exposure.extend(
                    HealthExposureItem(
                        kind=row.kind,
                        name=row.industry_l1 or ("现金" if row.kind == "cash" else "未分类"),
                        portfolio_weight=row.portfolio_weight,
                        benchmark_weight=row.benchmark_weight,
                        deviation=row.deviation,
                        observed_at=metric.observed_at,
                        scope_key=canonical_sha256(scope),
                        scope_label=account_labels[scope.account_id],
                        scope_detail=scope.model_dump_json(),
                        source_name=dataset_label(metric.owner_dataset_id),
                        source_generation_id=metric.source_generation_id,
                        source_identity=metric.source_identity,
                        valid_until=metric.validity.valid_until
                        if isinstance(metric.validity, RuntimeHealthRealtimeValidity)
                        else None,
                        link=HealthLink(href="/paper", label="原组合风控"),
                    )
                    for row in result.rows
                )
        visible.append(item)
    layers: list[HealthLayer] = []
    for key, name, ids, destinations in _LAYER_SPECS:
        links = [HealthLink(href=href, label=label) for href, label in destinations]
        selected = [item for item in visible if item.metric.metric_id in ids]
        metrics = []
        for item in selected:
            metric = item.metric
            as_of = isinstance(metric.validity, RuntimeHealthAsOfValidity)
            realtime = isinstance(metric.validity, RuntimeHealthRealtimeValidity)
            basis = (
                "as_of"
                if as_of
                else "realtime"
                if realtime or metric.fresh_until is not None
                else "unknown"
            )
            scope_label = (
                "最近保留订单"
                if metric.metric_id == "order_rejection_ratio"
                else "已完成回测区间"
                if metric.metric_id == "return_comparison"
                else "当前主机"
                if metric.metric_id.startswith("host_")
                else "服务组"
                if metric.metric_id.startswith("slice_")
                else "原业务范围"
            )
            if isinstance(metric.scope, private_scopes):
                scope_label = account_labels[metric.scope.account_id] + " · " + scope_label
            metric_name = _METRIC_NAMES[metric.metric_id]
            if metric.metric_id.startswith("slice_"):
                group_name = {
                    "rquant.slice": "全部服务",
                    "rquant-live.slice": "盘中服务",
                    "rquant-serving.slice": "页面服务",
                    "rquant-research.slice": "研究服务",
                    "rquant-maintenance.slice": "维护服务",
                }.get(metric.scope.slice_id, "服务组")
                metric_name = group_name + (" CPU" if metric.unit == "ratio" else "内存")
            metrics.append(
                HealthMetricItem(
                    key=canonical_sha256(
                        {
                            "id": metric.metric_id,
                            "source": metric.source_identity,
                            "scope": metric.scope,
                        }
                    ),
                    name=metric_name,
                    value=item.value,
                    unit=metric.unit,
                    status=_metric_status(item),
                    available=item.availability == "available",
                    observed_at=metric.observed_at,
                    valid_until=metric.validity.valid_until if realtime else metric.fresh_until,
                    temporal_basis=basis,
                    scope_label=scope_label,
                    scope_detail=metric.scope.model_dump_json(),
                    event_time_start=metric.event_time_start,
                    event_time_end=metric.event_time_end,
                    available_at=metric.available_at,
                    source_name=dataset_label(metric.owner_dataset_id),
                    source_generation_id=metric.source_generation_id,
                    link=links[0],
                )
            )
        status = min(
            (item.status for item in metrics),
            key=lambda state: STATE_ORDER[state.state],
            default=StatusInfo(state=UserState.IDLE, label="未运行", reason="尚无可核验观测"),
        )
        layers.append(
            HealthLayer(
                key=key,
                name=name,
                status=status,
                observed_at=max((item.observed_at for item in metrics), default=None),
                metrics=metrics,
                exposure=exposure if key == "risk" else [],
                links=links,
            )
        )
    return layers


def reference_published_on(manifest: ServingGenerationManifest) -> date | None:
    """The Shanghai date of the visible reference generation (None when not fresh)."""

    for watermark in manifest.watermarks:
        if watermark.dataset_id == "reference_slow":
            if watermark.status is not FreshnessStatus.FRESH:
                return None
            return shanghai_trade_date(watermark.event_time)
    return None


def service_item(
    row: RuntimeServiceRow,
    *,
    phase: MarketPhase,
    now: datetime,
    day: CalendarDay | None = None,
    reference_on: date | None = None,
) -> ServiceItem:
    status = service_status(
        service_id=row.service_id,
        plane=row.plane,
        status=row.status,
        stale=row.stale,
        heartbeat_at=row.heartbeat_at,
        consecutive_failures=row.consecutive_failures,
        backlog_count=row.backlog_count,
        phase=phase,
        now=now,
    )
    if day is not None and is_reference_publisher(row.service_id):
        status = reference_publisher_status(
            status,
            is_trading_day=day.is_trading_day,
            today=day.trade_date,
            now=now,
            published_on=reference_on,
            last_error=row.last_error,
        )
    return ServiceItem(
        service_id=row.service_id,
        name=service_label(row.service_id),
        plane=row.plane,
        plane_label=PLANE_LABELS.get(row.plane, "其他"),
        status=StatusInfo.of(status),
        heartbeat_at=row.heartbeat_at,
        observed_at=row.observed_at,
        raw_status=row.status,
        stale=row.stale,
        input_sequence=row.input_sequence,
        output_sequence=row.output_sequence,
        backlog_count=row.backlog_count,
        consecutive_failures=row.consecutive_failures,
        last_error=row.last_error,
        detail=None
        if row.details is None
        else HealthServiceDetail(
            available=row.details.availability == "available",
            reason="观测已核验" if row.details.availability == "available" else "尚无可核验观测",
            observed_at=row.details.observed_at,
            source_name="原服务心跳",
            started_at=None
            if row.details.startup_witness is None
            else row.details.startup_witness.started_at,
            observations=[]
            if row.observations is None
            or any(
                isinstance(
                    item.metric.scope,
                    (
                        RuntimeHealthRetainedOrderScope,
                        RuntimeHealthPortfolioScope,
                        RuntimeHealthComparisonScope,
                    ),
                )
                for item in row.details.metrics
            )
            else [
                HealthObservation(
                    key=key, label=_OBSERVATION_NAMES.get(key, "业务计数"), value=value
                )
                for key, value in row.observations.items()
            ],
            degraded_reason=None
            if not row.degraded_detail
            else "部分功能暂不可用，请查看原运行日志",
        ),
    )


def service_items(context: GenerationContext) -> list[ServiceItem]:
    reference_on = reference_published_on(context.manifest)
    items = [
        service_item(
            row,
            phase=context.phase,
            now=context.now,
            day=context.day,
            reference_on=reference_on,
        )
        for row in context.sections.services
    ]
    return sorted(
        items,
        key=lambda item: (
            STATE_ORDER[item.status.state],
            _PLANE_ORDER.get(item.plane, 9),
            item.name,
            item.service_id,
        ),
    )


def state_counts(states: Sequence[UserState]) -> StateCounts:
    counts = Counter(states)
    return StateCounts(
        total=len(states),
        ok=counts[UserState.OK],
        warn=counts[UserState.WARN],
        crit=counts[UserState.CRIT],
        idle=counts[UserState.IDLE],
        waiting=counts[UserState.WAITING],
    )


def _error_summary(item: ServiceItem) -> str:
    if item.consecutive_failures > 0:
        return f"连续失败 {item.consecutive_failures} 次"
    return "报告了一个错误"


def error_items(services: Sequence[ServiceItem]) -> list[ErrorItem]:
    # A service judged 正常 / 已完成 with an error text is refusing by design (the reference
    # publisher after 09:25); its text stays in the drawer, not in 最近错误.
    with_errors = [
        item for item in services if item.last_error and item.status.state is not UserState.OK
    ]
    with_errors.sort(
        key=lambda item: item.heartbeat_at or item.observed_at,
        reverse=True,
    )
    return [
        ErrorItem(
            service_id=item.service_id,
            name=item.name,
            at=item.heartbeat_at or item.observed_at,
            summary=_error_summary(item),
            message=str(item.last_error),
        )
        for item in with_errors[:_MAX_ERRORS]
    ]


# ------------------------------------------------------------------ freshness


def _daily_item(
    context: GenerationContext,
    *,
    key: str,
    name: str,
    latest: date | None,
    latest_at: datetime | None,
    ready_at: time,
    ready_note: str,
) -> FreshnessItem:
    expected = expected_daily_date(
        today_is_trading_day=context.day.is_trading_day,
        today=context.day.trade_date,
        previous_trading_day=context.day.previous_trading_day,
        now=context.now,
        ready_at=ready_at,
    )
    behind = (
        readers.trading_days_after(context.borrowed.cursor, context.tables, latest, expected)
        if latest is not None and expected is not None and latest < expected
        else None
    )
    status = daily_status(latest, expected, behind_days=behind, ready_note=ready_note)
    return FreshnessItem(
        key=key,
        name=name,
        kind="market",
        latest_at=latest_at,
        latest_date=latest,
        status=StatusInfo.of(status),
    )


def _calendar_item(context: GenerationContext) -> FreshnessItem:
    last = readers.calendar_last_date(context.borrowed.cursor, context.tables)
    today = context.day.trade_date
    if last is None:
        status = Status(UserState.IDLE, "未发布", "交易日历暂时没有数据")
    elif last < today + _CALENDAR_HORIZON:
        status = Status(
            UserState.WARN,
            "快到期",
            f"只覆盖到 {last.isoformat()}，需要补充下一段交易日历",
        )
    else:
        status = Status(UserState.OK, "按时", f"覆盖到 {last.isoformat()}")
    return FreshnessItem(
        key="trade_calendar",
        name="交易日历",
        kind="market",
        latest_at=None,
        latest_date=last,
        status=StatusInfo.of(status),
    )


def freshness_items(context: GenerationContext) -> list[FreshnessItem]:
    cursor = context.borrowed.cursor
    minute_at = readers.minute_latest(cursor, context.tables)
    minute_date = None if minute_at is None else shanghai_trade_date(minute_at)
    items = [
        _daily_item(
            context,
            key="daily_bar",
            name="日线",
            latest=readers.daily_latest(cursor, context.tables),
            latest_at=None,
            ready_at=_DAILY_READY,
            ready_note="每个交易日 18:30 后应更新到当天",
        ),
        _daily_item(
            context,
            key="minute_coverage",
            name="分钟线",
            latest=minute_date,
            latest_at=minute_at,
            ready_at=_MINUTE_READY,
            ready_note="每个交易日 19:30 后应补齐当天",
        ),
        _daily_item(
            context,
            key="canvas_latest_trade_date",
            name="选股结果",
            latest=readers.screen_latest(cursor, context.tables),
            latest_at=None,
            ready_at=_DAILY_READY,
            ready_note="每个交易日收盘后选出下一交易日的候选",
        ),
        _calendar_item(context),
    ]
    for watermark in context.manifest.watermarks:
        status = watermark_status(watermark)
        unavailable = status.state is UserState.IDLE
        items.append(
            FreshnessItem(
                key=watermark.dataset_id,
                name=dataset_label(watermark.dataset_id),
                kind="dataset",
                latest_at=None if unavailable else watermark.event_time,
                latest_date=None,
                status=StatusInfo.of(status),
            )
        )
    return items


# ------------------------------------------------------------------ page data


def page_data_status(
    context: GenerationContext,
    *,
    stale_after: timedelta,
    fallback_detail: str | None,
) -> PageDataStatus:
    manifest = context.manifest
    age = max((context.now - manifest.built_at).total_seconds(), 0.0)
    status = generation_status(age, stale_after)
    if status.state is UserState.OK and fallback_detail:
        status = Status(UserState.WARN, "注意", "最新一批数据没有通过校验，暂时显示上一批")
    unpublished = sorted(
        (state for state in context.tables.values() if not state.available),
        key=lambda state: table_label(state.table_name),
    )
    pointer = context.borrowed.pointer
    return PageDataStatus(
        status=StatusInfo.of(status),
        built_at=manifest.built_at,
        published_at=None if pointer is None else pointer.published_at,
        age_seconds=age,
        generation_id=manifest.generation_id,
        tables_total=len(context.tables),
        unpublished=[
            TableItem(key=state.table_name, name=table_label(state.table_name))
            for state in unpublished
        ],
    )


def build_health(
    context: GenerationContext,
    *,
    stale_after: timedelta,
    fallback_detail: str | None,
    viewer: str | None = None,
) -> HealthData:
    services = service_items(context)
    return HealthData(
        counts=state_counts([item.status.state for item in services]),
        services=services,
        freshness=freshness_items(context),
        page_data=page_data_status(
            context, stale_after=stale_after, fallback_detail=fallback_detail
        ),
        errors=error_items(services),
        layers=health_layers(context, viewer),
        viewer_id=viewer,
    )


def empty_health(stale_after: timedelta) -> HealthData:
    return HealthData(
        counts=state_counts([]),
        services=[],
        freshness=[],
        page_data=PageDataStatus(
            status=StatusInfo.of(generation_status(None, stale_after)),
            built_at=None,
            published_at=None,
            age_seconds=None,
            generation_id=None,
            tables_total=0,
            unpublished=[],
        ),
        errors=[],
        layers=[
            HealthLayer(
                key=key,
                name=name,
                status=StatusInfo(state=UserState.IDLE, label="未运行", reason="尚无可核验观测"),
                observed_at=None,
                metrics=[],
                links=[HealthLink(href=href, label=label) for href, label in destinations],
            )
            for key, name, _ids, destinations in _LAYER_SPECS
        ],
    )


@router.get("/health", response_model=Envelope[HealthData], summary="系统健康")
def get_health(
    request: Request,
    response: Response,
    _viewer: Annotated[str | None, Depends(current_user)],
) -> Envelope[HealthData]:
    web = request.app.state.web
    now = web.clock()
    stale_after = web.settings.stale_after
    with web.tracker.borrow() as borrowed:
        meta = serving_meta(borrowed, now=now, stale_after=stale_after, failure=web.tracker.failure)
        if borrowed is None:
            data = empty_health(stale_after).model_copy(update={"viewer_id": _viewer})
        else:
            try:
                data = build_health(
                    generation_context(borrowed, now),
                    stale_after=stale_after,
                    fallback_detail=borrowed.fallback_detail,
                    viewer=_viewer,
                )
            except ValueError as error:
                raise HTTPException(
                    status_code=503, detail="健康详情暂时无法核验，请稍后刷新"
                ) from error
    if meta.generation_id is not None:
        response.headers["X-Rquant-Generation"] = meta.generation_id
    return Envelope[HealthData](data=data, serving=meta)


__all__ = [
    "GenerationContext",
    "build_health",
    "freshness_items",
    "generation_context",
    "router",
    "service_items",
    "state_counts",
]
