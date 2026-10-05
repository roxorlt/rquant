"""``create_app()``: the FastAPI application behind ``/app/api/``.

nginx serves the static front end itself and proxies ``/app/api/`` here with the prefix
stripped, so this app only knows ``/api/v1/...``. No static files, no CORS, no docs pages
(the schema is exported by ``rquant web-openapi`` into ``web/src/api/openapi.json``).
"""

from __future__ import annotations

import json
import re
import secrets
import threading
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import anyio.to_thread
from fastapi import Depends, FastAPI, Request, Response
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse

from rquant.screen.dynamic_rsi import VerifiedDynamicRsiProjection
from rquant.screen.formula_history_projection import VerifiedFormulaHistoryProjection
from rquant.screen.replica_source import VerifiedReplicaScreenSource
from rquant.unit_log_service import UnitLogClient
from rquant.web.alert_ack_gateway import AckLookupGateway, AckLookupTransport
from rquant.web.backfill_plan_command_gateway import (
    BackfillPlanCommandGateway,
    BackfillPlanCommandTransport,
)
from rquant.web.data_audit_report_command_gateway import (
    AuditReportCommandGateway,
    AuditReportCommandTransport,
)
from rquant.web.formula_market_command_gateway import (
    FormulaMarketCommandGateway,
    FormulaMarketCommandTransport,
)
from rquant.web.lab_control_gateway import LabControlGateway, LabControlTransport
from rquant.web.nl_parser import OpenAiScreenPlanParser, ScreenPlanParser
from rquant.web.pool_editor_gateway import PoolCommandGateway, PoolCommandTransport
from rquant.web.pool_nl_preview import PoolNlRateLimiter
from rquant.web.proxy_identity import ProxyIdentityVerifier
from rquant.web.routes import (
    backfill_plan_commands,
    backfill_plans,
    backtests,
    catalog,
    data_audit,
    data_audit_report,
    data_audit_report_calendar,
    data_audit_report_commands,
    experiments,
    factor_results,
    factor_runs,
    factor_saves,
    factor_tracking,
    factors,
    formula_market_commands,
    formula_market_read,
    formula_pool_read,
    formula_pool_save_commands,
    fundamentals,
    health,
    manual_watchlist,
    meta,
    monitor,
    overview,
    panorama,
    paper,
    pool_editor,
    pools,
    price_alert_rules,
    research_query,
    screen,
    service_logs,
    stocks,
    strategies,
    tasks,
    tasks_controls,
)
from rquant.web.screen_service import ScreenApplicationService
from rquant.web.security import require_current_user
from rquant.web.service_log_access_audit import ServiceLogAccessAudit
from rquant.web.serving import GenerationTracker
from rquant.web.settings import WebSettings

if TYPE_CHECKING:
    from rquant.alert_ack_admission import AckAdmissionClient
    from rquant.factor_definition_admission import FactorDefinitionAdmissionClient
    from rquant.factor_run_admission import FactorRunAdmissionClient
    from rquant.factor_tracking_admission import FactorTrackingAdmissionClient
    from rquant.price_alert_admission import PriceAlertAdmissionClient
    from rquant.research_query.service import QueryPrivateClient
    from rquant.watchlist_admission import WatchlistAdmissionClient

API_TITLE = "rQuant Web API"
#: Version of the HTTP contract, bumped by hand; not the package version, so that a
#: release that does not touch the API leaves the OpenAPI snapshot unchanged.
API_VERSION = "1"
_WRITE_BODY_LIMITS = {
    "/api/v1/screen/nl-preview": screen.MAX_NL_REQUEST_BYTES,
    "/api/v1/pools/editor/commands": pool_editor.MAX_REQUEST_BYTES,
    "/api/v1/pools/editor/nl-preview": pool_editor.MAX_NL_REQUEST_BYTES,
    "/api/v1/monitor/ack": monitor.MAX_ACK_REQUEST_BYTES,
    "/api/v1/watchlist/commands": manual_watchlist.MAX_COMMAND_REQUEST_BYTES,
    "/api/v1/monitor/price-rules/commands": price_alert_rules.MAX_COMMAND_REQUEST_BYTES,
    "/api/v1/monitor/price-rules/commands/resume": price_alert_rules.MAX_COMMAND_REQUEST_BYTES,
    "/api/v1/data/backfill-plans/commands": backfill_plan_commands.MAX_REQUEST_BYTES,
    "/api/v1/data/audit-report/commands": data_audit_report_commands.MAX_REQUEST_BYTES,
    "/api/v1/screen/tdx/market/commands": formula_market_commands.MAX_REQUEST_BYTES,
    "/api/v1/pools/formula/commands": formula_pool_save_commands.MAX_REQUEST_BYTES,
    "/api/v1/tasks/jobs/commands": tasks_controls.MAX_REQUEST_BYTES,
    "/api/v1/research/query": research_query.MAX_REQUEST_BYTES,
    "/api/v1/research/queries/save": research_query.MAX_REQUEST_BYTES,
    "/api/v1/research/queries/resume": research_query.MAX_REQUEST_BYTES,
}
_FACTOR_ARCHIVE_WRITE = re.compile(
    r"^/api/v1/factors/definitions/[a-z][a-z0-9_]{0,63}/archive(?:/resume)?$"
)
_FACTOR_SAVE_WRITE = re.compile(r"^/api/v1/factors/definitions/save(?:/(?:resume|retry))?$")
_FACTOR_RUN_WRITE = re.compile(r"^/api/v1/factors/runs(?:/(?:resume|retry))?$")


def _utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True)
class WebContext:
    settings: WebSettings
    proxy_identity: ProxyIdentityVerifier | None
    tracker: GenerationTracker
    clock: Callable[[], datetime]
    cursor_key: bytes
    screen_gate: threading.BoundedSemaphore
    screen_service: ScreenApplicationService
    pool_commands: PoolCommandGateway
    nl_parser: ScreenPlanParser | None
    nl_gate: threading.BoundedSemaphore
    nl_rate_limiter: PoolNlRateLimiter
    ack_lookup: AckLookupGateway
    ack_admission: AckAdmissionClient | None
    watchlist_admission: WatchlistAdmissionClient | None
    price_alert_admission: PriceAlertAdmissionClient | None
    factor_admission: FactorDefinitionAdmissionClient | None
    factor_run_admission: FactorRunAdmissionClient | None
    factor_tracking_admission: FactorTrackingAdmissionClient | None
    unit_log_client: UnitLogClient | None
    unit_log_access_audit: ServiceLogAccessAudit | None
    unit_log_gate: threading.BoundedSemaphore
    backfill_plan_commands: BackfillPlanCommandGateway
    audit_report_commands: AuditReportCommandGateway
    formula_market_commands: FormulaMarketCommandGateway
    lab_controls: LabControlGateway
    research_query_client: QueryPrivateClient | None
    research_query_save_client: QueryPrivateClient | None


def create_app(
    settings: WebSettings,
    *,
    tracker: GenerationTracker | None = None,
    clock: Callable[[], datetime] = _utc_now,
    background: bool = True,
    pool_command_transport: PoolCommandTransport | None = None,
    nl_parser: ScreenPlanParser | None = None,
    ack_lookup_transport: AckLookupTransport | None = None,
    ack_admission_client: AckAdmissionClient | None = None,
    watchlist_admission_client: WatchlistAdmissionClient | None = None,
    price_alert_admission_client: PriceAlertAdmissionClient | None = None,
    factor_admission_client: FactorDefinitionAdmissionClient | None = None,
    factor_run_admission_client: FactorRunAdmissionClient | None = None,
    factor_tracking_admission_client: FactorTrackingAdmissionClient | None = None,
    unit_log_client: UnitLogClient | None = None,
    unit_log_access_audit: ServiceLogAccessAudit | None = None,
    backfill_plan_command_transport: BackfillPlanCommandTransport | None = None,
    audit_report_command_transport: AuditReportCommandTransport | None = None,
    formula_market_command_transport: FormulaMarketCommandTransport | None = None,
    lab_control_command_transport: LabControlTransport | None = None,
    research_query_client: QueryPrivateClient | None = None,
    research_query_save_client: QueryPrivateClient | None = None,
) -> FastAPI:
    """Build the app. Nothing is opened until the first request or startup."""

    generation_tracker = tracker or GenerationTracker(
        settings.serving_root,
        pointer_check_seconds=settings.pointer_check_seconds,
    )

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        anyio.to_thread.current_default_thread_limiter().total_tokens = settings.worker_threads
        await anyio.to_thread.run_sync(generation_tracker.refresh)
        if background:
            generation_tracker.start(settings.background_check_seconds)
        try:
            yield
        finally:
            generation_tracker.close()

    app = FastAPI(
        title=API_TITLE,
        version=API_VERSION,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )
    cursor_key = secrets.token_bytes(32)
    configured_research_query = research_query_client
    configured_research_query_save = research_query_save_client
    if settings.research_query_socket_path is not None:
        from rquant.research_query.service import QueryPrivateClient

        configured_research_query = configured_research_query or QueryPrivateClient(
            settings.research_query_socket_path,
            expected_service_uid=settings.research_query_service_uid,
            shared_gid=settings.research_query_shared_gid,
        )
        if settings.research_query_save_socket_path is not None:
            configured_research_query_save = configured_research_query_save or QueryPrivateClient(
                settings.research_query_save_socket_path,
                expected_service_uid=settings.research_query_save_service_uid,
                shared_gid=settings.research_query_shared_gid,
            )
    screen_replica = (
        VerifiedReplicaScreenSource(
            primary_path=settings.screen_primary_path,
            replica_path=settings.screen_replica_path,
        )
        if settings.screen_primary_path is not None and settings.screen_replica_path is not None
        else None
    )
    screen_history = (
        VerifiedFormulaHistoryProjection(settings.screen_history_root)
        if settings.screen_history_root is not None
        else None
    )
    screen_rsi = (
        VerifiedDynamicRsiProjection(settings.screen_rsi_root)
        if settings.screen_rsi_root is not None and screen_replica is not None
        else None
    )
    configured_ack_admission = None
    if settings.ack_admission_socket_path is not None:
        from rquant.alert_ack_admission import AckAdmissionClient

        configured_ack_admission = (
            ack_admission_client
            if ack_admission_client is not None
            else AckAdmissionClient(settings.ack_admission_socket_path)
        )
    configured_watchlist_admission = None
    if settings.watchlist_admission_socket_path is not None:
        from rquant.watchlist_admission import WatchlistAdmissionClient

        configured_watchlist_admission = (
            watchlist_admission_client
            if watchlist_admission_client is not None
            else WatchlistAdmissionClient(settings.watchlist_admission_socket_path)
        )
    configured_price_alert_admission = None
    if settings.price_alert_admission_socket_path is not None:
        from rquant.price_alert_admission import PriceAlertAdmissionClient

        configured_price_alert_admission = (
            price_alert_admission_client
            or PriceAlertAdmissionClient(
                settings.price_alert_admission_socket_path,
                expected_service_uid=settings.price_alert_admission_service_uid,
                shared_gid=settings.price_alert_admission_shared_gid,
            )
        )
    configured_factor_admission = None
    if settings.factor_admission_socket_path is not None:
        from rquant.factor_definition_admission import FactorDefinitionAdmissionClient

        configured_factor_admission = (
            factor_admission_client
            if factor_admission_client is not None
            else FactorDefinitionAdmissionClient(
                settings.factor_admission_socket_path,
                expected_service_uid=settings.factor_admission_service_uid,
                shared_gid=settings.factor_admission_shared_gid,
            )
        )
    configured_factor_run = None
    if settings.factor_run_admission_socket_path is not None:
        from rquant.factor_run_admission import FactorRunAdmissionClient

        configured_factor_run = factor_run_admission_client or FactorRunAdmissionClient(
            settings.factor_run_admission_socket_path,
            expected_service_uid=settings.factor_run_admission_service_uid,
            shared_gid=settings.factor_run_admission_shared_gid,
        )
    configured_factor_tracking = None
    if settings.factor_tracking_admission_socket_path is not None:
        from rquant.factor_tracking_admission import FactorTrackingAdmissionClient

        configured_factor_tracking = (
            factor_tracking_admission_client
            or FactorTrackingAdmissionClient(
                settings.factor_tracking_admission_socket_path,
                expected_service_uid=settings.factor_tracking_admission_service_uid,
                shared_gid=settings.factor_tracking_admission_shared_gid,
            )
        )
    configured_unit_log = None
    if settings.unit_log_socket_path is not None:
        assert settings.unit_log_service_uid is not None
        assert settings.unit_log_web_group_gid is not None
        configured_unit_log = (
            unit_log_client
            if unit_log_client is not None
            else UnitLogClient(
                socket_path=settings.unit_log_socket_path,
                service_uid=settings.unit_log_service_uid,
                web_group_gid=settings.unit_log_web_group_gid,
            )
        )
    configured_nl_parser = nl_parser
    if configured_nl_parser is None and settings.nl_openai_api_key is not None:
        assert settings.nl_openai_model is not None
        configured_nl_parser = OpenAiScreenPlanParser(
            api_key=settings.nl_openai_api_key,
            model=settings.nl_openai_model,
        )
    app.state.web = WebContext(
        settings=settings,
        proxy_identity=(
            None
            if settings.proxy_proof_file is None
            else ProxyIdentityVerifier.load(settings.proxy_proof_file)
        ),
        tracker=generation_tracker,
        clock=clock,
        cursor_key=cursor_key,
        screen_gate=threading.BoundedSemaphore(1),
        screen_service=ScreenApplicationService(
            cursor_key=cursor_key,
            replica=screen_replica,
            history=screen_history,
            rsi=screen_rsi,
        ),
        pool_commands=PoolCommandGateway(
            endpoint=settings.page_control_url,
            transport=pool_command_transport,
        ),
        nl_parser=configured_nl_parser,
        nl_gate=threading.BoundedSemaphore(1),
        nl_rate_limiter=PoolNlRateLimiter(),
        ack_lookup=AckLookupGateway(
            endpoint=settings.page_control_url,
            transport=ack_lookup_transport,
        ),
        ack_admission=configured_ack_admission,
        watchlist_admission=configured_watchlist_admission,
        price_alert_admission=configured_price_alert_admission,
        factor_admission=configured_factor_admission,
        factor_run_admission=configured_factor_run,
        factor_tracking_admission=configured_factor_tracking,
        unit_log_client=configured_unit_log,
        unit_log_access_audit=unit_log_access_audit,
        unit_log_gate=threading.BoundedSemaphore(1),
        backfill_plan_commands=BackfillPlanCommandGateway(
            endpoint=settings.page_control_url,
            transport=backfill_plan_command_transport,
        ),
        audit_report_commands=AuditReportCommandGateway(
            endpoint=settings.page_control_url,
            transport=audit_report_command_transport,
        ),
        formula_market_commands=FormulaMarketCommandGateway(
            endpoint=settings.page_control_url,
            transport=formula_market_command_transport,
        ),
        lab_controls=LabControlGateway(
            endpoint=settings.page_control_url,
            transport=lab_control_command_transport,
        ),
        research_query_client=configured_research_query,
        research_query_save_client=configured_research_query_save,
    )
    app.add_middleware(GZipMiddleware, minimum_size=1024)

    @app.middleware("http")
    async def security_headers(request: Request, call_next: Callable[..., Any]) -> Response:
        body_limit = _WRITE_BODY_LIMITS.get(request.url.path)
        if body_limit is None and _FACTOR_ARCHIVE_WRITE.fullmatch(request.url.path):
            body_limit = factors.MAX_ARCHIVE_REQUEST_BYTES
        if body_limit is None and _FACTOR_SAVE_WRITE.fullmatch(request.url.path):
            body_limit = factor_saves.MAX_SAVE_REQUEST_BYTES
        if body_limit is None and _FACTOR_RUN_WRITE.fullmatch(request.url.path):
            body_limit = factor_runs.MAX_RUN_REQUEST_BYTES
        if request.url.path in (
            "/api/v1/factors/tracking/commands",
            "/api/v1/factors/tracking/commands/resume",
            "/api/v1/factors/tracking/commands/retry",
        ):
            body_limit = factor_tracking.MAX_TRACKING_REQUEST_BYTES
        if request.method == "POST" and body_limit is not None:
            content_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
            if content_type != "application/json":
                return JSONResponse(
                    status_code=415,
                    content={"detail": "写接口只接受 application/json"},
                    headers={"X-Content-Type-Options": "nosniff", "Cache-Control": "no-store"},
                )
            parts: list[bytes] = []
            total = 0
            async for part in request.stream():
                total += len(part)
                if total > body_limit:
                    return JSONResponse(
                        status_code=413,
                        content={"detail": "请求内容过长，请删减后重试。"},
                        headers={"X-Content-Type-Options": "nosniff", "Cache-Control": "no-store"},
                    )
                parts.append(part)
            request._body = b"".join(parts)
        response: Response = await call_next(request)
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("Cache-Control", "no-store")
        return response

    @app.exception_handler(Exception)
    async def unexpected_error(_request: Request, _error: Exception) -> JSONResponse:
        return JSONResponse(status_code=500, content={"detail": "网页 API 内部错误"})

    @app.exception_handler(RequestValidationError)
    async def request_validation_error(
        request: Request,
        error: RequestValidationError,
    ) -> Response:
        if request.url.path.startswith("/api/v1/research/"):
            return JSONResponse(
                status_code=422, content={"detail": "查询或保存内容有误，请检查 SQL 和名称。"}
            )
        if request.url.path == "/api/v1/screen/tdx/preview":
            return JSONResponse(
                status_code=422,
                content={"detail": "预览输入有误，请检查股票、日期和公式。"},
            )
        if request.url.path == "/api/v1/screen/tdx/parse":
            return JSONResponse(
                status_code=422,
                content={"detail": "公式输入有误，请只填写文本公式。"},
            )
        if request.url.path == "/api/v1/screen/nl-preview":
            return JSONResponse(status_code=422, content={"detail": "条件描述有误，请检查后重试。"})
        if request.url.path == "/api/v1/pools/editor/commands":
            return JSONResponse(status_code=422, content={"detail": "编辑内容有误，请检查后重试。"})
        if request.url.path == "/api/v1/pools/editor/nl-preview":
            return JSONResponse(status_code=422, content={"detail": "修改描述有误，请检查后重试。"})
        if request.url.path == "/api/v1/monitor/ack":
            return JSONResponse(status_code=422, content={"detail": "确认请求有误，请刷新后重试。"})
        if request.url.path == "/api/v1/watchlist/commands":
            return JSONResponse(status_code=422, content={"detail": "名单请求有误，请检查后重试。"})
        if request.url.path == "/api/v1/data/backfill-plans/commands":
            return JSONResponse(status_code=422, content={"detail": "计划日期有误，请检查后重试。"})
        if request.url.path == "/api/v1/data/audit-report/commands":
            return JSONResponse(status_code=422, content={"detail": "审计日期有误，请检查后重试。"})
        if request.url.path == "/api/v1/screen/tdx/market/commands":
            return JSONResponse(
                status_code=422, content={"detail": "选股输入有误，请检查日期和公式。"}
            )
        if request.url.path == "/api/v1/pools/formula/commands":
            return JSONResponse(
                status_code=422, content={"detail": "保存信息有误，请检查名称和任务。"}
            )
        if request.url.path == "/api/v1/tasks/jobs/commands":
            return JSONResponse(
                status_code=422, content={"detail": "任务操作请求有误，请刷新后重试。"}
            )
        if request.url.path.startswith("/api/v1/watchlist/"):
            return JSONResponse(status_code=422, content={"detail": "股票代码有误，请检查后重试。"})
        return await request_validation_exception_handler(request, error)

    app.include_router(meta.router, prefix="/api/v1", tags=["meta"])
    app.include_router(research_query.router, prefix="/api/v1", tags=["research"])
    app.include_router(overview.router, prefix="/api/v1", tags=["overview"])
    app.include_router(pools.router, prefix="/api/v1", tags=["pools"])
    private = [Depends(require_current_user)]
    app.include_router(pool_editor.router, prefix="/api/v1", tags=["pools"], dependencies=private)
    app.include_router(paper.router, prefix="/api/v1", tags=["paper"], dependencies=private)
    app.include_router(monitor.router, prefix="/api/v1", tags=["monitor"], dependencies=private)
    app.include_router(manual_watchlist.router, prefix="/api/v1", tags=["watchlist"])
    app.include_router(price_alert_rules.router, prefix="/api/v1", tags=["monitor"])
    app.include_router(tasks.router, prefix="/api/v1", tags=["tasks"], dependencies=private)
    app.include_router(
        tasks_controls.router, prefix="/api/v1", tags=["tasks"], dependencies=private
    )
    app.include_router(service_logs.router, prefix="/api/v1", tags=["tasks"])
    app.include_router(backtests.router, prefix="/api/v1", tags=["backtests"], dependencies=private)
    app.include_router(
        experiments.router, prefix="/api/v1", tags=["experiments"], dependencies=private
    )
    app.include_router(
        strategies.router, prefix="/api/v1", tags=["strategies"], dependencies=private
    )
    app.include_router(factors.router, prefix="/api/v1", tags=["factors"], dependencies=private)
    app.include_router(factor_runs.router, prefix="/api/v1", tags=["factors"], dependencies=private)
    app.include_router(
        factor_tracking.router, prefix="/api/v1", tags=["factors"], dependencies=private
    )
    app.include_router(
        factor_saves.router, prefix="/api/v1", tags=["factors"], dependencies=private
    )
    app.include_router(
        factor_results.router, prefix="/api/v1", tags=["factors"], dependencies=private
    )
    app.include_router(health.router, prefix="/api/v1", tags=["health"])
    app.include_router(panorama.router, prefix="/api/v1", tags=["panorama"])
    app.include_router(screen.router, prefix="/api/v1", tags=["screen"])
    app.include_router(
        formula_market_commands.router, prefix="/api/v1", tags=["screen"], dependencies=private
    )
    app.include_router(
        formula_market_read.router, prefix="/api/v1", tags=["screen"], dependencies=private
    )
    app.include_router(
        formula_pool_read.router, prefix="/api/v1", tags=["pools"], dependencies=private
    )
    app.include_router(
        formula_pool_save_commands.router, prefix="/api/v1", tags=["pools"], dependencies=private
    )
    app.include_router(stocks.router, prefix="/api/v1", tags=["stocks"])
    app.include_router(catalog.router, prefix="/api/v1", tags=["catalog"])
    app.include_router(data_audit.router, prefix="/api/v1", tags=["data-audit"])
    app.include_router(
        data_audit_report.router, prefix="/api/v1", tags=["data-audit"], dependencies=private
    )
    app.include_router(
        data_audit_report_calendar.router,
        prefix="/api/v1",
        tags=["data-audit"],
        dependencies=private,
    )
    app.include_router(
        data_audit_report_commands.router,
        prefix="/api/v1",
        tags=["data-audit"],
        dependencies=private,
    )
    app.include_router(fundamentals.router, prefix="/api/v1", tags=["data-center"])
    app.include_router(
        backfill_plans.router, prefix="/api/v1", tags=["data-audit"], dependencies=private
    )
    app.include_router(
        backfill_plan_commands.router,
        prefix="/api/v1",
        tags=["data-audit"],
        dependencies=private,
    )
    return app


def openapi_document(app: FastAPI) -> str:
    """The canonical OpenAPI JSON the front end's types are generated from."""

    return json.dumps(app.openapi(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"
