"""``create_app()``: the FastAPI application behind ``/app/api/``.

nginx serves the static front end itself and proxies ``/app/api/`` here with the prefix
stripped, so this app only knows ``/api/v1/...``. No static files, no CORS, no docs pages
(the schema is exported by ``rquant web-openapi`` into ``web/src/api/openapi.json``).
"""

from __future__ import annotations

import json
import secrets
import threading
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import anyio.to_thread
from fastapi import FastAPI, Request, Response
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
from rquant.web.nl_parser import OpenAiScreenPlanParser, ScreenPlanParser
from rquant.web.pool_editor_gateway import PoolCommandGateway, PoolCommandTransport
from rquant.web.pool_nl_preview import PoolNlRateLimiter
from rquant.web.routes import (
    backfill_plan_commands,
    backfill_plans,
    backtests,
    catalog,
    data_audit,
    data_audit_report,
    data_audit_report_calendar,
    data_audit_report_commands,
    formula_market_commands,
    formula_market_read,
    formula_pool_read,
    formula_pool_save_commands,
    fundamentals,
    health,
    meta,
    monitor,
    overview,
    panorama,
    paper,
    pool_editor,
    pools,
    screen,
    service_logs,
    stocks,
    tasks,
)
from rquant.web.screen_service import ScreenApplicationService
from rquant.web.service_log_access_audit import ServiceLogAccessAudit
from rquant.web.serving import GenerationTracker
from rquant.web.settings import WebSettings

if TYPE_CHECKING:
    from rquant.alert_ack_admission import AckAdmissionClient

API_TITLE = "rQuant Web API"
#: Version of the HTTP contract, bumped by hand; not the package version, so that a
#: release that does not touch the API leaves the OpenAPI snapshot unchanged.
API_VERSION = "1"
_WRITE_BODY_LIMITS = {
    "/api/v1/pools/editor/commands": pool_editor.MAX_REQUEST_BYTES,
    "/api/v1/pools/editor/nl-preview": pool_editor.MAX_NL_REQUEST_BYTES,
    "/api/v1/monitor/ack": monitor.MAX_ACK_REQUEST_BYTES,
    "/api/v1/data/backfill-plans/commands": backfill_plan_commands.MAX_REQUEST_BYTES,
    "/api/v1/data/audit-report/commands": data_audit_report_commands.MAX_REQUEST_BYTES,
    "/api/v1/screen/tdx/market/commands": formula_market_commands.MAX_REQUEST_BYTES,
    "/api/v1/pools/formula/commands": formula_pool_save_commands.MAX_REQUEST_BYTES,
}


def _utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True)
class WebContext:
    settings: WebSettings
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
    unit_log_client: UnitLogClient | None
    unit_log_access_audit: ServiceLogAccessAudit | None
    unit_log_gate: threading.BoundedSemaphore
    backfill_plan_commands: BackfillPlanCommandGateway
    audit_report_commands: AuditReportCommandGateway
    formula_market_commands: FormulaMarketCommandGateway


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
    unit_log_client: UnitLogClient | None = None,
    unit_log_access_audit: ServiceLogAccessAudit | None = None,
    backfill_plan_command_transport: BackfillPlanCommandTransport | None = None,
    audit_report_command_transport: AuditReportCommandTransport | None = None,
    formula_market_command_transport: FormulaMarketCommandTransport | None = None,
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
    )
    app.add_middleware(GZipMiddleware, minimum_size=1024)

    @app.middleware("http")
    async def security_headers(request: Request, call_next: Callable[..., Any]) -> Response:
        if request.method == "POST" and request.url.path in _WRITE_BODY_LIMITS:
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
                if total > _WRITE_BODY_LIMITS[request.url.path]:
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
        if request.url.path == "/api/v1/pools/editor/commands":
            return JSONResponse(status_code=422, content={"detail": "编辑内容有误，请检查后重试。"})
        if request.url.path == "/api/v1/pools/editor/nl-preview":
            return JSONResponse(status_code=422, content={"detail": "修改描述有误，请检查后重试。"})
        if request.url.path == "/api/v1/monitor/ack":
            return JSONResponse(status_code=422, content={"detail": "确认请求有误，请刷新后重试。"})
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
        return await request_validation_exception_handler(request, error)

    app.include_router(meta.router, prefix="/api/v1", tags=["meta"])
    app.include_router(overview.router, prefix="/api/v1", tags=["overview"])
    app.include_router(pools.router, prefix="/api/v1", tags=["pools"])
    app.include_router(pool_editor.router, prefix="/api/v1", tags=["pools"])
    app.include_router(paper.router, prefix="/api/v1", tags=["paper"])
    app.include_router(monitor.router, prefix="/api/v1", tags=["monitor"])
    app.include_router(tasks.router, prefix="/api/v1", tags=["tasks"])
    app.include_router(service_logs.router, prefix="/api/v1", tags=["tasks"])
    app.include_router(backtests.router, prefix="/api/v1", tags=["backtests"])
    app.include_router(health.router, prefix="/api/v1", tags=["health"])
    app.include_router(panorama.router, prefix="/api/v1", tags=["panorama"])
    app.include_router(screen.router, prefix="/api/v1", tags=["screen"])
    app.include_router(formula_market_commands.router, prefix="/api/v1", tags=["screen"])
    app.include_router(formula_market_read.router, prefix="/api/v1", tags=["screen"])
    app.include_router(formula_pool_read.router, prefix="/api/v1", tags=["pools"])
    app.include_router(formula_pool_save_commands.router, prefix="/api/v1", tags=["pools"])
    app.include_router(stocks.router, prefix="/api/v1", tags=["stocks"])
    app.include_router(catalog.router, prefix="/api/v1", tags=["catalog"])
    app.include_router(data_audit.router, prefix="/api/v1", tags=["data-audit"])
    app.include_router(data_audit_report.router, prefix="/api/v1", tags=["data-audit"])
    app.include_router(data_audit_report_calendar.router, prefix="/api/v1", tags=["data-audit"])
    app.include_router(data_audit_report_commands.router, prefix="/api/v1", tags=["data-audit"])
    app.include_router(fundamentals.router, prefix="/api/v1", tags=["data-center"])
    app.include_router(backfill_plans.router, prefix="/api/v1", tags=["data-audit"])
    app.include_router(backfill_plan_commands.router, prefix="/api/v1", tags=["data-audit"])
    return app


def openapi_document(app: FastAPI) -> str:
    """The canonical OpenAPI JSON the front end's types are generated from."""

    return json.dumps(app.openapi(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"
