"""``create_app()``: the read-only FastAPI application behind ``/app/api/``.

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
from typing import Any

import anyio.to_thread
from fastapi import FastAPI, Request, Response
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse

from rquant.web.routes import catalog, health, meta, overview, panorama, screen, stocks
from rquant.web.serving import GenerationTracker
from rquant.web.settings import WebSettings

API_TITLE = "rQuant Web API"
#: Version of the HTTP contract, bumped by hand; not the package version, so that a
#: release that does not touch the API leaves the OpenAPI snapshot unchanged.
API_VERSION = "1"


def _utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True)
class WebContext:
    settings: WebSettings
    tracker: GenerationTracker
    clock: Callable[[], datetime]
    cursor_key: bytes
    screen_gate: threading.BoundedSemaphore


def create_app(
    settings: WebSettings,
    *,
    tracker: GenerationTracker | None = None,
    clock: Callable[[], datetime] = _utc_now,
    background: bool = True,
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
    app.state.web = WebContext(
        settings=settings,
        tracker=generation_tracker,
        clock=clock,
        cursor_key=secrets.token_bytes(32),
        screen_gate=threading.BoundedSemaphore(1),
    )
    app.add_middleware(GZipMiddleware, minimum_size=1024)

    @app.middleware("http")
    async def security_headers(request: Request, call_next: Callable[..., Any]) -> Response:
        response: Response = await call_next(request)
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("Cache-Control", "no-store")
        return response

    @app.exception_handler(Exception)
    async def unexpected_error(_request: Request, _error: Exception) -> JSONResponse:
        return JSONResponse(status_code=500, content={"detail": "网页 API 内部错误"})

    app.include_router(meta.router, prefix="/api/v1", tags=["meta"])
    app.include_router(overview.router, prefix="/api/v1", tags=["overview"])
    app.include_router(health.router, prefix="/api/v1", tags=["health"])
    app.include_router(panorama.router, prefix="/api/v1", tags=["panorama"])
    app.include_router(screen.router, prefix="/api/v1", tags=["screen"])
    app.include_router(stocks.router, prefix="/api/v1", tags=["stocks"])
    app.include_router(catalog.router, prefix="/api/v1", tags=["catalog"])
    return app


def openapi_document(app: FastAPI) -> str:
    """The canonical OpenAPI JSON the front end's types are generated from."""

    return json.dumps(app.openapi(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"
