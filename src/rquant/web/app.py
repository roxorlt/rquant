"""FastAPI app: /api/v1/* JSON + the built React app at /app/.

    uv run python -m rquant.web serve            # reads RQUANT_SERVING_ROOT
    uv run python -m rquant.web serve --fixture  # invented demo data, no Serving needed
    uv run python -m rquant.web openapi > web/src/api/openapi.json
"""

from __future__ import annotations

import os
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from rquant.web.routes import router
from rquant.web.source import ServingSource, Source, SourceUnavailableError

DIST = Path(__file__).resolve().parents[3] / "web" / "dist"


def _version() -> str:
    try:
        return version("rquant")
    except PackageNotFoundError:
        return "dev"


def create_app(source: Source | None = None, dist: Path | None = DIST,
               research_root: Path | None = None) -> FastAPI:
    app = FastAPI(title="rQuant web API", version=_version())
    #: portfolio backtest results (files); None → $RQUANT_RESEARCH_ROOT or data/research
    app.state.research_root = research_root
    app.add_middleware(GZipMiddleware, minimum_size=2048)
    if source is None:
        if os.environ.get("RQUANT_WEB_WARM", "1") == "1":
            import threading

            from rquant.backtest.store import research_root as _rr
            from rquant.web.market_live import warm_market_live

            threading.Thread(target=warm_market_live,
                             args=(Path(research_root or _rr()),), daemon=True,
                             name="market-live-warm").start()
        root = os.environ.get("RQUANT_SERVING_ROOT")
        if not root:
            from rquant.serving_paths import serving_root_from_env

            root = str(serving_root_from_env())
        source = ServingSource(root)
    app.state.source = source

    @app.exception_handler(SourceUnavailableError)
    async def _unavailable(_: Request, exc: SourceUnavailableError) -> JSONResponse:
        return JSONResponse({"detail": f"数据暂不可用：{exc}"}, status_code=503)

    app.include_router(router)
    if dist is not None and (dist / "index.html").is_file():
        # The page calls <page dir>/api/v1/... (nginx maps /app/api/ -> /api/ in production).
        app.include_router(router, prefix="/app", include_in_schema=False)
        app.mount("/app", StaticFiles(directory=dist, html=True), name="app")

        @app.get("/", include_in_schema=False)
        def _root() -> RedirectResponse:
            return RedirectResponse("/app/")

    return app
