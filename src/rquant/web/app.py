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
from rquant.web.source import ResearchSource, ServingSource, Source, SourceUnavailableError

DIST = Path(__file__).resolve().parents[3] / "web" / "dist"


def _version() -> str:
    try:
        return version("rquant")
    except PackageNotFoundError:
        return "dev"


def _default_commands_dir(research_dir: Path) -> Path:
    env_dir = os.environ.get("RQUANT_WEB_COMMANDS_DIR")
    if env_dir:
        return Path(env_dir)
    home_default = Path.home() / "rquant-web-commands"
    if home_default.is_dir():
        return home_default
    return research_dir / "web-commands"


def create_app(
    source: Source | None = None,
    dist: Path | None = DIST,
    research_root: Path | None = None,
    l2_cache_root: Path | None = None,
    commands_dir: Path | None = None,
) -> FastAPI:
    app = FastAPI(title="rQuant web API", version=_version())
    #: portfolio backtest results (files); None → $RQUANT_RESEARCH_ROOT or data/research
    app.state.research_root = research_root
    if commands_dir is not None:
        app.state.commands_dir = Path(commands_dir)
    app.add_middleware(GZipMiddleware, minimum_size=2048)
    if source is None:
        from rquant.backtest.store import research_root as _rr

        r_root = Path(research_root or _rr())
        if os.environ.get("RQUANT_WEB_WARM", "1") == "1":
            import threading

            from rquant.web.market_live import warm_market_live

            threading.Thread(
                target=warm_market_live,
                args=(r_root,),
                daemon=True,
                name="market-live-warm",
            ).start()
        mode = os.environ.get("RQUANT_WEB_SOURCE", "").strip().lower()
        serving_env = os.environ.get("RQUANT_SERVING_ROOT")
        if mode == "research":
            source = ResearchSource(r_root, commands_dir=_default_commands_dir(r_root))
        elif mode == "serving" or serving_env:
            root = serving_env
            if not root:
                from rquant.serving_paths import serving_root_from_env

                root = str(serving_root_from_env())
            source = ServingSource(root)
        else:
            from rquant.serving_paths import serving_root_from_env

            s_root = serving_root_from_env()
            if (s_root / "current.json").is_file():
                source = ServingSource(s_root)
            else:
                source = ResearchSource(r_root, commands_dir=_default_commands_dir(r_root))
    app.state.source = source

    @app.exception_handler(SourceUnavailableError)
    async def _unavailable(_: Request, exc: SourceUnavailableError) -> JSONResponse:
        return JSONResponse({"detail": f"数据暂不可用：{exc}"}, status_code=503)

    app.include_router(router)
    l2_router = None
    raw_root = l2_cache_root or os.environ.get("RQUANT_L2_RAW_ROOT")
    if raw_root:
        from rquant.l2.cache import RawL2Cache
        from rquant.web.l2 import router as l2_router

        app.state.l2_cache = RawL2Cache(Path(raw_root))
        app.include_router(l2_router)
    if dist is not None and (dist / "index.html").is_file():
        # The page calls <page dir>/api/v1/... (nginx maps /app/api/ -> /api/ in production).
        app.include_router(router, prefix="/app", include_in_schema=False)
        if l2_router is not None:
            app.include_router(l2_router, prefix="/app", include_in_schema=False)
        app.mount("/app", StaticFiles(directory=dist, html=True), name="app")

        @app.get("/", include_in_schema=False)
        def _root() -> RedirectResponse:
            return RedirectResponse("/app/")

    return app
