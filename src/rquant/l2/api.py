"""Standalone loopback HTTP access to the immutable raw L2 cache."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

from fastapi import FastAPI

from rquant.l2.cache import RawL2Cache
from rquant.web.l2 import router


def create_app(cache_root: Path) -> FastAPI:
    app = FastAPI(title="rQuant raw L2 cache", docs_url=None, redoc_url=None, openapi_url=None)
    app.state.l2_cache = RawL2Cache(cache_root)
    app.include_router(router)
    return app


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read the local raw L2 cache over loopback HTTP")
    parser.add_argument("--cache-root", type=Path, required=True)
    args = parser.parse_args(argv)
    import uvicorn

    uvicorn.run(create_app(args.cache_root), host="127.0.0.1", port=8771)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
