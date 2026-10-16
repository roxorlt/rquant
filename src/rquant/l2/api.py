"""Standalone loopback HTTP access to the immutable raw L2 cache."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

from fastapi import FastAPI

from rquant.l2.cache import RawL2Cache
from rquant.l2.models import CapacityPolicy
from rquant.l2.subscriptions import SubscriptionError, SubscriptionStore, load_capacity_policy
from rquant.l2.symbols import MAX_SYMBOLS
from rquant.web.l2 import router


def create_app(cache_root: Path, control_root: Path | None = None, max_symbols: int = 20,
               policy_metadata: CapacityPolicy | None = None) -> FastAPI:
    if type(max_symbols) is not int or not 1 <= max_symbols <= MAX_SYMBOLS:
        raise ValueError("invalid symbol capacity")
    if policy_metadata is not None and policy_metadata.max_symbols != max_symbols:
        raise ValueError("capacity policy mismatch")
    app = FastAPI(title="rQuant raw L2 cache", docs_url=None, redoc_url=None, openapi_url=None)
    app.state.l2_cache = RawL2Cache(cache_root)
    app.include_router(router)
    if control_root is not None:
        from rquant.l2.subscription_api import router as control_router

        app.state.l2_subscriptions = SubscriptionStore(control_root, max_symbols=max_symbols)
        app.state.l2_capacity_policy = policy_metadata
        app.include_router(control_router)
    return app


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read the local raw L2 cache over loopback HTTP")
    parser.add_argument("--cache-root", type=Path, required=True)
    parser.add_argument("--control-root", type=Path)
    capacity = parser.add_mutually_exclusive_group()
    capacity.add_argument("--max-symbols", type=int)
    capacity.add_argument("--capacity-file", type=Path)
    args = parser.parse_args(argv)
    policy = None
    if args.capacity_file is not None:
        try:
            policy = load_capacity_policy(args.capacity_file)
        except SubscriptionError:
            parser.error("invalid_capacity_policy")
    maximum = policy.max_symbols if policy is not None else args.max_symbols if args.max_symbols is not None else 20
    if not 1 <= maximum <= MAX_SYMBOLS:
        parser.error("invalid_symbol_capacity")
    import uvicorn

    uvicorn.run(create_app(args.cache_root, args.control_root, maximum, policy), host="127.0.0.1", port=8771)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
