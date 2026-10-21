"""Standalone loopback HTTP access to the immutable raw L2 cache."""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Sequence
from pathlib import Path

from anyio import CancelScope
from fastapi import FastAPI, WebSocket
from starlette.websockets import WebSocketState

from rquant.l2.cache import MAX_BATCH_PAGE, CacheError, RawL2Cache
from rquant.l2.models import BatchPage, CapacityPolicy
from rquant.l2.subscriptions import SubscriptionError, SubscriptionStore, load_capacity_policy
from rquant.l2.symbols import MAX_SYMBOLS
from rquant.web.l2 import router

_WS_MAX_CONNECTIONS = 8
_WS_POLL_SECONDS = .2
_WS_HEARTBEAT_SECONDS = 5.0
_WS_SEND_TIMEOUT_SECONDS = 5.0


def _stream_parameters(websocket: WebSocket) -> tuple[int, int]:
    items = websocket.query_params.multi_items()
    values = dict(items)
    if "origin" in websocket.headers or len(items) != len(values) or set(values) - {"after_sequence", "limit"}:
        raise ValueError
    if any(not value or len(value) > 20 or not value.isascii() or not value.isdecimal() for value in values.values()):
        raise ValueError
    cursor, limit = int(values.get("after_sequence", "0")), int(values.get("limit", "1"))
    if not 1 <= limit <= MAX_BATCH_PAGE:
        raise ValueError
    return cursor, limit


def create_app(cache_root: Path, control_root: Path | None = None, max_symbols: int = 20,
               policy_metadata: CapacityPolicy | None = None) -> FastAPI:
    if type(max_symbols) is not int or not 1 <= max_symbols <= MAX_SYMBOLS:
        raise ValueError("invalid symbol capacity")
    if policy_metadata is not None and policy_metadata.max_symbols != max_symbols:
        raise ValueError("capacity policy mismatch")
    app = FastAPI(title="rQuant raw L2 cache", docs_url=None, redoc_url=None, openapi_url=None)
    app.state.l2_cache = RawL2Cache(cache_root)
    app.state.l2_ws_connections = 0
    app.include_router(router)

    @app.websocket("/api/v1/l2/stream")
    async def stream(websocket: WebSocket) -> None:
        try:
            cursor, limit = _stream_parameters(websocket)
        except ValueError:
            await websocket.close(1008, "invalid_stream_request")
            return
        if app.state.l2_ws_connections >= _WS_MAX_CONNECTIONS:
            await websocket.close(1013, "stream_capacity")
            return
        app.state.l2_ws_connections += 1
        read_task: asyncio.Task[BatchPage] | None = None
        sender: asyncio.Task[None] | None = None
        receiver: asyncio.Task[bool] | None = None
        close_code: int | None = 1000
        reason = ""

        async def read_page(after: int) -> BatchPage:
            nonlocal read_task
            read_task = asyncio.create_task(asyncio.to_thread(app.state.l2_cache.read_batches,
                                                             after_sequence=after, limit=limit))
            try:
                return await asyncio.shield(read_task)
            finally:
                if read_task.done():
                    read_task = None

        async def send_pages() -> None:
            nonlocal cursor
            heartbeat = 0.0
            while True:
                page = await read_page(cursor)
                has_batches = bool(page.batches)
                if has_batches or asyncio.get_running_loop().time() >= heartbeat:
                    next_sequence = page.next_sequence
                    payload = page.model_dump_json()
                    del page
                    try:
                        await asyncio.wait_for(websocket.send_text(payload), _WS_SEND_TIMEOUT_SECONDS)
                    finally:
                        del payload
                    cursor = next_sequence
                    heartbeat = asyncio.get_running_loop().time() + _WS_HEARTBEAT_SECONDS
                else:
                    del page
                if not has_batches:
                    await asyncio.sleep(_WS_POLL_SECONDS)

        async def receive_client() -> bool:
            message = await websocket.receive()
            return message["type"] != "websocket.disconnect"

        try:
            await websocket.accept()
            sender = asyncio.create_task(send_pages())
            receiver = asyncio.create_task(receive_client())
            done, _ = await asyncio.wait((sender, receiver), return_when=asyncio.FIRST_COMPLETED)
            if receiver in done:
                close_code, reason = (1008, "client_data_not_allowed") if receiver.result() else (None, "")
            else:
                sender.result()
        except TimeoutError:
            close_code, reason = 1013, "stream_send_timeout"
        except ValueError:
            close_code, reason = 1008, "invalid_cursor"
        except CacheError:
            close_code, reason = 1011, "cache_unavailable"
        except Exception:
            close_code, reason = 1011, "stream_unavailable"
        finally:
            async def cleanup() -> None:
                tasks = [task for task in (sender, receiver) if task is not None]
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                if read_task is not None:
                    await asyncio.gather(read_task, return_exceptions=True)
                if (close_code is not None and websocket.application_state == WebSocketState.CONNECTED
                        and websocket.client_state != WebSocketState.DISCONNECTED):
                    try:
                        await asyncio.wait_for(websocket.close(close_code, reason), _WS_SEND_TIMEOUT_SECONDS)
                    except Exception:
                        pass
                app.state.l2_ws_connections -= 1

            # AnyIO level cancellation and direct asyncio cancellation need both shields.
            cancellation: asyncio.CancelledError | None = None
            with CancelScope(shield=True):
                cleanup_task = asyncio.create_task(cleanup())
                while not cleanup_task.done():
                    try:
                        await asyncio.shield(cleanup_task)
                    except asyncio.CancelledError as error:
                        cancellation = error
                cleanup_task.result()
            if cancellation is not None:
                raise cancellation

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

    uvicorn.run(create_app(args.cache_root, args.control_root, maximum, policy), host="127.0.0.1", port=8771,
                ws="websockets", ws_max_size=1024, ws_max_queue=1)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
