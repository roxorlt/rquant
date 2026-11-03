"""Shared valid-observation fixtures for the L2 cold-store/retention tests.

Every body is a committed real fixture (no synthetic frame layout), so the
cold-store decode path runs the same reviewed codec as the live feed.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from pathlib import Path

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"

ORDER640 = FIXTURES / "l2-order640.body"
TRADE763 = FIXTURES / "l2_trade_push_763.bin"
SEED_ORDER = FIXTURES / "l2-seed-order.body"
SEED_TRADE = FIXTURES / "l2-seed-trade.body"

_TEMPLATES = (
    # (fixture, kind, page, code, market, side, request_id, fd, delivery)
    (ORDER640, "vendor4027", 10199, "600276", 17, "sell", 0xFFFFFFFF, 1, "candidate_push"),
    (TRADE763, "vendor4013", 10237, "601899", 17, None, 0xFFFFFFFF, 3, "candidate_push"),
    (SEED_ORDER, "vendor4027", 10118, "603258", 17, "buy", 4, 5, "seed"),
    (SEED_TRADE, "vendor4013", 10117, "603258", 17, None, 3, 4, "seed"),
)


def _decode(template):
    from rquant.l2._codec import decode_order_response, decode_trade_response

    fixture, kind, page, code, market, side, request_id, fd, _delivery = template
    body = fixture.read_bytes()
    kwargs = dict(request_id=request_id, page_id=page, code=code, source_fd_ordinal=fd)
    if kind == "vendor4013":
        result = decode_trade_response(body, market=market, **kwargs)
    else:
        result = decode_order_response(body, **kwargs)
    return body, result


def observations(count: int = 24, *, epoch: str = "epoch_cold_a",
                 start: datetime | None = None) -> list:
    """Return ``count`` valid market observations cycling the four fixtures."""
    from rquant.l2.models import MarketObservation

    clock = start or datetime(2026, 11, 3, 1, 30, tzinfo=UTC)
    built = []
    for index in range(count):
        template = _TEMPLATES[index % len(_TEMPLATES)]
        body, result = _decode(template)
        fixture, kind, page, code, market, side, request_id, fd, delivery = template
        built.append(MarketObservation(
            epoch_id=epoch,
            binding={"page_id": page, "kind": kind, "code": code, "market": market, "side": side},
            received_at=clock + timedelta(seconds=index, microseconds=index * 137),
            request_id=request_id,
            source_fd_ordinal=fd,
            delivery=delivery,
            response_body_hex=body.hex(),
            raw_table_hex=result["raw_table"].hex(),
            table=result["table"],
            response_sha256=result["response_sha256"],
        ))
    return built


def build_cache(root: Path, count: int = 24, **kwargs) -> Path:
    """Write ``count`` committed batches into ``root`` and return the cache root."""
    from rquant.l2.cache import RawL2Cache

    cache_root = Path(root) / "cache"
    cache = RawL2Cache(cache_root)
    with cache.writer():
        for observation in observations(count, **kwargs):
            cache.append(observation)
    return cache_root


def payload_bytes(cache_root: Path, sequence: int) -> bytes:
    return Path(cache_root).joinpath("batches", f"{sequence:020d}", "batch.json").read_bytes()


def payload_sha(cache_root: Path, sequence: int) -> str:
    return hashlib.sha256(payload_bytes(cache_root, sequence)).hexdigest()
