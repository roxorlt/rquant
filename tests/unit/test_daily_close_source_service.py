from __future__ import annotations

import ast
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from rquant.daily_close_gateway import (
    DailyCloseGateway,
    DailyCloseGatewayConfig,
    DailyCloseSourceRequest,
)
from rquant.daily_close_source_service import capture_daily_close_step
from rquant.live_contracts import LiveChannel
from rquant.live_spool import LiveBatchSpool

TRADE_DATE = date(2026, 7, 31)
OBSERVED_AT = datetime(2026, 7, 31, 9, 5, tzinfo=UTC)


def _snapshot(*, partial: tuple[str, ...] = ()) -> dict[str, object]:
    return {
        "daily_bar": (
            {
                "ts_code": "600000.SH",
                "trade_date": TRADE_DATE,
                "open": 10.0,
                "high": 10.4,
                "low": 9.9,
                "close": 10.2,
                "pre_close": 9.95,
                "change": 0.25,
                "pct_chg": 2.5126,
                "vol": 1_000.0,
                "amount": 10_200.0,
            },
        ),
        "daily_basic": (
            {
                "ts_code": "600000.SH",
                "trade_date": TRADE_DATE,
                "turnover_rate": 0.5,
                "volume_ratio": 1.2,
                "total_mv": 200_000.0,
                "circ_mv": 180_000.0,
            },
        ),
        "adj_factor": (
            {
                "ts_code": "600000.SH",
                "trade_date": TRADE_DATE,
                "adj_factor": 1.01,
            },
        ),
        "index_daily": (
            {
                "ts_code": "000001.SH",
                "trade_date": TRADE_DATE,
                "open": 3200.0,
                "high": 3230.0,
                "low": 3190.0,
                "close": 3220.0,
                "pre_close": 3198.0,
                "change": 22.0,
                "pct_chg": 0.688,
                "vol": 2_000.0,
                "amount": 30_000.0,
            },
        ),
        "security_status": (
            {
                "ts_code": "600000.SH",
                "trade_date": TRADE_DATE,
                "name": "浦发银行",
                "is_st": False,
                "listing_status": "L",
            },
        ),
        "suspension_status": (),
        "partial_datasets": partial,
    }


def _gateway(tmp_path: Path, fetcher) -> DailyCloseGateway:
    return DailyCloseGateway(
        spool=LiveBatchSpool(tmp_path / "live"),
        fetcher=fetcher,
        config=DailyCloseGatewayConfig(
            producer_version="daily-close-v1",
            producer_commit="b" * 40,
        ),
        completion_clock=lambda: OBSERVED_AT + timedelta(seconds=1),
    )


def test_source_step_exposes_one_shareable_watermark_and_is_idempotent(tmp_path: Path) -> None:
    calls = 0

    def fetch(_request: DailyCloseSourceRequest) -> object:
        nonlocal calls
        calls += 1
        return _snapshot()

    gateway = _gateway(tmp_path, fetch)

    first = capture_daily_close_step(
        gateway,
        trade_date=TRADE_DATE,
        observed_at=OBSERVED_AT,
    )
    replay = capture_daily_close_step(
        gateway,
        trade_date=TRADE_DATE,
        observed_at=OBSERVED_AT,
    )

    descriptor = gateway.spool.source_descriptor(LiveChannel.DAILY_CLOSE)
    assert calls == 1
    assert first.processed_count == 1
    assert replay.processed_count == 0
    assert first.output_sequence == replay.output_sequence == 0
    assert first.source_generations == {"daily_close": descriptor.generation_id}
    assert first.degraded_reasons == ()


def test_source_step_reports_partial_and_quarantine_without_side_effects(tmp_path: Path) -> None:
    partial = _gateway(
        tmp_path / "partial",
        lambda _request: _snapshot(partial=("daily_basic",)),
    )
    invalid_payload = _snapshot()
    invalid_payload.pop("adj_factor")
    quarantined = _gateway(
        tmp_path / "quarantine",
        lambda _request: invalid_payload,
    )

    partial_result = capture_daily_close_step(
        partial,
        trade_date=TRADE_DATE,
        observed_at=OBSERVED_AT,
    )
    quarantine_result = capture_daily_close_step(
        quarantined,
        trade_date=TRADE_DATE,
        observed_at=OBSERVED_AT,
    )

    assert partial_result.degraded_reasons == ("daily_close:degraded:partial:daily_basic",)
    assert quarantine_result.output_sequence == -1
    assert quarantine_result.processed_count == 1
    assert quarantine_result.degraded_reasons == ("daily_close:quarantined:invalid_payload",)
    assert not list((tmp_path / "quarantine").rglob("*.duckdb"))


def test_source_service_has_no_production_database_notification_or_strategy_dependency() -> None:
    source_path = Path(__file__).parents[2] / "src/rquant/daily_close_source_service.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name.lower() for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            module = (node.module or "").lower()
            imported.add(module)
            imported.update(f"{module}.{alias.name.lower()}" for alias in node.names)

    forbidden_fragments = ("duckdb", "storage", "notif", "strategy", "pipeline", "ingest")
    assert all(
        fragment not in imported_name
        for imported_name in imported
        for fragment in forbidden_fragments
    )
