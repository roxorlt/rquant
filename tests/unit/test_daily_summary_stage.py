from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from rquant.daily_pool_stage import DailyDownstreamArtifactStore
from rquant.daily_summary_stage import DailySummaryStage
from rquant.signal_bus import SignalBusStore
from rquant.storage.duckdb import DuckDBStore

NOW = datetime(2026, 8, 3, 9, 1, tzinfo=UTC)


def test_summary_stage_uses_deterministic_signal_identity_for_outbox_dedupe(tmp_path) -> None:
    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    stage = DailySummaryStage(
        signal_bus=bus,
        strategy_version="daily-close-dag/v1",
        producer_commit="a" * 40,
        clock=lambda: NOW,
        artifact_store=DailyDownstreamArtifactStore(tmp_path / "artifacts"),
        canonical_reader_factory=lambda: DuckDBStore(tmp_path / "unused.duckdb", read_only=True),
    )

    first = stage.build_signal(
        trade_date=date(2026, 8, 3),
        canonical_generation_id="b" * 64,
        canonical_receipt_id="c" * 64,
        canonical_content_hash="d" * 64,
        screen_hits={"n-shape-pool1": 2},
        pool2_active_count=1,
        errors=("screen:user/bad",),
    )
    replay = stage.build_signal(
        trade_date=date(2026, 8, 3),
        canonical_generation_id="b" * 64,
        canonical_receipt_id="c" * 64,
        canonical_content_hash="d" * 64,
        screen_hits={"n-shape-pool1": 2},
        pool2_active_count=1,
        errors=("screen:user/bad",),
    )

    first_receipt = bus.ingest(first, received_at=NOW)
    second_receipt = bus.ingest(replay, received_at=NOW + timedelta(seconds=1))

    assert first.signal_id == replay.signal_id
    assert first_receipt.disposition.value == "accepted"
    assert second_receipt.disposition.value == "duplicate"
    assert bus.source_descriptor().high_watermark == 1


def test_summary_signal_identity_is_anchored_to_canonical_availability(tmp_path) -> None:
    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    canonical_available_at = NOW - timedelta(minutes=5)
    early = DailySummaryStage(
        signal_bus=bus,
        strategy_version="daily-close-dag/v1",
        producer_commit="a" * 40,
        clock=lambda: NOW,
        artifact_store=DailyDownstreamArtifactStore(tmp_path / "early-artifacts"),
        canonical_reader_factory=lambda: DuckDBStore(
            tmp_path / "unused-early.duckdb", read_only=True
        ),
    )
    late = DailySummaryStage(
        signal_bus=bus,
        strategy_version="daily-close-dag/v1",
        producer_commit="a" * 40,
        clock=lambda: NOW + timedelta(hours=1),
        artifact_store=DailyDownstreamArtifactStore(tmp_path / "late-artifacts"),
        canonical_reader_factory=lambda: DuckDBStore(
            tmp_path / "unused-late.duckdb", read_only=True
        ),
    )
    arguments = {
        "trade_date": date(2026, 8, 3),
        "canonical_generation_id": "b" * 64,
        "canonical_receipt_id": "c" * 64,
        "canonical_content_hash": "d" * 64,
        "screen_hits": {"n-shape-pool1": 2},
        "pool2_active_count": 1,
        "errors": (),
        "event_time": canonical_available_at,
    }

    assert early.build_signal(**arguments).signal_id == late.build_signal(**arguments).signal_id
