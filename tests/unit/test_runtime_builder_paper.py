from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from rquant.paper_broker import BrokerExecutionContext
from rquant.paper_signal_worker import PaperQuoteSnapshot
from rquant.runtime_builder_paper import paper_broker_builder, paper_consumer_builder
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.signal_bus import SignalBusStore
from rquant.signal_contracts import SignalAction, SignalEnvelope

NOW = datetime(2026, 7, 31, 1, 30, tzinfo=UTC)
COMMIT = "a" * 40


def _settings(tmp_path: Path) -> dict[str, object]:
    return {
        "signal_bus_path": str(tmp_path / "bus.sqlite3"),
        "queue_path": str(tmp_path / "queue.sqlite3"),
        "consumer_state_path": str(tmp_path / "consumer.sqlite3"),
        "broker_path": str(tmp_path / "broker.sqlite3"),
        "account_id": "paper-main",
        "execution_lag_seconds": 60,
        "buy_quantity": 1_000,
        "reduce_quantity": 500,
        "sell_quantity": 1_000,
        "initial_cash": "100000",
        "commission_rate": "0.0003",
        "minimum_commission": "5",
        "sell_stamp_tax_rate": "0.001",
        "limit": 10,
    }


def _manifest(tmp_path: Path, kind: RuntimeServiceKind) -> RuntimeServiceManifest:
    return RuntimeServiceManifest(
        service_id=f"paper.{kind.value}",
        service_kind=kind,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=1,
        stale_after_seconds=10,
        producer_commit=COMMIT,
        settings=_settings(tmp_path),
    )


def _signal() -> SignalEnvelope:
    return SignalEnvelope(
        schema_version=1,
        strategy_id="n-shape",
        strategy_version="1",
        parameter_fingerprint="b" * 64,
        dataset_snapshot_id="c" * 64,
        feature_snapshot_id="d" * 64,
        event_time=NOW - timedelta(seconds=5),
        available_at=NOW,
        candidate_id="600000.SH",
        action=SignalAction.B_INTENT,
        reason_codes=("paper-runtime",),
        evidence={},
        expires_at=NOW + timedelta(minutes=5),
        producer_commit="e" * 40,
    )


def test_paper_consumer_delegates_signal_with_durable_cursor(tmp_path: Path) -> None:
    bus = SignalBusStore(tmp_path / "bus.sqlite3")
    bus.ingest(_signal(), received_at=NOW)
    step = paper_consumer_builder(clock=lambda: NOW)(
        _manifest(tmp_path, RuntimeServiceKind.PAPER_CONSUMER)
    )

    first = step()
    replay = step()

    assert first.input_sequence == 0
    assert first.output_sequence == 1
    assert first.processed_count == 1
    assert first.backlog_count == 0
    assert first.source_generations["signal_bus"] == bus.source_descriptor().generation_id
    assert replay.processed_count == 0
    assert replay.output_sequence == 1


def test_paper_broker_executes_due_signal_from_independent_queue(tmp_path: Path) -> None:
    bus = SignalBusStore(tmp_path / "bus.sqlite3")
    signal = _signal()
    bus.ingest(signal, received_at=NOW)
    paper_consumer_builder(clock=lambda: NOW)(
        _manifest(tmp_path, RuntimeServiceKind.PAPER_CONSUMER)
    )()
    execution_time = NOW + timedelta(minutes=1)

    def quote_resolver(_signal: SignalEnvelope, _now: datetime) -> PaperQuoteSnapshot:
        return PaperQuoteSnapshot(
            ts_code="600000.SH",
            event_time=execution_time,
            available_at=execution_time,
            context=BrokerExecutionContext(
                executable_price=Decimal("10.00"),
                acquisition_available_date=date(2026, 8, 3),
            ),
            producer_commit=COMMIT,
        )

    step = paper_broker_builder(
        clock=lambda: execution_time,
        quote_resolver=quote_resolver,
        trade_date_resolver=lambda _now: date(2026, 7, 31),
    )(_manifest(tmp_path, RuntimeServiceKind.PAPER_BROKER))

    result = step()

    assert result.processed_count == 1
    assert result.backlog_count == 0
    assert result.degraded_reasons == ()
    assert len(result.source_generations) == 2


def test_paper_builders_reject_wrong_plane_and_relative_paths(tmp_path: Path) -> None:
    wrong_plane = RuntimeServiceManifest.model_validate(
        {
            **_manifest(tmp_path, RuntimeServiceKind.PAPER_CONSUMER).model_dump(mode="json"),
            "plane": "research",
        }
    )
    with pytest.raises(ValueError, match="live plane"):
        paper_consumer_builder(clock=lambda: NOW)(wrong_plane)

    relative = RuntimeServiceManifest.model_validate(
        {
            **_manifest(tmp_path, RuntimeServiceKind.PAPER_CONSUMER).model_dump(mode="json"),
            "settings": {**_settings(tmp_path), "queue_path": "queue.sqlite3"},
        }
    )
    with pytest.raises(ValueError, match="absolute"):
        paper_consumer_builder(clock=lambda: NOW)(relative)
