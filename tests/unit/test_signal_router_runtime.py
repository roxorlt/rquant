from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Barrier

import pytest
from pydantic import ValidationError

from rquant.delivery_contracts import DeliveryChannel, DeliveryTarget, OutboxStatus
from rquant.signal_bus import SignalBusStore
from rquant.signal_contracts import SignalAction, SignalEnvelope
from rquant.signal_router_runtime import (
    RouteSourceDescriptor,
    RoutingConfigurationUnavailableError,
    RoutingDecision,
    SignalRouteConflictError,
    SignalRouteCursorStore,
    SignalRouteSequenceError,
    route_runner_signals,
)
from rquant.strategy_runner import RunnerSignalRecord

NOW = datetime(2026, 7, 31, 2, 30, tzinfo=UTC)
POLICY = "e" * 64
GENERATION = "f" * 64
SPEC = "1" * 64


def _signal(seed: str = "a", *, available_at: datetime = NOW) -> SignalEnvelope:
    return SignalEnvelope(
        schema_version=1,
        strategy_id="n-shape",
        strategy_version="1",
        parameter_fingerprint=seed * 64,
        dataset_snapshot_id="b" * 64,
        feature_snapshot_id="c" * 64,
        event_time=available_at - timedelta(seconds=1),
        available_at=available_at,
        candidate_id="600000.SH",
        action=SignalAction.WATCH,
        reason_codes=("test",),
        evidence={},
        expires_at=available_at + timedelta(minutes=5),
        producer_commit="d" * 40,
    )


class FakeRunner:
    def __init__(
        self,
        records: tuple[RunnerSignalRecord, ...],
        *,
        source_id: str = "n-shape-v1",
        generation_id: str = GENERATION,
        spec_fingerprint: str = SPEC,
        first_sequence: int = 1,
        high_watermark: int | None = None,
    ) -> None:
        self.records = records
        self._descriptor = RouteSourceDescriptor(
            source_id=source_id,
            generation_id=generation_id,
            strategy_spec_fingerprint=spec_fingerprint,
            first_sequence=first_sequence,
            high_watermark=(
                max((record.sequence for record in records), default=first_sequence - 1)
                if high_watermark is None
                else high_watermark
            ),
        )

    def descriptor(self) -> RouteSourceDescriptor:
        return self._descriptor

    def signals_after(self, *, sequence: int) -> tuple[RunnerSignalRecord, ...]:
        return tuple(record for record in self.records if record.sequence > sequence)


def _bus(path: Path) -> SignalBusStore:
    return SignalBusStore(
        path,
        retry_base_delay=timedelta(seconds=5),
        retry_max_delay=timedelta(seconds=30),
        max_attempts=3,
    )


def _target(
    recipient: str = "admin",
    channel: DeliveryChannel = DeliveryChannel.PUSHDEER,
) -> DeliveryTarget:
    return DeliveryTarget(recipient_id=recipient, channel=channel)


def _route_decision(*targets: DeliveryTarget) -> RoutingDecision:
    return RoutingDecision.route(
        routing_policy_fingerprint=POLICY,
        targets=targets or (_target(),),
    )


def _cursors(tmp_path: Path) -> SignalRouteCursorStore:
    return SignalRouteCursorStore(
        tmp_path / "legacy-cursor.sqlite3",
        routing_policy_fingerprint=POLICY,
    )


def _run(
    *,
    runner: FakeRunner,
    bus: SignalBusStore,
    cursors: SignalRouteCursorStore,
    routed_at: datetime = NOW,
    resolver: object | None = None,
    limit: int = 10,
):
    return route_runner_signals(
        source_id="n-shape-v1",
        source=runner,
        bus=bus,
        cursors=cursors,
        routed_at=routed_at,
        target_resolver=resolver or (lambda _signal: _route_decision()),
        limit=limit,
    )


def test_route_commits_source_receipt_cursor_signal_and_outbox_in_bus(
    tmp_path: Path,
) -> None:
    signal = _signal()
    runner = FakeRunner((RunnerSignalRecord(sequence=1, signal=signal),))
    bus = _bus(tmp_path / "bus.sqlite3")
    cursors = _cursors(tmp_path)

    summary = _run(runner=runner, bus=bus, cursors=cursors)

    assert summary.model_dump() | {"routed_at": NOW} == {
        "source_id": "n-shape-v1",
        "started_after_sequence": 0,
        "last_sequence": 1,
        "routed_count": 1,
        "target_count": 1,
        "duplicate_count": 0,
        "no_target_count": 0,
        "expired_count": 0,
        "deferred_count": 0,
        "routed_at": NOW,
    }
    assert bus.signal(signal.signal_id) == signal
    assert bus.route_cursor("n-shape-v1").last_sequence == 1
    assert bus.route_receipts("n-shape-v1")[0].target_count == 1
    assert cursors.cursor("n-shape-v1").last_sequence == 1
    outbox = bus.outbox_records(signal_id=signal.signal_id)
    assert len(outbox) == 1 and outbox[0].status is OutboxStatus.PENDING

    with sqlite3.connect(tmp_path / "legacy-cursor.sqlite3") as connection:
        tables = {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
    assert "runner_cursor" not in tables


def test_atomic_fault_rolls_back_cursor_receipt_signal_and_outbox(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    signal = _signal()
    runner = FakeRunner((RunnerSignalRecord(sequence=1, signal=signal),))
    bus = _bus(tmp_path / "bus.sqlite3")
    cursors = _cursors(tmp_path)
    monkeypatch.setattr(
        bus,
        "_before_commit",
        lambda _connection: (_ for _ in ()).throw(RuntimeError("commit fault")),
    )

    with pytest.raises(RuntimeError, match="commit fault"):
        _run(runner=runner, bus=bus, cursors=cursors)

    assert bus.signal(signal.signal_id) is None
    assert bus.route_cursor("n-shape-v1").last_sequence == 0
    assert bus.route_receipts("n-shape-v1") == ()
    assert bus.outbox_records() == ()


def test_concurrent_exact_retry_is_idempotent_without_duplicate_target(
    tmp_path: Path,
) -> None:
    signal = _signal()
    runner = FakeRunner((RunnerSignalRecord(sequence=1, signal=signal),))
    bus = _bus(tmp_path / "bus.sqlite3")
    barrier = Barrier(2)

    def resolver(_signal: SignalEnvelope) -> RoutingDecision:
        barrier.wait()
        return _route_decision()

    def run() -> object:
        return _run(
            runner=runner,
            bus=bus,
            cursors=_cursors(tmp_path),
            resolver=resolver,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        summaries = tuple(executor.map(lambda _index: run(), range(2)))

    assert sum(summary.routed_count for summary in summaries) == 1
    assert sum(summary.duplicate_count for summary in summaries) == 1
    assert len(bus.route_receipts("n-shape-v1")) == 1
    assert len(bus.outbox_records(signal_id=signal.signal_id)) == 1


def test_concurrent_target_manifest_drift_conflicts_instead_of_forming_union(
    tmp_path: Path,
) -> None:
    signal = _signal()
    runner = FakeRunner((RunnerSignalRecord(sequence=1, signal=signal),))
    bus = _bus(tmp_path / "bus.sqlite3")
    barrier = Barrier(2)

    def run(target: DeliveryTarget) -> object:
        def resolver(_signal: SignalEnvelope) -> RoutingDecision:
            barrier.wait()
            return _route_decision(target)

        return _run(
            runner=runner,
            bus=bus,
            cursors=_cursors(tmp_path),
            resolver=resolver,
        )

    targets = (_target("admin"), _target("research"))
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = tuple(executor.submit(run, target) for target in targets)
        outcomes: list[object] = []
        errors: list[BaseException] = []
        for future in futures:
            try:
                outcomes.append(future.result())
            except BaseException as exc:  # noqa: BLE001 - asserting isolation result
                errors.append(exc)

    assert len(outcomes) == 1
    assert len(errors) == 1
    assert isinstance(errors[0], SignalRouteConflictError)
    outbox = bus.outbox_records(signal_id=signal.signal_id)
    assert len(outbox) == 1
    assert outbox[0].target in targets


def test_legacy_route_api_cannot_expand_a_frozen_source_target_manifest(
    tmp_path: Path,
) -> None:
    signal = _signal()
    bus = _bus(tmp_path / "bus.sqlite3")
    _run(
        runner=FakeRunner((RunnerSignalRecord(sequence=1, signal=signal),)),
        bus=bus,
        cursors=_cursors(tmp_path),
    )

    assert len(bus.route(signal.signal_id, (_target(),), now=NOW)) == 1
    with pytest.raises(SignalRouteConflictError, match="frozen target manifest"):
        bus.route(signal.signal_id, (_target("research"),), now=NOW)
    assert len(bus.outbox_records(signal_id=signal.signal_id)) == 1


def test_source_generation_drift_fails_even_when_source_returns_empty(
    tmp_path: Path,
) -> None:
    bus = _bus(tmp_path / "bus.sqlite3")
    cursors = _cursors(tmp_path)
    _run(runner=FakeRunner(()), bus=bus, cursors=cursors)

    with pytest.raises(SignalRouteConflictError, match="generation"):
        _run(
            runner=FakeRunner((), generation_id="2" * 64),
            bus=bus,
            cursors=cursors,
        )


def test_source_spec_and_routing_policy_are_frozen_in_the_bus(tmp_path: Path) -> None:
    bus = _bus(tmp_path / "bus.sqlite3")
    _run(runner=FakeRunner(()), bus=bus, cursors=_cursors(tmp_path))

    with pytest.raises(SignalRouteConflictError, match="strategy spec"):
        _run(
            runner=FakeRunner((), spec_fingerprint="3" * 64),
            bus=bus,
            cursors=_cursors(tmp_path),
        )
    with pytest.raises(SignalRouteConflictError, match="routing policy"):
        route_runner_signals(
            source_id="n-shape-v1",
            source=FakeRunner(()),
            bus=bus,
            cursors=SignalRouteCursorStore(
                tmp_path / "ignored.sqlite3",
                routing_policy_fingerprint="4" * 64,
            ),
            routed_at=NOW,
            target_resolver=lambda _signal: _route_decision(),
            limit=10,
        )


def test_empty_source_detects_high_watermark_rollback_and_tail_truncation(
    tmp_path: Path,
) -> None:
    signal = _signal()
    bus = _bus(tmp_path / "bus.sqlite3")
    cursors = _cursors(tmp_path)
    _run(
        runner=FakeRunner((RunnerSignalRecord(sequence=1, signal=signal),)),
        bus=bus,
        cursors=cursors,
    )

    with pytest.raises(SignalRouteSequenceError, match="high watermark regressed"):
        _run(
            runner=FakeRunner((), high_watermark=0),
            bus=bus,
            cursors=cursors,
        )

    fresh_bus = _bus(tmp_path / "fresh-bus.sqlite3")
    with pytest.raises(SignalRouteSequenceError, match="source tail is missing"):
        _run(
            runner=FakeRunner((), high_watermark=1),
            bus=fresh_bus,
            cursors=_cursors(tmp_path),
        )
    assert fresh_bus.route_cursor("n-shape-v1").last_sequence == 0


def test_sequence_gap_fails_closed_without_advancing_cursor(tmp_path: Path) -> None:
    runner = FakeRunner((RunnerSignalRecord(sequence=2, signal=_signal()),))
    bus = _bus(tmp_path / "bus.sqlite3")

    with pytest.raises(SignalRouteSequenceError, match="expected runner sequence 1"):
        _run(runner=runner, bus=bus, cursors=_cursors(tmp_path))

    assert bus.route_cursor("n-shape-v1").last_sequence == 0


def test_no_target_is_explicitly_persisted_and_counted(tmp_path: Path) -> None:
    signal = _signal()
    bus = _bus(tmp_path / "bus.sqlite3")

    summary = _run(
        runner=FakeRunner((RunnerSignalRecord(sequence=1, signal=signal),)),
        bus=bus,
        cursors=_cursors(tmp_path),
        resolver=lambda _signal: RoutingDecision.no_target(
            routing_policy_fingerprint=POLICY,
            reason_code="recipient-opted-out",
        ),
    )

    assert summary.no_target_count == 1
    assert summary.routed_count == 0
    assert summary.target_count == 0
    receipt = bus.route_receipts("n-shape-v1")[0]
    assert receipt.reason_code == "recipient-opted-out"
    assert receipt.target_count == 0
    assert bus.outbox_records() == ()


def test_temporary_routing_configuration_error_does_not_advance(tmp_path: Path) -> None:
    signal = _signal()
    bus = _bus(tmp_path / "bus.sqlite3")

    def unavailable(_signal: SignalEnvelope) -> RoutingDecision:
        raise RoutingConfigurationUnavailableError("recipient registry unavailable")

    with pytest.raises(
        RoutingConfigurationUnavailableError,
        match="recipient registry unavailable",
    ):
        _run(
            runner=FakeRunner((RunnerSignalRecord(sequence=1, signal=signal),)),
            bus=bus,
            cursors=_cursors(tmp_path),
            resolver=unavailable,
        )

    assert bus.route_cursor("n-shape-v1").last_sequence == 0
    assert bus.route_receipts("n-shape-v1") == ()


def test_future_and_expired_signals_have_independent_summary_counts(
    tmp_path: Path,
) -> None:
    expired = _signal("a", available_at=NOW - timedelta(minutes=10))
    future = _signal("2", available_at=NOW + timedelta(seconds=1))
    runner = FakeRunner(
        (
            RunnerSignalRecord(sequence=1, signal=expired),
            RunnerSignalRecord(sequence=2, signal=future),
        )
    )
    bus = _bus(tmp_path / "bus.sqlite3")

    summary = _run(runner=runner, bus=bus, cursors=_cursors(tmp_path))

    assert summary.expired_count == 1
    assert summary.deferred_count == 1
    assert summary.routed_count == 0
    assert summary.last_sequence == 1
    assert bus.outbox_records(signal_id=expired.signal_id)[0].status is OutboxStatus.EXPIRED
    assert bus.signal(future.signal_id) is None


def test_policy_fingerprint_and_limit_use_pydantic_validation(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="routing_policy_fingerprint"):
        SignalRouteCursorStore(
            tmp_path / "cursor.sqlite3",
            routing_policy_fingerprint="not-a-sha",
        )

    runner = FakeRunner(())
    bus = _bus(tmp_path / "bus.sqlite3")
    for invalid in (True, 1.5, 0):
        with pytest.raises((ValidationError, ValueError)):
            _run(
                runner=runner,
                bus=bus,
                cursors=_cursors(tmp_path),
                limit=invalid,  # type: ignore[arg-type]
            )


def test_restoring_the_bus_database_restores_cursor_receipts_and_outbox_together(
    tmp_path: Path,
) -> None:
    first_signal = _signal("a")
    second_signal = _signal("2", available_at=NOW + timedelta(seconds=1))
    records = (
        RunnerSignalRecord(sequence=1, signal=first_signal),
        RunnerSignalRecord(sequence=2, signal=second_signal),
    )
    source_path = tmp_path / "bus.sqlite3"
    source_bus = _bus(source_path)
    _run(
        runner=FakeRunner(records, high_watermark=2),
        bus=source_bus,
        cursors=_cursors(tmp_path),
        limit=1,
    )

    restored_path = tmp_path / "restored.sqlite3"
    with (
        sqlite3.connect(source_path) as source_connection,
        sqlite3.connect(restored_path) as restored_connection,
    ):
        source_connection.backup(restored_connection)
    restored_bus = _bus(restored_path)

    summary = _run(
        runner=FakeRunner(records, high_watermark=2),
        bus=restored_bus,
        cursors=SignalRouteCursorStore(
            tmp_path / "fresh-facade.sqlite3",
            routing_policy_fingerprint=POLICY,
        ),
        routed_at=NOW + timedelta(seconds=1),
    )

    assert summary.started_after_sequence == 1
    assert summary.last_sequence == 2
    assert len(restored_bus.route_receipts("n-shape-v1")) == 2
    assert len(restored_bus.outbox_records()) == 2
