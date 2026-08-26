from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from rquant.delivery_contracts import (
    DeliveryChannel,
    DeliveryTarget,
    OutboxStatus,
    RouterDisposition,
)
from rquant.signal_bus import (
    SignalBusLeaseError,
    SignalBusSourceSequenceError,
    SignalBusStore,
    SignalBusWatermarkError,
    recover_signal_bus_high_watermark,
)
from rquant.signal_contracts import SignalAction, SignalEnvelope

NOW = datetime(2026, 7, 31, 1, 30, tzinfo=UTC)


def _signal(
    seed: str = "a",
    *,
    available_at: datetime = NOW,
    expires_at: datetime | None = None,
) -> SignalEnvelope:
    return SignalEnvelope(
        schema_version=1,
        strategy_id="n-shape",
        strategy_version="2.0.0",
        parameter_fingerprint=seed * 64,
        dataset_snapshot_id="b" * 64,
        feature_snapshot_id="c" * 64,
        event_time=available_at - timedelta(seconds=2),
        available_at=available_at,
        candidate_id=f"60000{ord(seed) % 10}.SH",
        action=SignalAction.B_INTENT,
        reason_codes=("same-minute-volume", "vwap-hold"),
        evidence={"ratio": 1.25, "levels": ["10.20", "10.30"]},
        expires_at=expires_at or available_at + timedelta(minutes=5),
        producer_commit="d" * 40,
    )


def _target(
    recipient: str = "admin",
    channel: DeliveryChannel = DeliveryChannel.PUSHDEER,
) -> DeliveryTarget:
    return DeliveryTarget(recipient_id=recipient, channel=channel)


def _store(path: Path, **kwargs: object) -> SignalBusStore:
    return SignalBusStore(
        path,
        retry_base_delay=timedelta(seconds=5),
        retry_max_delay=timedelta(seconds=8),
        max_attempts=3,
        **kwargs,
    )


def _ingest_and_route(
    store: SignalBusStore,
    signal: SignalEnvelope | None = None,
    *,
    now: datetime = NOW,
) -> tuple[SignalEnvelope, str]:
    item = signal or _signal()
    store.ingest(item, received_at=now)
    record = store.route(item.signal_id, (_target(),), now=now)[0]
    return item, record.outbox_id


def test_ingest_allocates_one_monotonic_sequence_and_exact_retry_is_duplicate(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path / "signal-bus.sqlite3")
    first = _signal("a")
    second = _signal("e")

    accepted = store.ingest(first, received_at=NOW)
    duplicate = store.ingest(first, received_at=NOW + timedelta(seconds=1))
    next_receipt = store.ingest(second, received_at=NOW + timedelta(seconds=2))

    assert accepted.disposition is RouterDisposition.ACCEPTED
    assert accepted.global_sequence == 1
    assert duplicate.disposition is RouterDisposition.DUPLICATE
    assert duplicate.global_sequence == accepted.global_sequence
    assert next_receipt.global_sequence == 2


def test_legacy_route_with_empty_targets_remains_a_noop(tmp_path: Path) -> None:
    store = _store(tmp_path / "signal-bus.sqlite3")
    signal = _signal()
    store.ingest(signal, received_at=NOW)

    assert store.route(signal.signal_id, (), now=NOW) == ()
    assert store.outbox_records(signal_id=signal.signal_id) == ()


def test_tampered_model_copy_is_rejected_without_replacing_original(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path / "signal-bus.sqlite3")
    original = _signal()
    accepted = store.ingest(original, received_at=NOW)
    conflict = original.model_copy(update={"candidate_id": "000001.SZ"})

    with pytest.raises(ValueError, match="signal_id does not match"):
        store.ingest(conflict, received_at=NOW + timedelta(seconds=1))

    assert store.signal(original.signal_id) == original
    assert store.quarantines(original.signal_id) == ()
    assert store.ingest(original, received_at=NOW).global_sequence == accepted.global_sequence


def test_signal_round_trips_frozen_canonical_json_by_id_and_sequence(
    tmp_path: Path,
) -> None:
    path = tmp_path / "signal-bus.sqlite3"
    signal = _signal()
    first = _store(path)
    receipt = first.ingest(signal, received_at=NOW)
    payload = first.signal_payload(signal.signal_id)

    reopened = _store(path)

    assert reopened.signal(signal.signal_id) == signal
    assert reopened.signal(receipt.global_sequence) == signal
    assert reopened.signal_payload(receipt.global_sequence) == payload
    assert '"available_at":"2026-07-31T01:30:00Z"' in payload
    assert payload == reopened.signal_payload(signal.signal_id)
    with pytest.raises(TypeError):
        reopened.signal(signal.signal_id).evidence["new"] = 1  # type: ignore[index]


def test_source_generation_is_stable_across_reopen_and_changes_after_rebuild(
    tmp_path: Path,
) -> None:
    path = tmp_path / "signal-bus.sqlite3"
    first = _store(path).source_descriptor()
    reopened = _store(path).source_descriptor()

    assert reopened == first
    assert first.first_global_sequence == 1
    assert first.high_watermark == 0

    path.unlink()
    rebuilt = _store(path).source_descriptor()

    assert rebuilt.generation_id != first.generation_id
    assert rebuilt.first_global_sequence == 1
    assert rebuilt.high_watermark == 0


def test_bounded_global_sequence_read_is_ordered_frozen_and_read_only(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path / "signal-bus.sqlite3")
    first = _signal("a")
    second = _signal("e")
    store.ingest(first, received_at=NOW)
    frozen = store.source_descriptor()
    store.ingest(second, received_at=NOW + timedelta(seconds=1))

    records = store.signals_after_global_sequence(
        after_sequence=0,
        through_sequence=frozen.high_watermark,
        observed_at=NOW + timedelta(seconds=2),
        limit=10,
    )

    assert [record.global_sequence for record in records] == [1]
    assert records[0].signal == first
    assert records[0].signal_id == first.signal_id
    assert records[0].payload_hash == records[0].canonical_payload_hash
    assert store.source_descriptor().high_watermark == 2
    assert (
        store.signals_after_global_sequence(
            after_sequence=0,
            through_sequence=frozen.high_watermark,
            observed_at=NOW + timedelta(seconds=2),
            limit=10,
        )
        == records
    )


def test_future_signal_blocks_later_sequences_until_it_is_visible(tmp_path: Path) -> None:
    store = _store(tmp_path / "signal-bus.sqlite3")
    future = _signal("a", available_at=NOW + timedelta(minutes=2))
    later = _signal("e", available_at=NOW)
    store.ingest(future, received_at=NOW)
    store.ingest(later, received_at=NOW)
    watermark = store.source_descriptor().high_watermark

    assert (
        store.signals_after_global_sequence(
            after_sequence=0,
            through_sequence=watermark,
            observed_at=NOW,
            limit=10,
        )
        == ()
    )
    visible = store.signals_after_global_sequence(
        after_sequence=0,
        through_sequence=watermark,
        observed_at=NOW + timedelta(minutes=2),
        limit=10,
    )
    assert [record.signal_id for record in visible] == [future.signal_id, later.signal_id]


def test_bounded_read_rejects_sequence_gap_or_high_watermark_beyond_source(
    tmp_path: Path,
) -> None:
    path = tmp_path / "signal-bus.sqlite3"
    store = _store(path)
    store.ingest(_signal("a"), received_at=NOW)
    store.ingest(_signal("e"), received_at=NOW)
    with sqlite3.connect(path) as connection:
        connection.execute("DELETE FROM signal_envelope WHERE global_sequence = 1")

    with pytest.raises(SignalBusSourceSequenceError, match="gap|truncated"):
        store.signals_after_global_sequence(
            after_sequence=0,
            through_sequence=2,
            observed_at=NOW,
            limit=10,
        )
    with pytest.raises(SignalBusSourceSequenceError, match="high watermark"):
        store.signals_after_global_sequence(
            after_sequence=2,
            through_sequence=3,
            observed_at=NOW,
            limit=10,
        )


def test_route_deduplicates_targets_and_persists_across_reopen(tmp_path: Path) -> None:
    path = tmp_path / "signal-bus.sqlite3"
    store = _store(path)
    signal = _signal()
    store.ingest(signal, received_at=NOW)
    targets = (
        _target(),
        _target(),
        _target("admin", DeliveryChannel.PUSHPLUS),
    )

    first = store.route(signal.signal_id, targets, now=NOW)
    second = _store(path).route(signal.signal_id, reversed(targets), now=NOW)

    assert len(first) == 2
    assert second == first
    assert all(record.status is OutboxStatus.PENDING for record in first)
    assert len(store.outbox_records(signal_id=signal.signal_id)) == 2


def test_route_after_signal_expiry_creates_terminal_expired_record(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path / "signal-bus.sqlite3")
    signal = _signal(expires_at=NOW + timedelta(seconds=10))
    store.ingest(signal, received_at=NOW)

    record = store.route(
        signal.signal_id,
        (_target(),),
        now=NOW + timedelta(seconds=10),
    )[0]

    assert record.status is OutboxStatus.EXPIRED
    assert record.next_attempt_at is None
    assert record.last_error == "signal expired before routing"
    assert (
        store.claim_due(
            "worker-a",
            now=NOW + timedelta(seconds=10),
            lease_for=timedelta(seconds=3),
            limit=10,
        )
        == ()
    )


def test_route_rejects_signal_before_its_available_at(tmp_path: Path) -> None:
    store = _store(tmp_path / "signal-bus.sqlite3")
    signal = _signal(available_at=NOW + timedelta(minutes=1))
    store.ingest(signal, received_at=NOW)

    with pytest.raises(ValueError, match="available_at"):
        store.route(signal.signal_id, (_target(),), now=NOW)

    assert store.outbox_records(signal_id=signal.signal_id) == ()


def test_claim_due_is_ordered_and_cannot_double_claim_across_store_instances(
    tmp_path: Path,
) -> None:
    path = tmp_path / "signal-bus.sqlite3"
    first_store = _store(path)
    second_store = _store(path)
    first_signal, first_id = _ingest_and_route(first_store, _signal("a"))
    second_signal, second_id = _ingest_and_route(first_store, _signal("e"))

    def claim(store: SignalBusStore, worker: str) -> tuple[str, ...]:
        records = store.claim_due(
            worker,
            now=NOW,
            lease_for=timedelta(seconds=20),
            limit=1,
        )
        return tuple(record.outbox_id for record in records)

    with ThreadPoolExecutor(max_workers=2) as executor:
        claimed = list(
            executor.map(
                lambda pair: claim(*pair),
                ((first_store, "worker-a"), (second_store, "worker-b")),
            )
        )

    assert sorted(item for group in claimed for item in group) == sorted((first_id, second_id))
    assert all(len(group) == 1 for group in claimed)
    records = first_store.outbox_records()
    assert [record.signal_id for record in records] == [
        first_signal.signal_id,
        second_signal.signal_id,
    ]
    assert {record.lease_owner for record in records} == {"worker-a", "worker-b"}


def test_claim_expires_due_records_before_leasing(tmp_path: Path) -> None:
    store = _store(tmp_path / "signal-bus.sqlite3")
    signal, outbox_id = _ingest_and_route(
        store,
        _signal(expires_at=NOW + timedelta(seconds=10)),
    )

    claimed = store.claim_due(
        "worker-a",
        now=signal.expires_at,
        lease_for=timedelta(seconds=2),
        limit=1,
    )

    assert claimed == ()
    record = store.outbox_record(outbox_id)
    assert record is not None
    assert record.status is OutboxStatus.EXPIRED
    assert record.last_error == "signal expired before delivery claim"


def test_complete_success_persists_attempt_and_terminal_record_atomically(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path / "signal-bus.sqlite3")
    _, outbox_id = _ingest_and_route(store)
    leased = store.claim_due(
        "worker-a",
        now=NOW,
        lease_for=timedelta(seconds=20),
        limit=1,
    )[0]

    completed = store.complete_success(
        outbox_id,
        worker_id="worker-a",
        attempt_no=leased.attempt_count,
        completed_at=NOW + timedelta(seconds=2),
        provider_receipt="pushdeer:ok-1",
    )

    assert completed.status is OutboxStatus.SUCCEEDED
    assert completed.lease_owner is None
    attempts = store.attempts(outbox_id)
    assert len(attempts) == 1
    assert attempts[0].success is True
    assert attempts[0].started_at == NOW
    assert attempts[0].completed_at == NOW + timedelta(seconds=2)


def test_failure_retries_with_capped_backoff_then_dead_letters(tmp_path: Path) -> None:
    store = _store(tmp_path / "signal-bus.sqlite3")
    _, outbox_id = _ingest_and_route(store)
    expected_delays = (timedelta(seconds=5), timedelta(seconds=8))
    now = NOW

    for attempt_no, delay in enumerate(expected_delays, start=1):
        leased = store.claim_due(
            "worker-a",
            now=now,
            lease_for=timedelta(seconds=20),
            limit=1,
        )[0]
        assert leased.attempt_count == attempt_no
        failed_at = now + timedelta(seconds=1)
        failed = store.complete_failure(
            outbox_id,
            worker_id="worker-a",
            attempt_no=attempt_no,
            completed_at=failed_at,
            error=f"provider failure {attempt_no}",
        )
        assert failed.status is OutboxStatus.RETRY
        assert failed.next_attempt_at == failed_at + delay
        assert (
            store.claim_due(
                "worker-a",
                now=failed.next_attempt_at - timedelta(microseconds=1),
                lease_for=timedelta(seconds=1),
                limit=1,
            )
            == ()
        )
        now = failed.next_attempt_at

    leased = store.claim_due(
        "worker-a",
        now=now,
        lease_for=timedelta(seconds=20),
        limit=1,
    )[0]
    terminal = store.complete_failure(
        outbox_id,
        worker_id="worker-a",
        attempt_no=leased.attempt_count,
        completed_at=now + timedelta(seconds=1),
        error="provider failure 3",
    )

    assert terminal.status is OutboxStatus.DEAD_LETTER
    assert terminal.next_attempt_at is None
    assert [attempt.attempt_no for attempt in store.attempts(outbox_id)] == [1, 2, 3]


def test_failure_whose_next_retry_crosses_expiry_becomes_expired(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path / "signal-bus.sqlite3")
    _, outbox_id = _ingest_and_route(
        store,
        _signal(expires_at=NOW + timedelta(seconds=4)),
    )
    leased = store.claim_due(
        "worker-a",
        now=NOW,
        lease_for=timedelta(seconds=3),
        limit=1,
    )[0]

    failed = store.complete_failure(
        outbox_id,
        worker_id="worker-a",
        attempt_no=leased.attempt_count,
        completed_at=NOW + timedelta(seconds=2),
        error="provider unavailable",
    )

    assert failed.status is OutboxStatus.EXPIRED
    assert failed.last_error == "delivery window expired after failure: provider unavailable"


@pytest.mark.parametrize(
    ("worker_id", "attempt_no", "message"),
    [
        ("wrong-worker", 1, "lease owner"),
        ("worker-a", 2, "attempt number"),
    ],
)
def test_completion_verifies_lease_owner_and_attempt_number(
    tmp_path: Path,
    worker_id: str,
    attempt_no: int,
    message: str,
) -> None:
    store = _store(tmp_path / f"signal-bus-{worker_id}.sqlite3")
    _, outbox_id = _ingest_and_route(store)
    store.claim_due(
        "worker-a",
        now=NOW,
        lease_for=timedelta(seconds=20),
        limit=1,
    )

    with pytest.raises(SignalBusLeaseError, match=message):
        store.complete_success(
            outbox_id,
            worker_id=worker_id,
            attempt_no=attempt_no,
            completed_at=NOW + timedelta(seconds=1),
            provider_receipt="pushdeer:ok",
        )

    assert store.attempts(outbox_id) == ()
    assert store.outbox_record(outbox_id).status is OutboxStatus.LEASED  # type: ignore[union-attr]


def test_completion_at_exact_lease_deadline_is_rejected_deterministically(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path / "signal-bus.sqlite3")
    _, outbox_id = _ingest_and_route(store)
    leased = store.claim_due(
        "worker-a",
        now=NOW,
        lease_for=timedelta(seconds=2),
        limit=1,
    )[0]

    with pytest.raises(SignalBusLeaseError, match="expired"):
        store.complete_success(
            outbox_id,
            worker_id="worker-a",
            attempt_no=leased.attempt_count,
            completed_at=NOW + timedelta(seconds=2),
            provider_receipt="pushdeer:late",
        )

    assert store.recover_expired_leases(now=NOW + timedelta(seconds=2))[0].status is (
        OutboxStatus.DEAD_LETTER
    )


def test_release_unattempted_lease_requeues_without_consuming_attempt(tmp_path: Path) -> None:
    store = _store(tmp_path / "signal-bus.sqlite3")
    _, outbox_id = _ingest_and_route(store)
    leased = store.claim_due(
        "worker-a",
        now=NOW,
        lease_for=timedelta(seconds=2),
        limit=1,
    )[0]

    released = store.release_unattempted(
        outbox_id,
        worker_id="worker-a",
        attempt_no=leased.attempt_count,
        released_at=NOW + timedelta(seconds=2),
        reason="batch lease elapsed before provider call",
    )

    assert released.status is OutboxStatus.RETRY
    assert released.attempt_count == 0
    assert released.next_attempt_at == NOW + timedelta(seconds=2)
    assert store.attempts(outbox_id) == ()
    reclaimed = store.claim_due(
        "worker-b",
        now=NOW + timedelta(seconds=2),
        lease_for=timedelta(seconds=2),
        limit=1,
    )[0]
    assert reclaimed.attempt_count == 1


def test_unknown_delivery_evidence_survives_lease_recovery(tmp_path: Path) -> None:
    store = _store(tmp_path / "signal-bus.sqlite3")
    _, outbox_id = _ingest_and_route(store)
    leased = store.claim_due(
        "worker-a",
        now=NOW,
        lease_for=timedelta(seconds=2),
        limit=1,
    )[0]

    evidence = store.record_unknown_delivery(
        outbox_id,
        worker_id="worker-a",
        attempt_no=leased.attempt_count,
        observed_at=NOW + timedelta(seconds=1),
        reason="TimeoutError: response missing after send",
        provider_receipt=None,
    )
    recovered = store.recover_expired_leases(now=NOW + timedelta(seconds=2))[0]

    assert evidence.reason == "TimeoutError: response missing after send"
    assert store.unknown_deliveries(outbox_id) == (evidence,)
    assert recovered.status is OutboxStatus.DEAD_LETTER
    assert "response missing after send" in (recovered.last_error or "")


def test_recover_expired_leases_dead_letters_unknown_live_outcomes_and_expires_stale(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path / "signal-bus.sqlite3")
    _, live_id = _ingest_and_route(store, _signal("a"))
    _, stale_id = _ingest_and_route(
        store,
        _signal("e", expires_at=NOW + timedelta(seconds=3)),
    )
    store.claim_due(
        "worker-a",
        now=NOW,
        lease_for=timedelta(seconds=2),
        limit=2,
    )

    recovered = store.recover_expired_leases(now=NOW + timedelta(seconds=3))

    assert {record.outbox_id for record in recovered} == {live_id, stale_id}
    live = store.outbox_record(live_id)
    stale = store.outbox_record(stale_id)
    assert live is not None and live.status is OutboxStatus.DEAD_LETTER
    assert live.next_attempt_at is None
    assert live.last_error == "delivery outcome unknown after lease expiry"
    assert stale is not None and stale.status is OutboxStatus.EXPIRED
    assert stale.last_error == "delivery outcome unknown after signal expiry"
    assert store.attempts() == ()


def test_store_rejects_retry_policy_drift_when_reopened(tmp_path: Path) -> None:
    path = tmp_path / "signal-bus.sqlite3"
    _store(path)

    with pytest.raises(ValueError, match="retry policy"):
        SignalBusStore(
            path,
            retry_base_delay=timedelta(seconds=5),
            retry_max_delay=timedelta(seconds=8),
            max_attempts=4,
        )


def test_injected_failure_rolls_back_attempt_and_outbox_transition(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "signal-bus.sqlite3"
    store = _store(path)
    _, outbox_id = _ingest_and_route(store)
    leased = store.claim_due(
        "worker-a",
        now=NOW,
        lease_for=timedelta(seconds=20),
        limit=1,
    )[0]

    def explode(_connection: sqlite3.Connection) -> None:
        raise RuntimeError("injected failure")

    monkeypatch.setattr(store, "_before_commit", explode)
    with pytest.raises(RuntimeError, match="injected failure"):
        store.complete_success(
            outbox_id,
            worker_id="worker-a",
            attempt_no=leased.attempt_count,
            completed_at=NOW + timedelta(seconds=1),
            provider_receipt="pushdeer:ok",
        )

    reopened = _store(path)
    record = reopened.outbox_record(outbox_id)
    assert record is not None and record.status is OutboxStatus.LEASED
    assert reopened.attempts(outbox_id) == ()


def test_store_enables_wal_foreign_keys_and_busy_timeout(tmp_path: Path) -> None:
    path = tmp_path / "signal-bus.sqlite3"
    store = _store(path, busy_timeout_ms=1_234)

    with store._connect() as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert connection.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        assert connection.execute("PRAGMA busy_timeout").fetchone()[0] == 1_234


def test_missing_signal_and_outbox_are_explicit(tmp_path: Path) -> None:
    store = _store(tmp_path / "signal-bus.sqlite3")

    assert store.signal("f" * 64) is None
    assert store.outbox_record("f" * 64) is None
    with pytest.raises(KeyError, match="signal"):
        store.route("f" * 64, (_target(),), now=NOW)


# ---------------------------------------------------------------------------------------
# Codex round-2 ruling 5: `signal_high_watermark` is monotonic, never self-corrected, and
# fails closed when it disagrees with the durable rows. Only an explicit audited recovery
# may move it, and only upwards.
# ---------------------------------------------------------------------------------------


def _watermark(path: Path) -> int:
    connection = sqlite3.connect(path)
    try:
        row = connection.execute(
            "SELECT metadata_value FROM signal_bus_metadata "
            "WHERE metadata_key = 'signal_high_watermark'"
        ).fetchone()
    finally:
        connection.close()
    assert row is not None
    return int(row[0])


def _set_watermark(path: Path, value: int) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            "UPDATE signal_bus_metadata SET metadata_value = ? "
            "WHERE metadata_key = 'signal_high_watermark'",
            (str(value),),
        )
        connection.commit()
    finally:
        connection.close()


def test_the_high_watermark_never_regresses_through_the_ingest_path(tmp_path: Path) -> None:
    path = tmp_path / "signal-bus.sqlite3"
    store = _store(path)
    store.ingest(_signal("a"), received_at=NOW)
    store.ingest(_signal("e"), received_at=NOW + timedelta(seconds=1))

    assert _watermark(path) == 2
    store.ingest(_signal("a"), received_at=NOW + timedelta(seconds=2))
    store.ingest(_signal("f"), received_at=NOW + timedelta(seconds=3))

    assert _watermark(path) == 3
    assert store.source_descriptor().high_watermark == 3


def test_a_watermark_behind_the_durable_rows_fails_closed_on_open(tmp_path: Path) -> None:
    path = tmp_path / "signal-bus.sqlite3"
    store = _store(path)
    store.ingest(_signal("a"), received_at=NOW)
    store.ingest(_signal("e"), received_at=NOW + timedelta(seconds=1))
    _set_watermark(path, 1)

    with pytest.raises(SignalBusWatermarkError, match="signal bus high watermark"):
        _store(path)

    assert _watermark(path) == 1


def test_a_watermark_ahead_of_the_durable_rows_fails_closed_on_open(tmp_path: Path) -> None:
    """The append-only sequence cannot shrink, so a higher watermark means lost rows."""

    path = tmp_path / "signal-bus.sqlite3"
    store = _store(path)
    store.ingest(_signal("a"), received_at=NOW)
    _set_watermark(path, 9)

    with pytest.raises(SignalBusWatermarkError, match="signal bus high watermark"):
        _store(path)


def test_an_inconsistent_watermark_is_never_silently_corrected(tmp_path: Path) -> None:
    path = tmp_path / "signal-bus.sqlite3"
    store = _store(path)
    store.ingest(_signal("a"), received_at=NOW)
    store.ingest(_signal("e"), received_at=NOW + timedelta(seconds=1))
    _set_watermark(path, 1)

    for _attempt in range(3):
        with pytest.raises(SignalBusWatermarkError):
            _store(path)
        assert _watermark(path) == 1


def test_a_readback_of_an_inconsistent_watermark_fails_closed(tmp_path: Path) -> None:
    """A store that was already open when the metadata drifted must not serve it."""

    path = tmp_path / "signal-bus.sqlite3"
    store = _store(path)
    store.ingest(_signal("a"), received_at=NOW)
    store.ingest(_signal("e"), received_at=NOW + timedelta(seconds=1))
    _set_watermark(path, 1)

    with pytest.raises(SignalBusWatermarkError):
        store.source_descriptor()


def test_explicit_audited_recovery_repairs_a_lagging_watermark(tmp_path: Path) -> None:
    path = tmp_path / "signal-bus.sqlite3"
    store = _store(path)
    store.ingest(_signal("a"), received_at=NOW)
    store.ingest(_signal("e"), received_at=NOW + timedelta(seconds=1))
    _set_watermark(path, 1)

    recovery = recover_signal_bus_high_watermark(
        path,
        acknowledgement="operator reconciled the ledger after a restore",
        now=NOW + timedelta(minutes=1),
    )

    assert recovery.previous_watermark == 1
    assert recovery.observed_max_sequence == 2
    assert recovery.recovered_watermark == 2
    assert recovery.acknowledgement == "operator reconciled the ledger after a restore"
    assert recovery.recovered_at == NOW + timedelta(minutes=1)
    assert _watermark(path) == 2
    assert _store(path).source_descriptor().high_watermark == 2


def test_recovery_appends_an_audit_row_for_every_repair(tmp_path: Path) -> None:
    path = tmp_path / "signal-bus.sqlite3"
    store = _store(path)
    store.ingest(_signal("a"), received_at=NOW)
    _set_watermark(path, 0)
    recover_signal_bus_high_watermark(
        path,
        acknowledgement="first repair",
        now=NOW + timedelta(minutes=1),
    )

    connection = sqlite3.connect(path)
    try:
        rows = connection.execute(
            "SELECT acknowledgement, previous_watermark, observed_max_sequence, "
            "recovered_watermark FROM signal_bus_watermark_recovery ORDER BY recovery_id"
        ).fetchall()
    finally:
        connection.close()

    assert rows == [("first repair", 0, 1, 1)]


def test_recovery_refuses_to_lower_the_watermark(tmp_path: Path) -> None:
    """Rows are append-only, so a watermark above them is data loss, not a metadata bug."""

    path = tmp_path / "signal-bus.sqlite3"
    store = _store(path)
    store.ingest(_signal("a"), received_at=NOW)
    _set_watermark(path, 9)

    with pytest.raises(SignalBusWatermarkError, match="never lowers"):
        recover_signal_bus_high_watermark(path, acknowledgement="try to lower it", now=NOW)

    assert _watermark(path) == 9


def test_recovery_refuses_a_consistent_watermark(tmp_path: Path) -> None:
    path = tmp_path / "signal-bus.sqlite3"
    store = _store(path)
    store.ingest(_signal("a"), received_at=NOW)

    with pytest.raises(SignalBusWatermarkError, match="already consistent"):
        recover_signal_bus_high_watermark(path, acknowledgement="nothing to do", now=NOW)


@pytest.mark.parametrize("acknowledgement", ("", "   ", "\t\n"))
def test_recovery_requires_a_nonempty_acknowledgement(
    tmp_path: Path,
    acknowledgement: str,
) -> None:
    path = tmp_path / "signal-bus.sqlite3"
    store = _store(path)
    store.ingest(_signal("a"), received_at=NOW)
    _set_watermark(path, 0)

    with pytest.raises(ValueError, match="acknowledgement"):
        recover_signal_bus_high_watermark(path, acknowledgement=acknowledgement, now=NOW)

    assert _watermark(path) == 0


def test_recovery_reads_no_environment_override(tmp_path: Path) -> None:
    """There is no env bypass: the reason is an argument, never a variable.

    The check walks the executable body only, so prose in the docstring cannot satisfy or
    break it.
    """

    import ast
    import inspect

    import rquant.signal_bus as signal_bus_module

    source = inspect.getsource(signal_bus_module.recover_signal_bus_high_watermark)
    tree = ast.parse(inspect.cleandoc(source))
    function = tree.body[0]
    assert isinstance(function, ast.FunctionDef)
    body = function.body[1:] if ast.get_docstring(function) else function.body
    names = {
        node.attr if isinstance(node, ast.Attribute) else node.id
        for statement in body
        for node in ast.walk(statement)
        if isinstance(node, (ast.Attribute, ast.Name))
    }
    assert names.isdisjoint({"environ", "getenv", "os"})
