from __future__ import annotations

import hashlib
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from rquant.live_contracts import (
    BatchEnvelope,
    BatchPointer,
    BatchQualityStatus,
    ConsumerCursor,
    CurrentPointer,
    LiveChannel,
)
from rquant.live_spool import (
    LiveBatchSpool,
    LiveSpoolIntegrityError,
    ReferenceSourceBatchSigner,
    ReferenceSourceBatchVerifier,
)
from rquant.reference_data_registry import (
    ReferencePublicationAuthenticator,
    ReferencePublicationCommitIntent,
    ReferencePublicationCompletionReceipt,
)

NOW = datetime(2026, 7, 31, 1, 31, tzinfo=UTC)
STAGE_SHA256 = "7" * 64


def _publication_authenticator() -> ReferencePublicationAuthenticator:
    return ReferencePublicationAuthenticator(
        key_id="test-reference-v1",
        secret=b"reference-publication-test-secret-0001",
    )


def _source_signing_pair(
    tmp_path: Path,
) -> tuple[ReferenceSourceBatchSigner, ReferenceSourceBatchVerifier]:
    private_key = tmp_path / "source-ed25519"
    private_key.parent.mkdir(mode=0o700, parents=True)
    subprocess.run(
        ("ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(private_key)),
        check=True,
    )
    private_key.chmod(0o600)
    public_key = private_key.with_suffix(".pub").read_text(encoding="ascii").strip()
    return (
        ReferenceSourceBatchSigner(key_id="source-v1", private_key_path=private_key),
        ReferenceSourceBatchVerifier(key_id="source-v1", public_key=public_key),
    )


def _payload(sequence: int) -> bytes:
    return f"minute-{sequence}".encode()


def test_reference_source_signer_uses_bounded_absolute_ssh_keygen(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}

    def run(command: tuple[str, ...], **kwargs: object) -> SimpleNamespace:
        observed.update(command=command, kwargs=kwargs)
        Path(f"{command[-1]}.sig").write_bytes(b"signature")
        Path(f"{command[-1]}.sig").chmod(0o600)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr("rquant.live_spool.subprocess.run", run)
    signer = ReferenceSourceBatchSigner(key_id="source-v1", private_key="private-key")

    assert signer.sign(b"payload")
    command = observed["command"]
    kwargs = observed["kwargs"]
    assert isinstance(command, tuple)
    assert command[0] == "/usr/bin/ssh-keygen"
    assert kwargs == {
        "check": False,
        "stdin": subprocess.DEVNULL,
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL,
        "timeout": 5.0,
    }


def test_reference_source_signer_normalizes_ssh_keygen_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def timeout(*args: object, **kwargs: object) -> None:
        raise subprocess.TimeoutExpired(args[0], timeout=5.0)

    monkeypatch.setattr("rquant.live_spool.subprocess.run", timeout)
    signer = ReferenceSourceBatchSigner(key_id="source-v1", private_key="private-key")

    with pytest.raises(LiveSpoolIntegrityError, match="timed out"):
        signer.sign(b"payload")


def test_reference_source_verifier_uses_bounded_absolute_ssh_keygen(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}

    def run(command: tuple[str, ...], **kwargs: object) -> SimpleNamespace:
        observed.update(command=command, kwargs=kwargs)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr("rquant.live_spool.subprocess.run", run)
    verifier = ReferenceSourceBatchVerifier(
        key_id="source-v1",
        public_key="ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAITest",
    )

    assert verifier.verify(b"payload", "c2lnbmF0dXJl") is True
    command = observed["command"]
    kwargs = observed["kwargs"]
    assert isinstance(command, tuple)
    assert command[0] == "/usr/bin/ssh-keygen"
    assert kwargs == {
        "input": b"payload",
        "check": False,
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL,
        "timeout": 5.0,
    }


def _envelope(
    sequence: int,
    *,
    channel: LiveChannel = LiveChannel.MARKET_MINUTE,
    quality: BatchQualityStatus = BatchQualityStatus.PUBLISHED,
    payload: bytes | None = None,
) -> BatchEnvelope:
    body = payload if payload is not None else _payload(sequence)
    return BatchEnvelope(
        schema_version=1,
        channel=channel,
        dataset_id=channel.value,
        source=f"test.{channel.value}",
        source_request_id=f"request-{sequence}",
        batch_id=f"20260731-{sequence:06d}",
        sequence=sequence,
        revision=1,
        event_time_start=NOW + timedelta(minutes=sequence),
        event_time_end=NOW + timedelta(minutes=sequence),
        source_time=NOW + timedelta(minutes=sequence, seconds=1),
        received_at=NOW + timedelta(minutes=sequence, seconds=2),
        available_at=NOW + timedelta(minutes=sequence, seconds=2),
        row_count=1,
        content_sha256=hashlib.sha256(body).hexdigest(),
        quality_status=quality,
        degraded_reasons=("partial_source",)
        if quality in {BatchQualityStatus.DEGRADED, BatchQualityStatus.STALE}
        else (),
        producer_version="live-v1",
        producer_commit="a" * 40,
    )


def _reference_envelope(
    sequence: int,
    *,
    quality: BatchQualityStatus = BatchQualityStatus.PUBLISHED,
) -> tuple[BatchEnvelope, bytes]:
    payload = f"reference-{sequence}".encode()
    evidence_at = NOW + timedelta(seconds=sequence + 1)
    available_at = NOW + timedelta(seconds=sequence + 10)
    return (
        BatchEnvelope(
            schema_version=1,
            channel=LiveChannel.REFERENCE_SLOW,
            dataset_id="reference_slow",
            source="tushare.reference",
            source_request_id=f"reference-request-{sequence}",
            batch_id=f"reference-batch-{sequence}",
            sequence=sequence,
            revision=1,
            event_time_start=evidence_at,
            event_time_end=evidence_at,
            source_time=evidence_at,
            received_at=available_at,
            available_at=available_at,
            row_count=1,
            content_sha256=hashlib.sha256(payload).hexdigest(),
            quality_status=quality,
            degraded_reasons=("fixture_degraded",)
            if quality in {BatchQualityStatus.DEGRADED, BatchQualityStatus.STALE}
            else (),
            producer_version="reference-v1",
            producer_commit="a" * 40,
        ),
        payload,
    )


def _publish_live_batch(
    spool: LiveBatchSpool,
    envelope: BatchEnvelope,
    payload: bytes,
) -> BatchPointer:
    if envelope.channel is LiveChannel.REFERENCE_SLOW:
        return spool.publish(
            envelope,
            payload,
            completion_clock=lambda: envelope.available_at,
            not_after=envelope.available_at,
        )
    return spool.publish(envelope, payload)


def test_publish_is_immutable_ordered_and_replay_idempotent(tmp_path: Path) -> None:
    spool = LiveBatchSpool(tmp_path / "live")

    first = spool.publish(_envelope(0), _payload(0))
    replayed = spool.publish(_envelope(0), _payload(0))
    second = spool.publish(_envelope(1), _payload(1))

    assert replayed == first
    assert second.sequence == 1
    assert spool.current(LiveChannel.MARKET_MINUTE) == second
    records = spool.list_after(LiveChannel.MARKET_MINUTE, sequence=-1)
    assert [record.envelope.sequence for record in records] == [0, 1]
    assert spool.read_payload(records[0]) == _payload(0)


@pytest.mark.parametrize("channel", tuple(LiveChannel))
def test_authoritative_replay_is_idempotent_for_every_live_channel(
    tmp_path: Path,
    channel: LiveChannel,
) -> None:
    spool = LiveBatchSpool(tmp_path / channel.value)
    envelope = _envelope(0, channel=channel)

    first = _publish_live_batch(spool, envelope, _payload(0))
    replayed = _publish_live_batch(spool, envelope, _payload(0))

    assert replayed == first
    assert spool.current(channel) == first


def test_replay_repairs_current_after_crash_between_manifest_and_pointer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "live"
    spool = LiveBatchSpool(root)
    original_atomic_write = LiveBatchSpool._atomic_write
    crashed = False

    def crash_before_current(path: Path, payload: bytes) -> None:
        nonlocal crashed
        if path.parent.name == "current" and not crashed:
            crashed = True
            raise OSError("injected crash before current pointer")
        original_atomic_write(path, payload)

    monkeypatch.setattr(LiveBatchSpool, "_atomic_write", staticmethod(crash_before_current))
    with pytest.raises(OSError, match="injected crash"):
        spool.publish(_envelope(0), _payload(0))

    monkeypatch.setattr(LiveBatchSpool, "_atomic_write", staticmethod(original_atomic_write))
    restarted = LiveBatchSpool(root)
    repaired = restarted.publish(_envelope(0), _payload(0))

    assert restarted.current(LiveChannel.MARKET_MINUTE) == repaired
    assert restarted.source_descriptor(LiveChannel.MARKET_MINUTE).high_watermark == 0


def test_late_legacy_replay_rolls_back_current_without_deleting_immutable_batch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spool = LiveBatchSpool(tmp_path / "live")
    original_atomic_write = LiveBatchSpool._atomic_write

    def crash_before_current(path: Path, payload: bytes) -> None:
        if path.parent.name == "current":
            raise OSError("injected crash before current pointer")
        original_atomic_write(path, payload)

    monkeypatch.setattr(LiveBatchSpool, "_atomic_write", staticmethod(crash_before_current))
    with pytest.raises(OSError, match="injected crash"):
        spool.publish(_envelope(0), _payload(0))
    monkeypatch.setattr(LiveBatchSpool, "_atomic_write", staticmethod(original_atomic_write))
    spool._intent_path(LiveChannel.MARKET_MINUTE).unlink()

    manifest_path = spool._manifest_path(LiveChannel.MARKET_MINUTE, 0)
    payload_path = spool._payload_path(LiveChannel.MARKET_MINUTE, 0)
    with pytest.raises(
        LiveSpoolIntegrityError,
        match="pre-commit deadline|completed after deadline",
    ):
        LiveBatchSpool(spool.root).publish(
            _envelope(0),
            _payload(0),
            completion_clock=lambda: NOW + timedelta(microseconds=1),
            not_after=NOW,
        )

    assert not spool._current_path(LiveChannel.MARKET_MINUTE).exists()
    assert manifest_path.is_file()
    assert payload_path.read_bytes() == _payload(0)


def test_deadline_publish_preserves_proven_not_before_horizon(
    tmp_path: Path,
) -> None:
    spool = LiveBatchSpool(tmp_path / "live")
    committed_at = NOW + timedelta(seconds=3)
    visible_at = NOW + timedelta(seconds=4)
    envelope = BatchEnvelope.model_validate(
        _envelope(0).model_dump(mode="python")
        | {"received_at": visible_at, "available_at": visible_at}
    )

    pointer = spool.publish(
        envelope,
        _payload(0),
        completion_clock=lambda: committed_at,
        not_after=NOW + timedelta(seconds=4),
    )

    record = spool.list_after(LiveChannel.MARKET_MINUTE, sequence=-1)[0]
    assert record.envelope.available_at == visible_at
    assert record.envelope.received_at == visible_at
    assert pointer.published_at == visible_at


def test_restart_rolls_back_system_exit_after_current_replace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "live"
    spool = LiveBatchSpool(root)
    original_atomic_write = LiveBatchSpool._atomic_write

    def exit_after_current_replace(path: Path, payload: bytes) -> None:
        original_atomic_write(path, payload)
        if path.parent.name == "current":
            raise SystemExit("injected exit after current replace")

    monkeypatch.setattr(
        LiveBatchSpool,
        "_atomic_write",
        staticmethod(exit_after_current_replace),
    )
    with pytest.raises(SystemExit, match="injected exit"):
        spool.publish(
            _envelope(0),
            _payload(0),
            completion_clock=lambda: NOW + timedelta(microseconds=1),
            not_after=NOW + timedelta(seconds=1),
        )
    monkeypatch.setattr(LiveBatchSpool, "_atomic_write", staticmethod(original_atomic_write))

    restarted = LiveBatchSpool(root)
    assert restarted.current(LiveChannel.MARKET_MINUTE) is None
    assert tuple(restarted._channel_dir(LiveChannel.MARKET_MINUTE).iterdir()) == ()


def test_replay_fails_closed_when_current_exists_but_is_damaged(
    tmp_path: Path,
) -> None:
    spool = LiveBatchSpool(tmp_path / "live")
    spool.publish(_envelope(0), _payload(0))
    (spool.current_root / "market_minute.json").write_bytes(b"not-json")

    restarted = LiveBatchSpool(spool.root)
    with pytest.raises(LiveSpoolIntegrityError, match="current pointer is invalid"):
        restarted.publish(_envelope(0), _payload(0))


def test_replay_refuses_to_repair_current_when_immutable_prefix_is_corrupt(
    tmp_path: Path,
) -> None:
    spool = LiveBatchSpool(tmp_path / "live")
    spool.publish(_envelope(0), _payload(0))
    spool.publish(_envelope(1), _payload(1))
    spool._payload_path(LiveChannel.MARKET_MINUTE, 0).write_bytes(b"corrupt")
    spool._current_path(LiveChannel.MARKET_MINUTE).unlink()

    with pytest.raises(LiveSpoolIntegrityError, match="prefix payload"):
        LiveBatchSpool(spool.root).publish(_envelope(1), _payload(1))


@pytest.mark.parametrize(
    "updates, message",
    (
        ({"batch_id": "tampered-batch"}, "not recoverable"),
        ({"content_sha256": "f" * 64}, "not recoverable"),
        ({"revision": 2}, "not recoverable"),
        ({"source_generation_id": "f" * 64}, "not recoverable"),
    ),
)
def test_replay_fails_closed_when_valid_current_disagrees_with_immutable_batch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    updates: dict[str, object],
    message: str,
) -> None:
    spool = LiveBatchSpool(tmp_path / "live")
    previous = spool.publish(_envelope(0), _payload(0))
    original_atomic_write = LiveBatchSpool._atomic_write

    def crash_before_current(path: Path, payload: bytes) -> None:
        if path.parent.name == "current":
            raise OSError("injected crash before current pointer")
        original_atomic_write(path, payload)

    monkeypatch.setattr(LiveBatchSpool, "_atomic_write", staticmethod(crash_before_current))
    with pytest.raises(OSError, match="injected crash"):
        spool.publish(_envelope(1), _payload(1))
    monkeypatch.setattr(LiveBatchSpool, "_atomic_write", staticmethod(original_atomic_write))

    mismatched = previous.model_copy(update=updates)
    assert isinstance(mismatched, CurrentPointer)
    original_atomic_write(
        spool.current_root / "market_minute.json",
        LiveBatchSpool._json_bytes(mismatched),
    )

    with pytest.raises(LiveSpoolIntegrityError, match=message):
        LiveBatchSpool(spool.root).publish(_envelope(1), _payload(1))


def test_replay_refuses_missing_current_when_manifest_payload_pairs_are_not_exact(
    tmp_path: Path,
) -> None:
    spool = LiveBatchSpool(tmp_path / "live")
    spool.publish(_envelope(0), _payload(0))
    spool._current_path(LiveChannel.MARKET_MINUTE).unlink()
    spool._payload_path(LiveChannel.MARKET_MINUTE, 1).write_bytes(_payload(1))

    with pytest.raises(LiveSpoolIntegrityError, match="manifest/payload pairs"):
        LiveBatchSpool(spool.root).publish(_envelope(0), _payload(0))


@pytest.mark.parametrize(
    "updates",
    (
        {"channel": LiveChannel.DAILY_CLOSE},
        {"batch_id": "tampered-batch"},
        {"sequence": 1},
        {"revision": 2},
        {"content_sha256": "f" * 64},
        {"quality_status": BatchQualityStatus.DEGRADED},
        {"published_at": NOW + timedelta(seconds=1)},
        {"source_generation_id": "f" * 64},
    ),
)
def test_current_fails_closed_when_pointer_is_not_fully_bound_to_manifest(
    tmp_path: Path,
    updates: dict[str, object],
) -> None:
    spool = LiveBatchSpool(tmp_path / "live")
    pointer = spool.publish(_envelope(0), _payload(0))
    tampered = CurrentPointer.model_construct(**(pointer.model_dump(mode="python") | updates))
    spool._atomic_write(
        spool._current_path(LiveChannel.MARKET_MINUTE),
        spool._json_bytes(tampered),
    )

    with pytest.raises(LiveSpoolIntegrityError, match="current pointer|immutable prefix"):
        spool.current(LiveChannel.MARKET_MINUTE)


def test_current_and_next_publish_fail_closed_when_manifest_identity_is_tampered(
    tmp_path: Path,
) -> None:
    spool = LiveBatchSpool(tmp_path / "live")
    spool.publish(_envelope(0), _payload(0))
    manifest_path = spool._manifest_path(LiveChannel.MARKET_MINUTE, 0)
    manifest = BatchEnvelope.model_validate_json(manifest_path.read_bytes())
    tampered = BatchEnvelope.model_validate(
        manifest.model_dump(mode="python") | {"batch_id": "tampered-batch"}
    )
    spool._atomic_write(manifest_path, spool._json_bytes(tampered))

    with pytest.raises(LiveSpoolIntegrityError, match="current pointer"):
        spool.current(LiveChannel.MARKET_MINUTE)
    with pytest.raises(LiveSpoolIntegrityError, match="current pointer"):
        spool.publish(_envelope(1), _payload(1))


@pytest.mark.parametrize("missing_kind", ("manifest", "payload"))
def test_current_and_next_publish_require_complete_immutable_prefix(
    tmp_path: Path,
    missing_kind: str,
) -> None:
    spool = LiveBatchSpool(tmp_path / "live")
    spool.publish(_envelope(0), _payload(0))
    spool.publish(_envelope(1), _payload(1))
    missing_path = (
        spool._manifest_path(LiveChannel.MARKET_MINUTE, 0)
        if missing_kind == "manifest"
        else spool._payload_path(LiveChannel.MARKET_MINUTE, 0)
    )
    missing_path.unlink()

    with pytest.raises(LiveSpoolIntegrityError, match="immutable prefix"):
        spool.current(LiveChannel.MARKET_MINUTE)
    with pytest.raises(LiveSpoolIntegrityError, match="immutable prefix"):
        spool.publish(_envelope(2), _payload(2))


@pytest.mark.parametrize(
    "updates",
    (
        {"available_at": NOW + timedelta(seconds=11)},
        {"producer_commit": "b" * 40},
        {"source_time": NOW + timedelta(seconds=12)},
    ),
)
def test_reference_current_and_next_publish_bind_every_manifest_to_its_receipt(
    tmp_path: Path,
    updates: dict[str, object],
) -> None:
    spool = LiveBatchSpool(tmp_path / "live")
    first, first_payload = _reference_envelope(0)
    second, second_payload = _reference_envelope(1)
    spool.publish(
        first,
        first_payload,
        completion_clock=lambda: NOW + timedelta(seconds=3),
        not_after=NOW + timedelta(seconds=20),
    )
    spool.publish(
        second,
        second_payload,
        completion_clock=lambda: NOW + timedelta(seconds=4),
        not_after=NOW + timedelta(seconds=20),
    )
    first_path = spool._manifest_path(LiveChannel.REFERENCE_SLOW, 0)
    stored = BatchEnvelope.model_validate_json(first_path.read_bytes())
    tampered = BatchEnvelope.model_validate(stored.model_dump(mode="python") | updates)
    spool._atomic_write(first_path, spool._json_bytes(tampered))

    with pytest.raises(LiveSpoolIntegrityError, match="completion receipt|evidence"):
        spool.current(LiveChannel.REFERENCE_SLOW)
    with pytest.raises(LiveSpoolIntegrityError, match="completion receipt|evidence"):
        spool.publish(
            _reference_envelope(2)[0],
            _reference_envelope(2)[1],
            completion_clock=lambda: NOW + timedelta(seconds=5),
            not_after=NOW + timedelta(seconds=20),
        )


def test_reference_current_requires_receipt_for_every_prefix_manifest(
    tmp_path: Path,
) -> None:
    spool = LiveBatchSpool(tmp_path / "live")
    for sequence in range(2):
        envelope, payload = _reference_envelope(sequence)
        spool.publish(
            envelope,
            payload,
            completion_clock=lambda: NOW + timedelta(seconds=4),
            not_after=NOW + timedelta(seconds=20),
        )
    spool._publication_receipt_path(LiveChannel.REFERENCE_SLOW, 0).unlink()

    with pytest.raises(LiveSpoolIntegrityError, match="completion receipt"):
        spool.current(LiveChannel.REFERENCE_SLOW)
    next_envelope, next_payload = _reference_envelope(2)
    with pytest.raises(LiveSpoolIntegrityError, match="completion receipt"):
        spool.publish(
            next_envelope,
            next_payload,
            completion_clock=lambda: NOW + timedelta(seconds=5),
            not_after=NOW + timedelta(seconds=20),
        )


def test_reference_source_batch_signature_rejects_unsigned_and_forged_batches(
    tmp_path: Path,
) -> None:
    signer, verifier = _source_signing_pair(tmp_path / "trusted")
    envelope, payload = _reference_envelope(0)
    signed = LiveBatchSpool(tmp_path / "signed", source_signer=signer)
    signed.publish(
        envelope,
        payload,
        completion_clock=lambda: NOW + timedelta(seconds=3),
        not_after=NOW + timedelta(seconds=20),
    )
    consumer = LiveBatchSpool(
        signed.root,
        read_only=True,
        source_verifier=verifier,
    )
    record = consumer.list_after(LiveChannel.REFERENCE_SLOW, sequence=-1)[0]
    consumer.verify_reference_source_record(record)

    signature_path = signed._source_signature_path(LiveChannel.REFERENCE_SLOW, 0)
    signature = signature_path.read_text(encoding="utf-8")
    replacement = "A" if signature[-3] != "A" else "B"
    signed._atomic_write(
        signature_path,
        (signature[:-3] + replacement + signature[-2:]).encode(),
    )
    with pytest.raises(LiveSpoolIntegrityError, match="signature"):
        consumer.verify_reference_source_record(record)

    unsigned = LiveBatchSpool(tmp_path / "unsigned")
    unsigned.publish(
        envelope,
        payload,
        completion_clock=lambda: NOW + timedelta(seconds=3),
        not_after=NOW + timedelta(seconds=20),
    )
    unsigned_consumer = LiveBatchSpool(
        unsigned.root,
        read_only=True,
        source_verifier=verifier,
    )
    unsigned_record = unsigned_consumer.list_after(
        LiveChannel.REFERENCE_SLOW,
        sequence=-1,
    )[0]
    with pytest.raises(LiveSpoolIntegrityError, match="signature"):
        unsigned_consumer.verify_reference_source_record(unsigned_record)


def test_reference_source_reader_rejects_symlinked_payload(tmp_path: Path) -> None:
    signer, verifier = _source_signing_pair(tmp_path / "trusted")
    envelope, payload = _reference_envelope(0)
    producer = LiveBatchSpool(tmp_path / "source", source_signer=signer)
    producer.publish(
        envelope,
        payload,
        completion_clock=lambda: NOW + timedelta(seconds=3),
        not_after=NOW + timedelta(seconds=20),
    )
    payload_path = producer._payload_path(LiveChannel.REFERENCE_SLOW, 0)
    external = tmp_path / "external.payload"
    payload_path.replace(external)
    payload_path.symlink_to(external)
    consumer = LiveBatchSpool(
        producer.root,
        read_only=True,
        source_verifier=verifier,
    )

    with pytest.raises(LiveSpoolIntegrityError, match="unsafe|symlink"):
        consumer.current(LiveChannel.REFERENCE_SLOW)


def test_reference_list_after_is_bounded_and_does_not_glob_history(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spool = LiveBatchSpool(tmp_path / "source")
    for sequence in range(140):
        envelope, payload = _reference_envelope(sequence)
        spool.publish(
            envelope,
            payload,
            completion_clock=lambda envelope=envelope: envelope.available_at,
            not_after=NOW + timedelta(hours=1),
        )

    def forbid_glob(*args: object, **kwargs: object) -> None:
        raise AssertionError("reference spool must not glob full history")

    monkeypatch.setattr(Path, "glob", forbid_glob)

    records = spool.list_after(LiveChannel.REFERENCE_SLOW, sequence=119, limit=8)

    assert [record.envelope.sequence for record in records] == list(range(120, 128))


def test_reference_retention_never_passes_slow_consumer_and_keeps_cold_audit(
    tmp_path: Path,
) -> None:
    signer, verifier = _source_signing_pair(tmp_path / "trusted")
    spool = LiveBatchSpool(
        tmp_path / "source",
        source_signer=signer,
        source_verifier=verifier,
    )
    for sequence in range(6):
        envelope, payload = _reference_envelope(sequence)
        spool.publish(
            envelope,
            payload,
            completion_clock=lambda envelope=envelope: envelope.available_at,
            not_after=NOW + timedelta(hours=1),
        )
    descriptor = spool.source_descriptor(LiveChannel.REFERENCE_SLOW)
    cursor = ConsumerCursor(
        consumer_id="reference-publisher",
        channel=LiveChannel.REFERENCE_SLOW,
        source_generation_id=descriptor.generation_id,
        last_sequence=2,
        last_batch_id="reference-batch-2",
        last_content_sha256=_reference_envelope(2)[0].content_sha256,
        updated_at=NOW + timedelta(minutes=1),
    )

    receipt = spool.retire_reference_batches_single_consumer(
        cursor=cursor,
        retain_hot_batches=0,
        max_batches=16,
        retired_at=NOW + timedelta(minutes=2),
    )

    assert receipt is not None
    assert receipt.retired_through_sequence == 2
    assert all(
        not spool._manifest_path(LiveChannel.REFERENCE_SLOW, sequence).exists()
        for sequence in range(3)
    )
    assert all(
        spool._manifest_path(LiveChannel.REFERENCE_SLOW, sequence).exists()
        for sequence in range(3, 6)
    )
    assert all(path.exists() for path in spool.reference_archive_paths(0))
    assert spool.reference_retirement_receipt_path(2).exists()
    assert spool.current(LiveChannel.REFERENCE_SLOW) is not None


def test_reference_retention_resumes_after_crash_without_deleting_cold_copy(
    tmp_path: Path,
) -> None:
    signer, verifier = _source_signing_pair(tmp_path / "trusted")
    root = tmp_path / "source"
    spool = LiveBatchSpool(root, source_signer=signer, source_verifier=verifier)
    for sequence in range(4):
        envelope, payload = _reference_envelope(sequence)
        spool.publish(
            envelope,
            payload,
            completion_clock=lambda envelope=envelope: envelope.available_at,
            not_after=NOW + timedelta(hours=1),
        )
    descriptor = spool.source_descriptor(LiveChannel.REFERENCE_SLOW)
    cursor = ConsumerCursor(
        consumer_id="reference-publisher",
        channel=LiveChannel.REFERENCE_SLOW,
        source_generation_id=descriptor.generation_id,
        last_sequence=1,
        last_batch_id="reference-batch-1",
        last_content_sha256=_reference_envelope(1)[0].content_sha256,
        updated_at=NOW + timedelta(minutes=1),
    )

    def crash(stage: str) -> None:
        if stage == "after_first_archive_move":
            raise SystemExit("injected retention crash")

    with pytest.raises(SystemExit, match="retention crash"):
        spool.retire_reference_batches_single_consumer(
            cursor=cursor,
            retain_hot_batches=0,
            max_batches=16,
            retired_at=NOW + timedelta(minutes=2),
            fault_injector=crash,
        )

    restarted = LiveBatchSpool(root, source_signer=signer, source_verifier=verifier)
    receipt = restarted.retire_reference_batches_single_consumer(
        cursor=cursor,
        retain_hot_batches=0,
        max_batches=16,
        retired_at=NOW + timedelta(minutes=2),
    )

    assert receipt is not None
    assert receipt.retired_through_sequence == 1
    assert all(path.exists() for path in restarted.reference_archive_paths(0))
    assert all(path.exists() for path in restarted.reference_archive_paths(1))
    assert restarted.current(LiveChannel.REFERENCE_SLOW) is not None


def test_channel_generation_is_stable_and_cursor_rejects_another_generation(
    tmp_path: Path,
) -> None:
    first = LiveBatchSpool(tmp_path / "first")
    reopened = LiveBatchSpool(tmp_path / "first")
    rebuilt = LiveBatchSpool(tmp_path / "rebuilt")

    assert reopened.source_descriptor(LiveChannel.MARKET_MINUTE) == first.source_descriptor(
        LiveChannel.MARKET_MINUTE
    )
    assert (
        rebuilt.source_descriptor(LiveChannel.MARKET_MINUTE).generation_id
        != first.source_descriptor(LiveChannel.MARKET_MINUTE).generation_id
    )

    pointer = first.publish(_envelope(0), _payload(0))
    descriptor = first.source_descriptor(LiveChannel.MARKET_MINUTE)
    assert pointer.source_generation_id == descriptor.generation_id
    assert descriptor.high_watermark == 0

    with pytest.raises(LiveSpoolIntegrityError, match="generation"):
        first.commit_cursor(
            ConsumerCursor(
                consumer_id="feature-worker",
                channel=LiveChannel.MARKET_MINUTE,
                source_generation_id="b" * 64,
                last_sequence=0,
                last_batch_id=pointer.batch_id,
                last_content_sha256=pointer.content_sha256,
                updated_at=NOW,
            )
        )


def test_publish_rejects_sequence_gap_and_conflicting_replay(tmp_path: Path) -> None:
    spool = LiveBatchSpool(tmp_path / "live")
    spool.publish(_envelope(0), _payload(0))

    with pytest.raises(LiveSpoolIntegrityError, match="next sequence"):
        spool.publish(_envelope(2), _payload(2))
    conflicting = _payload(99)
    with pytest.raises(LiveSpoolIntegrityError, match="immutable"):
        spool.publish(_envelope(0, payload=conflicting), conflicting)


def test_publish_rejects_payload_hash_mismatch(
    tmp_path: Path,
) -> None:
    spool = LiveBatchSpool(tmp_path / "live")

    with pytest.raises(LiveSpoolIntegrityError, match="content hash"):
        spool.publish(_envelope(0), b"different")


def test_non_authoritative_batches_are_retained_without_advancing_current(
    tmp_path: Path,
) -> None:
    spool = LiveBatchSpool(tmp_path / "live")
    published = spool.publish(_envelope(0), _payload(0))

    for sequence, quality in enumerate(
        (
            BatchQualityStatus.DEGRADED,
            BatchQualityStatus.STALE,
            BatchQualityStatus.CANDIDATE,
            BatchQualityStatus.QUARANTINED,
        ),
        start=1,
    ):
        spool.publish(_envelope(sequence, quality=quality), _payload(sequence))

    assert spool.current(LiveChannel.MARKET_MINUTE) == published
    assert spool.source_descriptor(LiveChannel.MARKET_MINUTE).high_watermark == 0
    assert [
        record.envelope.quality_status
        for record in spool.list_after(
            LiveChannel.MARKET_MINUTE,
            sequence=-1,
        )
    ] == [
        BatchQualityStatus.PUBLISHED,
        BatchQualityStatus.DEGRADED,
        BatchQualityStatus.STALE,
        BatchQualityStatus.CANDIDATE,
        BatchQualityStatus.QUARANTINED,
    ]


@pytest.mark.parametrize("channel", tuple(LiveChannel))
@pytest.mark.parametrize(
    "quality",
    (
        BatchQualityStatus.DEGRADED,
        BatchQualityStatus.STALE,
        BatchQualityStatus.CANDIDATE,
        BatchQualityStatus.QUARANTINED,
    ),
)
def test_non_authoritative_replay_recovers_clear_crash_with_existing_current(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    channel: LiveChannel,
    quality: BatchQualityStatus,
) -> None:
    root = tmp_path / channel.value
    spool = LiveBatchSpool(root)
    current = _publish_live_batch(
        spool,
        _envelope(0, channel=channel),
        _payload(0),
    )
    envelope = _envelope(1, channel=channel, quality=quality)
    _publish_live_batch(spool, envelope, _payload(1))

    original_clear = LiveBatchSpool._clear_publication_intent

    def crash_after_clear(instance: LiveBatchSpool, crashed_channel: LiveChannel) -> None:
        original_clear(instance, crashed_channel)
        raise OSError("injected crash after publication intent clear")

    monkeypatch.setattr(LiveBatchSpool, "_clear_publication_intent", crash_after_clear)
    with pytest.raises(OSError, match="injected crash"):
        _publish_live_batch(spool, envelope, _payload(1))
    monkeypatch.setattr(LiveBatchSpool, "_clear_publication_intent", original_clear)

    intent = LiveBatchSpool(root)._load_publication_intent(channel)
    assert intent is not None
    assert intent.previous_pointer == current
    assert intent.advances_current is False

    restarted = LiveBatchSpool(root)
    replayed = _publish_live_batch(restarted, envelope, _payload(1))

    assert replayed.quality_status is quality
    assert restarted.current(channel) == current
    assert restarted._load_publication_intent(channel) is None


@pytest.mark.parametrize(
    "quality",
    (
        BatchQualityStatus.DEGRADED,
        BatchQualityStatus.STALE,
        BatchQualityStatus.CANDIDATE,
        BatchQualityStatus.QUARANTINED,
    ),
)
def test_reference_non_authoritative_batches_are_receipted_and_advance_sequence(
    tmp_path: Path,
    quality: BatchQualityStatus,
) -> None:
    spool = LiveBatchSpool(tmp_path / "reference")
    first, first_payload = _reference_envelope(0)
    _publish_live_batch(spool, first, first_payload)

    non_authoritative, non_authoritative_payload = _reference_envelope(
        1,
        quality=quality,
    )
    pointer = _publish_live_batch(spool, non_authoritative, non_authoritative_payload)
    next_envelope, next_payload = _reference_envelope(2)
    next_pointer = _publish_live_batch(spool, next_envelope, next_payload)

    assert pointer.quality_status is quality
    assert spool._publication_receipt_path(LiveChannel.REFERENCE_SLOW, 1).is_file()
    assert next_pointer.sequence == 2
    assert spool.current(LiveChannel.REFERENCE_SLOW) == next_pointer


@pytest.mark.parametrize(
    "crash_point",
    (
        "intent_before_write",
        "intent_after_write",
        "payload_before_write",
        "manifest_after_write",
        "intent_clear_before",
        "intent_clear_after",
    ),
)
@pytest.mark.parametrize(
    "quality",
    (BatchQualityStatus.PUBLISHED, BatchQualityStatus.DEGRADED),
)
def test_reference_replay_recovers_every_publication_crash_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    crash_point: str,
    quality: BatchQualityStatus,
) -> None:
    root = tmp_path / "reference"
    spool = LiveBatchSpool(root)
    first, first_payload = _reference_envelope(0)
    previous = _publish_live_batch(spool, first, first_payload)
    envelope, payload = _reference_envelope(1, quality=quality)
    payload_path = spool._payload_path(LiveChannel.REFERENCE_SLOW, 1)
    manifest_path = spool._manifest_path(LiveChannel.REFERENCE_SLOW, 1)
    original_atomic_write = LiveBatchSpool._atomic_write
    original_write_intent = LiveBatchSpool._write_publication_intent
    original_clear_intent = LiveBatchSpool._clear_publication_intent

    if crash_point == "intent_before_write":

        def crash_before_intent(_instance: LiveBatchSpool, _intent: object) -> None:
            raise OSError("injected crash before intent")

        monkeypatch.setattr(LiveBatchSpool, "_write_publication_intent", crash_before_intent)
    elif crash_point == "intent_after_write":

        def crash_after_intent(instance: LiveBatchSpool, intent: object) -> None:
            original_write_intent(instance, intent)  # type: ignore[arg-type]
            raise OSError("injected crash after intent")

        monkeypatch.setattr(LiveBatchSpool, "_write_publication_intent", crash_after_intent)
    elif crash_point == "payload_before_write":

        def crash_before_payload(path: Path, bytes_: bytes) -> None:
            if path == payload_path:
                raise OSError("injected crash before payload")
            original_atomic_write(path, bytes_)

        monkeypatch.setattr(LiveBatchSpool, "_atomic_write", staticmethod(crash_before_payload))
    elif crash_point == "manifest_after_write":

        def crash_after_manifest(path: Path, bytes_: bytes) -> None:
            original_atomic_write(path, bytes_)
            if path == manifest_path:
                raise OSError("injected crash after manifest")

        monkeypatch.setattr(LiveBatchSpool, "_atomic_write", staticmethod(crash_after_manifest))
    elif crash_point == "intent_clear_before":

        def crash_before_clear(_instance: LiveBatchSpool, _channel: LiveChannel) -> None:
            raise OSError("injected crash before intent clear")

        monkeypatch.setattr(LiveBatchSpool, "_clear_publication_intent", crash_before_clear)
    else:

        def crash_after_clear(instance: LiveBatchSpool, channel: LiveChannel) -> None:
            original_clear_intent(instance, channel)
            raise OSError("injected crash after intent clear")

        monkeypatch.setattr(LiveBatchSpool, "_clear_publication_intent", crash_after_clear)

    with pytest.raises(OSError, match="injected crash"):
        _publish_live_batch(spool, envelope, payload)
    monkeypatch.undo()

    restarted = LiveBatchSpool(root)
    replayed = _publish_live_batch(restarted, envelope, payload)

    if quality is BatchQualityStatus.PUBLISHED:
        assert restarted.current(LiveChannel.REFERENCE_SLOW) == replayed
    else:
        assert restarted.current(LiveChannel.REFERENCE_SLOW) == previous
        assert replayed.quality_status is quality
    assert restarted._publication_receipt_path(LiveChannel.REFERENCE_SLOW, 1).is_file()
    assert restarted._load_publication_intent(LiveChannel.REFERENCE_SLOW) is None


def test_reference_non_authoritative_replay_rejects_replaced_current_during_recovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "reference"
    spool = LiveBatchSpool(root)
    first, first_payload = _reference_envelope(0)
    current = _publish_live_batch(spool, first, first_payload)
    envelope, payload = _reference_envelope(1, quality=BatchQualityStatus.DEGRADED)
    _publish_live_batch(spool, envelope, payload)

    original_clear = LiveBatchSpool._clear_publication_intent

    def crash_before_clear(_instance: LiveBatchSpool, _channel: LiveChannel) -> None:
        raise OSError("injected crash before publication intent clear")

    monkeypatch.setattr(LiveBatchSpool, "_clear_publication_intent", crash_before_clear)
    with pytest.raises(OSError, match="injected crash"):
        _publish_live_batch(spool, envelope, payload)
    monkeypatch.setattr(LiveBatchSpool, "_clear_publication_intent", original_clear)

    tampered = current.model_copy(update={"content_sha256": "f" * 64})
    spool._atomic_write(
        spool._current_path(LiveChannel.REFERENCE_SLOW),
        spool._json_bytes(tampered),
    )

    with pytest.raises(LiveSpoolIntegrityError, match="current pointer"):
        _publish_live_batch(LiveBatchSpool(root), envelope, payload)


@pytest.mark.parametrize(
    "channel",
    tuple(channel for channel in LiveChannel if channel is not LiveChannel.REFERENCE_SLOW),
)
def test_non_authoritative_replay_rejects_replaced_current_during_recovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    channel: LiveChannel,
) -> None:
    root = tmp_path / channel.value
    spool = LiveBatchSpool(root)
    current = _publish_live_batch(
        spool,
        _envelope(0, channel=channel),
        _payload(0),
    )
    envelope = _envelope(1, channel=channel, quality=BatchQualityStatus.DEGRADED)
    _publish_live_batch(spool, envelope, _payload(1))

    original_clear = LiveBatchSpool._clear_publication_intent

    def crash_before_clear(_instance: LiveBatchSpool, _channel: LiveChannel) -> None:
        raise OSError("injected crash before publication intent clear")

    monkeypatch.setattr(LiveBatchSpool, "_clear_publication_intent", crash_before_clear)
    with pytest.raises(OSError, match="injected crash"):
        _publish_live_batch(spool, envelope, _payload(1))
    monkeypatch.setattr(LiveBatchSpool, "_clear_publication_intent", original_clear)

    tampered = current.model_copy(update={"content_sha256": "f" * 64})
    spool._atomic_write(spool._current_path(channel), spool._json_bytes(tampered))

    with pytest.raises(LiveSpoolIntegrityError, match="current pointer"):
        _publish_live_batch(LiveBatchSpool(root), envelope, _payload(1))


@pytest.mark.parametrize(
    "crash_point",
    (
        "intent_before_write",
        "intent_after_write",
        "payload_before_write",
        "manifest_after_write",
        "intent_clear_before",
        "intent_clear_after",
    ),
)
@pytest.mark.parametrize(
    "quality",
    (BatchQualityStatus.PUBLISHED, BatchQualityStatus.DEGRADED),
)
def test_replay_recovers_every_publication_crash_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    crash_point: str,
    quality: BatchQualityStatus,
) -> None:
    spool = LiveBatchSpool(tmp_path / "live")
    previous = spool.publish(_envelope(0), _payload(0))
    envelope = _envelope(1, quality=quality)
    payload_path = spool._payload_path(LiveChannel.MARKET_MINUTE, 1)
    manifest_path = spool._manifest_path(LiveChannel.MARKET_MINUTE, 1)
    original_atomic_write = LiveBatchSpool._atomic_write
    original_write_intent = LiveBatchSpool._write_publication_intent
    original_clear_intent = LiveBatchSpool._clear_publication_intent

    if crash_point == "intent_before_write":

        def crash_before_intent(_instance: LiveBatchSpool, _intent: object) -> None:
            raise OSError("injected crash before intent")

        monkeypatch.setattr(LiveBatchSpool, "_write_publication_intent", crash_before_intent)
    elif crash_point == "intent_after_write":

        def crash_after_intent(
            instance: LiveBatchSpool,
            intent: object,
        ) -> None:
            original_write_intent(instance, intent)  # type: ignore[arg-type]
            raise OSError("injected crash after intent")

        monkeypatch.setattr(LiveBatchSpool, "_write_publication_intent", crash_after_intent)
    elif crash_point == "payload_before_write":

        def crash_before_payload(path: Path, payload: bytes) -> None:
            if path == payload_path:
                raise OSError("injected crash before payload")
            original_atomic_write(path, payload)

        monkeypatch.setattr(LiveBatchSpool, "_atomic_write", staticmethod(crash_before_payload))
    elif crash_point == "manifest_after_write":

        def crash_after_manifest(path: Path, payload: bytes) -> None:
            original_atomic_write(path, payload)
            if path == manifest_path:
                raise OSError("injected crash after manifest")

        monkeypatch.setattr(LiveBatchSpool, "_atomic_write", staticmethod(crash_after_manifest))
    elif crash_point == "intent_clear_before":

        def crash_before_clear(_instance: LiveBatchSpool, _channel: LiveChannel) -> None:
            raise OSError("injected crash before intent clear")

        monkeypatch.setattr(LiveBatchSpool, "_clear_publication_intent", crash_before_clear)
    else:

        def crash_after_clear(instance: LiveBatchSpool, channel: LiveChannel) -> None:
            original_clear_intent(instance, channel)
            raise OSError("injected crash after intent clear")

        monkeypatch.setattr(LiveBatchSpool, "_clear_publication_intent", crash_after_clear)

    with pytest.raises(OSError, match="injected crash"):
        spool.publish(envelope, _payload(1))
    monkeypatch.undo()

    restarted = LiveBatchSpool(spool.root)
    replayed = restarted.publish(envelope, _payload(1))

    if quality is BatchQualityStatus.PUBLISHED:
        assert restarted.current(LiveChannel.MARKET_MINUTE) == replayed
    else:
        assert restarted.current(LiveChannel.MARKET_MINUTE) == previous
        assert replayed.quality_status is quality
    assert restarted._load_publication_intent(LiveChannel.MARKET_MINUTE) is None


def test_read_detects_payload_corruption(tmp_path: Path) -> None:
    spool = LiveBatchSpool(tmp_path / "live")
    spool.publish(_envelope(0), _payload(0))
    record = spool.list_after(LiveChannel.MARKET_MINUTE, sequence=-1)[0]
    record.payload_path.write_bytes(b"tampered")

    with pytest.raises(LiveSpoolIntegrityError, match="content hash"):
        spool.read_payload(record)


def test_list_after_detects_a_gap_before_current(tmp_path: Path) -> None:
    spool = LiveBatchSpool(tmp_path / "live")
    spool.publish(_envelope(0), _payload(0))
    spool.publish(_envelope(1), _payload(1))
    first = spool.list_after(LiveChannel.MARKET_MINUTE, sequence=-1)[0]
    first.manifest_path.unlink()

    with pytest.raises(LiveSpoolIntegrityError, match="immutable prefix"):
        spool.list_after(LiveChannel.MARKET_MINUTE, sequence=-1)


def test_consumer_cursor_is_persisted_independently_and_cannot_regress(
    tmp_path: Path,
) -> None:
    spool = LiveBatchSpool(tmp_path / "live")
    pointer = spool.publish(_envelope(0), _payload(0))
    cursor = ConsumerCursor(
        consumer_id="strategy-growth",
        channel=LiveChannel.MARKET_MINUTE,
        source_generation_id=spool.source_descriptor(LiveChannel.MARKET_MINUTE).generation_id,
        last_sequence=pointer.sequence,
        last_batch_id=pointer.batch_id,
        last_content_sha256=pointer.content_sha256,
        updated_at=NOW,
    )

    spool.commit_cursor(cursor)
    assert spool.load_cursor("strategy-growth", LiveChannel.MARKET_MINUTE) == cursor

    regressed = cursor.model_copy(
        update={
            "last_sequence": -1,
            "last_batch_id": None,
            "last_content_sha256": None,
        }
    )
    with pytest.raises(LiveSpoolIntegrityError, match="regress"):
        spool.commit_cursor(regressed)


def test_restart_rolls_back_system_exit_after_cursor_replace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "live"
    spool = LiveBatchSpool(root)
    pointer = spool.publish(_envelope(0), _payload(0))
    cursor = ConsumerCursor(
        consumer_id="reference-publisher",
        channel=LiveChannel.MARKET_MINUTE,
        source_generation_id=pointer.source_generation_id,
        last_sequence=pointer.sequence,
        last_batch_id=pointer.batch_id,
        last_content_sha256=pointer.content_sha256,
        updated_at=NOW,
    )
    cursor_path = spool._cursor_path(cursor.consumer_id, cursor.channel)
    original_atomic_write = LiveBatchSpool._atomic_write

    def exit_after_cursor_replace(path: Path, payload: bytes) -> None:
        original_atomic_write(path, payload)
        if path == cursor_path:
            raise SystemExit("injected exit after cursor replace")

    monkeypatch.setattr(
        LiveBatchSpool,
        "_atomic_write",
        staticmethod(exit_after_cursor_replace),
    )
    with pytest.raises(SystemExit, match="injected exit"):
        spool.commit_cursor_with_deadline(
            cursor,
            completion_clock=lambda: NOW,
            not_after=NOW + timedelta(seconds=1),
        )
    monkeypatch.setattr(LiveBatchSpool, "_atomic_write", staticmethod(original_atomic_write))

    restarted = LiveBatchSpool(root)
    assert restarted.load_cursor(cursor.consumer_id, cursor.channel) is None


def test_restart_rolls_back_when_late_receipt_cleanup_leaves_stale_marker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "live"
    authenticator = _publication_authenticator()
    spool = LiveBatchSpool(root, publication_authenticator=authenticator)
    pointer = spool.publish(_envelope(0), _payload(0))
    cursor = ConsumerCursor(
        consumer_id="reference-publisher",
        channel=LiveChannel.MARKET_MINUTE,
        source_generation_id=pointer.source_generation_id,
        last_sequence=pointer.sequence,
        last_batch_id=pointer.batch_id,
        last_content_sha256=pointer.content_sha256,
        updated_at=NOW,
    )
    publication_id = "c" * 64
    marker_path = spool.completion_receipt_path(publication_id)
    deadline = NOW + timedelta(seconds=1)
    spool.commit_cursor_with_deadline(
        cursor,
        completion_clock=lambda: NOW,
        not_after=deadline,
        retain_intent=True,
        publication_id=publication_id,
        completion_receipt_path=marker_path,
        registry_generation_id="d" * 64,
    )
    clock_values = iter((NOW, deadline + timedelta(microseconds=1)))
    original_unlink = Path.unlink

    def fail_marker_cleanup(path: Path, *args: object, **kwargs: object) -> None:
        if path == marker_path:
            raise OSError("injected marker cleanup failure")
        original_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", fail_marker_cleanup)
    with pytest.raises(OSError, match="marker cleanup failure"):
        spool.write_completion_receipt(
            publication_id=publication_id,
            registry_generation_id="d" * 64,
            cursor=cursor,
            stage_sha256=STAGE_SHA256,
            completion_clock=lambda: next(clock_values),
            not_after=deadline,
        )
    assert marker_path.is_file()
    monkeypatch.setattr(Path, "unlink", original_unlink)

    restarted = LiveBatchSpool(root, publication_authenticator=authenticator)
    assert restarted.load_cursor(cursor.consumer_id, cursor.channel) is None


def test_restart_rolls_back_when_process_exits_after_marker_replace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "live"
    authenticator = _publication_authenticator()
    spool = LiveBatchSpool(root, publication_authenticator=authenticator)
    pointer = spool.publish(_envelope(0), _payload(0))
    cursor = ConsumerCursor(
        consumer_id="reference-publisher",
        channel=LiveChannel.MARKET_MINUTE,
        source_generation_id=pointer.source_generation_id,
        last_sequence=pointer.sequence,
        last_batch_id=pointer.batch_id,
        last_content_sha256=pointer.content_sha256,
        updated_at=NOW,
    )
    publication_id = "9" * 64
    generation_id = "8" * 64
    marker_path = spool.completion_receipt_path(publication_id)
    spool.commit_cursor_with_deadline(
        cursor,
        completion_clock=lambda: NOW,
        not_after=NOW + timedelta(seconds=1),
        retain_intent=True,
        publication_id=publication_id,
        completion_receipt_path=marker_path,
        registry_generation_id=generation_id,
    )
    original_atomic_write = LiveBatchSpool._atomic_write

    def exit_after_marker_replace(path: Path, payload: bytes) -> None:
        original_atomic_write(path, payload)
        if path == marker_path:
            raise SystemExit("injected exit after marker replace")

    monkeypatch.setattr(
        LiveBatchSpool,
        "_atomic_write",
        staticmethod(exit_after_marker_replace),
    )
    with pytest.raises(SystemExit, match="after marker replace"):
        spool.write_completion_receipt(
            publication_id=publication_id,
            registry_generation_id=generation_id,
            cursor=cursor,
            stage_sha256=STAGE_SHA256,
            completion_clock=lambda: NOW,
            not_after=NOW + timedelta(seconds=1),
        )
    monkeypatch.setattr(LiveBatchSpool, "_atomic_write", staticmethod(original_atomic_write))

    assert marker_path.is_file()
    assert spool.completion_receipt_intent_path(publication_id).is_file()
    restarted = LiveBatchSpool(root, publication_authenticator=authenticator)
    assert restarted.load_cursor(cursor.consumer_id, cursor.channel) is None


def test_shared_completion_receipt_is_canonical_and_fully_bound(
    tmp_path: Path,
) -> None:
    authenticator = _publication_authenticator()
    spool = LiveBatchSpool(
        tmp_path / "live",
        publication_authenticator=authenticator,
    )
    pointer = spool.publish(_envelope(0), _payload(0))
    cursor = ConsumerCursor(
        consumer_id="reference-publisher",
        channel=LiveChannel.MARKET_MINUTE,
        source_generation_id=pointer.source_generation_id,
        last_sequence=pointer.sequence,
        last_batch_id=pointer.batch_id,
        last_content_sha256=pointer.content_sha256,
        updated_at=NOW,
    )
    publication_id = "e" * 64
    generation_id = "f" * 64
    marker_path = spool.completion_receipt_path(publication_id)
    spool.commit_cursor_with_deadline(
        cursor,
        completion_clock=lambda: NOW,
        not_after=NOW + timedelta(seconds=1),
        retain_intent=True,
        publication_id=publication_id,
        completion_receipt_path=marker_path,
        registry_generation_id=generation_id,
    )

    receipt = spool.write_completion_receipt(
        publication_id=publication_id,
        registry_generation_id=generation_id,
        cursor=cursor,
        stage_sha256=STAGE_SHA256,
        completion_clock=lambda: NOW,
        not_after=NOW + timedelta(seconds=1),
    )

    assert marker_path.read_bytes() == receipt.canonical_json_bytes()
    assert not spool.completion_receipt_intent_path(publication_id).exists()
    assert receipt == ReferencePublicationCompletionReceipt.model_validate_json(
        marker_path.read_bytes()
    )
    assert receipt.publication_id == publication_id
    assert receipt.registry_generation_id == generation_id
    assert receipt.target_cursor == cursor
    assert receipt.source_generation_id == pointer.source_generation_id
    assert receipt.channel is LiveChannel.MARKET_MINUTE
    assert receipt.deadline == NOW + timedelta(seconds=1)
    assert receipt.durable_completed_at == NOW
    assert len(receipt.content_sha256) == 64

    forged_intent = ReferencePublicationCommitIntent(
        publication_id=publication_id,
        registry_generation_id="0" * 64,
        target_cursor=cursor,
        source_generation_id=cursor.source_generation_id,
        channel=cursor.channel,
        deadline=NOW + timedelta(seconds=1),
        stage_sha256=STAGE_SHA256,
        key_id=authenticator.key_id,
    )
    forged = ReferencePublicationCompletionReceipt.create_authenticated(
        publication_id=publication_id,
        registry_generation_id="0" * 64,
        target_cursor=cursor,
        source_generation_id=cursor.source_generation_id,
        channel=cursor.channel,
        deadline=NOW + timedelta(seconds=1),
        durable_completed_at=NOW,
        intent_sha256=forged_intent.content_sha256,
        stage_sha256=STAGE_SHA256,
        authenticator=authenticator,
    )
    spool._atomic_write(marker_path, forged.canonical_json_bytes())
    with pytest.raises(LiveSpoolIntegrityError, match="does not match intent"):
        spool.load_cursor(cursor.consumer_id, cursor.channel)


def test_readonly_cursor_reader_does_not_create_or_write_missing_state(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "source"
    producer = LiveBatchSpool(source_root)
    pointer = producer.publish(_envelope(0), _payload(0))
    source_entries_before = tuple(
        sorted(path.relative_to(source_root) for path in source_root.rglob("*"))
    )
    cursor_parent = tmp_path / "publisher-state"
    cursor_parent.mkdir(mode=0o700)
    cursor_root = cursor_parent / "cursors"
    cursor_parent.chmod(0o500)
    try:
        consumer = LiveBatchSpool(
            source_root,
            cursor_root=cursor_root,
            read_only=True,
        )
    finally:
        cursor_parent.chmod(0o700)
    cursor = ConsumerCursor(
        consumer_id="feature-worker",
        channel=LiveChannel.MARKET_MINUTE,
        source_generation_id=consumer.source_descriptor(LiveChannel.MARKET_MINUTE).generation_id,
        last_sequence=0,
        last_batch_id=pointer.batch_id,
        last_content_sha256=pointer.content_sha256,
        updated_at=NOW,
    )

    assert consumer.load_cursor("feature-worker", LiveChannel.MARKET_MINUTE) is None
    with pytest.raises(LiveSpoolIntegrityError, match="read-only"):
        consumer.commit_cursor(cursor)
    assert (
        tuple(sorted(path.relative_to(source_root) for path in source_root.rglob("*")))
        == source_entries_before
    )
    assert not cursor_root.exists()
    with pytest.raises(LiveSpoolIntegrityError, match="read-only"):
        consumer.publish(_envelope(1), _payload(1))


def test_readonly_cursor_reader_rejects_unsafe_root_without_chmod(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    LiveBatchSpool(source_root)
    cursor_root = tmp_path / "publisher-state" / "cursors"
    cursor_root.mkdir(mode=0o755, parents=True)

    with pytest.raises(LiveSpoolIntegrityError, match="unsafe read-only cursor mode"):
        LiveBatchSpool(source_root, cursor_root=cursor_root, read_only=True)

    assert cursor_root.stat().st_mode & 0o777 == 0o755


def test_readonly_cursor_reader_does_not_recover_pending_intent(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    producer = LiveBatchSpool(source_root)
    pointer = producer.publish(_envelope(0), _payload(0))
    cursor_root = tmp_path / "publisher-state" / "cursors"
    publisher = LiveBatchSpool(source_root, cursor_root=cursor_root)
    cursor = ConsumerCursor(
        consumer_id="feature-worker",
        channel=LiveChannel.MARKET_MINUTE,
        source_generation_id=publisher.source_descriptor(LiveChannel.MARKET_MINUTE).generation_id,
        last_sequence=0,
        last_batch_id=pointer.batch_id,
        last_content_sha256=pointer.content_sha256,
        updated_at=NOW,
    )
    publisher.commit_cursor_with_deadline(
        cursor,
        completion_clock=lambda: NOW,
        not_after=NOW + timedelta(seconds=1),
        retain_intent=True,
    )
    intent_path = publisher._cursor_intent_path(
        "feature-worker",
        LiveChannel.MARKET_MINUTE,
    )
    before = {
        path.relative_to(cursor_root): path.read_bytes()
        for path in cursor_root.rglob("*")
        if path.is_file()
    }
    reader = LiveBatchSpool(source_root, cursor_root=cursor_root, read_only=True)

    with pytest.raises(LiveSpoolIntegrityError, match="writer recovery"):
        reader.load_cursor("feature-worker", LiveChannel.MARKET_MINUTE)

    after = {
        path.relative_to(cursor_root): path.read_bytes()
        for path in cursor_root.rglob("*")
        if path.is_file()
    }
    assert intent_path.exists()
    assert after == before


def test_source_readonly_publisher_can_write_only_its_cursor(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    producer = LiveBatchSpool(source_root)
    pointer = producer.publish(_envelope(0), _payload(0))
    publisher = LiveBatchSpool(
        source_root,
        cursor_root=tmp_path / "publisher-state" / "cursors",
        source_read_only=True,
    )
    cursor = ConsumerCursor(
        consumer_id="feature-worker",
        channel=LiveChannel.MARKET_MINUTE,
        source_generation_id=publisher.source_descriptor(LiveChannel.MARKET_MINUTE).generation_id,
        last_sequence=0,
        last_batch_id=pointer.batch_id,
        last_content_sha256=pointer.content_sha256,
        updated_at=NOW,
    )

    publisher.commit_cursor(cursor)

    assert publisher.load_cursor("feature-worker", LiveChannel.MARKET_MINUTE) == cursor
    with pytest.raises(LiveSpoolIntegrityError, match="source read-only"):
        publisher.publish(_envelope(1), _payload(1))


def test_readonly_consumer_can_start_before_source_initializes_channel(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir(mode=0o700)
    before = tuple(source_root.iterdir())

    consumer = LiveBatchSpool(
        source_root,
        cursor_root=tmp_path / "consumer" / "cursors",
        read_only=True,
    )

    assert consumer.current(LiveChannel.MARKET_MINUTE) is None
    assert tuple(source_root.iterdir()) == before
