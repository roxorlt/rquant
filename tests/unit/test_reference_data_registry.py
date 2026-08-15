from __future__ import annotations

import json
import os
import shutil
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

import rquant.reference_data_registry as registry_module
from rquant.live_contracts import ConsumerCursor, LiveChannel
from rquant.reference_data_registry import (
    ReadonlyReferenceRegistry,
    ReferenceDataConflictError,
    ReferenceDataIntegrityError,
    ReferenceDataset,
    ReferenceDataUnavailableError,
    ReferenceGenerationManifest,
    ReferencePublicationAuthenticationError,
    ReferencePublicationAuthenticator,
    ReferencePublicationCommitIntent,
    ReferencePublicationCompletionReceipt,
    ReferencePublicationDeadlineError,
    ReferenceRecord,
    ReferenceRegistry,
    reference_publication_commit_intent_path,
)

BASE = datetime(2026, 1, 1, tzinfo=UTC)


def _record(
    *,
    dataset_id: str = ReferenceDataset.ST_STATUS,
    key: str = "600000.SH",
    effective_from: datetime = BASE,
    effective_to: datetime | None = BASE + timedelta(days=10),
    revision: int = 1,
    first_available_at: datetime = BASE + timedelta(hours=1),
    payload: dict[str, object] | None = None,
    replacement_reason: str | None = None,
) -> ReferenceRecord:
    return ReferenceRecord(
        dataset_id=dataset_id,
        key=key,
        effective_from=effective_from,
        effective_to=effective_to,
        revision=revision,
        source="tushare",
        first_available_at=first_available_at,
        replacement_reason=replacement_reason,
        payload=payload or {"is_st": False},
    )


def _registry(tmp_path: Path) -> ReferenceRegistry:
    return ReferenceRegistry(
        tmp_path / "reference.sqlite",
        publication_authenticator=_publication_authenticator(),
    )


def _publication_authenticator(
    *,
    secret: bytes = b"reference-publication-test-secret-0001",
) -> ReferencePublicationAuthenticator:
    return ReferencePublicationAuthenticator(key_id="test-reference-v1", secret=secret)


def test_publication_authenticator_loads_only_private_canonical_credential(
    tmp_path: Path,
) -> None:
    path = tmp_path / "publication-hmac.json"
    path.write_bytes(
        b'{"key_id":"test-reference-v1","secret_hex":"'
        + b"reference-publication-test-secret-0001".hex().encode("ascii")
        + b'"}'
    )
    path.chmod(0o600)

    loaded = ReferencePublicationAuthenticator.from_file(path)

    assert loaded.key_id == "test-reference-v1"
    assert loaded.verify({"publication_id": "a" * 64}, loaded.sign({"publication_id": "a" * 64}))
    path.chmod(0o640)
    with pytest.raises(ReferencePublicationAuthenticationError, match="unsafe"):
        ReferencePublicationAuthenticator.from_file(path)


def test_readonly_registry_reads_published_generation_without_writing(
    tmp_path: Path,
) -> None:
    registry = _registry(tmp_path)
    registry.append(_record())
    published = registry.publish(published_at=BASE + timedelta(hours=2))
    before = registry.path.stat()

    readonly = ReadonlyReferenceRegistry(registry.path)

    assert readonly.current_manifest() == published
    assert readonly.as_of(
        dataset_id=ReferenceDataset.ST_STATUS,
        key="600000.SH",
        event_time=BASE + timedelta(days=1),
        decision_time=BASE + timedelta(hours=3),
        generation_id=published.generation_id,
    ).record.payload == {"is_st": False}
    after = registry.path.stat()
    assert (after.st_ino, after.st_size, after.st_mtime_ns) == (
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    )


def test_readonly_registry_does_not_create_or_initialize_missing_database(
    tmp_path: Path,
) -> None:
    path = tmp_path / "missing" / "reference.sqlite"

    with pytest.raises(ReferenceDataUnavailableError, match="unavailable"):
        ReadonlyReferenceRegistry(path)

    assert not path.exists()
    assert not path.parent.exists()


def test_readonly_registry_rejects_mutation(tmp_path: Path) -> None:
    registry = _registry(tmp_path)
    registry.append(_record())
    registry.publish(published_at=BASE + timedelta(hours=2))
    readonly = ReadonlyReferenceRegistry(registry.path)

    with pytest.raises(sqlite3.OperationalError, match="readonly"):
        readonly.append(
            _record(
                key="600001.SH",
                payload={"is_st": True},
            )
        )


def test_writer_registry_rejects_final_and_parent_symlinks(tmp_path: Path) -> None:
    real_path = tmp_path / "real" / "reference.sqlite"
    ReferenceRegistry(real_path)
    final_link = tmp_path / "reference-link.sqlite"
    final_link.symlink_to(real_path)

    with pytest.raises(ReferenceDataIntegrityError, match="symlink"):
        ReferenceRegistry(final_link)

    parent_link = tmp_path / "authority-link"
    parent_link.symlink_to(real_path.parent, target_is_directory=True)
    with pytest.raises(ReferenceDataIntegrityError, match="symlink"):
        ReferenceRegistry(parent_link / "other.sqlite")


def test_writer_registry_rejects_hardlink_and_unsafe_mode(tmp_path: Path) -> None:
    path = tmp_path / "reference.sqlite"
    ReferenceRegistry(path)
    assert path.stat().st_mode & 0o777 == 0o600

    hardlink = tmp_path / "reference-hardlink.sqlite"
    os.link(path, hardlink)
    with pytest.raises(ReferenceDataIntegrityError, match="link"):
        ReferenceRegistry(hardlink)

    hardlink.unlink()
    path.chmod(0o640)
    with pytest.raises(ReferenceDataIntegrityError, match="mode"):
        ReferenceRegistry(path)


def test_descriptor_attestation_prefers_linux_proc_fd(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_open = os.open
    descriptor_directory = tmp_path / "descriptor-directory"
    descriptor_directory.mkdir()
    opened: list[str] = []

    def open_proc_fixture(path: object, flags: int, *args: object) -> int:
        opened.append(os.fspath(path))
        if os.fspath(path) == "/proc/self/fd":
            return original_open(descriptor_directory, flags, *args)
        raise AssertionError("/dev/fd fallback must not be opened when /proc/self/fd works")

    monkeypatch.setattr(registry_module.os, "open", open_proc_fixture)

    registry_module._regular_descriptor_identities()

    assert opened == ["/proc/self/fd"]


def test_descriptor_attestation_fails_closed_without_fd_directory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def missing_fd_directory(path: object, flags: int, *args: object) -> int:
        raise FileNotFoundError(os.fspath(path))

    monkeypatch.setattr(registry_module.os, "open", missing_fd_directory)

    with pytest.raises(ReferenceDataIntegrityError, match="descriptor directory"):
        registry_module._regular_descriptor_identities()


def test_descriptor_attestation_has_hard_entry_limit() -> None:
    with pytest.raises(ReferenceDataIntegrityError, match="entry limit"):
        registry_module._regular_descriptor_identities(max_entries=1)


def test_registry_rejects_unregistered_same_inode_descriptor(tmp_path: Path) -> None:
    registry = _registry(tmp_path)
    extra_descriptor = os.open(registry.path, os.O_RDONLY)
    try:
        with pytest.raises(ReferenceDataIntegrityError, match="untracked SQLite"):
            registry.records(dataset_id=ReferenceDataset.ST_STATUS, key="600000.SH")
    finally:
        os.close(extra_descriptor)


def test_writer_registry_rejects_inode_replacement_during_connect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "reference.sqlite"
    ReferenceRegistry(path)
    replacement = tmp_path / "replacement.sqlite"
    ReferenceRegistry(replacement)
    original_connect = sqlite3.connect
    replaced = False

    def replace_after_connect(*args: object, **kwargs: object) -> sqlite3.Connection:
        nonlocal replaced
        connection = original_connect(*args, **kwargs)
        if not replaced:
            replaced = True
            os.replace(replacement, path)
        return connection

    monkeypatch.setattr(
        "rquant.reference_data_registry.sqlite3.connect",
        replace_after_connect,
    )

    with pytest.raises(ReferenceDataIntegrityError, match="identity"):
        ReferenceRegistry(path)


def test_writer_registry_rejects_aba_inode_during_sqlite_connect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = _registry(tmp_path)
    registry.append(_record())
    original_path = registry.path
    original_hold = tmp_path / "original-held.sqlite"
    replacement = tmp_path / "replacement.sqlite"
    shutil.copy2(original_path, replacement)
    replacement.chmod(0o600)
    original_connect = sqlite3.connect
    attacked = False

    def connect_through_aba(*args: object, **kwargs: object) -> sqlite3.Connection:
        nonlocal attacked
        if attacked or Path(args[0]) != original_path:
            return original_connect(*args, **kwargs)
        attacked = True
        os.replace(original_path, original_hold)
        os.replace(replacement, original_path)
        connection = original_connect(*args, **kwargs)
        os.replace(original_path, replacement)
        os.replace(original_hold, original_path)
        return connection

    monkeypatch.setattr(
        "rquant.reference_data_registry.sqlite3.connect",
        connect_through_aba,
    )

    with pytest.raises(ReferenceDataIntegrityError, match="connected database identity"):
        registry.records(dataset_id=ReferenceDataset.ST_STATUS, key="600000.SH")

    assert attacked is True
    assert (original_path.stat().st_dev, original_path.stat().st_ino) == registry._database_identity


def test_record_derives_stable_payload_and_record_hashes() -> None:
    first = _record(payload={"reason": None, "is_st": False})
    reordered = _record(payload={"is_st": False, "reason": None})

    assert first.payload_sha256 == reordered.payload_sha256
    assert first.record_id == reordered.record_id
    assert len(first.payload_sha256) == 64
    assert len(first.record_id) == 64

    with pytest.raises(ValidationError, match="payload_sha256"):
        ReferenceRecord.model_validate(
            {**_record().model_dump(mode="python"), "payload_sha256": "0" * 64}
        )


def test_record_requires_aware_ordered_times_and_revision_reason() -> None:
    with pytest.raises(ValidationError, match="timezone-aware"):
        _record(effective_from=datetime(2026, 1, 1))
    with pytest.raises(ValidationError, match="effective_to"):
        _record(effective_to=BASE)
    with pytest.raises(ValidationError, match="replacement_reason"):
        _record(revision=2)
    with pytest.raises(ValidationError, match="revision 1"):
        _record(replacement_reason="not a replacement")
    payload = _record().model_dump(mode="python")
    payload.pop("effective_from")
    with pytest.raises(ValidationError, match="effective_from"):
        ReferenceRecord.model_validate(payload)


@pytest.mark.parametrize(
    ("dataset_id", "payload"),
    [
        (ReferenceDataset.ST_STATUS, {"is_st": True, "name": "ST sample"}),
        (ReferenceDataset.SUSPENSION_STATUS, {"is_suspended": True}),
        (ReferenceDataset.LISTING_STATUS, {"status": "listed", "board": "main"}),
        (ReferenceDataset.BOARD_MEMBERSHIP, {"boards": ["SSE", "large_cap"]}),
        (ReferenceDataset.ADJUSTMENT_FACTOR, {"adj_factor": 3.125}),
        (ReferenceDataset.PRICE_LIMIT_REGIME, {"limit_percent": 10}),
    ],
)
def test_registry_keeps_strategy_reference_payloads_generic(
    tmp_path: Path,
    dataset_id: str,
    payload: dict[str, object],
) -> None:
    registry = _registry(tmp_path)
    record = _record(dataset_id=dataset_id, payload=payload)
    registry.append(record)
    registry.publish(published_at=BASE + timedelta(hours=2))

    observed = registry.as_of(
        dataset_id=dataset_id,
        key=record.key,
        event_time=BASE + timedelta(days=1),
        decision_time=BASE + timedelta(hours=2),
    )

    assert observed.record.payload == payload


def test_append_is_idempotent_but_never_silently_overwrites(tmp_path: Path) -> None:
    registry = _registry(tmp_path)
    original = _record()

    assert registry.append(original).inserted is True
    assert registry.append(original).inserted is False

    changed = _record(payload={"is_st": True})
    with pytest.raises(ReferenceDataConflictError, match="revision 1"):
        registry.append(changed)


def test_revision_is_append_only_sequential_and_available_time_is_monotonic(
    tmp_path: Path,
) -> None:
    registry = _registry(tmp_path)
    registry.append(_record())

    with pytest.raises(ReferenceDataConflictError, match="next revision"):
        registry.append(
            _record(
                revision=3,
                first_available_at=BASE + timedelta(hours=3),
                payload={"is_st": True},
                replacement_reason="late correction",
            )
        )
    with pytest.raises(ReferenceDataConflictError, match="first_available_at"):
        registry.append(
            _record(
                revision=2,
                first_available_at=BASE + timedelta(minutes=30),
                payload={"is_st": True},
                replacement_reason="late correction",
            )
        )

    revised = _record(
        revision=2,
        first_available_at=BASE + timedelta(hours=3),
        payload={"is_st": True},
        replacement_reason="late correction",
    )
    registry.append(revised)
    assert registry.records(dataset_id=revised.dataset_id, key=revised.key) == (
        _record(),
        revised,
    )


def test_as_of_uses_effective_period_and_decision_time_without_future_revision(
    tmp_path: Path,
) -> None:
    registry = _registry(tmp_path)
    original = _record()
    correction = _record(
        revision=2,
        first_available_at=BASE + timedelta(days=3),
        payload={"is_st": True},
        replacement_reason="exchange correction",
    )
    registry.append(original)
    registry.append(correction)
    registry.publish(published_at=BASE + timedelta(days=4))

    historical = registry.as_of(
        dataset_id=original.dataset_id,
        key=original.key,
        event_time=BASE + timedelta(days=2),
        decision_time=BASE + timedelta(days=2),
    )
    revised = registry.as_of(
        dataset_id=original.dataset_id,
        key=original.key,
        event_time=BASE + timedelta(days=2),
        decision_time=BASE + timedelta(days=4),
    )

    assert historical.record.revision == 1
    assert historical.record.payload == {"is_st": False}
    assert revised.record.revision == 2
    assert revised.record.payload == {"is_st": True}


def test_as_of_fails_closed_for_unknown_or_boundary_gap(tmp_path: Path) -> None:
    registry = _registry(tmp_path)
    record = _record()
    registry.append(record)
    registry.publish(published_at=BASE + timedelta(hours=2))

    with pytest.raises(ReferenceDataUnavailableError, match="not available"):
        registry.as_of(
            dataset_id=record.dataset_id,
            key=record.key,
            event_time=BASE + timedelta(days=1),
            decision_time=BASE + timedelta(minutes=30),
        )
    with pytest.raises(ReferenceDataUnavailableError, match="not effective"):
        registry.as_of(
            dataset_id=record.dataset_id,
            key=record.key,
            event_time=record.effective_to,
            decision_time=BASE + timedelta(hours=2),
        )


def test_overlapping_business_periods_are_rejected(tmp_path: Path) -> None:
    registry = _registry(tmp_path)
    registry.append(_record())

    with pytest.raises(ReferenceDataConflictError, match="overlap"):
        registry.append(
            _record(
                effective_from=BASE + timedelta(days=5),
                effective_to=BASE + timedelta(days=20),
                first_available_at=BASE + timedelta(hours=2),
            )
        )

    registry.append(
        _record(
            effective_from=BASE + timedelta(days=10),
            effective_to=None,
            first_available_at=BASE + timedelta(hours=2),
        )
    )


def test_publish_creates_immutable_hashed_generation_and_current_pointer(
    tmp_path: Path,
) -> None:
    registry = _registry(tmp_path)
    registry.append(_record())

    first = registry.publish(published_at=BASE + timedelta(hours=2))
    retry = registry.publish(published_at=BASE + timedelta(hours=2))
    pointer = registry.current_pointer()

    assert retry == first
    assert pointer.generation_id == first.generation_id
    assert pointer.manifest_sha256 == first.manifest_sha256
    assert first.row_count == 1
    assert len(first.generation_id) == 64
    assert len(first.manifest_sha256) == 64

    registry.append(
        _record(
            key="000001.SZ",
            first_available_at=BASE + timedelta(hours=3),
        )
    )
    second = registry.publish(published_at=BASE + timedelta(hours=4))
    assert second.previous_generation_id == first.generation_id
    assert registry.generation(first.generation_id) == first

    with pytest.raises(ValidationError, match="generation_id"):
        ReferenceGenerationManifest.model_validate(
            {**first.model_dump(mode="python"), "generation_id": "0" * 64}
        )


def test_generation_membership_freezes_late_records(tmp_path: Path) -> None:
    registry = _registry(tmp_path)
    first_record = _record()
    registry.append(first_record)
    first = registry.publish(published_at=BASE + timedelta(hours=2))
    late = _record(key="000001.SZ", first_available_at=BASE + timedelta(hours=3))
    registry.append(late)
    registry.publish(published_at=BASE + timedelta(hours=4))

    with pytest.raises(ReferenceDataUnavailableError, match="not present"):
        registry.as_of(
            dataset_id=late.dataset_id,
            key=late.key,
            event_time=BASE + timedelta(days=1),
            decision_time=BASE + timedelta(hours=4),
            generation_id=first.generation_id,
        )


def test_generation_stores_only_incremental_members_but_resolves_exact_snapshot(
    tmp_path: Path,
) -> None:
    registry = _registry(tmp_path)
    first_record = _record()
    registry.append(first_record)
    first = registry.publish(published_at=BASE + timedelta(hours=2))
    second_record = _record(key="000001.SZ", first_available_at=BASE + timedelta(hours=3))
    registry.append(second_record)
    second = registry.publish(published_at=BASE + timedelta(hours=4))

    with closing(sqlite3.connect(registry.path)) as connection:
        counts = dict(
            connection.execute(
                "SELECT generation_id, COUNT(*) FROM reference_generation_member "
                "GROUP BY generation_id"
            ).fetchall()
        )

    assert counts == {first.generation_id: 1, second.generation_id: 1}
    assert second.added_record_ids == (second_record.record_id,)
    assert (
        registry.as_of(
            dataset_id=first_record.dataset_id,
            key=first_record.key,
            event_time=BASE + timedelta(days=1),
            decision_time=BASE + timedelta(hours=4),
            generation_id=second.generation_id,
        ).record
        == first_record
    )


def test_append_many_and_publish_uses_one_write_transaction_and_is_restart_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = _registry(tmp_path)
    records = tuple(
        _record(key=f"{index:06d}.SZ", payload={"is_st": bool(index % 2)}) for index in range(500)
    )
    connect_count = 0
    original_connect = registry._connect

    def counted_connect() -> sqlite3.Connection:
        nonlocal connect_count
        connect_count += 1
        return original_connect()

    monkeypatch.setattr(registry, "_connect", counted_connect)
    results, manifest = registry.append_many_and_publish(
        records,
        published_at=BASE + timedelta(hours=2),
    )

    assert connect_count == 1
    assert sum(result.inserted for result in results) == 500
    assert manifest.row_count == 500
    reopened = ReferenceRegistry(registry.path)
    replay_results, replay_manifest = reopened.append_many_and_publish(
        records,
        published_at=BASE + timedelta(hours=2),
    )
    assert sum(result.inserted for result in replay_results) == 0
    assert replay_manifest == manifest


def test_deadline_compensation_restores_existing_current_and_removes_new_records(
    tmp_path: Path,
) -> None:
    registry = _registry(tmp_path)
    baseline_record = _record()
    registry.append(baseline_record)
    baseline = registry.publish(published_at=BASE + timedelta(hours=2))
    publication_at = BASE + timedelta(hours=3)
    deadline = publication_at + timedelta(seconds=1)
    late = deadline + timedelta(microseconds=1)
    clock_values = iter((publication_at, late))
    candidate = _record(
        key="000001.SZ",
        first_available_at=publication_at,
    )

    with pytest.raises(ReferencePublicationDeadlineError, match="after deadline"):
        registry.append_many_and_publish_before(
            (candidate,),
            published_at=publication_at,
            completion_clock=lambda: next(clock_values),
            not_after=deadline,
        )

    assert registry.current_manifest() == baseline
    assert registry.records(dataset_id=candidate.dataset_id, key=candidate.key) == ()
    with closing(sqlite3.connect(registry.path)) as connection:
        assert connection.execute("SELECT COUNT(*) FROM reference_generation").fetchone()[0] == 1


def test_deadline_publication_uses_proven_not_before_horizon_for_all_visibility(
    tmp_path: Path,
) -> None:
    registry = _registry(tmp_path)
    prepared = BASE + timedelta(hours=2)
    completed = prepared + timedelta(microseconds=1)
    visible_at = prepared + timedelta(microseconds=2)
    deadline = prepared + timedelta(seconds=1)
    candidate = _record(first_available_at=visible_at)
    clock_values = iter((prepared, completed))

    _results, manifest, _rollback = registry.append_many_and_publish_before(
        (candidate,),
        published_at=visible_at,
        completion_clock=lambda: next(clock_values),
        not_after=deadline,
    )

    stored = registry.records(dataset_id=candidate.dataset_id, key=candidate.key)[0]
    assert stored.first_available_at == visible_at
    assert manifest.published_at == visible_at
    assert registry.current_pointer().switched_at == visible_at
    with pytest.raises(ReferenceDataUnavailableError):
        registry.as_of(
            dataset_id=candidate.dataset_id,
            key=candidate.key,
            event_time=BASE + timedelta(days=1),
            decision_time=completed,
            generation_id=manifest.generation_id,
        )


def test_shared_publication_stays_staged_and_all_current_reads_fail_closed(
    tmp_path: Path,
) -> None:
    registry = _registry(tmp_path)
    baseline_record = _record(key="000001.SZ")
    registry.append(baseline_record)
    baseline = registry.publish(published_at=BASE + timedelta(hours=1, minutes=30))
    available_at = BASE + timedelta(hours=2)
    candidate = _record(first_available_at=available_at)
    publication_id = "a" * 64
    receipt_path = tmp_path / "completion" / f"{publication_id}.json"

    _results, manifest, _rollback = registry.append_many_and_publish_before(
        (candidate,),
        published_at=available_at,
        completion_clock=lambda: available_at,
        not_after=available_at + timedelta(seconds=1),
        retain_intent=True,
        publication_id=publication_id,
        completion_receipt_path=receipt_path,
    )

    with pytest.raises(ReferenceDataUnavailableError, match="pending"):
        registry.records(dataset_id=candidate.dataset_id, key=candidate.key)
    with pytest.raises(ReferenceDataUnavailableError, match="pending"):
        registry.generation(manifest.generation_id)
    with pytest.raises(ReferenceDataUnavailableError, match="pending"):
        registry.current_pointer()
    with pytest.raises(ReferenceDataUnavailableError, match="pending"):
        registry.as_of(
            dataset_id=candidate.dataset_id,
            key=candidate.key,
            event_time=BASE + timedelta(days=1),
            decision_time=available_at + timedelta(seconds=1),
        )
    with pytest.raises(ReferenceDataConflictError, match="pending"):
        registry.append(
            _record(
                key="000002.SZ",
                first_available_at=available_at,
            )
        )

    readonly = ReadonlyReferenceRegistry(registry.path)
    with pytest.raises(ReferenceDataUnavailableError, match="pending"):
        readonly.records(dataset_id=candidate.dataset_id, key=candidate.key)
    with pytest.raises(ReferenceDataUnavailableError, match="pending"):
        readonly.current_pointer()
    with pytest.raises(ReferenceDataUnavailableError, match="pending"):
        readonly.as_of(
            dataset_id=candidate.dataset_id,
            key=candidate.key,
            event_time=BASE + timedelta(days=1),
            decision_time=available_at + timedelta(seconds=1),
        )
    registry.compensate_publication(_rollback)
    assert registry.current_manifest() == baseline


def test_partial_completion_receipt_cannot_publish_staged_generation(
    tmp_path: Path,
) -> None:
    registry = _registry(tmp_path)
    available_at = BASE + timedelta(hours=2)
    candidate = _record(first_available_at=available_at)
    publication_id = "b" * 64
    receipt_path = tmp_path / "completion" / f"{publication_id}.json"
    target_cursor = ConsumerCursor(
        consumer_id="reference-publisher",
        channel=LiveChannel.REFERENCE_SLOW,
        source_generation_id="c" * 64,
        last_sequence=0,
        last_batch_id="reference-batch-0",
        last_content_sha256="d" * 64,
        updated_at=available_at,
    )
    _results, _manifest, rollback = registry.append_many_and_publish_before(
        (candidate,),
        published_at=available_at,
        completion_clock=lambda: available_at,
        not_after=available_at + timedelta(seconds=1),
        retain_intent=True,
        publication_id=publication_id,
        completion_receipt_path=receipt_path,
        target_cursor=target_cursor,
    )
    receipt_path.parent.mkdir(mode=0o700)
    receipt_path.write_text(
        json.dumps({"publication_id": publication_id}, separators=(",", ":")),
        encoding="utf-8",
    )
    receipt_path.chmod(0o600)

    with pytest.raises(ReferenceDataConflictError, match="receipt"):
        registry.complete_publication(rollback)
    with pytest.raises(ReferenceDataUnavailableError, match="pending"):
        registry.records(dataset_id=candidate.dataset_id, key=candidate.key)
    with pytest.raises(ReferenceDataUnavailableError, match="pending"):
        registry.current_pointer()


def test_same_uid_forged_matching_receipt_cannot_publish_staged_generation(
    tmp_path: Path,
) -> None:
    registry = _registry(tmp_path)
    available_at = BASE + timedelta(hours=2)
    deadline = available_at + timedelta(seconds=1)
    publication_id = "7" * 64
    receipt_path = tmp_path / "completion" / f"{publication_id}.json"
    target_cursor = ConsumerCursor(
        consumer_id="reference-publisher",
        channel=LiveChannel.REFERENCE_SLOW,
        source_generation_id="c" * 64,
        last_sequence=0,
        last_batch_id="reference-batch-0",
        last_content_sha256="d" * 64,
        updated_at=available_at,
    )
    candidate = _record(first_available_at=available_at)
    _results, manifest, rollback = registry.append_many_and_publish_before(
        (candidate,),
        published_at=available_at,
        completion_clock=lambda: available_at,
        not_after=deadline,
        retain_intent=True,
        publication_id=publication_id,
        completion_receipt_path=receipt_path,
        target_cursor=target_cursor,
    )
    trusted = registry.publication_authenticator
    assert trusted is not None
    stage_sha256 = registry.pending_publication_stage_sha256(rollback)
    intent = ReferencePublicationCommitIntent(
        publication_id=publication_id,
        registry_generation_id=manifest.generation_id,
        target_cursor=target_cursor,
        source_generation_id=target_cursor.source_generation_id,
        channel=target_cursor.channel,
        deadline=deadline,
        stage_sha256=stage_sha256,
        key_id=trusted.key_id,
    )
    attacker = _publication_authenticator(
        secret=b"attacker-controlled-reference-secret-0001",
    )
    forged = ReferencePublicationCompletionReceipt.create_authenticated(
        publication_id=publication_id,
        registry_generation_id=manifest.generation_id,
        target_cursor=target_cursor,
        source_generation_id=target_cursor.source_generation_id,
        channel=target_cursor.channel,
        deadline=deadline,
        durable_completed_at=available_at,
        intent_sha256=intent.content_sha256,
        stage_sha256=stage_sha256,
        authenticator=attacker,
    )
    receipt_path.parent.mkdir(mode=0o700)
    receipt_path.write_bytes(forged.canonical_json_bytes())
    receipt_path.chmod(0o600)

    with pytest.raises(ReferenceDataConflictError, match="authentication"):
        registry.complete_publication(rollback)


def test_restart_recovery_rejects_matching_receipt_signed_with_wrong_secret(
    tmp_path: Path,
) -> None:
    path = tmp_path / "reference.sqlite"
    trusted = _publication_authenticator()
    registry = ReferenceRegistry(path, publication_authenticator=trusted)
    available_at = BASE + timedelta(hours=2)
    deadline = available_at + timedelta(seconds=1)
    publication_id = "6" * 64
    receipt_path = tmp_path / "completion" / f"{publication_id}.json"
    target_cursor = ConsumerCursor(
        consumer_id="reference-publisher",
        channel=LiveChannel.REFERENCE_SLOW,
        source_generation_id="c" * 64,
        last_sequence=0,
        last_batch_id="reference-batch-0",
        last_content_sha256="d" * 64,
        updated_at=available_at,
    )
    candidate = _record(first_available_at=available_at)
    _results, manifest, rollback = registry.append_many_and_publish_before(
        (candidate,),
        published_at=available_at,
        completion_clock=lambda: available_at,
        not_after=deadline,
        retain_intent=True,
        publication_id=publication_id,
        completion_receipt_path=receipt_path,
        target_cursor=target_cursor,
    )
    stage_sha256 = registry.pending_publication_stage_sha256(rollback)
    intent = ReferencePublicationCommitIntent(
        publication_id=publication_id,
        registry_generation_id=manifest.generation_id,
        target_cursor=target_cursor,
        source_generation_id=target_cursor.source_generation_id,
        channel=target_cursor.channel,
        deadline=deadline,
        stage_sha256=stage_sha256,
        key_id=trusted.key_id,
    )
    attacker = _publication_authenticator(
        secret=b"attacker-controlled-reference-secret-0001",
    )
    forged = ReferencePublicationCompletionReceipt.create_authenticated(
        publication_id=publication_id,
        registry_generation_id=manifest.generation_id,
        target_cursor=target_cursor,
        source_generation_id=target_cursor.source_generation_id,
        channel=target_cursor.channel,
        deadline=deadline,
        durable_completed_at=available_at,
        intent_sha256=intent.content_sha256,
        stage_sha256=stage_sha256,
        authenticator=attacker,
    )
    receipt_path.parent.mkdir(mode=0o700)
    receipt_path.write_bytes(forged.canonical_json_bytes())
    receipt_path.chmod(0o600)

    reopened = ReferenceRegistry(path, publication_authenticator=trusted)

    with pytest.raises(ReferenceDataUnavailableError, match="pending"):
        reopened.records(dataset_id=candidate.dataset_id, key=candidate.key)
    reopened.compensate_publication(rollback)
    assert reopened.records(dataset_id=candidate.dataset_id, key=candidate.key) == ()
    with pytest.raises(ReferenceDataUnavailableError):
        reopened.current_pointer()


def test_restart_rolls_back_staged_registry_when_uncommitted_intent_survives(
    tmp_path: Path,
) -> None:
    path = tmp_path / "reference.sqlite"
    authenticator = _publication_authenticator()
    registry = ReferenceRegistry(path, publication_authenticator=authenticator)
    available_at = BASE + timedelta(hours=2)
    deadline = available_at + timedelta(seconds=1)
    publication_id = "e" * 64
    receipt_path = tmp_path / "completion" / f"{publication_id}.json"
    target_cursor = ConsumerCursor(
        consumer_id="reference-publisher",
        channel=LiveChannel.REFERENCE_SLOW,
        source_generation_id="c" * 64,
        last_sequence=0,
        last_batch_id="reference-batch-0",
        last_content_sha256="d" * 64,
        updated_at=available_at,
    )
    candidate = _record(first_available_at=available_at)
    _results, manifest, _rollback = registry.append_many_and_publish_before(
        (candidate,),
        published_at=available_at,
        completion_clock=lambda: available_at,
        not_after=deadline,
        retain_intent=True,
        publication_id=publication_id,
        completion_receipt_path=receipt_path,
        target_cursor=target_cursor,
    )
    stage_sha256 = registry.pending_publication_stage_sha256(_rollback)
    intent = ReferencePublicationCommitIntent(
        publication_id=publication_id,
        registry_generation_id=manifest.generation_id,
        target_cursor=target_cursor,
        source_generation_id=target_cursor.source_generation_id,
        channel=target_cursor.channel,
        deadline=deadline,
        stage_sha256=stage_sha256,
        key_id=authenticator.key_id,
    )
    receipt = ReferencePublicationCompletionReceipt.create_authenticated(
        publication_id=publication_id,
        registry_generation_id=manifest.generation_id,
        target_cursor=target_cursor,
        source_generation_id=target_cursor.source_generation_id,
        channel=target_cursor.channel,
        deadline=deadline,
        durable_completed_at=available_at,
        intent_sha256=intent.content_sha256,
        stage_sha256=stage_sha256,
        authenticator=authenticator,
    )
    receipt_path.parent.mkdir(mode=0o700)
    receipt_path.write_bytes(receipt.canonical_json_bytes())
    receipt_path.chmod(0o600)
    intent_path = reference_publication_commit_intent_path(receipt_path)
    intent_path.write_bytes(intent.canonical_json_bytes())
    intent_path.chmod(0o600)

    reopened = ReferenceRegistry(path, publication_authenticator=authenticator)

    with pytest.raises(ReferenceDataUnavailableError, match="pending"):
        reopened.records(dataset_id=candidate.dataset_id, key=candidate.key)
    reopened.compensate_publication(_rollback)
    assert reopened.records(dataset_id=candidate.dataset_id, key=candidate.key) == ()
    with pytest.raises(ReferenceDataUnavailableError):
        reopened.current_pointer()


def test_reopen_rolls_back_system_exit_after_registry_commit(tmp_path: Path) -> None:
    path = tmp_path / "reference.sqlite3"
    registry = ReferenceRegistry(path)
    provisional = BASE + timedelta(hours=2)
    candidate = _record(first_available_at=provisional)
    calls = 0

    def exit_after_commit() -> datetime:
        nonlocal calls
        calls += 1
        if calls == 1:
            return provisional
        raise SystemExit("injected exit after registry commit")

    with pytest.raises(SystemExit, match="injected exit"):
        registry.append_many_and_publish_before(
            (candidate,),
            published_at=provisional,
            completion_clock=exit_after_commit,
            not_after=provisional + timedelta(seconds=1),
        )

    reopened = ReferenceRegistry(path)
    assert reopened.records(dataset_id=candidate.dataset_id, key=candidate.key) == ()
    with pytest.raises(ReferenceDataUnavailableError):
        reopened.current_pointer()
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute("SELECT COUNT(*) FROM reference_generation").fetchone()[0] == 0


def test_rollback_switches_pointer_without_mutating_manifests(tmp_path: Path) -> None:
    registry = _registry(tmp_path)
    registry.append(_record())
    first = registry.publish(published_at=BASE + timedelta(hours=2))
    registry.append(_record(key="000001.SZ", first_available_at=BASE + timedelta(hours=3)))
    second = registry.publish(published_at=BASE + timedelta(hours=4))

    pointer = registry.rollback(
        first.generation_id,
        switched_at=BASE + timedelta(hours=5),
    )

    assert pointer.generation_id == first.generation_id
    assert pointer.previous_generation_id == second.generation_id
    assert registry.generation(first.generation_id) == first
    assert registry.generation(second.generation_id) == second
    assert ReferenceRegistry(registry.path).current_pointer() == pointer


def test_registry_reopens_with_wal_full_and_detects_manifest_tampering(
    tmp_path: Path,
) -> None:
    path = tmp_path / "reference.sqlite"
    registry = ReferenceRegistry(path)
    registry.append(_record())
    manifest = registry.publish(published_at=BASE + timedelta(hours=2))

    reopened = ReferenceRegistry(path)
    assert reopened.current_manifest() == manifest
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert connection.execute("PRAGMA synchronous").fetchone()[0] == 2
        connection.execute(
            "UPDATE reference_generation SET row_count = row_count + 1 WHERE generation_id = ?",
            (manifest.generation_id,),
        )
        connection.commit()
    with pytest.raises(ReferenceDataIntegrityError, match="manifest hash"):
        ReferenceRegistry(path)


def test_reopen_recomputes_record_payload_hash(tmp_path: Path) -> None:
    path = tmp_path / "reference.sqlite"
    registry = ReferenceRegistry(path)
    registry.append(_record())
    registry.publish(published_at=BASE + timedelta(hours=2))

    with closing(sqlite3.connect(path)) as connection:
        connection.execute(
            "UPDATE reference_record SET payload_json = ?",
            ('{"is_st":true}',),
        )
        connection.commit()

    with pytest.raises(ReferenceDataIntegrityError, match="record"):
        ReferenceRegistry(path)


def test_reopen_fails_closed_if_a_bypassed_writer_created_overlap(tmp_path: Path) -> None:
    path = tmp_path / "reference.sqlite"
    registry = ReferenceRegistry(path)
    registry.append(_record())
    overlapping = _record(
        effective_from=BASE + timedelta(days=5),
        effective_to=BASE + timedelta(days=20),
        first_available_at=BASE + timedelta(hours=2),
    )

    with closing(sqlite3.connect(path)) as connection:
        connection.execute(
            """
            INSERT INTO reference_record(
                record_id, dataset_id, business_key, effective_from, effective_to,
                revision, source, first_available_at, replacement_reason,
                payload_json, payload_sha256
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                overlapping.record_id,
                overlapping.dataset_id,
                overlapping.key,
                overlapping.effective_from.isoformat(),
                overlapping.effective_to.isoformat(),
                overlapping.revision,
                overlapping.source,
                overlapping.first_available_at.isoformat(),
                overlapping.replacement_reason,
                json.dumps(dict(overlapping.payload), sort_keys=True),
                overlapping.payload_sha256,
            ),
        )
        connection.commit()

    with pytest.raises(ReferenceDataIntegrityError, match="overlapping"):
        ReferenceRegistry(path)


def test_concurrent_exact_append_is_idempotent(tmp_path: Path) -> None:
    registry = _registry(tmp_path)
    record = _record()

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = tuple(executor.map(lambda _: registry.append(record), range(24)))

    assert sum(result.inserted for result in results) == 1
    assert registry.records(dataset_id=record.dataset_id, key=record.key) == (record,)


def test_concurrent_publish_converges_and_survives_reopen(tmp_path: Path) -> None:
    path = tmp_path / "reference.sqlite"
    registry = ReferenceRegistry(path)
    registry.append(_record())
    published_at = BASE + timedelta(hours=2)

    def publish(_: int) -> str:
        return ReferenceRegistry(path).publish(published_at=published_at).generation_id

    with ThreadPoolExecutor(max_workers=6) as executor:
        generation_ids = tuple(executor.map(publish, range(12)))

    assert len(set(generation_ids)) == 1
    reopened = ReferenceRegistry(path)
    assert reopened.current_pointer().generation_id == generation_ids[0]
