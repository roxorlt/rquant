from __future__ import annotations

import hashlib
import hmac
import json
import shutil
import sqlite3
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Barrier

import pytest

from rquant.research_run_spec import ResourceClass
from rquant.resource_admission import (
    AdmissionOutcome,
    AdmissionPolicy,
    AdmissionRequest,
    ResourceReservationIdentity,
    ResourceReservationLease,
    ResourceSnapshot,
    TradingSession,
)
from rquant.resource_journal_high_water import (
    RESOURCE_JOURNAL_ANTI_ROLLBACK_RECEIPT_NAMESPACE,
    RESOURCE_JOURNAL_HIGH_WATER_PURPOSE,
    ResourceJournalAntiRollbackReceipt,
    ResourceJournalHighWaterCheckpoint,
    ResourceJournalHighWaterError,
    SQLiteResourceJournalHighWaterAuthority,
)
from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_resource_admission import (
    RESOURCE_OPERATION_KEY_PURPOSE,
    TRUSTED_RESOURCE_ROLE_PURPOSES,
    ClosedResourceOperationKeyring,
    ResourceOperationConflictError,
    RuntimeResourceAdmissionError,
    SQLiteResourceAdmissionAuthority,
    SQLiteResourceReservationStore,
    TrustedRoleInventory,
    compose_production_resource_admission_authority,
)

NOW = datetime(2026, 8, 5, 8, 0, tzinfo=UTC)
TEST_ISSUER = "resource-authority-test-issuer"
ROOT_ISSUER = "resource-anti-rollback-root-test-issuer"


class _Signer:
    signature_algorithm = "ed25519"

    def __init__(
        self,
        *,
        key_id: str = "resource-test-key",
        secret: bytes = b"resource-operation-journal-test-secret" * 2,
        key_purpose: str = RESOURCE_OPERATION_KEY_PURPOSE,
        issuer: str = TEST_ISSUER,
    ) -> None:
        self.issuer = issuer
        self.key_id = key_id
        self.key_purpose = key_purpose
        self.public_key_fingerprint = hashlib.sha256(secret).hexdigest()
        self._secret = secret

    def sign(self, *, namespace: str, payload: bytes) -> str:
        message = namespace.encode("ascii") + b"\0" + payload
        return hmac.new(self._secret, message, hashlib.sha256).hexdigest()

    def verify(self, *, namespace: str, payload: bytes, signature: str) -> bool:
        return hmac.compare_digest(
            self.sign(namespace=namespace, payload=payload),
            signature,
        )


ROOT_SIGNER = _Signer(
    key_id="resource-root-test-key",
    secret=b"independent-resource-anti-rollback-root-key",
    key_purpose=RESOURCE_JOURNAL_HIGH_WATER_PURPOSE,
    issuer=ROOT_ISSUER,
)


def _role_inventory(
    *signers: _Signer,
    overrides: dict[str, frozenset[str]] | None = None,
) -> TrustedRoleInventory:
    records = signers or (_Signer(),)
    fingerprints = {
        purpose: frozenset({hashlib.sha256(f"role:{purpose}".encode()).hexdigest()})
        for purpose in TRUSTED_RESOURCE_ROLE_PURPOSES
    }
    fingerprints[RESOURCE_OPERATION_KEY_PURPOSE] = frozenset(
        signer.public_key_fingerprint for signer in records
    )
    fingerprints[RESOURCE_JOURNAL_HIGH_WATER_PURPOSE] = frozenset(
        {ROOT_SIGNER.public_key_fingerprint}
    )
    fingerprints.update(overrides or {})
    return TrustedRoleInventory(role_fingerprints=fingerprints)


def _keyring(*signers: _Signer) -> ClosedResourceOperationKeyring:
    records = signers or (_Signer(),)
    return ClosedResourceOperationKeyring(
        verifiers=records,
        trusted_issuer=TEST_ISSUER,
        trusted_role_inventory=_role_inventory(*records),
    )


def _identity(*, worker_id: str = "worker-a", attempt: int = 1) -> ResourceReservationIdentity:
    return ResourceReservationIdentity(
        job_id=f"00000000-0000-0000-0000-{attempt:012d}",
        run_id=f"{attempt:x}" * 64,
        shard_id=f"10000000-0000-0000-0000-{attempt:012d}",
        attempt_id=f"20000000-0000-0000-0000-{attempt:012d}",
        claim_generation=1,
        scheduler_fencing_token=1,
        worker_id=worker_id,
    )


def _request(identity: ResourceReservationIdentity) -> AdmissionRequest:
    return AdmissionRequest(
        job_id=str(identity.job_id),
        resource_class=ResourceClass.STANDARD,
        expected_memory_bytes=2 * 1024**3,
        expected_disk_bytes=1024,
        expected_quota_units=0,
        expected_duration_ms=1_000,
        source=None,
        preemptible=True,
        read_only=True,
        deadline=NOW + timedelta(minutes=30),
    )


def _policy() -> AdmissionPolicy:
    return AdmissionPolicy(
        allow_live_session=True,
        max_live_shard_duration_ms=5_000,
        max_snapshot_age_seconds=5,
        max_live_backlog_age_seconds=10,
        max_live_p95_latency_seconds=5,
        min_available_memory_bytes=0,
        min_available_disk_bytes=0,
        max_io_pressure_pct=100,
        max_cpu_load_pct=100,
        max_expected_memory_bytes=8 * 1024**3,
        max_expected_disk_bytes=8 * 1024**3,
        max_expected_quota_units=0,
        retry_delay_seconds=1,
    )


def _snapshot(
    observed_at: datetime = NOW,
    *,
    available_memory_bytes: int = 3 * 1024**3,
) -> ResourceSnapshot:
    return ResourceSnapshot(
        observed_at=observed_at,
        session=TradingSession.POST_MARKET,
        live_backlog_age_seconds=0,
        live_p95_latency_seconds=0,
        available_memory_bytes=available_memory_bytes,
        available_disk_bytes=20 * 1024**3,
        io_pressure_pct=0,
        cpu_load_pct=0,
        source_quota_remaining=0,
        live_healthy=True,
    )


def _authority(
    tmp_path: Path,
    *,
    current: list[datetime] | None = None,
    signer: _Signer | None = None,
    keyring: ClosedResourceOperationKeyring | None = None,
) -> SQLiteResourceAdmissionAuthority:
    values = current or [NOW]
    bound_signer = signer or _Signer()
    return SQLiteResourceAdmissionAuthority(
        tmp_path / "resource-authority-v3.sqlite3",
        authority_id="resource-authority-test",
        signer=bound_signer,
        keyring=keyring or _keyring(bound_signer),
        mode="test-standalone",
        clock=lambda: values[0],
    )


class _MemoryAntiRollbackRoot:
    authority_id = "independent-resource-anti-rollback-root"
    verifier_fingerprints = frozenset({ROOT_SIGNER.public_key_fingerprint})

    def __init__(self, *, lose_on_advance: int | None = None) -> None:
        self._current: dict[str, ResourceJournalAntiRollbackReceipt] = {}
        self._operations: dict[str, tuple[str, ResourceJournalAntiRollbackReceipt]] = {}
        self._lose_on_advance = lose_on_advance
        self._advance_count = 0

    def current(
        self,
        *,
        journal_authority_id: str,
    ) -> ResourceJournalAntiRollbackReceipt | None:
        return self._current.get(journal_authority_id)

    def pin(
        self,
        *,
        operation_id: str,
        high_water_authority_id: str,
        journal_authority_id: str,
        checkpoint: ResourceJournalHighWaterCheckpoint,
    ) -> ResourceJournalAntiRollbackReceipt:
        return self._close(
            operation_id=operation_id,
            high_water_authority_id=high_water_authority_id,
            journal_authority_id=journal_authority_id,
            previous_checkpoint_hash="0" * 64,
            checkpoint=checkpoint,
            advance=False,
        )

    def compare_and_advance(
        self,
        *,
        operation_id: str,
        high_water_authority_id: str,
        journal_authority_id: str,
        previous_checkpoint_hash: str,
        checkpoint: ResourceJournalHighWaterCheckpoint,
    ) -> ResourceJournalAntiRollbackReceipt:
        return self._close(
            operation_id=operation_id,
            high_water_authority_id=high_water_authority_id,
            journal_authority_id=journal_authority_id,
            previous_checkpoint_hash=previous_checkpoint_hash,
            checkpoint=checkpoint,
            advance=True,
        )

    def _close(
        self,
        *,
        operation_id: str,
        high_water_authority_id: str,
        journal_authority_id: str,
        previous_checkpoint_hash: str,
        checkpoint: ResourceJournalHighWaterCheckpoint,
        advance: bool,
    ) -> ResourceJournalAntiRollbackReceipt:
        request_hash = canonical_sha256(
            {
                "checkpoint": checkpoint,
                "high_water_authority_id": high_water_authority_id,
                "journal_authority_id": journal_authority_id,
                "operation_id": operation_id,
                "previous_checkpoint_hash": previous_checkpoint_hash,
            }
        )
        existing = self._operations.get(operation_id)
        if existing is not None:
            if existing[0] != request_hash:
                raise ResourceJournalHighWaterError("external root operation payload conflicts")
            return existing[1]
        current = self._current.get(journal_authority_id)
        if advance:
            if (
                current is None
                or previous_checkpoint_hash != current.checkpoint.checkpoint_hash
                or checkpoint.sequence != current.checkpoint.sequence + 1
                or checkpoint.previous_head_hash != current.checkpoint.head_hash
                or checkpoint.lineage_id != current.checkpoint.lineage_id
            ):
                raise ResourceJournalHighWaterError("external root compare-and-advance failed")
        elif current is not None or checkpoint.sequence != 0:
            raise ResourceJournalHighWaterError("external root pin conflicts")
        unsigned = ResourceJournalAntiRollbackReceipt(
            schema_version=1,
            contract="rquant-resource-journal-anti-rollback-receipt/v1",
            root_authority_id=self.authority_id,
            high_water_authority_id=high_water_authority_id,
            journal_authority_id=journal_authority_id,
            operation_id=operation_id,
            previous_checkpoint_hash=previous_checkpoint_hash,
            checkpoint=checkpoint,
            issuer=ROOT_SIGNER.issuer,
            key_id=ROOT_SIGNER.key_id,
            key_purpose=ROOT_SIGNER.key_purpose,
            namespace=RESOURCE_JOURNAL_ANTI_ROLLBACK_RECEIPT_NAMESPACE,
            signature_algorithm=ROOT_SIGNER.signature_algorithm,
            public_key_fingerprint=ROOT_SIGNER.public_key_fingerprint,
            signature="pending",
        )
        receipt = unsigned.model_copy(
            update={
                "signature": ROOT_SIGNER.sign(
                    namespace=RESOURCE_JOURNAL_ANTI_ROLLBACK_RECEIPT_NAMESPACE,
                    payload=unsigned.signing_bytes(),
                )
            }
        )
        self._current[journal_authority_id] = receipt
        self._operations[operation_id] = (request_hash, receipt)
        if advance:
            self._advance_count += 1
            if self._advance_count == self._lose_on_advance:
                raise ConnectionError("simulated external root commit-response loss")
        return receipt


def _high_water(
    tmp_path: Path,
    *,
    filename: str = "resource-journal-high-water.sqlite3",
    root: _MemoryAntiRollbackRoot | None = None,
    signer: _Signer | None = None,
) -> SQLiteResourceJournalHighWaterAuthority:
    journal_signer = signer or _Signer()
    return SQLiteResourceJournalHighWaterAuthority(
        tmp_path / filename,
        authority_id="resource-journal-cache",
        trusted_role_inventory=_role_inventory(journal_signer),
        journal_verifiers=(journal_signer,),
        trusted_journal_issuer=TEST_ISSUER,
        anti_rollback_root=root or _MemoryAntiRollbackRoot(),
        root_verifiers=(ROOT_SIGNER,),
        trusted_root_issuer=ROOT_ISSUER,
        mode="production",
    )


def _production_authority(
    tmp_path: Path,
    *,
    filename: str = "resource-authority-v3.sqlite3",
    high_water: SQLiteResourceJournalHighWaterAuthority | None = None,
) -> SQLiteResourceAdmissionAuthority:
    signer = _Signer()
    cache = high_water or _high_water(tmp_path, signer=signer)
    return SQLiteResourceAdmissionAuthority(
        tmp_path / filename,
        authority_id="resource-authority-test",
        signer=signer,
        keyring=_keyring(signer),
        high_water_authority=cache,
        mode="production",
        clock=lambda: NOW,
    )


def _reserve(
    authority: SQLiteResourceAdmissionAuthority,
    *,
    operation_id: str = "reserve-op",
    identity: ResourceReservationIdentity | None = None,
    snapshot_provider: Callable[[], ResourceSnapshot] = _snapshot,
):
    bound_identity = identity or _identity()
    return authority.reserve(
        operation_id=operation_id,
        identity=bound_identity,
        request=_request(bound_identity),
        policy=_policy(),
        snapshot_provider=snapshot_provider,
        lease_seconds=30,
    )


def test_reserve_journals_all_decisions_and_replay_never_resamples(tmp_path: Path) -> None:
    authority = _authority(tmp_path)
    calls = 0

    def deferred_snapshot() -> ResourceSnapshot:
        nonlocal calls
        calls += 1
        return _snapshot(available_memory_bytes=1)

    first = _reserve(authority, snapshot_provider=deferred_snapshot)
    replayed = _reserve(
        authority,
        snapshot_provider=lambda: pytest.fail("journal replay must not resample"),
    )

    assert first == replayed
    assert first.decision.outcome is AdmissionOutcome.DEFERRED
    assert first.lease is None
    assert first.receipt.closed is True
    assert calls == 1
    with sqlite3.connect(authority.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM resource_effect_operation").fetchone() == (
            1,
        )
        assert connection.execute("SELECT COUNT(*) FROM resource_reservation").fetchone() == (0,)


def test_operation_and_effect_keys_are_durable_idempotency_fences(tmp_path: Path) -> None:
    authority = _authority(tmp_path)
    first = _reserve(authority)
    identity = _identity()

    with pytest.raises(ResourceOperationConflictError, match="payload"):
        authority.reserve(
            operation_id="reserve-op",
            identity=identity,
            request=_request(identity).model_copy(update={"expected_disk_bytes": 2}),
            policy=_policy(),
            snapshot_provider=_snapshot,
            lease_seconds=30,
        )
    with pytest.raises(ResourceOperationConflictError, match="effect"):
        _reserve(authority, operation_id="another-reserve-op")

    assert authority.lookup(first.receipt.operation_id) == first


def test_rejected_reserve_is_journaled_without_creating_a_lease(tmp_path: Path) -> None:
    authority = _authority(tmp_path)
    identity = _identity()
    result = authority.reserve(
        operation_id="rejected-reserve-op",
        identity=identity,
        request=_request(identity).model_copy(update={"read_only": False}),
        policy=_policy(),
        snapshot_provider=_snapshot,
        lease_seconds=30,
    )

    assert result.decision is not None
    assert result.decision.outcome is AdmissionOutcome.REJECTED
    assert result.lease is None
    assert authority.lookup("rejected-reserve-op") == result
    assert authority.active_leases() == ()


def test_v3_never_reserves_source_quota_and_fails_closed_on_clock_rollback(
    tmp_path: Path,
) -> None:
    current = [NOW]
    authority = _authority(tmp_path, current=current)
    identity = _identity()
    quota_request = _request(identity).model_copy(
        update={"expected_quota_units": 1, "source": "tushare"}
    )

    with pytest.raises(RuntimeResourceAdmissionError, match="quota"):
        authority.reserve(
            operation_id="quota-reserve-op",
            identity=identity,
            request=quota_request,
            policy=_policy(),
            snapshot_provider=_snapshot,
            lease_seconds=30,
        )

    _reserve(authority, identity=identity)
    current[0] -= timedelta(seconds=1)
    with pytest.raises(RuntimeResourceAdmissionError, match="rollback"):
        _reserve(
            authority,
            operation_id="clock-rollback-op",
            identity=_identity(worker_id="worker-b", attempt=2),
        )


def test_concurrent_renew_and_release_cannot_bypass_the_prior_receipt_fence(
    tmp_path: Path,
) -> None:
    current = [NOW]
    authority = _authority(tmp_path, current=current)
    reserved = _reserve(authority)
    assert reserved.lease is not None
    barrier = Barrier(2)

    def renew() -> object:
        barrier.wait(timeout=2)
        try:
            return authority.renew(
                operation_id="concurrent-renew-op",
                lease=reserved.lease,
                identity=reserved.lease.identity,
                request=_request(reserved.lease.identity),
                policy=_policy(),
                snapshot_provider=lambda: _snapshot(current[0]),
                lease_seconds=30,
                prior_receipt=reserved.receipt,
            )
        except RuntimeResourceAdmissionError as exc:
            return exc

    def release() -> object:
        barrier.wait(timeout=2)
        try:
            return authority.release(
                operation_id="concurrent-release-op",
                lease=reserved.lease,
                identity=reserved.lease.identity,
                prior_receipt=reserved.receipt,
            )
        except RuntimeResourceAdmissionError as exc:
            return exc

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = tuple(
            future.result(timeout=5)
            for future in (executor.submit(renew), executor.submit(release))
        )

    assert sum(not isinstance(outcome, RuntimeResourceAdmissionError) for outcome in outcomes) == 1
    assert len(authority.active_leases()) in {0, 1}
    with sqlite3.connect(authority.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM resource_effect_operation").fetchone() == (
            2,
        )


def test_recheck_and_renew_bind_the_original_attempt_fence_and_prior_receipt(
    tmp_path: Path,
) -> None:
    current = [NOW]
    authority = _authority(tmp_path, current=current)
    reserved = _reserve(authority)
    assert reserved.lease is not None
    current[0] += timedelta(seconds=2)

    rechecked = authority.recheck(
        operation_id="recheck-op",
        lease=reserved.lease,
        identity=reserved.lease.identity,
        request=_request(reserved.lease.identity),
        policy=_policy(),
        snapshot_provider=lambda: _snapshot(current[0]),
        lease_seconds=30,
        prior_receipt=reserved.receipt,
    )
    assert rechecked.decision.outcome is AdmissionOutcome.ADMITTED
    assert rechecked.lease is not None
    assert rechecked.lease.expires_at == current[0] + timedelta(seconds=30)

    with pytest.raises(RuntimeResourceAdmissionError, match="fence|identity"):
        authority.renew(
            operation_id="wrong-fence",
            lease=rechecked.lease,
            identity=rechecked.lease.identity.model_copy(update={"scheduler_fencing_token": 2}),
            request=_request(rechecked.lease.identity),
            policy=_policy(),
            snapshot_provider=lambda: _snapshot(current[0]),
            lease_seconds=30,
            prior_receipt=rechecked.receipt,
        )
    with pytest.raises(RuntimeResourceAdmissionError, match="prior receipt"):
        authority.renew(
            operation_id="wrong-receipt",
            lease=rechecked.lease,
            identity=rechecked.lease.identity,
            request=_request(rechecked.lease.identity),
            policy=_policy(),
            snapshot_provider=lambda: _snapshot(current[0]),
            lease_seconds=30,
            prior_receipt=reserved.receipt,
        )

    renewed = authority.renew(
        operation_id="renew-op",
        lease=rechecked.lease,
        identity=rechecked.lease.identity,
        request=_request(rechecked.lease.identity),
        policy=_policy(),
        snapshot_provider=lambda: _snapshot(current[0]),
        lease_seconds=30,
        prior_receipt=rechecked.receipt,
    )
    assert renewed.lease is not None
    assert renewed.lease.expires_at == current[0] + timedelta(seconds=30)


def test_deferred_recheck_receipt_becomes_the_next_fence(tmp_path: Path) -> None:
    current = [NOW]
    authority = _authority(tmp_path, current=current)
    reserved = _reserve(authority)
    assert reserved.lease is not None
    current[0] += timedelta(seconds=1)

    deferred = authority.recheck(
        operation_id="deferred-recheck-op",
        lease=reserved.lease,
        identity=reserved.lease.identity,
        request=_request(reserved.lease.identity),
        policy=_policy(),
        snapshot_provider=lambda: _snapshot(current[0], available_memory_bytes=1),
        lease_seconds=30,
        prior_receipt=reserved.receipt,
    )
    assert deferred.decision is not None
    assert deferred.decision.outcome is AdmissionOutcome.DEFERRED
    assert deferred.lease == reserved.lease

    current[0] += timedelta(seconds=1)
    admitted = authority.recheck(
        operation_id="admitted-after-deferred-op",
        lease=deferred.lease,
        identity=deferred.lease.identity,
        request=_request(deferred.lease.identity),
        policy=_policy(),
        snapshot_provider=lambda: _snapshot(current[0]),
        lease_seconds=30,
        prior_receipt=deferred.receipt,
    )
    assert admitted.decision is not None
    assert admitted.decision.outcome is AdmissionOutcome.ADMITTED


def test_release_has_a_tombstone_and_response_loss_replays_closed_result(tmp_path: Path) -> None:
    authority = _authority(tmp_path)
    reserved = _reserve(authority)
    assert reserved.lease is not None

    first = authority.release(
        operation_id="release-op",
        lease=reserved.lease,
        identity=reserved.lease.identity,
        prior_receipt=reserved.receipt,
    )
    replayed = authority.release(
        operation_id="release-op",
        lease=reserved.lease,
        identity=reserved.lease.identity,
        prior_receipt=reserved.receipt,
    )

    assert first == replayed
    assert first.released is True
    assert first.lease is None
    with sqlite3.connect(authority.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM resource_reservation").fetchone() == (0,)
        assert connection.execute("SELECT COUNT(*) FROM resource_effect_operation").fetchone() == (
            2,
        )
    with pytest.raises(ResourceOperationConflictError, match="effect"):
        authority.release(
            operation_id="second-release-op",
            lease=reserved.lease,
            identity=reserved.lease.identity,
            prior_receipt=reserved.receipt,
        )

    restarted = _authority(tmp_path)
    assert restarted.lookup("release-op") == first
    assert restarted.active_leases() == ()
    assert (
        restarted.release(
            operation_id="release-op",
            lease=reserved.lease,
            identity=reserved.lease.identity,
            prior_receipt=reserved.receipt,
        )
        == first
    )


@pytest.mark.parametrize("column", ("result_json", "receipt_json", "applied_at"))
def test_journal_row_tampering_fails_closed(tmp_path: Path, column: str) -> None:
    authority = _authority(tmp_path)
    result = _reserve(authority)
    with sqlite3.connect(authority.path) as connection:
        replacement = (
            "tampered"
            if column != "applied_at"
            else (NOW + timedelta(seconds=1)).isoformat(timespec="microseconds")
        )
        connection.execute(
            f"UPDATE resource_effect_operation SET {column} = ? WHERE operation_id = ?",
            (replacement, result.receipt.operation_id),
        )

    with pytest.raises(RuntimeResourceAdmissionError, match="journal|receipt|integrity"):
        authority.lookup(result.receipt.operation_id)


@pytest.mark.parametrize("mutation", ("result", "payload", "receipt", "delete", "reorder"))
def test_unrelated_old_row_tamper_blocks_every_new_effect(
    tmp_path: Path,
    mutation: str,
) -> None:
    authority = _authority(tmp_path)
    first = _reserve(authority)
    second_identity = _identity(worker_id="worker-b", attempt=2)
    second = _reserve(
        authority,
        operation_id="second-reserve-op",
        identity=second_identity,
    )

    with sqlite3.connect(authority.path) as connection:
        if mutation == "result":
            connection.execute(
                "UPDATE resource_effect_operation SET result_json = '{}' WHERE operation_id = ?",
                (first.receipt.operation_id,),
            )
        elif mutation == "payload":
            connection.execute(
                "UPDATE resource_effect_operation SET payload_hash = ? WHERE operation_id = ?",
                ("0" * 64, first.receipt.operation_id),
            )
        elif mutation == "receipt":
            connection.execute(
                "UPDATE resource_effect_operation SET receipt_json = '{}' WHERE operation_id = ?",
                (first.receipt.operation_id,),
            )
        elif mutation == "delete":
            connection.execute(
                "DELETE FROM resource_effect_operation WHERE operation_id = ?",
                (first.receipt.operation_id,),
            )
        else:
            connection.execute(
                "UPDATE resource_effect_operation SET sequence = 3 WHERE operation_id = ?",
                (first.receipt.operation_id,),
            )
            connection.execute(
                "UPDATE resource_effect_operation SET sequence = 1 WHERE operation_id = ?",
                (second.receipt.operation_id,),
            )
            connection.execute(
                "UPDATE resource_effect_operation SET sequence = 2 WHERE operation_id = ?",
                (first.receipt.operation_id,),
            )

    third_identity = _identity(worker_id="worker-c", attempt=3)
    with pytest.raises(RuntimeResourceAdmissionError, match="journal|chain|head|integrity"):
        _reserve(
            authority,
            operation_id="third-reserve-op",
            identity=third_identity,
            snapshot_provider=lambda: pytest.fail("tampered journal must fail before snapshot"),
        )

    assert second.receipt.operation_id == "second-reserve-op"


def test_signer_identity_rejects_same_key_id_with_different_public_key(
    tmp_path: Path,
) -> None:
    original = _Signer(key_id="stable-name", secret=b"original-resource-key")
    _reserve(_authority(tmp_path, signer=original, keyring=_keyring(original)))

    swapped = _Signer(key_id="stable-name", secret=b"replacement-resource-key")
    with pytest.raises(RuntimeResourceAdmissionError, match="fingerprint|genesis|policy|trusted"):
        _authority(tmp_path, signer=swapped, keyring=_keyring(swapped))


def test_closed_keyring_rejects_in_place_verifier_identity_drift(tmp_path: Path) -> None:
    signer = _Signer(key_id="stable-name", secret=b"original-resource-key")
    keyring = _keyring(signer)
    signer.public_key_fingerprint = hashlib.sha256(b"replacement-resource-key").hexdigest()
    signer._secret = b"replacement-resource-key"

    with pytest.raises(ValueError, match="trusted|identity|purpose"):
        _authority(tmp_path, signer=signer, keyring=keyring)


def test_closed_keyring_allows_predeclared_rotation_and_verifies_old_receipts(
    tmp_path: Path,
) -> None:
    old = _Signer(key_id="resource-key-2026-a", secret=b"resource-old-key")
    new = _Signer(key_id="resource-key-2026-b", secret=b"resource-new-key")
    keyring = _keyring(old, new)
    first = _reserve(_authority(tmp_path, signer=old, keyring=keyring))

    rotated = _authority(tmp_path, signer=new, keyring=keyring)
    second = _reserve(
        rotated,
        operation_id="rotated-reserve-op",
        identity=_identity(worker_id="worker-b", attempt=2),
    )

    assert rotated.lookup(first.receipt.operation_id) == first
    assert second.receipt.key_id == new.key_id
    assert second.receipt.public_key_fingerprint == new.public_key_fingerprint


def test_signer_identity_rejects_wrong_purpose_and_cross_role_fingerprint_reuse(
    tmp_path: Path,
) -> None:
    trusted = _Signer()
    wrong_purpose = _Signer(key_purpose="source-quota-effect")
    with pytest.raises(ValueError, match="purpose|trusted"):
        _authority(tmp_path, signer=wrong_purpose, keyring=_keyring(trusted))

    with pytest.raises(ValueError, match="purpose|fingerprint"):
        _role_inventory(
            trusted,
            overrides={
                "quota_effect": frozenset({trusted.public_key_fingerprint}),
            },
        )


@pytest.mark.parametrize("inventory_kind", ("none", "empty", "missing", "duplicate"))
def test_trusted_role_inventory_is_complete_closed_and_globally_unique(
    inventory_kind: str,
) -> None:
    valid = {
        purpose: frozenset({hashlib.sha256(f"role:{purpose}".encode()).hexdigest()})
        for purpose in TRUSTED_RESOURCE_ROLE_PURPOSES
    }
    if inventory_kind == "none":
        supplied = None
    elif inventory_kind == "empty":
        supplied = {}
    elif inventory_kind == "missing":
        supplied = dict(valid)
        supplied.pop("quota_effect")
    else:
        supplied = dict(valid)
        supplied["quota_effect"] = supplied["broker_receipt"]

    with pytest.raises(ValueError, match="inventory|role|purpose|fingerprint"):
        TrustedRoleInventory(role_fingerprints=supplied)


def test_production_resource_journal_requires_an_external_monotonic_root(
    tmp_path: Path,
) -> None:
    signer = _Signer()
    with pytest.raises(RuntimeResourceAdmissionError, match="production.*high-water|root"):
        SQLiteResourceAdmissionAuthority(
            tmp_path / "production-resource-authority.sqlite3",
            authority_id="resource-authority-test",
            signer=signer,
            keyring=_keyring(signer),
            clock=lambda: NOW,
        )

    standalone = _authority(tmp_path)
    assert standalone.path.exists()

    with pytest.raises(RuntimeResourceAdmissionError, match="production.*non-production"):
        compose_production_resource_admission_authority(
            tmp_path / "helper-resource-authority.sqlite3",
            authority_id="resource-authority-test",
            signer=signer,
            keyring=_keyring(signer),
            high_water_authority=None,
            mode="test-standalone",
            clock=lambda: NOW,
        )


def test_production_journal_pins_genesis_and_checkpoints_each_closed_effect(
    tmp_path: Path,
) -> None:
    high_water = _high_water(tmp_path)
    authority = _production_authority(tmp_path, high_water=high_water)
    genesis = high_water.current(journal_authority_id="resource-authority-test")
    assert genesis is not None
    assert genesis.checkpoint.sequence == 0

    result = _reserve(authority)
    current = high_water.current(journal_authority_id="resource-authority-test")

    assert current is not None
    assert current.checkpoint.sequence == 1
    assert current.checkpoint.previous_head_hash == genesis.checkpoint.head_hash
    with sqlite3.connect(authority.path) as connection:
        meta = connection.execute(
            "SELECT prepared_operation_id, checkpoint_root_json FROM resource_authority_meta"
        ).fetchone()
    assert meta is not None
    assert meta[0] is None
    assert ResourceJournalAntiRollbackReceipt.model_validate_json(meta[1]) == current
    assert authority.lookup(result.receipt.operation_id) == result


def test_high_water_commit_response_loss_recovers_the_local_checkpoint_and_replay(
    tmp_path: Path,
) -> None:
    external_root = _MemoryAntiRollbackRoot(lose_on_advance=1)
    cache = _high_water(tmp_path, root=external_root)
    authority = _production_authority(tmp_path, high_water=cache)

    with pytest.raises(ConnectionError, match="response loss"):
        _reserve(authority)

    recovered_cache = _high_water(tmp_path, root=external_root)
    recovered = _production_authority(tmp_path, high_water=recovered_cache)
    replayed = _reserve(
        recovered,
        snapshot_provider=lambda: pytest.fail("recovery replay must not resample"),
    )

    assert replayed.receipt.operation_id == "reserve-op"
    assert (
        recovered_cache.current(journal_authority_id="resource-authority-test").checkpoint.sequence
        == 1
    )
    with sqlite3.connect(recovered.path) as connection:
        assert connection.execute(
            "SELECT prepared_operation_id FROM resource_authority_meta"
        ).fetchone() == (None,)


def test_release_tombstone_recovers_after_external_commit_response_loss(
    tmp_path: Path,
) -> None:
    external_root = _MemoryAntiRollbackRoot(lose_on_advance=2)
    cache = _high_water(tmp_path, root=external_root)
    authority = _production_authority(tmp_path, high_water=cache)
    reserved = _reserve(authority)
    assert reserved.lease is not None

    with pytest.raises(ConnectionError, match="response loss"):
        authority.release(
            operation_id="release-op",
            lease=reserved.lease,
            identity=reserved.lease.identity,
            prior_receipt=reserved.receipt,
        )

    recovered_cache = _high_water(tmp_path, root=external_root)
    recovered = _production_authority(tmp_path, high_water=recovered_cache)
    tombstone = recovered.lookup("release-op")
    assert tombstone.released is True
    assert recovered.active_leases() == ()
    assert (
        recovered.release(
            operation_id="release-op",
            lease=reserved.lease,
            identity=reserved.lease.identity,
            prior_receipt=reserved.receipt,
        )
        == tombstone
    )


def test_external_high_water_rejects_a_valid_local_database_rollback(tmp_path: Path) -> None:
    high_water = _high_water(tmp_path)
    authority = _production_authority(tmp_path, high_water=high_water)
    _reserve(authority)
    rollback = tmp_path / "resource-authority-rollback.sqlite3"
    with sqlite3.connect(authority.path) as source, sqlite3.connect(rollback) as target:
        source.backup(target)

    _reserve(
        authority,
        operation_id="second-reserve-op",
        identity=_identity(worker_id="worker-b", attempt=2),
    )
    shutil.copy2(rollback, authority.path)

    with pytest.raises(RuntimeResourceAdmissionError, match="high-water|rollback|current"):
        _production_authority(tmp_path, high_water=high_water)


def test_external_root_rejects_joint_resource_and_cache_history_restore(
    tmp_path: Path,
) -> None:
    external_root = _MemoryAntiRollbackRoot()
    cache = _high_water(tmp_path, root=external_root)
    authority = _production_authority(tmp_path, high_water=cache)
    _reserve(authority)
    resource_rollback = tmp_path / "resource-authority-history.sqlite3"
    cache_rollback = tmp_path / "resource-cache-history.sqlite3"
    with sqlite3.connect(authority.path) as source, sqlite3.connect(resource_rollback) as target:
        source.backup(target)
    with sqlite3.connect(cache.storage_path) as source, sqlite3.connect(cache_rollback) as target:
        source.backup(target)

    _reserve(
        authority,
        operation_id="second-reserve-op",
        identity=_identity(worker_id="worker-b", attempt=2),
    )
    shutil.copy2(resource_rollback, authority.path)
    shutil.copy2(cache_rollback, cache.storage_path)

    with pytest.raises(ResourceJournalHighWaterError, match="external.*rollback|rollback"):
        _high_water(tmp_path, root=external_root)


def test_external_high_water_rejects_tail_delete_with_a_restored_historical_head(
    tmp_path: Path,
) -> None:
    high_water = _high_water(tmp_path)
    authority = _production_authority(tmp_path, high_water=high_water)
    _reserve(authority)
    with sqlite3.connect(authority.path) as connection:
        historical_meta = connection.execute(
            "SELECT head_json, active_key_id, active_public_key_fingerprint, "
            "checkpoint_root_json FROM resource_authority_meta"
        ).fetchone()
    assert historical_meta is not None

    _reserve(
        authority,
        operation_id="tail-reserve-op",
        identity=_identity(worker_id="worker-b", attempt=2),
    )
    with sqlite3.connect(authority.path) as connection:
        connection.execute("DELETE FROM resource_effect_operation WHERE sequence = 2")
        connection.execute("DELETE FROM resource_reservation WHERE worker_id = 'worker-b'")
        connection.execute(
            """
            UPDATE resource_authority_meta
            SET head_json = ?, active_key_id = ?,
                active_public_key_fingerprint = ?, checkpoint_root_json = ?,
                prepared_operation_id = NULL,
                prepared_previous_checkpoint_hash = NULL,
                prepared_checkpoint_json = NULL
            """,
            historical_meta,
        )

    with pytest.raises(RuntimeResourceAdmissionError, match="high-water|rollback|current"):
        _production_authority(tmp_path, high_water=high_water)


def test_external_high_water_rejects_a_valid_donor_database(tmp_path: Path) -> None:
    root_a = _high_water(tmp_path, filename="root-a.sqlite3")
    root_b = _high_water(tmp_path, filename="root-b.sqlite3")
    authority_a = _production_authority(
        tmp_path,
        filename="authority-a.sqlite3",
        high_water=root_a,
    )
    donor = _production_authority(
        tmp_path,
        filename="authority-b.sqlite3",
        high_water=root_b,
    )
    _reserve(authority_a)
    _reserve(donor)

    with pytest.raises(RuntimeResourceAdmissionError, match="high-water|lineage|donor"):
        _production_authority(
            tmp_path,
            filename="authority-b.sqlite3",
            high_water=root_a,
        )


def test_expiry_never_deletes_materialized_state_without_a_release_tombstone(
    tmp_path: Path,
) -> None:
    current = [NOW]
    authority = _authority(tmp_path, current=current)
    reserved = _reserve(authority)
    assert reserved.lease is not None
    current[0] = reserved.lease.expires_at + timedelta(seconds=1)

    assert authority.active_leases() == ()
    with sqlite3.connect(authority.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM resource_reservation").fetchone() == (1,)
    restarted = _authority(tmp_path, current=current)
    assert restarted.active_leases() == ()


@pytest.mark.parametrize("tamper_kind", ("insert", "delete", "mutate"))
def test_materialized_reservation_insert_delete_or_mutation_fails_closed(
    tmp_path: Path,
    tamper_kind: str,
) -> None:
    authority = _authority(tmp_path)
    reserved = _reserve(authority)
    assert reserved.lease is not None
    with sqlite3.connect(authority.path) as connection:
        if tamper_kind == "insert":
            row = connection.execute("SELECT * FROM resource_reservation").fetchone()
            assert row is not None
            values = list(row)
            values[0] = "f" * 64
            values[-2] = "forged-renewal-op"
            values[-1] = "forged-effect-op"
            connection.execute(
                "INSERT INTO resource_reservation VALUES "
                "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                values,
            )
        elif tamper_kind == "delete":
            connection.execute("DELETE FROM resource_reservation")
        else:
            connection.execute(
                "UPDATE resource_reservation SET expected_memory_bytes = expected_memory_bytes + 1"
            )

    with pytest.raises(RuntimeResourceAdmissionError, match="materialized|lease|state root"):
        authority.active_leases()
    with pytest.raises(RuntimeResourceAdmissionError, match="materialized|lease|state root"):
        _authority(tmp_path)
    with pytest.raises(RuntimeResourceAdmissionError, match="materialized|lease|state root"):
        _reserve(
            authority,
            operation_id="blocked-after-state-tamper",
            identity=_identity(worker_id="worker-b", attempt=2),
            snapshot_provider=lambda: pytest.fail(
                "materialized state tamper must fail before capacity or snapshot"
            ),
        )


def test_signed_head_state_root_must_come_from_the_closed_journal_reducer(
    tmp_path: Path,
) -> None:
    signer = _Signer()
    authority = _authority(tmp_path, signer=signer, keyring=_keyring(signer))
    _reserve(authority)
    forged_identity = _identity(worker_id="forged-worker", attempt=2)
    forged_lease = ResourceReservationLease(
        identity=forged_identity,
        request_hash=canonical_sha256(_request(forged_identity)),
        expected_memory_bytes=1,
        expected_disk_bytes=1,
        expected_quota_units=0,
        granted_at=NOW,
        expires_at=NOW + timedelta(seconds=30),
    )

    with sqlite3.connect(authority.path) as connection:
        connection.row_factory = sqlite3.Row
        connection.execute(
            """
            INSERT INTO resource_reservation(
                lease_id, job_id, run_id, shard_id, attempt_id, claim_generation,
                scheduler_fencing_token, worker_id, request_hash,
                expected_memory_bytes, expected_disk_bytes, expected_quota_units,
                granted_at, expires_at, last_renewal_operation_id,
                last_effect_operation_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)
            """,
            (
                forged_lease.lease_id,
                str(forged_identity.job_id),
                forged_identity.run_id,
                str(forged_identity.shard_id),
                str(forged_identity.attempt_id),
                forged_identity.claim_generation,
                forged_identity.scheduler_fencing_token,
                forged_identity.worker_id,
                forged_lease.request_hash,
                forged_lease.expected_memory_bytes,
                forged_lease.expected_disk_bytes,
                forged_lease.granted_at.isoformat(timespec="microseconds"),
                forged_lease.expires_at.isoformat(timespec="microseconds"),
                "forged-renewal-op",
                "forged-effect-op",
            ),
        )
        columns = tuple(
            row["name"] for row in connection.execute("PRAGMA table_info(resource_reservation)")
        )
        materialized = [
            {column: row[column] for column in columns}
            for row in connection.execute(
                "SELECT * FROM resource_reservation ORDER BY lease_id"
            ).fetchall()
        ]
        head = json.loads(
            connection.execute("SELECT head_json FROM resource_authority_meta").fetchone()[0]
        )
        head["materialized_state_root"] = canonical_sha256(materialized)
        signing_payload = {key: value for key, value in head.items() if key != "signature"}
        signing_payload["contract"] = "rquant-resource-admission-journal-head/v1"
        head["signature"] = signer.sign(
            namespace="rquant-resource-admission-journal-head/v1",
            payload=json.dumps(
                signing_payload,
                sort_keys=True,
                separators=(",", ":"),
            ).encode(),
        )
        head_json = json.dumps(head, sort_keys=True, separators=(",", ":"))
        connection.execute(
            "UPDATE resource_effect_operation SET head_json = ? WHERE sequence = 1",
            (head_json,),
        )
        connection.execute(
            "UPDATE resource_authority_meta SET head_json = ?",
            (head_json,),
        )

    with pytest.raises(RuntimeResourceAdmissionError, match="reducer|materialized|journal"):
        authority.active_leases()


def test_active_v2_copy_migration_is_rejected_and_source_bytes_do_not_change(
    tmp_path: Path,
) -> None:
    source = tmp_path / "resource-authority-v2.sqlite3"
    legacy = SQLiteResourceReservationStore(source, clock=lambda: NOW)
    identity = _identity()
    admitted = legacy.reserve(
        identity=identity,
        request=_request(identity),
        policy=_policy(),
        snapshot_provider=_snapshot,
        lease_seconds=30,
    )
    assert admitted.lease is not None
    original_bytes = source.read_bytes()
    destination = tmp_path / "resource-authority-v3.sqlite3"
    signer = _Signer()
    keyring = _keyring(signer)

    with pytest.raises(RuntimeResourceAdmissionError, match="v2|migration"):
        SQLiteResourceAdmissionAuthority(
            source,
            authority_id="resource-authority-test",
            signer=signer,
            keyring=keyring,
            mode="test-standalone",
            clock=lambda: NOW,
        )

    with pytest.raises(RuntimeResourceAdmissionError, match="active leases|signed receipts"):
        SQLiteResourceAdmissionAuthority.copy_migrate_v2(
            source,
            destination,
            authority_id="resource-authority-test",
            signer=signer,
            keyring=keyring,
            mode="test-standalone",
            clock=lambda: NOW,
        )

    assert source.read_bytes() == original_bytes
    assert not destination.exists()


def test_empty_v2_copy_migration_creates_an_explicit_v3_file(tmp_path: Path) -> None:
    source = tmp_path / "resource-authority-v2.sqlite3"
    SQLiteResourceReservationStore(source, clock=lambda: NOW)
    original_bytes = source.read_bytes()
    destination = tmp_path / "resource-authority-v3.sqlite3"
    signer = _Signer()

    migrated = SQLiteResourceAdmissionAuthority.copy_migrate_v2(
        source,
        destination,
        authority_id="resource-authority-test",
        signer=signer,
        keyring=_keyring(signer),
        mode="test-standalone",
        clock=lambda: NOW,
    )

    assert source.read_bytes() == original_bytes
    assert migrated.path == destination.resolve()
    with sqlite3.connect(destination) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (3,)
        assert connection.execute("SELECT COUNT(*) FROM resource_effect_operation").fetchone() == (
            0,
        )
        assert connection.execute("SELECT COUNT(*) FROM resource_reservation").fetchone() == (0,)
        meta = connection.execute(
            "SELECT allowed_fingerprints_json, genesis_json, head_json FROM resource_authority_meta"
        ).fetchone()
    assert meta is not None
    assert signer.public_key_fingerprint in meta[0]
