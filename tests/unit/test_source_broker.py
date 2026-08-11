from __future__ import annotations

import json
import shutil
import sqlite3
import subprocess
import threading
import time
from collections import Counter
from collections.abc import Callable, Mapping
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path
from typing import Any, Literal

import pytest
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from rquant.adapter_manifest import (
    BROKER_OUTBOX_NAMESPACE,
    REPLAY_CLAIM_NAMESPACE,
    Ed25519ContractSigner,
    Ed25519PublicKeyRecord,
    PydanticModelSchema,
    VerifyOnlyEd25519Keyring,
)
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.source_broker import (
    EffectReceipt,
    LeaseClockReading,
    ProviderCapability,
    ProviderRegistry,
    QuotaEffectResponse,
    QuotaReservation,
    ReplayLineageCheckpointReceipt,
    SourceBroker,
    SourceBrokerError,
    SourceCallReceipt,
    SQLiteReplayAuthority,
)
from tests.unit.test_adapter_manifest import (
    NOW,
    Authorities,
    DailyRequest,
    DailyResponse,
    OpenSslSigningClient,
    create_test_authorities,
    signed_manifest,
    signed_plan,
)


class SimulatedCrash(BaseException):
    pass


class IdempotentQuotaLedger:
    def __init__(self, authorities: Authorities) -> None:
        self._authorities = authorities
        self._lock = threading.Lock()
        self._operations: dict[str, tuple[str, str, QuotaEffectResponse]] = {}
        self.effect_counts: Counter[str] = Counter()

    def _apply(
        self,
        operation_id: str,
        kind: str,
        payload: dict[str, object],
        result: QuotaReservation | None = None,
    ) -> QuotaEffectResponse:
        payload_hash = canonical_sha256(payload)
        with self._lock:
            existing = self._operations.get(operation_id)
            if existing is not None:
                if existing[:2] != (kind, payload_hash):
                    raise RuntimeError("idempotency operation conflicts")
                return existing[2]
            unsigned = EffectReceipt(
                authority_id=self._authorities.quota.issuer,
                operation_id=operation_id,
                payload_hash=payload_hash,
                effect=kind,
                outcome="applied",
                result_hash=canonical_sha256(result),
                key_id=self._authorities.quota.key_id,
                signature="",
            )
            receipt = unsigned.model_copy(
                update={
                    "signature": self._authorities.quota.sign(
                        namespace="rquant-source-quota-effect/v1",
                        payload=unsigned.signing_bytes(),
                    )
                }
            )
            response = QuotaEffectResponse(receipt=receipt, result=result)
            self._operations[operation_id] = (kind, payload_hash, response)
            self.effect_counts[kind] += 1
            return response

    def reserve(
        self,
        *,
        operation_id: str,
        claim_token: str,
        source: str,
        units: int,
    ) -> QuotaEffectResponse:
        reservation = QuotaReservation(
            reservation_id=canonical_sha256(
                {"claim_token": claim_token, "source": source, "units": units}
            ),
            claim_token=claim_token,
            source=source,
            reserved_units=units,
        )
        return self._apply(
            operation_id,
            "reserve",
            {"claim_token": claim_token, "source": source, "units": units},
            reservation,
        )

    def record_intent(
        self,
        *,
        operation_id: str,
        reservation_id: str,
        call_id: str,
        claim_token: str,
        idempotency_key: str,
        manifest_hash: str,
        source: str,
        operation: str,
        request_hash: str,
        cost: int,
    ) -> QuotaEffectResponse:
        return self._apply(
            operation_id,
            "intent",
            {
                "reservation_id": reservation_id,
                "call_id": call_id,
                "claim_token": claim_token,
                "idempotency_key": idempotency_key,
                "manifest_hash": manifest_hash,
                "source": source,
                "operation": operation,
                "request_hash": request_hash,
                "cost": cost,
            },
        )

    def mark_dispatched(
        self,
        *,
        operation_id: str,
        reservation_id: str,
        call_id: str,
    ) -> QuotaEffectResponse:
        return self._apply(
            operation_id,
            "dispatch",
            {"reservation_id": reservation_id, "call_id": call_id},
        )

    def finalize(
        self,
        *,
        operation_id: str,
        reservation_id: str,
        call_id: str,
        outcome: str,
    ) -> QuotaEffectResponse:
        return self._apply(
            operation_id,
            "finalize",
            {
                "reservation_id": reservation_id,
                "call_id": call_id,
                "outcome": outcome,
            },
        )

    def recover(
        self,
        *,
        operation_id: str,
        reservation_id: str,
        call_id: str,
    ) -> QuotaEffectResponse:
        return self._apply(
            operation_id,
            "recover",
            {"reservation_id": reservation_id, "call_id": call_id},
        )

    def release_unused(self, *, operation_id: str, reservation_id: str) -> QuotaEffectResponse:
        return self._apply(
            operation_id,
            "release",
            {"reservation_id": reservation_id},
        )

    def forge_effect_receipt(self, *, kind: str, payload_hash: str) -> None:
        with self._lock:
            for operation_id, (observed_kind, stored_hash, response) in self._operations.items():
                if observed_kind == kind:
                    forged = response.model_copy(
                        update={
                            "receipt": response.receipt.model_copy(
                                update={"payload_hash": payload_hash}
                            )
                        }
                    )
                    self._operations[operation_id] = (observed_kind, stored_hash, forged)
                    return
        raise AssertionError(f"no {kind} effect was recorded")


class EchoProvider:
    def __init__(self) -> None:
        self.calls: list[ProviderCapability] = []
        self._lock = threading.Lock()

    def dispatch(self, capability: ProviderCapability) -> BaseModel:
        with self._lock:
            self.calls.append(capability)
        return DailyResponse(rows=7)


class InterruptedProvider:
    def dispatch(self, capability: ProviderCapability) -> BaseModel:
        raise KeyboardInterrupt("simulated process interruption")


class BlockingProvider(EchoProvider):
    def __init__(self) -> None:
        super().__init__()
        self.entered = threading.Event()
        self.release = threading.Event()

    def dispatch(self, capability: ProviderCapability) -> BaseModel:
        with self._lock:
            self.calls.append(capability)
        self.entered.set()
        assert self.release.wait(timeout=5)
        return DailyResponse(rows=7)


class TimeoutProvider(EchoProvider):
    def dispatch(self, capability: ProviderCapability) -> BaseModel:
        with self._lock:
            self.calls.append(capability)
        raise TimeoutError("simulated provider timeout")


class CrashOnce:
    def __init__(self, *, kind: str, phase: str) -> None:
        self.kind = kind
        self.phase = phase
        self.triggered = False

    def __call__(self, kind: str, phase: str, operation_id: str) -> None:
        if not self.triggered and (kind, phase) == (self.kind, self.phase):
            self.triggered = True
            raise SimulatedCrash(f"{kind}:{phase}:{operation_id}")


class InMemoryReplayLineageAuthority:
    def __init__(
        self,
        *,
        signer: Ed25519ContractSigner,
        keyring: VerifyOnlyEd25519Keyring,
        barrier: threading.Barrier | None = None,
    ) -> None:
        self._signer = signer
        self._keyring = keyring
        self._barrier = barrier
        self._lock = threading.Lock()
        self._current: dict[str, tuple[str, str, int, str, ReplayLineageCheckpointReceipt]] = {}
        self._operations: dict[
            str,
            tuple[
                tuple[str, str, str, str, int, str],
                ReplayLineageCheckpointReceipt,
            ],
        ] = {}
        self._lose_next_response = False
        self._verification_unavailable = False
        self.advance_count = 0

    @property
    def authority_id(self) -> str:
        return self._signer.issuer

    @property
    def verifier_fingerprints(self) -> frozenset[str]:
        return frozenset({self._signer.public_key_fingerprint})

    def lose_next_response(self) -> None:
        with self._lock:
            self._lose_next_response = True

    def make_verification_unavailable(self) -> None:
        with self._lock:
            self._verification_unavailable = True

    def fork(self) -> InMemoryReplayLineageAuthority:
        fork = InMemoryReplayLineageAuthority(
            signer=self._signer,
            keyring=self._keyring,
        )
        with self._lock:
            fork._current = dict(self._current)
            fork._operations = dict(self._operations)
            fork.advance_count = self.advance_count
        return fork

    def compare_and_advance(
        self,
        *,
        operation_id: str,
        replay_authority_id: str,
        lineage_id: str,
        previous_head_hash: str,
        next_head_hash: str,
        sequence: int,
        claim_binding_hash: str,
    ) -> ReplayLineageCheckpointReceipt:
        if self._barrier is not None:
            self._barrier.wait(timeout=5)
        request = (
            replay_authority_id,
            lineage_id,
            previous_head_hash,
            next_head_hash,
            sequence,
            claim_binding_hash,
        )
        lose_response = False
        with self._lock:
            existing = self._operations.get(operation_id)
            if existing is not None:
                if existing[0] != request:
                    raise SourceBrokerError("lineage operation id was rebound")
                return existing[1]
            current = self._current.get(replay_authority_id)
            if current is None:
                if sequence != 1:
                    raise SourceBrokerError("lineage genesis sequence is invalid")
            elif (
                lineage_id != current[0]
                or previous_head_hash != current[1]
                or sequence != current[2] + 1
            ):
                raise SourceBrokerError("lineage fork or rollback was rejected")
            unsigned = ReplayLineageCheckpointReceipt(
                schema_version=1,
                contract="rquant-source-replay-lineage-checkpoint/v1",
                authority_id=self.authority_id,
                operation_id=operation_id,
                replay_authority_id=replay_authority_id,
                lineage_id=lineage_id,
                previous_head_hash=previous_head_hash,
                next_head_hash=next_head_hash,
                sequence=sequence,
                claim_binding_hash=claim_binding_hash,
                outcome="applied",
                key_id=self._signer.key_id,
                signature="",
            )
            receipt = unsigned.model_copy(
                update={
                    "signature": self._signer.sign(
                        namespace=REPLAY_CLAIM_NAMESPACE,
                        payload=unsigned.signing_bytes(),
                    )
                }
            )
            self._operations[operation_id] = (request, receipt)
            self._current[replay_authority_id] = (
                lineage_id,
                next_head_hash,
                sequence,
                claim_binding_hash,
                receipt,
            )
            self.advance_count += 1
            if self._lose_next_response:
                self._lose_next_response = False
                lose_response = True
        if lose_response:
            raise ConnectionError("lineage checkpoint response was lost")
        return receipt

    def verify_current(
        self,
        *,
        replay_authority_id: str,
        lineage_id: str,
        head_hash: str,
        sequence: int,
        receipt: ReplayLineageCheckpointReceipt | None,
    ) -> None:
        with self._lock:
            if self._verification_unavailable:
                raise ConnectionError("lineage current verification is unavailable")
            current = self._current.get(replay_authority_id)
            if current is None:
                if sequence == 0 and receipt is None:
                    return
                raise SourceBrokerError("lineage checkpoint is not externally pinned")
            if receipt is None:
                raise SourceBrokerError("lineage checkpoint receipt is missing")
            if (
                (lineage_id, head_hash, sequence) != current[:3]
                or receipt.receipt_hash != current[4].receipt_hash
                or receipt.authority_id != self.authority_id
                or not self._keyring.verify(
                    issuer=receipt.authority_id,
                    key_id=receipt.key_id,
                    key_purpose="replay_claim",
                    namespace=REPLAY_CLAIM_NAMESPACE,
                    payload=receipt.signing_bytes(),
                    signature=receipt.signature,
                )
            ):
                raise SourceBrokerError("lineage current checkpoint verification failed")


class SnapshotBarrier:
    def __init__(self) -> None:
        self.snapshot_taken = threading.Event()
        self.resume = threading.Event()

    def __call__(self, kind: str, phase: str, operation_id: str) -> None:
        if (kind, phase) == ("call", "after_snapshot"):
            self.snapshot_taken.set()
            assert self.resume.wait(timeout=5)


class RecoveryBarrier:
    def __init__(self) -> None:
        self._barrier = threading.Barrier(2, timeout=5)

    def __call__(self, kind: str, phase: str, operation_id: str) -> None:
        if (kind, phase) == ("recover", "after_snapshot"):
            self._barrier.wait()


class ReleaseAppliedBarrier:
    def __init__(self) -> None:
        self.entered = threading.Event()
        self.resume = threading.Event()
        self._lock = threading.Lock()
        self._armed = False
        self._triggered = False

    def arm(self) -> None:
        with self._lock:
            self._armed = True

    def __call__(self, kind: str, phase: str, operation_id: str) -> None:
        if (kind, phase) != ("release", "after_apply"):
            return
        with self._lock:
            if not self._armed or self._triggered:
                return
            self._triggered = True
        self.entered.set()
        assert self.resume.wait(timeout=5)


class BarrierReplayAuthority:
    def __init__(
        self,
        delegate: SQLiteReplayAuthority,
        barrier: threading.Barrier,
    ) -> None:
        self._delegate = delegate
        self._barrier = barrier

    @property
    def authority_id(self) -> str:
        return self._delegate.authority_id

    @property
    def lineage_verifier_fingerprints(self) -> frozenset[str]:
        return self._delegate.lineage_verifier_fingerprints

    def consume_once(
        self,
        *,
        operation_id: str,
        nonce: str,
        plan_hash: str,
        claim_token: str,
        broker_id: str,
    ) -> EffectReceipt:
        self._barrier.wait(timeout=5)
        return self._delegate.consume_once(
            operation_id=operation_id,
            nonce=nonce,
            plan_hash=plan_hash,
            claim_token=claim_token,
            broker_id=broker_id,
        )

    def verify_claim_binding(
        self,
        *,
        operation_id: str,
        nonce: str,
        plan_hash: str,
        claim_token: str,
        broker_id: str,
        receipt: EffectReceipt,
    ) -> EffectReceipt:
        return self._delegate.verify_claim_binding(
            operation_id=operation_id,
            nonce=nonce,
            plan_hash=plan_hash,
            claim_token=claim_token,
            broker_id=broker_id,
            receipt=receipt,
        )


class ManualLeaseClock:
    def __init__(
        self,
        *,
        wall_time: float = 10_000.0,
        monotonic_time: float = 1_000.0,
        boot_id: str = "test-boot",
    ) -> None:
        self._lock = threading.Lock()
        self._wall_time = wall_time
        self._monotonic_time = monotonic_time
        self._boot_id = boot_id

    def __call__(self) -> LeaseClockReading:
        with self._lock:
            return LeaseClockReading(
                wall_time=self._wall_time,
                monotonic_time=self._monotonic_time,
                boot_id=self._boot_id,
            )

    def advance(self, seconds: float) -> None:
        with self._lock:
            self._wall_time += seconds
            self._monotonic_time += seconds

    def roll_back_wall(self, seconds: float) -> None:
        with self._lock:
            self._wall_time -= seconds


class PlanValidationRace:
    def __init__(self, *, state_path: Path, replacement_json: str) -> None:
        self._state_path = state_path
        self._replacement_json = replacement_json
        self._started = False
        self._lock = threading.Lock()
        self.tamper_done = threading.Event()
        self.tamper_thread: threading.Thread | None = None

    def __call__(self, kind: str, phase: str, operation_id: str) -> None:
        if (kind, phase) == ("plan", "after_validation"):
            with self._lock:
                if self._started:
                    return
                self._started = True
            self.tamper_thread = threading.Thread(
                target=self._tamper,
                args=(operation_id,),
                daemon=True,
            )
            self.tamper_thread.start()
        elif phase == "before_effect_check":
            assert self.tamper_done.wait(timeout=5)

    def _tamper(self, claim_token: str) -> None:
        with sqlite3.connect(self._state_path, timeout=5) as connection:
            connection.execute(
                "UPDATE broker_session SET plan_json = ? WHERE claim_token = ?",
                (self._replacement_json, claim_token),
            )
        self.tamper_done.set()


def _registry(provider: object) -> ProviderRegistry:
    registry = ProviderRegistry()
    registry.register(
        source="tushare",
        operation="daily_bars",
        provider=provider,
        request_model=DailyRequest,
        response_model=DailyResponse,
    )
    return registry


_DEFAULT_LINEAGES: dict[str, InMemoryReplayLineageAuthority] = {}
_DEFAULT_LINEAGES_LOCK = threading.Lock()


def _new_lineage_authority(
    root: Path,
    *,
    barrier: threading.Barrier | None = None,
    authority_id: str = "test-replay-lineage-root",
) -> InMemoryReplayLineageAuthority:
    openssl = shutil.which("openssl")
    if openssl is None:
        pytest.skip("openssl is required for Ed25519 source broker tests")
    root.mkdir(parents=True, exist_ok=True)
    private_key = root / "lineage.private.pem"
    public_key = root / "lineage.public.pem"
    generated = subprocess.run(
        (openssl, "genpkey", "-algorithm", "ED25519", "-out", str(private_key)),
        check=False,
        capture_output=True,
    )
    if generated.returncode != 0:
        raise RuntimeError(generated.stderr.decode("utf-8", errors="replace"))
    exported = subprocess.run(
        (openssl, "pkey", "-in", str(private_key), "-pubout", "-out", str(public_key)),
        check=False,
        capture_output=True,
    )
    if exported.returncode != 0:
        raise RuntimeError(exported.stderr.decode("utf-8", errors="replace"))
    private_key.chmod(0o600)
    record = Ed25519PublicKeyRecord(
        key_id="lineage-v1",
        issuer=authority_id,
        key_purpose="replay_claim",
        rotation="active",
        public_key_pem=public_key.read_bytes(),
    )
    signer = Ed25519ContractSigner(
        key_id=record.key_id,
        issuer=authority_id,
        key_purpose="replay_claim",
        client=OpenSslSigningClient(
            private_key,
            key_purpose="replay_claim",
            allowed_namespaces=frozenset({REPLAY_CLAIM_NAMESPACE}),
            public_key_fingerprint=record.public_key_fingerprint,
        ),
    )
    keyring = VerifyOnlyEd25519Keyring(
        records=(record,),
        issuer_allowlist={"replay_claim": frozenset({authority_id})},
        rotation_allowlist={(authority_id, "replay_claim"): frozenset({record.key_id})},
    )
    return InMemoryReplayLineageAuthority(
        signer=signer,
        keyring=keyring,
        barrier=barrier,
    )


def _default_lineage_authority(
    path: Path,
    authorities: Authorities,
) -> InMemoryReplayLineageAuthority:
    key = authorities.replay.public_key_fingerprint
    with _DEFAULT_LINEAGES_LOCK:
        lineage = _DEFAULT_LINEAGES.get(key)
        if lineage is None:
            lineage = _new_lineage_authority(path.parent / ".lineage-root")
            _DEFAULT_LINEAGES[key] = lineage
        return lineage


def _sqlite_backup(source: Path, destination: Path) -> None:
    with (
        sqlite3.connect(source) as source_connection,
        sqlite3.connect(destination) as destination_connection,
    ):
        source_connection.backup(destination_connection)


def _replay(
    path: Path,
    authorities: Authorities,
    *,
    lineage_authority: InMemoryReplayLineageAuthority | None = None,
    fault_injector: Callable[[str, str, str], None] | None = None,
) -> SQLiteReplayAuthority:
    return SQLiteReplayAuthority(
        path,
        authority_id="global-source-use",
        signer=authorities.replay,
        keyring=authorities.replay_keyring,
        lineage_authority=(lineage_authority or _default_lineage_authority(path, authorities)),
        fault_injector=fault_injector,
    )


def _broker(
    path: Path,
    *,
    authorities: Authorities,
    ledger: IdempotentQuotaLedger,
    replay: SQLiteReplayAuthority,
    provider: object | None = None,
    fault_injector: Callable[[str, str, str], None] | None = None,
    lease_clock: Callable[[], LeaseClockReading] | None = None,
    lease_ttl_seconds: float = 5.0,
    heartbeat_interval_seconds: float = 0.02,
) -> SourceBroker:
    return SourceBroker(
        state_path=path,
        broker_id="lab-broker-a",
        quota_ledger=ledger,
        replay_authority=replay,
        provider_registry=_registry(provider or EchoProvider()),
        authorization_keyring=authorities.authorization_keyring,
        receipt_signer=authorities.broker,
        receipt_keyring=authorities.broker_keyring,
        quota_effect_keyring=authorities.quota_keyring,
        replay_claim_keyring=authorities.replay_keyring,
        outbox_signer=authorities.outbox,
        outbox_keyring=authorities.outbox_keyring,
        now=lambda: NOW,
        fault_injector=fault_injector,
        lease_clock=lease_clock,
        lease_ttl_seconds=lease_ttl_seconds,
        heartbeat_interval_seconds=heartbeat_interval_seconds,
    )


def test_broker_requires_distinct_receipt_signer_and_shared_replay_authority(
    tmp_path: Path,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    ledger = IdempotentQuotaLedger(authorities)

    with pytest.raises(ValueError, match="replay authority"):
        SourceBroker(
            state_path=tmp_path / "broker.sqlite3",
            broker_id="lab-broker-a",
            quota_ledger=ledger,
            replay_authority=None,
            provider_registry=_registry(EchoProvider()),
            authorization_keyring=authorities.authorization_keyring,
            receipt_signer=authorities.plan,
            receipt_keyring=authorities.broker_keyring,
            quota_effect_keyring=authorities.quota_keyring,
            replay_claim_keyring=authorities.replay_keyring,
            outbox_signer=authorities.outbox,
            outbox_keyring=authorities.outbox_keyring,
            now=lambda: NOW,
        )

    with pytest.raises(ValueError, match="receipt signer"):
        SourceBroker(
            state_path=tmp_path / "broker.sqlite3",
            broker_id="lab-broker-a",
            quota_ledger=ledger,
            replay_authority=_replay(tmp_path / "replay.sqlite3", authorities),
            provider_registry=_registry(EchoProvider()),
            authorization_keyring=authorities.authorization_keyring,
            receipt_signer=authorities.plan,
            receipt_keyring=authorities.broker_keyring,
            quota_effect_keyring=authorities.quota_keyring,
            replay_claim_keyring=authorities.replay_keyring,
            outbox_signer=authorities.outbox,
            outbox_keyring=authorities.outbox_keyring,
            now=lambda: NOW,
        )


def test_broker_allows_shared_verify_keyring_when_role_fingerprints_are_disjoint(
    tmp_path: Path,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    shared = VerifyOnlyEd25519Keyring(
        records=authorities.records,
        issuer_allowlist={
            "adapter_manifest": frozenset({"release-authority"}),
            "source_use_plan": frozenset({"lab-plan-authority"}),
            "broker_receipt": frozenset({"lab-broker-a"}),
            "quota_effect": frozenset({"quota-ledger"}),
            "replay_claim": frozenset({"global-source-use"}),
            "broker_outbox": frozenset({"lab-broker-a"}),
        },
        rotation_allowlist={
            ("release-authority", "adapter_manifest"): frozenset({"manifest-v1", "manifest-v2"}),
            ("lab-plan-authority", "source_use_plan"): frozenset({"plan-v1"}),
            ("lab-broker-a", "broker_receipt"): frozenset({"broker-v1"}),
            ("quota-ledger", "quota_effect"): frozenset({"quota-v1"}),
            ("global-source-use", "replay_claim"): frozenset({"replay-v1"}),
            ("lab-broker-a", "broker_outbox"): frozenset({"outbox-v1"}),
        },
    )
    ledger = IdempotentQuotaLedger(authorities)
    broker = SourceBroker(
        state_path=tmp_path / "broker.sqlite3",
        broker_id="lab-broker-a",
        quota_ledger=ledger,
        replay_authority=_replay(tmp_path / "replay.sqlite3", authorities),
        provider_registry=_registry(EchoProvider()),
        authorization_keyring=shared,
        receipt_signer=authorities.broker,
        receipt_keyring=shared,
        quota_effect_keyring=shared,
        replay_claim_keyring=shared,
        outbox_signer=authorities.outbox,
        outbox_keyring=shared,
        now=lambda: NOW,
    )

    assert broker.start(signed_plan(authorities)).claim_token == "claim-123"


def test_broker_rejects_distinct_signer_objects_backed_by_same_public_key(
    tmp_path: Path,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    shared_outbox_record = Ed25519PublicKeyRecord(
        key_id="outbox-shared-v1",
        issuer="lab-broker-a",
        key_purpose="broker_outbox",
        rotation="active",
        public_key_pem=(tmp_path / "keys" / "broker-v1.public.pem").read_bytes(),
    )
    shared_outbox_signer = Ed25519ContractSigner(
        key_id="outbox-shared-v1",
        issuer="lab-broker-a",
        key_purpose="broker_outbox",
        client=OpenSslSigningClient(
            tmp_path / "keys" / "broker-v1.private.pem",
            key_purpose="broker_outbox",
            allowed_namespaces=frozenset({BROKER_OUTBOX_NAMESPACE}),
            public_key_fingerprint=shared_outbox_record.public_key_fingerprint,
        ),
    )
    shared_outbox_keyring = VerifyOnlyEd25519Keyring(
        records=(shared_outbox_record,),
        issuer_allowlist={"broker_outbox": frozenset({"lab-broker-a"})},
        rotation_allowlist={("lab-broker-a", "broker_outbox"): frozenset({"outbox-shared-v1"})},
    )

    with pytest.raises(ValueError, match="fingerprint|role"):
        SourceBroker(
            state_path=tmp_path / "broker.sqlite3",
            broker_id="lab-broker-a",
            quota_ledger=IdempotentQuotaLedger(authorities),
            replay_authority=_replay(tmp_path / "replay.sqlite3", authorities),
            provider_registry=_registry(EchoProvider()),
            authorization_keyring=authorities.authorization_keyring,
            receipt_signer=authorities.broker,
            receipt_keyring=authorities.broker_keyring,
            quota_effect_keyring=authorities.quota_keyring,
            replay_claim_keyring=authorities.replay_keyring,
            outbox_signer=shared_outbox_signer,
            outbox_keyring=shared_outbox_keyring,
            now=lambda: NOW,
        )


def test_provider_registry_requires_closed_models_and_rejects_credential_aliases() -> None:
    class OpenRequest(BaseModel):
        value: str

    class AccessTokenRequest(RuntimeContractModel):
        access_token: str

    class EndpointRequest(RuntimeContractModel):
        endpoint: str

    class ApiKeyAliasRequest(RuntimeContractModel):
        model_config = ConfigDict(extra="forbid", populate_by_name=True)
        value: str = Field(alias="x-api-key")

    registry = ProviderRegistry()
    for request_model in (
        OpenRequest,
        AccessTokenRequest,
        EndpointRequest,
        ApiKeyAliasRequest,
    ):
        with pytest.raises(SourceBrokerError, match="closed|credential|alias"):
            registry.register(
                source="tushare",
                operation=request_model.__name__,
                provider=EchoProvider(),
                request_model=request_model,
                response_model=DailyResponse,
            )

    with pytest.raises(ValidationError):
        ProviderCapability(operation="daily_bars", request={"trade_date": "2026-08-04"})
    with pytest.raises(ValidationError):
        DailyRequest.model_validate(
            {
                "trade_date": "2026-08-04",
                "filters": {"market": "SZ", "access_token": "secret"},
            }
        )


def test_broker_rejects_schema_mismatch_audience_and_plan_time_window(tmp_path: Path) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    ledger = IdempotentQuotaLedger(authorities)
    replay = _replay(tmp_path / "replay.sqlite3", authorities)
    broker = _broker(
        tmp_path / "broker.sqlite3",
        authorities=authorities,
        ledger=ledger,
        replay=replay,
    )

    with pytest.raises(SourceBrokerError, match="audience"):
        broker.start(signed_plan(authorities, audience="other-broker"))
    with pytest.raises(SourceBrokerError, match="expired"):
        broker.start(
            signed_plan(
                authorities,
                claim_token="expired",
                nonce="expired",
                expires_at=NOW - timedelta(seconds=1),
                not_before=NOW - timedelta(minutes=1),
            )
        )

    mismatched_manifest = signed_manifest(authorities).model_copy(
        update={
            "request_schema": PydanticModelSchema(
                model_name="other.Request",
                schema_hash="b" * 64,
            )
        }
    )
    mismatched_plan = signed_plan(
        authorities,
        claim_token="schema-mismatch",
        nonce="schema-mismatch",
        manifest=mismatched_manifest,
    )
    with pytest.raises(SourceBrokerError, match="authorized|schema"):
        broker.start(mismatched_plan)


def test_global_replay_authority_blocks_same_plan_across_broker_databases(tmp_path: Path) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    ledger = IdempotentQuotaLedger(authorities)
    replay = _replay(tmp_path / "replay.sqlite3", authorities)
    plan = signed_plan(authorities)
    first = _broker(
        tmp_path / "first.sqlite3",
        authorities=authorities,
        ledger=ledger,
        replay=replay,
    )
    second = _broker(
        tmp_path / "second.sqlite3",
        authorities=authorities,
        ledger=ledger,
        replay=replay,
    )

    first.start(plan)
    with pytest.raises(SourceBrokerError, match="replay"):
        second.start(plan)
    assert ledger.effect_counts["reserve"] == 1


def test_replay_authority_rejects_complete_donor_anchor_transplant(tmp_path: Path) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    ledger = IdempotentQuotaLedger(authorities)
    target_lineage = _new_lineage_authority(tmp_path / "target-lineage")
    target_replay_path = tmp_path / "target-replay.sqlite3"
    donor_replay_path = tmp_path / "donor-replay.sqlite3"
    target_replay = _replay(
        target_replay_path,
        authorities,
        lineage_authority=target_lineage,
    )
    _sqlite_backup(target_replay_path, donor_replay_path)
    donor_lineage = target_lineage.fork()
    donor_replay = _replay(
        donor_replay_path,
        authorities,
        lineage_authority=donor_lineage,
    )
    target_state = tmp_path / "target.sqlite3"
    donor_state = tmp_path / "donor.sqlite3"
    original = signed_plan(authorities)
    donor_plan = signed_plan(authorities, nonce="donor-valid-nonce")
    _broker(
        target_state,
        authorities=authorities,
        ledger=ledger,
        replay=target_replay,
    ).start(original)
    _broker(
        donor_state,
        authorities=authorities,
        ledger=ledger,
        replay=donor_replay,
    ).start(donor_plan)

    _sqlite_backup(donor_replay_path, target_replay_path)
    _sqlite_backup(donor_state, target_state)

    with pytest.raises(SourceBrokerError, match="replay.*binding|genesis|authority|lineage"):
        _replay(
            target_replay_path,
            authorities,
            lineage_authority=target_lineage,
        )


def test_concurrent_brokers_cannot_bind_one_claim_to_different_plans(tmp_path: Path) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    ledger = IdempotentQuotaLedger(authorities)
    replay_path = tmp_path / "replay.sqlite3"
    barrier = threading.Barrier(2, timeout=5)
    first = _broker(
        tmp_path / "first.sqlite3",
        authorities=authorities,
        ledger=ledger,
        replay=BarrierReplayAuthority(_replay(replay_path, authorities), barrier),
    )
    second = _broker(
        tmp_path / "second.sqlite3",
        authorities=authorities,
        ledger=ledger,
        replay=BarrierReplayAuthority(_replay(replay_path, authorities), barrier),
    )
    plans = (
        signed_plan(authorities),
        signed_plan(authorities, nonce="concurrent-different-plan"),
    )

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = tuple(
            executor.submit(broker.start, plan)
            for broker, plan in zip((first, second), plans, strict=True)
        )
        results: list[object] = []
        errors: list[BaseException] = []
        for future in futures:
            try:
                results.append(future.result(timeout=10))
            except BaseException as exc:
                errors.append(exc)

    assert len(results) == 1
    assert len(errors) == 1
    assert isinstance(errors[0], SourceBrokerError)
    assert ledger.effect_counts["reserve"] == 1


def test_separate_replay_databases_cannot_create_alternate_genesis(
    tmp_path: Path,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    first = _replay(tmp_path / "first-replay.sqlite3", authorities)
    second = _replay(tmp_path / "second-replay.sqlite3", authorities)
    barrier = threading.Barrier(2, timeout=5)
    bindings = (
        {
            "operation_id": "a" * 64,
            "nonce": "first-genesis",
            "plan_hash": "c" * 64,
            "claim_token": "shared-claim",
            "broker_id": "lab-broker-a",
        },
        {
            "operation_id": "b" * 64,
            "nonce": "alternate-genesis",
            "plan_hash": "d" * 64,
            "claim_token": "shared-claim",
            "broker_id": "lab-broker-a",
        },
    )

    def consume(authority: SQLiteReplayAuthority, binding: dict[str, str]) -> EffectReceipt:
        barrier.wait(timeout=5)
        return authority.consume_once(**binding)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = tuple(
            executor.submit(consume, authority, binding)
            for authority, binding in zip((first, second), bindings, strict=True)
        )
        successes = 0
        failures = 0
        for future in futures:
            try:
                future.result(timeout=10)
                successes += 1
            except SourceBrokerError:
                failures += 1

    assert (successes, failures) == (1, 1)


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("operation_id", "b" * 64),
        ("nonce", "different-nonce"),
        ("plan_hash", "b" * 64),
        ("broker_id", "different-broker"),
    ],
)
def test_replay_claim_genesis_is_idempotent_only_for_the_exact_binding(
    tmp_path: Path,
    field: Literal["operation_id", "nonce", "plan_hash", "broker_id"],
    replacement: str,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    authority = _replay(tmp_path / "replay.sqlite3", authorities)
    binding = {
        "operation_id": "a" * 64,
        "nonce": "genesis-nonce",
        "plan_hash": "c" * 64,
        "claim_token": "genesis-claim",
        "broker_id": "lab-broker-a",
    }
    first = authority.consume_once(**binding)
    assert authority.consume_once(**binding) == first
    rebound = dict(binding)
    rebound[field] = replacement

    with pytest.raises(SourceBrokerError, match="replay|genesis|binding"):
        authority.consume_once(**rebound)


def test_replay_claim_commit_response_loss_is_idempotent_after_restart(tmp_path: Path) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    replay_path = tmp_path / "replay.sqlite3"
    binding = {
        "operation_id": "a" * 64,
        "nonce": "response-loss-nonce",
        "plan_hash": "c" * 64,
        "claim_token": "response-loss-claim",
        "broker_id": "lab-broker-a",
    }
    committed = _replay(replay_path, authorities).consume_once(**binding)
    restarted = _replay(replay_path, authorities)

    assert restarted.consume_once(**binding) == committed
    with sqlite3.connect(replay_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM source_claim_genesis").fetchone()[0] == 1


def test_lineage_checkpoint_commit_response_loss_recovers_from_pending_outbox(
    tmp_path: Path,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    lineage = _new_lineage_authority(tmp_path / "lineage")
    replay_path = tmp_path / "replay.sqlite3"
    binding = {
        "operation_id": "a" * 64,
        "nonce": "lineage-response-loss",
        "plan_hash": "c" * 64,
        "claim_token": "lineage-response-loss-claim",
        "broker_id": "lab-broker-a",
    }
    authority = _replay(
        replay_path,
        authorities,
        lineage_authority=lineage,
    )
    lineage.lose_next_response()

    with pytest.raises(ConnectionError, match="response was lost"):
        authority.consume_once(**binding)

    with sqlite3.connect(replay_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM source_claim_genesis").fetchone()[0] == 0
        assert (
            connection.execute("SELECT status FROM replay_lineage_outbox").fetchone()[0]
            == "pending"
        )
    restarted = _replay(
        replay_path,
        authorities,
        lineage_authority=lineage,
    )
    receipt = restarted.consume_once(**binding)

    assert receipt.operation_id == binding["operation_id"]
    assert lineage.advance_count == 1
    with sqlite3.connect(replay_path) as connection:
        assert (
            connection.execute("SELECT status FROM replay_lineage_outbox").fetchone()[0]
            == "applied"
        )


@pytest.mark.parametrize("phase", ["after_persist", "after_effect", "after_checkpoint"])
def test_lineage_checkpoint_crash_windows_recover_exactly_once(
    tmp_path: Path,
    phase: Literal["after_persist", "after_effect", "after_checkpoint"],
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    lineage = _new_lineage_authority(tmp_path / "lineage")
    replay_path = tmp_path / "replay.sqlite3"
    binding = {
        "operation_id": "a" * 64,
        "nonce": f"lineage-crash-{phase}",
        "plan_hash": "c" * 64,
        "claim_token": f"lineage-crash-claim-{phase}",
        "broker_id": "lab-broker-a",
    }
    crashing = _replay(
        replay_path,
        authorities,
        lineage_authority=lineage,
        fault_injector=CrashOnce(kind="lineage", phase=phase),
    )

    with pytest.raises(SimulatedCrash, match=f"lineage:{phase}"):
        crashing.consume_once(**binding)

    restarted = _replay(
        replay_path,
        authorities,
        lineage_authority=lineage,
    )
    assert restarted.consume_once(**binding).operation_id == binding["operation_id"]
    assert lineage.advance_count == 1


def test_external_lineage_rejects_local_database_rollback(tmp_path: Path) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    lineage = _new_lineage_authority(tmp_path / "lineage")
    replay_path = tmp_path / "replay.sqlite3"
    snapshot_path = tmp_path / "replay-snapshot.sqlite3"
    authority = _replay(
        replay_path,
        authorities,
        lineage_authority=lineage,
    )
    authority.consume_once(
        operation_id="a" * 64,
        nonce="rollback-first",
        plan_hash="c" * 64,
        claim_token="rollback-first-claim",
        broker_id="lab-broker-a",
    )
    _sqlite_backup(replay_path, snapshot_path)
    authority.consume_once(
        operation_id="b" * 64,
        nonce="rollback-second",
        plan_hash="d" * 64,
        claim_token="rollback-second-claim",
        broker_id="lab-broker-a",
    )
    _sqlite_backup(snapshot_path, replay_path)

    with pytest.raises(SourceBrokerError, match="lineage|rollback|current"):
        _replay(
            replay_path,
            authorities,
            lineage_authority=lineage,
        )


def test_lost_pending_checkpoint_cannot_open_an_alternate_fork(tmp_path: Path) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    lineage = _new_lineage_authority(tmp_path / "lineage")
    replay_path = tmp_path / "replay.sqlite3"
    authority = _replay(
        replay_path,
        authorities,
        lineage_authority=lineage,
    )
    lineage.lose_next_response()
    with pytest.raises(ConnectionError):
        authority.consume_once(
            operation_id="a" * 64,
            nonce="lost-pending",
            plan_hash="c" * 64,
            claim_token="lost-pending-claim",
            broker_id="lab-broker-a",
        )
    with sqlite3.connect(replay_path) as connection:
        connection.execute("DELETE FROM replay_lineage_outbox")

    with pytest.raises(SourceBrokerError, match="lineage|current|checkpoint"):
        _replay(
            replay_path,
            authorities,
            lineage_authority=lineage,
        )
    with pytest.raises(SourceBrokerError, match="lineage|current|checkpoint"):
        _replay(
            tmp_path / "alternate.sqlite3",
            authorities,
            lineage_authority=lineage,
        )


def test_broker_fails_closed_when_external_lineage_verification_is_unavailable(
    tmp_path: Path,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    lineage = _new_lineage_authority(tmp_path / "lineage")
    ledger = IdempotentQuotaLedger(authorities)
    replay = _replay(
        tmp_path / "replay.sqlite3",
        authorities,
        lineage_authority=lineage,
    )
    broker = _broker(
        tmp_path / "broker.sqlite3",
        authorities=authorities,
        ledger=ledger,
        replay=replay,
    )
    plan = signed_plan(authorities)
    broker.start(plan)
    lineage.make_verification_unavailable()

    with pytest.raises(ConnectionError, match="verification is unavailable"):
        broker.finalize(plan)

    assert ledger.effect_counts["release"] == 0


@pytest.mark.parametrize("tamper", ["delete", "row", "receipt"])
def test_replay_authority_detects_deleted_or_tampered_genesis(
    tmp_path: Path,
    tamper: Literal["delete", "row", "receipt"],
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    replay_path = tmp_path / "replay.sqlite3"
    authority = _replay(replay_path, authorities)
    authority.consume_once(
        operation_id="a" * 64,
        nonce="tamper-nonce",
        plan_hash="c" * 64,
        claim_token="tamper-claim",
        broker_id="lab-broker-a",
    )

    with sqlite3.connect(replay_path) as connection:
        if tamper == "delete":
            connection.execute("DELETE FROM source_claim_genesis")
        elif tamper == "row":
            connection.execute(
                "UPDATE source_claim_genesis SET plan_hash = ?",
                ("f" * 64,),
            )
        else:
            receipt = json.loads(
                connection.execute("SELECT receipt_json FROM source_claim_genesis").fetchone()[0]
            )
            receipt["signature"] = f"AAAA{receipt['signature']}"
            connection.execute(
                "UPDATE source_claim_genesis SET receipt_json = ?",
                (json.dumps(receipt, sort_keys=True, separators=(",", ":")),),
            )

    with pytest.raises(SourceBrokerError, match="replay authority|genesis|integrity"):
        _replay(replay_path, authorities)


def test_replay_authority_rejects_legacy_v1_schema_without_explicit_migration(
    tmp_path: Path,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    replay_path = tmp_path / "legacy-replay.sqlite3"
    with sqlite3.connect(replay_path) as connection:
        connection.execute(
            """
            CREATE TABLE source_plan_nonce (
                nonce TEXT PRIMARY KEY,
                operation_id TEXT NOT NULL UNIQUE,
                plan_hash TEXT NOT NULL,
                claim_token TEXT NOT NULL,
                broker_id TEXT NOT NULL,
                receipt_json TEXT NOT NULL
            )
            """
        )

    with pytest.raises(SourceBrokerError, match="legacy|migration|schema"):
        _replay(replay_path, authorities)


def test_replay_authority_requires_an_external_lineage_authority(tmp_path: Path) -> None:
    authorities = create_test_authorities(tmp_path / "keys")

    with pytest.raises(ValueError, match="external replay lineage authority"):
        SQLiteReplayAuthority(
            tmp_path / "replay.sqlite3",
            authority_id="global-source-use",
            signer=authorities.replay,
            keyring=authorities.replay_keyring,
            lineage_authority=None,
        )


def test_replay_authority_rejects_legacy_v2_schema_without_explicit_migration(
    tmp_path: Path,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    replay_path = tmp_path / "legacy-v2-replay.sqlite3"
    with sqlite3.connect(replay_path) as connection:
        connection.executescript(
            """
            CREATE TABLE replay_authority_meta (
                singleton INTEGER PRIMARY KEY,
                schema_version INTEGER NOT NULL,
                authority_id TEXT NOT NULL,
                head_json TEXT NOT NULL
            );
            CREATE TABLE source_claim_genesis (
                claim_token TEXT PRIMARY KEY,
                sequence INTEGER NOT NULL,
                plan_hash TEXT NOT NULL,
                nonce TEXT NOT NULL,
                broker_id TEXT NOT NULL,
                operation_id TEXT NOT NULL,
                record_json TEXT NOT NULL,
                receipt_json TEXT NOT NULL
            );
            """
        )

    with pytest.raises(SourceBrokerError, match="legacy.*v2|migration"):
        _replay(replay_path, authorities)


@pytest.mark.parametrize("tamper", ["delete_outbox", "request", "checkpoint_receipt"])
def test_replay_lineage_outbox_and_checkpoint_tamper_is_rejected(
    tmp_path: Path,
    tamper: Literal["delete_outbox", "request", "checkpoint_receipt"],
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    lineage = _new_lineage_authority(tmp_path / "lineage")
    replay_path = tmp_path / "replay.sqlite3"
    _replay(
        replay_path,
        authorities,
        lineage_authority=lineage,
    ).consume_once(
        operation_id="a" * 64,
        nonce="lineage-tamper",
        plan_hash="c" * 64,
        claim_token="lineage-tamper-claim",
        broker_id="lab-broker-a",
    )

    with sqlite3.connect(replay_path) as connection:
        if tamper == "delete_outbox":
            connection.execute("DELETE FROM replay_lineage_outbox")
        elif tamper == "request":
            request = json.loads(
                connection.execute("SELECT request_json FROM replay_lineage_outbox").fetchone()[0]
            )
            request["claim_binding_hash"] = "f" * 64
            connection.execute(
                "UPDATE replay_lineage_outbox SET request_json = ?",
                (json.dumps(request, sort_keys=True, separators=(",", ":")),),
            )
        else:
            receipt = json.loads(
                connection.execute(
                    "SELECT checkpoint_receipt_json FROM replay_lineage_outbox"
                ).fetchone()[0]
            )
            receipt["signature"] = f"AAAA{receipt['signature']}"
            receipt_json = json.dumps(receipt, sort_keys=True, separators=(",", ":"))
            connection.execute(
                "UPDATE replay_lineage_outbox SET checkpoint_receipt_json = ?",
                (receipt_json,),
            )
            connection.execute(
                "UPDATE replay_authority_meta SET checkpoint_receipt_json = ?",
                (receipt_json,),
            )

    with pytest.raises(SourceBrokerError, match="lineage|outbox|checkpoint"):
        _replay(
            replay_path,
            authorities,
            lineage_authority=lineage,
        )


def test_recovery_rejects_tampered_outbox_before_quota_effect(tmp_path: Path) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    ledger = IdempotentQuotaLedger(authorities)
    replay = _replay(tmp_path / "replay.sqlite3", authorities)
    state_path = tmp_path / "broker.sqlite3"
    clock = ManualLeaseClock()
    crash = CrashOnce(kind="reserve", phase="after_persist")
    broker = _broker(
        state_path,
        authorities=authorities,
        ledger=ledger,
        replay=replay,
        fault_injector=crash,
        lease_clock=clock,
    )
    with pytest.raises(SimulatedCrash):
        broker.start(signed_plan(authorities))

    with sqlite3.connect(state_path) as connection:
        connection.execute(
            """
            UPDATE broker_outbox
            SET payload_json = '{"claim_token":"claim-123","source":"evil","units":4}'
            WHERE kind = 'reserve'
            """
        )

    clock.advance(6.0)
    with pytest.raises(SourceBrokerError, match="operation id"):
        _broker(
            state_path,
            authorities=authorities,
            ledger=ledger,
            replay=replay,
            lease_clock=clock,
        )
    assert ledger.effect_counts["reserve"] == 0


def test_concurrent_start_reserves_once_and_finalize_race_releases_once(tmp_path: Path) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    ledger = IdempotentQuotaLedger(authorities)
    replay = _replay(tmp_path / "replay.sqlite3", authorities)
    state_path = tmp_path / "broker.sqlite3"
    first = _broker(
        state_path,
        authorities=authorities,
        ledger=ledger,
        replay=replay,
    )
    second = _broker(
        state_path,
        authorities=authorities,
        ledger=ledger,
        replay=replay,
    )
    plan = signed_plan(authorities)

    with ThreadPoolExecutor(max_workers=2) as executor:
        reservations = tuple(executor.map(lambda broker: broker.start(plan), (first, second)))
    assert reservations[0] == reservations[1]
    assert ledger.effect_counts["reserve"] == 1

    first.call(
        plan,
        DailyRequest(trade_date="2026-08-04"),
        idempotency_key="finalize-race-1",
    )
    with ThreadPoolExecutor(max_workers=2) as executor:
        statements = tuple(executor.map(lambda broker: broker.finalize(plan), (first, second)))
    assert statements[0] == statements[1]
    assert ledger.effect_counts["release"] == 1


def test_same_broker_concurrent_finalize_returns_the_same_statement_after_cas_loss(
    tmp_path: Path,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    ledger = IdempotentQuotaLedger(authorities)
    replay = _replay(tmp_path / "replay.sqlite3", authorities)
    barrier = ReleaseAppliedBarrier()
    broker = _broker(
        tmp_path / "broker.sqlite3",
        authorities=authorities,
        ledger=ledger,
        replay=replay,
        fault_injector=barrier,
    )
    plan = signed_plan(authorities)
    broker.start(plan)
    barrier.arm()

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(broker.finalize, plan)
        assert barrier.entered.wait(timeout=5)
        second = executor.submit(broker.finalize, plan)
        try:
            winning_statement = second.result(timeout=5)
        finally:
            barrier.resume.set()
        losing_statement = first.result(timeout=5)

    assert losing_statement == winning_statement
    assert ledger.effect_counts["release"] == 1


def test_finalize_winner_rejects_plan_rebind_after_release_effect(tmp_path: Path) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    ledger = IdempotentQuotaLedger(authorities)
    replay = _replay(tmp_path / "replay.sqlite3", authorities)
    barrier = ReleaseAppliedBarrier()
    state_path = tmp_path / "broker.sqlite3"
    broker = _broker(
        state_path,
        authorities=authorities,
        ledger=ledger,
        replay=replay,
        fault_injector=barrier,
    )
    plan = signed_plan(authorities)
    replacement = signed_plan(authorities, nonce="replacement-finalize-nonce")
    broker.start(plan)
    barrier.arm()

    with ThreadPoolExecutor(max_workers=1) as executor:
        winner = executor.submit(broker.finalize, plan)
        assert barrier.entered.wait(timeout=5)
        try:
            with sqlite3.connect(state_path) as connection:
                connection.execute(
                    """
                    UPDATE broker_session SET plan_json = ?, plan_hash = ?
                    WHERE claim_token = ?
                    """,
                    (
                        replacement.model_dump_json(),
                        replacement.plan_hash,
                        plan.claim_token,
                    ),
                )
        finally:
            barrier.resume.set()
        with pytest.raises(SourceBrokerError, match="anchor|finalization plan|persisted"):
            winner.result(timeout=5)

    with sqlite3.connect(state_path) as connection:
        status = connection.execute(
            "SELECT status FROM broker_session WHERE claim_token = ?",
            (plan.claim_token,),
        ).fetchone()[0]
    assert status == "finalizing"
    assert ledger.effect_counts["release"] == 1


@pytest.mark.parametrize("tamper", ["payload", "effect_receipt"])
def test_finalize_rejects_tampered_replay_plan_anchor(
    tmp_path: Path,
    tamper: Literal["payload", "effect_receipt"],
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    ledger = IdempotentQuotaLedger(authorities)
    replay = _replay(tmp_path / "replay.sqlite3", authorities)
    state_path = tmp_path / "broker.sqlite3"
    broker = _broker(
        state_path,
        authorities=authorities,
        ledger=ledger,
        replay=replay,
    )
    plan = signed_plan(authorities)
    broker.start(plan)

    with sqlite3.connect(state_path) as connection:
        if tamper == "payload":
            payload = json.loads(
                connection.execute(
                    "SELECT payload_json FROM broker_outbox WHERE kind = 'replay'"
                ).fetchone()[0]
            )
            payload["plan_hash"] = "f" * 64
            connection.execute(
                "UPDATE broker_outbox SET payload_json = ? WHERE kind = 'replay'",
                (json.dumps(payload, sort_keys=True, separators=(",", ":")),),
            )
        else:
            receipt = json.loads(
                connection.execute(
                    "SELECT effect_receipt_json FROM broker_outbox WHERE kind = 'replay'"
                ).fetchone()[0]
            )
            receipt["payload_hash"] = "f" * 64
            connection.execute(
                "UPDATE broker_outbox SET effect_receipt_json = ? WHERE kind = 'replay'",
                (json.dumps(receipt, sort_keys=True, separators=(",", ":")),),
            )

    with pytest.raises(SourceBrokerError, match="outbox|effect receipt|anchor"):
        broker.finalize(plan)
    assert ledger.effect_counts["release"] == 0


def test_final_statement_v2_binds_plan_hash_and_rejects_legacy_shape(tmp_path: Path) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    ledger = IdempotentQuotaLedger(authorities)
    replay = _replay(tmp_path / "replay.sqlite3", authorities)
    state_path = tmp_path / "broker.sqlite3"
    broker = _broker(
        state_path,
        authorities=authorities,
        ledger=ledger,
        replay=replay,
    )
    plan = signed_plan(authorities)
    broker.start(plan)
    statement = broker.finalize(plan)

    assert statement.schema_version == 2
    assert statement.plan_hash == plan.plan_hash
    legacy = statement.model_dump(mode="json")
    legacy.pop("schema_version")
    legacy.pop("plan_hash")
    with sqlite3.connect(state_path) as connection:
        connection.execute(
            "UPDATE broker_session SET statement_json = ? WHERE claim_token = ?",
            (json.dumps(legacy, sort_keys=True, separators=(",", ":")), plan.claim_token),
        )

    with pytest.raises(SourceBrokerError, match="statement.*schema"):
        broker.finalize(plan)


@pytest.mark.parametrize("tamper", ["signature", "other_statement"])
def test_finalize_cas_loss_does_not_swallow_tampered_or_inconsistent_statement(
    tmp_path: Path,
    tamper: Literal["signature", "other_statement"],
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    ledger = IdempotentQuotaLedger(authorities)
    replay = _replay(tmp_path / "replay.sqlite3", authorities)
    barrier = ReleaseAppliedBarrier()
    state_path = tmp_path / "broker.sqlite3"
    broker = _broker(
        state_path,
        authorities=authorities,
        ledger=ledger,
        replay=replay,
        fault_injector=barrier,
    )
    replacement_json: str | None = None
    if tamper == "other_statement":
        other_plan = signed_plan(
            authorities,
            claim_token="other-finalize-claim",
            nonce="other-finalize-nonce",
        )
        broker.start(other_plan)
        replacement_json = broker.finalize(other_plan).model_dump_json()

    plan = signed_plan(authorities)
    broker.start(plan)
    barrier.arm()
    with ThreadPoolExecutor(max_workers=1) as executor:
        losing = executor.submit(broker.finalize, plan)
        assert barrier.entered.wait(timeout=5)
        try:
            broker.finalize(plan)
            with sqlite3.connect(state_path) as connection:
                if tamper == "signature":
                    statement_json = connection.execute(
                        "SELECT statement_json FROM broker_session WHERE claim_token = ?",
                        (plan.claim_token,),
                    ).fetchone()[0]
                    replacement_json = statement_json.replace('"signature":"', '"signature":"AAAA')
                assert replacement_json is not None
                connection.execute(
                    "UPDATE broker_session SET statement_json = ? WHERE claim_token = ?",
                    (replacement_json, plan.claim_token),
                )
        finally:
            barrier.resume.set()
        with pytest.raises(SourceBrokerError, match="statement"):
            losing.result(timeout=5)


def test_old_session_snapshot_cannot_dispatch_after_finalize(tmp_path: Path) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    ledger = IdempotentQuotaLedger(authorities)
    replay = _replay(tmp_path / "replay.sqlite3", authorities)
    provider = EchoProvider()
    barrier = SnapshotBarrier()
    broker = _broker(
        tmp_path / "broker.sqlite3",
        authorities=authorities,
        ledger=ledger,
        replay=replay,
        provider=provider,
        fault_injector=barrier,
    )
    plan = signed_plan(authorities)
    broker.start(plan)

    with ThreadPoolExecutor(max_workers=1) as executor:
        pending = executor.submit(
            broker.call,
            plan,
            DailyRequest(trade_date="2026-08-04"),
            idempotency_key="old-snapshot-1",
        )
        assert barrier.snapshot_taken.wait(timeout=5)
        broker.finalize(plan)
        barrier.resume.set()
        with pytest.raises(SourceBrokerError, match="finalized|active"):
            pending.result(timeout=5)

    assert provider.calls == []
    assert ledger.effect_counts["dispatch"] == 0


@pytest.mark.parametrize("tamper", ["duplicate_receipt", "sequence_gap"])
def test_finalize_validates_receipt_rows_hash_uniqueness_and_continuous_sequence(
    tmp_path: Path,
    tamper: Literal["duplicate_receipt", "sequence_gap"],
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    ledger = IdempotentQuotaLedger(authorities)
    replay = _replay(tmp_path / "replay.sqlite3", authorities)
    state_path = tmp_path / "broker.sqlite3"
    broker = _broker(
        state_path,
        authorities=authorities,
        ledger=ledger,
        replay=replay,
    )
    plan = signed_plan(authorities)
    broker.call(
        plan,
        DailyRequest(trade_date="2026-08-04"),
        idempotency_key="receipt-row-1",
    )
    broker.call(
        plan,
        DailyRequest(trade_date="2026-08-05"),
        idempotency_key="receipt-row-2",
    )

    with sqlite3.connect(state_path) as connection:
        rows = connection.execute(
            "SELECT call_id, receipt_json, receipt_hash FROM broker_call ORDER BY call_seq"
        ).fetchall()
        if tamper == "duplicate_receipt":
            connection.execute(
                "UPDATE broker_call SET receipt_json = ?, receipt_hash = ? WHERE call_id = ?",
                (rows[0][1], rows[0][2], rows[1][0]),
            )
        else:
            connection.execute(
                "UPDATE broker_call SET call_seq = 3 WHERE call_id = ?",
                (rows[1][0],),
            )

    with pytest.raises(SourceBrokerError, match="receipt integrity"):
        broker.finalize(plan)
    assert ledger.effect_counts["release"] == 0


def test_finalized_statement_cannot_be_replaced_by_another_valid_broker_statement(
    tmp_path: Path,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    ledger = IdempotentQuotaLedger(authorities)
    replay = _replay(tmp_path / "replay.sqlite3", authorities)
    state_path = tmp_path / "broker.sqlite3"
    broker = _broker(
        state_path,
        authorities=authorities,
        ledger=ledger,
        replay=replay,
    )
    first_plan = signed_plan(authorities)
    second_plan = signed_plan(
        authorities,
        claim_token="claim-456",
        nonce="nonce-456",
    )
    broker.start(first_plan)
    first_statement = broker.finalize(first_plan)
    broker.start(second_plan)
    second_statement = broker.finalize(second_plan)
    assert first_statement.claim_token != second_statement.claim_token

    with sqlite3.connect(state_path) as connection:
        connection.execute(
            "UPDATE broker_session SET statement_json = ? WHERE claim_token = ?",
            (second_statement.model_dump_json(), first_plan.claim_token),
        )

    with pytest.raises(SourceBrokerError, match="statement integrity"):
        broker.finalize(first_plan)


@pytest.mark.parametrize("phase", ["after_persist", "after_effect"])
@pytest.mark.parametrize("kind", ["reserve", "finalize", "release", "recover"])
def test_outbox_crashes_replay_each_quota_effect_once(
    tmp_path: Path,
    kind: Literal["reserve", "finalize", "release", "recover"],
    phase: Literal["after_persist", "after_effect"],
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    ledger = IdempotentQuotaLedger(authorities)
    replay = _replay(tmp_path / "replay.sqlite3", authorities)
    state_path = tmp_path / "broker.sqlite3"
    plan = signed_plan(authorities)
    crash = CrashOnce(kind=kind, phase=phase)
    clock = ManualLeaseClock()

    if kind == "reserve":
        crashing = _broker(
            state_path,
            authorities=authorities,
            ledger=ledger,
            replay=replay,
            fault_injector=crash,
            lease_clock=clock,
        )
        with pytest.raises(SimulatedCrash):
            crashing.start(plan)
    elif kind == "recover":
        interrupted = _broker(
            state_path,
            authorities=authorities,
            ledger=ledger,
            replay=replay,
            provider=InterruptedProvider(),
            lease_clock=clock,
        )
        with pytest.raises(KeyboardInterrupt):
            interrupted.call(
                plan,
                DailyRequest(trade_date="2026-08-04"),
                idempotency_key="recover-crash-1",
            )
        clock.advance(6.0)
        with pytest.raises(SimulatedCrash):
            _broker(
                state_path,
                authorities=authorities,
                ledger=ledger,
                replay=replay,
                fault_injector=crash,
                lease_clock=clock,
            )
    else:
        crashing = _broker(
            state_path,
            authorities=authorities,
            ledger=ledger,
            replay=replay,
            fault_injector=crash,
            lease_clock=clock,
        )
        if kind == "finalize":
            with pytest.raises(SimulatedCrash):
                crashing.call(
                    plan,
                    DailyRequest(trade_date="2026-08-04"),
                    idempotency_key="finalize-crash-1",
                )
        else:
            crashing.call(
                plan,
                DailyRequest(trade_date="2026-08-04"),
                idempotency_key="release-crash-1",
            )
            with pytest.raises(SimulatedCrash):
                crashing.finalize(plan)

    clock.advance(6.0)
    recovered = _broker(
        state_path,
        authorities=authorities,
        ledger=ledger,
        replay=replay,
        lease_clock=clock,
    )
    recovered.start(plan)
    statement = recovered.finalize(plan)

    assert crash.triggered
    assert ledger.effect_counts[kind] == 1
    if kind == "recover":
        assert statement.calls_unknown == 1


@pytest.mark.parametrize("tamper", ["result", "effect_receipt"])
def test_outbox_applied_fast_path_rejects_tampered_result_and_effect_receipt(
    tmp_path: Path,
    tamper: Literal["result", "effect_receipt"],
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    ledger = IdempotentQuotaLedger(authorities)
    replay = _replay(tmp_path / "replay.sqlite3", authorities)
    state_path = tmp_path / "broker.sqlite3"
    clock = ManualLeaseClock()
    crash = CrashOnce(kind="reserve", phase="after_apply")
    with pytest.raises(SimulatedCrash):
        _broker(
            state_path,
            authorities=authorities,
            ledger=ledger,
            replay=replay,
            fault_injector=crash,
            lease_clock=clock,
        ).start(signed_plan(authorities))

    with sqlite3.connect(state_path) as connection:
        if tamper == "result":
            connection.execute(
                "UPDATE broker_outbox SET result_json = ? WHERE kind = 'reserve'",
                (
                    '{"claim_token":"claim-123","reservation_id":"fake",'
                    '"reserved_units":999,"source":"tushare"}',
                ),
            )
        else:
            receipt_json = connection.execute(
                "SELECT effect_receipt_json FROM broker_outbox WHERE kind = 'reserve'"
            ).fetchone()[0]
            connection.execute(
                "UPDATE broker_outbox SET effect_receipt_json = ? WHERE kind = 'reserve'",
                (receipt_json.replace('"signature":"', '"signature":"AAAA'),),
            )
    clock.advance(6.0)
    with pytest.raises(SourceBrokerError, match="outbox integrity|effect receipt"):
        _broker(
            state_path,
            authorities=authorities,
            ledger=ledger,
            replay=replay,
            lease_clock=clock,
        )


def test_active_session_revalidates_applied_reserve_effect_receipt(tmp_path: Path) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    ledger = IdempotentQuotaLedger(authorities)
    replay = _replay(tmp_path / "replay.sqlite3", authorities)
    state_path = tmp_path / "broker.sqlite3"
    broker = _broker(state_path, authorities=authorities, ledger=ledger, replay=replay)
    plan = signed_plan(authorities)
    broker.start(plan)
    with sqlite3.connect(state_path) as connection:
        receipt_json = connection.execute(
            "SELECT effect_receipt_json FROM broker_outbox WHERE kind = 'reserve'"
        ).fetchone()[0]
        connection.execute(
            "UPDATE broker_outbox SET effect_receipt_json = ? WHERE kind = 'reserve'",
            (receipt_json.replace('"signature":"', '"signature":"AAAA'),),
        )

    with pytest.raises(SourceBrokerError, match="effect receipt|outbox integrity"):
        broker.start(plan)


def test_operation_id_does_not_authorize_a_forged_ledger_effect_receipt(
    tmp_path: Path,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    ledger = IdempotentQuotaLedger(authorities)
    replay = _replay(tmp_path / "replay.sqlite3", authorities)
    state_path = tmp_path / "broker.sqlite3"
    plan = signed_plan(authorities)
    clock = ManualLeaseClock()
    crash = CrashOnce(kind="reserve", phase="after_effect")
    with pytest.raises(SimulatedCrash):
        _broker(
            state_path,
            authorities=authorities,
            ledger=ledger,
            replay=replay,
            fault_injector=crash,
            lease_clock=clock,
        ).start(plan)
    ledger.forge_effect_receipt(kind="reserve", payload_hash="f" * 64)

    clock.advance(6.0)
    with pytest.raises(SourceBrokerError, match="effect receipt"):
        _broker(
            state_path,
            authorities=authorities,
            ledger=ledger,
            replay=replay,
            lease_clock=clock,
        )
    assert ledger.effect_counts["reserve"] == 1


@pytest.mark.parametrize("tamper", ["status", "result", "payload999"])
def test_pending_outbox_database_tampering_fails_before_external_effect(
    tmp_path: Path,
    tamper: Literal["status", "result", "payload999"],
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    ledger = IdempotentQuotaLedger(authorities)
    replay = _replay(tmp_path / "replay.sqlite3", authorities)
    state_path = tmp_path / "broker.sqlite3"
    clock = ManualLeaseClock()
    crash = CrashOnce(kind="reserve", phase="after_persist")
    with pytest.raises(SimulatedCrash):
        _broker(
            state_path,
            authorities=authorities,
            ledger=ledger,
            replay=replay,
            fault_injector=crash,
            lease_clock=clock,
        ).start(signed_plan(authorities))

    with sqlite3.connect(state_path) as connection:
        if tamper == "status":
            connection.execute("UPDATE broker_outbox SET status = 'applied' WHERE kind = 'reserve'")
        elif tamper == "result":
            connection.execute(
                "UPDATE broker_outbox SET result_json = 'null' WHERE kind = 'reserve'"
            )
        else:
            payload = connection.execute(
                "SELECT payload_json FROM broker_outbox WHERE kind = 'reserve'"
            ).fetchone()[0]
            connection.execute(
                "UPDATE broker_outbox SET payload_json = ? WHERE kind = 'reserve'",
                (payload.replace('"units":4', '"units":999'),),
            )

    clock.advance(6.0)
    with pytest.raises(SourceBrokerError, match="outbox integrity|operation id"):
        _broker(
            state_path,
            authorities=authorities,
            ledger=ledger,
            replay=replay,
            lease_clock=clock,
        )
    assert ledger.effect_counts["reserve"] == 0


def test_shared_replay_authority_rejects_tampered_claim_receipt(tmp_path: Path) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    ledger = IdempotentQuotaLedger(authorities)
    replay_path = tmp_path / "replay.sqlite3"
    replay = _replay(replay_path, authorities)
    state_path = tmp_path / "broker.sqlite3"
    clock = ManualLeaseClock()
    crash = CrashOnce(kind="replay", phase="after_effect")
    with pytest.raises(SimulatedCrash):
        _broker(
            state_path,
            authorities=authorities,
            ledger=ledger,
            replay=replay,
            fault_injector=crash,
            lease_clock=clock,
        ).start(signed_plan(authorities))

    with sqlite3.connect(replay_path) as connection:
        receipt_json = connection.execute(
            "SELECT receipt_json FROM source_claim_genesis"
        ).fetchone()[0]
        connection.execute(
            "UPDATE source_claim_genesis SET receipt_json = ?",
            (receipt_json.replace('"signature":"', '"signature":"AAAA'),),
        )

    clock.advance(6.0)
    with pytest.raises(SourceBrokerError, match="replay claim|signature"):
        _broker(
            state_path,
            authorities=authorities,
            ledger=ledger,
            replay=replay,
            lease_clock=clock,
        )


def test_provider_registry_rejects_open_mutable_and_aliased_nested_schemas() -> None:
    class NonFrozen(BaseModel):
        model_config = ConfigDict(extra="forbid")
        value: str

    class DictRequest(RuntimeContractModel):
        value: dict[str, str]

    class MappingRequest(RuntimeContractModel):
        value: Mapping[str, str]

    class AnyRequest(RuntimeContractModel):
        value: Any

    class ObjectRequest(RuntimeContractModel):
        value: object

    class ListRequest(RuntimeContractModel):
        value: list[str]

    class SetRequest(RuntimeContractModel):
        value: set[str]

    class ValidationAliasRequest(RuntimeContractModel):
        value: str = Field(validation_alias="access_token")

    class SerializationAliasRequest(RuntimeContractModel):
        value: str = Field(serialization_alias="endpoint")

    class FrozenTupleItem(RuntimeContractModel):
        value: str

    class FrozenTupleRequest(RuntimeContractModel):
        values: tuple[FrozenTupleItem, ...]

    registry = ProviderRegistry()
    for request_model in (
        NonFrozen,
        DictRequest,
        MappingRequest,
        AnyRequest,
        ObjectRequest,
        ListRequest,
        SetRequest,
        ValidationAliasRequest,
        SerializationAliasRequest,
    ):
        with pytest.raises(SourceBrokerError, match="frozen|mapping|mutable|alias|unsupported"):
            registry.register(
                source="tushare",
                operation=request_model.__name__,
                provider=EchoProvider(),
                request_model=request_model,
                response_model=DailyResponse,
            )
    registry.register(
        source="tushare",
        operation="frozen_tuple",
        provider=EchoProvider(),
        request_model=FrozenTupleRequest,
        response_model=DailyResponse,
    )


def test_provider_receives_deeply_immutable_copy_bound_to_request_hash(tmp_path: Path) -> None:
    class MutationProvider:
        def __init__(self) -> None:
            self.capability: ProviderCapability | None = None
            self.mutation_failures = 0

        def dispatch(self, capability: ProviderCapability) -> BaseModel:
            self.capability = capability
            try:
                capability.request.trade_date = "tampered"  # type: ignore[attr-defined,misc]
            except ValidationError:
                self.mutation_failures += 1
            try:
                capability.request.filters.market = "tampered"  # type: ignore[attr-defined,misc,union-attr]
            except ValidationError:
                self.mutation_failures += 1
            return DailyResponse(rows=1)

    authorities = create_test_authorities(tmp_path / "keys")
    provider = MutationProvider()
    ledger = IdempotentQuotaLedger(authorities)
    broker = _broker(
        tmp_path / "broker.sqlite3",
        authorities=authorities,
        ledger=ledger,
        replay=_replay(tmp_path / "replay.sqlite3", authorities),
        provider=provider,
    )
    request = DailyRequest(trade_date="2026-08-04", filters={"market": "SZ"})
    receipt = broker.call(
        signed_plan(authorities),
        request,
        idempotency_key="immutable-request-1",
    )

    assert provider.capability is not None
    assert provider.capability.request is not request
    assert provider.mutation_failures == 2
    assert receipt.request_hash == canonical_sha256(provider.capability.request)


def test_untrusted_provider_output_cannot_replace_broker_receipt(tmp_path: Path) -> None:
    class ForgingProvider:
        def dispatch(self, capability: ProviderCapability) -> BaseModel:
            return SourceCallReceipt(
                broker_id="untrusted-provider",
                call_id="0" * 64,
                claim_token="forged",
                idempotency_key="forged-provider-1",
                manifest_hash="0" * 64,
                source="tushare",
                operation="daily_bars",
                call_seq=1,
                request_hash="0" * 64,
                response_hash="0" * 64,
                cost=1,
                outcome="success",
                key_id="forged",
                signature="forged",
            )

    authorities = create_test_authorities(tmp_path / "keys")
    ledger = IdempotentQuotaLedger(authorities)
    broker = _broker(
        tmp_path / "broker.sqlite3",
        authorities=authorities,
        ledger=ledger,
        replay=_replay(tmp_path / "replay.sqlite3", authorities),
        provider=ForgingProvider(),
    )
    receipt = broker.call(
        signed_plan(authorities),
        DailyRequest(trade_date="2026-08-04"),
        idempotency_key="forged-provider-1",
    )

    assert receipt.broker_id == "lab-broker-a"
    assert receipt.key_id == authorities.broker.key_id
    assert receipt.outcome == "failure"
    assert broker.verify_receipt(receipt)


@pytest.mark.parametrize("column,value", [("state", "failure"), ("manifest_hash", "f" * 64)])
def test_finalize_rejects_receipt_row_state_and_manifest_tampering(
    tmp_path: Path,
    column: Literal["state", "manifest_hash"],
    value: str,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    ledger = IdempotentQuotaLedger(authorities)
    replay = _replay(tmp_path / "replay.sqlite3", authorities)
    state_path = tmp_path / "broker.sqlite3"
    broker = _broker(state_path, authorities=authorities, ledger=ledger, replay=replay)
    plan = signed_plan(authorities)
    broker.call(
        plan,
        DailyRequest(trade_date="2026-08-04"),
        idempotency_key="row-tamper-1",
    )
    with sqlite3.connect(state_path) as connection:
        connection.execute(f"UPDATE broker_call SET {column} = ?", (value,))

    with pytest.raises(SourceBrokerError, match="receipt integrity"):
        broker.finalize(plan)


def test_intent_effect_crash_is_consumed_but_not_reported_as_dispatched(tmp_path: Path) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    ledger = IdempotentQuotaLedger(authorities)
    replay = _replay(tmp_path / "replay.sqlite3", authorities)
    state_path = tmp_path / "broker.sqlite3"
    plan = signed_plan(authorities)
    clock = ManualLeaseClock()
    crash = CrashOnce(kind="intent", phase="after_effect")
    broker = _broker(
        state_path,
        authorities=authorities,
        ledger=ledger,
        replay=replay,
        fault_injector=crash,
        lease_clock=clock,
    )
    with pytest.raises(SimulatedCrash):
        broker.call(
            plan,
            DailyRequest(trade_date="2026-08-04"),
            idempotency_key="intent-crash-1",
        )

    clock.advance(6.0)
    recovered = _broker(
        state_path,
        authorities=authorities,
        ledger=ledger,
        replay=replay,
        lease_clock=clock,
    )
    statement = recovered.finalize(plan)
    with sqlite3.connect(state_path) as connection:
        receipt_json = connection.execute("SELECT receipt_json FROM broker_call").fetchone()[0]

    assert '"outcome":"unknown_before_dispatch"' in receipt_json
    assert statement.calls_dispatched == 0
    assert statement.calls_unknown == 0
    assert statement.calls_consumed_unknown == 1


def test_two_concurrent_recoveries_are_idempotent_after_cas_loss(tmp_path: Path) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    ledger = IdempotentQuotaLedger(authorities)
    replay = _replay(tmp_path / "replay.sqlite3", authorities)
    state_path = tmp_path / "broker.sqlite3"
    plan = signed_plan(authorities)
    clock = ManualLeaseClock()
    interrupted = _broker(
        state_path,
        authorities=authorities,
        ledger=ledger,
        replay=replay,
        provider=InterruptedProvider(),
        lease_clock=clock,
    )
    with pytest.raises(KeyboardInterrupt):
        interrupted.call(
            plan,
            DailyRequest(trade_date="2026-08-04"),
            idempotency_key="concurrent-recover-1",
        )

    clock.advance(6.0)
    barrier = RecoveryBarrier()
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = tuple(
            executor.submit(
                _broker,
                state_path,
                authorities=authorities,
                ledger=ledger,
                replay=replay,
                fault_injector=barrier,
                lease_clock=clock,
            )
            for _ in range(2)
        )
        brokers = tuple(future.result(timeout=10) for future in futures)

    statements = tuple(broker.finalize(plan) for broker in brokers)
    assert statements[0] == statements[1]
    assert ledger.effect_counts["recover"] == 1


def test_recovery_rejects_rebound_persisted_plan_before_any_call_effect(
    tmp_path: Path,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    ledger = IdempotentQuotaLedger(authorities)
    replay = _replay(tmp_path / "replay.sqlite3", authorities)
    state_path = tmp_path / "broker.sqlite3"
    plan = signed_plan(authorities)
    crash = CrashOnce(kind="intent", phase="after_persist")
    broker = _broker(
        state_path,
        authorities=authorities,
        ledger=ledger,
        replay=replay,
        fault_injector=crash,
    )
    with pytest.raises(SimulatedCrash):
        broker.call(
            plan,
            DailyRequest(trade_date="2026-08-04"),
            idempotency_key="plan-rebind-1",
        )
    rebound = signed_plan(
        authorities,
        claim_token="other-claim",
        nonce="other-nonce",
    )
    with sqlite3.connect(state_path) as connection:
        connection.execute(
            "UPDATE broker_session SET plan_json = ? WHERE claim_token = ?",
            (rebound.model_dump_json(), plan.claim_token),
        )

    with pytest.raises(SourceBrokerError, match="persisted source plan|plan integrity"):
        _broker(state_path, authorities=authorities, ledger=ledger, replay=replay)
    assert ledger.effect_counts["intent"] == 0
    assert ledger.effect_counts["recover"] == 0


@pytest.mark.parametrize("idempotency_key", ["", "bad key", "../escape", "x" * 129])
def test_call_rejects_invalid_idempotency_keys_before_reserving_quota(
    tmp_path: Path,
    idempotency_key: str,
) -> None:
    authorities = create_test_authorities(tmp_path / idempotency_key.replace("/", "_") / "keys")
    ledger = IdempotentQuotaLedger(authorities)
    broker = _broker(
        tmp_path / idempotency_key.replace("/", "_") / "broker.sqlite3",
        authorities=authorities,
        ledger=ledger,
        replay=_replay(
            tmp_path / idempotency_key.replace("/", "_") / "replay.sqlite3",
            authorities,
        ),
    )

    with pytest.raises(SourceBrokerError, match="idempotency key"):
        broker.call(
            signed_plan(authorities),
            DailyRequest(trade_date="2026-08-04"),
            idempotency_key=idempotency_key,
        )
    assert ledger.effect_counts["reserve"] == 0


def test_same_idempotency_key_replays_terminal_receipt_without_new_effect(
    tmp_path: Path,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    ledger = IdempotentQuotaLedger(authorities)
    provider = EchoProvider()
    broker = _broker(
        tmp_path / "broker.sqlite3",
        authorities=authorities,
        ledger=ledger,
        replay=_replay(tmp_path / "replay.sqlite3", authorities),
        provider=provider,
    )
    plan = signed_plan(authorities)
    request = DailyRequest(trade_date="2026-08-04")

    first = broker.call(plan, request, idempotency_key="terminal-retry-1")
    second = broker.call(plan, request, idempotency_key="terminal-retry-1")

    assert second == first
    assert len(provider.calls) == 1
    assert ledger.effect_counts["intent"] == 1
    assert ledger.effect_counts["dispatch"] == 1


def test_same_idempotency_key_rejects_a_different_canonical_request(tmp_path: Path) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    ledger = IdempotentQuotaLedger(authorities)
    provider = EchoProvider()
    broker = _broker(
        tmp_path / "broker.sqlite3",
        authorities=authorities,
        ledger=ledger,
        replay=_replay(tmp_path / "replay.sqlite3", authorities),
        provider=provider,
    )
    plan = signed_plan(authorities)
    broker.call(
        plan,
        DailyRequest(trade_date="2026-08-04"),
        idempotency_key="request-bind-1",
    )

    with pytest.raises(SourceBrokerError, match="idempotency key.*different request|binding"):
        broker.call(
            plan,
            DailyRequest(trade_date="2026-08-05"),
            idempotency_key="request-bind-1",
        )
    assert len(provider.calls) == 1
    assert ledger.effect_counts["intent"] == 1


def test_concurrent_same_idempotency_key_dispatches_provider_once(tmp_path: Path) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    ledger = IdempotentQuotaLedger(authorities)
    provider = BlockingProvider()
    replay = _replay(tmp_path / "replay.sqlite3", authorities)
    state_path = tmp_path / "broker.sqlite3"
    first_broker = _broker(
        state_path,
        authorities=authorities,
        ledger=ledger,
        replay=replay,
        provider=provider,
    )
    second_broker = _broker(
        state_path,
        authorities=authorities,
        ledger=ledger,
        replay=replay,
        provider=provider,
    )
    plan = signed_plan(authorities)
    request = DailyRequest(trade_date="2026-08-04")

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(
            first_broker.call,
            plan,
            request,
            idempotency_key="concurrent-call-1",
        )
        assert provider.entered.wait(timeout=5)
        second = executor.submit(
            second_broker.call,
            plan,
            request,
            idempotency_key="concurrent-call-1",
        )
        provider.release.set()
        receipts = (first.result(timeout=5), second.result(timeout=5))

    assert receipts[0] == receipts[1]
    assert len(provider.calls) == 1
    assert ledger.effect_counts["intent"] == 1


def test_retry_resumes_crashed_intent_without_repeating_intent_or_provider(
    tmp_path: Path,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    ledger = IdempotentQuotaLedger(authorities)
    provider = EchoProvider()
    crash = CrashOnce(kind="intent", phase="after_effect")
    broker = _broker(
        tmp_path / "broker.sqlite3",
        authorities=authorities,
        ledger=ledger,
        replay=_replay(tmp_path / "replay.sqlite3", authorities),
        provider=provider,
        fault_injector=crash,
    )
    plan = signed_plan(authorities)
    request = DailyRequest(trade_date="2026-08-04")
    with pytest.raises(SimulatedCrash):
        broker.call(plan, request, idempotency_key="crashed-intent-retry-1")

    receipt = broker.call(plan, request, idempotency_key="crashed-intent-retry-1")

    assert receipt.outcome == "unknown_before_dispatch"
    assert provider.calls == []
    assert ledger.effect_counts["intent"] == 1


def test_timeout_retry_does_not_repeat_provider_or_intent(tmp_path: Path) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    ledger = IdempotentQuotaLedger(authorities)
    provider = TimeoutProvider()
    broker = _broker(
        tmp_path / "broker.sqlite3",
        authorities=authorities,
        ledger=ledger,
        replay=_replay(tmp_path / "replay.sqlite3", authorities),
        provider=provider,
    )
    plan = signed_plan(authorities)
    request = DailyRequest(trade_date="2026-08-04")

    first = broker.call(plan, request, idempotency_key="timeout-retry-1")
    second = broker.call(plan, request, idempotency_key="timeout-retry-1")

    assert first == second
    assert first.outcome == "failure"
    assert len(provider.calls) == 1
    assert ledger.effect_counts["intent"] == 1


def test_slow_provider_heartbeat_prevents_second_broker_takeover(tmp_path: Path) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    ledger = IdempotentQuotaLedger(authorities)
    replay = _replay(tmp_path / "replay.sqlite3", authorities)
    provider = BlockingProvider()
    clock = ManualLeaseClock()
    state_path = tmp_path / "broker.sqlite3"
    first_broker = _broker(
        state_path,
        authorities=authorities,
        ledger=ledger,
        replay=replay,
        provider=provider,
        lease_clock=clock,
    )
    second_broker = _broker(
        state_path,
        authorities=authorities,
        ledger=ledger,
        replay=replay,
        provider=provider,
        lease_clock=clock,
    )
    plan = signed_plan(authorities)
    request = DailyRequest(trade_date="2026-08-04")
    heartbeat_renewed = threading.Event()
    second_observed_live_fence = threading.Event()
    original_renew = first_broker._renew_lease
    original_acquire = second_broker._acquire_lease

    def observe_heartbeat(connection: sqlite3.Connection, guard: object) -> None:
        original_renew(connection, guard)  # type: ignore[arg-type]
        reading = clock()
        if getattr(guard, "table", None) == "broker_call" and reading.monotonic_time >= 1_006.0:
            heartbeat_renewed.set()

    def observe_live_fence(
        connection: sqlite3.Connection,
        *,
        table: str,
        row: sqlite3.Row,
    ) -> object:
        guard = original_acquire(connection, table=table, row=row)  # type: ignore[arg-type]
        if (
            table == "broker_call"
            and guard is None
            and row["lease_owner_id"] == first_broker.instance_id
            and int(row["fencing_token"]) == 1
            and float(row["lease_expires_monotonic"]) > clock().monotonic_time
        ):
            second_observed_live_fence.set()
        return guard

    first_broker._renew_lease = observe_heartbeat  # type: ignore[method-assign]
    second_broker._acquire_lease = observe_live_fence  # type: ignore[method-assign]
    executor = ThreadPoolExecutor(max_workers=2)
    first: Future[SourceCallReceipt] | None = None
    second: Future[SourceCallReceipt] | None = None
    try:
        first = executor.submit(
            first_broker.call,
            plan,
            request,
            idempotency_key="heartbeat-call-1",
        )
        assert provider.entered.wait(timeout=5)
        clock.advance(6.0)
        assert heartbeat_renewed.wait(timeout=5)
        with sqlite3.connect(state_path) as connection:
            heartbeat, lease_expires = connection.execute(
                "SELECT heartbeat_monotonic, lease_expires_monotonic FROM broker_call"
            ).fetchone()
        assert heartbeat >= 1_006.0
        assert lease_expires >= 1_011.0
        second = executor.submit(
            second_broker.call,
            plan,
            request,
            idempotency_key="heartbeat-call-1",
        )
        assert second_observed_live_fence.wait(timeout=5)
        assert not second.done()
        with sqlite3.connect(state_path) as connection:
            owner, fence = connection.execute(
                "SELECT lease_owner_id, fencing_token FROM broker_call"
            ).fetchone()
        assert owner == first_broker.instance_id
        assert fence == 1
        provider.release.set()
        receipts = first.result(timeout=5), second.result(timeout=5)
    finally:
        provider.release.set()
        for future in (first, second):
            if future is not None and not future.done():
                future.cancel()
        executor.shutdown(wait=True, cancel_futures=True)

    assert receipts[0] == receipts[1]
    assert len(provider.calls) == 1
    assert ledger.effect_counts["recover"] == 0
    with sqlite3.connect(state_path) as connection:
        owner, fence, state = connection.execute(
            "SELECT lease_owner_id, fencing_token, state FROM broker_call"
        ).fetchone()
    assert owner == first_broker.instance_id
    assert fence == 1
    assert state == "success"


def test_expired_call_lease_is_taken_over_with_a_new_fence(tmp_path: Path) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    ledger = IdempotentQuotaLedger(authorities)
    replay = _replay(tmp_path / "replay.sqlite3", authorities)
    clock = ManualLeaseClock()
    state_path = tmp_path / "broker.sqlite3"
    crash = CrashOnce(kind="intent", phase="after_effect")
    first_broker = _broker(
        state_path,
        authorities=authorities,
        ledger=ledger,
        replay=replay,
        lease_clock=clock,
        fault_injector=crash,
    )
    plan = signed_plan(authorities)
    request = DailyRequest(trade_date="2026-08-04")
    with pytest.raises(SimulatedCrash):
        first_broker.call(plan, request, idempotency_key="expired-call-1")

    second_broker = _broker(
        state_path,
        authorities=authorities,
        ledger=ledger,
        replay=replay,
        lease_clock=clock,
    )
    with ThreadPoolExecutor(max_workers=1) as executor:
        retry = executor.submit(
            second_broker.call,
            plan,
            request,
            idempotency_key="expired-call-1",
        )
        time.sleep(0.1)
        assert not retry.done()
        clock.advance(6.0)
        receipt = retry.result(timeout=5)

    with sqlite3.connect(state_path) as connection:
        owner, fence = connection.execute(
            "SELECT lease_owner_id, fencing_token FROM broker_call"
        ).fetchone()
    assert owner == second_broker.instance_id
    assert fence == 2
    assert receipt.outcome == "unknown_before_dispatch"
    assert ledger.effect_counts["intent"] == 1
    assert ledger.effect_counts["recover"] == 1


def test_stale_provider_executor_cannot_write_after_fence_takeover(tmp_path: Path) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    ledger = IdempotentQuotaLedger(authorities)
    replay = _replay(tmp_path / "replay.sqlite3", authorities)
    provider = BlockingProvider()
    clock = ManualLeaseClock()
    state_path = tmp_path / "broker.sqlite3"
    first_broker = _broker(
        state_path,
        authorities=authorities,
        ledger=ledger,
        replay=replay,
        provider=provider,
        lease_clock=clock,
        heartbeat_interval_seconds=30.0,
    )
    second_broker = _broker(
        state_path,
        authorities=authorities,
        ledger=ledger,
        replay=replay,
        provider=provider,
        lease_clock=clock,
        heartbeat_interval_seconds=30.0,
    )
    plan = signed_plan(authorities)
    request = DailyRequest(trade_date="2026-08-04")

    with ThreadPoolExecutor(max_workers=2) as executor:
        stale = executor.submit(
            first_broker.call,
            plan,
            request,
            idempotency_key="stale-fence-call-1",
        )
        assert provider.entered.wait(timeout=5)
        clock.advance(6.0)
        takeover = executor.submit(
            second_broker.call,
            plan,
            request,
            idempotency_key="stale-fence-call-1",
        )
        recovered = takeover.result(timeout=5)
        provider.release.set()
        with pytest.raises(SourceBrokerError, match="lease|fence|executor"):
            stale.result(timeout=5)

    assert recovered.outcome == "unknown"
    assert len(provider.calls) == 1
    assert ledger.effect_counts["recover"] == 1
    assert ledger.effect_counts["finalize"] == 0


def test_wall_clock_rollback_does_not_expire_same_boot_lease(tmp_path: Path) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    ledger = IdempotentQuotaLedger(authorities)
    replay = _replay(tmp_path / "replay.sqlite3", authorities)
    provider = BlockingProvider()
    clock = ManualLeaseClock()
    state_path = tmp_path / "broker.sqlite3"
    first_broker = _broker(
        state_path,
        authorities=authorities,
        ledger=ledger,
        replay=replay,
        provider=provider,
        lease_clock=clock,
        heartbeat_interval_seconds=30.0,
    )
    second_broker = _broker(
        state_path,
        authorities=authorities,
        ledger=ledger,
        replay=replay,
        provider=provider,
        lease_clock=clock,
        heartbeat_interval_seconds=30.0,
    )
    plan = signed_plan(authorities)
    request = DailyRequest(trade_date="2026-08-04")

    with ThreadPoolExecutor(max_workers=2) as executor:
        stale = executor.submit(
            first_broker.call,
            plan,
            request,
            idempotency_key="rollback-call-1",
        )
        assert provider.entered.wait(timeout=5)
        clock.roll_back_wall(20_000.0)
        takeover = executor.submit(
            second_broker.call,
            plan,
            request,
            idempotency_key="rollback-call-1",
        )
        time.sleep(0.1)
        assert not takeover.done()
        clock.advance(6.0)
        recovered = takeover.result(timeout=5)
        provider.release.set()
        with pytest.raises(SourceBrokerError, match="lease|fence|executor"):
            stale.result(timeout=5)

    assert recovered.outcome == "unknown"


def test_expired_session_lease_is_required_before_reserve_recovery(tmp_path: Path) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    ledger = IdempotentQuotaLedger(authorities)
    replay = _replay(tmp_path / "replay.sqlite3", authorities)
    clock = ManualLeaseClock()
    state_path = tmp_path / "broker.sqlite3"
    plan = signed_plan(authorities)
    crash = CrashOnce(kind="reserve", phase="after_persist")
    with pytest.raises(SimulatedCrash):
        _broker(
            state_path,
            authorities=authorities,
            ledger=ledger,
            replay=replay,
            lease_clock=clock,
            fault_injector=crash,
        ).start(plan)

    second_broker = _broker(
        state_path,
        authorities=authorities,
        ledger=ledger,
        replay=replay,
        lease_clock=clock,
    )
    assert ledger.effect_counts["reserve"] == 0
    with ThreadPoolExecutor(max_workers=1) as executor:
        retry = executor.submit(second_broker.start, plan)
        time.sleep(0.1)
        assert not retry.done()
        clock.advance(6.0)
        reservation = retry.result(timeout=5)

    assert reservation.claim_token == plan.claim_token
    assert ledger.effect_counts["reserve"] == 1


def test_plan_revalidation_and_dependent_effect_are_toctou_closed(tmp_path: Path) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    ledger = IdempotentQuotaLedger(authorities)
    replay = _replay(tmp_path / "replay.sqlite3", authorities)
    state_path = tmp_path / "broker.sqlite3"
    plan = signed_plan(authorities)
    replacement = signed_plan(authorities, nonce="replacement-nonce")
    race = PlanValidationRace(
        state_path=state_path,
        replacement_json=replacement.model_dump_json(),
    )
    broker = _broker(
        state_path,
        authorities=authorities,
        ledger=ledger,
        replay=replay,
        fault_injector=race,
    )

    with pytest.raises(SourceBrokerError, match="persisted source plan"):
        broker.start(plan)

    assert race.tamper_done.wait(timeout=5)
    if race.tamper_thread is not None:
        race.tamper_thread.join(timeout=5)
    assert sum(ledger.effect_counts.values()) == 0
