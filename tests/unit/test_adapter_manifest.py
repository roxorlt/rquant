from __future__ import annotations

import base64
import hashlib
import shutil
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from rquant.adapter_manifest import (
    ADAPTER_MANIFEST_NAMESPACE,
    BROKER_OUTBOX_NAMESPACE,
    BROKER_RECEIPT_NAMESPACE,
    BROKER_STATEMENT_NAMESPACE,
    LAB_CLAIM_FINALIZER_NAMESPACE,
    LAB_CLAIM_FINALIZER_ROOT_NAMESPACE,
    QUOTA_EFFECT_NAMESPACE,
    REPLAY_CLAIM_NAMESPACE,
    SCHEDULER_INTENT_AUTHORIZATION_NAMESPACE,
    SOURCE_USE_PLAN_NAMESPACE,
    SOURCE_USE_PLAN_V2_NAMESPACE,
    AdapterManifest,
    AdapterManifestTemporalPolicyV2,
    Ed25519ContractSigner,
    Ed25519PublicKeyRecord,
    KeyPurpose,
    PydanticModelSchema,
    SourceUsePlan,
    VerifyOnlyEd25519Keyring,
)
from rquant.runtime_contracts import RuntimeContractModel

NOW = datetime(2026, 8, 5, 4, tzinfo=UTC)


class RequestFilter(RuntimeContractModel):
    market: str


class DailyRequest(RuntimeContractModel):
    trade_date: str
    filters: RequestFilter | None = None


class DailyResponse(RuntimeContractModel):
    rows: int


class OpenSslSigningClient:
    def __init__(
        self,
        private_key: Path,
        *,
        key_purpose: KeyPurpose,
        allowed_namespaces: frozenset[str],
        public_key_fingerprint: str,
    ) -> None:
        self._private_key = private_key
        self.key_purpose = key_purpose
        self.allowed_namespaces = allowed_namespaces
        self.public_key_fingerprint = public_key_fingerprint
        self._lock = threading.Lock()

    def sign(
        self,
        *,
        key_purpose: KeyPurpose,
        namespace: str,
        payload: bytes,
    ) -> str:
        if key_purpose != self.key_purpose or namespace not in self.allowed_namespaces:
            raise ValueError("signing client purpose or namespace binding was violated")
        with self._lock:
            namespace_id = hashlib.sha256(namespace.encode("utf-8")).hexdigest()[:12]
            payload_path = self._private_key.with_suffix(f".{namespace_id}.payload")
            signature_path = self._private_key.with_suffix(f".{namespace_id}.signature")
            payload_path.write_bytes(payload)
            completed = subprocess.run(
                (
                    _openssl(),
                    "pkeyutl",
                    "-sign",
                    "-inkey",
                    str(self._private_key),
                    "-rawin",
                    "-in",
                    str(payload_path),
                    "-out",
                    str(signature_path),
                ),
                check=False,
                capture_output=True,
            )
            if completed.returncode != 0:
                raise RuntimeError(completed.stderr.decode("utf-8", errors="replace"))
            import base64

            return base64.b64encode(signature_path.read_bytes()).decode("ascii")


@dataclass(frozen=True)
class Authorities:
    manifest_active: Ed25519ContractSigner
    manifest_previous: Ed25519ContractSigner
    plan: Ed25519ContractSigner
    plan_v2: Ed25519ContractSigner
    scheduler_intent: Ed25519ContractSigner
    broker: Ed25519ContractSigner
    quota: Ed25519ContractSigner
    replay: Ed25519ContractSigner
    outbox: Ed25519ContractSigner
    finalizer_trust_root: Ed25519ContractSigner
    finalizer_runtime: Ed25519ContractSigner
    authorization_keyring: VerifyOnlyEd25519Keyring
    broker_keyring: VerifyOnlyEd25519Keyring
    quota_keyring: VerifyOnlyEd25519Keyring
    replay_keyring: VerifyOnlyEd25519Keyring
    outbox_keyring: VerifyOnlyEd25519Keyring
    finalizer_trust_root_keyring: VerifyOnlyEd25519Keyring
    finalizer_runtime_keyring: VerifyOnlyEd25519Keyring
    records: tuple[Ed25519PublicKeyRecord, ...]


def _openssl() -> str:
    executable = shutil.which("openssl")
    if executable is None:
        pytest.skip("openssl is required for Ed25519 source broker tests")
    return executable


def _key_pair(
    root: Path,
    *,
    key_id: str,
    issuer: str,
    key_purpose: KeyPurpose,
    rotation: str,
) -> tuple[Ed25519ContractSigner, Ed25519PublicKeyRecord]:
    root.mkdir(parents=True, exist_ok=True)
    private_key = root / f"{key_id}.private.pem"
    public_key = root / f"{key_id}.public.pem"
    generated = subprocess.run(
        (_openssl(), "genpkey", "-algorithm", "ED25519", "-out", str(private_key)),
        check=False,
        capture_output=True,
    )
    if generated.returncode != 0:
        raise RuntimeError(generated.stderr.decode("utf-8", errors="replace"))
    exported = subprocess.run(
        (_openssl(), "pkey", "-in", str(private_key), "-pubout", "-out", str(public_key)),
        check=False,
        capture_output=True,
    )
    if exported.returncode != 0:
        raise RuntimeError(exported.stderr.decode("utf-8", errors="replace"))
    private_key.chmod(0o600)
    record = Ed25519PublicKeyRecord(
        key_id=key_id,
        issuer=issuer,
        key_purpose=key_purpose,
        rotation=rotation,
        public_key_pem=public_key.read_bytes(),
    )
    signer = Ed25519ContractSigner(
        key_id=key_id,
        issuer=issuer,
        key_purpose=key_purpose,
        client=OpenSslSigningClient(
            private_key,
            key_purpose=key_purpose,
            allowed_namespaces=_namespaces_for_purpose(key_purpose),
            public_key_fingerprint=record.public_key_fingerprint,
        ),
    )
    return signer, record


def _namespaces_for_purpose(key_purpose: KeyPurpose) -> frozenset[str]:
    return {
        "adapter_manifest": frozenset({ADAPTER_MANIFEST_NAMESPACE}),
        "source_use_plan": frozenset({SOURCE_USE_PLAN_NAMESPACE}),
        "source_use_plan_v2": frozenset({SOURCE_USE_PLAN_V2_NAMESPACE}),
        "scheduler_intent_authorization": frozenset({SCHEDULER_INTENT_AUTHORIZATION_NAMESPACE}),
        "broker_receipt": frozenset({BROKER_RECEIPT_NAMESPACE, BROKER_STATEMENT_NAMESPACE}),
        "quota_effect": frozenset({QUOTA_EFFECT_NAMESPACE}),
        "replay_claim": frozenset({REPLAY_CLAIM_NAMESPACE}),
        "broker_outbox": frozenset({BROKER_OUTBOX_NAMESPACE}),
        "lab_claim_finalizer_root": frozenset({LAB_CLAIM_FINALIZER_ROOT_NAMESPACE}),
        "lab_claim_finalizer": frozenset({LAB_CLAIM_FINALIZER_NAMESPACE}),
    }[key_purpose]


def test_source_use_plan_v2_has_an_isolated_signing_namespace() -> None:
    assert SOURCE_USE_PLAN_V2_NAMESPACE == "rquant-source-use-plan/v2"
    assert _namespaces_for_purpose("source_use_plan_v2") == frozenset(
        {SOURCE_USE_PLAN_V2_NAMESPACE}
    )


def create_test_authorities(root: Path) -> Authorities:
    manifest_active, manifest_active_record = _key_pair(
        root,
        key_id="manifest-v2",
        issuer="release-authority",
        key_purpose="adapter_manifest",
        rotation="active",
    )
    manifest_previous, manifest_previous_record = _key_pair(
        root,
        key_id="manifest-v1",
        issuer="release-authority",
        key_purpose="adapter_manifest",
        rotation="previous",
    )
    plan, plan_record = _key_pair(
        root,
        key_id="plan-v1",
        issuer="lab-plan-authority",
        key_purpose="source_use_plan",
        rotation="active",
    )
    plan_v2, plan_v2_record = _key_pair(
        root,
        key_id="plan-v2",
        issuer="lab-plan-authority",
        key_purpose="source_use_plan_v2",
        rotation="active",
    )
    scheduler_intent, scheduler_intent_record = _key_pair(
        root,
        key_id="scheduler-intent-v1",
        issuer="lab-intent-authority",
        key_purpose="scheduler_intent_authorization",
        rotation="active",
    )
    broker, broker_record = _key_pair(
        root,
        key_id="broker-v1",
        issuer="lab-broker-a",
        key_purpose="broker_receipt",
        rotation="active",
    )
    quota, quota_record = _key_pair(
        root,
        key_id="quota-v1",
        issuer="quota-ledger",
        key_purpose="quota_effect",
        rotation="active",
    )
    replay, replay_record = _key_pair(
        root,
        key_id="replay-v1",
        issuer="global-source-use",
        key_purpose="replay_claim",
        rotation="active",
    )
    outbox, outbox_record = _key_pair(
        root,
        key_id="outbox-v1",
        issuer="lab-broker-a",
        key_purpose="broker_outbox",
        rotation="active",
    )
    finalizer_trust_root, finalizer_trust_root_record = _key_pair(
        root,
        key_id="finalizer-trust-root-v1",
        issuer="lab-finalizer-offline-root",
        key_purpose="lab_claim_finalizer_root",
        rotation="active",
    )
    finalizer_runtime, finalizer_runtime_record = _key_pair(
        root,
        key_id="finalizer-runtime-v1",
        issuer="lab-finalizer-runtime",
        key_purpose="lab_claim_finalizer",
        rotation="active",
    )
    records = (
        manifest_active_record,
        manifest_previous_record,
        plan_record,
        plan_v2_record,
        scheduler_intent_record,
        broker_record,
        quota_record,
        replay_record,
        outbox_record,
        finalizer_trust_root_record,
        finalizer_runtime_record,
    )
    authorization_keyring = VerifyOnlyEd25519Keyring(
        records=records,
        issuer_allowlist={
            "adapter_manifest": frozenset({"release-authority"}),
            "source_use_plan": frozenset({"lab-plan-authority"}),
            "source_use_plan_v2": frozenset({"lab-plan-authority"}),
            "scheduler_intent_authorization": frozenset({"lab-intent-authority"}),
        },
        rotation_allowlist={
            ("release-authority", "adapter_manifest"): frozenset({"manifest-v1", "manifest-v2"}),
            ("lab-plan-authority", "source_use_plan"): frozenset({"plan-v1"}),
            ("lab-plan-authority", "source_use_plan_v2"): frozenset({"plan-v2"}),
            ("lab-intent-authority", "scheduler_intent_authorization"): frozenset(
                {"scheduler-intent-v1"}
            ),
        },
    )
    broker_keyring = VerifyOnlyEd25519Keyring(
        records=records,
        issuer_allowlist={"broker_receipt": frozenset({"lab-broker-a"})},
        rotation_allowlist={("lab-broker-a", "broker_receipt"): frozenset({"broker-v1"})},
    )
    quota_keyring = VerifyOnlyEd25519Keyring(
        records=records,
        issuer_allowlist={"quota_effect": frozenset({"quota-ledger"})},
        rotation_allowlist={("quota-ledger", "quota_effect"): frozenset({"quota-v1"})},
    )
    replay_keyring = VerifyOnlyEd25519Keyring(
        records=records,
        issuer_allowlist={"replay_claim": frozenset({"global-source-use"})},
        rotation_allowlist={("global-source-use", "replay_claim"): frozenset({"replay-v1"})},
    )
    outbox_keyring = VerifyOnlyEd25519Keyring(
        records=records,
        issuer_allowlist={"broker_outbox": frozenset({"lab-broker-a"})},
        rotation_allowlist={("lab-broker-a", "broker_outbox"): frozenset({"outbox-v1"})},
    )
    finalizer_trust_root_keyring = VerifyOnlyEd25519Keyring(
        records=records,
        issuer_allowlist={"lab_claim_finalizer_root": frozenset({"lab-finalizer-offline-root"})},
        rotation_allowlist={
            ("lab-finalizer-offline-root", "lab_claim_finalizer_root"): frozenset(
                {"finalizer-trust-root-v1"}
            )
        },
    )
    finalizer_runtime_keyring = VerifyOnlyEd25519Keyring(
        records=records,
        issuer_allowlist={"lab_claim_finalizer": frozenset({"lab-finalizer-runtime"})},
        rotation_allowlist={
            ("lab-finalizer-runtime", "lab_claim_finalizer"): frozenset({"finalizer-runtime-v1"})
        },
    )
    return Authorities(
        manifest_active=manifest_active,
        manifest_previous=manifest_previous,
        plan=plan,
        plan_v2=plan_v2,
        scheduler_intent=scheduler_intent,
        broker=broker,
        quota=quota,
        replay=replay,
        outbox=outbox,
        finalizer_trust_root=finalizer_trust_root,
        finalizer_runtime=finalizer_runtime,
        authorization_keyring=authorization_keyring,
        broker_keyring=broker_keyring,
        quota_keyring=quota_keyring,
        replay_keyring=replay_keyring,
        outbox_keyring=outbox_keyring,
        finalizer_trust_root_keyring=finalizer_trust_root_keyring,
        finalizer_runtime_keyring=finalizer_runtime_keyring,
        records=records,
    )


def signed_manifest(
    authorities: Authorities,
    *,
    previous: bool = False,
) -> AdapterManifest:
    signer = authorities.manifest_previous if previous else authorities.manifest_active
    unsigned = AdapterManifest(
        issuer=signer.issuer,
        key_id=signer.key_id,
        signature="",
        adapter_id="research.daily-bars",
        adapter_version="2.1.0",
        adapter_code_hash="a" * 64,
        network="provider",
        source="tushare",
        operation="daily_bars",
        cost_per_call=2,
        max_calls=2,
        request_schema=PydanticModelSchema.from_model(DailyRequest),
        response_schema=PydanticModelSchema.from_model(DailyResponse),
        temporal_policy=AdapterManifestTemporalPolicyV2(
            valid_from=NOW - timedelta(days=1),
            expires_at=NOW + timedelta(days=30),
            availability_lag_seconds=0,
        ),
    )
    return unsigned.model_copy(
        update={
            "signature": signer.sign(
                namespace=ADAPTER_MANIFEST_NAMESPACE,
                payload=unsigned.signing_bytes(),
            )
        }
    )


def signed_plan(
    authorities: Authorities,
    *,
    claim_token: str = "claim-123",
    nonce: str = "nonce-123",
    audience: str = "lab-broker-a",
    authority_id: str = "global-source-use",
    manifest: AdapterManifest | None = None,
    not_before: datetime = NOW - timedelta(minutes=1),
    expires_at: datetime = NOW + timedelta(minutes=10),
) -> SourceUsePlan:
    unsigned = SourceUsePlan.from_manifest(
        manifest or signed_manifest(authorities),
        issuer=authorities.plan.issuer,
        key_id=authorities.plan.key_id,
        claim_token=claim_token,
        audience=audience,
        not_before=not_before,
        expires_at=expires_at,
        nonce=nonce,
        single_use_authority_id=authority_id,
    )
    return unsigned.model_copy(
        update={
            "signature": authorities.plan.sign(
                namespace=SOURCE_USE_PLAN_NAMESPACE,
                payload=unsigned.signing_bytes(),
            )
        }
    )


def _manifest_verify_request(
    manifest: AdapterManifest,
) -> tuple[str, str, KeyPurpose, str, bytes, str]:
    return (
        manifest.issuer,
        manifest.key_id,
        manifest.key_purpose,
        ADAPTER_MANIFEST_NAMESPACE,
        manifest.signing_bytes(),
        manifest.signature,
    )


def _signature_text(seed: int) -> str:
    return base64.b64encode(bytes([seed % 256]) * 64).decode("ascii")


def test_authorization_keyring_is_verify_only_and_enforces_rotation_allowlist(
    tmp_path: Path,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    active = signed_manifest(authorities)
    previous = signed_manifest(authorities, previous=True)

    assert not hasattr(authorities.authorization_keyring, "sign")
    assert active.verify(authorities.authorization_keyring)
    assert previous.verify(authorities.authorization_keyring)

    active_only = VerifyOnlyEd25519Keyring(
        records=authorities.records,
        issuer_allowlist={"adapter_manifest": frozenset({"release-authority"})},
        rotation_allowlist={("release-authority", "adapter_manifest"): frozenset({"manifest-v2"})},
    )
    assert not previous.verify(active_only)


def test_issuer_purpose_and_content_forgery_fail_verification(tmp_path: Path) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    manifest = signed_manifest(authorities)

    forged_content = manifest.model_copy(update={"adapter_code_hash": "b" * 64})
    wrong_issuer = manifest.model_copy(update={"issuer": "other-release-authority"})
    wrong_purpose = manifest.model_copy(update={"key_purpose": "source_use_plan"})

    assert not forged_content.verify(authorities.authorization_keyring)
    assert not wrong_issuer.verify(authorities.authorization_keyring)
    assert not wrong_purpose.verify(authorities.authorization_keyring)


def test_source_plan_binds_manifest_audience_time_nonce_and_single_use_authority(
    tmp_path: Path,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    plan = signed_plan(authorities)

    assert plan.verify(authorities.authorization_keyring)
    assert plan.manifest.verify(authorities.authorization_keyring)
    assert plan.audience == "lab-broker-a"
    assert plan.nonce == "nonce-123"
    assert plan.single_use_authority_id == "global-source-use"

    with pytest.raises(ValidationError, match="expires_at"):
        SourceUsePlan.model_validate(
            {
                **plan.model_dump(mode="python"),
                "not_before": NOW,
                "expires_at": NOW - timedelta(seconds=1),
            }
        )


def test_signing_client_rejects_namespace_for_a_different_purpose(tmp_path: Path) -> None:
    authorities = create_test_authorities(tmp_path / "keys")

    with pytest.raises(ValueError, match="namespace|purpose"):
        authorities.plan.sign(
            namespace=ADAPTER_MANIFEST_NAMESPACE,
            payload=b"purpose-misuse",
        )


def test_keyring_rejects_public_key_fingerprint_reused_by_rotation_role(
    tmp_path: Path,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    overlapping_record = Ed25519PublicKeyRecord(
        key_id="manifest-overlap-v1",
        issuer="release-authority",
        key_purpose="adapter_manifest",
        rotation="previous",
        public_key_pem=(tmp_path / "keys" / "plan-v1.public.pem").read_bytes(),
    )

    with pytest.raises(ValueError, match="fingerprint|role"):
        VerifyOnlyEd25519Keyring(
            records=authorities.records + (overlapping_record,),
            issuer_allowlist={
                "adapter_manifest": frozenset({"release-authority"}),
                "source_use_plan": frozenset({"lab-plan-authority"}),
            },
            rotation_allowlist={
                ("release-authority", "adapter_manifest"): frozenset(
                    {"manifest-v1", "manifest-v2", "manifest-overlap-v1"}
                ),
                ("lab-plan-authority", "source_use_plan"): frozenset({"plan-v1"}),
            },
        )


def test_verify_only_keyring_caches_successful_verifications_per_instance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    manifest = signed_manifest(authorities)
    calls: list[tuple[bytes, bytes, str]] = []

    def fake_verify_signature(*, public_key: bytes, payload: bytes, signature: str) -> bool:
        calls.append((public_key, payload, signature))
        return True

    monkeypatch.setattr("rquant.adapter_manifest._verify_signature", fake_verify_signature)

    assert manifest.verify(authorities.authorization_keyring)
    assert manifest.verify(authorities.authorization_keyring)
    assert len(calls) == 1


def test_verify_only_keyring_rechecks_purpose_key_and_policy_before_cache_hits(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    manifest = signed_manifest(authorities)
    calls = 0

    def fake_verify_signature(*, public_key: bytes, payload: bytes, signature: str) -> bool:
        nonlocal calls
        calls += 1
        return True

    monkeypatch.setattr("rquant.adapter_manifest._verify_signature", fake_verify_signature)

    assert manifest.verify(authorities.authorization_keyring)

    issuer, key_id, _, namespace, payload, signature = _manifest_verify_request(manifest)
    assert not authorities.authorization_keyring.verify(
        issuer=issuer,
        key_id=key_id,
        key_purpose="source_use_plan",
        namespace=namespace,
        payload=payload,
        signature=signature,
    )
    assert not authorities.authorization_keyring.verify(
        issuer=issuer,
        key_id="missing-key",
        key_purpose="adapter_manifest",
        namespace=namespace,
        payload=payload,
        signature=signature,
    )
    revoked_keyring = VerifyOnlyEd25519Keyring(
        records=authorities.records,
        issuer_allowlist={"adapter_manifest": frozenset({"release-authority"})},
        rotation_allowlist={("release-authority", "adapter_manifest"): frozenset()},
    )
    assert not manifest.verify(revoked_keyring)
    assert calls == 1


def test_verify_only_keyring_policy_mappings_are_immutable(
    tmp_path: Path,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    keyring = authorities.authorization_keyring

    with pytest.raises(TypeError):
        keyring._records["manifest-v2"] = authorities.records[0]
    with pytest.raises(TypeError):
        keyring._issuer_allowlist["adapter_manifest"] = frozenset()
    with pytest.raises(TypeError):
        keyring._rotation_allowlist[("release-authority", "adapter_manifest")] = frozenset()


def test_verify_only_keyring_issuer_revocation_uses_new_instance_and_reverifies(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    manifest = signed_manifest(authorities)
    calls = 0

    def fake_verify_signature(*, public_key: bytes, payload: bytes, signature: str) -> bool:
        nonlocal calls
        calls += 1
        return True

    monkeypatch.setattr("rquant.adapter_manifest._verify_signature", fake_verify_signature)

    assert manifest.verify(authorities.authorization_keyring)

    revoked_keyring = VerifyOnlyEd25519Keyring(
        records=authorities.records,
        issuer_allowlist={"adapter_manifest": frozenset()},
        rotation_allowlist={
            ("release-authority", "adapter_manifest"): frozenset({"manifest-v1", "manifest-v2"})
        },
    )
    restored_keyring = VerifyOnlyEd25519Keyring(
        records=authorities.records,
        issuer_allowlist={"adapter_manifest": frozenset({"release-authority"})},
        rotation_allowlist={
            ("release-authority", "adapter_manifest"): frozenset({"manifest-v1", "manifest-v2"})
        },
    )

    assert not manifest.verify(revoked_keyring)
    assert manifest.verify(restored_keyring)
    assert calls == 2


def test_verify_only_keyring_rotation_revocation_uses_new_instance_and_reverifies(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    manifest = signed_manifest(authorities)
    calls = 0

    def fake_verify_signature(*, public_key: bytes, payload: bytes, signature: str) -> bool:
        nonlocal calls
        calls += 1
        return True

    monkeypatch.setattr("rquant.adapter_manifest._verify_signature", fake_verify_signature)

    assert manifest.verify(authorities.authorization_keyring)

    revoked_keyring = VerifyOnlyEd25519Keyring(
        records=authorities.records,
        issuer_allowlist={"adapter_manifest": frozenset({"release-authority"})},
        rotation_allowlist={("release-authority", "adapter_manifest"): frozenset()},
    )
    restored_keyring = VerifyOnlyEd25519Keyring(
        records=authorities.records,
        issuer_allowlist={"adapter_manifest": frozenset({"release-authority"})},
        rotation_allowlist={
            ("release-authority", "adapter_manifest"): frozenset({"manifest-v1", "manifest-v2"})
        },
    )

    assert not manifest.verify(revoked_keyring)
    assert manifest.verify(restored_keyring)
    assert calls == 2


def test_verify_only_keyring_cache_key_uses_full_decoded_signature_bytes(
    tmp_path: Path,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    manifest = signed_manifest(authorities)

    verification = authorities.authorization_keyring._verification_state(
        issuer=manifest.issuer,
        key_id=manifest.key_id,
        key_purpose=manifest.key_purpose,
        namespace=ADAPTER_MANIFEST_NAMESPACE,
        payload=manifest.signing_bytes(),
        signature=manifest.signature,
    )

    assert verification is not None
    cache_key, _public_key, _domain_payload = verification
    assert cache_key.signature_bytes == base64.b64decode(manifest.signature, validate=True)
    assert len(cache_key.signature_bytes) == 64


def test_verify_only_keyring_rejects_invalid_base64_before_openssl_or_cache(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    manifest = signed_manifest(authorities)
    calls = 0

    def fake_verify_signature(*, public_key: bytes, payload: bytes, signature: str) -> bool:
        nonlocal calls
        calls += 1
        return True

    monkeypatch.setattr("rquant.adapter_manifest._verify_signature", fake_verify_signature)

    assert not authorities.authorization_keyring.verify(
        issuer=manifest.issuer,
        key_id=manifest.key_id,
        key_purpose=manifest.key_purpose,
        namespace=ADAPTER_MANIFEST_NAMESPACE,
        payload=manifest.signing_bytes(),
        signature="not-base64",
    )
    assert not authorities.authorization_keyring.verify(
        issuer=manifest.issuer,
        key_id=manifest.key_id,
        key_purpose=manifest.key_purpose,
        namespace=ADAPTER_MANIFEST_NAMESPACE,
        payload=manifest.signing_bytes(),
        signature=base64.b64encode(b"short").decode("ascii"),
    )
    assert calls == 0
    assert not authorities.authorization_keyring._verified_signatures


def test_verify_only_keyring_does_not_cache_bad_signatures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    manifest = signed_manifest(authorities)
    calls = 0

    def fake_verify_signature(*, public_key: bytes, payload: bytes, signature: str) -> bool:
        nonlocal calls
        calls += 1
        return False

    monkeypatch.setattr("rquant.adapter_manifest._verify_signature", fake_verify_signature)

    assert not manifest.verify(authorities.authorization_keyring)
    assert not manifest.verify(authorities.authorization_keyring)
    assert calls == 2


def test_verify_only_keyring_evicts_least_recently_used_success_entries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    record = authorities.records[0]
    calls = 0

    def fake_verify_signature(*, public_key: bytes, payload: bytes, signature: str) -> bool:
        nonlocal calls
        calls += 1
        return True

    monkeypatch.setattr("rquant.adapter_manifest._verify_signature", fake_verify_signature)

    first_signature = _signature_text(0)
    assert authorities.authorization_keyring.verify(
        issuer=record.issuer,
        key_id=record.key_id,
        key_purpose=record.key_purpose,
        namespace=ADAPTER_MANIFEST_NAMESPACE,
        payload=b"payload-0",
        signature=first_signature,
    )
    for index in range(1, 513):
        assert authorities.authorization_keyring.verify(
            issuer=record.issuer,
            key_id=record.key_id,
            key_purpose=record.key_purpose,
            namespace=ADAPTER_MANIFEST_NAMESPACE,
            payload=f"payload-{index}".encode("ascii"),
            signature=_signature_text(index),
        )
    assert calls == 513

    assert authorities.authorization_keyring.verify(
        issuer=record.issuer,
        key_id=record.key_id,
        key_purpose=record.key_purpose,
        namespace=ADAPTER_MANIFEST_NAMESPACE,
        payload=b"payload-0",
        signature=first_signature,
    )
    assert calls == 514


def test_verify_only_keyring_single_flights_concurrent_identical_requests(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    manifest = signed_manifest(authorities)
    started = threading.Event()
    release = threading.Event()
    all_waiters = threading.Event()
    gate = threading.Barrier(8)
    calls = 0

    class _WaitCountingEvent(threading.Event):
        def wait(self, timeout: float | None = None) -> bool:
            with wait_lock:
                waiters[0] += 1
                if waiters[0] == 7:
                    all_waiters.set()
            return super().wait(timeout)

    class _CountingFlight:
        def __init__(self) -> None:
            self.ready = _WaitCountingEvent()
            self.result: bool | None = None
            self.error: BaseException | None = None

    wait_lock = threading.Lock()
    waiters = [0]

    def fake_verify_signature(*, public_key: bytes, payload: bytes, signature: str) -> bool:
        nonlocal calls
        calls += 1
        started.set()
        if not release.wait(timeout=5):
            raise TimeoutError("verification did not release")
        return True

    monkeypatch.setattr("rquant.adapter_manifest._VerificationFlight", _CountingFlight)
    monkeypatch.setattr("rquant.adapter_manifest._verify_signature", fake_verify_signature)

    def worker() -> bool:
        gate.wait(timeout=5)
        return manifest.verify(authorities.authorization_keyring)

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(worker) for _ in range(8)]
        assert started.wait(timeout=5)
        assert all_waiters.wait(timeout=5)
        assert calls == 1
        release.set()
        assert all(future.result(timeout=5) for future in futures)
    assert calls == 1


def test_verify_only_keyring_cache_is_isolated_per_instance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    manifest = signed_manifest(authorities)
    calls = 0

    def fake_verify_signature(*, public_key: bytes, payload: bytes, signature: str) -> bool:
        nonlocal calls
        calls += 1
        return True

    monkeypatch.setattr("rquant.adapter_manifest._verify_signature", fake_verify_signature)

    second_keyring = VerifyOnlyEd25519Keyring(
        records=authorities.records,
        issuer_allowlist={
            "adapter_manifest": frozenset({"release-authority"}),
            "source_use_plan": frozenset({"lab-plan-authority"}),
            "source_use_plan_v2": frozenset({"lab-plan-authority"}),
        },
        rotation_allowlist={
            ("release-authority", "adapter_manifest"): frozenset({"manifest-v1", "manifest-v2"}),
            ("lab-plan-authority", "source_use_plan"): frozenset({"plan-v1"}),
            ("lab-plan-authority", "source_use_plan_v2"): frozenset({"plan-v2"}),
        },
    )

    assert manifest.verify(authorities.authorization_keyring)
    assert manifest.verify(authorities.authorization_keyring)
    assert manifest.verify(second_keyring)
    assert calls == 2


def test_verify_only_keyring_clears_single_flight_state_after_exceptions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    manifest = signed_manifest(authorities)
    calls = 0
    should_raise = True

    def fake_verify_signature(*, public_key: bytes, payload: bytes, signature: str) -> bool:
        nonlocal calls, should_raise
        calls += 1
        if should_raise:
            should_raise = False
            raise RuntimeError("openssl unavailable")
        return True

    monkeypatch.setattr("rquant.adapter_manifest._verify_signature", fake_verify_signature)

    with pytest.raises(RuntimeError, match="openssl unavailable"):
        manifest.verify(authorities.authorization_keyring)
    assert manifest.verify(authorities.authorization_keyring)
    assert calls == 2


def test_verify_only_keyring_shares_exception_semantics_with_waiters_and_allows_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    manifest = signed_manifest(authorities)
    started = threading.Event()
    release = threading.Event()
    all_waiters = threading.Event()
    gate = threading.Barrier(6)
    calls = 0
    should_raise = True

    class _WaitCountingEvent(threading.Event):
        def wait(self, timeout: float | None = None) -> bool:
            with wait_lock:
                waiters[0] += 1
                if waiters[0] == 5:
                    all_waiters.set()
            return super().wait(timeout)

    class _CountingFlight:
        def __init__(self) -> None:
            self.ready = _WaitCountingEvent()
            self.result: bool | None = None
            self.error: BaseException | None = None

    wait_lock = threading.Lock()
    waiters = [0]

    def fake_verify_signature(*, public_key: bytes, payload: bytes, signature: str) -> bool:
        nonlocal calls, should_raise
        calls += 1
        started.set()
        if not release.wait(timeout=5):
            raise TimeoutError("verification did not release")
        if should_raise:
            should_raise = False
            raise RuntimeError("openssl unavailable")
        return True

    monkeypatch.setattr("rquant.adapter_manifest._VerificationFlight", _CountingFlight)
    monkeypatch.setattr("rquant.adapter_manifest._verify_signature", fake_verify_signature)

    def worker() -> bool:
        gate.wait(timeout=5)
        return manifest.verify(authorities.authorization_keyring)

    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = [executor.submit(worker) for _ in range(6)]
        assert started.wait(timeout=5)
        assert all_waiters.wait(timeout=5)
        assert calls == 1
        release.set()
        for future in futures:
            with pytest.raises(RuntimeError, match="openssl unavailable"):
                future.result(timeout=5)

    release.clear()
    started.clear()
    release.set()
    assert manifest.verify(authorities.authorization_keyring)
    assert calls == 2
