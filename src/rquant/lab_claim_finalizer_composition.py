"""Production-only composition for the narrow claim publication finalizer."""

from __future__ import annotations

import base64
import os
import stat
import subprocess
import tempfile
import threading
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, Protocol

from pydantic import Field, model_validator

from rquant.adapter_manifest import (
    LAB_CLAIM_FINALIZER_NAMESPACE,
    SOURCE_USE_PLAN_V2_NAMESPACE,
    Ed25519ContractSigner,
    Ed25519PublicKeyRecord,
    KeyPurpose,
    VerifyOnlyEd25519Keyring,
    ed25519_public_key_fingerprint,
)
from rquant.authority_path_security import (
    AuthorityPathSecurityError,
    read_secure_regular_file,
)
from rquant.current_claim_authority import (
    ExternalCurrentClaimMonotonicRootAdapter,
    ExternalCurrentClaimRootConfig,
    compose_production_current_claim_authority,
)
from rquant.external_monotonic_root import (
    UnixSocketExternalMonotonicRootClient,
    UnixSocketExternalMonotonicRootManifest,
)
from rquant.external_monotonic_root_service import ClosedExternalMonotonicRootVerifier
from rquant.lab_claim_finalizer import LabClaimPublicationFinalizerAuthorityIssuer
from rquant.lab_claim_finalizer_daemon import LabClaimFinalizerDaemon
from rquant.lab_claim_finalizer_trust import (
    LabClaimFinalizerTrustCertificate,
    LabClaimFinalizerTrustVerifier,
)
from rquant.lab_claim_publication import (
    LabClaimPublicationFinalizerRootKey,
    LabClaimSpoolReceiptVerifier,
)
from rquant.lab_daemon import LabDaemonConfigurationError
from rquant.lab_jobs import LabJobStore
from rquant.lab_shard_protocol import LabClaimSpool
from rquant.lab_source_stage import LabSourceStageStore
from rquant.runtime_contracts import RuntimeContractModel
from rquant.source_broker_v2_job_protocol import SourceBrokerV2AuthorityRef
from rquant.strict_json import strict_model_validate_canonical_json


class _ClaimFinalizerSettings(Protocol):
    lab_v2_claim_publication_enabled: bool
    lab_claim_finalizer_runtime_material_path: Path | None
    lab_claim_finalizer_runtime_material_root: Path | None
    lab_claim_finalizer_runtime_trusted_base: Path
    lab_claim_finalizer_owner_id: str
    lab_claim_finalizer_lease_seconds: int
    lab_claim_finalizer_poll_interval_ms: int
    lab_claim_finalizer_max_publications_per_tick: int
    lab_claim_finalizer_failure_backoff_seconds: int
    lab_claim_finalizer_failure_backoff_max_seconds: int
    lab_jobs_path_resolved: Path
    lab_jobs_busy_timeout_ms: int
    lab_job_claim_dir_resolved: Path


class LabClaimFinalizerRuntimeMaterial(RuntimeContractModel):
    """Canonical references to the finalizer's private and verify-only material."""

    schema_version: Literal[1] = 1
    contract: Literal["rquant-lab-claim-finalizer-runtime-material/v1"] = (
        "rquant-lab-claim-finalizer-runtime-material/v1"
    )
    audience: str = Field(min_length=1, max_length=200)
    trust_certificate: LabClaimFinalizerTrustCertificate
    root_public_keys: tuple[Ed25519PublicKeyRecord, ...] = Field(min_length=1)
    finalizer_public_keys: tuple[Ed25519PublicKeyRecord, ...] = Field(min_length=1)
    adapter_manifest_public_keys: tuple[Ed25519PublicKeyRecord, ...] = Field(min_length=1)
    scheduler_intent_public_keys: tuple[Ed25519PublicKeyRecord, ...] = Field(min_length=1)
    source_plan_public_keys: tuple[Ed25519PublicKeyRecord, ...] = Field(min_length=1)
    finalizer_runtime_private_key_path: Path
    finalizer_root_secret_path: Path
    source_stage_path: Path
    source_queue_path: Path
    spool_receipt_publisher: SourceBrokerV2AuthorityRef
    current_claim_state_path: Path
    current_claim_authority_id: str = Field(min_length=1, max_length=200)
    current_claim_plan_private_key_path: Path
    current_claim_external_root_manifest: UnixSocketExternalMonotonicRootManifest
    current_claim_external_root_config: ExternalCurrentClaimRootConfig
    current_claim_external_root_public_key_path: Path

    @model_validator(mode="after")
    def validate_key_roles(self) -> LabClaimFinalizerRuntimeMaterial:
        expected = (
            (self.root_public_keys, "lab_claim_finalizer_root"),
            (self.finalizer_public_keys, "lab_claim_finalizer"),
            (self.adapter_manifest_public_keys, "adapter_manifest"),
            (self.scheduler_intent_public_keys, "scheduler_intent_authorization"),
            (self.source_plan_public_keys, "source_use_plan_v2"),
        )
        if any(
            record.key_purpose != purpose for records, purpose in expected for record in records
        ):
            raise ValueError("claim finalizer runtime public key role is invalid")
        active_plan = tuple(
            record for record in self.source_plan_public_keys if record.rotation == "active"
        )
        if len(active_plan) != 1:
            raise ValueError("claim finalizer requires one active source plan key")
        return self


class _OpenSslFileSigningClient:
    """Purpose-bound Ed25519 client; private bytes never enter a model or log."""

    def __init__(
        self,
        *,
        private_key_path: Path,
        public_record: Ed25519PublicKeyRecord,
        allowed_namespaces: frozenset[str],
    ) -> None:
        self._private_key_path = _require_private_file(
            private_key_path,
            label="claim finalizer signing key",
            maximum_bytes=16_384,
        )[0]
        self.key_purpose = public_record.key_purpose
        self.allowed_namespaces = allowed_namespaces
        self.public_key_fingerprint = public_record.public_key_fingerprint
        self._lock = threading.Lock()
        _require_private_key_matches_record(self._private_key_path, public_record)

    def sign(
        self,
        *,
        key_purpose: KeyPurpose,
        namespace: str,
        payload: bytes,
    ) -> str:
        if key_purpose != self.key_purpose or namespace not in self.allowed_namespaces:
            raise ValueError("claim finalizer signing boundary changed")
        with self._lock, tempfile.TemporaryDirectory(prefix="rquant-claim-finalizer-sign-") as name:
            root = Path(name)
            payload_path = root / "payload.bin"
            signature_path = root / "signature.bin"
            payload_path.write_bytes(payload)
            payload_path.chmod(0o600)
            completed = subprocess.run(
                (
                    "openssl",
                    "pkeyutl",
                    "-sign",
                    "-inkey",
                    str(self._private_key_path),
                    "-rawin",
                    "-in",
                    str(payload_path),
                    "-out",
                    str(signature_path),
                ),
                check=False,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=5,
            )
            if completed.returncode != 0:
                raise ValueError("claim finalizer signing failed")
            return base64.b64encode(signature_path.read_bytes()).decode("ascii")


def _require_private_file(path: Path, *, label: str, maximum_bytes: int) -> tuple[Path, bytes]:
    candidate = Path(path)
    if not candidate.is_absolute() or candidate != Path(os.path.abspath(candidate)):
        raise LabDaemonConfigurationError(f"{label} path is invalid")
    before = candidate.lstat()
    if (
        not stat.S_ISREG(before.st_mode)
        or stat.S_IMODE(before.st_mode) & 0o077
        or before.st_uid != os.geteuid()
        or before.st_size < 1
        or before.st_size > maximum_bytes
    ):
        raise LabDaemonConfigurationError(f"{label} is unavailable or insecure")
    try:
        payload = read_secure_regular_file(
            candidate,
            trusted_root=Path("/"),
            expected_uid=os.geteuid(),
            expected_gid=os.getegid(),
            allowed_final_uids=frozenset({os.geteuid()}),
            allowed_final_gids=frozenset({os.getegid()}),
            allowed_modes=frozenset({0o600}),
            max_bytes=maximum_bytes,
        )
    except AuthorityPathSecurityError as exc:
        raise LabDaemonConfigurationError(f"{label} is unavailable or insecure") from exc
    after = candidate.lstat()
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        raise LabDaemonConfigurationError(f"{label} changed while reading")
    return candidate, payload


def _require_private_key_matches_record(
    private_key_path: Path,
    record: Ed25519PublicKeyRecord,
) -> None:
    completed = subprocess.run(
        ("openssl", "pkey", "-in", str(private_key_path), "-pubout"),
        check=False,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        timeout=5,
    )
    if (
        completed.returncode != 0
        or ed25519_public_key_fingerprint(completed.stdout) != record.public_key_fingerprint
    ):
        raise LabDaemonConfigurationError("claim finalizer private signing key does not match")


def _keyring(
    records: tuple[Ed25519PublicKeyRecord, ...],
    purpose: KeyPurpose,
) -> VerifyOnlyEd25519Keyring:
    return VerifyOnlyEd25519Keyring(
        records=records,
        issuer_allowlist={purpose: frozenset(record.issuer for record in records)},
        rotation_allowlist={
            (record.issuer, purpose): frozenset(
                item.key_id for item in records if item.issuer == record.issuer
            )
            for record in records
        },
    )


def _source_authorization_keyring(
    material: LabClaimFinalizerRuntimeMaterial,
) -> VerifyOnlyEd25519Keyring:
    grouped = (
        (material.adapter_manifest_public_keys, "adapter_manifest"),
        (material.scheduler_intent_public_keys, "scheduler_intent_authorization"),
        (material.source_plan_public_keys, "source_use_plan_v2"),
    )
    records = tuple(record for selected, _purpose in grouped for record in selected)
    return VerifyOnlyEd25519Keyring(
        records=records,
        issuer_allowlist={
            purpose: frozenset(record.issuer for record in selected)
            for selected, purpose in grouped
        },
        rotation_allowlist={
            (record.issuer, purpose): frozenset(
                item.key_id for item in selected if item.issuer == record.issuer
            )
            for selected, purpose in grouped
            for record in selected
        },
    )


def _signer(
    *,
    record: Ed25519PublicKeyRecord,
    private_key_path: Path,
    namespace: str,
) -> Ed25519ContractSigner:
    return Ed25519ContractSigner(
        key_id=record.key_id,
        issuer=record.issuer,
        key_purpose=record.key_purpose,
        client=_OpenSslFileSigningClient(
            private_key_path=private_key_path,
            public_record=record,
            allowed_namespaces=frozenset({namespace}),
        ),
    )


def _load_runtime_material(path: Path) -> LabClaimFinalizerRuntimeMaterial:
    _, raw = _require_private_file(
        path,
        label="claim finalizer private runtime material",
        maximum_bytes=1_048_576,
    )
    return strict_model_validate_canonical_json(LabClaimFinalizerRuntimeMaterial, raw)


def compose_production_lab_claim_finalizer_daemon(
    *,
    settings: _ClaimFinalizerSettings,
    mutation_guard: Callable[[], object] | None = None,
) -> LabClaimFinalizerDaemon:
    """Validate every capability before returning the narrow daemon graph."""

    runtime_root = settings.lab_claim_finalizer_runtime_material_root
    if runtime_root is None:
        raise LabDaemonConfigurationError(
            "V2 claim finalizer requires a controlled runtime material root"
        )
    from rquant.lab_claim_finalizer_runtime import (
        FinalizerRuntimeError,
        load_current_lab_claim_finalizer_generation,
    )

    try:
        selected = load_current_lab_claim_finalizer_generation(
            runtime_root,
            trusted_base=settings.lab_claim_finalizer_runtime_trusted_base,
        )
    except FinalizerRuntimeError as exc:
        raise LabDaemonConfigurationError("claim finalizer current generation is invalid") from exc
    path = selected.runtime_material_path
    try:
        material = _load_runtime_material(path)
        ledger = LabJobStore(
            settings.lab_jobs_path_resolved,
            busy_timeout_ms=settings.lab_jobs_busy_timeout_ms,
            mutation_guard=mutation_guard,
        )
        with ledger._connect() as connection:  # noqa: SLF001 - certificate store binding
            binding = ledger._finalizer_authority_binding(  # noqa: SLF001
                connection,
                path=ledger.path,
            )
        root_keyring = _keyring(material.root_public_keys, "lab_claim_finalizer_root")
        finalizer_keyring = _keyring(material.finalizer_public_keys, "lab_claim_finalizer")
        source_plan_keyring = _source_authorization_keyring(material)
        trust_verifier = LabClaimFinalizerTrustVerifier(
            root_keyring=root_keyring,
            finalizer_keyring=finalizer_keyring,
        )
        trust_verifier.require_certificate(
            material.trust_certificate,
            store_id=str(binding["store_id"]),
            database_generation=binding["database_generation"],
            schema_version=int(binding["schema_version"]),
            now=datetime.now(UTC),
        )
        finalizer_record = next(
            record
            for record in material.finalizer_public_keys
            if record.key_id == material.trust_certificate.finalizer_key_id
            and record.issuer == material.trust_certificate.finalizer_issuer
        )
        finalizer_signer = _signer(
            record=finalizer_record,
            private_key_path=material.finalizer_runtime_private_key_path,
            namespace=LAB_CLAIM_FINALIZER_NAMESPACE,
        )
        trust_verifier.require_runtime_signer(material.trust_certificate, finalizer_signer)

        plan_record = next(
            record for record in material.source_plan_public_keys if record.rotation == "active"
        )
        plan_signer = _signer(
            record=plan_record,
            private_key_path=material.current_claim_plan_private_key_path,
            namespace=SOURCE_USE_PLAN_V2_NAMESPACE,
        )
        root_verifier = ClosedExternalMonotonicRootVerifier(
            public_key_path=material.current_claim_external_root_public_key_path,
            issuer=material.current_claim_external_root_config.root_issuer,
            key_id=material.current_claim_external_root_config.root_key_id,
            key_purpose=material.current_claim_external_root_config.root_key_purpose,
        )
        external_root = ExternalCurrentClaimMonotonicRootAdapter(
            config=material.current_claim_external_root_config,
            client=UnixSocketExternalMonotonicRootClient(
                material.current_claim_external_root_manifest
            ),
            root_verifiers=(root_verifier,),
        )
        current_claim_authority = compose_production_current_claim_authority(
            material.current_claim_state_path,
            authority_id=material.current_claim_authority_id,
            signer=plan_signer,
            keyring=source_plan_keyring,
            external_root=external_root,
            busy_timeout_ms=settings.lab_jobs_busy_timeout_ms,
        )
        _, root_secret = _require_private_file(
            material.finalizer_root_secret_path,
            label="claim finalizer root capability",
            maximum_bytes=4_096,
        )
        issuer = LabClaimPublicationFinalizerAuthorityIssuer(
            store=ledger,
            root_key=LabClaimPublicationFinalizerRootKey(secret=root_secret),
            trust_certificate=material.trust_certificate,
            trust_verifier=trust_verifier,
            runtime_signer=finalizer_signer,
        )
        stage_reader = LabSourceStageStore(
            material.source_stage_path,
            queue_store_path=material.source_queue_path,
            busy_timeout_ms=settings.lab_jobs_busy_timeout_ms,
        )
        spool = LabClaimSpool(
            settings.lab_job_claim_dir_resolved,
            mutation_guard=mutation_guard,
            publish_receipt_publisher=material.spool_receipt_publisher,
        )

        def record_published(
            *,
            attempt_id: str,
            evidence_hash: str,
            publication_identity: str,
        ) -> None:
            from rquant.lab_claim_finalizer_runtime import FinalizerRolloutStore

            FinalizerRolloutStore(
                settings.lab_finalizer_state_dir_resolved / "claim-finalizer-rollout.sqlite3",
                create=False,
            ).record_published(
                attempt_id=attempt_id,
                evidence_hash=evidence_hash,
                publication_identity=publication_identity,
            )

        evidence_recorder = record_published if settings.lab_v2_claim_publication_enabled else None

        return LabClaimFinalizerDaemon(
            ledger=ledger,
            stage_reader=stage_reader,
            authority_issuer=issuer,
            current_claim_authority=current_claim_authority,
            keyring=source_plan_keyring,
            audience=material.audience,
            spool=spool,
            spool_receipt_verifier=LabClaimSpoolReceiptVerifier.from_spool(spool),
            owner_id=settings.lab_claim_finalizer_owner_id,
            lease_seconds=settings.lab_claim_finalizer_lease_seconds,
            max_publications_per_tick=settings.lab_claim_finalizer_max_publications_per_tick,
            poll_interval_ms=settings.lab_claim_finalizer_poll_interval_ms,
            failure_backoff_seconds=settings.lab_claim_finalizer_failure_backoff_seconds,
            failure_backoff_max_seconds=settings.lab_claim_finalizer_failure_backoff_max_seconds,
            published_evidence_recorder=evidence_recorder,
        )
    except LabDaemonConfigurationError:
        raise
    except (OSError, StopIteration, subprocess.SubprocessError, TypeError, ValueError) as exc:
        raise LabDaemonConfigurationError("claim finalizer runtime material is invalid") from exc


__all__ = [
    "LabClaimFinalizerRuntimeMaterial",
    "compose_production_lab_claim_finalizer_daemon",
]
