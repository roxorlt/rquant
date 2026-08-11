"""Production-only authority and external-root composition for SourceBroker v2."""

from __future__ import annotations

import base64
import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Never, Protocol

from pydantic import ConfigDict, Field, ValidationError

from rquant.adapter_manifest import (
    REPLAY_CLAIM_NAMESPACE,
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
    secure_create_regular_file,
    secure_path_metadata,
)
from rquant.current_claim_authority import (
    CurrentClaimCheckpoint,
    CurrentClaimExternalRootReceipt,
    ExternalCurrentClaimMonotonicRootAdapter,
    ExternalCurrentClaimRootConfig,
    compose_production_current_claim_authority,
)
from rquant.external_monotonic_root import (
    EXTERNAL_MONOTONIC_ROOT_ZERO_HASH,
    ExternalMonotonicRootConfig,
    ExternalMonotonicRootReceiptIdentity,
    ExternalMonotonicRootRequest,
    ExternalMonotonicRootSecurityError,
    ExternalMonotonicRootTrustBoundary,
    UnixSocketExternalMonotonicRootClient,
    UnixSocketExternalMonotonicRootManifest,
)
from rquant.external_monotonic_root_service import (
    EXTERNAL_ROOT_SERVICE_PROBE_NAMESPACE,
    ClosedExternalMonotonicRootVerifier,
    ExternalMonotonicRootUnixService,
    ExternalRootServiceConfiguration,
    ExternalRootStoredState,
    OpenSslExternalMonotonicRootSigner,
    PersistentExternalMonotonicRootBackend,
    probe_external_monotonic_root_service,
)
from rquant.replay_lineage_authority import (
    AntiRollbackRoot,
    MonotonicCheckpointStore,
    SignedReplayLineageCheckpoint,
    compose_production_replay_lineage_authority,
)
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.source_broker_protocol import (
    PeerCredentialsPolicy,
    ServerCredentialsPolicy,
    SocketEndpointPolicy,
)
from rquant.source_broker_v2 import (
    SOURCE_BROKER_V2_AUTHORITY_PURPOSE,
    SourceAuthorityKeyring,
    SourceBrokerV2UnixClient,
)
from rquant.source_broker_v2_authority_service import (
    SourceBrokerV2AuthorityUnixService,
    SourceBrokerV2CurrentClaimUnixClient,
    SourceBrokerV2ReplayLineageUnixClient,
    SourceBrokerV2SourceQuotaUnixClient,
)
from rquant.source_broker_v2_runtime import (
    SourceBrokerV2AuthorityRuntime,
    SourceBrokerV2AuthorityRuntimePath,
    SourceBrokerV2ExternalRootRuntime,
    SourceBrokerV2ProcessIdentity,
    SourceBrokerV2ProcessRole,
    SourceBrokerV2RootRole,
)
from rquant.source_broker_v2_service import OpenSslSourceBrokerV2AuthoritySigner
from rquant.source_quota_authority import SourceQuotaParentAuthority
from rquant.source_quota_external_root_adapter import (
    SOURCE_QUOTA_ROOT_RECEIPT_NAMESPACE,
    SourceQuotaExternalRootConfig,
    SourceQuotaExternalRootRoleHandler,
)
from rquant.strict_json import (
    canonical_json_bytes,
    canonical_model_json_bytes,
    strict_model_validate_canonical_json,
)

REPLAY_LINEAGE_ROOT_KEY_PURPOSE = "replay-lineage-monotonic-root"
REPLAY_LINEAGE_ROOT_RECEIPT_NAMESPACE = "rquant-replay-lineage-anti-rollback-root/v1"


class SourceBrokerV2AuthorityCompositionError(RuntimeError):
    """The production v2 authority/root graph cannot be built safely."""


class _StrictAuthorityModel(RuntimeContractModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class SourceBrokerV2ReplayRootConfig(ExternalMonotonicRootConfig):
    role: Literal["replay_lineage_monotonic_root"] = "replay_lineage_monotonic_root"
    root_key_purpose: Literal["replay-lineage-monotonic-root"] = REPLAY_LINEAGE_ROOT_KEY_PURPOSE
    root_receipt_namespace: Literal["rquant-replay-lineage-anti-rollback-root/v1"] = (
        REPLAY_LINEAGE_ROOT_RECEIPT_NAMESPACE
    )


class SourceBrokerV2ReplayRootReceipt(_StrictAuthorityModel):
    schema_version: Literal[1] = 1
    contract: Literal["rquant-source-broker-v2-replay-root-receipt/v1"] = (
        "rquant-source-broker-v2-replay-root-receipt/v1"
    )
    role: Literal["replay_lineage_monotonic_root"] = "replay_lineage_monotonic_root"
    root_authority_id: str = Field(min_length=1, max_length=200)
    root_store_id: str = Field(min_length=1, max_length=200)
    replay_lineage_authority_id: str = Field(min_length=1, max_length=200)
    request_kind: Literal["current", "pin", "advance"]
    request_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    challenge_nonce: str = Field(pattern=r"^[0-9a-f]{64}$")
    operation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    previous_checkpoint_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    checkpoint: SignedReplayLineageCheckpoint
    closed: Literal[True] = True
    issuer: str = Field(min_length=1, max_length=200)
    key_id: str = Field(min_length=1, max_length=200)
    key_purpose: Literal["replay-lineage-monotonic-root"] = REPLAY_LINEAGE_ROOT_KEY_PURPOSE
    namespace: Literal["rquant-replay-lineage-anti-rollback-root/v1"] = (
        REPLAY_LINEAGE_ROOT_RECEIPT_NAMESPACE
    )
    signature_algorithm: Literal["ed25519"] = "ed25519"
    public_key_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    signature: str = Field(min_length=1, max_length=16_384)

    def signing_bytes(self) -> bytes:
        return canonical_json_bytes(self.model_dump(mode="json", exclude={"signature"}))


class _RootSigner(Protocol):
    issuer: str
    key_id: str
    key_purpose: str
    signature_algorithm: Literal["ed25519"]
    public_key_fingerprint: str

    def sign(self, *, namespace: str, payload: bytes) -> str: ...


class _CurrentClaimRootRoleHandler:
    def __init__(self, signer: _RootSigner) -> None:
        if signer.key_purpose != "current-claim-monotonic-root":
            raise SourceBrokerV2AuthorityCompositionError("current root signer purpose is invalid")
        self._signer = signer

    def response_json(
        self,
        request: ExternalMonotonicRootRequest,
        state: ExternalRootStoredState | None,
    ) -> str | None:
        if state is None:
            return None
        if request.role != SourceBrokerV2RootRole.CURRENT_CLAIM.value:
            raise SourceBrokerV2AuthorityCompositionError("current root role changed")
        checkpoint = strict_model_validate_canonical_json(
            CurrentClaimCheckpoint, state.checkpoint_json
        )
        if (
            checkpoint.checkpoint_hash != state.checkpoint_hash
            or checkpoint.authority_id != request.subject_authority_id
        ):
            raise SourceBrokerV2AuthorityCompositionError("current root checkpoint changed")
        unsigned = CurrentClaimExternalRootReceipt(
            schema_version=1,
            contract="rquant-current-claim-external-root-receipt/v1",
            role=request.role,
            root_authority_id=request.root_authority_id,
            root_store_id=request.root_store_id,
            current_claim_authority_id=request.subject_authority_id,
            request_kind=request.kind,
            request_hash=request.request_hash,
            challenge_nonce=request.challenge_nonce,
            operation_id=state.operation_id,
            previous_checkpoint_hash=state.previous_checkpoint_hash,
            checkpoint=checkpoint,
            issuer=self._signer.issuer,
            key_id=self._signer.key_id,
            public_key_fingerprint=self._signer.public_key_fingerprint,
            signature="pending",
        )
        return canonical_model_json_bytes(
            unsigned.model_copy(
                update={
                    "signature": self._signer.sign(
                        namespace="rquant-current-claim-anti-rollback-root/v1",
                        payload=unsigned.signing_bytes(),
                    )
                }
            )
        ).decode("utf-8")


class _ReplayLineageRootRoleHandler:
    def __init__(self, signer: _RootSigner) -> None:
        if signer.key_purpose != REPLAY_LINEAGE_ROOT_KEY_PURPOSE:
            raise SourceBrokerV2AuthorityCompositionError("replay root signer purpose is invalid")
        self._signer = signer

    def response_json(
        self,
        request: ExternalMonotonicRootRequest,
        state: ExternalRootStoredState | None,
    ) -> str | None:
        if state is None:
            return None
        if request.role != SourceBrokerV2RootRole.REPLAY_LINEAGE.value:
            raise SourceBrokerV2AuthorityCompositionError("replay root role changed")
        checkpoint = strict_model_validate_canonical_json(
            SignedReplayLineageCheckpoint,
            state.checkpoint_json,
        )
        if (
            checkpoint.checkpoint_hash != state.checkpoint_hash
            or checkpoint.authority_id != request.subject_authority_id
        ):
            raise SourceBrokerV2AuthorityCompositionError("replay root checkpoint changed")
        unsigned = SourceBrokerV2ReplayRootReceipt(
            root_authority_id=request.root_authority_id,
            root_store_id=request.root_store_id,
            replay_lineage_authority_id=request.subject_authority_id,
            request_kind=request.kind,
            request_hash=request.request_hash,
            challenge_nonce=request.challenge_nonce,
            operation_id=state.operation_id,
            previous_checkpoint_hash=state.previous_checkpoint_hash,
            checkpoint=checkpoint,
            issuer=self._signer.issuer,
            key_id=self._signer.key_id,
            public_key_fingerprint=self._signer.public_key_fingerprint,
            signature="pending",
        )
        return canonical_model_json_bytes(
            unsigned.model_copy(
                update={
                    "signature": self._signer.sign(
                        namespace=REPLAY_LINEAGE_ROOT_RECEIPT_NAMESPACE,
                        payload=unsigned.signing_bytes(),
                    )
                }
            )
        ).decode("utf-8")


class SourceBrokerV2ReplayLineageExternalRoot(MonotonicCheckpointStore):
    """Closed external Unix implementation of replay lineage's root protocol."""

    def __init__(
        self,
        *,
        config: SourceBrokerV2ReplayRootConfig,
        client: UnixSocketExternalMonotonicRootClient,
        verifier: ClosedExternalMonotonicRootVerifier,
    ) -> None:
        if (
            type(config) is not SourceBrokerV2ReplayRootConfig
            or type(client) is not UnixSocketExternalMonotonicRootClient
            or type(verifier) is not ClosedExternalMonotonicRootVerifier
        ):
            raise SourceBrokerV2AuthorityCompositionError(
                "replay lineage root requires closed Unix transport and verifier"
            )
        self._config = config
        self._trust = ExternalMonotonicRootTrustBoundary(
            config=config,
            client=client,
            root_verifiers=(verifier,),
        )

    @property
    def authority_id(self) -> str:
        return self._config.root_authority_id

    @property
    def storage_path(self) -> None:
        return None

    @property
    def verifier_fingerprints(self) -> frozenset[str]:
        return frozenset({self._config.root_public_key_fingerprint})

    def current(self, *, lineage_authority_id: str) -> AntiRollbackRoot | None:
        request = self._request(kind="current", authority_id=lineage_authority_id)
        response = self._invoke(request)
        return None if response is None else self._receipt(response, request=request)

    def pin(
        self,
        *,
        operation_id: str,
        lineage_authority_id: str,
        checkpoint: SignedReplayLineageCheckpoint,
    ) -> AntiRollbackRoot:
        request = self._request(
            kind="pin",
            authority_id=lineage_authority_id,
            operation_id=operation_id,
            previous_checkpoint_hash=EXTERNAL_MONOTONIC_ROOT_ZERO_HASH,
            checkpoint=checkpoint,
        )
        response = self._invoke(request)
        if response is None:
            raise SourceBrokerV2AuthorityCompositionError("replay root pin returned no receipt")
        return self._receipt(response, request=request)

    def compare_and_advance(
        self,
        *,
        operation_id: str,
        lineage_authority_id: str,
        previous_checkpoint_hash: str,
        checkpoint: SignedReplayLineageCheckpoint,
    ) -> AntiRollbackRoot:
        request = self._request(
            kind="advance",
            authority_id=lineage_authority_id,
            operation_id=operation_id,
            previous_checkpoint_hash=previous_checkpoint_hash,
            checkpoint=checkpoint,
        )
        response = self._invoke(request)
        if response is None:
            raise SourceBrokerV2AuthorityCompositionError("replay root advance returned no receipt")
        return self._receipt(response, request=request)

    def _request(
        self,
        *,
        kind: Literal["current", "pin", "advance"],
        authority_id: str,
        operation_id: str | None = None,
        previous_checkpoint_hash: str | None = None,
        checkpoint: SignedReplayLineageCheckpoint | None = None,
    ) -> ExternalMonotonicRootRequest:
        values: dict[str, object] = {
            "kind": kind,
            "role": self._config.role,
            "root_authority_id": self._config.root_authority_id,
            "root_store_id": self._config.root_store_id,
            "subject_authority_id": authority_id,
            "challenge_nonce": os.urandom(32).hex(),
        }
        if checkpoint is not None:
            values.update(
                operation_id=operation_id,
                previous_checkpoint_hash=previous_checkpoint_hash,
                checkpoint_contract=checkpoint.contract,
                checkpoint_hash=checkpoint.checkpoint_hash,
                checkpoint_json=canonical_model_json_bytes(checkpoint).decode("utf-8"),
            )
        return ExternalMonotonicRootRequest.close(**values)

    def _invoke(self, request: ExternalMonotonicRootRequest) -> str | None:
        try:
            return self._trust.invoke(request)
        except (ConnectionError, ExternalMonotonicRootSecurityError) as exc:
            raise SourceBrokerV2AuthorityCompositionError(
                "replay external root is unavailable or untrusted"
            ) from exc

    def _receipt(
        self,
        receipt_json: str,
        *,
        request: ExternalMonotonicRootRequest,
    ) -> AntiRollbackRoot:
        try:
            receipt = strict_model_validate_canonical_json(
                SourceBrokerV2ReplayRootReceipt,
                receipt_json,
            )
            self._trust.verify_receipt(
                identity=ExternalMonotonicRootReceiptIdentity(
                    role=receipt.role,
                    root_authority_id=receipt.root_authority_id,
                    root_store_id=receipt.root_store_id,
                    closed=receipt.closed,
                    issuer=receipt.issuer,
                    key_id=receipt.key_id,
                    key_purpose=receipt.key_purpose,
                    namespace=receipt.namespace,
                    signature_algorithm=receipt.signature_algorithm,
                    public_key_fingerprint=receipt.public_key_fingerprint,
                ),
                signing_bytes=receipt.signing_bytes(),
                signature=receipt.signature,
            )
        except (TypeError, ValueError, ValidationError, ExternalMonotonicRootSecurityError) as exc:
            raise SourceBrokerV2AuthorityCompositionError(
                "replay external root response is untrusted"
            ) from exc
        if (
            receipt.request_kind != request.kind
            or receipt.request_hash != request.request_hash
            or receipt.challenge_nonce != request.challenge_nonce
            or receipt.replay_lineage_authority_id != request.subject_authority_id
            or (
                request.kind != "current"
                and (
                    receipt.operation_id != request.operation_id
                    or receipt.previous_checkpoint_hash != request.previous_checkpoint_hash
                    or receipt.checkpoint.checkpoint_hash != request.checkpoint_hash
                )
            )
        ):
            raise SourceBrokerV2AuthorityCompositionError(
                "replay external root response does not bind the exact request"
            )
        return AntiRollbackRoot(
            schema_version=1,
            contract="rquant-replay-lineage-anti-rollback-root/v1",
            root_authority_id=receipt.root_authority_id,
            lineage_authority_id=receipt.replay_lineage_authority_id,
            operation_id=receipt.operation_id,
            previous_checkpoint_hash=receipt.previous_checkpoint_hash,
            checkpoint=receipt.checkpoint,
        )


class _OpenSslContractSigningClient:
    def __init__(
        self,
        *,
        private_key_path: Path,
        public_key_path: Path,
        key_purpose: KeyPurpose,
        allowed_namespaces: frozenset[str],
        uid: int,
        gid: int,
    ) -> None:
        self._private_key_path = private_key_path
        self._uid = uid
        self._gid = gid
        self.key_purpose = key_purpose
        self.allowed_namespaces = allowed_namespaces
        self.public_key_fingerprint = ed25519_public_key_fingerprint(
            _read_secure_key(public_key_path, uid=uid, gid=gid)
        )
        _assert_private_matches_public(
            private_key_path=private_key_path,
            public_key_path=public_key_path,
            uid=uid,
            gid=gid,
        )

    def sign(self, *, key_purpose: KeyPurpose, namespace: str, payload: bytes) -> str:
        if key_purpose != self.key_purpose or namespace not in self.allowed_namespaces:
            raise SourceBrokerV2AuthorityCompositionError("contract signer role binding changed")
        _validate_private_key(self._private_key_path, uid=self._uid, gid=self._gid)
        return _openssl_sign(self._private_key_path, payload)


class _OpenSslQuotaReceiptSigner:
    def __init__(
        self,
        *,
        key_id: str,
        private_key_path: Path,
        public_key_path: Path,
        uid: int,
        gid: int,
    ) -> None:
        self.key_id = key_id
        self._private_key_path = private_key_path
        self._public_key = _read_secure_key(public_key_path, uid=uid, gid=gid)
        self._uid = uid
        self._gid = gid
        _assert_private_matches_public(
            private_key_path=private_key_path,
            public_key_path=public_key_path,
            uid=uid,
            gid=gid,
        )

    def sign(self, payload: bytes) -> str:
        _validate_private_key(self._private_key_path, uid=self._uid, gid=self._gid)
        return _openssl_sign(self._private_key_path, payload)

    def verify(self, payload: bytes, signature: str) -> bool:
        try:
            raw = base64.b64decode(signature, validate=True)
        except ValueError:
            return False
        if len(raw) != 64:
            return False
        with tempfile.TemporaryDirectory(prefix="rquant-v2-quota-verify-") as temporary:
            directory = Path(temporary)
            public = directory / "public.pem"
            message = directory / "message.bin"
            signed = directory / "signature.bin"
            public.write_bytes(self._public_key)
            message.write_bytes(payload)
            signed.write_bytes(raw)
            for path in (public, message, signed):
                path.chmod(0o600)
            completed = _run_openssl(
                (
                    "pkeyutl",
                    "-verify",
                    "-pubin",
                    "-inkey",
                    str(public),
                    "-sigfile",
                    str(signed),
                    "-rawin",
                    "-in",
                    str(message),
                )
            )
        return completed.returncode == 0


@dataclass(frozen=True)
class SourceBrokerV2SchedulerClients:
    current_claim: SourceBrokerV2CurrentClaimUnixClient
    source_quota: SourceBrokerV2SourceQuotaUnixClient
    replay_lineage: SourceBrokerV2ReplayLineageUnixClient
    authority_keyring: VerifyOnlyEd25519Keyring
    source_authority_keyring: SourceAuthorityKeyring
    source_client: SourceBrokerV2UnixClient
    binding_hash: str


def compose_production_source_broker_v2_root_service(
    runtime: SourceBrokerV2AuthorityRuntime,
    *,
    role: SourceBrokerV2RootRole,
) -> ExternalMonotonicRootUnixService:
    """Compose one privileged root daemon without any local client fallback."""

    _require_process_identity(runtime.root(role).identity)
    try:
        runtime.validate_root_filesystem(role)
        root = runtime.root(role)
        policy = root.unix_policy
        signer = _root_signer(runtime, root)
        manifest = _root_manifest(runtime, root)
        configuration = ExternalRootServiceConfiguration(
            socket_path=root.socket_path,
            socket_uid=policy.service_identity.uid,
            socket_gid=policy.service_identity.gid,
            service_uid=policy.service_identity.uid,
            service_gid=policy.service_identity.gid,
            allowed_peer_uid=policy.allowed_peer_uid,
            allowed_peer_gid=policy.allowed_peer_gid,
            socket_mode=root.socket_mode,
            socket_directory_mode=root.run_directory_mode,
            role=root.binding.role,
            authority_id=root.authority_id,
            store_id=root.root_store_id,
            rollback_domain_id=root.rollback_domain_id,
            transport_manifest_hash=manifest.manifest_hash,
            request_timeout_ms=runtime.request_timeout_ms,
        )
        backend = PersistentExternalMonotonicRootBackend(
            root.state_path,
            role=root.binding.role,
            authority_id=root.authority_id,
            store_id=root.root_store_id,
        )
        return ExternalMonotonicRootUnixService(
            configuration=configuration,
            backend=backend,
            handler=_root_handler(role=role, signer=signer),
            probe_signer=signer,
        )
    except (
        AuthorityPathSecurityError,
        OSError,
        RuntimeError,
        ValidationError,
        ValueError,
    ) as exc:
        raise SourceBrokerV2AuthorityCompositionError(
            "production external root service configuration is invalid"
        ) from exc


def compose_production_source_broker_v2_source_signer(
    runtime: SourceBrokerV2AuthorityRuntime,
) -> OpenSslSourceBrokerV2AuthoritySigner:
    """Return the source-side signing capability; consumers never receive this object."""

    _require_process_identity(runtime.source_daemon.identity)
    try:
        runtime.validate_source_daemon_filesystem()
        daemon = runtime.source_daemon
        _assert_private_matches_public(
            private_key_path=daemon.private_key_path,
            public_key_path=daemon.public_key_path,
            uid=daemon.identity.uid,
            gid=daemon.identity.gid,
        )
        return OpenSslSourceBrokerV2AuthoritySigner(
            authority_id=daemon.authority_id,
            key_id=runtime.source_authority_current_key_id,
            private_key_path=daemon.private_key_path,
        )
    except (AuthorityPathSecurityError, OSError, RuntimeError, ValueError) as exc:
        raise SourceBrokerV2AuthorityCompositionError(
            "source authority key is unavailable or changed"
        ) from exc


def compose_production_source_broker_v2_source_daemon_policy(
    runtime: SourceBrokerV2AuthorityRuntime,
) -> PeerCredentialsPolicy:
    """Return the daemon-facing exact scheduler peer policy without wiring the daemon."""

    runtime.validate_layout()
    scheduler = runtime.identities.scheduler_client
    return PeerCredentialsPolicy(
        allowed_uids=frozenset({scheduler.uid}),
        allowed_gids=frozenset({scheduler.gid}),
    )


def compose_production_source_broker_v2_current_claim_service(
    runtime: SourceBrokerV2AuthorityRuntime,
) -> SourceBrokerV2AuthorityUnixService:
    """Build only the current-claim authority inside its dedicated process."""

    role = SourceBrokerV2ProcessRole.CURRENT_CLAIM_AUTHORITY
    layout = runtime.current_claim
    _require_process_identity(layout.identity)
    try:
        runtime.validate_authority_filesystem(role)
        _ensure_authority_state(layout)
        keyring = _current_authority_keyring(runtime)
        signer = _contract_signer(runtime, layout, keyring)
        root_client, root_verifier = _root_client(
            runtime,
            role=SourceBrokerV2RootRole.CURRENT_CLAIM,
        )
        external_root = ExternalCurrentClaimMonotonicRootAdapter(
            config=_current_root_config(runtime, root_verifier),
            client=root_client,
            root_verifiers=(root_verifier,),
        )
        authority = compose_production_current_claim_authority(
            layout.state_path,
            authority_id=layout.authority_id,
            signer=signer,
            keyring=keyring,
            external_root=external_root,
            busy_timeout_ms=runtime.busy_timeout_ms,
        )
        return _authority_service(runtime, layout=layout, authority=authority)
    except (OSError, RuntimeError, TypeError, ValidationError, ValueError) as exc:
        raise SourceBrokerV2AuthorityCompositionError(
            "production current-claim authority service is unavailable or untrusted"
        ) from exc


def compose_production_source_broker_v2_source_quota_service(
    runtime: SourceBrokerV2AuthorityRuntime,
) -> SourceBrokerV2AuthorityUnixService:
    """Build only the source-quota authority inside its dedicated process."""

    role = SourceBrokerV2ProcessRole.SOURCE_QUOTA_AUTHORITY
    layout = runtime.source_quota
    _require_process_identity(layout.identity)
    try:
        runtime.validate_authority_filesystem(role)
        _ensure_authority_state(layout)
        root_client, root_verifier = _root_client(
            runtime,
            role=SourceBrokerV2RootRole.SOURCE_QUOTA,
        )
        signer = _OpenSslQuotaReceiptSigner(
            key_id=layout.binding.key_id,
            private_key_path=layout.private_key_path,
            public_key_path=layout.public_key_path,
            uid=layout.identity.uid,
            gid=layout.identity.gid,
        )
        authority = SourceQuotaParentAuthority(
            layout.state_path,
            authority_id=layout.authority_id,
            signer=signer,
            external_root_config=_quota_root_config(runtime, root_verifier),
            external_root_client=root_client,
            external_root_verifiers=(root_verifier,),
            busy_timeout_ms=runtime.busy_timeout_ms,
        )
        return _authority_service(runtime, layout=layout, authority=authority)
    except (OSError, RuntimeError, TypeError, ValidationError, ValueError) as exc:
        raise SourceBrokerV2AuthorityCompositionError(
            "production source-quota authority service is unavailable or untrusted"
        ) from exc


def compose_production_source_broker_v2_replay_lineage_service(
    runtime: SourceBrokerV2AuthorityRuntime,
) -> SourceBrokerV2AuthorityUnixService:
    """Build only the replay-lineage authority inside its dedicated process."""

    role = SourceBrokerV2ProcessRole.REPLAY_LINEAGE_AUTHORITY
    layout = runtime.replay_lineage
    _require_process_identity(layout.identity)
    try:
        runtime.validate_authority_filesystem(role)
        _ensure_authority_state(layout)
        keyring = _replay_authority_keyring(runtime)
        signer = _contract_signer(runtime, layout, keyring)
        root_client, root_verifier = _root_client(
            runtime,
            role=SourceBrokerV2RootRole.REPLAY_LINEAGE,
        )
        monotonic_root = SourceBrokerV2ReplayLineageExternalRoot(
            config=_replay_root_config(runtime, root_verifier),
            client=root_client,
            verifier=root_verifier,
        )
        broker_state, replay_state = layout.separation_state_paths
        authority = compose_production_replay_lineage_authority(
            layout.state_path,
            authority_id=layout.authority_id,
            signer=signer,
            keyring=keyring,
            broker_db_path=broker_state,
            replay_db_path=replay_state,
            monotonic_checkpoint_store=monotonic_root,
            lineage_key_paths=(layout.private_key_path, layout.public_key_path),
            expected_verifier_fingerprints=keyring.fingerprints_for_purpose("replay_claim"),
            busy_timeout_ms=runtime.busy_timeout_ms,
        )
        return _authority_service(runtime, layout=layout, authority=authority)
    except (OSError, RuntimeError, TypeError, ValidationError, ValueError) as exc:
        raise SourceBrokerV2AuthorityCompositionError(
            "production replay-lineage authority service is unavailable or untrusted"
        ) from exc


def compose_production_source_broker_v2_scheduler_clients(
    runtime: SourceBrokerV2AuthorityRuntime,
) -> SourceBrokerV2SchedulerClients:
    """Build public-only Unix clients inside the scheduler process."""

    scheduler = runtime.scheduler_client
    _require_process_identity(scheduler.identity)
    try:
        runtime.validate_scheduler_filesystem()
        authority_keyring = _authority_keyring(runtime)
        source_keyring = _source_keyring(runtime)
        current = SourceBrokerV2CurrentClaimUnixClient(
            endpoint=_authority_endpoint(runtime.current_claim, scheduler.identity),
            server_policy=_authority_server_policy(runtime.current_claim),
            timeout_ms=runtime.request_timeout_ms,
        )
        quota = SourceBrokerV2SourceQuotaUnixClient(
            endpoint=_authority_endpoint(runtime.source_quota, scheduler.identity),
            server_policy=_authority_server_policy(runtime.source_quota),
            timeout_ms=runtime.request_timeout_ms,
        )
        replay = SourceBrokerV2ReplayLineageUnixClient(
            endpoint=_authority_endpoint(runtime.replay_lineage, scheduler.identity),
            server_policy=_authority_server_policy(runtime.replay_lineage),
            timeout_ms=runtime.request_timeout_ms,
        )
        daemon_policy = runtime.source_daemon.unix_policy(scheduler.identity)
        source_client = SourceBrokerV2UnixClient(
            endpoint=SocketEndpointPolicy(
                path=daemon_policy.socket_path,
                owner_uid=daemon_policy.service_identity.uid,
                group_gid=daemon_policy.access_gid,
                mode=runtime.source_daemon.socket_mode,
            ),
            server_policy=ServerCredentialsPolicy(
                expected_uid=daemon_policy.service_identity.uid,
                expected_gid=daemon_policy.service_identity.gid,
            ),
            total_request_deadline_seconds=runtime.source_request_deadline_seconds,
            source_authority_keyring=source_keyring,
            max_attempts=runtime.source_max_attempts,
        )
        binding_hash = canonical_sha256(
            {
                "contract": "rquant-source-broker-v2-scheduler-clients/v1",
                "identities": runtime.identities.model_dump(mode="json"),
                "authority_bindings": [
                    layout.binding.model_dump(mode="json")
                    for layout in (
                        runtime.current_claim,
                        runtime.source_quota,
                        runtime.replay_lineage,
                        runtime.source_daemon,
                    )
                ],
                "authority_sockets": [
                    os.fspath(layout.socket_path)
                    for layout in (
                        runtime.current_claim,
                        runtime.source_quota,
                        runtime.replay_lineage,
                        runtime.source_daemon,
                    )
                ],
                "manifest_verification_key_id": runtime.manifest_verification_key_id,
                "source_authority_key_ids": [
                    runtime.source_authority_current_key_id,
                    runtime.source_authority_next_key_id,
                ],
            }
        )
        return SourceBrokerV2SchedulerClients(
            current_claim=current,
            source_quota=quota,
            replay_lineage=replay,
            authority_keyring=authority_keyring,
            source_authority_keyring=source_keyring,
            source_client=source_client,
            binding_hash=binding_hash,
        )
    except (OSError, RuntimeError, TypeError, ValidationError, ValueError) as exc:
        raise SourceBrokerV2AuthorityCompositionError(
            "production scheduler clients are unavailable or untrusted"
        ) from exc


def compose_production_source_broker_v2_authorities(
    runtime: SourceBrokerV2AuthorityRuntime,
) -> Never:
    """Reject the former cross-UID in-process production graph."""

    del runtime
    raise SourceBrokerV2AuthorityCompositionError(
        "production aggregate authority composition is forbidden; use role-local factories"
    )


def _require_process_identity(identity: SourceBrokerV2ProcessIdentity) -> None:
    if os.geteuid() != identity.uid or os.getegid() != identity.gid:
        raise SourceBrokerV2AuthorityCompositionError(
            f"process identity does not match role identity {identity.role.value}"
        )


def _ensure_authority_state(layout: SourceBrokerV2AuthorityRuntimePath) -> None:
    try:
        secure_create_regular_file(
            layout.state_path,
            allowed_ancestor_uids=frozenset({0, layout.identity.uid}),
            expected_uid=layout.identity.uid,
            expected_gid=layout.identity.gid,
            expected_mode=layout.state_mode,
        )
    except AuthorityPathSecurityError as exc:
        raise SourceBrokerV2AuthorityCompositionError(
            "authority state path is unavailable or untrusted"
        ) from exc


def _authority_endpoint(
    layout: SourceBrokerV2AuthorityRuntimePath,
    scheduler: SourceBrokerV2ProcessIdentity,
) -> SocketEndpointPolicy:
    policy = layout.unix_policy(scheduler)
    return SocketEndpointPolicy(
        path=policy.socket_path,
        owner_uid=policy.service_identity.uid,
        group_gid=policy.access_gid,
        mode=layout.socket_mode,
    )


def _authority_server_policy(
    layout: SourceBrokerV2AuthorityRuntimePath,
) -> ServerCredentialsPolicy:
    return ServerCredentialsPolicy(
        expected_uid=layout.identity.uid,
        expected_gid=layout.identity.gid,
    )


def _authority_service(
    runtime: SourceBrokerV2AuthorityRuntime,
    *,
    layout: SourceBrokerV2AuthorityRuntimePath,
    authority: object,
) -> SourceBrokerV2AuthorityUnixService:
    scheduler = runtime.identities.scheduler_client
    return SourceBrokerV2AuthorityUnixService(
        endpoint=_authority_endpoint(layout, scheduler),
        peer_policy=PeerCredentialsPolicy(
            allowed_uids=frozenset({scheduler.uid}),
            allowed_gids=frozenset({scheduler.gid}),
        ),
        service_identity=layout.identity,
        authority=authority,
        timeout_ms=runtime.request_timeout_ms,
    )


def _root_manifest(
    runtime: SourceBrokerV2AuthorityRuntime,
    root: SourceBrokerV2ExternalRootRuntime,
) -> UnixSocketExternalMonotonicRootManifest:
    policy = root.unix_policy
    return UnixSocketExternalMonotonicRootManifest(
        role=root.binding.role,
        authority_id=root.authority_id,
        store_id=root.root_store_id,
        rollback_domain_id=root.rollback_domain_id,
        socket_path=root.socket_path,
        socket_uid=policy.service_identity.uid,
        socket_gid=policy.service_identity.gid,
        socket_mode=root.socket_mode,
        peer_uid=policy.service_identity.uid,
        peer_gid=policy.service_identity.gid,
        connect_timeout_ms=runtime.request_timeout_ms,
        max_response_bytes=1024 * 1024,
    )


def _root_signer(
    runtime: SourceBrokerV2AuthorityRuntime,
    root: SourceBrokerV2ExternalRootRuntime,
) -> OpenSslExternalMonotonicRootSigner:
    return OpenSslExternalMonotonicRootSigner(
        private_key_path=root.private_key_path,
        public_key_path=root.public_key_path,
        issuer=root.authority_id,
        key_id=root.binding.key_id,
        key_purpose=root.binding.key_purpose,
        allowed_namespaces=frozenset(
            {
                EXTERNAL_ROOT_SERVICE_PROBE_NAMESPACE,
                _root_receipt_namespace(root.role),
            }
        ),
    )


def _root_handler(*, role: SourceBrokerV2RootRole, signer: _RootSigner) -> object:
    if role is SourceBrokerV2RootRole.CURRENT_CLAIM:
        return _CurrentClaimRootRoleHandler(signer)
    if role is SourceBrokerV2RootRole.SOURCE_QUOTA:
        return SourceQuotaExternalRootRoleHandler(signer)
    return _ReplayLineageRootRoleHandler(signer)


def _root_receipt_namespace(role: SourceBrokerV2RootRole) -> str:
    if role is SourceBrokerV2RootRole.CURRENT_CLAIM:
        return "rquant-current-claim-anti-rollback-root/v1"
    if role is SourceBrokerV2RootRole.SOURCE_QUOTA:
        return SOURCE_QUOTA_ROOT_RECEIPT_NAMESPACE
    return REPLAY_LINEAGE_ROOT_RECEIPT_NAMESPACE


def _root_client(
    runtime: SourceBrokerV2AuthorityRuntime,
    *,
    role: SourceBrokerV2RootRole,
) -> tuple[UnixSocketExternalMonotonicRootClient, ClosedExternalMonotonicRootVerifier]:
    root = runtime.root(role)
    verifier = ClosedExternalMonotonicRootVerifier(
        public_key_path=root.consumer_public_key_path,
        issuer=root.authority_id,
        key_id=root.binding.key_id,
        key_purpose=root.binding.key_purpose,
    )
    manifest = _root_manifest(runtime, root)
    client = UnixSocketExternalMonotonicRootClient(manifest)
    probe_external_monotonic_root_service(
        manifest,
        verifier=verifier,
        expected_transport_manifest_hash=manifest.manifest_hash,
    )
    return client, verifier


def _current_root_config(
    runtime: SourceBrokerV2AuthorityRuntime,
    verifier: ClosedExternalMonotonicRootVerifier,
) -> ExternalCurrentClaimRootConfig:
    root = runtime.root(SourceBrokerV2RootRole.CURRENT_CLAIM)
    return ExternalCurrentClaimRootConfig(
        transport="unix-socket-v1",
        transport_manifest_hash=_root_manifest(runtime, root).manifest_hash,
        root_authority_id=root.authority_id,
        root_store_id=root.root_store_id,
        root_issuer=verifier.issuer,
        root_key_id=verifier.key_id,
        root_public_key_fingerprint=verifier.public_key_fingerprint,
        witness_rollback_domain_id=root.rollback_domain_id,
        local_rollback_domain_id=runtime.current_claim.binding.fence_domain_id,
    )


def _quota_root_config(
    runtime: SourceBrokerV2AuthorityRuntime,
    verifier: ClosedExternalMonotonicRootVerifier,
) -> SourceQuotaExternalRootConfig:
    root = runtime.root(SourceBrokerV2RootRole.SOURCE_QUOTA)
    return SourceQuotaExternalRootConfig(
        transport="unix-socket-v1",
        transport_manifest_hash=_root_manifest(runtime, root).manifest_hash,
        root_authority_id=root.authority_id,
        root_store_id=root.root_store_id,
        root_issuer=verifier.issuer,
        root_key_id=verifier.key_id,
        root_public_key_fingerprint=verifier.public_key_fingerprint,
        witness_rollback_domain_id=root.rollback_domain_id,
        local_rollback_domain_id=runtime.source_quota.binding.fence_domain_id,
    )


def _replay_root_config(
    runtime: SourceBrokerV2AuthorityRuntime,
    verifier: ClosedExternalMonotonicRootVerifier,
) -> SourceBrokerV2ReplayRootConfig:
    root = runtime.root(SourceBrokerV2RootRole.REPLAY_LINEAGE)
    return SourceBrokerV2ReplayRootConfig(
        transport="unix-socket-v1",
        transport_manifest_hash=_root_manifest(runtime, root).manifest_hash,
        root_authority_id=root.authority_id,
        root_store_id=root.root_store_id,
        root_issuer=verifier.issuer,
        root_key_id=verifier.key_id,
        root_public_key_fingerprint=verifier.public_key_fingerprint,
        witness_rollback_domain_id=root.rollback_domain_id,
        local_rollback_domain_id=runtime.replay_lineage.binding.fence_domain_id,
    )


def _authority_keyring(runtime: SourceBrokerV2AuthorityRuntime) -> VerifyOnlyEd25519Keyring:
    records = (
        Ed25519PublicKeyRecord(
            key_id=runtime.manifest_verification_key_id,
            issuer="source-broker-v2-manifest-authority",
            key_purpose="adapter_manifest",
            rotation="active",
            public_key_pem=_read_secure_key(
                runtime.manifest_verification_public_key_path,
                uid=runtime.scheduler_client.identity.uid,
                gid=runtime.scheduler_client.identity.gid,
            ),
        ),
        Ed25519PublicKeyRecord(
            key_id=runtime.current_claim.binding.key_id,
            issuer=runtime.current_claim.authority_id,
            key_purpose="source_use_plan_v2",
            rotation="active",
            public_key_pem=_read_secure_key(
                runtime.scheduler_client.current_claim_public_key_path,
                uid=runtime.scheduler_client.identity.uid,
                gid=runtime.scheduler_client.identity.gid,
            ),
        ),
        Ed25519PublicKeyRecord(
            key_id=runtime.replay_lineage.binding.key_id,
            issuer=runtime.replay_lineage.authority_id,
            key_purpose="replay_claim",
            rotation="active",
            public_key_pem=_read_secure_key(
                runtime.scheduler_client.replay_lineage_public_key_path,
                uid=runtime.scheduler_client.identity.uid,
                gid=runtime.scheduler_client.identity.gid,
            ),
        ),
    )
    return VerifyOnlyEd25519Keyring(
        records=records,
        issuer_allowlist={
            "adapter_manifest": frozenset({"source-broker-v2-manifest-authority"}),
            "source_use_plan_v2": frozenset({runtime.current_claim.authority_id}),
            "replay_claim": frozenset({runtime.replay_lineage.authority_id}),
        },
        rotation_allowlist={
            ("source-broker-v2-manifest-authority", "adapter_manifest"): frozenset(
                {runtime.manifest_verification_key_id}
            ),
            (runtime.current_claim.authority_id, "source_use_plan_v2"): frozenset(
                {runtime.current_claim.binding.key_id}
            ),
            (runtime.replay_lineage.authority_id, "replay_claim"): frozenset(
                {runtime.replay_lineage.binding.key_id}
            ),
        },
    )


def _current_authority_keyring(
    runtime: SourceBrokerV2AuthorityRuntime,
) -> VerifyOnlyEd25519Keyring:
    layout = runtime.current_claim
    manifest_path = layout.manifest_public_key_path
    if manifest_path is None:
        raise SourceBrokerV2AuthorityCompositionError(
            "current authority manifest verification key is unavailable"
        )
    records = (
        Ed25519PublicKeyRecord(
            key_id=runtime.manifest_verification_key_id,
            issuer="source-broker-v2-manifest-authority",
            key_purpose="adapter_manifest",
            rotation="active",
            public_key_pem=_read_secure_key(
                manifest_path,
                uid=layout.identity.uid,
                gid=layout.identity.gid,
            ),
        ),
        Ed25519PublicKeyRecord(
            key_id=layout.binding.key_id,
            issuer=layout.authority_id,
            key_purpose="source_use_plan_v2",
            rotation="active",
            public_key_pem=_read_secure_key(
                layout.public_key_path,
                uid=layout.identity.uid,
                gid=layout.identity.gid,
            ),
        ),
    )
    return VerifyOnlyEd25519Keyring(
        records=records,
        issuer_allowlist={
            "adapter_manifest": frozenset({"source-broker-v2-manifest-authority"}),
            "source_use_plan_v2": frozenset({layout.authority_id}),
        },
        rotation_allowlist={
            ("source-broker-v2-manifest-authority", "adapter_manifest"): frozenset(
                {runtime.manifest_verification_key_id}
            ),
            (layout.authority_id, "source_use_plan_v2"): frozenset({layout.binding.key_id}),
        },
    )


def _replay_authority_keyring(
    runtime: SourceBrokerV2AuthorityRuntime,
) -> VerifyOnlyEd25519Keyring:
    layout = runtime.replay_lineage
    return VerifyOnlyEd25519Keyring(
        records=(
            Ed25519PublicKeyRecord(
                key_id=layout.binding.key_id,
                issuer=layout.authority_id,
                key_purpose="replay_claim",
                rotation="active",
                public_key_pem=_read_secure_key(
                    layout.public_key_path,
                    uid=layout.identity.uid,
                    gid=layout.identity.gid,
                ),
            ),
        ),
        issuer_allowlist={"replay_claim": frozenset({layout.authority_id})},
        rotation_allowlist={
            (layout.authority_id, "replay_claim"): frozenset({layout.binding.key_id})
        },
    )


def _contract_signer(
    runtime: SourceBrokerV2AuthorityRuntime,
    authority: object,
    keyring: VerifyOnlyEd25519Keyring,
) -> Ed25519ContractSigner:
    if authority is runtime.current_claim:
        purpose: KeyPurpose = "source_use_plan_v2"
        namespaces = frozenset({SOURCE_USE_PLAN_V2_NAMESPACE})
    elif authority is runtime.replay_lineage:
        purpose = "replay_claim"
        namespaces = frozenset({REPLAY_CLAIM_NAMESPACE})
    else:
        raise SourceBrokerV2AuthorityCompositionError("unknown contract signing authority")
    client = _OpenSslContractSigningClient(
        private_key_path=authority.private_key_path,
        public_key_path=authority.public_key_path,
        key_purpose=purpose,
        allowed_namespaces=namespaces,
        uid=authority.identity.uid,
        gid=authority.identity.gid,
    )
    signer = Ed25519ContractSigner(
        key_id=authority.binding.key_id,
        issuer=authority.authority_id,
        key_purpose=purpose,
        client=client,
    )
    if not keyring.allows_signer(signer):
        raise SourceBrokerV2AuthorityCompositionError("authority signing key is not in its keyring")
    return signer


def _source_keyring(runtime: SourceBrokerV2AuthorityRuntime) -> SourceAuthorityKeyring:
    try:
        return SourceAuthorityKeyring(
            expected_authority_id=runtime.source_authority_id,
            allowed_public_keys={
                runtime.source_authority_current_key_id: _read_secure_key(
                    runtime.source_authority_current_public_key_path,
                    uid=runtime.scheduler_client.identity.uid,
                    gid=runtime.scheduler_client.identity.gid,
                ),
                runtime.source_authority_next_key_id: _read_secure_key(
                    runtime.source_authority_next_public_key_path,
                    uid=runtime.scheduler_client.identity.uid,
                    gid=runtime.scheduler_client.identity.gid,
                ),
            },
            expected_purpose=SOURCE_BROKER_V2_AUTHORITY_PURPOSE,
            expected_schema_version=2,
        )
    except (OSError, ValueError) as exc:
        raise SourceBrokerV2AuthorityCompositionError(
            "source authority key is unavailable"
        ) from exc


def _read_secure_key(path: Path, *, uid: int, gid: int) -> bytes:
    try:
        return read_secure_regular_file(
            path,
            allowed_ancestor_uids=frozenset({0, uid}),
            expected_uid=uid,
            expected_gid=gid,
            allowed_modes=frozenset({0o600}),
            max_bytes=64 * 1024,
        )
    except AuthorityPathSecurityError as exc:
        raise SourceBrokerV2AuthorityCompositionError("authority key is unsafe") from exc


def _validate_private_key(path: Path, *, uid: int, gid: int) -> None:
    try:
        secure_path_metadata(
            path,
            allowed_ancestor_uids=frozenset({0, uid}),
            kind="file",
            expected_uid=uid,
            expected_gid=gid,
            expected_mode=0o600,
        )
    except AuthorityPathSecurityError as exc:
        raise SourceBrokerV2AuthorityCompositionError("authority private key is unsafe") from exc


def _assert_private_matches_public(
    *, private_key_path: Path, public_key_path: Path, uid: int, gid: int
) -> None:
    _validate_private_key(private_key_path, uid=uid, gid=gid)
    expected = _read_secure_key(public_key_path, uid=uid, gid=gid)
    exported = _run_openssl(("pkey", "-in", str(private_key_path), "-pubout"))
    if exported.returncode != 0 or exported.stdout != expected:
        raise SourceBrokerV2AuthorityCompositionError(
            "authority private/public key binding changed"
        )


def _openssl_sign(private_key_path: Path, payload: bytes) -> str:
    with tempfile.TemporaryDirectory(prefix="rquant-v2-authority-sign-") as temporary:
        directory = Path(temporary)
        message = directory / "message.bin"
        signature = directory / "signature.bin"
        message.write_bytes(payload)
        message.chmod(0o600)
        completed = _run_openssl(
            (
                "pkeyutl",
                "-sign",
                "-rawin",
                "-inkey",
                str(private_key_path),
                "-in",
                str(message),
                "-out",
                str(signature),
            )
        )
        if completed.returncode != 0:
            raise SourceBrokerV2AuthorityCompositionError("authority signing failed")
        raw = signature.read_bytes()
    if len(raw) != 64:
        raise SourceBrokerV2AuthorityCompositionError("authority signature is invalid")
    return base64.b64encode(raw).decode("ascii")


def _run_openssl(arguments: tuple[str, ...]) -> subprocess.CompletedProcess[bytes]:
    executable = _openssl_binary()
    try:
        return subprocess.run(
            (executable, *arguments),
            check=False,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise SourceBrokerV2AuthorityCompositionError("openssl authority operation failed") from exc


def _openssl_binary() -> str:
    for candidate in ("/opt/homebrew/bin/openssl", "/usr/bin/openssl", shutil.which("openssl")):
        if candidate and Path(candidate).is_file():
            return candidate
    raise SourceBrokerV2AuthorityCompositionError("openssl is required for authority composition")


__all__ = [
    "REPLAY_LINEAGE_ROOT_KEY_PURPOSE",
    "REPLAY_LINEAGE_ROOT_RECEIPT_NAMESPACE",
    "SourceBrokerV2AuthorityCompositionError",
    "SourceBrokerV2ReplayLineageExternalRoot",
    "SourceBrokerV2ReplayRootConfig",
    "SourceBrokerV2ReplayRootReceipt",
    "SourceBrokerV2SchedulerClients",
    "compose_production_source_broker_v2_authorities",
    "compose_production_source_broker_v2_current_claim_service",
    "compose_production_source_broker_v2_replay_lineage_service",
    "compose_production_source_broker_v2_root_service",
    "compose_production_source_broker_v2_scheduler_clients",
    "compose_production_source_broker_v2_source_daemon_policy",
    "compose_production_source_broker_v2_source_quota_service",
    "compose_production_source_broker_v2_source_signer",
]
