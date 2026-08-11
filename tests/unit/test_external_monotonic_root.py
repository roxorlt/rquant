from __future__ import annotations

import socket
import struct
from pathlib import Path

import pytest

from rquant.external_monotonic_root import (
    ExternalMonotonicRootConfig,
    ExternalMonotonicRootReceiptIdentity,
    ExternalMonotonicRootRequest,
    ExternalMonotonicRootSecurityError,
    ExternalMonotonicRootTrustBoundary,
    UnixSocketExternalMonotonicRootClient,
    UnixSocketExternalMonotonicRootManifest,
)
from rquant.runtime_contracts import canonical_sha256
from rquant.strict_json import canonical_json_bytes, strict_model_validate_canonical_json


class _Verifier:
    issuer = "resource-root-issuer"
    key_id = "resource-root-key"
    key_purpose = "resource-journal-high-water"
    signature_algorithm = "ed25519"
    public_key_fingerprint = "a" * 64

    def sign(self, *, namespace: str, payload: bytes) -> str:
        return canonical_sha256(
            {"key": self.key_id, "namespace": namespace, "payload": payload.hex()}
        )

    def verify(self, *, namespace: str, payload: bytes, signature: str) -> bool:
        return signature == self.sign(namespace=namespace, payload=payload)


class _Client:
    role = "resource_journal_monotonic_root"
    authority_id = "resource-root-authority"
    store_id = "resource-root-store"
    transport = "nonproduction-inprocess-v1"
    manifest_hash = canonical_sha256({"client": "resource-root-test-client-v1"})
    rollback_domain_id = "remote-resource-root-domain"

    def __init__(self) -> None:
        self.response: str | None = None
        self.last_request: str | None = None
        self.fail = False

    def invoke(self, *, request_json: str) -> str | None:
        self.last_request = request_json
        if self.fail:
            raise ConnectionError("external CAS unavailable")
        return self.response


def _config(**updates: str) -> ExternalMonotonicRootConfig:
    values = {
        "transport": "nonproduction-inprocess-v1",
        "transport_manifest_hash": _Client.manifest_hash,
        "role": "resource_journal_monotonic_root",
        "root_authority_id": "resource-root-authority",
        "root_store_id": "resource-root-store",
        "root_issuer": _Verifier.issuer,
        "root_key_id": _Verifier.key_id,
        "root_key_purpose": _Verifier.key_purpose,
        "root_receipt_namespace": "rquant-resource-root-receipt/v1",
        "root_public_key_fingerprint": _Verifier.public_key_fingerprint,
        "witness_rollback_domain_id": "remote-resource-root-domain",
        "local_rollback_domain_id": "local-resource-cache-domain",
    }
    values.update(updates)
    return ExternalMonotonicRootConfig.model_validate(values)


def _trust(
    *,
    client: _Client | None = None,
    config: ExternalMonotonicRootConfig | None = None,
) -> tuple[ExternalMonotonicRootTrustBoundary, _Client, _Verifier]:
    selected_client = client or _Client()
    verifier = _Verifier()
    return (
        ExternalMonotonicRootTrustBoundary(
            config=config or _config(),
            client=selected_client,
            root_verifiers=(verifier,),
        ),
        selected_client,
        verifier,
    )


def test_role_neutral_transport_emits_one_canonical_versioned_request() -> None:
    trust, client, _ = _trust()
    checkpoint = {
        "contract": "rquant-resource-checkpoint/v1",
        "sequence": 4,
    }
    checkpoint_json = canonical_json_bytes(checkpoint).decode("utf-8")
    client.response = "signed-resource-receipt"
    request = ExternalMonotonicRootRequest.close(
        kind="advance",
        role=client.role,
        root_authority_id=client.authority_id,
        root_store_id=client.store_id,
        subject_authority_id="resource-journal-a",
        challenge_nonce="d" * 64,
        operation_id="b" * 64,
        previous_checkpoint_hash="c" * 64,
        checkpoint_contract="rquant-resource-checkpoint/v1",
        checkpoint_hash=canonical_sha256(checkpoint),
        checkpoint_json=checkpoint_json,
    )

    assert trust.invoke(request) == "signed-resource-receipt"
    assert client.last_request is not None
    assert (
        strict_model_validate_canonical_json(
            ExternalMonotonicRootRequest,
            client.last_request,
        )
        == request
    )


def test_binding_rejects_local_domain_and_wrong_client_or_verifier_identity() -> None:
    with pytest.raises(ValueError, match="independent rollback domain"):
        _config(
            witness_rollback_domain_id="same-domain",
            local_rollback_domain_id="same-domain",
        )

    client = _Client()
    client.store_id = "donor-store"
    with pytest.raises(ExternalMonotonicRootSecurityError, match="client identity"):
        _trust(client=client)

    with pytest.raises(ExternalMonotonicRootSecurityError, match="verifier"):
        ExternalMonotonicRootTrustBoundary(
            config=_config(root_public_key_fingerprint="f" * 64),
            client=_Client(),
            root_verifiers=(_Verifier(),),
        )


def test_closed_receipt_identity_and_signature_are_verified_independently() -> None:
    trust, _, verifier = _trust()
    identity = ExternalMonotonicRootReceiptIdentity(
        role="resource_journal_monotonic_root",
        root_authority_id="resource-root-authority",
        root_store_id="resource-root-store",
        closed=True,
        issuer=verifier.issuer,
        key_id=verifier.key_id,
        key_purpose=verifier.key_purpose,
        namespace="rquant-resource-root-receipt/v1",
        signature_algorithm=verifier.signature_algorithm,
        public_key_fingerprint=verifier.public_key_fingerprint,
    )
    payload = b"canonical-role-receipt"
    signature = verifier.sign(namespace=identity.namespace, payload=payload)
    trust.verify_receipt(identity=identity, signing_bytes=payload, signature=signature)

    with pytest.raises(ExternalMonotonicRootSecurityError, match="verification failed"):
        trust.verify_receipt(identity=identity, signing_bytes=payload, signature="bad")
    with pytest.raises(ExternalMonotonicRootSecurityError, match="trust binding"):
        trust.verify_receipt(
            identity=identity.model_copy(update={"root_store_id": "donor-store"}),
            signing_bytes=payload,
            signature=signature,
        )


def test_request_shape_response_loss_and_missing_mutation_receipt_fail_closed(
    tmp_path: Path,
) -> None:
    del tmp_path
    checkpoint = {
        "contract": "rquant-resource-checkpoint/v1",
        "sequence": 0,
    }
    with pytest.raises(ValueError, match="canonical"):
        ExternalMonotonicRootRequest.close(
            kind="pin",
            role="resource_journal_monotonic_root",
            root_authority_id="resource-root-authority",
            root_store_id="resource-root-store",
            subject_authority_id="resource-journal-a",
            challenge_nonce="d" * 64,
            operation_id="b" * 64,
            previous_checkpoint_hash="0" * 64,
            checkpoint_contract="rquant-resource-checkpoint/v1",
            checkpoint_hash=canonical_sha256(checkpoint),
            checkpoint_json=canonical_json_bytes(checkpoint).decode("utf-8") + "\n",
        )

    request = ExternalMonotonicRootRequest.close(
        kind="pin",
        role="resource_journal_monotonic_root",
        root_authority_id="resource-root-authority",
        root_store_id="resource-root-store",
        subject_authority_id="resource-journal-a",
        challenge_nonce="d" * 64,
        operation_id="b" * 64,
        previous_checkpoint_hash="0" * 64,
        checkpoint_contract="rquant-resource-checkpoint/v1",
        checkpoint_hash=canonical_sha256(checkpoint),
        checkpoint_json=canonical_json_bytes(checkpoint).decode("utf-8"),
    )
    trust, client, _ = _trust()
    with pytest.raises(ExternalMonotonicRootSecurityError, match="returned no receipt"):
        trust.invoke(request)
    client.fail = True
    with pytest.raises(ConnectionError, match="external CAS unavailable"):
        trust.invoke(request)


class _LinuxPeerCredentialSocket:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def getsockopt(self, level: int, option: int, length: int) -> bytes:
        assert level == socket.SOL_SOCKET
        assert option == socket.SO_PEERCRED
        assert length == 12
        return self._payload


def _peer_validation_client(*, peer_uid: int, peer_gid: int) -> object:
    client = object.__new__(UnixSocketExternalMonotonicRootClient)
    client._manifest = UnixSocketExternalMonotonicRootManifest(
        role="resource_journal_monotonic_root",
        authority_id="resource-root-authority",
        store_id="resource-root-store",
        rollback_domain_id="remote-resource-root-domain",
        socket_path=Path("/tmp/external-root.sock"),
        socket_uid=peer_uid,
        socket_gid=peer_gid,
        peer_uid=peer_uid,
        peer_gid=peer_gid,
        connect_timeout_ms=100,
        max_response_bytes=1_024,
    )
    return client


def test_linux_so_peercred_accepts_exact_uid_gid(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(socket, "SO_PEERCRED", 17, raising=False)
    client = _peer_validation_client(peer_uid=501, peer_gid=20)

    client._validate_peer(_LinuxPeerCredentialSocket(struct.pack("3i", 42, 501, 20)))


@pytest.mark.parametrize(
    "payload, message",
    [
        (struct.pack("3i", 42, 502, 20), "identity changed"),
        (struct.pack("3i", 42, 501, 21), "identity changed"),
        (b"short", "malformed"),
        (b"too-long-for-ucred", "malformed"),
    ],
)
def test_linux_so_peercred_rejects_wrong_identity_or_struct_length(
    monkeypatch: pytest.MonkeyPatch,
    payload: bytes,
    message: str,
) -> None:
    monkeypatch.setattr(socket, "SO_PEERCRED", 17, raising=False)
    client = _peer_validation_client(peer_uid=501, peer_gid=20)

    with pytest.raises(ExternalMonotonicRootSecurityError, match=message):
        client._validate_peer(_LinuxPeerCredentialSocket(payload))
