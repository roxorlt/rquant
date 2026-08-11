from __future__ import annotations

import inspect
import socket
import sys
import time
from pathlib import Path

import pytest

import rquant.daily_receipt_socket_authority as authority_module
from rquant.daily_receipt_socket_authority import (
    DAILY_RECEIPT_IDENTITY_PROTOCOL,
    DAILY_RECEIPT_NAMESPACE_RUN,
    DAILY_RECEIPT_NAMESPACE_STAGE,
    DAILY_RECEIPT_NAMESPACES,
    DAILY_RECEIPT_SOCKET_ENDPOINT,
    DAILY_RECEIPT_TRUSTED_KEYRING_PATH,
    DailyReceiptSocketAuthorityError,
    DailyReceiptSocketSigningClient,
    _DailyReceiptNonceStore,
    _validate_inherited_listener,
    daily_receipt_peer_credentials_supported,
    daily_receipt_socket_identity_envelope,
    daily_receipt_socket_signature_envelope,
    serve_daily_receipt_socket_authority,
)


def test_rejects_inherited_fd_that_is_not_socket() -> None:
    with pytest.raises(DailyReceiptSocketAuthorityError, match="socket"):
        _validate_inherited_listener(0, expected_path=DAILY_RECEIPT_SOCKET_ENDPOINT)


def test_rejects_inherited_fd_that_is_not_unix_stream_listener() -> None:
    first, second = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        with pytest.raises(
            DailyReceiptSocketAuthorityError,
            match="listening|listener|path|endpoint",
        ):
            _validate_inherited_listener(
                first.fileno(),
                expected_path=DAILY_RECEIPT_SOCKET_ENDPOINT,
            )
    finally:
        first.close()
        second.close()


def test_rejects_inherited_listener_bound_to_wrong_path(tmp_path: Path) -> None:
    wrong_path = Path("/tmp") / f"rqdrs-unit-{time.time_ns()}.sock"
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        listener.bind(str(wrong_path))
        listener.listen(1)
        # Linux reads SO_ACCEPTCONN and reaches the endpoint check; macOS cannot
        # read SO_ACCEPTCONN on an fromfd socket and fails closed as not listening.
        with pytest.raises(
            DailyReceiptSocketAuthorityError,
            match="path|endpoint|listening",
        ):
            _validate_inherited_listener(
                listener.fileno(),
                expected_path=DAILY_RECEIPT_SOCKET_ENDPOINT,
            )
    finally:
        listener.close()
        wrong_path.unlink(missing_ok=True)


def test_authority_has_no_injectable_peer_credential_seam() -> None:
    """Static contract: peer identity comes from the kernel, never from a caller."""

    parameters = inspect.signature(serve_daily_receipt_socket_authority).parameters
    assert "peer_credential_provider" not in parameters
    assert not any(
        fragment in name
        for name in parameters
        for fragment in ("peer", "credential", "provider")
    )
    assert not hasattr(authority_module, "PeerCredentialProvider")
    assert "SO_PEERCRED" in inspect.getsource(authority_module._peer_credentials)
    assert "SO_PEERCRED" in inspect.getsource(
        authority_module.daily_receipt_peer_credentials_supported
    )


def test_authority_refuses_to_serve_without_real_kernel_peer_credentials(
    tmp_path: Path,
) -> None:
    if daily_receipt_peer_credentials_supported():
        assert sys.platform.startswith("linux")
        assert hasattr(socket, "SO_PEERCRED")
        return
    with pytest.raises(DailyReceiptSocketAuthorityError, match="SO_PEERCRED"):
        serve_daily_receipt_socket_authority(
            socket_path=tmp_path / "daily-receipt-signer.sock",
            keys_path=tmp_path / "daily-receipt-keys.json",
            nonce_state_root=tmp_path / "state",
            max_connections=1,
        )


def test_signature_envelope_binds_namespace_nonce_payload_hash_and_key_id() -> None:
    envelope = daily_receipt_socket_signature_envelope(
        namespace=DAILY_RECEIPT_NAMESPACE_STAGE,
        nonce="a" * 64,
        payload_sha256="b" * 64,
        key_id="daily-v1",
    )
    assert envelope == {
        "version": 1,
        "operation": "sign",
        "namespace": DAILY_RECEIPT_NAMESPACE_STAGE,
        "nonce": "a" * 64,
        "payload_sha256": "b" * 64,
        "key_id": "daily-v1",
    }
    assert frozenset(
        {DAILY_RECEIPT_NAMESPACE_STAGE, DAILY_RECEIPT_NAMESPACE_RUN}
    ) == DAILY_RECEIPT_NAMESPACES

    for overrides, expected in (
        ({"namespace": "rquant-shadow-report-receipt"}, "namespace"),
        ({"nonce": "not-a-nonce"}, "nonce"),
        ({"payload_sha256": "b" * 63}, "payload hash"),
        ({"key_id": "Daily-V1"}, "key id"),
    ):
        arguments = {
            "namespace": DAILY_RECEIPT_NAMESPACE_STAGE,
            "nonce": "a" * 64,
            "payload_sha256": "b" * 64,
            "key_id": "daily-v1",
            **overrides,
        }
        with pytest.raises(DailyReceiptSocketAuthorityError, match=expected):
            daily_receipt_socket_signature_envelope(**arguments)


def test_identity_envelope_binds_protocol_nonce_source_and_key() -> None:
    envelope = daily_receipt_socket_identity_envelope(
        nonce="a" * 64,
        source_sha256="b" * 64,
        key_id="daily-v1",
    )
    assert envelope == {
        "version": 1,
        "operation": "identity",
        "protocol": DAILY_RECEIPT_IDENTITY_PROTOCOL,
        "nonce": "a" * 64,
        "source_sha256": "b" * 64,
        "key_id": "daily-v1",
    }


def test_nonce_store_claims_each_nonce_exactly_once(tmp_path: Path) -> None:
    store = _DailyReceiptNonceStore(root=tmp_path / "nonce-state")

    store.claim("a" * 64, envelope_hash="b" * 64)
    with pytest.raises(DailyReceiptSocketAuthorityError, match="replay"):
        store.claim("a" * 64, envelope_hash="b" * 64)
    # Re-binding the same nonce to a different envelope is still a replay.
    with pytest.raises(DailyReceiptSocketAuthorityError, match="replay"):
        store.claim("a" * 64, envelope_hash="c" * 64)

    store.claim("d" * 64, envelope_hash="b" * 64)
    with pytest.raises(DailyReceiptSocketAuthorityError, match="invalid"):
        store.claim("not-a-nonce", envelope_hash="b" * 64)


def test_production_client_pins_the_fixed_socket_and_daily_namespaces(
    tmp_path: Path,
) -> None:
    client = DailyReceiptSocketSigningClient.production(
        active_key_id="daily-v1",
        active_public_key_pem="public",
        timeout_seconds=5,
    )

    assert client.socket_path == DAILY_RECEIPT_SOCKET_ENDPOINT
    assert Path(
        "/etc/rquant/daily-receipt-trusted-keys.json"
    ) == DAILY_RECEIPT_TRUSTED_KEYRING_PATH
    with pytest.raises(ValueError, match="namespace"):
        client.sign(namespace="rquant-shadow-report-receipt", payload=b"payload")

    with pytest.raises(ValueError, match="fixed"):
        DailyReceiptSocketSigningClient(
            socket_path=tmp_path / "daily-receipt-signer.sock",
            active_key_id="daily-v1",
            active_public_key_pem="public",
            timeout_seconds=5,
        )
    with pytest.raises(ValueError, match="previous"):
        DailyReceiptSocketSigningClient.production(
            active_key_id="daily-v1",
            active_public_key_pem="public",
            previous_public_key_pems={"daily-v1": "public"},
            timeout_seconds=5,
        )
