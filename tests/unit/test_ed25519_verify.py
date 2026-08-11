from __future__ import annotations

import base64
import hashlib

import pytest

from rquant.ed25519_verify import (
    Ed25519VerificationError,
    ed25519_public_key_der,
    verify_ed25519_signature,
)

_PUBLIC_KEY = bytes.fromhex("d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a")
_SIGNATURE = bytes.fromhex(
    "e5564300c360ac729086e2cc806e828a"
    "84877f1eb8e5d974d873e06522490155"
    "5fb8821590a33bacc61e39701cf9b46b"
    "d25bf5f0595bbe24655141438e7a100b"
)
_DER = bytes.fromhex("302a300506032b6570032100") + _PUBLIC_KEY
_PEM = b"-----BEGIN PUBLIC KEY-----\n" + base64.b64encode(_DER) + b"\n-----END PUBLIC KEY-----\n"


def test_rfc8032_vector_is_verified_entirely_in_process() -> None:
    assert ed25519_public_key_der(_PEM) == _DER
    assert (
        hashlib.sha256(ed25519_public_key_der(_PEM)).hexdigest() == hashlib.sha256(_DER).hexdigest()
    )
    assert verify_ed25519_signature(
        public_key_pem=_PEM,
        message=b"",
        signature=_SIGNATURE,
    )


def test_ed25519_verifier_rejects_noncanonical_or_modified_inputs() -> None:
    changed = bytearray(_SIGNATURE)
    changed[0] ^= 1
    assert not verify_ed25519_signature(
        public_key_pem=_PEM,
        message=b"",
        signature=bytes(changed),
    )
    with pytest.raises(Ed25519VerificationError, match="canonical Ed25519"):
        ed25519_public_key_der(_PEM.replace(b"PUBLIC KEY", b"ED25519 KEY"))
