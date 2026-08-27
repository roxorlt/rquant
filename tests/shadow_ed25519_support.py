from __future__ import annotations

import base64
import os
import shutil
import subprocess
import tempfile
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path

import pytest

from rquant.runtime_shadow_validation import (
    Ed25519ShadowReceiptKeyring,
    Ed25519ShadowReceiptSigner,
)


class OpenSslSigningClient:
    def __init__(self, private_key_path: Path) -> None:
        self._private_key_path = private_key_path

    def sign(self, *, namespace: str, payload: bytes) -> str:
        del namespace  # the scratch files no longer take their name from it
        # Private to this call, and removed after it. Paths derived from the key are
        # shared by every holder of that key, and a lock inside one process does not
        # reach another: two signers then overwrite each other's scratch files and one
        # reads back a signature over the other's payload. See 064ffe8, where that cost
        # four spawned competitors an invalid signature about once in twenty runs.
        scratch = self._private_key_path.parent
        payload_descriptor, payload_name = tempfile.mkstemp(dir=scratch, suffix=".payload")
        signature_descriptor, signature_name = tempfile.mkstemp(dir=scratch, suffix=".signature")
        payload_path = Path(payload_name)
        signature_path = Path(signature_name)
        try:
            os.close(signature_descriptor)
            with os.fdopen(payload_descriptor, "wb") as handle:
                handle.write(payload)
            completed = subprocess.run(
                (
                    _openssl(),
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
                capture_output=True,
            )
            if completed.returncode != 0:
                raise RuntimeError(completed.stderr.decode("utf-8", errors="replace"))
            return base64.b64encode(signature_path.read_bytes()).decode("ascii")
        finally:
            for path in (payload_path, signature_path):
                with suppress(OSError):
                    path.unlink()


@dataclass(frozen=True)
class ShadowEd25519TestAuthority:
    signer: Ed25519ShadowReceiptSigner
    keyring: Ed25519ShadowReceiptKeyring


@dataclass(frozen=True)
class RotatingShadowEd25519TestAuthority:
    active_signer: Ed25519ShadowReceiptSigner
    previous_signer: Ed25519ShadowReceiptSigner
    previous_keyring: Ed25519ShadowReceiptKeyring
    keyring: Ed25519ShadowReceiptKeyring


def _openssl() -> str:
    executable = shutil.which("openssl")
    if executable is None:
        pytest.skip("openssl is required for Shadow Ed25519 tests")
    return executable


def _create_signer(root: Path, *, key_id: str) -> tuple[Ed25519ShadowReceiptSigner, bytes]:
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
    signer = Ed25519ShadowReceiptSigner(key_id=key_id, client=OpenSslSigningClient(private_key))
    return signer, public_key.read_bytes()


def create_shadow_ed25519_test_authority(root: Path) -> ShadowEd25519TestAuthority:
    signer, public_key = _create_signer(root, key_id="shadow-test-v1")
    keyring = Ed25519ShadowReceiptKeyring(
        active_key_id="shadow-test-v1",
        active_public_key=public_key,
    )
    return ShadowEd25519TestAuthority(signer=signer, keyring=keyring)


def create_rotating_shadow_ed25519_test_authority(
    root: Path,
) -> RotatingShadowEd25519TestAuthority:
    previous_signer, previous_public_key = _create_signer(root, key_id="shadow-test-v1")
    active_signer, active_public_key = _create_signer(root, key_id="shadow-test-v2")
    keyring = Ed25519ShadowReceiptKeyring(
        active_key_id="shadow-test-v2",
        active_public_key=active_public_key,
        previous_public_keys={"shadow-test-v1": previous_public_key},
    )
    return RotatingShadowEd25519TestAuthority(
        active_signer=active_signer,
        previous_signer=previous_signer,
        previous_keyring=Ed25519ShadowReceiptKeyring(
            active_key_id="shadow-test-v1",
            active_public_key=previous_public_key,
        ),
        keyring=keyring,
    )
