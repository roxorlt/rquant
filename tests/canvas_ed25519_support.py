from __future__ import annotations

import base64
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

import pytest

from rquant.canvas_publication_receipt import (
    Ed25519CanvasPublicationKeyring,
    Ed25519CanvasPublicationSigner,
)


class OpenSslCanvasSigningClient:
    def __init__(self, private_key_path: Path) -> None:
        self._private_key_path = private_key_path

    def sign(self, *, namespace: str, payload: bytes) -> str:
        payload_path = self._private_key_path.parent / f"{namespace}.payload"
        signature_path = self._private_key_path.parent / f"{namespace}.signature"
        payload_path.write_bytes(payload)
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


@dataclass(frozen=True)
class CanvasEd25519TestAuthority:
    signer: Ed25519CanvasPublicationSigner
    keyring: Ed25519CanvasPublicationKeyring


@dataclass(frozen=True)
class RotatingCanvasEd25519TestAuthority:
    active_signer: Ed25519CanvasPublicationSigner
    previous_signer: Ed25519CanvasPublicationSigner
    previous_keyring: Ed25519CanvasPublicationKeyring
    keyring: Ed25519CanvasPublicationKeyring


def _openssl() -> str:
    executable = shutil.which("openssl")
    if executable is None:
        pytest.skip("openssl is required for Canvas Ed25519 tests")
    return executable


def _create_signer(
    root: Path,
    *,
    key_id: str,
) -> tuple[Ed25519CanvasPublicationSigner, bytes]:
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
    signer = Ed25519CanvasPublicationSigner(
        key_id=key_id,
        client=OpenSslCanvasSigningClient(private_key),
    )
    return signer, public_key.read_bytes()


def create_canvas_ed25519_test_authority(root: Path) -> CanvasEd25519TestAuthority:
    signer, public_key = _create_signer(root, key_id="canvas-test-v1")
    keyring = Ed25519CanvasPublicationKeyring(
        active_key_id="canvas-test-v1",
        active_public_key=public_key,
    )
    return CanvasEd25519TestAuthority(signer=signer, keyring=keyring)


def create_rotating_canvas_ed25519_test_authority(
    root: Path,
) -> RotatingCanvasEd25519TestAuthority:
    previous_signer, previous_public_key = _create_signer(root, key_id="canvas-test-v1")
    active_signer, active_public_key = _create_signer(root, key_id="canvas-test-v2")
    keyring = Ed25519CanvasPublicationKeyring(
        active_key_id="canvas-test-v2",
        active_public_key=active_public_key,
        previous_public_keys={"canvas-test-v1": previous_public_key},
    )
    return RotatingCanvasEd25519TestAuthority(
        active_signer=active_signer,
        previous_signer=previous_signer,
        previous_keyring=Ed25519CanvasPublicationKeyring(
            active_key_id="canvas-test-v1",
            active_public_key=previous_public_key,
        ),
        keyring=keyring,
    )
