from __future__ import annotations

import base64
import hashlib
import shutil
import subprocess
import threading
from pathlib import Path

import pytest

from rquant.adapter_manifest import (
    Ed25519ContractSigner,
    Ed25519PublicKeyRecord,
    KeyPurpose,
    VerifyOnlyEd25519Keyring,
)


class OpenSslContractSigningClient:
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
            raise ValueError("test signer purpose or namespace mismatch")
        with self._lock:
            suffix = hashlib.sha256(namespace.encode("ascii")).hexdigest()[:12]
            payload_path = self._private_key.with_suffix(f".{suffix}.payload")
            signature_path = self._private_key.with_suffix(f".{suffix}.signature")
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
            return base64.b64encode(signature_path.read_bytes()).decode("ascii")


def _openssl() -> str:
    executable = shutil.which("openssl")
    if executable is None:
        pytest.skip("openssl is required for runtime code trust tests")
    return executable


def contract_key_pair(
    root: Path,
    *,
    key_id: str,
    issuer: str,
    key_purpose: KeyPurpose,
    namespace: str,
    rotation: str = "active",
) -> tuple[Ed25519ContractSigner, Ed25519PublicKeyRecord, VerifyOnlyEd25519Keyring]:
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
    from rquant.adapter_manifest import ed25519_public_key_fingerprint

    public_key_bytes = public_key.read_bytes()
    record = Ed25519PublicKeyRecord(
        key_id=key_id,
        issuer=issuer,
        key_purpose=key_purpose,
        rotation=rotation,
        public_key_pem=public_key_bytes,
    )
    signer = Ed25519ContractSigner(
        key_id=key_id,
        issuer=issuer,
        key_purpose=key_purpose,
        client=OpenSslContractSigningClient(
            private_key,
            key_purpose=key_purpose,
            allowed_namespaces=frozenset({namespace}),
            public_key_fingerprint=ed25519_public_key_fingerprint(public_key_bytes),
        ),
    )
    keyring = VerifyOnlyEd25519Keyring(
        records=(record,),
        issuer_allowlist={key_purpose: frozenset({issuer})},
        rotation_allowlist={(issuer, key_purpose): frozenset({key_id})},
    )
    return signer, record, keyring
