"""Public verification for externally anchored paper-ledger heads."""

from __future__ import annotations

import base64
import hashlib
import shutil
import subprocess
import tempfile
from collections.abc import Callable
from datetime import datetime, timedelta
from pathlib import Path

from rquant.paper_contracts import PaperLedgerAnchor, PaperLedgerAnchorClaims
from rquant.runtime_contracts import normalize_aware_utc
from rquant.strict_json import canonical_json_bytes

_ANCHOR_NAMESPACE = "rquant-paper-ledger-head"
_SIGNATURE_BYTES = 64


def paper_ledger_anchor_signing_payload(claims: PaperLedgerAnchorClaims) -> bytes:
    payload = canonical_json_bytes(claims.model_dump(mode="json"))
    return canonical_json_bytes(
        {
            "contract": "rquant-ed25519-domain-separation/v1",
            "namespace": _ANCHOR_NAMESPACE,
            "payload_sha256": hashlib.sha256(payload).hexdigest(),
        }
    )


def _openssl_binary() -> str:
    for candidate in ("/opt/homebrew/bin/openssl", "/usr/bin/openssl", shutil.which("openssl")):
        if candidate and Path(candidate).is_file():
            return candidate
    raise ValueError("openssl is required for paper ledger anchor verification")


def _validate_public_key(public_key: bytes) -> None:
    try:
        completed = subprocess.run(
            (
                _openssl_binary(),
                "pkey",
                "-pubin",
                "-pubcheck",
                "-text_pub",
                "-noout",
            ),
            input=public_key,
            check=False,
            capture_output=True,
            timeout=5.0,
        )
    except (OSError, subprocess.TimeoutExpired, ValueError) as exc:
        raise ValueError("paper ledger anchor public key is not usable") from exc
    if completed.returncode or b"ED25519" not in completed.stdout.upper():
        raise ValueError("paper ledger anchor public key is not Ed25519")


def _verify_signature(*, public_key: bytes, payload: bytes, signature: str) -> bool:
    try:
        decoded = base64.b64decode(signature, validate=True)
    except (TypeError, ValueError):
        return False
    if len(decoded) != _SIGNATURE_BYTES:
        return False
    try:
        with tempfile.TemporaryDirectory(prefix="rquant-paper-anchor-") as directory_name:
            root = Path(directory_name)
            root.chmod(0o700)
            public_path = root / "public.pem"
            payload_path = root / "payload.bin"
            signature_path = root / "signature.bin"
            public_path.write_bytes(public_key)
            payload_path.write_bytes(payload)
            signature_path.write_bytes(decoded)
            for path in (public_path, payload_path, signature_path):
                path.chmod(0o600)
            completed = subprocess.run(
                (
                    _openssl_binary(),
                    "pkeyutl",
                    "-verify",
                    "-pubin",
                    "-inkey",
                    str(public_path),
                    "-sigfile",
                    str(signature_path),
                    "-rawin",
                    "-in",
                    str(payload_path),
                ),
                check=False,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=5.0,
            )
    except (OSError, subprocess.TimeoutExpired, ValueError):
        return False
    return completed.returncode == 0


class Ed25519PaperLedgerAnchorVerifier:
    """Pinned public-key verifier for one paper-ledger identity."""

    def __init__(
        self,
        *,
        active_key_id: str,
        active_public_key: bytes,
        allowed_ledger_id: str,
        max_age: timedelta,
        future_skew: timedelta,
        clock: Callable[[], datetime],
    ) -> None:
        if not active_key_id.strip() or any(character.isspace() for character in active_key_id):
            raise ValueError("paper ledger anchor key id is invalid")
        if not allowed_ledger_id.strip():
            raise ValueError("paper ledger anchor ledger id is invalid")
        if not isinstance(active_public_key, bytes) or not active_public_key:
            raise ValueError("paper ledger anchor public key is invalid")
        if not isinstance(max_age, timedelta) or max_age <= timedelta(0):
            raise ValueError("paper ledger anchor max_age must be positive")
        if not isinstance(future_skew, timedelta) or future_skew < timedelta(0):
            raise ValueError("paper ledger anchor future_skew cannot be negative")
        if not callable(clock):
            raise ValueError("paper ledger anchor clock must be callable")
        _validate_public_key(active_public_key)
        self.active_key_id = active_key_id
        self.allowed_ledger_id = allowed_ledger_id
        self.max_age = max_age
        self.future_skew = future_skew
        self._clock = clock
        self._active_public_key = active_public_key

    def verify(self, anchor: PaperLedgerAnchor) -> bool:
        claims = anchor.claims
        if claims.key_id != self.active_key_id or claims.ledger_id != self.allowed_ledger_id:
            return False
        try:
            as_of = normalize_aware_utc(self._clock())
        except (AttributeError, TypeError, ValueError):
            return False
        if claims.issued_at > as_of + self.future_skew or as_of - claims.issued_at > self.max_age:
            return False
        return _verify_signature(
            public_key=self._active_public_key,
            payload=paper_ledger_anchor_signing_payload(claims),
            signature=anchor.signature,
        )


__all__ = [
    "Ed25519PaperLedgerAnchorVerifier",
    "PaperLedgerAnchor",
    "PaperLedgerAnchorClaims",
    "paper_ledger_anchor_signing_payload",
]
