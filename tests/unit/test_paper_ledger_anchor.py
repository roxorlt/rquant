from __future__ import annotations

import base64
import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest

from rquant.paper_ledger_anchor import (
    Ed25519PaperLedgerAnchorVerifier,
    PaperLedgerAnchor,
    PaperLedgerAnchorClaims,
    paper_ledger_anchor_signing_payload,
)


def _openssl() -> str:
    executable = shutil.which("openssl")
    if executable is None:
        pytest.skip("openssl is required for paper ledger anchor tests")
    return executable


def _keypair(root: Path) -> tuple[Path, bytes]:
    private_key = root / "paper-ledger.private.pem"
    public_key = root / "paper-ledger.public.pem"
    generated = subprocess.run(
        (_openssl(), "genpkey", "-algorithm", "ED25519", "-out", str(private_key)),
        check=False,
        capture_output=True,
    )
    if generated.returncode:
        raise RuntimeError(generated.stderr.decode("utf-8", errors="replace"))
    exported = subprocess.run(
        (_openssl(), "pkey", "-in", str(private_key), "-pubout", "-out", str(public_key)),
        check=False,
        capture_output=True,
    )
    if exported.returncode:
        raise RuntimeError(exported.stderr.decode("utf-8", errors="replace"))
    return private_key, public_key.read_bytes()


def _sign(private_key: Path, payload: bytes) -> str:
    payload_path = private_key.parent / "anchor.payload"
    signature_path = private_key.parent / "anchor.signature"
    payload_path.write_bytes(payload)
    signed = subprocess.run(
        (
            _openssl(),
            "pkeyutl",
            "-sign",
            "-inkey",
            str(private_key),
            "-rawin",
            "-in",
            str(payload_path),
            "-out",
            str(signature_path),
        ),
        check=False,
        capture_output=True,
    )
    if signed.returncode:
        raise RuntimeError(signed.stderr.decode("utf-8", errors="replace"))
    return base64.b64encode(signature_path.read_bytes()).decode("ascii")


def test_ed25519_paper_ledger_anchor_binds_the_complete_current_head(tmp_path: Path) -> None:
    private_key, public_key = _keypair(tmp_path)
    claims = PaperLedgerAnchorClaims(
        ledger_id="paper-ledger-test",
        schema_version=5,
        migration_attestation_digest="a" * 64,
        head_revision=7,
        head_marker_fingerprint="b" * 64,
        attestation_fingerprint="c" * 64,
        key_id="paper-ledger-test-v1",
        issued_at=datetime(2026, 8, 14, 1, 2, tzinfo=UTC),
    )
    anchor = PaperLedgerAnchor(
        claims=claims,
        signature=_sign(private_key, paper_ledger_anchor_signing_payload(claims)),
    )
    verifier = Ed25519PaperLedgerAnchorVerifier(
        active_key_id="paper-ledger-test-v1",
        active_public_key=public_key,
        allowed_ledger_id="paper-ledger-test",
    )

    assert verifier.verify(anchor)
    assert not verifier.verify(
        anchor.model_copy(
            update={"claims": claims.model_copy(update={"head_marker_fingerprint": "d" * 64})}
        )
    )
    assert not verifier.verify(
        anchor.model_copy(
            update={"claims": claims.model_copy(update={"migration_attestation_digest": "e" * 64})}
        )
    )
    assert not verifier.verify(anchor.model_copy(update={"signature": "not-base64"}))
