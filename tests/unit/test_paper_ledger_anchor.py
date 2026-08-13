from __future__ import annotations

import base64
import shutil
import subprocess
from datetime import UTC, datetime, timedelta
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


_NOW = datetime(2026, 8, 14, 1, 2, tzinfo=UTC)
_MAX_AGE = timedelta(minutes=5)
_FUTURE_SKEW = timedelta(seconds=30)


def _anchor(
    private_key: Path,
    *,
    issued_at: datetime,
) -> PaperLedgerAnchor:
    claims = PaperLedgerAnchorClaims(
        ledger_id="paper-ledger-test",
        schema_version=5,
        migration_attestation_digest="a" * 64,
        head_revision=7,
        head_marker_fingerprint="b" * 64,
        attestation_fingerprint="c" * 64,
        key_id="paper-ledger-test-v1",
        issued_at=issued_at,
    )
    return PaperLedgerAnchor(
        claims=claims,
        signature=_sign(private_key, paper_ledger_anchor_signing_payload(claims)),
    )


def _verifier(public_key: bytes) -> Ed25519PaperLedgerAnchorVerifier:
    return Ed25519PaperLedgerAnchorVerifier(
        active_key_id="paper-ledger-test-v1",
        active_public_key=public_key,
        allowed_ledger_id="paper-ledger-test",
        max_age=_MAX_AGE,
        future_skew=_FUTURE_SKEW,
        clock=lambda: _NOW,
    )


def test_ed25519_paper_ledger_anchor_binds_the_complete_current_head(tmp_path: Path) -> None:
    private_key, public_key = _keypair(tmp_path)
    anchor = _anchor(private_key, issued_at=_NOW)
    claims = anchor.claims
    verifier = _verifier(public_key)

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


@pytest.mark.parametrize(
    ("issued_at", "expected"),
    (
        (_NOW - timedelta(minutes=1), True),
        (_NOW - _MAX_AGE, True),
        (_NOW - _MAX_AGE - timedelta(microseconds=1), False),
        (_NOW + _FUTURE_SKEW, True),
        (_NOW + _FUTURE_SKEW + timedelta(microseconds=1), False),
    ),
    ids=("fresh", "oldest-boundary", "stale", "future-boundary", "future"),
)
def test_ed25519_paper_ledger_anchor_enforces_explicit_freshness_policy(
    tmp_path: Path,
    issued_at: datetime,
    expected: bool,
) -> None:
    private_key, public_key = _keypair(tmp_path)

    assert _verifier(public_key).verify(_anchor(private_key, issued_at=issued_at)) is expected


def test_ed25519_paper_ledger_anchor_requires_positive_explicit_max_age(
    tmp_path: Path,
) -> None:
    _private_key, public_key = _keypair(tmp_path)

    with pytest.raises(ValueError, match="max_age"):
        Ed25519PaperLedgerAnchorVerifier(
            active_key_id="paper-ledger-test-v1",
            active_public_key=public_key,
            allowed_ledger_id="paper-ledger-test",
            max_age=timedelta(0),
            future_skew=_FUTURE_SKEW,
            clock=lambda: _NOW,
        )
