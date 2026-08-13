from __future__ import annotations

import base64
import shutil
import sqlite3
import subprocess
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from rquant.paper_broker import paper_ledger_financial_state_digest
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


@dataclass(frozen=True)
class PaperLedgerTestAuthority:
    ledger_id: str
    private_key: Path
    verifier: Ed25519PaperLedgerAnchorVerifier

    def write_current_anchor(
        self,
        database: Path,
        anchor_path: Path,
        *,
        issued_at: datetime,
    ) -> PaperLedgerAnchor:
        with sqlite3.connect(database) as connection:
            connection.row_factory = sqlite3.Row
            migration = connection.execute(
                "SELECT migration_attestation_digest "
                "FROM paper_ledger_migration_attestation WHERE singleton = 1"
            ).fetchone()
            head = connection.execute(
                """
                SELECT revision, head_marker_fingerprint, attestation_fingerprint
                FROM paper_ledger_head_marker ORDER BY revision DESC LIMIT 1
                """
            ).fetchone()
            financial_state_digest = paper_ledger_financial_state_digest(connection)
        if migration is None or head is None:
            raise AssertionError("paper ledger current head is unavailable")
        claims = PaperLedgerAnchorClaims(
            ledger_id=self.ledger_id,
            schema_version=5,
            migration_attestation_digest=str(migration[0]),
            head_revision=int(head["revision"]),
            head_marker_fingerprint=str(head["head_marker_fingerprint"]),
            attestation_fingerprint=str(head["attestation_fingerprint"]),
            financial_state_digest=financial_state_digest,
            key_id="paper-ledger-test-v1",
            issued_at=issued_at,
        )
        payload_path = self.private_key.parent / "paper-ledger-anchor.payload"
        signature_path = self.private_key.parent / "paper-ledger-anchor.signature"
        payload_path.write_bytes(paper_ledger_anchor_signing_payload(claims))
        signed = subprocess.run(
            (
                _openssl(),
                "pkeyutl",
                "-sign",
                "-inkey",
                str(self.private_key),
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
        anchor = PaperLedgerAnchor(
            claims=claims,
            signature=base64.b64encode(signature_path.read_bytes()).decode("ascii"),
        )
        anchor_path.write_text(anchor.model_dump_json(), encoding="utf-8")
        return anchor


def create_paper_ledger_test_authority(
    root: Path,
    *,
    as_of: datetime,
    max_age: timedelta,
    future_skew: timedelta,
    ledger_id: str = "paper-ledger-test",
) -> PaperLedgerTestAuthority:
    root.mkdir(parents=True, exist_ok=True)
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
    private_key.chmod(0o600)
    verifier = Ed25519PaperLedgerAnchorVerifier(
        active_key_id="paper-ledger-test-v1",
        active_public_key=public_key.read_bytes(),
        allowed_ledger_id=ledger_id,
        max_age=max_age,
        future_skew=future_skew,
        clock=lambda: as_of,
    )
    return PaperLedgerTestAuthority(
        ledger_id=ledger_id,
        private_key=private_key,
        verifier=verifier,
    )
