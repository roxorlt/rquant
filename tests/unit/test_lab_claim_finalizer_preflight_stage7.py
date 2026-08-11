from __future__ import annotations

from datetime import UTC, datetime, timedelta


def test_finalizer_preflight_has_dependency_matrix_and_explicit_replica_skip() -> None:
    from rquant.lab_claim_finalizer_runtime import (
        FinalizerPreflightInputs,
        run_lab_claim_finalizer_preflight,
    )

    disabled = run_lab_claim_finalizer_preflight(
        FinalizerPreflightInputs(finalizer_enabled=False, v2_workers_enabled=False)
    )
    assert {check.status for check in disabled.checks} == {"skip"}

    report = run_lab_claim_finalizer_preflight(
        FinalizerPreflightInputs(
            finalizer_enabled=True,
            v2_workers_enabled=True,
            schema_version=16,
            certificate_valid=True,
            database_generation_matches=True,
            private_public_matches=True,
            filesystem_secure=True,
            unix_peer_secure=True,
            composition_valid=True,
            worker_verify_only=True,
            scheduler_has_no_secret=True,
            scheduler_sqlite_read_only=True,
            duckdb_dependency=False,
            readonly_replica_dependency=False,
            rotation_expires_at=datetime.now(UTC) + timedelta(hours=6),
            outbox_backlog=4,
            retry_latency_seconds=2,
        )
    )
    assert report.status == "ok"
    assert report.by_name("duckdb").status == "skip"
    assert report.by_name("readonly_replica").status == "skip"


def test_runtime_preflight_script_accepts_the_dedicated_finalizer_role() -> None:
    from pathlib import Path

    script = Path(__file__).resolve().parents[2] / "scripts" / "preflight-lab-runtime.py"
    assert '"lab-claim-finalizer"' in script.read_text(encoding="utf-8")
