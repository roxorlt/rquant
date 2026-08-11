from __future__ import annotations

import base64
import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

import rquant.legacy_shadow_export as legacy_shadow_export_module
from rquant.legacy_shadow_export import (
    Ed25519LegacyShadowRecoveryKeyring,
    Ed25519LegacyShadowRecoverySigner,
    LegacyShadowExportManifest,
    LegacyShadowExportUnavailableError,
    LegacyShadowRecoveryMarker,
    LegacyShadowRecoveryResumeBinding,
    LegacyShadowTestDependencies,
    legacy_shadow_test_filesystem_policy,
    publish_legacy_monitor_export,
    recover_legacy_shadow_export,
)
from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_shadow_sources import legacy_records_raw_input_id
from rquant.runtime_shadow_validation import (
    CompletionAttestationClaims,
    Ed25519CompletionAttestationKeyring,
    Ed25519CompletionAttestationSigner,
    SecureShadowSigningClient,
    ShadowSourceCompletionReceipt,
    shadow_completion_receipt_body_sha256,
    verify_completion_attestation,
)
from rquant.strict_json import canonical_json_bytes

COMMIT = "a" * 40


class _OpenSslSigningClient:
    """Test-only stand-in for the future protected credential client."""

    def __init__(self, private_key_path: Path) -> None:
        self._private_key_path = private_key_path

    def sign(self, *, namespace: str, payload: bytes) -> str:
        assert namespace.startswith("rquant-shadow-")
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
        assert completed.returncode == 0, completed.stderr.decode("utf-8", errors="replace")
        return base64.b64encode(signature_path.read_bytes()).decode("ascii")


def _openssl() -> str:
    executable = shutil.which("openssl")
    if executable is None:
        pytest.skip("openssl is required for Ed25519 shadow attestation tests")
    return executable


def _keypair(root: Path, key_id: str) -> tuple[Path, bytes]:
    private_key = root / f"{key_id}.private.pem"
    public_key = root / f"{key_id}.public.pem"
    created = subprocess.run(
        (_openssl(), "genpkey", "-algorithm", "ED25519", "-out", str(private_key)),
        check=False,
        capture_output=True,
    )
    assert created.returncode == 0, created.stderr.decode("utf-8", errors="replace")
    exported = subprocess.run(
        (_openssl(), "pkey", "-in", str(private_key), "-pubout", "-out", str(public_key)),
        check=False,
        capture_output=True,
    )
    assert exported.returncode == 0, exported.stderr.decode("utf-8", errors="replace")
    private_key.chmod(0o600)
    return private_key, public_key.read_bytes()


def _write_protected_signer_fixture(
    root: Path,
    *,
    key_manifest: Path,
    trusted_clock: dict[str, object] | None = None,
    trusted_clocks: tuple[dict[str, object], ...] | None = None,
    legacy_shadow_root: Path | None = None,
    recovery_state_root: Path | None = None,
    fault_point: str | None = None,
    kill_fault_point: str | None = None,
) -> Path:
    """Simulate the root helper boundary without passing a private-key path to a client."""
    if trusted_clock is not None and trusted_clocks is not None:
        raise ValueError("root signer fixture accepts one trusted clock source")
    helper = root / "shadow-signer-root-fixture.py"
    source = Path(__file__).resolve().parents[2] / "deploy/libexec/rquant-shadow-report-signer"
    helper.write_text(
        "\n".join(
            (
                "from __future__ import annotations",
                "from importlib.machinery import SourceFileLoader",
                "import importlib.util",
                "import sys",
                "from pathlib import Path",
                f"source = Path({str(source)!r})",
                f"keys_file = Path({str(key_manifest)!r})",
                "loader = SourceFileLoader('shadow_signer_fixture', str(source))",
                "spec = importlib.util.spec_from_loader('shadow_signer_fixture', loader)",
                "if spec is None or spec.loader is None: raise RuntimeError('fixture load failed')",
                "module = importlib.util.module_from_spec(spec)",
                "loader.exec_module(module)",
                "module.KEYS_FILE = keys_file",
                *(
                    (f"module.LEGACY_SHADOW_ROOT = Path({str(legacy_shadow_root)!r})",)
                    if legacy_shadow_root is not None
                    else ()
                ),
                *(
                    (f"module.RECOVERY_STATE_ROOT = Path({str(recovery_state_root)!r})",)
                    if recovery_state_root is not None
                    else ()
                ),
                *(
                    (
                        f"trusted_clock = {trusted_clock!r}",
                        "module._trusted_clock_snapshot = lambda: dict(trusted_clock)",
                    )
                    if trusted_clock is not None
                    else ()
                ),
                *(
                    (
                        f"trusted_clocks = iter({trusted_clocks!r})",
                        "module._trusted_clock_snapshot = lambda: dict(next(trusted_clocks))",
                    )
                    if trusted_clocks is not None
                    else ()
                ),
                *(
                    (
                        f"fault_point = {fault_point!r}",
                        "def injected_fault(point):",
                        "    if point == fault_point:",
                        "        raise module.ShadowSignerError(f'injected fault: {point}')",
                        "module._test_fault = injected_fault",
                    )
                    if fault_point is not None
                    else ()
                ),
                *(
                    (
                        "import os",
                        "import signal",
                        f"kill_fault_point = {kill_fault_point!r}",
                        "def kill_process(point):",
                        "    if point == kill_fault_point:",
                        "        os.kill(os.getpid(), signal.SIGKILL)",
                        "module._test_fault = kill_process",
                    )
                    if kill_fault_point is not None
                    else ()
                ),
                "try:",
                "    raise SystemExit(module.main(sys.argv[1:]))",
                "except module.ShadowSignerError as exc:",
                "    print(f'Shadow signer rejected request: {exc}', file=sys.stderr)",
                "    raise SystemExit(2) from exc",
            )
            + ("",),
        ),
        encoding="utf-8",
    )
    helper.chmod(0o700)
    return helper


def _write_recovery_calendar(root: Path) -> Path:
    calendar = root / "shadow-recovery-calendar.json"
    body = {
        "schema_version": 1,
        "exchange": "SSE",
        "coverage_start": "2026-08-01",
        "coverage_end": "2026-08-31",
        "open_dates": ["2026-08-03"],
    }
    calendar.write_bytes(canonical_json_bytes({**body, "content_sha256": canonical_sha256(body)}))
    calendar.chmod(0o600)
    return calendar


def _write_shadow_key_manifest(
    root: Path,
    *,
    private_key: Path,
    key_id: str,
    recovery_calendar: Path,
) -> Path:
    manifest = root / "shadow-report-keys.json"
    manifest.write_bytes(
        canonical_json_bytes(
            {
                "schema_version": 2,
                "active_key_id": key_id,
                "active_private_key_path": str(private_key),
                "previous_public_keys": {},
                "legacy_recovery_calendar_path": str(recovery_calendar),
            }
        )
    )
    manifest.chmod(0o600)
    return manifest


def _protected_recovery_request(
    *,
    operation: str,
    key_id: str,
    payload: dict[str, object],
) -> bytes:
    encoded = canonical_json_bytes(payload)
    payload_sha256 = hashlib.sha256(encoded).hexdigest()
    request_id = canonical_sha256(
        {
            "contract": "runtime-shadow-signing-request/v2",
            "operation": operation,
            "key_id": key_id,
            "namespace": "rquant-legacy-shadow-recovery-marker",
            "payload_sha256": payload_sha256,
        }
    )
    return canonical_json_bytes(
        {
            "schema_version": 2,
            "operation": operation,
            "request_id": request_id,
            "key_id": key_id,
            "namespace": "rquant-legacy-shadow-recovery-marker",
            "payload_base64": base64.b64encode(encoded).decode("ascii"),
            "payload_sha256": payload_sha256,
        }
    )


def _invoke_recovery_helper(
    helper: Path,
    *,
    operation: str,
    key_id: str,
    payload: dict[str, object],
) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        (sys.executable, str(helper)),
        input=_protected_recovery_request(
            operation=operation,
            key_id=key_id,
            payload=payload,
        ),
        check=False,
        capture_output=True,
        timeout=5,
    )


def _capture_request() -> dict[str, object]:
    return {
        "contract": "rquant-legacy-shadow-recovery-capture-request/v1",
        "trade_date": "2026-08-03",
        "source_id": "legacy-monitor-events",
        "producer_commit": COMMIT,
        "producer_version": "legacy-monitor-shadow-export/v1",
        "staging_name": ".staging-2026-08-03-" + "1" * 32,
    }


def _finish_request(
    capture_response: dict[str, object],
    *,
    batch_digest: str = "2" * 64,
    input_identity: str = "1" * 64,
) -> dict[str, object]:
    return {
        "contract": "rquant-legacy-shadow-recovery-sign-request/v2",
        "capture": {
            "contract": "legacy-shadow-recovery-capture/v1",
            "key_id": capture_response["key_id"],
            "signature_algorithm": "ed25519",
            "claims_base64": capture_response["signed_payload_base64"],
            "signature": capture_response["signature"],
        },
        "claims": {
            **_capture_request(),
            "contract": "legacy-shadow-recovery-marker-draft/v2",
            "input_identity": input_identity,
            "batch_digest": batch_digest,
            "surge_collection_proof_id": None,
            "runner_manifest_binding_id": None,
        },
    }


def _transaction_id_from_capture_response(capture_response: dict[str, object]) -> str:
    capture = _finish_request(capture_response)["capture"]
    return canonical_sha256(
        {
            "contract": "legacy-shadow-recovery-transaction-identity/v1",
            "capture_token_id": canonical_sha256(capture),
        }
    )


def _replace_protected_json(path: Path, payload: bytes) -> None:
    path.chmod(0o600)
    path.write_bytes(payload)
    path.chmod(0o600)
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _resume_request() -> dict[str, object]:
    capture = _capture_request()
    return {
        "contract": "rquant-legacy-shadow-recovery-resume-request/v1",
        "trade_date": capture["trade_date"],
        "source_id": capture["source_id"],
        "producer_commit": capture["producer_commit"],
        "staging_name": capture["staging_name"],
    }


def _artifact_batch_digest(artifacts: dict[str, bytes]) -> str:
    digest = hashlib.sha256()
    for filename in sorted(artifacts):
        payload = artifacts[filename]
        digest.update(len(filename).to_bytes(4, "big"))
        digest.update(filename.encode("ascii"))
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(hashlib.sha256(payload).digest())
    return digest.hexdigest()


def _write_complete_monitor_staging(
    legacy_shadow_root: Path,
    capture_response: dict[str, object],
) -> tuple[Path, str, str]:
    capture_claims = json.loads(base64.b64decode(str(capture_response["signed_payload_base64"])))
    staging = legacy_shadow_root / "monitor" / str(capture_claims["staging_name"])
    staging.mkdir(mode=0o700, parents=True)
    captured_at = datetime.fromisoformat(str(capture_claims["captured_at"]).replace("Z", "+00:00"))
    session_close = datetime(2026, 8, 3, 7, 0, tzinfo=UTC)
    input_identity = legacy_records_raw_input_id(
        (),
        source_id="legacy-monitor-events",
        trade_date=date(2026, 8, 3),
    )
    receipt = ShadowSourceCompletionReceipt(
        evidence_origin="production",
        source="legacy",
        source_id="legacy-monitor-events",
        trade_date=date(2026, 8, 3),
        session_close_at=session_close,
        complete_through=session_close,
        input_identity=input_identity,
        produced_at=captured_at,
        producer_commit=COMMIT,
        producer_version="legacy-monitor-shadow-export/v1",
    )
    records_payload = canonical_json_bytes([])
    envelopes_payload = b""
    completion_payload = canonical_json_bytes(receipt.model_dump(mode="json"))
    manifest_values = {
        "contract": "legacy-shadow-export/v2",
        "source_id": "legacy-monitor-events",
        "trade_date": date(2026, 8, 3),
        "producer_commit": COMMIT,
        "producer_version": "legacy-monitor-shadow-export/v1",
        "captured_at": captured_at,
        "as_of": session_close,
        "records_filename": "events.json",
        "records_sha256": hashlib.sha256(records_payload).hexdigest(),
        "records_count": 0,
        "record_envelopes_sha256": hashlib.sha256(envelopes_payload).hexdigest(),
        "completion_sha256": hashlib.sha256(completion_payload).hexdigest(),
        "completion_receipt_id": str(receipt.receipt_id),
        "input_identity": input_identity,
        "surge_collection_proof": None,
        "runner_manifest_binding": None,
        "accepted": True,
    }
    manifest = LegacyShadowExportManifest(
        batch_id=canonical_sha256(manifest_values),
        **manifest_values,
    )
    artifacts = {
        "events.json": records_payload,
        "records.jsonl": envelopes_payload,
        "completion.json": completion_payload,
        "manifest.json": canonical_json_bytes(manifest.model_dump(mode="json")),
    }
    for filename, payload in artifacts.items():
        path = staging / filename
        path.write_bytes(payload)
        path.chmod(0o444)
        descriptor = os.open(path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    directory_descriptor = os.open(staging, os.O_RDONLY)
    try:
        os.fsync(directory_descriptor)
    finally:
        os.close(directory_descriptor)
    return staging, _artifact_batch_digest(artifacts), input_identity


def _initial_prepared_case(
    root: Path,
) -> tuple[str, Path, Path, Path, Path, Path]:
    key_id = "protected-shadow-v1"
    private_key, _public_key = _keypair(root, key_id)
    manifest = _write_shadow_key_manifest(
        root,
        private_key=private_key,
        key_id=key_id,
        recovery_calendar=_write_recovery_calendar(root),
    )
    legacy_shadow_root = root / "legacy-shadow"
    recovery_state_root = root / "recovery-state"
    legacy_shadow_root.mkdir(mode=0o700)
    recovery_state_root.mkdir(mode=0o700)
    capture_helper = _write_protected_signer_fixture(
        root,
        key_manifest=manifest,
        trusted_clock={
            "wallclock_at": "2026-08-03T07:04:58.000000Z",
            "monotonic_ns": 100_000_000_000,
            "boot_id": "00000000-0000-4000-8000-000000000001",
            "clock_source": "CLOCK_BOOTTIME",
        },
        legacy_shadow_root=legacy_shadow_root,
        recovery_state_root=recovery_state_root,
    )
    captured = _invoke_recovery_helper(
        capture_helper,
        operation="capture-recovery",
        key_id=key_id,
        payload=_capture_request(),
    )
    assert captured.returncode == 0, captured.stderr.decode()
    capture_response = json.loads(captured.stdout)
    staging, batch_digest, input_identity = _write_complete_monitor_staging(
        legacy_shadow_root,
        capture_response,
    )
    faulty_helper = _write_protected_signer_fixture(
        root,
        key_manifest=manifest,
        trusted_clock={
            "wallclock_at": "2026-08-03T07:04:59.000000Z",
            "monotonic_ns": 101_000_000_000,
            "boot_id": "00000000-0000-4000-8000-000000000001",
            "clock_source": "CLOCK_BOOTTIME",
        },
        legacy_shadow_root=legacy_shadow_root,
        recovery_state_root=recovery_state_root,
        fault_point="after-journal-prepare",
    )
    interrupted = _invoke_recovery_helper(
        faulty_helper,
        operation="sign-recovery",
        key_id=key_id,
        payload=_finish_request(
            capture_response,
            batch_digest=batch_digest,
            input_identity=input_identity,
        ),
    )
    assert interrupted.returncode == 2
    assert b"injected fault: after-journal-prepare" in interrupted.stderr
    prepared_path = next(recovery_state_root.glob("*.prepared.json"))
    prepared_seed_path = next(recovery_state_root.glob("*.prepared-seed.json"))
    resume_helper = _write_protected_signer_fixture(
        root,
        key_manifest=manifest,
        trusted_clock={
            "wallclock_at": "2026-08-03T07:04:59.500000Z",
            "monotonic_ns": 101_500_000_000,
            "boot_id": "00000000-0000-4000-8000-000000000001",
            "clock_source": "CLOCK_BOOTTIME",
        },
        legacy_shadow_root=legacy_shadow_root,
        recovery_state_root=recovery_state_root,
    )
    return (
        key_id,
        resume_helper,
        recovery_state_root,
        staging,
        prepared_path,
        prepared_seed_path,
    )


def _committed_prepared_case(
    root: Path,
) -> tuple[str, Path, Path, Path, Path, Path, Path]:
    case = _initial_prepared_case(root)
    key_id, helper, state_root, _staging, _prepared_path, _seed_path = case
    completed = _invoke_recovery_helper(
        helper,
        operation="resume-recovery",
        key_id=key_id,
        payload=_resume_request(),
    )
    assert completed.returncode == 0, completed.stderr.decode()
    committed_path = next(state_root.glob("*.committed.json"))
    return (*case, committed_path)


def test_secure_shadow_signing_client_uses_only_the_protected_helper_boundary(
    tmp_path: Path,
) -> None:
    private_key, public_key = _keypair(tmp_path, "protected-shadow-v1")
    manifest = _write_shadow_key_manifest(
        tmp_path,
        private_key=private_key,
        key_id="protected-shadow-v1",
        recovery_calendar=_write_recovery_calendar(tmp_path),
    )
    helper = _write_protected_signer_fixture(tmp_path, key_manifest=manifest)
    client = SecureShadowSigningClient(
        command=(sys.executable, str(helper)),
        key_id="protected-shadow-v1",
        timeout_seconds=5,
    )
    signer = Ed25519CompletionAttestationSigner(
        key_id="protected-shadow-v1",
        client=client,
    )
    keyring = Ed25519CompletionAttestationKeyring(
        active_key_id="protected-shadow-v1",
        active_public_key=public_key,
    )

    assert str(private_key) not in client._command
    assert keyring.verify(signer.issue(_claims()))
    with pytest.raises(ValueError, match="namespace"):
        client.sign(
            namespace="rquant-legacy-shadow-recovery-marker",
            payload=b"caller-controlled recovery marker",
        )
    recovery_payload = b"caller-controlled recovery marker"
    recovery_payload_sha256 = hashlib.sha256(recovery_payload).hexdigest()
    recovery_request = {
        "schema_version": 1,
        "operation": "sign",
        "request_id": canonical_sha256(
            {
                "contract": "runtime-shadow-signing-request/v1",
                "key_id": "protected-shadow-v1",
                "namespace": "rquant-legacy-shadow-recovery-marker",
                "payload_sha256": recovery_payload_sha256,
            }
        ),
        "key_id": "protected-shadow-v1",
        "namespace": "rquant-legacy-shadow-recovery-marker",
        "payload_base64": base64.b64encode(recovery_payload).decode("ascii"),
        "payload_sha256": recovery_payload_sha256,
    }
    rejected = subprocess.run(
        (sys.executable, str(helper)),
        input=canonical_json_bytes(recovery_request),
        check=False,
        capture_output=True,
    )
    assert rejected.returncode == 2
    assert b"recovery" in rejected.stderr.lower()
    with pytest.raises(ValueError, match="namespace"):
        client.sign(namespace="arbitrary-signing", payload=b"must-not-sign")


def test_root_recovery_signer_rejects_completion_after_the_publish_window(
    tmp_path: Path,
) -> None:
    key_id = "protected-shadow-v1"
    private_key, _public_key = _keypair(tmp_path, key_id)
    manifest = _write_shadow_key_manifest(
        tmp_path,
        private_key=private_key,
        key_id=key_id,
        recovery_calendar=_write_recovery_calendar(tmp_path),
    )
    legacy_shadow_root = tmp_path / "legacy-shadow"
    recovery_state_root = tmp_path / "recovery-state"
    legacy_shadow_root.mkdir(mode=0o700)
    recovery_state_root.mkdir(mode=0o700)
    capture_helper = _write_protected_signer_fixture(
        tmp_path,
        key_manifest=manifest,
        trusted_clock={
            "wallclock_at": "2026-08-03T07:04:59.000000Z",
            "monotonic_ns": 100_000_000_000,
            "boot_id": "00000000-0000-4000-8000-000000000001",
            "clock_source": "CLOCK_BOOTTIME",
        },
        legacy_shadow_root=legacy_shadow_root,
        recovery_state_root=recovery_state_root,
    )
    captured = _invoke_recovery_helper(
        capture_helper,
        operation="capture-recovery",
        key_id=key_id,
        payload=_capture_request(),
    )
    assert captured.returncode == 0, captured.stderr.decode()
    capture_response = json.loads(captured.stdout)
    staging, batch_digest, input_identity = _write_complete_monitor_staging(
        legacy_shadow_root,
        capture_response,
    )

    finish_helper = _write_protected_signer_fixture(
        tmp_path,
        key_manifest=manifest,
        trusted_clock={
            "wallclock_at": "2026-08-03T07:15:00.000000Z",
            "monotonic_ns": 760_000_000_000,
            "boot_id": "00000000-0000-4000-8000-000000000001",
            "clock_source": "CLOCK_BOOTTIME",
        },
        legacy_shadow_root=legacy_shadow_root,
        recovery_state_root=recovery_state_root,
    )
    rejected = _invoke_recovery_helper(
        finish_helper,
        operation="sign-recovery",
        key_id=key_id,
        payload=_finish_request(
            capture_response,
            batch_digest=batch_digest,
            input_identity=input_identity,
        ),
    )
    assert rejected.returncode == 2
    assert rejected.stdout == b""
    assert b"window" in rejected.stderr.lower()
    assert not (staging / "recovery-marker.json").exists()

    forged = _finish_request(
        capture_response,
        batch_digest=batch_digest,
        input_identity=input_identity,
    )
    forged["claims"] = {
        **forged["claims"],
        "captured_at": "2026-08-03T07:04:59.000000Z",
        "produced_at": "2026-08-03T07:04:59.500000Z",
        "monotonic_ns": 100_500_000_000,
        "boot_id": "00000000-0000-4000-8000-000000000001",
    }
    backdated = _invoke_recovery_helper(
        finish_helper,
        operation="sign-recovery",
        key_id=key_id,
        payload=forged,
    )
    assert backdated.returncode == 2
    assert backdated.stdout == b""
    assert b"shape" in backdated.stderr.lower()


def test_root_recovery_signer_rejects_a_missing_trusted_staging(
    tmp_path: Path,
) -> None:
    key_id = "protected-shadow-v1"
    private_key, _public_key = _keypair(tmp_path, key_id)
    manifest = _write_shadow_key_manifest(
        tmp_path,
        private_key=private_key,
        key_id=key_id,
        recovery_calendar=_write_recovery_calendar(tmp_path),
    )
    legacy_shadow_root = tmp_path / "legacy-shadow"
    recovery_state_root = tmp_path / "recovery-state"
    legacy_shadow_root.mkdir(mode=0o700)
    recovery_state_root.mkdir(mode=0o700)
    helper = _write_protected_signer_fixture(
        tmp_path,
        key_manifest=manifest,
        trusted_clock={
            "wallclock_at": "2026-08-03T07:04:59.000000Z",
            "monotonic_ns": 100_000_000_000,
            "boot_id": "00000000-0000-4000-8000-000000000001",
            "clock_source": "CLOCK_BOOTTIME",
        },
        legacy_shadow_root=legacy_shadow_root,
        recovery_state_root=recovery_state_root,
    )
    captured = _invoke_recovery_helper(
        helper,
        operation="capture-recovery",
        key_id=key_id,
        payload=_capture_request(),
    )
    assert captured.returncode == 0, captured.stderr.decode()

    rejected = _invoke_recovery_helper(
        helper,
        operation="sign-recovery",
        key_id=key_id,
        payload=_finish_request(json.loads(captured.stdout)),
    )

    assert rejected.returncode == 2
    assert rejected.stdout == b""
    assert b"staging" in rejected.stderr.lower()


def test_root_recovery_signer_rejects_when_signing_finishes_after_the_window(
    tmp_path: Path,
) -> None:
    key_id = "protected-shadow-v1"
    private_key, _public_key = _keypair(tmp_path, key_id)
    manifest = _write_shadow_key_manifest(
        tmp_path,
        private_key=private_key,
        key_id=key_id,
        recovery_calendar=_write_recovery_calendar(tmp_path),
    )
    legacy_shadow_root = tmp_path / "legacy-shadow"
    recovery_state_root = tmp_path / "recovery-state"
    legacy_shadow_root.mkdir(mode=0o700)
    recovery_state_root.mkdir(mode=0o700)
    capture_helper = _write_protected_signer_fixture(
        tmp_path,
        key_manifest=manifest,
        trusted_clock={
            "wallclock_at": "2026-08-03T07:04:58.000000Z",
            "monotonic_ns": 100_000_000_000,
            "boot_id": "00000000-0000-4000-8000-000000000001",
            "clock_source": "CLOCK_BOOTTIME",
        },
        legacy_shadow_root=legacy_shadow_root,
        recovery_state_root=recovery_state_root,
    )
    captured = _invoke_recovery_helper(
        capture_helper,
        operation="capture-recovery",
        key_id=key_id,
        payload=_capture_request(),
    )
    assert captured.returncode == 0, captured.stderr.decode()
    capture_response = json.loads(captured.stdout)
    staging, batch_digest, input_identity = _write_complete_monitor_staging(
        legacy_shadow_root,
        capture_response,
    )
    finish_helper = _write_protected_signer_fixture(
        tmp_path,
        key_manifest=manifest,
        trusted_clocks=(
            {
                "wallclock_at": "2026-08-03T07:04:59.000000Z",
                "monotonic_ns": 101_000_000_000,
                "boot_id": "00000000-0000-4000-8000-000000000001",
                "clock_source": "CLOCK_BOOTTIME",
            },
            {
                "wallclock_at": "2026-08-03T07:15:00.000000Z",
                "monotonic_ns": 762_000_000_000,
                "boot_id": "00000000-0000-4000-8000-000000000001",
                "clock_source": "CLOCK_BOOTTIME",
            },
        ),
        legacy_shadow_root=legacy_shadow_root,
        recovery_state_root=recovery_state_root,
    )

    rejected = _invoke_recovery_helper(
        finish_helper,
        operation="sign-recovery",
        key_id=key_id,
        payload=_finish_request(
            capture_response,
            batch_digest=batch_digest,
            input_identity=input_identity,
        ),
    )

    assert rejected.returncode == 2
    assert rejected.stdout == b""
    assert b"window" in rejected.stderr.lower()
    assert not (staging / "recovery-marker.json").exists()
    assert len(tuple(recovery_state_root.glob("*.capture-seed.json"))) == 1
    assert not tuple(recovery_state_root.glob("*.prepared.json"))
    assert not tuple(recovery_state_root.glob("*.prepared-seed.json"))


def test_root_recovery_finalization_rejects_crossing_1505_after_marker_fsync(
    tmp_path: Path,
) -> None:
    key_id = "protected-shadow-v1"
    private_key, _public_key = _keypair(tmp_path, key_id)
    manifest = _write_shadow_key_manifest(
        tmp_path,
        private_key=private_key,
        key_id=key_id,
        recovery_calendar=_write_recovery_calendar(tmp_path),
    )
    legacy_shadow_root = tmp_path / "legacy-shadow"
    recovery_state_root = tmp_path / "recovery-state"
    legacy_shadow_root.mkdir(mode=0o700)
    recovery_state_root.mkdir(mode=0o700)
    capture_helper = _write_protected_signer_fixture(
        tmp_path,
        key_manifest=manifest,
        trusted_clock={
            "wallclock_at": "2026-08-03T07:04:58.000000Z",
            "monotonic_ns": 100_000_000_000,
            "boot_id": "00000000-0000-4000-8000-000000000001",
            "clock_source": "CLOCK_BOOTTIME",
        },
        legacy_shadow_root=legacy_shadow_root,
        recovery_state_root=recovery_state_root,
    )
    captured = _invoke_recovery_helper(
        capture_helper,
        operation="capture-recovery",
        key_id=key_id,
        payload=_capture_request(),
    )
    assert captured.returncode == 0, captured.stderr.decode()
    capture_response = json.loads(captured.stdout)
    staging, batch_digest, input_identity = _write_complete_monitor_staging(
        legacy_shadow_root,
        capture_response,
    )
    finish_helper = _write_protected_signer_fixture(
        tmp_path,
        key_manifest=manifest,
        trusted_clocks=(
            {
                "wallclock_at": "2026-08-03T07:04:59.700000Z",
                "monotonic_ns": 101_700_000_000,
                "boot_id": "00000000-0000-4000-8000-000000000001",
                "clock_source": "CLOCK_BOOTTIME",
            },
            {
                "wallclock_at": "2026-08-03T07:04:59.800000Z",
                "monotonic_ns": 101_800_000_000,
                "boot_id": "00000000-0000-4000-8000-000000000001",
                "clock_source": "CLOCK_BOOTTIME",
            },
            {
                "wallclock_at": "2026-08-03T07:04:59.900000Z",
                "monotonic_ns": 101_900_000_000,
                "boot_id": "00000000-0000-4000-8000-000000000001",
                "clock_source": "CLOCK_BOOTTIME",
            },
            {
                "wallclock_at": "2026-08-03T07:05:00.100000Z",
                "monotonic_ns": 102_100_000_000,
                "boot_id": "00000000-0000-4000-8000-000000000001",
                "clock_source": "CLOCK_BOOTTIME",
            },
        ),
        legacy_shadow_root=legacy_shadow_root,
        recovery_state_root=recovery_state_root,
    )

    rejected = _invoke_recovery_helper(
        finish_helper,
        operation="sign-recovery",
        key_id=key_id,
        payload=_finish_request(
            capture_response,
            batch_digest=batch_digest,
            input_identity=input_identity,
        ),
    )

    assert rejected.returncode == 2
    assert b"window" in rejected.stderr.lower()
    assert (staging / "recovery-marker.json").is_file()
    assert not (staging / "finalization-receipt.json").exists()
    assert not (staging / ".finalization-receipt.pending").exists()


@pytest.mark.parametrize(
    "fault_point",
    (
        "after-journal-prepare",
        "marker-write",
        "after-marker-fsync",
        "after-finalization-prepare",
        "finalization-receipt-materialize-write",
        "finalization-receipt-write",
        "after-finalization-receipt-fsync",
        "after-finalization-durability-prepare",
        "before-ledger-commit",
        "ledger-commit-write",
    ),
)
def test_root_recovery_transaction_resumes_exact_batch_after_crash(
    tmp_path: Path,
    fault_point: str,
) -> None:
    key_id = "protected-shadow-v1"
    private_key, _public_key = _keypair(tmp_path, key_id)
    manifest = _write_shadow_key_manifest(
        tmp_path,
        private_key=private_key,
        key_id=key_id,
        recovery_calendar=_write_recovery_calendar(tmp_path),
    )
    legacy_shadow_root = tmp_path / "legacy-shadow"
    recovery_state_root = tmp_path / "recovery-state"
    legacy_shadow_root.mkdir(mode=0o700)
    recovery_state_root.mkdir(mode=0o700)
    capture_helper = _write_protected_signer_fixture(
        tmp_path,
        key_manifest=manifest,
        trusted_clock={
            "wallclock_at": "2026-08-03T07:04:58.000000Z",
            "monotonic_ns": 100_000_000_000,
            "boot_id": "00000000-0000-4000-8000-000000000001",
            "clock_source": "CLOCK_BOOTTIME",
        },
        legacy_shadow_root=legacy_shadow_root,
        recovery_state_root=recovery_state_root,
    )
    captured = _invoke_recovery_helper(
        capture_helper,
        operation="capture-recovery",
        key_id=key_id,
        payload=_capture_request(),
    )
    assert captured.returncode == 0, captured.stderr.decode()
    capture_response = json.loads(captured.stdout)
    staging, batch_digest, input_identity = _write_complete_monitor_staging(
        legacy_shadow_root,
        capture_response,
    )
    request = _finish_request(
        capture_response,
        batch_digest=batch_digest,
        input_identity=input_identity,
    )
    faulty_helper = _write_protected_signer_fixture(
        tmp_path,
        key_manifest=manifest,
        trusted_clock={
            "wallclock_at": "2026-08-03T07:04:59.000000Z",
            "monotonic_ns": 101_000_000_000,
            "boot_id": "00000000-0000-4000-8000-000000000001",
            "clock_source": "CLOCK_BOOTTIME",
        },
        legacy_shadow_root=legacy_shadow_root,
        recovery_state_root=recovery_state_root,
        fault_point=fault_point,
    )
    interrupted = _invoke_recovery_helper(
        faulty_helper,
        operation="sign-recovery",
        key_id=key_id,
        payload=request,
    )
    assert interrupted.returncode == 2

    recovery_helper = _write_protected_signer_fixture(
        tmp_path,
        key_manifest=manifest,
        trusted_clock={
            "wallclock_at": "2026-08-03T07:04:59.500000Z",
            "monotonic_ns": 101_500_000_000,
            "boot_id": "00000000-0000-4000-8000-000000000001",
            "clock_source": "CLOCK_BOOTTIME",
        },
        legacy_shadow_root=legacy_shadow_root,
        recovery_state_root=recovery_state_root,
    )
    recovered = _invoke_recovery_helper(
        recovery_helper,
        operation="resume-recovery",
        key_id=key_id,
        payload=_resume_request(),
    )
    replayed = _invoke_recovery_helper(
        recovery_helper,
        operation="resume-recovery",
        key_id=key_id,
        payload=_resume_request(),
    )

    assert recovered.returncode == 0, recovered.stderr.decode()
    assert replayed.returncode == 0, replayed.stderr.decode()
    assert replayed.stdout == recovered.stdout
    assert (staging / "recovery-marker.json").is_file()
    assert (staging / "finalization-receipt.json").is_file()
    prepared = tuple(recovery_state_root.glob("*.prepared.json"))
    committed = tuple(recovery_state_root.glob("*.committed.json"))
    assert len(prepared) == 1
    assert len(committed) == 1
    prepared_payload = prepared[0].read_bytes()
    committed_document = json.loads(committed[0].read_bytes())
    assert committed_document["prepared_sha256"] == hashlib.sha256(prepared_payload).hexdigest()
    assert (
        committed_document["marker_sha256"]
        == hashlib.sha256((staging / "recovery-marker.json").read_bytes()).hexdigest()
    )
    assert (
        committed_document["finalization_receipt_sha256"]
        == hashlib.sha256((staging / "finalization-receipt.json").read_bytes()).hexdigest()
    )
    assert (
        committed_document["capture_token_id"] == json.loads(prepared_payload)["capture_token_id"]
    )


@pytest.mark.parametrize(
    "kill_fault_point",
    (
        "prepared-temp-write",
        "prepared-before-rename",
        "prepared-after-rename",
        "prepared-before-dir-fsync",
        "prepared-after-dir-fsync",
    ),
)
def test_root_prepared_journal_atomic_publish_recovers_after_sigkill(
    tmp_path: Path,
    kill_fault_point: str,
) -> None:
    key_id = "protected-shadow-v1"
    private_key, _public_key = _keypair(tmp_path, key_id)
    manifest = _write_shadow_key_manifest(
        tmp_path,
        private_key=private_key,
        key_id=key_id,
        recovery_calendar=_write_recovery_calendar(tmp_path),
    )
    legacy_shadow_root = tmp_path / "legacy-shadow"
    recovery_state_root = tmp_path / "recovery-state"
    legacy_shadow_root.mkdir(mode=0o700)
    recovery_state_root.mkdir(mode=0o700)
    capture_helper = _write_protected_signer_fixture(
        tmp_path,
        key_manifest=manifest,
        trusted_clock={
            "wallclock_at": "2026-08-03T07:04:58.000000Z",
            "monotonic_ns": 100_000_000_000,
            "boot_id": "00000000-0000-4000-8000-000000000001",
            "clock_source": "CLOCK_BOOTTIME",
        },
        legacy_shadow_root=legacy_shadow_root,
        recovery_state_root=recovery_state_root,
    )
    captured = _invoke_recovery_helper(
        capture_helper,
        operation="capture-recovery",
        key_id=key_id,
        payload=_capture_request(),
    )
    assert captured.returncode == 0, captured.stderr.decode()
    assert len(tuple(recovery_state_root.glob("*.capture-seed.json"))) == 1
    capture_response = json.loads(captured.stdout)
    staging, batch_digest, input_identity = _write_complete_monitor_staging(
        legacy_shadow_root,
        capture_response,
    )
    faulty_helper = _write_protected_signer_fixture(
        tmp_path,
        key_manifest=manifest,
        trusted_clock={
            "wallclock_at": "2026-08-03T07:04:59.000000Z",
            "monotonic_ns": 101_000_000_000,
            "boot_id": "00000000-0000-4000-8000-000000000001",
            "clock_source": "CLOCK_BOOTTIME",
        },
        legacy_shadow_root=legacy_shadow_root,
        recovery_state_root=recovery_state_root,
        kill_fault_point=kill_fault_point,
    )
    interrupted = _invoke_recovery_helper(
        faulty_helper,
        operation="sign-recovery",
        key_id=key_id,
        payload=_finish_request(
            capture_response,
            batch_digest=batch_digest,
            input_identity=input_identity,
        ),
    )

    assert interrupted.returncode == -signal.SIGKILL
    assert interrupted.stdout == b""
    assert len(tuple(recovery_state_root.glob("*.prepared-seed.json"))) == 1
    assert not (staging / "recovery-marker.json").exists()
    prepared = tuple(recovery_state_root.glob("*.prepared.json"))
    temporary = tuple(recovery_state_root.glob(".*.prepared.json.tmp-*"))
    if kill_fault_point in {"prepared-temp-write", "prepared-before-rename"}:
        assert prepared == ()
        assert len(temporary) == 1
    else:
        assert len(prepared) == 1
        assert temporary == ()

    recovery_helper = _write_protected_signer_fixture(
        tmp_path,
        key_manifest=manifest,
        trusted_clock={
            "wallclock_at": "2026-08-03T07:04:59.500000Z",
            "monotonic_ns": 101_500_000_000,
            "boot_id": "00000000-0000-4000-8000-000000000001",
            "clock_source": "CLOCK_BOOTTIME",
        },
        legacy_shadow_root=legacy_shadow_root,
        recovery_state_root=recovery_state_root,
    )
    recovered = _invoke_recovery_helper(
        recovery_helper,
        operation="resume-recovery",
        key_id=key_id,
        payload=_resume_request(),
    )
    replayed = _invoke_recovery_helper(
        recovery_helper,
        operation="resume-recovery",
        key_id=key_id,
        payload=_resume_request(),
    )

    assert recovered.returncode == 0, recovered.stderr.decode()
    assert replayed.returncode == 0, replayed.stderr.decode()
    assert replayed.stdout == recovered.stdout
    assert (staging / "recovery-marker.json").is_file()
    assert (staging / "finalization-receipt.json").is_file()
    assert len(tuple(recovery_state_root.glob("*.prepared.json"))) == 1
    assert len(tuple(recovery_state_root.glob("*.committed.json"))) == 1
    assert not tuple(recovery_state_root.glob(".*.prepared.json.tmp-*"))


def test_root_capture_seed_recovers_staging_visible_before_prepared(
    tmp_path: Path,
) -> None:
    key_id = "protected-shadow-v1"
    private_key, _public_key = _keypair(tmp_path, key_id)
    manifest = _write_shadow_key_manifest(
        tmp_path,
        private_key=private_key,
        key_id=key_id,
        recovery_calendar=_write_recovery_calendar(tmp_path),
    )
    legacy_shadow_root = tmp_path / "legacy-shadow"
    recovery_state_root = tmp_path / "recovery-state"
    legacy_shadow_root.mkdir(mode=0o700)
    recovery_state_root.mkdir(mode=0o700)
    helper = _write_protected_signer_fixture(
        tmp_path,
        key_manifest=manifest,
        trusted_clock={
            "wallclock_at": "2026-08-03T07:04:59.000000Z",
            "monotonic_ns": 101_000_000_000,
            "boot_id": "00000000-0000-4000-8000-000000000001",
            "clock_source": "CLOCK_BOOTTIME",
        },
        legacy_shadow_root=legacy_shadow_root,
        recovery_state_root=recovery_state_root,
    )
    captured = _invoke_recovery_helper(
        helper,
        operation="capture-recovery",
        key_id=key_id,
        payload=_capture_request(),
    )
    assert captured.returncode == 0, captured.stderr.decode()
    capture_response = json.loads(captured.stdout)
    staging, _batch_digest, _input_identity = _write_complete_monitor_staging(
        legacy_shadow_root,
        capture_response,
    )
    assert len(tuple(recovery_state_root.glob("*.capture-seed.json"))) == 1
    assert not tuple(recovery_state_root.glob("*.prepared.json"))

    recovered = _invoke_recovery_helper(
        helper,
        operation="resume-recovery",
        key_id=key_id,
        payload=_resume_request(),
    )

    assert recovered.returncode == 0, recovered.stderr.decode()
    assert (staging / "recovery-marker.json").is_file()
    assert (staging / "finalization-receipt.json").is_file()
    assert len(tuple(recovery_state_root.glob("*.prepared.json"))) == 1
    assert len(tuple(recovery_state_root.glob("*.committed.json"))) == 1


def test_root_recovery_survives_signer_and_publisher_parent_sigkill(
    tmp_path: Path,
) -> None:
    key_id = "protected-shadow-v1"
    private_key, _public_key = _keypair(tmp_path, key_id)
    manifest = _write_shadow_key_manifest(
        tmp_path,
        private_key=private_key,
        key_id=key_id,
        recovery_calendar=_write_recovery_calendar(tmp_path),
    )
    legacy_shadow_root = tmp_path / "legacy-shadow"
    recovery_state_root = tmp_path / "recovery-state"
    legacy_shadow_root.mkdir(mode=0o700)
    recovery_state_root.mkdir(mode=0o700)
    capture_helper = _write_protected_signer_fixture(
        tmp_path,
        key_manifest=manifest,
        trusted_clock={
            "wallclock_at": "2026-08-03T07:04:58.000000Z",
            "monotonic_ns": 100_000_000_000,
            "boot_id": "00000000-0000-4000-8000-000000000001",
            "clock_source": "CLOCK_BOOTTIME",
        },
        legacy_shadow_root=legacy_shadow_root,
        recovery_state_root=recovery_state_root,
    )
    captured = _invoke_recovery_helper(
        capture_helper,
        operation="capture-recovery",
        key_id=key_id,
        payload=_capture_request(),
    )
    assert captured.returncode == 0, captured.stderr.decode()
    capture_response = json.loads(captured.stdout)
    staging, batch_digest, input_identity = _write_complete_monitor_staging(
        legacy_shadow_root,
        capture_response,
    )
    faulty_helper = _write_protected_signer_fixture(
        tmp_path,
        key_manifest=manifest,
        trusted_clock={
            "wallclock_at": "2026-08-03T07:04:59.000000Z",
            "monotonic_ns": 101_000_000_000,
            "boot_id": "00000000-0000-4000-8000-000000000001",
            "clock_source": "CLOCK_BOOTTIME",
        },
        legacy_shadow_root=legacy_shadow_root,
        recovery_state_root=recovery_state_root,
        kill_fault_point="prepared-before-rename",
    )
    request_path = tmp_path / "sign-request.json"
    request_path.write_bytes(
        _protected_recovery_request(
            operation="sign-recovery",
            key_id=key_id,
            payload=_finish_request(
                capture_response,
                batch_digest=batch_digest,
                input_identity=input_identity,
            ),
        )
    )
    parent_code = "\n".join(
        (
            "import os, signal, subprocess, sys",
            "from pathlib import Path",
            "child = subprocess.run((sys.executable, sys.argv[1]), "
            "input=Path(sys.argv[2]).read_bytes())",
            "assert child.returncode == -signal.SIGKILL",
            "os.kill(os.getpid(), signal.SIGKILL)",
        )
    )
    interrupted_parent = subprocess.run(
        (sys.executable, "-c", parent_code, str(faulty_helper), str(request_path)),
        check=False,
        capture_output=True,
        timeout=5,
    )
    assert interrupted_parent.returncode == -signal.SIGKILL
    assert staging.is_dir()
    assert len(tuple(recovery_state_root.glob("*.prepared-seed.json"))) == 1

    recovery_helper = _write_protected_signer_fixture(
        tmp_path,
        key_manifest=manifest,
        trusted_clock={
            "wallclock_at": "2026-08-03T07:04:59.500000Z",
            "monotonic_ns": 101_500_000_000,
            "boot_id": "00000000-0000-4000-8000-000000000001",
            "clock_source": "CLOCK_BOOTTIME",
        },
        legacy_shadow_root=legacy_shadow_root,
        recovery_state_root=recovery_state_root,
    )
    recovered = _invoke_recovery_helper(
        recovery_helper,
        operation="resume-recovery",
        key_id=key_id,
        payload=_resume_request(),
    )

    assert recovered.returncode == 0, recovered.stderr.decode()
    assert (staging / "finalization-receipt.json").is_file()
    assert len(tuple(recovery_state_root.glob("*.committed.json"))) == 1


@pytest.mark.parametrize("journal_kind", ("torn", "canonical-tamper"))
def test_root_prepared_journal_repairs_only_from_matching_capture_seed(
    tmp_path: Path,
    journal_kind: str,
) -> None:
    key_id = "protected-shadow-v1"
    private_key, _public_key = _keypair(tmp_path, key_id)
    manifest = _write_shadow_key_manifest(
        tmp_path,
        private_key=private_key,
        key_id=key_id,
        recovery_calendar=_write_recovery_calendar(tmp_path),
    )
    legacy_shadow_root = tmp_path / "legacy-shadow"
    recovery_state_root = tmp_path / "recovery-state"
    legacy_shadow_root.mkdir(mode=0o700)
    recovery_state_root.mkdir(mode=0o700)
    helper = _write_protected_signer_fixture(
        tmp_path,
        key_manifest=manifest,
        trusted_clock={
            "wallclock_at": "2026-08-03T07:04:59.000000Z",
            "monotonic_ns": 101_000_000_000,
            "boot_id": "00000000-0000-4000-8000-000000000001",
            "clock_source": "CLOCK_BOOTTIME",
        },
        legacy_shadow_root=legacy_shadow_root,
        recovery_state_root=recovery_state_root,
    )
    captured = _invoke_recovery_helper(
        helper,
        operation="capture-recovery",
        key_id=key_id,
        payload=_capture_request(),
    )
    assert captured.returncode == 0, captured.stderr.decode()
    capture_response = json.loads(captured.stdout)
    staging, _batch_digest, _input_identity = _write_complete_monitor_staging(
        legacy_shadow_root,
        capture_response,
    )
    transaction_id = _transaction_id_from_capture_response(capture_response)
    prepared_path = recovery_state_root / f"transaction-{transaction_id}.prepared.json"
    payload = (
        b'{"contract":"legacy-shadow-recovery-transaction/v1"'
        if journal_kind == "torn"
        else canonical_json_bytes(
            {
                "contract": "legacy-shadow-recovery-transaction/v1",
                "state": "prepared",
                "transaction_id": transaction_id,
                "attacker": True,
            }
        )
    )
    if journal_kind == "torn":
        torn_writer = "\n".join(
            (
                "import os, signal, sys",
                "path, payload = sys.argv[1], bytes.fromhex(sys.argv[2])",
                "fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)",
                "os.write(fd, payload)",
                "os.fsync(fd)",
                "os.kill(os.getpid(), signal.SIGKILL)",
            )
        )
        interrupted_writer = subprocess.run(
            (sys.executable, "-c", torn_writer, str(prepared_path), payload.hex()),
            check=False,
            capture_output=True,
            timeout=5,
        )
        assert interrupted_writer.returncode == -signal.SIGKILL
    else:
        prepared_path.write_bytes(payload)
        prepared_path.chmod(0o600)
        descriptor = os.open(prepared_path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    state_descriptor = os.open(recovery_state_root, os.O_RDONLY)
    try:
        os.fsync(state_descriptor)
    finally:
        os.close(state_descriptor)

    recovered = _invoke_recovery_helper(
        helper,
        operation="resume-recovery",
        key_id=key_id,
        payload=_resume_request(),
    )

    if journal_kind == "canonical-tamper":
        assert recovered.returncode == 2
        assert b"seed" in recovered.stderr.lower() or b"schema" in recovered.stderr.lower()
        assert prepared_path.read_bytes() == payload
        assert not (staging / "recovery-marker.json").exists()
        assert not tuple(recovery_state_root.glob("*.committed.json"))
        return
    assert recovered.returncode == 0, recovered.stderr.decode()
    repaired = json.loads(prepared_path.read_bytes())
    assert repaired["transaction_id"] == transaction_id
    assert repaired["capture_token_id"] == canonical_sha256(
        _finish_request(capture_response)["capture"]
    )
    assert (staging / "finalization-receipt.json").is_file()


def test_root_prepared_rejects_extra_missing_and_noncanonical_fields(
    tmp_path: Path,
) -> None:
    key_id, helper, state_root, staging, prepared_path, _seed_path = _initial_prepared_case(
        tmp_path
    )
    baseline_payload = prepared_path.read_bytes()
    baseline = json.loads(baseline_payload)
    assert baseline["contract"] == "legacy-shadow-recovery-transaction/v2"
    assert baseline["schema_version"] == 2
    cases: tuple[tuple[str, bytes], ...] = (
        (
            "extra",
            canonical_json_bytes({**baseline, "attacker_extension": True}),
        ),
        (
            "missing",
            canonical_json_bytes(
                {key: value for key, value in baseline.items() if key != "marker_sha256"}
            ),
        ),
        (
            "reordered",
            json.dumps(
                dict(reversed(tuple(baseline.items()))),
                ensure_ascii=True,
                separators=(",", ":"),
            ).encode("utf-8"),
        ),
        (
            "equivalent-number",
            canonical_json_bytes({**baseline, "state_sequence": 0.0}),
        ),
    )
    for label, payload in cases:
        _replace_protected_json(prepared_path, payload)
        rejected = _invoke_recovery_helper(
            helper,
            operation="resume-recovery",
            key_id=key_id,
            payload=_resume_request(),
        )
        assert rejected.returncode == 2, (label, rejected.stderr.decode())
        assert prepared_path.read_bytes() == payload
        assert not (staging / "recovery-marker.json").exists()
        assert not (staging / "finalization-receipt.json").exists()
        assert not tuple(state_root.glob("*.committed.json"))
        _replace_protected_json(prepared_path, baseline_payload)


def test_root_prepared_rejects_each_immutable_field_change(
    tmp_path: Path,
) -> None:
    key_id, helper, state_root, staging, prepared_path, _seed_path = _initial_prepared_case(
        tmp_path
    )
    baseline_payload = prepared_path.read_bytes()
    baseline = json.loads(baseline_payload)
    immutable_fields = (
        "contract",
        "schema_version",
        "state",
        "transaction_id",
        "capture_token_id",
        "request_payload_sha256",
        "capture",
        "draft",
        "directory_device",
        "directory_inode",
        "artifact_digests",
        "batch_digest",
        "marker_payload_base64",
        "marker_sha256",
        "signed_payload_base64",
        "signed_payload_sha256",
        "marker_signature",
    )

    def changed(value: object) -> object:
        if isinstance(value, dict):
            return {**value, "attacker_extension": True}
        if isinstance(value, int):
            return value + 1
        if isinstance(value, str):
            return f"{value}x"
        raise AssertionError(f"unsupported immutable fixture: {value!r}")

    for field in immutable_fields:
        payload = canonical_json_bytes({**baseline, field: changed(baseline[field])})
        _replace_protected_json(prepared_path, payload)
        rejected = _invoke_recovery_helper(
            helper,
            operation="resume-recovery",
            key_id=key_id,
            payload=_resume_request(),
        )
        assert rejected.returncode == 2, (field, rejected.stderr.decode())
        assert prepared_path.read_bytes() == payload
        assert not (staging / "finalization-receipt.json").exists()
        assert not tuple(state_root.glob("*.committed.json"))
        _replace_protected_json(prepared_path, baseline_payload)


def test_root_prepared_legal_finalization_evolution_is_hash_chained(
    tmp_path: Path,
) -> None:
    key_id, helper, state_root, staging, prepared_path, seed_path = _initial_prepared_case(tmp_path)
    initial_payload = prepared_path.read_bytes()
    recovered = _invoke_recovery_helper(
        helper,
        operation="resume-recovery",
        key_id=key_id,
        payload=_resume_request(),
    )
    assert recovered.returncode == 0, recovered.stderr.decode()

    seed = json.loads(seed_path.read_bytes())
    assert base64.b64decode(seed["prepared_payload_base64"], validate=True) == initial_payload
    durable_payload = prepared_path.read_bytes()
    durable = json.loads(durable_payload)
    assert durable["phase"] == "finalization-durable"
    assert durable["state_sequence"] == 2
    finalization_prepared = {
        **durable,
        "phase": "finalization-prepared",
        "state_sequence": 1,
        "previous_state_sha256": hashlib.sha256(initial_payload).hexdigest(),
        "finalization_durable_at": None,
        "finalization_durable_monotonic_ns": None,
        "finalization_durable_boot_id": None,
        "finalization_durable_clock_source": None,
    }
    finalization_payload = canonical_json_bytes(finalization_prepared)
    assert durable["previous_state_sha256"] == hashlib.sha256(finalization_payload).hexdigest()
    committed = json.loads(next(state_root.glob("*.committed.json")).read_bytes())
    assert committed["prepared_sha256"] == hashlib.sha256(durable_payload).hexdigest()
    assert (staging / "finalization-receipt.json").is_file()


def test_root_prepared_rejects_float_equivalents_for_every_integer_domain(
    tmp_path: Path,
) -> None:
    (
        key_id,
        helper,
        state_root,
        _staging,
        prepared_path,
        seed_path,
        committed_path,
    ) = _committed_prepared_case(tmp_path)
    durable_payload = prepared_path.read_bytes()
    durable = json.loads(durable_payload)
    committed_payload = committed_path.read_bytes()
    seed_document = json.loads(seed_path.read_bytes())
    seed_payload = base64.b64decode(seed_document["prepared_payload_base64"], validate=True)

    finalization_prepared = {
        **durable,
        "phase": "finalization-prepared",
        "state_sequence": 1.0,
        "previous_state_sha256": hashlib.sha256(seed_payload).hexdigest(),
        "finalization_durable_at": None,
        "finalization_durable_monotonic_ns": None,
        "finalization_durable_boot_id": None,
        "finalization_durable_clock_source": None,
    }
    cases = (
        ("sequence-1", finalization_prepared),
        ("schema-version", {**durable, "schema_version": 2.0}),
        ("sequence-2", {**durable, "state_sequence": 2.0}),
        ("directory-device", {**durable, "directory_device": float(durable["directory_device"])}),
        ("directory-inode", {**durable, "directory_inode": float(durable["directory_inode"])}),
        (
            "durable-monotonic",
            {
                **durable,
                "finalization_durable_monotonic_ns": float(
                    durable["finalization_durable_monotonic_ns"]
                ),
            },
        ),
    )
    for label, candidate in cases:
        payload = canonical_json_bytes(candidate)
        _replace_protected_json(prepared_path, payload)
        rejected = _invoke_recovery_helper(
            helper,
            operation="resume-recovery",
            key_id=key_id,
            payload=_resume_request(),
        )
        assert rejected.returncode == 2, (label, rejected.stderr.decode())
        assert prepared_path.read_bytes() == payload
        assert committed_path.read_bytes() == committed_payload
    _replace_protected_json(prepared_path, durable_payload)


def test_root_committed_head_is_idempotent_only_for_the_exact_durable_branch(
    tmp_path: Path,
) -> None:
    (
        key_id,
        helper,
        _state_root,
        staging,
        prepared_path,
        seed_path,
        committed_path,
    ) = _committed_prepared_case(tmp_path)
    durable_payload = prepared_path.read_bytes()
    durable = json.loads(durable_payload)
    committed_payload = committed_path.read_bytes()
    marker_payload = (staging / "recovery-marker.json").read_bytes()
    receipt_payload = (staging / "finalization-receipt.json").read_bytes()

    replayed = _invoke_recovery_helper(
        helper,
        operation="resume-recovery",
        key_id=key_id,
        payload=_resume_request(),
    )
    assert replayed.returncode == 0, replayed.stderr.decode()
    assert prepared_path.read_bytes() == durable_payload
    assert committed_path.read_bytes() == committed_payload
    assert (staging / "recovery-marker.json").read_bytes() == marker_payload
    assert (staging / "finalization-receipt.json").read_bytes() == receipt_payload

    seed_document = json.loads(seed_path.read_bytes())
    seed_payload = base64.b64decode(seed_document["prepared_payload_base64"], validate=True)
    old_head = {
        **durable,
        "phase": "finalization-prepared",
        "state_sequence": 1,
        "previous_state_sha256": hashlib.sha256(seed_payload).hexdigest(),
        "finalization_durable_at": None,
        "finalization_durable_monotonic_ns": None,
        "finalization_durable_boot_id": None,
        "finalization_durable_clock_source": None,
    }
    branch = {
        **durable,
        "finalization_durable_at": "2026-08-03T07:04:59.600000Z",
        "finalization_durable_monotonic_ns": 101_600_000_000,
    }
    for label, candidate in (("old-sequence", old_head), ("parallel-sequence-2", branch)):
        payload = canonical_json_bytes(candidate)
        _replace_protected_json(prepared_path, payload)
        rejected = _invoke_recovery_helper(
            helper,
            operation="resume-recovery",
            key_id=key_id,
            payload=_resume_request(),
        )
        assert rejected.returncode == 2, (label, rejected.stderr.decode())
        assert prepared_path.read_bytes() == payload
        assert committed_path.read_bytes() == committed_payload
        assert (staging / "recovery-marker.json").read_bytes() == marker_payload
        assert (staging / "finalization-receipt.json").read_bytes() == receipt_payload


def test_root_committed_head_repairs_only_syntactically_torn_state(
    tmp_path: Path,
) -> None:
    (
        key_id,
        helper,
        state_root,
        staging,
        _prepared_path,
        _seed_path,
        committed_path,
    ) = _committed_prepared_case(tmp_path)
    committed_payload = committed_path.read_bytes()

    _replace_protected_json(committed_path, committed_payload[: len(committed_payload) // 2])
    helper = _write_protected_signer_fixture(
        tmp_path,
        key_manifest=tmp_path / "shadow-report-keys.json",
        trusted_clocks=(),
        legacy_shadow_root=staging.parents[1],
        recovery_state_root=state_root,
    )
    repaired = _invoke_recovery_helper(
        helper,
        operation="resume-recovery",
        key_id=key_id,
        payload=_resume_request(),
    )
    assert repaired.returncode == 0, repaired.stderr.decode()
    assert committed_path.read_bytes() == committed_payload

    canonical = json.loads(committed_payload)
    attacks = (
        {**canonical, "attacker_extension": True},
        {**canonical, "prepared_sha256": "f" * 64},
        {**canonical, "schema_version": 2.0},
        {**canonical, "state_sequence": 2.0},
        {**canonical, "directory_device": float(canonical["directory_device"])},
        {**canonical, "directory_inode": float(canonical["directory_inode"])},
        {
            **canonical,
            "finalization_durable_monotonic_ns": float(
                canonical["finalization_durable_monotonic_ns"]
            ),
        },
    )
    for attack in attacks:
        payload = canonical_json_bytes(attack)
        _replace_protected_json(committed_path, payload)
        rejected = _invoke_recovery_helper(
            helper,
            operation="resume-recovery",
            key_id=key_id,
            payload=_resume_request(),
        )
        assert rejected.returncode == 2, rejected.stderr.decode()
        assert committed_path.read_bytes() == payload


def test_root_torn_committed_repair_never_advances_another_prepared_state(
    tmp_path: Path,
) -> None:
    (
        key_id,
        _helper,
        state_root,
        staging,
        prepared_path,
        seed_path,
        committed_path,
    ) = _committed_prepared_case(tmp_path)
    durable_payload = prepared_path.read_bytes()
    durable = json.loads(durable_payload)
    committed_payload = committed_path.read_bytes()
    torn_payload = committed_payload[: len(committed_payload) // 2]
    marker_payload = (staging / "recovery-marker.json").read_bytes()
    receipt_payload = (staging / "finalization-receipt.json").read_bytes()
    seed_document = json.loads(seed_path.read_bytes())
    initial_payload = base64.b64decode(seed_document["prepared_payload_base64"], validate=True)
    finalization_prepared = {
        **durable,
        "phase": "finalization-prepared",
        "state_sequence": 1,
        "previous_state_sha256": hashlib.sha256(initial_payload).hexdigest(),
        "finalization_durable_at": None,
        "finalization_durable_monotonic_ns": None,
        "finalization_durable_boot_id": None,
        "finalization_durable_clock_source": None,
    }
    parallel_durable = {
        **durable,
        "finalization_durable_at": "2026-08-03T07:04:59.750000Z",
        "finalization_durable_monotonic_ns": 101_750_000_000,
    }
    helper = _write_protected_signer_fixture(
        tmp_path,
        key_manifest=tmp_path / "shadow-report-keys.json",
        trusted_clock={
            "wallclock_at": "2026-08-03T07:04:59.800000Z",
            "monotonic_ns": 101_800_000_000,
            "boot_id": "00000000-0000-4000-8000-000000000001",
            "clock_source": "CLOCK_BOOTTIME",
        },
        legacy_shadow_root=staging.parents[1],
        recovery_state_root=state_root,
    )

    for label, candidate_payload in (
        ("sequence-0", initial_payload),
        ("sequence-1", canonical_json_bytes(finalization_prepared)),
        ("parallel-sequence-2", canonical_json_bytes(parallel_durable)),
    ):
        _replace_protected_json(prepared_path, candidate_payload)
        _replace_protected_json(committed_path, torn_payload)
        rejected = _invoke_recovery_helper(
            helper,
            operation="resume-recovery",
            key_id=key_id,
            payload=_resume_request(),
        )
        assert rejected.returncode == 2, (label, rejected.stderr.decode())
        assert prepared_path.read_bytes() == candidate_payload
        assert committed_path.read_bytes() == torn_payload
        assert (staging / "recovery-marker.json").read_bytes() == marker_payload
        assert (staging / "finalization-receipt.json").read_bytes() == receipt_payload

    prepared_path.unlink()
    _replace_protected_json(committed_path, torn_payload)
    rejected = _invoke_recovery_helper(
        helper,
        operation="resume-recovery",
        key_id=key_id,
        payload=_resume_request(),
    )
    assert rejected.returncode == 2, rejected.stderr.decode()
    assert not prepared_path.exists()
    assert committed_path.read_bytes() == torn_payload
    assert (staging / "recovery-marker.json").read_bytes() == marker_payload
    assert (staging / "finalization-receipt.json").read_bytes() == receipt_payload


def test_root_torn_committed_atomic_repair_is_idempotent_after_interruption(
    tmp_path: Path,
) -> None:
    (
        key_id,
        _helper,
        state_root,
        staging,
        prepared_path,
        _seed_path,
        committed_path,
    ) = _committed_prepared_case(tmp_path)
    durable_payload = prepared_path.read_bytes()
    committed_payload = committed_path.read_bytes()
    torn_payload = committed_payload[: len(committed_payload) // 2]

    for fault_point in (
        "committed-repair-before-rename",
        "committed-repair-after-rename",
        "committed-repair-after-dir-fsync",
    ):
        _replace_protected_json(committed_path, torn_payload)
        faulty_helper = _write_protected_signer_fixture(
            tmp_path,
            key_manifest=tmp_path / "shadow-report-keys.json",
            trusted_clocks=(),
            legacy_shadow_root=staging.parents[1],
            recovery_state_root=state_root,
            fault_point=fault_point,
        )
        interrupted = _invoke_recovery_helper(
            faulty_helper,
            operation="resume-recovery",
            key_id=key_id,
            payload=_resume_request(),
        )
        assert interrupted.returncode == 2
        assert prepared_path.read_bytes() == durable_payload

        recovery_helper = _write_protected_signer_fixture(
            tmp_path,
            key_manifest=tmp_path / "shadow-report-keys.json",
            trusted_clocks=(),
            legacy_shadow_root=staging.parents[1],
            recovery_state_root=state_root,
        )
        recovered = _invoke_recovery_helper(
            recovery_helper,
            operation="resume-recovery",
            key_id=key_id,
            payload=_resume_request(),
        )
        assert recovered.returncode == 0, (fault_point, recovered.stderr.decode())
        assert prepared_path.read_bytes() == durable_payload
        assert committed_path.read_bytes() == committed_payload


def test_root_seed_bound_missing_head_rejects_seq1_then_recreates_durable_head(
    tmp_path: Path,
) -> None:
    (
        key_id,
        _helper,
        state_root,
        staging,
        prepared_path,
        seed_path,
        committed_path,
    ) = _committed_prepared_case(tmp_path)
    durable_payload = prepared_path.read_bytes()
    durable = json.loads(durable_payload)
    committed_payload = committed_path.read_bytes()
    committed_seed_path = next(state_root.glob("*.committed-seed.json"))
    committed_seed_payload = committed_seed_path.read_bytes()
    marker_payload = (staging / "recovery-marker.json").read_bytes()
    receipt_payload = (staging / "finalization-receipt.json").read_bytes()
    initial_seed = json.loads(seed_path.read_bytes())
    initial_payload = base64.b64decode(initial_seed["prepared_payload_base64"], validate=True)
    finalization_prepared = {
        **durable,
        "phase": "finalization-prepared",
        "state_sequence": 1,
        "previous_state_sha256": hashlib.sha256(initial_payload).hexdigest(),
        "finalization_durable_at": None,
        "finalization_durable_monotonic_ns": None,
        "finalization_durable_boot_id": None,
        "finalization_durable_clock_source": None,
    }
    seq1_payload = canonical_json_bytes(finalization_prepared)
    committed_path.unlink()
    _replace_protected_json(prepared_path, seq1_payload)
    new_clock_helper = _write_protected_signer_fixture(
        tmp_path,
        key_manifest=tmp_path / "shadow-report-keys.json",
        trusted_clock={
            "wallclock_at": "2026-08-03T07:04:59.900000Z",
            "monotonic_ns": 101_900_000_000,
            "boot_id": "00000000-0000-4000-8000-000000000001",
            "clock_source": "CLOCK_BOOTTIME",
        },
        legacy_shadow_root=staging.parents[1],
        recovery_state_root=state_root,
    )
    rejected = _invoke_recovery_helper(
        new_clock_helper,
        operation="resume-recovery",
        key_id=key_id,
        payload=_resume_request(),
    )
    assert rejected.returncode == 2, rejected.stderr.decode()
    assert not committed_path.exists()
    assert prepared_path.read_bytes() == seq1_payload
    assert committed_seed_path.read_bytes() == committed_seed_payload
    assert (staging / "recovery-marker.json").read_bytes() == marker_payload
    assert (staging / "finalization-receipt.json").read_bytes() == receipt_payload

    _replace_protected_json(prepared_path, durable_payload)
    no_clock_helper = _write_protected_signer_fixture(
        tmp_path,
        key_manifest=tmp_path / "shadow-report-keys.json",
        trusted_clocks=(),
        legacy_shadow_root=staging.parents[1],
        recovery_state_root=state_root,
    )
    repaired = _invoke_recovery_helper(
        no_clock_helper,
        operation="resume-recovery",
        key_id=key_id,
        payload=_resume_request(),
    )
    assert repaired.returncode == 0, repaired.stderr.decode()
    assert committed_path.read_bytes() == committed_payload
    assert prepared_path.read_bytes() == durable_payload
    assert committed_seed_path.read_bytes() == committed_seed_payload
    assert (staging / "recovery-marker.json").read_bytes() == marker_payload
    assert (staging / "finalization-receipt.json").read_bytes() == receipt_payload


def test_root_seed_bound_repair_cleans_repeated_sigkill_replace_temps(
    tmp_path: Path,
) -> None:
    (
        key_id,
        _helper,
        state_root,
        staging,
        prepared_path,
        _seed_path,
        committed_path,
    ) = _committed_prepared_case(tmp_path)
    durable_payload = prepared_path.read_bytes()
    committed_payload = committed_path.read_bytes()
    torn_payload = committed_payload[: len(committed_payload) // 2]
    replace_pattern = f".{committed_path.name}.replace-*"

    for head_state in ("missing", "torn"):
        for kill_fault_point in (
            "committed-repair-after-temp-fsync",
            "committed-repair-before-rename",
        ):
            if head_state == "missing":
                committed_path.unlink()
            else:
                _replace_protected_json(committed_path, torn_payload)
            for _attempt in range(5):
                killing_helper = _write_protected_signer_fixture(
                    tmp_path,
                    key_manifest=tmp_path / "shadow-report-keys.json",
                    trusted_clocks=(),
                    legacy_shadow_root=staging.parents[1],
                    recovery_state_root=state_root,
                    kill_fault_point=kill_fault_point,
                )
                killed = _invoke_recovery_helper(
                    killing_helper,
                    operation="resume-recovery",
                    key_id=key_id,
                    payload=_resume_request(),
                )
                assert killed.returncode == -signal.SIGKILL
                if head_state == "missing":
                    assert not committed_path.exists()
                else:
                    assert committed_path.read_bytes() == torn_payload
                assert len(tuple(state_root.glob(replace_pattern))) == 1

            recovery_helper = _write_protected_signer_fixture(
                tmp_path,
                key_manifest=tmp_path / "shadow-report-keys.json",
                trusted_clocks=(),
                legacy_shadow_root=staging.parents[1],
                recovery_state_root=state_root,
            )
            recovered = _invoke_recovery_helper(
                recovery_helper,
                operation="resume-recovery",
                key_id=key_id,
                payload=_resume_request(),
            )
            assert recovered.returncode == 0, (
                head_state,
                kill_fault_point,
                recovered.stderr.decode(),
            )
            assert committed_path.read_bytes() == committed_payload
            assert prepared_path.read_bytes() == durable_payload
            assert not tuple(state_root.glob(replace_pattern))


def test_root_seed_bound_repair_preserves_unknown_replace_state(
    tmp_path: Path,
) -> None:
    (
        key_id,
        _helper,
        state_root,
        staging,
        prepared_path,
        _seed_path,
        committed_path,
    ) = _committed_prepared_case(tmp_path)
    durable_payload = prepared_path.read_bytes()
    committed_payload = committed_path.read_bytes()
    torn_payload = committed_payload[: len(committed_payload) // 2]
    _replace_protected_json(committed_path, torn_payload)
    unknown = state_root / f".{committed_path.name}.replace-not-a-transaction-id"
    unknown.write_bytes(b"untrusted replacement")
    unknown.chmod(0o600)
    helper = _write_protected_signer_fixture(
        tmp_path,
        key_manifest=tmp_path / "shadow-report-keys.json",
        trusted_clocks=(),
        legacy_shadow_root=staging.parents[1],
        recovery_state_root=state_root,
    )

    rejected = _invoke_recovery_helper(
        helper,
        operation="resume-recovery",
        key_id=key_id,
        payload=_resume_request(),
    )
    assert rejected.returncode == 2, rejected.stderr.decode()
    assert unknown.read_bytes() == b"untrusted replacement"
    assert committed_path.read_bytes() == torn_payload
    assert prepared_path.read_bytes() == durable_payload


def test_root_recovery_revokes_unchecked_receipt_after_window(
    tmp_path: Path,
) -> None:
    key_id = "protected-shadow-v1"
    private_key, _public_key = _keypair(tmp_path, key_id)
    manifest = _write_shadow_key_manifest(
        tmp_path,
        private_key=private_key,
        key_id=key_id,
        recovery_calendar=_write_recovery_calendar(tmp_path),
    )
    legacy_shadow_root = tmp_path / "legacy-shadow"
    recovery_state_root = tmp_path / "recovery-state"
    legacy_shadow_root.mkdir(mode=0o700)
    recovery_state_root.mkdir(mode=0o700)
    capture_helper = _write_protected_signer_fixture(
        tmp_path,
        key_manifest=manifest,
        trusted_clock={
            "wallclock_at": "2026-08-03T07:04:58.000000Z",
            "monotonic_ns": 100_000_000_000,
            "boot_id": "00000000-0000-4000-8000-000000000001",
            "clock_source": "CLOCK_BOOTTIME",
        },
        legacy_shadow_root=legacy_shadow_root,
        recovery_state_root=recovery_state_root,
    )
    captured = _invoke_recovery_helper(
        capture_helper,
        operation="capture-recovery",
        key_id=key_id,
        payload=_capture_request(),
    )
    assert captured.returncode == 0, captured.stderr.decode()
    capture_response = json.loads(captured.stdout)
    staging, batch_digest, input_identity = _write_complete_monitor_staging(
        legacy_shadow_root,
        capture_response,
    )
    request = _finish_request(
        capture_response,
        batch_digest=batch_digest,
        input_identity=input_identity,
    )
    faulty_helper = _write_protected_signer_fixture(
        tmp_path,
        key_manifest=manifest,
        trusted_clock={
            "wallclock_at": "2026-08-03T07:04:59.000000Z",
            "monotonic_ns": 101_000_000_000,
            "boot_id": "00000000-0000-4000-8000-000000000001",
            "clock_source": "CLOCK_BOOTTIME",
        },
        legacy_shadow_root=legacy_shadow_root,
        recovery_state_root=recovery_state_root,
        fault_point="after-finalization-receipt-fsync",
    )
    interrupted = _invoke_recovery_helper(
        faulty_helper,
        operation="sign-recovery",
        key_id=key_id,
        payload=request,
    )
    assert interrupted.returncode == 2
    assert not (staging / "finalization-receipt.json").exists()
    assert (staging / ".finalization-receipt.pending").is_file()

    late_helper = _write_protected_signer_fixture(
        tmp_path,
        key_manifest=manifest,
        trusted_clock={
            "wallclock_at": "2026-08-03T07:15:00.000000Z",
            "monotonic_ns": 761_000_000_000,
            "boot_id": "00000000-0000-4000-8000-000000000001",
            "clock_source": "CLOCK_BOOTTIME",
        },
        legacy_shadow_root=legacy_shadow_root,
        recovery_state_root=recovery_state_root,
    )
    rejected = _invoke_recovery_helper(
        late_helper,
        operation="resume-recovery",
        key_id=key_id,
        payload=_resume_request(),
    )

    assert rejected.returncode == 2
    assert b"window" in rejected.stderr.lower()
    assert not (staging / "finalization-receipt.json").exists()
    assert not (staging / ".finalization-receipt.pending").exists()

    class MarkerOnlyVerifier:
        @staticmethod
        def verify(_marker: object) -> bool:
            return True

        @staticmethod
        def verify_finalization(_receipt: object) -> bool:
            return True

    with pytest.raises(LegacyShadowExportUnavailableError, match="finalization"):
        legacy_shadow_export_module._load_accepted_legacy_shadow_session(
            export_root=legacy_shadow_root / "monitor",
            session=staging,
            trade_date=date(2026, 8, 3),
            expected_source_id="legacy-monitor-events",
            expected_commit=COMMIT,
            allowed_modes=frozenset({0o700}),
            recovery_verifier=MarkerOnlyVerifier(),
        )


@pytest.mark.parametrize(
    ("fault_point", "complete_proof"),
    (
        ("marker-write", False),
        ("finalization-receipt-write", False),
        ("before-ledger-commit", True),
        ("ledger-commit-write", True),
    ),
)
def test_root_recovery_late_resume_only_closes_complete_finalization(
    tmp_path: Path,
    fault_point: str,
    complete_proof: bool,
) -> None:
    key_id = "protected-shadow-v1"
    private_key, public_key = _keypair(tmp_path, key_id)
    manifest = _write_shadow_key_manifest(
        tmp_path,
        private_key=private_key,
        key_id=key_id,
        recovery_calendar=_write_recovery_calendar(tmp_path),
    )
    legacy_shadow_root = tmp_path / "legacy-shadow"
    recovery_state_root = tmp_path / "recovery-state"
    legacy_shadow_root.mkdir(mode=0o700)
    recovery_state_root.mkdir(mode=0o700)
    capture_helper = _write_protected_signer_fixture(
        tmp_path,
        key_manifest=manifest,
        trusted_clock={
            "wallclock_at": "2026-08-03T07:04:58.000000Z",
            "monotonic_ns": 100_000_000_000,
            "boot_id": "00000000-0000-4000-8000-000000000001",
            "clock_source": "CLOCK_BOOTTIME",
        },
        legacy_shadow_root=legacy_shadow_root,
        recovery_state_root=recovery_state_root,
    )
    captured = _invoke_recovery_helper(
        capture_helper,
        operation="capture-recovery",
        key_id=key_id,
        payload=_capture_request(),
    )
    assert captured.returncode == 0, captured.stderr.decode()
    capture_response = json.loads(captured.stdout)
    staging, batch_digest, input_identity = _write_complete_monitor_staging(
        legacy_shadow_root,
        capture_response,
    )
    faulty_helper = _write_protected_signer_fixture(
        tmp_path,
        key_manifest=manifest,
        trusted_clock={
            "wallclock_at": "2026-08-03T07:04:59.000000Z",
            "monotonic_ns": 101_000_000_000,
            "boot_id": "00000000-0000-4000-8000-000000000001",
            "clock_source": "CLOCK_BOOTTIME",
        },
        legacy_shadow_root=legacy_shadow_root,
        recovery_state_root=recovery_state_root,
        fault_point=fault_point,
    )
    interrupted = _invoke_recovery_helper(
        faulty_helper,
        operation="sign-recovery",
        key_id=key_id,
        payload=_finish_request(
            capture_response,
            batch_digest=batch_digest,
            input_identity=input_identity,
        ),
    )
    assert interrupted.returncode == 2

    late_helper = _write_protected_signer_fixture(
        tmp_path,
        key_manifest=manifest,
        trusted_clock={
            "wallclock_at": "2026-08-03T07:15:00.000000Z",
            "monotonic_ns": 762_000_000_000,
            "boot_id": "00000000-0000-4000-8000-000000000001",
            "clock_source": "CLOCK_BOOTTIME",
        },
        legacy_shadow_root=legacy_shadow_root,
        recovery_state_root=recovery_state_root,
    )
    recovered = _invoke_recovery_helper(
        late_helper,
        operation="resume-recovery",
        key_id=key_id,
        payload=_resume_request(),
    )

    if complete_proof:
        assert recovered.returncode == 0, recovered.stderr.decode()
        assert (staging / "finalization-receipt.json").is_file()
        assert len(tuple(recovery_state_root.glob("*.committed.json"))) == 1
        return

    assert recovered.returncode == 2
    assert b"window" in recovered.stderr.lower()
    assert not tuple(recovery_state_root.glob("*.committed.json"))
    verifier = Ed25519LegacyShadowRecoveryKeyring(
        active_key_id=key_id,
        active_public_key=public_key,
    )
    with pytest.raises(LegacyShadowExportUnavailableError):
        legacy_shadow_export_module._load_accepted_legacy_shadow_session(
            export_root=legacy_shadow_root / "monitor",
            session=staging,
            trade_date=date(2026, 8, 3),
            expected_source_id="legacy-monitor-events",
            expected_commit=COMMIT,
            allowed_modes=frozenset({0o700}),
            recovery_verifier=verifier,
        )


def test_writer_restart_uses_controlled_resume_without_source_replay(
    tmp_path: Path,
) -> None:
    key_id = "protected-shadow-v1"
    private_key, public_key = _keypair(tmp_path, key_id)
    manifest = _write_shadow_key_manifest(
        tmp_path,
        private_key=private_key,
        key_id=key_id,
        recovery_calendar=_write_recovery_calendar(tmp_path),
    )
    legacy_shadow_root = (tmp_path / "legacy-shadow").resolve()
    recovery_state_root = tmp_path / "recovery-state"
    legacy_shadow_root.mkdir(mode=0o700)
    recovery_state_root.mkdir(mode=0o700)

    class WritableTestRecoverySigner(Ed25519LegacyShadowRecoverySigner):
        def resume(
            self,
            binding: LegacyShadowRecoveryResumeBinding,
            *,
            staging_root: Path,
        ) -> LegacyShadowRecoveryMarker:
            marker = super().resume(binding, staging_root=staging_root)
            (staging_root / binding.staging_name).chmod(0o700)
            return marker

    def dependencies(
        helper: Path,
        *,
        wallclock_at: datetime,
        monotonic_ns: int,
    ) -> LegacyShadowTestDependencies:
        verifier = Ed25519LegacyShadowRecoveryKeyring(
            active_key_id=key_id,
            active_public_key=public_key,
        )
        signer = WritableTestRecoverySigner(
            key_id=key_id,
            client=SecureShadowSigningClient(
                command=(sys.executable, str(helper)),
                key_id=key_id,
                timeout_seconds=5.0,
            ),
        )
        return LegacyShadowTestDependencies(
            wall_clock=lambda: wallclock_at,
            monotonic_ns=lambda: monotonic_ns,
            boot_id=lambda: "00000000-0000-4000-8000-000000000001",
            recovery_signer=signer,
            recovery_verifier=verifier,
            filesystem_policy=legacy_shadow_test_filesystem_policy(),
        )

    faulty_helper = _write_protected_signer_fixture(
        tmp_path,
        key_manifest=manifest,
        trusted_clock={
            "wallclock_at": "2026-08-03T07:04:59.000000Z",
            "monotonic_ns": 101_000_000_000,
            "boot_id": "00000000-0000-4000-8000-000000000001",
            "clock_source": "CLOCK_BOOTTIME",
        },
        legacy_shadow_root=legacy_shadow_root,
        recovery_state_root=recovery_state_root,
        fault_point="marker-write",
    )
    export_root = legacy_shadow_root / "monitor"
    with pytest.raises(RuntimeError, match="signing failed") as raised:
        publish_legacy_monitor_export(
            root=export_root,
            trade_date=date(2026, 8, 3),
            rows=(
                {
                    "trade_date": date(2026, 8, 3),
                    "ts_code": "600001.SH",
                    "level": "attack_strong_carry",
                    "trigger_time": datetime(2026, 8, 3, 14, 58),
                    "trigger_price": 10.1,
                },
            ),
            producer_commit=COMMIT,
            producer_version="legacy-monitor-shadow-export/v1",
            dependencies=dependencies(
                faulty_helper,
                wallclock_at=datetime(2026, 8, 3, 7, 4, 59, tzinfo=UTC),
                monotonic_ns=101_000_000_000,
            ),
        )
    staging = tuple(export_root.glob(".staging-*"))
    assert len(staging) == 1, repr(raised.value.__cause__)
    assert (staging[0] / "recovery-marker.json").is_file()
    assert not (staging[0] / "finalization-receipt.json").exists()

    recovery_helper = _write_protected_signer_fixture(
        tmp_path,
        key_manifest=manifest,
        trusted_clock={
            "wallclock_at": "2026-08-03T07:04:59.500000Z",
            "monotonic_ns": 101_500_000_000,
            "boot_id": "00000000-0000-4000-8000-000000000001",
            "clock_source": "CLOCK_BOOTTIME",
        },
        legacy_shadow_root=legacy_shadow_root,
        recovery_state_root=recovery_state_root,
    )
    published = recover_legacy_shadow_export(
        root=export_root,
        trade_date=date(2026, 8, 3),
        expected_source_id="legacy-monitor-events",
        expected_commit=COMMIT,
        dependencies=dependencies(
            recovery_helper,
            wallclock_at=datetime(2026, 8, 3, 7, 4, 59, 500_000, tzinfo=UTC),
            monotonic_ns=101_500_000_000,
        ),
    )

    assert published == export_root / "2026-08-03"
    assert (published / "finalization-receipt.json").is_file()
    assert not staging[0].exists()


def test_root_recovery_signer_recomputes_the_complete_staging_digest(
    tmp_path: Path,
) -> None:
    key_id = "protected-shadow-v1"
    private_key, _public_key = _keypair(tmp_path, key_id)
    manifest = _write_shadow_key_manifest(
        tmp_path,
        private_key=private_key,
        key_id=key_id,
        recovery_calendar=_write_recovery_calendar(tmp_path),
    )
    legacy_shadow_root = tmp_path / "legacy-shadow"
    recovery_state_root = tmp_path / "recovery-state"
    legacy_shadow_root.mkdir(mode=0o700)
    recovery_state_root.mkdir(mode=0o700)
    helper = _write_protected_signer_fixture(
        tmp_path,
        key_manifest=manifest,
        trusted_clock={
            "wallclock_at": "2026-08-03T07:04:59.000000Z",
            "monotonic_ns": 100_000_000_000,
            "boot_id": "00000000-0000-4000-8000-000000000001",
            "clock_source": "CLOCK_BOOTTIME",
        },
        legacy_shadow_root=legacy_shadow_root,
        recovery_state_root=recovery_state_root,
    )
    captured = _invoke_recovery_helper(
        helper,
        operation="capture-recovery",
        key_id=key_id,
        payload=_capture_request(),
    )
    assert captured.returncode == 0, captured.stderr.decode()
    _staging, actual_digest, input_identity = _write_complete_monitor_staging(
        legacy_shadow_root,
        json.loads(captured.stdout),
    )
    assert actual_digest != "2" * 64

    rejected = _invoke_recovery_helper(
        helper,
        operation="sign-recovery",
        key_id=key_id,
        payload=_finish_request(
            json.loads(captured.stdout),
            input_identity=input_identity,
        ),
    )

    assert rejected.returncode == 2
    assert rejected.stdout == b""
    assert b"digest" in rejected.stderr.lower()

    forged_input = _invoke_recovery_helper(
        helper,
        operation="sign-recovery",
        key_id=key_id,
        payload=_finish_request(
            json.loads(captured.stdout),
            batch_digest=actual_digest,
            input_identity="f" * 64,
        ),
    )
    assert forged_input.returncode == 2
    assert forged_input.stdout == b""
    assert b"digest" in forged_input.stderr.lower() or b"binding" in forged_input.stderr.lower()


def test_root_recovery_signer_injects_trusted_capture_and_completion_times(
    tmp_path: Path,
) -> None:
    key_id = "protected-shadow-v1"
    private_key, public_key = _keypair(tmp_path, key_id)
    manifest = _write_shadow_key_manifest(
        tmp_path,
        private_key=private_key,
        key_id=key_id,
        recovery_calendar=_write_recovery_calendar(tmp_path),
    )
    legacy_shadow_root = tmp_path / "legacy-shadow"
    recovery_state_root = tmp_path / "recovery-state"
    legacy_shadow_root.mkdir(mode=0o700)
    recovery_state_root.mkdir(mode=0o700)
    capture_helper = _write_protected_signer_fixture(
        tmp_path,
        key_manifest=manifest,
        trusted_clock={
            "wallclock_at": "2026-08-03T07:04:58.100000Z",
            "monotonic_ns": 100_000_000_000,
            "boot_id": "00000000-0000-4000-8000-000000000001",
            "clock_source": "CLOCK_BOOTTIME",
        },
        legacy_shadow_root=legacy_shadow_root,
        recovery_state_root=recovery_state_root,
    )
    captured = _invoke_recovery_helper(
        capture_helper,
        operation="capture-recovery",
        key_id=key_id,
        payload=_capture_request(),
    )
    assert captured.returncode == 0, captured.stderr.decode()
    capture_response = json.loads(captured.stdout)
    staging, batch_digest, input_identity = _write_complete_monitor_staging(
        legacy_shadow_root,
        capture_response,
    )

    finish_helper = _write_protected_signer_fixture(
        tmp_path,
        key_manifest=manifest,
        trusted_clock={
            "wallclock_at": "2026-08-03T07:04:59.900000Z",
            "monotonic_ns": 101_800_000_000,
            "boot_id": "00000000-0000-4000-8000-000000000001",
            "clock_source": "CLOCK_BOOTTIME",
        },
        legacy_shadow_root=legacy_shadow_root,
        recovery_state_root=recovery_state_root,
    )
    completed = _invoke_recovery_helper(
        finish_helper,
        operation="sign-recovery",
        key_id=key_id,
        payload=_finish_request(
            capture_response,
            batch_digest=batch_digest,
            input_identity=input_identity,
        ),
    )
    assert completed.returncode == 0, completed.stderr.decode()
    response = json.loads(completed.stdout)
    signed_payload = json.loads(base64.b64decode(response["signed_payload_base64"]))
    claims = signed_payload["claims"]
    assert claims["captured_at"] == "2026-08-03T07:04:58.100000Z"
    assert claims["produced_at"] == "2026-08-03T07:04:59.900000Z"
    assert claims["captured_monotonic_ns"] == 100_000_000_000
    assert claims["produced_monotonic_ns"] == 101_800_000_000
    assert claims["boot_id"] == "00000000-0000-4000-8000-000000000001"
    staging_stat = staging.stat()
    assert claims["directory_device"] == staging_stat.st_dev
    assert claims["directory_inode"] == staging_stat.st_ino
    assert claims["batch_digest"] == batch_digest
    assert claims["artifact_digests"] == {
        filename: hashlib.sha256((staging / filename).read_bytes()).hexdigest()
        for filename in (
            "completion.json",
            "events.json",
            "manifest.json",
            "records.jsonl",
        )
    }
    marker = json.loads((staging / "recovery-marker.json").read_bytes())
    assert (staging / "finalization-receipt.json").is_file()
    validated_marker = LegacyShadowRecoveryMarker.model_validate(marker)
    assert marker["marker_id"] == canonical_sha256(
        validated_marker.model_dump(mode="python", exclude={"marker_id"})
    )
    assert (staging.stat().st_mode & 0o777) == 0o555

    replayed = _invoke_recovery_helper(
        finish_helper,
        operation="sign-recovery",
        key_id=key_id,
        payload=_finish_request(
            capture_response,
            batch_digest=batch_digest,
            input_identity=input_identity,
        ),
    )
    assert replayed.returncode == 0, replayed.stderr.decode()
    assert replayed.stdout == completed.stdout

    verifier = Ed25519LegacyShadowRecoveryKeyring(
        active_key_id=key_id,
        active_public_key=public_key,
    )
    accepted = legacy_shadow_export_module._load_accepted_legacy_shadow_session(
        export_root=legacy_shadow_root / "monitor",
        session=staging,
        trade_date=date(2026, 8, 3),
        expected_source_id="legacy-monitor-events",
        expected_commit=COMMIT,
        allowed_modes=frozenset({0o555}),
        recovery_verifier=verifier,
    )
    assert accepted.manifest.input_identity == input_identity

    records_path = staging / "events.json"
    records_path.chmod(0o600)
    records_path.write_bytes(canonical_json_bytes([{"tampered": True}]))
    records_path.chmod(0o444)
    with pytest.raises(LegacyShadowExportUnavailableError, match="artifact digest"):
        legacy_shadow_export_module._load_accepted_legacy_shadow_session(
            export_root=legacy_shadow_root / "monitor",
            session=staging,
            trade_date=date(2026, 8, 3),
            expected_source_id="legacy-monitor-events",
            expected_commit=COMMIT,
            allowed_modes=frozenset({0o555}),
            recovery_verifier=verifier,
        )


def test_root_signer_preflight_requires_an_untampered_recovery_calendar(
    tmp_path: Path,
) -> None:
    key_id = "protected-shadow-v1"
    private_key, _public_key = _keypair(tmp_path, key_id)
    calendar = _write_recovery_calendar(tmp_path)
    manifest = _write_shadow_key_manifest(
        tmp_path,
        private_key=private_key,
        key_id=key_id,
        recovery_calendar=calendar,
    )
    helper = _write_protected_signer_fixture(tmp_path, key_manifest=manifest)

    accepted = subprocess.run(
        (sys.executable, str(helper), "--validate-key-material"),
        check=False,
        capture_output=True,
    )
    assert accepted.returncode == 0, accepted.stderr.decode()

    document = json.loads(calendar.read_bytes())
    document["open_dates"] = []
    calendar.write_bytes(canonical_json_bytes(document))
    rejected = subprocess.run(
        (sys.executable, str(helper), "--validate-key-material"),
        check=False,
        capture_output=True,
    )
    assert rejected.returncode == 2
    assert b"calendar" in rejected.stderr.lower()


def _claims() -> CompletionAttestationClaims:
    return CompletionAttestationClaims(
        completion_receipt_body_sha256="2" * 64,
        trade_date=date(2026, 8, 3),
        session_close_at=datetime(2026, 8, 3, 7, 0, tzinfo=UTC),
        source_id="strategy.n_shape.v1",
        input_identity="1" * 64,
        strategy_id="n_shape",
        strategy_version=1,
        strategy_registration_fingerprint="2" * 64,
        strategy_spec_fingerprint="3" * 64,
        executable_fingerprint="4" * 64,
        candidate_schema_fingerprint="5" * 64,
        feature_registration_fingerprint="6" * 64,
        feature_contract_fingerprint="7" * 64,
        routing_policy_fingerprint="8" * 64,
        producer_manifest_fingerprint="9" * 64,
        producer_commit=COMMIT,
        producer_version="shadow-ed25519-test",
        producer_service_id="strategy-live",
        producer_instance_id="instance-a",
        calendar_generation_id="a" * 64,
        feature_source_generation_id="b" * 64,
        feature_close_marker_id="c" * 64,
        feature_segment_chain_hash="d" * 64,
        runner_generation_id="e" * 64,
        runner_segment_start_sequence=0,
        runner_segment_final_sequence=0,
        runner_segment_record_count=0,
        runner_segment_chain_hash="f" * 64,
        signal_authority_generation_id="0" * 64,
        route_receipts_id="1" * 64,
    )


def test_ed25519_completion_keyring_rotates_and_rejects_expired_or_tampered_keys(
    tmp_path: Path,
) -> None:
    private_v1, public_v1 = _keypair(tmp_path, "shadow-v1")
    private_v2, public_v2 = _keypair(tmp_path, "shadow-v2")
    claims = _claims()

    old_attestation = Ed25519CompletionAttestationSigner(
        key_id="shadow-v1",
        client=_OpenSslSigningClient(private_v1),
    ).issue(claims)
    active_attestation = Ed25519CompletionAttestationSigner(
        key_id="shadow-v2",
        client=_OpenSslSigningClient(private_v2),
    ).issue(claims)

    rotating = Ed25519CompletionAttestationKeyring(
        active_key_id="shadow-v2",
        active_public_key=public_v2,
        previous_public_keys={"shadow-v1": public_v1},
    )
    assert rotating.trusted_key_ids == ("shadow-v2", "shadow-v1")
    assert rotating.verify(old_attestation)
    assert rotating.verify(active_attestation)
    assert not rotating.verify(old_attestation.model_copy(update={"key_id": "shadow-v2"}))

    retired = Ed25519CompletionAttestationKeyring(
        active_key_id="shadow-v2",
        active_public_key=public_v2,
    )
    assert not retired.verify(old_attestation)
    assert retired.verify(active_attestation)


def _signed_completion_receipt(
    tmp_path: Path,
) -> tuple[
    ShadowSourceCompletionReceipt,
    Ed25519CompletionAttestationKeyring,
]:
    private_key, public_key = _keypair(tmp_path, "receipt-v2")
    base_claims = _claims()
    unsigned_receipt = ShadowSourceCompletionReceipt(
        evidence_origin="production",
        source="isolated",
        source_id=base_claims.source_id,
        trade_date=base_claims.trade_date,
        session_close_at=base_claims.session_close_at,
        complete_through=base_claims.session_close_at,
        input_identity=base_claims.input_identity,
        produced_at=base_claims.session_close_at + timedelta(minutes=10),
        producer_commit=base_claims.producer_commit,
        producer_version=base_claims.producer_version,
        producer_service_id=base_claims.producer_service_id,
        producer_instance_id=base_claims.producer_instance_id,
        runner_generation_id=base_claims.runner_generation_id,
        signal_authority_generation_id=base_claims.signal_authority_generation_id,
        calendar_generation_id=base_claims.calendar_generation_id,
        last_sequence=0,
        high_watermark=base_claims.runner_segment_final_sequence,
        route_receipts_id=base_claims.route_receipts_id,
        feature_source_generation_id=base_claims.feature_source_generation_id,
        feature_close_marker_id=base_claims.feature_close_marker_id,
        feature_segment_chain_hash=base_claims.feature_segment_chain_hash,
        segment_start_sequence=base_claims.runner_segment_start_sequence,
        segment_record_count=base_claims.runner_segment_record_count,
        segment_chain_hash=base_claims.runner_segment_chain_hash,
    )
    claims = base_claims.model_copy(
        update={
            "completion_receipt_body_sha256": shadow_completion_receipt_body_sha256(
                unsigned_receipt
            )
        }
    )
    attestation = Ed25519CompletionAttestationSigner(
        key_id="receipt-v2",
        client=_OpenSslSigningClient(private_key),
    ).issue(claims)
    receipt = ShadowSourceCompletionReceipt.model_validate(
        {
            **unsigned_receipt.model_dump(mode="python", exclude={"receipt_id"}),
            "completion_attestation": attestation,
        }
    )
    return receipt, Ed25519CompletionAttestationKeyring(
        active_key_id="receipt-v2",
        active_public_key=public_key,
    )


@pytest.mark.parametrize(
    "update",
    (
        {"produced_at": datetime(2026, 8, 3, 7, 11, tzinfo=UTC)},
        {"complete_through": datetime(2026, 8, 3, 6, 59, tzinfo=UTC)},
        {"source": "legacy"},
        {"evidence_origin": "test_fixture"},
        {"last_sequence": 1},
    ),
)
def test_completion_signature_rejects_any_receipt_body_tamper_after_id_recalculation(
    tmp_path: Path,
    update: dict[str, object],
) -> None:
    receipt, keyring = _signed_completion_receipt(tmp_path)
    changed = receipt.model_copy(update={**update, "receipt_id": None})
    recalculated_id = canonical_sha256(changed.model_dump(mode="python", exclude={"receipt_id"}))
    tampered = changed.model_copy(update={"receipt_id": recalculated_id})

    assert not verify_completion_attestation(tampered, keyring)


def test_old_production_completion_attestation_contract_fails_closed(
    tmp_path: Path,
) -> None:
    receipt, keyring = _signed_completion_receipt(tmp_path)
    assert receipt.completion_attestation is not None
    old_claims = receipt.completion_attestation.claims.model_copy(
        update={"contract": "runtime-completion-attestation/v1"}
    )
    old_attestation = receipt.completion_attestation.model_copy(update={"claims": old_claims})
    historical = receipt.model_copy(update={"completion_attestation": old_attestation})

    assert not verify_completion_attestation(historical, keyring)
