"""The external high-water authority helper and its fail-closed client.

Every test drives the real deploy/libexec helper in a separate process; there
is no in-process fake of the authority.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from rquant.lab_highwater_authority import (
    PRODUCTION_LAB_HIGHWATER_COMMAND,
    LabHighWaterAuthorityClient,
    LabHighWaterAuthorityConfig,
    LabHighWaterDegradedError,
    LabHighWaterRollbackError,
    load_highwater_trusted_keys,
)
from rquant.strict_json import canonical_json_bytes
from tests import highwater_ed25519_support
from tests.highwater_ed25519_support import resolve_openssl

ROOT = Path(__file__).resolve().parents[2]
HELPER = ROOT / "deploy" / "libexec" / "rquant-lab-highwater-authority"
STABLE_IDENTITY = "strategy-lab-test-ledger"
CODE_IDENTITY = "1" * 40
PROFILE_IDENTITY = "2" * 64


def _key_pair(root: Path, key_id: str) -> tuple[Path, bytes]:
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    private_key = root / f"{key_id}.private.pem"
    public_key = root / f"{key_id}.public.pem"
    if not private_key.exists():
        if public_key.exists():
            return private_key, public_key.read_bytes()
        subprocess.run(
            [
                resolve_openssl(),
                "genpkey",
                "-algorithm",
                "ED25519",
                "-out",
                str(private_key),
            ],
            check=True,
            capture_output=True,
        )
        os.chmod(private_key, 0o600)
        subprocess.run(
            [
                resolve_openssl(),
                "pkey",
                "-in",
                str(private_key),
                "-pubout",
                "-out",
                str(public_key),
            ],
            check=True,
            capture_output=True,
        )
    return private_key, public_key.read_bytes()


def _trusted_keys(root: Path, key_ids: set[str]) -> dict[str, bytes]:
    return {key_id: _key_pair(root / "key-material", key_id)[1] for key_id in key_ids}


def _sign(private_key: Path, payload: bytes) -> str:
    message = private_key.parent / f"{private_key.stem}.message"
    message.write_bytes(payload)
    message.chmod(0o600)
    result = subprocess.run(
        [
            resolve_openssl(),
            "pkeyutl",
            "-sign",
            "-rawin",
            "-inkey",
            str(private_key),
            "-in",
            str(message),
        ],
        check=True,
        capture_output=True,
    )
    return base64.b64encode(result.stdout).decode("ascii")


def _write_public_keyring(
    path: Path,
    *,
    active_key_id: str,
    generation: int,
    previous_manifest_hash: str,
    previous_key_ids: set[str] | None = None,
) -> Path:
    active_private, active_public = _key_pair(path.parent / "key-material", active_key_id)
    body = {
        "schema_version": 3,
        "generation": generation,
        "previous_manifest_hash": previous_manifest_hash,
        "active_key_id": active_key_id,
        "active_public_key": active_public.decode("utf-8"),
        "previous_public_keys": {
            key_id: _key_pair(path.parent / "key-material", key_id)[1].decode("utf-8")
            for key_id in sorted(previous_key_ids or set())
        },
    }
    manifest_hash = hashlib.sha256(canonical_json_bytes(body)).hexdigest()
    payload = {
        **body,
        "manifest_hash": manifest_hash,
        "signature": _sign(active_private, manifest_hash.encode("ascii")),
    }
    path.write_bytes(canonical_json_bytes(payload))
    path.chmod(0o600)
    return path


def _write_private_key_manifest(
    path: Path,
    *,
    active_key_id: str = "hw-v1",
    key_ids: set[str] | None = None,
) -> Path:
    resolved_key_ids = key_ids or {"hw-v1"}
    previous_key_ids = resolved_key_ids - {active_key_id}
    active_private, _active_public = _key_pair(path.parent / "key-material", active_key_id)
    previous_public_keys: dict[str, str] = {}
    for key_id in sorted(previous_key_ids):
        previous_private, previous_public = _key_pair(path.parent / "key-material", key_id)
        previous_public_keys[key_id] = previous_public.decode("utf-8")
        previous_private.unlink(missing_ok=True)
    payload = {
        "schema_version": 3,
        "generation": 2 if previous_key_ids else 1,
        "previous_manifest_hash": "1" * 64 if previous_key_ids else "0" * 64,
        "active_key_id": active_key_id,
        "active_private_key_path": str(active_private),
        "previous_public_keys": previous_public_keys,
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    os.chmod(path, 0o600)
    return path


def _write_private_key_manifest_v3(
    path: Path,
    *,
    active_key_id: str,
    generation: int,
    previous_manifest_hash: str,
    previous_key_ids: set[str] | None = None,
) -> Path:
    active_private, _active_public = _key_pair(path.parent / "key-material", active_key_id)
    previous_public_keys: dict[str, str] = {}
    for key_id in sorted(previous_key_ids or set()):
        previous_private, previous_public = _key_pair(path.parent / "key-material", key_id)
        previous_public_keys[key_id] = previous_public.decode("utf-8")
        previous_private.unlink(missing_ok=True)
    payload = {
        "schema_version": 3,
        "generation": generation,
        "previous_manifest_hash": previous_manifest_hash,
        "active_key_id": active_key_id,
        "active_private_key_path": str(active_private),
        "previous_public_keys": previous_public_keys,
    }
    path.write_bytes(canonical_json_bytes(payload))
    path.chmod(0o600)
    return path


def _export_public_keyring(
    keys_file: Path,
    output: Path,
    *,
    current_keyring: Path | None = None,
) -> Path:
    command = [
        sys.executable,
        str(HELPER),
        "--keys-file",
        str(keys_file),
        "--export-public-keyring",
    ]
    if current_keyring is not None:
        command.extend(["--current-keyring", str(current_keyring)])
    result = subprocess.run(
        command,
        check=False,
        capture_output=True,
    )
    assert result.returncode == 0, result.stderr.decode("utf-8", "replace")
    output.write_bytes(result.stdout)
    output.chmod(0o444)
    return output


def _client(
    tmp_path: Path,
    *,
    key_ids: set[str] | None = None,
    active_key_id: str = "hw-v1",
    trusted: dict[str, bytes] | None = None,
    command: tuple[str, ...] | None = None,
    timeout_seconds: float = 30.0,
    allow_identity_rotation: bool = False,
    code_identity: str = CODE_IDENTITY,
    profile_identity: str = PROFILE_IDENTITY,
) -> LabHighWaterAuthorityClient:
    keys_file = _write_private_key_manifest(
        tmp_path / "keys.json",
        active_key_id=active_key_id,
        key_ids=key_ids,
    )
    resolved_command = command or (
        sys.executable,
        str(HELPER),
        "--state-root",
        str(tmp_path / "state"),
        "--keys-file",
        str(keys_file),
    )
    trusted_keys = trusted if trusted is not None else _trusted_keys(tmp_path, key_ids or {"hw-v1"})
    return LabHighWaterAuthorityClient(
        LabHighWaterAuthorityConfig(
            command=resolved_command,
            stable_identity=STABLE_IDENTITY,
            code_identity=code_identity,
            profile_identity=profile_identity,
            trusted_key_provider=trusted_keys.get,
            active_key_id=active_key_id,
            timeout_seconds=timeout_seconds,
            allow_identity_rotation=allow_identity_rotation,
        )
    )


def _observe(
    client: LabHighWaterAuthorityClient,
    *,
    mutation_epoch: int = 5,
    chain_generation: int = 5,
    chain_head_hash: str = "3" * 64,
    receipt_hash: str = "4" * 64,
    receipt_kind: str = "incremental",
    database_generation: tuple[int, int] = (11, 22),
    schema_generation: int = 7,
):
    return client.observe(
        database_generation=database_generation,
        schema_generation=schema_generation,
        mutation_epoch=mutation_epoch,
        chain_generation=chain_generation,
        chain_head_hash=chain_head_hash,
        receipt_kind=receipt_kind,  # type: ignore[arg-type]
        receipt_hash=receipt_hash,
    )


def test_observe_advances_then_deduplicates_identical_watermarks(tmp_path: Path) -> None:
    client = _client(tmp_path)
    first = _observe(client)
    assert first.outcome == "advanced"
    assert first.high_water is not None
    assert first.high_water.sequence == 0
    assert first.high_water.receipt_hash == "4" * 64

    duplicate = _observe(client)
    assert duplicate.outcome == "unchanged"
    assert duplicate.high_water is not None
    assert duplicate.high_water.sequence == 0

    advanced = _observe(
        client,
        mutation_epoch=9,
        chain_generation=9,
        chain_head_hash="5" * 64,
        receipt_hash="6" * 64,
        receipt_kind="full",
    )
    assert advanced.outcome == "advanced"
    assert advanced.high_water is not None
    assert advanced.high_water.sequence == 1
    assert advanced.high_water.receipt_kind == "full"

    chain_lines = [
        json.loads(line)
        for line in ((tmp_path / "state").glob("*/chain.jsonl").__next__().read_text().splitlines())
    ]
    assert [record["sequence"] for record in chain_lines] == [0, 1]


def test_observe_rejects_chain_generation_rollback(tmp_path: Path) -> None:
    client = _client(tmp_path)
    _observe(client, mutation_epoch=9, chain_generation=9)
    with pytest.raises(LabHighWaterRollbackError, match="rolled back"):
        _observe(client, mutation_epoch=9, chain_generation=8)


def test_observe_rejects_in_place_chain_head_change(tmp_path: Path) -> None:
    client = _client(tmp_path)
    _observe(client)
    with pytest.raises(LabHighWaterRollbackError, match="changed in place"):
        _observe(client, chain_head_hash="9" * 64)


def test_observe_rejects_database_generation_change_without_rotation(tmp_path: Path) -> None:
    client = _client(tmp_path)
    _observe(client)
    with pytest.raises(LabHighWaterRollbackError, match="database generation changed"):
        _observe(client, database_generation=(11, 33))


def test_observe_requires_explicit_identity_rotation(tmp_path: Path) -> None:
    client = _client(tmp_path)
    _observe(client)
    blocked = _client(tmp_path, code_identity="8" * 40)
    with pytest.raises(LabHighWaterRollbackError, match="identity conflicts"):
        _observe(blocked)
    rotated = _client(tmp_path, code_identity="8" * 40, allow_identity_rotation=True)
    receipt = _observe(rotated)
    assert receipt.outcome == "advanced"
    assert receipt.high_water is not None
    assert receipt.high_water.sequence == 1
    assert receipt.high_water.code_identity == "8" * 40


def test_rotation_never_permits_watermark_rollback(tmp_path: Path) -> None:
    client = _client(tmp_path)
    _observe(client, mutation_epoch=9, chain_generation=9)
    rotated = _client(tmp_path, code_identity="8" * 40, allow_identity_rotation=True)
    with pytest.raises(LabHighWaterRollbackError, match="rolled back"):
        _observe(rotated, mutation_epoch=3, chain_generation=3)


def test_key_rotation_keeps_historical_chain_verifiable(tmp_path: Path) -> None:
    client = _client(tmp_path)
    _observe(client)

    both_key_ids = {"hw-v1", "hw-v2"}
    trusted = _trusted_keys(tmp_path, both_key_ids)
    rotated = _client(
        tmp_path,
        key_ids=both_key_ids,
        active_key_id="hw-v2",
        trusted=trusted,
    )
    receipt = _observe(rotated, mutation_epoch=9, chain_generation=9, chain_head_hash="5" * 64)
    assert receipt.outcome == "advanced"
    assert receipt.key_id == "hw-v2"

    stale_trust = _client(
        tmp_path,
        key_ids=both_key_ids,
        active_key_id="hw-v2",
        trusted={"hw-v1": trusted["hw-v1"]},
    )
    with pytest.raises(LabHighWaterDegradedError, match="not trusted"):
        _observe(stale_trust, mutation_epoch=9, chain_generation=9, chain_head_hash="5" * 64)


def test_live_invoke_rejects_previous_key_signature_for_current_nonce(tmp_path: Path) -> None:
    fake_root = tmp_path / "fake-authority"
    fake_keys = _write_private_key_manifest(
        fake_root / "fake-keys.json",
        active_key_id="hw-v1",
        key_ids={"hw-v1"},
    )
    previous_public = _key_pair(fake_root / "key-material", "hw-v1")[1]
    active_public = _key_pair(tmp_path / "client-key-material", "hw-v2")[1]
    client = _client(
        tmp_path / "client",
        key_ids={"hw-v2"},
        active_key_id="hw-v2",
        trusted={"hw-v1": previous_public, "hw-v2": active_public},
        command=(
            sys.executable,
            str(HELPER),
            "--state-root",
            str(fake_root / "state"),
            "--keys-file",
            str(fake_keys),
        ),
    )

    with pytest.raises(LabHighWaterDegradedError, match="active signing key"):
        _observe(client)


def test_previous_key_remains_valid_for_historical_immutable_receipt(tmp_path: Path) -> None:
    historical = _observe(_client(tmp_path, active_key_id="hw-v1", key_ids={"hw-v1"}))
    first_ring = load_highwater_trusted_keys(
        _write_public_keyring(
            tmp_path / "public-v1.json",
            active_key_id="hw-v1",
            generation=1,
            previous_manifest_hash="0" * 64,
        )
    )
    rotated_ring = load_highwater_trusted_keys(
        _write_public_keyring(
            tmp_path / "public-v2.json",
            active_key_id="hw-v2",
            generation=2,
            previous_manifest_hash=first_ring.manifest_hash,
            previous_key_ids={"hw-v1"},
        )
    )

    historical.verify(rotated_ring.get)
    assert rotated_ring.active_key_id == "hw-v2"
    assert rotated_ring.previous_key_ids == ("hw-v1",)


def test_helper_exports_rotation_keyring_without_previous_private_keys(tmp_path: Path) -> None:
    first_private_manifest = _write_private_key_manifest_v3(
        tmp_path / "private-v1.json",
        active_key_id="hw-v1",
        generation=1,
        previous_manifest_hash="0" * 64,
    )
    first = load_highwater_trusted_keys(
        _export_public_keyring(first_private_manifest, tmp_path / "public-v1.json")
    )
    assert first.active_key_id == "hw-v1"

    rotated_private_manifest = _write_private_key_manifest_v3(
        tmp_path / "private-v2.json",
        active_key_id="hw-v2",
        generation=2,
        previous_manifest_hash=first.manifest_hash,
        previous_key_ids={"hw-v1"},
    )
    rotated_payload = json.loads(rotated_private_manifest.read_text(encoding="utf-8"))
    assert "previous_private_keys" not in rotated_payload
    assert "hw-v1" not in str(rotated_payload.get("active_private_key_path"))

    rotated = load_highwater_trusted_keys(
        _export_public_keyring(
            rotated_private_manifest,
            tmp_path / "public-v2.json",
            current_keyring=tmp_path / "public-v1.json",
        )
    )
    assert rotated.generation == 2
    assert rotated.previous_manifest_hash == first.manifest_hash
    assert rotated.active_key_id == "hw-v2"
    assert rotated.previous_key_ids == ("hw-v1",)


def test_helper_rejects_manifest_that_retains_previous_private_key(tmp_path: Path) -> None:
    previous_private, _ = _key_pair(tmp_path / "key-material", "hw-v1")
    active_private, active_public = _key_pair(tmp_path / "key-material", "hw-v2")
    unsafe = tmp_path / "unsafe-private-manifest.json"
    unsafe.write_bytes(
        canonical_json_bytes(
            {
                "schema_version": 3,
                "generation": 2,
                "previous_manifest_hash": "1" * 64,
                "active_key_id": "hw-v2",
                "active_private_key_path": str(active_private),
                "previous_public_keys": {"hw-v1": active_public.decode("utf-8")},
                "previous_private_keys": {"hw-v1": str(previous_private)},
            }
        )
    )
    unsafe.chmod(0o600)

    result = subprocess.run(
        [
            sys.executable,
            str(HELPER),
            "--keys-file",
            str(unsafe),
            "--export-public-keyring",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "shape" in result.stderr


def test_helper_fails_closed_when_signing_key_is_dropped_from_authority(tmp_path: Path) -> None:
    client = _client(tmp_path)
    _observe(client)
    replaced = _client(
        tmp_path,
        key_ids={"hw-v2"},
        active_key_id="hw-v2",
        trusted=_trusted_keys(tmp_path, {"hw-v2"}),
    )
    with pytest.raises(LabHighWaterDegradedError, match="failed"):
        _observe(replaced)


def test_tampered_chain_fails_closed(tmp_path: Path) -> None:
    client = _client(tmp_path)
    _observe(client)
    chain_path = next((tmp_path / "state").glob("*/chain.jsonl"))
    record = json.loads(chain_path.read_text())
    record["mutation_epoch"] = 0
    chain_path.write_text(
        json.dumps(record, ensure_ascii=True, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(LabHighWaterDegradedError, match="failed"):
        _observe(client)


def test_truncated_chain_with_stale_current_fails_closed(tmp_path: Path) -> None:
    client = _client(tmp_path)
    _observe(client)
    _observe(client, mutation_epoch=9, chain_generation=9, chain_head_hash="5" * 64)
    chain_path = next((tmp_path / "state").glob("*/chain.jsonl"))
    first_line = chain_path.read_text().splitlines()[0]
    chain_path.write_text(first_line + "\n", encoding="utf-8")
    with pytest.raises(LabHighWaterDegradedError, match="failed"):
        _observe(client)


def test_replayed_receipt_fails_closed(tmp_path: Path) -> None:
    client = _client(tmp_path)
    receipt = _observe(client)
    capture = tmp_path / "captured-receipt.json"
    capture.write_bytes(
        json.dumps(
            receipt.model_dump(mode="json"),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    )
    replaying = _client(
        tmp_path,
        command=(
            sys.executable,
            "-c",
            "import sys; sys.stdout.buffer.write(open(sys.argv[1], 'rb').read())",
            str(capture),
        ),
    )
    with pytest.raises(LabHighWaterDegradedError, match="replayed"):
        _observe(replaying)


def test_hard_timeout_fails_closed(tmp_path: Path) -> None:
    client = _client(
        tmp_path,
        command=(sys.executable, "-c", "import time; time.sleep(30)"),
        timeout_seconds=0.5,
    )
    with pytest.raises(LabHighWaterDegradedError, match="hard timeout"):
        _observe(client)


def test_killed_authority_process_fails_closed(tmp_path: Path) -> None:
    client = _client(
        tmp_path,
        command=(
            sys.executable,
            "-c",
            "import os, signal; os.kill(os.getpid(), signal.SIGKILL)",
        ),
    )
    with pytest.raises(LabHighWaterDegradedError, match="failed"):
        _observe(client)


def test_missing_helper_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with monkeypatch.context() as resolver_patch:
        resolver_patch.setattr(highwater_ed25519_support.shutil, "which", lambda _name: None)
        resolver_patch.setattr(
            highwater_ed25519_support.pytest,
            "skip",
            lambda reason: (_ for _ in ()).throw(RuntimeError(reason)),
        )
        with pytest.raises(RuntimeError) as skipped:
            highwater_ed25519_support.resolve_openssl()
        assert "openssl" in str(skipped.value)
        assert "missing-openssl-value" not in str(skipped.value)

    client = _client(tmp_path, command=(str(tmp_path / "missing-helper"),))
    with pytest.raises(LabHighWaterDegradedError, match="unavailable|failed"):
        _observe(client)


def test_status_reports_current_watermark(tmp_path: Path) -> None:
    client = _client(tmp_path)
    status_before = client.status()
    assert status_before.outcome == "current"
    assert status_before.high_water is None
    _observe(client, mutation_epoch=9, chain_generation=9)
    status_after = client.status()
    assert status_after.high_water is not None
    assert status_after.high_water.chain_generation == 9


def test_remediation_requires_root_authorization_and_is_single_use(tmp_path: Path) -> None:
    client = _client(tmp_path)
    _observe(client)
    client.mark_degraded("full audit exceeded its resource budget")
    with pytest.raises(LabHighWaterRollbackError, match="remediation is required"):
        _observe(client)
    with pytest.raises(LabHighWaterRollbackError, match="not authorized"):
        client.authorize_remediation()

    state = next((tmp_path / "state").glob("*"))
    (state / "remediation.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "stable_identity": STABLE_IDENTITY,
                "code_identity": CODE_IDENTITY,
                "profile_identity": PROFILE_IDENTITY,
            },
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )
    receipt = client.authorize_remediation()

    assert receipt.operation == "remediate"
    assert receipt.outcome == "authorized"
    assert not (state / "remediation.json").exists()
    with pytest.raises(LabHighWaterRollbackError, match="not pending"):
        client.authorize_remediation()
    assert _observe(client).outcome == "unchanged"


def test_load_highwater_trusted_keys_round_trip(tmp_path: Path) -> None:
    path = _write_public_keyring(
        tmp_path / "trusted-public-keys.json",
        active_key_id="hw-v1",
        generation=1,
        previous_manifest_hash="0" * 64,
    )
    keyring = load_highwater_trusted_keys(path)
    assert keyring.active_key_id == "hw-v1"
    assert keyring.previous_key_ids == ()
    assert keyring["hw-v1"] == _trusted_keys(tmp_path, {"hw-v1"})["hw-v1"]
    os.chmod(path, 0o644)
    with pytest.raises(ValueError, match="unsafe"):
        load_highwater_trusted_keys(path)


def test_public_keyring_tamper_and_schema_downgrade_fail_closed(tmp_path: Path) -> None:
    path = _write_public_keyring(
        tmp_path / "trusted-public-keys.json",
        active_key_id="hw-v2",
        generation=2,
        previous_manifest_hash="1" * 64,
        previous_key_ids={"hw-v1"},
    )
    original = json.loads(path.read_text(encoding="utf-8"))
    tampered = {**original, "active_key_id": "hw-v1"}
    path.write_bytes(canonical_json_bytes(tampered))
    with pytest.raises(ValueError, match="hash|signature|active"):
        load_highwater_trusted_keys(path)

    downgraded = {
        "schema_version": 2,
        "keys": {"hw-v1": _trusted_keys(tmp_path, {"hw-v1"})["hw-v1"].decode("utf-8")},
    }
    path.write_bytes(canonical_json_bytes(downgraded))
    with pytest.raises(ValueError, match="shape|version"):
        load_highwater_trusted_keys(path)


def test_runner_public_keyring_rejects_legacy_hmac_material_and_private_keys(
    tmp_path: Path,
) -> None:
    legacy = tmp_path / "legacy-hmac.json"
    legacy.write_text(
        '{"schema_version":1,"keys":{"hw-v1":"' + "aa" * 32 + '"}}',
        encoding="utf-8",
    )
    os.chmod(legacy, 0o600)
    with pytest.raises(ValueError, match="shape"):
        load_highwater_trusted_keys(legacy)

    private_key, _public_key = _key_pair(tmp_path / "key-material", "hw-v1")
    private_as_public = tmp_path / "private-as-public.json"
    body = {
        "schema_version": 3,
        "generation": 1,
        "previous_manifest_hash": "0" * 64,
        "active_key_id": "hw-v1",
        "active_public_key": private_key.read_text(encoding="utf-8"),
        "previous_public_keys": {},
    }
    manifest_hash = hashlib.sha256(canonical_json_bytes(body)).hexdigest()
    private_as_public.write_bytes(
        canonical_json_bytes(
            {
                **body,
                "manifest_hash": manifest_hash,
                "signature": _sign(private_key, manifest_hash.encode("ascii")),
            }
        )
    )
    os.chmod(private_as_public, 0o600)
    with pytest.raises(ValueError, match="non-Ed25519"):
        load_highwater_trusted_keys(private_as_public)


def test_config_validation_rejects_bad_identity() -> None:
    with pytest.raises(ValueError, match="code_identity"):
        LabHighWaterAuthorityConfig(
            command=("helper",),
            stable_identity=STABLE_IDENTITY,
            code_identity="zz",
            profile_identity=PROFILE_IDENTITY,
            trusted_key_provider=lambda _key_id: None,
        )


@pytest.mark.parametrize(
    "command",
    (
        ("/tmp/runner-owned-helper",),
        (*PRODUCTION_LAB_HIGHWATER_COMMAND, "--state-root", "/tmp/runner-state"),
        (*PRODUCTION_LAB_HIGHWATER_COMMAND, "--keys-file", "/tmp/runner-keys.json"),
    ),
)
def test_production_config_rejects_runner_controlled_helper_and_parameters(
    command: tuple[str, ...],
) -> None:
    with pytest.raises(ValueError, match="fixed sudo helper"):
        LabHighWaterAuthorityConfig(
            command=command,
            stable_identity=STABLE_IDENTITY,
            code_identity=CODE_IDENTITY,
            profile_identity=PROFILE_IDENTITY,
            trusted_key_provider=lambda _key_id: None,
            production_mode=True,
        )


def test_production_client_fails_closed_when_controlled_identity_fixture_is_not_root_owned(
    tmp_path: Path,
) -> None:
    client = LabHighWaterAuthorityClient(
        LabHighWaterAuthorityConfig(
            command=PRODUCTION_LAB_HIGHWATER_COMMAND,
            stable_identity=STABLE_IDENTITY,
            code_identity=CODE_IDENTITY,
            profile_identity=PROFILE_IDENTITY,
            trusted_key_provider=lambda _key_id: None,
            production_mode=True,
            helper_identity_validator=lambda _command: (_ for _ in ()).throw(
                ValueError("production high-water helper is runner-owned")
            ),
        )
    )

    with pytest.raises(LabHighWaterDegradedError, match="runner-owned"):
        client.status()
