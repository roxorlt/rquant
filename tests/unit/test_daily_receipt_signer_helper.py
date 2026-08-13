from __future__ import annotations

import base64
import hashlib
import io
import json
import os
import runpy
import stat
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from rquant.runtime_shadow_validation import _verify_ed25519_signature
from rquant.strict_json import canonical_json_bytes

ROOT = Path(__file__).resolve().parents[2]
HELPER = ROOT / "deploy/libexec/rquant-daily-receipt-signer"
SUDO_PROBE = ROOT / "tests/support/daily_sudo_signer_probe.py"
STAGE_NAMESPACE = "rquant-daily-shadow-stage-completion-receipt"
RUN_NAMESPACE = "rquant-daily-shadow-run-completion-receipt"
GENESIS_MANIFEST_HASH = "0" * 64


def _openssl() -> str:
    if Path("/opt/homebrew/bin/openssl").exists():
        return "/opt/homebrew/bin/openssl"
    return "/usr/bin/openssl"


def _generate_private_key(root: Path, *, key_id: str) -> Path:
    root.mkdir(parents=True, mode=0o700, exist_ok=True)
    private_key = root / f"{key_id}.private.pem"
    subprocess.run(
        [_openssl(), "genpkey", "-algorithm", "ED25519", "-out", str(private_key)],
        check=True,
        capture_output=True,
    )
    private_key.chmod(0o600)
    return private_key


def _public_key(private_key: Path) -> str:
    return subprocess.run(
        [_openssl(), "pkey", "-in", str(private_key), "-pubout"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout


def _key_material(root: Path, *, active_key_id: str = "daily-v1") -> tuple[Path, str, str]:
    key_dir = root / "etc/rquant/daily-receipt"
    private_key = _generate_private_key(key_dir, key_id=active_key_id)
    public = _public_key(private_key)
    manifest = root / "etc/rquant/daily-receipt-keys.json"
    manifest.write_bytes(
        canonical_json_bytes(
            {
                "schema_version": 2,
                "generation": 1,
                "previous_manifest_hash": GENESIS_MANIFEST_HASH,
                "active_key_id": active_key_id,
                "active_private_key_path": str(private_key),
                "previous_public_keys": {},
            }
        )
    )
    manifest.chmod(0o600)
    return manifest, public, active_key_id


def _rotated_key_material(root: Path) -> tuple[Path, str, str, Path]:
    keys_path, previous_public, _previous_key_id = _key_material(root, active_key_id="daily-v1")
    module = runpy.run_path(str(HELPER))
    current_keyring = root / "etc/rquant/daily-receipt-trusted-keys.json"
    stdout = SimpleNamespace(buffer=io.BytesIO())
    original_stdout = sys.stdout
    try:
        sys.stdout = stdout  # type: ignore[assignment]
        module["_export_public_keyring"](keys_path)
    finally:
        sys.stdout = original_stdout
    current_keyring.write_bytes(stdout.buffer.getvalue())
    current_keyring.chmod(0o444)
    first = json.loads(current_keyring.read_text(encoding="utf-8"))

    key_dir = root / "etc/rquant/daily-receipt"
    private_key = _generate_private_key(key_dir, key_id="daily-v2")
    public = _public_key(private_key)
    keys_path.write_bytes(
        canonical_json_bytes(
            {
                "schema_version": 2,
                "generation": 2,
                "previous_manifest_hash": first["manifest_hash"],
                "active_key_id": "daily-v2",
                "active_private_key_path": str(private_key),
                "previous_public_keys": {"daily-v1": previous_public},
            }
        )
    )
    keys_path.chmod(0o600)
    return keys_path, public, "daily-v2", current_keyring


def _request(*, key_id: str, namespace: str, payload: bytes = b"daily payload") -> bytes:
    return canonical_json_bytes(
        {
            "schema_version": 1,
            "operation": "sign",
            "request_id": "a" * 64,
            "key_id": key_id,
            "namespace": namespace,
            "payload_base64": base64.b64encode(payload).decode("ascii"),
            "payload_sha256": hashlib.sha256(payload).hexdigest(),
        }
    )


def _stdin_operation(module: dict[str, object], keys_path: Path, request: bytes) -> str:
    stdin = SimpleNamespace(buffer=io.BytesIO(request))
    stdout = SimpleNamespace(buffer=io.BytesIO())
    original_stdin = sys.stdin
    original_stdout = sys.stdout
    try:
        sys.stdin = stdin  # type: ignore[assignment]
        sys.stdout = stdout  # type: ignore[assignment]
        return module["_stdin_operation"](keys_path)
    finally:
        sys.stdin = original_stdin
        sys.stdout = original_stdout


def test_no_new_privileges_blocks_legacy_sudo_signer_and_sudoers_denies_extra_argv() -> None:
    fixed_command = [
        "/usr/bin/sudo",
        "-n",
        "/usr/local/libexec/rquant-daily-receipt-signer",
    ]

    blocked = subprocess.run(
        [sys.executable, str(SUDO_PROBE), "--no-new-privileges", "true", *fixed_command],
        input=b"{}",
        capture_output=True,
        timeout=10,
    )
    assert blocked.returncode == 1
    assert b"NoNewPrivileges" in blocked.stderr

    forged = subprocess.run(
        [
            sys.executable,
            str(SUDO_PROBE),
            "--no-new-privileges",
            "false",
            *fixed_command,
            "--keys-file",
            "/tmp/attacker-keys.json",
        ],
        input=b"{}",
        capture_output=True,
        timeout=10,
    )
    assert forged.returncode == 2
    assert b"extra arguments" in forged.stderr


def test_stdin_helper_is_not_a_receipt_signing_authority(tmp_path: Path) -> None:
    module = runpy.run_path(str(HELPER))
    keys_path, _public_key, key_id = _key_material(tmp_path)
    source = HELPER.read_text(encoding="utf-8")

    for namespace in (STAGE_NAMESPACE, RUN_NAMESPACE):
        with pytest.raises(ValueError, match="socket authority"):
            _stdin_operation(
                module,
                keys_path,
                _request(key_id=key_id, namespace=namespace),
            )

    with pytest.raises(ValueError, match="socket authority"):
        _stdin_operation(
            module,
            keys_path,
            _request(key_id=key_id, namespace="rquant-shadow-report-receipt"),
        )

    with pytest.raises(ValueError, match="not allowed"):
        _stdin_operation(
            module,
            keys_path,
            canonical_json_bytes({"operation": "attest-key-material", "schema_version": 1}),
        )

    assert "_sign_request" not in module
    assert "_decode_request" not in module
    assert module["KEY_MATERIAL_OPERATIONS"] == (
        "validate-key-material",
        "export-public-keyring",
    )
    assert STAGE_NAMESPACE not in source
    assert RUN_NAMESPACE not in source


def test_stdin_helper_validates_key_material_without_signing(tmp_path: Path) -> None:
    module = runpy.run_path(str(HELPER))
    keys_path, _public_key, _key_id = _key_material(tmp_path)

    assert (
        _stdin_operation(
            module,
            keys_path,
            canonical_json_bytes({"operation": "validate-key-material", "schema_version": 1}),
        )
        == "validate-key-material"
    )


def test_active_public_key_verifies_keyring_and_previous_keys_are_history_only(
    tmp_path: Path,
) -> None:
    module = runpy.run_path(str(HELPER))
    keys_path, public_key, key_id, current_keyring = _rotated_key_material(tmp_path)
    previous_public_key = json.loads(current_keyring.read_text(encoding="utf-8"))[
        "active_public_key"
    ]

    stdout = SimpleNamespace(buffer=io.BytesIO())
    original_stdout = sys.stdout
    try:
        sys.stdout = stdout  # type: ignore[assignment]
        module["_export_public_keyring"](keys_path, current_keyring_path=current_keyring)
    finally:
        sys.stdout = original_stdout
    keyring = json.loads(stdout.buffer.getvalue())
    assert keyring["active_key_id"] == key_id
    assert keyring["active_public_key"] == public_key
    assert keyring["generation"] == 2
    assert (
        keyring["previous_manifest_hash"]
        == json.loads(current_keyring.read_text(encoding="utf-8"))["manifest_hash"]
    )
    assert keyring["previous_public_keys"].keys() == {"daily-v1"}
    assert keyring["previous_public_keys"]["daily-v1"] == previous_public_key
    assert len(keyring["manifest_hash"]) == 64
    assert keyring["signature"]
    assert "PRIVATE KEY" not in stdout.buffer.getvalue().decode("utf-8")

    # The rotated keyring is self-signed by the active key only; the retired key is
    # carried for history verification and can never attest the current manifest.
    assert _verify_ed25519_signature(
        public_key=public_key.encode("utf-8"),
        payload=keyring["manifest_hash"].encode("ascii"),
        signature=str(keyring["signature"]),
    )
    assert not _verify_ed25519_signature(
        public_key=previous_public_key.encode("utf-8"),
        payload=keyring["manifest_hash"].encode("ascii"),
        signature=str(keyring["signature"]),
    )


def test_public_keyring_rotation_rejects_history_drop_and_bad_chain(
    tmp_path: Path,
) -> None:
    module = runpy.run_path(str(HELPER))
    keys_path, _active_public_key, _key_id, current_keyring = _rotated_key_material(tmp_path)
    bad = json.loads(keys_path.read_text(encoding="utf-8"))

    bad["previous_manifest_hash"] = "f" * 64
    keys_path.write_bytes(canonical_json_bytes(bad))
    keys_path.chmod(0o600)
    with pytest.raises(ValueError, match="previous manifest"):
        module["_export_public_keyring"](keys_path, current_keyring_path=current_keyring)

    current = json.loads(current_keyring.read_text(encoding="utf-8"))
    bad["previous_manifest_hash"] = current["manifest_hash"]
    wrong_previous = _generate_private_key(
        tmp_path / "etc/rquant/daily-receipt",
        key_id="daily-wrong-previous",
    )
    bad["previous_public_keys"] = {"daily-v1": _public_key(wrong_previous)}
    keys_path.write_bytes(canonical_json_bytes(bad))
    keys_path.chmod(0o600)
    with pytest.raises(ValueError, match="dropped|historical"):
        module["_export_public_keyring"](keys_path, current_keyring_path=current_keyring)


def test_rejects_forgery_extra_argv_and_environment_injection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = runpy.run_path(str(HELPER))
    keys_path, _public_key, key_id = _key_material(tmp_path)
    tampered = json.loads(_request(key_id=key_id, namespace=STAGE_NAMESPACE))
    tampered["payload_sha256"] = "b" * 64
    with pytest.raises(ValueError, match="socket authority"):
        _stdin_operation(module, keys_path, canonical_json_bytes(tampered))

    monkeypatch.setenv("RQUANT_DAILY_RECEIPT_KEYS_FILE", str(keys_path))
    with pytest.raises(ValueError, match="arguments"):
        module["main"](["--keys-file", str(keys_path)])
    with pytest.raises(ValueError, match="arguments"):
        module["main"](["--export-public-keyring"])

    assert stat.S_IMODE(keys_path.stat().st_mode) == 0o600
    assert os.environ["RQUANT_DAILY_RECEIPT_KEYS_FILE"] == str(keys_path)


def test_rejects_duplicate_keys_and_noncanonical_json(tmp_path: Path) -> None:
    module = runpy.run_path(str(HELPER))
    keys_path, _public_key, key_id = _key_material(tmp_path)

    duplicate = (
        b'{"schema_version":1,"operation":"sign","operation":"sign",'
        b'"request_id":"'
        + (b"a" * 64)
        + b'","key_id":"'
        + key_id.encode("ascii")
        + b'","namespace":"'
        + STAGE_NAMESPACE.encode("ascii")
        + b'","payload_base64":"ZGFpbHk=","payload_sha256":"'
        + hashlib.sha256(b"daily").hexdigest().encode("ascii")
        + b'"}'
    )
    with pytest.raises(ValueError, match="duplicate"):
        _stdin_operation(module, keys_path, duplicate)

    noncanonical = json.dumps(
        json.loads(_request(key_id=key_id, namespace=STAGE_NAMESPACE)),
        sort_keys=False,
        indent=2,
    ).encode("utf-8")
    with pytest.raises(ValueError, match="canonical"):
        _stdin_operation(module, keys_path, noncanonical)
