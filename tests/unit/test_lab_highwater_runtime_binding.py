from __future__ import annotations

import base64
import hashlib
import json
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from rquant.cli import _resolve_lab_highwater_runtime_binding
from rquant.config import settings
from rquant.lab_daemon import LabDaemonConfigurationError
from rquant.lab_highwater_authority import PRODUCTION_LAB_HIGHWATER_COMMAND
from rquant.strict_json import canonical_json_bytes
from tests.highwater_ed25519_support import resolve_openssl


def _trusted_keyring(path: Path) -> Path:
    private_key = path.with_suffix(".private.pem")
    public_key = path.with_suffix(".public.pem")
    subprocess.run(
        [resolve_openssl(), "genpkey", "-algorithm", "ED25519", "-out", str(private_key)],
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
    body = {
        "schema_version": 3,
        "generation": 1,
        "previous_manifest_hash": "0" * 64,
        "active_key_id": "hw-v1",
        "active_public_key": public_key.read_text(encoding="utf-8"),
        "previous_public_keys": {},
    }
    manifest_hash = hashlib.sha256(canonical_json_bytes(body)).hexdigest()
    message = path.with_suffix(".message")
    message.write_text(manifest_hash, encoding="ascii")
    os.chmod(message, 0o600)
    signed = subprocess.run(
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
    path.write_bytes(
        canonical_json_bytes(
            {
                **body,
                "manifest_hash": manifest_hash,
                "signature": base64.b64encode(signed.stdout).decode("ascii"),
            }
        )
    )
    os.chmod(path, 0o600)
    return path


def _configure_production_authority(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    runtime_root = tmp_path / "lab-runtime"
    runtime_root.mkdir(mode=0o700)
    monkeypatch.setattr(settings, "app_env", "prod")
    monkeypatch.setattr(settings, "lab_runtime_dir", runtime_root)
    monkeypatch.setattr(
        settings,
        "lab_highwater_authority_command_json",
        json.dumps(list(PRODUCTION_LAB_HIGHWATER_COMMAND)),
    )
    monkeypatch.setattr(settings, "lab_highwater_stable_identity", "lab-production-v1")
    monkeypatch.setattr(
        settings,
        "lab_highwater_trusted_keyring_path",
        _trusted_keyring(tmp_path / "lab-highwater-keys.json"),
    )
    monkeypatch.setattr(settings, "lab_highwater_timeout_seconds", 3.0)
    monkeypatch.setattr(settings, "lab_highwater_allow_identity_rotation", False)


def _profile_from_settings() -> SimpleNamespace:
    command = json.loads(settings.lab_highwater_authority_command_json)
    return SimpleNamespace(
        authority_command=tuple(command),
        stable_identity=settings.lab_highwater_stable_identity,
        trusted_keyring_path=settings.lab_highwater_trusted_keyring_path,
        timeout_seconds=settings.lab_highwater_timeout_seconds,
        allow_identity_rotation=settings.lab_highwater_allow_identity_rotation,
        production_mode=True,
    )


def test_production_binding_requires_fixed_authority_and_sealed_credential(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _configure_production_authority(monkeypatch, tmp_path)
    profile = _profile_from_settings()

    binding = _resolve_lab_highwater_runtime_binding(
        settings=settings,
        code_sha="1" * 40,
        profile_identity="2" * 64,
        highwater_profile=profile,
        require_profile=True,
    )

    assert binding.observer is not None
    assert binding.audit_command is not None
    assert "--machine-receipt" in binding.audit_command
    assert binding.state_path == (
        settings.lab_finalizer_state_dir_resolved / "full-integrity-audit.json"
    )


def test_production_binding_rejects_lab_controlled_authority_storage(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _configure_production_authority(monkeypatch, tmp_path)
    profile = SimpleNamespace(
        authority_command=(
            "/usr/bin/sudo",
            "-n",
            "/usr/local/libexec/rquant-lab-highwater-authority",
            "--state-root",
            str(settings.lab_runtime_dir_resolved / "highwater"),
        ),
        stable_identity=settings.lab_highwater_stable_identity,
        trusted_keyring_path=settings.lab_highwater_trusted_keyring_path,
        timeout_seconds=settings.lab_highwater_timeout_seconds,
        allow_identity_rotation=False,
        production_mode=True,
    )

    with pytest.raises(LabDaemonConfigurationError, match="fixed sudo helper"):
        _resolve_lab_highwater_runtime_binding(
            settings=settings,
            code_sha="1" * 40,
            profile_identity="2" * 64,
            highwater_profile=profile,
            require_profile=True,
        )


def test_development_binding_has_no_implicit_local_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "app_env", "dev")
    monkeypatch.setattr(settings, "lab_highwater_authority_command_json", "")
    monkeypatch.setattr(settings, "lab_highwater_stable_identity", "")
    monkeypatch.setattr(settings, "lab_highwater_trusted_keyring_path", None)

    binding = _resolve_lab_highwater_runtime_binding(
        settings=settings,
        code_sha="1" * 40,
        profile_identity="2" * 64,
    )

    assert binding.observer is None
    assert binding.audit_command is None


def test_profile_owned_production_binding_ignores_runtime_environment_downgrade(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _configure_production_authority(monkeypatch, tmp_path)
    trusted_keyring = settings.lab_highwater_trusted_keyring_path
    assert trusted_keyring is not None
    monkeypatch.setattr(settings, "app_env", "dev")
    monkeypatch.setattr(
        settings,
        "lab_highwater_authority_command_json",
        json.dumps([str(tmp_path / "attacker-helper")]),
    )
    monkeypatch.setattr(settings, "lab_highwater_stable_identity", "attacker-chain")
    monkeypatch.setattr(
        settings,
        "lab_highwater_trusted_keyring_path",
        tmp_path / "attacker-keys.json",
    )
    profile = SimpleNamespace(
        authority_command=PRODUCTION_LAB_HIGHWATER_COMMAND,
        stable_identity="lab-production-v1",
        trusted_keyring_path=trusted_keyring,
        timeout_seconds=3.0,
        allow_identity_rotation=False,
        production_mode=True,
    )

    binding = _resolve_lab_highwater_runtime_binding(
        settings=settings,
        code_sha="1" * 40,
        profile_identity="2" * 64,
        highwater_profile=profile,
    )

    assert binding.observer is not None
    config = binding.observer.config
    assert config.command == PRODUCTION_LAB_HIGHWATER_COMMAND
    assert config.stable_identity == "lab-production-v1"
    assert "attacker-helper" not in " ".join(binding.audit_command or ())
