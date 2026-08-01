from __future__ import annotations

import base64
import json
import os
import runpy
import stat
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
HELPER = ROOT / "deploy" / "libexec" / "rquant-runtime-credential-sealer"
GENERATION = "b" * 64


def _request(instances: tuple[str, ...], *, token: str = "secret") -> bytes:
    credential = json.dumps(
        {
            "schema_version": 1,
            "bundle_generation": GENERATION,
            "capabilities": {"TUSHARE_TOKEN_MAIN": token},
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return json.dumps(
        {
            "schema_version": 1,
            "credentials": {
                instance: base64.b64encode(credential).decode() for instance in instances
            },
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def test_root_helper_seals_idempotent_generation_and_switches_scoped_pointers(
    tmp_path: Path,
) -> None:
    module = runpy.run_path(str(HELPER))
    seal_request = module["seal_request"]
    instances = ("svc-" + "a" * 64, "svc-" + "c" * 64)
    encrypted: list[bytes] = []

    def encrypt(payload: bytes) -> bytes:
        encrypted.append(payload)
        return b"encrypted:" + payload

    def decrypt(payload: bytes) -> bytes:
        return payload.removeprefix(b"encrypted:")

    assert (
        seal_request(
            _request(instances),
            store_root=tmp_path / "credstore",
            owner_uid=os.getuid(),
            encrypt=encrypt,
            decrypt=decrypt,
        )
        == instances
    )
    assert len(encrypted) == 2

    for instance in instances:
        instance_root = tmp_path / "credstore" / "instances" / instance
        credential = instance_root / "generations" / f"{GENERATION}.cred"
        pointer = instance_root / "current.cred"
        assert stat.S_IMODE(credential.stat().st_mode) == 0o600
        assert pointer.is_symlink()
        assert os.readlink(pointer) == f"generations/{GENERATION}.cred"

    seal_request(
        _request(instances),
        store_root=tmp_path / "credstore",
        owner_uid=os.getuid(),
        encrypt=encrypt,
        decrypt=decrypt,
    )
    assert len(encrypted) == 2


def test_root_helper_rejects_mixed_or_tampered_generation(tmp_path: Path) -> None:
    module = runpy.run_path(str(HELPER))
    seal_request = module["seal_request"]
    instance = "svc-" + "a" * 64
    root = tmp_path / "credstore"
    seal_request(
        _request((instance,)),
        store_root=root,
        owner_uid=os.getuid(),
        encrypt=lambda payload: b"encrypted:" + payload,
        decrypt=lambda payload: payload.removeprefix(b"encrypted:"),
    )
    target = root / "instances" / instance / "generations" / f"{GENERATION}.cred"
    target.write_bytes(b"tampered")
    target.chmod(0o600)

    with pytest.raises(ValueError, match="unsafe"):
        seal_request(
            _request((instance,)),
            store_root=root,
            owner_uid=os.getuid(),
            encrypt=lambda payload: b"encrypted:" + payload,
            decrypt=lambda payload: payload.removeprefix(b"encrypted:"),
        )


def test_root_helper_rejects_all_command_line_arguments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = runpy.run_path(str(HELPER))
    monkeypatch.setattr(sys, "argv", [str(HELPER), "--unexpected"])

    with pytest.raises(ValueError, match="does not accept arguments"):
        module["main"]()
