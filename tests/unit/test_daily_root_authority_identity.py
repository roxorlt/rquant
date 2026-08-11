from __future__ import annotations

import hashlib
import importlib.util
import sys
import zipfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "deploy/root-runtime/daily_receipt_authority.py"


def _load_root_authority():
    spec = importlib.util.spec_from_file_location("rquant_daily_root_authority", SOURCE)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_loaded_source_identity_hashes_the_unique_zipapp_main(tmp_path: Path) -> None:
    module = _load_root_authority()
    source = SOURCE.read_bytes()
    archive = tmp_path / "authority.pyz"
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_STORED) as bundle:
        bundle.writestr("__main__.py", source)

    module.__file__ = f"{archive}/__main__.py"
    assert module._loaded_source_sha256() == hashlib.sha256(source).hexdigest()


def test_loaded_source_identity_rejects_non_single_file_zipapp(tmp_path: Path) -> None:
    module = _load_root_authority()
    archive = tmp_path / "authority.pyz"
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_STORED) as bundle:
        bundle.writestr("__main__.py", b"print('authority')\n")
        bundle.writestr("unexpected.txt", b"tampered\n")

    module.__file__ = f"{archive}/__main__.py"
    with pytest.raises(module.AuthorityError, match="contents"):
        module._loaded_source_sha256()


def test_loaded_source_identity_rejects_duplicate_main_entry(tmp_path: Path) -> None:
    module = _load_root_authority()
    archive = tmp_path / "authority.pyz"
    with pytest.warns(UserWarning, match="Duplicate name"), zipfile.ZipFile(
        archive, "w", compression=zipfile.ZIP_STORED
    ) as bundle:
        bundle.writestr("__main__.py", b"first\n")
        bundle.writestr("__main__.py", b"second\n")

    module.__file__ = f"{archive}/__main__.py"
    with pytest.raises(module.AuthorityError, match="contents"):
        module._loaded_source_sha256()


def test_identity_envelope_binds_protocol_nonce_source_and_key() -> None:
    module = _load_root_authority()
    envelope = module._identity_envelope(
        nonce="a" * 64,
        source_sha256="b" * 64,
        key_id="daily-v1",
    )
    assert envelope == {
        "version": 1,
        "operation": "identity",
        "protocol": "rquant-daily-receipt-authority.identity",
        "nonce": "a" * 64,
        "source_sha256": "b" * 64,
        "key_id": "daily-v1",
    }
