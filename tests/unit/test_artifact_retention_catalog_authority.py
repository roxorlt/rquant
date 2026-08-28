from __future__ import annotations

from pathlib import Path

import pytest

from rquant.artifact_retention_catalog_authority import (
    RetentionCatalogAuthorityError,
    initialize_retention_catalog_authority,
    load_retention_catalog_authority,
    quarantine_legacy_catalog_reference_store,
)

COMMIT = "a" * 40


def test_clean_retention_catalog_bootstrap_initializes_store_and_publishes_immutable_receipt(
    tmp_path: Path,
) -> None:
    root = tmp_path / "runtime" / "research" / "artifact-retention" / "svc-retention"
    root.mkdir(parents=True, mode=0o700)
    root.chmod(0o700)
    references = root / "references.sqlite3"
    first = initialize_retention_catalog_authority(
        state_root=root,
        reference_store_path=references,
        producer_commit=COMMIT,
    )
    first_receipt_inode = first.current_receipt_path.stat().st_ino
    second = initialize_retention_catalog_authority(
        state_root=root,
        reference_store_path=references,
        producer_commit=COMMIT,
    )

    assert second == first
    assert second.current_receipt_path.stat().st_ino == first_receipt_inode
    loaded = load_retention_catalog_authority(
        root / "catalog-authority",
        expected_producer_commit=COMMIT,
        expected_reference_store_path=references,
    )
    assert loaded.receipt.generation_id == first.receipt.generation_id
    assert loaded.current_receipt_path.is_file()
    assert loaded.current_receipt_path.stat().st_mode & 0o777 == 0o600
    assert references.is_file()
    assert references.stat().st_mode & 0o777 == 0o600


def test_clean_retention_catalog_initialization_recovers_after_receipt_publish_crash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "runtime" / "research" / "artifact-retention" / "svc-retention"
    root.mkdir(parents=True, mode=0o700)
    root.chmod(0o700)
    references = root / "references.sqlite3"
    original = __import__(
        "rquant.artifact_retention_catalog_authority",
        fromlist=["_write_current"],
    )._write_current
    calls = 0

    def crash_once(path: Path, payload: bytes) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("simulated power loss before current receipt")
        original(path, payload)

    monkeypatch.setattr(
        "rquant.artifact_retention_catalog_authority._write_current",
        crash_once,
    )
    with pytest.raises(RuntimeError, match="power loss"):
        initialize_retention_catalog_authority(
            state_root=root,
            reference_store_path=references,
            producer_commit=COMMIT,
        )

    recovered = initialize_retention_catalog_authority(
        state_root=root,
        reference_store_path=references,
        producer_commit=COMMIT,
    )
    assert recovered.current_receipt_path.is_file()
    assert len(tuple((root / "catalog-authority" / "snapshots").glob("*.json"))) == 1


def test_legacy_catalog_references_is_quarantined_without_becoming_authority(
    tmp_path: Path,
) -> None:
    catalog_root = tmp_path / "runtime" / "research" / "artifact-catalogs" / "svc-legacy"
    catalog_root.mkdir(parents=True, mode=0o700)
    catalog_root.chmod(0o700)
    legacy = catalog_root / "references.sqlite3"
    legacy.write_bytes(b"legacy metadata")
    legacy.chmod(0o600)
    retention_root = tmp_path / "runtime" / "research" / "artifact-retention" / "svc-retention"
    retention_root.mkdir(parents=True, mode=0o700)
    retention_root.chmod(0o700)
    references = retention_root / "references.sqlite3"
    references.touch(mode=0o600)

    receipt = quarantine_legacy_catalog_reference_store(
        legacy_path=legacy,
        retention_state_root=retention_root,
        reason="job-center-authority-v4",
    )

    assert receipt.legacy_path == legacy
    assert receipt.quarantine_path.is_file()
    assert legacy.exists()
    with pytest.raises(RetentionCatalogAuthorityError, match="legacy catalog reference store"):
        load_retention_catalog_authority(
            catalog_root,
            expected_producer_commit=COMMIT,
            expected_reference_store_path=references,
        )
