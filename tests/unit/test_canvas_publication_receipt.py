from __future__ import annotations

import base64
import os
from datetime import UTC, datetime
from pathlib import Path

import pytest

import rquant.canvas_publication_receipt as receipt_module
from rquant.canvas_publication_receipt import (
    CanvasPublicationCommand,
    CanvasPublicationReceipt,
    CanvasPublicationReceiptStore,
    Ed25519CanvasPublicationSigner,
    build_canvas_publication_claims,
)

NOW = datetime(2026, 8, 4, 1, 30, tzinfo=UTC)


class _DeterministicSigningClient:
    def sign(self, *, namespace: str, payload: bytes) -> str:
        assert namespace == receipt_module.CANVAS_PUBLICATION_NAMESPACE
        assert payload
        return base64.b64encode(b"\x00" * 64).decode("ascii")


def _publication(
    *,
    pool_refs: tuple[str, ...] = ("n-shape-pool1",),
) -> CanvasPublicationReceipt:
    command = CanvasPublicationCommand(
        command_id="canvas-publication-receipt-test",
        requested_at=NOW,
        name="breakout",
        description="receipt store adversarial test",
        pool_refs=pool_refs,
        source="page_control",
    )
    claims = build_canvas_publication_claims(
        command=command,
        catalog_created_at=NOW,
        catalog_updated_at=NOW,
        consumer_service_id="page-control-test",
        consumer_instance_id="page-control-instance-1",
    )
    return Ed25519CanvasPublicationSigner(
        key_id="canvas-test-v1",
        client=_DeterministicSigningClient(),
    ).issue_publication(claims)


def _replace_entry_after_first_read(
    monkeypatch: pytest.MonkeyPatch,
    *,
    target: Path,
    replacement: Path,
) -> list[bool]:
    original_read = receipt_module.os.read
    replaced = [False]

    def replace_after_read(descriptor: int, count: int) -> bytes:
        chunk = original_read(descriptor, count)
        if chunk and not replaced[0]:
            replaced[0] = True
            os.replace(replacement, target)
        return chunk

    monkeypatch.setattr(receipt_module.os, "read", replace_after_read)
    return replaced


def _rotate_directory_after_first_read(
    monkeypatch: pytest.MonkeyPatch,
    *,
    root: Path,
    replacement_root: Path,
    hidden_root: Path,
) -> list[bool]:
    original_read = receipt_module.os.read
    rotated = [False]

    def rotate_after_read(descriptor: int, count: int) -> bytes:
        chunk = original_read(descriptor, count)
        if chunk and not rotated[0]:
            rotated[0] = True
            root.rename(hidden_root)
            replacement_root.rename(root)
        return chunk

    monkeypatch.setattr(receipt_module.os, "read", rotate_after_read)
    return rotated


def test_read_rejects_named_entry_replacement_after_opened_inode_is_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publication = _publication()
    store = CanvasPublicationReceiptStore(tmp_path / "receipts")
    receipt_path = store.write_immutable(publication)
    replacement = tmp_path / "replacement.json"
    replacement.write_bytes(receipt_path.read_bytes())
    replacement.chmod(0o600)
    replaced = _replace_entry_after_first_read(
        monkeypatch,
        target=receipt_path,
        replacement=replacement,
    )

    with pytest.raises(ValueError, match="receipt.*changed|receipt.*rotated"):
        store.read(publication.receipt_id)

    assert replaced == [True]


def test_read_rejects_parent_directory_rotation_after_opened_inode_is_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publication = _publication()
    root = tmp_path / "receipts"
    store = CanvasPublicationReceiptStore(root)
    receipt_path = store.write_immutable(publication)
    replacement_root = tmp_path / "replacement-receipts"
    replacement_root.mkdir(mode=0o700)
    (replacement_root / receipt_path.name).write_bytes(receipt_path.read_bytes())
    (replacement_root / receipt_path.name).chmod(0o600)
    rotated = _rotate_directory_after_first_read(
        monkeypatch,
        root=root,
        replacement_root=replacement_root,
        hidden_root=tmp_path / "hidden-receipts",
    )

    with pytest.raises(ValueError, match="receipt directory changed"):
        store.read(publication.receipt_id)

    assert rotated == [True]


def test_identical_write_rejects_named_entry_replacement_after_existing_inode_is_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publication = _publication()
    store = CanvasPublicationReceiptStore(tmp_path / "receipts")
    receipt_path = store.write_immutable(publication)
    replacement = tmp_path / "replacement.json"
    replacement.write_bytes(receipt_path.read_bytes())
    replacement.chmod(0o600)
    replaced = _replace_entry_after_first_read(
        monkeypatch,
        target=receipt_path,
        replacement=replacement,
    )

    with pytest.raises(ValueError, match="receipt.*changed|receipt.*rotated"):
        store.write_immutable(publication)

    assert replaced == [True]


def test_identical_write_rejects_parent_directory_rotation_after_existing_inode_is_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publication = _publication()
    root = tmp_path / "receipts"
    store = CanvasPublicationReceiptStore(root)
    receipt_path = store.write_immutable(publication)
    replacement_root = tmp_path / "replacement-receipts"
    replacement_root.mkdir(mode=0o700)
    (replacement_root / receipt_path.name).write_bytes(receipt_path.read_bytes())
    (replacement_root / receipt_path.name).chmod(0o600)
    rotated = _rotate_directory_after_first_read(
        monkeypatch,
        root=root,
        replacement_root=replacement_root,
        hidden_root=tmp_path / "hidden-receipts",
    )

    with pytest.raises(ValueError, match="receipt directory changed"):
        store.write_immutable(publication)

    assert rotated == [True]


@pytest.mark.parametrize("pool_ref", ["", "x" * 129])
def test_publication_models_bound_each_pool_reference(pool_ref: str) -> None:
    with pytest.raises(ValueError, match="pool_refs"):
        _publication(pool_refs=(pool_ref,))


def test_publication_rejects_catalog_record_that_serving_would_reject_for_size() -> None:
    oversized_catalog_refs = tuple("量" * 128 for _ in range(256))

    with pytest.raises(ValueError, match="catalog.*size"):
        _publication(pool_refs=oversized_catalog_refs)


def test_write_rejects_oversized_receipt_before_creating_publication_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publication = _publication()
    payload_size = len(publication.canonical_json_bytes())
    monkeypatch.setattr(receipt_module, "MAX_RECEIPT_BYTES", payload_size - 1)
    root = tmp_path / "receipts"

    with pytest.raises(ValueError, match="receipt exceeds size bound"):
        CanvasPublicationReceiptStore(root).write_immutable(publication)

    assert not root.exists()
