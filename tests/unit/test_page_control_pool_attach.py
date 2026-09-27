"""The canvas attach command reads the current signed canvas and exact pool file."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from rquant import page_control
from rquant.llm.schemas import RuleCall
from rquant.page_control import (
    PageControlConsumer,
    PageControlOutbox,
    PageControlService,
    PageControlStatus,
    SaveCanvas,
    SaveUserPoolV2,
    parse_page_control_command,
)
from rquant.runtime_contracts import canonical_sha256
from tests.canvas_ed25519_support import create_canvas_ed25519_test_authority

NOW = datetime(2026, 9, 27, 5, 0, tzinfo=UTC)


def _service(root: Path) -> PageControlService:
    authority = create_canvas_ed25519_test_authority(root / "keys")
    outbox = PageControlOutbox(root / "control.sqlite3")
    return PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=root / "data",
            log_dir=root / "logs",
            clock=lambda: NOW,
            canvas_publication_signer=authority.signer,
            canvas_publication_keyring=authority.keyring,
        ),
    )


def _pool(command_id: str, name: str, *, expected_version: str | None = None) -> SaveUserPoolV2:
    return SaveUserPoolV2(
        command_id=command_id,
        requested_at=NOW,
        base_name=name,
        display_name=name,
        rule_calls=(RuleCall(name="not_st", args={}),),
        expected_version=expected_version,
    )


def _attach(command_id: str, name: str, version: str) -> page_control.AddPoolToCanvas:
    return page_control.AddPoolToCanvas(
        command_id=command_id,
        requested_at=NOW,
        canvas_name="观察",
        pool_name=f"user/{name}",
        expected_pool_version=version,
    )


def _canvas(root: Path) -> dict[str, object]:
    return json.loads((root / "data" / "canvases" / "观察.json").read_text(encoding="utf-8"))


def test_attach_parses_reuses_original_body_and_preserves_v2_hash(tmp_path: Path) -> None:
    service = _service(tmp_path)
    original = _pool("save-original", "first")
    saved = service.submit(original)
    assert saved.status is PageControlStatus.SUCCEEDED
    assert isinstance(saved.result, dict)
    version = saved.result["version"]
    raw = json.loads((tmp_path / "data" / "user_presets" / "first.json").read_text())
    assert raw["command_hash"] == canonical_sha256(original.model_dump(mode="json"))
    assert (
        service.submit(SaveCanvas(command_id="canvas", requested_at=NOW, name="观察")).status
        is PageControlStatus.SUCCEEDED
    )

    command = _attach("attach-first", "first", version)
    assert isinstance(
        parse_page_control_command(command.model_dump(mode="json")), page_control.AddPoolToCanvas
    )
    first = service.submit(command)
    assert first.status is PageControlStatus.SUCCEEDED
    assert isinstance(first.result, dict)
    assert first.result["canvas_name"] == "观察"
    assert first.result["pool_name"] == "user/first"
    assert first.result["pool_version"] == version
    assert _canvas(tmp_path)["pool_refs"] == ["user/first"]
    bytes_after = (tmp_path / "data" / "canvases" / "观察.json").read_bytes()
    repeated = service.submit(parse_page_control_command(command.model_dump(mode="json")))
    assert repeated == first
    assert (tmp_path / "data" / "canvases" / "观察.json").read_bytes() == bytes_after


def test_adjacent_canvas_appends_read_the_latest_authoritative_refs(tmp_path: Path) -> None:
    service = _service(tmp_path)
    assert (
        service.submit(
            SaveCanvas(
                command_id="canvas", requested_at=NOW, name="观察", pool_refs=("n-shape-pool1",)
            )
        ).status
        is PageControlStatus.SUCCEEDED
    )
    first = service.submit(_pool("save-first", "first"))
    second = service.submit(_pool("save-second", "second"))
    assert isinstance(first.result, dict) and isinstance(second.result, dict)
    left = _attach("attach-first", "first", first.result["version"])
    right = _attach("attach-second", "second", second.result["version"])
    service.outbox.enqueue(left)
    service.outbox.enqueue(right)
    drained = service.consumer.drain(limit=2)
    assert [receipt.status for receipt in drained] == [PageControlStatus.SUCCEEDED] * 2
    assert _canvas(tmp_path)["pool_refs"] == ["n-shape-pool1", "user/first", "user/second"]


def test_attach_rejects_changed_pool_and_failed_id_never_reexecutes(tmp_path: Path) -> None:
    service = _service(tmp_path)
    assert (
        service.submit(SaveCanvas(command_id="canvas", requested_at=NOW, name="观察")).status
        is PageControlStatus.SUCCEEDED
    )
    saved = service.submit(_pool("save-first", "first"))
    assert isinstance(saved.result, dict)
    updated = service.submit(
        _pool("update-first", "first", expected_version=saved.result["version"])
    )
    assert isinstance(updated.result, dict)
    stale = _attach("attach-stale", "first", saved.result["version"])
    failed = service.submit(stale)
    assert failed.status is PageControlStatus.FAILED
    assert _canvas(tmp_path)["pool_refs"] == []
    assert service.submit(stale) == failed
    retried = service.submit(_attach("attach-new", "first", updated.result["version"]))
    assert retried.status is PageControlStatus.SUCCEEDED
    assert _canvas(tmp_path)["pool_refs"] == ["user/first"]


def test_attach_fails_closed_for_missing_or_invalid_target(tmp_path: Path) -> None:
    service = _service(tmp_path)
    assert (
        service.submit(SaveCanvas(command_id="canvas", requested_at=NOW, name="观察")).status
        is PageControlStatus.SUCCEEDED
    )
    missing = service.submit(_attach("attach-missing", "missing", "a" * 64))
    assert missing.status is PageControlStatus.FAILED
    assert _canvas(tmp_path)["pool_refs"] == []
    with pytest.raises(ValueError):
        _attach("bad-name", "../other", "a" * 64)


def test_attach_recovers_after_canvas_write_without_appending_twice(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = _service(tmp_path)
    saved = service.submit(_pool("save-first", "first"))
    assert isinstance(saved.result, dict)
    assert (
        service.submit(SaveCanvas(command_id="canvas", requested_at=NOW, name="观察")).status
        is PageControlStatus.SUCCEEDED
    )
    original_write = PageControlConsumer._atomic_json
    writes: list[Path] = []

    def crash_after_write(path: Path, payload: object, *, command_id: str) -> None:
        writes.append(path)
        original_write(path, payload, command_id=command_id)
        raise KeyboardInterrupt("synthetic crash after canvas write")

    monkeypatch.setattr(PageControlConsumer, "_atomic_json", staticmethod(crash_after_write))
    command = _attach("attach-first", "first", saved.result["version"])
    with pytest.raises(KeyboardInterrupt):
        service.submit(command)

    outbox = PageControlOutbox(tmp_path / "control.sqlite3")
    resumed = PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=tmp_path / "data",
            log_dir=tmp_path / "logs",
            clock=lambda: NOW + timedelta(seconds=31),
            canvas_publication_signer=service.consumer.canvas_publication_signer,
            canvas_publication_keyring=service.consumer.canvas_publication_keyring,
        ),
    ).submit(command)
    assert resumed.status is PageControlStatus.SUCCEEDED
    assert writes == [tmp_path / "data" / "canvases" / "观察.json"]
    assert _canvas(tmp_path)["pool_refs"] == ["user/first"]
