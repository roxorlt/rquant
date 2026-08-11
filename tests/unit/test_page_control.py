from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest

from rquant import page_control
from rquant.lab_job_protocol import PauseJobCommand
from rquant.llm.schemas import RuleCall
from rquant.page_control import (
    AppendNlQueryLog,
    DeleteCanvas,
    DeleteUserPool,
    DiscardLabArtifactZip,
    ExportLabArtifactZip,
    ForkBuiltinPool,
    InitializeLabExports,
    PageControlClient,
    PageControlConsumer,
    PageControlOutbox,
    PageControlReceipt,
    PageControlService,
    PageControlStatus,
    PageControlUnavailableError,
    SaveCanvas,
    SaveNlPreset,
    SaveUserPool,
    SetCanvasPoolRefs,
    SubmitLabCommand,
)
from rquant.runtime_contracts import canonical_sha256
from tests.canvas_ed25519_support import (
    create_canvas_ed25519_test_authority,
    create_rotating_canvas_ed25519_test_authority,
)

NOW = datetime(2026, 8, 3, 1, 30, tzinfo=UTC)
_CANVAS_AUTHORITIES: dict[Path, object] = {}


class _LabControlBackendSpy:
    def __init__(self, export_path: Path) -> None:
        self.export_path = export_path
        self.calls: list[tuple[str, object]] = []

    def submit_command(self, command: object, *, interaction_key: str | None):
        self.calls.append(("submit", (command, interaction_key)))
        return {"result": "accepted"}

    def export_zip(self, job_id: UUID):
        self.calls.append(("export", job_id))
        self.export_path.write_bytes(b"zip")
        return {
            "request_id": str(UUID(int=2)),
            "job_id": str(job_id),
            "path": str(self.export_path),
            "byte_size": 3,
            "sha256": "a" * 64,
        }

    def discard_zip(self, command: DiscardLabArtifactZip):
        self.calls.append(("discard", command))
        if self.export_path.exists():
            self.export_path.unlink()
        return {"discarded": True}


class _CrashAfterLabEffectBackend:
    def __init__(self, export_path: Path) -> None:
        self.export_path = export_path
        self.calls: list[str] = []
        self.export_sequence = 0

    def submit_command(self, command: object, *, interaction_key: str | None):
        self.calls.append("submit")
        if self.calls.count("submit") == 1:
            raise KeyboardInterrupt("crash after submit side effect")
        return {"result": "accepted-after-replay"}

    def export_zip(self, job_id: UUID):
        self.calls.append("export")
        self.export_sequence += 1
        self.export_path.write_bytes(f"zip-{self.export_sequence}".encode())
        if self.calls.count("export") == 1:
            raise KeyboardInterrupt("crash after export side effect")
        return {
            "request_id": str(UUID(int=100 + self.export_sequence)),
            "job_id": str(job_id),
            "path": str(self.export_path),
            "byte_size": self.export_path.stat().st_size,
            "sha256": f"{self.export_sequence:064x}",
        }

    def discard_zip(self, command: DiscardLabArtifactZip):
        self.calls.append("discard")
        if self.export_path.exists():
            self.export_path.unlink()
        if self.calls.count("discard") == 1:
            raise KeyboardInterrupt("crash after discard side effect")
        return {"discarded": True, "replayed": True}


def _service_for(
    *,
    outbox: PageControlOutbox,
    tmp_path: Path,
    data_dir: Path | None = None,
    log_dir: Path | None = None,
    allowed_lab_export_roots: tuple[Path, ...] = (),
    lab_backend: object | None = None,
    now: datetime = NOW,
    with_canvas_authority: bool = True,
) -> PageControlService:
    authority = _canvas_authority(tmp_path) if with_canvas_authority else None
    return PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=tmp_path / "data" if data_dir is None else data_dir,
            log_dir=tmp_path / "logs" if log_dir is None else log_dir,
            allowed_lab_export_roots=allowed_lab_export_roots,
            lab_backend=lab_backend,
            clock=lambda: now,
            lease_seconds=1,
            canvas_publication_signer=(
                None if authority is None else authority.signer
            ),
            canvas_publication_keyring=(
                None if authority is None else authority.keyring
            ),
        ),
    )


def _canvas_authority(tmp_path: Path) -> object:
    root = (tmp_path / "canvas-keys").resolve(strict=False)
    authority = _CANVAS_AUTHORITIES.get(root)
    if authority is None:
        authority = create_canvas_ed25519_test_authority(root)
        _CANVAS_AUTHORITIES[root] = authority
    return authority


def _claim_as_crashed(outbox: PageControlOutbox, command: object) -> None:
    outbox.enqueue(command)
    claimed = outbox.claim(
        limit=1,
        owner_id="crashed-worker",
        lease_seconds=1,
        now=NOW,
    )
    assert tuple(item.command_id for item in claimed) == (command.command_id,)


def _crash_complete_once(monkeypatch: pytest.MonkeyPatch, outbox: PageControlOutbox) -> None:
    original_complete = outbox.complete
    crashed = False

    def crash_once(command_id: str, **kwargs: object):
        nonlocal crashed
        if not crashed:
            crashed = True
            raise KeyboardInterrupt(f"crash before final receipt for {command_id}")
        return original_complete(command_id, **kwargs)

    monkeypatch.setattr(outbox, "complete", crash_once)


def _assert_ambiguous_at_most_once(receipt: PageControlReceipt) -> None:
    assert receipt.status is PageControlStatus.AMBIGUOUS
    result = receipt.result
    assert isinstance(result, dict)
    assert result["outcome"] == "ambiguous_completed_at_most_once"


def _create_partial_migrated_page_control_db(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.execute(
        """
        CREATE TABLE page_control_command (
            command_id TEXT PRIMARY KEY,
            command_kind TEXT NOT NULL,
            command_hash TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            status TEXT NOT NULL,
            enqueued_at TEXT NOT NULL,
            completed_at TEXT,
            result_json TEXT,
            error TEXT,
            processing_owner TEXT,
            lease_expires_at TEXT,
            attempt_count INTEGER NOT NULL DEFAULT 0,
            claim_token TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE page_control_effect (
            command_id TEXT PRIMARY KEY,
            command_hash TEXT NOT NULL,
            effect_kind TEXT NOT NULL,
            status TEXT NOT NULL,
            owner_id TEXT NOT NULL,
            claim_token TEXT NOT NULL,
            started_at TEXT NOT NULL,
            completed_at TEXT,
            result_json TEXT,
            error TEXT
        )
        """
    )
    return connection


def _insert_processing_command(
    connection: sqlite3.Connection,
    command: object,
    *,
    lease_expires_at: datetime,
) -> None:
    connection.execute(
        """
        INSERT INTO page_control_command(
            command_id, command_kind, command_hash, payload_json, status,
            enqueued_at, processing_owner, lease_expires_at, attempt_count,
            claim_token
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            command.command_id,
            command.kind,
            canonical_sha256(command.model_dump(mode="json")),
            command.model_dump_json(),
            "processing",
            command.requested_at.isoformat(timespec="microseconds"),
            "pre-marker-worker",
            lease_expires_at.isoformat(timespec="microseconds"),
            1,
            f"pre-marker-{command.command_id}",
        ),
    )


def test_page_control_outbox_is_idempotent_and_rejects_command_id_reuse(
    tmp_path: Path,
) -> None:
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")
    command = SaveCanvas(
        command_id="command-1",
        requested_at=NOW,
        name="alpha",
        description="first",
        pool_refs=("pool1",),
    )

    first = outbox.enqueue(command)
    duplicate = outbox.enqueue(command)

    assert first.command_id == duplicate.command_id == "command-1"
    assert first.status == duplicate.status == "pending"
    with pytest.raises(ValueError, match="different payload"):
        outbox.enqueue(command.model_copy(update={"description": "changed"}))


def test_page_control_consumer_owns_canvas_preset_log_and_export_writes(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    log_dir = tmp_path / "logs"
    export_dir = tmp_path / "exports"
    runtime_dir = tmp_path / "runtime"
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")
    authority = _canvas_authority(tmp_path)
    consumer = PageControlConsumer(
        outbox=outbox,
        data_dir=data_dir,
        log_dir=log_dir,
        allowed_lab_export_roots=(export_dir, runtime_dir),
        canvas_publication_signer=authority.signer,
        canvas_publication_keyring=authority.keyring,
    )
    commands = (
        SaveCanvas(
            command_id="canvas",
            requested_at=NOW,
            name="alpha",
            description="canvas",
            pool_refs=("pool1", "pool2"),
        ),
        SaveUserPool(
            command_id="pool",
            requested_at=NOW,
            base_name="breakout",
            description="user pool",
            rule_calls=(RuleCall(name="price_gt", args={"value": 10}),),
            include_columns=("close",),
        ),
        SaveNlPreset(
            command_id="preset",
            requested_at=NOW,
            name="自然语言条件",
            description="query",
            rule_calls=(RuleCall(name="price_gt", args={"value": 10}),),
            include_columns=("close",),
        ),
        AppendNlQueryLog(
            command_id="log",
            requested_at=NOW,
            query="价格大于10",
            plan={"trade_date": "2026-08-03"},
            outcome="success",
        ),
        InitializeLabExports(
            command_id="exports",
            requested_at=NOW,
            export_root=export_dir,
            runtime_root=runtime_dir,
        ),
    )
    for command in commands:
        outbox.enqueue(command)

    receipts = consumer.drain(limit=10)

    assert {receipt.command_id for receipt in receipts} == {
        "canvas",
        "pool",
        "preset",
        "log",
        "exports",
    }
    assert all(receipt.status == "succeeded" for receipt in receipts)
    canvas = json.loads((data_dir / "canvases" / "alpha.json").read_text())
    assert canvas["pool_refs"] == ["pool1", "pool2"]
    pool = json.loads((data_dir / "user_presets" / "breakout.json").read_text())
    assert pool["rules"][0]["name"] == "price_gt"
    preset = json.loads((data_dir / "user_presets" / "自然语言条件.json").read_text())
    assert preset["source"] == "nl_input"
    lines = (log_dir / "nl_queries.jsonl").read_text().splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["command_id"] == "log"
    assert export_dir.is_dir()
    assert runtime_dir.is_dir()

    outbox.enqueue(commands[-2])
    assert consumer.drain(limit=10) == ()
    assert len((log_dir / "nl_queries.jsonl").read_text().splitlines()) == 1


def test_page_control_save_canvas_binds_receipt_command_and_source_identity(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")
    authority = _canvas_authority(tmp_path)
    service = PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=data_dir,
            log_dir=tmp_path / "logs",
            canvas_publication_signer=authority.signer,
            canvas_publication_keyring=authority.keyring,
        ),
    )
    command = SaveCanvas(
        command_id="canvas-provenance",
        requested_at=NOW,
        name="breakout",
        description="from command",
        pool_refs=("n-shape-pool1",),
        source="canvas_page",
    )

    receipt = service.submit(command)

    assert receipt.status == "succeeded"
    assert isinstance(receipt.result, dict)
    record_path = data_dir / "canvases" / "breakout.json"
    record = json.loads(record_path.read_text(encoding="utf-8"))
    command_hash = canonical_sha256(command.model_dump(mode="json"))
    source_identity_hash = canonical_sha256(
        {
            "schema_version": 1,
            "command_id": "canvas-provenance",
            "command_hash": command_hash,
            "source": "canvas_page",
        }
    )
    record_hash = canonical_sha256(
        {key: value for key, value in record.items() if key != "record_hash"}
    )

    assert record["schema_version"] == 1
    assert record["command_id"] == "canvas-provenance"
    assert record["command_hash"] == command_hash
    assert record["source_identity_hash"] == source_identity_hash
    assert record["record_hash"] == record_hash
    assert receipt.result["path"] == str(record_path)
    assert receipt.result["command_hash"] == command_hash
    assert receipt.result["source_identity_hash"] == source_identity_hash
    assert receipt.result["record_hash"] == record_hash


def test_page_control_save_canvas_writes_signed_immutable_publication_receipt(
    tmp_path: Path,
) -> None:
    from rquant.canvas_publication_receipt import CanvasPublicationReceipt

    data_dir = tmp_path / "data"
    authority = create_canvas_ed25519_test_authority(tmp_path / "keys")
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")
    service = PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=data_dir,
            log_dir=tmp_path / "logs",
            consumer_service_id="page-control-test",
            consumer_id="page-control-instance-1",
            canvas_publication_signer=authority.signer,
            canvas_publication_keyring=authority.keyring,
        ),
    )
    command = SaveCanvas(
        command_id="canvas-signed-publication",
        requested_at=NOW,
        name="breakout",
        description="from command",
        pool_refs=("n-shape-pool1",),
        source="canvas_page",
    )

    receipt = service.submit(command)

    assert receipt.status is PageControlStatus.SUCCEEDED
    assert isinstance(receipt.result, dict)
    record_path = data_dir / "canvases" / "breakout.json"
    catalog_record = json.loads(record_path.read_text(encoding="utf-8"))
    publication_receipt_id = receipt.result["publication_receipt_id"]
    publication_path = (
        data_dir / "canvas-publication-receipts" / f"{publication_receipt_id}.json"
    )
    assert publication_path.exists()
    publication = CanvasPublicationReceipt.model_validate_json(
        publication_path.read_text(encoding="utf-8")
    )

    assert authority.keyring.verify_publication_receipt(
        publication,
        require_active=True,
    )
    assert publication.key_id == "canvas-test-v1"
    assert publication.claims.command.model_dump(mode="json") == command.model_dump(mode="json")
    assert publication.claims.command_hash == canonical_sha256(command.model_dump(mode="json"))
    assert publication.claims.catalog_record.model_dump(mode="json") == catalog_record
    assert publication.claims.catalog_record_hash == catalog_record["record_hash"]
    assert publication.claims.source_identity_hash == catalog_record["source_identity_hash"]
    assert publication.claims.consumer_service_id == "page-control-test"
    assert publication.claims.consumer_instance_id == "page-control-instance-1"
    assert publication.claims.generation_id == catalog_record["publication_generation_id"]
    assert publication.receipt_id == catalog_record["publication_receipt_id"]
    assert publication.receipt_id == receipt.result["publication_receipt_id"]
    assert publication.receipt_hash == receipt.result["publication_receipt_hash"]
    assert receipt.result["publication_generation_id"] == catalog_record[
        "publication_generation_id"
    ]
    assert receipt.result["publication_effect_id"] == publication.claims.effect_id


def test_page_control_save_canvas_fails_closed_without_publication_signer(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")
    service = PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=data_dir,
            log_dir=tmp_path / "logs",
        ),
    )

    receipt = service.submit(
        SaveCanvas(
            command_id="canvas-no-signer",
            requested_at=NOW,
            name="breakout",
            description="must not publish unsigned",
            pool_refs=("n-shape-pool1",),
        )
    )

    assert receipt.status is PageControlStatus.FAILED
    assert receipt.error is not None
    assert "CanvasPublicationReceipt" in receipt.error
    assert not (data_dir / "canvases" / "breakout.json").exists()


@pytest.mark.parametrize("operation", ["save", "set_refs", "fork"])
def test_canvas_updates_reject_tampered_current_signed_catalog(
    tmp_path: Path,
    operation: str,
) -> None:
    data_dir = tmp_path / "data"
    setup = _service_for(
        outbox=PageControlOutbox(tmp_path / "setup.sqlite3"),
        tmp_path=tmp_path,
        data_dir=data_dir,
    )
    setup_receipt = setup.submit(
        SaveCanvas(
            command_id="signed-current",
            requested_at=NOW - timedelta(minutes=1),
            name="breakout",
            description="signed description",
            pool_refs=("n-shape-pool1",),
        )
    )
    assert setup_receipt.status is PageControlStatus.SUCCEEDED
    catalog_path = data_dir / "canvases" / "breakout.json"
    tampered = json.loads(catalog_path.read_text(encoding="utf-8"))
    tampered["description"] = "unsigned attacker description"
    tampered["record_hash"] = canonical_sha256(
        {key: value for key, value in tampered.items() if key != "record_hash"}
    )
    catalog_path.write_text(json.dumps(tampered), encoding="utf-8")
    if operation == "save":
        command = SaveCanvas(
            command_id="update-after-tamper",
            requested_at=NOW,
            name="breakout",
            description="authorized update",
            pool_refs=("n-shape-pool1", "user/strong"),
        )
    elif operation == "set_refs":
        command = SetCanvasPoolRefs(
            command_id="set-after-tamper",
            requested_at=NOW,
            name="breakout",
            pool_refs=("n-shape-pool1", "user/strong"),
        )
    else:
        command = ForkBuiltinPool(
            command_id="fork-after-tamper",
            requested_at=NOW,
            builtin_name="n-shape-pool1",
            target_base_name="forked-after-tamper",
            canvas_name="breakout",
        )

    receipt = _service_for(
        outbox=PageControlOutbox(tmp_path / f"{operation}.sqlite3"),
        tmp_path=tmp_path,
        data_dir=data_dir,
    ).submit(command)

    assert receipt.status is PageControlStatus.FAILED
    assert receipt.error is not None
    assert "catalog semantics" in receipt.error
    assert json.loads(catalog_path.read_text(encoding="utf-8")) == tampered


def test_page_control_rejects_previous_key_for_new_canvas_publication(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    authority = create_rotating_canvas_ed25519_test_authority(tmp_path / "keys")
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")
    service = PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=data_dir,
            log_dir=tmp_path / "logs",
            consumer_service_id="page-control-test",
            consumer_id="page-control-instance-1",
            canvas_publication_signer=authority.previous_signer,
            canvas_publication_keyring=authority.keyring,
        ),
    )

    receipt = service.submit(
        SaveCanvas(
            command_id="canvas-previous-key",
            requested_at=NOW,
            name="breakout",
            description="previous key must not issue",
            pool_refs=("n-shape-pool1",),
        )
    )

    assert receipt.status is PageControlStatus.FAILED
    assert receipt.error is not None
    assert "active" in receipt.error
    assert not (data_dir / "canvases" / "breakout.json").exists()


def test_page_control_restart_replays_incomplete_canvas_command_once(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    outbox_path = tmp_path / "control.sqlite3"
    command = SaveCanvas(
        command_id="restart-canvas",
        requested_at=NOW,
        name="breakout",
        description="restart safe",
        pool_refs=("n-shape-pool1",),
    )
    outbox = PageControlOutbox(outbox_path)
    _claim_as_crashed(outbox, command)

    restarted_outbox = PageControlOutbox(outbox_path)
    restarted = _service_for(
        outbox=restarted_outbox,
        tmp_path=tmp_path,
        data_dir=data_dir,
        now=NOW + timedelta(seconds=2),
    )

    first = restarted.submit(command)
    duplicate = restarted.submit(command)

    assert first.status == duplicate.status == "succeeded"
    assert first.result == duplicate.result
    assert json.loads((data_dir / "canvases" / "breakout.json").read_text())[
        "record_hash"
    ] == first.result["record_hash"]


def test_page_control_restart_after_signed_canvas_receipt_reuses_byte_identical_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = tmp_path / "data"
    outbox_path = tmp_path / "control.sqlite3"
    authority = create_canvas_ed25519_test_authority(tmp_path / "keys")
    command = SaveCanvas(
        command_id="restart-signed-canvas",
        requested_at=NOW,
        name="breakout",
        description="restart signed safe",
        pool_refs=("n-shape-pool1",),
        source="canvas_page",
    )
    outbox = PageControlOutbox(outbox_path)
    _crash_complete_once(monkeypatch, outbox)
    crashed = PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=data_dir,
            log_dir=tmp_path / "logs",
            consumer_service_id="page-control-test",
            consumer_id="page-control-instance-1",
            clock=lambda: NOW,
            lease_seconds=1,
            canvas_publication_signer=authority.signer,
            canvas_publication_keyring=authority.keyring,
        ),
    )

    with pytest.raises(KeyboardInterrupt):
        crashed.submit(command)

    receipt_files = tuple((data_dir / "canvas-publication-receipts").glob("*.json"))
    assert len(receipt_files) == 1
    first_bytes = receipt_files[0].read_bytes()
    restarted_outbox = PageControlOutbox(outbox_path)
    restarted = PageControlService(
        outbox=restarted_outbox,
        consumer=PageControlConsumer(
            outbox=restarted_outbox,
            data_dir=data_dir,
            log_dir=tmp_path / "logs",
            consumer_service_id="page-control-test",
            consumer_id="page-control-instance-1",
            clock=lambda: NOW + timedelta(seconds=2),
            lease_seconds=1,
            canvas_publication_signer=authority.signer,
            canvas_publication_keyring=authority.keyring,
        ),
    )

    first = restarted.submit(command)
    duplicate = restarted.submit(command)

    assert first.status is PageControlStatus.SUCCEEDED
    assert duplicate.result == first.result
    assert tuple((data_dir / "canvas-publication-receipts").glob("*.json")) == receipt_files
    assert receipt_files[0].read_bytes() == first_bytes
    assert first.result["publication_generation_id"] == duplicate.result[
        "publication_generation_id"
    ]
    assert first.result["publication_receipt_hash"] == duplicate.result[
        "publication_receipt_hash"
    ]


def test_page_control_default_consumer_identity_reuses_receipt_after_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.canvas_publication_receipt import CanvasPublicationReceipt

    data_dir = tmp_path / "data"
    outbox_path = tmp_path / "control.sqlite3"
    authority = create_canvas_ed25519_test_authority(tmp_path / "keys")
    command = SaveCanvas(
        command_id="restart-default-identity-canvas",
        requested_at=NOW,
        name="breakout",
        description="restart default identity safe",
        pool_refs=("n-shape-pool1",),
        source="canvas_page",
    )
    original_atomic_json = page_control.PageControlConsumer._atomic_json
    crashed = False

    def crash_before_catalog_publish(path: Path, payload: object, *, command_id: str) -> None:
        nonlocal crashed
        if not crashed and path == data_dir / "canvases" / "breakout.json":
            crashed = True
            raise KeyboardInterrupt("crash after receipt before catalog publish")
        original_atomic_json(path, payload, command_id=command_id)

    monkeypatch.setattr(
        page_control.PageControlConsumer,
        "_atomic_json",
        staticmethod(crash_before_catalog_publish),
    )
    outbox = PageControlOutbox(outbox_path)
    crashed_service = PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=data_dir,
            log_dir=tmp_path / "logs",
            clock=lambda: NOW,
            lease_seconds=1,
            canvas_publication_signer=authority.signer,
            canvas_publication_keyring=authority.keyring,
        ),
    )

    with pytest.raises(KeyboardInterrupt):
        crashed_service.submit(command)

    receipt_files = tuple((data_dir / "canvas-publication-receipts").glob("*.json"))
    assert len(receipt_files) == 1
    first_receipt_path = receipt_files[0]
    first_bytes = first_receipt_path.read_bytes()
    restarted_outbox = PageControlOutbox(outbox_path)
    restarted_service = PageControlService(
        outbox=restarted_outbox,
        consumer=PageControlConsumer(
            outbox=restarted_outbox,
            data_dir=data_dir,
            log_dir=tmp_path / "logs",
            clock=lambda: NOW + timedelta(seconds=2),
            lease_seconds=1,
            canvas_publication_signer=authority.signer,
            canvas_publication_keyring=authority.keyring,
        ),
    )

    first = restarted_service.submit(command)
    duplicate = restarted_service.submit(command)

    assert first.status is PageControlStatus.SUCCEEDED
    assert duplicate.result == first.result
    assert tuple((data_dir / "canvas-publication-receipts").glob("*.json")) == (
        first_receipt_path,
    )
    assert first_receipt_path.read_bytes() == first_bytes
    publication = CanvasPublicationReceipt.model_validate_json(
        first_receipt_path.read_text(encoding="utf-8")
    )
    assert first.result["publication_generation_id"] == publication.claims.generation_id
    assert duplicate.result["publication_generation_id"] == publication.claims.generation_id
    assert first.result["publication_receipt_hash"] == publication.receipt_hash


def test_page_control_recovery_rejects_previous_key_signed_canvas_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = tmp_path / "data"
    outbox_path = tmp_path / "control.sqlite3"
    authority = create_rotating_canvas_ed25519_test_authority(tmp_path / "keys")
    command = SaveCanvas(
        command_id="recover-previous-key-canvas",
        requested_at=NOW,
        name="breakout",
        description="previous key recovery must fail",
        pool_refs=("n-shape-pool1",),
        source="canvas_page",
    )
    outbox = PageControlOutbox(outbox_path)
    original_finish_effect = outbox.finish_effect
    crashed = False

    def crash_before_effect_terminal(command_id: str, **kwargs: object):
        nonlocal crashed
        if not crashed and command_id == command.command_id:
            crashed = True
            raise KeyboardInterrupt("crash after previous-key catalog publish")
        return original_finish_effect(command_id, **kwargs)

    monkeypatch.setattr(outbox, "finish_effect", crash_before_effect_terminal)
    previous_service = PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=data_dir,
            log_dir=tmp_path / "logs",
            consumer_service_id="page-control-test",
            consumer_id="page-control-instance-1",
            clock=lambda: NOW,
            lease_seconds=1,
            canvas_publication_signer=authority.previous_signer,
            canvas_publication_keyring=authority.previous_keyring,
        ),
    )

    with pytest.raises(KeyboardInterrupt):
        previous_service.submit(command)

    assert (data_dir / "canvases" / "breakout.json").is_file()
    restarted = PageControlService(
        outbox=PageControlOutbox(outbox_path),
        consumer=PageControlConsumer(
            outbox=PageControlOutbox(outbox_path),
            data_dir=data_dir,
            log_dir=tmp_path / "logs",
            consumer_service_id="page-control-test",
            consumer_id="page-control-instance-1",
            clock=lambda: NOW + timedelta(seconds=2),
            lease_seconds=1,
            canvas_publication_signer=authority.active_signer,
            canvas_publication_keyring=authority.keyring,
        ),
    )

    receipt = restarted.submit(command)

    assert receipt.status is PageControlStatus.FAILED
    assert receipt.error is not None
    assert "active" in receipt.error


def test_canvas_publication_receipt_store_crash_before_publish_leaves_no_final_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.canvas_publication_receipt as publication_receipt
    from rquant.canvas_publication_receipt import CanvasPublicationReceipt

    data_dir = tmp_path / "data"
    outbox_path = tmp_path / "control.sqlite3"
    authority = create_canvas_ed25519_test_authority(tmp_path / "keys")
    command = SaveCanvas(
        command_id="receipt-store-crash-before-publish",
        requested_at=NOW,
        name="breakout",
        description="receipt store crash safe",
        pool_refs=("n-shape-pool1",),
        source="canvas_page",
    )
    original_fsync = publication_receipt.os.fsync
    crashed = False

    def crash_first_fsync(descriptor: int) -> None:
        nonlocal crashed
        if not crashed:
            crashed = True
            raise KeyboardInterrupt("crash before receipt publication")
        original_fsync(descriptor)

    monkeypatch.setattr(publication_receipt.os, "fsync", crash_first_fsync)
    outbox = PageControlOutbox(outbox_path)
    service = PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=data_dir,
            log_dir=tmp_path / "logs",
            consumer_service_id="page-control-test",
            consumer_id="receipt-store-instance",
            clock=lambda: NOW,
            lease_seconds=1,
            canvas_publication_signer=authority.signer,
            canvas_publication_keyring=authority.keyring,
        ),
    )

    with pytest.raises(KeyboardInterrupt):
        service.submit(command)

    receipt_root = data_dir / "canvas-publication-receipts"
    assert tuple(receipt_root.glob("*.json")) == ()
    monkeypatch.setattr(publication_receipt.os, "fsync", original_fsync)
    restarted = PageControlService(
        outbox=PageControlOutbox(outbox_path),
        consumer=PageControlConsumer(
            outbox=PageControlOutbox(outbox_path),
            data_dir=data_dir,
            log_dir=tmp_path / "logs",
            consumer_service_id="page-control-test",
            consumer_id="receipt-store-instance",
            clock=lambda: NOW + timedelta(seconds=2),
            lease_seconds=1,
            canvas_publication_signer=authority.signer,
            canvas_publication_keyring=authority.keyring,
        ),
    )

    receipt = restarted.submit(command)
    duplicate = restarted.submit(command)

    assert receipt.status is PageControlStatus.SUCCEEDED
    assert duplicate.result == receipt.result
    receipt_files = tuple(receipt_root.glob("*.json"))
    assert len(receipt_files) == 1
    publication = CanvasPublicationReceipt.model_validate_json(
        receipt_files[0].read_text(encoding="utf-8")
    )
    assert receipt.result is not None
    assert receipt.result["publication_receipt_id"] == publication.receipt_id


def test_page_control_restart_replays_incomplete_user_pool_once(tmp_path: Path) -> None:
    outbox_path = tmp_path / "control.sqlite3"
    command = SaveUserPool(
        command_id="restart-user-pool",
        requested_at=NOW,
        base_name="breakout",
        description="restart safe",
        rule_calls=(RuleCall(name="price_gt", args={"value": 10}),),
        include_columns=("close",),
    )
    outbox = PageControlOutbox(outbox_path)
    _claim_as_crashed(outbox, command)
    restarted_outbox = PageControlOutbox(outbox_path)
    restarted = _service_for(
        outbox=restarted_outbox,
        tmp_path=tmp_path,
        now=NOW + timedelta(seconds=2),
    )

    first = restarted.submit(command)
    duplicate = restarted.submit(command)

    assert first.status == duplicate.status == "succeeded"
    assert first.result == duplicate.result
    pool_path = tmp_path / "data" / "user_presets" / "breakout.json"
    assert json.loads(pool_path.read_text(encoding="utf-8"))["rules"][0]["name"] == "price_gt"


def test_page_control_restart_replays_incomplete_nl_preset_once(tmp_path: Path) -> None:
    outbox_path = tmp_path / "control.sqlite3"
    command = SaveNlPreset(
        command_id="restart-nl-preset",
        requested_at=NOW,
        name="自然语言条件",
        description="restart safe",
        rule_calls=(RuleCall(name="price_gt", args={"value": 10}),),
        include_columns=("close",),
    )
    outbox = PageControlOutbox(outbox_path)
    _claim_as_crashed(outbox, command)
    restarted_outbox = PageControlOutbox(outbox_path)
    restarted = _service_for(
        outbox=restarted_outbox,
        tmp_path=tmp_path,
        now=NOW + timedelta(seconds=2),
    )

    first = restarted.submit(command)
    duplicate = restarted.submit(command)

    assert first.status == duplicate.status == "succeeded"
    assert first.result == duplicate.result
    preset_path = tmp_path / "data" / "user_presets" / "自然语言条件.json"
    assert json.loads(preset_path.read_text(encoding="utf-8"))["source"] == "nl_input"


def test_page_control_restart_replays_incomplete_nl_log_once(tmp_path: Path) -> None:
    outbox_path = tmp_path / "control.sqlite3"
    command = AppendNlQueryLog(
        command_id="restart-nl-log",
        requested_at=NOW,
        query="价格大于10",
        plan={"trade_date": "2026-08-03"},
        outcome="success",
    )
    outbox = PageControlOutbox(outbox_path)
    _claim_as_crashed(outbox, command)
    restarted_outbox = PageControlOutbox(outbox_path)
    restarted = _service_for(
        outbox=restarted_outbox,
        tmp_path=tmp_path,
        now=NOW + timedelta(seconds=2),
    )

    first = restarted.submit(command)
    duplicate = restarted.submit(command)

    assert first.status == duplicate.status == "succeeded"
    assert first.result == duplicate.result
    lines = (tmp_path / "logs" / "nl_queries.jsonl").read_text(encoding="utf-8").splitlines()
    assert [json.loads(line)["command_id"] for line in lines] == ["restart-nl-log"]


def test_page_control_restart_replays_incomplete_lab_commands_once(tmp_path: Path) -> None:
    export_path = tmp_path / "exports" / "result.zip"
    export_path.parent.mkdir()
    backend = _LabControlBackendSpy(export_path)
    outbox_path = tmp_path / "control.sqlite3"
    outbox = PageControlOutbox(outbox_path)
    job_id = UUID(int=1)
    runtime_root = tmp_path / "runtime"
    commands = (
        InitializeLabExports(
            command_id="restart-lab-init",
            requested_at=NOW,
            export_root=export_path.parent,
            runtime_root=runtime_root,
        ),
        SubmitLabCommand(
            command_id="restart-lab-submit",
            requested_at=NOW,
            command=PauseJobCommand(
                job_id=job_id,
                expected_version=4,
                reason="page",
            ),
            interaction_key="pause:1:4",
        ),
        ExportLabArtifactZip(
            command_id="restart-lab-export",
            requested_at=NOW,
            job_id=job_id,
        ),
        DiscardLabArtifactZip(
            command_id="restart-lab-discard",
            requested_at=NOW,
            request_id=UUID(int=2),
            job_id=job_id,
            path=export_path,
            byte_size=3,
            sha256="a" * 64,
        ),
    )
    export_path.write_bytes(b"zip")
    for command in commands:
        _claim_as_crashed(outbox, command)

    restarted_outbox = PageControlOutbox(outbox_path)
    restarted = _service_for(
        outbox=restarted_outbox,
        tmp_path=tmp_path,
        lab_backend=backend,
        allowed_lab_export_roots=(export_path.parent, runtime_root),
        now=NOW + timedelta(seconds=2),
    )

    receipts = tuple(restarted.submit(command) for command in commands)
    duplicates = tuple(restarted.submit(command) for command in commands)

    assert all(receipt.status == "succeeded" for receipt in receipts)
    assert tuple(receipt.result for receipt in receipts) == tuple(
        receipt.result for receipt in duplicates
    )
    assert [name for name, _ in backend.calls] == ["submit", "export", "discard"]
    assert export_path.parent.is_dir()
    assert runtime_root.is_dir()


def test_page_control_consumer_mutex_rejects_symlink_lock_path(tmp_path: Path) -> None:
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")
    outside = tmp_path / "outside.lock"
    outside.write_text("outside", encoding="utf-8")
    (tmp_path / "control.sqlite3.consumer.lock").symlink_to(outside)

    with pytest.raises(ValueError, match="consumer mutex.*symlink|consumer mutex.*safely"):
        _service_for(outbox=outbox, tmp_path=tmp_path).submit(
            SaveCanvas(
                command_id="mutex-symlink",
                requested_at=NOW,
                name="breakout",
            )
        )

    assert not (tmp_path / "data" / "canvases" / "breakout.json").exists()


def test_page_control_consumer_mutex_fails_closed_when_lock_replaced_before_flock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outbox_path = tmp_path / "control.sqlite3"
    outbox = PageControlOutbox(outbox_path)
    lock_path = outbox_path.with_name(f"{outbox_path.name}.consumer.lock")
    original_flock = page_control.fcntl.flock
    replaced = False

    def replace_lock_path_before_flock(descriptor: int, operation: int) -> None:
        nonlocal replaced
        if not replaced and operation & page_control.fcntl.LOCK_EX:
            replaced = True
            lock_path.unlink(missing_ok=True)
            lock_path.write_text("replacement", encoding="utf-8")
        original_flock(descriptor, operation)

    monkeypatch.setattr(page_control.fcntl, "flock", replace_lock_path_before_flock)
    command = SaveUserPool(
        command_id="mutex-replaced-before-flock",
        requested_at=NOW,
        base_name="breakout",
        description="user pool",
        rule_calls=(RuleCall(name="price_gt", args={"value": 10}),),
        include_columns=("close",),
    )

    receipt = _service_for(outbox=outbox, tmp_path=tmp_path).submit(command)

    assert replaced is True
    assert receipt.status is PageControlStatus.PENDING
    assert not (tmp_path / "data" / "user_presets" / "breakout.json").exists()


def test_page_control_save_canvas_fails_closed_when_canvas_directory_rotates_after_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")
    data_dir = tmp_path / "data"
    canvas_dir = data_dir / "canvases"
    hidden_canvas_dir = tmp_path / "hidden-canvases"
    original_open_directory = page_control._open_or_create_managed_directory
    rotated = False

    def rotate_canvas_directory_after_open(path: Path) -> int:
        nonlocal rotated
        descriptor = original_open_directory(path)
        if not rotated and Path(path) == canvas_dir:
            rotated = True
            canvas_dir.rename(hidden_canvas_dir)
            canvas_dir.mkdir(mode=0o700)
        return descriptor

    monkeypatch.setattr(
        page_control,
        "_open_or_create_managed_directory",
        rotate_canvas_directory_after_open,
    )
    command = SaveCanvas(
        command_id="canvas-directory-rotated-after-open",
        requested_at=NOW,
        name="breakout",
        description="canvas",
        pool_refs=("pool1",),
    )

    receipt = _service_for(
        outbox=outbox,
        tmp_path=tmp_path,
        data_dir=data_dir,
    ).submit(command)

    assert rotated is True
    assert receipt.status in {PageControlStatus.FAILED, PageControlStatus.PENDING}
    assert not (canvas_dir / "breakout.json").exists()
    assert not (hidden_canvas_dir / "breakout.json").exists()


def test_page_control_save_canvas_directory_rotation_after_replace_crash_is_ambiguous(
    tmp_path: Path,
) -> None:
    outbox_path = tmp_path / "control.sqlite3"
    data_dir = tmp_path / "data"
    hidden_canvas_dir = tmp_path / "hidden-canvases"
    script = (
        "import os\n"
        "from datetime import UTC, datetime\n"
        "from pathlib import Path\n"
        "from rquant import page_control\n"
        "from rquant.page_control import PageControlConsumer, PageControlOutbox, "
        "PageControlService, SaveCanvas\n"
        "from tests.canvas_ed25519_support import create_canvas_ed25519_test_authority\n"
        "now = datetime(2026, 8, 3, 1, 30, tzinfo=UTC)\n"
        f"outbox_path = Path({str(outbox_path)!r})\n"
        f"data_dir = Path({str(data_dir)!r})\n"
        f"hidden_canvas_dir = Path({str(hidden_canvas_dir)!r})\n"
        "authority = create_canvas_ed25519_test_authority(data_dir / 'canvas-keys')\n"
        "canvas_dir = data_dir / 'canvases'\n"
        "original_replace = page_control.os.replace\n"
        "def replace_then_crash(src, dst, *, src_dir_fd=None, dst_dir_fd=None):\n"
        "    canvas_dir.rename(hidden_canvas_dir)\n"
        "    canvas_dir.mkdir(mode=0o700)\n"
        "    original_replace(src, dst, src_dir_fd=src_dir_fd, dst_dir_fd=dst_dir_fd)\n"
        "    os._exit(9)\n"
        "page_control.os.replace = replace_then_crash\n"
        "service = PageControlService(\n"
        "    outbox=PageControlOutbox(outbox_path),\n"
        "    consumer=PageControlConsumer(\n"
        "        outbox=PageControlOutbox(outbox_path),\n"
        "        data_dir=data_dir,\n"
        "        log_dir=data_dir / 'logs',\n"
        "        clock=lambda: now,\n"
        "        lease_seconds=1,\n"
        "        canvas_publication_signer=authority.signer,\n"
        "        canvas_publication_keyring=authority.keyring,\n"
        "    ),\n"
        ")\n"
        "service.submit(SaveCanvas(\n"
        "    command_id='after-replace-directory-rotation',\n"
        "    requested_at=now,\n"
        "    name='breakout',\n"
        "    description='after replace crash',\n"
        "    pool_refs=('n-shape-pool1',),\n"
        "))\n"
        "os._exit(13)\n"
    )

    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path.cwd(),
        text=True,
        capture_output=True,
        timeout=10,
        check=False,
    )
    assert completed.returncode == 9, completed.stderr
    hidden_canvas = hidden_canvas_dir / "breakout.json"
    visible_canvas = data_dir / "canvases" / "breakout.json"
    assert json.loads(hidden_canvas.read_text())["command_id"] == (
        "after-replace-directory-rotation"
    )
    assert not visible_canvas.exists()
    command = SaveCanvas(
        command_id="after-replace-directory-rotation",
        requested_at=NOW,
        name="breakout",
        description="after replace crash",
        pool_refs=("n-shape-pool1",),
    )

    receipt = _service_for(
        outbox=PageControlOutbox(outbox_path),
        tmp_path=tmp_path,
        data_dir=data_dir,
        now=NOW + timedelta(seconds=2),
    ).submit(command)
    duplicate = _service_for(
        outbox=PageControlOutbox(outbox_path),
        tmp_path=tmp_path,
        data_dir=data_dir,
        now=NOW + timedelta(seconds=3),
    ).submit(command)

    assert receipt.status is PageControlStatus.AMBIGUOUS
    assert duplicate.status is PageControlStatus.AMBIGUOUS
    assert not visible_canvas.exists()
    assert json.loads(hidden_canvas.read_text())["command_id"] == (
        "after-replace-directory-rotation"
    )


def test_page_control_started_local_effect_with_mismatched_directory_fence_is_ambiguous(
    tmp_path: Path,
) -> None:
    commands_and_targets = (
        (
            SaveUserPool(
                command_id="started-fence-user-pool",
                requested_at=NOW,
                base_name="breakout",
                description="user",
                rule_calls=(RuleCall(name="price_gt", args={"value": 10}),),
                include_columns=("close",),
            ),
            tmp_path / "data" / "user_presets",
            "breakout.json",
        ),
        (
            SaveNlPreset(
                command_id="started-fence-nl-preset",
                requested_at=NOW,
                name="自然语言条件",
                description="nl",
                rule_calls=(RuleCall(name="price_gt", args={"value": 10}),),
                include_columns=("close",),
            ),
            tmp_path / "data" / "user_presets",
            "自然语言条件.json",
        ),
        (
            AppendNlQueryLog(
                command_id="started-fence-nl-log",
                requested_at=NOW,
                query="价格大于10",
                plan={"trade_date": "2026-08-03"},
                outcome="success",
            ),
            tmp_path / "logs",
            "nl_queries.jsonl",
        ),
    )

    for command, visible_dir, file_name in commands_and_targets:
        outbox_path = tmp_path / f"{command.command_id}.sqlite3"
        outbox = PageControlOutbox(outbox_path)
        outbox.enqueue(command)
        claim = outbox.claim_records(
            limit=1,
            owner_id="crashed-worker",
            lease_seconds=1,
            now=NOW,
        )[0]
        outbox.begin_effect(
            command,
            owner_id=claim.owner_id,
            claim_token=claim.claim_token,
            now=NOW,
        )
        visible_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
        observed = visible_dir.stat()
        hidden_dir = tmp_path / f"hidden-{command.command_id}"
        visible_dir.rename(hidden_dir)
        visible_dir.mkdir(mode=0o700)
        started_fence = {
            "schema_version": 1,
            "kind": "local_filesystem_fence",
            "targets": [
                {
                    "role": "primary",
                    "path": str(visible_dir),
                    "st_dev": observed.st_dev,
                    "st_ino": observed.st_ino,
                }
            ],
        }
        with sqlite3.connect(outbox_path) as connection:
            connection.execute(
                """
                UPDATE page_control_effect
                SET result_json = ?
                WHERE command_id = ?
                """,
                (json.dumps(started_fence, ensure_ascii=True), command.command_id),
            )

        receipt = _service_for(
            outbox=PageControlOutbox(outbox_path),
            tmp_path=tmp_path,
            data_dir=tmp_path / "data",
            log_dir=tmp_path / "logs",
            now=NOW + timedelta(seconds=2),
        ).submit(command)
        duplicate = _service_for(
            outbox=PageControlOutbox(outbox_path),
            tmp_path=tmp_path,
            data_dir=tmp_path / "data",
            log_dir=tmp_path / "logs",
            now=NOW + timedelta(seconds=3),
        ).submit(command)

        assert receipt.status is PageControlStatus.AMBIGUOUS
        assert duplicate.status is PageControlStatus.AMBIGUOUS
        assert not (visible_dir / file_name).exists()
        assert not (hidden_dir / file_name).exists()


def test_page_control_consumer_mutex_releases_after_process_exit(tmp_path: Path) -> None:
    outbox_path = tmp_path / "control.sqlite3"
    PageControlOutbox(outbox_path)
    lock_path = outbox_path.with_name(f"{outbox_path.name}.consumer.lock")
    script = (
        "import os\n"
        "from pathlib import Path\n"
        "from rquant.page_control import _PageControlExecutionMutex\n"
        f"with _PageControlExecutionMutex(Path({str(lock_path)!r})) as acquired:\n"
        "    os._exit(0 if acquired else 7)\n"
    )

    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path.cwd(),
        text=True,
        capture_output=True,
        timeout=10,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr

    receipt = _service_for(outbox=PageControlOutbox(outbox_path), tmp_path=tmp_path).submit(
        SaveCanvas(
            command_id="mutex-process-release",
            requested_at=NOW,
            name="breakout",
        )
    )

    assert receipt.status is PageControlStatus.SUCCEEDED


def test_page_control_save_canvas_resumes_after_effect_crash_without_rewriting(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = tmp_path / "data"
    outbox_path = tmp_path / "control.sqlite3"
    outbox = PageControlOutbox(outbox_path)
    service = _service_for(outbox=outbox, tmp_path=tmp_path, data_dir=data_dir)
    original_atomic_json = page_control.PageControlConsumer._atomic_json
    writes: list[Path] = []
    crashed = False

    def crash_after_write(path: Path, payload: object, *, command_id: str) -> None:
        nonlocal crashed
        writes.append(path)
        original_atomic_json(path, payload, command_id=command_id)
        if not crashed:
            crashed = True
            raise KeyboardInterrupt("crash after canvas write")

    monkeypatch.setattr(
        page_control.PageControlConsumer,
        "_atomic_json",
        staticmethod(crash_after_write),
    )
    command = SaveCanvas(
        command_id="after-effect-canvas",
        requested_at=NOW,
        name="breakout",
        description="after effect",
        pool_refs=("n-shape-pool1",),
    )

    with pytest.raises(KeyboardInterrupt):
        service.submit(command)
    receipt = _service_for(
        outbox=PageControlOutbox(outbox_path),
        tmp_path=tmp_path,
        data_dir=data_dir,
        now=NOW + timedelta(seconds=2),
    ).submit(command)

    assert receipt.status is PageControlStatus.SUCCEEDED
    assert writes == [data_dir / "canvases" / "breakout.json"]
    assert json.loads((data_dir / "canvases" / "breakout.json").read_text())[
        "command_id"
    ] == "after-effect-canvas"


def test_page_control_set_canvas_pool_refs_resumes_after_effect_crash_without_rewriting(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = tmp_path / "data"
    outbox_path = tmp_path / "control.sqlite3"
    setup_outbox = PageControlOutbox(outbox_path)
    _service_for(outbox=setup_outbox, tmp_path=tmp_path, data_dir=data_dir).submit(
        SaveCanvas(
            command_id="setup-canvas",
            requested_at=NOW,
            name="breakout",
            description="setup",
            pool_refs=("n-shape-pool1",),
        )
    )
    outbox = PageControlOutbox(outbox_path)
    service = _service_for(outbox=outbox, tmp_path=tmp_path, data_dir=data_dir)
    original_atomic_json = page_control.PageControlConsumer._atomic_json
    writes: list[Path] = []
    crashed = False

    def crash_after_write(path: Path, payload: object, *, command_id: str) -> None:
        nonlocal crashed
        writes.append(path)
        original_atomic_json(path, payload, command_id=command_id)
        if not crashed:
            crashed = True
            raise KeyboardInterrupt("crash after canvas refs write")

    monkeypatch.setattr(
        page_control.PageControlConsumer,
        "_atomic_json",
        staticmethod(crash_after_write),
    )
    command = SetCanvasPoolRefs(
        command_id="after-effect-set-canvas",
        requested_at=NOW,
        name="breakout",
        pool_refs=("n-shape-pool1", "user/strong"),
    )

    with pytest.raises(KeyboardInterrupt):
        service.submit(command)
    receipt = _service_for(
        outbox=PageControlOutbox(outbox_path),
        tmp_path=tmp_path,
        data_dir=data_dir,
        now=NOW + timedelta(seconds=2),
    ).submit(command)

    assert receipt.status is PageControlStatus.SUCCEEDED
    assert writes == [data_dir / "canvases" / "breakout.json"]
    assert json.loads((data_dir / "canvases" / "breakout.json").read_text())[
        "pool_refs"
    ] == ["n-shape-pool1", "user/strong"]


def test_page_control_user_pool_commands_resume_after_effect_crash_without_rewriting(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = tmp_path / "data"
    original_atomic_json = page_control.PageControlConsumer._atomic_json
    writes: list[Path] = []
    crashed_commands: set[str] = set()

    def crash_each_command_after_first_write(
        path: Path,
        payload: object,
        *,
        command_id: str,
    ) -> None:
        writes.append(path)
        original_atomic_json(path, payload, command_id=command_id)
        if command_id not in crashed_commands:
            crashed_commands.add(command_id)
            raise KeyboardInterrupt(f"crash after user pool write: {command_id}")

    monkeypatch.setattr(
        page_control.PageControlConsumer,
        "_atomic_json",
        staticmethod(crash_each_command_after_first_write),
    )
    commands = (
        SaveUserPool(
            command_id="after-effect-user-pool",
            requested_at=NOW,
            base_name="breakout",
            description="user",
            rule_calls=(RuleCall(name="price_gt", args={"value": 10}),),
            include_columns=("close",),
        ),
        SaveNlPreset(
            command_id="after-effect-nl-preset",
            requested_at=NOW,
            name="自然语言条件",
            description="nl",
            rule_calls=(RuleCall(name="price_gt", args={"value": 10}),),
            include_columns=("close",),
        ),
        ForkBuiltinPool(
            command_id="after-effect-fork",
            requested_at=NOW,
            builtin_name="n-shape-pool1",
            target_base_name="forked",
        ),
    )

    for command in commands:
        outbox_path = tmp_path / f"{command.command_id}.sqlite3"
        service = _service_for(
            outbox=PageControlOutbox(outbox_path),
            tmp_path=tmp_path,
            data_dir=data_dir,
        )
        with pytest.raises(KeyboardInterrupt):
            service.submit(command)
        receipt = _service_for(
            outbox=PageControlOutbox(outbox_path),
            tmp_path=tmp_path,
            data_dir=data_dir,
            now=NOW + timedelta(seconds=2),
        ).submit(command)
        assert receipt.status is PageControlStatus.SUCCEEDED

    assert writes == [
        data_dir / "user_presets" / "breakout.json",
        data_dir / "user_presets" / "自然语言条件.json",
        data_dir / "user_presets" / "forked.json",
    ]


def test_page_control_save_user_pool_canvas_chain_resumes_partial_effect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = tmp_path / "data"
    outbox_path = tmp_path / "control.sqlite3"
    setup_outbox = PageControlOutbox(outbox_path)
    _service_for(outbox=setup_outbox, tmp_path=tmp_path, data_dir=data_dir).submit(
        SaveCanvas(
            command_id="setup-chain-canvas",
            requested_at=NOW,
            name="breakout",
            description="setup",
        )
    )
    original_atomic_json = page_control.PageControlConsumer._atomic_json
    writes: list[Path] = []
    crashed = False

    def crash_after_pool_write(path: Path, payload: object, *, command_id: str) -> None:
        nonlocal crashed
        writes.append(path)
        original_atomic_json(path, payload, command_id=command_id)
        if not crashed:
            crashed = True
            raise KeyboardInterrupt("crash after pool write before canvas link")

    monkeypatch.setattr(
        page_control.PageControlConsumer,
        "_atomic_json",
        staticmethod(crash_after_pool_write),
    )
    command = SaveUserPool(
        command_id="after-effect-pool-chain",
        requested_at=NOW,
        base_name="breakout",
        description="user",
        rule_calls=(RuleCall(name="price_gt", args={"value": 10}),),
        include_columns=("close",),
        canvas_name="breakout",
    )

    with pytest.raises(KeyboardInterrupt):
        _service_for(
            outbox=PageControlOutbox(outbox_path),
            tmp_path=tmp_path,
            data_dir=data_dir,
        ).submit(command)
    receipt = _service_for(
        outbox=PageControlOutbox(outbox_path),
        tmp_path=tmp_path,
        data_dir=data_dir,
        now=NOW + timedelta(seconds=2),
    ).submit(command)

    assert receipt.status is PageControlStatus.SUCCEEDED
    assert writes == [
        data_dir / "user_presets" / "breakout.json",
        data_dir / "canvases" / "breakout.json",
    ]
    assert json.loads((data_dir / "canvases" / "breakout.json").read_text())[
        "pool_refs"
    ] == ["user/breakout"]


def test_page_control_fork_builtin_canvas_chain_resumes_partial_effect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = tmp_path / "data"
    outbox_path = tmp_path / "fork-control.sqlite3"
    _service_for(
        outbox=PageControlOutbox(outbox_path),
        tmp_path=tmp_path,
        data_dir=data_dir,
    ).submit(
        SaveCanvas(
            command_id="setup-fork-chain-canvas",
            requested_at=NOW - timedelta(minutes=1),
            name="breakout",
            description="setup",
        )
    )
    original_atomic_json = page_control.PageControlConsumer._atomic_json
    writes: list[Path] = []
    crashed = False

    def crash_after_pool_write(path: Path, payload: object, *, command_id: str) -> None:
        nonlocal crashed
        writes.append(path)
        original_atomic_json(path, payload, command_id=command_id)
        if not crashed:
            crashed = True
            raise KeyboardInterrupt("crash after fork pool write before canvas link")

    monkeypatch.setattr(
        page_control.PageControlConsumer,
        "_atomic_json",
        staticmethod(crash_after_pool_write),
    )
    command = ForkBuiltinPool(
        command_id="after-effect-fork-chain",
        requested_at=NOW,
        builtin_name="n-shape-pool1",
        target_base_name="forked",
        canvas_name="breakout",
    )

    with pytest.raises(KeyboardInterrupt):
        _service_for(
            outbox=PageControlOutbox(outbox_path),
            tmp_path=tmp_path,
            data_dir=data_dir,
        ).submit(command)
    receipt = _service_for(
        outbox=PageControlOutbox(outbox_path),
        tmp_path=tmp_path,
        data_dir=data_dir,
        now=NOW + timedelta(seconds=2),
    ).submit(command)

    assert receipt.status is PageControlStatus.SUCCEEDED
    assert isinstance(receipt.result, dict)
    assert isinstance(receipt.result["canvas_result"], dict)
    assert writes == [
        data_dir / "user_presets" / "forked.json",
        data_dir / "canvases" / "breakout.json",
    ]
    assert json.loads((data_dir / "canvases" / "breakout.json").read_text())[
        "pool_refs"
    ] == ["user/forked"]


def test_page_control_mutex_blocks_local_stale_reclaim_before_canvas_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = tmp_path / "data"
    authority = _canvas_authority(tmp_path)
    outbox_path = tmp_path / "control.sqlite3"
    setup_outbox = PageControlOutbox(outbox_path)
    _service_for(outbox=setup_outbox, tmp_path=tmp_path, data_dir=data_dir).submit(
        SaveCanvas(
            command_id="setup-local-stale-canvas",
            requested_at=NOW,
            name="breakout",
            description="setup",
        )
    )
    outbox = PageControlOutbox(outbox_path)
    newer_receipts: tuple[PageControlReceipt, ...] | None = None
    reentered = False
    original_add_pool = page_control.PageControlConsumer._add_pool_to_canvas

    def reenter_before_canvas_write(
        self: PageControlConsumer,
        canvas_name: str,
        pool_name: str,
        *,
        identity_command: object | None = None,
    ):
        nonlocal newer_receipts, reentered
        if not reentered:
            reentered = True
            newer_receipts = PageControlConsumer(
                outbox=PageControlOutbox(outbox_path),
                data_dir=data_dir,
                log_dir=tmp_path / "logs",
                clock=lambda: NOW + timedelta(seconds=2),
                lease_seconds=1,
                consumer_id="new-local-owner",
                canvas_publication_signer=authority.signer,
                canvas_publication_keyring=authority.keyring,
            ).drain(limit=1)
        return original_add_pool(
            self,
            canvas_name,
            pool_name,
            identity_command=identity_command,
        )

    monkeypatch.setattr(
        page_control.PageControlConsumer,
        "_add_pool_to_canvas",
        reenter_before_canvas_write,
    )
    command = SaveUserPool(
        command_id="stale-local-pool-chain",
        requested_at=NOW,
        base_name="breakout",
        description="user",
        rule_calls=(RuleCall(name="price_gt", args={"value": 10}),),
        include_columns=("close",),
        canvas_name="breakout",
    )

    receipt = PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=data_dir,
            log_dir=tmp_path / "logs",
            clock=lambda: NOW,
            lease_seconds=1,
            consumer_id="old-local-owner",
            canvas_publication_signer=authority.signer,
            canvas_publication_keyring=authority.keyring,
        ),
    ).submit(command)

    assert receipt.status is PageControlStatus.SUCCEEDED
    assert newer_receipts == ()
    assert isinstance(receipt.result, dict)
    assert isinstance(receipt.result["canvas_result"], dict)
    canvas_record = json.loads((data_dir / "canvases" / "breakout.json").read_text())
    assert canvas_record["record_hash"] == receipt.result["canvas_result"]["record_hash"]


def test_page_control_append_log_resumes_after_effect_crash_without_duplicate_line(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outbox_path = tmp_path / "control.sqlite3"
    original_append = page_control._append_managed_jsonl
    appends = 0

    def crash_after_append(path: Path, record: object, *, command_id: str) -> None:
        nonlocal appends
        appends += 1
        original_append(path, record, command_id=command_id)
        if appends == 1:
            raise KeyboardInterrupt("crash after log append")

    monkeypatch.setattr(page_control, "_append_managed_jsonl", crash_after_append)
    command = AppendNlQueryLog(
        command_id="after-effect-nl-log",
        requested_at=NOW,
        query="价格大于10",
        outcome="success",
    )

    with pytest.raises(KeyboardInterrupt):
        _service_for(outbox=PageControlOutbox(outbox_path), tmp_path=tmp_path).submit(command)
    receipt = _service_for(
        outbox=PageControlOutbox(outbox_path),
        tmp_path=tmp_path,
        now=NOW + timedelta(seconds=2),
    ).submit(command)

    assert receipt.status is PageControlStatus.SUCCEEDED
    assert appends == 1
    lines = (tmp_path / "logs" / "nl_queries.jsonl").read_text(encoding="utf-8").splitlines()
    assert [json.loads(line)["command_id"] for line in lines] == ["after-effect-nl-log"]


def test_page_control_deletes_resume_after_unlink_crash_with_stable_deleted_true(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = tmp_path / "data"
    canvas_path = data_dir / "canvases" / "breakout.json"
    pool_path = data_dir / "user_presets" / "breakout.json"
    canvas_outbox_path = tmp_path / "after-effect-delete-canvas.sqlite3"
    setup_receipt = _service_for(
        outbox=PageControlOutbox(canvas_outbox_path),
        tmp_path=tmp_path,
        data_dir=data_dir,
    ).submit(
        SaveCanvas(
            command_id="delete-setup-canvas",
            requested_at=NOW - timedelta(minutes=1),
            name="breakout",
        )
    )
    assert setup_receipt.status is PageControlStatus.SUCCEEDED
    pool_path.parent.mkdir(parents=True)
    pool_path.write_text("{}", encoding="utf-8")
    original_delete = page_control.PageControlConsumer._delete
    deletes: list[Path] = []
    crashed_commands: set[Path] = set()

    def crash_after_delete(path: Path) -> bool:
        deleted = original_delete(path)
        deletes.append(path)
        if path not in crashed_commands:
            crashed_commands.add(path)
            raise KeyboardInterrupt("crash after unlink")
        return deleted

    monkeypatch.setattr(
        page_control.PageControlConsumer,
        "_delete",
        staticmethod(crash_after_delete),
    )
    commands = (
        DeleteCanvas(command_id="after-effect-delete-canvas", requested_at=NOW, name="breakout"),
        DeleteUserPool(
            command_id="after-effect-delete-pool",
            requested_at=NOW,
            base_name="breakout",
        ),
    )

    for command in commands:
        outbox_path = (
            canvas_outbox_path
            if isinstance(command, DeleteCanvas)
            else tmp_path / f"{command.command_id}.sqlite3"
        )
        with pytest.raises(KeyboardInterrupt):
            _service_for(
                outbox=PageControlOutbox(outbox_path),
                tmp_path=tmp_path,
                data_dir=data_dir,
            ).submit(command)
        receipt = _service_for(
            outbox=PageControlOutbox(outbox_path),
            tmp_path=tmp_path,
            data_dir=data_dir,
            now=NOW + timedelta(seconds=2),
        ).submit(command)
        assert receipt.status is PageControlStatus.SUCCEEDED
        assert receipt.result == {"deleted": True}

    assert deletes == [canvas_path, pool_path]


def test_page_control_delete_recovers_ordinary_error_after_committed_tombstone(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = tmp_path / "data"
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")
    service = _service_for(outbox=outbox, tmp_path=tmp_path, data_dir=data_dir)
    setup = service.submit(
        SaveCanvas(
            command_id="ordinary-delete-setup",
            requested_at=NOW - timedelta(minutes=1),
            name="breakout",
        )
    )
    assert setup.status is PageControlStatus.SUCCEEDED
    original_delete = page_control.PageControlConsumer._delete
    delete_calls = 0

    def fail_once_after_unlink(path: Path) -> bool:
        nonlocal delete_calls
        delete_calls += 1
        deleted = original_delete(path)
        if delete_calls == 1:
            raise OSError("ordinary failure after committed canvas unlink")
        return deleted

    monkeypatch.setattr(
        page_control.PageControlConsumer,
        "_delete",
        staticmethod(fail_once_after_unlink),
    )
    command = DeleteCanvas(
        command_id="ordinary-delete",
        requested_at=NOW,
        name="breakout",
    )

    receipt = service.submit(command)
    duplicate = service.submit(command)

    assert receipt.status is PageControlStatus.SUCCEEDED
    assert duplicate.status is PageControlStatus.SUCCEEDED
    assert receipt.result == duplicate.result == {"deleted": True}
    assert delete_calls == 1
    assert not (data_dir / "canvases" / "breakout.json").exists()


def test_page_control_delete_retries_persistent_unlink_after_signed_tombstone(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = tmp_path / "data"
    outbox_path = tmp_path / "control.sqlite3"
    first_outbox = PageControlOutbox(outbox_path)
    first_service = _service_for(
        outbox=first_outbox,
        tmp_path=tmp_path,
        data_dir=data_dir,
    )
    setup = first_service.submit(
        SaveCanvas(
            command_id="persistent-delete-setup",
            requested_at=NOW - timedelta(minutes=1),
            name="breakout",
        )
    )
    assert setup.status is PageControlStatus.SUCCEEDED
    original_delete = page_control.PageControlConsumer._delete
    failed_unlinks = 0

    def fail_persistently(_path: Path) -> bool:
        nonlocal failed_unlinks
        failed_unlinks += 1
        raise OSError("persistent canvas unlink failure")

    monkeypatch.setattr(
        page_control.PageControlConsumer,
        "_delete",
        staticmethod(fail_persistently),
    )
    command = DeleteCanvas(
        command_id="persistent-delete",
        requested_at=NOW,
        name="breakout",
    )

    retryable = first_service.submit(command)
    head_root = data_dir / "canvas-publication-heads" / "breakout"
    committed_heads = tuple(sorted(path.name for path in head_root.glob("*.json")))
    effect = first_outbox.effect(command.command_id)
    with sqlite3.connect(outbox_path) as connection:
        command_state = connection.execute(
            """
            SELECT status, processing_owner, lease_expires_at, claim_token
            FROM page_control_command WHERE command_id = ?
            """,
            (command.command_id,),
        ).fetchone()

    assert retryable.status is PageControlStatus.PENDING
    assert retryable.completed_at is None
    assert retryable.error is None
    assert command_state == (PageControlStatus.PENDING.value, None, None, None)
    assert effect is not None
    assert effect.status.value == "started"
    assert failed_unlinks >= 2
    assert len(committed_heads) == 2
    assert (data_dir / "canvases" / "breakout.json").exists()

    monkeypatch.setattr(
        page_control.PageControlConsumer,
        "_delete",
        staticmethod(original_delete),
    )
    restarted_outbox = PageControlOutbox(outbox_path)
    restarted_service = _service_for(
        outbox=restarted_outbox,
        tmp_path=tmp_path,
        data_dir=data_dir,
        now=NOW + timedelta(seconds=2),
    )

    recovered = restarted_service.submit(command)
    duplicate = restarted_service.submit(command)

    assert recovered.status is PageControlStatus.SUCCEEDED
    assert duplicate.status is PageControlStatus.SUCCEEDED
    assert recovered.result == duplicate.result == {"deleted": True}
    assert recovered.completed_at == duplicate.completed_at
    assert tuple(sorted(path.name for path in head_root.glob("*.json"))) == committed_heads
    assert not (data_dir / "canvases" / "breakout.json").exists()


def test_page_control_delete_restart_repairs_watermark_after_head_publish_crash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = tmp_path / "data"
    outbox_path = tmp_path / "control.sqlite3"
    authority = _canvas_authority(tmp_path)
    service = _service_for(
        outbox=PageControlOutbox(outbox_path),
        tmp_path=tmp_path,
        data_dir=data_dir,
    )
    setup = service.submit(
        SaveCanvas(
            command_id="watermark-crash-setup",
            requested_at=NOW - timedelta(minutes=1),
            name="breakout",
        )
    )
    assert setup.status is PageControlStatus.SUCCEEDED
    original_write = page_control.CanvasPublicationReceiptStore.write_immutable
    crashed = False

    def crash_before_delete_watermark(
        store: object,
        publication: object,
    ) -> Path:
        nonlocal crashed
        if (
            not crashed
            and "canvas-publication-watermarks" in store.root.parts
            and publication.claims.command.command_id == "watermark-crash-delete"
        ):
            crashed = True
            raise KeyboardInterrupt("crash after tombstone head before watermark")
        return original_write(store, publication)

    monkeypatch.setattr(
        page_control.CanvasPublicationReceiptStore,
        "write_immutable",
        crash_before_delete_watermark,
    )
    command = DeleteCanvas(
        command_id="watermark-crash-delete",
        requested_at=NOW,
        name="breakout",
    )

    with pytest.raises(KeyboardInterrupt):
        service.submit(command)

    monkeypatch.setattr(
        page_control.CanvasPublicationReceiptStore,
        "write_immutable",
        original_write,
    )
    restarted = _service_for(
        outbox=PageControlOutbox(outbox_path),
        tmp_path=tmp_path,
        data_dir=data_dir,
        now=NOW + timedelta(seconds=2),
    )
    recovered = restarted.submit(command)
    head = page_control.read_canvas_current_head(
        data_dir / "canvas-publication-heads",
        "breakout",
        authority.keyring,
    )
    watermark = page_control.read_canvas_current_head(
        data_dir / "canvas-publication-watermarks",
        "breakout",
        authority.keyring,
    )

    assert crashed is True
    assert recovered.status is PageControlStatus.SUCCEEDED
    assert head is not None and watermark is not None
    assert head.receipt.receipt_id == watermark.receipt.receipt_id
    assert not (data_dir / "canvases" / "breakout.json").exists()


def test_page_control_initialize_lab_exports_resumes_after_directory_creation_crash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    export_root = tmp_path / "exports"
    runtime_root = tmp_path / "runtime"
    outbox_path = tmp_path / "control.sqlite3"
    original_open = page_control._open_or_create_managed_directory
    opened: list[Path] = []
    crashed = False

    def crash_after_open(path: Path) -> int:
        nonlocal crashed
        descriptor = original_open(path)
        if path not in {export_root, runtime_root}:
            return descriptor
        opened.append(path)
        if not crashed:
            crashed = True
            os_path = Path(path)
            assert os_path.is_dir()
            raise KeyboardInterrupt("crash after directory creation")
        return descriptor

    monkeypatch.setattr(page_control, "_open_or_create_managed_directory", crash_after_open)
    command = InitializeLabExports(
        command_id="after-effect-init-lab",
        requested_at=NOW,
        export_root=export_root,
        runtime_root=runtime_root,
    )

    with pytest.raises(KeyboardInterrupt):
        _service_for(
            outbox=PageControlOutbox(outbox_path),
            tmp_path=tmp_path,
            allowed_lab_export_roots=(export_root, runtime_root),
        ).submit(command)
    receipt = _service_for(
        outbox=PageControlOutbox(outbox_path),
        tmp_path=tmp_path,
        allowed_lab_export_roots=(export_root, runtime_root),
        now=NOW + timedelta(seconds=2),
    ).submit(command)

    assert receipt.status is PageControlStatus.SUCCEEDED
    assert receipt.result == {"paths": [str(export_root), str(runtime_root)]}
    assert export_root.is_dir()
    assert runtime_root.is_dir()
    assert opened == [export_root, runtime_root]


def test_page_control_lab_backend_effect_crash_is_ambiguous_and_not_replayed(
    tmp_path: Path,
) -> None:
    export_path = tmp_path / "exports" / "result.zip"
    export_path.parent.mkdir()
    job_id = UUID(int=1)
    commands = (
        SubmitLabCommand(
            command_id="after-effect-lab-submit",
            requested_at=NOW,
            command=PauseJobCommand(job_id=job_id, expected_version=4, reason="page"),
            interaction_key="pause:1:4",
        ),
        ExportLabArtifactZip(
            command_id="after-effect-lab-export",
            requested_at=NOW,
            job_id=job_id,
        ),
        DiscardLabArtifactZip(
            command_id="after-effect-lab-discard",
            requested_at=NOW,
            request_id=UUID(int=2),
            job_id=job_id,
            path=export_path,
            byte_size=3,
            sha256="a" * 64,
        ),
    )

    for command in commands:
        backend = _CrashAfterLabEffectBackend(export_path)
        outbox_path = tmp_path / f"{command.command_id}.sqlite3"
        if isinstance(command, DiscardLabArtifactZip):
            export_path.write_bytes(b"zip")
        with pytest.raises(KeyboardInterrupt):
            _service_for(
                outbox=PageControlOutbox(outbox_path),
                tmp_path=tmp_path,
                lab_backend=backend,
            ).submit(command)
        receipt = _service_for(
            outbox=PageControlOutbox(outbox_path),
            tmp_path=tmp_path,
            lab_backend=backend,
            now=NOW + timedelta(seconds=2),
        ).submit(command)
        _assert_ambiguous_at_most_once(receipt)
        expected_call = {
            "submit_lab_command": "submit",
            "export_lab_artifact_zip": "export",
            "discard_lab_artifact_zip": "discard",
        }[command.kind]
        assert backend.calls == [expected_call]


def test_page_control_lab_effect_result_resumes_after_final_receipt_crash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    export_path = tmp_path / "exports" / "result.zip"
    export_path.parent.mkdir()
    job_id = UUID(int=1)
    commands = (
        SubmitLabCommand(
            command_id="receipt-crash-lab-submit",
            requested_at=NOW,
            command=PauseJobCommand(job_id=job_id, expected_version=4, reason="page"),
            interaction_key="pause:1:4",
        ),
        ExportLabArtifactZip(
            command_id="receipt-crash-lab-export",
            requested_at=NOW,
            job_id=job_id,
        ),
        DiscardLabArtifactZip(
            command_id="receipt-crash-lab-discard",
            requested_at=NOW,
            request_id=UUID(int=2),
            job_id=job_id,
            path=export_path,
            byte_size=3,
            sha256="a" * 64,
        ),
    )

    for command in commands:
        backend = _LabControlBackendSpy(export_path)
        outbox_path = tmp_path / f"{command.command_id}.sqlite3"
        outbox = PageControlOutbox(outbox_path)
        if isinstance(command, DiscardLabArtifactZip):
            export_path.write_bytes(b"zip")
        _crash_complete_once(monkeypatch, outbox)
        with pytest.raises(KeyboardInterrupt):
            _service_for(
                outbox=outbox,
                tmp_path=tmp_path,
                lab_backend=backend,
            ).submit(command)
        receipt = _service_for(
            outbox=PageControlOutbox(outbox_path),
            tmp_path=tmp_path,
            lab_backend=backend,
            now=NOW + timedelta(seconds=2),
        ).submit(command)

        assert receipt.status is PageControlStatus.SUCCEEDED
        assert [name for name, _payload in backend.calls] == [
            {
                "submit_lab_command": "submit",
                "export_lab_artifact_zip": "export",
                "discard_lab_artifact_zip": "discard",
            }[command.kind]
        ]


def test_page_control_legacy_processing_external_lab_without_effect_is_ambiguous(
    tmp_path: Path,
) -> None:
    outbox_path = tmp_path / "legacy.sqlite3"
    command = SubmitLabCommand(
        command_id="legacy-processing-lab",
        requested_at=NOW - timedelta(minutes=5),
        command=PauseJobCommand(job_id=UUID(int=1), expected_version=4, reason="page"),
        interaction_key="pause:1:4",
    )
    connection = sqlite3.connect(outbox_path)
    try:
        connection.execute(
            """
            CREATE TABLE page_control_command (
                command_id TEXT PRIMARY KEY,
                command_kind TEXT NOT NULL,
                command_hash TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                status TEXT NOT NULL,
                enqueued_at TEXT NOT NULL,
                completed_at TEXT,
                result_json TEXT,
                error TEXT,
                processing_owner TEXT,
                lease_expires_at TEXT,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                claim_token TEXT
            )
            """
        )
        connection.execute(
            """
            INSERT INTO page_control_command(
                command_id, command_kind, command_hash, payload_json, status,
                enqueued_at, processing_owner, lease_expires_at, attempt_count,
                claim_token
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                command.command_id,
                command.kind,
                canonical_sha256(command.model_dump(mode="json")),
                command.model_dump_json(),
                "processing",
                command.requested_at.isoformat(timespec="microseconds"),
                "pre-journal-worker",
                (NOW - timedelta(seconds=1)).isoformat(timespec="microseconds"),
                1,
                "pre-journal-token",
            ),
        )
        connection.commit()
    finally:
        connection.close()
    backend = _LabControlBackendSpy(tmp_path / "exports" / "result.zip")

    receipt = _service_for(
        outbox=PageControlOutbox(outbox_path),
        tmp_path=tmp_path,
        lab_backend=backend,
        now=NOW + timedelta(seconds=2),
    ).submit(command)

    _assert_ambiguous_at_most_once(receipt)
    assert backend.calls == []


def test_page_control_first_safe_activation_terminalizes_partial_migrated_external_lab(
    tmp_path: Path,
) -> None:
    outbox_path = tmp_path / "partial.sqlite3"
    export_path = tmp_path / "exports" / "result.zip"
    job_id = UUID(int=1)
    commands = (
        SubmitLabCommand(
            command_id="partial-submit",
            requested_at=NOW - timedelta(minutes=5),
            command=PauseJobCommand(job_id=job_id, expected_version=4, reason="page"),
            interaction_key="pause:1:4",
        ),
        ExportLabArtifactZip(
            command_id="partial-export",
            requested_at=NOW - timedelta(minutes=5),
            job_id=job_id,
        ),
        DiscardLabArtifactZip(
            command_id="partial-discard",
            requested_at=NOW - timedelta(minutes=5),
            request_id=UUID(int=2),
            job_id=job_id,
            path=export_path,
            byte_size=3,
            sha256="a" * 64,
        ),
    )
    connection = _create_partial_migrated_page_control_db(outbox_path)
    try:
        for command in commands:
            _insert_processing_command(
                connection,
                command,
                lease_expires_at=datetime(1970, 1, 1, tzinfo=UTC),
            )
        connection.commit()
    finally:
        connection.close()
    backend = _LabControlBackendSpy(export_path)

    outbox = PageControlOutbox(outbox_path)
    receipts = tuple(
        _service_for(
            outbox=outbox,
            tmp_path=tmp_path,
            lab_backend=backend,
            now=NOW + timedelta(seconds=2),
        ).submit(command)
        for command in commands
    )
    reopened_receipts = tuple(
        _service_for(
            outbox=PageControlOutbox(outbox_path),
            tmp_path=tmp_path,
            lab_backend=backend,
            now=NOW + timedelta(seconds=3),
        ).submit(command)
        for command in commands
    )

    for receipt in (*receipts, *reopened_receipts):
        _assert_ambiguous_at_most_once(receipt)
    assert backend.calls == []
    marker_connection = sqlite3.connect(outbox_path)
    try:
        markers = marker_connection.execute(
            """
            SELECT marker_name FROM page_control_protocol_activation
            WHERE marker_name = 'safe-effect-journal-v2'
            """
        ).fetchall()
    finally:
        marker_connection.close()
    assert markers == [("safe-effect-journal-v2",)]


def test_page_control_first_safe_activation_preserves_nonexpired_external_processing(
    tmp_path: Path,
) -> None:
    outbox_path = tmp_path / "partial-nonexpired.sqlite3"
    command = SubmitLabCommand(
        command_id="partial-nonexpired-submit",
        requested_at=NOW,
        command=PauseJobCommand(job_id=UUID(int=1), expected_version=4, reason="page"),
        interaction_key="pause:1:4",
    )
    connection = _create_partial_migrated_page_control_db(outbox_path)
    try:
        _insert_processing_command(
            connection,
            command,
            lease_expires_at=datetime(2999, 1, 1, tzinfo=UTC),
        )
        connection.commit()
    finally:
        connection.close()
    backend = _LabControlBackendSpy(tmp_path / "exports" / "result.zip")

    receipt = _service_for(
        outbox=PageControlOutbox(outbox_path),
        tmp_path=tmp_path,
        lab_backend=backend,
        now=NOW + timedelta(seconds=2),
    ).submit(command)

    assert receipt.status is PageControlStatus.PROCESSING
    assert backend.calls == []


def test_page_control_safe_activation_marker_preserves_current_pre_effect_replay(
    tmp_path: Path,
) -> None:
    outbox_path = tmp_path / "current-marker.sqlite3"
    PageControlOutbox(outbox_path)
    command = SubmitLabCommand(
        command_id="current-marker-submit",
        requested_at=NOW,
        command=PauseJobCommand(job_id=UUID(int=1), expected_version=4, reason="page"),
        interaction_key="pause:1:4",
    )
    connection = sqlite3.connect(outbox_path)
    try:
        _insert_processing_command(
            connection,
            command,
            lease_expires_at=NOW - timedelta(seconds=1),
        )
        connection.commit()
    finally:
        connection.close()
    backend = _LabControlBackendSpy(tmp_path / "exports" / "result.zip")

    receipt = _service_for(
        outbox=PageControlOutbox(outbox_path),
        tmp_path=tmp_path,
        lab_backend=backend,
        now=NOW + timedelta(seconds=2),
    ).submit(command)

    assert receipt.status is PageControlStatus.SUCCEEDED
    assert [name for name, _payload in backend.calls] == ["submit"]


def test_page_control_stale_lab_owner_is_fenced_after_lease_reclaim(
    tmp_path: Path,
) -> None:
    outbox_path = tmp_path / "control.sqlite3"
    outbox = PageControlOutbox(outbox_path)
    job_id = UUID(int=1)
    command = SubmitLabCommand(
        command_id="stale-owner-lab",
        requested_at=NOW,
        command=PauseJobCommand(job_id=job_id, expected_version=4, reason="page"),
        interaction_key="pause:1:4",
    )
    reentered = False
    reclaim_receipts: tuple[PageControlReceipt, ...] | None = None

    class ReentrantBackend:
        calls = 0

        def submit_command(self, command: object, *, interaction_key: str | None):
            nonlocal reclaim_receipts, reentered
            self.calls += 1
            if not reentered:
                reentered = True
                reclaim_receipts = PageControlConsumer(
                    outbox=PageControlOutbox(outbox_path),
                    data_dir=tmp_path / "data",
                    log_dir=tmp_path / "logs",
                    lab_backend=self,
                    clock=lambda: NOW + timedelta(seconds=2),
                    lease_seconds=1,
                    consumer_id="new-owner",
                ).drain(limit=1)
            return {"result": "accepted"}

        def export_zip(self, job_id: UUID):
            raise AssertionError("not used")

        def discard_zip(self, command: DiscardLabArtifactZip):
            raise AssertionError("not used")

    backend = ReentrantBackend()

    receipt = PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=tmp_path / "data",
            log_dir=tmp_path / "logs",
            lab_backend=backend,
            clock=lambda: NOW,
            lease_seconds=1,
            consumer_id="old-owner",
        ),
    ).submit(command)

    assert receipt.status is PageControlStatus.SUCCEEDED
    assert reclaim_receipts == ()
    assert backend.calls == 1


def test_page_control_rejects_paths_outside_consumer_roots(tmp_path: Path) -> None:
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")
    consumer = PageControlConsumer(
        outbox=outbox,
        data_dir=tmp_path / "data",
        log_dir=tmp_path / "logs",
        allowed_lab_export_roots=(tmp_path / "exports",),
    )
    outbox.enqueue(
        InitializeLabExports(
            command_id="escape",
            requested_at=NOW,
            export_root=tmp_path / "outside",
            runtime_root=tmp_path / "exports",
        )
    )

    receipt = consumer.drain(limit=1)[0]

    assert receipt.status == "failed"
    assert "allowlisted" in (receipt.error or "")
    assert not (tmp_path / "outside").exists()


def test_page_control_rejects_canvas_catalog_directory_symlink(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    outside = tmp_path / "outside"
    outside.mkdir()
    (data_dir).mkdir()
    (data_dir / "canvases").symlink_to(outside, target_is_directory=True)
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")
    service = PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=data_dir,
            log_dir=tmp_path / "logs",
        ),
    )

    receipt = service.submit(SaveCanvas(command_id="symlink-dir", requested_at=NOW, name="alpha"))

    assert receipt.status == "failed"
    assert "symlink" in (receipt.error or "")
    assert not (outside / "alpha.json").exists()


def test_page_control_rejects_canvas_head_parent_symlink_without_external_write(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    outside = tmp_path / "outside-heads"
    data_dir.mkdir()
    outside.mkdir()
    (data_dir / "canvas-publication-heads").symlink_to(
        outside,
        target_is_directory=True,
    )
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")

    receipt = _service_for(
        outbox=outbox,
        tmp_path=tmp_path,
        data_dir=data_dir,
    ).submit(
        SaveCanvas(
            command_id="head-parent-symlink",
            requested_at=NOW,
            name="alpha",
        )
    )

    assert receipt.status is PageControlStatus.FAILED
    assert "symlink" in (receipt.error or "")
    assert tuple(outside.iterdir()) == ()


def test_page_control_rejects_command_beyond_server_future_clock_skew(
    tmp_path: Path,
) -> None:
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")

    receipt = _service_for(
        outbox=outbox,
        tmp_path=tmp_path,
        now=NOW,
    ).submit(
        DeleteCanvas(
            command_id="future-delete",
            requested_at=NOW + timedelta(days=3650),
            name="alpha",
        )
    )

    assert receipt.status is PageControlStatus.FAILED
    assert "future" in (receipt.error or "")


def test_page_control_canvas_writes_remain_bound_when_fenced_roots_rotate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = tmp_path / "data"
    canvas_dir = data_dir / "canvases"
    head_root = data_dir / "canvas-publication-heads"
    hidden_canvas_dir = tmp_path / "hidden-canvases"
    hidden_head_root = tmp_path / "hidden-heads"
    authority = _canvas_authority(tmp_path)
    original_issue = authority.signer.issue_publication
    rotated = False

    def rotate_after_fence(claims: object):
        nonlocal rotated
        publication = original_issue(claims)
        if not rotated:
            rotated = True
            canvas_dir.rename(hidden_canvas_dir)
            canvas_dir.mkdir(mode=0o700)
            head_root.rename(hidden_head_root)
            head_root.mkdir(mode=0o700)
        return publication

    monkeypatch.setattr(authority.signer, "issue_publication", rotate_after_fence)
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")

    receipt = _service_for(
        outbox=outbox,
        tmp_path=tmp_path,
        data_dir=data_dir,
    ).submit(
        SaveCanvas(
            command_id="rotate-after-fence",
            requested_at=NOW,
            name="alpha",
        )
    )

    assert rotated is True
    assert receipt.status is not PageControlStatus.SUCCEEDED
    assert not (canvas_dir / "alpha.json").exists()
    assert not (head_root / "alpha").exists()


def test_page_control_rejects_existing_canvas_symlink_before_reading_it(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    canvas_dir = data_dir / "canvases"
    canvas_dir.mkdir(parents=True)
    outside = tmp_path / "outside.json"
    outside.write_text('{"created_at":"1999-01-01T00:00:00Z"}', encoding="utf-8")
    (canvas_dir / "alpha.json").symlink_to(outside)
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")
    service = PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=data_dir,
            log_dir=tmp_path / "logs",
        ),
    )

    receipt = service.submit(SaveCanvas(command_id="symlink-file", requested_at=NOW, name="alpha"))

    assert receipt.status == "failed"
    assert "symlink" in (receipt.error or "")
    assert (canvas_dir / "alpha.json").is_symlink()
    assert outside.read_text(encoding="utf-8") == '{"created_at":"1999-01-01T00:00:00Z"}'


def test_page_control_rejects_nl_log_symlink_before_append(tmp_path: Path) -> None:
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    outside = tmp_path / "outside.jsonl"
    outside.write_text("outside\n", encoding="utf-8")
    (log_dir / "nl_queries.jsonl").symlink_to(outside)
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")
    service = _service_for(
        outbox=outbox,
        tmp_path=tmp_path,
        log_dir=log_dir,
    )

    receipt = service.submit(
        AppendNlQueryLog(
            command_id="nl-log-symlink",
            requested_at=NOW,
            query="价格大于10",
            outcome="success",
        )
    )

    assert receipt.status == "failed"
    assert "symlink" in (receipt.error or "")
    assert outside.read_text(encoding="utf-8") == "outside\n"


def test_page_control_rejects_nl_log_replaced_during_append(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    log_path = log_dir / "nl_queries.jsonl"
    log_path.write_text("", encoding="utf-8")
    outside = tmp_path / "outside.jsonl"
    outside.write_text("outside\n", encoding="utf-8")
    original_write = page_control.os.write
    replaced = False

    def replace_log_once(file_descriptor: int, payload: bytes) -> int:
        nonlocal replaced
        if not replaced:
            replaced = True
            log_path.unlink()
            log_path.symlink_to(outside)
        return original_write(file_descriptor, payload)

    monkeypatch.setattr(page_control.os, "write", replace_log_once)
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")
    service = _service_for(
        outbox=outbox,
        tmp_path=tmp_path,
        log_dir=log_dir,
    )

    receipt = service.submit(
        AppendNlQueryLog(
            command_id="nl-log-race",
            requested_at=NOW,
            query="价格大于10",
            outcome="success",
        )
    )

    assert receipt.status == "failed"
    assert "changed" in (receipt.error or "") or "symlink" in (receipt.error or "")
    assert outside.read_text(encoding="utf-8") == "outside\n"


def test_page_control_rejects_lab_export_root_symlink(tmp_path: Path) -> None:
    export_root = tmp_path / "exports"
    runtime_root = tmp_path / "runtime"
    outside = tmp_path / "outside"
    outside.mkdir()
    export_root.symlink_to(outside, target_is_directory=True)
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")
    service = _service_for(
        outbox=outbox,
        tmp_path=tmp_path,
        allowed_lab_export_roots=(export_root, runtime_root),
    )

    receipt = service.submit(
        InitializeLabExports(
            command_id="exports-symlink",
            requested_at=NOW,
            export_root=export_root,
            runtime_root=runtime_root,
        )
    )

    assert receipt.status == "failed"
    assert "symlink" in (receipt.error or "")


def test_page_control_rejects_lab_export_root_replaced_during_creation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    export_root = tmp_path / "exports"
    runtime_root = tmp_path / "runtime"
    outside = tmp_path / "outside"
    outside.mkdir()
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")
    service = _service_for(
        outbox=outbox,
        tmp_path=tmp_path,
        allowed_lab_export_roots=(export_root, runtime_root),
    )
    original_mkdir = Path.mkdir
    replaced = False

    def replace_after_mkdir(self: Path, *args: object, **kwargs: object) -> None:
        nonlocal replaced
        original_mkdir(self, *args, **kwargs)
        if self == export_root and not replaced:
            replaced = True
            self.rmdir()
            self.symlink_to(outside, target_is_directory=True)

    monkeypatch.setattr(Path, "mkdir", replace_after_mkdir)

    receipt = service.submit(
        InitializeLabExports(
            command_id="exports-race",
            requested_at=NOW,
            export_root=export_root,
            runtime_root=runtime_root,
        )
    )

    assert receipt.status == "failed"
    assert "changed" in (receipt.error or "") or "symlink" in (receipt.error or "")


def test_page_control_delete_is_executed_only_by_consumer(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    canvas_path = data_dir / "canvases" / "alpha.json"
    authority = _canvas_authority(tmp_path)
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")
    setup_receipt = _service_for(
        outbox=outbox,
        tmp_path=tmp_path,
        data_dir=data_dir,
    ).submit(
        SaveCanvas(
            command_id="delete-only-setup",
            requested_at=NOW - timedelta(minutes=1),
            name="alpha",
        )
    )
    assert setup_receipt.status is PageControlStatus.SUCCEEDED
    outbox.enqueue(DeleteCanvas(command_id="delete", requested_at=NOW, name="alpha"))
    assert canvas_path.exists()

    PageControlConsumer(
        outbox=outbox,
        data_dir=data_dir,
        log_dir=tmp_path / "logs",
        canvas_publication_signer=authority.signer,
        canvas_publication_keyring=authority.keyring,
    ).drain(limit=1)

    assert not canvas_path.exists()


def test_page_control_client_submits_typed_command_to_service_api(tmp_path: Path) -> None:
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")
    authority = _canvas_authority(tmp_path)
    service = PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=tmp_path / "data",
            log_dir=tmp_path / "logs",
            canvas_publication_signer=authority.signer,
            canvas_publication_keyring=authority.keyring,
        ),
    )
    submitted_payloads: list[dict[str, object]] = []

    def submit(payload: dict[str, object]) -> dict[str, object]:
        submitted_payloads.append(payload)
        command = SaveCanvas.model_validate(payload)
        return service.submit(command).model_dump(mode="json")

    client = PageControlClient(transport=submit)
    receipt = client.submit(
        SaveCanvas(
            command_id="client",
            requested_at=NOW,
            name="alpha",
            description="through API",
        )
    )

    assert submitted_payloads[0]["kind"] == "save_canvas"
    assert receipt.status == "succeeded"
    assert (tmp_path / "data" / "canvases" / "alpha.json").is_file()


def test_page_control_client_turns_transport_failure_into_typed_unavailable() -> None:
    def unavailable(_payload: dict[str, object]) -> dict[str, object]:
        raise OSError("connection refused")

    client = PageControlClient(transport=unavailable, timeout_seconds=0.1)
    with pytest.raises(PageControlUnavailableError, match="unavailable"):
        client.submit(SaveCanvas(command_id="down", requested_at=NOW, name="alpha"))


def test_page_control_consumer_exclusively_owns_lab_command_and_zip_mutations(
    tmp_path: Path,
) -> None:
    export_path = tmp_path / "exports" / "result.zip"
    export_path.parent.mkdir()
    backend = _LabControlBackendSpy(export_path)
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")
    consumer = PageControlConsumer(
        outbox=outbox,
        data_dir=tmp_path / "data",
        log_dir=tmp_path / "logs",
        lab_backend=backend,
    )
    job_id = UUID(int=1)
    submit = SubmitLabCommand(
        command_id="lab-submit",
        requested_at=NOW,
        command=PauseJobCommand(
            job_id=job_id,
            expected_version=4,
            reason="page",
        ),
        interaction_key="pause:1:4",
    )
    export = ExportLabArtifactZip(
        command_id="lab-export",
        requested_at=NOW,
        job_id=job_id,
    )
    outbox.enqueue(submit)
    outbox.enqueue(export)

    receipts = {receipt.command_id: receipt for receipt in consumer.drain(limit=2)}
    submitted = receipts["lab-submit"]
    exported = receipts["lab-export"]

    assert submitted.result == {"result": "accepted"}
    assert exported.result is not None
    assert exported.result["path"] == str(export_path)
    outbox.enqueue(
        DiscardLabArtifactZip(
            command_id="lab-discard",
            requested_at=NOW,
            request_id=UUID(str(exported.result["request_id"])),
            job_id=job_id,
            path=export_path,
            byte_size=int(exported.result["byte_size"]),
            sha256=str(exported.result["sha256"]),
        )
    )

    discarded = consumer.drain(limit=1)[0]

    assert discarded.result == {"discarded": True}
    assert not export_path.exists()
    assert sorted(name for name, _ in backend.calls) == ["discard", "export", "submit"]
