from __future__ import annotations

import hashlib
import hmac
import json
import shutil
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

from rquant.runtime_recovery_artifacts import (
    RealRecoveryArtifactKind,
    RealRecoveryArtifactSpec,
    RealRecoveryTargetManifest,
    RecoveryToolVerifierBundle,
)
from rquant.runtime_recovery_backup import (
    RecoveryBackupAuthenticator,
    RecoveryBackupConfig,
    RecoveryBackupIntegrityError,
    RecoveryBackupProducer,
    RecoveryBackupReceipt,
    RecoveryBackupSigner,
    load_recovery_backup_generation,
)
from rquant.runtime_recovery_coordinator import RuntimeRecoveryFixedReplayExpectation
from rquant.strict_json import strict_canonical_json_loads
from tests.unit.test_runtime_recovery_artifacts import _build_bundle
from tests.unit.test_runtime_recovery_coordinator import (
    COMMIT_A,
    COMMIT_B,
    DEPLOYMENT_PROFILE_GENERATION,
    _strategy_bindings,
)


class _HmacSigner(RecoveryBackupSigner):
    key_id = "recovery-backup-test-key"

    def sign(self, payload: bytes) -> str:
        return hmac.new(b"runtime-recovery-backup-secret", payload, hashlib.sha256).hexdigest()

    def verify(self, payload: bytes, signature: str) -> bool:
        return hmac.compare_digest(self.sign(payload), signature)


def _config(tmp_path: Path) -> RecoveryBackupConfig:
    source_root, target, _tool, _replay = _build_bundle(
        tmp_path,
        fixture_value=3,
        formal_replay=True,
    )
    artifacts = tuple(
        RealRecoveryArtifactSpec(
            logical_role=artifact.logical_role,
            kind=artifact.kind,
            source_path=artifact.source_path,
            restore_path=artifact.restore_path,
            generation_id=artifact.generation_id,
            schema_version=artifact.schema_version,
            available_at=artifact.available_at,
            price_basis=artifact.price_basis,
            relations=tuple(item.relation_name for item in artifact.relations),
            references=artifact.references,
        )
        for artifact in target.artifacts
    )
    return RecoveryBackupConfig(
        source_root=source_root,
        publication_root=tmp_path / "recovery-backups",
        target_commit=COMMIT_B,
        target_profile_generation=DEPLOYMENT_PROFILE_GENERATION,
        verifier_commit=COMMIT_A,
        signer_key_id=_HmacSigner.key_id,
        as_of=target.as_of,
        replay_start_date=date(2026, 6, 24),
        replay_end_date=date(2026, 6, 24),
        production_artifact_role="production",
        paper_ledger_artifact_role="paper_ledger",
        strategy_bindings=_strategy_bindings(),
        artifacts=artifacts,
        deadline_seconds=60,
        max_total_bytes=256 * 1024 * 1024,
    )


def _canonical(path: Path) -> dict[str, object]:
    decoded = strict_canonical_json_loads(path.read_bytes())
    assert isinstance(decoded, dict)
    return decoded


def test_backup_producer_checkpoints_real_databases_and_atomically_publishes_bundle(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    producer = RecoveryBackupProducer(config=config, signer=_HmacSigner())

    preview = producer.preview()
    receipt = producer.execute(expected_plan_id=preview.plan_id)

    assert isinstance(receipt, RecoveryBackupReceipt)
    assert receipt.status == "succeeded"
    current = _canonical(Path(config.publication_root) / "current.json")
    generation = Path(config.publication_root) / str(current["generation_path"])
    assert current["generation_id"] == receipt.manifest_id
    assert not (generation / "artifacts/production.duckdb.wal").exists()
    assert not (generation / "artifacts/paper.sqlite3-wal").exists()
    assert not (generation / "artifacts/paper.sqlite3-shm").exists()

    target = RealRecoveryTargetManifest.model_validate_json(
        (generation / "recovery-target.json").read_bytes()
    )
    tool = RecoveryToolVerifierBundle.model_validate_json(
        (generation / "recovery-tool.json").read_bytes()
    )
    expectations = tuple(
        RuntimeRecoveryFixedReplayExpectation.model_validate(item)
        for item in _canonical(generation / "fixed-replay-expectations.json")["expectations"]
    )
    assert len(expectations) == 3
    assert tool.target_manifest_id == target.manifest_id == receipt.manifest_id
    assert target.external_attestations["paper_ledger"] == receipt.paper_ledger_head.head_id
    assert receipt.paper_ledger_head.revision >= 1
    assert (Path(config.publication_root) / "receipts" / f"{receipt.receipt_id}.json").is_file()


def test_backup_config_rejects_incomplete_production_role_inventory(tmp_path: Path) -> None:
    incomplete = _config(tmp_path)
    payload = incomplete.model_dump(mode="python", exclude={"config_id"})
    payload["artifacts"] = tuple(
        artifact
        for artifact in incomplete.artifacts
        if artifact.logical_role
        in {
            incomplete.production_artifact_role,
            incomplete.paper_ledger_artifact_role,
        }
    )

    with pytest.raises(ValueError, match="complete|role|inventory"):
        RecoveryBackupConfig.model_validate(payload)


def test_backup_config_rejects_unbound_required_role_graph(tmp_path: Path) -> None:
    incomplete = _config(tmp_path)
    payload = incomplete.model_dump(mode="python", exclude={"config_id"})
    payload["artifacts"] = tuple(
        artifact.model_copy(update={"references": {}})
        if artifact.kind is RealRecoveryArtifactKind.RESEARCH_LAKE_MANIFEST
        else artifact
        for artifact in incomplete.artifacts
    )

    with pytest.raises(ValueError, match="complete|role|inventory|reference"):
        RecoveryBackupConfig.model_validate(payload)


@pytest.mark.parametrize("source_name", ["production.duckdb", "paper.sqlite3"])
def test_backup_execute_rejects_source_path_swap_after_preview(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    source_name: str,
) -> None:
    config = _config(tmp_path)
    producer = RecoveryBackupProducer(config=config, signer=_HmacSigner())
    preview = producer.preview()
    original = producer._snapshot_artifacts
    source = Path(config.source_root) / source_name

    def swap_then_snapshot(candidate: Path, *, guard: object) -> int:
        moved = source.with_name(f"{source.name}.replaced")
        source.rename(moved)
        shutil.copy2(moved, source)
        return original(candidate, guard=guard)

    monkeypatch.setattr(producer, "_snapshot_artifacts", swap_then_snapshot)

    with pytest.raises(RecoveryBackupIntegrityError, match="identity|swap|source"):
        producer.execute(expected_plan_id=preview.plan_id)

    assert not (Path(config.publication_root) / "current.json").exists()


@pytest.mark.parametrize(
    ("source_name", "mutation"),
    (
        (
            "production.duckdb",
            "CREATE TABLE recovery_preview_drift(value INTEGER)",
        ),
        (
            "catalog/research.duckdb",
            "CREATE TABLE recovery_preview_drift(value INTEGER)",
        ),
    ),
)
def test_backup_execute_rejects_old_plan_after_duckdb_content_changes(
    tmp_path: Path,
    source_name: str,
    mutation: str,
) -> None:
    config = _config(tmp_path)
    producer = RecoveryBackupProducer(config=config, signer=_HmacSigner())
    preview = producer.preview()
    connection = duckdb.connect(str(Path(config.source_root) / source_name))
    try:
        connection.execute(mutation)
        connection.execute("CHECKPOINT")
    finally:
        connection.close()

    with pytest.raises(RecoveryBackupIntegrityError, match="plan|content|changed"):
        producer.execute(expected_plan_id=preview.plan_id)

    assert not (Path(config.publication_root) / "current.json").exists()


@pytest.mark.parametrize("source_name", ("production.duckdb", "catalog/research.duckdb"))
def test_backup_snapshot_rejects_duckdb_drift_after_apply_repreview(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    source_name: str,
) -> None:
    import rquant.runtime_recovery_backup as backup_module

    config = _config(tmp_path)
    producer = RecoveryBackupProducer(config=config, signer=_HmacSigner())
    preview = producer.preview()
    original = backup_module._snapshot_duckdb
    drifted = False

    def drift_then_snapshot(
        source: Path,
        destination: Path,
        *,
        kind: RealRecoveryArtifactKind,
        max_bytes: int,
        check: object = None,
        remaining_seconds: object = None,
    ) -> int:
        nonlocal drifted
        if not drifted and source == Path(config.source_root) / source_name:
            drifted = True
            connection = duckdb.connect(str(source))
            try:
                connection.execute("CREATE TABLE recovery_apply_drift(value INTEGER)")
                connection.execute("CHECKPOINT")
            finally:
                connection.close()
        return original(
            source,
            destination,
            kind=kind,
            max_bytes=max_bytes,
            check=check,
            remaining_seconds=remaining_seconds,
        )

    monkeypatch.setattr(backup_module, "_snapshot_duckdb", drift_then_snapshot)

    with pytest.raises(RecoveryBackupIntegrityError, match="identity|content|changed|plan"):
        producer.execute(expected_plan_id=preview.plan_id)

    assert drifted is True
    assert not (Path(config.publication_root) / "current.json").exists()


def test_backup_rejects_same_profile_paper_ledger_rollback_and_keeps_current(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    producer = RecoveryBackupProducer(config=config, signer=_HmacSigner())
    first = producer.execute(expected_plan_id=producer.preview().plan_id)
    previous = (Path(config.publication_root) / "current.json").read_bytes()
    current = _canonical(Path(config.publication_root) / "current.json")
    authority = (
        Path(config.publication_root)
        / str(current["generation_path"])
        / "paper-ledger-authority.json"
    )
    head = _canonical(authority)
    head["revision"] = int(head["revision"]) + 1
    head.pop("head_id", None)
    authority.chmod(0o600)
    authority.write_text(json.dumps(head, separators=(",", ":"), sort_keys=True))

    second = RecoveryBackupProducer(config=config, signer=_HmacSigner())
    try:
        second.execute(expected_plan_id=second.preview().plan_id)
    except Exception as exc:
        assert "rollback" in str(exc) or "lineage" in str(exc)
    else:
        raise AssertionError("paper ledger rollback was accepted")

    assert (Path(config.publication_root) / "current.json").read_bytes() == previous
    assert first.status == "succeeded"


def test_backup_deadline_failure_preserves_previous_generation(tmp_path: Path) -> None:
    config = _config(tmp_path)
    producer = RecoveryBackupProducer(config=config, signer=_HmacSigner())
    first = producer.execute(expected_plan_id=producer.preview().plan_id)
    previous = (Path(config.publication_root) / "current.json").read_bytes()
    expired = config.model_copy(
        update={
            "deadline_seconds": 1,
            "as_of": config.as_of + timedelta(seconds=1),
        }
    )
    readings = iter((0.0, 2.0, 2.0))
    failing = RecoveryBackupProducer(
        config=expired,
        signer=_HmacSigner(),
        monotonic=lambda: next(readings, 2.0),
    )

    try:
        failing.execute(expected_plan_id=failing.preview().plan_id)
    except Exception as exc:
        assert "deadline" in str(exc)
    else:
        raise AssertionError("expired backup completed")

    assert (Path(config.publication_root) / "current.json").read_bytes() == previous
    assert first.status == "succeeded"


def test_current_generation_loader_binds_external_paper_head(tmp_path: Path) -> None:
    config = _config(tmp_path)
    producer = RecoveryBackupProducer(config=config, signer=_HmacSigner())
    producer.execute(expected_plan_id=producer.preview().plan_id)
    current = _canonical(Path(config.publication_root) / "current.json")
    authority = (
        Path(config.publication_root)
        / str(current["generation_path"])
        / "paper-ledger-authority.json"
    )
    authority.chmod(0o600)
    authority.write_bytes(b"{}")

    try:
        load_recovery_backup_generation(
            Path(config.publication_root),
            trusted_verifiers={_HmacSigner.key_id: _HmacSigner()},
        )
    except RecoveryBackupIntegrityError as exc:
        assert "external" in str(exc) or "authority" in str(exc)
    else:
        raise AssertionError("detached external paper ledger head was accepted")


@pytest.mark.parametrize("document_name", ("current.json", "recovery-backup-receipt.json"))
def test_backup_loader_rejects_rehashed_timing_tamper_without_valid_signature(
    tmp_path: Path,
    document_name: str,
) -> None:
    config = _config(tmp_path)
    signer = _HmacSigner()
    producer = RecoveryBackupProducer(config=config, signer=signer)
    receipt = producer.execute(expected_plan_id=producer.preview().plan_id)
    root = Path(config.publication_root)
    current = _canonical(root / "current.json")
    path = (
        root / "current.json"
        if document_name == "current.json"
        else root / str(current["generation_path"]) / document_name
    )
    payload = _canonical(path)
    path.chmod(0o600)
    if document_name == "current.json":
        payload["published_at"] = (
            datetime(2030, 1, 1, tzinfo=UTC).isoformat().replace("+00:00", "Z")
        )
    else:
        payload["duration_ms"] = int(payload["duration_ms"]) + 1
        payload["completed_at"] = (
            datetime(2030, 1, 1, tzinfo=UTC).isoformat().replace("+00:00", "Z")
        )
        payload = RecoveryBackupReceipt.model_validate({**payload, "receipt_id": None}).model_dump(
            mode="json"
        )
    path.write_text(json.dumps(payload, separators=(",", ":"), sort_keys=True), encoding="ascii")

    with pytest.raises(RecoveryBackupIntegrityError, match="signature|trusted"):
        load_recovery_backup_generation(
            root,
            trusted_verifiers={signer.key_id: signer},
        )

    assert receipt.status == "succeeded"


def test_backup_loader_accepts_rotated_trusted_key_set_and_rejects_unknown_key(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    old = _HmacSigner()
    producer = RecoveryBackupProducer(config=config, signer=old)
    producer.execute(expected_plan_id=producer.preview().plan_id)

    class NewSigner(_HmacSigner):
        key_id = "recovery-backup-next-key"

    loaded = load_recovery_backup_generation(
        Path(config.publication_root),
        trusted_verifiers={old.key_id: old, NewSigner.key_id: NewSigner()},
    )
    assert loaded[0].key_id == old.key_id
    with pytest.raises(RecoveryBackupIntegrityError, match="trusted|key|signature"):
        load_recovery_backup_generation(
            Path(config.publication_root),
            trusted_verifiers={NewSigner.key_id: NewSigner()},
        )


def test_backup_publication_crash_before_current_keeps_previous_bundle_loadable(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    signer = _HmacSigner()
    first = RecoveryBackupProducer(config=config, signer=signer)
    first_receipt = first.execute(expected_plan_id=first.preview().plan_id)
    root = Path(config.publication_root)
    previous = (root / "current.json").read_bytes()
    next_config = config.model_copy(update={"as_of": config.as_of + timedelta(seconds=1)})

    def crash(stage: str) -> None:
        if stage == "after_publication_intent":
            raise RuntimeError("simulated publication crash")

    second = RecoveryBackupProducer(config=next_config, signer=signer, fault_hook=crash)
    with pytest.raises(RuntimeError, match="publication crash"):
        second.execute(expected_plan_id=second.preview().plan_id)

    assert (root / "current.json").read_bytes() == previous
    pointer, receipt, *_ = load_recovery_backup_generation(
        root,
        trusted_verifiers={signer.key_id: signer},
    )
    assert pointer.manifest_id == receipt.manifest_id == first_receipt.manifest_id


def test_backup_publication_crash_after_current_loads_complete_new_bundle_and_converges(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    signer = _HmacSigner()
    first = RecoveryBackupProducer(config=config, signer=signer)
    first_receipt = first.execute(expected_plan_id=first.preview().plan_id)
    next_config = config.model_copy(update={"as_of": config.as_of + timedelta(seconds=1)})

    def crash(stage: str) -> None:
        if stage == "after_current":
            raise KeyboardInterrupt("simulated hard crash after current")

    second = RecoveryBackupProducer(config=next_config, signer=signer, fault_hook=crash)
    with pytest.raises(KeyboardInterrupt, match="hard crash"):
        second.execute(expected_plan_id=second.preview().plan_id)

    root = Path(config.publication_root)
    pointer, receipt, *_ = load_recovery_backup_generation(
        root,
        trusted_verifiers={signer.key_id: signer},
    )
    assert pointer.manifest_id == receipt.manifest_id
    assert pointer.manifest_id != first_receipt.manifest_id
    assert (root / ".publication-intent.json").is_file()

    recovered = RecoveryBackupProducer(config=next_config, signer=signer)
    recovered._prepare_layout()
    with recovered._lock():
        recovered._recover_interrupted_publication()
    assert not (root / ".publication-intent.json").exists()
    assert _canonical(recovered.authority_path)["head_id"] == receipt.paper_ledger_head.head_id


def test_private_credential_files_form_rotation_trusted_set(tmp_path: Path) -> None:
    from rquant.runtime_recovery_backup import load_recovery_backup_trusted_verifiers

    paths: list[Path] = []
    for key_id, byte in (("old", "ab"), ("active", "cd")):
        path = tmp_path / f"{key_id}.json"
        path.write_text(
            json.dumps(
                {"key_id": key_id, "secret_hex": byte * 32},
                separators=(",", ":"),
                sort_keys=True,
            ),
            encoding="ascii",
        )
        path.chmod(0o600)
        paths.append(path)

    trusted = load_recovery_backup_trusted_verifiers(tuple(paths))

    assert set(trusted) == {"old", "active"}
    assert trusted["old"].verify(b"payload", trusted["old"].sign(b"payload"))


def test_chunk_copy_honors_absolute_deadline_before_finishing(tmp_path: Path) -> None:
    import rquant.runtime_recovery_backup as backup_module

    source = tmp_path / "source.bin"
    destination = tmp_path / "destination.bin"
    source.write_bytes(b"x" * (3 * 1024 * 1024))
    checks = 0

    def check() -> None:
        nonlocal checks
        checks += 1
        if checks >= 2:
            raise RecoveryBackupIntegrityError("recovery backup deadline exceeded")

    with pytest.raises(RecoveryBackupIntegrityError, match="deadline"):
        backup_module._copy_regular(
            source,
            destination,
            max_bytes=source.stat().st_size,
            check=check,
        )

    assert checks == 2


def test_sqlite_snapshot_and_duckdb_checkpoint_honor_deep_deadline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import time

    import rquant.runtime_recovery_backup as backup_module

    sqlite_source = tmp_path / "source.sqlite3"
    connection = __import__("sqlite3").connect(sqlite_source)
    try:
        connection.execute("CREATE TABLE payload(value BLOB)")
        connection.executemany(
            "INSERT INTO payload VALUES (?)",
            ((b"x" * 4096,) for _ in range(1024)),
        )
        connection.commit()
    finally:
        connection.close()
    checks = 0

    def sqlite_check() -> None:
        nonlocal checks
        checks += 1
        if checks >= 2:
            raise RecoveryBackupIntegrityError("recovery backup deadline exceeded")

    with pytest.raises(RecoveryBackupIntegrityError, match="deadline"):
        backup_module._snapshot_sqlite(
            sqlite_source,
            tmp_path / "snapshot.sqlite3",
            check=sqlite_check,
        )

    duck_source = tmp_path / "source.duckdb"
    duck_source.write_bytes(b"duckdb-fixture")
    interrupted = False

    class BlockingConnection:
        def execute(self, _statement: str) -> None:
            time.sleep(0.05)

        def interrupt(self) -> None:
            nonlocal interrupted
            interrupted = True

        def close(self) -> None:
            return None

    monkeypatch.setattr(backup_module.duckdb, "connect", lambda _path: BlockingConnection())
    started = time.monotonic()

    def duck_check() -> None:
        if time.monotonic() - started > 0.01:
            raise RecoveryBackupIntegrityError("recovery backup deadline exceeded")

    with pytest.raises(RecoveryBackupIntegrityError, match="deadline"):
        backup_module._snapshot_duckdb(
            duck_source,
            tmp_path / "snapshot.duckdb",
            kind=RealRecoveryArtifactKind.PRODUCTION_DUCKDB,
            max_bytes=1024,
            check=duck_check,
            remaining_seconds=lambda: 0.01,
        )
    assert interrupted is True


def test_source_lease_constructor_closes_descriptor_on_unexpected_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.runtime_recovery_backup as backup_module

    config = _config(tmp_path)
    artifact = config.artifacts[0]
    real_fstat = backup_module.os.fstat
    real_close = backup_module.os.close
    opened: list[int] = []
    closed: list[int] = []

    def explode(descriptor: int):
        opened.append(descriptor)
        raise RuntimeError("fstat exploded")

    def record_close(descriptor: int) -> None:
        closed.append(descriptor)
        real_close(descriptor)

    monkeypatch.setattr(backup_module.os, "fstat", explode)
    monkeypatch.setattr(backup_module.os, "close", record_close)
    with pytest.raises(RuntimeError, match="fstat exploded"):
        backup_module._RecoverySourceLease(root=Path(config.source_root), artifact=artifact)
    monkeypatch.setattr(backup_module.os, "fstat", real_fstat)

    assert opened and closed == opened


def test_recovery_authenticator_loads_private_canonical_credential(tmp_path: Path) -> None:
    credential = tmp_path / "recovery-credential.json"
    credential.write_text(
        json.dumps(
            {"key_id": "production-recovery-v1", "secret_hex": "ab" * 32},
            separators=(",", ":"),
            sort_keys=True,
        ),
        encoding="ascii",
    )
    credential.chmod(0o600)

    authenticator = RecoveryBackupAuthenticator.from_file(credential)
    signature = authenticator.sign(b"sealed verifier bundle")

    assert authenticator.key_id == "production-recovery-v1"
    assert authenticator.verify(b"sealed verifier bundle", signature) is True
    assert authenticator.verify(b"tampered", signature) is False

    credential.chmod(0o644)
    with pytest.raises(RecoveryBackupIntegrityError, match="credential"):
        RecoveryBackupAuthenticator.from_file(credential)
