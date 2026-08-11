from __future__ import annotations

import gc
import hashlib
import hmac
import json
import multiprocessing
import os
import resource
import shutil
import sqlite3
import sys
import time
import tracemalloc
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from multiprocessing.connection import Connection
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from rquant.lab_worker import (
    LabShardArtifactManifest,
    LabShardResultManifest,
    canonical_shard_frame_digest,
)
from rquant.paper_broker import BrokerCostPolicy, PaperBrokerStore
from rquant.reference_data_registry import (
    ReferenceDataset,
    ReferenceRecord,
    ReferenceRegistry,
)
from rquant.research_catalog import ResearchCatalog
from rquant.research_lake import export_research_dataset
from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_recovery_artifacts import (
    FixedReplayReceipt,
    FixedReplayVerifier,
    RealRecoveryArtifactKind,
    RealRecoveryArtifactSpec,
    RealRecoveryIntegrityError,
    RealRecoveryRestorer,
    RealRecoveryTargetManifest,
    RecoveryToolVerifierBundle,
    RecoveryVerificationBudget,
    build_real_recovery_target,
    load_full_verified_current_recovery_receipt,
    load_verified_real_recovery_receipt,
    seal_recovery_tool_bundle,
)
from rquant.runtime_recovery_coordinator import (
    RuntimeRecoveryFixedReplayExpectation,
    RuntimeRecoveryFixedReplayVerifier,
)
from rquant.serving_contracts import (
    FreshnessStatus,
    ServingDatasetWatermark,
)
from rquant.serving_publisher import ServingPublisher, ServingTableSpec
from rquant.strict_json import canonical_json_bytes

COMMIT_A = "a" * 40
COMMIT_B = "b" * 40
PROFILE_GENERATION = canonical_sha256({"profile": "restore-target-v1"})
AS_OF = datetime(2026, 7, 31, 9, 0, tzinfo=UTC)


class _HmacSigner:
    key_id = "recovery-test-key"

    def __init__(self, secret: bytes) -> None:
        self.secret = secret

    def sign(self, payload: bytes) -> str:
        return hmac.new(self.secret, payload, hashlib.sha256).hexdigest()


class _HmacVerifier:
    key_id = "recovery-test-key"

    def __init__(self, secret: bytes) -> None:
        self.secret = secret

    def verify(self, payload: bytes, signature: str) -> bool:
        expected = hmac.new(self.secret, payload, hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected, signature)


class _FixedReplayVerifier:
    fingerprint = canonical_sha256({"fixed-replay": "real-mini-v1"})

    def verify(self, *, target_root: Path, dataset_path: Path) -> tuple[FixedReplayReceipt, ...]:
        connection = duckdb.connect(str(dataset_path), read_only=True)
        try:
            value = int(connection.execute("SELECT SUM(value) FROM replay_fixture").fetchone()[0])
        finally:
            connection.close()
        assert dataset_path.is_relative_to(target_root)
        return tuple(
            FixedReplayReceipt(
                strategy_id=strategy_id,
                replay_fingerprint=canonical_sha256(
                    {"strategy": strategy_id, "fixture_sum": value}
                ),
            )
            for strategy_id in ("auction_gap", "growth_board_surge", "n_shape")
        )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _create_production_database(path: Path, *, value: int = 3) -> None:
    connection = duckdb.connect(str(path))
    try:
        connection.execute(
            """
            CREATE TABLE daily_bar(
                ts_code VARCHAR NOT NULL,
                trade_date DATE NOT NULL,
                close DOUBLE NOT NULL,
                PRIMARY KEY(ts_code, trade_date)
            );
            INSERT INTO daily_bar VALUES ('000001.SZ', '2026-07-31', 10.5);
            CREATE TABLE replay_fixture(strategy_id VARCHAR, value INTEGER);
            INSERT INTO replay_fixture VALUES
                ('n_shape', ?), ('growth_board_surge', ?), ('auction_gap', ?);
            """,
            [value, value, value],
        )
        connection.execute("CHECKPOINT")
    finally:
        connection.close()


def _create_state_database(path: Path) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA application_id = 1381057106")
        connection.execute("PRAGMA user_version = 7")
        connection.execute(
            "CREATE TABLE state_event(sequence INTEGER PRIMARY KEY, payload TEXT NOT NULL) STRICT"
        )
        connection.execute('INSERT INTO state_event VALUES (1, \'{"state":"ready"}\')')
        connection.commit()
    finally:
        connection.close()
    path.chmod(0o600)


def _create_blob_database(path: Path, *, size_bytes: int = 8 * 1024 * 1024) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            "CREATE TABLE blob_event(sequence INTEGER PRIMARY KEY, payload BLOB NOT NULL) STRICT"
        )
        connection.execute(
            "INSERT INTO blob_event(sequence, payload) VALUES (1, zeroblob(?))",
            (size_bytes,),
        )
        connection.commit()
    finally:
        connection.close()
    path.chmod(0o600)


def _create_text_database(path: Path, *, size_bytes: int) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            "CREATE TABLE text_event(sequence INTEGER PRIMARY KEY, payload TEXT NOT NULL) STRICT"
        )
        connection.execute(
            "INSERT INTO text_event(sequence, payload) VALUES (1, ?)",
            ("x" * size_bytes,),
        )
        connection.commit()
    finally:
        connection.close()
    path.chmod(0o600)


def _oversized_blob_rss_probe(path: str, result: Connection) -> None:
    import rquant.runtime_recovery_artifacts as artifact_module

    gc.collect()
    unit = 1 if sys.platform == "darwin" else 1024
    before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * unit
    rejected = False
    try:
        artifact_module._sqlite_relation_evidence(
            Path(path),
            ("blob_event",),
            meter=artifact_module._VerificationMeter(
                RecoveryVerificationBudget(
                    max_row_bytes=1024,
                    duckdb_memory_bytes=16 * 1024 * 1024,
                    deadline_seconds=60,
                )
            ),
        )
    except RealRecoveryIntegrityError:
        rejected = True
    gc.collect()
    after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * unit
    result.send((rejected, after - before))
    result.close()


def _create_paper_ledger(path: Path) -> None:
    PaperBrokerStore(
        path,
        account_id="paper-main",
        initial_cash=Decimal("100000"),
        cost_policy=BrokerCostPolicy(
            commission_rate=Decimal("0.0003"),
            minimum_commission=Decimal("5"),
            sell_stamp_tax_rate=Decimal("0.001"),
        ),
    ).require_trusted_ledger()
    sealed = path.with_name("paper-sealed.sqlite3")
    source = sqlite3.connect(path)
    destination = sqlite3.connect(sealed)
    try:
        source.backup(destination)
        destination.commit()
    finally:
        destination.close()
        source.close()
    os.replace(sealed, path)
    for suffix in ("-wal", "-shm"):
        path.with_name(f"{path.name}{suffix}").unlink(missing_ok=True)


def _create_research_lake(root: Path) -> tuple[Path, Path, Path]:
    source_path = root / "research-source.duckdb"
    source = duckdb.connect(str(source_path))
    try:
        source.execute(
            """
            CREATE TABLE trade_calendar(
                exchange VARCHAR NOT NULL,
                cal_date DATE NOT NULL,
                is_open BOOLEAN NOT NULL,
                PRIMARY KEY(exchange, cal_date)
            );
            INSERT INTO trade_calendar VALUES ('SSE', '2026-07-31', TRUE);
            CREATE TABLE minute_bar(
                ts_code VARCHAR NOT NULL,
                trade_time TIMESTAMP NOT NULL,
                freq VARCHAR NOT NULL,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                vol DOUBLE,
                amount DOUBLE,
                source VARCHAR NOT NULL,
                created_at TIMESTAMP NOT NULL,
                PRIMARY KEY(ts_code, trade_time, freq, source)
            );
            INSERT INTO minute_bar VALUES
                ('000001.SZ', '2026-07-31 09:30:00', '1min', 10, 10.2, 9.9,
                 10.1, 1000, 10100, 'tushare', '2026-07-31 15:01:00');
            CREATE TABLE auction_bar(
                ts_code VARCHAR NOT NULL,
                trade_date DATE NOT NULL,
                auction_type VARCHAR NOT NULL,
                price DOUBLE,
                vol DOUBLE,
                amount DOUBLE,
                turnover_rate DOUBLE,
                volume_ratio DOUBLE,
                source VARCHAR NOT NULL,
                created_at TIMESTAMP NOT NULL,
                PRIMARY KEY(ts_code, trade_date, auction_type, source)
            );
            """
        )
        catalog_path = root / "catalog" / "research.duckdb"
        lake_root = root / "lake"
        summary = export_research_dataset(
            source,
            catalog=ResearchCatalog(catalog_path),
            lake_root=lake_root,
            dataset="minute_bar",
            start_date=date(2026, 7, 31),
            end_date=date(2026, 7, 31),
            code_commit=COMMIT_A,
            as_of_date=date(2026, 7, 31),
        )
    finally:
        source.close()
    readonly_path = root / "catalog" / "research_ro.duckdb"
    shutil.copy2(catalog_path, readonly_path)
    manifest_path = next(lake_root.rglob("manifest.json"))
    data_path = lake_root / summary.partitions[0].data_path
    return catalog_path, readonly_path, manifest_path, data_path


def _create_reference_registry(path: Path) -> str:
    registry = ReferenceRegistry(path)
    registry.append(
        ReferenceRecord(
            dataset_id=ReferenceDataset.ADJUSTMENT_FACTOR,
            key="000001.SZ",
            effective_from=datetime(2026, 7, 31, tzinfo=UTC),
            revision=1,
            source="sealed-test",
            first_available_at=datetime(2026, 7, 31, 0, 30, tzinfo=UTC),
            payload={"adj_factor": 1.0, "price_basis": "raw_session"},
        )
    )
    generation = registry.publish(published_at=datetime(2026, 7, 31, 0, 31, tzinfo=UTC))
    return generation.generation_id


def _create_serving(root: Path, *, reference_generation: str) -> str:
    publisher = ServingPublisher(
        root,
        COMMIT_A,
        table_specs={"dashboard_summary": ServingTableSpec(sort_keys=("metric",))},
    )
    manifest = publisher.publish(
        tables={"dashboard_summary": pd.DataFrame({"metric": ["ready"], "value": [1]})},
        watermarks=(
            ServingDatasetWatermark(
                dataset_id="reference_slow",
                generation_id=reference_generation,
                event_time=datetime(2026, 7, 31, 0, 30, tzinfo=UTC),
                published_at=datetime(2026, 7, 31, 0, 31, tzinfo=UTC),
                sequence=1,
                status=FreshnessStatus.FRESH,
            ),
        ),
        source_generations={"reference_slow": reference_generation},
        built_at=datetime(2026, 7, 31, 0, 32, tzinfo=UTC),
    )
    return manifest.generation_id


def _create_lab_artifact(root: Path) -> tuple[Path, Path]:
    root.mkdir(mode=0o700, parents=True)
    frame = pd.DataFrame({"ts_code": ["000001.SZ"], "score": [1.25]})
    payload = root / "001-candidates.parquet"
    frame.to_parquet(payload, index=False)
    artifact = LabShardArtifactManifest(
        name="candidates",
        file_name=payload.name,
        row_count=len(frame),
        columns=tuple(frame.columns),
        file_size=payload.stat().st_size,
        file_sha256=_sha256(payload),
        content_sha256=canonical_shard_frame_digest(frame),
    )
    manifest = LabShardResultManifest(
        schema_version=2,
        worker_code_sha=COMMIT_A,
        content_digest_algorithm="rquant-pandas-table-json-sha256-v2",
        job_id=uuid.uuid4(),
        shard_id=uuid.uuid4(),
        claim_token=uuid.uuid4(),
        claim_generation=1,
        scheduler_fencing_token=1,
        spec_hash=canonical_sha256({"spec": 1}),
        payload_hash=canonical_sha256({"payload": 1}),
        plan_hash=canonical_sha256({"plan": 1}),
        adapter_id="recovery-e2e",
        adapter_version="1",
        artifacts=(artifact,),
    )
    manifest_path = root / "manifest.json"
    manifest_path.write_text(manifest.canonical_json(), encoding="utf-8")
    return manifest_path, payload


def _build_bundle(
    tmp_path: Path,
    *,
    fixture_value: int = 3,
    formal_replay: bool = False,
):
    source = tmp_path / f"backup-{fixture_value}"
    source.mkdir(mode=0o700)
    production_path = source / "production.duckdb"
    if formal_replay:
        from tests.unit.test_runtime_recovery_coordinator import _write_duckdb_artifact

        _write_duckdb_artifact(production_path, rows=fixture_value)
    else:
        _create_production_database(production_path, value=fixture_value)
    _create_state_database(source / "state.sqlite3")
    _create_paper_ledger(source / "paper.sqlite3")
    catalog, readonly_catalog, lake_manifest, lake_object = _create_research_lake(source)
    reference_path = source / "reference" / "reference.sqlite3"
    reference_generation = _create_reference_registry(reference_path)
    serving_root = source / "serving"
    serving_generation = _create_serving(
        serving_root,
        reference_generation=reference_generation,
    )
    serving_manifest = serving_root / "generations" / serving_generation / "manifest.json"
    serving_database = serving_manifest.with_name("serving.duckdb")
    lab_manifest, lab_payload = _create_lab_artifact(source / "artifacts" / "bundle")

    specs = (
        RealRecoveryArtifactSpec(
            logical_role="production",
            kind=RealRecoveryArtifactKind.PRODUCTION_DUCKDB,
            source_path="production.duckdb",
            restore_path="production/rquant.duckdb",
            generation_id=_sha256(source / "production.duckdb"),
            schema_version="production-v1",
            relations=("daily_bar",) if formal_replay else ("daily_bar", "replay_fixture"),
        ),
        RealRecoveryArtifactSpec(
            logical_role="state",
            kind=RealRecoveryArtifactKind.STATE_SQLITE,
            source_path="state.sqlite3",
            restore_path="state/runtime.sqlite3",
            generation_id=_sha256(source / "state.sqlite3"),
            schema_version="state-v7",
            relations=("state_event",),
        ),
        RealRecoveryArtifactSpec(
            logical_role="paper_ledger",
            kind=RealRecoveryArtifactKind.STATE_SQLITE,
            source_path="paper.sqlite3",
            restore_path="state/paper.sqlite3",
            generation_id=_sha256(source / "paper.sqlite3"),
            schema_version="paper-ledger-v4",
            relations=(
                "paper_ledger_attestation",
                "paper_ledger_head_marker",
                "paper_ledger_schema",
            ),
        ),
        RealRecoveryArtifactSpec(
            logical_role="research_catalog",
            kind=RealRecoveryArtifactKind.RESEARCH_CATALOG,
            source_path=catalog.relative_to(source).as_posix(),
            restore_path="research/catalog/research.duckdb",
            generation_id=_sha256(catalog),
            schema_version="research-catalog-v1",
            relations=("research_partition", "research_dataset_coverage"),
            references={"partition": "lake_manifest"},
        ),
        RealRecoveryArtifactSpec(
            logical_role="research_catalog_ro",
            kind=RealRecoveryArtifactKind.RESEARCH_CATALOG_READONLY,
            source_path=readonly_catalog.relative_to(source).as_posix(),
            restore_path="research/catalog/research_ro.duckdb",
            generation_id=_sha256(readonly_catalog),
            schema_version="research-catalog-v1",
            relations=("research_partition", "research_dataset_coverage"),
            references={"authority": "research_catalog"},
        ),
        RealRecoveryArtifactSpec(
            logical_role="lake_manifest",
            kind=RealRecoveryArtifactKind.RESEARCH_LAKE_MANIFEST,
            source_path=lake_manifest.relative_to(source).as_posix(),
            restore_path=(
                Path("research/lake") / lake_manifest.relative_to(source / "lake")
            ).as_posix(),
            generation_id=_sha256(lake_manifest),
            schema_version="research-partition-v2",
            references={"parquet": "lake_object"},
        ),
        RealRecoveryArtifactSpec(
            logical_role="lake_object",
            kind=RealRecoveryArtifactKind.RESEARCH_LAKE_OBJECT,
            source_path=lake_object.relative_to(source).as_posix(),
            restore_path=(
                Path("research/lake") / lake_object.relative_to(source / "lake")
            ).as_posix(),
            generation_id=_sha256(lake_object),
            schema_version="parquet-v1",
            references={"manifest": "lake_manifest"},
        ),
        RealRecoveryArtifactSpec(
            logical_role="reference_slow",
            kind=RealRecoveryArtifactKind.REFERENCE_SLOW_SQLITE,
            source_path=reference_path.relative_to(source).as_posix(),
            restore_path="reference/reference.sqlite3",
            generation_id=reference_generation,
            schema_version="reference-v2",
            available_at=datetime(2026, 7, 31, 0, 31, tzinfo=UTC),
            price_basis="raw_session",
        ),
        RealRecoveryArtifactSpec(
            logical_role="serving_pointer",
            kind=RealRecoveryArtifactKind.SERVING_CURRENT,
            source_path="serving/current.json",
            restore_path="serving/current.json",
            generation_id=serving_generation,
            schema_version="serving-pointer-v1",
            references={"manifest": "serving_manifest"},
        ),
        RealRecoveryArtifactSpec(
            logical_role="serving_manifest",
            kind=RealRecoveryArtifactKind.SERVING_MANIFEST,
            source_path=serving_manifest.relative_to(source).as_posix(),
            restore_path=(
                Path("serving/generations") / serving_generation / "manifest.json"
            ).as_posix(),
            generation_id=serving_generation,
            schema_version="serving-manifest-v1",
            available_at=datetime(2026, 7, 31, 0, 32, tzinfo=UTC),
            price_basis="raw_session",
            references={"database": "serving_database", "reference": "reference_slow"},
        ),
        RealRecoveryArtifactSpec(
            logical_role="serving_database",
            kind=RealRecoveryArtifactKind.SERVING_DATABASE,
            source_path=serving_database.relative_to(source).as_posix(),
            restore_path=(
                Path("serving/generations") / serving_generation / "serving.duckdb"
            ).as_posix(),
            generation_id=serving_generation,
            schema_version="serving-database-v1",
            relations=("dashboard_summary",),
            references={"manifest": "serving_manifest"},
        ),
        RealRecoveryArtifactSpec(
            logical_role="lab_manifest",
            kind=RealRecoveryArtifactKind.LAB_ARTIFACT_MANIFEST,
            source_path=lab_manifest.relative_to(source).as_posix(),
            restore_path="artifacts/bundle/manifest.json",
            generation_id=_sha256(lab_manifest),
            schema_version="lab-shard-result-v2",
            references={f"file:{lab_payload.name}": "lab_payload"},
        ),
        RealRecoveryArtifactSpec(
            logical_role="lab_payload",
            kind=RealRecoveryArtifactKind.LAB_ARTIFACT_OBJECT,
            source_path=lab_payload.relative_to(source).as_posix(),
            restore_path=f"artifacts/bundle/{lab_payload.name}",
            generation_id=_sha256(lab_payload),
            schema_version="parquet-v1",
            references={"manifest": "lab_manifest"},
        ),
    )
    target = build_real_recovery_target(
        source_root=source,
        target_commit=COMMIT_B,
        target_profile_generation=PROFILE_GENERATION,
        as_of=AS_OF,
        production_artifact_role="production",
        paper_ledger_artifact_role="paper_ledger",
        artifacts=specs,
        external_attestations={
            "paper_ledger": canonical_sha256({"fixture": _sha256(source / "paper.sqlite3")})
        },
    )
    if formal_replay:
        from tests.unit.test_runtime_recovery_coordinator import (
            _fixed_replay_result_sha256,
            _real_strategy_replay_result,
        )

        expectations = tuple(
            RuntimeRecoveryFixedReplayExpectation.model_validate(
                {
                    "strategy_id": strategy_id,
                    "expected_result_sha256": _fixed_replay_result_sha256(result),
                    "result": {
                        **result,
                        "result_sha256": _fixed_replay_result_sha256(result),
                    },
                    "status": "passed",
                }
            )
            for strategy_id in ("auction_gap", "growth_board_surge", "n_shape")
            for result in (_real_strategy_replay_result(production_path, strategy_id),)
        )
        replay = RuntimeRecoveryFixedReplayVerifier(expectations=expectations)
    else:
        replay = _FixedReplayVerifier()
    signer = _HmacSigner(b"rquant-recovery-test-secret-32bytes")
    tool = seal_recovery_tool_bundle(
        target=target,
        verifier_commit=COMMIT_A,
        executable_fingerprint=replay.fingerprint,
        key_id=signer.key_id,
        signer=signer,
    )
    return source, target, tool, replay


def _restorer(
    *,
    source: Path,
    target: Path,
    replay: FixedReplayVerifier,
) -> RealRecoveryRestorer:
    return RealRecoveryRestorer(
        backup_root=source,
        restore_root=target,
        signature_verifier=_HmacVerifier(b"rquant-recovery-test-secret-32bytes"),
        fixed_replay_verifier=replay,
        max_artifacts=64,
        max_total_bytes=256 * 1024 * 1024,
        deadline_seconds=60,
    )


def _crash_real_restorer_after_current(
    source: Path,
    restore_root: Path,
    target: object,
    tool: object,
) -> None:
    restorer = _restorer(source=source, target=restore_root, replay=_FixedReplayVerifier())

    def crash(stage: str) -> None:
        if stage == "after_current":
            os._exit(73)

    restorer.restore(target=target, tool_bundle=tool, fault_hook=crash)


def test_real_recovery_restores_real_contracts_and_accepts_cross_commit(
    tmp_path: Path,
) -> None:
    source, target_manifest, tool, replay = _build_bundle(tmp_path)
    restore_root = tmp_path / "isolated-restore"
    restore_root.mkdir(mode=0o700)

    receipt = _restorer(source=source, target=restore_root, replay=replay).restore(
        target=target_manifest,
        tool_bundle=tool,
    )

    assert target_manifest.target_commit == COMMIT_B
    assert tool.verifier_commit == COMMIT_A
    assert receipt.status == "succeeded"
    current = json.loads((restore_root / "current.json").read_text(encoding="utf-8"))
    assert current["generation_id"] == target_manifest.manifest_id
    generation = restore_root / current["generation_path"]
    assert (generation / "serving/current.json").is_file()
    assert (generation / "research/catalog/research_ro.duckdb").is_file()
    assert {item.strategy_id for item in receipt.fixed_replays} == {
        "n_shape",
        "growth_board_surge",
        "auction_gap",
    }


@pytest.mark.parametrize("missing_role", ("paper_ledger", "state"))
def test_target_manifest_rejects_incomplete_production_role_graph(
    tmp_path: Path,
    missing_role: str,
) -> None:
    _source, target, _tool, _replay = _build_bundle(tmp_path)
    payload = target.model_dump(mode="python", exclude={"manifest_id"})
    payload["artifacts"] = tuple(
        artifact for artifact in target.artifacts if artifact.logical_role != missing_role
    )

    with pytest.raises(ValueError, match="complete production role|paper ledger|inventory"):
        RealRecoveryTargetManifest.model_validate(payload)


def test_restorer_rejects_model_constructed_incomplete_role_graph(tmp_path: Path) -> None:
    source, target, tool, replay = _build_bundle(tmp_path)
    restore_root = tmp_path / "isolated-restore"
    restore_root.mkdir(mode=0o700)
    incomplete = target.model_copy(
        update={
            "artifacts": tuple(
                artifact
                for artifact in target.artifacts
                if artifact.logical_role != target.paper_ledger_artifact_role
            )
        }
    )

    with pytest.raises(RealRecoveryIntegrityError, match="complete production role|paper ledger"):
        _restorer(source=source, target=restore_root, replay=replay).restore(
            target=incomplete,
            tool_bundle=tool,
        )


def test_current_loader_rejects_incomplete_role_graph_before_generation_scan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.runtime_recovery_artifacts as artifact_module

    source, target, tool, replay = _build_bundle(tmp_path)
    restore_root = tmp_path / "isolated-restore"
    restore_root.mkdir(mode=0o700)
    receipt = _restorer(source=source, target=restore_root, replay=replay).restore(
        target=target,
        tool_bundle=tool,
    )
    incomplete = target.model_copy(
        update={
            "artifacts": tuple(
                artifact
                for artifact in target.artifacts
                if artifact.logical_role != target.paper_ledger_artifact_role
            )
        }
    )

    def generation_scan_started(*_args: object, **_kwargs: object):
        raise AssertionError("generation scan started before role graph validation")

    monkeypatch.setattr(artifact_module.os, "walk", generation_scan_started)
    with pytest.raises(RealRecoveryIntegrityError, match="complete production role|paper ledger"):
        load_verified_real_recovery_receipt(
            restore_root=restore_root,
            receipt_id=str(receipt.receipt_id),
            target=incomplete,
            verification_budget=RecoveryVerificationBudget(deadline_seconds=60),
        )


def test_receipt_loader_rejects_missing_or_tampered_current_generation(
    tmp_path: Path,
) -> None:
    source, target, tool, replay = _build_bundle(tmp_path)
    restore_root = tmp_path / "isolated-restore"
    restore_root.mkdir(mode=0o700)
    receipt = _restorer(source=source, target=restore_root, replay=replay).restore(
        target=target,
        tool_bundle=tool,
    )
    generation = restore_root / "generations" / str(target.manifest_id)
    production = generation / "production" / "rquant.duckdb"
    production.parent.chmod(0o700)
    production.chmod(0o600)
    production.unlink()

    with pytest.raises(RealRecoveryIntegrityError, match="artifact|generation|missing"):
        load_verified_real_recovery_receipt(
            restore_root=restore_root,
            receipt_id=str(receipt.receipt_id),
            target=target,
            verification_budget=RecoveryVerificationBudget(deadline_seconds=60),
        )


def test_receipt_loader_rejects_empty_current_generation(tmp_path: Path) -> None:
    source, target, tool, replay = _build_bundle(tmp_path)
    restore_root = tmp_path / "isolated-restore"
    restore_root.mkdir(mode=0o700)
    receipt = _restorer(source=source, target=restore_root, replay=replay).restore(
        target=target,
        tool_bundle=tool,
    )
    generation = restore_root / "generations" / str(target.manifest_id)
    for path in generation.rglob("*"):
        path.chmod(0o700 if path.is_dir() else 0o600)
    generation.chmod(0o700)
    shutil.rmtree(generation)
    generation.mkdir(mode=0o700)

    with pytest.raises(RealRecoveryIntegrityError, match="artifact inventory|generation"):
        load_verified_real_recovery_receipt(
            restore_root=restore_root,
            receipt_id=str(receipt.receipt_id),
            target=target,
            verification_budget=RecoveryVerificationBudget(deadline_seconds=60),
        )


def test_full_receipt_verification_replays_and_binds_current_generation(
    tmp_path: Path,
) -> None:
    source, target, tool, replay = _build_bundle(tmp_path)
    restore_root = tmp_path / "isolated-restore"
    restore_root.mkdir(mode=0o700)
    receipt = _restorer(source=source, target=restore_root, replay=replay).restore(
        target=target,
        tool_bundle=tool,
    )
    calls = 0

    class CountingReplay:
        fingerprint = replay.fingerprint

        def verify(
            self,
            *,
            target_root: Path,
            dataset_path: Path,
        ) -> tuple[FixedReplayReceipt, ...]:
            nonlocal calls
            calls += 1
            return replay.verify(target_root=target_root, dataset_path=dataset_path)

    current, verified = load_full_verified_current_recovery_receipt(
        restore_root=restore_root,
        receipt_id=str(receipt.receipt_id),
        target=target,
        fixed_replay_verifier=CountingReplay(),
        verification_budget=RecoveryVerificationBudget(deadline_seconds=60),
    )

    assert current.generation_id == target.manifest_id
    assert verified.receipt_id == receipt.receipt_id
    assert calls == 1


def test_reference_payload_json_is_rejected_before_oversized_parse(tmp_path: Path) -> None:
    import rquant.runtime_recovery_artifacts as artifact_module

    source = tmp_path / "reference-source"
    reference = source / "reference.sqlite3"
    generation = _create_reference_registry(reference)
    connection = sqlite3.connect(reference)
    try:
        connection.execute(
            "UPDATE reference_record SET payload_json = ?",
            (
                json.dumps(
                    {"price_basis": "raw_session", "padding": "x" * 2048},
                    separators=(",", ":"),
                    sort_keys=True,
                ),
            ),
        )
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(RealRecoveryIntegrityError, match="JSON|byte budget|oversized"):
        artifact_module._isolated_contract_for_path(
            path=reference,
            kind=RealRecoveryArtifactKind.REFERENCE_SLOW_SQLITE,
            relations=(),
            generation_id=generation,
            price_basis="raw_session",
            meter=artifact_module._VerificationMeter(
                RecoveryVerificationBudget(
                    deadline_seconds=60,
                    max_json_bytes=1024,
                )
            ),
        )


def test_duckdb_relation_row_budget_is_checked_before_ordered_hashing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.runtime_recovery_artifacts as artifact_module
    from tests.unit.test_runtime_recovery_coordinator import _write_duckdb_artifact

    source = tmp_path / "relation-budget-source"
    source.mkdir(mode=0o700)
    database = source / "production.duckdb"
    _write_duckdb_artifact(database, rows=3)

    def hashing_started(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("ordered relation hashing started before budget validation")

    monkeypatch.setattr(artifact_module, "_update_length_prefixed", hashing_started)

    with pytest.raises(RealRecoveryIntegrityError, match="relation row total exceeds budget"):
        artifact_module._duckdb_relation_evidence(
            database,
            ("daily_bar",),
            meter=artifact_module._VerificationMeter(
                RecoveryVerificationBudget(
                    max_relation_rows=1,
                    deadline_seconds=60,
                )
            ),
        )


def test_sqlite_relation_row_budget_is_checked_before_keyset_digest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.runtime_recovery_artifacts as artifact_module

    database = tmp_path / "state.sqlite3"
    _create_state_database(database)
    connection = sqlite3.connect(database)
    try:
        connection.execute('INSERT INTO state_event VALUES (2, \'{"state":"second"}\')')
        connection.commit()
    finally:
        connection.close()

    def digest_started(*_args: object, **_kwargs: object) -> bytes:
        raise AssertionError("SQLite digest started before row budget validation")

    monkeypatch.setattr(artifact_module, "_sqlite_row_bytes", digest_started)
    meter = artifact_module._VerificationMeter(
        RecoveryVerificationBudget(max_relation_rows=1, deadline_seconds=60)
    )

    with pytest.raises(RealRecoveryIntegrityError, match="relation row total exceeds budget"):
        artifact_module._sqlite_relation_evidence(
            database,
            ("state_event",),
            meter=meter,
        )


def test_sqlite_oversized_blob_is_rejected_before_python_payload_allocation(
    tmp_path: Path,
) -> None:
    import rquant.runtime_recovery_artifacts as artifact_module

    database = tmp_path / "oversized.sqlite3"
    _create_blob_database(database)
    gc.collect()
    tracemalloc.start()
    try:
        with pytest.raises(
            RealRecoveryIntegrityError,
            match="row exceeds byte budget|memory budget",
        ):
            artifact_module._sqlite_relation_evidence(
                database,
                ("blob_event",),
                meter=artifact_module._VerificationMeter(
                    RecoveryVerificationBudget(
                        max_row_bytes=1024,
                        duckdb_memory_bytes=16 * 1024 * 1024,
                        deadline_seconds=60,
                    )
                ),
            )
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert peak < 2 * 1024 * 1024


def test_sqlite_oversized_blob_rejection_stays_below_rss_hard_limit(
    tmp_path: Path,
) -> None:
    database = tmp_path / "oversized-rss.sqlite3"
    _create_blob_database(database)
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe(duplex=False)
    process = context.Process(
        target=_oversized_blob_rss_probe,
        args=(str(database), child),
    )
    process.start()
    child.close()
    assert parent.poll(30), "oversized BLOB RSS probe timed out"
    rejected, rss_growth = parent.recv()
    process.join(timeout=10)

    assert process.exitcode == 0
    assert rejected is True
    assert rss_growth < 16 * 1024 * 1024


def test_sqlite_budgeted_blob_is_incrementally_hashed_with_stable_digest(
    tmp_path: Path,
) -> None:
    import rquant.runtime_recovery_artifacts as artifact_module

    database = tmp_path / "budgeted-blob.sqlite3"
    _create_blob_database(database, size_bytes=512 * 1024)
    budget = RecoveryVerificationBudget(
        max_row_bytes=2 * 1024 * 1024,
        duckdb_memory_bytes=16 * 1024 * 1024,
        deadline_seconds=60,
    )
    gc.collect()
    tracemalloc.start()
    try:
        first = artifact_module._sqlite_relation_evidence(
            database,
            ("blob_event",),
            meter=artifact_module._VerificationMeter(budget),
        )
        second = artifact_module._sqlite_relation_evidence(
            database,
            ("blob_event",),
            meter=artifact_module._VerificationMeter(budget),
        )
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert first == second
    assert first[0].row_count == 1
    assert peak < 2 * 1024 * 1024


def test_sqlite_incremental_digest_matches_existing_v1_row_encoding(
    tmp_path: Path,
) -> None:
    import rquant.runtime_recovery_artifacts as artifact_module

    database = tmp_path / "digest-compatibility.sqlite3"
    connection = sqlite3.connect(database)
    try:
        connection.execute(
            """
            CREATE TABLE mixed_event(
                sequence INTEGER PRIMARY KEY,
                count_value INTEGER,
                ratio_value REAL,
                text_value TEXT,
                blob_value BLOB
            ) STRICT
            """
        )
        connection.executemany(
            "INSERT INTO mixed_event VALUES (?, ?, ?, ?, ?)",
            (
                (1, 7, 1.25, 'quoted "text"', b"\x00\xff"),
                (2, None, None, None, None),
            ),
        )
        connection.commit()
    finally:
        connection.close()
    database.chmod(0o600)

    expected = hashlib.sha256()
    expected.update(artifact_module._RELATION_HASH_CONTRACT.encode("ascii"))
    connection = sqlite3.connect(database)
    try:
        rows = connection.execute("SELECT * FROM mixed_event ORDER BY sequence").fetchall()
    finally:
        connection.close()
    for row in rows:
        artifact_module._update_length_prefixed(
            expected,
            artifact_module._sqlite_row_bytes(row),
        )

    evidence = artifact_module._sqlite_relation_evidence(
        database,
        ("mixed_event",),
        meter=artifact_module._VerificationMeter(RecoveryVerificationBudget(deadline_seconds=60)),
    )

    assert evidence[0].content_sha256 == expected.hexdigest()


def test_sqlite_explicit_rowid_business_column_uses_unshadowed_internal_locator(
    tmp_path: Path,
) -> None:
    import rquant.runtime_recovery_artifacts as artifact_module

    database = tmp_path / "explicit-rowid.sqlite3"
    connection = sqlite3.connect(database)
    try:
        connection.execute(
            'CREATE TABLE business_event("rowid" TEXT, sequence INTEGER PRIMARY KEY, payload BLOB)'
        )
        connection.execute(
            'INSERT INTO business_event("rowid", sequence, payload) VALUES (?, ?, ?)',
            ("business-row-id", 1, b"payload"),
        )
        connection.commit()
    finally:
        connection.close()
    database.chmod(0o600)

    first = artifact_module._sqlite_relation_evidence(
        database,
        ("business_event",),
        meter=artifact_module._VerificationMeter(RecoveryVerificationBudget(deadline_seconds=60)),
    )
    second = artifact_module._sqlite_relation_evidence(
        database,
        ("business_event",),
        meter=artifact_module._VerificationMeter(RecoveryVerificationBudget(deadline_seconds=60)),
    )

    assert first == second
    assert first[0].row_count == 1


def test_sqlite_all_rowid_aliases_shadowed_without_primary_key_is_typed_failure(
    tmp_path: Path,
) -> None:
    import rquant.runtime_recovery_artifacts as artifact_module

    database = tmp_path / "shadowed-rowid.sqlite3"
    connection = sqlite3.connect(database)
    try:
        connection.execute(
            'CREATE TABLE ambiguous_event("rowid" TEXT, "_rowid_" TEXT, "oid" TEXT, payload BLOB)'
        )
        connection.execute('INSERT INTO ambiguous_event VALUES ("r", "u", "o", X\'00\')')
        connection.commit()
    finally:
        connection.close()
    database.chmod(0o600)

    with pytest.raises(
        RealRecoveryIntegrityError,
        match="primary key|rowid alias|locator",
    ):
        artifact_module._sqlite_relation_evidence(
            database,
            ("ambiguous_event",),
            meter=artifact_module._VerificationMeter(
                RecoveryVerificationBudget(deadline_seconds=60)
            ),
        )


def test_descriptor_hash_and_verification_meter_honor_cancel_inside_work(
    tmp_path: Path,
) -> None:
    import rquant.runtime_recovery_artifacts as artifact_module

    artifact = tmp_path / "large.bin"
    artifact.write_bytes(b"x" * (3 * 1024 * 1024))
    descriptor = os.open(artifact, os.O_RDONLY)
    checks = 0

    def check() -> None:
        nonlocal checks
        checks += 1
        if checks >= 2:
            raise RealRecoveryIntegrityError("recovery operation cancelled")

    try:
        with pytest.raises(RealRecoveryIntegrityError, match="cancelled"):
            artifact_module._hash_descriptor(
                descriptor,
                max_bytes=artifact.stat().st_size,
                check=check,
            )
    finally:
        os.close(descriptor)
    with pytest.raises(RealRecoveryIntegrityError, match="cancelled"):
        artifact_module._VerificationMeter(
            RecoveryVerificationBudget(deadline_seconds=60),
            cancelled=lambda: True,
        ).check_deadline()

    assert checks == 2


def test_sqlite_path_replacement_between_preflight_and_payload_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.runtime_recovery_artifacts as artifact_module

    database = tmp_path / "snapshot.sqlite3"
    replacement = tmp_path / "replacement.sqlite3"
    _create_state_database(database)
    _create_state_database(replacement)
    connection = sqlite3.connect(replacement)
    try:
        connection.execute(
            "UPDATE state_event SET payload = ? WHERE sequence = 1",
            ('{"state":"replacement"}',),
        )
        connection.commit()
    finally:
        connection.close()
    replacement.chmod(0o600)
    original_keyset = artifact_module._sqlite_keyset_rows
    replaced = False

    def replace_before_payload(*args: object, **kwargs: object):
        nonlocal replaced
        for locator in original_keyset(*args, **kwargs):
            if not replaced:
                replaced = True
                os.replace(replacement, database)
            yield locator

    monkeypatch.setattr(artifact_module, "_sqlite_keyset_rows", replace_before_payload)

    with pytest.raises(RealRecoveryIntegrityError, match="changed during snapshot"):
        artifact_module._sqlite_relation_evidence(
            database,
            ("state_event",),
            meter=artifact_module._VerificationMeter(
                RecoveryVerificationBudget(deadline_seconds=60)
            ),
        )

    assert replaced is True


def test_sqlite_text_over_nonincremental_hard_limit_is_rejected_before_fetch(
    tmp_path: Path,
) -> None:
    import rquant.runtime_recovery_artifacts as artifact_module

    database = tmp_path / "oversized-text.sqlite3"
    _create_text_database(database, size_bytes=600 * 1024)
    gc.collect()
    tracemalloc.start()
    try:
        with pytest.raises(
            RealRecoveryIntegrityError,
            match="TEXT exceeds the non-incremental verifier hard limit",
        ):
            artifact_module._sqlite_relation_evidence(
                database,
                ("text_event",),
                meter=artifact_module._VerificationMeter(
                    RecoveryVerificationBudget(
                        max_row_bytes=8 * 1024 * 1024,
                        duckdb_memory_bytes=16 * 1024 * 1024,
                        deadline_seconds=60,
                    )
                ),
            )
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert peak < 2 * 1024 * 1024


def test_sqlite_relation_digest_uses_bounded_reproducible_keyset_queries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.runtime_recovery_artifacts as artifact_module

    database = tmp_path / "state.sqlite3"
    _create_state_database(database)
    connection = sqlite3.connect(database)
    try:
        connection.executemany(
            "INSERT INTO state_event(sequence, payload) VALUES (?, ?)",
            (
                (index, json.dumps({"index": index}, separators=(",", ":")))
                for index in range(2, 2052)
            ),
        )
        connection.commit()
    finally:
        connection.close()

    statements: list[str] = []
    real_connect = artifact_module.sqlite3.connect

    def traced_connect(*args: object, **kwargs: object) -> sqlite3.Connection:
        observed = real_connect(*args, **kwargs)
        observed.set_trace_callback(statements.append)
        return observed

    monkeypatch.setattr(artifact_module.sqlite3, "connect", traced_connect)
    budget = RecoveryVerificationBudget(
        max_relation_rows=10_000,
        max_relation_bytes=16 * 1024 * 1024,
        deadline_seconds=60,
    )
    first = artifact_module._sqlite_relation_evidence(
        database,
        ("state_event",),
        meter=artifact_module._VerificationMeter(budget),
    )
    second = artifact_module._sqlite_relation_evidence(
        database,
        ("state_event",),
        meter=artifact_module._VerificationMeter(budget),
    )

    ordered_reads = tuple(
        statement
        for statement in statements
        if 'FROM "state_event"' in statement and "ORDER BY" in statement
    )
    assert ordered_reads
    assert all("LIMIT 1024" in statement for statement in ordered_reads)
    assert first == second


def test_research_lake_row_budget_is_checked_before_ordered_hashing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.runtime_recovery_artifacts as artifact_module
    from rquant.research_lake import ResearchPartitionManifest

    source = tmp_path / "lake-budget-source"
    source.mkdir(mode=0o700)
    _catalog, _readonly, manifest_path, data_path = _create_research_lake(source)
    manifest = ResearchPartitionManifest.model_validate_json(manifest_path.read_bytes())
    meter = artifact_module._VerificationMeter(
        RecoveryVerificationBudget(max_relation_rows=1, deadline_seconds=60)
    )
    meter.add_relation_row(b"already-consumed")

    def hashing_started(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("ordered lake hashing started before budget validation")

    monkeypatch.setattr(artifact_module, "_update_research_logical_hash", hashing_started)

    with pytest.raises(RealRecoveryIntegrityError, match="relation row total exceeds budget"):
        artifact_module._verify_bounded_research_partition(
            path=data_path,
            manifest=manifest,
            as_of=AS_OF,
            meter=meter,
        )


def test_fixed_replay_is_continuously_interrupted_at_deadline(tmp_path: Path) -> None:
    import rquant.runtime_recovery_artifacts as artifact_module

    class BlockingReplay:
        fingerprint = "blocking-replay-v1"

        def verify(
            self,
            *,
            target_root: Path,
            dataset_path: Path,
        ) -> tuple[FixedReplayReceipt, ...]:
            del target_root, dataset_path
            time.sleep(2)
            raise AssertionError("deadline did not interrupt fixed replay")

    meter = artifact_module._VerificationMeter(RecoveryVerificationBudget(deadline_seconds=0.05))
    started = time.monotonic()

    with pytest.raises(RealRecoveryIntegrityError, match="deadline"):
        artifact_module._run_fixed_replay_with_deadline(
            verifier=BlockingReplay(),
            target_root=tmp_path,
            dataset_path=tmp_path / "dataset.duckdb",
            meter=meter,
        )

    assert time.monotonic() - started < 1


def test_failed_real_recovery_preserves_previous_generation_and_audits_failure(
    tmp_path: Path,
) -> None:
    source, first_target, first_tool, replay = _build_bundle(tmp_path, fixture_value=3)
    restore_root = tmp_path / "isolated-restore"
    restore_root.mkdir(mode=0o700)
    restorer = _restorer(source=source, target=restore_root, replay=replay)
    restorer.restore(target=first_target, tool_bundle=first_tool)
    previous = (restore_root / "current.json").read_bytes()

    production = source / "production.duckdb"
    production.chmod(0o600)
    with production.open("ab") as handle:
        handle.write(b"tamper")

    with pytest.raises(RealRecoveryIntegrityError, match="hash|size"):
        restorer.restore(target=first_target, tool_bundle=first_tool)

    assert (restore_root / "current.json").read_bytes() == previous
    audits = [json.loads(path.read_text()) for path in (restore_root / "audits").glob("*.json")]
    assert any(item["status"] == "failed" for item in audits)


def test_tool_bundle_binds_target_profile_and_signature(tmp_path: Path) -> None:
    source, target, tool, replay = _build_bundle(tmp_path)
    restore_root = tmp_path / "isolated-restore"
    restore_root.mkdir(mode=0o700)
    tampered = RecoveryToolVerifierBundle.model_validate(
        {
            **tool.model_dump(mode="python", exclude={"bundle_id"}),
            "target_profile_generation": "f" * 64,
        }
    )

    with pytest.raises(RealRecoveryIntegrityError, match="profile|signature"):
        _restorer(source=source, target=restore_root, replay=replay).restore(
            target=target,
            tool_bundle=tampered,
        )
    assert not (restore_root / "current.json").exists()


def test_restore_rejects_symlink_hardlink_and_partial_copy(tmp_path: Path) -> None:
    source, target, tool, replay = _build_bundle(tmp_path)
    restore_root = tmp_path / "isolated-restore"
    restore_root.mkdir(mode=0o700)
    production = source / "production.duckdb"
    hardlink = source / "production-hardlink.duckdb"
    os.link(production, hardlink)

    with pytest.raises(RealRecoveryIntegrityError, match="link"):
        _restorer(source=source, target=restore_root, replay=replay).restore(
            target=target,
            tool_bundle=tool,
        )
    assert not (restore_root / "current.json").exists()


def test_restore_rejects_database_sidecar_not_bound_by_manifest(tmp_path: Path) -> None:
    source, target, tool, replay = _build_bundle(tmp_path)
    restore_root = tmp_path / "isolated-restore"
    restore_root.mkdir(mode=0o700)
    (source / "state.sqlite3-wal").write_bytes(b"unsealed transaction")

    with pytest.raises(RealRecoveryIntegrityError, match="unsealed sidecar"):
        _restorer(source=source, target=restore_root, replay=replay).restore(
            target=target,
            tool_bundle=tool,
        )

    assert not (restore_root / "current.json").exists()


def test_partial_copy_is_quarantined_before_publication(tmp_path: Path) -> None:
    source, target, tool, replay = _build_bundle(tmp_path)
    restore_root = tmp_path / "isolated-restore"
    restore_root.mkdir(mode=0o700)

    def truncate_candidate(stage: str) -> None:
        if stage != "after_copy":
            return
        candidate = next((restore_root / ".candidates").iterdir())
        production = candidate / "production" / "rquant.duckdb"
        with production.open("r+b") as handle:
            handle.truncate(max(0, production.stat().st_size // 2))

    with pytest.raises(RealRecoveryIntegrityError, match="hash|size"):
        _restorer(source=source, target=restore_root, replay=replay).restore(
            target=target,
            tool_bundle=tool,
            fault_hook=truncate_candidate,
        )

    assert not (restore_root / "current.json").exists()
    assert not tuple((restore_root / ".candidates").iterdir())
    assert len(tuple((restore_root / ".failed").iterdir())) == 1


def test_concurrent_restore_serializes_and_publishes_one_immutable_generation(
    tmp_path: Path,
) -> None:
    source, target, tool, replay = _build_bundle(tmp_path)
    restore_root = tmp_path / "isolated-restore"
    restore_root.mkdir(mode=0o700)

    def execute(_index: int):
        return _restorer(source=source, target=restore_root, replay=replay).restore(
            target=target,
            tool_bundle=tool,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        receipts = tuple(executor.map(execute, (1, 2)))

    assert {item.status for item in receipts} == {"succeeded"}
    assert len(tuple((restore_root / "generations").iterdir())) == 1
    current = json.loads((restore_root / "current.json").read_text(encoding="utf-8"))
    assert current["generation_id"] == target.manifest_id


def test_hard_crash_after_current_is_rolled_back_on_restart(tmp_path: Path) -> None:
    first_source, first_target, first_tool, replay = _build_bundle(tmp_path, fixture_value=3)
    second_source, second_target, second_tool, _ = _build_bundle(tmp_path, fixture_value=4)
    restore_root = tmp_path / "isolated-restore"
    restore_root.mkdir(mode=0o700)
    _restorer(source=first_source, target=restore_root, replay=replay).restore(
        target=first_target,
        tool_bundle=first_tool,
    )
    previous = (restore_root / "current.json").read_bytes()
    context = multiprocessing.get_context("fork")
    process = context.Process(
        target=_crash_real_restorer_after_current,
        args=(second_source, restore_root, second_target, second_tool),
    )
    process.start()
    process.join(timeout=30)

    assert process.exitcode == 73
    assert (restore_root / "current.json").read_bytes() != previous
    restarted = _restorer(source=second_source, target=restore_root, replay=replay)
    assert (restore_root / "current.json").read_bytes() == previous
    receipt = restarted.restore(target=second_target, tool_bundle=second_tool)
    assert receipt.status == "succeeded"
    assert json.loads((restore_root / "current.json").read_text())["generation_id"] == (
        second_target.manifest_id
    )
    assert (restore_root / "generations" / str(first_target.manifest_id)).is_dir()


@pytest.mark.parametrize(
    "lost_stage",
    (
        "before_generation_publish",
        "after_generation_publish",
        "before_current_publish",
        "after_current_publish",
        "before_completion_receipt",
    ),
)
def test_lost_service_fence_cannot_publish_current_or_success_receipt(
    tmp_path: Path,
    lost_stage: str,
) -> None:
    first_source, first_target, first_tool, replay = _build_bundle(tmp_path, fixture_value=3)
    second_source, second_target, second_tool, _ = _build_bundle(tmp_path, fixture_value=4)
    restore_root = tmp_path / "isolated-restore"
    restore_root.mkdir(mode=0o700)
    _restorer(source=first_source, target=restore_root, replay=replay).restore(
        target=first_target,
        tool_bundle=first_tool,
    )
    previous = (restore_root / "current.json").read_bytes()
    receipt_count = len(tuple((restore_root / "receipts").glob("*.json")))

    lost = False

    def fence(stage: str) -> None:
        nonlocal lost
        if stage == lost_stage:
            lost = True
        if lost:
            raise RealRecoveryIntegrityError("recovery service fence was lost")

    with pytest.raises(RealRecoveryIntegrityError, match="fence"):
        _restorer(source=second_source, target=restore_root, replay=replay).restore(
            target=second_target,
            tool_bundle=second_tool,
            publication_fence=fence,
        )

    assert (restore_root / "current.json").read_bytes() == previous
    assert len(tuple((restore_root / "receipts").glob("*.json"))) == receipt_count
    receipts = tuple(
        json.loads(path.read_text(encoding="utf-8"))
        for path in (restore_root / "receipts").glob("*.json")
    )
    assert not any(
        item["status"] == "succeeded" and item["manifest_id"] == second_target.manifest_id
        for item in receipts
    )


def test_candidate_path_replacement_during_contract_verification_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.runtime_recovery_artifacts as artifact_module

    source, target, tool, replay = _build_bundle(tmp_path)
    restore_root = tmp_path / "isolated-restore"
    restore_root.mkdir(mode=0o700)
    real_contract = artifact_module._contract_for_path
    replaced = False

    def replace_then_verify(**kwargs: object):
        nonlocal replaced
        path = Path(kwargs["path"])
        if not replaced and kwargs["kind"] is RealRecoveryArtifactKind.PRODUCTION_DUCKDB:
            replaced = True
            original = path.with_name(f".{path.name}.replaced")
            path.rename(original)
            shutil.copyfile(original, path)
        return real_contract(**kwargs)

    monkeypatch.setattr(artifact_module, "_contract_for_path", replace_then_verify)
    with pytest.raises(RealRecoveryIntegrityError, match="changed|identity"):
        _restorer(source=source, target=restore_root, replay=replay).restore(
            target=target,
            tool_bundle=tool,
        )

    assert replaced is True
    assert not (restore_root / "current.json").exists()


def test_recovery_service_e2e_restores_real_artifacts_with_formal_replays(
    tmp_path: Path,
) -> None:
    from rquant.runtime_recovery_service import RuntimeRecoveryService

    first_source, first_target, first_tool, first_replay = _build_bundle(
        tmp_path,
        fixture_value=3,
    )
    restore_root = tmp_path / "isolated-restore"
    restore_root.mkdir(mode=0o700)
    _restorer(source=first_source, target=restore_root, replay=first_replay).restore(
        target=first_target,
        tool_bundle=first_tool,
    )
    previous_pointer = json.loads((restore_root / "current.json").read_text(encoding="utf-8"))
    previous_generation = restore_root / previous_pointer["generation_path"]
    damaged_live_database = previous_generation / "production/rquant.duckdb"
    damaged_live_database.chmod(0o600)
    with damaged_live_database.open("ab") as handle:
        handle.write(b"damaged-live-generation")
    damaged_live_sha256 = hashlib.sha256(damaged_live_database.read_bytes()).hexdigest()

    source, target, tool, replay = _build_bundle(
        tmp_path,
        fixture_value=7,
        formal_replay=True,
    )
    manifest_path = source / "recovery-target.json"
    tool_path = source / "recovery-tool.json"
    manifest_path.write_bytes(canonical_json_bytes(target.model_dump(mode="json")))
    tool_path.write_bytes(canonical_json_bytes(tool.model_dump(mode="json")))
    service = RuntimeRecoveryService(
        state_path=tmp_path / "service-state" / "recovery.sqlite3",
        receipt_root=tmp_path / "service-receipts",
        worker_id="recovery-e2e",
        lease_seconds=30,
        max_attempts=2,
        retry_delay_seconds=1,
    )
    job = service.submit(
        request_id="real-formal-replay-e2e",
        backup_root=source,
        manifest_path=manifest_path,
        tool_bundle_path=tool_path,
        restore_root=restore_root,
        deadline_at=datetime.now(UTC) + timedelta(minutes=5),
    )

    result = service.run_real_once(
        signature_verifier=_HmacVerifier(b"rquant-recovery-test-secret-32bytes"),
        fixed_replay_verifier=replay,
        max_artifacts=64,
        max_total_bytes=256 * 1024 * 1024,
    )

    assert result is not None and result.status == "succeeded"
    assert service.job(job.job_id).status == "succeeded"
    current = json.loads((restore_root / "current.json").read_text(encoding="utf-8"))
    generation = restore_root / current["generation_path"]
    assert current["generation_id"] == target.manifest_id
    assert current["generation_id"] != previous_pointer["generation_id"]
    assert previous_generation.is_dir()
    assert hashlib.sha256(damaged_live_database.read_bytes()).hexdigest() == damaged_live_sha256
    assert (generation / "production/rquant.duckdb").is_file()
    assert (generation / "research/catalog/research_ro.duckdb").is_file()
    assert (generation / "serving/current.json").is_file()
    recovery_receipt = next((restore_root / "receipts").glob("*.json"))
    payload = json.loads(recovery_receipt.read_text(encoding="utf-8"))
    assert {item["strategy_id"] for item in payload["fixed_replays"]} == {
        "n_shape",
        "growth_board_surge",
        "auction_gap",
    }
