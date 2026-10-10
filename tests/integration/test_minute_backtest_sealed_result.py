from __future__ import annotations

import json
import os
import shutil
import time
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from rquant.definition_registry import ImmutableDefinitionRegistry
from rquant.experiment_registry import DateRange, ExperimentRegistry
from rquant.lab_artifact_preview import ArtifactCompleteTableBudget, ArtifactPreviewIntegrityError, ArtifactPreviewReader
from rquant.lab_artifact_protocol import LabArtifactCommitSpool, LabFinalizerAuthorityKey
from rquant.lab_artifacts import LabArtifactFileIdentity, LabJobArtifactStore
from rquant.lab_finalizer import LabFinalizer
from rquant.lab_job_center import LabCommandSubmissionFacade
from rquant.lab_job_protocol import LabCommandSpool
from rquant.lab_jobs import JobStatus, LabArtifactPreviewAuthority, LabJobReader, LabJobStore, LabResultState
from rquant.lab_shard_protocol import LabClaimSpool, LabReportSpool, LabShardSucceeded, LabShardTelemetry, LabWorkerReport
from rquant.lab_worker import LabClosedRegistryBinding, LabShardRuntimeManifest, LabWorker, build_builtin_shard_runtime_manifest
from rquant.lab_worker_registry import builtin_lab_shard_configuration, execute_builtin_lab_shard
from rquant.minute_backtest_artifact import MINUTE_RESULT_TABLE_NAMES, MinuteSealedReplayIntegrityError, MinuteSealedReplayReader
from rquant.minute_backtest_formal import MinuteExperimentProtocol, register_minute_plan
from rquant.minute_backtest_formal_adapter import minute_formal_adapter_registry
from rquant.storage.duckdb import DuckDBStore
from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry
from rquant.strict_json import canonical_json_bytes
from tests.unit.test_minute_backtest_formal import publication
from tests.unit.test_minute_backtest_producer import NOW, original_fixture


@pytest.fixture(scope="session")
def sealed_minute(tmp_path_factory: pytest.TempPathFactory) -> Iterator[SimpleNamespace]:
    import rquant.storage.duckdb as storage

    patch = pytest.MonkeyPatch()
    patch.setattr(storage, "_settings", lambda: SimpleNamespace(primary_writer_gate_path=None))
    root = tmp_path_factory.mktemp("minute-sealed-original")
    metadata_path = root / "metadata.duckdb"
    with DuckDBStore(metadata_path) as metadata:
        published, catalog, lake = publication(root, metadata)
    frozen = published.receipt.frozen
    definitions = ImmutableDefinitionRegistry(root / "definitions", execution_registry=BuiltinStrategyEvaluatorRegistry(
        producer_commit=frozen.runtime.producer_commit).trusted_executable_registry())
    trust = root / "experiment-trust"
    trust.mkdir(mode=0o700)
    experiments = ExperimentRegistry(trust / "experiments.sqlite3", managed_trust_root=trust)
    prepared = register_minute_plan(frozen, published, catalog=catalog, definitions=definitions, experiments=experiments,
        protocol=MinuteExperimentProtocol(
            train_range=DateRange(start_date=frozen.runtime.start_date - timedelta(days=2), end_date=frozen.runtime.start_date - timedelta(days=2)),
            validation_range=DateRange(start_date=frozen.runtime.start_date - timedelta(days=1), end_date=frozen.runtime.start_date - timedelta(days=1)),
            frozen_outer_test_range=DateRange(start_date=frozen.runtime.start_date, end_date=frozen.runtime.end_date)),
        now=NOW, deadline=NOW + timedelta(hours=1))
    submitted = prepared.submission(job_id=uuid4())
    jobs = LabJobStore(root / "jobs.sqlite3")
    jobs.initialize()
    reader = LabJobReader(jobs.path)
    commands = LabCommandSpool(root / "commands")
    facade = LabCommandSubmissionFacade(reader=reader, spool=commands, experiment_registry=experiments,
        definition_registry=definitions, clock=lambda: NOW)
    receipt = facade.submit_create(submitted.command, interaction_key="minute-complete-sealed-reader")
    assert receipt.result == "submitted"
    command, = commands.pending()
    lease = jobs.acquire_scheduler_lease(owner_id="controlled-minute-scheduler", lease_seconds=3600, now=NOW)
    accepted = jobs.apply_command(command.envelope, lease=lease, now=NOW,
        submission_authority=lambda envelope, at: facade.validate_prepared_experiment_submission(envelope, observed_at=at))
    assert accepted.status == "applied"
    commands.ack(command, accepted)
    registry = minute_formal_adapter_registry(catalog)
    jobs.plan_job(submitted.command.job_id, registry.plan(submitted.spec), lease=lease, now=NOW)
    claim = jobs.claim_next_shard(worker_id="controlled-minute-worker", shard_lease_seconds=3600, lease=lease, now=NOW)
    assert claim is not None
    validated = registry.validate_claim(claim)
    configuration = builtin_lab_shard_configuration(catalog_path=metadata_path, forbidden_paths=(),
        snapshot_root=root / "metadata-copies", research_lake_root=lake, minute_catalog=catalog)
    start = time.monotonic()
    actual = execute_builtin_lab_shard(json.loads(configuration.model_dump_json()), validated, runtime_code_sha=submitted.spec.code_sha)
    finish = time.monotonic()
    print("minute_seal: original worker result produced", flush=True)
    artifacts = LabJobArtifactStore(root / "artifacts")
    preview = ArtifactPreviewReader(reader=reader, artifact_root=artifacts.root)
    native_reader = MinuteSealedReplayReader(reader=reader, artifact_reader=preview, submission_facade=facade, catalog=catalog)
    read_args = dict(owner_id=frozen.runtime.owner_id, native_id=frozen.runtime.strategy.strategy_id,
        native_version=frozen.runtime.strategy.strategy_version, as_of=NOW + timedelta(minutes=1))
    assert native_reader.read(submitted.command.job_id, **read_args) is None
    commits = LabArtifactCommitSpool(root / "artifact-commits")
    key = LabFinalizerAuthorityKey(key_id="controlled-offline-minute-key", secret=b"controlled-offline-test-key-only-32bytes")
    registered = build_builtin_shard_runtime_manifest(catalog_path=metadata_path, forbidden_paths=(),
        snapshot_root=root / "metadata-copies", research_lake_root=lake)
    runtime = LabShardRuntimeManifest(registry=LabClosedRegistryBinding(registry_id=registered.registry.registry_id,
        registry_version=registered.registry.registry_version, registry_hash=registered.registry.registry_hash,
        configuration_json=canonical_json_bytes(configuration.model_dump(mode="json", round_trip=True)).decode("utf-8")))
    claims = LabClaimSpool(root / "claims")
    reports = LabReportSpool(root / "reports")
    claims.publish(claim)
    worker = None
    try:
        worker = LabWorker(worker_id=claim.worker_id, claim_spool=claims,
            report_spool=reports, artifact_root=root / "shard-artifacts",
            shard_runtime_manifest=runtime, verified_code_sha_provider=lambda: submitted.spec.code_sha, clock=lambda: NOW)
        assert worker.adapter_registry.closed_descriptor() == registry.closed_descriptor()
        manifest = worker._seal_result(claim, actual)
        assert reader.get_artifact_preview_authority(claim.job_id) is None
        telemetry = LabShardTelemetry.from_work_plan(claim.definition.work_plan, monotonic_started=start, monotonic_finished=finish)
        report = LabWorkerReport.from_claim(claim, report_id=uuid4(), reported_at=NOW + timedelta(seconds=1),
            body=LabShardSucceeded.current(result_manifest_hash=manifest.manifest_hash, worker_code_sha=submitted.spec.code_sha, telemetry=telemetry))
        report_entry = reports.publish(report)
        success = jobs.apply_worker_report(report, lease=lease, now=NOW + timedelta(seconds=1))
        assert success.status == "accepted"
        reports.ack(report_entry, success)
        print("minute_seal: original success report accepted", flush=True)
        assert reader.get_artifact_preview_authority(claim.job_id) is None
        finalizer = LabFinalizer(reader=reader, shard_artifact_root=worker.artifact_root, artifact_store=artifacts,
            commit_spool=commits, verified_code_sha_provider=lambda: submitted.spec.code_sha,
            finalizer_authority_key_provider=lambda: key, adapter_registry=registry)
        finalized = finalizer.finalize(claim.job_id)
        assert finalized.status == "published"
        print("minute_seal: original finalizer physically sealed and published", flush=True)
        entry, = commits.pending()
        with artifacts.bind_verified_sealed(entry.envelope.commit.sealed_path, indexed_at=NOW + timedelta(seconds=2)) as binding:
            with jobs.stage_artifact_commit(entry.envelope, binding, authority_key_provider=lambda key_id: key if key_id == key.key_id else None,
                lease=lease, now=NOW + timedelta(seconds=2)) as staged:
                committed = staged.commit(lease=lease, now=NOW + timedelta(seconds=2))
        assert committed.status == "accepted"
        commits.ack(entry, committed)
        assert finalizer.finalize(claim.job_id).status == "not_ready"
        job = reader.get_job(claim.job_id)
        assert job.status is JobStatus.SUCCEEDED and job.result_state is LabResultState.SEALED
        print("minute_seal: original artifact commit accepted", flush=True)
        yield SimpleNamespace(root=root, published=published, catalog=catalog, prepared=prepared, submitted=submitted,
            jobs=jobs, reader=reader, registry=registry, claim=claim, actual=actual, artifacts=artifacts,
            preview=preview, native_reader=native_reader, facade=facade, read_args=read_args,
            finalized=finalized, finalizer=finalizer, committed=committed)
    finally:
        if worker is not None:
            worker.close()
        artifacts.close()
        patch.undo()


def test_original_success_finalizer_seal_and_complete_minute_result(sealed_minute: SimpleNamespace) -> None:
    context = sealed_minute
    result = context.native_reader.read(context.claim.job_id, **context.read_args)
    assert result is not None
    assert result.result.publication == context.published.receipt
    assert result.accepted_spec == context.submitted.spec
    assert result.formal_plan == context.prepared.formal_plan
    assert result.manifest_hash == context.finalized.manifest_hash
    assert result.complete_result_hash == context.finalized.complete_result_hash
    assert len([entry for entry in result.manifest.files if entry.parquet is not None]) == 8
    assert (len(result.result.replay.signals), len(result.result.replay.orders), len(result.result.replay.fills)) == (3, 2, 2)
    original = original_fixture()
    actual = result.result.replay.model_dump(mode="json")
    for field in original["zero_tolerance_fields"]:
        assert actual[field] == original["minute_replay"][field]
    assert result.result.replay.daily_status == "complete"
    assert [day.basis for day in result.result.replay.daily_valuations] == ["pit_asof_15:00", "pit_asof_15:00"]
    (context.root / "sealed-minute-result.json").write_text(result.model_dump_json(exclude_computed_fields=True))
    preview = context.preview.preview(context.claim.job_id, table_name="signals", row_limit=2)
    assert preview.table.total_rows == 3 and len(preview.table.rows) == 2
    assert preview.table.rows_truncated and not preview.table.columns_truncated
    with pytest.raises(ValueError, match="unknown artifact table"):
        context.preview.preview(context.claim.job_id, table_name="missing")


@pytest.mark.parametrize("field,value", [("owner_id", "different-owner"), ("native_id", "auction_gap"),
    ("native_version", 2), ("native_version", True)])
def test_sealed_minute_exact_actor_and_native_selection(sealed_minute: SimpleNamespace, field: str, value: object) -> None:
    arguments = {**sealed_minute.read_args, field: value}
    with pytest.raises(PermissionError, match="owner/native"):
        sealed_minute.native_reader.read(sealed_minute.claim.job_id, **arguments)


def test_sealed_minute_cannot_be_visible_before_real_seal(sealed_minute: SimpleNamespace) -> None:
    context = sealed_minute
    assert context.native_reader.read(context.claim.job_id, **{**context.read_args, "as_of": NOW}) is None


def complete_tables(context: SimpleNamespace) -> object:
    if not hasattr(context, "complete_tables"):
        context.complete_tables = context.preview.read_complete_tables(context.claim.job_id,
            table_names=MINUTE_RESULT_TABLE_NAMES, budget=ArtifactCompleteTableBudget())
    return context.complete_tables


@pytest.mark.parametrize("kind", ["missing", "extra", "row_payload", "row_order", "sequence_type", "full_hash", "core_hash", "seed_hash", "profile", "provenance"])
def test_all_eight_tables_bind_the_complete_typed_body(sealed_minute: SimpleNamespace, kind: str) -> None:
    complete = complete_tables(sealed_minute)
    tables = list(complete.tables)
    if kind == "missing":
        tables.pop()
    elif kind == "extra":
        tables.append(tables[0])
    else:
        selected_name = "signals" if kind in {"row_payload", "row_order", "sequence_type"} else "execution_profile" if kind == "profile" else "replay_summary"
        index = next(i for i, item in enumerate(tables) if item.parquet.table_name == selected_name)
        selected = tables[index]
        rows = [list(row) for row in selected.rows]
        if kind == "row_payload":
            rows[0][selected.parquet.columns.index("payload")] = "{}"
        elif kind == "row_order":
            rows.reverse()
        elif kind == "sequence_type":
            rows[0][selected.parquet.columns.index("sequence")] = 1.0
        elif kind == "profile":
            payload_column = selected.parquet.columns.index("payload")
            profile = json.loads(rows[0][payload_column])
            assert "execution_costs" in profile
            profile["execution_costs"] = {"different_fee_profile": True}
            rows[0][payload_column] = json.dumps(profile)
        elif kind == "provenance":
            payload_column = selected.parquet.columns.index("payload")
            body = json.loads(rows[0][payload_column])
            body["publication"]["seed"]["provenance"]["capture_lineage"][0]["acquisition_commit"] = "b" * 40
            rows[0][payload_column] = json.dumps(body)
        else:
            field = {"full_hash": "full_input_hash", "core_hash": "core_input_hash", "seed_hash": "seed_hash"}[kind]
            rows[0][selected.parquet.columns.index(field)] = "b" * 64
        tables[index] = selected.model_copy(update={"rows": tuple(tuple(row) for row in rows)})
    # Isolate the semantic guard after a genuine complete physical read; this is no new seal fact.
    altered = complete.model_copy(update={"tables": tuple(tables)})
    with pytest.raises(MinuteSealedReplayIntegrityError):
        MinuteSealedReplayReader._complete_result(altered)


@pytest.mark.parametrize("kind", ["accepted_request", "native_registry"])
def test_sealed_minute_requires_original_request_and_registry(sealed_minute: SimpleNamespace, monkeypatch: pytest.MonkeyPatch, kind: str) -> None:
    context = sealed_minute
    if kind == "accepted_request":
        monkeypatch.setattr(context.facade.experiment_registry, "get_submission_intent_for_job", lambda job_id: None)
    else:
        original = context.facade.definition_registry.read_strategy_spec
        fingerprint = context.published.receipt.frozen.native_registration.fingerprint

        def read(fingerprint_value: str, *, as_of: object) -> object:
            return None if fingerprint_value == fingerprint else original(fingerprint_value, as_of=as_of)

        monkeypatch.setattr(context.facade.definition_registry, "read_strategy_spec", read)
    with pytest.raises(MinuteSealedReplayIntegrityError, match="(accepted submission|native registration)"):
        context.native_reader.read(context.claim.job_id, **context.read_args)


@pytest.mark.parametrize("kind", ["encoded", "decoded"])
def test_complete_reader_enforces_actual_table_bytes(sealed_minute: SimpleNamespace, kind: str) -> None:
    context = sealed_minute
    if kind == "encoded":
        budget = ArtifactCompleteTableBudget(max_table_bytes=1)
    else:
        complete = complete_tables(context)
        budget = ArtifactCompleteTableBudget(max_table_bytes=max(entry.size for entry in complete.manifest.files if entry.parquet is not None))
    with pytest.raises(ArtifactPreviewIntegrityError, match="byte budget|uncompressed data exceeds complete"):
        context.preview.read_complete_tables(context.claim.job_id, table_names=MINUTE_RESULT_TABLE_NAMES, budget=budget)


def test_sealed_minute_rechecks_ledger_after_the_complete_read(sealed_minute: SimpleNamespace, monkeypatch: pytest.MonkeyPatch) -> None:
    context = sealed_minute
    original = context.reader.get_artifact_preview_authority
    calls = 0

    def observe(job_id: object) -> object:
        nonlocal calls
        calls += 1
        return None if calls >= 3 else original(job_id)

    monkeypatch.setattr(context.reader, "get_artifact_preview_authority", observe)
    with pytest.raises(MinuteSealedReplayIntegrityError, match="authority changed after"):
        context.native_reader.read(context.claim.job_id, **context.read_args)


def copied_verification_graph(context: SimpleNamespace, root: Path) -> tuple[ArtifactPreviewReader, LabArtifactPreviewAuthority]:
    # Controlled copies isolate physical failures; only the original ledger graph proves success.
    original = context.reader.get_artifact_preview_authority(context.claim.job_id)
    destination = root / "sealed" / context.claim.job_id.hex
    destination.parent.mkdir(parents=True, mode=0o700)
    shutil.copytree(original.evidence.sealed_path, destination)
    identities = []
    for identity in original.evidence.file_identities:
        path = destination / identity.relative_path
        info = path.stat()
        identities.append(LabArtifactFileIdentity(relative_path=identity.relative_path, device=info.st_dev, inode=info.st_ino,
            size=info.st_size, mtime_ns=info.st_mtime_ns, ctime_ns=info.st_ctime_ns))
    evidence = original.evidence.model_copy(update={"sealed_path": destination, "bundle_device": destination.stat().st_dev,
        "bundle_inode": destination.stat().st_ino, "file_identities": tuple(identities)})
    return ArtifactPreviewReader(reader=context.reader, artifact_root=root), LabArtifactPreviewAuthority(job=original.job, evidence=evidence)


@pytest.mark.parametrize("kind", ["hash", "path_replacement_during_read"])
def test_complete_uses_the_original_physical_hash_and_toctou_fence(sealed_minute: SimpleNamespace, tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch, kind: str) -> None:
    reader, authority = copied_verification_graph(sealed_minute, tmp_path / "controlled-artifacts")
    path = authority.evidence.sealed_path / "tables/account.parquet"
    if kind == "hash":
        payload = path.read_bytes()
        path.chmod(0o600)
        path.write_bytes(payload[:-1] + bytes((payload[-1] ^ 1,)))
        path.chmod(0o400)
        info = path.stat()
        replaced = tuple(LabArtifactFileIdentity(relative_path=item.relative_path, device=info.st_dev, inode=info.st_ino,
            size=info.st_size, mtime_ns=info.st_mtime_ns, ctime_ns=info.st_ctime_ns) if item.relative_path == "tables/account.parquet" else item
            for item in authority.evidence.file_identities)
        authority = authority.model_copy(update={"evidence": authority.evidence.model_copy(update={"file_identities": replaced})})
    else:
        original = reader._read_parquet_complete_rows
        replaced = False

        def read(descriptor: int, **arguments: object) -> object:
            nonlocal replaced
            result = original(descriptor, **arguments)
            if not replaced:
                replaced = True
                payload = path.read_bytes()
                path.parent.chmod(0o700)
                path.rename(path.with_suffix(".old"))
                path.write_bytes(payload)
                path.chmod(0o400)
                path.parent.chmod(0o500)
            return result

        monkeypatch.setattr(reader, "_read_parquet_complete_rows", read)
    with pytest.raises(ArtifactPreviewIntegrityError, match="hash conflicts|changed during preview"):
        reader._read_authorized_bundle(authority, table_name=None, row_limit=1, column_limit=1,
            complete_budget=ArtifactCompleteTableBudget(), table_names=MINUTE_RESULT_TABLE_NAMES)
