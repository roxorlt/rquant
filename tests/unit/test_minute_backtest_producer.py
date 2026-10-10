from __future__ import annotations

import json
import base64
import hashlib
import os
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

import pytest

from rquant.minute_backtest_contracts import MAX_INPUT_BYTES, MAX_WORK_UNITS, FrozenMinuteRuntimeInput
from rquant.minute_backtest_definition import bootstrap_minute_definition
from rquant.minute_backtest_publication_contracts import (
    MinuteCaptureLineage, MinuteCodeFile, MinuteDerivation, MinuteOriginMaterial,
    MinuteProvenance, MinutePublicationEvidence, MinuteRuntimeContent,
    MinuteSourceContentSeed, MinuteVisibilityPolicy,
)
from rquant.minute_backtest_producer import (
    _secure_private_bytes, measure_minute_formal_work, minute_metadata_identities,
    publish_minute_input, read_minute_formal_input_table, verify_minute_source_content,
    write_minute_formal_input_table,
)
from rquant.research_catalog import ResearchCatalog
from rquant.runtime_definition_bootstrap import bootstrap_builtin_definitions, plan_builtin_definitions
from rquant.storage.duckdb import DuckDBStore

NOW = datetime(2026, 10, 7, 0, 0, tzinfo=UTC)
FIXTURE_SHA = "d670d16f860f0f11247c64ecf0985fc45316bb4e7dcaa6cb6f1180c618a958b8"


def archived_fixture() -> dict[str, object]:
    import hashlib

    path = Path(__file__).resolve().parents[2] / "data/verification/minute-engine-completion-20261007/core-implementation-01/behavior/daily-n_shape-bar_end.json"
    data = path.read_bytes()
    assert hashlib.sha256(data).hexdigest() == FIXTURE_SHA
    return json.loads(data)


@lru_cache(maxsize=1)
def _current_fixture_bytes() -> bytes:
    from rquant.minute_backtest_runner import run_minute_runtime_replay
    from tests.integration.test_minute_backtest_runtime_parity import _input, _original_replay, _save_parity_evidence

    with TemporaryDirectory(prefix="minute-native-current-abi-") as directory:
        root = Path(directory).resolve()
        value, receipt = _input(root, "n_shape", daily_quotes=True)
        expected = _original_replay(value, receipt, root / "paper")
        result = run_minute_runtime_replay(value, expected=receipt, research_root=root / "backtest")
        for actual, original in ((result.signals, expected["signals"]), (result.orders, expected["orders"]),
            (result.fills, expected["fills"]), (result.queue_records, expected["queue"]),
            (result.account, expected["account"]),
            (tuple(item.account for item in result.daily_valuations), expected["daily_accounts"]),
            (tuple(proof.quote for item in result.daily_valuations for proof in item.price_proofs), expected["daily_quotes"])):
            assert actual == original
        path = root / "current-abi-parity.json"
        _save_parity_evidence(path, value, receipt, expected, result)
        return path.read_bytes()


def original_fixture() -> dict[str, object]:
    # The old capture remains byte-checked; current execution needs its own ABI binding.
    archived_fixture()
    return json.loads(_current_fixture_bytes())


def source_seed(tmp_path: Path) -> MinuteSourceContentSeed:
    value = FrozenMinuteRuntimeInput.model_validate_json(json.dumps(original_fixture()["frozen_input"]))
    reference_sha = hashlib.sha256(_current_fixture_bytes()).hexdigest()
    definitions = tmp_path / "definitions"
    plan = plan_builtin_definitions(producer_commit=value.producer_commit)
    bootstrap_builtin_definitions(definitions, producer_commit=value.producer_commit,
        registered_at=value.available_at, available_at=value.available_at, expected_plan_id=plan.plan_id)
    wrapper = bootstrap_minute_definition(definitions, producer_commit=value.producer_commit, now=NOW)
    from rquant.definition_registry import ImmutableDefinitionRegistry
    from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry
    registry = ImmutableDefinitionRegistry(definitions,
        execution_registry=BuiltinStrategyEvaluatorRegistry(producer_commit=value.producer_commit).trusted_executable_registry())
    native = registry.read_strategy_spec(value.strategy.registration_fingerprint, as_of=value.available_at)
    policy = MinuteVisibilityPolicy(policy_id="synthetic-original-fixture-timeline", version=1,
        timestamp_semantics=value.execution_profile.timestamp_semantics,
        market_event_basis="preserved synthetic original envelope event_time",
        market_visibility_basis="preserved synthetic original envelope available_at; modeled, no captured completion receipt",
        candidate_visibility_basis="preserved synthetic original snapshot captured_at",
        constraint_visibility_basis="preserved synthetic original constraint published_at",
        native_definition_basis="original native registry private replay bootstrap; research assumption",
        limitations="Synthetic parity fixture only. No historical real capture or official close claim.")
    origins = tuple(MinuteOriginMaterial(object_key=f"original:{index}",
        content_base64=item.content_base64, content_sha256=item.content_sha256,
        format="parquet" if item.relative_path.endswith((".payload", ".parquet")) else "json")
        for index, item in enumerate(value.materials))
    derivations = tuple(MinuteDerivation(material_path=item.relative_path,
        origin_object_keys=(origin.object_key,), method="retained_research_archive",
        transformation_fingerprint=policy.fingerprint, time_basis="modeled")
        for item, origin in zip(value.materials, origins, strict=True))
    proofs = []
    for item, origin in zip(value.materials, origins, strict=True):
        if item.relative_path.startswith("market/batches/") and item.relative_path.endswith(".json"):
            from rquant.live_contracts import BatchEnvelope
            batch = BatchEnvelope.model_validate_json(item.payload())
            proofs.append(MinutePublicationEvidence(kind="market", sequence=batch.sequence,
                material_path=item.relative_path, origin_object_key=origin.object_key,
                published_at=batch.available_at, time_basis="modeled"))
        elif item.relative_path.startswith("constraint-publications/"):
            from rquant.paper_execution_constraints import PaperExecutionConstraintPointer
            pointer = PaperExecutionConstraintPointer.model_validate_json(item.payload())
            proofs.append(MinutePublicationEvidence(kind="constraint", sequence=pointer.sequence,
                material_path=item.relative_path, origin_object_key=origin.object_key,
                published_at=pointer.published_at, time_basis="modeled"))
        elif item.relative_path.startswith("candidates/generations/"):
            from rquant.strategy_candidate_snapshot import StrategyCandidateSnapshot
            snapshot = StrategyCandidateSnapshot.model_validate_json(item.payload())
            proofs.append(MinutePublicationEvidence(kind="candidate", sequence=snapshot.sequence,
                material_path=item.relative_path, origin_object_key=origin.object_key,
                published_at=snapshot.captured_at, time_basis="modeled"))
    provenance = MinuteProvenance(source_kind="reconstructed",
        capture_lineage=tuple(MinuteCaptureLineage(object_key=x.object_key, content_sha256=x.content_sha256,
            acquisition_commit=value.producer_commit, captured_at=None, timing_evidence_object_key=None)
            for x in origins), publication_evidence=tuple(proofs), extracted_at=NOW, published_at=NOW,
        extractor_code_commit=value.producer_commit, extractor_fingerprint="a" * 64,
        research_code_commit=value.producer_commit, source_index_sha256=reference_sha,
        code_files=(MinuteCodeFile(logical_name="synthetic_current_abi_fixture_reference", content_sha256=reference_sha),),
        replay_start=value.tick_times[0], replay_end=value.tick_times[-1],
        native_definition_replay_available_at=value.available_at, visibility_policy=policy)
    runtime = MinuteRuntimeContent.from_runtime(value)
    work = measure_minute_formal_work(runtime.work, origins=origins, provenance=provenance, derivations=derivations)
    return MinuteSourceContentSeed(runtime=runtime, native_registration=native, wrapper_registration=wrapper,
        provenance=provenance, origin_materials=origins, derivations=derivations,
        formal_work=work, result_budget=value.result_budget)


@pytest.fixture(scope="module")
def template_seed(tmp_path_factory: pytest.TempPathFactory) -> MinuteSourceContentSeed:
    return source_seed(tmp_path_factory.mktemp("minute-formal-seed"))


def exact_freeze(seed: MinuteSourceContentSeed) -> object:
    audit, snapshot = minute_metadata_identities(seed)
    return seed.freeze(audit_run_id=audit.audit_run_id, dataset_snapshot_id=snapshot.snapshot_id)


def extra_origin(seed: MinuteSourceContentSeed, data: bytes, format: str) -> MinuteSourceContentSeed:
    item = MinuteOriginMaterial(object_key="original:additional", content_base64=base64.b64encode(data).decode(),
        content_sha256=hashlib.sha256(data).hexdigest(), format=format)
    provenance = seed.provenance.model_copy(update={"capture_lineage": (*seed.provenance.capture_lineage,
        MinuteCaptureLineage(object_key=item.object_key, content_sha256=item.content_sha256,
            acquisition_commit="b" * 40, captured_at=None, timing_evidence_object_key=None))})
    work = measure_minute_formal_work(seed.runtime.work, origins=(*seed.origin_materials, item),
        provenance=provenance, derivations=seed.derivations)
    return MinuteSourceContentSeed.model_validate(seed.model_dump(mode="python") | {
        "origin_materials": (*seed.origin_materials, item), "provenance": provenance, "formal_work": work})


@pytest.fixture
def metadata(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> DuckDBStore:
    import rquant.storage.duckdb as storage
    monkeypatch.setattr(storage, "_settings", lambda: SimpleNamespace(primary_writer_gate_path=None))
    with DuckDBStore(tmp_path / "metadata.duckdb") as store:
        yield store


def test_seed_excludes_only_two_generated_ids(tmp_path: Path) -> None:
    seed = source_seed(tmp_path)
    frozen = seed.freeze(audit_run_id="one", dataset_snapshot_id="1" * 64)
    other = seed.freeze(audit_run_id="two", dataset_snapshot_id="2" * 64)
    assert frozen.source_content_seed == other.source_content_seed == seed
    assert frozen.full_input_hash != other.full_input_hash
    assert frozen.core_input_hash != other.core_input_hash
    fields = frozen.model_dump(mode="json")
    del fields["runtime"]["audit_run_id"]
    del fields["runtime"]["dataset_snapshot_id"]
    assert fields == seed.model_dump(mode="json")


def test_uninstalled_model_policy_rejected_before_metadata(tmp_path: Path, metadata: DuckDBStore) -> None:
    seed = source_seed(tmp_path)
    with pytest.raises(PermissionError, match="policy"):
        publish_minute_input(seed, metadata_store=metadata, source_path=tmp_path / "source.duckdb",
            receipt_path=tmp_path / "publication.json", catalog=ResearchCatalog(tmp_path / "catalog.sqlite3"),
            lake_root=tmp_path / "lake", installed_policies=(), now=NOW)
    assert not (tmp_path / "source.duckdb").exists()
    assert metadata._conn.execute("SELECT COUNT(*) FROM data_audit_run").fetchone()[0] == 0


def test_captured_without_real_completion_proofs_is_rejected(tmp_path: Path) -> None:
    seed = source_seed(tmp_path)
    data = seed.model_dump(mode="json")
    data["provenance"]["source_kind"] = "captured"
    data["provenance"]["visibility_policy"] = None
    with pytest.raises(ValueError, match="captured"):
        MinuteSourceContentSeed.model_validate_json(json.dumps(data))


@pytest.mark.parametrize("field", ["owner", "extract_time", "raw_commit", "work", "profile", "code_bundle"])
def test_seed_binds_complete_source_fields(template_seed: MinuteSourceContentSeed, field: str) -> None:
    from datetime import timedelta

    data = template_seed.model_dump(mode="json")
    if field == "owner":
        data["runtime"]["owner_id"] = "different-owner"
    elif field == "extract_time":
        data["provenance"]["extracted_at"] = (NOW - timedelta(seconds=1)).isoformat()
    elif field == "raw_commit":
        data["provenance"]["capture_lineage"][0]["acquisition_commit"] = "b" * 40
    elif field == "work":
        data["formal_work"]["origin_physical_rows"] += 1
    elif field == "profile":
        data["runtime"]["execution_profile"]["initial_cash"] = "100001"
    else:
        data["provenance"]["code_files"][0]["content_sha256"] = "b" * 64
    changed = MinuteSourceContentSeed.model_validate_json(json.dumps(data))
    assert changed.seed_hash != template_seed.seed_hash


def test_actual_physical_work_and_exact_capacity(template_seed: MinuteSourceContentSeed) -> None:
    room = MAX_WORK_UNITS - template_seed.formal_work.work_units - 2
    bounded = extra_origin(template_seed, json.dumps([0] * room).encode(), "json")
    assert bounded.formal_work.work_units == MAX_WORK_UNITS
    verify_minute_source_content(exact_freeze(bounded), installed_policies=(bounded.provenance.visibility_policy,))
    with pytest.raises(ValueError, match="20000"):
        extra_origin(template_seed, json.dumps([0] * (room + 1)).encode(), "json")
    understated = template_seed.model_dump(mode="json")
    understated["formal_work"]["origin_physical_rows"] -= 1
    value = MinuteSourceContentSeed.model_validate_json(json.dumps(understated))
    with pytest.raises(PermissionError, match="understates physical work"):
        verify_minute_source_content(exact_freeze(value), installed_policies=(value.provenance.visibility_policy,))


@pytest.mark.parametrize("origin_mib", [4, 7])
def test_complete_receipt_budget_rejected_before_publication(template_seed: MinuteSourceContentSeed,
    tmp_path: Path, metadata: DuckDBStore, origin_mib: int) -> None:
    seed = extra_origin(template_seed, b"x" * (origin_mib * 1024 * 1024), "bytes")
    assert len(seed.model_dump_json().encode()) <= MAX_INPUT_BYTES
    assert len(exact_freeze(seed).model_dump_json().encode()) <= MAX_INPUT_BYTES
    with pytest.raises(PermissionError, match="receipt.*(budget|storage)"):
        publish_minute_input(seed, metadata_store=metadata, source_path=tmp_path / "input.duckdb",
            receipt_path=tmp_path / "publication.json", catalog=ResearchCatalog(tmp_path / "catalog.duckdb"),
            lake_root=tmp_path / "lake", installed_policies=(seed.provenance.visibility_policy,), now=NOW)
    assert not (tmp_path / "input.duckdb").exists()
    assert metadata._conn.execute("SELECT COUNT(*) FROM data_audit_run").fetchone()[0] == 0


@pytest.mark.parametrize("defect", ["extra_table", "extra_column", "extra_row", "empty", "missing_pk", "hash", "duplicate_json"])
def test_exact_single_input_table(template_seed: MinuteSourceContentSeed, defect: str) -> None:
    import duckdb

    frozen = exact_freeze(template_seed)
    with duckdb.connect(":memory:") as connection:
        write_minute_formal_input_table(connection, frozen)
        if defect == "extra_table":
            connection.execute("CREATE TABLE extra (x INT)")
        elif defect == "extra_column":
            connection.execute("ALTER TABLE minute_runtime_replay_input ADD COLUMN extra INT")
        elif defect == "extra_row":
            connection.execute("INSERT INTO minute_runtime_replay_input VALUES (?, ?)", ["b" * 64, frozen.model_dump_json()])
        elif defect == "empty":
            connection.execute("DELETE FROM minute_runtime_replay_input")
        elif defect == "missing_pk":
            connection.execute("ALTER TABLE minute_runtime_replay_input RENAME TO old")
            connection.execute("CREATE TABLE minute_runtime_replay_input AS SELECT * FROM old")
            connection.execute("DROP TABLE old")
        elif defect == "hash":
            connection.execute("UPDATE minute_runtime_replay_input SET input_hash=?", ["b" * 64])
        else:
            payload = frozen.model_dump_json().replace('"contract":', '"contract":"duplicate","contract":', 1)
            connection.execute("UPDATE minute_runtime_replay_input SET payload=?", [payload])
        with pytest.raises((ValueError, PermissionError)):
            read_minute_formal_input_table(connection)


@pytest.mark.parametrize("defect", ["mode", "symlink", "hardlink", "replacement", "bytes", "oversize"])
def test_private_authority_file_identity(tmp_path: Path, defect: str) -> None:
    root = tmp_path / "private"
    root.mkdir(mode=0o700)
    path = root / "receipt.json"
    path.write_bytes(b"original")
    path.chmod(0o600)
    _, reference = _secure_private_bytes(path)
    if defect == "mode":
        path.chmod(0o644)
    elif defect == "symlink":
        target = root / "target.json"
        path.rename(target)
        path.symlink_to(target)
    elif defect == "hardlink":
        os.link(path, root / "alias")
    elif defect == "replacement":
        replacement = root / "replacement"
        replacement.write_bytes(b"original")
        replacement.chmod(0o600)
        os.replace(replacement, path)
    elif defect == "bytes":
        path.write_bytes(b"changed!")
    else:
        with path.open("wb") as stream:
            stream.truncate(MAX_INPUT_BYTES + 1)
    with pytest.raises((PermissionError, OSError)):
        _secure_private_bytes(path, reference)


def test_authority_replacement_during_complete_read(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import rquant.minute_backtest_producer as producer

    root = tmp_path / "private"
    root.mkdir(mode=0o700)
    path = root / "receipt.json"
    path.write_bytes(b"original")
    path.chmod(0o600)
    original_read = producer.os.read
    replaced = False
    def replace_after_read(fd: int, size: int) -> bytes:
        nonlocal replaced
        data = original_read(fd, size)
        if not replaced:
            replaced = True
            replacement = root / "replacement"
            replacement.write_bytes(b"original")
            replacement.chmod(0o600)
            os.replace(replacement, path)
        return data
    monkeypatch.setattr(producer.os, "read", replace_after_read)
    with pytest.raises(PermissionError, match="changed during complete read"):
        _secure_private_bytes(path)


def test_original_publication_proofs_preserve_captured_bytes(template_seed: MinuteSourceContentSeed, tmp_path: Path) -> None:
    from rquant.live_contracts import BatchEnvelope
    from rquant.live_spool import LiveBatchSpool, _SpoolCompletionReceipt

    seed = template_seed
    spool = LiveBatchSpool(tmp_path / "synthetic-original-spool")
    materials = {x.relative_path: x for x in seed.runtime.materials}
    source = spool.source_root / "market_minute.json"
    source.write_bytes(materials["market/sources/market_minute.json"].payload())
    source.chmod(0o600)
    origins = list(seed.origin_materials)
    proofs = []
    completion_times = []
    for proof in seed.provenance.publication_evidence:
        data = proof.model_dump(mode="python") | {"time_basis": "actual"}
        if proof.kind == "market":
            envelope = BatchEnvelope.model_validate_json(materials[proof.material_path].payload())
            payload = materials[proof.material_path.removesuffix(".json") + ".payload"].payload()
            spool.publish(envelope, payload, completion_clock=lambda: envelope.available_at, not_after=envelope.available_at)
            pointer_data = (spool.current_root / "market_minute.json").read_bytes()
            receipt_data = (spool.publication_receipt_root / "market_minute" / f"{envelope.sequence:020d}.json").read_bytes()
            receipt = _SpoolCompletionReceipt.model_validate_json(receipt_data)
            completion_times.append(receipt.completed_at)
            for label, raw in (("pointer", pointer_data), ("completion", receipt_data)):
                key = f"original:{label}:{envelope.sequence}"
                origins.append(MinuteOriginMaterial(object_key=key, content_base64=base64.b64encode(raw).decode(),
                    content_sha256=hashlib.sha256(raw).hexdigest(), format="json"))
                data["pointer_object_key" if label == "pointer" else "completion_receipt_object_key"] = key
        proofs.append(MinutePublicationEvidence.model_validate(data))
    capture_time = max(completion_times)
    evidence_key = next(x.completion_receipt_object_key for x in reversed(proofs) if x.kind == "market")
    provenance = MinuteProvenance.model_validate(seed.provenance.model_dump(mode="python") | {
        "source_kind": "captured", "visibility_policy": None, "publication_evidence": tuple(proofs),
        "capture_lineage": tuple(MinuteCaptureLineage(object_key=x.object_key, content_sha256=x.content_sha256,
            acquisition_commit="b" * 40, captured_at=capture_time, timing_evidence_object_key=evidence_key,
            collector_id="synthetic-original-spool-fixture") for x in origins)})
    derivations = tuple(MinuteDerivation.model_validate(x.model_dump(mode="python") | {
        "method": "identity", "time_basis": "actual"}) for x in seed.derivations)
    work = measure_minute_formal_work(seed.runtime.work, origins=tuple(origins), provenance=provenance, derivations=derivations)
    captured = MinuteSourceContentSeed.model_validate(seed.model_dump(mode="python") | {
        "provenance": provenance, "origin_materials": tuple(origins), "derivations": derivations, "formal_work": work})
    assert [x.payload() for x in captured.runtime.materials] == [x.payload() for x in seed.runtime.materials]
    assert captured.provenance.research_code_commit == "a" * 40
    assert all(x.acquisition_commit == "b" * 40 for x in captured.provenance.capture_lineage)
    verify_minute_source_content(exact_freeze(captured), installed_policies=())
    wrong = captured.model_dump(mode="json")
    first = next(x for x in wrong["provenance"]["publication_evidence"] if x["kind"] == "market")
    first["published_at"] = NOW.isoformat()
    changed = MinuteSourceContentSeed.model_validate_json(json.dumps(wrong))
    with pytest.raises(PermissionError, match="publication time differs"):
        verify_minute_source_content(exact_freeze(changed), installed_policies=())


def test_actual_today_native_registration_keeps_private_historical_bootstrap(template_seed: MinuteSourceContentSeed,
    tmp_path: Path) -> None:
    from datetime import timedelta
    from rquant.definition_registry import ImmutableDefinitionRegistry
    from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry

    value = template_seed.runtime
    plan = plan_builtin_definitions(producer_commit=value.producer_commit)
    for label, at in (("today", NOW), ("future", NOW + timedelta(seconds=1))):
        root = tmp_path / label
        bootstrap_builtin_definitions(root, producer_commit=value.producer_commit,
            registered_at=at, available_at=at, expected_plan_id=plan.plan_id)
        registry = ImmutableDefinitionRegistry(root, execution_registry=BuiltinStrategyEvaluatorRegistry(
            producer_commit=value.producer_commit).trusted_executable_registry())
        native = registry.read_strategy_spec(value.strategy.registration_fingerprint, as_of=at)
        assert native.available_at == at
        data = template_seed.model_dump(mode="python") | {"native_registration": native}
        if label == "future":
            with pytest.raises(ValueError, match="native registration.*actual publication"):
                MinuteSourceContentSeed.model_validate(data)
        else:
            actual = MinuteSourceContentSeed.model_validate(data)
            assert actual.native_registration.available_at > actual.runtime.tick_times[0]
            assert actual.native_registration == native
            assert actual.provenance.native_definition_replay_available_at == value.available_at < actual.runtime.tick_times[0]
            verify_minute_source_content(exact_freeze(actual), installed_policies=(actual.provenance.visibility_policy,))


@pytest.mark.parametrize("format", ["json", "parquet", "sqlite"])
def test_tabular_source_cannot_hide_rows_as_opaque_bytes(template_seed: MinuteSourceContentSeed, tmp_path: Path, format: str) -> None:
    import sqlite3
    from rquant.minute_backtest_producer import _origin_rows

    if format == "sqlite":
        path = tmp_path / "original.sqlite3"
        with sqlite3.connect(path) as connection:
            connection.execute("CREATE TABLE original_rows (x INTEGER)")
            connection.executemany("INSERT INTO original_rows VALUES (?)", ((x,) for x in range(5)))
        data = path.read_bytes()
        origin = MinuteOriginMaterial(object_key="original:sqlite", content_base64=base64.b64encode(data).decode(),
            content_sha256=hashlib.sha256(data).hexdigest(), format="sqlite")
    else:
        origin = next(x for x in template_seed.origin_materials if x.format == format)
    assert _origin_rows(origin) >= 1
    wrong = MinuteOriginMaterial.model_validate(origin.model_dump(mode="python") | {"format": "bytes"})
    with pytest.raises(PermissionError, match="format"):
        _origin_rows(wrong)
