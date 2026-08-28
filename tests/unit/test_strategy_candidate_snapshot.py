from __future__ import annotations

import fcntl
import json
import multiprocessing as mp
import os
import stat
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path
from queue import Empty
from typing import Any

import pytest
from pydantic import ValidationError

import rquant.strategy_candidate_snapshot as snapshot_module
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.strategy_candidate_snapshot import (
    StrategyCandidateAuthorityBinding,
    StrategyCandidatePriceBasis,
    StrategyCandidateRecord,
    StrategyCandidateSnapshot,
    StrategyCandidateSnapshotIntegrityError,
    StrategyCandidateSnapshotPointer,
    StrategyCandidateSnapshotSpool,
    strategy_candidate_schema_fingerprint,
    strategy_candidate_snapshot_content_sha256,
)

COMMIT_A = "a" * 40
HASH_A = "1" * 64
HASH_B = "2" * 64
TRADE_DATE = date(2026, 7, 31)
DECISION_AT = datetime(2026, 7, 31, 1, 25, tzinfo=UTC)
AVAILABLE_AT = datetime(2026, 7, 31, 1, 27, tzinfo=UTC)
CAPTURED_AT = datetime(2026, 7, 31, 1, 28, tzinfo=UTC)
DEFINITION_FINGERPRINT = "4" * 64
EXECUTABLE_FINGERPRINT = "5" * 64
STATIC_FEATURE_SCHEMA = {
    "nested": {"dtype": "object", "semantic": "candidate source metadata"},
    "pool": {"dtype": "string", "semantic": "candidate pool at publication"},
    "t_close": {"dtype": "number", "semantic": "reference close in raw price basis"},
}


def _candidate_schema_fingerprint(
    *,
    strategy_id: str = "n_shape",
    strategy_version: str = "1",
    static_feature_schema: dict[str, dict[str, str]] = STATIC_FEATURE_SCHEMA,
) -> str:
    return strategy_candidate_schema_fingerprint(
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        static_feature_schema=static_feature_schema,
    )


CANDIDATE_SCHEMA_FINGERPRINT = _candidate_schema_fingerprint()


@pytest.mark.parametrize("strategy_version", ("01", "0", "+1", "-1", "", " 1"))
def test_candidate_schema_fingerprint_rejects_noncanonical_strategy_version(
    strategy_version: str,
) -> None:
    with pytest.raises(ValueError, match="strategy_version.*canonical positive integer"):
        strategy_candidate_schema_fingerprint(
            strategy_id="n_shape",
            strategy_version=strategy_version,
            static_feature_schema={
                "pool": {"dtype": "string", "semantic": "candidate pool"},
            },
        )


def _build_strategy_snapshot_with_static_value(
    *,
    dtype: str,
    value: object,
) -> StrategyCandidateSnapshot:
    schema = {"value": {"dtype": dtype, "semantic": "contract test value"}}
    schema_fingerprint = strategy_candidate_schema_fingerprint(
        strategy_id="n_shape",
        strategy_version="1",
        static_feature_schema=schema,
    )
    binding = StrategyCandidateAuthorityBinding.create(
        strategy_id="n_shape",
        strategy_version="1",
        definition_fingerprint=DEFINITION_FINGERPRINT,
        executable_fingerprint=EXECUTABLE_FINGERPRINT,
        candidate_schema_fingerprint=schema_fingerprint,
        static_feature_schema=schema,
    )
    row = _row().model_copy(update={"strategy_version": "1", "static_features": {"value": value}})
    return StrategyCandidateSnapshot.build_strategy(
        sequence=0,
        trade_date=TRADE_DATE,
        captured_at=CAPTURED_AT,
        producer_commit=COMMIT_A,
        authority_binding=binding,
        source_snapshot_ids={"candidate_source": HASH_A},
        rows=(row,),
    )


@pytest.mark.parametrize("dtype", ("float64", "json", "boolean", "NUMBER"))
def test_candidate_schema_rejects_noncanonical_static_dtype(dtype: str) -> None:
    with pytest.raises(ValueError, match="canonical static feature dtype"):
        strategy_candidate_schema_fingerprint(
            strategy_id="n_shape",
            strategy_version="1",
            static_feature_schema={
                "value": {"dtype": dtype, "semantic": "contract test value"},
            },
        )


@pytest.mark.parametrize(
    ("dtype", "value"),
    (
        ("number", True),
        ("number", float("inf")),
        ("integer", True),
        ("integer", 1.5),
        ("string", 1),
        ("bool", 1),
        ("object", []),
        ("array", {}),
        ("null", ""),
    ),
)
def test_strategy_snapshot_rejects_static_value_outside_declared_dtype(
    dtype: str,
    value: object,
) -> None:
    with pytest.raises(ValueError, match="static feature.*dtype"):
        _build_strategy_snapshot_with_static_value(dtype=dtype, value=value)


@pytest.mark.parametrize(
    ("dtype", "value"),
    (
        ("number", 1.5),
        ("number", 1),
        ("integer", 1),
        ("string", "pool1"),
        ("bool", True),
        ("object", {"level": 1}),
        ("array", [1, "two"]),
        ("null", None),
    ),
)
def test_strategy_snapshot_accepts_exact_canonical_static_dtype(
    dtype: str,
    value: object,
) -> None:
    snapshot = _build_strategy_snapshot_with_static_value(dtype=dtype, value=value)

    assert snapshot.rows[0].static_features["value"] is not None or dtype == "null"


def test_legacy_publication_is_only_available_through_migration_namespace(
    tmp_path: Path,
) -> None:
    spool = StrategyCandidateSnapshotSpool((tmp_path / "legacy-migration").resolve())

    assert not hasattr(spool, "publish")
    assert not hasattr(spool, "publish_records")
    result = spool.publish_legacy_records_for_migration(
        trade_date=TRADE_DATE,
        captured_at=CAPTURED_AT,
        producer_commit=COMMIT_A,
        rows=(_row(),),
    )

    assert result.snapshot.schema_version == 2


def _read_binding(
    spool: StrategyCandidateSnapshotSpool,
    *,
    strategy_id: str = "n_shape",
    strategy_version: str = "1",
) -> StrategyCandidateAuthorityBinding:
    return spool.read_authority_binding(
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        definition_fingerprint=DEFINITION_FINGERPRINT,
        executable_fingerprint=EXECUTABLE_FINGERPRINT,
        candidate_schema_fingerprint=_candidate_schema_fingerprint(
            strategy_id=strategy_id,
            strategy_version=strategy_version,
        ),
        static_feature_schema=STATIC_FEATURE_SCHEMA,
    )


def _row(
    *,
    candidate_id: str = "000001.SZ",
    variant: str = "pool1",
    decision_at: datetime = DECISION_AT,
    available_at: datetime = AVAILABLE_AT,
    effective_trade_date: date = TRADE_DATE,
    reference_trade_date: date = date(2026, 7, 30),
    price_basis: StrategyCandidatePriceBasis = StrategyCandidatePriceBasis.QFQ_PIT,
    static_features: dict[str, object] | None = None,
    reference_snapshot_ids: dict[str, str] | None = None,
) -> StrategyCandidateRecord:
    return StrategyCandidateRecord(
        strategy_id="n_shape",
        strategy_version="1",
        candidate_id=candidate_id,
        variant=variant,
        decision_at=decision_at,
        available_at=available_at,
        effective_trade_date=effective_trade_date,
        reference_trade_date=reference_trade_date,
        price_basis=price_basis,
        static_features=static_features
        or {
            "pool": "pool1",
            "t_close": 10.25,
            "nested": {"levels": [10.1, 10.2]},
        },
        reference_snapshot_ids=reference_snapshot_ids
        or {"daily_state": HASH_A, "security_status": HASH_B},
    )


def _snapshot(
    *,
    sequence: int = 0,
    captured_at: datetime = CAPTURED_AT,
    rows: tuple[StrategyCandidateRecord, ...] | None = None,
    producer_commit: str = COMMIT_A,
    trade_date: date = TRADE_DATE,
) -> StrategyCandidateSnapshot:
    return StrategyCandidateSnapshot.build(
        sequence=sequence,
        trade_date=trade_date,
        captured_at=captured_at,
        producer_commit=producer_commit,
        rows=rows or (_row(),),
    )


def _canonical_bytes(model: RuntimeContractModel) -> bytes:
    payload = model.model_dump(mode="json")
    if isinstance(model, StrategyCandidateSnapshot) and model.schema_version in {1, 2}:
        payload.pop("authority_binding")
        payload.pop("source_snapshot_ids")
    return json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _tree_state(root: Path) -> tuple[tuple[str, int, int, bytes], ...]:
    return tuple(
        (
            str(path.relative_to(root)),
            path.lstat().st_mode,
            path.lstat().st_mtime_ns,
            path.read_bytes() if path.is_file() and not path.is_symlink() else b"",
        )
        for path in sorted(root.rglob("*"))
    )


def _publish_records_worker(
    root: str,
    *,
    variant: str,
    captured_at: datetime,
    results: Any,
    block_before_create: bool = False,
    entered_create: Any = None,
    release_create: Any = None,
    verify_lock_contended: Any = None,
) -> None:
    try:
        if verify_lock_contended is not None:
            descriptor = os.open(Path(root) / ".publish.lock", os.O_RDONLY)
            try:
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    verify_lock_contended.set()
                else:
                    raise AssertionError("publish lock was not held by the competing process")
            finally:
                os.close(descriptor)

        if block_before_create:
            original_create = StrategyCandidateSnapshotSpool._atomic_create_generation

            def blocked_create(
                cls: type[StrategyCandidateSnapshotSpool],
                root_fd: int,
                generations_fd: int,
                target_name: str,
                payload: bytes,
            ) -> None:
                entered_create.set()
                if not release_create.wait(timeout=10):
                    raise TimeoutError("generation creation was not released")
                original_create(root_fd, generations_fd, target_name, payload)

            StrategyCandidateSnapshotSpool._atomic_create_generation = classmethod(  # type: ignore[method-assign]
                blocked_create
            )

        result = StrategyCandidateSnapshotSpool(Path(root)).publish_legacy_records_for_migration(
            trade_date=TRADE_DATE,
            captured_at=captured_at,
            producer_commit=COMMIT_A,
            rows=(_row(variant=variant),),
        )
        results.put(
            {
                "error": None,
                "published": result.published,
                "snapshot": result.snapshot.model_dump(mode="json"),
            }
        )
    except BaseException as exc:
        results.put(
            {
                "error": f"{type(exc).__name__}:{exc}",
                "published": None,
                "snapshot": None,
            }
        )


def _publish_strategy_records_worker(
    root: str,
    *,
    strategy_id: str,
    captured_at: datetime,
    results: Any,
    block_stage: str | None = None,
    entered_stage: Any = None,
    release_stage: Any = None,
    verify_lock_contended: Any = None,
) -> None:
    try:
        if verify_lock_contended is not None:
            descriptor = os.open(Path(root) / ".publish.lock", os.O_RDONLY)
            try:
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    verify_lock_contended.set()
                else:
                    raise AssertionError("publish lock was not held by the competing process")
            finally:
                os.close(descriptor)

        if block_stage == "authority":
            original_authority = StrategyCandidateSnapshotSpool._atomic_create_authority_binding

            def blocked_authority(
                cls: type[StrategyCandidateSnapshotSpool],
                root_fd: int,
                payload: bytes,
            ) -> None:
                entered_stage.set()
                if not release_stage.wait(timeout=10):
                    raise TimeoutError("authority creation was not released")
                original_authority(root_fd, payload)

            StrategyCandidateSnapshotSpool._atomic_create_authority_binding = classmethod(  # type: ignore[method-assign]
                blocked_authority
            )
        elif block_stage == "generation":
            original_generation = StrategyCandidateSnapshotSpool._atomic_create_generation

            def blocked_generation(
                cls: type[StrategyCandidateSnapshotSpool],
                root_fd: int,
                generations_fd: int,
                target_name: str,
                payload: bytes,
            ) -> None:
                entered_stage.set()
                if not release_stage.wait(timeout=10):
                    raise TimeoutError("generation creation was not released")
                original_generation(root_fd, generations_fd, target_name, payload)

            StrategyCandidateSnapshotSpool._atomic_create_generation = classmethod(  # type: ignore[method-assign]
                blocked_generation
            )

        result = StrategyCandidateSnapshotSpool(Path(root)).publish_strategy_records(
            strategy_id=strategy_id,
            strategy_version="1",
            definition_fingerprint=DEFINITION_FINGERPRINT,
            executable_fingerprint=EXECUTABLE_FINGERPRINT,
            candidate_schema_fingerprint=_candidate_schema_fingerprint(
                strategy_id=strategy_id,
                strategy_version="1",
            ),
            static_feature_schema=STATIC_FEATURE_SCHEMA,
            source_snapshot_ids={"candidate_input": HASH_A},
            trade_date=TRADE_DATE,
            captured_at=captured_at,
            producer_commit=COMMIT_A,
            rows=(),
        )
        results.put(
            {
                "error": None,
                "published": result.published,
                "snapshot": result.snapshot.model_dump(mode="json"),
            }
        )
    except BaseException as exc:
        results.put(
            {
                "error": f"{type(exc).__name__}:{exc}",
                "published": None,
                "snapshot": None,
            }
        )


def _collect_process_results(processes: list[mp.Process], results: Any) -> list[dict[str, Any]]:
    for process in processes:
        process.join(timeout=15)
        if process.is_alive():
            process.terminate()
            process.join(timeout=2)
            pytest.fail("candidate publish worker timed out")
    try:
        observed = [results.get(timeout=2) for _ in processes]
    except Empty:
        pytest.fail("candidate publish worker did not report a result")
    assert [process.exitcode for process in processes] == [0] * len(processes)
    assert [item["error"] for item in observed] == [None] * len(processes)
    return observed


def _write_legacy_v1_authority(
    root: Path,
    *,
    tamper_variant: bool = False,
) -> tuple[StrategyCandidateSnapshotSpool, str]:
    snapshot = _snapshot()
    hash_identity = snapshot.model_dump(mode="python", exclude={"content_sha256"})
    hash_identity.pop("schema_version", None)
    hash_identity.pop("authority_binding", None)
    hash_identity.pop("source_snapshot_ids", None)
    for row in hash_identity["rows"]:
        row.pop("effective_trade_date")
    generation_sha256 = canonical_sha256(hash_identity)

    payload = snapshot.model_dump(mode="json")
    payload.pop("schema_version", None)
    payload.pop("authority_binding", None)
    payload.pop("source_snapshot_ids", None)
    for row in payload["rows"]:
        row.pop("effective_trade_date")
    payload["content_sha256"] = generation_sha256
    if tamper_variant:
        payload["rows"][0]["variant"] = "forged"

    generations = root / "generations"
    generations.mkdir(parents=True, mode=0o700)
    root.chmod(0o700)
    generations.chmod(0o700)
    lock = root / ".publish.lock"
    lock.touch(mode=0o600)
    lock.chmod(0o600)
    generation = generations / f"{generation_sha256}.json"
    generation.write_bytes(
        json.dumps(
            payload,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    )
    generation.chmod(0o600)
    pointer = {
        "captured_at": payload["captured_at"],
        "generation_sha256": generation_sha256,
        "producer_commit": payload["producer_commit"],
        "sequence": payload["sequence"],
        "trade_date": payload["trade_date"],
    }
    current = root / "current.json"
    current.write_bytes(
        json.dumps(pointer, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode(
            "utf-8"
        )
    )
    current.chmod(0o600)
    return StrategyCandidateSnapshotSpool(root.resolve()), generation_sha256


def _write_legacy_v1_midnight_boundary_authority(
    root: Path,
) -> tuple[StrategyCandidateSnapshotSpool, str, bytes, bytes]:
    decision_at = datetime(2026, 7, 31, 16, 30, tzinfo=UTC)
    available_at = decision_at + timedelta(minutes=1)
    captured_at = decision_at + timedelta(minutes=2)
    trade_date = date(2026, 7, 31)
    row_identity = {
        "strategy_id": "n_shape",
        "strategy_version": "1",
        "candidate_id": "000001.SZ",
        "variant": "pool1",
        "decision_at": decision_at,
        "available_at": available_at,
        "reference_trade_date": trade_date,
        "price_basis": StrategyCandidatePriceBasis.QFQ_PIT,
        "static_features": {"score": 0.8},
        "reference_snapshot_ids": {"daily_state": HASH_A},
    }
    identity = {
        "sequence": 0,
        "trade_date": trade_date,
        "captured_at": captured_at,
        "producer_commit": COMMIT_A,
        "rows": (row_identity,),
    }
    generation_sha256 = canonical_sha256(identity)
    payload = {
        "captured_at": "2026-07-31T16:32:00Z",
        "content_sha256": generation_sha256,
        "producer_commit": COMMIT_A,
        "rows": [
            {
                "available_at": "2026-07-31T16:31:00Z",
                "candidate_id": "000001.SZ",
                "decision_at": "2026-07-31T16:30:00Z",
                "price_basis": "qfq_pit",
                "reference_snapshot_ids": {"daily_state": HASH_A},
                "reference_trade_date": "2026-07-31",
                "static_features": {"score": 0.8},
                "strategy_id": "n_shape",
                "strategy_version": "1",
                "variant": "pool1",
            }
        ],
        "sequence": 0,
        "trade_date": "2026-07-31",
    }
    generation_bytes = json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    pointer = {
        "captured_at": payload["captured_at"],
        "generation_sha256": generation_sha256,
        "producer_commit": COMMIT_A,
        "sequence": 0,
        "trade_date": payload["trade_date"],
    }
    pointer_bytes = json.dumps(
        pointer,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")

    generations = root / "generations"
    generations.mkdir(parents=True, mode=0o700)
    root.chmod(0o700)
    generations.chmod(0o700)
    lock = root / ".publish.lock"
    lock.touch(mode=0o600)
    lock.chmod(0o600)
    generation = generations / f"{generation_sha256}.json"
    generation.write_bytes(generation_bytes)
    generation.chmod(0o600)
    current = root / "current.json"
    current.write_bytes(pointer_bytes)
    current.chmod(0o600)
    return (
        StrategyCandidateSnapshotSpool(root.resolve()),
        generation_sha256,
        generation_bytes,
        pointer_bytes,
    )


def test_all_cross_layer_contracts_inherit_runtime_contract_model() -> None:
    assert issubclass(StrategyCandidateRecord, RuntimeContractModel)
    assert issubclass(StrategyCandidateSnapshot, RuntimeContractModel)
    assert issubclass(StrategyCandidateSnapshotPointer, RuntimeContractModel)


def test_legacy_v1_generation_reads_and_can_advance_to_v2(tmp_path: Path) -> None:
    spool, legacy_hash = _write_legacy_v1_authority(tmp_path / "legacy")

    legacy = spool.read_legacy_for_migration(CAPTURED_AT + timedelta(minutes=1))

    assert legacy is not None
    assert legacy.schema_version == 1
    assert legacy.content_sha256 == legacy_hash
    assert legacy.rows[0].effective_trade_date == legacy.trade_date

    current = _snapshot(
        sequence=1,
        captured_at=CAPTURED_AT + timedelta(minutes=2),
        rows=(
            _row(
                variant="pool2",
                decision_at=CAPTURED_AT + timedelta(minutes=1),
                available_at=CAPTURED_AT + timedelta(minutes=1),
            ),
        ),
    )
    spool.publish_legacy_for_migration(current)

    assert current.schema_version == 2
    assert spool.read_legacy_for_migration(CAPTURED_AT + timedelta(minutes=3)) == current


def test_legacy_v1_generation_tampering_still_fails_closed(tmp_path: Path) -> None:
    spool, _ = _write_legacy_v1_authority(
        tmp_path / "legacy-tampered",
        tamper_variant=True,
    )

    with pytest.raises(StrategyCandidateSnapshotIntegrityError):
        spool.read_legacy_for_migration(CAPTURED_AT + timedelta(minutes=1))


def test_legacy_v1_generation_must_keep_original_canonical_bytes(tmp_path: Path) -> None:
    spool, generation_sha256 = _write_legacy_v1_authority(tmp_path / "legacy-noncanonical")
    generation = spool.generations_root / f"{generation_sha256}.json"
    payload = json.loads(generation.read_bytes())
    generation.write_text(json.dumps(payload, indent=2, sort_keys=True))
    generation.chmod(0o600)

    with pytest.raises(StrategyCandidateSnapshotIntegrityError, match="canonical"):
        spool.read_legacy_for_migration(CAPTURED_AT + timedelta(minutes=1))


def test_legacy_v1_uses_utc_date_semantics_across_shanghai_midnight(tmp_path: Path) -> None:
    spool, generation_sha256, generation_bytes, pointer_bytes = (
        _write_legacy_v1_midnight_boundary_authority(tmp_path / "legacy-midnight")
    )

    snapshot = spool.read_legacy_for_migration(datetime(2026, 7, 31, 16, 33, tzinfo=UTC))

    assert snapshot is not None
    assert snapshot.schema_version == 1
    assert snapshot.trade_date == date(2026, 7, 31)
    assert snapshot.rows[0].effective_trade_date == date(2026, 7, 31)
    assert snapshot.content_sha256 == generation_sha256
    assert (spool.generations_root / f"{generation_sha256}.json").read_bytes() == generation_bytes
    assert spool.current_path.read_bytes() == pointer_bytes

    with pytest.raises(ValidationError, match="effective_trade_date"):
        _row(
            decision_at=datetime(2026, 7, 31, 16, 30, tzinfo=UTC),
            available_at=datetime(2026, 7, 31, 16, 31, tzinfo=UTC),
            effective_trade_date=date(2026, 7, 31),
            reference_trade_date=date(2026, 7, 31),
        )


def test_v2_rejects_legacy_date_marker_and_marker_is_never_serialized() -> None:
    payload = _row().model_dump(mode="python")
    payload["legacy_utc_date_semantics"] = True
    legacy_row = StrategyCandidateRecord.model_validate(payload)

    assert "legacy_utc_date_semantics" not in legacy_row.model_dump(mode="json")
    with pytest.raises(ValidationError, match="legacy"):
        StrategyCandidateSnapshot.build(
            sequence=0,
            trade_date=TRADE_DATE,
            captured_at=CAPTURED_AT,
            producer_commit=COMMIT_A,
            rows=(legacy_row,),
        )


def test_v1_requires_same_utc_decision_date_while_v2_allows_prior_day() -> None:
    decision_at = datetime(2026, 7, 30, 9, 0, tzinfo=UTC)
    v2_row = _row(
        decision_at=decision_at,
        available_at=decision_at + timedelta(minutes=1),
        effective_trade_date=TRADE_DATE,
        reference_trade_date=date(2026, 7, 30),
    )
    legacy_payload = v2_row.model_dump(mode="python")
    legacy_payload["legacy_utc_date_semantics"] = True
    v1_row = StrategyCandidateRecord.model_validate(legacy_payload)
    captured_at = decision_at + timedelta(minutes=2)
    v1_hash = strategy_candidate_snapshot_content_sha256(
        schema_version=1,
        sequence=0,
        trade_date=TRADE_DATE,
        captured_at=captured_at,
        producer_commit=COMMIT_A,
        rows=(v1_row,),
    )

    with pytest.raises(ValidationError, match="decision"):
        StrategyCandidateSnapshot(
            schema_version=1,
            sequence=0,
            trade_date=TRADE_DATE,
            captured_at=captured_at,
            producer_commit=COMMIT_A,
            rows=(v1_row,),
            content_sha256=v1_hash,
        )

    v2 = StrategyCandidateSnapshot.build(
        sequence=0,
        trade_date=TRADE_DATE,
        captured_at=captured_at,
        producer_commit=COMMIT_A,
        rows=(v2_row,),
    )
    assert v2.rows[0].decision_at.date() == date(2026, 7, 30)
    assert v2.trade_date == TRADE_DATE


def test_candidate_normalizes_utc_and_deep_freezes_canonical_mappings() -> None:
    features = {"z": [{"inside": [1, 2]}], "a": True}
    references = {"z_source": HASH_B, "a_source": HASH_A}
    row = _row(
        decision_at=datetime(2026, 7, 31, 9, 25, tzinfo=timezone(timedelta(hours=8))),
        available_at=datetime(2026, 7, 31, 9, 27, tzinfo=timezone(timedelta(hours=8))),
        static_features=features,
        reference_snapshot_ids=references,
    )

    features["z"].append("forged")
    references["a_source"] = HASH_B

    assert row.decision_at == DECISION_AT
    assert row.available_at == AVAILABLE_AT
    assert tuple(row.static_features) == ("a", "z")
    assert row.static_features["z"] == ({"inside": (1, 2)},)
    assert dict(row.reference_snapshot_ids) == {
        "a_source": HASH_A,
        "z_source": HASH_B,
    }
    with pytest.raises(TypeError):
        row.reference_snapshot_ids["forged"] = HASH_A  # type: ignore[index]
    with pytest.raises(TypeError):
        row.static_features["z"][0]["inside"] = (99,)  # type: ignore[index]


def test_candidate_occurrence_id_is_canonical_and_changes_by_effective_trade_date() -> None:
    row = _row()
    next_day = _row(effective_trade_date=TRADE_DATE + timedelta(days=1))

    assert row.occurrence_id == canonical_sha256(
        {
            "strategy_id": "n_shape",
            "strategy_version": "1",
            "candidate_id": "000001.SZ",
            "variant": "pool1",
            "effective_trade_date": TRADE_DATE,
        }
    )
    assert next_day.occurrence_id != row.occurrence_id


def test_candidate_dates_use_asia_shanghai_calendar_day() -> None:
    shanghai_midnight = datetime(
        2026,
        7,
        31,
        0,
        30,
        tzinfo=timezone(timedelta(hours=8)),
    )

    with pytest.raises(ValidationError, match="effective_trade_date"):
        _row(
            decision_at=shanghai_midnight,
            available_at=shanghai_midnight,
            effective_trade_date=date(2026, 7, 30),
            reference_trade_date=date(2026, 7, 30),
        )

    accepted = _row(
        decision_at=shanghai_midnight,
        available_at=shanghai_midnight,
        effective_trade_date=date(2026, 7, 31),
        reference_trade_date=date(2026, 7, 31),
    )
    assert accepted.effective_trade_date == date(2026, 7, 31)


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"decision_at": DECISION_AT.replace(tzinfo=None)}, "timezone-aware"),
        ({"available_at": AVAILABLE_AT.replace(tzinfo=None)}, "timezone-aware"),
        ({"available_at": DECISION_AT - timedelta(seconds=1)}, "available_at"),
        ({"effective_trade_date": TRADE_DATE - timedelta(days=1)}, "effective_trade_date"),
        ({"reference_trade_date": date(2026, 8, 1)}, "future"),
        ({"reference_snapshot_ids": {"daily_state": "BAD"}}, "reference_snapshot_ids"),
    ],
)
def test_candidate_rejects_invalid_time_and_future_references(
    changes: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValidationError, match=message):
        _row(**changes)  # type: ignore[arg-type]


def test_candidate_rejects_non_string_static_feature_keys() -> None:
    with pytest.raises(ValidationError, match="JSON object keys must be strings"):
        _row(static_features={1: "numeric", "1": "text"})  # type: ignore[dict-item]


def test_snapshot_rejects_duplicate_candidates_and_future_rows() -> None:
    row = _row()
    with pytest.raises(ValidationError, match="duplicate candidate"):
        _snapshot(rows=(row, row))

    with pytest.raises(ValidationError, match="duplicate candidate"):
        _snapshot(rows=(row, _row(variant="pool2")))

    future = _row(
        decision_at=CAPTURED_AT,
        available_at=CAPTURED_AT + timedelta(seconds=1),
    )
    with pytest.raises(ValidationError, match="captured_at"):
        _snapshot(rows=(future,))


def test_prior_day_decision_can_publish_for_next_effective_trade_date() -> None:
    decision_at = datetime(2026, 7, 30, 9, 0, tzinfo=UTC)
    row = _row(
        decision_at=decision_at,
        available_at=decision_at + timedelta(minutes=1),
        effective_trade_date=TRADE_DATE,
    )

    snapshot = _snapshot(
        captured_at=decision_at + timedelta(minutes=2),
        rows=(row,),
    )

    assert snapshot.trade_date == TRADE_DATE
    assert snapshot.rows[0].decision_at.date() == TRADE_DATE - timedelta(days=1)
    assert snapshot.rows[0].effective_trade_date == TRADE_DATE


def test_snapshot_rejects_row_for_another_effective_trade_date() -> None:
    with pytest.raises(ValidationError, match="effective_trade_date"):
        _snapshot(rows=(_row(effective_trade_date=TRADE_DATE + timedelta(days=1)),))


@pytest.mark.parametrize(
    "change",
    [
        {"sequence": 1},
        {"captured_at": CAPTURED_AT + timedelta(minutes=1)},
        {"producer_commit": "b" * 40},
        {"rows": (_row(variant="pool2"),)},
        {"rows": (_row(static_features={"score": 0.9}),)},
        {"rows": (_row(reference_snapshot_ids={"daily_state": HASH_B}),)},
        {
            "trade_date": TRADE_DATE + timedelta(days=1),
            "rows": (_row(effective_trade_date=TRADE_DATE + timedelta(days=1)),),
        },
    ],
)
def test_snapshot_hash_binds_every_content_dimension(change: dict[str, object]) -> None:
    baseline = _snapshot()
    changed = _snapshot(**change)  # type: ignore[arg-type]
    assert changed.content_sha256 != baseline.content_sha256


def test_snapshot_rejects_supplied_hash_that_does_not_bind_content() -> None:
    valid = _snapshot()
    payload = valid.model_dump(mode="python")
    payload["content_sha256"] = "f" * 64

    with pytest.raises(ValidationError, match="content_sha256"):
        StrategyCandidateSnapshot.model_validate(payload)


def test_publish_is_atomic_private_immutable_and_idempotent(tmp_path: Path) -> None:
    root = tmp_path / "candidate-spool"
    spool = StrategyCandidateSnapshotSpool(root.resolve())
    snapshot = _snapshot()

    first = spool.publish_legacy_for_migration(snapshot)
    first_generation = spool.generations_root / f"{snapshot.content_sha256}.json"
    generation_bytes = first_generation.read_bytes()
    second = spool.publish_legacy_for_migration(snapshot)

    assert first == snapshot
    assert second == snapshot
    assert generation_bytes == first_generation.read_bytes() == _canonical_bytes(snapshot)
    assert stat.S_IMODE(root.stat().st_mode) == 0o700
    assert stat.S_IMODE(spool.generations_root.stat().st_mode) == 0o700
    assert stat.S_IMODE(first_generation.stat().st_mode) == 0o600
    assert stat.S_IMODE(spool.current_path.stat().st_mode) == 0o600
    assert list(spool.generations_root.glob("*.json")) == [first_generation]


def test_publish_records_returns_frozen_result_and_suppresses_same_semantics(
    tmp_path: Path,
) -> None:
    spool = StrategyCandidateSnapshotSpool((tmp_path / "spool").resolve())

    first = spool.publish_legacy_records_for_migration(
        trade_date=TRADE_DATE,
        captured_at=CAPTURED_AT,
        producer_commit=COMMIT_A,
        rows=(_row(),),
    )
    duplicate = spool.publish_legacy_records_for_migration(
        trade_date=TRADE_DATE,
        captured_at=CAPTURED_AT + timedelta(seconds=1),
        producer_commit=COMMIT_A,
        rows=(_row(),),
    )

    assert isinstance(first, snapshot_module.StrategyCandidatePublishResult)
    assert first.published is True
    assert duplicate.published is False
    assert duplicate.snapshot == first.snapshot
    assert duplicate.snapshot.captured_at == CAPTURED_AT
    assert first.snapshot.sequence == 0
    assert first.snapshot.schema_version == 2
    assert first.snapshot.content_sha256 == strategy_candidate_snapshot_content_sha256(
        schema_version=2,
        sequence=0,
        trade_date=TRADE_DATE,
        captured_at=CAPTURED_AT,
        producer_commit=COMMIT_A,
        rows=(_row(),),
    )
    assert StrategyCandidateSnapshotPointer.model_validate_json(
        spool.current_path.read_bytes()
    ) == StrategyCandidateSnapshotPointer.from_snapshot(first.snapshot)
    assert len(list(spool.generations_root.glob("*.json"))) == 1
    with pytest.raises(ValidationError, match="frozen"):
        first.published = False  # type: ignore[misc]


def test_publish_strategy_records_persists_and_enforces_root_identity(tmp_path: Path) -> None:
    root = (tmp_path / "spool").resolve()
    spool = StrategyCandidateSnapshotSpool(root)

    result = spool.publish_strategy_records(
        strategy_id="n_shape",
        strategy_version="1",
        definition_fingerprint=DEFINITION_FINGERPRINT,
        executable_fingerprint=EXECUTABLE_FINGERPRINT,
        candidate_schema_fingerprint=CANDIDATE_SCHEMA_FINGERPRINT,
        static_feature_schema=STATIC_FEATURE_SCHEMA,
        source_snapshot_ids={"candidate_input": "3" * 64},
        trade_date=TRADE_DATE,
        captured_at=CAPTURED_AT,
        producer_commit=COMMIT_A,
        rows=(_row(),),
    )
    binding = _read_binding(spool)
    before = _tree_state(root)

    assert result.published is True
    assert binding == StrategyCandidateAuthorityBinding.create(
        strategy_id="n_shape",
        strategy_version="1",
        definition_fingerprint=DEFINITION_FINGERPRINT,
        executable_fingerprint=EXECUTABLE_FINGERPRINT,
        candidate_schema_fingerprint=CANDIDATE_SCHEMA_FINGERPRINT,
        static_feature_schema=STATIC_FEATURE_SCHEMA,
    )
    assert binding.content_sha256 == canonical_sha256(
        binding.model_dump(mode="python", exclude={"content_sha256"})
    )
    assert set(json.loads((root / "authority.json").read_text())) == {
        "schema_version",
        "strategy_id",
        "strategy_version",
        "definition_fingerprint",
        "executable_fingerprint",
        "candidate_schema_fingerprint",
        "static_feature_names",
        "static_feature_schema",
        "content_sha256",
    }
    assert stat.S_IMODE((root / "authority.json").stat().st_mode) == 0o600
    with pytest.raises(StrategyCandidateSnapshotIntegrityError, match="identity|bound"):
        spool.publish_strategy_records(
            strategy_id="growth_board_surge",
            strategy_version="1",
            definition_fingerprint=DEFINITION_FINGERPRINT,
            executable_fingerprint=EXECUTABLE_FINGERPRINT,
            candidate_schema_fingerprint=_candidate_schema_fingerprint(
                strategy_id="growth_board_surge"
            ),
            static_feature_schema=STATIC_FEATURE_SCHEMA,
            source_snapshot_ids={"candidate_input": "3" * 64},
            trade_date=TRADE_DATE,
            captured_at=CAPTURED_AT + timedelta(seconds=1),
            producer_commit=COMMIT_A,
            rows=(),
        )
    assert _tree_state(root) == before


def test_empty_strategy_authority_binds_complete_static_feature_schema(tmp_path: Path) -> None:
    root = (tmp_path / "empty-schema-bound").resolve()
    fingerprint = _candidate_schema_fingerprint()

    result = StrategyCandidateSnapshotSpool(root).publish_strategy_records(
        strategy_id="n_shape",
        strategy_version="1",
        definition_fingerprint=DEFINITION_FINGERPRINT,
        executable_fingerprint=EXECUTABLE_FINGERPRINT,
        candidate_schema_fingerprint=fingerprint,
        static_feature_schema=STATIC_FEATURE_SCHEMA,
        source_snapshot_ids={"candidate_input": "3" * 64},
        trade_date=TRADE_DATE,
        captured_at=CAPTURED_AT,
        producer_commit=COMMIT_A,
        rows=(),
    )

    binding = result.snapshot.authority_binding
    assert binding is not None
    assert binding.static_feature_names == tuple(sorted(STATIC_FEATURE_SCHEMA))
    assert {
        name: semantic.model_dump(mode="json")
        for name, semantic in binding.static_feature_schema.items()
    } == STATIC_FEATURE_SCHEMA
    assert binding.candidate_schema_fingerprint == fingerprint


def test_strategy_authority_rejects_schema_fingerprint_not_matching_semantics(
    tmp_path: Path,
) -> None:
    with pytest.raises((ValueError, ValidationError), match="candidate schema fingerprint"):
        StrategyCandidateSnapshotSpool(
            (tmp_path / "schema-fingerprint-mismatch").resolve()
        ).publish_strategy_records(
            strategy_id="n_shape",
            strategy_version="1",
            definition_fingerprint=DEFINITION_FINGERPRINT,
            executable_fingerprint=EXECUTABLE_FINGERPRINT,
            candidate_schema_fingerprint="f" * 64,
            static_feature_schema=STATIC_FEATURE_SCHEMA,
            source_snapshot_ids={"candidate_input": "3" * 64},
            trade_date=TRADE_DATE,
            captured_at=CAPTURED_AT,
            producer_commit=COMMIT_A,
            rows=(),
        )


def test_generic_read_rejects_strategy_bound_authority(tmp_path: Path) -> None:
    spool = StrategyCandidateSnapshotSpool((tmp_path / "bound-generic-read").resolve())
    fingerprint = _candidate_schema_fingerprint()
    spool.publish_strategy_records(
        strategy_id="n_shape",
        strategy_version="1",
        definition_fingerprint=DEFINITION_FINGERPRINT,
        executable_fingerprint=EXECUTABLE_FINGERPRINT,
        candidate_schema_fingerprint=fingerprint,
        static_feature_schema=STATIC_FEATURE_SCHEMA,
        source_snapshot_ids={"candidate_input": "3" * 64},
        trade_date=TRADE_DATE,
        captured_at=CAPTURED_AT,
        producer_commit=COMMIT_A,
        rows=(_row(),),
    )

    with pytest.raises(StrategyCandidateSnapshotIntegrityError, match="strategy-aware"):
        spool.read_as_of(CAPTURED_AT + timedelta(seconds=1))


def test_generic_read_rejects_legacy_authority_without_migration_mode(tmp_path: Path) -> None:
    spool = StrategyCandidateSnapshotSpool((tmp_path / "legacy-generic-read").resolve())
    spool.publish_legacy_records_for_migration(
        trade_date=TRADE_DATE,
        captured_at=CAPTURED_AT,
        producer_commit=COMMIT_A,
        rows=(_row(),),
    )

    with pytest.raises(StrategyCandidateSnapshotIntegrityError, match="migration|republication"):
        spool.read_as_of(CAPTURED_AT + timedelta(seconds=1))


def test_strategy_authority_v3_binds_definition_executable_and_candidate_schema(
    tmp_path: Path,
) -> None:
    root = (tmp_path / "spool-v3").resolve()

    result = StrategyCandidateSnapshotSpool(root).publish_strategy_records(
        strategy_id="n_shape",
        strategy_version="1",
        definition_fingerprint=DEFINITION_FINGERPRINT,
        executable_fingerprint=EXECUTABLE_FINGERPRINT,
        candidate_schema_fingerprint=CANDIDATE_SCHEMA_FINGERPRINT,
        static_feature_schema=STATIC_FEATURE_SCHEMA,
        source_snapshot_ids={"candidate_input": "3" * 64},
        trade_date=TRADE_DATE,
        captured_at=CAPTURED_AT,
        producer_commit=COMMIT_A,
        rows=(_row(),),
    )

    binding = _read_binding(StrategyCandidateSnapshotSpool(root))
    assert result.snapshot.authority_binding == binding
    assert binding.schema_version == 3
    assert binding.definition_fingerprint == DEFINITION_FINGERPRINT
    assert binding.executable_fingerprint == EXECUTABLE_FINGERPRINT
    assert binding.candidate_schema_fingerprint == CANDIDATE_SCHEMA_FINGERPRINT


def test_strategy_aware_publish_rejects_legacy_unfingerprinted_binding(tmp_path: Path) -> None:
    with pytest.raises((TypeError, ValueError, ValidationError), match="fingerprint|required"):
        StrategyCandidateSnapshotSpool(
            (tmp_path / "legacy-strategy-authority").resolve()
        ).publish_strategy_records(
            strategy_id="n_shape",
            strategy_version="1",
            source_snapshot_ids={"candidate_input": "3" * 64},
            trade_date=TRADE_DATE,
            captured_at=CAPTURED_AT,
            producer_commit=COMMIT_A,
            rows=(_row(),),
        )


def test_strategy_authority_rejects_partial_static_semantic_binding(tmp_path: Path) -> None:
    with pytest.raises((TypeError, ValueError, ValidationError), match="together|fingerprint"):
        StrategyCandidateSnapshotSpool(
            (tmp_path / "partial-v2").resolve()
        ).publish_strategy_records(
            strategy_id="n_shape",
            strategy_version="1",
            definition_fingerprint="4" * 64,
            source_snapshot_ids={"candidate_input": "3" * 64},
            trade_date=TRADE_DATE,
            captured_at=CAPTURED_AT,
            producer_commit=COMMIT_A,
            rows=(_row(),),
        )


def test_publish_strategy_records_refuses_to_claim_legacy_generations(tmp_path: Path) -> None:
    spool = StrategyCandidateSnapshotSpool((tmp_path / "spool").resolve())
    spool.publish_legacy_records_for_migration(
        trade_date=TRADE_DATE,
        captured_at=CAPTURED_AT,
        producer_commit=COMMIT_A,
        rows=(_row(),),
    )
    before = _tree_state(spool.root)

    with pytest.raises(StrategyCandidateSnapshotIntegrityError, match="legacy|unbound"):
        spool.publish_strategy_records(
            strategy_id="n_shape",
            strategy_version="1",
            definition_fingerprint=DEFINITION_FINGERPRINT,
            executable_fingerprint=EXECUTABLE_FINGERPRINT,
            candidate_schema_fingerprint=CANDIDATE_SCHEMA_FINGERPRINT,
            static_feature_schema=STATIC_FEATURE_SCHEMA,
            source_snapshot_ids={"candidate_input": "3" * 64},
            trade_date=TRADE_DATE,
            captured_at=CAPTURED_AT + timedelta(seconds=1),
            producer_commit=COMMIT_A,
            rows=(_row(),),
        )

    assert _tree_state(spool.root) == before


def test_bound_generation_blocks_generic_publish_after_binding_is_deleted(
    tmp_path: Path,
) -> None:
    spool = StrategyCandidateSnapshotSpool((tmp_path / "spool").resolve())
    first = spool.publish_strategy_records(
        strategy_id="n_shape",
        strategy_version="1",
        definition_fingerprint=DEFINITION_FINGERPRINT,
        executable_fingerprint=EXECUTABLE_FINGERPRINT,
        candidate_schema_fingerprint=CANDIDATE_SCHEMA_FINGERPRINT,
        static_feature_schema=STATIC_FEATURE_SCHEMA,
        source_snapshot_ids={"candidate_input": "3" * 64},
        trade_date=TRADE_DATE,
        captured_at=CAPTURED_AT,
        producer_commit=COMMIT_A,
        rows=(_row(),),
    )
    spool.authority_path.unlink()
    before = _tree_state(spool.root)

    with pytest.raises(StrategyCandidateSnapshotIntegrityError, match="bound|schema v3"):
        spool.publish_legacy_records_for_migration(
            trade_date=TRADE_DATE,
            captured_at=CAPTURED_AT + timedelta(seconds=1),
            producer_commit=COMMIT_A,
            rows=(_row(variant="pool2"),),
        )

    assert first.snapshot.schema_version == 3
    assert first.snapshot.authority_binding is not None
    assert first.snapshot.authority_binding.strategy_id == "n_shape"
    assert dict(first.snapshot.source_snapshot_ids) == {"candidate_input": "3" * 64}
    assert _tree_state(spool.root) == before


@pytest.mark.parametrize(
    ("strategy_id", "strategy_version"),
    [("", "1"), ("n_shape", ""), ("bad/name", "1")],
)
def test_publish_strategy_records_rejects_invalid_identity_without_creating_authority(
    tmp_path: Path,
    strategy_id: str,
    strategy_version: str,
) -> None:
    root = (tmp_path / "spool").resolve()
    spool = StrategyCandidateSnapshotSpool(root)

    with pytest.raises((TypeError, ValueError, ValidationError)):
        spool.publish_strategy_records(
            strategy_id=strategy_id,
            strategy_version=strategy_version,
            source_snapshot_ids={"candidate_input": "3" * 64},
            trade_date=TRADE_DATE,
            captured_at=CAPTURED_AT,
            producer_commit=COMMIT_A,
            rows=(_row(),),
        )

    assert not root.exists()


@pytest.mark.parametrize(
    "rows",
    [
        "not-records",
        {"candidate_id": "000001.SZ"},
        True,
        [_row().model_dump(mode="python")],
        iter((_row(),)),
    ],
    ids=["str", "bare-dict", "bool", "dict-item", "iterator"],
)
def test_publish_records_requires_a_sequence_of_typed_records(
    tmp_path: Path,
    rows: object,
) -> None:
    root = (tmp_path / "spool").resolve()
    spool = StrategyCandidateSnapshotSpool(root)

    with pytest.raises(TypeError, match=r"Sequence\[StrategyCandidateRecord\]"):
        spool.publish_legacy_records_for_migration(
            trade_date=TRADE_DATE,
            captured_at=CAPTURED_AT,
            producer_commit=COMMIT_A,
            rows=rows,  # type: ignore[arg-type]
        )

    assert not root.exists()


@pytest.mark.parametrize(
    "changes",
    [
        {"producer_commit": "bad"},
        {"trade_date": object()},
        {"captured_at": AVAILABLE_AT - timedelta(seconds=1)},
        {"rows": (_row(effective_trade_date=TRADE_DATE + timedelta(days=1)),)},
    ],
    ids=["commit", "trade-date", "captured-at", "snapshot-constraint"],
)
def test_publish_records_rejects_invalid_request_without_creating_authority(
    tmp_path: Path,
    changes: dict[str, object],
) -> None:
    root = (tmp_path / "spool").resolve()
    spool = StrategyCandidateSnapshotSpool(root)
    arguments: dict[str, object] = {
        "trade_date": TRADE_DATE,
        "captured_at": CAPTURED_AT,
        "producer_commit": COMMIT_A,
        "rows": (_row(),),
    }
    arguments.update(changes)

    with pytest.raises((TypeError, ValueError, ValidationError)):
        spool.publish_legacy_records_for_migration(**arguments)  # type: ignore[arg-type]

    assert not root.exists()


@pytest.mark.parametrize(
    ("producer_commit", "trade_date", "rows"),
    [
        ("b" * 40, TRADE_DATE, (_row(),)),
        (COMMIT_A, TRADE_DATE, (_row(variant="pool2"),)),
        (COMMIT_A, TRADE_DATE, (_row(static_features={"score": 0.9}),)),
        (
            COMMIT_A,
            TRADE_DATE,
            (_row(decision_at=DECISION_AT - timedelta(minutes=1)),),
        ),
        (
            COMMIT_A,
            TRADE_DATE,
            (_row(reference_snapshot_ids={"daily_state": "3" * 64}),),
        ),
        (
            COMMIT_A,
            TRADE_DATE + timedelta(days=1),
            (_row(effective_trade_date=TRADE_DATE + timedelta(days=1)),),
        ),
    ],
    ids=["commit", "business", "static", "pit", "lineage", "trade-date"],
)
def test_publish_records_publishes_every_semantic_change(
    tmp_path: Path,
    producer_commit: str,
    trade_date: date,
    rows: tuple[StrategyCandidateRecord, ...],
) -> None:
    spool = StrategyCandidateSnapshotSpool((tmp_path / "spool").resolve())
    initial = spool.publish_legacy_records_for_migration(
        trade_date=TRADE_DATE,
        captured_at=CAPTURED_AT,
        producer_commit=COMMIT_A,
        rows=(_row(),),
    )

    changed = spool.publish_legacy_records_for_migration(
        trade_date=trade_date,
        captured_at=CAPTURED_AT + timedelta(minutes=1),
        producer_commit=producer_commit,
        rows=rows,
    )

    assert initial.published is True
    assert changed.published is True
    assert changed.snapshot.sequence == 1
    assert changed.snapshot.content_sha256 != initial.snapshot.content_sha256
    assert StrategyCandidateSnapshotPointer.model_validate_json(
        spool.current_path.read_bytes()
    ) == StrategyCandidateSnapshotPointer.from_snapshot(changed.snapshot)
    assert spool.read_legacy_for_migration(CAPTURED_AT + timedelta(minutes=2)) == changed.snapshot
    assert len(list(spool.generations_root.glob("*.json"))) == 2


@pytest.mark.parametrize("variant", ["pool1", "pool2"], ids=["duplicate", "changed"])
def test_publish_records_rejects_backwards_capture_before_semantic_suppression(
    tmp_path: Path,
    variant: str,
) -> None:
    spool = StrategyCandidateSnapshotSpool((tmp_path / "spool").resolve())
    current = spool.publish_legacy_records_for_migration(
        trade_date=TRADE_DATE,
        captured_at=CAPTURED_AT + timedelta(minutes=5),
        producer_commit=COMMIT_A,
        rows=(_row(),),
    )
    before = _tree_state(spool.root)

    with pytest.raises(StrategyCandidateSnapshotIntegrityError, match="backwards"):
        spool.publish_legacy_records_for_migration(
            trade_date=TRADE_DATE,
            captured_at=CAPTURED_AT + timedelta(minutes=4),
            producer_commit=COMMIT_A,
            rows=(_row(variant=variant),),
        )

    assert _tree_state(spool.root) == before
    assert spool.read_legacy_for_migration(CAPTURED_AT + timedelta(minutes=6)) == current.snapshot


@pytest.mark.parametrize("preinitialized", [False, True], ids=["cold", "initialized"])
def test_concurrent_identical_publish_records_creates_one_generation(
    tmp_path: Path,
    preinitialized: bool,
) -> None:
    root = (tmp_path / "spool").resolve()
    spool = StrategyCandidateSnapshotSpool(root)
    if preinitialized:
        baseline = spool.publish_legacy_records_for_migration(
            trade_date=TRADE_DATE,
            captured_at=CAPTURED_AT - timedelta(minutes=1),
            producer_commit=COMMIT_A,
            rows=(_row(variant="baseline"),),
        )
        assert baseline.snapshot.sequence == 0

    ctx = mp.get_context("spawn")
    results = ctx.Queue()
    entered_create = ctx.Event()
    release_create = ctx.Event()
    lock_contended = ctx.Event()
    first = ctx.Process(
        target=_publish_records_worker,
        kwargs={
            "root": str(root),
            "variant": "pool1",
            "captured_at": CAPTURED_AT,
            "results": results,
            "block_before_create": True,
            "entered_create": entered_create,
            "release_create": release_create,
        },
    )
    second = ctx.Process(
        target=_publish_records_worker,
        kwargs={
            "root": str(root),
            "variant": "pool1",
            "captured_at": CAPTURED_AT + timedelta(seconds=1),
            "results": results,
            "verify_lock_contended": lock_contended,
        },
    )

    first.start()
    assert entered_create.wait(timeout=10), "first publisher did not reach generation creation"
    second.start()
    try:
        assert lock_contended.wait(timeout=10), "second publisher did not contend on publish lock"
    finally:
        release_create.set()
    observed = _collect_process_results([first, second], results)
    snapshots = [StrategyCandidateSnapshot.model_validate(item["snapshot"]) for item in observed]

    assert sorted(item["published"] for item in observed) == [False, True]
    assert len({snapshot.content_sha256 for snapshot in snapshots}) == 1
    assert {snapshot.sequence for snapshot in snapshots} == {int(preinitialized)}
    assert {snapshot.captured_at for snapshot in snapshots} == {CAPTURED_AT}
    assert len(list(spool.generations_root.glob("*.json"))) == 1 + int(preinitialized)
    assert spool.read_legacy_for_migration(CAPTURED_AT + timedelta(seconds=2)) == snapshots[0]


def test_concurrent_different_publish_records_allocate_consecutive_sequences(
    tmp_path: Path,
) -> None:
    root = (tmp_path / "spool").resolve()
    spool = StrategyCandidateSnapshotSpool(root)
    ctx = mp.get_context("spawn")
    results = ctx.Queue()
    entered_create = ctx.Event()
    release_create = ctx.Event()
    lock_contended = ctx.Event()
    first = ctx.Process(
        target=_publish_records_worker,
        kwargs={
            "root": str(root),
            "variant": "pool1",
            "captured_at": CAPTURED_AT,
            "results": results,
            "block_before_create": True,
            "entered_create": entered_create,
            "release_create": release_create,
        },
    )
    second = ctx.Process(
        target=_publish_records_worker,
        kwargs={
            "root": str(root),
            "variant": "pool2",
            "captured_at": CAPTURED_AT + timedelta(seconds=1),
            "results": results,
            "verify_lock_contended": lock_contended,
        },
    )

    first.start()
    assert entered_create.wait(timeout=10), "first publisher did not reach generation creation"
    second.start()
    try:
        assert lock_contended.wait(timeout=10), "second publisher did not contend on publish lock"
    finally:
        release_create.set()
    observed = _collect_process_results([first, second], results)
    snapshots = [StrategyCandidateSnapshot.model_validate(item["snapshot"]) for item in observed]

    assert [item["published"] for item in observed] == [True, True]
    assert sorted(snapshot.sequence for snapshot in snapshots) == [0, 1]
    assert len({snapshot.content_sha256 for snapshot in snapshots}) == 2
    assert len(list(spool.generations_root.glob("*.json"))) == 2
    current = spool.read_legacy_for_migration(CAPTURED_AT + timedelta(seconds=2))
    assert current is not None
    assert current.sequence == 1
    assert current.rows[0].variant == "pool2"


def test_concurrent_identical_strategy_publish_creates_one_bound_generation(
    tmp_path: Path,
) -> None:
    root = (tmp_path / "spool").resolve()
    ctx = mp.get_context("spawn")
    results = ctx.Queue()
    entered_stage = ctx.Event()
    release_stage = ctx.Event()
    lock_contended = ctx.Event()
    first = ctx.Process(
        target=_publish_strategy_records_worker,
        kwargs={
            "root": str(root),
            "strategy_id": "n_shape",
            "captured_at": CAPTURED_AT,
            "results": results,
            "block_stage": "generation",
            "entered_stage": entered_stage,
            "release_stage": release_stage,
        },
    )
    second = ctx.Process(
        target=_publish_strategy_records_worker,
        kwargs={
            "root": str(root),
            "strategy_id": "n_shape",
            "captured_at": CAPTURED_AT + timedelta(seconds=1),
            "results": results,
            "verify_lock_contended": lock_contended,
        },
    )

    first.start()
    assert entered_stage.wait(timeout=10), "first publisher did not reach generation creation"
    second.start()
    try:
        assert lock_contended.wait(timeout=10), "second publisher did not contend on publish lock"
    finally:
        release_stage.set()
    observed = _collect_process_results([first, second], results)
    snapshots = [StrategyCandidateSnapshot.model_validate(item["snapshot"]) for item in observed]

    assert sorted(item["published"] for item in observed) == [False, True]
    assert len({snapshot.content_sha256 for snapshot in snapshots}) == 1
    assert {snapshot.schema_version for snapshot in snapshots} == {3}
    assert len(list((root / "generations").glob("*.json"))) == 1
    assert (
        StrategyCandidateSnapshotSpool(root).read_strategy_as_of(
            CAPTURED_AT + timedelta(seconds=2),
            strategy_id="n_shape",
            strategy_version="1",
            definition_fingerprint=DEFINITION_FINGERPRINT,
            executable_fingerprint=EXECUTABLE_FINGERPRINT,
            candidate_schema_fingerprint=_candidate_schema_fingerprint(strategy_version="1"),
            static_feature_schema=STATIC_FEATURE_SCHEMA,
        )
        == snapshots[0]
    )


def test_concurrent_different_strategies_cannot_both_bind_empty_root(tmp_path: Path) -> None:
    root = (tmp_path / "spool").resolve()
    ctx = mp.get_context("spawn")
    results = ctx.Queue()
    entered_stage = ctx.Event()
    release_stage = ctx.Event()
    lock_contended = ctx.Event()
    first = ctx.Process(
        target=_publish_strategy_records_worker,
        kwargs={
            "root": str(root),
            "strategy_id": "n_shape",
            "captured_at": CAPTURED_AT,
            "results": results,
            "block_stage": "authority",
            "entered_stage": entered_stage,
            "release_stage": release_stage,
        },
    )
    second = ctx.Process(
        target=_publish_strategy_records_worker,
        kwargs={
            "root": str(root),
            "strategy_id": "auction_gap",
            "captured_at": CAPTURED_AT + timedelta(seconds=1),
            "results": results,
            "verify_lock_contended": lock_contended,
        },
    )

    first.start()
    assert entered_stage.wait(timeout=10), "first publisher did not reach authority creation"
    second.start()
    try:
        assert lock_contended.wait(timeout=10), "second publisher did not contend on publish lock"
    finally:
        release_stage.set()
    for process in (first, second):
        process.join(timeout=15)
        assert process.exitcode == 0
    observed = [results.get(timeout=2), results.get(timeout=2)]

    assert sum(item["error"] is None for item in observed) == 1
    assert any("different identity" in str(item["error"]) for item in observed)
    binding = _read_binding(
        StrategyCandidateSnapshotSpool(root),
        strategy_version="1",
    )
    assert binding.strategy_id == "n_shape"
    assert len(list((root / "generations").glob("*.json"))) == 1


def test_strategy_publish_retries_after_binding_created_before_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spool = StrategyCandidateSnapshotSpool((tmp_path / "spool").resolve())
    original_create = StrategyCandidateSnapshotSpool._atomic_create_generation

    def fail_generation(cls: type[StrategyCandidateSnapshotSpool], *args: object) -> None:
        raise RuntimeError("simulated generation interruption")

    monkeypatch.setattr(
        StrategyCandidateSnapshotSpool,
        "_atomic_create_generation",
        classmethod(fail_generation),
    )
    with pytest.raises(RuntimeError, match="generation interruption"):
        spool.publish_strategy_records(
            strategy_id="n_shape",
            strategy_version="1",
            definition_fingerprint=DEFINITION_FINGERPRINT,
            executable_fingerprint=EXECUTABLE_FINGERPRINT,
            candidate_schema_fingerprint=_candidate_schema_fingerprint(strategy_version="1"),
            static_feature_schema=STATIC_FEATURE_SCHEMA,
            source_snapshot_ids={"candidate_input": HASH_A},
            trade_date=TRADE_DATE,
            captured_at=CAPTURED_AT,
            producer_commit=COMMIT_A,
            rows=(),
        )
    assert _read_binding(spool, strategy_version="1").strategy_id == "n_shape"
    assert list(spool.generations_root.glob("*.json")) == []
    monkeypatch.setattr(
        StrategyCandidateSnapshotSpool,
        "_atomic_create_generation",
        original_create,
    )

    recovered = spool.publish_strategy_records(
        strategy_id="n_shape",
        strategy_version="1",
        definition_fingerprint=DEFINITION_FINGERPRINT,
        executable_fingerprint=EXECUTABLE_FINGERPRINT,
        candidate_schema_fingerprint=_candidate_schema_fingerprint(strategy_version="1"),
        static_feature_schema=STATIC_FEATURE_SCHEMA,
        source_snapshot_ids={"candidate_input": HASH_A},
        trade_date=TRADE_DATE,
        captured_at=CAPTURED_AT + timedelta(seconds=1),
        producer_commit=COMMIT_A,
        rows=(),
    )

    assert recovered.published is True
    assert recovered.snapshot.sequence == 0
    assert recovered.snapshot.schema_version == 3
    assert recovered.snapshot.source_snapshot_ids == {"candidate_input": HASH_A}


def test_strategy_publish_recovers_generation_before_pointer_without_lineage_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spool = StrategyCandidateSnapshotSpool((tmp_path / "spool").resolve())
    original_replace = StrategyCandidateSnapshotSpool._atomic_replace_pointer

    def fail_pointer(cls: type[StrategyCandidateSnapshotSpool], *args: object) -> None:
        raise RuntimeError("simulated pointer interruption")

    monkeypatch.setattr(
        StrategyCandidateSnapshotSpool,
        "_atomic_replace_pointer",
        classmethod(fail_pointer),
    )
    with pytest.raises(RuntimeError, match="pointer interruption"):
        spool.publish_strategy_records(
            strategy_id="n_shape",
            strategy_version="1",
            definition_fingerprint=DEFINITION_FINGERPRINT,
            executable_fingerprint=EXECUTABLE_FINGERPRINT,
            candidate_schema_fingerprint=_candidate_schema_fingerprint(strategy_version="1"),
            static_feature_schema=STATIC_FEATURE_SCHEMA,
            source_snapshot_ids={"candidate_input": HASH_A},
            trade_date=TRADE_DATE,
            captured_at=CAPTURED_AT,
            producer_commit=COMMIT_A,
            rows=(),
        )
    monkeypatch.setattr(
        StrategyCandidateSnapshotSpool,
        "_atomic_replace_pointer",
        original_replace,
    )

    recovered = spool.publish_strategy_records(
        strategy_id="n_shape",
        strategy_version="1",
        definition_fingerprint=DEFINITION_FINGERPRINT,
        executable_fingerprint=EXECUTABLE_FINGERPRINT,
        candidate_schema_fingerprint=_candidate_schema_fingerprint(strategy_version="1"),
        static_feature_schema=STATIC_FEATURE_SCHEMA,
        source_snapshot_ids={"candidate_input": HASH_A},
        trade_date=TRADE_DATE,
        captured_at=CAPTURED_AT + timedelta(seconds=1),
        producer_commit=COMMIT_A,
        rows=(),
    )

    assert recovered.published is True
    assert recovered.snapshot.sequence == 0
    assert recovered.snapshot.captured_at == CAPTURED_AT
    assert recovered.snapshot.authority_binding == _read_binding(spool, strategy_version="1")
    assert recovered.snapshot.source_snapshot_ids == {"candidate_input": HASH_A}
    assert len(list(spool.generations_root.glob("*.json"))) == 1
    assert (
        spool.read_strategy_as_of(
            CAPTURED_AT + timedelta(seconds=2),
            strategy_id="n_shape",
            strategy_version="1",
            definition_fingerprint=DEFINITION_FINGERPRINT,
            executable_fingerprint=EXECUTABLE_FINGERPRINT,
            candidate_schema_fingerprint=_candidate_schema_fingerprint(strategy_version="1"),
            static_feature_schema=STATIC_FEATURE_SCHEMA,
        )
        == recovered.snapshot
    )


@pytest.mark.parametrize("preinitialized", [False, True], ids=["cold", "initialized"])
def test_publish_records_recovers_generation_linked_before_pointer_switch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    preinitialized: bool,
) -> None:
    spool = StrategyCandidateSnapshotSpool((tmp_path / "spool").resolve())
    variant = "pool1"
    captured_at = CAPTURED_AT
    if preinitialized:
        baseline = spool.publish_legacy_records_for_migration(
            trade_date=TRADE_DATE,
            captured_at=CAPTURED_AT - timedelta(minutes=1),
            producer_commit=COMMIT_A,
            rows=(_row(variant="baseline"),),
        )
        assert baseline.snapshot.sequence == 0
        variant = "pool2"

    original_replace = StrategyCandidateSnapshotSpool._atomic_replace_pointer

    def fail_pointer_switch(cls: type[StrategyCandidateSnapshotSpool], *args: object) -> None:
        raise RuntimeError("simulated pointer interruption")

    monkeypatch.setattr(
        StrategyCandidateSnapshotSpool,
        "_atomic_replace_pointer",
        classmethod(fail_pointer_switch),
    )
    with pytest.raises(RuntimeError, match="pointer interruption"):
        spool.publish_legacy_records_for_migration(
            trade_date=TRADE_DATE,
            captured_at=captured_at,
            producer_commit=COMMIT_A,
            rows=(_row(variant=variant),),
        )
    monkeypatch.setattr(
        StrategyCandidateSnapshotSpool,
        "_atomic_replace_pointer",
        original_replace,
    )

    recovered = spool.publish_legacy_records_for_migration(
        trade_date=TRADE_DATE,
        captured_at=captured_at + timedelta(seconds=1),
        producer_commit=COMMIT_A,
        rows=(_row(variant=variant),),
    )

    assert recovered.published is True
    assert recovered.snapshot.sequence == int(preinitialized)
    assert recovered.snapshot.captured_at == captured_at
    assert len(list(spool.generations_root.glob("*.json"))) == 1 + int(preinitialized)
    assert spool.read_legacy_for_migration(captured_at + timedelta(seconds=2)) == recovered.snapshot


def test_constructor_and_failed_read_do_not_create_or_modify_authority(tmp_path: Path) -> None:
    root = (tmp_path / "candidate-spool").resolve()
    before = _tree_state(tmp_path)

    spool = StrategyCandidateSnapshotSpool(root)

    assert _tree_state(tmp_path) == before
    with pytest.raises(StrategyCandidateSnapshotIntegrityError, match="missing"):
        spool.read_legacy_for_migration(CAPTURED_AT)
    assert _tree_state(tmp_path) == before


def test_publish_rejects_sequence_conflict_and_rollback(tmp_path: Path) -> None:
    spool = StrategyCandidateSnapshotSpool((tmp_path / "spool").resolve())
    spool.publish_legacy_for_migration(_snapshot(sequence=0))

    with pytest.raises(StrategyCandidateSnapshotIntegrityError, match="sequence"):
        spool.publish_legacy_for_migration(_snapshot(sequence=0, rows=(_row(variant="pool2"),)))
    with pytest.raises(StrategyCandidateSnapshotIntegrityError, match="next sequence"):
        spool.publish_legacy_for_migration(_snapshot(sequence=2, rows=(_row(variant="pool2"),)))


def test_as_of_falls_back_from_future_current_to_latest_visible_generation(
    tmp_path: Path,
) -> None:
    spool = StrategyCandidateSnapshotSpool((tmp_path / "spool").resolve())
    old = _snapshot(sequence=0)
    future = _snapshot(
        sequence=1,
        captured_at=CAPTURED_AT + timedelta(minutes=5),
        rows=(
            _row(
                variant="pool2",
                decision_at=CAPTURED_AT + timedelta(minutes=2),
                available_at=CAPTURED_AT + timedelta(minutes=3),
            ),
        ),
    )
    spool.publish_legacy_for_migration(old)
    spool.publish_legacy_for_migration(future)

    observed = spool.read_legacy_for_migration(CAPTURED_AT + timedelta(minutes=1))

    assert observed == old
    assert observed is not None
    assert observed.rows[0].variant == "pool1"
    assert spool.read_legacy_for_migration(CAPTURED_AT - timedelta(seconds=1)) is None
    assert spool.read_legacy_for_migration(CAPTURED_AT + timedelta(minutes=6)) == future


def test_read_as_of_does_not_write_or_repair_any_file(tmp_path: Path) -> None:
    spool = StrategyCandidateSnapshotSpool((tmp_path / "spool").resolve())
    spool.publish_legacy_for_migration(_snapshot())
    before = _tree_state(spool.root)

    assert spool.read_legacy_for_migration(CAPTURED_AT + timedelta(minutes=1)) is not None

    assert _tree_state(spool.root) == before


def test_new_reader_lifecycle_does_not_write(tmp_path: Path) -> None:
    root = (tmp_path / "spool").resolve()
    writer = StrategyCandidateSnapshotSpool(root)
    writer.publish_legacy_for_migration(_snapshot())
    before = _tree_state(root)

    reader = StrategyCandidateSnapshotSpool(root)
    assert reader.read_legacy_for_migration(CAPTURED_AT + timedelta(minutes=1)) is not None

    assert _tree_state(root) == before


def test_repeated_reads_cache_validated_immutable_generations(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = (tmp_path / "spool").resolve()
    writer = StrategyCandidateSnapshotSpool(root)
    writer.publish_legacy_for_migration(_snapshot(sequence=0))
    reader = StrategyCandidateSnapshotSpool(root)
    original = reader._read_snapshot
    read_names: list[str] = []

    def counting_read(parent_fd: int, name: str) -> StrategyCandidateSnapshot:
        read_names.append(name)
        return original(parent_fd, name)

    monkeypatch.setattr(reader, "_read_snapshot", counting_read)
    reader.read_legacy_for_migration(CAPTURED_AT + timedelta(minutes=1))
    reader.read_legacy_for_migration(CAPTURED_AT + timedelta(minutes=1))

    assert len(read_names) == 1

    writer.publish_legacy_for_migration(
        _snapshot(
            sequence=1,
            captured_at=CAPTURED_AT + timedelta(minutes=2),
            rows=(
                _row(
                    variant="pool2",
                    decision_at=CAPTURED_AT + timedelta(minutes=1),
                    available_at=CAPTURED_AT + timedelta(minutes=1),
                ),
            ),
        )
    )
    reader.read_legacy_for_migration(CAPTURED_AT + timedelta(minutes=3))

    assert len(read_names) == 2


def test_as_of_uses_bounded_index_and_cache_for_4096_sparse_large_generations(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = (tmp_path / "large-history").resolve()
    spool = StrategyCandidateSnapshotSpool(root)
    spool.publish_legacy_for_migration(_snapshot())
    for generation in spool.generations_root.glob("*.json"):
        generation.unlink()
    spool.current_path.unlink()
    index_path = root / "generation-index.json"
    if index_path.exists():
        index_path.unlink()

    large_blob = "x" * (16 * 1024 * 1024 - 4096)
    target = _snapshot(
        sequence=4095,
        rows=(_row(static_features={"blob": large_blob}),),
    )
    target_name = f"{target.content_sha256}.json"
    target_payload = _canonical_bytes(target)
    entries: list[dict[str, object]] = []
    for sequence in range(4096):
        generation_sha256 = (
            target.content_sha256
            if sequence == target.sequence
            else canonical_sha256({"large-history-sequence": sequence})
        )
        name = f"{generation_sha256}.json"
        path = spool.generations_root / name
        if sequence == target.sequence:
            path.write_bytes(target_payload)
        else:
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            try:
                os.ftruncate(descriptor, 16 * 1024 * 1024)
            finally:
                os.close(descriptor)
        path.chmod(0o600)
        entries.append(
            {
                "sequence": sequence,
                "generation_sha256": generation_sha256,
                "schema_version": 2,
                "trade_date": TRADE_DATE.isoformat(),
                "captured_at": CAPTURED_AT.isoformat().replace("+00:00", "Z"),
                "max_available_at": AVAILABLE_AT.isoformat().replace("+00:00", "Z"),
                "producer_commit": COMMIT_A,
                "authority_binding_sha256": None,
                "size_bytes": path.stat().st_size,
            }
        )
    index_path.write_bytes(
        json.dumps(
            {"schema_version": 1, "entries": entries},
            ensure_ascii=True,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    )
    index_path.chmod(0o600)
    spool.current_path.write_bytes(
        _canonical_bytes(StrategyCandidateSnapshotPointer.from_snapshot(target))
    )
    spool.current_path.chmod(0o600)

    reader = StrategyCandidateSnapshotSpool(root)
    original = reader._read_snapshot
    read_names: list[str] = []

    def counting_read(parent_fd: int, name: str) -> StrategyCandidateSnapshot:
        read_names.append(name)
        return original(parent_fd, name)

    monkeypatch.setattr(reader, "_read_snapshot", counting_read)

    assert reader.read_legacy_for_migration(CAPTURED_AT + timedelta(seconds=1)) == target
    assert read_names == [target_name]
    assert len(reader._generation_cache) <= 4
    assert reader._generation_cache_bytes <= 32 * 1024 * 1024


@pytest.mark.parametrize("mutation", ["delete", "content", "mode", "inode"])
def test_cached_generation_mutation_fails_closed(
    tmp_path: Path,
    mutation: str,
) -> None:
    root = (tmp_path / mutation).resolve()
    writer = StrategyCandidateSnapshotSpool(root)
    snapshot = _snapshot()
    writer.publish_legacy_for_migration(snapshot)
    reader = StrategyCandidateSnapshotSpool(root)
    assert reader.read_legacy_for_migration(CAPTURED_AT + timedelta(minutes=1)) == snapshot
    generation = root / "generations" / f"{snapshot.content_sha256}.json"

    if mutation == "delete":
        generation.unlink()
    elif mutation == "content":
        generation.write_bytes(generation.read_bytes() + b" ")
        os.chmod(generation, 0o600)
    elif mutation == "mode":
        generation.chmod(0o640)
    else:
        payload = generation.read_bytes()
        generation.unlink()
        generation.write_bytes(payload)
        generation.chmod(0o600)

    with pytest.raises(StrategyCandidateSnapshotIntegrityError):
        reader.read_legacy_for_migration(CAPTURED_AT + timedelta(minutes=1))


@pytest.mark.parametrize("target", ["generation", "pointer"])
def test_content_and_pointer_tampering_fail_closed(tmp_path: Path, target: str) -> None:
    spool = StrategyCandidateSnapshotSpool((tmp_path / "spool").resolve())
    snapshot = _snapshot()
    spool.publish_legacy_for_migration(snapshot)
    path = (
        spool.generations_root / f"{snapshot.content_sha256}.json"
        if target == "generation"
        else spool.current_path
    )
    payload = json.loads(path.read_text())
    if target == "generation":
        payload["rows"][0]["variant"] = "forged"
    else:
        payload["sequence"] = 99
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    os.chmod(path, 0o600)

    with pytest.raises(StrategyCandidateSnapshotIntegrityError):
        spool.read_legacy_for_migration(CAPTURED_AT + timedelta(minutes=1))


def test_missing_current_generation_fails_closed(tmp_path: Path) -> None:
    spool = StrategyCandidateSnapshotSpool((tmp_path / "spool").resolve())
    snapshot = _snapshot()
    spool.publish_legacy_for_migration(snapshot)
    (spool.generations_root / f"{snapshot.content_sha256}.json").unlink()

    with pytest.raises(StrategyCandidateSnapshotIntegrityError, match="missing"):
        spool.read_legacy_for_migration(CAPTURED_AT + timedelta(minutes=1))


def test_missing_current_pointer_fails_closed(tmp_path: Path) -> None:
    spool = StrategyCandidateSnapshotSpool((tmp_path / "spool").resolve())
    spool.publish_legacy_for_migration(_snapshot())
    spool.current_path.unlink()

    with pytest.raises(StrategyCandidateSnapshotIntegrityError, match="pointer is missing"):
        spool.read_legacy_for_migration(CAPTURED_AT + timedelta(minutes=1))


def test_persisted_sequence_gap_and_duplicate_fail_closed(tmp_path: Path) -> None:
    gap_spool = StrategyCandidateSnapshotSpool((tmp_path / "gap").resolve())
    first = _snapshot(sequence=0)
    second = _snapshot(
        sequence=1,
        captured_at=CAPTURED_AT + timedelta(minutes=2),
        rows=(
            _row(
                variant="pool2",
                decision_at=CAPTURED_AT + timedelta(minutes=1),
                available_at=CAPTURED_AT + timedelta(minutes=1),
            ),
        ),
    )
    gap_spool.publish_legacy_for_migration(first)
    gap_spool.publish_legacy_for_migration(second)
    (gap_spool.generations_root / f"{first.content_sha256}.json").unlink()
    with pytest.raises(StrategyCandidateSnapshotIntegrityError, match="sequence"):
        gap_spool.read_legacy_for_migration(CAPTURED_AT + timedelta(minutes=3))

    duplicate_spool = StrategyCandidateSnapshotSpool((tmp_path / "duplicate").resolve())
    original = _snapshot(sequence=0)
    conflicting = _snapshot(sequence=0, rows=(_row(variant="pool2"),))
    duplicate_spool.publish_legacy_for_migration(original)
    conflict_path = duplicate_spool.generations_root / f"{conflicting.content_sha256}.json"
    conflict_path.write_bytes(_canonical_bytes(conflicting))
    os.chmod(conflict_path, 0o600)
    with pytest.raises(StrategyCandidateSnapshotIntegrityError, match="duplicate"):
        duplicate_spool.read_legacy_for_migration(CAPTURED_AT + timedelta(minutes=1))


def test_pointer_to_valid_old_generation_fails_closed(tmp_path: Path) -> None:
    spool = StrategyCandidateSnapshotSpool((tmp_path / "spool").resolve())
    old = _snapshot(sequence=0)
    latest = _snapshot(
        sequence=1,
        captured_at=CAPTURED_AT + timedelta(minutes=2),
        rows=(
            _row(
                variant="pool2",
                decision_at=CAPTURED_AT + timedelta(minutes=1),
                available_at=CAPTURED_AT + timedelta(minutes=1),
            ),
        ),
    )
    spool.publish_legacy_for_migration(old)
    spool.publish_legacy_for_migration(latest)
    spool.current_path.write_bytes(
        _canonical_bytes(StrategyCandidateSnapshotPointer.from_snapshot(old))
    )
    os.chmod(spool.current_path, 0o600)

    with pytest.raises(StrategyCandidateSnapshotIntegrityError, match="latest"):
        spool.read_legacy_for_migration(CAPTURED_AT + timedelta(minutes=3))


def test_publish_recovers_owned_stale_temporary_without_poisoning_reads(
    tmp_path: Path,
) -> None:
    spool = StrategyCandidateSnapshotSpool((tmp_path / "spool").resolve())
    spool.publish_legacy_for_migration(_snapshot(sequence=0))
    stale = spool.root / f".candidate-generation.{'a' * 32}.tmp"
    first_generation = next(spool.generations_root.glob("*.json"))
    os.link(first_generation, stale)
    second = _snapshot(
        sequence=1,
        captured_at=CAPTURED_AT + timedelta(minutes=2),
        rows=(
            _row(
                variant="pool2",
                decision_at=CAPTURED_AT + timedelta(minutes=1),
                available_at=CAPTURED_AT + timedelta(minutes=1),
            ),
        ),
    )

    spool.publish_legacy_for_migration(second)

    assert not stale.exists()
    assert spool.read_legacy_for_migration(CAPTURED_AT + timedelta(minutes=3)) == second


def test_publish_finishes_generation_linked_before_pointer_switch(tmp_path: Path) -> None:
    spool = StrategyCandidateSnapshotSpool((tmp_path / "spool").resolve())
    spool.publish_legacy_for_migration(_snapshot(sequence=0))
    interrupted = _snapshot(
        sequence=1,
        captured_at=CAPTURED_AT + timedelta(minutes=2),
        rows=(
            _row(
                variant="pool2",
                decision_at=CAPTURED_AT + timedelta(minutes=1),
                available_at=CAPTURED_AT + timedelta(minutes=1),
            ),
        ),
    )
    generation = spool.generations_root / f"{interrupted.content_sha256}.json"
    generation.write_bytes(_canonical_bytes(interrupted))
    os.chmod(generation, 0o600)

    assert spool.publish_legacy_for_migration(interrupted) == interrupted
    assert spool.read_legacy_for_migration(CAPTURED_AT + timedelta(minutes=3)) == interrupted


def test_authority_size_and_generation_count_are_bounded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.strategy_candidate_snapshot as snapshot_module

    size_spool = StrategyCandidateSnapshotSpool((tmp_path / "size").resolve())
    size_spool.publish_legacy_for_migration(_snapshot())
    monkeypatch.setattr(snapshot_module, "_MAX_AUTHORITY_BYTES", 32)
    with pytest.raises(StrategyCandidateSnapshotIntegrityError, match="size limit"):
        size_spool.read_legacy_for_migration(CAPTURED_AT + timedelta(minutes=1))

    monkeypatch.setattr(snapshot_module, "_MAX_AUTHORITY_BYTES", 16 * 1024 * 1024)
    count_spool = StrategyCandidateSnapshotSpool((tmp_path / "count").resolve())
    count_spool.publish_legacy_for_migration(_snapshot(sequence=0))
    second = _snapshot(
        sequence=1,
        captured_at=CAPTURED_AT + timedelta(minutes=2),
        rows=(
            _row(
                variant="pool2",
                decision_at=CAPTURED_AT + timedelta(minutes=1),
                available_at=CAPTURED_AT + timedelta(minutes=1),
            ),
        ),
    )
    monkeypatch.setattr(snapshot_module, "_MAX_GENERATIONS", 1)
    with pytest.raises(StrategyCandidateSnapshotIntegrityError, match="count"):
        count_spool.publish_legacy_for_migration(second)
    assert count_spool.read_legacy_for_migration(CAPTURED_AT + timedelta(minutes=3)).sequence == 0


@pytest.mark.parametrize("target", ["root", "generation", "pointer"])
def test_symlink_paths_fail_closed(tmp_path: Path, target: str) -> None:
    if target == "root":
        real = tmp_path / "real"
        real.mkdir()
        alias = tmp_path / "alias"
        alias.symlink_to(real, target_is_directory=True)
        with pytest.raises(StrategyCandidateSnapshotIntegrityError, match="symlink"):
            StrategyCandidateSnapshotSpool(alias)
        return

    spool = StrategyCandidateSnapshotSpool((tmp_path / "spool").resolve())
    snapshot = _snapshot()
    spool.publish_legacy_for_migration(snapshot)
    external = tmp_path / "external.json"
    external.write_bytes(_canonical_bytes(snapshot))
    if target == "generation":
        path = spool.generations_root / f"{snapshot.content_sha256}.json"
    else:
        path = spool.current_path
    path.unlink()
    path.symlink_to(external)

    with pytest.raises(StrategyCandidateSnapshotIntegrityError, match="symlink"):
        spool.read_legacy_for_migration(CAPTURED_AT + timedelta(minutes=1))


def test_constructor_rejects_relative_and_parent_symlink_paths(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="absolute"):
        StrategyCandidateSnapshotSpool(Path("relative/spool"))

    real = tmp_path / "real"
    real.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(real, target_is_directory=True)
    with pytest.raises(StrategyCandidateSnapshotIntegrityError, match="symlink"):
        StrategyCandidateSnapshotSpool(alias / "child")


def test_read_rejects_ancestor_and_generations_symlink_after_construction(
    tmp_path: Path,
) -> None:
    parent = tmp_path / "authority"
    root = (parent / "spool").resolve()
    spool = StrategyCandidateSnapshotSpool(root)
    spool.publish_legacy_for_migration(_snapshot())
    moved_parent = tmp_path / "authority-real"
    parent.rename(moved_parent)
    parent.symlink_to(moved_parent, target_is_directory=True)

    with pytest.raises(StrategyCandidateSnapshotIntegrityError, match="symlink"):
        spool.read_legacy_for_migration(CAPTURED_AT + timedelta(minutes=1))

    parent.unlink()
    moved_parent.rename(parent)
    moved_generations = root / "generations-real"
    spool.generations_root.rename(moved_generations)
    spool.generations_root.symlink_to(moved_generations, target_is_directory=True)
    with pytest.raises(StrategyCandidateSnapshotIntegrityError, match="directory"):
        spool.read_legacy_for_migration(CAPTURED_AT + timedelta(minutes=1))


def test_pointer_hash_is_bound_to_generation_snapshot() -> None:
    snapshot = _snapshot()
    pointer = StrategyCandidateSnapshotPointer.from_snapshot(snapshot)

    assert pointer.generation_sha256 == snapshot.content_sha256
    assert (
        canonical_sha256(
            snapshot.model_dump(
                mode="python",
                exclude={"authority_binding", "content_sha256", "source_snapshot_ids"},
            )
        )
        == snapshot.content_sha256
    )
