from __future__ import annotations

import json
import os
import stat
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path

import pytest
from pydantic import ValidationError

from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.strategy_candidate_snapshot import (
    StrategyCandidatePriceBasis,
    StrategyCandidateRecord,
    StrategyCandidateSnapshot,
    StrategyCandidateSnapshotIntegrityError,
    StrategyCandidateSnapshotPointer,
    StrategyCandidateSnapshotSpool,
)

COMMIT_A = "a" * 40
HASH_A = "1" * 64
HASH_B = "2" * 64
TRADE_DATE = date(2026, 7, 31)
DECISION_AT = datetime(2026, 7, 31, 1, 25, tzinfo=UTC)
AVAILABLE_AT = datetime(2026, 7, 31, 1, 27, tzinfo=UTC)
CAPTURED_AT = datetime(2026, 7, 31, 1, 28, tzinfo=UTC)


def _row(
    *,
    candidate_id: str = "000001.SZ",
    variant: str = "pool1",
    decision_at: datetime = DECISION_AT,
    available_at: datetime = AVAILABLE_AT,
    reference_trade_date: date = date(2026, 7, 30),
    price_basis: StrategyCandidatePriceBasis = StrategyCandidatePriceBasis.QFQ_PIT,
    static_features: dict[str, object] | None = None,
    reference_snapshot_ids: dict[str, str] | None = None,
) -> StrategyCandidateRecord:
    return StrategyCandidateRecord(
        strategy_id="n_shape",
        strategy_version="b-v1",
        candidate_id=candidate_id,
        variant=variant,
        decision_at=decision_at,
        available_at=available_at,
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
) -> StrategyCandidateSnapshot:
    return StrategyCandidateSnapshot.build(
        sequence=sequence,
        trade_date=TRADE_DATE,
        captured_at=captured_at,
        producer_commit=producer_commit,
        rows=rows or (_row(),),
    )


def _canonical_bytes(model: RuntimeContractModel) -> bytes:
    return json.dumps(
        model.model_dump(mode="json"),
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


def test_all_cross_layer_contracts_inherit_runtime_contract_model() -> None:
    assert issubclass(StrategyCandidateRecord, RuntimeContractModel)
    assert issubclass(StrategyCandidateSnapshot, RuntimeContractModel)
    assert issubclass(StrategyCandidateSnapshotPointer, RuntimeContractModel)


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


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"decision_at": DECISION_AT.replace(tzinfo=None)}, "timezone-aware"),
        ({"available_at": AVAILABLE_AT.replace(tzinfo=None)}, "timezone-aware"),
        ({"available_at": DECISION_AT - timedelta(seconds=1)}, "available_at"),
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


@pytest.mark.parametrize(
    "change",
    [
        {"sequence": 1},
        {"captured_at": CAPTURED_AT + timedelta(minutes=1)},
        {"producer_commit": "b" * 40},
        {"rows": (_row(variant="pool2"),)},
        {"rows": (_row(static_features={"score": 0.9}),)},
        {"rows": (_row(reference_snapshot_ids={"daily_state": HASH_B}),)},
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

    first = spool.publish(snapshot)
    first_generation = spool.generations_root / f"{snapshot.content_sha256}.json"
    generation_bytes = first_generation.read_bytes()
    second = spool.publish(snapshot)

    assert first == snapshot
    assert second == snapshot
    assert generation_bytes == first_generation.read_bytes() == _canonical_bytes(snapshot)
    assert stat.S_IMODE(root.stat().st_mode) == 0o700
    assert stat.S_IMODE(spool.generations_root.stat().st_mode) == 0o700
    assert stat.S_IMODE(first_generation.stat().st_mode) == 0o600
    assert stat.S_IMODE(spool.current_path.stat().st_mode) == 0o600
    assert list(spool.generations_root.glob("*.json")) == [first_generation]


def test_constructor_and_failed_read_do_not_create_or_modify_authority(tmp_path: Path) -> None:
    root = (tmp_path / "candidate-spool").resolve()
    before = _tree_state(tmp_path)

    spool = StrategyCandidateSnapshotSpool(root)

    assert _tree_state(tmp_path) == before
    with pytest.raises(StrategyCandidateSnapshotIntegrityError, match="missing"):
        spool.read_as_of(CAPTURED_AT)
    assert _tree_state(tmp_path) == before


def test_publish_rejects_sequence_conflict_and_rollback(tmp_path: Path) -> None:
    spool = StrategyCandidateSnapshotSpool((tmp_path / "spool").resolve())
    spool.publish(_snapshot(sequence=0))

    with pytest.raises(StrategyCandidateSnapshotIntegrityError, match="sequence"):
        spool.publish(_snapshot(sequence=0, rows=(_row(variant="pool2"),)))
    with pytest.raises(StrategyCandidateSnapshotIntegrityError, match="next sequence"):
        spool.publish(_snapshot(sequence=2, rows=(_row(variant="pool2"),)))


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
    spool.publish(old)
    spool.publish(future)

    observed = spool.read_as_of(CAPTURED_AT + timedelta(minutes=1))

    assert observed == old
    assert observed is not None
    assert observed.rows[0].variant == "pool1"
    assert spool.read_as_of(CAPTURED_AT - timedelta(seconds=1)) is None
    assert spool.read_as_of(CAPTURED_AT + timedelta(minutes=6)) == future


def test_read_as_of_does_not_write_or_repair_any_file(tmp_path: Path) -> None:
    spool = StrategyCandidateSnapshotSpool((tmp_path / "spool").resolve())
    spool.publish(_snapshot())
    before = _tree_state(spool.root)

    assert spool.read_as_of(CAPTURED_AT + timedelta(minutes=1)) is not None

    assert _tree_state(spool.root) == before


def test_new_reader_lifecycle_does_not_write(tmp_path: Path) -> None:
    root = (tmp_path / "spool").resolve()
    writer = StrategyCandidateSnapshotSpool(root)
    writer.publish(_snapshot())
    before = _tree_state(root)

    reader = StrategyCandidateSnapshotSpool(root)
    assert reader.read_as_of(CAPTURED_AT + timedelta(minutes=1)) is not None

    assert _tree_state(root) == before


@pytest.mark.parametrize("target", ["generation", "pointer"])
def test_content_and_pointer_tampering_fail_closed(tmp_path: Path, target: str) -> None:
    spool = StrategyCandidateSnapshotSpool((tmp_path / "spool").resolve())
    snapshot = _snapshot()
    spool.publish(snapshot)
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
        spool.read_as_of(CAPTURED_AT + timedelta(minutes=1))


def test_missing_current_generation_fails_closed(tmp_path: Path) -> None:
    spool = StrategyCandidateSnapshotSpool((tmp_path / "spool").resolve())
    snapshot = _snapshot()
    spool.publish(snapshot)
    (spool.generations_root / f"{snapshot.content_sha256}.json").unlink()

    with pytest.raises(StrategyCandidateSnapshotIntegrityError, match="missing"):
        spool.read_as_of(CAPTURED_AT + timedelta(minutes=1))


def test_missing_current_pointer_fails_closed(tmp_path: Path) -> None:
    spool = StrategyCandidateSnapshotSpool((tmp_path / "spool").resolve())
    spool.publish(_snapshot())
    spool.current_path.unlink()

    with pytest.raises(StrategyCandidateSnapshotIntegrityError, match="pointer is missing"):
        spool.read_as_of(CAPTURED_AT + timedelta(minutes=1))


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
    gap_spool.publish(first)
    gap_spool.publish(second)
    (gap_spool.generations_root / f"{first.content_sha256}.json").unlink()
    with pytest.raises(StrategyCandidateSnapshotIntegrityError, match="sequence"):
        gap_spool.read_as_of(CAPTURED_AT + timedelta(minutes=3))

    duplicate_spool = StrategyCandidateSnapshotSpool((tmp_path / "duplicate").resolve())
    original = _snapshot(sequence=0)
    conflicting = _snapshot(sequence=0, rows=(_row(variant="pool2"),))
    duplicate_spool.publish(original)
    conflict_path = duplicate_spool.generations_root / f"{conflicting.content_sha256}.json"
    conflict_path.write_bytes(_canonical_bytes(conflicting))
    os.chmod(conflict_path, 0o600)
    with pytest.raises(StrategyCandidateSnapshotIntegrityError, match="duplicate"):
        duplicate_spool.read_as_of(CAPTURED_AT + timedelta(minutes=1))


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
    spool.publish(old)
    spool.publish(latest)
    spool.current_path.write_bytes(
        _canonical_bytes(StrategyCandidateSnapshotPointer.from_snapshot(old))
    )
    os.chmod(spool.current_path, 0o600)

    with pytest.raises(StrategyCandidateSnapshotIntegrityError, match="latest"):
        spool.read_as_of(CAPTURED_AT + timedelta(minutes=3))


def test_publish_recovers_owned_stale_temporary_without_poisoning_reads(
    tmp_path: Path,
) -> None:
    spool = StrategyCandidateSnapshotSpool((tmp_path / "spool").resolve())
    spool.publish(_snapshot(sequence=0))
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

    spool.publish(second)

    assert not stale.exists()
    assert spool.read_as_of(CAPTURED_AT + timedelta(minutes=3)) == second


def test_publish_finishes_generation_linked_before_pointer_switch(tmp_path: Path) -> None:
    spool = StrategyCandidateSnapshotSpool((tmp_path / "spool").resolve())
    spool.publish(_snapshot(sequence=0))
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

    assert spool.publish(interrupted) == interrupted
    assert spool.read_as_of(CAPTURED_AT + timedelta(minutes=3)) == interrupted


def test_authority_size_and_generation_count_are_bounded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.strategy_candidate_snapshot as snapshot_module

    size_spool = StrategyCandidateSnapshotSpool((tmp_path / "size").resolve())
    size_spool.publish(_snapshot())
    monkeypatch.setattr(snapshot_module, "_MAX_AUTHORITY_BYTES", 32)
    with pytest.raises(StrategyCandidateSnapshotIntegrityError, match="size limit"):
        size_spool.read_as_of(CAPTURED_AT + timedelta(minutes=1))

    monkeypatch.setattr(snapshot_module, "_MAX_AUTHORITY_BYTES", 16 * 1024 * 1024)
    count_spool = StrategyCandidateSnapshotSpool((tmp_path / "count").resolve())
    count_spool.publish(_snapshot(sequence=0))
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
        count_spool.publish(second)
    assert count_spool.read_as_of(CAPTURED_AT + timedelta(minutes=3)).sequence == 0


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
    spool.publish(snapshot)
    external = tmp_path / "external.json"
    external.write_bytes(_canonical_bytes(snapshot))
    if target == "generation":
        path = spool.generations_root / f"{snapshot.content_sha256}.json"
    else:
        path = spool.current_path
    path.unlink()
    path.symlink_to(external)

    with pytest.raises(StrategyCandidateSnapshotIntegrityError, match="symlink"):
        spool.read_as_of(CAPTURED_AT + timedelta(minutes=1))


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
    spool.publish(_snapshot())
    moved_parent = tmp_path / "authority-real"
    parent.rename(moved_parent)
    parent.symlink_to(moved_parent, target_is_directory=True)

    with pytest.raises(StrategyCandidateSnapshotIntegrityError, match="symlink"):
        spool.read_as_of(CAPTURED_AT + timedelta(minutes=1))

    parent.unlink()
    moved_parent.rename(parent)
    moved_generations = root / "generations-real"
    spool.generations_root.rename(moved_generations)
    spool.generations_root.symlink_to(moved_generations, target_is_directory=True)
    with pytest.raises(StrategyCandidateSnapshotIntegrityError, match="directory"):
        spool.read_as_of(CAPTURED_AT + timedelta(minutes=1))


def test_pointer_hash_is_bound_to_generation_snapshot() -> None:
    snapshot = _snapshot()
    pointer = StrategyCandidateSnapshotPointer.from_snapshot(snapshot)

    assert pointer.generation_sha256 == snapshot.content_sha256
    assert (
        canonical_sha256(snapshot.model_dump(mode="python", exclude={"content_sha256"}))
        == snapshot.content_sha256
    )
