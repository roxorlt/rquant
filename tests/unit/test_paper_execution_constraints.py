from __future__ import annotations

import json
import os
import stat
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

import rquant.paper_execution_constraints as constraint_module
from rquant.paper_execution_constraints import (
    PaperExecutionConstraintAuthority,
    PaperExecutionConstraintBatch,
    PaperExecutionConstraintIntegrityError,
    PaperExecutionConstraintPointer,
    PaperExecutionConstraintPublisher,
    PaperExecutionConstraintSnapshot,
    PaperExecutionConstraintUnavailableError,
)
from rquant.runtime_contracts import canonical_sha256
from rquant.signal_contracts import SignalAction

COMMIT = "a" * 40
NEXT_COMMIT = "b" * 40
AVAILABLE_AT = datetime(2026, 7, 21, 1, 31, tzinfo=UTC)
EXPIRES_AT = AVAILABLE_AT + timedelta(minutes=2)
PUBLISHED_AT = AVAILABLE_AT
TRADE_DATE = date(2026, 7, 21)
TS_CODE = "600000.SH"


def _snapshot_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "ts_code": TS_CODE,
        "trade_date": TRADE_DATE,
        "available_at": AVAILABLE_AT,
        "expires_at": EXPIRES_AT,
        "suspended": False,
        "buy_limit_locked": True,
        "sell_limit_locked": False,
        "risk_rejected": False,
        "instrument_context": None,
        "source_snapshot_ids": {"minute_bar": "c" * 64, "risk": "d" * 64},
        "producer_commit": COMMIT,
    }
    payload.update(overrides)
    payload["content_hash"] = canonical_sha256(payload)
    return payload


def _snapshot(**overrides: object) -> PaperExecutionConstraintSnapshot:
    return PaperExecutionConstraintSnapshot.model_validate(_snapshot_payload(**overrides))


def _batch_payload(
    *,
    sequence: int = 7,
    producer_commit: str = COMMIT,
    records: tuple[PaperExecutionConstraintSnapshot, ...] | None = None,
    **overrides: object,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_version": 1,
        "sequence": sequence,
        "producer_commit": producer_commit,
        "records": records or (_snapshot(producer_commit=producer_commit),),
    }
    payload.update(overrides)
    payload["content_hash"] = canonical_sha256(payload)
    return payload


def _batch(**overrides: object) -> PaperExecutionConstraintBatch:
    return PaperExecutionConstraintBatch.model_validate(_batch_payload(**overrides))


def _publisher(
    root: Path,
    *,
    producer_commit: str = COMMIT,
    now: datetime = PUBLISHED_AT,
    max_bytes: int = 8 * 1024 * 1024,
) -> PaperExecutionConstraintPublisher:
    return PaperExecutionConstraintPublisher(
        root=root,
        producer_commit=producer_commit,
        clock=lambda: now,
        max_bytes=max_bytes,
    )


def _publish_authority(
    tmp_path: Path,
    *,
    batch: PaperExecutionConstraintBatch | None = None,
    now: datetime = PUBLISHED_AT,
) -> tuple[Path, PaperExecutionConstraintPointer]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    root = tmp_path / "paper-execution-constraints"
    selected = batch or _batch()
    pointer = _publisher(
        root,
        producer_commit=selected.producer_commit,
        now=now,
    ).publish(selected)
    return root, pointer


def _authority(
    root: Path,
    *,
    expected_producer_commit: str = COMMIT,
    max_bytes: int = 8 * 1024 * 1024,
) -> PaperExecutionConstraintAuthority:
    return PaperExecutionConstraintAuthority(
        root=root,
        expected_producer_commit=expected_producer_commit,
        max_bytes=max_bytes,
    )


def _replace_pointer(root: Path, values: dict[str, object]) -> None:
    values["content_hash"] = canonical_sha256(
        {key: value for key, value in values.items() if key != "content_hash"}
    )
    temporary = root / "replacement-current.json"
    temporary.write_text(
        json.dumps(
            values,
            default=lambda value: value.isoformat() if isinstance(value, datetime) else value,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    os.replace(temporary, root / "current.json")


def test_publisher_and_reader_follow_dynamic_immutable_generations(tmp_path: Path) -> None:
    root = tmp_path / "authority"
    publisher = _publisher(root)
    reader = _authority(root)
    first = _batch(sequence=7)
    first_pointer = publisher.publish(first)

    first_decision = reader.resolve(
        ts_code=TS_CODE,
        trade_date=TRADE_DATE,
        observed_at=AVAILABLE_AT,
        action=SignalAction.B_INTENT,
    )

    second_record = _snapshot(
        available_at=AVAILABLE_AT + timedelta(seconds=30),
        expires_at=EXPIRES_AT + timedelta(minutes=1),
        buy_limit_locked=False,
    )
    second = _batch(sequence=8, records=(second_record,))
    second_pointer = _publisher(
        root,
        now=AVAILABLE_AT + timedelta(seconds=30),
    ).publish(second)
    second_decision = reader.resolve(
        ts_code=TS_CODE,
        trade_date=TRADE_DATE,
        observed_at=AVAILABLE_AT + timedelta(seconds=30),
        action=SignalAction.B_INTENT,
    )

    assert first_decision.limit_locked is True
    assert second_decision.limit_locked is False
    assert first_decision.authority_file_sha256 == first_pointer.file_sha256
    assert second_decision.authority_file_sha256 == second_pointer.file_sha256
    assert (root / "generations" / f"{first.content_hash}.json").is_file()
    assert (root / "generations" / f"{second.content_hash}.json").is_file()
    assert len(tuple((root / "generations").glob("*.json"))) == 2


def test_same_code_and_date_supports_multiple_non_overlapping_state_intervals(
    tmp_path: Path,
) -> None:
    intervals = (
        _snapshot(expires_at=AVAILABLE_AT + timedelta(seconds=30), buy_limit_locked=True),
        _snapshot(
            available_at=AVAILABLE_AT + timedelta(seconds=30),
            expires_at=AVAILABLE_AT + timedelta(seconds=60),
            buy_limit_locked=False,
        ),
        _snapshot(
            available_at=AVAILABLE_AT + timedelta(seconds=60),
            expires_at=AVAILABLE_AT + timedelta(seconds=90),
            buy_limit_locked=True,
        ),
    )
    root, _pointer = _publish_authority(
        tmp_path,
        batch=_batch(records=intervals),
        now=AVAILABLE_AT + timedelta(seconds=60),
    )
    reader = _authority(root)

    batch = reader.load(observed_at=AVAILABLE_AT + timedelta(seconds=60))
    latest = reader.resolve(
        ts_code=TS_CODE,
        trade_date=TRADE_DATE,
        observed_at=AVAILABLE_AT + timedelta(seconds=60),
        action=SignalAction.B_INTENT,
    )

    assert [record.buy_limit_locked for record in batch.records] == [True, False, True]
    assert latest.limit_locked is True


def test_batch_rejects_unsorted_or_overlapping_intervals_for_same_code_and_date() -> None:
    first = _snapshot(expires_at=AVAILABLE_AT + timedelta(minutes=1))
    overlapping = _snapshot(
        available_at=AVAILABLE_AT + timedelta(seconds=30),
        expires_at=AVAILABLE_AT + timedelta(minutes=2),
    )
    later = _snapshot(
        available_at=AVAILABLE_AT + timedelta(minutes=2),
        expires_at=AVAILABLE_AT + timedelta(minutes=3),
    )

    with pytest.raises(ValidationError, match="overlap"):
        PaperExecutionConstraintBatch.model_validate(_batch_payload(records=(first, overlapping)))
    with pytest.raises(ValidationError, match="sorted"):
        PaperExecutionConstraintBatch.model_validate(_batch_payload(records=(later, first)))


def test_direction_specific_locks_and_pit_expiry_are_preserved(tmp_path: Path) -> None:
    record = _snapshot(buy_limit_locked=True, sell_limit_locked=False)
    root, pointer = _publish_authority(tmp_path, batch=_batch(records=(record,)))
    reader = _authority(root)

    buy = reader.resolve(
        ts_code=TS_CODE,
        trade_date=TRADE_DATE,
        observed_at=AVAILABLE_AT,
        action=SignalAction.B_INTENT,
    )
    sell = reader.resolve(
        ts_code=TS_CODE,
        trade_date=TRADE_DATE,
        observed_at=AVAILABLE_AT,
        action=SignalAction.S_INTENT,
    )

    assert (buy.suspended, buy.limit_locked, buy.risk_rejected) == (False, True, False)
    assert sell.limit_locked is False
    assert buy.source_snapshot_ids == {"minute_bar": "c" * 64, "risk": "d" * 64}
    assert buy.constraint_content_hash == record.content_hash
    assert buy.authority_file_sha256 == pointer.file_sha256
    with pytest.raises(PaperExecutionConstraintUnavailableError, match="not yet available"):
        reader.resolve(
            ts_code=TS_CODE,
            trade_date=TRADE_DATE,
            observed_at=AVAILABLE_AT - timedelta(microseconds=1),
            action=SignalAction.B_INTENT,
        )
    with pytest.raises(PaperExecutionConstraintUnavailableError, match="expired"):
        reader.resolve(
            ts_code=TS_CODE,
            trade_date=TRADE_DATE,
            observed_at=EXPIRES_AT,
            action=SignalAction.B_INTENT,
        )


def test_reduce_uses_sell_limit_lock(tmp_path: Path) -> None:
    root, _pointer = _publish_authority(
        tmp_path,
        batch=_batch(records=(_snapshot(sell_limit_locked=True),)),
    )

    result = _authority(root).resolve(
        ts_code=TS_CODE,
        trade_date=TRADE_DATE,
        observed_at=AVAILABLE_AT,
        action=SignalAction.REDUCE,
    )

    assert result.limit_locked is True


@pytest.mark.parametrize("action", [SignalAction.WATCH, SignalAction.CANCEL])
def test_non_execution_actions_are_explicitly_unavailable(
    tmp_path: Path,
    action: SignalAction,
) -> None:
    root, _pointer = _publish_authority(tmp_path)

    with pytest.raises(PaperExecutionConstraintUnavailableError, match="execution action"):
        _authority(root).resolve(
            ts_code=TS_CODE,
            trade_date=TRADE_DATE,
            observed_at=AVAILABLE_AT,
            action=action,
        )


def test_missing_code_date_or_interval_is_explicitly_unavailable(tmp_path: Path) -> None:
    record = _snapshot(expires_at=AVAILABLE_AT + timedelta(seconds=30))
    root, _pointer = _publish_authority(tmp_path, batch=_batch(records=(record,)))
    reader = _authority(root)

    for code, trade_day, observed_at in (
        ("000001.SZ", TRADE_DATE, AVAILABLE_AT),
        (TS_CODE, date(2026, 7, 22), AVAILABLE_AT),
        (TS_CODE, TRADE_DATE, AVAILABLE_AT + timedelta(seconds=45)),
    ):
        with pytest.raises(PaperExecutionConstraintUnavailableError, match="not found|expired"):
            reader.resolve(
                ts_code=code,
                trade_date=trade_day,
                observed_at=observed_at,
                action=SignalAction.B_INTENT,
            )


def test_publisher_reads_clock_after_generation_fsync_before_pointer_switch(
    tmp_path: Path,
) -> None:
    root = tmp_path / "authority"
    actual_publish_time = AVAILABLE_AT + timedelta(seconds=15)
    batch = _batch()

    def atomic_clock() -> datetime:
        assert (root / "generations" / f"{batch.content_hash}.json").is_file()
        assert not (root / "current.json").exists()
        return actual_publish_time

    pointer = PaperExecutionConstraintPublisher(
        root=root,
        producer_commit=COMMIT,
        clock=atomic_clock,
    ).publish(batch)
    reader = _authority(root)

    assert pointer.published_at == actual_publish_time
    with pytest.raises(PaperExecutionConstraintUnavailableError, match="not yet available"):
        reader.resolve(
            ts_code=TS_CODE,
            trade_date=TRADE_DATE,
            observed_at=actual_publish_time - timedelta(microseconds=1),
            action=SignalAction.B_INTENT,
        )
    assert (
        reader.resolve(
            ts_code=TS_CODE,
            trade_date=TRADE_DATE,
            observed_at=actual_publish_time,
            action=SignalAction.B_INTENT,
        ).limit_locked
        is True
    )


def test_publisher_rejects_future_constraint_evidence(tmp_path: Path) -> None:
    future_record = _snapshot(
        available_at=AVAILABLE_AT + timedelta(seconds=1),
        expires_at=EXPIRES_AT + timedelta(seconds=1),
    )

    with pytest.raises(PaperExecutionConstraintIntegrityError, match="future evidence"):
        _publisher(tmp_path / "authority", now=AVAILABLE_AT).publish(
            _batch(records=(future_record,))
        )


def test_new_commit_can_advance_current_after_validating_old_generation(tmp_path: Path) -> None:
    root = tmp_path / "authority"
    first = _batch(sequence=7)
    first_pointer = _publisher(root).publish(first)
    second_record = _snapshot(
        producer_commit=NEXT_COMMIT,
        available_at=AVAILABLE_AT + timedelta(seconds=30),
        expires_at=EXPIRES_AT + timedelta(minutes=1),
        buy_limit_locked=False,
    )
    second = _batch(
        sequence=8,
        producer_commit=NEXT_COMMIT,
        records=(second_record,),
    )
    second_pointer = _publisher(
        root,
        producer_commit=NEXT_COMMIT,
        now=AVAILABLE_AT + timedelta(seconds=30),
    ).publish(second)

    assert second_pointer.producer_commit == NEXT_COMMIT
    assert (root / "generations" / f"{first_pointer.batch_hash}.json").is_file()
    assert (
        _authority(root, expected_producer_commit=NEXT_COMMIT)
        .resolve(
            ts_code=TS_CODE,
            trade_date=TRADE_DATE,
            observed_at=AVAILABLE_AT + timedelta(seconds=30),
            action=SignalAction.B_INTENT,
        )
        .limit_locked
        is False
    )

    old_generation = root / "generations" / f"{second.content_hash}.json"
    old_generation.write_bytes(b"tampered")
    third = _batch(sequence=9, producer_commit=NEXT_COMMIT, records=(second_record,))
    with pytest.raises(PaperExecutionConstraintIntegrityError, match="sha256|generation"):
        _publisher(
            root,
            producer_commit=NEXT_COMMIT,
            now=AVAILABLE_AT + timedelta(seconds=40),
        ).publish(third)


def test_publisher_rejects_sequence_and_publication_time_rollback(tmp_path: Path) -> None:
    root = tmp_path / "authority"
    _publisher(root).publish(_batch(sequence=7))

    with pytest.raises(PaperExecutionConstraintIntegrityError, match="sequence rollback"):
        _publisher(root, now=AVAILABLE_AT).publish(_batch(sequence=6))
    earlier_record = _snapshot(
        available_at=AVAILABLE_AT - timedelta(seconds=1),
    )
    with pytest.raises(PaperExecutionConstraintIntegrityError, match="publication rollback"):
        _publisher(root, now=PUBLISHED_AT - timedelta(microseconds=1)).publish(
            _batch(sequence=8, records=(earlier_record,))
        )


def test_reader_detects_pointer_rollback_after_observing_new_generation(tmp_path: Path) -> None:
    root = tmp_path / "authority"
    first_pointer = _publisher(root).publish(_batch(sequence=7))
    second_record = _snapshot(
        available_at=AVAILABLE_AT + timedelta(seconds=30),
        expires_at=EXPIRES_AT + timedelta(minutes=1),
    )
    _publisher(root, now=AVAILABLE_AT + timedelta(seconds=30)).publish(
        _batch(sequence=8, records=(second_record,))
    )
    reader = _authority(root)
    reader.load(observed_at=AVAILABLE_AT + timedelta(seconds=30))

    _replace_pointer(root, first_pointer.model_dump(mode="python", exclude={"content_hash"}))
    with pytest.raises(PaperExecutionConstraintIntegrityError, match="rollback"):
        reader.load(observed_at=AVAILABLE_AT + timedelta(seconds=30))


def test_idempotent_publish_never_overwrites_generation(tmp_path: Path) -> None:
    root = tmp_path / "authority"
    publisher = _publisher(root)
    batch = _batch()
    first = publisher.publish(batch)
    generation = root / "generations" / f"{batch.content_hash}.json"
    generation_bytes = generation.read_bytes()
    current_bytes = (root / "current.json").read_bytes()

    assert publisher.publish(batch) == first
    assert generation.read_bytes() == generation_bytes
    assert (root / "current.json").read_bytes() == current_bytes
    generation.write_bytes(b"conflict")
    with pytest.raises(PaperExecutionConstraintIntegrityError, match="generation"):
        publisher.publish(batch)
    assert generation.read_bytes() == b"conflict"


def test_publisher_uses_exclusive_lock_and_fsync(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    flock_calls: list[int] = []
    fsync_calls: list[int] = []
    real_flock = constraint_module.fcntl.flock
    real_fsync = constraint_module.os.fsync

    def track_flock(fd: int, operation: int) -> None:
        flock_calls.append(operation)
        real_flock(fd, operation)

    def track_fsync(fd: int) -> None:
        fsync_calls.append(fd)
        real_fsync(fd)

    monkeypatch.setattr(constraint_module.fcntl, "flock", track_flock)
    monkeypatch.setattr(constraint_module.os, "fsync", track_fsync)

    root, _pointer = _publish_authority(tmp_path)

    assert constraint_module.fcntl.LOCK_EX in flock_calls
    assert constraint_module.fcntl.LOCK_UN in flock_calls
    assert len(fsync_calls) >= 4
    assert stat.S_ISREG((root / ".publish.lock").stat().st_mode)


def test_pointer_generation_gap_tamper_and_commit_mismatch_fail_closed(tmp_path: Path) -> None:
    root, pointer = _publish_authority(tmp_path)
    generation = root / "generations" / f"{pointer.batch_hash}.json"
    generation.unlink()
    with pytest.raises(PaperExecutionConstraintIntegrityError, match="generation gap"):
        _authority(root).load(observed_at=AVAILABLE_AT)

    root, pointer = _publish_authority(tmp_path / "tamper")
    generation = root / "generations" / f"{pointer.batch_hash}.json"
    generation.write_bytes(generation.read_bytes() + b" ")
    with pytest.raises(PaperExecutionConstraintIntegrityError, match="file sha256"):
        _authority(root).load(observed_at=AVAILABLE_AT)

    root, _pointer = _publish_authority(tmp_path / "commit")
    with pytest.raises(PaperExecutionConstraintIntegrityError, match="producer_commit"):
        _authority(root, expected_producer_commit=NEXT_COMMIT).load(observed_at=AVAILABLE_AT)


@pytest.mark.parametrize("target", ["root", "current", "generations", "generation"])
def test_reader_rejects_symlinks_anywhere_in_authority_chain(
    tmp_path: Path,
    target: str,
) -> None:
    physical, pointer = _publish_authority(tmp_path / "physical")
    root = physical
    if target == "root":
        root = tmp_path / "alias"
        root.symlink_to(physical, target_is_directory=True)
    elif target == "current":
        current = physical / "current.json"
        saved = physical / "saved-current.json"
        current.replace(saved)
        current.symlink_to(saved)
    elif target == "generations":
        generations = physical / "generations"
        saved = physical / "saved-generations"
        generations.replace(saved)
        generations.symlink_to(saved, target_is_directory=True)
    else:
        generation = physical / "generations" / f"{pointer.batch_hash}.json"
        saved = physical / "generations" / "saved.json"
        generation.replace(saved)
        generation.symlink_to(saved)

    with pytest.raises(PaperExecutionConstraintIntegrityError, match="unsafe|symlink"):
        _authority(root).load(observed_at=AVAILABLE_AT)


@pytest.mark.parametrize("target", ["current", "generation"])
def test_reader_detects_pointer_or_generation_replacement_during_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    target: str,
) -> None:
    root, pointer = _publish_authority(tmp_path)
    selected = (
        root / "current.json"
        if target == "current"
        else root / "generations" / f"{pointer.batch_hash}.json"
    )
    replacement = tmp_path / f"replacement-{target}.json"
    replacement.write_bytes(selected.read_bytes())
    original_read = constraint_module.os.read
    replaced = False
    nonempty_reads = 0
    replace_on_read = 1 if target == "current" else 2

    def replace_matching_read(file_descriptor: int, size: int) -> bytes:
        nonlocal nonempty_reads, replaced
        data = original_read(file_descriptor, size)
        if data:
            nonempty_reads += 1
        if data and nonempty_reads == replace_on_read and not replaced:
            replaced = True
            os.replace(replacement, selected)
        return data

    monkeypatch.setattr(constraint_module.os, "read", replace_matching_read)

    with pytest.raises(PaperExecutionConstraintIntegrityError, match="changed"):
        _authority(root).load(observed_at=AVAILABLE_AT)


def test_missing_malformed_future_or_oversized_authority_fails_closed(tmp_path: Path) -> None:
    missing = tmp_path / "missing"
    with pytest.raises(PaperExecutionConstraintUnavailableError, match="unavailable"):
        _authority(missing).load(observed_at=AVAILABLE_AT)

    root, _pointer = _publish_authority(tmp_path / "malformed")
    (root / "current.json").write_text("{not-json", encoding="utf-8")
    with pytest.raises(PaperExecutionConstraintIntegrityError, match="pointer"):
        _authority(root).load(observed_at=AVAILABLE_AT)

    future_root, _pointer = _publish_authority(
        tmp_path / "future",
        now=AVAILABLE_AT + timedelta(microseconds=1),
    )
    with pytest.raises(PaperExecutionConstraintUnavailableError, match="not yet available"):
        _authority(future_root).load(observed_at=AVAILABLE_AT)

    large_root, _pointer = _publish_authority(tmp_path / "large")
    with pytest.raises(PaperExecutionConstraintIntegrityError, match="size"):
        _authority(large_root, max_bytes=32).load(observed_at=AVAILABLE_AT)


def test_naive_observed_at_and_unsafe_models_are_rejected(tmp_path: Path) -> None:
    root, _pointer = _publish_authority(tmp_path)
    with pytest.raises(PaperExecutionConstraintUnavailableError, match="timezone-aware"):
        _authority(root).load(observed_at=datetime(2026, 7, 21, 9, 31))

    snapshot_payload = _snapshot_payload()
    snapshot_payload["unknown"] = "forbidden"
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        PaperExecutionConstraintSnapshot.model_validate(snapshot_payload)

    batch_payload = _batch_payload()
    batch_payload["content_hash"] = "f" * 64
    with pytest.raises(ValidationError, match="batch content_hash"):
        PaperExecutionConstraintBatch.model_validate(batch_payload)


@pytest.mark.parametrize("ts_code", ["600000", "ABC.SH", "600000.NY", ""])
def test_snapshot_requires_canonical_a_share_code(ts_code: str) -> None:
    with pytest.raises(ValidationError, match="ts_code"):
        PaperExecutionConstraintSnapshot.model_validate(_snapshot_payload(ts_code=ts_code))
