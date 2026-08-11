from __future__ import annotations

import json
import os
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

import rquant.runtime_serving_authority as authority_module
from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_serving_authority import (
    ServingSourceAuthorityDocument,
    ServingSourceAuthorityIntegrityError,
    ServingSourceAuthorityPointer,
    ServingSourceAuthorityPublisher,
    ServingSourceAuthorityReader,
    ServingSourceAuthorityUnavailableError,
)
from rquant.runtime_serving_snapshot import (
    SIGNALS_DATASET_ID,
    SignalDeliveryPayload,
    SourceReadResult,
)
from rquant.serving_contracts import FreshnessStatus

NOW = datetime(2026, 8, 1, 2, 31, tzinfo=UTC)
COMMIT = "a" * 40


def _result(
    *,
    sequence: int = 7,
    event_time: datetime = NOW - timedelta(seconds=2),
    published_at: datetime = NOW - timedelta(seconds=1),
) -> SourceReadResult:
    values: dict[str, object] = {
        "dataset_id": SIGNALS_DATASET_ID,
        "sequence": sequence,
        "event_time": event_time,
        "published_at": published_at,
        "status": FreshnessStatus.FRESH,
        "reason": None,
        "payload": SignalDeliveryPayload(),
    }
    values["generation_id"] = canonical_sha256(values)
    return SourceReadResult.model_validate(values)


def _publisher(
    root: Path,
    *,
    clock: object | None = None,
    producer_commit: str = COMMIT,
) -> ServingSourceAuthorityPublisher:
    return ServingSourceAuthorityPublisher(
        root=root,
        producer_commit=producer_commit,
        dataset_id=SIGNALS_DATASET_ID,
        payload_kind="signal_delivery",
        clock=clock or (lambda: NOW),  # type: ignore[arg-type]
    )


def _reader(
    root: Path,
    *,
    max_bytes: int = 64 * 1024,
    history_scan_limit: int = 1_024,
) -> ServingSourceAuthorityReader:
    return ServingSourceAuthorityReader(
        root=root,
        expected_producer_commit=COMMIT,
        expected_dataset_id=SIGNALS_DATASET_ID,
        expected_payload_kind="signal_delivery",
        max_bytes=max_bytes,
        history_scan_limit=history_scan_limit,
    )


def _canonical_json(value: object) -> bytes:
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json")
    return json.dumps(
        value,
        default=lambda item: item.isoformat() if isinstance(item, datetime) else item,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _write_pointer(root: Path, values: dict[str, object]) -> None:
    if values.get("publication_id") is not None:
        publication_values = {
            key: value
            for key, value in values.items()
            if key not in {"content_hash", "publication_id"}
        }
        values["publication_id"] = canonical_sha256(
            {
                "contract": "serving-source-authority-publication/v1",
                **publication_values,
            }
        )
    values["content_hash"] = canonical_sha256(
        {key: value for key, value in values.items() if key != "content_hash"}
    )
    temporary = root / "replacement-current.json"
    temporary.write_bytes(_canonical_json(values))
    os.replace(temporary, root / "current.json")


def _mutate_after_initial_pointer_read(
    monkeypatch: pytest.MonkeyPatch,
    mutation: Callable[[], None],
) -> None:
    original_read = authority_module._read_regular_file_at
    mutated = False

    def read_regular_file_at(
        directory_fd: int,
        name: str,
        *,
        max_bytes: int,
        label: str,
        missing_unavailable: bool,
        optional: bool = False,
    ) -> bytes | None:
        nonlocal mutated
        payload = original_read(
            directory_fd,
            name,
            max_bytes=max_bytes,
            label=label,
            missing_unavailable=missing_unavailable,
            optional=optional,
        )
        if name == "current.json" and not mutated:
            mutated = True
            mutation()
        return payload

    monkeypatch.setattr(authority_module, "_read_regular_file_at", read_regular_file_at)


def _mutate_directory(
    path: Path,
    mutation: str,
    *,
    rename: Callable[[Path, Path], None] = os.rename,
) -> Path:
    if mutation == "mode":
        os.chmod(path, 0o777)
        return path
    retired = path.with_name(f"retired-{path.name}")
    rename(path, retired)
    if mutation == "replacement":
        path.mkdir()
    else:
        path.symlink_to(retired, target_is_directory=True)
    return retired


def _rebind_publication_pointer(
    pointer: ServingSourceAuthorityPointer,
    *,
    previous_publication_id: str | None,
) -> ServingSourceAuthorityPointer:
    values = pointer.model_dump(
        mode="python",
        exclude={"content_hash", "publication_id"},
    )
    values["previous_publication_id"] = previous_publication_id
    values["publication_id"] = canonical_sha256(
        {
            "contract": "serving-source-authority-publication/v1",
            **values,
        }
    )
    values["content_hash"] = canonical_sha256(values)
    return ServingSourceAuthorityPointer.model_validate(values)


def test_publisher_and_reader_follow_dynamic_current_pointer(tmp_path: Path) -> None:
    root = tmp_path / "authority"
    first = _result(sequence=7)
    second = _result(
        sequence=8,
        event_time=NOW + timedelta(seconds=8),
        published_at=NOW + timedelta(seconds=9),
    )
    publication_times = iter((NOW, NOW + timedelta(seconds=10)))
    publisher = _publisher(root, clock=lambda: next(publication_times))
    reader = _reader(root)

    first_pointer = publisher.publish(first)
    assert reader(NOW) == first

    second_pointer = publisher.publish(second)
    assert reader(NOW + timedelta(seconds=10)) == second
    assert first_pointer.generation_id != second_pointer.generation_id
    assert (root / "generations" / f"{first.generation_id}.json").is_file()
    assert (root / "generations" / f"{second.generation_id}.json").is_file()
    assert len(tuple((root / "generations").glob("*.json"))) == 2


def test_idempotent_publish_keeps_one_immutable_generation(tmp_path: Path) -> None:
    root = tmp_path / "authority"
    publication_times = iter((NOW, NOW + timedelta(seconds=10), NOW + timedelta(seconds=11)))
    publisher = _publisher(root, clock=lambda: next(publication_times))
    result = _result()

    first = publisher.publish(result)
    generation_bytes = (root / "generations" / f"{result.generation_id}.json").read_bytes()
    current_bytes = (root / "current.json").read_bytes()
    second = publisher.publish(result)

    assert second == first
    assert (root / "generations" / f"{result.generation_id}.json").read_bytes() == generation_bytes
    assert (root / "current.json").read_bytes() == current_bytes
    assert len(tuple((root / "generations").glob("*.json"))) == 1


@pytest.mark.parametrize("target", ("root", "generations"))
@pytest.mark.parametrize("mutation", ("replacement", "symlink", "mode"))
def test_publisher_fails_closed_when_authority_directory_changes_before_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    target: str,
    mutation: str,
) -> None:
    authority_parent = tmp_path / "trusted-parent"
    authority_parent.mkdir()
    root = authority_parent / "authority"
    publisher = _publisher(
        root,
        clock=lambda: NOW + timedelta(seconds=10),
    )
    first = _result()
    publisher.publish(first)
    first_current = (root / "current.json").read_bytes()
    original_replace = authority_module._replace_current_pointer
    physical_root = root

    def replace_current_pointer(
        root_fd: int,
        payload: bytes,
        **kwargs: object,
    ) -> None:
        nonlocal physical_root
        selected = root if target == "root" else root / "generations"
        retired = _mutate_directory(selected, mutation)
        if target == "root" and mutation != "mode":
            physical_root = retired
        original_replace(root_fd, payload, **kwargs)

    monkeypatch.setattr(
        authority_module,
        "_replace_current_pointer",
        replace_current_pointer,
    )
    second = _result(
        sequence=8,
        event_time=NOW + timedelta(seconds=8),
        published_at=NOW + timedelta(seconds=9),
    )

    with pytest.raises(ServingSourceAuthorityIntegrityError, match="directory changed"):
        publisher.publish(second)

    assert (physical_root / "current.json").read_bytes() == first_current
    assert not tuple(physical_root.glob(".current.*.tmp"))


@pytest.mark.parametrize("target", ("root", "generations"))
def test_publisher_fails_closed_when_authority_directory_swaps_after_current_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    target: str,
) -> None:
    authority_parent = tmp_path / "trusted-parent"
    authority_parent.mkdir()
    root = authority_parent / "authority"
    publisher = _publisher(
        root,
        clock=lambda: NOW + timedelta(seconds=10),
    )
    publisher.publish(_result())
    original_rename = authority_module.os.rename
    physical_root = root
    mutated = False

    def rename(
        source: str,
        destination: str,
        *,
        src_dir_fd: int | None = None,
        dst_dir_fd: int | None = None,
    ) -> None:
        nonlocal mutated, physical_root
        original_rename(
            source,
            destination,
            src_dir_fd=src_dir_fd,
            dst_dir_fd=dst_dir_fd,
        )
        if destination != "current.json" or mutated:
            return
        mutated = True
        selected = root if target == "root" else root / "generations"
        retired = _mutate_directory(selected, "replacement", rename=original_rename)
        if target == "root":
            physical_root = retired

    monkeypatch.setattr(authority_module.os, "rename", rename)
    second = _result(
        sequence=8,
        event_time=NOW + timedelta(seconds=8),
        published_at=NOW + timedelta(seconds=9),
    )

    with pytest.raises(ServingSourceAuthorityIntegrityError, match="directory changed"):
        publisher.publish(second)

    assert mutated is True
    assert not tuple(physical_root.glob(".current.*.tmp"))


@pytest.mark.parametrize("mutation", ("content", "inode"))
def test_publisher_verifies_current_target_after_atomic_replace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    root = tmp_path / "authority"
    publisher = _publisher(
        root,
        clock=lambda: NOW + timedelta(seconds=10),
    )
    publisher.publish(_result())
    original_rename = authority_module.os.rename
    mutated = False

    def rename(
        source: str,
        destination: str,
        *,
        src_dir_fd: int | None = None,
        dst_dir_fd: int | None = None,
    ) -> None:
        nonlocal mutated
        original_rename(
            source,
            destination,
            src_dir_fd=src_dir_fd,
            dst_dir_fd=dst_dir_fd,
        )
        if destination != "current.json" or mutated:
            return
        mutated = True
        current = root / "current.json"
        if mutation == "content":
            current.write_bytes(b'{"content_hash":"tampered"}')
            return
        replacement = root / "same-current.json"
        replacement.write_bytes(current.read_bytes())
        os.chmod(replacement, 0o600)
        os.replace(replacement, current)

    monkeypatch.setattr(authority_module.os, "rename", rename)
    second = _result(
        sequence=8,
        event_time=NOW + timedelta(seconds=8),
        published_at=NOW + timedelta(seconds=9),
    )

    with pytest.raises(ServingSourceAuthorityIntegrityError, match="current pointer"):
        publisher.publish(second)

    assert mutated is True
    assert not tuple(root.glob(".current.*.tmp"))


def test_idempotent_publisher_revalidates_directory_chain_before_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority_parent = tmp_path / "trusted-parent"
    authority_parent.mkdir()
    root = authority_parent / "authority"
    publisher = _publisher(root)
    result = _result()
    publisher.publish(result)
    original_archive = authority_module._archive_publication

    def archive_publication(
        directory_fd: int,
        *,
        pointer: ServingSourceAuthorityPointer,
        payload: bytes,
        max_bytes: int,
    ) -> str:
        publication_id = original_archive(
            directory_fd,
            pointer=pointer,
            payload=payload,
            max_bytes=max_bytes,
        )
        _mutate_directory(root, "replacement")
        return publication_id

    monkeypatch.setattr(authority_module, "_archive_publication", archive_publication)

    with pytest.raises(ServingSourceAuthorityIntegrityError, match="directory changed"):
        publisher.publish(result)


def test_publisher_allows_parent_create_delete_and_utime_during_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority_parent = tmp_path / "trusted-parent"
    authority_parent.mkdir()
    root = authority_parent / "authority"
    publisher = _publisher(
        root,
        clock=lambda: NOW + timedelta(seconds=10),
    )
    publisher.publish(_result())
    delete_target = authority_parent / "delete-me"
    delete_target.write_text("old", encoding="utf-8")
    original_publish_generation = authority_module._publish_immutable_generation

    def publish_immutable_generation(
        directory_fd: int,
        *,
        generation_id: str,
        producer_commit: str,
        commit_bound: bool,
        payload: bytes,
        max_bytes: int,
    ) -> None:
        original_publish_generation(
            directory_fd,
            generation_id=generation_id,
            producer_commit=producer_commit,
            commit_bound=commit_bound,
            payload=payload,
            max_bytes=max_bytes,
        )
        (authority_parent / "created-during-publish").write_text(
            "new",
            encoding="utf-8",
        )
        delete_target.unlink()
        parent_stat = authority_parent.stat()
        os.utime(
            authority_parent,
            ns=(parent_stat.st_atime_ns, parent_stat.st_mtime_ns - 1_000_000),
        )

    monkeypatch.setattr(
        authority_module,
        "_publish_immutable_generation",
        publish_immutable_generation,
    )
    second = _result(
        sequence=8,
        event_time=NOW + timedelta(seconds=8),
        published_at=NOW + timedelta(seconds=9),
    )

    pointer = publisher.publish(second)

    assert pointer.generation_id == second.generation_id
    assert _reader(root)(NOW + timedelta(seconds=10)) == second


def test_document_and_pointer_are_strict_frozen_and_content_bound() -> None:
    result = _result()
    document_values: dict[str, object] = {
        "schema_version": 1,
        "producer_commit": COMMIT,
        "result": result,
    }
    document_values["content_hash"] = canonical_sha256(document_values)
    document = ServingSourceAuthorityDocument.model_validate(document_values)
    pointer_values: dict[str, object] = {
        "schema_version": 1,
        "generation_id": result.generation_id,
        "file_sha256": "b" * 64,
        "published_at": result.published_at,
        "producer_commit": COMMIT,
        "dataset_id": SIGNALS_DATASET_ID,
        "payload_kind": "signal_delivery",
    }
    pointer_values["content_hash"] = canonical_sha256(pointer_values)
    pointer = ServingSourceAuthorityPointer.model_validate(pointer_values)

    with pytest.raises(ValidationError, match="frozen"):
        document.producer_commit = "b" * 40  # type: ignore[misc]
    with pytest.raises(ValidationError, match="frozen"):
        pointer.dataset_id = "other"  # type: ignore[misc]
    unknown = dict(pointer_values, unexpected=True)
    unknown["content_hash"] = canonical_sha256(
        {key: value for key, value in unknown.items() if key != "content_hash"}
    )
    with pytest.raises(ValidationError, match="extra"):
        ServingSourceAuthorityPointer.model_validate(unknown)
    coerced = dict(document_values, schema_version="1")
    coerced["content_hash"] = canonical_sha256(
        {key: value for key, value in coerced.items() if key != "content_hash"}
    )
    with pytest.raises(ValidationError, match="literal_error"):
        ServingSourceAuthorityDocument.model_validate(coerced)


def test_publisher_never_overwrites_conflicting_generation(tmp_path: Path) -> None:
    root = tmp_path / "authority"
    publisher = _publisher(root)
    result = _result()
    publisher.publish(result)
    path = root / "generations" / f"{result.generation_id}.json"
    path.write_bytes(b"conflicting immutable bytes")

    with pytest.raises(ServingSourceAuthorityIntegrityError, match="immutable generation"):
        publisher.publish(result)
    assert path.read_bytes() == b"conflicting immutable bytes"


def test_publisher_rejects_noncanonical_result_and_owner_mismatch(tmp_path: Path) -> None:
    root = tmp_path / "authority"
    result = _result()
    bad_generation = result.model_copy(update={"generation_id": "f" * 64})
    wrong_dataset = result.model_copy(update={"dataset_id": "paper_accounts"})

    with pytest.raises(ServingSourceAuthorityIntegrityError, match="generation_id"):
        _publisher(root).publish(bad_generation)
    with pytest.raises(ServingSourceAuthorityIntegrityError, match="dataset_id"):
        _publisher(root).publish(wrong_dataset)


def test_reader_rejects_corrupt_pointer_and_generation_gap(tmp_path: Path) -> None:
    root = tmp_path / "authority"
    pointer = _publisher(root).publish(_result())
    current = root / "current.json"
    current.write_text("{not-json", encoding="utf-8")
    with pytest.raises(ServingSourceAuthorityIntegrityError, match="pointer"):
        _reader(root)(NOW)

    gap_generation = "f" * 64
    _write_pointer(
        root,
        {
            "schema_version": 1,
            "generation_id": gap_generation,
            "file_sha256": pointer.file_sha256,
            "published_at": pointer.published_at,
            "producer_commit": COMMIT,
            "dataset_id": SIGNALS_DATASET_ID,
            "payload_kind": "signal_delivery",
        },
    )
    with pytest.raises(ServingSourceAuthorityIntegrityError, match="generation gap"):
        _reader(root)(NOW)


def test_reader_and_publisher_reject_generation_rollback(tmp_path: Path) -> None:
    root = tmp_path / "authority"
    publication_times = iter((NOW, NOW + timedelta(seconds=10), NOW + timedelta(seconds=11)))
    publisher = _publisher(root, clock=lambda: next(publication_times))
    first = _result(sequence=7)
    second = _result(
        sequence=8,
        event_time=NOW + timedelta(seconds=8),
        published_at=NOW + timedelta(seconds=9),
    )
    first_pointer = publisher.publish(first)
    second_pointer = publisher.publish(second)
    reader = _reader(root)
    assert reader(NOW + timedelta(seconds=10)) == second

    with pytest.raises(ServingSourceAuthorityIntegrityError, match="rollback"):
        publisher.publish(first)

    _write_pointer(
        root,
        first_pointer.model_dump(mode="python", exclude={"content_hash"}),
    )
    with pytest.raises(ServingSourceAuthorityIntegrityError, match="rollback"):
        reader(NOW + timedelta(seconds=10))
    assert second_pointer.generation_id == second.generation_id


@pytest.mark.parametrize("field", ["event_time", "published_at", "pointer"])
def test_reader_never_exposes_future_evidence(tmp_path: Path, field: str) -> None:
    root = tmp_path / "authority"
    result = _result()
    pointer = _publisher(root).publish(result)
    if field == "pointer":
        values = pointer.model_dump(mode="python", exclude={"content_hash"})
        values["published_at"] = NOW + timedelta(microseconds=1)
        _write_pointer(root, values)
    else:
        future = NOW + timedelta(microseconds=1)
        values = result.model_dump(mode="python", exclude={"generation_id"})
        values[field] = future
        if field == "event_time":
            values["published_at"] = future
        values["generation_id"] = canonical_sha256(values)
        future_result = SourceReadResult.model_validate(values)
        _publisher(
            tmp_path / f"future-{field}",
            clock=lambda: future,
        ).publish(future_result)
        root = tmp_path / f"future-{field}"

    with pytest.raises(ServingSourceAuthorityUnavailableError, match="not yet available"):
        _reader(root)(NOW)


def test_reader_rejects_naive_as_of_and_missing_current_is_unavailable(
    tmp_path: Path,
) -> None:
    root = tmp_path / "authority"
    root.mkdir()
    reader = _reader(root)

    with pytest.raises(ServingSourceAuthorityUnavailableError, match="timezone-aware"):
        reader(NOW.replace(tzinfo=None))
    with pytest.raises(ServingSourceAuthorityUnavailableError, match="current.*unavailable"):
        reader(NOW)


@pytest.mark.parametrize(
    "target",
    ["root", "current", "generations", "generation"],
)
def test_reader_rejects_symlinks_anywhere_in_authority_chain(
    tmp_path: Path,
    target: str,
) -> None:
    physical = tmp_path / "physical"
    result = _result()
    _publisher(physical).publish(result)
    root = physical
    if target == "root":
        alias = tmp_path / "authority"
        alias.symlink_to(physical, target_is_directory=True)
        root = alias
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
        generation = physical / "generations" / f"{result.generation_id}.json"
        saved = physical / "generations" / "saved.json"
        generation.replace(saved)
        generation.symlink_to(saved)

    with pytest.raises(ServingSourceAuthorityIntegrityError, match="unsafe|symlink"):
        _reader(root)(NOW)


def test_reader_accepts_unrelated_write_to_authority_parent_during_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority_parent = tmp_path / "trusted-parent"
    authority_parent.mkdir()
    root = authority_parent / "authority"
    result = _result()
    _publisher(root).publish(result)

    _mutate_after_initial_pointer_read(
        monkeypatch,
        lambda: (authority_parent / "unrelated").write_text("probe", encoding="utf-8"),
    )

    assert _reader(root)(NOW) == result


def test_reader_stays_stable_across_many_unrelated_authority_parent_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority_parent = tmp_path / "trusted-parent"
    authority_parent.mkdir()
    root = authority_parent / "authority"
    result = _result()
    _publisher(root).publish(result)
    mutation_count = 0
    original_read = authority_module._read_regular_file_at
    current_read_count = 0

    def read_regular_file_at(
        directory_fd: int,
        name: str,
        *,
        max_bytes: int,
        label: str,
        missing_unavailable: bool,
        optional: bool = False,
    ) -> bytes | None:
        nonlocal current_read_count, mutation_count
        payload = original_read(
            directory_fd,
            name,
            max_bytes=max_bytes,
            label=label,
            missing_unavailable=missing_unavailable,
            optional=optional,
        )
        if name == "current.json":
            current_read_count += 1
            if current_read_count % 2:
                (authority_parent / f"unrelated-{mutation_count}").write_text(
                    "probe",
                    encoding="utf-8",
                )
                mutation_count += 1
        return payload

    monkeypatch.setattr(authority_module, "_read_regular_file_at", read_regular_file_at)

    for _ in range(64):
        assert _reader(root)(NOW) == result
    assert mutation_count == 64


@pytest.mark.parametrize("mutation", ("replacement", "symlink", "mode"))
def test_reader_rejects_unsafe_ancestor_change_during_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    authority_parent = tmp_path / "trusted-parent"
    authority_parent.mkdir()
    root = authority_parent / "authority"
    _publisher(root).publish(_result())

    def mutate_ancestor() -> None:
        if mutation == "mode":
            os.chmod(authority_parent, 0o777)
            return
        retired_parent = tmp_path / "retired-parent"
        authority_parent.rename(retired_parent)
        if mutation == "replacement":
            authority_parent.mkdir()
            return
        authority_parent.symlink_to(retired_parent, target_is_directory=True)

    _mutate_after_initial_pointer_read(monkeypatch, mutate_ancestor)

    with pytest.raises(ServingSourceAuthorityIntegrityError, match="directory changed"):
        _reader(root)(NOW)


def test_reader_rejects_authority_root_replacement_during_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority_parent = tmp_path / "trusted-parent"
    authority_parent.mkdir()
    root = authority_parent / "authority"
    _publisher(root).publish(_result())

    def replace_root() -> None:
        retired_root = authority_parent / "retired-authority"
        root.rename(retired_root)
        root.mkdir()

    _mutate_after_initial_pointer_read(monkeypatch, replace_root)

    with pytest.raises(ServingSourceAuthorityIntegrityError, match="directory changed"):
        _reader(root)(NOW)


@pytest.mark.parametrize("field", ("st_uid", "st_gid"))
def test_directory_verifier_rejects_ancestor_owner_identity_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
) -> None:
    authority_parent = tmp_path / "trusted-parent"
    authority_parent.mkdir()
    root = authority_parent / "authority"
    _publisher(root).publish(_result())
    chain = authority_module._open_existing_directory_chain(root)
    ancestor_fd = chain[-2][0]
    original_fstat = authority_module.os.fstat

    def fstat(file_descriptor: int) -> object:
        observed = original_fstat(file_descriptor)
        if file_descriptor != ancestor_fd:
            return observed
        values = {
            "st_mode": observed.st_mode,
            "st_ino": observed.st_ino,
            "st_dev": observed.st_dev,
            "st_nlink": observed.st_nlink,
            "st_uid": observed.st_uid,
            "st_gid": observed.st_gid,
            "st_size": observed.st_size,
            "st_mtime_ns": observed.st_mtime_ns,
            "st_ctime_ns": observed.st_ctime_ns,
        }
        values[field] += 1
        return SimpleNamespace(**values)

    monkeypatch.setattr(authority_module.os, "fstat", fstat)
    try:
        with pytest.raises(
            ServingSourceAuthorityIntegrityError,
            match="authority directory changed while being read",
        ):
            authority_module._verify_directory_chain(chain)
    finally:
        authority_module._close_directory_chain(chain)


@pytest.mark.parametrize("target", ["current", "generation"])
def test_reader_detects_pointer_or_generation_replacement_during_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    target: str,
) -> None:
    root = tmp_path / "authority"
    result = _result()
    _publisher(root).publish(result)
    selected = (
        root / "current.json"
        if target == "current"
        else root / "generations" / f"{result.generation_id}.json"
    )
    replacement = tmp_path / f"replacement-{target}.json"
    replacement.write_bytes(selected.read_bytes())
    original_read = authority_module.os.read
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

    monkeypatch.setattr(authority_module.os, "read", replace_matching_read)

    with pytest.raises(ServingSourceAuthorityIntegrityError, match="changed"):
        _reader(root)(NOW)


def test_reader_validates_dynamic_pointer_file_and_owner_bindings(tmp_path: Path) -> None:
    root = tmp_path / "authority"
    pointer = _publisher(root).publish(_result())
    valid_pointer = pointer.model_dump(mode="python", exclude={"content_hash"})

    for field, value, message in (
        ("file_sha256", "f" * 64, "file sha256"),
        ("producer_commit", "b" * 40, "producer_commit"),
        ("dataset_id", "paper_accounts", "dataset_id"),
        ("payload_kind", "paper_accounts", "payload_kind"),
    ):
        values = pointer.model_dump(mode="python", exclude={"content_hash"})
        values[field] = value
        _write_pointer(root, values)
        with pytest.raises(ServingSourceAuthorityIntegrityError, match=message):
            _reader(root)(NOW)
        _write_pointer(root, dict(valid_pointer))


def test_reader_rejects_oversized_pointer_or_generation(tmp_path: Path) -> None:
    root = tmp_path / "authority"
    result = _result()
    pointer = _publisher(root).publish(result)
    current = root / "current.json"
    current.write_bytes(b"x" * 1025)
    with pytest.raises(ServingSourceAuthorityIntegrityError, match="size"):
        _reader(root, max_bytes=1024)(NOW)

    _write_pointer(
        root,
        pointer.model_dump(mode="python", exclude={"content_hash"}),
    )
    generation = root / "generations" / f"{result.generation_id}.json"
    generation.write_bytes(b"x" * 1025)
    with pytest.raises(ServingSourceAuthorityIntegrityError, match="size"):
        _reader(root, max_bytes=1024)(NOW)


def test_new_commit_publisher_can_supersede_verified_previous_commit(
    tmp_path: Path,
) -> None:
    root = tmp_path / "authority"
    old_commit = "a" * 40
    new_commit = "b" * 40
    _publisher(root, producer_commit=old_commit).publish(_result(sequence=7))
    next_result = _result(
        sequence=8,
        event_time=NOW + timedelta(seconds=1),
        published_at=NOW + timedelta(seconds=1),
    )

    pointer = _publisher(
        root,
        producer_commit=new_commit,
        clock=lambda: NOW + timedelta(seconds=2),
    ).publish(next_result)

    assert pointer.producer_commit == new_commit
    assert (
        ServingSourceAuthorityReader(
            root=root,
            expected_producer_commit=new_commit,
            expected_dataset_id=SIGNALS_DATASET_ID,
            expected_payload_kind="signal_delivery",
        )(NOW + timedelta(seconds=2))
        == next_result
    )


def test_new_commit_can_republish_identical_generation_without_advancing_source(
    tmp_path: Path,
) -> None:
    root = tmp_path / "authority"
    result = _result(sequence=7)
    first = _publisher(root, producer_commit=COMMIT).publish(result)
    next_commit = "b" * 40

    second = _publisher(
        root,
        producer_commit=next_commit,
        clock=lambda: NOW + timedelta(seconds=1),
    ).publish(result)

    assert second.generation_id == first.generation_id
    assert second.file_sha256 != first.file_sha256
    assert second.publication_id != first.publication_id
    assert second.previous_publication_id == first.publication_id
    reader = ServingSourceAuthorityReader(
        root=root,
        expected_producer_commit=next_commit,
        expected_dataset_id=SIGNALS_DATASET_ID,
        expected_payload_kind="signal_delivery",
        trusted_historical_producer_commits=(COMMIT,),
    )
    assert reader(NOW + timedelta(seconds=1)) == result
    assert reader(NOW) == result


def test_historical_commit_requires_an_explicit_reader_trust_binding(
    tmp_path: Path,
) -> None:
    root = tmp_path / "authority"
    old_commit = "a" * 40
    new_commit = "b" * 40
    old_result = _result(sequence=7)
    _publisher(root, producer_commit=old_commit).publish(old_result)
    _publisher(
        root,
        producer_commit=new_commit,
        clock=lambda: NOW + timedelta(seconds=2),
    ).publish(
        _result(
            sequence=8,
            event_time=NOW + timedelta(seconds=1),
            published_at=NOW + timedelta(seconds=1),
        )
    )
    strict_reader = ServingSourceAuthorityReader(
        root=root,
        expected_producer_commit=new_commit,
        expected_dataset_id=SIGNALS_DATASET_ID,
        expected_payload_kind="signal_delivery",
    )
    with pytest.raises(ServingSourceAuthorityIntegrityError, match="not trusted"):
        strict_reader(NOW)

    migration_reader = ServingSourceAuthorityReader(
        root=root,
        expected_producer_commit=new_commit,
        expected_dataset_id=SIGNALS_DATASET_ID,
        expected_payload_kind="signal_delivery",
        trusted_historical_producer_commits=(old_commit,),
    )
    assert migration_reader(NOW) == old_result


def test_pointer_visibility_uses_trusted_publisher_clock_not_result_timestamp(
    tmp_path: Path,
) -> None:
    root = tmp_path / "authority"
    delayed_publish = NOW + timedelta(minutes=5)
    result = _result(
        event_time=NOW - timedelta(minutes=2),
        published_at=NOW - timedelta(minutes=1),
    )
    pointer = _publisher(root, clock=lambda: delayed_publish).publish(result)

    assert pointer.published_at == delayed_publish
    with pytest.raises(ServingSourceAuthorityUnavailableError, match="not yet available"):
        _reader(root)(NOW)
    assert _reader(root)(delayed_publish) == result


def test_reader_uses_latest_verified_historical_generation_when_current_is_future(
    tmp_path: Path,
) -> None:
    root = tmp_path / "authority"
    first = _result(sequence=7)
    second_time = NOW + timedelta(seconds=10)
    second = _result(
        sequence=8,
        event_time=second_time - timedelta(seconds=2),
        published_at=second_time - timedelta(seconds=1),
    )
    third_time = NOW + timedelta(seconds=20)
    third = _result(
        sequence=9,
        event_time=third_time - timedelta(seconds=2),
        published_at=third_time - timedelta(seconds=1),
    )
    clocks = iter((NOW, second_time, third_time))
    publisher = _publisher(root, clock=lambda: next(clocks))
    publisher.publish(first)
    publisher.publish(second)
    publisher.publish(third)
    reader = _reader(root)

    assert reader(second_time + timedelta(seconds=1)) == second
    assert reader(third_time) == third


def test_reader_fails_closed_when_visible_history_exceeds_scan_bound(
    tmp_path: Path,
) -> None:
    root = tmp_path / "authority"
    clocks = iter((NOW, NOW + timedelta(seconds=10), NOW + timedelta(seconds=20)))
    publisher = _publisher(root, clock=lambda: next(clocks))
    publisher.publish(_result(sequence=7))
    publisher.publish(
        _result(
            sequence=8,
            event_time=NOW + timedelta(seconds=8),
            published_at=NOW + timedelta(seconds=9),
        )
    )
    publisher.publish(
        _result(
            sequence=9,
            event_time=NOW + timedelta(seconds=18),
            published_at=NOW + timedelta(seconds=19),
        )
    )

    with pytest.raises(ServingSourceAuthorityUnavailableError, match="scan limit"):
        _reader(root, history_scan_limit=1)(NOW)


def test_reader_rejects_tampered_historical_publication(tmp_path: Path) -> None:
    root = tmp_path / "authority"
    first = _publisher(root).publish(_result(sequence=7))
    _publisher(root, clock=lambda: NOW + timedelta(seconds=10)).publish(
        _result(
            sequence=8,
            event_time=NOW + timedelta(seconds=8),
            published_at=NOW + timedelta(seconds=9),
        )
    )
    assert first.publication_id is not None
    publication = root / "publications" / f"{first.publication_id}.json"
    publication.write_bytes(b"{not-json")

    with pytest.raises(ServingSourceAuthorityIntegrityError, match="publication"):
        _reader(root)(NOW)


def test_reader_rejects_ambiguous_historical_generations_at_same_rank(
    tmp_path: Path,
) -> None:
    root = tmp_path / "authority"
    first = _publisher(root).publish(_result(sequence=7))
    current = _publisher(root, clock=lambda: NOW + timedelta(seconds=10)).publish(
        _result(
            sequence=8,
            event_time=NOW + timedelta(seconds=8),
            published_at=NOW + timedelta(seconds=9),
        )
    )
    alternate_root = tmp_path / "alternate"
    alternate = _publisher(alternate_root).publish(
        _result(
            sequence=7,
            event_time=NOW - timedelta(seconds=3),
            published_at=NOW - timedelta(seconds=2),
        )
    )
    assert first.publication_id is not None
    alternate = _rebind_publication_pointer(
        alternate,
        previous_publication_id=first.publication_id,
    )
    assert alternate.publication_id is not None
    alternate_generation = alternate_root / "generations" / f"{alternate.generation_id}.json"
    (root / "generations" / f"{alternate.generation_id}.json").write_bytes(
        alternate_generation.read_bytes()
    )
    (root / "publications" / f"{alternate.publication_id}.json").write_bytes(
        _canonical_json(alternate)
    )
    current = _rebind_publication_pointer(
        current,
        previous_publication_id=alternate.publication_id,
    )
    _write_pointer(
        root,
        current.model_dump(mode="python", exclude={"content_hash"}),
    )

    with pytest.raises(ServingSourceAuthorityIntegrityError, match="ambiguous"):
        _reader(root)(NOW)
