"""Immutable SQLite authority for research factor definitions."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from pathlib import Path
from threading import Barrier

import pytest
from pydantic import ValidationError

from rquant.factor.definition import FactorDefinition, build_factor_definition
from rquant.factor.expression import FeatureCatalog
from rquant.factor.registry import (
    ArchiveFactorRequest,
    FactorConflictError,
    FactorDefinitionReceipt,
    FactorDefinitionRecord,
    FactorDefinitionRegistry,
    FactorHeadRef,
    FactorIntegrityError,
    FactorRegistryIdentityError,
    SaveFactorDefinitionRequest,
)


def _definition(
    *, version: int = 1, name: str = "价量强度", factor_id: str = "price_volume_1"
) -> FactorDefinition:
    return build_factor_definition(
        factor_id=factor_id,
        name_zh=name,
        category="technical",
        direction="higher_is_better",
        version=version,
        earliest_available_date=date(2024, 1, 2),
        expression="ts_mean(close, 5) / ref(volume, 2)",
        feature_catalog=FeatureCatalog(columns=("close", "volume")),
    )


def _store(path: Path) -> FactorDefinitionRegistry:
    registry = FactorDefinitionRegistry(path)
    if not path.exists():
        registry.initialize()
    return registry


def _save_request(
    registry: FactorDefinitionRegistry, request: SaveFactorDefinitionRequest
) -> FactorDefinitionReceipt:
    return registry.save(request, expected_identity=registry.identity())


def _archive_request(
    registry: FactorDefinitionRegistry, request: ArchiveFactorRequest
) -> FactorDefinitionReceipt:
    return registry.archive(request, expected_identity=registry.identity())


def _head(registry: FactorDefinitionRegistry, factor_id: str) -> FactorDefinitionRecord | None:
    return registry.get_head(factor_id, expected_identity=registry.identity())


def _version(
    registry: FactorDefinitionRegistry, factor_id: str, version: int
) -> FactorDefinitionRecord | None:
    return registry.get_version(factor_id, version, expected_identity=registry.identity())


def _list(
    registry: FactorDefinitionRegistry, *, include_archived: bool = False, limit: int = 100
) -> tuple[FactorDefinitionRecord, ...]:
    return registry.list_current(
        expected_identity=registry.identity(), include_archived=include_archived, limit=limit
    )


def test_first_save_survives_reopen_with_exact_definition(tmp_path: Path) -> None:
    path = tmp_path / "factors.sqlite3"
    registry = _store(path)
    definition = _definition()

    receipt = _save_request(
        registry,
        SaveFactorDefinitionRequest(
            command_id="save-first", definition=definition, expected_head=None
        ),
    )

    reopened = _store(path)
    head = _head(reopened, definition.factor_id)
    version = _version(reopened, definition.factor_id, 1)
    assert receipt.version == 1
    assert receipt.content_sha256 == head.content_sha256 == version.content_sha256
    assert head.definition == version.definition == definition
    assert head.is_head and version.is_head
    assert not head.head.archived


def test_known_date_definition_retains_legacy_content_digest(tmp_path: Path) -> None:
    registry = _store(tmp_path / "factors.sqlite3")
    receipt = _save_request(
        registry,
        SaveFactorDefinitionRequest(
            command_id="known-date", definition=_definition(), expected_head=None
        ),
    )
    assert receipt.content_sha256 == (
        "8d2108b76521105bd731e52873701613b20562de1be47a328a5918ff94f5f213"
    )


def _save(
    registry: FactorDefinitionRegistry,
    command_id: str,
    definition: FactorDefinition,
    expected_head: FactorHeadRef | None = None,
) -> FactorDefinitionReceipt:
    return _save_request(
        registry,
        SaveFactorDefinitionRequest(
            command_id=command_id, definition=definition, expected_head=expected_head
        ),
    )


def _ref(registry: FactorDefinitionRegistry, factor_id: str = "price_volume_1") -> FactorHeadRef:
    head = _head(registry, factor_id)
    assert head is not None
    return FactorHeadRef(version=head.definition.version, content_sha256=head.content_sha256)


def test_readonly_missing_store_does_not_create_file(tmp_path: Path) -> None:
    path = tmp_path / "absent.sqlite3"
    registry = FactorDefinitionRegistry(path)
    with pytest.raises(FactorRegistryIdentityError):
        registry.identity()
    assert not path.exists()


def test_save_cas_replay_after_advance_and_archival_preserves_original_receipt(
    tmp_path: Path,
) -> None:
    registry = _store(tmp_path / "factors.sqlite3")
    first = _definition()
    receipt_a = _save(registry, "command-a", first)
    receipt_b = _save(
        registry, "command-b", _definition(version=2, name="改进价量强度"), _ref(registry)
    )
    assert receipt_b.version == 2
    assert _version(registry, first.factor_id, 1).definition == first
    assert _save(registry, "command-a", first) == receipt_a
    assert _save(registry, "command-a", first).model_dump_json() == receipt_a.model_dump_json()

    archived = _archive_request(
        registry,
        ArchiveFactorRequest(
            command_id="command-c", factor_id=first.factor_id, expected_head=_ref(registry)
        ),
    )
    assert archived.archived
    assert _list(registry) == ()
    assert _version(registry, first.factor_id, 1).definition == first
    assert _save(registry, "command-a", first) == receipt_a
    assert (
        _archive_request(
            registry,
            ArchiveFactorRequest(
                command_id="command-c",
                factor_id=first.factor_id,
                expected_head=FactorHeadRef(version=2, content_sha256=receipt_b.content_sha256),
            ),
        )
        == archived
    )
    with pytest.raises(FactorConflictError):
        _save(registry, "command-a", _definition(name="重放异载荷"))

    reactivated = _save(
        registry, "command-d", _definition(version=3, name="重新启用"), _ref(registry)
    )
    assert reactivated.version == 3
    assert not reactivated.archived
    assert len(_list(registry)) == 1


def test_stale_or_missing_expected_head_rejects_new_command(tmp_path: Path) -> None:
    registry = _store(tmp_path / "factors.sqlite3")
    first = _save(registry, "first", _definition())
    with pytest.raises(FactorConflictError):
        _save(registry, "stale", _definition(version=2))
    with pytest.raises(FactorConflictError):
        _save(registry, "wrong-next", _definition(version=3), _ref(registry))
    with pytest.raises(FactorConflictError):
        _save(
            registry,
            "bad-cas",
            _definition(version=2),
            FactorHeadRef(version=1, content_sha256="0" * 64),
        )
    with pytest.raises(FactorConflictError):
        _archive_request(
            registry,
            ArchiveFactorRequest(
                command_id="bad-archive",
                factor_id="price_volume_1",
                expected_head=FactorHeadRef(version=1, content_sha256="0" * 64),
            ),
        )
    assert _head(registry, "price_volume_1").content_sha256 == first.content_sha256


def test_archive_replay_after_new_save_returns_original_archive_receipt(tmp_path: Path) -> None:
    registry = _store(tmp_path / "factors.sqlite3")
    _save(registry, "first", _definition())
    request = ArchiveFactorRequest(
        command_id="archive", factor_id="price_volume_1", expected_head=_ref(registry)
    )
    receipt = _archive_request(registry, request)
    _save(registry, "second", _definition(version=2), _ref(registry))
    assert _archive_request(registry, request) == receipt
    assert receipt.archived and not _head(registry, "price_volume_1").head.archived
    with pytest.raises(FactorConflictError):
        _archive_request(
            registry,
            ArchiveFactorRequest(
                command_id="archive", factor_id="price_volume_1", expected_head=_ref(registry)
            ),
        )


def test_replay_rejects_receipt_that_disagrees_with_committed_version(tmp_path: Path) -> None:
    path = tmp_path / "factors.sqlite3"
    registry = _store(path)
    definition = _definition()
    _save(registry, "first", definition)
    with sqlite3.connect(path) as connection:
        receipt = json.loads(
            connection.execute(
                "SELECT receipt_json FROM factor_commands WHERE command_id = 'first'"
            ).fetchone()[0]
        )
        receipt["version"] = 2
        payload = json.dumps(receipt, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        connection.execute(
            "UPDATE factor_commands SET receipt_json = ?, receipt_sha256 = ? "
            "WHERE command_id = 'first'",
            (payload, hashlib.sha256(payload.encode()).hexdigest()),
        )
    with pytest.raises(FactorIntegrityError):
        _save(FactorDefinitionRegistry(path), "first", definition)


def test_two_concurrent_saves_on_same_head_have_one_winner(tmp_path: Path) -> None:
    path = tmp_path / "factors.sqlite3"
    registry = _store(path)
    _save(registry, "first", _definition())
    expected = _ref(registry)
    barrier = Barrier(2)

    def attempt(command_id: str, name: str) -> str:
        barrier.wait(timeout=5)
        try:
            _save(
                FactorDefinitionRegistry(path),
                command_id,
                _definition(version=2, name=name),
                expected,
            )
        except FactorConflictError:
            return "conflict"
        return "saved"

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda arg: attempt(*arg), (("a", "版本甲"), ("b", "版本乙"))))
    assert sorted(results) == ["conflict", "saved"]
    assert _head(registry, "price_volume_1").definition.version == 2


def test_archive_and_save_race_has_serializable_outcome(tmp_path: Path) -> None:
    path = tmp_path / "factors.sqlite3"
    registry = _store(path)
    _save(registry, "first", _definition())
    expected = _ref(registry)
    barrier = Barrier(2)

    def save() -> str:
        barrier.wait(timeout=5)
        try:
            _save_request(
                _store(path),
                SaveFactorDefinitionRequest(
                    command_id="save", definition=_definition(version=2), expected_head=expected
                ),
            )
        except FactorConflictError:
            return "conflict"
        return "saved"

    def archive() -> str:
        barrier.wait(timeout=5)
        try:
            _archive_request(
                _store(path),
                ArchiveFactorRequest(
                    command_id="archive", factor_id="price_volume_1", expected_head=expected
                ),
            )
        except FactorConflictError:
            return "conflict"
        return "archived"

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = [future.result() for future in (executor.submit(save), executor.submit(archive))]
    assert results in (
        ["saved", "conflict"],
        ["conflict", "archived"],
        ["saved", "archived"],
    )
    assert len(_list(registry, include_archived=True)) == 1
    assert _version(registry, "price_volume_1", 1).definition == _definition()
    head = _head(registry, "price_volume_1")
    assert head.definition.version == (2 if results[0] == "saved" else 1)
    assert head.head.archived == (results[0] == "conflict")


@pytest.mark.parametrize("failure_table", ["factor_heads", "factor_commands"])
def test_failure_after_version_insert_rolls_back_entire_command(
    tmp_path: Path, failure_table: str
) -> None:
    path = tmp_path / "factors.sqlite3"
    registry = _store(path)
    _save(registry, "first", _definition())
    expected = _ref(registry)
    with sqlite3.connect(path) as connection:
        if failure_table == "factor_heads":
            connection.execute(
                "CREATE TRIGGER fail_command BEFORE UPDATE ON factor_heads "
                "BEGIN SELECT RAISE(ABORT, 'injected failure'); END"
            )
        else:
            connection.execute(
                "CREATE TRIGGER fail_command BEFORE INSERT ON factor_commands "
                "WHEN NEW.command_id = 'failed' "
                "BEGIN SELECT RAISE(ABORT, 'injected failure'); END"
            )
    with pytest.raises(sqlite3.DatabaseError, match="injected failure"):
        _save(registry, "failed", _definition(version=2), expected)
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM factor_versions").fetchone()[0] == 1
        assert connection.execute("SELECT version FROM factor_heads").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM factor_commands").fetchone()[0] == 1
        connection.execute("DROP TRIGGER fail_command")
    assert (
        _save(FactorDefinitionRegistry(path), "failed", _definition(version=2), expected).version
        == 2
    )


def test_first_write_failure_allows_same_command_to_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "factors.sqlite3"
    registry = _store(path)

    def fail_receipt_insert(
        connection: sqlite3.Connection,
        *,
        receipt: FactorDefinitionReceipt,
        request_sha256: str,
    ) -> None:
        connection.execute("INSERT INTO missing_table VALUES (1)")

    with monkeypatch.context() as patch:
        patch.setattr(
            FactorDefinitionRegistry, "_record_command", staticmethod(fail_receipt_insert)
        )
        with pytest.raises(sqlite3.DatabaseError, match="missing_table"):
            _save(registry, "first", _definition())

    assert _save(FactorDefinitionRegistry(path), "first", _definition()).version == 1
    assert _head(_store(path), "price_volume_1").definition == _definition()


@pytest.mark.parametrize(
    "damage",
    [
        "bad_json",
        "bad_digest",
        "missing_head_version",
        "missing_old_version",
        "orphan_without_head",
    ],
)
def test_every_read_fails_closed_on_damaged_history(tmp_path: Path, damage: str) -> None:
    path = tmp_path / "factors.sqlite3"
    registry = _store(path)
    _save(registry, "first", _definition())
    _save(registry, "second", _definition(version=2), _ref(registry))
    with sqlite3.connect(path) as connection:
        if damage == "bad_json":
            connection.execute(
                "UPDATE factor_versions SET definition_json = '{bad' WHERE version = 1"
            )
        elif damage == "bad_digest":
            connection.execute(
                "UPDATE factor_versions SET content_sha256 = ? WHERE version = 1", ("0" * 64,)
            )
        elif damage == "missing_head_version":
            connection.execute("DELETE FROM factor_versions WHERE version = 2")
        elif damage == "missing_old_version":
            connection.execute("DELETE FROM factor_versions WHERE version = 1")
        else:
            connection.execute("DELETE FROM factor_heads")
    reopened = _store(path)
    for read in (
        lambda: _list(reopened),
        lambda: _head(reopened, "price_volume_1"),
        lambda: _version(reopened, "price_volume_1", 1),
        lambda: _version(reopened, "price_volume_1", 3),
    ):
        with pytest.raises(FactorIntegrityError):
            read()


def test_reads_reject_old_version_rewritten_with_matching_row_digest(tmp_path: Path) -> None:
    path = tmp_path / "factors.sqlite3"
    registry = _store(path)
    original = _save(registry, "first", _definition())
    _save(registry, "second", _definition(version=2), _ref(registry))
    replacement = _definition(name="改写的旧版")
    payload = json.dumps(
        replacement.model_dump(mode="json", round_trip=True),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    replacement_digest = hashlib.sha256(payload.encode()).hexdigest()
    assert replacement_digest != original.content_sha256
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE factor_versions SET definition_json = ?, content_sha256 = ? "
            "WHERE factor_id = ? AND version = 1",
            (payload, replacement_digest, replacement.factor_id),
        )
    reopened = _store(path)
    for read in (
        lambda: _version(reopened, replacement.factor_id, 1),
        lambda: _head(reopened, replacement.factor_id),
        lambda: _list(reopened),
    ):
        with pytest.raises(FactorIntegrityError):
            read()


def test_list_is_sorted_bounded_and_validates_hidden_archived_rows(tmp_path: Path) -> None:
    path = tmp_path / "factors.sqlite3"
    registry = _store(path)
    _save(registry, "z", _definition(factor_id="z_factor"))
    _save(registry, "a", _definition(factor_id="a_factor"))
    _save(registry, "m", _definition(factor_id="m_factor"))
    _archive_request(
        registry,
        ArchiveFactorRequest(
            command_id="archive-a", factor_id="a_factor", expected_head=_ref(registry, "a_factor")
        ),
    )
    assert [item.definition.factor_id for item in _list(registry, limit=1)] == ["m_factor"]
    assert [item.definition.factor_id for item in _list(registry, include_archived=True)] == [
        "a_factor",
        "m_factor",
        "z_factor",
    ]
    with pytest.raises(ValueError):
        _list(registry, limit=0)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE factor_versions SET content_sha256 = ? WHERE factor_id = 'a_factor'",
            ("0" * 64,),
        )
    with pytest.raises(FactorIntegrityError):
        _list(registry, limit=1)


def test_list_rejects_head_with_invalid_identity(tmp_path: Path) -> None:
    path = tmp_path / "factors.sqlite3"
    registry = _store(path)
    first = _save(registry, "first", _definition())
    with sqlite3.connect(path) as connection:
        connection.execute(
            "INSERT INTO factor_heads (factor_id, version, content_sha256, archived) "
            "VALUES ('Bad-ID', 1, ?, 0)",
            (first.content_sha256,),
        )
    with pytest.raises(FactorIntegrityError):
        _list(registry)


def test_invalid_command_identity_and_factor_id_are_rejected_by_request_models() -> None:
    with pytest.raises(ValidationError):
        SaveFactorDefinitionRequest(command_id="", definition=_definition(), expected_head=None)
    with pytest.raises(ValidationError):
        ArchiveFactorRequest(
            command_id="archive",
            factor_id="Bad-ID",
            expected_head=FactorHeadRef(version=1, content_sha256="0" * 64),
        )
