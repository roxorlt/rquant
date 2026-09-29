"""Bounded read-only factor definition snapshots from one verified registry."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest
from pydantic import ValidationError

from rquant.factor.definition import FactorDefinition, build_factor_definition
from rquant.factor.expression import FeatureCatalog
from rquant.factor.registry import (
    ArchiveFactorRequest,
    FactorConflictError,
    FactorDefinitionReceipt,
    FactorDefinitionRegistry,
    FactorHeadRef,
    FactorIntegrityError,
    FactorRegistryIdentity,
    FactorRegistryIdentityError,
    SaveFactorDefinitionRequest,
)
from rquant.strict_json import canonical_json_bytes

_AT = datetime(2026, 9, 29, 15, tzinfo=timezone(timedelta(hours=8)))


def _registry(path: Path) -> tuple[FactorDefinitionRegistry, FactorRegistryIdentity]:
    registry = FactorDefinitionRegistry(path)
    return registry, registry.initialize()


def _definition(factor_id: str, *, version: int = 1, name_zh: str = "价格因子") -> FactorDefinition:
    return build_factor_definition(
        factor_id=factor_id,
        name_zh=name_zh,
        category="technical",
        direction="higher_is_better",
        version=version,
        earliest_available_date=date(2024, 1, 2),
        expression="ts_mean(close, 5) / ref(volume, 2)",
        feature_catalog=FeatureCatalog(columns=("close", "volume")),
    )


def _save(
    registry: FactorDefinitionRegistry,
    identity: FactorRegistryIdentity,
    command_id: str,
    definition: FactorDefinition,
    expected_head: FactorHeadRef | None = None,
) -> FactorDefinitionReceipt:
    return registry.save(
        SaveFactorDefinitionRequest(
            command_id=command_id, definition=definition, expected_head=expected_head
        ),
        expected_identity=identity,
    )


def test_initialized_empty_registry_has_trusted_snapshot_and_explicit_time(tmp_path: Path) -> None:
    from rquant.factor.definition_serving import project_factor_definition_serving_snapshot

    path = tmp_path / "factors.sqlite3"
    registry, identity = _registry(path)
    snapshot = project_factor_definition_serving_snapshot(
        registry, expected_identity=identity, available_at=_AT
    )

    assert snapshot.registry_instance_id == identity.instance_id
    assert snapshot.available_at == _AT
    assert snapshot.state.status == "empty"
    assert snapshot.state.definition_count == 0
    assert snapshot.definitions == ()
    assert str(path) not in snapshot.model_dump_json()
    assert (
        snapshot.sha256
        == hashlib.sha256(
            canonical_json_bytes(snapshot.model_dump(mode="json", exclude={"sha256"}))
        ).hexdigest()
    )
    assert snapshot == project_factor_definition_serving_snapshot(
        FactorDefinitionRegistry(path), expected_identity=identity, available_at=_AT
    )
    later = project_factor_definition_serving_snapshot(
        registry, expected_identity=identity, available_at=_AT + timedelta(seconds=1)
    )
    assert later.sha256 != snapshot.sha256
    with pytest.raises(ValidationError):
        snapshot.state.definition_count = 1


def test_current_heads_include_archived_and_latest_version_in_order(tmp_path: Path) -> None:
    from rquant.factor.definition_serving import project_factor_definition_serving_snapshot

    registry, identity = _registry(tmp_path / "factors.sqlite3")
    _save(registry, identity, "save-z1", _definition("z_factor"))
    _save(registry, identity, "save-a1", _definition("a_factor"))
    _save(registry, identity, "save-m1", _definition("m_factor"))
    z1 = registry.get_head("z_factor", expected_identity=identity)
    assert z1 is not None
    z2_definition = _definition("z_factor", version=2, name_zh="改进价格因子")
    z2 = _save(
        registry,
        identity,
        "save-z2",
        z2_definition,
        FactorHeadRef(version=1, content_sha256=z1.content_sha256),
    )
    a1 = registry.get_head("a_factor", expected_identity=identity)
    assert a1 is not None
    registry.archive(
        ArchiveFactorRequest(
            command_id="archive-a1",
            factor_id="a_factor",
            expected_head=FactorHeadRef(version=1, content_sha256=a1.content_sha256),
        ),
        expected_identity=identity,
    )

    snapshot = project_factor_definition_serving_snapshot(
        registry, expected_identity=identity, available_at=_AT
    )
    assert snapshot.state.status == "populated"
    assert snapshot.state.definition_count == len(snapshot.definitions) == 3
    assert [row.factor_id for row in snapshot.definitions] == [
        "a_factor",
        "m_factor",
        "z_factor",
    ]
    a_row, _, z_row = snapshot.definitions
    assert a_row.archived
    assert a_row.version == 1
    assert not z_row.archived
    assert z_row.version == 2
    assert z_row.content_sha256 == z2.content_sha256
    assert z_row.name_zh == z2_definition.name_zh
    assert z_row.category == z2_definition.category
    assert z_row.direction == z2_definition.direction
    assert z_row.expression == z2_definition.expression
    assert z_row.earliest_available_date == z2_definition.earliest_available_date
    assert z_row.dependency_columns == z2_definition.dependency_columns
    assert z_row.max_history_window == z2_definition.max_history_window
    assert snapshot == project_factor_definition_serving_snapshot(
        FactorDefinitionRegistry(registry.path), expected_identity=identity, available_at=_AT
    )


def test_513th_current_factor_is_rejected_before_snapshot_without_truncation(
    tmp_path: Path,
) -> None:
    from rquant.factor.definition_serving import project_factor_definition_serving_snapshot

    registry, identity = _registry(tmp_path / "factors.sqlite3")
    for index in range(512):
        _save(registry, identity, f"save-{index}", _definition(f"factor_{index:03d}"))
    full = project_factor_definition_serving_snapshot(
        registry, expected_identity=identity, available_at=_AT
    )
    assert full.state.definition_count == len(full.definitions) == 512
    with pytest.raises(FactorConflictError, match="capacity"):
        _save(registry, identity, "save-512", _definition("factor_512"))
    still_full = project_factor_definition_serving_snapshot(
        registry, expected_identity=identity, available_at=_AT
    )
    assert still_full.state.definition_count == len(still_full.definitions) == 512


@pytest.mark.parametrize("damage", ["missing", "replaced", "instance", "old_schema"])
def test_missing_replaced_or_old_registry_never_becomes_empty_snapshot(
    tmp_path: Path, damage: str
) -> None:
    from rquant.factor.definition_serving import project_factor_definition_serving_snapshot

    path = tmp_path / "factors.sqlite3"
    registry, identity = _registry(path)
    if damage == "missing":
        path.unlink()
    elif damage == "replaced":
        alternate = tmp_path / "other.sqlite3"
        _registry(alternate)
        os.replace(alternate, path)
    elif damage == "instance":
        with sqlite3.connect(path) as connection:
            connection.execute("UPDATE factor_registry_identity SET instance_id = ?", ("f" * 32,))
    else:
        with sqlite3.connect(path) as connection:
            connection.execute("PRAGMA user_version = 2")

    error_type = FactorIntegrityError if damage == "old_schema" else FactorRegistryIdentityError
    with pytest.raises(error_type):
        project_factor_definition_serving_snapshot(
            registry, expected_identity=identity, available_at=_AT
        )
    if damage == "missing":
        assert not path.exists()


@pytest.mark.parametrize("damage", ["old_definition", "old_command_receipt"])
def test_corrupt_historical_version_or_command_receipt_rejects_snapshot(
    tmp_path: Path, damage: str
) -> None:
    from rquant.factor.definition_serving import project_factor_definition_serving_snapshot

    path = tmp_path / "factors.sqlite3"
    registry, identity = _registry(path)
    first = _save(registry, identity, "save-v1", _definition("factor_a"))
    _save(
        registry,
        identity,
        "save-v2",
        _definition("factor_a", version=2),
        FactorHeadRef(version=1, content_sha256=first.content_sha256),
    )
    malformed = "{bad"
    malformed_sha = hashlib.sha256(malformed.encode("utf-8")).hexdigest()
    with sqlite3.connect(path) as connection:
        if damage == "old_definition":
            connection.execute(
                "UPDATE factor_versions SET definition_json = ?, content_sha256 = ? "
                "WHERE factor_id = 'factor_a' AND version = 1",
                (malformed, malformed_sha),
            )
        else:
            connection.execute(
                "UPDATE factor_commands SET receipt_json = ?, receipt_sha256 = ? "
                "WHERE command_id = 'save-v1'",
                (malformed, malformed_sha),
            )
    with pytest.raises(FactorIntegrityError):
        project_factor_definition_serving_snapshot(
            registry, expected_identity=identity, available_at=_AT
        )


@pytest.mark.parametrize("reactivated", [False, True])
@pytest.mark.parametrize(
    "damage",
    [
        "receipt_json",
        "receipt_sha256",
        "receipt_version",
        "request_sha256",
        "command_action",
        "missing_command",
        "event_content_sha256",
        "event_factor_id",
    ],
)
def test_projection_rejects_corrupt_current_or_historical_archive_command(
    tmp_path: Path, damage: str, reactivated: bool
) -> None:
    from rquant.factor.definition_serving import project_factor_definition_serving_snapshot

    path = tmp_path / "factors.sqlite3"
    registry, identity = _registry(path)
    first = _save(registry, identity, "save-v1", _definition("factor_a"))
    archive_request = ArchiveFactorRequest(
        command_id="archive-v1",
        factor_id="factor_a",
        expected_head=FactorHeadRef(version=1, content_sha256=first.content_sha256),
    )
    registry.archive(archive_request, expected_identity=identity)
    if reactivated:
        # Earlier registries allowed reactivation; retain coverage for that stored history.
        definition = _definition("factor_a", version=2)
        request = SaveFactorDefinitionRequest(
            command_id="save-v2", definition=definition, expected_head=archive_request.expected_head
        )
        definition_json = canonical_json_bytes(
            definition.model_dump(mode="json", round_trip=True)
        ).decode()
        digest = hashlib.sha256(definition_json.encode()).hexdigest()
        receipt = FactorDefinitionReceipt(
            command_id="save-v2",
            action="save",
            factor_id="factor_a",
            version=2,
            content_sha256=digest,
            archived=False,
        )
        receipt_json = canonical_json_bytes(receipt.model_dump(mode="json")).decode()
        request_digest = hashlib.sha256(
            canonical_json_bytes(
                {"action": "save", "request": request.model_dump(mode="json", round_trip=True)}
            )
        ).hexdigest()
        with sqlite3.connect(path) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute(
                "INSERT INTO factor_commands VALUES (?, 'save', ?, ?, ?)",
                (
                    "save-v2",
                    request_digest,
                    receipt_json,
                    hashlib.sha256(receipt_json.encode()).hexdigest(),
                ),
            )
            connection.execute(
                "INSERT INTO factor_versions VALUES (?, ?, ?, ?, ?)",
                ("factor_a", 2, definition_json, digest, "save-v2"),
            )
            connection.execute(
                "UPDATE factor_heads SET version = 2, content_sha256 = ?, archived = 0 "
                "WHERE factor_id = 'factor_a'",
                (digest,),
            )
    baseline = project_factor_definition_serving_snapshot(
        registry, expected_identity=identity, available_at=_AT
    )
    assert baseline.state.status == "populated"
    assert baseline.definitions[0].archived is not reactivated

    with sqlite3.connect(path) as connection:
        if damage == "receipt_json":
            malformed = "{bad"
            connection.execute(
                "UPDATE factor_commands SET receipt_json = ?, receipt_sha256 = ? "
                "WHERE command_id = 'archive-v1'",
                (malformed, hashlib.sha256(malformed.encode()).hexdigest()),
            )
        elif damage == "receipt_sha256":
            connection.execute(
                "UPDATE factor_commands SET receipt_sha256 = ? WHERE command_id = 'archive-v1'",
                ("f" * 64,),
            )
        elif damage == "receipt_version":
            payload = json.loads(
                connection.execute(
                    "SELECT receipt_json FROM factor_commands WHERE command_id = 'archive-v1'"
                ).fetchone()[0]
            )
            payload["version"] = 2
            encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            connection.execute(
                "UPDATE factor_commands SET receipt_json = ?, receipt_sha256 = ? "
                "WHERE command_id = 'archive-v1'",
                (encoded, hashlib.sha256(encoded.encode()).hexdigest()),
            )
        elif damage == "request_sha256":
            connection.execute(
                "UPDATE factor_commands SET request_sha256 = ? WHERE command_id = 'archive-v1'",
                ("f" * 64,),
            )
        elif damage == "command_action":
            connection.execute(
                "UPDATE factor_commands SET action = 'save' WHERE command_id = 'archive-v1'"
            )
        elif damage == "missing_command":
            connection.execute("DELETE FROM factor_commands WHERE command_id = 'archive-v1'")
        elif damage == "event_content_sha256":
            connection.execute(
                "UPDATE factor_archive_events SET content_sha256 = ? "
                "WHERE command_id = 'archive-v1'",
                ("f" * 64,),
            )
        else:
            connection.execute(
                "UPDATE factor_archive_events SET factor_id = 'orphan_factor' "
                "WHERE command_id = 'archive-v1'"
            )

    with pytest.raises(FactorIntegrityError):
        project_factor_definition_serving_snapshot(
            registry, expected_identity=identity, available_at=_AT
        )
