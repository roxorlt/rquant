"""The verified factor registry is published as one bounded Serving pair."""

from __future__ import annotations

import os
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from rquant.factor.definition import build_factor_definition
from rquant.factor.definition_serving import project_factor_definition_serving_snapshot
from rquant.factor.expression import FeatureCatalog
from rquant.factor.registry import (
    ArchiveFactorRequest,
    FactorDefinitionRegistry,
    FactorHeadRef,
    SaveFactorDefinitionRequest,
)
from rquant.factor.serving_projection import (
    FACTOR_DEFINITION_PROJECTION_TABLES,
    project_factor_definition_projections,
    validate_factor_definition_projections,
)
from rquant.lab_jobs import LabJobReader, LabJobStore
from rquant.lab_jobs_serving_authority import (
    LabJobsServingAuthorityIntegrityError,
    LabJobsServingAuthorityPublisher,
    LabJobsServingSourceReader,
    lab_jobs_state_identity,
)
from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_serving_authority import ServingSourceAuthorityPublisher
from rquant.runtime_serving_snapshot import LAB_JOBS_DATASET_ID, LabJobsPayload
from rquant.serving_page_projection_source import (
    DuckDBLabPageProjectionSource,
    LabPageProjectionSnapshot,
    PageProjectionSourceIntegrityError,
)
from rquant.serving_read_models import (
    PAGE_PROJECTION_CONTRACTS,
    SERVING_TABLE_SPECS,
    ServingProjectionInput,
    ServingProjectionPayload,
    ServingReadModelInput,
    build_serving_read_models,
)
from rquant.storage.duckdb import DuckDBStore

AT = datetime(2026, 9, 29, 7, 0, tzinfo=UTC)
GEN = "a" * 64


def _registry(path: Path) -> FactorDefinitionRegistry:
    registry = FactorDefinitionRegistry(path)
    registry.initialize()
    return registry


def _save(registry: FactorDefinitionRegistry, factor_id: str, *, version: int = 1) -> None:
    identity = registry.identity()
    head = registry.get_head(factor_id, expected_identity=identity)
    definition = build_factor_definition(
        factor_id=factor_id,
        name_zh="价格因子",
        category="technical",
        direction="higher_is_better",
        version=version,
        earliest_available_date=date(2024, 1, 2),
        expression="ts_mean(close, 5) / ref(volume, 2)",
        feature_catalog=FeatureCatalog(columns=("close", "volume")),
    )
    registry.save(
        SaveFactorDefinitionRequest(
            command_id=f"save-{factor_id}-{version}",
            definition=definition,
            expected_head=(
                None
                if head is None
                else FactorHeadRef(
                    version=head.head.version,
                    content_sha256=head.head.content_sha256,
                )
            ),
        ),
        expected_identity=identity,
    )


def _pair(registry: FactorDefinitionRegistry) -> tuple[ServingProjectionPayload, ...]:
    snapshot = project_factor_definition_serving_snapshot(
        registry, expected_identity=registry.identity(), available_at=AT
    )
    return project_factor_definition_projections(snapshot)


def _bound(pair: tuple[ServingProjectionPayload, ...]) -> tuple[ServingProjectionInput, ...]:
    return tuple(
        ServingProjectionInput.bind(item, owner_dataset_id="lab_jobs", owner_generation_id=GEN)
        for item in pair
    )


def _replace_rows(
    projection: ServingProjectionPayload, rows: tuple[dict[str, object], ...]
) -> ServingProjectionPayload:
    return ServingProjectionPayload(
        table_name=projection.table_name,
        available_at=projection.available_at,
        rows=rows,
    )


def test_empty_and_archived_multiversion_heads_round_trip_to_physical_tables(
    tmp_path: Path,
) -> None:
    registry = _registry(tmp_path / "factors.sqlite3")
    empty_pair = _pair(registry)
    assert {item.table_name for item in empty_pair} == FACTOR_DEFINITION_PROJECTION_TABLES
    assert (
        validate_factor_definition_projections(
            {item.table_name: item for item in empty_pair}
        ).state.status
        == "empty"
    )
    assert empty_pair[1].rows == ()

    _save(registry, "z_factor")
    _save(registry, "a_factor")
    _save(registry, "z_factor", version=2)
    a_head = registry.get_head("a_factor", expected_identity=registry.identity())
    assert a_head is not None
    registry.archive(
        ArchiveFactorRequest(
            command_id="archive-a-factor",
            factor_id="a_factor",
            expected_head=FactorHeadRef(
                version=a_head.head.version,
                content_sha256=a_head.head.content_sha256,
            ),
        ),
        expected_identity=registry.identity(),
    )
    pair = _pair(registry)
    restored = validate_factor_definition_projections({item.table_name: item for item in pair})
    assert restored.state.status == "populated"
    assert restored.state.definition_count == 2
    assert [(row.factor_id, row.version, row.archived) for row in restored.definitions] == [
        ("a_factor", 1, True),
        ("z_factor", 2, False),
    ]
    assert pair[0].rows[0]["snapshot_sha256"] == restored.sha256
    assert pair[1].rows[0]["dependency_columns_json"] == '["close","volume"]'
    assert all(
        PAGE_PROJECTION_CONTRACTS[name].owner_dataset_id == "lab_jobs"
        for name in FACTOR_DEFINITION_PROJECTION_TABLES
    )
    assert SERVING_TABLE_SPECS.keys() >= FACTOR_DEFINITION_PROJECTION_TABLES

    page = LabPageProjectionSnapshot.create(available_at=AT, factor_definition_projections=pair)
    bound = _bound(
        tuple(
            item
            for item in page.projections
            if item.table_name in FACTOR_DEFINITION_PROJECTION_TABLES
        )
    )
    serving = ServingReadModelInput(observed_at=AT, projections=bound)
    tables = build_serving_read_models(serving)
    assert len(tables["factor_definition_state"]) == 1
    assert len(tables["factor_definition"]) == 2
    status = tables["projection_status"].set_index("table_name")
    for name in FACTOR_DEFINITION_PROJECTION_TABLES:
        assert status.loc[name, "available"]
        assert status.loc[name, "owner_generation_id"] == GEN


def test_pair_rejects_missing_rows_digest_order_archive_and_dependencies(tmp_path: Path) -> None:
    registry = _registry(tmp_path / "factors.sqlite3")
    _save(registry, "a_factor")
    _save(registry, "z_factor")
    state, definitions = _pair(registry)
    rows = tuple(dict(row) for row in definitions.rows)

    mutations = (
        (state, _replace_rows(definitions, rows[:1])),
        (
            _replace_rows(state, ({**dict(state.rows[0]), "snapshot_sha256": "0" * 64},)),
            definitions,
        ),
        (state, _replace_rows(definitions, tuple(reversed(rows)))),
        (state, _replace_rows(definitions, ({**rows[0], "archived": True}, rows[1]))),
        (
            state,
            _replace_rows(
                definitions,
                ({**rows[0], "dependency_columns_json": '["volume","close"]'}, rows[1]),
            ),
        ),
        (
            state,
            _replace_rows(
                definitions,
                ({**rows[0], "dependency_columns_json": '[ "close", "volume" ]'}, rows[1]),
            ),
        ),
    )
    for first, second in mutations:
        with pytest.raises(ValueError):
            validate_factor_definition_projections(
                {first.table_name: first, second.table_name: second}
            )
    with pytest.raises(ValueError, match="incomplete"):
        validate_factor_definition_projections({state.table_name: state})


def test_lab_and_public_serving_input_reject_partial_or_mixed_pair(tmp_path: Path) -> None:
    registry = _registry(tmp_path / "factors.sqlite3")
    _save(registry, "a_factor")
    state, definitions = _pair(registry)
    for group in ((state,), (state, state), (state, definitions, definitions)):
        with pytest.raises(ValueError):
            LabPageProjectionSnapshot.create(available_at=AT, factor_definition_projections=group)

    bound_state, bound_definition = _bound((state, definitions))
    with pytest.raises(ValueError, match="incomplete"):
        LabJobsPayload(projections=(state,))
    with pytest.raises(ValueError, match="duplicate"):
        LabJobsPayload(projections=(state, definitions, definitions))
    with pytest.raises(ValueError, match="incomplete"):
        ServingReadModelInput(observed_at=AT, projections=(bound_state,))
    with pytest.raises(ValueError, match="generation"):
        ServingReadModelInput(
            observed_at=AT,
            projections=(
                bound_state,
                ServingProjectionInput.bind(
                    definitions, owner_dataset_id="lab_jobs", owner_generation_id="b" * 64
                ),
            ),
        )
    later = ServingProjectionPayload(
        table_name=definitions.table_name,
        available_at=AT + timedelta(seconds=1),
        rows=definitions.rows,
    )
    with pytest.raises(ValueError, match="availability"):
        ServingReadModelInput(
            observed_at=AT + timedelta(seconds=1),
            projections=(bound_state, _bound((later,))[0]),
        )
    bad = _replace_rows(definitions, ({**dict(definitions.rows[0]), "archived": True},))
    with pytest.raises(ValueError):
        ServingReadModelInput(observed_at=AT, projections=(bound_state, _bound((bad,))[0]))


def test_lab_source_optional_pair_and_fixed_identity_fail_closed(tmp_path: Path) -> None:
    database = tmp_path / "research_ro.duckdb"
    with DuckDBStore(database):
        pass
    registry_path = tmp_path / "factors.sqlite3"
    registry = _registry(registry_path)
    identity = registry.identity()
    unconfigured = DuckDBLabPageProjectionSource(database)(AT)
    assert not FACTOR_DEFINITION_PROJECTION_TABLES & {
        item.table_name for item in unconfigured.projections
    }
    unpublished = build_serving_read_models(ServingReadModelInput(observed_at=AT))[
        "projection_status"
    ].set_index("table_name")
    for name in FACTOR_DEFINITION_PROJECTION_TABLES:
        assert not unpublished.loc[name, "available"]
        assert unpublished.loc[name, "reason"] == "projection_not_published"
    with pytest.raises(ValueError, match="paired"):
        DuckDBLabPageProjectionSource(database, factor_registry=registry)
    source = DuckDBLabPageProjectionSource(
        database, factor_registry=registry, factor_registry_identity=identity
    )
    empty = source(AT)
    assert (
        validate_factor_definition_projections(
            {item.table_name: item for item in empty.projections}
        ).state.status
        == "empty"
    )
    _save(registry, "a_factor")
    assert (
        validate_factor_definition_projections(
            {item.table_name: item for item in source(AT).projections}
        ).state.status
        == "populated"
    )

    held = tmp_path / "held.sqlite3"
    os.replace(registry_path, held)
    with pytest.raises(PageProjectionSourceIntegrityError):
        source(AT)
    replacement = _registry(registry_path)
    with pytest.raises(PageProjectionSourceIntegrityError):
        source(AT)
    replacement.path.unlink()
    os.replace(held, registry_path)
    with sqlite3.connect(registry_path) as connection:
        connection.execute(
            "UPDATE factor_commands SET receipt_json = '{}' WHERE command_id = ?",
            ("save-a_factor-1",),
        )
    with pytest.raises(PageProjectionSourceIntegrityError):
        source(AT)


def test_source_rejects_old_schema_and_513th_head_without_truncation(tmp_path: Path) -> None:
    database = tmp_path / "research_ro.duckdb"
    with DuckDBStore(database):
        pass
    registry_path = tmp_path / "factors.sqlite3"
    registry = _registry(registry_path)
    source = DuckDBLabPageProjectionSource(
        database, factor_registry=registry, factor_registry_identity=registry.identity()
    )
    with sqlite3.connect(registry_path) as connection:
        connection.execute("DROP TABLE factor_archive_events")
    with pytest.raises(PageProjectionSourceIntegrityError):
        source(AT)

    registry_path.unlink()
    registry = _registry(registry_path)
    source = DuckDBLabPageProjectionSource(
        database, factor_registry=registry, factor_registry_identity=registry.identity()
    )
    for index in range(513):
        _save(registry, f"factor_{index:03d}")
    with pytest.raises(PageProjectionSourceIntegrityError, match="512"):
        source(AT)


def test_physical_definition_byte_budget_rejects_whole_payload(tmp_path: Path) -> None:
    registry = _registry(tmp_path / "factors.sqlite3")
    _save(registry, "a_factor")
    definition = dict(_pair(registry)[1].rows[0])
    oversized = tuple(
        {**definition, "factor_id": f"factor_{index:03d}", "expression": "x" * 8192}
        for index in range(260)
    )
    with pytest.raises(ValueError, match="byte budget"):
        ServingProjectionPayload(table_name="factor_definition", available_at=AT, rows=oversized)


def test_lab_authority_rejects_registry_change_between_two_reads(tmp_path: Path) -> None:
    database = tmp_path / "research_ro.duckdb"
    with DuckDBStore(database):
        pass
    registry = _registry(tmp_path / "factors.sqlite3")
    source = DuckDBLabPageProjectionSource(
        database, factor_registry=registry, factor_registry_identity=registry.identity()
    )
    jobs = LabJobStore(tmp_path / "jobs.sqlite3")
    jobs.initialize()
    calls = 0

    def read_page(observed_at: datetime) -> tuple[ServingProjectionPayload, ...]:
        nonlocal calls
        calls += 1
        projections = source(observed_at).projections
        if calls == 1:
            _save(registry, "a_factor")
        return projections

    authority = LabJobsServingSourceReader(
        reader=LabJobReader(jobs.path), page_projection_reader=read_page
    )
    with pytest.raises(
        LabJobsServingAuthorityIntegrityError, match="page projection authority changed"
    ):
        authority(AT)
    assert calls == 2


def _publishing_authority(
    tmp_path: Path, registry: FactorDefinitionRegistry
) -> tuple[LabJobsServingAuthorityPublisher, list[datetime]]:
    database = tmp_path / "research_ro.duckdb"
    with DuckDBStore(database):
        pass
    source = DuckDBLabPageProjectionSource(
        database, factor_registry=registry, factor_registry_identity=registry.identity()
    )
    jobs = LabJobStore(tmp_path / "jobs.sqlite3")
    jobs.initialize()
    published_at = [AT + timedelta(seconds=5)]
    authority = LabJobsServingAuthorityPublisher(
        reader=LabJobsServingSourceReader(
            reader=LabJobReader(jobs.path),
            page_projection_reader=lambda observed: source(observed).projections,
        ),
        publisher=ServingSourceAuthorityPublisher(
            root=tmp_path / "authority",
            producer_commit="a" * 40,
            dataset_id=LAB_JOBS_DATASET_ID,
            payload_kind="lab_jobs",
            clock=lambda: published_at[0],
        ),
    )
    return authority, published_at


def test_publish_if_changed_ignores_observation_only_and_publishes_real_changes(
    tmp_path: Path,
) -> None:
    registry_path = tmp_path / "factors.sqlite3"
    registry = _registry(registry_path)
    authority, published_at = _publishing_authority(tmp_path, registry)

    first = authority.publish(AT)
    published_at[0] = AT + timedelta(seconds=6)
    unchanged = authority.publish(AT + timedelta(seconds=1))
    assert first.written
    assert not unchanged.written
    assert unchanged.pointer == first.pointer

    _save(registry, "a_factor")
    published_at[0] = AT + timedelta(seconds=7)
    saved = authority.publish(AT + timedelta(seconds=2))
    assert saved.written
    assert saved.pointer.generation_id != first.pointer.generation_id

    head = registry.get_head("a_factor", expected_identity=registry.identity())
    assert head is not None
    registry.archive(
        ArchiveFactorRequest(
            command_id="archive-a_factor",
            factor_id="a_factor",
            expected_head=FactorHeadRef(
                version=head.head.version,
                content_sha256=head.head.content_sha256,
            ),
        ),
        expected_identity=registry.identity(),
    )
    published_at[0] = AT + timedelta(seconds=8)
    archived = authority.publish(AT + timedelta(seconds=3))
    assert archived.written
    assert archived.pointer.generation_id != saved.pointer.generation_id

    held = tmp_path / "held.sqlite3"
    os.replace(registry_path, held)
    _registry(registry_path)
    published_at[0] = AT + timedelta(seconds=9)
    with pytest.raises(PageProjectionSourceIntegrityError):
        authority.publish(AT + timedelta(seconds=4))
    assert authority.publisher.root.exists()
    registry_path.unlink()
    os.replace(held, registry_path)
    with sqlite3.connect(registry_path) as connection:
        connection.execute(
            "UPDATE factor_commands SET receipt_json = '{}' WHERE command_id = ?",
            ("save-a_factor-1",),
        )
    with pytest.raises(PageProjectionSourceIntegrityError):
        authority.publish(AT + timedelta(seconds=4))


def test_publish_shortcut_rejects_self_consistent_outer_generation_with_bad_digest(
    tmp_path: Path,
) -> None:
    registry = _registry(tmp_path / "factors.sqlite3")
    _save(registry, "a_factor")
    authority, published_at = _publishing_authority(tmp_path, registry)
    assert authority.publish(AT).written

    observed = AT + timedelta(seconds=1)
    published_at[0] = observed + timedelta(seconds=5)
    fresh = authority.reader(observed)
    values = fresh.model_dump(mode="python", exclude={"generation_id"})
    payload = values["payload"]
    state = next(
        item for item in payload["projections"] if item["table_name"] == "factor_definition_state"
    )
    state["rows"][0]["snapshot_sha256"] = "0" * 64
    values["generation_id"] = canonical_sha256(values)
    assert values["generation_id"] != fresh.generation_id
    forged = fresh.model_copy(update={"payload": payload, "generation_id": values["generation_id"]})

    with pytest.raises(ValueError, match="digest|snapshot"):
        authority.publisher.publish_if_changed(forged, unchanged_identity=lab_jobs_state_identity)
