"""Bounded committed template facts for the existing Serving publisher."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime
from typing import TYPE_CHECKING, Literal, Self
from uuid import UUID

from pydantic import Field, model_validator

from rquant.backtest.contracts import Sha256
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.serving_read_models import (
    ServingProjectionContract,
    ServingProjectionInput,
    ServingProjectionPayload,
)
from rquant.strategy_authoring import StrategyAuthoringStore, StrategyTemplateVersion
from rquant.strategy_authoring_commands import StrategyAuthoringIdentity, StrategyTemplateHead
from rquant.strategy_authoring_projection_contract import (
    MAX_TEMPLATE_PROJECTION_BYTES,
    MAX_TEMPLATE_VERSIONS,
    STRATEGY_TEMPLATE_PROJECTION_LAYOUTS,
    STRATEGY_TEMPLATE_PROJECTION_TABLES,
)
from rquant.strategy_authoring_source import (
    StrategySourceCatalog,
    TemplatePoolReference,
    TemplateSignalReference,
)
from rquant.strict_json import canonical_json_bytes, strict_model_validate_json

if TYPE_CHECKING:
    from rquant.strategy_template_artifact import StrategyTemplateSealedResultReader


class StrategyTemplateRecentRun(RuntimeContractModel):
    job_id: UUID
    owner_id: str
    strategy_id: str
    head: StrategyTemplateHead
    input_hash: Sha256
    result_hash: Sha256
    spec_hash: Sha256
    manifest_hash: Sha256
    complete_result_hash: Sha256
    completed_at: AwareUtcDatetime


class StrategyTemplatePublishedVersion(RuntimeContractModel):
    metadata: StrategyTemplateVersion
    is_head: bool = Field(strict=True)
    archived: bool = Field(strict=True)
    latest_run: StrategyTemplateRecentRun | None = None

    @model_validator(mode="after")
    def validate_exact_result(self) -> Self:
        result = self.latest_run
        if result is not None and (result.owner_id, result.strategy_id, result.head) != (
            self.metadata.owner_id,
            self.metadata.strategy_id,
            self.metadata.head,
        ):
            raise ValueError("recent result differs from exact owner and definition version")
        return self


class StrategyTemplateOwnerSources(RuntimeContractModel):
    owner_id: str
    pools: tuple[TemplatePoolReference, ...]
    signals: tuple[TemplateSignalReference, ...]


class StrategyAuthoringSnapshot(RuntimeContractModel):
    identity: StrategyAuthoringIdentity
    available_at: AwareUtcDatetime
    versions: tuple[StrategyTemplatePublishedVersion, ...] = Field(max_length=MAX_TEMPLATE_VERSIONS)
    sources: tuple[StrategyTemplateOwnerSources, ...] = Field(max_length=16)
    sha256: Sha256 | None = None

    @model_validator(mode="after")
    def validate_committed_graph(self) -> Self:
        keys = tuple((row.metadata.strategy_id, row.metadata.head.version) for row in self.versions)
        if keys != tuple(sorted(set(keys))):
            raise ValueError("strategy projection versions must be sorted and unique")
        groups: dict[str, list[StrategyTemplatePublishedVersion]] = {}
        for row in self.versions:
            if row.metadata.saved_at > self.available_at or (
                row.latest_run is not None and row.latest_run.completed_at > self.available_at
            ):
                raise ValueError("strategy projection contains future committed facts")
            groups.setdefault(row.metadata.strategy_id, []).append(row)
        if len(groups) > 500:
            raise ValueError("strategy projection exceeds template count budget")
        for rows in groups.values():
            if tuple(row.metadata.head.version for row in rows) != tuple(range(1, len(rows) + 1)):
                raise ValueError("strategy projection lacks complete version history")
            if [row.is_head for row in rows] != [False] * (len(rows) - 1) + [True]:
                raise ValueError("strategy projection current head is not exact")
            if (
                len({row.metadata.owner_id for row in rows}) != 1
                or len({row.archived for row in rows}) != 1
            ):
                raise ValueError("strategy projection mixes owner or archive facts")
            for previous, current in zip(rows, rows[1:]):
                if current.metadata.parent_head != previous.metadata.head:
                    raise ValueError("strategy projection parent version differs")
        owners = tuple(row.owner_id for row in self.sources)
        if owners != tuple(sorted(set(owners))):
            raise ValueError("strategy source owners must be sorted and unique")
        for source in self.sources:
            StrategySourceCatalog(
                owner_id=source.owner_id,
                generation_id="snapshot",
                pools=source.pools,
                signals=source.signals,
            )
            if any(
                item.owner_id not in (None, source.owner_id)
                for item in (*source.pools, *source.signals)
            ):
                raise ValueError("strategy source catalog exposes another owner")
        if len(self.model_dump_json().encode()) > MAX_TEMPLATE_PROJECTION_BYTES:
            raise ValueError("strategy projection exceeds byte budget")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"sha256"}))
        if self.sha256 is None:
            object.__setattr__(self, "sha256", expected)
        elif self.sha256 != expected:
            raise ValueError("strategy projection snapshot hash differs")
        return self

    def for_owner(self, owner_id: str) -> tuple[StrategyTemplatePublishedVersion, ...]:
        return tuple(row for row in self.versions if row.metadata.owner_id == owner_id)

    def sources_for(self, owner_id: str, *, generation_id: str) -> StrategySourceCatalog:
        source = next((item for item in self.sources if item.owner_id == owner_id), None)
        return StrategySourceCatalog(
            owner_id=owner_id,
            generation_id=generation_id,
            pools=() if source is None else source.pools,
            signals=() if source is None else source.signals,
        )


class StrategyDefinitionStateRow(RuntimeContractModel):
    status_key: Literal["current"] = "current"
    identity_json: str
    snapshot_sha256: Sha256
    available_at: AwareUtcDatetime


class StrategyDefinitionRow(RuntimeContractModel):
    strategy_id: str
    version: int = Field(strict=True, ge=1)
    owner_id: str
    definition_json: str = Field(max_length=64 * 1024)


class StrategyTemplateSourceRow(RuntimeContractModel):
    owner_id: str
    sources_json: str = Field(max_length=64 * 1024)


class StrategyAuthoringProjectionTables(RuntimeContractModel):
    state: StrategyDefinitionStateRow
    definitions: tuple[StrategyDefinitionRow, ...]
    sources: tuple[StrategyTemplateSourceRow, ...]

    def restore(self) -> StrategyAuthoringSnapshot:
        versions = tuple(
            strict_model_validate_json(StrategyTemplatePublishedVersion, row.definition_json)
            for row in self.definitions
        )
        for row, parsed in zip(self.definitions, versions, strict=True):
            if (row.strategy_id, row.version, row.owner_id) != (
                parsed.metadata.strategy_id,
                parsed.metadata.head.version,
                parsed.metadata.owner_id,
            ):
                raise ValueError("strategy projection owner/version key differs from body")
        sources = tuple(
            strict_model_validate_json(StrategyTemplateOwnerSources, row.sources_json)
            for row in self.sources
        )
        if any(
            row.owner_id != parsed.owner_id
            for row, parsed in zip(self.sources, sources, strict=True)
        ):
            raise ValueError("strategy projection source owner differs")
        return StrategyAuthoringSnapshot(
            identity=strict_model_validate_json(
                StrategyAuthoringIdentity, self.state.identity_json
            ),
            available_at=self.state.available_at,
            versions=versions,
            sources=sources,
            sha256=self.state.snapshot_sha256,
        )

    def serving_payloads(self) -> tuple[ServingProjectionPayload, ...]:
        """The root must register the three exact contracts before translation."""
        self.restore()
        return tuple(
            ServingProjectionPayload(
                table_name=name,
                available_at=self.state.available_at,
                rows=tuple(row.model_dump(mode="json") for row in rows),
            )
            for name, rows in (
                ("strategy_definition_state", (self.state,)),
                ("strategy_definition", self.definitions),
                ("strategy_template_source", self.sources),
            )
        )


STRATEGY_TEMPLATE_PROJECTION_CONTRACTS: Mapping[str, ServingProjectionContract] = {
    name: ServingProjectionContract(
        owner_dataset_id="lab_jobs",
        columns=columns,
        sort_keys=keys,
        max_rows=max_rows,
        max_bytes=max_bytes,
        event_time_columns=times,
    )
    for name, (
        columns,
        keys,
        max_rows,
        max_bytes,
        times,
    ) in STRATEGY_TEMPLATE_PROJECTION_LAYOUTS.items()
}


def project_strategy_authoring(
    snapshot: StrategyAuthoringSnapshot,
) -> StrategyAuthoringProjectionTables:
    snapshot = StrategyAuthoringSnapshot.model_validate(snapshot.model_dump(mode="python"))
    return StrategyAuthoringProjectionTables(
        state=StrategyDefinitionStateRow(
            identity_json=canonical_json_bytes(snapshot.identity.model_dump(mode="json")).decode(),
            snapshot_sha256=snapshot.sha256,
            available_at=snapshot.available_at,
        ),
        definitions=tuple(
            StrategyDefinitionRow(
                strategy_id=row.metadata.strategy_id,
                version=row.metadata.head.version,
                owner_id=row.metadata.owner_id,
                definition_json=canonical_json_bytes(row.model_dump(mode="json")).decode(),
            )
            for row in snapshot.versions
        ),
        sources=tuple(
            StrategyTemplateSourceRow(
                owner_id=row.owner_id,
                sources_json=canonical_json_bytes(row.model_dump(mode="json")).decode(),
            )
            for row in snapshot.sources
        ),
    )


def build_strategy_authoring_snapshot(
    store: StrategyAuthoringStore,
    *,
    available_at: datetime,
    source_catalogs: tuple[StrategySourceCatalog, ...],
    sealed_result_reader: StrategyTemplateSealedResultReader | None = None,
) -> StrategyAuthoringSnapshot:
    from rquant.strategy_template_artifact import StrategyTemplateSealedResultReader

    if (
        sealed_result_reader is not None
        and type(sealed_result_reader) is not StrategyTemplateSealedResultReader
    ):
        raise TypeError("strategy recent runs require the concrete original sealed result reader")
    identity = store.identity()
    recent_runs = (
        ()
        if sealed_result_reader is None
        else sealed_result_reader.recent_runs(store, expected_identity=identity, as_of=available_at)
    )
    recent: dict[tuple[str, int], StrategyTemplateRecentRun] = {}
    for run in recent_runs:
        key = (run.strategy_id, run.head.version)
        if key not in recent or (run.completed_at, str(run.job_id)) > (
            recent[key].completed_at,
            str(recent[key].job_id),
        ):
            recent[key] = run
    versions: list[StrategyTemplatePublishedVersion] = []
    with store._connection(expected_identity=identity) as connection:
        heads = connection.execute("SELECT * FROM heads ORDER BY strategy_id LIMIT 501").fetchall()
        if len(heads) > 500:
            raise ValueError("strategy projection exceeds template budget")
        for row in heads:
            head = StrategyTemplateHead.model_validate_json(row["head"])
            committed = connection.execute(
                "SELECT version FROM versions WHERE strategy_id=? ORDER BY version LIMIT 4097",
                (row["strategy_id"],),
            ).fetchall()
            if len(committed) + len(versions) > MAX_TEMPLATE_VERSIONS:
                raise ValueError("strategy projection exceeds confirmed version budget")
            for version in committed:
                metadata = store._version(
                    connection, row["strategy_id"], version[0], row["owner_id"]
                )
                versions.append(
                    StrategyTemplatePublishedVersion(
                        metadata=metadata,
                        is_head=metadata.head == head,
                        archived=bool(row["archived"]),
                        latest_run=recent.get((metadata.strategy_id, metadata.head.version)),
                    )
                )
    return StrategyAuthoringSnapshot(
        identity=identity,
        available_at=available_at,
        versions=tuple(versions),
        sources=tuple(
            sorted(
                (
                    StrategyTemplateOwnerSources(
                        owner_id=source.owner_id, pools=source.pools, signals=source.signals
                    )
                    for source in source_catalogs
                ),
                key=lambda item: item.owner_id,
            )
        ),
    )


def validate_strategy_authoring_projections(
    projections: Mapping[str, ServingProjectionPayload],
) -> StrategyAuthoringSnapshot | None:
    present = projections.keys() & STRATEGY_TEMPLATE_PROJECTION_TABLES
    if not present:
        return None
    if present != STRATEGY_TEMPLATE_PROJECTION_TABLES:
        raise ValueError("strategy projections are incomplete")
    selected = tuple(projections[name] for name in sorted(present))
    if len({p.available_at for p in selected}) != 1:
        raise ValueError("strategy projections mix availability")
    bound = tuple(p for p in selected if isinstance(p, ServingProjectionInput))
    if bound and (
        len(bound) != 3
        or {p.owner_dataset_id for p in bound} != {"lab_jobs"}
        or len({p.owner_generation_id for p in bound}) != 1
    ):
        raise ValueError("strategy projections mix source generation")
    state = projections["strategy_definition_state"]
    if len(state.rows) != 1:
        raise ValueError("strategy definition state must contain one row")
    tables = StrategyAuthoringProjectionTables(
        state=StrategyDefinitionStateRow.model_validate(dict(state.rows[0])),
        definitions=tuple(
            StrategyDefinitionRow.model_validate(dict(row))
            for row in projections["strategy_definition"].rows
        ),
        sources=tuple(
            StrategyTemplateSourceRow.model_validate(dict(row))
            for row in projections["strategy_template_source"].rows
        ),
    )
    if tables.state.available_at != state.available_at:
        raise ValueError("strategy state availability differs")
    return tables.restore()


class StrategyAuthoringProjectionSource:
    def __init__(
        self,
        store: StrategyAuthoringStore,
        *,
        expected_identity: StrategyAuthoringIdentity,
        source_catalog_provider: Callable[[datetime], tuple[StrategySourceCatalog, ...]],
        sealed_result_reader: StrategyTemplateSealedResultReader | None = None,
    ) -> None:
        from rquant.strategy_template_artifact import StrategyTemplateSealedResultReader

        if type(store) is not StrategyAuthoringStore:
            raise TypeError("strategy projection requires its concrete metadata store")
        self.store = store
        self.expected_identity = expected_identity
        self.source_catalog_provider = source_catalog_provider
        if (
            sealed_result_reader is not None
            and type(sealed_result_reader) is not StrategyTemplateSealedResultReader
        ):
            raise TypeError(
                "strategy projection requires its concrete original sealed result reader"
            )
        self.sealed_result_reader = sealed_result_reader

    def __call__(self, observed: datetime) -> tuple[ServingProjectionPayload, ...]:
        if self.store.identity() != self.expected_identity:
            raise RuntimeError("strategy original metadata identity changed")
        snapshot = build_strategy_authoring_snapshot(
            self.store,
            available_at=observed,
            source_catalogs=self.source_catalog_provider(observed),
            sealed_result_reader=self.sealed_result_reader,
        )
        if (
            snapshot.identity != self.expected_identity
            or self.store.identity() != self.expected_identity
        ):
            raise RuntimeError("strategy original metadata identity changed during projection")
        return project_strategy_authoring(snapshot).serving_payloads()
