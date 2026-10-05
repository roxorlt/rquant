"""Private facts from the original registry and original sealed Lab authority."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Literal
from uuid import UUID

from pydantic import Field

from rquant.experiment_platform import (
    ExperimentChildAdmission,
    ExperimentFamilyRecord,
    ExperimentNote,
    ExperimentOuterGrant,
    ExperimentPreparationReceipt,
    ExperimentSearchRequest,
    HoldoutPolicy,
    Owner,
    SearchDimension,
    Sha256,
)
from rquant.experiment_platform_template_models import (
    ExperimentTemplateSelection,
    ExperimentTemplateSlot,
)
from rquant.experiment_registry import (
    ExperimentAttempt,
    ExperimentRegistryReadonlyReader,
    _private_platform_schema,
)
from rquant.lab_jobs import (
    LabJobListFilters,
    LabJobPage,
    LabJobReader,
    LabJobRecord,
    LabPublishedJobsEventSnapshot,
)
from rquant.portfolio_backtest_models import PortfolioBacktestConfig
from rquant.portfolio_backtest_source import PortfolioExperimentProtocol
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.serving_read_models import ServingProjectionPayload, _projection_json_bytes
from rquant.strategy_template import StrategyTemplate

if TYPE_CHECKING:
    from rquant.experiment_platform_evidence import ExperimentOverfitEvidence

PRIVATE_TABLES = (
    "experiment_private_attempt",
    "experiment_private_family",
    "experiment_private_window",
)
_LEGACY_JOB_CLAUSE = (
    "NOT (COALESCE(json_extract(j.spec_json,'$.experiment.spec.hypothesis_family'),'') "
    "LIKE 'experiment-search:%' OR COALESCE(json_extract(j.spec_json,'$.experiment.spec.h"
    "ypothesis_family'),'') LIKE "
    "'experiment-outer:%')"
)


def legacy_job_page(
    reader: LabJobReader,
    *,
    filters: LabJobListFilters | None = None,
    limit: int = 50,
    cursor: str | None = None,
) -> LabJobPage:
    if type(limit) is not int or not 1 <= limit <= 200:
        raise ValueError("public Lab page limit must be from 1 through 200")
    selected = LabJobListFilters.model_validate(filters or LabJobListFilters())
    clauses, parameters = reader._job_filters_sql(selected)
    clauses.append(_LEGACY_JOB_CLAUSE)
    if cursor is not None:
        from rquant.lab_jobs import _dump_time

        boundary = reader._decode_job_list_cursor(cursor, filters=selected)
        at = _dump_time(boundary.created_at)
        clauses.append("(j.created_at < ? OR (j.created_at = ? AND j.job_id < ?))")
        parameters.extend((at, at, str(boundary.job_id)))
    with reader._read_snapshot(label="public Lab job page") as connection:
        return reader._list_jobs_in_snapshot(
            connection,
            limit=limit,
            clauses=clauses,
            parameters=parameters,
            include_total=False,
            filters=selected,
        )


def legacy_job_snapshot(reader: LabJobReader, *, limit: int) -> LabPublishedJobsEventSnapshot:
    with reader._read_snapshot(label="public Lab jobs and events") as connection:
        page = reader._list_jobs_in_snapshot(
            connection,
            limit=limit,
            clauses=[_LEGACY_JOB_CLAUSE],
            parameters=[],
            include_total=False,
            filters=LabJobListFilters(),
        )
        total = connection.execute(
            "SELECT count(*) FROM lab_job j WHERE " + _LEGACY_JOB_CLAUSE
        ).fetchone()[0]
        page = page.model_copy(update={"total_count": total})
        per_job = min(500, 4096 // len(page.items)) if page.items else 500
        if per_job < 1:
            raise ValueError("public Lab event budget cannot cover every job")
        windows = tuple(
            reader._published_event_window(connection, summary=summary, limit=per_job)
            for summary in page.items
        )
        return LabPublishedJobsEventSnapshot(page=page, windows=windows)


class ExperimentAttemptFact(RuntimeContractModel):
    owner: Owner
    family_id: str
    index: int = Field(ge=0, le=63)
    configuration: PortfolioBacktestConfig
    attempt: ExperimentAttempt
    child: ExperimentChildAdmission
    input_hash: Sha256
    source_identity: Sha256
    spec_hash: Sha256
    manifest_hash: Sha256 | None = None
    result_hash: Sha256 | None = None


class ExperimentSearchContext(RuntimeContractModel):
    template: ExperimentTemplateSelection | None = None
    request_fingerprint: Sha256
    protocol: PortfolioExperimentProtocol
    dimensions: tuple[SearchDimension, ...]
    method: Literal["grid", "random"]
    random_count: int = Field(strict=True, ge=1, le=64)
    seed: int = Field(strict=True, ge=0, le=2**32 - 1)
    confidence: Decimal = Field(gt=Decimal(".5"), lt=1, allow_inf_nan=False)
    target_period_sharpe: Decimal = Field(allow_inf_nan=False)
    pbo_slices: Literal[4, 6, 8, 10]

    @classmethod
    def from_request(cls, request: ExperimentSearchRequest) -> ExperimentSearchContext:
        # Complete immutable configurations stay in every attempt and in the
        # original request ledger; repeating a base configuration in each family
        # would consume the owner window budget without adding a result binding.
        return cls(
            request_fingerprint=canonical_sha256(request),
            **request.model_dump(mode="python", exclude={"name", "base_config"}),
        )


class ExperimentPlannedSlotFact(RuntimeContractModel):
    index: int = Field(ge=0, le=63)
    configuration: PortfolioBacktestConfig
    definition_state: Literal["pending", "saved", "failed", "cancelled"]
    input_prepared: bool
    failure: Literal["capacity", "source_changed", "invalid_definition"] | None = None


class ExperimentFamilyFact(RuntimeContractModel):
    owner: Owner
    family_id: str
    request_id: UUID
    name: str
    request: ExperimentSearchContext
    registered_at: AwareUtcDatetime
    policy: HoldoutPolicy
    phase: Literal["search", "outer"]
    parent_family_id: str | None
    planned_count: int = Field(ge=1, le=64)
    potential_count: int = Field(ge=1, le=4096)
    search_count: int = Field(ge=1, le=64)
    note: ExperimentNote | None = None
    outer_admitted: bool = False
    evidence_id: Sha256 | None = None
    selected_search_experiment_id: Sha256 | None = None
    preparation_state: Literal["preparing", "ready", "cancelled"] = "ready"
    preparations: tuple[ExperimentPlannedSlotFact, ...] = Field(default=(), max_length=64)
    preparation_window_truncated: bool = False
    template_name: str | None = None
    template_rules: StrategyTemplate | None = None


class ExperimentPrivateSnapshot(RuntimeContractModel):
    available_at: AwareUtcDatetime
    families: tuple[ExperimentFamilyFact, ...]
    attempts: tuple[ExperimentAttemptFact, ...]
    policy: HoldoutPolicy
    truncated_owners: tuple[Owner, ...]
    visible_owners: tuple[Owner, ...] = ()


class ExperimentPrivateResultAuthority:
    """Installed server authority; browser identities and permits are never parsed here."""

    def __init__(self, registry: ExperimentRegistryReadonlyReader) -> None:
        if not isinstance(registry, ExperimentRegistryReadonlyReader):
            raise TypeError("private results require the original readonly registry")
        self.registry = registry

    def refresh_live_identity(self) -> None:
        # Original SQLite setup accepts content/ctime updates, never a replacement
        # inode, directory, owner or mode. Each read still pins its actual generation.
        self.registry._path_authority.rebind_and_assert_current_after_trusted_sqlite_change()

    def evidence(self, owner: str, family_id: str, evidence_id: str) -> ExperimentOverfitEvidence:
        from rquant.experiment_platform_evidence import ExperimentOverfitEvidence

        self.refresh_live_identity()
        with self.registry._read_snapshot() as connection:
            if not _private_platform_schema(connection):
                raise PermissionError("private evidence authority is not installed")
            rows = connection.execute(
                (
                    "SELECT payload_json FROM experiment_evidence WHERE owner=? AND "
                    "family_id=? AND evidence_id=? LIMIT "
                    "2"
                ),
                (owner, family_id, evidence_id),
            ).fetchall()
            if len(rows) != 1 or len(rows[0][0].encode()) > 8 * 1024 * 1024:
                raise PermissionError("private evidence is unavailable")
            evidence = ExperimentOverfitEvidence.model_validate_json(rows[0][0]).seal()
            if (evidence.owner, evidence.family_id, evidence.evidence_id) != (
                owner,
                family_id,
                evidence_id,
            ):
                raise ValueError("private evidence owner or content identity differs")
            return evidence

    def authorize(self, job: LabJobRecord, owner: str) -> ExperimentPreparationReceipt:
        self.refresh_live_identity()
        with self.registry._read_snapshot() as connection:
            if not _private_platform_schema(connection):
                raise PermissionError("private experiment authority is not installed")
            row = connection.execute(
                "SELECT a.owner,a.hypothesis_family,a.payload_json,r.payload_json "
                "FROM experiment_child_admission a JOIN experiment_family_request r "
                "ON r.family_id=a.hypothesis_family WHERE a.job_id=? LIMIT 2",
                (str(job.job_id),),
            ).fetchall()
            if len(row) != 1 or row[0][0] != owner:
                raise PermissionError("private experiment is not owned by this viewer")
            child = ExperimentChildAdmission.model_validate_json(row[0][2])
            family = ExperimentFamilyRecord.model_validate_json(row[0][3])
            if family.state != "ready" or (
                child.owner,
                child.family_id,
                family.owner,
                family.family_id,
            ) != (owner, row[0][1], owner, row[0][1]):
                raise PermissionError("private experiment owner facts conflict")
            index = next(
                (
                    i
                    for i in range(len(family.actual_configurations))
                    if connection.execute(
                        "SELECT 1 FROM experiment_prepared_child "
                        "WHERE family_id=? AND child_index=? AND "
                        "json_extract(payload_json,'$.prepared.formal_plan.spec.experiment_id')=?",
                        (family.family_id, i, child.experiment_id),
                    ).fetchone()
                ),
                None,
            )
            if index is None:
                raise PermissionError("private result has no original preparation")
            prepared_row = connection.execute(
                "SELECT payload_json FROM experiment_prepared_child "
                "WHERE family_id=? AND child_index=?",
                (family.family_id, index),
            ).fetchone()
            prepared = ExperimentPreparationReceipt.model_validate_json(prepared_row[0])
            expected = prepared.prepared.submission(job_id=job.job_id).command.spec
            if (
                expected != job.spec
                or job.spec_hash != expected.spec_hash
                or prepared.owner != owner
                or prepared.configuration != family.actual_configurations[index]
            ):
                raise PermissionError("private result differs from its exact admitted definition")
            if family.phase == "outer":
                grant_row = connection.execute(
                    (
                        "SELECT payload_json FROM experiment_outer_grant WHERE owner=? "
                        "AND request_id=?"
                    ),
                    (owner, str(family.request_id)),
                ).fetchone()
                if grant_row is None:
                    raise PermissionError("private outer result has no grant")
                grant = ExperimentOuterGrant.model_validate_json(grant_row[0])
                if (
                    family.family_id != "experiment-outer:" + grant.grant_id
                    or family.parent_family_id != grant.family_id
                    or prepared.configuration != family.actual_configurations[0]
                ):
                    raise PermissionError("private outer result grant differs")
            return prepared


class ExperimentPrivateProjectionReader:
    def __init__(
        self,
        *,
        registry: ExperimentRegistryReadonlyReader,
        jobs: LabJobReader,
        owners: frozenset[str] = frozenset(),
    ) -> None:
        if len(owners) > 64:
            raise ValueError("private experiment source exceeds 64 owners")
        self.registry, self.jobs = registry, jobs
        self.owners = owners
        self.authority = ExperimentPrivateResultAuthority(registry)

    def snapshot(self, observed_at: datetime) -> ExperimentPrivateSnapshot | None:
        families: list[ExperimentFamilyFact] = []
        attempts: list[ExperimentAttemptFact] = []
        truncated: list[str] = []
        self.authority.refresh_live_identity()
        with self.registry._read_snapshot() as connection:
            if not _private_platform_schema(connection):
                return None
            policy = HoldoutPolicy.model_validate_json(
                connection.execute(
                    "SELECT value FROM experiment_platform_metadata WHERE key='holdout_policy'"
                ).fetchone()[0]
            )
            stored_owners = connection.execute(
                "SELECT owner FROM experiment_private_family UNION "
                "SELECT owner FROM experiment_family_request ORDER BY owner LIMIT 65"
            ).fetchall()
            owners = tuple(sorted(self.owners | {row[0] for row in stored_owners}))
            if len(owners) > 64:
                raise ValueError("private experiment source exceeds 64 owners")
            for owner in owners:
                pending_rows = connection.execute(
                    "SELECT payload_json FROM experiment_family_request WHERE owner=? "
                    "AND state!='ready' ORDER BY json_extract(payload_json,'$.registered_at') DESC,"
                    "family_id DESC LIMIT 501",
                    (owner,),
                ).fetchall()
                retained_pending = []
                pending_count = 0
                for pending_row in pending_rows:
                    if pending_count >= 500:
                        break
                    record = ExperimentFamilyRecord.model_validate_json(pending_row[0])
                    if record.owner != owner or record.state == "ready":
                        raise ValueError("private preparing owner or state differs")
                    retained_pending.append(record)
                    pending_count += len(record.actual_configurations)
                for record in retained_pending:
                    slot_rows = connection.execute(
                        "SELECT payload_json FROM experiment_template_slot WHERE family_id=? "
                        "ORDER BY child_index LIMIT 65",
                        (record.family_id,),
                    ).fetchall()
                    slots = tuple(
                        ExperimentTemplateSlot.model_validate_json(r[0]) for r in slot_rows
                    )
                    if record.template_baseline is not None and (
                        len(slots) != len(record.actual_configurations)
                        or tuple(s.index for s in slots) != tuple(range(len(slots)))
                        or any(
                            (s.owner, s.family_id, s.baseline_hash)
                            != (owner, record.family_id, canonical_sha256(record.template_baseline))
                            for s in slots
                        )
                    ):
                        raise ValueError(
                            "private planned slots differ from their complete admitted array"
                        )
                    indices = {
                        r[0]
                        for r in connection.execute(
                            "SELECT child_index FROM experiment_prepared_child WHERE family_id=?",
                            (record.family_id,),
                        ).fetchall()
                    }
                    if not indices <= set(range(len(record.actual_configurations))):
                        raise ValueError("private preparation contains an unplanned index")
                    preparations = tuple(
                        ExperimentPlannedSlotFact(
                            index=i,
                            configuration=cfg,
                            definition_state=slots[i].state
                            if slots
                            else ("cancelled" if record.state == "cancelled" else "pending"),
                            input_prepared=i in indices,
                            failure=slots[i].failure if slots else None,
                        )
                        for i, cfg in enumerate(record.actual_configurations)
                    )
                    parent = record
                    if record.parent_family_id is not None:
                        parent_row = connection.execute(
                            "SELECT payload_json FROM experiment_family_request "
                            "WHERE owner=? AND family_id=?",
                            (owner, record.parent_family_id),
                        ).fetchone()
                        if parent_row is None:
                            raise ValueError("preparing outer family lost its exact owner parent")
                        parent = ExperimentFamilyRecord.model_validate_json(parent_row[0])
                    potential = 1
                    for dimension in record.request.dimensions:
                        potential *= len(dimension.values)
                    note_row = connection.execute(
                        "SELECT payload_json FROM experiment_note WHERE owner=? AND family_id=?",
                        (owner, record.family_id),
                    ).fetchone()
                    families.append(
                        ExperimentFamilyFact(
                            owner=owner,
                            family_id=record.family_id,
                            request_id=record.request_id,
                            name=record.request.name,
                            template_name=None
                            if record.template_baseline is None
                            else record.template_baseline.name,
                            template_rules=None
                            if record.template_baseline is None
                            else record.template_baseline.version.rules,
                            request=ExperimentSearchContext.from_request(record.request),
                            registered_at=record.registered_at,
                            policy=record.policy,
                            phase=record.phase,
                            parent_family_id=record.parent_family_id,
                            planned_count=len(preparations),
                            potential_count=potential,
                            search_count=len(parent.actual_configurations),
                            preparation_state=record.state,
                            preparations=preparations,
                            preparation_window_truncated=len(retained_pending) < len(pending_rows),
                            note=None
                            if note_row is None
                            else ExperimentNote.model_validate_json(note_row[0]),
                        )
                    )
                rows = connection.execute(
                    "SELECT a.* FROM experiment_attempt a JOIN experiment_private_family f "
                    "ON a.hypothesis_family=f.hypothesis_family WHERE f.owner=? "
                    "ORDER BY a.registered_at DESC,a.experiment_id DESC LIMIT 501",
                    (owner,),
                ).fetchall()
                if len(rows) > 500:
                    truncated.append(owner)
                family_ids = tuple(sorted({r["hypothesis_family"] for r in rows[:500]}))
                records: dict[str, ExperimentFamilyRecord] = {}
                for family_id in family_ids:
                    row = connection.execute(
                        (
                            "SELECT payload_json FROM experiment_family_request WHERE "
                            "family_id=? AND "
                            "owner=?"
                        ),
                        (family_id, owner),
                    ).fetchone()
                    if row is None:
                        raise ValueError("private family request is missing")
                    record = ExperimentFamilyRecord.model_validate_json(row[0])
                    if (
                        record.state != "ready"
                        or record.owner != owner
                        or record.family_id != family_id
                    ):
                        raise ValueError("private family request conflicts with owner")
                    records[family_id] = record
                    count = len(record.actual_configurations)
                    parent = record
                    if record.parent_family_id is not None:
                        parent_row = connection.execute(
                            (
                                "SELECT payload_json FROM experiment_family_request "
                                "WHERE family_id=? AND "
                                "owner=?"
                            ),
                            (record.parent_family_id, owner),
                        ).fetchone()
                        if parent_row is None:
                            raise ValueError("outer search family is missing")
                        parent = ExperimentFamilyRecord.model_validate_json(parent_row[0])
                    note_row = connection.execute(
                        "SELECT payload_json FROM experiment_note WHERE family_id=? AND owner=?",
                        (family_id, owner),
                    ).fetchone()
                    head = connection.execute(
                        (
                            "SELECT evidence_id FROM experiment_evidence_head WHERE "
                            "family_id=? AND "
                            "owner=?"
                        ),
                        (family_id, owner),
                    ).fetchone()
                    outer = (
                        connection.execute(
                            (
                                "SELECT payload_json FROM experiment_outer_grant WHERE "
                                "owner=? AND request_id=?"
                            ),
                            (owner, str(record.request_id)),
                        ).fetchone()
                        if record.phase == "outer"
                        else None
                    )
                    potential = 1
                    for dimension in record.request.dimensions:
                        potential *= len(dimension.values)
                    families.append(
                        ExperimentFamilyFact(
                            owner=owner,
                            family_id=family_id,
                            request_id=record.request_id,
                            name=record.request.name,
                            template_name=None
                            if record.template_baseline is None
                            else record.template_baseline.name,
                            template_rules=None
                            if record.template_baseline is None
                            else record.template_baseline.version.rules,
                            request=ExperimentSearchContext.from_request(record.request),
                            registered_at=record.registered_at,
                            policy=record.policy,
                            phase=record.phase,
                            parent_family_id=record.parent_family_id,
                            planned_count=count,
                            potential_count=potential,
                            search_count=len(parent.actual_configurations),
                            evidence_id=None if head is None else head[0],
                            selected_search_experiment_id=None
                            if outer is None
                            else ExperimentOuterGrant.model_validate_json(outer[0]).experiment_id,
                            note=None
                            if note_row is None
                            else ExperimentNote.model_validate_json(note_row[0]),
                            outer_admitted=connection.execute(
                                (
                                    "SELECT 1 FROM experiment_outer_grant WHERE owner=? "
                                    "AND json_extract(payload_json,'$.family_id')=? "
                                    "LIMIT "
                                    "1"
                                ),
                                (owner, family_id),
                            ).fetchone()
                            is not None,
                        )
                    )
                # Publish full retained families: a family view must never omit its failed rows.
                for family_id, record in records.items():
                    full_rows = connection.execute(
                        "SELECT * FROM experiment_attempt WHERE hypothesis_family=? "
                        "ORDER BY registered_at DESC,experiment_id DESC LIMIT 65",
                        (family_id,),
                    ).fetchall()
                    if len(full_rows) != len(record.actual_configurations):
                        raise ValueError("private family attempt count conflicts")
                    for attempt_row in full_rows:
                        attempt = self.registry._validated_attempt_from_row(
                            connection, attempt_row, observed=observed_at
                        )
                        child_rows = connection.execute(
                            (
                                "SELECT payload_json FROM experiment_child_admission "
                                "WHERE experiment_id=? AND "
                                "owner=?"
                            ),
                            (attempt.spec.experiment_id, owner),
                        ).fetchall()
                        if len(child_rows) != 1:
                            raise ValueError("private child owner binding is missing")
                        child = ExperimentChildAdmission.model_validate_json(child_rows[0][0])
                        preps = connection.execute(
                            (
                                "SELECT child_index,payload_json FROM "
                                "experiment_prepared_child WHERE family_id=? ORDER BY "
                                "child_index LIMIT "
                                "65"
                            ),
                            (family_id,),
                        ).fetchall()
                        matched = tuple(
                            (i, ExperimentPreparationReceipt.model_validate_json(p))
                            for i, p in preps
                            if ExperimentPreparationReceipt.model_validate_json(
                                p
                            ).prepared.formal_plan.spec
                            == attempt.spec
                        )
                        if len(matched) != 1:
                            raise ValueError("private attempt has no exact preparation")
                        index, prepared = matched[0]
                        expected = prepared.prepared.submission(job_id=child.job_id).command.spec
                        job = self.jobs.get_job(child.job_id)
                        if job is not None and job.spec != expected:
                            raise ValueError("private Lab child spec differs")
                        sealed = self.jobs.get_artifact_preview_authority(child.job_id)
                        if sealed is not None and job is None:
                            raise ValueError("private result has no job")
                        if (
                            child.owner,
                            child.family_id,
                            prepared.owner,
                            prepared.index,
                            prepared.configuration,
                        ) != (owner, family_id, owner, index, record.actual_configurations[index]):
                            raise ValueError("private exact configuration differs")
                        attempts.append(
                            ExperimentAttemptFact(
                                owner=owner,
                                family_id=family_id,
                                index=index,
                                configuration=record.actual_configurations[index],
                                attempt=attempt,
                                child=child,
                                input_hash=prepared.prepared.published.input_hash,
                                source_identity=prepared.source_identity,
                                spec_hash=expected.spec_hash,
                                manifest_hash=None
                                if sealed is None
                                else sealed.evidence.manifest_hash,
                                result_hash=None
                                if sealed is None
                                else sealed.evidence.complete_result_hash,
                            )
                        )
        return ExperimentPrivateSnapshot(
            available_at=observed_at,
            families=tuple(families),
            attempts=tuple(attempts),
            policy=policy,
            truncated_owners=tuple(truncated),
            visible_owners=owners,
        )

    def __call__(self, observed_at: datetime) -> tuple[ServingProjectionPayload, ...]:
        snapshot = self.snapshot(observed_at)
        if snapshot is None:
            return ()
        rows = tuple(
            {
                "owner": f.owner,
                "experiment_id": f.attempt.spec.experiment_id,
                "family_id": f.family_id,
                "registered_at": f.attempt.registered_at.isoformat(),
                "payload_json": f.model_dump_json(),
            }
            for f in snapshot.attempts
        )
        families = tuple(
            {"owner": f.owner, "family_id": f.family_id, "payload_json": f.model_dump_json()}
            for f in snapshot.families
        )
        windows = []
        for owner in snapshot.visible_owners:
            visible = sorted(
                (f for f in snapshot.attempts if f.owner == owner),
                key=lambda f: (f.attempt.registered_at, f.attempt.spec.experiment_id),
                reverse=True,
            )[:500]
            windows.append(
                {
                    "owner": owner,
                    "retained_count": len(visible),
                    "truncated": owner in snapshot.truncated_owners,
                    "oldest_registered_at": visible[-1].attempt.registered_at.isoformat()
                    if visible
                    else None,
                    "policy_json": snapshot.policy.model_dump_json(),
                }
            )
        payloads = tuple(
            ServingProjectionPayload(table_name=name, available_at=observed_at, rows=value)
            for name, value in zip(PRIVATE_TABLES, (rows, families, tuple(windows)), strict=True)
        )
        if sum(_projection_json_bytes(p) for p in payloads) > 8 * 1024 * 1024:
            raise ValueError("private experiment source exceeds its 8 MiB budget")
        return payloads
