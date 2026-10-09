"""Recent template runs come only from original sealed Lab authority."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Self
from uuid import UUID

from pydantic import Field, model_validator

from rquant.backtest.contracts import Sha256
from rquant.lab_artifact_preview import ArtifactPreviewReader
from rquant.lab_jobs import LabArtifactPreviewAuthority, LabJobReader
from rquant.portfolio_backtest_models import MAX_BUNDLE_BYTES
from rquant.research_run_spec import ResearchRunSpec
from rquant.runtime_contracts import RuntimeContractModel
from rquant.strategy_authoring import StrategyAuthoringStore
from rquant.strategy_authoring_commands import StrategyAuthoringIdentity, StrategyTemplateHead
from rquant.strategy_authoring_projection_contract import MAX_TEMPLATE_RUN_ADMISSIONS
from rquant.strategy_authoring_source import template_source_code_identity
from rquant.strategy_template import TEMPLATE_ID_PATTERN
from rquant.strategy_template_adapter import (
    StrategyTemplateAdapter,
    StrategyTemplateAdapterCatalog,
    StrategyTemplateExecutionVersion,
    StrategyTemplateRunParameters,
)
from rquant.strategy_template_run import StrategyTemplateResult

if TYPE_CHECKING:
    from rquant.experiment_platform_projection import ExperimentPrivateResultAuthority
    from rquant.experiment_platform_template_models import PreparedExperimentTemplate
    from rquant.strategy_authoring_projection import StrategyTemplateRecentRun

REFERENCE_COLUMNS = (
    "owner_id",
    "strategy_id",
    "version",
    "registration_fingerprint",
    "record_hash",
    "spec_fingerprint",
    "input_hash",
    "rules_hash",
    "source_code_identity",
    "request_id",
    "result_hash",
    "complete",
)


class TemplateReadResult(RuntimeContractModel):
    job_id: UUID
    spec_hash: Sha256
    manifest_hash: Sha256
    result_hash: Sha256
    result: StrategyTemplateResult


def bind_complete_template_result(
    prepared: PreparedExperimentTemplate, result: StrategyTemplateResult
) -> None:
    """Bind the complete original payload; no account or result is computed here."""
    from rquant.experiment_platform_template_models import PreparedExperimentTemplate

    if type(prepared) is not PreparedExperimentTemplate:
        raise TypeError("template result needs its actual typed private preparation")
    result = StrategyTemplateResult.model_validate(result.model_dump(mode="python"))
    frozen = prepared.frozen
    if (
        result.owner_id,
        result.strategy_id,
        result.version,
        result.definition_fingerprint,
        result.definition_record_hash,
        result.input_hash,
        result.calendar_source_identity,
        result.cost_spec_id,
        result.status,
    ) != (
        frozen.owner_id,
        prepared.catalog.versions[0].strategy_id,
        prepared.catalog.versions[0].head.version,
        frozen.definition.fingerprint,
        frozen.definition.record_hash,
        frozen.input_hash,
        frozen.request.calendar.source_identity,
        frozen.request.execution_cost_spec.cost_spec_id,
        "complete",
    ):
        raise ValueError("full template result differs from its original preparation")
    if (
        tuple(d.trade_date for d in result.days) != tuple(d.trade_date for d in frozen.days)
        or any(d.account is None or d.daily_return is None for d in result.days)
        or len(result.model_dump_json().encode()) > MAX_BUNDLE_BYTES
    ):
        raise ValueError("full template result has incomplete days or exceeds its byte budget")


class TemplateSealedResultReference(RuntimeContractModel):
    owner_id: str = Field(min_length=1, max_length=128)
    strategy_id: str = Field(pattern=TEMPLATE_ID_PATTERN)
    version: int = Field(strict=True, ge=1, le=4096)
    registration_fingerprint: Sha256
    record_hash: Sha256
    spec_fingerprint: Sha256
    input_hash: Sha256
    rules_hash: Sha256
    source_code_identity: Sha256
    request_id: str
    result_hash: Sha256
    complete: bool = Field(strict=True)

    @model_validator(mode="after")
    def canonical_identity(self) -> Self:
        if str(UUID(self.request_id)) != self.request_id:
            raise ValueError("sealed template request ID is not canonical")
        return self

    def bind_parameters(self, parameters: StrategyTemplateRunParameters) -> None:
        expected = parameters.model_dump(mode="python", exclude={"work_units"})
        if self.model_dump(mode="python", exclude={"result_hash", "complete"}) != expected:
            raise ValueError("sealed template reference differs from original plan parameters")


class StrategyTemplateSealedResultReader:
    def __init__(self, *, reader: LabJobReader, artifact_reader: ArtifactPreviewReader) -> None:
        if (
            type(reader) is not LabJobReader
            or type(artifact_reader) is not ArtifactPreviewReader
            or artifact_reader.reader is not reader
        ):
            raise TypeError("template results require the same concrete original Lab authority")
        self.reader = reader
        self.artifact_reader = artifact_reader
        self.full_previews = ArtifactPreviewReader(
            reader=reader,
            artifact_root=artifact_reader.artifact_root,
            max_preview_rows=1,
            max_preview_columns=len(REFERENCE_COLUMNS),
            max_preview_cell_bytes=MAX_BUNDLE_BYTES,
            max_preview_arrow_bytes=MAX_BUNDLE_BYTES + 1024,
            max_preview_serialized_bytes=2 * MAX_BUNDLE_BYTES + 1024 * 1024,
        )

    def read_private(
        self,
        job_id: UUID,
        *,
        private_owner: str,
        private_authority: ExperimentPrivateResultAuthority,
        expected_result_hash: str,
    ) -> TemplateReadResult:
        from rquant.experiment_platform_projection import ExperimentPrivateResultAuthority
        from rquant.experiment_platform_template_models import PreparedExperimentTemplate

        if type(private_authority) is not ExperimentPrivateResultAuthority:
            raise TypeError("private template results require installed owner authority")
        authority = self.reader.get_artifact_preview_authority(job_id)
        if authority is None or authority.evidence.complete_result_hash != expected_result_hash:
            raise ValueError("exact private template sealed result is unavailable or changed")
        receipt = private_authority.authorize(authority.job, private_owner)
        prepared = receipt.prepared
        if type(prepared) is not PreparedExperimentTemplate:
            raise ValueError("private template result has another original source kind")
        preview = self.full_previews.preview(
            job_id, table_name="template_result", row_limit=1, column_limit=2
        )
        table = preview.table
        if (
            set(preview.available_tables)
            != {"template_reference", "template_result", "equity", "orders", "exits", "summary"}
            or table is None
            or table.columns != ("result_hash", "payload")
            or table.total_rows != 1
            or table.total_columns != 2
            or table.rows_truncated
            or table.columns_truncated
            or len(table.rows) != 1
            or any(not isinstance(v, str) for v in table.rows[0])
            or len(table.rows[0][1].encode()) > MAX_BUNDLE_BYTES
        ):
            raise ValueError("full template result schema, inventory or byte budget differs")
        result = StrategyTemplateResult.model_validate_json(table.rows[0][1])
        bind_complete_template_result(prepared, result)
        if result.content_hash != table.rows[0][0]:
            raise ValueError("template sealed payload hash differs from its table reference")
        after = self.reader.get_artifact_preview_authority(job_id)
        if (
            after is None
            or private_authority.authorize(after.job, private_owner) != receipt
            or (
                after != authority
                or preview.spec_hash != authority.job.spec_hash
                or preview.complete_result_hash != expected_result_hash
                or preview.manifest_hash != authority.evidence.manifest_hash
            )
        ):
            raise ValueError("private template authority changed during full sealed read")
        return TemplateReadResult(
            job_id=job_id,
            spec_hash=preview.spec_hash,
            manifest_hash=preview.manifest_hash,
            result_hash=preview.complete_result_hash,
            result=result,
        )

    def read_owned(self, job_id: UUID, *, private_owner: str, expected_result_hash: str,
                   collaboration: object) -> TemplateReadResult:
        """Read the ordinary accepted run through its original complete previews."""
        from rquant.web.collaboration_gateway import CollaborationGateway
        if type(collaboration) is not CollaborationGateway:
            raise PermissionError("original private ownership gateway is required")
        authority = self.reader.get_artifact_preview_authority(job_id)
        if authority is None or authority.evidence.complete_result_hash != expected_result_hash:
            raise ValueError("ordinary strategy sealed authority is unavailable or changed")
        original = collaboration.result_owner(private_owner, domain="strategy", job_id=str(job_id),
            spec_hash=authority.job.spec_hash)
        result = self._read_ordinary_payload(job_id, authority=authority,
            private_owner=private_owner, original_command_id=original.command_id,
            expected_result_hash=expected_result_hash)
        if collaboration.result_owner(private_owner, domain="strategy", job_id=str(job_id),
                spec_hash=result.spec_hash) != original:
            raise ValueError("ordinary strategy original owner changed during read")
        return result

    def read_run(
        self, job_id: UUID, *, store: StrategyAuthoringStore,
        expected_identity: StrategyAuthoringIdentity, private_owner: str,
        expected_result_hash: str,
    ) -> TemplateReadResult:
        """Internal domain read of the exact original admitted and sealed run."""
        if type(store) is not StrategyAuthoringStore or type(expected_identity) is not StrategyAuthoringIdentity:
            raise TypeError("template run requires the concrete original metadata authority")
        authority = self.reader.get_artifact_preview_authority(job_id)
        if authority is None or authority.evidence.complete_result_hash != expected_result_hash:
            raise ValueError("ordinary strategy sealed authority is unavailable or changed")
        parameters, admission = self._run_parameters(job_id, store=store,
            expected_identity=expected_identity, private_owner=private_owner,
            spec=authority.job.spec)
        result = self._read_ordinary_payload(job_id, authority=authority,
            private_owner=private_owner, original_command_id=str(job_id),
            expected_result_hash=expected_result_hash, parameters=parameters)
        if self._run_parameters(job_id, store=store, expected_identity=expected_identity,
                private_owner=private_owner, spec=authority.job.spec) != (parameters, admission):
            raise ValueError("ordinary strategy run admission changed during full read")
        return result

    @staticmethod
    def _run_parameters(job_id: UUID, *, store: StrategyAuthoringStore,
                        expected_identity: StrategyAuthoringIdentity, private_owner: str,
                        spec: ResearchRunSpec) -> tuple[StrategyTemplateRunParameters, tuple[object, ...]]:
        from rquant.strategy_template_run_commands import (
            AcceptedStrategyTemplateRun,
            StrategyTemplateRunReceipt,
        )

        with store._connection(expected_identity=expected_identity) as connection:
            lengths = connection.execute("SELECT length(CAST(frozen AS BLOB)), length(CAST(receipt AS BLOB)) FROM run_admissions WHERE command_id=?", (str(job_id),)).fetchone()
            if lengths is None or lengths[0] is None or lengths[1] is None or lengths[0] > 32 * 1024 or lengths[1] > 4096:
                raise ValueError("template run lacks its bounded original completed admission")
            row = connection.execute("SELECT * FROM run_admissions WHERE command_id=?", (str(job_id),)).fetchone()
            if row["owner_id"] != private_owner:
                raise PermissionError("ordinary strategy run belongs to another owner")
            accepted = AcceptedStrategyTemplateRun.model_validate_json(row["frozen"])
            receipt = StrategyTemplateRunReceipt.model_validate_json(row["receipt"])
            head = StrategyTemplateHead.model_validate_json(row["head"])
            metadata = store._version(connection, row["strategy_id"], head.version, private_owner)
            if (accepted.metadata_identity != expected_identity or accepted.spec != spec
                    or (accepted.owner_id, accepted.request.command_id, accepted.request.request_hash,
                        accepted.request.strategy_id, accepted.request.head, accepted.spec.spec_hash) !=
                        (private_owner, str(job_id), row["request_hash"], row["strategy_id"], head, row["spec_hash"])
                    or metadata.head != head or
                    (receipt.owner_id, receipt.command_id, receipt.strategy_id, receipt.head,
                        receipt.original_request_hash, receipt.job_id, receipt.spec_hash) !=
                        (private_owner, str(job_id), row["strategy_id"], head, row["request_hash"], job_id, row["spec_hash"])):
                raise ValueError("ordinary strategy original run, owner or submitted spec differs")
            definition = store.definition_registry(metadata.strategy_id).read_strategy_spec(head.registration_fingerprint)
            if definition is None:
                raise ValueError("template run lost its immutable original definition")
            catalog = StrategyTemplateAdapterCatalog(metadata_identity=expected_identity,
                source_code_identity=template_source_code_identity(), versions=(
                    StrategyTemplateExecutionVersion(owner_id=metadata.owner_id,
                        strategy_id=metadata.strategy_id, head=metadata.head, rules=metadata.rules,
                        definition=definition),))
            parameters = StrategyTemplateAdapter(metadata.strategy_id, catalog=catalog).parameters(spec)
            admission = tuple(row)
        if store.identity() != expected_identity:
            raise ValueError("template original metadata identity changed during full read")
        return parameters, admission

    def _read_ordinary_payload(self, job_id: UUID, *, authority: LabArtifactPreviewAuthority,
                               private_owner: str, original_command_id: str,
                               expected_result_hash: str,
                               parameters: StrategyTemplateRunParameters | None = None) -> TemplateReadResult:
        reference_preview = self.full_previews.preview(job_id, table_name="template_reference",
            row_limit=1, column_limit=len(REFERENCE_COLUMNS))
        reference_table = reference_preview.table
        if (reference_table is None or reference_table.columns != REFERENCE_COLUMNS
                or reference_table.total_rows != 1 or reference_table.total_columns != len(REFERENCE_COLUMNS)
                or reference_table.rows_truncated or reference_table.columns_truncated or len(reference_table.rows) != 1):
            raise ValueError("ordinary strategy reference is incomplete")
        reference = TemplateSealedResultReference.model_validate(dict(zip(REFERENCE_COLUMNS, reference_table.rows[0], strict=True)))
        arguments = {item.name: item.value for item in authority.job.spec.parameters.arguments}
        for name in ("owner_id", "strategy_id", "version", "registration_fingerprint", "record_hash", "spec_fingerprint", "input_hash", "rules_hash", "request_id", "source_code_identity"):
            if arguments.get(name) != getattr(reference, name):
                raise ValueError("ordinary strategy reference differs from original submitted spec")
        if parameters is not None:
            reference.bind_parameters(parameters)
        if (reference.owner_id != private_owner or reference.request_id != original_command_id
                or reference.request_id != str(job_id) or not reference.complete):
            raise PermissionError("ordinary strategy reference has another owner or original run")
        preview = self.full_previews.preview(job_id, table_name="template_result", row_limit=1, column_limit=2)
        table = preview.table
        if (set(preview.available_tables) != {"template_reference", "template_result", "equity", "orders", "exits", "summary"}
                or table is None or table.columns != ("result_hash", "payload") or table.total_rows != 1
                or table.total_columns != 2 or table.rows_truncated or table.columns_truncated
                or len(table.rows) != 1 or any(not isinstance(v, str) for v in table.rows[0])
                or len(table.rows[0][1].encode()) > MAX_BUNDLE_BYTES):
            raise ValueError("ordinary strategy full payload exceeds its original bounds")
        result = StrategyTemplateResult.model_validate_json(table.rows[0][1])
        if (result.content_hash != table.rows[0][0] or result.content_hash != reference.result_hash
                or (result.owner_id, result.strategy_id, result.version, result.input_hash,
                    result.definition_fingerprint, result.definition_record_hash) !=
                    (reference.owner_id, reference.strategy_id, reference.version, reference.input_hash,
                    reference.registration_fingerprint, reference.record_hash)
                or result.status != "complete" or any(day.account is None or day.daily_return is None for day in result.days)):
            raise ValueError("ordinary strategy complete payload/reference binding differs")
        after = self.reader.get_artifact_preview_authority(job_id)
        if (after != authority or (preview.spec_hash, preview.manifest_hash, preview.complete_result_hash) !=
                (authority.job.spec_hash, authority.evidence.manifest_hash, expected_result_hash)
                or (reference_preview.spec_hash, reference_preview.manifest_hash, reference_preview.complete_result_hash) !=
                    (preview.spec_hash, preview.manifest_hash, preview.complete_result_hash)):
            raise ValueError("ordinary strategy original authority changed during read")
        return TemplateReadResult(job_id=job_id, spec_hash=preview.spec_hash, manifest_hash=preview.manifest_hash,
            result_hash=preview.complete_result_hash, result=result)

    def recent_runs(
        self,
        store: StrategyAuthoringStore,
        *,
        expected_identity: StrategyAuthoringIdentity,
        as_of: datetime,
    ) -> tuple[StrategyTemplateRecentRun, ...]:
        from rquant.strategy_authoring_projection import StrategyTemplateRecentRun

        runs: list[StrategyTemplateRecentRun] = []
        with store._connection(expected_identity=expected_identity) as connection:
            rows = connection.execute(
                "SELECT * FROM run_admissions ORDER BY command_id LIMIT ?",
                (MAX_TEMPLATE_RUN_ADMISSIONS + 1,),
            ).fetchall()
            if len(rows) > MAX_TEMPLATE_RUN_ADMISSIONS:
                raise ValueError("template result references exceed the original command budget")
            for row in rows:
                job_id = UUID(row["command_id"])
                authority = self.reader.get_artifact_preview_authority(job_id)
                if authority is None or authority.job.updated_at > as_of:
                    continue
                head = StrategyTemplateHead.model_validate_json(row["head"])
                metadata = store._version(
                    connection, row["strategy_id"], head.version, row["owner_id"]
                )
                if metadata.head != head or authority.job.spec_hash != row["spec_hash"]:
                    raise ValueError(
                        "template recent result differs from the original accepted run"
                    )
                definition = store.definition_registry(metadata.strategy_id).read_strategy_spec(
                    head.registration_fingerprint
                )
                if definition is None:
                    raise ValueError("template result lost its original definition")
                catalog = StrategyTemplateAdapterCatalog(
                    metadata_identity=expected_identity,
                    source_code_identity=template_source_code_identity(),
                    versions=(
                        StrategyTemplateExecutionVersion(
                            owner_id=metadata.owner_id,
                            strategy_id=metadata.strategy_id,
                            head=metadata.head,
                            rules=metadata.rules,
                            definition=definition,
                        ),
                    ),
                )
                parameters = StrategyTemplateAdapter(
                    metadata.strategy_id, catalog=catalog
                ).parameters(authority.job.spec)
                if parameters.request_id != row["command_id"]:
                    raise ValueError("template result is not the original command's exact job")
                preview = self.artifact_reader.preview(
                    job_id,
                    table_name="template_reference",
                    row_limit=1,
                    column_limit=len(REFERENCE_COLUMNS),
                )
                table = preview.table
                if (
                    table is None
                    or table.columns != REFERENCE_COLUMNS
                    or table.total_rows != 1
                    or table.total_columns != len(REFERENCE_COLUMNS)
                    or table.rows_truncated
                    or table.columns_truncated
                    or len(table.rows) != 1
                    or preview.spec_hash != row["spec_hash"]
                ):
                    raise ValueError(
                        "sealed template reference has a different complete schema or job"
                    )
                reference = TemplateSealedResultReference.model_validate(
                    dict(zip(REFERENCE_COLUMNS, table.rows[0], strict=True))
                )
                reference.bind_parameters(parameters)
                if reference.complete:
                    runs.append(
                        StrategyTemplateRecentRun(
                            job_id=job_id,
                            owner_id=reference.owner_id,
                            strategy_id=reference.strategy_id,
                            head=head,
                            input_hash=reference.input_hash,
                            result_hash=reference.result_hash,
                            spec_hash=preview.spec_hash,
                            manifest_hash=preview.manifest_hash,
                            complete_result_hash=preview.complete_result_hash,
                            completed_at=authority.job.updated_at,
                        )
                    )
        if store.identity() != expected_identity:
            raise ValueError(
                "template original metadata identity changed during sealed result read"
            )
        return tuple(runs)
