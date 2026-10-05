"""Recent template runs come only from original sealed Lab authority."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Self
from uuid import UUID

from pydantic import Field, model_validator

from rquant.backtest.contracts import Sha256
from rquant.lab_artifact_preview import ArtifactPreviewReader
from rquant.lab_jobs import LabJobReader
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

if TYPE_CHECKING:
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
