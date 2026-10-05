"""Original sealed Lab authority is the only completion fact for paper analyses."""

from __future__ import annotations

from datetime import datetime
from typing import Literal, TYPE_CHECKING, Self
from uuid import UUID

from rquant.lab_artifact_preview import ArtifactPreviewReader
from rquant.lab_jobs import LabJobReader, JobStatus
from pydantic import Field, model_validator
from rquant.paper_portfolio_band import PaperBacktestBandResult
from rquant.paper_portfolio_models import Sha256
from rquant.paper_reconcile import PaperReconcileResult
from rquant.paper_research import PaperResearchRunParameters
from rquant.paper_research_commands import OwnedRunPaperPortfolioResearch
if TYPE_CHECKING:
    from rquant.paper_research_submission import PaperResearchRunBackend
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel


class PaperResearchSealedAnalysis(RuntimeContractModel):
    task_name: Literal["paper_reconcile", "paper_backtest_band"]
    job_id: UUID
    account_id: str
    configuration_fingerprint: Sha256
    configuration_version: int = Field(strict=True, ge=1, le=4096)
    spec_hash: Sha256
    manifest_hash: Sha256
    complete_result_hash: Sha256
    result_hash: Sha256
    completed_at: AwareUtcDatetime
    reconcile: PaperReconcileResult | None = None
    band: PaperBacktestBandResult | None = None

    @model_validator(mode="after")
    def bound_result(self) -> Self:
        result = self.reconcile if self.task_name == "paper_reconcile" else self.band
        if (result is None or (self.reconcile is not None) == (self.band is not None)
                or result.configuration_fingerprint != self.configuration_fingerprint or result.fingerprint != self.result_hash
                or self.reconcile is not None and self.reconcile.account.account_id != self.account_id):
            raise ValueError("paper sealed analysis differs from its exact task, account or result body")
        return self


class PaperResearchSummary(RuntimeContractModel):
    task_name: Literal["paper_reconcile", "paper_backtest_band"]
    job_id: UUID
    account_id: str
    configuration_fingerprint: Sha256
    configuration_version: int = Field(strict=True, ge=1, le=4096)
    status: JobStatus | Literal["submitted"]
    accepted_at: AwareUtcDatetime
    reason: str | None = Field(default=None, max_length=512)
    sealed: PaperResearchSealedAnalysis | None = None

    @model_validator(mode="after")
    def same_sealed_job(self) -> Self:
        if self.sealed is not None and (self.status is not JobStatus.SUCCEEDED or self.reason is not None
                or (self.task_name, self.job_id, self.account_id, self.configuration_fingerprint, self.configuration_version) != (
                self.sealed.task_name, self.sealed.job_id, self.sealed.account_id, self.sealed.configuration_fingerprint, self.sealed.configuration_version)
                or self.accepted_at > self.sealed.completed_at):
            raise ValueError("paper publication differs from the original sealed job identity")
        return self


class PaperResearchResultReader:
    def __init__(self, *, backend: PaperResearchRunBackend, reader: LabJobReader, artifact_reader: ArtifactPreviewReader) -> None:
        from rquant.paper_research_submission import PaperResearchRunBackend
        if (type(backend) is not PaperResearchRunBackend or type(reader) is not LabJobReader
                or type(artifact_reader) is not ArtifactPreviewReader or reader is not backend.facade.reader or artifact_reader.reader is not reader):
            raise TypeError("paper results require the same concrete original Lab authority")
        self.backend, self.reader, self.artifact_reader = backend, reader, artifact_reader

    def summary(self, *, account_id: str, job_id: UUID, owner_id: str, as_of: datetime) -> PaperResearchSummary:
        state = self.backend.preparer.source_for(account_id, owner_id).runtime.state
        self.backend._table(state)
        with state._connection() as connection:
            row = connection.execute("SELECT owned_body FROM paper_research_admissions WHERE command_id=?", (str(job_id),)).fetchone()
        if row is None:
            raise PermissionError("paper research job is not registered")
        command = OwnedRunPaperPortfolioResearch.model_validate_json(row[0])
        self.backend.validate(command)
        job = self.reader.get_job(job_id)
        if job is not None and job.spec != command.spec:
            raise ValueError("paper job differs from its original accepted plan")
        sealed = self.read(account_id=account_id, job_id=job_id, owner_id=owner_id, as_of=as_of)
        return PaperResearchSummary(task_name=command.task_name, job_id=job_id, account_id=account_id,
                                     configuration_fingerprint=command.configuration_fingerprint, configuration_version=command.catalog.configuration.version,
                                     status=job.status if job is not None and job.updated_at <= as_of else "submitted", accepted_at=command.accepted_at,
                                     reason="结果尚未封存" if sealed is None else None, sealed=sealed)

    def read(self, *, account_id: str, job_id: UUID, owner_id: str, as_of: datetime) -> PaperResearchSealedAnalysis | None:
        state = self.backend.preparer.source_for(account_id, owner_id).runtime.state
        self.backend._table(state)
        with state._connection() as connection:
            row = connection.execute("SELECT owned_body FROM paper_research_admissions WHERE command_id=?", (str(job_id),)).fetchone()
        if row is None:
            raise PermissionError("paper research job is not registered to this account")
        command = OwnedRunPaperPortfolioResearch.model_validate_json(row[0])
        if command.account_id != account_id or command.owner_id != owner_id:
            raise PermissionError("paper research job belongs to another account or owner")
        self.backend.validate(command)
        authority = self.reader.get_artifact_preview_authority(job_id)
        if authority is None or authority.job.updated_at > as_of:
            return None
        if authority.job.spec != command.spec or authority.job.spec_hash != command.spec.spec_hash:
            raise ValueError("paper sealed job differs from original accepted spec")
        parameters = PaperResearchRunParameters.model_validate({item.name: item.value for item in command.spec.parameters.arguments})
        names = (*type(parameters).model_fields, "task_name", "result_hash", "source_hash", "configuration", "metadata_identity", "available_at", "complete")
        reference = self.artifact_reader.preview(job_id, table_name="paper_reference", row_limit=1, column_limit=len(names))
        table = reference.table
        if (table is None or table.columns != names or table.total_rows != 1 or table.total_columns != len(names)
                or table.rows_truncated or table.columns_truncated or len(table.rows) != 1):
            raise ValueError("paper sealed reference has a different complete schema")
        values = dict(zip(names, table.rows[0], strict=True))
        if (PaperResearchRunParameters.model_validate({key: values[key] for key in type(parameters).model_fields}) != parameters
                or values["task_name"] != command.task_name or values["complete"] is not True
                or values["configuration"] != command.catalog.configuration.model_dump_json()
                or values["metadata_identity"] != command.metadata_identity.model_dump_json()
                or values["available_at"] != command.accepted_at.isoformat()):
            raise ValueError("paper sealed reference differs from original account, source or version")
        content = self.artifact_reader.preview(job_id, table_name="paper_result", row_limit=1, column_limit=2)
        table = content.table
        if (table is None or table.columns != ("result_hash", "payload") or table.total_rows != 1 or table.total_columns != 2
                or table.rows_truncated or table.columns_truncated or len(table.rows) != 1
                or (content.spec_hash, content.manifest_hash, content.complete_result_hash) != (
                command.spec.spec_hash, reference.manifest_hash, reference.complete_result_hash)):
            raise ValueError("paper complete sealed result differs from original reference graph")
        result_type = PaperReconcileResult if command.task_name == "paper_reconcile" else PaperBacktestBandResult
        result = result_type.model_validate_json(table.rows[0][1])
        if (result.fingerprint, result.configuration_fingerprint, result.input_hash, table.rows[0][0]) != (
                values["result_hash"], command.configuration_fingerprint, values["source_hash"], values["result_hash"]):
            raise ValueError("paper sealed result body differs from its bound complete source")
        return PaperResearchSealedAnalysis(task_name=command.task_name, job_id=job_id, account_id=account_id,
                                           configuration_fingerprint=command.configuration_fingerprint, configuration_version=parameters.configuration_version,
                                           spec_hash=command.spec.spec_hash, manifest_hash=content.manifest_hash, complete_result_hash=content.complete_result_hash,
                                           result_hash=result.fingerprint, completed_at=authority.job.updated_at,
                                           reconcile=result if isinstance(result, PaperReconcileResult) else None,
                                           band=result if isinstance(result, PaperBacktestBandResult) else None)
