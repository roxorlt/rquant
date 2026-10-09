"""Original owner and physical gates for complete minute HTML and ZIP exports."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Self
from uuid import UUID

from pydantic import model_validator

from rquant.lab_artifact_export import LabJobZipExportFacade, LabJobZipExportReceipt
from rquant.lab_artifacts import LabArtifactIntegrityError, LabJobArtifactStore, _secure_open_directory
from rquant.minute_backtest_artifact import MINUTE_RESULT_TABLE_NAMES, MinuteSealedReplayResult
from rquant.minute_backtest_contracts import MinuteReplayModel, Sha256
from rquant.minute_backtest_installation import InstalledMinuteReplay
from rquant.minute_backtest_parameter_artifact import MinuteParameterSealedReplayResult
from rquant.minute_backtest_parameter_adapter import MinuteParameterFormalReplayAdapter
from rquant.minute_backtest_parameter_producer import MinuteParameterPreparedPublication
from rquant.minute_backtest_parameter_definition import minute_parameter_validation_request
from rquant.minute_backtest_report import MinuteHtmlReport, build_minute_html_report, minute_artifact_fact
from rquant.page_control import PageControlService
from rquant.portfolio_backtest_artifact import (
    _discard_interrupted_html_temporary, _pack_verified_html_zip,
    _read_verified_html_zip, _recover_verified_html_zip,
)
from rquant.portfolio_backtest_models import MAX_ZIP_BYTES
from rquant.sealed_result_ownership import (
    OriginalEffectFact, OriginalSubmissionFact, SealedArtifactFact, SealedJobFact, SealedOwnerBinding,
    bind_sealed_owner, require_owned_sealed_result,
)
from rquant.minute_experiment_result_owner import (
    MinuteExperimentProvenance, MinuteExperimentSealedOwnerProof, require_minute_experiment_owner,
)
from rquant.web.collaboration_gateway import CollaborationGateway
from rquant.web.models.collaboration import ResultOwnerProof

if TYPE_CHECKING:
    from rquant.minute_backtest_native_experiment_reader import MinuteNativeExperimentReplayReader


class MinuteVerifiedReport(MinuteReplayModel):
    sealed: MinuteSealedReplayResult | MinuteParameterSealedReplayResult
    owner: SealedOwnerBinding | MinuteExperimentSealedOwnerProof
    report: MinuteHtmlReport

    @model_validator(mode="after")
    def exact_complete_binding(self) -> Self:
        if type(self.owner) is MinuteExperimentSealedOwnerProof:
            require_minute_experiment_owner(self.owner, requester=self.sealed.owner_id,
                current_artifact=minute_artifact_fact(self.sealed))
        else:
            require_owned_sealed_result(self.owner, requester=self.sealed.owner_id,
                current_artifact=minute_artifact_fact(self.sealed))
        if (self.report.job_id, self.report.result_hash, self.report.full_input_hash,
            self.report.core_input_hash, self.report.seed_hash, self.report.owner_binding_hash) != (
            self.sealed.job_id, self.sealed.complete_result_hash, self.sealed.full_input_hash,
            self.sealed.core_input_hash, self.sealed.seed_hash, self.owner.content_sha256):
            raise ValueError("minute report differs from its complete original result or owner")
        return self


class MinuteReportReader:
    def __init__(self, installation: InstalledMinuteReplay | None, *,
        owner_authority: PageControlService | CollaborationGateway,
        native_reader: MinuteNativeExperimentReplayReader | None = None) -> None:
        from rquant.minute_backtest_native_experiment_reader import MinuteNativeExperimentReplayReader

        if type(owner_authority) not in (PageControlService, CollaborationGateway) or not (
            type(installation) is InstalledMinuteReplay and native_reader is None
            or installation is None and type(native_reader) is MinuteNativeExperimentReplayReader
        ):
            raise TypeError("original installed minute and current owner authorities required")
        from rquant.web.minute_backtest_service import MinuteWebService

        self.installation, self.owner_authority = installation, owner_authority
        self.native_reader = native_reader
        self.reader = installation.reader if installation is not None else native_reader.reader
        self.service = MinuteWebService(installation) if installation is not None else None

    def _owner(self, job_id: UUID, *, owner_id: str, spec_hash: str,
        artifact: SealedArtifactFact | None = None,
    ) -> ResultOwnerProof | MinuteExperimentProvenance | MinuteExperimentSealedOwnerProof:
        if type(self.owner_authority) is CollaborationGateway:
            return self.owner_authority.minute_report_owner(owner_id, job_id=str(job_id), spec_hash=spec_hash,
                artifact=artifact)
        from rquant.web.models.collaboration import MinuteReportOwnerQuery
        return self.owner_authority._trusted_minute_report_owner(
            MinuteReportOwnerQuery(job_id=str(job_id), spec_hash=spec_hash, artifact=artifact),
            authenticated_actor_id=owner_id)

    @staticmethod
    def _binding(sealed: MinuteSealedReplayResult, proof: ResultOwnerProof) -> SealedOwnerBinding:
        artifact = minute_artifact_fact(sealed)
        if (proof.domain, proof.job_id, proof.spec_hash, proof.owner_id) != (
            "minute", str(sealed.job_id), sealed.spec_hash, sealed.owner_id):
            raise PermissionError("minute original owner proof differs")
        kind = "submit_minute_replay"
        binding = bind_sealed_owner(
            OriginalSubmissionFact(domain="minute", command_id=proof.command_id, command_kind=kind,
                command_sha256=proof.command_sha256, actor_id=proof.owner_id, job_id=str(sealed.job_id),
                spec_hash=sealed.spec_hash, origin_verified=True),
            OriginalEffectFact(command_id=proof.command_id, command_kind=kind,
                command_sha256=proof.command_sha256, status="succeeded", submitted_job_id=str(sealed.job_id),
                submitted_spec_hash=sealed.spec_hash, worker_owner_id=proof.worker_owner_id),
            SealedJobFact(domain="minute", job_id=str(sealed.job_id), spec_hash=sealed.spec_hash,
                status="succeeded", manifest_hash=sealed.manifest_hash,
                complete_result_hash=sealed.complete_result_hash), artifact)
        if binding is None:
            raise PermissionError("minute original actor has no sealed owner binding")
        return binding

    @minute_parameter_validation_request
    def read(self, job_id: UUID, *, owner_id: str, expected_result_hash: str) -> MinuteVerifiedReport:
        selected = (self.service.job(job_id, owner_id=owner_id) if self.service is not None
            else self.native_reader.job(job_id, owner_id=owner_id))
        proof = self._owner(job_id, owner_id=owner_id, spec_hash=selected.spec_hash)
        if self.service is not None:
            sealed = self.service.read_result(job_id, owner_id=owner_id, result_hash=expected_result_hash)
        else:
            from rquant.lab_artifact_preview import ArtifactPreviewUnavailableError

            if type(proof) is not MinuteExperimentProvenance:
                raise PermissionError("native report requires its independent original family provenance")
            sealed = self.native_reader.read(job_id, owner_id=owner_id,
                expected_spec_hash=selected.spec_hash, expected_result_hash=expected_result_hash,
                as_of=self.native_reader.clock())
            if sealed is None:
                raise ArtifactPreviewUnavailableError("native original finalizer seal is unavailable")
        if type(proof) is ResultOwnerProof:
            owner = self._binding(sealed, proof)
        elif type(proof) is MinuteExperimentProvenance:
            owner = self._owner(job_id, owner_id=owner_id, spec_hash=sealed.spec_hash,
                artifact=minute_artifact_fact(sealed))
            if type(owner) is not MinuteExperimentSealedOwnerProof or owner.provenance != proof:
                raise PermissionError("original family provenance changed before complete sealed binding")
            require_minute_experiment_owner(owner, requester=owner_id, current_artifact=minute_artifact_fact(sealed))
        else:
            raise PermissionError("initial minute report read requires its original provenance")
        nature = None
        if type(sealed) is MinuteParameterSealedReplayResult:
            if self.installation is None:
                raise PermissionError("native family report cannot use a parameter result")
            catalog = self.installation.profile.parameter_catalog
            context = self.installation.reader.get_command_context(job_id)
            if catalog is None or context is None:
                raise PermissionError("parameter report lacks its original catalog or job")
            adapter = MinuteParameterFormalReplayAdapter(catalog)
            parameters = adapter.parameters(context.job.spec)
            if parameters.prepared_publication_json is None:
                raise PermissionError("parameter report lacks its complete baseline publication")
            prepared = MinuteParameterPreparedPublication.model_validate_json(parameters.prepared_publication_json)
            if adapter.expected(parameters) != sealed.result.publication:
                raise PermissionError("parameter report full source changed")
            references = tuple(item for item in catalog.fact_sources if item.fact_identity == prepared.baseline)
            if len(references) != 1:
                raise PermissionError("parameter report lacks its exact installed baseline")
            nature = references[0].source_nature
        report = build_minute_html_report(sealed, owner=owner, requester=owner_id, parameter_source_nature=nature)
        if self._owner(job_id, owner_id=owner_id, spec_hash=sealed.spec_hash) != proof:
            raise PermissionError("minute original owner or current role changed during report read")
        if self.installation is not None:
            self.installation.verify_current()
        elif self.native_reader.job(job_id, owner_id=owner_id) != selected:
            raise PermissionError("native original job changed during report read")
        return MinuteVerifiedReport(sealed=sealed, owner=owner, report=report)


class MinuteZipReceipt(LabJobZipExportReceipt):
    result_hash: Sha256
    full_input_hash: Sha256
    core_input_hash: Sha256
    seed_hash: Sha256
    html_sha256: Sha256
    owner_binding_hash: Sha256


class MinuteZipExportFacade(LabJobZipExportFacade):
    def __init__(self, *, report_reader: MinuteReportReader, original_exports: LabJobZipExportFacade | None = None,
        read_only: bool = False, max_zip_bytes: int = MAX_ZIP_BYTES, **kwargs: object) -> None:
        if type(report_reader) is not MinuteReportReader or (
            type(original_exports) is not LabJobZipExportFacade and not (read_only and original_exports is None)):
            raise TypeError("original complete minute report and Lab export authorities required")
        if type(max_zip_bytes) is not int or not 1 <= max_zip_bytes <= MAX_ZIP_BYTES:
            raise ValueError("minute ZIP budget is invalid")
        if type(read_only) is not bool:
            raise ValueError("minute export mode must be exact")
        if read_only:
            if kwargs.get("artifact_store") is not None:
                raise TypeError("minute read-only export cannot hold a writer artifact store")
            descriptor = _secure_open_directory(kwargs["export_root"], create=False)
            os.close(descriptor)
        super().__init__(**kwargs)
        if self.reader is not report_reader.reader or (
            original_exports is not None and self.reader is not original_exports.reader):
            raise ValueError("minute export readers must use the same original installed ledger")
        if original_exports is not None and self.artifact_store is not original_exports.artifact_store:
            raise ValueError("minute export stores must share the original physical artifact authority")
        self.report_reader, self.original_exports, self.max_zip_bytes, self.read_only = report_reader, original_exports, max_zip_bytes, read_only

    @staticmethod
    def _receipt(receipt: LabJobZipExportReceipt, result: MinuteVerifiedReport) -> MinuteZipReceipt:
        return MinuteZipReceipt(**receipt.model_dump(mode="python"), result_hash=result.sealed.complete_result_hash,
            full_input_hash=result.sealed.full_input_hash, core_input_hash=result.sealed.core_input_hash,
            seed_hash=result.sealed.seed_hash, html_sha256=result.report.html_sha256,
            owner_binding_hash=result.owner.content_sha256)

    def _unchanged(self, before: MinuteVerifiedReport, *, owner_id: str) -> None:
        if self.report_reader.read(before.sealed.job_id, owner_id=owner_id,
            expected_result_hash=before.sealed.complete_result_hash) != before:
            raise LabArtifactIntegrityError("minute complete result or original owner changed during export")

    @minute_parameter_validation_request
    def export_minute(self, job_id: UUID, *, owner_id: str, expected_result_hash: str,
        request_id: UUID | None = None) -> MinuteZipReceipt:
        if self.read_only or self.original_exports is None:
            raise PermissionError("minute read-only exports cannot publish or repair an archive")
        if request_id is not None:
            recovered = self.recover_minute(job_id, owner_id=owner_id, request_id=request_id,
                expected_result_hash=expected_result_hash)
            if recovered is not None:
                return recovered
        result = self.report_reader.read(job_id, owner_id=owner_id, expected_result_hash=expected_result_hash)
        receipt = _pack_verified_html_zip(facade=self, original_exports=self.original_exports,
            job_id=job_id, request_id=request_id, html=result.report.html_bytes(), table_names=MINUTE_RESULT_TABLE_NAMES,
            max_zip_bytes=self.max_zip_bytes,
            discard_temporary=lambda descriptor: _discard_interrupted_html_temporary(descriptor,
                max_zip_bytes=self.max_zip_bytes),
            validate_unchanged=lambda: self._unchanged(result, owner_id=owner_id))
        return self._receipt(receipt, result)

    @minute_parameter_validation_request
    def recover_minute(self, job_id: UUID, *, owner_id: str, request_id: UUID,
        expected_result_hash: str) -> MinuteZipReceipt | None:
        result = self.report_reader.read(job_id, owner_id=owner_id, expected_result_hash=expected_result_hash)
        receipt = _recover_verified_html_zip(facade=self, job_id=job_id, request_id=request_id,
            result_hash=result.sealed.complete_result_hash, html=result.report.html_bytes(), max_zip_bytes=self.max_zip_bytes,
            validate_unchanged=lambda: self._unchanged(result, owner_id=owner_id),
            expected_bound_hashes=LabJobArtifactStore._expected_bound_hashes(result.sealed.manifest) if self.read_only else None)
        return None if receipt is None else self._receipt(receipt, result)

    @minute_parameter_validation_request
    def read_bytes(self, receipt: MinuteZipReceipt, *, owner_id: str) -> bytes:
        if type(receipt) is not MinuteZipReceipt:
            raise TypeError("exact minute ZIP receipt required")
        checked = self._build_receipt(request_id=receipt.request_id, job_id=receipt.job_id, path=receipt.path)
        result = self.report_reader.read(receipt.job_id, owner_id=owner_id, expected_result_hash=receipt.result_hash)
        if self._receipt(checked, result) != receipt or receipt.byte_size > self.max_zip_bytes:
            raise LabArtifactIntegrityError("minute ZIP receipt or complete result binding differs")
        payload = _read_verified_html_zip(facade=self, receipt=receipt, max_zip_bytes=self.max_zip_bytes)
        self._unchanged(result, owner_id=owner_id)
        return payload
