"""Writer-owned Lab mutations behind the page-control service boundary."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID

from pydantic import JsonValue

from rquant.definition_registry import ImmutableDefinitionRegistry
from rquant.lab_artifact_export import LabJobZipExportFacade, LabJobZipExportReceipt
from rquant.lab_artifacts import LabJobArtifactStore
from rquant.lab_daemon import LabJobCenterAuthorityManifest
from rquant.lab_job_center import CommandSubmissionResult, LabCommandSubmissionFacade
from rquant.lab_job_protocol import (
    CancelJobCommand,
    LabCommand,
    LabCommandSpool,
    PauseJobCommand,
    ResumeJobCommand,
    RetryJobCommand,
    SubmitJobCommand,
)
from rquant.lab_jobs import LabJobReader
from rquant.page_control import DiscardLabArtifactZip
from rquant.runtime_artifact_terminal_lifecycle import (
    build_production_artifact_terminal_lifecycle,
)
from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry


class _CommandWriter(Protocol):
    def submit_create(
        self,
        command: SubmitJobCommand,
        **kwargs: object,
    ) -> CommandSubmissionResult: ...

    def submit_pause(self, job_id: UUID, **kwargs: object) -> CommandSubmissionResult: ...

    def submit_resume(self, job_id: UUID, **kwargs: object) -> CommandSubmissionResult: ...

    def submit_cancel(self, job_id: UUID, **kwargs: object) -> CommandSubmissionResult: ...

    def submit_retry(self, job_id: UUID, **kwargs: object) -> CommandSubmissionResult: ...


class _ZipWriter(Protocol):
    def export(self, job_id: UUID) -> LabJobZipExportReceipt: ...

    def discard(self, receipt: LabJobZipExportReceipt) -> None: ...


class LabPageControlWriter:
    """Own command publication and request-scoped ZIP mutation for Lab pages."""

    def __init__(self, *, commands: _CommandWriter, zip_exports: _ZipWriter) -> None:
        self.commands = commands
        self.zip_exports = zip_exports

    def submit_command(
        self,
        command: LabCommand,
        *,
        interaction_key: str | None,
    ) -> JsonValue:
        if isinstance(command, SubmitJobCommand):
            result = self.commands.submit_create(command, interaction_key=interaction_key)
        elif isinstance(command, PauseJobCommand):
            result = self.commands.submit_pause(
                command.job_id,
                expected_version=command.expected_version,
                reason=command.reason,
                interaction_key=interaction_key,
            )
        elif isinstance(command, ResumeJobCommand):
            result = self.commands.submit_resume(
                command.job_id,
                expected_version=command.expected_version,
                reason=command.reason,
                interaction_key=interaction_key,
            )
        elif isinstance(command, CancelJobCommand):
            result = self.commands.submit_cancel(
                command.job_id,
                expected_version=command.expected_version,
                reason=command.reason,
                interaction_key=interaction_key,
            )
        elif isinstance(command, RetryJobCommand):
            result = self.commands.submit_retry(
                command.job_id,
                expected_version=command.expected_version,
                reason=command.reason,
                interaction_key=interaction_key,
            )
        else:  # pragma: no cover - discriminated protocol is exhaustive
            raise TypeError(f"unsupported Lab command: {type(command).__name__}")
        return result.model_dump(mode="json")

    def export_zip(self, job_id: UUID) -> JsonValue:
        receipt = self.zip_exports.export(job_id)
        return receipt.model_dump(mode="json")

    def discard_zip(self, command: DiscardLabArtifactZip) -> JsonValue:
        self.zip_exports.discard(
            LabJobZipExportReceipt(
                request_id=command.request_id,
                job_id=command.job_id,
                path=command.path,
                byte_size=command.byte_size,
                sha256=command.sha256,
            )
        )
        return {"discarded": True}


def build_lab_page_control_writer(
    manifest: LabJobCenterAuthorityManifest,
    *,
    clock: Callable[[], datetime] | None = None,
) -> LabPageControlWriter:
    """Bind all Lab writers only inside the control-service process."""

    authority = LabJobCenterAuthorityManifest.model_validate(manifest)
    authority_clock = clock or (lambda: datetime.now(UTC))
    export_root = authority.research_root / "exports"
    export_root.mkdir(parents=True, mode=0o700, exist_ok=True)
    export_root.chmod(0o700)
    reader = LabJobReader(authority.lab_jobs_path)
    terminal_lifecycle = build_production_artifact_terminal_lifecycle(
        runtime_root=authority.runtime_deployment_root,
        experiment_registry_path=authority.experiment_registry_path,
    )
    experiments = terminal_lifecycle.experiment_registry
    definitions = ImmutableDefinitionRegistry(
        authority.definition_registry_root,
        execution_registry=BuiltinStrategyEvaluatorRegistry(
            producer_commit=authority.code_sha
        ).trusted_executable_registry(),
    )
    commands = LabCommandSubmissionFacade(
        reader=reader,
        spool=LabCommandSpool(authority.command_spool_path),
        experiment_registry=experiments,
        definition_registry=definitions,
        clock=authority_clock,
    )
    artifacts = LabJobArtifactStore(authority.final_artifact_root)
    return LabPageControlWriter(
        commands=commands,
        zip_exports=LabJobZipExportFacade(
            reader=reader,
            artifact_store=artifacts,
            export_root=export_root,
        ),
    )


__all__ = ["LabPageControlWriter", "build_lab_page_control_writer"]
