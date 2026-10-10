"""Current installed Lab composition for native experiment reports and ZIPs."""

from __future__ import annotations

import os
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Self
from uuid import UUID

from pydantic import Field, JsonValue, field_validator, model_validator

from rquant.definition_registry import ImmutableDefinitionRegistry
from rquant.experiment_platform_projection import ExperimentPrivateResultAuthority
from rquant.experiment_registry import ExperimentRegistryReadonlyReader
from rquant.job_center_authority import (
    JobCenterAuthorityDeploymentBinding,
    JobCenterAuthorityManifest,
    resolve_current_job_center_authority_binding,
)
from rquant.lab_artifact_export import LabJobZipExportFacade
from rquant.lab_artifact_preview import ArtifactPreviewReader
from rquant.lab_artifacts import LabJobArtifactStore, _ensure_private_directory
from rquant.lab_daemon import load_lab_job_center_authority_manifest
from rquant.lab_job_center import LabCommandSubmissionFacade
from rquant.lab_job_protocol import LabCommandSpool
from rquant.lab_jobs import LabJobReader
from rquant.minute_backtest_commands import (
    ExportMinuteReplayZip,
    build_minute_zip_effect,
    parse_minute_zip_effect,
    require_minute_zip_report,
)
from rquant.minute_backtest_contracts import MinuteReplayModel
from rquant.minute_backtest_export import MinuteReportReader, MinuteZipExportFacade
from rquant.minute_backtest_native_experiment_reader import MinuteNativeExperimentReplayReader
from rquant.minute_backtest_producer import (
    MinutePrivateFileReference,
    _secure_private_bytes,
    _strict_json,
)
from rquant.minute_backtest_publication_contracts import MAX_MINUTE_CONTROL_BYTES
from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry
from rquant.strict_json import canonical_json_bytes

if TYPE_CHECKING:
    from rquant.page_control import PageControlService


class MinuteNativeReportLocator(MinuteReplayModel):
    contract: Literal["minute-native-report-runtime/v1"] = "minute-native-report-runtime/v1"
    code_sha: str = Field(pattern=r"^[0-9a-f]{40}$")
    runtime_deployment_root: Path
    runtime_root: Path
    lab_jobs_path: Path
    command_spool_path: Path
    final_artifact_root: Path

    @field_validator(
        "runtime_deployment_root",
        "runtime_root",
        "lab_jobs_path",
        "command_spool_path",
        "final_artifact_root",
    )
    @classmethod
    def normalized_path(cls, value: Path) -> Path:
        if not value.is_absolute() or value != Path(os.path.abspath(value)):
            raise ValueError("native report locator requires absolute normalized paths")
        return value

    @model_validator(mode="after")
    def bounded(self) -> Self:
        if len(canonical_json_bytes(self.model_dump(mode="json"))) > MAX_MINUTE_CONTROL_BYTES:
            raise ValueError("native report locator exceeds the original control budget")
        return self


def _current(
    locator: MinuteNativeReportLocator,
) -> tuple[JobCenterAuthorityDeploymentBinding, JobCenterAuthorityManifest]:
    binding = resolve_current_job_center_authority_binding(
        locator.runtime_deployment_root,
        expected_code_sha=locator.code_sha,
        runtime_root=locator.runtime_root,
        lab_jobs_path=locator.lab_jobs_path,
        command_spool_path=locator.command_spool_path,
        final_artifact_root=locator.final_artifact_root,
    )
    authority = load_lab_job_center_authority_manifest(
        binding.runtime_root / "job-center-authority.json",
        expected_code_sha=locator.code_sha,
        expected_research_root=binding.runtime_root,
        expected_lab_jobs_path=binding.lab_jobs_path,
        expected_command_spool_path=binding.command_spool_path,
        expected_final_artifact_root=binding.final_artifact_root,
        expected_runtime_deployment_root=binding.runtime_deployment_root,
        expected_deployment_profile_id=binding.deployment_profile_id,
        expected_deployment_generation_hash=binding.deployment_generation_hash,
    )
    if (
        type(binding) is not JobCenterAuthorityDeploymentBinding
        or type(authority) is not JobCenterAuthorityManifest
    ):
        raise TypeError("native reports require the original current binding and complete manifest")
    for name in (
        "code_sha",
        "runtime_deployment_root",
        "runtime_root",
        "lab_jobs_path",
        "command_spool_path",
        "final_artifact_root",
    ):
        if getattr(binding, name) != getattr(locator, name):
            raise PermissionError("native report locator differs from the original current binding")
    for name in type(binding).model_fields:
        if name not in {"runtime_mode", "lab_highwater"} and getattr(authority, name) != getattr(
            binding, name
        ):
            raise PermissionError(
                "native report manifest differs from the complete current binding"
            )
    return binding, authority


def _locator_bytes(data: bytes) -> MinuteNativeReportLocator:
    if len(data) > MAX_MINUTE_CONTROL_BYTES:
        raise PermissionError("native report locator exceeds the original control budget")
    _strict_json(data)
    return MinuteNativeReportLocator.model_validate_json(data)


def _reject_read_mutation() -> None:
    raise PermissionError("native report GET cannot initialize or repair original spool state")


class MinuteNativeReportRuntime:
    def __init__(
        self,
        locator: MinuteNativeReportLocator,
        reference: MinutePrivateFileReference,
        authority: JobCenterAuthorityManifest,
        *,
        writable: bool,
        clock: Callable[[], datetime],
    ) -> None:
        if (
            type(locator) is not MinuteNativeReportLocator
            or type(reference) is not MinutePrivateFileReference
            or type(authority) is not JobCenterAuthorityManifest
            or type(writable) is not bool
            or not callable(clock)
        ):
            raise TypeError("exact native report locator, file, manifest and mode are required")
        self.locator = MinuteNativeReportLocator.model_validate(locator.model_dump(mode="python"))
        self.reference = MinutePrivateFileReference.model_validate(
            reference.model_dump(mode="python")
        )
        self.authority = JobCenterAuthorityManifest.model_validate(
            authority.model_dump(mode="python")
        )
        self.writable, self.clock, self._closed = writable, clock, False
        data, _ = _secure_private_bytes(self.reference.path, self.reference)
        if _locator_bytes(data) != self.locator:
            raise PermissionError("native report locator differs from its original private file")
        self._binding, current = _current(self.locator)
        if current != self.authority:
            raise PermissionError("native report complete authority changed before construction")
        self.reader = LabJobReader(self.authority.lab_jobs_path)
        self.artifact_store: LabJobArtifactStore | None = None
        try:
            if writable:
                # Establish writer-owned namespaces before any readonly registry captures this root.
                for path in (
                    self.export_root.parent,
                    self.export_root.parent / "minute-base",
                    self.export_root,
                ):
                    _ensure_private_directory(
                        path, manage_existing=False, require_private_existing=True
                    )
                self.artifact_store = LabJobArtifactStore(
                    self.authority.final_artifact_root,
                    mutation_guard=self.verify_current,
                )
            self.verify_current()
        except BaseException as error:
            try:
                self.close()
            except BaseException as cleanup:
                raise BaseExceptionGroup(
                    "native report construction and cleanup failed", [error, cleanup]
                ) from error
            raise

    @property
    def export_root(self) -> Path:
        return self.authority.runtime_root / "exports" / "minute-reports"

    def verify_current(self) -> None:
        if self._closed:
            raise PermissionError("native report runtime is closed")
        data, _ = _secure_private_bytes(self.reference.path, self.reference)
        if _locator_bytes(data) != self.locator:
            raise PermissionError("native report locator changed")
        binding, authority = _current(self.locator)
        if binding != self._binding or authority != self.authority:
            raise PermissionError("native report current binding or complete manifest changed")

    def replay_reader(self, job_id: UUID) -> MinuteNativeExperimentReplayReader:
        self.verify_current()
        if type(job_id) is not UUID:
            raise TypeError("native report requires its exact original job UUID")
        if self.reader.get_artifact_preview_authority(job_id) is None:
            raise LookupError("native original succeeded sealed Lab job is unavailable")
        # SQLite's original readonly preflight precedes the fresh physical root capture.
        spool = LabCommandSpool(
            self.authority.command_spool_path,
            mutation_guard=None if self.writable else _reject_read_mutation,
        )
        registry = ExperimentRegistryReadonlyReader(
            self.authority.experiment_registry_path,
            managed_trust_root=self.authority.runtime_root,
        )
        definitions = ImmutableDefinitionRegistry(
            self.authority.definition_registry_root,
            execution_registry=BuiltinStrategyEvaluatorRegistry(
                producer_commit=self.locator.code_sha,
            ).trusted_executable_registry(),
        )
        facade = LabCommandSubmissionFacade(
            reader=self.reader,
            spool=spool,
            experiment_registry=registry,
            definition_registry=definitions,
            clock=self.clock,
        )
        port = MinuteNativeExperimentReplayReader(
            reader=self.reader,
            artifact_reader=ArtifactPreviewReader(
                reader=self.reader,
                artifact_root=self.authority.final_artifact_root,
            ),
            submission_facade=facade,
            private_authority=ExperimentPrivateResultAuthority(registry),
            clock=self.clock,
        )
        self.verify_current()
        return port

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            if self.artifact_store is not None:
                self.artifact_store.close()


def load_minute_native_report_runtime(
    path: Path,
    *,
    expected_code_sha: str,
    clock: Callable[[], datetime],
    writable: bool = False,
) -> MinuteNativeReportRuntime:
    data, reference = _secure_private_bytes(path)
    locator = _locator_bytes(data)
    if locator.code_sha != expected_code_sha:
        raise PermissionError("native report expected code differs from its original locator")
    _, authority = _current(locator)
    return MinuteNativeReportRuntime(locator, reference, authority, writable=writable, clock=clock)


class MinuteNativeReportCommandWriter:
    def __init__(self, runtime: MinuteNativeReportRuntime) -> None:
        if type(runtime) is not MinuteNativeReportRuntime or not runtime.writable:
            raise TypeError("native report writer requires its original writable runtime")
        runtime.verify_current()
        self.runtime = runtime
        self.owner_authority: PageControlService | None = None

    def bind_owner_authority(self, authority: PageControlService) -> None:
        from rquant.page_control import PageControlService

        if (
            type(authority) is not PageControlService
            or getattr(authority.consumer, "minute_native_report_backend", None) is not self
        ):
            raise TypeError("native report writer requires the same original PageControl consumer")
        if self.owner_authority is not None and self.owner_authority is not authority:
            raise PermissionError("native report original owner authority cannot be rebound")
        self.owner_authority = authority

    def _require_owner(self, command: ExportMinuteReplayZip) -> None:
        from rquant.page_control import PageControlEffectStatus, _command_hash

        if type(command) is not ExportMinuteReplayZip:
            raise TypeError("native report writer only accepts original ZIP commands")
        ExportMinuteReplayZip.model_validate(command.model_dump(mode="python"))
        authority = self.owner_authority
        if (
            authority is None
            or authority.collaboration.mode != "enforced"
            or getattr(authority.consumer, "minute_native_report_backend", None) is not self
        ):
            raise PermissionError("native report export requires its original enforced owner")
        command_hash = _command_hash(command)
        actor = authority.outbox.trusted_command_actor(command.command_id, command_hash)
        if actor != command.actor_id:
            raise PermissionError("native report export lacks its original authenticated actor")
        authority.collaboration.require_command(actor, command.kind)
        effect = authority.outbox.effect(command.command_id)
        if (
            effect is None
            or effect.status is not PageControlEffectStatus.STARTED
            or effect.command_hash != command_hash
            or effect.effect_kind != command.kind
            or effect.owner_id != authority.consumer.consumer_id
        ):
            raise PermissionError("native report export lacks its original active effect")
        authority.outbox.require_active_claim(
            command,
            owner_id=effect.owner_id,
            claim_token=effect.claim_token,
        )
        self.runtime.verify_current()

    def _exports(self, command: ExportMinuteReplayZip) -> MinuteZipExportFacade:
        self._require_owner(command)
        store = self.runtime.artifact_store
        if store is None:
            raise PermissionError("native report export has no original writer artifact store")
        native = self.runtime.replay_reader(command.job_id)
        report_reader = MinuteReportReader(
            None, owner_authority=self.owner_authority, native_reader=native
        )
        original = LabJobZipExportFacade(
            reader=self.runtime.reader,
            artifact_store=store,
            export_root=self.runtime.export_root.parent / "minute-base",
        )
        return MinuteZipExportFacade(
            reader=self.runtime.reader,
            artifact_store=store,
            export_root=self.runtime.export_root,
            report_reader=report_reader,
            original_exports=original,
        )

    def freeze(self, command: ExportMinuteReplayZip) -> JsonValue:
        result = self._exports(command).report_reader.read(
            command.job_id,
            owner_id=command.actor_id,
            expected_result_hash=command.result_hash,
        )
        self._require_owner(command)
        return build_minute_zip_effect(command, result).model_dump(mode="json")

    def submit(self, command: ExportMinuteReplayZip, marker: JsonValue) -> JsonValue:
        self._require_owner(command)
        checked = parse_minute_zip_effect(command, marker)
        exports = self._exports(command)
        report = exports.report_reader.read(
            command.job_id,
            owner_id=command.actor_id,
            expected_result_hash=command.result_hash,
        )
        require_minute_zip_report(checked, report)
        self._require_owner(command)
        receipt = exports.export_minute(
            checked.job_id,
            owner_id=command.actor_id,
            request_id=checked.request_id,
            expected_result_hash=checked.result_hash,
        )
        self._require_owner(command)
        return receipt.model_dump(mode="json")

    def recover(self, command: ExportMinuteReplayZip, marker: JsonValue) -> JsonValue | None:
        self._require_owner(command)
        checked = parse_minute_zip_effect(command, marker)
        exports = self._exports(command)
        report = exports.report_reader.read(
            command.job_id,
            owner_id=command.actor_id,
            expected_result_hash=command.result_hash,
        )
        require_minute_zip_report(checked, report)
        self._require_owner(command)
        receipt = exports.recover_minute(
            checked.job_id,
            owner_id=command.actor_id,
            request_id=checked.request_id,
            expected_result_hash=checked.result_hash,
        )
        self._require_owner(command)
        return None if receipt is None else receipt.model_dump(mode="json")

    def close(self) -> None:
        self.runtime.close()
