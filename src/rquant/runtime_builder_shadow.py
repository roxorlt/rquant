"""Production composition for the isolated post-close Shadow comparison service."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Literal, Protocol

from pydantic import Field, field_validator, model_validator

from rquant.legacy_shadow_export import (
    Ed25519LegacyShadowRecoveryKeyring,
    LegacyShadowExportUnavailableError,
    LegacyShadowFilesystemPolicy,
    LegacyShadowRecoveryVerifier,
    LegacyShadowRunnerManifestBinding,
    load_accepted_legacy_shadow_export,
)
from rquant.runtime_contracts import RuntimeContractModel, normalize_aware_utc
from rquant.runtime_market_session import load_market_calendar_authority
from rquant.runtime_service_control import RuntimeServicePlane, RuntimeStepResult
from rquant.runtime_service_entrypoint import (
    RuntimeServiceBuilder,
    RuntimeServiceKind,
    RuntimeServiceManifest,
    RuntimeServiceStep,
)
from rquant.runtime_shadow_job import ShadowInputUnavailableError, run_shadow_production_session
from rquant.runtime_shadow_sources import ShadowRunnerSignalSource
from rquant.runtime_shadow_validation import (
    Ed25519CompletionAttestationKeyring,
    Ed25519ShadowReceiptKeyring,
    Ed25519ShadowReceiptSigner,
    SecureShadowSigningClient,
    ShadowCalendarSelection,
    ShadowSessionReport,
    ShadowSourceCompletionReceipt,
    ShadowStrategyBinding,
    verify_completion_attestation,
)
from rquant.signal_router_runtime import RunnerSignalBatch


class ShadowSessionSettings(RuntimeContractModel):
    report_root: Path
    legacy_monitor_root: Path
    legacy_surge_root: Path
    isolated_runner_root: Path
    calendar_path: Path
    calendar_expected_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    calendar_content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    completion_active_key_id: str = Field(min_length=1)
    completion_active_public_key_pem: str = Field(min_length=1, max_length=16_384)
    completion_previous_public_key_pems: Mapping[str, str] = Field(default_factory=dict)
    report_active_key_id: str = Field(min_length=1)
    report_active_public_key_pem: str = Field(min_length=1, max_length=16_384)
    report_previous_public_key_pems: Mapping[str, str] = Field(default_factory=dict)
    signer_command: tuple[str, ...]
    report_producer_service_id: str = Field(min_length=1)
    report_producer_instance_id: str = Field(min_length=1)
    signer_timeout_seconds: float = Field(ge=0.1, le=30.0)
    producer_version: str = Field(min_length=1)
    match_tolerance_microseconds: int = Field(ge=0, le=1_800_000_000)
    mode: Literal["shadow"] = "shadow"
    strategy_bindings: tuple[ShadowStrategyBinding, ...] = Field(min_length=2, max_length=2)
    runner_manifest_bindings: tuple[LegacyShadowRunnerManifestBinding, ...] = Field(
        min_length=2,
        max_length=2,
    )

    @field_validator(
        "report_root",
        "legacy_monitor_root",
        "legacy_surge_root",
        "isolated_runner_root",
        "calendar_path",
    )
    @classmethod
    def require_normalized_absolute_path(cls, value: Path) -> Path:
        if not value.is_absolute() or value != value.resolve(strict=False):
            raise ValueError("Shadow runtime paths must be absolute and normalized")
        return value

    @field_validator("signer_command")
    @classmethod
    def require_fixed_signer_command(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if value != (
            "/usr/bin/sudo",
            "-n",
            "/usr/local/libexec/rquant-shadow-report-signer",
        ):
            raise ValueError("Shadow runtime signer must use the fixed protected helper")
        return value

    @model_validator(mode="after")
    def require_legacy_comparison_bindings(self) -> ShadowSessionSettings:
        strategy_ids = {binding.strategy_id for binding in self.strategy_bindings}
        if strategy_ids != {"n_shape", "growth_board_surge"}:
            raise ValueError("Shadow runtime requires the legacy-comparable strategy bindings")
        runner_bindings = {
            binding.strategy_id: binding for binding in self.runner_manifest_bindings
        }
        if set(runner_bindings) != strategy_ids or len(runner_bindings) != 2:
            raise ValueError("Shadow runtime requires exactly two runner manifest bindings")
        for binding in self.strategy_bindings:
            runner = runner_bindings[binding.strategy_id]
            if (
                runner.strategy_version != binding.strategy_version
                or runner.strategy_registration_fingerprint != binding.definition_fingerprint
                or runner.executable_fingerprint != binding.executable_fingerprint
            ):
                raise ValueError("Shadow runner manifest differs from its strategy binding")
        if self.completion_active_key_id in self.completion_previous_public_key_pems:
            raise ValueError("Shadow completion active key cannot also be previous")
        if self.report_active_key_id in self.report_previous_public_key_pems:
            raise ValueError("Shadow report active key cannot also be previous")
        return self


class _FilesystemRunnerSource(ShadowRunnerSignalSource):
    def __init__(
        self,
        root: Path,
        *,
        strategy_id: str,
        expected_commit: str,
        expected_runner_binding: LegacyShadowRunnerManifestBinding,
        recovery_verifier: LegacyShadowRecoveryVerifier,
        filesystem_policy: LegacyShadowFilesystemPolicy,
    ) -> None:
        self._root = root / strategy_id
        self._strategy_id = strategy_id
        self._expected_commit = expected_commit
        self._expected_runner_binding = expected_runner_binding
        self._recovery_verifier = recovery_verifier
        self._filesystem_policy = filesystem_policy
        self.source_id = f"isolated-runner:{strategy_id}"

    def _accepted(self, trade_date: date):
        accepted = load_accepted_legacy_shadow_export(
            root=self._root,
            trade_date=trade_date,
            expected_source_id=None,
            expected_commit=self._expected_commit,
            recovery_verifier=self._recovery_verifier,
            filesystem_policy=self._filesystem_policy,
        )
        attestation = accepted.completion_receipt.completion_attestation
        if (
            accepted.manifest.runner_manifest_binding != self._expected_runner_binding
            or attestation is None
            or attestation.claims.strategy_id != self._strategy_id
        ):
            raise LegacyShadowExportUnavailableError(
                "isolated shadow export strategy binding is invalid"
            )
        return accepted

    def read_completion_receipt(
        self,
        *,
        trade_date: date,
    ) -> ShadowSourceCompletionReceipt:
        return self._accepted(trade_date).completion_receipt

    def read_completed_batch(
        self,
        *,
        trade_date: date,
        after_sequence: int,
        limit: int,
    ) -> RunnerSignalBatch:
        batch = self._accepted(trade_date).completed_batch
        if batch is None:
            raise ValueError("Shadow isolated runner export batch is unavailable")
        records = tuple(record for record in batch.records if record.sequence > after_sequence)[
            :limit
        ]
        return RunnerSignalBatch(
            snapshot=batch.snapshot,
            after_sequence=after_sequence,
            limit=limit,
            records=records,
        )


@dataclass(frozen=True)
class ShadowSessionInputs:
    monitor_rows: tuple[Mapping[str, object], ...]
    monitor_completion_receipt: ShadowSourceCompletionReceipt
    surge_events_path: Path
    surge_completion_receipt: ShadowSourceCompletionReceipt
    runner_sources: tuple[tuple[ShadowStrategyBinding, ShadowRunnerSignalSource], ...]


class ShadowSessionInputLoader(Protocol):
    def load(
        self,
        *,
        settings: ShadowSessionSettings,
        trade_date: date,
        expected_export_commit: str,
    ) -> ShadowSessionInputs: ...


class FilesystemShadowSessionInputLoader:
    """Read exported legacy evidence; it never opens old runtime state for writing."""

    def __init__(
        self,
        *,
        recovery_verifier: LegacyShadowRecoveryVerifier | None = None,
        filesystem_policy: LegacyShadowFilesystemPolicy | None = None,
    ) -> None:
        if (recovery_verifier is None) != (filesystem_policy is None):
            raise ValueError("Shadow filesystem test dependencies must be provided together")
        self._recovery_verifier = recovery_verifier
        self._filesystem_policy = filesystem_policy

    def load(
        self,
        *,
        settings: ShadowSessionSettings,
        trade_date: date,
        expected_export_commit: str,
    ) -> ShadowSessionInputs:
        verifier = self._recovery_verifier or Ed25519LegacyShadowRecoveryKeyring(
            active_key_id=settings.report_active_key_id,
            active_public_key=settings.report_active_public_key_pem.encode("utf-8"),
            previous_public_keys={
                key_id: value.encode("utf-8")
                for key_id, value in settings.report_previous_public_key_pems.items()
            },
        )
        filesystem_policy = self._filesystem_policy or LegacyShadowFilesystemPolicy(
            mode="linux-production"
        )
        try:
            monitor = load_accepted_legacy_shadow_export(
                root=settings.legacy_monitor_root,
                trade_date=trade_date,
                expected_source_id="legacy-monitor-events",
                expected_commit=expected_export_commit,
                recovery_verifier=verifier,
                filesystem_policy=filesystem_policy,
            )
            surge = load_accepted_legacy_shadow_export(
                root=settings.legacy_surge_root,
                trade_date=trade_date,
                expected_source_id="legacy-surge-jsonl",
                expected_commit=expected_export_commit,
                recovery_verifier=verifier,
                filesystem_policy=filesystem_policy,
            )
            runner_bindings = {
                binding.strategy_id: binding for binding in settings.runner_manifest_bindings
            }
            if any(
                binding.producer_commit != expected_export_commit
                for binding in runner_bindings.values()
            ):
                raise LegacyShadowExportUnavailableError(
                    "isolated runner profile commit differs from shadow export"
                )
        except (OSError, TypeError, ValueError, LegacyShadowExportUnavailableError) as exc:
            raise LegacyShadowExportUnavailableError(
                "legacy shadow filesystem export is unavailable"
            ) from exc
        return ShadowSessionInputs(
            monitor_rows=monitor.records,
            monitor_completion_receipt=monitor.completion_receipt,
            surge_events_path=surge.records_path,
            surge_completion_receipt=surge.completion_receipt,
            runner_sources=tuple(
                (
                    binding,
                    _FilesystemRunnerSource(
                        settings.isolated_runner_root,
                        strategy_id=binding.strategy_id,
                        expected_commit=expected_export_commit,
                        expected_runner_binding=runner_bindings[binding.strategy_id],
                        recovery_verifier=verifier,
                        filesystem_policy=filesystem_policy,
                    ),
                )
                for binding in settings.strategy_bindings
            ),
        )


def _validate_isolated_inputs(
    inputs: ShadowSessionInputs,
    *,
    trade_date: date,
    completion_keyring: Ed25519CompletionAttestationKeyring,
) -> None:
    for binding, source in inputs.runner_sources:
        try:
            receipt = source.read_completion_receipt(trade_date=trade_date)
        except (LegacyShadowExportUnavailableError, ValueError) as exc:
            raise LegacyShadowExportUnavailableError(
                "isolated shadow export completion receipt is unavailable"
            ) from exc
        attestation = receipt.completion_attestation
        if (
            attestation is None
            or not verify_completion_attestation(receipt, completion_keyring)
            or attestation.claims.strategy_id != binding.strategy_id
            or attestation.claims.strategy_version != binding.strategy_version
            or attestation.claims.strategy_registration_fingerprint
            != binding.definition_fingerprint
            or attestation.claims.executable_fingerprint != binding.executable_fingerprint
        ):
            raise LegacyShadowExportUnavailableError(
                "isolated shadow export completion receipt is not accepted"
            )


ShadowSessionExecutor = Callable[..., ShadowSessionReport]


def shadow_session_builder(
    *,
    clock: Callable[[], datetime],
    input_loader: ShadowSessionInputLoader | None = None,
    session_executor: ShadowSessionExecutor | None = None,
) -> RuntimeServiceBuilder:
    """Build the public-key-only post-close Shadow execution boundary."""

    def build(manifest: RuntimeServiceManifest) -> RuntimeServiceStep:
        if manifest.service_kind is not RuntimeServiceKind.SHADOW_SESSION:
            raise ValueError("runtime service kind must be shadow_session")
        if manifest.plane is not RuntimeServicePlane.RESEARCH:
            raise ValueError("Shadow session must run on the research plane")
        settings = ShadowSessionSettings.model_validate(dict(manifest.settings))
        if any(
            binding.producer_commit != manifest.producer_commit
            for binding in settings.runner_manifest_bindings
        ):
            raise ValueError("Shadow runner manifest commit differs from service manifest")
        completion_keyring = Ed25519CompletionAttestationKeyring(
            active_key_id=settings.completion_active_key_id,
            active_public_key=settings.completion_active_public_key_pem.encode("utf-8"),
            previous_public_keys={
                key_id: value.encode("utf-8")
                for key_id, value in settings.completion_previous_public_key_pems.items()
            },
        )
        report_keyring = Ed25519ShadowReceiptKeyring(
            active_key_id=settings.report_active_key_id,
            active_public_key=settings.report_active_public_key_pem.encode("utf-8"),
            previous_public_keys={
                key_id: value.encode("utf-8")
                for key_id, value in settings.report_previous_public_key_pems.items()
            },
        )
        report_signer = Ed25519ShadowReceiptSigner(
            key_id=settings.report_active_key_id,
            client=SecureShadowSigningClient(
                command=settings.signer_command,
                key_id=settings.report_active_key_id,
                timeout_seconds=settings.signer_timeout_seconds,
            ),
        )
        resolved_input_loader = input_loader or FilesystemShadowSessionInputLoader()
        resolved_executor = session_executor or run_shadow_production_session

        def step() -> RuntimeStepResult:
            observed_at = normalize_aware_utc(clock())
            try:
                calendar = load_market_calendar_authority(
                    settings.calendar_path,
                    expected_commit=settings.calendar_expected_commit,
                )
                if calendar.content_sha256 != settings.calendar_content_sha256:
                    raise ValueError(
                        "Shadow calendar content identity does not match runtime settings"
                    )
                selection = ShadowCalendarSelection.create(
                    authority=calendar,
                    evaluated_at=observed_at,
                    maximum_sessions=20,
                )
            except (OSError, TypeError, ValueError):
                return RuntimeStepResult(
                    processed_count=0,
                    source_generations={"shadow_session": settings.calendar_content_sha256},
                    degraded_reasons=("shadow:legacy_export_unavailable",),
                )
            trade_date = selection.latest_closed_session
            try:
                inputs = resolved_input_loader.load(
                    settings=settings,
                    trade_date=trade_date,
                    expected_export_commit=manifest.producer_commit,
                )
                _validate_isolated_inputs(
                    inputs,
                    trade_date=trade_date,
                    completion_keyring=completion_keyring,
                )
            except LegacyShadowExportUnavailableError:
                return RuntimeStepResult(
                    processed_count=0,
                    source_generations={"shadow_session": calendar.content_sha256},
                    degraded_reasons=("shadow:legacy_export_unavailable",),
                )
            try:
                report = resolved_executor(
                    trade_date=trade_date,
                    observed_at=observed_at,
                    producer_commit=manifest.producer_commit,
                    producer_version=settings.producer_version,
                    calendar=calendar,
                    monitor_rows=inputs.monitor_rows,
                    monitor_completion_receipt=inputs.monitor_completion_receipt,
                    surge_events_path=inputs.surge_events_path,
                    surge_completion_receipt=inputs.surge_completion_receipt,
                    runner_sources=inputs.runner_sources,
                    report_root=settings.report_root,
                    match_tolerance_microseconds=settings.match_tolerance_microseconds,
                    attestation_verifier=completion_keyring,
                    report_receipt_signer=report_signer,
                    report_receipt_verifier=report_keyring,
                    report_producer_service_id=settings.report_producer_service_id,
                    report_producer_instance_id=settings.report_producer_instance_id,
                )
            except (LegacyShadowExportUnavailableError, ShadowInputUnavailableError):
                return RuntimeStepResult(
                    processed_count=0,
                    source_generations={"shadow_session": calendar.content_sha256},
                    degraded_reasons=("shadow:legacy_export_unavailable",),
                )
            if not isinstance(report, ShadowSessionReport):
                raise TypeError("Shadow production executor returned an invalid report")
            return RuntimeStepResult(
                processed_count=1,
                source_generations={"shadow_session": str(report.report_id)},
            )

        return step

    return build


__all__ = [
    "FilesystemShadowSessionInputLoader",
    "ShadowSessionInputLoader",
    "ShadowSessionInputs",
    "ShadowSessionSettings",
    "shadow_session_builder",
]
