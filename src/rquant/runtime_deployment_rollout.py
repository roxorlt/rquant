"""Audited systemd rollout transaction for an installed runtime generation."""

from __future__ import annotations

import json
import os
import re
import stat
import subprocess
import tempfile
import time
from collections.abc import Callable
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated, Literal, Protocol

from pydantic import StringConstraints, model_validator

from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.runtime_deployment_bundle import (
    RuntimeDeploymentReceipt,
    advance_runtime_schema_rollout,
    load_runtime_schema_rollout,
    rollback_runtime_schema_rollout,
)
from rquant.runtime_service_control import RuntimeServiceControl, RuntimeServiceStatus
from rquant.runtime_service_entrypoint import (
    RuntimeServiceKind,
    load_runtime_service_manifest,
)
from rquant.schema_compatibility import RolloutPhase, SchemaRolloutState
from rquant.strict_json import (
    canonical_json_bytes,
    strict_json_loads,
    strict_model_validate_canonical_json,
)

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]


class RuntimeDeploymentRolloutError(RuntimeError):
    """The isolated runtime rollout failed after durable intent publication."""


class RuntimeSchemaRetireObservation(RuntimeContractModel):
    plan_id: Sha256
    cutover_receipt_hash: Sha256
    cutover_observed_at: AwareUtcDatetime
    retire_eligible_at: AwareUtcDatetime


class RuntimeSchemaRetirementStatus(RuntimeContractModel):
    plan_id: Sha256
    phase: RolloutPhase
    receipt_hash: Sha256
    cutover_observed_at: AwareUtcDatetime
    retire_eligible_at: AwareUtcDatetime
    eligible: bool


class RuntimeRolloutController(Protocol):
    def daemon_reload(self) -> None: ...

    def restart(self, unit: str) -> None: ...

    def stop(self, unit: str) -> None: ...

    def wait_healthy(
        self,
        unit: str,
        *,
        receipt: RuntimeDeploymentReceipt,
        timeout_seconds: float,
    ) -> None: ...


class RuntimeDeploymentRolloutAudit(RuntimeContractModel):
    operation_id: Sha256
    profile_id: Sha256
    generation_hash: Sha256
    previous_generation_hash: Sha256 | None
    status: Literal["running", "succeeded", "rolled_back", "failed_closed"]
    service_units: tuple[str, ...]
    schema_rollout_plan_ids: tuple[Sha256, ...] = ()
    schema_receipt_hashes: tuple[Sha256, ...] = ()
    schema_retire_observations: tuple[RuntimeSchemaRetireObservation, ...] = ()
    error_type: str | None = None

    @model_validator(mode="after")
    def validate_schema_receipt_binding(self) -> RuntimeDeploymentRolloutAudit:
        if tuple(sorted(set(self.schema_rollout_plan_ids))) != self.schema_rollout_plan_ids:
            raise ValueError("schema rollout plan ids must be unique and sorted")
        if len(self.schema_receipt_hashes) != len(self.schema_rollout_plan_ids):
            raise ValueError("schema rollout receipt hashes must bind every plan")
        observation_ids = tuple(item.plan_id for item in self.schema_retire_observations)
        if observation_ids != tuple(sorted(set(observation_ids))):
            raise ValueError("schema retire observations must be unique and sorted")
        if self.status == "succeeded" and self.schema_rollout_plan_ids:
            if observation_ids != self.schema_rollout_plan_ids:
                raise ValueError("successful schema rollout must bind every retire observation")
            observed_hashes = tuple(
                item.cutover_receipt_hash for item in self.schema_retire_observations
            )
            if observed_hashes != self.schema_receipt_hashes:
                raise ValueError("schema retire observations differ from rollout receipts")
        elif self.schema_retire_observations:
            raise ValueError("only a successful rollout can record retire observations")
        return self


CurrentReceiptLoader = Callable[[], RuntimeDeploymentReceipt]
PreviousReceiptLoader = Callable[[str], RuntimeDeploymentReceipt | None]
PreviousGenerationActivator = Callable[[RuntimeDeploymentReceipt], object]
CommandRunner = Callable[[tuple[str, ...]], int]
HealthProbe = Callable[[RuntimeDeploymentReceipt, str], bool]
_MAX_RETIRE_STATUS_PLANS = 64

_UNIT_PRIORITY = (
    "reference-slow-source",
    "reference-slow-publisher",
    "auction-universe",
    "auction-match",
    "market-minute",
    "watchlist-quote",
    "daily-close",
    "candidate",
    "feature",
    "paper-constraint",
    "strategy",
    "signal-router",
    "notifier",
    "paper-broker",
    "shadow",
    "runtime-health",
    "lab-jobs",
    "artifact-catalog",
    "artifact-retention",
    "daily-orchestrator",
    "promotions",
    "research",
    "serving",
)
_UNIT_RUNTIME_BINDING = {
    "reference-slow-source": (
        RuntimeServiceKind.REFERENCE_SLOW_SOURCE,
        "reference-slow-sources",
    ),
    "reference-slow-publisher": (
        RuntimeServiceKind.REFERENCE_SLOW_PUBLISHER,
        "reference-slow-publishers",
    ),
    "auction-universe": (
        RuntimeServiceKind.AUCTION_UNIVERSE_PUBLISHER,
        "auction-universe-publishers",
    ),
    "auction-match": (RuntimeServiceKind.AUCTION_MATCH_SOURCE, "auction-match-sources"),
    "market-minute": (RuntimeServiceKind.MARKET_MINUTE_SOURCE, "market-minute-sources"),
    "watchlist-quote": (
        RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE,
        "watchlist-quote-sources",
    ),
    "daily-close": (RuntimeServiceKind.DAILY_CLOSE_SOURCE, "daily-close-sources"),
    "daily-orchestrator": (
        RuntimeServiceKind.DAILY_PIPELINE_ORCHESTRATOR,
        "daily-orchestrators",
    ),
    "shadow": (RuntimeServiceKind.SHADOW_SESSION, "shadow-sessions"),
    "candidate": (RuntimeServiceKind.CANDIDATE_PUBLISHER, "candidates"),
    "feature": (RuntimeServiceKind.FEATURE_LIVE, "features"),
    "strategy": (RuntimeServiceKind.STRATEGY_LIVE, "strategies"),
    "signal-router": (RuntimeServiceKind.SIGNAL_ROUTER, "signal-routers"),
    "notifier": (RuntimeServiceKind.NOTIFIER, "notifiers"),
    "paper-constraint": (
        RuntimeServiceKind.PAPER_CONSTRAINT_PUBLISHER,
        "paper-constraints",
    ),
    "paper-broker": (RuntimeServiceKind.PAPER_BROKER, "paper-brokers"),
    "runtime-health": (
        RuntimeServiceKind.RUNTIME_HEALTH_PUBLISHER,
        "runtime-health-publishers",
    ),
    "lab-jobs": (RuntimeServiceKind.LAB_JOBS_PUBLISHER, "lab-jobs-publishers"),
    "artifact-catalog": (
        RuntimeServiceKind.LAB_ARTIFACT_CATALOG,
        "artifact-catalogs",
    ),
    "artifact-retention": (
        RuntimeServiceKind.ARTIFACT_RETENTION,
        "artifact-retention",
    ),
    "promotions": (RuntimeServiceKind.PROMOTIONS_PUBLISHER, "promotions-publishers"),
    "serving": (RuntimeServiceKind.SERVING_PUBLISHER, "serving-publishers"),
}
_COMPLETION_SOURCE_KEYS = {
    "daily-close": ("daily_close",),
    "daily-orchestrator": (
        "daily_pipeline_profile",
        "daily_pipeline_namespace",
        "daily_pipeline_command_manifest",
        "daily_pipeline_completion_receipt",
    ),
    "shadow": ("shadow_session",),
    "artifact-retention": ("artifact_gc_health",),
}
_ONESHOT_COMPLETION_MARKERS = frozenset({"daily-orchestrator", "artifact-retention"})


def _unit_marker(unit: str) -> str:
    if unit == "rquant-artifact-retention.service":
        return "artifact-retention"
    for marker in _UNIT_PRIORITY:
        if f"rquant-runtime-{marker}@" in unit:
            return marker
    raise ValueError(f"runtime rollout unit is not allow-listed: {unit}")


def _run_systemctl(arguments: tuple[str, ...]) -> int:
    completed = subprocess.run(
        ("/usr/bin/systemctl", *arguments),
        check=False,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return completed.returncode


class SystemdRuntimeRolloutController:
    """Small no-shell adapter around systemctl plus a generation-bound health probe."""

    def __init__(
        self,
        *,
        health_probe: HealthProbe,
        command_runner: CommandRunner = _run_systemctl,
        monotonic_clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], object] = time.sleep,
        poll_interval_seconds: float = 0.5,
    ) -> None:
        if poll_interval_seconds <= 0:
            raise ValueError("systemd rollout poll interval must be positive")
        self._health_probe = health_probe
        self._command_runner = command_runner
        self._monotonic_clock = monotonic_clock
        self._sleep = sleep
        self._poll_interval_seconds = poll_interval_seconds

    def _require_success(self, arguments: tuple[str, ...]) -> None:
        if self._command_runner(arguments) != 0:
            raise RuntimeDeploymentRolloutError(
                f"systemctl {arguments[0]} failed for isolated runtime"
            )

    def daemon_reload(self) -> None:
        self._require_success(("daemon-reload",))

    def restart(self, unit: str) -> None:
        _unit_priority(unit)
        self._require_success(("restart", unit))

    def stop(self, unit: str) -> None:
        _unit_priority(unit)
        self._require_success(("stop", unit))

    def wait_healthy(
        self,
        unit: str,
        *,
        receipt: RuntimeDeploymentReceipt,
        timeout_seconds: float,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("systemd rollout health timeout must be positive")
        marker = _unit_marker(unit)
        oneshot = marker in _ONESHOT_COMPLETION_MARKERS
        deadline = self._monotonic_clock() + timeout_seconds
        while True:
            active = self._command_runner(("is-active", "--quiet", unit)) == 0
            health_bound = self._health_probe(receipt, unit) if active or oneshot else False
            if health_bound and (active or oneshot):
                return
            if self._monotonic_clock() >= deadline:
                raise RuntimeDeploymentRolloutError(
                    f"runtime unit did not become generation-bound healthy: {unit}"
                )
            self._sleep(self._poll_interval_seconds)


def _unit_priority(unit: str) -> tuple[int, str]:
    return _UNIT_PRIORITY.index(_unit_marker(unit)), unit


def _verify_daily_orchestrator_completion(
    *,
    deployment_receipt: RuntimeDeploymentReceipt,
    manifest: object,
    heartbeat: object,
) -> bool:
    from rquant.daily_pipeline_ledger import (
        DailyPipelineLedger,
        DailyPipelineMode,
        DailyPipelineStorageProfile,
        DailyRunState,
        DailyStageState,
    )
    from rquant.daily_pipeline_orchestrator import DEFAULT_DAILY_CLOSE_PIPELINE
    from rquant.runtime_builder_daily_orchestrator import (
        DailyPipelineOrchestratorSettings,
        DailyShadowStageArtifact,
        DailyShadowStageCompletionReceipt,
        daily_shadow_stage_receipt_keyring,
        daily_shadow_ledger_owner,
        load_daily_shadow_run_completion_receipt,
    )
    from rquant.runtime_deployment_profile import RuntimeDeploymentProfile
    from rquant.runtime_service_entrypoint import RuntimeServiceManifest

    if not isinstance(manifest, RuntimeServiceManifest):
        return False
    try:
        settings = DailyPipelineOrchestratorSettings.model_validate(dict(manifest.settings))
        if settings.service_owner != manifest.service_id:
            return False
        source_generations = dict(getattr(heartbeat, "source_generations"))
        profile_hash = source_generations["daily_pipeline_profile"]
        namespace_id = source_generations["daily_pipeline_namespace"]
        command_manifest_hash = source_generations["daily_pipeline_command_manifest"]
        completion_receipt_id = source_generations["daily_pipeline_completion_receipt"]
        if profile_hash != deployment_receipt.deployment_profile_id:
            return False
        profile = RuntimeDeploymentProfile.model_validate(
            strict_json_loads(settings.deployment_profile_path.read_bytes())
        )
        if (
            profile.profile_id != profile_hash
            or profile.producer_commit != deployment_receipt.producer_commit
        ):
            return False
        profile_matches = tuple(
            item for item in profile.manifests if item.service_id == manifest.service_id
        )
        if len(profile_matches) != 1:
            return False
        profile_manifest_fingerprint = str(profile_matches[0].manifest_fingerprint)
        if (
            len(profile_manifest_fingerprint) == 64
            and profile_manifest_fingerprint != manifest.manifest_fingerprint
        ):
            return False
        storage = DailyPipelineStorageProfile.create(
            root=settings.storage_root,
            mode=DailyPipelineMode.SHADOW,
            profile_hash=profile_hash,
        )
        if storage.namespace_id != namespace_id:
            return False
        keyring = daily_shadow_stage_receipt_keyring(settings)
        command_manifest = strict_json_loads(storage.command_manifest_path.read_bytes())
        if canonical_sha256(command_manifest) != command_manifest_hash:
            return False
        ledger = DailyPipelineLedger(
            storage_profile=storage,
            service_owner=daily_shadow_ledger_owner(settings.service_owner),
        )
        run_completion_receipt = load_daily_shadow_run_completion_receipt(
            storage,
            completion_receipt_id,
        )
        if not keyring.verify_run_completion_receipt(
            run_completion_receipt,
            require_active=True,
        ):
            return False
        run = ledger.run(run_completion_receipt.run_id)
        backup_stage_for_run = ledger.stage(run.run_id, "backup")
        if backup_stage_for_run.terminal_receipt_id is None:
            return False
        final_receipt = ledger.receipt(backup_stage_for_run.terminal_receipt_id)
        if final_receipt is None or final_receipt.stage_id != "backup":
            return False
        if (
            run.state is not DailyRunState.SUCCEEDED
            or run.spec.mode is not DailyPipelineMode.SHADOW
            or run.spec.profile_hash != profile_hash
            or run.spec.command_manifest_hash != command_manifest_hash
            or run.spec.code_commit != deployment_receipt.producer_commit
            or run.input_identity != final_receipt.input_identity
            or run_completion_receipt.run_id != run.run_id
            or run_completion_receipt.input_identity != run.input_identity
            or run_completion_receipt.profile_hash != profile_hash
            or run_completion_receipt.source_generation_id != run.spec.source_generation_id
            or run_completion_receipt.source_content_hash != run.spec.source_content_hash
            or run_completion_receipt.command_manifest_hash != command_manifest_hash
            or run_completion_receipt.mode is not DailyPipelineMode.SHADOW
            or run_completion_receipt.trade_date != run.spec.trade_date
            or run_completion_receipt.code_commit != deployment_receipt.producer_commit
            or run_completion_receipt.exit_status != 0
            or run_completion_receipt.key_id != settings.receipt_active_key_id
        ):
            return False
        if tuple(settings.stages) != DEFAULT_DAILY_CLOSE_PIPELINE.stage_ids:
            return False
        observed_stage_order: list[str] = []
        ledger_receipt_by_stage: dict[str, str] = {}
        signed_receipt_by_stage: dict[str, str] = {}
        for stage_id in settings.stages:
            runtime_stage = DEFAULT_DAILY_CLOSE_PIPELINE.runtime_spec(stage_id)
            dependency_receipt_ids = tuple(
                ledger_receipt_by_stage[item] for item in runtime_stage.depends_on
            )
            stage = ledger.stage(run.run_id, stage_id)
            if stage.state is not DailyStageState.SUCCEEDED:
                return False
            if stage.terminal_receipt_id is None:
                return False
            ledger_receipt = ledger.receipt(stage.terminal_receipt_id)
            effect = ledger.effect_for_stage(run.run_id, stage_id)
            if effect is None:
                return False
            stage_receipt = DailyShadowStageCompletionReceipt.model_validate(
                strict_json_loads(Path(effect.intent.receipt_locator).read_bytes())
            )
            if (
                ledger_receipt is None
                or ledger_receipt.run_id != run.run_id
                or ledger_receipt.stage_id != stage_id
                or ledger_receipt.input_identity != run.input_identity
                or stage_receipt.run_id != run.run_id
                or stage_receipt.stage_id != stage_id
                or stage_receipt.input_identity != run.input_identity
                or stage_receipt.mode is not DailyPipelineMode.SHADOW
                or stage_receipt.profile_hash != profile_hash
                or stage_receipt.command_manifest_hash != command_manifest_hash
                or stage_receipt.source_generation_id != run.spec.source_generation_id
                or stage_receipt.source_content_hash != run.spec.source_content_hash
                or stage_receipt.effect_id != effect.intent.effect_id
                or stage_receipt.idempotency_key != effect.intent.idempotency_key
                or stage_receipt.command_identity != effect.intent.adapter_identity
                or stage_receipt.stage_order != len(observed_stage_order)
                or stage_receipt.exit_status != 0
                or stage_receipt.dependency_receipt_ids != dependency_receipt_ids
                or stage_receipt.previous_receipt_hash
                != canonical_sha256(
                    {
                        "contract": "daily-shadow-stage-previous-receipt-chain/v1",
                        "dependency_receipt_ids": dependency_receipt_ids,
                    }
                )
                or stage_receipt.result != ledger_receipt.result
                or stage_receipt.key_id != settings.receipt_active_key_id
            ):
                return False
            if not keyring.verify_stage_receipt(stage_receipt, require_active=True):
                return False
            observed_stage_order.append(stage_receipt.stage_id)
            ledger_receipt_by_stage[stage_id] = stage.terminal_receipt_id
            signed_receipt_by_stage[stage_id] = str(stage_receipt.receipt_id)
        if tuple(observed_stage_order) != DEFAULT_DAILY_CLOSE_PIPELINE.stage_ids:
            return False
        if (
            run_completion_receipt.stage_order != tuple(observed_stage_order)
            or run_completion_receipt.stage_receipt_ids
            != tuple(signed_receipt_by_stage[stage_id] for stage_id in observed_stage_order)
            or run_completion_receipt.previous_receipt_hash
            != canonical_sha256(
                {
                    "contract": "daily-shadow-stage-previous-receipt-chain/v1",
                    "dependency_receipt_ids": run_completion_receipt.stage_receipt_ids,
                }
            )
        ):
            return False
        backup_stage = ledger.stage(run.run_id, "backup")
        if backup_stage.terminal_receipt_id != final_receipt.receipt_id:
            return False
        backup_effect = ledger.effect_for_stage(run.run_id, "backup")
        if backup_effect is None:
            return False
        stage_receipt = DailyShadowStageCompletionReceipt.model_validate(
            strict_json_loads(Path(backup_effect.intent.receipt_locator).read_bytes())
        )
        if stage_receipt.receipt_id != run_completion_receipt.final_backup_receipt_id:
            return False
        artifact = DailyShadowStageArtifact.model_validate(
            strict_json_loads(
                (
                    storage.namespace_root
                    / "effects"
                    / "backup"
                    / f"{backup_effect.intent.effect_id}.json"
                ).read_bytes()
            )
        )
        return (
            stage_receipt.stage_id == "backup"
            and stage_receipt.result == final_receipt.result
            and stage_receipt.result == run_completion_receipt.final_backup_result
            and stage_receipt.exit_status == 0
            and keyring.verify_stage_receipt(stage_receipt, require_active=True)
            and artifact.stage_id == "backup"
            and artifact.exit_status == 0
            and artifact.artifact_id == final_receipt.result.content_hash
            and artifact.output_identity == stage_receipt.output_identity
            and artifact.output_identity == run_completion_receipt.final_backup_output_identity
            and artifact.output_identity in {item.sha256 for item in artifact.output_files}
        )
    except (AttributeError, KeyError, OSError, TypeError, ValueError):
        return False


def build_runtime_generation_health_probe(
    *,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> HealthProbe:
    """Build a probe that binds systemd activity to the exact manifest heartbeat."""

    def probe(receipt: RuntimeDeploymentReceipt, unit: str) -> bool:
        try:
            service_id = next(
                service_id
                for service_id, mapped_unit in receipt.unit_mapping.items()
                if mapped_unit == unit
            )
            instance = receipt.instance_mapping[service_id]
            marker = _unit_marker(unit)
            expected_kind, control_bucket = _UNIT_RUNTIME_BINDING[marker]
            manifest = load_runtime_service_manifest(
                receipt.runtime_root / "current" / "manifests" / f"{instance}.json",
                expected_commit=receipt.producer_commit,
                expected_generation=receipt.generation_hash,
            )
            if manifest.service_id != service_id or manifest.service_kind is not expected_kind:
                return False
            heartbeat = RuntimeServiceControl.read_heartbeat(
                receipt.runtime_root / "control" / control_bucket / instance,
                manifest.service_spec,
            )
        except (AttributeError, KeyError, OSError, StopIteration, ValueError):
            return False
        if heartbeat is None or heartbeat.last_success_at is None:
            return False
        now = clock().astimezone(UTC)
        fresh = heartbeat.heartbeat_at <= now and heartbeat.heartbeat_at >= now - timedelta(
            seconds=manifest.stale_after_seconds
        )
        if not fresh:
            return False
        required_completion_keys = _COMPLETION_SOURCE_KEYS.get(marker)
        if required_completion_keys is not None and not all(
            key in heartbeat.source_generations for key in required_completion_keys
        ):
            return False
        if marker == "daily-orchestrator" and not _verify_daily_orchestrator_completion(
            deployment_receipt=receipt,
            manifest=manifest,
            heartbeat=heartbeat,
        ):
            return False
        if marker in _ONESHOT_COMPLETION_MARKERS:
            assert required_completion_keys is not None
            return (
                heartbeat.status is RuntimeServiceStatus.STOPPED
                and heartbeat.stop_reason == "loop completed"
                and heartbeat.consecutive_failures == 0
                and heartbeat.total_successes >= 1
            )
        return heartbeat.status is RuntimeServiceStatus.RUNNING

    return probe


def _ordered_units(receipt: RuntimeDeploymentReceipt) -> tuple[str, ...]:
    return tuple(sorted(receipt.unit_mapping.values(), key=_unit_priority))


def _prepare_audit_root(path: Path) -> Path:
    root = Path(path)
    if not root.is_absolute() or root != Path(os.path.abspath(root)):
        raise ValueError("runtime rollout audit root must be absolute and normalized")
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    observed = root.lstat()
    if (
        not stat.S_ISDIR(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != os.getuid()
    ):
        raise ValueError("runtime rollout audit root is unsafe")
    root.chmod(0o700)
    return root


def _audit_path(root: Path, operation_id: str) -> Path:
    return root / f"{operation_id}.json"


def _write_audit(root: Path, audit: RuntimeDeploymentRolloutAudit) -> None:
    payload = canonical_json_bytes(audit.model_dump(mode="json"))
    descriptor, temporary_name = tempfile.mkstemp(prefix=".rollout-", dir=root)
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb", closefd=True) as stream:
            descriptor = -1
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, _audit_path(root, audit.operation_id))
        directory = os.open(root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        with suppress(FileNotFoundError):
            temporary.unlink()


def _read_audit(root: Path, operation_id: str) -> RuntimeDeploymentRolloutAudit | None:
    path = _audit_path(root, operation_id)
    try:
        observed = path.lstat()
    except FileNotFoundError:
        return None
    if (
        not stat.S_ISREG(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != os.getuid()
        or stat.S_IMODE(observed.st_mode) != 0o600
    ):
        raise ValueError("runtime rollout audit is unsafe")
    return strict_model_validate_canonical_json(
        RuntimeDeploymentRolloutAudit,
        path.read_bytes(),
    )


def load_runtime_deployment_rollout_audit(
    audit_root: Path,
    *,
    operation_id: str,
) -> RuntimeDeploymentRolloutAudit:
    """Load one exact immutable rollout audit without creating control state."""

    root = Path(audit_root)
    if not root.is_absolute() or root != Path(os.path.abspath(root)):
        raise ValueError("runtime rollout audit root must be absolute and normalized")
    if re.fullmatch(r"[0-9a-f]{64}", operation_id) is None:
        raise ValueError("runtime rollout operation id is invalid")
    try:
        observed = root.lstat()
    except OSError as exc:
        raise ValueError("runtime rollout audit root is unavailable") from exc
    if (
        not stat.S_ISDIR(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != os.getuid()
    ):
        raise ValueError("runtime rollout audit root is unsafe")
    audit = _read_audit(root, operation_id)
    if audit is None:
        raise ValueError("runtime rollout audit does not exist")
    return audit


def _require_current(
    expected: RuntimeDeploymentReceipt,
    loader: CurrentReceiptLoader,
) -> None:
    current = loader()
    if not _same_deployment_receipt(current, expected):
        raise RuntimeDeploymentRolloutError("runtime current receipt changed during rollout")


def _same_deployment_receipt(
    current: RuntimeDeploymentReceipt,
    expected: RuntimeDeploymentReceipt,
) -> bool:
    return not (
        current.runtime_root != expected.runtime_root
        or current.producer_commit != expected.producer_commit
        or current.generation_hash != expected.generation_hash
        or current.deployment_profile_id != expected.deployment_profile_id
        or current.instance_mapping != expected.instance_mapping
        or current.unit_mapping != expected.unit_mapping
    )


def _restart_and_verify(
    receipt: RuntimeDeploymentReceipt,
    *,
    units: tuple[str, ...],
    controller: RuntimeRolloutController,
    current_receipt_loader: CurrentReceiptLoader | None,
    timeout_seconds: float,
    started_units: list[str] | None = None,
) -> list[str]:
    started = started_units if started_units is not None else []
    controller.daemon_reload()
    for unit in units:
        if current_receipt_loader is not None:
            _require_current(receipt, current_receipt_loader)
        controller.restart(unit)
        started.append(unit)
        controller.wait_healthy(
            unit,
            receipt=receipt,
            timeout_seconds=timeout_seconds,
        )
    return started


def _stop_units(
    controller: RuntimeRolloutController,
    units: tuple[str, ...] | list[str],
) -> None:
    first_error: BaseException | None = None
    for unit in reversed(units):
        try:
            controller.stop(unit)
        except BaseException as exc:
            if first_error is None:
                first_error = exc
    if first_error is not None:
        raise first_error


def _schema_receipt_hashes(receipt: RuntimeDeploymentReceipt) -> tuple[str, ...]:
    hashes: list[str] = []
    for plan_id in receipt.schema_rollout_plan_ids:
        authority, store = load_runtime_schema_rollout(
            receipt.runtime_root,
            plan_id=plan_id,
        )
        if authority.target_generation_id != receipt.generation_hash:
            raise RuntimeDeploymentRolloutError(
                "schema rollout target differs from deployment generation"
            )
        events = store.receipts(plan_id)
        if not events:
            raise RuntimeDeploymentRolloutError("schema rollout has no durable receipts")
        hashes.append(events[-1].event_hash)
    return tuple(hashes)


def _schema_receipt_hashes_or_last_verified(
    receipt: RuntimeDeploymentReceipt,
    *,
    last_verified: tuple[str, ...],
) -> tuple[str, ...]:
    try:
        return _schema_receipt_hashes(receipt)
    except Exception:
        return last_verified


def _schema_retire_observations(
    receipt: RuntimeDeploymentReceipt,
) -> tuple[RuntimeSchemaRetireObservation, ...]:
    if len(receipt.schema_rollout_plan_ids) > _MAX_RETIRE_STATUS_PLANS:
        raise RuntimeDeploymentRolloutError("schema retirement plan set exceeds the bounded limit")
    observations: list[RuntimeSchemaRetireObservation] = []
    for plan_id in receipt.schema_rollout_plan_ids:
        authority, store = load_runtime_schema_rollout(
            receipt.runtime_root,
            plan_id=plan_id,
        )
        state = store.get_state(plan_id)
        if state.phase is not RolloutPhase.CUTOVER:
            raise RuntimeDeploymentRolloutError(
                f"schema rollout {plan_id} did not finish at cutover"
            )
        required = {producer.participant_id for producer in authority.plan.producers}
        acknowledgements: dict[str, datetime] = {}
        events = store.receipts(plan_id)
        for event in events:
            try:
                payload = json.loads(event.payload_json)
            except json.JSONDecodeError as exc:  # pragma: no cover - store verifies its chain
                raise RuntimeDeploymentRolloutError(
                    "schema rollout receipt payload is invalid"
                ) from exc
            if (
                event.event_type == "participant_ack"
                and payload.get("phase") == RolloutPhase.CUTOVER.value
                and payload.get("participant_id") in required
            ):
                acknowledgements[str(payload["participant_id"])] = event.recorded_at
        if set(acknowledgements) != required:
            raise RuntimeDeploymentRolloutError(
                f"schema rollout {plan_id} lacks complete cutover acknowledgement"
            )
        observed_at = max(acknowledgements.values())
        observations.append(
            RuntimeSchemaRetireObservation(
                plan_id=plan_id,
                cutover_receipt_hash=events[-1].event_hash,
                cutover_observed_at=observed_at,
                retire_eligible_at=(
                    observed_at + timedelta(seconds=authority.plan.retire_observation_seconds)
                ),
            )
        )
    return tuple(sorted(observations, key=lambda item: item.plan_id))


def preview_runtime_schema_retirement(
    receipt: RuntimeDeploymentReceipt,
    *,
    rollout_audit: RuntimeDeploymentRolloutAudit,
    now: datetime,
) -> tuple[RuntimeSchemaRetirementStatus, ...]:
    """Return a bounded, read-only eligibility view for explicitly bound rollout plans."""

    if not isinstance(receipt, RuntimeDeploymentReceipt):
        raise TypeError("receipt must be a RuntimeDeploymentReceipt")
    if not isinstance(rollout_audit, RuntimeDeploymentRolloutAudit):
        raise TypeError("rollout_audit must be a RuntimeDeploymentRolloutAudit")
    observed_at = normalize_aware_utc(now)
    if rollout_audit.status != "succeeded":
        raise ValueError("schema retirement requires a successful rollout audit")
    if (
        rollout_audit.profile_id != receipt.deployment_profile_id
        or rollout_audit.generation_hash != receipt.generation_hash
        or rollout_audit.schema_rollout_plan_ids != receipt.schema_rollout_plan_ids
    ):
        raise ValueError("schema retirement audit differs from the deployment receipt")
    if len(receipt.schema_rollout_plan_ids) > _MAX_RETIRE_STATUS_PLANS:
        raise ValueError("schema retirement status exceeds the bounded plan limit")
    observations = {
        observation.plan_id: observation for observation in rollout_audit.schema_retire_observations
    }
    statuses: list[RuntimeSchemaRetirementStatus] = []
    for plan_id in receipt.schema_rollout_plan_ids:
        observation = observations[plan_id]
        authority, store = load_runtime_schema_rollout(
            receipt.runtime_root,
            plan_id=plan_id,
        )
        if authority.target_generation_id != receipt.generation_hash:
            raise ValueError("schema retirement plan differs from deployment generation")
        state = store.get_state(plan_id)
        if state.phase not in {
            RolloutPhase.CUTOVER,
            RolloutPhase.RETIRE,
            RolloutPhase.ROLLBACK,
        }:
            raise ValueError("schema retirement status requires a post-cutover plan")
        events = store.receipts(plan_id)
        if not events:
            raise ValueError("schema retirement plan has no durable receipt")
        if (
            state.phase is RolloutPhase.CUTOVER
            and events[-1].event_hash != observation.cutover_receipt_hash
        ):
            raise ValueError("schema cutover receipt changed after observation started")
        statuses.append(
            RuntimeSchemaRetirementStatus(
                plan_id=plan_id,
                phase=state.phase,
                receipt_hash=events[-1].event_hash,
                cutover_observed_at=observation.cutover_observed_at,
                retire_eligible_at=observation.retire_eligible_at,
                eligible=(
                    state.phase is RolloutPhase.RETIRE
                    or (
                        state.phase is RolloutPhase.CUTOVER
                        and observed_at >= observation.retire_eligible_at
                    )
                ),
            )
        )
    return tuple(statuses)


def retire_runtime_schema_plan(
    receipt: RuntimeDeploymentReceipt,
    *,
    rollout_audit: RuntimeDeploymentRolloutAudit,
    plan_id: str,
    now: datetime,
    operation_id: str,
) -> SchemaRolloutState:
    """Retire one explicitly named plan after an operator-reviewed observation window."""

    if plan_id not in receipt.schema_rollout_plan_ids:
        raise ValueError("schema retirement plan is not bound to the deployment receipt")
    if not isinstance(operation_id, str) or not operation_id.strip():
        raise ValueError("schema retirement operation_id cannot be empty")
    status = next(
        item
        for item in preview_runtime_schema_retirement(
            receipt,
            rollout_audit=rollout_audit,
            now=now,
        )
        if item.plan_id == plan_id
    )
    _authority, store = load_runtime_schema_rollout(
        receipt.runtime_root,
        plan_id=plan_id,
    )
    state = store.get_state(plan_id)
    if state.phase is RolloutPhase.RETIRE:
        return state
    if state.phase is RolloutPhase.ROLLBACK:
        raise ValueError("rolled-back schema plan cannot retire")
    if not status.eligible:
        raise ValueError("schema retirement observation window is not complete")
    return advance_runtime_schema_rollout(
        receipt.runtime_root,
        plan_id=plan_id,
        expected_revision=state.revision,
        target_phase=RolloutPhase.RETIRE,
        now=normalize_aware_utc(now),
        operation_id=operation_id,
    )


def _advance_schema_plans(
    receipt: RuntimeDeploymentReceipt,
    *,
    source_phase: RolloutPhase,
    target_phase: RolloutPhase,
    now: datetime,
) -> None:
    for plan_id in receipt.schema_rollout_plan_ids:
        _authority, store = load_runtime_schema_rollout(
            receipt.runtime_root,
            plan_id=plan_id,
        )
        state = store.get_state(plan_id)
        if state.phase is target_phase:
            continue
        if state.phase is not source_phase:
            raise RuntimeDeploymentRolloutError(
                f"schema rollout {plan_id} is in unexpected phase {state.phase.value}"
            )
        advance_runtime_schema_rollout(
            receipt.runtime_root,
            plan_id=plan_id,
            expected_revision=state.revision,
            target_phase=target_phase,
            now=now,
            operation_id=(
                f"deployment-rollout:{receipt.generation_hash}:"
                f"{source_phase.value}:{target_phase.value}:{plan_id}"
            ),
        )


def _schema_producer_units(receipt: RuntimeDeploymentReceipt) -> tuple[str, ...]:
    service_ids: set[str] = set()
    for plan_id in receipt.schema_rollout_plan_ids:
        authority, _store = load_runtime_schema_rollout(
            receipt.runtime_root,
            plan_id=plan_id,
        )
        service_ids.update(producer.participant_id for producer in authority.plan.producers)
    try:
        return tuple(
            sorted(
                (receipt.unit_mapping[service_id] for service_id in service_ids),
                key=_unit_priority,
            )
        )
    except KeyError as exc:
        raise RuntimeDeploymentRolloutError(
            "schema producer is absent from deployment receipt"
        ) from exc


def _rollback_schema_plans(
    receipt: RuntimeDeploymentReceipt,
    *,
    now: datetime,
    reason: str,
) -> None:
    first_error: BaseException | None = None
    for plan_id in reversed(receipt.schema_rollout_plan_ids):
        try:
            _authority, store = load_runtime_schema_rollout(
                receipt.runtime_root,
                plan_id=plan_id,
            )
            state = store.get_state(plan_id)
            if state.phase is RolloutPhase.ROLLBACK:
                continue
            rollback_runtime_schema_rollout(
                receipt.runtime_root,
                plan_id=plan_id,
                expected_revision=state.revision,
                reason=reason,
                now=now,
                operation_id=(f"deployment-rollout-rollback:{receipt.generation_hash}:{plan_id}"),
            )
        except BaseException as exc:
            first_error = first_error or exc
    if first_error is not None:
        raise first_error


def _recover_incomplete_rollout(
    receipt: RuntimeDeploymentReceipt,
    *,
    intent: RuntimeDeploymentRolloutAudit,
    controller: RuntimeRolloutController,
    current_receipt_loader: CurrentReceiptLoader,
    previous_receipt_loader: PreviousReceiptLoader,
    previous_generation_activator: PreviousGenerationActivator,
    audit_root: Path,
    health_timeout_seconds: float,
    now: datetime,
) -> RuntimeDeploymentRolloutAudit:
    status: Literal["rolled_back", "failed_closed"] = "failed_closed"
    recovery_error: BaseException | None = None
    rollback_started: list[str] = []
    try:
        previous = (
            previous_receipt_loader(intent.previous_generation_hash)
            if intent.previous_generation_hash is not None
            else None
        )
        current = current_receipt_loader()
        target_is_current = _same_deployment_receipt(current, receipt)
        previous_is_current = previous is not None and _same_deployment_receipt(
            current,
            previous,
        )
        if not target_is_current and not previous_is_current:
            raise RuntimeDeploymentRolloutError(
                "runtime current receipt changed during rollout recovery"
            )
        _stop_units(controller, list(intent.service_units))
        _rollback_schema_plans(
            receipt,
            now=now,
            reason="recovering interrupted deployment rollout",
        )
        if previous is not None:
            if target_is_current:
                previous_generation_activator(previous)
            _restart_and_verify(
                previous,
                units=_ordered_units(previous),
                controller=controller,
                current_receipt_loader=None,
                timeout_seconds=health_timeout_seconds,
                started_units=rollback_started,
            )
            status = "rolled_back"
    except BaseException as exc:
        recovery_error = exc
        with suppress(BaseException):
            _stop_units(controller, rollback_started)
    recovered = intent.model_copy(
        update={
            "status": status,
            "schema_receipt_hashes": _schema_receipt_hashes_or_last_verified(
                receipt,
                last_verified=intent.schema_receipt_hashes,
            ),
            "error_type": (
                type(recovery_error).__name__
                if recovery_error is not None
                else "InterruptedRuntimeRollout"
            ),
        }
    )
    _write_audit(audit_root, recovered)
    if recovery_error is not None and not isinstance(recovery_error, Exception):
        raise recovery_error
    return recovered


def rollout_runtime_deployment(
    receipt: RuntimeDeploymentReceipt,
    *,
    controller: RuntimeRolloutController,
    current_receipt_loader: CurrentReceiptLoader,
    previous_receipt_loader: PreviousReceiptLoader,
    previous_generation_activator: PreviousGenerationActivator,
    audit_root: Path,
    health_timeout_seconds: float,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> RuntimeDeploymentRolloutAudit:
    """Restart a verified generation, or atomically return to its verified predecessor."""
    if not isinstance(receipt, RuntimeDeploymentReceipt):
        raise TypeError("receipt must be a RuntimeDeploymentReceipt")
    if receipt.deployment_profile_id is None:
        raise ValueError("runtime rollout requires a deployment profile identity")
    if health_timeout_seconds <= 0:
        raise ValueError("runtime rollout health timeout must be positive")
    units = _ordered_units(receipt)
    operation_id = canonical_sha256(
        {
            "contract": "runtime-deployment-rollout/v2",
            "runtime_root": str(receipt.runtime_root),
            "profile_id": receipt.deployment_profile_id,
            "generation_hash": receipt.generation_hash,
            "previous_generation_hash": receipt.previous_generation_hash,
            "service_units": units,
            "schema_rollout_plan_ids": receipt.schema_rollout_plan_ids,
        }
    )
    root = _prepare_audit_root(audit_root)
    existing = _read_audit(root, operation_id)
    if existing is not None:
        if existing.status == "succeeded":
            _require_current(receipt, current_receipt_loader)
            return existing
        if existing.status == "running":
            return _recover_incomplete_rollout(
                receipt,
                intent=existing,
                controller=controller,
                current_receipt_loader=current_receipt_loader,
                previous_receipt_loader=previous_receipt_loader,
                previous_generation_activator=previous_generation_activator,
                audit_root=root,
                health_timeout_seconds=health_timeout_seconds,
                now=clock(),
            )

    _require_current(receipt, current_receipt_loader)
    intent = RuntimeDeploymentRolloutAudit(
        operation_id=operation_id,
        profile_id=receipt.deployment_profile_id,
        generation_hash=receipt.generation_hash,
        previous_generation_hash=receipt.previous_generation_hash,
        status="running",
        service_units=units,
        schema_rollout_plan_ids=receipt.schema_rollout_plan_ids,
        schema_receipt_hashes=_schema_receipt_hashes(receipt),
    )
    _write_audit(root, intent)
    started: list[str] = []
    try:
        _restart_and_verify(
            receipt,
            units=units,
            controller=controller,
            current_receipt_loader=current_receipt_loader,
            timeout_seconds=health_timeout_seconds,
            started_units=started,
        )
        if receipt.schema_rollout_plan_ids:
            _advance_schema_plans(
                receipt,
                source_phase=RolloutPhase.DUAL_WRITE,
                target_phase=RolloutPhase.CONSUMER_ACK,
                now=clock(),
            )
            _restart_and_verify(
                receipt,
                units=units,
                controller=controller,
                current_receipt_loader=current_receipt_loader,
                timeout_seconds=health_timeout_seconds,
                started_units=started,
            )
            _advance_schema_plans(
                receipt,
                source_phase=RolloutPhase.CONSUMER_ACK,
                target_phase=RolloutPhase.CUTOVER,
                now=clock(),
            )
            _restart_and_verify(
                receipt,
                units=_schema_producer_units(receipt),
                controller=controller,
                current_receipt_loader=current_receipt_loader,
                timeout_seconds=health_timeout_seconds,
                started_units=started,
            )
    except BaseException as rollout_error:
        with suppress(BaseException):
            _stop_units(controller, started)
        previous = (
            previous_receipt_loader(receipt.previous_generation_hash)
            if receipt.previous_generation_hash is not None
            else None
        )
        status: Literal["rolled_back", "failed_closed"] = "failed_closed"
        if previous is not None:
            rollback_started: list[str] = []
            try:
                _rollback_schema_plans(
                    receipt,
                    now=clock(),
                    reason="deployment rollout failed",
                )
                previous_generation_activator(previous)
                _restart_and_verify(
                    previous,
                    units=_ordered_units(previous),
                    controller=controller,
                    current_receipt_loader=None,
                    timeout_seconds=health_timeout_seconds,
                    started_units=rollback_started,
                )
            except BaseException:
                with suppress(BaseException):
                    _stop_units(controller, rollback_started)
                status = "failed_closed"
            else:
                status = "rolled_back"
        failed = intent.model_copy(
            update={
                "status": status,
                "schema_receipt_hashes": _schema_receipt_hashes_or_last_verified(
                    receipt,
                    last_verified=intent.schema_receipt_hashes,
                ),
                "error_type": type(rollout_error).__name__,
            }
        )
        _write_audit(root, failed)
        if not isinstance(rollout_error, Exception):
            raise
        suffix = "rolled back" if status == "rolled_back" else "failed closed"
        raise RuntimeDeploymentRolloutError(
            f"runtime deployment rollout failed and {suffix}"
        ) from rollout_error

    schema_receipt_hashes = _schema_receipt_hashes(receipt)
    schema_retire_observations = _schema_retire_observations(receipt)
    succeeded = intent.model_copy(
        update={
            "status": "succeeded",
            "schema_receipt_hashes": schema_receipt_hashes,
            "schema_retire_observations": schema_retire_observations,
        }
    )
    _write_audit(root, succeeded)
    return succeeded


def rollback_runtime_deployment(
    receipt: RuntimeDeploymentReceipt,
    *,
    operation_id: str,
    controller: RuntimeRolloutController,
    current_receipt_loader: CurrentReceiptLoader,
    previous_receipt_loader: PreviousReceiptLoader,
    previous_generation_activator: PreviousGenerationActivator,
    audit_root: Path,
    health_timeout_seconds: float,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> RuntimeDeploymentRolloutAudit:
    """Explicitly restore the predecessor of one currently active generation."""

    if not isinstance(receipt, RuntimeDeploymentReceipt):
        raise TypeError("receipt must be a RuntimeDeploymentReceipt")
    if receipt.deployment_profile_id is None:
        raise ValueError("runtime rollback requires a deployment profile identity")
    if receipt.previous_generation_hash is None:
        raise ValueError("runtime rollback has no previous generation")
    if re.fullmatch(r"[0-9a-f]{64}", operation_id) is None:
        raise ValueError("runtime rollback operation id is invalid")
    if health_timeout_seconds <= 0:
        raise ValueError("runtime rollback health timeout must be positive")
    root = _prepare_audit_root(audit_root)
    existing = _read_audit(root, operation_id)
    if existing is not None:
        if (
            existing.profile_id != receipt.deployment_profile_id
            or existing.generation_hash != receipt.generation_hash
            or existing.previous_generation_hash != receipt.previous_generation_hash
            or existing.service_units != _ordered_units(receipt)
            or existing.schema_rollout_plan_ids != receipt.schema_rollout_plan_ids
        ):
            raise ValueError("runtime rollback operation id is already bound elsewhere")
        if existing.status == "rolled_back":
            previous = previous_receipt_loader(receipt.previous_generation_hash)
            if previous is None or not _same_deployment_receipt(current_receipt_loader(), previous):
                raise RuntimeDeploymentRolloutError(
                    "completed runtime rollback no longer matches current"
                )
            return existing
        if existing.status != "running":
            raise RuntimeDeploymentRolloutError("runtime rollback is failed closed")
        intent = existing
    else:
        _require_current(receipt, current_receipt_loader)
        intent = RuntimeDeploymentRolloutAudit(
            operation_id=operation_id,
            profile_id=receipt.deployment_profile_id,
            generation_hash=receipt.generation_hash,
            previous_generation_hash=receipt.previous_generation_hash,
            status="running",
            service_units=_ordered_units(receipt),
            schema_rollout_plan_ids=receipt.schema_rollout_plan_ids,
            schema_receipt_hashes=_schema_receipt_hashes(receipt),
        )
        _write_audit(root, intent)
    recovered = _recover_incomplete_rollout(
        receipt,
        intent=intent,
        controller=controller,
        current_receipt_loader=current_receipt_loader,
        previous_receipt_loader=previous_receipt_loader,
        previous_generation_activator=previous_generation_activator,
        audit_root=root,
        health_timeout_seconds=health_timeout_seconds,
        now=clock(),
    )
    if recovered.status != "rolled_back":
        raise RuntimeDeploymentRolloutError("runtime rollback failed closed")
    return recovered


__all__ = [
    "RuntimeDeploymentRolloutAudit",
    "RuntimeDeploymentRolloutError",
    "RuntimeRolloutController",
    "RuntimeSchemaRetireObservation",
    "RuntimeSchemaRetirementStatus",
    "SystemdRuntimeRolloutController",
    "build_runtime_generation_health_probe",
    "load_runtime_deployment_rollout_audit",
    "preview_runtime_schema_retirement",
    "retire_runtime_schema_plan",
    "rollback_runtime_deployment",
    "rollout_runtime_deployment",
]
