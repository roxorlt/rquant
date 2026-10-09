"Compose full-condition evaluation and delivery with the original alert runtime."

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta
from hashlib import sha256
from pathlib import Path
import sqlite3
from typing import TYPE_CHECKING

from pydantic import Field, StrictBool, field_validator, model_validator

from rquant.alert_rule_contracts import OwnedConditionAlertRule
from rquant.condition_alert_runtime import (
    ConditionAlertRuntimeStore,
    evaluate_condition_alert_round,
)
from rquant.condition_alert_runtime_contracts import (
    ConditionAlertRuntimeActivation,
    require_verified_condition_activation,
    verify_condition_alert_activation,
)
from rquant.condition_alert_runtime_projection import (
    ConditionAlertDeliveryAuthorityInput,
    ConditionConsumerProof,
    ConditionDeliveryScope,
    read_condition_rule_authority,
    resolve_condition_scope,
)
from rquant.price_alert_runtime_contracts import _activation_bytes
from rquant.runtime_contracts import RuntimeContractModel, normalize_aware_utc
from rquant.runtime_market_session import load_market_calendar_authority
from rquant.runtime_service_control import RuntimeStepResult
from rquant.runtime_service_entrypoint import (
    RuntimeServiceKind,
    RuntimeServiceManifest,
    load_runtime_service_manifest,
)
from rquant.serving_publisher import ServingReader
from rquant.strict_json import canonical_json_bytes

if TYPE_CHECKING:
    from rquant.condition_alert_route import ConditionAlertRecipientPolicy
    from rquant.notification_state import NotificationStateStore
    from rquant.price_alert_runtime_store import (
        PriceAlertRuntimeStore,
        ReadonlyPriceAlertRuntimeStore,
    )
    from rquant.runtime_service_entrypoint import RuntimeServiceStep
    from rquant.signal_bus import SignalBusStore


def _condition_code_contract(label: str, names: tuple[str, ...]) -> str:
    root = Path(__file__).parent
    return sha256(
        canonical_json_bytes(
            {
                "contract": label,
                "sources": {name: sha256((root / name).read_bytes()).hexdigest() for name in names},
            }
        )
    ).hexdigest()


def condition_evaluation_contract_sha256() -> str:
    return _condition_code_contract(
        "condition-alert-evaluation/v1",
        (
            "alert_rule_contracts.py",
            "condition_alert_runtime.py",
            "condition_alert_runtime_contracts.py",
            "web/screen_intraday.py",
            "screen/daily_inputs.py",
            "llm/registry.py",
            "screen/rules.py",
            "screen/ranking.py",
        ),
    )


def condition_routing_contract_sha256() -> str:
    return _condition_code_contract(
        "condition-alert-routing/v1",
        (
            "condition_alert_route.py",
            "condition_alert_runtime_projection.py",
            "runtime_notification_providers.py",
            "delivery_contracts.py",
        ),
    )


def verify_condition_role_manifest(
    manifest: RuntimeServiceManifest, *, runtime_root: Path
) -> ConditionAlertRuntimeActivation:
    path = manifest.settings.get("condition_alert_runtime_manifest_path")
    if not isinstance(path, str):
        raise ValueError("condition role needs its actual frozen manifest")
    actual_path = Path(path)
    payload = _activation_bytes(actual_path, runtime_root)
    actual = load_runtime_service_manifest(actual_path, expected_commit=manifest.producer_commit)
    if actual != manifest:
        raise ValueError("condition runtime differs from its actual role")
    activation = verify_condition_alert_activation(
        actual_path,
        runtime_root=runtime_root,
        expected_manifest_sha256=sha256(payload).hexdigest(),
        expected_commit=manifest.producer_commit,
        expected_kind=manifest.service_kind,
    )
    binding = require_verified_condition_activation(activation, manifest.service_kind.value)
    from rquant.condition_alert_runtime import ConditionFrequencyPolicy

    if (
        binding.evaluation_contract_sha256,
        binding.routing_policy_sha256,
        binding.frequency_policy_sha256,
    ) != (
        condition_evaluation_contract_sha256(),
        condition_routing_contract_sha256(),
        ConditionFrequencyPolicy().sha256,
    ):
        raise ValueError("condition actual code or policy contract changed")
    return activation


class ConditionEvaluationSettings(RuntimeContractModel):
    scope_serving_root: Path
    calendar_path: Path
    calendar_expected_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    calendar_content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    primary_path: Path | None = None
    replica_path: Path | None = None
    rsi_root: Path | None = None
    install_namespace: StrictBool = False

    @field_validator(
        "scope_serving_root", "calendar_path", "primary_path", "replica_path", "rsi_root"
    )
    @classmethod
    def absolute(cls, value: Path | None) -> Path | None:
        if value is not None and (not value.is_absolute() or ".." in value.parts):
            raise ValueError("condition runtime paths must be absolute")
        return value

    @model_validator(mode="after")
    def paired_replica(self) -> ConditionEvaluationSettings:
        if (self.primary_path is None) != (self.replica_path is None):
            raise ValueError("condition daily source requires the original verified replica pair")
        return self


def build_borrowed_condition_step(
    manifest: RuntimeServiceManifest,
    settings: ConditionEvaluationSettings,
    ledger: PriceAlertRuntimeStore,
    *,
    runtime_root: Path,
    clock: Callable[[], datetime],
) -> RuntimeServiceStep:
    from rquant.screen.dynamic_rsi import VerifiedDynamicRsiProjection
    from rquant.screen.replica_source import VerifiedReplicaScreenSource
    from rquant.web.serving import BorrowedGeneration

    activation = verify_condition_role_manifest(manifest, runtime_root=runtime_root)
    store = (
        ConditionAlertRuntimeStore.install(ledger, activation=activation)
        if settings.install_namespace
        else ConditionAlertRuntimeStore(ledger, activation=activation)
    )
    replica = (
        None
        if settings.replica_path is None
        else VerifiedReplicaScreenSource(
            primary_path=settings.primary_path, replica_path=settings.replica_path
        )
    )
    rsi = None if settings.rsi_root is None else VerifiedDynamicRsiProjection(settings.rsi_root)
    reader = ServingReader(settings.scope_serving_root)
    from rquant.monitor_builtin_runtime import read_installed_builtin_settings

    builtin_settings = read_installed_builtin_settings(activation)

    def evaluate_step() -> RuntimeStepResult:
        now = normalize_aware_utc(clock())
        binding = require_verified_condition_activation(activation, manifest.service_kind.value)
        if (
            binding.evaluation_contract_sha256 != condition_evaluation_contract_sha256()
            or binding.routing_policy_sha256 != condition_routing_contract_sha256()
        ):
            raise ValueError("condition installed code contract changed")
        calendar = load_market_calendar_authority(
            settings.calendar_path, expected_commit=settings.calendar_expected_commit
        )
        if calendar.content_sha256 != settings.calendar_content_sha256:
            raise ValueError("condition calendar bytes changed")
        with reader.acquire_generation() as lease:
            cursor = lease.connection.cursor()
            try:
                borrowed = BorrowedGeneration(
                    manifest=lease.manifest,
                    pointer=lease.pointer,
                    cursor=cursor,
                    fallback_detail=None,
                )
                authority = read_condition_rule_authority(borrowed, now=now)
                if authority is None:
                    raise ValueError("condition rule authority is not installed")
                rules = tuple(
                    OwnedConditionAlertRule(
                        owner_id=row.owner_id,
                        version=row.version,
                        rule=row.rule,
                        updated_at=row.updated_at,
                    )
                    for row in authority.rows
                    if not row.deleted
                )
                scopes = []
                for rule in rules:
                    try:
                        scopes.append(
                            resolve_condition_scope(
                                borrowed, owner_id=rule.owner_id, rule=rule.rule, now=now
                            )
                        )
                    except Exception:
                        scopes.append(None)
                value = evaluate_condition_alert_round(
                    activation=activation,
                    borrowed=borrowed,
                    rules=rules,
                    scopes=tuple(scopes),
                    calendar=calendar,
                    evaluated_at=now,
                    replica=replica,
                    rsi=rsi,
                )
                receipt = store.commit_round(
                    value, current_scope=lambda: reader.current_pointer() == lease.pointer
                )
                return RuntimeStepResult(
                    output_sequence=receipt.source_high_watermark,
                    processed_count=receipt.decision_count,
                    source_generations={"condition_alert_source": store.binding.generation_id},
                    degraded_reasons=()
                    if receipt.unknown_count == 0 and value.source is not None
                    else ("condition_alert:source_unknown",),
                )
            finally:
                cursor.close()

    def generic_step() -> RuntimeStepResult:
        try:
            return evaluate_step()
        except Exception:
            receipt = store.record_unavailable(
                evaluated_at=normalize_aware_utc(clock()), reason="source_unavailable"
            )
            return RuntimeStepResult(
                output_sequence=receipt.source_high_watermark,
                degraded_reasons=("condition_alert:source_unknown",),
            )

    def step() -> RuntimeStepResult:
        result = generic_step()
        if builtin_settings is None or not builtin_settings.enabled:
            return result
        from rquant.monitor_builtin_runtime import commit_original_builtin_round, read_effective_builtin_settings, read_original_builtin_capture, require_builtin_capture_authority, verify_builtin_capture_authority

        now = normalize_aware_utc(clock())
        current_settings = read_effective_builtin_settings(activation, now=now)
        refs = {item.origin: item for item in current_settings.sources}
        required = set()
        for definition in current_settings.definitions:
            required.add("watchlist_quote" if definition.builtin_id in {"pool2_levels", "pool_attack"} else "original_" + definition.builtin_id)
        if "watchlist_quote" in required and "original_monitor" in refs:
            required.remove("watchlist_quote")
            required.add("original_monitor")
        processed, reasons = 0, set(result.degraded_reasons)
        for origin in sorted(required):
            reference = refs.get(origin)
            try:
                if reference is None:
                    raise ValueError("original builtin source has no configured role")
                authority = verify_builtin_capture_authority(reference.manifest_path, runtime_root=reference.runtime_root,
                    expected_sha256=reference.manifest_sha256, expected_commit=reference.producer_commit)
                _, actual = require_builtin_capture_authority(authority)
                if actual.origin != origin or reference.producer_commit != manifest.producer_commit:
                    raise ValueError("original builtin source role differs from its configured reference")
                captured = read_original_builtin_capture(authority, read_at=now)
                receipt = commit_original_builtin_round(store, captured=captured, definitions=current_settings.definitions, evaluated_at=now)
                processed += len(receipt.events)
            except Exception:
                store.record_builtin_unavailable(definitions=current_settings.definitions, origin=origin,
                    evaluated_at=now, reason="capture_unavailable")
                reasons.add("monitor_builtin:source_unknown")
        if any(row.source_state in {"unknown", "disconnected", "stale"} for row in store.builtin_heads()):
            reasons.add("monitor_builtin:source_unknown")
        return result.model_copy(update={"output_sequence": store.source_descriptor().high_watermark,
            "processed_count": result.processed_count + processed, "degraded_reasons": tuple(sorted(reasons))})

    step.condition_store = store
    return step


class ConditionAlertPeerSettings(RuntimeContractModel):
    producer_manifest_path: Path
    ledger_path: Path
    recipient_policy_path: Path
    scope_serving_root: Path
    install_namespace: StrictBool = False

    @field_validator(
        "producer_manifest_path", "ledger_path", "recipient_policy_path", "scope_serving_root"
    )
    @classmethod
    def absolute(cls, value: Path) -> Path:
        if not value.is_absolute() or ".." in value.parts:
            raise ValueError("condition peer paths must be absolute")
        return value


def open_condition_role_peer(
    manifest: RuntimeServiceManifest,
    settings: ConditionAlertPeerSettings,
    *,
    runtime_root: Path | None,
    borrowed: ReadonlyPriceAlertRuntimeStore | None = None,
    borrowed_condition: ConditionAlertRuntimeStore | None = None,
    source_connection: sqlite3.Connection | None = None,
) -> tuple[
    ConditionAlertRuntimeActivation, ConditionAlertRuntimeStore, ConditionAlertRecipientPolicy
]:
    from rquant.condition_alert_route import ConditionAlertRecipientPolicy
    from rquant.price_alert_runtime_store import PriceAlertRuntimeStore, ReadonlyPriceAlertRuntimeStore
    from rquant.price_alert_runtime_contracts import require_verified_price_alert_activation
    from rquant.runtime_builder_price_alert import verify_price_role_manifest

    if runtime_root is None or manifest.service_kind not in {
        RuntimeServiceKind.SIGNAL_ROUTER,
        RuntimeServiceKind.NOTIFIER,
    }:
        raise ValueError("condition peer requires the original router or notifier")
    activation = verify_condition_role_manifest(manifest, runtime_root=runtime_root)
    own = require_verified_condition_activation(activation, manifest.service_kind.value)
    producer_manifest = load_runtime_service_manifest(
        settings.producer_manifest_path, expected_commit=manifest.producer_commit
    )
    if producer_manifest.service_kind is not RuntimeServiceKind.PRICE_ALERT_RUNTIME:
        raise ValueError("condition producer must compose with the original price role")
    producer_activation = verify_condition_role_manifest(
        producer_manifest, runtime_root=runtime_root
    )
    producer = require_verified_condition_activation(
        producer_activation, producer_manifest.service_kind.value
    )
    keys = (
        "source_id",
        "ledger_id",
        "source_epoch",
        "generation_id",
        "evaluation_contract_sha256",
        "frequency_policy_sha256",
        "routing_policy_sha256",
    )
    if any(getattr(own, key) != getattr(producer, key) for key in keys):
        raise ValueError("condition producer and consumer installed source differ")
    policy = ConditionAlertRecipientPolicy.model_validate_json(
        _activation_bytes(settings.recipient_policy_path, runtime_root)
    )
    if policy.sha256 != own.recipient_policy_sha256:
        raise ValueError("condition actual recipient policy differs")
    if borrowed_condition is not None or source_connection is not None:
        if (borrowed is not None or type(borrowed_condition) is not ConditionAlertRuntimeStore
                or type(source_connection) is not sqlite3.Connection
                or type(borrowed_condition.ledger) is not PriceAlertRuntimeStore
                or not source_connection.in_transaction):
            raise ValueError("condition borrowed transaction requires its actual original writer")
        ledger = borrowed_condition.ledger
        databases = source_connection.execute("PRAGMA database_list").fetchall()
        if (ledger.path != settings.ledger_path or ledger._closed
                or ledger._file_identity() != ledger._identity
                or len(databases) != 1 or databases[0][1:] != ("main", str(ledger.path))):
            raise ValueError("condition borrowed transaction differs from the physical original ledger")
        if (require_verified_condition_activation(borrowed_condition.activation, producer_manifest.service_kind.value) != producer
                or require_verified_price_alert_activation(
                    verify_price_role_manifest(producer_manifest, runtime_root=runtime_root),
                    producer_manifest.service_kind.value) != ledger._binding):
            raise ValueError("condition borrowed owner differs from its verified composed producer")
        ledger._verify_installation(source_connection)
        return activation, borrowed_condition, policy
    if borrowed is not None:
        if borrowed.path != settings.ledger_path:
            raise ValueError("condition peer cannot borrow another original ledger")
        ledger = borrowed
    else:
        ledger = ReadonlyPriceAlertRuntimeStore(
            settings.ledger_path,
            activation=verify_price_role_manifest(producer_manifest, runtime_root=runtime_root),
        )
    peer = ConditionAlertRuntimeStore(ledger, activation=producer_activation)
    return activation, peer, policy


def route_condition_role(
    *,
    bus: SignalBusStore,
    peer: ConditionAlertRuntimeStore,
    activation: ConditionAlertRuntimeActivation,
    policy: ConditionAlertRecipientPolicy,
    observed_at: datetime,
    limit: int,
) -> tuple[int, int, int]:
    from rquant.condition_alert_route import _require_condition_history

    source = peer.source_descriptor()
    if (
        source.evaluation_contract_sha256 != condition_evaluation_contract_sha256()
        or source.routing_policy_sha256 != condition_routing_contract_sha256()
    ):
        raise ValueError("condition producer code contract changed before routing")
    with bus._read_snapshot() as connection:
        _require_condition_history(connection)
        row = connection.execute(
            "SELECT last_sequence FROM condition_alert_route_source WHERE source_id=?",
            (source.source_id,),
        ).fetchone()
    last = 0 if row is None else row[0]
    records = peer.events_after(last, inspected_at=observed_at, limit=min(limit, 100))
    for record in records:
        bus.commit_condition_alert_route(
            activation=activation,
            policy=policy,
            source=source,
            record=record,
            source_inspected_at=observed_at,
            routed_at=observed_at,
        )
        last = record.sequence
    return source.high_watermark, last, len(records)


def apply_condition_role_scope(
    store: NotificationStateStore,
    *,
    peer: ConditionAlertRuntimeStore,
    activation: ConditionAlertRuntimeActivation,
    policy: ConditionAlertRecipientPolicy,
    serving_root: Path,
    observed_at: datetime,
) -> None:
    from rquant.web.screen_intraday import read_intraday_screen_snapshot
    from rquant.web.serving import BorrowedGeneration

    now = normalize_aware_utc(observed_at)
    own = require_verified_condition_activation(activation, "notifier")
    source = peer.source_descriptor()
    producer = None
    authority = None
    scopes = []
    try:
        if (
            source.evaluation_contract_sha256 != condition_evaluation_contract_sha256()
            or source.routing_policy_sha256 != condition_routing_contract_sha256()
            or own.evaluation_contract_sha256 != source.evaluation_contract_sha256
            or own.routing_policy_sha256 != source.routing_policy_sha256
        ):
            raise ValueError("condition installed consumer code differs from producer")
        with ServingReader(serving_root).acquire_generation() as lease:
            cursor = lease.connection.cursor()
            try:
                borrowed = BorrowedGeneration(
                    manifest=lease.manifest,
                    pointer=lease.pointer,
                    cursor=cursor,
                    fallback_detail=None,
                )
                authority = read_condition_rule_authority(borrowed, now=now)
                if authority is not None:
                    for row in authority.rows:
                        if row.deleted:
                            continue
                        rule = OwnedConditionAlertRule(
                            owner_id=row.owner_id,
                            version=row.version,
                            rule=row.rule,
                            updated_at=row.updated_at,
                        )
                        try:
                            scope = resolve_condition_scope(
                                borrowed, owner_id=row.owner_id, rule=row.rule, now=now
                            )
                        except Exception:
                            scope = None
                        scopes.append(ConditionDeliveryScope(rule=rule, scope=scope))
                actual = read_intraday_screen_snapshot(borrowed, now=now)
                last = peer.latest_round()
                ready = (
                    last is not None
                    and last.input.source is not None
                    and last.evaluated_at <= now
                    and now - last.evaluated_at <= timedelta(seconds=90)
                    and last.input.source.source_identity == actual.source.source_identity
                    and not actual.source.missing_codes
                    and actual.source.feature_contract_version == 4
                )
                producer = ConditionConsumerProof(
                    producer_manifest_sha256=source.producer_manifest_sha256,
                    notifier_manifest_sha256=own.producer_manifest_sha256,
                    evaluation_contract_sha256=source.evaluation_contract_sha256,
                    routing_contract_sha256=source.routing_policy_sha256,
                    frequency_policy_sha256=source.frequency_policy_sha256,
                    source_epoch=source.source_epoch,
                    producer_generation_id=source.generation_id,
                    source_identity=actual.source.source_identity,
                    serving_generation_id=lease.manifest.generation_id,
                    inspected_at=now,
                    source_cutoff=actual.source.cutoff,
                    full_source_ready=ready,
                    feature_contract_version=actual.source.feature_contract_version,
                )
                if ServingReader(serving_root).current_pointer() != lease.pointer:
                    raise ValueError("condition scope changed during inspection")
            finally:
                cursor.close()
    except Exception:
        producer = None
        # A generation race cannot preserve a partially read set of live heads.
        authority = None
        scopes = []
    builtin, builtin_inspection = None, None
    try:
        from rquant.monitor_builtin_runtime import (
            inspect_original_builtin_delivery, read_installed_builtin_settings,
            read_original_builtin_capture, require_builtin_delivery_inspection,
            verify_builtin_capture_authority,
        )

        installed = read_installed_builtin_settings(peer.activation)
        if installed is not None and installed.enabled:
            if (source.evaluation_contract_sha256 != condition_evaluation_contract_sha256()
                    or source.routing_policy_sha256 != condition_routing_contract_sha256()):
                raise ValueError("builtin original owner code changed")
            captures = []
            for reference in installed.sources:
                try:
                    if reference.producer_commit != peer.binding.producer_commit:
                        raise ValueError("builtin capture has a different installed producer")
                    capture_authority = verify_builtin_capture_authority(reference.manifest_path,
                        runtime_root=reference.runtime_root, expected_sha256=reference.manifest_sha256,
                        expected_commit=reference.producer_commit)
                    captures.append(read_original_builtin_capture(capture_authority, read_at=now))
                except (OSError, ValueError, RuntimeError):
                    pass
            builtin_inspection = inspect_original_builtin_delivery(peer, captured=tuple(captures), inspected_at=now)
            builtin = require_builtin_delivery_inspection(builtin_inspection)
    except (OSError, ValueError, RuntimeError):
        builtin, builtin_inspection = None, None
    value = ConditionAlertDeliveryAuthorityInput(
        rules=authority,
        scopes=tuple(scopes),
        policy=policy,
        producer=producer,
        notifier_manifest_sha256=own.producer_manifest_sha256,
        delivery_enabled=own.delivery_enabled,
        inspected_at=now,
        builtin=builtin,
    )
    current = store.condition_alert_delivery_authority()
    store.apply_condition_alert_delivery_authority(
        value,
        activation=activation,
        expected_revision=0 if current is None else current.authority_revision,
        applied_at=now,
        builtin_inspection=builtin_inspection,
    )
