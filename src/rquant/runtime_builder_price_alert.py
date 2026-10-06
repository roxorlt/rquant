"""Trusted first-price runtime over the original Serving, gateway and event bus."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import Field, ValidationError, field_validator

from rquant.condition_alert_runtime_contracts import ConditionAlertActivationSettings
from rquant.live_contracts import CurrentPointer, LiveChannel
from rquant.live_spool import LiveBatchSpool, _secure_read_regular_file
from rquant.price_alert_runtime import evaluate_price_alert_round
from rquant.price_alert_runtime_contracts import (
    PriceAlertActivationSettings,
    PriceAlertCapacityExceeded,
    PriceAlertFrequencyPolicy,
    PriceAlertRuntimeActivation,
    _activation_bytes,
    require_price_alert_activation,
    verify_price_alert_activation,
)
from rquant.price_alert_runtime_source import (
    PriceAlertScopeSnapshot,
    PriceQuoteRequestBinding,
    read_latest_price_quote_snapshot,
    read_price_alert_scope,
)
from rquant.price_alert_runtime_store import PriceAlertRuntimeStore
from rquant.runtime_builder_condition_alert import (
    ConditionEvaluationSettings,
    build_borrowed_condition_step,
)
from rquant.runtime_contracts import RuntimeContractModel, normalize_aware_utc
from rquant.runtime_market_session import load_market_calendar_authority
from rquant.runtime_service_control import RuntimeServicePlane, RuntimeStepResult
from rquant.runtime_service_entrypoint import (
    RuntimeServiceBuilder,
    RuntimeServiceKind,
    RuntimeServiceManifest,
    RuntimeServiceStep,
    load_runtime_service_manifest,
)
from rquant.serving_publisher import ServingReader
from rquant.strict_json import canonical_json_bytes

if TYPE_CHECKING:
    from rquant.notification_state import NotificationStateStore
    from rquant.price_alert_route import PriceAlertRecipientPolicy
    from rquant.price_alert_runtime_store import ReadonlyPriceAlertRuntimeStore
    from rquant.signal_bus import SignalBusStore


def _code_contract(label: str, filenames: tuple[str, ...]) -> str:
    root = Path(__file__).parent
    return sha256(
        canonical_json_bytes(
            {
                "contract": label,
                "sources": {
                    filename: sha256((root / filename).read_bytes()).hexdigest()
                    for filename in filenames
                },
            }
        )
    ).hexdigest()


def price_evaluation_contract_sha256() -> str:
    return _code_contract(
        "price-alert-evaluation/v1",
        ("alert_price_rule.py", "price_alert_runtime.py", "price_alert_runtime_contracts.py"),
    )


def price_routing_contract_sha256() -> str:
    return _code_contract(
        "price-alert-routing/v1", ("price_alert_route.py", "delivery_contracts.py")
    )


def verify_price_role_manifest(
    manifest: RuntimeServiceManifest, *, runtime_root: Path
) -> PriceAlertRuntimeActivation:
    path = manifest.settings.get("price_alert_runtime_manifest_path")
    if not isinstance(path, str):
        raise ValueError("price runtime lacks its actual frozen manifest path")
    actual_path = Path(path)
    payload = _activation_bytes(actual_path, runtime_root)
    actual = load_runtime_service_manifest(actual_path, expected_commit=manifest.producer_commit)
    if actual != manifest:
        raise ValueError("price runtime model differs from its actual frozen role manifest")
    activation = verify_price_alert_activation(
        actual_path,
        runtime_root=runtime_root,
        expected_manifest_sha256=sha256(payload).hexdigest(),
        expected_commit=manifest.producer_commit,
        expected_kind=manifest.service_kind,
    )
    from rquant.price_alert_runtime_contracts import require_verified_price_alert_activation

    binding = require_verified_price_alert_activation(activation, manifest.service_kind.value)
    if (
        binding.evaluation_contract_sha256 != price_evaluation_contract_sha256()
        or binding.routing_policy_sha256 != price_routing_contract_sha256()
    ):
        raise ValueError("price runtime evaluation or routing code contract changed")
    return activation


class PriceAlertRuntimeSettings(RuntimeContractModel):
    condition_alert_runtime: ConditionAlertActivationSettings | None = None
    condition_alert_runtime_manifest_path: Path | None = None
    condition_alert: ConditionEvaluationSettings | None = None
    price_alert_runtime: PriceAlertActivationSettings
    price_alert_runtime_manifest_path: Path
    ledger_path: Path
    scope_serving_root: Path
    quote_spool_root: Path
    quote_request_root: Path
    quote_expected_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    calendar_path: Path
    calendar_expected_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    calendar_content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    frequency_policy: PriceAlertFrequencyPolicy = Field(default_factory=PriceAlertFrequencyPolicy)

    @field_validator(
        "price_alert_runtime_manifest_path",
        "ledger_path",
        "scope_serving_root",
        "quote_spool_root",
        "quote_request_root",
        "calendar_path",
    )
    @classmethod
    def absolute_path(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("price runtime paths must be absolute")
        return value


def latest_price_request(spool: LiveBatchSpool, request_root: Path) -> PriceQuoteRequestBinding:
    from rquant.live_contracts import BatchEnvelope

    pointer = CurrentPointer.model_validate_json(
        _secure_read_regular_file(
            spool._current_path(LiveChannel.WATCHLIST_QUOTE),
            label="price quote pointer",
            max_bytes=64 * 1024,
        )
    )
    envelope = BatchEnvelope.model_validate_json(
        _secure_read_regular_file(
            spool._manifest_path(LiveChannel.WATCHLIST_QUOTE, pointer.sequence),
            label="price quote batch",
            max_bytes=64 * 1024,
        )
    )
    if envelope.source_request_id is None or envelope.batch_id != pointer.batch_id:
        raise ValueError("price quote has no original named request")
    return PriceQuoteRequestBinding.model_validate_json(
        _activation_bytes(request_root / f"{envelope.source_request_id}.json", request_root)
    )


def price_alert_runtime_builder(
    *, clock: Callable[[], datetime], runtime_root: Path | None
) -> RuntimeServiceBuilder:
    def build(manifest: RuntimeServiceManifest) -> RuntimeServiceStep:
        if (
            manifest.service_kind is not RuntimeServiceKind.PRICE_ALERT_RUNTIME
            or manifest.plane is not RuntimeServicePlane.LIVE
            or manifest.interval_seconds != 5
            or runtime_root is None
        ):
            raise ValueError("price runtime requires the five-second actual live role")
        activation = verify_price_role_manifest(manifest, runtime_root=runtime_root)
        binding = require_price_alert_activation(activation, "evaluation")
        settings = PriceAlertRuntimeSettings.model_validate(
            manifest.model_dump(mode="python")["settings"]
        )
        if settings.frequency_policy.sha256 != binding.frequency_policy_sha256:
            raise ValueError("price runtime cooldown policy differs from its actual contract")
        for path in (
            settings.ledger_path,
            settings.quote_request_root,
            settings.price_alert_runtime_manifest_path,
        ):
            if not path.is_relative_to(runtime_root):
                raise ValueError("price runtime owned artifacts must stay in its private root")
        calendar = load_market_calendar_authority(
            settings.calendar_path, expected_commit=settings.calendar_expected_commit
        )
        if calendar.content_sha256 != settings.calendar_content_sha256:
            raise ValueError("price runtime calendar content differs from its configured authority")
        spool = LiveBatchSpool(settings.quote_spool_root, read_only=True)
        store = PriceAlertRuntimeStore(settings.ledger_path, activation=activation)
        last_at: datetime | None = None

        def step() -> RuntimeStepResult:
            nonlocal last_at
            now = normalize_aware_utc(clock())
            require_price_alert_activation(activation, "evaluation")
            if last_at is not None and now < last_at:
                raise ValueError("price runtime loop clock regressed")
            if last_at is not None and now - last_at < timedelta(seconds=5):
                return RuntimeStepResult(
                    output_sequence=store.source_descriptor().high_watermark,
                    degraded_reasons=("price_alert:waiting_cadence",),
                )
            last_at = now
            try:
                reader = ServingReader(settings.scope_serving_root)
                lease = reader.acquire_generation()
            except (OSError, ValueError, RuntimeError):
                receipt = store.record_unavailable(evaluated_at=now, reason="scope_unavailable")
                return RuntimeStepResult(
                    output_sequence=receipt.source_high_watermark,
                    degraded_reasons=("price_alert:scope_unavailable",),
                )
            with lease:
                scope = read_price_alert_scope(lease, evaluated_at=now)
                if type(scope) is not PriceAlertScopeSnapshot:
                    receipt = store.record_unavailable(evaluated_at=now, reason=scope.reason)
                    return RuntimeStepResult(
                        output_sequence=receipt.source_high_watermark,
                        degraded_reasons=(f"price_alert:{scope.reason}",),
                    )
                quotes = None
                if scope.codes:
                    try:
                        request = latest_price_request(spool, settings.quote_request_root)
                        if (
                            request.scope_generation_id,
                            request.scope_manifest_sha256,
                            request.codes,
                        ) != (scope.generation_id, scope.manifest_sha256, scope.codes):
                            raise ValueError("price quotes came from another scope generation")
                        quotes = read_latest_price_quote_snapshot(
                            spool,
                            request_root=settings.quote_request_root,
                            binding=request,
                            evaluated_at=now,
                            expected_producer_commit=settings.quote_expected_commit,
                        )
                    except (OSError, ValueError, RuntimeError):
                        quotes = None
                # Re-read the configured calendar bytes on this pass, not a guessed open day.
                actual_calendar = load_market_calendar_authority(
                    settings.calendar_path, expected_commit=settings.calendar_expected_commit
                )
                if actual_calendar.content_sha256 != calendar.content_sha256:
                    raise ValueError("price runtime calendar changed during its role lifetime")
                try:
                    value = evaluate_price_alert_round(
                        activation=activation,
                        scope=scope,
                        quotes=quotes,
                        calendar=actual_calendar,
                        evaluated_at=now,
                        policy=settings.frequency_policy,
                    )
                    receipt = store.commit_round(
                        value,
                        policy=settings.frequency_policy,
                        current_scope=lambda: reader.current_pointer() == lease.pointer,
                    )
                except ValueError as exc:
                    capacity = type(exc) is PriceAlertCapacityExceeded or (
                        isinstance(exc, ValidationError)
                        and any(
                            type(item.get("ctx", {}).get("error")) is PriceAlertCapacityExceeded
                            for item in exc.errors(include_input=False)
                        )
                    )
                    if not capacity:
                        raise
                    receipt = store.record_unavailable(evaluated_at=now, reason="capacity_exceeded")
                    return RuntimeStepResult(
                        output_sequence=receipt.source_high_watermark,
                        degraded_reasons=("price_alert:capacity_exceeded",),
                    )
                degraded = tuple(
                    sorted(
                        {
                            f"price_alert:{row.reason}"
                            for row in value.records
                            if row.state == "unavailable"
                        }
                    )
                )
                return RuntimeStepResult(
                    input_sequence=scope.source_sequence,
                    output_sequence=receipt.source_high_watermark,
                    processed_count=receipt.decision_count,
                    source_generations={
                        "price_alert_source": binding.generation_id,
                        "price_alert_scope": scope.generation_id,
                        "market_calendar": calendar.content_sha256,
                    },
                    degraded_reasons=degraded,
                )

        if settings.condition_alert is not None:
            if (
                settings.condition_alert_runtime is None
                or settings.condition_alert_runtime_manifest_path is None
            ):
                store.close()
                raise ValueError("condition composition requires its full actual manifest")
            try:
                condition_step = build_borrowed_condition_step(
                    manifest,
                    settings.condition_alert,
                    store,
                    runtime_root=runtime_root,
                    clock=clock,
                )
            except BaseException:
                store.close()
                raise
            original_step = step

            def combined_step() -> RuntimeStepResult:
                result = original_step()
                if "price_alert:waiting_cadence" in result.degraded_reasons:
                    return result
                try:
                    condition_step()
                except Exception:
                    return result.model_copy(
                        update={
                            "degraded_reasons": tuple(
                                sorted(
                                    set(
                                        result.degraded_reasons
                                        + ("condition_alert:evaluation_unavailable",)
                                    )
                                )
                            )
                        }
                    )
                return result

            combined_step.condition_store = condition_step.condition_store
            step = combined_step
        elif (
            settings.condition_alert_runtime is not None
            or settings.condition_alert_runtime_manifest_path is not None
        ):
            store.close()
            raise ValueError("partial condition composition is unavailable")
        step.store = store
        step.close = store.close
        return step

    return build


class PriceAlertPeerSettings(RuntimeContractModel):
    producer_manifest_path: Path
    ledger_path: Path
    recipient_policy_path: Path
    scope_serving_root: Path

    @field_validator(
        "producer_manifest_path", "ledger_path", "recipient_policy_path", "scope_serving_root"
    )
    @classmethod
    def private_absolute_path(cls, value: Path) -> Path:
        if not value.is_absolute() or ".." in value.parts:
            raise ValueError("price peer path must be absolute and canonical")
        return value


def open_price_role_peer(
    manifest: RuntimeServiceManifest, settings: PriceAlertPeerSettings, *, runtime_root: Path | None
) -> tuple[PriceAlertRuntimeActivation, ReadonlyPriceAlertRuntimeStore, PriceAlertRecipientPolicy]:
    from rquant.price_alert_route import PriceAlertRecipientPolicy
    from rquant.price_alert_runtime_contracts import require_verified_price_alert_activation
    from rquant.price_alert_runtime_store import ReadonlyPriceAlertRuntimeStore

    if runtime_root is None or manifest.service_kind not in {
        RuntimeServiceKind.SIGNAL_ROUTER,
        RuntimeServiceKind.NOTIFIER,
    }:
        raise ValueError("price peer requires the actual private router or notifier role")
    activation = verify_price_role_manifest(manifest, runtime_root=runtime_root)
    own = require_verified_price_alert_activation(activation, manifest.service_kind.value)
    producer_manifest = load_runtime_service_manifest(
        settings.producer_manifest_path, expected_commit=manifest.producer_commit
    )
    if producer_manifest.service_kind is not RuntimeServiceKind.PRICE_ALERT_RUNTIME:
        raise ValueError("price peer is not the registered producer role")
    producer_activation = verify_price_role_manifest(producer_manifest, runtime_root=runtime_root)
    producer = require_verified_price_alert_activation(producer_activation, "price_alert_runtime")
    names = (
        "source_id",
        "ledger_id",
        "source_epoch",
        "generation_id",
        "evaluation_contract_sha256",
        "frequency_policy_sha256",
        "routing_policy_sha256",
    )
    if any(getattr(own, name) != getattr(producer, name) for name in names):
        raise ValueError("price consumer and actual producer contracts differ")
    policy = PriceAlertRecipientPolicy.model_validate_json(
        _activation_bytes(settings.recipient_policy_path, runtime_root)
    )
    if policy.sha256 != own.recipient_policy_sha256:
        raise ValueError("price recipient policy differs from the actual role manifest")
    peer = ReadonlyPriceAlertRuntimeStore(settings.ledger_path, activation=producer_activation)
    return activation, peer, policy


def apply_price_role_scope(
    store: NotificationStateStore,
    *,
    activation: PriceAlertRuntimeActivation,
    policy: PriceAlertRecipientPolicy,
    serving_root: Path,
    observed_at: datetime,
) -> None:
    from rquant.price_alert_runtime_contracts import require_verified_price_alert_activation
    from rquant.price_alert_runtime_projection import PriceAlertDeliveryAuthorityInput
    from rquant.price_alert_runtime_source import UnavailablePriceAlertScope

    binding = require_verified_price_alert_activation(activation, "notifier")
    now = normalize_aware_utc(observed_at)
    try:
        with ServingReader(serving_root).acquire_generation() as lease:
            scope = read_price_alert_scope(lease, evaluated_at=now)
    except (OSError, ValueError, RuntimeError):
        scope = UnavailablePriceAlertScope(inspected_at=now, reason="scope_unavailable")
    old = store.price_alert_delivery_authority()
    value = PriceAlertDeliveryAuthorityInput(
        scope=scope,
        policy=policy,
        owner_policy_manifest_sha256=binding.producer_manifest_sha256,
        delivery_enabled=binding.delivery_enabled,
        inspected_at=now,
    )
    store.apply_price_alert_delivery_authority(
        value,
        activation=activation,
        expected_revision=0 if old is None else old.authority_revision,
        applied_at=now,
    )


def route_price_role(
    bus: SignalBusStore,
    *,
    peer: ReadonlyPriceAlertRuntimeStore,
    activation: PriceAlertRuntimeActivation,
    policy: PriceAlertRecipientPolicy,
    observed_at: datetime,
    limit: int,
) -> tuple[int, int, int]:
    from rquant.price_alert_route import _history_installed
    from rquant.price_alert_runtime_contracts import require_verified_price_alert_activation

    binding = require_verified_price_alert_activation(activation, "signal_router")
    if not binding.routing_enabled:
        return 0, 0, 0
    source = peer.source_descriptor()
    with bus._read_snapshot() as connection:
        _history_installed(connection)
        row = connection.execute(
            "SELECT last_sequence FROM price_alert_route_source WHERE source_id=?",
            (source.source_id,),
        ).fetchone()
        last = source.first_sequence - 1 if row is None else row[0]
    records = (
        peer.events_after(last, inspected_at=observed_at, limit=min(limit, 100)) if limit else ()
    )
    for record in records:
        bus.commit_price_alert_route(
            activation=activation,
            policy=policy,
            source=source,
            record=record,
            source_inspected_at=observed_at,
            routed_at=observed_at,
        )
        last = record.sequence
    return source.high_watermark, last, len(records)
