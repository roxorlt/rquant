"""Durable feature-spool to independent strategy-runner service step."""

from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass
from contextlib import closing
from datetime import UTC, date, datetime, time
from decimal import Decimal
from time import perf_counter
from typing import Protocol, TYPE_CHECKING
from zoneinfo import ZoneInfo

from pydantic import Field, model_validator

from rquant.feature_spool import FeatureBatchSpool, FeatureConsumerCursor
from rquant.runtime_candidate_universe import RuntimeCandidateUniverseLoader
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.runtime_health_details import (
    RuntimeHealthAsOfValidity,
    RuntimeHealthMetric,
    RuntimeHealthStrategyScope,
)
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.runtime_shadow_validation import CompletionAttestationSigner
from rquant.signal_router_runtime import SignalRouteBacklogError
from rquant.strategy_candidate_feature_join import join_strategy_candidate_features
from rquant.strategy_candidate_snapshot import asia_shanghai_trade_date
from rquant.strategy_runner import (
    RunnerSignalRouteDrainEvidence,
    StrategyEvaluator,
    StrategyRunnerStore,
    StrategySourceBatchReceipt,
)

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_SESSION_CLOSE = time(15, 0)

if TYPE_CHECKING:
    from rquant.paper_research_runtime import NativeMinuteForwardViewSource
    from rquant.paper_portfolio_views import PaperDailyNav


class StrategyHealthBatchFacts(RuntimeContractModel):
    original_batch_id: str
    source_receipt: StrategySourceBatchReceipt
    runner_generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    result_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    event_time: AwareUtcDatetime
    available_at: AwareUtcDatetime
    processing_started_at: AwareUtcDatetime | None = None
    processing_finished_at: AwareUtcDatetime | None = None
    processed_candidates: int = Field(strict=True, ge=0)
    duration_seconds: Decimal | None = Field(default=None, ge=0, allow_inf_nan=False)

    @model_validator(mode="after")
    def actual_receipt_and_interval(self) -> StrategyHealthBatchFacts:
        if self.original_batch_id != self.source_receipt.source_batch_id:
            raise ValueError("strategy health fact is detached from its original source receipt")
        if self.event_time > self.available_at:
            raise ValueError("strategy health fact contains a future source event")
        if self.duration_seconds is None:
            if self.processing_started_at is not None or self.processing_finished_at is not None:
                raise ValueError("replayed strategy fact cannot manufacture a processing interval")
        elif not (
            self.processing_started_at is not None
            and self.processing_finished_at is not None
            and self.event_time
            <= self.processing_started_at
            <= self.processing_finished_at
            == self.available_at
        ):
            raise ValueError("strategy duration requires its actual complete processing interval")
        return self


def strategy_health_metrics(fact: StrategyHealthBatchFacts) -> tuple[RuntimeHealthMetric, ...]:
    scope = RuntimeHealthStrategyScope(
        batch_id=canonical_sha256(fact.source_receipt), processed=True
    )
    common = dict(
        owner_dataset_id="runner_signal",
        source_generation_id=fact.runner_generation_id,
        source_identity=canonical_sha256(fact),
        scope=scope,
        event_time_start=fact.event_time,
        event_time_end=fact.event_time,
        available_at=fact.available_at,
        observed_at=fact.available_at,
    )
    duration_common = common | dict(
        event_time_start=fact.processing_started_at or fact.event_time,
        event_time_end=fact.processing_finished_at or fact.event_time,
    )

    def validity(material: dict[str, object]) -> RuntimeHealthAsOfValidity:
        return RuntimeHealthAsOfValidity(
            basis="batch",
            basis_identity=scope.batch_id,
            as_of=fact.available_at,
            scope_identity=canonical_sha256(scope),
            **{
                key: value for key, value in material.items() if key not in {"scope", "observed_at"}
            },
        )

    return (
        RuntimeHealthMetric(
            metric_id="strategy_duration",
            unit="seconds",
            **duration_common,
            validity=validity(duration_common),
            completeness="complete" if fact.duration_seconds is not None else "unavailable",
            value=fact.duration_seconds,
            verdict="unassessed" if fact.duration_seconds is not None else "unavailable",
            reason_code="actual_strategy_batch"
            if fact.duration_seconds is not None
            else "replayed_duration_unavailable",
        ),
        RuntimeHealthMetric(
            metric_id="strategy_candidates",
            unit="count",
            **common,
            validity=validity(common),
            completeness="complete",
            value=fact.processed_candidates,
            verdict="unassessed",
            reason_code="actual_strategy_batch",
        ),
    )


class StrategyLiveBatchSummary(RuntimeContractModel):
    observed_at: AwareUtcDatetime
    strategy_id: str = Field(min_length=1)
    strategy_version: int = Field(ge=1)
    source_generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_high_watermark: int = Field(ge=-1)
    started_after_sequence: int = Field(ge=-1)
    last_feature_sequence: int = Field(ge=-1)
    processed_count: int = Field(ge=0)
    replayed_count: int = Field(ge=0)
    signal_count: int = Field(ge=0)
    runner_signal_high_watermark: int = Field(ge=0)
    has_deferred_batches: bool
    completion_receipt_id: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    #: How many candidates of the newest feature batch this call handled were not
    #: evaluated because a required feature was not usable (STALE or unavailable); `None`
    #: when the call handled no batch. Since package AI a candidate whose feed went stale
    #: is skipped instead of failing the whole batch, so this is where a held position
    #: whose exits are not being evaluated shows up.
    last_batch_skipped_candidates: int | None = Field(default=None, ge=0)
    last_batch_health: StrategyHealthBatchFacts | None = Field(
        default=None, exclude_if=lambda value: value is None
    )


StrategyLiveFaultHook = Callable[[str], None]


@dataclass(frozen=True)
class StrategyCompletionAttestationConfig:
    signer: CompletionAttestationSigner
    strategy_registration_fingerprint: str
    executable_fingerprint: str
    candidate_schema_fingerprint: str
    feature_registration_fingerprint: str
    feature_contract_fingerprint: str
    producer_manifest_fingerprint: str


class SignalRouteDrainAuthority(Protocol):
    def read_drain_evidence(
        self,
        *,
        source_id: str,
        runner_generation_id: str,
        strategy_spec_fingerprint: str,
        trade_date: date,
        segment_start_sequence: int,
        routed_through_sequence: int,
        observed_at: datetime,
    ) -> RunnerSignalRouteDrainEvidence: ...


def _completion_authority_configured(
    *,
    calendar: MarketCalendarAuthority | None,
    route_authority: SignalRouteDrainAuthority | None,
    completion_source_id: str | None,
    producer_service_id: str | None,
    producer_instance_id: str | None,
    producer_version: str | None,
    completion_attestation: StrategyCompletionAttestationConfig | None,
) -> bool:
    values = (
        calendar,
        route_authority,
        completion_source_id,
        producer_service_id,
        producer_instance_id,
        producer_version,
        completion_attestation,
    )
    configured = tuple(value is not None for value in values)
    if any(configured) and not all(configured):
        raise ValueError("session completion authority must be configured as one complete group")
    return all(configured)


def publish_native_forward_close(
    source: NativeMinuteForwardViewSource, *, observed_at: datetime,
    completion_receipt_id: str | None,
) -> PaperDailyNav | None:
    """Publish after the original runner/router and broker finish the same day."""
    from rquant.paper_research_runtime import NativeMinuteForwardViewSource
    from rquant.paper_signal_worker import PaperSignalQueueStatus
    from rquant.paper_portfolio_ledger import MAX_FULL_ORDERS

    if type(source) is not NativeMinuteForwardViewSource:
        raise TypeError("forward close requires its installed original native source")
    runtime = source.runtime
    runtime.require_original_peers()
    configuration = runtime.state.configuration
    runtime.state.authorize(configuration.target.owner_id)
    observed = normalize_aware_utc(observed_at)
    local = observed.astimezone(_SHANGHAI)
    day = local.date()
    if (day not in runtime.calendar.dates or local.time().replace(tzinfo=None) < _SESSION_CLOSE
        or local.time().replace(tzinfo=None) > time(21)
        or day <= configuration.paper_approved_at.astimezone(_SHANGHAI).date()):
        return None
    if completion_receipt_id is None:
        return None
    receipt = runtime.runner.session_close_receipt(day)
    if receipt is None or receipt.completion_attestation is None:
        raise ValueError("native close lacks its original complete runner/route receipt")
    claims = receipt.completion_attestation.claims
    if (receipt.receipt_id != completion_receipt_id or receipt.produced_at > observed
        or receipt.complete_through < receipt.session_close_at
        or (receipt.source_id, receipt.runner_generation_id, receipt.calendar_generation_id,
            receipt.high_watermark) != (runtime.manifest.service_id, runtime.runner.source_generation_id,
            runtime.market_calendar.content_sha256, runtime.runner.signal_high_watermark())
        or (claims.strategy_id, claims.strategy_version, claims.strategy_spec_fingerprint,
            claims.strategy_registration_fingerprint, claims.producer_manifest_fingerprint)
        != (configuration.target.strategy_id, configuration.target.head.version,
            configuration.target.head.spec_fingerprint, configuration.target.head.registration_fingerprint,
            runtime.manifest.manifest_fingerprint)):
        raise ValueError("native close differs from its exact original completion authority")
    with closing(runtime.queue._connect()) as connection:
        connection.execute("BEGIN")
        if connection.execute("SELECT COUNT(*) FROM paper_signal_queue").fetchone()[0] > MAX_FULL_ORDERS:
            raise ValueError("native forward queue exceeds its complete financial read budget")
        pending = connection.execute("SELECT COUNT(*) FROM paper_signal_queue WHERE status IN (?,?)",
            (PaperSignalQueueStatus.PENDING.value, PaperSignalQueueStatus.PREPARED.value)).fetchone()[0]
    if pending:
        return None
    old = tuple(point for point in source.views.nav_series() if point.trade_date == day)
    if old:
        if old[0].published_at > observed:
            raise ValueError("native close contains a future original NAV publication")
        return old[0]
    return source.record_close(observed_at=observed, published_at=observed)


def run_strategy_live_batch(
    *,
    feature_spool: FeatureBatchSpool,
    candidate_universe_loader: RuntimeCandidateUniverseLoader,
    runner: StrategyRunnerStore,
    evaluator: StrategyEvaluator,
    observed_at: datetime,
    limit: int,
    consumer_id: str | None = None,
    fault_hook: StrategyLiveFaultHook | None = None,
    calendar: MarketCalendarAuthority | None = None,
    route_authority: SignalRouteDrainAuthority | None = None,
    completion_source_id: str | None = None,
    producer_service_id: str | None = None,
    producer_instance_id: str | None = None,
    producer_version: str | None = None,
    completion_attestation: StrategyCompletionAttestationConfig | None = None,
    health_metrics_enabled: bool = False,
    completion_clock: Callable[[], datetime] | None = None,
    monotonic_clock: Callable[[], float] = perf_counter,
    native_forward_source: NativeMinuteForwardViewSource | None = None,
) -> StrategyLiveBatchSummary:
    if type(health_metrics_enabled) is not bool:
        raise TypeError("strategy health opt-in must be bool")
    observed = normalize_aware_utc(observed_at)
    if native_forward_source is not None:
        from rquant.paper_research_runtime import NativeMinuteForwardViewSource
        if type(native_forward_source) is not NativeMinuteForwardViewSource or native_forward_source.runtime.runner is not runner:
            raise TypeError("strategy execution requires its exact original native forward runner")
        native_forward_source.runtime.require_original_peers()
        native_forward_source.runtime.state.authorize(native_forward_source.runtime.state.configuration.target.owner_id)
    if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
        raise ValueError("limit must be a positive integer")
    resolved_consumer_id = consumer_id or (
        f"strategy:{runner.spec.strategy_id}:{runner.spec.version}"
    )
    completion_configured = _completion_authority_configured(
        calendar=calendar,
        route_authority=route_authority,
        completion_source_id=completion_source_id,
        producer_service_id=producer_service_id,
        producer_instance_id=producer_instance_id,
        producer_version=producer_version,
        completion_attestation=completion_attestation,
    )
    if completion_configured:
        assert calendar is not None
        if calendar.generated_at > observed:
            raise ValueError("completion calendar was generated after observed_at")
    descriptor = feature_spool.source_descriptor()
    cursor = feature_spool.load_cursor(resolved_consumer_id)
    started_after = -1 if cursor is None else cursor.last_sequence
    records = feature_spool.list_after(
        sequence=started_after,
        through_sequence=descriptor.high_watermark,
        limit=limit,
    )

    processed = 0
    replayed = 0
    signal_count = 0
    last_batch_skipped: int | None = None
    last_batch_health: StrategyHealthBatchFacts | None = None
    last_sequence = started_after
    for record in records:
        envelope = record.envelope
        if envelope.available_at > observed:
            break
        source_receipt = StrategySourceBatchReceipt(
            source_generation_id=descriptor.generation_id,
            source_sequence=envelope.sequence,
            source_batch_id=envelope.batch_id,
            source_content_hash=envelope.content_hash,
        )
        result = runner.replay_source_batch(
            source_receipt,
            observed_at=observed,
        )
        was_processed = result is not None
        processing_started = None
        if health_metrics_enabled and not was_processed:
            processing_started = normalize_aware_utc(
                (completion_clock or (lambda: datetime.now(UTC)))()
            )
            if processing_started < observed:
                raise ValueError("strategy processing clock precedes its original observation")
        measurement_start = (
            monotonic_clock() if health_metrics_enabled and not was_processed else None
        )
        if result is None:
            stored = feature_spool.read_result(record)
            frame = stored.frame
            if frame.empty:
                required_columns = ["ts_code", *(item.name for item in envelope.field_statuses)]
                frame = frame.reindex(columns=tuple(dict.fromkeys(required_columns)))
            universe = candidate_universe_loader.load(
                as_of=envelope.available_at,
                required_trade_date=asia_shanghai_trade_date(envelope.event_time),
            )
            joined = join_strategy_candidate_features(
                envelope,
                frame,
                universe,
                runner.spec.strategy_id,
                str(runner.spec.version),
            )
            result = runner.process_batch(
                joined.envelope,
                joined.frame,
                feature_payload=joined.payload_bytes,
                source_receipt=source_receipt,
                dataset_snapshot_id=joined.envelope.input_fingerprint,
                observed_at=observed,
                evaluator=evaluator,
            )
        if health_metrics_enabled:
            duration = None
            finished = observed
            if not was_processed:
                measurement_end = monotonic_clock()
                if measurement_start is None or not (
                    math.isfinite(measurement_start)
                    and math.isfinite(measurement_end)
                    and measurement_start <= measurement_end
                ):
                    raise ValueError("strategy measurement requires an ordered monotonic interval")
                duration = Decimal(str(measurement_end - measurement_start))
                finished = normalize_aware_utc((completion_clock or (lambda: datetime.now(UTC)))())
                if finished < processing_started:
                    raise ValueError("strategy completion precedes its processing start")
            last_batch_health = StrategyHealthBatchFacts(
                original_batch_id=envelope.batch_id,
                source_receipt=source_receipt,
                runner_generation_id=runner.source_generation_id,
                result_identity=canonical_sha256(result),
                event_time=envelope.event_time,
                available_at=finished,
                processing_started_at=processing_started,
                processing_finished_at=None if was_processed else finished,
                processed_candidates=result.processed_candidates,
                duration_seconds=duration,
            )
        if fault_hook is not None:
            fault_hook("after_runner_commit")
        feature_spool.commit_cursor(
            FeatureConsumerCursor(
                consumer_id=resolved_consumer_id,
                source_generation_id=descriptor.generation_id,
                last_sequence=envelope.sequence,
                last_batch_id=envelope.batch_id,
                last_content_hash=envelope.content_hash,
                updated_at=observed,
            )
        )
        processed += 1
        replayed += int(was_processed)
        if not was_processed:
            signal_count += len(result.signals)
        last_batch_skipped = result.skipped_candidates
        last_sequence = envelope.sequence

    runner_signal_high_watermark = runner.signal_high_watermark()
    has_deferred_batches = last_sequence < descriptor.high_watermark
    completion_receipt_id: str | None = None
    if completion_configured and not has_deferred_batches:
        assert calendar is not None
        assert route_authority is not None
        assert completion_source_id is not None
        assert producer_service_id is not None
        assert producer_instance_id is not None
        assert producer_version is not None
        assert completion_attestation is not None
        local_observed = observed.astimezone(_SHANGHAI)
        trade_date = local_observed.date()
        calendar_covers_date = calendar.coverage_start <= trade_date <= calendar.coverage_end
        if calendar_covers_date and trade_date in calendar.open_dates:
            session_close = datetime.combine(
                trade_date,
                _SESSION_CLOSE,
                tzinfo=_SHANGHAI,
            ).astimezone(observed.tzinfo)
            if observed >= session_close:
                feature_marker = feature_spool.session_close_marker(trade_date)
                if feature_marker is not None:
                    if (
                        feature_marker.source_generation_id != descriptor.generation_id
                        or feature_marker.calendar_generation_id != calendar.content_sha256
                        or feature_marker.final_sequence != last_sequence
                        or feature_marker.produced_at > observed
                    ):
                        raise ValueError("feature close marker does not match the consumed session")
                    try:
                        segment_start, segment_final = runner.runner_session_route_bounds(
                            trade_date
                        )
                        if segment_final != runner_signal_high_watermark:
                            raise ValueError(
                                "runner session segment does not reach its signal watermark"
                            )
                        route_evidence = route_authority.read_drain_evidence(
                            source_id=completion_source_id,
                            runner_generation_id=runner.source_generation_id,
                            strategy_spec_fingerprint=runner.spec.spec_fingerprint,
                            trade_date=trade_date,
                            segment_start_sequence=segment_start,
                            routed_through_sequence=runner_signal_high_watermark,
                            observed_at=observed,
                        )
                    except SignalRouteBacklogError:
                        pass
                    else:
                        receipt = runner.publish_session_close_receipt(
                            trade_date=trade_date,
                            session_close_at=session_close,
                            source_id=completion_source_id,
                            calendar_generation_id=calendar.content_sha256,
                            producer_service_id=producer_service_id,
                            producer_instance_id=producer_instance_id,
                            producer_version=producer_version,
                            produced_at=observed,
                            feature_close_marker=feature_marker,
                            attestation_signer=completion_attestation.signer,
                            strategy_registration_fingerprint=(
                                completion_attestation.strategy_registration_fingerprint
                            ),
                            executable_fingerprint=(completion_attestation.executable_fingerprint),
                            candidate_schema_fingerprint=(
                                completion_attestation.candidate_schema_fingerprint
                            ),
                            feature_registration_fingerprint=(
                                completion_attestation.feature_registration_fingerprint
                            ),
                            feature_contract_fingerprint=(
                                completion_attestation.feature_contract_fingerprint
                            ),
                            producer_manifest_fingerprint=(
                                completion_attestation.producer_manifest_fingerprint
                            ),
                            route_evidence=route_evidence,
                            fault_hook=fault_hook,
                        )
                        completion_receipt_id = receipt.receipt_id

    if native_forward_source is not None and not has_deferred_batches:
        publish_native_forward_close(native_forward_source, observed_at=observed,
            completion_receipt_id=completion_receipt_id)

    return StrategyLiveBatchSummary(
        observed_at=observed,
        strategy_id=runner.spec.strategy_id,
        strategy_version=runner.spec.version,
        source_generation_id=descriptor.generation_id,
        source_high_watermark=descriptor.high_watermark,
        started_after_sequence=started_after,
        last_feature_sequence=last_sequence,
        processed_count=processed,
        replayed_count=replayed,
        signal_count=signal_count,
        runner_signal_high_watermark=runner_signal_high_watermark,
        has_deferred_batches=has_deferred_batches,
        completion_receipt_id=completion_receipt_id,
        last_batch_skipped_candidates=last_batch_skipped,
        last_batch_health=last_batch_health,
    )


__all__ = [
    "SignalRouteDrainAuthority",
    "StrategyCompletionAttestationConfig",
    "StrategyLiveBatchSummary",
    "run_strategy_live_batch",
]
