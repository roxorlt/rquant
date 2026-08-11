"""Durable feature-spool to independent strategy-runner service step."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Protocol
from zoneinfo import ZoneInfo

from pydantic import Field

from rquant.feature_spool import FeatureBatchSpool, FeatureConsumerCursor
from rquant.runtime_candidate_universe import RuntimeCandidateUniverseLoader
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    normalize_aware_utc,
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
) -> StrategyLiveBatchSummary:
    observed = normalize_aware_utc(observed_at)
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
    )


__all__ = [
    "SignalRouteDrainAuthority",
    "StrategyCompletionAttestationConfig",
    "StrategyLiveBatchSummary",
    "run_strategy_live_batch",
]
