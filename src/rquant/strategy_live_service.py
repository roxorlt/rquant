"""Durable feature-spool to independent strategy-runner service step."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime

from pydantic import Field

from rquant.feature_spool import FeatureBatchSpool, FeatureConsumerCursor
from rquant.runtime_candidate_universe import RuntimeCandidateUniverseLoader
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    normalize_aware_utc,
)
from rquant.strategy_candidate_feature_join import join_strategy_candidate_features
from rquant.strategy_candidate_snapshot import asia_shanghai_trade_date
from rquant.strategy_runner import (
    StrategyEvaluator,
    StrategyRunnerStore,
    StrategySourceBatchReceipt,
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


StrategyLiveFaultHook = Callable[[str], None]


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
) -> StrategyLiveBatchSummary:
    observed = normalize_aware_utc(observed_at)
    if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
        raise ValueError("limit must be a positive integer")
    resolved_consumer_id = consumer_id or (
        f"strategy:{runner.spec.strategy_id}:{runner.spec.version}"
    )
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
        runner_signal_high_watermark=runner.signal_high_watermark(),
        has_deferred_batches=last_sequence < descriptor.high_watermark,
    )


__all__ = ["StrategyLiveBatchSummary", "run_strategy_live_batch"]
