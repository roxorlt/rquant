"""Summary and error SignalEnvelope producer for the isolated daily-close DAG."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from datetime import date, datetime, timedelta
from typing import Literal, Self

from pydantic import Field, model_validator

from rquant.daily_canonical_publisher import DailyCanonicalPublishReceipt
from rquant.daily_close_candidate import DailyCloseCandidateFence, DailyCloseCandidateFenceGuard
from rquant.daily_ledger_fence import DailyLedgerFenceGuard
from rquant.daily_notification_producer import DailyNotificationProducer
from rquant.daily_pipeline_ledger import (
    DailyPipelineLedger,
    DailyPipelineLedgerError,
    DailyStageAttempt,
    DailyWriterLease,
    StageResult,
)
from rquant.daily_pool_stage import (
    DailyDownstreamArtifactStore,
    DailyDownstreamStageError,
    DailyPoolStageArtifact,
    DailyScreenStageArtifact,
    assert_current_canonical_receipt,
)
from rquant.delivery_contracts import DeliveryTarget
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.signal_bus import SignalBusStore
from rquant.signal_contracts import SignalAction, SignalEnvelope
from rquant.storage.duckdb import DuckDBStore


class DailySummaryStageError(RuntimeError):
    """Summary evidence or its durable outbox binding is invalid."""


class DailySummaryStageArtifact(RuntimeContractModel):
    contract: Literal["daily-summary-stage/v1"] = "daily-summary-stage/v1"
    canonical_receipt_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    canonical_generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    summary_signal_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    summary_outbox_ids: tuple[str, ...] = ()
    error_signal_ids: tuple[str, ...] = ()
    error_outbox_ids: tuple[str, ...] = ()
    stage_result: StageResult
    created_at: AwareUtcDatetime

    @model_validator(mode="after")
    def validate_errors(self) -> Self:
        for values, label in (
            (self.error_signal_ids, "summary error signal ids"),
            (self.summary_outbox_ids, "summary outbox ids"),
            (self.error_outbox_ids, "summary error outbox ids"),
        ):
            if len(values) != len(set(values)):
                raise ValueError(f"{label} must be unique")
        return self


class DailySummaryStage:
    expected_stage_id = "summary"

    def __init__(
        self,
        *,
        signal_bus: SignalBusStore,
        strategy_version: str,
        producer_commit: str,
        clock: Callable[[], datetime],
        artifact_store: DailyDownstreamArtifactStore,
        canonical_reader_factory: Callable[[], DuckDBStore],
        signal_ttl: timedelta = timedelta(days=7),
        notification_targets: tuple[DeliveryTarget, ...] = (),
        ledger_fence_verifier: DailyCloseCandidateFenceGuard | None = None,
    ) -> None:
        if not strategy_version:
            raise ValueError("strategy_version must not be empty")
        if len(producer_commit) != 40:
            raise ValueError("producer_commit must be a commit sha")
        if signal_ttl <= timedelta(0):
            raise ValueError("signal_ttl must be positive")
        self._strategy_version = strategy_version
        self._producer_commit = producer_commit
        self._clock = clock
        self._artifact_store = artifact_store
        self._signal_ttl = signal_ttl
        self._canonical_reader_factory = canonical_reader_factory
        self._ledger_fence_verifier = ledger_fence_verifier
        self._notification_producer = DailyNotificationProducer(
            signal_bus=signal_bus,
            targets=notification_targets,
        )

    @classmethod
    def from_ledger(
        cls,
        *,
        signal_bus: SignalBusStore,
        strategy_version: str,
        producer_commit: str,
        clock: Callable[[], datetime],
        artifact_store: DailyDownstreamArtifactStore,
        canonical_reader_factory: Callable[[], DuckDBStore],
        ledger: DailyPipelineLedger,
        lease: DailyWriterLease,
        signal_ttl: timedelta = timedelta(days=7),
        notification_targets: tuple[DeliveryTarget, ...] = (),
    ) -> DailySummaryStage:
        """Construct the daily summary stage with the formal ledger fence adapter."""
        return cls(
            signal_bus=signal_bus,
            strategy_version=strategy_version,
            producer_commit=producer_commit,
            clock=clock,
            artifact_store=artifact_store,
            canonical_reader_factory=canonical_reader_factory,
            signal_ttl=signal_ttl,
            notification_targets=notification_targets,
            ledger_fence_verifier=DailyLedgerFenceGuard(ledger=ledger, lease=lease),
        )

    def build_signal(
        self,
        *,
        trade_date: date,
        canonical_generation_id: str,
        canonical_receipt_id: str,
        canonical_content_hash: str,
        screen_hits: dict[str, int],
        pool2_active_count: int,
        errors: tuple[str, ...],
        event_time: datetime | None = None,
    ) -> SignalEnvelope:
        now = normalize_aware_utc(event_time) if event_time is not None else self._now()
        return SignalEnvelope(
            schema_version=1,
            strategy_id="daily-close-summary",
            strategy_version=self._strategy_version,
            parameter_fingerprint=canonical_sha256(
                {
                    "contract": "daily-summary-parameters/v1",
                    "strategy_version": self._strategy_version,
                }
            ),
            dataset_snapshot_id=canonical_generation_id,
            feature_snapshot_id=canonical_content_hash,
            event_time=now,
            available_at=now,
            candidate_id=f"daily-summary:{trade_date.isoformat()}",
            action=SignalAction.WATCH,
            reason_codes=("daily_summary",),
            evidence={
                "canonical_receipt_id": canonical_receipt_id,
                "trade_date": trade_date.isoformat(),
                "screen_hits": dict(sorted(screen_hits.items())),
                "pool2_active_count": pool2_active_count,
                "errors": list(sorted(errors)),
            },
            expires_at=now + self._signal_ttl,
            producer_commit=self._producer_commit,
        )

    def run(
        self,
        canonical: DailyCanonicalPublishReceipt,
        *,
        screen_result: DailyScreenStageArtifact,
        pool_result: DailyPoolStageArtifact,
        attempt: DailyStageAttempt,
        ledger_fence_verifier: DailyCloseCandidateFenceGuard | None = None,
        ledger_input_identity: str,
    ) -> DailySummaryStageArtifact:
        canonical = DailyCanonicalPublishReceipt.model_validate(canonical)
        screen_result = DailyScreenStageArtifact.model_validate(screen_result)
        pool_result = DailyPoolStageArtifact.model_validate(pool_result)
        self._assert_artifacts(canonical, screen_result, pool_result)
        self._assert_persisted_artifacts(canonical, screen_result, pool_result)
        if attempt.stage_id != self.expected_stage_id:
            raise DailySummaryStageError("daily summary ledger stage does not match")
        now = self._now()
        if not attempt.claimed_at <= now < attempt.lease_expires_at:
            raise DailySummaryStageError("daily summary stage lease is invalid")
        fence_verifier = ledger_fence_verifier or self._ledger_fence_verifier
        if fence_verifier is None:
            raise DailySummaryStageError("daily summary ledger fence is required")
        try:
            context = fence_verifier(attempt, now)
            with context as fence:
                self._assert_fence(fence, canonical, ledger_input_identity)
                self._assert_canonical_current(canonical, now)
                signal = self.build_signal(
                    trade_date=canonical.trade_date,
                    canonical_generation_id=canonical.generation_id,
                    canonical_receipt_id=canonical.receipt_id,
                    canonical_content_hash=canonical.db_content_sha256,
                    screen_hits=dict(screen_result.preset_hits),
                    pool2_active_count=pool_result.pool2_active_count,
                    errors=tuple(sorted((*screen_result.errors, *pool_result.errors))),
                    event_time=canonical.available_at,
                )
                summary_receipt = self._notification_producer.emit(signal, received_at=now)
                error_receipts = tuple(
                    self._notification_producer.emit(error, received_at=now)
                    for error in self._error_signals(
                        canonical,
                        tuple(sorted((*screen_result.errors, *pool_result.errors))),
                        canonical.available_at,
                    )
                )
                self._assert_fence(fence, canonical, ledger_input_identity)
                self._assert_canonical_current(canonical, self._now())
        except DailyPipelineLedgerError as exc:
            raise DailySummaryStageError("daily summary ledger fence rejected stage") from exc
        if summary_receipt.signal_id != signal.signal_id:
            raise DailySummaryStageError("daily summary signal receipt binding changed")
        payload = {
            "summary_signal_id": signal.signal_id,
            "summary_outbox_ids": summary_receipt.outbox_ids,
            "error_signal_ids": tuple(receipt.signal_id for receipt in error_receipts),
            "error_outbox_ids": tuple(
                outbox_id for receipt in error_receipts for outbox_id in receipt.outbox_ids
            ),
            "screen_artifact_id": screen_result.artifact_id,
            "pool_artifact_id": pool_result.artifact_id,
        }
        return DailySummaryStageArtifact(
            canonical_receipt_id=canonical.receipt_id,
            canonical_generation_id=canonical.generation_id,
            summary_signal_id=signal.signal_id,
            summary_outbox_ids=summary_receipt.outbox_ids,
            error_signal_ids=payload["error_signal_ids"],
            error_outbox_ids=payload["error_outbox_ids"],
            stage_result=StageResult(
                content_hash=canonical_sha256(payload),
                evidence_hash=canonical_sha256(
                    {"canonical_receipt_id": canonical.receipt_id, "payload": payload}
                ),
            ),
            created_at=now,
        )

    def _assert_canonical_current(
        self,
        canonical: DailyCanonicalPublishReceipt,
        checked_at: datetime,
    ) -> None:
        try:
            with self._canonical_reader_factory() as store:
                assert_current_canonical_receipt(store, canonical, checked_at)
        except DailyDownstreamStageError as exc:
            raise DailySummaryStageError("daily summary canonical receipt is stale") from exc

    def _error_signals(
        self,
        canonical: DailyCanonicalPublishReceipt,
        errors: tuple[str, ...],
        now: datetime,
    ) -> Iterator[SignalEnvelope]:
        for error in sorted(errors):
            yield SignalEnvelope(
                schema_version=1,
                strategy_id="daily-close-error",
                strategy_version=self._strategy_version,
                parameter_fingerprint=canonical_sha256(
                    {
                        "contract": "daily-error-parameters/v1",
                        "strategy_version": self._strategy_version,
                    }
                ),
                dataset_snapshot_id=canonical.generation_id,
                feature_snapshot_id=canonical.db_content_sha256,
                event_time=now,
                available_at=now,
                candidate_id=f"daily-error:{canonical.trade_date.isoformat()}:{error}",
                action=SignalAction.WATCH,
                reason_codes=("daily_stage_error",),
                evidence={
                    "canonical_receipt_id": canonical.receipt_id,
                    "trade_date": canonical.trade_date.isoformat(),
                    "component": error,
                },
                expires_at=now + self._signal_ttl,
                producer_commit=self._producer_commit,
            )

    def _assert_artifacts(
        self,
        canonical: DailyCanonicalPublishReceipt,
        screen: DailyScreenStageArtifact,
        pool: DailyPoolStageArtifact,
    ) -> None:
        expected = (canonical.receipt_id, canonical.generation_id, canonical.trade_date)
        if any(
            (artifact.canonical_receipt_id, artifact.canonical_generation_id, artifact.trade_date)
            != expected
            for artifact in (screen, pool)
        ):
            raise DailySummaryStageError("daily summary artifact canonical binding changed")

    def _assert_persisted_artifacts(
        self,
        canonical: DailyCanonicalPublishReceipt,
        screen: DailyScreenStageArtifact,
        pool: DailyPoolStageArtifact,
    ) -> None:
        if (
            self._artifact_store.load_screen(canonical.receipt_id) != screen
            or self._artifact_store.load_pool(canonical.receipt_id) != pool
        ):
            raise DailySummaryStageError(
                "daily summary persisted stage evidence is missing or changed"
            )

    def _assert_fence(
        self,
        fence: DailyCloseCandidateFence,
        canonical: DailyCanonicalPublishReceipt,
        ledger_input_identity: str,
    ) -> None:
        now = self._now()
        fence.assert_current(now)
        fence.assert_source(canonical.source_generation_id, canonical.raw_content_sha256)
        fence.assert_input(ledger_input_identity)

    def _now(self) -> datetime:
        try:
            return normalize_aware_utc(self._clock())
        except Exception as exc:
            raise DailySummaryStageError("daily summary stage clock is invalid") from exc
