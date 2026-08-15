"""Typed, side-effect-limited orchestration for the isolated daily-close DAG.

The orchestrator owns only the SQLite stage ledger.  Source acquisition,
DuckDB publication, and notification routing stay behind injected stage
adapters so this module cannot become another monolithic ``run-daily``.
"""

from __future__ import annotations

import os
import signal
import subprocess
import time
from collections.abc import Callable, Mapping
from contextlib import suppress
from datetime import date, datetime, timedelta
from typing import Annotated, Literal, Protocol, Self

from pydantic import Field, field_validator, model_validator

from rquant.daily_pipeline_ledger import (
    DailyPipelineLedger,
    DailyPipelineLedgerError,
    DailyPipelineMode,
    DailyRecoverySummary,
    DailyRunRecord,
    DailyRunSpec,
    DailyRunState,
    DailyStageAttempt,
    DailyStageEffectIntent,
    DailyStageReceipt,
    DailyStageSpec,
    DailyStageState,
    DailyWriterLease,
    LeaseLost,
    StageFailure,
    StageResult,
)
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, normalize_aware_utc

Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
CommitSha = Annotated[str, Field(pattern=r"^[0-9a-f]{40}$")]
StageId = Annotated[str, Field(pattern=r"^[a-z][a-z0-9_-]{0,63}$")]
_MAX_STAGE_WALL_SECONDS = 24 * 60 * 60
_DEFAULT_LEASE_FOR = timedelta(minutes=15)
_MAX_SUBPROCESS_OUTPUT_BYTES = 64 * 1024
_HEARTBEAT_INTERVAL_SECONDS = 2.0


def _contains_lease_loss(error: BaseException) -> bool:
    if isinstance(error, LeaseLost):
        return True
    if isinstance(error, BaseExceptionGroup):
        return any(_contains_lease_loss(nested) for nested in error.exceptions)
    return False


class DailyPipelineOrchestratorError(RuntimeError):
    """The DAG cannot safely continue from its durable state."""


class DailyStageAdapterError(DailyPipelineOrchestratorError):
    """A stage adapter exposes a typed, retryable operational failure."""

    def __init__(self, code: str, message: str, *, retryable: bool) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable


class DailyStageBudget(RuntimeContractModel):
    """Hard envelope declared for one external stage adapter."""

    max_wall_seconds: int = Field(ge=1, le=_MAX_STAGE_WALL_SECONDS)
    max_memory_mb: int = Field(default=512, ge=1, le=1_048_576)
    max_io_bytes: int = Field(default=512 * 1024 * 1024, ge=1, le=1 << 50)


class DailyStageRuntimeSpec(RuntimeContractModel):
    """Runtime policy wrapped around the immutable ledger stage declaration."""

    stage_id: StageId
    depends_on: tuple[StageId, ...] = ()
    budget: DailyStageBudget
    max_attempts: int = Field(default=3, ge=1, le=20)
    retry_backoff_seconds: int = Field(default=30, ge=0, le=24 * 60 * 60)
    deadline_at: AwareUtcDatetime | None = None

    @field_validator("depends_on")
    @classmethod
    def _unique_dependencies(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if len(values) != len(set(values)):
            raise ValueError("stage dependencies must be unique")
        return values

    @model_validator(mode="after")
    def _not_self_dependent(self) -> Self:
        if self.stage_id in self.depends_on:
            raise ValueError("stage cannot depend on itself")
        return self

    def ledger_spec(self) -> DailyStageSpec:
        return DailyStageSpec(
            stage_id=self.stage_id,
            depends_on=self.depends_on,
            max_attempts=self.max_attempts,
            retry_backoff_seconds=self.retry_backoff_seconds,
            deadline_at=self.deadline_at,
        )


class DailyPipelineDefinition(RuntimeContractModel):
    """Complete, typed dependency graph consumed by an orchestrator instance."""

    contract: Literal["daily-pipeline-definition/v1"] = "daily-pipeline-definition/v1"
    stages: tuple[DailyStageRuntimeSpec, ...] = Field(min_length=1, max_length=64)

    @model_validator(mode="after")
    def _validate_graph(self) -> Self:
        stage_ids = tuple(stage.stage_id for stage in self.stages)
        if len(stage_ids) != len(set(stage_ids)):
            raise ValueError("daily pipeline stages must be unique")
        declared = set(stage_ids)
        if any(dep not in declared for stage in self.stages for dep in stage.depends_on):
            raise ValueError("daily pipeline dependency is not declared")
        # Reuse the ledger's cycle validator; it is the execution authority.
        DailyRunSpec(
            mode=DailyPipelineMode.SHADOW,
            trade_date=datetime(2026, 1, 1).date(),
            source_generation_id="0" * 64,
            source_content_hash="1" * 64,
            command_manifest_hash="4" * 64,
            code_commit="2" * 40,
            profile_hash="3" * 64,
            stages=tuple(stage.ledger_spec() for stage in self.stages),
        )
        return self

    @property
    def stage_ids(self) -> tuple[str, ...]:
        return tuple(stage.stage_id for stage in self.stages)

    def runtime_spec(self, stage_id: str) -> DailyStageRuntimeSpec:
        for stage in self.stages:
            if stage.stage_id == stage_id:
                return stage
        raise DailyPipelineOrchestratorError(f"daily stage is not in definition: {stage_id}")

    def to_run_spec(
        self,
        *,
        trade_date: date,
        source_generation_id: str,
        source_content_hash: str,
        command_manifest_hash: str,
        code_commit: str,
        profile_hash: str,
        mode: DailyPipelineMode,
        deadline_at: datetime | None = None,
    ) -> DailyRunSpec:
        return DailyRunSpec(
            mode=mode,
            trade_date=trade_date,
            source_generation_id=source_generation_id,
            source_content_hash=source_content_hash,
            command_manifest_hash=command_manifest_hash,
            code_commit=code_commit,
            profile_hash=profile_hash,
            deadline_at=deadline_at,
            stages=tuple(stage.ledger_spec() for stage in self.stages),
        )


class DailyStageHealth(RuntimeContractModel):
    """An adapter health assessment recorded before every side effect."""

    ready: bool
    detail: str = Field(min_length=1, max_length=4_096)
    checked_at: AwareUtcDatetime | None = None
    estimated_memory_mb: int | None = Field(default=None, ge=0, le=1_048_576)
    estimated_io_bytes: int | None = Field(default=None, ge=0, le=1 << 50)


class DailyStageExecutionContext(RuntimeContractModel):
    """All immutable identities an adapter may consume for one attempt."""

    run: DailyRunRecord
    lease: DailyWriterLease
    attempt: DailyStageAttempt
    dependency_receipts: tuple[DailyStageReceipt, ...]
    budget: DailyStageBudget
    observed_at: AwareUtcDatetime

    @model_validator(mode="after")
    def _bind_dependencies(self) -> Self:
        expected = self.run.spec.stages
        stage = next((item for item in expected if item.stage_id == self.attempt.stage_id), None)
        if stage is None:
            raise ValueError("stage attempt is not present in the daily run")
        if self.lease.fencing_token != self.attempt.fencing_token:
            raise ValueError("daily stage attempt fencing token does not match its lease")
        actual = tuple(item.stage_id for item in self.dependency_receipts)
        if actual != stage.depends_on:
            raise ValueError("dependency receipts do not match the daily stage declaration")
        if any(item.input_identity != self.run.input_identity for item in self.dependency_receipts):
            raise ValueError("dependency receipt input identity changed")
        return self


class DailySourceIdentity(RuntimeContractModel):
    """Current raw-spool identity, reopened at every stage boundary."""

    source_generation_id: Sha256
    source_content_hash: Sha256


class DailyStageProcessSpec(RuntimeContractModel):
    """Bounded child-process declaration; only the orchestrator starts it."""

    argv: tuple[str, ...] = Field(min_length=1, max_length=128)
    environment: dict[str, str] = Field(default_factory=dict, max_length=64)
    working_directory: str | None = Field(default=None, min_length=1, max_length=4_096)

    @field_validator("argv")
    @classmethod
    def _non_empty_argv(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if any(not value for value in values):
            raise ValueError("daily stage child argv contains an empty argument")
        return values


class DailySourceIdentityResolver(Protocol):
    """Source authority that re-reads the immutable spool current marker."""

    def resolve(self, run: DailyRunRecord, /) -> DailySourceIdentity: ...


class DailyCloseSpoolSourceResolver:
    """Resolve a run's identity from the authoritative daily-close current pointer.

    The caller supplied identities are only a plan preview.  Execution always
    reopens the live spool and binds the run to the currently published raw
    payload for its exact SSE trade date.
    """

    def __init__(self, spool: object) -> None:
        self._spool = spool

    def resolve(self, run: DailyRunRecord, /) -> DailySourceIdentity:
        from rquant.daily_close_gateway import DailyCloseGateway
        from rquant.live_contracts import LiveChannel
        from rquant.live_spool import LiveBatchSpool

        spool = self._spool
        if not isinstance(spool, LiveBatchSpool):
            raise DailyPipelineOrchestratorError("daily source resolver requires LiveBatchSpool")
        pointer = spool.current(LiveChannel.DAILY_CLOSE)
        if pointer is None:
            raise DailyPipelineOrchestratorError("daily-close current source pointer is missing")
        records = tuple(
            record
            for record in spool.list_after(
                LiveChannel.DAILY_CLOSE,
                sequence=pointer.sequence - 1,
                limit=1,
            )
            if record.envelope.sequence == pointer.sequence
        )
        if len(records) != 1:
            raise DailyPipelineOrchestratorError("daily-close current source record is missing")
        record = records[0]
        if (
            record.envelope.sequence != pointer.sequence
            or record.envelope.batch_id != pointer.batch_id
            or record.envelope.content_sha256 != pointer.content_sha256
            or record.envelope.revision != pointer.revision
        ):
            raise DailyPipelineOrchestratorError(
                "daily-close source pointer changed while resolving"
            )
        payload = DailyCloseGateway.decode_payload(spool.read_payload(record))
        if payload.source_request.trade_date != run.spec.trade_date:
            raise DailyPipelineOrchestratorError("daily-close current source trade date is stale")
        return DailySourceIdentity(
            source_generation_id=pointer.source_generation_id,
            source_content_hash=payload.content_sha256,
        )


class DailyStageAdapter(Protocol):
    """Production protocol for a deterministic, idempotent child-process stage."""

    stage_id: str

    def health(self, context: DailyStageExecutionContext, /) -> DailyStageHealth: ...

    def prepare(self, context: DailyStageExecutionContext, /) -> DailyStageEffectIntent: ...

    def command(
        self,
        context: DailyStageExecutionContext,
        effect: DailyStageEffectIntent,
        /,
    ) -> DailyStageProcessSpec: ...

    def reconcile(
        self,
        context: DailyStageExecutionContext,
        effect: DailyStageEffectIntent,
        /,
    ) -> StageResult | None: ...


class DailyStageAdvanceOutcome(RuntimeContractModel):
    run_id: str
    stage_id: StageId
    disposition: Literal["succeeded", "retry_wait", "failed"]
    receipt_id: Sha256 | None = None
    dependency_receipt_ids: tuple[Sha256, ...] = ()
    failure_code: str | None = None
    failure_message: str | None = None
    elapsed_seconds: float = Field(ge=0.0)


class DailyPipelineStatus(RuntimeContractModel):
    run_id: str
    state: DailyRunState
    next_stage_id: StageId | None = None
    stage_states: Mapping[StageId, DailyStageState]


DEFAULT_DAILY_CLOSE_PIPELINE = DailyPipelineDefinition(
    stages=(
        DailyStageRuntimeSpec(
            stage_id="raw_capture",
            budget=DailyStageBudget(max_wall_seconds=15 * 60),
        ),
        DailyStageRuntimeSpec(
            stage_id="validate_candidate",
            depends_on=("raw_capture",),
            budget=DailyStageBudget(max_wall_seconds=10 * 60),
        ),
        DailyStageRuntimeSpec(
            stage_id="canonical_publish",
            depends_on=("validate_candidate",),
            budget=DailyStageBudget(max_wall_seconds=20 * 60),
        ),
        DailyStageRuntimeSpec(
            stage_id="screen",
            depends_on=("canonical_publish",),
            budget=DailyStageBudget(max_wall_seconds=15 * 60),
        ),
        DailyStageRuntimeSpec(
            stage_id="pool",
            depends_on=("screen",),
            budget=DailyStageBudget(max_wall_seconds=15 * 60),
        ),
        DailyStageRuntimeSpec(
            stage_id="summary",
            depends_on=("screen", "pool"),
            budget=DailyStageBudget(max_wall_seconds=5 * 60),
        ),
        DailyStageRuntimeSpec(
            stage_id="serving_refresh",
            depends_on=("summary",),
            budget=DailyStageBudget(max_wall_seconds=10 * 60),
        ),
        DailyStageRuntimeSpec(
            stage_id="replica_sync",
            depends_on=("serving_refresh",),
            budget=DailyStageBudget(max_wall_seconds=10 * 60),
        ),
        DailyStageRuntimeSpec(
            stage_id="research_ingest",
            depends_on=("replica_sync",),
            budget=DailyStageBudget(max_wall_seconds=30 * 60),
        ),
        DailyStageRuntimeSpec(
            stage_id="backup",
            depends_on=("replica_sync", "research_ingest"),
            budget=DailyStageBudget(max_wall_seconds=30 * 60),
        ),
    )
)


class DailyPipelineOrchestrator:
    """Advance one fenced stage at a time without becoming a stage implementation."""

    def __init__(
        self,
        *,
        ledger: DailyPipelineLedger,
        service_owner: str,
        definition: DailyPipelineDefinition = DEFAULT_DAILY_CLOSE_PIPELINE,
        adapters: tuple[DailyStageAdapter, ...],
        source_resolver: DailySourceIdentityResolver,
        clock: Callable[[], datetime],
        monotonic_clock: Callable[[], float] = time.monotonic,
        lease_for: timedelta = _DEFAULT_LEASE_FOR,
        execution_mode: Literal["production", "test_fixture"] = "production",
    ) -> None:
        if lease_for <= timedelta(0):
            raise ValueError("daily orchestrator lease must be positive")
        self.ledger = ledger
        self._service_owner = service_owner
        self._definition = DailyPipelineDefinition.model_validate(definition)
        self._source_resolver = source_resolver
        self._clock = clock
        self._monotonic_clock = monotonic_clock
        self._lease_for = lease_for
        self._execution_mode = execution_mode
        supplied = {adapter.stage_id: adapter for adapter in adapters}
        if set(supplied) != set(self._definition.stage_ids):
            raise ValueError("daily orchestrator adapters must exactly match the pipeline stages")
        self._adapters = supplied

    @property
    def definition(self) -> DailyPipelineDefinition:
        return self._definition

    @property
    def adapters(self) -> Mapping[str, DailyStageAdapter]:
        return self._adapters

    def create_run(
        self,
        *,
        trade_date: date,
        source_generation_id: str,
        source_content_hash: str,
        command_manifest_hash: str,
        code_commit: str,
        profile_hash: str,
        mode: DailyPipelineMode,
        now: datetime | None = None,
        deadline_at: datetime | None = None,
    ) -> DailyRunRecord:
        observed = self._now(now)
        spec = self._definition.to_run_spec(
            mode=mode,
            trade_date=trade_date,
            source_generation_id=source_generation_id,
            source_content_hash=source_content_hash,
            command_manifest_hash=command_manifest_hash,
            code_commit=code_commit,
            profile_hash=profile_hash,
            deadline_at=deadline_at,
        )
        # Do not allocate ledger state for a raw revision that is no longer
        # current.  The resolver is the source authority, not caller input.
        provisional = DailyRunRecord(
            run_id=spec.run_id,
            spec=spec,
            input_identity=spec.input_identity,
            state=DailyRunState.RUNNING,
            created_at=observed,
        )
        self._assert_source_identity(provisional)
        lease = self._acquire(observed)
        return self.ledger.create_run(lease, spec, now=observed)

    def advance(
        self,
        run_id: str,
        *,
        now: datetime | None = None,
    ) -> DailyStageAdvanceOutcome | None:
        observed = self._now(now)
        lease = self._acquire(observed)
        self.ledger.recover(lease, now=observed)
        run = self.ledger.run(run_id)
        self._assert_source_identity(run)
        if run.state is not DailyRunState.RUNNING:
            return None
        attempt = self._adopt_or_claim(lease, run, observed)
        if attempt is None:
            return None
        context = self._context_for(run, lease, attempt, observed)
        runtime_spec = self._definition.runtime_spec(attempt.stage_id)
        dependency_receipts = context.dependency_receipts
        adapter = self._adapter_for(attempt.stage_id)
        started = self._monotonic()
        try:
            if self._execution_mode == "test_fixture" and callable(getattr(adapter, "run", None)):
                return self._advance_fixture_stage(
                    lease,
                    attempt,
                    context,
                    adapter,
                    runtime_spec,
                    started,
                )
            effect_record = self.ledger.effect_for_stage(run_id, attempt.stage_id)
            if effect_record is None:
                health = DailyStageHealth.model_validate(adapter.health(context))
                if not health.ready:
                    return self._fail(
                        lease,
                        attempt,
                        code="health_unready",
                        message=health.detail,
                        retryable=True,
                        now=observed,
                        elapsed_seconds=self._monotonic() - started,
                        dependency_receipts=dependency_receipts,
                    )
                if (
                    health.estimated_memory_mb is not None
                    and health.estimated_memory_mb > runtime_spec.budget.max_memory_mb
                ) or (
                    health.estimated_io_bytes is not None
                    and health.estimated_io_bytes > runtime_spec.budget.max_io_bytes
                ):
                    return self._fail(
                        lease,
                        attempt,
                        code="resource_budget_exceeded",
                        message="stage resource estimate exceeds its declared budget",
                        retryable=False,
                        now=observed,
                        elapsed_seconds=self._monotonic() - started,
                        dependency_receipts=dependency_receipts,
                    )
                lease = self._heartbeat(lease, attempt)
                context = self._context_for(run, lease, attempt, self._now(None))
                intent = DailyStageEffectIntent.model_validate(adapter.prepare(context))
                self._assert_effect_identity(context, intent)
                effect_record = self.ledger.prepare_effect(
                    lease,
                    attempt,
                    intent,
                    now=self._now(None),
                )
            else:
                if effect_record.attempt_number != attempt.attempt_number:
                    raise DailyPipelineOrchestratorError(
                        "daily external effect intent does not bind the adopted attempt"
                    )

            recovered_result = adapter.reconcile(context, effect_record.intent)
            if recovered_result is None:
                lease, observed_exit_status = self._execute_child(
                    lease,
                    attempt,
                    context,
                    effect_record.intent,
                    adapter,
                    runtime_spec.budget,
                )
                context = self._context_for(run, lease, attempt, self._now(None))
                if observed_exit_status != 0:
                    return self._fail(
                        lease,
                        attempt,
                        code="child_process_failed",
                        message=(f"stage child exited with nonzero status: {observed_exit_status}"),
                        retryable=True,
                        now=self._now(None),
                        elapsed_seconds=self._monotonic() - started,
                        dependency_receipts=dependency_receipts,
                    )
                result = adapter.reconcile(context, effect_record.intent)
                if result is None:
                    return self._fail(
                        lease,
                        attempt,
                        code="missing_external_receipt",
                        message="child process exited without an immutable external receipt",
                        retryable=True,
                        now=self._now(None),
                        elapsed_seconds=self._monotonic() - started,
                        dependency_receipts=dependency_receipts,
                    )
                result = StageResult.model_validate(result)
            else:
                result = StageResult.model_validate(recovered_result)
            lease = self._heartbeat(lease, attempt)
            prepared = self.ledger.prepare_success(lease, attempt, result, now=self._now(None))
            lease = self._heartbeat(lease, attempt)
            receipt = self.ledger.finalize_success(lease, prepared, now=self._now(None))
            elapsed = self._monotonic() - started
            return DailyStageAdvanceOutcome(
                run_id=run_id,
                stage_id=attempt.stage_id,
                disposition="succeeded",
                receipt_id=receipt.receipt_id,
                dependency_receipt_ids=tuple(item.receipt_id for item in dependency_receipts),
                elapsed_seconds=elapsed,
            )
        except LeaseLost:
            raise
        except DailyStageAdapterError as exc:
            return self._fail(
                lease,
                attempt,
                code=exc.code,
                message=str(exc),
                retryable=exc.retryable,
                now=self._now(None),
                elapsed_seconds=self._monotonic() - started,
                dependency_receipts=dependency_receipts,
            )
        except Exception as exc:
            if _contains_lease_loss(exc):
                raise
            return self._fail(
                lease,
                attempt,
                code="adapter_exception",
                message=type(exc).__name__,
                retryable=True,
                now=self._now(None),
                elapsed_seconds=self._monotonic() - started,
                dependency_receipts=dependency_receipts,
            )

    def recover(
        self,
        *,
        run_id: str | None = None,
        now: datetime | None = None,
    ) -> DailyRecoverySummary:
        observed = self._now(now)
        lease = self._acquire(observed)
        baseline = self.ledger.recover(lease, now=observed, run_id=run_id)
        finalized = list(baseline.finalized_receipt_ids)
        failed = list(baseline.failed_stage_ids)
        for stale_attempt in self.ledger.active_effect_attempts(
            lease,
            now=observed,
            run_id=run_id,
        ):
            run = self.ledger.run(stale_attempt.run_id)
            self._assert_source_identity(run)
            attempt = self.ledger.adopt_effect_attempt(
                lease,
                run_id=stale_attempt.run_id,
                stage_id=stale_attempt.stage_id,
                now=observed,
            )
            if attempt is None:
                continue
            context = self._context_for(run, lease, attempt, observed)
            effect = self.ledger.effect_for_stage(run.run_id, attempt.stage_id)
            if effect is None:
                raise DailyPipelineOrchestratorError("daily recoverable effect disappeared")
            result = self._adapter_for(attempt.stage_id).reconcile(context, effect.intent)
            if result is None:
                continue
            try:
                prepared = self.ledger.prepare_success(
                    lease,
                    attempt,
                    StageResult.model_validate(result),
                    now=self._now(None),
                )
                finalized.append(
                    self.ledger.finalize_success(lease, prepared, now=self._now(None)).receipt_id
                )
            except DailyPipelineLedgerError:
                failed.append(attempt.stage_id)
                raise
        return DailyRecoverySummary(
            finalized_receipt_ids=tuple(finalized),
            retried_stage_ids=baseline.retried_stage_ids,
            failed_stage_ids=tuple(failed),
            next_cursor=baseline.next_cursor,
        )

    def cancel(self, run_id: str, *, reason: str, now: datetime | None = None) -> DailyRunRecord:
        observed = self._now(now)
        return self.ledger.cancel_run(self._acquire(observed), run_id, reason=reason, now=observed)

    def status(self, run_id: str) -> DailyPipelineStatus:
        run = self.ledger.run(run_id)
        states: dict[str, DailyStageState] = {}
        next_stage_id: str | None = None
        for stage_id in self._definition.stage_ids:
            state = self.ledger.stage(run_id, stage_id).state
            states[stage_id] = state
            spec = self._definition.runtime_spec(stage_id)
            if (
                next_stage_id is None
                and state is DailyStageState.PENDING
                and all(
                    states.get(dependency) is DailyStageState.SUCCEEDED
                    for dependency in spec.depends_on
                )
            ):
                next_stage_id = stage_id
        return DailyPipelineStatus(
            run_id=run_id,
            state=run.state,
            next_stage_id=next_stage_id,
            stage_states=states,
        )

    def _acquire(self, observed: datetime) -> DailyWriterLease:
        return self.ledger.acquire_writer(
            owner=self._service_owner,
            now=observed,
            lease_for=self._lease_for,
        )

    def _dependency_receipts(
        self,
        run: DailyRunRecord,
        stage_id: str,
    ) -> tuple[DailyStageReceipt, ...]:
        stage = self._definition.runtime_spec(stage_id)
        receipts: list[DailyStageReceipt] = []
        for dependency in stage.depends_on:
            record = self.ledger.stage(run.run_id, dependency)
            if record.state is not DailyStageState.SUCCEEDED or record.terminal_receipt_id is None:
                raise DailyPipelineOrchestratorError(
                    "daily stage dependency is not durably complete"
                )
            receipt = self.ledger.receipt(record.terminal_receipt_id)
            if receipt is None:
                raise DailyPipelineOrchestratorError("daily stage dependency receipt is missing")
            receipts.append(receipt)
        return tuple(receipts)

    def _adapter_for(self, stage_id: str) -> DailyStageAdapter:
        adapter = self._adapters.get(stage_id)
        if adapter is None:
            raise DailyPipelineOrchestratorError(f"daily stage adapter is missing: {stage_id}")
        return adapter

    def _advance_fixture_stage(
        self,
        lease: DailyWriterLease,
        attempt: DailyStageAttempt,
        context: DailyStageExecutionContext,
        adapter: DailyStageAdapter,
        runtime_spec: DailyStageRuntimeSpec,
        started: float,
    ) -> DailyStageAdvanceOutcome:
        """Compatibility boundary used only by pre-existing in-process fixtures.

        Production adapters cannot enter this path: the constructor defaults to
        ``production`` and the runtime builder never exposes ``test_fixture``.
        The real subprocess contract is covered by process E2E tests.
        """
        health = DailyStageHealth.model_validate(adapter.health(context))
        if not health.ready:
            return self._fail(
                lease,
                attempt,
                code="health_unready",
                message=health.detail,
                retryable=True,
                now=context.observed_at,
                elapsed_seconds=self._monotonic() - started,
                dependency_receipts=context.dependency_receipts,
            )
        if (
            health.estimated_memory_mb is not None
            and health.estimated_memory_mb > runtime_spec.budget.max_memory_mb
        ) or (
            health.estimated_io_bytes is not None
            and health.estimated_io_bytes > runtime_spec.budget.max_io_bytes
        ):
            return self._fail(
                lease,
                attempt,
                code="resource_budget_exceeded",
                message="stage resource estimate exceeds its declared budget",
                retryable=False,
                now=context.observed_at,
                elapsed_seconds=self._monotonic() - started,
                dependency_receipts=context.dependency_receipts,
            )
        fixture_run = getattr(adapter, "run", None)
        result = StageResult.model_validate(fixture_run(context))
        elapsed = self._monotonic() - started
        if elapsed > runtime_spec.budget.max_wall_seconds:
            return self._fail(
                lease,
                attempt,
                code="resource_budget_exceeded",
                message="stage exceeded its maximum wall-clock budget",
                retryable=False,
                now=self._now(None),
                elapsed_seconds=elapsed,
                dependency_receipts=context.dependency_receipts,
            )
        receipt = self.ledger.succeed(lease, attempt, result, now=self._now(None))
        return DailyStageAdvanceOutcome(
            run_id=attempt.run_id,
            stage_id=attempt.stage_id,
            disposition="succeeded",
            receipt_id=receipt.receipt_id,
            dependency_receipt_ids=tuple(item.receipt_id for item in context.dependency_receipts),
            elapsed_seconds=elapsed,
        )

    def _context_for(
        self,
        run: DailyRunRecord,
        lease: DailyWriterLease,
        attempt: DailyStageAttempt,
        observed: datetime,
    ) -> DailyStageExecutionContext:
        return DailyStageExecutionContext(
            run=run,
            lease=lease,
            attempt=attempt,
            dependency_receipts=self._dependency_receipts(run, attempt.stage_id),
            budget=self._definition.runtime_spec(attempt.stage_id).budget,
            observed_at=observed,
        )

    def _adopt_or_claim(
        self,
        lease: DailyWriterLease,
        run: DailyRunRecord,
        observed: datetime,
    ) -> DailyStageAttempt | None:
        running = self.ledger.active_running_attempts(lease, now=observed, run_id=run.run_id)
        if running:
            return self.ledger.adopt_effect_attempt(
                lease,
                run_id=running[0].run_id,
                stage_id=running[0].stage_id,
                now=observed,
            )
        return self.ledger.claim_next_for_run(lease, run.run_id, now=observed)

    def _heartbeat(
        self,
        lease: DailyWriterLease,
        attempt: DailyStageAttempt,
    ) -> DailyWriterLease:
        return self.ledger.heartbeat_stage(
            lease,
            attempt,
            now=self._now(None),
            lease_for=self._lease_for,
        )

    def _execute_child(
        self,
        lease: DailyWriterLease,
        attempt: DailyStageAttempt,
        context: DailyStageExecutionContext,
        effect: DailyStageEffectIntent,
        adapter: DailyStageAdapter,
        budget: DailyStageBudget,
    ) -> tuple[DailyWriterLease, int]:
        self._assert_source_identity(context.run)
        process_spec = DailyStageProcessSpec.model_validate(adapter.command(context, effect))
        lease = self._heartbeat(lease, attempt)
        self._assert_source_identity(context.run)
        # The child receives a stable idempotency capability, not an implicit
        # Python object.  A restarted parent may launch a new child only when
        # the adapter's external store resolves this same key to its immutable
        # receipt.  Fencing and lease expiry are included so a stage command can
        # fail closed before its own final external commit.
        effect_environment = {
            "RQUANT_DAILY_EFFECT_ID": str(effect.effect_id),
            "RQUANT_DAILY_EFFECT_IDEMPOTENCY_KEY": effect.idempotency_key,
            "RQUANT_DAILY_EFFECT_RECEIPT_LOCATOR": effect.receipt_locator,
            "RQUANT_DAILY_RUN_ID": attempt.run_id,
            "RQUANT_DAILY_RUN_MODE": context.run.spec.mode.value,
            "RQUANT_DAILY_STAGE_ID": attempt.stage_id,
            "RQUANT_DAILY_LEDGER_PATH": str(self.ledger.path),
            "RQUANT_DAILY_STORAGE_ROOT": str(self.ledger.storage_profile.root),
            "RQUANT_DAILY_PROFILE_HASH": context.run.spec.profile_hash,
            "RQUANT_DAILY_STORAGE_NAMESPACE_ID": str(self.ledger.storage_profile.namespace_id),
            "RQUANT_DAILY_SERVICE_OWNER": self._service_owner,
            "RQUANT_DAILY_INPUT_IDENTITY": context.run.input_identity,
            "RQUANT_DAILY_SOURCE_GENERATION_ID": context.run.spec.source_generation_id,
            "RQUANT_DAILY_SOURCE_CONTENT_HASH": context.run.spec.source_content_hash,
            "RQUANT_DAILY_COMMAND_MANIFEST_HASH": context.run.spec.command_manifest_hash,
            "RQUANT_DAILY_FENCING_TOKEN": str(lease.fencing_token),
            "RQUANT_DAILY_LEASE_EXPIRES_AT": lease.expires_at.isoformat(),
        }
        environment = {**os.environ, **process_spec.environment, **effect_environment}
        process = subprocess.Popen(
            process_spec.argv,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            cwd=process_spec.working_directory,
            env=environment,
            start_new_session=True,
        )
        deadline = self._monotonic() + budget.max_wall_seconds
        try:
            while True:
                remaining = deadline - self._monotonic()
                if remaining <= 0:
                    self._terminate_process_group(process)
                    raise DailyStageAdapterError(
                        "resource_budget_exceeded",
                        "stage child exceeded its maximum wall-clock budget",
                        retryable=False,
                    )
                try:
                    observed_exit_status = process.wait(
                        timeout=min(remaining, _HEARTBEAT_INTERVAL_SECONDS)
                    )
                    break
                except subprocess.TimeoutExpired:
                    lease = self._heartbeat(lease, attempt)
                    self._assert_source_identity(context.run)
            self._terminate_process_group(process)
        except LeaseLost:
            try:
                self._terminate_process_group(process)
            except Exception as cleanup_error:
                raise LeaseLost(
                    "writer lease lost and child process group cleanup failed"
                ) from cleanup_error
            raise
        self._assert_source_identity(context.run)
        if process.returncode is not None:
            observed_exit_status = process.returncode
        return lease, int(observed_exit_status)

    def _terminate_process_group(self, process: subprocess.Popen[bytes]) -> None:
        pgid = process.pid
        with suppress(ProcessLookupError):
            os.killpg(pgid, signal.SIGTERM)
        with suppress(subprocess.TimeoutExpired):
            self._wait_for_process(process, timeout=2.0)
        if self._process_group_exists(pgid):
            with suppress(ProcessLookupError):
                os.killpg(pgid, signal.SIGKILL)
            try:
                self._wait_for_process(process, timeout=2.0)
            except subprocess.TimeoutExpired as exc:
                raise DailyPipelineOrchestratorError(
                    "daily stage process group did not terminate"
                ) from exc
        self._assert_process_group_absent(pgid)

    def _wait_for_process(self, process: subprocess.Popen[bytes], *, timeout: float) -> int:
        deadline = self._monotonic() + timeout
        while True:
            remaining = deadline - self._monotonic()
            if remaining <= 0:
                raise subprocess.TimeoutExpired(process.args, timeout)
            try:
                return process.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                if self._monotonic() >= deadline:
                    raise

    @staticmethod
    def _process_group_exists(pgid: int) -> bool:
        try:
            os.killpg(pgid, 0)
        except ProcessLookupError:
            return False
        except OSError as exc:
            raise DailyPipelineOrchestratorError(
                "daily stage process group verification failed"
            ) from exc
        return True

    def _assert_process_group_absent(self, pgid: int) -> None:
        if self._process_group_exists(pgid):
            raise DailyPipelineOrchestratorError("daily stage process group was not reaped")

    @staticmethod
    def _assert_effect_identity(
        context: DailyStageExecutionContext,
        intent: DailyStageEffectIntent,
    ) -> None:
        from rquant.runtime_contracts import canonical_sha256

        expected = canonical_sha256(
            {
                "contract": "daily-stage-idempotency-key/v3",
                "mode": context.run.spec.mode,
                "run_id": context.run.run_id,
                "stage_id": context.attempt.stage_id,
                "input_identity": context.run.input_identity,
                "command_manifest_hash": context.run.spec.command_manifest_hash,
            }
        )
        if intent.idempotency_key != expected:
            raise DailyPipelineOrchestratorError(
                "daily stage adapter did not bind the required external idempotency key"
            )
        if intent.command_manifest_hash != context.run.spec.command_manifest_hash:
            raise DailyPipelineOrchestratorError(
                "daily stage adapter did not bind the reviewed command manifest"
            )
        if intent.mode is not context.run.spec.mode:
            raise DailyPipelineOrchestratorError(
                "daily stage adapter did not bind the immutable run mode"
            )

    def _fail(
        self,
        lease: DailyWriterLease,
        attempt: DailyStageAttempt,
        *,
        code: str,
        message: str,
        retryable: bool,
        now: datetime,
        elapsed_seconds: float,
        dependency_receipts: tuple[DailyStageReceipt, ...],
    ) -> DailyStageAdvanceOutcome:
        record = self.ledger.fail(
            lease,
            attempt,
            StageFailure(error_code=code, message=message),
            retryable=retryable,
            now=now,
        )
        if record.state is DailyStageState.RETRY_WAIT:
            disposition: Literal["retry_wait", "failed"] = "retry_wait"
        else:
            disposition = "failed"
        return DailyStageAdvanceOutcome(
            run_id=attempt.run_id,
            stage_id=attempt.stage_id,
            disposition=disposition,
            dependency_receipt_ids=tuple(item.receipt_id for item in dependency_receipts),
            failure_code=code,
            failure_message=message,
            elapsed_seconds=max(elapsed_seconds, 0.0),
        )

    def _assert_source_identity(self, run: DailyRunRecord) -> DailySourceIdentity:
        current = DailySourceIdentity.model_validate(self._source_resolver.resolve(run))
        if (
            current.source_generation_id != run.spec.source_generation_id
            or current.source_content_hash != run.spec.source_content_hash
        ):
            raise ValueError("daily run source identity has changed")
        return current

    def _now(self, supplied: datetime | None) -> datetime:
        return normalize_aware_utc(supplied if supplied is not None else self._clock())

    def _monotonic(self) -> float:
        return self._monotonic_clock()


__all__ = [
    "DEFAULT_DAILY_CLOSE_PIPELINE",
    "DailyPipelineDefinition",
    "DailyCloseSpoolSourceResolver",
    "DailyPipelineOrchestrator",
    "DailyPipelineOrchestratorError",
    "DailyPipelineStatus",
    "DailyStageAdapter",
    "DailyStageAdapterError",
    "DailyStageAdvanceOutcome",
    "DailyStageBudget",
    "DailyStageExecutionContext",
    "DailyStageHealth",
    "DailyStageProcessSpec",
    "DailyStageRuntimeSpec",
    "DailySourceIdentity",
    "DailySourceIdentityResolver",
]
