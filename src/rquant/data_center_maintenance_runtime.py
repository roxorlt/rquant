"""Bounded original maintenance results, with receipts in the original fact transaction."""
from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import closing
from contextlib import AbstractContextManager, contextmanager, nullcontext
from datetime import UTC, date, datetime
from threading import Event
from typing import TYPE_CHECKING, Literal
from types import FrameType
from pathlib import Path
import hashlib
import sqlite3

import pandas as pd
from pydantic import ConfigDict, Field, model_validator

from rquant.backfill_execute import require_execution_policy, require_stable_execution_policy, verify_backfill_day_receipt
from rquant.backfill_execute_contracts import BackfillExecutionSpec, DataCenterExecutionPolicy, maintenance_window
from rquant.backfill_state import BackfillStateStore, ClaimedBackfillTask, ProtectedBackfillClaim
from rquant.daily_canonical_publisher import CanonicalDatabaseIdentity, DailyCanonicalPublisher
from rquant.indicator_backfill import _load_indicator_history_batch, derive_daily_indicators
from rquant.market_backfill import PreparedDailyStateTail, _prepared_frame_sha256, prepare_daily_state_tail, recompute_daily_state
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.storage.duckdb import DuckDBStore
from rquant.storage.primary_writer_gate import PrimaryWriterGate, PrimaryWriterLease
from rquant.data_collection_contracts import (AuditCollectionReference,CollectionReceiptReference,CollectionRecorderConfig,
    IngestionCommitReceipt,CollectionReceiptManifest,CollectionManifestPage)
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.data_audit_report_jobs import DataAuditReportJobStore
from rquant.data_audit_report import CollectionDataAuditReport
from rquant.backfill_plan_core import BackfillEstimateAssumptions, DailyBarBackfillPlan

_MAX_ROWS = 250 * 3660
_MAX_BYTES = 256 * 1024 * 1024

if TYPE_CHECKING:
    from rquant.financial_runtime import FinancialExecutionSpec
    from rquant.backfill_execute import BackfillExecutionWorker,ControlledTransportObserver
    from rquant.backfill_execute_page_backend import BackfillExecutePageBackend
    from rquant.adapter.tushare import TushareAdapter
    from rquant.backfill_plan_page_backend import BackfillPlanPageBackend
    from rquant.backfill_plan_jobs import BackfillPlanJobWorker, BackfillPlanJobRequest
    from rquant.data_audit_report_page_backend import DataAuditReportPageBackend


class DerivedTailTableReceipt(RuntimeContractModel):
    table_name: Literal['daily_indicator', 'daily_state']
    row_count: int = Field(strict=True, ge=0, le=_MAX_ROWS)
    rows_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')


class DerivedTailBatchReceipt(RuntimeContractModel):
    contract: Literal['backfill-derived-tail-batch/v1'] = 'backfill-derived-tail-batch/v1'
    receipt_id: str | None = Field(default=None, pattern=r'^[0-9a-f]{64}$')
    execution_id: str
    owner: str
    manifest_id: str
    plan_sha256: str
    task_id: str
    batch_index: int = Field(strict=True, ge=0, le=31)
    universe_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    codes: tuple[str, ...] = Field(min_length=1, max_length=250)
    start_date: date
    end_date: date
    primary_identity: CanonicalDatabaseIdentity
    source_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    tables: tuple[DerivedTailTableReceipt, ...] = Field(min_length=2, max_length=2)
    committed_claim_token: str
    committed_attempt: int = Field(strict=True, ge=1)
    control_sequence: int = Field(strict=True, ge=1)
    policy_generation: str
    committed_at: AwareUtcDatetime

    @model_validator(mode='after')
    def bind(self) -> DerivedTailBatchReceipt:
        if self.codes != tuple(sorted(set(self.codes))) or self.start_date > self.end_date:
            raise ValueError('derived tail batch scope changed')
        if {item.table_name for item in self.tables} != {'daily_indicator', 'daily_state'}:
            raise ValueError('derived tail receipt requires both original tables')
        expected = canonical_sha256(self.model_dump(mode='python', exclude={'receipt_id'}))
        if self.receipt_id is not None and self.receipt_id != expected:
            raise ValueError('derived tail receipt content changed')
        object.__setattr__(self, 'receipt_id', expected)
        return self


class PreparedDerivedTailBatch(RuntimeContractModel):
    model_config = ConfigDict(extra='forbid', frozen=True, arbitrary_types_allowed=True)
    batch_index: int = Field(strict=True, ge=0, le=31)
    universe_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    state: PreparedDailyStateTail
    indicator_source_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    indicator_rows_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    indicators: pd.DataFrame


@contextmanager
def protected_maintenance_writer(state: BackfillStateStore, claim: ClaimedBackfillTask, *,
        spec: BackfillExecutionSpec | FinancialExecutionSpec, policy: Callable[[], DataCenterExecutionPolicy],
        control_sequence: int, clock: Callable[[], datetime],
        writer_factory: Callable[[PrimaryWriterLease], DuckDBStore] | None = None,
        kind: Literal['backfill','financial'] = 'backfill', primary_writer_lease: PrimaryWriterLease | None = None,
        ) -> Iterator[tuple[DuckDBStore, ProtectedBackfillClaim, DataCenterExecutionPolicy]]:
    current = policy()
    require_execution_policy(current, kind=kind, now=clock())
    require_stable_execution_policy(current, spec)
    if current.original_state_path != state.path:
        raise ValueError('original maintenance state path changed')
    if primary_writer_lease is not None and primary_writer_lease.config!=current.primary_writer_gate:
        raise ValueError('borrowed maintenance writer lease differs from current policy')
    with (PrimaryWriterGate(current.primary_writer_gate).acquire() if primary_writer_lease is None else nullcontext(primary_writer_lease)) as lease:
        lease.verify(current.primary_writer_gate.primary_path)
        def guard(connection: sqlite3.Connection, observed: datetime) -> None:
            lease.verify()
            status = state.verify_maintenance_claim(connection, claim, execution_id=spec.execution_id,
                owner=spec.owner, expected_sequence=control_sequence, now=observed)
            live = policy()
            require_execution_policy(live, kind=kind, now=observed)
            require_stable_execution_policy(live, spec)
            window = maintenance_window(observed)
            if (live.policy_generation != current.policy_generation or status.policy_generation != current.policy_generation
                    or not window.may_hold_writer or observed >= window.interrupt_at):
                raise ValueError('maintenance policy or commit window changed')
        with state.commit_claim(claim, now=clock(), guard=guard) as protected:
            factory = writer_factory or (lambda borrowed: DuckDBStore(current.primary_writer_gate.primary_path,
                primary_writer_lease=borrowed))
            with factory(lease) as store:
                if DailyCanonicalPublisher.database_identity(store) != spec.intent.primary_identity:
                    raise ValueError('maintenance belongs to another physical primary')
                yield store, protected, current


def _indicator_inputs(store: DuckDBStore, codes: tuple[str, ...], end_date: date) -> pd.DataFrame:
    if not codes or len(codes) > 250 or codes != tuple(sorted(set(codes))):
        raise ValueError('derived tail requires at most 250 exact securities')
    count = store._conn.execute('SELECT COUNT(*) FROM daily_bar WHERE ts_code IN (SELECT unnest(?)) AND trade_date<=?',
        [list(codes), end_date]).fetchone()[0]
    if count > _MAX_ROWS or count * 192 > _MAX_BYTES:
        raise ValueError('original indicator history exceeds bounded tail capacity')
    history = _load_indicator_history_batch(store, end_date=end_date, ts_codes=list(codes))
    if int(history.memory_usage(deep=True).sum()) > _MAX_BYTES:
        raise ValueError('original indicator history material exceeds bounded capacity')
    return history


def prepare_derived_tail_batch(store: DuckDBStore, spec: BackfillExecutionSpec, codes: tuple[str, ...], *,
        batch_index: int, universe: tuple[str, ...]) -> PreparedDerivedTailBatch:
    if (universe != tuple(sorted(set(universe))) or not 0 < len(universe) <= 8000
            or codes != universe[batch_index * 250:(batch_index + 1) * 250]):
        raise ValueError('derived batch differs from fixed affected securities')
    start, end = min(spec.plan.missing_dates), spec.plan.completed_through
    state = prepare_daily_state_tail(store, list(codes), start_date=start, end_date=end)
    store._conn.execute('BEGIN')
    try:
        history = _indicator_inputs(store, codes, end)
        source_sha256 = _prepared_frame_sha256(history)
        indicators = derive_daily_indicators(store, start_date=start, end_date=end, ts_codes=codes, batch_size=250)
        if len(indicators) != len(state.rows):
            raise ValueError('original indicator tail coverage is incomplete')
        material_bytes = sum(int(frame.memory_usage(deep=True).sum()) for frame in (history, indicators, state.rows))
        if material_bytes > _MAX_BYTES:
            raise ValueError('prepared original tail exceeds material capacity')
        result = PreparedDerivedTailBatch(batch_index=batch_index, universe_sha256=canonical_sha256(universe),
            state=state, indicator_source_sha256=source_sha256,
            indicator_rows_sha256=_prepared_frame_sha256(indicators), indicators=indicators.copy(deep=True))
        store._conn.execute('COMMIT')
        return result
    except BaseException:
        store._conn.execute('ROLLBACK')
        raise


def _table_receipt(store: DuckDBStore, table: Literal['daily_indicator', 'daily_state'], *,
        codes: tuple[str, ...], start: date, end: date) -> DerivedTailTableReceipt:
    count = store._conn.execute(f'SELECT COUNT(*) FROM {table} WHERE ts_code IN (SELECT unnest(?)) AND trade_date BETWEEN ? AND ?',
        [list(codes), start, end]).fetchone()[0]
    if count > _MAX_ROWS or count * 256 > _MAX_BYTES:
        raise ValueError('committed original tail exceeds recovery capacity')
    rows = store._conn.execute(f'SELECT * FROM {table} WHERE ts_code IN (SELECT unnest(?)) AND trade_date BETWEEN ? AND ? '
        'ORDER BY ts_code,trade_date', [list(codes), start, end]).df()
    if int(rows.memory_usage(deep=True).sum()) > _MAX_BYTES:
        raise ValueError('committed tail material exceeds recovery capacity')
    return DerivedTailTableReceipt(table_name=table, row_count=count, rows_sha256=_prepared_frame_sha256(rows))


def verify_derived_tail_batch(store: DuckDBStore, spec: BackfillExecutionSpec, task_id: str) -> DerivedTailBatchReceipt | None:
    row = store._conn.execute('SELECT receipt_id,trade_date,payload_json FROM backfill_day_commit_receipt '
        'WHERE execution_id=? AND task_id=?', [spec.execution_id, task_id]).fetchone()
    if row is None:
        return None
    if len(row[2].encode()) > 256 * 1024:
        raise ValueError('original tail receipt exceeds recovery capacity')
    receipt = DerivedTailBatchReceipt.model_validate_json(row[2])
    if (receipt.execution_id, receipt.owner, receipt.manifest_id, receipt.plan_sha256, receipt.task_id,
            receipt.receipt_id, receipt.start_date, receipt.end_date) != (spec.execution_id, spec.owner,
            spec.manifest_id, spec.plan.content_sha256, task_id, row[0], min(spec.plan.missing_dates), spec.plan.completed_through):
        raise ValueError('original tail receipt differs from immutable owner/plan/task')
    if row[1] != receipt.start_date or receipt.primary_identity != spec.intent.primary_identity or receipt.primary_identity != DailyCanonicalPublisher.database_identity(store):
        raise ValueError('original tail receipt primary identity changed')
    actual = tuple(_table_receipt(store, item.table_name, codes=receipt.codes, start=receipt.start_date, end=receipt.end_date)
        for item in receipt.tables)
    if actual != receipt.tables:
        raise ValueError('original derived tail differs from committed receipt')
    return receipt


def _write_tail_receipt(store: DuckDBStore, spec: BackfillExecutionSpec, task_id: str, receipt: RuntimeContractModel,
        *, clock: Callable[[], datetime]) -> None:
    payload = receipt.model_dump_json()
    if len(payload.encode()) > 256 * 1024:
        raise ValueError('original tail receipt exceeds capacity')
    store._conn.execute('INSERT INTO backfill_day_commit_receipt VALUES (?,?,?,?,?,?)',
        [spec.execution_id, task_id, receipt.receipt_id, min(spec.plan.missing_dates), payload, clock()])


def commit_derived_tail_batch(state: BackfillStateStore, claim: ClaimedBackfillTask, *, spec: BackfillExecutionSpec,
        prepared: PreparedDerivedTailBatch, policy: Callable[[], DataCenterExecutionPolicy], control_sequence: int,
        clock: Callable[[], datetime],writer_factory: Callable[[PrimaryWriterLease],DuckDBStore] | None=None) -> DerivedTailBatchReceipt:
    if claim.task_id != 'tail-derived' or prepared.state.start_date != min(spec.plan.missing_dates) or prepared.state.end_date != spec.plan.completed_through:
        raise ValueError('original derived tail task scope changed')
    task_id = f'tail-derived/batch-{prepared.batch_index:02d}'
    indicators = prepared.indicators.copy(deep=True)
    if _prepared_frame_sha256(indicators) != prepared.indicator_rows_sha256:
        raise ValueError('prepared original indicator results changed')
    with protected_maintenance_writer(state, claim, spec=spec, policy=policy, control_sequence=control_sequence, clock=clock,writer_factory=writer_factory) as (store, protected, current):
        store._conn.execute('BEGIN')
        committed = False
        try:
            restored = verify_derived_tail_batch(store, spec, task_id)
            if restored is not None:
                if restored.codes != prepared.state.codes or restored.universe_sha256 != prepared.universe_sha256:
                    raise ValueError('same tail batch identity has different securities')
                store._conn.execute('ROLLBACK')
                return restored
            history = _indicator_inputs(store, prepared.state.codes, prepared.state.end_date)
            if _prepared_frame_sha256(history) != prepared.indicator_source_sha256:
                raise ValueError('original indicator source changed after preparation')
            recompute_daily_state(store, list(prepared.state.codes), start_date=prepared.state.start_date,
                status_mode='verified_no_fetch', transaction_mode='existing', prepared_tail=prepared.state)
            store._conn.execute('DELETE FROM daily_indicator WHERE ts_code IN (SELECT unnest(?)) AND trade_date BETWEEN ? AND ?',
                [list(prepared.state.codes), prepared.state.start_date, prepared.state.end_date])
            store.upsert_indicators(indicators)
            tables = tuple(_table_receipt(store, table, codes=prepared.state.codes, start=prepared.state.start_date,
                end=prepared.state.end_date) for table in ('daily_indicator', 'daily_state'))
            receipt = DerivedTailBatchReceipt(execution_id=spec.execution_id, owner=spec.owner, manifest_id=spec.manifest_id,
                plan_sha256=spec.plan.content_sha256, task_id=task_id, batch_index=prepared.batch_index,
                universe_sha256=prepared.universe_sha256, codes=prepared.state.codes, start_date=prepared.state.start_date,
                end_date=prepared.state.end_date, primary_identity=DailyCanonicalPublisher.database_identity(store),
                source_sha256=canonical_sha256((prepared.state.source_sha256, prepared.indicator_source_sha256)), tables=tables,
                committed_claim_token=claim.claim_token, committed_attempt=claim.attempt,
                control_sequence=control_sequence, policy_generation=current.policy_generation, committed_at=clock())
            _write_tail_receipt(store, spec, task_id, receipt, clock=clock)
            protected.verify(now=clock())
            store._conn.execute('COMMIT')
            committed = True
            return receipt
        except BaseException:
            if not committed:
                store._conn.execute('ROLLBACK')
            raise


class DerivedTailCompletionReceipt(RuntimeContractModel):
    contract: Literal['backfill-derived-tail-completion/v1'] = 'backfill-derived-tail-completion/v1'
    receipt_id: str | None = None
    execution_id: str
    owner: str
    manifest_id: str
    plan_sha256: str
    codes: tuple[str, ...] = Field(min_length=1, max_length=8000)
    batches: tuple[DerivedTailBatchReceipt, ...] = Field(min_length=1, max_length=32)
    committed_at: AwareUtcDatetime

    @model_validator(mode='after')
    def bind(self) -> DerivedTailCompletionReceipt:
        if tuple(code for batch in self.batches for code in batch.codes) != self.codes or any(
                batch.execution_id != self.execution_id or batch.owner != self.owner
                or batch.manifest_id != self.manifest_id or batch.plan_sha256 != self.plan_sha256
                or batch.batch_index != index or batch.universe_sha256 != canonical_sha256(self.codes)
                for index, batch in enumerate(self.batches)):
            raise ValueError('derived tail completion differs from exact original batches')
        expected = canonical_sha256(self.model_dump(mode='python', exclude={'receipt_id'}))
        if self.receipt_id is not None and self.receipt_id != expected:
            raise ValueError('derived tail completion content changed')
        object.__setattr__(self, 'receipt_id', expected)
        return self


def run_derived_tail(state: BackfillStateStore, claim: ClaimedBackfillTask, *, spec: BackfillExecutionSpec,
        policy: Callable[[], DataCenterExecutionPolicy], control_sequence: int, clock: Callable[[], datetime],
        guard: Callable[[], None] | None = None, stopped: Event | None = None,
        prepare_context: Callable[[], AbstractContextManager[Callable[[], None]]] | None = None,
        writer_factory: Callable[[PrimaryWriterLease],DuckDBStore] | None=None,
        reader_factory: Callable[[Path],DuckDBStore] | None=None) -> bool:
    if claim.task_id != 'tail-derived':
        raise ValueError('original task is not the derived tail')
    current = policy()
    require_execution_policy(current, kind='backfill', now=clock())
    reader_factory=reader_factory or (lambda path:DuckDBStore(path,read_only=True))
    with reader_factory(current.primary_writer_gate.primary_path) as reader:
        for day in spec.plan.missing_dates:
            if state.get_task(spec.manifest_id, f'day-{day.isoformat()}').status != 'succeeded' or verify_backfill_day_receipt(
                    reader, spec, task_id=f'day-{day.isoformat()}', policy=current) is None:
                raise ValueError('all original gap day receipts are required before tail derivation')
        codes = tuple(row[0] for row in reader._conn.execute('SELECT DISTINCT ts_code FROM daily_bar '
            'WHERE trade_date IN (SELECT unnest(?)) ORDER BY ts_code LIMIT 8001', [list(spec.plan.missing_dates)]).fetchall())
        if not codes or len(codes) > 8000:
            raise ValueError('affected original securities exceed fixed capacity')
    batches: list[DerivedTailBatchReceipt] = []
    for index in range((len(codes) + 249) // 250):
        if stopped is not None and stopped.is_set() or not maintenance_window(clock()).may_start_day:
            return False
        if guard is not None:
            guard()
        with prepare_context() if prepare_context is not None else nullcontext(lambda: None) as stop_heartbeat:
            with reader_factory(current.primary_writer_gate.primary_path) as reader:
                restored = verify_derived_tail_batch(reader, spec, f'tail-derived/batch-{index:02d}')
                if restored is None:
                    prepared = prepare_derived_tail_batch(reader, spec, codes[index * 250:(index + 1) * 250],
                        batch_index=index, universe=codes)
                elif restored.codes != codes[index * 250:(index + 1) * 250] or restored.universe_sha256 != canonical_sha256(codes):
                    raise ValueError('committed tail batch differs from original affected securities')
            stop_heartbeat()
        if restored is None:
            if guard is not None:
                guard()
            state.renew_task_claim(claim, lease_seconds=120, now=clock(), guard=lambda connection, observed:
                state.verify_maintenance_claim(connection, claim, execution_id=spec.execution_id, owner=spec.owner,
                    expected_sequence=control_sequence, now=observed))
            restored = commit_derived_tail_batch(state, claim, spec=spec, prepared=prepared, policy=policy,
                control_sequence=control_sequence, clock=clock,writer_factory=writer_factory)
        batches.append(restored)
    completion = DerivedTailCompletionReceipt(execution_id=spec.execution_id, owner=spec.owner, manifest_id=spec.manifest_id,
        plan_sha256=spec.plan.content_sha256, codes=codes, batches=tuple(batches), committed_at=clock())
    with protected_maintenance_writer(state, claim, spec=spec, policy=policy, control_sequence=control_sequence, clock=clock,writer_factory=writer_factory) as (writer, protected, _):
        writer._conn.execute('BEGIN')
        committed = False
        try:
            for receipt in batches:
                if verify_derived_tail_batch(writer, spec, receipt.task_id) != receipt:
                    raise ValueError('tail batch changed before original completion')
            existing = writer._conn.execute('SELECT receipt_id,payload_json FROM backfill_day_commit_receipt WHERE execution_id=? AND task_id=?',
                [spec.execution_id, claim.task_id]).fetchone()
            if existing is None:
                _write_tail_receipt(writer, spec, claim.task_id, completion, clock=clock)
            else:
                saved = DerivedTailCompletionReceipt.model_validate_json(existing[1])
                if saved.receipt_id != existing[0] or saved.batches != completion.batches or saved.codes != codes:
                    raise ValueError('original completed tail receipt changed')
            protected.verify(now=clock())
            writer._conn.execute('COMMIT')
            committed = True
            protected.succeed(duration_seconds=0, now=clock())
        except BaseException:
            if not committed:
                writer._conn.execute('ROLLBACK')
            raise
    return True


class DataCenterMaintenanceRuntimeConfig(RuntimeContractModel):
    replica_path: Path
    audit_state_path: Path
    audit_directory: Path
    collection_directory: Path
    collection_snapshot_root: Path | None = None
    audit_null_fields: tuple[str,...] = Field(default=('close',),min_length=1,max_length=9)
    hash_timeout_seconds: int = Field(default=600,strict=True,ge=1,le=1800)

    @model_validator(mode='after')
    def paths(self) -> DataCenterMaintenanceRuntimeConfig:
        paths=(self.replica_path,self.audit_state_path,self.audit_directory,self.collection_directory)
        if len(set(paths))!=len(paths) or any(not path.is_absolute() or path.resolve(strict=False)!=path or path.is_symlink() for path in paths):
            raise ValueError('maintenance source paths must be distinct, canonical and trusted')
        return self


class DataCenterRuntimeProfile(RuntimeContractModel):
    contract: Literal['data-center-runtime-profile/v1']='data-center-runtime-profile/v1'
    policy_path: Path
    maintenance: DataCenterMaintenanceRuntimeConfig
    plan_state_path: Path
    plan_directory: Path
    financial_archive_path: Path | None=None
    allowed_owners: tuple[str,...]=Field(min_length=1,max_length=32)
    daily_collection: CollectionRecorderConfig | None=Field(default=None,exclude_if=lambda value:value is None)
    backfill_plan_assumptions: BackfillEstimateAssumptions | None=Field(default=None,exclude_if=lambda value:value is None)

    @model_validator(mode='after')
    def paths(self) -> DataCenterRuntimeProfile:
        paths=(self.policy_path,self.plan_state_path,self.plan_directory,self.financial_archive_path)
        if any(path is not None and (not path.is_absolute() or path.resolve(strict=False)!=path or path.is_symlink()) for path in paths):
            raise ValueError('runtime profile paths must be canonical and trusted')
        if len(set(self.allowed_owners))!=len(self.allowed_owners):
            raise ValueError('runtime profile owners are duplicated')
        if self.daily_collection is not None and (self.daily_collection.collector_id!='legacy_daily'
                or self.daily_collection.owner not in self.allowed_owners):
            raise ValueError('normal daily collection must use its original authorized collector')
        return self


def load_data_center_runtime_profile(path: Path) -> DataCenterRuntimeProfile:
    from rquant.backfill_execute import _read_private_json
    return DataCenterRuntimeProfile.model_validate(_read_private_json(path,expected_sha256=None,max_bytes=64*1024))


def collect_daily_from_profile(path: Path,trade_date: str,*,clock: Callable[[],datetime] | None=None) -> int | None:
    """Annotate original daily ingestion, then publish through the original replica and audit queue."""
    from rquant.ingest import ingest_daily
    from rquant.backfill_execute import load_execution_policy
    from rquant.data_collection_authority import (CollectionCommitRecorder,seal_data_collection_proof,_calendar_facts,
        collection_source_event_id,restore_collection_report_replica)
    from rquant.data_collection_bridge import DataCollectionBridge
    from rquant.data_audit_evidence import DailyBarNullFieldSpec
    from rquant.data_audit_report_jobs import DataAuditReportJobWorker
    from rquant.research_sync import refresh_readonly_replica
    observed=clock or (lambda:datetime.now(UTC))
    profile=load_data_center_runtime_profile(path)
    config=profile.daily_collection
    if config is None:
        return ingest_daily(trade_date)
    policy=load_execution_policy(profile.policy_path)
    if config.code_commit!=policy.code_commit:
        raise ValueError('normal daily collector code differs from the installed profile')
    day=date.fromisoformat(trade_date)
    calendar=config.calendar
    if (not calendar.coverage_start<=day<=calendar.coverage_end or calendar.generated_at>observed()
            or (calendar.coverage_end-calendar.coverage_start).days>=3660):
        raise ValueError('normal daily collector lacks a current bounded original SSE calendar authority')
    gate=PrimaryWriterGate(policy.primary_writer_gate)
    recorder=CollectionCommitRecorder(config,clock=observed)
    event_id=canonical_sha256({'collector':config.collector_id,'run':config.run_id,
        'source':config.source_generation_id,'date':day,'scope':'daily_core'})
    @contextmanager
    def reader_factory() -> Iterator[DuckDBStore]:
        with gate.acquire(),DuckDBStore(policy.primary_writer_gate.primary_path,read_only=True) as reader:
            yield reader
    with reader_factory() as reader:
        _calendar_facts(reader,calendar,calendar.coverage_start,calendar.coverage_end)
    if day not in calendar.open_dates:
        return None
    def original_receipt() -> IngestionCommitReceipt | None:
        with reader_factory() as reader:
            row=reader._conn.execute('SELECT payload_json FROM ingestion_commit_receipt WHERE event_id=?',[event_id]).fetchone()
            if row is None:
                return None
            if len(row[0].encode())>256*1024:
                raise ValueError('normal daily original receipt exceeds capacity')
            receipt=IngestionCommitReceipt.model_validate_json(row[0])
            if (receipt.collector_id,receipt.run_id,receipt.owner,receipt.code_commit,receipt.source_generation_id,receipt.calendar,
                    receipt.trade_date,receipt.database_identity)!=(config.collector_id,config.run_id,config.owner,config.code_commit,
                    config.source_generation_id,config.calendar,day,DailyCanonicalPublisher.database_identity(reader)):
                raise ValueError('normal daily original receipt belongs to another source or owner')
            return recorder.verify_receipt(reader,receipt)
    receipt=original_receipt()
    if receipt is None:
        count=ingest_daily(trade_date,completion_recorder=recorder,indicator_reader_factory=reader_factory,
            writer_factory=lambda:DuckDBStore(policy.primary_writer_gate.primary_path,primary_writer_gate=policy.primary_writer_gate))
        if count==0:
            return 0
        receipt=original_receipt()
        if receipt is None:
            raise ValueError('normal daily fact commit has no original collection receipt')
    daily=next((item for item in receipt.datasets if item.dataset_id=='daily_bar'),None)
    if daily is None:
        raise ValueError('normal daily original receipt lacks its daily facts')
    maintenance=profile.maintenance
    jobs=DataAuditReportJobStore(state_path=maintenance.audit_state_path,report_directory=maintenance.audit_directory,
        collection_directory=maintenance.collection_directory,collection_snapshot_root=maintenance.collection_snapshot_root,clock=observed)
    references=(CollectionReceiptReference(kind='ingestion',receipt_id=receipt.receipt_id,
        dataset_ids=tuple(item.dataset_id for item in receipt.datasets)),)
    with gate.acquire() as lease:
        identity=CanonicalDatabaseIdentity(canonical_path=str(lease.config.primary_path),
            device=lease.config.primary_device,inode=lease.config.primary_inode)
        event=collection_source_event_id(identity,references,audit_start=config.calendar.coverage_start,observed_through=day)
        with closing(jobs._connect()) as connection:
            existing=connection.execute('SELECT 1 FROM data_collection_source WHERE event_id=?',[event]).fetchone()
        if existing is None:
            ok,reason=refresh_readonly_replica(policy.primary_writer_gate.primary_path,maintenance.replica_path,primary_writer_lease=lease)
            if not ok:
                raise RuntimeError('normal daily original replica is not sealed: '+reason)
        reference=seal_data_collection_proof(jobs,primary_path=policy.primary_writer_gate.primary_path,
            replica_path=maintenance.replica_path,calendar=config.calendar,
            references=references,
            audit_start=config.calendar.coverage_start,observed_through=day,primary_writer_lease=lease,
            clock=observed,hash_timeout_seconds=maintenance.hash_timeout_seconds)
    null_fields=tuple(DailyBarNullFieldSpec(field_name=name,max_null_numerator=0,max_null_denominator=1)
        for name in sorted(maintenance.audit_null_fields))
    bridge=DataCollectionBridge(jobs,null_fields=null_fields,hash_timeout_seconds=maintenance.hash_timeout_seconds)
    accepted=bridge.lookup(reference)
    if accepted is None:
        bridge.run_one()
        accepted=bridge.lookup(reference)
    if accepted is None:
        raise RuntimeError('normal daily original audit awaits the next bounded source round')
    if accepted.status!='succeeded':
        DataAuditReportJobWorker(jobs).run_one()
    final=jobs.status(accepted.task_id)
    if final.status!='succeeded' or final.report_hash is None:
        raise RuntimeError('normal daily original audit is unfinished')
    report=_load_completion_report(jobs,final.task_id,reference)
    if report.collection_reference!=reference:
        raise ValueError('normal daily original audit belongs to another sealed source')
    restore_collection_report_replica(jobs,final.task_id,replica_path=maintenance.replica_path,
        primary_writer_gate=policy.primary_writer_gate,hash_timeout_seconds=maintenance.hash_timeout_seconds)
    return daily.row_count


def build_data_center_execution_backend(path: Path,*,clock: Callable[[],datetime] | None=None) -> BackfillExecutePageBackend:
    from rquant.backfill_execute import load_execution_policy
    from rquant.backfill_execute_page_backend import BackfillExecutePageBackend,BackfillExecutePageBackendConfig
    profile=load_data_center_runtime_profile(path)
    policy=load_execution_policy(profile.policy_path)
    return BackfillExecutePageBackend(BackfillExecutePageBackendConfig(policy_path=profile.policy_path,
        original_state_path=policy.original_state_path,plan_state_path=profile.plan_state_path,plan_directory=profile.plan_directory,
        audit_state_path=profile.maintenance.audit_state_path,audit_directory=profile.maintenance.audit_directory,
        collection_directory=profile.maintenance.collection_directory,collection_snapshot_root=profile.maintenance.collection_snapshot_root,
        financial_archive_path=profile.financial_archive_path,replica_path=profile.maintenance.replica_path,allowed_owners=profile.allowed_owners),clock=clock)


def build_data_center_plan_backend(path: Path,*,clock: Callable[[],datetime] | None=None) -> BackfillPlanPageBackend | None:
    from rquant.backfill_execute import load_execution_policy
    from rquant.backfill_plan_page_backend import BackfillPlanPageBackend,BackfillPlanPageBackendConfig
    profile=load_data_center_runtime_profile(path)
    if profile.backfill_plan_assumptions is None:
        return None
    policy=load_execution_policy(profile.policy_path)
    return BackfillPlanPageBackend(BackfillPlanPageBackendConfig(primary_path=policy.primary_writer_gate.primary_path,
        replica_path=profile.maintenance.replica_path,state_path=profile.plan_state_path,plan_directory=profile.plan_directory,
        evidence_code_revision=policy.code_commit,assumptions=profile.backfill_plan_assumptions),clock=clock)


def build_data_center_plan_worker(path: Path,*,clock: Callable[[],datetime] | None=None,
        stop_requested: Callable[[],bool] | None=None) -> BackfillPlanJobWorker:
    """Run the original plan, then restore only its already audited collection source."""
    from rquant.backfill_execute import load_execution_policy
    from rquant.backfill_plan_jobs import BackfillPlanJobWorker, _plan_matches_request
    from rquant.data_audit_report import data_audit_report_path, load_data_audit_report
    from rquant.data_collection_authority import load_collection_proof, restore_collection_report_replica
    profile=load_data_center_runtime_profile(path)
    backend=build_data_center_plan_backend(path,clock=clock)
    if backend is None or (backend.config.state_path,backend.config.plan_directory,backend.config.replica_path,
            backend.config.assumptions)!=(profile.plan_state_path,profile.plan_directory,
                profile.maintenance.replica_path,profile.backfill_plan_assumptions):
        raise ValueError('original plan runtime profile is absent or changed')

    def on_verified_plan(request: BackfillPlanJobRequest,plan: DailyBarBackfillPlan) -> None:
        current_profile=load_data_center_runtime_profile(path)
        policy=load_execution_policy(current_profile.policy_path)
        if (current_profile!=profile or request.owner not in current_profile.allowed_owners
                or request.snapshot_path!=current_profile.maintenance.replica_path
                or request.evidence_code_revision!=policy.code_commit
                or backend.config.primary_path!=policy.primary_writer_gate.primary_path):
            raise ValueError('original plan runtime owner, source or code changed')
        maintenance=current_profile.maintenance
        jobs=DataAuditReportJobStore(state_path=maintenance.audit_state_path,
            report_directory=maintenance.audit_directory,collection_directory=maintenance.collection_directory,
            collection_snapshot_root=maintenance.collection_snapshot_root,clock=clock)
        receipt=jobs.latest_success()
        if receipt is None or receipt.report_hash is None:
            raise ValueError('original plan source lacks a succeeded collection audit')
        report=load_data_audit_report(data_audit_report_path(jobs.report_directory,receipt.report_hash))
        if not isinstance(report,CollectionDataAuditReport):
            raise ValueError('original plan source lacks a collection proof')
        proof=load_collection_proof(jobs.collection_directory,report.collection_reference)
        identity=request.snapshot_file_identity
        if (report.collection_proof!=proof or not _plan_matches_request(plan,request,proof.replica_sha256)
                or (identity.device,identity.inode,identity.size,identity.mtime_ns)!=(
                    proof.replica_device,proof.replica_inode,proof.replica_size,proof.replica_mtime_ns)):
            raise ValueError('original plan source differs from the succeeded collection proof')
        restore_collection_report_replica(jobs,receipt.task_id,replica_path=maintenance.replica_path,
            primary_writer_gate=policy.primary_writer_gate,hash_timeout_seconds=maintenance.hash_timeout_seconds,
            stop_requested=stop_requested)

    return BackfillPlanJobWorker(backend.store,on_verified_plan=on_verified_plan)


def build_data_center_audit_backend(path: Path,*,clock: Callable[[],datetime] | None=None) -> DataAuditReportPageBackend:
    from rquant.backfill_execute import load_execution_policy
    from rquant.data_audit_evidence import DailyBarNullFieldSpec
    from rquant.data_audit_report_page_backend import DataAuditReportPageBackend,DataAuditReportPageBackendConfig
    profile=load_data_center_runtime_profile(path)
    policy=load_execution_policy(profile.policy_path)
    return DataAuditReportPageBackend(DataAuditReportPageBackendConfig(primary_path=policy.primary_writer_gate.primary_path,
        replica_path=profile.maintenance.replica_path,state_path=profile.maintenance.audit_state_path,
        report_directory=profile.maintenance.audit_directory,null_fields=tuple(DailyBarNullFieldSpec(field_name=name,
            max_null_numerator=0,max_null_denominator=1) for name in sorted(profile.maintenance.audit_null_fields))),clock=clock)


def build_data_center_worker(path: Path,execution_id: str,*,owner: str,clock: Callable[[],datetime] | None=None) -> BackfillExecutionWorker:
    from rquant.adapter.tushare import TushareAdapter
    from rquant.backfill_execute import BackfillExecutionWorker,load_execution_policy
    from rquant.financial_runtime import FinancialRuntimeWorker
    profile=load_data_center_runtime_profile(path)
    if owner not in profile.allowed_owners:
        raise ValueError('runtime owner is not authorized')
    policy=lambda:load_execution_policy(profile.policy_path)
    current=policy()
    state=BackfillStateStore(current.original_state_path,maintenance_enabled=True)
    status=state.get_maintenance_status(execution_id,owner=owner)
    from rquant.data_collection_manifest import preflight_manifest_capacity
    preflight_manifest_capacity(len(state.load_manifest(status.manifest_id).tasks)-1)
    observed=(clock or (lambda:datetime.now(UTC)))()
    require_execution_policy(current,kind=status.kind,now=observed)
    if profile.maintenance.replica_path==current.primary_writer_gate.primary_path:
        raise ValueError('maintenance replica aliases primary')
    if status.kind=='financial':
        spec=state.get_financial_execution_spec(execution_id,owner=owner)
        if spec is None or spec.plan.archive_path!=profile.financial_archive_path:
            raise ValueError('financial runtime archive differs from the original manifest')
    def adapter_factory(observer: ControlledTransportObserver) -> TushareAdapter:
        adapter=TushareAdapter(transport_observer=observer)
        if adapter._backup_token:
            raise ValueError('controlled runtime cannot change the original source account')
        adapter.bind_sdk_null_normalization('tushare-nullable-v1')
        return adapter
    if status.kind=='financial':
        worker=FinancialRuntimeWorker(state,policy=policy,adapter_factory=adapter_factory,clock=clock)
    else:
        from rquant.data_collection_authority import load_collection_proof
        def original_calendar(spec: BackfillExecutionSpec) -> MarketCalendarAuthority:
            proof=load_collection_proof(profile.maintenance.collection_directory,spec.intent.source_reference)
            if proof.binding_sha256!=spec.intent.source_generation_id or proof.primary_identity!=spec.intent.primary_identity:
                raise ValueError('runtime calendar belongs to another original source')
            return proof.calendar
        worker=BackfillExecutionWorker(state,policy=policy,adapter_factory=adapter_factory,
            calendar=original_calendar,clock=clock)
    worker.completion_runtime=DataCenterCompletionRuntime(state,config=profile.maintenance,policy=policy,clock=clock)
    return worker


def run_guarded_data_center_round(worker: BackfillExecutionWorker,execution_id: str,*,owner: str) -> int:
    """Run one original task; a native supervisor enforces the later kill/reap bound."""
    import signal
    from rquant.backfill_execute import BackfillExecutionWorker
    if not isinstance(worker,BackfillExecutionWorker):
        raise ValueError('finite round requires the original maintenance worker')
    now=worker.clock()
    window=maintenance_window(now)
    if not window.may_start_day:
        worker.run_one(execution_id,owner=owner)
        return 2
    def stop(signum: int,frame: FrameType | None) -> None:
        worker.request_stop()
    previous_term=signal.getsignal(signal.SIGTERM)
    previous_alarm=signal.getsignal(signal.SIGALRM)
    signal.signal(signal.SIGTERM,stop)
    signal.signal(signal.SIGALRM,stop)
    prior_timer=signal.setitimer(signal.ITIMER_REAL,max(0.001,min(1800,(window.interrupt_at-now).total_seconds())))
    try:
        result=worker.run_one(execution_id,owner=owner)
        return 0 if result.outcome not in {'paused','partial'} else 2
    finally:
        signal.setitimer(signal.ITIMER_REAL,0)
        signal.signal(signal.SIGTERM,previous_term)
        signal.signal(signal.SIGALRM,previous_alarm)
        if prior_timer[0]>0:
            signal.setitimer(signal.ITIMER_REAL,*prior_timer)


def verify_maintenance_process_reaped(profile: DataCenterRuntimeProfile) -> None:
    from rquant.backfill_execute import load_execution_policy
    import duckdb
    policy=load_execution_policy(profile.policy_path)
    # The child's exit status cannot prove a data commit. Check actual locks only.
    with PrimaryWriterGate(policy.primary_writer_gate).acquire():
        connection=duckdb.connect(str(policy.primary_writer_gate.primary_path),read_only=True)
        try:
            connection.execute('SELECT 1').fetchone()
        finally:
            connection.close()


class MaintenanceDomainEvidence(RuntimeContractModel):
    execution_id: str
    manifest_id: str
    owner: str
    plan_sha256: str
    primary_identity: CanonicalDatabaseIdentity
    calendar: MarketCalendarAuthority
    audit_start: date
    observed_through: date
    domain_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    domain_receipt_count: int = Field(strict=True,ge=1,le=4095)
    references: tuple[CollectionReceiptReference,...] = Field(min_length=1,max_length=4095)
    receipt_manifest: CollectionReceiptManifest
    receipt_pages: tuple[CollectionManifestPage,...] = Field(min_length=1,max_length=256)


class DataCenterCompletionReceipt(RuntimeContractModel):
    contract: Literal['data-center-maintenance-completion/v1'] = 'data-center-maintenance-completion/v1'
    receipt_id: str | None = None
    execution_id: str
    owner: str
    manifest_id: str
    plan_sha256: str
    primary_identity: CanonicalDatabaseIdentity
    domain_sha256: str
    domain_receipt_count: int = Field(strict=True,ge=1,le=4095)
    collection_reference: AuditCollectionReference
    replica_sha256: str
    audit_task_id: str
    audit_report_sha256: str
    committed_claim_token: str
    committed_attempt: int = Field(strict=True,ge=1)
    control_sequence: int = Field(strict=True,ge=1)
    policy_generation: str
    committed_at: AwareUtcDatetime

    @model_validator(mode='after')
    def bind(self) -> DataCenterCompletionReceipt:
        expected=canonical_sha256(self.model_dump(mode='python',exclude={'receipt_id'}))
        if self.receipt_id is not None and self.receipt_id!=expected:
            raise ValueError('original completion receipt content changed')
        object.__setattr__(self,'receipt_id',expected)
        return self


def _load_completion_report(jobs: DataAuditReportJobStore, task_id: str, reference: AuditCollectionReference) -> CollectionDataAuditReport:
    from rquant.data_audit_report import CollectionDataAuditReport,data_audit_report_path,load_data_audit_report
    receipt=jobs.status(task_id)
    if receipt.status!='succeeded' or receipt.report_hash is None:
        raise ValueError('original completion audit has not succeeded')
    report=load_data_audit_report(data_audit_report_path(jobs.report_directory,receipt.report_hash))
    if not isinstance(report,CollectionDataAuditReport) or report.collection_reference!=reference or report.content_hash!=receipt.report_hash:
        raise ValueError('original completion audit names another exact collection source')
    return report


def _maintenance_domain(state: BackfillStateStore, store: DuckDBStore, spec: BackfillExecutionSpec | FinancialExecutionSpec, *,
        policy: DataCenterExecutionPolicy, clock: Callable[[],datetime], guard: Callable[[],None] | None = None) -> MaintenanceDomainEvidence:
    from rquant.data_collection_contracts import CollectionReceiptReference,IngestionCommitReceipt
    from rquant.data_collection_authority import CollectionCommitRecorder
    from rquant.source_quota_transport import QuotaBoundTransportObserver
    from rquant.financial_runtime import FinancialExecutionSpec,_load_runtime_receipt,verify_financial_task_receipt,build_financial_manifest
    from rquant.backfill_execute_page_backend import build_backfill_execution_manifest
    from rquant.data_collection_manifest import preflight_manifest_capacity,receipt_entry,ReceiptSetAccumulator,build_receipt_manifest
    from rquant.data_collection_authority import _original_claims
    manifest=build_financial_manifest(spec.plan) if isinstance(spec,FinancialExecutionSpec) else build_backfill_execution_manifest(spec)
    preflight_manifest_capacity(len(manifest.tasks)-1)
    if state.load_manifest(spec.manifest_id)!=manifest or DailyCanonicalPublisher.database_identity(store)!=spec.intent.primary_identity:
        raise ValueError('original completion manifest or physical primary changed')
    observer=QuotaBoundTransportObserver(path=policy.quota_ledger_path,source=policy.quota_source,
        quota_units_per_window=policy.quota_units_per_window,window_kind=policy.quota_window_kind,clock=clock)
    digest=hashlib.sha256()
    refs: list[CollectionReceiptReference]=[]
    entries=[]
    accumulator=ReceiptSetAccumulator()
    count=0
    calendar=None
    for task in manifest.tasks:
        if task.task_id=='verify-completion':
            continue
        if guard is not None:
            guard()
        if state.get_task(spec.manifest_id,task.task_id).status!='succeeded':
            raise ValueError('unfinished original domain task cannot complete maintenance')
        if isinstance(spec,FinancialExecutionSpec):
            receipt=_load_runtime_receipt(store,execution_id=spec.execution_id,task_id=task.task_id)
            if receipt is None:
                raise ValueError('original financial domain receipt is missing')
            verify_financial_task_receipt(store,receipt,spec=spec,task_id=task.task_id,policy=policy,original=observer,as_of=clock())
            dataset='financial_observation' if receipt.kind=='query_group' else 'fundamental_daily_version'
            reference=CollectionReceiptReference(kind='financial',receipt_id=receipt.receipt_id,dataset_ids=(dataset,))
            entry_kind='financial'
            calendar=spec.plan.calendar
        elif task.task_id=='tail-derived':
            reference=None
            entry_kind='derived_tail'
            row=store._conn.execute('SELECT receipt_id,payload_json FROM backfill_day_commit_receipt WHERE execution_id=? AND task_id=?',
                [spec.execution_id,task.task_id]).fetchone()
            if row is None or len(row[1].encode())>256*1024:
                raise ValueError('original derived completion receipt is missing')
            receipt=DerivedTailCompletionReceipt.model_validate_json(row[1])
            if (receipt.receipt_id,receipt.execution_id,receipt.owner,receipt.manifest_id,receipt.plan_sha256)!=(
                    row[0],spec.execution_id,spec.owner,spec.manifest_id,spec.plan.content_sha256):
                raise ValueError('original derived completion binding changed')
            for batch in receipt.batches:
                if guard is not None:
                    guard()
                if verify_derived_tail_batch(store,spec,batch.task_id)!=batch:
                    raise ValueError('original derived batch changed before completion')
        else:
            receipt=verify_backfill_day_receipt(store,spec,task_id=task.task_id,policy=policy,quota_observer=observer)
            if receipt is None:
                raise ValueError('original exact day receipt is missing')
            calendar=receipt.calendar
            row=store._conn.execute('SELECT payload_json FROM ingestion_commit_receipt WHERE collector_id=? AND run_id=? AND trade_date=?',
                ['controlled_backfill',spec.execution_id+':'+receipt.trade_date.isoformat(),receipt.trade_date]).fetchone()
            if row is None or len(row[0].encode())>256*1024:
                raise ValueError('original exact day collection receipt is missing')
            collected=CollectionCommitRecorder.verify_receipt(store,IngestionCommitReceipt.model_validate_json(row[0]))
            if collected.owner!=spec.owner or collected.database_identity!=spec.intent.primary_identity or collected.source_generation_id!=spec.intent.source_generation_id:
                raise ValueError('original collection receipt owner/source changed')
            reference=CollectionReceiptReference(kind='ingestion',receipt_id=collected.receipt_id,
                dataset_ids=tuple(sorted(claim.dataset_id for claim in collected.datasets)))
            entry_kind='day'
        entries.append(receipt_entry(receipt,index=count,task_sha256=canonical_sha256(task),kind=entry_kind,reference=reference))
        if reference is not None:
            refs.append(reference)
            accumulator.add(reference,_original_claims(store,reference,primary_identity=spec.intent.primary_identity,
                snapshot_root=None,as_of=clock(),calendar=calendar))
        digest.update(canonical_sha256((task.task_id,receipt.receipt_id)).encode())
        count+=1
    if calendar is None:
        raise ValueError('completion lacks the original calendar authority')
    start=spec.plan.start_date if isinstance(spec,FinancialExecutionSpec) else spec.plan.audit_start
    end=spec.plan.end_date if isinstance(spec,FinancialExecutionSpec) else spec.plan.completed_through
    if isinstance(spec,FinancialExecutionSpec) and sum(entry.request_count for entry in entries)!=len(spec.plan.queries):
        raise ValueError('completion lacks the complete original financial query set')
    immutable=('environment','code_commit','primary_writer_gate','original_state_path','original_state_device',
        'original_state_inode','quota_ledger_path','quota_ledger_device','quota_ledger_inode','quota_source',
        'source_account_sha256','quota_units_per_window','quota_window_kind','source_material_directory')
    root,pages=build_receipt_manifest(tuple(entries),datasets=accumulator.summaries(),execution_id=spec.execution_id,owner=spec.owner,
        manifest_id=spec.manifest_id,plan_sha256=spec.plan.content_sha256,primary_identity=spec.intent.primary_identity,
        policy_generation=spec.intent.policy_generation,source_account_sha256=policy.source_account_sha256,
        source_config_sha256=canonical_sha256({name:getattr(policy,name) for name in immutable}),quota_source=policy.quota_source,
        quota_ledger_device=policy.quota_ledger_device,quota_ledger_inode=policy.quota_ledger_inode,
        audit_start=start,observed_through=end,domain_sha256=digest.hexdigest())
    return MaintenanceDomainEvidence(execution_id=spec.execution_id,manifest_id=spec.manifest_id,owner=spec.owner,
        plan_sha256=spec.plan.content_sha256,primary_identity=spec.intent.primary_identity,calendar=calendar,audit_start=start,
        observed_through=end,domain_sha256=digest.hexdigest(),domain_receipt_count=count,
        references=tuple(refs),receipt_manifest=root,receipt_pages=pages)


class DataCenterCompletionRuntime:
    """Finish the original finite manifest after its real replica and original audit succeed."""
    def __init__(self,state: BackfillStateStore,*,config: DataCenterMaintenanceRuntimeConfig,
            policy: Callable[[],DataCenterExecutionPolicy],clock: Callable[[],datetime] | None = None) -> None:
        from rquant.data_audit_report_jobs import DataAuditReportJobStore
        self.state,self.config,self.policy=state,DataCenterMaintenanceRuntimeConfig.model_validate(config),policy
        self.clock=clock or (lambda:datetime.now(UTC))
        self.jobs=DataAuditReportJobStore(state_path=config.audit_state_path,report_directory=config.audit_directory,
            collection_directory=config.collection_directory,collection_snapshot_root=config.collection_snapshot_root,clock=self.clock)

    def run(self,spec: BackfillExecutionSpec | FinancialExecutionSpec,claim: ClaimedBackfillTask,control_sequence: int,*,
            prepare_context: Callable[[],AbstractContextManager[Callable[[],None]]],guard: Callable[[],None],
            stopped: Event,writer_factory: Callable[[PrimaryWriterLease],DuckDBStore],
            reader_factory: Callable[[Path],DuckDBStore] | None=None) -> bool:
        from rquant.financial_runtime import FinancialExecutionSpec
        from rquant.data_collection_authority import (seal_data_collection_proof,load_collection_proof,
            collection_source_event_id,republish_verified_collection_sidecar)
        from rquant.data_collection_bridge import DataCollectionBridge
        from rquant.data_audit_evidence import DailyBarNullFieldSpec
        from rquant.data_audit_report_jobs import DataAuditReportJobWorker
        from rquant.research_sync import refresh_readonly_replica
        reader_factory=reader_factory or (lambda path:DuckDBStore(path,read_only=True))
        kind='financial' if isinstance(spec,FinancialExecutionSpec) else 'backfill'
        if claim.task_id!='verify-completion':
            raise ValueError('original claim is not the finite completion task')
        guard()
        current=self.policy()
        require_execution_policy(current,kind=kind,now=self.clock())
        require_stable_execution_policy(current,spec)
        with prepare_context() as stop_heartbeat:
            with PrimaryWriterGate(current.primary_writer_gate).acquire() as lease:
                with reader_factory(current.primary_writer_gate.primary_path) as reader:
                    domain=_maintenance_domain(self.state,reader,spec,policy=current,clock=self.clock,guard=guard)
                event=collection_source_event_id(domain.primary_identity,domain.references,
                    audit_start=domain.audit_start,observed_through=domain.observed_through,receipt_manifest=domain.receipt_manifest)
                with closing(self.jobs._connect()) as connection:
                    existing_source=connection.execute('SELECT 1 FROM data_collection_source WHERE event_id=?',[event]).fetchone()
                if existing_source is None:
                    guard()
                    ok,detail=refresh_readonly_replica(current.primary_writer_gate.primary_path,self.config.replica_path,primary_writer_lease=lease)
                    if not ok:
                        raise ValueError(detail)
                reference=seal_data_collection_proof(self.jobs,primary_path=current.primary_writer_gate.primary_path,
                    replica_path=self.config.replica_path,calendar=domain.calendar,references=domain.references,audit_start=domain.audit_start,
                    observed_through=domain.observed_through,primary_writer_lease=lease,clock=self.clock,
                    hash_timeout_seconds=self.config.hash_timeout_seconds,stop_requested=stopped.is_set,
                    receipt_manifest=domain.receipt_manifest,receipt_pages=domain.receipt_pages)
            guard()
            bridge=DataCollectionBridge(self.jobs,null_fields=tuple(DailyBarNullFieldSpec(field_name=name,max_null_numerator=0,
                max_null_denominator=1) for name in self.config.audit_null_fields),stop_requested=stopped.is_set,
                hash_timeout_seconds=self.config.hash_timeout_seconds)
            accepted=bridge.lookup(reference)
            if accepted is None:
                try:
                    bridge.run_one()
                except ValueError as error:
                    if str(error) not in {'another audit report task is active','audit report task cooldown has not elapsed'}:
                        raise
                accepted=bridge.lookup(reference)
            guard()
            if accepted is not None and accepted.status=='failed':
                accepted=self.jobs.retry_failed(accepted.task_id,expected_collection_reference=reference,maintenance_recovery=True)
            if accepted is None or accepted.status in {'queued','running'}:
                DataAuditReportJobWorker(self.jobs).run_one()
                accepted=bridge.lookup(reference)
            if accepted is None or accepted.status in {'queued','running'}:
                stop_heartbeat()
                self.state.release_task_claim(claim,now=self.clock())
                return False
            if accepted.status!='succeeded':
                raise ValueError('original completion audit failed')
            report=_load_completion_report(self.jobs,accepted.task_id,reference)
            proof=load_collection_proof(self.config.collection_directory,reference)
            if (report.collection_proof,report.audit_start,report.observed_through)!=(proof,domain.audit_start,domain.observed_through):
                raise ValueError('original report and completion source changed')
            stop_heartbeat()
        guard()
        self.state.renew_task_claim(claim,lease_seconds=120,now=self.clock(),guard=lambda connection,now:
            self.state.verify_maintenance_claim(connection,claim,execution_id=spec.execution_id,owner=spec.owner,
                expected_sequence=control_sequence,now=now))
        with prepare_context() as stop_heartbeat,PrimaryWriterGate(current.primary_writer_gate).acquire() as final_lease:
            with reader_factory(current.primary_writer_gate.primary_path) as reader:
                actual=_maintenance_domain(self.state,reader,spec,policy=current,clock=self.clock,guard=guard)
            if actual!=domain:
                raise ValueError('original facts changed after replica/audit completion')
            _load_completion_report(self.jobs,accepted.task_id,reference)
            republish_verified_collection_sidecar(proof,replica_path=self.config.replica_path,primary_writer_lease=final_lease,
                hash_timeout_seconds=self.config.hash_timeout_seconds,stop_requested=stopped.is_set)
            guard()
            before=current.primary_writer_gate.primary_path.stat(follow_symlinks=False)
            stop_heartbeat()
            with protected_maintenance_writer(self.state,claim,spec=spec,policy=self.policy,control_sequence=control_sequence,
                    clock=self.clock,kind=kind,writer_factory=writer_factory,primary_writer_lease=final_lease) as (writer,protected,current):
                after=current.primary_writer_gate.primary_path.stat(follow_symlinks=False)
                if (after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns)!=(
                        before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns,before.st_ctime_ns):
                    raise ValueError('primary changed between original completion validation and transaction')
                writer._conn.execute('BEGIN')
                committed=False
                try:
                    row=writer._conn.execute('SELECT receipt_id,payload_json FROM backfill_day_commit_receipt WHERE execution_id=? AND task_id=?',
                        [spec.execution_id,claim.task_id]).fetchone()
                    if row is None:
                        completion=DataCenterCompletionReceipt(execution_id=spec.execution_id,owner=spec.owner,manifest_id=spec.manifest_id,
                            plan_sha256=spec.plan.content_sha256,primary_identity=domain.primary_identity,domain_sha256=domain.domain_sha256,
                            domain_receipt_count=domain.domain_receipt_count,collection_reference=reference,replica_sha256=proof.replica_sha256,
                            audit_task_id=accepted.task_id,audit_report_sha256=accepted.report_hash,committed_claim_token=claim.claim_token,
                            committed_attempt=claim.attempt,control_sequence=control_sequence,policy_generation=current.policy_generation,committed_at=self.clock())
                        writer._conn.execute('INSERT INTO backfill_day_commit_receipt VALUES (?,?,?,?,?,?)',
                            [spec.execution_id,claim.task_id,completion.receipt_id,domain.audit_start,completion.model_dump_json(),self.clock()])
                    else:
                        if len(row[1].encode())>256*1024:
                            raise ValueError('original completion receipt exceeds recovery capacity')
                        completion=DataCenterCompletionReceipt.model_validate_json(row[1])
                        if (completion.receipt_id,completion.execution_id,completion.owner,completion.manifest_id,completion.plan_sha256,
                                completion.domain_sha256,completion.domain_receipt_count,completion.collection_reference,
                                completion.replica_sha256,completion.audit_task_id,completion.audit_report_sha256)!=(
                                row[0],spec.execution_id,spec.owner,spec.manifest_id,spec.plan.content_sha256,domain.domain_sha256,
                                domain.domain_receipt_count,reference,proof.replica_sha256,accepted.task_id,accepted.report_hash):
                            raise ValueError('original completion recovery evidence changed')
                    protected.verify(now=self.clock())
                    writer._conn.execute('COMMIT')
                    committed=True
                    protected.succeed(duration_seconds=0,now=self.clock())
                    self.state.transition_maintenance(spec.execution_id,owner=spec.owner,expected_sequence=control_sequence,status='completed',
                        completion_sha256=completion.receipt_id,audit_task_id=accepted.task_id,audit_report_sha256=accepted.report_hash,
                        now=self.clock(),connection=protected.connection)
                except BaseException:
                    if not committed:
                        writer._conn.execute('ROLLBACK')
                    raise
        return True
