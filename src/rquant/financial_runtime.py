"""Finite financial collection through the original archive, state and PIT functions."""
from __future__ import annotations

import hashlib
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Literal
from uuid import UUID
from threading import Event

import pandas as pd
from pydantic import Field, model_validator

from rquant.backfill_execute import BackfillExecutionWorker, ControlledTransportObserver, cleanup_verified_source_material, require_execution_policy, require_stable_execution_policy
from rquant.backfill_execute_contracts import BackfillSourceRequestBinding, DataCenterExecutionPolicy, FINANCIAL_EXECUTE_APIS, MaintenanceExecutionStatus
from rquant.daily_canonical_publisher import CanonicalDatabaseIdentity
from rquant.data_collection_contracts import DatasetCollectionClaim
from rquant.financial_pit_acquisition import FinancialAPI, FinancialArchive, FinancialCommittedPage, FinancialQuery, FinancialReceipt, _SHANGHAI, acquire_financial_batches
from rquant.financial_pit_facts import _cursor, _import_page, _json
from rquant.fundamental_daily import FundamentalDailyQuery, FundamentalDailyVersion, PreparedFundamentalDaily, TsCode, derive_fundamental_daily, prepare_fundamental_daily, read_fundamental_daily
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.source_quota_transport import QuotaBoundTransportObserver, SourceTransportCallReceipt

if TYPE_CHECKING:
    from rquant.backfill_state import BackfillManifestInput, BackfillStateStore, ClaimedBackfillTask
    from rquant.storage.duckdb import DuckDBStore
    from rquant.storage.primary_writer_gate import PrimaryWriterLease


def _query_ranges(queries: tuple[FinancialQuery, ...]) -> tuple[tuple[int, int], ...]:
    ranges: list[tuple[int, int]] = []
    start = 0
    symbols: set[str] = set()
    for index, query in enumerate(queries):
        if index - start == 32 or query.ts_code not in symbols and len(symbols) == 16:
            ranges.append((start, index))
            start, symbols = index, set()
        symbols.add(query.ts_code)
    if queries:
        ranges.append((start, len(queries)))
    return tuple(ranges)


class FinancialCollectionPlan(RuntimeContractModel):
    contract: Literal['financial-collection-plan/v1'] = 'financial-collection-plan/v1'
    content_sha256: str | None = None
    scope_sha256: str | None = None
    owner: str = Field(min_length=1, max_length=256)
    prepare_command_id: str = Field(min_length=1, max_length=128)
    source_generation_id: str = Field(pattern=r'^[0-9a-f]{64}$')
    primary_identity: CanonicalDatabaseIdentity
    calendar: MarketCalendarAuthority
    archive_path: Path
    archive_id: str = Field(pattern=r'^[0-9a-f]{32}$')
    securities: tuple[TsCode, ...] = Field(min_length=1, max_length=8000)
    start_date: date
    end_date: date
    report_periods: tuple[date, ...] = Field(min_length=1, max_length=41)
    code_commit: str = Field(pattern=r'^[0-9a-f]{40}$')
    queries: tuple[FinancialQuery, ...] = Field(min_length=1, max_length=100000)

    def scope_body(self) -> dict[str, object]:
        return self.model_dump(mode='python', exclude={'content_sha256', 'scope_sha256', 'queries'})

    @property
    def execution_id(self) -> str:
        return canonical_sha256({'contract': 'financial-execution/v1', 'scope': self.scope_sha256})

    @property
    def manifest_id(self) -> str:
        return canonical_sha256({'contract': 'financial-original-manifest/v1', 'scope': self.scope_sha256})

    @property
    def decision_dates(self) -> tuple[date, ...]:
        return tuple(day for day in self.calendar.open_dates if self.start_date <= day <= self.end_date)

    @model_validator(mode='after')
    def bind(self) -> FinancialCollectionPlan:
        if not 1 <= (self.end_date - self.start_date).days + 1 <= 3660:
            raise ValueError('financial range exceeds fixed capacity')
        if self.securities != tuple(sorted(set(self.securities))) or self.report_periods != tuple(sorted(set(self.report_periods))):
            raise ValueError('financial securities or periods are not unique and ordered')
        if any(period > self.end_date or (period.month, period.day) not in {(3,31),(6,30),(9,30),(12,31)} for period in self.report_periods):
            raise ValueError('financial report period is outside the fixed source scope')
        if not self.archive_path.is_absolute() or self.archive_path.resolve(strict=False) != self.archive_path:
            raise ValueError('financial archive path must be canonical and trusted')
        if self.calendar.coverage_start > self.start_date or self.calendar.coverage_end < self.end_date or not self.decision_dates:
            raise ValueError('financial decision dates require the complete original SSE calendar')
        scope = canonical_sha256(self.scope_body())
        if self.scope_sha256 is not None and self.scope_sha256 != scope:
            raise ValueError('financial fixed scope identity changed')
        object.__setattr__(self, 'scope_sha256', scope)
        expected = _build_queries(self)
        if self.queries != expected:
            raise ValueError('original financial queries differ from fixed manifest identity')
        task_count = len(_query_ranges(self.queries)) + (len(self.securities) * len(self.decision_dates) + 249) // 250 + 1
        if task_count > 4096 or len(self.model_dump_json().encode()) > 8_000_000:
            raise ValueError('financial original manifest exceeds fixed capacity')
        digest = canonical_sha256(self.model_dump(mode='python', exclude={'content_sha256'}))
        if self.content_sha256 is not None and self.content_sha256 != digest:
            raise ValueError('financial plan content changed')
        object.__setattr__(self, 'content_sha256', digest)
        return self

    @classmethod
    def create(cls, **scope: object) -> FinancialCollectionPlan:
        preliminary = cls.model_construct(**scope)
        object.__setattr__(preliminary, 'scope_sha256', canonical_sha256(preliminary.scope_body()))
        queries = _build_queries(preliminary)
        return cls.model_validate({**scope, 'queries': queries})


def _build_queries(plan: FinancialCollectionPlan) -> tuple[FinancialQuery, ...]:
    days = (plan.end_date - plan.start_date).days + 1
    windows = (days + 30) // 31
    count = len(plan.securities) * (len(plan.report_periods) + 5 * windows + days)
    if days < 1 or days > 3660 or count > 100000:
        raise ValueError('financial request scope exceeds fixed capacity')
    queries: list[FinancialQuery] = []
    for code in plan.securities:
        parameters: list[dict[str, object]] = [dict(api='fina_indicator', ts_code=code, period=period) for period in plan.report_periods]
        for api in ('income', 'balancesheet', 'cashflow', 'forecast', 'express'):
            start = plan.start_date
            while start <= plan.end_date:
                end = min(start + timedelta(days=30), plan.end_date)
                parameters.append(dict(api=api, ts_code=code, start_date=start, end_date=end))
                start = end + timedelta(days=1)
        parameters.extend(dict(api='dividend', ts_code=code, ann_date=plan.start_date + timedelta(days=offset)) for offset in range(days))
        for values in parameters:
            position = len(queries)
            request_id = UUID(hex=canonical_sha256({'manifest': plan.manifest_id, 'position': position, 'query': values})[:32])
            queries.append(FinancialQuery(request_id=request_id, **values))
    return tuple(queries)


def build_financial_manifest(plan: FinancialCollectionPlan) -> BackfillManifestInput:
    from rquant.backfill_state import BackfillManifestInput, BackfillTaskInput
    raw = tuple(BackfillTaskInput(task_id=f'financial-raw-{index:04d}', payload={
        'kind': 'original_financial_query_group', 'query_start': start, 'query_end': end,
    }, max_attempts=6) for index, (start, end) in enumerate(_query_ranges(plan.queries)))
    decisions = len(plan.securities) * len(plan.decision_dates)
    derived = tuple(BackfillTaskInput(task_id=f'financial-tail-{index:04d}', payload={
        'kind': 'original_fundamental_group', 'query_start': start, 'query_end': min(start + 250, decisions),
    }, max_attempts=6) for index, start in enumerate(range(0, decisions, 250)))
    return BackfillManifestInput(manifest_id=plan.manifest_id, payload={'contract': 'financial-original-manifest/v1',
        'execution_id': plan.execution_id, 'owner': plan.owner, 'plan_sha256': plan.content_sha256},
        tasks=raw + derived + (BackfillTaskInput(task_id='verify-completion', payload={'kind':'replica_and_original_audit'}, max_attempts=6),), eligibility=())


class FinancialExecutionIntent(RuntimeContractModel):
    contract: Literal['financial-execution-intent/v1'] = 'financial-execution-intent/v1'
    intent_id: str | None = None
    execution_id: str
    owner: str
    prepare_command_id: str
    plan: FinancialCollectionPlan
    plan_sha256: str
    source_generation_id: str
    primary_identity: CanonicalDatabaseIdentity
    policy_generation: str
    nonce_sha256: str
    issued_at: AwareUtcDatetime
    expires_at: AwareUtcDatetime
    prepare_request_sha256: str | None = Field(default=None,pattern=r'^[0-9a-f]{64}$')

    @model_validator(mode='after')
    def bind(self) -> FinancialExecutionIntent:
        if (self.execution_id, self.owner, self.prepare_command_id, self.plan_sha256, self.source_generation_id, self.primary_identity) != (
                self.plan.execution_id, self.plan.owner, self.plan.prepare_command_id, self.plan.content_sha256,
                self.plan.source_generation_id, self.plan.primary_identity) or self.expires_at - self.issued_at != timedelta(minutes=5):
            raise ValueError('financial confirmation differs from fixed source/owner/plan')
        body=self.model_dump(mode='python', exclude={'intent_id'})
        if self.prepare_request_sha256 is None:
            body.pop('prepare_request_sha256')
        expected = canonical_sha256(body)
        if self.intent_id is not None and self.intent_id != expected:
            raise ValueError('financial confirmation content changed')
        object.__setattr__(self, 'intent_id', expected)
        return self


class FinancialExecutionSpec(RuntimeContractModel):
    contract: Literal['financial-execution-spec/v1'] = 'financial-execution-spec/v1'
    execution_id: str
    owner: str
    manifest_id: str
    plan: FinancialCollectionPlan
    intent: FinancialExecutionIntent
    execute_command_id: str = Field(min_length=1, max_length=128)
    admission_policy: DataCenterExecutionPolicy

    @model_validator(mode='after')
    def bind(self) -> FinancialExecutionSpec:
        if (self.execution_id, self.owner, self.manifest_id, self.plan) != (
                self.intent.execution_id, self.intent.owner, self.plan.manifest_id, self.intent.plan
                ) or self.admission_policy.policy_generation != self.intent.policy_generation:
            raise ValueError('financial execution differs from original confirmation')
        return self


def require_financial_plan_policy(plan: FinancialCollectionPlan, policy: DataCenterExecutionPolicy, *, now: datetime) -> None:
    if policy.code_commit != plan.code_commit or (str(policy.primary_writer_gate.primary_path), policy.primary_writer_gate.primary_device,
            policy.primary_writer_gate.primary_inode) != (plan.primary_identity.canonical_path, plan.primary_identity.device, plan.primary_identity.inode):
        raise ValueError('financial plan belongs to another original primary/code')
    rights = {api: require_execution_policy(policy, kind='financial', now=now, api_name=api,
        parameters=next(query for query in plan.queries if query.api == api).supplier_parameters()) for api in FINANCIAL_EXECUTE_APIS}
    for query in plan.queries:
        rights[query.api].require_current(parameters=query.supplier_parameters(), now=now)


def financial_request_binding(spec: FinancialExecutionSpec, query: FinancialQuery, policy: DataCenterExecutionPolicy) -> BackfillSourceRequestBinding:
    return BackfillSourceRequestBinding(owner=spec.owner, execution_id=spec.execution_id, manifest_id=spec.manifest_id,
        plan_sha256=spec.plan.content_sha256, scope_sha256=canonical_sha256(query.model_dump(mode='python')), api_name=query.api,
        parameters=query.supplier_parameters(), source_account_sha256=policy.source_account_sha256, quota_source=policy.quota_source,
        quota_ledger_device=policy.quota_ledger_device, quota_ledger_inode=policy.quota_ledger_inode,
        source_normalization_version='tushare-nullable-v1')


class ControlledFinancialAdapter:
    def __init__(self, adapter: object, observer: ControlledTransportObserver, *, spec: FinancialExecutionSpec,
            policy: DataCenterExecutionPolicy, queries: tuple[FinancialQuery, ...]) -> None:
        from rquant.adapter.tushare import TushareAdapter
        if not isinstance(adapter, TushareAdapter) or adapter._transport_observer is not observer or adapter._backup_token:
            raise ValueError('financial execution requires the actual bound original adapter without backup')
        self.adapter, self.observer, self.spec, self.policy, self.queries = adapter, observer, spec, policy, queries

    def _fetch(self, api: FinancialAPI, parameters: dict[str, str]) -> pd.DataFrame:
        matches = tuple(query for query in self.queries if query.api == api and query.supplier_parameters() == parameters)
        if len(matches) != 1:
            raise ValueError('financial SDK call is outside the exact original query group')
        binding = financial_request_binding(self.spec, matches[0], self.policy)
        with self.observer.request(binding):
            return getattr(self.adapter, api)(**parameters)

    def fina_indicator(self, *, ts_code: str, period: str) -> pd.DataFrame:
        return self._fetch('fina_indicator', dict(ts_code=ts_code, period=period))

    def income(self, *, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        return self._fetch('income', dict(ts_code=ts_code, start_date=start_date, end_date=end_date))

    def balancesheet(self, *, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        return self._fetch('balancesheet', dict(ts_code=ts_code, start_date=start_date, end_date=end_date))

    def cashflow(self, *, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        return self._fetch('cashflow', dict(ts_code=ts_code, start_date=start_date, end_date=end_date))

    def forecast(self, *, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        return self._fetch('forecast', dict(ts_code=ts_code, start_date=start_date, end_date=end_date))

    def express(self, *, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        return self._fetch('express', dict(ts_code=ts_code, start_date=start_date, end_date=end_date))

    def dividend(self, *, ts_code: str, ann_date: str) -> pd.DataFrame:
        return self._fetch('dividend', dict(ts_code=ts_code, ann_date=ann_date))


class FinancialImportCursor(RuntimeContractModel):
    archive_id: str
    last_observed_at: AwareUtcDatetime | None
    anchor_generation: int = Field(strict=True, ge=0)
    anchor_record_sha256: str

    @classmethod
    def from_original(cls, value: tuple[str, datetime | None, int, str] | None) -> FinancialImportCursor | None:
        return None if value is None else cls(archive_id=value[0], last_observed_at=value[1], anchor_generation=value[2], anchor_record_sha256=value[3])


class FinancialRuntimeReceipt(RuntimeContractModel):
    contract: Literal['financial-runtime-receipt/v1'] = 'financial-runtime-receipt/v1'
    receipt_id: str | None = None
    execution_id: str
    owner: str
    manifest_id: str
    plan_sha256: str
    task_id: str
    kind: Literal['import_page', 'query_group', 'fundamental_group']
    primary_identity: CanonicalDatabaseIdentity
    archive_id: str
    original_receipts: tuple[FinancialReceipt, ...] = Field(default=(), max_length=32)
    source_requests: tuple[BackfillSourceRequestBinding, ...] = Field(default=(), max_length=32)
    dispatch_receipts: tuple[SourceTransportCallReceipt, ...] = Field(default=(), max_length=192)
    cursor_after: FinancialImportCursor | None = None
    imported_rows_sha256: str | None = None
    original_versions: tuple[FundamentalDailyVersion, ...] = Field(default=(), max_length=250)
    committed_claim_token: str
    committed_attempt: int = Field(strict=True, ge=1)
    control_sequence: int = Field(strict=True, ge=1)
    policy_generation: str
    committed_at: AwareUtcDatetime

    @model_validator(mode='after')
    def bind(self) -> FinancialRuntimeReceipt:
        if self.kind in {'query_group', 'import_page'} and (not self.original_receipts or self.cursor_after is None or self.imported_rows_sha256 is None):
            raise ValueError('financial runtime lacks the original imported evidence')
        if self.kind == 'query_group' and (not self.source_requests or not self.dispatch_receipts):
            raise ValueError('financial query group lacks original actual dispatch receipts')
        if self.kind == 'fundamental_group' and not self.original_versions:
            raise ValueError('financial daily group lacks original six-field versions')
        if self.kind == 'query_group':
            if len(self.source_requests) != len(self.original_receipts) or self.original_versions:
                raise ValueError('financial query group mixes source or derived evidence')
            for binding, receipt in zip(self.source_requests, self.original_receipts, strict=True):
                if (binding.owner, binding.execution_id, binding.manifest_id, binding.plan_sha256,
                        binding.api_name, binding.parameters, binding.scope_sha256) != (
                        self.owner, self.execution_id, self.manifest_id, self.plan_sha256, receipt.query.api,
                        receipt.query.supplier_parameters(), canonical_sha256(receipt.query.model_dump(mode='python'))):
                    raise ValueError('financial dispatch scope differs from original archive query')
        elif self.source_requests or self.dispatch_receipts:
            raise ValueError('only actual query groups carry dispatch evidence')
        if self.kind == 'fundamental_group' and (self.original_receipts or self.cursor_after or self.imported_rows_sha256):
            raise ValueError('financial daily group mixes raw and derived evidence')
        if self.kind == 'import_page' and self.original_versions:
            raise ValueError('financial import page cannot carry derived versions')
        expected = canonical_sha256(self.model_dump(mode='python', exclude={'receipt_id'}))
        if self.receipt_id is not None and self.receipt_id != expected:
            raise ValueError('financial runtime receipt content changed')
        object.__setattr__(self, 'receipt_id', expected)
        return self


def _imported_digest(store: DuckDBStore, archive_id: str, receipts: tuple[FinancialReceipt, ...]) -> str:
    digest = hashlib.sha256()
    for receipt in receipts:
        query = receipt.query
        batch = store._conn.execute('SELECT * FROM financial_import_batch WHERE archive_id=? AND request_id=?',
            [archive_id, str(query.request_id)]).fetchone()
        expected = (archive_id, str(query.request_id), _json(query.model_dump(mode='json')), receipt.observed_at,
            receipt.status, receipt.row_count, receipt.relative_path, receipt.file_sha256, receipt.byte_count)
        if batch != expected:
            raise ValueError('original financial imported batch differs from sealed receipt')
        cursor = store._conn.execute('SELECT * FROM financial_observation WHERE archive_id=? AND request_id=? ORDER BY row_index',
            [archive_id, str(query.request_id)])
        seen = 0
        raw_bytes = 0
        while rows := cursor.fetchmany(128):
            for row in rows:
                seen += 1
                raw_bytes += len(row[10].encode())
                if seen > 5000 or raw_bytes > 8_000_000 or row[2] != seen - 1:
                    raise ValueError('original financial observation capacity or row sequence changed')
                digest.update(canonical_sha256(row).encode())
        if seen != receipt.row_count:
            raise ValueError('original financial observation count differs from sealed receipt')
        digest.update(canonical_sha256(batch).encode())
    return digest.hexdigest()


def _load_runtime_receipt(store: DuckDBStore, *, receipt_id: str | None = None,
        execution_id: str | None = None, task_id: str | None = None) -> FinancialRuntimeReceipt | None:
    if receipt_id is not None:
        row = store._conn.execute('SELECT receipt_id,payload_json FROM data_center_financial_runtime_receipt WHERE receipt_id=?', [receipt_id]).fetchone()
    else:
        row = store._conn.execute('SELECT receipt_id,payload_json FROM data_center_financial_runtime_receipt WHERE execution_id=? AND task_id=?', [execution_id, task_id]).fetchone()
    if row is None:
        return None
    if len(row[1].encode()) > 8_000_000:
        raise ValueError('financial runtime receipt exceeds capacity')
    receipt = FinancialRuntimeReceipt.model_validate_json(row[1])
    if receipt.receipt_id != row[0]:
        raise ValueError('original financial runtime receipt identity changed')
    return receipt


def verify_financial_runtime_receipt(store: DuckDBStore, receipt_id: str, *, as_of: datetime,
        primary_identity: CanonicalDatabaseIdentity | None = None) -> tuple[DatasetCollectionClaim, ...]:
    from rquant.daily_canonical_publisher import DailyCanonicalPublisher
    receipt = _load_runtime_receipt(store, receipt_id=receipt_id)
    expected_primary = primary_identity or DailyCanonicalPublisher.database_identity(store)
    if receipt is None or receipt.committed_at > as_of or receipt.primary_identity != expected_primary:
        raise ValueError('original financial runtime source is unavailable or changed')
    if receipt.original_receipts:
        cursor = FinancialImportCursor.from_original(_cursor(store._conn))
        if (cursor is None or cursor.archive_id != receipt.archive_id or cursor.anchor_generation < receipt.cursor_after.anchor_generation
                or cursor.last_observed_at is None or cursor.last_observed_at < receipt.cursor_after.last_observed_at
                or _imported_digest(store, receipt.archive_id, receipt.original_receipts) != receipt.imported_rows_sha256):
            raise ValueError('original financial imported evidence changed')
        return tuple(DatasetCollectionClaim(dataset_id='financial_observation', trade_date=item.observed_at.date(),
            scope='actual_financial_queries', row_count=item.row_count, source_api=item.query.api,
            content_sha256=canonical_sha256((receipt.archive_id, item))) for item in receipt.original_receipts)
    for version in receipt.original_versions:
        if read_fundamental_daily(store._conn, FundamentalDailyQuery(ts_code=version.ts_code, trade_date=version.trade_date), version_id=version.version_id) != version:
            raise ValueError('original fundamental version changed')
    return tuple(DatasetCollectionClaim(dataset_id='fundamental_daily_version', trade_date=version.trade_date,
        scope='actual_financial_queries', row_count=1, source_api='financial_pit', content_sha256=version.version_id) for version in receipt.original_versions)


def _write_runtime_receipt(store: DuckDBStore, receipt: FinancialRuntimeReceipt) -> None:
    payload = receipt.model_dump_json()
    if len(payload.encode()) > 8_000_000:
        raise ValueError('financial runtime receipt exceeds capacity')
    store._conn.execute('INSERT INTO data_center_financial_runtime_receipt VALUES (?,?,?,?,?)',
        [receipt.execution_id, receipt.task_id, receipt.receipt_id, payload, receipt.committed_at])


def _receipt(spec: FinancialExecutionSpec, claim: ClaimedBackfillTask, policy: DataCenterExecutionPolicy, *, task_id: str,
        kind: Literal['import_page','query_group','fundamental_group'], control_sequence: int, clock: Callable[[],datetime],
        **evidence: object) -> FinancialRuntimeReceipt:
    return FinancialRuntimeReceipt(execution_id=spec.execution_id, owner=spec.owner, manifest_id=spec.manifest_id,
        plan_sha256=spec.plan.content_sha256, task_id=task_id, kind=kind, primary_identity=spec.plan.primary_identity,
        archive_id=spec.plan.archive_id, committed_claim_token=claim.claim_token, committed_attempt=claim.attempt,
        control_sequence=control_sequence, policy_generation=policy.policy_generation, committed_at=clock(), **evidence)


def commit_financial_import_page(state: BackfillStateStore, claim: ClaimedBackfillTask, *, spec: FinancialExecutionSpec,
        page: FinancialCommittedPage, expected_cursor: tuple[str, datetime | None, int, str] | None,
        policy: Callable[[],DataCenterExecutionPolicy], control_sequence: int, clock: Callable[[],datetime],
        writer_factory: Callable[[PrimaryWriterLease],DuckDBStore] | None=None) -> FinancialRuntimeReceipt:
    from rquant.data_center_maintenance_runtime import protected_maintenance_writer
    if page.archive_id != spec.plan.archive_id or not page.entries or len(page.entries) > 32 or sum(entry.receipt.byte_count for entry in page.entries) > 32 * 1024 * 1024:
        raise ValueError('original financial import page is outside fixed capacity/source')
    archive = FinancialArchive(spec.plan.archive_path)
    actual = archive.committed_page(after=expected_cursor[1] if expected_cursor is not None else None,
        limit=len(page.entries), accepted_anchor_generation=page.anchor_generation,
        accepted_anchor_sha256=page.anchor_record_sha256)
    if actual.archive_id != page.archive_id or actual.entries != page.entries or actual.high_water < page.high_water:
        raise ValueError('prepared financial page differs from the original immutable archive')
    task_id = f'{claim.task_id}/import-{canonical_sha256(tuple(entry.receipt for entry in page.entries))[:32]}'
    with protected_maintenance_writer(state, claim, spec=spec, policy=policy, control_sequence=control_sequence, clock=clock, kind='financial',writer_factory=writer_factory) as (writer, protected, current):
        writer._conn.execute('BEGIN')
        committed = False
        try:
            restored = _load_runtime_receipt(writer, execution_id=spec.execution_id, task_id=task_id)
            if restored is not None:
                verify_financial_runtime_receipt(writer, restored.receipt_id, as_of=clock())
                writer._conn.execute('ROLLBACK')
                return restored
            _import_page(writer._conn, page, expected_cursor, transaction_mode='existing')
            receipts = tuple(entry.receipt for entry in page.entries)
            receipt = _receipt(spec, claim, current, task_id=task_id, kind='import_page', control_sequence=control_sequence,
                clock=clock, original_receipts=receipts, cursor_after=FinancialImportCursor.from_original(_cursor(writer._conn)),
                imported_rows_sha256=_imported_digest(writer, page.archive_id, receipts))
            _write_runtime_receipt(writer, receipt)
            protected.verify(now=clock())
            writer._conn.execute('COMMIT')
            committed = True
            return receipt
        except BaseException:
            if not committed:
                writer._conn.execute('ROLLBACK')
            raise


def commit_fundamental_group(state: BackfillStateStore, claim: ClaimedBackfillTask, *, spec: FinancialExecutionSpec,
        prepared: tuple[PreparedFundamentalDaily, ...], policy: Callable[[],DataCenterExecutionPolicy], control_sequence: int,
        clock: Callable[[],datetime],writer_factory: Callable[[PrimaryWriterLease],DuckDBStore] | None=None) -> FinancialRuntimeReceipt:
    from rquant.data_center_maintenance_runtime import protected_maintenance_writer
    if not prepared or len(prepared) > 250:
        raise ValueError('original fundamental group exceeds finite capacity')
    task = state.get_task(claim.manifest_id, claim.task_id)
    if not claim.task_id.startswith('financial-tail-') or tuple(item.query for item in prepared) != fundamental_group_queries(
            spec.plan, start=task.payload['query_start'], end=task.payload['query_end']):
        raise ValueError('prepared six fields differ from the original exact financial task')
    with protected_maintenance_writer(state, claim, spec=spec, policy=policy, control_sequence=control_sequence, clock=clock, kind='financial',writer_factory=writer_factory) as (writer, protected, current):
        writer._conn.execute('BEGIN')
        committed = False
        try:
            restored = _load_runtime_receipt(writer, execution_id=spec.execution_id, task_id=claim.task_id)
            if restored is not None:
                verify_financial_runtime_receipt(writer, restored.receipt_id, as_of=clock())
                writer._conn.execute('ROLLBACK')
                protected.succeed(duration_seconds=0, now=clock())
                return restored
            versions = tuple(derive_fundamental_daily(writer._conn, item.query, transaction_mode='existing', prepared=item) for item in prepared)
            receipt = _receipt(spec, claim, current, task_id=claim.task_id, kind='fundamental_group', control_sequence=control_sequence,
                clock=clock, original_versions=versions)
            _write_runtime_receipt(writer, receipt)
            protected.verify(now=clock())
            writer._conn.execute('COMMIT')
            committed = True
            protected.succeed(duration_seconds=0, now=clock())
            return receipt
        except BaseException:
            if not committed:
                writer._conn.execute('ROLLBACK')
            raise


def fundamental_group_queries(plan: FinancialCollectionPlan, *, start: int, end: int) -> tuple[FundamentalDailyQuery, ...]:
    days = plan.decision_dates
    if not 0 <= start < end <= len(plan.securities) * len(days) or end - start > 250:
        raise ValueError('fundamental daily task exceeds the original query positions')
    return tuple(FundamentalDailyQuery(ts_code=plan.securities[position // len(days)], trade_date=days[position % len(days)])
        for position in range(start, end))


def _original_query_dispatches(spec: FinancialExecutionSpec, queries: tuple[FinancialQuery, ...], policy: DataCenterExecutionPolicy,
        original: QuotaBoundTransportObserver) -> tuple[tuple[BackfillSourceRequestBinding, ...], tuple[SourceTransportCallReceipt, ...]]:
    from rquant.source_quota_store import SourceQuotaAttemptOutcome
    bindings = tuple(financial_request_binding(spec, query, policy) for query in queries)
    receipts: list[SourceTransportCallReceipt] = []
    for binding in bindings:
        attempts = original.request_attempts(binding.logical_request_id)
        if not attempts or len(attempts) > 6 or attempts[-1].outcome is not SourceQuotaAttemptOutcome.SUCCESS:
            raise ValueError('original financial dispatch result is uncertain or unproved')
        actual_ids: set[str] = set()
        for ordinal in range(1, len(attempts) + 1):
            attempt = original.get_call_attempt(logical_request_id=binding.logical_request_id, api_name=binding.api_name, call_ordinal=ordinal)
            expected_outcome = SourceQuotaAttemptOutcome.SUCCESS if ordinal == len(attempts) else SourceQuotaAttemptOutcome.FAILURE
            if (attempt is None or attempt.source != policy.quota_source or attempt.outcome is not expected_outcome
                    or attempt.dispatched_at is None or attempt.committed_at is None):
                raise ValueError('original financial dispatch receipt is incomplete')
            actual_ids.add(attempt.attempt_id)
            receipts.append(SourceTransportCallReceipt(source=attempt.source, logical_request_id=binding.logical_request_id,
                api_name=binding.api_name, call_ordinal=ordinal, attempt_id=attempt.attempt_id, outcome=attempt.outcome,
                dispatched_at=attempt.dispatched_at, committed_at=attempt.committed_at))
        if actual_ids != {attempt.attempt_id for attempt in attempts}:
            raise ValueError('original financial request/API dispatch continuity changed')
    return bindings, tuple(receipts)


def verify_financial_task_receipt(store: DuckDBStore, receipt: FinancialRuntimeReceipt, *,
        spec: FinancialExecutionSpec, task_id: str, policy: DataCenterExecutionPolicy,
        original: QuotaBoundTransportObserver, as_of: datetime) -> None:
    if (receipt.execution_id, receipt.owner, receipt.manifest_id, receipt.plan_sha256, receipt.task_id,
            receipt.primary_identity, receipt.archive_id) != (spec.execution_id, spec.owner, spec.manifest_id,
            spec.plan.content_sha256, task_id, spec.plan.primary_identity, spec.plan.archive_id):
        raise ValueError('original financial receipt owner/manifest/source changed')
    task = next((task for task in build_financial_manifest(spec.plan).tasks if task.task_id == task_id), None)
    if task is None:
        raise ValueError('financial receipt is outside the original finite manifest')
    if task_id.startswith('financial-raw-'):
        queries = spec.plan.queries[task.payload['query_start']:task.payload['query_end']]
        bindings, dispatches = _original_query_dispatches(spec, queries, policy, original)
        if (receipt.kind != 'query_group' or tuple(item.query for item in receipt.original_receipts) != queries
                or receipt.source_requests != bindings or receipt.dispatch_receipts != dispatches):
            raise ValueError('original financial committed query/dispatch evidence changed')
    elif task_id.startswith('financial-tail-'):
        queries = fundamental_group_queries(spec.plan, start=task.payload['query_start'], end=task.payload['query_end'])
        if receipt.kind != 'fundamental_group' or tuple((item.ts_code, item.trade_date) for item in receipt.original_versions) != tuple(
                (query.ts_code, query.trade_date) for query in queries):
            raise ValueError('original financial daily receipt differs from exact task positions')
    else:
        raise ValueError('financial domain receipt cannot complete an audit task')
    verify_financial_runtime_receipt(store, receipt.receipt_id, as_of=as_of)


def _commit_query_group(state: BackfillStateStore, claim: ClaimedBackfillTask, *, spec: FinancialExecutionSpec,
        queries: tuple[FinancialQuery, ...], archive: FinancialArchive, original: QuotaBoundTransportObserver,
        policy: Callable[[],DataCenterExecutionPolicy], control_sequence: int, clock: Callable[[],datetime],
        writer_factory: Callable[[PrimaryWriterLease],DuckDBStore] | None=None) -> FinancialRuntimeReceipt:
    from rquant.data_center_maintenance_runtime import protected_maintenance_writer
    current = policy()
    bindings, dispatches = _original_query_dispatches(spec, queries, current, original)
    receipts = tuple(archive.receipt(query.request_id) for query in queries)
    for query, receipt in zip(queries, receipts, strict=True):
        if receipt.query != query:
            raise ValueError('original archive receipt differs from the exact task query')
        archive.read_batch(query.request_id)
    with protected_maintenance_writer(state, claim, spec=spec, policy=policy, control_sequence=control_sequence, clock=clock, kind='financial',writer_factory=writer_factory) as (writer, protected, current):
        writer._conn.execute('BEGIN')
        committed = False
        try:
            restored = _load_runtime_receipt(writer, execution_id=spec.execution_id, task_id=claim.task_id)
            if restored is not None:
                verify_financial_task_receipt(writer, restored, spec=spec, task_id=claim.task_id,
                    policy=current, original=original, as_of=clock())
                writer._conn.execute('ROLLBACK')
                protected.succeed(duration_seconds=0, now=clock())
                return restored
            receipt = _receipt(spec, claim, current, task_id=claim.task_id, kind='query_group', control_sequence=control_sequence,
                clock=clock, original_receipts=receipts, source_requests=bindings, dispatch_receipts=dispatches,
                cursor_after=FinancialImportCursor.from_original(_cursor(writer._conn)), imported_rows_sha256=_imported_digest(writer, spec.plan.archive_id, receipts))
            _write_runtime_receipt(writer, receipt)
            protected.verify(now=clock())
            writer._conn.execute('COMMIT')
            committed = True
            protected.succeed(duration_seconds=0, now=clock())
            return receipt
        except BaseException:
            if not committed:
                writer._conn.execute('ROLLBACK')
            raise


class FinancialExecutionStep(RuntimeContractModel):
    outcome: Literal['idle', 'query_committed', 'fundamentals_committed', 'completion_waiting', 'completed', 'paused', 'partial']
    execution: MaintenanceExecutionStatus
    task_id: str | None = None


class FinancialRuntimeWorker(BackfillExecutionWorker):
    """Use the same original maintenance slot, finite claims and writer protection."""
    def __init__(self, state: BackfillStateStore, *, policy: Callable[[], DataCenterExecutionPolicy],
            adapter_factory: Callable[[ControlledTransportObserver], object], clock: Callable[[],datetime] | None = None,
            completion_step: Callable[[FinancialExecutionSpec, ClaimedBackfillTask, int],bool] | None = None,stopped: Event | None=None) -> None:
        super().__init__(state, policy=policy, adapter_factory=adapter_factory, calendar=lambda spec: spec.plan.calendar,
            clock=clock, completion_step=completion_step,stopped=stopped)
        self._kind = 'financial'

    def run_one(self, execution_id: str, *, owner: str) -> FinancialExecutionStep:
        import duckdb
        from rquant.backfill_state import StaleTaskClaimError
        from rquant.backfill_execute_contracts import maintenance_window
        from rquant.source_quota_store import SourceQuotaStore, SourceQuotaConflictError, SourceQuotaExhaustedError
        from rquant.source_quota_transport import QuotaBoundTransportObserver
        from rquant.storage.duckdb import DuckDBStore
        from rquant.storage.primary_writer_gate import PrimaryWriterBusy
        spec = self.state.get_financial_execution_spec(execution_id, owner=owner)
        if spec is None or self.state.load_manifest(spec.manifest_id) != build_financial_manifest(spec.plan):
            raise ValueError('owned immutable original financial manifest is unavailable')
        current = self.state.get_maintenance_status(execution_id, owner=owner)
        if current.status in {'completed','failed','paused','partial'}:
            return FinancialExecutionStep(outcome='idle', execution=current)
        if current.pause_requested or self.stopped.is_set() or not maintenance_window(self.clock()).may_start_day:
            result = self._pause(execution_id, owner, code='pause_requested' if current.pause_requested or self.stopped.is_set() else 'maintenance_window_closed')
            return FinancialExecutionStep.model_validate(result.model_dump())
        claim = None
        try:
            policy = self.policy()
            require_stable_execution_policy(policy, spec)
            require_financial_plan_policy(spec.plan, policy, now=self.clock())
            if current.policy_generation != policy.policy_generation or policy.original_state_path != self.state.path:
                raise ValueError('current original financial policy changed')
            quota = SourceQuotaStore(policy.quota_ledger_path)
            identifier, start, end = quota._quota_window(self.clock().astimezone(UTC), window_kind=policy.quota_window_kind)
            quota.declare_window(source=policy.quota_source, window_id=identifier, starts_at=start, resets_at=end, total_units=policy.quota_units_per_window)
            original = QuotaBoundTransportObserver(store=quota, source=policy.quota_source, quota_units_per_window=policy.quota_units_per_window,
                window_kind=policy.quota_window_kind, clock=self.clock)
            original.remaining(now=self.clock())
            if current.status == 'queued':
                current = self.state.transition_maintenance(execution_id, owner=owner, expected_sequence=current.control_sequence, status='running', now=self.clock())
            claim = self.state.claim_task(spec.manifest_id, worker_id='controlled-financial-maintenance', lease_seconds=120,
                now=self.clock(), maintenance_execution_id=execution_id)
            if claim is None:
                return FinancialExecutionStep(outcome='idle', execution=self.state.get_maintenance_status(execution_id, owner=owner))
            deadline = min(self.monotonic() + 1800, self.monotonic() + max(0,(maintenance_window(self.clock()).interrupt_at-self.clock()).total_seconds()))
            guard = lambda: self._guard(spec, claim, current.control_sequence, deadline)
            guard()
            if claim.task_id == 'verify-completion':
                if self.completion_step is None and self.completion_runtime is not None:
                    done=self.completion_runtime.run(spec,claim,current.control_sequence,
                        prepare_context=lambda:self._heartbeat(spec,claim,current.control_sequence,deadline),guard=guard,
                        stopped=self.stopped,writer_factory=self._writer,reader_factory=self._reader)
                elif self.completion_step is None:
                    raise ValueError('original financial replica/audit continuation is not installed')
                else:
                    done = self.completion_step(spec, claim, current.control_sequence)
                return FinancialExecutionStep(outcome='completed' if done else 'completion_waiting',
                    execution=self.state.get_maintenance_status(execution_id, owner=owner), task_id=claim.task_id)
            with self._reader(policy.primary_writer_gate.primary_path) as reader:
                restored = _load_runtime_receipt(reader, execution_id=execution_id, task_id=claim.task_id)
                if restored is not None:
                    verify_financial_task_receipt(reader, restored, spec=spec, task_id=claim.task_id,
                        policy=policy, original=original, as_of=self.clock())
            if restored is not None:
                from rquant.data_center_maintenance_runtime import protected_maintenance_writer
                with protected_maintenance_writer(self.state, claim, spec=spec, policy=self.policy, control_sequence=current.control_sequence,
                        clock=self.clock, kind='financial',writer_factory=self._writer) as (writer, protected, _):
                    verify_financial_task_receipt(writer, restored, spec=spec, task_id=claim.task_id,
                        policy=policy, original=original, as_of=self.clock())
                    protected.succeed(duration_seconds=0, now=self.clock())
            elif claim.task_id.startswith('financial-raw-'):
                task = self.state.get_task(claim.manifest_id, claim.task_id)
                queries = spec.plan.queries[task.payload['query_start']:task.payload['query_end']]
                observer = ControlledTransportObserver(original, policy=self.policy, kind='financial',
                    material_directory=policy.source_material_directory, clock=self.clock, claim_guard=guard)
                adapter = ControlledFinancialAdapter(self.adapter_factory(observer), observer, spec=spec, policy=policy, queries=queries)
                archive = FinancialArchive(spec.plan.archive_path)
                with self._heartbeat(spec, claim, current.control_sequence, deadline) as stop_heartbeat:
                    for query in queries:
                        guard()
                        receipt = acquire_financial_batches(adapter, archive, (query,), run_day=self.clock().astimezone(_SHANGHAI).date(), clock=self.clock)[0]
                        if receipt.query != query:
                            raise ValueError('original financial acquisition returned another query')
                        archive.read_batch(query.request_id)
                        binding = financial_request_binding(spec, query, policy)
                        _original_query_dispatches(spec, (query,), policy, original)
                        cleanup_verified_source_material(binding, directory=policy.source_material_directory, observer=original)
                    stop_heartbeat()
                for _ in range(4096):
                    guard()
                    with self._reader(policy.primary_writer_gate.primary_path) as reader:
                        cursor = _cursor(reader._conn)
                    limit = 32
                    while True:
                        page = archive.committed_page(after=cursor[1] if cursor is not None else None, limit=limit,
                            accepted_anchor_generation=cursor[2] if cursor is not None else None,
                            accepted_anchor_sha256=cursor[3] if cursor is not None else None)
                        if sum(entry.receipt.byte_count for entry in page.entries) <= 32*1024*1024:
                            break
                        if limit == 1:
                            raise ValueError('original financial page exceeds material capacity')
                        limit //= 2
                    if page.archive_id != spec.plan.archive_id:
                        raise ValueError('original immutable financial archive identity changed')
                    if not page.entries:
                        break
                    commit_financial_import_page(self.state, claim, spec=spec, page=page, expected_cursor=cursor,
                        policy=self.policy, control_sequence=current.control_sequence, clock=self.clock,writer_factory=self._writer)
                    if not page.has_more:
                        break
                else:
                    raise ValueError('original financial import exceeds finite recovery pages')
                _commit_query_group(self.state, claim, spec=spec, queries=queries, archive=archive, original=original,
                    policy=self.policy, control_sequence=current.control_sequence, clock=self.clock,writer_factory=self._writer)
            elif claim.task_id.startswith('financial-tail-'):
                task = self.state.get_task(claim.manifest_id, claim.task_id)
                queries = fundamental_group_queries(spec.plan, start=task.payload['query_start'], end=task.payload['query_end'])
                with self._heartbeat(spec, claim, current.control_sequence, deadline) as stop_heartbeat:
                    with self._reader(policy.primary_writer_gate.primary_path) as reader:
                        prepared = tuple(prepare_fundamental_daily(reader._conn, query) for query in queries)
                    stop_heartbeat()
                guard()
                self.state.renew_task_claim(claim, lease_seconds=120, now=self.clock(), guard=lambda connection, observed:
                    self.state.verify_maintenance_claim(connection, claim, execution_id=execution_id, owner=owner,
                        expected_sequence=current.control_sequence, now=observed))
                commit_fundamental_group(self.state, claim, spec=spec, prepared=prepared, policy=self.policy,
                    control_sequence=current.control_sequence, clock=self.clock,writer_factory=self._writer)
            else:
                raise ValueError('unknown original financial manifest task')
            return FinancialExecutionStep(outcome='query_committed' if claim.task_id.startswith('financial-raw-') else 'fundamentals_committed',
                execution=self.state.get_maintenance_status(execution_id, owner=owner), task_id=claim.task_id)
        except PrimaryWriterBusy:
            if claim is not None:
                self.state.release_task_claim(claim, now=self.clock())
            return FinancialExecutionStep(outcome='idle', execution=self.state.get_maintenance_status(execution_id, owner=owner))
        except StaleTaskClaimError:
            if claim is not None and self.state.get_task(claim.manifest_id, claim.task_id).claim_token != claim.claim_token:
                return FinancialExecutionStep(outcome='idle', execution=self.state.get_maintenance_status(execution_id, owner=owner))
            result = self._pause(execution_id, owner, code='execution_control_changed', claim=claim, task_id=claim.task_id if claim is not None else None)
            return FinancialExecutionStep.model_validate(result.model_dump())
        except (OSError, ValueError, RuntimeError, duckdb.Error, ExceptionGroup, SourceQuotaConflictError, SourceQuotaExhaustedError) as error:
            code = 'source_quota_exhausted' if isinstance(error, SourceQuotaExhaustedError) else 'financial_source_or_result_unconfirmed'
            result = self._pause(execution_id, owner, code=code, claim=claim, task_id=claim.task_id if claim is not None else None)
            return FinancialExecutionStep.model_validate(result.model_dump())
