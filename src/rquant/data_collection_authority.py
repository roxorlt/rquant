"""Bind actual collector observations to original database transactions."""
from __future__ import annotations

import json
import hashlib
import os
import shutil
import stat
import sqlite3
import tempfile
from contextlib import closing
from collections.abc import Callable
from datetime import UTC, date, datetime
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from rquant.data_collection_contracts import (
    CollectionRecorderConfig, DatasetCollectionClaim, IngestionCommitReceipt, SourceObservation,
    AuditCollectionReference, DataCollectionProofV2, DatasetCollectionEvidence,
    CollectionReceiptReference, MAX_COLLECTION_PROOF_BYTES, DataCollectionProofV3, CollectionReceiptManifest,
    CollectionManifestPage, parse_collection_proof,
)
from rquant.daily_canonical_publisher import DailyCanonicalPublisher, DailyCanonicalPublishReceipt, CanonicalTableWatermark,CanonicalDatabaseIdentity
from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.storage.primary_writer_gate import PrimaryWriterLease,PrimaryWriterGate,PrimaryWriterGateConfig
from rquant.data_audit_report import AuditReplicaFileIdentity
from rquant.runtime_contracts import RuntimeContractModel
from pydantic import Field
from rquant.daily_valuation_pit import DailyValuationBatch,DailyValuationRow,_record_daily_valuation_batch_in_transaction

if TYPE_CHECKING:
    from rquant.storage.duckdb import DuckDBStore
    from rquant.data_audit_report_jobs import DataAuditReportJobStore

_DAILY_API_TABLE = {
    'daily':'daily_bar','daily_basic':'daily_basic','namechange':'stock_status_daily','stock_st':'stock_status_daily',
    'suspend_d':'stock_suspend_coverage','adj_factor':'adj_factor',
}


class CollectionObservationBatch:
    """Capture original responses before their existing normalizers run."""

    def __init__(self, observed_at: datetime,*,clock: Callable[[],datetime] | None=None,
                 source_normalization_version: str | None=None) -> None:
        self.observed_at = observed_at
        self.clock=clock or (lambda:observed_at)
        self.source_normalization_version=source_normalization_version
        self.observations: list[SourceObservation] = []
        self.daily_basic_response: pd.DataFrame | None=None

    def call(self, api_name: str, parameters: object,
             operation: Callable[[], pd.DataFrame]) -> pd.DataFrame:
        if len(self.observations) >= 32:
            raise ValueError('collection response count exceeds capacity')
        frame = operation()
        if frame is None:
            return frame
        if self.source_normalization_version is not None:
            if self.source_normalization_version!='tushare-nullable-v1':
                raise ValueError('unknown original SDK normalization version')
            from rquant.adapter.tushare import normalize_sdk_nullable_response
            frame=normalize_sdk_nullable_response(api_name,frame)
        self.observations.append(SourceObservation.from_frame(api_name, parameters, frame,
            observed_at=self.clock(),source_normalization_version=self.source_normalization_version))
        if api_name=='daily_basic':
            self.daily_basic_response=frame.copy(deep=True)
        return frame


class ObservedDailyClient:
    def __init__(self, client: Any, batch: CollectionObservationBatch) -> None:
        self.client = client
        self.batch = batch

    def stock_basic(self, **kwargs: object) -> pd.DataFrame:
        return self.batch.call('stock_basic', kwargs, lambda:self.client.stock_basic(**kwargs))

    def daily(self, **kwargs: object) -> pd.DataFrame:
        return self.batch.call('daily', kwargs, lambda:self.client.daily(**kwargs))

    def index_daily(self, **kwargs: object) -> pd.DataFrame:
        return self.batch.call('index_daily', kwargs, lambda:self.client.index_daily(**kwargs))

    def adj_factor(self, **kwargs: object) -> pd.DataFrame:
        return self.batch.call('adj_factor', kwargs, lambda:self.client.adj_factor(**kwargs))

    def daily_basic(self, **kwargs: object) -> pd.DataFrame:
        return self.batch.call('daily_basic', kwargs, lambda:self.client.daily_basic(**kwargs))


class ObservedSecurityClient:
    def __init__(self, client: Any, batch: CollectionObservationBatch) -> None:
        self.client = client
        self.batch = batch

    def namechange_raw(self, start_date: date, end_date: date,
                       ts_code: str | None = None) -> pd.DataFrame:
        return self.batch.call('namechange', dict(start_date=start_date,end_date=end_date,
            ts_code=ts_code),lambda:self.client.namechange_raw(start_date,end_date,ts_code=ts_code))

    def stock_st_raw(self, trade_date: date) -> pd.DataFrame:
        return self.batch.call('stock_st',dict(trade_date=trade_date),
            lambda:self.client.stock_st_raw(trade_date))

    def suspend_d_raw(self, trade_date: date) -> pd.DataFrame:
        return self.batch.call('suspend_d',dict(trade_date=trade_date),
            lambda:self.client.suspend_d_raw(trade_date))


class CollectionCommitRecorder:
    def __init__(self, config: CollectionRecorderConfig, *,
                 clock: Callable[[],datetime] | None = None) -> None:
        self.config=CollectionRecorderConfig.model_validate(config)
        self.clock=clock or (lambda:datetime.now(UTC))

    def record_daily(self, store: DuckDBStore, trade_date: date, *,
                     observations: tuple[SourceObservation,...],daily_basic_response: pd.DataFrame | None=None) -> IngestionCommitReceipt:
        transaction_id=store._conn.execute('SELECT txid_current()').fetchone()[0]
        if transaction_id!=store._conn.execute('SELECT txid_current()').fetchone()[0]:
            raise RuntimeError('collection receipt requires the original open transaction')
        config=self.config
        calendar_row=store._conn.execute("SELECT is_open,source FROM trade_calendar WHERE exchange='SSE' AND cal_date=?",[trade_date]).fetchone()
        if calendar_row is None or not calendar_row[0] or calendar_row[1]!='tushare':
            raise ValueError('collection calendar source is unknown')
        marks=DailyCanonicalPublisher.collect_table_watermarks(store,trade_date)
        mark_map={item.table_name:item for item in marks}
        claims: dict[str,DatasetCollectionClaim]={}
        for observation in observations:
            dataset=_DAILY_API_TABLE.get(observation.api_name)
            if dataset is not None:
                mark=mark_map[dataset]
                claims[dataset]=DatasetCollectionClaim(dataset_id=dataset,trade_date=trade_date,
                    scope='actual_date_rows',row_count=mark.row_count,source_api=observation.api_name,
                    content_sha256=mark.content_sha256)
        valuation=_collected_valuation_material(trade_date,observations,daily_basic_response) if daily_basic_response is not None else None
        return self._record(store,trade_date,observations=observations,marks=marks,
            claims=tuple(claims[key] for key in sorted(claims)),scope_id='daily_core',valuation=valuation)

    def record_dataset(self,store: DuckDBStore,trade_date: date,*,dataset_id: str,
                       source_api: str,observation: SourceObservation,snapshot: bool) -> IngestionCommitReceipt:
        mark=_dataset_watermark(store,dataset_id,trade_date,snapshot=snapshot)
        claim=DatasetCollectionClaim(dataset_id=dataset_id,trade_date=trade_date,
            scope='actual_snapshot_partition' if snapshot else 'actual_date_rows',row_count=mark.row_count,
            source_api=source_api,content_sha256=mark.content_sha256)
        return self._record(store,trade_date,observations=(observation,),marks=(mark,),claims=(claim,),scope_id=dataset_id)

    def _record(self,store: DuckDBStore,trade_date: date,*,observations: tuple[SourceObservation,...],
                marks: tuple[CanonicalTableWatermark,...],claims: tuple[DatasetCollectionClaim,...],
                scope_id: str,valuation: CollectedDailyValuationMaterial | None=None) -> IngestionCommitReceipt:
        transaction_id=store._conn.execute('SELECT txid_current()').fetchone()[0]
        if transaction_id!=store._conn.execute('SELECT txid_current()').fetchone()[0]:
            raise RuntimeError('collection receipt requires the original open transaction')
        config=self.config
        _calendar_facts(store,config.calendar,trade_date,trade_date)
        event=canonical_sha256({'collector':config.collector_id,'run':config.run_id,
            'source':config.source_generation_id,'date':trade_date,'scope':scope_id})
        existing=store._conn.execute('SELECT payload_json FROM ingestion_commit_receipt WHERE event_id=?',[event]).fetchone()
        old=IngestionCommitReceipt.model_validate_json(existing[0]) if existing is not None else None
        revision=None
        if valuation is not None:
            revision=old.valuation_revision if old is not None else store._conn.execute(
                'SELECT coalesce(max(revision),0)+1 FROM daily_basic_valuation_batch WHERE trade_date=?',[trade_date]).fetchone()[0]
            if revision is None:
                raise ValueError('old collection identity cannot acquire new valuation evidence')
        sequence=(store._conn.execute('SELECT coalesce(max(sequence),0)+1 FROM ingestion_commit_receipt').fetchone()[0]
                  if existing is None else IngestionCommitReceipt.model_validate_json(existing[0]).sequence)
        receipt=IngestionCommitReceipt(event_id=event,sequence=sequence,collector_id=config.collector_id,
            run_id=config.run_id,owner=config.owner,code_commit=config.code_commit,
            source_generation_id=config.source_generation_id,database_identity=DailyCanonicalPublisher.database_identity(store),
            calendar=config.calendar,trade_date=trade_date,observations=observations,watermarks=marks,
            datasets=claims,transaction_id=transaction_id,committed_at=self.clock(),valuation_revision=revision,
            valuation_content_sha256=_valuation_content_sha256(valuation,revision) if valuation is not None else None)
        if existing is not None:
            old=IngestionCommitReceipt.model_validate_json(existing[0])
            # Reusing a run means reusing its original facts, responses and receipt.
            comparable={'receipt_id','transaction_id','committed_at'}
            if old.model_dump(mode='python',exclude=comparable)!=receipt.model_dump(mode='python',exclude=comparable):
                raise ValueError('collection event content conflict')
            if valuation is not None:
                _record_collected_daily_valuation(store,old,valuation)
            return old
        payload=receipt.model_dump_json()
        if len(payload.encode())>256*1024:
            raise ValueError('collection receipt exceeds capacity')
        store._conn.execute('''INSERT INTO ingestion_commit_receipt VALUES (?,?,?,?,?,?,?,?)''',
            [event,receipt.receipt_id,sequence,trade_date,config.collector_id,config.run_id,payload,receipt.committed_at])
        if valuation is not None:
            _record_collected_daily_valuation(store,receipt,valuation)
        return receipt

    @staticmethod
    def verify_receipt(store: DuckDBStore, receipt: IngestionCommitReceipt) -> IngestionCommitReceipt:
        receipt=IngestionCommitReceipt.model_validate_json(receipt.model_dump_json())
        stored=store._conn.execute('SELECT payload_json FROM ingestion_commit_receipt WHERE receipt_id=?',[receipt.receipt_id]).fetchone()
        if stored is None or IngestionCommitReceipt.model_validate_json(stored[0])!=receipt:
            raise ValueError('original collection receipt is missing or changed')
        if receipt.collector_id=='dataset_backfill':
            current={item.table_name:item for claim in receipt.datasets for item in (
                _dataset_watermark(store,claim.dataset_id,receipt.trade_date,
                    snapshot=claim.scope=='actual_snapshot_partition'),)}
            expected=receipt.watermarks
        else:
            current={item.table_name:item for item in DailyCanonicalPublisher.collect_table_watermarks(store,receipt.trade_date)}
            declared={claim.dataset_id for claim in receipt.datasets}
            expected=tuple(item for item in receipt.watermarks if item.table_name in declared)
        if any(current.get(item.table_name)!=item for item in expected):
            raise ValueError('collection table content changed after original receipt')
        verify_collected_daily_valuation(store,receipt)
        return receipt


class CollectedDailyValuationMaterial(RuntimeContractModel):
    observation: SourceObservation
    valuation_observed: bool
    rows: tuple[DailyValuationRow,...] = Field(max_length=8000)


def _collected_valuation_material(day: date,observations: tuple[SourceObservation,...],raw: pd.DataFrame) -> CollectedDailyValuationMaterial:
    sources=tuple(item for item in observations if item.api_name=='daily_basic')
    if len(sources)!=1:
        raise ValueError('collected valuation requires one actual daily_basic response')
    source=sources[0]
    parameters={'trade_date':day.strftime('%Y%m%d'),'fields':'ts_code,trade_date,turnover_rate,volume_ratio,total_mv,circ_mv,pe_ttm,pb,dv_ttm'}
    if SourceObservation.from_frame('daily_basic',parameters,raw,observed_at=source.observed_at,
            possibly_truncated=source.possibly_truncated,source_normalization_version=source.source_normalization_version)!=source:
        raise ValueError('collected valuation content differs from the actual SDK response')
    present={'pe_ttm','pb','dv_ttm'}&set(raw.columns)
    if present and len(present)!=3:
        raise ValueError('collected valuation response is missing requested columns')
    rows=tuple(DailyValuationRow(ts_code=row['ts_code'],trade_date=datetime.strptime(str(row['trade_date']),'%Y%m%d').date(),
        pe_ttm=row['pe_ttm'],pb=row['pb'],dv_ttm=row['dv_ttm']) for row in raw.to_dict('records')) if present else ()
    if any(row.trade_date!=day for row in rows) or len({row.ts_code for row in rows})!=len(rows):
        raise ValueError('collected valuation contains another date or duplicate security')
    return CollectedDailyValuationMaterial(observation=source,valuation_observed=bool(present),rows=tuple(sorted(rows,key=lambda row:row.ts_code)))


def _valuation_content_sha256(material: CollectedDailyValuationMaterial,revision: int) -> str:
    return canonical_sha256({'contract':'collection-valuation-content/v1','revision':revision,'material':material.model_dump(mode='python')})


def _collection_valuation_generation(receipt: IngestionCommitReceipt) -> str:
    return canonical_sha256({'contract':'collection-valuation-batch/v1','ingestion_receipt_id':receipt.receipt_id})


def _record_collected_daily_valuation(store: DuckDBStore,receipt: IngestionCommitReceipt,material: CollectedDailyValuationMaterial) -> None:
    if material.observation.observed_at>receipt.committed_at or material.observation not in receipt.observations:
        raise ValueError('collected valuation observation is future or has another source')
    if receipt.valuation_revision is None or _valuation_content_sha256(material,receipt.valuation_revision)!=receipt.valuation_content_sha256:
        raise ValueError('collected valuation differs from the transaction receipt')
    # This identifier names a collection receipt. It does not claim a signed daily-close candidate.
    batch=DailyValuationBatch(candidate_generation_id=_collection_valuation_generation(receipt),
        source_generation_id=receipt.source_generation_id,source_sequence=receipt.sequence,
        source_batch_id=canonical_sha256(material.observation.model_dump(mode='python')),revision=receipt.valuation_revision,
        trade_date=receipt.trade_date,observed_at=material.observation.observed_at,
        valuation_observed=material.valuation_observed,rows=material.rows)
    _record_daily_valuation_batch_in_transaction(store._conn,batch)


def verify_collected_daily_valuation(store: DuckDBStore,receipt: IngestionCommitReceipt) -> bool:
    if receipt.valuation_content_sha256 is None:
        return False
    identity=_collection_valuation_generation(receipt)
    stored=store._conn.execute('SELECT source_generation_id,source_sequence,source_batch_id,revision,trade_date,observed_at,valuation_observed '
        'FROM daily_basic_valuation_batch WHERE candidate_generation_id=?',[identity]).fetchone()
    if stored is None:
        raise ValueError('original collected valuation batch is missing')
    sources=tuple(item for item in receipt.observations if item.api_name=='daily_basic')
    if len(sources)!=1 or stored[:6]!=(receipt.source_generation_id,receipt.sequence,canonical_sha256(sources[0].model_dump(mode='python')),
            receipt.valuation_revision,receipt.trade_date,sources[0].observed_at):
        raise ValueError('original collected valuation source or version changed')
    values=store._conn.execute('SELECT ts_code,trade_date,pe_ttm,pb,dv_ttm FROM daily_basic_valuation_observation '
        'WHERE candidate_generation_id=? ORDER BY ts_code LIMIT 8001',[identity]).fetchall()
    material=CollectedDailyValuationMaterial(observation=sources[0],valuation_observed=stored[6],rows=tuple(
        DailyValuationRow(ts_code=row[0],trade_date=row[1],pe_ttm=row[2],pb=row[3],dv_ttm=row[4]) for row in values))
    _record_collected_daily_valuation(store,receipt,material)
    return True


def _dataset_watermark(store: DuckDBStore,dataset_id: str,day: date,*,snapshot: bool) -> CanonicalTableWatermark:
    from rquant.data_catalog.build import CATALOG_CONTRACTS
    contract=next((item for item in CATALOG_CONTRACTS if item.dataset_id==dataset_id),None)
    if contract is None:
        raise ValueError('collection dataset is not declared in original catalog')
    order=','.join(f'"{name}"' for name in contract.physical_primary_key)
    where='' if snapshot else f'WHERE "{contract.event_date_column}"=?'
    cursor=store._conn.execute(f'SELECT * FROM "{contract.table_name}" {where} ORDER BY {order} LIMIT 8001',
        [] if snapshot else [day])
    columns=tuple(item[0] for item in cursor.description)
    rows=cursor.fetchall()
    if len(rows)>8000:
        raise ValueError('collection dataset exceeds receipt row capacity')
    # A physical TIMESTAMP has no timezone. Hash its stored spelling without inventing a UTC instant.
    normalized=tuple(tuple(value.isoformat() if isinstance(value,datetime) and value.tzinfo is None else value
        for value in row) for row in rows)
    return CanonicalTableWatermark(table_name=contract.table_name,trade_date=day,row_count=len(rows),
        content_sha256=canonical_sha256(dict(columns=columns,rows=normalized)))


def _json_bytes(value: object) -> bytes:
    return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).encode()


def _file_sha(path: Path, *, max_bytes: int, deadline: datetime | None = None,
              stop_requested: Callable[[], bool] | None = None) -> str:
    descriptor=os.open(path,os.O_RDONLY|os.O_NONBLOCK|getattr(os,'O_NOFOLLOW',0))
    with os.fdopen(descriptor,'rb') as handle:
        before=os.fstat(handle.fileno())
        if not stat.S_ISREG(before.st_mode) or not 0<before.st_size<=max_bytes:
            raise ValueError('collection file exceeds capacity or is not regular')
        digest=hashlib.sha256()
        remaining=before.st_size
        while remaining:
            if (deadline is not None and datetime.now(UTC)>=deadline) or (stop_requested is not None and stop_requested()):
                raise InterruptedError('collection source scan was stopped')
            chunk=handle.read(min(1024*1024,remaining))
            if not chunk:
                raise ValueError('collection file was truncated during read')
            digest.update(chunk)
            remaining-=len(chunk)
        if handle.read(1):
            raise ValueError('collection file grew during read')
        after=os.fstat(handle.fileno())
        if (before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns,before.st_ctime_ns)!=(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns):
            raise ValueError('collection file changed during read')
        current=path.stat(follow_symlinks=False)
        if (after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns)!=(current.st_dev,current.st_ino,current.st_size,current.st_mtime_ns,current.st_ctime_ns):
            raise ValueError('collection file path changed during read')
    return digest.hexdigest()


def _calendar_facts(store: DuckDBStore, calendar: MarketCalendarAuthority,
                    start: date, end: date) -> str:
    if not calendar.coverage_start<=start<=end<=calendar.coverage_end:
        raise ValueError('collection range is outside original SSE calendar')
    rows=store._conn.execute("SELECT exchange,cal_date,is_open,pretrade_date,source FROM trade_calendar "
        "WHERE exchange='SSE' AND cal_date BETWEEN ? AND ? ORDER BY cal_date",[start,end]).fetchall()
    if len(rows)!=(end-start).days+1 or any(row[4]!='tushare' for row in rows):
        raise ValueError('collection calendar is incomplete or source is unknown')
    expected=tuple(day for day in calendar.open_dates if start<=day<=end)
    if tuple(row[1] for row in rows if row[2])!=expected:
        raise ValueError('original SSE calendar differs from source calendar authority')
    return canonical_sha256(rows)


def _original_claims(store: DuckDBStore, reference: CollectionReceiptReference, *,
                     primary_identity: object, snapshot_root: Path | None,
                     as_of: datetime,calendar: MarketCalendarAuthority | None=None) -> tuple[DatasetCollectionClaim,...]:
    if reference.kind=='ingestion':
        row=store._conn.execute('SELECT payload_json FROM ingestion_commit_receipt WHERE receipt_id=?',
            [reference.receipt_id]).fetchone()
        if row is None:
            raise ValueError('original ingestion receipt is missing')
        receipt=CollectionCommitRecorder.verify_receipt(store,IngestionCommitReceipt.model_validate_json(row[0]))
        if receipt.database_identity!=primary_identity or receipt.committed_at>as_of:
            raise ValueError('ingestion receipt names another primary or future commit')
        claims=receipt.datasets
    elif reference.kind=='canonical':
        row=store._conn.execute('SELECT payload_json,payload_sha256 FROM daily_canonical_publish_receipt WHERE receipt_id=?',
            [reference.receipt_id]).fetchone()
        if row is None:
            raise ValueError('original canonical receipt is missing')
        receipt=DailyCanonicalPublishReceipt.model_validate_json(row[0])
        if (receipt.receipt_id!=reference.receipt_id or receipt.database_identity!=primary_identity
                or receipt.committed_at>as_of or hashlib.sha256(str(row[0]).encode()).hexdigest()!=row[1]):
            raise ValueError('canonical receipt content or physical source changed')
        if (calendar is None or (receipt.calendar_generation_id,receipt.calendar_producer_commit,
                receipt.calendar_content_sha256,receipt.calendar_as_of)!=(calendar.content_sha256,
                calendar.producer_commit,calendar.content_sha256,calendar.generated_at)):
            raise ValueError('canonical receipt calendar differs from sealed source')
        marks=DailyCanonicalPublisher.collect_table_watermarks(store,receipt.trade_date)
        if marks!=receipt.watermarks:
            raise ValueError('canonical tables changed after original commit')
        from rquant.data_catalog.build import CATALOG_CONTRACTS
        ids={item.table_name:item.dataset_id for item in CATALOG_CONTRACTS}
        claims=tuple(DatasetCollectionClaim(dataset_id=ids[item.table_name],trade_date=item.trade_date,
            scope='actual_date_rows',row_count=item.row_count,source_api='daily_close',
            content_sha256=item.content_sha256) for item in marks if item.table_name in ids)
    elif reference.kind=='dataset_snapshot':
        if snapshot_root is None:
            raise ValueError('original named lake source is not configured')
        binding=store.get_dataset_snapshot_binding(reference.receipt_id)
        if binding is None or binding.status!='ready' or binding.completed_at>as_of:
            raise ValueError('original dataset binding is not ready')
        root=Path(binding.artifact_root)
        if root!=snapshot_root or root.resolve(strict=True)!=root:
            raise ValueError('dataset binding names another trusted lake root')
        if len(binding.manifest.artifacts)>384:
            raise ValueError('dataset binding exceeds artifact capacity')
        from rquant.research_snapshot import verify_snapshot_artifact
        claims=[]
        for artifact in binding.manifest.artifacts:
            if artifact.dataset_id not in reference.dataset_ids:
                continue
            if artifact.artifact_type!='lake_partition':
                raise ValueError('only original immutable lake partitions establish named scope')
            verify_snapshot_artifact(artifact,lake_root=root,as_of_time=binding.manifest.as_of_time)
            claims.append(DatasetCollectionClaim(dataset_id=artifact.dataset_id,
                trade_date=binding.manifest.end_date,scope='actual_snapshot_partition',
                row_count=artifact.row_count,source_api=artifact.source or 'unknown',content_sha256=artifact.content_hash))
        claims=tuple(claims)
    else:
        from rquant.financial_runtime import verify_financial_runtime_receipt
        claims=verify_financial_runtime_receipt(store,reference.receipt_id,as_of=as_of,primary_identity=primary_identity)
    declared={item.dataset_id for item in claims}
    if not set(reference.dataset_ids)<=declared:
        raise ValueError('collection reference declares unproved datasets')
    return tuple(item for item in claims if item.dataset_id in reference.dataset_ids)


def _available_security_inventory(connection: object) -> tuple[str,...]:
    rows=connection.execute('SELECT ts_code FROM stock_basic ORDER BY ts_code LIMIT 8001').fetchall()
    if len(rows)>8000:
        raise ValueError('collection security inventory exceeds original capacity')
    return tuple(row[0] for row in rows)


def verify_collection_source(connection: object, proof: DataCollectionProofV2, *,
                             snapshot_root: Path | None = None) -> tuple[DatasetCollectionEvidence,...]:
    from rquant.storage.duckdb import DuckDBStore
    from rquant.data_catalog.build import CATALOG_CONTRACTS
    from rquant.data_audit_datasets import catalog_contract_sha256
    proof=parse_collection_proof(proof.model_dump_json())
    # Reuse the existing Store readers on the exact audit connection, without opening a writer.
    store=object.__new__(DuckDBStore)
    store._conn=connection
    store.path=proof.fixed_replica_path
    if proof.catalog_contract_sha256!=catalog_contract_sha256():
        raise ValueError('collection catalog contract changed')
    if _calendar_facts(store,proof.calendar,proof.audit_start,proof.observed_through)!=proof.calendar_facts_sha256:
        raise ValueError('collection calendar content changed')
    if proof.available_securities is not None and _available_security_inventory(connection)!=proof.available_securities:
        raise ValueError('collection security inventory differs from the original fixed source')
    if isinstance(proof,DataCollectionProofV3):
        from rquant.data_collection_manifest import ReceiptSetAccumulator,iter_receipt_manifest,verify_original_entry
        accumulator=ReceiptSetAccumulator()
        root=proof.receipt_manifest
        for entry in iter_receipt_manifest(proof.fixed_replica_path.parent,root):
            original=verify_original_entry(store,root,entry)
            if original.committed_at>proof.sealed_at:
                raise ValueError('complete manifest contains a future original commit')
            if entry.reference is None:
                continue
            claims=_original_claims(store,entry.reference,primary_identity=proof.primary_identity,
                snapshot_root=snapshot_root,as_of=proof.sealed_at,calendar=proof.calendar)
            if entry.kind=='day':
                row=store._conn.execute('SELECT payload_json FROM ingestion_commit_receipt WHERE receipt_id=?',[entry.reference.receipt_id]).fetchone()
                collected=IngestionCommitReceipt.model_validate_json(row[0])
                if (collected.owner,collected.run_id,collected.trade_date)!=(root.owner,root.execution_id+':'+original.trade_date.isoformat(),original.trade_date):
                    raise ValueError('complete day collection belongs to another original task')
            for claim in claims:
                if claim.dataset_id=='financial_observation':
                    if root.observed_start is None or not root.observed_start.date()<=claim.trade_date<=root.observed_end.date():
                        raise ValueError('financial observation exceeds original observed bounds')
                elif not proof.audit_start<=claim.trade_date<=proof.observed_through:
                    raise ValueError('collection claim exceeds sealed original query/derive range')
            accumulator.add(entry.reference,claims)
        summaries=accumulator.summaries()
        if summaries!=root.datasets:
            raise ValueError('complete original dataset receipt/scope set changed')
        by_dataset={item.dataset_id:item for item in summaries}
        return tuple(DatasetCollectionEvidence(dataset_id=key,status='partial' if key in by_dataset else 'unconfirmed',
            receipt_ids=(),scopes=(by_dataset[key].aggregate_scope(),) if key in by_dataset else (),
            receipt_set=by_dataset.get(key)) for key in sorted(item.dataset_id for item in CATALOG_CONTRACTS))
    receipts: dict[str,list[str]]={item.dataset_id:[] for item in CATALOG_CONTRACTS}
    scopes: dict[str,list[DatasetCollectionClaim]]={key:[] for key in receipts}
    for reference in proof.references:
        claims=_original_claims(store,reference,primary_identity=proof.primary_identity,
            snapshot_root=snapshot_root,as_of=proof.sealed_at,calendar=proof.calendar)
        for claim in claims:
            if claim.dataset_id not in receipts:
                continue
            if not proof.audit_start<=claim.trade_date<=proof.observed_through:
                raise ValueError('collection claim exceeds sealed audit range')
            if reference.receipt_id not in receipts[claim.dataset_id]:
                receipts[claim.dataset_id].append(reference.receipt_id)
            scopes[claim.dataset_id].append(claim)
    return tuple(DatasetCollectionEvidence(dataset_id=key,status='partial' if receipts[key] else 'unconfirmed',
        receipt_ids=tuple(receipts[key]),scopes=tuple(scopes[key])) for key in sorted(receipts))


def load_collection_proof(directory: Path, reference: AuditCollectionReference) -> DataCollectionProofV2:
    reference=AuditCollectionReference.model_validate(reference)
    if directory.is_symlink() or directory.resolve(strict=True)!=directory:
        raise ValueError('collection directory must be canonical')
    path=directory/reference.relative_proof_name
    descriptor=os.open(path,os.O_RDONLY|os.O_NONBLOCK|getattr(os,'O_NOFOLLOW',0))
    with os.fdopen(descriptor,'rb') as handle:
        observed=os.fstat(handle.fileno())
        if not stat.S_ISREG(observed.st_mode) or observed.st_size!=reference.byte_count:
            raise ValueError('collection proof file identity changed')
        payload=handle.read(MAX_COLLECTION_PROOF_BYTES+1)
    if len(payload)!=reference.byte_count or hashlib.sha256(payload).hexdigest()!=reference.proof_sha256:
        raise ValueError('collection proof bytes changed')
    proof=parse_collection_proof(payload)
    if _json_bytes(proof.model_dump(mode='json'))!=payload or path.name!=f'collection-{proof.binding_sha256}.json':
        raise ValueError('collection proof is not canonically sealed')
    if (proof.event_id,proof.binding_sha256,proof.sequence)!=(reference.event_id,reference.binding_sha256,reference.sequence):
        raise ValueError('collection reference differs from original proof')
    expected=directory/f'pin-{proof.event_id}.duckdb'
    if proof.fixed_replica_path!=expected or expected.resolve(strict=False)!=expected or expected.is_symlink():
        raise ValueError('collection proof names another fixed source')
    return proof


def verify_fixed_collection_file(proof: DataCollectionProofV2, *, deadline: datetime | None = None,
        stop_requested: Callable[[],bool] | None = None) -> None:
    from rquant.replica_generation import ReplicaGenerationMetadata
    observed=proof.fixed_replica_path.stat(follow_symlinks=False)
    if not stat.S_ISREG(observed.st_mode) or (observed.st_dev,observed.st_ino,observed.st_size,observed.st_mtime_ns)!=(
        proof.replica_device,proof.replica_inode,proof.replica_size,proof.replica_mtime_ns):
        raise ValueError('fixed collection replica identity changed')
    if _file_sha(proof.fixed_replica_path,max_bytes=64*1024**3,deadline=deadline,stop_requested=stop_requested)!=proof.replica_sha256:
        raise ValueError('fixed collection replica content changed')
    metadata=ReplicaGenerationMetadata.model_validate_json(proof.original_sidecar_bytes)
    if (str(metadata.source_database)!=proof.primary_identity.canonical_path or metadata.source_before!=metadata.source_after
            or (metadata.source_after.main.device,metadata.source_after.main.inode)!=(
                proof.primary_identity.device,proof.primary_identity.inode)
            or (metadata.replica.device,metadata.replica.inode,metadata.replica.size,metadata.replica.mtime_ns)!=(
                proof.replica_device,proof.replica_inode,proof.replica_size,proof.replica_mtime_ns)):
        raise ValueError('collection proof differs from original replica generation')


class VerifiedCollectionFile(RuntimeContractModel):
    identity: AuditReplicaFileIdentity
    replica_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    collection_reference: AuditCollectionReference


def capture_verified_collection_identity(proof: DataCollectionProofV2,reference: AuditCollectionReference,*,
    deadline: datetime,stop_requested: Callable[[],bool] | None = None) -> VerifiedCollectionFile:
    from rquant.data_audit_report import capture_data_audit_replica_identity
    path=proof.fixed_replica_path
    initial=capture_data_audit_replica_identity(Path(proof.primary_identity.canonical_path),path)
    descriptor=os.open(path,os.O_RDONLY|os.O_NONBLOCK|getattr(os,'O_NOFOLLOW',0))
    with os.fdopen(descriptor,'rb') as handle:
        observed=os.fstat(handle.fileno())
        identity=AuditReplicaFileIdentity(device=observed.st_dev,inode=observed.st_ino,size=observed.st_size,
            mtime_ns=observed.st_mtime_ns,ctime_ns=observed.st_ctime_ns)
        if identity!=initial or identity.size>64*1024**3:
            raise ValueError('collection initial strict source identity changed')
        digest=hashlib.sha256()
        remaining=identity.size
        while remaining:
            if datetime.now(UTC)>=deadline or (stop_requested is not None and stop_requested()):
                raise InterruptedError('collection source scan was stopped')
            chunk=handle.read(min(1024*1024,remaining))
            if not chunk:
                raise ValueError('collection source was truncated')
            digest.update(chunk)
            remaining-=len(chunk)
        if handle.read(1):
            raise ValueError('collection source grew during scan')
        after=os.fstat(handle.fileno())
        final=AuditReplicaFileIdentity(device=after.st_dev,inode=after.st_ino,size=after.st_size,
            mtime_ns=after.st_mtime_ns,ctime_ns=after.st_ctime_ns)
        if final!=initial or capture_data_audit_replica_identity(Path(proof.primary_identity.canonical_path),path)!=initial:
            raise ValueError('collection strict source identity changed during scan')
    if (initial.device,initial.inode,initial.size,initial.mtime_ns,digest.hexdigest())!=(
        proof.replica_device,proof.replica_inode,proof.replica_size,proof.replica_mtime_ns,proof.replica_sha256):
        raise ValueError('collection source differs from original sealed bytes')
    return VerifiedCollectionFile(identity=initial,replica_sha256=digest.hexdigest(),collection_reference=reference)


def _collection_pin_count(connection: object, directory: Path) -> int:
    rows=connection.execute('SELECT event_id FROM data_collection_source WHERE pin_released_at IS NULL LIMIT 4097').fetchall()
    if len(rows)>4096:
        raise ValueError('collection proof capacity reached')
    unfinished={row[0] for row in rows}
    entries=0
    with os.scandir(directory) as found:
        for item in found:
            entries+=1
            if entries>8194:
                raise ValueError('collection directory exceeds bounded capacity')
            if item.name.startswith('pin-') and item.name.endswith('.duckdb'):
                unfinished.add(item.name[4:-7])
    return len(unfinished)


def republish_verified_collection_sidecar(proof: DataCollectionProofV2, *, replica_path: Path,
        primary_writer_lease: PrimaryWriterLease, hash_timeout_seconds: int=600,
        deadline: datetime | None=None, stop_requested: Callable[[],bool] | None=None) -> bool:
    """Restore only the same proven snapshot after a legitimate hard-link transition."""
    from rquant.replica_generation import ReplicaGenerationMetadata,ReplicaFileWatermark,replica_generation_path
    from rquant.research_sync import _publish_replica_generation_sidecar
    proof=parse_collection_proof(proof.model_dump_json())
    if not 1<=hash_timeout_seconds<=1800:
        raise ValueError('collection sidecar deadline exceeds capacity')
    limit=datetime.now(UTC)+timedelta(seconds=hash_timeout_seconds)
    deadline=limit if deadline is None else min(limit,deadline)
    primary_path=Path(proof.primary_identity.canonical_path)
    lease=primary_writer_lease
    lease.verify(primary_path)
    if (lease.config.primary_device,lease.config.primary_inode)!=(proof.primary_identity.device,proof.primary_identity.inode):
        raise ValueError('collection sidecar primary identity changed')
    replica_path=Path(replica_path)
    if not replica_path.is_absolute() or replica_path.resolve(strict=True)!=replica_path or os.path.lexists(str(replica_path)+'.wal'):
        raise ValueError('collection sidecar source is not a canonical checkpointed replica')
    sidecar=replica_generation_path(replica_path)
    flags=os.O_RDONLY|os.O_NONBLOCK|getattr(os,'O_NOFOLLOW',0)|getattr(os,'O_CLOEXEC',0)
    def identity(found: os.stat_result) -> tuple[int,int,int,int,int]:
        return found.st_dev,found.st_ino,found.st_size,found.st_mtime_ns,found.st_ctime_ns
    def running() -> None:
        if datetime.now(UTC)>=deadline or (stop_requested is not None and stop_requested()):
            raise InterruptedError('collection sidecar verification stopped or expired')
        lease.verify(primary_path)
    running()
    with os.fdopen(os.open(replica_path,flags),'rb') as source,os.fdopen(os.open(sidecar,flags),'rb') as metadata_file:
        before=os.fstat(source.fileno())
        metadata_before=os.fstat(metadata_file.fileno())
        expected=(proof.replica_device,proof.replica_inode,proof.replica_size,proof.replica_mtime_ns)
        if (not stat.S_ISREG(before.st_mode) or identity(before)[:4]!=expected
                or (before.st_dev,before.st_ino)==(proof.primary_identity.device,proof.primary_identity.inode)):
            raise ValueError('collection sidecar replica identity changed')
        if not stat.S_ISREG(metadata_before.st_mode) or not 0<metadata_before.st_size<=8192:
            raise ValueError('collection sidecar is invalid or exceeds capacity')
        payload=metadata_file.read(8193)
        if payload!=proof.original_sidecar_bytes.encode() or hashlib.sha256(payload).hexdigest()!=proof.original_sidecar_sha256:
            raise ValueError('collection sidecar differs from the original proof')
        metadata=ReplicaGenerationMetadata.model_validate_json(payload)
        if (metadata.source_database!=primary_path or metadata.source_before!=metadata.source_after
                or (metadata.source_before.main.device,metadata.source_before.main.inode)!=(proof.primary_identity.device,proof.primary_identity.inode)
                or metadata.replica!=ReplicaFileWatermark(device=expected[0],inode=expected[1],size=expected[2],mtime_ns=expected[3])):
            raise ValueError('collection sidecar original source chain changed')
        def source_stable() -> None:
            running()
            if (identity(os.fstat(source.fileno()))!=identity(before)
                    or identity(replica_path.stat(follow_symlinks=False))!=identity(before)
                    or os.path.lexists(str(replica_path)+'.wal')):
                raise ValueError('collection sidecar replica moved during verification')
        def originals_stable() -> None:
            source_stable()
            if (identity(os.fstat(metadata_file.fileno()))!=identity(metadata_before)
                    or identity(sidecar.stat(follow_symlinks=False))!=identity(metadata_before)):
                raise ValueError('collection sidecar moved during verification')
        originals_stable()
        if before.st_ctime_ns<=metadata_before.st_ctime_ns:
            # This is the already published generation. No new content verification is claimed.
            return False
        if _file_sha(replica_path,max_bytes=64*1024**3,deadline=deadline,stop_requested=stop_requested)!=proof.replica_sha256:
            raise ValueError('collection sidecar full replica digest changed')
        originals_stable()
        descriptor,name=tempfile.mkstemp(prefix='.collection-generation-',dir=sidecar.parent)
        temporary=Path(name)
        published=False
        published_identity=None
        try:
            with os.fdopen(descriptor,'wb') as output:
                output.write(payload)
                output.flush()
                os.fchmod(output.fileno(),stat.S_IMODE(metadata_before.st_mode))
                os.fsync(output.fileno())
                published_identity=identity(os.fstat(output.fileno()))[:4]
                originals_stable()
                _publish_replica_generation_sidecar(temporary,sidecar)
                published=True
                source_stable()
                if (identity(os.fstat(output.fileno()))[:4]!=published_identity
                        or identity(sidecar.stat(follow_symlinks=False))[:4]!=published_identity):
                    raise ValueError('collection sidecar publication moved')
                directory_fd=os.open(sidecar.parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
                source_stable()
            return True
        except BaseException:
            # A raced or stopped publication must not make an unverified source readable.
            if published and published_identity is not None:
                found=sidecar.stat(follow_symlinks=False)
                if identity(found)[:4]==published_identity:
                    sidecar.unlink()
                    directory_fd=os.open(sidecar.parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
                    try:
                        os.fsync(directory_fd)
                    finally:
                        os.close(directory_fd)
            raise
        finally:
            temporary.unlink(missing_ok=True)


def restore_collection_report_replica(jobs: DataAuditReportJobStore, task_id: str, *,
        replica_path: Path, primary_writer_gate: PrimaryWriterGateConfig,
        hash_timeout_seconds: int=600, stop_requested: Callable[[],bool] | None=None) -> bool:
    from rquant.data_audit_report import CollectionDataAuditReport,load_data_audit_report,data_audit_report_path
    receipt=jobs.status(task_id)
    if receipt.status!='succeeded' or receipt.report_hash is None:
        raise ValueError('collection sidecar restoration requires a readable original success')
    report=load_data_audit_report(data_audit_report_path(jobs.report_directory,receipt.report_hash))
    if not isinstance(report,CollectionDataAuditReport):
        return False
    if jobs.collection_directory is None:
        raise ValueError('collection sidecar restoration lacks the original proof directory')
    original=jobs.admission_by_key(report.collection_reference.event_id)
    if original is None or original[1]!=task_id or original[0].collection_reference!=report.collection_reference:
        raise ValueError('collection sidecar restoration names another original task')
    proof=load_collection_proof(jobs.collection_directory,report.collection_reference)
    if report.collection_proof!=proof:
        raise ValueError('collection sidecar restoration proof changed')
    with PrimaryWriterGate(primary_writer_gate).acquire() as lease:
        return republish_verified_collection_sidecar(proof,replica_path=replica_path,primary_writer_lease=lease,
            hash_timeout_seconds=hash_timeout_seconds,stop_requested=stop_requested)


def collection_source_event_id(identity: CanonicalDatabaseIdentity, references: tuple[CollectionReceiptReference,...], *,
        audit_start: date, observed_through: date,receipt_manifest: CollectionReceiptManifest | None=None) -> str:
    from rquant.data_audit_datasets import catalog_contract_sha256
    body=dict(primary=identity.generation_id,refs=references,
        catalog=catalog_contract_sha256(),audit_start=audit_start,observed_through=observed_through)
    if receipt_manifest is not None:
        body['receipt_manifest']=receipt_manifest.content_sha256
    return canonical_sha256(body)


def release_completed_collection_pins(jobs: DataAuditReportJobStore, *, replica_path: Path | None=None,
        primary_writer_lease: PrimaryWriterLease | None=None, hash_timeout_seconds: int=600,
        deadline: datetime | None=None, stop_requested: Callable[[],bool] | None=None) -> int:
    """Release only a readable original terminal success. Unknown files remain capacity consumers."""
    directory=jobs.collection_directory
    if directory is None:
        return 0
    if (replica_path is None)!=(primary_writer_lease is None):
        raise ValueError('collection pin release requires both replica and original writer lease')
    if primary_writer_lease is not None:
        primary_writer_lease.verify()
    with closing(jobs._connect()) as connection:
        rows=connection.execute('SELECT s.*,j.status,j.lease_token,j.lease_until,j.request_json,j.report_hash,j.replica_sha256 '
            'FROM data_collection_source s JOIN data_audit_report_job j ON j.task_id=s.task_id '
            "WHERE s.pin_released_at IS NULL AND j.status='succeeded' AND j.lease_token IS NULL AND j.lease_until IS NULL "
            'ORDER BY s.sequence LIMIT 2').fetchall()
    released=0
    for row in rows:
        reference=AuditCollectionReference.model_validate_json(row['reference_json'])
        proof=load_collection_proof(directory,reference)
        try:
            receipt=jobs.status(row['task_id'])
        except (OSError,ValueError,RuntimeError):
            continue
        if receipt.status!='succeeded' or receipt.report_hash!=row['report_hash'] or row['replica_sha256']!=proof.replica_sha256:
            continue
        from rquant.data_audit_report_jobs import DataAuditReportJobRequest
        request=DataAuditReportJobRequest.model_validate_json(row['request_json'])
        if request.collection_reference!=reference or request.replica_path!=proof.fixed_replica_path:
            raise ValueError('terminal collection source differs from original audit request')
        with jobs._transaction() as connection:
            current=connection.execute('SELECT s.reference_json,s.task_id,s.pin_released_at,j.status,j.lease_token,j.lease_until,'
                'j.request_json,j.report_hash,j.replica_sha256 FROM data_collection_source s '
                'JOIN data_audit_report_job j ON j.task_id=s.task_id WHERE s.event_id=?',[reference.event_id]).fetchone()
            if current is None or tuple(current)!=(row['reference_json'],row['task_id'],None,'succeeded',None,None,
                    row['request_json'],row['report_hash'],row['replica_sha256']):
                continue
            try:
                found=proof.fixed_replica_path.stat(follow_symlinks=False)
            except FileNotFoundError:
                found=None
            if found is not None:
                if not stat.S_ISREG(found.st_mode) or (found.st_dev,found.st_ino,found.st_size,found.st_mtime_ns)!=(
                        proof.replica_device,proof.replica_inode,proof.replica_size,proof.replica_mtime_ns):
                    raise ValueError('terminal pin changed before release')
                proof.fixed_replica_path.unlink()
                descriptor=os.open(directory,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
            # If acknowledgement was lost after unlink, the same readable terminal row resumes this update.
            changed=connection.execute('UPDATE data_collection_source SET pin_released_at=? '
                'WHERE event_id=? AND task_id=? AND pin_released_at IS NULL',
                [jobs.clock().isoformat(),reference.event_id,row['task_id']]).rowcount
            if changed!=1:
                raise ValueError('collection pin release acknowledgement changed')
        released+=1
        if replica_path is not None:
            current_replica=replica_path.stat(follow_symlinks=False)
            if (current_replica.st_dev,current_replica.st_ino)==(proof.replica_device,proof.replica_inode):
                republish_verified_collection_sidecar(proof,replica_path=replica_path,primary_writer_lease=primary_writer_lease,
                    hash_timeout_seconds=hash_timeout_seconds,deadline=deadline,stop_requested=stop_requested)
    return released


def seal_data_collection_proof(jobs: DataAuditReportJobStore, *, primary_path: Path,
    replica_path: Path, calendar: MarketCalendarAuthority, references: tuple[CollectionReceiptReference,...],
    audit_start: date, observed_through: date, primary_writer_lease: PrimaryWriterLease,
    clock: Callable[[],datetime] | None = None, hash_timeout_seconds: int = 600,
    stop_requested: Callable[[],bool] | None = None,receipt_manifest: CollectionReceiptManifest | None=None,
    receipt_pages: tuple[CollectionManifestPage,...] | None=None) -> AuditCollectionReference:
    import duckdb
    from rquant.data_audit_datasets import catalog_contract_sha256
    from rquant.daily_canonical_publisher import CanonicalDatabaseIdentity
    from rquant.replica_generation import validate_replica_generation,replica_generation_path
    directory=jobs.collection_directory
    if directory is None:
        raise ValueError('collection bridge is not configured')
    if not 1<=hash_timeout_seconds<=1800:
        raise ValueError('collection scan deadline exceeds capacity')
    deadline=datetime.now(UTC)+timedelta(seconds=hash_timeout_seconds)
    primary_writer_lease.verify(primary_path)
    identity=CanonicalDatabaseIdentity(canonical_path=str(primary_path),device=primary_path.stat().st_dev,
        inode=primary_path.stat().st_ino)
    references=tuple(CollectionReceiptReference.model_validate(item) for item in references)
    if (receipt_manifest is None)!=(receipt_pages is None):
        raise ValueError('complete receipt manifest requires its exact pages')
    if receipt_manifest is not None:
        from rquant.data_collection_manifest import (publish_receipt_manifest,iter_receipt_manifest,
            validate_receipt_manifest_pages,receipt_manifest_directory,remove_owned_receipt_manifest)
        receipt_manifest=CollectionReceiptManifest.model_validate_json(receipt_manifest.model_dump_json())
        if (receipt_manifest.primary_identity,receipt_manifest.audit_start,receipt_manifest.observed_through)!=(identity,audit_start,observed_through):
            raise ValueError('complete manifest does not match original source/range')
        validate_receipt_manifest_pages(receipt_manifest,receipt_pages)
        if tuple(entry.reference for page in receipt_pages for entry in page.entries if entry.reference is not None)!=references:
            raise ValueError('complete manifest reference membership changed')
    event=collection_source_event_id(identity,references,audit_start=audit_start,observed_through=observed_through,receipt_manifest=receipt_manifest)
    release_completed_collection_pins(jobs,replica_path=replica_path,primary_writer_lease=primary_writer_lease,
        hash_timeout_seconds=hash_timeout_seconds,deadline=deadline,stop_requested=stop_requested)
    with closing(jobs._connect()) as state:
        previous=state.execute('SELECT reference_json,pin_released_at,task_id FROM data_collection_source WHERE event_id=?',[event]).fetchone()
        if previous is None:
            if _collection_pin_count(state,directory)>=2 or state.execute('SELECT COUNT(*) FROM data_collection_source').fetchone()[0]>=4096:
                raise ValueError('collection proof capacity reached')
            sequence=state.execute('SELECT coalesce(max(sequence),0)+1 FROM data_collection_source').fetchone()[0]
            latest=state.execute('SELECT event_id FROM data_collection_source ORDER BY sequence DESC LIMIT 1').fetchone()
    if previous is not None:
        reference=AuditCollectionReference.model_validate_json(previous[0])
        proof=load_collection_proof(directory,reference)
        if proof.calendar!=calendar or (proof.receipt_manifest!=receipt_manifest if isinstance(proof,DataCollectionProofV3)
                else receipt_manifest is not None or proof.references!=references):
            raise ValueError('same collection event has conflicting source binding')
        if receipt_manifest is not None and tuple(entry.reference for entry in iter_receipt_manifest(directory,receipt_manifest,
                deadline=deadline,stop_requested=stop_requested) if entry.reference is not None)!=references:
            raise ValueError('accepted complete manifest reference membership changed')
        if previous[1] is None:
            verify_fixed_collection_file(proof,deadline=deadline,stop_requested=stop_requested)
        elif previous[2] is None or jobs.status(previous[2]).status!='succeeded':
            raise ValueError('released collection source lacks readable original success')
        if previous[1] is not None:
            republish_verified_collection_sidecar(proof,replica_path=replica_path,primary_writer_lease=primary_writer_lease,
                hash_timeout_seconds=hash_timeout_seconds,deadline=deadline,stop_requested=stop_requested)
        return reference
    validate_replica_generation(primary_path=primary_path,replica_path=replica_path)
    if replica_path.stat().st_size>64*1024**3 or shutil.disk_usage(directory).free<16*1024*1024:
        raise ValueError('collection replica or disk capacity exceeded')
    sidecar=replica_generation_path(replica_path)
    descriptor=os.open(sidecar,os.O_RDONLY|os.O_NONBLOCK|getattr(os,'O_NOFOLLOW',0))
    with os.fdopen(descriptor,'rb') as handle:
        sidecar_stat=os.fstat(handle.fileno())
        if not stat.S_ISREG(sidecar_stat.st_mode) or sidecar_stat.st_size>8192:
            raise ValueError('replica sidecar exceeds capacity')
        sidecar_bytes=handle.read(8193)
    pin=directory/f'pin-{event}.duckdb'
    if pin.exists():
        raise ValueError('unknown collection pin requires explicit reconciliation')
    manifest_path=None if receipt_manifest is None else receipt_manifest_directory(directory,receipt_manifest)
    if manifest_path is not None and (manifest_path.exists() or manifest_path.is_symlink()):
        raise ValueError('unknown complete manifest requires original pin reconciliation')
    os.link(replica_path,pin,follow_symlinks=False)
    owned_proof_path=None
    owned_manifest_identity=None
    try:
        if manifest_path is not None:
            manifest_path.mkdir(mode=0o700)
            created=manifest_path.stat(follow_symlinks=False)
            owned_manifest_identity=(created.st_dev,created.st_ino)
            fd=os.open(directory,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
            publish_receipt_manifest(directory,receipt_manifest,receipt_pages,deadline=deadline,stop_requested=stop_requested)
            if tuple(entry.reference for entry in iter_receipt_manifest(directory,receipt_manifest,deadline=deadline,
                    stop_requested=stop_requested) if entry.reference is not None)!=references:
                raise ValueError('complete manifest reference membership changed')
        captured=pin.stat(follow_symlinks=False)
        digest=_file_sha(pin,max_bytes=64*1024**3,deadline=deadline,stop_requested=stop_requested)
        with duckdb.connect(str(pin),read_only=True) as connection:
            from rquant.storage.duckdb import DuckDBStore
            reader=object.__new__(DuckDBStore)
            reader._conn=connection
            reader.path=pin
            calendar_sha=_calendar_facts(reader,calendar,audit_start,observed_through)
            proof_model=DataCollectionProofV2 if receipt_manifest is None else DataCollectionProofV3
            manifest_fields={} if receipt_manifest is None else {'receipt_manifest':receipt_manifest}
            proof=proof_model(event_id=event,sequence=sequence,previous_event_id=None if latest is None else latest[0],
                primary_identity=identity,fixed_replica_path=pin,replica_device=captured.st_dev,replica_inode=captured.st_ino,
                replica_size=captured.st_size,replica_mtime_ns=captured.st_mtime_ns,replica_capture_ctime_ns=captured.st_ctime_ns,
                replica_sha256=digest,original_sidecar_bytes=sidecar_bytes.decode(),original_sidecar_sha256=hashlib.sha256(sidecar_bytes).hexdigest(),
                calendar=calendar,calendar_facts_sha256=calendar_sha,catalog_contract_sha256=catalog_contract_sha256(),
                audit_start=audit_start,observed_through=observed_through,references=references if receipt_manifest is None else (),
                available_securities=_available_security_inventory(connection),
                sealed_at=(clock or (lambda:datetime.now(UTC)))(),**manifest_fields)
            verify_collection_source(connection,proof,snapshot_root=jobs.collection_snapshot_root)
        # This full scan also establishes the final strict FD/path identity; it never holds audit SQLite.
        verify_fixed_collection_file(proof,deadline=deadline,stop_requested=stop_requested)
        before_admission=pin.stat(follow_symlinks=False)
        validate_replica_generation(primary_path=primary_path,replica_path=replica_path)
        payload=_json_bytes(proof.model_dump(mode='json'))
        reference=AuditCollectionReference(event_id=event,binding_sha256=proof.binding_sha256,sequence=sequence,
            proof_sha256=hashlib.sha256(payload).hexdigest(),relative_proof_name=f'collection-{proof.binding_sha256}.json',byte_count=len(payload))
        descriptor,temporary_name=tempfile.mkstemp(prefix='.collection-',dir=directory)
        temporary=Path(temporary_name)
        try:
            with os.fdopen(descriptor,'wb') as handle:
                handle.write(payload)
                handle.flush()
                os.fchmod(handle.fileno(),0o444)
                os.fsync(handle.fileno())
            owned_proof_path=directory/reference.relative_proof_name
            os.link(temporary,owned_proof_path,follow_symlinks=False)
        finally:
            temporary.unlink(missing_ok=True)
        fd=os.open(directory,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        with jobs._transaction() as state:
            primary_writer_lease.verify(primary_path)
            after=pin.stat(follow_symlinks=False)
            if (after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns)!=(
                    before_admission.st_dev,before_admission.st_ino,before_admission.st_size,before_admission.st_mtime_ns,before_admission.st_ctime_ns):
                raise ValueError('collection source identity changed before registration')
            if (datetime.now(UTC)>=deadline or (stop_requested is not None and stop_requested())
                    or state.execute('SELECT 1 FROM data_collection_source WHERE event_id=?',[event]).fetchone() is not None
                    or state.execute('SELECT coalesce(max(sequence),0)+1 FROM data_collection_source').fetchone()[0]!=sequence
                    or _collection_pin_count(state,directory)>2):
                raise ValueError('collection source sequence, capacity or deadline changed')
            state.execute('INSERT INTO data_collection_source(event_id,sequence,reference_json) VALUES(?,?,?)',
                [event,sequence,reference.model_dump_json()])
        return reference
    except BaseException:
        # A lost SQLite commit response must not remove a possibly accepted source.
        try:
            with closing(jobs._connect()) as state:
                absent=state.execute('SELECT 1 FROM data_collection_source WHERE event_id=?',[event]).fetchone() is None
        except (OSError,RuntimeError,sqlite3.Error):
            absent=False
        if absent:
            if manifest_path is not None:
                if owned_manifest_identity is None:
                    if manifest_path.exists() or manifest_path.is_symlink():
                        raise ValueError('complete manifest ownership is unknown; original pin retained')
                else:
                    remove_owned_receipt_manifest(directory,receipt_manifest,receipt_pages,
                        device=owned_manifest_identity[0],inode=owned_manifest_identity[1])
            pin.unlink(missing_ok=True)
            if owned_proof_path is not None:
                owned_proof_path.unlink(missing_ok=True)
        raise
