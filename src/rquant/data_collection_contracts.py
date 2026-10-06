"""Typed source observations and transaction-bound collection proofs."""
from __future__ import annotations

import hashlib
import json
import math
from datetime import date, datetime
from pathlib import Path
from typing import Annotated, Literal, Self

import pandas as pd
from pydantic import Field, StringConstraints, TypeAdapter, model_validator

from rquant.daily_canonical_publisher import CanonicalDatabaseIdentity, CanonicalTableWatermark
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.daily_valuation_pit import TsCode

Sha256 = Annotated[str,StringConstraints(pattern=r'^[0-9a-f]{64}$')]
CommitSha = Annotated[str,StringConstraints(pattern=r'^[0-9a-f]{40}$')]
MAX_COLLECTION_PROOF_BYTES = 256*1024


class SourceObservation(RuntimeContractModel):
    api_name: str = Field(min_length=1,max_length=80)
    request_sha256: Sha256
    response_sha256: Sha256
    observed_at: AwareUtcDatetime
    row_count: int = Field(strict=True,ge=0,le=8000)
    response_bytes: int = Field(strict=True,ge=0,le=32*1024*1024)
    possibly_truncated: bool = False
    source_normalization_version: Literal['tushare-nullable-v1'] | None = Field(default=None,exclude_if=lambda value:value is None)

    @classmethod
    def from_frame(cls, api_name: str, parameters: object, frame: pd.DataFrame, *,
                   observed_at: datetime, possibly_truncated: bool = False,
                   source_normalization_version: Literal['tushare-nullable-v1'] | None = None) -> SourceObservation:
        financial_api = api_name in {'fina_indicator','income','balancesheet','cashflow','forecast','express','dividend'}
        if len(frame)>8000 or len(frame.columns)>(256 if financial_api else 128):
            raise ValueError('collection response exceeds row/column capacity')
        records: list[list[object]] = []
        for row in frame.itertuples(index=False,name=None):
            normalized: list[object] = []
            for value in row:
                if isinstance(value,float) and not math.isfinite(value):
                    raise ValueError('collection response contains nonfinite numeric value')
                if value is pd.NA or value is pd.NaT:
                    value=None
                elif hasattr(value,'item') and not isinstance(value,(str,date,datetime)):
                    value=value.item()
                normalized.append(value)
            records.append(normalized)
        payload={'columns':tuple(str(item) for item in frame.columns),'rows':records}
        serialized=json.dumps(payload,default=lambda item:item.isoformat(),allow_nan=False,
                              sort_keys=True,separators=(',',':')).encode()
        if len(serialized)>32*1024*1024:
            raise ValueError('collection response exceeds byte capacity')
        return cls(api_name=api_name,request_sha256=canonical_sha256(parameters),
                   response_sha256=canonical_sha256(payload),observed_at=observed_at,
                   row_count=len(frame),response_bytes=len(serialized),possibly_truncated=possibly_truncated,
                   source_normalization_version=source_normalization_version)


class CollectionRecorderConfig(RuntimeContractModel):
    collector_id: Literal['legacy_daily','dataset_backfill','controlled_backfill','financial']
    run_id: str = Field(min_length=1,max_length=128)
    owner: str = Field(min_length=1,max_length=128)
    code_commit: CommitSha
    source_generation_id: Sha256
    calendar: MarketCalendarAuthority


class DatasetCollectionClaim(RuntimeContractModel):
    dataset_id: str = Field(min_length=1,max_length=80)
    trade_date: date
    scope: Literal['actual_date_rows','actual_snapshot_partition','actual_financial_queries','actual_receipt_set']
    row_count: int = Field(strict=True,ge=0)
    source_api: str = Field(min_length=1,max_length=80)
    content_sha256: Sha256
    coverage_complete: Literal[False] = False


class IngestionCommitReceipt(RuntimeContractModel):
    contract: Literal['ingestion-commit-receipt/v1'] = 'ingestion-commit-receipt/v1'
    receipt_id: Sha256 | None = None
    event_id: Sha256
    sequence: int = Field(strict=True,ge=1)
    collector_id: Literal['legacy_daily','dataset_backfill','controlled_backfill','financial']
    run_id: str = Field(min_length=1,max_length=128)
    owner: str = Field(min_length=1,max_length=128)
    code_commit: CommitSha
    source_generation_id: Sha256
    database_identity: CanonicalDatabaseIdentity
    calendar: MarketCalendarAuthority
    trade_date: date
    observations: tuple[SourceObservation,...] = Field(min_length=1,max_length=32)
    watermarks: tuple[CanonicalTableWatermark,...] = Field(min_length=1,max_length=24)
    datasets: tuple[DatasetCollectionClaim,...] = Field(max_length=24)
    transaction_id: int = Field(strict=True,ge=1)
    committed_at: AwareUtcDatetime
    valuation_content_sha256: Sha256 | None = Field(default=None,exclude_if=lambda value:value is None)
    valuation_revision: int | None = Field(default=None,strict=True,ge=1,exclude_if=lambda value:value is None)

    @model_validator(mode='after')
    def bind(self) -> Self:
        if (self.valuation_content_sha256 is None)!=(self.valuation_revision is None):
            raise ValueError('collected valuation requires both original version and content evidence')
        if any(item.trade_date != self.trade_date for item in (*self.watermarks,*self.datasets)):
            raise ValueError('receipt date scope changed')
        if any(item.possibly_truncated for item in self.observations):
            raise ValueError('truncated collection cannot claim a complete source response')
        if self.trade_date not in self.calendar.open_dates or self.calendar.generated_at>self.committed_at:
            raise ValueError('receipt lacks a current original SSE calendar')
        expected=canonical_sha256(self.model_dump(mode='python',exclude={'receipt_id'}))
        if self.receipt_id is not None and self.receipt_id!=expected:
            raise ValueError('receipt digest changed')
        object.__setattr__(self,'receipt_id',expected)
        return self


class CollectionReceiptReference(RuntimeContractModel):
    kind: Literal['ingestion','canonical','dataset_snapshot','financial']
    receipt_id: Sha256
    dataset_ids: tuple[str,...] = Field(min_length=1,max_length=24)


class DataCollectionProofV2(RuntimeContractModel):
    contract: Literal['data-collection-proof/v2'] = 'data-collection-proof/v2'
    event_id: Sha256
    binding_sha256: Sha256 | None = None
    sequence: int = Field(strict=True,ge=1)
    previous_event_id: Sha256 | None = None
    primary_identity: CanonicalDatabaseIdentity
    fixed_replica_path: Path
    replica_device: int = Field(strict=True,ge=0)
    replica_inode: int = Field(strict=True,gt=0)
    replica_size: int = Field(strict=True,gt=0,le=64*1024**3)
    replica_mtime_ns: int = Field(strict=True,gt=0)
    replica_capture_ctime_ns: int = Field(strict=True,gt=0)
    replica_sha256: Sha256
    original_sidecar_bytes: Annotated[str,StringConstraints(strip_whitespace=False)] = Field(min_length=1,max_length=8192)
    original_sidecar_sha256: Sha256
    calendar: MarketCalendarAuthority
    calendar_facts_sha256: Sha256
    catalog_contract_sha256: Sha256
    audit_start: date
    observed_through: date
    references: tuple[CollectionReceiptReference,...] = Field(min_length=1,max_length=384)
    available_securities: tuple[TsCode,...] | None = Field(default=None,max_length=8000,exclude_if=lambda value:value is None)
    sealed_at: AwareUtcDatetime

    @model_validator(mode='after')
    def bind(self) -> Self:
        if self.available_securities is not None and self.available_securities!=tuple(sorted(set(self.available_securities))):
            raise ValueError('collection security inventory must be unique and sorted')
        if not self.fixed_replica_path.is_absolute():
            raise ValueError('fixed replica path must be absolute')
        if hashlib.sha256(self.original_sidecar_bytes.encode()).hexdigest()!=self.original_sidecar_sha256:
            raise ValueError('original sidecar bytes changed')
        if self.audit_start>self.observed_through or (self.observed_through-self.audit_start).days>=3660:
            raise ValueError('collection audit range exceeds capacity')
        counts: dict[str,int]={}
        for reference in self.references:
            for dataset in reference.dataset_ids:
                counts[dataset]=counts.get(dataset,0)+1
        if len(counts)>24 or max(counts.values(),default=0)>16:
            raise ValueError('collection proof exceeds per-dataset receipt capacity')
        expected=canonical_sha256(self.model_dump(mode='python',exclude={'binding_sha256'}))
        if self.binding_sha256 is not None and self.binding_sha256!=expected:
            raise ValueError('collection proof binding changed')
        object.__setattr__(self,'binding_sha256',expected)
        if len(self.model_dump_json().encode())>MAX_COLLECTION_PROOF_BYTES:
            raise ValueError('collection proof exceeds byte capacity')
        return self


class AuditCollectionReference(RuntimeContractModel):
    event_id: Sha256
    binding_sha256: Sha256
    proof_sha256: Sha256
    relative_proof_name: str = Field(pattern=r'^collection-[0-9a-f]{64}\.json$')
    sequence: int = Field(strict=True,ge=1)
    byte_count: int = Field(strict=True,gt=0,le=MAX_COLLECTION_PROOF_BYTES)


MAX_RECEIPT_MANIFEST_ENTRIES = 4095
MAX_RECEIPT_MANIFEST_PAGES = 256
MAX_RECEIPT_PAGE_ENTRIES = 16
MAX_RECEIPT_ENTRY_BYTES = 1536
MAX_RECEIPT_MANIFEST_BYTES = 8*1024*1024


class CollectionReceiptSet(RuntimeContractModel):
    dataset_id: str = Field(min_length=1,max_length=80)
    receipt_count: int = Field(strict=True,ge=1,le=MAX_RECEIPT_MANIFEST_ENTRIES)
    scope_count: int = Field(strict=True,ge=1,le=MAX_RECEIPT_MANIFEST_ENTRIES*250)
    receipt_ids_sha256: Sha256
    scopes_sha256: Sha256
    first_date: date
    last_date: date
    row_count: int = Field(strict=True,ge=0,le=1_000_000_000_000)

    @model_validator(mode='after')
    def dates(self) -> Self:
        if self.first_date>self.last_date:
            raise ValueError('receipt set dates are reversed')
        return self

    def aggregate_scope(self) -> DatasetCollectionClaim:
        return DatasetCollectionClaim(dataset_id=self.dataset_id,trade_date=self.last_date,scope='actual_receipt_set',
            row_count=self.row_count,source_api='original_receipt_set',content_sha256=canonical_sha256(self))


class CollectionManifestEntry(RuntimeContractModel):
    index: int = Field(strict=True,ge=0,lt=MAX_RECEIPT_MANIFEST_ENTRIES)
    task_id: str = Field(pattern=r'^[a-z0-9:-]{1,128}$')
    task_sha256: Sha256
    kind: Literal['financial','day','derived_tail']
    task_receipt_id: Sha256
    reference: CollectionReceiptReference | None = None
    request_count: int = Field(strict=True,ge=0,le=32)
    requests_sha256: Sha256
    dispatch_count: int = Field(strict=True,ge=0,le=192)
    dispatches_sha256: Sha256
    version_count: int = Field(strict=True,ge=0,le=250)
    versions_sha256: Sha256
    observed_start: AwareUtcDatetime | None = None
    observed_end: AwareUtcDatetime | None = None

    @model_validator(mode='after')
    def bounded(self) -> Self:
        if (self.observed_start is None)!=(self.observed_end is None) or (
                self.observed_start is not None and self.observed_start>self.observed_end):
            raise ValueError('manifest actual observation bounds changed')
        if (self.kind=='derived_tail')!=(self.reference is None):
            raise ValueError('manifest source reference does not match original task kind')
        if self.reference is not None and (self.reference.kind!='financial' if self.kind=='financial' else self.reference.kind!='ingestion'):
            raise ValueError('manifest original receipt kind changed')
        if self.kind=='financial' and self.reference.receipt_id!=self.task_receipt_id:
            raise ValueError('manifest financial task/source receipt identity changed')
        if len(self.model_dump_json().encode())>MAX_RECEIPT_ENTRY_BYTES:
            raise ValueError('manifest entry exceeds fixed byte bound')
        return self


class CollectionManifestPage(RuntimeContractModel):
    contract: Literal['collection-receipt-page/v1']='collection-receipt-page/v1'
    index: int = Field(strict=True,ge=0,lt=MAX_RECEIPT_MANIFEST_PAGES)
    entries: tuple[CollectionManifestEntry,...] = Field(min_length=1,max_length=MAX_RECEIPT_PAGE_ENTRIES)

    @model_validator(mode='after')
    def ordered(self) -> Self:
        if tuple(item.index for item in self.entries)!=tuple(range(self.index*16,self.index*16+len(self.entries))):
            raise ValueError('manifest page entry order changed')
        if len(self.model_dump_json().encode())>MAX_COLLECTION_PROOF_BYTES:
            raise ValueError('manifest page exceeds fixed byte bound')
        return self


class CollectionManifestPageReference(RuntimeContractModel):
    index: int = Field(strict=True,ge=0,lt=MAX_RECEIPT_MANIFEST_PAGES)
    relative_name: str = Field(pattern=r'^page-[0-9]{3}-[0-9a-f]{64}\.json$')
    sha256: Sha256
    byte_count: int = Field(strict=True,gt=0,le=MAX_COLLECTION_PROOF_BYTES)
    entry_count: int = Field(strict=True,ge=1,le=MAX_RECEIPT_PAGE_ENTRIES)

    @model_validator(mode='after')
    def name(self) -> Self:
        if self.relative_name!=f'page-{self.index:03d}-{self.sha256}.json':
            raise ValueError('manifest page name differs from exact index/content')
        return self


class CollectionReceiptManifest(RuntimeContractModel):
    contract: Literal['collection-receipt-manifest/v1']='collection-receipt-manifest/v1'
    content_sha256: Sha256 | None = None
    execution_id: Sha256
    owner: str = Field(min_length=1,max_length=256)
    manifest_id: Sha256
    plan_sha256: Sha256
    primary_identity: CanonicalDatabaseIdentity
    policy_generation: Sha256
    source_account_sha256: Sha256
    source_config_sha256: Sha256
    quota_source: str = Field(min_length=1,max_length=128)
    quota_ledger_device: int = Field(strict=True,ge=0)
    quota_ledger_inode: int = Field(strict=True,gt=0)
    audit_start: date
    observed_through: date
    domain_sha256: Sha256
    entry_count: int = Field(strict=True,ge=1,le=MAX_RECEIPT_MANIFEST_ENTRIES)
    entries_sha256: Sha256
    request_count: int = Field(strict=True,ge=0,le=MAX_RECEIPT_MANIFEST_ENTRIES*32)
    requests_sha256: Sha256
    dispatch_count: int = Field(strict=True,ge=0,le=MAX_RECEIPT_MANIFEST_ENTRIES*192)
    dispatches_sha256: Sha256
    observed_start: AwareUtcDatetime | None = None
    observed_end: AwareUtcDatetime | None = None
    pages: tuple[CollectionManifestPageReference,...] = Field(min_length=1,max_length=MAX_RECEIPT_MANIFEST_PAGES)
    datasets: tuple[CollectionReceiptSet,...] = Field(min_length=1,max_length=26)

    @model_validator(mode='after')
    def bind(self) -> Self:
        if self.audit_start>self.observed_through or (self.observed_through-self.audit_start).days>=3660:
            raise ValueError('manifest original audit range exceeds capacity')
        if (self.observed_start is None)!=(self.observed_end is None) or (
                self.observed_start is not None and self.observed_start>self.observed_end):
            raise ValueError('manifest original observation bounds changed')
        if tuple(page.index for page in self.pages)!=tuple(range(len(self.pages))) or any(
                page.entry_count!=16 for page in self.pages[:-1]) or sum(page.entry_count for page in self.pages)!=self.entry_count:
            raise ValueError('manifest page count or complete entry order changed')
        if tuple(item.dataset_id for item in self.datasets)!=tuple(sorted(set(item.dataset_id for item in self.datasets))):
            raise ValueError('manifest datasets must be unique and sorted')
        expected=canonical_sha256(self.model_dump(mode='python',exclude={'content_sha256'}))
        if self.content_sha256 is not None and self.content_sha256!=expected:
            raise ValueError('manifest root binding changed')
        object.__setattr__(self,'content_sha256',expected)
        size=len(self.model_dump_json().encode())
        if size>MAX_COLLECTION_PROOF_BYTES or size+sum(page.byte_count for page in self.pages)>MAX_RECEIPT_MANIFEST_BYTES:
            raise ValueError('manifest root or full material exceeds capacity')
        return self


class DataCollectionProofV3(DataCollectionProofV2):
    contract: Literal['data-collection-proof/v3']='data-collection-proof/v3'
    references: tuple[CollectionReceiptReference,...] = Field(default=(),max_length=0)
    receipt_manifest: CollectionReceiptManifest

    @model_validator(mode='after')
    def complete_manifest(self) -> Self:
        root=self.receipt_manifest
        if (root.primary_identity,root.audit_start,root.observed_through)!=(self.primary_identity,self.audit_start,self.observed_through):
            raise ValueError('complete manifest differs from fixed original source/range')
        if root.observed_end is not None and root.observed_end>self.sealed_at:
            raise ValueError('complete manifest contains future source observations')
        return self


DataCollectionProof = Annotated[DataCollectionProofV2|DataCollectionProofV3,Field(discriminator='contract')]


def parse_collection_proof(payload: str | bytes) -> DataCollectionProofV2 | DataCollectionProofV3:
    return TypeAdapter(DataCollectionProof).validate_json(payload)


class DatasetCollectionEvidence(RuntimeContractModel):
    dataset_id: str = Field(min_length=1,max_length=80)
    status: Literal['verified','partial','unconfirmed']
    receipt_ids: tuple[Sha256,...] = Field(max_length=16)
    scopes: tuple[DatasetCollectionClaim,...] = Field(max_length=16)
    completed_through: date | None = None
    receipt_set: CollectionReceiptSet | None = Field(default=None,exclude_if=lambda value:value is None)

    @model_validator(mode='after')
    def bind(self) -> Self:
        if len(self.receipt_ids)!=len(set(self.receipt_ids)):
            raise ValueError('collection receipt references are duplicated')
        if self.receipt_set is not None:
            if (self.receipt_ids or self.status!='partial' or self.receipt_set.dataset_id!=self.dataset_id
                    or self.scopes!=(self.receipt_set.aggregate_scope(),) or self.completed_through is not None):
                raise ValueError('complete receipt set differs from aggregate evidence')
            return self
        if any(item.scope=='actual_receipt_set' for item in self.scopes):
            raise ValueError('aggregate scope requires the complete original receipt set')
        if self.status=='unconfirmed' and (self.receipt_ids or self.scopes or self.completed_through):
            raise ValueError('unconfirmed collection cannot claim source receipts')
        if self.status!='unconfirmed' and not self.receipt_ids:
            raise ValueError('verified collection requires original receipt references')
        if any(item.dataset_id!=self.dataset_id for item in self.scopes):
            raise ValueError('collection scope mixes datasets')
        if self.completed_through is not None:
            raise ValueError('population coverage is not established by date or snapshot receipts')
        return self
