"""Finite immutable receipt pages for the original collection worker, without a new queue."""
from __future__ import annotations

import hashlib
import json
import os
import stat
import tempfile
from collections.abc import Callable, Iterator
from datetime import UTC, date, datetime
from pathlib import Path

from rquant.data_collection_contracts import (
    CollectionManifestEntry, CollectionManifestPage, CollectionManifestPageReference,
    CollectionReceiptManifest, CollectionReceiptReference, CollectionReceiptSet, DatasetCollectionClaim,
    MAX_COLLECTION_PROOF_BYTES, MAX_RECEIPT_ENTRY_BYTES, MAX_RECEIPT_MANIFEST_BYTES,
    MAX_RECEIPT_MANIFEST_ENTRIES,
)
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256


def canonical_bytes(value: RuntimeContractModel) -> bytes:
    return json.dumps(value.model_dump(mode='json'),ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).encode()


def preflight_manifest_capacity(task_count: int) -> None:
    # Original controlled IDs and typed entry fields have a fixed serialized upper bound.
    # Page/root overhead is reserved before any SDK dispatch, even for the largest legal manifest.
    if not 1<=task_count<=MAX_RECEIPT_MANIFEST_ENTRIES or task_count*MAX_RECEIPT_ENTRY_BYTES+2*MAX_COLLECTION_PROOF_BYTES>MAX_RECEIPT_MANIFEST_BYTES:
        raise ValueError('complete receipt manifest exceeds original task/material capacity')


def receipt_entry(receipt: RuntimeContractModel,*,index: int,task_sha256: str,kind: str,
        reference: CollectionReceiptReference | None) -> CollectionManifestEntry:
    requests=getattr(receipt,'source_requests',())
    dispatches=getattr(receipt,'dispatch_receipts',())
    versions=getattr(receipt,'original_versions',())
    observed=tuple(item.observed_at for item in getattr(receipt,'original_receipts',()))
    return CollectionManifestEntry(index=index,task_id=getattr(receipt,'task_id','tail-derived'),task_sha256=task_sha256,kind=kind,
        task_receipt_id=receipt.receipt_id,reference=reference,request_count=len(requests),requests_sha256=canonical_sha256(requests),
        dispatch_count=len(dispatches),dispatches_sha256=canonical_sha256(dispatches),version_count=len(versions),
        versions_sha256=canonical_sha256(versions),observed_start=min(observed) if observed else None,observed_end=max(observed) if observed else None)


class ReceiptSetAccumulator:
    """One rolling hash/count per actual dataset, rather than keeping all SDK rows/scopes."""
    def __init__(self) -> None:
        self.values: dict[str,dict[str,object]]={}

    def add(self,reference: CollectionReceiptReference,claims: tuple[DatasetCollectionClaim,...]) -> None:
        seen: set[str]=set()
        for claim in claims:
            value=self.values.setdefault(claim.dataset_id,{'receipts':hashlib.sha256(),'scopes':hashlib.sha256(),
                'receipt_count':0,'scope_count':0,'row_count':0,'first_date':claim.trade_date,'last_date':claim.trade_date})
            if claim.dataset_id not in seen:
                value['receipts'].update(reference.receipt_id.encode())
                value['receipt_count']+=1
                seen.add(claim.dataset_id)
            value['scopes'].update(canonical_sha256(claim).encode())
            value['scope_count']+=1
            value['row_count']+=claim.row_count
            value['first_date']=min(value['first_date'],claim.trade_date)
            value['last_date']=max(value['last_date'],claim.trade_date)
        if len(self.values)>26:
            raise ValueError('complete source has undeclared dataset capacity')

    def summaries(self) -> tuple[CollectionReceiptSet,...]:
        return tuple(CollectionReceiptSet(dataset_id=key,receipt_count=value['receipt_count'],scope_count=value['scope_count'],
            receipt_ids_sha256=value['receipts'].hexdigest(),scopes_sha256=value['scopes'].hexdigest(),
            first_date=value['first_date'],last_date=value['last_date'],row_count=value['row_count']) for key,value in sorted(self.values.items()))


def _hash_entries(entries: tuple[CollectionManifestEntry,...],field: str | None=None) -> str:
    digest=hashlib.sha256()
    for entry in entries:
        digest.update((canonical_sha256(entry) if field is None else getattr(entry,field)).encode())
    return digest.hexdigest()


def build_receipt_manifest(entries: tuple[CollectionManifestEntry,...],*,datasets: tuple[CollectionReceiptSet,...],
        **identity: object) -> tuple[CollectionReceiptManifest,tuple[CollectionManifestPage,...]]:
    preflight_manifest_capacity(len(entries))
    if tuple(item.index for item in entries)!=tuple(range(len(entries))) or len({item.task_id for item in entries})!=len(entries):
        raise ValueError('complete manifest task membership or order changed')
    refs=tuple(item.reference.receipt_id for item in entries if item.reference is not None)
    if len(refs)!=len(set(refs)) or len({item.task_receipt_id for item in entries})!=len(entries):
        raise ValueError('complete manifest original receipt membership is duplicated')
    pages=tuple(CollectionManifestPage(index=index//16,entries=entries[index:index+16]) for index in range(0,len(entries),16))
    page_refs=[]
    for page in pages:
        payload=canonical_bytes(page)
        digest=hashlib.sha256(payload).hexdigest()
        page_refs.append(CollectionManifestPageReference(index=page.index,relative_name=f'page-{page.index:03d}-{digest}.json',
            sha256=digest,byte_count=len(payload),entry_count=len(page.entries)))
    observations=tuple(item for entry in entries for item in (entry.observed_start,entry.observed_end) if item is not None)
    root=CollectionReceiptManifest(**identity,entry_count=len(entries),entries_sha256=_hash_entries(entries),
        request_count=sum(item.request_count for item in entries),requests_sha256=_hash_entries(entries,'requests_sha256'),
        dispatch_count=sum(item.dispatch_count for item in entries),dispatches_sha256=_hash_entries(entries,'dispatches_sha256'),
        observed_start=min(observations) if observations else None,observed_end=max(observations) if observations else None,
        pages=tuple(page_refs),datasets=datasets)
    if len(canonical_bytes(root))+sum(page.byte_count for page in root.pages)>MAX_RECEIPT_MANIFEST_BYTES:
        raise ValueError('complete manifest canonical material exceeds byte capacity')
    return root,pages


def _identity(info: os.stat_result) -> tuple[int,...]:
    return info.st_dev,info.st_ino,info.st_size,info.st_mtime_ns,info.st_ctime_ns


def _read(path: Path,max_bytes: int) -> tuple[bytes,tuple[int,...]]:
    fd=os.open(path,os.O_RDONLY|os.O_NONBLOCK|os.O_NOFOLLOW)
    with os.fdopen(fd,'rb') as handle:
        before=os.fstat(handle.fileno())
        if not stat.S_ISREG(before.st_mode) or not 0<before.st_size<=max_bytes:
            raise ValueError('manifest artifact is not a bounded regular file')
        payload=handle.read(max_bytes+1)
        after=os.fstat(handle.fileno())
        if len(payload)!=before.st_size or _identity(before)!=_identity(after) or _identity(after)!=_identity(path.stat(follow_symlinks=False)):
            raise ValueError('manifest artifact changed while reading')
        return payload,_identity(after)


def _sync_directory(path: Path) -> None:
    fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _write_once(path: Path,payload: bytes) -> None:
    descriptor,name=tempfile.mkstemp(prefix='.manifest-',dir=path.parent)
    temporary=Path(name)
    try:
        with os.fdopen(descriptor,'wb') as handle:
            handle.write(payload)
            handle.flush()
            os.fchmod(handle.fileno(),0o400)
            os.fsync(handle.fileno())
        try:
            os.link(temporary,path,follow_symlinks=False)
        except FileExistsError:
            if _read(path,len(payload))[0]!=payload:
                raise ValueError('same manifest artifact identity has different content')
    finally:
        temporary.unlink(missing_ok=True)
    _sync_directory(path.parent)


def receipt_manifest_directory(directory: Path,root: CollectionReceiptManifest) -> Path:
    if directory.is_symlink() or directory.resolve(strict=True)!=directory:
        raise ValueError('manifest collection root is not canonical')
    return directory/f'receipts-{root.content_sha256}'


def validate_receipt_manifest_pages(root: CollectionReceiptManifest,pages: tuple[CollectionManifestPage,...]) -> None:
    root=CollectionReceiptManifest.model_validate_json(root.model_dump_json())
    if len(pages)!=len(root.pages):
        raise ValueError('manifest publication has missing or extra pages')
    for reference,page in zip(root.pages,pages,strict=True):
        payload=canonical_bytes(page)
        if page.index!=reference.index or len(payload)!=reference.byte_count or hashlib.sha256(payload).hexdigest()!=reference.sha256:
            raise ValueError('manifest page publication binding changed')


def publish_receipt_manifest(directory: Path,root: CollectionReceiptManifest,pages: tuple[CollectionManifestPage,...],*,
        deadline: datetime | None=None,stop_requested: Callable[[],bool] | None=None) -> None:
    validate_receipt_manifest_pages(root,pages)
    if (deadline is not None and datetime.now(UTC)>=deadline) or (stop_requested is not None and stop_requested()):
        raise InterruptedError('complete manifest publication stopped')
    target=receipt_manifest_directory(directory,root)
    if not target.exists():
        target.mkdir(mode=0o700)
        _sync_directory(directory)
    if target.is_symlink() or target.resolve(strict=True)!=target or not target.is_dir():
        raise ValueError('manifest destination is not an owned canonical directory')
    for reference,page in zip(root.pages,pages,strict=True):
        if (deadline is not None and datetime.now(UTC)>=deadline) or (stop_requested is not None and stop_requested()):
            raise InterruptedError('complete manifest publication stopped')
        _write_once(target/reference.relative_name,canonical_bytes(page))
    _write_once(target/'root.json',canonical_bytes(root))
    for _ in iter_receipt_manifest(directory,root,deadline=deadline,stop_requested=stop_requested):
        pass


def remove_owned_receipt_manifest(directory: Path,root: CollectionReceiptManifest,pages: tuple[CollectionManifestPage,...],*,
        device: int,inode: int) -> None:
    target=receipt_manifest_directory(directory,root)
    before=target.stat(follow_symlinks=False)
    if target.is_symlink() or not stat.S_ISDIR(before.st_mode) or (before.st_dev,before.st_ino)!=(device,inode):
        raise ValueError('failed manifest directory ownership is unknown')
    expected={'root.json':root,**{reference.relative_name:page
        for reference,page in zip(root.pages,pages,strict=True)}}
    found=[]
    with os.scandir(target) as files:
        for item in files:
            if len(found)>=257 or item.name not in expected:
                raise ValueError('failed manifest contains unknown material')
            path=target/item.name
            expected_bytes=canonical_bytes(expected[item.name])
            payload,physical=_read(path,len(expected_bytes))
            if payload!=expected_bytes:
                raise ValueError('failed manifest content ownership is unknown')
            found.append((path,physical))
    if _identity(target.stat(follow_symlinks=False))!=_identity(before):
        raise ValueError('failed manifest directory changed during reconciliation')
    for path,physical in found:
        if _identity(path.stat(follow_symlinks=False))!=physical or (
                target.stat(follow_symlinks=False).st_dev,target.stat(follow_symlinks=False).st_ino)!=(device,inode):
            raise ValueError('failed manifest material moved before reconciliation')
        path.unlink()
    target.rmdir()
    _sync_directory(directory)


def iter_receipt_manifest(directory: Path,root: CollectionReceiptManifest,*,deadline: datetime | None=None,
        stop_requested: Callable[[],bool] | None=None) -> Iterator[CollectionManifestEntry]:
    target=receipt_manifest_directory(directory,root)
    if target.is_symlink() or target.resolve(strict=True)!=target:
        raise ValueError('manifest artifact directory changed')
    before=_identity(target.stat(follow_symlinks=False))
    expected={'root.json',*(page.relative_name for page in root.pages)}
    names=set()
    with os.scandir(target) as files:
        for item in files:
            if len(names)>=257 or item.name not in expected or item.name in names:
                raise ValueError('manifest has extra or duplicate artifacts')
            names.add(item.name)
    if names!=expected:
        raise ValueError('manifest has missing artifacts')
    payload,root_identity=_read(target/'root.json',MAX_COLLECTION_PROOF_BYTES)
    if payload!=canonical_bytes(root):
        raise ValueError('manifest root content changed')
    physical=[(target/'root.json',root_identity)]
    entries_digest,domain_digest,requests_digest,dispatches_digest=(hashlib.sha256() for _ in range(4))
    task_ids,receipt_ids,source_ids=set(),set(),set()
    count=requests=dispatches=0
    observations=[]
    for reference in root.pages:
        if (deadline is not None and datetime.now(UTC)>=deadline) or (stop_requested is not None and stop_requested()):
            raise InterruptedError('complete manifest scan stopped')
        path=target/reference.relative_name
        data,identity=_read(path,MAX_COLLECTION_PROOF_BYTES)
        if len(data)!=reference.byte_count or hashlib.sha256(data).hexdigest()!=reference.sha256:
            raise ValueError('manifest page content changed')
        page=CollectionManifestPage.model_validate_json(data)
        if canonical_bytes(page)!=data or page.index!=reference.index or len(page.entries)!=reference.entry_count:
            raise ValueError('manifest page canonical order/count changed')
        physical.append((path,identity))
        for entry in page.entries:
            if entry.index!=count or entry.task_id in task_ids or entry.task_receipt_id in receipt_ids or (
                    entry.reference is not None and entry.reference.receipt_id in source_ids):
                raise ValueError('manifest original membership or order changed')
            task_ids.add(entry.task_id)
            receipt_ids.add(entry.task_receipt_id)
            if entry.reference is not None:
                source_ids.add(entry.reference.receipt_id)
            entries_digest.update(canonical_sha256(entry).encode())
            domain_digest.update(canonical_sha256((entry.task_id,entry.task_receipt_id)).encode())
            requests_digest.update(entry.requests_sha256.encode())
            dispatches_digest.update(entry.dispatches_sha256.encode())
            requests+=entry.request_count
            dispatches+=entry.dispatch_count
            count+=1
            observations.extend(item for item in (entry.observed_start,entry.observed_end) if item is not None)
            yield entry
    if (count,entries_digest.hexdigest(),domain_digest.hexdigest(),requests,requests_digest.hexdigest(),dispatches,
            dispatches_digest.hexdigest(),min(observations) if observations else None,max(observations) if observations else None)!=(
            root.entry_count,root.entries_sha256,root.domain_sha256,root.request_count,root.requests_sha256,root.dispatch_count,
            root.dispatches_sha256,root.observed_start,root.observed_end):
        raise ValueError('manifest full task/request/dispatch/observation set changed')
    if _identity(target.stat(follow_symlinks=False))!=before or any(_identity(path.stat(follow_symlinks=False))!=identity for path,identity in physical):
        raise ValueError('manifest material moved or changed during complete verification')


def verify_original_entry(store: object,root: CollectionReceiptManifest,entry: CollectionManifestEntry) -> RuntimeContractModel:
    if entry.kind=='financial':
        from rquant.financial_runtime import _load_runtime_receipt
        receipt=_load_runtime_receipt(store,receipt_id=entry.task_receipt_id)
    else:
        from rquant.backfill_execute_contracts import BackfillDayCommitReceipt
        from rquant.data_center_maintenance_runtime import DerivedTailCompletionReceipt
        row=store._conn.execute('SELECT receipt_id,payload_json FROM backfill_day_commit_receipt WHERE execution_id=? AND task_id=?',
            [root.execution_id,entry.task_id]).fetchone()
        if row is None or len(row[1].encode())>MAX_COLLECTION_PROOF_BYTES:
            raise ValueError('manifest original day/tail receipt is unavailable or oversized')
        receipt=(DerivedTailCompletionReceipt if entry.kind=='derived_tail' else BackfillDayCommitReceipt).model_validate_json(row[1])
        if receipt.receipt_id!=row[0]:
            raise ValueError('manifest original day/tail receipt bytes changed')
    primary=getattr(receipt,'primary_identity',None)
    if entry.kind=='derived_tail' and receipt is not None:
        if any(batch.primary_identity!=root.primary_identity for batch in receipt.batches):
            raise ValueError('manifest derived batch physical source changed')
        primary=root.primary_identity
    if receipt is None or (receipt.execution_id,receipt.owner,receipt.manifest_id,receipt.plan_sha256,getattr(receipt,'task_id','tail-derived'),primary)!=(
            root.execution_id,root.owner,root.manifest_id,root.plan_sha256,entry.task_id,root.primary_identity):
        raise ValueError('manifest receipt owner/execution/plan/physical source changed')
    if receipt_entry(receipt,index=entry.index,task_sha256=entry.task_sha256,kind=entry.kind,reference=entry.reference)!=entry:
        raise ValueError('manifest original SDK request/dispatch/version/observation set changed')
    for request in getattr(receipt,'source_requests',()):
        if (request.owner,request.execution_id,request.manifest_id,request.plan_sha256,request.source_account_sha256,
                request.quota_source,request.quota_ledger_device,request.quota_ledger_inode)!=(
                root.owner,root.execution_id,root.manifest_id,root.plan_sha256,root.source_account_sha256,
                root.quota_source,root.quota_ledger_device,root.quota_ledger_inode):
            raise ValueError('manifest original SDK account/configuration or immutable request changed')
    return receipt
