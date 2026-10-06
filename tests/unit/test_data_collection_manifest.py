from __future__ import annotations

import hashlib
import os
from datetime import UTC,date,datetime
from pathlib import Path

import pytest

from rquant.data_collection_contracts import (
    CollectionManifestEntry,CollectionReceiptReference,CollectionReceiptSet,DatasetCollectionEvidence,
    MAX_RECEIPT_MANIFEST_BYTES,
)
from rquant.data_collection_manifest import (
    build_receipt_manifest,canonical_bytes,iter_receipt_manifest,publish_receipt_manifest,preflight_manifest_capacity,
)
from rquant.daily_canonical_publisher import CanonicalDatabaseIdentity
from rquant.runtime_contracts import canonical_sha256


def _codec(tmp_path: Path,count: int=17):
    entries=tuple(CollectionManifestEntry(index=i,task_id=f'financial-raw-{i:04d}',task_sha256=canonical_sha256(('task',i)),
        kind='financial',task_receipt_id=canonical_sha256(('original',i)),
        reference=CollectionReceiptReference(kind='financial',receipt_id=canonical_sha256(('original',i)),dataset_ids=('financial_observation',)),
        request_count=1,requests_sha256=canonical_sha256(('request',i)),dispatch_count=1,dispatches_sha256=canonical_sha256(('dispatch',i)),
        version_count=0,versions_sha256=canonical_sha256(()),observed_start=datetime(2026,10,6,10,tzinfo=UTC),
        observed_end=datetime(2026,10,6,10,tzinfo=UTC)) for i in range(count))
    domain=hashlib.sha256()
    for entry in entries:
        domain.update(canonical_sha256((entry.task_id,entry.task_receipt_id)).encode())
    summary=CollectionReceiptSet(dataset_id='financial_observation',receipt_count=count,scope_count=count,
        receipt_ids_sha256='a'*64,scopes_sha256='b'*64,first_date=date(2026,10,6),last_date=date(2026,10,6),row_count=count)
    root,pages=build_receipt_manifest(entries,datasets=(summary,),execution_id='a'*64,owner='synthetic-codec-owner',
        manifest_id='b'*64,plan_sha256='c'*64,primary_identity=CanonicalDatabaseIdentity(canonical_path=str(tmp_path/'primary.duckdb'),device=1,inode=2),
        policy_generation='d'*64,source_account_sha256='e'*64,source_config_sha256='f'*64,quota_source='offline.codec',
        quota_ledger_device=1,quota_ledger_inode=3,audit_start=date(2026,10,5),observed_through=date(2026,10,5),domain_sha256=domain.hexdigest())
    return entries,root,pages


def test_complete_manifest_keeps_seventeen_original_members_and_exact_recovery_bytes(tmp_path: Path) -> None:
    entries,root,pages=_codec(tmp_path)
    directory=tmp_path/'collection'
    directory.mkdir(mode=0o700)
    publish_receipt_manifest(directory,root,pages)
    assert tuple(iter_receipt_manifest(directory,root))==entries
    assert len(root.pages)==2 and root.entry_count==17 and root.request_count==root.dispatch_count==17
    before={path:hashlib.sha256(path.read_bytes()).hexdigest() for path in (directory/f'receipts-{root.content_sha256}').iterdir()}
    publish_receipt_manifest(directory,root,pages)
    assert before=={path:hashlib.sha256(path.read_bytes()).hexdigest() for path in before}
    assert root.observed_start.date()>root.observed_through


@pytest.mark.parametrize('case',['missing','extra','content','symlink','root','wrong_owner','stop','moving'])
def test_manifest_rejects_missing_extra_tampered_identity_or_interrupted_artifacts(tmp_path: Path,case: str,monkeypatch) -> None:
    _,root,pages=_codec(tmp_path)
    directory=tmp_path/'collection'
    directory.mkdir(mode=0o700)
    publish_receipt_manifest(directory,root,pages)
    target=directory/f'receipts-{root.content_sha256}'
    page=target/root.pages[0].relative_name
    if case=='missing':
        page.unlink()
    elif case=='extra':
        (target/'extra.json').write_text('{}')
    elif case=='content':
        page.chmod(0o600)
        data=page.read_bytes()
        page.write_bytes(data.replace(b'financial_observation',b'financial_observatioX'))
    elif case=='symlink':
        copied=directory/'other.json'
        copied.write_bytes(page.read_bytes())
        page.unlink()
        page.symlink_to(copied)
    elif case=='root':
        (target/'root.json').chmod(0o600)
        (target/'root.json').write_text('{}')
    elif case=='wrong_owner':
        root=type(root).model_validate({**root.model_dump(mode='python'),'owner':'other','content_sha256':None})
    elif case=='moving':
        import rquant.data_collection_manifest as module
        read=module._read
        def move(path,max_bytes):
            result=read(path,max_bytes)
            if path==page:
                temporary=target/'moved.json'
                temporary.write_bytes(path.read_bytes())
                os.replace(temporary,path)
            return result
        monkeypatch.setattr(module,'_read',move)
    with pytest.raises((ValueError,OSError,InterruptedError)):
        tuple(iter_receipt_manifest(directory,root,stop_requested=lambda:case=='stop'))


def test_legal_original_task_bound_fits_finite_manifest_without_increasing_legacy_caps(tmp_path: Path) -> None:
    entries,root,pages=_codec(tmp_path,4095)
    preflight_manifest_capacity(4095)
    assert len(root.pages)==256
    assert len(canonical_bytes(root))+sum(page.byte_count for page in root.pages)<MAX_RECEIPT_MANIFEST_BYTES
    for value in (0,4096):
        with pytest.raises(ValueError,match='capacity'):
            preflight_manifest_capacity(value)
    summary=root.datasets[0]
    aggregate=DatasetCollectionEvidence(dataset_id=summary.dataset_id,status='partial',receipt_ids=(),
        scopes=(summary.aggregate_scope(),),receipt_set=summary)
    assert aggregate.receipt_ids==() and aggregate.completed_through is None
    with pytest.raises(ValueError):
        DatasetCollectionEvidence(dataset_id=summary.dataset_id,status='partial',receipt_ids=tuple(entry.task_receipt_id for entry in entries[:17]),scopes=())
    with pytest.raises(ValueError):
        DatasetCollectionEvidence(dataset_id=summary.dataset_id,status='partial',receipt_ids=(),scopes=(summary.aggregate_scope(),))


def test_manifest_rejects_duplicate_source_or_disordered_members_before_publication(tmp_path: Path) -> None:
    entries,root,_=_codec(tmp_path)
    for changed in (entries[::-1],entries[:1]+(entries[0].model_copy(update={'index':1}),)+entries[2:]):
        with pytest.raises(ValueError,match='membership|order'):
            build_receipt_manifest(changed,datasets=root.datasets,**root.model_dump(mode='python',include={
                'execution_id','owner','manifest_id','plan_sha256','primary_identity','policy_generation','source_account_sha256',
                'source_config_sha256','quota_source','quota_ledger_device','quota_ledger_inode','audit_start','observed_through','domain_sha256'}))
