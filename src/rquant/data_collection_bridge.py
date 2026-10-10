"""Admit one sealed source into the existing audit queue and cursor transaction."""
from __future__ import annotations

from contextlib import closing
from datetime import UTC,datetime,timedelta
from collections.abc import Callable
from pathlib import Path

from rquant.data_audit_evidence import DailyBarNullFieldSpec
from rquant.data_audit_report import capture_data_audit_replica_identity
from rquant.data_audit_report_jobs import DataAuditReportJobRequest, DataAuditReportJobReceipt, DataAuditReportJobStore
from rquant.data_collection_authority import load_collection_proof, capture_verified_collection_identity
from rquant.data_collection_contracts import AuditCollectionReference


class DataCollectionBridge:
    def __init__(self, store: DataAuditReportJobStore, *,
                 null_fields: tuple[DailyBarNullFieldSpec,...],stop_requested: Callable[[],bool] | None = None,
                 hash_timeout_seconds: int = 600) -> None:
        if store.collection_directory is None:
            raise ValueError('collection bridge is not configured')
        self.store=store
        self.null_fields=null_fields
        if not 1<=hash_timeout_seconds<=1800:
            raise ValueError('collection scan deadline exceeds capacity')
        self.hash_timeout_seconds=hash_timeout_seconds
        self.stop_requested=stop_requested

    def lookup(self, reference: AuditCollectionReference) -> DataAuditReportJobReceipt | None:
        existing=self.store.admission_by_key(reference.event_id)
        if existing is None:
            return None
        request,task_id=existing
        if request.collection_reference!=reference:
            raise ValueError('same collection event has conflicting admission binding')
        return self.store.status(task_id)

    def run_one(self) -> DataAuditReportJobReceipt | None:
        with closing(self.store._connect()) as connection:
            row=connection.execute('SELECT reference_json FROM data_collection_source WHERE task_id IS NULL '
                'ORDER BY sequence LIMIT 1').fetchone()
        if row is None:
            return None
        reference=AuditCollectionReference.model_validate_json(row[0])
        # A lost response is resolved before source lookup, even if a newer replica replaced the live name.
        existing=self.lookup(reference)
        if existing is not None:
            with self.store._transaction() as connection:
                self._ack(connection,reference,existing.task_id)
            return existing
        proof=load_collection_proof(self.store.collection_directory,reference)
        verified=capture_verified_collection_identity(proof,reference,
            deadline=datetime.now(UTC)+timedelta(seconds=self.hash_timeout_seconds),stop_requested=self.stop_requested)
        request=DataAuditReportJobRequest(idempotency_key=reference.event_id,
            primary_path=Path(proof.primary_identity.canonical_path),replica_path=proof.fixed_replica_path,
            replica_file_identity=verified.identity,audit_start=proof.audit_start,observed_through=proof.observed_through,
            null_fields=self.null_fields,collection_reference=reference)
        with self.store._transaction() as connection:
            task_id=self.store._submit_on(connection,request,verified_collection_file=verified)
            self._ack(connection,reference,task_id)
        return self.store.status(task_id)

    @staticmethod
    def _ack(connection: object, reference: AuditCollectionReference, task_id: str) -> None:
        current=connection.execute('SELECT sequence,event_id FROM data_collection_cursor WHERE singleton=1').fetchone()
        if current is not None and current[0]>reference.sequence:
            raise ValueError('collection source cursor cannot move backwards')
        updated=connection.execute('UPDATE data_collection_source SET task_id=? WHERE event_id=? '
            'AND (task_id IS NULL OR task_id=?)',[task_id,reference.event_id,task_id]).rowcount
        if updated!=1:
            raise ValueError('collection source was not registered or changed task')
        connection.execute('INSERT INTO data_collection_cursor VALUES(1,?,?,?) ON CONFLICT(singleton) '
            'DO UPDATE SET sequence=excluded.sequence,event_id=excluded.event_id,task_id=excluded.task_id',
            [reference.sequence,reference.event_id,task_id])
