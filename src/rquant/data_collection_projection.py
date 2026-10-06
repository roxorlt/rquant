"""Original v3 collection scopes published beside the unchanged catalog statistics."""
from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING
from pydantic import Field
from rquant.data_collection_contracts import DatasetCollectionEvidence
from rquant.runtime_contracts import RuntimeContractModel
from rquant.serving_read_models import ServingProjectionPayload

DATA_COLLECTION_PROJECTION_TABLES=frozenset({'data_collection_dataset'})

if TYPE_CHECKING:
    from rquant.data_audit_report import DataAuditReport


class CollectionDatasetProjectionRow(RuntimeContractModel):
    report_hash: str = Field(pattern=r'^[0-9a-f]{64}$')
    dataset_id: str
    source_binding_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    evidence_json: str = Field(min_length=1,max_length=8192)


def project_data_collection(report: DataAuditReport,*,available_at: datetime) -> tuple[ServingProjectionPayload,...]:
    from rquant.data_audit_report import CollectionDataAuditReport,validate_data_audit_report
    report=validate_data_audit_report(report)
    if not isinstance(report,CollectionDataAuditReport):
        return ()
    return (ServingProjectionPayload(table_name='data_collection_dataset',available_at=available_at,rows=tuple(
        CollectionDatasetProjectionRow(report_hash=report.content_hash,dataset_id=evidence.dataset_id,
            source_binding_sha256=report.collection_reference.binding_sha256,evidence_json=evidence.model_dump_json()).model_dump(mode='json')
        for evidence in report.collection_datasets)),)


def read_data_collection_projection_rows(rows: tuple[dict[str,object],...],*,report_hash: str) -> tuple[DatasetCollectionEvidence,...]:
    from rquant.data_catalog.build import CATALOG_CONTRACTS
    if len(rows)!=24:
        raise ValueError('collection projection must carry all original catalog datasets')
    validated=tuple(CollectionDatasetProjectionRow.model_validate(row) for row in rows)
    if any(row.report_hash!=report_hash for row in validated) or len({row.source_binding_sha256 for row in validated})!=1:
        raise ValueError('collection projection mixes report or source bindings')
    evidence=tuple(DatasetCollectionEvidence.model_validate_json(row.evidence_json) for row in validated)
    if tuple(row.dataset_id for row in validated)!=tuple(sorted(item.dataset_id for item in CATALOG_CONTRACTS)):
        raise ValueError('collection projection duplicates or omits catalog datasets')
    if any(row.dataset_id!=item.dataset_id or row.evidence_json!=item.model_dump_json() for row,item in zip(validated,evidence,strict=True)):
        raise ValueError('collection projection is not canonical original evidence')
    return evidence
