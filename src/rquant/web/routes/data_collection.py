"""Read collection coverage and original financial source permissions from Serving only."""
from __future__ import annotations
from typing import Annotated
from fastapi import APIRouter,Depends,HTTPException,Request,Response,Query
from rquant.backfill_execute_projection import FinancialSourceProjectionRow
from rquant.backfill_execute_contracts import FINANCIAL_EXECUTE_APIS
from rquant.data_collection_projection import CollectionDatasetProjectionRow,read_data_collection_projection_rows
from rquant.data_catalog.descriptions import DATASETS
from rquant.web.envelope import Envelope
from rquant.web.models.data_collection import DataCollectionData,CollectionDatasetView,FinancialSourcesData,FinancialSourceView
from rquant.web.models.data_audit_report import ReportOverviewRow
from rquant.web.routes.data_audit_report import _projection_state,_rows
from rquant.web.labels import FINANCIAL_SOURCE_LABELS
from rquant.web.security import current_user
from rquant.web.serving import serving_meta

router=APIRouter(prefix='/data')


@router.get('/collection',response_model=Envelope[DataCollectionData],summary='查看实际采集范围')
def get_collection(request: Request,response: Response,_viewer: Annotated[str | None,Depends(current_user)],
        generation: Annotated[str | None,Query(min_length=1,max_length=128)]=None) -> Envelope[DataCollectionData]:
    web=request.app.state.web
    with web.tracker.borrow() as borrowed:
        meta=serving_meta(borrowed,now=web.clock(),stale_after=web.settings.stale_after,failure=web.tracker.failure)
        if generation is not None and generation!=meta.generation_id:
            raise HTTPException(status_code=409,detail='数据已更新，请重新查看。')
        try:
            state,at=_projection_state(None if meta.state=='unavailable' else borrowed,('data_collection_dataset',),absent_state='not_published')
            data=DataCollectionData(status=state)
            if state=='ready':
                report_state,report_at=_projection_state(borrowed,('audit_report_overview',),absent_state='not_published')
                if report_state!='ready' or report_at!=at:
                    raise ValueError('collection and report source publication differ')
                overview=_rows(borrowed,'audit_report_overview',ReportOverviewRow,1)[0]
                if overview.schema_version!=3:
                    raise ValueError('collection source requires original verified report')
                rows=_rows(borrowed,'data_collection_dataset',CollectionDatasetProjectionRow,24)
                evidence=read_data_collection_projection_rows(tuple(row.model_dump() for row in rows),report_hash=overview.report_hash)
                data=DataCollectionData(status='ready',report_hash=overview.report_hash,datasets=tuple(CollectionDatasetView(
                    dataset_id=item.dataset_id,name=DATASETS[item.dataset_id].name,status=item.status,
                    status_label={'verified':'范围已核验','partial':'部分核验','unconfirmed':'尚未确认'}[item.status],
                    scopes=item.scopes,completed_through=item.completed_through) for item in evidence))
        except (ValueError,IndexError,KeyError) as error:
            raise HTTPException(status_code=503,detail='采集范围暂时无法读取，请稍后重试。') from error
    if meta.generation_id is not None:
        response.headers['X-Rquant-Generation']=meta.generation_id
    return Envelope[DataCollectionData](data=data,serving=meta)


@router.get('/financial-sources',response_model=Envelope[FinancialSourcesData],summary='查看财务接口权益和当前额度')
def get_financial_sources(request: Request,response: Response,_viewer: Annotated[str | None,Depends(current_user)],
        generation: Annotated[str | None,Query(min_length=1,max_length=128)]=None) -> Envelope[FinancialSourcesData]:
    web=request.app.state.web
    with web.tracker.borrow() as borrowed:
        meta=serving_meta(borrowed,now=web.clock(),stale_after=web.settings.stale_after,failure=web.tracker.failure)
        if generation is not None and generation!=meta.generation_id:
            raise HTTPException(status_code=409,detail='数据已更新，请重新查看。')
        try:
            state,_=_projection_state(None if meta.state=='unavailable' else borrowed,('data_center_financial_source',),absent_state='not_published')
            data=FinancialSourcesData(status=state,sources=tuple(FinancialSourceView(api_name=api,permission_status='unknown',
                name=FINANCIAL_SOURCE_LABELS[api],permission_label='未确认') for api in FINANCIAL_EXECUTE_APIS))
            if state=='ready':
                rows=_rows(borrowed,'data_center_financial_source',FinancialSourceProjectionRow,7)
                data=FinancialSourcesData(status='ready',sources=tuple(FinancialSourceView(**item.model_dump(),name=FINANCIAL_SOURCE_LABELS[item.api_name],
                    permission_label={'verified':'已核验','unknown':'未确认','unavailable':'不可用'}[item.permission_status]) for item in rows))
        except (ValueError,IndexError,KeyError) as error:
            raise HTTPException(status_code=503,detail='财务来源暂时无法读取，请稍后重试。') from error
    if meta.generation_id is not None:
        response.headers['X-Rquant-Generation']=meta.generation_id
    return Envelope[FinancialSourcesData](data=data,serving=meta)
