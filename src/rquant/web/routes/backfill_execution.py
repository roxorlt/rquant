"""Read original maintenance facts and submit commands through original PageControl."""
from __future__ import annotations
from typing import Annotated
import anyio.to_thread
from fastapi import APIRouter,Depends,HTTPException,Request,Response,Query
from pydantic import ValidationError
from rquant.backfill_execute_contracts import ExecutionConfirmation,MaintenanceExecutionStatus
from rquant.backfill_execute_projection import DATA_CENTER_EXECUTION_TABLES,ExecutionStateProjectionRow,ExecutionProjectionRow,ExecutionEventProjectionRow,read_execution_projection_rows
from rquant.page_control import parse_page_control_command
from rquant.web.backfill_plan_command_gateway import BackfillPlanWireReceipt,BackfillPlanCommandConflictError,BackfillPlanCommandInvalidReceiptError,BackfillPlanCommandUnavailableError
from rquant.web.models.backfill_execution import DataCenterCommandRequest,DataCenterCommandReceipt,ExecutionIndexData,ExecutionView,ExecutionEventView
from rquant.web.envelope import Envelope
from rquant.web.routes.data_audit_report import _projection_state,_rows
from rquant.web.security import current_user,require_csrf
from rquant.web.serving import serving_meta

router=APIRouter(prefix='/data/executions')
MAX_REQUEST_BYTES=4096


def _public_receipt(body: DataCenterCommandRequest,wire: BackfillPlanWireReceipt,*,owner: str) -> DataCenterCommandReceipt:
    if wire.status!='succeeded':
        return DataCenterCommandReceipt(command_id=body.command_id,status=wire.status,message={
            'pending':'已受理，等待处理','processing':'正在处理','failed':'请求未完成，请检查后重试。',
            'ambiguous':'状态待确认，请使用原请求重试。'}[wire.status])
    result=wire.result
    if not isinstance(result,dict):
        raise BackfillPlanCommandInvalidReceiptError('execution result is not a bounded object')
    try:
        if result.get('outcome')=='execution_prepared' and set(result)=={'outcome','confirmation'}:
            confirmation=ExecutionConfirmation.model_validate(result['confirmation'])
            if not body.kind.startswith('prepare_') or confirmation.prepare_command_id!=body.command_id:
                raise ValueError('prepare receipt identity changed')
            expected_kind='backfill' if body.kind=='prepare_backfill_execution' else 'financial'
            if confirmation.kind!=expected_kind:
                raise ValueError('prepared scope kind changed')
            if expected_kind=='backfill' and (confirmation.plan_task_id,confirmation.plan_hash)!=(body.plan_task_id,body.plan_hash):
                raise ValueError('prepared original plan changed')
            if expected_kind=='financial' and (confirmation.start_date,confirmation.end_date,confirmation.report_periods)!=(body.start_date,body.end_date,body.report_periods):
                raise ValueError('prepared financial dates changed')
            return DataCenterCommandReceipt(command_id=body.command_id,status='prepared',message='范围已确认，请检查后开始。',confirmation=confirmation)
        if result.get('outcome')=='execution_queued' and set(result)=={'outcome','execution_id','manifest_id'}:
            if not body.kind.startswith('execute_') or result['execution_id']!=body.execution_id:
                raise ValueError('execution receipt identity changed')
            return DataCenterCommandReceipt(command_id=body.command_id,status='queued',message='已排队，等待运行',execution_id=body.execution_id)
        if result.get('outcome')=='control_accepted' and set(result)=={'outcome','execution'}:
            original=MaintenanceExecutionStatus.model_validate(result['execution'])
            if body.kind not in {'pause_data_center_execution','resume_data_center_execution'} or original.owner!=owner or original.execution_id!=body.execution_id or original.control_sequence!=body.expected_sequence+1:
                raise ValueError('control receipt identity changed')
            return DataCenterCommandReceipt(command_id=body.command_id,status='control_accepted',message='操作已受理',execution_id=body.execution_id,execution=ExecutionView.from_original(original))
    except (ValueError,ValidationError,AttributeError) as error:
        raise BackfillPlanCommandInvalidReceiptError('execution receipt is invalid') from error
    raise BackfillPlanCommandInvalidReceiptError('execution outcome is invalid')


@router.post('/commands',response_model=DataCenterCommandReceipt,summary='确认、开始、暂停或继续数据任务')
async def submit_data_center_command(request: Request,body: DataCenterCommandRequest,
        viewer: Annotated[str | None,Depends(current_user)],_csrf: Annotated[None,Depends(require_csrf)]) -> DataCenterCommandReceipt:
    if viewer is None:
        raise HTTPException(status_code=401,detail='请先登录。')
    if len(await request.body())>MAX_REQUEST_BYTES:
        raise HTTPException(status_code=413,detail='请求内容过长，请重试。')
    payload={**body.model_dump(mode='json'),'actor_id':viewer}
    try:
        parse_page_control_command(payload)
    except ValueError as error:
        raise HTTPException(status_code=422,detail='范围或操作无效，请检查后重试。') from error
    try:
        wire=await anyio.to_thread.run_sync(request.app.state.web.backfill_plan_commands.submit,payload)
        return _public_receipt(body,wire,owner=viewer)
    except BackfillPlanCommandConflictError as error:
        raise HTTPException(status_code=409,detail='请求内容与已有记录不同，请保留原请求。') from error
    except BackfillPlanCommandUnavailableError as error:
        raise HTTPException(status_code=503,detail='提交状态待确认，请使用原请求重试。') from error
    except BackfillPlanCommandInvalidReceiptError as error:
        raise HTTPException(status_code=502,detail='回执无法核对，请使用原请求重试。') from error


@router.get('',response_model=Envelope[ExecutionIndexData],summary='查看数据任务进度')
def get_data_center_executions(request: Request,response: Response,viewer: Annotated[str | None,Depends(current_user)],
        generation: Annotated[str | None,Query(min_length=1,max_length=128)]=None) -> Envelope[ExecutionIndexData]:
    web=request.app.state.web
    with web.tracker.borrow() as borrowed:
        meta=serving_meta(borrowed,now=web.clock(),stale_after=web.settings.stale_after,failure=web.tracker.failure)
        if generation is not None and generation!=meta.generation_id:
            raise HTTPException(status_code=409,detail='数据已更新，请重新查看。')
        try:
            state,_=_projection_state(None if meta.state=='unavailable' else borrowed,tuple(sorted(DATA_CENTER_EXECUTION_TABLES)),absent_state='not_published')
            data=ExecutionIndexData(status=state)
            if state=='ready':
                flags=_rows(borrowed,'data_center_execution_state',ExecutionStateProjectionRow,1)[0]
                rows=_rows(borrowed,'data_center_execution',ExecutionProjectionRow,borrowed.manifest.row_counts['data_center_execution'])
                originals=read_execution_projection_rows(tuple(row.model_dump(mode='json') for row in rows))
                owned=tuple(ExecutionView.from_original(item) for item in sorted(originals,key=lambda value:value.updated_at,reverse=True) if item.owner==viewer)
                events=_rows(borrowed,'data_center_execution_event',ExecutionEventProjectionRow,borrowed.manifest.row_counts['data_center_execution_event'])
                owned_ids={item.execution_id for item in owned}
                public_events=tuple(ExecutionEventView.from_original(item) for item in sorted(events,key=lambda value:(value.occurred_at,value.event_id))
                    if item.owner==viewer and item.execution_id in owned_ids)
                data=ExecutionIndexData(status='ready',**flags.model_dump(exclude={'observed_at'}),executions=owned,events=public_events)
        except (ValueError,IndexError,KeyError) as error:
            raise HTTPException(status_code=503,detail='任务状态暂时无法读取，请稍后重试。') from error
    if meta.generation_id is not None:
        response.headers['X-Rquant-Generation']=meta.generation_id
    return Envelope[ExecutionIndexData](data=data,serving=meta)
