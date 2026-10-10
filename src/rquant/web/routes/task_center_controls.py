"""Six opt-in private operations, using the same original UUID journal."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Body, Depends, HTTPException, Path, Query, Request, Response
from pydantic import Field
from starlette.concurrency import run_in_threadpool

from rquant.lab_scheduling_control import LabSchedulingControlState
from rquant.page_control import PageControlCommandConflictError, PageControlStatus
from rquant.task_control_admission import (TaskControlAdmissionResult, TaskControlAdmissionRejectedError, TaskControlAdmissionUnavailableError,
    TaskControlAdmissionNotFoundError, read_task_center_view)
from rquant.task_control_commands import (PrepareUnitRun, RequestUnitRun, SetLabSchedulingPaused, TaskControlRequest,
    PrepareNotifierDeliveryMode, SetNotifierDeliveryMode, SetMonitorBuiltinEnabled)
from rquant.serving_publisher import ServingReader
from rquant.strict_json import strict_json_loads
from rquant.web.models.task_controls import (TaskControlCapabilitiesData, TaskControlCommandData, TaskUnitControlChoice, TaskSchedulingView,
    NotifierModeControlView, MonitorBuiltinControlView)
from rquant.web.security import current_user, require_csrf

router = APIRouter(prefix="/tasks")
_Viewer = Annotated[str | None, Depends(current_user)]
_CSRF = Annotated[None, Depends(require_csrf)]
_Unit = Annotated[str, Path(min_length=1, max_length=128)]
_Command = Annotated[TaskControlRequest, Body(discriminator="kind")]
MAX_TASK_COMMAND_BYTES = 4096
MAX_SCHEDULING_COMMAND_BYTES = 1024
_REASONS = {"available": "执行前会再次核验。", "disabled": "任务操作尚未开放。", "busy": "任务正在运行。", "window_closed": "盘中仅可运行只读任务。", "source_unavailable": "任务状态暂无法核验。", "cooldown": "测试频繁，请稍后重试。"}


def scheduling_view(state: LabSchedulingControlState | None) -> TaskSchedulingView:
    if state is None:
        return TaskSchedulingView()
    pending = state.desired_version != state.applied_version
    return TaskSchedulingView(available=True, desired_version=state.desired_version, applied_version=state.applied_version,
        desired_paused=state.desired_paused, applied_paused=state.applied_paused, draining_count=state.draining_count,
        accepted_at=state.accepted_at, applied_at=state.applied_at,
        note="正在等待当前分片和数据请求结束。" if pending and state.desired_paused else "调度正在恢复。" if pending else "研究调度已暂停。" if state.applied_paused else "研究调度正常。")


def _actor(request: Request, viewer: str | None, body: TaskControlRequest | None = None) -> str:
    web = request.app.state.web
    if viewer is None:
        raise HTTPException(401, detail="请先登录。")
    allowed = ({viewer} if type(body) is SetMonitorBuiltinEnabled else web.settings.task_scheduling_admin_users
        if type(body) in (SetLabSchedulingPaused, PrepareNotifierDeliveryMode, SetNotifierDeliveryMode) else web.settings.task_unit_run_users)
    if web.settings.ingress_socket_path is None or web.proxy_identity is None or viewer not in allowed:
        raise HTTPException(403, detail="当前账号不能执行此操作。")
    if web.task_control_gateway is None:
        raise HTTPException(503, detail="任务操作尚未开放。")
    return viewer


def _public(request: TaskControlRequest, result: TaskControlAdmissionResult | None) -> TaskControlCommandData:
    if result is None:
        return TaskControlCommandData(command_id=request.command_id, original_request=request, status="not_found", message="尚未查到原请求。")
    values = {"command_id": request.command_id, "original_request": request}
    receipt = result.receipt
    if receipt.status in (PageControlStatus.PENDING, PageControlStatus.PROCESSING):
        return TaskControlCommandData(**values, status="pending", message="请求已受理，等待原请求结果。", can_resume=True)
    if receipt.status is not PageControlStatus.SUCCEEDED:
        return TaskControlCommandData(**values, status="unknown" if receipt.status is PageControlStatus.AMBIGUOUS else "rejected", message="结果待确认，请核验原请求。" if receipt.status is PageControlStatus.AMBIGUOUS else "本次请求已拒绝。", can_resume=receipt.status is PageControlStatus.AMBIGUOUS)
    if type(request) is PrepareUnitRun:
        confirmation = result.confirmation
        return TaskControlCommandData(**values, status="prepared", message="准备已完成，请确认本次运行。",
            confirmation_id=confirmation.confirmation_id, confirmation_expires_at=confirmation.expires_at)
    if type(request) is SetLabSchedulingPaused:
        submission = result.scheduling_submission
        rejected = submission is not None and submission.status == "rejected"
        return TaskControlCommandData(**values, status="rejected" if rejected else "submitted", message="版本已变化，请刷新调度状态。" if rejected else "请求已受理，等待调度应用。", can_resume=not rejected, desired_version=None if submission is None or submission.receipt is None else submission.receipt.desired_version)
    if type(request) is PrepareNotifierDeliveryMode:
        confirmation = result.notifier_confirmation
        return TaskControlCommandData(**values, status="prepared", message="请确认本次通知模式。",
            confirmation_id=confirmation.confirmation_id, confirmation_expires_at=confirmation.expires_at)
    if type(request) in (SetNotifierDeliveryMode, SetMonitorBuiltinEnabled):
        return TaskControlCommandData(**values, status="submitted", message="设置已保存，等待监控应用。",
            desired_revision=result.monitor_state.revision,
            desired_installation_sha256=result.monitor_state.installation_sha256)
    effect = result.unit_effect
    run = effect.run
    status = "succeeded" if effect.stage == "completed" and run.status == "succeeded" else "failed" if effect.stage == "completed" else "started" if effect.stage == "started" else "rejected" if effect.stage == "rejected" else "unknown" if effect.stage in ("unknown", "start_intent", "acknowledged") else "pending"
    messages = {"succeeded": "本次运行已完成。", "failed": "本次运行未完成。", "started": "本次运行已开始。", "rejected": "本次运行未启动。", "unknown": "运行结果待确认，请核验原请求。", "pending": "请求已受理，等待执行。"}
    return TaskControlCommandData(**values, status=status, message=messages[status], can_resume=status in ("unknown", "pending", "started"),
        started_at=None if run is None else run.started_at, ended_at=None if run is None else run.ended_at,
        invocation_id=None if run is None else run.invocation_id, duration_seconds=None if run is None or run.duration_ns is None else run.duration_ns / 1_000_000_000)


def _error(exc: Exception) -> HTTPException:
    if isinstance(exc, PermissionError):
        return HTTPException(403, detail="当前账号不能执行此操作。")
    if isinstance(exc, TaskControlAdmissionNotFoundError):
        return HTTPException(404, detail="尚未查到原请求。")
    if isinstance(exc, (TaskControlAdmissionRejectedError, PageControlCommandConflictError, ValueError, KeyError)):
        return HTTPException(409, detail="请求已变化，请核验原请求。")
    return HTTPException(503, detail="结果待确认，请核验原请求。")


@router.get("/control-capabilities", response_model=TaskControlCapabilitiesData, summary="任务执行与调度权限")
def capabilities(request: Request, response: Response, viewer: _Viewer, generation_id: Annotated[str | None, Query(min_length=1, max_length=128)] = None) -> TaskControlCapabilitiesData:
    web = request.app.state.web
    if viewer is None or web.task_control_gateway is None or web.proxy_identity is None or web.settings.ingress_socket_path is None:
        return TaskControlCapabilitiesData()
    recovery = {"can_recover_units": viewer in web.settings.task_unit_run_users, "can_recover_scheduling": viewer in web.settings.task_scheduling_admin_users}
    with web.tracker.borrow() as borrowed:
        if borrowed is None:
            return TaskControlCapabilitiesData(**recovery, note="任务状态暂无法核验。")
        current = borrowed.manifest.generation_id
        if generation_id is not None and generation_id != current:
            raise HTTPException(409, detail="数据已更新，请刷新任务。")
        try:
            view = read_task_center_view(borrowed)
            if view is None or not 0 <= (web.clock() - view.snapshot.sampled_at).total_seconds() < 120:
                raise ValueError("task source unavailable")
            caps = web.task_control_gateway.capabilities(authenticated_actor_id=viewer, generation_id=current)
            if caps.source_payload_hash != view.material_hash:
                raise ValueError("task capability differs from the same complete source")
        except Exception:
            return TaskControlCapabilitiesData(generation_id=current, **recovery, note="任务权限暂无法核验。")
        response.headers["X-Rquant-Generation"] = current
        return TaskControlCapabilitiesData(generation_id=current,
            units=tuple(TaskUnitControlChoice(unit=item.unit, can_request=item.can_request and web.settings.task_control_enabled, requires_confirmation=item.requires_confirmation, reason=_REASONS[item.reason],
                next_allowed_at=getattr(item, "next_allowed_at", None)) for item in caps.units) if viewer in web.settings.task_unit_run_users else (),
            can_control_scheduling=caps.can_control_scheduling and web.settings.task_control_enabled and viewer in web.settings.task_scheduling_admin_users,
            can_recover_units=caps.can_recover_units and viewer in web.settings.task_unit_run_users,
            can_recover_scheduling=caps.can_recover_scheduling and viewer in web.settings.task_scheduling_admin_users,
            scheduling=scheduling_view(view.scheduling_control),
            notifier_mode=NotifierModeControlView(available=caps.notifier_mode is not None,
                mode=None if caps.notifier_mode is None else caps.notifier_mode.mode,
                revision=None if caps.notifier_mode is None else caps.notifier_mode.revision,
                installation_sha256=(
                    None if caps.notifier_mode is None else caps.notifier_mode.installation_sha256),
                can_request=caps.can_control_notifier_mode and web.settings.task_control_enabled and viewer in web.settings.task_scheduling_admin_users,
                can_set_live=caps.can_set_notifier_live and web.settings.task_control_enabled and viewer in web.settings.task_scheduling_admin_users,
                note="切换前需要再次确认。" if caps.notifier_mode is not None else "通知模式暂无法核验。"),
            monitor_builtins=tuple(MonitorBuiltinControlView(builtin_id=row.builtin_id,
                label={"pool2_levels": "回踩档位", "pool_attack": "攻击信号", "surge": "爆量", "pulse": "市场异动"}[row.builtin_id],
                enabled=row.definition.enabled, revision=row.revision,
                installation_sha256=row.installation_sha256,
                can_request=caps.can_control_builtins and web.settings.task_control_enabled)
                for row in caps.builtin_controls),
            note="执行前会再次核验。" if web.settings.task_control_enabled else "任务操作尚未开放。")


async def _strict(request: Request) -> None:
    try:
        strict_json_loads(await request.body())
    except (TypeError, ValueError):
        raise HTTPException(422, detail="请求内容有误，请检查后重试。") from None


async def _submit(request: Request, viewer: str | None, body: TaskControlRequest) -> TaskControlCommandData:
    await _strict(request)
    actor = _actor(request, viewer, body)
    return await run_in_threadpool(_submit_checked, request, actor, body)


def _submit_checked(request: Request, actor: str, body: TaskControlRequest) -> TaskControlCommandData:
    web = request.app.state.web
    try:
        old = web.task_control_gateway.lookup(body, authenticated_actor_id=actor)
        if old is not None:
            return _public(body, web.task_control_gateway.resume(body, authenticated_actor_id=actor))
        if not web.settings.task_control_enabled:
            raise HTTPException(503, detail="任务操作尚未开放。")
        with web.tracker.borrow() as borrowed:
            if borrowed is None or borrowed.manifest.generation_id != body.generation_id:
                raise HTTPException(409, detail="数据已更新，请刷新任务。")
            view = read_task_center_view(borrowed)
            if view is None or borrowed.manifest.built_at > web.clock() or not 0 <= (web.clock() - view.snapshot.sampled_at).total_seconds() < 120:
                raise HTTPException(503, detail="任务状态暂无法核验。")
            caps = web.task_control_gateway.capabilities(authenticated_actor_id=actor, generation_id=body.generation_id)
            if caps.source_payload_hash != view.material_hash:
                raise HTTPException(409, detail="数据已更新，请刷新任务。")
            pointer = ServingReader(web.settings.serving_root).current_pointer()
            if borrowed.pointer is None or (pointer.generation_id, pointer.manifest_sha256) != (borrowed.pointer.generation_id, borrowed.pointer.manifest_sha256):
                raise HTTPException(409, detail="数据已更新，请刷新任务。")
            result = web.task_control_gateway.submit(body, authenticated_actor_id=actor, verified_metadata_identity=caps.metadata_identity)
        return _public(body, result)
    except HTTPException:
        raise
    except Exception as exc:
        raise _error(exc) from exc


@router.post("/units/{unit}/run/prepare", response_model=TaskControlCommandData, summary="准备本次任务运行")
async def prepare_unit(unit: _Unit, request: Request, viewer: _Viewer, csrf: _CSRF, body: PrepareUnitRun) -> TaskControlCommandData:
    if body.run.unit != unit:
        raise HTTPException(422, detail="任务与本次请求不一致。")
    return await _submit(request, viewer, body)


@router.post("/units/{unit}/run", response_model=TaskControlCommandData, summary="请求运行精确任务")
async def run_unit(unit: _Unit, request: Request, viewer: _Viewer, csrf: _CSRF, body: RequestUnitRun) -> TaskControlCommandData:
    if body.unit != unit:
        raise HTTPException(422, detail="任务与本次请求不一致。")
    return await _submit(request, viewer, body)


@router.post("/scheduling/commands", response_model=TaskControlCommandData, summary="暂停或恢复研究调度")
async def scheduling(request: Request, viewer: _Viewer, csrf: _CSRF, body: SetLabSchedulingPaused) -> TaskControlCommandData:
    return await _submit(request, viewer, body)


@router.post("/notifications/mode/prepare", response_model=TaskControlCommandData, summary="准备切换通知模式")
async def prepare_notifier_mode(request: Request, viewer: _Viewer, csrf: _CSRF, body: PrepareNotifierDeliveryMode) -> TaskControlCommandData:
    return await _submit(request, viewer, body)


@router.post("/notifications/mode", response_model=TaskControlCommandData, summary="确认切换通知模式")
async def set_notifier_mode(request: Request, viewer: _Viewer, csrf: _CSRF, body: SetNotifierDeliveryMode) -> TaskControlCommandData:
    return await _submit(request, viewer, body)


@router.post("/monitor/builtins/commands", response_model=TaskControlCommandData, summary="启用或关闭内置监控")
async def set_monitor_builtin(request: Request, viewer: _Viewer, csrf: _CSRF, body: SetMonitorBuiltinEnabled) -> TaskControlCommandData:
    return await _submit(request, viewer, body)


async def _original(request: Request, viewer: str | None, body: TaskControlRequest, *, resume: bool) -> TaskControlCommandData:
    await _strict(request)
    actor = _actor(request, viewer, body)
    try:
        gateway = request.app.state.web.task_control_gateway
        result = await run_in_threadpool(gateway.resume if resume else gateway.lookup, body, authenticated_actor_id=actor)
        return _public(body, result)
    except Exception as exc:
        raise _error(exc) from exc


@router.post("/controls/lookup", response_model=TaskControlCommandData, summary="核验任务原请求")
async def lookup(request: Request, viewer: _Viewer, csrf: _CSRF, body: _Command) -> TaskControlCommandData:
    return await _original(request, viewer, body, resume=False)


@router.post("/controls/resume", response_model=TaskControlCommandData, summary="恢复任务原请求回执")
async def resume(request: Request, viewer: _Viewer, csrf: _CSRF, body: _Command) -> TaskControlCommandData:
    return await _original(request, viewer, body, resume=True)
