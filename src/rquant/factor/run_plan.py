"""Compile the existing 09:25 adapter schedule without dropping history or returns."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from pydantic import BaseModel

from rquant.factor.job_ledger import FactorLedgerIdentity
from rquant.factor.registry import FactorHeadRef, FactorRegistryIdentity
from rquant.factor.run_request import RUN_IMMUTABLE, FactorRunRequest


class FactorRunPlanRejectedError(ValueError):
    """A parameter window cannot run against the verified frozen calendar."""

    def __init__(self, message: str, *, reason: str) -> None:
        super().__init__(message)
        self.reason = reason


class FactorRunSchedule(BaseModel):
    model_config = RUN_IMMUTABLE

    evaluation_days: tuple[date, ...]
    calculation_days: tuple[date, ...]
    panel_day: date
    return_visibility_day: date


def compile_factor_run_schedule(
    open_days: tuple[date, ...],
    *,
    start_date: date,
    end_date: date,
    holding_sessions: int,
    history_window: int,
) -> FactorRunSchedule:
    if (
        not open_days
        or tuple(sorted(set(open_days))) != open_days
        or holding_sessions not in (1, 5, 10, 20)
        or type(holding_sessions) is not int
        or type(history_window) is not int
        or history_window < 1
        or start_date > end_date
    ):
        raise ValueError("检验参数或冻结日历不正确")
    indexes = tuple(i for i, day in enumerate(open_days) if start_date <= day <= end_date)
    if not indexes:
        raise FactorRunPlanRejectedError(
            "所选区间没有交易日", reason="所选区间没有交易日，请调整日期。"
        )
    evaluations = indexes[::holding_sessions]
    first = evaluations[0] - history_window + 1
    last = evaluations[-1] + holding_sessions - 1
    if first < 1:
        raise FactorRunPlanRejectedError(
            "来源缺少预热或首日前一交易日",
            reason="开始日期前的历史数据不足，请调整开始日期。",
        )
    if last + 1 >= len(open_days):
        raise FactorRunPlanRejectedError(
            "来源缺少完整收益窗口或下一交易日",
            reason="结束日期后的收益数据不足，请调整结束日期。",
        )
    calculation = open_days[first : last + 1]
    if len(calculation) > 1024:
        raise FactorRunPlanRejectedError(
            "计算区间超过 1024 个交易日", reason="检验区间过长，请缩短日期范围。"
        )
    return FactorRunSchedule(
        evaluation_days=tuple(open_days[i] for i in evaluations),
        calculation_days=calculation,
        panel_day=open_days[first - 1],
        return_visibility_day=open_days[last + 1],
    )


class FrozenFactorRunPlan(BaseModel):
    model_config = RUN_IMMUTABLE

    request: FactorRunRequest
    registry_identity: FactorRegistryIdentity
    ledger_identity: FactorLedgerIdentity
    spec: FactorStreamJobSpec


from rquant.factor.stream_job_spec import FactorStreamJobSpec  # noqa: E402

FrozenFactorRunPlan.model_rebuild()


def compile_factor_run_plan(
    root: Path,
    reference: FactorRunFileReference,
    request: FactorRunRequest,
    *,
    verified_registry_instance_id: str,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> FrozenFactorRunPlan:
    from rquant.factor.formula_stream import (
        FactorFormulaStreamRequest,
        FactorFormulaStreamSources,
        _compile,
    )
    from rquant.factor.historical_adapter import _market_time
    from rquant.factor.member_archive import (
        FactorMemberArchiveRequest,
        load_factor_member_archive,
        publish_factor_member_archive,
    )
    from rquant.factor.member_stream import open_factor_member_stream
    from rquant.factor.registry import FactorDefinitionRegistry
    from rquant.factor.run_configuration import open_factor_run_configuration
    from rquant.factor.stream_adapter import FactorStreamAdapterRequest
    from rquant.factor.stream_snapshot import open_factor_stream_snapshot_admission
    from rquant.factor.time_series import DecisionTime
    from rquant.research_snapshot import FactorReadQuery

    with open_factor_run_configuration(root, reference) as loaded:
        config, source = loaded.configuration, loaded.source
        if not config.enabled or not config.factor_run_users:
            raise PermissionError("运行入口尚未开启")
        if config.registry_identity.instance_id != verified_registry_instance_id:
            raise ValueError("定义来源已变化，请刷新")
        registry = FactorDefinitionRegistry(Path(config.registry_identity.path))
        record = registry.get_head(
            request.parameters.factor_id, expected_identity=config.registry_identity
        )
        if (
            record is None
            or record.head.archived
            or FactorHeadRef(version=record.head.version, content_sha256=record.head.content_sha256)
            != request.parameters.expected_head
        ):
            raise ValueError("定义版本已变化，请刷新")
        loaded.open_ledger(clock=clock)
        scope = source.admission_request.scope
        params = request.parameters
        if not scope.start_date <= params.start_date <= params.end_date <= scope.end_date:
            raise ValueError("所选区间超出冻结来源")
        with open_factor_stream_snapshot_admission(
            source.admission_request,
            metadata_store=loaded.metadata,
            lake_root=config.lake_root,
        ) as (lease, _):
            opened: list[date] = []
            start = scope.start_date
            while start <= scope.end_date:
                end = min(start + timedelta(days=365), scope.end_date)
                batch = lease.query_sse_calendar(
                    FactorReadQuery(
                        binding_hash=source.admission_request.binding_hash,
                        stock_codes=(scope.stock_codes[0],),
                        start_date=start,
                        end_date=end,
                        row_limit=366,
                    )
                )
                rows = sorted(batch.rows, key=lambda row: row.cal_date)
                if tuple(row.cal_date for row in rows) != tuple(
                    start + timedelta(days=i) for i in range((end - start).days + 1)
                ):
                    raise ValueError("冻结日历不完整")
                for row in rows:
                    if row.is_open:
                        if (opened and row.pretrade_date != opened[-1]) or (
                            not opened
                            and (row.pretrade_date is None or row.pretrade_date >= scope.start_date)
                        ):
                            raise ValueError("冻结日历前链不完整")
                        opened.append(row.cal_date)
                start = end + timedelta(days=1)
            if tuple(opened) != source.receipt.calendar_open_days:
                raise ValueError("来源包日历与实际冻结来源不同")
        try:
            schedule = compile_factor_run_schedule(
                tuple(opened),
                start_date=params.start_date,
                end_date=params.end_date,
                holding_sessions=params.holding_sessions,
                history_window=record.definition.max_history_window,
            )
        except FactorRunPlanRejectedError:
            loaded.recheck()
            loaded.open_ledger(clock=clock)
            if (
                registry.get_head(params.factor_id, expected_identity=config.registry_identity)
                != record
            ):
                raise ValueError("定义版本已变化，请刷新") from None
            raise
        binding = next(
            (item for item in config.members if item.selection == params.selection), None
        )
        if binding is None:
            raise ValueError("该股票池缺少历史归档")
        with open_factor_member_stream(config.member_root, binding.archive) as stream:
            parent = stream.manifest
            if (
                parent.request.selection != params.selection
                or parent.request.computation_stock_codes != scope.stock_codes
                or parent.request.as_of > scope.as_of_time
            ):
                raise ValueError("成员归档与来源范围不符")
            for universe in stream:
                del universe
            stream.require_completion()
            by_date = {day.trade_date: day.filename for day in parent.days}
            if not set(schedule.calculation_days) <= by_date.keys():
                raise ValueError("成员归档缺少计算日期")
            subset = publish_factor_member_archive(
                FactorMemberArchiveRequest(
                    selection=params.selection,
                    trading_days=schedule.calculation_days,
                    as_of=scope.as_of_time,
                    computation_stock_codes=scope.stock_codes,
                ),
                input_root=config.member_root,
                daily_filenames=(by_date[day] for day in schedule.calculation_days),
                root=config.member_root,
            )
            manifest = load_factor_member_archive(config.member_root, subset)
            stream.require_completion()
        sources = FactorFormulaStreamSources(
            **manifest.sources.model_dump(),
            feature_source_id=source.admission_request.snapshot_id,
            feature_source_sha256=source.admission_request.binding_hash,
        )
        formula = FactorFormulaStreamRequest(
            definition=record.definition,
            computation_stock_codes=scope.stock_codes,
            trading_days=schedule.calculation_days,
            decision_times=tuple(
                DecisionTime(trade_date=day, decision_at=_market_time(day, 9, 25))
                for day in schedule.calculation_days
            ),
            as_of=scope.as_of_time,
            selection=params.selection,
            sources=sources,
        )
        _compile(formula)
        spec = FactorStreamJobSpec(
            code_revision=config.code_revision,
            adapter_request=FactorStreamAdapterRequest(
                source=source.admission_request,
                scope_content_hash=source.scope_content_hash,
                formula=formula,
                evaluation_days=schedule.evaluation_days,
                holding_sessions=params.holding_sessions,
            ),
            member_archive=subset,
            definition_content_sha256=record.content_sha256,
            deadline=max(clock().astimezone(UTC), scope.as_of_time)
            + timedelta(seconds=config.deadline_seconds),
        )
        loaded.recheck()
        if (
            registry.get_head(params.factor_id, expected_identity=config.registry_identity)
            != record
        ):
            raise ValueError("定义版本已变化，请刷新")
        return FrozenFactorRunPlan(
            request=request,
            registry_identity=config.registry_identity,
            ledger_identity=config.ledger_identity,
            spec=spec,
        )


from rquant.factor.run_configuration import FactorRunFileReference  # noqa: E402
