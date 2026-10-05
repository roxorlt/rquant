"""Read the original Lab ledger and verified, immutable portfolio result views."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Literal
from uuid import UUID

from fastapi.responses import Response
from pydantic import BaseModel

from rquant.backtest.contracts import BacktestOrder
from rquant.lab_jobs import (
    JobStatus,
    LabJobListFilters,
    LabJobReader,
)
from rquant.paper_contracts import PaperOrderStatus, PaperRejectReason
from rquant.portfolio_backtest_artifact import (
    PortfolioResultReader,
    PortfolioViewReadResult,
    PortfolioZipExportFacade,
)
from rquant.portfolio_backtest_models import PortfolioBacktestConfig
from rquant.runtime_contracts import RuntimeContractModel
from rquant.web.envelope import Envelope, ServingMeta, ServingState
from rquant.web.models.backtests import (
    PortfolioCapabilities,
    PortfolioEditableConfig,
    PortfolioHoldingRow,
    PortfolioJob,
    PortfolioJobsData,
    PortfolioLogRow,
    PortfolioMonthRow,
    PortfolioNavData,
    PortfolioNavRow,
    PortfolioPerformanceData,
    PortfolioRowsData,
    PortfolioSourceOption,
    PortfolioSummaryData,
    PortfolioTradeRow,
)

MAX_HTTP_BYTES = 16 * 1024 * 1024
_ORDER_LABELS = {
    PaperOrderStatus.PENDING: "待处理",
    PaperOrderStatus.ACCEPTED: "已接收",
    PaperOrderStatus.PARTIALLY_FILLED: "部分成交",
    PaperOrderStatus.FILLED: "已成交",
    PaperOrderStatus.REJECTED: "未成交",
    PaperOrderStatus.CANCELLED: "已取消",
    PaperOrderStatus.EXPIRED: "已过期",
}
_ORDER_MESSAGES = {
    PaperOrderStatus.PENDING: "订单待处理。",
    PaperOrderStatus.ACCEPTED: "订单已接收。",
    PaperOrderStatus.PARTIALLY_FILLED: "订单部分成交。",
    PaperOrderStatus.FILLED: "订单已成交。",
    PaperOrderStatus.REJECTED: "条件不满足，本次未成交。",
    PaperOrderStatus.CANCELLED: "订单已取消。",
    PaperOrderStatus.EXPIRED: "订单已过期。",
}
_PAPER_REASONS = {
    PaperRejectReason.T_PLUS_ONE: "当日买入尚不可卖出。",
    PaperRejectReason.SUSPENDED: "股票停牌，未成交。",
    PaperRejectReason.LIMIT_LOCKED: "涨跌停限制，未成交。",
    PaperRejectReason.INSUFFICIENT_CASH: "资金不足，未成交。",
    PaperRejectReason.INSUFFICIENT_POSITION: "可卖数量不足，未成交。",
    PaperRejectReason.INVALID_LOT: "数量不符合整手要求，未成交。",
    PaperRejectReason.EXPIRED: "订单已过期，未成交。",
    PaperRejectReason.RISK_REJECTED: "风险限制生效，未成交。",
}
_BUSINESS_MESSAGES = {
    "skipped": "本次未提交订单。",
    "incomplete": "本次结果不完整。",
    "risk": "仓位限制状态已更新。",
}
_REASONS = {
    "missing_held_close": "持仓缺少收盘价，本次结果不完整。",
    "drawdown_blocked": "回撤限制生效，暂停开新仓。",
    "drawdown_capped": "回撤限制生效，降低目标仓位。",
    "drawdown_released": "回撤已恢复，解除仓位限制。",
    "missing_open_price": "缺少开盘价，未提交订单。",
    "missing_decision_price": "缺少参考价，未提交订单。",
    "unverified_conditions": "缺少开盘交易条件，未提交订单。",
    "below_lot": "目标不足一手，未提交订单。",
    "insufficient_cash": "资金不足，未成交。",
    "insufficient_position": "可卖数量不足，未成交。",
    "t_plus_one": "当日买入尚不可卖出。",
    "suspended": "股票停牌，未成交。",
    "limit_up": "涨停限制，未成交。",
    "limit_down": "跌停限制，未成交。",
    "invalid_lot": "数量不符合整手要求，未成交。",
}


def bounded_portfolio_response(value: BaseModel) -> Response:
    """Measure the complete HTTP JSON, including metadata, before publishing it."""
    raw = json.dumps(
        value.model_dump(mode="json"), ensure_ascii=False, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")
    if len(raw) > MAX_HTTP_BYTES:
        raise ValueError("查询范围过大，请缩小范围。 HTTP byte budget exceeded")
    headers = {"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"}
    if isinstance(value, Envelope) and value.serving.generation_id is not None:
        headers["X-Rquant-Generation"] = value.serving.generation_id
    return Response(raw, media_type="application/json", headers=headers)


def portfolio_meta(
    *,
    result_hash: str | None,
    built_at: datetime | None,
    available: bool = True,
    message: str | None = None,
) -> ServingMeta:
    # A sealed historical result has no five-minute freshness requirement.
    return ServingMeta(
        generation_id=result_hash,
        built_at=built_at,
        age_seconds=None,
        state=ServingState.READY if available else ServingState.UNAVAILABLE,
        message=message,
        detail="",
    )


class _LogFact(RuntimeContractModel):
    trade_date: str
    ts_code: str | None
    status: PaperOrderStatus | Literal["skipped", "incomplete", "risk"]
    reason: str | None


class PortfolioWebService:
    def __init__(
        self,
        *,
        reader: LabJobReader,
        results: PortfolioResultReader,
        sources: tuple[PortfolioSourceOption, ...] = (),
        default_config: PortfolioBacktestConfig | None = None,
        preparation_available: bool = False,
        exports: PortfolioZipExportFacade | None = None,
    ) -> None:
        self.reader, self.results, self.exports = reader, results, exports
        self.sources = tuple(
            PortfolioSourceOption.model_validate(source.model_dump(mode="python"))
            for source in sources
        )
        if len(self.sources) > 100 or len({(s.key, s.version) for s in self.sources}) != len(
            self.sources
        ):
            raise ValueError("portfolio source options exceed their budget or repeat")
        if default_config is not None and not any(
            (s.key, s.version) == (default_config.source_key, default_config.source_version)
            for s in self.sources
        ):
            raise ValueError("portfolio default config has no trusted source option")
        self.default_config = default_config
        self.preparation_available = preparation_available

    def capabilities(self, *, can_write: bool) -> PortfolioCapabilities:
        installed = self.preparation_available and bool(self.sources)
        return PortfolioCapabilities(
            available=True,
            can_run=installed and can_write,
            can_export=self.exports is not None and can_write,
            message=None if installed else "尚无可用回测来源，请先准备完整候选与行情。",
            sources=self.sources,
            benchmarks=(
                "000300.SH",
                "000905.SH",
                "000852.SH",
                "000001.SH",
                "399001.SZ",
                "399006.SZ",
            ),
            default_config=None
            if self.default_config is None
            else PortfolioEditableConfig.from_domain(self.default_config),
        )

    def job(self, job_id: UUID, *, progress: float | None = None) -> PortfolioJob:
        context = self.reader.get_command_context(job_id)
        if context is None or context.job.spec.parameters.strategy_name != "portfolio_backtest":
            raise LookupError("找不到这次组合回测。")
        job, availability = context.job, context.availability
        authority = self.reader.get_artifact_preview_authority(job_id)
        status: Literal[
            "queued", "running", "paused", "cancelled", "failed", "completed", "sealing"
        ]
        if job.status is JobStatus.SUCCEEDED:
            status = "completed" if authority is not None else "sealing"
        else:
            status = {
                JobStatus.QUEUED: "queued",
                JobStatus.RUNNING: "running",
                JobStatus.CHECKPOINTED: "paused",
                JobStatus.CANCELLED: "cancelled",
                JobStatus.FAILED: "failed",
            }[job.status]
        label = {
            "queued": "等待运行",
            "running": "正在运行",
            "paused": "已暂停",
            "cancelled": "已取消",
            "failed": "运行失败",
            "completed": "已完成",
            "sealing": "正在保存结果",
        }[status]
        return PortfolioJob(
            job_id=job_id,
            status=status,
            label=label,
            version=job.version,
            start_date=job.spec.parameters.start_date,
            end_date=job.spec.parameters.end_date,
            created_at=job.created_at,
            updated_at=job.updated_at,
            result_hash=None if authority is None else authority.evidence.complete_result_hash,
            progress=progress,
            can_pause=availability.pause,
            can_resume=availability.resume,
            can_cancel=availability.cancel,
            can_retry=availability.retry,
        )

    def jobs(self, *, limit: int, cursor: str | None) -> PortfolioJobsData:
        page = self.reader.list_jobs(
            filters=LabJobListFilters(keyword="portfolio_backtest"), limit=limit, cursor=cursor
        )
        jobs = tuple(
            self.job(item.job_id, progress=item.progress.fraction)
            for item in page.items
            if item.strategy_name == "portfolio_backtest"
        )
        return PortfolioJobsData(available=True, jobs=jobs, next_cursor=page.next_cursor)

    def summary(self, job_id: UUID) -> PortfolioSummaryData:
        job = self.job(job_id)
        if job.result_hash is None:
            return PortfolioSummaryData(
                available=False,
                job=job,
                result_hash=None,
                result_status=None,
                config=None,
                performance=None,
                benchmark_available=False,
                benchmark_message=None,
                source_updated_at=None,
                completed_days=0,
                message="完成后在这里查看净值、成交和报告。",
                can_report=False,
            )
        result = self.results.read(job_id, expected_result_hash=job.result_hash)
        bundle = result.bundle
        performance = (
            None
            if bundle.performance is None
            else PortfolioPerformanceData.model_validate(
                bundle.performance.model_dump(mode="python", exclude={"round_trips"})
            )
        )
        updated = next(
            (
                source.updated_at
                for source in self.sources
                if (source.key, source.version)
                == (bundle.frozen.config.source_key, bundle.frozen.config.source_version)
            ),
            None,
        )
        return PortfolioSummaryData(
            available=True,
            job=job,
            result_hash=result.result_hash,
            result_status=bundle.result.status,
            config=PortfolioEditableConfig.from_domain(bundle.frozen.config),
            performance=performance,
            benchmark_available=bundle.benchmark is not None,
            benchmark_message=None
            if bundle.benchmark is not None
            else "基准缺少完整行情，暂不计算超额收益。",
            source_updated_at=updated,
            completed_days=sum(day.account is not None for day in bundle.result.days),
            message="持仓缺少收盘价，结果仅包含已完成日期。"
            if bundle.result.status == "incomplete"
            else None,
            can_report=bundle.html is not None,
        )

    def _view(
        self, job_id: UUID, name: str, result_hash: str, offset: int, limit: int
    ) -> PortfolioViewReadResult:
        self.results.read(job_id, expected_result_hash=result_hash)
        return self.results.read_view(
            job_id,
            table_name="portfolio_" + name,
            expected_result_hash=result_hash,
            offset=offset,
            limit=limit,
        )

    def nav(self, job_id: UUID, *, result_hash: str) -> PortfolioNavData:
        view = self._view(job_id, "nav", result_hash, 0, 1830)
        if view.total_rows > 1830:
            raise ValueError("portfolio nav exceeds date budget")
        return PortfolioNavData(
            result_hash=result_hash,
            rows=tuple(PortfolioNavRow.model_validate_json(payload) for payload in view.payloads),
        )

    def rows(
        self,
        job_id: UUID,
        *,
        view: Literal["trades", "holdings", "daily", "monthly", "log"],
        result_hash: str,
        offset: int,
        limit: int,
    ) -> PortfolioRowsData:
        if not 1 <= limit <= 50:
            raise ValueError("portfolio public page exceeds row budget")
        read = self._view(job_id, view, result_hash, offset, limit)
        if view == "trades":
            rows = tuple(self._trade(payload) for payload in read.payloads)
        elif view == "holdings":
            rows = tuple(
                PortfolioHoldingRow.model_validate_json(payload) for payload in read.payloads
            )
        elif view == "daily":
            rows = tuple(PortfolioNavRow.model_validate_json(payload) for payload in read.payloads)
        elif view == "monthly":
            rows = tuple(
                PortfolioMonthRow.model_validate_json(payload) for payload in read.payloads
            )
        else:
            rows = tuple(self._log(payload) for payload in read.payloads)
        next_offset = offset + len(rows) if offset + len(rows) < read.total_rows else None
        return PortfolioRowsData(
            result_hash=result_hash,
            view=view,
            **{view: rows},
            total=read.total_rows,
            next_offset=next_offset,
        )

    @staticmethod
    def _trade(payload: str) -> PortfolioTradeRow:
        # The producer stores one typed order plus its trading date in the view.
        raw = json.loads(payload)
        trade_date = raw.pop("trade_date")
        order = BacktestOrder.model_validate_json(json.dumps(raw))
        fill = order.receipt.fill
        rejection = order.receipt.order.reject_reason
        return PortfolioTradeRow(
            trade_date=trade_date,
            ts_code=order.intent.ts_code,
            side=order.intent.side.value,
            quantity=order.intent.quantity if fill is None else fill.quantity,
            price=None if fill is None else fill.price,
            amount=None if fill is None else fill.notional,
            fees=None if fill is None else fill.total_fees,
            status=_ORDER_LABELS[order.receipt.order.status],
            reason=None if rejection is None else _PAPER_REASONS[rejection],
        )

    @staticmethod
    def _log(payload: str) -> PortfolioLogRow:
        fact = _LogFact.model_validate_json(payload)
        if isinstance(fact.status, PaperOrderStatus):
            message = (
                _ORDER_MESSAGES[fact.status]
                if fact.reason is None
                else _PAPER_REASONS[PaperRejectReason(fact.reason)]
            )
        else:
            message = _REASONS.get(fact.reason or "", _BUSINESS_MESSAGES[fact.status])
        return PortfolioLogRow(
            trade_date=fact.trade_date,
            ts_code=fact.ts_code,
            level="error"
            if fact.status == "incomplete"
            else "note"
            if fact.reason is not None
            else "normal",
            message=message,
        )
