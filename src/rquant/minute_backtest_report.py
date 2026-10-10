"""Offline HTML of verified minute values using the original shared renderer."""

from __future__ import annotations

import hashlib
from typing import Literal, Self
from uuid import UUID
from zoneinfo import ZoneInfo

from pydantic import Field, model_validator

from rquant.minute_backtest_artifact import MinuteSealedReplayResult
from rquant.minute_backtest_parameter_artifact import MinuteParameterSealedReplayResult
from rquant.minute_backtest_contracts import MinuteReplayModel, Sha256
from rquant.minute_backtest_performance import MinuteReplayPerformance, build_minute_performance
from rquant.paper_signal_worker import PaperQuoteSnapshot
from rquant.runtime_contracts import canonical_sha256
from rquant.sealed_result_html import MAX_HTML_BYTES, _Document, _chart, _table, validate_offline_html
from rquant.sealed_result_ownership import SealedArtifactFact, SealedOwnerBinding, require_owned_sealed_result
from rquant.minute_experiment_result_owner import MinuteExperimentSealedOwnerProof, require_minute_experiment_owner
from rquant.strategy_catalog_source import _NAMES


# Bind the complete approved policy; its name alone does not identify this source nature.
_HISTORICAL_RECONSTRUCTION_POLICY_FINGERPRINT = "744546c1df988b502d8f09bfc189737561ebc7072f3794682fa60dd217b87838"


def _report_quote(quote: PaperQuoteSnapshot) -> dict[str, object]:
    return quote.model_dump(mode="json", exclude={
        "context": {"instrument_context"}, "producer_commit": True,
    }) | {
        "instrument_context_hash": canonical_sha256(quote.context.instrument_context.model_dump(mode="json")),
        "complete_quote_hash": canonical_sha256(quote.model_dump(mode="json")),
    }


def minute_artifact_fact(sealed: MinuteSealedReplayResult | MinuteParameterSealedReplayResult,
    *, html_sha256: Sha256 | None = None,
) -> SealedArtifactFact:
    if type(sealed) not in (MinuteSealedReplayResult, MinuteParameterSealedReplayResult):
        raise TypeError("exact complete minute sealed result required")
    return SealedArtifactFact(domain="minute", job_id=str(sealed.job_id), spec_hash=sealed.spec_hash,
        manifest_hash=sealed.manifest_hash, complete_result_hash=sealed.complete_result_hash,
        full_artifact_hash=sealed.manifest.complete_result_hash, result_payload_hash=sealed.result_hash,
        input_hash=sealed.full_input_hash,
        display_hash=canonical_sha256(sealed.model_dump(mode="json", exclude_computed_fields=True)),
        html_sha256=html_sha256, complete=True, private_owner=sealed.owner_id)


class MinuteHtmlReport(MinuteReplayModel):
    contract: Literal["minute-replay-html/v1"] = "minute-replay-html/v1"
    job_id: UUID
    result_hash: Sha256
    full_input_hash: Sha256
    core_input_hash: Sha256
    seed_hash: Sha256
    owner_binding_hash: Sha256
    performance: MinuteReplayPerformance
    html: str = Field(min_length=1, max_length=MAX_HTML_BYTES)
    html_sha256: Sha256

    @model_validator(mode="after")
    def complete_offline_bytes(self) -> Self:
        raw = self.html.encode("utf-8")
        validate_offline_html(raw)
        if hashlib.sha256(raw).hexdigest() != self.html_sha256:
            raise ValueError("minute HTML digest differs from its exact original bytes")
        return self

    def html_bytes(self) -> bytes:
        return self.html.encode("utf-8")


def build_minute_html_report(
    sealed: MinuteSealedReplayResult | MinuteParameterSealedReplayResult, *,
    owner: SealedOwnerBinding | MinuteExperimentSealedOwnerProof, requester: str,
    max_bytes: int = MAX_HTML_BYTES,
    parameter_source_nature: Literal["real_retained", "historical_reconstruction", "synthetic_validation"] | None = None,
) -> MinuteHtmlReport:
    """Callers must first perform the original physical reader and current-role gate."""
    facts = minute_artifact_fact(sealed)
    if type(owner) is MinuteExperimentSealedOwnerProof:
        require_minute_experiment_owner(owner, requester=requester, current_artifact=facts)
    else:
        require_owned_sealed_result(owner, requester=requester, current_artifact=facts)
    runtime = sealed.result.publication.frozen.runtime
    replay = sealed.result.replay
    provenance = sealed.result.publication.frozen.provenance
    parameter_result = type(sealed) is MinuteParameterSealedReplayResult
    if parameter_result and parameter_source_nature is None:
        raise PermissionError("parameter report requires its verified installed source nature")
    if not parameter_result and parameter_source_nature is not None:
        raise PermissionError("native report derives source nature from its bound provenance")
    historical_native_source = (not parameter_result and provenance.source_kind == "reconstructed"
        and provenance.visibility_policy is not None
        and provenance.visibility_policy.fingerprint == _HISTORICAL_RECONSTRUCTION_POLICY_FINGERPRINT)
    name = _NAMES[runtime.parameters.parameters.family] if parameter_result else _NAMES[runtime.strategy.strategy_id]
    source_label = ("合成验收来源" if parameter_source_nature == "synthetic_validation" else
        "真实留存资料派生研究源" if parameter_source_nature == "real_retained" else
        "历史重建研究源" if parameter_source_nature == "historical_reconstruction" else
        "历史重建" if historical_native_source else
        "真实采集归档" if provenance.source_kind == "captured" else "重建研究源")
    performance = build_minute_performance(replay, runtime=runtime)
    doc = _Document("分钟回测报告", max_bytes)
    _table(doc, "研究配置", {
        "策略": name, "策略版本": runtime.strategy.strategy_version,
        "开始日期": runtime.start_date, "结束日期": runtime.end_date,
        "初始资金": runtime.execution_profile.initial_cash,
        "执行状态": "完整" if replay.status == "complete" else "未完整",
        "净值状态": "完整" if replay.daily_status == "complete" else "有估值缺口",
        "来源类型": source_label,
    })
    doc.add("<p>每日净值按15:00已确认的行情估值。报价的真实时间见逐日记录；此口径不等于官方收盘价。</p>")
    if historical_native_source:
        doc.add("<p>本输入为历史重建。模型可见时刻不代表当时实际捕获，来源限制见详情。</p>")
    elif provenance.source_kind == "reconstructed":
        doc.add("<p>本输入为重建研究源。建模时刻不代表当时真实捕获，原可见性政策与限制保存在详情中。</p>")
    _chart(doc, "每日净值", tuple((str(day.trade_date), day.nav) for day in performance.daily))
    _chart(doc, "回撤", tuple((str(day.trade_date), day.drawdown) for day in performance.daily))
    _table(doc, "逐日净值", tuple({"交易日": day.trade_date, "估值状态": "完整" if day.status == "complete" else "不可用",
        "净值": day.nav, "日收益": day.daily_return, "归一净值": day.normalized_nav, "回撤": day.drawdown}
        for day in performance.daily))
    _table(doc, "逐日行情依据", tuple({"交易日": day.trade_date, "估值时间": day.as_of,
        "原报价": tuple({"证券代码": proof.quote.ts_code, "报价时间": proof.quote.event_time,
            "可见时间": proof.quote.available_at}
            for proof in day.price_proofs), "不可用原因": day.unavailable_reasons}
        for day in replay.daily_valuations))
    if performance.metrics is None:
        _table(doc, "绩效不可用", {"原因": performance.unavailable_reasons})
    else:
        summary = performance.metrics.summary
        _table(doc, "绩效", {"收益观测数": summary.observations, "累计收益": summary.total_return,
            "年化收益": summary.annualized_return, "年化波动率": summary.annualized_volatility,
            "夏普比率": summary.sharpe, "索提诺比率": summary.sortino, "卡玛比率": summary.calmar,
            "最大回撤": summary.max_drawdown, "回撤持续交易日": summary.max_drawdown_duration,
            "日收益胜率": summary.win_rate, "日收益盈亏比": summary.payoff_ratio,
            "年化换手率": performance.metrics.annualized_turnover})
        _table(doc, "月度收益", tuple({"年份": item.year, "月份": item.month,
            "交易日数": item.daily_observations, "收益": item.return_value} for item in performance.monthly))
        trades = performance.metrics.round_trip_analysis.overall
        _table(doc, "成交成本与 FIFO 统计", {"完整交易数": trades.count, "净盈亏": trades.net_pnl,
            "交易胜率": trades.win_rate, "交易盈亏比": trades.payoff_ratio, "平均持有天数": trades.average_holding_days})
    doc.add("<p>本来源未提供基准收益，基准比较不可用。过拟合检验未评估。</p>")
    orders = {order.order_id: order for order in replay.orders}
    _table(doc, "成交明细", tuple({"成交时间": fill.executed_at.astimezone(ZoneInfo("Asia/Shanghai")),
        "证券代码": orders[fill.order_id].ts_code,
        "方向": "买入" if orders[fill.order_id].side.value == "BUY" else "卖出",
        "数量": fill.quantity, "成交价": fill.price, "佣金": fill.commission,
        "过户费": fill.transfer_fee, "印花税": fill.tax, "费用合计": fill.total_fees}
        for fill in replay.fills))
    doc.add("<details><summary>完整信号、执行口径与来源凭据</summary>")
    _table(doc, "原完整绩效", performance.model_dump(mode="json"))
    if type(owner) is MinuteExperimentSealedOwnerProof:
        shared_signal_fields = {"schema_version", "strategy_id", "strategy_version", "parameter_fingerprint",
            "dataset_snapshot_id", "producer_commit"}
        _table(doc, "信号共同身份", {canonical_sha256(value): value for value in (
            item.model_dump(mode="json", include=shared_signal_fields) for item in replay.signals)})
        _table(doc, "原完整信号", tuple(item.model_dump(mode="json", exclude=shared_signal_fields) | {
            "shared_identity_hash": canonical_sha256(item.model_dump(mode="json", include=shared_signal_fields)),
            "complete_signal_hash": canonical_sha256(item.model_dump(mode="json")),
        } for item in replay.signals))
        _table(doc, "原完整订单与成交", {"orders": tuple(item.model_dump(mode="json") for item in replay.orders),
            "fills": tuple(item.model_dump(mode="json") for item in replay.fills)})
        # The complete signal/order already appear above. Each original queue and
        # quote remains bound by its digest; only repeated internal data are elided.
        _table(doc, "原队列状态、意图与报价", tuple(
            item.model_dump(mode="json", exclude={"signal", "order", "quote"}) | {
                "signal_id": item.signal.signal_id,
                "order_id": item.order.order_id if item.order is not None else None,
                "quote": None if item.quote is None else _report_quote(item.quote),
                "complete_queue_record_hash": canonical_sha256(item.model_dump(mode="json")),
            } for item in replay.queue_records))
        _table(doc, "原逐日完整报价凭据", tuple(item.model_dump(mode="json", exclude={"price_proofs"}) | {
            "price_proofs": tuple({"entry_signal_id": proof.entry_signal_id, "quote": _report_quote(proof.quote)}
                for proof in item.price_proofs),
            "complete_daily_record_hash": canonical_sha256(item.model_dump(mode="json")),
        } for item in replay.daily_valuations))
    else:
        _table(doc, "原完整信号", tuple(item.model_dump(mode="json") for item in replay.signals))
        _table(doc, "原完整订单与成交", {"orders": tuple(item.model_dump(mode="json") for item in replay.orders),
            "fills": tuple(item.model_dump(mode="json") for item in replay.fills),
            "paper_queue": tuple(item.model_dump(mode="json") for item in replay.queue_records)})
        _table(doc, "原逐日完整报价凭据", tuple(item.model_dump(mode="json") for item in replay.daily_valuations))
    _table(doc, "原执行口径", replay.execution_profile.model_dump(mode="json"))
    if parameter_result:
        _table(doc, "完整参数与定义语义", {"parameters": runtime.parameters.model_dump(mode="json"),
            "evaluator_semantic_version": runtime.parameters.evaluator_semantic_version})
    elif type(owner) is MinuteExperimentSealedOwnerProof:
        _table(doc, "原完整策略定义与参数", sealed.result.publication.frozen.native_registration.spec.model_dump(mode="json"))
    if type(owner) is MinuteExperimentSealedOwnerProof:
        _table(doc, "原来源与真实发布时间", provenance.model_dump(mode="json",
            exclude={"capture_lineage", "publication_evidence"}) | {
                "capture_lineage_count": len(provenance.capture_lineage),
                "capture_lineage_sha256": canonical_sha256(tuple(
                    item.model_dump(mode="json") for item in provenance.capture_lineage)),
                "publication_evidence_count": len(provenance.publication_evidence),
                "publication_evidence_sha256": canonical_sha256(tuple(
                    item.model_dump(mode="json") for item in provenance.publication_evidence)),
            })
        doc.add("<p>完整 ZIP 保存同一封存结果的全部八表，可查看完整来源记录。HTML 中每条信号、订单、成交、队列状态、报价和每日净值均保留原值；重复的内部来源凭据以完整摘要表示。</p>")
    else:
        _table(doc, "原来源与真实发布时间", provenance.model_dump(mode="json"))
    _table(doc, "完整封存与原提交身份", {"job_id": str(sealed.job_id), "spec_hash": sealed.spec_hash,
        "payload_hash": sealed.payload_hash, "plan_hash": sealed.plan_hash, "manifest_hash": sealed.manifest_hash,
        "complete_result_hash": sealed.complete_result_hash, "full_input_hash": sealed.full_input_hash,
        "core_input_hash": sealed.core_input_hash, "seed_hash": sealed.seed_hash,
        "native_registration_hash": sealed.result.native_registration_hash,
        "wrapper_registration_hash": sealed.result.wrapper_registration_hash,
        "owner_binding": owner.model_dump(mode="json")})
    doc.add("</details>")
    raw = doc.finish()
    return MinuteHtmlReport(job_id=sealed.job_id, result_hash=sealed.complete_result_hash,
        full_input_hash=sealed.full_input_hash, core_input_hash=sealed.core_input_hash, seed_hash=sealed.seed_hash,
        owner_binding_hash=owner.content_sha256, performance=performance, html=raw.decode("utf-8"),
        html_sha256=hashlib.sha256(raw).hexdigest())
