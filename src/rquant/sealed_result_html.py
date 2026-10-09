"""Bounded offline views of original sealed values, without research calculations.

The original reader must verify the complete artifact and its trusted owner before
supplying these models/facts. Typed input does not prove private-file authority.
No file reader, export store, route, queue, broker, risk evaluator or projector is
installed here. Portfolio HTML is reused byte-for-byte after validation.
"""

from __future__ import annotations

import hashlib
import math
import re
from collections.abc import Iterator, Sequence
from datetime import date, datetime
from decimal import Decimal, localcontext
from enum import Enum
from html import escape
from html.parser import HTMLParser
from typing import TYPE_CHECKING, TypeAlias

from pydantic import BaseModel

from rquant.runtime_contracts import canonical_sha256
from rquant.sealed_result_ownership import (
    SealedArtifactFact,
    SealedOwnerBinding,
    require_owned_sealed_result,
)
from rquant.strict_json import canonical_json_bytes

if TYPE_CHECKING:
    from rquant.factor.display_artifact import FactorDisplayArtifactV1
    from rquant.factor.stream_job_artifact import FactorStreamDisplayArtifact
    from rquant.strategy_template_run import StrategyTemplateResult

MAX_HTML_BYTES = 4 * 1024 * 1024
MAX_ZIP_BYTES = 32 * 1024 * 1024
MAX_SERVING_CELL_BYTES = 64 * 1024
MAX_SERVING_OWNER_BYTES = 7 * 1024 * 1024
MAX_DISPLAY_DAYS = 1024
Point: TypeAlias = tuple[str, float | Decimal | None]

_CSS = """
*{box-sizing:border-box}body{margin:0;background:#f6f7f9;color:#18212b;font:16px/1.6 system-ui,sans-serif}
main{max-width:1080px;margin:auto;padding:24px}h1,h2{line-height:1.3}h2{font-size:20px}
section{margin:24px 0}svg{display:block;width:100%;height:auto;background:white;border:1px solid #d8dfe5}
table{border-collapse:collapse;min-width:100%;background:white}th,td{padding:8px 12px;border:1px solid #d8dfe5;text-align:left;vertical-align:top}
td{font-variant-numeric:tabular-nums;overflow-wrap:anywhere}th{font-weight:600}caption{text-align:left;padding:8px 0}
.scroll{overflow:auto}.scroll:focus,summary:focus{outline:3px solid #1769b0;outline-offset:3px}
summary{cursor:pointer;padding:12px;background:#eaf0f6}code{overflow-wrap:anywhere}p{margin:8px 0}
@media(max-width:480px){main{padding:12px}h1{font-size:24px}th,td{padding:6px 8px}}
"""
_LABELS: dict[str, str] = {
    "schema_version": "合同版本",
    "factor_id": "因子",
    "factor_version": "因子版本",
    "definition": "定义",
    "version": "版本",
    "name": "名称",
    "category": "类别",
    "expression": "表达式",
    "direction": "方向",
    "dependency_columns": "依赖列",
    "feature_catalog": "特征目录",
    "earliest_available_date": "最早可用日",
    "pool_label": "股票范围",
    "selection": "股票范围代码",
    "neutralization": "中性化",
    "mad_multiple": "极值倍数",
    "holding_sessions": "持有交易日",
    "return_price_basis": "价格口径",
    "source_mode": "来源模式",
    "source_read_boundary": "读取边界",
    "visibility_basis": "可见性依据",
    "as_of": "截至时间",
    "snapshot_as_of_time": "快照时间",
    "snapshot_id": "快照摘要",
    "binding_hash": "绑定摘要",
    "source_sha256": "来源摘要",
    "code_revision": "代码版本",
    "input_sha256": "输入摘要",
    "definition_content_sha256": "定义摘要",
    "full_artifact_sha256": "完整结果摘要",
    "result_sha256": "结果摘要",
    "content_sha256": "显示摘要",
    "summary_status": "评估状态",
    "portfolio_status": "分组状态",
    "normal_ic": "普通 IC",
    "rank_ic": "秩 IC",
    "ic_summary": "IC 汇总",
    "ic_points": "逐日 IC",
    "ic_cumulative_kind": "累计 IC 口径",
    "normal_ic_cumulative_sum": "普通 IC 累计和",
    "rank_ic_cumulative_sum": "秩 IC 累计和",
    "decision_date": "决策日",
    "trade_date": "交易日",
    "decision_at": "决策时间",
    "return_end_at": "收益窗口末时间",
    "status": "状态",
    "value": "原值",
    "source_sample_count": "来源样本数",
    "effective_sample_count": "有效样本数",
    "source_day_count": "来源日数",
    "valid_day_count": "有效日数",
    "insufficient_day_count": "样本不足日数",
    "zero_variance_day_count": "零方差日数",
    "mean": "均值",
    "sample_std": "样本标准差",
    "ir": "信息比",
    "positive_rate": "正值比例",
    "strong_signal_rate": "强信号比例",
    "t_value": "t 值",
    "p_value": "p 值",
    "skewness": "偏度",
    "excess_kurtosis": "超额峰度",
    "lag": "滞后交易日",
    "valid_pair_count": "有效配对数",
    "coverage": "覆盖",
    "expected_count": "应有数",
    "valid_count": "有效数",
    "factor_missing_count": "因子缺失数",
    "return_missing_count": "收益缺失数",
    "factor_missing_by_reason": "因子缺失原因",
    "return_missing_by_reason": "收益缺失原因",
    "reason": "原因",
    "count": "数量",
    "groupings": "分组",
    "group_count": "组数",
    "groups": "各组",
    "group_number": "组号",
    "member_count": "成员数",
    "period_return": "本期收益",
    "cumulative_return": "累计收益",
    "target_weight_turnover": "目标权重换手",
    "long_short_return": "本期多空差",
    "long_short_cumulative_spread": "累计多空差",
    "cumulative_status": "累计状态",
    "owner_id": "所属用户",
    "strategy_id": "策略",
    "contract": "结果合同",
    "execution_convention": "执行口径",
    "definition_fingerprint": "定义指纹",
    "definition_record_hash": "定义记录摘要",
    "input_hash": "输入摘要",
    "calendar_source_identity": "交易日历摘要",
    "cost_spec_id": "成本口径摘要",
    "content_hash": "结果摘要",
    "days": "逐日账本",
    "exit_decisions": "退出决定",
    "rebalanced": "是否再平衡",
    "decisions": "决定",
    "orders": "订单",
    "skipped": "跳过目标",
    "fees": "费用",
    "account": "账户",
    "market_value": "持仓市值",
    "daily_return": "日收益",
    "normalized_nav": "归一净值",
    "incomplete_reason": "未完成原因",
    "risk": "原风险记录",
    "account_id": "账户标识",
    "as_of_time": "估值时间",
    "cash": "现金",
    "available_cash": "可用现金",
    "frozen_cash": "冻结现金",
    "holdings": "持仓",
    "realized_pnl": "已实现盈亏",
    "unrealized_pnl": "未实现盈亏",
    "nav": "净值",
    "ts_code": "证券代码",
    "code": "证券代码",
    "quantity": "数量",
    "average_cost": "平均成本",
    "market_price": "市价",
    "entry_signal_id": "入场信号",
    "decision_id": "决定标识",
    "decided_at": "决定时间",
    "previous_trade_date": "前一交易日",
    "ic_method": "IC 方法",
    "industry_status": "行业状态",
    "industry_reason": "行业不可用原因",
    "industry_summaries": "行业汇总",
    "industry_coverage_days": "行业覆盖",
    "autocorrelation_points": "秩自相关",
    "l1_code": "行业代码",
    "l1_name": "行业名称",
    "panel_date": "面板日",
    "computation_stock_count": "计算股票数",
    "counts": "覆盖计数",
    "column": "列",
    "valid": "有效数",
    "missing": "缺失数",
    "null": "空值数",
    "non_finite": "非有限值数",
    "market_temperature_values": "市场温度原值",
    "auction_values": "竞价原值",
    "volume_profile_values": "筹码原值",
    "stock_code": "股票代码",
    "values": "原值",
    "fields": "字段",
    "dtype": "类型",
    "unit": "单位",
    "label": "名称",
}
_PRIVATE_KEYS = frozenset(
    {"path", "archive_path", "source_path", "file_path", "root_path", "directory"}
)


def require_export_capacity(
    *,
    html_bytes: int = 0,
    zip_bytes: int = 0,
    cell_bytes: int = 0,
    owner_bytes: int = 0,
) -> None:
    """Check measured sizes; the shared adapter must supply final owner/store totals."""
    for value, limit in (
        (html_bytes, MAX_HTML_BYTES),
        (zip_bytes, MAX_ZIP_BYTES),
        (cell_bytes, MAX_SERVING_CELL_BYTES),
        (owner_bytes, MAX_SERVING_OWNER_BYTES),
    ):
        if type(value) is not int or not 0 <= value <= limit:
            raise ValueError("export exceeds original capacity")


def _css_safe(value: str) -> None:
    # Generated CSS has no escapes. Reject escapes rather than interpret CSS URLs.
    value = re.sub(r"/\*.*?\*/", "", value, flags=re.S).lower()
    if "\\" in value or re.search(
        r"url\s*\(|expression\s*\(|behavior\s*:|-moz-binding|javascript:|data:|https?:|file:",
        value,
    ):
        raise ValueError("offline HTML cannot load or execute CSS resources")
    if re.search(r"@(?!media\b)", value):
        raise ValueError("offline HTML cannot import fonts or styles")


_PORTFOLIO_CSP = (
    "default-src 'none'; style-src 'unsafe-inline'; script-src 'none'; "
    "font-src 'none'; base-uri 'none'; form-action 'none'"
)


class _OfflineHTML(HTMLParser):
    _TAGS = frozenset(
        "html head meta title style body main header footer section div p span strong em small i code pre details summary h1 h2 h3 h4 h5 h6 table caption thead tbody tfoot tr th td ul ol li dl dt dd figure figcaption br hr svg g path line polyline circle rect text tspan desc".split()
    )
    _VOID = frozenset({"meta", "br", "hr"})
    _ATTRS = frozenset(
        "lang dir class id role aria-label aria-labelledby aria-describedby tabindex scope colspan rowspan charset name content style viewbox width height x y x1 x2 y1 y2 cx cy r d points fill stroke stroke-width font-size text-anchor preserveaspectratio data-field data-value".split()
    )
    _TAG_ATTRS = {
        "g": frozenset({"data-series"}),
        "td": frozenset({"data-label"}),
        "meta": frozenset({"http-equiv"}),
    }

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.stack: list[str] = []
        self.seen: list[str] = []
        self.csp_seen = False

    def handle_decl(self, decl: str) -> None:
        if decl.lower() != "doctype html":
            raise ValueError("unsupported HTML declaration")

    def unknown_decl(self, data: str) -> None:
        raise ValueError("unsupported HTML declaration")

    def handle_pi(self, data: str) -> None:
        raise ValueError("offline HTML cannot contain processing instructions")

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag not in self._TAGS:
            raise ValueError("offline HTML contains an active or unsupported element")
        names = [name for name, _ in attrs]
        if len(names) != len(set(names)):
            raise ValueError("duplicate HTML attribute")
        allowed = self._ATTRS | self._TAG_ATTRS.get(tag, frozenset())
        for name, value in attrs:
            if name not in allowed or value is None:
                raise ValueError("offline HTML contains an active or unsupported attribute")
            if name in {"style", "fill", "stroke"}:
                _css_safe(value)
            if tag == "meta" and name not in {"charset", "name", "content", "http-equiv"}:
                raise ValueError("offline HTML cannot redirect or declare active headers")
            if tag == "meta" and name == "charset" and value.lower() != "utf-8":
                raise ValueError("offline HTML must retain its UTF-8 encoding")
        if tag == "meta" and "http-equiv" in names:
            header = dict(attrs)
            # Only the original static report's complete CSP is a supported header.
            if (
                self.csp_seen
                or self.stack != ["html", "head"]
                or set(header) != {"http-equiv", "content"}
                or header["http-equiv"].lower() != "content-security-policy"
                or header["content"] != _PORTFOLIO_CSP
            ):
                raise ValueError("offline HTML header differs from the original CSP")
            self.csp_seen = True
        self.seen.append(tag)
        if tag not in self._VOID:
            self.stack.append(tag)

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.handle_starttag(tag, attrs)
        if tag not in self._VOID:
            self.handle_endtag(tag)

    def handle_endtag(self, tag: str) -> None:
        if not self.stack or self.stack[-1] != tag:
            raise ValueError("offline HTML structure differs")
        self.stack.pop()

    def handle_data(self, data: str) -> None:
        if self.stack and self.stack[-1] == "style":
            _css_safe(data)


def validate_offline_html(content: bytes, *, max_bytes: int = MAX_HTML_BYTES) -> None:
    if type(max_bytes) is not int or not 1 <= max_bytes <= MAX_HTML_BYTES:
        raise ValueError("HTML budget cannot exceed the original limit")
    if not isinstance(content, bytes) or not content or len(content) > max_bytes:
        raise ValueError("HTML is absent or exceeds capacity")
    text = content.decode("utf-8", errors="strict")
    if "\x00" in text:
        raise ValueError("HTML contains a NUL")
    parser = _OfflineHTML()
    parser.feed(text)
    parser.close()
    if parser.stack or any(
        parser.seen.count(tag) != 1 for tag in ("html", "head", "body")
    ):
        raise ValueError("HTML must be one complete self-contained document")


class _Document:
    def __init__(self, title: str, max_bytes: int) -> None:
        if type(max_bytes) is not int or not 1 <= max_bytes <= MAX_HTML_BYTES:
            raise ValueError("HTML budget cannot exceed the original limit")
        self.limit = max_bytes
        self.size = 0
        self.parts: list[bytes] = []
        title = _text(title)
        self.add(
            '<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">'
            '<meta http-equiv="Content-Security-Policy" content="'
            + _PORTFOLIO_CSP
            + '"><meta name="viewport" content="width=device-width,initial-scale=1"><title>'
        )
        self.add(escape(title, quote=True))
        self.add(
            "</title><style>"
            + _CSS
            + "</style></head><body><main><h1>"
            + escape(title, quote=True)
            + "</h1>"
        )
        self.add("<p>本页保存封存结果的原值。缺失值显示为 —。本页可离线查看。</p>")

    def add(self, text: str) -> None:
        data = text.encode("utf-8")
        if self.size + len(data) > self.limit:
            raise ValueError("whole HTML export exceeds capacity")
        self.parts.append(data)
        self.size += len(data)

    def finish(self) -> bytes:
        self.add("</main></body></html>")
        result = b"".join(self.parts)
        validate_offline_html(result, max_bytes=self.limit)
        return result


def _text(value: object) -> str:
    if value is None:
        return "—"
    if isinstance(value, Enum):
        return _text(value.value)
    if isinstance(value, (datetime, date)):
        text = value.isoformat()
    elif type(value) is bool:
        text = "true" if value else "false"
    elif isinstance(value, (str, int, float, Decimal)):
        if (
            isinstance(value, float)
            and not math.isfinite(value)
            or isinstance(value, Decimal)
            and not value.is_finite()
        ):
            raise ValueError("original display value is non-finite")
        text = str(value)
    else:
        raise ValueError("unsupported original scalar")
    require_export_capacity(cell_bytes=len(text.encode("utf-8")))
    return text


def _leaves(value: object, path: str = "") -> Iterator[tuple[str, object]]:
    if isinstance(value, BaseModel):
        for name in type(value).model_fields:
            if name not in _PRIVATE_KEYS:
                yield from _leaves(
                    getattr(value, name), f"{path}.{name}" if path else name
                )
    elif isinstance(value, dict):
        for name, child in value.items():
            if not isinstance(name, str):
                raise ValueError("original table key is not a string")
            if name not in _PRIVATE_KEYS:
                yield from _leaves(child, f"{path}.{name}" if path else name)
        if not value:
            yield path, None
    elif isinstance(value, (tuple, list)):
        for index, child in enumerate(value):
            yield from _leaves(child, f"{path}[{index + 1}]")
        if not value:
            yield path, None
    else:
        yield path, value


def _label(path: str) -> str:
    return (
        ".".join(
            _LABELS.get(part.split("[", 1)[0], part.split("[", 1)[0])
            + ("[" + part.split("[", 1)[1] if "[" in part else "")
            for part in path.split(".")
        )
        or "原值"
    )


def _table(doc: _Document, title: str, values: object) -> None:
    heading = escape(_text(title), quote=True)
    doc.add(
        "<section><h2>"
        + heading
        + '</h2><div class="scroll" tabindex="0" role="region" aria-label="'
        + heading
        + '"><table><caption>逐项封存原值</caption><thead><tr><th scope="col">字段</th><th scope="col">原值</th></tr></thead><tbody>'
    )
    for path, value in _leaves(values):
        doc.add(
            '<tr><th scope="row">'
            + escape(_text(_label(path)), quote=True)
            + '</th><td data-field="'
            + escape(_text(path), quote=True)
            + '">'
            + escape(_text(value), quote=True)
            + "</td></tr>"
        )
    doc.add("</tbody></table></div></section>")


def _chart(doc: _Document, title: str, points: Sequence[Point]) -> None:
    """Only SVG coordinates are scaled; every diagnostic value stays unchanged."""
    values = [Decimal(_text(value)) for _, value in points if value is not None]
    if not values:
        doc.add("<p>" + escape(title, quote=True) + "：—</p>")
        return
    lo, hi = min(values), max(values)
    heading = escape(title, quote=True)
    doc.add(
        "<section><h2>"
        + heading
        + '</h2><svg viewBox="0 0 720 240" role="img" aria-label="'
        + heading
        + '"><title>'
        + heading
        + '</title><desc>图示使用封存原值。缺失点保留间断。完整数字见下方表格。</desc><line x1="40" y1="200" x2="680" y2="200" stroke="#687586"/><text x="40" y="20" font-size="12">'
        + escape(str(hi), quote=True)
        + '</text><text x="40" y="195" font-size="12">'
        + escape(str(lo), quote=True)
        + "</text>"
    )
    segment: list[str] = []

    def finish_segment() -> None:
        if segment:
            doc.add(
                '<polyline fill="none" stroke="#1769b0" stroke-width="2" points="'
                + " ".join(segment)
                + '"/>'
            )
            segment.clear()

    with localcontext() as context:
        context.prec = 50
        for index, (day, value) in enumerate(points):
            if value is None:
                finish_segment()
                continue
            number = Decimal(_text(value))
            x = Decimal(40) + Decimal(640) * index / max(1, len(points) - 1)
            y = (
                Decimal(110)
                if hi == lo
                else Decimal(200) - Decimal(160) * (number - lo) / (hi - lo)
            )
            coordinate = f"{x:.2f},{y:.2f}"
            segment.append(coordinate)
            original = escape(_text(value), quote=True)
            doc.add(
                f'<circle cx="{x:.2f}" cy="{y:.2f}" r="2" fill="#1769b0" data-value="{original}"><title>'
                + escape(_text(day), quote=True)
                + "："
                + original
                + "</title></circle>"
            )
        finish_segment()
    doc.add(
        '<text x="40" y="228" font-size="12">'
        + escape(_text(points[0][0]), quote=True)
        + '</text><text x="680" y="228" font-size="12" text-anchor="end">'
        + escape(_text(points[-1][0]), quote=True)
        + "</text></svg></section>"
    )


def render_factor_html(
    display: FactorDisplayArtifactV1 | FactorStreamDisplayArtifact,
    *,
    binding: SealedOwnerBinding | None,
    current_artifact: SealedArtifactFact,
    requester: str,
    title: str = "因子封存结果",
    max_bytes: int = MAX_HTML_BYTES,
) -> bytes:
    from rquant.factor.display_artifact import FactorDisplayArtifactV1
    from rquant.factor.stream_job_artifact import FactorStreamDisplayArtifact

    if type(display) not in (FactorDisplayArtifactV1, FactorStreamDisplayArtifact):
        raise ValueError("export needs the original complete factor display contract")
    owned = require_owned_sealed_result(
        binding, requester=requester, current_artifact=current_artifact
    )
    if owned.domain != "factor":
        raise PermissionError("sealed result is not a factor result")
    # Digest-only verification: never call the full result validator/projector.
    data = display.model_dump(mode="json", round_trip=True, exclude={"content_sha256"})
    if hashlib.sha256(canonical_json_bytes(data)).hexdigest() != display.content_sha256:
        raise ValueError("factor display digest differs")
    if (
        hashlib.sha256(
            canonical_json_bytes(display.definition.model_dump(mode="json"))
        ).hexdigest()
        != display.definition_content_sha256
    ):
        raise ValueError("factor definition digest differs")
    if (display.factor_id, display.factor_version) != (
        display.definition.factor_id,
        display.definition.version,
    ):
        raise ValueError("original factor definition differs")
    if (
        display.full_artifact_sha256,
        display.result_sha256,
        display.input_sha256,
        display.content_sha256,
    ) != (
        owned.full_artifact_hash,
        owned.result_payload_hash,
        owned.input_hash,
        owned.display_hash,
    ):
        raise PermissionError("display does not bind the current full sealed result")
    if (
        not 1 <= len(display.ic_points) <= MAX_DISPLAY_DAYS
        or not 1 <= len(display.coverage_days) <= MAX_DISPLAY_DAYS
        or len(display.decay_periods) != 10
        or len(display.portfolio_days) > MAX_DISPLAY_DAYS
    ):
        raise ValueError("factor display grid exceeds original bounds")
    doc = _Document(title, max_bytes)
    doc.add("<p>累计 IC 是有效日 IC 的和。分组累计多空差为研究诊断值。</p>")
    _table(doc, "IC 汇总", display.ic_summary)
    _chart(
        doc,
        "普通 IC",
        tuple(
            (
                p.decision_date.isoformat(),
                p.normal_ic.value if p.normal_ic is not None else None,
            )
            for p in display.ic_points
        ),
    )
    _chart(
        doc,
        "秩 IC",
        tuple(
            (
                p.decision_date.isoformat(),
                p.rank_ic.value if p.rank_ic is not None else None,
            )
            for p in display.ic_points
        ),
    )
    _chart(
        doc,
        "普通 IC 累计和",
        tuple(
            (p.decision_date.isoformat(), p.normal_ic_cumulative_sum)
            for p in display.ic_points
        ),
    )
    _chart(
        doc,
        "秩 IC 累计和",
        tuple(
            (p.decision_date.isoformat(), p.rank_ic_cumulative_sum)
            for p in display.ic_points
        ),
    )
    _table(doc, "逐日 IC", display.ic_points)
    _table(doc, "IC 衰减", display.decay_periods)
    for count in (3, 5, 10):
        points = tuple(
            (
                day.decision_date.isoformat(),
                next(
                    (
                        g.long_short_cumulative_spread
                        for g in day.groupings
                        if g.group_count == count
                    ),
                    None,
                ),
            )
            for day in display.portfolio_days
        )
        _chart(doc, f"{count} 组累计多空差", points)
    _table(doc, "分组原值", display.portfolio_days)
    _table(doc, "覆盖", display.coverage_days)
    if isinstance(display, FactorStreamDisplayArtifact):
        _table(doc, "扩展诊断", display.extended_statistics)
        _table(doc, "逐日特征覆盖与原值", display.daily_feature_coverage_days)
    doc.add("<details><summary>查看封存参数</summary>")
    metadata = {
        name: getattr(display, name)
        for name in type(display).model_fields
        if name
        not in {
            "ic_summary",
            "ic_points",
            "decay_periods",
            "portfolio_days",
            "coverage_days",
            "extended_statistics",
            "daily_feature_coverage_days",
        }
    }
    _table(doc, "封存参数", metadata)
    doc.add("</details>")
    return doc.finish()


def render_strategy_html(
    result: StrategyTemplateResult,
    *,
    binding: SealedOwnerBinding | None,
    current_artifact: SealedArtifactFact,
    requester: str,
    title: str = "策略封存结果",
    max_bytes: int = MAX_HTML_BYTES,
) -> bytes:
    from rquant.strategy_template_run import StrategyTemplateResult

    if type(result) is not StrategyTemplateResult:
        raise ValueError("export needs the original complete strategy result contract")
    owned = require_owned_sealed_result(
        binding, requester=requester, current_artifact=current_artifact
    )
    if owned.domain != "strategy" or result.owner_id != owned.owner_id:
        raise PermissionError("strategy belongs to a different original owner")
    # Avoid model revalidation: account/risk validators may repeat aggregate math.
    if result.content_hash != canonical_sha256(
        result.model_dump(mode="python", exclude={"content_hash"})
    ):
        raise ValueError("strategy result digest differs")
    if (result.input_hash, result.content_hash) != (
        owned.input_hash,
        owned.result_payload_hash,
    ):
        raise PermissionError("strategy does not bind the current full sealed result")
    if (
        result.status != "complete"
        or not 1 <= len(result.days) <= MAX_DISPLAY_DAYS
        or any(
            day.account is None or day.incomplete_reason is not None
            for day in result.days
        )
    ):
        raise ValueError("strategy export needs the complete sealed daily ledger")
    doc = _Document(title, max_bytes)
    doc.add("<p>图表使用原账本净值。本页不计算新的收益、风险或汇总。</p>")
    _chart(
        doc,
        "归一净值",
        tuple((day.trade_date.isoformat(), day.normalized_nav) for day in result.days),
    )
    _chart(
        doc,
        "原账户净值",
        tuple((day.trade_date.isoformat(), day.account.nav) for day in result.days),
    )
    _table(doc, "逐日账本与订单", result.days)
    _table(doc, "退出决定", result.exit_decisions)
    doc.add("<details><summary>查看封存参数</summary>")
    _table(
        doc,
        "封存参数",
        {
            name: getattr(result, name)
            for name in type(result).model_fields
            if name not in {"days", "exit_decisions"}
        },
    )
    doc.add("</details>")
    return doc.finish()


def reuse_portfolio_html(
    content: bytes,
    *,
    expected_sha256: str,
    binding: SealedOwnerBinding | None,
    current_artifact: SealedArtifactFact,
    requester: str,
) -> bytes:
    owned = require_owned_sealed_result(
        binding, requester=requester, current_artifact=current_artifact
    )
    if owned.domain != "portfolio":
        raise PermissionError("sealed result is not a portfolio result")
    if owned.html_sha256 is None or expected_sha256 != owned.html_sha256:
        raise ValueError(
            "HTML digest is not the original verified portfolio bundle digest"
        )
    if (
        not isinstance(content, bytes)
        or hashlib.sha256(content).hexdigest() != owned.html_sha256
    ):
        raise ValueError("original portfolio HTML digest differs")
    validate_offline_html(content)
    return content
