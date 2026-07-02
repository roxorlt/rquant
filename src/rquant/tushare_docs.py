"""Tushare official-document parsing and strategy audit helpers."""

from __future__ import annotations

import json
import re
from datetime import datetime
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

import duckdb
from pydantic import BaseModel, Field

TUSHARE_DOC_BASE_URL = "https://tushare.pro/document/2"


class TushareDocField(BaseModel):
    name: str
    type: str | None = None
    required: str | None = None
    default_display: str | None = None
    description: str | None = None


class TushareDocLink(BaseModel):
    doc_id: int
    title: str
    href: str
    category_path: list[str]
    is_section: bool = False

    @property
    def url(self) -> str:
        return f"{TUSHARE_DOC_BASE_URL}?doc_id={self.doc_id}"


class TushareDocPage(BaseModel):
    doc_id: int
    title: str
    category_path: list[str]
    is_section: bool
    doc_url: str
    api_name: str | None = None
    description: str | None = None
    limit_note: str | None = None
    permission_note: str | None = None
    history_coverage_type: str = "unknown"
    history_start: str | None = None
    history_coverage_note: str | None = None
    input_fields: list[TushareDocField] = Field(default_factory=list)
    output_fields: list[TushareDocField] = Field(default_factory=list)
    raw_text: str = ""


class TushareMcpTool(BaseModel):
    name: str
    description: str = ""
    input_schema: dict[str, Any] = Field(default_factory=dict)


class TusharePurchaseGood(BaseModel):
    good_id: int
    good_type: int
    name: str
    description: str | None = None
    price: float | None = None
    duration_unit: str | None = None
    doc_url: str | None = None
    doc_id: int | None = None
    api_count: int | None = None
    api_names: list[str] = Field(default_factory=list)
    api_doc_ids: list[int] = Field(default_factory=list)
    raw_payload: dict[str, Any] = Field(default_factory=dict)
    fetched_at: datetime | None = None


class TushareAccountScore(BaseModel):
    scores: float
    raw_payload: dict[str, Any] = Field(default_factory=dict)
    fetched_at: datetime | None = None


class TushareActivityPackage(BaseModel):
    package_id: int
    name: str
    description: str | None = None
    price: float | None = None
    duration_unit: str | None = None
    raw_payload: dict[str, Any] = Field(default_factory=dict)
    fetched_at: datetime | None = None


class TushareAuditRow(BaseModel):
    doc_id: int
    title: str
    api_name: str | None
    category_path: list[str]
    is_section: bool
    is_a_share_related: bool
    priority: int
    capability_tags: list[str]
    strategy_uses: list[str]
    integration_status: str
    description: str | None = None
    limit_note: str | None = None
    permission_note: str | None = None
    history_coverage_type: str = "unknown"
    history_start: str | None = None
    history_coverage_note: str | None = None
    mcp_name: str | None = None
    mcp_description: str | None = None
    input_fields: list[TushareDocField] = Field(default_factory=list)
    output_fields: list[TushareDocField] = Field(default_factory=list)
    doc_url: str
    fetched_at: datetime | None = None


class TushareInterfaceCatalogRow(BaseModel):
    doc_id: int
    title: str
    api_name: str | None
    category_path: list[str]
    priority: int
    integration_status: str
    integration_stage: str
    update_cadence: str
    target_table_hint: str
    permission_level: str
    strategy_value: str
    capability_tags: list[str]
    strategy_uses: list[str]
    limit_note: str | None = None
    permission_note: str | None = None
    history_coverage_type: str = "unknown"
    history_start: str | None = None
    history_coverage_note: str | None = None
    doc_url: str
    fetched_at: datetime | None = None


class _MenuNode:
    def __init__(self, doc_id: int, title: str, href: str) -> None:
        self.doc_id = doc_id
        self.title = title
        self.href = href
        self.children: list[_MenuNode] = []


def _normalize_text(text: str) -> str:
    return " ".join(unescape(text).replace("\xa0", " ").split())


def _attrs(attrs: list[tuple[str, str | None]]) -> dict[str, str]:
    return {key: value or "" for key, value in attrs}


def _parse_doc_id(href: str) -> int | None:
    match = re.search(r"doc_id=(\d+)", href)
    if not match:
        return None
    return int(match.group(1))


class _MenuParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=False)
        self.roots: list[_MenuNode] = []
        self._in_jstree = False
        self._jstree_div_depth = 0
        self._li_stack: list[_MenuNode | None] = []
        self._capture_href: str | None = None
        self._capture_text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = _attrs(attrs)
        if tag == "div":
            if self._in_jstree:
                self._jstree_div_depth += 1
            elif attr_map.get("id") == "jstree":
                self._in_jstree = True
                self._jstree_div_depth = 1
            return

        if not self._in_jstree:
            return

        if tag == "li":
            self._li_stack.append(None)
        elif tag == "a" and self._li_stack:
            href = attr_map.get("href", "")
            if _parse_doc_id(href) is not None:
                self._capture_href = href
                self._capture_text = []

    def handle_data(self, data: str) -> None:
        if self._capture_href is not None:
            self._capture_text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "a" and self._capture_href is not None and self._li_stack:
            doc_id = _parse_doc_id(self._capture_href)
            title = _normalize_text("".join(self._capture_text))
            if doc_id is not None and title:
                node = _MenuNode(doc_id=doc_id, title=title, href=self._capture_href)
                parent = next((item for item in reversed(self._li_stack[:-1]) if item), None)
                if parent:
                    parent.children.append(node)
                else:
                    self.roots.append(node)
                self._li_stack[-1] = node
            self._capture_href = None
            self._capture_text = []
            return

        if tag == "li" and self._in_jstree and self._li_stack:
            self._li_stack.pop()
            return

        if tag == "div" and self._in_jstree:
            self._jstree_div_depth -= 1
            if self._jstree_div_depth <= 0:
                self._in_jstree = False


def _flatten_menu(nodes: list[_MenuNode], prefix: list[str] | None = None) -> list[TushareDocLink]:
    prefix = prefix or []
    out: list[TushareDocLink] = []
    for node in nodes:
        path = [*prefix, node.title]
        out.append(
            TushareDocLink(
                doc_id=node.doc_id,
                title=node.title,
                href=node.href,
                category_path=path,
                is_section=bool(node.children),
            )
        )
        out.extend(_flatten_menu(node.children, path))
    return out


def parse_tushare_menu(html: str) -> list[TushareDocLink]:
    parser = _MenuParser()
    parser.feed(html)
    return _flatten_menu(parser.roots)


class _TableBlock(BaseModel):
    label: str | None
    rows: list[list[str]]


class _ContentParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=False)
        self.title: str | None = None
        self.text_parts: list[str] = []
        self.tables: list[_TableBlock] = []
        self._in_content = False
        self._content_div_depth = 0
        self._heading_level: str | None = None
        self._heading_text: list[str] = []
        self._strong_text: list[str] | None = None
        self._last_strong: str | None = None
        self._table_rows: list[list[str]] | None = None
        self._table_label: str | None = None
        self._current_row: list[str] | None = None
        self._current_cell: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = _attrs(attrs)
        if tag == "div":
            if self._in_content:
                self._content_div_depth += 1
            else:
                class_name = attr_map.get("class", "")
                if "content" in class_name and "col-md-9" in class_name:
                    self._in_content = True
                    self._content_div_depth = 1
            return

        if not self._in_content:
            return

        if tag in {"p", "h1", "h2", "h3", "h4", "table"}:
            self.text_parts.append("\n")
        if tag == "br":
            self.text_parts.append("\n")
        elif tag in {"h1", "h2", "h3", "h4"}:
            self._heading_level = tag
            self._heading_text = []
        elif tag == "strong":
            self._strong_text = []
        elif tag == "table":
            self._table_rows = []
            self._table_label = self._last_strong
        elif tag == "tr" and self._table_rows is not None:
            self._current_row = []
        elif tag in {"th", "td"} and self._current_row is not None:
            self._current_cell = []

    def handle_data(self, data: str) -> None:
        if not self._in_content:
            return

        self.text_parts.append(data)
        if self._heading_level is not None:
            self._heading_text.append(data)
        if self._strong_text is not None:
            self._strong_text.append(data)
        if self._current_cell is not None:
            self._current_cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        if not self._in_content:
            return

        if tag in {"p", "h1", "h2", "h3", "h4", "table"}:
            self.text_parts.append("\n")

        if tag == self._heading_level:
            heading = _normalize_text("".join(self._heading_text))
            if heading and self.title is None:
                self.title = heading
            self._heading_level = None
            self._heading_text = []
        elif tag == "strong" and self._strong_text is not None:
            strong = _normalize_text("".join(self._strong_text))
            self._last_strong = strong or self._last_strong
            self._strong_text = None
        elif (
            tag in {"th", "td"}
            and self._current_cell is not None
            and self._current_row is not None
        ):
            self._current_row.append(_normalize_text("".join(self._current_cell)))
            self._current_cell = None
        elif tag == "tr" and self._current_row is not None and self._table_rows is not None:
            if any(self._current_row):
                self._table_rows.append(self._current_row)
            self._current_row = None
        elif tag == "table" and self._table_rows is not None:
            self.tables.append(_TableBlock(label=self._table_label, rows=self._table_rows))
            self._table_rows = None
            self._table_label = None
        elif tag == "div":
            self._content_div_depth -= 1
            if self._content_div_depth <= 0:
                self._in_content = False

    @property
    def normalized_text(self) -> str:
        raw = "".join(self.text_parts)
        lines = [_normalize_text(line) for line in raw.splitlines()]
        return "\n".join(line for line in lines if line)


def _extract_prefixed_line(text: str, label: str) -> str | None:
    match = re.search(rf"{re.escape(label)}[:：]\s*(.*?)(?:\n|$)", text)
    if not match:
        return None
    value = _normalize_text(match.group(1))
    return value or None


def _extract_api_name(text: str) -> str | None:
    line = _extract_prefixed_line(text, "接口")
    if not line:
        return None
    match = re.match(r"([A-Za-z_][A-Za-z0-9_]*)", line)
    return match.group(1) if match else line


def _compact_history_note(line: str) -> str:
    line = re.sub(r"^(描述|提示|说明|限量|数据说明|注)[:：]\s*", "", line)
    if "当日开盘以来" in line:
        return "仅当日开盘以来"
    keywords = ("历史", "数据开始", "数据从", "开始于", "起", "以来", "超过10年")
    for sentence in re.split(r"(?<=[。；;])", line):
        sentence = sentence.strip()
        if sentence and any(keyword in sentence for keyword in keywords):
            return sentence
    return line.strip()


def _format_history_start(match: re.Match[str]) -> str:
    value = match.group(0)
    digits = re.sub(r"\D", "", value)
    if len(digits) >= 8:
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
    if len(digits) >= 6:
        return f"{digits[:4]}-{digits[4:6]}"
    if "月" in value:
        year_month = re.search(r"((?:19|20)\d{2})年\s*(\d{1,2})月", value)
        if year_month:
            return f"{year_month.group(1)}-{int(year_month.group(2)):02d}"
    return digits[:4]


def _extract_history_start(note: str) -> str | None:
    for pattern in [
        r"(?:19|20)\d{2}[-/]\d{1,2}[-/]\d{1,2}",
        r"(?:19|20)\d{6}",
        r"(?:19|20)\d{2}年\s*\d{1,2}月\s*\d{1,2}日",
        r"(?:19|20)\d{2}年\s*\d{1,2}月",
        r"(?:19|20)\d{2}年",
    ]:
        match = re.search(pattern, note)
        if match:
            return _format_history_start(match)
    return None


def _extract_history_coverage(text: str) -> tuple[str, str | None, str | None]:
    for raw_line in text.splitlines():
        line = _normalize_text(raw_line)
        if not line:
            continue
        if "当日开盘以来" in line:
            return "intraday_today", None, "仅当日开盘以来"
        if "示例" in line and not _contains_any(line, {"数据从", "数据开始", "开始于", "历史"}):
            continue
        if line.startswith(("开始日期", "结束时间")):
            continue
        if not _contains_any(
            line,
            {
                "历史数据",
                "数据历史",
                "数据开始",
                "历史分钟数据",
                "数据从",
                "从20",
                "从19",
                "开始于",
                "起提供",
                "起，",
                "年至今",
                "超过10年",
                "可根据日期循环获取历史",
            },
        ):
            continue
        note = _compact_history_note(line)
        return "historical", _extract_history_start(note), note
    return "unknown", None, None


def _table_fields(table: _TableBlock) -> list[TushareDocField]:
    if len(table.rows) < 2:
        return []
    headers = table.rows[0]
    rows = table.rows[1:]
    try:
        name_idx = headers.index("名称")
    except ValueError:
        return []

    type_idx = headers.index("类型") if "类型" in headers else None
    required_idx = headers.index("必选") if "必选" in headers else None
    default_idx = headers.index("默认显示") if "默认显示" in headers else None
    desc_idx = headers.index("描述") if "描述" in headers else None

    fields: list[TushareDocField] = []
    for row in rows:
        if len(row) <= name_idx or not row[name_idx]:
            continue
        fields.append(
            TushareDocField(
                name=row[name_idx],
                type=row[type_idx] if type_idx is not None and len(row) > type_idx else None,
                required=row[required_idx]
                if required_idx is not None and len(row) > required_idx
                else None,
                default_display=row[default_idx]
                if default_idx is not None and len(row) > default_idx
                else None,
                description=row[desc_idx] if desc_idx is not None and len(row) > desc_idx else None,
            )
        )
    return fields


def parse_tushare_doc_page(html: str, link: TushareDocLink) -> TushareDocPage:
    parser = _ContentParser()
    parser.feed(html)
    text = parser.normalized_text

    input_fields: list[TushareDocField] = []
    output_fields: list[TushareDocField] = []
    for table in parser.tables:
        label = table.label or ""
        headers = table.rows[0] if table.rows else []
        if "输入参数" in label or "必选" in headers:
            input_fields.extend(_table_fields(table))
        elif "输出参数" in label or "默认显示" in headers:
            output_fields.extend(_table_fields(table))

    history_coverage_type, history_start, history_coverage_note = _extract_history_coverage(text)

    return TushareDocPage(
        doc_id=link.doc_id,
        title=parser.title or link.title,
        category_path=link.category_path,
        is_section=link.is_section,
        doc_url=link.url,
        api_name=_extract_api_name(text),
        description=_extract_prefixed_line(text, "描述"),
        limit_note=_extract_prefixed_line(text, "限量"),
        permission_note=_extract_prefixed_line(text, "权限"),
        history_coverage_type=history_coverage_type,
        history_start=history_start,
        history_coverage_note=history_coverage_note,
        input_fields=input_fields,
        output_fields=output_fields,
        raw_text=text,
    )


def parse_mcp_tools(payload: str | list[dict[str, Any]] | dict[str, Any]) -> list[TushareMcpTool]:
    data: Any = json.loads(payload) if isinstance(payload, str) else payload
    if isinstance(data, dict):
        tools = data.get("result", {}).get("tools", data.get("tools", []))
    else:
        tools = data
    out: list[TushareMcpTool] = []
    for tool in tools:
        if isinstance(tool, dict) and tool.get("name"):
            out.append(
                TushareMcpTool(
                    name=str(tool["name"]),
                    description=str(tool.get("description") or ""),
                    input_schema=tool.get("inputSchema") or tool.get("input_schema") or {},
                )
            )
    return out


def _payload_data(payload: str | list[dict[str, Any]] | dict[str, Any]) -> Any:
    data: Any = json.loads(payload) if isinstance(payload, str) else payload
    if isinstance(data, dict) and "data" in data:
        return data["data"]
    return data


def _unique_ints(values: list[int]) -> list[int]:
    seen: set[int] = set()
    out: list[int] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def parse_tushare_purchase_goods(
    payload: str | list[dict[str, Any]] | dict[str, Any],
    *,
    fetched_at: datetime | None = None,
) -> list[TusharePurchaseGood]:
    data = _payload_data(payload)
    if not isinstance(data, list):
        return []

    goods: list[TusharePurchaseGood] = []
    for item in data:
        if not isinstance(item, dict) or item.get("id") is None:
            continue
        api_list = item.get("api_list") if isinstance(item.get("api_list"), list) else []
        api_names: list[str] = []
        api_doc_ids: list[int] = []
        for api in api_list:
            if not isinstance(api, dict):
                continue
            api_name = api.get("api_name")
            if api_name:
                api_names.append(str(api_name))
            api_doc_id = _parse_doc_id(str(api.get("api_doc_url") or ""))
            if api_doc_id is not None:
                api_doc_ids.append(api_doc_id)
        doc_url = str(item.get("doc_url") or "") or None
        doc_id = _parse_doc_id(doc_url or "")
        if doc_id is not None:
            api_doc_ids.append(doc_id)
        goods.append(
            TusharePurchaseGood(
                good_id=int(item["id"]),
                good_type=int(item.get("type") or 0),
                name=str(item.get("name") or ""),
                description=str(item.get("desc") or "") or None,
                price=float(item["price"]) if item.get("price") is not None else None,
                duration_unit=str(item.get("duration_unit") or "") or None,
                doc_url=doc_url,
                doc_id=doc_id,
                api_count=int(item["api_count"]) if item.get("api_count") is not None else None,
                api_names=api_names,
                api_doc_ids=_unique_ints(api_doc_ids),
                raw_payload=item,
                fetched_at=fetched_at,
            )
        )
    return goods


def parse_tushare_account_score(
    payload: str | dict[str, Any],
    *,
    fetched_at: datetime | None = None,
) -> TushareAccountScore | None:
    data: Any = json.loads(payload) if isinstance(payload, str) else payload
    score_source = data.get("data") if isinstance(data, dict) else None
    if not isinstance(score_source, dict):
        score_source = data if isinstance(data, dict) else {}
    score = score_source.get("scores") or score_source.get("score")
    if score is None:
        return None
    return TushareAccountScore(scores=float(score), raw_payload=data, fetched_at=fetched_at)


def parse_tushare_activity_packages(
    payload: str | list[dict[str, Any]] | dict[str, Any],
    *,
    fetched_at: datetime | None = None,
) -> list[TushareActivityPackage]:
    data = _payload_data(payload)
    if not isinstance(data, list):
        return []
    packages: list[TushareActivityPackage] = []
    for item in data:
        if not isinstance(item, dict) or item.get("id") is None:
            continue
        packages.append(
            TushareActivityPackage(
                package_id=int(item["id"]),
                name=str(item.get("name") or ""),
                description=str(item.get("desc") or "") or None,
                price=float(item["price"]) if item.get("price") is not None else None,
                duration_unit=str(item.get("duration_unit") or "") or None,
                raw_payload=item,
                fetched_at=fetched_at,
            )
        )
    return packages


def _contains_any(text: str, keywords: set[str]) -> bool:
    return any(keyword in text for keyword in keywords)


def _capability_tags(page: TushareDocPage, mcp_tool: TushareMcpTool | None) -> list[str]:
    api = page.api_name or ""
    path_title = "/".join(page.category_path) + " " + page.title
    text = path_title + " " + (page.description or "")
    if mcp_tool:
        text += " " + mcp_tool.description

    tags: list[str] = []
    checks: list[tuple[str, bool]] = [
        (
            "intraday_realtime",
            api.startswith("rt_") or ("实时" in path_title and "分钟" in path_title),
        ),
        (
            "historical_minute",
            api.endswith("_mins") or api in {"stk_mins", "idx_mins", "sw_mins", "etf_mins"}
            or "历史分钟" in path_title
            or "分钟行情" in path_title,
        ),
        ("opening_auction", api.startswith("stk_auction") or "集合竞价" in path_title),
        ("limit_up", _contains_any(text, {"涨停", "跌停", "炸板", "连板", "打板"})),
        ("money_flow", _contains_any(text, {"资金流向", "大单", "小单", "主力净流入"})),
        ("theme_board", _contains_any(text, {"概念", "题材", "热榜", "板块"})),
        ("chip_distribution", "筹码" in text),
        ("index_context", _contains_any(text, {"指数", "申万", "中信", "市场每日交易统计"})),
        ("margin_financing", _contains_any(text, {"融资融券", "两融", "转融通"})),
        ("risk_filter", _contains_any(text, {"ST", "停复牌", "异常波动", "重点提示", "退市"})),
        ("daily_quote", _contains_any(text, {"历史日线", "实时日线", "复权", "每日指标"})),
        ("fundamental", _contains_any(text, {"利润表", "资产负债表", "现金流量表", "财务指标"})),
        ("shareholder_event", _contains_any(text, {"股东", "回购", "解禁", "增减持", "质押"})),
    ]
    for tag, enabled in checks:
        if enabled and tag not in tags:
            tags.append(tag)
    return tags


def _is_a_share_related(page: TushareDocPage, mcp_tool: TushareMcpTool | None) -> bool:
    if page.category_path:
        root = page.category_path[0]
        return root in {"股票数据", "指数专题", "ETF专题"}

    path = "/".join(page.category_path)
    text = path + " " + page.title + " " + (page.description or "")
    if mcp_tool:
        text += " " + mcp_tool.description

    if page.category_path and page.category_path[0] in {"股票数据", "指数专题", "ETF专题"}:
        return True
    return _contains_any(
        text,
        {
            "A股",
            "沪深",
            "股票",
            "涨停",
            "跌停",
            "龙虎榜",
            "申万",
            "中信",
            "ETF",
            "北交所",
        },
    )


def _strategy_uses(tags: list[str]) -> list[str]:
    mapping = {
        "intraday_realtime": "盘中准实时监控",
        "historical_minute": "历史分钟回测与90天价量分布",
        "opening_auction": "集合竞价强弱预判",
        "limit_up": "涨停/炸板/连板情绪刻画",
        "money_flow": "主力资金与大单行为验证",
        "theme_board": "题材热度与板块共振",
        "chip_distribution": "筹码密集区与压力支撑",
        "index_context": "指数/行业环境过滤",
        "margin_financing": "两融风险偏好观察",
        "risk_filter": "ST/停复牌/异常风险过滤",
        "daily_quote": "日线基础行情与复权校准",
        "fundamental": "基本面风险过滤",
        "shareholder_event": "股东事件与筹码变化过滤",
    }
    return [mapping[tag] for tag in tags if tag in mapping]


def _priority(is_a_share_related: bool, tags: list[str], is_section: bool) -> int:
    if not is_a_share_related:
        return 5
    if is_section:
        return 4
    if any(tag in tags for tag in {"intraday_realtime", "opening_auction"}):
        return 1
    if any(
        tag in tags
        for tag in {
            "historical_minute",
            "limit_up",
            "money_flow",
            "theme_board",
            "chip_distribution",
            "index_context",
        }
    ):
        return 2
    if any(
        tag in tags
        for tag in {"daily_quote", "risk_filter", "margin_financing", "fundamental"}
    ):
        return 3
    return 4


def _integration_status(api_name: str | None, priority: int) -> str:
    integrated = {"stock_basic", "daily", "adj_factor", "stk_mins", "daily_basic"}
    if api_name in integrated:
        return "already_integrated"
    if priority <= 2:
        return "recommended"
    if priority == 3:
        return "candidate"
    return "watchlist"


def classify_tushare_interface(
    page: TushareDocPage,
    mcp_tool: TushareMcpTool | None = None,
    *,
    fetched_at: datetime | None = None,
) -> TushareAuditRow:
    tags = _capability_tags(page, mcp_tool)
    related = _is_a_share_related(page, mcp_tool)
    priority = _priority(related, tags, page.is_section)
    return TushareAuditRow(
        doc_id=page.doc_id,
        title=page.title,
        api_name=page.api_name,
        category_path=page.category_path,
        is_section=page.is_section,
        is_a_share_related=related,
        priority=priority,
        capability_tags=tags,
        strategy_uses=_strategy_uses(tags),
        integration_status=_integration_status(page.api_name, priority),
        description=page.description,
        limit_note=page.limit_note,
        permission_note=page.permission_note,
        history_coverage_type=page.history_coverage_type,
        history_start=page.history_start,
        history_coverage_note=page.history_coverage_note,
        mcp_name=mcp_tool.name if mcp_tool else None,
        mcp_description=mcp_tool.description if mcp_tool else None,
        input_fields=page.input_fields,
        output_fields=page.output_fields,
        doc_url=page.doc_url,
        fetched_at=fetched_at,
    )


def _permission_level(row: TushareAuditRow) -> str:
    text = f"{row.permission_note or ''} {row.limit_note or ''}"
    if "单独" in text or "在线开通" in text or "权限说明" in text:
        return "official_permission"
    if "积分" in text:
        return "points_required"
    if row.integration_status == "already_integrated":
        return "available"
    return "unknown_or_included"


def _integration_stage(row: TushareAuditRow) -> str:
    tags = set(row.capability_tags)
    if row.integration_status == "already_integrated":
        return "stage_0_integrated"
    if tags & {"intraday_realtime", "opening_auction"}:
        return "stage_1_realtime"
    if tags & {"historical_minute", "limit_up", "money_flow", "theme_board", "chip_distribution"}:
        return "stage_2_strategy_features"
    if row.priority <= 3:
        return "stage_3_context_filters"
    return "stage_4_reference"


def _update_cadence(row: TushareAuditRow) -> str:
    tags = set(row.capability_tags)
    if "intraday_realtime" in tags:
        return "intraday_realtime"
    if "opening_auction" in tags:
        return "preopen_daily"
    if "historical_minute" in tags:
        return "historical_backfill"
    if tags & {"limit_up", "money_flow", "theme_board", "chip_distribution", "index_context"}:
        return "daily_after_close"
    if tags & {"fundamental", "shareholder_event", "risk_filter", "margin_financing"}:
        return "event_or_periodic"
    return "manual_reference"


def _target_table_hint(row: TushareAuditRow) -> str:
    tags = set(row.capability_tags)
    if "opening_auction" in tags:
        return "auction_bar"
    if "intraday_realtime" in tags or "historical_minute" in tags:
        return "minute_bar"
    if "chip_distribution" in tags:
        return "chip_distribution_daily"
    if "money_flow" in tags:
        return "moneyflow_daily"
    if "theme_board" in tags:
        return "theme_board_daily"
    if "limit_up" in tags:
        return "limit_up_event"
    if "index_context" in tags:
        return "market_context_daily"
    if "risk_filter" in tags:
        return "risk_filter_ref"
    if "fundamental" in tags:
        return "fundamental_ref"
    if "margin_financing" in tags:
        return "margin_financing_daily"
    if "shareholder_event" in tags:
        return "shareholder_event"
    if "daily_quote" in tags:
        return "daily_bar"
    return "reference_catalog"


def _strategy_value(row: TushareAuditRow) -> str:
    tags = set(row.capability_tags)
    values: list[str] = []
    if "intraday_realtime" in tags:
        values.append("盘中监控、实时触发、模拟盘")
    if "opening_auction" in tags:
        values.append("9:25-9:30 竞价强弱与预开盘过滤")
    if "historical_minute" in tags:
        values.append("分钟级无未来函数回测、价量分布回补")
    if "limit_up" in tags:
        values.append("涨停/炸板/连板情绪归因")
    if "money_flow" in tags:
        values.append("资金流强弱与主力行为验证")
    if "theme_board" in tags:
        values.append("题材强度、板块共振、热点扩散")
    if "chip_distribution" in tags:
        values.append("筹码压力支撑与止盈止损辅助")
    if "index_context" in tags:
        values.append("指数/行业环境过滤")
    if "risk_filter" in tags:
        values.append("风险剔除与异常状态识别")
    if "fundamental" in tags:
        values.append("基本面风险过滤")
    if not values:
        values.extend(row.strategy_uses or ["参考信息"])
    return "；".join(values)


def derive_interface_catalog_rows(
    rows: list[TushareAuditRow],
) -> list[TushareInterfaceCatalogRow]:
    out: list[TushareInterfaceCatalogRow] = []
    for row in rows:
        if not row.is_a_share_related or row.is_section:
            continue
        out.append(
            TushareInterfaceCatalogRow(
                doc_id=row.doc_id,
                title=row.title,
                api_name=row.api_name,
                category_path=row.category_path,
                priority=row.priority,
                integration_status=row.integration_status,
                integration_stage=_integration_stage(row),
                update_cadence=_update_cadence(row),
                target_table_hint=_target_table_hint(row),
                permission_level=_permission_level(row),
                strategy_value=_strategy_value(row),
                capability_tags=row.capability_tags,
                strategy_uses=row.strategy_uses,
                limit_note=row.limit_note,
                permission_note=row.permission_note,
                history_coverage_type=row.history_coverage_type,
                history_start=row.history_start,
                history_coverage_note=row.history_coverage_note,
                doc_url=row.doc_url,
                fetched_at=row.fetched_at,
            )
        )
    return sorted(out, key=lambda item: (item.integration_stage, item.priority, item.doc_id))


TUSHARE_INTERFACE_AUDIT_DDL = """
CREATE TABLE IF NOT EXISTS tushare_interface_audit (
    doc_id             INTEGER PRIMARY KEY,
    title              VARCHAR NOT NULL,
    api_name           VARCHAR,
    category_path      JSON,
    is_section         BOOLEAN,
    is_a_share_related BOOLEAN,
    priority           INTEGER,
    capability_tags    JSON,
    strategy_uses      JSON,
    integration_status VARCHAR,
    description        VARCHAR,
    limit_note         VARCHAR,
    permission_note    VARCHAR,
    history_coverage_type VARCHAR,
    history_start      VARCHAR,
    history_coverage_note VARCHAR,
    mcp_name           VARCHAR,
    mcp_description    VARCHAR,
    input_fields       JSON,
    output_fields      JSON,
    doc_url            VARCHAR,
    fetched_at         TIMESTAMP
);
"""

TUSHARE_INTERFACE_CATALOG_DDL = """
CREATE TABLE IF NOT EXISTS tushare_interface_catalog (
    doc_id             INTEGER PRIMARY KEY,
    title              VARCHAR NOT NULL,
    api_name           VARCHAR,
    category_path      JSON,
    priority           INTEGER,
    integration_status VARCHAR,
    integration_stage  VARCHAR,
    update_cadence     VARCHAR,
    target_table_hint  VARCHAR,
    permission_level   VARCHAR,
    strategy_value     VARCHAR,
    capability_tags    JSON,
    strategy_uses      JSON,
    limit_note         VARCHAR,
    permission_note    VARCHAR,
    history_coverage_type VARCHAR,
    history_start      VARCHAR,
    history_coverage_note VARCHAR,
    doc_url            VARCHAR,
    fetched_at         TIMESTAMP
);
"""

TUSHARE_PURCHASE_GOODS_DDL = """
CREATE TABLE IF NOT EXISTS tushare_purchase_goods (
    good_type      INTEGER,
    good_id        INTEGER,
    name           VARCHAR NOT NULL,
    description    VARCHAR,
    price          DOUBLE,
    duration_unit  VARCHAR,
    doc_url        VARCHAR,
    doc_id         INTEGER,
    api_count      INTEGER,
    api_names      JSON,
    api_doc_ids    JSON,
    raw_payload    JSON,
    fetched_at     TIMESTAMP,
    PRIMARY KEY (good_type, good_id)
);
"""

TUSHARE_ACCOUNT_SCORE_DDL = """
CREATE TABLE IF NOT EXISTS tushare_account_score (
    fetched_at  TIMESTAMP,
    scores      DOUBLE,
    raw_payload JSON
);
"""

TUSHARE_ACTIVITY_PACKAGE_DDL = """
CREATE TABLE IF NOT EXISTS tushare_activity_packages (
    package_id    INTEGER PRIMARY KEY,
    name          VARCHAR NOT NULL,
    description   VARCHAR,
    price         DOUBLE,
    duration_unit VARCHAR,
    raw_payload   JSON,
    fetched_at    TIMESTAMP
);
"""


def write_audit_rows(rows: list[TushareAuditRow], db_path: Path) -> int:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute("DROP TABLE IF EXISTS tushare_interface_audit")
        conn.execute(TUSHARE_INTERFACE_AUDIT_DDL)
        payload = [
            (
                row.doc_id,
                row.title,
                row.api_name,
                json.dumps(row.category_path, ensure_ascii=False),
                row.is_section,
                row.is_a_share_related,
                row.priority,
                json.dumps(row.capability_tags, ensure_ascii=False),
                json.dumps(row.strategy_uses, ensure_ascii=False),
                row.integration_status,
                row.description,
                row.limit_note,
                row.permission_note,
                row.history_coverage_type,
                row.history_start,
                row.history_coverage_note,
                row.mcp_name,
                row.mcp_description,
                json.dumps([field.model_dump() for field in row.input_fields], ensure_ascii=False),
                json.dumps([field.model_dump() for field in row.output_fields], ensure_ascii=False),
                row.doc_url,
                row.fetched_at,
            )
            for row in rows
        ]
        conn.executemany(
            """
            INSERT INTO tushare_interface_audit
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            payload,
        )
    finally:
        conn.close()
    return len(rows)


def write_interface_catalog_rows(
    rows: list[TushareInterfaceCatalogRow],
    db_path: Path,
) -> int:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute("DROP TABLE IF EXISTS tushare_interface_catalog")
        conn.execute(TUSHARE_INTERFACE_CATALOG_DDL)
        payload = [
            (
                row.doc_id,
                row.title,
                row.api_name,
                json.dumps(row.category_path, ensure_ascii=False),
                row.priority,
                row.integration_status,
                row.integration_stage,
                row.update_cadence,
                row.target_table_hint,
                row.permission_level,
                row.strategy_value,
                json.dumps(row.capability_tags, ensure_ascii=False),
                json.dumps(row.strategy_uses, ensure_ascii=False),
                row.limit_note,
                row.permission_note,
                row.history_coverage_type,
                row.history_start,
                row.history_coverage_note,
                row.doc_url,
                row.fetched_at,
            )
            for row in rows
        ]
        conn.executemany(
            """
            INSERT INTO tushare_interface_catalog
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            payload,
        )
    finally:
        conn.close()
    return len(rows)


def write_purchase_goods(rows: list[TusharePurchaseGood], db_path: Path) -> int:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(TUSHARE_PURCHASE_GOODS_DDL)
        conn.execute("DELETE FROM tushare_purchase_goods")
        payload = [
            (
                row.good_type,
                row.good_id,
                row.name,
                row.description,
                row.price,
                row.duration_unit,
                row.doc_url,
                row.doc_id,
                row.api_count,
                json.dumps(row.api_names, ensure_ascii=False),
                json.dumps(row.api_doc_ids, ensure_ascii=False),
                json.dumps(row.raw_payload, ensure_ascii=False),
                row.fetched_at,
            )
            for row in rows
        ]
        if payload:
            conn.executemany(
                """
                INSERT INTO tushare_purchase_goods
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
    finally:
        conn.close()
    return len(rows)


def write_account_score(row: TushareAccountScore | None, db_path: Path) -> int:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(TUSHARE_ACCOUNT_SCORE_DDL)
        conn.execute("DELETE FROM tushare_account_score")
        if row is None:
            return 0
        conn.execute(
            """
            INSERT INTO tushare_account_score
            VALUES (?, ?, ?)
            """,
            [row.fetched_at, row.scores, json.dumps(row.raw_payload, ensure_ascii=False)],
        )
    finally:
        conn.close()
    return 1


def write_activity_packages(rows: list[TushareActivityPackage], db_path: Path) -> int:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(TUSHARE_ACTIVITY_PACKAGE_DDL)
        conn.execute("DELETE FROM tushare_activity_packages")
        payload = [
            (
                row.package_id,
                row.name,
                row.description,
                row.price,
                row.duration_unit,
                json.dumps(row.raw_payload, ensure_ascii=False),
                row.fetched_at,
            )
            for row in rows
        ]
        if payload:
            conn.executemany(
                """
                INSERT INTO tushare_activity_packages
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
    finally:
        conn.close()
    return len(rows)


def _md_cell(value: object) -> str:
    text = "" if value is None else str(value)
    return text.replace("|", "\\|").replace("\n", "<br>")


def _joined(values: list[str]) -> str:
    return "、".join(values)


def render_audit_markdown(rows: list[TushareAuditRow]) -> str:
    total = len(rows)
    a_share_rows = [row for row in rows if row.is_a_share_related]
    actionable = [
        row
        for row in a_share_rows
        if not row.is_section and row.priority <= 2
    ]
    actionable.sort(key=lambda row: (row.priority, "/".join(row.category_path), row.doc_id))

    status_counts: dict[str, int] = {}
    tag_counts: dict[str, int] = {}
    for row in a_share_rows:
        status_counts[row.integration_status] = status_counts.get(row.integration_status, 0) + 1
        for tag in row.capability_tags:
            tag_counts[tag] = tag_counts.get(tag, 0) + 1

    lines = [
        "# Tushare A股接口审计",
        "",
        "## 概览",
        "",
        f"- 文档页总数：{total}",
        f"- A股相关页数：{len(a_share_rows)}",
        f"- 优先级 1-2 且非分类页：{len(actionable)}",
        "",
        "## 接入状态",
        "",
    ]
    for status, count in sorted(status_counts.items(), key=lambda item: item[0]):
        lines.append(f"- `{status}`：{count}")

    lines.extend(["", "## 能力标签", ""])
    for tag, count in sorted(tag_counts.items(), key=lambda item: (-item[1], item[0])):
        lines.append(f"- `{tag}`：{count}")

    lines.extend(
        [
            "",
            "## 优先接入接口",
            "",
            "| doc_id | 标题 | API | 优先级 | 路径 | 能力标签 | 策略用途 | 状态 |",
            "|---:|---|---|---:|---|---|---|---|",
        ]
    )
    for row in actionable:
        lines.append(
            "| "
            + " | ".join(
                [
                    _md_cell(row.doc_id),
                    _md_cell(row.title),
                    _md_cell(row.api_name),
                    _md_cell(row.priority),
                    _md_cell(" > ".join(row.category_path)),
                    _md_cell(_joined(row.capability_tags)),
                    _md_cell(_joined(row.strategy_uses)),
                    _md_cell(row.integration_status),
                ]
            )
            + " |"
        )

    lines.extend(
        [
            "",
            "## 全部 A股相关接口",
            "",
            "| doc_id | 标题 | API | 优先级 | 路径 | 限量 | 权限 |",
            "|---:|---|---|---:|---|---|---|",
        ]
    )
    sorted_a_share_rows = sorted(
        a_share_rows,
        key=lambda row: (row.priority, "/".join(row.category_path), row.doc_id),
    )
    for row in sorted_a_share_rows:
        lines.append(
            "| "
            + " | ".join(
                [
                    _md_cell(row.doc_id),
                    _md_cell(row.title),
                    _md_cell(row.api_name),
                    _md_cell(row.priority),
                    _md_cell(" > ".join(row.category_path)),
                    _md_cell(row.limit_note),
                    _md_cell(row.permission_note),
                ]
            )
            + " |"
        )

    lines.extend(
        [
            "",
            "## 历史覆盖信息",
            "",
            "| doc_id | 标题 | API | 覆盖类型 | 起始 | 说明 |",
            "|---:|---|---|---|---|---|",
        ]
    )
    history_rows = [
        row
        for row in sorted_a_share_rows
        if row.history_coverage_type != "unknown" or row.history_coverage_note
    ]
    for row in history_rows:
        lines.append(
            "| "
            + " | ".join(
                [
                    _md_cell(row.doc_id),
                    _md_cell(row.title),
                    _md_cell(row.api_name),
                    _md_cell(row.history_coverage_type),
                    _md_cell(row.history_start),
                    _md_cell(row.history_coverage_note),
                ]
            )
            + " |"
        )

    return "\n".join(lines) + "\n"
