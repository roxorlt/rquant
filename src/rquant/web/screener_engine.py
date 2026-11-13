"""Server-side Mode-A Screener Workbench engine (read-only, DAG-ready).

Supports:
1. Cascading upstream scope resolution (all_market, presets, single preset, saved pool,
   watchlist, plus set union/intersection/difference).
2. Unified condition block execution across 4 families:
   - 26 built-in ``rquant.screen.rules`` blocks validated via ``rquant.llm.registry``
   - TDX formulas translated via ``rquant.factor.tdx`` and evaluated on daily panel
   - Numeric / fundamental / validated factor percentile filters (``>=``, ``<=``,
     ``between``, ``top_pct``, ``bottom_pct``)
   - User-saved reusable custom blocks (``screener_custom_block`` in SQLite Web Store)
3. Step-by-step funnel diagnostics with intermediate step slice preview.
4. White-box natural language compilation (DeepSeek structured blocks + TDX fallback +
   deterministic local parser fallback).

Iron rule: strictly read-only; NEVER opens the primary writable ``DUCKDB_PATH``.
"""

from __future__ import annotations

import contextlib
import json
import math
import os
import re
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd

from rquant.factor.expr import evaluate as evaluate_expr
from rquant.factor.store import list_factors, read_latest, with_chips
from rquant.factor.tdx import TdxFormulaError
from rquant.factor.tdx import translate as translate_tdx
from rquant.fundamentals import read as read_fundamentals
from rquant.llm.registry import REGISTRY, REGISTRY_BY_NAME, RuleSpec, get_rule_spec
from rquant.screen.rules import AggregateRequest, Rule
from rquant.web.llm import Complete
from rquant.web.models import (
    CustomBlockItem,
    FieldCatalogGroup,
    FieldCatalogItem,
    PoolItem,
    RuleCatalogItem,
    RuleParamSpec,
    ScreenerBlock,
    ScreenerCatalogData,
    ScreenerFunnelStep,
    ScreenerNlCompileData,
    ScreenerRankSpec,
    ScreenerRunData,
    ScreenerRunRequest,
    ScreenerUpstream,
    ScreenRow,
    UpstreamOptionItem,
    WatchlistItemRow,
)
from rquant.web.screener_schemes import SchemeStore
from rquant.web.source import Source, _same_file, _sql_quote, table_missing
from rquant.web.stocks import enrich

CATEGORY_LABELS: dict[str, str] = {
    "filter": "基础过滤",
    "state": "涨跌停与连板",
    "shape": "K线形态",
    "indicator": "技术指标",
    "aggregate": "窗口统计",
    "compare": "通用字段比较",
}

RULE_TITLES: dict[str, str] = {
    "not_st": "排除 ST / *ST",
    "not_bj": "排除北交所",
    "circ_mv_lt": "流通市值上限",
    "board_in": "板块白名单",
    "first_limit_up": "某日首板",
    "not_limit_up": "某日未涨停",
    "limit_up": "某日涨停",
    "limit_down": "某日跌停",
    "yiziban": "某日一字板",
    "not_yiziban": "某日非一字板",
    "consecutive_ups_gte": "连板数下限",
    "has_lower_shadow": "长下影线形态",
    "gt": "字段大于 (>)",
    "lt": "字段小于 (<)",
    "gte": "字段不小于 (≥)",
    "lte": "字段不大于 (≤)",
    "between": "字段闭区间",
    "cross_above": "均线上穿 (金叉)",
    "cross_below": "均线下穿 (死叉)",
    "above_ma": "收盘站上均线",
    "rsi_oversold": "RSI 超卖",
    "rsi_overbought": "RSI 超买",
    "volume_ratio_gte": "放量倍数 (量比)",
    "no_consec_ups_in_window": "近N日避开高连板",
    "no_limit_down_in_window": "近N日无跌停",
    "has_prior_limit_up": "近N日曾涨停 (活跃股)",
}

PARAM_LABELS: dict[str, str] = {
    "offset": "交易日偏移",
    "threshold_yi": "市值上限(亿)",
    "boards": "板块",
    "n": "阈值",
    "min_ratio": "下影/实体比",
    "min_amplitude": "最小振幅",
    "threshold": "阈值",
    "window": "回看天数",
    "exclude_offset": "排除偏移日",
    "left": "左操作数",
    "right": "右操作数",
    "field": "比较字段",
    "low": "下限",
    "high": "上限",
    "fast": "快线",
    "slow": "慢线",
    "period": "周期",
}

BOARD_OPTIONS = [
    {"value": "main", "label": "主板"},
    {"value": "gem", "label": "创业板"},
    {"value": "star", "label": "科创板"},
    {"value": "bj", "label": "北交所"},
]

MA_LINE_OPTIONS = [
    {"value": "MA5", "label": "MA5"},
    {"value": "MA10", "label": "MA10"},
    {"value": "MA20", "label": "MA20"},
    {"value": "MA30", "label": "MA30"},
    {"value": "MA60", "label": "MA60"},
]

OPERAND_OPTIONS = [
    {"value": "CLOSE[0]", "label": "今日收盘 CLOSE[0]"},
    {"value": "CLOSE[1]", "label": "昨日收盘 CLOSE[1]"},
    {"value": "OPEN[0]", "label": "今日开盘 OPEN[0]"},
    {"value": "HIGH[0]", "label": "今日最高 HIGH[0]"},
    {"value": "HIGH[1]", "label": "昨日最高 HIGH[1]"},
    {"value": "LOW[0]", "label": "今日最低 LOW[0]"},
    {"value": "LOW[1]", "label": "昨日最低 LOW[1]"},
    {"value": "PCT_CHG[0]", "label": "今日涨跌幅% PCT_CHG[0]"},
    {"value": "PCT_CHG[1]", "label": "昨日涨跌幅% PCT_CHG[1]"},
    {"value": "BODY_UPPER[0]", "label": "今实体上沿 BODY_UPPER[0]"},
    {"value": "BODY_UPPER[1]", "label": "昨实体上沿 BODY_UPPER[1]"},
    {"value": "BODY_LOWER[0]", "label": "今实体下沿 BODY_LOWER[0]"},
    {"value": "MA5[0]", "label": "今日 MA5"},
    {"value": "MA10[0]", "label": "今日 MA10"},
    {"value": "MA20[0]", "label": "今日 MA20"},
    {"value": "MA60[0]", "label": "今日 MA60"},
]

BASE_FIELD_GROUPS: list[tuple[str, str, list[tuple[str, str]]]] = [
    (
        "行情",
        "market",
        [
            ("pct_chg", "涨跌幅 %"),
            ("close", "收盘价"),
            ("amount", "成交额(亿)"),
            ("amplitude", "振幅 %"),
            ("vol_ratio", "量比"),
            ("ret5", "5日涨幅 %"),
            ("ret20", "20日涨幅 %"),
            ("turnover_rate", "换手率 %"),
            ("turnover_rate_f", "换手率(自由流通) %"),
        ],
    ),
    (
        "估值",
        "valuation",
        [
            ("pe_ttm", "PE(TTM)"),
            ("pb", "PB"),
            ("total_mv", "总市值(亿)"),
            ("circ_mv", "流通市值(亿)"),
        ],
    ),
    (
        "财务",
        "fundamental",
        [
            ("roe", "ROE %"),
            ("grossprofit_margin", "毛利率 %"),
            ("netprofit_margin", "净利率 %"),
            ("netprofit_yoy", "净利同比 %"),
            ("or_yoy", "营收同比 %"),
            ("debt_to_assets", "资产负债率 %"),
        ],
    ),
    (
        "资金",
        "flow",
        [
            ("main_net", "主力净流入(亿)"),
            ("main_net_5d", "5日主力净流入(亿)"),
        ],
    ),
]

BASE_FIELD_LABELS: dict[str, str] = {
    k: label for _, _, pairs in BASE_FIELD_GROUPS for k, label in pairs
}

_PARAM_DEFAULTS_FALLBACK: dict[tuple[str, str], Any] = {
    ("circ_mv_lt", "threshold_yi"): 80.0,
    ("board_in", "boards"): ["main", "gem"],
    ("consecutive_ups_gte", "n"): 2,
    ("gt", "left"): "HIGH[0]",
    ("gt", "right"): "CLOSE[1]",
    ("lt", "left"): "BODY_UPPER[0]",
    ("lt", "right"): "BODY_UPPER[1]",
    ("gte", "left"): "PCT_CHG[0]",
    ("gte", "right"): 3.0,
    ("lte", "left"): "PCT_CHG[0]",
    ("lte", "right"): 5.0,
    ("between", "field"): "PCT_CHG[0]",
    ("between", "low"): -2.0,
    ("between", "high"): 5.0,
    ("cross_above", "fast"): "MA5",
    ("cross_above", "slow"): "MA20",
    ("cross_below", "fast"): "MA5",
    ("cross_below", "slow"): "MA20",
    ("above_ma", "period"): 20,
    ("rsi_oversold", "threshold"): 30.0,
    ("rsi_overbought", "threshold"): 70.0,
    ("volume_ratio_gte", "n"): 2.0,
}


def _primary_db_path() -> Path | None:
    raw = os.environ.get("DUCKDB_PATH")
    if raw:
        return Path(raw)
    try:
        from rquant.config import get_settings

        return Path(get_settings().duckdb_path)
    except Exception:  # noqa: BLE001
        return None


def _resolve_readonly_replica(market_db: Path | None) -> Path | None:
    raw = os.environ.get("DUCKDB_READONLY_PATH")
    candidate: Path | None = Path(raw) if raw else None
    if candidate is None:
        try:
            from rquant.config import get_settings

            candidate = Path(get_settings().duckdb_readonly_path)
        except Exception:  # noqa: BLE001
            candidate = None
    if candidate is None or not candidate.is_file():
        return None
    primary = _primary_db_path()
    if primary is not None and _same_file(candidate, primary):
        return None
    if market_db is not None and market_db.is_file() and _same_file(candidate, market_db):
        return None
    return candidate


@contextmanager
def _open_readonly_catalogs(
    root: Path,
) -> Iterator[tuple[duckdb.DuckDBPyConnection, dict[str, set[str]], dict[str, set[str]]]]:
    """Attach ``market.duckdb`` (as ``mkt``) and optional ``rquant_ro.duckdb`` (as ``ro``)
    read-only on an in-memory DuckDB connection. Never opens primary writable ``duckdb_path``."""
    con = duckdb.connect(":memory:")
    mkt_cols: dict[str, set[str]] = {}
    ro_cols: dict[str, set[str]] = {}
    try:
        primary = _primary_db_path()
        mkt = root / "market.duckdb"
        if mkt.is_file() and not (primary is not None and _same_file(mkt, primary)):
            with contextlib.suppress(duckdb.Error):
                con.execute(f"ATTACH '{_sql_quote(mkt)}' AS mkt (READ_ONLY)")
                mkt_cols = _catalog_cols(con, "mkt")
        ro = _resolve_readonly_replica(mkt if mkt_cols else None)
        if ro is not None:
            with contextlib.suppress(duckdb.Error):
                con.execute(f"ATTACH '{_sql_quote(ro)}' AS ro (READ_ONLY)")
                ro_cols = _catalog_cols(con, "ro")
        yield con, mkt_cols, ro_cols
    finally:
        con.close()


def _catalog_cols(con: duckdb.DuckDBPyConnection, catalog: str) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    rows = con.execute(
        "SELECT table_name, column_name FROM information_schema.columns "
        "WHERE table_catalog = ? AND table_schema = 'main'",
        [catalog],
    ).fetchall()
    for tbl, col in rows:
        out.setdefault(str(tbl), set()).add(str(col))
    return out


def _clean_float(val: Any) -> float | None:
    if val is None:
        return None
    try:
        f = float(val)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(f) or math.isinf(f) else f


def build_rule_catalog() -> list[RuleCatalogItem]:
    """Serialize all 26 ``rquant.llm.registry.REGISTRY`` rules into UI-ready specs."""
    items: list[RuleCatalogItem] = []
    for spec in REGISTRY:
        schema = spec.args_model.model_json_schema()
        props: dict[str, Any] = schema.get("properties", {})
        required_set = set(schema.get("required", []))
        params: list[RuleParamSpec] = []
        for pname, pinfo in props.items():
            default = pinfo.get("default")
            if default is None and (spec.name, pname) in _PARAM_DEFAULTS_FALLBACK:
                default = _PARAM_DEFAULTS_FALLBACK[(spec.name, pname)]
            kind: Any = "float"
            options: list[dict[str, str]] = []
            if pname == "boards":
                kind = "boards"
                options = BOARD_OPTIONS
            elif pname in ("fast", "slow"):
                kind = "ma_line"
                options = MA_LINE_OPTIONS
            elif pname in ("left", "right", "field"):
                kind = "operand"
                options = OPERAND_OPTIONS
            elif pinfo.get("type") == "integer":
                kind = "int"
            elif pinfo.get("type") == "string":
                kind = "str"
            params.append(
                RuleParamSpec(
                    name=pname,
                    label=PARAM_LABELS.get(pname, pname),
                    kind=kind,
                    required=pname in required_set,
                    default=default,
                    min=_clean_float(pinfo.get("minimum") or pinfo.get("exclusiveMinimum")),
                    max=_clean_float(pinfo.get("maximum") or pinfo.get("exclusiveMaximum")),
                    step=1.0 if kind == "int" else (0.1 if pname == "min_ratio" else None),
                    options=options,
                )
            )
        items.append(
            RuleCatalogItem(
                name=spec.name,
                title=RULE_TITLES.get(spec.name, spec.name),
                description=spec.description,
                category=spec.category,
                category_label=CATEGORY_LABELS.get(spec.category, spec.category),
                examples=list(spec.examples),
                params=params,
            )
        )
    return items


def build_field_groups(root: Path) -> list[FieldCatalogGroup]:
    from rquant.factor.store import read_tracking

    groups: list[FieldCatalogGroup] = [
        FieldCatalogGroup(
            group=gname,
            items=[FieldCatalogItem(field=k, label=lbl, kind=kind) for k, lbl in pairs],  # type: ignore[arg-type]
        )
        for gname, kind, pairs in BASE_FIELD_GROUPS
    ]
    factor_pct = read_latest(root)
    ic_by_name: dict[str, tuple[float | None, float | None]] = {}
    for run in list_factors(root):
        trk = read_tracking(run.factor_id, root)
        ic_by_name[run.name] = (
            _clean_float(run.result.mean_ic),
            _clean_float(trk.recent_ic) if trk else None,
        )
    factor_items: list[FieldCatalogItem] = []
    for fname in sorted(factor_pct):
        mean_ic, recent_ic = ic_by_name.get(fname, (None, None))
        factor_items.append(
            FieldCatalogItem(
                field=f"f:{fname}",
                label=f"{fname} (分位)",
                kind="factor",
                mean_ic=mean_ic,
                recent_ic=recent_ic,
            )
        )
    if factor_items:
        groups.append(FieldCatalogGroup(group="已验证因子", items=factor_items))
    return groups


def _offset_word(offset: int) -> str:
    if offset == 0:
        return "今日"
    if offset == 1:
        return "昨日"
    if offset == 2:
        return "前日"
    return f"T-{offset}"


def format_block_label(
    block: ScreenerBlock,
    custom_map: dict[str, dict[str, Any]],
    field_labels: dict[str, str],
) -> str:
    if block.label and block.label.strip():
        return block.label.strip()
    if block.type == "custom":
        cb = custom_map.get(block.custom_id or "")
        return f"★ {cb['name']}" if cb else f"★ 自定义积木({block.custom_id})"
    if block.type == "tdx":
        raw = (block.tdx or "").strip().replace("\n", " ")
        short = raw if len(raw) <= 32 else f"{raw[:29]}…"
        return f"TDX: {short}" if short else "TDX 公式"
    if block.type == "field":
        fld = block.field or "pct_chg"
        flabel = field_labels.get(fld) or fld.removeprefix("f:")
        if block.op == "between":
            hi = block.value2 if block.value2 is not None else block.value
            return f"{flabel} ∈ [{block.value:g}, {hi:g}]"
        if block.op == "top_pct":
            return f"{flabel} 前 {block.value:g}%"
        if block.op == "bottom_pct":
            return f"{flabel} 后 {block.value:g}%"
        return f"{flabel} {block.op} {block.value:g}"
    rname = block.rule_name or ""
    a = block.args or {}
    off = int(a.get("offset", 0) or 0)
    day_w = _offset_word(off)
    if rname == "not_st":
        return "排除 ST"
    if rname == "not_bj":
        return "排除北交所"
    if rname == "circ_mv_lt":
        return f"流通市值 < {a.get('threshold_yi', 80):g}亿"
    if rname == "board_in":
        bmap = {"main": "主板", "gem": "创业板", "star": "科创板", "bj": "北交"}
        bs = "+".join(bmap.get(str(x), str(x)) for x in (a.get("boards") or []))
        return f"板块: {bs or '全部'}"
    if rname == "first_limit_up":
        return f"{day_w}首板"
    if rname == "limit_up":
        return f"{day_w}涨停"
    if rname == "not_limit_up":
        return f"{day_w}未涨停"
    if rname == "limit_down":
        return f"{day_w}跌停"
    if rname == "yiziban":
        return f"{day_w}一字板"
    if rname == "not_yiziban":
        return f"{day_w}非一字板"
    if rname == "consecutive_ups_gte":
        return f"{day_w} ≥ {a.get('n', 2)}连板"
    if rname == "above_ma":
        return f"{day_w}站上 MA{a.get('period', 20)}"
    if rname == "cross_above":
        return f"{a.get('fast', 'MA5')} 上穿 {a.get('slow', 'MA20')}"
    if rname == "cross_below":
        return f"{a.get('fast', 'MA5')} 下穿 {a.get('slow', 'MA20')}"
    if rname == "volume_ratio_gte":
        return f"{day_w}量比 ≥ {a.get('n', 2):g}"
    if rname == "no_limit_down_in_window":
        return f"近{a.get('window', 30)}日无跌停"
    if rname == "no_consec_ups_in_window":
        return f"近{a.get('window', 8)}日无{a.get('threshold', 3)}连板"
    if rname == "has_prior_limit_up":
        return f"近{a.get('window', 90)}日曾涨停"
    if rname == "has_lower_shadow":
        return f"{day_w}下影 ≥ {a.get('min_ratio', 1.5):g}×实体"
    if rname == "rsi_oversold":
        return f"RSI{a.get('period', 14)} < {a.get('threshold', 30):g}"
    if rname == "rsi_overbought":
        return f"RSI{a.get('period', 14)} > {a.get('threshold', 70):g}"
    if rname in ("gt", "lt", "gte", "lte"):
        sym = {"gt": ">", "lt": "<", "gte": "≥", "lte": "≤"}[rname]
        return f"{a.get('left', '')} {sym} {a.get('right', '')}"
    if rname == "between":
        return f"{a.get('field', '')} ∈ [{a.get('low', 0):g}, {a.get('high', 0):g}]"
    return RULE_TITLES.get(rname, rname or "条件")


def _board_for_code(code: str) -> str:
    c = code.upper()
    if c.endswith(".BJ") or c.startswith(("4", "8", "920")):
        return "bj"
    if c.startswith("688") or c.startswith("689"):
        return "star"
    if c.startswith(("300", "301")):
        return "gem"
    return "main"


def _limit_pct_series(board: pd.Series, is_st: pd.Series) -> pd.Series:
    pct = pd.Series(0.10, index=board.index, dtype="float64")
    pct = pct.mask(board.isin(["gem", "star"]), 0.20)
    pct = pct.mask(board == "bj", 0.30)
    pct = pct.mask(is_st & (board == "main"), 0.05)
    return pct


def _screen_rows_from_sources(
    source: Source,
    con: duckdb.DuckDBPyConnection,
    mkt_cols: dict[str, set[str]],
    trade_date: date | None,
) -> tuple[date | None, list[dict[str, Any]]]:
    day = trade_date
    rows: list[dict[str, Any]] = []
    with contextlib.suppress(Exception):
        if day is None:
            latest = source.query("SELECT max(trade_date) AS d FROM screen_result")
            day = latest[0]["d"] if latest else None
        if day is not None:
            rows = source.query(
                "SELECT trade_date, ts_code, preset_name, name, close, pct_chg FROM screen_result "
                "WHERE trade_date = ? ORDER BY pct_chg DESC NULLS LAST LIMIT 5000",
                [day],
            )
    if not rows and "screen_result" in mkt_cols:
        with contextlib.suppress(duckdb.Error):
            if day is None:
                row0 = con.execute("SELECT max(trade_date) FROM mkt.screen_result").fetchone()
                day = row0[0] if row0 else None
            if day is not None:
                cur = con.execute(
                    "SELECT trade_date, ts_code, preset_name, name, close, pct_chg "
                    "FROM mkt.screen_result WHERE trade_date = ? LIMIT 5000",
                    [day],
                )
                cols = [c[0] for c in cur.description]
                rows = [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]
    return day, rows


def _latest_market_date(
    source: Source,
    con: duckdb.DuckDBPyConnection,
    mkt_cols: dict[str, set[str]],
    ro_cols: dict[str, set[str]],
) -> date | None:
    for catalog, cols in (("mkt", mkt_cols), ("ro", ro_cols)):
        if "daily_bar" in cols:
            with contextlib.suppress(duckdb.Error):
                r = con.execute(f"SELECT max(trade_date) FROM {catalog}.daily_bar").fetchone()
                if r and r[0] is not None:
                    return r[0]
    day, _ = _screen_rows_from_sources(source, con, mkt_cols, None)
    return day


def _all_market_codes(
    source: Source,
    con: duckdb.DuckDBPyConnection,
    mkt_cols: dict[str, set[str]],
    ro_cols: dict[str, set[str]],
    day: date | None,
) -> tuple[list[str], dict[str, str]]:
    names: dict[str, str] = {}
    for catalog, cols in (("mkt", mkt_cols), ("ro", ro_cols)):
        if "stock_basic" in cols:
            with contextlib.suppress(duckdb.Error):
                for c, n in con.execute(
                    f"SELECT ts_code, name FROM {catalog}.stock_basic LIMIT 10000"
                ).fetchall():
                    if c:
                        names.setdefault(str(c), str(n) if n else "")
    with contextlib.suppress(Exception):
        for r in source.query("SELECT ts_code, name FROM stock_basic LIMIT 10000"):
            if r.get("ts_code"):
                names.setdefault(str(r["ts_code"]), str(r.get("name") or ""))
    codes: list[str] = []
    if day is not None:
        for catalog, cols in (("mkt", mkt_cols), ("ro", ro_cols)):
            if "daily_bar" in cols:
                with contextlib.suppress(duckdb.Error):
                    fetched = con.execute(
                        f"SELECT DISTINCT ts_code FROM {catalog}.daily_bar "
                        f"WHERE trade_date = ("
                        f"SELECT max(trade_date) FROM {catalog}.daily_bar WHERE trade_date <= ?) "
                        "ORDER BY ts_code",
                        [day],
                    ).fetchall()
                    if fetched:
                        codes = [str(r[0]) for r in fetched if r[0]]
                        break
    if not codes and names:
        codes = sorted(names)
    return codes, names


def list_catalog(
    *,
    root: Path,
    source: Source,
    scheme_store: SchemeStore,
    pool_items: list[PoolItem],
    watchlist_items: list[WatchlistItemRow],
) -> ScreenerCatalogData:
    with _open_readonly_catalogs(root) as (con, mkt_cols, ro_cols):
        day = _latest_market_date(source, con, mkt_cols, ro_cols)
        _, screen_rows = _screen_rows_from_sources(source, con, mkt_cols, None)
        all_codes, _ = _all_market_codes(source, con, mkt_cols, ro_cols, day)

    by_preset: dict[str, set[str]] = {}
    for r in screen_rows:
        by_preset.setdefault(str(r["preset_name"]), set()).add(str(r["ts_code"]))
    presets = sorted(by_preset)

    upstream_opts: list[UpstreamOptionItem] = []
    for p in presets:
        upstream_opts.append(
            UpstreamOptionItem(
                ref=f"preset:{p}",
                kind="preset",
                name=p,
                label=f"预设 · {p}",
                count=len(by_preset[p]),
            )
        )
    for pool in pool_items:
        if pool.name == "自选股":
            continue
        member_codes = {m.code for m in pool.members}
        upstream_opts.append(
            UpstreamOptionItem(
                ref=f"pool:{pool.name}",
                kind="pool",
                name=pool.name,
                label=f"池子 · {pool.name}",
                count=len(member_codes),
            )
        )
    upstream_opts.append(
        UpstreamOptionItem(
            ref="watchlist",
            kind="watchlist",
            name="自选股",
            label="自选股",
            count=len({w.code for w in watchlist_items}),
        )
    )

    try:
        raw_blocks = scheme_store.list_blocks()
    except Exception:  # noqa: BLE001
        raw_blocks = []
    custom_blocks = [CustomBlockItem(**b) for b in raw_blocks]
    return ScreenerCatalogData(
        trade_date=day,
        all_market_count=len(all_codes),
        presets=presets,
        upstream_options=upstream_opts,
        rules=build_rule_catalog(),
        field_groups=build_field_groups(root),
        custom_blocks=custom_blocks,
    )


def _resolve_ref_codes(
    ref: str,
    by_preset: dict[str, list[str]],
    by_pool: dict[str, list[str]],
    watch_codes: list[str],
) -> list[str]:
    r = ref.strip()
    if not r:
        return []
    if r in ("watchlist", "pool:自选股", "自选股"):
        return list(watch_codes)
    if r.startswith("preset:"):
        return list(by_preset.get(r.removeprefix("preset:"), []))
    if r.startswith("pool:"):
        return list(by_pool.get(r.removeprefix("pool:"), []))
    if r in by_pool:
        return list(by_pool[r])
    if r in by_preset:
        return list(by_preset[r])
    return []


def _resolve_upstream(
    upstream: ScreenerUpstream,
    all_codes: list[str],
    screen_rows: list[dict[str, Any]],
    pool_items: list[PoolItem],
    watchlist_items: list[WatchlistItemRow],
) -> tuple[list[str], dict[str, str], dict[str, list[str]], list[str], str]:
    by_preset: dict[str, list[str]] = {}
    presets_by_code: dict[str, list[str]] = {}
    for r in screen_rows:
        c = str(r["ts_code"])
        p = str(r["preset_name"])
        plist = presets_by_code.setdefault(c, [])
        if p not in plist:
            plist.append(p)
        lst = by_preset.setdefault(p, [])
        if c not in lst:
            lst.append(c)

    preset_by_code: dict[str, str] = {
        c: ",".join(ps) for c, ps in presets_by_code.items() if ps
    }

    by_pool: dict[str, list[str]] = {}
    for pool in pool_items:
        by_pool[pool.name] = list(dict.fromkeys(m.code for m in pool.members))
    watch_codes = list(dict.fromkeys(w.code for w in watchlist_items))

    if upstream.base == "codes":
        base_codes = list(
            dict.fromkeys(str(c).strip().upper() for c in upstream.codes if str(c).strip())
        )
        pool_refs = ["saved"]
        base_label = (upstream.base_label or "").strip() or "画布上游输入"
        for c in base_codes:
            preset_by_code.setdefault(c, "saved")
    elif upstream.base == "all_market":
        base_codes = list(all_codes)
        pool_refs = ["all_market"]
        base_label = "全市场 A 股"
        for c in base_codes:
            preset_by_code.setdefault(c, "all_market")
    elif upstream.base == "preset" and upstream.preset:
        pname = upstream.preset
        base_codes = list(by_preset.get(pname, []))
        pool_refs = [pname]
        base_label = f"预设 · {pname}"
        for c in base_codes:
            preset_by_code[c] = pname
    elif upstream.base == "pool" and upstream.pool:
        pname = upstream.pool
        base_codes = list(by_pool.get(pname, []))
        pool_refs = [pname]
        base_label = f"池子 · {pname}"
        for c in base_codes:
            preset_by_code.setdefault(c, pname)
    elif upstream.base == "watchlist":
        base_codes = list(watch_codes)
        pool_refs = ["manual"]
        base_label = "自选股"
        for c in base_codes:
            preset_by_code.setdefault(c, "manual")
    else:
        # Default "presets": union of all today's preset screen rows
        seen: set[str] = set()
        base_codes = []
        for r in screen_rows:
            c = str(r["ts_code"])
            if c not in seen:
                seen.add(c)
                base_codes.append(c)
        pool_refs = sorted(by_preset) or ["all_market"]
        base_label = "今日候选池"

    code_set = list(dict.fromkeys(base_codes))
    set_notes: list[str] = []

    for ref in upstream.include_pools:
        extra = _resolve_ref_codes(ref, by_preset, by_pool, watch_codes)
        for c in extra:
            if c not in code_set:
                code_set.append(c)
                preset_by_code.setdefault(c, ref.split(":", 1)[-1])
        if ref:
            set_notes.append(f"+{ref.split(':', 1)[-1]}")

    for ref in upstream.intersect_pools:
        keep = set(_resolve_ref_codes(ref, by_preset, by_pool, watch_codes))
        code_set = [c for c in code_set if c in keep]
        if ref:
            set_notes.append(f"∩{ref.split(':', 1)[-1]}")

    for ref in upstream.exclude_pools:
        drop = set(_resolve_ref_codes(ref, by_preset, by_pool, watch_codes))
        code_set = [c for c in code_set if c not in drop]
        if ref:
            set_notes.append(f"-{ref.split(':', 1)[-1]}")

    if set_notes:
        base_label = f"{base_label} ({' '.join(set_notes)})"
    return code_set, preset_by_code, presets_by_code, pool_refs, base_label


def _rsi_frame(close_piv: pd.DataFrame, period: int) -> pd.DataFrame:
    delta = close_piv.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    avg_gain = gain.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi.fillna(100.0).where(avg_gain.notna() | avg_loss.notna())


def _collect_rule_needs(
    blocks: list[ScreenerBlock],
    custom_map: dict[str, dict[str, Any]],
) -> tuple[bool, bool, int, set[int], set[int], list[AggregateRequest]]:
    """Inspect blocks to determine needed wide columns, MA/RSI periods, and lookback."""
    need_wide = False
    need_panel = False
    max_offset = 0
    ma_periods: set[int] = {5, 10, 20, 60}
    rsi_periods: set[int] = {14}
    agg_reqs: list[AggregateRequest] = []
    seen_agg: set[str] = set()

    def visit(b: ScreenerBlock, depth: int = 0) -> None:
        nonlocal need_wide, need_panel, max_offset
        if not b.enabled or depth > 3:
            return
        if b.type == "custom":
            cb = custom_map.get(b.custom_id or "")
            if not cb:
                return
            spec = cb.get("spec") or {}
            kind = cb.get("kind")
            if kind == "composite":
                for raw_sub in spec.get("blocks") or []:
                    with contextlib.suppress(Exception):
                        visit(ScreenerBlock.model_validate(raw_sub), depth + 1)
            elif kind in ("rule", "tdx", "field"):
                with contextlib.suppress(Exception):
                    visit(ScreenerBlock.model_validate({"type": kind, **spec}), depth + 1)
            return
        if b.type == "tdx":
            need_panel = True
            return
        if b.type != "rule" or not b.rule_name:
            return
        need_wide = True
        try:
            spec = get_rule_spec(b.rule_name)
            validated = spec.args_model.model_validate(b.args)
            rule_fn: Rule = spec.fn(**validated.model_dump(exclude_unset=False))
        except Exception:  # noqa: BLE001
            return
        max_offset = max(max_offset, int(getattr(rule_fn, "min_lookback", 0)))
        for req in getattr(rule_fn, "aggregate_requests", []):
            if req.name not in seen_agg:
                seen_agg.add(req.name)
                agg_reqs.append(req)
        a = validated.model_dump()
        if b.rule_name == "above_ma" and "period" in a:
            ma_periods.add(int(a["period"]))
        if b.rule_name in ("rsi_oversold", "rsi_overbought") and "period" in a:
            rsi_periods.add(int(a["period"]))
        for k in ("fast", "slow", "left", "right", "field"):
            val = a.get(k)
            if isinstance(val, str):
                m_ma = re.match(r"^MA(\d+)", val)
                if m_ma:
                    ma_periods.add(int(m_ma.group(1)))
                m_rsi = re.match(r"^RSI(\d+)", val)
                if m_rsi:
                    rsi_periods.add(int(m_rsi.group(1)))

    for blk in blocks:
        visit(blk)
    return need_wide, need_panel, max_offset, ma_periods, rsi_periods, agg_reqs


def _build_wide_and_panel(
    con: duckdb.DuckDBPyConnection,
    mkt_cols: dict[str, set[str]],
    ro_cols: dict[str, set[str]],
    codes: list[str],
    names_map: dict[str, str],
    day: date,
    need_wide: bool,
    need_panel: bool,
    max_offset: int,
    ma_periods: set[int],
    rsi_periods: set[int],
    agg_reqs: list[AggregateRequest],
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    if not codes or (not need_wide and not need_panel):
        return pd.DataFrame({"ts_code": codes}).set_index("ts_code", drop=False), {}

    bar_cat = "mkt" if "daily_bar" in mkt_cols else ("ro" if "daily_bar" in ro_cols else None)
    if bar_cat is None:
        return pd.DataFrame({"ts_code": codes}).set_index("ts_code", drop=False), {}

    max_agg_win = max((r.window for r in agg_reqs), default=0)
    max_ind_win = max(max(ma_periods, default=20), max(rsi_periods, default=14) * 2)
    trading_days_needed = max(
        (250 if need_panel else 30),
        max_offset + max_ind_win + 10,
        max_agg_win + max_offset + 10,
    )
    cal_days = int(trading_days_needed * 1.65) + 25
    start_day = day - timedelta(days=cal_days)

    code_clause = ""
    params: list[Any] = [start_day, day]
    if len(codes) <= 1000:
        marks = ",".join("?" * len(codes))
        code_clause = f" AND ts_code IN ({marks})"
        params.extend(codes)

    bar_df = con.execute(
        "SELECT trade_date, ts_code, open, high, low, close, pre_close, pct_chg, vol, amount "
        f"FROM {bar_cat}.daily_bar WHERE trade_date BETWEEN ? AND ?{code_clause}",
        params,
    ).df()
    if bar_df.empty:
        return pd.DataFrame({"ts_code": codes}).set_index("ts_code", drop=False), {}

    bar_df = bar_df[bar_df["ts_code"].isin(codes)]
    if bar_df.empty:
        return pd.DataFrame({"ts_code": codes}).set_index("ts_code", drop=False), {}

    bar_df["trade_date"] = pd.to_datetime(bar_df["trade_date"])
    fields = ("open", "high", "low", "close", "pre_close", "pct_chg", "vol", "amount")
    panel: dict[str, pd.DataFrame] = {
        f: bar_df.pivot(index="trade_date", columns="ts_code", values=f).sort_index()
        for f in fields
    }

    basic_cat = (
        "mkt" if "daily_basic" in mkt_cols else ("ro" if "daily_basic" in ro_cols else None)
    )
    circ_piv: pd.DataFrame | None = None
    turn_piv: pd.DataFrame | None = None
    if basic_cat is not None:
        basic_table_cols = (mkt_cols if basic_cat == "mkt" else ro_cols).get("daily_basic", set())
        tcol = (
            "COALESCE(turnover_rate_f, turnover_rate)"
            if "turnover_rate_f" in basic_table_cols
            else "turnover_rate"
        )
        with contextlib.suppress(duckdb.Error):
            bdf = con.execute(
                f"SELECT trade_date, ts_code, circ_mv, {tcol} AS turn "
                f"FROM {basic_cat}.daily_basic WHERE trade_date BETWEEN ? AND ?{code_clause}",
                params,
            ).df()
            if not bdf.empty:
                bdf = bdf[bdf["ts_code"].isin(codes)]
                bdf["trade_date"] = pd.to_datetime(bdf["trade_date"])
                circ_piv = bdf.pivot(
                    index="trade_date", columns="ts_code", values="circ_mv"
                ).sort_index()
                turn_piv = bdf.pivot(
                    index="trade_date", columns="ts_code", values="turn"
                ).sort_index()

    if need_panel:
        if turn_piv is not None:
            panel["turnover"] = turn_piv.reindex_like(panel["close"])
        mf_cat = "mkt" if "moneyflow" in mkt_cols else ("ro" if "moneyflow" in ro_cols else None)
        if mf_cat is not None:
            with contextlib.suppress(duckdb.Error):
                mdf = con.execute(
                    "SELECT trade_date, ts_code,"
                    " sell_sm_amount+sell_md_amount+sell_lg_amount+sell_elg_amount AS s,"
                    " buy_sm_amount+buy_md_amount+buy_lg_amount+buy_elg_amount AS b,"
                    " buy_lg_amount+buy_elg_amount-sell_lg_amount-sell_elg_amount AS lg"
                    f" FROM {mf_cat}.moneyflow WHERE trade_date BETWEEN ? AND ?{code_clause}",
                    params,
                ).df()
                if not mdf.empty:
                    mdf = mdf[mdf["ts_code"].isin(codes)]
                    mdf["trade_date"] = pd.to_datetime(mdf["trade_date"])
                    piv = {
                        c: mdf.pivot(index="trade_date", columns="ts_code", values=c).reindex_like(
                            panel["close"]
                        )
                        for c in ("s", "b", "lg")
                    }
                    amt = panel["amount"]
                    panel["mf_io"] = piv["s"] / piv["b"].where(piv["b"] > 0)
                    panel["mf_lg"] = piv["lg"] / (amt / 10.0).where(amt > 0)

    if not need_wide:
        return pd.DataFrame({"ts_code": codes}).set_index("ts_code", drop=False), panel

    idx_codes = pd.Index(codes, name="ts_code")
    wide_cols: dict[str, Any] = {
        "ts_code": idx_codes,
        "name": [names_map.get(c, "") for c in codes],
    }
    name_s = pd.Series(wide_cols["name"], index=idx_codes, dtype="string")
    is_st = name_s.str.upper().str.contains("ST", na=False)
    board_type = pd.Series([_board_for_code(c) for c in codes], index=idx_codes, dtype="string")
    wide_cols["is_st"] = is_st
    wide_cols["board_type"] = board_type
    wide_cols["is_bj"] = board_type == "bj"

    close_p = panel["close"].reindex(columns=idx_codes)
    open_p = panel["open"].reindex(columns=idx_codes)
    high_p = panel["high"].reindex(columns=idx_codes)
    low_p = panel["low"].reindex(columns=idx_codes)
    pre_p = panel["pre_close"].reindex(columns=idx_codes)
    pct_p = panel["pct_chg"].reindex(columns=idx_codes)
    vol_p = panel["vol"].reindex(columns=idx_codes)
    amt_p = panel["amount"].reindex(columns=idx_codes)
    body_up_p = np.maximum(open_p, close_p)
    body_lo_p = np.minimum(open_p, close_p)
    circ_p = (
        circ_piv.reindex(index=close_p.index, columns=idx_codes).ffill()
        if circ_piv is not None
        else pd.DataFrame(np.nan, index=close_p.index, columns=idx_codes)
    )

    lim_pct = _limit_pct_series(board_type, is_st)
    lim_up_px = pre_p.mul(1.0 + lim_pct, axis=1).round(2)
    lim_dn_px = pre_p.mul(1.0 - lim_pct, axis=1).round(2)
    is_lim_up_p = (close_p >= (lim_up_px - 0.01)) & pre_p.notna() & close_p.notna()
    is_lim_dn_p = (close_p <= (lim_dn_px + 0.01)) & pre_p.notna() & close_p.notna()
    is_yizi_p = is_lim_up_p & (high_p <= (low_p + 1e-6))
    prev_up = is_lim_up_p.shift(1, fill_value=False)
    is_first_up_p = is_lim_up_p & (~prev_up)

    # Vectorized consecutive limit-up streak across trading days
    up_arr = is_lim_up_p.to_numpy(dtype=bool)
    streaks = np.zeros(up_arr.shape, dtype=np.int32)
    for row_i in range(up_arr.shape[0]):
        if row_i == 0:
            streaks[row_i] = up_arr[row_i].astype(np.int32)
        else:
            streaks[row_i] = np.where(up_arr[row_i], streaks[row_i - 1] + 1, 0)
    consec_up_p = pd.DataFrame(streaks, index=close_p.index, columns=idx_codes)

    # Override state matrices with authoritative daily_state when full columns exist in ro/mkt
    for st_cat, st_cols_map in (("ro", ro_cols), ("mkt", mkt_cols)):
        st_cols = st_cols_map.get("daily_state", set())
        needed_st = {
            "is_limit_up",
            "is_limit_down",
            "is_first_limit_up",
            "is_yiziban",
            "consecutive_limit_ups",
        }
        if needed_st.issubset(st_cols):
            with contextlib.suppress(duckdb.Error):
                sdf = con.execute(
                    "SELECT trade_date, ts_code, is_limit_up, is_limit_down, "
                    "is_first_limit_up, is_yiziban, consecutive_limit_ups "
                    f"FROM {st_cat}.daily_state WHERE trade_date BETWEEN ? AND ?{code_clause}",
                    params,
                ).df()
                if not sdf.empty:
                    sdf = sdf[sdf["ts_code"].isin(codes)]
                    sdf["trade_date"] = pd.to_datetime(sdf["trade_date"])
                    is_lim_up_p = (
                        sdf.pivot(index="trade_date", columns="ts_code", values="is_limit_up")
                        .reindex(index=close_p.index, columns=idx_codes)
                        .fillna(False)
                        .astype(bool)
                    )
                    is_lim_dn_p = (
                        sdf.pivot(index="trade_date", columns="ts_code", values="is_limit_down")
                        .reindex(index=close_p.index, columns=idx_codes)
                        .fillna(False)
                        .astype(bool)
                    )
                    is_first_up_p = (
                        sdf.pivot(
                            index="trade_date", columns="ts_code", values="is_first_limit_up"
                        )
                        .reindex(index=close_p.index, columns=idx_codes)
                        .fillna(False)
                        .astype(bool)
                    )
                    is_yizi_p = (
                        sdf.pivot(index="trade_date", columns="ts_code", values="is_yiziban")
                        .reindex(index=close_p.index, columns=idx_codes)
                        .fillna(False)
                        .astype(bool)
                    )
                    consec_up_p = (
                        sdf.pivot(
                            index="trade_date",
                            columns="ts_code",
                            values="consecutive_limit_ups",
                        )
                        .reindex(index=close_p.index, columns=idx_codes)
                        .fillna(0)
                    )
                    break

    ma_frames = {
        p: close_p.rolling(p, min_periods=p).mean() for p in sorted(ma_periods)
    }
    rsi_frames = {p: _rsi_frame(close_p, p) for p in sorted(rsi_periods)}

    n_dates = len(close_p.index)
    max_k = max(max_offset + 1, 2)
    base_matrices: dict[str, pd.DataFrame] = {
        "OPEN": open_p,
        "HIGH": high_p,
        "LOW": low_p,
        "CLOSE": close_p,
        "PRE_CLOSE": pre_p,
        "PCT_CHG": pct_p,
        "VOL": vol_p,
        "AMOUNT": amt_p,
        "BODY_UPPER": body_up_p,
        "BODY_LOWER": body_lo_p,
        "CIRC_MV": circ_p,
        "IS_LIMIT_UP": is_lim_up_p,
        "IS_LIMIT_DOWN": is_lim_dn_p,
        "IS_FIRST_LIMIT_UP": is_first_up_p,
        "IS_YIZIBAN": is_yizi_p,
        "CONSECUTIVE_LIMIT_UPS": consec_up_p,
    }
    for p, mf in ma_frames.items():
        base_matrices[f"MA{p}"] = mf
    for p, rf in rsi_frames.items():
        base_matrices[f"RSI{p}"] = rf

    for col_name, mat in base_matrices.items():
        for k in range(max_k + 1):
            pos = n_dates - 1 - k
            if pos >= 0:
                wide_cols[f"{col_name}[{k}]"] = mat.iloc[pos]
            else:
                wide_cols[f"{col_name}[{k}]"] = pd.Series(np.nan, index=idx_codes)

    for req in agg_reqs:
        src_mat = {
            "consecutive_limit_ups": consec_up_p,
            "is_limit_down": is_lim_dn_p,
            "is_limit_up": is_lim_up_p,
        }.get(req.source_col)
        if src_mat is None or n_dates == 0:
            wide_cols[req.name] = pd.Series(np.nan, index=idx_codes)
            continue
        tail = src_mat.iloc[max(0, n_dates - req.window) : n_dates]
        if req.exclude_offset is not None:
            ex_pos = len(tail) - 1 - req.exclude_offset
            if 0 <= ex_pos < len(tail):
                tail = pd.concat([tail.iloc[:ex_pos], tail.iloc[ex_pos + 1 :]])
        if req.agg_func == "max":
            wide_cols[req.name] = tail.max(axis=0)
        elif req.agg_func == "any":
            wide_cols[req.name] = tail.astype(bool).any(axis=0)
        elif req.agg_func == "count_nonzero":
            wide_cols[req.name] = tail.astype(bool).sum(axis=0)
        else:
            wide_cols[req.name] = tail.sum(axis=0)

    return pd.DataFrame(wide_cols, index=idx_codes), panel


def _row_field_val(row: ScreenRow, field: str) -> float | None:
    if field.startswith("f:"):
        return row.factors.get(field[2:])
    val = getattr(row, field, None)
    return float(val) if isinstance(val, (int, float)) and not math.isnan(val) else None


def _score_rows(rows: list[ScreenRow], ranks: list[ScreenerRankSpec]) -> dict[str, float]:
    if not rows or not ranks:
        return {}
    total = sum(r.weight for r in ranks) or 1.0
    acc: dict[str, float] = {r.code: 0.0 for r in rows}
    n = len(rows)
    for rk in ranks:
        if rk.weight <= 0:
            continue
        sorted_rows = sorted(
            rows,
            key=lambda r: (
                (_row_field_val(r, rk.field) is not None),
                (
                    (_row_field_val(r, rk.field) or 0.0)
                    if rk.desc
                    else -(_row_field_val(r, rk.field) or 0.0)
                ),
            ),
        )
        for i, row in enumerate(sorted_rows):
            pct = (i / (n - 1)) if n > 1 else 1.0
            acc[row.code] += (pct * 100.0 * rk.weight) / total
    return {c: round(v, 2) for c, v in acc.items()}


def _eval_single_block(
    block: ScreenerBlock,
    current_codes: list[str],
    row_by_code: dict[str, ScreenRow],
    wide_df: pd.DataFrame,
    panel: dict[str, pd.DataFrame],
    custom_map: dict[str, dict[str, Any]],
    depth: int = 0,
) -> tuple[list[str], str | None]:
    if not current_codes or depth > 3:
        return current_codes, None

    if block.type == "custom":
        cb = custom_map.get(block.custom_id or "")
        if not cb:
            return current_codes, f"未找到自定义积木：{block.custom_id}"
        kind = cb.get("kind")
        spec = cb.get("spec") or {}
        if kind == "composite":
            codes = list(current_codes)
            for raw_sub in spec.get("blocks") or []:
                sub = ScreenerBlock.model_validate(raw_sub)
                if not sub.enabled:
                    continue
                codes, err = _eval_single_block(
                    sub, codes, row_by_code, wide_df, panel, custom_map, depth + 1
                )
                if err:
                    return codes, err
            return codes, None
        sub = ScreenerBlock.model_validate({"type": kind, **spec})
        return _eval_single_block(
            sub, current_codes, row_by_code, wide_df, panel, custom_map, depth + 1
        )

    if block.type == "field":
        fld = block.field or "pct_chg"
        op = block.op
        if op in ("top_pct", "bottom_pct"):
            pct = max(0.0, min(100.0, float(block.value)))
            scored = [
                (c, _row_field_val(row_by_code[c], fld))
                for c in current_codes
                if c in row_by_code and _row_field_val(row_by_code[c], fld) is not None
            ]
            if not scored or pct <= 0:
                return [], None
            scored.sort(key=lambda item: float(item[1] or 0.0), reverse=(op == "top_pct"))
            keep_n = max(1, math.ceil(len(scored) * pct / 100.0))
            keep_set = {c for c, _ in scored[:keep_n]}
            return [c for c in current_codes if c in keep_set], None

        kept: list[str] = []
        for c in current_codes:
            row = row_by_code.get(c)
            if row is None:
                continue
            v = _row_field_val(row, fld)
            if v is None:
                continue
            if (op == ">=" and v >= block.value) or (op == "<=" and v <= block.value):
                kept.append(c)
            elif op == "between":
                hi = block.value2 if block.value2 is not None else block.value
                lo_v, hi_v = min(block.value, hi), max(block.value, hi)
                if lo_v <= v <= hi_v:
                    kept.append(c)
        return kept, None

    if block.type == "tdx":
        formula = (block.tdx or "").strip()
        if not formula:
            return current_codes, "公式为空"
        try:
            expr = translate_tdx(formula)
        except TdxFormulaError as exc:
            return current_codes, str(exc)
        if not panel or "close" not in panel or panel["close"].empty:
            return [], "缺少日线面板数据，无法执行 TDX 公式"
        try:
            res = evaluate_expr(expr, with_chips(panel, expr))
        except Exception as exc:  # noqa: BLE001
            return current_codes, f"公式计算错误：{exc}"
        if res.empty:
            return [], None
        last_s = res.iloc[-1]
        hit_set = {
            str(code)
            for code, val in last_s.items()
            if pd.notna(val) and bool(val)
        }
        return [c for c in current_codes if c in hit_set], None

    if block.type == "rule":
        rname = block.rule_name or ""
        try:
            spec = get_rule_spec(rname)
            validated = spec.args_model.model_validate(block.args)
            rule_fn = spec.fn(**validated.model_dump(exclude_unset=False))
        except Exception as exc:  # noqa: BLE001
            return current_codes, f"积木参数错误：{exc}"
        sub_df = wide_df.reindex(current_codes)
        try:
            mask = rule_fn(sub_df)
        except KeyError as exc:
            return [], f"缺少列 {exc}"
        except Exception as exc:  # noqa: BLE001
            return current_codes, f"积木执行异常：{exc}"
        hit_set = {str(code) for code, ok in mask.items() if pd.notna(ok) and bool(ok)}
        return [c for c in current_codes if c in hit_set], None

    return current_codes, None


def run_screener(
    *,
    body: ScreenerRunRequest,
    root: Path,
    source: Source,
    scheme_store: SchemeStore,
    pool_items: list[PoolItem],
    watchlist_items: list[WatchlistItemRow],
) -> ScreenerRunData:
    try:
        raw_blocks = scheme_store.list_blocks()
    except Exception:  # noqa: BLE001
        raw_blocks = []
    custom_map = {b["block_id"]: b for b in raw_blocks}
    field_groups = build_field_groups(root)
    field_labels = {item.field: item.label for g in field_groups for item in g.items}

    with _open_readonly_catalogs(root) as (con, mkt_cols, ro_cols):
        screen_day, screen_rows = _screen_rows_from_sources(
            source, con, mkt_cols, body.trade_date
        )
        market_day = body.trade_date or _latest_market_date(source, con, mkt_cols, ro_cols)
        day = (
            screen_day
            if body.upstream.base in ("presets", "preset") and screen_day is not None
            else (market_day or screen_day)
        )
        all_codes, names_map = _all_market_codes(source, con, mkt_cols, ro_cols, day)
        for r in screen_rows:
            if r.get("ts_code") and r.get("name"):
                names_map.setdefault(str(r["ts_code"]), str(r["name"]))

        presets = sorted({str(r["preset_name"]) for r in screen_rows})
        candidate_codes, preset_by_code, presets_by_code, pool_refs, base_label = _resolve_upstream(
            body.upstream, all_codes, screen_rows, pool_items, watchlist_items
        )

        if day is None or not candidate_codes:
            step0 = ScreenerFunnelStep(
                step=0, block_id=None, label=base_label, remaining=len(candidate_codes)
            )
            return ScreenerRunData(
                trade_date=day,
                active_step=0,
                steps=[step0],
                rows=[],
                presets=presets,
                pool_refs=pool_refs,
            )

        # Build ScreenRow metadata for candidate universe
        bar_snapshot: dict[str, tuple[float | None, float | None]] = {}
        for r in screen_rows:
            c = str(r["ts_code"])
            bar_snapshot.setdefault(
                c, (_clean_float(r.get("close")), _clean_float(r.get("pct_chg")))
            )

        bar_cat = "mkt" if "daily_bar" in mkt_cols else ("ro" if "daily_bar" in ro_cols else None)
        if bar_cat is not None:
            missing_snap = [c for c in candidate_codes if c not in bar_snapshot]
            if missing_snap:
                with contextlib.suppress(duckdb.Error):
                    if len(missing_snap) <= 1000:
                        marks = ",".join("?" * len(missing_snap))
                        b_rows = con.execute(
                            f"SELECT ts_code, close, pct_chg FROM {bar_cat}.daily_bar "
                            f"WHERE trade_date = ("
                            f"SELECT max(trade_date) FROM {bar_cat}.daily_bar "
                            f"WHERE trade_date <= ?) AND ts_code IN ({marks})",
                            [day, *missing_snap],
                        ).fetchall()
                    else:
                        b_rows = con.execute(
                            f"SELECT ts_code, close, pct_chg FROM {bar_cat}.daily_bar "
                            f"WHERE trade_date = ("
                            f"SELECT max(trade_date) FROM {bar_cat}.daily_bar "
                            "WHERE trade_date <= ?)",
                            [day],
                        ).fetchall()
                    for tc, cl, pc in b_rows:
                        bar_snapshot[str(tc)] = (_clean_float(cl), _clean_float(pc))

        mkt_db_con: duckdb.DuckDBPyConnection | None = None
        mkt_file = root / "market.duckdb"
        primary = _primary_db_path()
        if mkt_file.is_file() and not (primary is not None and _same_file(mkt_file, primary)):
            with contextlib.suppress(duckdb.Error):
                mkt_db_con = duckdb.connect(str(mkt_file), read_only=True)
        try:
            extra_by_code = (
                enrich(mkt_db_con, candidate_codes, day)
                if mkt_db_con is not None and len(candidate_codes) <= 1500
                else _bulk_enrich(con, mkt_cols, ro_cols, candidate_codes, day)
            )
        finally:
            if mkt_db_con is not None:
                mkt_db_con.close()

        doc = read_fundamentals(root)
        fund_map = {f.code: f.values for f in doc.rows} if doc else {}
        factor_pct = read_latest(root)

        row_by_code: dict[str, ScreenRow] = {}
        for c in candidate_codes:
            cl, pc = bar_snapshot.get(c, (None, None))
            fvals = fund_map.get(c, {})
            p_str = preset_by_code.get(c, "all_market")
            p_list = presets_by_code.get(c) or [x for x in p_str.split(",") if x]
            row_by_code[c] = ScreenRow(
                trade_date=day,
                code=c,
                name=names_map.get(c) or None,
                preset=p_str,
                presets=p_list,
                close=cl,
                pct_chg=pc,
                **extra_by_code.get(c, {}),
                roe=_clean_float(fvals.get("roe")),
                grossprofit_margin=_clean_float(fvals.get("grossprofit_margin")),
                netprofit_margin=_clean_float(fvals.get("netprofit_margin")),
                netprofit_yoy=_clean_float(fvals.get("netprofit_yoy")),
                or_yoy=_clean_float(fvals.get("or_yoy")),
                debt_to_assets=_clean_float(fvals.get("debt_to_assets")),
                factors={n: v[c] for n, v in factor_pct.items() if c in v},
            )

        need_wide, need_panel, max_offset, ma_periods, rsi_periods, agg_reqs = (
            _collect_rule_needs(body.blocks, custom_map)
        )
        wide_df, panel = _build_wide_and_panel(
            con,
            mkt_cols,
            ro_cols,
            candidate_codes,
            names_map,
            day,
            need_wide,
            need_panel,
            max_offset,
            ma_periods,
            rsi_periods,
            agg_reqs,
        )

    steps: list[ScreenerFunnelStep] = [
        ScreenerFunnelStep(
            step=0,
            block_id=None,
            label=base_label,
            remaining=len(candidate_codes),
            dropped=0,
            dropped_pct=0.0,
        )
    ]
    codes_by_step: list[list[str]] = [list(candidate_codes)]
    current_codes = list(candidate_codes)

    step_idx = 0
    for block in body.blocks:
        if not block.enabled:
            continue
        step_idx += 1
        prev_n = len(current_codes)
        current_codes, err = _eval_single_block(
            block, current_codes, row_by_code, wide_df, panel, custom_map
        )
        rem_n = len(current_codes)
        dropped = max(0, prev_n - rem_n)
        dropped_pct = round((dropped / prev_n) * 100.0, 1) if prev_n > 0 else 0.0
        steps.append(
            ScreenerFunnelStep(
                step=step_idx,
                block_id=block.id or f"step-{step_idx}",
                label=format_block_label(block, custom_map, field_labels),
                remaining=rem_n,
                dropped=dropped,
                dropped_pct=dropped_pct,
                error=err,
            )
        )
        codes_by_step.append(list(current_codes))

    active_step = (
        body.preview_step
        if body.preview_step is not None and 0 <= body.preview_step < len(codes_by_step)
        else len(codes_by_step) - 1
    )
    selected_codes = codes_by_step[active_step]

    # Enrich surviving rows in detail if candidate universe was large (> 1500)
    if len(candidate_codes) > 1500 and selected_codes and day is not None:
        mkt_file = root / "market.duckdb"
        primary = _primary_db_path()
        if mkt_file.is_file() and not (primary is not None and _same_file(mkt_file, primary)):
            with contextlib.suppress(duckdb.Error):
                mcon = duckdb.connect(str(mkt_file), read_only=True)
                try:
                    detailed = enrich(mcon, selected_codes[:500], day)
                    for c, ext in detailed.items():
                        if c in row_by_code:
                            row_by_code[c] = row_by_code[c].model_copy(update=ext)
                finally:
                    mcon.close()

    out_rows = [row_by_code[c] for c in selected_codes if c in row_by_code]
    scores = _score_rows(out_rows, body.ranks)
    if scores:
        out_rows = [
            r.model_copy(update={"score": scores.get(r.code, 0.0)}) for r in out_rows
        ]
        out_rows.sort(key=lambda r: (r.score is not None, r.score or 0.0), reverse=True)
    else:
        out_rows.sort(key=lambda r: (r.pct_chg is not None, r.pct_chg or -999.0), reverse=True)

    return ScreenerRunData(
        trade_date=day,
        active_step=active_step,
        steps=steps,
        rows=out_rows,
        presets=presets,
        pool_refs=pool_refs,
    )


def _bulk_enrich(
    con: duckdb.DuckDBPyConnection,
    mkt_cols: dict[str, set[str]],
    ro_cols: dict[str, set[str]],
    codes: list[str],
    day: date,
) -> dict[str, dict[str, Any]]:
    """Fast whole-market single-day valuation + industry lookup for large universes."""
    out: dict[str, dict[str, Any]] = {c: {} for c in codes}
    sb_cat = "mkt" if "stock_basic" in mkt_cols else ("ro" if "stock_basic" in ro_cols else None)
    if sb_cat:
        with contextlib.suppress(duckdb.Error):
            for tc, ind in con.execute(
                f"SELECT ts_code, industry FROM {sb_cat}.stock_basic"
            ).fetchall():
                if str(tc) in out:
                    out[str(tc)]["industry"] = ind
    db_cat = "mkt" if "daily_basic" in mkt_cols else ("ro" if "daily_basic" in ro_cols else None)
    if db_cat:
        with contextlib.suppress(duckdb.Error):
            for tc, pe, pb, tmv, cmv, tr in con.execute(
                f"SELECT ts_code, pe_ttm, pb, total_mv, circ_mv, turnover_rate "
                f"FROM {db_cat}.daily_basic WHERE trade_date = ("
                f"SELECT max(trade_date) FROM {db_cat}.daily_basic WHERE trade_date <= ?)",
                [day],
            ).fetchall():
                s = str(tc)
                if s in out:
                    out[s].update(
                        pe_ttm=_clean_float(pe),
                        pb=_clean_float(pb),
                        total_mv=None if tmv is None else round(float(tmv) / 1e4, 2),
                        circ_mv=None if cmv is None else round(float(cmv) / 1e4, 2),
                        turnover_rate=_clean_float(tr),
                    )
    return out


# ---- White-Box Natural Language Compiler -------------------------------------

_CN_NUM = {
    "一": 1.0,
    "二": 2.0,
    "两": 2.0,
    "三": 3.0,
    "四": 4.0,
    "五": 5.0,
    "六": 6.0,
    "七": 7.0,
    "八": 8.0,
    "九": 9.0,
    "十": 10.0,
}


def _new_id() -> str:
    return f"b-{uuid.uuid4().hex[:8]}"


def _extract_offset(clause: str) -> int:
    if "前天" in clause or "前日" in clause or "T-2" in clause.upper():
        return 2
    if "昨天" in clause or "昨日" in clause or "昨" in clause or "T-1" in clause.upper():
        return 1
    m = re.search(r"T-(\d+)", clause, flags=re.IGNORECASE)
    if m:
        return min(30, int(m.group(1)))
    return 0


def parse_nl_locally(text: str) -> list[ScreenerBlock]:
    """Deterministic white-box parser for common Chinese quant screening sentences."""
    raw = text.strip()
    if not raw:
        return []

    # Direct TDX formula pasted into the NL box
    with contextlib.suppress(TdxFormulaError):
        if re.search(r"[A-Za-z]", raw) and not re.search(r"[\u4e00-\u9fff]", raw):
            translate_tdx(raw)
            return [ScreenerBlock(id=_new_id(), type="tdx", tdx=raw)]

    blocks: list[ScreenerBlock] = []
    # Check global exclusions first (supports combined phrases like "排除ST和北交所")
    if re.search(r"(排除|剔除|不要|非|去掉)[^，,；;。]{0,12}\*?ST", raw, flags=re.IGNORECASE):
        blocks.append(ScreenerBlock(id=_new_id(), type="rule", rule_name="not_st", args={}))
    if re.search(r"(排除|剔除|不要|非|去掉)[^，,；;。]{0,12}北交", raw):
        blocks.append(ScreenerBlock(id=_new_id(), type="rule", rule_name="not_bj", args={}))

    clauses = [
        c.strip()
        for c in re.split(r"[，,；;。]+|并且|而且|同时|以及|且", raw)
        if c.strip()
    ]
    seen_sigs: set[str] = {f"{b.type}:{b.rule_name}" for b in blocks}

    def add(blk: ScreenerBlock) -> None:
        sig = f"{blk.type}:{blk.rule_name}:{blk.field}:{blk.op}:{blk.args}:{blk.value}:{blk.tdx}"
        if sig not in seen_sigs:
            seen_sigs.add(sig)
            blocks.append(blk)

    for cl in clauses:
        off = _extract_offset(cl)

        # Board whitelist
        if re.search(r"(只看|仅看|限定)", cl) and any(
            k in cl for k in ("主板", "创业板", "科创板")
        ):
            boards: list[str] = []
            if "主板" in cl:
                boards.append("main")
            if "创业板" in cl:
                boards.append("gem")
            if "科创板" in cl:
                boards.append("star")
            if boards:
                add(
                    ScreenerBlock(
                        id=_new_id(), type="rule", rule_name="board_in", args={"boards": boards}
                    )
                )

        # Circulating MV < X 亿
        m_mv = re.search(
            r"(?:流通市值|市值|小盘|微盘)\s*(?:小于|低于|不超过|<|<=|≤)?\s*(\d+(?:\.\d+)?)\s*亿",
            cl,
        )
        if m_mv:
            add(
                ScreenerBlock(
                    id=_new_id(),
                    type="rule",
                    rule_name="circ_mv_lt",
                    args={"threshold_yi": float(m_mv.group(1)), "offset": 0},
                )
            )

        # Window aggregates
        m_nold = re.search(r"近\s*(\d+)\s*[日天]\s*(?:内)?(?:无|没有|未曾?)跌停", cl)
        if m_nold:
            add(
                ScreenerBlock(
                    id=_new_id(),
                    type="rule",
                    rule_name="no_limit_down_in_window",
                    args={"window": int(m_nold.group(1))},
                )
            )
            continue

        m_nocu = re.search(r"近\s*(\d+)\s*[日天]\s*(?:内)?(?:无|避开|没有)\s*(\d+)\s*连板", cl)
        if m_nocu:
            add(
                ScreenerBlock(
                    id=_new_id(),
                    type="rule",
                    rule_name="no_consec_ups_in_window",
                    args={"window": int(m_nocu.group(1)), "threshold": int(m_nocu.group(2))},
                )
            )
            continue

        m_prior = re.search(r"近\s*(\d+)\s*[日天]\s*(?:内)?(?:有过|曾有?|至少一次)涨停", cl)
        if m_prior:
            add(
                ScreenerBlock(
                    id=_new_id(),
                    type="rule",
                    rule_name="has_prior_limit_up",
                    args={"window": int(m_prior.group(1)), "exclude_offset": 1},
                )
            )
            continue

        # Limit-up / consecutive / yiziban states
        if "首板" in cl:
            add(
                ScreenerBlock(
                    id=_new_id(),
                    type="rule",
                    rule_name="first_limit_up",
                    args={"offset": off},
                )
            )
        elif re.search(r"(?:非|不是|排除)一字板", cl):
            add(
                ScreenerBlock(
                    id=_new_id(), type="rule", rule_name="not_yiziban", args={"offset": off}
                )
            )
        elif "一字板" in cl:
            add(
                ScreenerBlock(
                    id=_new_id(), type="rule", rule_name="yiziban", args={"offset": off}
                )
            )
        elif re.search(r"(?:未|不|非|没)涨停", cl):
            add(
                ScreenerBlock(
                    id=_new_id(), type="rule", rule_name="not_limit_up", args={"offset": off}
                )
            )
        elif "涨停" in cl and "曾" not in cl and "有过" not in cl:
            add(
                ScreenerBlock(
                    id=_new_id(), type="rule", rule_name="limit_up", args={"offset": off}
                )
            )
        elif "跌停" in cl and "无跌停" not in cl:
            add(
                ScreenerBlock(
                    id=_new_id(), type="rule", rule_name="limit_down", args={"offset": off}
                )
            )

        m_cu = re.search(r"(\d+)\s*连板", cl)
        if m_cu and "无" not in cl and "避开" not in cl:
            add(
                ScreenerBlock(
                    id=_new_id(),
                    type="rule",
                    rule_name="consecutive_ups_gte",
                    args={"n": int(m_cu.group(1)), "offset": off},
                )
            )

        # MA crossover / above MA
        m_cross_up = re.search(
            r"(?:MA)?(\d+)(?:日线?)?\s*(?:上穿|金叉)\s*(?:MA)?(\d+)", cl, flags=re.IGNORECASE
        )
        if m_cross_up:
            add(
                ScreenerBlock(
                    id=_new_id(),
                    type="rule",
                    rule_name="cross_above",
                    args={
                        "fast": f"MA{m_cross_up.group(1)}",
                        "slow": f"MA{m_cross_up.group(2)}",
                        "offset": off,
                    },
                )
            )
        m_cross_dn = re.search(
            r"(?:MA)?(\d+)(?:日线?)?\s*(?:下穿|死叉)\s*(?:MA)?(\d+)", cl, flags=re.IGNORECASE
        )
        if m_cross_dn:
            add(
                ScreenerBlock(
                    id=_new_id(),
                    type="rule",
                    rule_name="cross_below",
                    args={
                        "fast": f"MA{m_cross_dn.group(1)}",
                        "slow": f"MA{m_cross_dn.group(2)}",
                        "offset": off,
                    },
                )
            )
        m_ma = re.search(
            r"(?:站上|高于|突破|大于)\s*(?:MA)?\s*(\d+)\s*(?:日(?:均)?线)?",
            cl,
            flags=re.IGNORECASE,
        )
        if m_ma and not m_cross_up:
            p = int(m_ma.group(1))
            if 2 <= p <= 250:
                add(
                    ScreenerBlock(
                        id=_new_id(),
                        type="rule",
                        rule_name="above_ma",
                        args={"period": p, "offset": off},
                    )
                )

        # Volume ratio / 放量
        m_vol_cn = re.search(r"放量\s*([一二两三四五六七八九十])\s*倍", cl)
        m_vol_num = re.search(
            r"(?:放量|量比)\s*(?:>=|≥|>|超|大于|至少)?\s*(\d+(?:\.\d+)?)\s*倍?", cl
        )
        if m_vol_cn:
            cn_val = _CN_NUM[m_vol_cn.group(1)]
            # "放量一倍" in A-share parlance means 2x volume (+100%)
            mult = 2.0 if m_vol_cn.group(1) == "一" else cn_val
            add(
                ScreenerBlock(
                    id=_new_id(),
                    type="rule",
                    rule_name="volume_ratio_gte",
                    args={"n": mult, "offset": off, "window": 5},
                )
            )
        elif m_vol_num:
            add(
                ScreenerBlock(
                    id=_new_id(),
                    type="rule",
                    rule_name="volume_ratio_gte",
                    args={"n": float(m_vol_num.group(1)), "offset": off, "window": 5},
                )
            )

        # Lower shadow & RSI
        if "下影" in cl:
            add(
                ScreenerBlock(
                    id=_new_id(),
                    type="rule",
                    rule_name="has_lower_shadow",
                    args={"min_ratio": 1.5, "min_amplitude": 0.02, "offset": off},
                )
            )
        if "超卖" in cl or re.search(r"RSI\s*(?:<|<=|≤|低于)\s*(\d+)", cl, flags=re.IGNORECASE):
            m_r = re.search(r"(\d+(?:\.\d+)?)", cl)
            th = float(m_r.group(1)) if m_r and float(m_r.group(1)) <= 50 else 30.0
            add(
                ScreenerBlock(
                    id=_new_id(),
                    type="rule",
                    rule_name="rsi_oversold",
                    args={"period": 14, "threshold": th, "offset": off},
                )
            )
        elif "超买" in cl or re.search(r"RSI\s*(?:>|>=|≥|高于)\s*(\d+)", cl, flags=re.IGNORECASE):
            m_r = re.search(r"(\d+(?:\.\d+)?)", cl)
            th = float(m_r.group(1)) if m_r and float(m_r.group(1)) >= 50 else 70.0
            add(
                ScreenerBlock(
                    id=_new_id(),
                    type="rule",
                    rule_name="rsi_overbought",
                    args={"period": 14, "threshold": th, "offset": off},
                )
            )

        # Fundamental & market numeric fields
        for pat, fld in (
            (r"ROE\s*(?:>=|≥|>|大于|高于)\s*(-?\d+(?:\.\d+)?)", "roe"),
            (
                r"(?:净利同比|净利润同比)\s*(?:>=|≥|>|大于|高于)\s*(-?\d+(?:\.\d+)?)",
                "netprofit_yoy",
            ),
            (r"营收同比\s*(?:>=|≥|>|大于|高于)\s*(-?\d+(?:\.\d+)?)", "or_yoy"),
            (r"毛利率\s*(?:>=|≥|>|大于|高于)\s*(-?\d+(?:\.\d+)?)", "grossprofit_margin"),
            (r"换手率?\s*(?:>=|≥|>|大于|高于)\s*(\d+(?:\.\d+)?)", "turnover_rate"),
            (r"涨跌幅?\s*(?:>=|≥|>|大于|高于)\s*(-?\d+(?:\.\d+)?)", "pct_chg"),
        ):
            mf = re.search(pat, cl, flags=re.IGNORECASE)
            if mf:
                add(
                    ScreenerBlock(
                        id=_new_id(),
                        type="field",
                        field=fld,
                        op=">=",
                        value=float(mf.group(1)),
                    )
                )
        for pat, fld in (
            (r"PE\s*(?:<=|≤|<|小于|低于)\s*(\d+(?:\.\d+)?)", "pe_ttm"),
            (r"PB\s*(?:<=|≤|<|小于|低于)\s*(\d+(?:\.\d+)?)", "pb"),
            (r"资产负债率\s*(?:<=|≤|<|小于|低于)\s*(\d+(?:\.\d+)?)", "debt_to_assets"),
            (r"涨跌幅?\s*(?:<=|≤|<|小于|低于)\s*(-?\d+(?:\.\d+)?)", "pct_chg"),
        ):
            mf = re.search(pat, cl, flags=re.IGNORECASE)
            if mf:
                add(
                    ScreenerBlock(
                        id=_new_id(),
                        type="field",
                        field=fld,
                        op="<=",
                        value=float(mf.group(1)),
                    )
                )

    return blocks


def _build_nl_system_prompt() -> str:
    rule_lines: list[str] = []
    for s in REGISTRY:
        props = s.args_model.model_json_schema().get("properties", {})
        arg_sig = ", ".join(props.keys())
        rule_lines.append(f"- {s.name}({arg_sig}): {s.description}")
    rules_doc = "\n".join(rule_lines)
    return (
        "你是 A 股量化选股白盒编译器。把用户的一句话选股条件拆解为 JSON 对象：\n"
        '{"blocks": [ ... ]}\n'
        "每个元素优先使用已注册积木（type='rule'），其次使用财务/行情字段（type='field'），"
        "只有积木无法表达的量价算式才用通达信公式（type='tdx'）。只输出合法 JSON，不要解释。\n\n"
        "1) type='rule' 格式: {\"type\":\"rule\",\"rule_name\":\"...\",\"args\":{...}}\n"
        f"可用 rule_name:\n{rules_doc}\n\n"
        "2) type='field' 格式: {\"type\":\"field\",\"field\":\"roe|netprofit_yoy|or_yoy|"
        "grossprofit_margin|debt_to_assets|pe_ttm|pb|pct_chg|turnover_rate\","
        "\"op\":\">=|<=|between|top_pct\",\"value\":数字}\n\n"
        "3) type='tdx' 格式: {\"type\":\"tdx\",\"tdx\":\"C>MA(C,20) AND V>MA(V,5)*2\"} "
        "（仅限 C O H L V AMOUNT，函数 MA EMA REF HHV LLV SUM STD ABS CROSS）。"
    )


def _synthesize_expression_preview(blocks: list[ScreenerBlock]) -> tuple[str | None, str | None]:
    tdx_parts: list[str] = []
    expr_parts: list[str] = []
    for b in blocks:
        if b.type == "tdx" and b.tdx:
            tdx_parts.append(b.tdx)
            with contextlib.suppress(TdxFormulaError):
                expr_parts.append(translate_tdx(b.tdx))
        elif b.type == "rule":
            a = b.args or {}
            if b.rule_name == "above_ma":
                p = int(a.get("period", 20))
                tdx_parts.append(f"C>MA(C,{p})")
                expr_parts.append(f"close > ts_mean(close,{p})")
            elif b.rule_name == "volume_ratio_gte":
                n = a.get("n", 2)
                w = int(a.get("window", 5))
                tdx_parts.append(f"V>=MA(V,{w})*{n:g}")
                expr_parts.append(f"vol >= ts_mean(vol,{w}) * {n:g}")
            elif b.rule_name == "cross_above":
                f_ma = str(a.get("fast", "MA5")).removeprefix("MA")
                s_ma = str(a.get("slow", "MA20")).removeprefix("MA")
                if f_ma.isdigit() and s_ma.isdigit():
                    tdx = f"CROSS(MA(C,{f_ma}),MA(C,{s_ma}))"
                    tdx_parts.append(tdx)
                    with contextlib.suppress(TdxFormulaError):
                        expr_parts.append(translate_tdx(tdx))
            else:
                expr_parts.append(f"{b.rule_name}({json.dumps(a, ensure_ascii=False)})")
        elif b.type == "field":
            expr_parts.append(f"{b.field} {b.op} {b.value:g}")
    return (
        " AND ".join(tdx_parts) if tdx_parts else None,
        " and ".join(expr_parts) if expr_parts else None,
    )


def _validate_compiled_blocks(raw_blocks: list[Any]) -> tuple[list[ScreenerBlock], list[str]]:
    valid: list[ScreenerBlock] = []
    errors: list[str] = []
    for item in raw_blocks:
        if not isinstance(item, dict):
            continue
        btype = item.get("type")
        if btype == "rule":
            rname = str(item.get("rule_name") or "")
            spec: RuleSpec | None = REGISTRY_BY_NAME.get(rname)
            if spec is None:
                errors.append(f"未知积木：{rname}")
                continue
            try:
                args = spec.args_model.model_validate(item.get("args") or {}).model_dump()
            except Exception as exc:  # noqa: BLE001
                errors.append(f"积木 {rname} 参数不合法：{exc}")
                continue
            valid.append(ScreenerBlock(id=_new_id(), type="rule", rule_name=rname, args=args))
        elif btype == "tdx":
            tdx = str(item.get("tdx") or "").strip()
            try:
                translate_tdx(tdx)
                valid.append(ScreenerBlock(id=_new_id(), type="tdx", tdx=tdx))
            except TdxFormulaError as exc:
                errors.append(str(exc))
        elif btype == "field":
            fld = str(item.get("field") or "")
            op = str(item.get("op") or ">=")
            if fld not in BASE_FIELD_LABELS and not fld.startswith("f:"):
                errors.append(f"未知字段：{fld}")
                continue
            if op not in (">=", "<=", "between", "top_pct", "bottom_pct"):
                op = ">="
            valid.append(
                ScreenerBlock(
                    id=_new_id(),
                    type="field",
                    field=fld,
                    op=op,  # type: ignore[arg-type]
                    value=float(item.get("value", 0.0)),
                    value2=_clean_float(item.get("value2")),
                )
            )
    return valid, errors


def compile_nl_to_blocks(
    text: str,
    llm: tuple[Complete, str] | None,
) -> ScreenerNlCompileData:
    """Compile natural language into validated white-box ``ScreenerBlock`` items."""
    model_used = "local-whitebox"
    blocks: list[ScreenerBlock] = []
    errors: list[str] = []

    if llm is not None:
        complete, model_used = llm
        reply = complete(
            [
                {"role": "system", "content": _build_nl_system_prompt()},
                {"role": "user", "content": text},
            ]
        )
        cleaned = reply.strip()
        if cleaned.startswith("```"):
            cleaned = re.sub(r"^```(?:json|tdx)?\s*", "", cleaned, flags=re.IGNORECASE)
            cleaned = re.sub(r"\s*```$", "", cleaned)
        parsed_json: Any = None
        with contextlib.suppress(ValueError):
            parsed_json = json.loads(cleaned)
        if isinstance(parsed_json, dict) and isinstance(parsed_json.get("blocks"), list):
            blocks, errors = _validate_compiled_blocks(parsed_json["blocks"])
        elif isinstance(parsed_json, list):
            blocks, errors = _validate_compiled_blocks(parsed_json)
        else:
            # LLM or demo mock returned a TDX line directly: combine local white-box blocks if
            # matched, or validate the TDX formula into a tdx block.
            local = parse_nl_locally(text)
            tdx_line = cleaned.removeprefix("tdx").strip().splitlines()[0].strip()[:500]
            try:
                expr = translate_tdx(tdx_line)
                blocks = local or [ScreenerBlock(id=_new_id(), type="tdx", tdx=tdx_line)]
                tdx_syn, expr_syn = _synthesize_expression_preview(blocks)
                labels = [format_block_label(b, {}, BASE_FIELD_LABELS) for b in blocks]
                return ScreenerNlCompileData(
                    blocks=blocks,
                    summary=" · ".join(labels),
                    tdx=tdx_syn or tdx_line,
                    expression=expr_syn or expr,
                    error=None,
                    model=model_used,
                )
            except TdxFormulaError as exc:
                if local:
                    blocks = local
                else:
                    errors.append(str(exc))

    if not blocks:
        blocks = parse_nl_locally(text)

    if not blocks:
        err_msg = (
            "；".join(errors)
            if errors
            else (
                "未能识别为有效选股积木或通达信公式"
                "（可尝试：排除ST，近30日无跌停，昨日首板，流通市值<80亿，站上20日线且放量2倍）"
            )
        )
        return ScreenerNlCompileData(
            blocks=[],
            summary="",
            tdx=None,
            expression=None,
            error=err_msg,
            model=model_used,
        )

    tdx_syn, expr_syn = _synthesize_expression_preview(blocks)
    labels = [format_block_label(b, {}, BASE_FIELD_LABELS) for b in blocks]
    return ScreenerNlCompileData(
        blocks=blocks,
        summary=" · ".join(labels),
        tdx=tdx_syn,
        expression=expr_syn,
        error="；".join(errors) if errors else None,
        model=model_used,
    )


__all__ = [
    "build_field_groups",
    "build_rule_catalog",
    "compile_nl_to_blocks",
    "format_block_label",
    "list_catalog",
    "parse_nl_locally",
    "run_screener",
    "table_missing",
]
