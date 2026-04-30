"""风险控制名单：PDF 解析 + DuckDB 落库 + 过滤 / 标签。

数据模型：每条 blacklist 项 = (list_label, ts_code) 主键。
同一只票可命中多个 sub_categories（如华夏幸福同时属于"净资产为负"+"年报审计"），
导入时按 ts_code 合并。
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
from loguru import logger

from rquant.storage.duckdb import DuckDBStore

# 默认黑名单有效期 = 1 年
DEFAULT_VALIDITY_DAYS = 365


@dataclass
class BlacklistEntry:
    """一条黑名单条目（导入前的中间结构）。"""
    ts_code: str
    name: str
    risk_type: str
    sub_categories: list[str]


@dataclass
class BlacklistHit:
    """命中查询返回结构。"""
    ts_code: str
    name: str
    list_label: str
    sub_categories: list[str]
    risk_type: str
    imported_at: date
    expires_at: date

    @property
    def is_expired(self) -> bool:
        return date.today() > self.expires_at

    @property
    def label_short(self) -> str:
        """简短标签，用于推送 subject 前缀。"""
        return f"[{self.list_label}]"


# ---------- 代码标准化 ----------

def normalize_ts_code(raw_code: str) -> str:
    """6 位代码 + 交易所后缀。

    - 92xxxx → BJ（北交所）
    - 688/689 → SH（科创板）
    - 600/601/603/605 → SH
    - 000/001/002/003/300/301 → SZ
    - 其他默认 SZ（保守）
    """
    code = str(raw_code).strip().zfill(6)

    if not code.isdigit() or len(code) != 6:
        raise ValueError(f"非法股票代码：{raw_code}")

    if code.startswith("92"):
        suffix = "BJ"
    elif code.startswith(("600", "601", "603", "605", "688", "689")):
        suffix = "SH"
    else:
        suffix = "SZ"

    return f"{code}.{suffix}"


# ---------- PDF 解析 ----------

# 行格式：股票名称 股票代码 预警类别 细分类别
# 名称和代码之间可能没有空格（如 "新宁创智联300078"）
_LINE_RE = re.compile(
    r"^(?P<name>.+?)\s*(?P<code>\d{1,6})\s+(?P<risk>ST预警)\s+(?P<sub>\S.+?)$"
)

# 噪音行：水印、声明、表头、空行
_NOISE_PREFIXES = ("仅供学习", "及交易", "不涉及交易", "【信息来源】", "【免责声明】", "股票名称")


def parse_blacklist_pdf(pdf_path: Path) -> list[BlacklistEntry]:
    """解析风险控制名单 PDF，按 ts_code 合并多类别。"""
    from pypdf import PdfReader

    pdf_path = Path(pdf_path)
    if not pdf_path.exists():
        raise FileNotFoundError(pdf_path)

    reader = PdfReader(str(pdf_path))
    raw_text = "\n".join(page.extract_text() or "" for page in reader.pages)

    # ts_code → BlacklistEntry（合并 sub_categories）
    by_code: dict[str, BlacklistEntry] = {}
    skipped = 0

    for line in raw_text.splitlines():
        line = line.strip()
        if not line or any(line.startswith(p) for p in _NOISE_PREFIXES):
            continue

        m = _LINE_RE.match(line)
        if not m:
            skipped += 1
            continue

        try:
            ts_code = normalize_ts_code(m.group("code"))
        except ValueError:
            skipped += 1
            continue

        name = m.group("name").strip()
        sub = m.group("sub").strip()
        risk = m.group("risk").strip()

        existing = by_code.get(ts_code)
        if existing:
            if sub not in existing.sub_categories:
                existing.sub_categories.append(sub)
        else:
            by_code[ts_code] = BlacklistEntry(
                ts_code=ts_code,
                name=name,
                risk_type=risk,
                sub_categories=[sub],
            )

    entries = list(by_code.values())
    logger.info(
        f"解析 {pdf_path.name}: {len(entries)} 个独立标的"
        f"（已合并多类别），跳过 {skipped} 行噪音"
    )
    return entries


# ---------- DuckDB 落库 ----------

def import_blacklist(
    entries: list[BlacklistEntry],
    list_label: str,
    source_file: str,
    store: DuckDBStore,
    imported_at: date | None = None,
    validity_days: int = DEFAULT_VALIDITY_DAYS,
    replace: bool = True,
) -> int:
    """将黑名单条目落库 risk_blacklist 表。

    :param replace: 默认 True，先删除同 list_label 的旧数据再写新数据。
    """
    if not entries:
        return 0

    imported_at = imported_at or date.today()
    expires_at = imported_at + timedelta(days=validity_days)

    if replace:
        store._conn.execute(
            "DELETE FROM risk_blacklist WHERE list_label = ?", [list_label]
        )

    rows = [
        {
            "list_label": list_label,
            "ts_code": e.ts_code,
            "name": e.name,
            "sub_categories": e.sub_categories,
            "risk_type": e.risk_type,
            "source_file": source_file,
            "imported_at": imported_at,
            "expires_at": expires_at,
        }
        for e in entries
    ]
    df = pd.DataFrame(rows)
    store._conn.register("blacklist_tmp", df)
    store._conn.execute(
        """
        INSERT INTO risk_blacklist
            (list_label, ts_code, name, sub_categories, risk_type,
             source_file, imported_at, expires_at)
        SELECT
            list_label, ts_code, name, sub_categories, risk_type,
            source_file, imported_at, expires_at
        FROM blacklist_tmp
        """
    )
    store._conn.unregister("blacklist_tmp")

    logger.info(
        f"导入黑名单 '{list_label}': {len(entries)} 只，"
        f"有效期 {imported_at} → {expires_at}"
    )
    return len(entries)


# ---------- Parquet 导入 / 导出（mac → 云端 round-trip） ----------

def load_blacklist_parquet(
    parquet_path: Path,
    store: DuckDBStore,
    list_label: str | None = None,
) -> int:
    """从 parquet 加载黑名单到 DuckDB risk_blacklist 表。

    用于 v0.10.0 upload-api 推送后云端落库。

    :param parquet_path: parquet 文件路径，columns 必须匹配 risk_blacklist schema
    :param list_label: 仅替换该 label 的行（默认替换全表，便于跨 label 整体覆盖）
    :return: 落库后该 label（或全表）的总行数
    :raises ValueError: 文件不存在或 schema 不匹配（DuckDB 错误透传）
    """
    if not parquet_path.exists():
        raise FileNotFoundError(f"parquet 文件不存在: {parquet_path}")

    if list_label:
        store._conn.execute(
            "DELETE FROM risk_blacklist WHERE list_label = ?", [list_label]
        )
    else:
        store._conn.execute("DELETE FROM risk_blacklist")

    store._conn.execute(
        "INSERT INTO risk_blacklist SELECT * FROM read_parquet(?)",
        [str(parquet_path)],
    )

    if list_label:
        n = store._conn.execute(
            "SELECT COUNT(*) FROM risk_blacklist WHERE list_label = ?",
            [list_label],
        ).fetchone()[0]
    else:
        n = store._conn.execute(
            "SELECT COUNT(*) FROM risk_blacklist"
        ).fetchone()[0]

    logger.info(
        f"从 parquet 加载黑名单: {n} 行 → {'list_label=' + list_label if list_label else '全表'}"
    )
    return n


def export_blacklist_parquet(
    store: DuckDBStore,
    output_path: Path,
    list_label: str | None = None,
) -> int:
    """导出 risk_blacklist 表到 parquet（用于 mac → 云端推送）。

    :param output_path: 导出文件路径（父目录必须存在）
    :param list_label: 仅导出该 label（默认导出全表）
    :return: 导出的行数
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if list_label:
        # CREATE VIEW 不支持 prepared param（DuckDB 限制），
        # 改成 fetch → register DataFrame → COPY，全程参数绑定
        df = store._conn.execute(
            "SELECT * FROM risk_blacklist WHERE list_label = ?", [list_label]
        ).fetchdf()
        n = len(df)
        store._conn.register("_bl_export_df", df)
        store._conn.execute(
            "COPY _bl_export_df TO ? (FORMAT PARQUET)", [str(output_path)]
        )
        store._conn.unregister("_bl_export_df")
    else:
        n = store._conn.execute("SELECT COUNT(*) FROM risk_blacklist").fetchone()[0]
        store._conn.execute(
            "COPY risk_blacklist TO ? (FORMAT PARQUET)", [str(output_path)]
        )

    logger.info(
        f"导出黑名单 parquet: {n} 行 → {output_path} "
        f"({'list_label=' + list_label if list_label else '全表'})"
    )
    return n


# ---------- 查询 / 过滤 / 标签 ----------

def load_active_blacklist(
    store: DuckDBStore,
    today: date | None = None,
    include_expired: bool = False,
    list_label: str | None = None,
) -> dict[str, BlacklistHit]:
    """加载有效黑名单（默认排除过期），key = ts_code。

    多个 list_label 命中同一 ts_code 时，保留 expires_at 最晚的那条。
    """
    today = today or date.today()
    where = []
    params: list = []
    if not include_expired:
        where.append("expires_at >= ?")
        params.append(today)
    if list_label is not None:
        where.append("list_label = ?")
        params.append(list_label)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    df = store._conn.execute(
        f"""
        SELECT list_label, ts_code, name, sub_categories, risk_type,
               imported_at, expires_at
        FROM risk_blacklist
        {where_sql}
        ORDER BY expires_at DESC
        """,
        params,
    ).fetchdf()

    out: dict[str, BlacklistHit] = {}
    for _, row in df.iterrows():
        code = row["ts_code"]
        if code in out:
            continue  # 第一条已是 expires 最晚
        sub = row["sub_categories"]
        sub_list = list(sub) if sub is not None else []
        out[code] = BlacklistHit(
            ts_code=code,
            name=row["name"] or "",
            list_label=row["list_label"],
            sub_categories=sub_list,
            risk_type=row["risk_type"] or "",
            imported_at=_to_date(row["imported_at"]),
            expires_at=_to_date(row["expires_at"]),
        )
    return out


def filter_blacklist(
    ts_codes: Iterable[str],
    blacklist: dict[str, BlacklistHit] | None = None,
    store: DuckDBStore | None = None,
) -> tuple[list[str], list[BlacklistHit]]:
    """硬过滤：返回 (clean_codes, removed_hits)。

    用于 Pool 1 / Pool 2 / watchlist 的 **新推荐** 落库前——黑名单内的票直接剔除。
    """
    if blacklist is None:
        if store is None:
            raise ValueError("必须提供 blacklist 或 store 之一")
        blacklist = load_active_blacklist(store)

    clean: list[str] = []
    removed: list[BlacklistHit] = []
    for code in ts_codes:
        hit = blacklist.get(code)
        if hit:
            removed.append(hit)
        else:
            clean.append(code)
    return clean, removed


def annotate_blacklist(
    ts_codes: Iterable[str],
    blacklist: dict[str, BlacklistHit] | None = None,
    store: DuckDBStore | None = None,
) -> list[tuple[str, BlacklistHit | None]]:
    """软标签：返回 [(ts_code, hit_or_none), ...]。

    用于 monitor 已持仓盯盘 + dashboard 展示——保留全部，命中的打标签。
    """
    if blacklist is None:
        if store is None:
            raise ValueError("必须提供 blacklist 或 store 之一")
        blacklist = load_active_blacklist(store)

    return [(code, blacklist.get(code)) for code in ts_codes]


# ---------- 工具 ----------

def _to_date(value) -> date:
    # 注意：pd.Timestamp / datetime 都是 date 的子类，必须先 narrow
    if isinstance(value, datetime):
        return value.date()
    if hasattr(value, "date") and callable(value.date):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value)
    raise TypeError(f"无法转换为 date: {value!r}")
