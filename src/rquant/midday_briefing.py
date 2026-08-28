"""盘中 30 分钟脉搏 + 午间战报（morning-pulse / midday-report）。

设计要点（对齐 docs/plans/2026-07-06-midday-briefing.md）：

- **绝不写 DuckDB 主库**：实时数据从外部源现拉（复用 panorama_data 三级路由），
  T-1 数据只走 ``open_readonly_store()``（副本优先）；自产数据全部落 parquet
  （``data/midday/``）+ markdown（``data/reports/midday/``）+ meta.json；
- **全机单一取数者**：实时快照/资金流优先读全景 poller 的共享 drop
  （``data/panorama_live/``，≤300s 新鲜才用，route 标注「共享:{原route}」）；
  drop 陈旧/缺失才自拉三级路由，总失败 sleep 60s 重试一次后再降级——
  2026-07-06 下午办公网 IP 因全机多进程高频访问被东财+sina 双风控，
  midday CLI 独立再拉一遍只会雪上加霜（见 fetch_slot_frames）；
- 槽位守卫：pulse 10:00/10:30/11:00/11:30，digest 12:00；自动归槽容差 ±5min，
  迟到 >10min 跳过（Mac 睡过头的过期脉搏没意义）；
- 增量（Δ）来自与上一槽位 parquet 对比，首槽无前项只报绝对值；
- 连板现算：昨 N 板（limit_list_daily T-1）+ 今涨停 = N+1 板；
- 候选池单位对齐：daily_bar.amount 单位千元，×1000 对齐快照元（见 build_candidate_pool）。

parquet 引擎：venv 实测已装 pyarrow（24.0.0），走 parquet；两引擎都缺时降级
pickle（``_FRAME_FORMAT`` 单点决定，读写封装在 _write_frame / _read_frame）。
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone
from math import isfinite
from pathlib import Path
from typing import Literal

import pandas as pd
from loguru import logger
from pydantic import BaseModel

from rquant.config import settings
from rquant.monitor import is_trading_day
from rquant.notify import notify
from rquant.panorama_data import (
    BOARD_SYSTEMS,
    add_limit_prices,
    build_board_overview,
    compute_market_pulse,
    fetch_market_snapshot,
    fetch_sector_fund_flow,
    industry_fallback_members,
    load_board_members,
    load_kpl_concept_members,
)
from rquant.panorama_poller import (
    FLOW_TYPES,
    LIVE_DIR_NAME,
    SNAPSHOT_KEY,
    read_live_frame,
    read_live_meta,
)
from rquant.state.derive import _classify_board
from rquant.storage.duckdb import DuckDBStore, open_readonly_store

# ── 常量 ───────────────────────────────────────────────────────────────────────

CST = timezone(timedelta(hours=8))  # A 股墙钟（Asia/Shanghai）

PULSE_SLOTS: tuple[str, ...] = ("10:00", "10:30", "11:00", "11:30")
DIGEST_SLOT = "12:00"
_SLOT_TOLERANCE_MIN = 5   # 早到容差：now 落在 [slot-5, slot] 也归该槽（时钟抖动）
_LATE_SKIP_MIN = 10       # 迟到 >10min 该槽作废（过期脉搏无意义）

PRICE_TOL = 0.01          # 涨停判定容差（对齐 panorama compute_market_pulse）
_CANDIDATE_VOL_RATIO_MIN = 0.8   # 半日量比阈值（半日额 ≥ 0.8×20 日全日均额）
_CANDIDATE_TOP_N = 20
_PULSE_ANOMALY_TOP_N = 5
_THEME_TOP_N = 5
_NEW_LIMIT_UP_MAX = 8
_MAX_THEME_LADDER_BOARDS = 20

_KPL_SYSTEM = "开盘啦题材"
_SYSTEM_FLOW_TYPE: dict[str, str | None] = {
    "东财行业": "行业资金流",
    "东财概念": "概念资金流",
    _KPL_SYSTEM: None,
}

_SHARED_MAX_AGE_SECONDS = 300   # 共享 drop 新鲜度门槛（poller 60s 一轮，5 轮内可用）
_RETRY_SLEEP_SECONDS = 60       # 自拉总失败后的重试间隔（仍在槽位容差内）
_sleep = time.sleep             # 可注入（单测替换，不真等 60s）

# parquet 引擎单点探测：pyarrow / fastparquet 任一可用即 parquet，否则降级 pickle
try:
    import pyarrow  # noqa: F401

    _FRAME_FORMAT = "parquet"
    _FRAME_ENGINE = "pyarrow"
except ImportError:  # pragma: no cover - venv 已装 pyarrow，降级分支难触发
    try:
        import fastparquet  # noqa: F401

        _FRAME_FORMAT = "parquet"
        _FRAME_ENGINE = "fastparquet"
    except ImportError:
        _FRAME_FORMAT = "pickle"
        _FRAME_ENGINE = ""

_FRAME_SUFFIX = ".parquet" if _FRAME_FORMAT == "parquet" else ".pkl"


def _fake_enabled() -> bool:
    """与 panorama_data 同一开关：RQUANT_PANORAMA_FAKE=1 时数据层返回确定性 fixture。"""
    import os

    return os.environ.get("RQUANT_PANORAMA_FAKE", "").strip() == "1"


# ── 跨层数据结构（Pydantic） ────────────────────────────────────────────────────


class SlotMeta(BaseModel):
    """meta.json 单槽位记录。"""

    fetched_at: str = ""
    route: str = "none"
    pushed: bool = False


class NewLimitUp(BaseModel):
    ts_code: str
    name: str
    theme: str = ""


class ThemeHeat(BaseModel):
    theme: str
    limit_up_count: int
    delta: int | None = None  # 首槽无前项 → None
    amount: float = 0.0


class VolAnomaly(BaseModel):
    ts_code: str
    name: str
    vol_ratio: float


class PulseView(BaseModel):
    """渲染 pulse 报文所需的全部结构（compute 层产出，render 层消费）。"""

    slot_hhmm: str
    has_prev: bool
    limit_up_count: int
    broken_count: int
    limit_down_count: int
    up_count: int
    down_count: int
    limit_up_delta: int | None = None
    broken_delta: int | None = None
    new_limit_ups: list[NewLimitUp] = []
    theme_ladders: list[ThemeLadderSummary] = []
    theme_heat: list[ThemeHeat] = []
    new_anomalies: list[VolAnomaly] = []
    degraded: bool = False  # 快照三路全失败 → 降级短讯
    route: str = ""         # 数据来源（"共享:xxx" 时报文尾标注）


class LadderStock(BaseModel):
    ts_code: str
    name: str
    boards: int  # 连板高度（今日现算）
    theme: str = ""


class ThemeLadderRung(BaseModel):
    boards: int
    stocks: list[LadderStock] = []


class ThemeLadderSummary(BaseModel):
    theme: str
    limit_up_count: int
    amount: float
    rungs: list[ThemeLadderRung] = []
    slot_series: list[int | None] = []
    rank_change: int | None = None
    rank_state: Literal["hidden", "new", "changed"] = "hidden"


class ThemeRank(BaseModel):
    theme: str
    limit_up_count: int
    amount: float
    slot_series: list[int | None] = []  # 上午四槽位涨停数演变


class CandidateStock(BaseModel):
    ts_code: str
    name: str
    theme: str = ""
    vol_ratio: float
    pct_chg: float
    room_to_limit_pct: float  # 距涨停空间（%）


class PositionCheck(BaseModel):
    ts_code: str
    name: str
    pnl_pct: float
    dist_stop_pct: float
    board_note: str = ""


class DigestView(BaseModel):
    """渲染 digest 报文所需的五节结构。"""

    day: date
    limit_up_count: int
    broken_count: int
    limit_down_count: int
    up_count: int
    down_count: int
    broken_ratio_pct: float | None = None
    slot_limit_up_series: list[tuple[str, int | None]] = []  # (hhmm, 涨停数)
    prev_day_limit_up: int | None = None
    theme_ladders: list[ThemeLadderSummary] = []
    ladder: list[LadderStock] = []
    top_themes: list[ThemeRank] = []
    candidates: list[CandidateStock] = []
    positions: list[PositionCheck] = []
    degraded: bool = False
    route: str = ""         # 数据来源（"共享:xxx" 时报文尾标注）


# ── 槽位归属 ────────────────────────────────────────────────────────────────────


def _slot_tag(hhmm: str) -> str:
    """"10:30" → "1030"（parquet/meta 文件名槽位键）。"""
    return hhmm.replace(":", "")


def _slot_hhmm(tag: str) -> str:
    """"1030" → "10:30"（展示用）。"""
    return f"{tag[:2]}:{tag[2:]}"


def _to_minutes(hhmm: str) -> int:
    hour, minute = hhmm.split(":")
    return int(hour) * 60 + int(minute)


def resolve_slot(now: datetime, *, explicit: str | None = None) -> tuple[str | None, str | None]:
    """归槽：返回 (slot_tag, skip_reason)。

    - explicit（``--slot HH:MM``）：手动补跑，直接归该槽（跳过时间守卫），非法值抛；
    - 自动模式：now 落在某槽 [slot-5min, slot+10min] 归该槽（取 |diff| 最小）；
      不落任何槽（迟到 >10min / 早于首槽 / 午间 pulse 间隙）→ (None, 原因)。
    """
    if explicit is not None:
        norm = explicit.strip()
        if norm not in PULSE_SLOTS:
            raise ValueError(f"--slot 必须是 {PULSE_SLOTS} 之一，收到 {explicit!r}")
        return _slot_tag(norm), None

    now_min = now.hour * 60 + now.minute
    best: tuple[int, str] | None = None
    for hhmm in PULSE_SLOTS:
        diff = now_min - _to_minutes(hhmm)  # 正=迟到，负=早到
        if (-_SLOT_TOLERANCE_MIN <= diff <= _LATE_SKIP_MIN) and (
            best is None or abs(diff) < abs(best[0])
        ):
            best = (diff, hhmm)
    if best is None:
        return None, f"{now:%H:%M} 不在任何 pulse 槽位容差内（迟到>10min 或非脉搏时段）"
    return _slot_tag(best[1]), None


def _prev_pulse_tag(tag: str) -> str | None:
    """当前槽位的上一 pulse 槽（首槽返回 None）。"""
    hhmm = _slot_hhmm(tag)
    if hhmm not in PULSE_SLOTS:
        return None
    idx = PULSE_SLOTS.index(hhmm)
    return _slot_tag(PULSE_SLOTS[idx - 1]) if idx > 0 else None


# ── 落盘布局 + 读写封装 ─────────────────────────────────────────────────────────


def _base_dir(base: Path | None) -> Path:
    return base if base is not None else settings.data_dir


def _midday_dir(base: Path | None, day: date) -> Path:
    return _base_dir(base) / "midday" / day.isoformat()


def _reports_dir(base: Path | None) -> Path:
    return _base_dir(base) / "reports" / "midday"


def _frame_path(mdir: Path, kind: str, tag: str) -> Path:
    return mdir / f"{kind}_{tag}{_FRAME_SUFFIX}"


def _meta_path(mdir: Path) -> Path:
    return mdir / "meta.json"


def _write_frame(df: pd.DataFrame, path: Path) -> None:
    """DataFrame 落盘（parquet 优先，无引擎降级 pickle）。df.attrs 不随 parquet 持久化，
    route 单独存 meta.json。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    if _FRAME_FORMAT == "parquet":
        df.to_parquet(path, engine=_FRAME_ENGINE, index=False)
    else:
        df.to_pickle(path)


def _read_frame(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    try:
        if _FRAME_FORMAT == "parquet":
            return pd.read_parquet(path, engine=_FRAME_ENGINE)
        return pd.read_pickle(path)
    except Exception as e:
        logger.warning(f"读取落盘帧失败 {path}: {type(e).__name__}: {e}")
        return None


def _read_meta(mdir: Path) -> dict[str, SlotMeta]:
    path = _meta_path(mdir)
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return {k: SlotMeta(**v) for k, v in raw.items()}
    except Exception as e:
        logger.warning(f"读取 meta.json 失败 {path}: {type(e).__name__}: {e}")
        return {}


def _write_meta(mdir: Path, meta: dict[str, SlotMeta]) -> None:
    mdir.mkdir(parents=True, exist_ok=True)
    payload = {k: v.model_dump() for k, v in meta.items()}
    _meta_path(mdir).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def _upsert_report_section(base: Path | None, day: date, header: str, content: str) -> Path:
    """把一节内容按 header 幂等写入当日报告（同 header 存在则替换，否则追加）。"""
    rdir = _reports_dir(base)
    rdir.mkdir(parents=True, exist_ok=True)
    path = rdir / f"{day.isoformat()}.md"
    text = path.read_text(encoding="utf-8") if path.exists() else f"# {day.isoformat()} 盘中简报\n"
    lines = text.split("\n")
    section = [header, "", *content.split("\n"), ""]

    start = next((i for i, ln in enumerate(lines) if ln.strip() == header), None)
    if start is None:
        new_lines = [*lines, "", *section]
    else:
        end = start + 1
        while end < len(lines) and not lines[end].startswith("## "):
            end += 1
        new_lines = lines[:start] + section + lines[end:]
    path.write_text("\n".join(new_lines), encoding="utf-8")
    return path


# ── 快照 + 板块聚合获取（① 共享 drop → ② 自拉+重试 → ③ 降级） ─────────────────


def _load_members(store: DuckDBStore | None = None) -> pd.DataFrame:
    """东财成分：dc_board_member 优先，空则降级 stock_basic.industry 粗分。"""
    try:
        members = load_board_members(store)
    except Exception as e:
        logger.warning(f"东财板块成分读取失败: {type(e).__name__}: {e}")
        members = pd.DataFrame()
    if not members.empty:
        return members
    try:
        return industry_fallback_members(store)
    except Exception as e:
        logger.warning(f"行业板块成分兜底失败: {type(e).__name__}: {e}")
        return pd.DataFrame()


def _meta_age_seconds(entry: dict, now: datetime) -> float | None:
    """drop meta 条目的 as_of 距 now 的秒数；时间解析失败 → None（视为不可用）。"""
    try:
        as_of = datetime.fromisoformat(str(entry.get("as_of_iso", "")))
    except ValueError:
        return None
    if as_of.tzinfo is None:
        as_of = as_of.replace(tzinfo=CST)
    if now.tzinfo is None:
        now = now.replace(tzinfo=CST)
    return (now - as_of).total_seconds()


def _shared_frames(
    base: Path | None, now: datetime
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame], str] | None:
    """读全景 poller 共享 drop（data/panorama_live/）。

    快照 as_of 距今 ≤300s 才算新鲜可用（poller 60s 一轮，陈旧说明 poller 没在跑
    或它也三路全败）；资金流按各自新鲜度取，陈旧的给空表（build_board_overview
    补 NaN 列）。不可用返回 None（调用方走自拉）。
    """
    drop_dir = _base_dir(base) / LIVE_DIR_NAME
    meta = read_live_meta(drop_dir)
    snap_meta = meta.get(SNAPSHOT_KEY)
    if not isinstance(snap_meta, dict):
        return None
    age = _meta_age_seconds(snap_meta, now)
    if age is None or age > _SHARED_MAX_AGE_SECONDS:
        return None
    snapshot = read_live_frame(drop_dir, SNAPSHOT_KEY)
    if snapshot is None or snapshot.empty:
        return None

    flows: dict[str, pd.DataFrame] = {}
    for flow_type in FLOW_TYPES:
        f_meta = meta.get(flow_type)
        f_age = _meta_age_seconds(f_meta, now) if isinstance(f_meta, dict) else None
        frame = None
        if f_age is not None and f_age <= _SHARED_MAX_AGE_SECONDS:
            frame = read_live_frame(drop_dir, flow_type)
        flows[flow_type] = frame if frame is not None else pd.DataFrame()

    route = f"共享:{snap_meta.get('route', 'none')}"
    logger.info(f"使用全景 poller 共享快照（age {age:.0f}s，route {route}）")
    return snapshot, flows, route


def _self_fetch_with_retry() -> tuple[pd.DataFrame, dict[str, pd.DataFrame], str]:
    """自拉三级路由；总失败 sleep 60s 重试一次（仍在槽位容差内），再失败给空表。"""
    route = "none"
    for attempt in (1, 2):
        raw = fetch_market_snapshot()
        route = raw.attrs.get("route", "none")
        if not raw.empty:
            flows = {ft: fetch_sector_fund_flow(ft) for ft in FLOW_TYPES}
            return add_limit_prices(raw), flows, route
        if attempt == 1:
            logger.warning(f"快照三路全失败，{_RETRY_SLEEP_SECONDS}s 后重试一次")
            _sleep(_RETRY_SLEEP_SECONDS)
    return pd.DataFrame(), {}, route


def fetch_slot_frames(
    base_dir: Path | None = None,
    now: datetime | None = None,
    *,
    members: pd.DataFrame | None = None,
    kpl_members: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    """获取当前时刻快照（含涨停价）+ 三体系板块聚合，返回 (snapshot, boards, route)。

    获取优先级（全机单一取数者纪律）：
    ① 全景 poller 共享 drop（≤300s 新鲜，route 标注「共享:{原route}」）；
    ② 自拉三级路由（drop 陈旧/缺失时），总失败 sleep 60s 重试一次；
    ③ 仍失败 → 返回空快照 + 空 boards（调用方降级短讯）。
    boards 含各体系 build_board_overview 输出 + system 列。调用方传入 ``members`` /
    ``kpl_members`` 时直接复用，避免通知编排层为同一批成分重复读取只读副本。
    """
    now = now or _now_cst()
    shared = _shared_frames(base_dir, now)
    if shared is not None:
        snapshot, flows, route = shared
        if "limit_up_price" not in snapshot.columns:
            snapshot = add_limit_prices(snapshot)  # 防御：老版 drop 无涨停价列
    else:
        snapshot, flows, route = _self_fetch_with_retry()
    if snapshot.empty:
        return pd.DataFrame(), pd.DataFrame(), route

    if members is None:
        members = _load_members()
    if kpl_members is None:
        kpl_members = load_kpl_concept_members()
    frames: list[pd.DataFrame] = []
    for system in BOARD_SYSTEMS:
        flow_type = _SYSTEM_FLOW_TYPE.get(system)
        flow = flows.get(flow_type, pd.DataFrame()) if flow_type else pd.DataFrame()
        overview = build_board_overview(snapshot, members, kpl_members, flow, system)
        if not overview.empty:
            overview = overview.copy()
            overview["system"] = system
            frames.append(overview)
    boards = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return snapshot, boards, route


def theme_map_from_kpl(kpl_members: pd.DataFrame) -> dict[str, str]:
    """con_code → 开盘啦题材名（首个命中；一票多题材时取第一）。"""
    if kpl_members.empty or "con_code" not in kpl_members.columns:
        return {}
    mapping: dict[str, str] = {}
    for _, row in kpl_members.iterrows():
        code = str(row["con_code"])
        if code not in mapping:
            mapping[code] = str(row["board_name"])
    return mapping


# ── 只读副本读入（T-1 数据；绝不写主库） ───────────────────────────────────────


def load_prev_limit_list(store: DuckDBStore | None = None) -> pd.DataFrame:
    """最近一个交易日的涨停榜（limit_list_daily，limit_status='U'），连板现算的输入。

    返回列：ts_code, name, limit_times。副本缺表/撞锁 → 空（连板全按首板算）。
    """
    if _fake_enabled():
        return _fake_prev_limit_list()
    owns = store is None
    try:
        store = store or open_readonly_store(required_tables=("limit_list_daily",))
    except Exception as e:
        logger.warning(f"涨停榜只读库打开失败: {type(e).__name__}: {e}")
        return pd.DataFrame()
    try:
        return store._conn.execute(
            """
            SELECT ts_code, name, limit_times
            FROM limit_list_daily
            WHERE limit_status = 'U'
              AND trade_date = (
                SELECT MAX(trade_date) FROM limit_list_daily WHERE limit_status = 'U'
              )
            """
        ).fetchdf()
    except Exception as e:
        logger.warning(f"涨停榜查询失败: {type(e).__name__}: {e}")
        return pd.DataFrame()
    finally:
        if owns:
            store.close()


def load_avg_amount_20d(store: DuckDBStore | None = None) -> pd.DataFrame:
    """各票近 20 个交易日全日均成交额（daily_bar.amount，**单位千元，未换算**）。

    返回列：ts_code, avg_amount_20d（千元）。候选池对齐时 ×1000 换算为元
    （见 build_candidate_pool）。副本缺表/撞锁 → 空。
    """
    if _fake_enabled():
        return _fake_avg_amount_20d()
    owns = store is None
    try:
        store = store or open_readonly_store(required_tables=("daily_bar",))
    except Exception as e:
        logger.warning(f"20 日均额只读库打开失败: {type(e).__name__}: {e}")
        return pd.DataFrame()
    try:
        return store._conn.execute(
            """
            SELECT ts_code, AVG(amount) AS avg_amount_20d
            FROM (
              SELECT ts_code, amount,
                     ROW_NUMBER() OVER (
                       PARTITION BY ts_code ORDER BY trade_date DESC
                     ) AS rn
              FROM daily_bar
            ) WHERE rn <= 20 GROUP BY ts_code
            """
        ).fetchdf()
    except Exception as e:
        logger.warning(f"20 日均额查询失败: {type(e).__name__}: {e}")
        return pd.DataFrame()
    finally:
        if owns:
            store.close()


def load_active_positions(store: DuckDBStore | None = None) -> pd.DataFrame:
    """当前活跃模拟仓（paper_position status='open'，只看 live）。空仓 → 空表（该节省略）。"""
    if _fake_enabled():
        return _fake_active_positions()
    owns = store is None
    try:
        store = store or open_readonly_store(required_tables=("paper_position",))
    except Exception as e:
        logger.warning(f"持仓只读库打开失败: {type(e).__name__}: {e}")
        return pd.DataFrame()
    try:
        df = store.query_active_paper_positions()
    except Exception as e:
        logger.warning(f"持仓查询失败: {type(e).__name__}: {e}")
        return pd.DataFrame()
    finally:
        if owns:
            store.close()
    if not df.empty and "run_mode" in df.columns:
        df = df[df["run_mode"].fillna("live") == "live"].reset_index(drop=True)
    return df


# ── 纯计算层（全部吃 DataFrame，离线可测） ─────────────────────────────────────


def _limit_up_codes(snapshot: pd.DataFrame) -> set[str]:
    """快照中已涨停的 ts_code 集合（price ≥ limit_up_price − tol）。"""
    if snapshot.empty or not {"price", "limit_up_price", "ts_code"}.issubset(snapshot.columns):
        return set()
    valid = snapshot["price"].notna() & (snapshot["price"] > 0) & snapshot["limit_up_price"].notna()
    hit = valid & (snapshot["price"] >= snapshot["limit_up_price"] - PRICE_TOL)
    return set(snapshot.loc[hit, "ts_code"].astype(str))


def compute_volume_anomalies(
    snapshot: pd.DataFrame, avg20: pd.DataFrame, *, ratio_min: float = _CANDIDATE_VOL_RATIO_MIN
) -> list[VolAnomaly]:
    """创业/科创半日放量异动（半日量比 ≥ 阈值），按量比降序。"""
    codes = _filter_gem_star(snapshot)
    if codes.empty or avg20.empty:
        return []
    merged = _merge_vol_ratio(codes, avg20)
    hit = merged[merged["_vol_ratio"] >= ratio_min].sort_values("_vol_ratio", ascending=False)
    return [
        VolAnomaly(
            ts_code=str(r["ts_code"]),
            name=str(r.get("name", "")),
            vol_ratio=round(float(r["_vol_ratio"]), 2),
        )
        for _, r in hit.iterrows()
    ]


def compute_pulse_view(
    slot_hhmm: str,
    snapshot: pd.DataFrame,
    boards: pd.DataFrame,
    prev_snapshot: pd.DataFrame | None,
    prev_boards: pd.DataFrame | None,
    theme_map: dict[str, str],
    avg20: pd.DataFrame,
    *,
    kpl_members: pd.DataFrame | None = None,
    prev_limit: pd.DataFrame | None = None,
) -> PulseView:
    """组装 pulse 视图：绝对计数 + 对上一槽位 Δ（首槽无前项则 Δ 省略）。

    ``kpl_members`` 和 ``prev_limit`` 以关键字接收，保留既有调用方的参数位置；
    缺少题材成员或 T-1 涨停榜时，题材梯队分别省略或按首板计算。
    """
    pulse = compute_market_pulse(snapshot)
    has_prev = prev_snapshot is not None and not prev_snapshot.empty
    view = PulseView(
        slot_hhmm=slot_hhmm,
        has_prev=has_prev,
        limit_up_count=pulse.limit_up_count,
        broken_count=pulse.broken_count,
        limit_down_count=pulse.limit_down_count,
        up_count=pulse.up_count,
        down_count=pulse.down_count,
    )

    if kpl_members is not None:
        view.theme_ladders = _compute_pulse_theme_ladders(
            slot_hhmm,
            snapshot,
            prev_snapshot if has_prev else None,
            kpl_members,
            prev_limit if prev_limit is not None else pd.DataFrame(),
        )

    curr_lu = _limit_up_codes(snapshot)
    curr_anom = compute_volume_anomalies(snapshot, avg20)
    if has_prev:
        prev_pulse = compute_market_pulse(prev_snapshot)
        view.limit_up_delta = pulse.limit_up_count - prev_pulse.limit_up_count
        view.broken_delta = pulse.broken_count - prev_pulse.broken_count
        prev_lu = _limit_up_codes(prev_snapshot)
        new_codes = curr_lu - prev_lu
        view.new_limit_ups = _build_new_limit_ups(snapshot, new_codes, theme_map)
        prev_anom_codes = {a.ts_code for a in compute_volume_anomalies(prev_snapshot, avg20)}
        view.new_anomalies = [a for a in curr_anom if a.ts_code not in prev_anom_codes][
            :_PULSE_ANOMALY_TOP_N
        ]
    else:
        view.new_anomalies = curr_anom[:_PULSE_ANOMALY_TOP_N]
    return view


def _compute_pulse_theme_ladders(
    slot_hhmm: str,
    snapshot: pd.DataFrame,
    prev_snapshot: pd.DataFrame | None,
    kpl_members: pd.DataFrame,
    prev_limit: pd.DataFrame,
) -> list[ThemeLadderSummary]:
    """用统一题材梯队计算脉搏 Top5，并比较上一个脉搏的同口径排名。"""
    slot_tag = _slot_tag(slot_hhmm)
    slot_snapshots = {slot_tag: snapshot}
    has_prev = prev_snapshot is not None and not prev_snapshot.empty
    if has_prev:
        previous_tag = _prev_pulse_tag(slot_tag)
        if previous_tag is not None:
            slot_snapshots[previous_tag] = prev_snapshot

    current = compute_theme_ladder_summaries(
        snapshot, prev_limit, kpl_members, slot_snapshots
    )
    if not has_prev:
        return current

    previous = compute_theme_ladder_summaries(
        prev_snapshot, prev_limit, kpl_members, slot_snapshots
    )
    previous_ranks = {summary.theme: rank for rank, summary in enumerate(previous, start=1)}
    ranked: list[ThemeLadderSummary] = []
    for rank, summary in enumerate(current, start=1):
        previous_rank = previous_ranks.get(summary.theme)
        if previous_rank is None:
            ranked.append(summary.model_copy(update={"rank_state": "new"}))
        else:
            ranked.append(
                summary.model_copy(
                    update={
                        "rank_change": previous_rank - rank,
                        "rank_state": "changed",
                    }
                )
            )
    return ranked


def _build_new_limit_ups(
    snapshot: pd.DataFrame, codes: set[str], theme_map: dict[str, str]
) -> list[NewLimitUp]:
    if not codes:
        return []
    sub = snapshot[snapshot["ts_code"].astype(str).isin(codes)]
    out: list[NewLimitUp] = []
    for _, r in sub.iterrows():
        code = str(r["ts_code"])
        out.append(
            NewLimitUp(ts_code=code, name=str(r.get("name", "")), theme=theme_map.get(code, ""))
        )
    return out[:_NEW_LIMIT_UP_MAX]


def _theme_heat(boards: pd.DataFrame, prev_boards: pd.DataFrame | None) -> list[ThemeHeat]:
    """开盘啦体系题材热度 top5（涨停数降序），Δ 对上一槽位同题材涨停数。"""
    kpl = _kpl_rows(boards)
    if kpl.empty:
        return []
    kpl = kpl[kpl["limit_up_count"] > 0].sort_values(
        ["limit_up_count", "amount"], ascending=False
    ).head(_THEME_TOP_N)
    prev_lu: dict[str, int] = {}
    if prev_boards is not None:
        pk = _kpl_rows(prev_boards)
        if not pk.empty:
            prev_lu = dict(
                zip(pk["board_name"].astype(str), pk["limit_up_count"].astype(int), strict=False)
            )
    out: list[ThemeHeat] = []
    for _, r in kpl.iterrows():
        name = str(r["board_name"])
        delta = int(r["limit_up_count"]) - prev_lu[name] if name in prev_lu else None
        out.append(
            ThemeHeat(
                theme=name,
                limit_up_count=int(r["limit_up_count"]),
                delta=delta,
                amount=float(r.get("amount", 0.0) or 0.0),
            )
        )
    return out


def _kpl_rows(boards: pd.DataFrame) -> pd.DataFrame:
    if boards.empty or "system" not in boards.columns:
        return pd.DataFrame()
    return boards[boards["system"] == _KPL_SYSTEM]


def compute_board_ladder(
    snapshot: pd.DataFrame, prev_limit: pd.DataFrame, theme_map: dict[str, str]
) -> list[LadderStock]:
    """连板梯队现算：昨 N 板 + 今涨停 = N+1 板；昨无 + 今涨停 = 首板。今未涨停不入梯队。"""
    codes = _limit_up_codes(snapshot)
    if not codes:
        return []
    prev_boards: dict[str, int] = {}
    if not prev_limit.empty and {"ts_code", "limit_times"}.issubset(prev_limit.columns):
        for _, r in prev_limit.iterrows():
            lt = r["limit_times"]
            if pd.notna(lt):
                prev_boards[str(r["ts_code"])] = int(lt)
    name_map = dict(
        zip(snapshot["ts_code"].astype(str), snapshot["name"].astype(str), strict=False)
    )
    out: list[LadderStock] = []
    for code in codes:
        boards = prev_boards.get(code, 0) + 1  # 昨无 → 0+1 = 首板
        out.append(
            LadderStock(
                ts_code=code,
                name=name_map.get(code, ""),
                boards=boards,
                theme=theme_map.get(code, ""),
            )
        )
    return sorted(out, key=lambda s: (-s.boards, s.ts_code))


def compute_theme_ladder_summaries(
    snapshot: pd.DataFrame,
    prev_limit: pd.DataFrame,
    kpl_members: pd.DataFrame,
    slot_snapshots: dict[str, pd.DataFrame],
    *,
    top_n: int = _THEME_TOP_N,
) -> list[ThemeLadderSummary]:
    """按开盘啦真实多对多题材关系计算连续涨停梯队。"""
    required_columns = {"board_name", "con_code"}
    if kpl_members.empty or not required_columns.issubset(kpl_members.columns):
        return []
    if snapshot.empty or not {"ts_code", "amount"}.issubset(snapshot.columns):
        return []

    members = kpl_members[["board_name", "con_code"]].dropna().drop_duplicates().copy()
    if members.empty:
        return []
    members["board_name"] = members["board_name"].astype(str)
    members["con_code"] = members["con_code"].astype(str)

    joined = members.merge(snapshot, left_on="con_code", right_on="ts_code", how="inner")
    if joined.empty:
        return []
    joined["_amount"] = pd.to_numeric(joined.get("amount"), errors="coerce").fillna(0.0)
    limit_codes = _limit_up_codes(snapshot)
    joined["_is_limit_up"] = joined["ts_code"].astype(str).isin(limit_codes)
    current = joined[joined["_is_limit_up"]].copy()
    if current.empty:
        return []

    previous_boards = _previous_limit_boards(prev_limit)
    current["_boards"] = (
        current["ts_code"].astype(str).map(previous_boards).fillna(0).astype(int) + 1
    )
    amount_by_theme = joined.groupby("board_name")["_amount"].sum().to_dict()
    slot_counts = {
        _slot_tag(hhmm): _theme_limit_up_counts(frame, members)
        for hhmm in PULSE_SLOTS
        if (frame := slot_snapshots.get(_slot_tag(hhmm))) is not None
    }

    summaries: list[ThemeLadderSummary] = []
    for theme, group in current.groupby("board_name", sort=False):
        stocks_by_height: dict[int, list[LadderStock]] = {}
        for _, row in group.iterrows():
            boards = int(row["_boards"])
            stocks_by_height.setdefault(boards, []).append(
                LadderStock(
                    ts_code=str(row["ts_code"]),
                    name=str(row.get("name", "")),
                    boards=boards,
                    theme=str(theme),
                )
            )
        highest_board = max(stocks_by_height)
        rungs = [
            ThemeLadderRung(
                boards=boards,
                stocks=sorted(stocks_by_height.get(boards, []), key=lambda stock: stock.ts_code),
            )
            for boards in range(highest_board, 0, -1)
        ]
        summaries.append(
            ThemeLadderSummary(
                theme=str(theme),
                limit_up_count=len(group),
                amount=float(amount_by_theme.get(theme, 0.0)),
                rungs=rungs,
                slot_series=[
                    slot_counts[_slot_tag(hhmm)].get(str(theme), 0)
                    if _slot_tag(hhmm) in slot_counts
                    else None
                    for hhmm in PULSE_SLOTS
                ],
            )
        )
    return sorted(
        summaries,
        key=lambda summary: (-summary.limit_up_count, -summary.amount, summary.theme),
    )[:top_n]


def _previous_limit_boards(prev_limit: pd.DataFrame) -> dict[str, int]:
    if prev_limit.empty or not {"ts_code", "limit_times"}.issubset(prev_limit.columns):
        return {}
    limit_times = pd.to_numeric(prev_limit["limit_times"], errors="coerce")
    boards: dict[str, int] = {}
    for ts_code, raw_limit_times in zip(prev_limit["ts_code"], limit_times, strict=False):
        if pd.isna(raw_limit_times):
            continue
        value = float(raw_limit_times)
        if not isfinite(value) or not value.is_integer():
            continue
        previous = int(value)
        if not 1 <= previous < _MAX_THEME_LADDER_BOARDS:
            continue
        boards[str(ts_code)] = previous
    return boards


def _theme_limit_up_counts(snapshot: pd.DataFrame, members: pd.DataFrame) -> dict[str, int]:
    limit_codes = _limit_up_codes(snapshot)
    if not limit_codes or "ts_code" not in snapshot.columns:
        return {}
    current = snapshot[snapshot["ts_code"].astype(str).isin(limit_codes)]
    joined = members.merge(current, left_on="con_code", right_on="ts_code", how="inner")
    if joined.empty:
        return {}
    return joined.groupby("board_name")["ts_code"].count().astype(int).to_dict()


def compute_top_themes(
    boards: pd.DataFrame, slot_frames: dict[str, pd.DataFrame], *, top_n: int = _THEME_TOP_N
) -> list[ThemeRank]:
    """最强题材 Top5（kpl 口径，涨停数×半日额综合排序）+ 上午四槽位涨停数演变。"""
    kpl = _kpl_rows(boards)
    if kpl.empty:
        return []
    ranked = kpl[kpl["limit_up_count"] > 0].sort_values(
        ["limit_up_count", "amount"], ascending=False
    ).head(top_n)
    out: list[ThemeRank] = []
    for _, r in ranked.iterrows():
        name = str(r["board_name"])
        series: list[int | None] = []
        for hhmm in PULSE_SLOTS:
            frame = slot_frames.get(_slot_tag(hhmm))
            series.append(_theme_limit_up_at(frame, name))
        out.append(
            ThemeRank(
                theme=name,
                limit_up_count=int(r["limit_up_count"]),
                amount=float(r.get("amount", 0.0) or 0.0),
                slot_series=series,
            )
        )
    return out


def _theme_limit_up_at(boards: pd.DataFrame | None, theme: str) -> int | None:
    if boards is None or boards.empty:
        return None
    kpl = _kpl_rows(boards)
    row = kpl[kpl["board_name"].astype(str) == theme]
    return int(row["limit_up_count"].iloc[0]) if not row.empty else None


def _filter_gem_star(snapshot: pd.DataFrame) -> pd.DataFrame:
    """筛出创业板/科创板票（板块分档复用 state.derive._classify_board）。"""
    if snapshot.empty or "ts_code" not in snapshot.columns:
        return pd.DataFrame()
    mask = snapshot["ts_code"].astype(str).map(lambda c: _classify_board(c) in ("gem", "star"))
    return snapshot[mask].copy()


def _merge_vol_ratio(codes: pd.DataFrame, avg20: pd.DataFrame) -> pd.DataFrame:
    """半日量比 = 快照半日额（元） / 20 日全日均额（千元 ×1000 → 元）。

    单位陷阱：daily_bar.amount 是**千元**、快照 amount 是**元**，直接相除会差 1000 倍，
    必须 ×1000 对齐后再比。
    """
    merged = codes.merge(avg20[["ts_code", "avg_amount_20d"]], on="ts_code", how="inner")
    denom = merged["avg_amount_20d"] * 1000.0  # 千元 → 元
    merged["_vol_ratio"] = merged["amount"] / denom.where(denom > 0)
    return merged[merged["_vol_ratio"].notna()]


def build_candidate_pool(
    snapshot: pd.DataFrame, avg20: pd.DataFrame, theme_map: dict[str, str]
) -> list[CandidateStock]:
    """下午候选观察池：创业/科创 半日量能预筛。

    条件（全部满足）：
    - 半日额 ≥ 0.8 × 20 日全日均额（daily_bar 千元 ×1000 对齐快照元）；
    - 0 < pct_chg < 15 且未涨停（price < limit_up_price − tol）；
    - 现价 ≥ 半日均价（amount/volume 近似 VWAP）。
    按半日量比降序取 top20。
    """
    codes = _filter_gem_star(snapshot)
    if codes.empty or avg20.empty:
        return []
    merged = _merge_vol_ratio(codes, avg20)
    if merged.empty:
        return []

    limit_up = _limit_up_codes(snapshot)
    vwap = merged["amount"] / merged["volume"].where(merged["volume"] > 0)
    cond = (
        (merged["_vol_ratio"] >= _CANDIDATE_VOL_RATIO_MIN)
        & (merged["pct_chg"] > 0)
        & (merged["pct_chg"] < 15)
        & ~merged["ts_code"].astype(str).isin(limit_up)
        & merged["price"].notna()
        & vwap.notna()
        & (merged["price"] >= vwap)
    )
    hit = merged[cond].sort_values("_vol_ratio", ascending=False).head(_CANDIDATE_TOP_N)
    out: list[CandidateStock] = []
    for _, r in hit.iterrows():
        code = str(r["ts_code"])
        price = float(r["price"])
        limit_price = float(r["limit_up_price"])
        room = (limit_price / price - 1) * 100 if price > 0 else 0.0
        out.append(
            CandidateStock(
                ts_code=code,
                name=str(r.get("name", "")),
                theme=theme_map.get(code, ""),
                vol_ratio=round(float(r["_vol_ratio"]), 2),
                pct_chg=round(float(r["pct_chg"]), 2),
                room_to_limit_pct=round(room, 2),
            )
        )
    return out


def check_positions(
    positions: pd.DataFrame,
    snapshot: pd.DataFrame,
    theme_map: dict[str, str],
    boards: pd.DataFrame,
) -> list[PositionCheck]:
    """持仓午间体检：逐仓 mark-to-market（浮盈% / 距止损% / 板块半日强弱）。

    空仓 → 空列表（渲染层整节省略）。快照无报价的仓跳过（无法盯价）。
    """
    if positions.empty or snapshot.empty:
        return []
    price_map = dict(
        zip(
            snapshot["ts_code"].astype(str),
            pd.to_numeric(snapshot["price"], errors="coerce"),
            strict=False,
        )
    )
    kpl = _kpl_rows(boards)
    theme_pct: dict[str, float] = {}
    if not kpl.empty:
        theme_pct = dict(
            zip(
                kpl["board_name"].astype(str),
                pd.to_numeric(kpl["pct_chg_median"], errors="coerce"),
                strict=False,
            )
        )

    out: list[PositionCheck] = []
    for _, r in positions.iterrows():
        code = str(r["ts_code"])
        price = price_map.get(code)
        if price is None or pd.isna(price) or price <= 0:
            continue
        entry = float(r["entry_price"])
        stop = r.get("stop_loss_price")
        pnl = (price / entry - 1) * 100 if entry > 0 else 0.0
        dist_stop = (price / float(stop) - 1) * 100 if stop and float(stop) > 0 else 0.0
        theme = theme_map.get(code, "")
        note = ""
        if theme and theme in theme_pct and pd.notna(theme_pct[theme]):
            note = f"{theme} {theme_pct[theme]:+.1f}%"
        elif theme:
            note = theme
        out.append(
            PositionCheck(
                ts_code=code,
                name=str(r.get("name", "")),
                pnl_pct=round(pnl, 2),
                dist_stop_pct=round(dist_stop, 2),
                board_note=note,
            )
        )
    return out


# ── 报文渲染 ────────────────────────────────────────────────────────────────────


def _delta(value: int | None) -> str:
    return f"({value:+d})" if value is not None else ""


def _body_delta(value: int | None) -> str:
    return f"（{value:+d}）" if value is not None else ""


def _pulse_theme_ladder_lines(
    rank: int, summary: ThemeLadderSummary
) -> list[str] | None:
    """将连续题材梯队压缩为三行脉搏摘要，最高档最多展示两个标的。"""
    highest = next((rung for rung in summary.rungs if rung.stocks), None)
    if highest is None:
        return None
    names = [stock.name or stock.ts_code for stock in highest.stocks]
    leader_names = "、".join(names[:2])
    if len(names) > 2:
        leader_names += f"等{len(names)}只"
    rank_text = ""
    if summary.rank_state == "new":
        rank_text = "新晋"
    elif summary.rank_state == "changed" and summary.rank_change is not None:
        if summary.rank_change > 0:
            rank_text = f"↑{summary.rank_change}"
        elif summary.rank_change < 0:
            rank_text = f"↓{-summary.rank_change}"
        else:
            rank_text = "持平"
    title = f"{rank}. {summary.theme}"
    if rank_text:
        title += f" {rank_text}"
    rung_counts = " ｜ ".join(
        f"{'首板' if rung.boards == 1 else f'{rung.boards}板'} {len(rung.stocks)}"
        for rung in summary.rungs
    )
    return [
        title,
        f"   涨停 {summary.limit_up_count} ｜ 最高 {highest.boards}板：{leader_names}",
        f"   梯队：{rung_counts}",
    ]


def render_pulse(view: PulseView) -> tuple[str, str]:
    """30 分钟脉搏报文（短，手机一屏）。返回 (title, body)。"""
    hhmm = view.slot_hhmm
    if view.degraded:
        title = f"脉搏 {hhmm} 快照不可用"
        return title, f"# 脉搏 {hhmm}\n\n> 快照三路全部失败，本槽位无数据（不落盘）。"

    title = (
        f"脉搏 {hhmm} 涨停{view.limit_up_count}{_delta(view.limit_up_delta)} "
        f"炸板{view.broken_count}{_delta(view.broken_delta)}"
    )
    lines = [
        f"# 脉搏 {hhmm}",
        "",
        "## 市场温度",
        f"- 涨停：{view.limit_up_count}{_body_delta(view.limit_up_delta)}"
        f" ｜ 炸板：{view.broken_count}{_body_delta(view.broken_delta)}"
        f" ｜ 跌停：{view.limit_down_count}",
        f"- 上涨：{view.up_count} ｜ 下跌：{view.down_count}",
    ]
    if view.has_prev and view.new_limit_ups:
        lines.extend(["", "## 新晋涨停"])
        lines.extend(
            f"- {n.name} ｜ {n.theme}" if n.theme else f"- {n.name}"
            for n in view.new_limit_ups
        )
    theme_lines: list[str] = []
    for rank, summary in enumerate(view.theme_ladders, start=1):
        if summary_lines := _pulse_theme_ladder_lines(rank, summary):
            theme_lines.extend(summary_lines)
    if theme_lines:
        lines.extend(["", "## 题材梯队 Top5", *theme_lines])
    if view.new_anomalies:
        heading = "放量异动新增" if view.has_prev else "放量异动"
        lines.extend(["", f"## {heading}"])
        lines.extend(f"- {a.name}：量比 {a.vol_ratio}" for a in view.new_anomalies)
    if view.route.startswith("共享"):
        lines.extend(["", f"> 数据源：{view.route}"])
    return title, "\n".join(lines)


def _digest_theme_ladder_lines(rank: int, summary: ThemeLadderSummary) -> list[str]:
    """将一个题材的完整连续梯队渲染为午间战报的手机分块。"""
    series = " / ".join(str(count) if count is not None else "—" for count in summary.slot_series)
    meta = (
        f"{rank}. {summary.theme} ｜ 涨停 {summary.limit_up_count} ｜ "
        f"半日额 {summary.amount / 1e8:.1f}亿"
    )
    lines = [
        meta,
        f"   上午：{series or '—'}",
    ]
    for rung in summary.rungs:
        label = "首板" if rung.boards == 1 else f"{rung.boards}板"
        names = "、".join(stock.name or stock.ts_code for stock in rung.stocks) or "暂无"
        lines.append(f"   - {label}（{len(rung.stocks)}）：{names}")
    return [f"{line}  " for line in lines[:-1]] + lines[-1:]


def render_digest(view: DigestView) -> tuple[str, str]:
    """午间战报（digest，四节）。返回 (title, body)。"""
    mmdd = view.day.strftime("%m-%d")
    if view.degraded:
        title = f"午间战报 {mmdd} 快照不可用"
        return title, f"# 午间战报 {mmdd}\n\n> 快照三路全部失败，无法生成战报。"

    title = f"午间战报 {mmdd} 涨停{view.limit_up_count} 炸板{view.broken_count}"
    lines = [f"# 午间战报 {mmdd}", ""]

    # ① 情绪温度
    lines.append("## ① 情绪温度")
    broken_ratio = f"{view.broken_ratio_pct:.1f}%" if view.broken_ratio_pct is not None else "—"
    lines.append(
        f"涨停 {view.limit_up_count} | 炸板 {view.broken_count} | "
        f"跌停 {view.limit_down_count} | 炸板率 {broken_ratio} | "
        f"涨跌比 {view.up_count}/{view.down_count}"
    )
    if view.slot_limit_up_series:
        seq = " → ".join(
            f"{hhmm} {c}" if c is not None else f"{hhmm} —" for hhmm, c in view.slot_limit_up_series
        )
        lines.append(f"今日涨停走势：{seq}")
    if view.prev_day_limit_up is not None:
        lines.append(f"昨日终值：涨停 {view.prev_day_limit_up} 家（T-1）")
    lines.append("")

    # ② 最强题材·连板梯队 Top5
    lines.append("## ② 最强题材·连板梯队 Top5")
    if view.theme_ladders:
        for rank, summary in enumerate(view.theme_ladders, start=1):
            lines.extend(_digest_theme_ladder_lines(rank, summary))
            lines.append("")
        lines.pop()
    else:
        lines.append("- 暂无")
    lines.append("")

    # ③ 下午候选观察池
    lines.append("## ③ 下午候选观察池（创业/科创 半日量能预筛）")
    if view.candidates:
        for c in view.candidates:
            lines.extend([
                f"- {c.name}（{c.ts_code}）  ",
                f"  题材：{c.theme or '—'} ｜ 半日量比：{c.vol_ratio:.2f}  ",
                f"  涨幅：{c.pct_chg:+.1f}% ｜ 距涨停：{c.room_to_limit_pct:.1f}%",
                "",
            ])
        lines.pop()
    else:
        lines.append("- 暂无")
    lines.append("")

    # ④ 持仓风险（空仓整节省略）
    if view.positions:
        lines.append("## ④ 持仓风险")
        lines.append("| 代码 | 名称 | 浮盈 | 距止损 | 板块 |")
        lines.append("|---|---|---|---|---|")
        for p in view.positions:
            lines.append(
                f"| {p.ts_code} | {p.name} | {p.pnl_pct:+.1f}% | "
                f"{p.dist_stop_pct:+.1f}% | {p.board_note or '—'} |"
            )
        lines.append("")

    if view.route.startswith("共享"):
        lines.append(f"数据源：{view.route}")

    return title, "\n".join(lines).rstrip()


# ── 编排：morning-pulse / midday-report ─────────────────────────────────────────


def _now_cst() -> datetime:
    return datetime.now(CST)


def run_morning_pulse(
    *,
    slot: str | None = None,
    force: bool = False,
    dry_run: bool = False,
    now: datetime | None = None,
    base_dir: Path | None = None,
) -> int:
    """30 分钟脉搏：守卫 → 拉快照+板块 → 落盘 → 对上一槽位求 Δ → 渲染 → 推送/落 md。"""
    now = now or _now_cst()
    day = now.date()
    if not is_trading_day(day):
        logger.info(f"{day} 非交易日，morning-pulse 跳过")
        return 0

    try:
        tag, skip = resolve_slot(now, explicit=slot)
    except ValueError as e:
        logger.error(str(e))
        return 1
    if tag is None:
        logger.info(f"morning-pulse 跳过：{skip}")
        return 0

    hhmm = _slot_hhmm(tag)
    mdir = _midday_dir(base_dir, day)
    meta = _read_meta(mdir)
    if meta.get(tag, SlotMeta()).pushed and not force and not dry_run:
        logger.info(f"脉搏 {hhmm} 当日已推送，跳过（--force 覆盖）")
        return 0

    members, kpl_members, prev_limit, avg20, _ = _load_notification_inputs(include_positions=False)
    snapshot, boards, route = fetch_slot_frames(
        base_dir, now=now, members=members, kpl_members=kpl_members
    )
    if snapshot.empty:
        logger.warning(f"脉搏 {hhmm} 快照三路全失败，降级短讯（不落 snapshot parquet）")
        view = PulseView(slot_hhmm=hhmm, has_prev=False, limit_up_count=0, broken_count=0,
                         limit_down_count=0, up_count=0, down_count=0, degraded=True)
        title, body = render_pulse(view)
        _finish(mdir, meta, tag, route, "脉搏", day, title, body, base_dir, dry_run)
        return 0

    _write_frame(snapshot, _frame_path(mdir, "snapshot", tag))
    if not boards.empty:
        _write_frame(boards, _frame_path(mdir, "boards", tag))
    meta[tag] = SlotMeta(fetched_at=now.isoformat(timespec="seconds"), route=route, pushed=False)
    _write_meta(mdir, meta)

    prev_tag = _prev_pulse_tag(tag)
    prev_snapshot = _read_frame(_frame_path(mdir, "snapshot", prev_tag)) if prev_tag else None
    prev_boards = _read_frame(_frame_path(mdir, "boards", prev_tag)) if prev_tag else None

    theme_map = theme_map_from_kpl(kpl_members)
    view = compute_pulse_view(
        hhmm,
        snapshot,
        boards,
        prev_snapshot,
        prev_boards,
        theme_map,
        avg20,
        kpl_members=kpl_members,
        prev_limit=prev_limit,
    )
    view.route = route
    title, body = render_pulse(view)
    _finish(mdir, meta, tag, route, "脉搏", day, title, body, base_dir, dry_run)
    return 0


def run_midday_report(
    *,
    report_date: str | None = None,
    force: bool = False,
    dry_run: bool = False,
    now: datetime | None = None,
    base_dir: Path | None = None,
) -> int:
    """午间战报：取 11:30 后最新快照 → 五节全量战报 → 落盘 digest → 推送/落 md。"""
    now = now or _now_cst()
    day = date.fromisoformat(report_date) if report_date else now.date()
    if not is_trading_day(day):
        logger.info(f"{day} 非交易日，midday-report 跳过")
        return 0

    tag = _slot_tag(DIGEST_SLOT)
    mdir = _midday_dir(base_dir, day)
    meta = _read_meta(mdir)
    if meta.get(tag, SlotMeta()).pushed and not force and not dry_run:
        logger.info("午间战报当日已推送，跳过（--force 覆盖）")
        return 0

    members, kpl_members, prev_limit, avg20, positions = _load_notification_inputs(
        include_positions=True
    )
    snapshot, boards, route = fetch_slot_frames(
        base_dir, now=now, members=members, kpl_members=kpl_members
    )
    if snapshot.empty:
        logger.warning("午间战报快照三路全失败，降级短讯")
        view = DigestView(day=day, limit_up_count=0, broken_count=0, limit_down_count=0,
                          up_count=0, down_count=0, degraded=True)
        title, body = render_digest(view)
        _finish(mdir, meta, tag, route, "午间战报", day, title, body, base_dir, dry_run)
        return 0

    _write_frame(snapshot, _frame_path(mdir, "snapshot", tag))
    if not boards.empty:
        _write_frame(boards, _frame_path(mdir, "boards", tag))
    meta[tag] = SlotMeta(fetched_at=now.isoformat(timespec="seconds"), route=route, pushed=False)
    _write_meta(mdir, meta)

    view = _build_digest_view(
        day,
        snapshot,
        boards,
        mdir,
        kpl_members=kpl_members,
        prev_limit=prev_limit,
        avg20=avg20,
        positions=positions,
    )
    view.route = route
    title, body = render_digest(view)
    _finish(mdir, meta, tag, route, "午间战报", day, title, body, base_dir, dry_run)
    return 0


def _build_digest_view(
    day: date,
    snapshot: pd.DataFrame,
    boards: pd.DataFrame,
    mdir: Path,
    *,
    kpl_members: pd.DataFrame | None = None,
    prev_limit: pd.DataFrame | None = None,
    avg20: pd.DataFrame | None = None,
    positions: pd.DataFrame | None = None,
) -> DigestView:
    """从当前快照 + 落盘的各槽位帧 + 只读副本组装五节战报视图。"""
    pulse = compute_market_pulse(snapshot)
    kpl_members = kpl_members if kpl_members is not None else pd.DataFrame()
    prev_limit = prev_limit if prev_limit is not None else pd.DataFrame()
    avg20 = avg20 if avg20 is not None else pd.DataFrame()
    positions = positions if positions is not None else pd.DataFrame()
    theme_map = theme_map_from_kpl(kpl_members)

    slot_frames = {
        _slot_tag(hhmm): _read_frame(_frame_path(mdir, "snapshot", _slot_tag(hhmm)))
        for hhmm in PULSE_SLOTS
    }
    slot_frames = {k: v for k, v in slot_frames.items() if v is not None}
    board_frames = {
        _slot_tag(hhmm): _read_frame(_frame_path(mdir, "boards", _slot_tag(hhmm)))
        for hhmm in PULSE_SLOTS
    }
    board_frames = {k: v for k, v in board_frames.items() if v is not None}

    slot_series: list[tuple[str, int | None]] = []
    for hhmm in PULSE_SLOTS:
        frame = slot_frames.get(_slot_tag(hhmm))
        count = compute_market_pulse(frame).limit_up_count if frame is not None else None
        slot_series.append((hhmm, count))

    sealed = pulse.limit_up_count + pulse.broken_count
    broken_ratio = round(pulse.broken_count / sealed * 100, 1) if sealed else None
    prev_day_lu = int(len(prev_limit)) if not prev_limit.empty else None

    return DigestView(
        day=day,
        limit_up_count=pulse.limit_up_count,
        broken_count=pulse.broken_count,
        limit_down_count=pulse.limit_down_count,
        up_count=pulse.up_count,
        down_count=pulse.down_count,
        broken_ratio_pct=broken_ratio,
        slot_limit_up_series=slot_series,
        prev_day_limit_up=prev_day_lu,
        theme_ladders=compute_theme_ladder_summaries(
            snapshot, prev_limit, kpl_members, slot_frames, top_n=_THEME_TOP_N
        ),
        ladder=compute_board_ladder(snapshot, prev_limit, theme_map),
        top_themes=compute_top_themes(boards, board_frames),
        candidates=build_candidate_pool(snapshot, avg20, theme_map),
        positions=check_positions(positions, snapshot, theme_map, boards),
    )


def _open_ro_store() -> DuckDBStore | None:
    """打开一个只读副本连接（digest 三张 T-1 表复用）。fake / 打不开 → None。"""
    if _fake_enabled():
        return None
    try:
        return open_readonly_store()
    except Exception as e:
        logger.warning(f"只读副本打开失败，digest 降级空 T-1 数据: {type(e).__name__}: {e}")
        return None


def _load_notification_inputs(
    *, include_positions: bool
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """在快照网络读取前取齐通知依赖，并在同一短生命周期内关闭只读连接。"""
    store = _open_ro_store()
    if store is None and not _fake_enabled():
        return (
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
        )
    try:
        members = _load_ro_frame("东财板块成分", _load_members, store)
        kpl_members = _load_ro_frame("开盘啦题材成分", load_kpl_concept_members, store)
        prev_limit = _load_ro_frame("涨停榜", load_prev_limit_list, store)
        avg20 = _load_ro_frame("20 日均额", load_avg_amount_20d, store)
        positions = (
            _load_ro_frame("持仓", load_active_positions, store)
            if include_positions
            else pd.DataFrame()
        )
        return members, kpl_members, prev_limit, avg20, positions
    finally:
        if store is not None:
            store.close()


def _load_ro_frame(
    label: str,
    loader: Callable[[DuckDBStore | None], pd.DataFrame],
    store: DuckDBStore | None,
) -> pd.DataFrame:
    """在既有只读连接上读取一张通知依赖表，失败时独立降级为空表。"""
    try:
        return loader(store)
    except Exception as e:
        logger.warning(f"{label}只读查询失败，通知降级为空数据: {type(e).__name__}: {e}")
        return pd.DataFrame()


def _finish(
    mdir: Path,
    meta: dict[str, SlotMeta],
    tag: str,
    route: str,
    kind_label: str,
    day: date,
    title: str,
    body: str,
    base_dir: Path | None,
    dry_run: bool,
) -> None:
    """收尾：落 markdown + 推送 + 标记 pushed（dry-run 只打印不推送、不标记）。"""
    header = f"## {kind_label} {_slot_hhmm(tag)}" if kind_label == "脉搏" else f"## {kind_label}"
    _upsert_report_section(base_dir, day, header, body)
    if dry_run:
        print(f"\n===== [DRY-RUN] {title} =====\n{body}\n")
        return
    scene = "morning_pulse" if kind_label == "脉搏" else "midday_report"
    notify(scene, title=title, body=body)
    current = meta.get(tag, SlotMeta(route=route))
    current.pushed = True
    meta[tag] = current
    _write_meta(mdir, meta)


# ── Fake fixtures（RQUANT_PANORAMA_FAKE=1，digest T-1 输入离线可测/可演示） ──────
#
# panorama_data 的 fake 快照只有 30 只主板票（600001..600030.SH），涨停 600001/600002、
# 炸板 600003。这里补齐 midday 专属的 T-1 只读表 fake：连板现算与持仓体检的输入。


def _fake_prev_limit_list() -> pd.DataFrame:
    """昨日涨停榜 fake：600001 昨 2 板、600002 昨 1 板（今皆涨停 → 今 3 板 / 2 板）。"""
    return pd.DataFrame(
        [
            {"ts_code": "600001.SH", "name": "样本01", "limit_times": 2},
            {"ts_code": "600002.SH", "name": "样本02", "limit_times": 1},
            {"ts_code": "600009.SH", "name": "样本09", "limit_times": 3},
        ]
    )


def _fake_avg_amount_20d() -> pd.DataFrame:
    """20 日均额 fake（单位千元）。fake 快照全为主板票 → 候选池天然空，仅供接口贯通。"""
    return pd.DataFrame({"ts_code": [f"6000{i:02d}.SH" for i in range(1, 31)],
                         "avg_amount_20d": [5.0e5] * 30})


def _fake_active_positions() -> pd.DataFrame:
    """活跃持仓 fake：一仓 600001.SH（今涨停），演示 digest 第 5 节渲染。"""
    return pd.DataFrame(
        [
            {
                "ts_code": "600001.SH",
                "name": "样本01",
                "entry_price": 8.20,
                "stop_loss_price": 7.80,
                "take_profit_price": 9.50,
                "status": "open",
                "run_mode": "live",
            }
        ]
    )
