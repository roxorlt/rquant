"""每分钟爆量推送（surge-watch）：云端常驻单进程循环。

盘中每分钟拉一次创业板+科创板全量快照，两层判定后聚合推 PushDeer：

- **粗筛（零外部调用）**：当日累计成交额 ≥ ``K_rough × 20 日均额 × 进度曲线(t)``
  且 pct_chg>0、非 ST、有 20 日基线（缺基线的次新自动落选），只进候选不推送；
- **确认层（近 3 天口径，用户 pinned）**：对新候选拉 tushare stk_mins 近 3 个
  交易日 1min bars，构造 3 日同刻累计额中位基准，
  ``rel_cum_3d = cum(t) / median_3d_same_time_cum(t) ≥ K_confirm`` 且现价 ≥ 当日
  均价（快照 amount/volume 近似 VWAP，对齐回测 require_vwap_strength）才确认。

口径说明（诚实标注）：确认层 3 日窗口与回测验证的 20 日不同源，K_rough/K_confirm
为产品初始值（非回测标定）；报文尾注口径版本。

纪律（对齐 CLAUDE.md）：
- **绝不写 DuckDB**：只在 9:25 启动时读一次只读副本预载 20 日均额 + kpl 题材成分，
  全部载内存，盘中零 DB 访问；自产数据全 parquet/jsonl；
- 时钟 / 数据源 / 推送 / sleep 全部可注入——单测不真 sleep、不碰网络；
- em clist 快照复用 panorama_data 的加固 Session（每次全新 + trust_env=False +
  桌面 UA + Connection:close），fs 换创业/科创。
"""

from __future__ import annotations

import bisect
import importlib.resources
import json
import os
import time as _time_module
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from datetime import time as dt_time
from pathlib import Path

import numpy as np
import pandas as pd
from loguru import logger
from pydantic import BaseModel

from rquant.state.derive import _detect_st

CST = timezone(timedelta(hours=8))  # A 股墙钟（Asia/Shanghai）

# ── 盘中 241 分钟规范网格（与本地 minute_bar 实测一致）─────────────────────────
# 上午 09:30..11:30（121 点）+ 下午 13:01..15:00（120 点）= 241 点。
# tushare stk_mins 1min bar 以分钟末打戳：09:30 为开盘首根，13:00 不出（并入 11:30
# 收盘根后下午从 13:01 起），故规范网格无 13:00。

CURVE_POINTS = 241
_GRID_MINUTES: list[int] = list(range(9 * 60 + 30, 11 * 60 + 30 + 1)) + list(
    range(13 * 60 + 1, 15 * 60 + 0 + 1)
)
assert len(_GRID_MINUTES) == CURVE_POINTS  # noqa: S101 - 模块加载期自证网格长度

# 会话时刻边界
OPEN_TIME = dt_time(9, 30)
MORNING_END = dt_time(11, 30)
AFTERNOON_START = dt_time(13, 0)
CLOSE_TIME = dt_time(15, 0)
EXIT_TIME = dt_time(15, 2)  # 15:02 自然退出（收盘后无新增量）

DEFAULT_CURVE_FILENAME = "intraday_progress_curve.json"

# 东财 clist fs 段：创业板 m:0+t:80、科创板 m:1+t:23（与已验证策略同域）
_BOARD_FS: dict[str, str] = {
    "gem": "m:0+t:80",
    "star": "m:1+t:23",
    "main_sh": "m:1+t:2",
    "main_sz": "m:0+t:6",
    "bj": "m:0+t:81+s:2048",
}
_DEFAULT_SURGE_BOARDS = ("gem", "star")

LIVE_DIR_NAME = "surge_live"

_DEFAULT_SOCKS_PROXY = "socks5h://127.0.0.1:1086"


def _grid_minute(t: dt_time) -> int:
    return t.hour * 60 + t.minute


def grid_index(t: dt_time) -> int:
    """墙钟时刻 → 241 网格下标（≤t 的最后一个网格点，越界钳制到 [0,240]）。

    盘前 → 0；午休 → 上午末点（120）；收盘后 → 240。用于进度曲线与累计序列对齐。
    """
    m = _grid_minute(t)
    idx = bisect.bisect_right(_GRID_MINUTES, m) - 1
    if idx < 0:
        return 0
    if idx >= CURVE_POINTS:
        return CURVE_POINTS - 1
    return idx


def linear_progress_curve() -> np.ndarray:
    """线性兜底曲线（首≈0 尾=1，严格单调），标定文件缺失时用。"""
    return np.linspace(1.0 / CURVE_POINTS, 1.0, CURVE_POINTS)


def load_progress_curve(path: Path | None = None) -> np.ndarray:
    """加载盘中累计额进度曲线（241 点 float）。

    path 缺省时读包内 ``rquant/data/intraday_progress_curve.json``（importlib.resources
    定位，兼容源码/editable）。文件缺失/损坏/点数不符 → 线性兜底 + warning。
    保证严格单调不减、尾值=1（fp 兜底 cummax + 末点归一）。
    """
    points: list[float] | None = None
    try:
        if path is not None:
            raw = json.loads(Path(path).read_text(encoding="utf-8"))
        else:
            res = importlib.resources.files("rquant.data").joinpath(DEFAULT_CURVE_FILENAME)
            raw = json.loads(res.read_text(encoding="utf-8"))
        pts = raw.get("points") if isinstance(raw, dict) else raw
        if isinstance(pts, list) and len(pts) == CURVE_POINTS:
            points = [float(x) for x in pts]
    except FileNotFoundError:
        logger.warning("盘中进度曲线标定文件缺失，用线性曲线兜底")
    except Exception as e:  # 解析/类型异常一律兜底，不让曲线问题拖垮盘中主循环
        logger.warning(f"盘中进度曲线加载失败（{type(e).__name__}: {e}），用线性曲线兜底")

    if points is None:
        return linear_progress_curve()
    arr = np.asarray(points, dtype="float64")
    arr = np.maximum.accumulate(arr)  # fp 噪声防御：强制单调不减
    last = arr[-1]
    if last > 0:
        arr = arr / last  # 末点归一到 1（rough 判定按占比，尺度须锚定）
    return arr


# ── 跨层结构（Pydantic） ────────────────────────────────────────────────────────


class SurgeConfig(BaseModel):
    """surge-watch 判定参数（产品初始值，跑几天按实际量级调）。"""

    k_rough: float = 1.5
    # 确认层量比门：2026-07-06 全天真实分钟重放 + 24 组门槛扫描的 Pareto 收敛值。
    # v1 默认 2.0 会推 81 只/天（预期 5-15）、胜率 32%；kc3.0 + 同分钟增量门 3.0× →
    # 31 只/胜率 35.5%。逆选择上限：kc4.0 反而更差（样本更少却更噪），故封顶 3.0，
    # 不要再往上抬。
    k_confirm: float = 3.0
    # 同分钟增量门：确认时点要求「当分钟增量 ≥ k_delta × 3 日同分钟中位」。
    # 0 = 关闭。中位≤0 或增量缺失按不通过（None-fail 语义，对齐 step5 GatedWatcher）。
    k_delta_confirm: float = 3.0
    # 可买性守卫：确认时现价距涨停价 ≤ 该 %（或已封板）则不推送（仍占「每票每日一次」
    # 名额、仍落 events 标 unbuyable）。0 = 只挡已封板；负值可整体关闭（room 恒 > 负值）。
    max_room_to_limit_pct: float = 1.0
    confirm_lookback_days: int = 3
    max_per_push: int = 8            # 单条报文最多 N 只，超出折叠
    silent_until_hhmm: str = "09:33"  # 该时刻前只收集不推送
    tushare_rate_per_min: int = 2    # 确认层 stk_mins 限频（次/分）
    tushare_max_retries: int = 3     # 单候选取数失败重试上限（延后不阻塞队列）
    miss_circuit_threshold: int = 5  # 快照连续 miss 触发降级告警 + 退避
    boards: tuple[str, ...] = _DEFAULT_SURGE_BOARDS

    @property
    def silent_until(self) -> dt_time:
        h, m = self.silent_until_hhmm.split(":")
        return dt_time(int(h), int(m))


class SurgeConfirmed(BaseModel):
    """确认通过、待推送/落 events 的一只票。"""

    ts_code: str
    name: str
    theme: str = ""
    confirmed_at: str = ""            # HH:MM
    pct_chg: float = 0.0
    cum_amount: float = 0.0           # 当日累计成交额（元）
    rel_cum_3d: float = 0.0           # cum / 3 日同刻累计额中位
    rough_ratio: float = 0.0          # cum / (20 日均额 × 曲线(t))
    minute_delta: float | None = None       # 本分钟增量（元）
    minute_delta_median_3d: float | None = None  # 3 日同分钟增量中位（元）
    room_to_limit_pct: float | None = None  # 距涨停空间（%）
    # confirmed（可买、推送）| unbuyable（距涨停≤门 / 已封板，只落 events 不推送）
    status: str = "confirmed"


class TickResult(BaseModel):
    """单分钟 tick 的产出：待推送报文 + 本分钟新确认（落 events）。"""

    pushes: list[tuple[str, str]] = []   # [(title, body)]
    confirmed: list[SurgeConfirmed] = []


# ── 基线预载（只在启动读一次只读副本；盘中零 DB 访问） ──────────────────────────


@dataclass
class SurgeBaseline:
    """启动预载的市场基线（全部载内存）。"""

    avg_amount_20d: dict[str, float]  # ts_code → 元（daily_bar 千元 ×1000）
    theme: dict[str, str]             # ts_code → 题材名（三级兜底链首个命中，见 load_theme_map）
    curve: np.ndarray                 # 241 点进度曲线


def load_avg_amount_20d(store=None) -> dict[str, float]:
    """各票近 20 交易日全日均成交额（元）。daily_bar.amount 千元 ×1000 换算。

    只读副本缺表/撞锁 → 空 dict（无基线 → 全部落选，主循环仍活）。
    """
    from rquant.storage.duckdb import open_readonly_store

    owns = store is None
    try:
        store = store or open_readonly_store(required_tables=("daily_bar",))
    except Exception as e:
        logger.warning(f"20 日均额只读库打开失败: {type(e).__name__}: {e}")
        return {}
    try:
        df = store._conn.execute(
            """
            SELECT ts_code, AVG(amount) * 1000 AS avg_amount_20d
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
        return {}
    finally:
        if owns:
            store.close()
    return {str(r.ts_code): float(r.avg_amount_20d) for r in df.itertuples()}


# 题材映射三级兜底链（每级 con_code → 题材名，首个命中保留）：
# 1. kpl_concept_member —— 开盘啦题材快照，本地日终采集权威（PK 每题材最近打点）；
# 2. kpl_concept_member_daily —— 开盘啦题材日度表最新 trade_date 打点（本地有历史时）；
# 3. dc_board_member JOIN dc_board（idx_type='概念板块'）—— 东财概念，云端只读副本大概率有。
# 云端只读副本没有 kpl_*（题材成分是本地研究数据，数据分家未上云），靠第 3 级兜住。
_THEME_MAP_FALLBACKS: tuple[tuple[str, str], ...] = (
    ("kpl_concept_member", "SELECT board_name, con_code FROM kpl_concept_member"),
    (
        "kpl_concept_member_daily",
        """
        SELECT board_name, con_code
        FROM kpl_concept_member_daily
        WHERE trade_date = (SELECT MAX(trade_date) FROM kpl_concept_member_daily)
        """,
    ),
    (
        "dc_board_member",
        """
        SELECT b.name AS board_name, m.con_code
        FROM dc_board_member m
        JOIN dc_board b ON m.board_code = b.ts_code
        WHERE b.idx_type = '概念板块'
        """,
    ),
)


def load_theme_map(store=None) -> dict[str, str]:
    """con_code → 题材名（首个命中）。三级兜底链，逐级 try/except、命中即止。

    依次尝试 kpl_concept_member（本地权威）→ kpl_concept_member_daily 最新打点 →
    dc_board 东财概念（云端兜底）。某级缺表/撞锁/查空即降级下一级，三级全失败 →
    空 dict（现状降级语义保留，主循环仍活，只是报文缺题材标签）。
    """
    from rquant.storage.duckdb import open_readonly_store

    owns = store is None
    try:
        store = store or open_readonly_store()
    except Exception as e:
        logger.warning(f"题材映射只读库打开失败: {type(e).__name__}: {e}")
        return {}
    try:
        for level, sql in _THEME_MAP_FALLBACKS:
            try:
                df = store._conn.execute(sql).fetchdf()
            except Exception as e:
                logger.warning(f"题材映射 {level} 查询失败，降级下一级: {type(e).__name__}: {e}")
                continue
            mapping: dict[str, str] = {}
            for r in df.itertuples():
                code = str(r.con_code)
                if code not in mapping:  # 一票多题材保留首个
                    mapping[code] = str(r.board_name)
            if mapping:
                logger.info(f"surge 题材映射命中 {level}：{len(mapping)} 只")
                return mapping
            logger.debug(f"题材映射 {level} 无数据，降级下一级")
        return {}
    finally:
        if owns:
            store.close()


def preload_baseline(curve_path: Path | None = None) -> SurgeBaseline:
    """启动预载：一次性打开只读副本读 20 日均额 + kpl 题材，加载进度曲线。"""
    from rquant.storage.duckdb import open_readonly_store

    avg20: dict[str, float] = {}
    theme: dict[str, str] = {}
    try:
        store = open_readonly_store(required_tables=("daily_bar",))
    except Exception as e:
        logger.warning(f"基线预载只读库打开失败，avg20 空: {type(e).__name__}: {e}")
        store = None
    if store is not None:
        try:
            avg20 = load_avg_amount_20d(store)
            theme = load_theme_map(store)
        finally:
            store.close()
    curve = load_progress_curve(curve_path)
    logger.info(f"surge 基线预载：avg20={len(avg20)} 只、题材映射={len(theme)} 只")
    return SurgeBaseline(avg_amount_20d=avg20, theme=theme, curve=curve)


# ── 快照拉取（复用 panorama 加固 Session，fs 换创业/科创） ──────────────────────


def _boards_env() -> tuple[str, ...]:
    """RQUANT_SURGE_BOARDS 覆盖检测板块（逗号分隔 gem/star/main/all）；缺省创业+科创。"""
    raw = os.environ.get("RQUANT_SURGE_BOARDS", "").strip().lower()
    if not raw:
        return _DEFAULT_SURGE_BOARDS
    if raw in ("all", "*"):
        return ("gem", "star", "main_sh", "main_sz", "bj")
    parts: list[str] = []
    for p in raw.split(","):
        p = p.strip()
        if p == "main":
            parts.extend(["main_sh", "main_sz"])
        elif p in _BOARD_FS:
            parts.append(p)
    return tuple(dict.fromkeys(parts)) or _DEFAULT_SURGE_BOARDS


def _fs_for_boards(boards: tuple[str, ...]) -> str:
    return ",".join(_BOARD_FS[b] for b in boards if b in _BOARD_FS)


def _fetch_em_clist(fs: str, proxies: dict[str, str] | None, timeout: float) -> pd.DataFrame:
    """东财 push2 clist 分页拉取给定 fs 的全量快照，归一化为标准快照列。

    复用 panorama_data 的加固 Session + 归一化（f5 手 ×100 成股），仅 fs 可变——
    不改 panorama 现有函数，只借其模块级构件。
    """
    from rquant.panorama_data import (
        _EM_CLIST_URL,
        _EM_SPOT_FIELDS,
        _EM_SPOT_MAX_PAGES,
        _EM_SPOT_PAGE_SIZE,
        _em_session,
        _normalize_em_spot_rows,
    )

    rows: list[dict] = []
    with _em_session() as session:
        for pn in range(1, _EM_SPOT_MAX_PAGES + 1):
            params = {
                "pn": pn, "pz": _EM_SPOT_PAGE_SIZE, "po": 1, "np": 1,
                "fltt": 2, "invt": 2, "fid": "f6", "fs": fs,
                "fields": _EM_SPOT_FIELDS,
            }
            resp = session.get(_EM_CLIST_URL, params=params, timeout=timeout, proxies=proxies)
            resp.raise_for_status()
            data = (resp.json() or {}).get("data") or {}
            diff = data.get("diff") or []
            if not diff:
                break
            rows.extend(diff)
            if len(rows) >= int(data.get("total") or 0):
                break
    return _normalize_em_spot_rows(rows)


def fetch_board_snapshot(boards: tuple[str, ...] | None = None) -> pd.DataFrame:
    """拉一次检测域快照（默认创业+科创），带涨停价，``df.attrs['route']`` 标注。

    东财直连 → SOCKS 云端出口（RQUANT_PANORAMA_SOCKS，置空禁用）→ 空表 route=none
    （本分钟 miss，由主循环熔断退避）。云端 IP 干净，直连通常一击即中。
    """
    from rquant.panorama_data import add_limit_prices

    boards = boards or _boards_env()
    fs = _fs_for_boards(boards)
    for route, proxies, timeout in _snapshot_routes():
        try:
            df = _fetch_em_clist(fs, proxies=proxies, timeout=timeout)
            if not df.empty:
                out = add_limit_prices(df)
                out.attrs["route"] = route
                return out
            logger.warning(f"surge 快照 {route} 返回空")
        except Exception as e:
            logger.warning(f"surge 快照 {route} 失败: {type(e).__name__}: {e}")
    empty = pd.DataFrame()
    empty.attrs["route"] = "none"
    return empty


def fetch_full_market_snapshot() -> pd.DataFrame:
    """全市场快照（供 P2 Mac feed），带涨停价，route 标注。每 5 分钟拉一次。"""
    from rquant.panorama_data import _EM_SPOT_FS, add_limit_prices

    for route, proxies, timeout in _snapshot_routes():
        try:
            df = _fetch_em_clist(_EM_SPOT_FS, proxies=proxies, timeout=timeout)
            if not df.empty:
                out = add_limit_prices(df)
                out.attrs["route"] = route
                return out
        except Exception as e:
            logger.warning(f"surge 全市场快照 {route} 失败: {type(e).__name__}: {e}")
    empty = pd.DataFrame()
    empty.attrs["route"] = "none"
    return empty


def _snapshot_routes() -> list[tuple[str, dict[str, str] | None, float]]:
    routes: list[tuple[str, dict[str, str] | None, float]] = [("em_direct", None, 5.0)]
    socks = os.environ.get("RQUANT_PANORAMA_SOCKS", _DEFAULT_SOCKS_PROXY).strip()
    if socks:
        routes.append(("em_socks", {"http": socks, "https": socks}, 10.0))
    return routes


# ── 确认层：3 日同刻基线（stk_mins） ────────────────────────────────────────────


@dataclass
class ThreeDayBaseline:
    """某票近 3 交易日的同刻累计额 / 同分钟增量中位（对齐 241 网格）。"""

    cum_median: np.ndarray      # 241 点：3 日同刻累计额中位（元）
    minute_median: np.ndarray   # 241 点：3 日同分钟增量中位（元）
    days_used: int


_EMPTY_BASELINE = ThreeDayBaseline(
    cum_median=np.zeros(CURVE_POINTS),
    minute_median=np.zeros(CURVE_POINTS),
    days_used=0,
)


def _day_grid_amount(day_bars: pd.DataFrame) -> np.ndarray:
    """单日分钟 bars → 241 网格逐格成交额（同格相加，缺格 0）。"""
    arr = np.zeros(CURVE_POINTS)
    for r in day_bars.itertuples():
        t = pd.Timestamp(r.trade_time).time()
        gi = grid_index(t)
        amt = getattr(r, "amount", None)
        if amt is not None and not pd.isna(amt):
            arr[gi] += float(amt)
    return arr


def build_three_day_baseline(
    bars: pd.DataFrame, today: date, lookback_days: int = 3
) -> ThreeDayBaseline:
    """从 stk_mins 结果构造近 3 交易日同刻中位基线（严格早于 today 的最近 N 日）。

    每日在 241 网格上累计，跨日逐格取中位 → 同刻累计额中位 + 同分钟增量中位。
    无可用历史日 → 空基线（该票不可确认）。
    """
    if bars is None or bars.empty or "trade_time" not in bars.columns:
        return _EMPTY_BASELINE
    df = bars.copy()
    df["trade_time"] = pd.to_datetime(df["trade_time"])
    df["_d"] = df["trade_time"].dt.date
    dates = sorted(d for d in df["_d"].unique() if d < today)[-lookback_days:]
    if not dates:
        return _EMPTY_BASELINE
    cum_stack: list[np.ndarray] = []
    min_stack: list[np.ndarray] = []
    for d in dates:
        amt = _day_grid_amount(df[df["_d"] == d])
        min_stack.append(amt)
        cum_stack.append(np.cumsum(amt))
    cum_median = np.median(np.vstack(cum_stack), axis=0)
    minute_median = np.median(np.vstack(min_stack), axis=0)
    return ThreeDayBaseline(
        cum_median=cum_median, minute_median=minute_median, days_used=len(dates)
    )


# ── 主检测器 ────────────────────────────────────────────────────────────────────


def _rough_candidates(
    snapshot: pd.DataFrame, baseline: SurgeBaseline, config: SurgeConfig, gi: int
) -> list[str]:
    """粗筛（零外部调用）：cum(t) ≥ K_rough × 20 日均额 × 曲线(t)，pct_chg>0、非 ST、有基线。

    缺 20 日基线（次新）自动落选。返回通过的 ts_code 列表（曲线值兜底 >0）。
    """
    if snapshot.empty:
        return []
    curve_v = float(baseline.curve[gi])
    if curve_v <= 0:
        curve_v = 1.0 / CURVE_POINTS
    out: list[str] = []
    for r in snapshot.itertuples():
        code = str(r.ts_code)
        avg20 = baseline.avg_amount_20d.get(code)
        if avg20 is None or avg20 <= 0:
            continue
        pct = getattr(r, "pct_chg", None)
        if pct is None or pd.isna(pct) or pct <= 0:
            continue
        if _detect_st(getattr(r, "name", None)):
            continue
        amount = getattr(r, "amount", None)
        if amount is None or pd.isna(amount):
            continue
        threshold = config.k_rough * avg20 * curve_v
        if float(amount) >= threshold:
            out.append(code)
    return out


def _snapshot_row(snapshot: pd.DataFrame, code: str) -> pd.Series | None:
    sub = snapshot[snapshot["ts_code"].astype(str) == code]
    if sub.empty:
        return None
    return sub.iloc[0]


class SurgeWatcher:
    """无 IO 的检测状态机：喂快照 + 墙钟，产出待推送报文 + 新确认。

    落盘 / 推送 / 网络全在 ``run``；本类只做判定与去重，单测直接驱动 ``tick``。
    """

    def __init__(
        self,
        baseline: SurgeBaseline,
        *,
        config: SurgeConfig | None = None,
        minute_fetcher: Callable[[str, date], pd.DataFrame] | None = None,
        theme_map: dict[str, str] | None = None,
    ) -> None:
        self.baseline = baseline
        self.config = config or SurgeConfig()
        # 默认取数器在 run 注入；tick 单测必须显式传 fetcher（不碰网络）
        self._minute_fetcher = minute_fetcher or _default_minute_fetcher
        self.theme_map = theme_map if theme_map is not None else baseline.theme

        self.pushed_today: set[str] = set()
        self.confirm_cache: dict[str, ThreeDayBaseline] = {}
        self._pending_fetch: deque[str] = deque()
        self._queued: set[str] = set()
        self._fetch_fail: dict[str, int] = {}
        self._pending_push: list[SurgeConfirmed] = []
        # 可买性守卫拦下的确认：不进 _pending_push（不推送），但仍随本分钟 flush 落 events
        self._pending_events: list[SurgeConfirmed] = []
        self.cum_series: dict[str, np.ndarray] = {}

    # ── 单分钟 tick ──────────────────────────────────────────────────────────

    def tick(self, snapshot: pd.DataFrame, now: datetime) -> TickResult:
        """处理一分钟快照：更新累计序列 → 粗筛 → 确认（限频） → 去重/静默/聚合推送。"""
        gi = grid_index(now.time())
        self._update_cum_series(snapshot, gi)

        rough = _rough_candidates(snapshot, self.baseline, self.config, gi)
        rough_set = set(rough)
        for code in rough:
            if code in self.pushed_today:
                continue
            if code in self.confirm_cache:
                self._evaluate(code, snapshot, now, gi)
            elif code not in self._queued:
                self._queued.add(code)
                self._pending_fetch.append(code)

        self._drain_fetch_queue(snapshot, now, gi, rough_set)

        return self._flush(now)

    def _update_cum_series(self, snapshot: pd.DataFrame, gi: int) -> None:
        for r in snapshot.itertuples():
            code = str(r.ts_code)
            amount = getattr(r, "amount", None)
            if amount is None or pd.isna(amount):
                continue
            arr = self.cum_series.get(code)
            if arr is None:
                arr = np.full(CURVE_POINTS, np.nan)
                self.cum_series[code] = arr
            arr[gi] = float(amount)

    def _drain_fetch_queue(
        self, snapshot: pd.DataFrame, now: datetime, gi: int, rough_set: set[str]
    ) -> None:
        """限频消费确认队列：每 tick 至多 rate_per_min 次 stk_mins 取数。

        取数失败延后重试（回队尾，不阻塞后续候选）；超重试上限丢弃。当日缓存命中
        不占限频、不重拉。
        """
        budget = self.config.tushare_rate_per_min
        requeue: list[str] = []
        while budget > 0 and self._pending_fetch:
            code = self._pending_fetch.popleft()
            self._queued.discard(code)
            if code in self.pushed_today:
                continue
            if code in self.confirm_cache:  # 期间已被缓存 → 直接评估，不耗预算
                self._evaluate(code, snapshot, now, gi)
                continue
            budget -= 1
            try:
                bars = self._minute_fetcher(code, now.date())
            except Exception as e:
                self._fetch_fail[code] = self._fetch_fail.get(code, 0) + 1
                logger.warning(
                    f"surge 确认取数失败 {code}"
                    f"（第 {self._fetch_fail[code]}/{self.config.tushare_max_retries} 次）:"
                    f" {type(e).__name__}: {e}"
                )
                if self._fetch_fail[code] < self.config.tushare_max_retries:
                    requeue.append(code)  # 延后重试，回队尾
                continue
            self.confirm_cache[code] = build_three_day_baseline(
                bars, now.date(), self.config.confirm_lookback_days
            )
            self._evaluate(code, snapshot, now, gi)
        for code in requeue:
            if code not in self.pushed_today:
                self._queued.add(code)
                self._pending_fetch.append(code)

    def _evaluate(self, code: str, snapshot: pd.DataFrame, now: datetime, gi: int) -> None:
        """确认判定（口径 v2）：rel_cum_3d ≥ K_confirm 且现价 ≥ VWAP，再过同分钟增量门；
        通过后若现价距涨停 ≤ max_room（或已封板）标 unbuyable 只落 events 不推送。"""
        if code in self.pushed_today:
            return
        base = self.confirm_cache.get(code)
        if base is None or base.days_used == 0:
            return
        row = _snapshot_row(snapshot, code)
        if row is None:
            return
        amount = row.get("amount")
        volume = row.get("volume")
        price = row.get("price")
        if amount is None or pd.isna(amount) or price is None or pd.isna(price):
            return
        base_cum = float(base.cum_median[gi])
        if base_cum <= 0:
            return
        rel = float(amount) / base_cum
        if rel < self.config.k_confirm:
            return
        # VWAP 门：现价 ≥ 当日均价（amount/volume）
        if volume is None or pd.isna(volume) or float(volume) <= 0:
            return
        vwap = float(amount) / float(volume)
        if float(price) < vwap:
            return

        arr = self.cum_series.get(code)
        minute_delta = _minute_delta(arr, gi) if arr is not None else None
        # 同分钟增量门：当分钟增量 ≥ k_delta × 3 日同分钟中位（中位≤0 / 增量缺失 → 不过）。
        if self.config.k_delta_confirm > 0:
            med = float(base.minute_median[gi])
            if med <= 0 or minute_delta is None or minute_delta < self.config.k_delta_confirm * med:
                return

        curve_v = float(self.baseline.curve[gi]) or (1.0 / CURVE_POINTS)
        avg20 = self.baseline.avg_amount_20d.get(code)
        rough_ratio = float(amount) / (avg20 * curve_v) if avg20 else 0.0
        limit_up = row.get("limit_up_price")
        room = None
        if limit_up is not None and not pd.isna(limit_up) and float(price) > 0:
            room = (float(limit_up) / float(price) - 1) * 100
        confirmed = SurgeConfirmed(
            ts_code=code,
            name=str(row.get("name", "")),
            theme=self.theme_map.get(code, ""),
            confirmed_at=now.strftime("%H:%M"),
            pct_chg=round(float(row.get("pct_chg", 0.0) or 0.0), 2),
            cum_amount=round(float(amount), 0),
            rel_cum_3d=round(rel, 2),
            rough_ratio=round(rough_ratio, 2),
            minute_delta=round(minute_delta, 0) if minute_delta is not None else None,
            minute_delta_median_3d=round(float(base.minute_median[gi]), 0),
            room_to_limit_pct=round(room, 2) if room is not None else None,
        )
        # 可买性守卫：现价距涨停 ≤ 门（或已封板 room≤0）→ 买不进，仅落 events 标 unbuyable。
        # 仍占「每票每日一次」名额：封板回落再爆当天已看过，防同一票反复刷屏。
        # room 未知（缺涨停价）→ 无法判定不可买，按可买放行（fail-open）。
        if room is not None and room <= self.config.max_room_to_limit_pct:
            confirmed.status = "unbuyable"
            self.pushed_today.add(code)
            self._pending_events.append(confirmed)
            return
        self.pushed_today.add(code)  # 每票每日仅推一次（入待推送即定）
        self._pending_push.append(confirmed)

    def _flush(self, now: datetime) -> TickResult:
        """静默窗后聚合本分钟待推送为报文（单条 ≤N 只，超出折叠）。

        pushes 只含可买确认（_pending_push）；confirmed（落 events）含可买 + 被可买性
        守卫拦下的 unbuyable（_pending_events），后者不进报文但仍留研究痕迹。
        """
        if now.time() < self.config.silent_until:
            return TickResult()
        if not self._pending_push and not self._pending_events:
            return TickResult()
        push_batch = self._pending_push
        event_batch = self._pending_events
        self._pending_push = []
        self._pending_events = []
        pushes = build_surge_messages(push_batch, now, self.config)
        return TickResult(pushes=pushes, confirmed=push_batch + event_batch)

    def dump_series(self) -> pd.DataFrame:
        """当日累计额序列长表：ts_code, minute_idx, cum_amount（收盘落 parquet 研究）。"""
        return series_to_frame(self.cum_series)


def _minute_delta(cum_arr: np.ndarray, gi: int) -> float | None:
    """本分钟增量 = cum[gi] − 前一个非空累计值（首格无前值 → cum[gi] 本身）。"""
    cur = cum_arr[gi]
    if np.isnan(cur):
        return None
    for j in range(gi - 1, -1, -1):
        if not np.isnan(cum_arr[j]):
            return float(cur - cum_arr[j])
    return float(cur)


def series_to_frame(cum_series: dict[str, np.ndarray]) -> pd.DataFrame:
    rows: list[dict] = []
    for code, arr in cum_series.items():
        for idx, v in enumerate(arr):
            if not np.isnan(v):
                rows.append({"ts_code": code, "minute_idx": idx, "cum_amount": float(v)})
    return pd.DataFrame(rows, columns=["ts_code", "minute_idx", "cum_amount"])


# ── 报文渲染 ────────────────────────────────────────────────────────────────────


def _fmt_amount(v: float | None) -> str:
    if v is None:
        return "—"
    if abs(v) >= 1e8:
        return f"{v / 1e8:.2f}亿"
    return f"{v / 1e4:.0f}万"


def build_surge_messages(
    confirmed: list[SurgeConfirmed], now: datetime, config: SurgeConfig
) -> list[tuple[str, str]]:
    """本分钟新确认 → 一条聚合报文（超 max_per_push 折叠「另有 X 只」）。"""
    if not confirmed:
        return []
    hhmm = now.strftime("%H:%M")
    shown = confirmed[: config.max_per_push]
    extra = len(confirmed) - len(shown)
    title = f"爆量 {hhmm} 新确认 {len(confirmed)} 只"
    lines = [f"# 爆量确认 {hhmm}（{len(confirmed)} 只）", ""]
    for c in shown:
        theme = f"·{c.theme}" if c.theme else ""
        room = f" 距涨停{c.room_to_limit_pct:.1f}%" if c.room_to_limit_pct is not None else ""
        lines.append(
            f"- {c.ts_code} {c.name}{theme} +{c.pct_chg:.1f}% "
            f"量比3日{c.rel_cum_3d:.1f}× 累计{_fmt_amount(c.cum_amount)}"
            f"（本分钟{_fmt_amount(c.minute_delta)}/3日中位{_fmt_amount(c.minute_delta_median_3d)}）"
            f"{room}"
        )
    if extra > 0:
        lines.append(f"- 另有 {extra} 只（本分钟共 {len(confirmed)} 只确认）")
    lines.append("")
    delta_seg = (
        f" / 增量门{config.k_delta_confirm:g}×{config.confirm_lookback_days}d同分钟"
        if config.k_delta_confirm > 0
        else ""
    )
    lines.append(
        f"> 口径 v2: rough{config.k_rough:g}×20d·curve / "
        f"confirm{config.k_confirm:g}×{config.confirm_lookback_days}d同刻{delta_seg}"
        f" / VWAP门 / 距涨停>{config.max_room_to_limit_pct:g}%"
    )
    lines.append("> ⚠️ 观察提示，非买入信号（收紧后按推送价持有到收盘均值仍为负）")
    return [(title, "\n".join(lines))]


# ── 落盘（parquet / jsonl，原子写；绝不碰 DuckDB） ──────────────────────────────


def default_live_dir() -> Path:
    from rquant.config import settings

    return settings.data_dir / LIVE_DIR_NAME


def atomic_write_parquet(df: pd.DataFrame, path: Path) -> None:
    """tmp+rename 原子写 parquet；失败只 log（落盘是共享层，不打断主循环）。"""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        df.to_parquet(tmp, index=False)
        os.replace(tmp, path)
    except Exception as e:
        logger.warning(f"surge 落盘失败（不影响主循环）: {path.name} {type(e).__name__}: {e}")


def append_events(path: Path, confirmed: list[SurgeConfirmed]) -> None:
    """确认事件 append 到当日 jsonl（每行一只票的完整判定字段）。"""
    if not confirmed:
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            for c in confirmed:
                f.write(json.dumps(c.model_dump(), ensure_ascii=False) + "\n")
    except Exception as e:
        logger.warning(f"surge events 落盘失败: {path.name} {type(e).__name__}: {e}")


# ── 默认取数器（run 用；tick 单测注入 fake，不碰网络） ──────────────────────────


def _default_minute_fetcher(ts_code: str, today: date) -> pd.DataFrame:
    """默认 stk_mins 取数：拉 today 前 ~8 日窗口 1min bars（含足够 3 交易日历史）。"""
    from rquant.adapter.tushare import TushareAdapter

    start = datetime.combine(today - timedelta(days=8), dt_time(9, 0))
    end = datetime.combine(today - timedelta(days=1), CLOSE_TIME)
    adapter = TushareAdapter()
    return adapter.stk_mins(ts_code, "1min", start, end)


# ── 守卫 + 主循环 ───────────────────────────────────────────────────────────────


def _now_cst() -> datetime:
    return datetime.now(CST)


def _is_lunch(t: dt_time) -> bool:
    return MORNING_END < t < AFTERNOON_START


def _backoff_next(streak: int) -> float:
    """快照连续 miss 退避序列 60→120→300（封顶）。"""
    ladder = [60.0, 120.0, 300.0]
    return ladder[min(streak - 1, len(ladder) - 1)] if streak >= 1 else 60.0


def run_surge_watch(
    *,
    dry_run: bool = False,
    force_session: bool = False,
    config: SurgeConfig | None = None,
    base_dir: Path | None = None,
    now_fn: Callable[[], datetime] = _now_cst,
    sleep_fn: Callable[[float], None] = _time_module.sleep,
    snapshot_fetcher: Callable[[], pd.DataFrame] | None = None,
    full_snapshot_fetcher: Callable[[], pd.DataFrame] | None = None,
    minute_fetcher: Callable[[str, date], pd.DataFrame] | None = None,
    notify_fn: Callable[..., None] | None = None,
    is_trading_day_fn: Callable[[date], bool] | None = None,
    baseline: SurgeBaseline | None = None,
    max_ticks: int | None = None,
) -> int:
    """常驻主循环：守卫 → 每分钟拉快照 → tick → 推送/落盘 → 15:02 退出。

    时钟/源/推送/sleep 全可注入。非交易日即退；午休 sleep；快照连续 5 miss 推一条
    降级告警（每日至多一条）并退避 60/120/300。``force_session`` 忽略时段守卫（盘后
    验收）；``max_ticks`` 限定循环次数（dry-run/测试）。
    """
    config = config or SurgeConfig()
    day = now_fn().date()
    trading_check = is_trading_day_fn or _load_is_trading_day
    if not force_session and not trading_check(day):
        logger.info(f"{day} 非交易日，surge-watch 退出")
        return 0

    baseline = baseline or preload_baseline()
    snapshot_fetcher = snapshot_fetcher or (lambda: fetch_board_snapshot(config.boards))
    full_snapshot_fetcher = full_snapshot_fetcher or fetch_full_market_snapshot
    notify_fn = notify_fn or _default_notify
    live_dir = (base_dir or default_live_dir())

    watcher = SurgeWatcher(baseline, config=config, minute_fetcher=minute_fetcher)
    events_path = live_dir / f"events-{day.isoformat()}.jsonl"

    miss_streak = 0
    degraded_alerted = False
    last_full_minute: int | None = None
    ticks = 0

    logger.info(
        f"surge-watch 启动 day={day} boards={config.boards} "
        f"k_rough={config.k_rough} k_confirm={config.k_confirm} dry_run={dry_run}"
    )
    try:
        while True:
            now = now_fn()
            if max_ticks is not None and ticks >= max_ticks:
                break
            if not force_session and now.time() >= EXIT_TIME:
                break
            if not force_session and _is_lunch(now.time()):
                sleep_fn(30.0)
                continue

            snapshot = snapshot_fetcher()
            route = snapshot.attrs.get("route", "none") if snapshot is not None else "none"
            if snapshot is None or snapshot.empty:
                miss_streak += 1
                logger.warning(f"surge 快照 miss（连续 {miss_streak}），route={route}")
                if miss_streak >= config.miss_circuit_threshold:
                    if not degraded_alerted and not dry_run:
                        notify_fn(
                            "error",
                            component="surge-watch:snapshot",
                            exc=RuntimeError(f"快照连续 {miss_streak} 分钟拉取失败，暂停检测退避"),
                        )
                        degraded_alerted = True
                    sleep_fn(_backoff_next(miss_streak - config.miss_circuit_threshold + 1))
                    ticks += 1
                    continue
                sleep_fn(60.0)
                ticks += 1
                continue

            miss_streak = 0
            result = watcher.tick(snapshot, now)
            atomic_write_parquet(snapshot, live_dir / "snapshot.parquet")

            cur_minute = now.hour * 60 + now.minute
            if last_full_minute is None or cur_minute - last_full_minute >= 5:
                full = full_snapshot_fetcher()
                if full is not None and not full.empty:
                    atomic_write_parquet(full, live_dir / "snapshot_full.parquet")
                    last_full_minute = cur_minute

            if result.confirmed:
                append_events(events_path, result.confirmed)
            for title, body in result.pushes:
                if dry_run:
                    print(f"\n===== [DRY-RUN] {title} =====\n{body}\n")
                else:
                    notify_fn("surge_watch", title=title, body=body)

            ticks += 1
            sleep_fn(_seconds_to_next_minute(now))
    except KeyboardInterrupt:
        logger.info("surge-watch 收到中断，收尾退出")

    series = watcher.dump_series()
    if not series.empty:
        atomic_write_parquet(series, live_dir / f"{day.isoformat()}-series.parquet")
    logger.info(f"surge-watch 退出 day={day} 累计推送票数={len(watcher.pushed_today)}")
    return 0


def run_simulate(
    sim_dir: Path,
    *,
    dry_run: bool = True,
    config: SurgeConfig | None = None,
    base_dir: Path | None = None,
    minute_fetcher: Callable[[str, date], pd.DataFrame] | None = None,
    baseline: SurgeBaseline | None = None,
    notify_fn: Callable[..., None] | None = None,
) -> int:
    """--simulate：读目录内按文件名排序的快照 parquet 序列逐分钟回放，复用主检测逻辑。

    文件名内嵌墙钟（如 ``0931.parquet`` / ``2026-07-06T0931.parquet``），据此构造
    tick 的 now。events / 推送与真实路径同构，便于验收逐条核对。

    自包含离线回放（CLI 无网/无 DB 也能跑）：目录可携带
    ``baseline.json``（``{"avg20":{code:元}, "theme":{code:题材}}``）与
    ``confirm_bars.parquet``（含 ts_code/trade_time/amount，作 3 日同刻基线源）；
    显式传入的 baseline / minute_fetcher 优先。
    """
    sim_dir = Path(sim_dir)
    config = config or SurgeConfig()
    baseline = baseline or _load_sim_baseline(sim_dir) or preload_baseline()
    minute_fetcher = minute_fetcher or _sim_minute_fetcher(sim_dir)
    notify_fn = notify_fn or _default_notify
    live_dir = (base_dir or default_live_dir())
    reserved = {"confirm_bars.parquet"}
    files = sorted(
        p for p in sim_dir.glob("*.parquet")
        if not p.name.endswith(".tmp") and p.name not in reserved
    )
    if not files:
        logger.warning(f"--simulate 目录无快照 parquet: {sim_dir}")
        return 1

    watcher = SurgeWatcher(baseline, config=config, minute_fetcher=minute_fetcher)
    day = _sim_day(files[0])
    events_path = live_dir / f"events-{day.isoformat()}.jsonl"
    for f in files:
        snapshot = pd.read_parquet(f)
        now = _sim_now(f)
        result = watcher.tick(snapshot, now)
        if result.confirmed:
            append_events(events_path, result.confirmed)
        for title, body in result.pushes:
            if dry_run:
                print(f"\n===== [SIM DRY-RUN] {title} =====\n{body}\n")
            else:
                notify_fn("surge_watch", title=title, body=body)
    series = watcher.dump_series()
    if not series.empty:
        atomic_write_parquet(series, live_dir / f"{day.isoformat()}-series.parquet")
    logger.info(f"--simulate 回放 {len(files)} 帧完成，累计推送票数={len(watcher.pushed_today)}")
    return 0


def _load_sim_baseline(sim_dir: Path) -> SurgeBaseline | None:
    """读 sim 目录自带 baseline.json（avg20 + theme），加载包内曲线。缺失 → None。"""
    p = sim_dir / "baseline.json"
    if not p.exists():
        return None
    raw = json.loads(p.read_text(encoding="utf-8"))
    avg20 = {str(k): float(v) for k, v in (raw.get("avg20") or {}).items()}
    theme = {str(k): str(v) for k, v in (raw.get("theme") or {}).items()}
    return SurgeBaseline(avg_amount_20d=avg20, theme=theme, curve=load_progress_curve())


def _sim_minute_fetcher(sim_dir: Path) -> Callable[[str, date], pd.DataFrame]:
    """从 sim 目录 confirm_bars.parquet 切片当只票的历史分钟（离线确认基线源）。"""
    path = sim_dir / "confirm_bars.parquet"
    cache: dict[str, pd.DataFrame] = {}

    def fetch(ts_code: str, today: date) -> pd.DataFrame:
        if "all" not in cache:
            cache["all"] = pd.read_parquet(path) if path.exists() else pd.DataFrame()
        allbars = cache["all"]
        if allbars.empty or "ts_code" not in allbars.columns:
            return pd.DataFrame()
        return allbars[allbars["ts_code"].astype(str) == ts_code].copy()

    return fetch


def _sim_now(path: Path) -> datetime:
    """从文件名解析墙钟：支持 ``HHMM`` 或 ``...THHMM`` / ``...-HHMM`` 后缀。"""
    stem = path.stem
    token = stem.split("T")[-1].split("_")[-1].split("-")[-1]
    digits = "".join(ch for ch in token if ch.isdigit())[-4:]
    if len(digits) == 4:
        hh, mm = int(digits[:2]), int(digits[2:])
    else:
        hh, mm = 9, 31
    return datetime.combine(_sim_day(path), dt_time(hh, mm), tzinfo=CST)


def _sim_day(path: Path) -> date:
    """文件名含 YYYY-MM-DD 则用之，否则回落今天。"""
    stem = path.stem
    for i in range(len(stem) - 9):
        chunk = stem[i:i + 10]
        try:
            return date.fromisoformat(chunk)
        except ValueError:
            continue
    return _now_cst().date()


def _seconds_to_next_minute(now: datetime) -> float:
    return max(1.0, 60.0 - now.second - now.microsecond / 1e6)


def _load_is_trading_day(day: date) -> bool:
    from rquant.monitor import is_trading_day

    return is_trading_day(day)


def _default_notify(scene: str, **kwargs) -> None:
    from rquant.notify import notify

    notify(scene, **kwargs)  # type: ignore[arg-type]
