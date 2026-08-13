"""每分钟爆量推送（surge-watch）：云端常驻单进程循环。

盘中每分钟拉一次**全市场**快照（一次取数兼作检测输入 + 全景页共享 feed），
检测层按 ``config.boards`` 收窄到创业/科创后两层判定，聚合推 PushDeer：

- **粗筛（零外部调用）**：当日累计成交额 ≥ ``K_rough × 20 日均额 × 进度曲线(t)``
  且 pct_chg>0、非 ST、有 20 日基线（缺基线的次新自动落选），只进候选不推送。
  粗筛只负责「值不值得拉 tushare 确认」，不卡收益——2026-07-06 回测发现原 1.5×
  会把候选挡在确认池外、延迟信号，故放松到 1.2×，让候选早进确认层。
- **确认层（口径 v4，累计 + 价格方向）**：对新候选拉 tushare stk_mins 近 N（默认 4）个
  交易日 1min bars，构造同刻累计额中位基准，
  ``rel_cum = today_cum(t) / median_Nday_same_time_cum(t) ∈ [k_cum, ratio_cap]``
  并用 rt_min_daily 精确复核红盘、当分钟上涨、tick-rule 近似外盘占优；所有计算只取
  当前分钟及之前数据。t 越过 skip_first_minutes 后才确认。

口径演进（诚实标注）：v2 曾叠加 VWAP 门 + 单分钟增量门收紧，但 2026-07-06 全天真实
分钟回测证明这些门把信号系统性拖到爆量展开后、买在阶段高点；纯累计口径买在爆量刚起
（86% 在 10:00 前触发），完胜 v2。故 v3 移除两门（字段保留但默认关：k_delta_confirm=0、
require_vwap=False），ratio_cap 上限挡极端出货毒尾（比值 11-20× 在负收益组扎堆）。
v4 保留 v3 成交额核心，补上确认时价格方向门，修复候选排队后转跌仍推送的问题。

数据源（2026-07-07 事故后根治）：全市场快照改用 tushare ``rt_min``（token 认证，
不吃 IP 反爬）。此前爬东财 push2 clist / 新浪快照，2026-07-07 盘中云端 IP 被东财
（RemoteDisconnected）+ 新浪（HTML 反爬页）双双拉黑，surge 零快照饿死、一早无推送；
tushare rt_min 一次拉全部 A 股（~5500 只）从本机秒回，是根治。``rt_min`` 每根返回
**当分钟成交量/额**（非累计），``CumulativeTracker`` 按分钟去重累加成当日累计额，
组装快照喂检测层 + 全景页共享 feed；确认层今日累计改用 ``rt_min_daily`` 单只当日全
序列 cumsum（精确权威），前 N 日基线仍 stk_mins。

纪律（对齐 CLAUDE.md）：
- **绝不写 DuckDB**：只在 9:25 启动时读一次只读副本预载 20 日均额 + kpl 题材成分
  + 全 A 股代码全集/名称/昨收（rt_min 不带名称/昨收），全部载内存，盘中零 DB 访问；
  累加器纯内存、自产数据全 parquet/jsonl；
- 时钟 / 数据源 / 推送 / sleep 全部可注入——单测不真 sleep、不碰网络；
- rt_min 全市场快照（含主板/创业/科创/北交所）每分钟原子落 ``snapshot_full.parquet``
  供全景页共享 feed，检测层再按 ``config.boards`` 收窄到创业/科创。
"""

from __future__ import annotations

import bisect
import importlib.resources
import json
import os
import time as _time_module
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta, timezone
from datetime import time as dt_time
from pathlib import Path

import numpy as np
import pandas as pd
from loguru import logger
from pydantic import BaseModel, Field

from rquant.legacy_shadow_export import LegacySurgeCollectionProof
from rquant.runtime_contracts import canonical_sha256
from rquant.state.derive import _classify_board, _detect_st

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
COLLECTION_PROOF_START = dt_time(9, 25)
MORNING_END = dt_time(11, 30)
AFTERNOON_START = dt_time(13, 0)
CLOSE_TIME = dt_time(15, 0)
EXIT_TIME = dt_time(15, 2)  # 15:02 自然退出（收盘后无新增量）

DEFAULT_CURVE_FILENAME = "intraday_progress_curve.json"

# 检测板块判定复用 state.derive._classify_board（ts_code 前缀 → main/gem/star/bj）：
# surge 盘中只对创业(gem)/科创(star)检测；全市场快照拉回后在检测层按 config.boards
# 过滤下来（取数范围=全市场，检测范围=config.boards，两者解耦）。
_ALL_DETECTION_BOARDS = ("main", "gem", "star", "bj")
_DEFAULT_SURGE_BOARDS = ("gem", "star")

LIVE_DIR_NAME = "surge_live"

SNAPSHOT_FULL_NAME = "snapshot_full.parquet"
_MIN_FULL_MARKET_COVERAGE_COUNT = 4_000
_MIN_MARKET_COVERAGE_BPS = 9_800


def _active_session_offset_seconds(observed: datetime) -> int | None:
    local_time = observed.time()
    if OPEN_TIME <= local_time <= MORNING_END:
        return int(
            (
                datetime.combine(observed.date(), local_time)
                - datetime.combine(observed.date(), OPEN_TIME)
            ).total_seconds()
        )
    if AFTERNOON_START <= local_time <= CLOSE_TIME:
        morning_seconds = int(
            (
                datetime.combine(observed.date(), MORNING_END)
                - datetime.combine(observed.date(), OPEN_TIME)
            ).total_seconds()
        )
        return morning_seconds + int(
            (
                datetime.combine(observed.date(), local_time)
                - datetime.combine(observed.date(), AFTERNOON_START)
            ).total_seconds()
        )
    return None


@dataclass
class SurgeCollectionTracker:
    trade_date: date
    started_at: datetime
    market_universe: frozenset[str]
    minimum_market_coverage_count: int
    first_success_at: datetime | None = None
    last_success_at: datetime | None = None
    successful_snapshots: int = 0
    empty_successful_snapshots: int = 0
    failed_snapshots: int = 0
    maximum_active_gap_seconds: int = 0
    maximum_consecutive_misses: int = 0
    ending_consecutive_misses: int = 0
    source_routes: set[str] = field(default_factory=set)
    observed_minimum_market_coverage_count: int | None = None
    _last_success_offset: int | None = None

    def __post_init__(self) -> None:
        if self.minimum_market_coverage_count < 1:
            raise ValueError("surge market universe contract is invalid")

    @property
    def market_universe_id(self) -> str:
        return canonical_sha256(
            {
                "contract": "legacy-surge-market-universe/v1",
                "source": "stock-basic-all-a-share",
                "codes": tuple(sorted(self.market_universe)),
            }
        )

    def observe_snapshot(
        self,
        observed_at: datetime,
        snapshot: pd.DataFrame | None,
    ) -> bool:
        route = str(snapshot.attrs.get("route", "none")) if snapshot is not None else "none"
        if snapshot is None or snapshot.empty or "ts_code" not in snapshot.columns:
            self.observe_failure(observed_at, route=route)
            return False
        observed_codes = frozenset(snapshot["ts_code"].astype(str))
        coverage_count = len(observed_codes & self.market_universe)
        required_ratio_count = (
            len(self.market_universe) * _MIN_MARKET_COVERAGE_BPS + 9_999
        ) // 10_000
        required_count = max(
            self.minimum_market_coverage_count,
            required_ratio_count,
        )
        if (
            route != "tushare_rt"
            or observed_codes - self.market_universe
            or coverage_count < required_count
        ):
            self.observe_failure(observed_at, route=route)
            return False
        offset = _active_session_offset_seconds(observed_at)
        if offset is None or observed_at.date() != self.trade_date:
            return True
        if self.first_success_at is None:
            self.first_success_at = observed_at
        if self._last_success_offset is not None:
            self.maximum_active_gap_seconds = max(
                self.maximum_active_gap_seconds,
                offset - self._last_success_offset,
            )
        self._last_success_offset = offset
        self.last_success_at = observed_at
        self.successful_snapshots += 1
        self.ending_consecutive_misses = 0
        self.source_routes.add(route)
        self.observed_minimum_market_coverage_count = (
            coverage_count
            if self.observed_minimum_market_coverage_count is None
            else min(self.observed_minimum_market_coverage_count, coverage_count)
        )
        return True

    def observe_failure(self, observed_at: datetime, *, route: str) -> None:
        if (
            _active_session_offset_seconds(observed_at) is None
            or observed_at.date() != self.trade_date
        ):
            return
        self.failed_snapshots += 1
        self.ending_consecutive_misses += 1
        self.maximum_consecutive_misses = max(
            self.maximum_consecutive_misses,
            self.ending_consecutive_misses,
        )

    def complete_proof(self) -> LegacySurgeCollectionProof | None:
        if (
            self.first_success_at is None
            or self.last_success_at is None
            or self.observed_minimum_market_coverage_count is None
        ):
            return None
        try:
            return LegacySurgeCollectionProof.create(
                trade_date=self.trade_date,
                started_at=self.started_at.astimezone(UTC),
                first_success_at=self.first_success_at.astimezone(UTC),
                last_success_at=self.last_success_at.astimezone(UTC),
                successful_snapshots=self.successful_snapshots,
                nonempty_successful_snapshots=self.successful_snapshots,
                empty_successful_snapshots=0,
                failed_snapshots=self.failed_snapshots,
                maximum_active_gap_seconds=self.maximum_active_gap_seconds,
                maximum_consecutive_misses=self.maximum_consecutive_misses,
                ending_consecutive_misses=self.ending_consecutive_misses,
                source_routes=tuple(sorted(self.source_routes)),
                market_universe_id=self.market_universe_id,
                market_universe_expected_count=len(self.market_universe),
                minimum_market_coverage_count=(self.observed_minimum_market_coverage_count),
                minimum_market_coverage_bps=(
                    self.observed_minimum_market_coverage_count
                    * 10_000
                    // len(self.market_universe)
                ),
                source_health=("recovered" if self.failed_snapshots else "healthy"),
            )
        except ValueError:
            return None


def _boards_env() -> tuple[str, ...]:
    """RQUANT_SURGE_BOARDS 覆盖**检测**板块（逗号分隔 gem/star/main/bj，或 all）；缺省创业+科创。

    语义是检测范围（不是取数范围）：全市场快照拉回后在检测层按此过滤。无效值忽略、
    全无效回落缺省。作 SurgeConfig.boards 的 default_factory（构造时读一次 env）。
    """
    raw = os.environ.get("RQUANT_SURGE_BOARDS", "").strip().lower()
    if not raw:
        return _DEFAULT_SURGE_BOARDS
    if raw in ("all", "*"):
        return _ALL_DETECTION_BOARDS
    parts = [p.strip() for p in raw.split(",") if p.strip() in _ALL_DETECTION_BOARDS]
    return tuple(dict.fromkeys(parts)) or _DEFAULT_SURGE_BOARDS


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
    """surge-watch 判定参数（口径 v4：v3 累计核心 + 当前分钟价格方向）。"""

    # 粗筛门：2026-07-06 回测发现 1.5× 会把候选挡在确认池外、延迟信号，放松到 1.2×
    # 让候选早进确认层（粗筛只决定「值不值得拉 tushare」，真正的推送决策交给 rel_cum）。
    k_rough: float = 1.2
    # 确认层纯累计比值下门：rel_cum = today_cum / N日同刻累计中位 ≥ k_cum 才确认。
    # 2026-07-06 全天真实分钟回测：纯累计（买爆量刚起）完胜 v2 曲线粗筛+VWAP门+增量门
    # （门把信号拖到爆量展开后、买阶段高点）。示例 300499 从 v2 的 -2.30% 翻成 +2.83%。
    k_cum: float = 2.5
    # 累计比值上门（毒尾封顶）：rel_cum > ratio_cap 视为极端出货，不推——2026-07-06
    # 回测里 ratio 11-20× 在负收益组扎堆（放巨量往往是出货而非启动）。
    ratio_cap: float = 8.0
    # 跳过开盘前 N 分钟确认：9:30 开盘首格（gi=0，分母仅 1 分钟）恒不确认，再额外跳
    # skip_first_minutes 分钟。0 → 9:31（gi=1）起即可确认（用户要求尽早推；代价是 9:31
    # 分母只 2 分钟累计、rel 略抖，靠 rel∈[k_cum,ratio_cap] + 粗筛兜住）；1 → 9:32 起。
    skip_first_minutes: int = 0
    # 单分钟增量门（v2 遗留，2026-07-06 回测证明拖累信号，v3 默认关）：>0 时确认要求
    # 「当分钟增量 ≥ k_delta × N日同分钟中位」。0 = 关闭（默认）。
    k_delta_confirm: float = 0.0
    # VWAP 门（v2 遗留，同上默认关）：True 时确认要求现价 ≥ 当日均价（amount/volume）。
    require_vwap: bool = False
    # 价格方向门：真实盘中使用 rt_min_daily 截至确认分钟的精确 K 线，要求当分钟上涨且
    # tick-rule 近似外盘大于内盘。分钟接口尚未覆盖当前格时暂缓确认，避免用 rt_min
    # 全市场快照的滞后价格误报。离线 simulate 未注入 rt_min_daily 时只复核当前涨幅。
    require_price_strength: bool = True
    min_return_1m_pct: float = 0.0
    min_outer_inner_ratio: float = Field(default=1.0, gt=0)
    # 可买性守卫：确认时现价距涨停价 ≤ 该 %（或已封板）标 unbuyable（报文加「临近涨停」
    # icon,仍推送、仍占「每票每日一次」名额）。0 = 只标已封板；负值可整体关闭（room 恒 > 负值）。
    max_room_to_limit_pct: float = 1.0
    cum_lookback_days: int = 4  # 同刻累计中位回溯交易日数（v3 默认 4）
    max_per_push: int = 8  # 单条报文最多 N 只，超出折叠
    silent_until_hhmm: str = "09:31"  # 该时刻前只收集不推送（用户要求 9:31 起就推）
    tushare_rate_per_min: int = 2  # 确认层 stk_mins 限频（次/分）
    tushare_max_retries: int = 3  # 单候选取数失败重试上限（延后不阻塞队列）
    miss_circuit_threshold: int = 5  # 快照连续 miss 触发降级告警 + 退避
    # 检测范围（不是取数范围）：全市场快照在检测层按此过滤。默认创业+科创，
    # RQUANT_SURGE_BOARDS 可覆盖（default_factory 构造时读 env）。
    boards: tuple[str, ...] = Field(default_factory=_boards_env)

    @property
    def silent_until(self) -> dt_time:
        h, m = self.silent_until_hhmm.split(":")
        return dt_time(int(h), int(m))


class SurgeConfirmed(BaseModel):
    """确认通过、待推送/落 events 的一只票。"""

    ts_code: str
    name: str
    theme: str = ""
    confirmed_at: str = ""  # HH:MM
    price: float = 0.0  # 推送当时价（入场价,供次日 S 点/最终收益闭环用）
    pct_chg: float = 0.0
    cum_amount: float = 0.0  # 当日累计成交额（元）
    rel_cum: float = 0.0  # today_cum / N 日同刻累计额中位（v3 纯累计核心判据）
    rough_ratio: float = 0.0  # cum / (20 日均额 × 曲线(t))
    minute_delta: float | None = None  # 本分钟增量（元，v2 遗留研究字段）
    minute_delta_median: float | None = None  # N 日同分钟增量中位（元，v2 遗留研究字段）
    room_to_limit_pct: float | None = None  # 距涨停空间（%）
    return_1m_pct: float | None = None
    outer_inner_ratio_approx: float | None = None
    price_source: str = "snapshot"
    push_count_5d: int = Field(default=1, ge=1)
    # confirmed（可买、推送）| unbuyable（距涨停≤门 / 已封板，只落 events 不推送）
    status: str = "confirmed"


class SurgePushHistory(BaseModel):
    """近五个交易日的逐票推送日期；历史只计数，今日记录同时恢复去重。"""

    as_of: date
    window_dates: tuple[date, ...] = ()
    push_dates_by_code: dict[str, frozenset[date]] = Field(default_factory=dict)

    @property
    def pushed_today(self) -> set[str]:
        return {
            code for code, push_dates in self.push_dates_by_code.items() if self.as_of in push_dates
        }

    def count(self, ts_code: str) -> int:
        return len(self.push_dates_by_code.get(ts_code, frozenset()))


class TickResult(BaseModel):
    """单分钟 tick 的产出：待推送报文 + 本分钟新确认（落 events）。"""

    pushes: list[tuple[str, str]] = []  # [(title, body)]
    confirmed: list[SurgeConfirmed] = []


# ── 基线预载（只在启动读一次只读副本；盘中零 DB 访问） ──────────────────────────


@dataclass
class SurgeBaseline:
    """启动预载的市场基线（全部载内存）。"""

    avg_amount_20d: dict[str, float]  # ts_code → 元（daily_bar 千元 ×1000）
    theme: dict[str, str]  # ts_code → 题材名（三级兜底链首个命中，见 load_theme_map）
    curve: np.ndarray  # 241 点进度曲线
    # rt_min 快照只带 ts_code/close/vol/amount，不带名称/昨收，故预载补齐（2026-07-07 换源）：
    code_universe: list[str] = field(default_factory=list)  # 全 A 股代码（stock_basic，喂 rt_min）
    name_map: dict[str, str] = field(default_factory=dict)  # ts_code → 名称（ST 判定 + 报文）
    pre_close: dict[str, float] = field(default_factory=dict)  # ts_code → T-1 收盘（涨停价/涨幅）


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


def load_stock_meta(store=None) -> tuple[list[str], dict[str, str]]:
    """全 A 股代码全集 + ts_code → 名称（rt_min 需代码全集、快照需名称做 ST 判定）。

    优先读 stock_basic（含名称）；缺表则退回 daily_bar distinct ts_code（无名称）。
    代码全集含主板/创业/科创/北交所（全景 feed 需全市场；检测层再 filter 创业科创）。
    只读副本缺表/撞锁 → 空（rt_min 无代码可拉 → 快照 miss，主循环仍活）。
    """
    from rquant.storage.duckdb import open_readonly_store

    owns = store is None
    try:
        store = store or open_readonly_store()
    except Exception as e:
        logger.warning(f"股票元数据只读库打开失败: {type(e).__name__}: {e}")
        return [], {}
    try:
        try:
            df = store._conn.execute("SELECT ts_code, name FROM stock_basic").fetchdf()
            codes = [str(c) for c in df["ts_code"]]
            names = {str(r.ts_code): str(r.name) for r in df.itertuples()}
            return codes, names
        except Exception as e:
            logger.warning(
                f"stock_basic 读取失败，退回 daily_bar 代码全集: {type(e).__name__}: {e}"
            )
        try:
            df = store._conn.execute("SELECT DISTINCT ts_code FROM daily_bar").fetchdf()
            return [str(c) for c in df["ts_code"]], {}
        except Exception as e:
            logger.warning(f"daily_bar 代码全集读取失败: {type(e).__name__}: {e}")
            return [], {}
    finally:
        if owns:
            store.close()


def load_pre_close(store=None) -> dict[str, float]:
    """各票 T-1 收盘价（ts_code → 元）：daily_bar 每票最新 trade_date 的 close。

    rt_min 快照不带昨收，涨停价推算 + 涨幅计算靠此。缺表/撞锁 → 空（涨停价/涨幅缺失，
    可买性守卫 fail-open、涨幅门落选，主循环仍活）。
    """
    from rquant.storage.duckdb import open_readonly_store

    owns = store is None
    try:
        store = store or open_readonly_store(required_tables=("daily_bar",))
    except Exception as e:
        logger.warning(f"昨收只读库打开失败: {type(e).__name__}: {e}")
        return {}
    try:
        df = store._conn.execute(
            """
            SELECT ts_code, close FROM (
              SELECT ts_code, close,
                     ROW_NUMBER() OVER (
                       PARTITION BY ts_code ORDER BY trade_date DESC
                     ) AS rn
              FROM daily_bar
            ) WHERE rn = 1
            """
        ).fetchdf()
    except Exception as e:
        logger.warning(f"昨收查询失败: {type(e).__name__}: {e}")
        return {}
    finally:
        if owns:
            store.close()
    return {str(r.ts_code): float(r.close) for r in df.itertuples() if not pd.isna(r.close)}


def preload_baseline(curve_path: Path | None = None) -> SurgeBaseline:
    """启动预载：一次打开只读副本读 20 日均额 + kpl 题材 + 全 A 股代码/名称/昨收，加载进度曲线。"""
    from rquant.storage.duckdb import open_readonly_store

    avg20: dict[str, float] = {}
    theme: dict[str, str] = {}
    codes: list[str] = []
    names: dict[str, str] = {}
    pre_close: dict[str, float] = {}
    try:
        store = open_readonly_store(required_tables=("daily_bar",))
    except Exception as e:
        logger.warning(f"基线预载只读库打开失败，avg20 空: {type(e).__name__}: {e}")
        store = None
    if store is not None:
        try:
            avg20 = load_avg_amount_20d(store)
            theme = load_theme_map(store)
            codes, names = load_stock_meta(store)
            pre_close = load_pre_close(store)
        finally:
            store.close()
    curve = load_progress_curve(curve_path)
    logger.info(
        f"surge 基线预载：avg20={len(avg20)} 只、题材映射={len(theme)} 只、"
        f"代码全集={len(codes)} 只、昨收={len(pre_close)} 只"
    )
    return SurgeBaseline(
        avg_amount_20d=avg20,
        theme=theme,
        curve=curve,
        code_universe=codes,
        name_map=names,
        pre_close=pre_close,
    )


# ── 全市场快照（tushare rt_min；2026-07-07 换源，token 认证不吃 IP 反爬） ────────
#
# 单位核对（2026-07-07 本机实测比对，归一到元）：
#   - rt_min / rt_min_daily 的 amount = 当分钟成交额，**元**（与 stk_mins 同族）；
#     实测 300499.SZ 2026-07-06 stk_mins 全日 amount 合计 = 2.446936e9 元，
#     daily_bar 同日 amount = 2.446936e6 千元 ×1000 = 2.446936e9 元，逐位吻合。
#   - rt_min vol = 当分钟成交量，**股**（806400 股 × 39.88 元 ≈ 32.2M ≈ amount）；
#     与旧东财快照 volume（f5 手 ×100 归一后的股）同口径，快照 volume 列契约不变。
#   结论：rt_min amount/vol 无需换算即对齐旧快照列契约（volume=股, amount=元）；
#   唯 daily_bar 是千元（load_avg_amount_20d 已 ×1000），不涉 rt_min。

_RT_MIN_SNAPSHOT_COLS = [
    "ts_code",
    "name",
    "price",
    "open",
    "high",
    "low",
    "pre_close",
    "pct_chg",
    "volume",
    "amount",
    "trade_time",
]


def _minute_of_day(ts: pd.Timestamp) -> int:
    """时间戳 → 当日分钟序号（时×60+分），当日内严格单调，作累加器去重键。"""
    return int(ts.hour) * 60 + int(ts.minute)


class CumulativeTracker:
    """rt_min 当分钟量 → 当日累计额/量累加器（纯内存，盘中零 DB）。

    per ts_code 记 ``{last_minute, cum_amount, cum_volume}``：只在某只 rt_min 的
    trade_time 分钟 **严格大于** 上次记录分钟时，才把该分钟 amount/vol 累加进当日累计
    （防同分钟重复计、防分钟回退）。输出快照的 amount/volume = 累加后当日累计，
    price/high/low/pre_close 用 rt_min 最新值——语义对齐旧东财快照（累计额/累计量）。

    重启续算：``seed`` 从上一份当日 snapshot_full.parquet 读回累计额/量 + 最近分钟，
    续到最近一 tick（重启只丢 tick 间隙，确认层 rt_min_daily 恒精确兜底）。
    真实盘中传入 ``session_date`` 后，接口在开盘前返回的上一交易日末根只作快照展示，
    不得写入当日累计或分钟锚点。
    """

    _CUM_COLS: tuple[str, ...] = ("amount", "volume")

    def __init__(self, session_date: date | None = None) -> None:
        self._session_date = session_date
        self._cum: dict[str, dict[str, float]] = {}
        self._last_minute: dict[str, int] = {}

    def seed(self, prev: pd.DataFrame, day: date) -> int:
        """从上一份 snapshot_full（须为 ``day`` 当日）seed 累计额/量 + 最近分钟。

        仅 seed trade_time 属于 ``day`` 的行（非当日整份跳过，从零开始）；缺
        trade_time 列（旧格式/首次运行）→ 0 seed。返回 seed 的股票数。
        """
        if prev is None or prev.empty:
            return 0
        if "trade_time" not in prev.columns or "amount" not in prev.columns:
            return 0
        seeded = 0
        for r in prev.itertuples():
            tt = getattr(r, "trade_time", None)
            if tt is None or pd.isna(tt):
                continue
            ts = pd.Timestamp(tt)
            if ts.date() != day:  # 非当日不 seed
                continue
            code = str(r.ts_code)
            self._cum[code] = {
                col: (
                    float(getattr(r, col))
                    if getattr(r, col, None) is not None and not pd.isna(getattr(r, col, None))
                    else 0.0
                )
                for col in self._CUM_COLS
            }
            self._last_minute[code] = _minute_of_day(ts)
            seeded += 1
        return seeded

    def update(self, raw: pd.DataFrame) -> pd.DataFrame:
        """当分钟量快照 → 当日累计快照（amount/volume 就地替换为累计值）。

        raw 每行是某只票的最新一根分钟 K（trade_time/amount=当分钟量/volume=当分钟量）。
        仅当该只 trade_time 分钟 > 上次记录分钟才累加（否则沿用既有累计，防重复/回退）。
        """
        if raw is None or raw.empty:
            return raw if raw is not None else pd.DataFrame()
        df = raw.copy()
        has_tt = "trade_time" in df.columns
        out: dict[str, list[float]] = {col: [] for col in self._CUM_COLS}
        for r in df.itertuples():
            code = str(r.ts_code)
            minute: int | None = None
            belongs_to_session = self._session_date is None
            if has_tt:
                tt = getattr(r, "trade_time", None)
                if tt is not None and not pd.isna(tt):
                    ts = pd.Timestamp(tt)
                    belongs_to_session = (
                        self._session_date is None or ts.date() == self._session_date
                    )
                    if belongs_to_session:
                        minute = _minute_of_day(ts)
            cum = self._cum.setdefault(code, {c: 0.0 for c in self._CUM_COLS})
            last = self._last_minute.get(code)
            advance = belongs_to_session and minute is not None and (last is None or minute > last)
            if advance:
                for col in self._CUM_COLS:
                    v = getattr(r, col, None)
                    if v is not None and not pd.isna(v):
                        cum[col] += float(v)
                self._last_minute[code] = minute
            for col in self._CUM_COLS:
                out[col].append(cum[col])
        for col in self._CUM_COLS:
            df[col] = out[col]
        return df


def _normalize_rt_min(raw: pd.DataFrame, baseline: SurgeBaseline) -> pd.DataFrame:
    """rt_min 原始 → 快照列（当分钟量，累加器待累计）。名称/昨收从预载补齐。

    rt_min 只带 ts_code/close/vol/amount，故 name 取预载 name_map（缺→ts_code、供 ST
    判定/报文），pre_close 取预载 pre_close（缺→NaN、涨停价/涨幅随之 NaN），
    pct_chg 由 price/pre_close 现算。amount=当分钟额（元）、volume=当分钟量（股）。
    """
    if raw is None or raw.empty:
        return pd.DataFrame(columns=_RT_MIN_SNAPSHOT_COLS)
    codes = raw["ts_code"].astype(str)
    out = pd.DataFrame(index=raw.index)
    out["ts_code"] = codes
    out["name"] = codes.map(lambda c: baseline.name_map.get(c, c))
    out["price"] = pd.to_numeric(raw.get("close"), errors="coerce")
    for col in ("open", "high", "low"):
        out[col] = pd.to_numeric(raw.get(col), errors="coerce")
    out["pre_close"] = codes.map(lambda c: baseline.pre_close.get(c, np.nan)).astype("float64")
    out["pct_chg"] = (out["price"] / out["pre_close"] - 1) * 100
    out["volume"] = pd.to_numeric(raw.get("vol"), errors="coerce")
    out["amount"] = pd.to_numeric(raw.get("amount"), errors="coerce")
    out["trade_time"] = pd.to_datetime(raw.get("trade_time"))
    return out[_RT_MIN_SNAPSHOT_COLS].reset_index(drop=True)


def _default_rt_min_fn(codes: list[str]) -> pd.DataFrame:
    """默认 rt_min 取数：一次拉全 A 股最新分钟 K（token 认证，不吃 IP 反爬）。"""
    from rquant.adapter.tushare import TushareAdapter

    return TushareAdapter().rt_min(codes, "1min")


def fetch_full_market_snapshot(
    baseline: SurgeBaseline,
    tracker: CumulativeTracker,
    *,
    rt_min_fn: Callable[[list[str]], pd.DataFrame] | None = None,
) -> pd.DataFrame:
    """拉一次**全市场**快照（rt_min 全 A 股 → 累加器累计 → 涨停价），``attrs['route']`` 标注。

    2026-07-07 换源：token 认证的 rt_min 一次拉全部 A 股当分钟 K，``CumulativeTracker``
    按分钟去重累加成当日累计额/量，组装快照。一次取数兼两用：① 检测层按 ``config.boards``
    过滤，② 原样落 ``snapshot_full.parquet`` 供全景页共享 feed。rt_min 空/失败 →
    空表 route=none（本分钟 miss，熔断退避不变）；成功 route='tushare_rt'。
    """
    fn = rt_min_fn or _default_rt_min_fn
    if not baseline.code_universe:
        logger.warning("surge 全市场快照：代码全集为空（stock_basic 预载失败），本分钟 miss")
        return _empty_snapshot()
    try:
        raw = fn(baseline.code_universe)
    except Exception as e:
        logger.warning(f"surge 全市场快照 rt_min 失败: {type(e).__name__}: {e}")
        return _empty_snapshot()
    if raw is None or raw.empty:
        logger.warning("surge 全市场快照 rt_min 返回空")
        return _empty_snapshot()
    from rquant.panorama_data import add_limit_prices

    normalized = _normalize_rt_min(raw, baseline)
    cumulative = tracker.update(normalized)
    out = add_limit_prices(cumulative)
    out.attrs["route"] = "tushare_rt"
    return out


def _empty_snapshot() -> pd.DataFrame:
    empty = pd.DataFrame()
    empty.attrs["route"] = "none"
    return empty


def _detection_domain(snapshot: pd.DataFrame, boards: tuple[str, ...]) -> pd.DataFrame:
    """从全市场快照收窄到检测板块（``config.boards``，按 ts_code 前缀，复用 _classify_board）。

    只收窄检测范围（行为与旧「只拉创业/科创」一致）；ST 排除仍留在 _rough_candidates。
    snapshot_full 落盘用的是过滤前的全市场（含主板/ST 行）。空表或缺 ts_code 列原样返回。
    """
    if snapshot.empty or "ts_code" not in snapshot.columns:
        return snapshot
    board_set = set(boards)
    mask = snapshot["ts_code"].astype(str).map(_classify_board).isin(board_set)
    return snapshot[mask].reset_index(drop=True)


# ── 确认层：3 日同刻基线（stk_mins） ────────────────────────────────────────────


@dataclass
class ThreeDayBaseline:
    """某票近 3 交易日的同刻累计额 / 同分钟增量中位（对齐 241 网格）。"""

    cum_median: np.ndarray  # 241 点：3 日同刻累计额中位（元）
    minute_median: np.ndarray  # 241 点：3 日同分钟增量中位（元）
    days_used: int


_EMPTY_BASELINE = ThreeDayBaseline(
    cum_median=np.zeros(CURVE_POINTS),
    minute_median=np.zeros(CURVE_POINTS),
    days_used=0,
)


class IntradayPriceStrength(BaseModel):
    """截至某分钟、仅由该分钟及之前 K 线计算的价格与 tick-rule 近似订单流。"""

    minute_index: int
    price: float
    return_1m_pct: float | None
    inner_volume: float
    outer_volume: float

    @property
    def outer_inner_ratio(self) -> float | None:
        if self.inner_volume <= 0:
            return None
        return self.outer_volume / self.inner_volume


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


# ── 确认层今日累计：rt_min_daily 精确 cumsum（2026-07-07 换源，替代快照近似累计） ──


def today_cum_series_from_rt_min_daily(bars: pd.DataFrame) -> np.ndarray:
    """rt_min_daily 当日全序列（当分钟量）→ 241 网格当日累计额序列（cumsum，元）。

    每根当分钟 amount 按 trade_time 落 241 网格 cumsum；已填格之间的空档前向填充，
    首根之前 / 末根之后留 NaN——超出 rt_min_daily 覆盖时刻（未来分钟）取 NaN，
    确认层据此退回累加器近似（不会把陈旧累计当作现值）。空/缺字段 → 全 NaN。
    """
    arr = np.full(CURVE_POINTS, np.nan)
    if bars is None or bars.empty:
        return arr
    if "trade_time" not in bars.columns or "amount" not in bars.columns:
        return arr
    df = bars.copy()
    df["trade_time"] = pd.to_datetime(df["trade_time"])
    df = df.sort_values("trade_time")
    df["cumamt"] = pd.to_numeric(df["amount"], errors="coerce").cumsum()
    for r in df.itertuples():
        if pd.isna(r.cumamt):
            continue
        arr[grid_index(pd.Timestamp(r.trade_time).time())] = float(r.cumamt)  # 同格取分钟末
    filled = np.where(~np.isnan(arr))[0]
    if filled.size == 0:
        return arr
    last = np.nan
    for i in range(int(filled[0]), int(filled[-1]) + 1):  # 仅覆盖区间内前向填充
        if not np.isnan(arr[i]):
            last = arr[i]
        else:
            arr[i] = last
    return arr


def today_price_strength_from_rt_min_daily(
    bars: pd.DataFrame,
    gi: int,
) -> IntradayPriceStrength | None:
    """用不晚于 ``gi`` 的分钟 K 计算当前涨速与外/内盘近似，不读取未来分钟。"""
    required = {"trade_time", "open", "close", "vol"}
    if bars is None or bars.empty or not required.issubset(bars.columns):
        return None
    df = bars.copy()
    df["trade_time"] = pd.to_datetime(df["trade_time"], errors="coerce")
    df["open"] = pd.to_numeric(df["open"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["vol"] = pd.to_numeric(df["vol"], errors="coerce").fillna(0.0)
    df = df.dropna(subset=["trade_time", "close"])
    if df.empty:
        return None
    df["_gi"] = df["trade_time"].map(lambda v: grid_index(pd.Timestamp(v).time()))
    df = df[df["_gi"] <= gi].sort_values("trade_time")
    if df.empty:
        return None
    # 同一分钟若接口返回多根，以最后一根为分钟末状态。
    df = df.groupby("_gi", sort=True, as_index=False).tail(1).sort_values("_gi")
    latest = df.iloc[-1]
    latest_gi = int(latest["_gi"])
    previous_close: float | None = None
    inner = 0.0
    outer = 0.0
    for row in df.itertuples():
        close = float(row.close)
        reference = previous_close
        if reference is None:
            reference = float(row.open) if pd.notna(row.open) else close
        volume = max(float(row.vol), 0.0)
        if close > reference:
            outer += volume
        elif close < reference:
            inner += volume
        else:
            inner += volume / 2
            outer += volume / 2
        previous_close = close

    return_1m: float | None = None
    if len(df) >= 2:
        prior = float(df.iloc[-2]["close"])
        if prior > 0:
            return_1m = (float(latest["close"]) / prior - 1) * 100
    return IntradayPriceStrength(
        minute_index=latest_gi,
        price=float(latest["close"]),
        return_1m_pct=return_1m,
        inner_volume=inner,
        outer_volume=outer,
    )


def _default_today_cum_fetcher(ts_code: str, today: date) -> pd.DataFrame:
    """默认今日累计取数：rt_min_daily 单只当日开盘以来全序列（当分钟量）。"""
    from rquant.adapter.tushare import TushareAdapter

    return TushareAdapter().rt_min_daily([ts_code], "1min")


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
        today_cum_fetcher: Callable[[str, date], pd.DataFrame] | None = None,
        theme_map: dict[str, str] | None = None,
        push_history: SurgePushHistory | None = None,
    ) -> None:
        self.baseline = baseline
        self.config = config or SurgeConfig()
        # 默认取数器在 run 注入；tick 单测必须显式传 fetcher（不碰网络）
        self._minute_fetcher = minute_fetcher or _default_minute_fetcher
        # 今日累计精确取数（rt_min_daily）；None → 确认层今日累计退回快照累加器近似值。
        # 刻意 opt-in：直接驱动 tick 的单测/simulate 不注入即走累加器近似（离线、不碰网络），
        # 只有 run（真实盘中）注入 rt_min_daily 网络取数器取精确当日累计。
        self._today_cum_fetcher = today_cum_fetcher
        self.theme_map = theme_map if theme_map is not None else baseline.theme

        self.pushed_today: set[str] = push_history.pushed_today if push_history else set()
        self._push_dates_5d: dict[str, set[date]] = {
            code: set(push_dates)
            for code, push_dates in (
                push_history.push_dates_by_code.items() if push_history else ()
            )
        }
        self.confirm_cache: dict[str, ThreeDayBaseline] = {}
        # 今日累计精确序列缓存；价格方向未覆盖当前格时允许下一分钟刷新。
        self.today_cum_series: dict[str, np.ndarray] = {}
        self.today_price_strength: dict[str, IntradayPriceStrength] = {}
        self._pending_fetch: deque[str] = deque()
        self._queued: set[str] = set()
        self._fetch_fail: dict[str, int] = {}
        self._pending_push: list[SurgeConfirmed] = []
        # 保留字段（2026-07-08 起 unbuyable 也进 _pending_push 推送，本队列恒空，留作扩展）
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
                self._fetch_today_cum(code, now.date(), gi)
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
                bars, now.date(), self.config.cum_lookback_days
            )
            self._fetch_today_cum(code, now.date(), gi)
            self._evaluate(code, snapshot, now, gi)
        for code in requeue:
            if code not in self.pushed_today:
                self._queued.add(code)
                self._pending_fetch.append(code)

    def _fetch_today_cum(self, code: str, today: date, required_gi: int) -> None:
        """拉 rt_min_daily 精确累计与价格方向；已覆盖当前格即复用缓存。

        opt-in：未注入 today_cum_fetcher（tick 单测/simulate）直接跳过，确认层退回快照
        累加器近似值。价格方向门开启时，缓存未覆盖当前格会在后续 tick 重试。
        """
        if self._today_cum_fetcher is None:
            return
        cached_strength = self.today_price_strength.get(code)
        if code in self.today_cum_series and (
            not self.config.require_price_strength
            or (cached_strength is not None and cached_strength.minute_index >= required_gi)
        ):
            return
        try:
            bars = self._today_cum_fetcher(code, today)
        except Exception as e:
            logger.warning(
                f"surge 今日累计 rt_min_daily 取数失败 {code}，退累加器近似: "
                f"{type(e).__name__}: {e}"
            )
            self.today_cum_series[code] = np.full(CURVE_POINTS, np.nan)
            return
        if bars is None or bars.empty:
            logger.warning(f"surge 今日累计 rt_min_daily 返回空 {code}，退累加器近似")
            self.today_cum_series[code] = np.full(CURVE_POINTS, np.nan)
            return
        self.today_cum_series[code] = today_cum_series_from_rt_min_daily(bars)
        strength = today_price_strength_from_rt_min_daily(bars, required_gi)
        if strength is not None:
            self.today_price_strength[code] = strength

    def _evaluate(self, code: str, snapshot: pd.DataFrame, now: datetime, gi: int) -> None:
        """确认判定（口径 v4）：v3 累计比值门叠加当前红盘、1 分钟上涨和外盘占优。"""
        if code in self.pushed_today:
            return
        # skip_first_minutes：9:30 开盘首格（gi=0）恒不确认，再额外跳 skip_first_minutes
        # 分钟。默认 1 → gi≤1（9:30/9:31）不确认，9:32 起才评估（base 分母噪声大）。
        if gi <= self.config.skip_first_minutes:
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
        pct_chg = row.get("pct_chg")
        if pct_chg is None or pd.isna(pct_chg) or float(pct_chg) <= 0:
            return

        price_source = "snapshot"
        return_1m: float | None = None
        outer_inner_ratio: float | None = None
        strength = self.today_price_strength.get(code)
        if self._today_cum_fetcher is not None and self.config.require_price_strength:
            # 付费实时分钟源存在时必须精确覆盖当前格；滞后或缺字段就等下一分钟重取。
            if strength is None or strength.minute_index != gi:
                return
            if (
                strength.return_1m_pct is None
                or strength.return_1m_pct <= self.config.min_return_1m_pct
                or strength.outer_volume
                <= self.config.min_outer_inner_ratio * strength.inner_volume
            ):
                return
            price = strength.price
            return_1m = strength.return_1m_pct
            outer_inner_ratio = strength.outer_inner_ratio
            price_source = "tushare_rt_daily"
            pre_close = row.get("pre_close")
            if pre_close is None or pd.isna(pre_close):
                pre_close = self.baseline.pre_close.get(code)
            if pre_close is not None and not pd.isna(pre_close) and float(pre_close) > 0:
                pct_chg = (float(price) / float(pre_close) - 1) * 100
                if pct_chg <= 0:
                    return
        base_cum = float(base.cum_median[gi])
        if base_cum <= 0:
            return
        # 今日累计（分子）：优先 rt_min_daily 精确 cumsum（2026-07-07 换源），该刻覆盖到位
        # 才用；未注入 / 取数失败 / 超出覆盖时刻（future 分钟 NaN）→ 退快照累加器近似值。
        today_cum = float(amount)
        series = self.today_cum_series.get(code)
        if series is not None and not np.isnan(series[gi]):
            today_cum = float(series[gi])
        rel = today_cum / base_cum
        # 纯累计单条门：比值须落在 [k_cum, ratio_cap]。低于下门量能不足；高于上门视为
        # 极端出货毒尾（2026-07-06 回测 11-20× 扎堆负收益组）→ 不推。
        if rel < self.config.k_cum or rel > self.config.ratio_cap:
            return
        # VWAP 门（v3 默认关）：require_vwap 时才要求现价 ≥ 当日均价（amount/volume）。
        if self.config.require_vwap:
            if volume is None or pd.isna(volume) or float(volume) <= 0:
                return
            vwap = float(amount) / float(volume)
            if float(price) < vwap:
                return

        arr = self.cum_series.get(code)
        minute_delta = _minute_delta(arr, gi) if arr is not None else None
        # 单分钟增量门（v3 默认关）：>0 时要求当分钟增量 ≥ k_delta × N日同分钟中位
        # （中位≤0 / 增量缺失 → 不过，None-fail 语义）。
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
            price=round(float(price), 2),  # 推送当时价 = 入场价
            pct_chg=round(float(pct_chg), 2),
            cum_amount=round(today_cum, 0),  # 决策所用今日累计（rt_min_daily 精确 or 累加器近似）
            rel_cum=round(rel, 2),
            rough_ratio=round(rough_ratio, 2),
            minute_delta=round(minute_delta, 0) if minute_delta is not None else None,
            minute_delta_median=round(float(base.minute_median[gi]), 0),
            room_to_limit_pct=round(room, 2) if room is not None else None,
            return_1m_pct=round(return_1m, 3) if return_1m is not None else None,
            outer_inner_ratio_approx=(
                round(outer_inner_ratio, 3) if outer_inner_ratio is not None else None
            ),
            price_source=price_source,
        )
        # 可买性守卫：现价距涨停 ≤ 门（或已封板 room≤0）→ 买不进，标 unbuyable，但**仍推送**
        # （报文标「临近涨停」icon 让用户自行判断），不再吞掉——2026-07-07 回测证明最强的爆量
        # 往往就是这批秒板票。room 未知（缺涨停价）→ 按可买（confirmed）放行（fail-open）。
        if room is not None and room <= self.config.max_room_to_limit_pct:
            confirmed.status = "unbuyable"
        push_dates = self._push_dates_5d.setdefault(code, set())
        push_dates.add(now.date())
        confirmed.push_count_5d = len(push_dates)
        self.pushed_today.add(code)  # 每票每日仅推一次（入待推送即定，含 unbuyable）
        self._pending_push.append(confirmed)

    def _flush(self, now: datetime) -> TickResult:
        """静默窗后聚合本分钟待推送为报文（单条 ≤N 只，超出折叠）。

        pushes 含可买 + unbuyable（2026-07-08 起 unbuyable 也推、报文标「临近涨停」）；
        confirmed（落 events）= push_batch + event_batch（后者恒空，保留结构）。
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
    n = config.cum_lookback_days
    n_unbuyable = sum(1 for c in confirmed if c.status == "unbuyable")
    for c in shown:
        # 临近涨停/封板标记：买不进的确认票也推,加 icon 让用户自行判断（2026-07-08 起）。
        flag = ""
        if c.status == "unbuyable":
            sealed = c.room_to_limit_pct is not None and c.room_to_limit_pct <= 0
            flag = "🔒已封板 " if sealed else "🔔临近涨停 "
        lines.append(f"## {flag}{c.ts_code} {c.name}")
        if c.theme:
            lines.append(f"**题材**：{c.theme}")

        price_parts = [f"涨幅：+{c.pct_chg:.1f}%"]
        if c.room_to_limit_pct is not None:
            price_parts.append(f"距涨停：{c.room_to_limit_pct:.1f}%")
        lines.append(f"- {' ｜ '.join(price_parts)}")
        lines.append(f"- 累计比：{n}日 {c.rel_cum:.1f}× ｜ 累计额：{_fmt_amount(c.cum_amount)}")
        lines.append(f"- 近5日推送次数：{c.push_count_5d}")

        # 增量段仅在增量门开启时展示（v3 默认关，纯累计口径不看单分钟）。
        if config.k_delta_confirm > 0:
            lines.append(
                f"- 增量：本分钟 {_fmt_amount(c.minute_delta)}"
                f" ｜ {n}日中位 {_fmt_amount(c.minute_delta_median)}"
            )
        if c.return_1m_pct is not None:
            flow = (
                f"外/内≈{c.outer_inner_ratio_approx:.2f}×"
                if c.outer_inner_ratio_approx is not None
                else "外盘占优"
            )
            lines.append(f"- 方向：1分钟 {c.return_1m_pct:+.2f}% ｜ {flow}")
        lines.append("")
    if extra > 0:
        lines.append(f"- 另有 {extra} 只（本分钟共 {len(confirmed)} 只确认）")
    if n_unbuyable > 0:
        lines.append(f"> 🔔临近涨停/🔒已封板 {n_unbuyable} 只：现价贴近涨停，买入难度大，自行判断")
    delta_seg = (
        f" / 增量门{config.k_delta_confirm:g}×{n}d同分钟" if config.k_delta_confirm > 0 else ""
    )
    vwap_seg = " / VWAP门" if config.require_vwap else ""
    direction_seg = (
        f" / 1分钟>{config.min_return_1m_pct:g}%且外/内≈>{config.min_outer_inner_ratio:g}"
        if config.require_price_strength
        else ""
    )
    first_gi = min(config.skip_first_minutes + 1, CURVE_POINTS - 1)
    first_m = _GRID_MINUTES[first_gi]
    lines.append(
        f"> 口径 v4(累计+方向): rough{config.k_rough:g}×20d·curve / "
        f"累计比值∈[{config.k_cum:g},{config.ratio_cap:g}]×{n}d同刻"
        f" / {first_m // 60}:{first_m % 60:02d}起判{delta_seg}{vwap_seg}{direction_seg}"
    )
    lines.append("> ⚠️ 观察提示，非买入信号")
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


RUNTIME_CONFIG_NAME = "runtime_config.json"


def write_runtime_config(live_dir: Path, config: SurgeConfig, day: date) -> None:
    """启动时落生效口径（原子写），供全景页动态展示检测范围。失败只 log。"""
    payload = {
        "day": day.isoformat(),
        "boards": list(config.boards),
        "k_rough": config.k_rough,
        "k_cum": config.k_cum,
        "ratio_cap": config.ratio_cap,
        "skip_first_minutes": config.skip_first_minutes,
        "tushare_rate_per_min": config.tushare_rate_per_min,
        "require_price_strength": config.require_price_strength,
        "max_room_to_limit_pct": config.max_room_to_limit_pct,
    }
    try:
        live_dir.mkdir(parents=True, exist_ok=True)
        tmp = live_dir / (RUNTIME_CONFIG_NAME + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, live_dir / RUNTIME_CONFIG_NAME)
    except Exception as e:
        logger.warning(f"runtime_config 落盘失败（不影响主循环）: {type(e).__name__}: {e}")


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


def load_recent_push_history(
    live_dir: Path,
    as_of: date,
    trading_days: tuple[date, ...] | list[date],
) -> SurgePushHistory:
    """从精确交易日窗口的 events JSONL 恢复逐票出现日期；坏行 fail-soft。"""
    window_dates = tuple(sorted({day for day in trading_days if day <= as_of})[-5:])
    if as_of not in window_dates:
        window_dates = tuple(sorted({*window_dates, as_of})[-5:])

    push_dates_by_code: dict[str, set[date]] = {}
    invalid_lines = 0
    for trading_day in window_dates:
        path = live_dir / f"events-{trading_day.isoformat()}.jsonl"
        if not path.is_file():
            continue
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError as e:
            logger.warning(f"surge 推送历史读取失败 {path.name}: {type(e).__name__}: {e}")
            continue
        for line in lines:
            if not line.strip():
                continue
            try:
                record = json.loads(line)
                raw_code = record["ts_code"]
                if not isinstance(raw_code, str) or not raw_code.strip():
                    raise ValueError("invalid ts_code")
                code = raw_code.strip()
            except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                invalid_lines += 1
                continue
            push_dates_by_code.setdefault(code, set()).add(trading_day)
    if invalid_lines:
        logger.warning(f"surge 推送历史跳过坏行 {invalid_lines} 条")
    return SurgePushHistory(
        as_of=as_of,
        window_dates=window_dates,
        push_dates_by_code={
            code: frozenset(push_dates) for code, push_dates in push_dates_by_code.items()
        },
    )


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


def _publish_legacy_shadow_exports(
    *,
    trade_date: date,
    events_path: Path,
    collection_proof: LegacySurgeCollectionProof,
) -> tuple[Path, dict[str, Path]]:
    """Publish closed old surge evidence and read-only isolated runner fan-in."""

    from rquant.config import settings
    from rquant.legacy_shadow_export import (
        fan_in_production_isolated_runner_exports,
        publish_legacy_surge_production_export,
    )

    surge_export = publish_legacy_surge_production_export(
        data_dir=settings.data_dir,
        trade_date=trade_date,
        events_path=events_path,
        collection_proof=collection_proof,
    )
    isolated_exports = dict(
        fan_in_production_isolated_runner_exports(
            data_dir=settings.data_dir,
            trade_date=trade_date,
        )
    )
    return surge_export, isolated_exports


def _recover_legacy_shadow_exports(trade_date: date) -> None:
    from rquant.config import settings
    from rquant.legacy_shadow_export import recover_production_legacy_shadow_exports

    recover_production_legacy_shadow_exports(
        data_dir=settings.data_dir,
        trade_date=trade_date,
        source="surge",
    )
    recover_production_legacy_shadow_exports(
        data_dir=settings.data_dir,
        trade_date=trade_date,
        source="isolated-runners",
    )


def run_surge_watch(
    *,
    dry_run: bool = False,
    force_session: bool = False,
    config: SurgeConfig | None = None,
    base_dir: Path | None = None,
    now_fn: Callable[[], datetime] = _now_cst,
    sleep_fn: Callable[[float], None] = _time_module.sleep,
    snapshot_fetcher: Callable[[], pd.DataFrame] | None = None,
    minute_fetcher: Callable[[str, date], pd.DataFrame] | None = None,
    today_cum_fetcher: Callable[[str, date], pd.DataFrame] | None = None,
    notify_fn: Callable[..., None] | None = None,
    is_trading_day_fn: Callable[[date], bool] | None = None,
    recent_trading_days_fn: Callable[[date], tuple[date, ...] | list[date]] | None = None,
    baseline: SurgeBaseline | None = None,
    max_ticks: int | None = None,
) -> int:
    """常驻主循环：守卫 → 每分钟拉**全市场**快照 → 落 snapshot_full → 检测层过滤 → tick
    → 推送/落盘 → 15:02 退出。

    一次全市场取数兼作两用：原样落 ``snapshot_full.parquet``（全景页共享 feed），并按
    ``config.boards`` 收窄成检测输入喂 tick。``snapshot_fetcher`` 默认基于 tushare rt_min
    的全市场取数器（含累加器 + 重启 seed），返回空 = 本分钟 miss；``today_cum_fetcher``
    默认 rt_min_daily 精确今日累计取数器（确认层用）。时钟/源/推送/sleep 全可注入。
    非交易日即退；午休 sleep；快照连续 5 miss 推一条降级告警（每日至多一条）并退避
    60/120/300。``force_session`` 忽略时段守卫（盘后验收）；``max_ticks`` 限定循环次数。
    """
    config = config or SurgeConfig()
    started_at = now_fn()
    day = started_at.date()
    trading_check = is_trading_day_fn or _load_is_trading_day
    if not force_session and not trading_check(day):
        logger.info(f"{day} 非交易日，surge-watch 退出")
        return 0
    if not force_session and started_at.time() >= EXIT_TIME:
        try:
            _recover_legacy_shadow_exports(day)
        except Exception:
            logger.warning("legacy shadow surge recovery unavailable; shadow will degrade")
        return 0

    baseline = baseline or preload_baseline()
    notify_fn = notify_fn or _default_notify
    live_dir = base_dir or default_live_dir()
    trading_days_loader = recent_trading_days_fn or _load_recent_trading_days
    try:
        recent_trading_days = trading_days_loader(day)
    except Exception as e:
        logger.warning(f"近5交易日窗口加载失败，退化为仅今日: {type(e).__name__}: {e}")
        recent_trading_days = (day,)
    push_history = load_recent_push_history(live_dir, day, recent_trading_days)

    # 默认全市场快照 = tushare rt_min + 累加器（token 认证不吃 IP 反爬，2026-07-07 换源）。
    # 累加器 seed：上一份 snapshot_full.parquet 若为当日则续算（重启只丢 tick 间隙）。
    # 仅当 snapshot_fetcher 未注入（真实盘中）时才启用 rt_min + rt_min_daily 网络默认；
    # 注入 fake snapshot_fetcher 的单测同时不启用 today_cum_fetcher 网络默认（离线兜底累加器）。
    use_real_sources = snapshot_fetcher is None
    if use_real_sources:
        tracker = CumulativeTracker(session_date=day)
        prev_path = live_dir / SNAPSHOT_FULL_NAME
        if prev_path.exists():
            try:
                seeded = tracker.seed(pd.read_parquet(prev_path), day)
                if seeded:
                    logger.info(f"surge 累加器 seed {seeded} 只（重启续算当日 snapshot_full）")
            except Exception as e:
                logger.warning(f"surge 累加器 seed 失败（从零开始）: {type(e).__name__}: {e}")

        def _rt_min_snapshot() -> pd.DataFrame:
            return fetch_full_market_snapshot(baseline, tracker)

        snapshot_fetcher = _rt_min_snapshot
    if today_cum_fetcher is None and use_real_sources:
        today_cum_fetcher = _default_today_cum_fetcher

    watcher = SurgeWatcher(
        baseline,
        config=config,
        minute_fetcher=minute_fetcher,
        today_cum_fetcher=today_cum_fetcher,
        push_history=push_history,
    )
    events_path = live_dir / f"events-{day.isoformat()}.jsonl"

    from rquant.pulse_watch import PulseSession  # 函数级导入：pulse_watch 顶层引本模块，避免环

    write_runtime_config(live_dir, config, day)
    pulse_session = PulseSession(live_dir, day, notify_fn=notify_fn, dry_run=dry_run)

    miss_streak = 0
    degraded_alerted = False
    ticks = 0
    collection_tracker = SurgeCollectionTracker(
        trade_date=day,
        started_at=started_at,
        market_universe=frozenset(baseline.code_universe),
        minimum_market_coverage_count=_MIN_FULL_MARKET_COVERAGE_COUNT,
    )
    natural_close = False

    logger.info(
        f"surge-watch 启动 day={day} 检测板块={config.boards} "
        f"k_rough={config.k_rough} k_cum={config.k_cum} ratio_cap={config.ratio_cap} "
        f"skip_first_minutes={config.skip_first_minutes} dry_run={dry_run} "
        f"history_days={len(push_history.window_dates)} "
        f"pushed_today={len(push_history.pushed_today)}"
    )
    try:
        while True:
            now = now_fn()
            if max_ticks is not None and ticks >= max_ticks:
                break
            if not force_session and now.time() >= EXIT_TIME:
                natural_close = True
                break
            if not force_session and _is_lunch(now.time()):
                sleep_fn(30.0)
                continue

            try:
                full = snapshot_fetcher()
            except Exception as exc:
                logger.warning(f"surge 快照 provider 失败（{type(exc).__name__}: {exc}）")
                full = None
            route = full.attrs.get("route", "none") if full is not None else "none"
            collection_tracker.observe_snapshot(now, full)
            if full is None or full.empty:
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
            assert full is not None
            # 全市场快照每分钟原子落盘（共享 feed：云端/Mac 全景页 poller 读它，与主循环同拍）
            atomic_write_parquet(full, live_dir / SNAPSHOT_FULL_NAME)
            if now.time() >= OPEN_TIME:  # 集合竞价快照不喂脉搏（09:25-09:30 无成交分钟含义）
                pulse_session.on_snapshot(full, now)
            # 检测层收窄到 config.boards（行为与旧「只拉创业/科创」一致）
            detection = _detection_domain(full, config.boards)
            result = watcher.tick(detection, now)
            atomic_write_parquet(detection, live_dir / "snapshot.parquet")

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
    collection_proof = collection_tracker.complete_proof() if natural_close else None
    if collection_proof is not None:
        try:
            surge_export, isolated_exports = _publish_legacy_shadow_exports(
                trade_date=day,
                events_path=events_path,
                collection_proof=collection_proof,
            )
            logger.info(
                f"legacy shadow surge export: {surge_export}; "
                f"isolated strategies={sorted(isolated_exports)}"
            )
        except Exception:
            logger.exception("legacy shadow close exports unavailable; shadow will degrade")
    elif natural_close:
        logger.warning("surge collection coverage incomplete; legacy shadow will degrade")
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
    live_dir = base_dir or default_live_dir()
    reserved = {"confirm_bars.parquet"}
    files = sorted(
        p
        for p in sim_dir.glob("*.parquet")
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
        chunk = stem[i : i + 10]
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


def _load_recent_trading_days(day: date) -> tuple[date, ...]:
    """从只读副本取截至今日最近五个 SSE 交易日；盘中不写主库。"""
    from rquant.storage.duckdb import open_readonly_store

    store = open_readonly_store(required_tables=("trade_calendar",))
    try:
        rows = store.list_trade_calendar("SSE", day - timedelta(days=31), day)
    finally:
        store.close()
    open_days = tuple(row.cal_date for row in rows if row.is_open and row.cal_date <= day)
    if day not in open_days:
        raise ValueError(f"trade_calendar 未把 {day} 标记为交易日")
    return open_days[-5:]


def _default_notify(scene: str, **kwargs) -> None:
    from rquant.notify import notify

    notify(scene, **kwargs)  # type: ignore[arg-type]
