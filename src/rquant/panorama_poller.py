"""盘中全景页后台拉取器：渲染永不等取数（2026-07-06 盘中事故的架构修复）。

同步取数架构的致命放大器：fragment rerun 阻塞在取数链上（东财直连败 1s +
SOCKS 败 1s + sina 10-90s），交互全部排队。SourcePoller 把 快照 + 行业/概念
资金流 挪进单个 daemon 线程循环拉取，UI 只读 last-known-good slot：

- 单飞：一个线程串行拉三源（快照 → 行业流 → 概念流），多浏览器会话共享
  （UI 层 st.cache_resource 单例），消掉「会话数 × 源数」的请求放大；
- 成功才覆盖 slot，失败保留旧数据只记 last_error；
- 每源独立熔断：连败 ≥3 → 冷却 180s 内跳过；sina 兜底级特殊（连败 ≥2 →
  冷却 300s——它单次 10-90s，不允许反复吊死循环），由 allow_sina 参数控制；
- 时钟可注入（now()），单测不碰网络不长 sleep；
- **成功槽位落盘共享 drop**（``data/panorama_live/``，parquet + live_meta.json，
  tmp+rename 原子替换）：全机其他消费者（midday_briefing 等）优先读 drop 而非
  自拉——2026-07-06 下午办公网 IP 因全天多进程高频访问被东财+sina 双风控，
  全机必须只有 poller 一个取数者。落盘失败只 log，不影响轮询。
"""

from __future__ import annotations

import json
import os
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta, timezone
from datetime import time as dt_time
from pathlib import Path

import pandas as pd
from loguru import logger

from rquant.config import settings
from rquant.panorama_data import add_limit_prices, fetch_market_snapshot, fetch_sector_fund_flow

CST = timezone(timedelta(hours=8))

SNAPSHOT_KEY = "snapshot"
FLOW_TYPES = ("行业资金流", "概念资金流")
_SINA_KEY = "sina"

_DEFAULT_FAIL_THRESHOLD = 3
_DEFAULT_COOLDOWN = 180.0
_SINA_FAIL_THRESHOLD = 2
_SINA_COOLDOWN = 300.0

# 分时段轮询节奏（云端 24/7 常驻的取数卫生）：交易时段每分钟，盘外/周末 600s——非盘中
# 数据不变，没必要每分钟打源。盘中大多命中 surge 本地 feed（零请求），自拉只是兜底。
_TRADING_START = dt_time(9, 0)
_TRADING_END = dt_time(15, 10)


def is_off_hours(now: datetime) -> bool:
    """非交易时段判定（纯函数，now 注入可测）：工作日 09:00–15:10 为盘中，其余 → True。

    周末恒 True；节假日不额外判（600s 打一次无害）。边界含端点：09:00、15:10 仍算盘中。
    """
    if now.weekday() >= 5:  # 周六/周日
        return True
    return not (_TRADING_START <= now.time() <= _TRADING_END)

# 云端 feed 第 0 路由：优先读 surge-watch 全市场快照，新鲜（≤120s）则本机不自拉。
# 两种形态——云端同机读本地 parquet 文件（mtime 判新鲜），Mac P2 走 HTTP（Last-Modified
# 判新鲜）；由 RQUANT_CLOUD_FEED_URL 是否以 `/`、`file://` 开头路由。env 未配 → None，
# 行为不变。
_CLOUD_FEED_MAX_AGE = 120.0

# ── 共享 drop 布局（poller 写、midday_briefing 等只读；纯 parquet 无 DuckDB 锁） ─

LIVE_DIR_NAME = "panorama_live"
LIVE_META_FILE = "live_meta.json"


def default_live_drop_dir() -> Path:
    """默认 drop 目录（settings.data_dir 下，与 midday 落盘同根）。"""
    return settings.data_dir / LIVE_DIR_NAME


def live_frame_filename(key: str) -> str:
    """drop 文件名：快照 → snapshot.parquet；资金流 → flow_{类型}.parquet。"""
    return "snapshot.parquet" if key == SNAPSHOT_KEY else f"flow_{key}.parquet"


def read_live_meta(drop_dir: Path) -> dict[str, dict]:
    """读 live_meta.json（{源key: {as_of_iso, route, written_at}}）。缺失/损坏 → 空。"""
    path = drop_dir / LIVE_META_FILE
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else {}
    except Exception as e:
        logger.warning(f"live drop meta 读取失败 {path}: {type(e).__name__}: {e}")
        return {}


def read_live_frame(drop_dir: Path, key: str) -> pd.DataFrame | None:
    """读单个 drop parquet。缺失/损坏 → None（调用方走自拉）。"""
    path = drop_dir / live_frame_filename(key)
    if not path.exists():
        return None
    try:
        return pd.read_parquet(path)
    except Exception as e:
        logger.warning(f"live drop 读取失败 {path}: {type(e).__name__}: {e}")
        return None


def _default_snapshot_fetcher(allow_sina: bool) -> pd.DataFrame:
    """真实快照拉取 + 涨停价推算（在拉取线程做，UI 读 slot 零计算）。"""
    raw = fetch_market_snapshot(allow_sina=allow_sina)
    df = add_limit_prices(raw)
    df.attrs["route"] = raw.attrs.get("route", "none")
    return df


def _default_cloud_feed() -> tuple[pd.DataFrame, str] | None:
    """第 0 路由：读 RQUANT_CLOUD_FEED_URL 指向的 surge 全市场 ``snapshot_full.parquet``。

    env 未配 → None（现状零变化）。两种形态按 env 值路由：
    - **本地文件**（值以 ``/`` 开头或 ``file://`` 前缀）：云端部署形态——全景页与
      surge-watch 同机，直接读 surge 落盘的 parquet，mtime 判新鲜 ≤120s（陈旧/缺失
      → None 回落自拉），无网络无凭据；
    - **HTTP(S)**（P2 Mac 侧）：GET 云 nginx 暴露的 feed，Last-Modified ≤120s 判新鲜，
      凭据走 RQUANT_CLOUD_FEED_USER/PASS（basic auth）。

    命中即返回 ``(df, "cloud_feed")``；陈旧 / 空 / 失败一律 None，调用方回落现有三级路由。
    """
    url = os.environ.get("RQUANT_CLOUD_FEED_URL", "").strip()
    if not url:
        return None
    if url.startswith(("/", "file://")):
        return _local_cloud_feed(url)
    return _http_cloud_feed(url)


def _local_cloud_feed(url: str) -> tuple[pd.DataFrame, str] | None:
    """本地文件分支：读 parquet，mtime 距今 ≤120s 才算新鲜；缺失/陈旧/空/损坏 → None。"""
    path = Path(url[len("file://"):] if url.startswith("file://") else url)
    try:
        if not path.exists():
            return None
        age = time.time() - path.stat().st_mtime
        if age > _CLOUD_FEED_MAX_AGE:
            logger.warning(f"云端 feed 本地文件陈旧（{age:.0f}s>120s），回落自拉")
            return None
        df = pd.read_parquet(path)
        if df is None or df.empty:
            return None
        if "limit_up_price" not in df.columns:
            df = add_limit_prices(df)
        return df, "cloud_feed"
    except Exception as e:
        logger.warning(f"云端 feed 本地读取失败，回落自拉: {type(e).__name__}: {e}")
        return None


def _http_cloud_feed(url: str) -> tuple[pd.DataFrame, str] | None:
    """HTTP(S) 分支（P2 Mac）：GET + basic auth，Last-Modified ≤120s 判新鲜。"""
    from email.utils import parsedate_to_datetime
    from io import BytesIO

    import requests

    user = os.environ.get("RQUANT_CLOUD_FEED_USER", "").strip()
    pwd = os.environ.get("RQUANT_CLOUD_FEED_PASS", "").strip()
    auth = (user, pwd) if user else None
    try:
        session = requests.Session()
        session.trust_env = False
        resp = session.get(url, auth=auth, timeout=8.0)
        resp.raise_for_status()
        last_mod = resp.headers.get("Last-Modified")
        if last_mod:
            age = (datetime.now(UTC) - parsedate_to_datetime(last_mod)).total_seconds()
            if age > _CLOUD_FEED_MAX_AGE:
                logger.warning(f"云端 feed 陈旧（{age:.0f}s>120s），回落自拉")
                return None
        df = pd.read_parquet(BytesIO(resp.content))
        if df is None or df.empty:
            return None
        if "limit_up_price" not in df.columns:
            df = add_limit_prices(df)
        return df, "cloud_feed"
    except Exception as e:
        logger.warning(f"云端 feed 拉取失败，回落自拉: {type(e).__name__}: {e}")
        return None


def _stale_local_feed() -> tuple[pd.DataFrame, float] | None:
    """冷启动兜底专用：复用云端 feed 本地文件分支的读取逻辑，但**不设 120s 新鲜度门槛**
    ——调用方（``_restore_from_stale_feed``）自行按返回的 age 判断陈旧程度用于 UI 提示，
    而非直接拒绝。只对本地文件形态生效（env 以 ``/`` 或 ``file://`` 开头）；HTTP 变体
    不做陈旧兜底（超出本次范围）。env 未配置本地路径 / 文件缺失 / 空 / 出错 → None。
    """
    url = os.environ.get("RQUANT_CLOUD_FEED_URL", "").strip()
    if not url.startswith(("/", "file://")):
        return None
    path = Path(url[len("file://"):] if url.startswith("file://") else url)
    try:
        if not path.exists():
            return None
        # clamp 负数：跨机器时钟偏差下 mtime 可能超前本机时钟，负 age 会让
        # mono - age > mono、age_seconds 显示负数，陈旧横幅漏触发（与
        # _restore_from_drop 的 age 处理保持一致）
        age = max(0.0, time.time() - path.stat().st_mtime)
        df = pd.read_parquet(path)
        if df is None or df.empty:
            return None
        if "limit_up_price" not in df.columns:
            df = add_limit_prices(df)
        return df, age
    except Exception as e:
        logger.warning(f"冷启动兜底：陈旧 feed 读取失败: {type(e).__name__}: {e}")
        return None


@dataclass
class _SourceState:
    """单源 slot + 熔断状态（slot 由 SourcePoller._lock 保护）。"""

    df: pd.DataFrame = field(default_factory=pd.DataFrame)
    as_of: str = ""  # 最近成功的墙钟时间 HH:MM:SS（快照的它同时充当下游缓存键）
    route: str = "none"
    last_success_mono: float | None = None
    consec_fail: int = 0
    cooling_until: float = 0.0
    last_error: str = ""
    fail_threshold: int = _DEFAULT_FAIL_THRESHOLD
    cooldown: float = _DEFAULT_COOLDOWN

    def mark_success(self, df: pd.DataFrame, route: str, mono: float, wall: str) -> None:
        self.df = df
        self.as_of = wall
        self.route = route
        self.last_success_mono = mono
        self.consec_fail = 0
        self.cooling_until = 0.0
        self.last_error = ""

    def mark_failure(self, error: str, mono: float) -> None:
        self.consec_fail += 1
        self.last_error = error
        if self.consec_fail >= self.fail_threshold:
            self.cooling_until = mono + self.cooldown


class SourcePoller:
    """后台单飞拉取器：快照 + 行业/概念资金流。UI 只读 slot，绝不同步拉外部源。"""

    def __init__(
        self,
        interval: float = 60.0,
        *,
        off_hours_interval: float = 600.0,
        now: Callable[[], float] = time.monotonic,
        wall_now: Callable[[], datetime] | None = None,
        snapshot_fetcher: Callable[[bool], pd.DataFrame] | None = None,
        flow_fetcher: Callable[[str], pd.DataFrame] | None = None,
        cloud_feed_fetcher: Callable[[], tuple[pd.DataFrame, str] | None] | None = None,
        drop_dir: Path | None = None,
    ) -> None:
        self._interval = interval
        # 盘外/周末轮询间隔（云端常驻取数卫生）；判定用墙钟（wall_now，可注入）
        self._off_hours_interval = off_hours_interval
        self._now = now
        self._wall_now = wall_now or (lambda: datetime.now(CST))
        self._snapshot_fetcher = snapshot_fetcher or _default_snapshot_fetcher
        self._flow_fetcher = flow_fetcher or fetch_sector_fund_flow
        # 云端 feed 第 0 路由；默认读 env（未配 → None，行为不变）
        self._cloud_feed_fetcher = cloud_feed_fetcher or _default_cloud_feed
        self._drop_dir = drop_dir if drop_dir is not None else default_live_drop_dir()
        self._drop_meta: dict[str, dict] = {}  # 仅 poll 线程读写，无需加锁
        self._lock = threading.Lock()
        self._wake = threading.Event()
        self._stopped = False
        self._thread: threading.Thread | None = None
        self._fetching = False
        self._states: dict[str, _SourceState] = {
            SNAPSHOT_KEY: _SourceState(),
            **{ft: _SourceState() for ft in FLOW_TYPES},
        }
        # sina 是快照的兜底「级」不是独立 slot：只有熔断状态，冷却期间快照
        # 以 allow_sina=False 拉取（跳过该级）
        self._sina = _SourceState(
            fail_threshold=_SINA_FAIL_THRESHOLD, cooldown=_SINA_COOLDOWN
        )

    # ── 生命周期 ──────────────────────────────────────────────────────────────

    def start(self) -> None:
        """启动后台循环（幂等；daemon 线程随进程退出）。"""
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stopped = False
            self._thread = threading.Thread(
                target=self._run, name="panorama-poller", daemon=True
            )
            self._thread.start()

    def stop(self) -> None:
        """停止循环（测试收尾用；生产依赖 daemon 随进程退出）。"""
        self._stopped = True
        self._wake.set()

    def refresh_now(self) -> None:
        """置 wake 事件让循环立即开跑一轮，不等待结果（调用方不阻塞）。"""
        self._wake.set()

    def _current_interval(self) -> float:
        """本轮 sleep 间隔：盘中（工作日 09:00–15:10）用 interval，盘外用 off_hours_interval。"""
        return self._off_hours_interval if is_off_hours(self._wall_now()) else self._interval

    def _run(self) -> None:
        while not self._stopped:
            try:
                self.poll_once()
            except Exception as e:  # 循环永不死
                logger.warning(f"poller 轮询异常: {type(e).__name__}: {e}")
            self._wake.wait(timeout=self._current_interval())
            self._wake.clear()

    # ── 拉取循环 ──────────────────────────────────────────────────────────────

    def poll_once(self) -> None:
        """单轮：快照 → 行业流 → 概念流（各自独立熔断，失败互不影响）。"""
        with self._lock:
            self._fetching = True
        try:
            self._poll_snapshot()
            for sector_type in FLOW_TYPES:
                self._poll_flow(sector_type)
        finally:
            with self._lock:
                self._fetching = False

    def _wall_clock(self) -> str:
        return datetime.now(CST).strftime("%H:%M:%S")

    def _poll_snapshot(self) -> None:
        state = self._states[SNAPSHOT_KEY]

        # 第 0 路由：云端 feed 新鲜命中则本机不自拉（env 未配时 fetcher 返回 None）
        cloud = self._cloud_feed_fetcher()
        if cloud is not None:
            cdf, croute = cloud
            if cdf is not None and not cdf.empty:
                mono = self._now()
                with self._lock:
                    state.mark_success(cdf, croute, mono, self._wall_clock())
                self._write_drop(SNAPSHOT_KEY, cdf, croute)
                return

        mono = self._now()
        if mono < state.cooling_until:
            return
        allow_sina = mono >= self._sina.cooling_until
        error = ""
        try:
            df = self._snapshot_fetcher(allow_sina)
            route = df.attrs.get("route", "none") if df is not None else "none"
        except Exception as e:
            df, route = None, "none"
            error = f"{type(e).__name__}: {e}"

        mono = self._now()  # 取数可能耗时，熔断时间戳用取数后的时钟
        if df is not None and not df.empty:
            with self._lock:
                state.mark_success(df, route, mono, self._wall_clock())
                if route == "sina":
                    self._sina.mark_success(pd.DataFrame(), route, mono, state.as_of)
            self._write_drop(SNAPSHOT_KEY, df, route)
            return
        error = error or "三级路由全部失败（返回空）"
        with self._lock:
            state.mark_failure(error, mono)
            # 快照整体失败且本轮允许了 sina → sina 级实际被尝试且失败
            if allow_sina:
                self._sina.mark_failure(error, mono)
        logger.warning(f"poller 快照拉取失败: {error}")

        # 冷启动兜底：本进程从未成功过（slot 恒空），UI 会永远卡在「等 as_of」的
        # st.rerun 死循环——哪怕带 ⚠️ 陈旧提示的旧数据也好过白屏。仅这一次触发；
        # 一旦恢复成功 state.df 非空，后续轮次回到正常 last-known-good 语义。
        # state.df 只在 poll 线程内读写（_lock 只保护跨线程的 UI 读），此处无锁读安全。
        if state.df.empty:
            self._cold_start_fallback(state, mono)

    def _cold_start_fallback(self, state: _SourceState, mono: float) -> None:
        """从磁盘恢复陈旧快照（不刷新 drop，不影响熔断计数）：先试本进程自己落盘的
        drop，再试云端 feed 本地文件；两者皆无 → 原样保持空，交由下轮重试。
        整体 try/except 兜底：任何解析/IO 异常都绝不能打断轮询主循环。
        """
        try:
            restored = self._restore_from_drop()
            if restored is None:
                restored = self._restore_from_stale_feed()
            if restored is None:
                return
            df, route, age, wall = restored
            with self._lock:
                state.mark_success(df, route, mono - age, wall)
            logger.info(f"冷启动兜底：恢复陈旧快照 route={route} age={age:.0f}s")
        except Exception as e:
            logger.warning(f"冷启动兜底失败（不影响轮询）: {type(e).__name__}: {e}")

    def _restore_from_drop(self) -> tuple[pd.DataFrame, str, float, str] | None:
        """兜底档 1——本进程自己的落盘 drop（``read_live_frame`` + ``read_live_meta``）。

        ``written_at`` 是墙钟 ISO 秒（``datetime.now(CST).isoformat()`` 写入，带
        tzinfo），``age = now(CST) - written_at``；``as_of_iso`` 转 ``HH:MM:SS`` 作为
        恢复数据的展示墙钟。meta 缺失/字段缺失/解析失败 → 跳过（不让脏 meta 拖垮轮询）。
        """
        df = read_live_frame(self._drop_dir, SNAPSHOT_KEY)
        if df is None or df.empty:
            return None
        meta = read_live_meta(self._drop_dir).get(SNAPSHOT_KEY)
        if not meta:
            return None
        try:
            written_at = datetime.fromisoformat(meta["written_at"])
            as_of_dt = datetime.fromisoformat(meta["as_of_iso"])
            age = max(0.0, (datetime.now(CST) - written_at).total_seconds())
            wall = as_of_dt.strftime("%H:%M:%S")
        except (KeyError, ValueError, TypeError) as e:
            logger.warning(f"冷启动兜底：drop meta 解析失败，跳过: {type(e).__name__}: {e}")
            return None
        return df, "drop_stale", age, wall

    def _restore_from_stale_feed(self) -> tuple[pd.DataFrame, str, float, str] | None:
        """兜底档 2——drop 也没有时，退到云端 feed 本地文件（不设 120s 新鲜度门槛）。"""
        result = _stale_local_feed()
        if result is None:
            return None
        df, age = result
        wall = (datetime.now(CST) - timedelta(seconds=age)).strftime("%H:%M:%S")
        return df, "cloud_feed_stale", age, wall

    def _poll_flow(self, sector_type: str) -> None:
        state = self._states[sector_type]
        mono = self._now()
        if mono < state.cooling_until:
            return
        error = ""
        try:
            df = self._flow_fetcher(sector_type)
            route = df.attrs.get("route", "none") if df is not None else "none"
        except Exception as e:
            df, route = None, "none"
            error = f"{type(e).__name__}: {e}"

        mono = self._now()
        if df is not None and not df.empty:
            with self._lock:
                state.mark_success(df, route, mono, self._wall_clock())
            self._write_drop(sector_type, df, route)
            return
        error = error or "三级路由全部失败（返回空）"
        with self._lock:
            state.mark_failure(error, mono)
        logger.warning(f"poller 资金流拉取失败: {sector_type} {error}")

    def _write_drop(self, key: str, df: pd.DataFrame, route: str) -> None:
        """成功槽位落盘共享 drop（tmp+rename 原子替换）。

        失败只 log 不打断轮询（drop 是「锦上添花」的共享层，不是主路径）。
        仅 poll 线程调用：_drop_meta 无需加锁；df 写盘时已在 slot 内，视为只读。
        首次写入时合并磁盘上已有 meta——进程重启后不丢其他源的记录。
        """
        try:
            self._drop_dir.mkdir(parents=True, exist_ok=True)
            if not self._drop_meta:
                self._drop_meta = read_live_meta(self._drop_dir)
            fname = live_frame_filename(key)
            tmp = self._drop_dir / (fname + ".tmp")
            df.to_parquet(tmp, index=False)
            os.replace(tmp, self._drop_dir / fname)
            now_iso = datetime.now(CST).isoformat(timespec="seconds")
            self._drop_meta[key] = {
                "as_of_iso": now_iso,
                "route": route,
                "written_at": now_iso,
            }
            meta_tmp = self._drop_dir / (LIVE_META_FILE + ".tmp")
            meta_tmp.write_text(
                json.dumps(self._drop_meta, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            os.replace(meta_tmp, self._drop_dir / LIVE_META_FILE)
        except Exception as e:
            logger.warning(f"live drop 落盘失败（不影响轮询）: {key} {type(e).__name__}: {e}")

    # ── UI 读取口（last-known-good；调用方视为只读，不得原地修改） ────────────

    def snapshot(self) -> tuple[pd.DataFrame, str, str]:
        """(带涨停价快照, as_of "HH:MM:SS", route)。从未成功 → (空表, "", "none")。"""
        with self._lock:
            s = self._states[SNAPSHOT_KEY]
            return s.df, s.as_of, s.route

    def flow(self, sector_type: str) -> tuple[pd.DataFrame, str]:
        """(资金流表, route)。从未成功 → (空表, "none")。"""
        with self._lock:
            s = self._states[sector_type]
            return s.df, s.route

    def status(self) -> dict:
        """每源熔断/新鲜度状态 + 顶层 fetching 标记（供 UI 动态状态行）。"""
        with self._lock:
            mono = self._now()
            out: dict = {"fetching": self._fetching}
            for key, s in {**self._states, _SINA_KEY: self._sina}.items():
                out[key] = {
                    "last_success_ts": s.as_of,
                    "age_seconds": (
                        mono - s.last_success_mono
                        if s.last_success_mono is not None
                        else None
                    ),
                    "route": s.route,
                    "consec_fail": s.consec_fail,
                    "cooling_until": s.cooling_until,
                    "last_error": s.last_error,
                }
            return out
