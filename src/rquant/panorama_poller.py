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

# 云端 feed（P2）：Mac 优先读云 nginx 暴露的 surge-watch 全市场快照，
# 新鲜（Last-Modified ≤120s）则本机不再自拉。env 未配 → 返回 None，行为不变。
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
    """P2 第 0 路由：GET RQUANT_CLOUD_FEED_URL（云端 surge 全市场 parquet）。

    env 未配 → None（现状零变化）。Last-Modified 距今 >120s（陈旧）/ 空 / HTTP 失败
    → None，调用方回落现有三级路由。凭据走 RQUANT_CLOUD_FEED_USER/PASS（basic auth）。
    """
    url = os.environ.get("RQUANT_CLOUD_FEED_URL", "").strip()
    if not url:
        return None
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
        now: Callable[[], float] = time.monotonic,
        snapshot_fetcher: Callable[[bool], pd.DataFrame] | None = None,
        flow_fetcher: Callable[[str], pd.DataFrame] | None = None,
        cloud_feed_fetcher: Callable[[], tuple[pd.DataFrame, str] | None] | None = None,
        drop_dir: Path | None = None,
    ) -> None:
        self._interval = interval
        self._now = now
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

    def _run(self) -> None:
        while not self._stopped:
            try:
                self.poll_once()
            except Exception as e:  # 循环永不死
                logger.warning(f"poller 轮询异常: {type(e).__name__}: {e}")
            self._wake.wait(timeout=self._interval)
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
