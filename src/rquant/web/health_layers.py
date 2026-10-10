"""Six-layer health summary (roadmap item 19) from what the API can already see.

Layers: 数据采集 → 存储与审计 → Serving → 信号与盯盘 → 研究产出 → 网页. Each is the
worst state of its inputs; nothing new is collected (no new heartbeat fields).
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from rquant.web.models import HealthLayer, ServiceItem

_RANK = {"ok": 0, "unknown": 1, "warn": 2, "crit": 3}
SIGNAL_WORDS = ("monitor", "notifier", "signal", "page-control", "surge")
INGEST_WORDS = ("daily", "ingest", "reference", "collect", "replica", "sync")


def _worst(states: list[str]) -> str:
    return max(states, key=_RANK.__getitem__) if states else "unknown"


def _service_state(s: ServiceItem) -> str:
    if s.stale or s.status in ("failed", "error") or s.consecutive_failures >= 3:
        return "crit"
    if s.consecutive_failures or s.backlog_count > 100:
        return "warn"
    return "ok"


def _weekdays_between(a: date, b: date) -> int:
    return sum(1 for i in range(1, (b - a).days + 1) if (a + timedelta(days=i)).weekday() < 5)


def build_layers(services: list[ServiceItem], freshness: dict[str, str | None],
                 generated_at: datetime | None, audit: Any, research_root: Path,
                 version: str, today: date | None = None,
                 now: datetime | None = None) -> list[HealthLayer]:
    today = today or date.today()
    now = now or datetime.now(UTC)

    def group(words: tuple[str, ...]) -> tuple[list[str], list[str]]:
        picked = [s for s in services if any(w in s.service_id for w in words)]
        return ([_service_state(s) for s in picked],
                [f"{s.service_id}：{s.status}{'（过期）' if s.stale else ''}" for s in picked])

    ingest_states, ingest_detail = group(INGEST_WORDS)
    latest = freshness.get("latest_daily_bar")
    if latest:
        lag = _weekdays_between(date.fromisoformat(latest[:10]), today)
        ingest_states.append("ok" if lag <= 1 else "warn" if lag <= 3 else "crit")
        ingest_detail.insert(0, f"日线最新 {latest[:10]}（落后 {lag} 个工作日）")

    if audit is None:
        storage = HealthLayer(key="storage", title="存储与审计", state="unknown",
                              detail=["尚无覆盖审计（python -m rquant.data_catalog.audit）"])
    else:
        gaps = [d for d in audit.datasets if d.missing_open_days]
        errors = [d for d in audit.datasets if d.error]
        storage = HealthLayer(
            key="storage", title="存储与审计",
            state="crit" if errors else "warn" if gaps else "ok",
            detail=[f"审计 {audit.window_start}~{audit.window_end}，{len(audit.datasets)} 个数据集",
                    *[f"{d.dataset_id} 缺 {len(d.missing_open_days)} 天" for d in gaps[:5]],
                    *[f"{d.dataset_id} 读取失败" for d in errors[:3]]])

    if generated_at is None:
        serving = HealthLayer(key="serving", title="Serving", state="unknown",
                              detail=["没有已发布的代"])
    else:
        age = now - generated_at.astimezone(UTC)
        hours = age.total_seconds() / 3600
        serving = HealthLayer(key="serving", title="Serving",
                              state="ok" if hours <= 24 else "warn" if hours <= 72 else "crit",
                              detail=[f"当前代发布于 {hours:.1f} 小时前"])

    signal_states, signal_detail = group(SIGNAL_WORDS)

    research_files = sorted(research_root.glob("*/*/result.json"),
                            key=lambda p: p.stat().st_mtime) if research_root.is_dir() else []
    if research_files:
        newest = datetime.fromtimestamp(research_files[-1].stat().st_mtime, UTC)
        research = HealthLayer(key="research", title="研究产出", state="ok",
                               detail=[f"{len(research_files)} 个结果文件，最新 "
                                       f"{newest.astimezone().strftime('%Y-%m-%d %H:%M')}"])
    else:
        research = HealthLayer(key="research", title="研究产出", state="unknown",
                               detail=["还没有回测/因子/条件结果文件"])

    return [
        HealthLayer(key="ingest", title="数据采集", state=_worst(ingest_states),  # type: ignore[arg-type]
                    detail=ingest_detail or ["没有识别到采集类服务"]),
        storage,
        serving,
        HealthLayer(key="signal", title="信号与盯盘", state=_worst(signal_states),  # type: ignore[arg-type]
                    detail=signal_detail or ["没有识别到盯盘/通知服务"]),
        research,
        HealthLayer(key="web", title="网页", state="ok", detail=[f"API {version} 正常响应"]),
    ]
