from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from rquant.web.health_layers import build_layers
from rquant.web.models import ServiceItem


def _svc(sid: str, **kw) -> ServiceItem:
    base = dict(service_id=sid, plane="live", status="running", stale=False, heartbeat_at=None,
                backlog_count=0, consecutive_failures=0, last_error=None)
    return ServiceItem(**(base | kw))


def test_layers_take_the_worst_input(tmp_path) -> None:
    now = datetime(2026, 10, 12, 10, tzinfo=UTC)
    layers = {item.key: item for item in build_layers(
        [_svc("rquant-monitor", stale=True), _svc("rquant-daily")],
        {"latest_daily_bar": "2026-10-09"}, now - timedelta(hours=80), None, tmp_path, "t",
        today=date(2026, 10, 12), now=now)}
    assert [k for k in layers] == ["ingest", "storage", "serving", "signal", "research", "web"]
    assert layers["signal"].state == "crit"
    assert layers["ingest"].state == "ok"          # Friday's bar on Monday = 1 weekday behind
    assert layers["serving"].state == "crit"       # 80h old
    assert layers["storage"].state == layers["research"].state == "unknown"
