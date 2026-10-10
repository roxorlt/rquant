"""Alert rules gate for the monitor (roadmap module 11).

Rules come from Serving ``alert_rule`` (written by page control's
``save_alert_rule``). No rules published = every event notifies (the old
behaviour). Once any rule exists, an event notifies only if an *enabled* rule
matches its pool and level and that rule's cooldown for the code has elapsed.
Events are still stored either way; the gate only decides the push.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

from loguru import logger


@dataclass(frozen=True)
class Rule:
    rule_id: str
    enabled: bool
    pools: frozenset[str]
    levels: frozenset[str]
    cooldown: timedelta

    def matches(self, pool: str, level: str) -> bool:
        return (self.enabled and (not self.pools or pool in self.pools)
                and (not self.levels or level in self.levels))


def _split(value: object) -> frozenset[str]:
    return frozenset(p for p in str(value or "").split(",") if p)


def rules_from_rows(rows: list[dict[str, object]]) -> list[Rule]:
    return [Rule(rule_id=str(r["rule_id"]), enabled=bool(r["enabled"]),
                 pools=_split(r.get("pools")), levels=_split(r.get("levels")),
                 cooldown=timedelta(minutes=int(r.get("cooldown_minutes") or 0)))
            for r in rows]


@dataclass
class AlertGate:
    rules: list[Rule] | None
    _last: dict[tuple[str, str], datetime] = field(default_factory=dict)

    def allow(self, pool: str, level: str, code: str, now: datetime) -> bool:
        if not self.rules:
            return True
        for rule in self.rules:
            if not rule.matches(pool, level):
                continue
            last = self._last.get((rule.rule_id, code))
            if last is not None and now - last < rule.cooldown:
                continue
            self._last[(rule.rule_id, code)] = now
            return True
        return False


def load_alert_rules(serving_root: str | Path | None = None) -> list[Rule] | None:
    """None when the projection is not published (→ gate allows everything)."""
    from rquant.dashboard.serving_only_page_data import ServingFrameState, query_serving_frame
    from rquant.serving_paths import serving_root_from_env

    try:
        result = query_serving_frame(
            serving_root or serving_root_from_env(),
            "SELECT rule_id, enabled, pools, levels, cooldown_minutes FROM alert_rule "
            "ORDER BY rule_id LIMIT 200",
            stale_after=timedelta(days=3650),
        )
    except Exception as exc:  # noqa: BLE001 - optional input
        logger.info(f"告警规则不可用：{type(exc).__name__}: {exc}")
        return None
    if result.state is ServingFrameState.UNAVAILABLE:
        return None
    keys = ("rule_id", "enabled", "pools", "levels", "cooldown_minutes")
    return rules_from_rows([dict(zip(keys, row, strict=True)) for row in result.rows])
