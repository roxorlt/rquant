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


def load_alert_rules(
    serving_root: str | Path | None = None,
    commands_dir: str | Path | None = None,
) -> list[Rule] | None:
    """Read rules from ``rquant-web-commands/alert_rules/rules.jsonl`` first, then Serving."""
    from rquant.research.snapshots import web_commands_dir
    from rquant.web.commands_service import read_alert_rules

    by_id: dict[str, dict[str, object]] = {}
    try:
        from rquant.dashboard.serving_only_page_data import ServingFrameState, query_serving_frame
        from rquant.serving_paths import serving_root_from_env

        result = query_serving_frame(
            serving_root or serving_root_from_env(),
            "SELECT rule_id, enabled, pools, levels, cooldown_minutes FROM alert_rule "
            "ORDER BY rule_id LIMIT 200",
            stale_after=timedelta(days=3650),
        )
        if result.state is not ServingFrameState.UNAVAILABLE:
            keys = ("rule_id", "enabled", "pools", "levels", "cooldown_minutes")
            for row in result.rows:
                rec = dict(zip(keys, row, strict=True))
                by_id[str(rec["rule_id"])] = rec
    except Exception as exc:  # noqa: BLE001 - optional input
        logger.info(f"Serving 告警规则不可用：{type(exc).__name__}: {exc}")

    try:
        cmd_path = Path(commands_dir) if commands_dir else web_commands_dir()
        for rid, item in read_alert_rules(cmd_path).items():
            by_id[rid] = {
                "rule_id": item.rule_id,
                "enabled": item.enabled,
                "pools": ",".join(item.pools),
                "levels": ",".join(item.levels),
                "cooldown_minutes": item.cooldown_minutes,
            }
    except Exception as exc:  # noqa: BLE001 - optional input
        logger.info(f"命令日志告警规则不可用：{type(exc).__name__}: {exc}")

    if not by_id:
        return None
    return rules_from_rows([by_id[k] for k in sorted(by_id)])
