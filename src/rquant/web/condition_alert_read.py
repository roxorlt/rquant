"Read only actual owned heads and scope versions from one pinned generation."

from __future__ import annotations

from datetime import datetime

from duckdb import Error as DuckDBError

from rquant.alert_rule_contracts import (
    ConditionAlertMarketScope,
    ConditionAlertPoolScope,
    ConditionAlertRuleDefinition,
    ConditionAlertSectorScope,
    ConditionAlertWatchlistScope,
)
from rquant.condition_alert_rule_store import ConditionAlertRuleEntry
from rquant.condition_alert_runtime_projection import (
    condition_table_rows,
    read_condition_rule_authority,
    resolve_condition_scope,
)
from rquant.runtime_contracts import canonical_sha256
from rquant.serving_manual_watchlist_projection import ManualWatchlistProjectionRow
from rquant.web.models.condition_alert_rules import (
    ConditionAlertRuleItem,
    ConditionAlertScopeOption,
)
from rquant.web.serving import BorrowedGeneration


def owned_condition_heads(
    borrowed: BorrowedGeneration, *, owner_id: str, now: datetime
) -> tuple[ConditionAlertRuleEntry, ...] | None:
    actual = read_condition_rule_authority(borrowed, now=now)
    if actual is None:
        return None
    return tuple(row for row in actual.rows if row.owner_id == owner_id)


def condition_rule_item(
    entry: ConditionAlertRuleEntry, borrowed: BorrowedGeneration, *, now: datetime
) -> ConditionAlertRuleItem:
    if entry.deleted or entry.rule is None:
        raise ValueError("condition tombstone cannot carry live editor fields")
    try:
        resolve_condition_scope(borrowed, owner_id=entry.owner_id, rule=entry.rule, now=now)
        status, message = "bound", ""
    except (ValueError, RuntimeError, OSError, DuckDBError):
        status, message = "unavailable", "范围已更新或暂不可用，请重新选择。"
    item = ConditionAlertRuleItem(
        rule_id=entry.rule_id,
        version=entry.version,
        rule=entry.rule,
        updated_at=entry.updated_at,
        scope_status=status,
        scope_message=message,
    )
    try:
        from rquant.condition_alert_runtime_projection import read_condition_runtime_item

        facts = read_condition_runtime_item(borrowed, entry=entry, now=now)
        if facts is None:
            return item
        return ConditionAlertRuleItem.model_validate(
            {
                **item.model_dump(mode="python"),
                **facts.model_dump(
                    include={
                        "status_label",
                        "evaluated_at",
                        "matched_count",
                        "unknown_count",
                        "ranking_digest",
                        "last_triggered_at",
                    }
                ),
            }
        )
    except (ImportError, ValueError, RuntimeError, OSError, DuckDBError):
        return item


def condition_scope_options(
    borrowed: BorrowedGeneration, *, owner_id: str, now: datetime
) -> list[ConditionAlertScopeOption]:
    scopes: list[ConditionAlertScopeOption] = []
    sample = ConditionAlertRuleDefinition(
        rule_id="scope-read",
        name="范围",
        priority="P2",
        enabled=False,
        conditions=({"name": "not_st", "args": {}},),
        scope=ConditionAlertMarketScope(),
        governance={"channels": ("pushdeer",)},
    )
    try:
        actual = resolve_condition_scope(borrowed, owner_id=owner_id, rule=sample, now=now)
        scopes.append(
            ConditionAlertScopeOption(
                label="全市场",
                scope=sample.scope,
                available=True,
                member_count=len(actual.member_codes),
            )
        )
    except (ValueError, RuntimeError, OSError, DuckDBError):
        scopes.append(
            ConditionAlertScopeOption(
                label="全市场", scope=sample.scope, available=False, message="盘中数据暂不可用。"
            )
        )
    try:
        rows = condition_table_rows(borrowed, "manual_watchlist", now=now)
        own = tuple(
            ManualWatchlistProjectionRow.model_validate(row)
            for row in rows
            if row["owner_id"] == owner_id
        )
        membership = canonical_sha256({"owner": owner_id, "heads": own})
        scopes.append(
            ConditionAlertScopeOption(
                label="我的盯盘",
                scope=ConditionAlertWatchlistScope(membership_version=membership),
                available=True,
                member_count=sum(
                    not row.deleted and (row.expires_at is None or row.expires_at > now)
                    for row in own
                ),
            )
        )
    except (ValueError, RuntimeError, OSError, DuckDBError):
        pass
    try:
        definitions = condition_table_rows(borrowed, "pool_definition", now=now)
        receipts = condition_table_rows(borrowed, "screen_run_receipt", now=now)
        for row in definitions:
            receipt = next(
                (item for item in receipts if item["preset_name"] == row["pool_name"]), None
            )
            if row["state"] != "available" or receipt is None:
                continue
            scope = ConditionAlertPoolScope(
                pool_name=row["pool_name"],
                definition_version=row["version"],
                result_version=receipt["result_version"],
            )
            definition = sample.model_copy(update={"scope": scope})
            try:
                actual = resolve_condition_scope(
                    borrowed, owner_id=owner_id, rule=definition, now=now
                )
                scopes.append(
                    ConditionAlertScopeOption(
                        label=f"池子 · {row['display_name']}",
                        scope=scope,
                        available=True,
                        member_count=len(actual.member_codes),
                    )
                )
            except (ValueError, RuntimeError, OSError, DuckDBError):
                continue
    except (ValueError, RuntimeError, OSError, DuckDBError):
        pass
    for system, table, key, label in (
        ("industry", "stock_basic", "industry", "行业"),
        ("concept", "kpl_concept_member", "board_code", "概念"),
    ):
        try:
            rows = condition_table_rows(borrowed, table, now=now)
            for code in sorted({row[key] for row in rows if row[key]}):
                selected = tuple(row for row in rows if row[key] == code)
                version = canonical_sha256({"table": table, "components": selected})
                scope = ConditionAlertSectorScope(
                    sector_system=system, sector_code=code, component_source_version=version
                )
                scopes.append(
                    ConditionAlertScopeOption(
                        label=f"{label} · {selected[0].get('board_name') or code}",
                        scope=scope,
                        available=True,
                        member_count=len(
                            {
                                row["ts_code" if system == "industry" else "con_code"]
                                for row in selected
                            }
                        ),
                    )
                )
        except (ValueError, RuntimeError, OSError, DuckDBError):
            continue
    if len(scopes) > 1000:
        raise ValueError("condition scope catalogue exceeds its full domain capacity")
    return scopes
