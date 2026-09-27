"""Validate pool definitions against command evidence before read-only publication."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime

from rquant.builtin_presets import BUILTIN_PRESET_SCREENS
from rquant.llm.registry import REGISTRY_BY_NAME
from rquant.runtime_contracts import canonical_sha256


@dataclass(frozen=True)
class PoolMutation:
    command_id: str
    command_kind: str
    command_hash: str
    payload: Mapping[str, object]
    result: Mapping[str, object]


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _empty_row(
    pool_name: str,
    *,
    state: str,
    reason: str | None,
    mutation: PoolMutation | None = None,
) -> dict[str, object]:
    return {
        "pool_name": pool_name,
        "display_name": pool_name.removeprefix("user/"),
        "description": "",
        "source_kind": "user",
        "state": state,
        "reason": reason,
        "version": None,
        "command_id": None if mutation is None else mutation.command_id,
        "command_hash": None if mutation is None else mutation.command_hash,
        "depends_on": None,
        "delay_mode": "none",
        "delay_days": 0,
        "rules_json": None,
        "include_columns_json": None,
        "can_edit": False,
    }


def _builtins() -> dict[str, dict[str, object]]:
    rows: dict[str, dict[str, object]] = {}
    for name, preset in BUILTIN_PRESET_SCREENS.items():
        rules = [item.model_dump(mode="json") for item in preset.rule_calls]
        identity = {
            "name": name,
            "description": preset.description,
            "depends_on": preset.depends_on,
            "offset_days": preset.offset_days,
            "rules": rules,
            "include_columns": preset.include_columns,
        }
        rows[name] = {
            "pool_name": name,
            "display_name": preset.display_name or name,
            "description": preset.ui_description or preset.description,
            "source_kind": "builtin",
            "state": "available",
            "reason": None,
            "version": canonical_sha256({"contract": "builtin-pool/v1", **identity}),
            "command_id": None,
            "command_hash": None,
            "depends_on": preset.depends_on,
            "delay_mode": "legacy_window" if preset.offset_days else "none",
            "delay_days": preset.offset_days,
            "rules_json": _json(rules),
            "include_columns_json": _json(preset.include_columns),
            "can_edit": False,
        }
    return rows


def _expected_fields(mutation: PoolMutation) -> dict[str, object] | None:
    payload = mutation.payload
    kind = mutation.command_kind
    if kind == "save_user_pool_v2":
        return {
            "schema_version": 2,
            "name": payload["base_name"],
            "display_name": str(payload["display_name"]).strip(),
            "description": payload["description"],
            "rules": payload["rule_calls"],
            "include_columns": payload["include_columns"],
            "depends_on": payload["depends_on"],
            "delay_days": payload["delay_days"],
            "source": "page_control_v2",
        }
    if kind == "save_user_pool":
        return {
            "name": payload["base_name"],
            "description": payload["description"],
            "rules": payload["rule_calls"],
            "include_columns": payload["include_columns"],
            "source": payload["source"],
        }
    if kind == "save_nl_preset":
        return {
            "name": payload["name"],
            "description": payload["description"],
            "rules": payload["rule_calls"],
            "include_columns": payload["include_columns"],
            "source": "nl_input",
        }
    if kind == "fork_builtin_pool":
        builtin = BUILTIN_PRESET_SCREENS.get(str(payload["builtin_name"]))
        if builtin is None:
            return None
        return {
            "name": payload["target_base_name"],
            "description": (f"Fork from builtin/{payload['builtin_name']}: {builtin.description}"),
            "rules": [rule.model_dump(mode="json") for rule in builtin.rule_calls],
            "include_columns": builtin.include_columns,
            "source": "fork_from_builtin",
        }
    return None


def _rules_are_registered(rules: object) -> bool:
    if not isinstance(rules, list):
        return False
    for item in rules:
        if not isinstance(item, dict) or set(item) != {"name", "args"}:
            return False
        spec = REGISTRY_BY_NAME.get(item["name"])
        if spec is None:
            return False
        try:
            spec.args_model.model_validate(item["args"])
        except (TypeError, ValueError):
            return False
    return True


def _user_row(
    pool_name: str,
    raw: Mapping[str, object] | None,
    mutation: PoolMutation | None,
    *,
    file_present: bool,
    file_path: str,
) -> dict[str, object]:
    if mutation is None:
        return _empty_row(pool_name, state="migration_required", reason="no_audit")
    if mutation.command_kind == "delete_user_pool":
        if file_present:
            return _empty_row(
                pool_name, state="unavailable", reason="delete_conflict", mutation=mutation
            )
        return _empty_row(pool_name, state="deleted", reason=None, mutation=mutation)
    if raw is None:
        return _empty_row(
            pool_name,
            state="unavailable",
            reason="invalid_content" if file_present else "file_missing",
            mutation=mutation,
        )

    expected = _expected_fields(mutation)
    if expected is None:
        return _empty_row(
            pool_name, state="unavailable", reason="command_invalid", mutation=mutation
        )
    timestamp = datetime.fromisoformat(str(mutation.payload["requested_at"]))
    expected["updated_at"] = timestamp.isoformat(timespec="seconds")
    expected["command_id"] = mutation.command_id
    expected["command_hash"] = mutation.command_hash
    if set(raw) != set(expected) or any(raw.get(key) != value for key, value in expected.items()):
        return _empty_row(
            pool_name, state="unavailable", reason="content_mismatch", mutation=mutation
        )
    if raw.get("name") != pool_name.removeprefix("user/"):
        return _empty_row(pool_name, state="unavailable", reason="name_mismatch", mutation=mutation)
    version = canonical_sha256(raw)
    if mutation.result.get("path") != file_path or (
        mutation.command_kind == "save_user_pool_v2" and mutation.result.get("version") != version
    ):
        return _empty_row(
            pool_name, state="unavailable", reason="version_mismatch", mutation=mutation
        )
    if not _rules_are_registered(raw.get("rules")):
        return _empty_row(pool_name, state="unavailable", reason="rules_invalid", mutation=mutation)
    dependent = raw.get("depends_on")
    if dependent is not None and (not isinstance(dependent, str) or not dependent):
        return _empty_row(
            pool_name, state="unavailable", reason="dependency_invalid", mutation=mutation
        )
    if mutation.command_kind == "save_user_pool_v2":
        delay_mode = "exact" if dependent else "none"
        delay_days = raw.get("delay_days")
    else:
        delay_days = raw.get("offset_days", 0)
        delay_mode = "legacy_window" if dependent else "none"
    if type(delay_days) is not int or delay_days < 0 or (dependent and delay_days == 0):
        return _empty_row(pool_name, state="unavailable", reason="delay_invalid", mutation=mutation)
    if dependent is None and delay_days != 0:
        return _empty_row(pool_name, state="unavailable", reason="delay_invalid", mutation=mutation)
    return {
        "pool_name": pool_name,
        "display_name": raw.get("display_name", raw["name"]),
        "description": (
            (
                BUILTIN_PRESET_SCREENS[str(mutation.payload["builtin_name"])].ui_description
                or BUILTIN_PRESET_SCREENS[str(mutation.payload["builtin_name"])].description
            )
            if mutation.command_kind == "fork_builtin_pool"
            else raw.get("description", "")
        ),
        "source_kind": "user",
        "state": "available",
        "reason": None,
        "version": version,
        "command_id": mutation.command_id,
        "command_hash": mutation.command_hash,
        "depends_on": dependent,
        "delay_mode": delay_mode,
        "delay_days": delay_days,
        "rules_json": _json(raw["rules"]),
        "include_columns_json": _json(raw.get("include_columns", [])),
        "can_edit": True,
    }


def build_pool_definition_rows(
    files: Mapping[str, Mapping[str, object] | None],
    mutations: Mapping[str, PoolMutation],
    *,
    root_path: str,
) -> tuple[dict[str, object], ...]:
    """Return only validated facts; invalid sources retain a non-editable status row."""
    rows = _builtins()
    for base_name in sorted(set(files) | set(mutations)):
        key = f"user/{base_name}"
        rows[key] = _user_row(
            key,
            files.get(base_name),
            mutations.get(base_name),
            file_present=base_name in files,
            file_path=f"{root_path}/{base_name}.json",
        )

    def usable(name: str, chain: frozenset[str]) -> str | None:
        row = rows.get(name)
        if row is None:
            return "parent_missing"
        if row["state"] != "available":
            return "parent_unavailable"
        if name in chain:
            return "dependency_cycle"
        parent = row["depends_on"]
        if parent is None:
            return None
        return usable(str(parent), chain | {name})

    for name, row in tuple(rows.items()):
        if row["state"] != "available":
            continue
        reason = usable(name, frozenset())
        if reason is not None:
            invalid = _empty_row(
                name,
                state="unavailable",
                reason=reason,
                mutation=mutations.get(name.removeprefix("user/")),
            )
            rows[name] = invalid
    return tuple(rows[name] for name in sorted(rows))
