"""One-generation, bounded editor facts from verified Serving projections."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

from pydantic import ValidationError

from rquant.llm.registry import REGISTRY_BY_NAME
from rquant.web import readers
from rquant.web.models.pool_editor import (
    BuiltinPoolCopySource,
    EditableCanvas,
    EditablePool,
    EditorRuleCall,
    PoolEditorData,
)
from rquant.web.serving import BorrowedGeneration

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MAX_POOLS = 512
_MAX_CANVASES = 512
_MAX_RULES = 32
_MAX_COLUMNS = 64
_MAX_CANVAS_REFS = 256
_MAX_JSON_BYTES = 64 * 1024


@dataclass(frozen=True)
class PoolEditorSnapshot:
    data: PoolEditorData
    present_user_names: frozenset[str]
    builtin_names: frozenset[str]


def _sha(value: object) -> bool:
    return isinstance(value, str) and _SHA256.fullmatch(value) is not None


def _json_list(value: object, *, limit: int) -> list[Any] | None:
    if not isinstance(value, str) or len(value.encode("utf-8")) > _MAX_JSON_BYTES:
        return None
    try:
        decoded = json.loads(value)
    except ValueError:
        return None
    return decoded if isinstance(decoded, list) and len(decoded) <= limit else None


def _rules(value: object) -> list[EditorRuleCall] | None:
    raw_rules = _json_list(value, limit=_MAX_RULES)
    if raw_rules is None:
        return None
    rules: list[EditorRuleCall] = []
    for item in raw_rules:
        if not isinstance(item, dict) or set(item) != {"name", "args"}:
            return None
        try:
            rule = EditorRuleCall.model_validate(item)
            spec = REGISTRY_BY_NAME[rule.name]
            if not set(rule.args) <= set(spec.args_model.model_fields):
                return None
            spec.args_model.model_validate(rule.args)
        except (KeyError, TypeError, ValueError, ValidationError):
            return None
        rules.append(rule)
    return rules


def _columns(value: object) -> list[str] | None:
    columns = _json_list(value, limit=_MAX_COLUMNS)
    if columns is None or any(
        not isinstance(item, str) or not 1 <= len(item) <= 64 for item in columns
    ):
        return None
    return columns


def _pool(row: tuple[object, ...]) -> EditablePool | None:
    (
        key,
        display_name,
        description,
        source_kind,
        state,
        version,
        depends_on,
        delay_mode,
        delay_days,
        rules_json,
        columns_json,
        can_edit,
    ) = row
    if (
        source_kind != "user"
        or state != "available"
        or can_edit is not True
        or not isinstance(key, str)
        or not key.startswith("user/")
        or not _sha(version)
        or not isinstance(display_name, str)
        or not 1 <= len(display_name) <= 80
        or not isinstance(description, str)
        or len(description) > 1_024
    ):
        return None
    if depends_on is None:
        if delay_mode != "none" or delay_days != 0:
            return None
    elif (
        not isinstance(depends_on, str)
        or delay_mode != "exact"
        or type(delay_days) is not int
        or not 1 <= delay_days <= 252
    ):
        return None
    rules = _rules(rules_json)
    columns = _columns(columns_json)
    if rules is None or columns is None:
        return None
    return EditablePool(
        key=key,
        display_name=display_name,
        description=description,
        version=version,
        depends_on=depends_on,
        delay_days=delay_days,
        rule_calls=rules,
        include_columns=columns,
    )


def _copy_source(row: tuple[object, ...]) -> BuiltinPoolCopySource | None:
    (
        key,
        display_name,
        description,
        source_kind,
        state,
        version,
        depends_on,
        delay_mode,
        delay_days,
        rules_json,
        columns_json,
        can_edit,
    ) = row
    if (
        source_kind != "builtin"
        or state != "available"
        or can_edit is not False
        or not isinstance(key, str)
        or not 1 <= len(key) <= 100
        or key.startswith("user/")
        or not _sha(version)
        or not isinstance(display_name, str)
        or not 1 <= len(display_name) <= 80
        or not isinstance(description, str)
        or len(description) > 1_024
        or type(delay_days) is not int
        or not 0 <= delay_days <= 10_000
    ):
        return None
    if depends_on is None:
        if delay_mode != "none" or delay_days != 0:
            return None
    elif (
        not isinstance(depends_on, str)
        or not 1 <= len(depends_on) <= 100
        or delay_mode not in {"exact", "legacy_window"}
        or delay_days < 1
    ):
        return None
    rules = _rules(rules_json)
    columns = _columns(columns_json)
    if rules is None or columns is None:
        return None
    block_reason = None
    if delay_mode == "legacy_window":
        block_reason = "旧版时间窗口与精确延后日不同，暂不能无损复制。"
    elif delay_days > 252:
        block_reason = "延后天数超出可保存范围，暂不能复制。"
    elif not rules:
        block_reason = "没有可复制的选股条件。"
    return BuiltinPoolCopySource(
        key=key,
        display_name=display_name,
        description=description,
        version=version,
        depends_on=depends_on,
        delay_mode=delay_mode,
        delay_days=delay_days,
        rule_calls=rules,
        include_columns=columns,
        copyable=block_reason is None,
        copy_block_reason=block_reason,
    )


def _canvas(row: tuple[object, ...]) -> EditableCanvas | None:
    name, description, refs_json, version, command_id, record_hash = row
    refs = _json_list(refs_json, limit=_MAX_CANVAS_REFS)
    if (
        not isinstance(name, str)
        or not 1 <= len(name) <= 80
        or not isinstance(description, str)
        or len(description) > 1_024
        or not _sha(version)
        or not isinstance(command_id, str)
        or not 1 <= len(command_id) <= 128
        or not _sha(record_hash)
        or refs is None
        or any(not isinstance(item, str) or not item for item in refs)
        or len(set(refs)) != len(refs)
    ):
        return None
    return EditableCanvas(
        name=name,
        description=description,
        version=version,
        pool_refs=refs,
        command_id=command_id,
        record_hash=record_hash,
    )


def read_pool_editor(borrowed: BorrowedGeneration | None) -> PoolEditorSnapshot:
    unavailable = PoolEditorSnapshot(
        data=PoolEditorData(
            state="unavailable",
            pools=[],
            copy_sources=[],
            canvases=[],
            canvas_create_available=False,
        ),
        present_user_names=frozenset(),
        builtin_names=frozenset(),
    )
    if borrowed is None:
        return unavailable
    cursor = borrowed.cursor
    tables = readers.table_states(cursor)
    definition = tables.get("pool_definition")
    if definition is None or not definition.available:
        return unavailable
    pool_rows = cursor.execute(
        "SELECT pool_name, display_name, description, source_kind, state, version, "
        "depends_on, delay_mode, delay_days, rules_json, include_columns_json, can_edit "
        "FROM pool_definition ORDER BY pool_name LIMIT ?",
        (_MAX_POOLS + 1,),
    ).fetchall()
    if len(pool_rows) > _MAX_POOLS:
        return unavailable
    pools = [item for row in pool_rows if (item := _pool(row)) is not None]
    copy_sources = [item for row in pool_rows if (item := _copy_source(row)) is not None]
    canvases: list[EditableCanvas] = []
    canvas_table = tables.get("canvas_definition")
    canvas_create_available = canvas_table is not None and canvas_table.available
    if canvas_create_available:
        canvas_rows = cursor.execute(
            "SELECT name, description, pool_refs_json, version_hash, command_id, record_hash "
            "FROM canvas_definition ORDER BY name LIMIT ?",
            (_MAX_CANVASES + 1,),
        ).fetchall()
        if len(canvas_rows) <= _MAX_CANVASES:
            canvases = [item for row in canvas_rows if (item := _canvas(row)) is not None]
        else:
            canvas_create_available = False
    return PoolEditorSnapshot(
        data=PoolEditorData(
            state="ready",
            pools=pools,
            copy_sources=copy_sources,
            canvases=canvases,
            canvas_create_available=canvas_create_available,
        ),
        present_user_names=frozenset(
            key for key, *_ in pool_rows if isinstance(key, str) and key.startswith("user/")
        ),
        builtin_names=frozenset(
            key
            for key, _display, _description, source_kind, *_rest in pool_rows
            if isinstance(key, str) and source_kind == "builtin"
        ),
    )
