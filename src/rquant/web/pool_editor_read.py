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
    raw_rules = _json_list(rules_json, limit=_MAX_RULES)
    raw_columns = _json_list(columns_json, limit=_MAX_COLUMNS)
    if raw_rules is None or raw_columns is None:
        return None
    rules: list[EditorRuleCall] = []
    for item in raw_rules:
        if not isinstance(item, dict) or set(item) != {"name", "args"}:
            return None
        try:
            rule = EditorRuleCall.model_validate(item)
            spec = REGISTRY_BY_NAME[rule.name]
            spec.args_model.model_validate(rule.args)
        except (KeyError, TypeError, ValueError, ValidationError):
            return None
        rules.append(rule)
    if any(not isinstance(item, str) or not 1 <= len(item) <= 64 for item in raw_columns):
        return None
    return EditablePool(
        key=key,
        display_name=display_name,
        description=description,
        version=version,
        depends_on=depends_on,
        delay_days=delay_days,
        rule_calls=rules,
        include_columns=raw_columns,
    )


def _canvas(row: tuple[object, ...]) -> EditableCanvas | None:
    name, description, refs_json, version = row
    refs = _json_list(refs_json, limit=_MAX_CANVAS_REFS)
    if (
        not isinstance(name, str)
        or not 1 <= len(name) <= 80
        or not isinstance(description, str)
        or len(description) > 1_024
        or not _sha(version)
        or refs is None
        or any(not isinstance(item, str) or not item for item in refs)
        or len(set(refs)) != len(refs)
    ):
        return None
    return EditableCanvas(name=name, description=description, version=version, pool_refs=refs)


def read_pool_editor(borrowed: BorrowedGeneration | None) -> PoolEditorSnapshot:
    unavailable = PoolEditorSnapshot(
        data=PoolEditorData(state="unavailable", pools=[], canvases=[]),
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
    canvases: list[EditableCanvas] = []
    canvas_table = tables.get("canvas_definition")
    if canvas_table is not None and canvas_table.available:
        canvas_rows = cursor.execute(
            "SELECT name, description, pool_refs_json, version_hash "
            "FROM canvas_definition ORDER BY name LIMIT ?",
            (_MAX_CANVASES + 1,),
        ).fetchall()
        if len(canvas_rows) <= _MAX_CANVASES:
            canvases = [item for row in canvas_rows if (item := _canvas(row)) is not None]
    return PoolEditorSnapshot(
        data=PoolEditorData(state="ready", pools=pools, canvases=canvases),
        present_user_names=frozenset(
            key for key, *_ in pool_rows if isinstance(key, str) and key.startswith("user/")
        ),
        builtin_names=frozenset(
            key
            for key, _display, _description, source_kind, *_rest in pool_rows
            if isinstance(key, str) and source_kind == "builtin"
        ),
    )
