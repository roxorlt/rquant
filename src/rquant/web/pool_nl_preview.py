"""Pure pool draft validation, semantic diff, and bounded per-user admission."""

from __future__ import annotations

import json
import math
import threading
import time
from collections import defaultdict, deque
from collections.abc import Mapping, Sequence
from functools import lru_cache

from pydantic import ValidationError

from rquant.llm.dispatch import build_rules
from rquant.llm.registry import REGISTRY_BY_NAME
from rquant.llm.schemas import RuleCall, ScreenPlan, Stage
from rquant.screen.loader import FUNDAMENTAL_COLS_MAP
from rquant.web.models.pool_editor import (
    EditablePool,
    EditorRuleCall,
    PoolRuleChange,
)
from rquant.web.screen_catalog import screen_blocks

_MAX_CANDIDATE_BYTES = 16_384
_MAX_RULES = 32
_UNPUBLISHED_POOL_COLUMNS = frozenset(f"{name}[0]" for name in FUNDAMENTAL_COLS_MAP.values())


class InvalidPoolDraftError(Exception):
    """The untrusted candidate cannot be saved as this pool's rules."""


class NoPoolRuleChangeError(Exception):
    """The instruction produced no semantic change to the pool."""


class PoolNlRateLimiter:
    def __init__(self, *, max_per_minute: int = 3, max_users: int = 64) -> None:
        self._max_per_minute = max_per_minute
        self._max_users = max_users
        self._lock = threading.Lock()
        self._requests: dict[str, deque[float]] = {}

    def admit(self, user: str) -> bool:
        now = time.monotonic()
        with self._lock:
            for name, recent in tuple(self._requests.items()):
                while recent and recent[0] <= now - 60:
                    recent.popleft()
                if not recent:
                    del self._requests[name]
            recent = self._requests.get(user)
            if recent is None:
                if len(self._requests) >= self._max_users:
                    return False
                recent = deque()
                self._requests[user] = recent
            if len(recent) >= self._max_per_minute:
                return False
            recent.append(now)
            return True


@lru_cache(maxsize=1)
def _labels() -> dict[str, str]:
    return {block.key: block.label for block in screen_blocks()}


def _normalized(rule: EditorRuleCall) -> EditorRuleCall:
    spec = REGISTRY_BY_NAME.get(rule.name)
    if spec is None or not set(rule.args) <= set(spec.args_model.model_fields):
        raise InvalidPoolDraftError
    if any(
        type(value) is str and value in _UNPUBLISHED_POOL_COLUMNS
        for value in rule.args.values()
    ):
        raise InvalidPoolDraftError
    try:
        args = dict(rule.args)
        if rule.name in {"gt", "lt", "gte", "lte"}:
            for operand in ("left", "right"):
                value = args.get(operand)
                if isinstance(value, str):
                    try:
                        number = float(value)
                    except ValueError:
                        continue
                    if not math.isfinite(number):
                        raise InvalidPoolDraftError
                    args[operand] = number
        args = spec.args_model.model_validate(args).model_dump(mode="json")
        return EditorRuleCall(name=rule.name, args=args)
    except (TypeError, ValueError, ValidationError) as error:
        raise InvalidPoolDraftError from error


def _canonical(rule: EditorRuleCall) -> str:
    normalized = _normalized(rule)
    return json.dumps(
        [normalized.name, normalized.args], sort_keys=True, separators=(",", ":"), allow_nan=False
    )


def _same_name_changes(
    old: Sequence[EditorRuleCall], new: Sequence[EditorRuleCall]
) -> list[PoolRuleChange]:
    new_by_key: dict[str, deque[int]] = defaultdict(deque)
    for index, rule in enumerate(new):
        new_by_key[_canonical(rule)].append(index)
    exact_old: set[int] = set()
    exact_new: set[int] = set()
    for index, rule in enumerate(old):
        candidates = new_by_key[_canonical(rule)]
        if candidates:
            exact_old.add(index)
            exact_new.add(candidates.popleft())

    remaining_old = [rule for index, rule in enumerate(old) if index not in exact_old]
    remaining_new = [rule for index, rule in enumerate(new) if index not in exact_new]
    new_by_name: dict[str, deque[int]] = defaultdict(deque)
    for index, rule in enumerate(remaining_new):
        new_by_name[rule.name].append(index)
    paired_new: set[int] = set()
    changes: list[PoolRuleChange] = []
    for rule in remaining_old:
        candidates = new_by_name[rule.name]
        if candidates:
            index = candidates.popleft()
            paired_new.add(index)
            changes.append(
                PoolRuleChange(
                    kind="parameter_changed",
                    label=_labels()[rule.name],
                    before=rule,
                    after=remaining_new[index],
                )
            )
        else:
            changes.append(
                PoolRuleChange(kind="removed", label=_labels()[rule.name], before=rule)
            )
    for index, rule in enumerate(remaining_new):
        if index not in paired_new:
            changes.append(PoolRuleChange(kind="added", label=_labels()[rule.name], after=rule))
    return changes


def validate_pool_draft(
    raw: Mapping[str, object], base: EditablePool
) -> tuple[list[EditorRuleCall], list[PoolRuleChange]]:
    """Accept only a complete, saveable rule set; no model-owned pool metadata."""

    if not isinstance(raw, dict) or not set(raw) <= {
        "trade_date", "stages", "include_columns", "rationale"
    }:
        raise InvalidPoolDraftError
    try:
        encoded = json.dumps(raw, ensure_ascii=False, allow_nan=False).encode("utf-8")
        if len(encoded) > _MAX_CANDIDATE_BYTES:
            raise InvalidPoolDraftError
    except (TypeError, ValueError) as error:
        raise InvalidPoolDraftError from error
    if raw.get("trade_date") not in {"", "1900-01-01"}:
        raise InvalidPoolDraftError
    if raw.get("include_columns", base.include_columns) != base.include_columns:
        raise InvalidPoolDraftError
    stages = raw.get("stages")
    if not isinstance(stages, list) or not 1 <= len(stages) <= 8:
        raise InvalidPoolDraftError
    calls: list[EditorRuleCall] = []
    for stage in stages:
        if (
            not isinstance(stage, dict)
            or set(stage) != {"label", "rules"}
            or not isinstance(stage["label"], str)
            or not 1 <= len(stage["label"]) <= 80
            or not isinstance(stage["rules"], list)
        ):
            raise InvalidPoolDraftError
        for item in stage["rules"]:
            if not isinstance(item, dict) or set(item) != {"name", "args"}:
                raise InvalidPoolDraftError
            try:
                rule = EditorRuleCall.model_validate(item)
            except ValidationError as error:
                raise InvalidPoolDraftError from error
            calls.append(_normalized(rule))
            if len(calls) > _MAX_RULES:
                raise InvalidPoolDraftError
    if not calls:
        raise InvalidPoolDraftError
    try:
        build_rules(
            ScreenPlan(
                trade_date="1900-01-01",
                stages=[
                    Stage(label="已核对", rules=[RuleCall(name=r.name, args=r.args) for r in calls])
                ],
                include_columns=base.include_columns,
            )
        )
    except (TypeError, ValueError, ValidationError) as error:
        raise InvalidPoolDraftError from error
    changes = _same_name_changes([_normalized(rule) for rule in base.rule_calls], calls)
    if not changes:
        raise NoPoolRuleChangeError
    return calls, changes
