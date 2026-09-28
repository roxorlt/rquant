"""Validate untrusted language-model conditions against one offered screen catalog."""

from __future__ import annotations

import json
import math
from datetime import date
from typing import Any

from pydantic import ValidationError

from rquant.llm.compile import compile_screen_plan
from rquant.llm.registry import REGISTRY_BY_NAME
from rquant.llm.schemas import RuleCall, ScreenPlan, Stage
from rquant.screen.loader import FUNDAMENTAL_COLS_MAP
from rquant.web.models.screen import ScreenCatalogData, ScreenCondition
from rquant.web.screen_catalog import validate_screen_choices

_MAX_CANDIDATE_BYTES = 16_384
_MAX_CONDITIONS = 26
_COMPARE_RULES = frozenset({"gt", "lt", "gte", "lte"})
_CROSS_RULES = frozenset({"cross_above", "cross_below"})


class InvalidScreenDraftError(Exception):
    """The model candidate is not executable under the offered catalog."""


class ScreenDateMismatchError(Exception):
    """The model asked for a different date than the one the user selected."""


def _profile(catalog: ScreenCatalogData) -> tuple[bool, bool, frozenset[str]]:
    dynamic_ma = catalog.source_kind == "replica"
    dynamic_rsi = any(
        block.key == "rsi_oversold"
        and any(
            parameter.key == "period" and parameter.input == "integer"
            for parameter in block.parameters
        )
        for block in catalog.blocks
    )
    values = {
        option.value
        for block in catalog.blocks
        for parameter in block.parameters
        for option in parameter.options
    }
    fundamental_fields = frozenset(
        name for name, column in FUNDAMENTAL_COLS_MAP.items() if f"{column}[0]" in values
    )
    return dynamic_ma, dynamic_rsi, fundamental_fields


def _condition(name: str, raw_args: dict[str, Any], *, dynamic_ma: bool) -> ScreenCondition:
    spec = REGISTRY_BY_NAME.get(name)
    if spec is None or not set(raw_args) <= set(spec.args_model.model_fields):
        raise InvalidScreenDraftError
    args = dict(raw_args)
    if name in _COMPARE_RULES:
        for operand in ("left", "right"):
            value = args.get(operand)
            if type(value) is str:
                try:
                    number = float(value)
                except ValueError:
                    continue
                if not math.isfinite(number):
                    raise InvalidScreenDraftError
                args[operand] = number
    if dynamic_ma and name in _CROSS_RULES:
        for operand in ("fast", "slow"):
            if type(args.get(operand)) is int:
                args[operand] = f"MA{args[operand]}"
    try:
        normalized = spec.args_model.model_validate(args).model_dump(mode="json")
    except (TypeError, ValueError, ValidationError) as error:
        raise InvalidScreenDraftError from error
    if dynamic_ma and name in _CROSS_RULES:
        for operand in ("fast", "slow"):
            value = normalized[operand]
            if type(value) is not str or not value.startswith("MA") or not value[2:].isdigit():
                raise InvalidScreenDraftError
            normalized[operand] = int(value[2:])
    return ScreenCondition(key=name, args=normalized)


def validate_screen_draft(
    raw: object, catalog: ScreenCatalogData, trade_date: date
) -> list[ScreenCondition]:
    if not isinstance(raw, dict) or not set(raw) <= {
        "trade_date", "stages", "rationale"
    }:
        raise InvalidScreenDraftError
    try:
        raw_bytes = json.dumps(raw, ensure_ascii=False, allow_nan=False).encode("utf-8")
        if len(raw_bytes) > _MAX_CANDIDATE_BYTES:
            raise InvalidScreenDraftError
    except (TypeError, ValueError) as error:
        raise InvalidScreenDraftError from error
    proposed_date = raw.get("trade_date")
    if proposed_date not in {"", trade_date.isoformat()}:
        raise ScreenDateMismatchError
    if not isinstance(raw.get("rationale", ""), str):
        raise InvalidScreenDraftError
    stages = raw.get("stages")
    if not isinstance(stages, list) or not 1 <= len(stages) <= 8:
        raise InvalidScreenDraftError
    dynamic_ma, dynamic_rsi, fundamental_fields = _profile(catalog)
    conditions: list[ScreenCondition] = []
    for stage in stages:
        if (
            not isinstance(stage, dict)
            or set(stage) != {"label", "rules"}
            or not isinstance(stage["label"], str)
            or not 1 <= len(stage["label"]) <= 80
            or not isinstance(stage["rules"], list)
        ):
            raise InvalidScreenDraftError
        for item in stage["rules"]:
            if (
                not isinstance(item, dict)
                or set(item) != {"name", "args"}
                or not isinstance(item["name"], str)
                or not isinstance(item["args"], dict)
            ):
                raise InvalidScreenDraftError
            conditions.append(_condition(item["name"], item["args"], dynamic_ma=dynamic_ma))
            if len(conditions) > _MAX_CONDITIONS:
                raise InvalidScreenDraftError
    if not conditions:
        raise InvalidScreenDraftError
    try:
        args_for_compiler = validate_screen_choices(
            conditions,
            dynamic_ma=dynamic_ma,
            dynamic_rsi=dynamic_rsi,
            fundamental_fields=fundamental_fields,
        )
        plan = ScreenPlan(
            trade_date=trade_date.isoformat(),
            stages=[
                Stage(
                    label="条件",
                    rules=[
                        RuleCall(name=condition.key, args=args)
                        for condition, args in zip(conditions, args_for_compiler, strict=True)
                    ],
                )
            ],
        )
        compile_screen_plan(plan)
        normalized_bytes = json.dumps(
            [condition.model_dump(mode="json") for condition in conditions],
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
        if len(normalized_bytes) > _MAX_CANDIDATE_BYTES:
            raise InvalidScreenDraftError
    except (KeyError, TypeError, ValueError, ValidationError) as error:
        raise InvalidScreenDraftError from error
    return conditions
