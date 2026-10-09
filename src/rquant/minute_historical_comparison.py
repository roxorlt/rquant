"""Pure diagnostics over separately frozen historical executions.

Origins are evidence supplied by a caller, not authenticated artifact-owner reads.
No result from this module certifies a trusted historical installation or execution.
"""

from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation, localcontext
from typing import Literal

from pydantic import Field, model_validator

from rquant.minute_backtest_artifact import MinuteSealedReplayResult
from rquant.minute_backtest_contracts import MAX_INPUT_BYTES, CommitSha, Sha256
from rquant.minute_backtest_publication_contracts import MinuteOriginMaterial
from rquant.minute_backtest_runner import MinuteRuntimeReplayResult
from rquant.order_execution_costs import calculate_execution_costs, calculate_order_execution_costs
from rquant.paper_contracts import PaperHolding
from rquant.research_run_spec import ExecutionCostOrderInput, ExecutionCostSpec, InstrumentContext
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.strict_json import strict_json_loads

HistoricalScope = Literal["signals", "fills", "fees", "ledger", "daily_nav"]

# Only these exact, reviewed source bytes implement the numeric evaluators below.
# An older implementation requires a separately reviewed adapter, not a name alias.
_COST_SOURCE_SHA256 = "1566383e2a19307b5610679c5a5bf0b32bb9ca04b4b5bd0a8c11f7e39859ca4a"
_V2_SEMANTICS_SHA256 = "d396fba659d7636f2636cfb30f920604aa51d934bbeb2e0b670185ff071f9d5e"

_SIGNAL_FIELDS = ("candidate_id", "action", "event_time", "available_at", "expires_at", "parameter_fingerprint", "dataset_snapshot_id", "feature_snapshot_id", "reason_codes", "evidence")
_FILL_FIELDS = ("ts_code", "side", "quantity", "price", "executed_at", "price_snapshot_id")
_FEE_FIELDS = ("commission", "transfer_fee", "tax", "total_fees")
_LEDGER_FIELDS = ("trade_date", "event_time", "kind", "amount", "cash_before", "cash_after", "fill_id", "currency")
_NAV_FIELDS = ("trade_date", "as_of", "cash", "nav", "realized_pnl", "unrealized_pnl", "holdings")
_REQUIRED = {"signals": _SIGNAL_FIELDS, "fills": (*_FILL_FIELDS, *_FEE_FIELDS), "ledger": _LEDGER_FIELDS, "daily_nav": _NAV_FIELDS}
_DECIMAL_FIELDS = frozenset((*_FEE_FIELDS, "price", "amount", "cash_before", "cash_after", "cash", "nav", "realized_pnl", "unrealized_pnl"))
_TIME_FIELDS = frozenset(("event_time", "available_at", "expires_at", "executed_at", "as_of"))


class HistoricalFrozenIdentity(RuntimeContractModel):
    engine_id: str | None
    engine_version: str | None
    producer_commit: CommitSha | None
    strategy_id: str | None
    strategy_version: str | None
    input_hash: Sha256 | None
    profile_hash: Sha256 | None


class HistoricalColumnMapping(RuntimeContractModel):
    field: str = Field(min_length=1)
    pointer: str = Field(pattern=r"^(?:$|/)")


class HistoricalTableMapping(RuntimeContractModel):
    rows_pointer: str = Field(pattern=r"^(?:$|/)")
    row_id_pointer: str = Field(pattern=r"^(?:$|/)")
    columns: tuple[HistoricalColumnMapping, ...]


class HistoricalResultMapping(RuntimeContractModel):
    identity_pointer: str = Field(pattern=r"^(?:$|/)")
    status_pointer: str = Field(pattern=r"^(?:$|/)")
    signals: HistoricalTableMapping | None
    fills: HistoricalTableMapping | None
    ledger: HistoricalTableMapping | None
    daily_nav: HistoricalTableMapping | None


class HistoricalRuleManifest(RuntimeContractModel):
    identity: HistoricalFrozenIdentity
    shared_input_sha256: Sha256 | None
    initial_cash: Decimal | None = Field(ge=0, allow_inf_nan=False)
    execution_costs: ExecutionCostSpec | None
    cost_evaluator: str | None
    cost_source_sha256: Sha256 | None
    legacy_semantics_sha256: Sha256 | None = None
    signal_rule_sha256: Sha256 | None
    execution_rule_sha256: Sha256 | None
    ledger_rule_sha256: Sha256 | None
    nav_rule_sha256: Sha256 | None


class HistoricalCostInputReference(RuntimeContractModel):
    fill_id: str = Field(min_length=1)
    object_key: str
    content_sha256: Sha256
    fill_id_pointer: str = Field(pattern=r"^(?:$|/)")
    order_input_pointer: str = Field(pattern=r"^(?:$|/)")
    instrument_context_pointer: str = Field(pattern=r"^(?:$|/)")
    quote_pointer: str = Field(pattern=r"^(?:$|/)")
    entry_reference_price_pointer: str | None = Field(default=None, pattern=r"^(?:$|/)")
    order_columns: tuple[HistoricalColumnMapping, ...] | None = None
    quote_columns: tuple[HistoricalColumnMapping, ...] | None = None


class HistoricalSideEvidence(RuntimeContractModel):
    identity: HistoricalFrozenIdentity
    result_origin: MinuteOriginMaterial
    rules_origin: MinuteOriginMaterial
    rule_sources: tuple[MinuteOriginMaterial, ...]
    inputs: tuple[MinuteOriginMaterial, ...]
    cost_inputs: tuple[HistoricalCostInputReference, ...] = ()
    result_mapping: HistoricalResultMapping | None = None
    ledger_origin: MinuteOriginMaterial | None = None
    ledger_mapping: HistoricalTableMapping | None = None


class HistoricalRowMatch(RuntimeContractModel):
    scope: HistoricalScope
    old_row_id: str = Field(min_length=1)
    new_row_id: str = Field(min_length=1)


class HistoricalValueReference(RuntimeContractModel):
    object_key: str
    content_sha256: Sha256
    pointer: str


class HistoricalObservedValue(RuntimeContractModel):
    field: str
    value: str | None
    source: HistoricalValueReference


class HistoricalFrozenRow(RuntimeContractModel):
    scope: HistoricalScope
    row_id: str
    source: HistoricalValueReference
    fields: tuple[HistoricalObservedValue, ...]

    def value(self, name: str) -> str | None:
        return next((item.value for item in self.fields if item.field == name), None)


class HistoricalNumericExplanation(RuntimeContractModel):
    old_evaluator: str
    new_evaluator: str
    old_rules: HistoricalValueReference
    new_rules: HistoricalValueReference
    old_rule_source_sha256: Sha256
    new_rule_source_sha256: Sha256
    old_input: HistoricalValueReference
    new_input: HistoricalValueReference
    old_quantity_basis: HistoricalValueReference | None
    new_quantity_basis: HistoricalValueReference | None
    old_selected_rule_ids: tuple[str, ...]
    new_selected_rule_ids: tuple[str, ...]
    old_observed: Decimal
    new_observed: Decimal
    old_computed: Decimal
    new_computed: Decimal
    delta: Decimal


class HistoricalDifference(RuntimeContractModel):
    scope: HistoricalScope
    old_row_id: str | None
    new_row_id: str | None
    field: str
    kind: Literal["observed_difference", "missing_row", "missing_fact"]
    old_value: HistoricalObservedValue | None = None
    new_value: HistoricalObservedValue | None = None
    same_basis: bool = False
    explanation: HistoricalNumericExplanation | None = None


class HistoricalFrozenSideView(RuntimeContractModel):
    identity: HistoricalFrozenIdentity
    result_origin: MinuteOriginMaterial
    rules_origin: MinuteOriginMaterial
    rule_sources: tuple[MinuteOriginMaterial, ...]
    inputs: tuple[MinuteOriginMaterial, ...]
    ledger_origin: MinuteOriginMaterial | None
    cost_inputs: tuple[HistoricalCostInputReference, ...]
    result_mapping: HistoricalResultMapping | None
    ledger_mapping: HistoricalTableMapping | None
    execution_status: Literal["complete", "incomplete", "unknown"]
    rows: tuple[HistoricalFrozenRow, ...]


class HistoricalExecutionComparison(RuntimeContractModel):
    status: Literal["unavailable", "execution_incomplete", "blocked", "diagnostic_complete"]
    new_side: HistoricalFrozenSideView
    old_side: HistoricalFrozenSideView
    differences: tuple[HistoricalDifference, ...]
    unexplained_count: int = Field(ge=0)
    same_basis_failures: tuple[str, ...]
    unavailable_reasons: tuple[str, ...]
    execution_complete: bool
    formal_history_passed: Literal[False] = False

    @model_validator(mode="after")
    def consistent_conclusion(self) -> HistoricalExecutionComparison:
        count = sum(item.explanation is None for item in self.differences)
        if self.unexplained_count != count:
            raise ValueError("historical unexplained count differs from retained differences")
        if self.status == "diagnostic_complete" and (count or self.same_basis_failures or self.unavailable_reasons or not self.execution_complete):
            raise ValueError("historical diagnostic conclusion is detached from its blockers")
        return self


@dataclass
class _Side:
    name: str
    evidence: HistoricalSideEvidence
    rules: HistoricalRuleManifest | None = None
    rows: list[HistoricalFrozenRow] = field(default_factory=list)
    unavailable: list[str] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)
    execution_status: Literal["complete", "incomplete", "unknown"] = "unknown"

    def unavailable_fact(self, reason: str) -> None:
        self.unavailable.append(f"{self.name}:{reason}")

    def view(self) -> HistoricalFrozenSideView:
        evidence = self.evidence
        return HistoricalFrozenSideView(identity=evidence.identity, result_origin=evidence.result_origin,
            rules_origin=evidence.rules_origin, rule_sources=evidence.rule_sources, inputs=evidence.inputs,
            ledger_origin=evidence.ledger_origin, cost_inputs=evidence.cost_inputs, result_mapping=evidence.result_mapping,
            ledger_mapping=evidence.ledger_mapping, execution_status=self.execution_status, rows=tuple(self.rows))


def _reference(origin: MinuteOriginMaterial, pointer: str) -> HistoricalValueReference:
    return HistoricalValueReference(object_key=origin.object_key, content_sha256=origin.content_sha256, pointer=pointer)


def _load(origin: MinuteOriginMaterial) -> object:
    if origin.format != "json":
        raise ValueError("strict JSON mapping requires an original JSON material")
    return strict_json_loads(origin.payload(), parse_float=Decimal,
        parse_constant=lambda value: (_ for _ in ()).throw(ValueError(f"nonfinite JSON value {value}")))


def _get(root: object, pointer: str) -> object:
    if pointer == "":
        return root
    if not pointer.startswith("/"):
        raise ValueError("mapping is not an RFC 6901 pointer")
    value = root
    for token in pointer[1:].split("/"):
        if re.search(r"~(?![01])", token):
            raise ValueError("invalid JSON pointer escape")
        key = token.replace("~1", "/").replace("~0", "~")
        if isinstance(value, dict):
            value = value[key]
        elif isinstance(value, list) and re.fullmatch(r"0|[1-9][0-9]*", key):
            value = value[int(key)]
        else:
            raise ValueError("mapping pointer is absent or ambiguous")
    return value


def _project(root: object, columns: tuple[HistoricalColumnMapping, ...] | None, expected: tuple[str, ...]) -> object:
    if columns is None:
        return root
    fields = tuple(column.field for column in columns)
    if len(set(fields)) != len(fields) or set(fields) != set(expected):
        raise ValueError("original input mapping must name every required field exactly once")
    return {column.field: _get(root, column.pointer) for column in columns}


def _decimal(value: object) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        raise ValueError("observed decimal must retain exact original digits")
    result = Decimal(value)
    if not result.is_finite():
        raise ValueError("observed decimal is nonfinite")
    return result


def _number(value: Decimal) -> str:
    if not value:
        return "0"
    sign, original, exponent = value.as_tuple()
    digits = list(original)
    while digits[-1] == 0:
        digits.pop()
        exponent += 1
    coefficient = "".join(str(digit) for digit in digits)
    prefix = "-" if sign else ""
    if len(coefficient) + abs(exponent) > 4096:
        return prefix + coefficient + "e" + str(exponent)
    point = len(coefficient) + exponent
    if exponent >= 0:
        return prefix + coefficient + "0" * exponent
    if point <= 0:
        return prefix + "0." + "0" * -point + coefficient
    return prefix + coefficient[:point] + "." + coefficient[point:]


def _exact_sum(values: tuple[Decimal, ...]) -> Decimal:
    if not values:
        return Decimal("0")
    precision = max(value.adjusted() for value in values) - min(value.as_tuple().exponent for value in values) + 4
    if precision > MAX_INPUT_BYTES:
        raise ValueError("exact arithmetic expansion exceeds original material byte bound")
    with localcontext() as context:
        context.prec = max(precision, 28)
        return sum(values, Decimal("0"))


def _timestamp(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("observed timestamp must be explicit")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("observed timestamp has no timezone")
    return parsed.astimezone(UTC).isoformat()


def _json_exact(value: object) -> str:
    if isinstance(value, Decimal):
        return _number(value)
    if isinstance(value, dict):
        return "{" + ",".join(json.dumps(key, ensure_ascii=False) + ":" + _json_exact(value[key]) for key in sorted(value)) + "}"
    if isinstance(value, list):
        return "[" + ",".join(_json_exact(child) for child in value) + "]"
    if isinstance(value, float):
        raise ValueError("original JSON numeric digits were not retained")
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def _normalize(name: str, value: object) -> str | None:
    if value is None:
        if name != "fill_id":
            raise ValueError(f"missing original value {name}")
        return None
    if name in _DECIMAL_FIELDS:
        return _number(_decimal(value))
    if name in _TIME_FIELDS:
        return _timestamp(value)
    if name == "quantity":
        if type(value) is not int or value <= 0 or value % 100:
            raise ValueError("observed fill quantity must be an explicit positive lot")
        return str(value)
    if name == "trade_date":
        if not isinstance(value, str):
            raise ValueError("observed trade date must be explicit")
        return date.fromisoformat(value).isoformat()
    if name == "holdings":
        if not isinstance(value, list):
            raise ValueError("original holdings are not an array")
        holdings = tuple(PaperHolding.model_validate(item) for item in value)
        codes = tuple(item.code for item in holdings)
        if len(codes) != len(set(codes)):
            raise ValueError("original holdings contain duplicate securities")
        observed = []
        for holding in sorted(holdings, key=lambda item: item.code):
            item = holding.model_dump(mode="json")
            item.update(average_cost=_number(holding.average_cost), market_price=_number(holding.market_price))
            observed.append(item)
        return _json_exact(observed)
    if name in {"evidence", "reason_codes"}:
        return _json_exact(value)
    if not isinstance(value, str) or not value:
        raise ValueError(f"observed {name} must be explicit text")
    return value


def _table(side: _Side, origin: MinuteOriginMaterial, root: object, scope: HistoricalScope, mapping: HistoricalTableMapping | None) -> None:
    if mapping is None:
        side.unavailable_fact(f"{scope}:missing mapping")
        return
    expected = _REQUIRED[scope]
    columns = {column.field: column for column in mapping.columns}
    if len(columns) != len(mapping.columns) or set(columns) != set(expected):
        side.unavailable_fact(f"{scope}:mapping must include every required original field exactly once")
    try:
        rows = _get(root, mapping.rows_pointer)
        if not isinstance(rows, list):
            raise ValueError("original table is not an array")
    except (KeyError, IndexError, ValueError, TypeError) as exc:
        side.unavailable_fact(f"{scope}:{exc}")
        return
    identifiers: list[str] = []
    for index, raw in enumerate(rows):
        path = f"{mapping.rows_pointer}/{index}"
        try:
            own_id = _get(raw, mapping.row_id_pointer)
            if not isinstance(own_id, str) or not own_id:
                raise ValueError("original row id is absent")
        except (KeyError, IndexError, ValueError, TypeError) as exc:
            side.unavailable_fact(f"{scope}:{index}:{exc}")
            own_id = f"unavailable-original-row:{index}"
        identifiers.append(own_id)
        observed: list[HistoricalObservedValue] = []
        for name in expected:
            column = columns.get(name)
            if column is None:
                continue
            try:
                value = _normalize(name, _get(raw, column.pointer))
                observed.append(HistoricalObservedValue(field=name, value=value, source=_reference(origin, path + column.pointer)))
            except (KeyError, IndexError, ValueError, TypeError, InvalidOperation) as exc:
                side.unavailable_fact(f"{scope}:{own_id}:{name}:{exc}")
        primary = tuple(item for item in observed if scope != "fills" or item.field in _FILL_FIELDS)
        side.rows.append(HistoricalFrozenRow(scope=scope, row_id=own_id, source=_reference(origin, path), fields=primary))
        if scope == "fills":
            side.rows.append(HistoricalFrozenRow(scope="fees", row_id=own_id, source=_reference(origin, path),
                fields=tuple(item for item in observed if item.field in _FEE_FIELDS)))
    if len(set(identifiers)) != len(identifiers):
        side.unavailable_fact(f"{scope}:duplicate original row ids")


def _rules(side: _Side) -> None:
    evidence = side.evidence
    for name, value in evidence.identity.model_dump().items():
        if value is None:
            side.unavailable_fact(f"identity:{name}:unknown frozen identity")
    for collection, label in ((evidence.rule_sources, "rule_sources"), (evidence.inputs, "inputs")):
        keys = [item.object_key for item in collection]
        if len(keys) != len(set(keys)):
            side.unavailable_fact(f"{label}:duplicate object keys")
    try:
        raw = _load(evidence.rules_origin)
        rules = HistoricalRuleManifest.model_validate(raw)
        side.rules = rules
        if rules.identity != evidence.identity:
            side.unavailable_fact("rules:identity differs from original result identity")
        source_hashes = {item.content_sha256 for item in evidence.rule_sources}
        for name in ("cost_source_sha256", "signal_rule_sha256", "execution_rule_sha256", "ledger_rule_sha256", "nav_rule_sha256"):
            digest = getattr(rules, name)
            if digest is None or digest not in source_hashes:
                side.unavailable_fact(f"rules:{name}:missing original rule bytes")
        if rules.shared_input_sha256 is None or rules.shared_input_sha256 not in {item.content_sha256 for item in evidence.inputs}:
            side.unavailable_fact("rules:shared input material missing")
        if rules.initial_cash is None:
            side.unavailable_fact("rules:initial cash missing")
        spec = rules.execution_costs
        if rules.cost_source_sha256 != _COST_SOURCE_SHA256:
            side.unavailable_fact("rules:cost_source_sha256:unknown original evaluator source")
        if spec is None:
            side.unavailable_fact("rules:execution costs missing")
        elif rules.cost_evaluator == "v3-shared-fill" and spec.schema_version == 3:
            if spec.cost_engine_version != evidence.identity.engine_version:
                side.unavailable_fact("rules:cost engine version differs from frozen identity")
        elif rules.cost_evaluator == "v2-notional-executed" and spec.schema_version == 2:
            if (rules.legacy_semantics_sha256 != _V2_SEMANTICS_SHA256 or rules.legacy_semantics_sha256 not in source_hashes
                or evidence.identity.engine_version != "a-share-round-trip-notional-v2" or spec.research_notional_per_trade is None):
                side.unavailable_fact("rules:unknown original v2 notional semantics")
        else:
            side.unavailable_fact("rules:unknown original cost evaluator/version")
    except (KeyError, ValueError, TypeError) as exc:
        side.unavailable_fact(f"rules:{exc}")


def _old(side: _Side) -> None:
    mapping = side.evidence.result_mapping
    if mapping is None:
        side.unavailable_fact("result:missing original legacy mapping")
        return
    try:
        raw = _load(side.evidence.result_origin)
    except (ValueError, TypeError) as exc:
        side.unavailable_fact(f"result:{exc}")
        return
    try:
        identity = HistoricalFrozenIdentity.model_validate(_get(raw, mapping.identity_pointer))
        if identity != side.evidence.identity:
            side.unavailable_fact("result:frozen identity differs")
        status = _get(raw, mapping.status_pointer)
        if status not in {"complete", "incomplete"}:
            raise ValueError("original execution status missing")
        side.execution_status = status
    except (KeyError, IndexError, ValueError, TypeError) as exc:
        side.unavailable_fact(f"result:identity/status:{exc}")
    for scope in ("signals", "fills", "ledger", "daily_nav"):
        origin = side.evidence.result_origin
        root = raw
        layout = getattr(mapping, scope)
        if scope == "ledger" and side.evidence.ledger_origin is not None:
            origin = side.evidence.ledger_origin
            layout = side.evidence.ledger_mapping
            try:
                root = _load(origin)
            except (ValueError, TypeError) as exc:
                side.unavailable_fact(f"ledger:{exc}")
                continue
        _table(side, origin, root, scope, layout)


def _native(side: _Side, original: MinuteSealedReplayResult | MinuteRuntimeReplayResult) -> MinuteRuntimeReplayResult | None:
    try:
        validated = type(original).model_validate(original)
        runtime = validated.result.replay if isinstance(validated, MinuteSealedReplayResult) else validated
        raw = _load(side.evidence.result_origin)
        if canonical_sha256(raw) != canonical_sha256(validated.model_dump(mode="json", exclude_computed_fields=True)):
            side.unavailable_fact("result:native DTO differs from original frozen result bytes")
            return None
        identity = side.evidence.identity
        if (identity.engine_id != "minute_runtime_replay" or identity.input_hash != runtime.input_hash
            or identity.profile_hash != runtime.profile_hash or identity.strategy_id != runtime.strategy_id
            or identity.strategy_version != str(runtime.strategy_version)
            or identity.producer_commit != runtime.execution_profile.paper_policy.producer_commit):
            side.unavailable_fact("result:native frozen identity differs from original profile/result")
        if side.rules is not None and (side.rules.execution_costs != runtime.execution_profile.execution_costs
            or side.rules.initial_cash != runtime.execution_profile.initial_cash):
            side.unavailable_fact("rules:native costs/initial cash differ from original profile")
        side.execution_status = runtime.status
        prefix = "/result/replay" if isinstance(validated, MinuteSealedReplayResult) else ""
        orders = {order.order_id: order for order in runtime.orders}
        origin = side.evidence.result_origin
        for index, signal in enumerate(runtime.signals):
            path = f"{prefix}/signals/{index}"
            data = _get(raw, path)
            values = tuple(HistoricalObservedValue(field=name, value=_normalize(name, data[name]), source=_reference(origin, path + "/" + name)) for name in _SIGNAL_FIELDS)
            side.rows.append(HistoricalFrozenRow(scope="signals", row_id=signal.signal_id, source=_reference(origin, path), fields=values))
        for index, fill in enumerate(runtime.fills):
            path = f"{prefix}/fills/{index}"
            data = fill.model_dump(mode="json")
            order = orders.get(fill.order_id)
            if order is None:
                side.unavailable_fact(f"fills:{fill.fill_id}:missing original order")
            else:
                data.update(ts_code=order.ts_code, side=order.side.value)
            for scope, names in (("fills", _FILL_FIELDS), ("fees", _FEE_FIELDS)):
                values: list[HistoricalObservedValue] = []
                for name in names:
                    try:
                        value = _normalize(name, data[name])
                        source = _reference(origin, f"{prefix}/orders/{runtime.orders.index(order)}/{name}" if name in {"ts_code", "side"} and order is not None else path + "/" + name)
                        values.append(HistoricalObservedValue(field=name, value=value, source=source))
                    except (KeyError, ValueError, TypeError, InvalidOperation) as exc:
                        side.unavailable_fact(f"{scope}:{fill.fill_id}:{name}:{exc}")
                side.rows.append(HistoricalFrozenRow(scope=scope, row_id=fill.fill_id, source=_reference(origin, path), fields=tuple(values)))
        for index, observation in enumerate(runtime.daily_valuations):
            path = f"{prefix}/daily_valuations/{index}"
            if observation.account is None:
                side.unavailable_fact(f"daily_nav:{observation.trade_date}:original valuation unavailable")
                continue
            data = observation.account.model_dump(mode="json")
            data.update(trade_date=observation.trade_date.isoformat(), as_of=observation.as_of.isoformat())
            values = tuple(HistoricalObservedValue(field=name, value=_normalize(name, data[name]),
                source=_reference(origin, path + ("/" + name if name in {"trade_date", "as_of"} else "/account/" + name))) for name in _NAV_FIELDS)
            side.rows.append(HistoricalFrozenRow(scope="daily_nav", row_id=observation.trade_date.isoformat(), source=_reference(origin, path), fields=values))
        if side.evidence.ledger_origin is None:
            side.unavailable_fact("ledger:missing actual complete frozen ledger material")
        else:
            _table(side, side.evidence.ledger_origin, _load(side.evidence.ledger_origin), "ledger", side.evidence.ledger_mapping)
        return runtime
    except (KeyError, IndexError, ValueError, TypeError, InvalidOperation) as exc:
        side.unavailable_fact(f"result:native adapter:{exc}")
        return None


def _row_index(side: _Side) -> dict[tuple[str, str], HistoricalFrozenRow]:
    counts = Counter((row.scope, row.row_id) for row in side.rows)
    return {(row.scope, row.row_id): row for row in side.rows if counts[row.scope, row.row_id] == 1}


@dataclass(frozen=True)
class _CostReceipt:
    values: dict[str, Decimal]
    source: HistoricalValueReference
    selected_rule_ids: tuple[str, ...]
    input_basis: str
    quantity_basis: HistoricalValueReference | None


def _receipts(side: _Side, native: MinuteRuntimeReplayResult | None = None) -> dict[str, _CostReceipt]:
    rules = side.rules
    if rules is None or rules.execution_costs is None or any(reason.startswith(f"{side.name}:rules:") for reason in side.unavailable):
        return {}
    rows = _row_index(side)
    references = Counter(item.fill_id for item in side.evidence.cost_inputs)
    materials = {(item.object_key, item.content_sha256): item for item in side.evidence.inputs}
    receipts: dict[str, _CostReceipt] = {}
    native_fills = {} if native is None else {item.fill_id: item for item in native.fills}
    for key, fill in rows.items():
        if key[0] != "fills":
            continue
        try:
            if references[fill.row_id] != 1:
                raise ValueError("missing or duplicate original cost input reference")
            reference = next(item for item in side.evidence.cost_inputs if item.fill_id == fill.row_id)
            material = materials[reference.object_key, reference.content_sha256]
            raw = _load(material)
            if _get(raw, reference.fill_id_pointer) != fill.row_id:
                raise ValueError("cost input is detached from the original fill")
            order = ExecutionCostOrderInput.model_validate(_project(_get(raw, reference.order_input_pointer), reference.order_columns, ("side", "reference_price", "quantity")))
            context = InstrumentContext.model_validate(_get(raw, reference.instrument_context_pointer))
            quote = _project(_get(raw, reference.quote_pointer), reference.quote_columns, ("snapshot_id", "ts_code", "reference_price", "event_time", "available_at"))
            if not isinstance(quote, dict) or set(quote) != {"snapshot_id", "ts_code", "reference_price", "event_time", "available_at"}:
                raise ValueError("complete original reference quote context missing")
            if (str(order.quantity) != fill.value("quantity") or order.side.value != fill.value("side") or context.ts_code != fill.value("ts_code")
                or quote["ts_code"] != context.ts_code or quote["snapshot_id"] != fill.value("price_snapshot_id")
                or _decimal(quote["reference_price"]) != order.reference_price):
                raise ValueError("cost input/quote differs from original fill identity or quantities")
            if not (_timestamp(quote["event_time"]) <= _timestamp(quote["available_at"]) <= fill.value("executed_at")):
                raise ValueError("cost quote is from the future")
            spec = rules.execution_costs
            quantity_basis = None
            if rules.cost_evaluator == "v3-shared-fill":
                calculated = calculate_execution_costs(spec, order, context)
                own_fill = native_fills.get(fill.row_id)
                if own_fill is not None and (own_fill.cost_spec_id != spec.cost_spec_id or own_fill.cost_context_fingerprint != calculated.cost_context_fingerprint):
                    raise ValueError("native fill cost provenance differs from its original calculation")
                values = {"price": calculated.executed_price, "commission": calculated.commission, "transfer_fee": calculated.transfer_fee,
                    "tax": calculated.stamp_duty, "total_fees": calculated.total_fees}
                selected = tuple(calculated.selected_rule_ids.values())
            else:
                assert spec.research_notional_per_trade is not None
                if reference.entry_reference_price_pointer is None:
                    if order.side.value == "SELL":
                        raise ValueError("original entry reference price missing for v2 sell lot")
                    entry_price = order.reference_price
                    relative = "/reference_price" if reference.order_columns is None else next(column.pointer for column in reference.order_columns if column.field == "reference_price")
                    entry_pointer = reference.order_input_pointer + relative
                else:
                    entry_pointer = reference.entry_reference_price_pointer
                    entry_price = _decimal(_get(raw, entry_pointer))
                    if entry_price <= 0 or (order.side.value == "BUY" and entry_price != order.reference_price):
                        raise ValueError("original entry reference price differs from v2 entry")
                quantity_basis = _reference(material, entry_pointer)
                quantity = int(spec.research_notional_per_trade // (entry_price * 100)) * 100
                if quantity != order.quantity:
                    raise ValueError("v2 research notional does not produce the original fill lot")
                calculated_old = calculate_order_execution_costs(side=order.side.value.lower(), reference_price=order.reference_price, quantity=order.quantity,
                    commission_rate=spec.commission_bps / 10000, minimum_commission=spec.minimum_commission,
                    transfer_fee_rate=spec.transfer_fee_bps / 10000, sell_stamp_duty_rate=spec.stamp_duty_bps / 10000,
                    slippage_bps=spec.slippage_bps, fee_notional_basis="executed", price_tick=Decimal("0.0001"), money_quantum=Decimal("0.01"))
                values = {"price": calculated_old.executed_price, "commission": calculated_old.commission, "transfer_fee": calculated_old.transfer_fee,
                    "tax": calculated_old.stamp_duty, "total_fees": calculated_old.fee_amount}
                selected = ("schema-v2:executed-notional:order:HALF_UP",)
            fee = rows.get(("fees", fill.row_id))
            valid = True
            for name, amount in values.items():
                row = fill if name == "price" else fee
                if row is None or row.value(name) is None:
                    raise ValueError(f"missing original observed cost component {name}")
                if _decimal(row.value(name)) != amount:
                    side.failures.append(f"{side.name}:{row.scope}:{row.row_id}:{name}:original observation differs from frozen rule calculation")
                    valid = False
            if valid:
                basis = canonical_sha256({"side": order.side.value, "quantity": order.quantity, "reference_price": _number(order.reference_price),
                    "context": context.model_dump(mode="json"), "quote_id": quote["snapshot_id"], "quote_code": quote["ts_code"],
                    "quote_reference_price": _number(_decimal(quote["reference_price"])),
                    "quote_event_time": _timestamp(quote["event_time"]), "quote_available_at": _timestamp(quote["available_at"])})
                receipts[fill.row_id] = _CostReceipt(values=values, source=_reference(material, reference.order_input_pointer), selected_rule_ids=selected, input_basis=basis, quantity_basis=quantity_basis)
        except (KeyError, IndexError, ValueError, TypeError, InvalidOperation) as exc:
            side.unavailable_fact(f"cost_inputs:{fill.row_id}:{exc}")
    excess = set(references) - {row.row_id for row in side.rows if row.scope == "fills"}
    if excess:
        side.unavailable_fact("cost_inputs:references name absent original fills")
    return receipts


def _same_basis(scope: HistoricalScope, left: _Side, right: _Side, old_receipt: _CostReceipt | None, new_receipt: _CostReceipt | None) -> bool:
    a, b = left.rules, right.rules
    if a is None or b is None or a.shared_input_sha256 is None or a.shared_input_sha256 != b.shared_input_sha256:
        return False
    if scope == "signals":
        return a.signal_rule_sha256 is not None and a.signal_rule_sha256 == b.signal_rule_sha256
    same_costs = a.execution_costs is not None and a.execution_costs == b.execution_costs and a.cost_evaluator == b.cost_evaluator
    if scope in {"fills", "fees"}:
        return bool(same_costs and a.execution_rule_sha256 == b.execution_rule_sha256 and old_receipt and new_receipt and old_receipt.input_basis == new_receipt.input_basis)
    rule_name = "ledger_rule_sha256" if scope == "ledger" else "nav_rule_sha256"
    return bool(same_costs and a.initial_cash == b.initial_cash and a.execution_rule_sha256 == b.execution_rule_sha256
        and getattr(a, rule_name) is not None and getattr(a, rule_name) == getattr(b, rule_name))


def _observed_accounting(side: _Side) -> None:
    fills = {row.row_id for row in side.rows if row.scope == "fills"}
    ledger = tuple(row for row in side.rows if row.scope == "ledger")
    linked = {row.value("fill_id") for row in ledger if row.value("fill_id") is not None}
    if fills - linked:
        side.unavailable_fact("ledger:complete actual flow rows missing for original fills")
    if linked - fills:
        side.unavailable_fact("ledger:flow references absent original fills")
    for row in side.rows:
        try:
            if row.scope == "ledger":
                before, amount, after = (_decimal(row.value(name)) for name in ("cash_before", "amount", "cash_after"))
                if _exact_sum((before, amount)) != after:
                    side.failures.append(f"{side.name}:ledger:{row.row_id}:cash_after:observed cash flow does not reconcile")
            elif row.scope == "daily_nav":
                holdings = strict_json_loads(row.value("holdings"), parse_float=Decimal)
                market_values: list[Decimal] = []
                unrealized: list[Decimal] = []
                for holding in holdings:
                    quantity = holding["quantity"]
                    price, cost = _decimal(holding["market_price"]), _decimal(holding["average_cost"])
                    # Multiplication by an integer lot must also retain all digits.
                    with localcontext() as context:
                        context.prec = max(len(price.as_tuple().digits), len(cost.as_tuple().digits)) + len(str(quantity)) + abs(price.as_tuple().exponent - cost.as_tuple().exponent) + 4
                        if context.prec > MAX_INPUT_BYTES:
                            raise ValueError("exact holdings arithmetic exceeds original material byte bound")
                        market_values.append(price * quantity)
                        unrealized.append((price - cost) * quantity)
                if _exact_sum((_decimal(row.value("cash")), *market_values)) != _decimal(row.value("nav")):
                    side.failures.append(f"{side.name}:daily_nav:{row.row_id}:nav:observed cash and holdings do not reconcile")
                if _exact_sum(tuple(unrealized)) != _decimal(row.value("unrealized_pnl")):
                    side.failures.append(f"{side.name}:daily_nav:{row.row_id}:unrealized_pnl:observed holdings do not reconcile")
        except (KeyError, ValueError, TypeError, InvalidOperation) as exc:
            side.unavailable_fact(f"{row.scope}:{row.row_id}:accounting:{exc}")


def compare_frozen_executions(
    *,
    native_result: MinuteSealedReplayResult | MinuteRuntimeReplayResult,
    new_evidence: HistoricalSideEvidence,
    old_evidence: HistoricalSideEvidence,
    matches: tuple[HistoricalRowMatch, ...],
) -> HistoricalExecutionComparison:
    """Retain exact observed differences; only verify supported causal cost math.

    ``matches`` is an explicit, exhaustive one-to-one correspondence. It does not
    authorize renaming rules, dropping rows, supplying explanations or tolerances.
    The caller must supply original complete ledger rows on each side separately.
    """
    if type(native_result) not in {MinuteSealedReplayResult, MinuteRuntimeReplayResult}:
        raise TypeError("historical comparison requires an original typed native result")
    left = _Side("old", HistoricalSideEvidence.model_validate(old_evidence))
    right = _Side("new", HistoricalSideEvidence.model_validate(new_evidence))
    bound_matches = tuple(HistoricalRowMatch.model_validate(item) for item in matches)
    _rules(left)
    _rules(right)
    _old(left)
    runtime = _native(right, native_result)
    old_receipts, new_receipts = _receipts(left), _receipts(right, runtime)
    _observed_accounting(left)
    _observed_accounting(right)
    old_rows, new_rows = _row_index(left), _row_index(right)
    old_counts = Counter((item.scope, item.old_row_id) for item in bound_matches)
    new_counts = Counter((item.scope, item.new_row_id) for item in bound_matches)
    valid_matches = tuple(item for item in bound_matches if old_counts[item.scope, item.old_row_id] == new_counts[item.scope, item.new_row_id] == 1)
    if len(valid_matches) != len(bound_matches):
        left.unavailable_fact("matches:correspondence is not one-to-one")
    fill_pairs = {(item.old_row_id, item.new_row_id) for item in valid_matches if item.scope == "fills"}
    differences: list[HistoricalDifference] = []
    failures = [*left.failures, *right.failures]
    covered_old: set[tuple[str, str]] = set()
    covered_new: set[tuple[str, str]] = set()
    for match in valid_matches:
        old_key, new_key = (match.scope, match.old_row_id), (match.scope, match.new_row_id)
        old_row, new_row = old_rows.get(old_key), new_rows.get(new_key)
        if old_row is None or new_row is None:
            left.unavailable_fact(f"matches:{match.scope}:{match.old_row_id}/{match.new_row_id}:original row absent or ambiguous")
            continue
        covered_old.add(old_key)
        covered_new.add(new_key)
        a = {value.field: value for value in old_row.fields}
        b = {value.field: value for value in new_row.fields}
        old_receipt, new_receipt = old_receipts.get(match.old_row_id), new_receipts.get(match.new_row_id)
        same = _same_basis(match.scope, left, right, old_receipt, new_receipt)
        for name in sorted(a.keys() | b.keys()):
            old_value, new_value = a.get(name), b.get(name)
            if old_value is not None and new_value is not None:
                if old_value.value == new_value.value:
                    continue
                if match.scope == "ledger" and name == "fill_id" and (old_value.value, new_value.value) in fill_pairs:
                    continue
            explanation = None
            if (not same and match.scope in {"fills", "fees"} and name in {"price", *_FEE_FIELDS}
                and old_receipt is not None and new_receipt is not None and old_value is not None and new_value is not None
                and old_receipt.input_basis == new_receipt.input_basis and left.rules is not None and right.rules is not None):
                explanation = HistoricalNumericExplanation(old_evaluator=left.rules.cost_evaluator, new_evaluator=right.rules.cost_evaluator,
                    old_rules=_reference(left.evidence.rules_origin, "/execution_costs"), new_rules=_reference(right.evidence.rules_origin, "/execution_costs"),
                    old_rule_source_sha256=left.rules.cost_source_sha256, new_rule_source_sha256=right.rules.cost_source_sha256,
                    old_input=old_receipt.source, new_input=new_receipt.source, old_selected_rule_ids=old_receipt.selected_rule_ids, new_selected_rule_ids=new_receipt.selected_rule_ids,
                    old_quantity_basis=old_receipt.quantity_basis, new_quantity_basis=new_receipt.quantity_basis,
                    old_observed=_decimal(old_value.value), new_observed=_decimal(new_value.value), old_computed=old_receipt.values[name], new_computed=new_receipt.values[name],
                    delta=_exact_sum((_decimal(new_value.value), _decimal(old_value.value).copy_negate())))
            differences.append(HistoricalDifference(scope=match.scope, old_row_id=match.old_row_id, new_row_id=match.new_row_id, field=name,
                kind="observed_difference" if old_value is not None and new_value is not None else "missing_fact",
                old_value=old_value, new_value=new_value, same_basis=same, explanation=explanation))
            if same:
                failures.append(f"{match.scope}:{match.old_row_id}/{match.new_row_id}:{name}:exact same-basis comparison failed")
    for side, covered, is_old in ((left, covered_old, True), (right, covered_new, False)):
        for row in side.rows:
            if (row.scope, row.row_id) in covered:
                continue
            side.unavailable_fact(f"matches:{row.scope}:{row.row_id}:unmapped original row")
            differences.append(HistoricalDifference(scope=row.scope, old_row_id=row.row_id if is_old else None,
                new_row_id=None if is_old else row.row_id, field="*", kind="missing_row"))
    unavailable = tuple(dict.fromkeys((*left.unavailable, *right.unavailable)))
    if unavailable:
        differences.append(HistoricalDifference(scope="ledger", old_row_id=None, new_row_id=None, field="required_frozen_facts", kind="missing_fact"))
    unexplained = sum(item.explanation is None for item in differences)
    complete = left.execution_status == right.execution_status == "complete"
    same_basis_failures = tuple(dict.fromkeys(failures))
    status = "unavailable" if unavailable else "execution_incomplete" if not complete else "blocked" if unexplained or same_basis_failures else "diagnostic_complete"
    return HistoricalExecutionComparison(status=status, new_side=right.view(), old_side=left.view(), differences=tuple(differences),
        unexplained_count=unexplained, same_basis_failures=same_basis_failures, unavailable_reasons=unavailable, execution_complete=complete)
