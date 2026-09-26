"""Pure, bounded TDX evaluation over caller-supplied closed daily bars.

The caller must prove that every stock's rows share one verified data generation,
contain every applicable trading day, and were visible at the decision time. This
module checks local shape and time bounds but cannot prove provenance or coverage.
Fixed windows require every bar. EMA/SMA seed from the first listing bar when known,
using MyTT's adjust=False recurrence and stay unknown after a missing input.
BARSLAST is unknown before its first true bar; a later true bar resets it.
"""

from __future__ import annotations

import math
import re
from datetime import date, datetime, time
from typing import Literal, TypeAlias
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, field_validator

from rquant.screen.tdx.ast import (
    SYNTAX_VERSION,
    BinaryExpr,
    CallExpr,
    Expr,
    FieldExpr,
    FormulaAst,
    LocalExpr,
    NumberExpr,
    UnaryExpr,
)
from rquant.screen.tdx.validate import parse_formula

MAX_STOCKS = 10_000
MAX_BARS_PER_STOCK = 12_000
MAX_TOTAL_ROWS = 200_000
MAX_HISTORY_SPAN_DAYS = 20_000
MAX_HISTORY_INPUT_BYTES = 24 * 1024 * 1024
MAX_VECTOR_CELLS = 1_000_000
MAX_WORK_CELLS = 8_000_000
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_STOCK_CODE = re.compile(r"[A-Z0-9._-]{1,32}\Z")
_EVALUATED_CALLS = frozenset({
    "MA", "EMA", "SMA", "REF", "HHV", "LLV", "CROSS", "COUNT", "SUM",
    "IF", "BARSLAST", "EVERY", "EXIST", "AND", "OR",
})

MissingReason: TypeAlias = Literal[
    "missing_date", "insufficient_history", "incomplete_history", "missing_value",
    "division_by_zero", "non_finite", "numeric_underflow", "never_true",
]
Scalar: TypeAlias = float | bool | None
Atom: TypeAlias = tuple[Scalar, MissingReason | None]
Vector: TypeAlias = list[Atom]


class HistoricalBar(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    trade_date: date
    open: float | None = Field(default=None, allow_inf_nan=False)
    high: float | None = Field(default=None, allow_inf_nan=False)
    low: float | None = Field(default=None, allow_inf_nan=False)
    close: float | None = Field(default=None, allow_inf_nan=False)
    vol: float | None = Field(default=None, allow_inf_nan=False)
    amount: float | None = Field(default=None, allow_inf_nan=False)


class StockHistory(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    stock_code: str
    complete_from_listing: bool
    bars: tuple[HistoricalBar, ...] = Field(strict=False)


class FormulaEvaluationInput(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    formula: str
    decision_date: date
    decision_at: datetime
    stocks: tuple[StockHistory, ...] = Field(strict=False)

    @field_validator("decision_at")
    @classmethod
    def _aware_decision_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("决策时点须包含时区。")
        return value


class StockDecision(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    stock_code: str
    status: Literal["match", "no_match", "unknown"]
    reason: MissingReason | None
    required_from: date | None


class FormulaEvaluationResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    syntax_version: Literal["tdx-v1"] = SYNTAX_VERSION
    decision_date: date
    required_lookback_bars: int
    requires_full_history: bool
    decisions: list[StockDecision]


class EvaluationRejectedError(ValueError):
    def __init__(self, code: Literal["formula", "shape", "limit", "time"], message: str) -> None:
        super().__init__(message)
        self.code = code


def _known(value: float | bool) -> Atom:
    if isinstance(value, float) and not math.isfinite(value):
        return (None, "non_finite")
    return (value, None)


def _unknown(reason: MissingReason) -> Atom:
    return (None, reason)


def _reason(*atoms: Atom) -> MissingReason:
    for atom in atoms:
        if atom[1] is not None:
            return atom[1]
    return "missing_value"


def _logic(op: str, left: Atom, right: Atom) -> Atom:
    if op == "AND":
        if left[0] is False or right[0] is False:
            return _known(False)
        if left[0] is None or right[0] is None:
            return _unknown(_reason(left, right))
        return _known(True)
    if left[0] is True or right[0] is True:
        return _known(True)
    if left[0] is None or right[0] is None:
        return _unknown(_reason(left, right))
    return _known(False)


def _number_op(op: str, left: Atom, right: Atom) -> Atom:
    if left[0] is None or right[0] is None:
        return _unknown(_reason(left, right))
    a, b = float(left[0]), float(right[0])
    if op == "/" and b == 0:
        return _unknown("division_by_zero")
    try:
        if op == "+":
            return _known(a + b)
        if op == "-":
            return _known(a - b)
        if op == "*":
            result = a * b
            if result == 0 and a != 0 and b != 0:
                return _unknown("numeric_underflow")
            return _known(result)
        if op == "/":
            result = a / b
            return _unknown("numeric_underflow") if result == 0 and a != 0 else _known(result)
        if op in {"=", "<>"}:
            same = a == b
            return _known(same if op == "=" else not same)
        if op == ">":
            return _known(a > b)
        if op == ">=":
            return _known(a >= b)
        if op == "<":
            return _known(a < b)
        if op == "<=":
            return _known(a <= b)
        raise EvaluationRejectedError("formula", "公式运算符尚未实现。")
    except OverflowError:
        return _unknown("non_finite")


def _window_size(node: Expr) -> int:
    if isinstance(node, UnaryExpr):
        return int(node.operand.value) * (-1 if node.op == "-" else 1)
    return int(node.value)


def _node_cost(node: Expr) -> tuple[int, int]:
    children: list[Expr]
    if isinstance(node, UnaryExpr):
        children = [node.operand]
    elif isinstance(node, BinaryExpr):
        children = [node.left, node.right]
    elif isinstance(node, CallExpr):
        children = node.args
    else:
        children = []
    parts = [_node_cost(item) for item in children]
    nodes = 1 + sum(item[0] for item in parts)
    work = 1 + sum(item[1] for item in parts)
    if isinstance(node, CallExpr) and node.name in {
        "MA", "HHV", "LLV", "COUNT", "SUM", "EVERY", "EXIST",
    }:
        work += _window_size(node.args[1]) - 1
    return nodes, work


def _budget_and_validate(request: FormulaEvaluationInput, ast: FormulaAst) -> None:
    if not isinstance(request.stocks, tuple):
        raise EvaluationRejectedError("shape", "股票历史格式不正确。")
    if len(request.stocks) > MAX_STOCKS:
        raise EvaluationRejectedError("limit", "股票数量超出本次计算上限。")
    roots = [item.value for item in ast.assignments] + [ast.output]
    costs = [_node_cost(item) for item in roots]
    nodes = sum(item[0] for item in costs)
    work_per_row = sum(item[1] for item in costs)
    row_count = 0
    input_bytes = 0
    seen_codes: set[str] = set()
    for stock in request.stocks:
        if not isinstance(stock, StockHistory) or not isinstance(stock.bars, tuple):
            raise EvaluationRejectedError("shape", "股票历史格式不正确。")
        if not isinstance(stock.stock_code, str) or not _STOCK_CODE.fullmatch(stock.stock_code):
            raise EvaluationRejectedError("shape", "股票代码格式不正确。")
        if type(stock.complete_from_listing) is not bool:
            raise EvaluationRejectedError("shape", "历史覆盖标记格式不正确。")
        if stock.stock_code in seen_codes:
            raise EvaluationRejectedError("shape", "股票代码重复，请检查历史输入。")
        seen_codes.add(stock.stock_code)
        n = len(stock.bars)
        if n > MAX_BARS_PER_STOCK:
            raise EvaluationRejectedError("limit", "单只股票历史行数超出上限。")
        if n * (nodes + 2) > MAX_VECTOR_CELLS:
            raise EvaluationRejectedError("limit", "公式中间计算量超出上限。")
        row_count += n
        if row_count > MAX_TOTAL_ROWS:
            raise EvaluationRejectedError("limit", "历史总行数超出上限。")
        if row_count * work_per_row > MAX_WORK_CELLS:
            raise EvaluationRejectedError("limit", "公式计算量超出上限。")
        input_bytes += len(stock.stock_code) + 32
        previous: date | None = None
        for bar in stock.bars:
            if not isinstance(bar, HistoricalBar) or type(bar.trade_date) is not date:
                raise EvaluationRejectedError("shape", "行情行格式不正确。")
            if bar.trade_date > request.decision_date:
                raise EvaluationRejectedError("time", "行情包含决策日之后的未来数据。")
            if previous is not None and bar.trade_date <= previous:
                raise EvaluationRejectedError("shape", "股票历史日期须严格递增且不重复。")
            previous = bar.trade_date
            for field in ("open", "high", "low", "close", "vol", "amount"):
                value = getattr(bar, field)
                if value is not None and (
                    type(value) is not float or not math.isfinite(value)
                ):
                    raise EvaluationRejectedError("shape", "行情数值格式不正确。")
            input_bytes += len(bar.model_dump_json())
            if input_bytes > MAX_HISTORY_INPUT_BYTES:
                raise EvaluationRejectedError("limit", "历史输入字节数超出上限。")
        if (
            n and (stock.bars[-1].trade_date - stock.bars[0].trade_date).days
            > MAX_HISTORY_SPAN_DAYS
        ):
            raise EvaluationRejectedError("limit", "历史日期跨度超出上限。")


class _StockEvaluator:
    def __init__(
        self, bars: tuple[HistoricalBar, ...], *, complete_from_listing: bool,
    ) -> None:
        self.bars = bars
        self.size = len(bars)
        self.complete_from_listing = complete_from_listing
        self.locals: dict[str, Vector] = {}

    def expression(self, node: Expr) -> Vector:
        if isinstance(node, NumberExpr):
            return [_known(node.value)] * self.size
        if isinstance(node, FieldExpr):
            name = node.name.lower()
            return [
                _known(value) if (value := getattr(bar, name)) is not None
                else _unknown("missing_value")
                for bar in self.bars
            ]
        if isinstance(node, LocalExpr):
            return self.locals[node.name]
        if isinstance(node, UnaryExpr):
            operand = self.expression(node.operand)
            if node.op == "NOT":
                return [
                    _known(not atom[0]) if atom[0] is not None else atom
                    for atom in operand
                ]
            if node.op == "+":
                return operand
            return [
                _known(-float(atom[0])) if atom[0] is not None else atom
                for atom in operand
            ]
        if isinstance(node, BinaryExpr):
            left, right = self.expression(node.left), self.expression(node.right)
            if node.op in {"AND", "OR"}:
                return [_logic(node.op, a, b) for a, b in zip(left, right, strict=True)]
            return [_number_op(node.op, a, b) for a, b in zip(left, right, strict=True)]
        if isinstance(node, CallExpr):
            return self._call(node)
        raise EvaluationRejectedError("formula", "公式表达式尚未实现。")

    def _call(self, node: CallExpr) -> Vector:
        name = node.name
        if name not in _EVALUATED_CALLS:
            raise EvaluationRejectedError("formula", "公式函数尚未实现。")
        if name in {"EMA", "SMA", "BARSLAST"} and not self.complete_from_listing:
            return [_unknown("incomplete_history")] * self.size
        if name in {"AND", "OR"}:
            left, right = self.expression(node.args[0]), self.expression(node.args[1])
            return [_logic(name, a, b) for a, b in zip(left, right, strict=True)]
        if name == "IF":
            condition = self.expression(node.args[0])
            yes = self.expression(node.args[1])
            no = self.expression(node.args[2])
            return [
                yes[index] if atom[0] is True else no[index] if atom[0] is False else atom
                for index, atom in enumerate(condition)
            ]
        if name == "REF":
            source = self.expression(node.args[0])
            offset = _window_size(node.args[1])
            return [
                source[index - offset] if index >= offset else _unknown("insufficient_history")
                for index in range(self.size)
            ]
        if name == "CROSS":
            left, right = self.expression(node.args[0]), self.expression(node.args[1])
            result = [_unknown("insufficient_history")]
            for index in range(1, self.size):
                earlier_left, earlier_right = left[index - 1], right[index - 1]
                now_left, now_right = left[index], right[index]
                if any(atom[0] is None for atom in (
                    earlier_left, earlier_right, now_left, now_right,
                )):
                    result.append(_unknown(_reason(
                        earlier_left, earlier_right, now_left, now_right,
                    )))
                else:
                    result.append(_known(
                        float(earlier_left[0]) <= float(earlier_right[0])
                        and float(now_left[0]) > float(now_right[0])
                    ))
            return result[: self.size]
        if name == "BARSLAST":
            source = self.expression(node.args[0])
            result: Vector = []
            elapsed: int | None = None
            unresolved: MissingReason = "never_true"
            for atom in source:
                if atom[0] is True:
                    elapsed = 0
                    unresolved = "never_true"
                    result.append(_known(0.0))
                elif atom[0] is None:
                    elapsed = None
                    unresolved = _reason(atom)
                    result.append(_unknown(unresolved))
                elif elapsed is None:
                    result.append(_unknown(unresolved))
                else:
                    elapsed += 1
                    result.append(_known(float(elapsed)))
            return result
        source = self.expression(node.args[0])
        window = _window_size(node.args[1])
        if name in {"EMA", "SMA"}:
            alpha = (2 / (window + 1)) if name == "EMA" else (
                _window_size(node.args[2]) / window
            )
            result = []
            prior: Atom | None = None
            for current in source:
                if current[0] is None:
                    prior = current
                elif prior is None:
                    prior = _known(float(current[0]))
                elif prior[0] is not None:
                    current_value = float(current[0])
                    prior_value = float(prior[0])
                    current_term = alpha * current_value
                    prior_weight = 1 - alpha
                    prior_term = prior_weight * prior_value
                    if (
                        (current_value != 0 and current_term == 0)
                        or (prior_value != 0 and prior_weight != 0 and prior_term == 0)
                    ):
                        prior = _unknown("numeric_underflow")
                    else:
                        prior = _known(current_term + prior_term)
                result.append(prior)
            return result
        result = []
        for index in range(self.size):
            if index + 1 < window:
                result.append(_unknown("insufficient_history"))
                continue
            selected = source[index + 1 - window : index + 1]
            if name == "EVERY":
                if any(atom[0] is False for atom in selected):
                    result.append(_known(False))
                elif any(atom[0] is None for atom in selected):
                    result.append(_unknown(_reason(*selected)))
                else:
                    result.append(_known(True))
                continue
            if name == "EXIST":
                if any(atom[0] is True for atom in selected):
                    result.append(_known(True))
                elif any(atom[0] is None for atom in selected):
                    result.append(_unknown(_reason(*selected)))
                else:
                    result.append(_known(False))
                continue
            if any(atom[0] is None for atom in selected):
                result.append(_unknown(_reason(*selected)))
                continue
            values = [float(atom[0]) for atom in selected]
            try:
                if name == "MA":
                    total = math.fsum(values)
                    mean = total / window
                    if total != 0 and mean == 0:
                        result.append(_unknown("numeric_underflow"))
                    else:
                        result.append(_known(mean))
                elif name == "SUM":
                    result.append(_known(math.fsum(values)))
                elif name == "COUNT":
                    result.append(_known(float(sum(bool(atom[0]) for atom in selected))))
                elif name == "HHV":
                    result.append(_known(max(values)))
                elif name == "LLV":
                    result.append(_known(min(values)))
                else:
                    raise EvaluationRejectedError("formula", "公式函数尚未实现。")
            except OverflowError:
                result.append(_unknown("non_finite"))
        return result


def evaluate_formula(request: FormulaEvaluationInput) -> FormulaEvaluationResult:
    if not isinstance(request, FormulaEvaluationInput):
        raise EvaluationRejectedError("shape", "求值输入格式不正确。")
    if not isinstance(request.formula, str):
        raise EvaluationRejectedError("shape", "公式文本格式不正确。")
    parsed = parse_formula(request.formula)
    if parsed.status != "parsed" or parsed.ast is None or parsed.translation is None:
        issue = parsed.issues[0].message if parsed.issues else parsed.unsupported[0].message
        raise EvaluationRejectedError("formula", issue)
    if type(request.decision_date) is not date or not isinstance(request.decision_at, datetime):
        raise EvaluationRejectedError("time", "决策日期或时点格式不正确。")
    if request.decision_at.tzinfo is None or request.decision_at.utcoffset() is None:
        raise EvaluationRejectedError("time", "决策时点须包含时区。")
    daily_visible_at = datetime.combine(request.decision_date, time(17), _SHANGHAI)
    if request.decision_at.astimezone(_SHANGHAI) < daily_visible_at:
        raise EvaluationRejectedError("time", "决策时点早于当日日线可用时间。")
    _budget_and_validate(request, parsed.ast)

    decisions: list[StockDecision] = []
    for stock in request.stocks:
        target_index = next(
            (index for index, bar in enumerate(stock.bars)
             if bar.trade_date == request.decision_date),
            None,
        )
        reason: MissingReason | None = None
        required_from: date | None = None
        if target_index is None:
            reason = "missing_date"
        elif parsed.translation.requires_full_history:
            required_from = stock.bars[0].trade_date if stock.complete_from_listing else None
        elif target_index >= parsed.translation.window_lookback_bars:
            earliest_index = target_index - parsed.translation.window_lookback_bars
            required_from = stock.bars[earliest_index].trade_date
        if reason is not None:
            decisions.append(StockDecision(
                stock_code=stock.stock_code, status="unknown", reason=reason,
                required_from=required_from,
            ))
            continue

        evaluator = _StockEvaluator(
            stock.bars, complete_from_listing=stock.complete_from_listing,
        )
        for assignment in parsed.ast.assignments:
            evaluator.locals[assignment.name] = evaluator.expression(assignment.value)
        value, reason = evaluator.expression(parsed.ast.output)[target_index]
        decisions.append(StockDecision(
            stock_code=stock.stock_code,
            status="unknown" if value is None else "match" if value else "no_match",
            reason=reason,
            required_from=required_from,
        ))
    return FormulaEvaluationResult(
        decision_date=request.decision_date,
        required_lookback_bars=parsed.translation.window_lookback_bars,
        requires_full_history=parsed.translation.requires_full_history,
        decisions=decisions,
    )
