"""Static type, name and history checks. This module never evaluates a formula."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

from rquant.screen.tdx.ast import (
    BinaryExpr,
    CallExpr,
    Expr,
    FieldExpr,
    LocalExpr,
    NumberExpr,
    ParseIssue,
    ParseResult,
    SourcePosition,
    TranslationPlan,
    UnaryExpr,
    UnsupportedItem,
)
from rquant.screen.tdx.parser import ParsedFormula, parse_syntax
from rquant.screen.tdx.tokens import FormulaError, check_source_budget

MAX_HISTORY_BARS = 500
MARKET_FIELDS = frozenset({"CLOSE", "OPEN", "HIGH", "LOW", "VOL", "AMOUNT"})
FUNCTIONS = frozenset(
    {
        "MA", "EMA", "SMA", "REF", "HHV", "LLV", "CROSS", "COUNT", "SUM",
        "IF", "BARSLAST", "EVERY", "EXIST", "AND", "OR", "NOT",
    }
)
_WINDOW_FIRST_NUMERIC = frozenset({"MA", "EMA", "HHV", "LLV", "SUM", "SMA"})
_WINDOW_FIRST_BOOLEAN = frozenset({"COUNT", "EVERY", "EXIST"})


@dataclass(frozen=True)
class TypeInfo:
    kind: Literal["number", "boolean"]
    lookback: int = 0
    full_history: bool = False


class FormulaValidationError(Exception):
    def __init__(self, issue: ParseIssue) -> None:
        super().__init__(issue.message)
        self.issue = issue


def _children(expr: Expr) -> list[Expr]:
    if isinstance(expr, UnaryExpr):
        return [expr.operand]
    if isinstance(expr, BinaryExpr):
        return [expr.left, expr.right]
    if isinstance(expr, CallExpr):
        return expr.args
    return []


def _walk(expr: Expr) -> list[Expr]:
    nodes = [expr]
    for child in _children(expr):
        nodes.extend(_walk(child))
    return nodes


class Validator:
    def __init__(self, parsed: ParsedFormula) -> None:
        self.parsed = parsed
        self.locals: dict[str, TypeInfo] = {}
        self.fields: list[str] = []
        self.functions: list[str] = []

    def _position(self, node: object) -> SourcePosition:
        return self.parsed.positions[id(node)]

    def _fail(self, node: object, code: str, message: str) -> None:
        raise FormulaValidationError(
            ParseIssue(code=code, message=message, position=self._position(node))
        )

    def _remember(self, items: list[str], name: str) -> None:
        if name not in items:
            items.append(name)

    def unsupported(self) -> list[UnsupportedItem]:
        found: list[UnsupportedItem] = []
        seen: set[tuple[str, str]] = set()
        roots = [assignment.value for assignment in self.parsed.ast.assignments]
        roots.append(self.parsed.ast.output)
        for root in roots:
            for node in _walk(root):
                kind: Literal["function", "field"] | None = None
                if isinstance(node, CallExpr) and node.name not in FUNCTIONS:
                    kind = "function"
                elif isinstance(node, FieldExpr) and node.name not in MARKET_FIELDS:
                    kind = "field"
                if kind is not None and (kind, node.name) not in seen:
                    seen.add((kind, node.name))
                    found.append(
                        UnsupportedItem(
                            kind=kind,
                            name=node.name,
                            position=self._position(node),
                            message=(
                                f"暂不支持函数「{node.name}」，请修改公式。"
                                if kind == "function"
                                else f"暂不支持字段「{node.name}」，请修改公式。"
                            ),
                        )
                    )
        return found

    def _number_argument(self, node: Expr, *, allow_zero: bool, name: str) -> int:
        sign = 1
        value_node = node
        if isinstance(node, UnaryExpr) and node.op in {"+", "-"}:
            sign = -1 if node.op == "-" else 1
            value_node = node.operand
        if not isinstance(value_node, NumberExpr):
            self._fail(node, "range", f"{name}请填固定整数。")
        exact_value = Decimal(self.parsed.number_text[id(value_node)])
        if exact_value != exact_value.to_integral_value():
            self._fail(node, "range", f"{name}请填固定整数。")
        value = sign * int(exact_value)
        if value < 0 and name == "REF偏移":
            self._fail(node, "future", "不能引用未来数据，请改为非负偏移。")
        if value < 0 or (value == 0 and not allow_zero):
            self._fail(node, "range", f"{name}请填正整数。")
        if value > MAX_HISTORY_BARS:
            self._fail(node, "limit", f"{name}最多为500个交易日。")
        return value

    def _arity(self, node: CallExpr, expected: int) -> None:
        if len(node.args) != expected:
            self._fail(node, "type", f"{node.name}需要{expected}个参数。")

    def _require(self, node: Expr, actual: TypeInfo, wanted: str) -> None:
        if actual.kind != wanted:
            self._fail(node, "type", "条件和数值不能混用，请检查这里。")

    def _result(
        self, node: Expr, kind: Literal["number", "boolean"], *inputs: TypeInfo,
        extra: int = 0, full_history: bool = False,
    ) -> TypeInfo:
        lookback = max((item.lookback for item in inputs), default=0) + extra
        if lookback > MAX_HISTORY_BARS:
            self._fail(node, "limit", "历史回看超过500个交易日，请缩短窗口。")
        return TypeInfo(
            kind=kind,
            lookback=lookback,
            full_history=full_history or any(item.full_history for item in inputs),
        )

    def _infer(self, node: Expr) -> TypeInfo:
        if isinstance(node, NumberExpr):
            return TypeInfo("number")
        if isinstance(node, FieldExpr):
            self._remember(self.fields, node.name)
            return TypeInfo("number")
        if isinstance(node, LocalExpr):
            return self.locals[node.name]
        if isinstance(node, UnaryExpr):
            operand = self._infer(node.operand)
            if node.op == "NOT":
                self._remember(self.functions, "NOT")
                self._require(node.operand, operand, "boolean")
                return self._result(node, "boolean", operand)
            self._require(node.operand, operand, "number")
            return self._result(node, "number", operand)
        if isinstance(node, BinaryExpr):
            if node.op == "/" and self._is_literal_zero(node.right):
                self._fail(node.right, "range", "除数不能为零，请修改公式。")
            left = self._infer(node.left)
            right = self._infer(node.right)
            if node.op in {"AND", "OR"}:
                self._require(node.left, left, "boolean")
                self._require(node.right, right, "boolean")
                return self._result(node, "boolean", left, right)
            self._require(node.left, left, "number")
            self._require(node.right, right, "number")
            kind: Literal["number", "boolean"] = (
                "number" if node.op in {"+", "-", "*", "/"} else "boolean"
            )
            return self._result(node, kind, left, right)
        return self._infer_call(node)

    def _is_literal_zero(self, node: Expr) -> bool:
        if isinstance(node, NumberExpr):
            return Decimal(self.parsed.number_text[id(node)]) == 0
        if isinstance(node, UnaryExpr) and node.op in {"+", "-"}:
            return self._is_literal_zero(node.operand)
        return False

    def _infer_call(self, node: CallExpr) -> TypeInfo:
        self._remember(self.functions, node.name)
        if node.name in _WINDOW_FIRST_NUMERIC | _WINDOW_FIRST_BOOLEAN:
            self._arity(node, 3 if node.name == "SMA" else 2)
            first = self._infer(node.args[0])
            wanted = "boolean" if node.name in _WINDOW_FIRST_BOOLEAN else "number"
            self._require(node.args[0], first, wanted)
            window = self._number_argument(node.args[1], allow_zero=False, name="窗口")
            if node.name == "SMA":
                weight = self._number_argument(node.args[2], allow_zero=False, name="权重")
                if weight > window:
                    self._fail(node.args[2], "range", "权重不能大于窗口。")
            kind: Literal["number", "boolean"] = (
                "boolean" if node.name in {"EVERY", "EXIST"} else "number"
            )
            return self._result(
                node, kind, first, extra=window - 1,
                full_history=node.name in {"EMA", "SMA"},
            )
        if node.name == "REF":
            self._arity(node, 2)
            first = self._infer(node.args[0])
            offset = self._number_argument(node.args[1], allow_zero=True, name="REF偏移")
            return self._result(node, first.kind, first, extra=offset)
        if node.name == "CROSS":
            self._arity(node, 2)
            left, right = (self._infer(item) for item in node.args)
            self._require(node.args[0], left, "number")
            self._require(node.args[1], right, "number")
            return self._result(node, "boolean", left, right, extra=1)
        if node.name == "BARSLAST":
            self._arity(node, 1)
            first = self._infer(node.args[0])
            self._require(node.args[0], first, "boolean")
            return self._result(node, "number", first, full_history=True)
        if node.name == "IF":
            self._arity(node, 3)
            condition, yes, no = (self._infer(item) for item in node.args)
            self._require(node.args[0], condition, "boolean")
            if yes.kind != no.kind:
                self._fail(node, "type", "IF的两个结果类型要一致。")
            return self._result(node, yes.kind, condition, yes, no)
        if node.name in {"AND", "OR"}:
            self._arity(node, 2)
            left, right = (self._infer(item) for item in node.args)
            self._require(node.args[0], left, "boolean")
            self._require(node.args[1], right, "boolean")
            return self._result(node, "boolean", left, right)
        self._fail(node, "type", "这个函数暂不支持，请修改公式。")

    def validate(self) -> TranslationPlan:
        required: list[TypeInfo] = []
        for assignment in self.parsed.ast.assignments:
            if assignment.name in MARKET_FIELDS | FUNCTIONS or assignment.name in self.locals:
                self._fail(assignment, "name", "名称重复或与内置名称冲突，请换一个。")
            self.locals[assignment.name] = self._infer(assignment.value)
            required.append(self.locals[assignment.name])
        output = self._infer(self.parsed.ast.output)
        required.append(output)
        self._require(self.parsed.ast.output, output, "boolean")
        return TranslationPlan(
            fields=self.fields,
            functions=self.functions,
            assignments=[item.name for item in self.parsed.ast.assignments],
            window_lookback_bars=max(item.lookback for item in required),
            requires_full_history=any(item.full_history for item in required),
        )


def parse_formula(source: str) -> ParseResult:
    try:
        check_source_budget(source)
    except FormulaError as error:
        return ParseResult(
            status="rejected", ast=None, translation=None,
            issues=[error.issue], unsupported=[],
        )
    if not source.strip():
        return ParseResult(
            status="rejected", ast=None, translation=None, unsupported=[],
            issues=[
                ParseIssue(
                    code="syntax", message="请粘贴公式。",
                    position=SourcePosition(line=1, column=1, offset=0),
                )
            ],
        )
    try:
        parsed = parse_syntax(source)
    except FormulaError as error:
        return ParseResult(
            status="rejected", ast=None, translation=None,
            issues=[error.issue], unsupported=[],
        )
    validator = Validator(parsed)
    unsupported = validator.unsupported()
    if unsupported:
        return ParseResult(
            status="rejected", ast=parsed.ast, translation=None,
            issues=[], unsupported=unsupported,
        )
    try:
        translation = validator.validate()
    except FormulaValidationError as error:
        return ParseResult(
            status="rejected", ast=parsed.ast, translation=None,
            issues=[error.issue], unsupported=[],
        )
    return ParseResult(
        status="parsed", ast=parsed.ast, translation=translation,
        issues=[], unsupported=[],
    )
