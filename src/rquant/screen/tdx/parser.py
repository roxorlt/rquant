"""Explicit precedence parser for a small Tongdaxin subset."""

from __future__ import annotations

from dataclasses import dataclass

from rquant.screen.tdx.ast import (
    Assignment,
    BinaryExpr,
    CallExpr,
    Expr,
    FieldExpr,
    FormulaAst,
    LocalExpr,
    NumberExpr,
    ParseIssue,
    SourcePosition,
    UnaryExpr,
)
from rquant.screen.tdx.tokens import FormulaError, Token, tokenize

MAX_ASSIGNMENTS = 32
MAX_AST_DEPTH = 32
MAX_AST_NODES = 512

_BINDING_POWER = {
    "OR": 10,
    "AND": 20,
    ">": 30,
    ">=": 30,
    "<": 30,
    "<=": 30,
    "=": 30,
    "<>": 30,
    "+": 40,
    "-": 40,
    "*": 50,
    "/": 50,
}


@dataclass(frozen=True)
class ParsedFormula:
    ast: FormulaAst
    positions: dict[int, SourcePosition]
    number_text: dict[int, str]


class Parser:
    def __init__(self, source: str) -> None:
        self.tokens = tokenize(source)
        self.index = 0
        self.positions: dict[int, SourcePosition] = {}
        self.number_text: dict[int, str] = {}
        self.locals: set[str] = set()
        self.node_count = 0

    @property
    def current(self) -> Token:
        return self.tokens[self.index]

    def _fail(self, token: Token, code: str, message: str) -> None:
        raise FormulaError(ParseIssue(code=code, message=message, position=token.position))

    def _take(self) -> Token:
        token = self.current
        self.index += 1
        return token

    def _expect(self, value: str) -> Token:
        token = self.current
        if token.value != value:
            self._fail(token, "syntax", "公式写法有误，请检查括号和分号。")
        return self._take()

    def _node(self, node: Expr, position: SourcePosition) -> Expr:
        self.node_count += 1
        if self.node_count > MAX_AST_NODES:
            raise FormulaError(
                ParseIssue(code="limit", message="公式项太多，请删减后重试。", position=position)
            )
        if self._height(node) > MAX_AST_DEPTH:
            raise FormulaError(
                ParseIssue(code="limit", message="公式嵌套太深，请简化后重试。", position=position)
            )
        self.positions[id(node)] = position
        return node

    def _height(self, node: Expr) -> int:
        if isinstance(node, UnaryExpr):
            return self._height(node.operand) + 1
        if isinstance(node, BinaryExpr):
            return max(self._height(node.left), self._height(node.right)) + 1
        if isinstance(node, CallExpr):
            return max((self._height(arg) for arg in node.args), default=0) + 1
        return 1

    def parse(self) -> ParsedFormula:
        assignments: list[Assignment] = []
        while (
            self.current.kind == "identifier"
            and self.tokens[self.index + 1].value == ":="
        ):
            name_token = self._take()
            if len(assignments) >= MAX_ASSIGNMENTS:
                self._fail(name_token, "limit", "赋值太多，请删减后重试。")
            self._take()
            value = self._expression(0, 0)
            self._expect(";")
            assignment = Assignment(name=name_token.value, value=value)
            self.positions[id(assignment)] = name_token.position
            assignments.append(assignment)
            self.locals.add(name_token.value)

        output_name: str | None = None
        if self.current.kind == "identifier" and self.tokens[self.index + 1].value == ":":
            output_name = self._take().value
            self._take()
        output = self._expression(0, 0)
        if self.current.value == ";":
            self._take()
        if self.current.kind != "eof":
            self._fail(self.current, "syntax", "只保留最后一条选股结果，请检查分号。")
        ast = FormulaAst(assignments=assignments, output_name=output_name, output=output)
        return ParsedFormula(ast=ast, positions=self.positions, number_text=self.number_text)

    def _expression(self, min_power: int, depth: int) -> Expr:
        if depth > MAX_AST_DEPTH:
            self._fail(self.current, "limit", "公式嵌套太深，请简化后重试。")
        token = self._take()
        if token.kind == "number":
            left = self._node(NumberExpr(value=float(token.value)), token.position)
            self.number_text[id(left)] = token.value
        elif token.kind == "identifier":
            if token.value == "NOT":
                operand = self._expression(30, depth + 1)
                left = self._node(UnaryExpr(op="NOT", operand=operand), token.position)
            elif self.current.value == "(":
                self._take()
                args: list[Expr] = []
                if self.current.value != ")":
                    while True:
                        args.append(self._expression(0, depth + 1))
                        if self.current.value != ",":
                            break
                        self._take()
                self._expect(")")
                left = self._node(CallExpr(name=token.value, args=args), token.position)
            elif token.value in self.locals:
                left = self._node(LocalExpr(name=token.value), token.position)
            else:
                left = self._node(FieldExpr(name=token.value), token.position)
        elif token.value in {"+", "-"}:
            operand = self._expression(60, depth + 1)
            left = self._node(UnaryExpr(op=token.value, operand=operand), token.position)
        elif token.value == "(":
            left = self._expression(0, depth + 1)
            self._expect(")")
        else:
            self._fail(token, "syntax", "公式写法有误，请检查括号和条件。")

        while True:
            operator = self.current
            op = operator.value
            if operator.kind == "identifier" and op not in {"AND", "OR"}:
                break
            if op == "!=":
                op = "<>"
            power = _BINDING_POWER.get(op)
            if power is None or power < min_power:
                break
            self._take()
            right = self._expression(power + 1, depth + 1)
            left = self._node(BinaryExpr(op=op, left=left, right=right), operator.position)
        return left


def parse_syntax(source: str) -> ParsedFormula:
    return Parser(source).parse()
