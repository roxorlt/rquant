"""Versioned syntax tree and parse report for the restricted formula language."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

SYNTAX_VERSION = "tdx-v1"


class SourcePosition(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    line: int
    column: int
    offset: int


class NumberExpr(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["number"] = "number"
    value: float = Field(allow_inf_nan=False)


class FieldExpr(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["field"] = "field"
    name: str


class LocalExpr(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["local"] = "local"
    name: str


class UnaryExpr(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["unary"] = "unary"
    op: Literal["+", "-", "NOT"]
    operand: Expr


class BinaryExpr(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["binary"] = "binary"
    op: Literal["+", "-", "*", "/", ">", ">=", "<", "<=", "=", "<>", "AND", "OR"]
    left: Expr
    right: Expr


class CallExpr(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["call"] = "call"
    name: str
    args: list[Expr]


Expr = Annotated[
    NumberExpr | FieldExpr | LocalExpr | UnaryExpr | BinaryExpr | CallExpr,
    Field(discriminator="kind"),
]


class Assignment(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    value: Expr


class FormulaAst(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    syntax_version: Literal["tdx-v1"] = SYNTAX_VERSION
    assignments: list[Assignment]
    output_name: str | None
    output: Expr


class ParseIssue(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    code: Literal["syntax", "type", "range", "future", "limit", "name"]
    message: str
    position: SourcePosition


class UnsupportedItem(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["function", "field"]
    name: str
    position: SourcePosition
    message: str


class TranslationPlan(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    fields: list[str]
    functions: list[str]
    assignments: list[str]
    window_lookback_bars: int
    requires_full_history: bool


class ParseResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    syntax_version: Literal["tdx-v1"] = SYNTAX_VERSION
    status: Literal["parsed", "rejected"]
    ast: FormulaAst | None
    translation: TranslationPlan | None
    issues: list[ParseIssue]
    unsupported: list[UnsupportedItem]


UnaryExpr.model_rebuild()
BinaryExpr.model_rebuild()
CallExpr.model_rebuild()
Assignment.model_rebuild()
FormulaAst.model_rebuild()
