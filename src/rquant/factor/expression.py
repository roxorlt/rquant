"""Validate and normalize a bounded factor expression without evaluating it.

History is counted in input rows including the current row: a five-row rolling
window needs five rows, while ``ref(x, 2)`` and ``ts_delta(x, 2)`` need three.
"""

from __future__ import annotations

import ast
import math
import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, field_validator
from pydantic_core import PydanticCustomError

MAX_FEATURE_COLUMNS = 128
MAX_EXPRESSION_LENGTH = 2048
MAX_AST_NODES = 256
MAX_AST_DEPTH = 24
MAX_WINDOW = 252
MAX_OFFSET = 252
MIN_MAD_MULTIPLE = 0.5
MAX_MAD_MULTIPLE = 10.0
MAX_LITERAL_ABS = 1_000_000_000_000

_COLUMN_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9_]{0,63}\Z")
_CROSS_SECTIONAL_UNARY = frozenset(
    {"cs_rank", "cs_zscore", "industry_neutralize", "size_neutralize"}
)
_ROLLING_UNARY = frozenset({"ts_mean", "ts_std", "ts_delta", "ts_rank"})
_TWO_ARG_FUNCTIONS = _ROLLING_UNARY | {"cs_winsorize", "ref"}
_FUNCTIONS = _CROSS_SECTIONAL_UNARY | _TWO_ARG_FUNCTIONS | {"ts_corr"}

FactorExpressionReason = Literal[
    "invalid_catalog",
    "invalid_expression",
    "expression_empty",
    "expression_too_long",
    "expression_too_complex",
    "invalid_syntax",
    "unsupported_syntax",
    "invalid_constant",
    "unknown_feature",
    "unknown_function",
    "invalid_arguments",
    "invalid_window",
    "invalid_offset",
    "invalid_mad_multiple",
    "dependency_mismatch",
]


class FactorExpressionError(ValueError):
    """Stable domain reason for an invalid declarative expression."""

    def __init__(self, reason: FactorExpressionReason) -> None:
        self.reason = reason
        super().__init__(reason)


class FeatureCatalog(BaseModel):
    """A frozen, bounded snapshot of names the caller permits in one definition."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    columns: tuple[str, ...]

    @field_validator("columns")
    @classmethod
    def _validate_columns(cls, columns: tuple[str, ...]) -> tuple[str, ...]:
        if len(columns) > MAX_FEATURE_COLUMNS:
            raise PydanticCustomError("catalog_too_large", "feature catalog is too large")
        if any(_COLUMN_PATTERN.fullmatch(column) is None for column in columns):
            raise PydanticCustomError(
                "catalog_invalid_column", "feature catalog has an invalid column"
            )
        if len(set(columns)) != len(columns):
            raise PydanticCustomError("catalog_duplicate", "feature catalog has duplicate columns")
        return tuple(sorted(columns))


class ParsedFactorExpression(BaseModel):
    """Only portable values cross the parser boundary; no Python AST escapes."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    expression: str
    dependency_columns: tuple[str, ...]
    max_history_window: int


def _check_budget(tree: ast.Expression) -> None:
    pending: list[tuple[ast.AST, int]] = [(tree, 1)]
    count = 0
    while pending:
        node, depth = pending.pop()
        count += 1
        if count > MAX_AST_NODES or depth > MAX_AST_DEPTH:
            raise FactorExpressionError("expression_too_complex")
        pending.extend((child, depth + 1) for child in ast.iter_child_nodes(node))


def _numeric_literal(node: ast.AST, reason: FactorExpressionReason) -> int | float:
    sign = 1
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.UAdd, ast.USub)):
        sign = -1 if isinstance(node.op, ast.USub) else 1
        node = node.operand
    if not isinstance(node, ast.Constant) or type(node.value) not in (int, float):
        raise FactorExpressionError(reason)
    value = node.value
    if (isinstance(value, float) and not math.isfinite(value)) or abs(value) > MAX_LITERAL_ABS:
        raise FactorExpressionError(reason)
    return sign * value


def _integer_argument(
    node: ast.AST, reason: FactorExpressionReason, *, minimum: int, maximum: int
) -> int:
    value = _numeric_literal(node, reason)
    if type(value) is not int or not minimum <= value <= maximum:
        raise FactorExpressionError(reason)
    return value


def _inspect(node: ast.AST, columns: frozenset[str], dependencies: set[str]) -> int:
    if isinstance(node, ast.Constant):
        _numeric_literal(node, "invalid_constant")
        return 1
    if isinstance(node, ast.Name):
        if node.id not in columns:
            raise FactorExpressionError("unknown_feature")
        dependencies.add(node.id)
        return 1
    if isinstance(node, ast.BinOp):
        if not isinstance(node.op, (ast.Add, ast.Sub, ast.Mult, ast.Div)):
            raise FactorExpressionError("unsupported_syntax")
        return max(
            _inspect(node.left, columns, dependencies),
            _inspect(node.right, columns, dependencies),
        )
    if isinstance(node, ast.UnaryOp):
        if not isinstance(node.op, (ast.UAdd, ast.USub)):
            raise FactorExpressionError("unsupported_syntax")
        return _inspect(node.operand, columns, dependencies)
    if isinstance(node, ast.Compare):
        if len(node.ops) != 1 or not isinstance(node.ops[0], (ast.Lt, ast.LtE, ast.Gt, ast.GtE)):
            raise FactorExpressionError("unsupported_syntax")
        return max(
            _inspect(node.left, columns, dependencies),
            _inspect(node.comparators[0], columns, dependencies),
        )
    if isinstance(node, ast.Call):
        if not isinstance(node.func, ast.Name):
            raise FactorExpressionError("unsupported_syntax")
        name = node.func.id
        if name not in _FUNCTIONS:
            raise FactorExpressionError("unknown_function")
        if node.keywords or any(isinstance(arg, ast.Starred) for arg in node.args):
            raise FactorExpressionError("invalid_arguments")
        expected = 3 if name == "ts_corr" else 2 if name in _TWO_ARG_FUNCTIONS else 1
        if len(node.args) != expected:
            raise FactorExpressionError("invalid_arguments")
        if name in _CROSS_SECTIONAL_UNARY:
            return _inspect(node.args[0], columns, dependencies)
        if name == "cs_winsorize":
            multiple = _numeric_literal(node.args[1], "invalid_mad_multiple")
            if not MIN_MAD_MULTIPLE <= multiple <= MAX_MAD_MULTIPLE:
                raise FactorExpressionError("invalid_mad_multiple")
            return _inspect(node.args[0], columns, dependencies)
        if name == "ref":
            offset = _integer_argument(
                node.args[1], "invalid_offset", minimum=0, maximum=MAX_OFFSET
            )
            return _inspect(node.args[0], columns, dependencies) + offset
        window = _integer_argument(node.args[-1], "invalid_window", minimum=1, maximum=MAX_WINDOW)
        if name == "ts_corr":
            history = max(
                _inspect(node.args[0], columns, dependencies),
                _inspect(node.args[1], columns, dependencies),
            )
        else:
            history = _inspect(node.args[0], columns, dependencies)
        return history + window if name == "ts_delta" else history + window - 1
    raise FactorExpressionError("unsupported_syntax")


def parse_factor_expression(
    expression: str, feature_catalog: FeatureCatalog
) -> ParsedFactorExpression:
    """Return canonical syntax, exact feature dependencies, and required input bars."""
    if not isinstance(feature_catalog, FeatureCatalog):
        raise FactorExpressionError("invalid_catalog")
    if not isinstance(expression, str):
        raise FactorExpressionError("invalid_expression")
    if not expression.strip():
        raise FactorExpressionError("expression_empty")
    if len(expression) > MAX_EXPRESSION_LENGTH:
        raise FactorExpressionError("expression_too_long")
    try:
        tree = ast.parse(expression.strip(), mode="eval")
    except SyntaxError as error:
        raise FactorExpressionError("invalid_syntax") from error
    except RecursionError as error:
        raise FactorExpressionError("expression_too_complex") from error
    _check_budget(tree)
    if any(isinstance(node, ast.Compare) and node is not tree.body for node in ast.walk(tree.body)):
        raise FactorExpressionError("unsupported_syntax")
    dependencies: set[str] = set()
    history = _inspect(tree.body, frozenset(feature_catalog.columns), dependencies)
    normalized = ast.unparse(tree)
    if len(normalized) > MAX_EXPRESSION_LENGTH:
        raise FactorExpressionError("expression_too_long")
    return ParsedFactorExpression(
        expression=normalized,
        dependency_columns=tuple(sorted(dependencies)),
        max_history_window=history,
    )
