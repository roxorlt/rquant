"""A small, closed expression language over a daily panel (date × code).

    cs_rank(ts_delta(close, 5) / ts_std(close, 20))

Only the names and functions below exist; anything else is rejected before
evaluation, so an expression can be stored and re-run safely.
"""

from __future__ import annotations

import ast
from collections.abc import Callable, Mapping

import numpy as np
import pandas as pd

FIELDS = ("open", "high", "low", "close", "pre_close", "pct_chg", "vol", "amount")
MAX_WINDOW = 250


class FactorExpressionError(ValueError):
    pass


def _window(n: object) -> int:
    if not isinstance(n, int) or not 1 <= n <= MAX_WINDOW:
        raise FactorExpressionError(f"窗口须为 1..{MAX_WINDOW} 的整数")
    return n


def _ts_rank(x: pd.DataFrame, n: int) -> pd.DataFrame:
    return x.rolling(n, min_periods=n).rank(pct=True)


FUNCS: Mapping[str, tuple[int, Callable[..., pd.DataFrame]]] = {
    "ts_mean": (2, lambda x, n: x.rolling(n, min_periods=n).mean()),
    "ts_sum": (2, lambda x, n: x.rolling(n, min_periods=n).sum()),
    "ts_std": (2, lambda x, n: x.rolling(n, min_periods=n).std()),
    "ts_max": (2, lambda x, n: x.rolling(n, min_periods=n).max()),
    "ts_min": (2, lambda x, n: x.rolling(n, min_periods=n).min()),
    "ts_delta": (2, lambda x, n: x - x.shift(n)),
    "ts_rank": (2, _ts_rank),
    "delay": (2, lambda x, n: x.shift(n)),
    "cs_rank": (1, lambda x: x.rank(axis=1, pct=True)),
    "cs_zscore": (1, lambda x: x.sub(x.mean(axis=1), axis=0).div(x.std(axis=1), axis=0)),
    "log": (1, lambda x: np.log(x.where(x > 0))),
    "abs": (1, lambda x: x.abs()),
}

_BIN = {ast.Add: np.add, ast.Sub: np.subtract, ast.Mult: np.multiply, ast.Div: np.divide}


def validate(expression: str) -> ast.Expression:
    if len(expression) > 500:
        raise FactorExpressionError("表达式过长")
    try:
        tree = ast.parse(expression, mode="eval")
    except SyntaxError as exc:
        raise FactorExpressionError(f"语法错误：{exc.msg}") from exc
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            if not isinstance(node.func, ast.Name) or node.func.id not in FUNCS:
                raise FactorExpressionError("未知函数")
            arity = FUNCS[node.func.id][0]
            if len(node.args) != arity or node.keywords:
                raise FactorExpressionError(f"{node.func.id} 需要 {arity} 个参数")
            if arity == 2 and not (isinstance(node.args[1], ast.Constant)
                                   and isinstance(node.args[1].value, int)):
                raise FactorExpressionError(f"{node.func.id} 的窗口须为整数常量")
        elif isinstance(node, ast.Name):
            if node.id not in FIELDS and node.id not in FUNCS:
                raise FactorExpressionError(f"未知字段：{node.id}")
        elif not isinstance(node, (ast.Expression, ast.BinOp, ast.UnaryOp, ast.Constant,
                                   ast.Load, ast.USub, *_BIN)):
            raise FactorExpressionError(f"不支持的语法：{type(node).__name__}")
        if isinstance(node, ast.Constant) and not isinstance(node.value, (int, float)):
            raise FactorExpressionError("只允许数字常量")
    return tree


def evaluate(expression: str, panel: Mapping[str, pd.DataFrame]) -> pd.DataFrame:
    tree = validate(expression)

    def ev(node: ast.AST) -> object:
        if isinstance(node, ast.Expression):
            return ev(node.body)
        if isinstance(node, ast.Constant):
            return node.value
        if isinstance(node, ast.Name):
            return panel[node.id]
        if isinstance(node, ast.UnaryOp):
            return -ev(node.operand)  # type: ignore[operator]
        if isinstance(node, ast.BinOp):
            return _BIN[type(node.op)](ev(node.left), ev(node.right))
        assert isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        args = [ev(a) for a in node.args]
        if len(args) == 2:
            args[1] = _window(args[1])
        return FUNCS[node.func.id][1](*args)

    out = ev(tree)
    if not isinstance(out, pd.DataFrame):
        raise FactorExpressionError("表达式须引用至少一个字段")
    return out.replace([np.inf, -np.inf], np.nan)
