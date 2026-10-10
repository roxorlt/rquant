"""Translate a subset of 通达信 (TDX) formulas into the factor expression language.

Supported: C/O/H/L/V/CLOSE/OPEN/HIGH/LOW/VOL/AMOUNT, MA/REF/HHV/LLV/SUM/STD/ABS,
+ − × ÷, > >= < <= = <>, AND/OR/NOT, and a final condition line. ``X:=expr;``
assignments are inlined. Anything else raises ``TdxFormulaError`` with the
offending token, so nothing unrecognised is silently dropped.
"""

from __future__ import annotations

import re

from rquant.factor.expr import validate

FIELD = {"C": "close", "CLOSE": "close", "O": "open", "OPEN": "open", "H": "high",
         "HIGH": "high", "L": "low", "LOW": "low", "V": "vol", "VOL": "vol",
         "AMOUNT": "amount", "AMO": "amount"}
FUNC = {"MA": "ts_mean", "REF": "delay", "HHV": "ts_max", "LLV": "ts_min",
        "SUM": "ts_sum", "STD": "ts_std", "ABS": "abs"}
WORD = {"AND": " and ", "OR": " or ", "NOT": " not "}

_TOKEN = re.compile(r"\s*(?:(\d+\.?\d*)|([A-Za-z_][A-Za-z0-9_]*)|(<>|>=|<=|:=|[-+*/(),<>=;:]))")


class TdxFormulaError(ValueError):
    pass


def _tokens(text: str) -> list[str]:
    out, pos = [], 0
    text = text.strip()
    while pos < len(text):
        m = _TOKEN.match(text, pos)
        if not m or m.end() == pos:
            raise TdxFormulaError(f"无法识别：{text[pos:pos + 10]}")
        out.append(next(g for g in m.groups() if g is not None))
        pos = m.end()
    return out


def translate(formula: str) -> str:
    """Return a validated expression string; the last statement is the condition."""
    statements = [s for s in re.split(r";|\n", formula) if s.strip()]
    if not statements:
        raise TdxFormulaError("公式为空")
    names: dict[str, str] = {}
    result = ""
    for statement in statements:
        toks = _tokens(statement)
        target = None
        if len(toks) > 2 and toks[1] in (":=", ":") and re.match(r"^[A-Za-z_]", toks[0]):
            target, toks = toks[0].upper(), toks[2:]
        parts: list[str] = []
        for tok in toks:
            up = tok.upper()
            if re.match(r"^\d", tok):
                parts.append(tok)
            elif up in names:
                parts.append(f"({names[up]})")
            elif up in FIELD:
                parts.append(FIELD[up])
            elif up in FUNC:
                parts.append(FUNC[up])
            elif up in WORD:
                parts.append(WORD[up])
            elif tok == "=":
                parts.append("==")
            elif tok == "<>":
                parts.append("!=")
            elif tok in "+-*/(),<>" or tok in (">=", "<="):
                parts.append(tok)
            else:
                raise TdxFormulaError(f"不支持：{tok}")
        expr = "".join(parts).strip()
        if target:
            names[target] = expr
        result = expr
    try:
        validate(result)
    except ValueError as exc:
        raise TdxFormulaError(str(exc)) from exc
    return result
