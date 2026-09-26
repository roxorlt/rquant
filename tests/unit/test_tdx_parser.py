"""Untrusted Tongdaxin text must stay inside a bounded, parse-only contract."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from rquant.screen.tdx import parse_formula
from rquant.screen.tdx.ast import NumberExpr


def test_common_formula_normalizes_names_and_tracks_dependencies() -> None:
    result = parse_formula(
        "fast:=ma(close,5); slow:=ema(CLOSE,20); "
        "xg:cross(fast,slow) AND CLOSE>REF(CLOSE,1);"
    )

    assert result.status == "parsed"
    assert result.syntax_version == "tdx-v1"
    assert result.ast is not None
    assert [item.name for item in result.ast.assignments] == ["FAST", "SLOW"]
    assert result.ast.output_name == "XG"
    assert result.ast.output.kind == "binary"
    assert result.ast.output.op == "AND"
    assert result.translation is not None
    assert result.translation.fields == ["CLOSE"]
    assert result.translation.functions == ["MA", "EMA", "CROSS", "REF"]
    assert result.translation.requires_full_history is True
    assert result.issues == []
    assert result.unsupported == []


def test_precedence_and_boolean_assignment_reuse() -> None:
    result = parse_formula("A:=CLOSE>OPEN; A OR HIGH>LOW AND VOL>0")

    assert result.status == "parsed"
    assert result.ast is not None
    output = result.ast.output
    assert output.kind == "binary" and output.op == "OR"
    assert output.right.kind == "binary" and output.right.op == "AND"
    assert output.left.kind == "local" and output.left.name == "A"


def test_chinese_local_labels_are_allowed_without_becoming_market_fields() -> None:
    result = parse_formula("快线:=MA(CLOSE,5); 选股:CLOSE>快线")

    assert result.status == "parsed"
    assert result.ast is not None
    assert result.ast.assignments[0].name == "快线"
    assert result.ast.output_name == "选股"
    assert result.translation is not None
    assert result.translation.fields == ["CLOSE"]


def test_all_supported_functions_and_market_fields_are_explicitly_recognized() -> None:
    formula = (
        "A:=SMA(CLOSE,5,2)+HHV(HIGH,5)-LLV(LOW,5)+SUM(VOL,5)+AMOUNT;"
        "B:=IF(OPEN>0,A,0);"
        "XG:EVERY(CLOSE>0,3) AND EXIST(COUNT(CLOSE>REF(CLOSE,1),5)>0,5) "
        "AND BARSLAST(CROSS(B,MA(CLOSE,3)))>=0 "
        "AND NOT(OR(CLOSE<0,AND(VOL<0,AMOUNT<0)))"
    )
    result = parse_formula(formula)

    assert result.status == "parsed", result.issues
    assert result.translation is not None
    assert result.translation.fields == ["CLOSE", "HIGH", "LOW", "VOL", "AMOUNT", "OPEN"]
    assert set(result.translation.functions) == {
        "SMA", "HHV", "LLV", "SUM", "IF", "EVERY", "EXIST", "COUNT",
        "REF", "BARSLAST", "CROSS", "MA", "NOT", "OR", "AND",
    }


def test_unknown_functions_and_fields_are_reported_individually_with_locations() -> None:
    result = parse_formula("MACD(CLOSE,12,26)>DYNAINFO(7) AND CLOSE>UNKNOWN")

    assert result.status == "rejected"
    assert [(item.kind, item.name) for item in result.unsupported] == [
        ("function", "MACD"),
        ("function", "DYNAINFO"),
        ("field", "UNKNOWN"),
    ]
    assert [(item.position.line, item.position.column) for item in result.unsupported] == [
        (1, 1), (1, 19), (1, 41),
    ]
    assert result.translation is None
    assert all("暂不支持" in item.message for item in result.unsupported)


@pytest.mark.parametrize(
    ("source", "code"),
    [
        ("REF(CLOSE,-1)>0", "future"),
        ("MA(CLOSE,0)>0", "range"),
        ("EMA(CLOSE,501)>0", "limit"),
        ("SMA(CLOSE,5,6)>0", "range"),
        ("REF(MA(CLOSE,400),200)>0", "limit"),
        ("MA(CLOSE,1.5)>0", "range"),
        ("MA(CLOSE,500.00000000000000001)>0", "range"),
        ("MA(CLOSE,VOL)>0", "range"),
        ("CLOSE/0>1", "range"),
        ("CLOSE/(0." + "0" * 400 + "1)>1", "range"),
        ("CLOSE + 1", "type"),
        ("CLOSE AND OPEN", "type"),
        ("A:=CLOSE; A", "type"),
        ("CLOSE:=1; CLOSE>0", "name"),
        ("A:=CLOSE; A:=OPEN; A>0", "name"),
    ],
)
def test_static_validation_rejects_unsafe_or_non_boolean_formula(source: str, code: str) -> None:
    result = parse_formula(source)

    assert result.status == "rejected"
    assert any(issue.code == code for issue in result.issues), result.issues
    assert all(issue.position.line >= 1 and issue.position.column >= 1 for issue in result.issues)


@pytest.mark.parametrize(
    "source",
    [
        "__import__('os').system('true')",
        "CLOSE.__class__>0",
        "CLOSE[0]>0",
        "CLOSE>0 # comment",
        "CLOSE>0; OPEN>0",
        "A:=CLOSE>0;",
    ],
)
def test_unsupported_syntax_and_code_like_text_are_rejected(source: str) -> None:
    result = parse_formula(source)

    assert result.status == "rejected"
    assert result.issues
    assert result.translation is None


def test_byte_token_assignment_depth_and_node_budgets_reject_explicitly() -> None:
    samples = [
        ("中" * 1400, "limit"),
        ("CLOSE+" * 600 + "OPEN>0", "limit"),
        (";".join(f"A{i}:=CLOSE" for i in range(33)) + ";CLOSE>0", "limit"),
        ("(" * 40 + "CLOSE>0" + ")" * 40, "limit"),
        ("CLOSE+" * 40 + "OPEN>0", "limit"),
        ("CLOSE+" * 260 + "OPEN>0", "limit"),
    ]
    for source, code in samples:
        result = parse_formula(source)
        assert result.status == "rejected"
        assert any(issue.code == code for issue in result.issues), source[:30]


def test_case_and_whitespace_do_not_change_canonical_ast() -> None:
    first = parse_formula("x:=MA(CLOSE,5);X>OPEN")
    second = parse_formula(" X := ma( close , 5 ) ; x > open ")

    assert first.status == second.status == "parsed"
    assert first.ast == second.ast


def test_translation_includes_history_required_by_every_assignment() -> None:
    result = parse_formula("A:=EMA(CLOSE,20); OPEN>0")

    assert result.status == "parsed"
    assert result.translation is not None
    assert result.translation.window_lookback_bars == 19
    assert result.translation.requires_full_history is True


def test_unpaired_unicode_is_a_positioned_rejection() -> None:
    result = parse_formula("CLOSE>\ud800")

    assert result.status == "rejected"
    assert result.issues[0].code == "syntax"


@pytest.mark.parametrize(
    ("source", "column"),
    [(".7.>0", 3), (".５>0", 1)],
)
def test_malformed_decimal_is_rejected_at_its_position(source: str, column: int) -> None:
    result = parse_formula(source)

    assert result.status == "rejected"
    assert result.issues[0].code == "syntax"
    assert result.issues[0].position.column == column
    assert "修改" in result.issues[0].message


def test_original_whitespace_counts_toward_source_byte_limit() -> None:
    result = parse_formula(" " * 4097)

    assert result.status == "rejected"
    assert result.issues[0].code == "limit"
    assert "太长" in result.issues[0].message


def test_ast_rejects_non_finite_numbers_and_extra_fields() -> None:
    with pytest.raises(ValidationError):
        NumberExpr(value=float("inf"))
    with pytest.raises(ValidationError):
        NumberExpr.model_validate({"kind": "number", "value": 1, "python": "hidden"})
