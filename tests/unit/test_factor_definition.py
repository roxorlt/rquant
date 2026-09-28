"""Restricted factor definitions stay declarative and independent of runtime state."""

from __future__ import annotations

import subprocess
import sys
from datetime import date
from pathlib import Path

import pytest
from pydantic import ValidationError

from rquant.factor.definition import FactorDefinition, build_factor_definition
from rquant.factor.expression import (
    MAX_AST_NODES,
    MAX_EXPRESSION_LENGTH,
    MAX_FEATURE_COLUMNS,
    MAX_MAD_MULTIPLE,
    MAX_OFFSET,
    MAX_WINDOW,
    FactorExpressionError,
    FeatureCatalog,
    parse_factor_expression,
)


def _catalog() -> FeatureCatalog:
    return FeatureCatalog(columns=("volume", "close", "industry"))


def _definition(
    expression: str = "ts_mean(close, 5) / ref(volume, 2)",
    *,
    catalog: FeatureCatalog | None = None,
    dependency_columns: tuple[str, ...] | None = None,
) -> FactorDefinition:
    return build_factor_definition(
        factor_id="price_volume_1",
        name_zh="价量强度",
        category="technical",
        direction="higher_is_better",
        version=1,
        earliest_available_date=date(2024, 1, 2),
        expression=expression,
        feature_catalog=catalog or _catalog(),
        dependency_columns=dependency_columns,
    )


def test_valid_expression_normalizes_and_derives_sorted_dependencies_and_history() -> None:
    result = parse_factor_expression(
        "  cs_rank(ts_mean(ref(close, 2), 5) / volume) >= ts_corr(close, volume, 4)  ",
        _catalog(),
    )

    assert result.expression == (
        "cs_rank(ts_mean(ref(close, 2), 5) / volume) >= ts_corr(close, volume, 4)"
    )
    assert result.dependency_columns == ("close", "volume")
    assert result.max_history_window == 7
    assert result == parse_factor_expression(result.expression, _catalog())


@pytest.mark.parametrize(
    ("expression", "history"),
    [
        ("+close - -volume * 2", 1),
        ("cs_zscore(close)", 1),
        ("cs_winsorize(close, 3)", 1),
        ("industry_neutralize(close)", 1),
        ("size_neutralize(close)", 1),
        ("ts_std(close, 5)", 5),
        ("ts_rank(close, 5)", 5),
        ("ts_delta(close, 5)", 6),
        ("ref(close, 0)", 1),
        ("ts_corr(ref(close, 2), volume, 5)", 7),
        ("close < volume", 1),
        ("close <= volume", 1),
        ("close > volume", 1),
        ("close >= volume", 1),
    ],
)
def test_allowed_operators_have_deterministic_history(expression: str, history: int) -> None:
    assert parse_factor_expression(expression, _catalog()).max_history_window == history


@pytest.mark.parametrize(
    "columns",
    [
        ("close", "close"),
        ("close", " close"),
        ("close", ""),
        ("close", "price.close"),
        ("close", "__class__"),
        ("close", 1),
        tuple(f"feature_{index}" for index in range(MAX_FEATURE_COLUMNS + 1)),
    ],
)
def test_catalog_rejects_duplicate_invalid_or_oversized_columns(
    columns: tuple[object, ...],
) -> None:
    with pytest.raises(ValidationError):
        FeatureCatalog(columns=columns)


@pytest.mark.parametrize(
    ("expression", "reason"),
    [
        ("", "expression_empty"),
        ("   ", "expression_empty"),
        ("close +", "invalid_syntax"),
        ("missing + close", "unknown_feature"),
        ("close.real", "unsupported_syntax"),
        ("close[0]", "unsupported_syntax"),
        ("[close]", "unsupported_syntax"),
        ("{'x': close}", "unsupported_syntax"),
        ("[x for x in (close,)]", "unsupported_syntax"),
        ("(lambda x: x)(close)", "unsupported_syntax"),
        ("(x := close)", "unsupported_syntax"),
        ("close and volume", "unsupported_syntax"),
        ("close == volume", "unsupported_syntax"),
        ("close < volume < close", "unsupported_syntax"),
        ("(close > volume) + 1", "unsupported_syntax"),
        ("ts_mean(close > 0, 5)", "unsupported_syntax"),
        ("close ** 2", "unsupported_syntax"),
        ("close // 2", "unsupported_syntax"),
        ("close if True else volume", "unsupported_syntax"),
        ("(close, volume)", "unsupported_syntax"),
        ("'close'", "invalid_constant"),
        ("True", "invalid_constant"),
        ("1e309 + close", "invalid_constant"),
        ("unknown(close)", "unknown_function"),
        ("cs_rank(close, volume)", "invalid_arguments"),
        ("ts_mean(close, window=5)", "invalid_arguments"),
        ("cs_rank(*(close,))", "invalid_arguments"),
        ("ts_mean(close, 2.0)", "invalid_window"),
        ("ts_mean(close, 0)", "invalid_window"),
        (f"ts_mean(close, {MAX_WINDOW + 1})", "invalid_window"),
        ("ts_delta(close, -1)", "invalid_window"),
        ("ref(close, -1)", "invalid_offset"),
        (f"ref(close, {MAX_OFFSET + 1})", "invalid_offset"),
        ("cs_winsorize(close, 0)", "invalid_mad_multiple"),
        (f"cs_winsorize(close, {MAX_MAD_MULTIPLE + 1})", "invalid_mad_multiple"),
        ("cs_winsorize(close, 1e309)", "invalid_mad_multiple"),
    ],
)
def test_expression_rejects_unsafe_or_invalid_syntax_with_stable_reason(
    expression: str, reason: str
) -> None:
    with pytest.raises(FactorExpressionError) as error:
        parse_factor_expression(expression, _catalog())
    assert error.value.reason == reason


def test_expression_maps_unpaired_surrogate_to_domain_reason() -> None:
    with pytest.raises(FactorExpressionError) as error:
        parse_factor_expression("close\ud800", FeatureCatalog(columns=("close",)))
    assert error.value.reason == "invalid_syntax"
    assert error.value.__cause__ is None


def test_expression_rejects_input_budgets_before_normalization() -> None:
    expressions = (
        ("x" * (MAX_EXPRESSION_LENGTH + 1), "expression_too_long"),
        ("+".join(["close"] * (MAX_AST_NODES + 1)), "expression_too_complex"),
    )
    for expression, reason in expressions:
        with pytest.raises(FactorExpressionError) as error:
            parse_factor_expression(expression, _catalog())
        assert error.value.reason == reason
    with pytest.raises(FactorExpressionError) as error:
        parse_factor_expression("cs_rank(" * 30 + "close" + ")" * 30, _catalog())
    assert error.value.reason == "expression_too_complex"


def test_definition_binds_catalog_and_declared_dependencies() -> None:
    definition = _definition(dependency_columns=("close", "volume"))

    assert definition.factor_id == "price_volume_1"
    assert definition.name_zh == "价量强度"
    assert definition.category == "technical"
    assert definition.direction == "higher_is_better"
    assert definition.version == 1
    assert definition.earliest_available_date == date(2024, 1, 2)
    assert definition.expression == "ts_mean(close, 5) / ref(volume, 2)"
    assert definition.dependency_columns == ("close", "volume")
    assert definition.max_history_window == 5
    assert definition.feature_catalog == _catalog()
    with pytest.raises(ValidationError):
        definition.factor_id = "other"


@pytest.mark.parametrize(
    "dependency_columns",
    [("close",), ("close", "close"), ("close", " volume"), ("volume", "close")],
)
def test_definition_rejects_mismatched_or_bad_dependency_claim(
    dependency_columns: tuple[str, ...],
) -> None:
    with pytest.raises(FactorExpressionError) as error:
        _definition(dependency_columns=dependency_columns)
    assert error.value.reason == "dependency_mismatch"


def test_definition_rejects_feature_removed_from_catalog() -> None:
    old = _definition()
    assert old.dependency_columns == ("close", "volume")
    with pytest.raises(FactorExpressionError) as error:
        _definition(catalog=FeatureCatalog(columns=("close",)))
    assert error.value.reason == "unknown_feature"


def test_direct_definition_validation_rechecks_expression_and_catalog() -> None:
    definition = _definition()
    payload = definition.model_dump()
    payload["expression"] = "close + missing"
    with pytest.raises(ValidationError):
        FactorDefinition.model_validate(payload)
    payload = definition.model_dump()
    payload["dependency_columns"] = ("close",)
    with pytest.raises(ValidationError):
        FactorDefinition.model_validate(payload)


def test_parser_never_executes_user_expression(tmp_path: Path) -> None:
    marker = tmp_path / "executed"
    expression = f"__import__('pathlib').Path({str(marker)!r}).touch()"
    with pytest.raises(FactorExpressionError):
        parse_factor_expression(expression, _catalog())
    assert not marker.exists()


def test_factor_package_does_not_import_paper_signal_write_path() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import rquant.factor; "
            "assert 'rquant.signal_provenance' not in sys.modules; "
            "assert 'rquant.storage.duckdb' not in sys.modules",
        ],
        cwd=Path(__file__).resolve().parents[2],
        env={
            "PYTHONPATH": "src",
            "PYTHONDONTWRITEBYTECODE": "1",
            "RQUANT_DISABLE_DOTENV": "1",
        },
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
