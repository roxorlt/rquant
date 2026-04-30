"""RuleSpec + REGISTRY 测试。"""

import pytest
from pydantic import BaseModel, ValidationError

from rquant.llm.registry import (
    REGISTRY,
    REGISTRY_BY_NAME,
    RuleSpec,
    get_rule_spec,
)


class TestRegistryStructure:
    def test_registry_not_empty(self) -> None:
        assert len(REGISTRY) >= 5  # 第一轮至少 5 条

    def test_registry_by_name_lookup(self) -> None:
        assert "not_st" in REGISTRY_BY_NAME
        assert isinstance(REGISTRY_BY_NAME["not_st"], RuleSpec)

    def test_get_rule_spec_known(self) -> None:
        spec = get_rule_spec("not_st")
        assert spec.name == "not_st"
        assert spec.category == "filter"

    def test_get_rule_spec_unknown_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown rule"):
            get_rule_spec("nonexistent_rule")

    def test_registry_names_unique(self) -> None:
        names = [s.name for s in REGISTRY]
        assert len(names) == len(set(names))


class TestArgsModelValidation:
    def test_no_args_rule(self) -> None:
        spec = get_rule_spec("not_st")
        # _NoArgs 接受空 dict
        validated = spec.args_model.model_validate({})
        assert validated.model_dump() == {}

    def test_no_args_rule_rejects_extra(self) -> None:
        spec = get_rule_spec("not_st")
        with pytest.raises(ValidationError):
            spec.args_model.model_validate({"foo": "bar"})

    def test_circ_mv_lt_args(self) -> None:
        spec = get_rule_spec("circ_mv_lt")
        validated = spec.args_model.model_validate({"threshold_yi": 100})
        assert validated.threshold_yi == 100  # type: ignore[attr-defined]

    def test_circ_mv_lt_rejects_negative(self) -> None:
        spec = get_rule_spec("circ_mv_lt")
        with pytest.raises(ValidationError):
            spec.args_model.model_validate({"threshold_yi": -10})


class TestRuleSpecCallable:
    def test_each_rule_instantiates_with_default_args(self) -> None:
        """每条 RuleSpec.fn(**args_model().model_dump()) 应能产出 callable Rule。"""
        for spec in REGISTRY:
            try:
                args = spec.args_model.model_construct().model_dump(exclude_unset=True)
                # Required-only fields can't be defaulted; skip those rules
                if any(f.is_required() for f in spec.args_model.model_fields.values()):
                    continue
                rule = spec.fn(**args)
                assert callable(rule), f"{spec.name} produced non-callable"
            except Exception as e:
                pytest.fail(f"{spec.name} failed: {e}")
