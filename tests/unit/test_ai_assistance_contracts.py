"""Typed AI bindings preserve owner, account window and unknown measured usage."""

from __future__ import annotations

import importlib
from datetime import UTC, date, datetime
from decimal import Decimal
from types import ModuleType
from typing import TYPE_CHECKING
from uuid import UUID

if TYPE_CHECKING:
    from rquant.ai_assistance_contracts import AIRequestBinding

import pytest
from pydantic import ValidationError


def contracts() -> ModuleType:
    return importlib.import_module("rquant.ai_assistance_contracts")


def binding(**changes: object) -> AIRequestBinding:
    values = dict(
        owner_uid="alice",
        request_id=UUID("44af75e7-12ae-43fd-970a-b602668d0c41"),
        request_body_sha256="a" * 64,
        purpose="interpretation",
        account_id="account-main",
        model_id="model-a",
        template_version="interpretation-v1",
        context_sha256="b" * 64,
        reserved_at=datetime(2026, 10, 5, 16, tzinfo=UTC),
        budget_date=date(2026, 10, 6),
    )
    values.update(changes)
    return contracts().AIRequestBinding(**values)


def test_request_preserves_shanghai_reservation_day_and_old_model() -> None:
    original = binding()
    assert original.budget_date == date(2026, 10, 6)
    assert original.reserved_at == datetime(2026, 10, 5, 16, tzinfo=UTC)
    changed = binding(model_id="model-b", template_version="interpretation-v2")
    assert original.account_budget_key == changed.account_budget_key
    assert original.content_binding_sha256 != changed.content_binding_sha256
    with pytest.raises(ValidationError):
        original.model_id = "model-b"


@pytest.mark.parametrize(
    "changes",
    [
        {"budget_date": date(2026, 10, 5)},
        {"reserved_at": datetime(2026, 10, 6)},
        {"owner_uid": " alice "},
        {"path": "/tmp/private"},
        {"context_sha256": "bad"},
    ],
)
def test_request_rejects_wrong_day_naive_time_ambiguous_owner_and_extra_fields(
    changes: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        binding(**changes)


def test_content_binding_is_private_while_account_budget_is_shared() -> None:
    assert binding().account_budget_key == binding(owner_uid="bob").account_budget_key
    assert binding().content_binding_sha256 != binding(owner_uid="bob").content_binding_sha256
    assert binding().account_budget_key != binding(account_id="another-account").account_budget_key


def test_unknown_usage_is_not_zero_and_partial_usage_stays_unknown() -> None:
    usage = contracts().AIMeasuredUsage(input_tokens=None, output_tokens=None)
    assert usage.total_tokens is None
    assert usage.known is False
    partial = contracts().AIMeasuredUsage(input_tokens=17, output_tokens=None)
    assert partial.input_tokens == 17 and partial.total_tokens is None
    known = contracts().AIMeasuredUsage(input_tokens=17, output_tokens=3)
    assert known.total_tokens == 20 and known.known is True


@pytest.mark.parametrize("value", [True, -1, "3", 1.5, 2**63])
def test_usage_rejects_invalid_counter(value: object) -> None:
    with pytest.raises(ValidationError):
        contracts().AIMeasuredUsage(input_tokens=value, output_tokens=0)


def test_fact_rendering_keeps_fraction_percent_unit_unknown_and_exact_date() -> None:
    model = contracts().AISealedFact
    common = dict(source_path="performance.summary.total_return", source_sha256="c" * 64)
    percent = model(
        fact_id="summary.total_return",
        label="累计收益",
        kind="number",
        value=Decimal("0.125"),
        unit="%",
        **common,
    )
    assert percent.display_value == "12.50%"
    unknown = model(fact_id="summary.sharpe", label="夏普比率", kind="number", value=None, **common)
    assert unknown.display_value == "未知"
    period = model(
        fact_id="period.start", label="开始日期", kind="date", value=date(2026, 10, 6), **common
    )
    assert period.display_value == "2026-10-06"
    assert model.model_validate_json(percent.model_dump_json()) == percent


@pytest.mark.parametrize("value", [True, Decimal("NaN"), Decimal("Infinity"), Decimal("1e100000")])
def test_fact_rejects_non_finite_boolean_and_unbounded_number(value: object) -> None:
    with pytest.raises(ValidationError):
        contracts().AISealedFact(
            fact_id="summary.return",
            label="收益",
            kind="number",
            value=value,
            source_path="performance.summary.total_return",
            source_sha256="c" * 64,
        )


def test_result_context_hash_binds_all_sealed_identities_owner_model_and_template() -> None:
    c = contracts()
    values = dict(
        owner_uid="alice",
        source_kind="portfolio",
        job_id=UUID("44af75e7-12ae-43fd-970a-b602668d0c41"),
        spec_sha256="a" * 64,
        manifest_sha256="b" * 64,
        result_sha256="c" * 64,
    )
    original = c.AISealedResultBinding(**values)
    context = c.AIInterpretationBinding(
        result=original, facts_sha256="d" * 64, model_id="model-a", template_version="v1"
    )
    for field in ("spec_sha256", "manifest_sha256", "result_sha256"):
        changed = c.AISealedResultBinding(**(values | {field: "e" * 64}))
        assert (
            context.cache_key
            != c.AIInterpretationBinding(
                result=changed, facts_sha256="d" * 64, model_id="model-a", template_version="v1"
            ).cache_key
        )
    assert (
        context.cache_key
        != c.AIInterpretationBinding(
            result=c.AISealedResultBinding(**(values | {"owner_uid": "bob"})),
            facts_sha256="d" * 64,
            model_id="model-a",
            template_version="v1",
        ).cache_key
    )
    assert (
        context.cache_key
        != c.AIInterpretationBinding(
            result=original, facts_sha256="d" * 64, model_id="model-b", template_version="v1"
        ).cache_key
    )
    assert (
        context.cache_key
        != c.AIInterpretationBinding(
            result=original, facts_sha256="d" * 64, model_id="model-a", template_version="v2"
        ).cache_key
    )
