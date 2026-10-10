"""Interpretation consumes complete original results and renders only cited facts."""

from __future__ import annotations

import hashlib
import importlib
from decimal import Decimal
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING
from uuid import UUID

if TYPE_CHECKING:
    from rquant.ai_interpretation import InterpretationDraft, SealedInterpretationFacts

import pytest
from pydantic import ValidationError

from rquant.ai_assistance_contracts import AIInterpretationBinding, AISealedFact
from rquant.portfolio_backtest_artifact import PortfolioReadResult
from rquant.portfolio_backtest_models import (
    FrozenPortfolioInput,
    PortfolioBacktestConfig,
    PortfolioSourceManifest,
)
from rquant.portfolio_backtest_product import execute_portfolio_input
from rquant.runtime_contracts import canonical_sha256
from rquant.strategy_template_artifact import TemplateReadResult
from rquant.strategy_template_run import StrategyTemplateResult
from tests.unit.test_portfolio_backtest import _request


def implementation() -> ModuleType:
    return importlib.import_module("rquant.ai_interpretation")


@pytest.fixture(scope="module")
def sealed(tmp_path_factory: pytest.TempPathFactory) -> PortfolioReadResult:
    request = _request((None, None))
    frozen = FrozenPortfolioInput(
        config=PortfolioBacktestConfig.from_request(request),
        request=request,
        sources=PortfolioSourceManifest(
            source_mode="captured_with_retrospective_prices",
            market_hash="a" * 64,
            reference_hash="b" * 64,
            opening_hash="c" * 64,
        ),
        benchmark_closes=None,
        benchmark_unavailable="missing_source",
    )
    bundle = execute_portfolio_input(
        frozen, research_root=tmp_path_factory.mktemp("original-result")
    )
    return PortfolioReadResult(
        job_id=UUID("44af75e7-12ae-43fd-970a-b602668d0c41"),
        spec_hash="d" * 64,
        manifest_hash="e" * 64,
        result_hash="f" * 64,
        bundle=bundle,
    )


def packet(sealed: PortfolioReadResult) -> SealedInterpretationFacts:
    return implementation().facts_from_portfolio(sealed, owner_uid="alice")


def context(facts: SealedInterpretationFacts) -> AIInterpretationBinding:
    return AIInterpretationBinding(
        result=facts.binding,
        facts_sha256=facts.facts_sha256,
        model_id="model-a",
        template_version="interpretation-v1",
    )


def draft(
    facts: SealedInterpretationFacts,
    *,
    text: str = "累计收益为 {{summary.total_return}}。",
    citations: tuple[str, ...] = ("summary.total_return",),
) -> InterpretationDraft:
    module = implementation()
    sections = []
    for key, body, refs in [
        ("overview", text, citations),
        ("annual", "分年收益为 {{annual.2026.total_return}}。", ("annual.2026.total_return",)),
        ("risk", "过拟合检验：{{overfit.state}}。", ("overfit.state",)),
        ("suggestions", "建议进一步验证。", ()),
    ]:
        sections.append(
            module.InterpretationSection(
                key=key, paragraphs=(module.AICitedText(text=body, citations=refs),)
            )
        )
    return module.InterpretationDraft(
        context_sha256=context(facts).cache_key,
        sections=tuple(sections),
        metrics=(module.AICitedMetric(fact_id="summary.total_return", display_value="0.00%"),),
    )


def test_complete_portfolio_facts_use_original_summary_unknown_and_annual_formula(
    sealed: PortfolioReadResult,
) -> None:
    facts = packet(sealed)
    by_id = {fact.fact_id: fact for fact in facts.facts}
    assert by_id["summary.total_return"].value == Decimal(
        str(sealed.bundle.performance.summary.total_return)
    )
    assert by_id["summary.sharpe"].value is None
    assert by_id["overfit.state"].display_value == "未评估"
    assert by_id["annual.2026.total_return"].value == Decimal("0.0")
    assert by_id["annual.2026.total_return"].derivation_sha256 is not None
    assert facts.source_payload_sha256 == hashlib.sha256(sealed.bundle.json_bytes()).hexdigest()
    assert facts.daily_returns_sha256 == canonical_sha256(
        tuple((day.trade_date, day.daily_return) for day in sealed.bundle.result.days)
    )
    assert facts.complete is True


def test_closed_trade_facts_bind_exact_complete_original_serialized_payload(
    sealed: PortfolioReadResult, tmp_path: Path
) -> None:
    def computed(selections: tuple[str | None, ...], name: str) -> PortfolioReadResult:
        request = _request(selections)
        frozen = FrozenPortfolioInput(
            config=PortfolioBacktestConfig.from_request(request),
            request=request,
            sources=sealed.bundle.frozen.sources,
            benchmark_closes=None,
            benchmark_unavailable="missing_source",
        )
        research_root = tmp_path / name
        research_root.mkdir(mode=0o700)
        bundle = execute_portfolio_input(frozen, research_root=research_root)
        return PortfolioReadResult.model_validate(
            {**sealed.model_dump(mode="python"), "bundle": bundle}
        )

    original = computed(("600000.SH", None), "closed-position")
    assert original.bundle.result.status == "complete"
    assert original.bundle.performance.round_trip_analysis.overall.count == 1
    assert set(original.bundle.performance.round_trip_analysis.by_holding_days) == {1}
    facts = packet(original)
    assert facts.source_payload_sha256 == hashlib.sha256(original.bundle.json_bytes()).hexdigest()
    assert all(fact.source_sha256 == facts.source_payload_sha256 for fact in facts.facts)

    changed = packet(computed(("600000.SH", "600001.SH"), "changed-content"))
    assert changed.binding == facts.binding
    assert changed.source_payload_sha256 != facts.source_payload_sha256
    assert changed.facts_sha256 != facts.facts_sha256
    assert context(changed).cache_key != context(facts).cache_key
    with pytest.raises(ValueError):
        implementation().validate_interpretation(
            changed, binding=context(facts), draft=draft(facts)
        )


def test_four_sections_render_canonical_body_and_metric_values(sealed: PortfolioReadResult) -> None:
    facts = packet(sealed)
    result = implementation().validate_interpretation(
        facts, binding=context(facts), draft=draft(facts)
    )
    assert len(result.sections) == 4
    assert result.sections[0].paragraphs[0].text == "累计收益为 0.00%。"
    assert result.sections[2].paragraphs[0].text == "过拟合检验：未评估。"
    assert result.metrics[0].display_value == "0.00%"
    assert result.binding == context(facts)


@pytest.mark.parametrize(
    "text",
    [
        "累计收益为 1.00%。",
        "累计收益为 １．００％。",
        "累计收益为百分之十二。",
        "结束日期是 2026-10-07。",
        "结束日期是二〇二六年十月七日。",
        "回撤为 Ⅻ 倍。",
        "累计收益为 {{summary.total_return}} 元。",
        "累计收益为 {{summary.total_return}}%。",
    ],
)
def test_any_unbound_body_number_date_or_wrong_unit_rejects_whole_interpretation(
    sealed: PortfolioReadResult, text: str
) -> None:
    facts = packet(sealed)
    with pytest.raises(ValueError):
        implementation().validate_interpretation(
            facts, binding=context(facts), draft=draft(facts, text=text)
        )


def test_one_wrong_metric_rejects_whole_interpretation(sealed: PortfolioReadResult) -> None:
    facts = packet(sealed)
    bad = draft(facts).model_copy(
        update={
            "metrics": (
                implementation().AICitedMetric(
                    fact_id="summary.total_return", display_value="1.00%"
                ),
            )
        }
    )
    with pytest.raises(ValueError):
        implementation().validate_interpretation(facts, binding=context(facts), draft=bad)


@pytest.mark.parametrize(
    "refs", [("forged.fact",), (), ("summary.total_return", "summary.total_return")]
)
def test_unknown_missing_or_duplicate_citation_rejects(
    sealed: PortfolioReadResult, refs: tuple[str, ...]
) -> None:
    facts = packet(sealed)
    with pytest.raises((ValueError, ValidationError)):
        implementation().validate_interpretation(
            facts, binding=context(facts), draft=draft(facts, citations=refs)
        )


@pytest.mark.parametrize(
    "field",
    ["owner_uid", "spec_sha256", "manifest_sha256", "result_sha256", "job_id", "source_kind"],
)
def test_changed_sealed_context_cannot_reuse_original_model_response(
    sealed: PortfolioReadResult, field: str
) -> None:
    facts = packet(sealed)
    value = (
        "bob"
        if field == "owner_uid"
        else UUID("da838dd2-7b1a-4614-92a6-c18e3635ac35")
        if field == "job_id"
        else "strategy_template"
        if field == "source_kind"
        else "a" * 64
    )
    wrong = context(facts).model_copy(
        update={"result": facts.binding.model_copy(update={field: value})}
    )
    with pytest.raises(ValueError):
        implementation().validate_interpretation(facts, binding=wrong, draft=draft(facts))


def test_changed_model_template_or_facts_invalidates_response_cache(
    sealed: PortfolioReadResult,
) -> None:
    facts = packet(sealed)
    for change in (
        {"model_id": "model-b"},
        {"template_version": "interpretation-v2"},
        {"facts_sha256": "a" * 64},
    ):
        with pytest.raises(ValueError):
            implementation().validate_interpretation(
                facts, binding=context(facts).model_copy(update=change), draft=draft(facts)
            )


def test_changed_complete_payload_is_revalidated_before_fact_extraction(
    sealed: PortfolioReadResult,
) -> None:
    bad = sealed.model_copy(update={"bundle": sealed.bundle.model_copy(update={"html": "changed"})})
    with pytest.raises(ValueError):
        packet(bad)


def test_over_capacity_facts_and_missing_sections_are_rejected(sealed: PortfolioReadResult) -> None:
    facts = packet(sealed)
    model = implementation().SealedInterpretationFacts
    additions = tuple(
        AISealedFact(
            fact_id=f"extra.n{index}",
            label="附加事实",
            kind="number",
            value=Decimal(index),
            source_path="synthetic.extra",
            source_sha256="a" * 64,
        )
        for index in range(257)
    )
    with pytest.raises(ValidationError):
        model(
            binding=facts.binding,
            source_payload_sha256=facts.source_payload_sha256,
            daily_returns_sha256=facts.daily_returns_sha256,
            performance_formula_sha256=facts.performance_formula_sha256,
            facts=additions,
        )
    with pytest.raises(ValidationError):
        implementation().InterpretationDraft(
            context_sha256=context(facts).cache_key, sections=draft(facts).sections[:3], metrics=()
        )


def test_template_facts_bind_original_owner_and_complete_daily_rows(
    sealed: PortfolioReadResult,
) -> None:
    result = StrategyTemplateResult(
        owner_id="alice",
        strategy_id="template-one",
        version=1,
        definition_fingerprint="a" * 64,
        definition_record_hash="b" * 64,
        input_hash="c" * 64,
        calendar_source_identity=sealed.bundle.result.calendar_source_identity,
        cost_spec_id=sealed.bundle.result.cost_spec_id,
        status="complete",
        days=sealed.bundle.result.days,
        exit_decisions=(),
    )
    read = TemplateReadResult(
        job_id=sealed.job_id,
        spec_hash=sealed.spec_hash,
        manifest_hash=sealed.manifest_hash,
        result_hash=sealed.result_hash,
        result=result,
    )
    facts = implementation().facts_from_template(read, owner_uid="alice")
    assert facts.binding.source_kind == "strategy_template"
    assert {fact.fact_id: fact for fact in facts.facts}["summary.total_return"].value == Decimal(
        "0.0"
    )
    with pytest.raises(PermissionError):
        implementation().facts_from_template(read, owner_uid="bob")


def test_full_unicode_interpretation_obeys_the_one_mib_byte_budget(
    sealed: PortfolioReadResult,
) -> None:
    module = implementation()
    facts = packet(sealed)
    paragraphs = tuple(module.AICitedText(text="验证" * 4096, citations=()) for _ in range(16))
    sections = tuple(
        module.InterpretationSection(key=key, paragraphs=paragraphs)
        for key in ("overview", "annual", "risk", "suggestions")
    )
    oversized = module.InterpretationDraft(
        context_sha256=context(facts).cache_key, sections=sections, metrics=()
    )
    with pytest.raises(ValueError, match="byte budget"):
        module.validate_interpretation(facts, binding=context(facts), draft=oversized)
