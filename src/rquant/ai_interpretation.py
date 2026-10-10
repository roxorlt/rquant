"""Build facts from full original results and reject unbound model quantities."""

from __future__ import annotations

import hashlib
import inspect
import re
import unicodedata
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Self

from pydantic import Field, model_validator

from rquant.ai_assistance_contracts import (
    MAX_FACTS,
    MAX_INTERPRETATION_BYTES,
    AIInterpretationBinding,
    AISealedFact,
    AISealedResultBinding,
    FactId,
    Sha256,
)
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256

if TYPE_CHECKING:
    from rquant.backtest.contracts import BacktestDayResult
    from rquant.perf import PerformanceSummary
    from rquant.portfolio_backtest_artifact import PortfolioReadResult
    from rquant.strategy_template_artifact import TemplateReadResult

SectionKey = Literal["overview", "annual", "risk", "suggestions"]
_SECTIONS = ("overview", "annual", "risk", "suggestions")
_SLOT = re.compile(r"\{\{([a-z][a-z0-9_.:-]{0,127})\}\}")
_CHINESE_NUMBERS = re.compile(r"[零〇一二三四五六七八九十百千万亿两壹贰叁肆伍陆柒捌玖拾佰仟萬億兆]")
_NON_QUANTITIES = ("进一步", "一致", "统一", "一般", "唯一", "另一", "一旦", "一并", "一贯")
_UNIT_SUFFIX = re.compile(r"^(?:%|‰|亿元|万元|元|股|倍|天|次|年|月|日|条|百分点)")
_METRICS = (
    ("observations", "交易日数", "天", 0),
    ("total_return", "累计收益", "%", 2),
    ("annualized_return", "年化收益", "%", 2),
    ("annualized_volatility", "年化波动", "%", 2),
    ("sharpe", "夏普比率", "", 2),
    ("sortino", "索提诺比率", "", 2),
    ("calmar", "卡玛比率", "", 2),
    ("max_drawdown", "最大回撤", "%", 2),
    ("max_drawdown_duration", "回撤持续天数", "天", 0),
    ("win_rate", "盈利日比例", "%", 2),
    ("payoff_ratio", "盈亏比", "倍", 2),
)


class SealedInterpretationFacts(RuntimeContractModel):
    binding: AISealedResultBinding
    complete: Literal[True] = True
    source_payload_sha256: Sha256
    daily_returns_sha256: Sha256
    performance_formula_sha256: Sha256
    facts: tuple[AISealedFact, ...] = Field(min_length=1, max_length=MAX_FACTS)

    @model_validator(mode="after")
    def validate_inventory(self) -> Self:
        ids = tuple(fact.fact_id for fact in self.facts)
        if len(ids) != len(set(ids)):
            raise ValueError("sealed facts contain duplicate identities")
        if len(self.model_dump_json().encode()) > MAX_INTERPRETATION_BYTES:
            raise ValueError("full sealed facts exceed the interpretation byte budget")
        return self

    @property
    def facts_sha256(self) -> str:
        return canonical_sha256(self)


class AICitedText(RuntimeContractModel):
    text: str = Field(min_length=1, max_length=8192)
    citations: tuple[FactId, ...] = Field(default=(), max_length=MAX_FACTS)

    @model_validator(mode="after")
    def unique_references(self) -> Self:
        if len(self.citations) != len(set(self.citations)):
            raise ValueError("duplicate fact citations are not allowed")
        return self


class AICitedMetric(RuntimeContractModel):
    fact_id: FactId
    display_value: str = Field(min_length=1, max_length=8192)


class InterpretationSection(RuntimeContractModel):
    key: SectionKey
    paragraphs: tuple[AICitedText, ...] = Field(min_length=1, max_length=16)


class InterpretationDraft(RuntimeContractModel):
    context_sha256: Sha256
    sections: tuple[InterpretationSection, ...] = Field(min_length=4, max_length=4)
    metrics: tuple[AICitedMetric, ...] = Field(default=(), max_length=MAX_FACTS)

    @model_validator(mode="after")
    def require_four_sections(self) -> Self:
        if tuple(section.key for section in self.sections) != _SECTIONS:
            raise ValueError("interpretation requires the four sections in their fixed order")
        return self


class ValidatedInterpretation(RuntimeContractModel):
    binding: AIInterpretationBinding
    sections: tuple[InterpretationSection, ...]
    metrics: tuple[AICitedMetric, ...]
    content_sha256: Sha256 | None = None

    @model_validator(mode="after")
    def seal_content(self) -> Self:
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"content_sha256"}))
        if self.content_sha256 is None:
            object.__setattr__(self, "content_sha256", expected)
        elif self.content_sha256 != expected:
            raise ValueError("validated interpretation content changed")
        if len(self.model_dump_json().encode()) > MAX_INTERPRETATION_BYTES:
            raise ValueError("interpretation exceeds the sealed output byte budget")
        return self


def render_cited_text(value: AICitedText, *, facts: tuple[AISealedFact, ...]) -> AICitedText:
    """Quantities come from slots, never from free model text or guessed equivalence."""
    value = AICitedText.model_validate(value.model_dump(mode="python"))
    inventory = {fact.fact_id: fact for fact in facts}
    slots = tuple(match.group(1) for match in _SLOT.finditer(value.text))
    if set(slots) != set(value.citations) or any(key not in inventory for key in value.citations):
        raise ValueError("every fact slot needs its exact original citation")
    for match in _SLOT.finditer(value.text):
        suffix = unicodedata.normalize("NFKC", value.text[match.end() :]).lstrip()
        if _UNIT_SUFFIX.match(suffix):
            raise ValueError("a model cannot append or change a cited fact unit")
    prose = unicodedata.normalize("NFKC", _SLOT.sub("", value.text))
    if "{{" in prose or "}}" in prose or re.search(r"https?://|www\.|\]\(|[<>]", prose):
        raise ValueError("model text cannot supply an unresolved slot or a source link")
    for word in _NON_QUANTITIES:
        prose = prose.replace(word, "")
    if (
        any(unicodedata.category(char).startswith("N") for char in prose)
        or _CHINESE_NUMBERS.search(prose)
        or "%" in prose
        or "‰" in prose
    ):
        raise ValueError("free body quantities, dates and percentages require sealed fact slots")
    rendered = _SLOT.sub(lambda match: inventory[match.group(1)].display_value, value.text)
    return AICitedText(text=rendered, citations=value.citations)


def validate_interpretation(
    facts: SealedInterpretationFacts,
    *,
    binding: AIInterpretationBinding,
    draft: InterpretationDraft,
) -> ValidatedInterpretation:
    facts = SealedInterpretationFacts.model_validate(facts.model_dump(mode="python"))
    binding = AIInterpretationBinding.model_validate(binding.model_dump(mode="python"))
    draft = InterpretationDraft.model_validate(draft.model_dump(mode="python"))
    if (
        binding.result != facts.binding
        or binding.facts_sha256 != facts.facts_sha256
        or draft.context_sha256 != binding.cache_key
    ):
        raise ValueError("interpretation differs from owner, sealed result, model or template")
    inventory = {fact.fact_id: fact for fact in facts.facts}
    if len({metric.fact_id for metric in draft.metrics}) != len(draft.metrics):
        raise ValueError("duplicate metric identities are not allowed")
    for metric in draft.metrics:
        fact = inventory.get(metric.fact_id)
        if fact is None or metric.display_value != fact.display_value:
            raise ValueError("one metric differs from the sealed facts")
    sections = tuple(
        InterpretationSection(
            key=section.key,
            paragraphs=tuple(
                render_cited_text(row, facts=facts.facts) for row in section.paragraphs
            ),
        )
        for section in draft.sections
    )
    return ValidatedInterpretation(binding=binding, sections=sections, metrics=draft.metrics)


def _complete_rows(days: tuple[BacktestDayResult, ...]) -> tuple[tuple[date, Decimal], ...]:
    rows = tuple((day.trade_date, day.daily_return) for day in days)
    dates = tuple(row[0] for row in rows)
    if (
        not rows
        or dates != tuple(sorted(set(dates)))
        or any(day.account is None or day.daily_return is None for day in days)
    ):
        raise ValueError("interpretation needs every complete original daily result")
    return rows  # type: ignore[return-value]


def _summary_facts(
    summary: PerformanceSummary,
    *,
    prefix: str,
    source_path: str,
    source_sha256: str,
    derivation_sha256: str | None = None,
) -> tuple[AISealedFact, ...]:
    return tuple(
        AISealedFact(
            fact_id=f"{prefix}.{name}",
            label=label,
            kind="number",
            value=None if getattr(summary, name) is None else Decimal(str(getattr(summary, name))),
            unit=unit,
            decimals=decimals,
            source_path=f"{source_path}.{name}",
            source_sha256=source_sha256,
            derivation_sha256=derivation_sha256,
        )
        for name, label, unit, decimals in _METRICS
    )


def _derived_facts(
    rows: tuple[tuple[date, Decimal], ...], *, payload_hash: str
) -> tuple[tuple[AISealedFact, ...], str]:
    import pandas as pd

    from rquant.perf import performance_summary

    source_file = inspect.getsourcefile(performance_summary)
    if source_file is None:
        raise ValueError("original performance function source identity is unavailable")
    formula_hash = hashlib.sha256(Path(source_file).read_bytes()).hexdigest()
    output: list[AISealedFact] = []
    for year in sorted({trade_date.year for trade_date, _ in rows}):
        year_rows = tuple(row for row in rows if row[0].year == year)
        daily = pd.Series(
            [float(value) for _, value in year_rows],
            index=pd.DatetimeIndex([day for day, _ in year_rows]),
            dtype="float64",
        )
        proof = canonical_sha256(
            {
                "function": "rquant.perf.core.performance_summary",
                "source": formula_hash,
                "source_payload": payload_hash,
                "daily_rows": year_rows,
            }
        )
        output.extend(
            _summary_facts(
                performance_summary(daily),
                prefix=f"annual.{year}",
                source_path=f"derived.annual.{year}",
                source_sha256=payload_hash,
                derivation_sha256=proof,
            )
        )
    return tuple(output), formula_hash


def _period_facts(
    rows: tuple[tuple[date, Decimal], ...], *, payload_hash: str
) -> tuple[AISealedFact, ...]:
    return (
        AISealedFact(
            fact_id="period.start",
            label="开始日期",
            kind="date",
            value=rows[0][0],
            source_path="days.0.trade_date",
            source_sha256=payload_hash,
        ),
        AISealedFact(
            fact_id="period.end",
            label="结束日期",
            kind="date",
            value=rows[-1][0],
            source_path="days.last.trade_date",
            source_sha256=payload_hash,
        ),
        AISealedFact(
            fact_id="overfit.state",
            label="过拟合检验",
            kind="text",
            value="未评估",
            source_path="overfit_state:not_evaluated",
            source_sha256=payload_hash,
        ),
    )


def facts_from_portfolio(read: PortfolioReadResult, *, owner_uid: str) -> SealedInterpretationFacts:
    from rquant.portfolio_backtest_artifact import PortfolioReadResult

    if type(read) is not PortfolioReadResult:
        raise TypeError("portfolio facts require the original typed full-result reader output")
    checked = PortfolioReadResult.model_validate(read.model_dump(mode="python"))
    bundle = checked.bundle
    if bundle.result.status != "complete" or bundle.performance is None:
        raise ValueError("an incomplete result cannot produce a complete interpretation")
    rows = _complete_rows(bundle.result.days)
    payload_hash = hashlib.sha256(bundle.json_bytes()).hexdigest()
    annual, formula_hash = _derived_facts(rows, payload_hash=payload_hash)
    facts = _summary_facts(
        bundle.performance.summary,
        prefix="summary",
        source_path="performance.summary",
        source_sha256=payload_hash,
    )
    if bundle.performance.benchmark_summary is not None:
        facts += _summary_facts(
            bundle.performance.benchmark_summary,
            prefix="benchmark",
            source_path="performance.benchmark_summary",
            source_sha256=payload_hash,
        )
    return SealedInterpretationFacts(
        binding=AISealedResultBinding(
            owner_uid=owner_uid,
            source_kind="portfolio",
            job_id=checked.job_id,
            spec_sha256=checked.spec_hash,
            manifest_sha256=checked.manifest_hash,
            result_sha256=checked.result_hash,
        ),
        source_payload_sha256=payload_hash,
        daily_returns_sha256=canonical_sha256(rows),
        performance_formula_sha256=formula_hash,
        facts=(*facts, *annual, *_period_facts(rows, payload_hash=payload_hash)),
    )


def facts_from_template(read: TemplateReadResult, *, owner_uid: str) -> SealedInterpretationFacts:
    import pandas as pd

    from rquant.perf import performance_summary
    from rquant.strategy_template_artifact import TemplateReadResult

    if type(read) is not TemplateReadResult:
        raise TypeError("template facts require the original typed full-result reader output")
    checked = TemplateReadResult.model_validate(read.model_dump(mode="python"))
    result = checked.result
    if result.owner_id != owner_uid:
        raise PermissionError("template result belongs to another original owner")
    if result.status != "complete":
        raise ValueError("an incomplete template cannot produce a complete interpretation")
    rows = _complete_rows(result.days)
    payload_hash = canonical_sha256(result)
    annual, formula_hash = _derived_facts(rows, payload_hash=payload_hash)
    summary = performance_summary(
        pd.Series(
            [float(value) for _, value in rows],
            index=pd.DatetimeIndex([day for day, _ in rows]),
            dtype="float64",
        )
    )
    proof = canonical_sha256(
        {
            "function": "rquant.perf.core.performance_summary",
            "source": formula_hash,
            "source_payload": payload_hash,
            "daily_rows": rows,
        }
    )
    facts = _summary_facts(
        summary,
        prefix="summary",
        source_path="derived.summary",
        source_sha256=payload_hash,
        derivation_sha256=proof,
    )
    return SealedInterpretationFacts(
        binding=AISealedResultBinding(
            owner_uid=owner_uid,
            source_kind="strategy_template",
            job_id=checked.job_id,
            spec_sha256=checked.spec_hash,
            manifest_sha256=checked.manifest_hash,
            result_sha256=checked.result_hash,
        ),
        source_payload_sha256=payload_hash,
        daily_returns_sha256=canonical_sha256(rows),
        performance_formula_sha256=formula_hash,
        facts=(*facts, *annual, *_period_facts(rows, payload_hash=payload_hash)),
    )
