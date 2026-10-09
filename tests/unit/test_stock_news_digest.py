"""Stock summaries preserve original document bytes, affiliation and factual nature."""

from __future__ import annotations

import hashlib
import importlib
from datetime import UTC, date, datetime
from decimal import Decimal
from types import ModuleType
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rquant.stock_news_digest import (
        StockNewsCoverage,
        StockNewsDigestDraft,
        StockNewsDocument,
        StockNewsFacts,
    )

import pytest
from pydantic import ValidationError

from rquant.ai_assistance_contracts import AISealedFact
from rquant.ai_interpretation import AICitedText

BODY = "公司披露实际增长 12.50%；预测增长 15.00%。"
COLLECTED = datetime(2026, 9, 30, 9, tzinfo=UTC)


def implementation() -> ModuleType:
    return importlib.import_module("rquant.stock_news_digest")


def document(**changes: object) -> StockNewsDocument:
    module = implementation()
    body_hash = hashlib.sha256(BODY.encode()).hexdigest()
    records = []
    for suffix, phrase, value, nature in [
        ("actual", "实际增长 12.50%", "0.125", "actual"),
        ("forecast", "预测增长 15.00%", "0.15", "forecast"),
    ]:
        start = BODY.index(phrase)
        records.append(
            module.NewsSourceFact(
                fact=AISealedFact(
                    fact_id=f"doc.one.{suffix}",
                    label="增长",
                    kind="number",
                    value=Decimal(value),
                    unit="%",
                    source_path=f"document.one.{suffix}",
                    source_sha256=body_hash,
                ),
                nature=nature,
                quote=phrase,
                body_start=start,
                body_end=start + len(phrase),
                period_end=date(2026, 12, 31) if nature == "forecast" else date(2026, 6, 30),
            )
        )
    values = dict(
        provider="eastmoney",
        source_kind="research",
        document_id="original-report-one",
        source_url="https://pdf.dfcfw.com/pdf/original-report-one.pdf",
        title="公司研究报告",
        source_stock_codes=("000001.SZ",),
        affiliation_kind="issuer_code",
        affiliation_source_sha256="a" * 64,
        published_date=date(2026, 8, 25),
        published_at=None,
        body_date=date(2026, 8, 20),
        first_collected_at=COLLECTED,
        body=BODY,
        body_sha256=body_hash,
        facts=tuple(records),
    )
    values.update(changes)
    return module.StockNewsDocument(**values)


def coverage(**changes: object) -> StockNewsCoverage:
    values = dict(
        provider="eastmoney",
        source_kind="research",
        start_date=date(2026, 8, 1),
        end_date=date(2026, 9, 30),
        collected_at=COLLECTED,
        status="available",
        requested_pages=(1,),
        collected_pages=(1,),
        total_pages=16,
        returned_documents=1,
        complete=False,
    )
    values.update(changes)
    return implementation().StockNewsCoverage(**values)


def collection(**changes: object) -> StockNewsFacts:
    values = dict(
        owner_uid="alice", stock_code="000001.SZ", documents=(document(),), coverage=(coverage(),)
    )
    values.update(changes)
    return implementation().StockNewsFacts(**values)


def draft(
    facts: StockNewsFacts,
    *,
    nature: str = "actual",
    fact_id: str = "doc.one.actual",
    text: str | None = None,
) -> StockNewsDigestDraft:
    module = implementation()
    return module.StockNewsDigestDraft(
        context_sha256=facts.context_sha256,
        statements=(
            module.NewsDigestStatement(
                nature=nature,
                content=AICitedText(
                    text=text or "报告记录 {{" + fact_id + "}}。", citations=(fact_id,)
                ),
            ),
        ),
    )


def test_original_report_keeps_platform_body_and_first_collection_dates_separate() -> None:
    source = document()
    assert source.published_date == date(2026, 8, 25)
    assert source.body_date == date(2026, 8, 20)
    assert source.first_collected_at == COLLECTED
    assert source.body == BODY
    assert source.known_at == COLLECTED
    assert (
        source.document_sha256
        == implementation()
        .StockNewsDocument.model_validate_json(source.model_dump_json())
        .document_sha256
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"body_sha256": "b" * 64},
        {"body": "没有原文"},
        {"body": ""},
        {"affiliation_kind": "keyword"},
        {"source_url": "https://evil.example/report.pdf"},
        {"source_stock_codes": ()},
        {"body_date": date(2026, 10, 1)},
        {"published_at": datetime(2026, 10, 1, tzinfo=UTC)},
    ],
)
def test_source_rejects_wrong_digest_missing_body_keyword_future_date_and_non_source_url(
    changes: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        document(**changes)


def test_stock_affiliation_is_explicit_original_code_not_keyword_match() -> None:
    with pytest.raises(ValidationError):
        collection(stock_code="600000.SH")
    source = document(source_stock_codes=("600000.SH",))
    with pytest.raises(ValidationError):
        collection(documents=(source,))


def test_source_quote_and_numeric_value_bind_to_exact_original_body() -> None:
    source = document()
    original = source.facts[0]
    wrong = original.model_copy(update={"quote": "实际增长 20.00%"})
    with pytest.raises(ValidationError):
        document(facts=(wrong,))
    wrong_value = original.model_copy(
        update={"fact": original.fact.model_copy(update={"value": Decimal("0.20")})}
    )
    with pytest.raises(ValidationError):
        document(facts=(wrong_value,))
    with pytest.raises(ValidationError):
        document(facts=(original.model_copy(update={"period_end": date(2026, 12, 31)}),))


def test_forecast_remains_forecast_and_cannot_be_relabelled_actual() -> None:
    facts = collection()
    result = implementation().validate_stock_news_digest(
        facts, draft=draft(facts, nature="forecast", fact_id="doc.one.forecast")
    )
    assert result.statements[0].nature == "forecast"
    assert result.statements[0].content.text == "报告记录 15.00%。"
    with pytest.raises(ValueError):
        implementation().validate_stock_news_digest(
            facts, draft=draft(facts, nature="actual", fact_id="doc.one.forecast")
        )
    with pytest.raises(ValueError):
        implementation().validate_stock_news_digest(
            facts, draft=draft(facts, nature="forecast", fact_id="doc.one.actual")
        )


def test_first_page_is_limited_and_cannot_claim_complete_history() -> None:
    facts = collection()
    result = implementation().validate_stock_news_digest(facts, draft=draft(facts))
    assert result.coverage_complete is False
    assert result.status == "limited"
    with pytest.raises(ValidationError):
        coverage(complete=True)
    with pytest.raises(ValidationError):
        coverage(collected_pages=(2,))


def test_unavailable_source_is_unknown_while_real_zero_response_can_be_empty() -> None:
    missing = coverage(
        status="permission_denied", collected_pages=(), total_pages=None, returned_documents=None
    )
    facts = collection(documents=(), coverage=(missing,))
    empty_draft = implementation().StockNewsDigestDraft(
        context_sha256=facts.context_sha256, statements=()
    )
    result = implementation().validate_stock_news_digest(facts, draft=empty_draft)
    assert result.status == "unavailable" and result.coverage_complete is False
    known_empty = coverage(total_pages=0, returned_documents=0, complete=True)
    facts = collection(documents=(), coverage=(known_empty,))
    result = implementation().validate_stock_news_digest(
        facts, draft=empty_draft.model_copy(update={"context_sha256": facts.context_sha256})
    )
    assert result.status == "empty" and result.coverage_complete is True
    with pytest.raises(ValidationError):
        coverage(status="permission_denied", returned_documents=0, collected_pages=())


@pytest.mark.parametrize(
    "text",
    [
        "报告记录 20.00%。",
        "报告记录二十。",
        "报告记录 ２０％。",
        "https://evil.example/report",
        "报告记录 {{forged.fact}}。",
    ],
)
def test_body_fake_quantity_and_source_reference_reject_whole_digest(text: str) -> None:
    facts = collection()
    with pytest.raises(ValueError):
        implementation().validate_stock_news_digest(facts, draft=draft(facts, text=text))


def test_owner_document_and_coverage_changes_invalidate_old_summary() -> None:
    original = collection()
    old = draft(original)
    for current in (
        collection(owner_uid="bob"),
        collection(documents=(document(title="另一份报告"),)),
        collection(coverage=(coverage(end_date=date(2026, 9, 29)),)),
    ):
        assert current.context_sha256 != original.context_sha256
        with pytest.raises(ValueError):
            implementation().validate_stock_news_digest(current, draft=old)


def test_research_scope_retains_every_requested_stock_and_pending_member() -> None:
    module = implementation()
    codes = tuple(f"{index:06d}.SZ" for index in range(600))
    scope = module.StockNewsResearchScope(
        owner_uid="alice", pool_watchlist_sha256="a" * 64, stock_codes=codes
    )
    progress = module.StockNewsResearchProgress(
        scope=scope, completed_codes=codes[:256], pending_codes=codes[256:]
    )
    assert len(progress.scope.stock_codes) == 600 and len(progress.pending_codes) == 344
    assert progress.complete is False
    with pytest.raises(ValidationError):
        module.StockNewsResearchProgress(
            scope=scope, completed_codes=codes[:256], pending_codes=codes[256:-1]
        )
    with pytest.raises(ValidationError):
        module.StockNewsResearchProgress(
            scope=scope, completed_codes=codes[:256], pending_codes=codes[255:]
        )


def test_observed_zero_count_cannot_contain_a_sealed_document() -> None:
    with pytest.raises(ValidationError):
        collection(coverage=(coverage(total_pages=0, returned_documents=0, complete=True),))


def test_complete_coverage_cannot_include_a_future_date_window() -> None:
    with pytest.raises(ValidationError):
        coverage(end_date=date(2026, 10, 1), total_pages=1, complete=True)


def test_numeric_fact_cannot_match_only_a_substring_of_a_larger_source_number() -> None:
    source = document()
    body = BODY.replace("12.50%", "112.50%")
    body_hash = hashlib.sha256(body.encode()).hexdigest()
    quote = "实际增长 112.50%"
    start = body.index(quote)
    record = source.facts[0].model_copy(
        update={
            "fact": source.facts[0].fact.model_copy(update={"source_sha256": body_hash}),
            "quote": quote,
            "body_start": start,
            "body_end": start + len(quote),
        }
    )
    with pytest.raises(ValidationError):
        document(body=body, body_sha256=body_hash, facts=(record,))
