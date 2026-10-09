"""Stock-bound sealed news facts; fetching, quotas and scheduling stay with their owners."""

from __future__ import annotations

import hashlib
import re
from datetime import date
from typing import Annotated, Literal, Self
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

from pydantic import Field, StringConstraints, field_validator, model_validator

from rquant.ai_assistance_contracts import (
    MAX_FACTS,
    MAX_INTERPRETATION_BYTES,
    MAX_SOURCE_BODY_BYTES,
    AISealedFact,
    Sha256,
    _exact_identity,
)
from rquant.ai_interpretation import AICitedText, render_cited_text
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256

StockCode = Annotated[str, Field(pattern=r"^[0-9]{6}\.(?:SH|SZ|BJ)$")]
Provider = Literal["eastmoney", "tushare"]
SourceKind = Literal["announcement", "news", "research"]
FactNature = Literal["actual", "forecast", "context"]
Page = Annotated[int, Field(strict=True, ge=1, le=2**31 - 1)]
OriginalText = Annotated[str, StringConstraints(strip_whitespace=False)]
_SHANGHAI = ZoneInfo("Asia/Shanghai")


class NewsSourceFact(RuntimeContractModel):
    fact: AISealedFact
    nature: FactNature
    quote: OriginalText = Field(min_length=1, max_length=8192)
    body_start: int = Field(strict=True, ge=0, le=MAX_SOURCE_BODY_BYTES)
    body_end: int = Field(strict=True, ge=1, le=MAX_SOURCE_BODY_BYTES)
    period_end: date | None = None

    @model_validator(mode="after")
    def validate_quote_extent(self) -> Self:
        if self.body_end <= self.body_start:
            raise ValueError("source fact needs a nonempty exact original body span")
        return self


class StockNewsDocument(RuntimeContractModel):
    provider: Provider
    source_kind: SourceKind
    document_id: str = Field(min_length=1, max_length=128)
    source_url: str = Field(min_length=1, max_length=2048)
    title: str = Field(min_length=1, max_length=512)
    source_stock_codes: tuple[StockCode, ...] = Field(min_length=1)
    affiliation_kind: Literal["issuer_code", "verified_company"]
    affiliation_source_sha256: Sha256
    published_date: date | None = None
    published_at: AwareUtcDatetime | None = None
    body_date: date | None = None
    first_collected_at: AwareUtcDatetime
    body: OriginalText = Field(min_length=1)
    body_sha256: Sha256
    facts: tuple[NewsSourceFact, ...] = Field(default=(), max_length=MAX_FACTS)

    @model_validator(mode="after")
    def validate_original_document(self) -> Self:
        body = self.body.encode()
        if (
            len(body) > MAX_SOURCE_BODY_BYTES
            or hashlib.sha256(body).hexdigest() != self.body_sha256
        ):
            raise ValueError("original body is missing, changed or exceeds its byte budget")
        if not self.body.strip():
            raise ValueError("a metadata-only record cannot stand in for original body text")
        if len(set(self.source_stock_codes)) != len(self.source_stock_codes):
            raise ValueError("source company affiliation must use distinct exact stock codes")
        parts = urlsplit(self.source_url)
        host = parts.hostname or ""
        allowed = (
            host == "eastmoney.com" or host.endswith(".eastmoney.com") or host == "pdf.dfcfw.com"
            if self.provider == "eastmoney"
            else host == "tushare.pro" or host.endswith(".tushare.pro")
        )
        if (
            parts.scheme != "https"
            or not allowed
            or parts.username is not None
            or parts.password is not None
            or parts.fragment
            or parts.port not in (None, 443)
        ):
            raise ValueError("original document URL must retain its approved source origin")
        collected_day = self.first_collected_at.astimezone(_SHANGHAI).date()
        if self.published_at is not None:
            platform_day = self.published_at.astimezone(_SHANGHAI).date()
            if self.published_at > self.first_collected_at or (
                self.published_date is not None and self.published_date != platform_day
            ):
                raise ValueError("platform time differs from its date or is not yet known")
        if (
            self.published_date is not None
            and self.published_date > collected_day
            or self.body_date is not None
            and self.body_date > collected_day
        ):
            raise ValueError("document dates cannot be future known facts")
        ids: set[str] = set()
        for record in self.facts:
            fact = record.fact
            if fact.fact_id in ids or fact.source_sha256 != self.body_sha256:
                raise ValueError("source fact identity or body binding differs")
            ids.add(fact.fact_id)
            if self.body[record.body_start : record.body_end] != record.quote:
                raise ValueError("source quotation differs from the exact original body span")
            if fact.value is not None:
                display = re.escape(fact.display_value)
                pattern = (
                    rf"(?<![\d.,+\-]){display}(?![\d.])"
                    if fact.kind in ("number", "date")
                    else display
                )
                if re.search(pattern, record.quote) is None:
                    raise ValueError("source fact value is not a full original quoted quantity")
            if (
                record.nature == "actual"
                and record.period_end is not None
                and record.period_end > collected_day
            ):
                raise ValueError("a future-period value cannot be recorded as an actual fact")
        return self

    @property
    def known_at(self) -> AwareUtcDatetime:
        return self.first_collected_at

    @property
    def document_sha256(self) -> str:
        # Full body is already bound by its checked SHA; do not copy it into compact references.
        return canonical_sha256(self.model_dump(mode="python", exclude={"body"}))


class StockNewsCoverage(RuntimeContractModel):
    provider: Provider
    source_kind: SourceKind
    start_date: date
    end_date: date
    collected_at: AwareUtcDatetime
    status: Literal["available", "unavailable", "permission_denied"]
    requested_pages: tuple[Page, ...] = Field(min_length=1)
    collected_pages: tuple[Page, ...]
    total_pages: int | None = Field(default=None, strict=True, ge=0, le=2**31 - 1)
    returned_documents: int | None = Field(default=None, strict=True, ge=0, le=2**31 - 1)
    complete: bool = Field(default=False, strict=True)

    @model_validator(mode="after")
    def retain_real_coverage(self) -> Self:
        if self.start_date > self.end_date:
            raise ValueError("source coverage date range is reversed")
        for pages in (self.requested_pages, self.collected_pages):
            if tuple(sorted(set(pages))) != pages:
                raise ValueError("source page identities must be sorted and distinct")
        if not set(self.collected_pages) <= set(self.requested_pages):
            raise ValueError("collected page was not part of the original finite request")
        if self.status != "available":
            if self.returned_documents is not None or self.complete:
                raise ValueError("unavailable source cannot report zero or complete coverage")
        elif self.returned_documents is None or not self.collected_pages:
            raise ValueError("available source needs an observed response and original count")
        if self.complete:
            if self.end_date > self.collected_at.astimezone(_SHANGHAI).date():
                raise ValueError("future date window cannot claim complete known coverage")
            empty = (
                self.total_pages == 0
                and self.returned_documents == 0
                and self.requested_pages == self.collected_pages == (1,)
            )
            all_pages = (
                self.total_pages is not None
                and self.total_pages > 0
                and len(self.collected_pages) == self.total_pages
                and self.requested_pages == self.collected_pages
                and all(page == index for index, page in enumerate(self.collected_pages, 1))
            )
            if not (empty or all_pages):
                raise ValueError("a first-page or unknown-range response is not complete coverage")
        return self


class StockNewsFacts(RuntimeContractModel):
    owner_uid: str = Field(min_length=1, max_length=128)
    stock_code: StockCode
    documents: tuple[StockNewsDocument, ...] = Field(default=(), max_length=MAX_FACTS)
    coverage: tuple[StockNewsCoverage, ...] = Field(min_length=1, max_length=16)

    _owner = field_validator("owner_uid", mode="before")(_exact_identity)

    @model_validator(mode="after")
    def bind_stock_and_inventory(self) -> Self:
        documents: set[tuple[str, str, str]] = set()
        facts: set[str] = set()
        coverage = {(record.provider, record.source_kind) for record in self.coverage}
        if len(coverage) != len(self.coverage):
            raise ValueError("source coverage records are duplicated")
        for document in self.documents:
            if self.stock_code not in document.source_stock_codes:
                raise ValueError(
                    "keyword match does not establish this stock's company affiliation"
                )
            identity = (document.provider, document.source_kind, document.document_id)
            if identity in documents or (document.provider, document.source_kind) not in coverage:
                raise ValueError("source document is duplicated or lacks its original coverage")
            documents.add(identity)
            for record in document.facts:
                if record.fact.fact_id in facts:
                    raise ValueError("source fact citation is ambiguous across original documents")
                facts.add(record.fact.fact_id)
        if len(facts) > MAX_FACTS:
            raise ValueError("full news fact inventory exceeds its budget; no silent truncation")
        for record in self.coverage:
            selected = sum(
                document.provider == record.provider and document.source_kind == record.source_kind
                for document in self.documents
            )
            if record.returned_documents is not None and selected > record.returned_documents:
                raise ValueError("sealed document inventory exceeds the observed source count")
        return self

    @property
    def context_sha256(self) -> str:
        return canonical_sha256(
            {
                "owner_uid": self.owner_uid,
                "stock_code": self.stock_code,
                "documents": tuple(document.document_sha256 for document in self.documents),
                "coverage": self.coverage,
            }
        )


class NewsDigestStatement(RuntimeContractModel):
    nature: FactNature
    content: AICitedText


class StockNewsDigestDraft(RuntimeContractModel):
    context_sha256: Sha256
    statements: tuple[NewsDigestStatement, ...] = Field(default=(), max_length=MAX_FACTS)


class ValidatedStockNewsDigest(RuntimeContractModel):
    owner_uid: str
    stock_code: StockCode
    context_sha256: Sha256
    status: Literal["complete", "limited", "empty", "unavailable"]
    coverage_complete: bool = Field(strict=True)
    statements: tuple[NewsDigestStatement, ...]
    content_sha256: Sha256 | None = None

    @model_validator(mode="after")
    def seal_digest(self) -> Self:
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"content_sha256"}))
        if self.content_sha256 is None:
            object.__setattr__(self, "content_sha256", expected)
        elif self.content_sha256 != expected:
            raise ValueError("validated stock digest content changed")
        if len(self.model_dump_json().encode()) > MAX_INTERPRETATION_BYTES:
            raise ValueError("stock digest exceeds its sealed byte budget")
        return self


def validate_stock_news_digest(
    facts: StockNewsFacts, *, draft: StockNewsDigestDraft
) -> ValidatedStockNewsDigest:
    facts = StockNewsFacts.model_validate(facts.model_dump(mode="python"))
    draft = StockNewsDigestDraft.model_validate(draft.model_dump(mode="python"))
    if draft.context_sha256 != facts.context_sha256:
        raise ValueError("stock summary differs from original owner, documents or coverage")
    records = {
        record.fact.fact_id: record for document in facts.documents for record in document.facts
    }
    inventory = tuple(record.fact for record in records.values())
    statements: list[NewsDigestStatement] = []
    for statement in draft.statements:
        if not statement.content.citations:
            raise ValueError("a stock summary statement needs already collected original facts")
        for key in statement.content.citations:
            record = records.get(key)
            if record is None or record.nature not in (statement.nature, "context"):
                raise ValueError("a source reference is missing or actual/forecast nature changed")
        statements.append(
            NewsDigestStatement(
                nature=statement.nature,
                content=render_cited_text(statement.content, facts=inventory),
            )
        )
    complete = all(record.complete for record in facts.coverage)
    unavailable = all(record.status != "available" for record in facts.coverage)
    zero = all(record.returned_documents == 0 for record in facts.coverage)
    status = (
        "unavailable"
        if unavailable
        else "empty"
        if complete and zero
        else ("complete" if complete else "limited")
    )
    return ValidatedStockNewsDigest(
        owner_uid=facts.owner_uid,
        stock_code=facts.stock_code,
        context_sha256=facts.context_sha256,
        status=status,
        coverage_complete=complete,
        statements=tuple(statements),
    )


class StockNewsResearchScope(RuntimeContractModel):
    """A read-only original pool/watchlist union, not an independent work queue."""

    owner_uid: str = Field(min_length=1, max_length=128)
    pool_watchlist_sha256: Sha256
    stock_codes: tuple[StockCode, ...] = Field(min_length=1)

    _owner = field_validator("owner_uid", mode="before")(_exact_identity)

    @model_validator(mode="after")
    def complete_union(self) -> Self:
        if len(set(self.stock_codes)) != len(self.stock_codes):
            raise ValueError("original research union has duplicate stock codes")
        if len(self.model_dump_json().encode()) > 2 * 1024 * 1024:
            raise ValueError("full research scope exceeds original metadata capacity")
        return self


class StockNewsResearchProgress(RuntimeContractModel):
    scope: StockNewsResearchScope
    completed_codes: tuple[StockCode, ...]
    pending_codes: tuple[StockCode, ...]

    @model_validator(mode="after")
    def retain_every_member(self) -> Self:
        completed, pending = set(self.completed_codes), set(self.pending_codes)
        if (
            len(completed) != len(self.completed_codes)
            or len(pending) != len(self.pending_codes)
            or completed & pending
            or completed | pending != set(self.scope.stock_codes)
        ):
            raise ValueError("research progress must retain each original stock exactly once")
        return self

    @property
    def complete(self) -> bool:
        return not self.pending_codes
