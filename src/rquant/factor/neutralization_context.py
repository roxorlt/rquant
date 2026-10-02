"""Sealed retrospective context paired with one original prepared price source."""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, model_validator

from rquant.factor.industry_source import (
    FactorIndustryDayBatch,
    FactorIndustryFact,
    FactorIndustryQuery,
    FactorIndustryReadLease,
    FactorIndustrySource,
    open_factor_industry_source,
)
from rquant.factor.market_cap_source import (
    FactorMarketCapDayBatch,
    FactorMarketCapFact,
    FactorMarketCapQuery,
    FactorMarketCapReadLease,
    FactorMarketCapSource,
    open_factor_market_cap_source,
)
from rquant.factor.run_request import RUN_IMMUTABLE, NeutralizationMode
from rquant.factor.source_prepare import FactorPreparedStreamSource, FactorSourceGeneration
from rquant.factor.time_series import IndustryObservation, MarketCapObservation
from rquant.factor.universe import ObservedTime, Sha256, StockCode
from rquant.research_snapshot import FactorComputationScope, verify_materialized_table_artifact
from rquant.runtime_contracts import canonical_sha256


class FactorIndustryContextBinding(BaseModel):
    model_config = RUN_IMMUTABLE
    source_sha256: Sha256
    collection_sha256: Sha256
    captured_at: ObservedTime
    source_read_boundary: Literal["captured_api_responses"] = "captured_api_responses"


class FactorMarketCapContextBinding(BaseModel):
    model_config = RUN_IMMUTABLE
    source_sha256: Sha256
    generation_sha256: Sha256
    unit: Literal["CNY_10000"] = "CNY_10000"
    source_read_boundary: Literal["single_snapshot_transaction"] = "single_snapshot_transaction"


class FactorNeutralizationSources(BaseModel):
    model_config = RUN_IMMUTABLE
    context_sha256: Sha256
    prepared_source_sha256: Sha256
    snapshot_id: Sha256
    binding_hash: Sha256
    scope_content_hash: Sha256
    code_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    industry: FactorIndustryContextBinding | None = Field(
        default=None, exclude_if=lambda v: v is None
    )
    market_cap: FactorMarketCapContextBinding | None = Field(
        default=None, exclude_if=lambda v: v is None
    )
    source_mode: Literal["historical_retrospective"] = "historical_retrospective"

    @model_validator(mode="after")
    def _present(self) -> FactorNeutralizationSources:
        if self.industry is None and self.market_cap is None:
            raise ValueError("context has no sources")
        return self

    def require_binding(
        self,
        *,
        mode: NeutralizationMode,
        snapshot_id: str,
        binding_hash: str,
        as_of: datetime,
        code_commit: str | None = None,
    ) -> None:
        if (
            self.snapshot_id != snapshot_id
            or self.binding_hash != binding_hash
            or (code_commit is not None and self.code_commit != code_commit)
        ):
            raise ValueError("context differs from original price binding")
        if self.industry is not None and self.industry.captured_at > as_of:
            raise ValueError("industry capture follows request cutoff")
        if (mode in ("industry", "industry_size") and self.industry is None) or (
            mode == "industry_size" and self.market_cap is None
        ):
            raise ValueError("neutralization requires its context sources")


def require_factor_neutralization_binding(
    context: FactorNeutralizationSources | None,
    *,
    mode: NeutralizationMode,
    snapshot_id: str,
    binding_hash: str,
    as_of: datetime,
    code_commit: str | None = None,
) -> None:
    if context is None:
        if mode != "none":
            raise ValueError("neutralization lacks context")
    else:
        context.require_binding(
            mode=mode,
            snapshot_id=snapshot_id,
            binding_hash=binding_hash,
            as_of=as_of,
            code_commit=code_commit,
        )


class FactorNeutralizationContext(BaseModel):
    model_config = RUN_IMMUTABLE
    schema_version: Literal[1] = 1
    prepared_source_sha256: Sha256
    snapshot_id: Sha256
    binding_hash: Sha256
    scope_content_hash: Sha256
    scope: FactorComputationScope
    generation: FactorSourceGeneration
    code_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    industry: FactorIndustrySource | None = Field(default=None, exclude_if=lambda v: v is None)
    market_cap: FactorMarketCapSource | None = Field(default=None, exclude_if=lambda v: v is None)

    @model_validator(mode="after")
    def _paired(self) -> FactorNeutralizationContext:
        if self.industry is None and self.market_cap is None:
            raise ValueError("context has no sources")
        for source in (self.industry, self.market_cap):
            if source is not None and (
                source.prepared_source_sha256 != self.prepared_source_sha256
                or source.prepared_snapshot_id != self.snapshot_id
                or source.prepared_binding_hash != self.binding_hash
                or source.scope_content_hash != self.scope_content_hash
                or source.scope != self.scope
                or source.code_commit != self.code_commit
            ):
                raise ValueError("context differs from its prepared price source")
        if self.market_cap is not None and self.market_cap.generation != self.generation:
            raise ValueError("market cap uses another RO generation")
        if self.industry is not None and self.industry.captured_at > self.scope.as_of_time:
            raise ValueError("industry capture follows prepared cutoff")
        return self

    def require_prepared(self, prepared: FactorPreparedStreamSource) -> None:
        if (
            self.prepared_source_sha256 != prepared.sha256
            or self.snapshot_id != prepared.admission_request.snapshot_id
            or self.binding_hash != prepared.admission_request.binding_hash
            or self.scope_content_hash != prepared.scope_content_hash
            or self.scope != prepared.admission_request.scope
            or self.generation != prepared.receipt.generation
            or self.code_commit != prepared.snapshot.code_commit
        ):
            raise ValueError("context is not paired with this prepared source")

    @property
    def sources(self) -> FactorNeutralizationSources:
        return FactorNeutralizationSources(
            context_sha256=canonical_sha256(self),
            prepared_source_sha256=self.prepared_source_sha256,
            snapshot_id=self.snapshot_id,
            binding_hash=self.binding_hash,
            scope_content_hash=self.scope_content_hash,
            code_commit=self.code_commit,
            industry=None
            if self.industry is None
            else FactorIndustryContextBinding(
                source_sha256=self.industry.sha256,
                collection_sha256=self.industry.collection_sha256,
                captured_at=self.industry.captured_at,
            ),
            market_cap=None
            if self.market_cap is None
            else FactorMarketCapContextBinding(
                source_sha256=self.market_cap.sha256,
                generation_sha256=canonical_sha256(self.generation),
            ),
        )


def bind_factor_neutralization_context(
    prepared: FactorPreparedStreamSource,
    *,
    industry: FactorIndustrySource | None = None,
    market_cap: FactorMarketCapSource | None = None,
) -> FactorNeutralizationContext:
    prepared = FactorPreparedStreamSource.model_validate(prepared)
    context = FactorNeutralizationContext(
        prepared_source_sha256=prepared.sha256,
        snapshot_id=prepared.admission_request.snapshot_id,
        binding_hash=prepared.admission_request.binding_hash,
        scope_content_hash=prepared.scope_content_hash,
        scope=prepared.admission_request.scope,
        generation=prepared.receipt.generation,
        code_commit=prepared.snapshot.code_commit,
        industry=industry,
        market_cap=market_cap,
    )
    context.require_prepared(prepared)
    return context


def select_factor_neutralization_context(
    context: FactorNeutralizationContext | None, *, industry: bool, market_cap: bool
) -> FactorNeutralizationContext | None:
    if not industry and not market_cap:
        return None
    if (
        context is None
        or (industry and context.industry is None)
        or (market_cap and context.market_cap is None)
    ):
        raise ValueError("缺少可核验的行业或市值来源")
    return FactorNeutralizationContext.model_validate(
        {
            **context.model_dump(),
            "industry": context.industry if industry else None,
            "market_cap": context.market_cap if market_cap else None,
        }
    )


class FactorNeutralizationDayBatch(BaseModel):
    model_config = RUN_IMMUTABLE
    sources: FactorNeutralizationSources
    trade_date: date
    panel_date: date
    stock_codes: tuple[StockCode, ...] = Field(min_length=1, max_length=7000)
    assumed_visible_at: ObservedTime
    visibility_basis: Literal["retrospective_adapter_assumption"] = (
        "retrospective_adapter_assumption"
    )
    industry_facts: tuple[FactorIndustryFact, ...] | None = Field(
        default=None, max_length=7000, exclude_if=lambda v: v is None
    )
    market_cap_facts: tuple[FactorMarketCapFact, ...] | None = Field(
        default=None, max_length=7000, exclude_if=lambda v: v is None
    )

    @model_validator(mode="after")
    def _grid(self) -> FactorNeutralizationDayBatch:
        if (
            tuple(sorted(set(self.stock_codes))) != self.stock_codes
            or self.panel_date >= self.trade_date
        ):
            raise ValueError("context grid or previous panel date is invalid")
        for binding, facts in (
            (self.sources.industry, self.industry_facts),
            (self.sources.market_cap, self.market_cap_facts),
        ):
            if (binding is None) != (facts is None):
                raise ValueError("context fact sources differ")
            if facts is not None and (
                tuple(f.stock_code for f in facts) != self.stock_codes
                or any(f.trade_date != self.panel_date for f in facts)
            ):
                raise ValueError("context facts differ from complete panel grid")
        return self

    @property
    def sha256(self) -> str:
        # Raw invalid caps serialize as explicit strings; their status and value remain bound.
        return canonical_sha256(json.loads(self.model_dump_json()))

    def observations(
        self,
    ) -> tuple[dict[str, IndustryObservation], dict[str, MarketCapObservation]]:
        # The observation time is explicitly an adapter assumption, not raw historical PIT.
        industries = {
            fact.stock_code: IndustryObservation(
                stock_code=fact.stock_code,
                trade_date=self.trade_date,
                industry=fact.l1_code if fact.status == "valid" else None,
                first_visible_at=self.assumed_visible_at,
            )
            for fact in self.industry_facts or ()
        }
        caps = {
            fact.stock_code: MarketCapObservation(
                stock_code=fact.stock_code,
                trade_date=self.trade_date,
                market_cap=fact.total_mv if fact.status == "valid" else None,
                first_visible_at=self.assumed_visible_at,
            )
            for fact in self.market_cap_facts or ()
        }
        return industries, caps


class FactorNeutralizationReadLease:
    def __init__(
        self,
        context: FactorNeutralizationContext,
        industry: FactorIndustryReadLease | None,
        market_cap: FactorMarketCapReadLease | None,
    ) -> None:
        self.context, self.industry, self.market_cap = context, industry, market_cap
        self.closed = False
        self.read_query_count = 0

    def query(
        self,
        *,
        trade_date: date,
        panel_date: date,
        stock_codes: tuple[str, ...],
        assumed_visible_at: datetime,
    ) -> FactorNeutralizationDayBatch:
        if self.closed:
            raise RuntimeError("context reader is closed")
        if (
            not set(stock_codes) <= set(self.context.scope.stock_codes)
            or not self.context.scope.start_date
            <= panel_date
            < trade_date
            <= self.context.scope.end_date
        ):
            raise ValueError("context query exceeds prepared scope")
        industries, caps = [], []
        for start in range(0, len(stock_codes), 500):
            codes = stock_codes[start : start + 500]
            if self.industry is not None:
                query = FactorIndustryQuery(
                    source_sha256=self.context.industry.sha256,
                    trade_date=panel_date,
                    stock_codes=codes,
                )
                batch = FactorIndustryDayBatch.model_validate(self.industry.query(query))
                if (
                    batch.query != query
                    or batch.prepared_source_sha256 != self.context.prepared_source_sha256
                ):
                    raise ValueError("industry reader returned another query or source")
                industries.extend(batch.facts)
                self.read_query_count += 1
            if self.market_cap is not None:
                query = FactorMarketCapQuery(
                    source_sha256=self.context.market_cap.sha256,
                    trade_date=panel_date,
                    stock_codes=codes,
                )
                batch = FactorMarketCapDayBatch.model_validate(self.market_cap.query(query))
                if (
                    batch.query != query
                    or batch.prepared_source_sha256 != self.context.prepared_source_sha256
                ):
                    raise ValueError("market cap reader returned another query or source")
                caps.extend(batch.facts)
                self.read_query_count += 1
        return FactorNeutralizationDayBatch(
            sources=self.context.sources,
            trade_date=trade_date,
            panel_date=panel_date,
            stock_codes=stock_codes,
            assumed_visible_at=assumed_visible_at,
            industry_facts=tuple(industries) if self.industry is not None else None,
            market_cap_facts=tuple(caps) if self.market_cap is not None else None,
        )


def _artifact_identities(
    context: FactorNeutralizationContext, lake_root: Path
) -> dict[str, tuple[int, ...]]:
    from rquant.factor.result_artifact import _file_identity

    result = {}
    for source in (context.industry, context.market_cap):
        if source is not None:
            path = verify_materialized_table_artifact(
                source.artifact, lake_root=lake_root, as_of_time=context.scope.as_of_time
            )
            result[str(path)] = _file_identity(os.stat(path, follow_symlinks=False))
    return result


@contextmanager
def open_factor_neutralization_context(
    context: FactorNeutralizationContext, *, lake_root: Path
) -> Iterator[FactorNeutralizationReadLease]:
    context = FactorNeutralizationContext.model_validate(context)
    identities = _artifact_identities(context, lake_root)
    with ExitStack() as stack:
        industry = (
            stack.enter_context(open_factor_industry_source(context.industry, lake_root=lake_root))
            if context.industry is not None
            else None
        )
        cap = (
            stack.enter_context(
                open_factor_market_cap_source(context.market_cap, lake_root=lake_root)
            )
            if context.market_cap is not None
            else None
        )
        lease = FactorNeutralizationReadLease(context, industry, cap)
        try:
            yield lease
            if _artifact_identities(context, lake_root) != identities:
                raise ValueError("context artifacts changed during execution")
        finally:
            lease.closed = True
