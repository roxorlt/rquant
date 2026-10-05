"""Complete typed paper facts restored together from one original Serving owner."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from decimal import Decimal
from typing import Literal, Self

from pydantic import Field, model_validator

from rquant.paper_operator_commands import PaperOperatorApplication
from rquant.paper_portfolio_band import PaperBacktestBandResult, require_complete_band_dates
from rquant.paper_portfolio_exposure import PaperExposureView
from rquant.paper_portfolio_exposure_source import PaperPublishedAttribution
from rquant.paper_research_artifact import PaperResearchSummary
from rquant.paper_portfolio_ledger import PaperPortfolioLedgerFrame
from rquant.paper_portfolio_models import PaperPortfolioConfiguration, PaperPortfolioStateIdentity, PaperRiskObservation, Sha256
from rquant.paper_portfolio_projection_contract import MAX_PAPER_PUBLICATION_BYTES, PAPER_PORTFOLIO_PROJECTION_TABLES
from rquant.paper_portfolio_source import PaperPortfolioMarketSnapshot
from rquant.paper_portfolio_views import PaperDailyNav
from rquant.paper_portfolio_reductions import PaperReductionStatus
from rquant.backtest.contracts import SSECalendar
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.strict_json import strict_model_validate_json
from rquant.serving_read_models import ServingProjectionInput, ServingProjectionPayload


class PaperDailyNavPoint(RuntimeContractModel):
    trade_date: date
    configuration_fingerprint: Sha256
    calendar_source_identity: Sha256
    ledger_revision: int = Field(strict=True, ge=1)
    ledger_head_fingerprint: Sha256
    material_fingerprint: Sha256
    close_at: AwareUtcDatetime
    published_at: AwareUtcDatetime
    status: Literal["complete", "unavailable"]
    nav: Decimal | None = Field(default=None, gt=0, le=Decimal("1000000000000"), allow_inf_nan=False)
    cash: Decimal | None = Field(default=None, ge=0, le=Decimal("1000000000000"), allow_inf_nan=False)
    normalized_nav: Decimal | None = Field(default=None, gt=0, allow_inf_nan=False)
    daily_return: Decimal | None = Field(default=None, gt=-1, allow_inf_nan=False)
    reason: str | None = Field(default=None, max_length=512)

    @model_validator(mode="after")
    def exact_gap(self) -> Self:
        if self.close_at > self.published_at or ((self.status == "complete") != (self.nav is not None and self.cash is not None and self.normalized_nav is not None)):
            raise ValueError("paper published NAV lacks its original completed close")
        if self.status == "unavailable" and (self.daily_return is not None or not self.reason):
            raise ValueError("paper close gap cannot carry a substituted return")
        return self

    @classmethod
    def from_record(cls, value: PaperDailyNav) -> Self:
        return cls(**value.model_dump(mode="python", exclude={"account", "account_id"}),
                   nav=value.account.nav if value.account else None, cash=value.account.cash if value.account else None)


class PaperPortfolioPublishedAccount(RuntimeContractModel):
    metadata_identity: PaperPortfolioStateIdentity | None = None
    configuration: PaperPortfolioConfiguration
    operator: PaperOperatorApplication
    frame: PaperPortfolioLedgerFrame | None = None
    market_material: PaperPortfolioMarketSnapshot | None = None
    status: Literal["complete", "unavailable"]
    reason: str | None = Field(default=None, max_length=512)
    nav: tuple[PaperDailyNavPoint, ...] = Field(max_length=2520)
    risk: PaperRiskObservation | None = None
    exposure: PaperExposureView | None = None
    exposure_reason: str | None = Field(default=None, max_length=512)
    attribution: PaperPublishedAttribution | None = None
    reduction: PaperReductionStatus | None = None
    calendar: SSECalendar | None = None
    band: PaperBacktestBandResult | None = None
    recent_research: tuple[PaperResearchSummary, ...] = Field(default=(), max_length=20)

    @model_validator(mode="after")
    def same_account(self) -> Self:
        configuration, operator = self.configuration, self.operator
        fingerprint = configuration.fingerprint
        if operator.account_id != configuration.binding.account_id or operator.configuration_fingerprint != fingerprint:
            raise ValueError("paper applied control belongs to a different configuration")
        if (self.status == "complete" and (self.frame is None or self.frame.account is None)) or (self.status == "unavailable" and not self.reason):
            raise ValueError("paper current valuation coverage is missing")
        if self.frame is not None:
            if self.frame.configuration_fingerprint != fingerprint or self.frame.account_id != configuration.binding.account_id:
                raise ValueError("paper view belongs to a different original ledger")
            if self.status == "complete" and self.market_material is None:
                raise ValueError("paper current valuation lacks its bound raw material")
            if self.market_material is not None:
                facts = {item.ts_code: item for item in self.market_material.facts}
                if (self.market_material.binding != configuration.binding or self.market_material.configuration_fingerprint != fingerprint
                        or self.market_material.available_at > self.frame.as_of
                        or (self.frame.account is not None and any(item.code not in facts or facts[item.code].valuation_price != item.market_price for item in self.frame.account.holdings))):
                    raise ValueError("paper current valuation differs from its raw facts")
        days = tuple(item.trade_date for item in self.nav)
        if days != tuple(sorted(set(days))) or any(item.configuration_fingerprint != fingerprint for item in self.nav):
            raise ValueError("paper NAV sequence mixes dates or configurations")
        if self.nav and (self.calendar is None or any(item.calendar_source_identity != self.calendar.source_identity or item.trade_date not in self.calendar.dates for item in self.nav)):
            raise ValueError("paper NAV sequence differs from its original open-day calendar")
        if self.nav:
            require_complete_band_dates(days, self.calendar)
        if self.risk is not None and (self.risk.account_id != configuration.binding.account_id or self.risk.configuration_fingerprint != fingerprint):
            raise ValueError("paper risk belongs to a different configured account")
        if self.exposure is not None and (self.frame is None or self.exposure.configuration_fingerprint != fingerprint or self.exposure.ledger_frame_fingerprint != self.frame.fingerprint):
            raise ValueError("paper industry view is detached from the original ledger")
        if self.band is not None and self.band.configuration_fingerprint != fingerprint:
            raise ValueError("paper backtest band belongs to a different configuration")
        if self.attribution is not None and self.attribution.source.start_frame.configuration_fingerprint != fingerprint:
            raise ValueError("paper period attribution belongs to a different configuration")
        if any(item.account_id != configuration.binding.account_id for item in self.recent_research):
            raise ValueError("paper research publication belongs to a different account")
        if self.reduction is not None and (self.reduction.configuration_fingerprint != fingerprint or self.frame is None
                                           or self.reduction.ledger_frame_fingerprint != self.frame.fingerprint):
            raise ValueError("paper reduction fact lacks its actual original ledger")
        return self

    def complete_comparison_dates(self) -> tuple[date, ...] | None:
        if not self.nav or self.calendar is None or any(item.status != "complete" or item.daily_return is None for item in self.nav):
            return None
        return require_complete_band_dates(tuple(item.trade_date for item in self.nav), self.calendar)


class PaperPortfolioSnapshot(RuntimeContractModel):
    contract: Literal["paper-portfolio-snapshot/v1"] = "paper-portfolio-snapshot/v1"
    available_at: AwareUtcDatetime
    accounts: tuple[PaperPortfolioPublishedAccount, ...] = Field(max_length=64)

    @model_validator(mode="after")
    def same_publication(self) -> Self:
        keys = tuple(item.configuration.binding.account_id for item in self.accounts)
        if keys != tuple(sorted(set(keys))):
            raise ValueError("paper portfolio directory must be finite, sorted and unique")
        for item in self.accounts:
            times = [item.configuration.configured_at]
            if item.operator.observed_at is not None:
                times.append(item.operator.observed_at)
            if item.frame is not None:
                if item.frame.as_of != self.available_at:
                    raise ValueError("paper complete frame must use the publication cutoff")
            times.extend(point.published_at for point in item.nav)
            if item.risk is not None:
                times.append(item.risk.observed_at)
            if item.exposure is not None:
                times.append(item.exposure.as_of)
            if item.attribution is not None:
                times.extend((item.attribution.source.period.available_at, item.attribution.view.as_of))
            times.extend(result.sealed.completed_at for result in item.recent_research if result.sealed is not None)
            times.extend(result.accepted_at for result in item.recent_research)
            if any(value > self.available_at for value in times):
                raise ValueError("paper publication contains future facts")
        if len(self.model_dump_json().encode()) > MAX_PAPER_PUBLICATION_BYTES:
            raise ValueError("paper complete publication exceeds its bounded material budget")
        return self

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))

    def for_owner(self, owner_id: str) -> tuple[PaperPortfolioPublishedAccount, ...]:
        return tuple(item for item in self.accounts if item.configuration.binding.owner_id == owner_id)


def _account_index(value: PaperPortfolioPublishedAccount) -> dict[str, str | int]:
    return {"account_id": value.configuration.binding.account_id, "owner_id": value.configuration.binding.owner_id,
            "configuration_fingerprint": value.configuration.fingerprint, "configuration_version": value.configuration.version,
            "ledger_revision": value.frame.ledger_revision if value.frame else 0,
            "applied_sequence": value.operator.sequence if value.operator.status == "applied" else 0}


def paper_portfolio_projections(value: PaperPortfolioSnapshot) -> tuple[ServingProjectionPayload, ...]:
    value = PaperPortfolioSnapshot.model_validate(value.model_dump(mode="python"))
    raw = value.model_dump_json()
    chunks: list[str] = []
    part, used = [], 0
    for char in raw:
        size = len(char.encode("utf-8"))
        if used+size > 16*1024:
            chunks.append("".join(part))
            part, used = [], 0
        part.append(char)
        used += size
    if part:
        chunks.append("".join(part))
    digest = value.fingerprint
    rows = ({"snapshot_hash": digest, "as_of_time": value.available_at.isoformat(), "account_count": len(value.accounts),
             "material_bytes": len(raw.encode()), "material_chunks": len(chunks)},)
    return (ServingProjectionPayload(table_name="paper_portfolio_state", available_at=value.available_at, rows=rows),
            ServingProjectionPayload(table_name="paper_portfolio_account", available_at=value.available_at, rows=tuple(_account_index(item) for item in value.accounts)),
            ServingProjectionPayload(table_name="paper_portfolio_material", available_at=value.available_at,
                                     rows=tuple({"snapshot_hash": digest, "chunk_index": index, "payload": chunk} for index, chunk in enumerate(chunks))))


def validate_paper_portfolio_projections(tables: Mapping[str, ServingProjectionPayload | ServingProjectionInput]) -> PaperPortfolioSnapshot | None:
    selected = {name: tables[name] for name in PAPER_PORTFOLIO_PROJECTION_TABLES if name in tables}
    if not selected:
        return None
    if set(selected) != PAPER_PORTFOLIO_PROJECTION_TABLES:
        raise ValueError("paper complete projection graph is incomplete")
    points = tuple(selected.values())
    if len({value.available_at for value in points}) != 1 or any(type(value) is ServingProjectionInput and value.owner_dataset_id != "paper_accounts" for value in points):
        raise ValueError("paper projection cutoffs or owners differ")
    bound = tuple(value for value in points if type(value) is ServingProjectionInput)
    if bound and (len(bound) != len(points) or len({value.owner_generation_id for value in bound}) != 1):
        raise ValueError("paper projection generations differ")
    state_rows = selected["paper_portfolio_state"].rows
    chunks = selected["paper_portfolio_material"].rows
    if len(state_rows) != 1:
        raise ValueError("paper complete projection state is absent")
    state = state_rows[0]
    if (type(state["material_chunks"]) is not int or state["material_chunks"] != len(chunks)
            or tuple(row["chunk_index"] for row in chunks) != tuple(range(len(chunks)))
            or any(row["snapshot_hash"] != state["snapshot_hash"] for row in chunks)
            or any(not isinstance(row["payload"], str) or len(row["payload"].encode()) > 16*1024 for row in chunks)):
        raise ValueError("paper complete material chunks are detached or missing")
    raw = "".join(str(row["payload"]) for row in chunks)
    if len(raw.encode()) != state["material_bytes"] or len(raw.encode()) > MAX_PAPER_PUBLICATION_BYTES:
        raise ValueError("paper complete material size differs")
    value = strict_model_validate_json(PaperPortfolioSnapshot, raw)
    if (value.fingerprint != state["snapshot_hash"] or value.model_dump_json() != raw or value.available_at != points[0].available_at
            or value.available_at.isoformat() != state["as_of_time"] or len(value.accounts) != state["account_count"]
            or tuple(dict(row) for row in selected["paper_portfolio_account"].rows) != tuple(_account_index(item) for item in value.accounts)):
        raise ValueError("paper complete projection graph differs from its typed original facts")
    return value
