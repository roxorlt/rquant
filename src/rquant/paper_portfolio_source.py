"""Bounded producer facts consumed by the exact configured paper role."""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
from typing import Literal, Self

from pydantic import Field, StrictBool, model_validator

from rquant.paper_portfolio_models import MAX_PAPER_CODES, MAX_PAPER_MATERIAL_BYTES, PaperPortfolioBinding, Sha256
from rquant.paper_portfolio_state import PaperPortfolioStateStore
from rquant.research_run_spec import _parse_decimal
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.signal_contracts import SignalEnvelopeFamily


class PaperPortfolioRawFact(RuntimeContractModel):
    ts_code: str = Field(pattern=r"^\d{6}\.(SH|SZ|BJ)$")
    candidate: StrictBool = True
    rank_score: Decimal | None = Field(default=None, allow_inf_nan=False)
    industry_l1: str | None = Field(default=None, min_length=1, max_length=80)
    valuation_price: Decimal | None = Field(default=None, gt=0, le=Decimal("1000000000"), allow_inf_nan=False)
    trading_status: Literal["normal", "suspended", "missing", "error"]
    observed_at: AwareUtcDatetime
    available_at: AwareUtcDatetime
    source_snapshot_id: Sha256

    @model_validator(mode="before")
    @classmethod
    def numeric_admission(cls, value: object) -> object:
        if isinstance(value, Mapping):
            for key in ("rank_score", "valuation_price"):
                if value.get(key) is not None:
                    _parse_decimal(value[key], field_name=key)
        return value

    @model_validator(mode="after")
    def source_time(self) -> Self:
        if self.observed_at > self.available_at or (self.candidate and self.rank_score is None):
            raise ValueError("paper producer fact lacks an available full ranking score")
        if self.trading_status in ("missing", "error") and self.valuation_price is not None:
            raise ValueError("missing paper valuation cannot carry a substituted price")
        return self


class PaperPortfolioMarketSnapshot(RuntimeContractModel):
    contract: Literal["paper-portfolio-market/v1"] = "paper-portfolio-market/v1"
    binding: PaperPortfolioBinding
    configuration_fingerprint: Sha256
    dataset_snapshot_id: Sha256
    feature_snapshot_id: Sha256
    observed_at: AwareUtcDatetime
    available_at: AwareUtcDatetime
    facts: tuple[PaperPortfolioRawFact, ...] = Field(min_length=1, max_length=MAX_PAPER_CODES)

    @model_validator(mode="after")
    def complete_facts(self) -> Self:
        if (self.observed_at > self.available_at or len({item.ts_code for item in self.facts}) != len(self.facts)
                or any(item.observed_at > self.observed_at or item.available_at > self.available_at for item in self.facts)):
            raise ValueError("paper full ranking and valuation facts have inconsistent cutoffs")
        if len(self.model_dump_json().encode()) > MAX_PAPER_MATERIAL_BYTES:
            raise ValueError("paper producer facts exceed their byte budget")
        return self

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class PaperPortfolioMaterialStore:
    def __init__(self, state: PaperPortfolioStateStore) -> None:
        self.state = state
        with state._connection(write=True) as connection:
            connection.execute("CREATE TABLE IF NOT EXISTS portfolio_market_materials(digest TEXT PRIMARY KEY,configuration TEXT NOT NULL,available_at TEXT NOT NULL,body TEXT NOT NULL,UNIQUE(configuration,available_at))")

    def publish(self, value: PaperPortfolioMarketSnapshot) -> PaperPortfolioMarketSnapshot:
        value = PaperPortfolioMarketSnapshot.model_validate(value.model_dump(mode="python"))
        configuration = self.state.configuration
        if value.binding != configuration.binding or value.configuration_fingerprint != configuration.fingerprint:
            raise ValueError("paper producer facts do not belong to this exact configured role")
        with self.state._connection(write=True) as connection:
            old = connection.execute("SELECT body FROM portfolio_market_materials WHERE configuration=? AND available_at=?",
                                     (value.configuration_fingerprint, value.available_at.isoformat())).fetchone()
            if old is not None:
                if PaperPortfolioMarketSnapshot.model_validate_json(old[0]) != value:
                    raise ValueError("paper facts at the original cutoff differ")
                return value
            connection.execute("INSERT INTO portfolio_market_materials VALUES(?,?,?,?)",
                               (value.fingerprint, value.configuration_fingerprint, value.available_at.isoformat(), value.model_dump_json()))
        return value

    def latest(self, *, decision_at: AwareUtcDatetime) -> PaperPortfolioMarketSnapshot:
        with self.state._connection() as connection:
            rows = connection.execute("SELECT digest,body FROM portfolio_market_materials WHERE configuration=? AND available_at<=? ORDER BY available_at DESC LIMIT 1",
                                      (self.state.configuration.fingerprint, decision_at.isoformat())).fetchall()
        if not rows:
            raise ValueError("缺少决策前的完整排名与估值")
        value = PaperPortfolioMarketSnapshot.model_validate_json(rows[0]["body"])
        if (value.fingerprint != rows[0]["digest"] or value.binding != self.state.configuration.binding
                or value.configuration_fingerprint != self.state.configuration.fingerprint
                or value.available_at > decision_at):
            raise ValueError("paper material projection differs from the original configured source")
        return value

    def read_for(self, signal: SignalEnvelopeFamily, *, decision_at: AwareUtcDatetime) -> PaperPortfolioMarketSnapshot:
        value = self.latest(decision_at=decision_at)
        if value.dataset_snapshot_id != signal.dataset_snapshot_id or value.feature_snapshot_id != signal.feature_snapshot_id:
            raise ValueError("paper material projection differs from the original signal source")
        return value
