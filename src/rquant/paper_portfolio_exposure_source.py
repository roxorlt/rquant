"""Trusted benchmark and period materials use the original exposure calculations."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Self

from pydantic import Field, model_validator

from rquant.paper_portfolio_exposure import PaperBenchmarkIndustryWeight, PaperIndustryMaterials, PaperAttributionMaterials, PaperExposureView, calculate_paper_exposure
from rquant.paper_portfolio_ledger import PaperPortfolioLedgerFrame
from rquant.paper_portfolio_models import Sha256
from rquant.paper_portfolio_source import PaperPortfolioRawFact
from rquant.paper_portfolio_state import PaperPortfolioStateStore
from rquant.portfolio.exposure import ExposureInput, IndustryWeight
from rquant.research_run_spec import _parse_decimal
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256


class PaperBenchmarkSnapshot(RuntimeContractModel):
    configuration_fingerprint: Sha256
    observed_at: AwareUtcDatetime
    available_at: AwareUtcDatetime
    valid_through: AwareUtcDatetime
    source_identity: Sha256
    weights: tuple[PaperBenchmarkIndustryWeight, ...] = Field(max_length=500)
    cash_weight: Decimal = Field(ge=0, le=1, allow_inf_nan=False)

    @model_validator(mode="before")
    @classmethod
    def admission(cls, value: object) -> object:
        if isinstance(value, dict):
            _parse_decimal(value.get("cash_weight"), field_name="benchmark cash weight")
        return value

    @model_validator(mode="after")
    def exact_weights(self) -> Self:
        if not self.observed_at <= self.available_at <= self.valid_through or len({item.industry_l1 for item in self.weights}) != len(self.weights):
            raise ValueError("paper benchmark dates or industry identities differ")
        ExposureInput(industries=tuple(IndustryWeight(industry_l1=item.industry_l1, portfolio_weight=0, benchmark_weight=item.weight) for item in self.weights),
                      portfolio_cash_weight=1, benchmark_cash_weight=self.cash_weight)
        return self


class PaperPeriodAttributionInput(RuntimeContractModel):
    start_frame: PaperPortfolioLedgerFrame
    industries: PaperIndustryMaterials
    period: PaperAttributionMaterials

    @model_validator(mode="after")
    def same_original_start(self) -> Self:
        if (self.start_frame.configuration_fingerprint, self.start_frame.fingerprint, self.start_frame.as_of) != (
                self.period.configuration_fingerprint, self.period.start_frame_fingerprint, self.period.start_at):
            raise ValueError("paper attribution requires its original period-start frame")
        if self.industries.ledger_frame_fingerprint != self.start_frame.fingerprint or len(self.model_dump_json().encode()) > 1024*1024:
            raise ValueError("paper period source differs or exceeds its material budget")
        return self

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class PaperPublishedAttribution(RuntimeContractModel):
    source: PaperPeriodAttributionInput
    view: PaperExposureView

    @model_validator(mode="after")
    def exact_source(self) -> Self:
        if (self.view.configuration_fingerprint, self.view.ledger_frame_fingerprint, self.view.material_fingerprint,
                self.view.attribution_material_fingerprint) != (self.source.start_frame.configuration_fingerprint,
                self.source.start_frame.fingerprint, self.source.industries.fingerprint, self.source.period.fingerprint):
            raise ValueError("paper period result differs from its complete source")
        return self


class PaperPortfolioExposureStore:
    def __init__(self, state: PaperPortfolioStateStore) -> None:
        if type(state) is not PaperPortfolioStateStore:
            raise TypeError("paper industry producer requires its concrete private metadata")
        self.state = state
        with state._connection(write=True) as connection:
            connection.execute("CREATE TABLE IF NOT EXISTS paper_benchmarks(configuration TEXT NOT NULL,available_at TEXT NOT NULL,body TEXT NOT NULL,PRIMARY KEY(configuration,available_at))")
            connection.execute("CREATE TABLE IF NOT EXISTS paper_attribution(configuration TEXT PRIMARY KEY,body TEXT NOT NULL)")

    def publish_benchmark(self, value: PaperBenchmarkSnapshot) -> None:
        value = PaperBenchmarkSnapshot.model_validate(value.model_dump(mode="python"))
        if value.configuration_fingerprint != self.state.configuration.fingerprint:
            raise ValueError("paper benchmark configuration differs")
        with self.state._connection(write=True) as connection:
            old = connection.execute("SELECT body FROM paper_benchmarks WHERE configuration=? AND available_at=?", (value.configuration_fingerprint, value.available_at.isoformat())).fetchone()
            if old is not None:
                if PaperBenchmarkSnapshot.model_validate_json(old[0]) != value:
                    raise ValueError("original paper benchmark at this cutoff differs")
                return
            count = connection.execute("SELECT count(*) FROM paper_benchmarks WHERE configuration=?", (value.configuration_fingerprint,)).fetchone()[0]
            if count >= 2520:
                raise ValueError("paper benchmark source exceeds its day budget")
            connection.execute("INSERT INTO paper_benchmarks VALUES(?,?,?)", (value.configuration_fingerprint, value.available_at.isoformat(), value.model_dump_json()))

    def exposure(self, frame: PaperPortfolioLedgerFrame, *, facts: tuple[PaperPortfolioRawFact, ...], as_of: datetime) -> PaperExposureView | None:
        with self.state._connection() as connection:
            row = connection.execute("SELECT body FROM paper_benchmarks WHERE configuration=? AND available_at<=? ORDER BY available_at DESC LIMIT 1", (frame.configuration_fingerprint, as_of.isoformat())).fetchone()
        if row is None:
            return None
        benchmark = PaperBenchmarkSnapshot.model_validate_json(row[0])
        if benchmark.valid_through < as_of:
            return None
        material = PaperIndustryMaterials(configuration_fingerprint=frame.configuration_fingerprint, ledger_frame_fingerprint=frame.fingerprint,
                                          observed_at=max(benchmark.observed_at, max((item.observed_at for item in facts), default=benchmark.observed_at)),
                                          available_at=max(benchmark.available_at, max((item.available_at for item in facts), default=benchmark.available_at)),
                                          benchmark_source_identity=benchmark.source_identity, benchmark_weights=benchmark.weights,
                                          benchmark_cash_weight=benchmark.cash_weight, facts=facts)
        return calculate_paper_exposure(frame, material, as_of=as_of)

    def publish_attribution(self, value: PaperPeriodAttributionInput, *, as_of: datetime) -> None:
        value = PaperPeriodAttributionInput.model_validate(value.model_dump(mode="python"))
        if value.start_frame.configuration_fingerprint != self.state.configuration.fingerprint:
            raise ValueError("paper period configuration differs")
        with self.state._connection(write=True) as connection:
            row = connection.execute("SELECT body FROM paper_attribution WHERE configuration=?", (value.start_frame.configuration_fingerprint,)).fetchone()
            old = PaperPublishedAttribution.model_validate_json(row[0]) if row else None
            if old is not None:
                if old.source == value:
                    return
                if value.period.end_at <= old.source.period.end_at:
                    raise ValueError("original paper period was changed or rolled back")
            result = PaperPublishedAttribution(source=value, view=calculate_paper_exposure(value.start_frame, value.industries, as_of=as_of, attribution=value.period))
            connection.execute("INSERT OR REPLACE INTO paper_attribution VALUES(?,?)", (value.start_frame.configuration_fingerprint, result.model_dump_json()))

    def attribution(self, *, as_of: datetime) -> PaperPublishedAttribution | None:
        with self.state._connection() as connection:
            row = connection.execute("SELECT body FROM paper_attribution WHERE configuration=?", (self.state.configuration.fingerprint,)).fetchone()
        value = PaperPublishedAttribution.model_validate_json(row[0]) if row else None
        return value if value is not None and value.source.period.available_at <= as_of else None
