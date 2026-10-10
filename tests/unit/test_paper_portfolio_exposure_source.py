"""Published industry facts keep actual current and period-start weights apart."""

from datetime import timedelta
from decimal import Decimal
import pytest

from tests.unit.test_paper_portfolio_publication_sequence import fixture


def test_current_industry_and_period_bf_publish_from_original_distinct_frames(tmp_path):
    from rquant.paper_portfolio_exposure_source import PaperBenchmarkSnapshot, PaperPortfolioExposureStore, PaperPeriodAttributionInput
    from rquant.paper_portfolio_exposure import PaperAttributionMaterials, PaperIndustryMaterials, calculate_paper_exposure
    from tests.unit.test_paper_portfolio_view_source import market

    source, value = fixture(tmp_path)
    store = PaperPortfolioExposureStore(source.runtime.state)
    source.runtime.exposure_store = store
    at, start = value.available_at, value.accounts[0].frame
    benchmark = PaperBenchmarkSnapshot(configuration_fingerprint=start.configuration_fingerprint, observed_at=at, available_at=at,
                                       valid_through=at+timedelta(days=1), source_identity="b"*64,
                                       weights=({"industry_l1": "银行", "weight": 1},), cash_weight=0)
    store.publish_benchmark(benchmark)
    current = source.read(as_of=at)
    assert current.exposure.exposure.rows[0].portfolio_weight == (Decimal(1600)/Decimal(1795)).quantize(Decimal("1e-18"))
    material = PaperIndustryMaterials(configuration_fingerprint=start.configuration_fingerprint, ledger_frame_fingerprint=start.fingerprint,
                                      observed_at=at, available_at=at, benchmark_source_identity=benchmark.source_identity,
                                      benchmark_weights=benchmark.weights, benchmark_cash_weight=0, facts=value.accounts[0].market_material.facts)
    end = at+timedelta(seconds=1)
    returns = PaperAttributionMaterials(configuration_fingerprint=start.configuration_fingerprint, start_frame_fingerprint=start.fingerprint,
                                        start_at=at, end_at=end, available_at=end, source_identity="f"*64,
                                        returns=({"industry_l1": "银行", "portfolio_return": ".1", "benchmark_return": ".08",
                                                  "observed_at": end, "available_at": end, "source_identity": "f"*64},))
    frozen = PaperPeriodAttributionInput(start_frame=start, industries=material, period=returns)
    reference = calculate_paper_exposure(start, material, as_of=end, attribution=returns)
    store.publish_attribution(frozen, as_of=end)
    market(source.runtime, at=end, price="3")
    current = source.read(as_of=end)
    assert current.frame.account.nav == 2595
    assert current.attribution.view.attribution == reference.attribution
    assert current.attribution.source.start_frame.fingerprint == start.fingerprint
    assert current.attribution.view.ledger_frame_fingerprint != current.frame.fingerprint
    with pytest.raises(ValueError):
        store.publish_attribution(frozen.model_copy(update={"start_frame": current.frame}), as_of=end)
    with pytest.raises(ValueError):
        store.publish_benchmark(benchmark.model_copy(update={"source_identity": "c"*64}))
