"""The original Lab entry consumes actual sealed C5 returns and the fixed sampler."""

from datetime import timedelta
from fractions import Fraction
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

from rquant.lab_worker_registry import builtin_lab_shard_configuration, execute_builtin_lab_shard
from rquant.paper_portfolio_band import PaperBacktestBandResult
from rquant.paper_portfolio_state import PaperPortfolioStateStore
from rquant.paper_research import FrozenPaperResearchInput, PaperResearchAdapterCatalog, PaperResearchRunParameters
from rquant.paper_research_adapter import paper_research_adapter_registry
from rquant.paper_research_source import paper_research_code_identity, publish_paper_research_input
from rquant.research_catalog import ResearchCatalog
from rquant.research_run_spec import ResearchParameter
from rquant.storage.duckdb import DuckDBStore
from tests.unit.test_paper_backtest_source import fixture
from tests.unit.test_paper_research_admission import spec_for
from tests.unit.test_strategy_template_adapter import claimed


def test_original_band_materializer_and_entry_match_separate_fraction_reference(tmp_path: Path) -> None:
    source, configuration, sealed, job_id, now = fixture(tmp_path)
    dates = tuple(day.trade_date for day in sealed.request.days)
    band_input = source.band_input(configuration=configuration, job_id=job_id, calendar=sealed.request.calendar,
                                   comparison_dates=dates, as_of=now)
    state = PaperPortfolioStateStore(tmp_path / "band-private" / "metadata.sqlite", configuration=configuration)
    catalog = PaperResearchAdapterCatalog(metadata_identity=state.identity(), configuration=configuration,
                                         source_code_identity=paper_research_code_identity())
    value = FrozenPaperResearchInput(task_name="paper_backtest_band", catalog=catalog, code_sha="a" * 40,
                                    available_at=now, band=band_input)
    metadata_path, lake = tmp_path / "band-research.duckdb", tmp_path / "band-lake"
    with DuckDBStore(metadata_path) as metadata:
        published = publish_paper_research_input(value, metadata_store=metadata, source_path=tmp_path / "band-input" / "source.duckdb",
            research_catalog=ResearchCatalog(tmp_path / "band-catalog.duckdb"), lake_root=lake, now=now)
        assert published.gate_decision.allowed and published.gate_decision.research_status == "exploratory"
    parameters = PaperResearchRunParameters.from_input(value, request_id="11111111-1111-4111-8111-111111111111")
    raw = spec_for(catalog, name=value.task_name, work_units=len(dates)).model_dump(mode="python")
    raw["dataset_snapshot"] = published.identity
    raw["deadline"] = now + timedelta(hours=2)
    raw["parameters"].update(start_date=dates[0], end_date=dates[-1], arguments=tuple(
        ResearchParameter(name=name, kind="integer" if type(item) is int else "text", value=item)
        for name, item in parameters.model_dump(mode="python").items()))
    from rquant.research_run_spec import ResearchRunSpec
    spec = ResearchRunSpec.model_validate(raw)
    registry = paper_research_adapter_registry(catalog)
    config = builtin_lab_shard_configuration(catalog_path=metadata_path, forbidden_paths=(), snapshot_root=tmp_path / "band-snapshot",
                                             research_lake_root=lake, paper_catalog=catalog)
    result = execute_builtin_lab_shard(config, claimed(registry, spec), runtime_code_sha=spec.code_sha)
    actual = PaperBacktestBandResult.model_validate_json(result.tables[1].frame.iloc[0]["payload"])
    assert actual.input_hash == band_input.fingerprint and actual.dates == dates
    path = Path(__file__).resolve().parents[2] / "data/verification/paper-portfolio-completion-20261005/bootstrap-independent-reference.py"
    reference_spec = spec_from_file_location("independent_paper_fraction", path)
    assert reference_spec is not None and reference_spec.loader is not None
    reference = module_from_spec(reference_spec)
    reference_spec.loader.exec_module(reference)
    expected = reference.reference(tuple(str(day.daily_return) for day in band_input.backtest.returns), len(dates))
    for point, exact in zip(actual.points, expected, strict=True):
        for observed, key in ((point.lower, "lower_fraction"), (point.upper, "upper_fraction")):
            fraction = Fraction(exact[key])
            assert abs(Fraction(observed) - fraction) <= max(Fraction(1), abs(fraction)) / 10**28
    print("ACTUAL_SEALED_C5_RETURN_INPUT=True; ORIGINAL_IMMUTABLE_MATERIALIZER_AND_INLINE_CHILD_ENTRY=True; INDEPENDENT_FRACTION_QUANTILES=True; ISOLATED_CHILD_STARTED=False")
