"""Paper intervals use an actual sealed original result and exact definition."""

from dataclasses import replace
import pytest

from rquant.lab_artifact_preview import ArtifactPreviewReader
from rquant.strategy_template_artifact import StrategyTemplateSealedResultReader
from tests.unit.test_strategy_template_artifact import sealed_template
from tests.unit.test_paper_portfolio_ledger_views import filled


def fixture(tmp_path, *, seal=True):
    from rquant.paper_backtest_source import PaperBacktestSourceReader
    (tmp_path/"template").mkdir(mode=0o700)
    (tmp_path/"paper").mkdir(mode=0o700)
    target, value, jobs, artifact_root, job_id, now = sealed_template(tmp_path/"template", seal=seal)
    _, _, _, runtime = filled(tmp_path/"paper")
    body = runtime.state.configuration.model_dump(mode="python")
    body["binding"].update(strategy_id=value.definition.logical_id, strategy_version=str(value.definition.version),
                           parameter_fingerprint=value.definition.spec.parameter_fingerprint, cost_spec_id=value.request.execution_cost_spec.cost_spec_id)
    body["execution_cost_spec"] = value.request.execution_cost_spec
    configuration = type(runtime.state.configuration).model_validate(body)
    sealed = StrategyTemplateSealedResultReader(reader=jobs, artifact_reader=ArtifactPreviewReader(reader=jobs, artifact_root=artifact_root))
    source = PaperBacktestSourceReader(store=target, expected_identity=target.identity(), sealed_reader=sealed)
    return source, configuration, value, job_id, now


def test_actual_sealed_template_daily_returns_bind_original_owner_version_cost_calendar(tmp_path):
    source, configuration, value, job_id, now = fixture(tmp_path)
    dates = tuple(item.trade_date for item in value.request.days)
    result = source.band_input(configuration=configuration, job_id=job_id, calendar=value.request.calendar, comparison_dates=dates, as_of=now)
    assert result.backtest.complete_result_hash and result.backtest.spec_hash and result.backtest.returns
    assert result.backtest.parameter_fingerprint == configuration.binding.parameter_fingerprint
    from rquant.paper_portfolio_band import execute_paper_backtest_band
    band = execute_paper_backtest_band(result)
    assert band.dates == dates and band.points[0].day_index == 1
    for change in ({"strategy_version": "2"}, {"owner_id": "bob"}, {"parameter_fingerprint": "e"*64}):
        changed = configuration.model_copy(update={"binding": configuration.binding.model_copy(update=change)})
        with pytest.raises((ValueError, PermissionError)):
            source.band_input(configuration=changed, job_id=job_id, calendar=value.request.calendar, comparison_dates=dates, as_of=now)
    with pytest.raises(ValueError):
        source.band_input(configuration=configuration, job_id=job_id, calendar=value.request.calendar.model_copy(update={"source_identity": "0"*64}), comparison_dates=dates, as_of=now)


def test_unsealed_original_job_is_not_a_backtest_return_source(tmp_path):
    source, configuration, value, job_id, now = fixture(tmp_path, seal=False)
    with pytest.raises(ValueError, match="封存|sealed"):
        source.band_input(configuration=configuration, job_id=job_id, calendar=value.request.calendar,
                          comparison_dates=tuple(item.trade_date for item in value.request.days), as_of=now)
