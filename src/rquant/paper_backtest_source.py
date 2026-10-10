"""Only an exact original sealed template result supplies bootstrap returns."""

from __future__ import annotations

from datetime import date, datetime
from uuid import UUID

from rquant.backtest.contracts import SSECalendar
from rquant.lab_artifact_preview import ArtifactPreviewReader
from rquant.paper_portfolio_band import PaperBacktestBandInput, SealedPaperBacktestReturns, SealedPaperDailyReturn
from rquant.paper_portfolio_models import PaperPortfolioConfiguration
from rquant.strategy_authoring import StrategyAuthoringStore
from rquant.strategy_authoring_commands import StrategyAuthoringIdentity
from rquant.strategy_template_artifact import StrategyTemplateSealedResultReader
from rquant.strategy_template_run import StrategyTemplateResult


class PaperBacktestSourceReader:
    def __init__(self, *, store: StrategyAuthoringStore, expected_identity: StrategyAuthoringIdentity,
                 sealed_reader: StrategyTemplateSealedResultReader) -> None:
        if type(store) is not StrategyAuthoringStore or type(sealed_reader) is not StrategyTemplateSealedResultReader or store.identity() != expected_identity:
            raise TypeError("paper band requires its concrete original committed metadata and sealed reader")
        self.store, self.expected_identity, self.sealed_reader = store, expected_identity, sealed_reader
        self._content_reader = ArtifactPreviewReader(reader=sealed_reader.reader, artifact_root=sealed_reader.artifact_reader.artifact_root,
                                                     max_preview_rows=1, max_preview_columns=2, max_preview_cell_bytes=16*1024*1024,
                                                     max_preview_serialized_bytes=17*1024*1024, max_preview_arrow_bytes=20*1024*1024)

    def band_input(self, *, configuration: PaperPortfolioConfiguration, job_id: UUID, calendar: SSECalendar,
                   comparison_dates: tuple[date, ...], as_of: datetime) -> PaperBacktestBandInput:
        runs = self.sealed_reader.recent_runs(self.store, expected_identity=self.expected_identity, as_of=as_of)
        selected = next((item for item in runs if item.job_id == job_id), None)
        if selected is None:
            raise ValueError("缺少同版本封存回测")
        binding = configuration.binding
        if (selected.owner_id, selected.strategy_id, str(selected.head.version)) != (binding.owner_id, binding.strategy_id, binding.strategy_version):
            raise ValueError("paper backtest differs from exact account strategy version or owner")
        version = self.store.get_version(selected.strategy_id, selected.head.version, owner_id=binding.owner_id)
        definition = self.store.definition_registry(selected.strategy_id).read_strategy_spec(version.head.registration_fingerprint)
        authority = self.sealed_reader.reader.get_artifact_preview_authority(job_id)
        if definition is None or authority is None or (definition.spec.parameter_fingerprint, authority.job.spec.execution_costs.cost_spec_id) != (
                binding.parameter_fingerprint, binding.cost_spec_id):
            raise ValueError("paper backtest original parameters or execution cost differ")
        preview = self._content_reader.preview(job_id, table_name="template_result", row_limit=1, column_limit=2)
        table = preview.table
        if (table is None or table.columns != ("result_hash", "payload") or table.total_rows != 1 or table.total_columns != 2
                or table.rows_truncated or table.columns_truncated or len(table.rows) != 1
                or (preview.spec_hash, preview.manifest_hash, preview.complete_result_hash) != (
                selected.spec_hash, selected.manifest_hash, selected.complete_result_hash)):
            raise ValueError("paper backtest complete sealed graph differs")
        result = StrategyTemplateResult.model_validate_json(table.rows[0][1])
        if (table.rows[0][0], result.content_hash, result.owner_id, result.strategy_id, result.version,
                result.definition_fingerprint, result.definition_record_hash, result.input_hash, result.cost_spec_id,
                result.calendar_source_identity, result.status) != (
                selected.result_hash, selected.result_hash, binding.owner_id, binding.strategy_id, selected.head.version,
                selected.head.registration_fingerprint, selected.head.record_hash, selected.input_hash, binding.cost_spec_id,
                calendar.source_identity, "complete"):
            raise ValueError("paper sealed backtest body differs from its original exact reference")
        if any(item.daily_return is None or item.account is None or item.trade_date > as_of.date() for item in result.days):
            raise ValueError("paper sealed backtest daily return coverage is incomplete")
        backtest = SealedPaperBacktestReturns(job_id=job_id, owner_id=binding.owner_id, strategy_id=binding.strategy_id,
                                             strategy_version=binding.strategy_version, parameter_fingerprint=binding.parameter_fingerprint,
                                             cost_spec_id=binding.cost_spec_id, calendar_source_identity=calendar.source_identity,
                                             definition_fingerprint=selected.head.registration_fingerprint, definition_record_hash=selected.head.record_hash,
                                             spec_hash=selected.spec_hash, manifest_hash=selected.manifest_hash,
                                             complete_result_hash=selected.complete_result_hash, backtest_content_hash=result.content_hash,
                                             returns=tuple(SealedPaperDailyReturn(trade_date=item.trade_date, daily_return=item.daily_return) for item in result.days))
        return PaperBacktestBandInput(configuration=configuration, backtest=backtest, calendar=calendar, comparison_dates=comparison_dates)
