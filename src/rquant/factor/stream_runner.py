"""One offline, exploratory execution over an admitted immutable v2 generation."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from contextlib import ExitStack
from pathlib import Path

from pydantic import BaseModel, ConfigDict, model_validator

from rquant.factor.daily_stream import (
    FactorDailyStreamBatch,
    FactorDailyStreamResult,
    evaluate_factor_daily_stream,
    factor_daily_stream_request_sha256,
)
from rquant.factor.decay_stream import (
    FactorICDecayStream,
    FactorICDecayStreamRequest,
    FactorICDecayStreamResult,
)
from rquant.factor.formula_stream import (
    FactorFormulaStreamCompletion,
    evaluate_factor_formula_stream,
    factor_formula_stream_request_sha256,
)
from rquant.factor.neutralization_context import open_factor_neutralization_context
from rquant.factor.stream_adapter import (
    FactorStreamAdapter,
    FactorStreamAdapterCompletion,
    FactorStreamAdapterRequest,
    factor_stream_adapter_request_sha256,
    factor_stream_statistics_request,
)
from rquant.factor.stream_snapshot import open_factor_stream_snapshot_admission
from rquant.factor.universe import FactorUniverseRequest, Sha256
from rquant.research_snapshot import SnapshotMetadataStore
from rquant.runtime_contracts import canonical_sha256


class FactorStreamResearchResult(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    request: FactorStreamAdapterRequest
    request_sha256: Sha256
    adapter_completion: FactorStreamAdapterCompletion
    formula_completion: FactorFormulaStreamCompletion
    statistics: FactorDailyStreamResult
    sha256: Sha256

    @model_validator(mode="after")
    def _completed_bindings(self) -> FactorStreamResearchResult:
        request_sha = factor_stream_adapter_request_sha256(self.request)
        formula_sha = factor_formula_stream_request_sha256(self.request.formula)
        raw = self.adapter_completion
        formula = self.formula_completion
        statistics = self.statistics
        if (
            self.request_sha256 != request_sha
            or raw.request_sha256 != request_sha
            or raw.admission.snapshot_id != self.request.source.snapshot_id
            or raw.admission.binding_hash != self.request.source.binding_hash
            or raw.admission.scope != self.request.source.scope
            or raw.admission.scope_content_hash != self.request.scope_content_hash
            or raw.formula_request_sha256 != formula_sha
            or raw.context != self.request.formula.sources.context
            or formula.request_sha256 != formula_sha
            or formula.sources != self.request.formula.sources
            or formula.definition_sha256 != canonical_sha256(self.request.formula.definition)
            or formula.processed_days != len(self.request.formula.trading_days)
            or raw.processed_days != formula.processed_days
            or tuple(day.trade_date for day in raw.feature_days)
            != self.request.formula.trading_days
            or tuple(day.window.decision_date for day in raw.return_days)
            != self.request.evaluation_days
            or statistics.request != factor_stream_statistics_request(self.request)
            or statistics.request_sha256 != factor_daily_stream_request_sha256(statistics.request)
            or raw.statistics_request_sha256 != statistics.request_sha256
            or tuple(day.trade_date for day in statistics.days) != self.request.evaluation_days
            or len(statistics.days) != len(raw.return_days)
            or statistics.batch_sha256s
            != tuple(day.statistics_batch_sha256 for day in raw.return_days)
        ):
            raise ValueError("stream research completion bindings differ")
        if any(
            day.decision_at
            != self.request.formula.decision_times[
                self.request.formula.trading_days.index(day.trade_date)
            ].decision_at
            or day.return_end_at != receipt.window.return_end_at
            or day.coverage.expected_count != receipt.selected_count
            for day, receipt in zip(statistics.days, raw.return_days, strict=True)
        ):
            raise ValueError("statistics windows differ from completed source")
        history_sha = formula_sha
        for day in raw.feature_days:
            history_sha = canonical_sha256((history_sha, day.input_sha256))
        if formula.history_sha256 != history_sha:
            raise ValueError("formula history differs from completed adapter inputs")
        if formula.sha256 != canonical_sha256(formula.model_dump(exclude={"sha256"})):
            raise ValueError("formula completion digest differs")
        if statistics.sha256 != canonical_sha256(statistics.model_dump(exclude={"sha256"})):
            raise ValueError("statistics digest differs")
        if statistics.input_sha256 != canonical_sha256(
            (statistics.request_sha256, statistics.batch_sha256s)
        ):
            raise ValueError("statistics input digest differs")
        if self.sha256 != canonical_sha256(self.model_dump(exclude={"sha256"})):
            raise ValueError("stream research result digest differs")
        return self


def run_factor_stream_research(
    request: FactorStreamAdapterRequest,
    *,
    metadata_store: SnapshotMetadataStore,
    lake_root: Path,
    universe_requests: Iterable[FactorUniverseRequest],
) -> FactorStreamResearchResult:
    """Return success only after all calculation dates, raw tail and statistics complete."""
    return _run_factor_stream_research(
        request,
        metadata_store=metadata_store,
        lake_root=lake_root,
        universe_requests=universe_requests,
        decay=None,
    )


class FactorStreamResearchWithDecayResult(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    research: FactorStreamResearchResult
    decay: FactorICDecayStreamResult
    sha256: Sha256

    @model_validator(mode="after")
    def _completed_decay_binding(self) -> FactorStreamResearchWithDecayResult:
        expected = FactorICDecayStreamRequest(
            statistics_request=self.research.statistics.request,
            computation_stock_codes=self.research.request.source.scope.stock_codes,
        )
        if (
            self.decay.request != expected
            or self.decay.statistics_input_sha256 != self.research.statistics.input_sha256
            or self.decay.batch_sha256s != self.research.statistics.batch_sha256s
        ):
            raise ValueError("research and decay completed inputs differ")
        if self.sha256 != canonical_sha256(self.model_dump(exclude={"sha256"})):
            raise ValueError("research with decay result digest differs")
        return self


def run_factor_stream_research_with_decay(
    request: FactorStreamAdapterRequest,
    *,
    metadata_store: SnapshotMetadataStore,
    lake_root: Path,
    universe_requests: Iterable[FactorUniverseRequest],
    batch_observer: Callable[[FactorDailyStreamBatch], None] | None = None,
) -> FactorStreamResearchWithDecayResult:
    try:
        request = FactorStreamAdapterRequest.model_validate(request)
        decay = FactorICDecayStream(
            FactorICDecayStreamRequest(
                statistics_request=factor_stream_statistics_request(request),
                computation_stock_codes=request.source.scope.stock_codes,
            )
        )
    except BaseException:
        close = getattr(universe_requests, "close", None)
        if close is not None:
            close()
        raise
    try:
        research = _run_factor_stream_research(
            request,
            metadata_store=metadata_store,
            lake_root=lake_root,
            universe_requests=universe_requests,
            decay=decay,
            batch_observer=batch_observer,
        )
        fields = {"research": research, "decay": decay.finish(research.statistics)}
        return FactorStreamResearchWithDecayResult(**fields, sha256=canonical_sha256(fields))
    finally:
        decay.close()


def _run_factor_stream_research(
    request: FactorStreamAdapterRequest,
    *,
    metadata_store: SnapshotMetadataStore,
    lake_root: Path,
    universe_requests: Iterable[FactorUniverseRequest],
    decay: FactorICDecayStream | None,
    batch_observer: Callable[[FactorDailyStreamBatch], None] | None = None,
) -> FactorStreamResearchResult:
    adapter = None
    formula = None
    statistics_batches = None
    try:
        request = FactorStreamAdapterRequest.model_validate(request)
        with ExitStack() as stack:
            lease, decision = stack.enter_context(
                open_factor_stream_snapshot_admission(
                    request.source, metadata_store=metadata_store, lake_root=lake_root
                )
            )
            context_lease = (
                stack.enter_context(
                    open_factor_neutralization_context(request.context, lake_root=lake_root)
                )
                if request.context is not None
                else None
            )
            adapter = FactorStreamAdapter(
                request,
                lease=lease,
                decision=decision,
                universe_requests=universe_requests,
                context_lease=context_lease,
            )
            formula = evaluate_factor_formula_stream(request.formula, adapter)
            evaluated = frozenset(request.evaluation_days)

            def batches() -> Iterator[FactorDailyStreamBatch]:
                assert formula is not None
                for day in formula:
                    if day.universe.trade_date in evaluated:
                        batch = adapter.statistics_batch(day)
                        yield batch
                        if decay is not None:
                            decay.consume(batch)
                        if batch_observer is not None:
                            batch_observer(batch)
                        del batch

            statistics_batches = batches()
            statistics = evaluate_factor_daily_stream(
                adapter.statistics_request, statistics_batches
            )
            if adapter.completion is None or formula.completion is None:
                raise ValueError("source or formula stream did not naturally complete")
            fields = {
                "request": request,
                "request_sha256": factor_stream_adapter_request_sha256(request),
                "adapter_completion": adapter.completion,
                "formula_completion": formula.completion,
                "statistics": statistics,
            }
            return FactorStreamResearchResult(**fields, sha256=canonical_sha256(fields))
    finally:
        try:
            if statistics_batches is not None:
                statistics_batches.close()
        finally:
            try:
                if formula is not None:
                    formula.close()
            finally:
                if adapter is not None:
                    adapter.close()
                else:
                    close = getattr(universe_requests, "close", None)
                    if close is not None:
                        close()
