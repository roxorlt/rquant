"""The role publishes complete original history even when marks are unavailable."""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from rquant.paper_portfolio_source import PaperPortfolioMarketSnapshot, PaperPortfolioRawFact
from rquant.paper_signal_worker import PaperSignalQueueStore
from tests.unit.test_paper_portfolio_ledger_views import close_material, filled
from tests.unit.test_paper_signal_worker import EXECUTION_TIME, _policy

if TYPE_CHECKING:
    from uuid import UUID

    from rquant.paper_portfolio_projection import PaperPortfolioPublishedAccount
    from rquant.paper_portfolio_view_source import PaperPortfolioViewSource
    from rquant.paper_research_artifact import PaperResearchSummary


def market(runtime, *, at=EXECUTION_TIME, price="2", status="normal"):
    return runtime.materials.publish(PaperPortfolioMarketSnapshot(
        binding=runtime.state.configuration.binding, configuration_fingerprint=runtime.state.configuration.fingerprint,
        dataset_snapshot_id="c"*64, feature_snapshot_id="d"*64, observed_at=at, available_at=at,
        facts=(PaperPortfolioRawFact(ts_code="600000.SH", candidate=True, rank_score="1", industry_l1="银行",
                                    valuation_price=price, trading_status=status, observed_at=at,
                                    available_at=at, source_snapshot_id="e"*64),)))


def test_role_source_uses_full_original_frame_and_same_applied_control(tmp_path: Path) -> None:
    from rquant.paper_portfolio_view_source import PaperPortfolioViewSource

    broker, _, operator, runtime = filled(tmp_path)
    at = EXECUTION_TIME+timedelta(seconds=1)
    market(runtime, at=at)
    queue = PaperSignalQueueStore(tmp_path / "queue.sqlite", policy=_policy())
    value = PaperPortfolioViewSource(runtime, broker=broker, queue=queue).read(as_of=at)
    assert value.status == "complete" and value.frame.account.cash == 195 and value.frame.account.nav == 1795
    assert len(value.frame.history) == 1 and value.frame.history[0].order.filled_quantity == 800
    assert value.operator == operator.current() and value.operator.status == "applied"
    assert value.frame.as_of == at and value.market_material.facts[0].valuation_price == 2
    assert value.nav == ()


def test_missing_or_stale_mark_preserves_history_without_substitute_nav(tmp_path: Path) -> None:
    from rquant.paper_portfolio_view_source import PaperPortfolioViewSource
    from rquant.paper_portfolio_history import paper_history_page

    broker, _, _, runtime = filled(tmp_path)
    at = EXECUTION_TIME+timedelta(seconds=1)
    market(runtime, at=at, price=None, status="missing")
    queue = PaperSignalQueueStore(tmp_path / "queue.sqlite", policy=_policy())
    source = PaperPortfolioViewSource(runtime, broker=broker, queue=queue)
    value = source.read(as_of=at)
    assert value.status == "unavailable" and value.frame.account is None
    assert value.frame.missing_valuation_codes == ("600000.SH",) and value.nav == ()
    page = paper_history_page(value.frame, configuration=runtime.state.configuration, generation_id="a"*64,
                              authenticated_actor_id="alice")
    assert page.total_orders == 1 and page.records[0].order.filled_quantity == 800
    market(runtime, at=at+timedelta(seconds=1))
    stale = source.read(as_of=at+timedelta(seconds=100))
    assert stale.status == "unavailable" and stale.frame.account is None
    assert "估值" in stale.reason and stale.frame.history == value.frame.history


def test_real_close_is_recorded_once_and_intraday_material_is_not_a_close(tmp_path: Path) -> None:
    from rquant.paper_portfolio_view_source import PaperPortfolioViewSource

    broker, basis, _, runtime = filled(tmp_path)
    material = close_material(basis.configuration)
    runtime.calendar = material.calendar
    market(runtime, at=material.close_at-timedelta(minutes=1))
    source = PaperPortfolioViewSource(runtime, broker=broker, queue=PaperSignalQueueStore(tmp_path / "queue.sqlite", policy=_policy()))
    assert source.read(as_of=material.close_at-timedelta(minutes=1)).nav == ()
    market(runtime, at=material.close_at)
    first = source.read(as_of=material.close_at)
    assert len(first.nav) == 1 and first.nav[0].normalized_nav == Decimal("1.795")
    assert first.nav[0].daily_return == Decimal(".795") and first.nav[0].nav == 1795
    assert source.read(as_of=material.close_at+timedelta(seconds=1)).nav == first.nav
    with pytest.raises(ValueError):
        runtime.calendar = material.calendar.model_copy(update={"source_identity": "9"*64})


def test_missing_close_is_published_as_gap_with_actual_order_history(tmp_path: Path) -> None:
    from rquant.paper_portfolio_view_source import PaperPortfolioViewSource

    broker, basis, _, runtime = filled(tmp_path)
    close = close_material(basis.configuration)
    runtime.calendar = close.calendar
    market(runtime, at=close.close_at, price=None, status="error")
    value = PaperPortfolioViewSource(runtime, broker=broker, queue=PaperSignalQueueStore(tmp_path / "queue.sqlite", policy=_policy())).read(as_of=close.close_at)
    assert value.frame.account is None and len(value.frame.history) == 1
    assert len(value.nav) == 1 and value.nav[0].status == "unavailable"
    assert value.nav[0].nav is None and value.nav[0].daily_return is None
    assert value.calendar == close.calendar


def comparison_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, lower: str, upper: str
) -> tuple[PaperPortfolioViewSource, PaperPortfolioPublishedAccount, datetime]:
    """Original closed ledger with an explicit typed sealed-summary transport fixture."""
    from rquant.lab_artifact_preview import ArtifactPreviewReader
    from rquant.lab_job_center import LabCommandSubmissionFacade
    from rquant.lab_job_protocol import LabCommandSpool
    from rquant.lab_jobs import LabJobReader, LabJobStore
    from rquant.paper_research_artifact import PaperResearchResultReader
    from rquant.paper_research_submission import PaperResearchRunBackend, PaperResearchRunPreparer
    from rquant.research_catalog import ResearchCatalog
    from rquant.storage.duckdb import DuckDBStore
    from tests.unit.test_runtime_health_owner_metrics import _closed_comparison

    source, account, at = _closed_comparison(tmp_path, lower=lower, upper=upper)
    jobs = LabJobStore(tmp_path / "comparison-jobs.sqlite")
    jobs.initialize()
    reader = LabJobReader(jobs.path)
    facade = LabCommandSubmissionFacade(
        reader=reader, spool=LabCommandSpool(tmp_path / "comparison-spool"), clock=lambda: at
    )
    inputs = tmp_path / "comparison-inputs"
    inputs.mkdir(mode=0o700)
    preparer = PaperResearchRunPreparer(
        sources=(source,),
        metadata_store_factory=lambda: DuckDBStore(tmp_path / "comparison-metadata.duckdb"),
        research_catalog=ResearchCatalog(tmp_path / "comparison-catalog.duckdb"),
        input_root=inputs,
        lake_root=tmp_path / "comparison-lake",
        code_sha="a" * 40,
        clock=lambda: at,
    )
    backend = PaperResearchRunBackend(preparer=preparer, facade=facade)
    summary = account.recent_research[0]
    backend._table(source.runtime.state)
    with source.runtime.state._connection(write=True) as connection:
        connection.execute(
            "INSERT INTO paper_research_admissions VALUES(?,?,?,?,NULL)",
            (str(summary.job_id), "alice", "{}", '{"accepted_at":"' + at.isoformat() + '"}'),
        )
    result_reader = PaperResearchResultReader(
        backend=backend,
        reader=reader,
        artifact_reader=ArtifactPreviewReader(reader=reader, artifact_root=tmp_path / "artifacts"),
    )

    def original_summary(
        *, account_id: str, job_id: UUID, owner_id: str, as_of: datetime
    ) -> PaperResearchSummary:
        assert (account_id, job_id, owner_id, as_of) == (
            summary.account_id, summary.job_id, account.configuration.binding.owner_id, at
        )
        return summary

    monkeypatch.setattr(result_reader, "summary", original_summary)
    source.research_results = result_reader
    return source, account, at


@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.parametrize(
    "lower,upper,expected",
    [("1", "2", "inside"), ("2", "3", "outside"), ("1.795", "1.795", "inside")],
)
def test_original_band_position_is_published_with_health_on_or_off(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, enabled: bool,
    lower: str, upper: str, expected: str
) -> None:
    import sqlite3
    from contextlib import closing

    source, original, at = comparison_source(tmp_path, monkeypatch, lower=lower, upper=upper)
    source.health_metrics_enabled = enabled
    with closing(sqlite3.connect(source.broker.path)) as pinned:
        pinned.execute("SELECT count(*) FROM paper_order").fetchone()
        value = source.read(as_of=at)
    assert value.band == original.band and value.nav == original.nav
    assert value.complete_comparison_dates() == value.band.dates
    assert value.band_position == expected
