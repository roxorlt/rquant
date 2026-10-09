from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import closing, contextmanager
from datetime import timedelta
from pathlib import Path
from uuid import UUID

import pytest

from rquant.experiment_platform_evidence import (
    ExperimentEvidencePublisher,
    build_overfit_evidence,
    compare_experiment_results,
    experiment_heatmap,
    read_experiment_result,
)
from rquant.experiment_platform_projection import (
    ExperimentAttemptFact,
    ExperimentPrivateProjectionReader,
)
from rquant.experiment_registry import ExperimentRegistryReadonlyReader
from rquant.lab_artifacts import LabArtifactIndexEvidence, LabJobArtifactStore
from rquant.lab_jobs import (
    ControlIntent,
    JobStatus,
    LabArtifactPreviewAuthority,
    LabJobReader,
    LabJobRecord,
    LabResultState,
)
from rquant.portfolio_backtest_artifact import PortfolioResultReader
from rquant.portfolio_backtest_product import bundle_tables, execute_portfolio_input
from rquant.web.experiment_platform_models import ExperimentResultData
from tests.unit.test_experiment_platform import NOW, prepared_family


@pytest.fixture
def complete_family(tmp_path: Path):
    store, record, children, _ = prepared_family(tmp_path)
    store.register_family_submission(
        owner=record.owner, request_id=record.request_id, children=children
    )
    artifacts = LabJobArtifactStore(tmp_path / "artifacts")
    authorities: dict[UUID, LabArtifactPreviewAuthority] = {}
    for index, child in enumerate(children):
        prepared = store.preparation("alice", record.family_id, index).prepared
        store.admit_publication(child.intent, now=NOW)
        bundle = execute_portfolio_input(prepared.frozen, research_root=tmp_path)
        assert bundle.result.status == "complete"
        spec = prepared.submission(job_id=child.intent.job_id).command.spec
        candidate = artifacts.prepare_candidate(
            job_id=child.intent.job_id,
            spec=spec,
            plan_hash="1" * 64,
            adapter_id="portfolio-backtest",
            adapter_version="1",
            result_contract_version="p14b1-v1",
            metrics={"status": "complete"},
            report_markdown="# 组合\n",
            tables=bundle_tables(bundle),
        )
        sealed = artifacts.seal_candidate(candidate)
        evidence = LabArtifactIndexEvidence(
            job_id=child.intent.job_id,
            sealed_path=sealed.path,
            manifest_hash=sealed.manifest_hash,
            complete_result_hash=sealed.manifest.complete_result_hash,
            bundle_device=sealed.device,
            bundle_inode=sealed.inode,
            file_identities=sealed.file_identities,
            indexed_at=NOW + timedelta(seconds=2),
        )
        job = LabJobRecord(
            job_id=child.intent.job_id,
            spec=spec,
            spec_hash=spec.spec_hash,
            job_type=spec.job_type,
            resource_class=spec.resource_class,
            deadline=spec.deadline,
            status=JobStatus.SUCCEEDED,
            control_intent=ControlIntent.NONE,
            version=3,
            attempt_count=1,
            max_attempts=2,
            recoverable=True,
            requires_complete_result=True,
            result_state=LabResultState.SEALED,
            created_at=NOW,
            updated_at=NOW + timedelta(seconds=2),
        )
        authorities[child.intent.job_id] = LabArtifactPreviewAuthority(job=job, evidence=evidence)
        store.registry.start_attempt(
            child.plan.spec.experiment_id, started_at=NOW + timedelta(seconds=1)
        )
        store.registry.record_execution_completed(
            child.plan.spec.experiment_id, completed_at=NOW + timedelta(seconds=2)
        )

    ledger_path = tmp_path / "sealed-ledger-fixture.sqlite3"
    with closing(sqlite3.connect(ledger_path)) as connection:
        connection.execute(
            "CREATE TABLE lab_job (job_id TEXT PRIMARY KEY, authority_json TEXT NOT NULL)"
        )
        connection.executemany(
            "INSERT INTO lab_job (job_id, authority_json) VALUES (?, ?)",
            ((str(job_id), authority.model_dump_json()) for job_id, authority in authorities.items()),
        )
        connection.commit()

    class SealedLedgerFixture:
        """Original synthetic authorities in SQLite, without a physical Lab graph."""

        path = ledger_path
        _storage_revision = LabJobReader._storage_revision

        @contextmanager
        def _read_snapshot(self, *, label: str) -> Iterator[sqlite3.Connection]:
            with closing(
                sqlite3.connect(f"{self.path.as_uri()}?mode=ro", uri=True, isolation_level=None)
            ) as connection:
                connection.row_factory = sqlite3.Row
                connection.execute("PRAGMA query_only = ON")
                connection.execute("BEGIN")
                try:
                    yield connection
                finally:
                    connection.rollback()

        @staticmethod
        def _job_from_row(row: sqlite3.Row) -> LabJobRecord:
            return LabArtifactPreviewAuthority.model_validate_json(row["authority_json"]).job

        def _validate_complete_result_graph(
            self, connection: sqlite3.Connection, job: LabJobRecord
        ) -> LabArtifactIndexEvidence:
            row = connection.execute(
                "SELECT authority_json FROM lab_job WHERE job_id = ?", (str(job.job_id),)
            ).fetchone()
            assert row is not None
            authority = LabArtifactPreviewAuthority.model_validate_json(row["authority_json"])
            if authority.job != job:
                raise ValueError("synthetic sealed ledger job binding differs")
            return authority.evidence

        def get_job(self, job_id: UUID) -> LabJobRecord | None:
            return authorities.get(job_id).job if job_id in authorities else None

        def get_artifact_preview_authority(self, job_id: UUID) -> LabArtifactPreviewAuthority | None:
            return authorities.get(job_id)

    ledger = SealedLedgerFixture()
    projection = ExperimentPrivateProjectionReader(
        registry=ExperimentRegistryReadonlyReader(
            store.registry.path, managed_trust_root=store.registry.path.parent
        ),
        jobs=ledger,
    )
    results = PortfolioResultReader(reader=ledger, artifact_root=artifacts.root)
    try:
        yield store, projection, results, authorities
    finally:
        artifacts.close()


def test_exp14_actual_original_bundle_private_owner_and_exact_hash(complete_family) -> None:
    _, projection, results, _ = complete_family
    snapshot = projection.snapshot(NOW + timedelta(seconds=3))
    family = snapshot.families[0]
    fact = next(f for f in snapshot.attempts if f.index == 0)
    with pytest.raises(PermissionError):
        results.read(fact.child.job_id)
    with pytest.raises(PermissionError):
        results.read(fact.child.job_id, private_owner="bob", private_authority=projection.authority)
    result = read_experiment_result(fact, family, results=results, authority=projection.authority)
    assert len(result.curves) == 4 and tuple(p.trade_date for p in result.curves) == tuple(
        p.trade_date for stage in result.phases for p in stage.curves
    )
    assert len(result.metrics) >= 18
    with pytest.raises(ValueError):
        read_experiment_result(
            fact.model_copy(update={"input_hash": "0" * 64}),
            family,
            results=results,
            authority=projection.authority,
        )


def test_exp15_actual_evidence_seal_has_no_invented_pvalue_or_approval(complete_family) -> None:
    store, projection, results, _ = complete_family
    snapshot = projection.snapshot(NOW + timedelta(seconds=3))
    family = snapshot.families[0]
    facts = tuple(sorted(snapshot.attempts, key=lambda f: f.index))

    def read(fact: ExperimentAttemptFact) -> ExperimentResultData:
        return read_experiment_result(fact, family, results=results, authority=projection.authority)

    evidence = build_overfit_evidence(family, facts, read=read)
    assert evidence.search_count == 4
    assert all(
        s.psr is None
        and s.dsr is None
        and s.mintrl is None
        and s.bh_adjusted_p is None
        and s.reasons
        for s in evidence.statistics
    )
    assert all(f.attempt.outcome is None and f.attempt.status.value == "executed" for f in facts)
    publisher = ExperimentEvidencePublisher(store=store, projection=projection, results=results)
    first = publisher(NOW + timedelta(seconds=3))
    second = publisher(NOW + timedelta(seconds=3))
    assert first == second == (evidence.evidence_id,)
    assert (
        projection.authority.evidence("alice", family.family_id, evidence.evidence_id) == evidence
    )
    assert (
        projection.snapshot(NOW + timedelta(seconds=3)).families[0].evidence_id
        == evidence.evidence_id
    )


def test_exp20_and21_complete_heatmap_and_two_full_curves(complete_family) -> None:
    _, projection, results, _ = complete_family
    snapshot = projection.snapshot(NOW + timedelta(seconds=3))
    family = snapshot.families[0]
    facts = tuple(sorted(snapshot.attempts, key=lambda f: f.index))

    def read(fact: ExperimentAttemptFact) -> ExperimentResultData:
        return read_experiment_result(fact, family, results=results, authority=projection.authority)

    a, b = read(facts[0]), read(facts[1])
    comparison = compare_experiment_results(a, b)
    assert comparison.comparable and comparison.differences
    assert comparison.a.curves == a.curves and comparison.b.curves == b.curves
    for metric in comparison.metric_differences:
        x = next(m.value for m in a.metrics if m.key == metric.key)
        y = next(m.value for m in b.metrics if m.key == metric.key)
        assert metric.value == (None if x is None or y is None else y - x)
    with pytest.raises(ValueError):
        compare_experiment_results(a, a)
    other = b.model_copy(update={"basis_hash": "0" * 64})
    assert compare_experiment_results(a, other).metric_differences == ()
    heat = experiment_heatmap(
        family,
        facts,
        selected=facts[0].attempt.spec.experiment_id,
        x="weight_rule.max_positions",
        y="weight_rule.cash_reserve",
        phase="validation",
        metric="total_return",
        read=read,
    )
    assert len(heat.cells) == 4 and heat.neighbor_count == 3 and heat.available_neighbors == 3
    assert heat.complete_neighborhood
    with pytest.raises(ValueError):
        experiment_heatmap(
            family,
            facts,
            selected=facts[0].attempt.spec.experiment_id,
            x="weight_rule.max_positions",
            y="weight_rule.max_positions",
            phase="validation",
            metric="total_return",
            read=read,
        )


def test_c5t08_comparison_includes_full_rules_and_excludes_internal_template_identity(
    complete_family, tmp_path: Path
) -> None:
    from rquant.strategy_template import StrategyTemplate
    from rquant.web.experiment_platform_models import ExperimentTemplateResultIdentity
    from tests.unit.test_experiment_platform_templates import binding_for

    _, projection, results, _ = complete_family
    snapshot = projection.snapshot(NOW + timedelta(seconds=3))
    facts = tuple(sorted(snapshot.attempts, key=lambda f: f.index))
    a, b = tuple(
        read_experiment_result(
            fact, snapshot.families[0], results=results, authority=projection.authority
        )
        for fact in facts[:2]
    )
    ordinary = compare_experiment_results(a, b)
    binding, _, _, saved, _ = binding_for(tmp_path)
    original = binding.original.get_version(saved.strategy_id, 1, owner_id="alice")
    # Comparison DTO inputs are synthetic; this is not a template execution proof.
    left = ExperimentTemplateResultIdentity(
        strategy_id=saved.strategy_id, head=saved.head, rules=original.rules, content_hash="a" * 64
    )
    right = left.model_copy(
        update={
            "strategy_id": "template_" + "f" * 32,
            "content_hash": "b" * 64,
            "rules": StrategyTemplate.model_validate(
                original.rules.model_dump(mode="python")
                | {
                    "entry": {
                        "kind": "pool",
                        "pool_key": "actual-pool",
                        "version": 1,
                        "body_hash": "e" * 64,
                    },
                    "exit": {"stop_loss": "0.05", "max_holding_days": 5},
                    "index_filter": {
                        "benchmark_code": "000300.SH",
                        "ma_days": 20,
                        "direction": "above",
                    },
                }
            ),
        }
    )
    comparison = compare_experiment_results(
        a.model_copy(update={"template": left}), b.model_copy(update={"template": right})
    )
    changes = {row.path: (row.a, row.b) for row in comparison.differences}
    assert changes["template.exit.stop_loss"] == (None, "0.05")
    assert changes["template.exit.max_holding_days"] == (None, "5")
    assert changes["template.index_filter.ma_days"] == (None, "20")
    assert changes["template.entry.pool_key"] == (None, "actual-pool")
    assert not any("head" in p or "content_hash" in p or "strategy_id" in p for p in changes)
    assert (
        tuple(d for d in comparison.differences if not d.path.startswith("template."))
        == ordinary.differences
    )
    assert comparison.metric_differences == ordinary.metric_differences
    same_rules = compare_experiment_results(
        a.model_copy(update={"template": left}),
        b.model_copy(update={"template": right.model_copy(update={"rules": left.rules})}),
    )
    assert same_rules.differences == ordinary.differences


def test_exp12_and13_outer_commits_once_before_failed_read_and_exact_replay(
    complete_family,
) -> None:
    store, projection, _, _ = complete_family
    snapshot = projection.snapshot(NOW + timedelta(seconds=3))
    family = snapshot.families[0]
    fact = snapshot.attempts[0]
    kwargs = dict(
        owner="alice",
        family_id=family.family_id,
        experiment_id=fact.attempt.spec.experiment_id,
        now=NOW + timedelta(seconds=4),
        body_hash="a" * 64,
        result_hash=fact.result_hash,
        source_identity=fact.source_identity,
        expected_policy_version=store.policy().version,
        cutoff=family.request.protocol.frozen_outer_test_range.end_date,
    )
    grant = store.admit_outer(request_id=UUID(int=701), **kwargs)
    assert store.admit_outer(request_id=UUID(int=701), **kwargs) == grant
    record = store.begin_outer_request(grant)
    assert (
        record.phase == "outer"
        and len(record.actual_configurations) == 1
        and record.parent_family_id == family.family_id
    )
    assert record.actual_configurations[0].start_date == grant.outer_range.start_date
    with pytest.raises(ValueError, match="overlap"):
        store.admit_outer(request_id=UUID(int=702), **kwargs)
    assert store.list_outer_grants("alice") == (grant,)


def test_exp15_to18_explicit_synthetic_independence_original_statistics_and_full_n(
    complete_family,
) -> None:
    import math
    from datetime import date
    from decimal import Decimal
    from statistics import mean, stdev

    from rquant.experiment_platform_evidence import ExperimentIndependenceEvidence
    from rquant.experiment_registry import ExperimentAttempt, ExperimentRegistry
    from rquant.overfit import (
        DeflatedSharpeInput,
        SinglePeriodSharpeInput,
        deflated_sharpe_ratio_per_period,
        minimum_track_record_length_per_period,
        probabilistic_sharpe_ratio_per_period,
    )
    from rquant.overfit_pbo import CSCVInput, calculate_cscv_pbo
    from rquant.web.experiment_platform_models import ExperimentCurvePoint

    _, projection, reader, _ = complete_family
    snapshot = projection.snapshot(NOW + timedelta(seconds=3))
    family = snapshot.families[0]
    facts = tuple(sorted(snapshot.attempts, key=lambda f: f.index))
    dates = tuple(date(2026, 1, 1) + timedelta(days=i) for i in range(120))
    # Pure wrapper fixture. These invented independent vectors are never passed
    # off as a new sealed artifact, actual market study or production evidence.
    vectors = tuple(
        tuple(
            0.0003 * (j + 1) + 0.002 * math.sin(i / (3 + j)) + 0.001 * math.cos(i / (7 + j))
            for i in range(120)
        )
        for j in range(4)
    )
    results = {}
    for index, fact in enumerate(facts):
        original = read_experiment_result(
            fact, family, results=reader, authority=projection.authority
        )
        nav = 100.0
        points = []
        for day, value in zip(dates, vectors[index], strict=True):
            nav *= 1 + value
            points.append(
                ExperimentCurvePoint(
                    trade_date=day, nav=nav, daily_return=value, benchmark_nav=None
                )
            )
        results[fact.attempt.spec.experiment_id] = original.model_copy(
            update={"curves": tuple(points)}
        )
    independence = ExperimentIndependenceEvidence(
        evidence_id="synthetic-independent-fixture",
        body_hash="b" * 64,
        family_id=family.family_id,
        period_end_dates=dates,
        result_hashes=tuple(results[f.attempt.spec.experiment_id].result_hash for f in facts),
        independent_observations=120,
        independent_trial_count=2,
        assumptions=("synthetic independent observations; wrapper test only",),
    )
    evidence = build_overfit_evidence(
        family,
        facts,
        read=lambda f: results[f.attempt.spec.experiment_id],
        independence=independence,
    )
    inputs = []
    for values in vectors:
        center = mean(values)
        variance = mean((v - center) ** 2 for v in values)
        inputs.append(
            SinglePeriodSharpeInput(
                observed_sharpe_per_period=center / stdev(values),
                benchmark_sharpe_per_period=0.0,
                skewness=mean((v - center) ** 3 for v in values) / variance**1.5,
                pearson_kurtosis=mean((v - center) ** 4 for v in values) / variance**2,
                independent_observations=120,
            )
        )
    spread = stdev(value.observed_sharpe_per_period for value in inputs)
    by_id = {s.experiment_id: s for s in evidence.statistics}
    for fact, value in zip(facts, inputs, strict=True):
        actual = by_id[fact.attempt.spec.experiment_id]
        assert actual.psr == probabilistic_sharpe_ratio_per_period(value)
        assert actual.mintrl == minimum_track_record_length_per_period(value, confidence=0.95)
        assert actual.dsr == deflated_sharpe_ratio_per_period(
            DeflatedSharpeInput(
                selected_strategy=value,
                independent_trial_count=2,
                family_sharpe_std_per_period=spread,
            )
        )
    assert evidence.pbo == calculate_cscv_pbo(
        CSCVInput(
            candidate_ids=tuple(f.attempt.spec.experiment_id for f in facts),
            period_end_dates=dates,
            returns_by_observation=tuple(tuple(row[i] for row in vectors) for i in range(120)),
            slice_count=4,
        )
    )
    assert (
        evidence.period_end_dates == dates
        and len(evidence.return_vector_digests) == 4
        and len(evidence.sharpe_inputs) == 4
    )
    incomplete = list(facts)
    for index, status in ((2, "failed"), (3, "cancelled")):
        attempt = ExperimentAttempt.model_validate(
            {
                **facts[index].attempt.model_dump(mode="python"),
                "status": status,
                "first_error": "synthetic " + status,
            }
        )
        incomplete[index] = facts[index].model_copy(
            update={"attempt": attempt, "result_hash": None, "manifest_hash": None}
        )
    partial = independence.model_copy(update={"result_hashes": independence.result_hashes[:2]})
    changed = build_overfit_evidence(
        family,
        tuple(incomplete),
        read=lambda f: results[f.attempt.spec.experiment_id],
        independence=partial,
    )
    pvalues = tuple(
        (s.experiment_id, Decimal(str(1 - s.psr.probability)))
        for s in changed.statistics
        if s.psr is not None
    )
    corrected = ExperimentRegistry._benjamini_hochberg(pvalues, hypothesis_count=4)
    assert len(pvalues) == 2 and changed.search_count == 4 and changed.pbo is None
    assert all(
        s.dsr is None and s.failed_count == s.cancelled_count == 1 for s in changed.statistics
    )
    assert all(
        s.bh_adjusted_p
        == (None if s.experiment_id not in corrected else float(corrected[s.experiment_id]))
        for s in changed.statistics
    )
    high = family.model_copy(
        update={"request": family.request.model_copy(update={"target_period_sharpe": Decimal("5")})}
    )
    unreachable = build_overfit_evidence(
        high, facts, read=lambda f: results[f.attempt.spec.experiment_id], independence=independence
    )
    assert all(
        s.mintrl.status == "unreachable"
        and s.mintrl.minimum_observations is None
        and "当前收益水平无法达到目标。" in s.reasons
        for s in unreachable.statistics
    )
    with pytest.raises(ValueError, match="independence"):
        build_overfit_evidence(
            family,
            facts,
            read=lambda f: results[f.attempt.spec.experiment_id],
            independence=independence.model_copy(update={"independent_trial_count": 5}),
        )
    tied = {
        identifier: result.model_copy(update={"curves": next(iter(results.values())).curves})
        for identifier, result in results.items()
    }
    tied_evidence = build_overfit_evidence(
        family, facts, read=lambda f: tied[f.attempt.spec.experiment_id]
    )
    assert tied_evidence.pbo is None and all(
        "候选夏普并列，不能计算过拟合概率。" in s.reasons for s in tied_evidence.statistics
    )
    short = {
        identifier: result.model_copy(update={"curves": result.curves[:58]})
        for identifier, result in results.items()
    }
    short_evidence = build_overfit_evidence(
        family, facts, read=lambda f: short[f.attempt.spec.experiment_id]
    )
    assert short_evidence.pbo is None and all(
        "每半段至少需要 30 个收益样本。" in s.reasons
        or "收益样本数不能等分为所选切片。" in s.reasons
        for s in short_evidence.statistics
    )
    shifted = dict(results)
    identifier = facts[0].attempt.spec.experiment_id
    shifted[identifier] = results[identifier].model_copy(
        update={"curves": results[identifier].curves[1:]}
    )
    mismatch = build_overfit_evidence(
        family, facts, read=lambda f: shifted[f.attempt.spec.experiment_id]
    )
    assert mismatch.pbo is None and all(s.psr is None and s.reasons for s in mismatch.statistics)
