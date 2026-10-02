"""The original owned worker publishes and replays every diagnostic binding."""

from __future__ import annotations

import json
import os
import statistics
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb
import pytest

from rquant.factor.run_configuration import (
    open_factor_run_configuration,
    run_configured_factor_worker,
)
from tests.unit.test_factor_neutralization_jobs import _ready, _service


def _executed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, industry: bool = True) -> tuple:
    from rquant.factor.run_backend import FactorRunPageControlBackend
    from tests.unit.test_factor_run_configuration import _configured

    if industry:
        root, reference, request, prepared, config, _, _ = _ready(tmp_path, monkeypatch, "none")
    else:
        root, reference, request = _configured(tmp_path)
        with open_factor_run_configuration(root, reference) as loaded:
            config, prepared = loaded.configuration, loaded.source
    request = type(request).model_validate(
        {
            **request.model_dump(),
            "parameters": {
                **request.parameters.model_dump(),
                "mad_multiple": 0.1,
                "extended_statistics": True,
                "ic_method": "normal",
                "holding_sessions": 1,
            },
        }
    )
    backend = FactorRunPageControlBackend(root, reference, clock=lambda: request.requested_at)
    service, outbox = _service(tmp_path, backend, request)
    receipt = service._submit_trusted_factor_run(
        request,
        authenticated_actor_id="alice",
        verified_registry_instance_id=config.registry_identity.instance_id,
    )
    assert receipt.result["status"] == "queued"
    result = run_configured_factor_worker(root, reference, clock=lambda: request.requested_at)
    return root, reference, request, prepared, config, result, outbox


@pytest.mark.parametrize("industry", [True, False])
def test_extended_owned_worker_replay_serving_web_and_independent_mad(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    industry: bool,
) -> None:
    import weakref

    from rquant.factor.daily_stream import FactorDailyStreamBatch
    from rquant.factor.neutralization_context import FactorNeutralizationReadLease
    from rquant.factor.result_serving import (
        project_factor_result_projections,
        validate_factor_result_projections,
    )
    from rquant.factor.stream_adapter import FactorStreamAdapter
    from rquant.factor.stream_job_artifact import verify_factor_stream_artifacts
    from rquant.web.app import create_app
    from rquant.web.settings import WebSettings
    from tests.support.web_proxy_identity import TEST_PROXY_PROOF, ResearcherTestClient
    from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture

    contexts = []
    query = FactorNeutralizationReadLease.query

    def queried(lease: FactorNeutralizationReadLease, **kwargs: object) -> object:
        assert all(ref() is None for ref in contexts)
        batch = query(lease, **kwargs)
        contexts.append(weakref.ref(batch))
        return batch

    monkeypatch.setattr(FactorNeutralizationReadLease, "query", queried)
    advance = FactorStreamAdapter.__next__

    def advanced(adapter: FactorStreamAdapter) -> object:
        assert adapter._current_context is None
        return advance(adapter)

    monkeypatch.setattr(FactorStreamAdapter, "__next__", advanced)

    root, reference, request, prepared, config, result, outbox = _executed(
        tmp_path, monkeypatch, industry=industry
    )
    assert result.status == "succeeded", result
    original = outbox.lookup_factor_run_command(request, authenticated_actor_id="alice")[0]
    assert result.record.spec == original.spec
    verified = verify_factor_stream_artifacts(
        result.record.spec, result.record.completion, config.artifact_root, config.member_root
    )
    full, display = verified.full, verified.display
    extended = full.result.research.research.statistics.extended_statistics
    assert display.mad_multiple == 0.1 and display.extended_statistics == extended
    assert extended.industry_status == ("available" if industry else "unavailable")
    assert extended.ic_method == "normal"
    assert full.spec.adapter_request.formula.neutralization == "none"
    raw = full.result.research.research.adapter_completion
    panels = {f.trade_date: f.panel_date for f in raw.feature_days}
    with duckdb.connect(str(prepared.receipt.request.replica_path), read_only=True) as replica:
        for journal in full.journal.days:
            batch = FactorDailyStreamBatch.model_validate_json(
                (config.artifact_root / journal.artifact.filename).read_bytes()
            )
            assert (batch.context is not None) == industry
            if industry:
                assert batch.context.panel_date == panels[journal.trade_date]
                assert batch.context.sources == full.spec.adapter_request.formula.sources.context
            if all(v.value is None for v in batch.factor_values):
                continue
            at = tuple(panels.values()).index(panels[journal.trade_date])
            dates = tuple(panels.values())[at - 2 : at + 1]
            rows = replica.execute(
                "SELECT ts_code,close FROM daily_bar WHERE trade_date IN (SELECT unnest(?)) "
                "ORDER BY ts_code,trade_date",
                [list(dates)],
            ).fetchall()
            samples = {c: [] for c in batch.universe.stock_codes}
            for c, value in rows:
                if c in samples:
                    samples[c].append(value)
            y = [statistics.mean(samples[c]) for c in batch.universe.stock_codes]
            center = statistics.median(y)
            spread = 0.1 * 1.4826 * statistics.median(abs(v - center) for v in y)
            assert [v.value for v in batch.factor_values] == pytest.approx(
                [min(center + spread, max(center - spread, v)) for v in y]
            )
    if industry:
        assert raw.context_read_query_count == raw.processed_days
        assert any(
            day.trade_date.weekday() == 0 and day.panel_date.weekday() == 4
            for day in extended.industry_coverage_days
        )
    projections = project_factor_result_projections(
        config.ledger_identity, config.artifact_root, available_at=FIXTURE_BUILT_AT
    )
    snapshot = validate_factor_result_projections({p.table_name: p for p in projections})
    assert snapshot.displays[0].extended_statistics == extended
    serving = tmp_path / "serving"
    build_web_fixture(serving, "baseline", factor_result_projections=projections)
    with TemporaryDirectory(
        prefix=".diagnostics-web-", dir=Path(__file__).resolve().parents[2]
    ) as private:
        proof = Path(private) / "proof"
        proof.write_text(TEST_PROXY_PROOF)
        proof.chmod(0o400)
        app = create_app(
            WebSettings(
                serving_root=serving,
                stale_after_seconds=1e9,
                ingress_socket_path=Path(private) / "web.sock",
                proxy_proof_file=proof,
            ),
            clock=lambda: FIXTURE_BUILT_AT + timedelta(seconds=30),
            background=False,
        )
        with ResearcherTestClient(app) as client:
            response = client.get("/api/v1/factors/results")
            assert response.status_code == 200, response.text
            detail = client.get(
                "/api/v1/factors/results/" + result.record.job_id,
                params={"generation_id": response.json()["serving"]["generation_id"]},
            )
            assert detail.status_code == 200, detail.text
            research = detail.json()["data"]["research"]
            assert research["mad_multiple"] == 0.1
            assert research["extended_statistics"] == json.loads(extended.model_dump_json())
    assert not Path(private).exists()
    assert not list(config.lake_root.glob(".execution_sessions/*"))
    assert not list(config.lake_root.glob(".industry-reader-*"))
    assert all(ref() is None for ref in contexts)


def test_extended_industry_natural_tail_cannot_publish_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor.industry_source import FactorIndustryReadLease
    from rquant.factor.run_backend import FactorRunPageControlBackend

    root, reference, request, _, config, context, _ = _ready(tmp_path, monkeypatch, "none")
    request = type(request).model_validate(
        {
            **request.model_dump(),
            "parameters": {**request.parameters.model_dump(), "extended_statistics": True},
        }
    )
    backend = FactorRunPageControlBackend(root, reference, clock=lambda: request.requested_at)
    service, _ = _service(tmp_path, backend, request)
    service._submit_trusted_factor_run(
        request,
        authenticated_actor_id="alice",
        verified_registry_instance_id=config.registry_identity.instance_id,
    )
    query = FactorIndustryReadLease.query
    changed = []

    def replaced(lease: object, requested: object) -> object:
        batch = query(lease, requested)
        if not changed:
            path = config.lake_root / context.industry.artifact.relative_path
            replacement = path.with_name("synthetic-replacement.parquet")
            replacement.write_bytes(path.read_bytes())
            os.replace(replacement, path)
            changed.append(True)
        return batch

    monkeypatch.setattr(FactorIndustryReadLease, "query", replaced)
    result = run_configured_factor_worker(root, reference, clock=lambda: request.requested_at)
    assert changed and result.status == "failed"
    assert not list(config.artifact_root.glob("factor-stream-full-*"))
    assert not list(config.artifact_root.glob("factor-stream-display-*"))
    assert not list(config.lake_root.glob(".execution_sessions/*"))
    assert not list(config.lake_root.glob(".industry-reader-*"))


@pytest.mark.parametrize("damage", ["industry", "autocorrelation", "label_day"])
def test_extended_rehashed_wrong_diagnostics_or_panel_are_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, damage: str
) -> None:
    from rquant.factor.stream_job_artifact import verify_factor_stream_artifacts
    from tests.unit.test_factor_stream_job_artifact import _hashed, _replace_full

    _, _, _, _, config, result, _ = _executed(tmp_path, monkeypatch)
    assert result.status == "succeeded"
    full = verify_factor_stream_artifacts(
        result.record.spec, result.record.completion, config.artifact_root, config.member_root
    ).full
    research = full.result.research.research
    extended = research.statistics.extended_statistics
    if damage == "industry":
        first = extended.industry_summaries[0]
        bad = first.model_copy(
            update={"ic_summary": first.ic_summary.model_copy(update={"mean": 0.123})}
        )
        extended = extended.model_copy(
            update={"industry_summaries": (bad, *extended.industry_summaries[1:])}
        )
    elif damage == "autocorrelation":
        points = list(extended.autocorrelation_points)
        index = next(i for i, p in enumerate(points) if p.status == "ok")
        points[index] = points[index].model_copy(update={"value": 0.123})
        extended = extended.model_copy(update={"autocorrelation_points": tuple(points)})
    else:
        from rquant.factor.daily_stream import FactorDailyStreamBatch
        from rquant.factor.stream_job_artifact import (
            FactorStreamJournalDay,
            publish_stream_artifact,
        )
        from rquant.runtime_contracts import canonical_sha256

        journal = full.journal.days[0]
        batch = FactorDailyStreamBatch.model_validate_json(
            (config.artifact_root / journal.artifact.filename).read_bytes()
        )
        context = batch.context
        panel = context.panel_date - timedelta(days=1)
        context = type(context).model_validate(
            {
                **context.model_dump(),
                "panel_date": panel,
                "industry_facts": tuple(
                    f.model_copy(update={"trade_date": panel}) for f in context.industry_facts
                ),
            }
        )
        batch = type(batch).model_validate({**batch.model_dump(), "context": context})
        replacement = FactorStreamJournalDay(
            trade_date=journal.trade_date,
            artifact=publish_stream_artifact(config.artifact_root, "journal-day", batch),
            batch_sha256=canonical_sha256(batch),
        )
        # Preserve the outer grid and original batch digests, then alter the owned day bytes.
        target = config.artifact_root / journal.artifact.filename
        changed_bytes = (config.artifact_root / replacement.artifact.filename).read_bytes()
        target.write_bytes(changed_bytes)
        with pytest.raises(ValueError):
            verify_factor_stream_artifacts(
                result.record.spec,
                result.record.completion,
                config.artifact_root,
                config.member_root,
            )
        return
    stats = _hashed(research.statistics, extended_statistics=extended)
    changed = _hashed(
        full.result,
        research=_hashed(full.result.research, research=_hashed(research, statistics=stats)),
    )
    forged = _replace_full(full, config.artifact_root, result.record.completion, result=changed)
    with pytest.raises(ValueError):
        verify_factor_stream_artifacts(
            result.record.spec, forged, config.artifact_root, config.member_root
        )


def test_extended_lost_receipt_recovers_original_parameters_and_rejects_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor.run_backend import FactorRunPageControlBackend

    root, reference, request, _, config, _, _ = _ready(tmp_path, monkeypatch, "none")
    request = type(request).model_validate(
        {
            **request.model_dump(),
            "parameters": {
                **request.parameters.model_dump(),
                "mad_multiple": 0.1,
                "extended_statistics": True,
                "ic_method": "normal",
            },
        }
    )
    backend = FactorRunPageControlBackend(root, reference, clock=lambda: request.requested_at)
    service, outbox = _service(tmp_path, backend, request)
    finish = outbox.finish_effect

    def interrupted(*args: object, **kwargs: object) -> None:
        raise KeyboardInterrupt("synthetic original receipt lost")

    monkeypatch.setattr(outbox, "finish_effect", interrupted)
    with pytest.raises(KeyboardInterrupt):
        service._submit_trusted_factor_run(
            request,
            authenticated_actor_id="alice",
            verified_registry_instance_id=config.registry_identity.instance_id,
        )
    original = outbox.lookup_factor_run_command(request, authenticated_actor_id="alice")[0]
    backend.reference = reference.model_copy(update={"sha256": "0" * 64})
    monkeypatch.setattr(outbox, "finish_effect", finish)
    service.consumer.clock = lambda: request.requested_at + timedelta(minutes=10)
    receipt = service._resume_trusted_factor_run(request, authenticated_actor_id="alice")
    assert receipt.result["spec_sha256"] == original.spec.spec_sha256
    assert original.spec.adapter_request.formula.mad_multiple == 0.1
    assert original.spec.adapter_request.extended_statistics
    assert original.spec.adapter_request.ic_method == "normal"
    for update in ({"mad_multiple": None}, {"extended_statistics": False}):
        with pytest.raises(ValueError):
            service._resume_trusted_factor_run(
                request.model_copy(
                    update={
                        "parameters": request.parameters.model_copy(update=update),
                    }
                ),
                authenticated_actor_id="alice",
            )
    result = run_configured_factor_worker(root, reference, clock=lambda: request.requested_at)
    assert result.status == "succeeded" and result.record.spec == original.spec
    assert not list(config.lake_root.glob(".execution_sessions/*"))
