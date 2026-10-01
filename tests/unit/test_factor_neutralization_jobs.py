"""The original owned command and ledger remain the execution authority."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from rquant.factor.run_backend import FactorRunPageControlBackend
from rquant.factor.run_configuration import (
    open_factor_run_configuration,
    run_configured_factor_worker,
    save_factor_run_configuration,
)
from tests.unit.test_factor_neutralization_sources import _configured_context, _context_module


def _ready(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str = "industry_size") -> tuple:
    from rquant.factor.run_configuration import save_factor_neutralization_context

    root, reference, request, prepared, config, industry, cap = _configured_context(
        tmp_path, monkeypatch
    )
    context = _context_module().bind_factor_neutralization_context(
        prepared, industry=industry, market_cap=cap
    )
    context_ref = save_factor_neutralization_context(root, context)
    config = type(config).model_validate(
        {**config.model_dump(), "neutralization_context": context_ref}
    )
    reference = save_factor_run_configuration(root, config)
    request = type(request).model_validate(
        {
            **request.model_dump(),
            "parameters": {**request.parameters.model_dump(), "neutralization": mode},
        }
    )
    backend = FactorRunPageControlBackend(root, reference, clock=lambda: request.requested_at)
    return root, reference, request, prepared, config, context, backend


def _service(tmp_path: Path, backend: object, request: object) -> tuple:
    from rquant.page_control import PageControlConsumer, PageControlOutbox, PageControlService

    outbox = PageControlOutbox(tmp_path / "outbox.sqlite")
    consumer = PageControlConsumer(
        outbox=outbox,
        data_dir=tmp_path,
        log_dir=tmp_path,
        factor_run_backend=backend,
        clock=lambda: request.requested_at,
    )
    return PageControlService(outbox=outbox, consumer=consumer), outbox


@pytest.mark.parametrize("mode", ["industry", "industry_size"])
def test_owned_run_actual_worker_publishes_original_mode_and_verified_context(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    from rquant.factor.stream_job_artifact import (
        FactorStreamFullArtifact,
        load_factor_stream_display,
    )

    root, reference, request, prepared, config, context, backend = _ready(
        tmp_path, monkeypatch, mode
    )
    assert all(option.available for option in backend.availability("alice").neutralizations)
    service, outbox = _service(tmp_path, backend, request)
    receipt = service._submit_trusted_factor_run(
        request,
        authenticated_actor_id="alice",
        verified_registry_instance_id=config.registry_identity.instance_id,
    )
    assert receipt.result["status"] == "queued"
    owned = outbox.lookup_factor_run_command(request, authenticated_actor_id="alice")[0]
    import weakref

    from rquant.factor.neutralization_context import FactorNeutralizationDayBatch

    contexts, observations = [], FactorNeutralizationDayBatch.observations

    def tracked(batch: object) -> object:
        assert all(reference() is None for reference in contexts)
        contexts.append(weakref.ref(batch))
        return observations(batch)

    monkeypatch.setattr(FactorNeutralizationDayBatch, "observations", tracked)
    result = run_configured_factor_worker(root, reference, clock=lambda: request.requested_at)
    assert result.status == "succeeded", result
    assert result.record.job_id == receipt.result["job_id"] and result.record.spec == owned.spec
    assert contexts and all(reference() is None for reference in contexts)
    completion = result.record.completion
    full = FactorStreamFullArtifact.model_validate_json(
        (config.artifact_root / completion.artifact_filename).read_bytes()
    )
    display = load_factor_stream_display(config.artifact_root, completion.display_artifact_sha256)
    assert full.spec.adapter_request.formula.neutralization == mode
    assert display.neutralization == completion.neutralization == mode
    assert (
        display.context == completion.context == full.spec.adapter_request.formula.sources.context
    )
    assert display.context.industry.source_read_boundary == "captured_api_responses"
    assert all(
        day.coverage.valid_count == len(context.scope.stock_codes) for day in display.coverage_days
    )
    assert any(
        day.trade_date.weekday() == 0 and day.panel_date.weekday() == 4
        for day in full.result.research.research.adapter_completion.feature_days
    )
    import duckdb
    import numpy as np

    from rquant.factor.daily_stream import FactorDailyStreamBatch

    panels = tuple(
        day.panel_date for day in full.result.research.research.adapter_completion.feature_days
    )
    with duckdb.connect(str(prepared.receipt.request.replica_path), read_only=True) as original:
        for day in full.journal.days:
            index = tuple(
                receipt.trade_date
                for receipt in full.result.research.research.adapter_completion.feature_days
            ).index(day.trade_date)
            batch = FactorDailyStreamBatch.model_validate_json(
                (config.artifact_root / day.artifact.filename).read_bytes()
            )
            if index < 2:
                assert all(value.value is None for value in batch.factor_values)
                continue
            raw = original.execute(
                "SELECT ts_code, trade_date, close FROM daily_bar "
                "WHERE trade_date IN (SELECT unnest(?)) ORDER BY ts_code, trade_date",
                [list(panels[index - 2 : index + 1])],
            ).fetchall()
            values = {code: [] for code in context.scope.stock_codes}
            for code, _, value in raw:
                values[code].append(value)
            y = np.array([sum(values[code]) / 3 for code in context.scope.stock_codes])
            matrix = np.ones((len(y), 1))
            if mode == "industry_size":
                caps = original.execute(
                    "SELECT total_mv FROM daily_basic WHERE trade_date=? ORDER BY ts_code",
                    [panels[index]],
                ).fetchall()
                matrix = np.column_stack((matrix, np.log([value for (value,) in caps])))
            expected = y - matrix @ np.linalg.lstsq(matrix, y, rcond=None)[0]
            assert [value.value for value in batch.factor_values] == pytest.approx(
                expected, abs=1e-10
            )
    from datetime import timedelta
    from tempfile import TemporaryDirectory

    from rquant.factor.result_serving import (
        project_factor_result_projections,
        validate_factor_result_projections,
    )
    from rquant.web.app import create_app
    from rquant.web.settings import WebSettings
    from tests.support.web_proxy_identity import TEST_PROXY_PROOF, ResearcherTestClient
    from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture

    projections = project_factor_result_projections(
        config.ledger_identity, config.artifact_root, available_at=FIXTURE_BUILT_AT
    )
    snapshot = validate_factor_result_projections(
        {projection.table_name: projection for projection in projections}
    )
    assert snapshot.displays[0].neutralization == mode
    serving = tmp_path / "serving"
    build_web_fixture(serving, "baseline", factor_result_projections=projections)
    # The unchanged proxy verifier requires every proof parent to be private.
    with TemporaryDirectory(
        prefix=".neutralization-web-", dir=Path(__file__).resolve().parents[2]
    ) as proof_root:
        proof = Path(proof_root) / "synthetic-proof"
        proof.write_text(TEST_PROXY_PROOF)
        proof.chmod(0o400)
        app = create_app(
            WebSettings(
                serving_root=serving,
                stale_after_seconds=1e9,
                ingress_socket_path=Path(proof_root) / "web.sock",
                proxy_proof_file=proof,
            ),
            clock=lambda: FIXTURE_BUILT_AT + timedelta(seconds=30),
            background=False,
        )
        with ResearcherTestClient(app) as client:
            response = client.get("/api/v1/factors/results")
            assert response.status_code == 200, response.text
            generation = response.json()["serving"]["generation_id"]
            detail = client.get(
                f"/api/v1/factors/results/{result.record.job_id}",
                params={"generation_id": generation},
            )
            assert detail.status_code == 200, detail.text
            research = detail.json()["data"]["research"]
            assert research["neutralization"] == mode
            assert research["neutralization_label"] == (
                "行业" if mode == "industry" else "行业 + 市值"
            )
            assert "独立 API" in research["context_basis_label"]
    assert not list(config.lake_root.glob(".industry-reader-*")) and not list(
        config.lake_root.glob(".market-cap-reader-*")
    )
    assert not list(config.lake_root.glob(".execution_sessions/*"))


def test_missing_sources_disable_modes_and_refuse_without_task(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tests.unit.test_factor_run_configuration import _configured

    root, reference, request = _configured(tmp_path)
    backend = FactorRunPageControlBackend(root, reference, clock=lambda: request.requested_at)
    modes = backend.availability("alice").neutralizations
    assert [option.available for option in modes] == [True, False, False]
    request = type(request).model_validate(
        {
            **request.model_dump(),
            "parameters": {**request.parameters.model_dump(), "neutralization": "industry"},
        }
    )
    service, outbox = _service(tmp_path, backend, request)
    with pytest.raises(ValueError):
        backend.compile(
            request,
            verified_registry_instance_id=backend.configuration().registry_identity.instance_id,
        )
    with open_factor_run_configuration(root, reference) as loaded:
        assert not loaded.open_ledger(clock=lambda: request.requested_at).list_recent()


def test_lost_effect_recovers_original_context_after_configuration_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, reference, request, _, config, _, backend = _ready(tmp_path, monkeypatch)
    service, outbox = _service(tmp_path, backend, request)
    finish = outbox.finish_effect

    def crash(*args: object, **kwargs: object) -> None:
        raise KeyboardInterrupt("effect lost after original ledger commit")

    monkeypatch.setattr(outbox, "finish_effect", crash)
    with pytest.raises(KeyboardInterrupt):
        service._submit_trusted_factor_run(
            request,
            authenticated_actor_id="alice",
            verified_registry_instance_id=config.registry_identity.instance_id,
        )
    original = outbox.lookup_factor_run_command(request, authenticated_actor_id="alice")[0]
    backend.reference = backend.reference.model_copy(update={"sha256": "0" * 64})
    monkeypatch.setattr(outbox, "finish_effect", finish)
    from datetime import timedelta

    service.consumer.clock = lambda: request.requested_at + timedelta(minutes=10)
    restored = service._resume_trusted_factor_run(request, authenticated_actor_id="alice")
    assert restored.result["spec_sha256"] == original.spec.spec_sha256
    assert original.spec.adapter_request.formula.neutralization == "industry_size"
    with pytest.raises(ValueError):
        service._resume_trusted_factor_run(
            request.model_copy(
                update={
                    "parameters": request.parameters.model_copy(
                        update={"neutralization": "industry"}
                    )
                }
            ),
            authenticated_actor_id="alice",
        )
    with pytest.raises(PermissionError):
        service._resume_trusted_factor_run(request, authenticated_actor_id="bob")


@pytest.mark.parametrize("failure", ["middle_query", "tail_replacement"])
def test_context_failure_has_no_success_artifacts_and_closes_private_readers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    from rquant.factor.market_cap_source import FactorMarketCapReadLease

    root, reference, request, _, config, context, backend = _ready(tmp_path, monkeypatch)
    plan = backend.compile(
        request, verified_registry_instance_id=config.registry_identity.instance_id
    )
    with open_factor_run_configuration(root, reference) as loaded:
        ledger = loaded.open_ledger(clock=lambda: request.requested_at)
        ledger.submit(request.command_id, plan.spec)
    original_query, calls = FactorMarketCapReadLease.query, []

    def query(self: object, argument: object) -> object:
        calls.append(argument)
        if failure == "middle_query" and len(calls) == 2:
            raise OSError("synthetic context read failure")
        batch = original_query(self, argument)
        if failure == "tail_replacement" and len(calls) == len(
            plan.spec.adapter_request.formula.trading_days
        ):
            path = config.lake_root / context.market_cap.artifact.relative_path
            replace = path.with_name("synthetic-replacement.parquet")
            replace.write_bytes(path.read_bytes())
            replace.replace(path)
        return batch

    monkeypatch.setattr(FactorMarketCapReadLease, "query", query)
    result = run_configured_factor_worker(root, reference, clock=lambda: request.requested_at)
    assert result.status == "failed", result
    assert not list(config.artifact_root.glob("factor-stream-full-*"))
    assert not list(config.artifact_root.glob("factor-stream-display-*"))
    assert not list(config.lake_root.glob(".industry-reader-*")) and not list(
        config.lake_root.glob(".market-cap-reader-*")
    )
    assert not list(config.lake_root.glob(".execution_sessions/*"))


def test_sealed_context_tampering_refuses_configuration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, reference, _, _, config, _, _ = _ready(tmp_path, monkeypatch)
    path = root / config.neutralization_context.filename
    payload = json.loads(path.read_bytes())
    payload["prepared_source_sha256"] = "0" * 64
    path.write_text(json.dumps(payload))
    with pytest.raises(ValueError), open_factor_run_configuration(root, reference):
        pytest.fail("tampered context admitted")


def test_completion_rejects_unpaired_context_and_missing_mode_sources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, reference, request, _, config, _, backend = _ready(tmp_path, monkeypatch)
    plan = backend.compile(
        request, verified_registry_instance_id=config.registry_identity.instance_id
    )
    with open_factor_run_configuration(root, reference) as loaded:
        loaded.open_ledger(clock=lambda: request.requested_at).submit(request.command_id, plan.spec)
    result = run_configured_factor_worker(root, reference, clock=lambda: request.requested_at)
    assert result.status == "succeeded"
    completion = result.record.completion
    for context in (None, completion.context.model_copy(update={"binding_hash": "0" * 64})):
        with pytest.raises(ValueError):
            type(completion).model_validate({**completion.model_dump(), "context": context})
