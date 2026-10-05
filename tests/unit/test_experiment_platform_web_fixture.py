from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

from rquant.experiment_platform import ExperimentSourceProfile
from rquant.experiment_platform_evidence import ExperimentEvidencePublisher
from rquant.promotions_serving_authority import PromotionsSourceReader
from rquant.serving_contracts import FreshnessStatus, ServingDatasetWatermark
from rquant.serving_publisher import ServingPublisher
from rquant.serving_read_models import (
    SERVING_TABLE_SPECS,
    ServingProjectionInput,
    ServingReadModelInput,
    build_serving_read_models,
)
from rquant.web.experiment_platform_service import ExperimentWebService
from rquant.web.settings import WebSettings
from tests.support.web_proxy_identity import ProofTestClient, create_private_test_app
from tests.unit.test_experiment_platform import NOW, search
from tests.unit.test_experiment_platform_results import complete_family as complete_family
from tests.unit.test_portfolio_backtest import _CODES, _request


def test_exp23_typed_web_fixture_from_original_synthetic_sealed_results(
    complete_family, tmp_path: Path
) -> None:
    store, projection, results, _ = complete_family
    observed = NOW + timedelta(seconds=3)
    ExperimentEvidencePublisher(store=store, projection=projection, results=results)(observed)
    snapshot = projection.snapshot(observed)
    family = snapshot.families[0]
    facts = tuple(sorted(snapshot.attempts, key=lambda f: f.index))
    prepared = store.preparation("alice", family.family_id, 0).prepared
    raw = _request((_CODES[0],) * 6)
    profile = ExperimentSourceProfile(
        source_key=prepared.frozen.config.source_key,
        source_version=prepared.frozen.config.source_version,
        label="每日候选",
        producer_commit=raw.producer_commit,
        sources=prepared.frozen.sources,
        calendar=raw.calendar,
        coverage=family.request.protocol.train_range.model_copy(
            update={"end_date": family.request.protocol.frozen_outer_test_range.end_date}
        ),
        latest_complete=raw.days[-1].trade_date,
        phase_slice_available=True,
    )
    source = PromotionsSourceReader(
        registry=projection.registry,
        include_experiments=True,
        private_experiment_reader=projection,
    )(observed)
    tables = build_serving_read_models(
        ServingReadModelInput(
            observed_at=observed,
            projections=tuple(
                ServingProjectionInput.bind(
                    p, owner_dataset_id="promotions", owner_generation_id=source.generation_id
                )
                for p in source.payload.projections
            ),
        )
    )
    serving = tmp_path / "serving"
    manifest = ServingPublisher(
        serving,
        producer_commit=raw.producer_commit,
        schema_version=3,
        table_specs=SERVING_TABLE_SPECS,
    ).publish(
        tables,
        source_generations={"promotions": source.generation_id},
        built_at=observed,
        watermarks=(
            ServingDatasetWatermark(
                dataset_id="promotions",
                generation_id=source.generation_id,
                sequence=source.sequence,
                event_time=observed,
                published_at=observed,
                status=FreshnessStatus.FRESH,
            ),
        ),
    )
    service = ExperimentWebService(
        results=results,
        private_authority=projection.authority,
        profiles=(profile,),
        default_config=search().base_config,
        owners=frozenset({"alice"}),
        administrators=frozenset({"alice"}),
        enabled=True,
    )
    app = create_private_test_app(
        WebSettings(serving_root=serving),
        clock=lambda: observed,
        background=False,
        experiment_platform=service,
    )
    fixture = {
        "kind": "m8-typed-synthetic-web-fixture/v1",
        "actual_market_source": False,
        "actual_native_worker": False,
        "generation_id": manifest.generation_id,
    }
    with ProofTestClient(app, headers={"x-rquant-user": "alice"}) as client:

        def get(path: str, **params: str) -> dict:
            response = client.get(path, params={"generation_id": manifest.generation_id, **params})
            assert response.status_code == 200, response.text
            return response.json()

        prefix = "/api/v1/experiments"
        fixture["capabilities"] = get(prefix + "/capabilities")
        fixture["mine"] = get(prefix + "/mine")
        fixture["family"] = get(prefix + "/families/" + family.family_id)
        fixture["results"] = {
            f.attempt.spec.experiment_id: get(
                prefix + "/results/" + f.attempt.spec.experiment_id,
                result_hash=f.result_hash,
            )
            for f in facts
        }
        fixture["statistics"] = {
            f.attempt.spec.experiment_id: get(
                prefix + "/results/" + f.attempt.spec.experiment_id + "/statistics"
            )
            for f in facts
        }
        fixture["heatmap"] = get(
            prefix + "/families/" + family.family_id + "/heatmap",
            selected=facts[0].attempt.spec.experiment_id,
            x="weight_rule.max_positions",
            y="weight_rule.cash_reserve",
            phase="validation",
            metric="total_return",
        )
        fixture["comparison"] = get(
            prefix + "/compare",
            a=facts[0].attempt.spec.experiment_id,
            b=facts[1].attempt.spec.experiment_id,
        )
        for row in fixture["mine"]["data"]["items"]:
            full = fixture["results"][row["experiment_id"]]["data"]
            assert row["metrics"] == full["metrics"]
            assert row["result_hash"] == full["result_hash"]
            assert row["configuration"] == full["configuration"]
        assert fixture["family"]["data"]["items"] == sorted(
            fixture["mine"]["data"]["items"], key=lambda row: row["index"]
        )
        assert (
            client.get(
                prefix + "/results/" + facts[0].attempt.spec.experiment_id,
                params={"generation_id": manifest.generation_id, "result_hash": "0" * 64},
            ).status_code
            == 409
        )
    output = Path(__file__).resolve().parents[2] / (
        "data/verification/experiment-platform-20261005/implementation/final-repair-01/typed-web-fixture-04.json"
    )
    assert not output.exists(), "retain the preceding actual fixture"
    output.write_text(json.dumps(fixture, ensure_ascii=False, indent=2) + "\n")
