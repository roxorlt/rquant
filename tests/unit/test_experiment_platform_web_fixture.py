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
    output = tmp_path / "typed-web-fixture-04.json"
    assert not output.exists(), "retain the preceding actual fixture"
    output.write_text(json.dumps(fixture, ensure_ascii=False, indent=2) + "\n")


def _native_public_family_request() -> dict[str, object]:
    return {
        "command_id": "5a1fe4a6-6a1d-4e05-96ed-ec9da5e063e4",
        "requested_at": "2026-10-07T20:40:35.280523Z",
        "kind": "register_experiment_family",
        "request": {
            "kind": "native_minute_experiment",
            "name": "原生分钟请求类型验证",
            "configurations": [
                {
                    "kind": "native_minute",
                    "selection": {
                        "target": {
                            "source_kind": "builtin",
                            "owner_id": "researcher",
                            "strategy_id": "n_shape",
                            "name": "N 字形态",
                            "head": {
                                "version": 1,
                                "registration_fingerprint": "a" * 64,
                                "record_hash": "b" * 64,
                                "spec_fingerprint": "c" * 64,
                            },
                            "parameter_fingerprint": "d" * 64,
                            "cost_fingerprint": "e" * 64,
                        },
                        "source_key": "synthetic-public-type-only",
                        "source_version": 1,
                        "profile_hash": "f" * 64,
                    },
                    "start_date": "2026-01-05",
                    "end_date": "2026-01-06",
                }
            ],
            "protocol": {
                "train_range": {"start_date": "2026-01-05", "end_date": "2026-01-05"},
                "validation_range": {"start_date": "2026-01-06", "end_date": "2026-01-06"},
                "frozen_outer_test_range": {"start_date": "2026-01-07", "end_date": "2026-01-07"},
            },
        },
    }


def test_exp_native_public_request_preserves_the_exact_original_domain_command() -> None:
    from pydantic import TypeAdapter

    from rquant.experiment_platform import NativeMinuteExperimentRequest
    from rquant.experiment_platform_commands import RegisterExperimentFamily
    from rquant.runtime_contracts import canonical_sha256
    from rquant.web.experiment_platform_models import ExperimentWrite

    raw = _native_public_family_request()
    parsed = TypeAdapter(ExperimentWrite).validate_python(raw)
    assert type(parsed.request) is NativeMinuteExperimentRequest
    actor_payload = parsed.model_dump(mode="json") | {"actor_id": "researcher"}
    original = RegisterExperimentFamily.model_validate(raw | {"actor_id": "researcher"})
    converted = RegisterExperimentFamily.model_validate(actor_payload)
    assert converted == original
    assert canonical_sha256(converted) == canonical_sha256(original)


def test_exp_native_public_union_preserves_the_original_daily_request_json() -> None:
    from pydantic import TypeAdapter

    from rquant.backtest import RebalanceRule
    from rquant.experiment_platform import SearchDimension
    from rquant.portfolio.weights import PortfolioWeightRule
    from rquant.web.experiment_platform_models import ExperimentEditableRequest, ExperimentWrite
    from rquant.web.models.backtests import PortfolioEditableConfig
    from tests.paper_cost_fixtures import paper_execution_cost_spec

    original = ExperimentEditableRequest(
        name="日线原格式",
        base_config=PortfolioEditableConfig(
            start_date="2026-01-05",
            end_date="2026-01-06",
            initial_cash="1000000",
            weight_rule=PortfolioWeightRule(max_positions=2),
            rebalance_rule=RebalanceRule(kind="daily"),
            execution_cost_spec=paper_execution_cost_spec().model_dump(mode="python"),
        ),
        protocol=_native_public_family_request()["request"]["protocol"],
        dimensions=(SearchDimension(parameter="weight_rule.max_positions", values=(1, 2)),),
    )
    raw = _native_public_family_request() | {"request": original.model_dump(mode="json")}
    parsed = TypeAdapter(ExperimentWrite).validate_python(raw)
    assert type(parsed.request) is ExperimentEditableRequest
    assert parsed.request.model_dump(mode="json") == original.model_dump(mode="json")


def test_exp_native_public_union_does_not_accept_an_actor_or_unknown_native_fields() -> None:
    import pytest
    from pydantic import TypeAdapter, ValidationError

    from rquant.web.experiment_platform_models import ExperimentWrite

    raw = _native_public_family_request()
    with pytest.raises(ValidationError):
        TypeAdapter(ExperimentWrite).validate_python(raw | {"actor_id": "foreign"})
    request = raw["request"]
    with pytest.raises(ValidationError):
        TypeAdapter(ExperimentWrite).validate_python(raw | {"request": request | {"trusted": True}})
    with pytest.raises(ValidationError):
        TypeAdapter(ExperimentWrite).validate_python(
            raw | {"request": request | {"kind": "unknown"}}
        )


def test_exp_native_public_union_keeps_the_original_dates_and_head_rejections() -> None:
    import pytest
    from pydantic import TypeAdapter, ValidationError

    from rquant.web.experiment_platform_models import ExperimentWrite

    raw = _native_public_family_request()
    raw["request"]["protocol"]["validation_range"] = raw["request"]["protocol"]["train_range"]
    with pytest.raises(ValidationError):
        TypeAdapter(ExperimentWrite).validate_python(raw)
    raw = _native_public_family_request()
    raw["request"]["configurations"][0]["selection"]["target"]["head"]["record_hash"] = "not-a-hash"
    with pytest.raises(ValidationError):
        TypeAdapter(ExperimentWrite).validate_python(raw)
