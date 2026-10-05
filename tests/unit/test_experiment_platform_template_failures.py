"""Private original C5 capacity and preparing-family failures retain planned N."""

from __future__ import annotations

from uuid import UUID

import pytest

from rquant.strategy_authoring import StrategyAuthoringConflict
from tests.unit.test_experiment_platform import NOW
from tests.unit.test_experiment_platform_templates import admitted, binding_for


def test_c5t04_actual_original_pending_id_budget_rejects_whole_n(tmp_path) -> None:
    binding, _, private, _, request = binding_for(tmp_path)
    store, record = admitted(tmp_path, binding, request)
    original = store.template_slots("alice", record.family_id)[0].request
    # These are actual original accepted saves, including their server-generated IDs.
    for index in range(497):
        private.accept(
            original.model_copy(update={"command_id": str(UUID(int=10000 + index))}),
            owner_id="alice",
            catalog=binding._catalog("alice"),
            expected_identity=private.identity(),
        )
    with pytest.raises(StrategyAuthoringConflict, match="complete planned"):
        binding.prepare_definitions(store, record)
    slots = store.template_slots("alice", record.family_id)
    assert len(slots) == len(record.actual_configurations) == 4
    assert all(s.state == "failed" and s.failure == "capacity" for s in slots)
    assert store.registry.list_family_attempts(record.family_id) == ()
    with private._connection(expected_identity=private.identity()) as connection:
        assert connection.execute("SELECT count(*) FROM command_refs").fetchone()[0] == 497


def test_c5t03_cancel_during_original_save_keeps_all_slots_and_no_jobs(
    tmp_path, monkeypatch
) -> None:
    binding, _, private, _, request = binding_for(tmp_path)
    store, record = admitted(tmp_path, binding, request)
    complete = private.complete_save
    seen = []

    def interrupted(*args, **kwargs):
        saved = complete(*args, **kwargs)
        seen.append(saved.strategy_id)
        raise RuntimeError("save committed, reply lost")

    monkeypatch.setattr(private, "complete_save", interrupted)
    with pytest.raises(RuntimeError):
        binding.prepare_definitions(store, record)
    assert (
        store.cancel_family(
            owner="alice", family_id=record.family_id, request_id=UUID(int=890), now=NOW
        )
        == ()
    )
    assert (
        store.cancel_family(
            owner="alice", family_id=record.family_id, request_id=UUID(int=890), now=NOW
        )
        == ()
    )
    assert len(store.template_slots("alice", record.family_id)) == 4
    assert all(s.state == "cancelled" for s in store.template_slots("alice", record.family_id))
    assert store.registry.list_family_attempts(record.family_id) == ()
    monkeypatch.setattr(private, "complete_save", complete)
    with pytest.raises(ValueError, match="cancelled"):
        binding.prepare_definitions(store, store.get_family("alice", record.family_id))
    assert len(private.list_current(owner_id="alice")) == 1


def test_c5t03_preparing_failed_and_cancelled_projection_retains_owned_complete_n(tmp_path) -> None:
    from rquant.experiment_platform_projection import ExperimentPrivateProjectionReader
    from rquant.experiment_registry import ExperimentRegistryReadonlyReader
    from rquant.lab_jobs import LabJobReader, LabJobStore

    binding, _, _, _, request = binding_for(tmp_path)
    store, record = admitted(tmp_path, binding, request)
    jobs = LabJobStore(tmp_path / "jobs.sqlite")
    jobs.initialize()
    readonly = ExperimentRegistryReadonlyReader(
        store.registry.path, managed_trust_root=store.registry._path_authority._managed_trust_root
    )
    projection = ExperimentPrivateProjectionReader(
        registry=readonly, jobs=LabJobReader(jobs.path), owners=frozenset({"alice", "bob"})
    )
    first = projection.snapshot(NOW)
    assert len(first.families) == 1, "preparing family is omitted from the private projection"
    assert first.attempts == ()
    family = first.families[0]
    assert (family.owner, family.preparation_state, family.planned_count, family.search_count) == (
        "alice",
        "preparing",
        4,
        4,
    )
    assert tuple(s.index for s in family.preparations) == tuple(range(4))
    store.save_template_slot(store.template_slots("alice", record.family_id)[0], failure="capacity")
    failed = projection.snapshot(NOW).families[0]
    assert failed.preparations[0].definition_state == "failed"
    assert len(failed.preparations) == 4 and projection.snapshot(NOW).attempts == ()
    store.cancel_family(
        owner="alice", family_id=record.family_id, request_id=UUID(int=898), now=NOW
    )
    cancelled = projection.snapshot(NOW).families[0]
    assert cancelled.preparation_state == "cancelled"
    assert cancelled.preparations[0].definition_state == "failed"
    assert all(s.definition_state == "cancelled" for s in cancelled.preparations[1:])
    assert LabJobReader(jobs.path).list_jobs().items == ()


def test_c5t03_actual_serving_api_shows_failed_preparation_only_to_owner(tmp_path) -> None:
    from rquant.experiment_platform_projection import ExperimentPrivateProjectionReader
    from rquant.experiment_registry import ExperimentRegistryReadonlyReader
    from rquant.lab_jobs import LabJobReader, LabJobStore
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

    binding, _, _, _, request = binding_for(tmp_path)
    store, record = admitted(tmp_path, binding, request)
    store.save_template_slot(store.template_slots("alice", record.family_id)[0], failure="capacity")
    jobs = LabJobStore(tmp_path / "jobs.sqlite")
    jobs.initialize()
    projection = ExperimentPrivateProjectionReader(
        registry=ExperimentRegistryReadonlyReader(
            store.registry.path,
            managed_trust_root=store.registry._path_authority._managed_trust_root,
        ),
        jobs=LabJobReader(jobs.path),
        owners=frozenset({"alice", "bob"}),
    )
    source = PromotionsSourceReader(
        registry=projection.registry, include_experiments=True, private_experiment_reader=projection
    )(NOW)
    tables = build_serving_read_models(
        ServingReadModelInput(
            observed_at=NOW,
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
        serving, producer_commit="0" * 40, schema_version=3, table_specs=SERVING_TABLE_SPECS
    ).publish(
        tables,
        source_generations={"promotions": source.generation_id},
        built_at=NOW,
        watermarks=(
            ServingDatasetWatermark(
                dataset_id="promotions",
                generation_id=source.generation_id,
                sequence=source.sequence,
                event_time=NOW,
                published_at=NOW,
                status=FreshnessStatus.FRESH,
            ),
        ),
    )
    app = create_private_test_app(
        WebSettings(serving_root=serving),
        clock=lambda: NOW,
        background=False,
        experiment_platform=ExperimentWebService(owners=frozenset({"alice", "bob"})),
    )
    params = {"generation_id": manifest.generation_id}
    with ProofTestClient(app, headers={"x-rquant-user": "alice"}) as client:
        mine = client.get("/api/v1/experiments/mine", params=params)
        assert mine.status_code == 200, mine.text
        data = mine.json()["data"]
        assert data["items"] == [] and data["retained_count"] == 0
        assert len(data["preparing_families"]) == 1
        assert data["preparing_families"][0]["planned_count"] == 4
        family = client.get("/api/v1/experiments/families/" + record.family_id, params=params)
        assert family.status_code == 200, family.text
        detail = family.json()["data"]
        assert (
            len(detail["preparations"]) == 4
            and detail["failed_count"] == 1
            and detail["items"] == []
        )
        assert detail["preparation_state"] == "preparing"
        assert record.template_baseline is not None
        assert tuple(slot["index"] for slot in detail["preparations"]) == tuple(range(4))
        for slot in detail["preparations"]:
            assert slot["strategy_name"] == record.template_baseline.name
            assert slot["strategy_version"] == request.template.head.version
            assert (
                slot["rules"]["entry"]
                == record.template_baseline.version.rules.model_dump(mode="json")["entry"]
            )
            assert slot["rules"]["weight_rule"] == slot["configuration"]["weight_rule"]
            assert len(slot["metrics"]) == 18 and all(m["value"] is None for m in slot["metrics"])
            assert "job_id" not in slot and "result_hash" not in slot
        assert "source_path" not in family.text and "metadata_identity" not in family.text
        client.headers["x-rquant-user"] = "bob"
        mine = client.get("/api/v1/experiments/mine", params=params)
        assert mine.status_code == 200 and mine.json()["data"]["preparing_families"] == []
        assert client.get(
            "/api/v1/experiments/families/" + record.family_id, params=params
        ).status_code in (404, 409)
