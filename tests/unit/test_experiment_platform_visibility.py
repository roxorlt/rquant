from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from uuid import UUID

import pytest

from rquant.experiment_platform import (
    ExperimentChildRegistration,
    ExperimentPreparationReceipt,
    ExperimentSearchRequest,
    SearchDimension,
    stable_experiment_interaction,
    stable_experiment_job,
)
from rquant.experiment_platform_projection import (
    ExperimentPrivateProjectionReader,
    ExperimentSearchContext,
)
from rquant.experiment_registry import ExperimentRegistryReadonlyReader, HypothesisFamilyManifest
from rquant.lab_job_center import LabCommandSubmissionFacade
from rquant.lab_job_protocol import LabCommandEnvelope
from rquant.lab_jobs import LabJobReader, LabJobStore
from rquant.portfolio_backtest_source import build_portfolio_plan
from rquant.promotions_serving_authority import PromotionsSourceReader
from rquant.runtime_contracts import canonical_sha256
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
from tests.unit.test_experiment_platform import NOW, prepared_family, search
from tests.unit.test_experiment_platform_flow import preparation as preparation


def test_exp22_compact_context_keeps_complete_request_identity() -> None:
    request = search()
    context = ExperimentSearchContext.from_request(request)
    assert context.request_fingerprint == canonical_sha256(request)
    assert context.protocol == request.protocol and context.dimensions == request.dimensions
    assert {"base_config", "name"}.isdisjoint(context.model_dump())
    assert ExperimentSearchContext.model_validate_json(context.model_dump_json()) == context


def test_exp22_actual_registry_ab501_legacy501_source_and_owner_pagination(
    tmp_path: Path, monkeypatch
) -> None:
    store, seed, _, definitions = prepared_family(tmp_path)
    original = store.preparation(seed.owner, seed.family_id, 0)
    # This routing fixture reuses the original exact registration already read
    # and checked by prepared_family. Runtime submission still reads the original
    # registry. Re-fingerprinting that callable 1,503 times is not this test's gate.
    registration = original.prepared.registration
    monkeypatch.setattr(
        definitions,
        "latest_strategy_spec",
        lambda name, as_of: (
            registration
            if name == "portfolio_backtest" and registration.available_at <= as_of
            else None
        ),
    )
    # All ledger rows reuse this real synthetic, immutable input. This case tests
    # visibility and cursors; it makes no claim of 1,503 executed research jobs.
    request = ExperimentSearchRequest.model_validate(
        {
            **search().model_dump(mode="python"),
            "dimensions": (
                SearchDimension(parameter="weight_rule.max_positions", values=(1,)),
                SearchDimension(parameter="weight_rule.cash_reserve", values=(".25",)),
            ),
        }
    )
    expected: dict[str, list[str]] = {"alice": [], "bob": [], "legacy": []}
    for index in range(501):
        for offset, owner in enumerate(("legacy", "alice", "bob")):
            at = NOW + timedelta(seconds=3 * index + offset + 1)
            rid = UUID(int=10000 + 3 * index + offset)
            chosen_request = request.model_copy(update={"seed": rid.int})
            if owner == "legacy":
                family_id = f"ordinary-{index}"
            else:
                record = store.begin_request(
                    owner=owner,
                    request_id=rid,
                    body_hash=f"{rid.int:064x}",
                    request=chosen_request,
                    registered_at=at,
                )
                family_id = record.family_id
            prepared = build_portfolio_plan(
                original.prepared.frozen,
                original.prepared.published,
                definitions=definitions,
                protocol=request.protocol,
                now=at,
                deadline=NOW + timedelta(hours=1),
                random_seed=rid.int,
                family_id=family_id,
                hypothesis_variant="configuration-0",
            )
            experiment_id = prepared.formal_plan.spec.experiment_id
            expected[owner].append(experiment_id)
            if owner == "legacy":
                manifest = HypothesisFamilyManifest(
                    hypothesis_family=family_id,
                    experiment_ids=(experiment_id,),
                    preregistered_at=at,
                    search_space_fingerprint=canonical_sha256((prepared.frozen.config,)),
                    metric_definition_fingerprint=prepared.formal_plan.spec.metric_definition_fingerprint,
                )
                store.registry.register_formal_plan(prepared.formal_plan, family_manifest=manifest)
                store.registry.register_attempt(prepared.formal_plan.spec, registered_at=at)
                continue
            receipt = ExperimentPreparationReceipt(
                owner=owner,
                family_id=family_id,
                index=0,
                source_identity=original.source_identity,
                source_path=original.source_path,
                file_identity=original.file_identity,
                file_sha256=original.file_sha256,
                prepared=prepared,
            )
            store.save_preparation(receipt)
            envelope = LabCommandEnvelope(
                request_id=LabCommandSubmissionFacade._request_id(
                    stable_experiment_interaction(owner, rid, 0)
                ),
                command=prepared.submission(job_id=stable_experiment_job(owner, rid, 0)).command,
            )
            child = ExperimentChildRegistration(
                config=prepared.frozen.config,
                plan=prepared.formal_plan,
                published=prepared.published,
                intent=LabCommandSubmissionFacade._experiment_submission_intent(envelope),
            )
            store.register_family_submission(owner=owner, request_id=rid, children=(child,))
    observed = NOW + timedelta(hours=1)
    reader = ExperimentRegistryReadonlyReader(
        store.registry.path, managed_trust_root=store.registry.path.parent
    )
    old = reader.read_legacy_shared_serving_snapshot(observed_at=observed)
    assert tuple(a.spec.experiment_id for a in old.attempts) == tuple(
        reversed(expected["legacy"][1:])
    )
    assert old.truncated and old.oldest_registered_at == NOW + timedelta(seconds=4)

    jobs = LabJobStore(tmp_path / "lab-jobs.sqlite")
    jobs.initialize()
    private = ExperimentPrivateProjectionReader(registry=reader, jobs=LabJobReader(jobs.path))
    source = PromotionsSourceReader(
        registry=reader, include_experiments=True, private_experiment_reader=private
    )(observed)
    event_only = PromotionsSourceReader(registry=reader, include_experiments=False)(observed)
    assert event_only.payload.promotions == source.payload.promotions == ()
    bound = tuple(
        ServingProjectionInput.bind(
            p, owner_dataset_id="promotions", owner_generation_id=source.generation_id
        )
        for p in source.payload.projections
    )
    tables = build_serving_read_models(
        ServingReadModelInput(observed_at=observed, projections=bound)
    )
    root = tmp_path / "serving"
    publisher = ServingPublisher(
        root, producer_commit="a" * 40, schema_version=3, table_specs=SERVING_TABLE_SPECS
    )
    manifest = publisher.publish(
        tables,
        source_generations={"promotions": source.generation_id},
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
        built_at=observed,
    )
    service = ExperimentWebService(enabled=False)

    def app():
        return create_private_test_app(
            WebSettings(serving_root=root, stale_after_seconds=1e9),
            clock=lambda: observed,
            background=False,
            experiment_platform=service,
        )

    for owner in ("alice", "bob"):
        with ProofTestClient(app(), headers={"x-rquant-user": owner}) as client:
            ids = []
            cursor = None
            first_cursor = None
            while True:
                params = {"limit": 50, "generation_id": manifest.generation_id}
                if cursor:
                    params["cursor"] = cursor
                response = client.get("/api/v1/experiments/mine", params=params)
                assert response.status_code == 200, response.text
                data = response.json()["data"]
                assert data["retained_count"] == 500 and data["truncated"]
                ids.extend(item["experiment_id"] for item in data["items"])
                cursor = data["next_cursor"]
                first_cursor = first_cursor or cursor
                if not cursor:
                    break
            assert ids == list(reversed(expected[owner][1:])) and len(ids) == len(set(ids)) == 500
            assert all(item not in expected["bob" if owner == "alice" else "alice"] for item in ids)
            with ProofTestClient(
                app(), headers={"x-rquant-user": "bob" if owner == "alice" else "alice"}
            ) as other:
                denied = other.get(
                    "/api/v1/experiments/mine",
                    params={
                        "limit": 50,
                        "generation_id": manifest.generation_id,
                        "cursor": first_cursor,
                    },
                )
                assert denied.status_code == 409 and not any(
                    item in denied.text for item in expected[owner]
                )
            old_ids = []
            old_cursor = None
            while True:
                params = {"limit": 50, "generation_id": manifest.generation_id}
                if old_cursor:
                    params["cursor"] = old_cursor
                response = client.get("/api/v1/experiments", params=params)
                assert response.status_code == 200, response.text
                data = response.json()["data"]
                old_ids.extend(item["experiment_id"] for item in data["items"])
                old_cursor = data["next_cursor"]
                assert data["retained_count"] == 500 and data["truncated"]
                if not old_cursor:
                    break
            assert old_ids == list(reversed(expected["legacy"][1:]))
            assert (
                client.get(
                    "/api/v1/experiments/mine", params={"generation_id": "f" * 64}
                ).status_code
                == 409
            )


@pytest.mark.parametrize("failure", ("inode", "mode", "root"))
def test_exp26_live_read_refresh_keeps_original_path_identity(preparation, failure: str) -> None:
    from rquant.experiment_platform_projection import ExperimentPrivateResultAuthority
    from rquant.experiment_registry import ExperimentRegistryError

    store, _, _, _, _, _ = preparation
    reader = ExperimentRegistryReadonlyReader(
        store.registry.path, managed_trust_root=store.registry.path.parent
    )
    authority = ExperimentPrivateResultAuthority(reader)
    store.install_policy(months=0, now=NOW)
    authority.refresh_live_identity()
    path = store.registry.path
    if failure == "mode":
        path.chmod(0o644)
    elif failure == "inode":
        replacement = path.with_name("replacement.sqlite3")
        replacement.write_bytes(path.read_bytes())
        replacement.chmod(0o600)
        replacement.replace(path)
    else:
        moved = path.parent.with_name("replaced-root")
        path.parent.rename(moved)
        path.parent.mkdir(mode=0o700)
        path.write_bytes((moved / path.name).read_bytes())
        path.chmod(0o600)
    with pytest.raises((ExperimentRegistryError, PermissionError, ValueError, OSError)):
        authority.refresh_live_identity()
