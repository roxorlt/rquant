from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from pathlib import Path

from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_deployment_bundle import install_runtime_deployment_bundle
from rquant.runtime_service_builtin import build_builtin_registry
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import (
    RuntimeServiceKind,
    RuntimeServiceManifest,
    load_runtime_service_manifest,
)
from rquant.runtime_serving_authority import ServingSourceAuthorityPublisher
from rquant.runtime_serving_snapshot import (
    LAB_JOBS_DATASET_ID,
    PAPER_ACCOUNTS_DATASET_ID,
    PROMOTIONS_DATASET_ID,
    REFERENCE_SLOW_AUTHORITY_DATASET_ID,
    RUNTIME_HEALTH_DATASET_ID,
    SIGNALS_DATASET_ID,
    LabJobsPayload,
    PaperAccountsPayload,
    PromotionsPayload,
    ReferenceSlowPayload,
    RuntimeHealthPayload,
    SignalDeliveryPayload,
    SourceReadResult,
)
from rquant.serving_contracts import FreshnessStatus
from rquant.serving_publisher import ServingReader

NOW = datetime(2026, 8, 1, 3, 0, tzinfo=UTC)
COMMIT = "a" * 40


def _source_result(dataset_id: str, payload: object) -> SourceReadResult:
    values: dict[str, object] = {
        "dataset_id": dataset_id,
        "sequence": 0,
        "event_time": NOW - timedelta(seconds=2),
        "published_at": NOW - timedelta(seconds=1),
        "status": FreshnessStatus.FRESH,
        "reason": None,
        "payload": payload,
    }
    values["generation_id"] = canonical_sha256(values)
    return SourceReadResult.model_validate(values)


def test_bundle_to_default_registry_publishes_readonly_serving_generation(
    tmp_path: Path,
) -> None:
    runtime_root = tmp_path / "runtime"
    notifier_instance = "svc-" + hashlib.sha256(b"notifier.admin.shadow.v1").hexdigest()
    paper_instance = "svc-" + hashlib.sha256(b"paper-broker.shadow-main.v1").hexdigest()
    authorities = {
        SIGNALS_DATASET_ID: (
            runtime_root / "live" / "notifications" / notifier_instance / "serving-authority",
            SignalDeliveryPayload(),
        ),
        PAPER_ACCOUNTS_DATASET_ID: (
            runtime_root / "live" / "paper-brokers" / paper_instance / "serving-authority",
            PaperAccountsPayload(),
        ),
        RUNTIME_HEALTH_DATASET_ID: (
            runtime_root / "control" / "authority-runtime-health",
            RuntimeHealthPayload(),
        ),
        LAB_JOBS_DATASET_ID: (
            runtime_root / "research" / "serving-authorities" / "lab-jobs",
            LabJobsPayload(),
        ),
        PROMOTIONS_DATASET_ID: (
            runtime_root / "research" / "serving-authorities" / "promotions",
            PromotionsPayload(),
        ),
        REFERENCE_SLOW_AUTHORITY_DATASET_ID: (
            runtime_root / "live" / "reference-slow" / "serving-authority",
            ReferenceSlowPayload(
                reference_generation_id="f" * 64,
                revision=1,
                price_basis="raw_session",
                adjustment_basis="tushare_adj_factor",
                available_at=NOW - timedelta(seconds=1),
            ),
        ),
    }
    for dataset_id, (root, payload) in authorities.items():
        root.parent.mkdir(parents=True, exist_ok=True)
        ServingSourceAuthorityPublisher(
            root=root,
            producer_commit=COMMIT,
            dataset_id=dataset_id,
            payload_kind=payload.payload_kind,
            clock=lambda: NOW,
        ).publish(_source_result(dataset_id, payload))

    manifest = RuntimeServiceManifest(
        service_id="serving-publisher",
        service_kind=RuntimeServiceKind.SERVING_PUBLISHER,
        plane=RuntimeServicePlane.SERVING,
        interval_seconds=15,
        stale_after_seconds=60,
        producer_commit=COMMIT,
        settings={
            "serving_root": str(runtime_root / "serving"),
            "schema_version": 3,
            "source_authorities": [
                {"dataset_id": dataset_id, "root": str(root)}
                for dataset_id, (root, _payload) in authorities.items()
            ],
        },
    )
    receipt = install_runtime_deployment_bundle(
        runtime_root,
        producer_commit=COMMIT,
        manifests=(manifest,),
        capability_env={manifest.service_id: {}},
        schema_bootstrap_reason="fixed integration fixture initial schema",
    )
    instance = receipt.instance_mapping[manifest.service_id]
    loaded = load_runtime_service_manifest(
        runtime_root / "current" / "manifests" / f"{instance}.json",
        expected_commit=COMMIT,
        expected_generation=receipt.generation_hash,
    )

    result = build_builtin_registry(clock=lambda: NOW).build(loaded)()

    assert result.source_generations.keys() >= authorities.keys()
    with ServingReader(runtime_root / "serving").open_current_readonly() as connection:
        assert connection.execute("SELECT count(*) FROM serving_status").fetchone() == (1,)
        assert connection.execute("SELECT count(*) FROM signals").fetchone() == (0,)
    assert receipt.unit_mapping[manifest.service_id] == (
        "rquant-runtime-serving@svc-"
        + hashlib.sha256(manifest.service_id.encode()).hexdigest()
        + ".service"
    )
