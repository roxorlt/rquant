from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from rquant.runtime_builder_serving import (
    ServingReferenceSlowEvidence,
    ServingRuntimeSnapshot,
    serving_publisher_builder,
)
from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.runtime_serving_authority import ServingSourceAuthorityPublisher
from rquant.runtime_serving_snapshot import (
    LAB_JOBS_DATASET_ID,
    PAPER_ACCOUNTS_DATASET_ID,
    PROMOTIONS_DATASET_ID,
    REFERENCE_SLOW_AUTHORITY_DATASET_ID,
    REFERENCE_SLOW_CONTRACT_DATASET_ID,
    REFERENCE_SLOW_DATASET_ID,
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
from rquant.serving_contracts import FreshnessStatus, ServingDatasetWatermark
from rquant.serving_publisher import ServingPublisher, ServingReader
from rquant.serving_read_models import (
    SERVING_TABLE_SPECS,
    ServingReadModelInput,
    serving_physical_table_specs_fingerprint,
)

NOW = datetime(2026, 7, 31, 2, 10, tzinfo=UTC)
COMMIT = "a" * 40
SIGNAL_GENERATION = "b" * 64
PAPER_GENERATION = "c" * 64
REFERENCE_GENERATION = "d" * 64
REFERENCE_AUTHORITY_GENERATION = "e" * 64


def _reference_evidence(*, available_at: datetime = NOW) -> ServingReferenceSlowEvidence:
    return ServingReferenceSlowEvidence(
        reference_generation_id=REFERENCE_GENERATION,
        revision=1,
        price_basis="raw_session",
        adjustment_basis="tushare_adj_factor",
        available_at=available_at,
    )


def _watermark(
    dataset_id: str,
    generation_id: str,
    *,
    sequence: int,
    status: FreshnessStatus = FreshnessStatus.FRESH,
    reason: str | None = None,
    published_at: datetime = NOW,
) -> ServingDatasetWatermark:
    return ServingDatasetWatermark(
        dataset_id=dataset_id,
        generation_id=generation_id,
        event_time=published_at - timedelta(seconds=1),
        published_at=published_at,
        sequence=sequence,
        status=status,
        reason=reason,
    )


def _snapshot(
    *,
    observed_at: datetime = NOW,
    paper_status: FreshnessStatus = FreshnessStatus.FRESH,
    paper_reason: str | None = None,
) -> ServingRuntimeSnapshot:
    reference = _reference_evidence(available_at=observed_at)
    return ServingRuntimeSnapshot(
        read_model=ServingReadModelInput(observed_at=observed_at),
        reference_slow=reference,
        watermarks=(
            _watermark("signal_bus", SIGNAL_GENERATION, sequence=7),
            _watermark(
                "paper",
                PAPER_GENERATION,
                sequence=3,
                status=paper_status,
                reason=paper_reason,
            ),
            _watermark(
                REFERENCE_SLOW_AUTHORITY_DATASET_ID,
                REFERENCE_AUTHORITY_GENERATION,
                sequence=1,
                published_at=observed_at,
            ),
            _watermark(
                REFERENCE_SLOW_DATASET_ID,
                reference.reference_generation_id,
                sequence=reference.revision,
                published_at=reference.available_at,
            ),
            _watermark(
                REFERENCE_SLOW_CONTRACT_DATASET_ID,
                reference.contract_generation_id,
                sequence=reference.revision,
                published_at=reference.available_at,
            ),
        ),
        source_generations={
            "signal_bus": SIGNAL_GENERATION,
            "paper": PAPER_GENERATION,
            REFERENCE_SLOW_AUTHORITY_DATASET_ID: REFERENCE_AUTHORITY_GENERATION,
            REFERENCE_SLOW_DATASET_ID: reference.reference_generation_id,
            REFERENCE_SLOW_CONTRACT_DATASET_ID: reference.contract_generation_id,
        },
    )


def _manifest(
    tmp_path: Path,
    *,
    plane: RuntimeServicePlane = RuntimeServicePlane.SERVING,
    kind: RuntimeServiceKind = RuntimeServiceKind.SERVING_PUBLISHER,
    settings: dict[str, object] | None = None,
) -> RuntimeServiceManifest:
    return RuntimeServiceManifest(
        service_id="serving.publisher",
        service_kind=kind,
        plane=plane,
        interval_seconds=15,
        stale_after_seconds=60,
        producer_commit=COMMIT,
        settings=settings
        or {
            "serving_root": str(tmp_path / "serving"),
            "schema_version": 3,
        },
    )


def _authority_result(
    dataset_id: str,
    payload: object,
    *,
    sequence: int = 1,
    published_at: datetime = NOW - timedelta(seconds=1),
) -> SourceReadResult:
    values: dict[str, object] = {
        "dataset_id": dataset_id,
        "sequence": sequence,
        "event_time": published_at - timedelta(seconds=1),
        "published_at": published_at,
        "status": FreshnessStatus.FRESH,
        "reason": None,
        "payload": payload,
    }
    values["generation_id"] = canonical_sha256(values)
    return SourceReadResult.model_validate(values)


def _authority_settings(tmp_path: Path) -> tuple[dict[str, object], dict[str, Path]]:
    payloads = {
        SIGNALS_DATASET_ID: SignalDeliveryPayload(),
        PAPER_ACCOUNTS_DATASET_ID: PaperAccountsPayload(),
        RUNTIME_HEALTH_DATASET_ID: RuntimeHealthPayload(),
        LAB_JOBS_DATASET_ID: LabJobsPayload(),
        PROMOTIONS_DATASET_ID: PromotionsPayload(),
        REFERENCE_SLOW_AUTHORITY_DATASET_ID: ReferenceSlowPayload(
            **_reference_evidence(available_at=NOW - timedelta(seconds=1)).model_dump()
        ),
    }
    roots = {dataset_id: tmp_path / "authorities" / dataset_id for dataset_id in payloads}
    (tmp_path / "authorities").mkdir(parents=True)
    for dataset_id, payload in payloads.items():
        ServingSourceAuthorityPublisher(
            root=roots[dataset_id],
            producer_commit=COMMIT,
            dataset_id=dataset_id,
            payload_kind=payload.payload_kind,
            clock=lambda: NOW,
        ).publish(_authority_result(dataset_id, payload))
    return (
        {
            "serving_root": str(tmp_path / "serving"),
            "schema_version": 3,
            "source_authorities": [
                {"dataset_id": dataset_id, "root": str(root)} for dataset_id, root in roots.items()
            ],
        },
        roots,
    )


def test_builder_publishes_snapshot_and_maps_runtime_result(tmp_path: Path) -> None:
    snapshot = _snapshot(
        paper_status=FreshnessStatus.DEGRADED,
        paper_reason="paper snapshot delayed",
    )
    loader_calls: list[datetime] = []

    def load_snapshot(as_of: datetime) -> ServingRuntimeSnapshot:
        loader_calls.append(as_of)
        return snapshot

    step = serving_publisher_builder(
        snapshot_loader=load_snapshot,
        clock=lambda: NOW,
    )(_manifest(tmp_path))

    result = step()
    publisher = ServingPublisher(
        tmp_path / "serving",
        producer_commit=COMMIT,
        schema_version=3,
        table_specs=SERVING_TABLE_SPECS,
    )

    assert loader_calls == [NOW]
    assert result.input_sequence == 7
    assert result.output_sequence == 7
    assert result.processed_count == 1
    assert result.backlog_count == 0
    assert result.degraded_reasons == ("serving:paper:degraded:paper snapshot delayed",)
    assert result.source_generations["paper"] == PAPER_GENERATION
    assert result.source_generations["signal_bus"] == SIGNAL_GENERATION
    assert len(result.source_generations["serving_generation"]) == 64
    assert (
        publisher.current_manifest().generation_id
        == (result.source_generations["serving_generation"])
    )
    assert publisher.current_manifest().row_counts["serving_status"] == 1


def test_builder_acknowledges_schema_only_after_serving_generation_is_published(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []

    class Acknowledger:
        def acknowledge_published_generation(self, **kwargs: object) -> None:
            assert (tmp_path / "serving" / "current.json").is_file()
            calls.append(dict(kwargs))

    monkeypatch.setattr(
        "rquant.runtime_builder_serving.current_runtime_schema_consumer_acknowledgers",
        lambda **_kwargs: (Acknowledger(),),
    )
    step = serving_publisher_builder(
        snapshot_loader=lambda _as_of: _snapshot(),
        clock=lambda: NOW,
    )(_manifest(tmp_path))

    result = step()

    assert calls == [
        {
            "serving_generation_id": result.source_generations["serving_generation"],
            "serving_physical_schema_fingerprint": (serving_physical_table_specs_fingerprint()),
            "observed_at": NOW,
        }
    ]


def test_builder_does_not_acknowledge_when_serving_publish_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[object] = []
    monkeypatch.setattr(
        "rquant.runtime_builder_serving.current_runtime_schema_consumer_acknowledgers",
        lambda **_kwargs: calls.append("resolved") or (),
    )
    monkeypatch.setattr(
        "rquant.runtime_builder_serving.ServingPublisher.publish",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("publish failed")),
    )
    step = serving_publisher_builder(
        snapshot_loader=lambda _as_of: _snapshot(),
        clock=lambda: NOW,
    )(_manifest(tmp_path))

    with pytest.raises(RuntimeError, match="publish failed"):
        step()

    assert calls == []


def test_repeated_identical_snapshot_is_idempotent_without_extra_generation(
    tmp_path: Path,
) -> None:
    snapshot = _snapshot()
    step = serving_publisher_builder(
        snapshot_loader=lambda _as_of: snapshot,
        clock=lambda: NOW + timedelta(minutes=5),
    )(_manifest(tmp_path))

    first = step()
    first_paths = tuple((tmp_path / "serving" / "generations").iterdir())
    second = step()
    second_paths = tuple((tmp_path / "serving" / "generations").iterdir())

    assert second == first
    assert second.processed_count == 1
    assert len(first_paths) == 1
    assert second_paths == first_paths


def test_step_rejects_snapshot_evidence_after_clock(tmp_path: Path) -> None:
    future = NOW + timedelta(seconds=1)
    snapshot = _snapshot(observed_at=future)
    step = serving_publisher_builder(
        snapshot_loader=lambda _as_of: snapshot,
        clock=lambda: NOW,
    )(_manifest(tmp_path))

    with pytest.raises(ValueError, match="future evidence"):
        step()
    assert not (tmp_path / "serving" / "current.json").exists()


def test_builder_rejects_invalid_kind_plane_settings_and_snapshot_binding(
    tmp_path: Path,
) -> None:
    builder = serving_publisher_builder(
        snapshot_loader=lambda _as_of: _snapshot(),
        clock=lambda: NOW,
    )

    with pytest.raises(ValueError, match="kind"):
        builder(_manifest(tmp_path, kind=RuntimeServiceKind.NOTIFIER))
    with pytest.raises(ValueError, match="serving plane"):
        builder(_manifest(tmp_path, plane=RuntimeServicePlane.LIVE))

    relative_payload = _manifest(tmp_path).model_dump(mode="json")
    relative_payload["settings"]["serving_root"] = "relative/serving"
    with pytest.raises(ValidationError, match="absolute"):
        builder(RuntimeServiceManifest.model_validate(relative_payload))

    bool_schema_payload = _manifest(tmp_path).model_dump(mode="json")
    bool_schema_payload["settings"]["schema_version"] = True
    with pytest.raises(ValidationError, match="schema_version"):
        builder(RuntimeServiceManifest.model_validate(bool_schema_payload))

    with pytest.raises(ValidationError, match="exactly one watermark"):
        ServingRuntimeSnapshot(
            read_model=ServingReadModelInput(observed_at=NOW),
            reference_slow=_reference_evidence(),
            watermarks=(),
            source_generations={"signal_bus": SIGNAL_GENERATION},
        )


def test_default_builder_reads_five_dynamic_owner_authorities(tmp_path: Path) -> None:
    settings, roots = _authority_settings(tmp_path)
    step = serving_publisher_builder(snapshot_loader=None, clock=lambda: NOW)(
        _manifest(tmp_path, settings=settings)
    )

    first = step()
    updated = _authority_result(
        SIGNALS_DATASET_ID,
        SignalDeliveryPayload(),
        sequence=2,
        published_at=NOW,
    )
    ServingSourceAuthorityPublisher(
        root=roots[SIGNALS_DATASET_ID],
        producer_commit=COMMIT,
        dataset_id=SIGNALS_DATASET_ID,
        payload_kind="signal_delivery",
        clock=lambda: NOW,
    ).publish(updated)
    second = step()

    assert first.source_generations[SIGNALS_DATASET_ID] != updated.generation_id
    assert second.source_generations[SIGNALS_DATASET_ID] == updated.generation_id
    assert second.input_sequence == 2
    assert second.output_sequence == 2


def test_reference_revision_publishes_new_generation_and_keeps_old_readable(
    tmp_path: Path,
) -> None:
    settings, roots = _authority_settings(tmp_path)
    step = serving_publisher_builder(snapshot_loader=None, clock=lambda: NOW)(
        _manifest(tmp_path, settings=settings)
    )
    first = step()
    first_generation = first.source_generations["serving_generation"]
    revised_reference = ReferenceSlowPayload(
        reference_generation_id="f" * 64,
        revision=2,
        price_basis="raw_session",
        adjustment_basis="tushare_adj_factor",
        available_at=NOW,
    )
    ServingSourceAuthorityPublisher(
        root=roots[REFERENCE_SLOW_AUTHORITY_DATASET_ID],
        producer_commit=COMMIT,
        dataset_id=REFERENCE_SLOW_AUTHORITY_DATASET_ID,
        payload_kind="reference_slow",
        clock=lambda: NOW,
    ).publish(
        _authority_result(
            REFERENCE_SLOW_AUTHORITY_DATASET_ID,
            revised_reference,
            sequence=2,
            published_at=NOW,
        )
    )

    second = step()

    assert second.source_generations[REFERENCE_SLOW_DATASET_ID] == "f" * 64
    assert second.source_generations["serving_generation"] != first_generation
    reader = ServingReader(tmp_path / "serving")
    with reader.acquire_historical_generation(first_generation) as acquired:
        assert acquired.manifest.generation_id == first_generation
        assert acquired.manifest.source_generations[REFERENCE_SLOW_DATASET_ID] == (
            REFERENCE_GENERATION
        )


def test_default_builder_requires_exact_owner_authority_set(tmp_path: Path) -> None:
    settings, _roots = _authority_settings(tmp_path)
    authorities = list(settings["source_authorities"])
    settings["source_authorities"] = authorities[:-1]

    with pytest.raises(ValidationError, match="exactly.*six|missing"):
        serving_publisher_builder(snapshot_loader=None, clock=lambda: NOW)(
            _manifest(tmp_path, settings=settings)
        )

    settings, _roots = _authority_settings(tmp_path / "duplicate")
    authorities = list(settings["source_authorities"])
    settings["source_authorities"] = [*authorities, authorities[0]]
    with pytest.raises(ValidationError, match="duplicate|exactly.*six"):
        serving_publisher_builder(snapshot_loader=None, clock=lambda: NOW)(
            _manifest(tmp_path, settings=settings)
        )


def test_injected_loader_cannot_mix_with_owner_authorities(tmp_path: Path) -> None:
    settings, _roots = _authority_settings(tmp_path)

    with pytest.raises(ValueError, match="cannot be combined"):
        serving_publisher_builder(
            snapshot_loader=lambda _as_of: _snapshot(),
            clock=lambda: NOW,
        )(_manifest(tmp_path, settings=settings))
