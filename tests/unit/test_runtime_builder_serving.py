from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from rquant.runtime_builder_serving import (
    ServingRuntimeSnapshot,
    serving_publisher_builder,
)
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.serving_contracts import FreshnessStatus, ServingDatasetWatermark
from rquant.serving_publisher import ServingPublisher
from rquant.serving_read_models import SERVING_TABLE_SPECS, ServingReadModelInput

NOW = datetime(2026, 7, 31, 2, 10, tzinfo=UTC)
COMMIT = "a" * 40
SIGNAL_GENERATION = "b" * 64
PAPER_GENERATION = "c" * 64


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
    return ServingRuntimeSnapshot(
        read_model=ServingReadModelInput(observed_at=observed_at),
        watermarks=(
            _watermark("signal_bus", SIGNAL_GENERATION, sequence=7),
            _watermark(
                "paper",
                PAPER_GENERATION,
                sequence=3,
                status=paper_status,
                reason=paper_reason,
            ),
        ),
        source_generations={
            "signal_bus": SIGNAL_GENERATION,
            "paper": PAPER_GENERATION,
        },
    )


def _manifest(
    tmp_path: Path,
    *,
    plane: RuntimeServicePlane = RuntimeServicePlane.SERVING,
    kind: RuntimeServiceKind = RuntimeServiceKind.SERVING_PUBLISHER,
) -> RuntimeServiceManifest:
    return RuntimeServiceManifest(
        service_id="serving.publisher",
        service_kind=kind,
        plane=plane,
        interval_seconds=15,
        stale_after_seconds=60,
        producer_commit=COMMIT,
        settings={
            "serving_root": str(tmp_path / "serving"),
            "schema_version": 3,
        },
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
    assert result.degraded_reasons == (
        "serving:paper:degraded:paper snapshot delayed",
    )
    assert result.source_generations["paper"] == PAPER_GENERATION
    assert result.source_generations["signal_bus"] == SIGNAL_GENERATION
    assert len(result.source_generations["serving_generation"]) == 64
    assert publisher.current_manifest().generation_id == (
        result.source_generations["serving_generation"]
    )
    assert publisher.current_manifest().row_counts["serving_status"] == 1


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
    snapshot = ServingRuntimeSnapshot(
        read_model=ServingReadModelInput(observed_at=future),
        watermarks=(
            _watermark(
                "signal_bus",
                SIGNAL_GENERATION,
                sequence=7,
                published_at=future,
            ),
        ),
        source_generations={"signal_bus": SIGNAL_GENERATION},
    )
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
            watermarks=(),
            source_generations={"signal_bus": SIGNAL_GENERATION},
        )
