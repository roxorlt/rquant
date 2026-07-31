from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from rquant.runtime_contracts import canonical_sha256
from rquant.serving_contracts import (
    FreshnessStatus,
    ServingCurrentPointer,
    ServingDatasetWatermark,
    ServingGenerationManifest,
)

BUILT_AT = datetime(2026, 7, 31, 8, 0, tzinfo=UTC)
EVENT_TIME = datetime(2026, 7, 31, 7, 55, tzinfo=UTC)


def _watermark(
    dataset_id: str,
    generation_id: str,
    *,
    sequence: int = 1,
) -> ServingDatasetWatermark:
    return ServingDatasetWatermark(
        dataset_id=dataset_id,
        generation_id=generation_id,
        event_time=EVENT_TIME,
        published_at=BUILT_AT,
        sequence=sequence,
        status=FreshnessStatus.FRESH,
    )


def _manifest(**overrides: object) -> ServingGenerationManifest:
    payload: dict[str, object] = {
        "schema_version": 1,
        "source_generations": {
            "market_minute": "minute-generation",
            "strategy_signal": "signal-generation",
        },
        "watermarks": (
            _watermark("market_minute", "minute-generation", sequence=10),
            _watermark("strategy_signal", "signal-generation", sequence=20),
        ),
        "content_sha256": "a" * 64,
        "row_counts": {"market_minute": 240, "strategy_signal": 3},
        "built_at": BUILT_AT,
        "producer_commit": "b" * 40,
    }
    payload.update(overrides)
    return ServingGenerationManifest(**payload)


def test_watermark_enforces_freshness_reason_and_time_invariants() -> None:
    stale = ServingDatasetWatermark(
        dataset_id="market_minute",
        generation_id="minute-generation",
        event_time=EVENT_TIME,
        published_at=BUILT_AT,
        sequence=0,
        status=FreshnessStatus.STALE,
        reason="source delayed",
    )

    assert stale.reason == "source delayed"
    with pytest.raises(ValidationError, match="fresh watermark cannot have reason"):
        ServingDatasetWatermark(
            dataset_id="market_minute",
            generation_id="minute-generation",
            event_time=EVENT_TIME,
            published_at=BUILT_AT,
            sequence=0,
            status=FreshnessStatus.FRESH,
            reason="not allowed",
        )
    with pytest.raises(ValidationError, match="non-fresh watermark requires reason"):
        ServingDatasetWatermark(
            dataset_id="market_minute",
            generation_id="minute-generation",
            event_time=EVENT_TIME,
            published_at=BUILT_AT,
            sequence=0,
            status=FreshnessStatus.UNAVAILABLE,
        )
    with pytest.raises(ValidationError, match="published_at cannot precede event_time"):
        ServingDatasetWatermark(
            dataset_id="market_minute",
            generation_id="minute-generation",
            event_time=BUILT_AT,
            published_at=EVENT_TIME,
            sequence=0,
            status=FreshnessStatus.FRESH,
        )


def test_manifest_derives_stable_generation_id_from_canonical_content() -> None:
    first = _manifest()
    local_tz = timezone(timedelta(hours=8))
    reordered = _manifest(
        source_generations={
            "strategy_signal": "signal-generation",
            "market_minute": "minute-generation",
        },
        watermarks=tuple(reversed(first.watermarks)),
        row_counts={"strategy_signal": 3, "market_minute": 240},
        built_at=BUILT_AT.astimezone(local_tz),
    )

    assert first.generation_id == reordered.generation_id
    assert len(first.generation_id) == 64
    assert tuple(item.dataset_id for item in first.watermarks) == (
        "market_minute",
        "strategy_signal",
    )

    explicit = _manifest(generation_id=first.generation_id)
    assert explicit == first
    with pytest.raises(ValidationError, match="generation_id does not match"):
        _manifest(generation_id="0" * 64)


def test_manifest_rejects_duplicate_or_mismatched_dataset_generations() -> None:
    duplicate = _watermark("market_minute", "minute-generation", sequence=11)
    with pytest.raises(ValidationError, match="watermarks must be unique"):
        _manifest(watermarks=(_watermark("market_minute", "minute-generation"), duplicate))

    with pytest.raises(ValidationError, match="does not match source_generations"):
        _manifest(
            watermarks=(
                _watermark("market_minute", "wrong-generation"),
                _watermark("strategy_signal", "signal-generation"),
            )
        )
    with pytest.raises(ValidationError, match="must have exactly one watermark"):
        _manifest(
            source_generations={"market_minute": "minute-generation"},
            watermarks=(),
            row_counts={"market_minute": 1},
        )
    with pytest.raises(ValidationError):
        _manifest(row_counts={"market_minute": -1, "strategy_signal": 3})
    output_tables = _manifest(row_counts={"signals": 240, "paper_holdings": 3})
    assert output_tables.row_counts == {"paper_holdings": 3, "signals": 240}


def test_manifest_cannot_be_built_before_source_watermarks_are_published() -> None:
    published_later = BUILT_AT + timedelta(seconds=1)

    with pytest.raises(ValidationError, match="built_at cannot precede watermark"):
        _manifest(
            watermarks=(
                _watermark("market_minute", "minute-generation").model_copy(
                    update={"published_at": published_later}
                ),
                _watermark("strategy_signal", "signal-generation"),
            )
        )


def test_manifest_round_trips_without_exposing_mutable_mappings() -> None:
    manifest = _manifest()

    restored = ServingGenerationManifest.model_validate_json(manifest.model_dump_json())

    assert restored == manifest
    with pytest.raises(TypeError):
        manifest.source_generations["market_minute"] = "changed"
    with pytest.raises(TypeError):
        manifest.row_counts["market_minute"] = 0


def test_current_pointer_binds_manifest_and_previous_generation() -> None:
    manifest = _manifest()
    manifest_sha256 = canonical_sha256(manifest)
    pointer = ServingCurrentPointer(
        generation_id=manifest.generation_id,
        manifest_sha256=manifest_sha256,
        published_at=BUILT_AT,
        previous_generation_id="previous-generation",
    )

    assert pointer.manifest_sha256 == manifest_sha256
    with pytest.raises(ValidationError, match="previous_generation_id must differ"):
        ServingCurrentPointer(
            generation_id=manifest.generation_id,
            manifest_sha256=manifest_sha256,
            published_at=BUILT_AT,
            previous_generation_id=manifest.generation_id,
        )
    with pytest.raises(ValidationError):
        ServingCurrentPointer(
            generation_id=manifest.generation_id,
            manifest_sha256=manifest_sha256,
            published_at=BUILT_AT,
            unexpected=True,
        )
