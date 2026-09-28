"""Adapt one complete ops sample to the immutable optional Serving owner."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

from rquant.ops_status import OpsSnapshot, OpsStatusCollector, load_signed_ops_manifest
from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_serving_authority import (
    ServingSourceAuthorityPointer,
    ServingSourceAuthorityPublisher,
)
from rquant.runtime_serving_snapshot import OpsStatusPayload, SourceReadResult
from rquant.serving_contracts import FreshnessStatus
from rquant.serving_read_models import ServingProjectionPayload


def ops_status_projections(sample: OpsSnapshot) -> tuple[ServingProjectionPayload, ...]:
    at = sample.sampled_at
    host = ServingProjectionPayload(
        table_name="ops_host_status",
        available_at=at,
        rows=(
            {
                "host_name": sample.host_name,
                "boot_id": sample.boot_id,
                "manifest_digest": sample.manifest_digest,
                "sampled_at": at.isoformat(),
                "memory_total_bytes": sample.host_memory_total_bytes,
                "memory_available_bytes": sample.host_memory_available_bytes,
            },
        ),
    )
    units = ServingProjectionPayload(
        table_name="ops_unit_status",
        available_at=at,
        rows=tuple(unit.model_dump(mode="json") for unit in sample.units),
    )
    resources = ServingProjectionPayload(
        table_name="ops_resource_status",
        available_at=at,
        rows=tuple(resource.model_dump(mode="json") for resource in sample.resources),
    )
    return host, units, resources


def ops_status_source_result(sample: OpsSnapshot) -> SourceReadResult:
    sample = OpsSnapshot.model_validate(sample)
    values: dict[str, object] = {
        "dataset_id": "ops_status",
        "sequence": int(sample.sampled_at.timestamp() * 1_000_000),
        "event_time": sample.sampled_at,
        "published_at": sample.sampled_at,
        "status": FreshnessStatus.FRESH,
        "reason": None,
        "payload": OpsStatusPayload(snapshot=sample, projections=ops_status_projections(sample)),
    }
    return SourceReadResult.model_validate(
        {**values, "generation_id": canonical_sha256(values)}
    )


def publish_ops_status_snapshot(
    sample: OpsSnapshot,
    *,
    root: Path,
    producer_commit: str,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> ServingSourceAuthorityPointer:
    publisher = ServingSourceAuthorityPublisher(
        root=root,
        producer_commit=producer_commit,
        dataset_id="ops_status",
        payload_kind="ops_status",
        clock=clock,
        max_bytes=512 * 1024,
    )
    return publisher.publish(ops_status_source_result(sample))


def collect_and_publish_ops_status(
    *,
    manifest_path: Path,
    manifest_public_key_pem: bytes,
    authority_root: Path,
    producer_commit: str,
    collector: OpsStatusCollector,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> ServingSourceAuthorityPointer:
    manifest, digest = load_signed_ops_manifest(
        manifest_path,
        public_key_pem=manifest_public_key_pem,
        expected_host=collector.host_name(),
    )
    sample = collector.collect(manifest)
    if sample.manifest_digest != digest:
        raise ValueError("ops sample manifest identity changed before publication")
    return publish_ops_status_snapshot(
        sample,
        root=authority_root,
        producer_commit=producer_commit,
        clock=clock,
    )


__all__ = [
    "collect_and_publish_ops_status",
    "ops_status_source_result",
    "ops_status_projections",
    "publish_ops_status_snapshot",
]
