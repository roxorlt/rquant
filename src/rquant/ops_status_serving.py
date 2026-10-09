"""Adapt one complete ops sample to the immutable optional Serving owner."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

from rquant.ops_status import OpsSnapshot, OpsStatusCollector, load_signed_ops_manifest
from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_health_details import RuntimeHealthMetric
from rquant.runtime_serving_authority import (
    ServingSourceAuthorityPointer,
    ServingSourceAuthorityPublisher,
    ServingSourceAuthorityReader,
    ServingSourceAuthorityUnavailableError,
)
from rquant.runtime_serving_snapshot import OpsStatusPayload, SourceReadResult
from rquant.serving_contracts import FreshnessStatus
from rquant.serving_read_models import ServingProjectionPayload
from rquant.task_center_projection import TaskOpsSample, task_ops_projections
from rquant.task_center_runtime import TaskUnitRunSource
from rquant.task_cpu import LinuxTaskCpuReader, TaskCpuObservation


def ops_status_projections(
    sample: OpsSnapshot | TaskOpsSample,
) -> tuple[ServingProjectionPayload, ...]:
    task_sample = (
        TaskOpsSample.model_validate(sample) if isinstance(sample, TaskOpsSample) else None
    )
    if task_sample is not None:
        sample = task_sample.snapshot
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
    original = host, units, resources
    return original if task_sample is None else (*original, *task_ops_projections(task_sample))


def ops_status_source_result(sample: OpsSnapshot | TaskOpsSample) -> SourceReadResult:
    task_sample = (
        TaskOpsSample.model_validate(sample) if isinstance(sample, TaskOpsSample) else None
    )
    sample = OpsSnapshot.model_validate(sample if task_sample is None else task_sample.snapshot)
    values: dict[str, object] = {
        "dataset_id": "ops_status",
        "sequence": int(sample.sampled_at.timestamp() * 1_000_000),
        "event_time": sample.sampled_at,
        "published_at": sample.sampled_at,
        "status": FreshnessStatus.FRESH,
        "reason": None,
        "payload": OpsStatusPayload(
            snapshot=sample,
            task_evidence=None if task_sample is None else task_sample.evidence,
            projections=ops_status_projections(sample if task_sample is None else task_sample),
        ),
    }
    return SourceReadResult.model_validate({**values, "generation_id": canonical_sha256(values)})


def publish_ops_status_snapshot(
    sample: OpsSnapshot | TaskOpsSample,
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
    _require_collected_host_cpu(sample, collector)
    return publish_ops_status_snapshot(
        sample,
        root=authority_root,
        producer_commit=producer_commit,
        clock=clock,
    )


def read_previous_task_cpu(
    *, root: Path, producer_commit: str, as_of: datetime
) -> TaskCpuObservation | None:
    reader = ServingSourceAuthorityReader(
        root=root,
        expected_producer_commit=producer_commit,
        expected_dataset_id="ops_status",
        expected_payload_kind="ops_status",
        max_bytes=512 * 1024,
    )
    try:
        result = reader(as_of)
    except ServingSourceAuthorityUnavailableError:
        return None
    if not isinstance(result.payload, OpsStatusPayload) or result.payload.task_evidence is None:
        return None
    cpu = result.payload.task_evidence.cpu
    if cpu is None or (as_of - cpu.pair.current.observed_at).total_seconds() >= 120:
        return None
    return cpu.pair.current


def collect_and_publish_ops_tasks(
    *,
    manifest_path: Path,
    manifest_public_key_pem: bytes,
    authority_root: Path,
    producer_commit: str,
    collector: OpsStatusCollector,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    run_source: TaskUnitRunSource | None = None,
) -> ServingSourceAuthorityPointer:
    if type(collector) is not OpsStatusCollector:
        raise TypeError("task publisher requires the exact original ops collector")
    manifest, digest = load_signed_ops_manifest(
        manifest_path, public_key_pem=manifest_public_key_pem, expected_host=collector.host_name()
    )
    previous = read_previous_task_cpu(
        root=authority_root, producer_commit=producer_commit, as_of=clock()
    )
    sample = collector.collect_tasks(
        manifest, previous=previous, cpu_reader=LinuxTaskCpuReader(), run_source=run_source
    )
    if sample.snapshot.manifest_digest != digest:
        raise ValueError("ops sample manifest identity changed before publication")
    _require_collected_host_cpu(sample.snapshot, collector)
    return publish_ops_status_snapshot(
        sample, root=authority_root, producer_commit=producer_commit, clock=clock
    )


def _require_collected_host_cpu(sample: OpsSnapshot, collector: OpsStatusCollector) -> None:
    if sample.host_cpu is not None:
        if type(collector) is not OpsStatusCollector or not collector.observe_host_cpu:
            raise ValueError("host CPU evidence requires the original opt-in collector")
        if collector.host_name() != sample.host_name:
            raise ValueError("host CPU collector host changed before publication")


__all__ = [
    "collect_and_publish_ops_tasks",
    "read_previous_task_cpu",
    "collect_and_publish_ops_status",
    "ops_status_source_result",
    "ops_status_projections",
    "publish_ops_status_snapshot",
]


def ops_runtime_health_metrics(source: SourceReadResult) -> tuple[RuntimeHealthMetric, ...]:
    """Transfer original owner results and units; do not recompute kernel CPU."""
    from datetime import timedelta
    from decimal import Decimal

    from rquant.runtime_health_details import (
        RuntimeHealthHostScope,
        RuntimeHealthMetric,
        RuntimeHealthRealtimeValidity,
    )

    source = SourceReadResult.model_validate(source)
    if source.dataset_id != "ops_status" or not isinstance(source.payload, OpsStatusPayload):
        raise ValueError("health Ops facts require the original owner")
    sample = source.payload.snapshot
    if sample is None:
        return ()
    identity = canonical_sha256(source)
    rule_identity = canonical_sha256(
        {"owner": "ops_status", "max_age_seconds": 120, "boundary": "exclusive"}
    )
    facts: list[RuntimeHealthMetric] = []

    def add(
        metric_id: str,
        value: int | Decimal | None,
        *,
        slice_name: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        reason: str = "original_ops_measurement",
    ) -> None:
        facts.append(
            RuntimeHealthMetric(
                metric_id=metric_id,
                owner_dataset_id="ops_status",
                source_generation_id=source.generation_id,
                source_identity=identity,
                scope=RuntimeHealthHostScope(
                    host_name=sample.host_name,
                    boot_id=sample.boot_id,
                    kind="host" if slice_name is None else "slice",
                    slice_id=slice_name,
                ),
                event_time_start=start or sample.sampled_at,
                event_time_end=end or sample.sampled_at,
                available_at=sample.sampled_at,
                observed_at=sample.sampled_at,
                validity=RuntimeHealthRealtimeValidity(
                    rule_identity=rule_identity,
                    valid_until=(end or sample.sampled_at) + timedelta(seconds=120),
                    boundary="exclusive",
                ),
                unit="ratio" if metric_id.endswith("cpu") else "bytes",
                completeness="complete" if value is not None else "unavailable",
                value=value,
                verdict="unassessed" if value is not None else "unavailable",
                reason_code=reason,
            )
        )

    cpu = sample.host_cpu
    add(
        "host_cpu",
        None if cpu is None else cpu.busy_fraction,
        start=None if cpu is None or cpu.previous is None else cpu.previous.observed_at,
        end=None if cpu is None or cpu.current is None else cpu.current.observed_at,
        reason="host_cpu_disabled" if cpu is None else cpu.reason_code,
    )
    add("host_memory", sample.host_memory_available_bytes, reason="host_memory_available")
    task = source.payload.task_evidence
    groups = {} if task is None or task.cpu is None else {g.slice_name: g for g in task.cpu.groups}
    for resource in sample.resources:
        add("slice_memory", resource.memory_current_bytes, slice_name=resource.slice_name)
        group = groups.get(resource.slice_name)
        add(
            "slice_cpu",
            None
            if group is None or group.percent is None
            else Decimal(group.percent) / Decimal(100),
            slice_name=resource.slice_name,
            start=None
            if task is None or task.cpu is None or task.cpu.pair.previous is None
            else task.cpu.pair.previous.observed_at,
            end=None if task is None or task.cpu is None else task.cpu.pair.current.observed_at,
            reason="slice_cpu_unavailable"
            if group is None
            else group.reason or "original_task_cpu",
        )
    return tuple(facts)
