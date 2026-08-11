from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import rquant.runtime_health_authority as health_module
from rquant.runtime_health_authority import (
    RuntimeHealthAuthorityIntegrityError,
    RuntimeHealthAuthorityPublisher,
    RuntimeHealthControlSource,
    RuntimeHealthSourceReader,
)
from rquant.runtime_service_control import (
    RuntimeServiceControl,
    RuntimeServicePlane,
    RuntimeServiceSpec,
    RuntimeServiceStatus,
    RuntimeStepResult,
)
from rquant.runtime_serving_authority import (
    ServingSourceAuthorityPublisher,
    ServingSourceAuthorityReader,
)
from rquant.runtime_serving_snapshot import RUNTIME_HEALTH_DATASET_ID, RuntimeHealthPayload
from rquant.serving_contracts import FreshnessStatus

NOW = datetime(2026, 8, 1, 3, 0, tzinfo=UTC)
COMMIT = "a" * 40


def _spec(
    service_id: str,
    *,
    plane: RuntimeServicePlane = RuntimeServicePlane.LIVE,
    stale_after: timedelta = timedelta(seconds=10),
) -> RuntimeServiceSpec:
    return RuntimeServiceSpec(
        service_id=service_id,
        plane=plane,
        stale_after=stale_after,
        producer_commit=COMMIT,
    )


def _source(root: Path, service_id: str) -> RuntimeHealthControlSource:
    return RuntimeHealthControlSource(control_root=root, spec=_spec(service_id))


def _running(root: Path, service_id: str, at: datetime = NOW) -> None:
    control = RuntimeServiceControl(root, spec=_spec(service_id), clock=lambda: at)
    control.start()
    control.record_success(
        RuntimeStepResult(input_sequence=2, output_sequence=1),
        duration_seconds=0.25,
    )
    control.stop(reason="fixture complete")
    path = RuntimeServiceControl._path_for(root, _spec(service_id))
    heartbeat = RuntimeServiceControl.read_heartbeat(root, _spec(service_id))
    assert heartbeat is not None
    path.write_text(
        heartbeat.model_copy(
            update={"status": RuntimeServiceStatus.RUNNING, "stopped_at": None, "stop_reason": None}
        ).model_dump_json(),
        encoding="utf-8",
    )


def test_reader_builds_canonical_fresh_health_result_in_stable_order(tmp_path: Path) -> None:
    feature_root = tmp_path / "control" / "features" / "feature"
    router_root = tmp_path / "control" / "signal-routers" / "router"
    _running(feature_root, "feature")
    _running(router_root, "router")

    result = RuntimeHealthSourceReader(
        sources=(_source(router_root, "router"), _source(feature_root, "feature")),
        serving_service_id="serving",
    )(NOW)

    assert result.dataset_id == RUNTIME_HEALTH_DATASET_ID
    assert result.status is FreshnessStatus.FRESH
    assert result.reason is None
    assert result.event_time == NOW
    assert result.published_at == NOW
    assert result.sequence == int(NOW.timestamp() * 1_000_000)
    assert isinstance(result.payload, RuntimeHealthPayload)
    assert tuple(item.service_id for item in result.payload.runtime_services) == (
        "feature",
        "router",
    )
    assert all(
        item.status is RuntimeServiceStatus.RUNNING for item in result.payload.runtime_services
    )
    assert all(not item.stale for item in result.payload.runtime_services)
    assert result.payload.live_backlog_age_seconds == 0.0
    assert result.payload.live_p95_latency_seconds == 0.25
    assert result.payload.live_healthy is True
    assert result.payload.dashboard_summary_observed_at == NOW
    assert result.payload.dashboard_summary_generation_id is not None
    assert set(result.payload.dashboard_summary_source_receipts) == {"feature", "router"}
    assert tuple(item.table_name for item in result.payload.projections) == ("dashboard_summary",)
    dashboard = result.payload.projections[0].rows[0]
    assert dashboard["snapshot_key"] == "current"
    assert dashboard["host_name"]
    assert dashboard["monitor_state"] == "unavailable"

    repeated = RuntimeHealthSourceReader(
        sources=(_source(feature_root, "feature"), _source(router_root, "router")),
        serving_service_id="serving",
    )(NOW)
    assert repeated == result


def test_missing_and_expired_services_map_to_explicit_degraded_health(tmp_path: Path) -> None:
    feature_root = tmp_path / "control" / "features" / "feature"
    missing_root = tmp_path / "control" / "notifiers" / "missing"
    _running(feature_root, "feature", NOW - timedelta(seconds=11))

    result = RuntimeHealthSourceReader(
        sources=(_source(missing_root, "notifier"), _source(feature_root, "feature")),
        serving_service_id="serving",
    )(NOW)

    assert result.status is FreshnessStatus.DEGRADED
    assert result.reason == "missing:notifier,stale:feature"
    assert isinstance(result.payload, RuntimeHealthPayload)
    feature, notifier = result.payload.runtime_services
    assert feature.status is RuntimeServiceStatus.DEGRADED
    assert feature.stale is True
    assert feature.heartbeat is not None
    assert notifier.status is RuntimeServiceStatus.MISSING
    assert notifier.stale is True
    assert notifier.heartbeat is None
    assert result.payload.live_healthy is False


def test_reader_excludes_current_serving_service_instead_of_self_certifying(tmp_path: Path) -> None:
    feature_root = tmp_path / "control" / "features" / "feature"
    serving_root = tmp_path / "control" / "serving-publishers" / "serving"
    _running(feature_root, "feature")
    serving_root.parent.mkdir(parents=True)
    serving_root.symlink_to(tmp_path / "untrusted-serving-control")

    result = RuntimeHealthSourceReader(
        sources=(
            _source(serving_root, "serving"),
            _source(feature_root, "feature"),
        ),
        serving_service_id="serving",
    )(NOW)

    assert isinstance(result.payload, RuntimeHealthPayload)
    assert tuple(item.service_id for item in result.payload.runtime_services) == ("feature",)
    assert result.status is FreshnessStatus.FRESH


def test_reader_rejects_future_heartbeat_evidence(tmp_path: Path) -> None:
    root = tmp_path / "control" / "features" / "feature"
    _running(root, "feature", NOW + timedelta(microseconds=1))

    with pytest.raises(RuntimeHealthAuthorityIntegrityError, match="future evidence"):
        RuntimeHealthSourceReader(
            sources=(_source(root, "feature"),),
            serving_service_id="serving",
        )(NOW)


def test_reader_rejects_corrupt_and_symlinked_heartbeat(tmp_path: Path) -> None:
    corrupt_root = tmp_path / "control" / "features" / "corrupt"
    path = RuntimeServiceControl._path_for(corrupt_root, _spec("corrupt"))
    path.parent.mkdir(parents=True)
    path.write_text("{not-json", encoding="utf-8")
    path.chmod(0o600)

    with pytest.raises(RuntimeHealthAuthorityIntegrityError, match="invalid"):
        RuntimeHealthSourceReader(
            sources=(_source(corrupt_root, "corrupt"),),
            serving_service_id="serving",
        )(NOW)

    real_root = tmp_path / "control" / "features" / "real"
    linked_root = tmp_path / "control" / "features" / "linked"
    _running(real_root, "linked")
    linked_root.symlink_to(real_root)
    with pytest.raises(RuntimeHealthAuthorityIntegrityError, match="symlink|unsafe"):
        RuntimeHealthSourceReader(
            sources=(_source(linked_root, "linked"),),
            serving_service_id="serving",
        )(NOW)

    heartbeat_link_root = tmp_path / "control" / "features" / "heartbeat-link"
    heartbeat_path = RuntimeServiceControl._path_for(
        heartbeat_link_root,
        _spec("heartbeat-link"),
    )
    heartbeat_path.parent.mkdir(parents=True)
    heartbeat_path.symlink_to(RuntimeServiceControl._path_for(real_root, _spec("linked")))
    with pytest.raises(RuntimeHealthAuthorityIntegrityError, match="symlink|unsafe"):
        RuntimeHealthSourceReader(
            sources=(_source(heartbeat_link_root, "heartbeat-link"),),
            serving_service_id="serving",
        )(NOW)


def test_reader_rejects_heartbeat_replaced_during_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "control" / "features" / "feature"
    _running(root, "feature")
    path = RuntimeServiceControl._path_for(root, _spec("feature"))
    original_read = health_module._read_descriptor_bytes
    replaced = False

    def replace_after_read(descriptor: int, *, max_bytes: int) -> bytes:
        nonlocal replaced
        payload = original_read(descriptor, max_bytes=max_bytes)
        if not replaced:
            replacement = path.with_name("replacement.json")
            replacement.write_bytes(payload)
            replacement.chmod(0o600)
            os.replace(replacement, path)
            replaced = True
        return payload

    monkeypatch.setattr(health_module, "_read_descriptor_bytes", replace_after_read)

    with pytest.raises(RuntimeHealthAuthorityIntegrityError, match="changed during read"):
        RuntimeHealthSourceReader(
            sources=(_source(root, "feature"),),
            serving_service_id="serving",
        )(NOW)


def test_reader_rejects_ambiguous_sources_and_relative_roots(tmp_path: Path) -> None:
    source = _source(tmp_path / "control" / "feature", "feature")
    with pytest.raises(ValueError, match="unique service ids"):
        RuntimeHealthSourceReader(
            sources=(source, source),
            serving_service_id="serving",
        )
    with pytest.raises(ValueError, match="exclusive control roots"):
        RuntimeHealthSourceReader(
            sources=(
                source,
                RuntimeHealthControlSource(
                    control_root=source.control_root,
                    spec=_spec("router"),
                ),
            ),
            serving_service_id="serving",
        )
    with pytest.raises(ValueError, match="absolute"):
        RuntimeHealthControlSource(control_root=Path("relative"), spec=_spec("feature"))


def test_runtime_health_publisher_uses_existing_serving_authority(tmp_path: Path) -> None:
    control_root = tmp_path / "control" / "features" / "feature"
    authority_root = tmp_path / "authority"
    _running(control_root, "feature")
    source_reader = RuntimeHealthSourceReader(
        sources=(_source(control_root, "feature"),),
        serving_service_id="serving",
    )
    generic_publisher = ServingSourceAuthorityPublisher(
        root=authority_root,
        producer_commit=COMMIT,
        dataset_id=RUNTIME_HEALTH_DATASET_ID,
        payload_kind="runtime_health",
        clock=lambda: NOW,
    )

    pointer = RuntimeHealthAuthorityPublisher(
        reader=source_reader,
        publisher=generic_publisher,
    ).publish(NOW)
    loaded = ServingSourceAuthorityReader(
        root=authority_root,
        expected_producer_commit=COMMIT,
        expected_dataset_id=RUNTIME_HEALTH_DATASET_ID,
        expected_payload_kind="runtime_health",
    )(NOW)

    assert pointer.generation_id == loaded.generation_id
    assert loaded == source_reader(NOW)
