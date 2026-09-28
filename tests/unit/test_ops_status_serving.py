from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from rquant.ops_status import (
    STATIC_TIMER_STEMS,
    OpsInstallManifest,
    OpsResourceEvidence,
    OpsSnapshot,
    OpsStatusCollector,
    OpsUnitEvidence,
    OpsUnitInstall,
    SignedOpsInstallManifest,
)
from rquant.ops_status_serving import (
    collect_and_publish_ops_status,
    ops_status_source_result,
    publish_ops_status_snapshot,
)
from rquant.runtime_serving_authority import ServingSourceAuthorityReader
from rquant.runtime_serving_snapshot import OpsStatusPayload
from rquant.serving_contracts import FreshnessStatus
from rquant.serving_read_models import PAGE_PROJECTION_CONTRACTS

NOW = datetime(2026, 9, 28, 1, 0, tzinfo=UTC)
SLICES = (
    "rquant.slice",
    "rquant-live.slice",
    "rquant-serving.slice",
    "rquant-research.slice",
    "rquant-maintenance.slice",
)


def _sample() -> OpsSnapshot:
    return OpsSnapshot(
        sampled_at=NOW,
        host_name="rquant-test",
        boot_id="12345678-1234-1234-1234-123456789abc",
        manifest_digest="a" * 64,
        host_memory_total_bytes=10_000,
        host_memory_available_bytes=4_000,
        units=tuple(
            OpsUnitEvidence(
                timer=f"rquant-{stem}.timer",
                service=f"rquant-{stem}.service",
                label="每日选股" if stem == "daily" else "定时任务",
                expected_enabled=True,
                session="trading_day",
                resource_group="maintenance",
                timer_load_state="loaded",
                timer_unit_file_state="enabled",
                timer_active_state="active",
                timer_sub_state="waiting",
                service_result="success" if stem == "daily" else None,
            )
            for stem in STATIC_TIMER_STEMS
        ),
        resources=tuple(
            OpsResourceEvidence(
                slice_name=name,
                memory_current_bytes=1000 if name == "rquant.slice" else 100,
                memory_peak_bytes=2000 if name == "rquant.slice" else 200,
            )
            for name in SLICES
        ),
    )


def test_ops_source_projection_preserves_current_vs_peak_and_unknown_result() -> None:
    result = ops_status_source_result(_sample())
    assert result.dataset_id == "ops_status"
    assert result.status is FreshnessStatus.FRESH
    assert isinstance(result.payload, OpsStatusPayload)
    projections = {item.table_name: item for item in result.payload.projections}
    assert set(projections) == {"ops_host_status", "ops_unit_status", "ops_resource_status"}
    assert projections["ops_unit_status"].rows[0]["last_result"] is None
    assert projections["ops_host_status"].rows[0]["memory_total_bytes"] == 10_000
    assert projections["ops_resource_status"].rows[0]["memory_current_bytes"] == 1000
    assert projections["ops_resource_status"].rows[0]["memory_peak_bytes"] == 2000
    assert all(
        PAGE_PROJECTION_CONTRACTS[name].owner_dataset_id == "ops_status" for name in projections
    )


def test_ops_source_atomic_publication_and_reader_rejects_old_cutoff(tmp_path: Path) -> None:
    root = tmp_path / "ops-authority"
    publish_ops_status_snapshot(
        _sample(),
        root=root,
        producer_commit="b" * 40,
        clock=lambda: NOW + timedelta(seconds=1),
    )
    reader = ServingSourceAuthorityReader(
        root=root,
        expected_producer_commit="b" * 40,
        expected_dataset_id="ops_status",
        expected_payload_kind="ops_status",
    )
    assert reader(NOW + timedelta(seconds=1)).payload.snapshot == _sample()
    assert (root / "current.json").is_file()


def test_invalid_install_signature_never_starts_systemctl_or_publishes(tmp_path: Path) -> None:
    manifest = OpsInstallManifest(
        version=1,
        host_name="rquant-test",
        units=tuple(
            OpsUnitInstall(
                timer=f"rquant-{stem}.timer",
                service=f"rquant-{stem}.service",
                label="定时任务",
                expected_enabled=False,
                session="all",
                resource_group="maintenance",
            )
            for stem in STATIC_TIMER_STEMS
        ),
    )
    path = tmp_path / "manifest.json"
    unsigned = SignedOpsInstallManifest(manifest=manifest, signature="A" * 88)
    path.write_bytes(unsigned.canonical_bytes())
    collector = OpsStatusCollector(
        command_runner=lambda *_args: pytest.fail("systemctl must not run"),
        host_name=lambda: "rquant-test",
    )

    with pytest.raises(ValueError, match="signature"):
        collect_and_publish_ops_status(
            manifest_path=path,
            manifest_public_key_pem=b"invalid-public-key",
            authority_root=tmp_path / "ops-authority",
            producer_commit="b" * 40,
            collector=collector,
        )
    assert not (tmp_path / "ops-authority").exists()
