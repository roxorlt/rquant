from __future__ import annotations

import base64
import shutil
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

from rquant.ops_status import (
    STATIC_TIMER_STEMS,
    OpsInstallManifest,
    OpsResourceEvidence,
    OpsSnapshot,
    OpsStatusCollector,
    OpsUnitInstall,
    SignedOpsInstallManifest,
    _run_bounded,
    load_signed_ops_manifest,
    verify_ops_manifest,
)

NOW = datetime(2026, 9, 28, 1, 0, tzinfo=UTC)
BOOT = b"12345678-1234-1234-1234-123456789abc\n"
MEMINFO = b"MemTotal:       10485760 kB\nMemAvailable:    5242880 kB\n"


def _manifest(*, extra: tuple[OpsUnitInstall, ...] = ()) -> OpsInstallManifest:
    return OpsInstallManifest(
        version=1,
        host_name="rquant-test",
        units=tuple(
            OpsUnitInstall(
                timer=f"rquant-{stem}.timer",
                service=f"rquant-{stem}.service",
                label=f"任务 {stem}",
                expected_enabled=True,
                session="all",
                resource_group="maintenance",
            )
            for stem in STATIC_TIMER_STEMS
        )
        + extra,
    )


def _signed(manifest: OpsInstallManifest, tmp_path: Path) -> tuple[SignedOpsInstallManifest, bytes]:
    openssl = shutil.which("openssl")
    if openssl is None:
        pytest.skip("openssl is required for Ed25519 manifest tests")
    private = tmp_path / "private.pem"
    public = tmp_path / "public.pem"
    payload = tmp_path / "manifest.payload"
    signature = tmp_path / "manifest.signature"
    subprocess.run(
        (openssl, "genpkey", "-algorithm", "ED25519", "-out", str(private)),
        check=True,
        capture_output=True,
    )
    subprocess.run(
        (openssl, "pkey", "-in", str(private), "-pubout", "-out", str(public)),
        check=True,
        capture_output=True,
    )
    payload.write_bytes(manifest.signing_bytes())
    subprocess.run(
        (
            openssl, "pkeyutl", "-sign", "-inkey", str(private), "-rawin",
            "-in", str(payload), "-out", str(signature),
        ),
        check=True,
        capture_output=True,
    )
    signed = SignedOpsInstallManifest(
        manifest=manifest,
        signature=base64.b64encode(signature.read_bytes()).decode("ascii"),
    )
    return signed, public.read_bytes()


def test_signed_manifest_requires_exact_static_set_and_rejects_tampering(tmp_path: Path) -> None:
    signed, public = _signed(_manifest(), tmp_path)
    path = tmp_path / "manifest.json"
    path.write_bytes(signed.canonical_bytes())

    loaded, digest = load_signed_ops_manifest(
        path, public_key_pem=public, expected_host="rquant-test"
    )
    assert loaded == signed.manifest
    assert digest == loaded.digest

    tampered = SignedOpsInstallManifest(
        manifest=_manifest().model_copy(update={"host_name": "other-host"}),
        signature=signed.signature,
    )
    with pytest.raises(ValueError, match="signature"):
        verify_ops_manifest(tampered, public_key_pem=public, expected_host="other-host")
    with pytest.raises(ValueError, match="host"):
        verify_ops_manifest(signed, public_key_pem=public, expected_host="other-host")
    with pytest.raises(ValueError, match="exact static"):
        OpsInstallManifest.model_validate(
            _manifest().model_dump() | {"units": _manifest().units[:-1]}
        )


def test_manifest_rejects_unapproved_or_malformed_template_instances() -> None:
    with pytest.raises(ValueError, match="allowlist"):
        _manifest(
            extra=(
                OpsUnitInstall(
                    timer="ssh.timer",
                    service="ssh.service",
                    label="其他任务",
                    expected_enabled=True,
                    session="all",
                    resource_group="maintenance",
                ),
            )
        )
    with pytest.raises(ValueError, match="allowlist"):
        _manifest(
            extra=(
                OpsUnitInstall(
                    timer="rquant-runtime-daily-orchestrator@bad;id.timer",
                    service="rquant-runtime-daily-orchestrator@bad;id.service",
                    label="编排",
                    expected_enabled=True,
                    session="all",
                    resource_group="maintenance",
                ),
            )
        )


def test_snapshot_cannot_publish_a_cherry_picked_static_subset() -> None:
    with pytest.raises(ValueError, match="exact static"):
        OpsSnapshot(
            sampled_at=NOW,
            host_name="rquant-test",
            boot_id=BOOT.decode().strip(),
            manifest_digest="a" * 64,
            units=(),
            resources=tuple(
                OpsResourceEvidence(slice_name=name)
                for name in (
                    "rquant.slice",
                    "rquant-live.slice",
                    "rquant-serving.slice",
                    "rquant-research.slice",
                    "rquant-maintenance.slice",
                )
            ),
        )


def test_collector_uses_only_manifest_units_and_fixed_resource_paths() -> None:
    manifest = _manifest()
    calls: list[tuple[str, ...]] = []
    files: list[str] = []

    def command(argv: tuple[str, ...], timeout_seconds: float, max_bytes: int) -> bytes:
        calls.append(argv)
        assert 0 < timeout_seconds <= 1
        assert max_bytes <= 4096
        if argv[2].endswith(".timer"):
            return (
                b"LoadState=loaded\nUnitFileState=enabled\nActiveState=active\n"
                b"SubState=waiting\nLastTriggerUSec=Mon 2026-09-28 00:59:00 UTC\n"
                b"NextElapseUSecRealtime=Mon 2026-09-28 01:00:00 UTC\n"
            )
        if argv[2].endswith(".service"):
            return b"LoadState=loaded\nActiveState=inactive\nResult=success\n"
        return b"LoadState=loaded\nActiveState=active\nMemoryCurrent=1048576\nMemoryPeak=2097152\n"

    def proc(path: str, max_bytes: int) -> bytes:
        files.append(path)
        return BOOT if path.endswith("boot_id") else MEMINFO

    collector = OpsStatusCollector(
        command_runner=command,
        proc_reader=proc,
        clock=lambda: NOW,
        monotonic=lambda: 1.0,
        host_name=lambda: "rquant-test",
    )
    sample = collector.collect(manifest)

    assert sample.manifest_digest == manifest.digest
    assert sample.boot_id == BOOT.decode().strip()
    assert len(sample.units) == 14
    assert sample.units[0].last_result is None  # service Result alone is not timer attribution
    assert sample.units[0].last_trigger_at == datetime(2026, 9, 28, 0, 59, tzinfo=UTC)
    assert sample.host_memory_total_bytes == 10485760 * 1024
    assert sample.host_memory_available_bytes == 5242880 * 1024
    assert len(calls) == 33  # 14 timer+service pairs, parent plus four child slices
    assert {argv[2] for argv in calls} == {
        *(unit.timer for unit in manifest.units),
        *(unit.service for unit in manifest.units),
        "rquant.slice",
        "rquant-live.slice",
        "rquant-serving.slice",
        "rquant-research.slice",
        "rquant-maintenance.slice",
    }
    assert files == [
        "/proc/sys/kernel/random/boot_id",
        "/proc/meminfo",
        "/proc/sys/kernel/random/boot_id",
    ]


def test_collector_refuses_partial_publication_when_budget_or_boot_changes() -> None:
    manifest = _manifest()
    calls = 0

    def command(argv: tuple[str, ...], timeout_seconds: float, max_bytes: int) -> bytes:
        nonlocal calls
        calls += 1
        return b"x" * (max_bytes + 1)

    collector = OpsStatusCollector(
        command_runner=command,
        proc_reader=lambda path, _limit: BOOT if path.endswith("boot_id") else MEMINFO,
        clock=lambda: NOW,
        monotonic=lambda: 1.0,
        host_name=lambda: "rquant-test",
    )
    with pytest.raises(ValueError, match="byte budget"):
        collector.collect(manifest)
    assert calls == 1

    boot_reads = iter((BOOT, b"other-boot\n"))
    collector = OpsStatusCollector(
        command_runner=lambda *_args: b"LoadState=loaded\n",
        proc_reader=lambda path, _limit: (
            next(boot_reads) if path.endswith("boot_id") else MEMINFO
        ),
        clock=lambda: NOW,
        monotonic=lambda: 1.0,
        host_name=lambda: "rquant-test",
    )
    with pytest.raises(ValueError, match="boot"):
        collector.collect(manifest)


def test_collector_refuses_expired_total_budget() -> None:
    counter = iter((1.0, 22.0))
    collector = OpsStatusCollector(
        command_runner=lambda *_args: b"LoadState=loaded\n",
        proc_reader=lambda path, _limit: BOOT if path.endswith("boot_id") else MEMINFO,
        clock=lambda: NOW,
        monotonic=lambda: next(counter),
        host_name=lambda: "rquant-test",
    )
    with pytest.raises(TimeoutError, match="total"):
        collector.collect(_manifest())


def test_subprocess_runner_enforces_output_and_time_budgets() -> None:
    with pytest.raises(ValueError, match="byte budget"):
        _run_bounded((sys.executable, "-c", "print('x' * 10000)"), 1.0, 512)
    with pytest.raises(TimeoutError, match="timed out"):
        _run_bounded((sys.executable, "-c", "import time; time.sleep(1)"), 0.05, 512)
