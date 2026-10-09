"""Synthetic aggregate rules; no Linux installation or live sampling evidence."""

from __future__ import annotations

import base64
import io
import json
import shutil
import subprocess
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from rquant import ops_status as ops
from rquant.ops_status_serving import (
    collect_and_publish_ops_status,
    ops_status_source_result,
    publish_ops_status_snapshot,
)
from rquant.runtime_serving_authority import ServingSourceAuthorityReader
from rquant.strict_json import canonical_json_bytes

AT = datetime(2026, 10, 6, 3, 0, tzinfo=UTC)
BOOT = b"12345678-1234-1234-1234-123456789abc\n"
FIRST = b"cpu 100 20 50 300 10 5 5 0 30 5\ncpu0 1 1 1 1 1 1 1 1\n"
LAST = b"cpu 130 30 70 340 20 10 10 10 60 10\ncpu0 9 9 9 9 9 9 9 9\n"


def manifest() -> ops.OpsInstallManifest:
    return ops.OpsInstallManifest(
        version=1,
        host_name="fixture-host",
        units=tuple(
            ops.OpsUnitInstall(
                timer=f"rquant-{stem}.timer",
                service=f"rquant-{stem}.service",
                label="job",
                expected_enabled=False,
                session="all",
                resource_group="maintenance",
            )
            for stem in ops.STATIC_TIMER_STEMS
        ),
    )


def collector(
    *,
    enabled: bool | None = True,
    first: bytes | OSError = FIRST,
    last: bytes | OSError = LAST,
    boot_changes: bool = False,
) -> tuple[ops.OpsStatusCollector, list[tuple[str, int]]]:
    reads: list[tuple[str, int]] = []
    stat_values = iter((first, last))
    boots = iter((BOOT, b"87654321-1234-1234-1234-123456789abc\n" if boot_changes else BOOT))
    clocks = iter((AT, AT + timedelta(seconds=1), AT + timedelta(seconds=1)))
    tick = 0.0

    def monotonic() -> float:
        nonlocal tick
        tick += 0.01
        return tick

    def read(path: str, limit: int) -> bytes:
        reads.append((path, limit))
        if path == "/proc/stat":
            value = next(stat_values)
            if isinstance(value, OSError):
                raise value
            return value
        if path.endswith("boot_id"):
            return next(boots)
        assert path == "/proc/meminfo"
        return b"MemTotal: 1000 kB\nMemAvailable: 500 kB\n"

    options = {} if enabled is None else {"observe_host_cpu": enabled}
    return ops.OpsStatusCollector(
        command_runner=lambda *_args: b"LoadState=loaded\n",
        proc_reader=read,
        clock=lambda: next(clocks),
        monotonic=monotonic,
        host_name=lambda: "fixture-host",
        **options,
    ), reads


def test_original_collect_measures_aggregate_same_window_and_guest_is_not_added() -> None:
    owner, reads = collector()
    sample = owner.collect(manifest())
    cpu = sample.host_cpu
    assert cpu.availability == "available"
    assert cpu.total_delta == 130
    assert cpu.busy_delta == 80
    assert cpu.busy_fraction == Decimal(8) / Decimal(13)
    assert cpu.previous.counters.guest == 30
    assert cpu.current.counters.guest == 60
    assert cpu.previous.observed_at == AT
    assert cpu.current.observed_at == AT + timedelta(seconds=1)
    assert cpu.previous.monotonic_seconds < cpu.current.monotonic_seconds
    assert cpu.previous.boot_id == cpu.current.boot_id == sample.boot_id
    assert cpu.previous.manifest_digest == cpu.current.manifest_digest == manifest().digest
    assert reads == [
        ("/proc/sys/kernel/random/boot_id", 128),
        ("/proc/stat", 32768),
        ("/proc/meminfo", 131072),
        ("/proc/stat", 32768),
        ("/proc/sys/kernel/random/boot_id", 128),
    ]


@pytest.mark.parametrize("enabled", [None, False])
def test_default_off_keeps_exact_old_snapshot_and_projection_material(enabled: bool | None) -> None:
    owner, reads = collector(enabled=enabled)
    sample = owner.collect(manifest())
    assert "/proc/stat" not in {path for path, _limit in reads}
    assert "host_cpu" not in sample.model_dump(mode="json")
    result = ops_status_source_result(sample)
    assert "host_cpu" not in result.model_dump(mode="json")["payload"]["snapshot"]
    assert set(p.table_name for p in result.payload.projections) == {
        "ops_host_status",
        "ops_unit_status",
        "ops_resource_status",
    }
    # Frozen from the accepted 7cd0521 owner in red-host-02, before product edits.
    assert (
        result.generation_id == "9a271bcc15688b342b9665d346fc882db77a0b2d6463c7ad22bf4714200dc6f3"
    )


@pytest.mark.parametrize(
    "first,last,reason",
    [
        (OSError("unavailable"), LAST, "capture_unavailable"),
        (FIRST, OSError("unavailable"), "capture_unavailable"),
        (FIRST, FIRST, "no_counter_delta"),
        (FIRST, b"cpu 130 30 70 340 9 10 10 10 60 10\n", "counter_regression"),
        (FIRST, b"cpu 99 30 70 340 20 10 10 10 60 10\n", "counter_regression"),
        (FIRST, b"cpu 1 2\n", "capture_unavailable"),
        (FIRST, LAST + b"cpu 1 2 3 4 5 6 7 8\n", "capture_unavailable"),
        (FIRST, b"cpu 1 2 3 4 5 6 7 -8\n", "capture_unavailable"),
        (FIRST, LAST + b"x" * 32768, "capture_unavailable"),
    ],
    ids=[
        "first-unavailable",
        "last-unavailable",
        "no-counter-delta",
        "iowait-regression",
        "user-regression",
        "missing-counters",
        "duplicate-cpu-row",
        "negative-counter",
        "oversized-capture",
    ],
)
def test_unavailable_pair_keeps_unknown_and_preserves_other_ops_facts(
    first: bytes | OSError, last: bytes | OSError, reason: str
) -> None:
    owner, _reads = collector(first=first, last=last)
    sample = owner.collect(manifest())
    assert sample.host_cpu.availability == "unavailable"
    assert sample.host_cpu.reason_code == reason
    assert sample.host_cpu.busy_fraction is None
    assert sample.host_cpu.total_delta is None
    assert sample.host_memory_total_bytes == 1000 * 1024
    assert len(sample.resources) == 5


def test_actual_zero_busy_with_a_positive_aggregate_delta_is_zero() -> None:
    owner, _reads = collector(last=b"cpu 100 20 50 320 10 5 5 0 30 5\n")
    sample = owner.collect(manifest())
    assert sample.host_cpu.availability == "available"
    assert sample.host_cpu.busy_fraction == Decimal(0)
    assert sample.host_cpu.total_delta == 20


def test_boot_change_keeps_original_atomic_failure() -> None:
    owner, _reads = collector(boot_changes=True)
    with pytest.raises(ValueError, match="boot"):
        owner.collect(manifest())


@pytest.mark.parametrize(
    "field,value",
    [
        ("total_delta", 131),
        ("busy_delta", 81),
        ("busy_fraction", "0.9"),
        ("availability", "unavailable"),
    ],
)
def test_bad_present_result_rejects_instead_of_recalculating_success(
    field: str, value: object
) -> None:
    owner, _reads = collector()
    raw = owner.collect(manifest()).model_dump(mode="json")
    raw["host_cpu"][field] = value
    with pytest.raises(ValueError, match="CPU|cpu"):
        ops.OpsSnapshot.model_validate(raw)


@pytest.mark.parametrize("change", ["host", "boot", "install", "monotonic", "utc", "future"])
def test_present_pair_cannot_detach_identity_or_window(change: str) -> None:
    owner, _reads = collector()
    raw = owner.collect(manifest()).model_dump(mode="json")
    if change in {"host", "boot", "install"}:
        field = {"host": "host_name", "boot": "boot_id", "install": "manifest_digest"}[change]
        raw["host_cpu"]["current"][field] = {
            "host": "other-host",
            "boot": "87654321-1234-1234-1234-123456789abc",
            "install": "f" * 64,
        }[change]
    elif change == "future":
        raw["host_cpu"]["current"]["observed_at"] = (AT + timedelta(seconds=2)).isoformat()
    else:
        field = "monotonic_seconds" if change == "monotonic" else "observed_at"
        raw["host_cpu"]["current"][field] = raw["host_cpu"]["previous"][field]
    with pytest.raises(ValueError, match="CPU|cpu"):
        ops.OpsSnapshot.model_validate(raw)


def signed_install(path: Path) -> bytes:
    openssl = shutil.which("openssl")
    assert openssl is not None, "existing Ed25519 test tool is required"
    private, public, body, signature = (
        path / name for name in ("private.pem", "public.pem", "body", "signature")
    )
    subprocess.run(
        [openssl, "genpkey", "-algorithm", "ED25519", "-out", str(private)],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        [openssl, "pkey", "-in", str(private), "-pubout", "-out", str(public)],
        check=True,
        capture_output=True,
    )
    body.write_bytes(manifest().signing_bytes())
    subprocess.run(
        [
            openssl,
            "pkeyutl",
            "-sign",
            "-inkey",
            str(private),
            "-rawin",
            "-in",
            str(body),
            "-out",
            str(signature),
        ],
        check=True,
        capture_output=True,
    )
    signed = ops.SignedOpsInstallManifest(
        manifest=manifest(), signature=base64.b64encode(signature.read_bytes()).decode()
    )
    (path / "install.json").write_bytes(signed.canonical_bytes())
    return public.read_bytes()


def test_true_signed_path_carries_pair_in_original_authority_without_projection_changes(
    tmp_path: Path,
) -> None:
    key = signed_install(tmp_path)
    owner, _reads = collector()
    authority = tmp_path / "authority"
    pointer = collect_and_publish_ops_status(
        manifest_path=tmp_path / "install.json",
        manifest_public_key_pem=key,
        authority_root=authority,
        producer_commit="a" * 40,
        collector=owner,
        clock=lambda: AT + timedelta(seconds=2),
    )
    reader = ServingSourceAuthorityReader(
        root=authority,
        expected_producer_commit="a" * 40,
        expected_dataset_id="ops_status",
        expected_payload_kind="ops_status",
        max_bytes=512 * 1024,
    )
    result = reader(AT + timedelta(seconds=2))
    assert pointer.generation_id == result.generation_id
    assert result.payload.snapshot.host_cpu.busy_fraction == Decimal(8) / Decimal(13)
    assert {p.table_name for p in result.payload.projections} == {
        "ops_host_status",
        "ops_unit_status",
        "ops_resource_status",
    }
    assert "host_cpu" not in result.payload.projections[0].rows[0]
    assert all(path.stat().st_size <= 512 * 1024 for path in authority.rglob("*.json"))


def test_invalid_signed_install_never_samples_even_with_opt_in(tmp_path: Path) -> None:
    (tmp_path / "install.json").write_bytes(
        ops.SignedOpsInstallManifest(manifest=manifest(), signature="A" * 88).canonical_bytes()
    )
    owner = ops.OpsStatusCollector(
        observe_host_cpu=True,
        host_name=lambda: "fixture-host",
        command_runner=lambda *_args: pytest.fail("no command"),
        proc_reader=lambda *_args: pytest.fail("no proc read"),
    )
    with pytest.raises(ValueError, match="signature"):
        collect_and_publish_ops_status(
            manifest_path=tmp_path / "install.json",
            manifest_public_key_pem=b"invalid",
            authority_root=tmp_path / "authority",
            producer_commit="a" * 40,
            collector=owner,
        )
    assert not (tmp_path / "authority").exists()


def test_original_ops_document_cap_is_not_raised(tmp_path: Path) -> None:
    owner, _reads = collector()
    raw = owner.collect(manifest()).model_dump(mode="json")
    for unit in raw["units"]:
        unit["label"] = "x" * 50000
    with pytest.raises(ValueError, match="byte budget"):
        publish_ops_status_snapshot(
            ops.OpsSnapshot.model_validate(raw),
            root=tmp_path / "authority",
            producer_commit="a" * 40,
            clock=lambda: AT + timedelta(seconds=2),
        )


def test_raw_cpu_pair_has_the_frozen_small_budget() -> None:
    owner, _reads = collector()
    cpu = owner.collect(manifest()).host_cpu
    raw = {
        "previous": cpu.previous.model_dump(mode="json"),
        "current": cpu.current.model_dump(mode="json"),
    }
    assert len(canonical_json_bytes(raw)) <= 4096
    assert len(json.dumps(raw).encode()) < 4096


@pytest.mark.parametrize("clock_error", ["monotonic", "utc"])
def test_actual_unordered_collection_window_is_unavailable(clock_error: str) -> None:
    owner, _reads = collector()
    if clock_error == "monotonic":
        owner.monotonic = lambda: 1.0
    else:
        times = iter((AT, AT - timedelta(seconds=1), AT))
        owner.clock = lambda: next(times)
    assert owner.collect(manifest()).host_cpu.reason_code == "invalid_window"


def test_host_opt_in_keeps_original_twenty_second_atomic_deadline() -> None:
    owner, reads = collector()
    elapsed = [0.0]
    owner.monotonic = lambda: elapsed[0]

    def slow_command(*_args: object) -> bytes:
        elapsed[0] = 20.0
        return b"LoadState=loaded\n"

    owner.command_runner = slow_command
    with pytest.raises(TimeoutError, match="total"):
        owner.collect(manifest())
    assert sum(path == "/proc/stat" for path, _limit in reads) == 1


def test_fixed_proc_stat_reader_enforces_allowlist_and_bound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("builtins.open", lambda *_args: io.BytesIO(FIRST))
    assert ops._bounded_proc_read("/proc/stat", 32768) == FIRST
    with pytest.raises(ValueError, match="allowlist"):
        ops._bounded_proc_read("/unapproved", 32768)
    monkeypatch.setattr("builtins.open", lambda *_args: io.BytesIO(b"x" * 32769))
    with pytest.raises(ValueError, match="byte budget"):
        ops._bounded_proc_read("/proc/stat", 32768)
