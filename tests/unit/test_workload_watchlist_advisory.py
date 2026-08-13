from __future__ import annotations

import hashlib
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from rquant.runtime_deployment_preflight import RuntimeDeploymentInspection
from rquant.workload_isolation import WorkloadCheck


def test_run_all_checks_passes_receipt_bound_quote_advisory_to_real_workload_probe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import preflight

    generation_hash = "c" * 64
    quote_unit = f"rquant-runtime-watchlist-quote@svc-{'2' * 64}.service"
    inspection = RuntimeDeploymentInspection(
        runtime_root=tmp_path / "runtime",
        generation_hash=generation_hash,
        inventory_units=(quote_unit,),
        strict_authority_units=(),
        watchlist_quote_units=(quote_unit,),
        status="warn",
        summary="watchlist quote heartbeat is missing or stale for current generation",
    )
    advisory_result = preflight.CheckResult(
        "watchlist_quote_runtime",
        "warn",
        inspection.summary,
        [f"  advisory unit: {quote_unit}"],
    )
    cgroup_root = tmp_path / "cgroup"
    cgroup_root.mkdir()
    (cgroup_root / "cgroup.controllers").write_text(
        "cpu io memory pids\n",
        encoding="utf-8",
    )
    arbiter = tmp_path / "rquant-workload-arbiter"
    source_arbiter = Path(__file__).resolve().parents[2] / "deploy/libexec/rquant-workload-arbiter"
    arbiter.write_bytes(source_arbiter.read_bytes())
    arbiter.chmod(0o755)
    arbiter_hash = tmp_path / "rquant-workload-arbiter.sha256"
    arbiter_hash.write_text(
        f"{hashlib.sha256(arbiter.read_bytes()).hexdigest()}\n",
        encoding="ascii",
    )
    arbiter_hash.chmod(0o444)

    def fake_systemctl(command: list[str], **_kwargs: object) -> SimpleNamespace:
        if "list-units" in command:
            return SimpleNamespace(
                returncode=0,
                stdout=f"{quote_unit} loaded failed failed advisory\n",
                stderr="",
            )
        unit = command[2]
        if unit == quote_unit:
            return SimpleNamespace(
                returncode=0,
                stdout=(
                    "LoadState=loaded\nActiveState=failed\nSlice=rquant-live.slice\nControlGroup=\n"
                ),
                stderr="",
            )
        return SimpleNamespace(
            returncode=0,
            stdout="LoadState=loaded\nActiveState=inactive\nControlGroup=\n",
            stderr="",
        )

    ok = preflight.CheckResult("stub", "ok", "ok")
    monkeypatch.setattr(
        preflight,
        "runtime_deployment_service_checks",
        lambda _root: (inspection, advisory_result),
    )
    for name in (
        "verify_runtime_dependencies",
        "verify_daily_receipt_authority_runtime",
        "detail_duckdb_lock",
        "smoke_screen",
    ):
        monkeypatch.setattr(preflight, name, lambda *_args, **_kwargs: ok)
    monkeypatch.setattr(preflight, "verify_unit_files", lambda _path: ok)
    monkeypatch.setattr(preflight, "detail_systemd_state", lambda _units: ok)
    monkeypatch.setattr(preflight, "check_data_freshness", lambda **_kwargs: ok)
    monkeypatch.setattr(
        preflight,
        "verify_workload_unit_declarations",
        lambda _path: WorkloadCheck("workload_unit_declarations", "ok", "ok"),
    )
    monkeypatch.setattr(
        preflight,
        "check_workload_capacity_baseline",
        lambda: WorkloadCheck("workload_capacity", "ok", "ok"),
    )
    monkeypatch.setattr(
        preflight,
        "check_workload_high_water_evidence",
        lambda: WorkloadCheck("workload_high_water", "ok", "ok"),
    )
    monkeypatch.setattr("rquant.workload_isolation.subprocess.run", fake_systemctl)

    results = preflight.run_all_checks(
        systemd_dir=tmp_path,
        runtime_root=inspection.runtime_root,
        workload_runtime_config=preflight.WorkloadRuntimeProbeConfig(
            cgroup_root=cgroup_root,
            platform_name="Linux",
            strict=True,
            arbiter_path=arbiter,
            arbiter_hash_path=arbiter_hash,
            arbiter_expected_uid=os.getuid(),
        ),
    )

    workload = next(result for result in results if result.name == "workload_runtime")
    assert workload.status == "warn"
    assert any(generation_hash in detail for detail in workload.details)
    assert any(quote_unit in detail and "failed" in detail for detail in workload.details)
    assert any("stale" in detail for detail in workload.details)
    assert not any(detail.startswith("  x ") for detail in workload.details)
    assert not any(result.status == "fail" for result in results)
