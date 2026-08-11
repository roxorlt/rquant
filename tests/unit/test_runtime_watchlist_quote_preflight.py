from __future__ import annotations

from pathlib import Path

import pytest

from rquant.runtime_deployment_bundle import RuntimeDeploymentReceipt
from rquant.runtime_deployment_preflight import (
    RuntimeDeploymentInspection,
    inspect_runtime_deployment,
)

COMMIT = "a" * 40
PROFILE = "b" * 64
GENERATION = "c" * 64


def _receipt(root: Path) -> RuntimeDeploymentReceipt:
    minute_instance = f"svc-{'1' * 64}"
    quote_instance = f"svc-{'2' * 64}"
    return RuntimeDeploymentReceipt(
        runtime_root=root,
        producer_commit=COMMIT,
        generation_hash=GENERATION,
        deployment_profile_id=PROFILE,
        instance_mapping={
            "market.minute.source.v1": minute_instance,
            "watchlist.quote.source.v1": quote_instance,
        },
        unit_mapping={
            "market.minute.source.v1": (f"rquant-runtime-market-minute@{minute_instance}.service"),
            "watchlist.quote.source.v1": (
                f"rquant-runtime-watchlist-quote@{quote_instance}.service"
            ),
        },
    )


def test_inspection_fans_in_dynamic_units_and_requires_quote_generation_health(
    tmp_path: Path,
) -> None:
    root = tmp_path / "runtime"
    receipt = _receipt(root)
    state_calls: list[str] = []
    health_calls: list[tuple[RuntimeDeploymentReceipt, str]] = []

    result = inspect_runtime_deployment(
        root,
        receipt_loader=lambda observed_root: receipt,
        unit_state_probe=lambda unit: state_calls.append(unit) or "active",
        generation_health_probe=lambda observed_receipt, unit: (
            health_calls.append((observed_receipt, unit)) is None
        ),
    )

    quote_unit = receipt.unit_mapping["watchlist.quote.source.v1"]
    minute_unit = receipt.unit_mapping["market.minute.source.v1"]
    assert result.status == "ok"
    assert result.inventory_units == tuple(sorted(receipt.unit_mapping.values()))
    assert result.strict_authority_units == (minute_unit,)
    assert result.watchlist_quote_units == (quote_unit,)
    assert state_calls == [quote_unit]
    assert health_calls == [(receipt, quote_unit)]


def test_inspection_without_current_receipt_is_degraded_not_green_or_failed(
    tmp_path: Path,
) -> None:
    root = tmp_path / "runtime"

    result = inspect_runtime_deployment(
        root,
        receipt_loader=lambda _root: (_ for _ in ()).throw(
            ValueError("runtime current deployment is missing")
        ),
        unit_state_probe=lambda _unit: pytest.fail("no unit may be probed"),
    )

    assert result.status == "warn"
    assert result.inventory_units == ()
    assert result.strict_authority_units == ()
    assert "no current deployment receipt" in result.summary


def test_inspection_contract_rejects_quote_unit_inside_strict_gate() -> None:
    quote_unit = f"rquant-runtime-watchlist-quote@svc-{'2' * 64}.service"

    with pytest.raises(ValueError, match="partition"):
        RuntimeDeploymentInspection(
            runtime_root=Path("/runtime"),
            inventory_units=(quote_unit,),
            strict_authority_units=(quote_unit,),
            watchlist_quote_units=(quote_unit,),
            status="warn",
            summary="invalid overlap",
        )


@pytest.mark.parametrize(
    ("unit_state", "heartbeat_ok", "expected_fragment"),
    (
        ("inactive", True, "candidate unit is not active"),
        ("failed", True, "candidate unit is not active"),
        ("active", False, "heartbeat is missing or stale"),
    ),
)
def test_inspection_degrades_unstarted_or_unhealthy_quote_candidate(
    tmp_path: Path,
    unit_state: str,
    heartbeat_ok: bool,
    expected_fragment: str,
) -> None:
    root = tmp_path / "runtime"
    receipt = _receipt(root)

    result = inspect_runtime_deployment(
        root,
        receipt_loader=lambda _root: receipt,
        unit_state_probe=lambda _unit: unit_state,
        generation_health_probe=lambda _receipt, _unit: heartbeat_ok,
    )

    assert result.status == "warn"
    assert expected_fragment in result.summary


def test_preflight_and_pre_market_keep_quote_inventory_out_of_strict_units(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import pre_market_check, preflight

    quote_unit = f"rquant-runtime-watchlist-quote@svc-{'2' * 64}.service"
    inspection = RuntimeDeploymentInspection(
        runtime_root=Path("/runtime"),
        generation_hash=GENERATION,
        inventory_units=(quote_unit,),
        strict_authority_units=(),
        watchlist_quote_units=(quote_unit,),
        status="warn",
        summary="watchlist quote candidate unit is not active",
    )
    monkeypatch.setattr(preflight, "inspect_runtime_deployment", lambda _root: inspection)
    monkeypatch.setattr(pre_market_check, "inspect_runtime_deployment", lambda _root: inspection)

    preflight_inspection, preflight_result = preflight.runtime_deployment_service_checks(
        Path("/runtime")
    )
    pre_market_inspection, pre_market_result = pre_market_check.runtime_deployment_service_checks(
        Path("/runtime")
    )

    assert preflight_inspection == pre_market_inspection == inspection
    assert preflight_inspection.inventory_units == (quote_unit,)
    assert preflight_inspection.strict_authority_units == ()
    assert preflight_result.status == pre_market_result.status == "warn"
    assert preflight_result.name == pre_market_result.name == "watchlist_quote_runtime"


def test_pre_market_candidate_failure_is_advisory_but_remains_in_journal_inventory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import pre_market_check

    strict_unit = f"rquant-runtime-market-minute@svc-{'1' * 64}.service"
    quote_unit = f"rquant-runtime-watchlist-quote@svc-{'2' * 64}.service"
    inspection = RuntimeDeploymentInspection(
        runtime_root=Path("/runtime"),
        generation_hash=GENERATION,
        inventory_units=(strict_unit, quote_unit),
        strict_authority_units=(strict_unit,),
        watchlist_quote_units=(quote_unit,),
        status="warn",
        summary=f"watchlist quote candidate unit is not active: {quote_unit}=failed",
    )
    advisory = pre_market_check.CheckResult(
        "watchlist_quote_runtime",
        "warn",
        inspection.summary,
    )
    service_units: list[str] = []
    journal_units: list[str] = []
    ok = pre_market_check.CheckResult("stub", "ok", "ok")
    monkeypatch.setattr(
        pre_market_check,
        "runtime_deployment_service_checks",
        lambda _root: (inspection, advisory),
    )
    monkeypatch.setattr(pre_market_check, "check_duckdb_lock", lambda _path: ok)
    monkeypatch.setattr(pre_market_check, "check_disk_space", lambda _path: ok)
    monkeypatch.setattr(pre_market_check, "check_tushare_credits", lambda _token: ok)
    monkeypatch.setattr(
        pre_market_check,
        "check_systemd_services",
        lambda units: service_units.extend(units) or [ok],
    )
    monkeypatch.setattr(
        pre_market_check,
        "check_daily_receipt_authority_identity",
        lambda: ok,
    )
    monkeypatch.setattr(
        pre_market_check,
        "check_recent_errors",
        lambda units: journal_units.extend(units) or ok,
    )

    results = pre_market_check.run_all_checks(runtime_root=Path("/runtime"))

    assert strict_unit in service_units
    assert quote_unit not in service_units
    assert strict_unit in journal_units
    assert quote_unit in journal_units
    quote_result = next(result for result in results if result.name == "watchlist_quote_runtime")
    assert quote_result.status == "warn"
    assert quote_unit in quote_result.message


def test_preflight_candidate_failure_stays_out_of_generic_systemd_fail_gate(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from rquant import preflight

    strict_unit = f"rquant-runtime-market-minute@svc-{'1' * 64}.service"
    quote_unit = f"rquant-runtime-watchlist-quote@svc-{'2' * 64}.service"
    inspection = RuntimeDeploymentInspection(
        runtime_root=Path("/runtime"),
        generation_hash=GENERATION,
        inventory_units=(strict_unit, quote_unit),
        strict_authority_units=(strict_unit,),
        watchlist_quote_units=(quote_unit,),
        status="warn",
        summary=f"watchlist quote candidate unit is not active: {quote_unit}=failed",
    )
    advisory = preflight.CheckResult(
        "watchlist_quote_runtime",
        "warn",
        inspection.summary,
        [f"  inventory unit: {unit}" for unit in inspection.inventory_units],
    )
    generic_units: list[str] = []
    ok = preflight.CheckResult("stub", "ok", "ok")
    monkeypatch.setattr(
        preflight,
        "runtime_deployment_service_checks",
        lambda _root: (inspection, advisory),
    )
    monkeypatch.setattr(preflight, "verify_runtime_dependencies", lambda: ok)
    monkeypatch.setattr(preflight, "verify_daily_receipt_authority_runtime", lambda: ok)
    monkeypatch.setattr(preflight, "verify_unit_files", lambda _path: ok)
    monkeypatch.setattr(
        preflight,
        "detail_systemd_state",
        lambda units: generic_units.extend(units) or ok,
    )
    monkeypatch.setattr(preflight, "detail_duckdb_lock", lambda _path: ok)
    monkeypatch.setattr(preflight, "check_data_freshness", lambda **_kwargs: ok)
    monkeypatch.setattr(preflight, "smoke_screen", lambda: ok)
    monkeypatch.setattr(
        preflight,
        "verify_workload_unit_declarations",
        lambda _path: object(),
    )
    monkeypatch.setattr(preflight, "_safe_workload_probe", lambda *_args: object())
    monkeypatch.setattr(preflight, "_workload_check_result", lambda _result: ok)

    results = preflight.run_all_checks(
        systemd_dir=tmp_path,
        runtime_root=Path("/runtime"),
    )

    assert strict_unit in generic_units
    assert quote_unit not in generic_units
    quote_result = next(result for result in results if result.name == "watchlist_quote_runtime")
    assert quote_result.status == "warn"
    assert any(quote_unit in detail for detail in quote_result.details)
