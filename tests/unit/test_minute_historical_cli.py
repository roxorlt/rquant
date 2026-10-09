from __future__ import annotations

import base64
import hashlib
import importlib
import importlib.util
import json
import sys
from decimal import Decimal
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest


def _api() -> ModuleType:
    name = "rquant.minute_historical_cli"
    if importlib.util.find_spec(name) is None:
        pytest.fail("The config-free historical CLI is not implemented")
    return importlib.import_module(name)


def _origin(key: str, value: object) -> dict[str, str]:
    raw = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()
    return {
        "object_key": key,
        "format": "json",
        "content_base64": base64.b64encode(raw).decode(),
        "content_sha256": hashlib.sha256(raw).hexdigest(),
    }


def _prepare_input(*, facts: bool = True) -> dict[str, Any]:
    code = "600000.SH"
    kinds = (
        "eligibility",
        "risk_warning",
        "suspension",
        "price_limits",
        "quotes",
        "warm_history",
        "prior_reference",
        "prior_state",
    )
    materials = [
        _origin(
            "minute-original",
            {
                "rows": [
                    {
                        "ts_code": code,
                        "trade_time": f"2026-09-28T09:{minute}:00+08:00",
                        "freq": "1min",
                        "source": "explicit-minute-start",
                        "open": "10.1234567890123456789",
                        "high": "10.2",
                        "low": "10.0",
                        "close": "10.15",
                        "vol": "123.456",
                        "amount": "1234.56789",
                    }
                    for minute in (30, 31)
                ]
            },
        ),
        _origin(
            "screen-original",
            {"rows": [{"ts_code": code, "trade_date": "2026-09-25", "preset_name": "n-shape"}]},
        ),
        _origin(
            "facts-original",
            {
                "rows": [
                    {
                        "kind": kind,
                        "ts_code": code,
                        "trade_date": "2026-09-28",
                        "status": "complete",
                        "value": kind not in {"risk_warning", "suspension"},
                        "available_at": "2026-09-25T15:00:00+08:00",
                        "reference_trade_date": "2026-09-25"
                        if kind in {"prior_reference", "prior_state"}
                        else None,
                    }
                    for kind in kinds
                ]
                if facts
                else []
            },
        ),
        _origin("configuration-original", {"synthetic": True, "commission_bps": "3.1"}),
    ]
    windows = [{"opens_at": "09:30:00", "closes_at": "09:32:00"}]
    return {
        "action": "prepare",
        "request": {
            "policy": {
                "policy_id": "retained-minute-research",
                "version": 1,
                "timestamp_semantics": "bar_end",
                "market_event_basis": (
                    "Synthetic original minute starts become ends plus one minute."
                ),
                "market_visibility_basis": "Modeled completed bar, not actual capture.",
                "candidate_visibility_basis": "Previous completed-day screen at replay bootstrap.",
                "constraint_visibility_basis": "Missing original facts remain unavailable.",
                "native_definition_basis": "Exact current definition with modeled visibility.",
                "limitations": (
                    "Synthetic CLI fixture; no trusted source, formal execution or 32-day proof."
                ),
            },
            "origin_materials": materials,
            "minute_archives": [{"origin_object_key": "minute-original", "rows_pointer": "/rows"}],
            "candidate_archives": [
                {
                    "origin_object_key": "screen-original",
                    "rows_pointer": "/rows",
                    "kind": "completed_screen",
                    "completed_trade_dates": ["2026-09-25"],
                }
            ],
            "fact_archives": [{"origin_object_key": "facts-original", "rows_pointer": "/rows"}],
            "timestamp_bases": [
                {
                    "provider_label": "explicit-minute-start",
                    "semantics": "minute_start",
                    "source_timezone": "Asia/Shanghai",
                    "explanation": "Explicit synthetic start labels.",
                }
            ],
            "calendar_timezone": "Asia/Shanghai",
            "calendar": [
                {
                    "trade_date": f"2026-09-{day}",
                    "is_open": day in (25, 28),
                    "windows": windows if day in (25, 28) else [],
                }
                for day in range(25, 29)
            ],
            "replay_days": [
                {"trade_date": "2026-09-28", "initialized_at": "2026-09-28T09:30:00+08:00"}
            ],
            "registration": {
                "strategy_id": "n_shape",
                "strategy_version": "7",
                "definition_fingerprint": "a" * 64,
                "configuration_origin_key": "configuration-original",
                "configuration_sha256": materials[3]["content_sha256"],
                "registered_at": "2026-10-08T10:00:00+08:00",
            },
            "prepared_at": "2026-10-08T11:00:00+08:00",
        },
    }


def _invoke(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    payload: object,
    *,
    action: str = "prepare",
    filename: str = "result.json",
) -> tuple[int, dict[str, Any], bytes]:
    from rquant import cli, config

    def forbidden_settings() -> None:
        pytest.fail("Historical CLI called Settings")

    monkeypatch.setattr(config, "get_settings", forbidden_settings)
    _api()
    source = tmp_path / "input.json"
    raw = json.dumps(payload, ensure_ascii=False).encode()
    source.write_bytes(raw)
    output = tmp_path / filename
    monkeypatch.setattr(
        sys,
        "argv",
        ["rquant", "minute-historical", action, "--input", str(source), "--output", str(output)],
    )
    exit_code = cli.main()
    assert source.read_bytes() == raw
    assert config._SETTINGS is None
    return exit_code, json.loads(output.read_bytes()), raw


def test_main_dispatch_and_help_do_not_load_settings(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from rquant import cli, config

    def forbidden_settings() -> None:
        pytest.fail("Historical command is missing before get_settings()")

    monkeypatch.setattr(config, "get_settings", forbidden_settings)
    monkeypatch.setattr(sys, "argv", ["rquant", "minute-historical", "--help"])
    with pytest.raises(SystemExit) as exc:
        cli.main()
    assert exc.value.code == 0
    assert "prepare" in capsys.readouterr().out


def test_prepare_calls_real_helper_and_retains_raw_identity_and_modeled_times(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = _prepare_input()
    exit_code, receipt, raw = _invoke(tmp_path, monkeypatch, original)
    assert exit_code == 0 and receipt["status"] == "complete"
    assert receipt["input_sha256"] == hashlib.sha256(raw).hexdigest()
    assert receipt["diagnostic_only"] is True and receipt["formal_history_passed"] is False
    assert receipt["formal_authorization"] == receipt["policy_installation"] == "not_assessed"
    value = receipt["preparation"]
    assert value["origin_materials"] == original["request"]["origin_materials"]
    assert value["registration"] == original["request"]["registration"]
    assert (
        value["native_definition_visibility"][0]["modeled_available_at"]
        == "2026-09-28T09:30:00+08:00"
    )
    assert value["minutes"][0]["open"] == "10.1234567890123456789"
    assert value["minutes"][0]["bar_end"] == "2026-09-28T09:31:00+08:00"
    assert (
        value["ready_for_original_executor"] is False and value["formal_source_published"] is False
    )


def test_prepare_missing_facts_is_unavailable_and_preserves_each_reason(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    exit_code, receipt, _ = _invoke(tmp_path, monkeypatch, _prepare_input(facts=False))
    assert exit_code != 0 and receipt["status"] == "unavailable"
    value = receipt["preparation"]
    assert value["materials_ready"] is True and value["facts_ready"] is False
    assert (
        len({x["fact_kind"] for x in value["unavailable_reasons"] if x["code"] == "fact_missing"})
        == 8
    )
    assert len(value["minutes"]) == 2
    assert receipt["formal_history_passed"] is False


def test_prepare_columnar_originals_keeps_original_cell_and_header_pointers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    value = _prepare_input()
    for index, archive_kind in enumerate(
        ("minute_archives", "candidate_archives", "fact_archives")
    ):
        origin = value["request"]["origin_materials"][index]
        rows = json.loads(base64.b64decode(origin["content_base64"]))["rows"]
        columns = list(rows[0])
        value["request"]["origin_materials"][index] = _origin(
            origin["object_key"],
            {"columns": columns, "rows": [[row[field] for field in columns] for row in rows]},
        )
        value["request"][archive_kind][0]["columns_pointer"] = "/columns"
    exit_code, receipt, _ = _invoke(tmp_path, monkeypatch, value)
    assert exit_code == 0 and receipt["status"] == "complete"
    prepared = receipt["preparation"]
    assert prepared["origin_materials"] == value["request"]["origin_materials"]
    fields = {
        field["field_name"]: field for field in prepared["minutes"][0]["origins"][0]["fields"]
    }
    assert fields["vol"]["value_json_pointer"] == "/rows/0/8"
    assert fields["vol"]["column_name_json_pointer"] == "/columns/8"
    assert prepared["minutes"][0]["volume"] == "123.456"


@pytest.mark.parametrize("mutation", ["hash", "configuration", "action", "extra"])
def test_bad_typed_input_emits_unavailable_without_repairing_original(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    value = _prepare_input()
    if mutation == "hash":
        value["request"]["origin_materials"][0]["content_sha256"] = "0" * 64
    elif mutation == "configuration":
        value["request"]["registration"]["configuration_sha256"] = "0" * 64
    elif mutation == "action":
        value["action"] = "compare"
    else:
        value["source_installed"] = True
    exit_code, receipt, _ = _invoke(tmp_path, monkeypatch, value)
    assert exit_code != 0 and receipt["status"] == "unavailable"
    assert receipt["preparation"] is None and receipt["errors"]
    assert receipt["formal_authorization"] == "not_assessed"


def test_duplicate_json_keys_are_rejected_and_failure_is_saved(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    api = _api()
    source = tmp_path / "duplicate.json"
    raw = b'{"action":"prepare","action":"compare","request":{}}'
    source.write_bytes(raw)
    output = tmp_path / "failure.json"
    assert api.main(["prepare", "--input", str(source), "--output", str(output)]) != 0
    receipt = json.loads(output.read_bytes())
    assert source.read_bytes() == raw
    assert receipt["status"] == "unavailable" and receipt["errors"][0]["code"] == "invalid_json"


def test_existing_receipt_is_preserved_without_overwrite(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    api = _api()
    source = tmp_path / "input.json"
    source.write_text(json.dumps(_prepare_input()))
    output = tmp_path / "prior-result.json"
    prior = b"prior immutable evidence\n"
    output.write_bytes(prior)
    assert api.main(["prepare", "--input", str(source), "--output", str(output)]) != 0
    assert output.read_bytes() == prior
    assert "output_unavailable" in capsys.readouterr().out


def test_missing_input_file_keeps_a_typed_failure_receipt(tmp_path: Path) -> None:
    api = _api()
    output = tmp_path / "missing-input-result.json"
    assert (
        api.main(["prepare", "--input", str(tmp_path / "absent.json"), "--output", str(output)])
        != 0
    )
    receipt = json.loads(output.read_bytes())
    assert receipt["status"] == "unavailable" and receipt["input_sha256"] is None
    assert receipt["errors"][0]["code"] == "input_unavailable"


def test_trade_csv_cannot_supply_old_complete_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    exit_code, receipt, _ = _invoke(
        tmp_path,
        monkeypatch,
        {
            "action": "compare",
            "native_result": {},
            "new_evidence": {},
            "old_evidence": {"trade_csv": "entry_price,exit_price\n10,11\n"},
            "matches": [],
        },
        action="compare",
    )
    assert exit_code != 0 and receipt["status"] == "unavailable"
    assert receipt["comparison"] is None and receipt["errors"]
    assert receipt["formal_history_passed"] is False


def _compare_input(*, same_costs: bool = False) -> dict[str, Any]:
    # Reuse the component owner's explicit frozen synthetic ledgers, never infer
    # cash-flow evidence from the runtime DTO's fills.
    from tests.unit.test_minute_historical_comparison import _sample

    sample = _sample(same_costs=same_costs)
    return {
        "action": "compare",
        "native_result": sample.native.model_dump(mode="json", exclude_computed_fields=True),
        "new_evidence": sample.new.model_dump(mode="json", exclude_computed_fields=True),
        "old_evidence": sample.old.model_dump(mode="json", exclude_computed_fields=True),
        "matches": [item.model_dump(mode="json") for item in sample.matches],
    }


def _edit_origin(origin: dict[str, str], edit: Any) -> dict[str, str]:
    value = json.loads(base64.b64decode(origin["content_base64"]))
    edit(value)
    return _origin(origin["object_key"], value)


def test_compare_calls_real_helper_and_preserves_precise_explanations(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = _compare_input()
    exit_code, receipt, _ = _invoke(tmp_path, monkeypatch, original, action="compare")
    assert exit_code == 0 and receipt["status"] == "diagnostic_complete"
    result = receipt["comparison"]
    assert result["formal_history_passed"] is False and receipt["formal_history_passed"] is False
    assert result["new_side"]["result_origin"] == original["new_evidence"]["result_origin"]
    assert result["old_side"]["result_origin"] == original["old_evidence"]["result_origin"]
    explanations = {(x["scope"], x["field"]): x["explanation"] for x in result["differences"]}
    assert set(explanations) == {("fills", "price"), ("fees", "commission"), ("fees", "total_fees")}
    assert Decimal(explanations[("fills", "price")]["delta"]) == Decimal("0.01")
    assert Decimal(explanations[("fees", "commission")]["delta"]) == Decimal("-1")
    assert all(x["old_input"]["content_sha256"] for x in explanations.values())


@pytest.mark.parametrize(
    "missing", ["signals", "ledger", "daily_nav", "rules", "new_ledger", "cost_context"]
)
def test_compare_missing_material_stays_unavailable_with_observed_differences(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    missing: str,
) -> None:
    original = _compare_input()
    if missing in {"signals", "ledger", "daily_nav"}:
        original["old_evidence"]["result_mapping"][missing] = None
    elif missing == "rules":
        original["old_evidence"]["rules_origin"] = _edit_origin(
            original["old_evidence"]["rules_origin"],
            lambda value: value.update(cost_source_sha256="0" * 64),
        )
    elif missing == "new_ledger":
        original["new_evidence"].update(ledger_origin=None, ledger_mapping=None)
    else:
        original["old_evidence"]["cost_inputs"] = []
    exit_code, receipt, _ = _invoke(tmp_path, monkeypatch, original, action="compare")
    assert exit_code != 0 and receipt["status"] == "unavailable"
    result = receipt["comparison"]
    assert result["unavailable_reasons"] and result["differences"]
    assert result["old_side"]["result_origin"] == original["old_evidence"]["result_origin"]
    assert result["formal_history_passed"] is False
    if missing == "new_ledger":
        assert not any(x["scope"] == "ledger" for x in result["new_side"]["rows"])


def test_compare_detached_native_origin_cannot_be_a_diagnostic_pass(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    value = _compare_input()
    value["new_evidence"]["result_origin"] = _edit_origin(
        value["new_evidence"]["result_origin"], lambda raw: raw.update(status="incomplete")
    )
    exit_code, receipt, _ = _invoke(tmp_path, monkeypatch, value, action="compare")
    assert exit_code != 0 and receipt["status"] == "unavailable"
    assert any("native" in x for x in receipt["comparison"]["unavailable_reasons"])


def test_compare_incomplete_execution_and_same_basis_failure_have_nonzero_exit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    value = _compare_input(same_costs=True)
    value["native_result"].update(
        status="incomplete", incomplete_reasons=["synthetic stopped execution"]
    )
    value["new_evidence"]["result_origin"] = _origin("new-result", value["native_result"])
    exit_code, receipt, _ = _invoke(tmp_path, monkeypatch, value, action="compare")
    assert exit_code != 0 and receipt["status"] == "execution_incomplete"
    assert receipt["comparison"]["execution_complete"] is False

    value = _compare_input(same_costs=True)
    value["old_evidence"]["result_origin"] = _edit_origin(
        value["old_evidence"]["result_origin"],
        lambda raw: raw["daily_nav"][0].update(nav="9983.900000000001"),
    )
    exit_code, receipt, _ = _invoke(
        tmp_path, monkeypatch, value, action="compare", filename="blocked.json"
    )
    assert exit_code != 0 and receipt["status"] == "blocked"
    assert receipt["comparison"]["same_basis_failures"]
    assert receipt["comparison"]["differences"][0]["old_value"]["value"] == "9983.900000000001"


def test_missing_real_registration_and_calendar_are_typed_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    value = _prepare_input()
    del value["request"]["registration"]
    del value["request"]["calendar"]
    exit_code, receipt, _ = _invoke(tmp_path, monkeypatch, value)
    assert exit_code != 0 and receipt["status"] == "unavailable"
    paths = {item["field_path"] for item in receipt["errors"]}
    assert {"request.registration", "request.calendar"} <= paths
