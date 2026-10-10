"""Frozen diagnostic samples; these are not historical execution evidence."""

from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import replace, dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest


def _origin(key: str, value: Any, *, raw: bool = False) -> Any:
    from rquant.minute_backtest_publication_contracts import MinuteOriginMaterial

    payload = value if raw else json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return MinuteOriginMaterial(
        object_key=key,
        content_base64=base64.b64encode(payload).decode(),
        content_sha256=hashlib.sha256(payload).hexdigest(),
        format="bytes" if raw else "json",
    )


@dataclass(frozen=True)
class Sample:
    native: Any
    new: Any
    old: Any
    matches: tuple[Any, ...]


def _sample(*, same_costs: bool = False) -> Sample:
    from rquant.minute_historical_comparison import (
        HistoricalColumnMapping,
        HistoricalCostInputReference,
        HistoricalFrozenIdentity,
        HistoricalResultMapping,
        HistoricalRowMatch,
        HistoricalRuleManifest,
        HistoricalSideEvidence,
        HistoricalTableMapping,
    )
    from rquant.minute_backtest_contracts import MinuteReplayExecutionProfile
    from rquant.minute_backtest_runner import MinuteRuntimeReplayResult
    from rquant.order_execution_costs import calculate_execution_costs
    from rquant.paper_contracts import PaperAccountSnapshot, PaperOrder
    from rquant.paper_signal_worker import PaperQuoteSnapshot, PaperSignalPolicy
    from rquant.signal_contracts import SignalEnvelope
    from tests.paper_cost_fixtures import paper_execution_cost_spec, paper_instrument_context

    cost = paper_execution_cost_spec(transfer_fee_bps=Decimal("1"), buy_slippage_bps=Decimal("10"))
    context = paper_instrument_context()
    calc = calculate_execution_costs(cost, {"side": "BUY", "reference_price": "10", "quantity": 100}, context)
    at = "2026-09-30T01:32:00Z"
    profile = MinuteReplayExecutionProfile(
        key="frozen-diagnostic", version=1, initial_cash=Decimal("10000"), execution_costs=cost,
        paper_policy=PaperSignalPolicy.model_validate_json(json.dumps({"account_id": "diagnostic", "execution_lag": "PT1M",
            "action_quantities": {"b_intent": 100, "reduce": 100, "s_intent": 100}, "producer_commit": "a" * 40})),
        routing_policy_fingerprint="e" * 64,
    )
    signal = SignalEnvelope.model_validate_json(json.dumps({
        "schema_version": 1, "strategy_id": "n_shape", "strategy_version": "1",
        "parameter_fingerprint": "1" * 64, "dataset_snapshot_id": "2" * 64, "feature_snapshot_id": "3" * 64,
        "event_time": "2026-09-30T01:30:00Z", "available_at": "2026-09-30T01:31:00Z",
        "candidate_id": "600000.SH", "action": "b_intent", "reason_codes": ["diagnostic"],
        "evidence": {"close": "10"}, "expires_at": "2026-09-30T02:00:00Z", "producer_commit": "a" * 40,
    }))
    quote = PaperQuoteSnapshot.model_validate_json(json.dumps({
        "ts_code": "600000.SH", "event_time": "2026-09-30T07:00:00Z", "available_at": "2026-09-30T07:00:00Z",
        "context": {"executable_price": "9.9", "acquisition_available_date": "2026-10-01", "instrument_context": context.model_dump(mode="json")},
        "producer_commit": "a" * 40,
    }))
    order = PaperOrder.model_validate_json(json.dumps({
        "intent_id": "b" * 64, "account_id": "diagnostic", "ts_code": "600000.SH", "side": "BUY", "order_type": "MARKET",
        "quantity": 100, "filled_quantity": 100, "average_fill_price": "10.0100", "status": "FILLED", "created_at": at, "updated_at": at,
    }))
    account = PaperAccountSnapshot.model_validate_json(json.dumps({
        "account_id": "diagnostic", "as_of_time": "2026-09-30T07:00:00Z", "cash": "8993.90", "available_cash": "8993.90", "frozen_cash": "0",
        "holdings": [{"code": "600000.SH", "quantity": 100, "available_quantity": 0, "frozen_quantity": 100, "average_cost": "10.061", "market_price": "9.9"}],
        "realized_pnl": "0", "unrealized_pnl": "-16.10", "nav": "9983.90",
    }))
    native = MinuteRuntimeReplayResult.model_validate_json(json.dumps({
        "input_hash": "4" * 64, "profile_hash": profile.profile_hash, "strategy_id": "n_shape", "strategy_version": 1,
        "status": "complete", "daily_status": "complete", "result_budget": {},
        "work": {"raw_rows": 1, "warmup_rows": 0, "static_rows": 0, "market_batches": 1, "union_codes": 1, "daily_observations": 1},
        "execution_profile": profile.model_dump(mode="json"), "signals": [signal.model_dump(mode="json")],
        "orders": [order.model_dump(mode="json")],
        "fills": [{"execution_id": "c" * 64, "order_id": order.order_id, "sequence": 1, "quantity": 100,
                   "price": "10.0100", "commission": "5.00", "transfer_fee": "0.10", "tax": "0", "total_fees": "5.10",
                   "cost_spec_id": cost.cost_spec_id, "cost_spec_schema_version": 3, "cost_context_fingerprint": calc.cost_context_fingerprint,
                   "cost_provenance_state": "KNOWN_V3", "executed_at": at, "price_snapshot_id": "f" * 64}],
        "queue_records": [], "account": account.model_dump(mode="json"),
        "daily_valuations": [{"input_hash": "4" * 64, "profile_hash": profile.profile_hash, "calendar_sha256": "5" * 64,
                             "trade_date": "2026-09-30", "as_of": "2026-09-30T07:00:00Z", "observed_at": "2026-09-30T07:00:00Z", "status": "complete",
                             "market_pointer": {"channel": "market_minute", "source_generation_id": "6" * 64, "batch_id": "diagnostic", "sequence": 0,
                                                "revision": 1, "content_sha256": "7" * 64, "quality_status": "published", "published_at": "2026-09-30T07:00:00Z"},
                             "price_proofs": [{"entry_signal_id": signal.signal_id, "quote": quote.model_dump(mode="json")}], "account": account.model_dump(mode="json")}],
    }))
    fill_id = native.fills[0].fill_id
    assert fill_id is not None
    identity = HistoricalFrozenIdentity(engine_id="minute_runtime_replay", engine_version="test-paper-cost-engine-v3", producer_commit="a" * 40,
        strategy_id="n_shape", strategy_version="1", input_hash=native.input_hash, profile_hash=native.profile_hash)
    old_identity = identity if same_costs else identity.model_copy(update={"engine_id": "legacy-frozen", "engine_version": "a-share-round-trip-notional-v2", "profile_hash": "8" * 64})
    sources = tuple(_origin(name, (Path(__file__).parents[2] / "src/rquant" / filename).read_bytes(), raw=True)
                    for name, filename in (("cost-source", "order_execution_costs.py"), ("legacy-cost-source", "strategy_execution_costs.py")))
    semantic = _origin("diagnostic-rules", {"signal": "frozen sample", "execution": "frozen sample", "ledger": "actual cash rows", "nav": "actual daily rows"})
    shared = _origin("shared-input", {"calendar": ["2026-09-30"], "security": "600000.SH", "quantity": 100})

    def rules(side_identity: Any, old: bool) -> Any:
        execution = cost.model_dump(mode="json") if not old or same_costs else {
            "schema_version": 2, "commission_bps": "3", "stamp_duty_bps": "10", "transfer_fee_bps": "1", "slippage_bps": "0",
            "minimum_commission": "6", "research_notional_per_trade": "1000",
        }
        manifest = HistoricalRuleManifest(identity=side_identity, shared_input_sha256=shared.content_sha256, initial_cash=Decimal("10000"),
            execution_costs=execution, cost_evaluator="v3-shared-fill" if not old or same_costs else "v2-notional-executed",
            cost_source_sha256=sources[0].content_sha256, legacy_semantics_sha256=sources[1].content_sha256 if old and not same_costs else None,
            signal_rule_sha256=semantic.content_sha256, execution_rule_sha256=semantic.content_sha256,
            ledger_rule_sha256=semantic.content_sha256, nav_rule_sha256=semantic.content_sha256)
        return _origin("new-rules" if not old else "old-rules", manifest.model_dump(mode="json"))

    def table(scope: str, fields: tuple[str, ...]) -> Any:
        return HistoricalTableMapping(rows_pointer=f"/{scope}", row_id_pointer="/id",
            columns=tuple(HistoricalColumnMapping(field=field, pointer=f"/{field}") for field in fields))

    signal_fields = ("candidate_id", "action", "event_time", "available_at", "expires_at", "parameter_fingerprint", "dataset_snapshot_id", "feature_snapshot_id", "reason_codes", "evidence")
    fill_fields = ("ts_code", "side", "quantity", "price", "executed_at", "price_snapshot_id", "commission", "transfer_fee", "tax", "total_fees")
    ledger_fields = ("trade_date", "event_time", "kind", "amount", "cash_before", "cash_after", "fill_id", "currency")
    nav_fields = ("trade_date", "as_of", "cash", "nav", "realized_pnl", "unrealized_pnl", "holdings")
    signal_row = {"id": "old-signal", **{key: signal.model_dump(mode="json")[key] for key in signal_fields}}
    old_price, old_commission, old_total = ("10.0100", "5.00", "5.10") if same_costs else ("10.0000", "6.00", "6.10")
    old_fill = {"id": "old-fill", "ts_code": "600000.SH", "side": "BUY", "quantity": 100, "price": old_price, "executed_at": at,
                "price_snapshot_id": "f" * 64, "commission": old_commission, "transfer_fee": "0.10", "tax": "0", "total_fees": old_total}
    old_ledger = {"id": "old-cash", "trade_date": "2026-09-30", "event_time": at, "kind": "BUY", "amount": "-1006.10",
                  "cash_before": "10000", "cash_after": "8993.90", "fill_id": "old-fill", "currency": "CNY"}
    old_nav = {"id": "2026-09-30", "trade_date": "2026-09-30", "as_of": "2026-09-30T07:00:00Z",
               **{key: account.model_dump(mode="json")[key] for key in ("cash", "nav", "realized_pnl", "unrealized_pnl", "holdings")}}
    old_raw = {"identity": old_identity.model_dump(mode="json"), "status": "complete", "signals": [signal_row], "fills": [old_fill], "ledger": [old_ledger], "daily_nav": [old_nav]}
    mapping = HistoricalResultMapping(identity_pointer="/identity", status_pointer="/status", signals=table("signals", signal_fields),
        fills=table("fills", fill_fields), ledger=table("ledger", ledger_fields), daily_nav=table("daily_nav", nav_fields))
    new_ledger = _origin("new-ledger", {"ledger": [{**old_ledger, "id": "new-cash", "fill_id": fill_id}]})

    def cost_input(old: bool) -> tuple[Any, Any]:
        own_id = "old-fill" if old else fill_id
        material = _origin("old-cost-input" if old else "new-cost-input", {
            "fill_id": own_id, "order_input": {"side": "BUY", "reference_price": "10", "quantity": 100},
            "instrument_context": context.model_dump(mode="json"),
            "quote": {"snapshot_id": "f" * 64, "ts_code": "600000.SH", "reference_price": "10", "event_time": "2026-09-30T01:31:00Z", "available_at": "2026-09-30T01:31:00Z"},
        })
        reference = HistoricalCostInputReference(fill_id=own_id, object_key=material.object_key, content_sha256=material.content_sha256,
            fill_id_pointer="/fill_id", order_input_pointer="/order_input", instrument_context_pointer="/instrument_context", quote_pointer="/quote")
        return material, reference

    new_input, new_ref = cost_input(False)
    old_input, old_ref = cost_input(True)
    new = HistoricalSideEvidence(identity=identity, result_origin=_origin("new-result", native.model_dump(mode="json", exclude_computed_fields=True)),
        rules_origin=rules(identity, False), rule_sources=(*sources, semantic), inputs=(shared, new_input), cost_inputs=(new_ref,),
        ledger_origin=new_ledger, ledger_mapping=table("ledger", ledger_fields))
    old = HistoricalSideEvidence(identity=old_identity, result_origin=_origin("old-result", old_raw), rules_origin=rules(old_identity, True),
        rule_sources=(*sources, semantic), inputs=(shared, old_input), cost_inputs=(old_ref,), result_mapping=mapping)
    matches = tuple(HistoricalRowMatch(scope=scope, old_row_id=old_id, new_row_id=new_id) for scope, old_id, new_id in (
        ("signals", "old-signal", signal.signal_id), ("fills", "old-fill", fill_id), ("fees", "old-fill", fill_id),
        ("ledger", "old-cash", "new-cash"), ("daily_nav", "2026-09-30", "2026-09-30")))
    return Sample(native=native, new=new, old=old, matches=matches)


def _compare(sample: Sample) -> Any:
    from rquant.minute_historical_comparison import compare_frozen_executions

    return compare_frozen_executions(native_result=sample.native, new_evidence=sample.new, old_evidence=sample.old, matches=sample.matches)


def _edit_old(sample: Sample, edit: Any) -> Sample:
    raw = json.loads(sample.old.result_origin.payload())
    edit(raw)
    return replace(sample, old=sample.old.model_copy(update={"result_origin": _origin("old-result", raw)}))


def test_cost_differences_require_both_real_calculations_and_keep_originals() -> None:
    sample = _sample()
    result = _compare(sample)
    assert result.status == "diagnostic_complete"
    assert result.execution_complete and not result.formal_history_passed
    assert result.unexplained_count == 0 and result.same_basis_failures == () and result.unavailable_reasons == ()
    assert result.old_side.result_origin.payload() == sample.old.result_origin.payload()
    assert result.new_side.result_origin.content_sha256 == sample.new.result_origin.content_sha256
    explained = {(d.scope, d.field): d.explanation for d in result.differences}
    assert set(explained) == {("fills", "price"), ("fees", "commission"), ("fees", "total_fees")}
    price = explained["fills", "price"]
    assert (price.old_observed, price.new_observed, price.old_computed, price.new_computed, price.delta) == (
        Decimal("10"), Decimal("10.01"), Decimal("10"), Decimal("10.01"), Decimal("0.01"))
    commission = explained["fees", "commission"]
    assert commission.delta == Decimal("-1")
    assert commission.old_rule_source_sha256 != "0" * 64
    assert commission.old_input.content_sha256 == sample.old.inputs[1].content_sha256
    assert result.old_side.rows[-1].value("nav") == "9983.9"


def test_original_order_and_paper_quote_are_read_by_pointers_without_a_new_receipt() -> None:
    from rquant.minute_backtest_runner import MinuteRuntimeReplayResult
    from rquant.minute_historical_comparison import HistoricalColumnMapping, HistoricalCostInputReference
    from rquant.paper_signal_worker import PaperQuoteSnapshot
    from tests.paper_cost_fixtures import paper_instrument_context

    sample = _sample()
    quote = PaperQuoteSnapshot.model_validate_json(json.dumps({"ts_code": "600000.SH", "event_time": "2026-09-30T01:31:00Z",
        "available_at": "2026-09-30T01:31:00Z", "producer_commit": "a" * 40,
        "context": {"executable_price": "10", "acquisition_available_date": "2026-10-01", "instrument_context": paper_instrument_context().model_dump(mode="json")}}))
    native_raw = sample.native.model_dump(mode="json", exclude_computed_fields=True)
    native_raw["fills"][0]["price_snapshot_id"] = quote.snapshot_id
    native = MinuteRuntimeReplayResult.model_validate_json(json.dumps(native_raw))
    sample = replace(sample, native=native, new=sample.new.model_copy(update={"result_origin": _origin("new-result", native_raw)}))
    sample = _edit_old(sample, lambda raw: raw["fills"][0].update(price_snapshot_id=quote.snapshot_id))
    sides = []
    for old, evidence in ((False, sample.new), (True, sample.old)):
        original_fill = json.loads(sample.old.result_origin.payload())["fills"][0] if old else native.fills[0].model_dump(mode="json")
        own_id = "old-fill" if old else native.fills[0].fill_id
        original = _origin("old-cost-input" if old else "new-cost-input", {"fill": original_fill,
            "order": native.orders[0].model_dump(mode="json"), "quote": quote.model_dump(mode="json")})
        columns = lambda fields: tuple(HistoricalColumnMapping(field=name, pointer=pointer) for name, pointer in fields)
        reference = HistoricalCostInputReference(fill_id=own_id, object_key=original.object_key, content_sha256=original.content_sha256,
            fill_id_pointer="/fill/id" if old else "/fill/fill_id", order_input_pointer="", instrument_context_pointer="/quote/context/instrument_context", quote_pointer="/quote",
            order_columns=columns((("side", "/order/side"), ("quantity", "/order/quantity"), ("reference_price", "/quote/context/executable_price"))),
            quote_columns=columns((("snapshot_id", "/snapshot_id"), ("ts_code", "/ts_code"), ("reference_price", "/context/executable_price"), ("event_time", "/event_time"), ("available_at", "/available_at"))))
        sides.append(evidence.model_copy(update={"inputs": (evidence.inputs[0], original), "cost_inputs": (reference,)}))
    sample = replace(sample, new=sides[0], old=sides[1])
    result = _compare(sample)
    assert result.status == "diagnostic_complete" and result.unexplained_count == 0
    assert not result.formal_history_passed and len(result.differences) == 3
    assert result.new_side.inputs[1].payload() == sample.new.inputs[1].payload()


def test_wrong_observed_fee_is_not_explained_by_a_valid_rule_name() -> None:
    sample = _edit_old(_sample(), lambda raw: raw["fills"][0].update(commission="99", total_fees="99.10"))
    result = _compare(sample)
    assert result.status == "blocked"
    assert result.unexplained_count >= 2
    assert result.same_basis_failures
    assert all(d.explanation is None for d in result.differences if d.scope == "fees")


@pytest.mark.parametrize("nav", ["9983.900000000001", "9983.9000000000000000000000000001"])
def test_same_basis_failure_is_exact_and_cannot_use_an_explanation(nav: str) -> None:
    sample = _edit_old(_sample(same_costs=True), lambda raw: raw["daily_nav"][0].update(nav=nav))
    result = _compare(sample)
    assert result.status == "blocked" and result.unexplained_count == 1
    assert result.same_basis_failures
    assert result.differences[0].same_basis and result.differences[0].explanation is None
    assert result.differences[0].old_value.value == nav


def test_equal_but_inconsistent_observed_cash_rows_still_block() -> None:
    sample = _sample(same_costs=True)
    sample = _edit_old(sample, lambda raw: raw["ledger"][0].update(amount="-1"))
    new_ledger = json.loads(sample.new.ledger_origin.payload())
    new_ledger["ledger"][0]["amount"] = "-1"
    sample = replace(sample, new=sample.new.model_copy(update={"ledger_origin": _origin("new-ledger", new_ledger)}))
    result = _compare(sample)
    assert result.unexplained_count == 0 and result.same_basis_failures
    assert result.status == "blocked" and not result.formal_history_passed


def test_future_reference_quote_cannot_explain_cost_differences() -> None:
    sample = _sample()
    input_raw = json.loads(sample.old.inputs[1].payload())
    input_raw["quote"]["available_at"] = "2026-09-30T01:33:00Z"
    material = _origin("old-cost-input", input_raw)
    reference = sample.old.cost_inputs[0].model_copy(update={"content_sha256": material.content_sha256})
    sample = replace(sample, old=sample.old.model_copy(update={"inputs": (sample.old.inputs[0], material), "cost_inputs": (reference,)}))
    result = _compare(sample)
    assert result.status == "unavailable" and result.unexplained_count >= 3
    assert any("future" in reason for reason in result.unavailable_reasons)
    assert all(d.explanation is None for d in result.differences)


def test_v2_sell_lot_cannot_assume_the_sell_reference_is_the_entry_price() -> None:
    sample = _sample()
    sample = _edit_old(sample, lambda raw: raw["fills"][0].update(side="SELL"))
    input_raw = json.loads(sample.old.inputs[1].payload())
    input_raw["order_input"]["side"] = "SELL"
    material = _origin("old-cost-input", input_raw)
    reference = sample.old.cost_inputs[0].model_copy(update={"content_sha256": material.content_sha256})
    sample = replace(sample, old=sample.old.model_copy(update={"inputs": (sample.old.inputs[0], material), "cost_inputs": (reference,)}))
    result = _compare(sample)
    assert result.status == "unavailable" and result.unexplained_count > 0
    assert any("entry reference price" in reason for reason in result.unavailable_reasons)


def test_missing_actual_ledger_is_unavailable_and_never_generated_from_fills() -> None:
    sample = _sample()
    sample = replace(sample, new=sample.new.model_copy(update={"ledger_origin": None, "ledger_mapping": None}))
    result = _compare(sample)
    assert result.status == "unavailable" and not result.formal_history_passed
    assert result.unexplained_count > 0
    assert not any(row.scope == "ledger" for row in result.new_side.rows)
    assert any("ledger" in reason for reason in result.unavailable_reasons)
    assert sample.native.fills and any(row.scope == "ledger" for row in result.old_side.rows)


def test_empty_ledger_with_actual_fills_is_not_a_complete_flow_history() -> None:
    sample = _sample(same_costs=True)
    sample = _edit_old(sample, lambda raw: raw.update(ledger=[]))
    sample = replace(sample, new=sample.new.model_copy(update={"ledger_origin": _origin("new-ledger", {"ledger": []})}))
    result = _compare(sample)
    assert result.status == "unavailable" and result.unexplained_count > 0
    assert any("flow rows missing" in reason for reason in result.unavailable_reasons)


def test_missing_cost_context_keeps_observed_differences_unexplained() -> None:
    sample = _sample()
    sample = replace(sample, old=sample.old.model_copy(update={"cost_inputs": ()}))
    result = _compare(sample)
    assert result.status == "unavailable" and result.unexplained_count >= 3
    assert {(d.scope, d.field) for d in result.differences if d.kind == "observed_difference"} == {
        ("fills", "price"), ("fees", "commission"), ("fees", "total_fees")}
    assert all(d.explanation is None for d in result.differences)


def test_unknown_old_rule_source_cannot_explain_or_hide_observed_differences() -> None:
    sample = _sample()
    rules = json.loads(sample.old.rules_origin.payload())
    rules["cost_source_sha256"] = "0" * 64
    sample = replace(sample, old=sample.old.model_copy(update={"rules_origin": _origin("old-rules", rules)}))
    result = _compare(sample)
    assert result.status == "unavailable" and result.unexplained_count >= 3
    assert result.old_side.result_origin == sample.old.result_origin
    assert any("cost_source" in reason for reason in result.unavailable_reasons)


def test_unmatched_original_signal_is_preserved_and_blocks() -> None:
    sample = _edit_old(_sample(), lambda raw: raw["signals"].append({**raw["signals"][0], "id": "old-extra"}))
    result = _compare(sample)
    assert result.status == "unavailable" and result.unexplained_count > 0
    assert any(row.row_id == "old-extra" for row in result.old_side.rows)
    assert any(d.old_row_id == "old-extra" and d.kind == "missing_row" for d in result.differences)


def test_incomplete_execution_and_zero_difference_are_not_formal_passes() -> None:
    sample = _sample(same_costs=True)
    complete = _compare(sample)
    assert complete.status == "diagnostic_complete" and complete.unexplained_count == 0
    assert not complete.formal_history_passed
    native = sample.native.model_copy(update={"status": "incomplete", "incomplete_reasons": ("execution stopped",)})
    sample = replace(sample, native=native, new=sample.new.model_copy(update={
        "result_origin": _origin("new-result", native.model_dump(mode="json", exclude_computed_fields=True))}))
    incomplete = _compare(sample)
    assert incomplete.status == "execution_incomplete" and not incomplete.execution_complete
    assert not incomplete.formal_history_passed


@pytest.mark.parametrize("mutation", ["duplicate_keys", "duplicate_rows", "detached_native", "incomplete_mapping"])
def test_strict_adapters_reject_ambiguous_or_detached_material(mutation: str) -> None:
    sample = _sample()
    if mutation == "duplicate_keys":
        origin = _origin("old-result", b'{"status":"complete","status":"incomplete"}', raw=True).model_copy(update={"format": "json"})
        sample = replace(sample, old=sample.old.model_copy(update={"result_origin": origin}))
    elif mutation == "duplicate_rows":
        sample = _edit_old(sample, lambda raw: raw["fills"].append(raw["fills"][0].copy()))
    elif mutation == "detached_native":
        data = sample.native.model_dump(mode="json", exclude_computed_fields=True)
        data["fills"][0]["commission"] = "55"
        sample = replace(sample, new=sample.new.model_copy(update={"result_origin": _origin("new-result", data)}))
    else:
        mapping = sample.old.result_mapping
        table = mapping.fills.model_copy(update={"columns": mapping.fills.columns[:-1]})
        sample = replace(sample, old=sample.old.model_copy(update={"result_mapping": mapping.model_copy(update={"fills": table})}))
    result = _compare(sample)
    assert result.status == "unavailable" and result.unexplained_count > 0
    assert result.unavailable_reasons and not result.formal_history_passed
