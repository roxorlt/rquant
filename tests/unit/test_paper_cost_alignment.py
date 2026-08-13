from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest
from pydantic import ValidationError

import rquant.paper_broker as paper_broker_module
from rquant.paper_broker import (
    BrokerCostPolicy,
    BrokerExecutionContext,
    DuplicateExecutionConflictError,
    PaperBrokerReconciliationError,
    PaperBrokerStore,
)
from rquant.paper_contracts import (
    PaperAccountSnapshot,
    PaperOrderIntent,
    PaperOrderStatus,
    PaperOrderType,
    PaperRejectReason,
    PaperSide,
)
from rquant.research_run_spec import ExecutionCostSpec, InstrumentContext
from tests.paper_ledger_anchor_support import PaperLedgerTestAuthority

_ACCOUNT_ID = "paper-cost-alignment"
_BUY_TIME = datetime(2026, 8, 10, 1, 31, tzinfo=UTC)
_BUY_DATE = date(2026, 8, 10)
_NEXT_TRADE_DATE = date(2026, 8, 11)
_PRICE_SNAPSHOT_ID = "b" * 64
_PRODUCER_COMMIT = "c" * 40
_ANCHOR_NOW = datetime(2026, 8, 14, 1, 2, tzinfo=UTC)
_ANCHOR_MAX_AGE = timedelta(minutes=5)
_ANCHOR_FUTURE_SKEW = timedelta(seconds=30)


def _v3_spec(
    *,
    engine_version: str = "fixture-cost-engine-v3",
    buy_slippage_bps: str = "5",
) -> ExecutionCostSpec:
    return ExecutionCostSpec.model_validate(
        {
            "schema_version": 3,
            "cost_engine_version": engine_version,
            "instrument_selectors": [
                {
                    "selector_id": "cn-sse-a-share",
                    "market": "CN",
                    "exchange": "SSE",
                    "instrument_class": "EQUITY",
                    "security_class": "A_SHARE",
                }
            ],
            "commission_rules": [
                {
                    "rule_id": "commission-cn-sse",
                    "selector_id": "cn-sse-a-share",
                    "rate_bps": "17",
                    "minimum_amount": "1.50",
                    "applies_to": "BOTH",
                }
            ],
            "transfer_fee_rules": [
                {
                    "rule_id": "transfer-cn-sse",
                    "selector_id": "cn-sse-a-share",
                    "rate_bps": "3",
                    "minimum_amount": "0.07",
                    "applies_to": "BOTH",
                }
            ],
            "stamp_duty_rules": [
                {
                    "rule_id": "stamp-cn-sse",
                    "selector_id": "cn-sse-a-share",
                    "rate_bps": "29",
                    "minimum_amount": "0.11",
                    "applies_to": "SELL",
                }
            ],
            "fee_notional_basis": "EXECUTED_NOTIONAL",
            "assessment_unit": "FILL",
            "slippage": {
                "owner": "shared_cost_engine",
                "buy_bps": buy_slippage_bps,
                "sell_bps": "7",
                "price_tick": "0.01",
                "price_rounding": "HALF_UP",
            },
            "money": {
                "quantum": "0.01",
                "rounding": "HALF_UP",
            },
            "research_notional_per_trade": "1000",
        }
    )


def _context() -> dict[str, object]:
    return {
        "ts_code": "600000.SH",
        "market": "CN",
        "exchange": "SSE",
        "instrument_class": "EQUITY",
        "security_class": "A_SHARE",
        "classification_provenance": {
            "reference_dataset": "security_listing_status",
            "reference_record_id": "a" * 64,
            "reference_generation_id": "b" * 64,
        },
    }


def _paper_context() -> InstrumentContext:
    return InstrumentContext.model_validate(_context())


def _paper_policy(spec: ExecutionCostSpec | None = None) -> BrokerCostPolicy:
    return BrokerCostPolicy.from_execution_cost_spec(_v3_spec() if spec is None else spec)


def _paper_store(
    path: Path,
    *,
    spec: ExecutionCostSpec | None = None,
    initial_cash: Decimal = Decimal("10000.00"),
    anchor_authority: PaperLedgerTestAuthority | None = None,
    anchor_path: Path | None = None,
) -> PaperBrokerStore:
    return PaperBrokerStore(
        path,
        account_id=_ACCOUNT_ID,
        initial_cash=initial_cash,
        cost_policy=_paper_policy(spec),
        **(
            {}
            if anchor_authority is None or anchor_path is None
            else {
                "ledger_id": anchor_authority.ledger_id,
                "ledger_anchor_path": anchor_path,
                "ledger_anchor_verifier": anchor_authority.verifier,
            }
        ),
    )


def _paper_intent(
    *,
    signal_seed: str,
    side: PaperSide = PaperSide.BUY,
    quantity: int = 100,
    event_time: datetime = _BUY_TIME - timedelta(seconds=2),
    entry_signal_id: str | None = None,
    sell_quantity_authority: object | None = None,
    ts_code: str = "600000.SH",
) -> PaperOrderIntent:
    return PaperOrderIntent(
        signal_id=signal_seed * 64,
        entry_signal_id=entry_signal_id,
        sell_quantity_authority=sell_quantity_authority,
        account_id=_ACCOUNT_ID,
        ts_code=ts_code,
        side=side,
        order_type=PaperOrderType.MARKET,
        quantity=quantity,
        event_time=event_time,
        available_at=event_time + timedelta(seconds=1),
        expires_at=event_time + timedelta(minutes=5),
        earliest_execution_at=event_time + timedelta(seconds=1),
        price_snapshot_id=_PRICE_SNAPSHOT_ID,
        producer_commit=_PRODUCER_COMMIT,
    )


def _paper_quote(
    price: str,
    *,
    available_date: date | None = _NEXT_TRADE_DATE,
    executable_quantity: int | None = None,
) -> BrokerExecutionContext:
    return BrokerExecutionContext(
        executable_price=Decimal(price),
        acquisition_available_date=available_date,
        instrument_context=_paper_context(),
        **({} if executable_quantity is None else {"executable_quantity": executable_quantity}),
    )


def test_v3_cost_spec_identity_is_canonical_and_legacy_is_not_alignment_eligible() -> None:
    first = _v3_spec()
    same = _v3_spec()
    changed_engine = _v3_spec(engine_version="fixture-cost-engine-v3b")
    changed_research_topology = ExecutionCostSpec.model_validate(
        {
            **_v3_spec().model_dump(mode="json", exclude={"cost_spec_id"}),
            "research_notional_per_trade": "2000",
        }
    )
    legacy = ExecutionCostSpec(
        schema_version=2,
        commission_bps=Decimal("17"),
        stamp_duty_bps=Decimal("29"),
        transfer_fee_bps=Decimal("3"),
        slippage_bps=Decimal("5"),
        minimum_commission=Decimal("1.50"),
        research_notional_per_trade=Decimal("1000"),
    )

    assert first.cost_spec_id == same.cost_spec_id
    assert first.cost_spec_id != changed_engine.cost_spec_id
    assert first.cost_spec_id == changed_research_topology.cost_spec_id
    assert first.canonical_json().startswith('{"assessment_unit"')
    authority = ExecutionCostSpec.from_canonical_json(first.canonical_json())
    assert authority.cost_spec_id == first.cost_spec_id
    assert authority.research_notional_per_trade is None
    assert first.schema_version == 3
    assert first.is_alignment_eligible
    assert not legacy.is_alignment_eligible
    assert legacy.cost_spec_id is None


def test_paper_rejects_mismatched_instrument_context_before_ledger_mutation(tmp_path: Path) -> None:
    path = tmp_path / "paper.sqlite3"
    store = _paper_store(path)

    with pytest.raises(ValueError, match="instrument_context ts_code"):
        store.submit_intent(
            _paper_intent(signal_seed="a", ts_code="000001.SZ"),
            execution_id="e" * 64,
            decision_time=_BUY_TIME,
            trade_date=_BUY_DATE,
            quote=_paper_quote("10.00"),
        )

    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM paper_intent").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM paper_order").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM paper_fill").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM paper_execution_receipt").fetchone()[0] == 0


def test_paper_rejects_unclassified_context_before_ledger_mutation(tmp_path: Path) -> None:
    path = tmp_path / "paper.sqlite3"
    store = _paper_store(path)

    with pytest.raises(ValueError, match="trusted.*classification"):
        store.submit_intent(
            _paper_intent(signal_seed="a"),
            execution_id="e" * 64,
            decision_time=_BUY_TIME,
            trade_date=_BUY_DATE,
            quote=BrokerExecutionContext(
                executable_price=Decimal("10.00"),
                acquisition_available_date=_NEXT_TRADE_DATE,
                instrument_context=InstrumentContext(
                    ts_code="600000.SH",
                    market="CN",
                    exchange="SSE",
                    instrument_class="EQUITY",
                    security_class="A_SHARE",
                ),
            ),
        )

    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM paper_intent").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM paper_order").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM paper_fill").fetchone()[0] == 0


@pytest.mark.parametrize(
    ("ts_code", "exchange", "instrument_class", "security_class"),
    [
        ("510300.SH", "SSE", "FUND", "ETF"),
        ("113001.SH", "SSE", "BOND", "CONVERTIBLE"),
        ("159915.SZ", "SZSE", "FUND", "ETF"),
    ],
)
def test_paper_rejects_non_a_share_context_even_when_a_rule_would_match(
    tmp_path: Path,
    ts_code: str,
    exchange: str,
    instrument_class: str,
    security_class: str,
) -> None:
    payload = _v3_spec().model_dump(mode="json")
    payload.pop("cost_spec_id", None)
    selector_id = "attested-non-a-share"
    payload["instrument_selectors"].append(
        {
            "selector_id": selector_id,
            "market": "CN",
            "exchange": exchange,
            "instrument_class": instrument_class,
            "security_class": security_class,
        }
    )
    for rules_name in ("commission_rules", "transfer_fee_rules", "stamp_duty_rules"):
        copied = dict(payload[rules_name][0])
        copied["rule_id"] = f"{rules_name}-{selector_id}"
        copied["selector_id"] = selector_id
        payload[rules_name].append(copied)
    path = tmp_path / "paper.sqlite3"
    store = _paper_store(path, spec=ExecutionCostSpec.model_validate(payload))

    with pytest.raises(ValueError, match="A_SHARE"):
        store.submit_intent(
            _paper_intent(signal_seed="b", ts_code=ts_code),
            execution_id="f" * 64,
            decision_time=_BUY_TIME,
            trade_date=_BUY_DATE,
            quote=BrokerExecutionContext(
                executable_price=Decimal("10.00"),
                acquisition_available_date=_NEXT_TRADE_DATE,
                instrument_context=InstrumentContext(
                    ts_code=ts_code,
                    market="CN",
                    exchange=exchange,
                    instrument_class=instrument_class,
                    security_class=security_class,
                    classification_provenance={
                        "reference_dataset": "security_listing_status",
                        "reference_record_id": "a" * 64,
                        "reference_generation_id": "b" * 64,
                    },
                ),
            ),
        )

    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM paper_intent").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM paper_order").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM paper_fill").fetchone()[0] == 0


def test_shared_v3_calculator_returns_buy_sell_components_and_context_identity() -> None:
    from rquant.order_execution_costs import calculate_execution_costs

    spec = _v3_spec()
    buy = calculate_execution_costs(
        spec,
        {
            "side": "BUY",
            "reference_price": "10.00",
            "quantity": 100,
        },
        _context(),
    )
    sell = calculate_execution_costs(
        spec,
        {
            "side": "SELL",
            "reference_price": "11.00",
            "quantity": 100,
        },
        _context(),
    )

    assert buy.executed_price == Decimal("10.01")
    assert buy.executed_notional == Decimal("1001.00")
    assert buy.commission == Decimal("1.70")
    assert buy.transfer_fee == Decimal("0.30")
    assert buy.stamp_duty == Decimal("0.00")
    assert buy.slippage_amount == Decimal("1.00")
    assert buy.total_fees == Decimal("2.00")
    assert sell.executed_price == Decimal("10.99")
    assert sell.executed_notional == Decimal("1099.00")
    assert sell.commission == Decimal("1.87")
    assert sell.transfer_fee == Decimal("0.33")
    assert sell.stamp_duty == Decimal("3.19")
    assert sell.slippage_amount == Decimal("1.00")
    assert sell.total_fees == Decimal("5.39")
    assert buy.cost_spec_id == sell.cost_spec_id == spec.cost_spec_id
    assert buy.cost_context_fingerprint != sell.cost_context_fingerprint
    assert buy.selected_rule_ids == {
        "selector": "cn-sse-a-share",
        "commission": "commission-cn-sse",
        "transfer_fee": "transfer-cn-sse",
        "stamp_duty": "stamp-cn-sse",
    }


@pytest.mark.parametrize(
    ("price", "expected"),
    [("10.005", Decimal("10.01")), ("10.004", Decimal("10.00"))],
)
def test_v3_price_rounding_is_half_up(price: str, expected: Decimal) -> None:
    from rquant.order_execution_costs import calculate_execution_costs

    result = calculate_execution_costs(
        _v3_spec(buy_slippage_bps="0"),
        {"side": "BUY", "reference_price": price, "quantity": 100},
        _context(),
    )

    assert result.executed_price == expected


def test_v3_research_replay_uses_shared_costs_but_is_unbound_for_paper() -> None:
    from rquant.strategy_execution_costs import apply_round_trip_execution_costs

    result = apply_round_trip_execution_costs(
        pd.DataFrame(
            [
                {
                    "entry_price": 10.0,
                    "exit_price": 11.0,
                    "ret_pct": 10.0,
                    "instrument_context": _context(),
                }
            ]
        ),
        _v3_spec(),
    )

    row = result.iloc[0]
    assert row["research_quantity"] == 100
    assert row["buy_transfer_fee_amount"] == 0.30
    assert row["sell_transfer_fee_amount"] == 0.33
    assert row["buy_total_fees"] == 2.00
    assert row["sell_total_fees"] == 5.39
    assert not bool(row["paper_execution_comparable"])
    assert row["paper_execution_comparability_reason"] == "UNBOUND_RESEARCH_COST"
    assert row["execution_cost_spec_id"] == _v3_spec().cost_spec_id


def test_pure_v3_cost_math_match_is_explicitly_non_authoritative() -> None:
    from rquant.order_execution_costs import calculate_execution_costs
    from rquant.strategy_execution_costs import (
        ExecutionCostBindingEvidence,
        compare_execution_cost_math,
    )

    spec = _v3_spec()
    calculation = calculate_execution_costs(
        spec,
        {"side": "BUY", "reference_price": "10.00", "quantity": 100},
        _context(),
    )
    research = ExecutionCostBindingEvidence(
        provenance_state="KNOWN_V3",
        execution_cost_spec=spec,
        calculations=(calculation,),
    )
    match = compare_execution_cost_math(research, spec, (calculation,))
    assert match.matches
    assert match.reason == "EXACT_MATH_MATCH"
    assert not hasattr(match, "is_comparable")


def test_store_backed_comparator_rejects_all_caller_forged_paper_facts(
    tmp_path: Path,
) -> None:
    import rquant.strategy_execution_costs as strategy_costs
    from rquant.order_execution_costs import calculate_execution_costs
    from tests.paper_ledger_anchor_support import create_paper_ledger_test_authority

    spec = _v3_spec()
    calculation = calculate_execution_costs(
        spec,
        {"side": "BUY", "reference_price": "10.00", "quantity": 100},
        _context(),
    )
    research = strategy_costs.ExecutionCostBindingEvidence(
        provenance_state="KNOWN_V3",
        execution_cost_spec=spec,
        calculations=(calculation,),
    )

    issuer = getattr(
        strategy_costs,
        "_issue_reconciled_paper_execution_cost_binding",
        None,
    )
    assert issuer is None, "caller-accessible paper evidence issuer remains authoritative"
    assert not hasattr(strategy_costs, "PaperExecutionCostBindingExport")
    assert not hasattr(strategy_costs, "_VerifiedPaperExecutionCostBinding")
    assert not hasattr(strategy_costs, "_TRUSTED_PAPER_EXECUTION_BINDINGS")
    assert not hasattr(strategy_costs, "compare_execution_cost_bindings")
    assert not hasattr(PaperBrokerStore, "export_reconciled_execution_cost_binding")

    authority = create_paper_ledger_test_authority(
        tmp_path / "anchor-key",
        as_of=_ANCHOR_NOW,
        max_age=_ANCHOR_MAX_AGE,
        future_skew=_ANCHOR_FUTURE_SKEW,
    )
    anchor_path = tmp_path / "current-head-anchor.json"
    store = _paper_store(
        tmp_path / "paper.sqlite3",
        spec=spec,
        anchor_authority=authority,
        anchor_path=anchor_path,
    )
    assert not hasattr(research, "is_comparable")
    assert callable(store.compare_research_execution_costs)
    store.submit_intent(
        _paper_intent(signal_seed="a"),
        execution_id="1" * 64,
        decision_time=_BUY_TIME,
        trade_date=_BUY_DATE,
        quote=_paper_quote("10.00"),
    )
    unanchored = store.compare_research_execution_costs(
        research,
        account_id=_ACCOUNT_ID,
        execution_ids=("1" * 64,),
    )
    assert not unanchored.is_comparable
    assert unanchored.reason == "PAPER_LEDGER_RECONCILIATION_FAILED"

    store.account_authority_snapshot(
        as_of=_BUY_TIME + timedelta(seconds=1),
        market_prices={"600000.SH": calculation.executed_price},
        producer_commit=_PRODUCER_COMMIT,
    )
    still_unanchored = store.compare_research_execution_costs(
        research,
        account_id=_ACCOUNT_ID,
        execution_ids=("1" * 64,),
    )
    assert not still_unanchored.is_comparable
    assert still_unanchored.reason == "CURRENT_HEAD_UNANCHORED"
    authority.write_current_anchor(store.path, anchor_path, issued_at=_ANCHOR_NOW)
    exact = store.compare_research_execution_costs(
        research,
        account_id=_ACCOUNT_ID,
        execution_ids=("1" * 64,),
    )
    fabricated = calculation.model_copy(
        update={"executed_price": calculation.executed_price + Decimal("0.01")}
    )
    forged = store.compare_research_execution_costs(
        strategy_costs.ExecutionCostBindingEvidence(
            provenance_state="KNOWN_V3",
            execution_cost_spec=spec,
            calculations=(fabricated,),
        ),
        account_id=_ACCOUNT_ID,
        execution_ids=("1" * 64,),
    )

    assert exact.is_comparable
    assert exact.reason == "EXACT_V3_BOUND"
    assert exact.reconciliation_digest is not None
    assert exact.head_marker_fingerprint is not None
    assert not forged.is_comparable
    assert forged.reason == "RESOLVED_CALCULATION_MISMATCH"


def test_store_comparator_rejects_stale_signed_anchor(tmp_path: Path) -> None:
    from rquant.order_execution_costs import calculate_execution_costs
    from rquant.strategy_execution_costs import ExecutionCostBindingEvidence
    from tests.paper_ledger_anchor_support import create_paper_ledger_test_authority

    spec = _v3_spec()
    calculation = calculate_execution_costs(
        spec,
        {"side": "BUY", "reference_price": "10.00", "quantity": 100},
        _context(),
    )
    research = ExecutionCostBindingEvidence(
        provenance_state="KNOWN_V3",
        execution_cost_spec=spec,
        calculations=(calculation,),
    )
    authority = create_paper_ledger_test_authority(
        tmp_path / "anchor-key",
        as_of=_ANCHOR_NOW,
        max_age=_ANCHOR_MAX_AGE,
        future_skew=_ANCHOR_FUTURE_SKEW,
    )
    anchor_path = tmp_path / "current-head-anchor.json"
    store = _paper_store(
        tmp_path / "paper.sqlite3",
        spec=spec,
        anchor_authority=authority,
        anchor_path=anchor_path,
    )
    store.submit_intent(
        _paper_intent(signal_seed="a"),
        execution_id="1" * 64,
        decision_time=_BUY_TIME,
        trade_date=_BUY_DATE,
        quote=_paper_quote("10.00"),
    )
    store.account_authority_snapshot(
        as_of=_BUY_TIME + timedelta(seconds=1),
        market_prices={"600000.SH": calculation.executed_price},
        producer_commit=_PRODUCER_COMMIT,
    )
    authority.write_current_anchor(
        store.path,
        anchor_path,
        issued_at=_ANCHOR_NOW - _ANCHOR_MAX_AGE - timedelta(microseconds=1),
    )

    comparison = store.compare_research_execution_costs(
        research,
        account_id=_ACCOUNT_ID,
        execution_ids=("1" * 64,),
    )

    assert not comparison.is_comparable
    assert comparison.reason == "CURRENT_HEAD_UNANCHORED"


def test_rqs8_p1_007_anchor_requires_the_committed_live_financial_state_root(
    tmp_path: Path,
) -> None:
    from rquant.order_execution_costs import calculate_execution_costs
    from rquant.strategy_execution_costs import ExecutionCostBindingEvidence
    from tests.paper_ledger_anchor_support import create_paper_ledger_test_authority

    spec = _v3_spec()
    calculation = calculate_execution_costs(
        spec,
        {"side": "BUY", "reference_price": "10.00", "quantity": 100},
        _context(),
    )
    research = ExecutionCostBindingEvidence(
        provenance_state="KNOWN_V3",
        execution_cost_spec=spec,
        calculations=(calculation,),
    )
    authority = create_paper_ledger_test_authority(
        tmp_path / "anchor-key",
        as_of=_ANCHOR_NOW,
        max_age=_ANCHOR_MAX_AGE,
        future_skew=_ANCHOR_FUTURE_SKEW,
    )
    anchor_path = tmp_path / "current-head-anchor.json"
    store = _paper_store(
        tmp_path / "paper.sqlite3",
        spec=spec,
        anchor_authority=authority,
        anchor_path=anchor_path,
    )
    store.submit_intent(
        _paper_intent(signal_seed="a"),
        execution_id="1" * 64,
        decision_time=_BUY_TIME,
        trade_date=_BUY_DATE,
        quote=_paper_quote("10.00"),
    )
    store.account_authority_snapshot(
        as_of=_BUY_TIME + timedelta(seconds=1),
        market_prices={"600000.SH": calculation.executed_price},
        producer_commit=_PRODUCER_COMMIT,
    )
    authority.write_current_anchor(store.path, anchor_path, issued_at=_ANCHOR_NOW)
    signed_anchor_before = anchor_path.read_bytes()

    exact_before = store.compare_research_execution_costs(
        research,
        account_id=_ACCOUNT_ID,
        execution_ids=("1" * 64,),
    )
    assert exact_before.is_comparable
    assert exact_before.reason == "EXACT_V3_BOUND"
    assert exact_before.financial_state_digest is not None

    with sqlite3.connect(store.path) as connection:
        head_before = connection.execute(
            "SELECT head_marker_fingerprint, payload_json "
            "FROM paper_ledger_head_marker ORDER BY revision DESC LIMIT 1"
        ).fetchone()
        connection.execute(
            "UPDATE paper_account_authority SET producer_commit = ? WHERE account_id = ?",
            ("b" * 40, _ACCOUNT_ID),
        )
        head_after = connection.execute(
            "SELECT head_marker_fingerprint, payload_json "
            "FROM paper_ledger_head_marker ORDER BY revision DESC LIMIT 1"
        ).fetchone()

    assert head_after == head_before
    assert anchor_path.read_bytes() == signed_anchor_before
    stale_root = store.compare_research_execution_costs(
        research,
        account_id=_ACCOUNT_ID,
        execution_ids=("1" * 64,),
    )
    assert not stale_root.is_comparable
    assert stale_root.reason == "CURRENT_HEAD_UNANCHORED"
    assert stale_root.head_marker_fingerprint == exact_before.head_marker_fingerprint
    assert stale_root.financial_state_digest != exact_before.financial_state_digest

    authority.write_current_anchor(store.path, anchor_path, issued_at=_ANCHOR_NOW)
    exact_after = store.compare_research_execution_costs(
        research,
        account_id=_ACCOUNT_ID,
        execution_ids=("1" * 64,),
    )
    assert exact_after.is_comparable
    assert exact_after.reason == "EXACT_V3_BOUND"
    assert exact_after.financial_state_digest == stale_root.financial_state_digest


def test_rqs8_p1_007_anchor_rejects_synchronized_non_fifo_financial_state_tamper(
    tmp_path: Path,
) -> None:
    from rquant.order_execution_costs import calculate_execution_costs
    from rquant.strategy_execution_costs import ExecutionCostBindingEvidence
    from tests.paper_ledger_anchor_support import create_paper_ledger_test_authority

    spec = _v3_spec()
    authority = create_paper_ledger_test_authority(
        tmp_path / "anchor-key",
        as_of=_ANCHOR_NOW,
        max_age=_ANCHOR_MAX_AGE,
        future_skew=_ANCHOR_FUTURE_SKEW,
    )
    anchor_path = tmp_path / "current-head-anchor.json"
    store = _paper_store(
        tmp_path / "paper.sqlite3",
        spec=spec,
        anchor_authority=authority,
        anchor_path=anchor_path,
    )
    buy_intent = _paper_intent(signal_seed="a", quantity=200)
    buy = store.submit_intent(
        buy_intent,
        execution_id="1" * 64,
        decision_time=_BUY_TIME,
        trade_date=_BUY_DATE,
        quote=_paper_quote("10.00", executable_quantity=100),
    )
    store.apply_execution(
        buy.order_id,
        execution_id="2" * 64,
        executed_at=_BUY_TIME + timedelta(minutes=1),
        trade_date=_BUY_DATE,
        quantity=100,
        price_snapshot_id="d" * 64,
        quote=_paper_quote("20.00", executable_quantity=100),
    )
    sell_time = datetime(2026, 8, 11, 1, 31, tzinfo=UTC)
    sell_authority = store.sell_quantity_authority(
        exit_signal_id="c" * 64,
        entry_signal_id=buy_intent.signal_id,
        ts_code="600000.SH",
        action="REDUCE",
        tranche_fraction=Decimal("0.5"),
        decision_cutoff=sell_time,
        trade_date=_NEXT_TRADE_DATE,
    )
    sell = store.submit_intent(
        _paper_intent(
            signal_seed="c",
            side=PaperSide.SELL,
            quantity=100,
            event_time=sell_time - timedelta(seconds=2),
            entry_signal_id=buy_intent.signal_id,
            sell_quantity_authority=sell_authority,
        ),
        execution_id="3" * 64,
        decision_time=sell_time,
        trade_date=_NEXT_TRADE_DATE,
        quote=_paper_quote("30.00", available_date=None),
    )
    calculation = calculate_execution_costs(
        spec,
        {"side": "SELL", "reference_price": "30.00", "quantity": 100},
        _context(),
    )
    research = ExecutionCostBindingEvidence(
        provenance_state="KNOWN_V3",
        execution_cost_spec=spec,
        calculations=(calculation,),
    )
    store.account_authority_snapshot(
        as_of=sell_time + timedelta(seconds=1),
        market_prices={"600000.SH": calculation.executed_price},
        producer_commit=_PRODUCER_COMMIT,
    )
    authority.write_current_anchor(store.path, anchor_path, issued_at=_ANCHOR_NOW)

    with sqlite3.connect(store.path) as connection:
        connection.row_factory = sqlite3.Row
        head_before = connection.execute(
            "SELECT head_marker_fingerprint, payload_json "
            "FROM paper_ledger_head_marker ORDER BY revision DESC LIMIT 1"
        ).fetchone()
        lots = connection.execute(
            """
            SELECT lot_id, original_quantity, unit_cost
            FROM paper_lot
            WHERE account_id = ?
            ORDER BY buy_executed_at, buy_persisted_at, buy_fill_sequence, lot_id
            """,
            (_ACCOUNT_ID,),
        ).fetchall()
        consumption = connection.execute(
            "SELECT lot_id, quantity FROM paper_lot_consumption WHERE fill_id = ?",
            (store.fills(sell.order_id)[0].fill_id,),
        ).fetchone()
        account = connection.execute(
            "SELECT realized_pnl FROM broker_account WHERE account_id = ?",
            (_ACCOUNT_ID,),
        ).fetchone()
        authority_row = connection.execute(
            "SELECT state_fingerprint, snapshot_json FROM paper_account_authority "
            "WHERE account_id = ?",
            (_ACCOUNT_ID,),
        ).fetchone()
        assert head_before is not None
        assert len(lots) == 2
        assert consumption is not None and account is not None and authority_row is not None
        first_lot, second_lot = lots
        assert str(consumption["lot_id"]) == str(first_lot["lot_id"])

        immutable_trigger = str(
            connection.execute(
                "SELECT sql FROM sqlite_master WHERE type = 'trigger' "
                "AND name = 'paper_lot_consumption_row_immutable'"
            ).fetchone()[0]
        )
        connection.execute("DROP TRIGGER paper_lot_consumption_row_immutable")
        connection.execute(
            "UPDATE paper_lot_consumption SET lot_id = ?, unit_cost = ? "
            "WHERE fill_id = ? AND lot_id = ?",
            (
                str(second_lot["lot_id"]),
                str(second_lot["unit_cost"]),
                store.fills(sell.order_id)[0].fill_id,
                str(first_lot["lot_id"]),
            ),
        )
        connection.execute(immutable_trigger)
        connection.execute(
            "UPDATE paper_lot SET remaining_quantity = original_quantity WHERE lot_id = ?",
            (str(first_lot["lot_id"]),),
        )
        connection.execute(
            "UPDATE paper_lot SET remaining_quantity = 0 WHERE lot_id = ?",
            (str(second_lot["lot_id"]),),
        )
        adjusted_realized_pnl = Decimal(str(account["realized_pnl"])) - (
            Decimal(str(second_lot["unit_cost"])) - Decimal(str(first_lot["unit_cost"]))
        ) * int(consumption["quantity"])
        connection.execute(
            "UPDATE broker_account SET realized_pnl = ? WHERE account_id = ?",
            (str(adjusted_realized_pnl), _ACCOUNT_ID),
        )
        snapshot = PaperAccountSnapshot.model_validate_json(authority_row["snapshot_json"])
        updated_snapshot = PaperAccountSnapshot.model_validate(
            {
                **snapshot.model_dump(mode="python", exclude={"snapshot_id"}),
                "realized_pnl": adjusted_realized_pnl,
            }
        )
        connection.execute(
            """
            UPDATE paper_account_authority
            SET state_fingerprint = ?, snapshot_json = ?
            WHERE account_id = ?
            """,
            (
                paper_broker_module._account_state_fingerprint(updated_snapshot),
                updated_snapshot.model_dump_json(),
                _ACCOUNT_ID,
            ),
        )
        head_after = connection.execute(
            "SELECT head_marker_fingerprint, payload_json "
            "FROM paper_ledger_head_marker ORDER BY revision DESC LIMIT 1"
        ).fetchone()

    assert head_after == head_before
    comparison = store.compare_research_execution_costs(
        research,
        account_id=_ACCOUNT_ID,
        execution_ids=("3" * 64,),
    )
    assert not comparison.is_comparable

    authority.write_current_anchor(store.path, anchor_path, issued_at=_ANCHOR_NOW)
    freshly_anchored = store.compare_research_execution_costs(
        research,
        account_id=_ACCOUNT_ID,
        execution_ids=("3" * 64,),
    )
    assert not freshly_anchored.is_comparable
    assert freshly_anchored.reason == "PAPER_LEDGER_RECONCILIATION_FAILED"


def test_strict_v3_binding_comparator_has_machine_negative_reasons(tmp_path: Path) -> None:
    from rquant.order_execution_costs import calculate_execution_costs
    from rquant.strategy_execution_costs import (
        ExecutionCostBindingEvidence,
    )
    from tests.paper_ledger_anchor_support import create_paper_ledger_test_authority

    spec = _v3_spec()
    calculation = calculate_execution_costs(
        spec,
        {"side": "BUY", "reference_price": "10.00", "quantity": 100},
        _context(),
    )
    exact = ExecutionCostBindingEvidence(
        provenance_state="KNOWN_V3",
        execution_cost_spec=spec,
        calculations=(calculation,),
    )
    authority = create_paper_ledger_test_authority(
        tmp_path / "anchor-key",
        as_of=_ANCHOR_NOW,
        max_age=_ANCHOR_MAX_AGE,
        future_skew=_ANCHOR_FUTURE_SKEW,
    )
    anchor_path = tmp_path / "current-head-anchor.json"
    store = _paper_store(
        tmp_path / "paper.sqlite3",
        spec=spec,
        anchor_authority=authority,
        anchor_path=anchor_path,
    )
    store.submit_intent(
        _paper_intent(signal_seed="a"),
        execution_id="1" * 64,
        decision_time=_BUY_TIME,
        trade_date=_BUY_DATE,
        quote=_paper_quote("10.00"),
    )
    store.account_authority_snapshot(
        as_of=_BUY_TIME + timedelta(seconds=1),
        market_prices={"600000.SH": calculation.executed_price},
        producer_commit=_PRODUCER_COMMIT,
    )
    authority.write_current_anchor(store.path, anchor_path, issued_at=_ANCHOR_NOW)
    different_spec = _v3_spec(engine_version="different-engine-v3")
    different_calculation = calculate_execution_costs(
        different_spec,
        {"side": "BUY", "reference_price": "10.00", "quantity": 100},
        _context(),
    )
    different_context = calculate_execution_costs(
        spec,
        {"side": "BUY", "reference_price": "10.00", "quantity": 100},
        {**_context(), "ts_code": "600001.SH"},
    )
    tampered_rules = calculation.model_copy(
        update={
            "selected_rule_ids": {
                **calculation.selected_rule_ids,
                "commission": "tampered-commission-rule",
            }
        }
    )

    cases = (
        (
            ExecutionCostBindingEvidence(provenance_state="LEGACY_UNKNOWN"),
            "LEGACY_UNKNOWN_COST_PROVENANCE",
        ),
        (
            ExecutionCostBindingEvidence(provenance_state="V3_UNBOUND", rate_only=True),
            "RATE_ONLY_RESEARCH_COST",
        ),
        (
            ExecutionCostBindingEvidence(
                provenance_state="KNOWN_V3",
                execution_cost_spec=different_spec,
                calculations=(different_calculation,),
            ),
            "COST_SPEC_ID_MISMATCH",
        ),
        (
            ExecutionCostBindingEvidence(
                provenance_state="KNOWN_V3",
                execution_cost_spec=spec,
                calculations=(calculation, calculation),
            ),
            "FILL_TOPOLOGY_MISMATCH",
        ),
        (
            ExecutionCostBindingEvidence(
                provenance_state="KNOWN_V3",
                execution_cost_spec=spec,
                calculations=(different_context,),
            ),
            "INSTRUMENT_CONTEXT_MISMATCH",
        ),
        (
            ExecutionCostBindingEvidence(
                provenance_state="KNOWN_V3",
                execution_cost_spec=spec,
                calculations=(tampered_rules,),
            ),
            "SELECTED_RULE_MISMATCH",
        ),
    )

    for research, reason in cases:
        result = store.compare_research_execution_costs(
            research,
            account_id=_ACCOUNT_ID,
            execution_ids=("1" * 64,),
        )
        assert not result.is_comparable
        assert result.reason == reason

    exact_result = store.compare_research_execution_costs(
        exact,
        account_id=_ACCOUNT_ID,
        execution_ids=("1" * 64,),
    )
    assert exact_result.is_comparable


@pytest.mark.parametrize(
    ("applies_to", "side", "expected_commission"),
    [
        ("BUY", "BUY", Decimal("1.70")),
        ("BUY", "SELL", Decimal("0.00")),
        ("SELL", "BUY", Decimal("0.00")),
        ("SELL", "SELL", Decimal("1.87")),
        ("BOTH", "BUY", Decimal("1.70")),
        ("BOTH", "SELL", Decimal("1.87")),
    ],
)
def test_v3_rule_side_applicability_and_transfer_minimums(
    applies_to: str,
    side: str,
    expected_commission: Decimal,
) -> None:
    from rquant.order_execution_costs import calculate_execution_costs

    payload = _v3_spec().model_dump(mode="json")
    payload.pop("cost_spec_id", None)
    payload["commission_rules"][0]["applies_to"] = applies_to
    payload["transfer_fee_rules"][0]["minimum_amount"] = "0.00"
    zero_minimum = ExecutionCostSpec.model_validate(payload)
    amount = calculate_execution_costs(
        zero_minimum,
        {
            "side": side,
            "reference_price": "10.00" if side == "BUY" else "11.00",
            "quantity": 100,
        },
        _context(),
    )
    with_minimum = calculate_execution_costs(
        _v3_spec(),
        {
            "side": side,
            "reference_price": "10.00" if side == "BUY" else "11.00",
            "quantity": 100,
        },
        _context(),
    )

    assert amount.commission == expected_commission
    assert amount.transfer_fee == (Decimal("0.30") if side == "BUY" else Decimal("0.33"))
    assert with_minimum.transfer_fee == amount.transfer_fee


def test_v3_selector_requires_one_exact_non_overlapping_match() -> None:
    from rquant.order_execution_costs import calculate_execution_costs

    with pytest.raises(ValueError, match="no matching"):
        calculate_execution_costs(
            _v3_spec(),
            {"side": "BUY", "reference_price": "10.00", "quantity": 100},
            {**_context(), "exchange": "SZSE"},
        )

    overlapping = _v3_spec().model_dump(mode="json")
    overlapping.pop("cost_spec_id", None)
    overlapping["instrument_selectors"].append(
        {
            "selector_id": "cn-sse-a-share-duplicate",
            "market": "CN",
            "exchange": "SSE",
            "instrument_class": "EQUITY",
            "security_class": "A_SHARE",
        }
    )
    for field in ("commission_rules", "transfer_fee_rules", "stamp_duty_rules"):
        rule = dict(overlapping[field][0])
        rule["rule_id"] = f"{rule['rule_id']}-duplicate"
        rule["selector_id"] = "cn-sse-a-share-duplicate"
        overlapping[field].append(rule)
    with pytest.raises(ValidationError, match="overlap"):
        ExecutionCostSpec.model_validate(overlapping)


def test_paper_uses_shared_v3_receipts_and_fee_inclusive_accounting(tmp_path: Path) -> None:
    spec = _v3_spec()
    store = _paper_store(tmp_path / "paper.sqlite3", spec=spec)
    buy_intent = _paper_intent(signal_seed="a")

    buy_order = store.submit_intent(
        buy_intent,
        execution_id="1" * 64,
        decision_time=_BUY_TIME,
        trade_date=_BUY_DATE,
        quote=_paper_quote("10.00"),
    )
    buy_receipt = store.execution("1" * 64)
    assert buy_receipt is not None
    assert buy_order.status is PaperOrderStatus.FILLED
    assert buy_receipt.fill is not None
    assert buy_receipt.fill.price == Decimal("10.01")
    assert buy_receipt.fill.commission == Decimal("1.70")
    assert buy_receipt.fill.transfer_fee == Decimal("0.30")
    assert buy_receipt.fill.tax == Decimal("0.00")
    assert buy_receipt.fill.total_fees == Decimal("2.00")
    assert buy_receipt.fill.cost_spec_id == spec.cost_spec_id
    assert buy_receipt.fill.cost_context_fingerprint == buy_receipt.cost_context_fingerprint
    assert buy_receipt.cost_calculation is not None
    assert buy_receipt.cost_calculation.executed_price == buy_receipt.fill.price

    authority = store.sell_quantity_authority(
        exit_signal_id="d" * 64,
        entry_signal_id=buy_intent.signal_id,
        ts_code="600000.SH",
        action="S_INTENT",
        tranche_fraction=Decimal("1"),
        decision_cutoff=_BUY_TIME + timedelta(days=1),
        trade_date=_NEXT_TRADE_DATE,
    )
    sell_intent = _paper_intent(
        signal_seed="d",
        side=PaperSide.SELL,
        event_time=_BUY_TIME + timedelta(days=1, seconds=-2),
        entry_signal_id=buy_intent.signal_id,
        sell_quantity_authority=authority,
    )
    sell_order = store.submit_intent(
        sell_intent,
        execution_id="2" * 64,
        decision_time=_BUY_TIME + timedelta(days=1),
        trade_date=_NEXT_TRADE_DATE,
        quote=_paper_quote("11.00", available_date=None),
    )
    sell_receipt = store.execution("2" * 64)
    assert sell_receipt is not None
    assert sell_order.status is PaperOrderStatus.FILLED
    assert sell_receipt.fill is not None
    assert sell_receipt.fill.price == Decimal("10.99")
    assert sell_receipt.fill.commission == Decimal("1.87")
    assert sell_receipt.fill.transfer_fee == Decimal("0.33")
    assert sell_receipt.fill.tax == Decimal("3.19")
    assert sell_receipt.fill.total_fees == Decimal("5.39")
    assert sell_receipt.cost_calculation is not None
    from rquant.strategy_execution_costs import apply_round_trip_execution_costs

    research = apply_round_trip_execution_costs(
        pd.DataFrame(
            [
                {
                    "entry_price": 10.0,
                    "exit_price": 11.0,
                    "ret_pct": 10.0,
                    "instrument_context": _context(),
                }
            ]
        ),
        spec,
    ).iloc[0]
    assert Decimal(str(research["buy_executed_notional"])) == buy_receipt.fill.notional
    assert Decimal(str(research["sell_executed_notional"])) == sell_receipt.fill.notional
    assert Decimal(str(research["buy_commission_amount"])) == buy_receipt.fill.commission
    assert Decimal(str(research["sell_commission_amount"])) == sell_receipt.fill.commission
    assert Decimal(str(research["buy_transfer_fee_amount"])) == buy_receipt.fill.transfer_fee
    assert Decimal(str(research["sell_transfer_fee_amount"])) == sell_receipt.fill.transfer_fee
    assert Decimal(str(research["buy_total_fees"])) == buy_receipt.fill.total_fees
    assert Decimal(str(research["sell_total_fees"])) == sell_receipt.fill.total_fees
    assert research["execution_cost_spec_id"] == buy_receipt.cost_spec_id
    assert buy_receipt.cost_spec_id == sell_receipt.cost_spec_id
    assert research["buy_cost_context_fingerprint"] == buy_receipt.cost_context_fingerprint
    assert research["sell_cost_context_fingerprint"] == sell_receipt.cost_context_fingerprint
    snapshot = store.account_snapshot(
        as_of=_BUY_TIME + timedelta(days=1, minutes=1),
        market_prices={},
    )
    assert snapshot.cash == Decimal("10090.61")
    assert snapshot.realized_pnl == Decimal("90.61")
    with sqlite3.connect(store.path) as connection:
        unit_cost = Decimal(
            connection.execute("SELECT unit_cost FROM paper_lot LIMIT 1").fetchone()[0]
        )
    assert unit_cost * buy_receipt.fill.quantity == (
        buy_receipt.fill.notional + buy_receipt.fill.total_fees
    )
    assert store.reconcile().is_consistent


def test_paper_per_fill_minimum_and_reconciliation_are_not_order_aggregated(tmp_path: Path) -> None:
    spec_payload = _v3_spec(buy_slippage_bps="0").model_dump(mode="json")
    spec_payload.pop("cost_spec_id", None)
    spec_payload["commission_rules"][0].update({"rate_bps": "1", "minimum_amount": "1.50"})
    spec_payload["transfer_fee_rules"][0].update({"rate_bps": "0", "minimum_amount": "0.07"})
    spec = ExecutionCostSpec.model_validate(spec_payload)

    one_fill = _paper_store(tmp_path / "one.sqlite3", spec=spec)
    one_fill.submit_intent(
        _paper_intent(signal_seed="a", quantity=200),
        execution_id="1" * 64,
        decision_time=_BUY_TIME,
        trade_date=_BUY_DATE,
        quote=_paper_quote("10.00"),
    )
    one_total = one_fill.fills()[0].total_fees

    split = _paper_store(tmp_path / "split.sqlite3", spec=spec)
    order = split.submit_intent(
        _paper_intent(signal_seed="b", quantity=200),
        execution_id="2" * 64,
        decision_time=_BUY_TIME,
        trade_date=_BUY_DATE,
        quote=_paper_quote("10.00", executable_quantity=100),
    )
    split.apply_execution(
        order.order_id,
        execution_id="3" * 64,
        executed_at=_BUY_TIME + timedelta(minutes=1),
        trade_date=_BUY_DATE,
        quantity=100,
        quote=_paper_quote("10.00", executable_quantity=100),
        price_snapshot_id="4" * 64,
    )
    split_total = sum((fill.total_fees for fill in split.fills()), Decimal("0"))

    assert one_total == Decimal("1.57")
    assert split_total == Decimal("3.14")
    assert one_total != split_total
    assert one_fill.reconcile().is_consistent
    assert split.reconcile().is_consistent


def test_paper_v3_insufficient_cash_t_plus_one_and_cost_evidence_retry(tmp_path: Path) -> None:
    insufficient = _paper_store(tmp_path / "cash.sqlite3", initial_cash=Decimal("1002.99"))
    rejected = insufficient.submit_intent(
        _paper_intent(signal_seed="a"),
        execution_id="1" * 64,
        decision_time=_BUY_TIME,
        trade_date=_BUY_DATE,
        quote=_paper_quote("10.00"),
    )
    assert rejected.reject_reason is PaperRejectReason.INSUFFICIENT_CASH
    assert insufficient.account_snapshot(as_of=_BUY_TIME, market_prices={}).cash == Decimal(
        "1002.99"
    )

    store = _paper_store(tmp_path / "retry.sqlite3")
    intent = _paper_intent(signal_seed="b")
    first = store.submit_intent(
        intent,
        execution_id="2" * 64,
        decision_time=_BUY_TIME,
        trade_date=_BUY_DATE,
        quote=_paper_quote("10.00"),
    )
    same = store.submit_intent(
        intent,
        execution_id="2" * 64,
        decision_time=_BUY_TIME,
        trade_date=_BUY_DATE,
        quote=_paper_quote("10.00"),
    )
    with pytest.raises(DuplicateExecutionConflictError, match="immutable content"):
        store.submit_intent(
            intent,
            execution_id="2" * 64,
            decision_time=_BUY_TIME,
            trade_date=_BUY_DATE,
            quote=_paper_quote("10.01"),
        )
    assert same == first

    authority = store.sell_quantity_authority(
        exit_signal_id="e" * 64,
        entry_signal_id=intent.signal_id,
        ts_code="600000.SH",
        action="S_INTENT",
        tranche_fraction=Decimal("1"),
        decision_cutoff=_BUY_TIME,
        trade_date=_BUY_DATE,
    )
    t_plus_one = store.submit_intent(
        _paper_intent(
            signal_seed="e",
            side=PaperSide.SELL,
            event_time=_BUY_TIME - timedelta(seconds=2),
            entry_signal_id=intent.signal_id,
            sell_quantity_authority=authority,
        ),
        execution_id="3" * 64,
        decision_time=_BUY_TIME,
        trade_date=_BUY_DATE,
        quote=_paper_quote("11.00", available_date=None),
    )
    assert t_plus_one.reject_reason is PaperRejectReason.T_PLUS_ONE


@pytest.mark.parametrize(
    ("column", "value"),
    (
        ("commission", "99.99"),
        ("transfer_fee", "99.99"),
        ("tax", "99.99"),
        ("total_fees", "99.99"),
        ("cost_spec_id", "f" * 64),
        ("cost_spec_schema_version", "2"),
        ("cost_context_fingerprint", "d" * 64),
        ("cost_provenance_state", "LEGACY_UNKNOWN"),
    ),
)
def test_paper_v3_tampered_fee_or_identity_fails_reconciliation(
    tmp_path: Path,
    column: str,
    value: str,
) -> None:
    path = tmp_path / "paper.sqlite3"
    store = _paper_store(path)
    store.submit_intent(
        _paper_intent(signal_seed="a"),
        execution_id="1" * 64,
        decision_time=_BUY_TIME,
        trade_date=_BUY_DATE,
        quote=_paper_quote("10.00"),
    )

    with sqlite3.connect(path) as connection:
        trigger_sql = connection.execute(
            "SELECT sql FROM sqlite_master "
            "WHERE type = 'trigger' AND name = 'paper_fill_row_immutable'"
        ).fetchone()[0]
        connection.execute("DROP TRIGGER paper_fill_row_immutable")
        connection.execute(f"UPDATE paper_fill SET {column} = ?", (value,))
        connection.execute(trigger_sql)
    with pytest.raises(PaperBrokerReconciliationError, match="receipt|cost"):
        store.reconcile()
