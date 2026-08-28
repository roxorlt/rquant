"""Explicit v3 paper-cost fixtures used by broker-facing tests."""

from __future__ import annotations

from decimal import Decimal

from rquant.paper_broker import BrokerCostPolicy
from rquant.research_run_spec import (
    ExecutionCostSpec,
    InstrumentClassificationProvenance,
    InstrumentContext,
)


def paper_execution_cost_spec(
    *,
    commission_bps: Decimal = Decimal("3"),
    minimum_commission: Decimal = Decimal("5"),
    stamp_duty_bps: Decimal = Decimal("10"),
    transfer_fee_bps: Decimal = Decimal("0"),
    buy_slippage_bps: Decimal = Decimal("0"),
    sell_slippage_bps: Decimal = Decimal("0"),
    engine_version: str = "test-paper-cost-engine-v3",
    price_tick: Decimal = Decimal("0.0001"),
) -> ExecutionCostSpec:
    selectors = (
        ("cn-sse-a-share", "SSE"),
        ("cn-szse-a-share", "SZSE"),
    )
    return ExecutionCostSpec.model_validate(
        {
            "schema_version": 3,
            "cost_engine_version": engine_version,
            "instrument_selectors": [
                {
                    "selector_id": selector_id,
                    "market": "CN",
                    "exchange": exchange,
                    "instrument_class": "EQUITY",
                    "security_class": "A_SHARE",
                }
                for selector_id, exchange in selectors
            ],
            "commission_rules": [
                {
                    "rule_id": f"commission-{selector_id}",
                    "selector_id": selector_id,
                    "rate_bps": str(commission_bps),
                    "minimum_amount": str(minimum_commission),
                    "applies_to": "BOTH",
                }
                for selector_id, _exchange in selectors
            ],
            "transfer_fee_rules": [
                {
                    "rule_id": f"transfer-{selector_id}",
                    "selector_id": selector_id,
                    "rate_bps": str(transfer_fee_bps),
                    "minimum_amount": "0",
                    "applies_to": "BOTH",
                }
                for selector_id, _exchange in selectors
            ],
            "stamp_duty_rules": [
                {
                    "rule_id": f"stamp-{selector_id}",
                    "selector_id": selector_id,
                    "rate_bps": str(stamp_duty_bps),
                    "minimum_amount": "0",
                    "applies_to": "SELL",
                }
                for selector_id, _exchange in selectors
            ],
            "fee_notional_basis": "EXECUTED_NOTIONAL",
            "assessment_unit": "FILL",
            "slippage": {
                "owner": "shared_cost_engine",
                "buy_bps": str(buy_slippage_bps),
                "sell_bps": str(sell_slippage_bps),
                "price_tick": str(price_tick),
                "price_rounding": "HALF_UP",
            },
            "money": {"quantum": "0.01", "rounding": "HALF_UP"},
        }
    )


def paper_cost_policy(**kwargs: Decimal | str) -> BrokerCostPolicy:
    return BrokerCostPolicy.from_execution_cost_spec(paper_execution_cost_spec(**kwargs))


def paper_instrument_context(ts_code: str = "600000.SH") -> InstrumentContext:
    normalized = ts_code.upper()
    if normalized.endswith(".SH"):
        exchange = "SSE"
    elif normalized.endswith(".SZ"):
        exchange = "SZSE"
    else:
        raise ValueError("test paper context supports only .SH and .SZ A-share symbols")
    return InstrumentContext(
        ts_code=normalized,
        market="CN",
        exchange=exchange,
        instrument_class="EQUITY",
        security_class="A_SHARE",
        classification_provenance=InstrumentClassificationProvenance(
            reference_dataset="security_listing_status",
            reference_record_id="a" * 64,
            reference_generation_id="b" * 64,
        ),
    )
