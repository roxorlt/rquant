"""Concrete fixed-function declarations for exact server-generated template IDs."""

from __future__ import annotations

import re

from rquant.definition_registry import StrategyExitEligibility, StrategyExitPriceBasis, StrategyExitRule, StrategyPercentStop, StrategySellTranche, StrategyStructureStop, StrategyTrailingTakeProfit, TrustedFeatureImplementation, TrustedStrategyImplementation
from rquant.feature_contracts import FeatureContract, FeatureDefinition
from rquant.runtime_contracts import canonical_sha256
from rquant.strategy_template import EXIT_REASONS, FEATURE_NAMES, TEMPLATE_CONTRACT, TEMPLATE_FEATURE_CONTRACT, TEMPLATE_ID_PATTERN
from rquant.strategy_template_execution import TemplateEntryEvidence, TemplatePosition, TemplatePrice, strategy_template_entry, strategy_template_exit, strategy_template_feature, strategy_template_runtime


def strategy_template_feature_contract(*, producer_commit: str) -> FeatureContract:
    return FeatureContract(contract_id=TEMPLATE_FEATURE_CONTRACT, version=1, producer_commit=producer_commit, features=tuple(FeatureDefinition(name=name, dtype="object", source_datasets=("strategy_template_input",), lookback=0, pit_rule="trusted template input evidence available_at <= decision_time", price_basis="raw", availability_contract={"source_available_at_basis": "per_candidate_source_available_at", "max_delay_seconds": 120, "missing_policy": "fail_closed", "late_policy": "fail_closed", "decision_visibility_gate": "available_at_lte_decision_time"}) for name in FEATURE_NAMES))


def template_executables(strategy_ids: tuple[str, ...]) -> tuple[tuple[TrustedFeatureImplementation, ...], tuple[TrustedStrategyImplementation, ...]]:
    if len(set(strategy_ids)) != len(strategy_ids) or any(re.fullmatch(TEMPLATE_ID_PATTERN, item) is None for item in strategy_ids):
        raise ValueError("trusted template IDs must be exact and unique")
    features = tuple(TrustedFeatureImplementation(feature_name=name, implementation_version=TEMPLATE_CONTRACT, evaluator=strategy_template_feature) for name in FEATURE_NAMES)
    exits = tuple(StrategyExitRule(event=reason, fill_event=f"{reason}_filled", action="s_intent", evaluator_id="rquant.strategy_template_execution:strategy_template_exit", eligibility=StrategyExitEligibility(settlement_rule="a_share_t_plus_one", minimum_holding_trading_sessions=1, same_day_sell_allowed=False, sellable_position_required=True), price_basis=StrategyExitPriceBasis(adjustment_basis="raw", decision_price="minute_close", execution_price="next_minute_open"), structure_stop=StrategyStructureStop(reference="signal_support", buffer_bps=0), percent_stop=StrategyPercentStop(maximum_loss_bps=5000, acts_as_fallback=True), trailing_take_profit=StrategyTrailingTakeProfit(activation_gain_bps=1, retracement_bps=10000, high_watermark="eligible_intraday_high"), sell_tranche=StrategySellTranche(sequence=index + 1, position_fraction=1.0, reevaluate_after_fill=False, terminal_after_fill=True)) for index, reason in enumerate(EXIT_REASONS))
    candidate_hash = canonical_sha256({"contract": TEMPLATE_CONTRACT, "entry": TemplateEntryEvidence.model_json_schema(), "position": TemplatePosition.model_json_schema(), "price": TemplatePrice.model_json_schema()})
    strategies = tuple(TrustedStrategyImplementation(strategy_id=strategy_id, implementation_version=TEMPLATE_CONTRACT, candidate_schema_fingerprint=candidate_hash, entry_evaluator=strategy_template_entry, exit_evaluator=strategy_template_exit, runtime_evaluator=strategy_template_runtime, entry_event="entry_filled", exit_rules=exits) for strategy_id in strategy_ids)
    return features, strategies
