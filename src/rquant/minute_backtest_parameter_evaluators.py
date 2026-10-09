"""Pure parameter evaluators; entry and risk math remain with their owners."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import timedelta, time
from zoneinfo import ZoneInfo

import pandas as pd

from rquant.auction_gap_strategy import auction_candidate_mask, evaluate_auction_gap_entry
from rquant.growth_board_surge_strategy import (
    evaluate_growth_entry, passes_growth_board_filter, passes_growth_fresh_filter,
    passes_growth_listing_filter, prior_days_had_surge,
)
from rquant.minute_backtest_parameter_features import (
    PARAMETER_BAR_FEATURE, PARAMETER_CANDIDATE_FEATURE, PARAMETER_LIFECYCLE_FEATURE,
    MinuteParameterBar, MinuteParameterCandidate, MinuteParameterLifecycle,
)
from rquant.minute_backtest_parameters import (
    MinuteAuctionGapParameters, MinuteGrowthParameters, MinuteNShapeParameters, MinuteParameterSet,
)
from rquant.minute_replay import evaluate_n_shape_entry
from rquant.paper import check_position_exit
from rquant.signal_contracts import SignalAction, _thaw_json
from rquant.strategy_runner import StrategyCandidateState, StrategyDecision
from rquant.strategy_spec import StrategyLifecycleState, StrategySpec
from rquant.strict_json import strict_json_loads
from rquant.volume_profile import build_volume_profile_risk_plan

_SHANGHAI = ZoneInfo("Asia/Shanghai")


def parameter_intent_validity_seconds(recipe: MinuteParameterSet) -> int:
    minutes = int(recipe.parameters.freq.removesuffix("min"))
    return 120 if minutes == 1 else minutes * 60 + 120


def _parameters(spec: StrategySpec, state: StrategyCandidateState) -> MinuteParameterSet:
    raw = spec.parameters.get("minute_parameter_set_json")
    if not isinstance(raw, str):
        raise ValueError("parameter evaluator requires the complete parameter JSON")
    recipe = MinuteParameterSet.model_validate(strict_json_loads(raw))
    if (raw != recipe.model_dump_json() or spec.strategy_id != recipe.definition_id
            or spec.version != recipe.definition_version
            or spec.parameters.get("minute_parameter_set_hash") != recipe.fingerprint
            or spec.parameters.get("intent_validity_seconds") != parameter_intent_validity_seconds(recipe)
            or spec.parameters.get("intent_validity_basis") != "frequency_plus_original_120s_margin"
            or state.strategy_spec_fingerprint != spec.spec_fingerprint):
        raise ValueError("parameter evaluator identity differs from full immutable spec")
    return recipe


def _text(features: Mapping[str, object], name: str) -> str:
    raw = features.get(name)
    if not isinstance(raw, str):
        raise ValueError(f"parameter evaluator requires full typed {name}")
    return raw


def _visible_inputs(recipe: MinuteParameterSet, state: StrategyCandidateState,
                    features: Mapping[str, object]) -> tuple[MinuteParameterCandidate, MinuteParameterBar]:
    candidate = MinuteParameterCandidate.model_validate(strict_json_loads(_text(features, PARAMETER_CANDIDATE_FEATURE)))
    bar = MinuteParameterBar.model_validate(strict_json_loads(_text(features, PARAMETER_BAR_FEATURE)))
    if (candidate.parameter_hash != recipe.fingerprint or bar.parameter_hash != recipe.fingerprint
            or candidate.family != recipe.parameters.family or bar.frequency != recipe.parameters.freq
            or candidate.ts_code != state.candidate_id or bar.ts_code != state.candidate_id
            or candidate.available_at > bar.decision_cutoff):
        raise ValueError("parameter PIT feature identity differs from full spec and candidate")
    if any(features.get(key) != value for key, value in (("latest_close", bar.close),
            ("session_low", bar.session_low), ("session_high", bar.session_high))):
        raise ValueError("parameter scalar feature differs from full PIT observation")
    if candidate.study_binding != (None if bar.study_decision is None else bar.study_decision.binding):
        raise ValueError("parameter study candidate and full dynamic selection proof differ")
    return candidate, bar


def _transition(event: str, state: StrategyCandidateState, to_state: StrategyLifecycleState) -> StrategyDecision:
    return StrategyDecision(event=event, expected_from_state=state.state, expected_to_state=to_state,
        expected_action=None)


def parameter_entry_evaluator(spec: StrategySpec, state: StrategyCandidateState,
                              features: Mapping[str, object]) -> StrategyDecision | None:
    recipe = _parameters(spec, state)
    if state.state is StrategyLifecycleState.ARMED:
        status = features.get("entry_fill_status")
        if status in (None, "pending"):
            return None
        if status == "filled":
            return _transition("entry_filled", state, StrategyLifecycleState.HOLDING)
        if status == "rejected":
            return _transition("entry_rejected", state, StrategyLifecycleState.TERMINAL)
        raise ValueError("parameter entry fill status is invalid")
    if state.state is not StrategyLifecycleState.IDLE:
        return None
    candidate, bar = _visible_inputs(recipe, state, features)
    decision = parameter_entry_decision(recipe, candidate, bar)
    if decision is None or candidate.study_binding is None:
        return decision
    if bar.study_decision is None:
        raise ValueError("study entry lacks its complete dynamic prefix proof")
    selected = next((row for row in bar.study_decision.selected if row.ts_code == candidate.ts_code), None)
    if selected is None:
        return None
    return StrategyDecision.model_validate(decision.model_dump(mode="python") | {
        "evidence": _thaw_json(decision.evidence) | {"minute_study_binding_hash": candidate.study_binding.binding_hash,
            "minute_study_id": candidate.study_binding.study_id,
            "minute_study_selection": selected.model_dump(mode="json"),
            "minute_study_prefix": bar.study_decision.model_dump_json()}})


def parameter_entry_decision(recipe: MinuteParameterSet, candidate: MinuteParameterCandidate,
                             bar: MinuteParameterBar) -> StrategyDecision | None:
    """The original parameter entry owner, before any per-run TopN restriction."""
    if bar.event_time.astimezone(_SHANGHAI).date() != candidate.trade_date:
        return None
    config = recipe.parameters
    moment = bar.event_time.astimezone(_SHANGHAI).replace(tzinfo=None)
    score = None
    risk_plan = None
    if isinstance(config, MinuteNShapeParameters):
        pools = {"n-shape-pool1": {"pool1"}, "n-shape-pool2": {"pool2"}, "n-shape-combined": {"pool1", "pool2"}}
        if candidate.pool not in pools[config.preset_name]:
            return None
        check = evaluate_n_shape_entry(config=config.owner_config(), quote_time=moment,
            latest_price=bar.close, bar_low=bar.low, session_low=bar.session_low, session_high=bar.session_high,
            t_close=candidate.t_close, t_high=candidate.t_high, vwap=bar.vwap,
            minute_amount=bar.minute_amount, prior_amounts=bar.prior_amounts,
            first_signal_time=None if bar.first_signal_time is None else bar.first_signal_time.astimezone(_SHANGHAI).replace(tzinfo=None),
            static_factors=candidate.static_factors)
        if not check.eligible:
            return None
        if any(profile.lookback_days not in config.volume_profile.lookback_days for profile in candidate.volume_profiles):
            raise ValueError("volume profile differs from complete selected lookbacks")
        risk_plan = build_volume_profile_risk_plan(list(candidate.volume_profiles),
            entry_price=bar.close * (1 + config.paper.entry_slippage_pct), config=config.volume_profile)
        if not risk_plan.entry_allowed:
            return None
        score = check.factor_score
    elif isinstance(config, MinuteAuctionGapParameters):
        if candidate.auction_prev5_count != 5:
            return None
        causal_candidate_config = config.owner_config().auction_config().model_copy(update={"require_next_day": False})
        if not bool(auction_candidate_mask(pd.DataFrame([{
                "entry_price": candidate.auction_price, "pre_close": candidate.t_close, "pre_high": candidate.t_high,
                "auction_vol_ratio_5d": candidate.auction_vol_ratio_5d, "limit_up_price": candidate.limit_up_price,
                "is_st": candidate.is_st, "name": candidate.name}]), causal_candidate_config).iloc[0]):
            return None
        check = evaluate_auction_gap_entry(config=config.owner_config(), quote_time=moment,
            latest_price=bar.close, session_low=bar.session_low, session_high=bar.session_high,
            auction_price=candidate.auction_price, limit_up_price=candidate.limit_up_price, vwap=bar.vwap)
        if not check.eligible:
            return None
        if config.factor_score_threshold is not None:
            from rquant.auction_gap_strategy import AUCTION_GAP_B_V1_SCORE_TERMS
            from rquant.topn_selection import score_feature_terms

            values = {"auction_vol_ratio_5d": candidate.auction_vol_ratio_5d,
                "gap_pct_close": candidate.auction_gap_pct_close,
                "vwap_position": bar.close/check.price_floor if check.price_floor > 0 else None,
                "limit_progress": check.limit_progress, "support_ok": int(check.support_ok),
                "rel_amount_same_minute_20d": bar.compact_relative_features.get("signal_rel_amount_same_minute"),
                "amount_accel_5m": bar.compact_relative_features.get("signal_amount_accel_5m")}
            score = score_feature_terms(values, AUCTION_GAP_B_V1_SCORE_TERMS)
            if score < config.factor_score_threshold:
                return None
    elif isinstance(config, MinuteGrowthParameters):
        owner = config.owner_config()
        if config.min_listing_trading_days > 0:
            if candidate.listed_trading_days is None:
                raise ValueError("growth parameter source lacks the original listed-day count")
            if not passes_growth_listing_filter(candidate.listed_trading_days, owner):
                return None
        if config.require_fresh_surge:
            if candidate.prior_nonmissing_volume_ratios is None:
                raise ValueError("growth parameter source lacks the complete original prior-volume query")
            prior = prior_days_had_surge(candidate.prior_nonmissing_volume_ratios[:config.fresh_lookback_days],
                lookback_days=config.fresh_lookback_days, max_prior_volume_ratio=config.fresh_max_prior_volume_ratio)
            if not passes_growth_fresh_filter(prior, owner):
                return None
        board_strength = {"board_gap_up_ratio": candidate.board_gap_up_ratio,
            "board_auction_amount_ratio": candidate.board_auction_amount_ratio}
        if not passes_growth_board_filter(board_strength, owner):
            return None
        check = evaluate_growth_entry(config=config.owner_config(), quote_time=moment, latest_price=bar.close,
            limit_up_price=candidate.limit_up_price, vwap=bar.vwap,
            features=dict(bar.compact_relative_features), inner_outer_ratio=bar.inner_outer_ratio,
            static_factors=dict(candidate.static_factors), board_strength=board_strength)
        if not check.eligible:
            return None
        score = check.factor_score
    else:
        raise TypeError("unknown minute parameter math owner")
    return StrategyDecision(event="entry_ready", expected_from_state=StrategyLifecycleState.IDLE,
        expected_to_state=StrategyLifecycleState.ARMED, expected_action=SignalAction.B_INTENT,
        action=SignalAction.B_INTENT, reason_codes=("legacy_parameter_entry", config.family),
        expires_after=timedelta(seconds=parameter_intent_validity_seconds(recipe)), evidence={
            "minute_parameter_set_hash": recipe.fingerprint,
            "minute_parameter_set_json": recipe.model_dump_json(),
            PARAMETER_CANDIDATE_FEATURE: candidate.model_dump_json(),
            PARAMETER_BAR_FEATURE: bar.model_dump_json(), "latest_close": bar.close,
            "t_close_session_raw": candidate.t_close, "t_high_session_raw": candidate.t_high,
            "factor_score": score, "risk_plan": None if risk_plan is None else risk_plan.model_dump(mode="json"),
            "decision_basis": "visible_bar_close", "execution_basis": "original_pit_paper_broker"})


def parameter_exit_evaluator(spec: StrategySpec, state: StrategyCandidateState,
                             features: Mapping[str, object]) -> StrategyDecision | None:
    recipe = _parameters(spec, state)
    if state.state is not StrategyLifecycleState.HOLDING:
        return None
    if features.get("position_closed") is True:
        if features.get("remaining_position_fraction") != 0.0 or features.get("exit_execution_status") != "filled":
            raise ValueError("parameter terminal transition requires the original verified fill")
        return _transition("exit_filled", state, StrategyLifecycleState.TERMINAL)
    if features.get("exit_execution_status") == "pending":
        return None
    candidate, bar = _visible_inputs(recipe, state, features)
    from rquant.minute_backtest_parameter_features import MinuteParameterLifecycles

    complete_lifecycles = MinuteParameterLifecycles.model_validate(strict_json_loads(_text(features, PARAMETER_LIFECYCLE_FEATURE)))
    matches = tuple(item for item in complete_lifecycles.positions if item.candidate_state_key == state.state_key)
    if len(matches) != 1:
        raise ValueError("parameter exit lacks its exact original candidate occurrence")
    lifecycle = matches[0]
    if (lifecycle.parameter_hash != recipe.fingerprint or lifecycle.bar != bar
            or lifecycle.position.ts_code != state.candidate_id or features.get("position_sellable") != lifecycle.sellable):
        raise ValueError("parameter lifecycle differs from the original visible broker and market")
    if not lifecycle.sellable or lifecycle.holding_trading_sessions < 1:
        return None
    moment = bar.event_time.astimezone(_SHANGHAI).replace(tzinfo=None)
    # This structural quote is the same original Paper QuoteLike input. The
    # authoritative producer binds its full bar and rebuilt original position.
    from rquant.minute_replay import _MinuteQuote

    exited = check_position_exit(lifecycle.position, _MinuteQuote(ts_code=bar.ts_code, price=bar.close, high=bar.high, low=bar.low),
        moment, recipe.parameters.paper)
    reason = lifecycle.auction_exit_reason or (None if exited is None else exited.exit_reason)
    held = lifecycle.holding_trading_sessions
    cutoff_local = bar.decision_cutoff.astimezone(_SHANGHAI)
    if reason is None and held >= lifecycle.max_hold_days and cutoff_local.time().replace(tzinfo=None) == time(15):
        reason = f"time_{held}d"
    if reason is None:
        return None
    return StrategyDecision(event="exit", expected_from_state=state.state,
        expected_to_state=StrategyLifecycleState.HOLDING, expected_action=SignalAction.S_INTENT,
        action=SignalAction.S_INTENT, reason_codes=("legacy_parameter_exit", reason),
        expires_after=timedelta(seconds=parameter_intent_validity_seconds(recipe)), evidence={"minute_parameter_set_hash": recipe.fingerprint,
            "minute_parameter_set_json": recipe.model_dump_json(), "exit_reason": reason,
            PARAMETER_LIFECYCLE_FEATURE: lifecycle.model_dump_json(),
            "original_risk_exit": None if exited is None else exited.model_dump(mode="json"),
            "sell_tranche_fraction": 1.0, "sell_tranche_sequence": 1,
            "remaining_position_fraction": features.get("remaining_position_fraction"),
            "position": lifecycle.position.model_dump(mode="json"), "candidate_price_basis": "raw_session"})


def parameter_runtime_evaluator(spec: StrategySpec, state: StrategyCandidateState,
                                features: Mapping[str, object]) -> StrategyDecision | None:
    if state.state is StrategyLifecycleState.HOLDING:
        return parameter_exit_evaluator(spec, state, features)
    return parameter_entry_evaluator(spec, state, features)
