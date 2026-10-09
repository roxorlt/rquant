"""Complete causal score evidence on the original parameter feature path."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import TYPE_CHECKING, Self

import pandas as pd
from pydantic import Field, model_validator

from rquant.intraday_feature_engine import INPUT_COLUMNS, _normalize_frame
from rquant.minute_backtest_contracts import MAX_CODES, MAX_INPUT_BYTES, Sha256
from rquant.minute_backtest_parameter_optimizer import (
    MinuteStudyThreePartSelection, select_minute_three_part_study_candidates,
)
from rquant.minute_backtest_parameter_study import MinuteParameterStudyBinding
from rquant.minute_backtest_study_protocols import SHANGHAI, MinuteStudyCandidate, MinuteStudyFeature
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, normalize_aware_utc
from rquant.stock_features import build_intraday_relative_volume_features_from_history
from rquant.topn_selection import resolve_score_profiles

if TYPE_CHECKING:
    from rquant.minute_backtest_parameter_features import MinuteParameterCandidate
    from rquant.minute_backtest_parameters import MinuteFrequency

_DYNAMIC_FEATURES = frozenset(("signal_rel_amount_same_minute_20d",
    "signal_rel_cum_amount_asof_20d", "signal_amount_accel_5m", "signal_amount_accel_10m"))


class MinuteParameterStudyCandidateProof(RuntimeContractModel):
    candidate_json: str = Field(min_length=1, max_length=MAX_INPUT_BYTES)
    bar_json: str = Field(min_length=1, max_length=MAX_INPUT_BYTES)
    fact: MinuteStudyCandidate


class MinuteParameterStudyDecision(RuntimeContractModel):
    binding: MinuteParameterStudyBinding
    ts_code: str
    decision_cutoff: AwareUtcDatetime
    raw_prefix_hash: Sha256
    history_hash: Sha256
    new_entry_codes: tuple[str, ...] = Field(max_length=MAX_CODES)
    prefix: tuple[MinuteParameterStudyCandidateProof, ...] = Field(max_length=MAX_CODES)

    @model_validator(mode="after")
    def complete_visible_prefix(self) -> Self:
        from rquant.minute_backtest_parameter_features import MinuteParameterBar, MinuteParameterCandidate

        codes = tuple(value.fact.ts_code for value in self.prefix)
        if codes != tuple(sorted(set(codes))) or self.new_entry_codes != tuple(sorted(set(self.new_entry_codes))):
            raise ValueError("study prefix/new-entry codes must be unique and ordered")
        if not set(self.new_entry_codes).issubset(codes):
            raise ValueError("study new-entry codes are outside the complete current prefix")
        protocol = self.binding.protocol
        profile = resolve_score_profiles([protocol.score_profile])[0]
        required = {term.name for term in profile.terms}
        if profile.env_gate is not None:
            required.add(profile.env_gate.feature)
        for proof in self.prefix:
            candidate = MinuteParameterCandidate.model_validate_json(proof.candidate_json)
            bar = MinuteParameterBar.model_validate_json(proof.bar_json)
            fact = proof.fact
            if (candidate.study_binding != self.binding or bar.study_decision is not None
                    or (candidate.ts_code, bar.ts_code, candidate.parameter_hash, bar.parameter_hash,
                        candidate.trade_date, bar.event_time, bar.available_at, bar.decision_cutoff) != (
                        fact.ts_code, fact.ts_code, protocol.parameters.fingerprint,
                        protocol.parameters.fingerprint, fact.trade_date, fact.event_time,
                        fact.available_at, self.decision_cutoff)
                    or fact.source != protocol.source or fact.head != protocol.head
                    or fact.available_at > self.decision_cutoff
                    or any(item.available_at > self.decision_cutoff for item in fact.features)
                    or not required.issubset({item.name for item in fact.features})):
                raise ValueError("study prefix is future, incomplete or detached from its full candidate/bar")
        return self

    @property
    def selected(self) -> tuple[MinuteStudyThreePartSelection, ...]:
        from rquant.minute_backtest_parameter_evaluators import parameter_entry_decision
        from rquant.minute_backtest_parameter_features import MinuteParameterBar, MinuteParameterCandidate

        try:
            self.binding.partition(self.decision_cutoff.astimezone(SHANGHAI).date())
        except ValueError:
            return ()
        eligible = []
        for proof in self.prefix:
            if proof.fact.ts_code not in self.new_entry_codes:
                continue
            candidate = MinuteParameterCandidate.model_validate_json(proof.candidate_json)
            bar = MinuteParameterBar.model_validate_json(proof.bar_json)
            if parameter_entry_decision(self.binding.protocol.parameters, candidate, bar) is not None:
                eligible.append(proof.fact)
        return select_minute_three_part_study_candidates(self.binding, eligible,
            decision_cutoff=self.decision_cutoff)


def project_minute_parameter_study_decisions(
    binding: MinuteParameterStudyBinding, *, candidates: tuple[MinuteParameterCandidate, ...],
    minutes: pd.DataFrame, historical_minutes: pd.DataFrame, source_frequency: MinuteFrequency,
    decision_cutoff: datetime, new_entry_codes: tuple[str, ...],
) -> Mapping[str, MinuteParameterStudyDecision]:
    from rquant.minute_backtest_parameter_features import (
        PARAMETER_BAR_FEATURE, MinuteParameterBar, project_minute_parameter_features,
    )

    binding = MinuteParameterStudyBinding.model_validate(binding)
    cutoff = normalize_aware_utc(decision_cutoff)
    protocol = binding.protocol
    if source_frequency != protocol.source.frequency or cutoff > protocol.requested_at:
        raise ValueError("study source frequency or decision clock differs")
    profile = resolve_score_profiles([protocol.score_profile])[0]
    required = {term.name for term in profile.terms}
    if profile.env_gate is not None:
        required.add(profile.env_gate.feature)
    raw = _normalize_frame(minutes.reset_index(drop=True), label="minute-study-prefix")
    history = _normalize_frame(historical_minutes.reset_index(drop=True), label="minute-study-history")
    day = cutoff.astimezone(SHANGHAI).date()
    prefix = []
    bars = {}
    for candidate in sorted(candidates, key=lambda item: item.ts_code):
        if candidate.study_binding != binding:
            raise ValueError("study candidate does not bind the complete runtime protocol")
        values = project_minute_parameter_features(protocol.parameters, candidate, minutes, historical_minutes,
            source_frequency=source_frequency, decision_cutoff=cutoff)
        bar = MinuteParameterBar.model_validate_json(values[PARAMETER_BAR_FEATURE])
        bars[candidate.ts_code] = bar
        if candidate.trade_date != day:
            continue
        current = raw[(raw.ts_code == candidate.ts_code) & (raw._trade_date == day)
            & (raw._utc_time <= cutoff) & (raw._available_utc <= cutoff)].copy()
        past = history[(history.ts_code == candidate.ts_code) & (history._trade_date < day)
            & (history._available_utc <= cutoff)].copy()
        dates = tuple(sorted(past._trade_date.unique())[-20:])
        past = past[past._trade_date.isin(dates)].copy()
        local_history = past.loc[:, INPUT_COLUMNS].copy()
        local_history["trade_time"] = past._local_time.dt.tz_localize(None)
        amounts = [(moment.time(), float(amount)) for moment, amount in
            zip(current["_local_time"], current["amount"], strict=True)]
        moment = bar.event_time.astimezone(SHANGHAI).replace(tzinfo=None)
        relative = build_intraday_relative_volume_features_from_history(local_history, dates, moment,
            current_minute_amount=bar.minute_amount, current_cum_amount=bar.cumulative_amount,
            current_day_amounts=amounts[:-1], lookback_days=20)
        dynamic_available = max((bar.available_at, *past._available_utc.tolist()))
        features = []
        for name in sorted(required):
            if name in _DYNAMIC_FEATURES:
                if name not in relative:
                    raise ValueError("original dynamic score feature is unknown")
                value, available = relative[name], dynamic_available
            else:
                if name not in candidate.static_factors:
                    raise ValueError("original static score feature is unknown: " + name)
                value, available = candidate.static_factors[name], candidate.available_at
            features.append(MinuteStudyFeature(name=name,
                value=None if value is None else float(value), available_at=available))
        fact = MinuteStudyCandidate(source=protocol.source, head=protocol.head,
            parameter_fingerprint=protocol.parameters.fingerprint, candidate_id=candidate.ts_code,
            ts_code=candidate.ts_code, trade_date=day, event_time=bar.event_time,
            available_at=bar.available_at, features=tuple(features))
        prefix.append(MinuteParameterStudyCandidateProof(candidate_json=candidate.model_dump_json(),
            bar_json=bar.model_dump_json(), fact=fact))
    prefix = tuple(prefix)
    return {code: MinuteParameterStudyDecision(binding=binding, ts_code=code, decision_cutoff=cutoff,
        raw_prefix_hash=bar.raw_prefix_hash, history_hash=bar.history_hash,
        new_entry_codes=tuple(sorted(new_entry_codes)), prefix=prefix) for code, bar in bars.items()}
