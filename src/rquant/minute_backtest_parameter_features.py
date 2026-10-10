"""PIT inputs for parameterized minute definitions, using the legacy kernels."""

from __future__ import annotations

import math
import hashlib
import json
from collections.abc import Mapping
from datetime import date, datetime
from types import MappingProxyType
from typing import Literal, Self
from zoneinfo import ZoneInfo

import pandas as pd
from pydantic import ConfigDict, Field, SerializerFunctionWrapHandler, StrictBool, StrictInt, field_serializer, field_validator, model_serializer, model_validator

from rquant.growth_board_surge_strategy import _extract_relative_volume_features, _tick_rule_split
from rquant.feature_contracts import FeatureAvailability, FeatureBatchEnvelope, FeatureFieldStatus
from rquant.feature_spool import FeatureBatchSpool, FeatureCurrentPointer
from rquant.intraday_feature_engine import INPUT_COLUMNS, MARKET_MINUTE_FEATURE_MAX_DELAY_SECONDS, _normalize_frame
from rquant.minute_backtest_contracts import Sha256
from rquant.minute_backtest_parameters import MinuteFrequency, MinuteNShapeParameters, MinuteParameterSet
from rquant.minute_backtest_parameter_study import MinuteParameterStudyBinding
from rquant.minute_backtest_parameter_study_features import MinuteParameterStudyDecision
from rquant.minute_replay import evaluate_n_shape_entry
from rquant.live_contracts import BatchQualityStatus
from rquant.paper import PaperPosition
from rquant.paper_contracts import PaperFill, PaperSide
from rquant.paper_signal_worker import PaperSignalQueueRecord
from rquant.signal_contracts import SignalAction, _freeze_json, _thaw_json
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256, normalize_aware_utc
from rquant.stock_features import build_intraday_relative_volume_features_from_history
from rquant.strategy_runner import canonical_feature_payload
from rquant.volume_profile import VolumeProfile

PARAMETER_FEATURE_CONTRACT = "minute-parameter-pit"
PARAMETER_CANDIDATE_FEATURE = "minute_parameter_candidate"
PARAMETER_BAR_FEATURE = "minute_parameter_bar"
PARAMETER_LIFECYCLE_FEATURE = "minute_parameter_lifecycle"
_SHANGHAI = ZoneInfo("Asia/Shanghai")


class _ParameterEvidence(RuntimeContractModel):
    model_config = ConfigDict(extra="forbid", frozen=True, revalidate_instances="always", allow_inf_nan=False)


class MinuteParameterVolumeProfile(VolumeProfile):
    model_config = _ParameterEvidence.model_config


class MinuteParameterCandidate(_ParameterEvidence):
    family: Literal["n_shape", "auction_gap", "growth_board_surge"]
    parameter_hash: Sha256
    study_binding: MinuteParameterStudyBinding | None = None
    ts_code: str = Field(pattern=r"^[0-9]{6}\.(?:SH|SZ|BJ)$")
    name: str = ""
    pool: str = Field(min_length=1)
    trade_date: date
    reference_date: date
    available_at: AwareUtcDatetime
    t_close: float = Field(gt=0)
    t_high: float | None = Field(default=None, gt=0)
    limit_up_price: float = Field(gt=0)
    stop_weak: float = Field(default=0, ge=0)
    static_factors: Mapping[str, StrictBool | StrictInt | float | None] = Field(default_factory=dict)
    volume_profiles: tuple[MinuteParameterVolumeProfile, ...] = ()
    auction_price: float | None = Field(default=None, gt=0)
    auction_vol_ratio_5d: float | None = Field(default=None, ge=0)
    auction_gap_pct_close: float | None = None
    auction_prev5_count: StrictInt | None = Field(default=None, ge=0, le=5)
    is_st: StrictBool | None = None
    listed_trading_days: StrictInt | None = Field(default=None, ge=0)
    prior_nonmissing_volume_ratios: tuple[float, ...] | None = Field(default=None, max_length=90)
    board_gap_up_ratio: float | None = Field(default=None, ge=0, le=1)
    board_auction_amount_ratio: float | None = Field(default=None, ge=0)

    @field_validator("static_factors")
    @classmethod
    def freeze_factors(cls, value: Mapping[str, bool | float | int | None]) -> Mapping[str, bool | float | int | None]:
        if any(not key or (item is not None and not math.isfinite(item))
               for key, item in value.items()):
            raise ValueError("parameter static factors must be finite original values")
        return MappingProxyType(dict(sorted(value.items())))

    @field_serializer("static_factors")
    def serialize_factors(self, value: Mapping[str, bool | float | int | None]) -> dict[str, bool | float | int | None]:
        return dict(value)

    @model_serializer(mode="wrap")
    def original_default_fields(self, handler: SerializerFunctionWrapHandler) -> dict[str, object]:
        value = handler(self)
        if self.study_binding is None:
            value.pop("study_binding", None)
        return value

    @model_validator(mode="after")
    def past_complete_candidate(self) -> Self:
        if self.study_binding is not None and self.study_binding.protocol.parameters.fingerprint != self.parameter_hash:
            raise ValueError("candidate study does not bind its complete parameter recipe")
        if self.reference_date >= self.trade_date or self.available_at.astimezone(_SHANGHAI).date() > self.trade_date:
            raise ValueError("parameter candidate reference or availability is future")
        if self.limit_up_price <= self.t_close:
            raise ValueError("parameter candidate limit must exceed raw previous close")
        if self.family == "n_shape" and (self.t_high is None or self.pool not in {"pool1", "pool2"}):
            raise ValueError("N parameter candidate requires its original pool and high")
        if self.family == "auction_gap" and any(value is None for value in (
                self.auction_price, self.t_high, self.auction_vol_ratio_5d,
                self.auction_gap_pct_close, self.auction_prev5_count, self.is_st)):
            raise ValueError("auction parameter candidate requires the complete original candidate fields")
        if self.prior_nonmissing_volume_ratios is not None and any(
                not math.isfinite(value) for value in self.prior_nonmissing_volume_ratios):
            raise ValueError("prior nonmissing volume ratios must be finite original values")
        if any(profile.ts_code != self.ts_code or profile.reference_date != self.trade_date
               or profile.end_date > self.reference_date for profile in self.volume_profiles):
            raise ValueError("parameter volume profile is detached from the past raw-session basis")
        return self


class MinuteParameterOfficialSeal(_ParameterEvidence):
    open_times: StrictInt | None = Field(default=None, ge=0)
    fd_amount: float | None = Field(default=None, ge=0)
    float_mv: float | None = Field(default=None, ge=0)


class MinuteParameterSessionFacts(_ParameterEvidence):
    """Original completed reference and dated query evidence, with separate PIT times."""

    ts_code: str = Field(pattern=r"^[0-9]{6}\.(?:SH|SZ|BJ)$")
    trade_date: date
    reference_date: date
    previous_close: float = Field(gt=0)
    session_pre_close: float = Field(gt=0)
    basis_available_at: AwareUtcDatetime
    source_snapshot_id: Sha256
    auction_query_complete: StrictBool = False
    auction_available_at: AwareUtcDatetime | None = None
    auction_price: float | None = Field(default=None, gt=0)
    seal_query_complete: StrictBool = False
    seal_available_at: AwareUtcDatetime | None = None
    official_seal: MinuteParameterOfficialSeal | None = None

    @model_validator(mode="after")
    def complete_query_proofs(self) -> Self:
        if self.reference_date >= self.trade_date:
            raise ValueError("session basis requires an original completed previous session")
        if self.basis_available_at.astimezone(_SHANGHAI).date() > self.trade_date:
            raise ValueError("session price basis is from a future session")
        for complete, available, payload in ((self.auction_query_complete, self.auction_available_at, self.auction_price),
                (self.seal_query_complete, self.seal_available_at, self.official_seal)):
            if complete != (available is not None) or (payload is not None and not complete):
                raise ValueError("query payload or absence requires its complete original query proof")
        return self

    def source_payload(self) -> bytes:
        return json.dumps(self.model_dump(mode="json", exclude={"source_snapshot_id"}),
            ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


class MinuteParameterBar(_ParameterEvidence):
    parameter_hash: Sha256
    ts_code: str
    frequency: MinuteFrequency
    event_time: AwareUtcDatetime
    available_at: AwareUtcDatetime
    decision_cutoff: AwareUtcDatetime
    open: float = Field(gt=0)
    high: float = Field(gt=0)
    low: float = Field(gt=0)
    close: float = Field(gt=0)
    session_low: float = Field(gt=0)
    session_high: float = Field(gt=0)
    minute_amount: float = Field(ge=0)
    cumulative_amount: float = Field(ge=0)
    vwap: float | None = Field(default=None, gt=0)
    prior_amounts: tuple[float, ...]
    first_signal_time: AwareUtcDatetime | None = None
    compact_relative_features: Mapping[str, float | int | None]
    inner_outer_ratio: float | None = Field(default=None, ge=0)
    raw_prefix_hash: Sha256
    history_hash: Sha256
    study_decision: MinuteParameterStudyDecision | None = None

    @model_serializer(mode="wrap")
    def original_default_fields(self, handler: SerializerFunctionWrapHandler) -> dict[str, object]:
        value = handler(self)
        if self.study_decision is None:
            value.pop("study_decision", None)
        return value

    _freeze_relative = field_validator("compact_relative_features")(MinuteParameterCandidate.freeze_factors.__func__)

    @field_serializer("compact_relative_features")
    def serialize_relative(self, value: Mapping[str, float | int | None]) -> dict[str, float | int | None]:
        return dict(value)

    @model_validator(mode="after")
    def visible_bar(self) -> Self:
        if self.study_decision is not None and (self.study_decision.ts_code, self.study_decision.decision_cutoff,
                self.study_decision.raw_prefix_hash, self.study_decision.history_hash,
                self.study_decision.binding.protocol.parameters.fingerprint) != (
                self.ts_code, self.decision_cutoff, self.raw_prefix_hash, self.history_hash, self.parameter_hash):
            raise ValueError("bar study proof differs from its complete visible raw prefix")
        if not self.event_time <= self.available_at <= self.decision_cutoff:
            raise ValueError("parameter bar is not PIT visible")
        if not (self.low <= min(self.open, self.close) <= max(self.open, self.close) <= self.high
                and self.session_low <= self.low <= self.high <= self.session_high):
            raise ValueError("parameter bar price geometry differs")
        if self.first_signal_time is not None and (self.first_signal_time > self.event_time
                or self.first_signal_time.astimezone(_SHANGHAI).date() != self.event_time.astimezone(_SHANGHAI).date()):
            raise ValueError("parameter first signal is future or from another session")
        if any(not math.isfinite(value) or value < 0 for value in self.prior_amounts):
            raise ValueError("parameter prior amounts must be finite nonnegative originals")
        return self


class MinuteParameterPosition(PaperPosition):
    model_config = _ParameterEvidence.model_config
    risk_payload: Mapping[str, object] | None = None

    @field_validator("risk_payload", mode="before")
    @classmethod
    def immutable_risk(cls, value: object) -> object:
        return None if value is None else _freeze_json(_thaw_json(value))

    @field_serializer("risk_payload")
    def serialize_risk(self, value: Mapping[str, object] | None) -> object:
        return None if value is None else _thaw_json(value)


class MinuteParameterLifecycle(_ParameterEvidence):
    parameter_hash: Sha256
    candidate_state_key: str = Field(min_length=1)
    entry_signal_id: str = Field(min_length=1)
    runtime_input_hash: Sha256
    account_id: str = Field(min_length=1)
    entry_record: PaperSignalQueueRecord
    entry_fill: PaperFill
    remaining_quantity: int = Field(gt=0, multiple_of=100)
    risk_basis: Literal["original_paper_two_decimal"] = "original_paper_two_decimal"
    position: MinuteParameterPosition
    position_available_at: AwareUtcDatetime
    holding_trading_sessions: int = Field(ge=0)
    sellable: bool
    bar: MinuteParameterBar
    max_hold_days: int = Field(ge=1, le=20)
    hold_policy: Literal["fixed", "t1", "seal_hold"] = "fixed"
    auction_exit_reason: Literal["next_auction_weak", "next_morning_vwap_break"] | None = None
    session_fact_hashes: tuple[Sha256, ...] = ()

    @model_validator(mode="after")
    def actual_visible_position(self) -> Self:
        if (self.position.ts_code != self.bar.ts_code or self.parameter_hash != self.bar.parameter_hash
                or self.position_available_at > self.bar.decision_cutoff
                or normalize_aware_utc(self.position.entry_time) > self.bar.event_time):
            raise ValueError("parameter lifecycle is future or detached")
        record, fill = self.entry_record, self.entry_fill
        if record.order is None or record.intent is None or record.quote is None:
            raise ValueError("parameter lifecycle requires the complete original prepared execution")
        if (record.signal.action is not SignalAction.B_INTENT or record.signal.signal_id != self.entry_signal_id
                or record.signal.candidate_id != self.position.ts_code
                or record.signal.evidence.get("minute_parameter_set_hash") != self.parameter_hash
                or record.intent.signal_id != self.entry_signal_id or record.intent.account_id != self.account_id
                or record.intent.ts_code != self.position.ts_code or record.intent.side is not PaperSide.BUY
                or record.order.intent_id != record.intent.intent_id or record.order.side is not PaperSide.BUY
                or record.order.account_id != self.account_id or record.order.ts_code != self.position.ts_code
                or record.order.order_id != fill.order_id or record.execution_id != fill.execution_id
                or record.quote.snapshot_id != fill.price_snapshot_id or record.quote.ts_code != self.position.ts_code
                or record.intent.price_snapshot_id != fill.price_snapshot_id
                or record.order.filled_quantity != fill.quantity or self.remaining_quantity > fill.quantity
                or record.quote.available_at > fill.executed_at or fill.executed_at > self.position_available_at
                or normalize_aware_utc(self.position.entry_time) != fill.executed_at):
            raise ValueError("parameter lifecycle execution/code/quantity/time differs from its original fill")
        return self


class MinuteParameterLifecycles(_ParameterEvidence):
    parameter_hash: Sha256
    ts_code: str
    positions: tuple[MinuteParameterLifecycle, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def exact_occurrences(self) -> Self:
        keys = tuple(position.candidate_state_key for position in self.positions)
        if keys != tuple(sorted(set(keys))):
            raise ValueError("parameter lifecycle occurrences must be unique and ordered")
        if any(position.parameter_hash != self.parameter_hash or position.position.ts_code != self.ts_code
               for position in self.positions):
            raise ValueError("parameter lifecycle set is detached from its full recipe or code")
        if len({position.entry_signal_id for position in self.positions}) != len(self.positions):
            raise ValueError("parameter lifecycle entry signal belongs to multiple occurrences")
        return self


def _frame_hash(frame: pd.DataFrame) -> str:
    return canonical_sha256(frame.loc[:, INPUT_COLUMNS].to_dict(orient="records"))


def project_minute_parameter_lifecycle(
    parameters: MinuteParameterSet, lifecycle: MinuteParameterLifecycles,
) -> Mapping[str, object]:
    if lifecycle.parameter_hash != parameters.fingerprint:
        raise ValueError("parameter lifecycle differs from the complete parameter recipe")
    checked = MinuteParameterLifecycles.model_validate(lifecycle.model_dump(mode="python"))
    return MappingProxyType({PARAMETER_LIFECYCLE_FEATURE: checked.model_dump_json()})


def project_minute_parameter_features(
    parameters: MinuteParameterSet,
    candidate: MinuteParameterCandidate,
    minutes: pd.DataFrame,
    historical_minutes: pd.DataFrame,
    *,
    source_frequency: MinuteFrequency,
    decision_cutoff: datetime,
) -> Mapping[str, object]:
    """Project the visible finalized prefix; never resample or alter archive bytes."""
    cutoff = normalize_aware_utc(decision_cutoff)
    config = parameters.parameters
    if source_frequency != config.freq:
        raise ValueError("parameter frequency differs from the physical minute source")
    if candidate.parameter_hash != parameters.fingerprint or candidate.family != config.family:
        raise ValueError("candidate parameter binding differs")
    if candidate.available_at > cutoff:
        raise ValueError("parameter candidate is not PIT visible")
    raw = _normalize_frame(minutes.reset_index(drop=True), label="parameter-minute")
    history = _normalize_frame(historical_minutes.reset_index(drop=True), label="parameter-history")
    current_date = cutoff.astimezone(_SHANGHAI).date()
    if candidate.trade_date > current_date:
        raise ValueError("parameter candidate session is future")
    visible = raw[(raw["ts_code"] == candidate.ts_code) & (raw["_trade_date"] == current_date)
        & (raw["_utc_time"] <= cutoff) & (raw["_available_utc"] <= cutoff)].copy()
    if visible.empty:
        raise ValueError("parameter source has no visible finalized bar")
    selected_history = history[(history["ts_code"] == candidate.ts_code)
        & (history["_trade_date"] < current_date) & (history["_available_utc"] <= cutoff)].copy()
    lookback = config.lookback_days if config.family == "growth_board_surge" else 20
    previous_dates = tuple(sorted(selected_history["_trade_date"].unique())[-lookback:])
    selected_history = selected_history[selected_history["_trade_date"].isin(previous_dates)].copy()
    # The legacy kernel compares local time-only clocks. This detached view has
    # the same wall clocks; the archived aware timestamps and hashes stay intact.
    local_history = selected_history.loc[:, INPUT_COLUMNS].copy()
    local_history["trade_time"] = selected_history["_local_time"].dt.tz_localize(None)
    cum_vol = cum_amount = 0.0
    cum_low, cum_high = float("inf"), float("-inf")
    inner_vol = outer_vol = 0.0
    prior_amounts: list[float] = []
    clocked_amounts = []
    previous_close = None
    first_signal_time = None
    for _, row in visible.iterrows():
        moment = row["_local_time"].to_pydatetime().replace(tzinfo=None)
        vol, amount = float(row["vol"]), float(row["amount"])
        cum_vol += vol
        cum_amount += amount
        cum_low, cum_high = min(cum_low, float(row["low"])), max(cum_high, float(row["high"]))
        vwap = cum_amount / cum_vol if cum_vol > 0 else None
        inner, outer = _tick_rule_split(float(row["open"]) if previous_close is None else previous_close,
            float(row["close"]), vol)
        inner_vol += inner
        outer_vol += outer
        previous_close = float(row["close"])
        if isinstance(config, MinuteNShapeParameters) and candidate.trade_date == current_date:
            checked = evaluate_n_shape_entry(config=config.owner_config(), quote_time=moment,
                latest_price=previous_close, bar_low=float(row["low"]), session_low=cum_low, session_high=cum_high,
                t_close=candidate.t_close, t_high=candidate.t_high, vwap=vwap, minute_amount=amount,
                prior_amounts=tuple(prior_amounts), first_signal_time=first_signal_time,
                static_factors=candidate.static_factors)
            first_signal_time = checked.signal_time
        prior_amounts.append(amount)
        clocked_amounts.append((moment.time(), amount))
    relative = build_intraday_relative_volume_features_from_history(local_history, previous_dates, moment,
        current_minute_amount=amount, current_cum_amount=cum_amount,
        current_day_amounts=clocked_amounts[:-1], lookback_days=lookback)
    bar = MinuteParameterBar(parameter_hash=parameters.fingerprint, ts_code=candidate.ts_code,
        frequency=source_frequency, event_time=row["_utc_time"].to_pydatetime(),
        available_at=max(candidate.available_at, visible["_available_utc"].max().to_pydatetime(),
            *(selected_history["_available_utc"].tolist())), decision_cutoff=cutoff,
        open=float(row["open"]), high=float(row["high"]), low=float(row["low"]), close=previous_close,
        session_low=cum_low, session_high=cum_high, minute_amount=amount, cumulative_amount=cum_amount,
        vwap=vwap, prior_amounts=tuple(prior_amounts[:-1]), first_signal_time=None if first_signal_time is None else first_signal_time.replace(tzinfo=_SHANGHAI),
        compact_relative_features=_extract_relative_volume_features(relative, lookback),
        inner_outer_ratio=round(inner_vol/outer_vol, 4) if outer_vol > 0 else None,
        raw_prefix_hash=_frame_hash(visible), history_hash=_frame_hash(selected_history))
    return MappingProxyType({PARAMETER_CANDIDATE_FEATURE: candidate.model_dump_json(),
        PARAMETER_BAR_FEATURE: bar.model_dump_json(), "latest_close": bar.close,
        "session_low": bar.session_low, "session_high": bar.session_high})


def publish_minute_parameter_features(
    spool: FeatureBatchSpool,
    *,
    parameters: MinuteParameterSet,
    candidates: tuple[MinuteParameterCandidate, ...],
    minutes: pd.DataFrame,
    historical_minutes: pd.DataFrame,
    source_frequency: MinuteFrequency,
    decision_cutoff: datetime,
    sequence: int,
    input_batch_ids: tuple[str, ...],
    producer_commit: str,
    lifecycles: Mapping[str, MinuteParameterLifecycles] | None = None,
    source_quality: BatchQualityStatus = BatchQualityStatus.PUBLISHED,
    study_binding: MinuteParameterStudyBinding | None = None,
    new_entry_codes: tuple[str, ...] | None = None,
) -> FeatureCurrentPointer:
    """Publish the real common batch; the original candidate join owns static JSON."""
    cutoff = normalize_aware_utc(decision_cutoff)
    rows, statuses, bars = [], [], []
    if len({candidate.ts_code for candidate in candidates}) != len(candidates):
        raise ValueError("parameter feature candidates must contain one row per code")
    lifecycle_by_code = {} if lifecycles is None else dict(lifecycles)
    study_decisions = {}
    if study_binding is not None:
        if new_entry_codes is None:
            raise ValueError("study publication lacks its actual original new-entry state prefix")
        from rquant.minute_backtest_parameter_study_features import project_minute_parameter_study_decisions

        study_decisions = project_minute_parameter_study_decisions(study_binding, candidates=candidates,
            minutes=minutes, historical_minutes=historical_minutes, source_frequency=source_frequency,
            decision_cutoff=cutoff, new_entry_codes=new_entry_codes)
    elif any(candidate.study_binding is not None for candidate in candidates):
        raise ValueError("study candidate publication cannot omit its complete runtime binding")
    if set(lifecycle_by_code) - {candidate.ts_code for candidate in candidates}:
        raise ValueError("parameter lifecycle has a detached candidate")
    for candidate in candidates:
        values = dict(project_minute_parameter_features(parameters, candidate, minutes, historical_minutes,
            source_frequency=source_frequency, decision_cutoff=cutoff))
        values.pop(PARAMETER_CANDIDATE_FEATURE)
        bar = MinuteParameterBar.model_validate_json(values[PARAMETER_BAR_FEATURE])
        if study_binding is not None:
            bar = MinuteParameterBar.model_validate(bar.model_dump(mode="python") | {
                "study_decision": study_decisions[candidate.ts_code]})
            values[PARAMETER_BAR_FEATURE] = bar.model_dump_json()
        bars.append(bar)
        lifecycle = lifecycle_by_code.get(candidate.ts_code)
        if lifecycle is not None:
            if study_binding is not None:
                lifecycle = MinuteParameterLifecycles.model_validate(lifecycle.model_dump(mode="python") | {
                    "positions": tuple(position.model_copy(update={"bar":
                        position.bar.model_copy(update={"study_decision": study_decisions[candidate.ts_code]})})
                        for position in lifecycle.positions)})
            if lifecycle.ts_code != candidate.ts_code or any(position.bar != bar for position in lifecycle.positions):
                raise ValueError("parameter lifecycle does not bind this exact visible market bar")
            values.update(project_minute_parameter_lifecycle(parameters, lifecycle))
        else:
            values[PARAMETER_LIFECYCLE_FEATURE] = None
        rows.append({"ts_code": candidate.ts_code, **values})
        for name, value in values.items():
            available = max(cutoff, bar.available_at, *(position.position_available_at for position in lifecycle.positions)) if lifecycle is not None else max(cutoff, bar.available_at)
            missing = value is None
            delay = (available-bar.event_time).total_seconds()
            lifecycle_field = name == PARAMETER_LIFECYCLE_FEATURE
            late = not missing and delay > MARKET_MINUTE_FEATURE_MAX_DELAY_SECONDS * (2 if lifecycle_field else 1)
            status = FeatureAvailability.UNAVAILABLE if missing else FeatureAvailability.AVAILABLE
            reason = "no_verified_parameter_position" if missing else None
            if not missing and (late or source_quality is not BatchQualityStatus.PUBLISHED):
                status = (FeatureAvailability.DEGRADED if lifecycle_field or source_quality is BatchQualityStatus.DEGRADED
                    else FeatureAvailability.STALE)
                reason = "original_market_visibility_delay" if late else "original_market_quality"
            statuses.append(FeatureFieldStatus(candidate_id=candidate.ts_code, name=name,
                status=status,
                source_event_time=bar.event_time, available_at=available, decision_cutoff=cutoff,
                actual_delay_seconds=delay,
                reason=reason))
    columns = ("ts_code", PARAMETER_BAR_FEATURE, "latest_close", "session_low", "session_high", PARAMETER_LIFECYCLE_FEATURE)
    frame = pd.DataFrame(rows, columns=columns)
    payload = canonical_feature_payload(frame, schema_version=2)
    event = max((bar.event_time for bar in bars), default=cutoff)
    available = max((item.available_at for item in statuses), default=cutoff)
    content_hash = hashlib.sha256(payload).hexdigest()
    envelope = FeatureBatchEnvelope(schema_version=2,
        batch_id="minute-parameter:"+canonical_sha256({"parameter_hash": parameters.fingerprint,
            "sequence": sequence, "cutoff": cutoff, "inputs": input_batch_ids, "payload": content_hash}),
        contract_id=PARAMETER_FEATURE_CONTRACT, contract_version=1,
        input_batch_ids=input_batch_ids, sequence=sequence, event_time=event, available_at=available,
        decision_cutoff=cutoff, actual_delay_seconds=(available-event).total_seconds(),
        row_count=len(frame), content_hash=content_hash, field_statuses=tuple(statuses), producer_commit=producer_commit)
    return spool.publish(envelope, payload)
