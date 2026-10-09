from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest
from pydantic import ValidationError

from rquant.auction_gap_strategy import AuctionGapConfig, auction_candidate_mask
from rquant.minute_backtest_commands import SubmitMinuteReplay
from rquant.minute_backtest_parameter_artifact import MinuteParameterSealedReplayReader
from rquant.minute_backtest_parameter_definition import build_minute_parameter_definition
from rquant.minute_backtest_parameter_evaluators import parameter_entry_evaluator
from rquant.minute_backtest_parameter_features import MinuteParameterCandidate, project_minute_parameter_features
from rquant.minute_backtest_parameters import MinuteAuctionGapParameters, MinuteParameterSet
from rquant.live_spool import LiveBatchSpool
from rquant.runtime_paper_quote import PaperQuoteCandidateMissingError, PaperTradeCalendarError
from rquant.signal_contracts import SignalAction
from rquant.strategy_runner import StrategyCandidateState
from rquant.strategy_spec import StrategyLifecycleState
from rquant.web.models.minute_backtests import (
    MinuteCreateRequest, MinuteJobsData, MinuteParameterJob, MinuteParameterResultSource, MinuteSummaryData,
)

POLICY = "keep_candidate_mark_unavailable"
COMMIT = "a" * 40
AT = datetime(2026, 7, 31, 1, 33, tzinfo=UTC)
ORIGINAL = Path(__file__).resolve().parents[2] / "data/verification/minute-engine-completion-20261007/parameter-family-acceptance-01/auction-04-outputs/parameter-family-public-create-request.json"


def public_body() -> dict[str, object]:
    return json.loads(ORIGINAL.read_bytes())


def current_body() -> dict[str, object]:
    value = public_body()
    value["config"]["parameters"]["schema_version"] = 2
    value["config"]["parameters"]["parameters"]["next_day_price_policy"] = POLICY
    return value


def current_recipe() -> MinuteParameterSet:
    return MinuteParameterSet.model_validate(current_body()["config"]["parameters"])


def test_versioned_public_recipe_submission_and_recovery_keep_exact_policy() -> None:
    request = MinuteCreateRequest.model_validate(current_body())
    recipe = request.config.parameters
    assert recipe.schema_version == 2
    assert recipe.parameters.next_day_price_policy == POLICY
    assert recipe.definition_version == 1
    assert recipe.evaluator_semantic_version == "2.1.0"
    assert MinuteCreateRequest.model_validate_json(request.model_dump_json()) == request
    command = SubmitMinuteReplay(command_id=str(request.command_id), requested_at=request.requested_at,
        actor_id="synthetic-researcher", config=request.config)
    assert SubmitMinuteReplay.model_validate_json(command.model_dump_json()).config.parameters == recipe
    old = MinuteCreateRequest.model_validate(public_body()).config.parameters
    assert recipe.fingerprint != old.fingerprint and recipe.definition_id != old.definition_id


def test_original_v1_public_recipe_bytes_hash_and_native_identity_stay_readable() -> None:
    raw = public_body()["config"]["parameters"]
    recipe = MinuteParameterSet.model_validate(raw)
    assert recipe.model_dump_json() == json.dumps(raw, ensure_ascii=False, separators=(",", ":"))
    assert recipe.model_dump(mode="json") == raw
    assert recipe.fingerprint == "cf55e58ade9c03001b916efd043a5d28822133ed65824f4fad5ae82cffc9dc43"
    assert recipe.definition_id == "ap.z5k6lcw6tqbqag4rn36qios5fcbccm7nmwbe6t5nllucz76j3rbq"
    assert recipe.definition_version == 1 and recipe.evaluator_semantic_version == "2.0.0"


@pytest.mark.parametrize("change", ("missing_policy", "legacy_with_policy", "unknown_policy", "wrong_family"))
def test_policy_version_mismatch_or_unsupported_value_is_rejected(change: str) -> None:
    raw = current_body()["config"]["parameters"]
    if change == "missing_policy":
        raw["parameters"].pop("next_day_price_policy")
    elif change == "legacy_with_policy":
        raw["schema_version"] = 1
    elif change == "unknown_policy":
        raw["parameters"]["next_day_price_policy"] = "drop_candidate_without_future_price"
    else:
        raw["parameters"] = {"family": "n_shape"}
    with pytest.raises(ValidationError):
        MinuteParameterSet.model_validate(raw)


def test_public_output_schema_keeps_original_auction_fields_and_explicit_policy() -> None:
    schema = MinuteAuctionGapParameters.model_json_schema(mode="serialization")
    assert schema["additionalProperties"] is False
    assert schema["properties"]["family"]["const"] == "auction_gap"
    assert "entry_pullback_tolerance_pct" in schema["properties"]
    assert "paper" in schema["properties"]
    policy_schema = schema["properties"]["next_day_price_policy"]
    assert policy_schema["anyOf"][0]["const"] == POLICY
    assert "next_day_price_policy" not in schema["required"]
    assert "next_day_price_policy" not in MinuteParameterSet.model_validate(public_body()["config"]["parameters"]).parameters.model_dump(mode="json")


def test_real_entry_evaluator_uses_current_facts_without_future_price_and_keeps_policy_evidence() -> None:
    recipe = current_recipe()
    definition = build_minute_parameter_definition(recipe, producer_commit=COMMIT)
    candidate = MinuteParameterCandidate(family="auction_gap", parameter_hash=recipe.fingerprint,
        ts_code="600000.SH", pool="auction_gap", trade_date=date(2026, 7, 31),
        reference_date=date(2026, 7, 30), available_at=AT - timedelta(minutes=8), name="合成样本",
        t_close=10.0, t_high=10.1, limit_up_price=11.0, auction_price=10.2,
        auction_vol_ratio_5d=1.0, auction_gap_pct_close=2.0, auction_prev5_count=5, is_st=False)
    minutes = pd.DataFrame([dict(ts_code=candidate.ts_code, trade_time=AT-timedelta(minutes=2-index),
        available_at=AT-timedelta(minutes=2-index), open=10.3, high=10.6, low=10.25,
        close=price, vol=1000.0, amount=1000.0*price) for index, price in enumerate((10.3, 10.4, 10.5))])
    features = project_minute_parameter_features(recipe, candidate, minutes, minutes.iloc[:0],
        source_frequency="1min", decision_cutoff=AT)
    state = StrategyCandidateState(strategy_spec_fingerprint=definition.spec.spec_fingerprint,
        candidate_id=candidate.ts_code, state=StrategyLifecycleState.IDLE, last_feature_sequence=-1, updated_at=AT)
    result = parameter_entry_evaluator(definition.spec, state, features)
    assert result is not None and result.action is SignalAction.B_INTENT
    assert result.evidence["minute_parameter_set_hash"] == recipe.fingerprint
    assert result.evidence["minute_parameter_set_json"] == recipe.model_dump_json()
    assert definition.evaluator_semantic_version == "2.1.0"
    assert definition.spec.parameters["semantic_version"] == "2.1.0"
    assert "next_open" not in candidate.model_dump()
    future = minutes.iloc[[-1]].copy()
    future["trade_time"], future["available_at"] = AT+timedelta(days=3), AT+timedelta(days=3)
    future[["open", "high", "low", "close"]] = 0.01
    later_features = project_minute_parameter_features(recipe, candidate, pd.concat([minutes, future]), minutes.iloc[:0],
        source_frequency="1min", decision_cutoff=AT)
    assert parameter_entry_evaluator(definition.spec, state, later_features) == result


def test_original_daily_default_still_requires_future_price() -> None:
    config = AuctionGapConfig(start_date="2026-07-31", end_date="2026-08-03")
    assert config.require_next_day is True
    row = dict(entry_price=10.2, pre_close=10.0, pre_high=10.1, auction_vol_ratio_5d=1.0,
        limit_up_price=11.0, is_st=False, name="合成样本")
    with_future = pd.DataFrame([row | {"next_open": 10.3}])
    without_future = pd.DataFrame([row | {"next_open": None}])
    assert bool(auction_candidate_mask(with_future, config).iloc[0])
    assert not bool(auction_candidate_mask(without_future, config).iloc[0])


def test_job_recovery_binds_versioned_policy_and_semantic_metadata() -> None:
    request = MinuteCreateRequest.model_validate(current_body())
    recipe = request.config.parameters
    common = dict(job_id=request.command_id, status="completed", version=1,
        created_at=request.requested_at, updated_at=request.requested_at, spec_hash="a"*64,
        source_key=request.config.source_key, source_version=1, full_input_hash=request.config.full_input_hash,
        native_id=recipe.definition_id, native_name="竞价高开", native_version=1, family="auction_gap",
        parameters=recipe, parameter_hash=recipe.fingerprint, start_date=date(2026, 7, 31),
        end_date=date(2026, 8, 4), result_hash="b"*64)
    job = MinuteParameterJob(**common)
    assert job.evaluator_semantic_version == "2.1.0"
    restored, = MinuteJobsData.model_validate_json(MinuteJobsData(available=True, jobs=(job,)).model_dump_json()).jobs
    assert restored == job and restored.parameters.model_dump_json() == recipe.model_dump_json()
    for wrong in ("2.0.0", "1.0.0"):
        with pytest.raises(ValidationError):
            MinuteParameterJob.model_validate(job.model_dump(mode="python") | {"evaluator_semantic_version": wrong})


def test_current_result_read_model_keeps_original_v1_sealed_identity_and_summary() -> None:
    raw = (ORIGINAL.parent / "parameter-sealed-full.json").read_bytes()
    sealed = MinuteParameterSealedReplayReader._sealed_result_model().model_validate_json(raw)
    assert sealed.complete_result_hash == "8ec2f6150e950aa97e2cfd15638597f03588622d626436535744065fb90a0ce9"
    assert sealed.result.replay.parameters.fingerprint == "cf55e58ade9c03001b916efd043a5d28822133ed65824f4fad5ae82cffc9dc43"
    assert sealed.result.replay.parameters.schema_version == 1
    assert sealed.result.replay.parameters.evaluator_semantic_version == "2.0.0"
    summary = MinuteSummaryData.model_validate_json(json.dumps(json.loads((ORIGINAL.parent / "parameter-family-public-summary.json").read_bytes())["data"]))
    assert summary.job.parameters == sealed.result.replay.parameters
    assert summary.result_hash == sealed.complete_result_hash
    assert summary.job.evaluator_semantic_version == "2.0.0"


def test_current_public_summary_read_keeps_original_v1_result_and_recipe() -> None:
    raw = json.loads((ORIGINAL.parent / "parameter-family-public-summary.json").read_bytes())["data"]
    summary = MinuteSummaryData.model_validate_json(json.dumps(raw))
    assert summary.model_dump(mode="json") == raw
    assert summary.job.parameters.fingerprint == "cf55e58ade9c03001b916efd043a5d28822133ed65824f4fad5ae82cffc9dc43"
    assert summary.result_hash == "8ec2f6150e950aa97e2cfd15638597f03588622d626436535744065fb90a0ce9"
    assert summary.job.evaluator_semantic_version == summary.source.evaluator_semantic_version == "2.0.0"


def test_result_source_metadata_derives_policy_semantic_and_rejects_wrong_version() -> None:
    raw = json.loads((ORIGINAL.parent / "parameter-family-public-summary.json").read_bytes())["data"]["source"]
    recipe = current_recipe()
    raw.update(parameters=recipe.model_dump(mode="json"), parameter_hash=recipe.fingerprint,
        native_id=recipe.definition_id)
    raw.pop("evaluator_semantic_version")
    source = MinuteParameterResultSource.model_validate(raw)
    assert source.evaluator_semantic_version == "2.1.0"
    assert MinuteParameterResultSource.model_validate_json(source.model_dump_json()) == source
    with pytest.raises(ValidationError):
        MinuteParameterResultSource.model_validate(source.model_dump(mode="python") | {"evaluator_semantic_version": "2.0.0"})


def test_real_quote_uses_next_open_session_and_distinguishes_closed_day_from_missing_price(tmp_path: Path) -> None:
    from tests.unit.test_runtime_paper_quote import _minute_row, _publish, _resolver, _signal

    recipe = current_recipe()
    signal = type(_signal()).model_validate(_signal().model_dump(mode="python", exclude={"signal_id"}) | {
        "strategy_id": recipe.definition_id, "parameter_fingerprint": recipe.fingerprint,
        "evidence": {"minute_parameter_set_json": recipe.model_dump_json(), "minute_parameter_set_hash": recipe.fingerprint}})
    spool = LiveBatchSpool(tmp_path / "synthetic-raw")
    _publish(spool, sequence=0, available_at=AT, rows=[_minute_row()])
    resolver = _resolver(tmp_path / "synthetic-authority", spool)
    buy = resolver(signal, AT)
    assert buy.context.acquisition_available_date == date(2026, 8, 3)
    assert resolver.trade_date_at(datetime(2026, 8, 3, 1, 33, tzinfo=UTC)) == date(2026, 8, 3)
    for closed in (date(2026, 8, 1), date(2026, 8, 2)):
        with pytest.raises(PaperTradeCalendarError, match="not an SSE open day"):
            resolver.trade_date_at(datetime.combine(closed, AT.timetz()))
    next_at = AT + timedelta(days=3)
    _publish(spool, sequence=1, available_at=next_at,
        rows=[_minute_row(ts_code="600001.SH", trade_time=next_at)])
    sell = type(signal).model_validate(signal.model_dump(mode="python", exclude={"signal_id"}) | {
        "action": SignalAction.S_INTENT, "event_time": next_at, "available_at": next_at,
        "expires_at": next_at + timedelta(minutes=10)})
    with pytest.raises(PaperQuoteCandidateMissingError):
        resolver(sell, next_at)
    assert recipe.parameters.next_day_price_policy == POLICY
    assert resolver.trade_date_at(next_at) == date(2026, 8, 3)


def test_actual_v2_sparse_runtime_adapter_and_results_preserve_policy_and_unavailable_values(
    tmp_path: Path,
) -> None:
    from rquant.minute_backtest_parameter_adapter import MinuteParameterFormalParameters, MinuteParameterFormalRunInput
    from rquant.minute_backtest_parameter_contracts import MinuteParameterRuntimeReceipt
    from rquant.minute_backtest_parameter_definition import minute_parameter_validation_scope
    from rquant.minute_backtest_parameter_runner import (
        MinuteParameterReplayResult, MinuteParameterReplaySummary, minute_parameter_result_tables, run_minute_parameter_replay,
    )
    from rquant.minute_backtest_performance import build_minute_performance
    from rquant.minute_backtest_producer import minute_metadata_identities
    from tests.support.minute_parameter_formal_fixture import parameter_source_seed

    raw = current_recipe().model_dump(mode="json")
    raw["parameters"]["end_date"] = "2026-08-03"
    recipe = MinuteParameterSet.model_validate(raw)
    with minute_parameter_validation_scope():
        seed = parameter_source_seed(tmp_path / "explicit-v2-source", recipe,
            days=(date(2026, 7, 31), date(2026, 8, 3)), sparse=True)
        audit, snapshot = minute_metadata_identities(seed)
        source = seed.freeze(audit_run_id=audit.audit_run_id, dataset_snapshot_id=snapshot.snapshot_id)
        formal = MinuteParameterFormalParameters.from_frozen(source)
        assert MinuteParameterFormalParameters.model_validate_json(formal.model_dump_json()) == formal
        assert formal.parameter_set_json == recipe.model_dump_json()
        assert formal.parameter_hash == recipe.fingerprint and formal.native_strategy_version == 1
        assert source.native_registration.execution_binding.entry_evaluator_version == "2.1.0"
        run_input = MinuteParameterFormalRunInput.from_frozen(source)
        assert run_input.parameters == formal
        replay = run_minute_parameter_replay(source.runtime,
            expected=MinuteParameterRuntimeReceipt(frozen=source.runtime),
            research_root=tmp_path / "actual-original-runtime")
        assert replay.parameters == recipe and replay.parameters.evaluator_semantic_version == "2.1.0"
        assert replay.signals and replay.fills
        assert all(signal.evidence["minute_parameter_set_json"] == recipe.model_dump_json() for signal in replay.signals)
        assert tuple(day.trade_date for day in replay.daily_valuations) == (date(2026, 7, 31), date(2026, 8, 3))
        entry, = (record for record in replay.queue_records if record.signal.action is SignalAction.B_INTENT)
        assert entry.quote.context.acquisition_available_date == date(2026, 8, 3)
        assert entry.signal.event_time.date() == date(2026, 7, 31)
        tables = minute_parameter_result_tables(replay)
        assert set(tables) == {"signals", "orders", "fills", "paper_queue", "account", "daily_valuations",
            "execution_profile", "replay_summary"}
        summary = MinuteParameterReplaySummary.model_validate_json(tables["replay_summary"].payload.iloc[0])
        assert summary.parameters.model_dump_json() == formal.parameter_set_json
        assert MinuteParameterReplayResult.model_validate_json(replay.model_dump_json()) == replay

        # A separate typed incomplete result exercises the unchanged math gate;
        # it is not a claim that this complete synthetic run lost a quote.
        incomplete = replay.model_dump(mode="python")
        incomplete["status"], incomplete["daily_status"] = "incomplete", "unavailable"
        incomplete["daily_valuations"][1].update(status="unavailable", account=None, price_proofs=(),
            unavailable_reasons=("synthetic_missing_next_session_price",))
        missing = MinuteParameterReplayResult.model_validate(incomplete)
        performance = build_minute_performance(missing, runtime=source.runtime)
        assert performance.status == "unavailable" and performance.metrics is None
        assert tuple(day.trade_date for day in performance.daily) == (date(2026, 7, 31), date(2026, 8, 3))
        assert performance.daily[1].nav is None and performance.daily[1].daily_return is None
        assert performance.daily[1].normalized_nav is None and performance.daily[1].drawdown is None
        assert performance.monthly == ()
