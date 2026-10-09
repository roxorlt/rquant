"""Explicit synthetic parameter archive over the original physical raw fixture."""

from __future__ import annotations

import base64
import hashlib
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from rquant.minute_backtest_contracts import MinuteReplayMaterial
from rquant.minute_backtest_parameter_contracts import (
    FrozenMinuteParameterInput, MinuteParameterRuntimeReceipt, MinuteParameterStrategyBinding,
    MinuteParameterWork,
)
from rquant.minute_backtest_parameter_definition import (
    bootstrap_minute_parameter_definition, build_minute_parameter_definition,
)
from rquant.minute_backtest_parameter_features import (
    PARAMETER_CANDIDATE_FEATURE, MinuteParameterCandidate, MinuteParameterSessionFacts,
)
from rquant.minute_backtest_parameters import MinuteParameterSet
from rquant.live_contracts import LiveChannel
from rquant.live_spool import LiveBatchSpool
from rquant.market_minute_gateway import MarketMinuteGateway
from rquant.minute_backtest_source import restore_minute_runtime_source
from rquant.strategy_candidate_snapshot import (
    StrategyCandidatePriceBasis, StrategyCandidateRecord, StrategyCandidateSnapshotSpool,
)
from tests.integration.test_minute_backtest_runtime_parity import COMMIT, FROZEN_AT, _at, _input


def parameter_runtime_fixture(
    root: Path, parameters: MinuteParameterSet, *, days: tuple[date, ...] | None = None,
    sparse: bool = False, delayed_next_bar: bool = False, study_facts: bool = False,
) -> tuple[FrozenMinuteParameterInput, MinuteParameterRuntimeReceipt]:
    root.mkdir(mode=0o700, parents=True)
    (root / "raw-blueprint").mkdir(mode=0o700)
    original, raw_receipt = _input(root / "raw-blueprint", parameters.parameters.family, daily_quotes=True,
        **({"days": days} if days is not None else {}))
    code = "300001.SZ" if parameters.parameters.family == "growth_board_surge" else "600000.SH"
    frequency = parameters.parameters.freq
    if frequency != "1min":
        # Complete new synthetic bars, rather than relabeling a 1-minute payload.
        # The original blueprint remains unchanged in raw-blueprint.
        minutes = {
            "5min": (*range(35, 71, 5), 355, 360),
            "15min": (*range(45, 151, 15), 345, 360),
            "30min": (60, 90, 120, 150, 270, 300, 330, 360),
            "60min": (90, 150, 300, 360),
        }[frequency]
        synthetic = LiveBatchSpool(root / "physical-frequency-synthetic-market")
        ticks = []
        for day_index, day in enumerate(original.daily_trade_dates):
            for index, minute in enumerate(minutes):
                actual_minute = minute + (5 if delayed_next_bar and frequency == "5min" and 2 <= index < 8 else 0)
                event = _at(day, actual_minute)
                price = (9.9 if index == 0 else 10.1+(index-1)*.02) if day_index == 0 else 9.4
                volume = 150.0 if day_index == 0 and index == 0 else 1500.0
                frame = pd.DataFrame([dict(ts_code=code, trade_time=event, open=price,
                    high=price+.01, low=price-.01, close=price, vol=volume, amount=volume*price)])
                payload = MarketMinuteGateway.encode_payload(MarketMinuteGateway.normalize_frame(frame))
                available = event if minute == 360 else event + timedelta(seconds=5)
                from rquant.live_contracts import BatchEnvelope, BatchQualityStatus

                sequence = len(ticks)
                synthetic.publish(BatchEnvelope(schema_version=1, channel=LiveChannel.MARKET_MINUTE,
                    dataset_id="market_minute", source="explicit-synthetic-complete-"+frequency,
                    source_request_id=f"{frequency}-{sequence}", batch_id=f"{frequency}-{sequence}", sequence=sequence,
                    revision=1, event_time_start=event, event_time_end=event, source_time=event,
                    received_at=available, available_at=available, row_count=1,
                    content_sha256=hashlib.sha256(payload).hexdigest(), quality_status=BatchQualityStatus.PUBLISHED,
                    producer_version="1", producer_commit=COMMIT), payload)
                ticks.append(available)
        history = []
        for day in pd.bdate_range("2026-07-02", "2026-07-30")[-20:]:
            for minute in minutes:
                history.append(dict(ts_code=code, trade_time=_at(day.date(), minute),
                    available_at=_at(day.date(), minute)+timedelta(seconds=5), open=10.0, high=10.02,
                    low=9.98, close=10.0, vol=100.0, amount=1000.0))
        history_path = root / "physical-frequency-warmup.parquet"
        pd.DataFrame(history).to_parquet(history_path, index=False)
        payload = history_path.read_bytes()
        materials = [item for item in original.materials if not item.relative_path.startswith("market/")
            and item.relative_path != "warmup.parquet"]
        materials.append(MinuteReplayMaterial(relative_path="warmup.parquet", content_base64=base64.b64encode(payload).decode(),
            content_sha256=hashlib.sha256(payload).hexdigest()))
        for path in sorted(synthetic.root.rglob("*")):
            if not path.is_file() or any(part.startswith(".") for part in path.relative_to(synthetic.root).parts):
                continue
            payload = path.read_bytes()
            materials.append(MinuteReplayMaterial(relative_path="market/"+path.relative_to(synthetic.root).as_posix(),
                content_base64=base64.b64encode(payload).decode(), content_sha256=hashlib.sha256(payload).hexdigest()))
        work = original.work.model_copy(update={"raw_rows": len(ticks), "market_batches": len(ticks),
            "warmup_rows": len(history), "static_rows": original.work.static_rows-len(original.tick_times)+len(ticks)})
        if delayed_next_bar:
            expiry_ticks = tuple(_at(day, 47)+timedelta(seconds=6) for day in original.daily_trade_dates)
            ticks.extend(expiry_ticks)
            work = work.model_copy(update={"static_rows": work.static_rows + len(expiry_ticks)})
        original = type(original).model_validate(original.model_dump(mode="python") | {
            "materials": tuple(sorted(materials, key=lambda item: item.relative_path)),
            "tick_times": tuple(sorted(ticks)), "work": work})
    elif sparse:
        # New, explicitly synthetic raw publications. The inherited blueprint is
        # retained separately; no captured source or complete receipt is edited.
        restored = restore_minute_runtime_source(original, expected=raw_receipt,
            research_root=root / "blueprint-read")
        archive = LiveBatchSpool(restored.root / "market", read_only=True)
        selected = LiveBatchSpool(root / "sparse-synthetic-market")
        ticks = []
        for record in archive.list_after(LiveChannel.MARKET_MINUTE, sequence=-1):
            stamp = record.envelope.event_time_end
            if stamp.minute not in (30, 31, 32, 33, 59, 0):
                continue
            fields = record.envelope.model_dump(mode="python") | {"sequence": len(ticks),
                "source": "explicit-sparse-synthetic-parameter", "batch_id": f"synthetic-{len(ticks)}",
                "source_request_id": f"synthetic-{len(ticks)}"}
            selected.publish(type(record.envelope).model_validate(fields), archive.read_payload(record))
            ticks.append(record.envelope.available_at)
        source_materials = [item for item in original.materials if not item.relative_path.startswith("market/")]
        for path in sorted(selected.root.rglob("*")):
            if not path.is_file() or any(part.startswith(".") for part in path.relative_to(selected.root).parts):
                continue
            payload = path.read_bytes()
            source_materials.append(MinuteReplayMaterial(relative_path="market/"+path.relative_to(selected.root).as_posix(),
                content_base64=base64.b64encode(payload).decode("ascii"), content_sha256=hashlib.sha256(payload).hexdigest()))
        work = original.work.model_copy(update={"raw_rows": len(ticks), "market_batches": len(ticks),
            "static_rows": original.work.static_rows-len(original.tick_times)+len(ticks)})
        original = type(original).model_validate(original.model_dump(mode="python") | {
            "materials": tuple(sorted(source_materials, key=lambda item: item.relative_path)),
            "tick_times": tuple(ticks), "work": work})
    definition = build_minute_parameter_definition(parameters, producer_commit=COMMIT)
    registered = bootstrap_minute_parameter_definition(root / "actual-parameter-definitions", parameters,
        producer_commit=COMMIT, registered_at=FROZEN_AT, available_at=FROZEN_AT)
    candidates = StrategyCandidateSnapshotSpool(root / "actual-parameter-candidates")
    facts = []
    for index, day in enumerate(original.daily_trade_dates):
        reference = original.start_date - timedelta(days=1) if index == 0 else original.daily_trade_dates[index - 1]
        candidate = MinuteParameterCandidate(family=parameters.parameters.family, parameter_hash=parameters.fingerprint,
            ts_code=code, pool="pool1" if parameters.parameters.family == "n_shape" else parameters.parameters.family,
            trade_date=day, reference_date=reference, available_at=_at(day, 25), name="合成样本",
            t_close=9.8 if index == 0 else 10.22,
            t_high=(10.115 if frequency == "60min" else 10.0) if index == 0 else 10.23,
            limit_up_price=12.0, auction_price=9.9, auction_vol_ratio_5d=1.0, auction_gap_pct_close=1.0,
            auction_prev5_count=5, is_st=False, listed_trading_days=300,
            prior_nonmissing_volume_ratios=(1.0,) * 20,
            static_factors={"ma_alignment": True, "large_net_vol_t1": 1.0},
            board_gap_up_ratio=0.5, board_auction_amount_ratio=2.0)
        if study_facts:
            # New, explicit synthetic static observations; the original blueprint
            # and every earlier frozen source remain unchanged.
            candidate = candidate.model_copy(update={"static_factors": {
                **candidate.static_factors, "accum_obv_change_20d_pct": 20.0,
                "accum_ad_flow_20d_pct": 10.0, "accum_up_down_amount_ratio_20d": 1.5,
                "accum_heavy_no_drop_days_20d": 2.0, "price_position_90d_pct": 60.0,
                "distance_to_high_90d_pct": 10.0, "market_up_ratio_pct": 50.0,
                "index_csi1000_pct_chg": 1.0, "price_percentile_250d": 0.1,
                "market_above_ma20_ratio_pct": 35.0}})
        fact = MinuteParameterSessionFacts(ts_code=code, trade_date=day,
            reference_date=reference, previous_close=candidate.t_close, session_pre_close=candidate.t_close,
            basis_available_at=candidate.available_at, source_snapshot_id=parameters.fingerprint,
            auction_query_complete=True, auction_available_at=_at(day, 25), auction_price=candidate.auction_price,
            seal_query_complete=True, seal_available_at=_at(day, 360), official_seal=None)
        fact = fact.model_copy(update={"source_snapshot_id": hashlib.sha256(fact.source_payload()).hexdigest()})
        candidates.publish_strategy_records(strategy_id=definition.strategy_id, strategy_version="1",
            definition_fingerprint=registered.fingerprint, executable_fingerprint=registered.executable_fingerprint,
            candidate_schema_fingerprint=definition.candidate_schema_fingerprint,
            static_feature_schema={key: field.contract_payload() for key, field in definition.static_feature_schema.items()},
            source_snapshot_ids={"synthetic_parameter_facts": fact.source_snapshot_id},
            trade_date=day, captured_at=candidate.available_at, producer_commit=COMMIT,
            rows=(StrategyCandidateRecord(strategy_id=definition.strategy_id, strategy_version="1",
                candidate_id=code, variant=candidate.pool, decision_at=candidate.available_at,
                available_at=candidate.available_at, effective_trade_date=day, reference_trade_date=reference,
                price_basis=StrategyCandidatePriceBasis.RAW,
                static_features={PARAMETER_CANDIDATE_FEATURE: candidate.model_dump_json()},
                reference_snapshot_ids={"synthetic_parameter_facts": fact.source_snapshot_id}),))
        facts.append(fact)
    materials = [item for item in original.materials if not item.relative_path.startswith("candidates/")]
    for path in sorted(candidates.root.rglob("*")):
        if not path.is_file() or path.name.startswith("."):
            continue
        payload = path.read_bytes()
        materials.append(MinuteReplayMaterial(relative_path="candidates/"+path.relative_to(candidates.root).as_posix(),
            content_base64=base64.b64encode(payload).decode("ascii"), content_sha256=hashlib.sha256(payload).hexdigest()))
    from rquant.minute_backtest_parameter_source import measure_minute_parameter_work

    measured = measure_minute_parameter_work(parameters, materials=tuple(materials), tick_times=original.tick_times,
        warmup_available_at=original.warmup_available_at, runtime_work=original.work, session_facts=tuple(facts))
    value = FrozenMinuteParameterInput(source_key="synthetic.parameter-runtime", source_version=1,
        owner_id=original.owner_id, producer_commit=COMMIT, available_at=FROZEN_AT,
        start_date=original.start_date, end_date=original.end_date, complete_through=original.complete_through,
        warmup_available_at=original.warmup_available_at, warmup_complete=True, holding_tail_complete=True,
        audit_run_id="explicit-synthetic-parameter-audit", dataset_snapshot_id="d" * 64,
        parameters=parameters, source_frequency=frequency, strategy=MinuteParameterStrategyBinding.from_registration(registered,
            parameters=parameters, producer_commit=COMMIT), market_calendar=original.market_calendar,
        execution_profile=original.execution_profile, parameter_work=measured, session_facts=tuple(facts),
        tick_times=original.tick_times, materials=tuple(sorted(materials, key=lambda item: item.relative_path)))
    return value, MinuteParameterRuntimeReceipt(frozen=value)
