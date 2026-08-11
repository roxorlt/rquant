from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from rquant.runtime_shadow_sources import (
    LegacyMonitorEvent,
    LegacySurgeEvent,
    isolated_signal_observations,
    isolated_signals_to_shadow_observations,
    legacy_monitor_observations,
    legacy_monitor_rows_to_shadow_observations,
    legacy_records_raw_input_id,
    legacy_surge_file_raw_input_id,
    read_isolated_runner_shadow_observations,
    read_isolated_runner_shadow_snapshot,
    read_legacy_surge_events_shadow_snapshot,
    read_legacy_surge_shadow_observations,
    read_legacy_surge_shadow_snapshot,
    runner_source_raw_input_id,
)
from rquant.runtime_shadow_validation import (
    CompletionAttestationClaims,
    HmacCompletionAttestationAuthority,
    ShadowSourceCompletionReceipt,
    ShadowStrategyBinding,
    shadow_completion_receipt_body_sha256,
    shadow_session_boundaries,
)
from rquant.signal_contracts import SignalAction, SignalEnvelope
from rquant.signal_router_runtime import (
    RouteSourceDescriptor,
    RunnerSignalBatch,
    SourceSnapshot,
)
from rquant.strategy_runner import RunnerSignalRecord

TRADE_DATE = date(2026, 7, 31)
COMMIT = "a" * 40
EXPORTED_AT = datetime(2026, 7, 31, 7, 10, tzinfo=UTC)
CALENDAR_AUTHORITY_ID = "5590" * 16
SOURCE_CALENDAR_AUTHORITY_ID = "b" * 64
ATTESTATION_AUTHORITY = HmacCompletionAttestationAuthority(
    key_id="shadow-source-test-key-v1",
    secret=b"shadow-source-test-completion-attestation-key",
)


def _binding(strategy_id: str = "n_shape", version: int = 1) -> ShadowStrategyBinding:
    return ShadowStrategyBinding(
        strategy_id=strategy_id,
        strategy_version=version,
        definition_fingerprint="1" * 64,
        executable_fingerprint="2" * 64,
    )


def _completion_receipt(
    *,
    source: str,
    source_id: str,
    input_identity: str,
    complete_through: datetime | None = None,
    producer_version: str,
    high_watermark: int = 0,
) -> ShadowSourceCompletionReceipt:
    _session_open, session_close = shadow_session_boundaries(TRADE_DATE)
    authority = (
        {
            "producer_service_id": "strategy-live",
            "producer_instance_id": "test-primary",
            "runner_generation_id": "e" * 64,
            "signal_authority_generation_id": "a" * 64,
            "calendar_generation_id": "b" * 64,
            "last_sequence": 0,
            "high_watermark": high_watermark,
            "route_receipts_id": "c" * 64,
            "feature_source_generation_id": "f" * 64,
            "feature_close_marker_id": "1" * 64,
            "feature_segment_chain_hash": "2" * 64,
            "segment_start_sequence": 0,
            "segment_record_count": high_watermark,
            "segment_chain_hash": "3" * 64,
        }
        if source == "isolated"
        else {}
    )
    receipt = ShadowSourceCompletionReceipt(
        evidence_origin="production",
        source=source,
        source_id=source_id,
        trade_date=TRADE_DATE,
        session_close_at=session_close,
        complete_through=complete_through or session_close,
        input_identity=input_identity,
        produced_at=EXPORTED_AT,
        producer_commit=COMMIT,
        producer_version=producer_version,
        **authority,
    )
    if source != "isolated":
        return receipt
    binding = _binding()
    claims = CompletionAttestationClaims(
        completion_receipt_body_sha256=shadow_completion_receipt_body_sha256(receipt),
        trade_date=TRADE_DATE,
        session_close_at=session_close,
        source_id=source_id,
        input_identity=input_identity,
        strategy_id=binding.strategy_id,
        strategy_version=binding.strategy_version,
        strategy_registration_fingerprint=binding.definition_fingerprint,
        strategy_spec_fingerprint="d" * 64,
        executable_fingerprint=binding.executable_fingerprint,
        candidate_schema_fingerprint="4" * 64,
        feature_registration_fingerprint="5" * 64,
        feature_contract_fingerprint="6" * 64,
        routing_policy_fingerprint="7" * 64,
        producer_manifest_fingerprint="8" * 64,
        producer_commit=COMMIT,
        producer_version=producer_version,
        producer_service_id="strategy-live",
        producer_instance_id="test-primary",
        calendar_generation_id="b" * 64,
        feature_source_generation_id="f" * 64,
        feature_close_marker_id="1" * 64,
        feature_segment_chain_hash="2" * 64,
        runner_generation_id="e" * 64,
        runner_segment_start_sequence=0,
        runner_segment_final_sequence=high_watermark,
        runner_segment_record_count=high_watermark,
        runner_segment_chain_hash="3" * 64,
        signal_authority_generation_id="a" * 64,
        route_receipts_id="c" * 64,
    )
    payload = receipt.model_dump(mode="python", exclude={"receipt_id"})
    payload["completion_attestation"] = ATTESTATION_AUTHORITY.issue(claims)
    return ShadowSourceCompletionReceipt.model_validate(payload)


def _surge_receipt(
    path: Path,
    *,
    complete_through: datetime | None = None,
) -> ShadowSourceCompletionReceipt:
    return _completion_receipt(
        source="legacy",
        source_id="legacy-surge-jsonl",
        input_identity=legacy_surge_file_raw_input_id(path, trade_date=TRADE_DATE),
        complete_through=complete_through,
        producer_version="legacy-surge-test-v1",
    )


def _signal(
    *,
    action: SignalAction = SignalAction.B_INTENT,
    strategy_id: str = "n_shape",
    version: str = "1",
    code: str = "600001.SH",
    evidence: dict[str, str] | None = None,
) -> SignalEnvelope:
    event_time = datetime(2026, 7, 31, 1, 33, tzinfo=UTC)
    return SignalEnvelope(
        schema_version=1,
        strategy_id=strategy_id,
        strategy_version=version,
        parameter_fingerprint="b" * 64,
        dataset_snapshot_id="c" * 64,
        feature_snapshot_id="d" * 64,
        event_time=event_time,
        available_at=event_time + timedelta(seconds=3),
        candidate_id=code,
        action=action,
        reason_codes=("entry",),
        evidence=evidence or {"visible_minute": "09:33"},
        expires_at=event_time + timedelta(minutes=5),
        producer_commit=COMMIT,
    )


def test_monitor_events_preserve_n_shape_watch_then_buy_state_machine() -> None:
    events = (
        LegacyMonitorEvent.model_validate(
            {
                "trade_date": TRADE_DATE,
                "ts_code": "600001.SH",
                "level": "attack_strong_carry",
                "trigger_time": datetime(2026, 7, 31, 9, 31),
                "trigger_price": 10.2,
                "level_price": 10.0,
                "trigger_type": "strong_carry",
                "pool": "pool1",
            }
        ),
        LegacyMonitorEvent.model_validate(
            {
                "trade_date": TRADE_DATE,
                "ts_code": "600001.SH",
                "level": "attack_break_high",
                "trigger_time": datetime(2026, 7, 31, 9, 33),
                "trigger_price": 10.5,
                "level_price": 10.4,
                "trigger_type": "break_t_high",
                "pool": "pool1",
            }
        ),
    )

    observations = legacy_monitor_observations(
        events,
        trade_date=TRADE_DATE,
        exported_at=EXPORTED_AT,
        producer_commit=COMMIT,
        binding=_binding(),
    )

    assert [item.action for item in observations] == ["watch", "b_intent"]
    assert [item.event_time for item in observations] == [
        datetime(2026, 7, 31, 1, 31, tzinfo=UTC),
        datetime(2026, 7, 31, 1, 33, tzinfo=UTC),
    ]
    assert all(item.strategy_id == "n_shape" for item in observations)
    assert all(item.available_at == EXPORTED_AT for item in observations)
    assert all(item.availability_basis == "export_observed_proxy" for item in observations)


def test_monitor_does_not_invent_strong_carry_from_breakout_only() -> None:
    observations = legacy_monitor_observations(
        (
            LegacyMonitorEvent.model_validate(
                {
                    "trade_date": TRADE_DATE,
                    "ts_code": "600001.SH",
                    "level": "attack_break_high",
                    "trigger_time": datetime(2026, 7, 31, 9, 33),
                    "trigger_price": 10.5,
                    "level_price": 10.4,
                    "trigger_type": "break_t_high",
                    "pool": "pool1",
                }
            ),
        ),
        trade_date=TRADE_DATE,
        exported_at=EXPORTED_AT,
        producer_commit=COMMIT,
        binding=_binding(),
    )

    assert len(observations) == 1
    assert observations[0].action == "b_intent"


def test_monitor_row_adapter_preserves_the_same_event_semantics() -> None:
    rows = (
        {
            "trade_date": TRADE_DATE,
            "ts_code": "600001.SH",
            "level": "attack_strong_carry",
            "trigger_time": datetime(2026, 7, 31, 9, 31),
            "trigger_price": 10.2,
            "level_price": 10.0,
            "trigger_type": "strong_carry",
            "pool": "pool1",
        },
        {
            "trade_date": TRADE_DATE,
            "ts_code": "600001.SH",
            "level": "attack_break_high",
            "trigger_time": datetime(2026, 7, 31, 9, 33),
            "trigger_price": 10.5,
            "level_price": 10.4,
            "trigger_type": "break_t_high",
            "pool": "pool1",
        },
    )

    from_rows = legacy_monitor_rows_to_shadow_observations(
        rows,
        trade_date=TRADE_DATE,
        exported_at=EXPORTED_AT,
        producer_commit=COMMIT,
        binding=_binding(),
    )
    direct = legacy_monitor_observations(
        tuple(LegacyMonitorEvent.model_validate(row) for row in rows),
        trade_date=TRADE_DATE,
        exported_at=EXPORTED_AT,
        producer_commit=COMMIT,
        binding=_binding(),
    )

    assert from_rows == direct


def test_monitor_rejects_export_time_before_observed_event() -> None:
    with pytest.raises(ValueError, match="exported_at"):
        legacy_monitor_observations(
            (
                LegacyMonitorEvent.model_validate(
                    {
                        "trade_date": TRADE_DATE,
                        "ts_code": "600001.SH",
                        "level": "attack_strong_carry",
                        "trigger_time": datetime(2026, 7, 31, 9, 31),
                        "trigger_price": 10.2,
                        "level_price": 10.0,
                        "trigger_type": "strong_carry",
                        "pool": "pool1",
                    }
                ),
                LegacyMonitorEvent.model_validate(
                    {
                        "trade_date": TRADE_DATE,
                        "ts_code": "600001.SH",
                        "level": "attack_break_high",
                        "trigger_time": datetime(2026, 7, 31, 9, 33),
                        "trigger_price": 10.5,
                        "level_price": 10.4,
                        "trigger_type": "break_t_high",
                        "pool": "pool1",
                    }
                ),
            ),
            trade_date=TRADE_DATE,
            exported_at=datetime(2026, 7, 31, 1, 30, tzinfo=UTC),
            producer_commit=COMMIT,
            binding=_binding(),
        )


def test_surge_reader_keeps_first_confirmed_and_excludes_unbuyable(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    rows = (
        {
            "ts_code": "300001.SZ",
            "name": "first",
            "confirmed_at": "09:35",
            "status": "confirmed",
            "rel_cum": 2.8,
        },
        {
            "ts_code": "300001.SZ",
            "name": "duplicate",
            "confirmed_at": "09:36",
            "status": "confirmed",
            "rel_cum": 3.0,
        },
        {
            "ts_code": "688001.SH",
            "name": "sealed",
            "confirmed_at": "09:34",
            "status": "unbuyable",
            "rel_cum": 4.0,
        },
    )
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")

    observations = read_legacy_surge_shadow_observations(
        path,
        trade_date=TRADE_DATE,
        exported_at=EXPORTED_AT,
        producer_commit=COMMIT,
        binding=_binding("growth_board_surge"),
        completion_receipt=_surge_receipt(path),
    )

    assert len(observations) == 1
    assert observations[0].strategy_id == "growth_board_surge"
    assert observations[0].ts_code == "300001.SZ"
    assert observations[0].event_time == datetime(2026, 7, 31, 1, 35, tzinfo=UTC)
    assert observations[0].availability_basis == "export_observed_proxy"


def test_surge_snapshot_identity_binds_filtered_raw_records(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    confirmed = {
        "ts_code": "300001.SZ",
        "confirmed_at": "09:35",
        "status": "confirmed",
    }
    filtered = {
        "ts_code": "688001.SH",
        "confirmed_at": "09:34",
        "status": "unbuyable",
        "name": "first",
    }
    path.write_text(
        "".join(json.dumps(row) + "\n" for row in (confirmed, filtered)),
        encoding="utf-8",
    )
    first = read_legacy_surge_shadow_snapshot(
        path,
        trade_date=TRADE_DATE,
        exported_at=EXPORTED_AT,
        producer_commit=COMMIT,
        binding=_binding("growth_board_surge"),
        completion_receipt=_surge_receipt(path),
    )
    filtered["name"] = "changed-but-still-filtered"
    path.write_text(
        "".join(json.dumps(row) + "\n" for row in (confirmed, filtered)),
        encoding="utf-8",
    )
    second = read_legacy_surge_shadow_snapshot(
        path,
        trade_date=TRADE_DATE,
        exported_at=EXPORTED_AT,
        producer_commit=COMMIT,
        binding=_binding("growth_board_surge"),
        completion_receipt=_surge_receipt(path),
    )

    assert first.observations == second.observations
    assert first.upstream_snapshot_id != second.upstream_snapshot_id


def test_surge_reader_rejects_symlink_and_malformed_records(tmp_path: Path) -> None:
    target = tmp_path / "events.jsonl"
    target.write_text("{}\n", encoding="utf-8")
    link = tmp_path / "events-link.jsonl"
    link.symlink_to(target)
    with pytest.raises(ValueError, match="regular file"):
        read_legacy_surge_shadow_observations(
            link,
            trade_date=TRADE_DATE,
            exported_at=EXPORTED_AT,
            producer_commit=COMMIT,
            binding=_binding("growth_board_surge"),
        )

    with pytest.raises(ValueError, match="surge record"):
        read_legacy_surge_shadow_observations(
            target,
            trade_date=TRADE_DATE,
            exported_at=EXPORTED_AT,
            producer_commit=COMMIT,
            binding=_binding("growth_board_surge"),
        )


def test_surge_reader_rejects_file_and_line_over_budget(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    path.write_text(
        json.dumps(
            {
                "ts_code": "300001.SZ",
                "name": "x" * 200,
                "confirmed_at": "09:35",
                "status": "confirmed",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="budget"):
        read_legacy_surge_shadow_observations(
            path,
            trade_date=TRADE_DATE,
            exported_at=EXPORTED_AT,
            producer_commit=COMMIT,
            binding=_binding("growth_board_surge"),
            max_file_bytes=64,
            max_line_bytes=32,
        )


def test_surge_reader_counts_filtered_raw_records_before_parsing(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    rows = (
        {
            "ts_code": "688001.SH",
            "name": "filtered-one",
            "confirmed_at": "09:34",
            "status": "unbuyable",
        },
        {
            "ts_code": "688002.SH",
            "name": "filtered-two",
            "confirmed_at": "09:35",
            "status": "unbuyable",
        },
    )
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")

    with pytest.raises(ValueError, match="record.*budget"):
        read_legacy_surge_shadow_snapshot(
            path,
            trade_date=TRADE_DATE,
            exported_at=EXPORTED_AT,
            producer_commit=COMMIT,
            binding=_binding("growth_board_surge"),
            max_records=1,
        )


def test_surge_reader_budgets_eight_megabyte_filtered_record_before_filtering(
    tmp_path: Path,
) -> None:
    path = tmp_path / "events.jsonl"
    path.write_text(
        json.dumps(
            {
                "ts_code": "688001.SH",
                "name": "x" * (8 * 1024 * 1024),
                "confirmed_at": "09:34",
                "status": "unbuyable",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    receipt = _surge_receipt(path)

    with pytest.raises(ValueError, match="read budget"):
        read_legacy_surge_shadow_snapshot(
            path,
            trade_date=TRADE_DATE,
            exported_at=EXPORTED_AT,
            producer_commit=COMMIT,
            binding=_binding("growth_board_surge"),
            completion_receipt=receipt,
            max_file_bytes=10 * 1024 * 1024,
            max_line_bytes=8 * 1024 * 1024,
        )

    accepted = read_legacy_surge_shadow_snapshot(
        path,
        trade_date=TRADE_DATE,
        exported_at=EXPORTED_AT,
        producer_commit=COMMIT,
        binding=_binding("growth_board_surge"),
        completion_receipt=receipt,
        max_file_bytes=10 * 1024 * 1024,
        max_line_bytes=9 * 1024 * 1024,
    )
    assert accepted.observations == ()


def test_legacy_read_time_cannot_substitute_for_producer_completion(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    path.write_text(
        json.dumps(
            {
                "ts_code": "688001.SH",
                "confirmed_at": "09:34",
                "status": "unbuyable",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="complete session"):
        read_legacy_surge_shadow_snapshot(
            path,
            trade_date=TRADE_DATE,
            exported_at=EXPORTED_AT,
            producer_commit=COMMIT,
            binding=_binding("growth_board_surge"),
            completion_receipt=_surge_receipt(
                path,
                complete_through=datetime(2026, 7, 31, 2, 0, tzinfo=UTC),
            ),
        )


def test_legacy_stream_stops_at_raw_record_budget_before_materializing_tail() -> None:
    records = (
        LegacySurgeEvent(ts_code="688001.SH", confirmed_at="09:34", status="unbuyable"),
        LegacySurgeEvent(ts_code="688002.SH", confirmed_at="09:35", status="unbuyable"),
    )
    receipt = _completion_receipt(
        source="legacy",
        source_id="legacy-surge-events",
        input_identity=legacy_records_raw_input_id(
            records,
            source_id="legacy-surge-events",
            trade_date=TRADE_DATE,
        ),
        producer_version="legacy-surge-test-v1",
    )

    def one_shot_records() -> object:
        yield records[0]
        yield records[1]
        raise AssertionError("reader consumed beyond the bounded prefix")

    with pytest.raises(ValueError, match="record budget"):
        read_legacy_surge_events_shadow_snapshot(
            one_shot_records(),
            trade_date=TRADE_DATE,
            exported_at=EXPORTED_AT,
            producer_commit=COMMIT,
            binding=_binding("growth_board_surge"),
            completion_receipt=receipt,
            max_records=1,
        )


def test_isolated_adapter_uses_real_availability_and_strategy_comparable_actions() -> None:
    buy = _signal()
    watch = _signal(action=SignalAction.WATCH, code="600002.SH")
    growth_watch = _signal(
        action=SignalAction.WATCH,
        strategy_id="growth_board_surge",
        code="300001.SZ",
    )

    observations = isolated_signal_observations(
        (watch, buy, growth_watch),
        trade_date=TRADE_DATE,
        binding=_binding(),
    )

    assert len(observations) == 2
    assert {item.action for item in observations} == {"watch", "b_intent"}
    buy_observation = next(item for item in observations if item.action == "b_intent")
    assert buy_observation.source == "isolated"
    assert buy_observation.available_at == buy.available_at
    assert buy_observation.availability_basis == "observed_completion"
    assert buy_observation.evidence_id == buy.signal_id
    assert buy_observation.upstream_event_id == buy.signal_id


def test_legacy_isolated_adapter_keeps_n_shape_watch_and_growth_buy_only() -> None:
    n_watch = _signal(action=SignalAction.WATCH, code="600002.SH")
    n_buy = _signal(code="600003.SH")
    growth_watch = _signal(
        action=SignalAction.WATCH,
        strategy_id="growth_board_surge",
        code="300001.SZ",
    )
    growth_buy = _signal(
        strategy_id="growth_board_surge",
        code="300002.SZ",
    )

    observations = isolated_signals_to_shadow_observations(
        (n_watch, n_buy, growth_watch, growth_buy),
        trade_date=TRADE_DATE,
        bindings=(_binding(), _binding("growth_board_surge")),
    )

    assert {(item.strategy_id, item.action) for item in observations} == {
        ("n_shape", "watch"),
        ("n_shape", "b_intent"),
        ("growth_board_surge", "b_intent"),
    }


def test_multi_binding_adapter_freezes_one_shot_signal_iterable() -> None:
    signals = iter(
        (
            _signal(strategy_id="n_shape", code="600001.SH"),
            _signal(strategy_id="growth_board_surge", code="300001.SZ"),
        )
    )

    observations = isolated_signals_to_shadow_observations(
        signals,
        trade_date=TRADE_DATE,
        bindings=(_binding(), _binding("growth_board_surge")),
    )

    assert {item.strategy_id for item in observations} == {
        "n_shape",
        "growth_board_surge",
    }


def test_isolated_adapter_rejects_duplicate_upstream_signal() -> None:
    signal = _signal()

    with pytest.raises(ValueError, match="duplicate upstream signal"):
        isolated_signal_observations(
            (signal, signal),
            trade_date=TRADE_DATE,
            binding=_binding(),
        )


@pytest.mark.parametrize("version", ["v1", "0", "01", "1.0"])
def test_isolated_adapter_rejects_noncanonical_strategy_version(version: str) -> None:
    with pytest.raises(ValueError, match="strategy_version"):
        isolated_signal_observations(
            (_signal(version=version),),
            trade_date=TRADE_DATE,
            binding=_binding(),
        )


class _RunnerSource:
    def __init__(
        self,
        signals: tuple[SignalEnvelope, ...],
        *,
        mutate_snapshot_after_first_read: bool = False,
        complete_through: datetime | None = None,
    ) -> None:
        self.records = tuple(
            RunnerSignalRecord(sequence=index, signal=signal)
            for index, signal in enumerate(signals, start=1)
        )
        self.mutate_snapshot_after_first_read = mutate_snapshot_after_first_read
        self.complete_through = complete_through
        self.read_count = 0

    def _descriptor(self, *, generation: str = "e" * 64) -> RouteSourceDescriptor:
        return RouteSourceDescriptor(
            source_id="n-shape-v1",
            generation_id=generation,
            strategy_spec_fingerprint="d" * 64,
            first_sequence=1,
            high_watermark=len(self.records),
        )

    def read_completed_batch(
        self,
        *,
        trade_date: date,
        after_sequence: int,
        limit: int,
    ) -> RunnerSignalBatch:
        assert trade_date == TRADE_DATE
        self.read_count += 1
        generation = (
            "f" * 64 if self.mutate_snapshot_after_first_read and self.read_count > 1 else "e" * 64
        )
        return RunnerSignalBatch(
            snapshot=SourceSnapshot(descriptor=self._descriptor(generation=generation)),
            after_sequence=after_sequence,
            limit=limit,
            records=tuple(record for record in self.records if record.sequence > after_sequence)[
                :limit
            ],
        )

    def read_completion_receipt(self, *, trade_date: date) -> ShadowSourceCompletionReceipt:
        assert trade_date == TRADE_DATE
        return _completion_receipt(
            source="isolated",
            source_id="n-shape-v1",
            input_identity=runner_source_raw_input_id(self._descriptor(), self.records),
            complete_through=self.complete_through,
            producer_version="test-runner-v1",
            high_watermark=len(self.records),
        )


def test_runner_reader_freezes_and_pages_one_complete_source_snapshot() -> None:
    signals = tuple(_signal(code=f"60000{index}.SH") for index in range(1, 5))
    source = _RunnerSource(signals)

    observations = read_isolated_runner_shadow_observations(
        source,
        trade_date=TRADE_DATE,
        observed_at=EXPORTED_AT,
        binding=_binding(),
        expected_calendar_authority_id=SOURCE_CALENDAR_AUTHORITY_ID,
        attestation_verifier=ATTESTATION_AUTHORITY,
        batch_size=2,
        max_records=10,
    )

    assert len(observations) == 4
    assert source.read_count == 2


def test_runner_snapshot_binds_frozen_descriptor_and_cutoff() -> None:
    source = _RunnerSource((_signal(),))

    snapshot = read_isolated_runner_shadow_snapshot(
        source,
        trade_date=TRADE_DATE,
        observed_at=EXPORTED_AT,
        binding=_binding(),
        expected_calendar_authority_id=SOURCE_CALENDAR_AUTHORITY_ID,
        attestation_verifier=ATTESTATION_AUTHORITY,
        batch_size=10,
        max_records=10,
    )

    assert snapshot.source_id == "n-shape-v1"
    assert snapshot.captured_at == EXPORTED_AT
    assert snapshot.complete_through == shadow_session_boundaries(TRADE_DATE)[1]
    assert len(snapshot.observations) == 1
    assert len(snapshot.upstream_snapshot_id) == 64


def test_runner_reader_rejects_completion_from_another_calendar_generation() -> None:
    with pytest.raises(ValueError, match="calendar.*generation|calendar.*authority"):
        read_isolated_runner_shadow_snapshot(
            _RunnerSource((_signal(),)),
            trade_date=TRADE_DATE,
            observed_at=EXPORTED_AT,
            binding=_binding(),
            expected_calendar_authority_id=CALENDAR_AUTHORITY_ID,
            attestation_verifier=ATTESTATION_AUTHORITY,
        )


def test_runner_read_time_cannot_substitute_for_producer_completion() -> None:
    source = _RunnerSource(
        (_signal(),),
        complete_through=datetime(2026, 7, 31, 2, 0, tzinfo=UTC),
    )

    with pytest.raises(ValueError, match="completion receipt"):
        read_isolated_runner_shadow_snapshot(
            source,
            trade_date=TRADE_DATE,
            observed_at=EXPORTED_AT,
            binding=_binding(),
            expected_calendar_authority_id=SOURCE_CALENDAR_AUTHORITY_ID,
            attestation_verifier=ATTESTATION_AUTHORITY,
        )


def test_runner_snapshot_identity_binds_filtered_raw_records() -> None:
    comparable = _signal()
    first_filtered = _signal(
        strategy_id="growth_board_surge",
        code="300001.SZ",
        evidence={"filtered": "first"},
    )
    second_filtered = _signal(
        strategy_id="growth_board_surge",
        code="300001.SZ",
        evidence={"filtered": "changed"},
    )

    first = read_isolated_runner_shadow_snapshot(
        _RunnerSource((comparable, first_filtered)),
        trade_date=TRADE_DATE,
        observed_at=EXPORTED_AT,
        binding=_binding(),
        expected_calendar_authority_id=SOURCE_CALENDAR_AUTHORITY_ID,
        attestation_verifier=ATTESTATION_AUTHORITY,
    )
    second = read_isolated_runner_shadow_snapshot(
        _RunnerSource((comparable, second_filtered)),
        trade_date=TRADE_DATE,
        observed_at=EXPORTED_AT,
        binding=_binding(),
        expected_calendar_authority_id=SOURCE_CALENDAR_AUTHORITY_ID,
        attestation_verifier=ATTESTATION_AUTHORITY,
    )

    assert first.observations == second.observations
    assert first.upstream_snapshot_id != second.upstream_snapshot_id


def test_runner_reader_budgets_filtered_raw_bytes_before_signal_filtering() -> None:
    filtered = _signal(
        strategy_id="growth_board_surge",
        code="300001.SZ",
        evidence={"padding": "x" * (8 * 1024 * 1024)},
    )

    with pytest.raises(ValueError, match="record.*byte budget"):
        read_isolated_runner_shadow_snapshot(
            _RunnerSource((filtered,)),
            trade_date=TRADE_DATE,
            observed_at=EXPORTED_AT,
            binding=_binding(),
            expected_calendar_authority_id=SOURCE_CALENDAR_AUTHORITY_ID,
            attestation_verifier=ATTESTATION_AUTHORITY,
            max_raw_bytes=9 * 1024 * 1024,
            max_record_bytes=8 * 1024 * 1024,
        )

    accepted = read_isolated_runner_shadow_snapshot(
        _RunnerSource((filtered,)),
        trade_date=TRADE_DATE,
        observed_at=EXPORTED_AT,
        binding=_binding(),
        expected_calendar_authority_id=SOURCE_CALENDAR_AUTHORITY_ID,
        attestation_verifier=ATTESTATION_AUTHORITY,
        max_raw_bytes=10 * 1024 * 1024,
        max_record_bytes=9 * 1024 * 1024,
    )
    assert accepted.observations == ()


def test_runner_reader_fails_closed_when_snapshot_changes_during_paging() -> None:
    source = _RunnerSource(
        tuple(_signal(code=f"60000{index}.SH") for index in range(1, 4)),
        mutate_snapshot_after_first_read=True,
    )

    with pytest.raises(ValueError, match="snapshot changed"):
        read_isolated_runner_shadow_observations(
            source,
            trade_date=TRADE_DATE,
            observed_at=EXPORTED_AT,
            binding=_binding(),
            expected_calendar_authority_id=SOURCE_CALENDAR_AUTHORITY_ID,
            attestation_verifier=ATTESTATION_AUTHORITY,
            batch_size=1,
            max_records=10,
        )


def test_runner_reader_rejects_signals_not_yet_visible_at_report_cutoff() -> None:
    source = _RunnerSource((_signal(),))

    with pytest.raises(ValueError, match="evaluation cutoff"):
        read_isolated_runner_shadow_observations(
            source,
            trade_date=TRADE_DATE,
            observed_at=datetime(2026, 7, 31, 1, 33, 2, tzinfo=UTC),
            binding=_binding(),
            expected_calendar_authority_id=SOURCE_CALENDAR_AUTHORITY_ID,
            attestation_verifier=ATTESTATION_AUTHORITY,
            batch_size=10,
            max_records=10,
        )
