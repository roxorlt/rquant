"""Strict adapters from legacy alerts and isolated signals to shadow evidence."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from collections.abc import Iterable, Mapping
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Literal, Protocol
from zoneinfo import ZoneInfo

from pydantic import Field, field_validator

from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.runtime_shadow_validation import (
    CompletionAttestationVerifier,
    ShadowObservation,
    ShadowSourceCompletionReceipt,
    ShadowStrategyBinding,
    shadow_session_boundaries,
    shadow_upstream_snapshot_id,
    verify_completion_attestation,
)
from rquant.signal_contracts import (
    CurrentSignalEnvelope,
    GitCommitClaimIdentity,
    SignalAction,
    SignalEnvelope,
    SignalEnvelopeFamily,
    parse_signal_envelope,
)
from rquant.signal_router_runtime import RouteSourceDescriptor, RunnerSignalBatch
from rquant.strategy_runner import RunnerSignalRecord
from rquant.strict_json import canonical_json_bytes

_CST = ZoneInfo("Asia/Shanghai")
_MONITOR_ACTIONS = {
    "attack_strong_carry": SignalAction.WATCH,
    "attack_break_high": SignalAction.B_INTENT,
}
_COMPARABLE_SIGNAL_ACTIONS = {
    "n_shape": frozenset((SignalAction.WATCH, SignalAction.B_INTENT)),
    "growth_board_surge": frozenset((SignalAction.B_INTENT,)),
}


class LegacyMonitorEvent(RuntimeContractModel):
    trade_date: date
    ts_code: str = Field(pattern=r"^[0-9]{6}\.(?:SH|SZ|BJ)$")
    level: Literal["attack_strong_carry", "attack_break_high"]
    trigger_time: datetime
    trigger_price: float | None = None
    level_price: float | None = None
    trigger_type: str | None = None
    pool: str | None = None
    body_upper: float | None = None
    body_lower: float | None = None

    @field_validator("level", mode="before")
    @classmethod
    def validate_level(cls, value: object) -> object:
        if value not in _MONITOR_ACTIONS:
            raise ValueError("unsupported legacy monitor level")
        return value

    @field_validator("trigger_time")
    @classmethod
    def validate_trigger_time(cls, value: datetime) -> datetime:
        if value.tzinfo is not None and value.utcoffset() is not None:
            raise ValueError("legacy monitor trigger_time must be naive Asia/Shanghai time")
        return value


class LegacySurgeEvent(RuntimeContractModel):
    ts_code: str = Field(pattern=r"^[0-9]{6}\.(?:SH|SZ|BJ)$")
    name: str = ""
    theme: str = ""
    confirmed_at: str = Field(pattern=r"^(?:0[9]|1[0-4]):[0-5][0-9]$")
    price: float = 0.0
    pct_chg: float = 0.0
    cum_amount: float = 0.0
    rel_cum: float = 0.0
    rough_ratio: float = 0.0
    minute_delta: float | None = None
    minute_delta_median: float | None = None
    room_to_limit_pct: float | None = None
    return_1m_pct: float | None = None
    outer_inner_ratio_approx: float | None = None
    price_source: str = "snapshot"
    status: Literal["confirmed", "unbuyable"] = "confirmed"

    @field_validator("confirmed_at")
    @classmethod
    def validate_market_time(cls, value: str) -> str:
        hour, minute = (int(part) for part in value.split(":"))
        observed = time(hour, minute)
        in_morning = time(9, 25) <= observed <= time(11, 30)
        in_afternoon = time(13, 0) <= observed <= time(15, 0)
        if not in_morning and not in_afternoon:
            raise ValueError("legacy surge confirmation is outside the A-share session")
        return value


class ShadowRunnerSignalSource(Protocol):
    def read_completion_receipt(
        self,
        *,
        trade_date: date,
    ) -> ShadowSourceCompletionReceipt: ...

    def read_completed_batch(
        self,
        *,
        trade_date: date,
        after_sequence: int,
        limit: int,
    ) -> RunnerSignalBatch: ...


class ShadowRunnerReadSnapshot(RuntimeContractModel):
    observations: tuple[ShadowObservation, ...]
    source_id: str = Field(min_length=1)
    raw_input_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    completion_receipt: ShadowSourceCompletionReceipt
    upstream_snapshot_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    captured_at: AwareUtcDatetime
    complete_through: AwareUtcDatetime


class ShadowLegacyReadSnapshot(RuntimeContractModel):
    observations: tuple[ShadowObservation, ...]
    source_id: str = Field(min_length=1)
    raw_input_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    completion_receipt: ShadowSourceCompletionReceipt
    upstream_snapshot_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    captured_at: AwareUtcDatetime
    complete_through: AwareUtcDatetime


def _update_framed_digest(digest: object, payload: bytes) -> None:
    digest.update(len(payload).to_bytes(8, "big"))
    digest.update(payload)


def _stream_raw_input_id(
    *,
    contract: str,
    descriptor: object,
    records_sha256: str,
    record_count: int,
    raw_bytes: int,
) -> str:
    return canonical_sha256(
        {
            "contract": contract,
            "descriptor": descriptor,
            "records_sha256": records_sha256,
            "record_count": record_count,
            "raw_bytes": raw_bytes,
        }
    )


def _raw_record_bytes(record: object) -> bytes:
    if isinstance(record, RuntimeContractModel):
        return canonical_json_bytes(record.model_dump(mode="json"))
    if isinstance(record, Mapping):

        def encode_special(value: object) -> object:
            if isinstance(value, datetime):
                return {"$datetime": value.isoformat(timespec="microseconds")}
            if isinstance(value, date):
                return {"$date": value.isoformat()}
            raise TypeError(f"unsupported raw record value: {type(value).__name__}")

        return json.dumps(
            dict(record),
            default=encode_special,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    raise TypeError("legacy raw record must be a mapping or runtime contract")


def legacy_records_raw_input_id(
    records: Iterable[object],
    *,
    source_id: str,
    trade_date: date,
) -> str:
    digest = hashlib.sha256()
    count = 0
    consumed = 0
    for record in records:
        payload = _raw_record_bytes(record)
        _update_framed_digest(digest, payload)
        count += 1
        consumed += len(payload)
    return _stream_raw_input_id(
        contract="shadow-legacy-record-stream/v2",
        descriptor={"source_id": source_id, "trade_date": trade_date},
        records_sha256=digest.hexdigest(),
        record_count=count,
        raw_bytes=consumed,
    )


def runner_source_raw_input_id(
    descriptor: RouteSourceDescriptor,
    records: Iterable[object],
    *,
    trade_date: date | None = None,
) -> str:
    prepared = tuple(RunnerSignalRecord.model_validate(record) for record in records)
    selected_trade_date = trade_date
    if selected_trade_date is None:
        if not prepared:
            raise ValueError("empty runner session identity requires trade_date")
        selected_trade_date = prepared[0].signal.event_time.astimezone(_CST).date()
    chain = canonical_sha256(
        {
            "contract": "runner-session-segment-chain/v1",
            "trade_date": selected_trade_date,
            "runner_generation_id": descriptor.generation_id,
            "strategy_spec_fingerprint": descriptor.strategy_spec_fingerprint,
        }
    )
    count = 0
    consumed = 0
    expected = descriptor.first_sequence
    for record in prepared:
        if record.sequence != expected:
            raise ValueError("runner session raw identity has a sequence gap")
        payload = json.dumps(
            record.model_dump(mode="json"),
            ensure_ascii=True,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        chain = canonical_sha256(
            {
                "previous": chain,
                "record_sha256": hashlib.sha256(payload).hexdigest(),
                "record_bytes": len(payload),
                "sequence": record.sequence,
            }
        )
        count += 1
        consumed += len(payload)
        expected += 1
    if count != descriptor.high_watermark - descriptor.first_sequence + 1:
        raise ValueError("runner session record count does not match descriptor")
    return canonical_sha256(
        {
            "contract": "shadow-runner-session-raw-input/v3",
            "descriptor": {
                "source_id": descriptor.source_id,
                "generation_id": descriptor.generation_id,
                "strategy_spec_fingerprint": descriptor.strategy_spec_fingerprint,
                "first_sequence": descriptor.first_sequence,
                "high_watermark": descriptor.high_watermark,
                "trade_date": selected_trade_date,
            },
            "records_chain_hash": chain,
            "record_count": count,
            "raw_bytes": consumed,
        }
    )


def legacy_surge_file_raw_input_id(
    path: Path,
    *,
    trade_date: date,
    max_file_bytes: int = 64 * 1024 * 1024,
    max_line_bytes: int = 16 * 1024 * 1024,
    max_records: int = 1_000_000,
) -> str:
    if (
        isinstance(max_file_bytes, bool)
        or isinstance(max_line_bytes, bool)
        or isinstance(max_records, bool)
        or not isinstance(max_file_bytes, int)
        or not isinstance(max_line_bytes, int)
        or not isinstance(max_records, int)
        or max_file_bytes < 1
        or max_line_bytes < 1
        or max_records < 1
        or max_line_bytes > max_file_bytes
    ):
        raise ValueError("legacy surge identity budget is invalid")
    digest = hashlib.sha256()
    consumed = 0
    record_count = 0
    with Path(path).open("rb") as source:
        for raw_line in source:
            consumed += len(raw_line)
            record_count += 1
            if record_count > max_records:
                raise ValueError("legacy surge raw record budget exceeded")
            if len(raw_line) > max_line_bytes or consumed > max_file_bytes:
                raise ValueError("legacy surge source exceeds the identity budget")
            digest.update(raw_line)
    return _stream_raw_input_id(
        contract="shadow-legacy-surge-raw-input/v2",
        descriptor={
            "source_id": "legacy-surge-jsonl",
            "trade_date": trade_date,
            "format": "jsonl",
        },
        records_sha256=digest.hexdigest(),
        record_count=record_count,
        raw_bytes=consumed,
    )


def _validated_completion_receipt(
    receipt: ShadowSourceCompletionReceipt,
    *,
    source: Literal["legacy", "isolated"],
    source_id: str,
    trade_date: date,
    raw_input_id: str,
    cutoff: datetime,
) -> ShadowSourceCompletionReceipt:
    verified = ShadowSourceCompletionReceipt.model_validate(receipt)
    _session_open, session_close = shadow_session_boundaries(trade_date)
    if verified.evidence_origin != "production":
        raise ValueError("shadow source requires a production completion receipt")
    if verified.source != source or verified.source_id != source_id:
        raise ValueError("completion receipt does not match the shadow source")
    if verified.trade_date != trade_date or verified.session_close_at != session_close:
        raise ValueError("completion receipt does not match the closed session")
    if verified.input_identity != raw_input_id:
        raise ValueError("completion receipt does not bind the raw input")
    if verified.produced_at > cutoff:
        raise ValueError("completion receipt was unavailable at the evaluation cutoff")
    if verified.complete_through < session_close:
        raise ValueError("completion receipt does not cover the complete session")
    return verified


def _read_legacy_record_stream(
    records: Iterable[object],
    *,
    model: type[LegacyMonitorEvent] | type[LegacySurgeEvent],
    source_id: str,
    trade_date: date,
    completion_receipt: ShadowSourceCompletionReceipt,
    captured_at: datetime,
    producer_commit: str,
    max_records: int,
    max_raw_bytes: int,
    max_record_bytes: int,
) -> tuple[tuple[LegacyMonitorEvent | LegacySurgeEvent, ...], str, ShadowSourceCompletionReceipt]:
    if (
        isinstance(max_records, bool)
        or isinstance(max_raw_bytes, bool)
        or isinstance(max_record_bytes, bool)
        or not isinstance(max_records, int)
        or not isinstance(max_raw_bytes, int)
        or not isinstance(max_record_bytes, int)
        or max_records < 1
        or max_raw_bytes < 1
        or max_record_bytes < 1
        or max_record_bytes > max_raw_bytes
    ):
        raise ValueError("legacy shadow stream budget is invalid")
    digest = hashlib.sha256()
    prepared: list[LegacyMonitorEvent | LegacySurgeEvent] = []
    consumed = 0
    for raw_record in records:
        payload = _raw_record_bytes(raw_record)
        record_count = len(prepared) + 1
        consumed += len(payload)
        if record_count > max_records:
            raise ValueError("legacy shadow raw record budget exceeded")
        if len(payload) > max_record_bytes:
            raise ValueError("legacy shadow record exceeds the byte budget")
        if consumed > max_raw_bytes:
            raise ValueError("legacy shadow stream exceeds the raw byte budget")
        _update_framed_digest(digest, payload)
        prepared.append(model.model_validate(raw_record))
    raw_input_id = _stream_raw_input_id(
        contract="shadow-legacy-record-stream/v2",
        descriptor={"source_id": source_id, "trade_date": trade_date},
        records_sha256=digest.hexdigest(),
        record_count=len(prepared),
        raw_bytes=consumed,
    )
    cutoff = normalize_aware_utc(captured_at)
    receipt = _validated_completion_receipt(
        completion_receipt,
        source="legacy",
        source_id=source_id,
        trade_date=trade_date,
        raw_input_id=raw_input_id,
        cutoff=cutoff,
    )
    if receipt.producer_commit != producer_commit:
        raise ValueError("legacy completion receipt producer commit mismatch")
    return tuple(prepared), raw_input_id, receipt


def _legacy_event_time(trade_date: date, observed: datetime | str) -> datetime:
    if isinstance(observed, str):
        hour, minute = (int(part) for part in observed.split(":"))
        local = datetime.combine(trade_date, time(hour, minute), tzinfo=_CST)
    else:
        if observed.date() != trade_date:
            raise ValueError("legacy event does not match the requested trade date")
        local = observed.replace(tzinfo=_CST)
    return local.astimezone(UTC)


def _validated_exported_at(exported_at: datetime, *, latest_event: datetime) -> datetime:
    normalized = normalize_aware_utc(exported_at)
    if normalized < latest_event:
        raise ValueError("exported_at cannot precede the legacy event")
    return normalized


def legacy_monitor_observations(
    events: Iterable[LegacyMonitorEvent],
    *,
    trade_date: date,
    exported_at: datetime,
    producer_commit: str,
    binding: ShadowStrategyBinding,
) -> tuple[ShadowObservation, ...]:
    if binding.strategy_id != "n_shape":
        raise ValueError("legacy monitor binding must target n_shape")
    prepared: list[tuple[LegacyMonitorEvent, datetime]] = []
    for event in events:
        if event.trade_date != trade_date:
            raise ValueError("legacy monitor event does not match the requested trade date")
        prepared.append((event, _legacy_event_time(trade_date, event.trigger_time)))
    latest_event = max((item[1] for item in prepared), default=datetime.min.replace(tzinfo=UTC))
    available_at = _validated_exported_at(exported_at, latest_event=latest_event)
    observations = [
        ShadowObservation(
            source="legacy",
            strategy_id=binding.strategy_id,
            strategy_version=binding.strategy_version,
            definition_fingerprint=binding.definition_fingerprint,
            executable_fingerprint=binding.executable_fingerprint,
            trade_date=trade_date,
            ts_code=event.ts_code,
            action=_MONITOR_ACTIONS[event.level].value,
            event_time=event_time,
            available_at=available_at,
            availability_basis="export_observed_proxy",
            producer_commit=producer_commit,
            upstream_event_id=canonical_sha256(
                {
                    "contract": "legacy-monitor-shadow-event/v1",
                    "event": {
                        **event.model_dump(mode="python", exclude={"trigger_time"}),
                        "trigger_time": event_time,
                    },
                }
            ),
            evidence_id=canonical_sha256(
                {
                    "contract": "legacy-monitor-shadow-evidence/v1",
                    "event": {
                        **event.model_dump(mode="python", exclude={"trigger_time"}),
                        "trigger_time": event_time,
                    },
                }
            ),
        )
        for event, event_time in prepared
    ]
    return tuple(sorted(observations, key=lambda item: (item.event_time, str(item.observation_id))))


def legacy_monitor_rows_to_shadow_observations(
    rows: Iterable[Mapping[str, object]],
    *,
    trade_date: date,
    exported_at: datetime,
    producer_commit: str,
    binding: ShadowStrategyBinding,
) -> tuple[ShadowObservation, ...]:
    events = tuple(LegacyMonitorEvent.model_validate(dict(row)) for row in rows)
    return legacy_monitor_observations(
        events,
        trade_date=trade_date,
        exported_at=exported_at,
        producer_commit=producer_commit,
        binding=binding,
    )


def legacy_surge_observations(
    events: Iterable[LegacySurgeEvent],
    *,
    trade_date: date,
    exported_at: datetime,
    producer_commit: str,
    binding: ShadowStrategyBinding,
) -> tuple[ShadowObservation, ...]:
    if binding.strategy_id != "growth_board_surge":
        raise ValueError("legacy surge binding must target growth_board_surge")
    confirmed = tuple(event for event in events if event.status == "confirmed")
    earliest_by_code: dict[str, LegacySurgeEvent] = {}
    for event in confirmed:
        current = earliest_by_code.get(event.ts_code)
        rank = (event.confirmed_at, canonical_sha256(event.model_dump(mode="python")))
        if current is None or rank < (
            current.confirmed_at,
            canonical_sha256(current.model_dump(mode="python")),
        ):
            earliest_by_code[event.ts_code] = event
    prepared = tuple(
        (event, _legacy_event_time(trade_date, event.confirmed_at))
        for event in earliest_by_code.values()
    )
    latest_event = max((item[1] for item in prepared), default=datetime.min.replace(tzinfo=UTC))
    available_at = _validated_exported_at(exported_at, latest_event=latest_event)
    observations = tuple(
        ShadowObservation(
            source="legacy",
            strategy_id=binding.strategy_id,
            strategy_version=binding.strategy_version,
            definition_fingerprint=binding.definition_fingerprint,
            executable_fingerprint=binding.executable_fingerprint,
            trade_date=trade_date,
            ts_code=event.ts_code,
            action=SignalAction.B_INTENT.value,
            event_time=event_time,
            available_at=available_at,
            availability_basis="export_observed_proxy",
            producer_commit=producer_commit,
            upstream_event_id=canonical_sha256(
                {
                    "contract": "legacy-surge-shadow-event/v1",
                    "trade_date": trade_date,
                    "event": event.model_dump(mode="python"),
                }
            ),
            evidence_id=canonical_sha256(
                {
                    "contract": "legacy-surge-shadow-evidence/v1",
                    "trade_date": trade_date,
                    "event": event.model_dump(mode="python"),
                }
            ),
        )
        for event, event_time in prepared
    )
    return tuple(sorted(observations, key=lambda item: (item.event_time, str(item.observation_id))))


def read_legacy_monitor_shadow_snapshots(
    records: Iterable[object],
    *,
    trade_date: date,
    exported_at: datetime,
    producer_commit: str,
    bindings: Iterable[ShadowStrategyBinding],
    completion_receipt: ShadowSourceCompletionReceipt,
    max_records: int = 100_000,
    max_raw_bytes: int = 128 * 1024 * 1024,
    max_record_bytes: int = 4 * 1024 * 1024,
) -> tuple[tuple[ShadowStrategyBinding, ShadowLegacyReadSnapshot], ...]:
    prepared, raw_input_id, receipt = _read_legacy_record_stream(
        records,
        model=LegacyMonitorEvent,
        source_id="legacy-monitor-events",
        trade_date=trade_date,
        completion_receipt=completion_receipt,
        captured_at=exported_at,
        producer_commit=producer_commit,
        max_records=max_records,
        max_raw_bytes=max_raw_bytes,
        max_record_bytes=max_record_bytes,
    )
    cutoff = normalize_aware_utc(exported_at)
    return tuple(
        (
            binding,
            ShadowLegacyReadSnapshot(
                observations=legacy_monitor_observations(
                    prepared,
                    trade_date=trade_date,
                    exported_at=exported_at,
                    producer_commit=producer_commit,
                    binding=binding,
                ),
                source_id=receipt.source_id,
                raw_input_id=raw_input_id,
                completion_receipt=receipt,
                upstream_snapshot_id=shadow_upstream_snapshot_id(raw_input_id, receipt),
                captured_at=cutoff,
                complete_through=receipt.complete_through,
            ),
        )
        for binding in tuple(bindings)
    )


def read_legacy_monitor_shadow_snapshot(
    records: Iterable[object],
    *,
    trade_date: date,
    exported_at: datetime,
    producer_commit: str,
    binding: ShadowStrategyBinding,
    completion_receipt: ShadowSourceCompletionReceipt,
    max_records: int = 100_000,
    max_raw_bytes: int = 128 * 1024 * 1024,
    max_record_bytes: int = 4 * 1024 * 1024,
) -> ShadowLegacyReadSnapshot:
    return read_legacy_monitor_shadow_snapshots(
        records,
        trade_date=trade_date,
        exported_at=exported_at,
        producer_commit=producer_commit,
        bindings=(binding,),
        completion_receipt=completion_receipt,
        max_records=max_records,
        max_raw_bytes=max_raw_bytes,
        max_record_bytes=max_record_bytes,
    )[0][1]


def read_legacy_surge_events_shadow_snapshots(
    records: Iterable[object],
    *,
    trade_date: date,
    exported_at: datetime,
    producer_commit: str,
    bindings: Iterable[ShadowStrategyBinding],
    completion_receipt: ShadowSourceCompletionReceipt,
    max_records: int = 100_000,
    max_raw_bytes: int = 128 * 1024 * 1024,
    max_record_bytes: int = 4 * 1024 * 1024,
) -> tuple[tuple[ShadowStrategyBinding, ShadowLegacyReadSnapshot], ...]:
    prepared, raw_input_id, receipt = _read_legacy_record_stream(
        records,
        model=LegacySurgeEvent,
        source_id="legacy-surge-events",
        trade_date=trade_date,
        completion_receipt=completion_receipt,
        captured_at=exported_at,
        producer_commit=producer_commit,
        max_records=max_records,
        max_raw_bytes=max_raw_bytes,
        max_record_bytes=max_record_bytes,
    )
    cutoff = normalize_aware_utc(exported_at)
    return tuple(
        (
            binding,
            ShadowLegacyReadSnapshot(
                observations=legacy_surge_observations(
                    prepared,
                    trade_date=trade_date,
                    exported_at=exported_at,
                    producer_commit=producer_commit,
                    binding=binding,
                ),
                source_id=receipt.source_id,
                raw_input_id=raw_input_id,
                completion_receipt=receipt,
                upstream_snapshot_id=shadow_upstream_snapshot_id(raw_input_id, receipt),
                captured_at=cutoff,
                complete_through=receipt.complete_through,
            ),
        )
        for binding in tuple(bindings)
    )


def read_legacy_surge_events_shadow_snapshot(
    records: Iterable[object],
    *,
    trade_date: date,
    exported_at: datetime,
    producer_commit: str,
    binding: ShadowStrategyBinding,
    completion_receipt: ShadowSourceCompletionReceipt,
    max_records: int = 100_000,
    max_raw_bytes: int = 128 * 1024 * 1024,
    max_record_bytes: int = 4 * 1024 * 1024,
) -> ShadowLegacyReadSnapshot:
    return read_legacy_surge_events_shadow_snapshots(
        records,
        trade_date=trade_date,
        exported_at=exported_at,
        producer_commit=producer_commit,
        bindings=(binding,),
        completion_receipt=completion_receipt,
        max_records=max_records,
        max_raw_bytes=max_raw_bytes,
        max_record_bytes=max_record_bytes,
    )[0][1]


def isolated_signal_observations(
    signals: Iterable[SignalEnvelopeFamily],
    *,
    trade_date: date,
    binding: ShadowStrategyBinding,
    current_producer_commit: str | None = None,
) -> tuple[ShadowObservation, ...]:
    observations: dict[str, ShadowObservation] = {}
    seen_upstream: set[tuple[str, str]] = set()
    for signal in signals:
        comparable_actions = _COMPARABLE_SIGNAL_ACTIONS.get(signal.strategy_id)
        if signal.strategy_id != binding.strategy_id:
            continue
        if comparable_actions is None:
            continue
        if signal.action not in comparable_actions:
            continue
        version_text = signal.strategy_version
        if (
            not version_text.isascii()
            or not version_text.isdecimal()
            or version_text.startswith("0")
            or int(version_text) < 1
        ):
            raise ValueError("isolated signal strategy_version must be canonical")
        if int(version_text) != binding.strategy_version:
            raise ValueError("isolated signal strategy_version mismatch")
        if signal.event_time.date() != trade_date:
            raise ValueError("isolated signal does not match the requested trade date")
        upstream_key = (signal.strategy_id, str(signal.signal_id))
        if upstream_key in seen_upstream:
            raise ValueError("duplicate upstream signal in isolated shadow source")
        seen_upstream.add(upstream_key)
        observation = ShadowObservation(
            source="isolated",
            strategy_id=signal.strategy_id,
            strategy_version=binding.strategy_version,
            definition_fingerprint=binding.definition_fingerprint,
            executable_fingerprint=binding.executable_fingerprint,
            trade_date=trade_date,
            ts_code=signal.candidate_id,
            action=signal.action.value,
            event_time=signal.event_time,
            available_at=signal.available_at,
            availability_basis="observed_completion",
            producer_commit=_signal_producer_commit(
                signal,
                current_producer_commit=current_producer_commit,
            ),
            upstream_event_id=str(signal.signal_id),
            evidence_id=str(signal.signal_id),
        )
        observations[str(observation.observation_id)] = observation
    return tuple(
        sorted(observations.values(), key=lambda item: (item.event_time, str(item.observation_id)))
    )


def isolated_signals_to_shadow_observations(
    signals: Iterable[SignalEnvelopeFamily],
    *,
    trade_date: date,
    bindings: Iterable[ShadowStrategyBinding],
) -> tuple[ShadowObservation, ...]:
    prepared = tuple(bindings)
    prepared_signals = tuple(signals)
    identities = [(item.strategy_id, item.strategy_version) for item in prepared]
    if len(identities) != len(set(identities)):
        raise ValueError("unbound signal source cannot disambiguate parallel implementations")
    observations = tuple(
        item
        for binding in prepared
        for item in isolated_signal_observations(
            prepared_signals,
            trade_date=trade_date,
            binding=binding,
        )
    )
    return tuple(sorted(observations, key=lambda item: (item.event_time, str(item.observation_id))))


def _signal_producer_commit(
    signal: SignalEnvelopeFamily,
    *,
    current_producer_commit: str | None,
) -> str:
    if type(signal) is SignalEnvelope:
        return signal.producer_commit
    if type(signal) is CurrentSignalEnvelope:
        identity = signal.producer_identity
        if type(identity) is GitCommitClaimIdentity:
            return identity.producer_commit
        if current_producer_commit is not None:
            return current_producer_commit
        raise ValueError("full-manifest isolated signal requires an attested producer commit")
    raise TypeError("isolated shadow source received an unknown signal envelope family")


def read_isolated_runner_shadow_snapshot(
    source: ShadowRunnerSignalSource,
    *,
    trade_date: date,
    observed_at: datetime,
    binding: ShadowStrategyBinding,
    expected_calendar_authority_id: str,
    batch_size: int = 1_000,
    max_records: int = 100_000,
    max_raw_bytes: int = 128 * 1024 * 1024,
    max_record_bytes: int = 4 * 1024 * 1024,
    attestation_verifier: CompletionAttestationVerifier | None = None,
) -> ShadowRunnerReadSnapshot:
    if (
        isinstance(batch_size, bool)
        or isinstance(max_records, bool)
        or not isinstance(batch_size, int)
        or isinstance(max_raw_bytes, bool)
        or isinstance(max_record_bytes, bool)
        or not isinstance(max_records, int)
        or not isinstance(max_raw_bytes, int)
        or not isinstance(max_record_bytes, int)
        or batch_size < 1
        or max_records < 1
        or max_raw_bytes < 1
        or max_record_bytes < 1
        or max_record_bytes > max_raw_bytes
    ):
        raise ValueError("runner shadow read bounds must be positive integers")
    if len(expected_calendar_authority_id) != 64 or any(
        character not in "0123456789abcdef" for character in expected_calendar_authority_id
    ):
        raise ValueError("expected shadow calendar authority must be SHA-256")
    cutoff = normalize_aware_utc(observed_at)
    try:
        supplied_receipt = source.read_completion_receipt(trade_date=trade_date)
    except AttributeError as exc:
        raise ValueError("runner shadow source has no producer completion receipt") from exc
    preliminary_receipt = ShadowSourceCompletionReceipt.model_validate(supplied_receipt)
    if preliminary_receipt.calendar_generation_id != expected_calendar_authority_id:
        raise ValueError(
            "runner completion receipt calendar generation does not match "
            "the expected shadow calendar authority"
        )
    if preliminary_receipt.produced_at > cutoff:
        raise ValueError("completion receipt was unavailable at the evaluation cutoff")
    if not verify_completion_attestation(preliminary_receipt, attestation_verifier):
        raise ValueError("runner completion attestation is not trusted")
    attestation = preliminary_receipt.completion_attestation
    if attestation is None:
        raise ValueError("runner completion attestation is missing")
    claims = attestation.claims
    if (
        claims.strategy_id != binding.strategy_id
        or claims.strategy_version != binding.strategy_version
        or claims.strategy_registration_fingerprint != binding.definition_fingerprint
        or claims.executable_fingerprint != binding.executable_fingerprint
    ):
        raise ValueError("runner completion attestation binding does not match shadow binding")
    if preliminary_receipt.high_watermark is None:
        raise ValueError("runner completion receipt has no signal high watermark")
    cursor = 0
    frozen = None
    visible: list[SignalEnvelopeFamily] = []
    raw_chain: str | None = None
    raw_record_count = 0
    raw_bytes = 0
    while True:
        try:
            batch = source.read_completed_batch(
                trade_date=trade_date,
                after_sequence=cursor,
                limit=batch_size,
            )
        except AttributeError as exc:
            raise ValueError("runner shadow source cannot read a completed session") from exc
        descriptor = batch.snapshot.descriptor
        if batch.after_sequence != cursor or batch.limit != batch_size:
            raise ValueError("runner shadow source did not honor the read request")
        if frozen is None:
            frozen = descriptor
            if descriptor.high_watermark != preliminary_receipt.high_watermark:
                raise ValueError("runner completed snapshot does not match the receipt watermark")
            record_count = descriptor.high_watermark - descriptor.first_sequence + 1
            if record_count > max_records:
                raise ValueError("runner shadow source exceeds the record budget")
            if cursor < descriptor.first_sequence - 1:
                cursor = descriptor.first_sequence - 1
            raw_chain = canonical_sha256(
                {
                    "contract": "runner-session-segment-chain/v1",
                    "trade_date": trade_date,
                    "runner_generation_id": descriptor.generation_id,
                    "strategy_spec_fingerprint": descriptor.strategy_spec_fingerprint,
                }
            )
        elif descriptor != frozen:
            raise ValueError("runner shadow source snapshot changed during paging")
        if cursor >= descriptor.high_watermark:
            if batch.records:
                raise ValueError("runner shadow source returned records beyond its high watermark")
            break
        if not batch.records:
            raise ValueError("runner shadow source has a sequence gap")
        expected_sequence = cursor + 1
        for record in batch.records:
            if record.sequence != expected_sequence:
                raise ValueError("runner shadow source has a sequence gap")
            if record.sequence > descriptor.high_watermark:
                raise ValueError("runner shadow source returned records beyond its high watermark")
            raw_payload = json.dumps(
                record.model_dump(mode="json"),
                ensure_ascii=True,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            raw_record_count += 1
            raw_bytes += len(raw_payload)
            if raw_record_count > max_records:
                raise ValueError("runner shadow source exceeds the raw record budget")
            if len(raw_payload) > max_record_bytes:
                raise ValueError("runner shadow record exceeds the byte budget")
            if raw_bytes > max_raw_bytes:
                raise ValueError("runner shadow source exceeds the raw byte budget")
            if raw_chain is None:
                raise RuntimeError("runner raw chain was not initialized")
            raw_chain = canonical_sha256(
                {
                    "previous": raw_chain,
                    "record_sha256": hashlib.sha256(raw_payload).hexdigest(),
                    "record_bytes": len(raw_payload),
                    "sequence": record.sequence,
                }
            )
            signal = parse_signal_envelope(record.signal.model_dump(mode="json"))
            if signal.event_time.date() == trade_date:
                if signal.available_at > cutoff:
                    raise ValueError("runner signal became available after observed_at")
                visible.append(signal)
            expected_sequence += 1
        cursor = batch.records[-1].sequence
        if cursor == descriptor.high_watermark:
            break
    if frozen is None:
        raise ValueError("runner shadow source did not expose a snapshot")
    if raw_chain is None:
        raise RuntimeError("runner raw chain was not initialized")
    raw_input_id = canonical_sha256(
        {
            "contract": "shadow-runner-session-raw-input/v3",
            "descriptor": {
                "source_id": frozen.source_id,
                "generation_id": frozen.generation_id,
                "strategy_spec_fingerprint": frozen.strategy_spec_fingerprint,
                "first_sequence": frozen.first_sequence,
                "high_watermark": frozen.high_watermark,
                "trade_date": trade_date,
            },
            "records_chain_hash": raw_chain,
            "record_count": raw_record_count,
            "raw_bytes": raw_bytes,
        }
    )
    receipt = _validated_completion_receipt(
        preliminary_receipt,
        source="isolated",
        source_id=frozen.source_id,
        trade_date=trade_date,
        raw_input_id=raw_input_id,
        cutoff=cutoff,
    )
    observations = isolated_signal_observations(
        visible,
        trade_date=trade_date,
        binding=binding,
        current_producer_commit=receipt.producer_commit,
    )
    return ShadowRunnerReadSnapshot(
        observations=observations,
        source_id=frozen.source_id,
        raw_input_id=raw_input_id,
        completion_receipt=receipt,
        upstream_snapshot_id=shadow_upstream_snapshot_id(raw_input_id, receipt),
        captured_at=cutoff,
        complete_through=receipt.complete_through,
    )


def read_isolated_runner_shadow_observations(
    source: ShadowRunnerSignalSource,
    *,
    trade_date: date,
    observed_at: datetime,
    binding: ShadowStrategyBinding,
    expected_calendar_authority_id: str,
    batch_size: int = 1_000,
    max_records: int = 100_000,
    max_raw_bytes: int = 128 * 1024 * 1024,
    max_record_bytes: int = 4 * 1024 * 1024,
    attestation_verifier: CompletionAttestationVerifier | None = None,
) -> tuple[ShadowObservation, ...]:
    return read_isolated_runner_shadow_snapshot(
        source,
        trade_date=trade_date,
        observed_at=observed_at,
        binding=binding,
        expected_calendar_authority_id=expected_calendar_authority_id,
        batch_size=batch_size,
        max_records=max_records,
        max_raw_bytes=max_raw_bytes,
        max_record_bytes=max_record_bytes,
        attestation_verifier=attestation_verifier,
    ).observations


def read_legacy_surge_shadow_snapshot(
    path: Path,
    *,
    trade_date: date,
    exported_at: datetime,
    producer_commit: str,
    binding: ShadowStrategyBinding,
    completion_receipt: ShadowSourceCompletionReceipt | None = None,
    max_file_bytes: int = 16 * 1024 * 1024,
    max_line_bytes: int = 1024 * 1024,
    max_records: int = 100_000,
) -> ShadowLegacyReadSnapshot:
    if (
        isinstance(max_file_bytes, bool)
        or isinstance(max_line_bytes, bool)
        or isinstance(max_records, bool)
        or not isinstance(max_file_bytes, int)
        or not isinstance(max_line_bytes, int)
        or not isinstance(max_records, int)
        or max_file_bytes < 1
        or max_line_bytes < 1
        or max_records < 1
        or max_line_bytes > max_file_bytes
    ):
        raise ValueError("legacy surge read budget is invalid")
    candidate = Path(path)
    try:
        observed = candidate.lstat()
    except FileNotFoundError as exc:
        raise ValueError("legacy surge source must be a regular file") from exc
    if not stat.S_ISREG(observed.st_mode) or stat.S_ISLNK(observed.st_mode):
        raise ValueError("legacy surge source must be a regular file")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(candidate, flags)
    try:
        actual = os.fstat(descriptor)
        if not stat.S_ISREG(actual.st_mode) or (actual.st_dev, actual.st_ino) != (
            observed.st_dev,
            observed.st_ino,
        ):
            raise ValueError("legacy surge source must be a stable regular file")
        if actual.st_size > max_file_bytes:
            raise ValueError("legacy surge source exceeds the read budget")
        with os.fdopen(os.dup(descriptor), "rb") as source:
            events: list[LegacySurgeEvent] = []
            consumed = 0
            raw_digest = hashlib.sha256()
            record_count = 0
            for line_number, raw_line in enumerate(source, start=1):
                raw_digest.update(raw_line)
                consumed += len(raw_line)
                record_count += 1
                if record_count > max_records:
                    raise ValueError("legacy surge raw record budget exceeded")
                if len(raw_line) > max_line_bytes or consumed > max_file_bytes:
                    raise ValueError("legacy surge record exceeds the read budget")
                if not raw_line.strip():
                    continue
                try:
                    payload = json.loads(raw_line.decode("utf-8"))
                    events.append(LegacySurgeEvent.model_validate(payload))
                except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
                    raise ValueError(f"invalid legacy surge record at line {line_number}") from exc
        completed = os.fstat(descriptor)
        if (
            completed.st_size != actual.st_size
            or completed.st_mtime_ns != actual.st_mtime_ns
            or completed.st_ctime_ns != actual.st_ctime_ns
        ):
            raise ValueError("legacy surge source changed while it was read")
    finally:
        os.close(descriptor)
    observations = legacy_surge_observations(
        events,
        trade_date=trade_date,
        exported_at=exported_at,
        producer_commit=producer_commit,
        binding=binding,
    )
    normalized_exported_at = normalize_aware_utc(exported_at)
    raw_input_id = _stream_raw_input_id(
        contract="shadow-legacy-surge-raw-input/v2",
        descriptor={
            "source_id": "legacy-surge-jsonl",
            "trade_date": trade_date,
            "format": "jsonl",
        },
        records_sha256=raw_digest.hexdigest(),
        record_count=record_count,
        raw_bytes=consumed,
    )
    if completion_receipt is None:
        raise ValueError("legacy surge source has no producer completion receipt")
    receipt = _validated_completion_receipt(
        completion_receipt,
        source="legacy",
        source_id="legacy-surge-jsonl",
        trade_date=trade_date,
        raw_input_id=raw_input_id,
        cutoff=normalized_exported_at,
    )
    return ShadowLegacyReadSnapshot(
        observations=observations,
        source_id="legacy-surge-jsonl",
        raw_input_id=raw_input_id,
        completion_receipt=receipt,
        upstream_snapshot_id=shadow_upstream_snapshot_id(raw_input_id, receipt),
        captured_at=normalized_exported_at,
        complete_through=receipt.complete_through,
    )


def read_legacy_surge_shadow_observations(
    path: Path,
    *,
    trade_date: date,
    exported_at: datetime,
    producer_commit: str,
    binding: ShadowStrategyBinding,
    completion_receipt: ShadowSourceCompletionReceipt | None = None,
    max_file_bytes: int = 16 * 1024 * 1024,
    max_line_bytes: int = 1024 * 1024,
    max_records: int = 100_000,
) -> tuple[ShadowObservation, ...]:
    return read_legacy_surge_shadow_snapshot(
        path,
        trade_date=trade_date,
        exported_at=exported_at,
        producer_commit=producer_commit,
        binding=binding,
        completion_receipt=completion_receipt,
        max_file_bytes=max_file_bytes,
        max_line_bytes=max_line_bytes,
        max_records=max_records,
    ).observations


__all__ = [
    "LegacyMonitorEvent",
    "LegacySurgeEvent",
    "ShadowRunnerSignalSource",
    "ShadowRunnerReadSnapshot",
    "ShadowLegacyReadSnapshot",
    "isolated_signal_observations",
    "isolated_signals_to_shadow_observations",
    "legacy_monitor_observations",
    "legacy_monitor_rows_to_shadow_observations",
    "legacy_records_raw_input_id",
    "legacy_surge_file_raw_input_id",
    "legacy_surge_observations",
    "read_isolated_runner_shadow_observations",
    "read_isolated_runner_shadow_snapshot",
    "read_legacy_monitor_shadow_snapshot",
    "read_legacy_monitor_shadow_snapshots",
    "read_legacy_surge_events_shadow_snapshot",
    "read_legacy_surge_events_shadow_snapshots",
    "read_legacy_surge_shadow_observations",
    "read_legacy_surge_shadow_snapshot",
    "runner_source_raw_input_id",
]
