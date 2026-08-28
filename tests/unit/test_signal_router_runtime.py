from __future__ import annotations

import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from threading import Barrier

import pytest
from pydantic import ValidationError

from rquant.delivery_contracts import DeliveryChannel, DeliveryTarget, OutboxStatus
from rquant.runtime_shadow_validation import (
    ShadowSourceCompletionReceipt,
    shadow_session_boundaries,
)
from rquant.signal_bus import SignalBusStore
from rquant.signal_contracts import SignalAction, SignalEnvelope
from rquant.signal_router_runtime import (
    ReadonlySignalRouteAuthority,
    ReadonlyStrategyRunnerSignalSource,
    RouteSourceDescriptor,
    RoutingConfigurationUnavailableError,
    RoutingDecision,
    RunnerSignalBatch,
    SignalRouteConflictError,
    SignalRouteCursorStore,
    SignalRouteSequenceError,
    SourceSnapshot,
    StrategyRunnerSignalSource,
    route_runner_signals,
)
from rquant.strategy_runner import RunnerSignalRecord, runner_signal_raw_input_id

NOW = datetime(2026, 7, 31, 2, 30, tzinfo=UTC)
POLICY = "e" * 64
GENERATION = "f" * 64
SPEC = "1" * 64


def _signal(
    seed: str = "a",
    *,
    available_at: datetime = NOW,
    candidate_id: str = "600000.SH",
) -> SignalEnvelope:
    return SignalEnvelope(
        schema_version=1,
        strategy_id="n-shape",
        strategy_version="1",
        parameter_fingerprint=seed * 64,
        dataset_snapshot_id="b" * 64,
        feature_snapshot_id="c" * 64,
        event_time=available_at - timedelta(seconds=1),
        available_at=available_at,
        candidate_id=candidate_id,
        action=SignalAction.WATCH,
        reason_codes=("test",),
        evidence={},
        expires_at=available_at + timedelta(minutes=5),
        producer_commit="d" * 40,
    )


class FakeRunner:
    def __init__(
        self,
        records: tuple[RunnerSignalRecord, ...],
        *,
        source_id: str = "n-shape-v1",
        generation_id: str = GENERATION,
        spec_fingerprint: str = SPEC,
        first_sequence: int = 1,
        high_watermark: int | None = None,
    ) -> None:
        self.records = records
        self._descriptor = RouteSourceDescriptor(
            source_id=source_id,
            generation_id=generation_id,
            strategy_spec_fingerprint=spec_fingerprint,
            first_sequence=first_sequence,
            high_watermark=(
                max((record.sequence for record in records), default=first_sequence - 1)
                if high_watermark is None
                else high_watermark
            ),
        )

    def read_batch(self, *, after_sequence: int, limit: int) -> RunnerSignalBatch:
        return RunnerSignalBatch(
            snapshot=SourceSnapshot(descriptor=self._descriptor),
            after_sequence=after_sequence,
            limit=limit,
            records=tuple(record for record in self.records if record.sequence > after_sequence)[
                :limit
            ],
        )


def _bus(path: Path) -> SignalBusStore:
    return SignalBusStore(
        path,
        retry_base_delay=timedelta(seconds=5),
        retry_max_delay=timedelta(seconds=30),
        max_attempts=3,
    )


def _target(
    recipient: str = "admin",
    channel: DeliveryChannel = DeliveryChannel.PUSHDEER,
) -> DeliveryTarget:
    return DeliveryTarget(recipient_id=recipient, channel=channel)


def _route_decision(*targets: DeliveryTarget) -> RoutingDecision:
    return RoutingDecision.route(
        routing_policy_fingerprint=POLICY,
        targets=targets or (_target(),),
    )


def _cursors(tmp_path: Path) -> SignalRouteCursorStore:
    return SignalRouteCursorStore(
        tmp_path / "legacy-cursor.sqlite3",
        routing_policy_fingerprint=POLICY,
    )


def _run(
    *,
    runner: FakeRunner,
    bus: SignalBusStore,
    cursors: SignalRouteCursorStore,
    routed_at: datetime = NOW,
    resolver: object | None = None,
    limit: int = 10,
):
    return route_runner_signals(
        source_id="n-shape-v1",
        source=runner,
        bus=bus,
        cursors=cursors,
        routed_at=routed_at,
        target_resolver=resolver or (lambda _signal: _route_decision()),
        limit=limit,
    )


def test_route_commits_source_receipt_cursor_signal_and_outbox_in_bus(
    tmp_path: Path,
) -> None:
    signal = _signal()
    runner = FakeRunner((RunnerSignalRecord(sequence=1, signal=signal),))
    bus = _bus(tmp_path / "bus.sqlite3")
    cursors = _cursors(tmp_path)

    summary = _run(runner=runner, bus=bus, cursors=cursors)

    assert summary.model_dump() | {"routed_at": NOW} == {
        "source_id": "n-shape-v1",
        "source_generation_id": GENERATION,
        "source_high_watermark": 1,
        "started_after_sequence": 0,
        "last_sequence": 1,
        "routed_count": 1,
        "target_count": 1,
        "duplicate_count": 0,
        "no_target_count": 0,
        "expired_count": 0,
        "deferred_count": 0,
        "routed_at": NOW,
    }
    assert bus.signal(signal.signal_id) == signal
    assert bus.route_cursor("n-shape-v1").last_sequence == 1
    assert bus.route_receipts("n-shape-v1")[0].target_count == 1
    assert cursors.cursor("n-shape-v1").last_sequence == 1
    outbox = bus.outbox_records(signal_id=signal.signal_id)
    assert len(outbox) == 1 and outbox[0].status is OutboxStatus.PENDING

    with sqlite3.connect(tmp_path / "legacy-cursor.sqlite3") as connection:
        tables = {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
    assert "runner_cursor" not in tables


def test_atomic_fault_rolls_back_cursor_receipt_signal_and_outbox(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    signal = _signal()
    runner = FakeRunner((RunnerSignalRecord(sequence=1, signal=signal),))
    bus = _bus(tmp_path / "bus.sqlite3")
    cursors = _cursors(tmp_path)
    monkeypatch.setattr(
        bus,
        "_before_commit",
        lambda _connection: (_ for _ in ()).throw(RuntimeError("commit fault")),
    )

    with pytest.raises(RuntimeError, match="commit fault"):
        _run(runner=runner, bus=bus, cursors=cursors)

    assert bus.signal(signal.signal_id) is None
    assert bus.route_cursor("n-shape-v1").last_sequence == 0
    assert bus.route_receipts("n-shape-v1") == ()
    assert bus.outbox_records() == ()


def test_concurrent_exact_retry_is_idempotent_without_duplicate_target(
    tmp_path: Path,
) -> None:
    signal = _signal()
    runner = FakeRunner((RunnerSignalRecord(sequence=1, signal=signal),))
    bus = _bus(tmp_path / "bus.sqlite3")
    barrier = Barrier(2)

    def resolver(_signal: SignalEnvelope) -> RoutingDecision:
        barrier.wait()
        return _route_decision()

    def run() -> object:
        return _run(
            runner=runner,
            bus=bus,
            cursors=_cursors(tmp_path),
            resolver=resolver,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        summaries = tuple(executor.map(lambda _index: run(), range(2)))

    assert sum(summary.routed_count for summary in summaries) == 1
    assert sum(summary.duplicate_count for summary in summaries) == 1
    assert len(bus.route_receipts("n-shape-v1")) == 1
    assert len(bus.outbox_records(signal_id=signal.signal_id)) == 1


def test_strategy_runner_adapter_exposes_one_persisted_snapshot(tmp_path: Path) -> None:
    records = (RunnerSignalRecord(sequence=1, signal=_signal()),)
    path = tmp_path / "runner.sqlite3"
    _write_runner_source(path, signal=records[0].signal)

    class Store:
        @staticmethod
        def _connect() -> sqlite3.Connection:
            connection = sqlite3.connect(path, isolation_level=None)
            connection.row_factory = sqlite3.Row
            return connection

    source = StrategyRunnerSignalSource(source_id="n-shape-v1", store=Store())

    assert source.read_batch(after_sequence=0, limit=10) == RunnerSignalBatch(
        snapshot=SourceSnapshot(
            descriptor=RouteSourceDescriptor(
                source_id="n-shape-v1",
                generation_id=GENERATION,
                strategy_spec_fingerprint=SPEC,
                first_sequence=1,
                high_watermark=1,
            )
        ),
        after_sequence=0,
        limit=10,
        records=records,
    )


def _runner_completion_receipt(
    *,
    records: tuple[RunnerSignalRecord, ...],
    source_id: str = "n-shape-v1",
) -> ShadowSourceCompletionReceipt:
    trade_date = date(2026, 7, 31)
    _session_open, session_close = shadow_session_boundaries(trade_date)
    return ShadowSourceCompletionReceipt(
        evidence_origin="production",
        source="isolated",
        source_id=source_id,
        trade_date=trade_date,
        session_close_at=session_close,
        complete_through=session_close,
        input_identity=runner_signal_raw_input_id(
            source_id=source_id,
            runner_generation_id=GENERATION,
            strategy_spec_fingerprint=SPEC,
            high_watermark=len(records),
            records=records,
        ),
        produced_at=session_close + timedelta(seconds=5),
        producer_commit="d" * 40,
        producer_version="test-runner-v1",
        producer_service_id="strategy-live",
        producer_instance_id="n-shape-primary",
        runner_generation_id=GENERATION,
        signal_authority_generation_id="8" * 64,
        calendar_generation_id="9" * 64,
        last_sequence=0,
        high_watermark=len(records),
        route_receipts_id="7" * 64,
        feature_source_generation_id="6" * 64,
        feature_close_marker_id="5" * 64,
        feature_segment_chain_hash="4" * 64,
        segment_start_sequence=0,
        segment_record_count=len(records),
        segment_chain_hash="3" * 64,
    )


def _write_runner_source(
    path: Path,
    *,
    signal: SignalEnvelope | None = None,
    completion_receipt: ShadowSourceCompletionReceipt | None = None,
) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA journal_mode = WAL")
        connection.executescript(
            """
            CREATE TABLE runner_metadata (
                singleton INTEGER PRIMARY KEY,
                strategy_spec_fingerprint TEXT NOT NULL,
                strategy_spec_json TEXT NOT NULL,
                evaluator_contract_fingerprint TEXT NOT NULL
            );
            CREATE TABLE runner_source_identity (
                singleton INTEGER PRIMARY KEY,
                source_generation_id TEXT NOT NULL
            );
            CREATE TABLE runner_signal (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id TEXT NOT NULL UNIQUE,
                feature_sequence INTEGER NOT NULL,
                payload_json TEXT NOT NULL
            );
            CREATE TABLE runner_session_close_receipt (
                trade_date TEXT PRIMARY KEY,
                receipt_id TEXT NOT NULL UNIQUE,
                source_id TEXT NOT NULL,
                signal_high_watermark INTEGER NOT NULL,
                payload_json TEXT NOT NULL
            );
            """
        )
        connection.execute(
            "INSERT INTO runner_metadata VALUES (1, ?, '{}', ?)",
            (SPEC, "2" * 64),
        )
        connection.execute(
            "INSERT INTO runner_source_identity VALUES (1, ?)",
            (GENERATION,),
        )
        if signal is not None:
            connection.execute(
                """
                INSERT INTO runner_signal(signal_id, feature_sequence, payload_json)
                VALUES (?, 0, ?)
                """,
                (
                    signal.signal_id,
                    json.dumps(
                        signal.model_dump(mode="json"),
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                ),
            )
        if completion_receipt is not None:
            connection.execute(
                """
                INSERT INTO runner_session_close_receipt(
                    trade_date, receipt_id, source_id,
                    signal_high_watermark, payload_json
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    completion_receipt.trade_date.isoformat(),
                    completion_receipt.receipt_id,
                    completion_receipt.source_id,
                    completion_receipt.high_watermark,
                    json.dumps(
                        completion_receipt.model_dump(mode="json"),
                        ensure_ascii=False,
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                ),
            )
        connection.commit()
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")


def test_readonly_runner_source_requires_persisted_completion_and_freezes_prefix(
    tmp_path: Path,
) -> None:
    path = tmp_path / "runner.sqlite3"
    first = _signal("a")
    first_record = RunnerSignalRecord(sequence=1, signal=first)
    receipt = _runner_completion_receipt(records=(first_record,))
    _write_runner_source(path, signal=first, completion_receipt=receipt)
    second = _signal("2")
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            INSERT INTO runner_signal(signal_id, feature_sequence, payload_json)
            VALUES (?, 1, ?)
            """,
            (
                second.signal_id,
                json.dumps(second.model_dump(mode="json"), sort_keys=True),
            ),
        )
    source = ReadonlyStrategyRunnerSignalSource(
        source_id="n-shape-v1",
        path=path,
        expected_strategy_spec_fingerprint=SPEC,
        expected_evaluator_contract_fingerprint="2" * 64,
    )

    assert source.read_completion_receipt(trade_date=date(2026, 7, 31)) == receipt
    batch = source.read_completed_batch(
        trade_date=date(2026, 7, 31),
        after_sequence=0,
        limit=10,
    )
    assert batch.snapshot.descriptor.high_watermark == 1
    assert batch.records == (first_record,)


def test_readonly_runner_source_rejects_missing_or_tampered_completion_receipt(
    tmp_path: Path,
) -> None:
    path = tmp_path / "runner.sqlite3"
    signal = _signal()
    record = RunnerSignalRecord(sequence=1, signal=signal)
    _write_runner_source(path, signal=signal)
    source = ReadonlyStrategyRunnerSignalSource(
        source_id="n-shape-v1",
        path=path,
        expected_strategy_spec_fingerprint=SPEC,
        expected_evaluator_contract_fingerprint="2" * 64,
    )
    with pytest.raises(ValueError, match="completion receipt"):
        source.read_completion_receipt(trade_date=date(2026, 7, 31))

    receipt = _runner_completion_receipt(records=(record,))
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            INSERT INTO runner_session_close_receipt VALUES (?, ?, ?, ?, ?)
            """,
            (
                receipt.trade_date.isoformat(),
                receipt.receipt_id,
                receipt.source_id,
                receipt.high_watermark,
                json.dumps(receipt.model_dump(mode="json"), sort_keys=True),
            ),
        )
        connection.execute("UPDATE runner_session_close_receipt SET source_id = 'tampered'")
    with pytest.raises(ValueError, match="completion receipt"):
        source.read_completion_receipt(trade_date=date(2026, 7, 31))


def test_readonly_runner_source_reads_exact_identity_without_writing(tmp_path: Path) -> None:
    path = tmp_path / "runner.sqlite3"
    signal = _signal()
    _write_runner_source(path, signal=signal)
    before = (path.stat().st_size, path.stat().st_mtime_ns, tuple(tmp_path.iterdir()))

    source = ReadonlyStrategyRunnerSignalSource(
        source_id="n-shape-v1",
        path=path,
        expected_strategy_spec_fingerprint=SPEC,
        expected_evaluator_contract_fingerprint="2" * 64,
    )

    assert source.read_batch(after_sequence=0, limit=10) == RunnerSignalBatch(
        snapshot=SourceSnapshot(
            descriptor=RouteSourceDescriptor(
                source_id="n-shape-v1",
                generation_id=GENERATION,
                strategy_spec_fingerprint=SPEC,
                first_sequence=1,
                high_watermark=1,
            )
        ),
        after_sequence=0,
        limit=10,
        records=(RunnerSignalRecord(sequence=1, signal=signal),),
    )
    after = (path.stat().st_size, path.stat().st_mtime_ns, tuple(tmp_path.iterdir()))
    assert after == before


def test_readonly_runner_batch_uses_one_snapshot_during_concurrent_append(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "runner.sqlite3"
    first = _signal("a")
    second = _signal("2")
    _write_runner_source(path, signal=first)
    source = ReadonlyStrategyRunnerSignalSource(
        source_id="n-shape-v1",
        path=path,
        expected_strategy_spec_fingerprint=SPEC,
        expected_evaluator_contract_fingerprint="2" * 64,
    )
    real_connect = source._connect
    watermark_read = Barrier(2)
    append_committed = Barrier(2)

    class _ConnectionProxy:
        def __init__(self, connection: sqlite3.Connection) -> None:
            self.connection = connection

        def __enter__(self) -> _ConnectionProxy:
            self.connection.__enter__()
            return self

        def __exit__(self, *args: object) -> object:
            return self.connection.__exit__(*args)

        def execute(self, sql: str, parameters: object = ()) -> object:
            result = self.connection.execute(sql, parameters)  # type: ignore[arg-type]
            if "max(sequence)" in sql:
                watermark_read.wait()
                append_committed.wait()
            return result

    monkeypatch.setattr(source, "_connect", lambda: _ConnectionProxy(real_connect()))

    def append() -> None:
        watermark_read.wait()
        with sqlite3.connect(path) as connection:
            connection.execute(
                """
                INSERT INTO runner_signal(signal_id, feature_sequence, payload_json)
                VALUES (?, 0, ?)
                """,
                (
                    second.signal_id,
                    json.dumps(second.model_dump(mode="json"), sort_keys=True),
                ),
            )
        append_committed.wait()

    with ThreadPoolExecutor(max_workers=2) as executor:
        writer = executor.submit(append)
        batch = source.read_batch(after_sequence=0, limit=10)
        writer.result()

    assert batch.snapshot.descriptor.high_watermark == 1
    assert tuple(record.sequence for record in batch.records) == (1,)


def test_readonly_runner_batch_decodes_only_limit_rows_from_large_backlog(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "runner.sqlite3"
    _write_runner_source(path)
    candidates = (_signal(candidate_id=f"{index:06d}.SZ") for index in range(10_000))
    with sqlite3.connect(path) as connection:
        connection.executemany(
            """
            INSERT INTO runner_signal(signal_id, feature_sequence, payload_json)
            VALUES (?, 0, ?)
            """,
            (
                (
                    candidate.signal_id,
                    json.dumps(
                        candidate.model_dump(mode="json"),
                        ensure_ascii=True,
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                )
                for candidate in candidates
            ),
        )
    source = ReadonlyStrategyRunnerSignalSource(
        source_id="n-shape-v1",
        path=path,
        expected_strategy_spec_fingerprint=SPEC,
        expected_evaluator_contract_fingerprint="2" * 64,
    )
    real_loads = json.loads
    decoded = 0

    def counting_loads(value: str | bytes, **kwargs: object) -> object:
        nonlocal decoded
        decoded += 1
        return real_loads(value, **kwargs)

    monkeypatch.setattr("rquant.signal_router_runtime.json.loads", counting_loads)

    batch = source.read_batch(after_sequence=0, limit=7)

    assert len(batch.records) == 7
    assert batch.snapshot.descriptor.high_watermark == 10_000
    assert decoded == 7


def test_readonly_runner_source_rejects_oversized_utf8_row_before_json_parse(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "runner.sqlite3"
    signal = _signal()
    _write_runner_source(path)
    payload = json.dumps(
        {
            **signal.model_dump(mode="json"),
            "evidence": {"note": "量" * 64},
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            INSERT INTO runner_signal(signal_id, feature_sequence, payload_json)
            VALUES (?, 0, ?)
            """,
            (signal.signal_id, payload),
        )
    source = ReadonlyStrategyRunnerSignalSource(
        source_id="n-shape-v1",
        path=path,
        expected_strategy_spec_fingerprint=SPEC,
        expected_evaluator_contract_fingerprint="2" * 64,
        max_record_bytes=len(payload.encode("utf-8")) - 1,
    )
    monkeypatch.setattr(
        "rquant.signal_router_runtime.json.loads",
        lambda _value: pytest.fail("oversized payload must be rejected before json.loads"),
    )

    with pytest.raises(ValueError, match="record.*byte budget|too large"):
        source.read_batch(after_sequence=0, limit=1)


def test_readonly_runner_source_rejects_total_batch_before_stream_decode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "runner.sqlite3"
    signal = _signal()
    _write_runner_source(path)
    payload = json.dumps(signal.model_dump(mode="json"), separators=(",", ":"), sort_keys=True)
    with sqlite3.connect(path) as connection:
        connection.executemany(
            """
            INSERT INTO runner_signal(signal_id, feature_sequence, payload_json)
            VALUES (?, 0, ?)
            """,
            ((f"signal-{index}", payload) for index in range(3)),
        )
    source = ReadonlyStrategyRunnerSignalSource(
        source_id="n-shape-v1",
        path=path,
        expected_strategy_spec_fingerprint=SPEC,
        expected_evaluator_contract_fingerprint="2" * 64,
        max_raw_bytes=len(payload.encode("utf-8")) * 2,
        max_record_bytes=len(payload.encode("utf-8")),
    )
    monkeypatch.setattr(
        "rquant.signal_router_runtime.json.loads",
        lambda _value: pytest.fail("over-budget batch must fail in SQL preflight"),
    )

    with pytest.raises(ValueError, match="batch.*byte budget|too large"):
        source.read_batch(after_sequence=0, limit=3)


def test_readonly_runner_source_rejects_deep_json_before_decode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "runner.sqlite3"
    signal = _signal()
    _write_runner_source(path)
    nested: object = "leaf"
    for _ in range(80):
        nested = {"child": nested}
    payload = json.dumps(
        {**signal.model_dump(mode="json"), "evidence": {"nested": nested}},
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    with sqlite3.connect(path) as connection:
        connection.execute(
            "INSERT INTO runner_signal(signal_id, feature_sequence, payload_json) VALUES (?, 0, ?)",
            (signal.signal_id, payload),
        )
    source = ReadonlyStrategyRunnerSignalSource(
        source_id="n-shape-v1",
        path=path,
        expected_strategy_spec_fingerprint=SPEC,
        expected_evaluator_contract_fingerprint="2" * 64,
    )
    monkeypatch.setattr(
        "rquant.signal_router_runtime.json.loads",
        lambda _value: pytest.fail("deep payload must be rejected before json.loads"),
    )

    with pytest.raises(ValueError, match="depth"):
        source.read_batch(after_sequence=0, limit=1)


def test_readonly_runner_source_rejects_wide_json_before_decode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "runner.sqlite3"
    signal = _signal()
    _write_runner_source(path)
    payload = json.dumps(
        {
            **signal.model_dump(mode="json"),
            "evidence": {"wide": list(range(32))},
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    with sqlite3.connect(path) as connection:
        connection.execute(
            "INSERT INTO runner_signal(signal_id, feature_sequence, payload_json) VALUES (?, 0, ?)",
            (signal.signal_id, payload),
        )
    source = ReadonlyStrategyRunnerSignalSource(
        source_id="n-shape-v1",
        path=path,
        expected_strategy_spec_fingerprint=SPEC,
        expected_evaluator_contract_fingerprint="2" * 64,
    )
    monkeypatch.setattr("rquant.signal_router_runtime._MAX_JSON_NODES", 8)
    monkeypatch.setattr(
        "rquant.signal_router_runtime.json.loads",
        lambda _value: pytest.fail("wide payload must be rejected before json.loads"),
    )

    with pytest.raises(ValueError, match="node|width"):
        source.read_batch(after_sequence=0, limit=1)


def test_readonly_runner_receipt_rejects_deep_json_before_decode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "runner.sqlite3"
    signal = _signal()
    receipt = _runner_completion_receipt(records=(RunnerSignalRecord(sequence=1, signal=signal),))
    _write_runner_source(path, signal=signal, completion_receipt=receipt)
    nested = "{}"
    for _ in range(80):
        nested = '{"child":' + nested + "}"
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE runner_session_close_receipt SET payload_json = ?",
            (nested,),
        )
    source = ReadonlyStrategyRunnerSignalSource(
        source_id="n-shape-v1",
        path=path,
        expected_strategy_spec_fingerprint=SPEC,
        expected_evaluator_contract_fingerprint="2" * 64,
    )
    monkeypatch.setattr(
        "rquant.signal_router_runtime.json.loads",
        lambda _value: pytest.fail("deep receipt must be rejected before json.loads"),
    )

    with pytest.raises(ValueError, match="depth"):
        source.read_completion_receipt(trade_date=date(2026, 7, 31))


def test_readonly_route_authority_binds_persisted_generation_and_receipt_prefix(
    tmp_path: Path,
) -> None:
    signal = _signal()
    runner = FakeRunner((RunnerSignalRecord(sequence=1, signal=signal),))
    bus_path = tmp_path / "bus.sqlite3"
    bus = _bus(bus_path)
    _run(runner=runner, bus=bus, cursors=_cursors(tmp_path))

    evidence = ReadonlySignalRouteAuthority(
        path=bus_path,
        expected_routing_policy_fingerprint=POLICY,
    ).read_drain_evidence(
        source_id="n-shape-v1",
        runner_generation_id=GENERATION,
        strategy_spec_fingerprint=SPEC,
        trade_date=date(2026, 7, 31),
        segment_start_sequence=0,
        routed_through_sequence=1,
        observed_at=NOW + timedelta(seconds=1),
    )

    assert evidence.source_id == "n-shape-v1"
    assert evidence.runner_generation_id == GENERATION
    assert evidence.routed_through_sequence == 1
    assert evidence.last_sequence == 1
    assert evidence.signal_authority_generation_id == bus.source_descriptor().generation_id
    assert evidence.route_receipts_sha256 != "0" * 64


def test_route_receipt_hash_binds_routing_policy(tmp_path: Path) -> None:
    signal = _signal()
    runner = FakeRunner((RunnerSignalRecord(sequence=1, signal=signal),))
    bus_path = tmp_path / "bus.sqlite3"
    bus = _bus(bus_path)
    _run(runner=runner, bus=bus, cursors=_cursors(tmp_path))
    first = ReadonlySignalRouteAuthority(
        path=bus_path,
        expected_routing_policy_fingerprint=POLICY,
    ).read_drain_evidence(
        source_id="n-shape-v1",
        runner_generation_id=GENERATION,
        strategy_spec_fingerprint=SPEC,
        trade_date=date(2026, 7, 31),
        segment_start_sequence=0,
        routed_through_sequence=1,
        observed_at=NOW + timedelta(seconds=1),
    )
    changed_policy = "8" * 64
    with sqlite3.connect(bus_path) as connection:
        connection.execute(
            "UPDATE signal_route_source SET routing_policy_fingerprint = ? WHERE source_id = ?",
            (changed_policy, "n-shape-v1"),
        )
    second = ReadonlySignalRouteAuthority(
        path=bus_path,
        expected_routing_policy_fingerprint=changed_policy,
    ).read_drain_evidence(
        source_id="n-shape-v1",
        runner_generation_id=GENERATION,
        strategy_spec_fingerprint=SPEC,
        trade_date=date(2026, 7, 31),
        segment_start_sequence=0,
        routed_through_sequence=1,
        observed_at=NOW + timedelta(seconds=1),
    )

    assert second.route_receipts_sha256 != first.route_receipts_sha256


def test_route_authority_reads_only_current_session_segment_after_100k_history(
    tmp_path: Path,
) -> None:
    signal = _signal()
    runner = FakeRunner((RunnerSignalRecord(sequence=1, signal=signal),))
    bus_path = tmp_path / "bus.sqlite3"
    bus = _bus(bus_path)
    _run(runner=runner, bus=bus, cursors=_cursors(tmp_path))
    with sqlite3.connect(bus_path) as connection:
        connection.execute("PRAGMA foreign_keys = OFF")
        connection.execute(
            """
            WITH RECURSIVE counter(value) AS (
                SELECT 2
                UNION ALL SELECT value + 1 FROM counter WHERE value < 100001
            )
            INSERT INTO signal_route_receipt(
                source_id, source_sequence, signal_id, decision_fingerprint,
                disposition, reason_code, target_manifest_hash,
                target_manifest_json, routed_at
            )
            SELECT 'n-shape-v1', value, printf('historical-%06d', value),
                   ?, 'no_target', 'historical', ?, '[]', ?
            FROM counter
            """,
            ("1" * 64, "2" * 64, NOW.isoformat().replace("+00:00", "Z")),
        )
        connection.execute(
            """
            UPDATE signal_route_source
            SET observed_high_watermark = 100001, last_sequence = 100001
            WHERE source_id = 'n-shape-v1'
            """
        )

    evidence = ReadonlySignalRouteAuthority(
        path=bus_path,
        expected_routing_policy_fingerprint=POLICY,
        max_session_records=1,
        max_session_raw_bytes=4096,
        max_receipt_bytes=4096,
        deadline_seconds=1.0,
    ).read_drain_evidence(
        source_id="n-shape-v1",
        runner_generation_id=GENERATION,
        strategy_spec_fingerprint=SPEC,
        trade_date=date(2026, 7, 31),
        segment_start_sequence=100000,
        routed_through_sequence=100001,
        observed_at=NOW + timedelta(seconds=1),
    )

    assert evidence.segment_start_sequence == 100000
    assert evidence.segment_record_count == 1
    assert evidence.routed_through_sequence == 100001


def test_readonly_route_authority_fails_closed_for_backlog_or_identity_drift(
    tmp_path: Path,
) -> None:
    bus_path = tmp_path / "bus.sqlite3"
    bus = _bus(bus_path)
    bus.bind_route_source(
        RouteSourceDescriptor(
            source_id="n-shape-v1",
            generation_id=GENERATION,
            strategy_spec_fingerprint=SPEC,
            first_sequence=1,
            high_watermark=1,
        ),
        routing_policy_fingerprint=POLICY,
        observed_at=NOW,
    )
    authority = ReadonlySignalRouteAuthority(
        path=bus_path,
        expected_routing_policy_fingerprint=POLICY,
    )

    with pytest.raises(ValueError, match="backlog|routed through"):
        authority.read_drain_evidence(
            source_id="n-shape-v1",
            runner_generation_id=GENERATION,
            strategy_spec_fingerprint=SPEC,
            trade_date=date(2026, 7, 31),
            segment_start_sequence=0,
            routed_through_sequence=1,
            observed_at=NOW,
        )
    with pytest.raises(ValueError, match="generation"):
        authority.read_drain_evidence(
            source_id="n-shape-v1",
            runner_generation_id="0" * 64,
            strategy_spec_fingerprint=SPEC,
            trade_date=date(2026, 7, 31),
            segment_start_sequence=0,
            routed_through_sequence=0,
            observed_at=NOW,
        )


def test_readonly_route_authority_rejects_connection_to_another_database(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bus_path = tmp_path / "bus.sqlite3"
    other_path = tmp_path / "other.sqlite3"
    _bus(bus_path)
    _bus(other_path)
    authority = ReadonlySignalRouteAuthority(
        path=bus_path,
        expected_routing_policy_fingerprint=POLICY,
    )
    original_connect = sqlite3.connect

    def connect_other_database(*_args: object, **kwargs: object) -> sqlite3.Connection:
        kwargs.pop("uri", None)
        return original_connect(other_path, **kwargs)

    monkeypatch.setattr("rquant.signal_router_runtime.sqlite3.connect", connect_other_database)

    with pytest.raises(ValueError, match="resolved to another file"):
        authority.read_drain_evidence(
            source_id="n-shape-v1",
            runner_generation_id=GENERATION,
            strategy_spec_fingerprint=SPEC,
            trade_date=date(2026, 7, 31),
            segment_start_sequence=0,
            routed_through_sequence=0,
            observed_at=NOW,
        )


def test_readonly_runner_source_fails_closed_for_missing_symlink_or_identity_drift(
    tmp_path: Path,
) -> None:
    missing = tmp_path / "missing.sqlite3"
    with pytest.raises(ValueError, match="unavailable"):
        ReadonlyStrategyRunnerSignalSource(
            source_id="n-shape-v1",
            path=missing,
            expected_strategy_spec_fingerprint=SPEC,
            expected_evaluator_contract_fingerprint="2" * 64,
        )
    assert not missing.exists()

    path = tmp_path / "runner.sqlite3"
    _write_runner_source(path)
    linked = tmp_path / "linked.sqlite3"
    linked.symlink_to(path)
    with pytest.raises(ValueError, match="symlink"):
        ReadonlyStrategyRunnerSignalSource(
            source_id="n-shape-v1",
            path=linked,
            expected_strategy_spec_fingerprint=SPEC,
            expected_evaluator_contract_fingerprint="2" * 64,
        )

    with pytest.raises(ValueError, match="strategy spec"):
        ReadonlyStrategyRunnerSignalSource(
            source_id="n-shape-v1",
            path=path,
            expected_strategy_spec_fingerprint="3" * 64,
            expected_evaluator_contract_fingerprint="2" * 64,
        )
    with pytest.raises(ValueError, match="evaluator contract"):
        ReadonlyStrategyRunnerSignalSource(
            source_id="n-shape-v1",
            path=path,
            expected_strategy_spec_fingerprint=SPEC,
            expected_evaluator_contract_fingerprint="4" * 64,
        )


def test_concurrent_target_manifest_drift_conflicts_instead_of_forming_union(
    tmp_path: Path,
) -> None:
    signal = _signal()
    runner = FakeRunner((RunnerSignalRecord(sequence=1, signal=signal),))
    bus = _bus(tmp_path / "bus.sqlite3")
    barrier = Barrier(2)

    def run(target: DeliveryTarget) -> object:
        def resolver(_signal: SignalEnvelope) -> RoutingDecision:
            barrier.wait()
            return _route_decision(target)

        return _run(
            runner=runner,
            bus=bus,
            cursors=_cursors(tmp_path),
            resolver=resolver,
        )

    targets = (_target("admin"), _target("research"))
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = tuple(executor.submit(run, target) for target in targets)
        outcomes: list[object] = []
        errors: list[BaseException] = []
        for future in futures:
            try:
                outcomes.append(future.result())
            except BaseException as exc:  # noqa: BLE001 - asserting isolation result
                errors.append(exc)

    assert len(outcomes) == 1
    assert len(errors) == 1
    assert isinstance(errors[0], SignalRouteConflictError)
    outbox = bus.outbox_records(signal_id=signal.signal_id)
    assert len(outbox) == 1
    assert outbox[0].target in targets


def test_legacy_route_api_cannot_expand_a_frozen_source_target_manifest(
    tmp_path: Path,
) -> None:
    signal = _signal()
    bus = _bus(tmp_path / "bus.sqlite3")
    _run(
        runner=FakeRunner((RunnerSignalRecord(sequence=1, signal=signal),)),
        bus=bus,
        cursors=_cursors(tmp_path),
    )

    assert len(bus.route(signal.signal_id, (_target(),), now=NOW)) == 1
    with pytest.raises(SignalRouteConflictError, match="frozen target manifest"):
        bus.route(signal.signal_id, (_target("research"),), now=NOW)
    assert len(bus.outbox_records(signal_id=signal.signal_id)) == 1


def test_source_generation_drift_fails_even_when_source_returns_empty(
    tmp_path: Path,
) -> None:
    bus = _bus(tmp_path / "bus.sqlite3")
    cursors = _cursors(tmp_path)
    _run(runner=FakeRunner(()), bus=bus, cursors=cursors)

    with pytest.raises(SignalRouteConflictError, match="generation"):
        _run(
            runner=FakeRunner((), generation_id="2" * 64),
            bus=bus,
            cursors=cursors,
        )


def test_source_spec_and_routing_policy_are_frozen_in_the_bus(tmp_path: Path) -> None:
    bus = _bus(tmp_path / "bus.sqlite3")
    _run(runner=FakeRunner(()), bus=bus, cursors=_cursors(tmp_path))

    with pytest.raises(SignalRouteConflictError, match="strategy spec"):
        _run(
            runner=FakeRunner((), spec_fingerprint="3" * 64),
            bus=bus,
            cursors=_cursors(tmp_path),
        )
    with pytest.raises(SignalRouteConflictError, match="routing policy"):
        route_runner_signals(
            source_id="n-shape-v1",
            source=FakeRunner(()),
            bus=bus,
            cursors=SignalRouteCursorStore(
                tmp_path / "ignored.sqlite3",
                routing_policy_fingerprint="4" * 64,
            ),
            routed_at=NOW,
            target_resolver=lambda _signal: _route_decision(),
            limit=10,
        )


def test_empty_source_detects_high_watermark_rollback_and_tail_truncation(
    tmp_path: Path,
) -> None:
    signal = _signal()
    bus = _bus(tmp_path / "bus.sqlite3")
    cursors = _cursors(tmp_path)
    _run(
        runner=FakeRunner((RunnerSignalRecord(sequence=1, signal=signal),)),
        bus=bus,
        cursors=cursors,
    )

    with pytest.raises(SignalRouteSequenceError, match="high watermark regressed"):
        _run(
            runner=FakeRunner((), high_watermark=0),
            bus=bus,
            cursors=cursors,
        )

    fresh_bus = _bus(tmp_path / "fresh-bus.sqlite3")
    with pytest.raises(SignalRouteSequenceError, match="source tail is missing"):
        _run(
            runner=FakeRunner((), high_watermark=1),
            bus=fresh_bus,
            cursors=_cursors(tmp_path),
        )
    assert fresh_bus.route_cursor("n-shape-v1").last_sequence == 0


def test_sequence_gap_fails_closed_without_advancing_cursor(tmp_path: Path) -> None:
    runner = FakeRunner((RunnerSignalRecord(sequence=2, signal=_signal()),))
    bus = _bus(tmp_path / "bus.sqlite3")

    with pytest.raises(SignalRouteSequenceError, match="expected runner sequence 1"):
        _run(runner=runner, bus=bus, cursors=_cursors(tmp_path))

    assert bus.route_cursor("n-shape-v1").last_sequence == 0


@pytest.mark.parametrize(
    ("returned_after_sequence", "returned_limit"),
    [(1, 10), (0, 11)],
)
def test_source_batch_must_match_the_exact_router_request(
    tmp_path: Path,
    returned_after_sequence: int,
    returned_limit: int,
) -> None:
    class MismatchedSource:
        @staticmethod
        def read_batch(
            *,
            after_sequence: int,
            limit: int,
        ) -> RunnerSignalBatch:
            del after_sequence, limit
            return RunnerSignalBatch(
                snapshot=SourceSnapshot(
                    descriptor=RouteSourceDescriptor(
                        source_id="n-shape-v1",
                        generation_id=GENERATION,
                        strategy_spec_fingerprint=SPEC,
                        first_sequence=1,
                        high_watermark=0,
                    )
                ),
                after_sequence=returned_after_sequence,
                limit=returned_limit,
                records=(),
            )

    with pytest.raises(SignalRouteConflictError, match="batch request"):
        route_runner_signals(
            source_id="n-shape-v1",
            source=MismatchedSource(),
            bus=_bus(tmp_path / "bus.sqlite3"),
            cursors=_cursors(tmp_path),
            routed_at=NOW,
            target_resolver=lambda _signal: _route_decision(),
            limit=10,
        )


def test_no_target_is_explicitly_persisted_and_counted(tmp_path: Path) -> None:
    signal = _signal()
    bus = _bus(tmp_path / "bus.sqlite3")

    summary = _run(
        runner=FakeRunner((RunnerSignalRecord(sequence=1, signal=signal),)),
        bus=bus,
        cursors=_cursors(tmp_path),
        resolver=lambda _signal: RoutingDecision.no_target(
            routing_policy_fingerprint=POLICY,
            reason_code="recipient-opted-out",
        ),
    )

    assert summary.no_target_count == 1
    assert summary.routed_count == 0
    assert summary.target_count == 0
    receipt = bus.route_receipts("n-shape-v1")[0]
    assert receipt.reason_code == "recipient-opted-out"
    assert receipt.target_count == 0
    assert bus.outbox_records() == ()


def test_temporary_routing_configuration_error_does_not_advance(tmp_path: Path) -> None:
    signal = _signal()
    bus = _bus(tmp_path / "bus.sqlite3")

    def unavailable(_signal: SignalEnvelope) -> RoutingDecision:
        raise RoutingConfigurationUnavailableError("recipient registry unavailable")

    with pytest.raises(
        RoutingConfigurationUnavailableError,
        match="recipient registry unavailable",
    ):
        _run(
            runner=FakeRunner((RunnerSignalRecord(sequence=1, signal=signal),)),
            bus=bus,
            cursors=_cursors(tmp_path),
            resolver=unavailable,
        )

    assert bus.route_cursor("n-shape-v1").last_sequence == 0
    assert bus.route_receipts("n-shape-v1") == ()


def test_future_and_expired_signals_have_independent_summary_counts(
    tmp_path: Path,
) -> None:
    expired = _signal("a", available_at=NOW - timedelta(minutes=10))
    future = _signal("2", available_at=NOW + timedelta(seconds=1))
    runner = FakeRunner(
        (
            RunnerSignalRecord(sequence=1, signal=expired),
            RunnerSignalRecord(sequence=2, signal=future),
        )
    )
    bus = _bus(tmp_path / "bus.sqlite3")

    summary = _run(runner=runner, bus=bus, cursors=_cursors(tmp_path))

    assert summary.expired_count == 1
    assert summary.deferred_count == 1
    assert summary.routed_count == 0
    assert summary.last_sequence == 1
    assert bus.outbox_records(signal_id=expired.signal_id)[0].status is OutboxStatus.EXPIRED
    assert bus.signal(future.signal_id) is None


def test_policy_fingerprint_and_limit_use_pydantic_validation(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="routing_policy_fingerprint"):
        SignalRouteCursorStore(
            tmp_path / "cursor.sqlite3",
            routing_policy_fingerprint="not-a-sha",
        )

    runner = FakeRunner(())
    bus = _bus(tmp_path / "bus.sqlite3")
    for invalid in (True, 1.5, 0):
        with pytest.raises((ValidationError, ValueError)):
            _run(
                runner=runner,
                bus=bus,
                cursors=_cursors(tmp_path),
                limit=invalid,  # type: ignore[arg-type]
            )


def test_restoring_the_bus_database_restores_cursor_receipts_and_outbox_together(
    tmp_path: Path,
) -> None:
    first_signal = _signal("a")
    second_signal = _signal("2", available_at=NOW + timedelta(seconds=1))
    records = (
        RunnerSignalRecord(sequence=1, signal=first_signal),
        RunnerSignalRecord(sequence=2, signal=second_signal),
    )
    source_path = tmp_path / "bus.sqlite3"
    source_bus = _bus(source_path)
    _run(
        runner=FakeRunner(records, high_watermark=2),
        bus=source_bus,
        cursors=_cursors(tmp_path),
        limit=1,
    )

    restored_path = tmp_path / "restored.sqlite3"
    with (
        sqlite3.connect(source_path) as source_connection,
        sqlite3.connect(restored_path) as restored_connection,
    ):
        source_connection.backup(restored_connection)
    restored_bus = _bus(restored_path)

    summary = _run(
        runner=FakeRunner(records, high_watermark=2),
        bus=restored_bus,
        cursors=SignalRouteCursorStore(
            tmp_path / "fresh-facade.sqlite3",
            routing_policy_fingerprint=POLICY,
        ),
        routed_at=NOW + timedelta(seconds=1),
    )

    assert summary.started_after_sequence == 1
    assert summary.last_sequence == 2
    assert len(restored_bus.route_receipts("n-shape-v1")) == 2
    assert len(restored_bus.outbox_records()) == 2
