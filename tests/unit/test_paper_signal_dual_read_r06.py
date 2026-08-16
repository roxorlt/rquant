from __future__ import annotations

import hashlib
import sqlite3
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from rquant.feature_contracts import FeatureAvailability, FeatureFieldStatus
from rquant.live_contracts import BatchQualityStatus
from rquant.paper_broker import PaperBrokerStore
from rquant.paper_signal_consumer import (
    PaperSignalConsumerStateStore,
    consume_signal_bus_to_paper,
)
from rquant.paper_signal_worker import (
    PaperQuoteSnapshot,
    PaperSignalPolicy,
    PaperSignalQueueStore,
    run_paper_signal_batch,
)
from rquant.runtime_paper_quote import PaperPitQuoteResolver
from rquant.signal_bus import SignalBusSignalRecord, SignalBusSourceDescriptor
from rquant.signal_contracts import (
    CurrentSignalEnvelope,
    SignalEnvelope,
    SignalEnvelopeFamily,
    parse_signal_envelope,
)
from rquant.strategy_paper_lifecycle import PaperBrokerLifecycleReader
from tests.paper_cost_fixtures import paper_cost_policy, paper_instrument_context
from tests.unit.test_signal_contracts import (
    _CURRENT_CANONICAL_FIXTURES,
    _LEGACY_CANONICAL_FIXTURES,
)

NOW = datetime(2026, 7, 31, 1, 40, tzinfo=UTC)
COMMIT = "a" * 40
CODE = "600000.SH"

_FAMILIES = (
    (
        "legacy-v1",
        SignalEnvelope,
        _LEGACY_CANONICAL_FIXTURES[0][2],
        _LEGACY_CANONICAL_FIXTURES[0][3],
    ),
    (
        "legacy-v2",
        SignalEnvelope,
        _LEGACY_CANONICAL_FIXTURES[2][2],
        _LEGACY_CANONICAL_FIXTURES[2][3],
    ),
    (
        "legacy-v3",
        SignalEnvelope,
        _LEGACY_CANONICAL_FIXTURES[4][2],
        _LEGACY_CANONICAL_FIXTURES[4][3],
    ),
    (
        "current-git-claim",
        CurrentSignalEnvelope,
        _CURRENT_CANONICAL_FIXTURES[0][1],
        _CURRENT_CANONICAL_FIXTURES[0][2],
    ),
    (
        "current-full-manifest",
        CurrentSignalEnvelope,
        _CURRENT_CANONICAL_FIXTURES[1][1],
        _CURRENT_CANONICAL_FIXTURES[1][2],
    ),
)

_INVALID_CURRENT_PAYLOADS = (
    ("malformed", b"{"),
    (
        "unknown",
        _CURRENT_CANONICAL_FIXTURES[0][2].replace(
            b'"rquant.signal-envelope/v1"', b'"rquant.signal-envelope/v999"'
        ),
    ),
    (
        "mixed",
        _CURRENT_CANONICAL_FIXTURES[0][2][:-1]
        + b',"producer_commit":"dddddddddddddddddddddddddddddddddddddddd"}',
    ),
    (
        "duplicate",
        _CURRENT_CANONICAL_FIXTURES[0][2].replace(
            b'"strategy_id":"n-shape"',
            b'"strategy_id":"n-shape","strategy_id":"n-shape"',
        ),
    ),
)


def _policy() -> PaperSignalPolicy:
    return PaperSignalPolicy(
        account_id="paper-r06",
        execution_lag=timedelta(seconds=1),
        action_quantities={
            "b_intent": 100,
            "reduce": 100,
            "s_intent": 100,
        },
        producer_commit=COMMIT,
    )


def _queue(path: Path) -> PaperSignalQueueStore:
    return PaperSignalQueueStore(path, policy=_policy())


def _quote(signal: SignalEnvelopeFamily) -> PaperQuoteSnapshot:
    from rquant.paper_broker import BrokerExecutionContext, paper_instrument_context

    return PaperQuoteSnapshot(
        ts_code=signal.candidate_id,
        event_time=NOW,
        available_at=NOW,
        context=BrokerExecutionContext(
            executable_price=Decimal("10.00"),
            instrument_context=paper_instrument_context(signal.candidate_id),
            acquisition_available_date=date(2026, 8, 3),
        ),
        producer_commit=COMMIT,
    )


def _record(sequence: int, literal: bytes) -> SignalBusSignalRecord:
    signal = parse_signal_envelope(literal)
    return SignalBusSignalRecord(
        global_sequence=sequence,
        signal_id=signal.signal_id,
        payload_hash=hashlib.sha256(literal).hexdigest(),
        payload_json=literal.decode("utf-8"),
        signal=signal,
        received_at=NOW,
    )


class _Source:
    def __init__(self, records: tuple[object, ...]) -> None:
        self.records = records
        self.descriptor = SignalBusSourceDescriptor(
            generation_id="f" * 64,
            high_watermark=len(records),
        )

    def source_descriptor(self) -> SignalBusSourceDescriptor:
        return self.descriptor

    def signals_after_global_sequence(
        self,
        *,
        after_sequence: int,
        through_sequence: int,
        observed_at: datetime,
        limit: int,
    ) -> tuple[object, ...]:
        del through_sequence, observed_at
        return tuple(self.records[after_sequence : after_sequence + limit])


def _snapshot(path: Path) -> tuple[tuple[str, tuple[tuple[object, ...], ...]], ...]:
    with sqlite3.connect(path) as connection:
        tables = tuple(
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
            )
        )
        return tuple(
            (table, tuple(connection.execute(f'SELECT * FROM "{table}" ORDER BY rowid')))
            for table in tables
        )


def _schema_and_data_snapshot(path: Path) -> tuple[object, ...]:
    with sqlite3.connect(path) as connection:
        schema = tuple(
            connection.execute(
                """
                SELECT type, name, tbl_name, sql
                FROM sqlite_master
                WHERE name NOT LIKE 'sqlite_%'
                ORDER BY type, name
                """
            )
        )
        tables = tuple(
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
            )
        )
        columns = tuple(
            (table, tuple(connection.execute(f'PRAGMA table_info("{table}")'))) for table in tables
        )
    return schema, columns, _snapshot(path)


@pytest.mark.parametrize(("_name", "expected_type", "expected_id", "literal"), _FAMILIES)
def test_paper_consumer_copies_every_verified_signal_family_to_the_queue(
    tmp_path: Path,
    _name: str,
    expected_type: type[SignalEnvelopeFamily],
    expected_id: str,
    literal: bytes,
) -> None:
    queue = _queue(tmp_path / "queue.sqlite3")
    state = PaperSignalConsumerStateStore(tmp_path / "consumer.sqlite3")

    summary = consume_signal_bus_to_paper(
        _Source((_record(1, literal),)),  # type: ignore[arg-type]
        queue,
        state,
        observed_at=NOW,
        limit=1,
    )

    queued = queue.record(expected_id)
    assert summary.delegated_count == 1
    assert queued is not None
    assert type(queued.signal) is expected_type
    assert queued.signal.signal_id == expected_id
    assert state.receipt(1) is not None


@pytest.mark.parametrize(("_name", "_expected_type", "expected_id", "literal"), _FAMILIES)
@pytest.mark.parametrize("tamper", ("signal_id", "payload_hash", "payload_size"))
def test_queue_integrity_mismatch_stops_before_quote_or_paper_order_mutation(
    tmp_path: Path,
    _name: str,
    _expected_type: type[SignalEnvelopeFamily],
    expected_id: str,
    literal: bytes,
    tamper: str,
) -> None:
    queue_path = tmp_path / "queue.sqlite3"
    queue = _queue(queue_path)
    state = PaperSignalConsumerStateStore(tmp_path / "consumer.sqlite3")
    consume_signal_bus_to_paper(
        _Source((_record(1, literal),)),  # type: ignore[arg-type]
        queue,
        state,
        observed_at=NOW,
        limit=1,
    )
    with sqlite3.connect(queue_path) as connection:
        if tamper == "signal_id":
            connection.execute(
                "UPDATE paper_signal_queue SET signal_id = ? WHERE signal_id = ?",
                ("0" * 64, expected_id),
            )
        elif tamper == "payload_hash":
            connection.execute(
                "UPDATE paper_signal_queue SET signal_hash = ? WHERE signal_id = ?",
                ("0" * 64, expected_id),
            )
        else:
            connection.execute(
                "UPDATE paper_signal_queue SET signal_size = 0 WHERE signal_id = ?",
                (expected_id,),
            )
    before = _snapshot(queue_path)
    quote_calls = 0

    def quote_resolver(signal: SignalEnvelopeFamily, observed_at: datetime) -> PaperQuoteSnapshot:
        nonlocal quote_calls
        del observed_at
        quote_calls += 1
        return _quote(signal)

    with pytest.raises(RuntimeError, match="stored signal|payload"):
        run_paper_signal_batch(
            queue,
            object(),  # type: ignore[arg-type]
            now=NOW + timedelta(seconds=5),
            trade_date=date(2026, 7, 31),
            quote_resolver=quote_resolver,
            limit=1,
        )

    assert quote_calls == 0
    assert _snapshot(queue_path) == before


def test_due_batch_integrity_failure_does_not_expire_another_row(tmp_path: Path) -> None:
    queue_path = tmp_path / "queue.sqlite3"
    queue = _queue(queue_path)
    expiring = _record(1, _FAMILIES[0][3])
    corrupt_due = _record(2, _FAMILIES[1][3])
    for record in (expiring, corrupt_due):
        queue.ingest(
            record.signal,
            received_at=NOW,
            payload_json=record.payload_json,
            payload_hash=record.payload_hash,
            payload_size=len(record.payload_json.encode("utf-8")),
        )
    with sqlite3.connect(queue_path) as connection:
        connection.execute(
            "UPDATE paper_signal_queue SET expires_at = ? WHERE signal_id = ?",
            (NOW.isoformat(), expiring.signal_id),
        )
        connection.execute(
            "UPDATE paper_signal_queue SET signal_hash = ? WHERE signal_id = ?",
            ("0" * 64, corrupt_due.signal_id),
        )
    before = _snapshot(queue_path)

    with pytest.raises(RuntimeError, match="stored signal|payload"):
        queue.due_records(now=NOW, limit=10)

    assert _snapshot(queue_path) == before


def test_corrupt_late_legacy_row_rolls_back_the_entire_queue_migration(
    tmp_path: Path,
) -> None:
    queue_path = tmp_path / "legacy-queue.sqlite3"
    policy = _policy()
    valid = _record(1, _FAMILIES[0][3])
    corrupt = _record(2, _FAMILIES[1][3])
    with sqlite3.connect(queue_path) as connection:
        connection.executescript(
            """
            CREATE TABLE paper_signal_metadata (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                policy_fingerprint TEXT NOT NULL,
                policy_json TEXT NOT NULL
            );
            CREATE TABLE paper_signal_queue (
                signal_id TEXT PRIMARY KEY,
                signal_json TEXT NOT NULL,
                status TEXT NOT NULL,
                due_at TEXT NOT NULL,
                received_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_error TEXT,
                quote_json TEXT,
                intent_json TEXT,
                order_json TEXT
            );
            """
        )
        connection.execute(
            """
            INSERT INTO paper_signal_metadata(
                singleton, policy_fingerprint, policy_json
            ) VALUES (1, ?, ?)
            """,
            (policy.provenance_fingerprint, policy.model_dump_json()),
        )
        for record, signal_id in (
            (valid, valid.signal_id),
            (corrupt, "0" * 64),
        ):
            connection.execute(
                """
                INSERT INTO paper_signal_queue(
                    signal_id, signal_json, status, due_at,
                    received_at, updated_at, last_error,
                    quote_json, intent_json, order_json
                ) VALUES (?, ?, 'pending', ?, ?, ?, NULL, NULL, NULL, NULL)
                """,
                (
                    signal_id,
                    record.payload_json,
                    record.signal.available_at.isoformat(),
                    NOW.isoformat(),
                    NOW.isoformat(),
                ),
            )
    before = _schema_and_data_snapshot(queue_path)

    with pytest.raises(RuntimeError, match="stored signal|payload"):
        PaperSignalQueueStore(queue_path, policy=policy)

    assert _schema_and_data_snapshot(queue_path) == before


@pytest.mark.parametrize(("_name", "literal"), _INVALID_CURRENT_PAYLOADS)
def test_invalid_bus_payload_fails_before_consumer_or_queue_mutation(
    tmp_path: Path,
    _name: str,
    literal: bytes,
) -> None:
    queue_path = tmp_path / "queue.sqlite3"
    state_path = tmp_path / "consumer.sqlite3"
    queue = _queue(queue_path)
    state = PaperSignalConsumerStateStore(state_path)
    valid = _record(1, _FAMILIES[0][3])
    invalid = SimpleNamespace(
        global_sequence=1,
        signal_id=valid.signal_id,
        payload_hash=hashlib.sha256(literal).hexdigest(),
        payload_json=literal.decode("utf-8", errors="replace"),
        signal=valid.signal,
        received_at=NOW,
    )
    before_queue = _snapshot(queue_path)
    before_state = _snapshot(state_path)

    with pytest.raises((TypeError, ValueError, RuntimeError)):
        consume_signal_bus_to_paper(
            _Source((invalid,)),  # type: ignore[arg-type]
            queue,
            state,
            observed_at=NOW,
            limit=1,
        )

    assert _snapshot(queue_path) == before_queue
    assert _snapshot(state_path) == before_state


@pytest.mark.parametrize(("_name", "expected_type", "_expected_id", "literal"), _FAMILIES)
def test_paper_quote_and_lifecycle_accept_every_verified_signal_family(
    tmp_path: Path,
    _name: str,
    expected_type: type[SignalEnvelopeFamily],
    _expected_id: str,
    literal: bytes,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    signal = parse_signal_envelope(literal)
    assert type(signal) is expected_type
    resolver = object.__new__(PaperPitQuoteResolver)
    resolver.config = SimpleNamespace(quote_max_age_seconds=300, timestamp_semantics="bar_end")
    monkeypatch.setattr(
        resolver, "_require_trade_session", lambda _at: (date(2026, 7, 31), "morning")
    )
    monkeypatch.setattr(
        resolver,
        "_latest_visible_batch",
        lambda _at: (
            SimpleNamespace(
                quality_status=BatchQualityStatus.PUBLISHED,
                sequence=1,
                available_at=NOW,
                producer_commit=COMMIT,
            ),
            b"",
        ),
    )
    monkeypatch.setattr(
        resolver,
        "_latest_candidate_row",
        lambda *_args, **_kwargs: {
            "trade_time": SimpleNamespace(to_pydatetime=lambda: NOW),
            "close": 10.0,
        },
    )
    monkeypatch.setattr(resolver, "_next_sse_open_day", lambda _day: date(2026, 8, 3))
    resolver._execution_constraints = SimpleNamespace(
        resolve=lambda **_kwargs: SimpleNamespace(
            available_at=NOW,
            suspended=False,
            limit_locked=False,
            risk_rejected=False,
            instrument_context=paper_instrument_context(CODE),
            constraint_content_hash="3" * 64,
            batch_content_hash="4" * 64,
            authority_file_sha256="5" * 64,
            source_snapshot_ids={"listing": "6" * 64},
        )
    )

    quote = resolver.resolve(signal, observed_at=NOW)
    PaperBrokerStore(
        tmp_path / "broker.sqlite3",
        account_id="paper-r06",
        initial_cash=Decimal("100000"),
        cost_policy=paper_cost_policy(),
    )
    lifecycle = PaperBrokerLifecycleReader(tmp_path / "broker.sqlite3", account_id="paper-r06")
    result = lifecycle.resolve(
        candidate_id=signal.candidate_id,
        entry_signal=signal,
        exit_signals=(),
        decision_cutoff=NOW,
        market_features={"latest_close": 10.0, "session_high": 10.0},
        market_feature_statuses={
            "session_high": FeatureFieldStatus(
                candidate_id=signal.candidate_id,
                name="session_high",
                status=FeatureAvailability.AVAILABLE,
                source_event_time=NOW,
                available_at=NOW,
                decision_cutoff=NOW,
                actual_delay_seconds=0.0,
            )
        },
        previous_eligible_high_price_raw=None,
        previous_high_source_event_time=None,
        previous_high_available_at=None,
    )

    assert quote.ts_code == signal.candidate_id
    assert result.values["entry_fill_status"] == "pending"
