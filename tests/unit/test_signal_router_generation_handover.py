"""#248 shapes (1) and (2) on the router's side of the generation change.

Shape (1), read side: on 2026-09-09 `signal_router` refused with `runner source strategy
spec identity does not match` for the same reason the strategies did — the runner
databases on disk were the previous generation's. But the router is not the owner of
those files: the strategy is, and the strategy rotates them the moment it starts. So a
runner whose identity belongs to our own previous generation is something to *wait* for,
the way package J made the router wait for a runner that does not exist yet (#232), not
something to refuse. A foreign identity is still a refusal.

Shape (2): once the strategy has rotated, its new runner database carries a fresh random
`source_generation_id`, and `signal_route_source` still holds the old one, so
`bind_route_source` raised `SignalRouteConflictError: source '...' generation changed` on
every iteration. Rotating that row is only allowed when the *stored* strategy spec
fingerprint is one of our own previous generations' and the incoming one is the current
generation's; every other difference stays a conflict.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest

from rquant.runtime_peer_artifacts import DeferredPeerArtifact, PeerArtifactUnavailableError
from rquant.signal_bus import RouteSourceDescriptor, SignalBusStore, SignalRouteConflictError
from rquant.signal_router_runtime import ReadonlyStrategyRunnerSignalSource
from rquant.strategy_runner import StrategyRunnerStore
from tests.unit.test_strategy_runner import EVALUATOR_FINGERPRINT, _spec

PREVIOUS_COMMIT = "1" * 40
CURRENT_COMMIT = "2" * 40
FOREIGN_COMMIT = "3" * 40
PREVIOUS_EVALUATOR = "a" * 64
PREVIOUS_GENERATION = "f" * 64
ROUTING_POLICY = "9" * 64
NOW = datetime(2026, 9, 9, 3, 2, tzinfo=UTC)


def _previous_generation(mapping: dict[tuple[str, str], str]) -> object:
    def resolve(spec_fingerprint: str, evaluator_fingerprint: str) -> str | None:
        return mapping.get((spec_fingerprint, evaluator_fingerprint))

    return resolve


def _runner(path: Path, *, commit: str, evaluator: str) -> str:
    spec = _spec(producer_commit=commit)
    StrategyRunnerStore(path, spec=spec, evaluator_contract_fingerprint=evaluator)
    return spec.spec_fingerprint


# ---------------------------------------------------------------------------------------
# Shape (1), read side: waiting for the owner to rotate is not refusing
# ---------------------------------------------------------------------------------------


def test_a_runner_from_our_own_previous_generation_is_waited_for(tmp_path: Path) -> None:
    path = tmp_path / "runner.sqlite3"
    previous_fingerprint = _runner(path, commit=PREVIOUS_COMMIT, evaluator=PREVIOUS_EVALUATOR)

    with pytest.raises(PeerArtifactUnavailableError) as raised:
        ReadonlyStrategyRunnerSignalSource(
            source_id="strategy/growth",
            path=path,
            expected_strategy_spec_fingerprint=_spec(
                producer_commit=CURRENT_COMMIT
            ).spec_fingerprint,
            expected_evaluator_contract_fingerprint=EVALUATOR_FINGERPRINT,
            previous_generation_of_identity=_previous_generation(
                {(previous_fingerprint, PREVIOUS_EVALUATOR): PREVIOUS_GENERATION}
            ),
        )

    assert PREVIOUS_GENERATION in str(raised.value)
    assert str(path) in str(raised.value)


def test_a_foreign_runner_identity_is_still_refused(tmp_path: Path) -> None:
    path = tmp_path / "runner.sqlite3"
    _runner(path, commit=FOREIGN_COMMIT, evaluator=PREVIOUS_EVALUATOR)

    with pytest.raises(ValueError, match="runner source strategy spec identity does not match"):
        ReadonlyStrategyRunnerSignalSource(
            source_id="strategy/growth",
            path=path,
            expected_strategy_spec_fingerprint=_spec(
                producer_commit=CURRENT_COMMIT
            ).spec_fingerprint,
            expected_evaluator_contract_fingerprint=EVALUATOR_FINGERPRINT,
            previous_generation_of_identity=_previous_generation({}),
        )


def test_waiting_for_a_rotation_does_not_stop_the_probe(tmp_path: Path) -> None:
    """`DeferredPeerArtifact.probe()` is what the router runs while it is being built."""

    path = tmp_path / "runner.sqlite3"
    previous_fingerprint = _runner(path, commit=PREVIOUS_COMMIT, evaluator=PREVIOUS_EVALUATOR)
    current = _spec(producer_commit=CURRENT_COMMIT)

    def open_source() -> ReadonlyStrategyRunnerSignalSource:
        return ReadonlyStrategyRunnerSignalSource(
            source_id="strategy/growth",
            path=path,
            expected_strategy_spec_fingerprint=current.spec_fingerprint,
            expected_evaluator_contract_fingerprint=EVALUATOR_FINGERPRINT,
            previous_generation_of_identity=_previous_generation(
                {(previous_fingerprint, PREVIOUS_EVALUATOR): PREVIOUS_GENERATION}
            ),
        )

    deferred: DeferredPeerArtifact[ReadonlyStrategyRunnerSignalSource] = DeferredPeerArtifact(
        reader="signal_router",
        artifact="runner source",
        path=path,
        open_artifact=open_source,
    )

    assert deferred.probe() is None
    with pytest.raises(PeerArtifactUnavailableError, match=PREVIOUS_GENERATION):
        deferred.get()

    #: the strategy rotates, and the next iteration finds the database it was waiting for
    path.unlink()
    for suffix in ("-wal", "-shm"):
        sidecar = path.with_name(f"{path.name}{suffix}")
        if sidecar.exists():
            sidecar.unlink()
    StrategyRunnerStore(
        path,
        spec=current,
        evaluator_contract_fingerprint=EVALUATOR_FINGERPRINT,
    )

    assert deferred.probe() is not None


def test_a_probe_that_refuses_for_any_other_reason_still_stops_the_router(
    tmp_path: Path,
) -> None:
    path = tmp_path / "runner.sqlite3"
    path.write_bytes(b"not a database")

    def open_source() -> ReadonlyStrategyRunnerSignalSource:
        raise ValueError("runner source schema is unavailable")

    deferred: DeferredPeerArtifact[ReadonlyStrategyRunnerSignalSource] = DeferredPeerArtifact(
        reader="signal_router",
        artifact="runner source",
        path=path,
        open_artifact=open_source,
    )

    with pytest.raises(ValueError, match="runner source schema is unavailable"):
        deferred.probe()


# ---------------------------------------------------------------------------------------
# Shape (2): the route ledger's source row
# ---------------------------------------------------------------------------------------


def _descriptor(*, generation: str, spec_fingerprint: str, high_watermark: int = 0) -> object:
    return RouteSourceDescriptor(
        source_id="strategy/growth",
        generation_id=generation,
        strategy_spec_fingerprint=spec_fingerprint,
        first_sequence=1,
        high_watermark=high_watermark,
    )


def _bus(tmp_path: Path, **kwargs: object) -> SignalBusStore:
    return SignalBusStore(tmp_path / "signal_bus.sqlite3", **kwargs)


def test_a_source_generation_written_by_our_own_previous_generation_rotates(
    tmp_path: Path,
) -> None:
    previous_spec = _spec(producer_commit=PREVIOUS_COMMIT).spec_fingerprint
    current_spec = _spec(producer_commit=CURRENT_COMMIT).spec_fingerprint
    bus = _bus(
        tmp_path,
        previous_generation_of_strategy_spec={previous_spec: PREVIOUS_GENERATION},
    )
    bus.bind_route_source(
        _descriptor(generation="1" * 64, spec_fingerprint=previous_spec),
        routing_policy_fingerprint=ROUTING_POLICY,
        observed_at=NOW,
    )

    cursor = bus.bind_route_source(
        _descriptor(generation="2" * 64, spec_fingerprint=current_spec),
        routing_policy_fingerprint=ROUTING_POLICY,
        observed_at=NOW,
    )

    assert cursor.last_sequence == 0
    rotations = bus.route_source_rotations("strategy/growth")
    assert [item.previous_generation_id for item in rotations] == [PREVIOUS_GENERATION]
    assert rotations[0].previous_source_generation_id == "1" * 64
    assert rotations[0].abandoned_sequences == 0
    assert rotations[0].event == "source_generation_rotated:strategy/growth"


def test_a_foreign_source_generation_is_still_a_conflict(tmp_path: Path) -> None:
    previous_spec = _spec(producer_commit=PREVIOUS_COMMIT).spec_fingerprint
    foreign_spec = _spec(producer_commit=FOREIGN_COMMIT).spec_fingerprint
    bus = _bus(
        tmp_path,
        previous_generation_of_strategy_spec={previous_spec: PREVIOUS_GENERATION},
    )
    bus.bind_route_source(
        _descriptor(generation="1" * 64, spec_fingerprint=foreign_spec),
        routing_policy_fingerprint=ROUTING_POLICY,
        observed_at=NOW,
    )

    with pytest.raises(SignalRouteConflictError, match="generation changed"):
        bus.bind_route_source(
            _descriptor(generation="2" * 64, spec_fingerprint=previous_spec),
            routing_policy_fingerprint=ROUTING_POLICY,
            observed_at=NOW,
        )


def test_a_generation_that_changes_with_the_same_strategy_spec_is_still_a_conflict(
    tmp_path: Path,
) -> None:
    """A runner database recreated without a release is not a generation handover."""

    previous_spec = _spec(producer_commit=PREVIOUS_COMMIT).spec_fingerprint
    bus = _bus(
        tmp_path,
        previous_generation_of_strategy_spec={previous_spec: PREVIOUS_GENERATION},
    )
    bus.bind_route_source(
        _descriptor(generation="1" * 64, spec_fingerprint=previous_spec),
        routing_policy_fingerprint=ROUTING_POLICY,
        observed_at=NOW,
    )

    with pytest.raises(SignalRouteConflictError, match="generation changed"):
        bus.bind_route_source(
            _descriptor(generation="2" * 64, spec_fingerprint=previous_spec),
            routing_policy_fingerprint=ROUTING_POLICY,
            observed_at=NOW,
        )


def test_a_routing_policy_change_alongside_it_is_still_a_conflict(tmp_path: Path) -> None:
    """A handover carries the generation across; it does not carry a new policy across."""

    previous_spec = _spec(producer_commit=PREVIOUS_COMMIT).spec_fingerprint
    current_spec = _spec(producer_commit=CURRENT_COMMIT).spec_fingerprint
    bus = _bus(
        tmp_path,
        previous_generation_of_strategy_spec={previous_spec: PREVIOUS_GENERATION},
    )
    bus.bind_route_source(
        _descriptor(generation="1" * 64, spec_fingerprint=previous_spec),
        routing_policy_fingerprint=ROUTING_POLICY,
        observed_at=NOW,
    )

    with pytest.raises(SignalRouteConflictError, match="changed"):
        bus.bind_route_source(
            _descriptor(generation="2" * 64, spec_fingerprint=current_spec),
            routing_policy_fingerprint="8" * 64,
            observed_at=NOW,
        )
    assert bus.route_source_rotations("strategy/growth") == ()


def test_without_a_lineage_the_conflict_is_unchanged(tmp_path: Path) -> None:
    previous_spec = _spec(producer_commit=PREVIOUS_COMMIT).spec_fingerprint
    current_spec = _spec(producer_commit=CURRENT_COMMIT).spec_fingerprint
    bus = _bus(tmp_path)
    bus.bind_route_source(
        _descriptor(generation="1" * 64, spec_fingerprint=previous_spec),
        routing_policy_fingerprint=ROUTING_POLICY,
        observed_at=NOW,
    )

    with pytest.raises(SignalRouteConflictError, match="generation changed"):
        bus.bind_route_source(
            _descriptor(generation="2" * 64, spec_fingerprint=current_spec),
            routing_policy_fingerprint=ROUTING_POLICY,
            observed_at=NOW,
        )


def test_the_old_generations_receipts_are_archived_not_dropped(tmp_path: Path) -> None:
    """The conservative half of shape (2): issued receipts move, they never disappear."""

    previous_spec = _spec(producer_commit=PREVIOUS_COMMIT).spec_fingerprint
    current_spec = _spec(producer_commit=CURRENT_COMMIT).spec_fingerprint
    path = tmp_path / "signal_bus.sqlite3"
    bus = SignalBusStore(
        path,
        previous_generation_of_strategy_spec={previous_spec: PREVIOUS_GENERATION},
    )
    bus.bind_route_source(
        _descriptor(generation="1" * 64, spec_fingerprint=previous_spec, high_watermark=4),
        routing_policy_fingerprint=ROUTING_POLICY,
        observed_at=NOW,
    )
    #: two of the four sequences the old generation declared were actually routed; the
    #: receipts are written straight in, because building two real signal envelopes says
    #: nothing more about the rotation than their rows do
    scaffold = sqlite3.connect(path, isolation_level=None)
    try:
        scaffold.execute("PRAGMA foreign_keys = OFF")
        for sequence in (1, 2):
            scaffold.execute(
                """
                INSERT INTO signal_route_receipt(
                    source_id, source_sequence, signal_id, decision_fingerprint,
                    disposition, reason_code, target_manifest_hash, target_manifest_json,
                    routed_at
                ) VALUES (?, ?, ?, ?, 'routed', NULL, ?, '[]', ?)
                """,
                (
                    "strategy/growth",
                    sequence,
                    f"signal-{sequence}",
                    "d" * 64,
                    "e" * 64,
                    "2026-09-08T15:37:00Z",
                ),
            )
        scaffold.execute(
            "UPDATE signal_route_source SET last_sequence = 2 WHERE source_id = ?",
            ("strategy/growth",),
        )
    finally:
        scaffold.close()

    bus.bind_route_source(
        _descriptor(generation="2" * 64, spec_fingerprint=current_spec),
        routing_policy_fingerprint=ROUTING_POLICY,
        observed_at=NOW,
    )

    rotation = bus.route_source_rotations("strategy/growth")[0]
    assert rotation.routed_through_sequence == 2
    #: high watermark 4, routed through 2: two signals the old generation produced and
    #: nobody routed. They stay unrouted, on purpose, and the number says so.
    assert rotation.abandoned_sequences == 2

    reader = sqlite3.connect(path, isolation_level=None)
    reader.row_factory = sqlite3.Row
    try:
        archived = reader.execute(
            "SELECT source_sequence, signal_id FROM signal_route_receipt "
            "WHERE source_id = ? ORDER BY source_sequence",
            (rotation.archived_source_id,),
        ).fetchall()
        remaining = reader.execute(
            "SELECT COUNT(*) FROM signal_route_receipt WHERE source_id = ?",
            ("strategy/growth",),
        ).fetchone()[0]
        archived_source = reader.execute(
            "SELECT generation_id, last_sequence FROM signal_route_source WHERE source_id = ?",
            (rotation.archived_source_id,),
        ).fetchone()
    finally:
        reader.close()

    assert [(row["source_sequence"], row["signal_id"]) for row in archived] == [
        (1, "signal-1"),
        (2, "signal-2"),
    ]
    #: the new generation restarts at sequence 1 against an empty namespace
    assert remaining == 0
    assert bus.route_cursor("strategy/growth").last_sequence == 0
    assert archived_source["generation_id"] == "1" * 64
    assert archived_source["last_sequence"] == 2
