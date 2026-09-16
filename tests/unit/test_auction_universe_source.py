from __future__ import annotations

import os
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pytest

from rquant.auction_universe_authority import load_auction_universe_authority
from rquant.auction_universe_source import (
    AuctionUniverseSourceError,
    publish_auction_universe_from_daily_snapshot,
)
from rquant.readside_replica_gate import ReplicaReadGate
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.runtime_read_interrupt import (
    ReadInterruptedError,
    is_read_interrupt,
    request_read_interrupt,
    reset_read_interrupts,
)

COMMIT = "a" * 40


def _calendar() -> MarketCalendarAuthority:
    return MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit=COMMIT,
        coverage_start=date(2026, 7, 30),
        coverage_end=date(2026, 8, 4),
        open_dates=(date(2026, 7, 30), date(2026, 7, 31), date(2026, 8, 3)),
        generated_at=datetime(2026, 7, 29, tzinfo=UTC),
    )


def _database(path: Path, *, include_reference: bool = True) -> Path:
    with duckdb.connect(str(path)) as connection:
        connection.execute(
            """
            CREATE TABLE daily_bar(ts_code VARCHAR NOT NULL, trade_date DATE NOT NULL);
            INSERT INTO daily_bar VALUES
                ('600001.SH', DATE '2026-07-30'),
                ('000001.SZ', DATE '2026-07-31'),
                ('600000.SH', DATE '2026-07-31'),
                ('688001.SH', DATE '2026-08-03');
            """
        )
        if not include_reference:
            connection.execute("DELETE FROM daily_bar WHERE trade_date = DATE '2026-07-31'")
    path.chmod(0o600)
    return path


def test_source_publishes_next_open_date_from_exact_prior_daily_snapshot(
    tmp_path: Path,
) -> None:
    database = _database((tmp_path / "operational-ro.duckdb").resolve())
    root = (tmp_path / "authority").resolve()
    observed_at = datetime(2026, 7, 31, 10, 30, tzinfo=UTC)  # 18:30 Shanghai

    receipt = publish_auction_universe_from_daily_snapshot(
        database_path=database,
        authority_root=root,
        calendar=_calendar(),
        observed_at=observed_at,
        producer_commit=COMMIT,
    )

    authority = load_auction_universe_authority(
        root / "current.json",
        expected_commit=COMMIT,
        required_trade_date=date(2026, 8, 3),
        as_of=observed_at,
    )
    assert receipt.published is True
    assert receipt.code_count == 2
    assert authority.reference_trade_date == date(2026, 7, 31)
    assert authority.codes == ("000001.SZ", "600000.SH")
    assert authority.source_snapshot_id == receipt.source_snapshot_id
    assert len(receipt.source_snapshot_id) == 64


def test_source_is_idempotent_and_never_reads_future_daily_rows(tmp_path: Path) -> None:
    database = _database((tmp_path / "operational-ro.duckdb").resolve())
    root = (tmp_path / "authority").resolve()
    observed_at = datetime(2026, 7, 31, 10, 30, tzinfo=UTC)

    first = publish_auction_universe_from_daily_snapshot(
        database_path=database,
        authority_root=root,
        calendar=_calendar(),
        observed_at=observed_at,
        producer_commit=COMMIT,
    )
    second = publish_auction_universe_from_daily_snapshot(
        database_path=database,
        authority_root=root,
        calendar=_calendar(),
        observed_at=observed_at,
        producer_commit=COMMIT,
    )

    assert first.source_snapshot_id == second.source_snapshot_id
    assert second.published is False


def test_source_fails_closed_on_stale_reference_or_protected_session(tmp_path: Path) -> None:
    stale = _database(
        (tmp_path / "stale.duckdb").resolve(),
        include_reference=False,
    )
    root = (tmp_path / "authority").resolve()
    with pytest.raises(AuctionUniverseSourceError, match="exact prior open date"):
        publish_auction_universe_from_daily_snapshot(
            database_path=stale,
            authority_root=root,
            calendar=_calendar(),
            observed_at=datetime(2026, 7, 31, 10, 30, tzinfo=UTC),
            producer_commit=COMMIT,
        )

    current = _database((tmp_path / "current.duckdb").resolve())
    with pytest.raises(AuctionUniverseSourceError, match="protection window"):
        publish_auction_universe_from_daily_snapshot(
            database_path=current,
            authority_root=root,
            calendar=_calendar(),
            observed_at=datetime(2026, 8, 3, 2, 0, tzinfo=UTC),  # 10:00 Shanghai
            producer_commit=COMMIT,
        )


def test_source_rejects_unsafe_or_mutated_readonly_snapshot(tmp_path: Path) -> None:
    real = _database((tmp_path / "real.duckdb").resolve())
    linked = (tmp_path / "linked.duckdb").resolve()
    linked.symlink_to(real)

    with pytest.raises(AuctionUniverseSourceError, match="unsafe|symlink"):
        publish_auction_universe_from_daily_snapshot(
            database_path=linked,
            authority_root=(tmp_path / "authority").resolve(),
            calendar=_calendar(),
            observed_at=datetime(2026, 7, 31, 10, 30, tzinfo=UTC),
            producer_commit=COMMIT,
        )


def _count_connects(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """How many times the source actually opened the replica (#256)."""

    import duckdb

    opens = [0]
    original = duckdb.connect

    def counted(*args: object, **kwargs: object) -> object:
        opens[0] += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(duckdb, "connect", counted)
    return opens


def test_an_unchanged_replica_is_read_once_and_then_only_stat_ed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#256: this publisher runs every thirty seconds against a 10 GB file."""

    database = _database((tmp_path / "operational-ro.duckdb").resolve())
    root = (tmp_path / "authority").resolve()
    observed_at = datetime(2026, 7, 31, 10, 30, tzinfo=UTC)
    gate: ReplicaReadGate[tuple[str, ...]] = ReplicaReadGate(database)
    opens = _count_connects(monkeypatch)

    receipts = [
        publish_auction_universe_from_daily_snapshot(
            database_path=database,
            authority_root=root,
            calendar=_calendar(),
            observed_at=observed_at,
            producer_commit=COMMIT,
            read_gate=gate,
        )
        for _ in range(3)
    ]

    assert opens[0] == 1
    assert {receipt.source_snapshot_id for receipt in receipts} == {
        receipts[0].source_snapshot_id
    }
    assert gate.last_read is not None and gate.last_read.opened is False


def test_a_replaced_replica_is_read_once_more(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = _database((tmp_path / "operational-ro.duckdb").resolve())
    replacement = _database((tmp_path / "operational-ro.duckdb.tmp.1").resolve())
    root = (tmp_path / "authority").resolve()
    observed_at = datetime(2026, 7, 31, 10, 30, tzinfo=UTC)
    gate: ReplicaReadGate[tuple[str, ...]] = ReplicaReadGate(database)
    opens = _count_connects(monkeypatch)

    def publish() -> object:
        return publish_auction_universe_from_daily_snapshot(
            database_path=database,
            authority_root=root,
            calendar=_calendar(),
            observed_at=observed_at,
            producer_commit=COMMIT,
            read_gate=gate,
        )

    publish()
    publish()
    os.replace(replacement, database)
    publish()
    publish()

    assert opens[0] == 2


def test_a_stop_during_the_scan_is_reported_as_a_stop_not_a_snapshot_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#268: `InterruptException` is a `duckdb.Error`, and this path converts those.

    Without the re-raise, an operator's own `systemctl stop` came back through the loop as
    `daily snapshot query failed` -- a fault the role would have recorded, backed off on,
    and left in its last heartbeat before `stopped`.
    """

    import duckdb

    database = _database((tmp_path / "operational-ro.duckdb").resolve())
    root = (tmp_path / "authority").resolve()
    original = duckdb.connect

    class _Interrupting:
        def __init__(self, inner: object) -> None:
            self._inner = inner

        def interrupt(self) -> None:
            self._inner.interrupt()  # type: ignore[attr-defined]

        def execute(self, *_args: object, **_kwargs: object) -> object:
            raise duckdb.InterruptException("INTERRUPT Error: Interrupted!")

        def close(self) -> None:
            self._inner.close()  # type: ignore[attr-defined]

    monkeypatch.setattr(duckdb, "connect", lambda *a, **k: _Interrupting(original(*a, **k)))

    with pytest.raises(duckdb.InterruptException) as raised:
        publish_auction_universe_from_daily_snapshot(
            database_path=database,
            authority_root=root,
            calendar=_calendar(),
            observed_at=datetime(2026, 7, 31, 10, 30, tzinfo=UTC),
            producer_commit=COMMIT,
        )

    assert is_read_interrupt(raised.value)
    assert not isinstance(raised.value, AuctionUniverseSourceError)


def test_a_read_may_not_begin_once_this_process_has_been_asked_to_stop(
    tmp_path: Path,
) -> None:
    """An idle `interrupt()` does not arm the next query, so the read must not start."""

    database = _database((tmp_path / "operational-ro.duckdb").resolve())
    root = (tmp_path / "authority").resolve()
    reset_read_interrupts()
    request_read_interrupt()
    try:
        with pytest.raises(ReadInterruptedError):
            publish_auction_universe_from_daily_snapshot(
                database_path=database,
                authority_root=root,
                calendar=_calendar(),
                observed_at=datetime(2026, 7, 31, 10, 30, tzinfo=UTC),
                producer_commit=COMMIT,
            )
    finally:
        reset_read_interrupts()
