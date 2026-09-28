"""Trusted, bounded paper order history from a single SQLite snapshot."""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

import rquant.paper_broker as broker_module
from rquant.paper_broker import PaperBrokerReconciliationError
from rquant.paper_contracts import PaperOrderStatus, PaperOrderType, PaperRejectReason
from rquant.runtime_contracts import canonical_sha256
from tests.paper_cost_fixtures import paper_cost_policy
from tests.unit.test_paper_broker import (
    BUY_DATE,
    BUY_TIME,
    _intent,
    _quote,
    _store,
)


def test_empty_history_is_a_trusted_window(tmp_path: Path) -> None:
    store = _store(tmp_path / "paper.sqlite3", paper_cost_policy())

    snapshot = store.recent_order_history(as_of=BUY_TIME)

    assert snapshot.account_id == store.account_id
    assert snapshot.as_of == BUY_TIME
    assert snapshot.ledger_revision >= 1
    assert snapshot.total_orders == 0
    assert snapshot.has_more is False
    assert snapshot.orders == ()
    assert snapshot.fills == ()

    store.submit_intent(
        _intent(order_type=PaperOrderType.LIMIT, limit_price=Decimal("9.90")),
        decision_time=BUY_TIME + timedelta(minutes=1),
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )
    changed = store.recent_order_history(as_of=BUY_TIME + timedelta(minutes=1))
    assert changed.ledger_revision > snapshot.ledger_revision


def test_window_keeps_all_fills_and_rejects_without_inventing_trades(tmp_path: Path) -> None:
    store = _store(tmp_path / "paper.sqlite3", paper_cost_policy())
    partial = store.submit_intent(
        _intent(quantity=1_000),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00", executable_quantity=400),
    )
    store.apply_execution(
        partial.order_id,
        execution_id="9" * 64,
        executed_at=BUY_TIME + timedelta(minutes=1),
        trade_date=BUY_DATE,
        quantity=600,
        quote=_quote("11.00", executable_quantity=600),
        price_snapshot_id="9" * 64,
    )
    rejected = store.submit_intent(
        _intent(signal_seed="d", event_time=BUY_TIME + timedelta(minutes=2)),
        decision_time=BUY_TIME + timedelta(minutes=2, seconds=2),
        trade_date=BUY_DATE,
        quote=_quote("10.00", suspended=True),
    )

    snapshot = store.recent_order_history(as_of=BUY_TIME + timedelta(minutes=3))

    assert snapshot.total_orders == 2
    assert [row.order_id for row in snapshot.orders] == [rejected.order_id, partial.order_id]
    assert snapshot.orders[0].status is PaperOrderStatus.REJECTED
    assert snapshot.orders[0].reject_reason is PaperRejectReason.SUSPENDED
    assert snapshot.orders[1].average_fill_price == Decimal("10.6000")
    assert [(fill.order_id, fill.sequence) for fill in snapshot.fills] == [
        (partial.order_id, 1),
        (partial.order_id, 2),
    ]
    assert [fill.persisted_at for fill in snapshot.fills] == [
        BUY_TIME,
        BUY_TIME + timedelta(minutes=1),
    ]


def test_recent_window_reports_true_total_and_truncation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path / "paper.sqlite3", paper_cost_policy())
    monkeypatch.setattr(broker_module, "_MAX_HISTORY_ORDERS", 2)
    orders = [
        store.submit_intent(
            _intent(
                signal_seed=seed,
                order_type=PaperOrderType.LIMIT,
                limit_price=Decimal("9.90"),
                event_time=BUY_TIME + timedelta(minutes=index),
            ),
            decision_time=BUY_TIME + timedelta(minutes=index, seconds=2),
            trade_date=BUY_DATE,
            quote=_quote("10.00"),
        )
        for index, seed in enumerate("abc")
    ]

    snapshot = store.recent_order_history(as_of=BUY_TIME + timedelta(minutes=3))

    assert snapshot.total_orders == 3
    assert snapshot.has_more is True
    assert [row.order_id for row in snapshot.orders] == [orders[2].order_id, orders[1].order_id]
    assert snapshot.fills == ()


def test_overfull_fill_window_fails_instead_of_publishing_partial_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path / "paper.sqlite3", paper_cost_policy())
    partial = store.submit_intent(
        _intent(quantity=1_000),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00", executable_quantity=400),
    )
    store.apply_execution(
        partial.order_id,
        execution_id="9" * 64,
        executed_at=BUY_TIME + timedelta(minutes=1),
        trade_date=BUY_DATE,
        quantity=600,
        quote=_quote("11.00", executable_quantity=600),
        price_snapshot_id="9" * 64,
    )
    monkeypatch.setattr(broker_module, "_MAX_HISTORY_FILLS", 1)

    with pytest.raises(PaperBrokerReconciliationError, match="fill.*capacity"):
        store.recent_order_history(as_of=BUY_TIME + timedelta(minutes=2))


def test_tampered_order_is_not_read_as_valid_history(tmp_path: Path) -> None:
    path = tmp_path / "paper.sqlite3"
    store = _store(path, paper_cost_policy())
    store.submit_intent(
        _intent(), decision_time=BUY_TIME, trade_date=BUY_DATE, quote=_quote("10.00")
    )
    with sqlite3.connect(path) as connection:
        connection.execute("UPDATE paper_order SET status = 'PENDING'")

    with pytest.raises(PaperBrokerReconciliationError):
        store.recent_order_history(as_of=BUY_TIME + timedelta(minutes=1))


def test_order_identity_must_match_immutable_intent_and_receipt(tmp_path: Path) -> None:
    path = tmp_path / "paper.sqlite3"
    store = _store(path, paper_cost_policy())
    order = store.submit_intent(
        _intent(), decision_time=BUY_TIME, trade_date=BUY_DATE, quote=_quote("10.00")
    )
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE paper_order SET ts_code = '600999.SH' WHERE order_id = ?",
            (order.order_id,),
        )

    with pytest.raises(PaperBrokerReconciliationError, match="intent/order mismatch"):
        store.recent_order_history(as_of=BUY_TIME + timedelta(minutes=1))


def test_order_creation_time_must_match_initial_receipt(tmp_path: Path) -> None:
    path = tmp_path / "paper.sqlite3"
    store = _store(path, paper_cost_policy())
    order = store.submit_intent(
        _intent(), decision_time=BUY_TIME, trade_date=BUY_DATE, quote=_quote("10.00")
    )
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE paper_order SET created_at = ? WHERE order_id = ?",
            ((BUY_TIME - timedelta(minutes=1)).isoformat(), order.order_id),
        )

    with pytest.raises(PaperBrokerReconciliationError, match="initial execution receipt mismatch"):
        store.recent_order_history(as_of=BUY_TIME + timedelta(minutes=1))


def test_rejected_order_reason_must_match_immutable_receipt(tmp_path: Path) -> None:
    path = tmp_path / "paper.sqlite3"
    store = _store(path, paper_cost_policy())
    rejected = store.submit_intent(
        _intent(),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00", suspended=True),
    )
    assert rejected.reject_reason is PaperRejectReason.SUSPENDED
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE paper_order SET reject_reason = ? WHERE order_id = ?",
            (PaperRejectReason.RISK_REJECTED.value, rejected.order_id),
        )

    with pytest.raises(PaperBrokerReconciliationError, match="final execution receipt mismatch"):
        store.recent_order_history(as_of=BUY_TIME + timedelta(minutes=1))


@pytest.mark.parametrize(
    ("status", "minutes"),
    [(PaperOrderStatus.CANCELLED, 1), (PaperOrderStatus.EXPIRED, 6)],
)
def test_new_closed_order_has_replayable_evidence(
    tmp_path: Path, status: PaperOrderStatus, minutes: int
) -> None:
    path = tmp_path / "paper.sqlite3"
    store = _store(path, paper_cost_policy())
    accepted = store.submit_intent(
        _intent(
            quantity=1_000,
            order_type=PaperOrderType.LIMIT,
            limit_price=Decimal("9.90"),
        ),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote(
            "10.00" if status is PaperOrderStatus.CANCELLED else "9.80",
            executable_quantity=400,
        ),
    )
    closed = store.close_open_order(
        accepted.order_id,
        status=status,
        decided_at=BUY_TIME + timedelta(minutes=minutes),
    )

    snapshot = store.recent_order_history(as_of=datetime.now(UTC) + timedelta(minutes=1))
    assert snapshot.orders == (closed,)
    assert len(snapshot.fills) == (0 if status is PaperOrderStatus.CANCELLED else 1)
    with sqlite3.connect(path) as connection:
        assert (
            connection.execute(
                "SELECT count(*) FROM paper_ledger_attestation WHERE event_kind = 'order_close_v2'"
            ).fetchone()[0]
            == 1
        )


def test_close_writer_rejects_order_without_matching_last_receipt(tmp_path: Path) -> None:
    path = tmp_path / "paper.sqlite3"
    store = _store(path, paper_cost_policy())
    accepted = store.submit_intent(
        _intent(order_type=PaperOrderType.LIMIT, limit_price=Decimal("9.90")),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE paper_order SET updated_at = ? WHERE order_id = ?",
            ((accepted.updated_at + timedelta(seconds=5)).isoformat(), accepted.order_id),
        )

    with pytest.raises(PaperBrokerReconciliationError, match="last execution receipt"):
        store.close_open_order(
            accepted.order_id,
            status=PaperOrderStatus.CANCELLED,
            decided_at=BUY_TIME + timedelta(minutes=1),
        )
    with sqlite3.connect(path) as connection:
        assert (
            connection.execute(
                "SELECT count(*) FROM paper_ledger_attestation WHERE event_kind = 'order_close_v2'"
            ).fetchone()[0]
            == 0
        )
        assert (
            connection.execute(
                "SELECT status FROM paper_order WHERE order_id = ?", (accepted.order_id,)
            ).fetchone()[0]
            == PaperOrderStatus.ACCEPTED.value
        )


def test_partial_close_writer_rejects_mismatched_last_fill_receipt(tmp_path: Path) -> None:
    path = tmp_path / "paper.sqlite3"
    store = _store(path, paper_cost_policy())
    partial = store.submit_intent(
        _intent(quantity=1_000),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00", executable_quantity=400),
    )
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE paper_order SET average_fill_price = '10.1234' WHERE order_id = ?",
            (partial.order_id,),
        )

    with pytest.raises(PaperBrokerReconciliationError, match="last execution receipt"):
        store.close_open_order(
            partial.order_id,
            status=PaperOrderStatus.EXPIRED,
            decided_at=BUY_TIME + timedelta(minutes=6),
        )
    with sqlite3.connect(path) as connection:
        assert (
            connection.execute(
                "SELECT count(*) FROM paper_ledger_attestation WHERE event_kind = 'order_close_v2'"
            ).fetchone()[0]
            == 0
        )


def test_close_cannot_claim_future_persistence_before_attestation(tmp_path: Path) -> None:
    path = tmp_path / "paper.sqlite3"
    store = _store(path, paper_cost_policy())
    accepted = store.submit_intent(
        _intent(order_type=PaperOrderType.LIMIT, limit_price=Decimal("9.90")),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )
    future = datetime.now(UTC) + timedelta(days=1)

    with pytest.raises(ValueError, match="future persistence"):
        store.close_open_order(
            accepted.order_id,
            status=PaperOrderStatus.CANCELLED,
            decided_at=BUY_TIME + timedelta(minutes=1),
            persisted_at=future,
        )
    with sqlite3.connect(path) as connection:
        assert (
            connection.execute(
                "SELECT status FROM paper_order WHERE order_id = ?", (accepted.order_id,)
            ).fetchone()[0]
            == PaperOrderStatus.ACCEPTED.value
        )
        assert (
            connection.execute(
                "SELECT count(*) FROM paper_ledger_attestation WHERE event_kind = 'order_close_v2'"
            ).fetchone()[0]
            == 0
        )


def test_close_event_cannot_be_reused_for_another_order(tmp_path: Path) -> None:
    path = tmp_path / "paper.sqlite3"
    store = _store(path, paper_cost_policy())
    first = store.submit_intent(
        _intent(order_type=PaperOrderType.LIMIT, limit_price=Decimal("9.90")),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )
    second = store.submit_intent(
        _intent(
            signal_seed="b",
            event_time=BUY_TIME + timedelta(seconds=10),
            order_type=PaperOrderType.LIMIT,
            limit_price=Decimal("9.90"),
        ),
        decision_time=BUY_TIME + timedelta(seconds=12),
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )
    store.close_open_order(
        first.order_id,
        status=PaperOrderStatus.CANCELLED,
        decided_at=BUY_TIME + timedelta(minutes=1),
    )
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE paper_order SET status = 'CANCELLED' WHERE order_id = ?",
            (second.order_id,),
        )

    with pytest.raises(PaperBrokerReconciliationError, match="unverifiable close status"):
        store.recent_order_history(as_of=datetime.now(UTC) + timedelta(minutes=1))


def test_closed_order_time_change_is_not_covered_by_close_event(tmp_path: Path) -> None:
    path = tmp_path / "paper.sqlite3"
    store = _store(path, paper_cost_policy())
    accepted = store.submit_intent(
        _intent(order_type=PaperOrderType.LIMIT, limit_price=Decimal("9.90")),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )
    closed = store.close_open_order(
        accepted.order_id,
        status=PaperOrderStatus.CANCELLED,
        decided_at=BUY_TIME + timedelta(minutes=1),
    )
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE paper_order SET updated_at = ? WHERE order_id = ?",
            ((closed.updated_at + timedelta(seconds=1)).isoformat(), closed.order_id),
        )

    with pytest.raises(PaperBrokerReconciliationError, match="unverifiable close status"):
        store.recent_order_history(as_of=datetime.now(UTC) + timedelta(minutes=1))


def test_duplicate_close_event_is_not_accepted(tmp_path: Path) -> None:
    path = tmp_path / "paper.sqlite3"
    store = _store(path, paper_cost_policy())
    accepted = store.submit_intent(
        _intent(order_type=PaperOrderType.LIMIT, limit_price=Decimal("9.90")),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )
    store.close_open_order(
        accepted.order_id,
        status=PaperOrderStatus.CANCELLED,
        decided_at=BUY_TIME + timedelta(minutes=1),
    )
    with store._connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        fingerprint = connection.execute(
            "SELECT event_fingerprint FROM paper_ledger_attestation "
            "WHERE event_kind = 'order_close_v2'"
        ).fetchone()[0]
        store._append_ledger_attestation(
            connection,
            event_kind="order_close_v2",
            event_fingerprint=fingerprint,
            count_deltas={},
        )
        connection.commit()

    with pytest.raises(PaperBrokerReconciliationError, match="unique|duplicate"):
        store.recent_order_history(as_of=datetime.now(UTC) + timedelta(minutes=1))


def test_close_attestation_scan_capacity_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path / "paper.sqlite3", paper_cost_policy())
    accepted = store.submit_intent(
        _intent(order_type=PaperOrderType.LIMIT, limit_price=Decimal("9.90")),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )
    store.close_open_order(
        accepted.order_id,
        status=PaperOrderStatus.CANCELLED,
        decided_at=BUY_TIME + timedelta(minutes=1),
    )
    monkeypatch.setattr(broker_module, "_MAX_HISTORY_CLOSE_ATTESTATION_STEPS", 0)
    monkeypatch.setattr(broker_module, "_CLOSE_ATTESTATION_PROGRESS_INTERVAL", 1)

    with pytest.raises(PaperBrokerReconciliationError, match="scan capacity exceeded"):
        store.recent_order_history(as_of=datetime.now(UTC) + timedelta(minutes=1))


def test_close_event_after_cutoff_is_not_visible(tmp_path: Path) -> None:
    store = _store(tmp_path / "paper.sqlite3", paper_cost_policy())
    accepted = store.submit_intent(
        _intent(order_type=PaperOrderType.LIMIT, limit_price=Decimal("9.90")),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )
    store.close_open_order(
        accepted.order_id,
        status=PaperOrderStatus.CANCELLED,
        decided_at=BUY_TIME + timedelta(minutes=1),
    )

    with pytest.raises(PaperBrokerReconciliationError, match="unverifiable close status"):
        store.recent_order_history(as_of=BUY_TIME + timedelta(minutes=2))


def test_old_close_event_does_not_upgrade_without_replayable_evidence(tmp_path: Path) -> None:
    path = tmp_path / "paper.sqlite3"
    store = _store(path, paper_cost_policy())
    accepted = store.submit_intent(
        _intent(order_type=PaperOrderType.LIMIT, limit_price=Decimal("9.90")),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )
    decided_at = BUY_TIME + timedelta(minutes=1)
    with store._connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            "UPDATE paper_order SET status = 'CANCELLED', updated_at = ? WHERE order_id = ?",
            (decided_at.isoformat(), accepted.order_id),
        )
        store._append_ledger_attestation(
            connection,
            event_kind="order_close",
            event_fingerprint=canonical_sha256(
                {
                    "order_id": accepted.order_id,
                    "status": PaperOrderStatus.CANCELLED.value,
                    "decided_at": decided_at,
                    "persisted_at": decided_at,
                }
            ),
            count_deltas={},
        )
        connection.commit()

    with pytest.raises(PaperBrokerReconciliationError, match="unverifiable close status"):
        store.recent_order_history(as_of=datetime.now(UTC) + timedelta(minutes=1))


def test_forged_close_raises_same_fail_closed_error(tmp_path: Path) -> None:
    path = tmp_path / "paper.sqlite3"
    store = _store(path, paper_cost_policy())
    accepted = store.submit_intent(
        _intent(order_type=PaperOrderType.LIMIT, limit_price=Decimal("9.90")),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE paper_order SET status = ? WHERE order_id = ?",
            (PaperOrderStatus.CANCELLED.value, accepted.order_id),
        )

    with pytest.raises(PaperBrokerReconciliationError, match="unverifiable close status") as error:
        store.recent_order_history(as_of=BUY_TIME + timedelta(minutes=1))
    assert type(error.value) is PaperBrokerReconciliationError


def test_writer_commit_during_history_read_does_not_split_count_and_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "paper.sqlite3"
    store = _store(path, paper_cost_policy())
    writer = _store(path, paper_cost_policy())
    original_connect = store._connect
    committed = False

    def interleaved_connect() -> sqlite3.Connection:
        nonlocal committed
        connection = original_connect()

        def on_query(statement: str) -> None:
            nonlocal committed
            if committed or not statement.startswith("SELECT count(*) FROM paper_order"):
                return
            committed = True
            writer.submit_intent(
                _intent(
                    order_type=PaperOrderType.LIMIT,
                    limit_price=Decimal("9.90"),
                    event_time=BUY_TIME + timedelta(minutes=1),
                ),
                decision_time=BUY_TIME + timedelta(minutes=1, seconds=2),
                trade_date=BUY_DATE,
                quote=_quote("10.00"),
            )

        connection.set_trace_callback(on_query)
        return connection

    monkeypatch.setattr(store, "_connect", interleaved_connect)
    cutoff = BUY_TIME + timedelta(minutes=2)

    first = store.recent_order_history(as_of=cutoff)
    after = writer.recent_order_history(as_of=cutoff)

    assert committed is True
    assert first.total_orders == 0
    assert first.orders == ()
    assert after.total_orders == 1
