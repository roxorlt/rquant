"""Persistent notification deduplication and operations alert tests."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch


def test_gate_suppresses_same_event_across_instances(tmp_path: Path) -> None:
    from rquant.notify.gate import NotificationGate

    path = tmp_path / "notification-state.sqlite3"
    first = NotificationGate(path)
    second = NotificationGate(path)
    now = datetime(2026, 7, 16, 9, 30, tzinfo=UTC)

    lease = first.claim("service:rquant-monitor", 1_800, now=now)
    assert lease is not None
    first.complete(lease, 1_800, now=now + timedelta(seconds=10))
    assert (
        second.claim(
            "service:rquant-monitor",
            1_800,
            now=now + timedelta(minutes=1),
        )
        is None
    )
    assert (
        second.claim(
            "service:rquant-monitor",
            1_800,
            now=now + timedelta(minutes=31, seconds=10),
        )
        is not None
    )


def test_pending_lease_expires_quickly_after_sender_crash(tmp_path: Path) -> None:
    from rquant.notify.gate import NotificationGate

    gate = NotificationGate(tmp_path / "notification-state.sqlite3")
    now = datetime(2026, 7, 16, 9, 30, tzinfo=UTC)

    assert gate.claim("service:rquant-monitor", 1_800, now=now) is not None
    assert (
        gate.claim(
            "service:rquant-monitor",
            1_800,
            now=now + timedelta(seconds=30),
        )
        is None
    )
    assert (
        gate.claim(
            "service:rquant-monitor",
            1_800,
            now=now + timedelta(seconds=61),
        )
        is not None
    )


def test_gate_release_allows_retry_after_delivery_failure(tmp_path: Path) -> None:
    from rquant.notify.gate import NotificationGate

    gate = NotificationGate(tmp_path / "notification-state.sqlite3")
    now = datetime(2026, 7, 16, 9, 30, tzinfo=UTC)
    lease = gate.claim("service:rquant-monitor", 1_800, now=now)

    assert lease is not None
    gate.release(lease)
    assert gate.claim("service:rquant-monitor", 1_800, now=now) is not None


def test_old_lease_cannot_release_newer_claim(tmp_path: Path) -> None:
    from rquant.notify.gate import NotificationGate

    gate = NotificationGate(tmp_path / "notification-state.sqlite3")
    now = datetime(2026, 7, 16, 9, 30, tzinfo=UTC)
    old = gate.claim("service:rquant-monitor", 1_800, now=now)
    newer = gate.claim(
        "service:rquant-monitor",
        1_800,
        now=now + timedelta(seconds=61),
    )

    assert old is not None
    assert newer is not None
    gate.release(old)
    assert (
        gate.claim(
            "service:rquant-monitor",
            1_800,
            now=now + timedelta(seconds=62),
        )
        is None
    )


def test_clear_closes_incident_after_service_recovers(tmp_path: Path) -> None:
    from rquant.notify.gate import NotificationGate

    gate = NotificationGate(tmp_path / "notification-state.sqlite3")
    now = datetime(2026, 7, 16, 9, 30, tzinfo=UTC)
    lease = gate.claim("service:rquant-monitor", 1_800, now=now)
    assert lease is not None
    gate.complete(lease, 1_800, now=now)
    assert gate.claim("service:rquant-monitor", 1_800, now=now) is None

    gate.clear("service:rquant-monitor")
    assert gate.claim("service:rquant-monitor", 1_800, now=now) is not None


def test_file_gate_is_used_when_sqlite_is_unavailable(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from rquant.notify.gate import NotificationGate

    monkeypatch.setattr(
        "rquant.notify.gate.sqlite3.connect",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("sqlite unavailable")),
    )
    gate = NotificationGate(tmp_path / "notification-state.sqlite3")
    now = datetime(2026, 7, 16, 9, 30, tzinfo=UTC)
    lease = gate.claim("service:rquant-monitor", 1_800, now=now)

    assert lease is not None
    assert lease.backend == "file"
    gate.complete(lease, 1_800, now=now)
    assert gate.claim("service:rquant-monitor", 1_800, now=now) is None


def test_alert_command_persistently_suppresses_same_key(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from rquant import config
    from rquant.cli import cmd_alert

    monkeypatch.setattr(config.settings, "pushdeer_keys", "key-1")
    monkeypatch.setattr(config.settings, "pushplus_tokens", "")
    monkeypatch.setattr(config.settings, "notify_ops_cooldown_seconds", 1_800)
    monkeypatch.setattr(
        config.settings,
        "notification_state_path",
        tmp_path / "notification-state.sqlite3",
    )
    args = argparse.Namespace(
        subject="monitor failed",
        body="schema mismatch",
        dedup_key="service:rquant-monitor",
        cooldown_seconds=None,
        force=False,
    )

    with patch("rquant.notify.client.PushDeerClient") as client:
        client.return_value.push.return_value = [(True, None)]
        assert cmd_alert(args) == 0
        assert cmd_alert(args) == 0

    assert client.return_value.push.call_count == 1


def test_alert_command_releases_gate_when_every_channel_fails(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from rquant import config
    from rquant.cli import cmd_alert

    monkeypatch.setattr(config.settings, "pushdeer_keys", "key-1")
    monkeypatch.setattr(config.settings, "pushplus_tokens", "")
    monkeypatch.setattr(
        config.settings,
        "notification_state_path",
        tmp_path / "notification-state.sqlite3",
    )
    args = argparse.Namespace(
        subject="monitor failed",
        body="schema mismatch",
        dedup_key="service:rquant-monitor",
        cooldown_seconds=1_800,
        force=False,
    )

    with patch("rquant.notify.client.PushDeerClient") as client:
        client.return_value.push.return_value = [(False, "network down")]
        assert cmd_alert(args) == 1
        assert cmd_alert(args) == 1

    assert client.return_value.push.call_count == 2


def test_alert_command_fails_closed_when_every_gate_is_unavailable(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from rquant import config
    from rquant.cli import cmd_alert

    monkeypatch.setattr(config.settings, "pushdeer_keys", "key-1")
    monkeypatch.setattr(config.settings, "pushplus_tokens", "")
    monkeypatch.setattr(
        config.settings,
        "notification_state_path",
        tmp_path / "notification-state.sqlite3",
    )
    args = argparse.Namespace(
        subject="monitor failed",
        body="schema mismatch",
        dedup_key="service:rquant-monitor",
        cooldown_seconds=1_800,
        force=False,
    )

    with (
        patch(
            "rquant.notify.gate.NotificationGate.claim",
            side_effect=OSError("all gates unavailable"),
        ),
        patch("rquant.notify.client.PushDeerClient") as client,
    ):
        assert cmd_alert(args) == 1

    client.return_value.push.assert_not_called()


def test_alert_resolve_closes_recovered_service_incident(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from rquant import config
    from rquant.cli import cmd_alert_resolve
    from rquant.notify.gate import NotificationGate

    state_path = tmp_path / "notification-state.sqlite3"
    monkeypatch.setattr(config.settings, "notification_state_path", state_path)
    gate = NotificationGate(state_path)
    lease = gate.claim("service:rquant-monitor", 1_800)
    assert lease is not None
    gate.complete(lease, 1_800)

    assert cmd_alert_resolve(argparse.Namespace(dedup_key="service:rquant-monitor")) == 0
    assert gate.claim("service:rquant-monitor", 1_800) is not None
