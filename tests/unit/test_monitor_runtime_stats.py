from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from rquant import delivery_contracts as contracts
from rquant.notify.client import PushDeerClient, PushPlusClient

NOW = datetime(2026, 7, 31, 2, 0, tzinfo=UTC)


def _binding(channel: str = "pushdeer") -> Any:
    assert hasattr(contracts, "PhysicalPostBinding"), "real POST observation contract missing"
    return contracts.PhysicalPostBinding(
        group_id="a" * 64,
        owner_id="admin",
        target=contracts.DeliveryTarget(recipient_id="admin", channel=channel),
        members=({"outbox_id": "b" * 64, "attempt_no": 1},),
        request_sha256=contracts.canonical_sha256({"title": "提示", "body": "原事实"}),
        request_utf8_bytes=len("提示原事实".encode()),
        issued_at=NOW,
    )


@pytest.mark.parametrize("client,channel,code", [(PushDeerClient, "pushdeer", 0), (PushPlusClient, "pushplus", 200)])
def test_observation_counts_only_original_post_and_keeps_secret_out(
    monkeypatch: pytest.MonkeyPatch, client: Any, channel: str, code: int,
) -> None:
    binding = _binding(channel)
    observations: list[Any] = []
    calls: list[dict[str, Any]] = []

    class Reply:
        def json(self) -> dict[str, int]:
            return {"code": code}

    def post(endpoint: str, **kwargs: Any) -> Reply:
        calls.append({"endpoint": endpoint, **kwargs})
        return Reply()

    monkeypatch.setattr("rquant.notify.client.requests.post", post)
    values = iter((NOW, NOW + timedelta(seconds=1)))
    result = client(["private-credential"], "https://private.example/path").push(
        "提示", "原事实", observation_binding=binding,
        observation_sink=observations.append, observation_clock=lambda: next(values),
    )
    assert result == [(True, None)]
    assert len(calls) == len(observations) == 1
    observed = observations[0]
    assert observed.binding == binding
    assert observed.disposition == "accepted"
    assert observed.called_at == NOW
    assert observed.completed_at == NOW + timedelta(seconds=1)
    assert observed.key_slot == 0
    encoded = observed.model_dump_json()
    assert "private-credential" not in encoded
    assert "private.example" not in encoded
    assert "原事实" not in encoded


def test_rejected_and_lost_reply_do_not_become_mobile_delivery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    binding = _binding()
    observed: list[Any] = []

    def timeout(*args: Any, **kwargs: Any) -> None:
        raise TimeoutError("credential-must-not-enter-observation")

    monkeypatch.setattr("rquant.notify.client.requests.post", timeout)
    result = PushDeerClient(["private"], "https://private.example").push(
        "提示", "原事实", observation_binding=binding,
        observation_sink=observed.append, observation_clock=lambda: NOW,
    )
    assert result[0][0] is False
    assert len(observed) == 1
    assert observed[0].disposition == "unknown"
    assert observed[0].reason == "post_exception"
    assert "credential" not in observed[0].model_dump_json()


def test_no_keys_no_post_and_observer_failure_preserves_legacy_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    binding = _binding()
    observed: list[Any] = []
    assert PushDeerClient([], "https://private.example").push(
        "提示", "原事实", observation_binding=binding, observation_sink=observed.append,
    ) == []
    assert observed == []

    class Reply:
        def json(self) -> dict[str, int]:
            return {"code": 0}

    monkeypatch.setattr("rquant.notify.client.requests.post", lambda *a, **k: Reply())

    def broken(_: Any) -> None:
        raise OSError("readback unknown")

    assert PushDeerClient(["private"], "https://private.example").push(
        "提示", "原事实", observation_binding=binding, observation_sink=broken,
    ) == [(True, None)]


def test_binding_rejects_unbound_multi_key_and_wrong_request_before_post(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    binding = _binding()
    called: list[object] = []
    monkeypatch.setattr("rquant.notify.client.requests.post", lambda *a, **k: called.append(a))
    with pytest.raises(ValueError, match="single recipient"):
        PushDeerClient(["one", "two"], "https://private.example").push(
            "提示", "原事实", observation_binding=binding, observation_sink=lambda _: None,
        )
    with pytest.raises(ValueError, match="request"):
        PushDeerClient(["one"], "https://private.example").push(
            "changed", "原事实", observation_binding=binding, observation_sink=lambda _: None,
        )
    assert called == []


def test_runtime_projection_uses_the_original_transaction_and_exact_post_counts(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    import json
    from tests.unit.test_notification_merge import _store, _route, _provider, NOW
    from rquant.delivery_contracts import DeliveryChannel
    from rquant.notification_worker import run_notification_batch

    store = _store(tmp_path / "notification.sqlite3")
    _route(store, "600001.SH")
    _route(store, "600002.SH")

    class Reply:
        def json(self) -> dict[str, int]:
            return {"code": 0}

    monkeypatch.setattr("rquant.notify.client.requests.post", lambda *a, **k: Reply())
    run_notification_batch(store, {DeliveryChannel.PUSHDEER: _provider()}, worker_id="worker", now=NOW,
        lease_for=timedelta(seconds=30), limit=100, clock=lambda: NOW)
    at = NOW + timedelta(seconds=30)
    run_notification_batch(store, {DeliveryChannel.PUSHDEER: _provider()}, worker_id="worker", now=at,
        lease_for=timedelta(seconds=30), limit=100, clock=lambda: at)
    monkeypatch.setattr(store, "merge_channel_stats", lambda: (_ for _ in ()).throw(AssertionError("second statistics read")))
    snapshot = store.serving_snapshot(observed_at=at, history_limit=10)
    tables = {row.table_name: row for row in snapshot.payload.projections}
    assert {"notification_runtime_state", "notification_runtime_delivery", "monitor_builtin_state", "monitor_builtin_event"} <= set(tables), "typed same-read runtime projections missing"
    rows = tables["notification_runtime_state"].rows
    channel = json.loads(next(row["body_json"] for row in rows if row["owner_id"] == "admin"))
    assert channel["logical_count"] == 2 and channel["physical_requests"] == 1
    assert channel["member_attempts"] == 2 and channel["accepted_count"] == 1
    assert channel.get("accepted_pct") == 100.0, "actual owner acceptance rate was not published"
    assert channel["covered_from"] == NOW.isoformat().replace("+00:00", "Z")
    assert channel["covered_through"] == at.isoformat().replace("+00:00", "Z")
    assert channel["complete"] is True and channel["physical_unknown_count"] == 0
    assert len(tables["notification_runtime_delivery"].rows) == 1
    assert len(tables["monitor_builtin_event"].rows) == 0


def test_runtime_stats_default_off_and_empty_observation_do_not_fabricate_zero(tmp_path: Any) -> None:
    import json
    from tests.unit.test_notification_merge import _store, NOW

    old = _store(tmp_path / "old.sqlite3", enabled=False).serving_snapshot(observed_at=NOW, history_limit=10)
    assert not any(row.table_name.startswith("notification_runtime") for row in old.payload.projections)
    enabled = _store(tmp_path / "current.sqlite3").serving_snapshot(observed_at=NOW, history_limit=10)
    state = next(row for row in enabled.payload.projections if row.table_name == "notification_runtime_state")
    header = json.loads(state.rows[0]["body_json"])
    assert header["covered_from"] is None and header["complete"] is False
    assert header["state"] == "unavailable"


def test_optional_runtime_tables_bind_their_full_rows_and_owner_cutoff(tmp_path: Any) -> None:
    import json
    from rquant import condition_alert_runtime_projection as projection
    from rquant.serving_read_models import ServingProjectionPayload
    from tests.unit.test_notification_merge import _route, _store

    store = _store(tmp_path / "notification.sqlite3")
    _route(store, "600001.SH")
    store.claim_due(worker_id="worker", now=NOW, lease_for=timedelta(seconds=30), limit=100)
    tables = {item.table_name: item for item in store.serving_snapshot(observed_at=NOW, history_limit=10).payload.projections}
    assert hasattr(projection, "validate_monitor_runtime_projections"), "optional runtime tables need exact typed row validation"
    validate = projection.validate_monitor_runtime_projections
    result = validate(tables)
    assert result.notification_window.covered_from == NOW
    assert len(result.groups) == 1
    assert result.builtin_window.history_count is None
    assert validate({}) is None
    missing = dict(tables)
    missing.pop("monitor_builtin_event")
    with pytest.raises(ValueError, match="complete"):
        validate(missing)
    for field, value in (("source_receipt_sha256", "e" * 64), ("inspected_at", (NOW + timedelta(seconds=1)).isoformat())):
        broken = dict(tables)
        row = dict(tables["notification_runtime_delivery"].rows[0])
        body = json.loads(row["body_json"])
        body[field] = value
        row["body_json"] = json.dumps(body)
        broken["notification_runtime_delivery"] = ServingProjectionPayload(table_name="notification_runtime_delivery", available_at=NOW, rows=(row,))
        with pytest.raises(ValueError, match="receipt|cutoff"):
            validate(broken)
    broken = dict(tables)
    row = dict(tables["notification_runtime_delivery"].rows[0])
    row["owner_id"] = "other"
    broken["notification_runtime_delivery"] = ServingProjectionPayload(table_name="notification_runtime_delivery", available_at=NOW, rows=(row,))
    with pytest.raises(ValueError, match="identity"):
        validate(broken)


def test_signal_read_payload_rejects_partial_optional_runtime_extension(tmp_path: Any) -> None:
    from rquant.runtime_serving_snapshot import SignalDeliveryReadPayload
    from tests.unit.test_notification_merge import _store

    original = _store(tmp_path / "notification.sqlite3").serving_snapshot(observed_at=NOW, history_limit=10).payload
    SignalDeliveryReadPayload.model_validate_json(original.model_dump_json())
    with pytest.raises(ValueError, match="complete"):
        SignalDeliveryReadPayload(projections=tuple(item for item in original.projections if item.table_name != "monitor_builtin_event"))


def test_claim_only_and_unattempted_release_do_not_count_member_sends(tmp_path: Path) -> None:
    import json

    from tests.unit.test_notification_merge import NOW, _route, _store

    store = _store(tmp_path / "notification.sqlite3")
    outbox_id = _route(store, "600001.SH")
    assert store.claim_due(
        worker_id="worker", now=NOW, lease_for=timedelta(seconds=30), limit=100
    ) == ()
    due = NOW + timedelta(seconds=30)
    (leased,) = store.claim_due(
        worker_id="worker", now=due, lease_for=timedelta(seconds=30), limit=100
    )
    assert leased.attempt_count == 1 and store.attempts(outbox_id) == ()
    before_send = store.merge_channel_stats()[0]
    assert (
        before_send.member_attempts
        == before_send.member_retries
        == before_send.physical_requests
        == 0
    )
    projections = {
        row.table_name: row
        for row in store.serving_snapshot(observed_at=due, history_limit=10).payload.projections
    }
    channel = json.loads(
        next(
            row["body_json"]
            for row in projections["notification_runtime_state"].rows
            if row["owner_id"] == "admin"
        )
    )
    assert channel["logical_count"] == 1 and channel["member_attempts"] == 0
    released = store.release_unattempted(
        outbox_id,
        worker_id="worker",
        attempt_no=leased.attempt_count,
        released_at=due,
        reason="original admission has not been consumed",
    )
    assert released.attempt_count == 0 and store.attempts(outbox_id) == ()
    after_release = store.merge_channel_stats()[0]
    assert (
        after_release.member_attempts
        == after_release.member_retries
        == after_release.physical_requests
        == 0
    )


def test_original_unknown_member_evidence_counts_without_inventing_post(tmp_path: Path) -> None:
    from tests.unit.test_notification_merge import NOW, _route, _store

    store = _store(tmp_path / "notification.sqlite3")
    outbox_id = _route(store, "600001.SH")
    store.claim_due(worker_id="worker", now=NOW, lease_for=timedelta(seconds=30), limit=100)
    due = NOW + timedelta(seconds=30)
    (leased,) = store.claim_due(
        worker_id="worker", now=due, lease_for=timedelta(seconds=30), limit=100
    )
    assert store.merge_channel_stats()[0].member_attempts == 0
    store.record_unknown_delivery(
        outbox_id,
        worker_id="worker",
        attempt_no=leased.attempt_count,
        observed_at=due,
        reason="original provider outcome is unknown",
        provider_receipt=None,
    )
    uncertain = store.merge_channel_stats()[0]
    assert uncertain.member_attempts == 1 and uncertain.physical_requests == 0
    assert store.attempts(outbox_id) == ()


def test_future_claim_is_still_rejected_by_original_same_read_cutoff(tmp_path: Path) -> None:
    from tests.unit.test_notification_merge import NOW, _route, _store

    store = _store(tmp_path / "notification.sqlite3")
    _route(store, "600001.SH")
    store.claim_due(worker_id="worker", now=NOW, lease_for=timedelta(seconds=30), limit=100)
    store.claim_due(
        worker_id="worker", now=NOW + timedelta(seconds=30),
        lease_for=timedelta(seconds=30), limit=100,
    )
    with pytest.raises(ValueError, match="claim is not yet visible"):
        store.serving_snapshot(observed_at=NOW, history_limit=10)
