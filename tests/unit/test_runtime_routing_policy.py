from __future__ import annotations

import hashlib
import json
import os
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from rquant.delivery_contracts import DeliveryChannel, DeliveryTarget
from rquant.runtime_routing_policy import (
    FrozenRoutingPolicyResolver,
    RoutingPolicyConflictError,
    RoutingPolicyIntegrityError,
    load_frozen_routing_policy,
)
from rquant.signal_contracts import SignalAction, SignalEnvelope
from rquant.signal_router_runtime import RoutingDecisionAction

NOW = datetime(2026, 7, 31, 1, 0, tzinfo=UTC)


def _observed_at() -> datetime:
    return datetime.now(UTC)


def _policy_payload() -> dict[str, object]:
    return {
        "default_no_target_reason": "routing_policy_no_target",
        "rules": [
            {
                "strategy_id": "n_shape",
                "strategy_version": "1",
                "action": "b_intent",
                "recipient_id": "admin",
                "channel": "pushdeer",
                "enabled": True,
            }
        ],
    }


def _freeze_policy(path: Path, payload: object) -> bytes:
    content = json.dumps(payload, sort_keys=True).encode()
    path.write_bytes(content)
    path.chmod(0o444)
    return content


def _write_policy(path: Path) -> bytes:
    return _freeze_policy(path, _policy_payload())


def _signal(
    *,
    strategy_id: str = "n_shape",
    strategy_version: str = "1",
    action: SignalAction = SignalAction.B_INTENT,
) -> SignalEnvelope:
    return SignalEnvelope(
        schema_version=1,
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        parameter_fingerprint="a" * 64,
        dataset_snapshot_id="b" * 64,
        feature_snapshot_id="c" * 64,
        event_time=NOW,
        available_at=NOW,
        candidate_id="000001.SZ",
        action=action,
        reason_codes=("strong_support",),
        expires_at=NOW + timedelta(minutes=5),
        producer_commit="d" * 40,
    )


def test_loads_frozen_policy_as_target_resolver(tmp_path: Path) -> None:
    path = tmp_path / "routing-policy.json"
    content = _write_policy(path)
    expected = hashlib.sha256(content).hexdigest()

    resolver = load_frozen_routing_policy(
        path,
        routing_policy_fingerprint=expected,
        observed_at=_observed_at(),
    )
    decision = resolver(_signal())

    assert resolver.routing_policy_fingerprint == expected
    assert decision.routing_policy_fingerprint == expected
    assert decision.action is RoutingDecisionAction.ROUTE
    assert decision.targets == (
        DeliveryTarget(
            recipient_id="admin",
            channel=DeliveryChannel.PUSHDEER,
        ),
    )


def test_unknown_strategy_version_or_action_fail_closed(tmp_path: Path) -> None:
    path = tmp_path / "routing-policy.json"
    content = _write_policy(path)
    expected = hashlib.sha256(content).hexdigest()
    resolver = load_frozen_routing_policy(path, observed_at=_observed_at())

    for signal in (
        _signal(strategy_id="unknown"),
        _signal(strategy_version="2"),
        _signal(action=SignalAction.S_INTENT),
    ):
        decision = resolver(signal)
        assert decision.action is RoutingDecisionAction.NO_TARGET
        assert decision.reason_code == "routing_policy_no_target"
        assert decision.targets == ()
        assert decision.routing_policy_fingerprint == expected


def test_disabled_targets_are_ignored_and_enabled_targets_are_canonical(
    tmp_path: Path,
) -> None:
    path = tmp_path / "routing-policy.json"
    payload = _policy_payload()
    rules = payload["rules"]
    assert isinstance(rules, list)
    rules.extend(
        [
            {
                "strategy_id": "n_shape",
                "strategy_version": "1",
                "action": "b_intent",
                "recipient_id": "secondary",
                "channel": "pushplus",
                "enabled": True,
            },
            {
                "strategy_id": "n_shape",
                "strategy_version": "1",
                "action": "b_intent",
                "recipient_id": "disabled",
                "channel": "pushplus",
                "enabled": False,
            },
        ]
    )
    _freeze_policy(path, payload)

    decision = load_frozen_routing_policy(path, observed_at=_observed_at())(_signal())

    assert decision.targets == (
        DeliveryTarget(recipient_id="admin", channel=DeliveryChannel.PUSHDEER),
        DeliveryTarget(recipient_id="secondary", channel=DeliveryChannel.PUSHPLUS),
    )


@pytest.mark.parametrize("forbidden_key", ["token", "password", "import_path"])
def test_policy_rejects_credentials_and_dynamic_import_fields(
    tmp_path: Path,
    forbidden_key: str,
) -> None:
    path = tmp_path / "routing-policy.json"
    payload = _policy_payload()
    rules = payload["rules"]
    assert isinstance(rules, list)
    rule = rules[0]
    assert isinstance(rule, dict)
    rule[forbidden_key] = "forbidden"
    _freeze_policy(path, payload)

    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        load_frozen_routing_policy(path, observed_at=_observed_at())


def test_policy_rejects_unknown_document_fields(tmp_path: Path) -> None:
    path = tmp_path / "routing-policy.json"
    payload = _policy_payload()
    payload["provider_config"] = {"token": "forbidden"}
    _freeze_policy(path, payload)

    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        load_frozen_routing_policy(path, observed_at=_observed_at())


def test_explicit_routing_fingerprint_must_match_content(tmp_path: Path) -> None:
    path = tmp_path / "routing-policy.json"
    _write_policy(path)

    with pytest.raises(RoutingPolicyIntegrityError, match="fingerprint"):
        load_frozen_routing_policy(
            path,
            routing_policy_fingerprint="f" * 64,
            observed_at=_observed_at(),
        )


@pytest.mark.parametrize("second_enabled", [True, False])
def test_duplicate_or_conflicting_target_is_rejected(
    tmp_path: Path,
    second_enabled: bool,
) -> None:
    path = tmp_path / "routing-policy.json"
    payload = _policy_payload()
    rules = payload["rules"]
    assert isinstance(rules, list)
    duplicate = dict(rules[0])
    duplicate["enabled"] = second_enabled
    rules.append(duplicate)
    _freeze_policy(path, payload)

    with pytest.raises(RoutingPolicyConflictError, match="duplicate or conflicting"):
        load_frozen_routing_policy(path, observed_at=_observed_at())


def test_policy_requires_absolute_normal_json_path(tmp_path: Path) -> None:
    relative = Path("routing-policy.json")
    dotted = tmp_path / "nested" / ".." / "routing-policy.json"

    for path in (relative, dotted, tmp_path / "routing-policy.txt"):
        with pytest.raises(RoutingPolicyIntegrityError, match="absolute normal JSON"):
            load_frozen_routing_policy(path, observed_at=_observed_at())


def test_policy_rejects_symlinked_file_or_parent(tmp_path: Path) -> None:
    physical = tmp_path / "physical"
    physical.mkdir()
    policy = physical / "routing-policy.json"
    _write_policy(policy)

    linked_file = tmp_path / "linked-policy.json"
    linked_file.symlink_to(policy)
    linked_parent = tmp_path / "linked-parent"
    linked_parent.symlink_to(physical, target_is_directory=True)

    for path in (linked_file, linked_parent / policy.name):
        with pytest.raises(RoutingPolicyIntegrityError, match="symlink"):
            load_frozen_routing_policy(path, observed_at=_observed_at())


def test_policy_rejects_writable_or_hardlinked_file(tmp_path: Path) -> None:
    writable = tmp_path / "writable.json"
    _write_policy(writable)
    writable.chmod(0o644)

    with pytest.raises(RoutingPolicyIntegrityError, match="read-only"):
        load_frozen_routing_policy(writable, observed_at=_observed_at())

    physical = tmp_path / "physical.json"
    _write_policy(physical)
    alias = tmp_path / "alias.json"
    os.link(physical, alias)
    with pytest.raises(RoutingPolicyIntegrityError, match="single-link"):
        load_frozen_routing_policy(physical, observed_at=_observed_at())


def test_policy_rejects_future_timestamp(tmp_path: Path) -> None:
    path = tmp_path / "routing-policy.json"
    _write_policy(path)
    observed_at = _observed_at()
    future = observed_at + timedelta(hours=1)
    os.utime(path, (future.timestamp(), future.timestamp()))

    with pytest.raises(RoutingPolicyIntegrityError, match="future"):
        load_frozen_routing_policy(path, observed_at=observed_at)


def test_observed_at_must_be_timezone_aware(tmp_path: Path) -> None:
    path = tmp_path / "routing-policy.json"
    _write_policy(path)

    with pytest.raises(ValueError, match="timezone-aware"):
        load_frozen_routing_policy(
            path,
            observed_at=datetime(2026, 7, 31, 1, 0),
        )


def test_policy_rejects_duplicate_json_keys(tmp_path: Path) -> None:
    path = tmp_path / "routing-policy.json"
    path.write_text(
        '{"default_no_target_reason":"first","default_no_target_reason":"second","rules":[]}',
        encoding="utf-8",
    )
    path.chmod(0o444)

    with pytest.raises(RoutingPolicyConflictError, match="duplicate JSON key"):
        load_frozen_routing_policy(path, observed_at=_observed_at())


def test_policy_rejects_file_changed_while_reading(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "routing-policy.json"
    _write_policy(path)
    original_read = os.read
    changed = False

    def mutate_after_read(descriptor: int, size: int) -> bytes:
        nonlocal changed
        content = original_read(descriptor, size)
        if content and not changed:
            changed = True
            path.chmod(0o644)
            path.write_bytes(path.read_bytes() + b" ")
        return content

    monkeypatch.setattr(os, "read", mutate_after_read)

    with pytest.raises(RoutingPolicyIntegrityError, match="changed while reading"):
        load_frozen_routing_policy(path, observed_at=_observed_at())


def test_loaded_resolver_is_frozen(tmp_path: Path) -> None:
    path = tmp_path / "routing-policy.json"
    _write_policy(path)
    resolver = load_frozen_routing_policy(path, observed_at=_observed_at())

    assert isinstance(resolver, FrozenRoutingPolicyResolver)
    with pytest.raises(FrozenInstanceError):
        resolver.default_no_target_reason = "changed"  # type: ignore[misc]
