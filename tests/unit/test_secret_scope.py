from __future__ import annotations

import json

import pytest

from rquant.secret_scope import (
    NOTIFIER_SECRET_POLICY,
    SOURCE_GATEWAY_SECRET_POLICY,
    STRATEGY_SECRET_POLICY,
    SecretScopeError,
    load_scoped_secrets,
)


def test_source_gateway_only_receives_declared_tushare_secret() -> None:
    environment = {
        "TUSHARE_TOKEN_MAIN": "tushare-value",
        "PUSHDEER_KEYS": "push-value",
        "ORDINARY_SETTING": "visible elsewhere",
    }

    bundle = load_scoped_secrets(SOURCE_GATEWAY_SECRET_POLICY, environment)

    assert bundle.reveal("TUSHARE_TOKEN_MAIN") == "tushare-value"
    with pytest.raises(KeyError):
        bundle.reveal("PUSHDEER_KEYS")
    assert "tushare-value" not in repr(bundle)
    assert "push-value" not in repr(bundle)


def test_strategy_process_receives_no_known_secret() -> None:
    bundle = load_scoped_secrets(
        STRATEGY_SECRET_POLICY,
        {"TUSHARE_TOKEN_MAIN": "token", "PUSHPLUS_TOKENS": "push"},
    )

    assert bundle.present_keys == ()
    with pytest.raises(KeyError):
        bundle.reveal("TUSHARE_TOKEN_MAIN")


def test_notifier_policy_is_independent_and_audit_payload_never_contains_values() -> None:
    first = load_scoped_secrets(
        NOTIFIER_SECRET_POLICY,
        {"PUSHDEER_KEYS": "first", "PUSHPLUS_TOKENS": "plus"},
    )
    rotated = load_scoped_secrets(
        NOTIFIER_SECRET_POLICY,
        {"PUSHDEER_KEYS": "rotated", "PUSHPLUS_TOKENS": "plus-2"},
    )

    assert first.policy_fingerprint == rotated.policy_fingerprint
    assert first.audit_payload() == rotated.audit_payload()
    encoded = json.dumps(first.audit_payload(), sort_keys=True)
    assert "first" not in encoded
    assert "PUSHPLUS_TOKENS" in encoded  # key name is auditable; value is not


def test_missing_required_secret_and_unknown_secret_like_key_fail_closed() -> None:
    with pytest.raises(SecretScopeError, match="missing required"):
        load_scoped_secrets(SOURCE_GATEWAY_SECRET_POLICY, {})
    with pytest.raises(SecretScopeError, match="unregistered secret-like"):
        load_scoped_secrets(
            STRATEGY_SECRET_POLICY,
            {"NEW_VENDOR_API_TOKEN": "must-be-governed"},
        )
