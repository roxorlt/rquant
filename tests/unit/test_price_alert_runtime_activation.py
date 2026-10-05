import json
import os
from hashlib import sha256
from pathlib import Path

import pytest

from rquant.price_alert_runtime_contracts import (
    PriceAlertRuntimeActivation,
    require_price_alert_activation,
    verify_price_alert_activation,
)
from rquant.runtime_service_entrypoint import RuntimeServiceKind


def activation_fixture(tmp_path: Path, **changes: object) -> tuple[Path, str]:
    tmp_path.chmod(0o700)
    body = {
        "schema_version": 2,
        "service_id": "price-runtime",
        "service_kind": "price_alert_runtime",
        "plane": "live",
        "interval_seconds": 5,
        "stale_after_seconds": 30,
        "producer_commit": "b" * 40,
        "settings": {
            "price_alert_runtime": {
                "protocol": "price-alert-runtime/v1",
                "source_id": "price-local",
                "source_epoch": "1" * 64,
                "ledger_id": "2" * 64,
                "generation_id": "3" * 64,
                "evaluation_contract_sha256": "4" * 64,
                "frequency_policy_sha256": "5" * 64,
                "routing_policy_sha256": "6" * 64,
                "recipient_policy_sha256": "7" * 64,
                "event_write_enabled": True,
                "evaluation_enabled": True,
                "routing_enabled": False,
                "delivery_enabled": False,
            }
        },
    }
    body["settings"]["price_alert_runtime"].update(changes)
    path = tmp_path / "runtime.json"
    path.write_text(json.dumps(body))
    path.chmod(0o600)
    return path, sha256(path.read_bytes()).hexdigest()


def test_default_and_self_constructed_activation_cannot_authorize() -> None:
    assert RuntimeServiceKind.PRICE_ALERT_RUNTIME.value == "price_alert_runtime"
    for value in (None, True, {}, object()):
        with pytest.raises((TypeError, ValueError)):
            require_price_alert_activation(value, "evaluation")
    with pytest.raises(TypeError):
        PriceAlertRuntimeActivation()


def test_actual_manifest_mints_only_enabled_role_stages(tmp_path: Path) -> None:
    path, digest = activation_fixture(tmp_path)
    activation = verify_price_alert_activation(
        path,
        runtime_root=tmp_path,
        expected_manifest_sha256=digest,
        expected_commit="b" * 40,
        expected_kind=RuntimeServiceKind.PRICE_ALERT_RUNTIME,
    )
    binding = require_price_alert_activation(activation, "evaluation")
    assert binding.source_id == "price-local"
    assert binding.producer_manifest_sha256 == digest
    with pytest.raises(ValueError):
        require_price_alert_activation(activation, "routing")
    path.write_text(path.read_text() + " ")
    with pytest.raises(ValueError):
        require_price_alert_activation(activation, "event_write")


@pytest.mark.parametrize(
    "unsafe", ["symlink", "hardlink", "permission", "digest", "commit", "root"]
)
def test_private_actual_identity_refuses_unsafe_manifest(tmp_path: Path, unsafe: str) -> None:
    path, digest = activation_fixture(tmp_path)
    kwargs = dict(
        runtime_root=tmp_path,
        expected_manifest_sha256=digest,
        expected_commit="b" * 40,
        expected_kind=RuntimeServiceKind.PRICE_ALERT_RUNTIME,
    )
    if unsafe == "symlink":
        link = tmp_path / "link.json"
        link.symlink_to(path)
        path = link
    elif unsafe == "hardlink":
        os.link(path, tmp_path / "copy.json")
    elif unsafe == "permission":
        path.chmod(0o644)
    elif unsafe == "digest":
        kwargs["expected_manifest_sha256"] = "a" * 64
    elif unsafe == "commit":
        kwargs["expected_commit"] = "a" * 40
    else:
        tmp_path.chmod(0o755)
    with pytest.raises((TypeError, ValueError)):
        verify_price_alert_activation(path, **kwargs)


@pytest.mark.parametrize("tamper", ["extra_column", "extra_trigger"])
def test_registered_producer_rejects_changed_schema(tmp_path: Path, tamper: str) -> None:
    import sqlite3

    from rquant.price_alert_runtime_store import (
        PriceAlertRuntimeStore,
        ReadonlyPriceAlertRuntimeStore,
    )
    from tests.unit.test_price_alert_runtime_store import store_fixture

    store, activation, unused = store_fixture(tmp_path)
    store.close()
    with sqlite3.connect(store.path) as connection:
        if tamper == "extra_column":
            connection.execute(
                "ALTER TABLE price_alert_frequency_state ADD COLUMN unregistered TEXT"
            )
        else:
            connection.execute(
                "CREATE TRIGGER unregistered AFTER INSERT ON price_alert_event_log "
                "BEGIN DELETE FROM price_alert_frequency_state; END"
            )
    before = store.path.read_bytes()
    for opener in (PriceAlertRuntimeStore, ReadonlyPriceAlertRuntimeStore):
        with pytest.raises(ValueError, match="schema"):
            opener(store.path, activation=activation)
    assert store.path.read_bytes() == before


@pytest.mark.parametrize("tamper", ["extra_column", "missing_revision_trigger"])
def test_registered_delivery_rejects_changed_schema_without_repair(
    tmp_path: Path, tamper: str
) -> None:
    import sqlite3

    from tests.unit.test_price_alert_notification_admission import admission_fixture

    producer, unused, state, activation, *rest = admission_fixture(tmp_path)
    try:
        with sqlite3.connect(state.path) as connection:
            if tamper == "extra_column":
                connection.execute(
                    "ALTER TABLE price_alert_send_admission ADD COLUMN unregistered TEXT"
                )
            else:
                connection.execute(
                    "DROP TRIGGER notification_revision_price_alert_send_admission_insert"
                )
        before = state.path.read_bytes()
        with pytest.raises(ValueError, match="schema"):
            state.install_price_alert_delivery_v1(activation)
        assert state.path.read_bytes() == before
    finally:
        producer.close()
