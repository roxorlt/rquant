from __future__ import annotations

from pathlib import Path

import pytest

from rquant.runtime_capabilities import (
    CAPABILITY_KEYS,
    load_systemd_runtime_capabilities,
    serialize_runtime_credential,
)
from rquant.runtime_service_entrypoint import RuntimeServiceKind
from tests.support.systemd_credential_delivery import Delivery, deliver

GENERATION = "b" * 64
SERVICE_ID = "source.market-minute"
INSTANCE = "svc-" + "a" * 64
KIND = RuntimeServiceKind.MARKET_MINUTE_SOURCE


def test_auction_source_receives_only_the_paid_api_primary_token() -> None:
    assert CAPABILITY_KEYS[RuntimeServiceKind.AUCTION_MATCH_SOURCE] == frozenset(
        {"TUSHARE_TOKEN_MAIN"}
    )


def test_reference_source_receives_only_the_primary_tushare_token() -> None:
    assert CAPABILITY_KEYS[RuntimeServiceKind.REFERENCE_SLOW_SOURCE] == frozenset(
        {
            "TUSHARE_TOKEN_MAIN",
            "RQ_REFERENCE_SOURCE_SIGNING_KEY_ID",
            "RQ_REFERENCE_SOURCE_PRIVATE_KEY_BASE64",
            "RQ_REFERENCE_SOURCE_PUBLIC_KEY",
        }
    )
    assert CAPABILITY_KEYS[RuntimeServiceKind.REFERENCE_SLOW_PUBLISHER] == frozenset(
        {
            "RQ_REFERENCE_PUBLICATION_HMAC_KEY_ID",
            "RQ_REFERENCE_PUBLICATION_HMAC_SECRET_HEX",
            "RQ_REFERENCE_SOURCE_SIGNING_KEY_ID",
            "RQ_REFERENCE_SOURCE_PUBLIC_KEY",
        }
    )
    assert all(
        "RQ_REFERENCE_PUBLICATION_HMAC_SECRET_HEX" not in allowed
        for kind, allowed in CAPABILITY_KEYS.items()
        if kind is not RuntimeServiceKind.REFERENCE_SLOW_PUBLISHER
    )
    assert all(
        "RQ_REFERENCE_SOURCE_PRIVATE_KEY_BASE64" not in allowed
        for kind, allowed in CAPABILITY_KEYS.items()
        if kind is not RuntimeServiceKind.REFERENCE_SLOW_SOURCE
    )


def test_retention_writer_credential_is_scoped_only_to_retention_service() -> None:
    capability = "RQ_ARTIFACT_RETENTION_WRITER_CREDENTIAL"
    assert CAPABILITY_KEYS[RuntimeServiceKind.ARTIFACT_RETENTION] == frozenset({capability})
    assert all(
        capability not in allowed
        for kind, allowed in CAPABILITY_KEYS.items()
        if kind is not RuntimeServiceKind.ARTIFACT_RETENTION
    )


def _credential(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    values: dict[str, str],
) -> Delivery:
    """A credential delivered the way systemd delivers one (#215's third break).

    `directory_mode=0o700` is the mode Docker's own `--tmpfs` and a root-run unit both
    produce, and the one that leaves this file's mutations able to rewrite the credential
    in place; the ACL shape (0550 with a root-owned 0440 file) is held next door in
    `tests/unit/test_systemd_credential_delivery_shape.py`.
    """

    return deliver(
        monkeypatch,
        root=tmp_path / "run-credentials",
        payload=serialize_runtime_credential(
            service_id=SERVICE_ID,
            service_kind=KIND,
            instance_name=INSTANCE,
            bundle_generation=GENERATION,
            values=values,
        ),
        directory_mode=0o700,
    )


def test_loads_only_service_scoped_systemd_credentials(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    values = {
        "TUSHARE_TOKEN_MAIN": "main-secret",
        "TUSHARE_TOKEN_BACKUP": "backup-secret",
    }
    delivery = _credential(monkeypatch, tmp_path, values)
    environ = {"CREDENTIALS_DIRECTORY": str(delivery.directory)}

    loaded = load_systemd_runtime_capabilities(
        KIND,
        expected_service_id=SERVICE_ID,
        expected_instance=INSTANCE,
        expected_generation=GENERATION,
        environ=environ,
    )

    assert dict(loaded) == values
    assert "TUSHARE_TOKEN_MAIN" not in environ
    assert "main-secret" not in repr(loaded)


def test_missing_systemd_credential_directory_is_dependency_free() -> None:
    assert (
        dict(
            load_systemd_runtime_capabilities(
                RuntimeServiceKind.STRATEGY_LIVE,
                expected_service_id="strategy.n-shape",
                expected_instance="svc-" + "d" * 64,
                expected_generation=GENERATION,
                environ={},
            )
        )
        == {}
    )


@pytest.mark.parametrize(
    "mutation",
    ("public", "symlink", "unknown", "conflict", "preloaded"),
)
def test_rejects_unsafe_or_cross_service_credentials(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    values = {"TUSHARE_TOKEN_MAIN": "main-secret"}
    delivery = _credential(monkeypatch, tmp_path, values)
    path = delivery.path
    environ = {"CREDENTIALS_DIRECTORY": str(delivery.directory)}
    if mutation == "public":
        path.chmod(0o444)
        message = "group|world"
    elif mutation == "symlink":
        real = delivery.directory / "real.json"
        path.replace(real)
        path.symlink_to(real)
        message = "unsafe|unavailable"
    elif mutation == "unknown":
        path.chmod(0o600)
        path.write_bytes(
            serialize_runtime_credential(
                service_id=SERVICE_ID,
                service_kind=KIND,
                instance_name=INSTANCE,
                bundle_generation=GENERATION,
                values={"PUSHDEER_KEYS": "secret"},
            )
        )
        path.chmod(0o400)
        message = "allowlist"
    elif mutation == "conflict":
        environ["TUSHARE_TOKEN_MAIN"] = "different"
        message = "conflicts"
    else:
        environ["TUSHARE_TOKEN_MAIN"] = "main-secret"
        message = "already present"

    with pytest.raises(ValueError, match=message):
        load_systemd_runtime_capabilities(
            KIND,
            expected_service_id=SERVICE_ID,
            expected_instance=INSTANCE,
            expected_generation=GENERATION,
            environ=environ,
        )


def test_rejects_credential_from_another_runtime_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    delivery = _credential(monkeypatch, tmp_path, {"TUSHARE_TOKEN_MAIN": "main-secret"})

    with pytest.raises(ValueError, match="generation"):
        load_systemd_runtime_capabilities(
            KIND,
            expected_service_id=SERVICE_ID,
            expected_instance=INSTANCE,
            expected_generation="c" * 64,
            environ={"CREDENTIALS_DIRECTORY": str(delivery.directory)},
        )


@pytest.mark.parametrize(
    ("expected_service_id", "expected_kind", "expected_instance", "message"),
    [
        ("source.other", KIND, INSTANCE, "service"),
        (SERVICE_ID, RuntimeServiceKind.NOTIFIER, INSTANCE, "kind"),
        (SERVICE_ID, KIND, "svc-" + "c" * 64, "instance"),
    ],
)
def test_rejects_credential_bound_to_another_service_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    expected_service_id: str,
    expected_kind: RuntimeServiceKind,
    expected_instance: str,
    message: str,
) -> None:
    delivery = _credential(monkeypatch, tmp_path, {"TUSHARE_TOKEN_MAIN": "main-secret"})

    with pytest.raises(ValueError, match=message):
        load_systemd_runtime_capabilities(
            expected_kind,
            expected_service_id=expected_service_id,
            expected_instance=expected_instance,
            expected_generation=GENERATION,
            environ={"CREDENTIALS_DIRECTORY": str(delivery.directory)},
        )
