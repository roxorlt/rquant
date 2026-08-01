from __future__ import annotations

from pathlib import Path

import pytest

from rquant.runtime_capabilities import (
    load_systemd_runtime_capabilities,
    serialize_runtime_credential,
)
from rquant.runtime_service_entrypoint import RuntimeServiceKind

GENERATION = "b" * 64


def _credential(root: Path, values: dict[str, str]) -> Path:
    root.mkdir(mode=0o700)
    path = root / "capabilities.json"
    path.write_bytes(serialize_runtime_credential(GENERATION, values))
    path.chmod(0o400)
    return path


def test_loads_only_service_scoped_systemd_credentials(tmp_path: Path) -> None:
    root = tmp_path / "credentials"
    values = {
        "TUSHARE_TOKEN_MAIN": "main-secret",
        "TUSHARE_TOKEN_BACKUP": "backup-secret",
    }
    _credential(root, values)
    environ = {"CREDENTIALS_DIRECTORY": str(root)}

    loaded = load_systemd_runtime_capabilities(
        RuntimeServiceKind.MARKET_MINUTE_SOURCE,
        expected_generation=GENERATION,
        environ=environ,
    )

    assert dict(loaded) == values
    assert environ["TUSHARE_TOKEN_MAIN"] == "main-secret"
    assert "main-secret" not in repr(loaded)


def test_missing_systemd_credential_directory_is_dependency_free() -> None:
    assert (
        dict(
            load_systemd_runtime_capabilities(
                RuntimeServiceKind.STRATEGY_LIVE,
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
    mutation: str,
) -> None:
    root = tmp_path / "credentials"
    values = {"TUSHARE_TOKEN_MAIN": "main-secret"}
    path = _credential(root, values)
    environ = {"CREDENTIALS_DIRECTORY": str(root)}
    if mutation == "public":
        path.chmod(0o444)
        message = "group|world"
    elif mutation == "symlink":
        real = root / "real.json"
        path.replace(real)
        path.symlink_to(real)
        message = "unsafe|unavailable"
    elif mutation == "unknown":
        path.chmod(0o600)
        path.write_bytes(serialize_runtime_credential(GENERATION, {"PUSHDEER_KEYS": "secret"}))
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
            RuntimeServiceKind.MARKET_MINUTE_SOURCE,
            expected_generation=GENERATION,
            environ=environ,
        )


def test_rejects_credential_from_another_runtime_generation(tmp_path: Path) -> None:
    root = tmp_path / "credentials"
    _credential(root, {"TUSHARE_TOKEN_MAIN": "main-secret"})

    with pytest.raises(ValueError, match="generation"):
        load_systemd_runtime_capabilities(
            RuntimeServiceKind.MARKET_MINUTE_SOURCE,
            expected_generation="c" * 64,
            environ={"CREDENTIALS_DIRECTORY": str(root)},
        )
