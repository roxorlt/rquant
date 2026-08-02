from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from rquant.config import Settings


def _settings(tmp_path: Path, **overrides: object) -> Settings:
    values: dict[str, object] = {
        "_env_file": None,
        "tushare_token_main": "x" * 32,
        "data_dir": tmp_path / "data",
        "duckdb_path": tmp_path / "data" / "rquant.duckdb",
        "parquet_dir": tmp_path / "data" / "parquet",
        "log_dir": tmp_path / "logs",
    }
    values.update(overrides)
    return Settings(**values)  # type: ignore[arg-type]


def test_notification_endpoints_default_to_https(tmp_path: Path) -> None:
    configured = _settings(tmp_path)

    assert configured.pushdeer_endpoint.startswith("https://")
    assert configured.pushplus_endpoint.startswith("https://")


@pytest.mark.parametrize("field", ("pushdeer_endpoint", "pushplus_endpoint"))
def test_notification_config_rejects_non_https_endpoint(
    tmp_path: Path,
    field: str,
) -> None:
    with pytest.raises(ValidationError, match="HTTPS"):
        _settings(tmp_path, **{field: "http://notify.invalid/send"})


def test_legacy_pushplus_http_default_is_upgraded_without_using_http(
    tmp_path: Path,
) -> None:
    configured = _settings(
        tmp_path,
        pushplus_endpoint="http://www.pushplus.plus/send",
    )

    assert configured.pushplus_endpoint == "https://www.pushplus.plus/send"


def test_single_recipient_id_expands_to_all_pushdeer_device_keys(tmp_path: Path) -> None:
    configured = _settings(
        tmp_path,
        pushdeer_keys="iphone-key,mac-key",
        pushdeer_recipient_ids="admin",
    )

    assert configured.pushdeer_recipient_id_list == ["admin", "admin"]


def test_legacy_missing_recipient_ids_uses_stable_default(tmp_path: Path) -> None:
    configured = _settings(tmp_path, pushdeer_keys="iphone-key,mac-key")

    assert configured.pushdeer_recipient_id_list == ["admin", "admin"]
