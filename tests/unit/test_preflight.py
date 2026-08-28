"""preflight must remain safe while the intraday monitor owns the primary DB."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import subprocess
import zipfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pandas as pd
import pytest

from rquant.data_contracts import CONTRACTS_BY_ID, EXCHANGE_TIMEZONE
from rquant.preflight import (
    RuntimeRecoveryPreflightConfig,
    check_data_freshness,
    detail_duckdb_lock,
    smoke_screen,
    verify_resource_authority_services,
    verify_runtime_dependencies,
    verify_runtime_recovery,
    verify_source_quota_ledger,
)
from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_recovery_artifacts import (
    RealRecoveryReceipt,
    RealRecoveryRestorer,
)
from rquant.runtime_recovery_backup import (
    RecoveryBackupProducer,
    load_recovery_backup_generation,
)
from rquant.runtime_recovery_coordinator import RuntimeRecoveryFixedReplayVerifier
from rquant.runtime_recovery_service import (
    LegacyRecoveryServiceReceipt,
    RuntimeRecoveryService,
)
from rquant.storage import duckdb as duckdb_module
from rquant.strict_json import canonical_json_bytes
from tests.unit.test_runtime_recovery_backup import _config, _HmacSigner

LEGACY_RECEIPT_FIXTURE = Path(__file__).parents[1] / "fixtures" / "recovery_service_receipt_v1.json"


def test_resource_authority_preflight_skips_only_when_both_configs_are_absent() -> None:
    assert verify_resource_authority_services(None, None).status == "skip"
    assert verify_resource_authority_services(None, None, required=True).status == "fail"
    assert verify_resource_authority_services(Path("/missing/root.json"), None).status == "fail"
    assert verify_resource_authority_services(None, Path("/missing/resource.json")).status == "fail"


def test_required_resource_authority_preflight_rejects_unknown_environment_key(
    tmp_path: Path,
) -> None:
    systemd = tmp_path / "systemd"
    systemd.mkdir()
    external_config = tmp_path / "external.json"
    resource_config = tmp_path / "resource.json"
    external_env = tmp_path / "external.env"
    resource_env = tmp_path / "resource.env"
    external_env.write_text(
        f"APP_ENV=prod\nRQUANT_EXTERNAL_MONOTONIC_ROOT_SERVICE_CONFIG_PATH={external_config}\n",
        encoding="ascii",
    )
    resource_env.write_text(
        "APP_ENV=prod\n"
        "RQUANT_CODE_COMMIT=" + "1" * 40 + "\n"
        "RQUANT_LAB_LIVE_SLO_AUTHORITY_ROOT=/var/lib/rquant-serving/runtime_health\n"
        "RQUANT_LAB_RESOURCE_AUTHORITY_CONFIG_JSON={}\n"
        "RQUANT_LAB_RESOURCE_POLICY_VERSION=lab-resource-v1\n"
        "RQUANT_LAB_TRADE_CALENDAR_PATH=/var/lib/rquant-serving/calendar.json\n"
        f"RQUANT_RESOURCE_AUTHORITY_SERVICE_CONFIG_PATH={resource_config}\n"
        "RQUANT_RESOURCE_AUTHORITY_STATE_DIR=/var/lib/rquant-resource-authority\n"
        "UNKNOWN_AUTHORITY_KEY=denied\n",
        encoding="ascii",
    )
    external_env.chmod(0o444)
    resource_env.chmod(0o444)

    result = verify_resource_authority_services(
        external_config,
        resource_config,
        required=True,
        systemd_dir=systemd,
        external_environment_path=external_env,
        resource_environment_path=resource_env,
        authority_expected_uid=os.geteuid(),
        authority_expected_gid=os.getegid(),
    )

    assert result.status == "fail"
    assert "closed authority probe failed" in result.summary


def test_required_resource_authority_preflight_rejects_wrong_unit_environment_file(
    tmp_path: Path,
) -> None:
    systemd = tmp_path / "systemd"
    systemd.mkdir()
    external_config = tmp_path / "external.json"
    resource_config = tmp_path / "resource.json"
    external_env = tmp_path / "external.env"
    resource_env = tmp_path / "resource.env"
    external_env.write_text(
        f"APP_ENV=prod\nRQUANT_EXTERNAL_MONOTONIC_ROOT_SERVICE_CONFIG_PATH={external_config}\n",
        encoding="ascii",
    )
    resource_env.write_text(
        "APP_ENV=prod\n"
        "RQUANT_CODE_COMMIT=" + "1" * 40 + "\n"
        "RQUANT_LAB_LIVE_SLO_AUTHORITY_ROOT=/var/lib/rquant-serving/runtime_health\n"
        "RQUANT_LAB_RESOURCE_AUTHORITY_CONFIG_JSON={}\n"
        "RQUANT_LAB_RESOURCE_POLICY_VERSION=lab-resource-v1\n"
        "RQUANT_LAB_TRADE_CALENDAR_PATH=/var/lib/rquant-serving/calendar.json\n"
        f"RQUANT_RESOURCE_AUTHORITY_SERVICE_CONFIG_PATH={resource_config}\n"
        "RQUANT_RESOURCE_AUTHORITY_STATE_DIR=/var/lib/rquant-resource-authority\n",
        encoding="ascii",
    )
    external_env.chmod(0o444)
    resource_env.chmod(0o444)
    executable = "/usr/local/libexec/rquant-authority-runtime/current/venv/bin/rquant"
    (systemd / "rquant-external-monotonic-root.service").write_text(
        "[Service]\n"
        "EnvironmentFile=/etc/rquant/wrong.env\n"
        f"ExecStart={executable} external-monotonic-root-serve --config {external_config}\n",
        encoding="utf-8",
    )
    (systemd / "rquant-resource-authority.service").write_text(
        "[Service]\n"
        f"EnvironmentFile={resource_env}\n"
        f"ExecStart={executable} resource-authority-serve --config {resource_config}\n",
        encoding="utf-8",
    )

    result = verify_resource_authority_services(
        external_config,
        resource_config,
        required=True,
        systemd_dir=systemd,
        external_environment_path=external_env,
        resource_environment_path=resource_env,
        authority_expected_uid=os.geteuid(),
        authority_expected_gid=os.getegid(),
    )

    assert result.status == "fail"
    assert "closed authority probe failed" in result.summary


class _Cursor:
    def __init__(self, row: tuple[Any, ...]) -> None:
        self._row = row

    def fetchone(self) -> tuple[Any, ...]:
        return self._row


class _Connection:
    def __init__(self, row: tuple[Any, ...]) -> None:
        self._row = row

    def execute(self, *_: object, **__: object) -> _Cursor:
        return _Cursor(self._row)


class _ReadonlyStore:
    def __init__(self, row: tuple[Any, ...]) -> None:
        self._conn = _Connection(row)
        self.entered = False
        self.closed = False

    def __enter__(self) -> _ReadonlyStore:
        self.entered = True
        return self

    def __exit__(self, *_: object) -> None:
        self.closed = True

    def latest_trading_day(self, anchor: date, *, exchange: str = "SSE") -> date:
        del exchange
        return anchor

    def previous_trading_day(self, anchor: date, *, exchange: str = "SSE") -> date:
        del anchor, exchange
        return date(2026, 7, 10)

    def is_trading_day(self, exchange: str, cal_date: date) -> bool:
        del exchange, cal_date
        return True

    def list_trade_calendar(self, exchange: str, start: date, end: date) -> list[object]:
        del exchange, start, end
        return []


def _forbid_primary_store(*_: object, **__: object) -> None:
    raise AssertionError("preflight must not construct DuckDBStore directly")


@pytest.mark.parametrize(
    ("profile", "expected_status"),
    (("production", "fail"), ("candidate", "warn")),
)
def test_source_quota_preflight_gates_uninitialized_ledger_without_creating_it(
    tmp_path: Path,
    profile: str,
    expected_status: str,
) -> None:
    path = tmp_path / "quota.sqlite3"

    result = verify_source_quota_ledger(
        path,
        source="tushare.daily_close",
        profile=profile,
    )

    assert result.status == expected_status
    assert not path.exists()


def test_source_quota_preflight_rejects_a_symlink_even_for_candidate(
    tmp_path: Path,
) -> None:
    target = tmp_path / "target.sqlite3"
    target.touch()
    path = tmp_path / "quota.sqlite3"
    path.symlink_to(target)

    result = verify_source_quota_ledger(
        path,
        source="tushare.daily_close",
        profile="candidate",
    )

    assert result.status == "fail"


def test_source_quota_preflight_rejects_an_incomplete_v3_schema(tmp_path: Path) -> None:
    path = tmp_path / "quota.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.executescript(
            """
            CREATE TABLE quota_attempt (outcome TEXT NOT NULL);
            PRAGMA user_version = 3;
            """
        )

    result = verify_source_quota_ledger(path, source="tushare.daily_close")

    assert result.status == "fail"


def test_source_quota_preflight_fails_for_a_pending_provider_attempt(tmp_path: Path) -> None:
    from rquant.source_quota_store import SourceQuotaStore

    path = tmp_path / "quota.sqlite3"
    store = SourceQuotaStore(path)
    start = datetime(2026, 7, 31, 9, tzinfo=UTC)
    store.declare_window(
        source="tushare.daily_close",
        window_id="20260731",
        starts_at=start,
        resets_at=start + timedelta(days=1),
        total_units=20,
    )
    attempt = store.begin_attempt(
        source="tushare.daily_close",
        owner="daily-close:test",
        attempt_id="test",
        units=1,
        now=start,
        expires_at=start + timedelta(days=1),
    )
    store.mark_dispatched(attempt.attempt_id, now=start)

    result = verify_source_quota_ledger(path, source="tushare.daily_close")

    assert result.status == "fail"
    assert "pending=1" in result.summary


def test_runtime_dependencies_require_safe_ssh_keygen_and_linux_package(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    binary = tmp_path / "ssh-keygen"
    binary.write_bytes(b"binary")
    binary.chmod(0o755)
    observed: list[tuple[str, ...]] = []

    def run(command: list[str], **kwargs: object) -> SimpleNamespace:
        observed.append(tuple(command))
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr("rquant.preflight.subprocess.run", run)
    result = verify_runtime_dependencies(
        ssh_keygen_path=binary,
        platform_name="Linux",
        rpm_path=Path("/usr/bin/rpm"),
    )

    assert result.status == "ok"
    assert observed == [("/usr/bin/rpm", "-q", "openssh-clients")]


def test_runtime_dependencies_fail_when_ssh_keygen_is_unsafe(tmp_path: Path) -> None:
    binary = tmp_path / "ssh-keygen"
    binary.write_bytes(b"binary")
    binary.chmod(0o777)

    result = verify_runtime_dependencies(
        ssh_keygen_path=binary,
        platform_name="Darwin",
    )

    assert result.status == "fail"
    assert "ssh-keygen" in result.summary


def test_production_deploy_explicitly_requires_openssh_clients() -> None:
    script = (Path(__file__).resolve().parents[2] / "scripts/deploy-production.sh").read_text()

    assert "/usr/bin/ssh-keygen" in script
    assert "rpm -q openssh-clients" in script


def test_data_freshness_uses_readonly_store_helper(monkeypatch: Any) -> None:
    store = _ReadonlyStore((date(2026, 7, 10), 123))
    seen_required_tables: list[tuple[str, ...]] = []

    def open_readonly_store(
        *, required_tables: tuple[str, ...] | list[str] | None = None
    ) -> _ReadonlyStore:
        seen_required_tables.append(tuple(required_tables or ()))
        return store

    monkeypatch.setattr(duckdb_module, "open_readonly_store", open_readonly_store)
    monkeypatch.setattr(duckdb_module, "DuckDBStore", _forbid_primary_store)

    result = check_data_freshness(
        (CONTRACTS_BY_ID["daily_bar"],),
        as_of=datetime.combine(
            date(2026, 7, 13),
            datetime.min.time(),
            tzinfo=EXCHANGE_TIMEZONE,
        ),
        replica_path=None,
    )

    assert result.status == "ok"
    assert seen_required_tables == [()]
    assert store.entered is True
    assert store.closed is True


def test_smoke_screen_uses_readonly_store_helper(monkeypatch: Any) -> None:
    store = _ReadonlyStore((date(2026, 7, 10),))
    seen_required_tables: list[tuple[str, ...]] = []

    def open_readonly_store(
        *, required_tables: tuple[str, ...] | list[str] | None = None
    ) -> _ReadonlyStore:
        seen_required_tables.append(tuple(required_tables or ()))
        return store

    def fake_screen(*_: object, **kwargs: object) -> pd.DataFrame:
        assert kwargs["store"] is store
        return pd.DataFrame()

    monkeypatch.setattr(duckdb_module, "open_readonly_store", open_readonly_store)
    monkeypatch.setattr(duckdb_module, "DuckDBStore", _forbid_primary_store)
    monkeypatch.setattr("rquant.screen.core.screen", fake_screen)

    result = smoke_screen()

    assert result.status == "ok"
    assert seen_required_tables == [
        (
            "screen_result",
            "daily_bar",
            "daily_indicator",
            "daily_state",
            "daily_basic",
            "stock_basic",
        )
    ]
    assert store.entered is True
    assert store.closed is True


def test_lock_detail_does_not_treat_unclassified_fd_as_no_writer(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "rquant.duckdb"
    db_path.touch()
    output = "\n".join(
        [
            "COMMAND PID USER FD TYPE DEVICE SIZE/OFF NODE NAME",
            f"python3.14 3847085 lighthouse mem REG 1,2 0 42 {db_path}",
        ]
    )
    completed = subprocess.CompletedProcess(
        args=["lsof", str(db_path)], returncode=0, stdout=output, stderr=""
    )

    monkeypatch.setattr("rquant.preflight.shutil.which", lambda _: "/usr/bin/lsof")
    monkeypatch.setattr("rquant.preflight.subprocess.run", lambda *_args, **_kwargs: completed)

    result = detail_duckdb_lock(db_path)

    assert result.status == "warn"
    assert "不能判断 monitor 未运行" in result.summary
    assert any("python3.14 pid=3847085 FD=mem" in line for line in result.details)


def _recovery_preflight_fixture(
    tmp_path: Path,
    *,
    now: datetime,
) -> tuple[RuntimeRecoveryPreflightConfig, RuntimeRecoveryService, str]:
    backup_config = _config(tmp_path)
    producer = RecoveryBackupProducer(
        config=backup_config,
        signer=_HmacSigner(),
        clock=lambda: now - timedelta(minutes=5),
    )
    receipt = producer.execute(expected_plan_id=producer.preview().plan_id)
    publication_root = Path(backup_config.publication_root)
    generation = publication_root / "generations" / receipt.manifest_id
    restore_root = tmp_path / "rehearsal-restore"
    restore_root.mkdir(mode=0o700)
    _pointer, _backup, target, tool, expectations = load_recovery_backup_generation(
        publication_root,
        trusted_verifiers={_HmacSigner.key_id: _HmacSigner()},
    )
    replay = RuntimeRecoveryFixedReplayVerifier(expectations=expectations.expectations)
    recovery = RealRecoveryRestorer(
        backup_root=generation,
        restore_root=restore_root,
        signature_verifier=_HmacSigner(),
        fixed_replay_verifier=replay,
        deadline_seconds=60,
    ).restore(target=target, tool_bundle=tool)
    service = RuntimeRecoveryService(
        state_path=tmp_path / "recovery-state" / "state.sqlite3",
        receipt_root=tmp_path / "recovery-service-receipts",
        worker_id="preflight-test",
        clock=lambda: now,
    )
    job = service.submit(
        request_id="scheduled-rehearsal",
        backup_root=generation,
        manifest_path=generation / "recovery-target.json",
        tool_bundle_path=generation / "recovery-tool.json",
        restore_root=restore_root,
        deadline_at=now + timedelta(minutes=5),
    )
    service.run_once(lambda _lease: recovery)
    config = RuntimeRecoveryPreflightConfig(
        publication_root=publication_root,
        service_state_path=service.state_path,
        service_receipt_root=service.receipt_root,
        restore_root=restore_root,
        expected_profile_generation=receipt.target_profile_generation,
        expected_manifest_id=receipt.manifest_id,
        max_rpo=timedelta(minutes=30),
        max_rehearsal_age=timedelta(hours=2),
        max_rto=timedelta(minutes=2),
        trusted_backup_verifiers={_HmacSigner.key_id: _HmacSigner()},
    )
    return config, service, job.job_id


def test_runtime_recovery_preflight_accepts_current_fresh_rehearsal(tmp_path: Path) -> None:
    now = datetime(2026, 8, 2, 8, 0, tzinfo=UTC)
    config, _service, _job_id = _recovery_preflight_fixture(tmp_path, now=now)

    result = verify_runtime_recovery(config, as_of=now)

    assert result.status == "ok"
    assert "rehearsal" in result.summary
    assert any(config.expected_manifest_id in line for line in result.details)


def test_runtime_recovery_preflight_accepts_v3_receipt_state_after_v1_upgrade(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 8, 2, 1, 30, tzinfo=UTC)
    config, service, _job_id = _recovery_preflight_fixture(tmp_path, now=now)
    current_path = next(service.receipt_root.glob("*.json"))
    legacy_raw = LEGACY_RECEIPT_FIXTURE.read_bytes()
    document = LegacyRecoveryServiceReceipt.model_validate_json(legacy_raw).model_dump(mode="json")
    assert canonical_json_bytes(document) == legacy_raw
    legacy_path = service.receipt_root / f"{document['receipt_id']}.json"
    current_path.chmod(0o600)
    current_path.unlink()
    legacy_path.write_bytes(legacy_raw)
    legacy_path.chmod(0o400)

    connection = sqlite3.connect(service.state_path)
    try:
        connection.execute("DELETE FROM recovery_receipt")
        connection.execute(
            """
            INSERT INTO recovery_receipt(
                receipt_id, job_id, fence, status, verification_level,
                recovery_receipt_id, content_sha256, relative_path, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                document["receipt_id"],
                document["job_id"],
                document["fence"],
                document["status"],
                document["verification_level"],
                document["recovery_receipt_id"],
                hashlib.sha256(legacy_raw).hexdigest(),
                legacy_path.name,
                str(document["completed_at"]).replace("Z", ".000000Z"),
            ),
        )
        connection.execute("DROP INDEX recovery_receipt_outbox_created_idx")
        connection.execute("DROP TABLE recovery_receipt_outbox")
        connection.execute("DROP INDEX recovery_receipt_migration_status_idx")
        connection.execute("DROP TABLE recovery_receipt_migration")
        connection.execute("PRAGMA user_version = 3")
        connection.execute("UPDATE recovery_metadata SET value = '3' WHERE key = 'schema_version'")
        connection.commit()
    finally:
        connection.close()

    RuntimeRecoveryService(
        state_path=service.state_path,
        receipt_root=service.receipt_root,
        worker_id="preflight-v1-upgrade",
        clock=lambda: now,
    )

    fixture_completed_at = datetime.fromisoformat(
        str(document["completed_at"]).replace("Z", "+00:00")
    )

    def accept_fixture_recovery_receipt(**kwargs: object) -> tuple[object, object]:
        assert kwargs["receipt_id"] == document["recovery_receipt_id"]
        target = kwargs["target"]
        return (
            SimpleNamespace(generation_id=target.manifest_id),
            SimpleNamespace(
                manifest_id=target.manifest_id,
                target_profile_generation=target.target_profile_generation,
                started_at=fixture_completed_at - timedelta(seconds=45),
                completed_at=fixture_completed_at,
            ),
        )

    monkeypatch.setattr(
        "rquant.preflight.load_verified_real_recovery_receipt",
        accept_fixture_recovery_receipt,
    )

    assert verify_runtime_recovery(config, as_of=now).status == "ok"


def test_runtime_recovery_preflight_selects_old_artifact_key_from_rotation_set(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 2, 8, 0, tzinfo=UTC)
    config, _service, _job_id = _recovery_preflight_fixture(tmp_path, now=now)

    class NextSigner(_HmacSigner):
        key_id = "recovery-backup-next-key"

    during_rotation = RuntimeRecoveryPreflightConfig(
        **{
            **config.__dict__,
            "trusted_backup_verifiers": {
                NextSigner.key_id: NextSigner(),
                _HmacSigner.key_id: _HmacSigner(),
            },
        }
    )
    after_old_key_removal = RuntimeRecoveryPreflightConfig(
        **{
            **config.__dict__,
            "trusted_backup_verifiers": {NextSigner.key_id: NextSigner()},
        }
    )

    assert verify_runtime_recovery(during_rotation, as_of=now).status == "ok"
    rejected = verify_runtime_recovery(after_old_key_removal, as_of=now)
    assert rejected.status == "fail"
    assert "backup" in rejected.summary or "验证" in rejected.summary


def test_runtime_recovery_preflight_rejects_stale_rpo_and_old_generation(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 2, 8, 0, tzinfo=UTC)
    config, _service, _job_id = _recovery_preflight_fixture(tmp_path, now=now)

    stale = verify_runtime_recovery(config, as_of=now + timedelta(hours=1))
    old = verify_runtime_recovery(
        RuntimeRecoveryPreflightConfig(
            **{
                **config.__dict__,
                "expected_profile_generation": canonical_sha256({"old": "profile"}),
            }
        ),
        as_of=now,
    )

    assert stale.status == "fail"
    assert "RPO" in stale.summary
    assert old.status == "fail"
    assert "generation" in old.summary


def test_runtime_recovery_preflight_rehashes_current_generation(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 2, 8, 0, tzinfo=UTC)
    config, _service, _job_id = _recovery_preflight_fixture(tmp_path, now=now)
    generation = config.restore_root / "generations" / str(config.expected_manifest_id)
    artifact = next(
        path
        for path in generation.rglob("*")
        if path.is_file() and path.name != "recovery-generation-manifest.json"
    )
    artifact.chmod(0o600)
    artifact.write_bytes(artifact.read_bytes() + b"tampered")

    result = verify_runtime_recovery(config, as_of=now)

    assert result.status == "fail"
    assert "restore receipt" in result.summary


def test_runtime_recovery_preflight_rejects_rehashed_current_without_signature(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 2, 8, 0, tzinfo=UTC)
    config, _service, _job_id = _recovery_preflight_fixture(tmp_path, now=now)
    current_path = config.publication_root / "current.json"
    payload = json.loads(current_path.read_text(encoding="utf-8"))
    payload["published_at"] = (now + timedelta(minutes=1)).isoformat().replace("+00:00", "Z")
    current_path.chmod(0o600)
    current_path.write_text(
        json.dumps(payload, separators=(",", ":"), sort_keys=True),
        encoding="utf-8",
    )

    result = verify_runtime_recovery(config, as_of=now)

    assert result.status == "fail"
    assert "backup" in result.summary or "验证" in result.summary


def test_runtime_recovery_preflight_rejects_latest_failed_rehearsal(tmp_path: Path) -> None:
    now = datetime(2026, 8, 2, 8, 0, tzinfo=UTC)
    config, service, _job_id = _recovery_preflight_fixture(tmp_path, now=now)
    failed_at = now + timedelta(seconds=61)
    service.clock = lambda: failed_at
    service.max_attempts = 1
    publication_root = config.publication_root
    pointer, _backup, _target, _tool, _expectations = load_recovery_backup_generation(
        publication_root,
        trusted_verifiers={_HmacSigner.key_id: _HmacSigner()},
    )
    generation = publication_root / pointer.generation_path
    service.submit(
        request_id="failed-rehearsal",
        backup_root=generation,
        manifest_path=generation / "recovery-target.json",
        tool_bundle_path=generation / "recovery-tool.json",
        restore_root=config.restore_root,
        deadline_at=failed_at + timedelta(minutes=5),
    )

    def fail_rehearsal(_lease: object) -> RealRecoveryReceipt:
        raise RuntimeError("rehearsal failed")

    service.run_once(fail_rehearsal)

    result = verify_runtime_recovery(config, as_of=failed_at)

    assert result.status == "fail"
    assert "failed" in result.summary


def test_daily_receipt_signer_units_are_health_checked_by_preflight_and_pre_market() -> None:
    """The socket-activated Daily signer is a first-class production unit everywhere."""

    from rquant import pre_market_check
    from rquant.preflight import SERVICES_TO_CHECK

    for units in (SERVICES_TO_CHECK, pre_market_check.SERVICES_TO_CHECK):
        assert "rquant-daily-receipt-signer.socket" in units
        assert "rquant-daily-receipt-signer.service" in units

    # The two lists share one ground truth for the signer boundary.
    assert set(SERVICES_TO_CHECK) >= set(pre_market_check.SERVICES_TO_CHECK)


def test_preflight_rejects_a_daily_authority_release_without_root_runtime_invariants(
    tmp_path: Path,
) -> None:
    from rquant.preflight import verify_daily_receipt_authority_runtime

    source = b"import socket\n"
    source_hash = hashlib.sha256(source).hexdigest()
    root = tmp_path / "usr/local/libexec/rquant-daily-receipt-authority"
    release = root / "releases" / source_hash
    release.mkdir(parents=True, mode=0o755)
    artifact = release / "authority.pyz"
    with zipfile.ZipFile(artifact, "w") as bundle:
        bundle.writestr("__main__.py", source)
    artifact.chmod(0o555)
    source_hash_path = release / "source.sha256"
    source_hash_path.write_text(f"{source_hash}\n", encoding="utf-8")
    source_hash_path.chmod(0o444)
    (root / "current").symlink_to(f"releases/{source_hash}")

    result = verify_daily_receipt_authority_runtime(
        root=root,
        expected_uid=os.geteuid(),
        platform_name="Linux",
    )
    assert result.status == "ok"

    artifact.chmod(0o644)
    rejected = verify_daily_receipt_authority_runtime(
        root=root,
        expected_uid=os.geteuid(),
        platform_name="Linux",
    )
    assert rejected.status == "fail"
    assert "authority" in rejected.name

    artifact.chmod(0o555)
    (root / "unexpected").symlink_to(f"releases/{source_hash}")
    extra_link = verify_daily_receipt_authority_runtime(
        root=root,
        expected_uid=os.geteuid(),
        platform_name="Linux",
    )
    assert extra_link.status == "fail"
    assert "链接" in extra_link.summary


@pytest.mark.parametrize("running_sha", ("match", "stale"))
def test_preflight_identity_probe_must_match_selected_release(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    running_sha: str,
) -> None:
    from rquant.preflight import verify_daily_receipt_authority_runtime

    source = b"import socket\n"
    source_hash = hashlib.sha256(source).hexdigest()
    root = tmp_path / "usr/local/libexec/rquant-daily-receipt-authority"
    release = root / "releases" / source_hash
    release.mkdir(parents=True, mode=0o755)
    artifact = release / "authority.pyz"
    with zipfile.ZipFile(artifact, "w") as bundle:
        bundle.writestr("__main__.py", source)
    artifact.chmod(0o555)
    source_hash_path = release / "source.sha256"
    source_hash_path.write_text(f"{source_hash}\n", encoding="utf-8")
    source_hash_path.chmod(0o444)
    (root / "current").symlink_to(f"releases/{source_hash}")

    observed_sha = source_hash if running_sha == "match" else "f" * 64
    monkeypatch.setattr(
        "rquant.preflight.probe_daily_receipt_authority_identity",
        lambda **_: SimpleNamespace(source_sha256=observed_sha, key_id="daily-v1"),
    )

    result = verify_daily_receipt_authority_runtime(
        root=root,
        expected_uid=os.geteuid(),
        platform_name="Linux",
        probe_identity=True,
        identity_socket_path=tmp_path / "identity.sock",
        trusted_keyring_path=tmp_path / "trusted-keys.json",
    )

    assert result.status == ("ok" if running_sha == "match" else "fail")
    if running_sha == "stale":
        assert "identity mismatch" in result.summary


@pytest.mark.parametrize("ancestor", ("usr/local", "usr/local/libexec"))
def test_preflight_rejects_an_untrusted_daily_authority_ancestor(
    tmp_path: Path,
    ancestor: str,
) -> None:
    from rquant.preflight import verify_daily_receipt_authority_runtime

    source = b"import socket\n"
    source_hash = hashlib.sha256(source).hexdigest()
    root = tmp_path / "usr/local/libexec/rquant-daily-receipt-authority"
    release = root / "releases" / source_hash
    release.mkdir(parents=True, mode=0o755)
    artifact = release / "authority.pyz"
    with zipfile.ZipFile(artifact, "w") as bundle:
        bundle.writestr("__main__.py", source)
    artifact.chmod(0o555)
    source_hash_path = release / "source.sha256"
    source_hash_path.write_text(f"{source_hash}\n", encoding="utf-8")
    source_hash_path.chmod(0o444)
    (root / "current").symlink_to(f"releases/{source_hash}")

    ancestor_path = tmp_path / ancestor
    ancestor_path.chmod(0o777)
    writable = verify_daily_receipt_authority_runtime(
        root=root,
        expected_uid=os.geteuid(),
        platform_name="Linux",
    )
    assert writable.status == "fail"
    assert "ancestor" in writable.summary.lower()

    ancestor_path.chmod(0o755)
    renamed = ancestor_path.with_name(f"{ancestor_path.name}-real")
    ancestor_path.rename(renamed)
    ancestor_path.symlink_to(renamed.name, target_is_directory=True)
    symlinked = verify_daily_receipt_authority_runtime(
        root=root,
        expected_uid=os.geteuid(),
        platform_name="Linux",
    )
    assert symlinked.status == "fail"
    assert "ancestor" in symlinked.summary.lower()
