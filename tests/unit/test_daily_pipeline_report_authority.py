from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from rquant.daily_pipeline_ledger import DailyPipelineMode, DailyPipelineStorageProfile

PROFILE_HASH = "b" * 64


def _profile(root: Path, mode: DailyPipelineMode) -> DailyPipelineStorageProfile:
    return DailyPipelineStorageProfile.create(
        root=root.resolve(),
        mode=mode,
        profile_hash=PROFILE_HASH,
    )


@pytest.mark.parametrize(
    "name",
    [
        "RQUANT_DAILY_REPORT_AUTHORITY_COMMAND",
        "RQUANT_DAILY_REPORT_AUTHORITY_COMMAND_JSON",
        "RQUANT_DAILY_REPORT_AUTHORITY_ARGV",
        "RQUANT_DAILY_REPORT_AUTHORITY_ARGUMENTS",
        "RQUANT_DAILY_REPORT_AUTHORITY_ROOT",
        "RQUANT_DAILY_REPORT_AUTHORITY_STATE_ROOT",
        "RQUANT_DAILY_REPORT_AUTHORITY_KEYS_FILE",
        "RQUANT_DAILY_REPORT_AUTHORITY_SECRET",
    ],
)
def test_production_report_authority_rejects_runner_owned_injection(
    monkeypatch: pytest.MonkeyPatch,
    name: str,
) -> None:
    from rquant.daily_pipeline_report_authority import (
        DailyPipelineReportAuthorityClient,
        DailyPipelineReportAuthorityError,
    )

    monkeypatch.setenv(name, "/tmp/attacker-controlled")
    profile = _profile(Path("/tmp/daily-report-test"), DailyPipelineMode.PRODUCTION)

    with pytest.raises(DailyPipelineReportAuthorityError, match="runner-owned"):
        DailyPipelineReportAuthorityClient.from_production_profile(
            code_identity="a" * 40,
            profile_identity=PROFILE_HASH,
            mode=profile.mode,
            namespace_id=str(profile.namespace_id),
        )


def test_production_report_authority_uses_fixed_shared_public_key_client(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from rquant import daily_pipeline_report_authority as report_authority
    from rquant.config import settings
    from rquant.lab_highwater_authority import PRODUCTION_LAB_HIGHWATER_COMMAND

    public_key = b"-----BEGIN PUBLIC KEY-----\ntrusted-only\n-----END PUBLIC KEY-----\n"
    keyring = tmp_path / "public-keys.json"
    captured: dict[str, object] = {}

    class _SharedClient:
        def __init__(self, config: object) -> None:
            captured["client_config"] = config

    def _config(**kwargs: object) -> object:
        captured.update(kwargs)
        return SimpleNamespace(**kwargs)

    monkeypatch.setattr(settings, "app_env", "prod")
    monkeypatch.setattr(settings, "lab_highwater_trusted_keyring_path", keyring)
    monkeypatch.setattr(
        settings,
        "lab_highwater_authority_command_json",
        '["/tmp/runner-owned-helper","--state-root","/tmp/runner-state"]',
    )
    monkeypatch.setattr(settings, "lab_highwater_stable_identity", "runner-owned-identity")
    monkeypatch.setattr(
        report_authority,
        "load_highwater_trusted_keys",
        lambda path: {"authority-v1": public_key} if path == keyring else {},
    )
    monkeypatch.setattr(report_authority, "LabHighWaterAuthorityConfig", _config)
    monkeypatch.setattr(report_authority, "LabHighWaterAuthorityClient", _SharedClient)

    production_profile = _profile(tmp_path / "production", DailyPipelineMode.PRODUCTION)
    client = report_authority.DailyPipelineReportAuthorityClient.from_production_profile(
        code_identity="a" * 40,
        profile_identity=PROFILE_HASH,
        mode=production_profile.mode,
        namespace_id=str(production_profile.namespace_id),
    )

    assert isinstance(client, report_authority.DailyPipelineReportAuthorityClient)
    assert captured["command"] == PRODUCTION_LAB_HIGHWATER_COMMAND
    assert captured["stable_identity"] == (
        "daily-pipeline-report:daily-close:production:"
        f"{production_profile.namespace_id}:v2"
    )
    assert captured["production_mode"] is True
    assert captured["allow_identity_rotation"] is True
    assert captured["trusted_key_provider"]("authority-v1") == public_key
    assert "secret" not in captured
    assert "state_root" not in captured

    shadow_profile = _profile(tmp_path / "shadow", DailyPipelineMode.SHADOW)
    report_authority.DailyPipelineReportAuthorityClient.from_production_profile(
        code_identity="a" * 40,
        profile_identity=PROFILE_HASH,
        mode=shadow_profile.mode,
        namespace_id=str(shadow_profile.namespace_id),
    )
    assert captured["stable_identity"] == (
        "daily-pipeline-report:daily-close:shadow:"
        f"{shadow_profile.namespace_id}:v2"
    )


def test_development_environment_cannot_enable_production_report_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.config import settings
    from rquant.daily_pipeline_report_authority import (
        DailyPipelineReportAuthorityClient,
        DailyPipelineReportAuthorityError,
    )

    monkeypatch.setattr(settings, "app_env", "dev")
    profile = _profile(Path("/tmp/daily-report-test"), DailyPipelineMode.PRODUCTION)

    with pytest.raises(DailyPipelineReportAuthorityError, match="production profile"):
        DailyPipelineReportAuthorityClient.from_production_profile(
            code_identity="a" * 40,
            profile_identity=PROFILE_HASH,
            mode=profile.mode,
            namespace_id=str(profile.namespace_id),
        )


def test_independent_monotonic_authority_rejects_report_root_rollback(tmp_path: Path) -> None:
    from rquant.daily_pipeline_report_authority import (
        DailyPipelineReportAuthorityError,
        DailyPipelineReportStore,
        DailyPipelineRunReport,
    )

    profile = _profile(tmp_path, DailyPipelineMode.SHADOW)
    reports = profile.report_root

    class _AuthorityCapability:
        def __init__(self) -> None:
            self.head = None

        def compare_and_advance(self, report):
            if self.head is not None and (
                report.trade_date < self.head.trade_date
                or (
                    report.trade_date == self.head.trade_date
                    and report.report_id != self.head.report_id
                )
            ):
                raise DailyPipelineReportAuthorityError("monotonic authority refused")
            sequence = 1 if self.head is None or report.report_id == self.head.report_id else 2
            self.head = report
            return sequence

    authority = _AuthorityCapability()
    store = DailyPipelineReportStore(
        storage_profile=profile,
        authority=authority,
    )
    first = DailyPipelineRunReport.create(
        mode=profile.mode,
        profile_hash=profile.profile_hash,
        namespace_id=str(profile.namespace_id),
        run_id="daily-" + "a" * 40,
        plan_hash="b" * 64,
        trade_date=date(2026, 8, 3),
        receipt_ids=("c" * 64,),
        generated_at=datetime(2026, 8, 3, 9, 0, tzinfo=UTC),
    )
    store.publish(first)

    # Replacing only the report directory must not reset the independent CAS
    # authority or allow a different immutable report for the same session.
    reports.rename(tmp_path / "rolled-back-reports")
    reports.mkdir(mode=0o700)
    replacement = DailyPipelineRunReport.create(
        mode=profile.mode,
        profile_hash=profile.profile_hash,
        namespace_id=str(profile.namespace_id),
        run_id="daily-" + "d" * 40,
        plan_hash="e" * 64,
        trade_date=date(2026, 8, 3),
        receipt_ids=("f" * 64,),
        generated_at=datetime(2026, 8, 3, 9, 1, tzinfo=UTC),
    )

    with pytest.raises(DailyPipelineReportAuthorityError, match="binding changed"):
        store.publish(replacement)
    assert list(reports.iterdir()) == []

    reopened = DailyPipelineReportStore(
        storage_profile=profile,
        authority=authority,
    )
    with pytest.raises(DailyPipelineReportAuthorityError, match="monotonic"):
        reopened.publish(replacement)
