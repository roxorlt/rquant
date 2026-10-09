from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

import pytest

from rquant.minute_backtest_commands import (
    ExportMinuteReplayZip,
    build_minute_zip_effect,
    parse_minute_zip_effect,
    require_minute_zip_report,
)
from rquant.minute_backtest_export import MinuteVerifiedReport
from rquant.minute_backtest_report import build_minute_html_report
from tests.unit.test_minute_backtest_report import report_inputs


@pytest.fixture(scope="module")
def zip_report() -> MinuteVerifiedReport:
    sealed, owner = report_inputs()
    return MinuteVerifiedReport(
        sealed=sealed,
        owner=owner,
        report=build_minute_html_report(sealed, owner=owner, requester=sealed.owner_id),
    )


def export_command(report: MinuteVerifiedReport) -> ExportMinuteReplayZip:
    return ExportMinuteReplayZip(
        command_id="02d39111-fae2-4d4e-b4cf-bd72f0ac51b9",
        requested_at=datetime(2026, 10, 7, 18, tzinfo=UTC),
        actor_id=report.sealed.owner_id,
        job_id=report.sealed.job_id,
        result_hash=report.sealed.complete_result_hash,
    )


def test_complete_zip_marker_survives_original_strict_json_roundtrip(
    zip_report: MinuteVerifiedReport,
) -> None:
    command = export_command(zip_report)
    effect = build_minute_zip_effect(command, zip_report)
    checked = parse_minute_zip_effect(command, effect.model_dump(mode="json"))
    assert checked == effect
    require_minute_zip_report(checked, zip_report)


@pytest.mark.parametrize(
    "field,value",
    [
        ("command_id", "dafac3f6-ed19-4bc2-a5ef-e1b355d17d9f"),
        ("actor_id", "another-owner"),
        ("job_id", UUID("6b3e2385-ae3a-43f6-bd0a-27c1e747070e")),
        ("result_hash", "0" * 64),
    ],
)
def test_zip_marker_cannot_be_reused_for_another_original_command(
    zip_report: MinuteVerifiedReport,
    field: str,
    value: object,
) -> None:
    command = export_command(zip_report)
    marker = build_minute_zip_effect(command, zip_report).model_dump(mode="json")
    with pytest.raises(PermissionError, match="original command"):
        parse_minute_zip_effect(command.model_copy(update={field: value}), marker)


@pytest.mark.parametrize(
    "field",
    [
        "full_input_hash",
        "core_input_hash",
        "seed_hash",
        "html_sha256",
        "owner_binding_hash",
    ],
)
def test_zip_marker_rejects_each_changed_complete_report_binding(
    zip_report: MinuteVerifiedReport,
    field: str,
) -> None:
    effect = build_minute_zip_effect(export_command(zip_report), zip_report)
    with pytest.raises(PermissionError, match="complete source, report or original owner"):
        require_minute_zip_report(effect.model_copy(update={field: "0" * 64}), zip_report)
