from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pytest

import rquant.runtime_builder_shadow as builder_module
from rquant.legacy_shadow_export import (
    LegacyShadowExportUnavailableError,
    LegacyShadowRunnerManifestBinding,
)
from rquant.runtime_builder_shadow import shadow_session_builder
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.runtime_shadow_job import ShadowInputUnavailableError

COMMIT = "a" * 40
TRADE_DATE = date(2026, 8, 3)


class _UnavailableInputs:
    def load(
        self,
        *,
        settings: object,
        trade_date: date,
        expected_export_commit: str,
    ) -> object:
        del settings, trade_date, expected_export_commit
        raise LegacyShadowExportUnavailableError("legacy shadow export batch is unavailable")


def _calendar(
    path: Path,
    *,
    producer_commit: str = COMMIT,
) -> MarketCalendarAuthority:
    authority = MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit=producer_commit,
        coverage_start=TRADE_DATE,
        coverage_end=TRADE_DATE,
        open_dates=(TRADE_DATE,),
        generated_at=datetime(2026, 8, 1, 0, 0, tzinfo=UTC),
    )
    path.write_text(authority.model_dump_json(), encoding="utf-8")
    path.chmod(0o600)
    return authority


def _manifest(tmp_path: Path, calendar: MarketCalendarAuthority) -> RuntimeServiceManifest:
    runner_manifest_bindings = [
        LegacyShadowRunnerManifestBinding.create(
            strategy_id=strategy_id,
            strategy_version=1,
            producer_manifest_fingerprint=fingerprint * 64,
            producer_commit=COMMIT,
            producer_service_id=f"strategy.{strategy_id}.v1",
            producer_instance_id=f"strategy-{strategy_id}-primary",
            producer_version=f"strategy-live-{strategy_id}-v1",
            strategy_registration_fingerprint=registration * 64,
            strategy_spec_fingerprint="5" * 64,
            evaluator_contract_fingerprint=executable * 64,
            executable_fingerprint=executable * 64,
        ).model_dump(mode="json")
        for strategy_id, registration, executable, fingerprint in (
            ("n_shape", "1", "2", "6"),
            ("growth_board_surge", "3", "4", "7"),
        )
    ]
    return RuntimeServiceManifest(
        service_id="shadow.session.production.v1",
        service_kind=RuntimeServiceKind.SHADOW_SESSION,
        plane=RuntimeServicePlane.RESEARCH,
        interval_seconds=300,
        stale_after_seconds=172_800,
        producer_commit=COMMIT,
        settings={
            "report_root": str((tmp_path / "reports").resolve()),
            "legacy_monitor_root": str((tmp_path / "legacy-shadow" / "monitor").resolve()),
            "legacy_surge_root": str((tmp_path / "legacy-shadow" / "surge").resolve()),
            "isolated_runner_root": str(
                (tmp_path / "legacy-shadow" / "isolated-runners").resolve()
            ),
            "calendar_path": str((tmp_path / "calendar.json").resolve()),
            "calendar_expected_commit": calendar.producer_commit,
            "calendar_content_sha256": calendar.content_sha256,
            "completion_active_key_id": "completion-v1",
            "completion_active_public_key_pem": "not-used-before-input-load",
            "report_active_key_id": "report-v1",
            "report_active_public_key_pem": "not-used-before-input-load",
            "signer_command": [
                "/usr/bin/sudo",
                "-n",
                "/usr/local/libexec/rquant-shadow-report-signer",
            ],
            "report_producer_service_id": "shadow.session.production.v1",
            "report_producer_instance_id": "shadow-primary",
            "signer_timeout_seconds": 1.0,
            "producer_version": "shadow-session-production-v1",
            "match_tolerance_microseconds": 60_000_000,
            "mode": "shadow",
            "strategy_bindings": [
                {
                    "strategy_id": "n_shape",
                    "strategy_version": 1,
                    "definition_fingerprint": "1" * 64,
                    "executable_fingerprint": "2" * 64,
                },
                {
                    "strategy_id": "growth_board_surge",
                    "strategy_version": 1,
                    "definition_fingerprint": "3" * 64,
                    "executable_fingerprint": "4" * 64,
                },
            ],
            "runner_manifest_bindings": runner_manifest_bindings,
        },
    )


def test_shadow_service_marks_incomplete_legacy_export_degraded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calendar = _calendar(tmp_path / "calendar.json")
    monkeypatch.setattr(
        builder_module,
        "Ed25519CompletionAttestationKeyring",
        lambda **_kwargs: object(),
    )
    monkeypatch.setattr(
        builder_module,
        "Ed25519ShadowReceiptKeyring",
        lambda **_kwargs: object(),
    )
    monkeypatch.setattr(builder_module, "SecureShadowSigningClient", lambda **_kwargs: object())
    monkeypatch.setattr(
        builder_module,
        "Ed25519ShadowReceiptSigner",
        lambda **_kwargs: object(),
    )

    step = shadow_session_builder(
        clock=lambda: datetime(2026, 8, 3, 7, 5, tzinfo=UTC),
        input_loader=_UnavailableInputs(),
    )(_manifest(tmp_path, calendar))

    result = step()

    assert result.processed_count == 0
    assert result.degraded_reasons == ("shadow:legacy_export_unavailable",)


def test_shadow_builder_passes_manifest_commit_to_export_loader_not_calendar_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calendar = _calendar(tmp_path / "calendar.json", producer_commit="b" * 40)
    observed: list[str] = []

    class _CaptureUnavailable:
        def load(
            self,
            *,
            settings: object,
            trade_date: date,
            expected_export_commit: str,
        ) -> object:
            del settings, trade_date
            observed.append(expected_export_commit)
            raise LegacyShadowExportUnavailableError("missing export")

    monkeypatch.setattr(
        builder_module,
        "Ed25519CompletionAttestationKeyring",
        lambda **_kwargs: object(),
    )
    monkeypatch.setattr(
        builder_module,
        "Ed25519ShadowReceiptKeyring",
        lambda **_kwargs: object(),
    )
    monkeypatch.setattr(builder_module, "SecureShadowSigningClient", lambda **_kwargs: object())
    monkeypatch.setattr(
        builder_module,
        "Ed25519ShadowReceiptSigner",
        lambda **_kwargs: object(),
    )
    step = shadow_session_builder(
        clock=lambda: datetime(2026, 8, 3, 7, 5, tzinfo=UTC),
        input_loader=_CaptureUnavailable(),
    )(_manifest(tmp_path, calendar))

    result = step()

    assert observed == [COMMIT]
    assert result.degraded_reasons == ("shadow:legacy_export_unavailable",)


def test_shadow_builder_maps_calendar_binding_failure_to_degraded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calendar = _calendar(tmp_path / "calendar.json")
    manifest = _manifest(tmp_path, calendar)
    settings = dict(manifest.settings)
    settings["calendar_content_sha256"] = "f" * 64
    forged = manifest.model_copy(update={"settings": settings})
    monkeypatch.setattr(
        builder_module,
        "Ed25519CompletionAttestationKeyring",
        lambda **_kwargs: object(),
    )
    monkeypatch.setattr(
        builder_module,
        "Ed25519ShadowReceiptKeyring",
        lambda **_kwargs: object(),
    )
    monkeypatch.setattr(builder_module, "SecureShadowSigningClient", lambda **_kwargs: object())
    monkeypatch.setattr(
        builder_module,
        "Ed25519ShadowReceiptSigner",
        lambda **_kwargs: object(),
    )
    step = shadow_session_builder(
        clock=lambda: datetime(2026, 8, 3, 7, 5, tzinfo=UTC),
        input_loader=_UnavailableInputs(),
    )(forged)

    result = step()

    assert result.processed_count == 0
    assert result.degraded_reasons == ("shadow:legacy_export_unavailable",)


class _LoadedInputs:
    def load(
        self,
        *,
        settings: object,
        trade_date: date,
        expected_export_commit: str,
    ) -> object:
        del settings, trade_date, expected_export_commit
        return type(
            "Inputs",
            (),
            {
                "monitor_rows": (),
                "monitor_completion_receipt": object(),
                "surge_events_path": Path("/not-read"),
                "surge_completion_receipt": object(),
                "runner_sources": (),
            },
        )()


@pytest.mark.parametrize(
    ("failure", "degraded"),
    (
        (LegacyShadowExportUnavailableError("receipt invalid"), True),
        (ShadowInputUnavailableError("receipt invalid"), True),
        (ValueError("internal executor bug"), False),
    ),
)
def test_shadow_service_degrades_only_external_artifact_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: Exception,
    degraded: bool,
) -> None:
    calendar = _calendar(tmp_path / "calendar.json")
    monkeypatch.setattr(
        builder_module,
        "Ed25519CompletionAttestationKeyring",
        lambda **_kwargs: object(),
    )
    monkeypatch.setattr(
        builder_module,
        "Ed25519ShadowReceiptKeyring",
        lambda **_kwargs: object(),
    )
    monkeypatch.setattr(builder_module, "SecureShadowSigningClient", lambda **_kwargs: object())
    monkeypatch.setattr(
        builder_module,
        "Ed25519ShadowReceiptSigner",
        lambda **_kwargs: object(),
    )
    monkeypatch.setattr(builder_module, "_validate_isolated_inputs", lambda *_args, **_kwargs: None)

    def fail(**_kwargs: object) -> object:
        raise failure

    step = shadow_session_builder(
        clock=lambda: datetime(2026, 8, 3, 7, 5, tzinfo=UTC),
        input_loader=_LoadedInputs(),
        session_executor=fail,
    )(_manifest(tmp_path, calendar))

    if not degraded:
        with pytest.raises(ValueError, match="internal executor bug"):
            step()
        return
    result = step()
    assert result.processed_count == 0
    assert result.degraded_reasons == ("shadow:legacy_export_unavailable",)
