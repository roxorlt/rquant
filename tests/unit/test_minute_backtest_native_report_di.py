from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import NoReturn
from uuid import UUID

import pytest
from pydantic import ValidationError

from rquant.web.app import create_app
from rquant.web.minute_backtest_service import LazyMinuteWebService
from rquant.web.settings import WebSettings


def test_native_report_configuration_requires_the_actual_code_pair(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="configured together"):
        WebSettings(serving_root=tmp_path, minute_native_report_runtime=tmp_path / "locator.json")
    with pytest.raises(ValidationError, match="configured together"):
        WebSettings(serving_root=tmp_path, minute_native_report_expected_code_sha="a" * 40)


@pytest.mark.parametrize("path", [Path("relative.json"), Path("/private/tmp/one/../locator.json")])
def test_native_report_locator_cannot_be_relative_or_noncanonical(
    tmp_path: Path, path: Path
) -> None:
    with pytest.raises(ValidationError, match="absolute and normalized"):
        WebSettings(
            serving_root=tmp_path,
            minute_native_report_runtime=path,
            minute_native_report_expected_code_sha="a" * 40,
        )


def test_cold_openapi_does_not_open_or_create_the_native_locator(tmp_path: Path) -> None:
    locator = tmp_path / "unavailable-private-locator.json"
    settings = WebSettings(
        serving_root=tmp_path / "serving",
        minute_native_report_runtime=locator,
        minute_native_report_expected_code_sha="a" * 40,
    )
    before = tuple(tmp_path.rglob("*"))
    app = create_app(settings, clock=lambda: datetime.now(UTC), background=False)
    selected = app.state.web.minute_backtests
    assert type(selected) is LazyMinuteWebService
    assert selected.path is None and selected.native_report_path == locator
    assert selected._service is None
    schema = app.openapi()
    assert "/api/v1/backtests/minute-runtime/runs/{job_id}/report.html" in schema["paths"]
    assert selected._service is None and not locator.exists()
    assert tuple(tmp_path.rglob("*")) == before


def test_missing_private_runtime_cannot_be_misreported_as_an_installed_run() -> None:
    with pytest.raises(ValueError, match="unavailable"):
        LazyMinuteWebService(None, expected_code_sha=None, clock=lambda: datetime.now(UTC))


def test_native_service_factory_supplies_an_aware_default_clock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import minute_backtest_native_report_runtime as native
    from rquant.page_control_service import build_page_control_service_with_dependencies

    class LoaderProbeCompleteError(Exception):
        pass

    def check_clock(
        path: Path,
        *,
        expected_code_sha: str,
        clock: Callable[[], datetime],
        writable: bool,
    ) -> NoReturn:
        assert path == tmp_path / "locator.json"
        assert expected_code_sha == "a" * 40 and writable is True
        assert clock().tzinfo is UTC
        raise LoaderProbeCompleteError

    monkeypatch.delenv("RQUANT_MINUTE_REPLAY_INSTALLATION", raising=False)
    monkeypatch.setattr(native, "load_minute_native_report_runtime", check_clock)
    with pytest.raises(LoaderProbeCompleteError):
        build_page_control_service_with_dependencies(
            minute_native_report_path=tmp_path / "locator.json",
            minute_native_expected_code_sha="a" * 40,
        )


@pytest.mark.parametrize("denied", [False, True])
def test_original_report_uses_its_lab_before_a_distinct_native_lab(
    monkeypatch: pytest.MonkeyPatch, denied: bool
) -> None:
    from rquant import minute_backtest_export
    from rquant.web.minute_backtest_service import MinuteWebService
    from rquant.web.models.collaboration import ResultOwnerProof

    # Selection is isolated here. Full physical and owner gates have separate tests.
    job_id = UUID("dd590bcd-f302-443a-94c0-b6a20956f3c9")
    owner_id = "fixture-owner"
    context = SimpleNamespace(job=SimpleNamespace(spec_hash="b" * 64))
    original = SimpleNamespace(
        reader=SimpleNamespace(get_command_context=lambda job: context if job == job_id else None)
    )

    def native_must_not_replace_original(*args: object) -> NoReturn:
        raise AssertionError("the original target must use its own Lab")

    native = SimpleNamespace(
        verify_current=native_must_not_replace_original,
        reader=SimpleNamespace(get_command_context=native_must_not_replace_original),
    )
    service = object.__new__(MinuteWebService)
    service.installation, service.native_report_runtime = original, native

    def result_owner(actor: str, *, domain: str, job_id: str, spec_hash: str) -> ResultOwnerProof:
        assert actor == owner_id and domain == "minute"
        assert job_id == str(context_job_id) and spec_hash == context.job.spec_hash
        if denied:
            raise PermissionError("original current role rejects this read")
        return ResultOwnerProof(
            domain="minute",
            job_id=job_id,
            spec_hash=spec_hash,
            owner_id=actor,
            command_id=job_id,
            command_sha256="c" * 64,
            effect_sha256="d" * 64,
            worker_owner_id="original-owner-worker",
        )

    context_job_id = job_id
    gateway = SimpleNamespace(result_owner=result_owner)
    monkeypatch.setattr(
        minute_backtest_export,
        "MinuteReportReader",
        lambda installation, *, owner_authority: SimpleNamespace(
            installation=installation, owner_authority=owner_authority
        ),
    )
    if denied:
        with pytest.raises(PermissionError, match="current role"):
            service._report_reader(job_id, owner_id=owner_id, collaboration=gateway)
    else:
        selected = service._report_reader(job_id, owner_id=owner_id, collaboration=gateway)
        assert selected.installation is original and selected.owner_authority is gateway


@pytest.mark.parametrize("denied", [False, True])
def test_original_export_uses_its_lab_and_never_falls_back_after_denial(denied: bool) -> None:
    from rquant.minute_backtest_commands import ExportMinuteReplayZip, MinuteCommandWriter
    from rquant.minute_backtest_native_report_runtime import MinuteNativeReportCommandWriter
    from rquant.page_control import PageControlConsumer, _command_hash
    from rquant.web.models.collaboration import ResultOwnerProof, ResultOwnerQuery

    command = ExportMinuteReplayZip(
        command_id="c76f97e5-0c89-4df3-8a40-b46c9a5f81b8",
        requested_at=datetime.now(UTC),
        actor_id="fixture-owner",
        job_id=UUID("dd590bcd-f302-443a-94c0-b6a20956f3c9"),
        result_hash="e" * 64,
    )
    context = SimpleNamespace(job=SimpleNamespace(spec_hash="b" * 64))
    installation = SimpleNamespace(
        verify_current=lambda: None,
        reader=SimpleNamespace(
            get_command_context=lambda job: context if job == command.job_id else None
        ),
    )

    def result_owner(query: ResultOwnerQuery, *, authenticated_actor_id: str) -> ResultOwnerProof:
        assert type(query) is ResultOwnerQuery
        assert (query.domain, query.job_id, query.spec_hash) == (
            "minute",
            str(command.job_id),
            context.job.spec_hash,
        )
        assert authenticated_actor_id == command.actor_id
        if denied:
            raise PermissionError("original current role rejects this export")
        return ResultOwnerProof(
            **query.model_dump(),
            owner_id=authenticated_actor_id,
            command_id=command.command_id,
            command_sha256="c" * 64,
            effect_sha256="d" * 64,
            worker_owner_id="original-owner-worker",
        )

    authority = SimpleNamespace(_trusted_result_owner=result_owner)
    original = MinuteCommandWriter(installation)
    original.owner_authority = authority
    native = object.__new__(MinuteNativeReportCommandWriter)
    native.owner_authority = authority

    def native_must_not_replace_original(*args: object) -> NoReturn:
        raise AssertionError("the original target must use its own writer")

    native.runtime = SimpleNamespace(verify_current=native_must_not_replace_original)
    consumer = object.__new__(PageControlConsumer)
    consumer.minute_backend, consumer.minute_native_report_backend = original, native

    def actor(command_id: str, command_hash: str) -> str:
        assert command_id == command.command_id and command_hash == _command_hash(command)
        return command.actor_id

    consumer.outbox = SimpleNamespace(trusted_command_actor=actor)
    if denied:
        with pytest.raises(PermissionError, match="current role"):
            consumer._selected_minute_backend(command)
    else:
        assert consumer._selected_minute_backend(command) is original
