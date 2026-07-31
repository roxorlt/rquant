from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from threading import Event

import pytest
from pydantic import ValidationError

from rquant.runtime_service_control import (
    RuntimeServicePlane,
    RuntimeServiceStatus,
    RuntimeStepResult,
)
from rquant.runtime_service_entrypoint import (
    RuntimeServiceKind,
    RuntimeServiceManifest,
    RuntimeServiceRegistry,
    load_runtime_service_manifest,
    run_runtime_service_manifest,
)

NOW = datetime(2026, 7, 31, 1, 40, 2, tzinfo=UTC)
COMMIT = "a" * 40


def _manifest() -> RuntimeServiceManifest:
    return RuntimeServiceManifest(
        schema_version=1,
        service_id="source.market-minute",
        service_kind=RuntimeServiceKind.MARKET_MINUTE_SOURCE,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=0,
        stale_after_seconds=10,
        producer_commit=COMMIT,
        settings={"spool_root": "/srv/rquant/live"},
    )


def test_manifest_is_frozen_typed_and_fingerprinted() -> None:
    first = _manifest()
    second = RuntimeServiceManifest.model_validate(first.model_dump(mode="json"))

    assert second.manifest_fingerprint == first.manifest_fingerprint
    assert second.service_spec.producer_commit == COMMIT
    with pytest.raises(TypeError):
        first.settings["new"] = True  # type: ignore[index]
    with pytest.raises(ValidationError):
        RuntimeServiceManifest.model_validate(
            {**first.model_dump(mode="json"), "service_kind": "python.import.path"}
        )


def test_manifest_settings_are_deeply_frozen_and_forbid_secrets() -> None:
    manifest = RuntimeServiceManifest.model_validate(
        {
            **_manifest().model_dump(mode="json"),
            "settings": {"paths": {"spool": "/srv/live"}, "channels": ["minute"]},
        }
    )

    with pytest.raises(TypeError):
        manifest.settings["paths"]["spool"] = "/tmp"  # type: ignore[index]
    with pytest.raises(TypeError):
        manifest.settings["channels"][0] = "other"  # type: ignore[index]
    with pytest.raises(ValidationError, match="secret"):
        RuntimeServiceManifest.model_validate(
            {
                **_manifest().model_dump(mode="json"),
                "settings": {"provider": {"tushare_token": "do-not-store-here"}},
            }
        )


def test_manifest_loader_rejects_symlink_public_mode_and_commit_drift(tmp_path: Path) -> None:
    path = tmp_path / "service.json"
    path.write_text(_manifest().model_dump_json())
    path.chmod(0o600)

    assert load_runtime_service_manifest(path, expected_commit=COMMIT) == _manifest()
    with pytest.raises(ValueError, match="commit"):
        load_runtime_service_manifest(path, expected_commit="b" * 40)

    path.chmod(0o644)
    with pytest.raises(ValueError, match="0600"):
        load_runtime_service_manifest(path, expected_commit=COMMIT)
    path.chmod(0o600)
    linked = tmp_path / "linked.json"
    linked.symlink_to(path)
    with pytest.raises(ValueError, match="symlink"):
        load_runtime_service_manifest(linked, expected_commit=COMMIT)

    parent_link = tmp_path / "linked-parent"
    real_parent = tmp_path / "real-parent"
    real_parent.mkdir()
    nested = real_parent / "service.json"
    nested.write_text(_manifest().model_dump_json())
    nested.chmod(0o600)
    parent_link.symlink_to(real_parent, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        load_runtime_service_manifest(parent_link / nested.name, expected_commit=COMMIT)


def test_manifest_loader_reads_from_one_secure_descriptor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "service.json"
    path.write_text(_manifest().model_dump_json())
    path.chmod(0o600)

    def fail_path_read(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("path was checked or reopened after secure open")

    monkeypatch.setattr(Path, "lstat", fail_path_read)
    monkeypatch.setattr(Path, "read_bytes", fail_path_read)

    assert load_runtime_service_manifest(path, expected_commit=COMMIT) == _manifest()


def test_registered_service_runs_once_with_durable_heartbeat(tmp_path: Path) -> None:
    registry = RuntimeServiceRegistry()
    calls: list[str] = []

    def builder(manifest: RuntimeServiceManifest):
        assert manifest == _manifest()

        def step() -> RuntimeStepResult:
            calls.append("step")
            return RuntimeStepResult(
                output_sequence=7,
                processed_count=1,
                source_generations={"market_minute": "b" * 64},
            )

        return step

    registry.register(RuntimeServiceKind.MARKET_MINUTE_SOURCE, builder)
    final = run_runtime_service_manifest(
        _manifest(),
        registry=registry,
        control_root=tmp_path / "control",
        stop_event=Event(),
        max_iterations=1,
        clock=lambda: NOW,
    )

    assert calls == ["step"]
    assert final.status is RuntimeServiceStatus.STOPPED
    assert final.output_sequence == 7
    assert final.total_successes == 1


def test_duplicate_or_missing_builder_fails_before_service_start(tmp_path: Path) -> None:
    registry = RuntimeServiceRegistry()
    registry.register(
        RuntimeServiceKind.MARKET_MINUTE_SOURCE,
        lambda _manifest: lambda: RuntimeStepResult(),
    )
    assert registry.registered_kinds == (RuntimeServiceKind.MARKET_MINUTE_SOURCE,)
    with pytest.raises(ValueError, match="already registered"):
        registry.register(
            RuntimeServiceKind.MARKET_MINUTE_SOURCE,
            lambda _manifest: lambda: RuntimeStepResult(),
        )

    empty = RuntimeServiceRegistry()
    with pytest.raises(KeyError, match="builder"):
        run_runtime_service_manifest(
            _manifest(),
            registry=empty,
            control_root=tmp_path / "control",
            stop_event=Event(),
            max_iterations=1,
            clock=lambda: NOW,
        )
    assert not (tmp_path / "control" / "heartbeats").exists()


def test_loader_rejects_manifest_content_not_json(tmp_path: Path) -> None:
    path = tmp_path / "service.json"
    path.write_text(json.dumps({"schema_version": 1}))
    path.chmod(0o600)

    with pytest.raises(ValueError, match="invalid runtime service manifest"):
        load_runtime_service_manifest(path, expected_commit=COMMIT)
