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


def test_candidate_publisher_is_an_allow_listed_runtime_service_kind() -> None:
    assert RuntimeServiceKind("candidate_publisher") is RuntimeServiceKind.CANDIDATE_PUBLISHER


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


def test_manifest_defaults_to_v2_but_still_reads_legacy_v1() -> None:
    current = RuntimeServiceManifest(
        service_id="runtime.current",
        service_kind=RuntimeServiceKind.MARKET_MINUTE_SOURCE,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=1,
        stale_after_seconds=10,
        producer_commit=COMMIT,
        settings={},
    )
    legacy = RuntimeServiceManifest.model_validate(
        {**current.model_dump(mode="json"), "schema_version": 1}
    )

    assert current.schema_version == 2
    assert legacy.schema_version == 1
    with pytest.raises(ValidationError, match="schema_version"):
        RuntimeServiceManifest.model_validate(
            {**current.model_dump(mode="json"), "schema_version": 3}
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


def test_manifest_loader_binds_current_pointer_to_expected_generation(
    tmp_path: Path,
) -> None:
    generation_hash = "b" * 64
    manifest_dir = tmp_path / "generations" / generation_hash / "manifests"
    manifest_dir.mkdir(parents=True)
    path = manifest_dir / "service.json"
    path.write_text(_manifest().model_dump_json())
    path.chmod(0o600)
    current = tmp_path / "current"
    current.symlink_to(Path("generations") / generation_hash, target_is_directory=True)

    assert (
        load_runtime_service_manifest(
            current / "manifests" / "service.json",
            expected_commit=COMMIT,
            expected_generation=generation_hash,
        )
        == _manifest()
    )

    with pytest.raises(ValueError, match="generation|instance"):
        load_runtime_service_manifest(
            current / "manifests" / "service.json",
            expected_commit=COMMIT,
            expected_generation="c" * 64,
        )


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


def test_registered_service_closes_step_resources_after_stopping(tmp_path: Path) -> None:
    registry = RuntimeServiceRegistry()
    events: list[str] = []

    class CloseableStep:
        def __call__(self) -> RuntimeStepResult:
            events.append("step")
            return RuntimeStepResult()

        def close(self) -> None:
            events.append("close")

    registry.register(
        RuntimeServiceKind.MARKET_MINUTE_SOURCE,
        lambda _manifest: CloseableStep(),
    )

    run_runtime_service_manifest(
        _manifest(),
        registry=registry,
        control_root=tmp_path / "control",
        stop_event=Event(),
        max_iterations=1,
        clock=lambda: NOW,
    )

    assert events == ["step", "close"]


def test_duplicate_builder_fails_before_service_start() -> None:
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


def test_watchlist_source_is_rejected_by_a_partial_registry_before_service_start(
    tmp_path: Path,
) -> None:
    registry = RuntimeServiceRegistry()
    manifest = _manifest().model_copy(
        update={"service_kind": RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE}
    )

    with pytest.raises(
        ValueError,
        match="runtime service configuration error.*watchlist_quote_source",
    ):
        run_runtime_service_manifest(
            manifest,
            registry=registry,
            control_root=tmp_path / "control",
            stop_event=Event(),
            max_iterations=1,
            clock=lambda: NOW,
        )
    assert not (tmp_path / "control" / "heartbeats").exists()


def test_watchlist_source_does_not_fall_back_to_an_old_registry_builder(tmp_path: Path) -> None:
    registry = RuntimeServiceRegistry()
    calls: list[str] = []
    registry.register(
        RuntimeServiceKind.MARKET_MINUTE_SOURCE,
        lambda _manifest: lambda: calls.append("old-builder") or RuntimeStepResult(),
    )
    manifest = _manifest().model_copy(
        update={"service_kind": RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE}
    )

    with pytest.raises(
        ValueError,
        match="runtime service configuration error.*watchlist_quote_source",
    ):
        run_runtime_service_manifest(
            manifest,
            registry=registry,
            control_root=tmp_path / "control",
            stop_event=Event(),
            max_iterations=1,
            clock=lambda: NOW,
        )

    assert calls == []


def test_loader_rejects_manifest_content_not_json(tmp_path: Path) -> None:
    path = tmp_path / "service.json"
    path.write_text(json.dumps({"schema_version": 1}))
    path.chmod(0o600)

    with pytest.raises(ValueError, match="invalid runtime service manifest"):
        load_runtime_service_manifest(path, expected_commit=COMMIT)
