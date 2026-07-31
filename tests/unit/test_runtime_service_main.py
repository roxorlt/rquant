from __future__ import annotations

from pathlib import Path
from threading import Event

import pytest

from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.runtime_service_main import build_parser, run

COMMIT = "a" * 40


def test_parser_requires_absolute_private_runtime_paths(tmp_path: Path) -> None:
    manifest = tmp_path / "source.json"
    control = tmp_path / "control"
    args = build_parser().parse_args(
        [
            "--manifest",
            str(manifest),
            "--control-root",
            str(control),
            "--expected-commit",
            COMMIT,
            "--once",
        ]
    )

    assert args.manifest == manifest
    assert args.control_root == control
    assert args.expected_commit == COMMIT
    assert args.once is True

    with pytest.raises(SystemExit):
        build_parser().parse_args(
            [
                "--manifest",
                "relative.json",
                "--control-root",
                str(control),
                "--expected-commit",
                COMMIT,
            ]
        )
    with pytest.raises(SystemExit):
        build_parser().parse_args(
            [
                "--manifest",
                str(manifest),
                "--control-root",
                "relative",
                "--expected-commit",
                COMMIT,
            ]
        )


def test_run_loads_exact_manifest_and_limits_once_mode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest_path = tmp_path / "source.json"
    control_root = tmp_path / "control"
    manifest = RuntimeServiceManifest(
        service_id="source.market-minute",
        service_kind=RuntimeServiceKind.MARKET_MINUTE_SOURCE,
        plane="live",
        interval_seconds=15,
        stale_after_seconds=45,
        producer_commit=COMMIT,
        settings={"spool_root": str(tmp_path / "spool")},
    )
    manifest_path.write_text(manifest.model_dump_json())
    manifest_path.chmod(0o600)
    observed: dict[str, object] = {}

    class _Registry:
        pass

    def fake_run(
        loaded: RuntimeServiceManifest,
        *,
        registry: object,
        control_root: Path,
        stop_event: Event,
        max_iterations: int | None,
    ) -> object:
        observed.update(
            manifest=loaded,
            registry=registry,
            control_root=control_root,
            stop_event=stop_event,
            max_iterations=max_iterations,
        )
        return object()

    registry = _Registry()
    monkeypatch.setattr("rquant.runtime_service_main.build_builtin_registry", lambda: registry)
    monkeypatch.setattr("rquant.runtime_service_main.run_runtime_service_manifest", fake_run)

    args = build_parser().parse_args(
        [
            "--manifest",
            str(manifest_path),
            "--control-root",
            str(control_root),
            "--expected-commit",
            COMMIT,
            "--once",
        ]
    )

    assert run(args) == 0
    assert observed["manifest"] == manifest
    assert observed["registry"] is registry
    assert observed["control_root"] == control_root
    assert isinstance(observed["stop_event"], Event)
    assert observed["max_iterations"] == 1
