from __future__ import annotations

from pathlib import Path
from subprocess import CompletedProcess
from threading import Event

import pytest

from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.runtime_service_main import build_parser, resolve_checkout_commit, run

COMMIT = "a" * 40
GENERATION = "b" * 64


def _write_current_manifest(tmp_path: Path, manifest: RuntimeServiceManifest) -> Path:
    manifest_dir = tmp_path / "generations" / GENERATION / "manifests"
    manifest_dir.mkdir(parents=True)
    path = manifest_dir / "source.json"
    path.write_text(manifest.model_dump_json())
    path.chmod(0o600)
    (tmp_path / "current").symlink_to(
        Path("generations") / GENERATION,
        target_is_directory=True,
    )
    return tmp_path / "current" / "manifests" / "source.json"


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
            "--expected-generation",
            GENERATION,
            "--once",
        ]
    )

    assert args.manifest == manifest
    assert args.control_root == control
    assert args.expected_commit == COMMIT
    assert args.expected_generation == GENERATION
    assert args.expected_kind is None
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
                "--expected-generation",
                GENERATION,
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
                "--expected-generation",
                GENERATION,
            ]
        )


def test_run_rejects_manifest_kind_outside_unit_allowlist(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
    manifest_path = _write_current_manifest(tmp_path, manifest)
    called = False

    def fail_run(*_args: object, **_kwargs: object) -> object:
        nonlocal called
        called = True
        return object()

    monkeypatch.setattr("rquant.runtime_service_main.run_runtime_service_manifest", fail_run)
    monkeypatch.setattr("rquant.runtime_service_main.resolve_checkout_commit", lambda: COMMIT)
    args = build_parser().parse_args(
        [
            "--manifest",
            str(manifest_path),
            "--control-root",
            str(control_root),
            "--expected-commit",
            COMMIT,
            "--expected-generation",
            GENERATION,
            "--expected-kind",
            "candidate_publisher",
            "--once",
        ]
    )

    with pytest.raises(ValueError, match="kind.*unit|unit.*kind"):
        run(args)
    assert called is False


def test_run_loads_exact_manifest_and_limits_once_mode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
    manifest_path = _write_current_manifest(tmp_path, manifest)
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
    monkeypatch.setattr("rquant.runtime_service_main.resolve_checkout_commit", lambda: COMMIT)

    args = build_parser().parse_args(
        [
            "--manifest",
            str(manifest_path),
            "--control-root",
            str(control_root),
            "--expected-commit",
            COMMIT,
            "--expected-generation",
            GENERATION,
            "--once",
        ]
    )

    assert run(args) == 0
    assert observed["manifest"] == manifest
    assert observed["registry"] is registry
    assert observed["control_root"] == control_root
    assert isinstance(observed["stop_event"], Event)
    assert observed["max_iterations"] == 1


def test_run_rejects_checkout_commit_mismatch_before_loading_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = False

    def fail_load(*_args: object, **_kwargs: object) -> object:
        nonlocal loaded
        loaded = True
        return object()

    monkeypatch.setattr(
        "rquant.runtime_service_main.resolve_checkout_commit",
        lambda: "b" * 40,
    )
    monkeypatch.setattr("rquant.runtime_service_main.load_runtime_service_manifest", fail_load)
    args = build_parser().parse_args(
        [
            "--manifest",
            str(tmp_path / "missing.json"),
            "--control-root",
            str(tmp_path / "control"),
            "--expected-commit",
            COMMIT,
            "--expected-generation",
            GENERATION,
        ]
    )

    with pytest.raises(RuntimeError, match="checkout.*commit|commit.*checkout"):
        run(args)
    assert loaded is False


def test_checkout_commit_resolver_requires_exact_clean_source_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    results = iter(
        (
            CompletedProcess(args=(), returncode=0, stdout=f"{COMMIT}\n", stderr=""),
            CompletedProcess(args=(), returncode=0, stdout="", stderr=""),
        )
    )
    calls: list[list[str]] = []

    def fake_run(command: list[str], **_kwargs: object) -> CompletedProcess[str]:
        calls.append(command)
        return next(results)

    monkeypatch.setattr("rquant.runtime_service_main.subprocess.run", fake_run)

    assert resolve_checkout_commit(tmp_path) == COMMIT
    assert calls == [
        ["/usr/bin/git", "-C", str(tmp_path), "rev-parse", "--verify", "HEAD^{commit}"],
        [
            "/usr/bin/git",
            "-C",
            str(tmp_path),
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
        ],
    ]


@pytest.mark.parametrize(
    ("revision", "status", "message"),
    [
        ("not-a-sha\n", "", "SHA|commit"),
        (f"{COMMIT}\n", " M src/rquant/runtime_builder_strategy.py\n", "dirty|clean"),
    ],
)
def test_checkout_commit_resolver_rejects_untrusted_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    revision: str,
    status: str,
    message: str,
) -> None:
    results = iter(
        (
            CompletedProcess(args=(), returncode=0, stdout=revision, stderr=""),
            CompletedProcess(args=(), returncode=0, stdout=status, stderr=""),
        )
    )
    monkeypatch.setattr(
        "rquant.runtime_service_main.subprocess.run",
        lambda *_args, **_kwargs: next(results),
    )

    with pytest.raises(RuntimeError, match=message):
        resolve_checkout_commit(tmp_path)


def test_checkout_commit_resolver_normalizes_missing_git_to_runtime_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def missing(*_args: object, **_kwargs: object) -> None:
        raise OSError("git missing")

    monkeypatch.setattr("rquant.runtime_service_main.subprocess.run", missing)

    with pytest.raises(RuntimeError, match="cannot be verified"):
        resolve_checkout_commit(tmp_path)


def test_run_loads_scoped_systemd_capabilities_after_manifest_admission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = RuntimeServiceManifest(
        service_id="source.market-minute",
        service_kind=RuntimeServiceKind.MARKET_MINUTE_SOURCE,
        plane="live",
        interval_seconds=15,
        stale_after_seconds=45,
        producer_commit=COMMIT,
        settings={"spool_root": str(tmp_path / "spool")},
    )
    events: list[object] = []
    monkeypatch.setattr("rquant.runtime_service_main.resolve_checkout_commit", lambda: COMMIT)
    monkeypatch.setattr(
        "rquant.runtime_service_main.load_runtime_service_manifest",
        lambda *_args, **_kwargs: events.append("manifest") or manifest,
    )
    monkeypatch.setattr(
        "rquant.runtime_service_main.load_systemd_runtime_capabilities",
        lambda kind, *, expected_generation: (
            events.append(("credentials", kind, expected_generation)) or {}
        ),
    )
    monkeypatch.setattr(
        "rquant.runtime_service_main.run_runtime_service_manifest",
        lambda *_args, **_kwargs: events.append("run"),
    )
    monkeypatch.setattr("rquant.runtime_service_main.build_builtin_registry", lambda: object())
    args = build_parser().parse_args(
        [
            "--manifest",
            str(tmp_path / "source.json"),
            "--control-root",
            str(tmp_path / "control"),
            "--expected-commit",
            COMMIT,
            "--expected-generation",
            GENERATION,
            "--once",
        ]
    )

    assert run(args) == 0
    assert events == [
        "manifest",
        ("credentials", RuntimeServiceKind.MARKET_MINUTE_SOURCE, GENERATION),
        "run",
    ]
