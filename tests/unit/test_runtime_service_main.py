from __future__ import annotations

import hashlib
from pathlib import Path
from subprocess import CompletedProcess
from threading import Event
from types import SimpleNamespace

import pytest

from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_deployment_profile import PRODUCTION_SHADOW_SIGNER_COMMAND
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.runtime_service_main import build_parser, resolve_checkout_commit, run
from rquant.runtime_shadow_validation import Ed25519CompletionAttestationSigner
from rquant.strict_json import canonical_json_bytes
from tests.shadow_ed25519_support import create_shadow_ed25519_test_authority

COMMIT = "a" * 40
GENERATION = "b" * 64


def _write_current_manifest(tmp_path: Path, manifest: RuntimeServiceManifest) -> Path:
    manifest_dir = tmp_path / "generations" / GENERATION / "manifests"
    manifest_dir.mkdir(parents=True)
    instance = "svc-" + hashlib.sha256(manifest.service_id.encode()).hexdigest()
    path = manifest_dir / f"{instance}.json"
    path.write_text(manifest.model_dump_json())
    path.chmod(0o600)
    (tmp_path / "current").symlink_to(
        Path("generations") / GENERATION,
        target_is_directory=True,
    )
    return tmp_path / "current" / "manifests" / f"{instance}.json"


def _strategy_manifest(tmp_path: Path) -> RuntimeServiceManifest:
    return RuntimeServiceManifest(
        service_id="strategy.n_shape.v1",
        service_kind=RuntimeServiceKind.STRATEGY_LIVE,
        plane="live",
        interval_seconds=2,
        stale_after_seconds=30,
        producer_commit=COMMIT,
        settings={
            "feature_spool_root": str(tmp_path / "live" / "features"),
            "runner_state_path": str(tmp_path / "live" / "runner.sqlite3"),
            "definition_registry_root": str(tmp_path / "external" / "definitions"),
            "strategy_registration_fingerprint": "1" * 64,
            "strategy_executable_fingerprint": "2" * 64,
            "candidate_schema_fingerprint": "3" * 64,
            "candidate_snapshot_root": str(tmp_path / "external" / "candidates"),
            "paper_broker_path": str(tmp_path / "external" / "paper.sqlite3"),
            "paper_account_id": "shadow-main",
            "candidate_max_age_seconds": 60,
            "strategy_id": "n_shape",
            "strategy_version": 1,
            "calendar_path": str(tmp_path / "authorities" / "calendar.json"),
            "calendar_expected_commit": COMMIT,
            "calendar_content_sha256": "4" * 64,
            "signal_bus_path": str(tmp_path / "live" / "signal-bus.sqlite3"),
            "routing_policy_fingerprint": "5" * 64,
            "producer_instance_id": "svc-" + hashlib.sha256(b"strategy.n_shape.v1").hexdigest(),
            "producer_version": "strategy-live/strategy.n_shape.v1/strategy-v1/commit-" + COMMIT,
        },
    )


def _shadow_profile(
    manifest: RuntimeServiceManifest,
    *,
    public_key_pem: str,
    profile_manifests: tuple[RuntimeServiceManifest, ...] | None = None,
    **updates: object,
):
    shadow = SimpleNamespace(
        completion_active_key_id="shadow-test-v1",
        completion_active_public_key_pem=public_key_pem,
        completion_previous_public_key_pems={},
        signer_command=PRODUCTION_SHADOW_SIGNER_COMMAND,
        timeout_seconds=5.0,
    )
    for name, value in updates.items():
        setattr(shadow, name, value)
    return SimpleNamespace(
        producer_commit=COMMIT,
        profile_id="p" * 64,
        manifests=profile_manifests or (manifest,),
        shadow=shadow,
    )


def _guard_no_strategy_side_effects(
    monkeypatch: pytest.MonkeyPatch,
) -> list[str]:
    events: list[str] = []

    def build_registry(**_kwargs: object) -> object:
        events.append("registry")
        raise AssertionError("registry must not be built")

    def run_manifest(*_args: object, **_kwargs: object) -> object:
        events.append("run")
        raise AssertionError("service must not run")

    def sign(*_args: object, **_kwargs: object) -> str:
        events.append("sign")
        raise AssertionError("protected signer must not be invoked")

    monkeypatch.setattr("rquant.runtime_service_main.build_builtin_registry", build_registry)
    monkeypatch.setattr("rquant.runtime_service_main.run_runtime_service_manifest", run_manifest)
    monkeypatch.setattr("rquant.runtime_shadow_validation.SecureShadowSigningClient.sign", sign)
    return events


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


def test_parser_accepts_multiple_explicit_unit_kinds(tmp_path: Path) -> None:
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
            "--expected-kind",
            "market_minute_source",
            "--expected-kind",
            "feature_live",
        ]
    )

    assert args.expected_kind == [
        RuntimeServiceKind.MARKET_MINUTE_SOURCE,
        RuntimeServiceKind.FEATURE_LIVE,
    ]


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
    monkeypatch.setattr(
        "rquant.runtime_service_main.build_builtin_registry",
        lambda *, runtime_capabilities: (
            observed.update(runtime_capabilities=dict(runtime_capabilities)) or registry
        ),
    )
    monkeypatch.setattr("rquant.runtime_service_main.run_runtime_service_manifest", fake_run)
    monkeypatch.setattr("rquant.runtime_service_main.resolve_checkout_commit", lambda: COMMIT)
    monkeypatch.setattr(
        "rquant.runtime_service_main.load_runtime_schema_service_bindings",
        lambda *_args, **_kwargs: (),
    )

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
    assert observed["runtime_capabilities"] == {}


def test_run_derives_strategy_completion_signer_from_current_shadow_profile(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = _strategy_manifest(tmp_path)
    manifest_path = _write_current_manifest(tmp_path, manifest)
    authority = create_shadow_ed25519_test_authority(tmp_path / "shadow-keys")
    public_key_pem = authority.keyring._keys[authority.keyring.active_key_id].decode("utf-8")
    profile = _shadow_profile(manifest, public_key_pem=public_key_pem)
    observed: dict[str, object] = {}

    monkeypatch.setattr("rquant.runtime_service_main.resolve_checkout_commit", lambda: COMMIT)
    monkeypatch.setattr(
        "rquant.runtime_service_main.load_current_runtime_deployment_profile",
        lambda root: observed.update(profile_root=root) or profile,
        raising=False,
    )
    monkeypatch.setattr(
        "rquant.runtime_service_main.load_runtime_schema_service_bindings",
        lambda *_args, **_kwargs: (),
    )
    monkeypatch.setattr(
        "rquant.runtime_service_main.build_builtin_registry",
        lambda **kwargs: observed.update(kwargs) or object(),
    )
    monkeypatch.setattr(
        "rquant.runtime_service_main.run_runtime_service_manifest",
        lambda *_args, **_kwargs: observed.update(ran=True),
    )

    args = build_parser().parse_args(
        [
            "--manifest",
            str(manifest_path),
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
    assert observed["profile_root"] == tmp_path
    assert observed["runtime_capabilities"] == {}
    assert observed["completion_attestation_active_key_id"] == "shadow-test-v1"
    signer = observed["completion_attestation_signer"]
    assert isinstance(signer, Ed25519CompletionAttestationSigner)
    assert signer.key_id == "shadow-test-v1"
    assert observed["ran"] is True


def test_run_rejects_strategy_without_profile_shadow_signer_before_registry_build(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = _strategy_manifest(tmp_path)
    manifest_path = _write_current_manifest(tmp_path, manifest)
    registry_built = False

    monkeypatch.setattr("rquant.runtime_service_main.resolve_checkout_commit", lambda: COMMIT)
    monkeypatch.setattr(
        "rquant.runtime_service_main.load_current_runtime_deployment_profile",
        lambda _root: SimpleNamespace(manifests=(manifest,), shadow=None),
        raising=False,
    )
    monkeypatch.setattr(
        "rquant.runtime_service_main.load_runtime_schema_service_bindings",
        lambda *_args, **_kwargs: (),
    )

    def build_registry(**_kwargs: object) -> object:
        nonlocal registry_built
        registry_built = True
        return object()

    monkeypatch.setattr("rquant.runtime_service_main.build_builtin_registry", build_registry)

    args = build_parser().parse_args(
        [
            "--manifest",
            str(manifest_path),
            "--control-root",
            str(tmp_path / "control"),
            "--expected-commit",
            COMMIT,
            "--expected-generation",
            GENERATION,
            "--once",
        ]
    )

    with pytest.raises(ValueError, match="Shadow.*signer|completion signer"):
        run(args)
    assert registry_built is False


def test_run_rejects_strategy_shadow_profile_with_free_helper_injection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = _strategy_manifest(tmp_path)
    manifest_path = _write_current_manifest(tmp_path, manifest)
    authority = create_shadow_ed25519_test_authority(tmp_path / "shadow-keys")
    public_key_pem = authority.keyring._keys[authority.keyring.active_key_id].decode("utf-8")
    profile = _shadow_profile(
        manifest,
        public_key_pem=public_key_pem,
        signer_command=("/usr/bin/python3", "/tmp/free-shadow-helper.py"),
    )

    monkeypatch.setattr("rquant.runtime_service_main.resolve_checkout_commit", lambda: COMMIT)
    monkeypatch.setattr(
        "rquant.runtime_service_main.load_current_runtime_deployment_profile",
        lambda _root: profile,
        raising=False,
    )
    monkeypatch.setattr(
        "rquant.runtime_service_main.load_runtime_schema_service_bindings",
        lambda *_args, **_kwargs: (),
    )

    args = build_parser().parse_args(
        [
            "--manifest",
            str(manifest_path),
            "--control-root",
            str(tmp_path / "control"),
            "--expected-commit",
            COMMIT,
            "--expected-generation",
            GENERATION,
            "--once",
        ]
    )

    with pytest.raises(ValueError, match="fixed protected"):
        run(args)


def test_run_rejects_strategy_shadow_profile_with_malformed_completion_key(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = _strategy_manifest(tmp_path)
    manifest_path = _write_current_manifest(tmp_path, manifest)
    profile = _shadow_profile(manifest, public_key_pem="not an ed25519 public key")

    monkeypatch.setattr("rquant.runtime_service_main.resolve_checkout_commit", lambda: COMMIT)
    monkeypatch.setattr(
        "rquant.runtime_service_main.load_current_runtime_deployment_profile",
        lambda _root: profile,
        raising=False,
    )
    monkeypatch.setattr(
        "rquant.runtime_service_main.load_runtime_schema_service_bindings",
        lambda *_args, **_kwargs: (),
    )

    args = build_parser().parse_args(
        [
            "--manifest",
            str(manifest_path),
            "--control-root",
            str(tmp_path / "control"),
            "--expected-commit",
            COMMIT,
            "--expected-generation",
            GENERATION,
            "--once",
        ]
    )

    with pytest.raises(ValueError, match="Ed25519|public key"):
        run(args)


@pytest.mark.parametrize("profile_case", ["duplicate", "mismatched-fingerprint"])
def test_run_rejects_strategy_profile_manifest_binding_drift_before_registry_build(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    profile_case: str,
) -> None:
    manifest = _strategy_manifest(tmp_path)
    manifest_path = _write_current_manifest(tmp_path, manifest)
    authority = create_shadow_ed25519_test_authority(tmp_path / "shadow-keys")
    public_key_pem = authority.keyring._keys[authority.keyring.active_key_id].decode("utf-8")
    if profile_case == "duplicate":
        profile_manifests = (manifest, manifest)
    else:
        replaced = manifest.model_copy(
            update={"stale_after_seconds": manifest.stale_after_seconds + 1}
        )
        profile_manifests = (replaced,)
    profile = _shadow_profile(
        manifest,
        public_key_pem=public_key_pem,
        profile_manifests=profile_manifests,
    )
    events = _guard_no_strategy_side_effects(monkeypatch)

    monkeypatch.setattr("rquant.runtime_service_main.resolve_checkout_commit", lambda: COMMIT)
    monkeypatch.setattr(
        "rquant.runtime_service_main.load_current_runtime_deployment_profile",
        lambda _root: profile,
        raising=False,
    )
    monkeypatch.setattr(
        "rquant.runtime_service_main.load_runtime_schema_service_bindings",
        lambda *_args, **_kwargs: (),
    )

    args = build_parser().parse_args(
        [
            "--manifest",
            str(manifest_path),
            "--control-root",
            str(tmp_path / "control"),
            "--expected-commit",
            COMMIT,
            "--expected-generation",
            GENERATION,
            "--expected-kind",
            RuntimeServiceKind.STRATEGY_LIVE.value,
            "--once",
        ]
    )

    with pytest.raises(ValueError, match="bind this manifest"):
        run(args)
    assert events == []


def test_run_rejects_malformed_previous_completion_key_before_registry_build(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = _strategy_manifest(tmp_path)
    manifest_path = _write_current_manifest(tmp_path, manifest)
    authority = create_shadow_ed25519_test_authority(tmp_path / "shadow-keys")
    public_key_pem = authority.keyring._keys[authority.keyring.active_key_id].decode("utf-8")
    profile = _shadow_profile(
        manifest,
        public_key_pem=public_key_pem,
        completion_previous_public_key_pems={
            "shadow-old-v1": "this is not PEM encoded Ed25519 public key material"
        },
    )
    events = _guard_no_strategy_side_effects(monkeypatch)

    monkeypatch.setattr("rquant.runtime_service_main.resolve_checkout_commit", lambda: COMMIT)
    monkeypatch.setattr(
        "rquant.runtime_service_main.load_current_runtime_deployment_profile",
        lambda _root: profile,
        raising=False,
    )
    monkeypatch.setattr(
        "rquant.runtime_service_main.load_runtime_schema_service_bindings",
        lambda *_args, **_kwargs: (),
    )

    args = build_parser().parse_args(
        [
            "--manifest",
            str(manifest_path),
            "--control-root",
            str(tmp_path / "control"),
            "--expected-commit",
            COMMIT,
            "--expected-generation",
            GENERATION,
            "--expected-kind",
            RuntimeServiceKind.STRATEGY_LIVE.value,
            "--once",
        ]
    )

    with pytest.raises(ValueError, match="invalid Ed25519 public key material"):
        run(args)
    assert events == []


def test_run_injects_lazy_terminal_lifecycle_into_standard_owner_assembly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = RuntimeServiceManifest(
        service_id="lab-jobs.serving.v1",
        service_kind=RuntimeServiceKind.LAB_JOBS_PUBLISHER,
        plane="research",
        interval_seconds=30,
        stale_after_seconds=120,
        producer_commit=COMMIT,
        settings={
            "lab_jobs_path": str(tmp_path / "research" / "lab_jobs.sqlite3"),
            "authority_root": str(tmp_path / "research" / "authority"),
        },
    )
    manifest_path = _write_current_manifest(tmp_path, manifest)
    events: list[object] = []
    sentinel = object()

    monkeypatch.setattr("rquant.runtime_service_main.resolve_checkout_commit", lambda: COMMIT)
    monkeypatch.setattr(
        "rquant.runtime_service_main.build_runtime_artifact_terminal_lifecycle_factory",
        lambda root, *, service_kind: (
            events.append(("factory", root, service_kind)) or (lambda: sentinel)
        ),
    )
    monkeypatch.setattr(
        "rquant.runtime_service_main.load_runtime_schema_service_bindings",
        lambda *_args, **_kwargs: (),
    )
    monkeypatch.setattr(
        "rquant.runtime_service_main.run_runtime_service_manifest",
        lambda *_args, **_kwargs: events.append("run"),
    )

    def build_registry(
        *,
        runtime_capabilities: object,
        artifact_terminal_lifecycle_factory: object,
    ) -> object:
        assert runtime_capabilities == {}
        assert callable(artifact_terminal_lifecycle_factory)
        assert artifact_terminal_lifecycle_factory() is sentinel
        events.append("registry")
        return object()

    monkeypatch.setattr("rquant.runtime_service_main.build_builtin_registry", build_registry)
    args = build_parser().parse_args(
        [
            "--manifest",
            str(manifest_path),
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
        ("factory", tmp_path, RuntimeServiceKind.LAB_JOBS_PUBLISHER),
        "registry",
        "run",
    ]


def test_run_injects_manifest_bound_descriptor_schema_resolver_for_retention(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema_root = tmp_path / "schema-authority"
    schema_root.mkdir(mode=0o700)
    artifact = tmp_path / "artifact.bin"
    artifact.write_bytes(b"entrypoint trusted artifact")
    artifact.chmod(0o600)
    binding = {
        "content_sha256": hashlib.sha256(artifact.read_bytes()).hexdigest(),
        "size_bytes": artifact.stat().st_size,
        "schema_sha256": "7" * 64,
    }
    authority = {"schema_version": 1, "bindings": [binding]}
    authority["authority_id"] = canonical_sha256(authority)
    payload = canonical_json_bytes(authority)
    authority_path = schema_root / "descriptor-schema-authority.json"
    authority_path.write_bytes(payload)
    authority_path.chmod(0o600)
    manifest = RuntimeServiceManifest(
        service_id="artifact-retention.primary.v1",
        service_kind=RuntimeServiceKind.ARTIFACT_RETENTION,
        plane="research",
        interval_seconds=300,
        stale_after_seconds=900,
        producer_commit=COMMIT,
        settings={
            "schema_authority_root": str(schema_root),
            "schema_authority_path": str(authority_path),
            "schema_authority_sha256": hashlib.sha256(payload).hexdigest(),
        },
    )
    manifest_path = _write_current_manifest(tmp_path, manifest)
    observed: dict[str, object] = {}

    def fake_registry(
        *,
        runtime_capabilities: object,
        artifact_retention_schema_resolver: object,
        artifact_terminal_lifecycle_factory: object,
    ) -> object:
        observed["capabilities"] = runtime_capabilities
        observed["resolver"] = artifact_retention_schema_resolver
        observed["lifecycle_factory"] = artifact_terminal_lifecycle_factory
        return object()

    monkeypatch.setattr("rquant.runtime_service_main.resolve_checkout_commit", lambda: COMMIT)
    monkeypatch.setattr("rquant.runtime_service_main.build_builtin_registry", fake_registry)
    monkeypatch.setattr(
        "rquant.runtime_service_main.load_runtime_schema_service_bindings",
        lambda *_args, **_kwargs: (),
    )
    monkeypatch.setattr(
        "rquant.runtime_service_main.run_runtime_service_manifest",
        lambda *_args, **_kwargs: object(),
    )
    args = build_parser().parse_args(
        [
            "--manifest",
            str(manifest_path),
            "--control-root",
            str(tmp_path / "control"),
            "--expected-commit",
            COMMIT,
            "--expected-generation",
            GENERATION,
            "--expected-kind",
            RuntimeServiceKind.ARTIFACT_RETENTION.value,
            "--once",
        ]
    )

    assert run(args) == 0
    resolver = observed["resolver"]
    assert callable(observed["lifecycle_factory"])
    with artifact.open("rb") as handle:
        assert callable(resolver)
        assert resolver(handle.fileno()) == "7" * 64


def test_run_installs_hash_bound_schema_bindings_for_the_service_lifetime(
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
    manifest_path = _write_current_manifest(tmp_path, manifest)
    binding = object()
    events: list[object] = []

    class _Context:
        def __enter__(self) -> None:
            events.append("context-enter")

        def __exit__(self, *_args: object) -> None:
            events.append("context-exit")

    monkeypatch.setattr("rquant.runtime_service_main.resolve_checkout_commit", lambda: COMMIT)
    monkeypatch.setattr(
        "rquant.runtime_service_main.load_runtime_schema_service_bindings",
        lambda root, *, manifest, generation_id, observed_at: (
            events.append(("bindings", root, manifest.service_id, generation_id, observed_at))
            or (binding,)
        ),
    )
    monkeypatch.setattr(
        "rquant.runtime_service_main.runtime_schema_dual_write_context",
        lambda bindings: events.append(("context", bindings)) or _Context(),
    )
    monkeypatch.setattr(
        "rquant.runtime_service_main.run_runtime_service_manifest",
        lambda *_args, **_kwargs: events.append("run"),
    )
    monkeypatch.setattr(
        "rquant.runtime_service_main.build_builtin_registry",
        lambda **_kwargs: object(),
    )
    args = build_parser().parse_args(
        [
            "--manifest",
            str(manifest_path),
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
    binding_event = next(
        event for event in events if isinstance(event, tuple) and event[0] == "bindings"
    )
    assert binding_event[1] == tmp_path
    assert binding_event[2:4] == (manifest.service_id, GENERATION)
    assert events[-4:] == [("context", (binding,)), "context-enter", "run", "context-exit"]


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
        lambda kind, *, expected_service_id, expected_instance, expected_generation: (
            events.append(
                (
                    "credentials",
                    kind,
                    expected_service_id,
                    expected_instance,
                    expected_generation,
                )
            )
            or {"TUSHARE_TOKEN_MAIN": "secret"}
        ),
    )
    monkeypatch.setattr(
        "rquant.runtime_service_main.run_runtime_service_manifest",
        lambda *_args, **_kwargs: events.append("run"),
    )
    monkeypatch.setattr(
        "rquant.runtime_service_main.build_builtin_registry",
        lambda *, runtime_capabilities: (
            events.append(("registry", dict(runtime_capabilities))) or object()
        ),
    )
    instance = "svc-" + hashlib.sha256(manifest.service_id.encode()).hexdigest()
    args = build_parser().parse_args(
        [
            "--manifest",
            str(tmp_path / f"{instance}.json"),
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
        (
            "credentials",
            RuntimeServiceKind.MARKET_MINUTE_SOURCE,
            manifest.service_id,
            instance,
            GENERATION,
        ),
        ("registry", {"TUSHARE_TOKEN_MAIN": "secret"}),
        "run",
    ]
