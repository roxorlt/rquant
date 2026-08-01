from __future__ import annotations

import hashlib
import json
import os
import stat
from pathlib import Path

import pytest

from rquant.runtime_deployment_bundle import install_runtime_deployment_bundle
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import (
    RuntimeServiceKind,
    RuntimeServiceManifest,
)

COMMIT = "a" * 40


@pytest.fixture(autouse=True)
def isolated_root_credential_sealer(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "rquant.runtime_deployment_bundle._seal_runtime_credentials",
        lambda _credentials: None,
    )


def _manifest(
    root: Path,
    *,
    service_id: str,
    kind: RuntimeServiceKind,
    plane: RuntimeServicePlane,
    interval_seconds: float = 1,
) -> RuntimeServiceManifest:
    instance = "svc-" + hashlib.sha256(service_id.encode("utf-8")).hexdigest()
    if kind is RuntimeServiceKind.MARKET_MINUTE_SOURCE:
        settings: dict[str, object] = {
            "spool_root": str(root / "live" / "market-minute"),
            "quota_path": str(root / "live" / "quota.sqlite"),
        }
    elif kind is RuntimeServiceKind.CANDIDATE_PUBLISHER:
        settings = {
            "strategy_id": "n_shape",
            "strategy_version": 1,
            "candidate_input_path": str(root.parent / "inputs" / "n-shape.json"),
            "snapshot_root": str(root / "live" / "candidates" / instance),
        }
    elif kind is RuntimeServiceKind.STRATEGY_LIVE:
        settings = {
            "feature_spool_root": str(root / "live" / "features"),
            "runner_state_path": str(root / "live" / "strategies" / instance / "runner.sqlite3"),
            "strategy_spec_path": str(root.parent / "specs" / "n-shape.json"),
            "strategy_spec_sha256": "9" * 64,
            "candidate_snapshot_root": str(root / "live" / "candidates" / "source"),
            "candidate_max_age_seconds": 120,
            "strategy_id": "n_shape",
            "strategy_version": 1,
            "batch_limit": 128,
        }
    elif kind is RuntimeServiceKind.NOTIFIER:
        settings = {"signal_bus_path": str(root / "live" / "signal.sqlite")}
    elif kind is RuntimeServiceKind.SERVING_PUBLISHER:
        settings = {"serving_root": str(root / "serving")}
    else:
        settings = {}
    return RuntimeServiceManifest(
        service_id=service_id,
        service_kind=kind,
        plane=plane,
        interval_seconds=interval_seconds,
        stale_after_seconds=30,
        producer_commit=COMMIT,
        settings=settings,
    )


def _bundle_inputs(
    root: Path,
) -> tuple[
    tuple[RuntimeServiceManifest, ...],
    dict[str, dict[str, str]],
]:
    manifests = (
        _manifest(
            root,
            service_id="minute/source:primary",
            kind=RuntimeServiceKind.MARKET_MINUTE_SOURCE,
            plane=RuntimeServicePlane.LIVE,
        ),
        _manifest(
            root,
            service_id="candidate-n-shape",
            kind=RuntimeServiceKind.CANDIDATE_PUBLISHER,
            plane=RuntimeServicePlane.LIVE,
        ),
        _manifest(
            root,
            service_id="notifier-admin",
            kind=RuntimeServiceKind.NOTIFIER,
            plane=RuntimeServicePlane.LIVE,
        ),
        _manifest(
            root,
            service_id="serving-publisher",
            kind=RuntimeServiceKind.SERVING_PUBLISHER,
            plane=RuntimeServicePlane.SERVING,
        ),
    )
    capabilities = {
        "minute/source:primary": {
            "TUSHARE_TOKEN_MAIN": "main-token",
            "TUSHARE_TOKEN_BACKUP": "backup-token",
        },
        "candidate-n-shape": {},
        "notifier-admin": {
            "PUSHDEER_KEYS": "pushdeer-key",
            "PUSHPLUS_TOKENS": "pushplus-token",
            "PUSHDEER_ENDPOINT": "https://pushdeer.invalid/send",
            "PUSHPLUS_ENDPOINT": "https://pushplus.invalid/send",
        },
        "serving-publisher": {},
    }
    return manifests, capabilities


def test_candidate_bundle_owns_only_snapshot_output_and_receives_no_secrets(
    tmp_path: Path,
) -> None:
    root = tmp_path / "runtime"
    manifest = _manifest(
        root,
        service_id="candidate-n-shape",
        kind=RuntimeServiceKind.CANDIDATE_PUBLISHER,
        plane=RuntimeServicePlane.LIVE,
    )

    receipt = install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=(manifest,),
        capability_env={manifest.service_id: {}},
    )

    assert Path(manifest.settings["candidate_input_path"]).is_relative_to(tmp_path / "inputs")
    instance = receipt.instance_mapping[manifest.service_id]
    assert Path(manifest.settings["snapshot_root"]) == root / "live" / "candidates" / instance
    assert instance.startswith("svc-")
    assert receipt.unit_mapping[manifest.service_id] == (
        f"rquant-runtime-candidate@{instance}.service"
    )
    assert not (root / "current" / "credentials").exists()
    assert Path(manifest.settings["snapshot_root"]).is_dir()
    assert (root / "control" / "candidates" / instance).is_dir()

    overprivileged = {manifest.service_id: {"TUSHARE_TOKEN_MAIN": "forbidden"}}
    with pytest.raises(ValueError, match="unknown capability"):
        install_runtime_deployment_bundle(
            tmp_path / "other-runtime",
            producer_commit=COMMIT,
            manifests=(
                manifest.model_copy(
                    update={
                        "settings": {
                            **manifest.model_dump(mode="json")["settings"],
                            "snapshot_root": str(
                                tmp_path / "other-runtime" / "live" / "candidates" / instance
                            ),
                        }
                    }
                ),
            ),
            capability_env=overprivileged,
        )


def test_candidate_bundle_rejects_snapshot_output_outside_live_plane(
    tmp_path: Path,
) -> None:
    root = tmp_path / "runtime"
    manifest = _manifest(
        root,
        service_id="candidate-n-shape",
        kind=RuntimeServiceKind.CANDIDATE_PUBLISHER,
        plane=RuntimeServicePlane.LIVE,
    ).model_copy(
        update={
            "settings": {
                "strategy_id": "n_shape",
                "strategy_version": 1,
                "candidate_input_path": str(tmp_path / "input.json"),
                "snapshot_root": str(root / "serving" / "candidates"),
            }
        }
    )

    with pytest.raises(ValueError, match="owned by the live plane"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=(manifest,),
            capability_env={manifest.service_id: {}},
        )


@pytest.mark.parametrize(
    "candidate_input_path",
    (
        "inside-live",
        "inside-control",
        "relative",
        "traversal",
    ),
)
def test_candidate_bundle_rejects_unsafe_readonly_input_path(
    tmp_path: Path,
    candidate_input_path: str,
) -> None:
    root = tmp_path / "runtime"
    if candidate_input_path == "inside-live":
        input_path = root / "live" / "inputs" / "n-shape.json"
        message = "read-only|writable|live"
    elif candidate_input_path == "inside-control":
        input_path = root / "control" / "inputs" / "n-shape.json"
        message = "read-only|writable|control"
    elif candidate_input_path == "relative":
        input_path = Path("inputs/n-shape.json")
        message = "absolute|normalized"
    else:
        input_path = tmp_path / "inputs" / ".." / "inputs" / "n-shape.json"
        message = "absolute|normalized"
    manifest = _manifest(
        root,
        service_id="candidate-n-shape",
        kind=RuntimeServiceKind.CANDIDATE_PUBLISHER,
        plane=RuntimeServicePlane.LIVE,
    ).model_copy(
        update={
            "settings": {
                "strategy_id": "n_shape",
                "strategy_version": 1,
                "candidate_input_path": str(input_path),
                "snapshot_root": str(
                    root
                    / "live"
                    / "candidates"
                    / ("svc-" + hashlib.sha256(b"candidate-n-shape").hexdigest())
                ),
            }
        }
    )

    with pytest.raises(ValueError, match=message):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=(manifest,),
            capability_env={manifest.service_id: {}},
        )


def _mode(path: Path) -> int:
    return stat.S_IMODE(path.stat().st_mode)


def test_installs_canonical_generation_with_systemd_instance_mapping(
    tmp_path: Path,
) -> None:
    root = tmp_path / "runtime"
    manifests, capabilities = _bundle_inputs(root)

    receipt = install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=manifests,
        capability_env=capabilities,
    )

    assert receipt.producer_commit == COMMIT
    assert len(receipt.generation_hash) == 64
    assert receipt.instance_mapping == {
        manifest.service_id: "svc-"
        + hashlib.sha256(manifest.service_id.encode("utf-8")).hexdigest()
        for manifest in manifests
    }
    assert receipt.unit_mapping == {
        manifest.service_id: (
            f"rquant-runtime-candidate@{receipt.instance_mapping[manifest.service_id]}.service"
            if manifest.service_kind is RuntimeServiceKind.CANDIDATE_PUBLISHER
            else (
                f"rquant-runtime-{manifest.plane.value}@"
                f"{receipt.instance_mapping[manifest.service_id]}.service"
            )
        )
        for manifest in manifests
    }
    current = root / "current"
    assert current.is_symlink()
    assert os.readlink(current) == f"generations/{receipt.generation_hash}"
    generation = current.resolve(strict=True)
    assert generation.parent == root / "generations"
    assert _mode(generation) == 0o700
    assert (generation / "runtime.env").read_text() == (
        f"RQUANT_RUNTIME_COMMIT={COMMIT}\nRQUANT_RUNTIME_GENERATION={receipt.generation_hash}\n"
    )
    assert _mode(generation / "runtime.env") == 0o600

    for manifest in manifests:
        instance = receipt.instance_mapping[manifest.service_id]
        assert instance.startswith("svc-")
        assert len(instance) == 68
        manifest_path = generation / "manifests" / f"{instance}.json"
        assert _mode(manifest_path) == 0o600
        assert manifest_path.read_bytes() == json.dumps(
            manifest.model_dump(mode="json"),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")

    manifest_payload = b"".join(
        path.read_bytes() for path in sorted((generation / "manifests").iterdir())
    )
    for secret in (b"main-token", b"pushdeer-key", b"pushplus-token"):
        assert secret not in manifest_payload
        assert all(
            secret not in path.read_bytes() for path in generation.rglob("*") if path.is_file()
        )
    assert not (generation / "secrets").exists()
    assert not (generation / "credentials").exists()


def test_seals_generation_bound_credentials_outside_runtime_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "runtime"
    manifests, capabilities = _bundle_inputs(root)
    captured: dict[str, bytes] = {}
    monkeypatch.setattr(
        "rquant.runtime_deployment_bundle._seal_runtime_credentials",
        lambda credentials: captured.update(credentials),
    )

    receipt = install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=manifests,
        capability_env=capabilities,
    )

    expected_service_ids = {
        manifest.service_id
        for manifest in manifests
        if manifest.plane is RuntimeServicePlane.LIVE
        and manifest.service_kind
        not in {
            RuntimeServiceKind.CANDIDATE_PUBLISHER,
            RuntimeServiceKind.STRATEGY_LIVE,
        }
    }
    assert set(captured) == {
        receipt.instance_mapping[service_id] for service_id in expected_service_ids
    }
    for payload in captured.values():
        decoded = json.loads(payload)
        assert decoded["bundle_generation"] == receipt.generation_hash
        assert isinstance(decoded["capabilities"], dict)
    generation = (root / "current").resolve(strict=True)
    assert all(
        secret not in path.read_bytes()
        for secret in (b"main-token", b"pushdeer-key", b"pushplus-token")
        for path in generation.rglob("*")
        if path.is_file()
    )


def test_installer_creates_required_systemd_plane_and_control_directories(
    tmp_path: Path,
) -> None:
    root = tmp_path / "runtime"
    manifest = _manifest(
        root,
        service_id="minute/source:primary",
        kind=RuntimeServiceKind.MARKET_MINUTE_SOURCE,
        plane=RuntimeServicePlane.LIVE,
    )

    install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=(manifest,),
        capability_env={manifest.service_id: {"TUSHARE_TOKEN_MAIN": "secret"}},
    )

    assert (root / "live").is_dir()
    assert (root / "control").is_dir()


def test_credential_sealer_failure_prevents_runtime_publish(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "runtime"
    manifests, capabilities = _bundle_inputs(root)

    def fail(_credentials: object) -> None:
        raise RuntimeError("root sealer unavailable")

    monkeypatch.setattr("rquant.runtime_deployment_bundle._seal_runtime_credentials", fail)

    with pytest.raises(RuntimeError, match="root sealer"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=manifests,
            capability_env=capabilities,
        )

    assert not (root / "current").exists()


@pytest.mark.parametrize("suffix", ("", "nested", "sibling"))
def test_candidate_bundle_requires_its_exclusive_instance_output_root(
    tmp_path: Path,
    suffix: str,
) -> None:
    root = tmp_path / "runtime"
    manifest = _manifest(
        root,
        service_id="candidate-n-shape",
        kind=RuntimeServiceKind.CANDIDATE_PUBLISHER,
        plane=RuntimeServicePlane.LIVE,
    )
    expected = Path(manifest.settings["snapshot_root"])
    if suffix == "":
        unsafe = root / "live" / "candidates"
    elif suffix == "nested":
        unsafe = expected / "nested"
    else:
        unsafe = expected.parent / ("svc-" + "f" * 64)
    manifest = manifest.model_copy(
        update={
            "settings": {
                **manifest.model_dump(mode="json")["settings"],
                "snapshot_root": str(unsafe),
            }
        }
    )

    with pytest.raises(ValueError, match="exclusive|instance"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=(manifest,),
            capability_env={manifest.service_id: {}},
        )


def test_strategy_bundle_uses_dedicated_unit_and_exclusive_state_root(
    tmp_path: Path,
) -> None:
    root = tmp_path / "runtime"
    manifest = _manifest(
        root,
        service_id="strategy-n-shape",
        kind=RuntimeServiceKind.STRATEGY_LIVE,
        plane=RuntimeServicePlane.LIVE,
    )

    receipt = install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=(manifest,),
        capability_env={manifest.service_id: {}},
    )

    instance = receipt.instance_mapping[manifest.service_id]
    state_path = root / "live" / "strategies" / instance / "runner.sqlite3"
    assert Path(manifest.settings["runner_state_path"]) == state_path
    assert state_path.parent.is_dir()
    assert (root / "control" / "strategies" / instance).is_dir()
    assert receipt.unit_mapping[manifest.service_id] == (
        f"rquant-runtime-strategy@{instance}.service"
    )
    assert not (root / "current" / "credentials").exists()


def test_rejects_legacy_plaintext_secret_generation(tmp_path: Path) -> None:
    root = tmp_path / "runtime"
    legacy = root / "generations" / ("f" * 64) / "secrets"
    legacy.mkdir(parents=True)
    (legacy / "svc.env").write_text("TUSHARE_TOKEN_MAIN=plaintext\n")
    manifests, capabilities = _bundle_inputs(root)

    with pytest.raises(ValueError, match="legacy plaintext"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=manifests,
            capability_env=capabilities,
        )


@pytest.mark.parametrize("mutation", ("shared-root", "nested", "sibling"))
def test_strategy_bundle_rejects_nonexclusive_state_root(
    tmp_path: Path,
    mutation: str,
) -> None:
    root = tmp_path / "runtime"
    manifest = _manifest(
        root,
        service_id="strategy-n-shape",
        kind=RuntimeServiceKind.STRATEGY_LIVE,
        plane=RuntimeServicePlane.LIVE,
    )
    expected = Path(manifest.settings["runner_state_path"])
    if mutation == "shared-root":
        unsafe = root / "live" / "strategies" / "runner.sqlite3"
    elif mutation == "nested":
        unsafe = expected.parent / "nested" / "runner.sqlite3"
    else:
        unsafe = expected.parent.parent / ("svc-" + "f" * 64) / "runner.sqlite3"
    manifest = manifest.model_copy(
        update={
            "settings": {
                **manifest.model_dump(mode="json")["settings"],
                "runner_state_path": str(unsafe),
            }
        }
    )

    with pytest.raises(ValueError, match="exclusive|instance"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=(manifest,),
            capability_env={manifest.service_id: {}},
        )


@pytest.mark.parametrize(
    "setting_name",
    ("feature_spool_root", "strategy_spec_path", "candidate_snapshot_root"),
)
def test_strategy_bundle_rejects_readonly_input_inside_its_writable_root(
    tmp_path: Path,
    setting_name: str,
) -> None:
    root = tmp_path / "runtime"
    manifest = _manifest(
        root,
        service_id="strategy-n-shape",
        kind=RuntimeServiceKind.STRATEGY_LIVE,
        plane=RuntimeServicePlane.LIVE,
    )
    own_root = Path(manifest.settings["runner_state_path"]).parent
    manifest = manifest.model_copy(
        update={
            "settings": {
                **manifest.model_dump(mode="json")["settings"],
                setting_name: str(own_root / "forbidden"),
            }
        }
    )

    with pytest.raises(ValueError, match="read-only|writable|strategy"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=(manifest,),
            capability_env={manifest.service_id: {}},
        )


def test_same_inputs_are_deterministic_regardless_of_manifest_order(tmp_path: Path) -> None:
    root = tmp_path / "runtime"
    manifests, capabilities = _bundle_inputs(root)

    first = install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=manifests,
        capability_env=capabilities,
    )
    second = install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=tuple(reversed(manifests)),
        capability_env=dict(reversed(tuple(capabilities.items()))),
    )

    assert second == first
    assert len(tuple((root / "generations").iterdir())) == 1


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        ("commit", "commit"),
        ("duplicate", "duplicate"),
        ("wrong_plane", "plane"),
        ("unknown_env", "unknown capability environment"),
        ("serving_env", "cannot receive capability environment"),
        ("missing_mapping", "exactly match"),
        ("secret_in_manifest", "plaintext capability value"),
        ("wrong_path_owner", "owned by the live plane"),
    ),
)
def test_rejects_invalid_or_overprivileged_bundle(
    tmp_path: Path,
    mutation: str,
    message: str,
) -> None:
    root = tmp_path / "runtime"
    manifests, capabilities = _bundle_inputs(root)
    candidate = list(manifests)
    commit = COMMIT
    if mutation == "commit":
        candidate[0] = candidate[0].model_copy(update={"producer_commit": "b" * 40})
    elif mutation == "duplicate":
        candidate.append(candidate[0])
    elif mutation == "wrong_plane":
        candidate[0] = candidate[0].model_copy(update={"plane": RuntimeServicePlane.SERVING})
    elif mutation == "unknown_env":
        capabilities["minute/source:primary"]["AWS_SECRET_ACCESS_KEY"] = "nope"
    elif mutation == "serving_env":
        capabilities["serving-publisher"]["PUSHDEER_KEYS"] = "nope"
    elif mutation == "missing_mapping":
        capabilities.pop("notifier-admin")
    elif mutation == "secret_in_manifest":
        candidate[0] = candidate[0].model_copy(update={"settings": {"label": "main-token"}})
    elif mutation == "wrong_path_owner":
        candidate[0] = candidate[0].model_copy(
            update={
                "settings": {
                    "spool_root": str(root / "serving" / "raw"),
                    "quota_path": str(root / "live" / "quota.sqlite"),
                }
            }
        )

    with pytest.raises(ValueError, match=message):
        install_runtime_deployment_bundle(
            root,
            producer_commit=commit,
            manifests=tuple(candidate),
            capability_env=capabilities,
        )

    assert not (root / "current").exists()
    assert not list(root.glob(".staging-*"))


def test_failure_before_publish_preserves_previous_current_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import runtime_deployment_bundle as module

    root = tmp_path / "runtime"
    manifests, capabilities = _bundle_inputs(root)
    first = install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=manifests,
        capability_env=capabilities,
    )
    previous_target = os.readlink(root / "current")
    changed = manifests[0].model_copy(update={"interval_seconds": 2})
    write = module._write_secure_file
    calls = 0

    def fail_during_staging(path: Path, payload: bytes) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("simulated disk failure")
        write(path, payload)

    monkeypatch.setattr(module, "_write_secure_file", fail_during_staging)

    with pytest.raises(OSError, match="simulated disk failure"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=(changed, *manifests[1:]),
            capability_env=capabilities,
        )

    assert os.readlink(root / "current") == previous_target
    assert (root / previous_target).is_dir()
    assert (root / previous_target).name == first.generation_hash
    assert not list(root.glob(".staging-*"))


def test_rejects_symlinked_runtime_parent_without_writing_outside(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    linked = tmp_path / "linked"
    linked.symlink_to(outside, target_is_directory=True)
    root = linked / "runtime"
    manifests, capabilities = _bundle_inputs(root)

    with pytest.raises(ValueError, match="symlink"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=manifests,
            capability_env=capabilities,
        )

    assert not (outside / "runtime").exists()


def test_rejects_symlinked_plane_path_that_escapes_its_owner(tmp_path: Path) -> None:
    root = tmp_path / "runtime"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    (root / "live").symlink_to(outside, target_is_directory=True)
    manifests, capabilities = _bundle_inputs(root)

    with pytest.raises(ValueError, match="symlink"):
        install_runtime_deployment_bundle(
            root,
            producer_commit=COMMIT,
            manifests=manifests,
            capability_env=capabilities,
        )

    assert not (root / "current").exists()
    assert not tuple(outside.iterdir())
