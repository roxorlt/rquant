"""An offline stand-in for the Phase C root-verifier world.

The production verifier is anchored at `/`, `/etc/rquant`, `/usr/local/libexec`, and
`/var/lib/rquant`, all owned by `root`. None of that exists on a development macOS host,
so this module builds a byte-faithful replica beneath one temporary trusted root and hands
it to `RootVerifier` through the explicit `VerifierAnchors` constructor injection of ruling
O5. Nothing here is importable by the production entry point: the anchors are constructor
arguments, never an environment variable or a flag.

Every hash the external policy carries is minted from profile-derived or raw-byte values.
The declaration-side `create()` helpers of the in-generation manifests are never used to
produce a policy field, because an external policy that recomputed a generation's own
preimages would be vouching for the generation's bytes with the generation's own arithmetic.
"""

from __future__ import annotations

import hashlib
import os
import platform
import shutil
import stat
import sys
import sysconfig
import zipapp
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from rquant import signal_family_verification as verification
from rquant.runtime_authority import (
    GENERATION_MANIFEST_NAME,
    RECORD_SCHEMA_VERSION,
    RuntimeAuthorityRecord,
    RuntimeAuthorityState,
    RuntimeGenerationLifecycle,
    RuntimeGenerationSlot,
    RuntimeRoleSpec,
)
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.signal_family_successor_registry import (
    SUCCESSOR_CHANNEL_BINDINGS,
    model_descriptor_hash,
)
from rquant.strict_json import canonical_json_bytes

PRODUCER_COMMIT = "a" * 40
OPERATION_ID = "0" * 32
OTHER_OPERATION_ID = "1" * 32
PROFILE_ID = "d" * 64

POLICY_RELATIVE_PATH = "etc/rquant/signal-family-verifier-policy-v1.json"
HARNESS_RELATIVE_PATH = "usr/local/libexec/rquant-signal-family-verifier-harness-v1.pyz"
STORE_RELATIVE_PATH = "var/lib/rquant/signal-family-verification"
CHILD_WORKSPACE_RELATIVE_PATH = "var/lib/rquant/signal-family-verifier-workspace"
GENERATIONS_RELATIVE_PATH = "srv/rquant/generations"
_PROFILE_DOCUMENT_RELATIVE_PATH = "signal-family/profile-service-manifests-v1.json"

_PARTICIPANTS: tuple[tuple[str, RuntimeServiceKind, float], ...] = (
    ("strategy.alpha.v1", RuntimeServiceKind.STRATEGY_LIVE, 90.0),
    ("strategy.beta.v1", RuntimeServiceKind.STRATEGY_LIVE, 120.0),
    ("signal-router", RuntimeServiceKind.SIGNAL_ROUTER, 60.0),
    ("shadow-session", RuntimeServiceKind.SHADOW_SESSION, 45.0),
    ("notifier", RuntimeServiceKind.NOTIFIER, 75.0),
    ("paper-broker", RuntimeServiceKind.PAPER_BROKER, 180.0),
    ("serving-publisher", RuntimeServiceKind.SERVING_PUBLISHER, 50.0),
)
_BYSTANDERS: tuple[tuple[str, RuntimeServiceKind, float], ...] = (
    ("feature-live", RuntimeServiceKind.FEATURE_LIVE, 5.0),
    ("paper-consumer", RuntimeServiceKind.PAPER_CONSUMER, 7.0),
)
_ROLE_BY_KIND: Mapping[RuntimeServiceKind, str] = {
    RuntimeServiceKind.STRATEGY_LIVE: "strategy",
    RuntimeServiceKind.SIGNAL_ROUTER: "router",
    RuntimeServiceKind.SHADOW_SESSION: "shadow",
    RuntimeServiceKind.NOTIFIER: "notifier",
    RuntimeServiceKind.PAPER_BROKER: "paper-broker",
    RuntimeServiceKind.SERVING_PUBLISHER: "serving",
}
_MODULE_BY_KIND: Mapping[RuntimeServiceKind, str] = {
    RuntimeServiceKind.STRATEGY_LIVE: "rquant.strategy_runner",
    RuntimeServiceKind.SIGNAL_ROUTER: "rquant.signal_router_runtime",
    RuntimeServiceKind.SHADOW_SESSION: "rquant.runtime_shadow_sources",
    RuntimeServiceKind.NOTIFIER: "rquant.notification_state",
    RuntimeServiceKind.PAPER_BROKER: "rquant.paper_signal_worker",
    RuntimeServiceKind.SERVING_PUBLISHER: "rquant.serving_read_models",
}
_SOURCE_BY_KIND: Mapping[RuntimeServiceKind, str] = {
    kind: f"src/{module.replace('.', '/')}.py" for kind, module in _MODULE_BY_KIND.items()
}


def digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------------------
# The stub harness. WP4-b verifies the protocol, so this child replays one frozen result
# per vector instead of executing production builders; the real harness belongs to WP4-c.
# ---------------------------------------------------------------------------------------

HARNESS_SOURCE = '''"""A stdlib-only stub harness that replays one frozen result per vector."""

import json
import os
import sys

MODE = {mode!r}
REPLAY = {replay!r}
REPORT_PATH = {report_path!r}
RUN_ID_OVERRIDE = {run_id_override!r}
TEST_MANIFEST_HASH_OVERRIDE = {test_manifest_hash_override!r}


def canonical(value):
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def cwd_ancestor_modes():
    import stat as stat_module

    entries = []
    node = os.getcwd()
    while True:
        observed = os.stat(node)
        entries.append(
            [
                node,
                stat_module.S_IMODE(observed.st_mode),
                observed.st_uid,
            ]
        )
        parent = os.path.dirname(node)
        if parent == node:
            break
        node = parent
    return entries


def open_descriptors():
    observed = []
    for candidate in range(0, 256):
        try:
            os.fstat(candidate)
        except OSError:
            continue
        observed.append(candidate)
    return observed


def import_outcome(name):
    try:
        __import__(name)
    except BaseException as error:
        return type(error).__name__
    return "imported"


def write_report(request_fd, result_fd, request_bytes):
    if not REPORT_PATH:
        return
    report = {{
        "argv": list(sys.argv),
        "cwd": os.getcwd(),
        "cwd_ancestor_modes": cwd_ancestor_modes(),
        "cwd_entries": sorted(os.listdir(".")),
        "environ": {{key: os.environ[key] for key in sorted(os.environ)}},
        "executable": sys.executable,
        "open_descriptors": open_descriptors(),
        "request_fd": request_fd,
        "result_fd": result_fd,
        "request_sha256": __import__("hashlib").sha256(request_bytes).hexdigest(),
        "sys_path": list(sys.path),
        "uid": os.getuid(),
        "gid": os.getgid(),
        "imports": {{
            "rquant": import_outcome("rquant"),
            "verifier": import_outcome("rquant.signal_family_root_verifier"),
            "sqlite3": import_outcome("sqlite3"),
        }},
        "flags_isolated": bool(sys.flags.isolated),
    }}
    with open(REPORT_PATH, "wb") as handle:
        handle.write(canonical(report))


def read_request(request_fd):
    chunks = []
    while True:
        chunk = os.read(request_fd, 65536)
        if not chunk:
            break
        chunks.append(chunk)
    return b"".join(chunks)


def build_results(request):
    results = []
    for vector in request["vectors"]:
        payload = REPLAY.get(vector["vector_id"])
        if payload is None:
            continue
        results.append(
            {{
                "vector_id": vector["vector_id"],
                "pair_id": vector["pair_id"],
                "family_id": vector["family_id"],
                "surface_id": vector["surface_id"],
                "canonical_result_json": payload,
                "canonical_result_sha256": __import__("hashlib")
                .sha256(payload.encode("utf-8"))
                .hexdigest(),
            }}
        )
    results.sort(
        key=lambda row: (
            row["pair_id"],
            row["family_id"],
            row["surface_id"],
            row["vector_id"],
        )
    )
    return results


def build_body(request, results):
    run_id = RUN_ID_OVERRIDE or request["run_id"]
    manifest_hash = TEST_MANIFEST_HASH_OVERRIDE or request["test_manifest_hash"]
    body = {{
        "schema_version": 1,
        "run_id": run_id,
        "test_manifest_hash": manifest_hash,
        "vector_results": results,
    }}
    body["result_hash"] = (
        __import__("hashlib").sha256(canonical(body)).hexdigest()
    )
    return canonical(body)


def main():
    request_fd = int(os.environ["RQUANT_SIGNAL_FAMILY_REQUEST_FD"])
    result_fd = int(os.environ["RQUANT_SIGNAL_FAMILY_RESULT_FD"])
    raw_request = read_request(request_fd)
    write_report(request_fd, result_fd, raw_request)
    os.close(request_fd)
    if MODE == "timeout":
        import time

        time.sleep(3600)
    if MODE == "signal":
        os.kill(os.getpid(), 9)
    request = json.loads(raw_request)
    results = build_results(request)
    if MODE == "missing_vector":
        results = results[:-1]
    if MODE == "unknown_vector":
        results[0] = dict(results[0])
        results[0]["vector_id"] = "f" * 64
    if MODE == "unsorted":
        results = list(reversed(results))
    if MODE == "wrong_result":
        payload = canonical({{"forged": True}}).decode("utf-8")
        results[0] = dict(results[0])
        results[0]["canonical_result_json"] = payload
        results[0]["canonical_result_sha256"] = (
            __import__("hashlib").sha256(payload.encode("utf-8")).hexdigest()
        )
    body = build_body(request, results)
    if MODE == "forged_hash":
        parsed = json.loads(body)
        parsed["result_hash"] = "0" * 64
        body = canonical(parsed)
    if MODE == "trailing":
        body = body + b"\\n"
    if MODE == "oversized":
        body = b'{{"padding":"' + b"x" * 1_100_000 + b'"}}'
    if MODE == "noncanonical":
        body = b" " + body
    if MODE == "extra_output":
        sys.stdout.write("noise")
        sys.stdout.flush()
    if MODE == "stderr_output":
        sys.stderr.write("noise")
        sys.stderr.flush()
    if MODE == "open_pipe":
        if os.fork() == 0:
            import time

            time.sleep(5)
            os._exit(0)
    os.write(result_fd, body)
    os.close(result_fd)
    if MODE == "nonzero":
        sys.exit(3)
    sys.exit(0)


main()
'''


def build_harness(
    destination: Path,
    *,
    mode: str,
    replay: Mapping[str, str],
    report_path: Path | None,
    run_id_override: str | None = None,
    test_manifest_hash_override: str | None = None,
) -> bytes:
    """Build the stub harness zipapp and return its bytes."""

    staging = destination.parent / f".{destination.name}.build"
    if staging.exists():  # pragma: no cover - defensive, each build gets a fresh path
        shutil.rmtree(staging)
    staging.mkdir(parents=True)
    (staging / "__main__.py").write_text(
        HARNESS_SOURCE.format(
            mode=mode,
            replay=dict(replay),
            report_path=None if report_path is None else str(report_path),
            run_id_override=run_id_override,
            test_manifest_hash_override=test_manifest_hash_override,
        ),
        encoding="utf-8",
    )
    zipapp.create_archive(staging, destination)
    shutil.rmtree(staging)
    destination.chmod(0o555)
    return destination.read_bytes()


# ---------------------------------------------------------------------------------------
# The real WP4-c harness, and the importable generation it needs
# ---------------------------------------------------------------------------------------


def repository_root() -> Path:
    """The checkout that owns `src/rquant`, derived from the imported package itself."""

    return Path(verification.__file__).resolve().parent.parent.parent


def build_real_harness(destination: Path) -> bytes:
    """Build the production harness zipapp with its own deterministic build script."""

    import importlib.util

    script = repository_root() / "scripts" / "build-signal-family-verifier-harness.py"
    spec = importlib.util.spec_from_file_location("_wp4c_harness_builder", script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.build_harness(repository_root(), destination)
    return destination.read_bytes()


def make_generation_importable(generation_path: Path) -> None:
    """Give the generation-local interpreter a real importable `rquant`.

    The child is launched as `<generation>/bin/python -I <harness>.pyz` with a sanitized
    environment, so nothing on the caller's side can put the generation on the child's
    path: the interpreter has to resolve it by itself. Turning the generation into a venv
    whose `pyvenv.cfg` sits beside `bin/python` is what does that, and a single path
    configuration file inside its site directory names the closure the generation ships.
    Opt-in, because the default world exists to prove the child can import *nothing*.
    """

    version = f"python{sys.version_info.major}.{sys.version_info.minor}"
    site_packages = generation_path / "lib" / version / "site-packages"
    site_packages.mkdir(mode=0o755, parents=True, exist_ok=True)
    (generation_path / "pyvenv.cfg").write_text(
        "\n".join(
            (
                f"home = {Path(sys.base_prefix) / 'bin'}",
                "include-system-site-packages = false",
                f"version_info = {platform.python_version()}",
                "",
            )
        ),
        encoding="utf-8",
    )
    closure = (
        str(repository_root() / "src"),
        str(sysconfig.get_paths()["purelib"]),
    )
    (site_packages / "_generation_closure.pth").write_text(
        "\n".join((*closure, "")),
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------------------
# The replica world
# ---------------------------------------------------------------------------------------


@dataclass
class StubDeploymentLock:
    """The two methods `RootVerifier` may use; the real lock exposes no descriptor."""

    identity_changes: bool = False
    closed: bool = False
    asserted: int = 0

    def assert_current(self) -> None:
        self.asserted += 1
        if self.identity_changes and self.asserted > 1:
            raise RuntimeError("deployment lock identity changed")

    def close(self) -> None:
        self.closed = True


@dataclass
class StubAuthorityGateway:
    """Serve one authority snapshot per reopen, optionally diverging after the first."""

    snapshots: list[Any] = field(default_factory=list)
    lock: StubDeploymentLock = field(default_factory=StubDeploymentLock)
    loads: int = 0
    lock_failure: BaseException | None = None

    def acquire_deployment_lock(self) -> StubDeploymentLock:
        if self.lock_failure is not None:
            raise self.lock_failure
        return self.lock

    def load_snapshot(self) -> Any:
        index = min(self.loads, len(self.snapshots) - 1)
        self.loads += 1
        return self.snapshots[index]


@dataclass(frozen=True)
class VerifierWorld:
    root: Path
    policy_path: Path
    harness_path: Path
    store_root: Path
    child_workspace_root: Path
    generation_path: Path
    report_path: Path
    anchors: Any
    gateway: StubAuthorityGateway
    policy: verification.SignalFamilyVerifierPolicyV1
    entry: verification.ReleaseVerificationEntryV1
    test_manifest: verification.SignalFamilyTestManifestV1
    test_manifest_sha256: str
    verification_manifest_sha256: str
    successor_bundle_content_hash: str
    overlay_content_hash: str
    profile_manifests: tuple[RuntimeServiceManifest, ...]
    bindings: tuple[verification.VerificationServiceBindingV1, ...]
    replay: Mapping[str, str]
    authority_epoch_key: str
    profile_document_sha256: str
    operation_id: str
    sequence: int

    @property
    def store_database(self) -> Path:
        return self.store_root / "store.sqlite3"

    def read_report(self) -> dict[str, Any]:
        import json

        return json.loads(self.report_path.read_bytes())


def _manifest(
    service_id: str,
    kind: RuntimeServiceKind,
    stale_after_seconds: float,
) -> RuntimeServiceManifest:
    return RuntimeServiceManifest(
        service_id=service_id,
        service_kind=kind,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=1.0,
        stale_after_seconds=stale_after_seconds,
        producer_commit=PRODUCER_COMMIT,
        settings={},
    )


def profile_manifests(
    overrides: Mapping[str, float] | None = None,
    *,
    participants: Sequence[tuple[str, RuntimeServiceKind, float]] = _PARTICIPANTS,
) -> tuple[RuntimeServiceManifest, ...]:
    stale = dict(overrides or {})
    rows = list(participants) + list(_BYSTANDERS)
    return tuple(
        _manifest(service_id, kind, stale.get(service_id, default_stale))
        for service_id, kind, default_stale in rows
    )


def _write_generation_sources(generation: Path) -> dict[str, str]:
    """One real source file per binding module; the hash is the file's own SHA-256."""

    hashes: dict[str, str] = {}
    for kind, relative in _SOURCE_BY_KIND.items():
        target = generation / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        body = f"# generation source for {_MODULE_BY_KIND[kind]}\n".encode()
        target.write_bytes(body)
        target.chmod(0o444)
        hashes[relative] = hashlib.sha256(body).hexdigest()
    return hashes


def _bindings(
    manifests: tuple[RuntimeServiceManifest, ...],
    source_hashes: Mapping[str, str],
) -> tuple[verification.VerificationServiceBindingV1, ...]:
    surfaces = verification.expected_surface_ids(manifests)
    by_id = {manifest.service_id: manifest for manifest in manifests}
    built = []
    for service_id in verification.participating_service_ids(manifests):
        manifest = by_id[service_id]
        kind = manifest.service_kind
        relative = _SOURCE_BY_KIND[kind]
        built.append(
            verification.VerificationServiceBindingV1.create(
                service_id=service_id,
                runtime_service_kind=kind,
                role_name=_ROLE_BY_KIND[kind],
                service_manifest_fingerprint=manifest.manifest_fingerprint,
                executable_module=_MODULE_BY_KIND[kind],
                executable_source_relative_path=relative,
                executable_source_sha256=source_hashes[relative],
                surface_ids=surfaces[service_id],
            )
        )
    return tuple(built)


def _vectors(
    manifests: tuple[RuntimeServiceManifest, ...],
) -> tuple[verification.SignalFamilyVectorV1, ...]:
    from rquant.signal_family_successor_registry import ACCEPTED_FAMILY_IDS

    family_id = ACCEPTED_FAMILY_IDS[0]
    built = [
        verification.SignalFamilyVectorV1.create(
            pair_id=pair.pair_id,
            family_id=family_id,
            surface_id=surface_id,
            input_json=canonical_json_bytes(
                {"pair": pair.pair_id, "surface": surface_id.value}
            ).decode("utf-8"),
        )
        for pair in verification.resolve_pair_bindings(manifests)
        for surface_id in verification.READER_SURFACES[pair.pair_id]
    ]
    return tuple(sorted(built, key=lambda vector: vector.vector_id))


def _replay(vectors: Sequence[verification.SignalFamilyVectorV1]) -> dict[str, str]:
    return {
        vector.vector_id: canonical_json_bytes(
            {"observed": vector.vector_id, "surface": vector.surface_id.value}
        ).decode("utf-8")
        for vector in vectors
    }


def _channel_payload(channel_id: str, producers: Sequence[str], consumers: Sequence[str]) -> Any:
    payload_model = SUCCESSOR_CHANNEL_BINDINGS[channel_id]
    declaration_fingerprint = digest(f"declaration:{channel_id}")
    physical_fingerprint = digest(f"physical:{channel_id}")
    values: dict[str, Any] = {
        "channel_id": channel_id,
        "payload_model": payload_model,
        "declaration_schema_fingerprint": declaration_fingerprint,
        "physical_schema_fingerprint": physical_fingerprint,
        "model_descriptor_hash": model_descriptor_hash(
            payload_model=payload_model,
            declaration_schema_fingerprint=declaration_fingerprint,
            physical_schema_fingerprint=physical_fingerprint,
        ),
        "producer_service_ids": sorted(producers),
        "consumer_service_ids": sorted(consumers),
    }
    values["channel_hash"] = digest_of(values)
    return values


def digest_of(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _successor_bundle(manifests: tuple[RuntimeServiceManifest, ...]) -> dict[str, Any]:
    pairs = {pair.pair_id: pair for pair in verification.resolve_pair_bindings(manifests)}
    channels = [
        _channel_payload(
            "signal-bus-routed-record/current",
            pairs["router-notifier"].producer_service_ids,
            pairs["router-notifier"].consumer_service_ids,
        ),
        _channel_payload(
            "signal-envelope/current",
            pairs["strategy-router"].producer_service_ids,
            pairs["strategy-router"].consumer_service_ids,
        ),
        _channel_payload(
            "signal-route-spool-record/current",
            pairs["router-paper"].producer_service_ids,
            pairs["router-paper"].consumer_service_ids,
        ),
    ]
    channels.sort(key=lambda channel: channel["channel_id"])
    body: dict[str, Any] = {
        "schema_version": 1,
        "bundle_namespace": "rquant.signal-family.successor",
        "channels": channels,
    }
    body["content_hash"] = digest_of(body)
    return body


def _overlay_bundle(successor: Mapping[str, Any]) -> dict[str, Any]:
    from rquant.signal_family_successor_registry import ACCEPTED_FAMILY_IDS, PAIR_IDS

    declarations = []
    for channel in successor["channels"]:
        values: dict[str, Any] = {
            "channel_id": channel["channel_id"],
            "base_bundle_content_hash": successor["content_hash"],
            "base_declaration_fingerprint": channel["declaration_schema_fingerprint"],
            "base_physical_fingerprint": channel["physical_schema_fingerprint"],
            "model_descriptor_hash": channel["model_descriptor_hash"],
            "accepted_family_ids": list(ACCEPTED_FAMILY_IDS),
            "pair_ids": list(PAIR_IDS),
        }
        values["declaration_hash"] = digest_of(values)
        declarations.append(values)
    declarations.sort(key=lambda declaration: declaration["channel_id"])
    body: dict[str, Any] = {
        "overlay_namespace": "rquant.signal-family.overlay",
        "overlay_version": 1,
        "base_bundle_content_hash": successor["content_hash"],
        "declarations": declarations,
    }
    body["content_hash"] = digest_of(body)
    return body


def _slot(
    generation_path: Path,
    *,
    generation_id: str,
    profile_id: str,
    interpreter: Path,
) -> RuntimeGenerationSlot:
    roles = {
        role_name: RuntimeRoleSpec(
            python_path=interpreter,
            module=_MODULE_BY_KIND[kind],
            working_directory=generation_path,
            app_source=generation_path / "src",
            site_packages=(generation_path / "site-packages",),
        )
        for kind, role_name in _ROLE_BY_KIND.items()
    }
    return RuntimeGenerationSlot(
        lifecycle=RuntimeGenerationLifecycle.ACTIVE,
        generation_id=generation_id,
        generation_path=generation_path,
        commit="b" * 40,
        full_manifest_hash=generation_id,
        profile_id=profile_id,
        roles=roles,
    )


def build_world(
    tmp_path: Path,
    *,
    harness_mode: str = "ok",
    harness: str = "stub",
    blocked_surface_id: Any | None = None,
    vector_pair_ids: tuple[str, ...] | None = None,
    policy_max_age_seconds: int | None = None,
    stale_overrides: Mapping[str, float] | None = None,
    run_id_override: str | None = None,
    test_manifest_hash_override: str | None = None,
    sequence: int = 3,
    operation_id: str = OPERATION_ID,
    profile_id: str = PROFILE_ID,
) -> VerifierWorld:
    """Assemble one complete replica: policy, harness, generation, authority, anchors.

    `harness="stub"` keeps the WP4-b protocol replayer, whose child imports nothing.
    `harness="real"` installs the WP4-c production harness zipapp instead, replaces the
    synthetic vectors with the ones that harness actually exercises, and makes the
    generation importable so the child can reach the production builders.
    """

    if harness not in {"stub", "real"}:
        raise ValueError("harness must be 'stub' or 'real'")
    if harness == "real" and harness_mode != "ok":
        raise ValueError("the real harness has no injected failure modes")
    if blocked_surface_id is not None and harness != "real":
        raise ValueError("a blocked-surface vector only means something to the real harness")
    if vector_pair_ids is not None and not vector_pair_ids:
        raise ValueError("a restricted vector set must still name at least one pair")

    from rquant import signal_family_root_verifier as verifier

    root = tmp_path / "root"
    root.mkdir(mode=0o755, parents=True)
    for relative in ("etc/rquant", "usr/local/libexec", "var/lib/rquant", "srv/rquant"):
        target = root / relative
        target.mkdir(mode=0o755, parents=True, exist_ok=True)
        for parent in [target, *target.parents]:
            if parent == root.parent:
                break
            parent.chmod(0o755)

    manifests = profile_manifests(stale_overrides)
    generations = root / GENERATIONS_RELATIVE_PATH
    generations.mkdir(mode=0o755, parents=True, exist_ok=True)
    # The generation is assembled under a staging name because its identity is the
    # SHA-256 of its own `full-manifest.json`, which cannot be known until every file it
    # covers exists. The directory is renamed to that identity once the manifest is built.
    generation_path = generations / "staging"
    generation_path.mkdir(mode=0o755)
    (generation_path / "site-packages").mkdir(mode=0o755)
    interpreter_dir = generation_path / "bin"
    interpreter_dir.mkdir(mode=0o755)
    interpreter = interpreter_dir / "python"
    os.symlink(sys.executable, interpreter)

    source_hashes = _write_generation_sources(generation_path)
    bindings = _bindings(manifests, source_hashes)
    if harness == "real":
        from tests.support.signal_family_harness_vectors import (
            blocked_surface_vector,
            expected_results_for,
            harness_vectors,
        )

        vectors = harness_vectors()
        if vector_pair_ids is not None:
            vectors = tuple(
                vector for vector in vectors if vector.pair_id in vector_pair_ids
            )
        # The policy author derives the expected results by running the same exercise the
        # child will run. The child is never told any of this; the root compares after exit.
        policy_scratch = tmp_path / "policy-expected"
        policy_scratch.mkdir(mode=0o700, parents=True)
        derived = dict(expected_results_for(vectors, policy_scratch))
        if blocked_surface_id is not None:
            blocked_vector, placeholder = blocked_surface_vector(blocked_surface_id)
            vectors = tuple(
                sorted((*vectors, blocked_vector), key=lambda vector: vector.vector_id)
            )
            derived[blocked_vector.vector_id] = placeholder
        replay: Mapping[str, str] = derived
    else:
        vectors = _vectors(manifests)
        if vector_pair_ids is not None:
            vectors = tuple(
                vector for vector in vectors if vector.pair_id in vector_pair_ids
            )
        replay = _replay(vectors)
    expected_results = tuple(
        verification.SignalFamilyExpectedResultV1(
            vector_id=vector.vector_id,
            canonical_result_sha256=hashlib.sha256(
                replay[vector.vector_id].encode("utf-8")
            ).hexdigest(),
        )
        for vector in vectors
    )
    test_manifest = verification.SignalFamilyTestManifestV1.create(
        vectors=vectors,
        expected_results=expected_results,
        profile_manifests=manifests,
        service_bindings=bindings,
    )

    successor = _successor_bundle(manifests)
    overlay = _overlay_bundle(successor)
    signal_family = generation_path / "signal-family"
    signal_family.mkdir(mode=0o755)
    successor_bytes = canonical_json_bytes(successor)
    overlay_bytes = canonical_json_bytes(overlay)
    test_manifest_bytes = verification.test_manifest_canonical_json_bytes(test_manifest)
    test_manifest_sha256 = hashlib.sha256(test_manifest_bytes).hexdigest()
    verification_manifest = verification.SignalFamilyVerificationManifestV1.create(
        successor_bundle_content_hash=successor["content_hash"],
        overlay_content_hash=overlay["content_hash"],
        test_manifest_sha256=test_manifest_sha256,
        test_manifest=test_manifest,
    )
    verification_manifest_bytes = verification.verification_manifest_canonical_json_bytes(
        verification_manifest
    )
    profile_document_bytes = profile_document(manifests)
    documents = {
        verifier.PROFILE_SERVICE_MANIFESTS_RELATIVE_PATH: profile_document_bytes,
        verification.SUCCESSOR_BUNDLE_RELATIVE_PATH: successor_bytes,
        verification.OVERLAY_BUNDLE_RELATIVE_PATH: overlay_bytes,
        verification.VERIFICATION_MANIFEST_RELATIVE_PATH: verification_manifest_bytes,
        verification.TEST_MANIFEST_RELATIVE_PATH: test_manifest_bytes,
    }
    for relative, payload in documents.items():
        target = generation_path / relative
        target.write_bytes(payload)
        target.chmod(0o444)

    full_manifest_entries = dict(source_hashes)
    for relative, payload in documents.items():
        full_manifest_entries[relative] = hashlib.sha256(payload).hexdigest()
    verification_manifest_sha256 = full_manifest_entries[
        verification.VERIFICATION_MANIFEST_RELATIVE_PATH
    ]
    profile_document_sha256 = full_manifest_entries[
        verifier.PROFILE_SERVICE_MANIFESTS_RELATIVE_PATH
    ]
    full_manifest_bytes = full_manifest_payload(
        generation_path,
        full_manifest_entries,
        profile_id=profile_id,
    )
    generation_id = hashlib.sha256(full_manifest_bytes).hexdigest()
    staged = generation_path
    generation_path = generations / generation_id
    staged.rename(generation_path)
    interpreter = generation_path / "bin" / "python"
    write_full_manifest_bytes(generation_path, full_manifest_bytes)

    report_path = tmp_path / "child-report.json"
    harness_path = root / HARNESS_RELATIVE_PATH
    if harness == "real":
        make_generation_importable(generation_path)
        harness_bytes = build_real_harness(harness_path)
    else:
        harness_bytes = build_harness(
            harness_path,
            mode=harness_mode,
            replay=replay,
            report_path=report_path,
            run_id_override=run_id_override,
            test_manifest_hash_override=test_manifest_hash_override,
        )

    # A1: every policy field is minted from a profile-derived or raw-byte value. The
    # declaration-side manifest `create()` helpers never produce one of these hashes.
    entry = verification.ReleaseVerificationEntryV1.create(
        successor_bundle_content_hash=successor["content_hash"],
        overlay_content_hash=overlay["content_hash"],
        verification_manifest_sha256=verification_manifest_sha256,
        vector_set_hash=verification.vector_set_hash(vectors),
        expected_result_set_hash=verification.expected_result_set_hash(expected_results),
        five_pair_service_binding_set_hash=verification.five_pair_service_binding_set_hash(
            manifests,
            bindings,
        ),
        verifier_policy_max_age_seconds=policy_max_age_seconds,
    )
    policy = verification.SignalFamilyVerifierPolicyV1.create(
        harness_sha256=hashlib.sha256(harness_bytes).hexdigest(),
        release_entries=(entry,),
    )
    policy_path = root / POLICY_RELATIVE_PATH
    policy_path.write_bytes(verification.verifier_policy_canonical_json_bytes(policy))
    policy_path.chmod(0o444)

    store_root = root / STORE_RELATIVE_PATH
    slot = _slot(
        generation_path,
        generation_id=generation_id,
        profile_id=profile_id,
        interpreter=interpreter,
    )
    snapshot = verifier.GenerationAuthoritySnapshot(
        operation_id=operation_id,
        sequence=sequence,
        authority_state=RuntimeAuthorityState.ACTIVE,
        slot=slot,
        profile_manifests=manifests,
        full_manifest_entries=full_manifest_entries,
        full_manifest_sha256=generation_id,
        profile_document_sha256=profile_document_sha256,
    )
    gateway = StubAuthorityGateway(snapshots=[snapshot])
    anchors = verifier.VerifierAnchors(
        policy_trusted_root=root,
        policy_path=policy_path,
        harness_path=harness_path,
        store_root=store_root,
        child_workspace_root=root / CHILD_WORKSPACE_RELATIVE_PATH,
        expected_owner_uid=os.getuid(),
        # macOS gives a new file the group of its parent directory rather than the
        # caller's, so the replica's owning group is read back rather than assumed.
        expected_owner_gid=root.stat().st_gid,
        child_uid=os.getuid(),
        child_gid=os.getgid(),
    )
    return VerifierWorld(
        root=root,
        policy_path=policy_path,
        harness_path=harness_path,
        store_root=store_root,
        child_workspace_root=root / CHILD_WORKSPACE_RELATIVE_PATH,
        generation_path=generation_path,
        report_path=report_path,
        anchors=anchors,
        gateway=gateway,
        policy=policy,
        entry=entry,
        test_manifest=test_manifest,
        test_manifest_sha256=test_manifest_sha256,
        verification_manifest_sha256=verification_manifest_sha256,
        successor_bundle_content_hash=successor["content_hash"],
        overlay_content_hash=overlay["content_hash"],
        profile_manifests=manifests,
        bindings=bindings,
        replay=replay,
        authority_epoch_key=verification.authority_epoch_key(
            operation_id=operation_id,
            sequence=sequence,
            generation_id=generation_id,
            full_manifest_hash=generation_id,
            profile_id=profile_id,
        ),
        profile_document_sha256=profile_document_sha256,
        operation_id=operation_id,
        sequence=sequence,
    )


def profile_document(manifests: Sequence[RuntimeServiceManifest]) -> bytes:
    """The root-owned generation document that carries the profile's service manifests.

    Its authority comes from two places at once: it is an entry of the full generation
    manifest, and every `manifest_fingerprint` inside it must equal the
    `service_manifest_fingerprint` of the matching service binding, which the external
    root policy anchors through `five_pair_service_binding_set_hash`.
    """

    return canonical_json_bytes(
        {
            "schema_version": 1,
            "service_manifests": [
                manifest.model_dump(mode="json")
                for manifest in sorted(manifests, key=lambda item: item.service_id)
            ],
        }
    )


def full_manifest_payload(
    generation_path: Path,
    entries: Mapping[str, str],
    *,
    profile_id: str,
) -> bytes:
    """The generation's `full-manifest.json` bytes, in the shape the root parser reads.

    Its SHA-256 is the generation identity, so it is built before the generation
    directory is named and never contains an entry for itself.
    """

    return canonical_json_bytes(
        {
            "entries": [
                {
                    "mode": 292,
                    "nlink": 1,
                    "owner_uid": os.getuid(),
                    "path": relative,
                    "sha256": digest_value,
                    "size": (generation_path / relative).stat().st_size,
                    "type": "file",
                }
                for relative, digest_value in sorted(entries.items())
            ],
            "profile_id": profile_id,
            "roles": {},
            "schema_id": "runtime-generation-manifest-v1",
        }
    )


def write_full_manifest_bytes(generation_path: Path, payload: bytes) -> bytes:
    """Publish one already-built full manifest into its generation."""

    target = generation_path / GENERATION_MANIFEST_NAME
    if target.exists():
        target.chmod(0o644)
    target.write_bytes(payload)
    target.chmod(0o444)
    return payload


def production_authority_record(world: VerifierWorld) -> RuntimeAuthorityRecord:
    """The `load_runtime_authority()`-shaped record the production gateway consumes."""

    return RuntimeAuthorityRecord(
        schema_version=RECORD_SCHEMA_VERSION,
        operation_id=world.operation_id,
        sequence=world.sequence,
        state=RuntimeAuthorityState.ACTIVE,
        current=world.gateway.snapshots[0].slot,
        prior=None,
    )


def rewrite_profile_document(world: VerifierWorld, payload: bytes) -> None:
    """Replace the in-generation profile document without touching the full manifest."""

    target = world.generation_path / _PROFILE_DOCUMENT_RELATIVE_PATH
    target.chmod(0o644)
    target.write_bytes(payload)
    target.chmod(0o444)


def rewrite_policy(
    world: VerifierWorld,
    policy: verification.SignalFamilyVerifierPolicyV1,
) -> None:
    """Replace the anchored policy file in place, preserving its 0444 identity."""

    world.policy_path.chmod(0o644)
    world.policy_path.write_bytes(verification.verifier_policy_canonical_json_bytes(policy))
    world.policy_path.chmod(0o444)


def write_policy_bytes(world: VerifierWorld, payload: bytes) -> None:
    world.policy_path.chmod(0o644)
    world.policy_path.write_bytes(payload)
    world.policy_path.chmod(0o444)


def snapshot_with(world: VerifierWorld, **changes: Any) -> Any:
    """Clone the world's authority snapshot with the named fields replaced."""

    import dataclasses

    from rquant import signal_family_root_verifier as verifier

    base = world.gateway.snapshots[0]
    slot_changes = {key: changes.pop(key) for key in list(changes) if hasattr(base.slot, key)}
    slot = base.slot
    if slot_changes:
        slot = RuntimeGenerationSlot(
            lifecycle=slot_changes.get("lifecycle", slot.lifecycle),
            generation_id=slot_changes.get("generation_id", slot.generation_id),
            generation_path=slot_changes.get("generation_path", slot.generation_path),
            commit=slot_changes.get("commit", slot.commit),
            full_manifest_hash=slot_changes.get("full_manifest_hash", slot.full_manifest_hash),
            profile_id=slot_changes.get("profile_id", slot.profile_id),
            roles=slot_changes.get("roles", slot.roles),
        )
    assert isinstance(base, verifier.GenerationAuthoritySnapshot)
    return dataclasses.replace(base, slot=slot, **changes)


def mode_of(path: Path) -> int:
    return stat.S_IMODE(path.stat().st_mode)
