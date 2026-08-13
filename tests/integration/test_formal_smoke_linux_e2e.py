from __future__ import annotations

import hashlib
import json
import os
import sys
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

import pytest

from rquant.external_monotonic_root import (
    EXTERNAL_MONOTONIC_ROOT_ZERO_HASH,
    ExternalMonotonicRootConfig,
    ExternalMonotonicRootRequest,
    UnixSocketExternalMonotonicRootManifest,
)
from rquant.external_monotonic_root_service import (
    ExternalMonotonicRootUnixService,
    ExternalRootServiceConfiguration,
    ExternalRootStoredState,
    PersistentExternalMonotonicRootBackend,
)
from rquant.formal_runtime_composition import FormalRuntimeBootstrapConfiguration
from rquant.runtime_contracts import canonical_sha256
from rquant.strict_json import canonical_model_json_bytes
from tests.runtime_code_e2e_support import (
    RuntimeCodeTestPackage,
    build_test_package,
    install_test_package,
)

_LINUX_GATE_REASON = (
    "Linux-only full formal smoke gate requires os.execve(fd) and Linux descriptor exec semantics"
)
pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux"),
    reason=_LINUX_GATE_REASON,
)


def _generation_launcher(mode: str) -> bytes:
    template = r"""
import hashlib
import json
import os
import stat
import sys
import time

MODE = __MODE__


def canonical(value):
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def digest_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        while True:
            chunk = stream.read(65536)
            if not chunk:
                return digest.hexdigest()
            digest.update(chunk)


def write_all(descriptor, payload):
    view = memoryview(payload)
    while view:
        written = os.write(descriptor, view)
        if written <= 0:
            raise RuntimeError("receipt write failed")
        view = view[written:]


arguments = sys.argv[1:]
assert arguments[0] == "formal-smoke-runtime-execute"
request_fd = int(arguments[arguments.index("--request-fd") + 1])
receipt_fd = int(arguments[arguments.index("--receipt-fd") + 1])
assert stat.S_ISFIFO(os.fstat(request_fd).st_mode)
assert stat.S_ISFIFO(os.fstat(receipt_fd).st_mode)
chunks = []
while True:
    chunk = os.read(request_fd, 65536)
    if not chunk:
        break
    chunks.append(chunk)
request_bytes = b"".join(chunks)
request = json.loads(request_bytes)
assert canonical(request) == request_bytes
assert request["contract"] == "rquant-formal-smoke-execution-request/v1"
identity = request["execution_identity"]
evidence = request["code_trust_evidence"]
assert request["code_commit"] == evidence["provenance_commit"]
assert identity["generation_id"] == evidence["generation_id"]
generation_root = identity["generation_root"]
assert os.path.realpath(generation_root) == generation_root
working_directory = os.path.join(generation_root, identity["working_directory"])
assert os.getcwd() == working_directory
assert os.path.realpath(sys.executable) == os.path.join(
    generation_root, identity["interpreter"]["path"]
)
assert os.path.realpath(sys.argv[0]) == os.path.join(
    generation_root, identity["launcher"]["path"]
)
for root in identity["import_roots"]:
    absolute = os.path.join(generation_root, root)
    assert absolute in sys.path
    assert os.path.isdir(absolute)
for descriptor in identity["code_files"]:
    absolute = os.path.join(generation_root, descriptor["path"])
    observed = os.stat(absolute, follow_symlinks=False)
    assert stat.S_IMODE(observed.st_mode) == descriptor["mode"]
    assert observed.st_size == descriptor["size"]
    assert digest_file(absolute) == descriptor["sha256"]

if MODE == "timeout":
    time.sleep(30)
    raise AssertionError("parent deadline did not terminate generation A")
if MODE == "nonzero":
    raise SystemExit(23)
if MODE == "malformed":
    write_all(receipt_fd, b"{malformed")
    raise SystemExit(0)

run_id = "generation-a-linux-e2e"
run_directory = os.path.join(request["staging_root"], "strategy_lab_runs")
os.mkdir(run_directory, 0o700)
json_payload = b'{"generation_marker":"A"}\n'
markdown_payload = b"# generation A\n"
artifact_values = (
    ("json", run_id + ".json", json_payload),
    ("markdown", run_id + ".md", markdown_payload),
)
artifacts = []
for kind, name, payload in artifact_values:
    path = os.path.join(run_directory, name)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        write_all(descriptor, payload)
    finally:
        os.close(descriptor)
    artifacts.append(
        {
            "kind": kind,
            "relative_path": "strategy_lab_runs/" + name,
            "sha256": hashlib.sha256(payload).hexdigest(),
            "size": len(payload),
        }
    )
result = {
    "audit_run_id": request["audit_run_id"],
    "code_commit": request["code_commit"],
    "dataset_binding_hash": request["dataset_binding_hash"],
    "dataset_snapshot_id": request["dataset_snapshot_id"],
    "fixed_spec_version": "stage1-smoke-v1",
    "metrics": {"generation_marker": "A", "launcher_mode": MODE},
    "missing_evidence": [],
    "result_hash": "8" * 64,
    "run_id": run_id,
    "sample_count": 1,
    "status": "comparable",
    "strategy": request["strategy"],
    "strategy_spec_hash": "7" * 64,
}
result_digest = hashlib.sha256(
    canonical({"artifacts": artifacts, "result": result})
).hexdigest()
receipt = {
    "artifacts": artifacts,
    "code_trust_evidence": evidence,
    "contract": "rquant-formal-smoke-execution-receipt/v1",
    "execution_identity": identity,
    "request_digest": hashlib.sha256(request_bytes).hexdigest(),
    "result": result,
    "result_digest": result_digest,
    "schema_version": 1,
}
receipt_bytes = canonical(receipt)
if MODE == "partial":
    write_all(receipt_fd, receipt_bytes[: len(receipt_bytes) // 2])
    raise SystemExit(0)
write_all(receipt_fd, receipt_bytes)
if MODE == "staging_tamper":
    with open(os.path.join(run_directory, run_id + ".json"), "wb") as stream:
        stream.write(b"tampered-after-receipt")
"""
    return template.replace("__MODE__", repr(mode)).encode("utf-8")


def _build_real_generation(
    root: Path,
    *,
    mode: str,
) -> tuple[RuntimeCodeTestPackage, Path, Path]:
    python_abi = sys.implementation.cache_tag
    assert python_abi is not None
    package = build_test_package(
        root / "package",
        source=b'GENERATION_MARKER = "A"\n',
        interpreter_bytes=Path(sys.executable).resolve().read_bytes(),
        launcher_bytes=_generation_launcher(mode),
        python_abi=python_abi,
        environment_allowlist=(),
        now=datetime.now(UTC),
    )
    trusted_base, runtime_root, _installer = install_test_package(root, package)
    return package, trusted_base, runtime_root


class _PromotionReceiptHandler:
    def response_json(
        self,
        _request: object,
        state: ExternalRootStoredState | None,
    ) -> str | None:
        if state is None:
            return None
        return state.checkpoint_json


class _PromotionProbeSigner:
    signature_algorithm = "ed25519"

    def __init__(self, signer: object) -> None:
        self.issuer = signer.issuer
        self.key_id = signer.key_id
        self.key_purpose = signer.key_purpose
        self.public_key_fingerprint = signer.public_key_fingerprint
        self._signer = signer

    def sign(self, *, namespace: str, payload: bytes) -> str:
        return self._signer.sign(namespace=namespace, payload=payload)


@contextmanager
def _real_promotion_authority(
    root: Path,
    *,
    package: RuntimeCodeTestPackage,
    trusted_base: Path,
    runtime_root: Path,
) -> Iterator[Path]:
    receipt = package.receipt
    short_parent = Path(__file__).resolve().parents[2] / ".s"
    short_parent.mkdir(mode=0o700, exist_ok=True)
    short_parent.chmod(0o700)
    short_root = short_parent / hashlib.sha256(os.fspath(root).encode()).hexdigest()[:6]
    short_root.mkdir(mode=0o700)
    socket_path = short_root / "p.sock"
    transport = UnixSocketExternalMonotonicRootManifest(
        role=receipt.role,
        authority_id=receipt.root_authority_id,
        store_id=receipt.root_store_id,
        rollback_domain_id=receipt.rollback_domain_id,
        socket_path=socket_path,
        socket_uid=os.getuid(),
        socket_gid=os.getgid(),
        socket_mode=0o600,
        peer_uid=os.getuid(),
        peer_gid=os.getgid(),
        connect_timeout_ms=2_000,
        max_response_bytes=1024 * 1024,
    )
    service_configuration = ExternalRootServiceConfiguration(
        socket_path=socket_path,
        socket_uid=os.getuid(),
        socket_gid=os.getgid(),
        service_uid=os.getuid(),
        service_gid=os.getgid(),
        allowed_peer_uid=os.getuid(),
        allowed_peer_gid=os.getgid(),
        socket_mode=0o600,
        socket_directory_mode=0o700,
        role=receipt.role,
        authority_id=receipt.root_authority_id,
        store_id=receipt.root_store_id,
        rollback_domain_id=receipt.rollback_domain_id,
        transport_manifest_hash=transport.manifest_hash,
    )
    backend = PersistentExternalMonotonicRootBackend(
        root / "promotion.sqlite3",
        role=receipt.role,
        authority_id=receipt.root_authority_id,
        store_id=receipt.root_store_id,
    )
    checkpoint = receipt.model_dump(mode="json")
    backend.apply(
        ExternalMonotonicRootRequest.close(
            kind="pin",
            role=receipt.role,
            root_authority_id=receipt.root_authority_id,
            root_store_id=receipt.root_store_id,
            subject_authority_id="installation-a-test-platform",
            challenge_nonce="9" * 64,
            operation_id="8" * 64,
            previous_checkpoint_hash=EXTERNAL_MONOTONIC_ROOT_ZERO_HASH,
            checkpoint_contract=receipt.contract,
            checkpoint_hash=canonical_sha256(checkpoint),
            checkpoint_json=package.receipt_bytes.decode("utf-8"),
        )
    )
    service = ExternalMonotonicRootUnixService(
        configuration=service_configuration,
        backend=backend,
        handler=_PromotionReceiptHandler(),
        probe_signer=_PromotionProbeSigner(package.authorities[6]),
    )
    stop = threading.Event()
    thread = threading.Thread(
        target=service.serve_forever,
        kwargs={"stop": stop},
        daemon=True,
    )
    thread.start()
    assert service.ready.wait(timeout=5)
    promotion_config = ExternalMonotonicRootConfig(
        transport="unix-socket-v1",
        transport_manifest_hash=transport.manifest_hash,
        role=receipt.role,
        root_authority_id=receipt.root_authority_id,
        root_store_id=receipt.root_store_id,
        root_issuer=receipt.issuer,
        root_key_id=receipt.key_id,
        root_key_purpose=receipt.key_purpose,
        root_receipt_namespace=receipt.namespace,
        root_public_key_fingerprint=receipt.public_key_fingerprint,
        witness_rollback_domain_id=receipt.rollback_domain_id,
        local_rollback_domain_id="local-runtime-code-domain",
    )
    configuration = FormalRuntimeBootstrapConfiguration(
        runtime_root=runtime_root,
        trusted_base=trusted_base,
        expected_material_uid=os.getuid(),
        expected_material_gid=os.getgid(),
        expected_audience="formal-lab",
        expected_installation_id="installation-a",
        expected_target_platform="test-platform",
        expected_python_abi=sys.implementation.cache_tag or "invalid",
        root_keys=(package.authorities[1],),
        runtime_keys=(package.authorities[4],),
        promotion_key=package.authorities[7],
        promotion_config=promotion_config,
        promotion_transport=transport,
        promotion_subject_authority_id="installation-a-test-platform",
    )
    configuration_path = trusted_base / "runtime-code-bootstrap.json"
    configuration_path.write_bytes(canonical_model_json_bytes(configuration))
    configuration_path.chmod(0o444)
    try:
        yield configuration_path
    finally:
        stop.set()
        service.wake()
        thread.join(timeout=5)
        assert not thread.is_alive()
        short_root.rmdir()


def _cli_arguments(
    *,
    configuration_path: Path,
    trusted_base: Path,
    output: Path,
    timeout_seconds: float,
) -> list[str]:
    return [
        "rquant",
        "formal-smoke-replay",
        "--strategy",
        "n_shape",
        "--start-date",
        "2026-04-01",
        "--end-date",
        "2026-07-02",
        "--audit-run-id",
        "a" * 64,
        "--snapshot-id",
        "b" * 64,
        "--binding-hash",
        "c" * 64,
        "--output-dir",
        os.fspath(output),
        "--execution-timeout-seconds",
        str(timeout_seconds),
        "--runtime-code-config",
        os.fspath(configuration_path),
        "--runtime-code-trusted-base",
        os.fspath(trusted_base),
        "--runtime-code-authority-uid",
        str(os.getuid()),
        "--runtime-code-authority-gid",
        str(os.getgid()),
    ]


def _invoke_real_cli(
    monkeypatch: pytest.MonkeyPatch,
    *,
    configuration_path: Path,
    trusted_base: Path,
    output: Path,
    timeout_seconds: float = 5,
) -> int:
    from rquant.cli import main

    monkeypatch.setattr(
        sys,
        "argv",
        _cli_arguments(
            configuration_path=configuration_path,
            trusted_base=trusted_base,
            output=output,
            timeout_seconds=timeout_seconds,
        ),
    )
    return main()


def test_linux_real_cli_executes_signed_generation_a_and_publishes_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    import rquant

    package, trusted_base, runtime_root = _build_real_generation(
        tmp_path,
        mode="success",
    )
    generation_root = runtime_root / "generations" / package.receipt.generation_id
    assert not Path(rquant.__file__).resolve().is_relative_to(generation_root)
    output = tmp_path / "output"
    output.mkdir(mode=0o700)
    with _real_promotion_authority(
        tmp_path,
        package=package,
        trusted_base=trusted_base,
        runtime_root=runtime_root,
    ) as configuration_path:
        assert (
            _invoke_real_cli(
                monkeypatch,
                configuration_path=configuration_path,
                trusted_base=trusted_base,
                output=output,
            )
            == 0
        )

    result = json.loads(capsys.readouterr().out)
    assert result["metrics"] == {"generation_marker": "A", "launcher_mode": "success"}
    assert Path(result["json_path"]).read_bytes() == b'{"generation_marker":"A"}\n'
    assert result["execution_receipt"]["code_trust_evidence"]["generation_id"] == (
        package.receipt.generation_id
    )


@pytest.mark.parametrize(
    ("mode", "timeout_seconds"),
    (
        ("malformed", 5.0),
        ("partial", 5.0),
        ("nonzero", 5.0),
        ("timeout", 0.2),
        ("staging_tamper", 5.0),
    ),
)
def test_linux_real_cli_child_failures_leave_no_artifact_or_staging(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    timeout_seconds: float,
) -> None:
    package, trusted_base, runtime_root = _build_real_generation(tmp_path, mode=mode)
    output = tmp_path / "output"
    output.mkdir(mode=0o700)
    with _real_promotion_authority(
        tmp_path,
        package=package,
        trusted_base=trusted_base,
        runtime_root=runtime_root,
    ) as configuration_path:
        assert (
            _invoke_real_cli(
                monkeypatch,
                configuration_path=configuration_path,
                trusted_base=trusted_base,
                output=output,
                timeout_seconds=timeout_seconds,
            )
            == 2
        )

    assert not list(output.glob("strategy_lab_runs/*"))
    assert not list(output.glob(".formal-smoke-*"))


def _replace_generation_identity(
    runtime_root: Path,
    package: RuntimeCodeTestPackage,
    mutation: str,
) -> None:
    generation = runtime_root / "generations" / package.receipt.generation_id
    if mutation == "generation":
        displaced = generation.with_name(generation.name + ".displaced")
        generation.rename(displaced)
        generation.mkdir(mode=0o555)
        return
    relative = {
        "launcher": Path("release/bin/rquant"),
        "interpreter": Path("release/bin/python"),
        "import_root": Path("release/src"),
    }[mutation]
    target = generation / relative
    target.parent.chmod(0o755)
    if mutation == "import_root":
        displaced = target.with_name("src.displaced")
        target.rename(displaced)
        target.symlink_to(displaced.name, target_is_directory=True)
        return
    replacement = target.with_name(target.name + ".replacement")
    replacement.write_bytes(b"checkout-B replacement\n")
    replacement.chmod(0o555)
    os.replace(replacement, target)


@pytest.mark.parametrize(
    "mutation",
    ("generation", "launcher", "interpreter", "import_root"),
)
def test_linux_real_cli_rejects_replaced_generation_execution_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    package, trusted_base, runtime_root = _build_real_generation(
        tmp_path,
        mode="success",
    )
    _replace_generation_identity(runtime_root, package, mutation)
    output = tmp_path / "output"
    output.mkdir(mode=0o700)
    with _real_promotion_authority(
        tmp_path,
        package=package,
        trusted_base=trusted_base,
        runtime_root=runtime_root,
    ) as configuration_path:
        assert (
            _invoke_real_cli(
                monkeypatch,
                configuration_path=configuration_path,
                trusted_base=trusted_base,
                output=output,
            )
            == 2
        )

    assert not list(output.glob("strategy_lab_runs/*"))
    assert not list(output.glob(".formal-smoke-*"))
