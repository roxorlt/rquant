from __future__ import annotations

import argparse
import json
import os
import signal
import sqlite3
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from rquant.cli import (
    build_parser,
    cmd_lab_finalizer,
    cmd_lab_integrity_audit,
    cmd_lab_scheduler,
    cmd_lab_worker,
)
from rquant.lab_daemon import LabDaemonConfigurationError
from rquant.lab_jobs import LabJobStore
from tests.highwater_ed25519_support import export_public_keyring, write_private_manifest

EXPECTED_ROOT = "/tmp/rquant-expected"
TRUSTED_GIT = "/usr/bin/git"
GENERATION = "1" * 40
STARTUP_DEADLINE = 9_999_999_999.0


@pytest.fixture(autouse=True)
def _prepared_lab_runtime(monkeypatch: pytest.MonkeyPatch) -> None:
    from rquant import lab_daemon

    monkeypatch.setattr(lab_daemon, "verify_lab_runtime_prepared", lambda *_a, **_k: {})


class _FakeSqliteAuthority:
    def __init__(self, path: Path, calls: list[str] | None = None) -> None:
        self.path = path
        self.calls = calls

    def close(self) -> None:
        if self.calls is not None:
            self.calls.append("sqlite_close")


def test_parser_registers_finalizer_and_keeps_legacy_lab_run() -> None:
    finalizer = build_parser().parse_args(
        [
            "lab-finalizer",
            "--expected-checkout-root",
            EXPECTED_ROOT,
            "--trusted-git-path",
            TRUSTED_GIT,
            "--deployment-generation",
            GENERATION,
            "--deployment-lock-path",
            "/tmp/.rquant-deploy/rquant-expected.lock",
            "--deployment-generation-fd",
            "9",
            "--startup-deadline-monotonic",
            str(STARTUP_DEADLINE),
            "--deployment-operation-id",
            "a" * 32,
            "--deployment-environment-generation",
            "b" * 64,
            "--once",
        ]
    )
    legacy = build_parser().parse_args(["lab-run", "--spec", "/tmp/spec.json"])

    assert finalizer.command == "lab-finalizer"
    assert finalizer.once is True
    assert legacy.command == "lab-run"


def test_lab_integrity_audit_cli_reports_healthy_and_degraded_ledger(tmp_path: Path) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    parser = build_parser()
    healthy = parser.parse_args(["lab-integrity-audit", "--jobs-path", str(store.path)])

    assert healthy.command == "lab-integrity-audit"
    assert cmd_lab_integrity_audit(healthy) == 0

    with sqlite3.connect(store.path) as connection:
        connection.execute("DROP TABLE lab_job_list_summary")
    degraded = parser.parse_args(["lab-integrity-audit", "--jobs-path", str(store.path)])

    assert cmd_lab_integrity_audit(degraded) == 2


def test_lab_integrity_audit_cli_uses_external_highwater_and_emits_machine_receipt(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    private_keyring, _public_key = write_private_manifest(
        tmp_path / "authority-private-keys.json",
        active_key_id="hw-v1",
    )
    trusted_keyring = export_public_keyring(
        private_keyring,
        tmp_path / "trusted-public-keys.json",
    )
    helper = (
        Path(__file__).resolve().parents[2]
        / "deploy"
        / "libexec"
        / "rquant-lab-highwater-authority"
    )
    args = build_parser().parse_args(
        [
            "lab-integrity-audit",
            "--jobs-path",
            str(store.path),
            "--require-external-highwater",
            "--highwater-command-json",
            json.dumps(
                [
                    sys.executable,
                    str(helper),
                    "--state-root",
                    str(tmp_path / "highwater-state"),
                    "--keys-file",
                    str(private_keyring),
                ]
            ),
            "--highwater-stable-identity",
            "lab-test-ledger",
            "--highwater-code-identity",
            "1" * 40,
            "--highwater-profile-identity",
            "2" * 64,
            "--highwater-trusted-keyring",
            str(trusted_keyring),
            "--machine-receipt",
        ]
    )

    assert cmd_lab_integrity_audit(args) == 0
    payload = json.loads(capsys.readouterr().out)
    assert len(payload["receipt_hash"]) == 64


def test_parser_registers_generation_bound_launchd_install_lifecycle() -> None:
    parser = build_parser()
    install = parser.parse_args(
        [
            "lab-launchd-install",
            "--expected-checkout-root",
            EXPECTED_ROOT,
            "--deployment-lock-path",
            "/tmp/.rquant-deploy/rquant.lock",
            "--no-activate",
        ]
    )
    uninstall = parser.parse_args(
        [
            "lab-launchd-uninstall",
            "--expected-checkout-root",
            EXPECTED_ROOT,
            "--deployment-lock-path",
            "/tmp/.rquant-deploy/rquant.lock",
            "--no-deactivate",
        ]
    )

    assert install.command == "lab-launchd-install"
    assert install.no_activate is True
    assert uninstall.command == "lab-launchd-uninstall"
    assert uninstall.no_deactivate is True


@pytest.mark.parametrize(
    "label",
    (
        "com.roxor.rquant-lab-scheduler",
        "com.roxor.rquant-lab-worker",
        "com.roxor.rquant-lab-finalizer",
    ),
)
def test_cli_readiness_context_releases_lease_when_heartbeat_thread_start_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    label: str,
) -> None:
    from rquant import lab_daemon
    from rquant.cli import _lab_daemon_readiness_context
    from rquant.config import settings

    deployment_lock = tmp_path / "deployment.lock"
    deployment_fd = os.open(deployment_lock, os.O_RDWR | os.O_CREAT, 0o600)
    daemon_authority = tmp_path / "daemon.lock"
    daemon_fd = os.open(daemon_authority, os.O_RDWR | os.O_CREAT, 0o600)
    duplicated: list[int] = []
    primary = OSError("start boom")

    class RuntimeGuard:
        @staticmethod
        def verify_identity(_identity: object) -> str:
            return "c" * 40

    class DaemonLock:
        @staticmethod
        def duplicate_authority_lease() -> int:
            descriptor = os.dup(daemon_fd)
            duplicated.append(descriptor)
            return descriptor

    class FailingThread:
        def __init__(self, **_kwargs: object) -> None:
            pass

        def start(self) -> None:
            raise primary

        def join(self, *, timeout: float) -> None:
            pytest.fail(f"unstarted thread was joined with timeout={timeout}")

        def is_alive(self) -> bool:
            return False

    monkeypatch.setattr(lab_daemon, "Thread", FailingThread)
    monkeypatch.setattr(settings, "lab_readiness_dir", tmp_path / "readiness")
    args = argparse.Namespace(
        deployment_generation=GENERATION,
        deployment_lock_path=deployment_lock,
        deployment_generation_fd=deployment_fd,
        deployment_operation_id="a" * 32,
        deployment_environment_generation="b" * 64,
    )
    try:
        context = _lab_daemon_readiness_context(
            args,
            label=label,
            code_sha="c" * 40,
            runtime_guard=RuntimeGuard(),
            runtime_identity=object(),
            daemon_lock=DaemonLock(),
        )
        with pytest.raises(OSError) as caught, context:
            pytest.fail("heartbeat thread start failure must not enter the daemon body")
        assert caught.value is primary
        assert len(duplicated) == 1
        with pytest.raises(OSError):
            os.fstat(duplicated[0])
    finally:
        os.close(daemon_fd)
        os.close(deployment_fd)


def test_cli_readiness_heartbeat_uses_verified_identity_without_full_verify(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.cli import _lab_daemon_readiness_context
    from rquant.config import settings
    from rquant.lab_daemon import LabDaemonReadinessPublisher, LabRuntimeGuard

    checkout = tmp_path / "checkout"
    (checkout / "src" / "rquant").mkdir(parents=True)
    (checkout / "src" / "rquant" / "__init__.py").write_text("", encoding="utf-8")
    (checkout / "pyproject.toml").write_text("[project]\nname='fixture'\n", encoding="utf-8")
    (checkout / "uv.lock").write_text("version = 1\n", encoding="utf-8")
    full_verify_threads: list[threading.Thread] = []

    def full_verify(_root: Path) -> str:
        full_verify_threads.append(threading.current_thread())
        if threading.current_thread() is not threading.main_thread():
            raise ValueError("signal only works in main thread")
        return "c" * 40

    runtime_guard = LabRuntimeGuard(checkout, "c" * 40, verifier=full_verify)
    identity = runtime_guard.verify_runtime_identity()
    deployment_lock = tmp_path / "deployment.lock"
    deployment_fd = os.open(deployment_lock, os.O_RDWR | os.O_CREAT, 0o600)
    daemon_authority = tmp_path / "daemon.lock"
    daemon_fd = os.open(daemon_authority, os.O_RDWR | os.O_CREAT, 0o600)

    class DaemonLock:
        @staticmethod
        def duplicate_authority_lease() -> int:
            return os.dup(daemon_fd)

    monkeypatch.setattr(settings, "lab_readiness_dir", tmp_path / "readiness")
    args = argparse.Namespace(
        deployment_generation=GENERATION,
        deployment_lock_path=deployment_lock,
        deployment_generation_fd=deployment_fd,
        deployment_operation_id="a" * 32,
        deployment_environment_generation="b" * 64,
    )
    try:
        context = _lab_daemon_readiness_context(
            args,
            label="com.roxor.rquant-lab-worker",
            code_sha="c" * 40,
            runtime_guard=runtime_guard,
            runtime_identity=identity,
            daemon_lock=DaemonLock(),
        )
        assert isinstance(context, LabDaemonReadinessPublisher)
        context.heartbeat_interval_seconds = 0.02
        with context:
            first = LabDaemonReadinessPublisher.read(
                deployment_lock_path=deployment_lock,
                label="com.roxor.rquant-lab-worker",
                readiness_root=tmp_path / "readiness",
            )
            deadline = time.monotonic() + 1
            observed = first
            while observed.heartbeat_monotonic == first.heartbeat_monotonic:
                assert time.monotonic() < deadline
                time.sleep(0.01)
                observed = LabDaemonReadinessPublisher.read(
                    deployment_lock_path=deployment_lock,
                    label="com.roxor.rquant-lab-worker",
                    readiness_root=tmp_path / "readiness",
                )
            assert context._thread is not None
            assert context._thread.is_alive()
            assert not context._stop.is_set()
        assert full_verify_threads == [threading.main_thread()]
    finally:
        os.close(daemon_fd)
        os.close(deployment_fd)


def test_cli_establishes_one_full_proof_for_repeated_mutation_guards(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import lab_daemon
    from rquant.cli import _establish_lab_runtime_identity

    checkout = tmp_path / "checkout"
    (checkout / "src" / "rquant").mkdir(parents=True)
    (checkout / "src" / "rquant" / "__init__.py").write_text("", encoding="utf-8")
    (checkout / "pyproject.toml").write_text("[project]\nname='fixture'\n", encoding="utf-8")
    (checkout / "uv.lock").write_text("version = 1\n", encoding="utf-8")
    full_verify_calls = 0

    def full_verify(_root: Path, _git: Path, **_kwargs: object) -> str:
        nonlocal full_verify_calls
        full_verify_calls += 1
        return "d" * 40

    monkeypatch.setattr(lab_daemon, "require_lab_runtime_binding", full_verify)
    code_sha, runtime_guard, identity, mutation_guard = _establish_lab_runtime_identity(
        argparse.Namespace(
            expected_checkout_root=checkout,
            trusted_git_path=Path(TRUSTED_GIT),
            startup_deadline_monotonic=STARTUP_DEADLINE,
        )
    )

    assert code_sha == "d" * 40
    assert identity.code_sha == code_sha
    for _ in range(100):
        assert mutation_guard() == code_sha
    assert full_verify_calls == 1
    assert mutation_guard.func.__self__ is runtime_guard
    assert mutation_guard.args == (identity,)


def test_scheduler_rejects_missing_authority_configuration_before_sqlite(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import job_center_authority, lab_daemon, lab_jobs
    from rquant.config import settings

    monkeypatch.setattr(
        lab_daemon,
        "require_lab_runtime_binding",
        lambda _root, _git, **_kwargs: "1" * 40,
    )
    monkeypatch.setattr(
        lab_daemon,
        "load_lab_job_center_authority_manifest",
        lambda *_args, **_kwargs: SimpleNamespace(),
    )
    runtime_root = settings.lab_runtime_dir_resolved
    monkeypatch.setattr(
        job_center_authority,
        "resolve_current_job_center_authority_binding",
        lambda *_args, **_kwargs: SimpleNamespace(
            runtime_root=runtime_root,
            lab_jobs_path=settings.lab_jobs_path_resolved,
            command_spool_path=settings.lab_job_command_dir_resolved,
            final_artifact_root=settings.lab_final_artifact_dir_resolved,
            runtime_deployment_root=Path("/tmp/rquant-production-runtime"),
            deployment_profile_id="2" * 64,
            deployment_generation_hash="3" * 64,
        ),
    )
    monkeypatch.setattr(settings, "lab_finalizer_authority_key_id", "")
    monkeypatch.setattr(settings, "lab_finalizer_authority_key_path", None)
    monkeypatch.setattr(settings, "lab_finalizer_authority_keyring_path", None)
    monkeypatch.setattr(
        lab_jobs,
        "LabJobStore",
        lambda *_args, **_kwargs: pytest.fail("scheduler opened SQLite before key validation"),
    )

    with pytest.raises(LabDaemonConfigurationError, match="incomplete"):
        cmd_lab_scheduler(
            argparse.Namespace(
                once=True,
                expected_checkout_root=EXPECTED_ROOT,
                trusted_git_path=TRUSTED_GIT,
                runtime_deployment_root="/tmp/rquant-production-runtime",
                startup_deadline_monotonic=STARTUP_DEADLINE,
            )
        )


def test_worker_rejects_unlisted_identity_before_constructing_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import lab_daemon, lab_worker
    from rquant.config import settings

    monkeypatch.setattr(settings, "lab_worker_id", "worker-a")
    monkeypatch.setattr(settings, "lab_scheduler_worker_ids", "worker-b")
    monkeypatch.setattr(
        lab_daemon,
        "require_lab_runtime_binding",
        lambda _root, _git, **_kwargs: "1" * 40,
    )
    monkeypatch.setattr(
        lab_worker,
        "LabWorker",
        lambda **_kwargs: pytest.fail("worker constructed before allowlist validation"),
    )

    with pytest.raises(LabDaemonConfigurationError, match="allowlist"):
        cmd_lab_worker(
            argparse.Namespace(
                worker_id="worker-a",
                once=True,
                expected_checkout_root=EXPECTED_ROOT,
                trusted_git_path=TRUSTED_GIT,
                startup_deadline_monotonic=STARTUP_DEADLINE,
            )
        )


@pytest.mark.parametrize(
    ("outcome", "expected_result"),
    (("success", 0), ("failed", 1), ("exception", None)),
)
def test_finalizer_once_closes_authority_after_success_failure_or_exception(
    monkeypatch: pytest.MonkeyPatch,
    outcome: str,
    expected_result: int | None,
) -> None:
    from rquant import (
        lab_artifact_protocol,
        lab_artifacts,
        lab_daemon,
        lab_finalizer,
        lab_jobs,
    )
    from rquant.config import settings

    calls: list[str] = []

    class FakeReader:
        def __init__(
            self,
            path: Path,
            *,
            busy_timeout_ms: int,
            identity_authority: object,
        ) -> None:
            calls.append(f"reader:{path.name}:{busy_timeout_ms}")
            assert isinstance(identity_authority, _FakeSqliteAuthority)

    class FakeSpool:
        def __init__(self, path: Path, *, mutation_guard: object) -> None:
            calls.append(f"spool:{path.name}")
            assert callable(mutation_guard)

    class FakeStore:
        def __init__(self, path: Path, *, mutation_guard: object) -> None:
            calls.append(f"store:{path.name}")
            assert callable(mutation_guard)

        def close(self) -> None:
            calls.append("store_close")

    class FakeKeyring:
        def signing_key(self) -> object:
            return object()

        def verification_key(self, _key_id: str) -> None:
            return None

    class FakeFinalizer:
        def __init__(self, **kwargs: object) -> None:
            calls.append("finalizer")
            assert isinstance(kwargs["reader"], FakeReader)

    class FakeDaemon:
        def __init__(self, **kwargs: object) -> None:
            calls.append("daemon")
            assert isinstance(kwargs["reader"], FakeReader)
            assert isinstance(kwargs["finalizer"], FakeFinalizer)

        def run_once(self) -> SimpleNamespace:
            calls.append("run_once")
            if outcome == "exception":
                raise RuntimeError("finalizer exception")
            return SimpleNamespace(failed=outcome == "failed", model_dump_json=lambda: "{}")

    class FakeLock:
        def __init__(self, _path: Path, name: str, *, mutation_guard: object) -> None:
            calls.append(f"lock:{name}")
            assert callable(mutation_guard)

        def __enter__(self) -> FakeLock:
            return self

        def __exit__(self, *_args: object) -> None:
            calls.append("unlock")

    monkeypatch.setattr(lab_jobs, "LabJobReader", FakeReader)
    monkeypatch.setattr(lab_artifact_protocol, "LabArtifactCommitSpool", FakeSpool)
    monkeypatch.setattr(lab_artifacts, "LabJobArtifactStore", FakeStore)
    monkeypatch.setattr(lab_finalizer, "LabFinalizer", FakeFinalizer)
    monkeypatch.setattr(lab_daemon, "LabFinalizerDaemon", FakeDaemon)
    monkeypatch.setattr(lab_daemon, "LabDaemonLock", FakeLock)
    monkeypatch.setattr(
        lab_daemon,
        "require_lab_runtime_binding",
        lambda _root, _git, **_kwargs: "1" * 40,
    )
    monkeypatch.setattr(
        lab_daemon,
        "prepare_private_sqlite_path",
        lambda path, *, label, create, mutation_guard: (
            calls.append(f"sqlite:{path.name}:{label}:{create}")
            or _FakeSqliteAuthority(path, calls)
        ),
    )
    monkeypatch.setattr(
        lab_daemon,
        "ensure_private_directory",
        lambda path, *, label, mutation_guard: path,
    )
    monkeypatch.setattr(lab_daemon, "require_unique_runtime_paths", lambda _paths: None)
    monkeypatch.setattr(
        lab_daemon.LabAuthorityKeyring,
        "load",
        classmethod(lambda cls, **kwargs: FakeKeyring()),
    )
    monkeypatch.setattr(settings, "lab_finalizer_authority_key_id", "active")
    monkeypatch.setattr(settings, "lab_finalizer_authority_key_path", Path("/tmp/key"))
    monkeypatch.setattr(
        settings,
        "lab_finalizer_authority_keyring_path",
        Path("/tmp/keyring"),
    )
    monkeypatch.setattr("rquant.cli.setup_logging", lambda: None)

    args = argparse.Namespace(
        once=True,
        expected_checkout_root=EXPECTED_ROOT,
        trusted_git_path=TRUSTED_GIT,
        startup_deadline_monotonic=STARTUP_DEADLINE,
    )

    if expected_result is None:
        with pytest.raises(RuntimeError, match="finalizer exception"):
            cmd_lab_finalizer(args)
    else:
        assert cmd_lab_finalizer(args) == expected_result
    assert "reader:lab_jobs.sqlite3:5000" in calls
    assert "sqlite:lab_jobs.sqlite3:lab jobs SQLite:False" in calls
    assert "spool:artifact-commits" in calls
    assert "store:final-artifacts" in calls
    assert calls[-4:] == ["run_once", "store_close", "sqlite_close", "unlock"]


def test_finalizer_requires_prepared_runtime_before_state_or_sqlite_access(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import lab_daemon
    from rquant.config import settings

    monkeypatch.setattr(
        lab_daemon,
        "require_lab_runtime_binding",
        lambda _root, _git, **_kwargs: "1" * 40,
    )
    monkeypatch.setattr(
        lab_daemon,
        "verify_lab_runtime_prepared",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            LabDaemonConfigurationError("prepared sentinel missing")
        ),
    )
    monkeypatch.setattr(
        lab_daemon,
        "ensure_private_directory",
        lambda *_args, **_kwargs: pytest.fail(
            "finalizer touched state before prepared sentinel validation"
        ),
    )
    monkeypatch.setattr(settings, "lab_finalizer_authority_key_id", "active")
    monkeypatch.setattr(settings, "lab_finalizer_authority_key_path", Path("/tmp/key"))
    monkeypatch.setattr(
        settings,
        "lab_finalizer_authority_keyring_path",
        Path("/tmp/keyring"),
    )

    with pytest.raises(LabDaemonConfigurationError, match="prepared sentinel"):
        cmd_lab_finalizer(
            argparse.Namespace(
                once=True,
                expected_checkout_root=EXPECTED_ROOT,
                trusted_git_path=TRUSTED_GIT,
                startup_deadline_monotonic=STARTUP_DEADLINE,
            )
        )


def test_finalizer_forever_installs_both_stop_signals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import (
        lab_artifact_protocol,
        lab_artifacts,
        lab_daemon,
        lab_finalizer,
        lab_jobs,
    )
    from rquant.config import settings

    handlers: dict[int, object] = {}
    calls: list[str] = []

    class FakeReader:
        def __init__(
            self,
            _path: Path,
            *,
            busy_timeout_ms: int,
            identity_authority: object,
        ) -> None:
            assert busy_timeout_ms > 0
            assert isinstance(identity_authority, _FakeSqliteAuthority)

    class FakeSpool:
        def __init__(self, _path: Path, *, mutation_guard: object) -> None:
            assert callable(mutation_guard)

    class FakeStore:
        def __init__(self, _path: Path, *, mutation_guard: object) -> None:
            assert callable(mutation_guard)

        def close(self) -> None:
            calls.append("close")

    class FakeKeyring:
        def signing_key(self) -> object:
            return object()

        def verification_key(self, _key_id: str) -> None:
            return None

    class FakeFinalizer:
        def __init__(self, **_kwargs: object) -> None:
            pass

    class FakeDaemon:
        def __init__(self, **_kwargs: object) -> None:
            pass

        def request_stop(self) -> None:
            calls.append("request_stop")

        def run_forever(self) -> None:
            calls.append("run_forever")
            for signum in (signal.SIGINT, signal.SIGTERM):
                handler = handlers[signum]
                assert callable(handler)
                handler(signum, None)

    class FakeLock:
        def __init__(self, *_args: object, mutation_guard: object) -> None:
            assert callable(mutation_guard)

        def __enter__(self) -> FakeLock:
            return self

        def __exit__(self, *_args: object) -> None:
            pass

    def fake_signal(signum: int, handler: object) -> object:
        previous = handlers.get(signum, signal.SIG_DFL)
        handlers[signum] = handler
        return previous

    monkeypatch.setattr(lab_jobs, "LabJobReader", FakeReader)
    monkeypatch.setattr(lab_artifact_protocol, "LabArtifactCommitSpool", FakeSpool)
    monkeypatch.setattr(lab_artifacts, "LabJobArtifactStore", FakeStore)
    monkeypatch.setattr(lab_finalizer, "LabFinalizer", FakeFinalizer)
    monkeypatch.setattr(lab_daemon, "LabFinalizerDaemon", FakeDaemon)
    monkeypatch.setattr(lab_daemon, "LabDaemonLock", FakeLock)
    monkeypatch.setattr(
        lab_daemon,
        "require_lab_runtime_binding",
        lambda _root, _git, **_kwargs: "1" * 40,
    )
    monkeypatch.setattr(
        lab_daemon,
        "prepare_private_sqlite_path",
        lambda path, *, label, create, mutation_guard: _FakeSqliteAuthority(path, calls),
    )
    monkeypatch.setattr(
        lab_daemon,
        "ensure_private_directory",
        lambda path, *, label, mutation_guard: path,
    )
    monkeypatch.setattr(lab_daemon, "require_unique_runtime_paths", lambda _paths: None)
    monkeypatch.setattr(
        lab_daemon.LabAuthorityKeyring,
        "load",
        classmethod(lambda cls, **kwargs: FakeKeyring()),
    )
    monkeypatch.setattr(settings, "lab_finalizer_authority_key_id", "active")
    monkeypatch.setattr(settings, "lab_finalizer_authority_key_path", Path("/tmp/key"))
    monkeypatch.setattr(
        settings,
        "lab_finalizer_authority_keyring_path",
        Path("/tmp/keyring"),
    )
    monkeypatch.setattr("rquant.cli.setup_logging", lambda: None)
    monkeypatch.setattr(signal, "signal", fake_signal)

    result = cmd_lab_finalizer(
        argparse.Namespace(
            once=False,
            expected_checkout_root=EXPECTED_ROOT,
            trusted_git_path=TRUSTED_GIT,
            startup_deadline_monotonic=STARTUP_DEADLINE,
        )
    )

    assert result == 0
    assert calls == [
        "run_forever",
        "request_stop",
        "request_stop",
        "close",
        "sqlite_close",
    ]
