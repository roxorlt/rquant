from __future__ import annotations

import argparse
import signal
from pathlib import Path
from types import SimpleNamespace

import pytest

from rquant.cli import build_parser, cmd_lab_finalizer, cmd_lab_scheduler, cmd_lab_worker
from rquant.lab_daemon import LabDaemonConfigurationError

EXPECTED_ROOT = "/tmp/rquant-expected"
TRUSTED_GIT = "/usr/bin/git"
GENERATION = "1" * 40


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
            "--once",
        ]
    )
    legacy = build_parser().parse_args(["lab-run", "--spec", "/tmp/spec.json"])

    assert finalizer.command == "lab-finalizer"
    assert finalizer.once is True
    assert legacy.command == "lab-run"


def test_scheduler_rejects_missing_authority_configuration_before_sqlite(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import lab_daemon, lab_jobs
    from rquant.config import settings

    monkeypatch.setattr(
        lab_daemon,
        "require_lab_runtime_binding",
        lambda _root, _git: "1" * 40,
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
        lambda _root, _git: "1" * 40,
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
            )
        )


def test_finalizer_once_uses_readonly_reader_and_commit_spool(
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
            return SimpleNamespace(failed=0, model_dump_json=lambda: "{}")

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
        lambda _root, _git: "1" * 40,
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

    result = cmd_lab_finalizer(
        argparse.Namespace(
            once=True,
            expected_checkout_root=EXPECTED_ROOT,
            trusted_git_path=TRUSTED_GIT,
        )
    )

    assert result == 0
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
        lambda _root, _git: "1" * 40,
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
        lambda _root, _git: "1" * 40,
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
