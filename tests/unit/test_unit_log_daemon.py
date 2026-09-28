from __future__ import annotations

import base64
import importlib
import os
import shutil
import signal
import socket
import stat
import subprocess
import sys
import threading
from collections.abc import Iterator
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from rquant.ops_status import (
    STATIC_TIMER_STEMS,
    OpsInstallManifest,
    OpsUnitInstall,
    SignedOpsInstallManifest,
)
from rquant.unit_log_reader import JournalLogReader


@pytest.fixture
def installed() -> Iterator[tuple[list[str], Path]]:
    openssl = shutil.which("openssl")
    if openssl is None:
        pytest.skip("openssl is required for signed manifest tests")
    short_tmp = "/private/tmp" if Path("/private/tmp").is_dir() else "/tmp"
    with TemporaryDirectory(prefix="rqld-", dir=short_tmp) as directory:
        root = Path(directory)
        private = root / "signing.pem"
        public = root / "public.pem"
        payload = root / "manifest.payload"
        signature = root / "manifest.signature"
        subprocess.run(
            (openssl, "genpkey", "-algorithm", "ED25519", "-out", str(private)),
            check=True,
            capture_output=True,
        )
        subprocess.run(
            (openssl, "pkey", "-in", str(private), "-pubout", "-out", str(public)),
            check=True,
            capture_output=True,
        )
        public.chmod(0o644)
        host = socket.gethostname()
        manifest = OpsInstallManifest(
            version=1,
            host_name=host,
            units=tuple(
                OpsUnitInstall(
                    timer=f"rquant-{stem}.timer",
                    service=f"rquant-{stem}.service",
                    label="任务",
                    expected_enabled=True,
                    session="all",
                    resource_group="maintenance",
                )
                for stem in STATIC_TIMER_STEMS
            ),
        )
        payload.write_bytes(manifest.signing_bytes())
        subprocess.run(
            (
                openssl,
                "pkeyutl",
                "-sign",
                "-inkey",
                str(private),
                "-rawin",
                "-in",
                str(payload),
                "-out",
                str(signature),
            ),
            check=True,
            capture_output=True,
        )
        signed = SignedOpsInstallManifest(
            manifest=manifest,
            signature=base64.b64encode(signature.read_bytes()).decode("ascii"),
        )
        manifest_path = root / "manifest.json"
        manifest_path.write_bytes(signed.canonical_bytes())
        key = root / "cursor.key"
        key.write_bytes(b"sensitive-cursor-secret-" + b"x" * 32)
        key.chmod(0o600)
        socket_dir = root / "private"
        socket_dir.mkdir(mode=0o710)
        os.chown(socket_dir, -1, os.getegid())
        socket_dir.chmod(0o710)
        args = [
            "--socket-path",
            str(socket_dir / "logs.sock"),
            "--web-uid",
            str(os.geteuid() + 1),
            "--web-group-gid",
            str(os.getegid()),
            "--manifest-path",
            str(manifest_path),
            "--public-key-path",
            str(public),
            "--expected-host",
            host,
            "--cursor-key-file",
            str(key),
        ]
        yield args, root


def _replace_arg(args: list[str], name: str, value: str) -> list[str]:
    changed = args.copy()
    changed[changed.index(name) + 1] = value
    return changed


def _daemon() -> object:
    return importlib.import_module("rquant.unit_log_daemon")


def test_self_check_verifies_signed_install_without_journal_or_socket(
    installed: tuple[list[str], Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    daemon = _daemon()
    args, root = installed
    directory = (root / "private").lstat()
    assert (directory.st_uid, directory.st_gid, stat.S_IMODE(directory.st_mode)) == (
        os.geteuid(),
        os.getegid(),
        0o710,
    )

    def forbidden(*_args: object, **_kwargs: object) -> None:
        pytest.fail("self-check entered the journal or socket serve path")

    monkeypatch.setattr(JournalLogReader, "read", forbidden)
    monkeypatch.setattr(daemon.UnitLogService, "serve", forbidden)
    daemon._preflight(daemon._parser().parse_args([*args, "--self-check"]))
    assert daemon.main([*args, "--self-check"]) == 0


def test_module_entrypoint_runs_self_check_without_application_settings(
    installed: tuple[list[str], Path],
) -> None:
    args, root = installed
    source = Path(__file__).resolve().parents[2] / "src"
    result = subprocess.run(
        (sys.executable, "-m", "rquant.unit_log_daemon", *args, "--self-check"),
        cwd=root,
        env={"PYTHONPATH": str(source)},
        check=False,
        capture_output=True,
        timeout=5,
    )
    assert result.returncode == 0
    assert result.stdout == b""
    assert result.stderr == b""


def test_daemon_assembles_reader_and_stops_on_sigterm(
    installed: tuple[list[str], Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    daemon = _daemon()
    args, root = installed
    observed: dict[str, object] = {}
    old_term = signal.getsignal(signal.SIGTERM)
    old_int = signal.getsignal(signal.SIGINT)

    class StubService:
        def __init__(self, **kwargs: object) -> None:
            observed.update(kwargs)

        def serve(self, *, stop: threading.Event) -> None:
            assert not stop.is_set()
            handler = signal.getsignal(signal.SIGTERM)
            assert callable(handler)
            handler(signal.SIGTERM, None)
            assert stop.is_set()

    monkeypatch.setattr(daemon, "UnitLogService", StubService)
    assert daemon.main(args) == 0
    assert observed["socket_path"] == root / "private" / "logs.sock"
    assert observed["web_uid"] == os.geteuid() + 1
    assert observed["web_group_gid"] == os.getegid()
    assert isinstance(observed["reader"], JournalLogReader)
    assert signal.getsignal(signal.SIGTERM) is old_term
    assert signal.getsignal(signal.SIGINT) is old_int


@pytest.mark.parametrize(
    "failure", ["short", "oversize", "mode", "hardlink", "symlink", "ancestor"]
)
def test_cursor_key_rejects_unsafe_file(
    installed: tuple[list[str], Path], failure: str, capsys: pytest.CaptureFixture[str]
) -> None:
    daemon = _daemon()
    args, root = installed
    key = root / "cursor.key"
    if failure == "short":
        key.write_bytes(b"small")
    elif failure == "oversize":
        key.write_bytes(b"x" * 4097)
    elif failure == "mode":
        key.chmod(0o644)
    elif failure == "hardlink":
        os.link(key, root / "second-key")
    elif failure == "symlink":
        alias = root / "key-link"
        alias.symlink_to(key)
        args = _replace_arg(args, "--cursor-key-file", str(alias))
    else:
        alias_dir = root / "alias"
        alias_dir.symlink_to(root, target_is_directory=True)
        args = _replace_arg(args, "--cursor-key-file", str(alias_dir / key.name))
    assert daemon.main([*args, "--self-check"]) != 0
    output = capsys.readouterr()
    assert "sensitive-cursor-secret" not in output.out + output.err


@pytest.mark.parametrize("failure", ["symlink", "ancestor", "writable", "oversize"])
def test_public_key_rejects_unsafe_file(installed: tuple[list[str], Path], failure: str) -> None:
    daemon = _daemon()
    args, root = installed
    public = root / "public.pem"
    if failure == "symlink":
        alias = root / "public-link"
        alias.symlink_to(public)
        args = _replace_arg(args, "--public-key-path", str(alias))
    elif failure == "ancestor":
        alias_dir = root / "alias"
        alias_dir.symlink_to(root, target_is_directory=True)
        args = _replace_arg(args, "--public-key-path", str(alias_dir / public.name))
    elif failure == "writable":
        public.chmod(0o666)
    else:
        public.write_bytes(b"x" * 8193)
    assert daemon.main([*args, "--self-check"]) != 0


@pytest.mark.parametrize("failure", ["symlink", "ancestor", "bad_signature"])
def test_signed_manifest_rejects_unsafe_or_invalid_install(
    installed: tuple[list[str], Path], failure: str
) -> None:
    daemon = _daemon()
    args, root = installed
    manifest = root / "manifest.json"
    if failure == "symlink":
        alias = root / "manifest-link"
        alias.symlink_to(manifest)
        args = _replace_arg(args, "--manifest-path", str(alias))
    elif failure == "ancestor":
        alias_dir = root / "alias"
        alias_dir.symlink_to(root, target_is_directory=True)
        args = _replace_arg(args, "--manifest-path", str(alias_dir / manifest.name))
    else:
        manifest.write_bytes(b"{}")
    assert daemon.main([*args, "--self-check"]) != 0


@pytest.mark.parametrize(
    "flag", ["--cursor-key-file", "--public-key-path", "--manifest-path", "--socket-path"]
)
def test_relative_paths_are_rejected(installed: tuple[list[str], Path], flag: str) -> None:
    daemon = _daemon()
    args, _root = installed
    assert daemon.main([*_replace_arg(args, flag, "relative/path"), "--self-check"]) != 0


def test_web_uid_and_host_must_match_trust_boundary(installed: tuple[list[str], Path]) -> None:
    daemon = _daemon()
    args, _root = installed
    assert daemon.main([*_replace_arg(args, "--web-uid", str(os.geteuid())), "--self-check"]) != 0
    assert daemon.main([*_replace_arg(args, "--expected-host", "wrong-host"), "--self-check"]) != 0


def test_socket_directory_must_be_preexisting_and_private(
    installed: tuple[list[str], Path],
) -> None:
    daemon = _daemon()
    args, root = installed
    directory = root / "private"
    directory.chmod(0o750)
    assert daemon.main([*args, "--self-check"]) != 0
    directory.chmod(0o710)
    (directory / "logs.sock").write_text("occupied")
    assert daemon.main([*args, "--self-check"]) != 0


def test_public_key_replacement_during_read_is_rejected(
    installed: tuple[list[str], Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    daemon = _daemon()
    _args, root = installed
    public = root / "public.pem"
    original_read = os.read

    def replace_after_read(fd: int, count: int) -> bytes:
        payload = original_read(fd, count)
        replacement = root / "replacement.pem"
        replacement.write_bytes(payload)
        replacement.chmod(0o644)
        replacement.replace(public)
        return payload

    monkeypatch.setattr(daemon.os, "read", replace_after_read)
    with pytest.raises(ValueError):
        daemon._read_stable_file(public, max_bytes=8192, kind="public")


@pytest.mark.parametrize("kind", ["public", "key"])
def test_trust_file_replacement_after_manifest_check_is_rejected(
    installed: tuple[list[str], Path], monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    daemon = _daemon()
    args, root = installed
    target = root / ("public.pem" if kind == "public" else "cursor.key")
    original = daemon.load_signed_ops_manifest

    def replace_after_verify(*call_args: object, **call_kwargs: object) -> object:
        result = original(*call_args, **call_kwargs)
        replacement = root / "replacement"
        replacement.write_bytes(target.read_bytes())
        replacement.chmod(0o644 if kind == "public" else 0o600)
        replacement.replace(target)
        return result

    monkeypatch.setattr(daemon, "load_signed_ops_manifest", replace_after_verify)
    assert daemon.main([*args, "--self-check"]) != 0


def test_key_file_is_service_owned_regular_file(
    installed: tuple[list[str], Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    daemon = _daemon()
    _args, root = installed
    key = root / "cursor.key"
    original_fstat = os.fstat

    def wrong_owner(fd: int) -> os.stat_result:
        state = original_fstat(fd)
        values = list(state)
        values[stat.ST_UID] = os.geteuid() + 1
        return os.stat_result(values)

    monkeypatch.setattr(daemon.os, "fstat", wrong_owner)
    with pytest.raises(ValueError):
        daemon._read_stable_file(key, max_bytes=4096, kind="key")
