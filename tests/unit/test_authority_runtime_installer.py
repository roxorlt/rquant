from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import re
import socket
import stat
import subprocess
import tempfile
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[2]
INSTALLER = ROOT / "scripts" / "install-resource-authority-infra.sh"
PUBLISHER = ROOT / "scripts" / "publish-authority-runtime.py"
RELEASE_SHA = "a" * 40
PUBLISHER_VERSION = "rquant-authority-runtime-publisher/v2"


def _load_publisher() -> ModuleType:
    spec = importlib.util.spec_from_file_location("authority_runtime_installer", PUBLISHER)
    if spec is None or spec.loader is None:
        raise AssertionError("publisher module is unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _manifest(files: dict[str, tuple[bytes, int]]) -> bytes:
    publisher_sha256 = hashlib.sha256(PUBLISHER.read_bytes()).hexdigest()
    return _canonical(
        {
            "contract": "rquant-authority-runtime-manifest/v2",
            "executable": "venv/bin/rquant",
            "files": [
                {
                    "mode": mode,
                    "path": path,
                    "sha256": hashlib.sha256(payload).hexdigest(),
                    "size": len(payload),
                }
                for path, (payload, mode) in sorted(files.items())
            ],
            "publisher_sha256": publisher_sha256,
            "publisher_version": PUBLISHER_VERSION,
            "release_sha": RELEASE_SHA,
            "schema_version": 2,
        }
    )


def _write_candidate(
    root: Path,
    *,
    files: dict[str, tuple[bytes, int]] | None = None,
) -> tuple[Path, dict[str, tuple[bytes, int]]]:
    contents = files or {
        "pkg/module.py": (b"VALUE = 1\n", 0o444),
        "venv/bin/rquant": (b"#!/bin/sh\nexit 0\n", 0o555),
    }
    candidate = root / "candidate"
    payload_root = candidate / "payload"
    payload_root.mkdir(parents=True, mode=0o755)
    for relative, (payload, mode) in contents.items():
        path = payload_root.joinpath(*relative.split("/"))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        path.chmod(mode)
    for directory in sorted(
        (path for path in payload_root.rglob("*") if path.is_dir()),
        key=lambda path: len(path.parts),
        reverse=True,
    ):
        directory.chmod(0o555)
    payload_root.chmod(0o555)
    manifest = candidate / "manifest.json"
    manifest.write_bytes(_manifest(contents))
    manifest.chmod(0o444)
    candidate.chmod(0o500)
    return candidate, contents


def _signing_key(root: Path) -> Path:
    private_key = root / "runtime.private.pem"
    completed = subprocess.run(
        ("openssl", "genpkey", "-algorithm", "Ed25519", "-out", str(private_key)),
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
    private_key.chmod(0o400)
    return private_key


def _publish(module: ModuleType, root: Path, candidate: Path, key: Path) -> Path:
    generations = root / "generations"
    generations.mkdir(mode=0o755)
    return module.publish_authority_runtime(
        candidate_root=candidate,
        generations_root=generations,
        release_sha=RELEASE_SHA,
        signing_private_key_path=key,
        expected_source_uid=os.geteuid(),
        expected_source_gid=os.getegid(),
        published_uid=os.geteuid(),
        published_gid=os.getegid(),
        expected_publisher_sha256=hashlib.sha256(PUBLISHER.read_bytes()).hexdigest(),
        expected_publisher_version=PUBLISHER_VERSION,
    )


def _write_unsealed_candidate(
    root: Path,
    *,
    files: dict[str, tuple[bytes, int]] | None = None,
) -> tuple[Path, dict[str, tuple[bytes, int]]]:
    contents = files or {
        "pkg/module.py": (b"VALUE = 1\n", 0o644),
        "venv/bin/rquant": (b"#!/bin/sh\nexit 0\n", 0o755),
    }
    candidate = root / "candidate"
    payload = candidate / "payload"
    payload.mkdir(parents=True, mode=0o700)
    for relative, (body, mode) in contents.items():
        target = payload.joinpath(*relative.split("/"))
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body)
        target.chmod(mode)
    candidate.chmod(0o700)
    return candidate, contents


def _seal(module: ModuleType, candidate: Path) -> Path:
    return module.seal_authority_runtime_candidate(
        candidate_root=candidate,
        release_sha=RELEASE_SHA,
        build_root_bytes=os.fsencode(candidate),
        runtime_release_bytes=os.fsencode(candidate.parent / "runtime-release"),
        expected_source_uid=os.geteuid(),
        expected_source_gid=os.getegid(),
        publisher_sha256=hashlib.sha256(PUBLISHER.read_bytes()).hexdigest(),
        publisher_version=PUBLISHER_VERSION,
    )


def _make_removable(path: Path) -> None:
    if path.is_symlink():
        path.unlink()
        return
    if not path.exists():
        return
    if path.is_dir():
        path.chmod(0o700)
        for child in path.iterdir():
            _make_removable(child)
        return
    path.chmod(0o600)


def test_secure_publisher_copies_normal_payload_and_signs_manifest(tmp_path: Path) -> None:
    module = _load_publisher()
    candidate, files = _write_candidate(tmp_path)
    key = _signing_key(tmp_path)

    release = _publish(module, tmp_path, candidate, key)

    assert release == tmp_path / "generations" / RELEASE_SHA
    assert stat.S_IMODE(release.stat().st_mode) == 0o555
    assert (release / "manifest.json").read_bytes() == _manifest(files)
    assert (release / "manifest.sha256").read_text(encoding="ascii") == (
        hashlib.sha256(_manifest(files)).hexdigest() + "\n"
    )
    assert (release / "manifest.sig").is_file()
    for relative, (payload, mode) in files.items():
        installed = release / "payload" / relative
        assert installed.read_bytes() == payload
        assert stat.S_IMODE(installed.stat().st_mode) == mode
        assert not installed.is_symlink()


@pytest.mark.parametrize(
    "link_kind",
    ("external-file", "external-directory", "private-key", "internal"),
)
def test_secure_publisher_rejects_every_payload_symlink(
    tmp_path: Path,
    link_kind: str,
) -> None:
    module = _load_publisher()
    candidate, _files = _write_candidate(tmp_path)
    key = _signing_key(tmp_path)
    payload = candidate / "payload"
    candidate.chmod(0o700)
    payload.chmod(0o755)
    package = payload / "pkg"
    package.chmod(0o755)
    target = package / "link"
    if link_kind == "external-file":
        external = tmp_path / "external.txt"
        external.write_text("outside", encoding="ascii")
        target.symlink_to(external)
    elif link_kind == "external-directory":
        external = tmp_path / "external-directory"
        external.mkdir()
        target.symlink_to(external, target_is_directory=True)
    elif link_kind == "private-key":
        target.symlink_to(key)
    else:
        target.symlink_to("module.py")
    package.chmod(0o555)
    payload.chmod(0o555)
    candidate.chmod(0o500)

    with pytest.raises(module.AuthorityRuntimeInstallError, match="symlink"):
        _publish(module, tmp_path, candidate, key)

    assert not (tmp_path / "generations" / RELEASE_SHA).exists()


def test_secure_publisher_rejects_payload_hardlinks(tmp_path: Path) -> None:
    module = _load_publisher()
    candidate, _files = _write_candidate(tmp_path)
    key = _signing_key(tmp_path)
    payload = candidate / "payload"
    candidate.chmod(0o700)
    payload.chmod(0o755)
    package = payload / "pkg"
    package.chmod(0o755)
    os.link(package / "module.py", package / "module-copy.py")
    package.chmod(0o555)
    payload.chmod(0o555)
    candidate.chmod(0o500)

    with pytest.raises(module.AuthorityRuntimeInstallError, match="hardlink"):
        _publish(module, tmp_path, candidate, key)


@pytest.mark.parametrize("special_kind", ("fifo", "socket"))
def test_secure_publisher_rejects_special_files(tmp_path: Path, special_kind: str) -> None:
    module = _load_publisher()
    candidate_root = tmp_path
    temporary: tempfile.TemporaryDirectory[str] | None = None
    if special_kind == "socket":
        temporary = tempfile.TemporaryDirectory(prefix="rqi-", dir="/tmp")
        candidate_root = Path(temporary.name)
        os.chown(candidate_root, os.geteuid(), os.getegid())
    candidate, _files = _write_candidate(candidate_root)
    key = _signing_key(tmp_path)
    payload = candidate / "payload"
    candidate.chmod(0o700)
    payload.chmod(0o755)
    special = payload / "special"
    listener: socket.socket | None = None
    if special_kind == "fifo":
        os.mkfifo(special)
    else:
        listener = socket.socket(socket.AF_UNIX)
        listener.bind(str(special))
    payload.chmod(0o555)
    candidate.chmod(0o500)
    try:
        with pytest.raises(module.AuthorityRuntimeInstallError, match="special"):
            _publish(module, tmp_path, candidate, key)
    finally:
        try:
            if listener is not None:
                listener.close()
        finally:
            try:
                payload.chmod(0o700)
                special.unlink(missing_ok=True)
            finally:
                if temporary is not None:
                    temporary.cleanup()


@pytest.mark.parametrize(
    ("paths", "message"),
    (
        (("../escape", "venv/bin/rquant"), "unsafe"),
        (("pkg/A.py", "pkg/a.py", "venv/bin/rquant"), "case"),
        (("pkg/a.py", "pkg/a.py", "venv/bin/rquant"), "duplicate"),
    ),
)
def test_secure_publisher_rejects_unsafe_or_conflicting_manifest_paths(
    tmp_path: Path,
    paths: tuple[str, ...],
    message: str,
) -> None:
    module = _load_publisher()
    candidate, _files = _write_candidate(tmp_path)
    key = _signing_key(tmp_path)
    manifest = {
        "contract": "rquant-authority-runtime-manifest/v2",
        "executable": "venv/bin/rquant",
        "files": [
            {
                "mode": 0o555 if path == "venv/bin/rquant" else 0o444,
                "path": path,
                "sha256": "0" * 64,
                "size": 0,
            }
            for path in paths
        ],
        "publisher_sha256": hashlib.sha256(PUBLISHER.read_bytes()).hexdigest(),
        "publisher_version": PUBLISHER_VERSION,
        "release_sha": RELEASE_SHA,
        "schema_version": 2,
    }
    candidate.chmod(0o700)
    manifest_path = candidate / "manifest.json"
    manifest_path.chmod(0o600)
    manifest_path.write_bytes(_canonical(manifest))
    manifest_path.chmod(0o444)
    candidate.chmod(0o500)

    with pytest.raises(module.AuthorityRuntimeInstallError, match=message):
        _publish(module, tmp_path, candidate, key)


def test_secure_publisher_rejects_file_replaced_between_inventory_and_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_publisher()
    candidate, _files = _write_candidate(tmp_path)
    key = _signing_key(tmp_path)
    private = tmp_path / "do-not-read.private"
    private.write_text("PRIVATE", encoding="ascii")
    original = module._open_regular_at
    replaced = False

    def replace_before_open(parent_fd: int, name: str, *args: object, **kwargs: object) -> int:
        nonlocal replaced
        if name == "module.py" and not replaced:
            replaced = True
            os.fchmod(parent_fd, 0o755)
            os.unlink(name, dir_fd=parent_fd)
            os.symlink(private, name, dir_fd=parent_fd)
            os.fchmod(parent_fd, 0o555)
        return original(parent_fd, name, *args, **kwargs)

    monkeypatch.setattr(module, "_open_regular_at", replace_before_open)

    with pytest.raises(module.AuthorityRuntimeInstallError, match="symlink|identity|changed"):
        _publish(module, tmp_path, candidate, key)

    assert replaced
    assert not (tmp_path / "generations" / RELEASE_SHA).exists()
    candidate.chmod(0o700)
    payload = candidate / "payload"
    payload.chmod(0o700)
    package = payload / "pkg"
    package.chmod(0o700)
    (package / "module.py").unlink()
    _make_removable(candidate)


@pytest.mark.parametrize("entry", ("manifest", "payload"))
def test_secure_publisher_rejects_candidate_entry_replaced_after_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    entry: str,
) -> None:
    module = _load_publisher()
    candidate, _files = _write_candidate(tmp_path)
    key = _signing_key(tmp_path)
    private = tmp_path / "do-not-read.private"
    private.write_text("PRIVATE", encoding="ascii")
    replaced = False

    if entry == "manifest":
        original = module._read_bounded

        def replace_after_read(
            descriptor: int,
            *,
            max_bytes: int,
            label: str,
        ) -> bytes:
            nonlocal replaced
            result = original(descriptor, max_bytes=max_bytes, label=label)
            if "candidate manifest" in label and not replaced:
                replaced = True
                candidate.chmod(0o700)
                (candidate / "manifest.json").unlink()
                (candidate / "manifest.json").symlink_to(private)
                candidate.chmod(0o500)
            return result

        monkeypatch.setattr(module, "_read_bounded", replace_after_read)
    else:
        original = module._scan_payload
        external = tmp_path / "external-payload"
        external.mkdir()

        def replace_after_scan(
            payload_fd: int,
            *,
            expected_uid: int,
            expected_gid: int,
        ) -> object:
            nonlocal replaced
            result = original(
                payload_fd,
                expected_uid=expected_uid,
                expected_gid=expected_gid,
            )
            if not replaced:
                replaced = True
                candidate.chmod(0o700)
                os.fchmod(payload_fd, 0o755)
                (candidate / "payload").rename(tmp_path / "opened-payload")
                (candidate / "payload").symlink_to(external, target_is_directory=True)
                os.fchmod(payload_fd, 0o555)
                candidate.chmod(0o500)
            return result

        monkeypatch.setattr(module, "_scan_payload", replace_after_scan)

    with pytest.raises(module.AuthorityRuntimeInstallError, match="symlink|identity|changed"):
        _publish(module, tmp_path, candidate, key)

    assert replaced
    assert not (tmp_path / "generations" / RELEASE_SHA).exists()
    _make_removable(candidate)
    _make_removable(tmp_path / "opened-payload")


def test_installer_dry_run_is_nonprivileged_and_does_not_build_payload() -> None:
    release_sha = subprocess.run(
        ("git", "rev-parse", "HEAD"),
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    completed = subprocess.run(
        ("/bin/bash", str(INSTALLER), "--dry-run", "--release-sha", release_sha),
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    assert "nonprivileged payload build" in completed.stdout
    assert "descriptor-bound root publication" in completed.stdout
    assert "uv sync" not in completed.stdout


def test_installer_delegates_payload_build_and_never_resolves_links_as_root() -> None:
    script = INSTALLER.read_text(encoding="utf-8")
    root_publication = script.split("build_runtime_release() {", 1)[1].split(
        "select_runtime_release() {", 1
    )[0]

    assert "--prepare-payload" in script
    assert "runuser --user" in script
    assert "publish-authority-runtime.py" in script
    assert "resolve(" not in root_publication
    assert "shutil.copy" not in root_publication
    assert "chown -R" not in root_publication


def test_installer_requires_preinstalled_pinned_bootstrap_and_never_executes_worktree() -> None:
    script = INSTALLER.read_text(encoding="utf-8")
    expected = hashlib.sha256(PUBLISHER.read_bytes()).hexdigest()
    pinned = re.search(r'^BOOTSTRAP_EXPECTED_SHA256="([0-9a-f]{64})"$', script, re.MULTILINE)
    assert pinned is not None
    assert pinned.group(1) == expected
    assert 'BOOTSTRAP_PUBLISHER="/usr/libexec/rquant-authority-runtime-publisher"' in script
    root_publication = script.split("build_runtime_release() {", 1)[1].split(
        "select_runtime_release() {", 1
    )[0]
    assert '"$BOOTSTRAP_PUBLISHER"' in root_publication
    assert "$PROJECT_ROOT/scripts/publish-authority-runtime.py" not in root_publication
    assert "/usr/bin/install" not in root_publication
    assert "BOOTSTRAP_INSTALL_COMMAND=" in script
    assert '"$BOOTSTRAP_PUBLISHER" "$BOOTSTRAP_EXPECTED_SHA256" 0 0 /' in script


def _verify_bootstrap(
    path: Path,
    *,
    digest: str,
    trusted_root: Path,
    expected_uid: int,
    expected_gid: int,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        (
            "/bin/bash",
            str(INSTALLER),
            "--verify-bootstrap-only",
            str(path),
            "--expected-bootstrap-sha256",
            digest,
            "--expected-bootstrap-uid",
            str(expected_uid),
            "--expected-bootstrap-gid",
            str(expected_gid),
            "--bootstrap-trusted-root",
            str(trusted_root),
        ),
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )


def test_bootstrap_verifier_accepts_closed_mock_and_rejects_mutation(tmp_path: Path) -> None:
    trusted = tmp_path / "trusted"
    trusted.mkdir(mode=0o755)
    bootstrap = trusted / "publisher"
    bootstrap.write_bytes(PUBLISHER.read_bytes())
    bootstrap.chmod(0o555)
    digest = hashlib.sha256(bootstrap.read_bytes()).hexdigest()

    accepted = _verify_bootstrap(
        bootstrap,
        digest=digest,
        trusted_root=trusted,
        expected_uid=os.geteuid(),
        expected_gid=os.getegid(),
    )
    assert accepted.returncode == 0, accepted.stderr

    bootstrap.chmod(0o755)
    bootstrap.write_bytes(bootstrap.read_bytes() + b"\n# altered\n")
    bootstrap.chmod(0o555)
    rejected = _verify_bootstrap(
        bootstrap,
        digest=digest,
        trusted_root=trusted,
        expected_uid=os.geteuid(),
        expected_gid=os.getegid(),
    )
    assert rejected.returncode != 0
    assert "hash" in rejected.stderr.lower()


def test_bootstrap_verifier_rejects_worktree_publisher() -> None:
    rejected = _verify_bootstrap(
        PUBLISHER,
        digest=hashlib.sha256(PUBLISHER.read_bytes()).hexdigest(),
        trusted_root=Path("/"),
        expected_uid=0,
        expected_gid=0,
    )
    assert rejected.returncode != 0
    assert "owner" in rejected.stderr.lower() or "ancestor" in rejected.stderr.lower()


@pytest.mark.parametrize("target_kind", ("external", "private", "etc-hosts"))
def test_prepare_rejects_external_symlink_without_opening_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    target_kind: str,
) -> None:
    module = _load_publisher()
    candidate, _files = _write_unsealed_candidate(tmp_path)
    target = tmp_path / target_kind
    if target_kind == "etc-hosts":
        target = Path("/etc/hosts")
    else:
        target.write_text("DO NOT READ", encoding="ascii")
        if target_kind == "private":
            target.chmod(0o000)
    link = candidate / "payload" / "unsafe-link"
    link.symlink_to(target)
    original_open = module.os.open

    def guarded_open(path: object, *args: object, **kwargs: object) -> int:
        if path == "unsafe-link":
            raise AssertionError("prepare attempted to open the symlink target")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(module.os, "open", guarded_open)
    with pytest.raises(module.AuthorityRuntimeInstallError, match="symlink"):
        _seal(module, candidate)
    assert not (candidate / "manifest.json").exists()
    if target_kind == "private":
        target.chmod(0o600)


def test_prepare_rejects_case_conflicting_inventory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_publisher()
    candidate, _files = _write_unsealed_candidate(
        tmp_path,
        files={
            "pkg/A.py": (b"A\n", 0o644),
            "pkg/a.py": (b"a\n", 0o644),
            "venv/bin/rquant": (b"#!/bin/sh\n", 0o755),
        },
    )
    package = candidate / "payload/pkg"
    if len(tuple(package.iterdir())) < 2:
        original_listdir = module.os.listdir
        package_identity = (package.stat().st_dev, package.stat().st_ino)

        def case_conflicting_listdir(path: object) -> list[str]:
            names = list(original_listdir(path))
            if isinstance(path, int):
                metadata = os.fstat(path)
                if (metadata.st_dev, metadata.st_ino) == package_identity:
                    return ["A.py", "a.py"]
            return names

        monkeypatch.setattr(module.os, "listdir", case_conflicting_listdir)
    with pytest.raises(module.AuthorityRuntimeInstallError, match="case"):
        _seal(module, candidate)


def test_prepare_manifest_order_is_utf8_byte_stable_across_locale(tmp_path: Path) -> None:
    module = _load_publisher()
    files = {
        "pkg/A.py": (b"A\n", 0o644),
        "pkg/z.py": (b"z\n", 0o644),
        "pkg/ä.py": (b"utf8\n", 0o644),
        "venv/bin/rquant": (b"#!/bin/sh\n", 0o755),
    }
    observed: list[bytes] = []
    previous = os.environ.get("LC_ALL")
    try:
        for index, locale_name in enumerate(("C", "tr_TR.UTF-8")):
            os.environ["LC_ALL"] = locale_name
            root = tmp_path / str(index)
            root.mkdir()
            candidate, _contents = _write_unsealed_candidate(root, files=files)
            observed.append(_seal(module, candidate).read_bytes())
    finally:
        if previous is None:
            os.environ.pop("LC_ALL", None)
        else:
            os.environ["LC_ALL"] = previous
    assert observed[0] == observed[1]
    paths = [entry["path"] for entry in json.loads(observed[0])["files"]]
    assert paths == sorted(paths, key=lambda value: value.encode("utf-8"))


def test_real_prepare_publish_verify_roundtrip(tmp_path: Path) -> None:
    from rquant.authority_runtime_release import (
        AuthorityRuntimeReleaseError,
        verify_authority_runtime_release,
    )

    module = _load_publisher()
    candidate, _files = _write_unsealed_candidate(tmp_path)
    manifest = _seal(module, candidate)
    key = _signing_key(tmp_path)

    release = _publish(module, tmp_path, candidate, key)
    runtime = tmp_path / "runtime"
    runtime.mkdir(mode=0o755)
    (tmp_path / "generations").rename(runtime / "generations")
    (runtime / "current").symlink_to(f"generations/{RELEASE_SHA}")
    public = tmp_path / "runtime.public.pem"
    completed = subprocess.run(
        (
            "openssl",
            "pkey",
            "-in",
            str(key),
            "-pubout",
            "-out",
            str(public),
        ),
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
    public.chmod(0o444)

    verified = verify_authority_runtime_release(
        root=runtime,
        signing_public_key_path=public,
        trusted_root=tmp_path,
        expected_uid=os.geteuid(),
        expected_gid=os.getegid(),
        signing_key_uid=os.geteuid(),
        signing_key_gid=os.getegid(),
        expected_release_sha=RELEASE_SHA,
        expected_publisher_sha256=hashlib.sha256(PUBLISHER.read_bytes()).hexdigest(),
        expected_publisher_version=PUBLISHER_VERSION,
    )
    assert manifest == candidate / "manifest.json"
    assert release.name == RELEASE_SHA
    assert verified.publisher_sha256 == hashlib.sha256(PUBLISHER.read_bytes()).hexdigest()
    with pytest.raises(AuthorityRuntimeReleaseError, match="publisher"):
        verify_authority_runtime_release(
            root=runtime,
            signing_public_key_path=public,
            trusted_root=tmp_path,
            expected_uid=os.geteuid(),
            expected_gid=os.getegid(),
            signing_key_uid=os.geteuid(),
            signing_key_gid=os.getegid(),
            expected_release_sha=RELEASE_SHA,
            expected_publisher_sha256="0" * 64,
            expected_publisher_version=PUBLISHER_VERSION,
        )
