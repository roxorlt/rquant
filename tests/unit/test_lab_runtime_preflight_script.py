from __future__ import annotations

import fcntl
import os
import runpy
import shutil
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
from pydantic import ValidationError

from rquant.config import Settings
from rquant.release_generation import ReleaseGenerationAuthority

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "preflight-lab-runtime.py"
TRUSTED_GIT = Path("/usr/bin/git")
RELEASE_AUTHORITY = ROOT / "src" / "rquant" / "release_generation.py"
_ORIGINAL_OS_WALK = os.walk
_SETTINGS_ENVIRONMENT_KEYS = frozenset(
    {"TUSHARE_TOKEN_MAIN", "DATA_DIR", "DUCKDB_PATH", "PARQUET_DIR", "LOG_DIR"}
)


def _clear_settings_environment(monkeypatch: pytest.MonkeyPatch, *keys: str) -> None:
    selected = _SETTINGS_ENVIRONMENT_KEYS if not keys else frozenset(keys)
    casefolded = {key.casefold() for key in selected}
    for key in tuple(os.environ):
        if key.casefold() in casefolded:
            monkeypatch.delenv(key, raising=False)


@pytest.fixture(autouse=True)
def _remove_immutable_test_generations(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[None]:
    monkeypatch.setenv("RQUANT_RELEASE_GENERATION_MIN_FREE_BYTES", "0")
    try:
        yield
    finally:
        generation_roots = [
            Path(current_root) / name
            for current_root, directory_names, _file_names in _ORIGINAL_OS_WALK(tmp_path)
            for name in directory_names
            if name.endswith(".venvs")
        ]
        for root in generation_roots:
            if root.is_symlink() or not root.is_dir():
                continue
            for current_root, _directory_names, file_names in _ORIGINAL_OS_WALK(root):
                current = Path(current_root)
                if hasattr(os, "chflags"):
                    os.chflags(current, 0)
                current.chmod(0o700)
                for name in file_names:
                    path = current / name
                    if not path.is_symlink():
                        if hasattr(os, "chflags"):
                            os.chflags(path, 0)
                        path.chmod(0o600)
            shutil.rmtree(root)


def _tiny_test_venv(checkout: Path) -> None:
    venv_root = checkout / ".venv"
    python = venv_root / "bin" / "python"
    python.parent.mkdir(parents=True)
    shutil.copy2(sys.executable, python)
    python.chmod(0o700)
    version = f"{sys.version_info.major}.{sys.version_info.minor}"
    (venv_root / "pyvenv.cfg").write_text(
        f"home = {Path(sys.base_prefix) / 'bin'}\nversion = {version}\n",
        encoding="utf-8",
    )
    (venv_root / "lib" / f"python{version}" / "site-packages").mkdir(parents=True)
    python_library = Path(sys.base_prefix) / "lib" / f"libpython{version}.dylib"
    if python_library.exists():
        shutil.copy2(python_library, venv_root / "lib" / python_library.name)


def _checkout(tmp_path: Path) -> tuple[Path, Path]:
    checkout = tmp_path / "checkout"
    package = checkout / "src" / "rquant"
    package.mkdir(parents=True)
    subprocess.run(["git", "init", "-q"], cwd=checkout, check=True)
    (checkout / ".gitignore").write_text(
        "__pycache__/\n*.pyc\n*.pyo\n*.so\n*.dylib\n*.pyd\n",
        encoding="utf-8",
    )
    (package / "__init__.py").write_text("", encoding="utf-8")
    shutil.copy2(RELEASE_AUTHORITY, package / RELEASE_AUTHORITY.name)
    (checkout / "pyproject.toml").write_text(
        '[project]\nname = "rquant"\nversion = "0.99.0"\n',
        encoding="utf-8",
    )
    (checkout / "uv.lock").write_text("version = 1\n", encoding="utf-8")
    _tiny_test_venv(checkout)
    subprocess.run(["git", "add", "."], cwd=checkout, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=rQuant Tests",
            "-c",
            "user.email=tests@rquant.invalid",
            "commit",
            "-qm",
            "test fixture",
        ],
        cwd=checkout,
        check=True,
    )
    commit = subprocess.run(
        [str(TRUSTED_GIT), "rev-parse", "HEAD"],
        cwd=checkout,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    lock_path = _lock_path(checkout)
    lock_path.parent.mkdir(mode=0o700)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        authority = ReleaseGenerationAuthority(
            repo=checkout,
            lock_path=lock_path,
            lock_fd=lock_fd,
            python_path=checkout / ".venv" / "bin" / "python",
            git_path=TRUSTED_GIT,
            writable=True,
            environment_builder=lambda destination: shutil.copytree(
                checkout / ".venv",
                destination,
                dirs_exist_ok=True,
                symlinks=True,
            ),
        )
        initialization = authority.begin_initialization(target_sha=commit)
        authority.publish(
            expected_commit=commit,
            operation_id=initialization.operation_id,
            transaction_kind="initialization",
        )
        authority.complete_initialization(operation_id=initialization.operation_id)
        authority.commit_generation(
            operation_id=initialization.operation_id,
            transaction_kind="initialization",
        )
    finally:
        os.close(lock_fd)
    return checkout, package


def _lock_path(checkout: Path) -> Path:
    return checkout.parent / ".rquant-deploy" / f"{checkout.name}.lock"


def _prepare_lab_runtime(checkout: Path) -> Path:
    from rquant.lab_daemon import (
        prepare_lab_runtime_layout,
        prepare_private_sqlite_path,
        register_lab_runtime_managed_file,
    )

    runtime_root = checkout / "data" / "lab-runtime"
    directories = {
        label: runtime_root / default_name
        for label, (_key, default_name) in {
            "lab command spool": ("LAB_JOB_COMMAND_DIR", "commands"),
            "lab claim spool": ("LAB_JOB_CLAIM_DIR", "claims"),
            "lab report spool": ("LAB_JOB_REPORT_DIR", "reports"),
            "lab worker artifact root": ("LAB_WORKER_ARTIFACT_DIR", "worker-artifacts"),
            "lab final artifact root": ("LAB_FINAL_ARTIFACT_DIR", "final-artifacts"),
            "lab artifact commit spool": ("LAB_ARTIFACT_COMMIT_DIR", "artifact-commits"),
            "lab daemon lock root": ("LAB_DAEMON_LOCK_DIR", "locks"),
            "lab finalizer state root": ("LAB_FINALIZER_STATE_DIR", "finalizer-state"),
            "lab readiness root": ("LAB_READINESS_DIR", "readiness"),
        }.items()
    }
    database = runtime_root / "lab_jobs.sqlite3"
    prepare_lab_runtime_layout(
        runtime_root,
        checkout_root=checkout,
        managed_directories=directories,
        managed_files={"lab jobs SQLite": database},
        legacy_paths={},
        mutation_guard=lambda: "a" * 40,
    )
    authority = prepare_private_sqlite_path(
        database,
        label="lab jobs SQLite",
        create=True,
        mutation_guard=lambda: "a" * 40,
    )
    try:
        register_lab_runtime_managed_file(
            runtime_root,
            label="lab jobs SQLite",
            path=database,
            mutation_guard=lambda: "a" * 40,
        )
    finally:
        authority.close()
    return runtime_root


def _replace_path_during_read(
    monkeypatch: pytest.MonkeyPatch,
    *,
    target: Path,
    replacement: Path,
) -> None:
    displaced = target.with_name(f"{target.name}.displaced")
    original_read_text = Path.read_text
    original_os_read = os.read
    swapped = False

    def swap() -> None:
        nonlocal swapped
        if swapped:
            return
        swapped = True
        target.rename(displaced)
        target.symlink_to(replacement)

    def guarded_read_text(path: Path, *args: object, **kwargs: object) -> str:
        if path == target:
            swap()
        return original_read_text(path, *args, **kwargs)

    def guarded_os_read(descriptor: int, size: int) -> bytes:
        swap()
        return original_os_read(descriptor, size)

    monkeypatch.setattr(Path, "read_text", guarded_read_text)
    monkeypatch.setattr(os, "read", guarded_os_read)


def _invoke(
    checkout: Path,
    *,
    expected_commit: str,
    git_path: Path,
    environment: dict[str, str] | None = None,
    arguments: tuple[str, ...] = (),
) -> subprocess.CompletedProcess[str]:
    lock_path = _lock_path(checkout)
    lock_fd = os.open(lock_path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
        return subprocess.run(
            [
                sys.executable,
                "-I",
                "-S",
                str(SCRIPT),
                "--checkout-root",
                str(checkout),
                "--expected-commit",
                expected_commit,
                "--trusted-git-path",
                str(git_path),
                "--deployment-lock-path",
                str(lock_path),
                "--deployment-lock-fd",
                str(lock_fd),
                "--python-path",
                str(checkout / ".venv" / "bin" / "python"),
                "--lab-daemon-command",
                "lab-runtime-prepare",
                *arguments,
            ],
            cwd=checkout,
            env=environment,
            capture_output=True,
            text=True,
            check=False,
            pass_fds=(lock_fd,),
        )
    finally:
        os.close(lock_fd)


def _run(checkout: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    expected_commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=checkout,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    return _invoke(
        checkout,
        expected_commit=expected_commit,
        git_path=TRUSTED_GIT,
        arguments=arguments,
    )


def test_lab_runtime_preflight_rejects_dirty_tracked_package_source(
    tmp_path: Path,
) -> None:
    checkout, package = _checkout(tmp_path)
    tracked = package / "__init__.py"
    tracked.write_text("UNTRUSTED = True\n", encoding="utf-8")

    result = _run(checkout)

    assert result.returncode == 1
    assert "tracked" in result.stderr.lower()
    assert tracked.read_text(encoding="utf-8") == "UNTRUSTED = True\n"


def test_lab_runtime_preflight_uses_explicit_trusted_git_not_path(
    tmp_path: Path,
) -> None:
    checkout, package = _checkout(tmp_path)
    fake_bin = checkout / ".venv" / "bin"
    fake_bin.mkdir(parents=True, exist_ok=True)
    marker = tmp_path / "fake-git-ran"
    fake_git = fake_bin / "git"
    fake_git.write_text(
        f"#!/bin/sh\ntouch {marker!s}\nexit 0\n",
        encoding="utf-8",
    )
    fake_git.chmod(0o700)
    (package / "__init__.py").write_text("UNTRUSTED = True\n", encoding="utf-8")
    expected_commit = subprocess.run(
        [str(TRUSTED_GIT), "rev-parse", "HEAD"],
        cwd=checkout,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    environment = os.environ.copy()
    environment["PATH"] = f"{fake_bin}:{environment.get('PATH', '')}"

    result = _invoke(
        checkout,
        expected_commit=expected_commit,
        git_path=TRUSTED_GIT,
        environment=environment,
    )

    assert result.returncode == 1
    assert "tracked" in result.stderr.lower()
    assert not marker.exists()


def test_lab_runtime_preflight_rejects_symlinked_trusted_git(tmp_path: Path) -> None:
    checkout, _package = _checkout(tmp_path)
    linked_git = tmp_path / "linked-git"
    linked_git.symlink_to(TRUSTED_GIT)
    expected_commit = subprocess.run(
        [str(TRUSTED_GIT), "rev-parse", "HEAD"],
        cwd=checkout,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    result = _invoke(
        checkout,
        expected_commit=expected_commit,
        git_path=linked_git,
    )

    assert result.returncode == 1
    assert "physical" in result.stderr


def test_lab_runtime_preflight_rejects_expected_commit_mismatch(tmp_path: Path) -> None:
    checkout, _package = _checkout(tmp_path)

    result = _invoke(
        checkout,
        expected_commit="0" * 40,
        git_path=TRUSTED_GIT,
    )

    assert result.returncode == 1
    assert "commit" in result.stderr.lower()


@pytest.mark.parametrize("suffix", [".pyc", ".pyo", ".so", ".dylib", ".pyd"])
def test_lab_runtime_preflight_rejects_executable_artifacts_without_mutation(
    tmp_path: Path,
    suffix: str,
) -> None:
    checkout, package = _checkout(tmp_path)
    artifact = package / f"payload{suffix}"
    artifact.write_bytes(b"untrusted executable artifact")

    result = _run(checkout)

    assert result.returncode == 1
    assert "executable artifact" in result.stderr
    assert artifact.read_bytes() == b"untrusted executable artifact"


def test_lab_runtime_preflight_rejects_package_symlink_without_mutation(
    tmp_path: Path,
) -> None:
    checkout, package = _checkout(tmp_path)
    external = tmp_path / "external-package"
    external.mkdir()
    external_init = external / "__init__.py"
    external_init.write_text("VALUE = 'external'\n", encoding="utf-8")
    package_link = package / "external_package"
    package_link.symlink_to(external)

    result = _run(checkout)

    assert result.returncode == 1
    assert "package symlink" in result.stderr
    assert package_link.is_symlink()
    assert external_init.read_text(encoding="utf-8") == "VALUE = 'external'\n"


def test_lab_runtime_preflight_has_no_automatic_cleanup_mode(tmp_path: Path) -> None:
    checkout, package = _checkout(tmp_path)
    cache = package / "__pycache__" / "payload.cpython-312.pyc"
    cache.parent.mkdir()
    cache.write_bytes(b"stale bytecode")

    result = _run(checkout, "--clean-bytecode")

    assert result.returncode != 0
    assert cache.read_bytes() == b"stale bytecode"


def test_lab_runtime_preflight_symlink_swap_never_deletes_external_bytecode(
    tmp_path: Path,
) -> None:
    checkout, package = _checkout(tmp_path)
    external = tmp_path / "external-cache"
    external.mkdir()
    victim = external / "payload.cpython-312.pyc"
    victim.write_bytes(b"external bytecode must survive")
    os.symlink(external, package / "__pycache__")

    result = _run(checkout)

    assert result.returncode == 1
    assert "manual" in result.stderr.lower()
    assert (package / "__pycache__").is_symlink()
    assert victim.read_bytes() == b"external bytecode must survive"


def test_lab_runtime_preflight_readonly_git_disables_optional_locks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checkout, _package = _checkout(tmp_path)
    namespace = runpy.run_path(str(SCRIPT))
    git_path, git_identity = namespace["_trusted_git"](str(TRUSTED_GIT))
    observed_environments: list[dict[str, str]] = []
    original_run = namespace["_git_command"].__globals__["run_contained"]

    def capture_git_environment(
        command: list[str], **kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        environment = kwargs.get("env")
        assert isinstance(environment, dict)
        observed_environments.append(environment)
        return original_run(command, **kwargs)

    monkeypatch.setitem(
        namespace["_git_command"].__globals__,
        "run_contained",
        capture_git_environment,
    )

    namespace["_git_command"](
        checkout,
        ["rev-parse", "--verify", "HEAD^{commit}"],
        git_path=git_path,
        git_identity=git_identity,
    )

    assert observed_environments
    assert all(values["GIT_OPTIONAL_LOCKS"] == "0" for values in observed_environments)
    assert all(values["GIT_TERMINAL_PROMPT"] == "0" for values in observed_environments)


@pytest.mark.parametrize(
    ("data_line", "duplicate_lines"),
    (
        ("export DATA_DIR = '{data}' # exported upper-case", ()),
        ('data_dir = "{data}" # lower-case Settings key', ()),
        (
            'data_dir = "{data}" # lower-case Settings key',
            ("DATA_DIR = '{superseded}'", 'data_dir = "{data}"'),
        ),
    ),
)
def test_stdlib_dotenv_lab_paths_match_settings_supported_subset(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    data_line: str,
    duplicate_lines: tuple[str, ...],
) -> None:
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    data = tmp_path / "configured-data"
    dotenv = checkout / ".env"
    dotenv.write_text(
        "\n".join(
            (
                "TUSHARE_TOKEN_MAIN=" + "x" * 32,
                data_line.format(data=data),
                f"duckdb_path = '{data / 'rquant.duckdb'}'",
                f"PARQUET_DIR = '{tmp_path / 'parquet'}'",
                f"LOG_DIR = '{tmp_path / 'logs'}'",
                *(
                    line.format(data=data, superseded=tmp_path / "superseded")
                    for line in duplicate_lines
                ),
            )
        )
        + "\n",
        encoding="utf-8",
    )
    dotenv.chmod(0o600)
    namespace = runpy.run_path(str(SCRIPT))
    _clear_settings_environment(monkeypatch, "DATA_DIR")

    values = namespace["_dotenv_values"](dotenv)
    preflight_path = namespace["_configured_path"](
        values,
        "DATA_DIR",
        checkout / "data",
        label="DATA_DIR",
    )
    configured = Settings(_env_file=dotenv)

    assert preflight_path == configured.data_dir
    if not duplicate_lines:
        assert configured.data_dir == data


@pytest.mark.parametrize("data_assignment", (None, "DATA_DIR="))
def test_stdlib_preflight_requires_nonempty_data_dir_like_settings(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    data_assignment: str | None,
) -> None:
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    _prepare_lab_runtime(checkout)
    dotenv = checkout / ".env"
    lines = [
        "TUSHARE_TOKEN_MAIN=" + "x" * 32,
        f"DUCKDB_PATH='{checkout / 'data' / 'rquant.duckdb'}'",
        f"PARQUET_DIR='{tmp_path / 'parquet'}'",
        f"LOG_DIR='{tmp_path / 'logs'}'",
    ]
    if data_assignment is not None:
        lines.append(data_assignment)
    dotenv.write_text("\n".join(lines) + "\n", encoding="utf-8")
    dotenv.chmod(0o600)
    _clear_settings_environment(monkeypatch, "DATA_DIR")
    namespace = runpy.run_path(str(SCRIPT))

    with pytest.raises(ValidationError):
        Settings(_env_file=dotenv)
    with pytest.raises(namespace["PreflightError"], match="DATA_DIR is required"):
        namespace["_verify_prepared_lab_runtime"](
            checkout,
            daemon_command="lab-scheduler",
        )


def test_stdlib_preflight_rejects_duplicate_prepared_sentinel_keys(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    runtime = _prepare_lab_runtime(checkout)
    dotenv = checkout / ".env"
    dotenv.write_text(
        "\n".join(
            (
                "TUSHARE_TOKEN_MAIN=" + "x" * 32,
                f"DATA_DIR='{checkout / 'data'}'",
                f"DUCKDB_PATH='{checkout / 'data' / 'rquant.duckdb'}'",
                f"PARQUET_DIR='{tmp_path / 'parquet'}'",
                f"LOG_DIR='{tmp_path / 'logs'}'",
            )
        )
        + "\n",
        encoding="utf-8",
    )
    dotenv.chmod(0o600)
    sentinel = runtime / ".prepared.json"
    original = sentinel.read_text(encoding="utf-8").lstrip()
    sentinel.write_text(
        '{"schema_version":2,' + original[1:],
        encoding="utf-8",
    )
    sentinel.chmod(0o600)
    namespace = runpy.run_path(str(SCRIPT))
    for key in namespace["LAB_RUNTIME_PATH_KEYS"]:
        for environment_key in tuple(os.environ):
            if environment_key.casefold() == key.casefold():
                monkeypatch.delenv(environment_key, raising=False)

    with pytest.raises(namespace["PreflightError"], match="duplicate|malformed"):
        namespace["_verify_prepared_lab_runtime"](
            checkout,
            daemon_command="lab-scheduler",
        )


@pytest.mark.parametrize(
    ("key", "default_name", "settings_property"),
    (
        ("LAB_RUNTIME_DIR", "lab-runtime", "lab_runtime_dir_resolved"),
        ("LAB_JOBS_PATH", "lab_jobs.sqlite3", "lab_jobs_path_resolved"),
        ("LAB_JOB_COMMAND_DIR", "commands", "lab_job_command_dir_resolved"),
        ("LAB_JOB_CLAIM_DIR", "claims", "lab_job_claim_dir_resolved"),
        ("LAB_JOB_REPORT_DIR", "reports", "lab_job_report_dir_resolved"),
        ("LAB_WORKER_ARTIFACT_DIR", "worker-artifacts", "lab_worker_artifact_dir_resolved"),
        ("LAB_FINAL_ARTIFACT_DIR", "final-artifacts", "lab_final_artifact_dir_resolved"),
        ("LAB_ARTIFACT_COMMIT_DIR", "artifact-commits", "lab_artifact_commit_dir_resolved"),
        ("LAB_DAEMON_LOCK_DIR", "locks", "lab_daemon_lock_dir_resolved"),
        ("LAB_FINALIZER_STATE_DIR", "finalizer-state", "lab_finalizer_state_dir_resolved"),
        ("LAB_READINESS_DIR", "readiness", "lab_readiness_dir_resolved"),
    ),
)
def test_stdlib_preflight_optional_lab_path_defaults_match_settings(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    key: str,
    default_name: str,
    settings_property: str,
) -> None:
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    data = tmp_path / "data"
    dotenv = checkout / ".env"
    dotenv.write_text(
        "\n".join(
            (
                "TUSHARE_TOKEN_MAIN=" + "x" * 32,
                f"DATA_DIR='{data}'",
                f"DUCKDB_PATH='{data / 'rquant.duckdb'}'",
                f"PARQUET_DIR='{tmp_path / 'parquet'}'",
                f"LOG_DIR='{tmp_path / 'logs'}'",
                f"{key}=",
            )
        )
        + "\n",
        encoding="utf-8",
    )
    dotenv.chmod(0o600)
    _clear_settings_environment(monkeypatch, "DATA_DIR", key)
    namespace = runpy.run_path(str(SCRIPT))
    values = namespace["_dotenv_values"](dotenv)
    configured = Settings(_env_file=dotenv)
    runtime_root = configured.lab_runtime_dir_resolved
    default = data / default_name if key == "LAB_RUNTIME_DIR" else runtime_root / default_name

    observed = namespace["_configured_path"](
        values,
        key,
        default,
        label=key,
    )

    assert observed == getattr(configured, settings_property)


@pytest.mark.parametrize(
    "line",
    (
        "DATA_DIR=${HOME}/rquant-data",
        r"LAB_RUNTIME_DIR='C:\\unsafe-escape'",
        "export DATA_DIR /tmp/rquant-data",
    ),
)
def test_stdlib_dotenv_rejects_unsupported_lab_path_syntax(
    tmp_path: Path,
    line: str,
) -> None:
    dotenv = tmp_path / ".env"
    dotenv.write_text(f"{line}\n", encoding="utf-8")
    dotenv.chmod(0o600)
    namespace = runpy.run_path(str(SCRIPT))

    with pytest.raises(namespace["PreflightError"], match="unsupported"):
        namespace["_dotenv_values"](dotenv)


@pytest.mark.parametrize("keyword", ("EXPORT", "Export", "eXport"))
def test_stdlib_dotenv_rejects_ambiguous_nonlowercase_export_like_python_dotenv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    keyword: str,
) -> None:
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    ambiguous = tmp_path / "ambiguous-data"
    configured_data = tmp_path / "configured-data"
    dotenv = checkout / ".env"
    dotenv.write_text(
        "\n".join(
            (
                "TUSHARE_TOKEN_MAIN=" + "x" * 32,
                f"{keyword} DATA_DIR='{ambiguous}'",
                f"data_dir='{configured_data}'",
                f"DUCKDB_PATH='{configured_data / 'rquant.duckdb'}'",
                f"PARQUET_DIR='{tmp_path / 'parquet'}'",
                f"LOG_DIR='{tmp_path / 'logs'}'",
            )
        )
        + "\n",
        encoding="utf-8",
    )
    dotenv.chmod(0o600)
    _clear_settings_environment(monkeypatch)
    configured = Settings(_env_file=dotenv)
    namespace = runpy.run_path(str(SCRIPT))

    assert configured.data_dir == configured_data
    with pytest.raises(namespace["PreflightError"], match="unsupported DATA_DIR"):
        namespace["_dotenv_values"](dotenv)


def test_stdlib_dotenv_read_is_descriptor_bound_across_symlink_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    dotenv = checkout / ".env"
    dotenv.write_text(f"DATA_DIR='{tmp_path / 'data'}'\n", encoding="utf-8")
    dotenv.chmod(0o600)
    replacement = tmp_path / "external.env"
    replacement.write_bytes(dotenv.read_bytes())
    replacement.chmod(0o600)
    namespace = runpy.run_path(str(SCRIPT))
    _replace_path_during_read(
        monkeypatch,
        target=dotenv,
        replacement=replacement,
    )

    with pytest.raises(namespace["PreflightError"], match="identity changed"):
        namespace["_dotenv_values"](dotenv)

    assert replacement.read_text(encoding="utf-8") == f"DATA_DIR='{tmp_path / 'data'}'\n"


def test_prepared_sentinel_read_is_descriptor_bound_across_symlink_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checkout, _package = _checkout(tmp_path)
    runtime_root = _prepare_lab_runtime(checkout)
    sentinel = runtime_root / ".prepared.json"
    replacement = tmp_path / "external-prepared.json"
    replacement.write_bytes(sentinel.read_bytes())
    replacement.chmod(0o600)
    namespace = runpy.run_path(str(SCRIPT))
    monkeypatch.setenv("DATA_DIR", str(checkout / "data"))
    _replace_path_during_read(
        monkeypatch,
        target=sentinel,
        replacement=replacement,
    )

    with pytest.raises(namespace["PreflightError"], match="identity changed"):
        namespace["_verify_prepared_lab_runtime"](
            checkout,
            daemon_command="lab-scheduler",
        )

    assert replacement.read_bytes()


def test_lab_runtime_preflight_detect_only_scan_survives_mid_walk_symlink_swap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checkout, package = _checkout(tmp_path)
    cache = package / "__pycache__"
    cache.mkdir()
    repository_bytecode = cache / "payload.cpython-312.pyc"
    repository_bytecode.write_bytes(b"repository bytecode")
    external = tmp_path / "external-cache"
    external.mkdir()
    victim = external / repository_bytecode.name
    victim.write_bytes(b"external bytecode must survive")
    displaced = tmp_path / "displaced-cache"
    namespace = runpy.run_path(str(SCRIPT))

    def swapping_walk(
        *_args: object,
        **_kwargs: object,
    ) -> Iterator[tuple[str, list[str], list[str]]]:
        yield str(package), [cache.name], []
        cache.rename(displaced)
        cache.symlink_to(external, target_is_directory=True)
        yield str(cache), [], [victim.name]

    monkeypatch.setattr(os, "walk", swapping_walk)

    artifacts = namespace["_runtime_artifacts"](checkout)

    assert cache / victim.name in artifacts
    assert cache.is_symlink()
    assert victim.read_bytes() == b"external bytecode must survive"
    assert (displaced / repository_bytecode.name).read_bytes() == b"repository bytecode"
