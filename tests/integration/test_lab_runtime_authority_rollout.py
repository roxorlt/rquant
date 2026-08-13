from __future__ import annotations

import fcntl
import hashlib
import json
import os
import shutil
import signal
import site
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from rquant.artifact_retention_catalog_authority import (
    load_retention_catalog_authority,
)
from rquant.experiment_registry import ExperimentRegistry
from rquant.job_center_authority import load_job_center_authority
from rquant.release_generation import ReleaseGenerationAuthority
from rquant.runtime_definition_bootstrap import (
    bootstrap_builtin_definitions,
    plan_builtin_definitions,
)
from rquant.runtime_deployment_profile import (
    RuntimeDeploymentProfile,
    install_runtime_deployment_profile,
)
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from tests.formal_smoke_real_generation_support import real_promotion_authority
from tests.runtime_code_e2e_support import build_test_package, install_test_package

ROOT = Path(__file__).resolve().parents[2]
TRUSTED_GIT = Path("/usr/bin/git")


def _run(command: list[str], *, cwd: Path, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    process = subprocess.Popen(
        command,
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        stdout, stderr = process.communicate(timeout=60)
    except subprocess.TimeoutExpired as exc:
        os.killpg(process.pid, signal.SIGKILL)
        stdout, stderr = process.communicate()
        raise AssertionError(
            f"subprocess exceeded 60s: {command!r}\nstdout={stdout}\nstderr={stderr}"
        ) from exc
    return subprocess.CompletedProcess(command, process.returncode, stdout, stderr)


def _git(checkout: Path, *args: str) -> str:
    return subprocess.run(
        [str(TRUSTED_GIT), *args],
        cwd=checkout,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _physical_test_venv(checkout: Path) -> Path:
    root = checkout / ".venv"
    python = root / "bin" / "python"
    python.parent.mkdir(parents=True)
    shutil.copy2(Path(sys.executable).resolve(strict=True), python)
    python.chmod(0o700)
    version = f"{sys.version_info.major}.{sys.version_info.minor}"
    (root / "pyvenv.cfg").write_text(
        f"home = {Path(sys.base_prefix) / 'bin'}\nversion = {version}\n",
        encoding="utf-8",
    )
    target_site = root / "lib" / f"python{version}" / "site-packages"
    target_site.mkdir(parents=True)
    source_site = next(
        Path(candidate)
        for candidate in site.getsitepackages()
        if Path(candidate).is_dir() and "site-packages" in candidate
    )
    dependency_names = {
        "annotated_types",
        "dateutil",
        "dotenv",
        "duckdb",
        "loguru",
        "numpy",
        "pandas",
        "pyarrow",
        "pydantic",
        "pydantic_core",
        "pydantic_settings",
        "pytz",
        "typing_extensions.py",
        "typing_inspection",
        "six.py",
    }
    dependency_names.update(
        entry.name for entry in source_site.iterdir() if entry.name.startswith("_duckdb.")
    )
    dependency_names.update(
        entry.name for entry in source_site.iterdir() if entry.name.endswith(".dist-info")
    )
    for name in dependency_names:
        entry = source_site / name
        target = target_site / name
        if entry.is_dir():
            shutil.copytree(entry, target, symlinks=False)
        elif entry.is_file():
            shutil.copy2(entry, target)
    python_library = Path(sys.base_prefix) / "lib" / f"libpython{version}.dylib"
    if python_library.exists():
        shutil.copy2(python_library, root / "lib" / python_library.name)
    launcher = root / "bin" / "rquant"
    launcher.write_text(
        f"#!{python}\n"
        "import sys\n"
        f"sys.path.insert(0, {str(checkout / 'src')!r})\n"
        "from rquant.cli import main\n"
        "raise SystemExit(main())\n",
        encoding="utf-8",
    )
    launcher.chmod(0o700)
    return python


def _publish_release_generation(
    checkout: Path,
    *,
    previous_sha: str | None,
    target_sha: str,
    previous_marker_generation: str = "",
    previous_environment_generation: str = "",
) -> tuple[Path, str, str]:
    lock_path = checkout.parent / ".rquant-deploy" / f"{checkout.name}.lock"
    lock_path.parent.mkdir(mode=0o700, exist_ok=True)
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
            minimum_free_bytes=0,
            environment_builder=lambda destination: shutil.copytree(
                checkout / ".venv",
                destination,
                dirs_exist_ok=True,
                symlinks=False,
            ),
        )
        if previous_sha is None:
            initialization = authority.begin_initialization(target_sha=target_sha)
            marker = authority.publish(
                expected_commit=target_sha,
                operation_id=initialization.operation_id,
                transaction_kind="initialization",
            )
            authority.complete_initialization(operation_id=initialization.operation_id)
            authority.commit_generation(
                operation_id=initialization.operation_id,
                transaction_kind="initialization",
            )
        else:
            intent = authority.begin_deployment_intent(
                previous_sha=previous_sha,
                target_sha=target_sha,
                target_ref=target_sha,
                changed_files=("rollout-marker.txt",),
                restart_services=(),
                active_services=(),
                active_timers=(),
                marker_generation=previous_marker_generation,
                previous_generation_id=previous_environment_generation,
            )
            authority.invalidate()
            for stage in (
                "timers_stopped",
                "deploy_checkout_ready",
                "deploy_dependencies_ready",
                "deploy_preflight_ready",
                "services_transitioning",
                "services_ready",
                "post_restart_preflight_ready",
                "timers_restored",
            ):
                authority.update_deployment_intent(
                    operation_id=intent.operation_id,
                    stage=stage,
                )
            marker = authority.publish(
                expected_commit=target_sha,
                operation_id=intent.operation_id,
                transaction_kind="deployment",
            )
            for stage in ("marker_published", "awaiting_readiness", "completed"):
                authority.update_deployment_intent(
                    operation_id=intent.operation_id,
                    stage=stage,
                )
            authority.commit_generation(
                operation_id=intent.operation_id,
                transaction_kind="deployment",
            )
        return (
            Path(marker.venv_path),
            marker.content_hash(),
            marker.environment_generation_id,
        )
    finally:
        os.close(lock_fd)


def _prepare_scheduler_keys(research: Path, env: dict[str, str]) -> None:
    key_root = research.parent.parent / "authority-keys"
    key_root.mkdir(mode=0o700)
    key = key_root / "active.key"
    key.write_text("61" * 32 + "\n", encoding="ascii")
    key.chmod(0o600)
    keyring = key_root / "keyring.json"
    keyring.write_text(
        json.dumps(
            {"keys": {"active": "61" * 32}, "schema_version": 1},
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n",
        encoding="ascii",
    )
    keyring.chmod(0o600)
    env.update(
        {
            "LAB_FINALIZER_AUTHORITY_KEY_ID": "active",
            "LAB_FINALIZER_AUTHORITY_KEY_PATH": str(key),
            "LAB_FINALIZER_AUTHORITY_KEYRING_PATH": str(keyring),
        }
    )


def _checkout(tmp_path: Path) -> tuple[Path, Path, str]:
    checkout = tmp_path / "checkout"
    shutil.copytree(
        ROOT / "src",
        checkout / "src",
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "*.pyo", "*.so", "*.dylib"),
    )
    shutil.copytree(ROOT / "deploy" / "launchd", checkout / "deploy" / "launchd")
    (checkout / "scripts").mkdir()
    for name in (
        "bootstrap-lab-daemon.py",
        "preflight-lab-runtime.py",
        "run-lab-daemon.py",
        "strict_json.py",
    ):
        shutil.copy2(ROOT / "scripts" / name, checkout / "scripts" / name)
    shutil.copy2(ROOT / "pyproject.toml", checkout / "pyproject.toml")
    shutil.copy2(ROOT / "uv.lock", checkout / "uv.lock")
    (checkout / ".gitignore").write_text(
        "/.env\n/.venv\n__pycache__/\n*.pyc\n*.so\n*.dylib\n",
        encoding="utf-8",
    )
    python = _physical_test_venv(checkout)
    subprocess.run([str(TRUSTED_GIT), "init", "-q"], cwd=checkout, check=True)
    subprocess.run([str(TRUSTED_GIT), "add", "."], cwd=checkout, check=True)
    subprocess.run(
        [
            str(TRUSTED_GIT),
            "-c",
            "user.name=rQuant Tests",
            "-c",
            "user.email=tests@rquant.invalid",
            "commit",
            "-qm",
            "authority rollout fixture",
        ],
        cwd=checkout,
        check=True,
    )
    return checkout, python, _git(checkout, "rev-parse", "HEAD")


def _profile(runtime_root: Path, *, commit: str) -> RuntimeDeploymentProfile:
    research = runtime_root / "research"
    catalog_id = "artifact-catalog.primary.v1"
    catalog_instance = "svc-" + hashlib.sha256(catalog_id.encode()).hexdigest()
    retention_id = "artifact-retention.primary.v1"
    retention_instance = "svc-" + hashlib.sha256(retention_id.encode()).hexdigest()
    retention_state_root = research / "artifact-retention" / retention_instance
    manifests = (
        RuntimeServiceManifest(
            service_id="lab-jobs.serving.v1",
            service_kind=RuntimeServiceKind.LAB_JOBS_PUBLISHER,
            plane=RuntimeServicePlane.RESEARCH,
            interval_seconds=30,
            stale_after_seconds=120,
            producer_commit=commit,
            settings={
                "lab_jobs_path": str(research / "lab_jobs.sqlite3"),
                "authority_root": str(research / "serving-authorities" / "lab-jobs"),
            },
        ),
        RuntimeServiceManifest(
            service_id=catalog_id,
            service_kind=RuntimeServiceKind.LAB_ARTIFACT_CATALOG,
            plane=RuntimeServicePlane.RESEARCH,
            interval_seconds=30,
            stale_after_seconds=120,
            producer_commit=commit,
            settings={
                "artifact_root": str(research / "final-artifacts"),
                "state_root": str(research / "artifact-catalogs" / catalog_instance),
                "research_root": str(research),
                "lab_jobs_path": str(research / "lab_jobs.sqlite3"),
                "dataset_authority_path": str(research / "research_ro.duckdb"),
                "experiment_registry_path": str(research / "experiment_registry.sqlite3"),
                "definition_registry_root": str(research / "definitions"),
                "location_id": "test-primary",
                "failure_domain": "test-local",
            },
        ),
        RuntimeServiceManifest(
            service_id=retention_id,
            service_kind=RuntimeServiceKind.ARTIFACT_RETENTION,
            plane=RuntimeServicePlane.RESEARCH,
            interval_seconds=300,
            stale_after_seconds=900,
            producer_commit=commit,
            settings={
                "managed_root": str(research / "final-artifacts"),
                "state_root": str(retention_state_root),
                "reference_store_path": str(retention_state_root / "references.sqlite3"),
                "catalog_authority_root": str(retention_state_root / "catalog-authority"),
                "recovery_publication_root": str(runtime_root.parent / "recovery-publication"),
                "recovery_restore_root": str(runtime_root.parent / "recovery-restore"),
            },
        ),
    )
    return RuntimeDeploymentProfile(
        producer_commit=commit,
        production_runtime_root=str(runtime_root),
        manifests=manifests,
        capability_environment={
            manifest.service_id: (
                ("RQ_ARTIFACT_RETENTION_WRITER_CREDENTIAL",)
                if manifest.service_kind is RuntimeServiceKind.ARTIFACT_RETENTION
                else ()
            )
            for manifest in manifests
        },
    )


def _install_profile(runtime_root: Path, *, commit: str) -> tuple[str, str]:
    class _CredentialTransaction:
        def commit(self) -> None:
            return None

        def rollback(self) -> None:
            return None

    class _CredentialRecovery:
        outcome = "none"
        transaction_id = None

    profile = _profile(runtime_root, commit=commit)
    current_generation = runtime_root / "current"
    with (
        patch(
            "rquant.runtime_deployment_bundle._seal_runtime_credentials",
            lambda _credentials: _CredentialTransaction(),
        ),
        patch(
            "rquant.runtime_deployment_bundle._recover_runtime_credentials",
            lambda **_kwargs: _CredentialRecovery(),
        ),
    ):
        receipt = install_runtime_deployment_profile(
            profile,
            runtime_root=runtime_root,
            environ={"RQ_ARTIFACT_RETENTION_WRITER_CREDENTIAL": "test-retention-capability"},
            schema_bootstrap_reason=(
                None if current_generation.is_symlink() else "Job Center production authority E2E"
            ),
        )
    assert profile.profile_id is not None
    return profile.profile_id, receipt.generation_hash


def _prepare_authority_dependencies(runtime_root: Path, *, commit: str) -> None:
    research = runtime_root / "research"
    research.mkdir(parents=True, mode=0o700)
    research.chmod(0o700)
    definitions = research / "definitions"
    plan = plan_builtin_definitions(producer_commit=commit)
    bootstrap_builtin_definitions(
        definitions,
        producer_commit=commit,
        registered_at=datetime(2026, 8, 3, tzinfo=UTC),
        available_at=datetime(2026, 8, 3, tzinfo=UTC),
        expected_plan_id=plan.plan_id,
    )
    experiments = research / "experiment_registry.sqlite3"
    ExperimentRegistry(experiments, managed_trust_root=research)
    experiments.chmod(0o600)
    dataset = research / "research_ro.duckdb"
    dataset.touch(mode=0o600)


def _runtime_env(runtime_root: Path, research: Path, tmp_path: Path) -> dict[str, str]:
    data = tmp_path / "data"
    env = {
        **os.environ,
        "TUSHARE_TOKEN_MAIN": "x" * 32,
        "DATA_DIR": str(data),
        "DUCKDB_PATH": str(data / "main.duckdb"),
        "PARQUET_DIR": str(data / "parquet"),
        "LOG_DIR": str(data / "logs"),
        "LAB_RUNTIME_DIR": str(research),
        "LAB_JOBS_PATH": str(research / "lab_jobs.sqlite3"),
        "LAB_JOB_COMMAND_DIR": str(research / "commands"),
        "LAB_FINAL_ARTIFACT_DIR": str(research / "final-artifacts"),
        "LAB_TRUSTED_GIT_PATH": str(TRUSTED_GIT),
        "RQUANT_RUNTIME_ROOT": str(runtime_root),
        "RQUANT_RELEASE_GENERATION_MIN_FREE_BYTES": "0",
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    for name in ("PYTHONHOME", "PYTHONINSPECT", "PYTHONPATH", "PYTHONSTARTUP"):
        env.pop(name, None)
    return env


def _runtime_code_arguments(configuration_path: Path, trusted_base: Path) -> tuple[str, ...]:
    return (
        "--runtime-code-config",
        str(configuration_path),
        "--runtime-code-trusted-base",
        str(trusted_base),
        "--runtime-code-authority-uid",
        str(os.getuid()),
        "--runtime-code-authority-gid",
        str(os.getgid()),
    )


def _install_runtime_code_generation(
    root: Path,
    *,
    provenance_commit: str,
) -> tuple[object, Path, Path, object]:
    package = build_test_package(
        root / "package",
        provenance_commit=provenance_commit,
        python_abi=sys.implementation.cache_tag or "invalid",
        now=datetime.now(UTC),
    )
    trusted_base, runtime_root, installer = install_test_package(root, package)
    return package, trusted_base, runtime_root, installer


def _wrapper_command(checkout: Path, *daemon_args: str) -> list[str]:
    deployment_lock = checkout.parent / ".rquant-deploy" / f"{checkout.name}.lock"
    return [
        str(checkout / ".venv" / "bin" / "python"),
        "-I",
        "-S",
        str(checkout / "scripts" / "run-lab-daemon.py"),
        "--expected-checkout-root",
        str(checkout),
        "--trusted-git-path",
        str(TRUSTED_GIT),
        "--deployment-lock-path",
        str(deployment_lock),
        "--",
        str(checkout / ".venv" / "bin" / "rquant"),
        *daemon_args,
    ]


def _relax_release_tree(lock_path: Path) -> None:
    root = lock_path.with_name(f"{lock_path.stem}.venvs")
    if not root.exists():
        return
    for current_root, _directories, files in os.walk(root):
        current = Path(current_root)
        try:
            if hasattr(os, "chflags"):
                os.chflags(current, 0)
            current.chmod(0o700)
        except FileNotFoundError:
            continue
        for name in files:
            path = current / name
            if path.is_symlink():
                continue
            try:
                if hasattr(os, "chflags"):
                    os.chflags(path, 0)
                path.chmod(0o600)
            except FileNotFoundError:
                continue


def test_real_wrapper_cli_first_install_sha_roll_and_scheduler_auto_load(
    tmp_path: Path,
) -> None:
    checkout, _python, first_sha = _checkout(tmp_path)
    runtime_root = tmp_path / "production-runtime"
    research = runtime_root / "research"
    runtime_root.mkdir(mode=0o700)
    _prepare_authority_dependencies(runtime_root, commit=first_sha)
    first_profile_id, first_generation = _install_profile(runtime_root, commit=first_sha)
    env = _runtime_env(runtime_root, research, tmp_path)
    first_package, trusted_base, code_runtime_root, installer = _install_runtime_code_generation(
        tmp_path / "runtime-code-first",
        provenance_commit=first_sha,
    )
    (
        _first_release,
        first_marker_generation,
        first_environment_generation,
    ) = _publish_release_generation(
        checkout,
        previous_sha=None,
        target_sha=first_sha,
    )
    try:
        first_generation_context = SimpleNamespace(
            package=first_package,
            trusted_base=trusted_base,
            runtime_root=code_runtime_root,
        )
        with real_promotion_authority(
            tmp_path / "promotion-first",
            generation=first_generation_context,
        ) as first_configuration:
            first_arguments = _runtime_code_arguments(first_configuration, trusted_base)
            first = _run(
                _wrapper_command(checkout, "lab-runtime-prepare", *first_arguments),
                cwd=checkout,
                env=env,
            )
            assert first.returncode == 0, first.stderr
            retention = next(
                manifest.settings
                for manifest in _profile(runtime_root, commit=first_sha).manifests
                if manifest.service_kind is RuntimeServiceKind.ARTIFACT_RETENTION
            )
            retention_authority = load_retention_catalog_authority(
                Path(str(retention["catalog_authority_root"])),
                expected_producer_commit=first_sha,
                expected_reference_store_path=Path(str(retention["reference_store_path"])),
            )
            assert retention_authority.current_receipt_path.exists()
            current = research / "job-center-authority.json"
            first_manifest = load_job_center_authority(
                current,
                expected_code_sha=first_sha,
                runtime_root=research,
                runtime_deployment_root=runtime_root,
                deployment_profile_id=first_profile_id,
                deployment_generation_hash=first_generation,
            )
            first_payload = current.read_bytes()

            missing_env = {**env, "RQUANT_RUNTIME_ROOT": str(tmp_path / "missing-runtime")}
            missing = _run(
                _wrapper_command(checkout, "lab-runtime-prepare", *first_arguments),
                cwd=checkout,
                env=missing_env,
            )
            assert missing.returncode != 0
            assert current.read_bytes() == first_payload

            (checkout / "rollout-marker.txt").write_text("second\n", encoding="utf-8")
            subprocess.run(
                [str(TRUSTED_GIT), "add", "rollout-marker.txt"], cwd=checkout, check=True
            )
            subprocess.run(
                [
                    str(TRUSTED_GIT),
                    "-c",
                    "user.name=rQuant Tests",
                    "-c",
                    "user.email=tests@rquant.invalid",
                    "commit",
                    "-qm",
                    "roll authority SHA",
                ],
                cwd=checkout,
                check=True,
            )
            second_sha = _git(checkout, "rev-parse", "HEAD")
            second_profile_id, second_generation = _install_profile(
                runtime_root,
                commit=second_sha,
            )
            (research / ".job-center-authority.candidate.json").write_bytes(b'{"half":')
            (research / ".job-center-authority.candidate.json").chmod(0o600)

            scheduler_before_roll = _run(
                [
                    str(checkout / ".venv" / "bin" / "rquant"),
                    "lab-scheduler",
                    *first_arguments,
                    "--runtime-deployment-root",
                    str(runtime_root),
                    "--startup-deadline-monotonic",
                    str(time.monotonic() + 30),
                    "--once",
                ],
                cwd=checkout,
                env=env,
            )
            assert scheduler_before_roll.returncode != 0
            assert current.read_bytes() == first_payload

        _publish_release_generation(
            checkout,
            previous_sha=first_sha,
            target_sha=second_sha,
            previous_marker_generation=first_marker_generation,
            previous_environment_generation=first_environment_generation,
        )
        second_package = build_test_package(
            tmp_path / "runtime-code-second" / "package",
            sequence=2,
            previous_receipt_sha256=hashlib.sha256(first_package.receipt_bytes).hexdigest(),
            provenance_commit=second_sha,
            authorities=first_package.authorities,
            promotion_state=first_package.promotion_state,
            python_abi=sys.implementation.cache_tag or "invalid",
            now=datetime.now(UTC),
        )
        installer.install(second_package.request())
        second_generation_context = SimpleNamespace(
            package=second_package,
            trusted_base=trusted_base,
            runtime_root=code_runtime_root,
        )
        with real_promotion_authority(
            tmp_path / "promotion-second",
            generation=second_generation_context,
        ) as second_configuration:
            second_arguments = _runtime_code_arguments(second_configuration, trusted_base)
            second_prepare = _run(
                _wrapper_command(checkout, "lab-runtime-prepare", *second_arguments),
                cwd=checkout,
                env=env,
            )
            assert second_prepare.returncode == 0, second_prepare.stderr
            second_manifest = load_job_center_authority(
                current,
                expected_code_sha=second_sha,
                runtime_root=research,
                runtime_deployment_root=runtime_root,
                deployment_profile_id=second_profile_id,
                deployment_generation_hash=second_generation,
            )
            assert second_manifest.manifest_hash != first_manifest.manifest_hash
            assert not (research / ".job-center-authority.candidate.json").exists()
            _prepare_scheduler_keys(research, env)
            scheduler_after_roll = _run(
                _wrapper_command(checkout, "lab-scheduler", *second_arguments, "--once"),
                cwd=checkout,
                env=env,
            )
            assert scheduler_after_roll.returncode == 0, scheduler_after_roll.stderr
    finally:
        _relax_release_tree(checkout.parent / ".rquant-deploy" / f"{checkout.name}.lock")
