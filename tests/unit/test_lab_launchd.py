from __future__ import annotations

import os
import plistlib
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
LAUNCHD_DIR = ROOT / "deploy" / "launchd"
WORKING_DIRECTORY = "/Users/roxor/brain/30-projects/rQuant"
PYTHON = "__RQUANT_GENERATION_PYTHON__"
CODE_ROOT = "__RQUANT_CODE_ROOT__"
WRAPPER = f"{CODE_ROOT}/scripts/run-lab-daemon.py"
EXECUTABLE = "__RQUANT_LAUNCHER__"
TRUSTED_GIT = "/usr/bin/git"
EXPECTED_ROOT_ARGUMENTS = ["--expected-checkout-root", CODE_ROOT]
EXPECTED_CODE_ARGUMENTS = [
    "--expected-code-root",
    CODE_ROOT,
    "--expected-commit",
    "__RQUANT_COMMIT__",
]
TRUSTED_GIT_ARGUMENTS = ["--trusted-git-path", "__RQUANT_TRUSTED_GIT__"]
DEPLOYMENT_LOCK_ARGUMENTS = ["--deployment-lock-path", "__RQUANT_DEPLOYMENT_LOCK__"]
WRAPPER_ARGUMENTS = [
    PYTHON,
    "-I",
    "-S",
    WRAPPER,
    *EXPECTED_ROOT_ARGUMENTS,
    *EXPECTED_CODE_ARGUMENTS,
    *TRUSTED_GIT_ARGUMENTS,
    *DEPLOYMENT_LOCK_ARGUMENTS,
    "--",
]


@pytest.mark.parametrize(
    ("name", "label", "command", "extra_environment"),
    [
        (
            "com.roxor.rquant-lab-scheduler.plist",
            "com.roxor.rquant-lab-scheduler",
            "lab-scheduler",
            {"APP_ENV": "prod", "RQUANT_DISABLE_DOTENV": "1"},
        ),
        (
            "com.roxor.rquant-lab-worker.plist",
            "com.roxor.rquant-lab-worker",
            "lab-worker",
            {},
        ),
        (
            "com.roxor.rquant-lab-finalizer.plist",
            "com.roxor.rquant-lab-finalizer",
            "lab-finalizer",
            {},
        ),
    ],
)
def test_lab_launchd_plists_are_private_bounded_daemons(
    name: str,
    label: str,
    command: str,
    extra_environment: dict[str, str],
) -> None:
    path = LAUNCHD_DIR / name
    with path.open("rb") as stream:
        document = plistlib.load(stream)

    assert document["Label"] == label
    expected_prefix = [
        *WRAPPER_ARGUMENTS,
        EXECUTABLE,
        command,
    ]
    assert document["ProgramArguments"][: len(expected_prefix)] == expected_prefix
    assert document["WorkingDirectory"] == CODE_ROOT
    assert document["RunAtLoad"] is True
    assert document["KeepAlive"] == {"SuccessfulExit": False}
    assert document["ThrottleInterval"] >= 10
    assert document["ExitTimeOut"] >= 30
    assert document["ProcessType"] == "Background"
    assert document["Umask"] == 0o077
    assert document["StandardOutPath"] == "__RQUANT_STDOUT__"
    assert document["StandardErrorPath"] == "__RQUANT_STDERR__"
    assert document.get("EnvironmentVariables", {}) == {
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "PYTHONDONTWRITEBYTECODE": "1",
        "RQUANT_RELEASE_HANDOFF_MANAGED": "1",
        "RQUANT_TRUSTED_GIT_PATH": "__RQUANT_TRUSTED_GIT__",
        **extra_environment,
    }
    serialized = path.read_text(encoding="utf-8")
    assert "SECRET" not in serialized
    assert "KEY=" not in serialized


def test_lab_worker_launchd_uses_configured_stable_identity() -> None:
    path = LAUNCHD_DIR / "com.roxor.rquant-lab-worker.plist"
    with path.open("rb") as stream:
        document = plistlib.load(stream)

    assert document["ProgramArguments"] == [
        *WRAPPER_ARGUMENTS,
        EXECUTABLE,
        "lab-worker",
        "--worker-id",
        "__RQUANT_WORKER_ID__",
    ]


def test_lab_launchd_templates_do_not_bind_mutable_checkout_runtime() -> None:
    for path in LAUNCHD_DIR.glob("com.roxor.rquant-lab-*.plist"):
        serialized = path.read_text(encoding="utf-8")
        assert f"{WORKING_DIRECTORY}/.venv" not in serialized
        assert f"{WORKING_DIRECTORY}/scripts" not in serialized
        assert "__RQUANT_CODE_ROOT__" in serialized


def test_real_worktree_launcher_rejects_symlinked_venv_before_config(
    tmp_path: Path,
) -> None:
    checkout = tmp_path / "synthetic-checkout"
    scripts = checkout / "scripts"
    scripts.mkdir(parents=True, mode=0o700)
    wrapper = scripts / "run-lab-daemon.py"
    shutil.copy2(ROOT / "scripts" / "run-lab-daemon.py", wrapper)
    wrapper.chmod(0o700)
    contained_runner = checkout / "src" / "rquant" / "contained_subprocess.py"
    contained_runner.parent.mkdir(parents=True, mode=0o700)
    shutil.copy2(ROOT / "src" / "rquant" / "contained_subprocess.py", contained_runner)
    benign_venv = Path(sys.executable).resolve().parent.parent
    (checkout / ".venv").symlink_to(benign_venv, target_is_directory=True)

    environment = os.environ.copy()
    environment.pop("PYTHONPATH", None)
    environment["DATA_DIR"] = "relative-data-must-not-be-read"

    result = subprocess.run(
        [
            str(sys.executable),
            "-I",
            "-S",
            str(wrapper),
            "--expected-checkout-root",
            str(checkout),
            "--trusted-git-path",
            TRUSTED_GIT,
            "--deployment-lock-path",
            str(tmp_path / "deployment.lock"),
            "--",
            str(checkout / ".venv" / "bin" / "rquant"),
            "lab-worker",
            "--expected-checkout-root",
            str(checkout),
            "--trusted-git-path",
            TRUSTED_GIT,
            "--worker-id",
            "rquant-mac-primary",
            "--once",
        ],
        cwd=checkout,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )

    output = result.stdout + result.stderr
    assert result.returncode != 0
    assert "physical directory" in output
    assert "relative-data-must-not-be-read" not in output


def test_lab_launchd_plists_pass_plutil_lint() -> None:
    paths = sorted(LAUNCHD_DIR.glob("com.roxor.rquant-lab-*.plist"))
    assert len(paths) == 3
    if sys.platform != "darwin":
        for path in paths:
            with path.open("rb") as stream:
                plistlib.load(stream)
        return
    result = subprocess.run(
        ["plutil", "-lint", *(str(path) for path in paths)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
